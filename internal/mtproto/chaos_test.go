package mtproto

// Chaos soak — the port of tests/test_chaos_soak.py to the Go manager.
// Simulates the production failure this product exists to prevent:
// a hard crash with locks stranded and updates queued-but-unacked, then
// a second manager on the same data dir. Invariants:
//
//  1. ZERO AUTH_KEY_DUPLICATED — a second manager while a lock is LIVE
//     is refused (covered by TestSecondSidecarRefusedBeforeTelegram);
//     a STALE (dead holder) lock is reclaimed and exactly one owner
//     connects again.
//  2. ZERO LOST UPDATES — everything pushed before the crash is
//     delivered exactly once per ack.
//  3. Seq continuity — the replayed backlog continues the seq counter.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestKillNineRestartNoDuplicatesNoLoss: two manager LIFETIMES on the
// same data dir; the first is abandoned mid-flight (no Close — process
// death). The stranded lock models a dead holder: the boot janitor
// removes it before the second lifetime starts.
func TestKillNineRestartNoDuplicatesNoLoss(t *testing.T) {
	dir := t.TempDir()
	key := mustTestKey()

	// ── lifetime 1: register, feed updates, NEVER ack ──
	fake1 := &fakeMTClient{}
	app1, ts1 := newTestAppKey(t, fake1, dir, key)
	regADM(t, ts1.URL, "acct0")
	waitLive(t, app1, fake1, "acct0")

	s := app1.Mgr.Get("acct0")
	for i := 0; i < 5; i++ {
		s.pushUpdate(map[string]any{
			"update_id": i + 1, "message": map[string]any{"text": fmt.Sprintf("m%d", i)},
		})
	}

	// KILL -9 equivalent: NO app.Close() — model the kernel releasing
	// the dead holder's flock (releaseLock drops the session's lock
	// without touching its client or supervisor).
	app1.Mgr.releaseLock(app1.Mgr.Get("acct0"))

	// ── lifetime 2: a new app on the SAME data dir ──
	fake2 := &fakeMTClient{}
	app2, _ := newTestAppKey(t, fake2, dir, key)
	if got := app2.Mgr.RestoreAll(context.Background()); got != 1 {
		t.Fatalf("restored %d sessions, want 1", got)
	}
	waitLive(t, app2, fake2, "acct0")

	// invariant 2+3: every fed update delivered EXACTLY once, seq
	// continuous 1..5
	s2 := app2.Mgr.Get("acct0")
	pulled := drainUpdateCh(s2, 5)
	if len(pulled) != 5 {
		t.Fatalf("lost updates: got %d, want 5", len(pulled))
	}
	seqs := make([]int64, 0, len(pulled))
	for _, u := range pulled {
		if v, ok := anyToInt64(u["_gw_seq"]); ok {
			seqs = append(seqs, v)
		}
	}
	for i, v := range seqs {
		if v != int64(i+1) {
			t.Fatalf("seq not continuous/exactly-once: %v", seqs)
		}
	}

	// ack → persistence releases them; a THIRD lifetime finds nothing
	if err := app2.Store.QueueAck("acct0", seqs[len(seqs)-1]); err != nil {
		t.Fatal(err)
	}

	// lifetime 2 "dies" the same way — kernel drops its flock
	app2.Mgr.releaseLock(app2.Mgr.Get("acct0"))

	fake3 := &fakeMTClient{}
	app3, _ := newTestAppKey(t, fake3, dir, key)
	_ = app3.Mgr.RestoreAll(context.Background())
	waitLive(t, app3, fake3, "acct0")
	if backlog := app3.Store.QueuePull("acct0", 0, 100); len(backlog) != 0 {
		t.Fatalf("acked updates replayed: %v", backlog)
	}
}

// TestStaleLockReclaimedExactlyOneOwner: a stranded lock from a DEAD
// holder is reclaimed by the boot janitor and the session connects
// exactly once — never two live owners.
func TestStaleLockReclaimedExactlyOneOwner(t *testing.T) {
	dir := t.TempDir()
	sessDir := filepath.Join(dir, "sessions")
	if err := os.MkdirAll(sessDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// a lock whose holder PID does not exist (dead process) → stale
	deadPID := os.Getpid() + 50000
	lockPath := filepath.Join(sessDir, "acct9.lock")
	stale := float64(time.Now().Add(-2*time.Minute).UnixNano()) / 1e9
	if err := os.WriteFile(lockPath,
		[]byte(fmt.Sprintf("%d\n%f\n", deadPID, stale)), 0o644); err != nil {
		t.Fatal(err)
	}
	// mtime must ALSO be old (heartbeat staleness)
	old := time.Now().Add(-2 * time.Minute)
	if err := os.Chtimes(lockPath, old, old); err != nil {
		t.Fatal(err)
	}

	// the planted file is unheld — flock acquires over it regardless
	// (SweepDir is gone: lock files are never deleted, see ADR-0003)
	_ = lockPath

	fake := &fakeMTClient{}
	app, ts := newTestAppKey(t, fake, dir, mustTestKey())
	regADM(t, ts.URL, "acct9")
	waitLive(t, app, fake, "acct9")
	if fake.startCalls() != 1 {
		t.Fatalf("connect calls = %d, want exactly 1", fake.startCalls())
	}
}
