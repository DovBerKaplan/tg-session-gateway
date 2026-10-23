package lock

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestAcquireRelease(t *testing.T) {
	dir := t.TempDir()
	l := &Lock{Alias: "b1", LockDir: dir}
	if !l.Acquire() {
		t.Fatal("first acquire must succeed")
	}
	if !l.Held() {
		t.Fatal("held after acquire")
	}
	if l.Token < 1 {
		t.Fatalf("token = %d, want > 0", l.Token)
	}
	if _, err := os.Stat(l.Path()); err != nil {
		t.Fatalf("lock file must exist for diagnostics: %v", err)
	}

	// a second instance while the first holds → refused
	l2 := &Lock{Alias: "b1", LockDir: dir}
	if l2.Acquire() {
		t.Fatal("second acquire must fail while held")
	}
	if l2.Held() {
		t.Fatal("refused lock must not be held")
	}

	// release → second acquirer wins; the FILE stays (never unlinked)
	l.Release()
	if l.Held() {
		t.Fatal("not held after release")
	}
	if !l2.Acquire() {
		t.Fatal("acquire after release must succeed")
	}
	if _, err := os.Stat(l.Path()); err != nil {
		t.Fatalf("lock file must persist across release (unlink races flock): %v", err)
	}
	// double release is safe
	l.Release()
	l2.Release()
}

// TestDistinctAliasesDontBlock: locks are per-alias files.
func TestDistinctAliasesDontBlock(t *testing.T) {
	dir := t.TempDir()
	a := &Lock{Alias: "a", LockDir: dir}
	b := &Lock{Alias: "b", LockDir: dir}
	if !a.Acquire() || !b.Acquire() {
		t.Fatal("different aliases must not block each other")
	}
	a.Release()
	b.Release()
}

// TestHolderDeathReleasesInstantly is the Kubernetes regression test:
// a StatefulSet pod is PID 1 with a stable hostname, so the old
// PID/heartbeat scheme saw the crashed pod's successor as "the live
// holder" and waited out a full staleness window. With flock the
// kernel releases on holder death — the successor must acquire
// immediately, no sleep.
func TestHolderDeathReleasesInstantly(t *testing.T) {
	if testing.Short() {
		t.Skip("subprocess test")
	}
	dir := t.TempDir()

	// child: acquire the lock, then die on SIGKILL without releasing
	// (simulates OOM-kill / crash — the kernel drops the flock)
	self := os.Args[0]
	cmd := exec.Command(self, "-test.run=TestLockHelperProcess", "--", dir)
	cmd.Env = append(os.Environ(),
		"GO_WANT_HELPER_PROCESS=1",
		"LOCK_DIR="+dir)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("helper should have been killed, exited cleanly:\n%s", out)
	}

	// the helper is dead NOW; acquiring must succeed without waiting
	start := time.Now()
	l := &Lock{Alias: "dead", LockDir: dir}
	if !l.Acquire() {
		t.Fatal("acquire after holder death failed — flock should pass instantly")
	}
	defer l.Release()
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("acquire took %v — flock should be instant after holder death", elapsed)
	}
}

// TestLockHelperProcess is not a real test; it is the child of
// TestHolderDeathReleasesInstantly (os/exec helper-process pattern).
func TestLockHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}
	l := &Lock{Alias: "dead", LockDir: os.Getenv("LOCK_DIR")}
	if !l.Acquire() {
		os.Exit(2)
	}
	// die without Release: SIGKILL self → kernel drops the flock
	p, _ := os.FindProcess(os.Getpid())
	_ = p.Signal(os.Kill)
	select {} // never reached
}

// TestSweepDirSkipsHeldLocks: housekeeping must not touch a live
// holder's file, and must clean stale unheld ones (by truncation —
// never unlink, which would race open+flock into two winners).
func TestSweepDirSkipsHeldLocks(t *testing.T) {
	dir := t.TempDir()

	held := &Lock{Alias: "held", LockDir: dir}
	if !held.Acquire() {
		t.Fatal("acquire held")
	}
	defer held.Release()
	// age it past StaleAfter: mtime only, flock still ours
	old := time.Now().Add(-2 * time.Minute)
	if err := os.Chtimes(held.Path(), old, old); err != nil {
		t.Fatal(err)
	}

	// an unheld stale file (dead holder left it behind)
	stalePath := filepath.Join(dir, "gone.lock")
	if err := os.WriteFile(stalePath, []byte("999999\noldhost\n\n1.0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(stalePath, old, old); err != nil {
		t.Fatal(err)
	}

	if cleaned := SweepDir(dir); cleaned != 1 {
		t.Fatalf("SweepDir cleaned %d, want 1 (the unheld stale file)", cleaned)
	}
	if _, err := os.Stat(held.Path()); err != nil {
		t.Fatalf("live holder's lock file must survive: %v", err)
	}
	info, err := os.Stat(stalePath)
	if err != nil {
		t.Fatalf("stale file must survive as a truncated inode: %v", err)
	}
	if info.Size() != 0 {
		t.Fatalf("stale unheld file must be truncated, size=%d", info.Size())
	}
	if ReclaimsTotal() < 1 {
		t.Fatal("reclaim metric must count the cleanup")
	}

	// fresh unheld files are left alone
	fresh := filepath.Join(dir, "fresh.lock")
	if err := os.WriteFile(fresh, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if cleaned := SweepDir(dir); cleaned != 0 {
		t.Fatalf("fresh unheld file must not be cleaned, got %d", cleaned)
	}
}

// TestRefusalMessageContainsHolder: the diagnostic identity written at
// acquire surfaces in readHolder for refusal messages.
func TestRefusalMessageContainsHolder(t *testing.T) {
	dir := t.TempDir()
	l := &Lock{Alias: "diag", LockDir: dir}
	if !l.Acquire() {
		t.Fatal("acquire")
	}
	defer l.Release()
	l2 := &Lock{Alias: "diag", LockDir: dir}
	l2.path = l2.Path()
	holder := l2.readHolder()
	if holder == "unknown" || holder == "" {
		t.Fatalf("holder diagnostic = %q, want pid@host", holder)
	}
}
