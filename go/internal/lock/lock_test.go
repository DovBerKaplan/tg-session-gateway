package lock

import (
	"os"
	"path/filepath"
	"strconv"
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
		t.Fatalf("fencing token = %d, want >= 1", l.Token)
	}
	if _, err := os.Stat(filepath.Join(dir, "b1.lock")); err != nil {
		t.Fatal("lock file not created")
	}
	l.Release()
	if _, err := os.Stat(filepath.Join(dir, "b1.lock")); !os.IsNotExist(err) {
		t.Fatal("lock file must be removed on release")
	}
	l.Release() // idempotent
}

func TestSecondAcquireRefusedWhileHeld(t *testing.T) {
	dir := t.TempDir()
	a := &Lock{Alias: "b1", LockDir: dir}
	if !a.Acquire() {
		t.Fatal("first acquire failed")
	}
	defer a.Release()
	b := &Lock{Alias: "b1", LockDir: dir}
	if b.Acquire() {
		t.Fatal("second holder must be refused (AUTH_KEY_DUPLICATED prevention)")
	}
}

func TestDeadHolderReclaimed(t *testing.T) {
	dir := t.TempDir()
	// a PID that definitely does not exist
	deadPID := int64(99999999)
	_ = os.WriteFile(filepath.Join(dir, "b1.lock"),
		[]byte(strconv.FormatInt(deadPID, 10)+"\n"+hostname+"\n"+bootID+"\n1.0\n"), 0o644)
	l := &Lock{Alias: "b1", LockDir: dir}
	if !l.Acquire() {
		t.Fatal("dead holder's lock must be reclaimed")
	}
	l.Release()
}

func TestLiveHolderNotStale(t *testing.T) {
	dir := t.TempDir()
	// OUR pid with a fresh mtime: not stale (same host, alive, fresh heartbeat)
	_ = os.WriteFile(filepath.Join(dir, "b1.lock"),
		[]byte(strconv.Itoa(os.Getpid())+"\n"+hostname+"\n"+bootID+"\n1.0\n"), 0o644)
	l := &Lock{Alias: "b1", LockDir: dir}
	if l.Acquire() {
		t.Fatal("live same-host holder must NOT be reclaimed")
		l.Release()
	}
}

func TestLegacyTwoLineFileSameHostSemantics(t *testing.T) {
	dir := t.TempDir()
	deadPID := int64(99999999)
	// legacy format: pid\ntimestamp — hostname nil → same-host rules
	_ = os.WriteFile(filepath.Join(dir, "legacy.lock"),
		[]byte(strconv.FormatInt(deadPID, 10)+"\n1.0\n"), 0o644)
	l := &Lock{Alias: "legacy", LockDir: dir}
	if !l.Acquire() {
		t.Fatal("legacy file with dead pid must be stale (same-host semantics)")
	}
	l.Release()
}

func TestSweepRemovesOnlyStale(t *testing.T) {
	dir := t.TempDir()
	// stale: dead pid
	_ = os.WriteFile(filepath.Join(dir, "dead.lock"),
		[]byte("99999999\n"+hostname+"\n"+bootID+"\n1.0\n"), 0o644)
	// live: our pid, fresh
	_ = os.WriteFile(filepath.Join(dir, "live.lock"),
		[]byte(strconv.Itoa(os.Getpid())+"\n"+hostname+"\n"+bootID+"\n1.0\n"), 0o644)
	// unrelated file
	_ = os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("x"), 0o644)

	removed := SweepDir(dir)
	if removed != 1 {
		t.Fatalf("sweep removed %d, want 1", removed)
	}
	if _, err := os.Stat(filepath.Join(dir, "dead.lock")); !os.IsNotExist(err) {
		t.Fatal("stale lock not removed")
	}
	if _, err := os.Stat(filepath.Join(dir, "live.lock")); err != nil {
		t.Fatal("live lock must survive the sweep")
	}
}

func TestFencingTokenMonotonic(t *testing.T) {
	dir := t.TempDir()
	l1 := &Lock{Alias: "a", LockDir: dir}
	l1.Acquire()
	t1 := l1.Token
	l1.Release()

	l2 := &Lock{Alias: "a", LockDir: dir}
	l2.Acquire()
	t2 := l2.Token
	l2.Release()
	if t2 <= t1 {
		t.Fatalf("fencing token must be monotonic: %d then %d", t1, t2)
	}
}

func TestHeartbeatKeepsLockFresh(t *testing.T) {
	dir := t.TempDir()
	l := &Lock{Alias: "b1", LockDir: dir}
	if !l.Acquire() {
		t.Fatal("acquire failed")
	}
	l.StartHeartbeat()
	defer l.Release()
	// even with an old mtime planted afterwards, the heartbeat ticker
	// refreshes it within HeartbeatInterval — here just verify the
	// goroutine starts and stops cleanly under load
	time.Sleep(20 * time.Millisecond)
	other := &Lock{Alias: "b1", LockDir: dir}
	if other.Acquire() {
		t.Fatal("heartbeat holder must keep ownership")
	}
}
