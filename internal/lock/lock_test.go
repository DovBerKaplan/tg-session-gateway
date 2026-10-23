package lock

import (
	"fmt"
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

// TestPythonHolderFreshRefused: during the 0.1.x transition a live
// PYTHON sidecar's lock file (4-line format, fresh mtime) must make
// Acquire refuse — its O_EXCL lock is invisible to flock.
func TestPythonHolderFreshRefused(t *testing.T) {
	dir := t.TempDir()
	lockPath := filepath.Join(dir, "py.lock")
	ts := float64(time.Now().UnixNano()) / 1e9
	content := fmt.Sprintf("4242\noldhost\nbootid\n%f\n", ts)
	if err := os.WriteFile(lockPath, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	l := &Lock{Alias: "py", LockDir: dir}
	if l.Acquire() {
		t.Fatal("acquire must refuse while a fresh Python-format lock exists")
	}
	if l.Held() {
		t.Fatal("refused lock must not be held")
	}
}

// TestPythonHolderStaleProceeds: an OLD Python lock file (dead sidecar)
// must not block — flock acquires and the transition check ignores it.
func TestPythonHolderStaleProceeds(t *testing.T) {
	dir := t.TempDir()
	lockPath := filepath.Join(dir, "py.lock")
	stale := float64(time.Now().Add(-2*time.Minute).UnixNano()) / 1e9
	content := fmt.Sprintf("4242\noldhost\nbootid\n%f\n", stale)
	if err := os.WriteFile(lockPath, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-2 * time.Minute)
	if err := os.Chtimes(lockPath, old, old); err != nil {
		t.Fatal(err)
	}
	l := &Lock{Alias: "py", LockDir: dir}
	if !l.Acquire() {
		t.Fatal("stale Python lock must not block acquire")
	}
	defer l.Release()
}

// TestGoWrittenFileDoesNotTripPythonGuard: a released Go lock leaves a
// fresh holder file behind — the "tggw" 5th line must keep the
// transitional Python guard from firing on it (release → immediate
// re-acquire must succeed).
func TestGoWrittenFileDoesNotTripPythonGuard(t *testing.T) {
	dir := t.TempDir()
	l1 := &Lock{Alias: "g", LockDir: dir}
	if !l1.Acquire() {
		t.Fatal("first acquire")
	}
	l1.Release() // leaves a fresh 5-line (tggw-marked) file on disk
	l2 := &Lock{Alias: "g", LockDir: dir}
	if !l2.Acquire() {
		t.Fatal("re-acquire after release must not trip the Python guard")
	}
	l2.Release()
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
