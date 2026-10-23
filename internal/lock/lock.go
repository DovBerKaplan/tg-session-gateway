// Package lock is the singleton session lock that prevents
// AUTH_KEY_DUPLICATED at the process level.
//
// Mechanism: advisory flock(LOCK_EX|LOCK_NB) on a lock file. The
// KERNEL releases the lock the moment the holder dies — no PID
// liveness probing, no heartbeat staleness window, no PID-namespace
// confusion. This matters in Kubernetes, where every pod is PID 1 and
// a StatefulSet keeps a stable hostname: a PID/heartbeat scheme sees
// the crashed pod's successor as "the live holder" and refuses to
// reconnect for a full staleness window. With flock the successor
// acquires instantly after a crash.
//
// The lock file still records holder identity (PID/host/boot_id/mtime)
// purely for diagnostics — nothing decides on it.
//
// NFS caveat: flock is client-local on NFS. That is fine here: the
// product pins storage to a ReadWriteOncePod PVC (or a single Docker
// host), so two holders can never mount the volume from different
// nodes.
package lock

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

// HeartbeatInterval and StaleAfter are kept for API compatibility;
// they no longer gate anything (flock needs no heartbeat). StaleAfter
// still bounds SweepDir housekeeping.
const (
	HeartbeatInterval = 10 * time.Second
	StaleAfter        = 60 * time.Second
)

// reclaims counts lock files removed by SweepDir (housekeeping, not
// reclaim-anymore: flock makes stale reclaim unnecessary).
var reclaims atomic.Int64

var (
	hostname = func() string {
		h, err := os.Hostname()
		if err != nil {
			return "unknown"
		}
		return h
	}()
	bootID = func() string {
		b, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
		if err != nil {
			return ""
		}
		return strings.TrimSpace(string(b))
	}()
)

// Lock is an advisory file lock around one session. Acquire it BEFORE
// connecting to Telegram so a second process fails at the filesystem
// level and never opens a TCP connection.
type Lock struct {
	Alias   string
	LockDir string
	fd      *os.File
	path    string
	// Token is a per-process acquisition counter (diagnostics/log only).
	Token int64
}

var tokenCounter atomic.Int64

// Path is the lock file path for the alias.
func (l *Lock) Path() string { return filepath.Join(l.LockDir, l.Alias+".lock") }

// Held is true while THIS instance owns the lock (fd open + flocked).
func (l *Lock) Held() bool { return l.fd != nil }

// Acquire takes the lock; false means another LIVE process holds it.
// A dead holder's lock is released by the kernel — there is no stale
// window to wait out.
func (l *Lock) Acquire() bool {
	_ = os.MkdirAll(l.LockDir, 0o755)
	l.path = l.Path()

	f, err := os.OpenFile(l.path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[%s] LOCK REFUSED: cannot open lock file: %v\n", l.Alias, err)
		return false
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		fmt.Fprintf(os.Stderr,
			"[%s] LOCK REFUSED: another process holds this session (holder: %s). "+
				"A second connection would trigger AUTH_KEY_DUPLICATED and may invalidate the auth_key.\n",
			l.Alias, l.readHolder())
		return false
	}
	l.fd = f
	l.Token = tokenCounter.Add(1)
	// diagnostics only — nothing below decides on this content
	content := fmt.Sprintf("%d\n%s\n%s\n%f\n", os.Getpid(), hostname, bootID, float64(time.Now().UnixNano())/1e9)
	_, _ = f.WriteString(content)
	_ = f.Truncate(int64(len(content)))
	return true
}

// Release drops the lock (safe to call multiple times). The file
// itself stays — the next acquirer opens and flocks it; deleting it
// would race an in-flight open+flock pair into two "winners".
func (l *Lock) Release() {
	if l.fd == nil {
		return
	}
	_ = syscall.Flock(int(l.fd.Fd()), syscall.LOCK_UN)
	_ = l.fd.Close()
	l.fd = nil
}

// StartHeartbeat is a no-op kept for API compatibility: flock needs no
// heartbeat, the kernel is the liveness authority.
func (l *Lock) StartHeartbeat() {}

// readHolder reads the diagnostic identity for refusal messages.
func (l *Lock) readHolder() string {
	raw, err := os.ReadFile(l.path)
	if err != nil {
		return "unknown"
	}
	lines := splitLines(raw)
	if len(lines) == 0 || lines[0] == "" {
		return "unknown"
	}
	host := "unknown-host"
	if len(lines) >= 2 && lines[1] != "" {
		host = lines[1]
	}
	return fmt.Sprintf("pid=%s@%s", lines[0], host)
}

func splitLines(raw []byte) []string {
	var out []string
	start := 0
	for i, b := range raw {
		if b == '\n' {
			out = append(out, string(raw[start:i]))
			start = i + 1
		}
	}
	if start < len(raw) {
		out = append(out, string(raw[start:]))
	}
	return out
}

// SweepDir zeroes lock files that are stale AND currently unheld
// (housekeeping for abandoned dirs — flock correctness never depends
// on it). It never UNLINKS: deleting a flock file races an in-flight
// open+flock pair into two "winners" on two inodes; truncating keeps
// the inode and is invisible to flock. Returns how many were cleaned.
func SweepDir(lockDir string) int {
	entries, err := os.ReadDir(lockDir)
	if err != nil {
		return 0
	}
	cleaned := 0
	for _, e := range entries {
		if filepath.Ext(e.Name()) != ".lock" {
			continue
		}
		p := filepath.Join(lockDir, e.Name())
		st, err := os.Stat(p)
		if err != nil || time.Since(st.ModTime()) <= StaleAfter {
			continue
		}
		f, err := os.OpenFile(p, os.O_RDWR, 0o644)
		if err != nil {
			continue
		}
		if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
			_ = f.Close()
			continue // a live holder keeps it
		}
		_ = f.Truncate(0)
		_ = f.Close()
		cleaned++
	}
	if cleaned > 0 {
		reclaims.Add(int64(cleaned))
	}
	return cleaned
}

// ReclaimsTotal reports how many lock files SweepDir cleaned up.
func ReclaimsTotal() int64 { return reclaims.Load() }
