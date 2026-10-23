// Package lock is the singleton session lock that prevents
// AUTH_KEY_DUPLICATED at the process level — a port of
// mtgateway/lock.py. O_CREAT|O_EXCL is atomic on all POSIX
// filesystems; the file records the holder identity (PID, hostname,
// boot_id) plus an mtime heartbeat for staleness.
//
// Staleness rules:
//   - same host (or legacy 2-line files): a dead PID is stale
//     IMMEDIATELY; a live PID is stale only after STALE_AFTER_SECONDS
//     without a heartbeat
//   - other host: PID liveness is meaningless across PID namespaces,
//     so only the heartbeat window decides
//
// The Redis lease backend was removed with the Go rewrite: in
// Kubernetes a StatefulSet + ReadWriteOncePod PVC already guarantees
// single ownership, and this file lock remains the third guard before
// connect().
package lock

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	// HeartbeatInterval is how often the holder refreshes the lock mtime.
	HeartbeatInterval = 10 * time.Second
	// StaleAfter is how long a lock without a heartbeat is considered stale.
	StaleAfter = 60 * time.Second
)

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

	// Reclaims counts stale locks reclaimed (acquire path + janitor).
	Reclaims int64
	mu       sync.Mutex
)

// Lock is an advisory file lock around one session. Acquire it BEFORE
// connecting to Telegram so a second process fails at the filesystem
// level and never opens a TCP connection.
type Lock struct {
	Alias    string
	LockDir  string
	fd       *os.File
	path     string
	heartbeatStop chan struct{}
	Token    int64 // fencing token (monotonic per acquisition)
}

// Path is the lock file path for the alias.
func (l *Lock) Path() string { return filepath.Join(l.LockDir, l.Alias+".lock") }

// Held is true while THIS instance owns the lock (fd open).
func (l *Lock) Held() bool { return l.fd != nil }

// Acquire tries to take the lock exclusively; false means another live
// process holds it. A stale lock is reclaimed automatically.
func (l *Lock) Acquire() bool {
	_ = os.MkdirAll(l.LockDir, 0o755)
	l.path = l.Path()

	if _, err := os.Stat(l.path); err == nil {
		if l.isStale() {
			fmt.Fprintf(os.Stderr, "[%s] removing stale lock (holder dead + no heartbeat)\n", l.Alias)
			_ = os.Remove(l.path)
			mu.Lock()
			Reclaims++
			mu.Unlock()
		} else {
			fmt.Fprintf(os.Stderr,
				"[%s] LOCK REFUSED: another process holds this session (holder: %s). "+
					"A second connection would trigger AUTH_KEY_DUPLICATED and may invalidate the auth_key.\n",
				l.Alias, l.readHolder())
			return false
		}
	}

	f, err := os.OpenFile(l.path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[%s] LOCK REFUSED: race lost acquiring lock\n", l.Alias)
		return false
	}
	l.fd = f
	content := fmt.Sprintf("%d\n%s\n%s\n%f\n", os.Getpid(), hostname, bootID, float64(time.Now().UnixNano())/1e9)
	_, _ = f.WriteString(content)
	l.Token = l.nextFencingToken()
	return true
}

func (l *Lock) nextFencingToken() int64 {
	counter := filepath.Join(l.LockDir, ".fence_counter")
	n := int64(1)
	if raw, err := os.ReadFile(counter); err == nil {
		if v, err := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64); err == nil {
			n = v + 1
		}
	}
	_ = os.WriteFile(counter, []byte(strconv.FormatInt(n, 10)), 0o644)
	return n
}

// Release drops the lock (safe to call multiple times); only removes
// the file when it still carries THIS holder's identity.
func (l *Lock) Release() {
	if l.fd == nil {
		return
	}
	if l.heartbeatStop != nil {
		close(l.heartbeatStop)
		l.heartbeatStop = nil
	}
	_ = l.fd.Close()
	l.fd = nil
	if l.path != "" {
		if _, err := os.Stat(l.path); err == nil {
			pid, host, _ := l.readIdentity()
			ours := pid != nil && *pid == int64(os.Getpid()) && (host == nil || *host == hostname)
			if ours {
				_ = os.Remove(l.path)
			}
		}
	}
}

// StartHeartbeat touches the lock mtime to prove we're alive.
func (l *Lock) StartHeartbeat() {
	if l.heartbeatStop != nil {
		return
	}
	stop := make(chan struct{})
	l.heartbeatStop = stop
	go func() {
		t := time.NewTicker(HeartbeatInterval)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				now := time.Now()
				_ = os.Chtimes(l.path, now, now)
			}
		}
	}()
}

// readIdentity parses (pid, hostname, boot_id) from the lock file.
// Legacy v1 files (pid + timestamp, 2 lines) return hostname=nil →
// same-host semantics, exactly like the Python implementation.
func (l *Lock) readIdentity() (*int64, *string, *string) {
	raw, err := os.ReadFile(l.path)
	if err != nil {
		return nil, nil, nil
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	var pid *int64
	if len(lines) >= 1 {
		if v, err := strconv.ParseInt(strings.TrimSpace(lines[0]), 10, 64); err == nil {
			pid = &v
		}
	}
	if len(lines) < 4 {
		return pid, nil, nil
	}
	host := strings.TrimSpace(lines[1])
	bid := strings.TrimSpace(lines[2])
	return pid, &host, &bid
}

func (l *Lock) readHolder() string {
	pid, host, _ := l.readIdentity()
	if pid == nil {
		return "unknown"
	}
	h := hostname
	if host != nil {
		h = *host
	}
	return fmt.Sprintf("pid=%d@%s", *pid, h)
}

func (l *Lock) heartbeatStale() bool {
	st, err := os.Stat(l.path)
	if err != nil {
		return true
	}
	return time.Since(st.ModTime()) > StaleAfter
}

func (l *Lock) isStale() bool {
	pid, host, _ := l.readIdentity()
	if pid == nil && host == nil {
		return true // unreadable = treat as stale
	}
	if host != nil && *host != hostname {
		return l.heartbeatStale()
	}
	if pid != nil {
		if err := syscall.Kill(int(*pid), 0); err != nil {
			if err == syscall.ESRCH {
				return true // holder is dead — reclaim now
			}
			// EPERM: exists but owned by another user
		}
	}
	if l.heartbeatStale() {
		return true
	}
	return false
}

// SweepDir is the boot janitor: remove every *.lock stale by the
// Acquire rules. Returns how many were removed.
func SweepDir(lockDir string) int {
	entries, err := os.ReadDir(lockDir)
	if err != nil {
		return 0
	}
	removed := 0
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".lock") {
			continue
		}
		probe := &Lock{Alias: strings.TrimSuffix(e.Name(), ".lock"), LockDir: lockDir}
		probe.path = filepath.Join(lockDir, e.Name())
		if probe.isStale() {
			if err := os.Remove(probe.path); err == nil {
				removed++
				mu.Lock()
				Reclaims++
				mu.Unlock()
				fmt.Fprintf(os.Stderr, "janitor: removed stale lock %s\n", e.Name())
			}
		}
	}
	return removed
}

// ReclaimsTotal reports the process-wide stale-lock reclaim count.
func ReclaimsTotal() int64 {
	mu.Lock()
	defer mu.Unlock()
	return Reclaims
}
