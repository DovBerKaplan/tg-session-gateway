//go:build windows

package lock

import (
	"golang.org/x/sys/windows"
	"os"
)

// The Windows counterpart of flock(LOCK_EX|LOCK_NB): LockFileEx on the
// whole file range, exclusive, fail-immediately. Like flock, the
// kernel (not a PID file) is the liveness authority — the OS drops
// range locks when the holding process dies.

const (
	_lockfileFailImmediately = 0x00000001
	_lockfileExclusiveLock   = 0x00000002
	// lock the entire file: max 32-bit range from offset 0
	_lockRange = ^uint32(0) // 0xFFFFFFFF
)

func flockExclusive(f *os.File) error {
	ol := new(windows.Overlapped) // zero offsets: lock starts at byte 0
	return windows.LockFileEx(
		windows.Handle(f.Fd()),
		_lockfileExclusiveLock|_lockfileFailImmediately,
		0, _lockRange, _lockRange, ol)
}

func flockUnlock(f *os.File) error {
	ol := new(windows.Overlapped)
	return windows.UnlockFileEx(
		windows.Handle(f.Fd()),
		0, _lockRange, _lockRange, ol)
}
