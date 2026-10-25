//go:build unix

package lock

import (
	"os"
	"syscall"
)

// flockExclusive takes a non-blocking exclusive whole-file advisory
// lock; an error means another live process holds it.
func flockExclusive(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
}

// flockUnlock releases the whole-file advisory lock.
func flockUnlock(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}
