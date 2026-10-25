//go:build unix

package main

import (
	"os"
	"syscall"
)

func sameDevice(a, b os.FileInfo) bool {
	as, aok := a.Sys().(*syscall.Stat_t)
	bs, bok := b.Sys().(*syscall.Stat_t)
	return aok && bok && as.Dev == bs.Dev
}

// isRootMount reports whether the path itself is an explicit mountpoint
// by comparing to the parent's device (the classic unix definition).
func isRootMount(path string) bool {
	st, err := os.Stat(path)
	if err != nil {
		return false
	}
	pst, err := os.Stat(filepathDir(path))
	if err != nil {
		return false
	}
	return !sameDevice(st, pst)
}

func filepathDir(p string) string {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' && i > 0 {
			return p[:i]
		}
	}
	return "/"
}
