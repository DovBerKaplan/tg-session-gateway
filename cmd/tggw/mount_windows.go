//go:build windows

package main

import "path/filepath"

// Windows volume roots (C:\, D:\) are real filesystem boundaries, so
// they satisfy the mounted-volume preflight the same way a unix
// mountpoint does; everything below the root is one per-drive
// namespace with no volume boundary.

func isRootMount(path string) bool {
	vol := filepath.VolumeName(path)
	return vol != "" && path == vol+`\`
}
