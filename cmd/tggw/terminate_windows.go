//go:build windows

package main

import "fmt"

// Windows containers have no PID-1 SIGTERM: `docker stop` delivers
// console ctrl events, which the gateway's signal handler treats as
// the same graceful drain — so this command has nothing to add. The
// Linux preStop hook (distroless has no shell) is the only caller that
// needs it.
func runTerminate() error {
	return fmt.Errorf("terminate: not needed on Windows; docker stop already triggers the graceful drain")
}
