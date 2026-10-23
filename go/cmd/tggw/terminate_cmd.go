package main

import (
	"fmt"
	"os"
	"syscall"
)

// runTerminate sends SIGTERM to PID 1 — the gateway process in a
// container (distroless has no shell, so the preStop hook is a real
// executable). This kicks off the graceful drain (flush offsets,
// persist state, release locks) the moment Kubernetes starts the pod
// termination, ahead of the runtime's own signal.
func runTerminate() error {
	if err := syscall.Kill(1, syscall.SIGTERM); err != nil {
		return fmt.Errorf("terminate: %w", err)
	}
	fmt.Fprintln(os.Stderr, "terminate: SIGTERM delivered to pid 1")
	return nil
}
