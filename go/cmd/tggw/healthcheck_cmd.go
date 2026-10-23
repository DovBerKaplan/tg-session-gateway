package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

// runHealthcheck GETs /healthz on the local gateway. Exit code 0 =
// alive. This exists because the distroless image has no shell, so
// container healthchecks (Docker Compose, plain Docker) need a real
// executable: healthcheck: ["CMD", "/tggw", "healthcheck"].
func runHealthcheck(args []string) error {
	addr := "http://127.0.0.1:8080"
	if len(args) > 0 && args[0] != "" {
		addr = args[0]
	}
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(addr + "/healthz")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != 200 {
		return fmt.Errorf("healthz status %d", resp.StatusCode)
	}
	fmt.Fprintln(os.Stderr, "healthcheck ok")
	return nil
}
