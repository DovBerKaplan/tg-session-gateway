package health

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestBeatAlive(t *testing.T) {
	b := NewBeat()
	defer b.Stop()
	if !b.Alive() {
		t.Fatal("fresh beat must be alive")
	}
	if b.Lag() > 0.1 {
		t.Fatalf("lag too high for a fresh beat: %v", b.Lag())
	}
}

func TestBootTrackerSafeMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "boot_history.json")
	tr := &BootTracker{Path: path}
	for i := 0; i < 5; i++ {
		safe, n := tr.RecordBoot()
		if safe {
			t.Fatalf("boots=%d must not be safe mode yet (max 5)", n)
		}
	}
	safe, n := tr.RecordBoot() // 6th boot in the window
	if !safe || n != 6 {
		t.Fatalf("6th boot must trip safe mode: safe=%v n=%d", safe, n)
	}
	tr.Clear()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("clear must remove the history file")
	}
	safe, _ = tr.RecordBoot()
	if safe {
		t.Fatal("after ack the next boot must be normal")
	}
}

func TestBootTrackerWindowExpiry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "boot_history.json")
	tr := &BootTracker{Path: path, WindowS: 0.05}
	for i := 0; i < 8; i++ {
		tr.RecordBoot()
	}
	time.Sleep(80 * time.Millisecond) // window passes
	safe, n := tr.RecordBoot()
	if safe || n != 1 {
		t.Fatalf("expired boots must not count: safe=%v n=%d", safe, n)
	}
}

func TestStorageDegradationLifecycle(t *testing.T) {
	s := NewStorage()
	if s.Degraded() != "" {
		t.Fatal("fresh storage must not be degraded")
	}
	s.NotePersistFailure("queue.push:b1", errors.New("disk full"))
	if s.Degraded() == "" {
		t.Fatal("persist failure must degrade")
	}
	snap := s.Snapshot()
	if snap["persist_failures"] != 1 {
		t.Fatalf("snapshot persist_failures = %v", snap["persist_failures"])
	}
	s.NoteQuarantine("/data/gateway.db")
	if s.Snapshot()["quarantined_files"] != 1 {
		t.Fatal("quarantine must be counted")
	}
}
