// Package health provides the cross-plane liveness pieces: a goroutine
// heartbeat (the Go analogue of the asyncio loop beat — a starved
// scheduler shows up as lag), the crash-loop safe-mode boot tracker,
// and the visible storage-health state that degrades /readyz.
package health

import (
	"encoding/json"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Beat is a 1s background heartbeat; Lag reports how long since the
// last tick. /healthz serves Alive so a wedged process is restarted,
// not just a process with a PID.
type Beat struct {
	last atomic.Int64 // unix nanos
	stop chan struct{}
	once sync.Once
}

// NewBeat starts the heartbeat goroutine.
func NewBeat() *Beat {
	b := &Beat{stop: make(chan struct{})}
	b.last.Store(time.Now().UnixNano())
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-b.stop:
				return
			case <-t.C:
				b.last.Store(time.Now().UnixNano())
			}
		}
	}()
	return b
}

// Lag is seconds since the last beat.
func (b *Beat) Lag() float64 {
	return float64(time.Now().UnixNano()-b.last.Load()) / 1e9
}

// Alive is true while the beat lag is under 5s.
func (b *Beat) Alive() bool { return b.Lag() < 5.0 }

// Stop terminates the heartbeat goroutine.
func (b *Beat) Stop() { b.once.Do(func() { close(b.stop) }) }

// ── crash-loop safe mode ───────────────────────────────────────────

// BootTracker records boots in a plain JSON file next to the data (not
// in SQLite: if we're crash-looping, the DB may be part of the
// problem). Too many boots in the window → SAFE MODE: sessions stay on
// disk, nothing auto-connects, an admin must ack via the API.
type BootTracker struct {
	Path     string
	WindowS  float64
	MaxBoots int
}

// RecordBoot logs this boot; returns (safeMode, bootsInWindow).
func (t *BootTracker) RecordBoot() (bool, int) {
	if t.WindowS == 0 {
		t.WindowS = 600
	}
	if t.MaxBoots == 0 {
		t.MaxBoots = 5
	}
	now := time.Now()
	var boots []float64
	if raw, err := os.ReadFile(t.Path); err == nil {
		_ = json.Unmarshal(raw, &boots)
	}
	kept := boots[:0]
	for _, b := range boots {
		if now.Sub(time.Unix(int64(b), 0)).Seconds() < t.WindowS {
			kept = append(kept, b)
		}
	}
	kept = append(kept, float64(now.UnixNano())/1e9)
	if len(kept) > 50 {
		kept = kept[len(kept)-50:]
	}
	if raw, err := json.Marshal(kept); err == nil {
		_ = os.WriteFile(t.Path, raw, 0o644) // read-only data dir is a separate, visible failure
	}
	return len(kept) > t.MaxBoots, len(kept)
}

// Clear resets the history after an admin ack — the next crash loop
// needs a fresh MaxBoots boots to re-trigger.
func (t *BootTracker) Clear() { _ = os.Remove(t.Path) }

// ── storage health ─────────────────────────────────────────────────

// Storage makes storage problems VISIBLE (metrics + degraded
// readiness) instead of vanishing into ignored errors.
type Storage struct {
	mu                sync.Mutex
	PersistFailures   int
	CorruptRows       int
	QuarantinedFiles  int
	reasons           map[string]bool
	LastPersistError  string
	LastMaintenanceAt float64
}

// NewStorage builds a healthy storage state.
func NewStorage() *Storage { return &Storage{reasons: map[string]bool{}} }

// NotePersistFailure records a persistence failure (degrades /readyz).
func (s *Storage) NotePersistFailure(where string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.PersistFailures++
	s.LastPersistError = where + ": " + err.Error()
	s.reasons["persist:"+where] = true
	slog.Error("storage persist failure — /readyz degraded", "where", where, "err", err)
}

// NoteCorruptRow counts a skipped corrupt row.
func (s *Storage) NoteCorruptRow() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.CorruptRows++
}

// NoteQuarantine records a quarantined DB file.
func (s *Storage) NoteQuarantine(path string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.QuarantinedFiles++
	s.reasons["quarantined:"+filepathBase(path)] = true
}

// NoteMaintenanceDone stamps the last successful maintenance pass.
func (s *Storage) NoteMaintenanceDone(at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.LastMaintenanceAt = float64(at.UnixNano()) / 1e9
}

func (s *Storage) Degraded() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return joinSorted(s.reasons)
}

// Snapshot is the /readyz view.
func (s *Storage) Snapshot() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	reason := joinSorted(s.reasons)
	if reason == "" {
		reason = ""
	}
	last := s.LastPersistError
	if len(last) > 200 {
		last = last[:200]
	}
	return map[string]any{
		"degraded_reason":    nilIfEmpty(reason),
		"persist_failures":   s.PersistFailures,
		"corrupt_rows":       s.CorruptRows,
		"quarantined_files":  s.QuarantinedFiles,
		"last_persist_error": last,
	}
}

func nilIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func joinSorted(m map[string]bool) string {
	if len(m) == 0 {
		return ""
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sortStrings(keys)
	out := ""
	for i, k := range keys {
		if i > 0 {
			out += ", "
		}
		out += k
	}
	return out
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

func filepathBase(p string) string {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' {
			return p[i+1:]
		}
	}
	return p
}
