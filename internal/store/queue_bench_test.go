package store

// Benchmark for the update-passthrough hot path: every consumer poll
// decodes every queued update from the persisted JSON. The numbers to
// compare across the json.RawMessage refactor: ns/op and allocs/op of
// Pull over a full queue.

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
)

func benchQueue(b *testing.B, depth int) {
	dir := b.TempDir()
	q, err := OpenQueue(depth*4, "drop_oldest", 3600, filepath.Join(dir, "queue.db"), "bench", "normal", nil)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(q.Close)
	updates := make([]map[string]any, depth)
	for i := range updates {
		updates[i] = map[string]any{
			"update_id": float64(1000 + i),
			"message": map[string]any{
				"message_id": float64(i),
				"text":       fmt.Sprintf("message body number %d with some length to it", i),
				"chat":       map[string]any{"id": 10.0, "type": "private"},
			},
		}
	}
	if err := q.PushAll(updates); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got := q.Pull(nil, 0, depth, nil)
		if len(got) != depth {
			b.Fatalf("pulled %d, want %d", len(got), depth)
		}
	}
}

func BenchmarkQueuePull100(b *testing.B)  { benchQueue(b, 100) }
func BenchmarkQueuePull1000(b *testing.B) { benchQueue(b, 1000) }

// BenchmarkMarshalUpdate measures the encode side of one update.
func BenchmarkMarshalUpdate(b *testing.B) {
	u := map[string]any{
		"update_id": 1001.0,
		"message":   map[string]any{"text": "hello", "chat": map[string]any{"id": 10.0}},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := json.Marshal(u); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkMTPullRaw vs BenchmarkMTPullMaps: the persisted-pull hot
// path — raw passthrough (splice-ready bytes) against the legacy
// full decode into maps.
func benchMTPull(b *testing.B, raw bool, depth int) {
	dir := b.TempDir()
	mt, err := OpenMT(dir, "CfR-eL8QzhZJQb22PBP1zxH7CzJMFK4LwKvFACh1pfw=", "normal", "", nil)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(mt.Close)
	if err := mt.Register("bench", "tok"); err != nil {
		b.Fatal(err)
	}
	payload := `{"_":"UpdateNewMessage","message":{"id":42,"message":"hello world","peer_id":{"_":"PeerUser","user_id":10}},"pts":100}`
	for i := 0; i < depth; i++ {
		if err := mt.QueuePush("bench", int64(i+1), payload); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if raw {
			if got := mt.QueuePullRaw("bench", 0, depth); len(got) != depth {
				b.Fatalf("raw %d", len(got))
			}
		} else {
			if got := mt.QueuePull("bench", 0, depth); len(got) != depth {
				b.Fatalf("maps %d", len(got))
			}
		}
	}
}

func BenchmarkMTPullMaps100(b *testing.B)  { benchMTPull(b, false, 100) }
func BenchmarkMTPullRaw100(b *testing.B)   { benchMTPull(b, true, 100) }
func BenchmarkMTPullMaps1000(b *testing.B) { benchMTPull(b, false, 1000) }
func BenchmarkMTPullRaw1000(b *testing.B)  { benchMTPull(b, true, 1000) }
