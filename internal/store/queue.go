package store

import (
	"container/list"
	"database/sql"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/DovBerKaplan/tg-session-gateway/internal/health"
)

// Queue is the internal update bus between Telegram and consumers:
// bounded in-memory deque + SQLite WAL persistence so unacked updates
// survive gateway restarts. Consumers pull with their own offset and
// ack exactly like against Telegram (offset = first id they want back,
// everything strictly below is acknowledged and dropped).
//
// overflow: "drop_oldest" (default) or "reject_new".
type Queue struct {
	mu             sync.Mutex
	maxsize        int
	overflow       string
	ttlS           float64
	q              *list.List // front = oldest
	seen           map[int64]bool
	consumerOffset int64
	newData        chan struct{} // closed on push; replaced after each wait
	dropped        int
	expired        int

	db              *sql.DB
	botAlias        string
	unloadedBacklog int

	health *health.Storage
}

// QueuedUpdate is one queued Telegram update.
type QueuedUpdate struct {
	UpdateID int64
	Update   map[string]any
	Enqueued time.Time // carries a monotonic reading
}

// OpenQueue opens (or creates) the persistent queue in queuePath for
// one bot alias and reloads its unacked backlog. persistPath empty =
// memory-only.
func OpenQueue(maxsize int, overflow string, ttlS float64, persistPath, botAlias, durability string, h *health.Storage) (*Queue, error) {
	q := &Queue{
		maxsize:  maxsize,
		overflow: overflow,
		ttlS:     ttlS,
		q:        list.New(),
		seen:     map[int64]bool{},
		newData:  make(chan struct{}),
		botAlias: botAlias,
		health:   h,
	}
	if persistPath == "" {
		return q, nil
	}
	dir := filepath.Dir(persistPath)
	_ = os.MkdirAll(dir, 0o755)
	backupsDir := filepath.Join(dir, "backups")
	CheckOrQuarantine(persistPath, backupsDir, h)
	db, err := Open(persistPath, durability, false)
	if err != nil {
		return nil, err
	}
	_, err = db.Exec(`
CREATE TABLE IF NOT EXISTS update_queue (
    bot_alias   TEXT NOT NULL,
    update_id   INTEGER NOT NULL,
    update_json TEXT NOT NULL,
    enqueued_at REAL NOT NULL,
    PRIMARY KEY (bot_alias, update_id)
);
CREATE TABLE IF NOT EXISTS consumer_offsets (
    bot_alias       TEXT PRIMARY KEY,
    consumer_offset INTEGER NOT NULL DEFAULT 0
);`)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	q.db = db
	q.loadFromDB()
	return q, nil
}

// loadFromDB restores the consumer offset and unacked updates after a
// restart. One corrupt row is skipped without losing the rest of the
// backlog; rows beyond the reload cap stay durable but invisible —
// surfaced as unloadedBacklog instead of pretending they loaded.
func (q *Queue) loadFromDB() {
	var offset int64
	err := q.db.QueryRow(
		"SELECT consumer_offset FROM consumer_offsets WHERE bot_alias = ?", q.botAlias).Scan(&offset)
	if err == nil {
		q.consumerOffset = offset
	}
	var cutoff float64
	if q.ttlS > 0 {
		cutoff = wallNow() - q.ttlS
	}
	rows, err := q.db.Query(
		"SELECT update_id, update_json, enqueued_at FROM update_queue "+
			"WHERE bot_alias = ? AND update_id > ? AND enqueued_at > ? ORDER BY update_id LIMIT ?",
		q.botAlias, q.consumerOffset, cutoff, q.maxsize)
	if err != nil {
		slog.Warn("failed to reload queue from SQLite", "alias", q.botAlias, "err", err)
		return
	}
	loaded := 0
	for rows.Next() {
		var uid int64
		var ujson string
		var enqueuedAt float64
		if err := rows.Scan(&uid, &ujson, &enqueuedAt); err != nil {
			continue
		}
		var update map[string]any
		if err := json.Unmarshal([]byte(ujson), &update); err != nil {
			if q.health != nil {
				q.health.NoteCorruptRow()
			}
			slog.Error("corrupt queue row skipped", "alias", q.botAlias, "update_id", uid, "err", err)
			continue
		}
		// wall-clock backfill: age is measured from the original enqueue
		enq := time.Now().Add(-time.Duration((wallNow() - enqueuedAt) * float64(time.Second)))
		q.q.PushBack(QueuedUpdate{uid, update, enq})
		q.seen[uid] = true
		loaded++
	}
	_ = rows.Close()
	_, _ = q.db.Exec("DELETE FROM update_queue WHERE bot_alias = ? AND update_id <= ?", q.botAlias, q.consumerOffset)
	var total int
	_ = q.db.QueryRow(
		"SELECT COUNT(*) FROM update_queue WHERE bot_alias = ? AND update_id > ?",
		q.botAlias, q.consumerOffset).Scan(&total)
	q.unloadedBacklog = total - loaded
	if q.unloadedBacklog < 0 {
		q.unloadedBacklog = 0
	}
	if loaded > 0 {
		slog.Info("restored unacked updates from disk", "alias", q.botAlias, "count", loaded, "offset", q.consumerOffset)
	}
}

// PushAll is the poller side: dedupe, prune, overflow-policy, persist
// the whole batch in one transaction (a single commit instead of one
// fsync per update). ErrQueueOverflow reports the reject_new policy.
func (q *Queue) PushAll(updates []map[string]any) error {
	q.mu.Lock()
	q.pruneExpiredLocked()
	type persisted struct {
		id   int64
		json string
	}
	var batch []persisted
	var overflow error
	for _, u := range updates {
		uid, ok := toInt64(u["update_id"])
		if !ok {
			// synthesize a sequential id when Telegram's is missing
			if q.q.Len() > 0 {
				uid = q.q.Back().Value.(QueuedUpdate).UpdateID + 1
			} else {
				uid = q.consumerOffset + 1
			}
			cp := cloneMap(u)
			cp["update_id"] = uid
			u = cp
		}
		if q.seen[uid] {
			continue // Telegram redelivered — already queued
		}
		if q.q.Len() >= q.maxsize {
			if q.overflow == "reject_new" {
				overflow = ErrQueueOverflow
				break
			}
			old := q.q.Remove(q.q.Front()).(QueuedUpdate)
			delete(q.seen, old.UpdateID)
			q.dropped++
		}
		q.q.PushBack(QueuedUpdate{uid, u, time.Now()})
		q.seen[uid] = true
		if raw, err := json.Marshal(u); err == nil {
			batch = append(batch, persisted{uid, string(raw)})
		}
	}
	q.signalLocked()
	q.mu.Unlock()

	if overflow != nil {
		return overflow
	}
	if q.db != nil && len(batch) > 0 {
		tx, err := q.db.Begin()
		if err != nil {
			q.notePersistFailure("queue.push:"+q.botAlias, err)
			return nil
		}
		stmt, err := tx.Prepare(
			"INSERT OR IGNORE INTO update_queue (bot_alias, update_id, update_json, enqueued_at) VALUES (?, ?, ?, ?)")
		if err != nil {
			_ = tx.Rollback()
			q.notePersistFailure("queue.push:"+q.botAlias, err)
			return nil
		}
		now := wallNow()
		for _, p := range batch {
			_, _ = stmt.Exec(q.botAlias, p.id, p.json, now)
		}
		_ = stmt.Close()
		if err := tx.Commit(); err != nil {
			q.notePersistFailure("queue.push:"+q.botAlias, err)
		}
	}
	return nil
}

// ErrQueueOverflow is the reject_new overflow policy.
var ErrQueueOverflow = errQueueOverflow{}

type errQueueOverflow struct{}

func (errQueueOverflow) Error() string { return "queue overflow (reject_new)" }

// Pull long-polls with REAL Bot API semantics: offset is the id of the
// FIRST update the consumer wants back (highest_seen + 1). Everything
// strictly below offset is acknowledged and dropped. Cancelling ctx
// aborts the wait.
func (q *Queue) Pull(offset *int64, timeout float64, limit int, done <-chan struct{}) []map[string]any {
	if offset != nil {
		q.Ack(*offset - 1)
	}
	if limit <= 0 {
		limit = 100
	}
	deadline := time.Now().Add(time.Duration(timeout * float64(time.Second)))
	for {
		q.mu.Lock()
		var out []map[string]any
		for e := q.q.Front(); e != nil && len(out) < limit; e = e.Next() {
			qu := e.Value.(QueuedUpdate)
			if qu.UpdateID > q.consumerOffset {
				out = append(out, qu.Update)
			}
		}
		q.mu.Unlock()
		if len(out) > 0 {
			return out
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil
		}
		q.mu.Lock()
		ch := q.newData
		q.mu.Unlock()
		timer := time.NewTimer(remaining)
		select {
		case <-ch:
			timer.Stop()
		case <-timer.C:
			return nil
		case <-done:
			timer.Stop()
			return nil
		}
	}
}

// Ack drops everything up to and including lastSeen — consumers ack
// exactly like against Telegram. The DB writes use a snapshot taken
// under the lock: concurrent Acks must not read consumerOffset outside
// the mutex (data race, and a torn DELETE/INSERT pair across acks).
func (q *Queue) Ack(lastSeen int64) {
	q.mu.Lock()
	if lastSeen > q.consumerOffset {
		q.consumerOffset = lastSeen
	}
	for q.q.Len() > 0 {
		front := q.q.Front().Value.(QueuedUpdate)
		if front.UpdateID > q.consumerOffset {
			break
		}
		q.q.Remove(q.q.Front())
		delete(q.seen, front.UpdateID)
	}
	acked := q.consumerOffset
	q.mu.Unlock()
	if q.db != nil {
		_, err := q.db.Exec("DELETE FROM update_queue WHERE bot_alias = ? AND update_id <= ?", q.botAlias, acked)
		if err == nil {
			_, err = q.db.Exec(
				"INSERT INTO consumer_offsets (bot_alias, consumer_offset) VALUES (?, ?) "+
					"ON CONFLICT(bot_alias) DO UPDATE SET consumer_offset = excluded.consumer_offset",
				q.botAlias, acked)
		}
		if err != nil {
			q.notePersistFailure("queue.ack:"+q.botAlias, err)
		}
	}
}

func (q *Queue) pruneExpiredLocked() {
	if q.ttlS <= 0 {
		return
	}
	cutoff := time.Now().Add(-time.Duration(q.ttlS * float64(time.Second)))
	for q.q.Len() > 0 {
		front := q.q.Front().Value.(QueuedUpdate)
		if front.Enqueued.After(cutoff) {
			break
		}
		q.q.Remove(q.q.Front())
		delete(q.seen, front.UpdateID)
		q.expired++
	}
	if q.db != nil {
		_, _ = q.db.Exec("DELETE FROM update_queue WHERE bot_alias = ? AND enqueued_at < ?",
			q.botAlias, wallNow()-q.ttlS)
	}
}

// signalLocked wakes all waiters (asyncio-Event close/recreate port).
// Caller holds q.mu.
func (q *Queue) signalLocked() {
	close(q.newData)
	q.newData = make(chan struct{})
}

// Depth is the current in-memory queue depth.
func (q *Queue) Depth() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.q.Len()
}

// UnloadedBacklog is persisted-but-not-reloaded rows (surface, don't hide).
func (q *Queue) UnloadedBacklog() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.unloadedBacklog
}

// Persistent reports whether the queue survives restarts.
func (q *Queue) Persistent() bool { return q.db != nil }

// Stats is the admin/metrics view.
func (q *Queue) Stats() map[string]any {
	q.mu.Lock()
	defer q.mu.Unlock()
	maxAge := 0.0
	for e := q.q.Front(); e != nil; e = e.Next() {
		age := time.Since(e.Value.(QueuedUpdate).Enqueued).Seconds()
		if age > maxAge {
			maxAge = age
		}
	}
	return map[string]any{
		"depth":           q.q.Len(),
		"dropped_oldest":  q.dropped,
		"expired_by_ttl":  q.expired,
		"max_age_s":       round1(maxAge),
		"consumer_offset": q.consumerOffset,
		"persistent":      q.db != nil,
	}
}

// Close flushes and closes the persistence DB.
func (q *Queue) Close() {
	if q.db != nil {
		_ = q.db.Close()
		q.db = nil
	}
}

func (q *Queue) notePersistFailure(where string, err error) {
	if q.health != nil {
		q.health.NotePersistFailure(where, err)
	} else {
		slog.Warn("persist failure", "where", where, "err", err)
	}
}

func wallNow() float64 { return float64(time.Now().UnixNano()) / 1e9 }

func toInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case float64:
		return int64(n), true
	case int64:
		return n, true
	case int:
		return int64(n), true
	case json.Number:
		i, err := n.Int64()
		return i, err == nil
	}
	return 0, false
}

func cloneMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func round1(f float64) float64 {
	return float64(int(f*10+0.5)) / 10 // matches Python round(x,1) for positives
}
