package store

import (
	"database/sql"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/DovBerKaplan/tg-session-gateway/go/internal/cryptoutil"
	"github.com/DovBerKaplan/tg-session-gateway/go/internal/health"
)

// mtSchema matches mtgateway/store.py so an existing mtgateway.db
// opens in place.
const mtSchema = `
CREATE TABLE IF NOT EXISTS sessions (
    alias       TEXT PRIMARY KEY,
    token_enc   BLOB NOT NULL,
    phone_or_token TEXT NOT NULL DEFAULT '',
    pts         INTEGER NOT NULL DEFAULT 0,
    qts         INTEGER NOT NULL DEFAULT 0,
    date        INTEGER NOT NULL DEFAULT 0,
    seq         INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    paused      INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS update_queue (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    alias       TEXT NOT NULL,
    update_id   INTEGER NOT NULL,
    update_json TEXT NOT NULL,
    enqueued_at REAL NOT NULL,
    FOREIGN KEY (alias) REFERENCES sessions(alias) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_queue_alias ON update_queue(alias, update_id);

CREATE TABLE IF NOT EXISTS admin_audit (
    ts       TEXT NOT NULL DEFAULT (datetime('now')),
    action   TEXT NOT NULL,
    alias    TEXT NOT NULL,
    detail   TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS idempotency_keys (
    alias    TEXT NOT NULL,
    key      TEXT NOT NULL,
    state    TEXT NOT NULL DEFAULT 'pending',
    seen_at  REAL NOT NULL,
    PRIMARY KEY (alias, key)
);
CREATE TABLE IF NOT EXISTS dead_letters (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    alias   TEXT NOT NULL,
    payload TEXT NOT NULL,
    reason  TEXT NOT NULL,
    at      REAL NOT NULL
);
`

// MT is the MTProto plane store: session metadata, Telegram update
// state (pts/qts/date/seq), the persisted update queue, send
// idempotency, dead letters and the audit trail.
type MT struct {
	DB         *sql.DB
	Path       string
	BackupsDir string
	box        *cryptoutil.Box
}

// OpenMT opens (or creates) mtgateway.db in dataDir.
func OpenMT(dataDir, fernetKey, durability string, backupsDir string, h *health.Storage) (*MT, error) {
	_ = os.MkdirAll(dataDir, 0o755)
	_ = os.MkdirAll(filepath.Join(dataDir, "sessions"), 0o755)
	box, err := cryptoutil.LoadKey(fernetKey, dataDir)
	if err != nil {
		return nil, err
	}
	path := filepath.Join(dataDir, "mtgateway.db")
	CheckOrQuarantine(path, backupsDir, h)
	db, err := Open(path, durability, true)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(mtSchema); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := migrateIdempotencyV1(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := EnsureSchema(db, 1, map[int]string{}, path, backupsDir); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &MT{DB: db, Path: path, BackupsDir: backupsDir, box: box}, nil
}

// Close releases the DB handle.
func (m *MT) Close() { _ = m.DB.Close() }

// migrateIdempotencyV1 upgrades the pre-versioning idempotency table
// (key as lone PK) to (alias, key) PK + pending/done state. Old rows
// migrate as done. One-time, idempotent.
func migrateIdempotencyV1(db *sql.DB) error {
	rows, err := db.Query("PRAGMA table_info(idempotency_keys)")
	if err != nil {
		return err
	}
	hasState := false
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull, pk int
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err == nil && name == "state" {
			hasState = true
		}
	}
	_ = rows.Close()
	if hasState {
		return nil
	}
	slog.Warn("migrating idempotency_keys to (alias,key) PK + state")
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS idempotency_keys_v2 (
    alias    TEXT NOT NULL,
    key      TEXT NOT NULL,
    state    TEXT NOT NULL DEFAULT 'pending',
    seen_at  REAL NOT NULL,
    PRIMARY KEY (alias, key)
);`); err != nil {
		return err
	}
	if _, err := db.Exec(
		"INSERT OR IGNORE INTO idempotency_keys_v2 (alias, key, state, seen_at) " +
			"SELECT alias, key, 'done', seen_at FROM idempotency_keys"); err != nil {
		slog.Warn("idempotency migration copy skipped", "err", err)
	}
	if _, err := db.Exec("DROP TABLE IF EXISTS idempotency_keys"); err != nil {
		return err
	}
	_, err = db.Exec("ALTER TABLE idempotency_keys_v2 RENAME TO idempotency_keys")
	return err
}

// Register upserts a session with its encrypted token.
func (m *MT) Register(alias, token string) error {
	enc, err := m.box.Encrypt([]byte(token))
	if err != nil {
		return err
	}
	phone := token
	if len(phone) > 20 {
		phone = phone[:20] + "…"
	}
	_, err = m.DB.Exec(
		"INSERT INTO sessions (alias, token_enc, phone_or_token) VALUES (?, ?, ?) "+
			"ON CONFLICT(alias) DO UPDATE SET token_enc = excluded.token_enc",
		alias, enc, phone)
	return err
}

// Delete removes a session; false when unknown.
func (m *MT) Delete(alias string) (bool, error) {
	res, err := m.DB.Exec("DELETE FROM sessions WHERE alias = ?", alias)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// SessionRow is the list view.
type SessionRow struct {
	Alias        string `json:"alias"`
	PhoneOrToken string `json:"phone_or_token"`
	PTS          int64  `json:"pts"`
	QTS          int64  `json:"qts"`
	Date         int64  `json:"date"`
	Seq          int64  `json:"seq"`
	Paused       bool   `json:"paused"`
	CreatedAt    string `json:"created_at"`
}

// ListSessions returns every registered session.
func (m *MT) ListSessions() ([]SessionRow, error) {
	rows, err := m.DB.Query(
		"SELECT alias, phone_or_token, pts, qts, date, seq, paused, created_at FROM sessions")
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []SessionRow
	for rows.Next() {
		var r SessionRow
		var paused int
		if err := rows.Scan(&r.Alias, &r.PhoneOrToken, &r.PTS, &r.QTS, &r.Date, &r.Seq, &paused, &r.CreatedAt); err != nil {
			return nil, err
		}
		r.Paused = paused != 0
		out = append(out, r)
	}
	return out, rows.Err()
}

// GetToken decrypts the stored session token.
func (m *MT) GetToken(alias string) (string, bool) {
	var tokenEnc []byte
	err := m.DB.QueryRow("SELECT token_enc FROM sessions WHERE alias = ?", alias).Scan(&tokenEnc)
	if err != nil {
		return "", false
	}
	plain, ok := m.box.Decrypt(tokenEnc)
	if !ok {
		slog.Error("cannot decrypt token (wrong GW_FERNET_KEY?)", "alias", alias)
		return "", false
	}
	return string(plain), true
}

// SetPaused records the admin pause flag.
func (m *MT) SetPaused(alias string, paused bool) error {
	_, err := m.DB.Exec("UPDATE sessions SET paused = ? WHERE alias = ?", boolInt(paused), alias)
	return err
}

// VerifyTokens is the boot probe for undecryptable tokens.
func (m *MT) VerifyTokens() []string {
	var bad []string
	sessions, err := m.ListSessions()
	if err != nil {
		return bad
	}
	for _, s := range sessions {
		if _, ok := m.GetToken(s.Alias); !ok {
			bad = append(bad, s.Alias)
		}
	}
	return bad
}

// MTState is the Telegram update state snapshot.
type MTState struct {
	PTS  int64
	QTS  int64
	Date int64
	Seq  int64
}

// GetState loads pts/qts/date/seq for a session.
func (m *MT) GetState(alias string) MTState {
	var st MTState
	_ = m.DB.QueryRow("SELECT pts, qts, date, seq FROM sessions WHERE alias = ?", alias).
		Scan(&st.PTS, &st.QTS, &st.Date, &st.Seq)
	return st
}

// SaveState persists the update state snapshot.
func (m *MT) SaveState(alias string, st MTState) error {
	_, err := m.DB.Exec(
		"UPDATE sessions SET pts = ?, qts = ?, date = ?, seq = ? WHERE alias = ?",
		st.PTS, st.QTS, st.Date, st.Seq, alias)
	return err
}

// QueuePush persists one update (write-through; unacked survive kill -9).
func (m *MT) QueuePush(alias string, updateID int64, updateJSON string) error {
	_, err := m.DB.Exec(
		"INSERT INTO update_queue (alias, update_id, update_json, enqueued_at) VALUES (?, ?, ?, ?)",
		alias, updateID, updateJSON, wallNow())
	return err
}

// QueuePull returns persisted updates with id > offset, in order,
// re-attached with their persisted _gw_seq and payload fields.
func (m *MT) QueuePull(alias string, offset int64, limit int) []map[string]any {
	if limit <= 0 {
		limit = 100
	}
	rows, err := m.DB.Query(
		"SELECT update_id, update_json FROM update_queue "+
			"WHERE alias = ? AND update_id > ? ORDER BY update_id LIMIT ?",
		alias, offset, limit)
	if err != nil {
		return nil
	}
	defer func() { _ = rows.Close() }()
	var out []map[string]any
	for rows.Next() {
		var uid int64
		var ujson string
		if err := rows.Scan(&uid, &ujson); err != nil {
			continue
		}
		var update map[string]any
		if err := json.Unmarshal([]byte(ujson), &update); err != nil {
			continue
		}
		if update == nil {
			update = map[string]any{}
		}
		update["update_id"] = uid
		out = append(out, update)
	}
	return out
}

// QueueAck deletes everything up to and including lastSeen.
func (m *MT) QueueAck(alias string, lastSeen int64) error {
	_, err := m.DB.Exec("DELETE FROM update_queue WHERE alias = ? AND update_id <= ?", alias, lastSeen)
	return err
}

// QueueDepth is the persisted backlog size.
func (m *MT) QueueDepth(alias string) int {
	var n int
	_ = m.DB.QueryRow("SELECT COUNT(*) FROM update_queue WHERE alias = ?", alias).Scan(&n)
	return n
}

// QueueTTLCleanup drops persisted updates older than the cutoff
// (spec §12: queued update content is short-lived).
func (m *MT) QueueTTLCleanup(alias string, cutoff float64) int {
	res, err := m.DB.Exec("DELETE FROM update_queue WHERE alias = ? AND enqueued_at < ?", alias, cutoff)
	if err != nil {
		return 0
	}
	n, _ := res.RowsAffected()
	return int(n)
}

// IdempotencyClaim atomically claims a send key (same contract as the
// Bot API store): claimed / reclaimed / replay.
func (m *MT) IdempotencyClaim(alias, key string) string {
	now := wallNow()
	_, _ = m.DB.Exec(
		"DELETE FROM idempotency_keys WHERE alias = ? AND key = ? AND seen_at < ?",
		alias, key, now-IdempotencyTTL)
	res, err := m.DB.Exec(
		"INSERT INTO idempotency_keys (alias, key, state, seen_at) "+
			"VALUES (?, ?, 'pending', ?) ON CONFLICT(alias, key) DO NOTHING",
		alias, key, now)
	if err == nil {
		if n, _ := res.RowsAffected(); n == 1 {
			return "claimed"
		}
	}
	res, err = m.DB.Exec(
		"UPDATE idempotency_keys SET seen_at = ? "+
			"WHERE alias = ? AND key = ? AND state = 'pending' AND seen_at < ?",
		now, alias, key, now-StalePendingS)
	if err == nil {
		if n, _ := res.RowsAffected(); n == 1 {
			return "reclaimed"
		}
	}
	return "replay"
}

// IdempotencyFinish marks a completed send.
func (m *MT) IdempotencyFinish(alias, key string) {
	_, _ = m.DB.Exec(
		"UPDATE idempotency_keys SET state = 'done', seen_at = ? WHERE alias = ? AND key = ?",
		wallNow(), alias, key)
}

// IdempotencyFail releases the claim so a retry is not suppressed.
func (m *MT) IdempotencyFail(alias, key string) {
	_, _ = m.DB.Exec(
		"DELETE FROM idempotency_keys WHERE alias = ? AND key = ? AND state = 'pending'",
		alias, key)
}

// IdempotencyCleanup drops expired keys (the table grows unbounded
// otherwise).
func (m *MT) IdempotencyCleanup() {
	_, _ = m.DB.Exec("DELETE FROM idempotency_keys WHERE seen_at < ?", wallNow()-IdempotencyTTL)
}

// DeadLetter captures an unprocessable update instead of dropping it.
func (m *MT) DeadLetter(alias, payload, reason string) {
	_, err := m.DB.Exec(
		"INSERT INTO dead_letters (alias, payload, reason, at) VALUES (?, ?, ?, ?)",
		alias, payload, reason, wallNow())
	if err != nil {
		slog.Error("dead-letter write failed", "err", err)
	}
}

// DeadLetterRow is one poison update.
type DeadLetterRow struct {
	ID      int64   `json:"id"`
	Payload string  `json:"payload"`
	Reason  string  `json:"reason"`
	At      float64 `json:"at"`
}

// ListDeadLetters inspects poison updates.
func (m *MT) ListDeadLetters(alias string, limit int) []DeadLetterRow {
	if limit <= 0 {
		limit = 100
	}
	rows, err := m.DB.Query(
		"SELECT id, payload, reason, at FROM dead_letters WHERE alias = ? ORDER BY id DESC LIMIT ?",
		alias, limit)
	if err != nil {
		return nil
	}
	defer func() { _ = rows.Close() }()
	var out []DeadLetterRow
	for rows.Next() {
		var r DeadLetterRow
		if err := rows.Scan(&r.ID, &r.Payload, &r.Reason, &r.At); err == nil {
			out = append(out, r)
		}
	}
	return out
}

// ClearDeadLetters drops the poison queue; returns how many.
func (m *MT) ClearDeadLetters(alias string) int {
	res, err := m.DB.Exec("DELETE FROM dead_letters WHERE alias = ?", alias)
	if err != nil {
		return 0
	}
	n, _ := res.RowsAffected()
	return int(n)
}

// Audit records an admin action; never raises.
func (m *MT) Audit(action, alias, detail string) {
	_, err := m.DB.Exec("INSERT INTO admin_audit (action, alias, detail) VALUES (?, ?, ?)", action, alias, detail)
	if err != nil {
		slog.Error("audit write failed", "action", action, "alias", alias, "err", err)
	}
}

// GetAudit returns the newest audit entries.
func (m *MT) GetAudit(limit int) []AuditEntry {
	if limit <= 0 {
		limit = 100
	}
	rows, err := m.DB.Query(
		"SELECT ts, action, alias, detail FROM admin_audit ORDER BY rowid DESC LIMIT ?", limit)
	if err != nil {
		return nil
	}
	defer func() { _ = rows.Close() }()
	var out []AuditEntry
	for rows.Next() {
		var e AuditEntry
		if err := rows.Scan(&e.TS, &e.Action, &e.Alias, &e.Detail); err == nil {
			out = append(out, e)
		}
	}
	return out
}
