package store

import (
	"database/sql"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/DovBerKaplan/tg-session-gateway/go/internal/cryptoutil"
	"github.com/DovBerKaplan/tg-session-gateway/go/internal/health"
)

// gatewaySchema matches gateway/store.py byte-for-byte semantics so an
// existing gateway.db opens in place.
const gatewaySchema = `
CREATE TABLE IF NOT EXISTS bots (
    alias        TEXT PRIMARY KEY,
    token_enc    BLOB NOT NULL,
    telegram_offset INTEGER NOT NULL DEFAULT 0,
    created_at   TEXT NOT NULL DEFAULT (datetime('now')),
    paused       INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS admin_audit (
    ts       TEXT NOT NULL DEFAULT (datetime('now')),
    action   TEXT NOT NULL,
    alias    TEXT NOT NULL,
    detail   TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS chat_backoff (
    alias        TEXT NOT NULL,
    chat_id      INTEGER NOT NULL,
    paused_until REAL NOT NULL,
    PRIMARY KEY (alias, chat_id)
);
CREATE TABLE IF NOT EXISTS idempotency_keys (
    alias    TEXT NOT NULL,
    key      TEXT NOT NULL,
    state    TEXT NOT NULL DEFAULT 'pending',
    seen_at  REAL NOT NULL,
    PRIMARY KEY (alias, key)
);
`

// Idempotency timing constants (spec E2).
const (
	IdempotencyTTL   = 300.0
	StalePendingS    = 60.0 // a claim older than this died mid-send
)

// Gateway is the Bot API plane store: registered bots, encrypted
// tokens, telegram offsets, the FLOOD_WAIT backoff ledger, send
// idempotency and the admin audit trail.
type Gateway struct {
	DB         *sql.DB
	Path       string
	BackupsDir string
	box        *cryptoutil.Box
}

// OpenGateway opens (or creates) gateway.db in dataDir.
func OpenGateway(dataDir, fernetKey, durability string, backupsDir string, h *health.Storage) (*Gateway, error) {
	_ = os.MkdirAll(dataDir, 0o755)
	box, err := cryptoutil.LoadKey(fernetKey, dataDir)
	if err != nil {
		return nil, err
	}
	path := filepath.Join(dataDir, "gateway.db")
	CheckOrQuarantine(path, backupsDir, h)
	db, err := Open(path, durability, false)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(gatewaySchema); err != nil {
		db.Close()
		return nil, err
	}
	if err := EnsureSchema(db, 1, map[int]string{}, path, backupsDir); err != nil {
		db.Close()
		return nil, err
	}
	return &Gateway{DB: db, Path: path, BackupsDir: backupsDir, box: box}, nil
}

// Close releases the DB handle.
func (g *Gateway) Close() { _ = g.DB.Close() }

// RegisterBot upserts alias → encrypted token.
func (g *Gateway) RegisterBot(alias, token string) error {
	enc, err := g.box.Encrypt([]byte(token))
	if err != nil {
		return err
	}
	_, err = g.DB.Exec(
		"INSERT INTO bots (alias, token_enc) VALUES (?, ?) "+
			"ON CONFLICT(alias) DO UPDATE SET token_enc = excluded.token_enc",
		alias, enc)
	return err
}

// DeleteBot removes a bot; false when the alias was unknown.
func (g *Gateway) DeleteBot(alias string) (bool, error) {
	res, err := g.DB.Exec("DELETE FROM bots WHERE alias = ?", alias)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// SetPaused records the admin pause flag.
func (g *Gateway) SetPaused(alias string, paused bool) error {
	_, err := g.DB.Exec("UPDATE bots SET paused = ? WHERE alias = ?", boolInt(paused), alias)
	return err
}

// BotRow is the list_bots view.
type BotRow struct {
	Alias         string `json:"alias"`
	TelegramOffset int64 `json:"telegram_offset"`
	Paused        bool   `json:"paused"`
}

// ListBots returns every registered alias.
func (g *Gateway) ListBots() ([]BotRow, error) {
	rows, err := g.DB.Query("SELECT alias, telegram_offset, paused FROM bots")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []BotRow
	for rows.Next() {
		var r BotRow
		var paused int
		if err := rows.Scan(&r.Alias, &r.TelegramOffset, &paused); err != nil {
			return nil, err
		}
		r.Paused = paused != 0
		out = append(out, r)
	}
	return out, rows.Err()
}

// GetToken decrypts the stored bot token; ok=false when the alias is
// unknown or the token no longer decrypts (wrong GW_FERNET_KEY).
func (g *Gateway) GetToken(alias string) (string, bool) {
	var tokenEnc []byte
	err := g.DB.QueryRow("SELECT token_enc FROM bots WHERE alias = ?", alias).Scan(&tokenEnc)
	if err != nil {
		return "", false
	}
	plain, ok := g.box.Decrypt(tokenEnc)
	if !ok {
		slog.Error("cannot decrypt token (wrong GW_FERNET_KEY?)", "alias", alias)
		return "", false
	}
	return string(plain), true
}

// VerifyTokens is the boot probe for undecryptable tokens.
func (g *Gateway) VerifyTokens() []string {
	var bad []string
	bots, err := g.ListBots()
	if err != nil {
		return bad
	}
	for _, b := range bots {
		if _, ok := g.GetToken(b.Alias); !ok {
			bad = append(bad, b.Alias)
		}
	}
	return bad
}

// GetOffset reads the persisted Telegram offset (survives restarts).
func (g *Gateway) GetOffset(alias string) int64 {
	var off int64
	err := g.DB.QueryRow("SELECT telegram_offset FROM bots WHERE alias = ?", alias).Scan(&off)
	if err != nil {
		return 0
	}
	return off
}

// SaveOffset persists the Telegram poll offset.
func (g *Gateway) SaveOffset(alias string, offset int64) error {
	_, err := g.DB.Exec("UPDATE bots SET telegram_offset = ? WHERE alias = ?", offset, alias)
	return err
}

// SaveBackoff persists a chat/egress pause so a restart mid-FLOOD_WAIT
// does not resume hammering Telegram. Best-effort, never blocks sends.
func (g *Gateway) SaveBackoff(alias string, chatID *int64, until float64) {
	if chatID == nil {
		zero := int64(0) // bot-wide egress pause: single well-known row
		chatID = &zero
	}
	_, err := g.DB.Exec(
		"INSERT INTO chat_backoff (alias, chat_id, paused_until) VALUES (?, ?, ?) "+
			"ON CONFLICT(alias, chat_id) DO UPDATE SET paused_until = excluded.paused_until",
		alias, *chatID, until)
	if err != nil {
		slog.Warn("save_backoff failed", "alias", alias, "err", err)
	}
}

// BackoffRow is one active persisted pause.
type BackoffRow struct {
	ChatID      int64
	PausedUntil float64
}

// LoadBackoffs returns active (chat_id, paused_until) rows, pruning
// expired ones. Best-effort: errors answer an empty list.
func (g *Gateway) LoadBackoffs(alias string) []BackoffRow {
	now := float64(time.Now().UnixNano()) / 1e9
	_, _ = g.DB.Exec("DELETE FROM chat_backoff WHERE alias = ? AND paused_until <= ?", alias, now)
	rows, err := g.DB.Query("SELECT chat_id, paused_until FROM chat_backoff WHERE alias = ?", alias)
	if err != nil {
		slog.Warn("load_backoffs failed", "alias", alias, "err", err)
		return nil
	}
	defer rows.Close()
	var out []BackoffRow
	for rows.Next() {
		var r BackoffRow
		if err := rows.Scan(&r.ChatID, &r.PausedUntil); err == nil {
			out = append(out, r)
		}
	}
	return out
}

// IdempotencyClaim atomically claims a send key: "claimed"/"reclaimed"
// (proceed) or "replay" (a completed send exists — suppress). A FAILED
// send releases the key; only a completed send suppresses replays.
func (g *Gateway) IdempotencyClaim(alias, key string) string {
	now := float64(time.Now().UnixNano()) / 1e9
	_, _ = g.DB.Exec(
		"DELETE FROM idempotency_keys WHERE alias = ? AND key = ? AND seen_at < ?",
		alias, key, now-IdempotencyTTL)
	res, err := g.DB.Exec(
		"INSERT INTO idempotency_keys (alias, key, state, seen_at) "+
			"VALUES (?, ?, 'pending', ?) ON CONFLICT(alias, key) DO NOTHING",
		alias, key, now)
	if err == nil {
		if n, _ := res.RowsAffected(); n == 1 {
			return "claimed"
		}
	}
	res, err = g.DB.Exec(
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

// IdempotencyFinish marks a completed send (this key now suppresses).
func (g *Gateway) IdempotencyFinish(alias, key string) {
	_, _ = g.DB.Exec(
		"UPDATE idempotency_keys SET state = 'done', seen_at = ? WHERE alias = ? AND key = ?",
		float64(time.Now().UnixNano())/1e9, alias, key)
}

// IdempotencyFail releases the claim so retries pass.
func (g *Gateway) IdempotencyFail(alias, key string) {
	_, _ = g.DB.Exec(
		"DELETE FROM idempotency_keys WHERE alias = ? AND key = ? AND state = 'pending'",
		alias, key)
}

// Audit records an admin action; never raises into the caller's path.
func (g *Gateway) Audit(action, alias, detail string) {
	if detail == "" {
		detail = ""
	}
	_, err := g.DB.Exec("INSERT INTO admin_audit (action, alias, detail) VALUES (?, ?, ?)", action, alias, detail)
	if err != nil {
		slog.Error("audit write failed", "action", action, "alias", alias, "err", err)
	}
}

// AuditEntry is one admin-audit row.
type AuditEntry struct {
	TS     string `json:"ts"`
	Action string `json:"action"`
	Alias  string `json:"alias"`
	Detail string `json:"detail"`
}

// GetAudit returns the newest audit entries.
func (g *Gateway) GetAudit(limit int) []AuditEntry {
	if limit <= 0 {
		limit = 100
	}
	rows, err := g.DB.Query(
		"SELECT ts, action, alias, detail FROM admin_audit ORDER BY rowid DESC LIMIT ?", limit)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []AuditEntry
	for rows.Next() {
		var e AuditEntry
		if err := rows.Scan(&e.TS, &e.Action, &e.Alias, &e.Detail); err == nil {
			out = append(out, e)
		}
	}
	return out
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// TokenSuffix is the logging-safe token identity (alias/suffix only).
func TokenSuffix(token string) string {
	if len(token) >= 4 {
		return "…" + token[len(token)-4:]
	}
	if token == "" {
		return "…"
	}
	return "…" + token
}
