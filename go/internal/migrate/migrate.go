// Package migrate converts pyrofork (Pyrogram fork) .session SQLite
// files into gotd session blobs inside mtgateway.db — the cutover step
// that lets the Go sidecar open every session WITHOUT a new Telegram
// login (AUTH_FLOOD risk). pts/qts/date/seq need no conversion: they
// already live in mtgateway.db's sessions table, which the Go store
// opens in place.
package migrate

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/gotd/td/crypto"
	"github.com/gotd/td/session"

	_ "modernc.org/sqlite"
)

// SessionReport is one .session file's outcome.
type SessionReport struct {
	Alias     string `json:"alias"`
	DC        int    `json:"dc"`
	UserID    int64  `json:"user_id"`
	IsBot     bool   `json:"is_bot"`
	TestMode  bool   `json:"test_mode"`
	AuthKeyID string `json:"auth_key_id"` // key fingerprint (hex) — never the key itself
	Action    string `json:"action"`      // migrated | skipped (already migrated) | error
	Err       string `json:"err,omitempty"`
}

// Report summarizes a migration run. LoginsRequired must be 0 for a
// clean cutover ("Telegram logins performed: 0").
type Report struct {
	Sessions       []SessionReport `json:"sessions"`
	Migrated       int             `json:"migrated"`
	Skipped        int             `json:"skipped"`
	Errors         int             `json:"errors"`
	NeedsNewLogin  int             `json:"needs_new_login"` // registered aliases with no usable .session
	LoginsRequired int             `json:"logins_required"`
}

// pyroforkSession is one row of the pyrogram `sessions` table.
type pyroforkSession struct {
	DCID     int
	APIID    int
	TestMode bool
	AuthKey  []byte
	UserID   int64
	IsBot    bool
}

// gotdSessionEnvelope mirrors gotd's private jsonData wire format
// ({"Version":1,"Data":{...}}) so Loader.Load parses our blob.
type gotdSessionEnvelope struct {
	Version int
	Data    session.Data
}

// readPyroforkSession opens a .session file and extracts the auth row.
func readPyroforkSession(path string) (*pyroforkSession, error) {
	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("session file: %w", err)
	}
	db, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
	if err != nil {
		return nil, err
	}
	defer db.Close()
	row := db.QueryRow(
		"SELECT dc_id, api_id, test_mode, auth_key, user_id, is_bot FROM sessions LIMIT 1")
	var s pyroforkSession
	var testMode, isBot int
	var authKey []byte
	var userID sql.NullInt64
	if err := row.Scan(&s.DCID, &s.APIID, &testMode, &authKey, &userID, &isBot); err != nil {
		return nil, fmt.Errorf("read sessions row: %w", err)
	}
	s.TestMode = testMode != 0
	s.IsBot = isBot != 0
	s.UserID = userID.Int64
	if len(authKey) != 256 {
		return nil, fmt.Errorf("auth_key has %d bytes, want 256", len(authKey))
	}
	s.AuthKey = authKey
	return &s, nil
}

// buildGotdBlob converts a pyrofork auth row into the gotd session
// blob. AuthKeyID comes from gotd's own crypto.Key.WithID so
// restoreConnection's fingerprint check (key.Value.ID() == key.ID)
// passes by construction.
func buildGotdBlob(s *pyroforkSession) ([]byte, string, error) {
	var key crypto.Key
	copy(key[:], s.AuthKey)
	id := key.WithID()

	data := session.Data{
		DC:        s.DCID,
		AuthKey:   key[:],
		AuthKeyID: id.ID[:],
		Config:    session.Config{TestMode: s.TestMode},
	}
	blob, err := json.Marshal(gotdSessionEnvelope{Version: 1, Data: data})
	if err != nil {
		return nil, "", err
	}
	return blob, fmt.Sprintf("%x", id.ID[:]), nil
}

// Run scans dataDir/sessions/*.session and writes gotd session blobs
// into mtgateway.db's gotd_sessions table. Idempotent: an identical
// existing blob is skipped, a changed one is overwritten (key rotation
// means the .session file is newer). With dryRun nothing is written.
func Run(dataDir string, dryRun bool) (*Report, error) {
	sessionDir := filepath.Join(dataDir, "sessions")
	entries, err := filepath.Glob(filepath.Join(sessionDir, "*.session"))
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("sqlite", filepath.Join(dataDir, "mtgateway.db"))
	if err != nil {
		return nil, err
	}
	defer db.Close()
	if _, err := db.Exec(
		"CREATE TABLE IF NOT EXISTS gotd_sessions (alias TEXT PRIMARY KEY, session_blob BLOB NOT NULL)"); err != nil {
		return nil, err
	}

	rep := &Report{Sessions: []SessionReport{}}
	byFile := map[string]bool{}

	for _, path := range entries {
		alias := strings.TrimSuffix(filepath.Base(path), ".session")
		r := SessionReport{Alias: alias}

		ps, err := readPyroforkSession(path)
		if err != nil {
			r.Action, r.Err = "error", err.Error()
			rep.Errors++
			rep.Sessions = append(rep.Sessions, r)
			continue
		}
		r.DC, r.UserID, r.IsBot, r.TestMode = ps.DCID, ps.UserID, ps.IsBot, ps.TestMode

		blob, keyID, err := buildGotdBlob(ps)
		if err != nil {
			r.Action, r.Err = "error", err.Error()
			rep.Errors++
			rep.Sessions = append(rep.Sessions, r)
			continue
		}
		r.AuthKeyID = keyID
		byFile[alias] = true

		var existing []byte
		err = db.QueryRow("SELECT session_blob FROM gotd_sessions WHERE alias = ?", alias).Scan(&existing)
		if err == nil && string(existing) == string(blob) {
			r.Action = "skipped (already migrated)"
			rep.Skipped++
			rep.Sessions = append(rep.Sessions, r)
			continue
		}

		if dryRun {
			r.Action = "would migrate"
		} else {
			if _, err := db.Exec(
				"INSERT INTO gotd_sessions (alias, session_blob) VALUES (?, ?) "+
					"ON CONFLICT(alias) DO UPDATE SET session_blob = excluded.session_blob",
				alias, blob); err != nil {
				r.Action, r.Err = "error", err.Error()
				rep.Errors++
				rep.Sessions = append(rep.Sessions, r)
				continue
			}
			r.Action = "migrated"
		}
		rep.Migrated++
		rep.Sessions = append(rep.Sessions, r)
	}

	// registered aliases with no migrated key would log in from scratch
	// on first connect — the cutover gate wants zero of those
	if rows, err := db.Query("SELECT alias FROM sessions"); err == nil {
		for rows.Next() {
			var alias string
			if rows.Scan(&alias) == nil && !byFile[alias] {
				rep.NeedsNewLogin++
			}
		}
		rows.Close()
	}
	rep.LoginsRequired = rep.Errors + rep.NeedsNewLogin
	return rep, nil
}

// Context satisfies linters that expect a context-aware entrypoint.
func RunContext(ctx context.Context, dataDir string, dryRun bool) (*Report, error) {
	_ = ctx
	return Run(dataDir, dryRun)
}
