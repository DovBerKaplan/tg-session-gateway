package store

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"
)

func TestMTUserSessionMeta(t *testing.T) {
	m := openTestMT(t, t.TempDir())

	if err := m.RegisterUser("u1", "+972500000099", `{"ipv6":"2001:db8::5"}`); err != nil {
		t.Fatal(err)
	}
	meta, ok := m.GetSessionMeta("u1")
	if !ok || meta.Kind != "user" || meta.Phone != "+972500000099" ||
		meta.ConnOpts != `{"ipv6":"2001:db8::5"}` {
		t.Fatalf("meta=%+v ok=%v", meta, ok)
	}
	// the list view exposes kind + phone
	rows, _ := m.ListSessions()
	if len(rows) != 1 || rows[0].Kind != "user" || rows[0].Phone != "+972500000099" {
		t.Fatalf("rows=%+v", rows)
	}
	// phone_or_token keeps the phone for display
	if rows[0].PhoneOrToken != "+972500000099" {
		t.Fatalf("phone_or_token=%q", rows[0].PhoneOrToken)
	}

	// conn_opts rewrite
	if err := m.SetConnOpts("u1", `{"device":{"model":"Pixel 8 Pro"}}`); err != nil {
		t.Fatal(err)
	}
	meta, _ = m.GetSessionMeta("u1")
	if meta.ConnOpts != `{"device":{"model":"Pixel 8 Pro"}}` {
		t.Fatalf("after SetConnOpts=%q", meta.ConnOpts)
	}

	// a bot register over the same alias keeps kind=user on disk
	// (only token_enc updates)
	if err := m.Register("u1", "720000:tok"); err != nil {
		t.Fatal(err)
	}
	meta, _ = m.GetSessionMeta("u1")
	if meta.Kind != "user" {
		t.Fatalf("kind after bot register=%q (must stay user)", meta.Kind)
	}

	// unknown alias
	if _, ok := m.GetSessionMeta("ghost"); ok {
		t.Fatal("unknown alias must not resolve")
	}
}

func TestMTGotdSessionBlob(t *testing.T) {
	m := openTestMT(t, t.TempDir())
	if m.HasGotdSession("u1") {
		t.Fatal("fresh store has no blob")
	}
	blob := []byte(`{"Version":1,"Data":{"DC":2,"Addr":"10.0.0.1:443"}}`)
	if err := m.SaveGotdSession("u1", blob); err != nil {
		t.Fatal(err)
	}
	if !m.HasGotdSession("u1") {
		t.Fatal("blob not stored")
	}
	// sealed at rest: the raw column must not contain the plaintext JSON
	var raw []byte
	if err := m.DB.QueryRow("SELECT session_blob FROM gotd_sessions WHERE alias = 'u1'").Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if string(raw) == string(blob) {
		t.Fatal("blob stored unsealed")
	}
	// overwrite
	if err := m.SaveGotdSession("u1", []byte(`{"Version":1,"Data":{"DC":4}}`)); err != nil {
		t.Fatal(err)
	}
}

// TestMTMigratesV1DB builds a pre-user-sessions database (user_version=1,
// sessions without kind/phone/conn_opts) and expects OpenMT to migrate
// it in place with the data intact.
func TestMTMigratesV1DB(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mtgateway.db")
	old, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := old.Exec(`CREATE TABLE sessions (
		alias       TEXT PRIMARY KEY,
		token_enc   BLOB NOT NULL,
		phone_or_token TEXT NOT NULL DEFAULT '',
		pts         INTEGER NOT NULL DEFAULT 0,
		qts         INTEGER NOT NULL DEFAULT 0,
		date        INTEGER NOT NULL DEFAULT 0,
		seq         INTEGER NOT NULL DEFAULT 0,
		created_at  TEXT NOT NULL DEFAULT (datetime('now')),
		paused      INTEGER NOT NULL DEFAULT 0);
	INSERT INTO sessions (alias, token_enc, phone_or_token) VALUES ('legacy', X'00', '720000:old');
	PRAGMA user_version = 1;`); err != nil {
		t.Fatal(err)
	}
	_ = old.Close()

	m, err := OpenMT(dir, "CfR-eL8QzhZJQb22PBP1zxH7CzJMFK4LwKvFACh1pfw=", "normal",
		filepath.Join(dir, "backups"), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)

	var version int
	if err := m.DB.QueryRow("PRAGMA user_version").Scan(&version); err != nil || version != 3 {
		t.Fatalf("user_version=%d err=%v (want 3)", version, err)
	}
	rows, err := m.ListSessions()
	if err != nil || len(rows) != 1 || rows[0].Alias != "legacy" || rows[0].Kind != "bot" {
		t.Fatalf("rows=%+v err=%v — legacy row must survive as kind=bot", rows, err)
	}
	// clients table exists after migration
	if err := m.RegisterClient("cl-1", "x", []byte{1}, []string{"full"}, []string{"legacy"}, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
}
