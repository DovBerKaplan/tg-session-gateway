package migrate

// Migration tests: a pyrofork-shaped .session fixture (the exact
// DDL pyrogram's sqlite storage creates) converts into a gotd session
// blob that gotd's own session.Loader parses — proving the cutover
// needs zero Telegram logins.

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/gotd/td/session"

	_ "modernc.org/sqlite"

	"github.com/DovBerKaplan/tg-session-gateway/internal/cryptoutil"
)

// testKey is the fixed fernet key every migrate test runs with —
// identical to the conformance suite's, so sealed-blob assertions are
// reproducible across runs.
const testKey = "CfR-eL8QzhZJQb22PBP1zxH7CzJMFK4LwKvFACh1pfw="

const pyroforkSchema = `
CREATE TABLE sessions
(
    dc_id     INTEGER PRIMARY KEY,
    api_id    INTEGER,
    test_mode INTEGER,
    auth_key  BLOB,
    date      INTEGER NOT NULL,
    user_id   INTEGER,
    is_bot    INTEGER
);
CREATE TABLE peers
(
    id             INTEGER PRIMARY KEY,
    access_hash    INTEGER,
    type           INTEGER NOT NULL,
    username       TEXT,
    phone_number   TEXT,
    last_update_on INTEGER NOT NULL DEFAULT (CAST(STRFTIME('%s', 'now') AS INTEGER))
);
CREATE TABLE update_state
(
    id   INTEGER PRIMARY KEY,
    pts  INTEGER,
    qts  INTEGER,
    date INTEGER,
    seq  INTEGER
);
CREATE TABLE version (number INTEGER PRIMARY KEY);
`

// writePyroforkSession creates a real pyrofork-layout .session file.
func writePyroforkSession(t *testing.T, path string, dcID int, authKey []byte, userID int64) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec(pyroforkSchema); err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(
		"INSERT INTO sessions (dc_id, api_id, test_mode, auth_key, date, user_id, is_bot) "+
			"VALUES (?, ?, 0, ?, 1700000000, ?, 1)",
		dcID, 12345, authKey, userID)
	if err != nil {
		t.Fatal(err)
	}
}

func fakeAuthKey() []byte {
	// deterministic 256-byte key (any bytes work — the key is opaque)
	key := make([]byte, 256)
	for i := range key {
		key[i] = byte(i * 7)
	}
	return key
}

func TestMigrateConvertsAndIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	writePyroforkSession(t, filepath.Join(dir, "sessions", "bot1.session"), 2, fakeAuthKey(), 42)

	// dry-run reports but writes nothing
	rep, err := Run(dir, testKey, true)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Migrated != 1 || rep.Sessions[0].Action != "would migrate" {
		t.Fatalf("dry-run report = %+v", rep)
	}
	db, _ := sql.Open("sqlite", filepath.Join(dir, "mtgateway.db"))
	var n int
	_ = db.QueryRow("SELECT COUNT(*) FROM gotd_sessions").Scan(&n)
	_ = db.Close()
	if n != 0 {
		t.Fatalf("dry-run wrote %d rows", n)
	}

	// real run
	rep, err = Run(dir, testKey, false)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Migrated != 1 || rep.Errors != 0 || rep.LoginsRequired != 0 {
		t.Fatalf("run report = %+v", rep)
	}
	sr := rep.Sessions[0]
	if sr.DC != 2 || sr.UserID != 42 || !sr.IsBot {
		t.Fatalf("session report = %+v", sr)
	}

	// the blob parses through gotd's own Loader — the auth survives.
	// The stored blob must be SEALED (the at-rest invariant); open it
	// the way the production session storage does before loading.
	db, err = sql.Open("sqlite", filepath.Join(dir, "mtgateway.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var blob []byte
	if err := db.QueryRow("SELECT session_blob FROM gotd_sessions WHERE alias = 'bot1'").Scan(&blob); err != nil {
		t.Fatal(err)
	}
	box, err := cryptoutil.LoadKey(testKey, dir)
	if err != nil {
		t.Fatal(err)
	}
	if !cryptoutil.IsSealed(blob) {
		t.Fatal("migrated gotd blob stored as plaintext — must be sealed at rest")
	}
	plain, err := box.OpenBlob(blob)
	if err != nil {
		t.Fatal(err)
	}
	mem := &session.StorageMemory{}
	if err := mem.StoreSession(context.Background(), plain); err != nil {
		t.Fatal(err)
	}
	data, err := (&session.Loader{Storage: mem}).Load(context.Background())
	if err != nil {
		t.Fatalf("gotd loader rejects migrated blob: %v", err)
	}
	if data.DC != 2 {
		t.Fatalf("DC = %d", data.DC)
	}
	want := fakeAuthKey()
	if len(data.AuthKey) != 256 {
		t.Fatalf("auth key length = %d", len(data.AuthKey))
	}
	for i := 0; i < 256; i++ {
		if data.AuthKey[i] != want[i] {
			t.Fatal("auth key corrupted in migration")
		}
	}
	// AuthKeyID must be the gotd-computed fingerprint (restoreConnection
	// validates key.Value.ID() == key.ID; mismatch = "corrupted key")
	if len(data.AuthKeyID) != 8 {
		t.Fatalf("auth key id length = %d", len(data.AuthKeyID))
	}

	// second run is a no-op (skipped, still zero logins)
	rep, err = Run(dir, testKey, false)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Skipped != 1 || rep.Migrated != 0 || rep.LoginsRequired != 0 {
		t.Fatalf("idempotency broken: %+v", rep)
	}
}

func TestMigrateReportsMissingSessionsAsNewLogins(t *testing.T) {
	dir := t.TempDir()

	// an alias registered in mtgateway.db but NO .session file
	db, err := sql.Open("sqlite", filepath.Join(dir, "mtgateway.db"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		"CREATE TABLE sessions (alias TEXT PRIMARY KEY, token_enc BLOB NOT NULL, " +
			"phone_or_token TEXT NOT NULL DEFAULT '', pts INTEGER NOT NULL DEFAULT 0, " +
			"qts INTEGER NOT NULL DEFAULT 0, date INTEGER NOT NULL DEFAULT 0, " +
			"seq INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL DEFAULT (datetime('now')), " +
			"paused INTEGER NOT NULL DEFAULT 0)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO sessions (alias, token_enc) VALUES ('orphan', X'00')"); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	rep, err := Run(dir, testKey, false)
	if err != nil {
		t.Fatal(err)
	}
	if rep.NeedsNewLogin != 1 || rep.LoginsRequired != 1 {
		t.Fatalf("orphan not reported: %+v", rep)
	}
}

func TestMigrateRejectsCorruptAuthKey(t *testing.T) {
	dir := t.TempDir()
	writePyroforkSession(t, filepath.Join(dir, "sessions", "bad.session"), 2, []byte{1, 2, 3}, 1)

	rep, err := Run(dir, testKey, false)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Errors != 1 || rep.Sessions[0].Action != "error" {
		t.Fatalf("corrupt key not rejected: %+v", rep)
	}
	if rep.Sessions[0].Err == "" {
		t.Fatal("error report lacks a reason")
	}
}

func TestReportJSONShape(t *testing.T) {
	dir := t.TempDir()
	writePyroforkSession(t, filepath.Join(dir, "sessions", "b.session"), 1, fakeAuthKey(), 7)
	rep, err := Run(dir, testKey, false)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(rep)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`"logins_required":0`, `"migrated":1`, `"auth_key_id"`,
	} {
		if !contains(string(raw), want) {
			t.Fatalf("report JSON missing %q: %s", want, raw)
		}
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
