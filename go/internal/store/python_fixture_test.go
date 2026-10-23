package store

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

func pythonBin() string {
	for _, cand := range []string{
		"/home/dovber/tg-session-gateway/.venv/bin/python",
		"python3",
	} {
		if p, err := exec.LookPath(cand); err == nil {
			return p
		}
	}
	return ""
}

func pythonAvailable() bool { return pythonBin() != "" }

// runPython builds fixtures with the REAL Python implementation
// (aiosqlite + cryptography.fernet) so the Go store proves it opens
// Python-created database files in place.
func runPython(dir, kind string) error {
	py := pythonBin()
	if py == "" {
		return fmt.Errorf("no python")
	}
	key := "CfR-eL8QzhZJQb22PBP1zxH7CzJMFK4LwKvFACh1pfw="
	var script string
	switch kind {
	case "gateway":
		script = fmt.Sprintf(`
import asyncio, aiosqlite, sys
from cryptography.fernet import Fernet

async def main():
    db = await aiosqlite.connect(%q)
    await db.executescript("""
    CREATE TABLE IF NOT EXISTS bots (
        alias        TEXT PRIMARY KEY,
        token_enc    BLOB NOT NULL,
        telegram_offset INTEGER NOT NULL DEFAULT 0,
        created_at   TEXT NOT NULL DEFAULT (datetime('now')),
        paused       INTEGER NOT NULL DEFAULT 0
    );""")
    enc = Fernet(%q.encode()).encrypt(b"720000:python-created")
    await db.execute("INSERT INTO bots (alias, token_enc, telegram_offset) VALUES ('pybot', ?, 5001)", (enc,))
    await db.commit()
    await db.close()

asyncio.run(main())
`, filepath.Join(dir, "gateway.db"), key)
	case "queue":
		script = fmt.Sprintf(`
import asyncio, aiosqlite, json, time

async def main():
    db = await aiosqlite.connect(%q)
    await db.executescript("""
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
    );""")
    for uid, text in ((9001, "from python"), (9002, "also python")):
        u = {"update_id": uid, "message": {"text": text}}
        await db.execute(
            "INSERT INTO update_queue (bot_alias, update_id, update_json, enqueued_at) VALUES ('pybot', ?, ?, ?)",
            (uid, json.dumps(u), time.time()))
    await db.commit()
    await db.close()

asyncio.run(main())
`, filepath.Join(dir, "queue.db"))
	default:
		return fmt.Errorf("unknown fixture kind %q", kind)
	}
	cmd := exec.Command(py, "-c", script)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "PYTHONHASHSEED=0")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("python: %v\n%s", err, out)
	}
	return nil
}
