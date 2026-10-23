// Package store holds all SQLite persistence for both planes. It opens
// the exact schema the Python implementation created — gateway.db,
// queue.db and mtgateway.db migrate in place, no data export.
package store

import (
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	_ "modernc.org/sqlite" // pure-Go driver: CGO-free static binary

	"github.com/DovBerKaplan/tg-session-gateway/internal/health"
)

// PragmaStatements returns the durability PRAGMAs every SQLite file in
// this project gets: WAL, busy_timeout (a locked writer waits instead
// of failing instantly), synchronous NORMAL (survives process death;
// FULL additionally survives machine power loss) and bounded WAL.
func PragmaStatements(durability string, foreignKeys bool) []string {
	sync := "NORMAL"
	if strings.EqualFold(durability, "FULL") {
		sync = "FULL"
	}
	stmts := []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=" + sync,
		"PRAGMA busy_timeout=5000",
		"PRAGMA wal_autocheckpoint=1000",
	}
	if foreignKeys {
		stmts = append(stmts, "PRAGMA foreign_keys=ON")
	}
	return stmts
}

// Open SQLite with the durability pragmas applied. A single pooled
// connection mirrors the Python single-connection behavior (writes are
// tiny; serializing them sidesteps SQLITE_BUSY entirely).
func Open(path, durability string, foreignKeys bool) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	for _, stmt := range PragmaStatements(durability, foreignKeys) {
		if _, err := db.Exec(stmt); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("%s: %w", stmt, err)
		}
	}
	return db, nil
}

// ── boot integrity ─────────────────────────────────────────────────

// QuickCheck runs PRAGMA quick_check on a throwaway read-only reader.
func QuickCheck(path string) (bool, string) {
	db, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
	if err != nil {
		return false, err.Error()
	}
	defer func() { _ = db.Close() }()
	var result string
	row := db.QueryRow("PRAGMA quick_check")
	if err := row.Scan(&result); err != nil {
		return false, err.Error()
	}
	return result == "ok", result
}

// CheckOrQuarantine is the boot gate for one SQLite file: healthy →
// unchanged; corrupt → renamed to <path>.corrupt-<ts> and the newest
// valid backup (if any) restored in place. Either way the process
// LIVES — crash-looping a supervisor over a corrupt file helps nobody.
//
// The -wal and -shm sidecars move ASIDE with the main file: leaving
// them behind would let a hot WAL apply corrupt pages onto the
// restored backup.
func CheckOrQuarantine(path string, backupsDir string, h *health.Storage) {
	if _, err := os.Stat(path); err != nil {
		return
	}
	ok, detail := QuickCheck(path)
	if ok {
		return
	}
	ts := time.Now().Format("20060102-150405")
	quarantine := fmt.Sprintf("%s.corrupt-%s", path, ts)
	if err := os.Rename(path, quarantine); err != nil {
		slog.Error("cannot quarantine corrupt DB", "path", path, "err", err)
		return
	}
	for _, side := range []string{path + "-wal", path + "-shm"} {
		if _, err := os.Stat(side); err == nil {
			_ = os.Rename(side, side+".corrupt-"+ts)
		}
	}
	slog.Error("DB corrupt — quarantined", "path", path, "detail", detail, "to", quarantine)
	if h != nil {
		h.NoteQuarantine(path)
	}
	if restoreNewestBackup(path, backupsDir) {
		slog.Warn("restored from backup (data since last backup is lost)", "path", path)
	}
}

func restoreNewestBackup(path, backupsDir string) bool {
	if backupsDir == "" {
		return false
	}
	base := filepath.Base(path)
	matches, _ := filepath.Glob(filepath.Join(backupsDir, base+"-*.db"))
	sort.Sort(sort.Reverse(sort.StringSlice(matches)))
	for _, cand := range matches {
		if ok, _ := QuickCheck(cand); ok {
			data, err := os.ReadFile(cand)
			if err != nil {
				continue
			}
			return os.WriteFile(path, data, 0o644) == nil
		}
		slog.Warn("backup failed quick_check — skipping", "backup", cand)
	}
	return false
}

// ── schema versioning ──────────────────────────────────────────────

// EnsureSchema is the tiny PRAGMA user_version migration runner with
// the same adoption rules as the Python one: version 0 on an EXISTING
// db = pre-versioning install → adopt as current; version < current →
// snapshot first, then apply each missing migration in order.
func EnsureSchema(db *sql.DB, currentVersion int, migrations map[int]string, dbPath, backupsDir string) error {
	var have int
	if err := db.QueryRow("PRAGMA user_version").Scan(&have); err != nil {
		return err
	}
	if have == 0 {
		if _, err := os.Stat(dbPath); err == nil {
			_, err := db.Exec(fmt.Sprintf("PRAGMA user_version = %d", currentVersion))
			return err
		}
	} else if have >= currentVersion {
		return nil
	}
	if backupsDir != "" {
		if _, err := os.Stat(dbPath); err == nil {
			BackupDB(dbPath, backupsDir, 5) // snapshot BEFORE migrating
		}
	}
	for _, version := range sortedKeys(migrations) {
		if have < version && version <= currentVersion {
			slog.Info("applying schema migration", "version", version, "db", dbPath)
			// each migration commits atomically WITH its version bump:
			// a failure mid-way leaves the db at the last good version,
			// never at a half-applied schema that fails the next boot
			// with "duplicate column"
			if err := applyMigration(db, version, migrations[version]); err != nil {
				return err
			}
			have = version
		}
	}
	if have < currentVersion {
		_, err := db.Exec(fmt.Sprintf("PRAGMA user_version = %d", currentVersion))
		return err
	}
	return nil
}

// applyMigration runs one migration SQL plus its user_version bump in
// a single transaction.
func applyMigration(db *sql.DB, version int, script string) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(script); err != nil {
		return err
	}
	// PRAGMA inside a transaction: user_version is transactional in
	// SQLite, so the bump rides the same commit
	if _, err := tx.Exec(fmt.Sprintf("PRAGMA user_version = %d", version)); err != nil {
		return err
	}
	return tx.Commit()
}

func sortedKeys(m map[int]string) []int {
	keys := make([]int, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	return keys
}

// ── backups ────────────────────────────────────────────────────────

// BackupDB takes a consistent snapshot via VACUUM INTO, rotated to the
// newest `keep`. Empty string on failure.
func BackupDB(path, backupsDir string, keep int) string {
	if _, err := os.Stat(path); err != nil {
		return ""
	}
	_ = os.MkdirAll(backupsDir, 0o755)
	base := filepath.Base(path)
	ts := time.Now().Format("20060102-150405")
	dest := filepath.Join(backupsDir, fmt.Sprintf("%s-%s-%d-%d.db", base, ts, os.Getpid(), time.Now().UnixNano()/1e6%1000))
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return ""
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec("VACUUM INTO ?", dest); err != nil {
		// parameter binding unsupported for VACUUM targets — escape manually
		literal := "'" + strings.ReplaceAll(dest, "'", "''") + "'"
		if _, err2 := db.Exec("VACUUM INTO " + literal); err2 != nil {
			slog.Error("backup failed", "path", path, "err", err2)
			_ = os.Remove(dest)
			return ""
		}
	}
	rotate(filepath.Join(backupsDir, base+"-*.db"), keep)
	return dest
}

func rotate(pattern string, keep int) {
	files, _ := filepath.Glob(pattern)
	sort.Sort(sort.Reverse(sort.StringSlice(files)))
	for _, stale := range files[keep:] {
		_ = os.Remove(stale)
	}
}

// ── periodic maintenance ───────────────────────────────────────────

// MaintenancePass runs one maintenance sweep: integrity_check on every
// DB, WAL checkpoint(TRUNCATE), optional audit retention SQL and
// rotating backups at most once per backupInterval.
func MaintenancePass(dbPaths []string, backupsDir string, h *health.Storage, keep int, backupInterval float64, auditRetentionSQL string, auditRetentionDays int) {
	now := time.Now()
	for _, path := range dbPaths {
		if _, err := os.Stat(path); err != nil {
			continue
		}
		db, err := sql.Open("sqlite", path)
		if err != nil {
			continue
		}
		var result string
		if err := db.QueryRow("PRAGMA integrity_check").Scan(&result); err == nil && result != "ok" {
			slog.Error("integrity_check FAILED", "path", path, "result", result)
			h.NotePersistFailure("integrity:"+filepath.Base(path), fmt.Errorf("%s", result))
		}
		_, _ = db.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
		if auditRetentionSQL != "" {
			cutoff := now.Add(-time.Duration(auditRetentionDays) * 24 * time.Hour)
			if _, err := db.Exec(auditRetentionSQL, cutoff.Unix()); err != nil {
				h.NotePersistFailure("maintenance:"+filepath.Base(path), err)
			}
		}
		_ = db.Close()
	}
	if now.Sub(lastBackupAt(backupsDir)).Seconds() >= backupInterval {
		for _, path := range dbPaths {
			BackupDB(path, backupsDir, keep)
		}
	}
	h.NoteMaintenanceDone(now)
}

func lastBackupAt(backupsDir string) time.Time {
	var latest time.Time
	patterns := []string{filepath.Join(backupsDir, "*-*.db"), filepath.Join(backupsDir, "*.zip.enc")}
	for _, p := range patterns {
		files, _ := filepath.Glob(p)
		for _, f := range files {
			if st, err := os.Stat(f); err == nil && st.ModTime().After(latest) {
				latest = st.ModTime()
			}
		}
	}
	return latest
}
