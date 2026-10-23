package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/DovBerKaplan/tg-session-gateway/internal/config"
	"github.com/DovBerKaplan/tg-session-gateway/internal/migrate"
)

// runMigrate converts pyrofork .session files into gotd session blobs
// inside mtgateway.db — the cutover step. Exit code 0 with
// logins_required=0 is the "no Telegram logins needed" gate.
//
// Usage: tggw migrate [--from <data_dir>] [--dry-run] [--json]
func runMigrate(args []string) error {
	fs := flag.NewFlagSet("migrate", flag.ContinueOnError)
	from := fs.String("from", "", "data dir holding sessions/ and mtgateway.db (default: $GW_DATA_DIR)")
	dryRun := fs.Bool("dry-run", false, "report what would happen; write nothing")
	asJSON := fs.Bool("json", false, "machine-readable report")
	if err := fs.Parse(args); err != nil {
		return err
	}

	dataDir := *from
	if dataDir == "" {
		dataDir = config.MTProtoFromEnv().DataDir
	}
	if dataDir == "" {
		return fmt.Errorf("--from or GW_DATA_DIR is required")
	}

	rep, err := migrate.Run(dataDir, *dryRun)
	if err != nil {
		return err
	}

	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(rep)
	}
	for _, s := range rep.Sessions {
		line := fmt.Sprintf("  %-20s dc=%d user=%d bot=%v key=%s → %s",
			s.Alias, s.DC, s.UserID, s.IsBot, s.AuthKeyID, s.Action)
		if s.Err != "" {
			line += " (" + s.Err + ")"
		}
		fmt.Println(line)
	}
	fmt.Printf("migrated=%d skipped=%d errors=%d needs_new_login=%d\n",
		rep.Migrated, rep.Skipped, rep.Errors, rep.NeedsNewLogin)
	fmt.Printf("Telegram logins required: %d\n", rep.LoginsRequired)
	if *dryRun {
		fmt.Println("(dry-run: nothing was written)")
	}
	if rep.LoginsRequired > 0 {
		fmt.Println("WARNING: some sessions have no migrated auth key — they would login from scratch.")
	}
	return nil
}
