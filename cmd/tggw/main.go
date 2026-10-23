// Command tggw is the single static binary of the Telegram session
// gateway: `tggw botapi` (Bot API gateway), `tggw mtproto` (MTProto
// sidecar), `tggw genkey` (Fernet key) and `tggw migrate` (pyrofork →
// gotd session migration).
package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/DovBerKaplan/tg-session-gateway/internal/botapi"
	"github.com/DovBerKaplan/tg-session-gateway/internal/config"
	"github.com/DovBerKaplan/tg-session-gateway/internal/cryptoutil"
	"github.com/DovBerKaplan/tg-session-gateway/internal/version"
)

const genkeyHint = `Generate one outside the container:
  docker run --rm <image> genkey
or: openssl rand -base64 32 | tr '+/' '-_'`

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd := os.Args[1]
	var err error
	switch cmd {
	case "botapi":
		err = runBotAPI()
	case "mtproto":
		err = runMTProto()
	case "genkey":
		var key string
		key, err = cryptoutil.GenerateKey()
		if err == nil {
			fmt.Println(key)
		}
	case "migrate":
		err = runMigrate(os.Args[2:])
	case "healthcheck":
		err = runHealthcheck(os.Args[2:])
	case "terminate":
		err = runTerminate()
	case "version":
		fmt.Println(version.Version)
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "tggw:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: tggw <command>

commands:
  botapi     Bot API gateway (proxy, poller, update queue, rate guard)
  mtproto    MTProto session sidecar
  genkey     print a fresh GW_FERNET_KEY and exit
  migrate    pyrofork session file → gotd session (no re-login)
  healthcheck GET /healthz on localhost (exit 0 = alive; container
             healthcheck for shell-less distroless images)
  terminate  SIGTERM to PID 1 — the K8s preStop drain hook (starts
             flush offsets + release locks before the runtime's signal)
  version    print the version`)
}

// preflight enforces the Postgres-style hard requirements: secrets and
// the data volume exist OUTSIDE the container layer, or the gateway
// refuses to start instead of inventing identity that dies on restart.
// Long operator guidance goes straight to stderr; the returned error
// stays a short single line.
func preflight(fernetKey, dataDir string) error {
	if fernetKey == "" {
		fmt.Fprintf(os.Stderr,
			"GW_FERNET_KEY encrypts the stored bot tokens.\n%s\n"+
				"The gateway refuses to generate secrets inside the container:\n"+
				"a key written to the container layer is lost on restart and\n"+
				"every stored token becomes undecryptable.\n", genkeyHint)
		return errors.New("GW_FERNET_KEY is required")
	}
	if os.Getenv("GW_ALLOW_EPHEMERAL_DATA") != "1" && !isMount(dataDir) {
		fmt.Fprintf(os.Stderr,
			"%s must be a mounted volume (-v <name>:%s).\n"+
				"Sessions, offsets, the update queue and the audit log live there;\n"+
				"container-local storage silently loses them on restart.\n"+
				"Set GW_ALLOW_EPHEMERAL_DATA=1 to run anyway (throwaway demos only).\n",
			dataDir, dataDir)
		return fmt.Errorf("%s must be a mounted volume", dataDir)
	}
	return nil
}

// isMount reports whether the path sits on a mounted filesystem (a
// container volume or a real mount — NOT the container layer): walk up
// to the deepest EXISTING ancestor and check if IT is a mountpoint.
func isMount(path string) bool {
	abs, err := filepath.Abs(path)
	if err != nil {
		return false
	}
	p := abs
	for {
		if _, err := os.Stat(p); err == nil {
			return isRootMount(p)
		}
		parent := filepath.Dir(p)
		if parent == p {
			return false
		}
		p = parent
	}
}

func runBotAPI() error {
	cfg := config.BotAPIFromEnv()
	if err := cfg.Validate(); err != nil {
		return err
	}
	if err := preflight(cfg.FernetKey, cfg.DataDir); err != nil {
		return err
	}
	app, err := botapi.NewApp(cfg, botapi.NewUpstreamClient(cfg.Upstream))
	if err != nil {
		return err
	}
	return app.RunServe()
}
