package botapi

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"io"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/DovBerKaplan/tg-session-gateway/internal/config"
	"github.com/DovBerKaplan/tg-session-gateway/internal/health"
	"github.com/DovBerKaplan/tg-session-gateway/internal/store"
	"github.com/DovBerKaplan/tg-session-gateway/internal/tracing"
	"github.com/DovBerKaplan/tg-session-gateway/internal/version"
)

// App is the whole Bot API gateway: store, sessions, proxy, admin,
// health and metrics wired together with graceful shutdown.
type App struct {
	Cfg       config.BotAPI
	Store     *store.Gateway
	Mgr       *Manager
	Proxy     *Proxy
	Health    *health.Storage
	Beat      *health.Beat
	Boot      *health.BootTracker
	SafeMode  atomic.Bool
	maintStop chan struct{}
	closeOnce sync.Once
	srv       *http.Server
	// adminSrv is non-nil when GW_ADMIN_PORT splits the operator surface
	// onto its own (unexposed) listener.
	adminSrv *http.Server
	// Supervisor owns failover / env-detect (nil in unit tests with fakes).
	Supervisor *UpstreamSupervisor
}

// NewApp wires everything. The HTTPClient is injectable so tests fake
// api.telegram.org; production passes NewUpstreamClient(cfg.Upstream)
// or an UpstreamSupervisor.
func NewApp(cfg config.BotAPI, client HTTPClient) (*App, error) {
	h := health.NewStorage()
	st, err := store.OpenGateway(cfg.DataDir, cfg.FernetKey, cfg.Storage.Durability, cfg.BackupsDir(), h)
	if err != nil {
		return nil, err
	}
	cfg.PathPrefix = config.NormalizePathPrefix(cfg.PathPrefix)
	mgr := NewManager(cfg, st, client, h)
	proxy := NewProxy(mgr, client, cfg.Policy.QueueWaitTimeout)
	proxy.SetPathPrefix(cfg.PathPrefix)
	proxy.SetLocalKind(cfg.EffectiveKind() == "local")
	app := &App{
		Cfg: cfg, Store: st, Mgr: mgr, Health: h,
		Proxy:     proxy,
		Boot:      &health.BootTracker{Path: cfg.DataDir + "/boot_history.json"},
		maintStop: make(chan struct{}),
	}
	if sup, ok := client.(*UpstreamSupervisor); ok {
		app.Supervisor = sup
		sup.StartLoop()
	}
	app.Beat = health.NewBeat()
	app.buildLifecycle()

	mux := http.NewServeMux()
	if cfg.AdminPort > 0 {
		app.publicRoutes(mux)
		adminMux := http.NewServeMux()
		app.adminRoutes(adminMux)
		app.adminSrv = &http.Server{
			Addr:              fmt.Sprintf("%s:%d", cfg.Host, cfg.AdminPort),
			Handler:           adminMux,
			ReadHeaderTimeout: 10 * time.Second, // slowloris guard
			IdleTimeout:       120 * time.Second,
		}
	} else {
		app.routes(mux)
	}
	app.srv = &http.Server{
		Addr:              fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second, // slowloris guard
		IdleTimeout:       120 * time.Second,
	}
	return app, nil
}

// buildLifecycle runs the boot sequence (safe-mode check, restore,
// maintenance loop) — split from NewApp so tests can inspect state.
func (a *App) buildLifecycle() {
	go a.maintenanceLoop()

	// crash-loop guard: too many boots in the window → SAFE MODE
	safe, boots := a.Boot.RecordBoot()
	a.SafeMode.Store(safe)
	restored := 0
	if safe {
		slog.Error("SAFE MODE: boots in the last window — sessions stay on disk, nothing auto-connects. Ack via POST /admin/safe_mode/ack",
			"boots", boots, "window_s", 600)
	} else {
		restored = a.Mgr.RestoreAll(context.Background())
	}
	if bad := a.Store.VerifyTokens(); len(bad) > 0 {
		slog.Error("GW_FERNET_KEY cannot decrypt tokens (wrong key? those bots will not start)",
			"aliases", strings.Join(bad, ", "))
	}
	slog.Info("gateway up",
		"restored", restored,
		"version", version.Version,
		"upstream", a.Cfg.Upstream,
		"upstream_kind", a.Cfg.EffectiveKind(),
		"env_detect", a.Cfg.EnvDetect,
		"path_prefix", a.Cfg.PathPrefix,
	)
}

// maintenanceLoop runs the hourly integrity/backup/retention sweep.
func (a *App) maintenanceLoop() {
	interval := a.Cfg.Storage.MaintenanceInterval
	if interval <= 0 {
		return
	}
	t := time.NewTicker(time.Duration(interval * float64(time.Second)))
	defer t.Stop()
	for {
		select {
		case <-a.maintStop:
			return
		case <-t.C:
			store.MaintenancePass(
				[]string{a.Store.Path, a.Cfg.DataDir + "/queue.db"},
				a.Cfg.BackupsDir(), a.Health,
				a.Cfg.Storage.BackupKeep, a.Cfg.Storage.BackupInterval,
				"DELETE FROM admin_audit WHERE CAST(strftime('%s', ts) AS REAL) < ?",
				a.Cfg.Storage.AuditRetentionDays)
		}
	}
}

// Close drains everything (graceful shutdown path); idempotent.
func (a *App) Close() {
	a.closeOnce.Do(func() {
		close(a.maintStop)
		if a.Supervisor != nil {
			a.Supervisor.Stop()
		}
		a.Beat.Stop()
		a.Mgr.Shutdown()
		a.Store.Close()
	})
}

// ListenAndServe blocks serving HTTP (both listeners when split).
func (a *App) ListenAndServe() error {
	if a.adminSrv != nil {
		go func() { _ = a.adminSrv.ListenAndServe() }()
	}
	return a.srv.ListenAndServe()
}

// Shutdown gracefully drains in-flight requests.
func (a *App) Shutdown(ctx context.Context) error {
	if a.adminSrv != nil {
		_ = a.adminSrv.Shutdown(ctx)
	}
	return a.srv.Shutdown(ctx)
}

// RunServe boots and blocks until SIGTERM/SIGINT.
func (a *App) RunServe() error {
	shutdownTracing, err := tracing.Setup(context.Background(), a.Cfg.OTELEndpoint, "botapi")
	if err != nil {
		return err
	}
	defer func() { _ = shutdownTracing(context.Background()) }()
	errCh := make(chan error, 1)
	go func() { errCh <- a.ListenAndServe() }()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	select {
	case <-sig:
	case err := <-errCh:
		if err != nil && err != http.ErrServerClosed {
			return err
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	_ = a.Shutdown(ctx)
	a.Close()
	return nil
}

// ── routing ────────────────────────────────────────────────────────

func (a *App) routes(mux *http.ServeMux) {
	a.publicRoutes(mux)
	a.adminRoutes(mux)
}

// publicRoutes is the consumer surface: proxy, alias routes, files and
// the kubelet probes. When GW_ADMIN_PORT splits the listeners, this is
// everything the API Service exposes.
func (a *App) publicRoutes(mux *http.ServeMux) {
	prefix := a.Cfg.PathPrefix // "" or "/tgapi"
	if prefix != "" {
		mux.HandleFunc("POST "+prefix+"/", a.handleTGAPIPost)
		mux.HandleFunc("GET "+prefix+"/", a.handleTGAPIGetRoute)
		mux.HandleFunc("GET "+prefix+"/file/", a.handleFileRoute)
	}
	// Bare Bot API paths (/botTOKEN/method, /file/botTOKEN/…) — catch-all
	// under / with more-specific /admin /bots /healthz winning first.
	mux.HandleFunc("POST /", a.handleRootBotPost)
	mux.HandleFunc("GET /", a.handleRootBotGet)
	mux.HandleFunc("POST /bots/{alias}/{method}", a.handleByAlias)
	mux.HandleFunc("GET /file/", a.handleFileRoute)

	mux.HandleFunc("GET /healthz", a.handleHealthz)
	mux.HandleFunc("GET /readyz", a.handleReadyz)
}

// adminRoutes is the operator surface: /admin/* and /metrics.
func (a *App) adminRoutes(mux *http.ServeMux) {
	a.mountPprof(mux)
	mux.HandleFunc("POST /admin/bots", a.adminRegister)
	mux.HandleFunc("GET /admin/status", a.adminStatus)
	mux.HandleFunc("GET /admin/upstream", a.adminUpstream)
	mux.HandleFunc("GET /admin/env", a.adminEnv)
	mux.HandleFunc("POST /admin/upstream/failover", a.adminUpstreamFailover)
	mux.HandleFunc("POST /admin/upstream/failback", a.adminUpstreamFailback)
	mux.HandleFunc("POST /admin/bots/{alias}/pause", a.adminPause)
	mux.HandleFunc("POST /admin/bots/{alias}/resume", a.adminResume)
	mux.HandleFunc("POST /admin/bots/{alias}/drain", a.adminDrain)
	mux.HandleFunc("DELETE /admin/bots/{alias}", a.adminDelete)
	mux.HandleFunc("POST /admin/bots/{alias}/push", a.adminSetPush)
	mux.HandleFunc("POST /admin/bots/{alias}/paid", a.adminSetPaid)
	mux.HandleFunc("POST /admin/bots/{alias}/typing", a.adminSetBotTyping)
	mux.HandleFunc("GET /admin/settings/typing", a.adminGetTypingSettings)
	mux.HandleFunc("POST /admin/settings/typing", a.adminSetTypingSettings)
	mux.HandleFunc("POST /admin/safe_mode/ack", a.adminSafeModeAck)
	mux.HandleFunc("GET /admin/audit", a.adminAudit)

	mux.HandleFunc("GET /metrics", a.handleMetrics)
}

// splitBotTokenPath parses "<prefix>/bot<token>/<rest>" paths.
func splitBotTokenPath(path, prefix string) (token, rest string, ok bool) {
	p := strings.TrimPrefix(path, prefix)
	p = strings.TrimPrefix(p, "/")
	if !strings.HasPrefix(p, "bot") {
		return "", "", false
	}
	remainder := p[len("bot"):]
	idx := strings.Index(remainder, "/")
	if idx < 0 {
		return "", "", false
	}
	return remainder[:idx], remainder[idx+1:], true
}

func (a *App) handleTGAPIPost(w http.ResponseWriter, r *http.Request) {
	a.dispatchBotAPI(w, r, a.Cfg.PathPrefix, true)
}

func (a *App) handleTGAPIGetRoute(w http.ResponseWriter, r *http.Request) {
	prefix := a.Cfg.PathPrefix
	if strings.HasPrefix(r.URL.Path, prefix+"/file/") || strings.HasPrefix(r.URL.Path, "/file/") {
		a.handleFileRoute(w, r)
		return
	}
	a.dispatchBotAPI(w, r, prefix, false)
}

func (a *App) handleRootBotPost(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, "/bot") {
		writeJSON(w, 404, map[string]any{"ok": false, "error_code": 404})
		return
	}
	a.dispatchBotAPI(w, r, "", true)
}

func (a *App) handleRootBotGet(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/file/") {
		a.handleFileRoute(w, r)
		return
	}
	if !strings.HasPrefix(r.URL.Path, "/bot") {
		writeJSON(w, 404, map[string]any{"ok": false, "error_code": 404})
		return
	}
	a.dispatchBotAPI(w, r, "", false)
}

// dispatchBotAPI parses /[prefix]/botTOKEN[/test]/method and invokes the proxy.
func (a *App) dispatchBotAPI(w http.ResponseWriter, r *http.Request, prefix string, isPost bool) {
	token, method, ok := splitBotTokenPath(r.URL.Path, prefix)
	if !ok {
		writeJSON(w, 404, map[string]any{"ok": false, "error_code": 404})
		return
	}
	// Support /botTOKEN/test/method (Telegram test DC path).
	method = strings.TrimPrefix(method, "/")
	logConsumer(r, token, method)
	s := a.Mgr.FindByToken(token)
	if s == nil {
		writeJSON(w, 404, map[string]any{"ok": false, "error_code": 404, "description": "unknown bot token"})
		return
	}
	if isPost {
		a.callRoute(w, r, s, method)
		return
	}
	q := map[string]any{}
	for k, vals := range r.URL.Query() {
		if len(vals) > 0 {
			q[k] = vals[0]
		}
	}
	a.writeGwResponse(w, a.Proxy.Call(r.Context(), s, method, q, "", nil, ""))
}

func logConsumer(r *http.Request, token, method string) {
	slog.Debug("botapi request",
		"method", method,
		"token", RedactToken(token),
		"ua", r.Header.Get("User-Agent"),
		"x_forwarded_for", r.Header.Get("X-Forwarded-For"),
		"x_forwarded_proto", r.Header.Get("X-Forwarded-Proto"),
	)
}

func (a *App) handleFileRoute(w http.ResponseWriter, r *http.Request) {
	token, path, ok := splitBotTokenPath(r.URL.Path, "/file")
	if !ok && a.Cfg.PathPrefix != "" {
		token, path, ok = splitBotTokenPath(r.URL.Path, a.Cfg.PathPrefix+"/file")
	}
	if !ok {
		writeJSON(w, 404, map[string]any{"ok": false, "error_code": 404})
		return
	}
	if a.Mgr.FindByToken(token) == nil {
		writeJSON(w, 404, map[string]any{"ok": false, "error_code": 404})
		return
	}
	logConsumer(r, token, "file:"+path)

	// Local companion + shared volume: serve absolute disk paths directly.
	if a.Cfg.EffectiveKind() == "local" && (strings.HasPrefix(path, "/") || looksAbsFilePath(path)) {
		disk := path
		if !strings.HasPrefix(disk, "/") {
			disk = "/" + disk
		}
		if st, err := os.Stat(disk); err == nil && !st.IsDir() {
			f, err := os.Open(disk)
			if err == nil {
				defer f.Close()
				w.Header().Set("Content-Type", "application/octet-stream")
				w.Header().Set("Content-Length", strconv.FormatInt(st.Size(), 10))
				w.WriteHeader(200)
				_, _ = io.Copy(w, f)
				return
			}
		}
	}

	req := &httpRequest{Method: "GET", URL: "/file/bot" + token + "/" + path}
	if streamer, ok := a.Mgr.client.(StreamHTTPClient); ok {
		resp, err := streamer.DoStream(req)
		if err != nil {
			writeJSON(w, 502, map[string]any{"ok": false, "error_code": 502, "description": "upstream: " + err.Error()})
			return
		}
		defer resp.BodyStream.Close()
		ct := resp.Header.Get("Content-Type")
		if ct == "" {
			ct = "application/octet-stream"
		}
		w.Header().Set("Content-Type", ct)
		if cl := resp.Header.Get("Content-Length"); cl != "" {
			w.Header().Set("Content-Length", cl)
		}
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.BodyStream)
		return
	}
	resp, err := a.Mgr.client.Do(req)
	if err != nil {
		writeJSON(w, 502, map[string]any{"ok": false, "error_code": 502, "description": "upstream: " + err.Error()})
		return
	}
	ct := resp.Header.Get("Content-Type")
	if ct == "" {
		ct = "application/octet-stream"
	}
	w.Header().Set("Content-Type", ct)
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(resp.Body)
}

func looksAbsFilePath(p string) bool {
	return strings.Contains(p, "/") && (strings.HasPrefix(p, "var/") || strings.HasPrefix(p, "home/") || strings.HasPrefix(p, "data/") || strings.HasPrefix(p, "tmp/"))
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeRawJSON(w http.ResponseWriter, status int, raw []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(raw)
}

func (a *App) writeGwResponse(w http.ResponseWriter, r *gwResponse) {
	for k, v := range r.header {
		w.Header().Set(k, v)
	}
	if r.isRaw() {
		writeRawJSON(w, r.status, r.raw)
		return
	}
	writeJSON(w, r.status, r.jsonBody)
}

// mountPprof attaches net/http/pprof behind the admin secret, only
// when GW_PPROF=1. Profiling is an operator tool on the admin
// listener — never on the public API surface.
func (a *App) mountPprof(mux *http.ServeMux) {
	if !a.Cfg.Pprof {
		return
	}
	auth := func(h http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if !a.adminAuthed(r) {
				writeJSON(w, 401, map[string]any{"ok": false, "error_code": 401})
				return
			}
			h(w, r)
		}
	}
	mux.HandleFunc("GET /debug/pprof/", auth(pprof.Index))
	mux.HandleFunc("GET /debug/pprof/cmdline", auth(pprof.Cmdline))
	mux.HandleFunc("GET /debug/pprof/profile", auth(pprof.Profile))
	mux.HandleFunc("GET /debug/pprof/symbol", auth(pprof.Symbol))
	mux.HandleFunc("GET /debug/pprof/trace", auth(pprof.Trace))
}

func (a *App) adminAuthed(r *http.Request) bool {
	return bearerOK(r.Header.Get("Authorization"), a.Cfg.AdminSecret)
}

func (a *App) appAuthed(r *http.Request) bool {
	return bearerOK(r.Header.Get("Authorization"), a.Cfg.AppSecret)
}

// upload limits: over the cap the gateway answers 413 instead of
// silently truncating (a truncated upload would send a CORRUPT file).
const (
	defaultMaxMultipartBody = 64 << 20
	maxMultipartBody        = defaultMaxMultipartBody // tests + backward-compat alias
	maxFormBody             = 1 << 20
	maxJSONBody             = 8 << 20
)

func (a *App) maxMultipartBody() int64 {
	if a.Cfg.MaxMultipartMB > 0 {
		return int64(a.Cfg.MaxMultipartMB) << 20
	}
	return defaultMaxMultipartBody
}

func tooLarge(w http.ResponseWriter, limit int64) bool {
	const msg = `{"ok":false,"error_code":413,"description":"request body exceeds the gateway limit"}`
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusRequestEntityTooLarge)
	_, _ = w.Write([]byte(msg))
	return true
}

// callRoute parses the request body the way the Python gateway does:
// JSON dict, urlencoded form (ints/bools coerced), or raw multipart.
func (a *App) callRoute(w http.ResponseWriter, r *http.Request, s *BotSession, method string) {
	rctx, span := tracing.Tracer("botapi").Start(r.Context(), "tggw.call",
		trace.WithAttributes(
			attribute.String("tggw.method", method),
			attribute.String("tggw.alias", s.Alias),
		))
	defer span.End()
	r = r.WithContext(rctx)
	ctype := r.Header.Get("Content-Type")
	maxMP := a.maxMultipartBody()
	if strings.HasPrefix(ctype, "multipart/") {
		r.Body = http.MaxBytesReader(w, r.Body, maxMP)
		if r.ContentLength > 0 && r.ContentLength <= maxMP {
			// STREAM: read a prefix (the chat_id form field lives in the
			// leading parts), forward the rest verbatim without ever
			// holding the whole upload in memory
			prefix := make([]byte, 64<<10)
			n, _ := io.ReadFull(r.Body, prefix)
			a.writeGwResponse(w, a.Proxy.ForwardStream(
				r.Context(), s, method, prefix[:n], r.Body, r.ContentLength, ctype))
			return
		}
		// chunked/unknown length: buffer (bounded) — a ContentLength is
		// mandatory for streaming (api.telegram.org rejects chunked)
		raw, err := io.ReadAll(r.Body)
		if err != nil && isTooLarge(err) {
			tooLarge(w, maxMP)
			return
		}
		a.writeGwResponse(w, a.Proxy.Call(r.Context(), s, method, nil, ctype, raw, ""))
		return
	}
	if strings.HasPrefix(ctype, "application/x-www-form-urlencoded") {
		r.Body = http.MaxBytesReader(w, r.Body, maxFormBody)
		raw, err := io.ReadAll(r.Body)
		if err != nil && isTooLarge(err) {
			tooLarge(w, maxFormBody)
			return
		}
		// urlencoded bodies forward VERBATIM (data-integrity fix over
		// the Python plane, which coerced "007"→7 and "true"→boolean —
		// corrupting user text; see CHANGELOG). Only the chat_id field
		// is parsed, for the rate guard. Gateway-owned methods (the
		// gateway answers them itself) still see a parsed dict.
		m := strings.ToLower(method)
		if gatewayOwned[m] {
			a.writeGwResponse(w, a.Proxy.Call(r.Context(), s, method, parseFormBody(raw), "", nil, r.Header.Get("Idempotency-Key")))
			return
		}
		chatID := formChatID(raw)
		a.writeGwResponse(w, a.Proxy.CallRawWithChat(r.Context(), s, method, raw, ctype, chatID, r.Header.Get("Idempotency-Key")))
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxJSONBody)
	raw, err := io.ReadAll(r.Body)
	if err != nil && isTooLarge(err) {
		tooLarge(w, maxJSONBody)
		return
	}
	body := map[string]any{}
	if len(raw) > 0 {
		// invalid JSON stays TOLERANT (body = {}): the Python gateway's
		// _json_body does exactly that — parity over taste
		var parsed map[string]any
		if err := json.Unmarshal(raw, &parsed); err == nil && parsed != nil {
			body = parsed
		}
	}
	a.writeGwResponse(w, a.Proxy.Call(r.Context(), s, method, body, "", nil, r.Header.Get("Idempotency-Key")))
}

func isTooLarge(err error) bool {
	var maxErr *http.MaxBytesError
	return errors.As(err, &maxErr)
}

// formChatID extracts ONLY the chat_id field for rate-guard scoping —
// the rest of the body forwards verbatim (url.ParseQuery, no value
// coercion). Returns nil when absent or @username (global bucket until
// resolved on the JSON path).
func formChatID(raw []byte) *int64 {
	vals, err := url.ParseQuery(string(raw))
	if err != nil {
		return nil
	}
	v := vals.Get("chat_id")
	if v == "" {
		v = vals.Get("chat")
	}
	if v == "" || strings.HasPrefix(v, "@") {
		return nil
	}
	id, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return nil
	}
	return &id
}

// parseFormBody builds a map for GATEWAY-OWNED methods (they answer
// locally and need the args). Values stay STRINGS — the gateway-owned
// handlers parse what they need; no silent "007"→7 corruption.
func parseFormBody(raw []byte) map[string]any {
	vals, err := url.ParseQuery(string(raw))
	if err != nil {
		return map[string]any{}
	}
	out := map[string]any{}
	for k := range vals {
		out[k] = vals.Get(k)
	}
	return out
}

func (a *App) handleByAlias(w http.ResponseWriter, r *http.Request) {
	if !a.appAuthed(r) {
		writeJSON(w, 401, map[string]any{"ok": false, "error_code": 401})
		return
	}
	s := a.Mgr.Get(r.PathValue("alias"))
	if s == nil {
		writeJSON(w, 404, map[string]any{"ok": false, "error_code": 404, "description": "unknown alias"})
		return
	}
	a.callRoute(w, r, s, r.PathValue("method"))
}

// ── admin ──────────────────────────────────────────────────────────

func (a *App) adminRegister(w http.ResponseWriter, r *http.Request) {
	if !a.adminAuthed(r) {
		writeJSON(w, 401, map[string]any{"ok": false, "error_code": 401})
		return
	}
	var body struct {
		Alias          *string `json:"alias"`
		Token          string  `json:"token"`
		OnLimit        *string `json:"on_limit"`
		PaidBroadcasts bool    `json:"paid_broadcasts"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&body); err != nil || body.Token == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error_code": 400, "description": "token required"})
		return
	}
	if body.OnLimit != nil && *body.OnLimit != "queue" && *body.OnLimit != "reject" {
		writeJSON(w, 400, map[string]any{"ok": false, "error_code": 400, "description": "on_limit must be queue|reject"})
		return
	}
	alias := ""
	if body.Alias != nil {
		alias = *body.Alias
	}
	if alias == "" {
		alias = "bot-" + randHex(4)
	}
	s, err := a.Mgr.Attach(r.Context(), alias, body.Token, body.OnLimit, body.PaidBroadcasts)
	if err != nil {
		writeJSON(w, 500, map[string]any{"ok": false, "error_code": 500, "description": err.Error()})
		return
	}
	a.Store.Audit("register", alias, "token="+store.TokenSuffix(body.Token)+" on_limit="+s.EffectiveOnLimit())
	writeJSON(w, 200, map[string]any{
		"ok": true, "alias": alias, "state": string(s.CurrentState()),
		"on_limit": s.EffectiveOnLimit(),
	})
}

func randHex(n int) string {
	const hexdigits = "0123456789abcdef"
	out := make([]byte, n)
	for i := range out {
		out[i] = hexdigits[int(time.Now().UnixNano()>>(uint(i)*4))&0xf]
	}
	return string(out)
}

func (a *App) adminStatus(w http.ResponseWriter, r *http.Request) {
	if !a.adminAuthed(r) {
		writeJSON(w, 401, map[string]any{"ok": false, "error_code": 401})
		return
	}
	bots := []map[string]any{}
	for _, alias := range sortedAliases(a.Mgr.Sessions()) {
		s := a.Mgr.Get(alias)
		if s == nil {
			continue
		}
		st := s.PublicStatus()
		st["typing"] = a.Mgr.Typing().StatusFor(s)
		bots = append(bots, st)
	}
	out := map[string]any{"ok": true, "bots": bots, "upstream_kind": a.Cfg.EffectiveKind()}
	if a.Supervisor != nil {
		out["upstream"] = a.Supervisor.Status()
	}
	writeJSON(w, 200, out)
}

func (a *App) adminUpstream(w http.ResponseWriter, r *http.Request) {
	if !a.adminAuthed(r) {
		writeJSON(w, 401, map[string]any{"ok": false, "error_code": 401})
		return
	}
	if a.Supervisor == nil {
		writeJSON(w, 200, map[string]any{
			"ok": true, "active": a.Cfg.Upstream, "failover_active": false,
			"kind": a.Cfg.EffectiveKind(),
		})
		return
	}
	st := a.Supervisor.Status()
	st["ok"] = true
	writeJSON(w, 200, st)
}

func (a *App) adminEnv(w http.ResponseWriter, r *http.Request) {
	if !a.adminAuthed(r) {
		writeJSON(w, 401, map[string]any{"ok": false, "error_code": 401})
		return
	}
	if a.Supervisor == nil {
		writeJSON(w, 200, map[string]any{"ok": true, "env_detect": a.Cfg.EnvDetect})
		return
	}
	st := a.Supervisor.Status()
	writeJSON(w, 200, map[string]any{
		"ok": true,
		"env_detect": a.Cfg.EnvDetect,
		"facts": st["facts"],
		"decision": st["decision"],
		"tls_state": st["tls_state"],
		"clock_skew_seconds": st["clock_skew_seconds"],
		"env_runtime": st["env_runtime"],
		"upstream_active": st["active"],
	})
}

func (a *App) adminUpstreamFailover(w http.ResponseWriter, r *http.Request) {
	if !a.adminAuthed(r) {
		writeJSON(w, 401, map[string]any{"ok": false, "error_code": 401})
		return
	}
	if a.Supervisor == nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error_code": 400, "description": "upstream supervisor not enabled"})
		return
	}
	if err := a.Supervisor.ForceFailover("admin"); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error_code": 400, "description": err.Error()})
		return
	}
	a.Proxy.SetLocalKind(true)
	a.Store.Audit("upstream_failover", "", "admin")
	writeJSON(w, 200, map[string]any{"ok": true, "upstream": a.Supervisor.Status()})
}

func (a *App) adminUpstreamFailback(w http.ResponseWriter, r *http.Request) {
	if !a.adminAuthed(r) {
		writeJSON(w, 401, map[string]any{"ok": false, "error_code": 401})
		return
	}
	if a.Supervisor == nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error_code": 400, "description": "upstream supervisor not enabled"})
		return
	}
	_ = a.Supervisor.Failback()
	a.Proxy.SetLocalKind(a.Cfg.EffectiveKind() == "local")
	a.Store.Audit("upstream_failback", "", "admin")
	writeJSON(w, 200, map[string]any{"ok": true, "upstream": a.Supervisor.Status()})
}

func (a *App) adminPause(w http.ResponseWriter, r *http.Request) {
	if !a.adminAuthed(r) {
		writeJSON(w, 401, map[string]any{"ok": false, "error_code": 401})
		return
	}
	alias := r.PathValue("alias")
	if a.Mgr.Pause(alias) {
		a.Store.Audit("pause", alias, "")
		writeJSON(w, 200, map[string]any{"ok": true})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": false})
}

func (a *App) adminResume(w http.ResponseWriter, r *http.Request) {
	if !a.adminAuthed(r) {
		writeJSON(w, 401, map[string]any{"ok": false, "error_code": 401})
		return
	}
	alias := r.PathValue("alias")
	if a.Mgr.Resume(r.Context(), alias) {
		a.Store.Audit("resume", alias, "")
		writeJSON(w, 200, map[string]any{"ok": true})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": false})
}

func (a *App) adminDrain(w http.ResponseWriter, r *http.Request) {
	if !a.adminAuthed(r) {
		writeJSON(w, 401, map[string]any{"ok": false, "error_code": 401})
		return
	}
	alias := r.PathValue("alias")
	if a.Mgr.Drain(alias) {
		a.Store.Audit("drain", alias, "")
		writeJSON(w, 200, map[string]any{"ok": true})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": false})
}

func (a *App) adminDelete(w http.ResponseWriter, r *http.Request) {
	if !a.adminAuthed(r) {
		writeJSON(w, 401, map[string]any{"ok": false, "error_code": 401})
		return
	}
	alias := r.PathValue("alias")
	if a.Mgr.Delete(r.Context(), alias) {
		a.Store.Audit("delete", alias, "")
		writeJSON(w, 200, map[string]any{"ok": true})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": false})
}

func (a *App) adminSetPush(w http.ResponseWriter, r *http.Request) {
	if !a.adminAuthed(r) {
		writeJSON(w, 401, map[string]any{"ok": false, "error_code": 401})
		return
	}
	alias := r.PathValue("alias")
	var body struct {
		URL *string `json:"url"`
	}
	_ = json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&body)
	url := ""
	if body.URL != nil {
		url = *body.URL
	}
	if !a.Mgr.SetPush(r.Context(), alias, url) {
		writeJSON(w, 404, map[string]any{"ok": false, "error_code": 404})
		return
	}
	a.Store.Audit("set_push", alias, "url="+url)
	writeJSON(w, 200, map[string]any{"ok": true, "push_url": nilIfEmpty(a.Mgr.Get(alias).PushURL())})
}

func (a *App) adminSetPaid(w http.ResponseWriter, r *http.Request) {
	if !a.adminAuthed(r) {
		writeJSON(w, 401, map[string]any{"ok": false, "error_code": 401})
		return
	}
	alias := r.PathValue("alias")
	var body struct {
		Enabled bool `json:"enabled"`
	}
	_ = json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&body)
	s := a.Mgr.Get(alias)
	if s == nil {
		writeJSON(w, 404, map[string]any{"ok": false, "error_code": 404})
		return
	}
	s.PaidBroadcasts = body.Enabled
	s.guard.PaidEnabled = body.Enabled
	a.Store.Audit("set_paid", alias, fmt.Sprintf("enabled=%v", body.Enabled))
	writeJSON(w, 200, map[string]any{"ok": true, "paid_broadcasts": s.PaidBroadcasts})
}

func (a *App) adminSetBotTyping(w http.ResponseWriter, r *http.Request) {
	if !a.adminAuthed(r) {
		writeJSON(w, 401, map[string]any{"ok": false, "error_code": 401})
		return
	}
	alias := r.PathValue("alias")
	var patch TypingPatch
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&patch); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error_code": 400, "description": "invalid JSON"})
		return
	}
	s := a.Mgr.Get(alias)
	if s == nil {
		writeJSON(w, 404, map[string]any{"ok": false, "error_code": 404})
		return
	}
	s.typMu.Lock()
	s.typingEnabled = patch.Enabled
	s.typingPrivateOnly = patch.PrivateOnly
	s.typingIntervalS = patch.IntervalS
	s.typingTimeoutS = patch.TimeoutS
	s.typingTimeoutText = patch.TimeoutText
	s.typMu.Unlock()
	a.Store.Audit("set_typing", alias, describePatch(&patch))
	writeJSON(w, 200, map[string]any{"ok": true, "typing": a.Mgr.Typing().StatusFor(s)})
}

func describePatch(p *TypingPatch) string {
	var parts []string
	if p.Enabled != nil {
		parts = append(parts, fmt.Sprintf("enabled=%v", *p.Enabled))
	}
	if p.PrivateOnly != nil {
		parts = append(parts, fmt.Sprintf("private_only=%v", *p.PrivateOnly))
	}
	if p.IntervalS != nil {
		parts = append(parts, fmt.Sprintf("interval_s=%v", *p.IntervalS))
	}
	if p.TimeoutS != nil {
		parts = append(parts, fmt.Sprintf("timeout_s=%v", *p.TimeoutS))
	}
	if p.TimeoutText != nil {
		parts = append(parts, fmt.Sprintf("timeout_text=%v", *p.TimeoutText))
	}
	if len(parts) == 0 {
		return "reset-all"
	}
	return strings.Join(parts, ", ")
}

func (a *App) adminGetTypingSettings(w http.ResponseWriter, r *http.Request) {
	if !a.adminAuthed(r) {
		writeJSON(w, 401, map[string]any{"ok": false, "error_code": 401})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "settings": a.Mgr.Typing().Settings().Snapshot()})
}

func (a *App) adminSetTypingSettings(w http.ResponseWriter, r *http.Request) {
	if !a.adminAuthed(r) {
		writeJSON(w, 401, map[string]any{"ok": false, "error_code": 401})
		return
	}
	var patch TypingPatch
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&patch); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error_code": 400, "description": "invalid JSON"})
		return
	}
	settings := a.Mgr.Typing().Settings().Apply(patch)
	a.Store.Audit("set_typing_global", "global", describePatch(&patch))
	writeJSON(w, 200, map[string]any{"ok": true, "settings": settings})
}

func (a *App) adminSafeModeAck(w http.ResponseWriter, r *http.Request) {
	if !a.adminAuthed(r) {
		writeJSON(w, 401, map[string]any{"ok": false, "error_code": 401})
		return
	}
	a.Boot.Clear()
	a.SafeMode.Store(false)
	restored := a.Mgr.RestoreAll(r.Context())
	a.Store.Audit("safe_mode_ack", "global", fmt.Sprintf("restored=%d", restored))
	writeJSON(w, 200, map[string]any{"ok": true, "safe_mode": false, "restored": restored})
}

func (a *App) adminAudit(w http.ResponseWriter, r *http.Request) {
	if !a.adminAuthed(r) {
		writeJSON(w, 401, map[string]any{"ok": false, "error_code": 401})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "entries": a.Store.GetAudit(100)})
}

// ── health & metrics ───────────────────────────────────────────────

// bearerOK compares secrets in constant time (timing side channels).
func bearerOK(header, secret string) bool {
	got := strings.TrimPrefix(header, "Bearer ")
	if len(got) != len(secret) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(secret)) == 1
}

func (a *App) handleHealthz(w http.ResponseWriter, r *http.Request) {
	lag := round2(a.Beat.Lag())
	writeJSON(w, 200, map[string]any{"ok": a.Beat.Alive(), "version": version.Version, "loop_lag_s": lag})
}

func (a *App) handleReadyz(w http.ResponseWriter, r *http.Request) {
	// write probe: catches full/read-only/locked storage
	if _, err := a.Store.DB.Exec("BEGIN IMMEDIATE"); err != nil {
		writeJSON(w, 503, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if _, err := a.Store.DB.Exec("ROLLBACK"); err != nil {
		writeJSON(w, 503, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if reason := a.Health.Degraded(); reason != "" {
		writeJSON(w, 503, map[string]any{"ok": false, "storage": a.Health.Snapshot()})
		return
	}
	if a.SafeMode.Load() {
		writeJSON(w, 503, map[string]any{
			"ok": false, "safe_mode": true,
			"description": "crash-loop safe mode — ack via POST /admin/safe_mode/ack",
		})
		return
	}
	states := map[string]string{}
	live := 0
	for alias, s := range a.Mgr.Sessions() {
		st := string(s.CurrentState())
		states[alias] = st
		if st == "live" {
			live++
		}
	}
	writeJSON(w, 200, map[string]any{"ok": true, "sessions": states, "live": live})
}

func round2(f float64) float64 { return float64(int64(f*100+0.5)) / 100 }

func (a *App) handleMetrics(w http.ResponseWriter, r *http.Request) {
	var lines []string
	for _, alias := range sortedAliases(a.Mgr.Sessions()) {
		s := a.Mgr.Get(alias)
		if s == nil {
			continue
		}
		labels := fmt.Sprintf("alias=%q", s.Alias)
		stateVal := 0
		s.mu.Lock()
		connectedSince := s.connectedSince
		s.mu.Unlock()
		if s.CurrentState() == StateLive {
			stateVal = 1
		}
		age := int64(0)
		if connectedSince > 0 {
			age = int64(wallNow() - connectedSince)
		}
		q := s.queue.Stats()
		lines = append(lines,
			fmt.Sprintf("tg_gateway_session_state{%s} %d", labels, stateVal),
			fmt.Sprintf("tg_gateway_session_connection_age_seconds{%s} %d", labels, age),
			fmt.Sprintf("tg_gateway_update_queue_depth{%s} %d", labels, q["depth"]),
			fmt.Sprintf("tg_gateway_updates_dropped_oldest_total{%s} %d", labels, q["dropped_oldest"]),
			fmt.Sprintf("tg_gateway_updates_expired_by_ttl_total{%s} %d", labels, q["expired_by_ttl"]),
			fmt.Sprintf("tg_gateway_queue_persistent{%s} %d", labels, b2i(s.queue.Persistent())),
			fmt.Sprintf("tg_gateway_real_429_total{%s} %d", labels, s.real429Total()),
			fmt.Sprintf("tg_gateway_synthetic_429_total{%s} %d", labels, s.synth429Total()),
			fmt.Sprintf("tg_gateway_typing_active{%s} %d", labels, a.Mgr.Typing().ActiveCount(s.Alias)),
			fmt.Sprintf("tg_gateway_persist_failures_total{%s} %d", labels, a.Health.PersistFailures),
			fmt.Sprintf("tg_gateway_corrupt_rows_total{%s} %d", labels, a.Health.CorruptRows),
			fmt.Sprintf("tg_gateway_queue_max_age_seconds{%s} %v", labels, q["max_age_s"]),
		)
		if bl := s.queue.UnloadedBacklog(); bl > 0 {
			lines = append(lines, fmt.Sprintf("tg_gateway_backlog_unloaded{%s} %d", labels, bl))
		}
	}
	degraded := 0
	if a.Health.Degraded() != "" {
		degraded = 1
	}
	if n := multipartChatUnresolved.Load(); n > 0 {
		lines = append(lines, fmt.Sprintf("tg_gateway_multipart_chat_unresolved_total %d", n))
	}
	lines = append(lines,
		fmt.Sprintf("tg_gateway_storage_degraded{plane=\"bot\"} %d", degraded),
		fmt.Sprintf("tg_gateway_loop_lag_seconds{plane=\"bot\"} %v", round2(a.Beat.Lag())),
	)
	if a.Supervisor != nil {
		st := a.Supervisor.Status()
		failover := 0
		if st["failover_active"] == true {
			failover = 1
		}
		tlsState, _ := st["tls_state"].(string)
		rt, _ := st["env_runtime"].(string)
		active, _ := st["active"].(string)
		skew, _ := st["clock_skew_seconds"].(int64)
		lines = append(lines,
			fmt.Sprintf("tg_gateway_upstream_failover_active %d", failover),
			fmt.Sprintf("tg_gateway_clock_skew_seconds %d", skew),
			fmt.Sprintf("tg_gateway_tls_state{state=%q} 1", tlsState),
			fmt.Sprintf("tg_gateway_env_runtime{runtime=%q} 1", rt),
			fmt.Sprintf("tg_gateway_upstream_active{url=%q} 1", RedactToken(active)),
		)
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	w.WriteHeader(200)
	_, _ = w.Write([]byte(strings.Join(lines, "\n") + "\n"))
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

func (s *BotSession) real429Total() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.real429
}

func (s *BotSession) synth429Total() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.synth429
}
