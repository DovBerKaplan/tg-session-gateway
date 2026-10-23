// Package botapi is the Bot API gateway: drop-in proxy in front of
// api.telegram.org, one poller per token, a persistent update queue and
// the full rate guard. Port of gateway/sessions.py.
package botapi

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/DovBerKaplan/tg-session-gateway/go/internal/config"
	"github.com/DovBerKaplan/tg-session-gateway/go/internal/health"
	"github.com/DovBerKaplan/tg-session-gateway/go/internal/rateguard"
	"github.com/DovBerKaplan/tg-session-gateway/go/internal/store"
)

// State is the session lifecycle state.
type State string

const (
	StateConnecting State = "connecting"
	StateLive       State = "live"
	StatePaused     State = "paused"
	StateDraining   State = "draining"
	StateError      State = "error"
)

// gatewayOwned methods are never forwarded upstream (spec §4.6): the
// gateway IS the poller; setWebhook/logout/close would break ownership.
var gatewayOwned = map[string]bool{
	"getupdates": true, "setwebhook": true, "deletewebhook": true,
	"logout": true, "close": true,
}

// BotSession is one bot token owned exclusively by this gateway.
type BotSession struct {
	Alias string
	Token string

	mu             sync.Mutex
	state          State
	queue          *store.Queue
	guard          *rateguard.Guard
	connectedSince float64
	lastError      string
	real429        int
	synth429       int
	last429        map[string]any
	pushURL        string
	pushHealthy    *bool

	pollCancel context.CancelFunc
	pushCancel context.CancelFunc

	chatNameCache map[string]int64 // @username → numeric id (spec §8.2 §4)

	// per-bot overrides — nil inherits the gateway default
	OnLimit        *string
	PaidBroadcasts bool

	// typing overrides — nil inherits the runtime global layer
	typMu             sync.Mutex
	typingEnabled     *bool
	typingPrivateOnly *bool
	typingIntervalS   *float64
	typingTimeoutS    *float64
	typingTimeoutText *string

	sendTimes []float64 // rolling 1s send meter (§13)
}

// EffectiveOnLimit is the per-bot policy with the gateway default.
func (s *BotSession) EffectiveOnLimit() string {
	if s.OnLimit != nil {
		return *s.OnLimit
	}
	return "queue"
}

// State returns the current lifecycle state.
func (s *BotSession) CurrentState() State {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

func (s *BotSession) setState(st State) {
	s.mu.Lock()
	s.state = st
	s.mu.Unlock()
}

// Queue exposes the session's update queue (poller + pull consumers).
func (s *BotSession) Queue() *store.Queue { return s.queue }

// Guard exposes the session's rate guard (typing reads send-state).
func (s *BotSession) Guard() *rateguard.Guard { return s.guard }

func (s *BotSession) noteSend() {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := float64(time.Now().UnixNano()) / 1e9
	s.sendTimes = append(s.sendTimes, now)
	if len(s.sendTimes) > 200 {
		s.sendTimes = s.sendTimes[len(s.sendTimes)-200:]
	}
}

// sendsPerSLocked requires s.mu held (PublicStatus already owns it).
func (s *BotSession) sendsPerSLocked() float64 {
	now := float64(time.Now().UnixNano()) / 1e9
	kept := s.sendTimes[:0]
	for _, t := range s.sendTimes {
		if now-t <= 1.0 {
			kept = append(kept, t)
		}
	}
	s.sendTimes = kept
	return float64(len(kept))
}

func (s *BotSession) noteReal429() {
	s.mu.Lock()
	s.real429++
	s.mu.Unlock()
}

func (s *BotSession) noteSynthetic429() {
	s.mu.Lock()
	s.synth429++
	s.mu.Unlock()
}

// PublicStatus is the /admin/status view of one session.
func (s *BotSession) PublicStatus() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	connectionAge := any(nil)
	if s.connectedSince > 0 {
		connectionAge = round1(wallNow() - s.connectedSince)
	}
	var last429 any
	if s.last429 != nil {
		last429 = s.last429
	}
	var pushHealthy any
	if s.pushHealthy != nil {
		pushHealthy = *s.pushHealthy
	}
	lastErr := s.lastError
	if len(lastErr) > 200 {
		lastErr = lastErr[:200]
	}
	return map[string]any{
		"alias":            s.Alias,
		"state":            string(s.state),
		"connected_since":  s.connectedSince,
		"connection_age_s": connectionAge,
		"queue":            s.queue.Stats(),
		"buckets":          s.guard.Snapshot(),
		"sends_per_s":      s.sendsPerSLocked(),
		"real_429":         s.real429,
		"synthetic_429":    s.synth429,
		"last_429":         last429,
		"last_error":       lastErr,
		"push_url":         nilIfEmpty(s.pushURL),
		"push_healthy":     pushHealthy,
		"on_limit":         s.effectiveOnLimitLocked(),
		"paid_broadcasts":  s.PaidBroadcasts,
	}
}

func (s *BotSession) effectiveOnLimitLocked() string {
	if s.OnLimit != nil {
		return *s.OnLimit
	}
	return "queue"
}

func nilIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func wallNow() float64 { return float64(time.Now().UnixNano()) / 1e9 }

func round1(f float64) float64 { return float64(int64(f*10+0.5)) / 10 }

// Manager owns every bot session and its poller goroutine.
type Manager struct {
	cfg    config.BotAPI
	store  *store.Gateway
	client HTTPClient
	health *health.Storage
	typing *TypingManager

	mu       sync.Mutex
	sessions map[string]*BotSession
}

// NewManager wires the session plane.
func NewManager(cfg config.BotAPI, st *store.Gateway, client HTTPClient, h *health.Storage) *Manager {
	m := &Manager{cfg: cfg, store: st, client: client, health: h, sessions: map[string]*BotSession{}}
	m.typing = NewTypingManager(cfg, client)
	return m
}

// Typing exposes the typing manager (proxy/admin surface).
func (m *Manager) Typing() *TypingManager { return m.typing }

// Sessions returns the live alias→session map (for status/metrics).
func (m *Manager) Sessions() map[string]*BotSession {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]*BotSession, len(m.sessions))
	for k, v := range m.sessions {
		out[k] = v
	}
	return out
}

// Get returns one session.
func (m *Manager) Get(alias string) *BotSession {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sessions[alias]
}

// FindByToken routes token-addressed calls to the owning session.
func (m *Manager) FindByToken(token string) *BotSession {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, s := range m.sessions {
		if s.Token == token {
			return s
		}
	}
	return nil
}

// Attach registers a bot and starts its poller. A rotated token tears
// the old session down first.
func (m *Manager) Attach(ctx context.Context, alias, token string, onLimit *string, paidBroadcasts bool) (*BotSession, error) {
	m.mu.Lock()
	existing, ok := m.sessions[alias]
	m.mu.Unlock()
	if ok {
		if existing.Token == token {
			return existing, nil
		}
		_ = m.Delete(ctx, alias) // token rotated → old poller dies, new one owns
	}

	queue, err := store.OpenQueue(
		m.cfg.Policy.UpdateQueueMax, "drop_oldest", m.cfg.Policy.UpdateTTLS,
		m.cfg.DataDir+"/queue.db", alias, m.cfg.Storage.Durability, m.health)
	if err != nil {
		return nil, err
	}
	s := &BotSession{
		Alias:          alias,
		Token:          token,
		state:          StateConnecting,
		queue:          queue,
		guard:          rateguard.NewGuard(m.cfg.Rate),
		chatNameCache:  map[string]int64{},
		OnLimit:        onLimit,
		PaidBroadcasts: paidBroadcasts,
	}
	s.guard.PaidEnabled = paidBroadcasts
	m.restoreBackoffs(s)

	m.mu.Lock()
	m.sessions[alias] = s
	m.mu.Unlock()

	if err := m.store.RegisterBot(alias, token); err != nil {
		slog.Error("register_bot persist failed", "alias", alias, "err", err)
	}

	pollCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	s.pollCancel = cancel
	go m.poller(pollCtx, s)
	return s, nil
}

// restoreBackoffs re-arms FLOOD_WAIT pauses that outlived a restart.
func (m *Manager) restoreBackoffs(s *BotSession) {
	rows := m.store.LoadBackoffs(s.Alias)
	now := wallNow()
	for _, r := range rows {
		chatID := r.ChatID
		if chatID == 0 {
			s.guard.RestoreBackoff(nil, r.PausedUntil-now)
		} else {
			s.guard.RestoreBackoff(&chatID, r.PausedUntil-now)
		}
	}
	if len(rows) > 0 {
		slog.Info("restored backoff pauses from ledger", "alias", s.Alias, "count", len(rows))
	}
}

// RecordBackoff persists a chat/egress pause (best-effort).
func (m *Manager) RecordBackoff(s *BotSession, chatID *int64, retryAfter float64) {
	m.store.SaveBackoff(s.Alias, chatID, wallNow()+retryAfter)
}

// Pause stops the poller NOW (kills the in-flight long poll).
func (m *Manager) Pause(alias string) bool {
	s := m.Get(alias)
	if s == nil {
		return false
	}
	s.setState(StatePaused)
	m.typing.StopAll(s)
	if s.pollCancel != nil {
		s.pollCancel()
	}
	_ = m.store.SetPaused(alias, true)
	return true
}

// Resume restarts a dead poller or unpauses a live one.
func (m *Manager) Resume(ctx context.Context, alias string) bool {
	s := m.Get(alias)
	if s == nil {
		return false
	}
	_ = m.store.SetPaused(alias, false)
	if s.pollCancel == nil {
		pollCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
		s.pollCancel = cancel
		go m.poller(pollCtx, s)
	}
	s.setState(StateLive)
	return true
}

// Drain stops polling after the current batch.
func (m *Manager) Drain(alias string) bool {
	s := m.Get(alias)
	if s == nil {
		return false
	}
	s.setState(StateDraining)
	m.typing.StopAll(s)
	return true
}

// Delete tears a session down and removes it from the store.
func (m *Manager) Delete(_ context.Context, alias string) bool {
	m.mu.Lock()
	s, ok := m.sessions[alias]
	delete(m.sessions, alias)
	m.mu.Unlock()
	if !ok {
		return false
	}
	s.setState(StateDraining)
	m.typing.StopAll(s)
	if s.pollCancel != nil {
		s.pollCancel()
	}
	if s.pushCancel != nil {
		s.pushCancel()
	}
	s.queue.Close()
	_, _ = m.store.DeleteBot(alias)
	return true
}

// RestoreAll rebuilds sessions from the store after a restart.
// Poller starts are staggered (thundering herd on boot).
func (m *Manager) RestoreAll(ctx context.Context) int {
	n := 0
	rows, err := m.store.ListBots()
	if err != nil {
		return 0
	}
	for _, row := range rows {
		token, ok := m.store.GetToken(row.Alias)
		if !ok {
			continue
		}
		s, err := m.Attach(ctx, row.Alias, token, nil, false)
		if err != nil {
			continue
		}
		if row.Paused {
			s.setState(StatePaused)
		}
		n++
		if row.Alias != rows[len(rows)-1].Alias {
			time.Sleep(time.Duration(rateguard.Jittered(0.25, 0.5) * float64(time.Second)))
		}
	}
	return n
}

// SetPush wires push mode + the consumer healthcheck (every
// pushHealthInterval; failure marks unhealthy — the queue keeps
// growing, Telegram stays).
func (m *Manager) SetPush(ctx context.Context, alias, url string) bool {
	s := m.Get(alias)
	if s == nil {
		return false
	}
	s.mu.Lock()
	s.pushURL = url
	s.pushHealthy = nil
	if s.pushCancel != nil {
		s.pushCancel()
		s.pushCancel = nil
	}
	s.mu.Unlock()
	if url != "" {
		pushCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
		s.pushCancel = cancel
		go m.pushHealthcheck(pushCtx, s)
	}
	return true
}

// PushURL reads the configured push target.
func (s *BotSession) PushURL() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pushURL
}

func (m *Manager) pushHealthcheck(ctx context.Context, s *BotSession) {
	t := time.NewTicker(time.Duration(m.cfg.PushHealthInterval * float64(time.Second)))
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			url := s.PushURL()
			if url == "" {
				s.setPushHealthy(nil)
				continue
			}
			req := &httpRequest{Method: "GET", URL: url}
			ctxTO, cancel := context.WithTimeout(ctx, 5*time.Second)
			req.ctx = ctxTO
			resp, err := m.client.Do(req)
			cancel()
			if err != nil || resp.StatusCode >= 400 {
				v := false
				s.setPushHealthy(&v) // queue grows; never disconnect Telegram
			} else {
				v := true
				s.setPushHealthy(&v)
			}
		}
	}
}

func (s *BotSession) setPushHealthy(v *bool) {
	s.mu.Lock()
	s.pushHealthy = v
	s.mu.Unlock()
}

// Shutdown gracefully stops everything.
func (m *Manager) Shutdown() {
	m.mu.Lock()
	sessions := make([]*BotSession, 0, len(m.sessions))
	for _, s := range m.sessions {
		sessions = append(sessions, s)
	}
	m.mu.Unlock()
	for _, s := range sessions {
		s.setState(StateDraining)
		m.typing.StopAll(s)
		if s.pollCancel != nil {
			s.pollCancel()
		}
		if s.pushCancel != nil {
			s.pushCancel()
		}
		s.queue.Close()
	}
	m.typing.Close()
}

// ── the poller — the ONLY getUpdates caller in the world ──────────

func (m *Manager) poller(ctx context.Context, s *BotSession) {
	backoff := 1.0
	offset := m.store.GetOffset(s.Alias)

	req := &httpRequest{
		Method: "POST",
		URL:    fmt.Sprintf("/bot%s/deleteWebhook", s.Token),
		Body:   map[string]any{"drop_pending_updates": false},
	}
	if _, err := m.client.Do(req); err != nil {
		slog.Warn("deleteWebhook failed", "alias", s.Alias, "err", err)
	}
	s.setState(StateLive)
	s.mu.Lock()
	s.connectedSince = wallNow()
	s.mu.Unlock()
	slog.Info("live", "alias", s.Alias, "offset", offset)

	for {
		if ctx.Err() != nil {
			return
		}
		if st := s.CurrentState(); st == StatePaused || st == StateDraining {
			select {
			case <-ctx.Done():
				return
			case <-time.After(500 * time.Millisecond):
			}
			continue
		}
		body := map[string]any{"offset": offset + 1, "timeout": m.cfg.PollTimeout}
		pollCtx, cancel := context.WithTimeout(ctx, time.Duration((float64(m.cfg.PollTimeout)+10)*float64(time.Second)))
		resp, err := m.client.Do(&httpRequest{
			Method: "POST",
			URL:    fmt.Sprintf("/bot%s/getUpdates", s.Token),
			Body:   body,
			ctx:    pollCtx,
		})
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return // pause/drain/shutdown cancelled us — not an upstream error
			}
			s.setState(StateError)
			s.setError(err.Error())
			slog.Warn("poller error — backing off", "alias", s.Alias, "err", err)
			if !sleepJittered(ctx, backoff) {
				return
			}
			backoff = minF(backoff*2, m.cfg.PollBackoffMax)
			s.setState(StateLive)
			continue
		}
		var data struct {
			Ok          bool             `json:"ok"`
			Description string           `json:"description"`
			Parameters  map[string]any   `json:"parameters"`
			Result      []map[string]any `json:"result"`
		}
		if err := json.Unmarshal(resp.Body, &data); err != nil {
			s.setError(err.Error())
			if !sleepJittered(ctx, backoff) {
				return
			}
			backoff = minF(backoff*2, m.cfg.PollBackoffMax)
			continue
		}
		if !data.Ok {
			if retry := numberOf(data.Parameters["retry_after"]); retry > 0 {
				s.noteReal429()
				s.guard.Report429(nil, retry)
				m.RecordBackoff(s, nil, retry)
				if !sleepJittered(ctx, retry+1.0) {
					return
				}
				continue
			}
			slog.Warn("getUpdates error", "alias", s.Alias, "description", data.Description)
			if !sleepJittered(ctx, backoff) {
				return
			}
			backoff = minF(backoff*2, m.cfg.PollBackoffMax)
			continue
		}
		backoff = 1.0
		if len(data.Result) > 0 {
			// queue FIRST, then persist offset, then loop (spec §6.2)
			if err := s.queue.PushAll(data.Result); err != nil {
				slog.Error("queue push failed", "alias", s.Alias, "err", err)
			}
			for _, u := range data.Result {
				if id := numberOf(u["update_id"]); int64(id) > offset {
					offset = int64(id)
				}
			}
			_ = m.store.SaveOffset(s.Alias, offset)
			// auto typing indicator (spec §10.4) — covers pull and push
			m.typing.OnUpdates(s, data.Result)
			if url := s.PushURL(); url != "" {
				// official webhook shape: ONE Update per POST — the Update
				// object IS the body; webhook libraries connect as-is
				for _, u := range data.Result {
					go m.pushToConsumer(url, u)
				}
			}
		} else if s.CurrentState() == StateError {
			// recover from a transient error state; never stomp a
			// pause/drain that landed mid-poll
			s.setState(StateLive)
		}
	}
}

func (m *Manager) pushToConsumer(url string, update map[string]any) {
	raw, err := json.Marshal(update)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, _ = m.client.Do(&httpRequest{Method: "POST", URL: url, RawBody: raw, ctx: ctx})
}

func (s *BotSession) setError(msg string) {
	s.mu.Lock()
	s.lastError = msg
	s.mu.Unlock()
}

func sleepJittered(ctx context.Context, base float64) bool {
	d := time.Duration(rateguard.Jittered(base, 60.0) * float64(time.Second))
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

func minF(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func numberOf(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case int:
		return float64(n)
	case int64:
		return float64(n)
	case string:
		var f float64
		if _, err := fmt.Sscanf(n, "%g", &f); err == nil {
			return f
		}
	}
	return 0
}

// sortedAliases is a stable iteration order for metrics.
func sortedAliases(m map[string]*BotSession) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
