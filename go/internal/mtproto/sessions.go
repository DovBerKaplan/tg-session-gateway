package mtproto

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/DovBerKaplan/tg-session-gateway/go/internal/config"
	"github.com/DovBerKaplan/tg-session-gateway/go/internal/health"
	"github.com/DovBerKaplan/tg-session-gateway/go/internal/lock"
	"github.com/DovBerKaplan/tg-session-gateway/go/internal/rateguard"
	"github.com/DovBerKaplan/tg-session-gateway/go/internal/store"
)

// terminalMarkers mean the session itself is dead/revoked/duplicated —
// retrying only makes Telegram burn the session harder.
var terminalMarkers = []string{
	"AUTH_KEY_DUPLICATED", "AUTH_KEY_INVALID", "AuthKeyUnregistered",
	"AuthKeyInvalid", "SessionDuplicated", "SESSION_REVOKED",
	"SessionRevoked", "ACCESS_TOKEN_EXPIRED", "ACCESS_TOKEN_INVALID",
	"UNAUTHORIZED",
}

// ClassifyStartError buckets a start failure: flood (wait it out),
// terminal (quarantine) or retry (transient, exponential backoff).
func ClassifyStartError(err error) string {
	if rateguard.ParseFloodWait(err.Error()) != nil {
		return "flood"
	}
	msg := err.Error()
	for _, m := range terminalMarkers {
		if strings.Contains(msg, m) {
			return "terminal"
		}
	}
	return "retry"
}

// SessionState is the lifecycle state.
type SessionState string

const (
	StateConnecting SessionState = "connecting"
	StateLive       SessionState = "live"
	StatePaused     SessionState = "paused"
	StateDraining   SessionState = "draining"
	StateError      SessionState = "error"
)

// Consumer is a connected WS consumer (app replica).
type Consumer struct {
	ID          string
	Send        func(string) error
	ConnectedAt time.Time
	LastSeen    time.Time
}

// Session is one bot token owned exclusively by this sidecar.
type Session struct {
	Alias string
	Token string

	mu    sync.Mutex
	state SessionState
	Guard *rateguard.MTGuard

	clientFactory func() Client
	client        Client

	ConnectedSince   float64
	LastError        string
	RealFloodWait    int
	Synthetic429     int
	LastFloodWait    map[string]any

	// supervisor state
	terminalError   bool
	nextRetryAt     float64
	stopSupervisor  chan struct{}
	reconnects      int
	startAttempts   int
	LastPingOKAt    float64 // monotonic
	pingAt          float64 // monotonic — last ping attempt
	LastUpdateAt    float64 // wall clock
	IdempotencyClaims int
	PersistFailures int
	SendPaused      bool

	sessionLock *lock.Lock

	// update routing
	updateCh      chan map[string]any
	consumers     []*Consumer
	nextSeq       int64
	activeConsumer string

	// write-through store + fast-path idempotency cache
	st            *store.MT
	h             *health.Storage
	seenIdemKeys  map[string]float64

	firstSendAt float64
}

// Manager owns every session and its supervisor goroutine.
type Manager struct {
	cfg config.MTProto
	st  *store.MT
	h   *health.Storage

	mu       sync.Mutex
	sessions map[string]*Session

	// ClientFactory builds the Telegram client; tests inject fakes.
	ClientFactory func(alias string) Client
}

// NewManager wires the session plane.
func NewManager(cfg config.MTProto, st *store.MT, h *health.Storage) *Manager {
	m := &Manager{cfg: cfg, st: st, h: h, sessions: map[string]*Session{}}
	m.ClientFactory = func(alias string) Client {
		return newGotdClient(cfg, alias, st)
	}
	return m
}

// Get returns one session.
func (m *Manager) Get(alias string) *Session {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sessions[alias]
}

// FindByToken routes token-addressed calls.
func (m *Manager) FindByToken(token string) *Session {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, s := range m.sessions {
		if s.Token == token {
			return s
		}
	}
	return nil
}

// Sessions returns all sessions (status/metrics).
func (m *Manager) Sessions() map[string]*Session {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]*Session, len(m.sessions))
	for k, v := range m.sessions {
		out[k] = v
	}
	return out
}

// Register adds a session; its supervisor starts the client. A rotated
// token tears the old session down first.
func (m *Manager) Register(ctx context.Context, alias, token string) (*Session, error) {
	m.mu.Lock()
	existing, ok := m.sessions[alias]
	m.mu.Unlock()
	if ok {
		if existing.Token == token {
			return existing, nil
		}
		_ = m.Unregister(ctx, alias)
	}
	if err := m.st.Register(alias, token); err != nil {
		return nil, err
	}
	s := &Session{
		Alias:        alias,
		Token:        token,
		state:        StateConnecting,
		Guard:        rateguard.NewMTGuard(m.cfg.Rate, m.cfg.MTRate),
		clientFactory: func() Client { return m.ClientFactory(alias) },
		updateCh:     make(chan map[string]any, m.cfg.Policy.UpdateQueueMax),
		st:           m.st,
		h:            m.h,
		seenIdemKeys: map[string]float64{},
		stopSupervisor: make(chan struct{}),
	}
	m.mu.Lock()
	m.sessions[alias] = s
	m.mu.Unlock()
	go m.supervise(s)
	return s, nil
}

// ── the supervisor — owns one session for life ────────────────────

func (m *Manager) supervise(s *Session) {
	backoff := 1.0
	pingInterval := 60.0
	for {
		select {
		case <-s.stopSupervisor:
			return
		default:
		}
		if s.IsTerminal() {
			return
		}
		if st := s.CurrentState(); st == StatePaused || st == StateDraining {
			sleepCtx(s.stopSupervisor, 2*time.Second)
			continue
		}
		s.mu.Lock()
		connected := s.client != nil && s.client.IsConnected() && s.state == StateLive
		s.mu.Unlock()
		if connected {
			// active liveness: IsConnected lies through half-open TCP —
			// ping Telegram for real; a stuck ping forces a reconnect
			if monoNow()-s.lastPingAt() > pingInterval {
				s.setPingAt()
				pingCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				err := s.client.Ping(pingCtx)
				cancel()
				if err != nil {
					slog.Warn("liveness ping stuck — half-open suspected, forcing reconnect", "alias", s.Alias, "err", err)
					dctx, dcancel := context.WithTimeout(context.Background(), 10*time.Second)
					_ = s.client.Disconnect(dctx)
					dcancel()
					continue
				}
				s.mu.Lock()
				s.LastPingOKAt = monoNow()
				s.mu.Unlock()
			}
			sleepCtx(s.stopSupervisor, 5*time.Second)
			continue
		}
		if monoNow() < s.retryAt() {
			sleepCtx(s.stopSupervisor, 500*time.Millisecond)
			continue
		}
		if !s.lockHeld() {
			if !m.acquireLock(s) {
				s.setState(StateError)
				s.setError("session lock refused: another process holds '" + s.Alias +
					"'. A second connection would trigger AUTH_KEY_DUPLICATED.")
				s.setRetryAt(monoNow() + 60.0) // don't hammer the holder
				continue
			}
		}
		wasLive := s.ConnectedSince > 0
		s.setState(StateConnecting)
		s.mu.Lock()
		s.startAttempts++
		s.mu.Unlock()
		if err := m.startClient(s); err != nil {
			kind := ClassifyStartError(err)
			switch kind {
			case "terminal":
				s.setState(StateError)
				s.mu.Lock()
				s.terminalError = true
				s.LastError = "terminal (quarantined): " + err.Error()
				s.mu.Unlock()
				slog.Error("session quarantined — key revoked/duplicated, admin action required", "alias", s.Alias, "err", err)
				continue
			case "flood":
				if info := rateguard.ParseFloodWait(err.Error()); info != nil {
					wait := rateguard.Jittered(float64(info.Wait)+60.0, 60.0)
					s.setState(StateConnecting)
					s.setError(fmt.Sprintf("auth flood: retrying in %ds", int64(wait)))
					slog.Warn("auth flood on start — sleeping before retry", "alias", s.Alias, "wait_s", int64(wait))
					s.setRetryAt(monoNow() + wait)
					continue
				}
			default:
				s.setState(StateError)
				s.setError(fmt.Sprintf("start failed (retry in %.0fs): %s", backoff, err))
				slog.Warn("start failed — backing off", "alias", s.Alias, "wait_s", int64(backoff), "err", err)
				s.setRetryAt(monoNow() + backoff)
				backoff = minF(backoff*2, 300.0)
				continue
			}
		}
		backoff = 1.0
		if wasLive {
			s.mu.Lock()
			s.reconnects++
			s.mu.Unlock()
			slog.Info("reconnected", "alias", s.Alias)
		}
	}
}

// startClient connects the client under the already-held lock. Raises
// on failure — the supervisor classifies and decides.
func (m *Manager) startClient(s *Session) error {
	s.mu.Lock()
	old := s.client
	factory := s.clientFactory
	s.mu.Unlock()
	if old != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = old.Disconnect(ctx)
		cancel()
	}
	client := factory()
	// route updates into the session BEFORE connecting
	if gc, ok := client.(*gotdClient); ok {
		gc.updateHandler = s.pushUpdate
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	err := client.Connect(ctx)
	cancel()
	if err != nil {
		m.releaseLock(s)
		return err
	}
	s.mu.Lock()
	s.client = client
	s.state = StateLive
	s.ConnectedSince = wallNow()
	s.mu.Unlock()

	s.sessionLock.StartHeartbeat()

	info, err := client.Me(context.Background())
	if err == nil {
		slog.Info("live", "alias", s.Alias, "username", info.Username, "id", info.ID)
	} else {
		slog.Info("live", "alias", s.Alias)
	}

	// warm the peer cache: a fresh session holds no access hashes, so
	// first sends fail with "Peer id invalid" until entities are seen
	wctx, wcancel := context.WithTimeout(context.Background(), 30*time.Second)
	if err := client.WarmPeers(wctx); err != nil {
		slog.Warn("peer warm-up failed (non-fatal)", "alias", s.Alias, "err", err)
	}
	wcancel()

	// restore persisted pts/qts + replay the unacked backlog
	if state := m.st.GetState(s.Alias); state.PTS > 0 {
		slog.Info("restored state", "alias", s.Alias, "pts", state.PTS, "qts", state.QTS)
	}
	backlog := m.st.QueuePull(s.Alias, 0, 5000)
	if len(backlog) > 0 {
		maxSeq := int64(0)
		s.mu.Lock()
		for _, u := range backlog {
			if seq, ok := anyToInt64(u["_gw_seq"]); ok && seq > s.nextSeq {
				s.nextSeq = seq
			}
		}
		maxSeq = s.nextSeq
		for _, u := range backlog {
			select {
			case s.updateCh <- u:
			default: // belt full — drained by TTL housekeeping
			}
		}
		s.mu.Unlock()
		slog.Info("replayed unacked updates", "alias", s.Alias, "count", len(backlog), "seq_to", maxSeq)
	}
	return nil
}

func (m *Manager) acquireLock(s *Session) bool {
	l := &lock.Lock{Alias: s.Alias, LockDir: m.cfg.SessionDir}
	if !l.Acquire() {
		return false
	}
	s.mu.Lock()
	s.sessionLock = l
	s.mu.Unlock()
	slog.Info("lock fence token", "alias", s.Alias, "token", l.Token)
	return true
}

func (m *Manager) releaseLock(s *Session) {
	s.mu.Lock()
	l := s.sessionLock
	s.sessionLock = nil
	s.mu.Unlock()
	if l != nil {
		l.Release()
	}
}

// pushUpdate is the write-through path from Telegram to consumers:
// persist FIRST (survives kill -9), then fan out to memory + WS.
func (s *Session) pushUpdate(update map[string]any) {
	s.mu.Lock()
	s.nextSeq++
	update["_gw_seq"] = s.nextSeq
	update["_gw_enqueued_at"] = wallNow()
	s.LastUpdateAt = wallNow()
	st := s.st
	h := s.h
	s.mu.Unlock()

	payload, err := json.Marshal(update)
	if err != nil {
		// poison update: capture for inspection instead of losing it
		st.DeadLetter(s.Alias, fmt.Sprintf("%v", update)[:min(10000, len(fmt.Sprintf("%v", update)))], "unserializable: "+err.Error())
		slog.Error("unserializable update captured to dead letters", "alias", s.Alias, "err", err)
		return
	}
	seq := int64(0)
	if v, ok := anyToInt64(update["_gw_seq"]); ok {
		seq = v
	}
	if err := st.QueuePush(s.Alias, seq, string(payload)); err != nil {
		s.mu.Lock()
		s.PersistFailures++
		s.mu.Unlock()
		if h != nil {
			h.NotePersistFailure("queue.push:"+s.Alias, err)
		}
		slog.Error("update persistence FAILED (update lives in memory only, lost on restart)", "alias", s.Alias, "err", err)
	}

	select {
	case s.updateCh <- update:
	default:
		// drop oldest
		select {
		case <-s.updateCh:
		default:
		}
		select {
		case s.updateCh <- update:
		default:
		}
	}

	s.mu.Lock()
	consumers := append([]*Consumer{}, s.consumers...)
	s.mu.Unlock()
	raw, _ := json.Marshal(update)
	for _, c := range consumers {
		if c.Send != nil {
			if err := c.Send(string(raw)); err != nil {
				slog.Warn("WS push failed", "alias", s.Alias, "consumer", c.ID, "err", err)
			}
		}
	}
}

// ── idempotency (E2) ───────────────────────────────────────────────

// CheckIdempotency: claimed/reclaimed (proceed) or replay (suppress).
// The STORE decides atomically; the in-memory map is a fast path.
func (s *Session) CheckIdempotency(key string) string {
	now := wallNow()
	s.mu.Lock()
	if ts, ok := s.seenIdemKeys[key]; ok && now-ts < 300 {
		s.mu.Unlock()
		return "replay"
	}
	s.mu.Unlock()
	verdict := s.st.IdempotencyClaim(s.Alias, key)
	s.mu.Lock()
	if verdict == "replay" {
		s.seenIdemKeys[key] = now
	} else {
		s.IdempotencyClaims++
	}
	cutoff := now - 300
	for k, t := range s.seenIdemKeys {
		if t < cutoff {
			delete(s.seenIdemKeys, k)
		}
	}
	s.mu.Unlock()
	return verdict
}

// NoteIdempotencyDone caches a finished key.
func (s *Session) NoteIdempotencyDone(key string) {
	s.mu.Lock()
	s.seenIdemKeys[key] = wallNow()
	s.mu.Unlock()
}

// ── lifecycle ops ──────────────────────────────────────────────────

// Pause freezes the session (supervisor idles).
func (m *Manager) Pause(alias string) bool {
	s := m.Get(alias)
	if s == nil {
		return false
	}
	s.setState(StatePaused)
	m.st.SetPaused(alias, true)
	return true
}

// Resume unpauses.
func (m *Manager) Resume(alias string) bool {
	s := m.Get(alias)
	if s == nil {
		return false
	}
	m.st.SetPaused(alias, false)
	s.setState(StateLive)
	return true
}

// Drain stops processing new updates.
func (m *Manager) Drain(alias string) bool {
	s := m.Get(alias)
	if s == nil {
		return false
	}
	s.setState(StateDraining)
	time.Sleep(2 * time.Second) // let consumers pull the remainder
	return true
}

// Restart clears the retry cooldown (escalation rung 1) — terminal
// sessions are refused: a revoked key needs unregister + re-auth.
func (m *Manager) Restart(alias string) bool {
	s := m.Get(alias)
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.terminalError {
		return false
	}
	s.nextRetryAt = 0
	return true
}

// PauseSending freezes outbound sends but keeps receiving (E4).
func (m *Manager) PauseSending(alias string) bool {
	s := m.Get(alias)
	if s == nil {
		return false
	}
	s.mu.Lock()
	s.SendPaused = true
	s.mu.Unlock()
	return true
}

// ResumeSending lifts the E4 pause.
func (m *Manager) ResumeSending(alias string) bool {
	s := m.Get(alias)
	if s == nil {
		return false
	}
	s.mu.Lock()
	s.SendPaused = false
	s.mu.Unlock()
	return true
}

// Unregister stops a session and removes it, lock released.
func (m *Manager) Unregister(ctx context.Context, alias string) bool {
	m.mu.Lock()
	s, ok := m.sessions[alias]
	delete(m.sessions, alias)
	m.mu.Unlock()
	if !ok {
		return false
	}
	s.setState(StateDraining)
	close(s.stopSupervisor)
	s.mu.Lock()
	client := s.client
	s.mu.Unlock()
	if client != nil {
		cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = client.Disconnect(cctx)
		cancel()
	}
	m.releaseLock(s)
	m.st.Delete(alias)
	return true
}

// RestoreAll rebuilds sessions after restart (boot janitor first).
func (m *Manager) RestoreAll(ctx context.Context) int {
	if removed := lock.SweepDir(m.cfg.SessionDir); removed > 0 {
		slog.Info("boot janitor: removed stale locks", "count", removed)
	}
	n := 0
	rows, err := m.st.ListSessions()
	if err != nil {
		return 0
	}
	for _, row := range rows {
		token, ok := m.st.GetToken(row.Alias)
		if !ok {
			continue
		}
		if _, err := m.Register(ctx, row.Alias, token); err != nil {
			continue
		}
		if row.Paused {
			if s := m.Get(row.Alias); s != nil {
				s.setState(StatePaused)
			}
		}
		n++
		if row.Alias != rows[len(rows)-1].Alias {
			time.Sleep(time.Duration(rateguard.Jittered(0.25, 0.5) * float64(time.Second)))
		}
	}
	return n
}

// PersistStates snapshots pts/qts/date/seq for every live session.
func (m *Manager) PersistStates() {
	for _, s := range m.Sessions() {
		if s.CurrentState() != StateLive {
			continue
		}
		s.mu.Lock()
		client := s.client
		s.mu.Unlock()
		if client == nil {
			continue
		}
		if gc, ok := client.(*gotdClient); ok {
			st := gc.UpdateState()
			if st.PTS != 0 || st.QTS != 0 {
				_ = m.st.SaveState(s.Alias, st)
			}
		}
	}
}

// Shutdown stops everything, locks released.
func (m *Manager) Shutdown() {
	for alias, s := range m.Sessions() {
		s.setState(StateDraining)
		select {
		case <-s.stopSupervisor:
			// already closed by Unregister
		default:
			close(s.stopSupervisor)
		}
		s.mu.Lock()
		client := s.client
		s.mu.Unlock()
		if client != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = client.Disconnect(ctx)
			cancel()
		}
		m.releaseLock(s)
		_ = alias
	}
	slog.Info("all MTProto sessions shut down (locks released)")
}

// ── session state helpers ─────────────────────────────────────────

// CurrentState reads the lifecycle state.
func (s *Session) CurrentState() SessionState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

func (s *Session) setState(st SessionState) {
	s.mu.Lock()
	s.state = st
	s.mu.Unlock()
}

func (s *Session) setError(msg string) {
	s.mu.Lock()
	s.LastError = msg
	s.mu.Unlock()
}

func (s *Session) setRetryAt(t float64) {
	s.mu.Lock()
	s.nextRetryAt = t
	s.mu.Unlock()
}

func (s *Session) lastPingAt() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pingAt
}

func (s *Session) retryAt() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nextRetryAt
}

// lockHeld reads the session-lock ownership under the mutex (Restart
// and the supervisor run on different goroutines).
func (s *Session) lockHeld() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessionLock != nil && s.sessionLock.Held()
}

func (s *Session) setPingAt() {
	s.mu.Lock()
	s.pingAt = monoNow()
	s.mu.Unlock()
}

// IsTerminal reports quarantine.
func (s *Session) IsTerminal() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.terminalError
}

// SendPausedNow reads the E4 flag.
func (s *Session) SendPausedNow() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.SendPaused
}

// LiveClient returns the live client (nil unless connected).
func (s *Session) LiveClient() Client {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.client != nil && s.state == StateLive {
		return s.client
	}
	return nil
}

// AddConsumer attaches a WS consumer.
func (s *Session) AddConsumer(c *Consumer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.consumers = append(s.consumers, c)
	s.activeConsumer = c.ID
}

// RemoveConsumer detaches by id.
func (s *Session) RemoveConsumer(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	kept := s.consumers[:0]
	for _, c := range s.consumers {
		if c.ID != id {
			kept = append(kept, c)
		}
	}
	s.consumers = kept
	if s.activeConsumer == id {
		if len(kept) > 0 {
			s.activeConsumer = kept[len(kept)-1].ID
		} else {
			s.activeConsumer = ""
		}
	}
}

// NoteFirstSend records the deploy-to-first-message SLO anchor.
func (s *Session) NoteFirstSend() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.firstSendAt == 0 && len(s.consumers) > 0 {
		s.firstSendAt = wallNow()
	}
}

// DeployToFirstMessageS reports the E7 SLO (0 when unset).
func (s *Session) DeployToFirstMessageS() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.deployToFirstMessageSLocked()
}

// deployToFirstMessageSLocked is DeployToFirstMessageS with s.mu held
// (Stats calls it mid-snapshot).
func (s *Session) deployToFirstMessageSLocked() float64 {
	if s.firstSendAt == 0 || len(s.consumers) == 0 {
		return 0
	}
	return s.firstSendAt - float64(s.consumers[len(s.consumers)-1].ConnectedAt.UnixNano())/1e9
}

// Stats is the /v1/admin/status view.
func (s *Session) Stats() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	connectionAge := any(nil)
	if s.ConnectedSince > 0 {
		connectionAge = round1(wallNow() - s.ConnectedSince)
	}
	var lastFlood any
	if s.LastFloodWait != nil {
		lastFlood = s.LastFloodWait
	}
	lastUpdateAge := any(nil)
	if s.LastUpdateAt > 0 {
		lastUpdateAge = round1(wallNow() - s.LastUpdateAt)
	}
	pingAge := any(nil)
	if s.LastPingOKAt > 0 {
		pingAge = round1(monoNow() - s.LastPingOKAt)
	}
	slo := any(nil)
	if v := s.deployToFirstMessageSLocked(); v > 0 {
		slo = round1(v)
	}
	lastErr := s.LastError
	if len(lastErr) > 200 {
		lastErr = lastErr[:200]
	}
	return map[string]any{
		"alias":            s.Alias,
		"state":            string(s.state),
		"connection_age_s": connectionAge,
		"queue_depth":      len(s.updateCh),
		"consumers":        len(s.consumers),
		"active_consumer":  nilIfEmptyStr(s.activeConsumer),
		"real_flood_wait":  s.RealFloodWait,
		"synthetic_429":    s.Synthetic429,
		"last_flood_wait":  lastFlood,
		"last_error":       lastErr,
		"send_paused":      s.SendPaused,
		"terminal_error":   s.terminalError,
		"reconnects":       s.reconnects,
		"start_attempts":   s.startAttempts,
		"persist_failures": s.PersistFailures,
		"idempotency_claims": s.IdempotencyClaims,
		"seconds_since_last_update": lastUpdateAge,
		"seconds_since_ping_ok":     pingAge,
		"deploy_to_first_message_s": slo,
	}
}

// PruneExpiredUpdates enforces GW_UPDATE_TTL_S on the in-memory queue.
func (s *Session) PruneExpiredUpdates(ttlS float64) int {
	if ttlS <= 0 {
		return 0
	}
	cutoff := wallNow() - ttlS
	kept, dropped := []map[string]any{}, 0
	for {
		select {
		case u := <-s.updateCh:
			if at, ok := anyToFloat(u["_gw_enqueued_at"]); ok && at < cutoff {
				dropped++
			} else {
				kept = append(kept, u)
			}
			continue
		default:
		}
		break
	}
	for _, u := range kept {
		select {
		case s.updateCh <- u:
		default:
		}
	}
	return dropped
}

func nilIfEmptyStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func anyToInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case float64:
		return int64(n), true
	}
	return 0, false
}

func anyToFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int64:
		return float64(n), true
	case int:
		return float64(n), true
	}
	return 0, false
}

func monoNow() float64 { return float64(time.Now().UnixNano()) / 1e9 }
func wallNow() float64 { return float64(time.Now().UnixNano()) / 1e9 }

func round1(f float64) float64 { return float64(int64(f*10+0.5)) / 10 }

func minF(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func sleepCtx(stop <-chan struct{}, d time.Duration) {
	select {
	case <-stop:
	case <-time.After(d):
	}
}
