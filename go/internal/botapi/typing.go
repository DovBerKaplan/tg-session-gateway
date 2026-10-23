package botapi

import (
	"context"
	"encoding/json"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/DovBerKaplan/tg-session-gateway/go/internal/config"
)

// settingFields drive the three-layer resolution (per-bot override →
// runtime global → env boot defaults).
var settingFields = []string{"enabled", "private_only", "interval_s", "timeout_s", "timeout_text"}

// RuntimeTypingSettings is the gateway-wide layer, live-mutable via the
// admin API.
type RuntimeTypingSettings struct {
	mu          sync.Mutex
	Enabled     bool
	PrivateOnly bool
	IntervalS   float64
	TimeoutS    float64
	TimeoutText string
}

func settingsFromConfig(c config.TypingConfig) *RuntimeTypingSettings {
	return &RuntimeTypingSettings{
		Enabled: c.Enabled, PrivateOnly: c.PrivateOnly,
		IntervalS: c.IntervalS, TimeoutS: c.TimeoutS, TimeoutText: c.TimeoutText,
	}
}

// Apply patches non-nil fields.
func (r *RuntimeTypingSettings) Apply(patch TypingPatch) map[string]any {
	r.mu.Lock()
	defer r.mu.Unlock()
	if patch.Enabled != nil {
		r.Enabled = *patch.Enabled
	}
	if patch.PrivateOnly != nil {
		r.PrivateOnly = *patch.PrivateOnly
	}
	if patch.IntervalS != nil {
		r.IntervalS = *patch.IntervalS
	}
	if patch.TimeoutS != nil {
		r.TimeoutS = *patch.TimeoutS
	}
	if patch.TimeoutText != nil {
		r.TimeoutText = *patch.TimeoutText
	}
	return r.snapshotLocked()
}

func (r *RuntimeTypingSettings) snapshotLocked() map[string]any {
	return map[string]any{
		"enabled": r.Enabled, "private_only": r.PrivateOnly,
		"interval_s": r.IntervalS, "timeout_s": r.TimeoutS, "timeout_text": r.TimeoutText,
	}
}

// Snapshot is the GET view.
func (r *RuntimeTypingSettings) Snapshot() map[string]any {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.snapshotLocked()
}

// TypingPatch is a settings update (nil fields stay unchanged).
type TypingPatch struct {
	Enabled     *bool    `json:"enabled"`
	PrivateOnly *bool    `json:"private_only"`
	IntervalS   *float64 `json:"interval_s"`
	TimeoutS    *float64 `json:"timeout_s"`
	TimeoutText *string  `json:"timeout_text"`
}

type activeLoop struct {
	deadline float64 // monotonic; +Inf after cancel_timeout
	notify   bool
	cancel   context.CancelFunc
}

// TypingManager owns every live typing loop: re-sends sendChatAction
// every interval straight upstream WITHOUT consuming rate-guard tokens
// (the indicator must never steal capacity from the real reply).
type TypingManager struct {
	cfg    config.BotAPI
	client HTTPClient
	global *RuntimeTypingSettings

	mu       sync.Mutex
	active   map[string]map[int64]*activeLoop // alias → chat → loop
	cbToChat map[string][2]any                // callback id → (alias, chat)
	closed   bool
}

// NewTypingManager builds the manager from boot config.
func NewTypingManager(cfg config.BotAPI, client HTTPClient) *TypingManager {
	return &TypingManager{
		cfg: cfg, client: client,
		global:   settingsFromConfig(cfg.Typing),
		active:   map[string]map[int64]*activeLoop{},
		cbToChat: map[string][2]any{},
	}
}

// Settings exposes the runtime global layer.
func (t *TypingManager) Settings() *RuntimeTypingSettings { return t.global }

// effective resolves the three layers for one session.
func (t *TypingManager) effective(s *BotSession) map[string]any {
	g := t.global.Snapshot()
	s.typMu.Lock()
	defer s.typMu.Unlock()
	out := map[string]any{}
	for _, name := range settingFields {
		switch name {
		case "enabled":
			if s.typingEnabled != nil {
				out[name] = *s.typingEnabled
			} else {
				out[name] = g[name]
			}
		case "private_only":
			if s.typingPrivateOnly != nil {
				out[name] = *s.typingPrivateOnly
			} else {
				out[name] = g[name]
			}
		case "interval_s":
			if s.typingIntervalS != nil {
				out[name] = *s.typingIntervalS
			} else {
				out[name] = g[name]
			}
		case "timeout_s":
			if s.typingTimeoutS != nil {
				out[name] = *s.typingTimeoutS
			} else {
				out[name] = g[name]
			}
		case "timeout_text":
			if s.typingTimeoutText != nil {
				out[name] = *s.typingTimeoutText
			} else {
				out[name] = g[name]
			}
		}
	}
	return out
}

// StatusFor is the per-session admin view.
func (t *TypingManager) StatusFor(s *BotSession) map[string]any {
	out := t.effective(s)
	s.typMu.Lock()
	overridden := []string{}
	if s.typingEnabled != nil {
		overridden = append(overridden, "enabled")
	}
	if s.typingPrivateOnly != nil {
		overridden = append(overridden, "private_only")
	}
	if s.typingIntervalS != nil {
		overridden = append(overridden, "interval_s")
	}
	if s.typingTimeoutS != nil {
		overridden = append(overridden, "timeout_s")
	}
	if s.typingTimeoutText != nil {
		overridden = append(overridden, "timeout_text")
	}
	s.typMu.Unlock()
	out["overridden"] = overridden
	out["active_chats"] = t.ActiveCount(s.Alias)
	return out
}

// ActiveCount is the live typing-loop count for one alias.
func (t *TypingManager) ActiveCount(alias string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.active[alias])
}

// OnUpdates triggers on every user action that awaits a bot reply:
// message, edited_message, callback_query. Private chats only by
// default; a hard-paused chat never opens a session (the bot cannot
// answer, so it must not pretend to type).
func (t *TypingManager) OnUpdates(s *BotSession, updates []map[string]any) {
	eff := t.effective(s)
	if enabled, _ := eff["enabled"].(bool); !enabled {
		return
	}
	for _, u := range updates {
		chatID, ok := chatOfUpdate(u)
		if !ok {
			continue
		}
		if privateOnly, _ := eff["private_only"].(bool); privateOnly && chatID <= 0 {
			continue
		}
		if cbq, ok := u["callback_query"].(map[string]any); ok && cbq["id"] != nil {
			t.mu.Lock()
			t.cbToChat[toStr(cbq["id"])] = [2]any{s.Alias, chatID}
			t.mu.Unlock()
		}
		if s.Guard().ChatSendState(&chatID) == "paused" {
			continue
		}
		timeoutS, _ := eff["timeout_s"].(float64)
		t.start(s, chatID, timeoutS)
	}
}

// chatOfUpdate extracts the chat id from message/edited_message/
// callback_query.message shapes.
func chatOfUpdate(update map[string]any) (int64, bool) {
	msg, _ := update["message"].(map[string]any)
	if msg == nil {
		msg, _ = update["edited_message"].(map[string]any)
	}
	if msg == nil {
		if cbq, ok := update["callback_query"].(map[string]any); ok {
			msg, _ = cbq["message"].(map[string]any)
		}
	}
	if msg == nil {
		return 0, false
	}
	chat, ok := msg["chat"].(map[string]any)
	if !ok {
		return 0, false
	}
	switch cid := chat["id"].(type) {
	case float64:
		return int64(cid), true
	case int64:
		return cid, true
	case int:
		return int64(cid), true
	}
	return 0, false
}

func toStr(v any) string {
	switch s := v.(type) {
	case string:
		return s
	case float64:
		return json.Number(itoa64(int64(s))).String()
	case json.Number:
		return s.String()
	}
	return ""
}

func itoa64(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [24]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func (t *TypingManager) start(s *BotSession, chatID int64, timeoutS float64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return
	}
	perBot := t.active[s.Alias]
	if perBot == nil {
		perBot = map[int64]*activeLoop{}
		t.active[s.Alias] = perBot
	}
	if rec, ok := perBot[chatID]; ok && rec.cancel != nil {
		// new user action = new work: push the deadline out — but never
		// re-arm a timeout the consumer cancelled
		if rec.notify {
			rec.deadline = monoNow() + timeoutS
		}
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	rec := &activeLoop{deadline: monoNow() + timeoutS, notify: true, cancel: cancel}
	perBot[chatID] = rec
	go t.loop(ctx, s, chatID, rec)
}

func monoNow() float64 { return float64(time.Now().UnixNano()) / 1e9 }

func (t *TypingManager) loop(ctx context.Context, s *BotSession, chatID int64, rec *activeLoop) {
	defer t.cleanup(s.Alias, chatID)
	for {
		eff := t.effective(s) // live: admin changes apply now
		if enabled, _ := eff["enabled"].(bool); !enabled {
			return
		}
		if privateOnly, _ := eff["private_only"].(bool); privateOnly && chatID <= 0 {
			return // excluded mid-flight by a settings change
		}
		state := s.Guard().ChatSendState(&chatID)
		if state == "paused" {
			slog.Info("typing stopped: chat is rate-paused", "alias", s.Alias, "chat", chatID)
			return
		}
		now := monoNow()
		if now >= rec.deadline {
			t.onTimeout(s, chatID, rec, eff)
			return
		}
		if state == "ok" {
			t.beat(s, chatID)
		}
		// "empty": skip this beat; typing resumes when the bucket
		// refills — the queued reply goes out then too
		interval, _ := eff["interval_s"].(float64)
		if interval <= 0 {
			interval = 4.5
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(interval * float64(time.Second))):
		}
	}
}

func (t *TypingManager) beat(s *BotSession, chatID int64) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := t.client.Do(&httpRequest{
		Method: "POST",
		URL:    "/bot" + s.Token + "/sendChatAction",
		Body:   map[string]any{"chat_id": chatID, "action": "typing"},
		ctx:    ctx,
	})
	if err != nil {
		slog.Warn("typing beat failed", "alias", s.Alias, "chat", chatID, "err", err)
	}
}

func (t *TypingManager) onTimeout(s *BotSession, chatID int64, rec *activeLoop, eff map[string]any) {
	// the notice goes to PRIVATE chats only — groups stay silent
	if chatID <= 0 || !rec.notify {
		return
	}
	text, _ := eff["timeout_text"].(string)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := t.client.Do(&httpRequest{
		Method: "POST",
		URL:    "/bot" + s.Token + "/sendMessage",
		Body:   map[string]any{"chat_id": chatID, "text": text},
		ctx:    ctx,
	})
	if err != nil {
		slog.Warn("timeout notice failed", "alias", s.Alias, "chat", chatID, "err", err)
	}
}

// Stop cancels the loop for one chat (a consumer write IS the reply).
func (t *TypingManager) Stop(s *BotSession, chatID *int64) {
	if chatID == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if rec, ok := t.active[s.Alias][*chatID]; ok && rec.cancel != nil {
		rec.cancel()
	}
}

// StopAll cancels every loop for one session (pause/drain/delete).
func (t *TypingManager) StopAll(s *BotSession) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, rec := range t.active[s.Alias] {
		if rec.cancel != nil {
			rec.cancel()
		}
	}
	delete(t.active, s.Alias)
	t.purgeCallbacksLocked(s.Alias)
}

// StopByCallbackID ends the session the button press started.
func (t *TypingManager) StopByCallbackID(s *BotSession, callbackQueryID any) {
	if callbackQueryID == nil {
		return
	}
	t.mu.Lock()
	target, ok := t.cbToChat[toStr(callbackQueryID)]
	delete(t.cbToChat, toStr(callbackQueryID))
	t.mu.Unlock()
	if !ok {
		return
	}
	if alias, _ := target[0].(string); alias == s.Alias {
		if chatID, ok := target[1].(int64); ok {
			t.Stop(s, &chatID)
		}
	}
}

// CancelTimeout suppresses the timeout notice for this chat's session;
// typing continues until a stop signal.
func (t *TypingManager) CancelTimeout(s *BotSession, chatID *int64) bool {
	if chatID == nil {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	rec, ok := t.active[s.Alias][*chatID]
	if !ok {
		return false
	}
	rec.notify = false
	rec.deadline = math.Inf(1)
	return true
}

func (t *TypingManager) cleanup(alias string, chatID int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if perBot, ok := t.active[alias]; ok {
		delete(perBot, chatID)
		if len(perBot) == 0 {
			delete(t.active, alias)
		}
	}
	for k, v := range t.cbToChat {
		if a, _ := v[0].(string); a == alias {
			if c, ok := v[1].(int64); ok && c == chatID {
				delete(t.cbToChat, k)
			}
		}
	}
}

func (t *TypingManager) purgeCallbacksLocked(alias string) {
	for k, v := range t.cbToChat {
		if a, _ := v[0].(string); a == alias {
			delete(t.cbToChat, k)
		}
	}
}

// Close stops every loop (process shutdown).
func (t *TypingManager) Close() {
	t.mu.Lock()
	t.closed = true
	for _, perBot := range t.active {
		for _, rec := range perBot {
			if rec.cancel != nil {
				rec.cancel()
			}
		}
	}
	t.mu.Unlock()
}
