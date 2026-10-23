// Package rateguard is the single source of truth for outbound pacing.
// It is a faithful port of gateway/rateguard.py + mtgateway/rateguard.py:
// FAQ token buckets, fast-path/read classification, real-429 learning,
// per-peer adaptive backoff and FLOOD_WAIT parsing.
package rateguard

import (
	"math"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/DovBerKaplan/tg-session-gateway/go/internal/config"
)

// Methods that must never starve, and methods that are reads.
var fastPath = map[string]bool{
	"answercallbackquery":    true,
	"answerinlinequery":      true,
	"answershippingquery":    true,
	"answerprecheckoutquery": true,
	"setmessageeffect":       true,
}

var readMethods = map[string]bool{
	"getme": true, "getchat": true, "getchatmember": true,
	"getchatadministrators": true, "getuser": true, "getmycommands": true,
	"getmyname": true, "getmydescription": true, "getmyshortdescription": true,
	"getwebhookinfo": true, "getchatmenubutton": true, "getmydefaultadministratorrights": true,
	"getfile": true, "getchatmembercount": true, "getforumtopiciconstickers": true,
	"getstickerset": true,
}

// MethodKind classifies a Bot-API/Pyrogram method name.
func MethodKind(method string) string {
	m := strings.ToLower(method)
	if fastPath[m] {
		return "fast"
	}
	if readMethods[m] || strings.HasPrefix(m, "get") {
		return "read"
	}
	return "write"
}

// Bucket is a token bucket with pause support. Start-full semantics:
// tokens<0 at construction → burst available immediately.
type Bucket struct {
	Rate      float64 // tokens per second
	Capacity  int
	Tokens    float64
	Updated   float64 // monotonic
	PausedUntil float64
}

// NewBucket starts full (burst available immediately).
func NewBucket(rate float64, capacity int) *Bucket {
	return &Bucket{Rate: rate, Capacity: capacity, Tokens: float64(capacity), Updated: nowMono()}
}

func (b *Bucket) Refill(now float64) {
	if b.PausedUntil != 0 && now >= b.PausedUntil {
		b.PausedUntil = 0
		// retry_after already waited — one send may go immediately
		b.Tokens = math.Max(b.Tokens, 1.0)
	}
	elapsed := now - b.Updated
	b.Tokens = math.Min(float64(b.Capacity), b.Tokens+elapsed*b.Rate)
	b.Updated = now
}

func (b *Bucket) TryTake(now float64) bool {
	b.Refill(now)
	if b.PausedUntil > now {
		return false
	}
	if b.Tokens >= 1 {
		b.Tokens--
		return true
	}
	return false
}

func (b *Bucket) NextAvailable(now float64) float64 {
	b.Refill(now)
	if b.PausedUntil > now {
		return b.PausedUntil - now
	}
	deficit := 1.0 - b.Tokens
	if deficit > 0 {
		return deficit / b.Rate
	}
	return 0
}

func (b *Bucket) Pause(seconds, now float64) {
	b.PausedUntil = math.Max(b.PausedUntil, now+seconds)
	b.Tokens = 0
}

func nowMono() float64 { return float64(time.Now().UnixNano()) / 1e9 }

// Guard is all pacing state for one bot token (Bot API plane).
// Single-owner (guarded by its mutex); the HTTP layer serializes per
// session admission under it.
type Guard struct {
	mu sync.Mutex

	cfg    config.RateConfig
	Private map[int64]*Bucket
	Groups  map[int64]*Bucket
	globalWrites *Bucket
	paid         *Bucket
	reads        *Bucket
	PaidEnabled  bool // double opt-in
	egressPausedUntil float64

	paidLane bool
}

// NewGuard builds a guard for one bot with the shared rate config.
func NewGuard(cfg config.RateConfig) *Guard {
	return &Guard{
		cfg:           cfg,
		Private:       map[int64]*Bucket{},
		Groups:        map[int64]*Bucket{},
		globalWrites:  NewBucket(cfg.GlobalRate, cfg.GlobalBurst),
		paid:          NewBucket(cfg.PaidRate, int(cfg.PaidRate)),
		reads:         NewBucket(cfg.ReadRate, max(10, int(cfg.ReadRate))),
	}
}

// ChatBucket returns the bucket for a chat: negative chat_id is never
// private (spec §8.2 §4).
func (g *Guard) chatBucket(chatID int64) *Bucket {
	var table map[int64]*Bucket
	var rate float64
	var burst int
	if chatID >= 0 {
		table, rate, burst = g.Private, g.cfg.PrivateRate, g.cfg.PrivateBurst
	} else {
		table, rate, burst = g.Groups, g.cfg.GroupRate, g.cfg.GroupBurst
	}
	b, ok := table[chatID]
	if !ok {
		b = NewBucket(rate, burst)
		table[chatID] = b
	}
	return b
}

// EnterPaidLane arms the paid lane for one request (double opt-in).
func (g *Guard) EnterPaidLane(allowed bool) { g.paidLane = allowed && g.PaidEnabled }

func (g *Guard) ExitPaidLane() { g.paidLane = false }

// TryAcquire is the non-blocking admission attempt.
// A blocked chat attempt never charges the global bucket.
func (g *Guard) TryAcquire(method string, chatID *int64) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := nowMono()
	if now < g.egressPausedUntil {
		return false
	}
	kind := MethodKind(method)
	if kind == "fast" {
		return true
	}
	if kind == "read" {
		return g.reads.TryTake(now)
	}
	var chatB *Bucket
	if chatID != nil {
		chatB = g.chatBucket(*chatID)
		chatB.Refill(now)
		if chatB.PausedUntil > now || chatB.Tokens < 1 {
			return false
		}
	}
	if g.paidLane {
		if !g.paid.TryTake(now) {
			return false
		}
		if chatB != nil {
			chatB.Tokens--
		}
		return true
	}
	if chatB != nil {
		chatB.Refill(now)
		if chatB.PausedUntil > now || chatB.Tokens < 1 {
			return false
		}
	}
	if !g.globalWrites.TryTake(now) {
		return false
	}
	if chatB != nil {
		chatB.Refill(now)
		if chatB.PausedUntil > now || chatB.Tokens < 1 {
			g.globalWrites.Tokens++ // refund
			return false
		}
		chatB.Tokens--
	}
	return true
}

// WaitTime is seconds until the request may go.
func (g *Guard) WaitTime(method string, chatID *int64) float64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := nowMono()
	kind := MethodKind(method)
	wait := math.Max(0, g.egressPausedUntil-now)
	if kind == "fast" {
		return wait
	}
	if kind == "read" {
		return math.Max(wait, g.reads.NextAvailable(now))
	}
	wait = math.Max(wait, g.globalWrites.NextAvailable(now))
	if chatID != nil {
		wait = math.Max(wait, g.chatBucket(*chatID).NextAvailable(now))
	}
	return wait
}

// Report429 learns from a real Telegram 429: chat-associated pauses the
// chat bucket; otherwise the whole bot egress pauses.
func (g *Guard) Report429(chatID *int64, retryAfter float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := nowMono()
	if chatID != nil {
		g.chatBucket(*chatID).Pause(retryAfter, now)
	} else {
		g.egressPausedUntil = math.Max(g.egressPausedUntil, now+retryAfter)
		g.globalWrites.Pause(retryAfter, now)
	}
}

// SendState is the read-only probe for the auto-typing loop:
// "paused" (hard 429 block), "empty" (drained bucket) or "ok".
// Never charges tokens.
func (g *Guard) ChatSendState(chatID *int64) string {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := nowMono()
	if now < g.egressPausedUntil {
		return "paused"
	}
	wait := g.globalWrites.NextAvailable(now)
	if chatID != nil {
		b := g.chatBucket(*chatID)
		if b.PausedUntil > now {
			return "paused"
		}
		wait = math.Max(wait, b.NextAvailable(now))
	}
	if wait > 0 {
		return "empty"
	}
	return "ok"
}

// SnapshotEmpty is the admin-status view of throttled buckets.
func (g *Guard) Snapshot() map[string]any {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := nowMono()
	privateEmpty, groupEmpty := 0, 0
	for _, b := range g.Private {
		if b.NextAvailable(now) > 0 {
			privateEmpty++
		}
	}
	for _, b := range g.Groups {
		if b.NextAvailable(now) > 0 {
			groupEmpty++
		}
	}
	return map[string]any{
		"private":            privateEmpty,
		"groups":             groupEmpty,
		"egress_paused_for_s": math.Round(math.Max(0, g.egressPausedUntil-now)*10) / 10,
	}
}

// RestoreBackoff re-arms a persisted FLOOD_WAIT pause after restart.
func (g *Guard) RestoreBackoff(chatID *int64, remaining float64) {
	if remaining <= 0 {
		return
	}
	g.Report429(chatID, remaining)
}

// Jittered adds additive-only jitter for retry/backoff waits: never
// below base (a shorter sleep is another violation, not a speedup).
func Jittered(base, capExtra float64) float64 {
	return base + rand.Float64()*math.Min(capExtra, base*0.1)
}
