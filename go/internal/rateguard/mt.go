package rateguard

import (
	"math"
	"regexp"
	"strings"
	"sync"

	"github.com/DovBerKaplan/tg-session-gateway/go/internal/config"
)

// ── FLOOD_WAIT parsing (Pyrogram/Telegram error strings) ──────────

var (
	floodWaitRe     = regexp.MustCompile(`(?i)FloodWait\((\d+)\)`)
	floodPremiumRe  = regexp.MustCompile(`(?i)FloodPremiumWait\((\d+)\)`)
	slowmodeWaitRe  = regexp.MustCompile(`(?i)SlowmodeWait\((\d+)\)`)
	peerFloodRe     = regexp.MustCompile(`(?i)PeerFloodInvalid`)
	tgFloodWaitRe   = regexp.MustCompile(`(?is)FLOOD_WAIT\S*.*?wait of (\d+) seconds`)
	tgSlowmodeRe    = regexp.MustCompile(`(?is)SLOWMODE_WAIT\S*.*?wait of (\d+) seconds`)
	allDigitsRe     = regexp.MustCompile(`\d+`)
)

// FloodInfo describes a parsed rate-limit error.
type FloodInfo struct {
	Type string `json:"type"` // flood_wait | flood_premium | slowmode | peer_flood
	Wait int    `json:"wait"`
}

// ParseFloodWait extracts rate-limit info from an error string
// (Pyrogram FloodWait(42) / Telegram raw [420 FLOOD_WAIT_X] formats).
func ParseFloodWait(errText string) *FloodInfo {
	s := errText
	if m := floodWaitRe.FindStringSubmatch(s); m != nil {
		return &FloodInfo{Type: "flood_wait", Wait: atoi(m[1])}
	}
	if m := floodPremiumRe.FindStringSubmatch(s); m != nil {
		return &FloodInfo{Type: "flood_premium", Wait: atoi(m[1])}
	}
	if m := slowmodeWaitRe.FindStringSubmatch(s); m != nil {
		return &FloodInfo{Type: "slowmode", Wait: atoi(m[1])}
	}
	if peerFloodRe.MatchString(s) {
		return &FloodInfo{Type: "peer_flood", Wait: 300}
	}
	if m := tgFloodWaitRe.FindStringSubmatch(s); m != nil {
		return &FloodInfo{Type: "flood_wait", Wait: atoi(m[1])}
	}
	if m := tgSlowmodeRe.FindStringSubmatch(s); m != nil {
		return &FloodInfo{Type: "slowmode", Wait: atoi(m[1])}
	}
	return nil
}

func atoi(s string) int {
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return n
		}
		n = n*10 + int(c-'0')
	}
	return n
}

// ── Per-peer adaptive backoff ─────────────────────────────────────

type peerBackoff struct {
	offences   int
	lastWait   float64
	pausedUntil float64
}

// MTGuard is all pacing state for one MTProto session: FAQ buckets
// plus reactive FLOOD_WAIT learning with per-peer adaptive backoff.
type MTGuard struct {
	mu sync.Mutex

	faq    config.RateConfig
	MT     config.MTProtoRate
	Private map[int64]*Bucket
	Groups  map[int64]*Bucket
	globalWrites *Bucket
	reads        *Bucket
	silent       *Bucket
	paid         *Bucket
	PaidEnabled  bool
	egressPausedUntil float64
	paidLane bool

	peerBackoffs map[int64]*peerBackoff
}

// NewMTGuard builds a guard for one MTProto session.
func NewMTGuard(faq config.RateConfig, mt config.MTProtoRate) *MTGuard {
	return &MTGuard{
		faq:      faq,
		MT:       mt,
		Private:  map[int64]*Bucket{},
		Groups:   map[int64]*Bucket{},
		globalWrites: NewBucket(faq.GlobalRate, faq.GlobalBurst),
		reads:        NewBucket(faq.ReadRate, max(10, int(faq.ReadRate))),
		silent:       NewBucket(mt.SilentRate, max(5, int(mt.SilentRate))),
		paid:         NewBucket(faq.PaidRate, int(faq.PaidRate)),
		peerBackoffs: map[int64]*peerBackoff{},
	}
}

func (g *MTGuard) chatBucket(chatID int64) *Bucket {
	var table map[int64]*Bucket
	var rate float64
	var burst int
	if chatID >= 0 {
		table, rate, burst = g.Private, g.faq.PrivateRate, g.faq.PrivateBurst
	} else {
		table, rate, burst = g.Groups, g.faq.GroupRate, g.faq.GroupBurst
	}
	b, ok := table[chatID]
	if !ok {
		b = NewBucket(rate, burst)
		table[chatID] = b
	}
	return b
}

func (g *MTGuard) peerState(peerID int64) *peerBackoff {
	pb, ok := g.peerBackoffs[peerID]
	if !ok {
		pb = &peerBackoff{}
		g.peerBackoffs[peerID] = pb
	}
	return pb
}

func (g *MTGuard) EnterPaidLane(allowed bool) { g.paidLane = allowed && g.PaidEnabled }
func (g *MTGuard) ExitPaidLane()              { g.paidLane = false }

var silentMethods = map[string]bool{
	"resolve_username": true, "get_participants": true, "join_channel": true, "leave_chat": true,
}

// TryAcquire is the non-blocking admission attempt.
func (g *MTGuard) TryAcquire(method string, chatID *int64) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := nowMono()

	if chatID != nil {
		if pb := g.peerState(*chatID); pb.pausedUntil > now {
			return false
		}
	}
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
	if silentMethods[strings.ToLower(method)] {
		return g.silent.TryTake(now)
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

// WaitTime is seconds until the request may proceed.
func (g *MTGuard) WaitTime(method string, chatID *int64) float64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := nowMono()
	wait := math.Max(0, g.egressPausedUntil-now)
	if chatID != nil {
		pb := g.peerState(*chatID)
		wait = math.Max(wait, pb.pausedUntil-now)
	}
	kind := MethodKind(method)
	if kind == "fast" {
		return math.Max(0, wait)
	}
	if kind == "read" {
		return math.Max(wait, g.reads.NextAvailable(now))
	}
	wait = math.Max(wait, g.globalWrites.NextAvailable(now))
	if chatID != nil {
		wait = math.Max(wait, g.chatBucket(*chatID).NextAvailable(now))
	}
	return math.Max(0, wait)
}

// ReportFloodWait learns from a real FLOOD_WAIT; returns the parsed
// info plus the adaptive wait actually applied.
func (g *MTGuard) ReportFloodWait(errText string, chatID *int64) *FloodInfo {
	info := ParseFloodWait(errText)
	if info == nil {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	now := nowMono()
	wait := float64(info.Wait)

	if info.Type == "peer_flood" {
		wait = g.MT.PeerFloodCooldown
		if chatID != nil {
			pb := g.peerState(*chatID)
			pb.offences++
			pb.pausedUntil = now + wait
		}
		return info
	}
	if info.Type == "slowmode" && !g.MT.SlowmodeRespect {
		return info
	}
	if chatID != nil {
		pb := g.peerState(*chatID)
		pb.offences++
		pb.lastWait = wait
		adaptive := math.Min(wait*math.Pow(g.MT.FloodBackoffBase, float64(pb.offences-1)), 3600)
		pb.pausedUntil = now + adaptive
		g.chatBucket(*chatID).Pause(adaptive, now)
	} else {
		g.egressPausedUntil = math.Max(g.egressPausedUntil, now+wait)
		g.globalWrites.Pause(wait, now)
	}
	return info
}

// AdaptiveWait reports the adaptive wait for a flood report (for the
// HTTP 429 description), or the plain wait when no chat is associated.
func (g *MTGuard) AdaptiveWait(info *FloodInfo, chatID *int64) int {
	if info == nil {
		return 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if chatID == nil {
		return info.Wait
	}
	pb := g.peerState(*chatID)
	return int(math.Min(float64(info.Wait)*math.Pow(g.MT.FloodBackoffBase, float64(pb.offences-1)), 3600))
}

// ClearPeerBackoff resets backoff after a successful send.
func (g *MTGuard) ClearPeerBackoff(chatID int64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.peerBackoffs, chatID)
}

// Snapshot is the admin-status view.
func (g *MTGuard) Snapshot() map[string]any {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := nowMono()
	peer := map[string]any{}
	for pid, pb := range g.peerBackoffs {
		if pb.pausedUntil > now {
			peer[itoa(pid)] = map[string]any{
				"offences":    pb.offences,
				"paused_for_s": math.Round((pb.pausedUntil-now)*10) / 10,
			}
		}
	}
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
		"private_empty":       privateEmpty,
		"group_empty":         groupEmpty,
		"egress_paused_for_s": math.Round(math.Max(0, g.egressPausedUntil-now)*10) / 10,
		"peer_backoffs":       peer,
	}
}

func itoa(n int64) string {
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
