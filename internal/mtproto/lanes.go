package mtproto

// Priority lanes: every facade call rides a lane — interactive (sends,
// live-critical acks), normal (default reads), bulk (history scanning).
// The session's lane scheduler decides WHO gets to try the rate guard
// next: highest non-empty lane first, round-robin between clients
// within a lane. A history scan therefore never takes the token a live
// update needs, and two scanner workers share a lane fairly.

import (
	"context"
	"sync"
	"time"
)

const (
	LaneInteractive = "interactive"
	LaneNormal      = "normal"
	LaneBulk        = "bulk"

	// laneTick is the admission retry granularity — coarse on purpose:
	// a Telegram round-trip is 100ms+, and a coarse tick keeps the
	// scheduler from hot-looping when every lane is rate-limited.
	laneTick = 50 * time.Millisecond
)

// bulkMethods ride the bulk lane (the heavy read scans). Everything
// that mutates rides interactive; the rest is normal.
var bulkMethods = map[string]bool{
	"get_history":      true,
	"get_messages":     true,
	"get_dialogs":      true,
	"get_participants": true,
}

var interactivePrefixes = []string{"send_", "edit_", "delete_", "forward_", "copy_", "answer_"}

// MethodLane classifies a facade method into its lane. A client token
// may pin itself to a lane (Caller.Lane) — e.g. a shadow scanner pinned
// to bulk even for its occasional reads.
func MethodLane(method string, callerLane string) string {
	if callerLane != "" {
		return callerLane
	}
	if bulkMethods[method] {
		return LaneBulk
	}
	for _, p := range interactivePrefixes {
		if len(method) >= len(p) && method[:len(p)] == p {
			return LaneInteractive
		}
	}
	return LaneNormal
}

// laneWaiter is one queued admission attempt. ready is a buffered
// token: a send (never close — the waiter requeues and can be picked
// again) hands it a turn.
type laneWaiter struct {
	lane   string
	caller string
	ready  chan struct{}
}

func newLaneWaiter(lane, caller string) *laneWaiter {
	return &laneWaiter{lane: lane, caller: caller, ready: make(chan struct{}, 1)}
}

// offerLocked hands the waiter a turn (idempotent).
func (w *laneWaiter) offer() {
	select {
	case w.ready <- struct{}{}:
	default:
	}
}

// take consumes a pending turn without blocking.
func (w *laneWaiter) take() bool {
	select {
	case <-w.ready:
		return true
	default:
		return false
	}
}

// laneScheduler serializes rate-guard attempts per session in lane
// priority order. try() (the guard token consumption) only runs when
// the scheduler hands the waiter a turn; a janitor tick re-offers
// turns while waiters remain, so nobody is forgotten and nothing
// hot-loops.
type laneScheduler struct {
	mu     sync.Mutex
	queues map[string][]*laneWaiter
	// pausedUntil[lane]: monotonic time until which the lane is held
	// back after an account-level FLOOD_WAIT (bulk longest).
	pausedUntil map[string]float64
	// counters for metrics
	admissions map[string]int64
	janitor    chan struct{} // non-nil while the janitor runs
}

func newLaneScheduler() *laneScheduler {
	return &laneScheduler{
		queues:      map[string][]*laneWaiter{},
		pausedUntil: map[string]float64{},
		admissions:  map[string]int64{},
	}
}

// pickLocked chooses the next waiter: the head of the highest-priority
// non-empty, non-paused lane (round-robin falls out of FIFO order —
// waiters requeue at the tail after a failed turn).
func (l *laneScheduler) pickLocked() *laneWaiter {
	for _, lane := range []string{LaneInteractive, LaneNormal, LaneBulk} {
		if monoNow() < l.pausedUntil[lane] {
			continue
		}
		if q := l.queues[lane]; len(q) > 0 {
			w := q[0]
			l.queues[lane] = q[1:]
			return w
		}
	}
	return nil
}

// nudgeLocked hands the turn to the next eligible waiter (if any).
func (l *laneScheduler) nudgeLocked() {
	if w := l.pickLocked(); w != nil {
		w.offer()
	}
}

func (l *laneScheduler) emptyLocked() bool {
	for _, q := range l.queues {
		if len(q) > 0 {
			return false
		}
	}
	return true
}

// ensureJanitorLocked starts the ticker goroutine while waiters exist.
func (l *laneScheduler) ensureJanitorLocked() {
	if l.janitor != nil {
		return
	}
	stop := make(chan struct{})
	l.janitor = stop
	go func() {
		t := time.NewTicker(laneTick)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				l.mu.Lock()
				if l.emptyLocked() {
					l.janitor = nil
					l.mu.Unlock()
					return
				}
				l.nudgeLocked()
				l.mu.Unlock()
			}
		}
	}()
}

// Admit runs one admission attempt through the turnstile. It blocks
// until try() succeeds, the deadline passes, or ctx is done. try is
// ONLY invoked when the scheduler hands the waiter a turn, so the
// lanes decide who competes for the guard next.
func (l *laneScheduler) Admit(ctx context.Context, lane, caller string, deadline time.Time, try func() bool) bool {
	l.mu.Lock()
	// fast path: nobody waiting, lane unpaused — try immediately
	if l.emptyLocked() && monoNow() >= l.pausedUntil[lane] {
		l.mu.Unlock()
		if try() {
			l.noteAdmission(lane)
			return true
		}
	} else {
		l.mu.Unlock()
	}

	w := newLaneWaiter(lane, caller)
	l.mu.Lock()
	l.queues[lane] = append(l.queues[lane], w)
	l.ensureJanitorLocked()
	l.nudgeLocked()
	l.mu.Unlock()

	for {
		select {
		case <-w.ready: // our turn
			if try() {
				l.noteAdmission(lane)
				return true
			}
			// no token for us — requeue at the tail (round-robin);
			// the janitor re-offers turns on its next tick
			l.mu.Lock()
			l.queues[lane] = append(l.queues[lane], w)
			l.mu.Unlock()
		case <-ctx.Done():
			l.abandon(w)
			return false
		case <-time.After(time.Until(deadline)):
			l.abandon(w)
			return false
		}
	}
}

// abandon removes a waiter and, if it held an unpicked turn, hands the
// turn onward.
func (l *laneScheduler) abandon(w *laneWaiter) {
	l.mu.Lock()
	defer l.mu.Unlock()
	q := l.queues[w.lane]
	for i, cand := range q {
		if cand == w {
			l.queues[w.lane] = append(q[:i], q[i+1:]...)
			break
		}
	}
	if w.take() { // we held the turn — pass it on
		l.nudgeLocked()
	}
}

func (l *laneScheduler) noteAdmission(lane string) {
	l.mu.Lock()
	l.admissions[lane]++
	l.mu.Unlock()
}

// PauseForFlood applies the degradation ladder after an account-level
// FLOOD_WAIT: bulk pauses for the full wait, normal for half, and
// interactive keeps running (it is the lane the account needs to
// survive). Per-peer floods (a specific chat) do NOT pause lanes —
// MTGuard's per-peer backoff already scopes them.
func (l *laneScheduler) PauseForFlood(waitSeconds float64) {
	if waitSeconds <= 0 {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := monoNow()
	if until := now + waitSeconds; until > l.pausedUntil[LaneBulk] {
		l.pausedUntil[LaneBulk] = until
	}
	if until := now + waitSeconds/2; until > l.pausedUntil[LaneNormal] {
		l.pausedUntil[LaneNormal] = until
	}
}

// Depth returns the queued waiter count per lane (metrics).
func (l *laneScheduler) Depth() map[string]int {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := map[string]int{}
	for lane, q := range l.queues {
		out[lane] = len(q)
	}
	return out
}

// Admissions returns cumulative admissions per lane (metrics).
func (l *laneScheduler) Admissions() map[string]int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := map[string]int64{}
	for lane, n := range l.admissions {
		out[lane] = n
	}
	return out
}

// IsPaused reports whether a lane is flood-paused (tests/status).
func (l *laneScheduler) IsPaused(lane string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return monoNow() < l.pausedUntil[lane]
}
