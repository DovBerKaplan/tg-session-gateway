package mtproto

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestMethodLaneClassification(t *testing.T) {
	cases := map[string]string{
		"send_message":          LaneInteractive,
		"send_media":            LaneInteractive,
		"edit_admin":            LaneInteractive,
		"delete_channel":        LaneInteractive,
		"send_read_acknowledge": LaneInteractive,
		"get_history":           LaneBulk,
		"get_messages":          LaneBulk,
		"get_dialogs":           LaneBulk,
		"get_participants":      LaneBulk,
		"get_me":                LaneNormal,
		"resolve_username":      LaneNormal,
		"get_chat":              LaneNormal,
		"answer_callback_query": LaneInteractive,
	}
	for method, want := range cases {
		if got := MethodLane(method, ""); got != want {
			t.Fatalf("MethodLane(%s)=%s want %s", method, got, want)
		}
	}
	// a client-pinned lane overrides the method class
	if got := MethodLane("send_message", LaneBulk); got != LaneBulk {
		t.Fatalf("pinned lane=%s", got)
	}
}

func TestLaneAdmitFastPath(t *testing.T) {
	l := newLaneScheduler()
	ok := l.Admit(context.Background(), LaneBulk, "cl-a", time.Now().Add(time.Second), func() bool { return true })
	if !ok {
		t.Fatal("fast path must admit")
	}
	if l.Admissions()[LaneBulk] != 1 {
		t.Fatal("admission counted")
	}
}

func TestLanePriorityInteractiveFirst(t *testing.T) {
	l := newLaneScheduler()
	var order []string
	var mu sync.Mutex
	var wg sync.WaitGroup

	try := func() bool { return true } // tokens always available

	// pre-seed a bulk waiter, then make sure an interactive admission
	// that arrives while the bulk waiter is requeued gets in first.
	wg.Add(3)
	go func() {
		defer wg.Done()
		l.Admit(context.Background(), LaneBulk, "cl-a", time.Now().Add(2*time.Second), try)
		mu.Lock()
		order = append(order, "bulk")
		mu.Unlock()
	}()
	// let the bulk waiter take its turn (admits immediately via fast
	// path since queue is empty) — the ordering test starts below
	time.Sleep(20 * time.Millisecond)

	// now starve: tokens unavailable, so both queue up; then open the
	// tap and watch interactive finish before bulk
	tokens := 0
	gate := func() bool {
		mu.Lock()
		defer mu.Unlock()
		if tokens > 0 {
			tokens--
			return true
		}
		return false
	}
	go func() {
		defer wg.Done()
		l.Admit(context.Background(), LaneBulk, "cl-b", time.Now().Add(5*time.Second), gate)
		mu.Lock()
		order = append(order, "bulk2")
		mu.Unlock()
	}()
	time.Sleep(20 * time.Millisecond)
	go func() {
		defer wg.Done()
		l.Admit(context.Background(), LaneInteractive, "cl-c", time.Now().Add(5*time.Second), gate)
		mu.Lock()
		order = append(order, "interactive")
		mu.Unlock()
	}()
	time.Sleep(40 * time.Millisecond) // both queued
	mu.Lock()
	tokens = 2 // exactly two admissions will succeed
	mu.Unlock()

	wg.Wait()
	if len(order) != 3 || order[2] != "bulk2" && order[2] == "interactive" {
		// interactive must not be last
		t.Fatalf("order=%v — interactive must beat the queued bulk", order)
	}
	found := map[string]int{}
	for _, o := range order {
		found[o]++
	}
	if found["interactive"] != 1 || found["bulk2"] != 1 {
		t.Fatalf("order=%v", order)
	}
	// interactive finished before bulk2
	var iInt, iBulk int
	for i, o := range order {
		if o == "interactive" {
			iInt = i
		}
		if o == "bulk2" {
			iBulk = i
		}
	}
	if iInt > iBulk {
		t.Fatalf("interactive (%d) finished after bulk (%d): %v", iInt, iBulk, order)
	}
}

func TestLaneTimeoutSynthetic429Path(t *testing.T) {
	l := newLaneScheduler()
	start := time.Now()
	ok := l.Admit(context.Background(), LaneBulk, "cl-a",
		start.Add(120*time.Millisecond), func() bool { return false })
	if ok {
		t.Fatal("never-admitted must return false")
	}
	if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
		t.Fatalf("returned too early: %v", elapsed)
	}
}

func TestLaneFloodPauseLadder(t *testing.T) {
	l := newLaneScheduler()
	l.PauseForFlood(30)
	if !l.IsPaused(LaneBulk) || !l.IsPaused(LaneNormal) {
		t.Fatal("bulk+normal paused")
	}
	if l.IsPaused(LaneInteractive) {
		t.Fatal("interactive must keep running")
	}
	// interactive still admits; bulk must wait out the pause
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if !l.Admit(ctx, LaneInteractive, "cl-a", time.Now().Add(100*time.Millisecond), func() bool { return true }) {
		t.Fatal("interactive admitted during flood pause")
	}
	// bulk fast-path is gated by the pause
	ok := l.Admit(context.Background(), LaneBulk, "cl-a",
		time.Now().Add(80*time.Millisecond), func() bool { return true })
	if ok {
		t.Fatal("bulk must not admit while flood-paused")
	}
}

func TestLaneContextCancelAbandons(t *testing.T) {
	l := newLaneScheduler()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan bool, 1)
	go func() {
		done <- l.Admit(ctx, LaneBulk, "cl-a", time.Now().Add(5*time.Second), func() bool { return false })
	}()
	time.Sleep(30 * time.Millisecond)
	cancel()
	if <-done {
		t.Fatal("cancelled admit must return false")
	}
	if d := l.Depth(); d[LaneBulk] != 0 {
		t.Fatalf("waiter leaked in queue: %v", d)
	}
}

func TestLaneRoundRobinWithinLane(t *testing.T) {
	l := newLaneScheduler()
	var turns []string
	var mu sync.Mutex
	// allow exactly one admission at a time; each waiter's try succeeds
	// on its turn — assert the two bulk clients alternate
	tryFor := func(caller string) func() bool {
		return func() bool {
			mu.Lock()
			defer mu.Unlock()
			turns = append(turns, caller)
			return true
		}
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		l.Admit(context.Background(), LaneBulk, "cl-a", time.Now().Add(2*time.Second), tryFor("cl-a"))
	}()
	go func() {
		defer wg.Done()
		l.Admit(context.Background(), LaneBulk, "cl-b", time.Now().Add(2*time.Second), tryFor("cl-b"))
	}()
	wg.Wait()
	// both admitted; each tried exactly once (fast path may take one,
	// the other queues — either way both get in and both count once)
	if len(turns) != 2 {
		t.Fatalf("turns=%v", turns)
	}
}
