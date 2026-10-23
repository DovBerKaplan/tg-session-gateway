package store

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/DovBerKaplan/tg-session-gateway/internal/health"
)

func openTestGateway(t *testing.T, dir string) *Gateway {
	t.Helper()
	g, err := OpenGateway(dir, "CfR-eL8QzhZJQb22PBP1zxH7CzJMFK4LwKvFACh1pfw=", "normal", filepath.Join(dir, "backups"), health.NewStorage())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(g.Close)
	return g
}

func TestGatewayBotLifecycle(t *testing.T) {
	g := openTestGateway(t, t.TempDir())
	if err := g.RegisterBot("b1", "700000:token"); err != nil {
		t.Fatal(err)
	}
	tok, ok := g.GetToken("b1")
	if !ok || tok != "700000:token" {
		t.Fatalf("token roundtrip failed: %q ok=%v", tok, ok)
	}
	bots, _ := g.ListBots()
	if len(bots) != 1 || bots[0].Alias != "b1" {
		t.Fatalf("list_bots = %+v", bots)
	}
	if err := g.SaveOffset("b1", 42); err != nil {
		t.Fatal(err)
	}
	if g.GetOffset("b1") != 42 {
		t.Fatal("offset not persisted")
	}
	deleted, _ := g.DeleteBot("b1")
	if !deleted {
		t.Fatal("delete must report true")
	}
	if _, ok := g.GetToken("b1"); ok {
		t.Fatal("deleted bot must not resolve")
	}
}

func TestGatewayReopenKeepsData(t *testing.T) {
	dir := t.TempDir()
	g := openTestGateway(t, dir)
	_ = g.RegisterBot("keep", "710000:token")
	_ = g.SaveOffset("keep", 1001)
	g.Close()

	g2 := openTestGateway(t, dir)
	defer g2.Close()
	if g2.GetOffset("keep") != 1001 {
		t.Fatal("offset must survive reopen")
	}
	if tok, _ := g2.GetToken("keep"); tok != "710000:token" {
		t.Fatal("token must survive reopen")
	}
}

func TestIdempotencyClaimFinishReplay(t *testing.T) {
	g := openTestGateway(t, t.TempDir())
	if v := g.IdempotencyClaim("b1", "k1"); v != "claimed" {
		t.Fatalf("first claim = %q, want claimed", v)
	}
	// a concurrent second claim while pending is a replay (suppressed)
	if v := g.IdempotencyClaim("b1", "k1"); v != "replay" {
		t.Fatalf("pending duplicate = %q, want replay", v)
	}
	g.IdempotencyFinish("b1", "k1")
	if v := g.IdempotencyClaim("b1", "k1"); v != "replay" {
		t.Fatalf("finished key = %q, want replay", v)
	}
	// a FAILED send releases the claim so retries pass
	if v := g.IdempotencyClaim("b1", "k2"); v != "claimed" {
		t.Fatalf("k2 claim = %q", v)
	}
	g.IdempotencyFail("b1", "k2")
	if v := g.IdempotencyClaim("b1", "k2"); v != "claimed" {
		t.Fatalf("after fail, retry claim = %q, want claimed", v)
	}
	// the same key on another bot never collides
	if v := g.IdempotencyClaim("b2", "k1"); v != "claimed" {
		t.Fatalf("per-bot keys must not collide: %q", v)
	}
}

func TestBackoffLedgerRoundtrip(t *testing.T) {
	g := openTestGateway(t, t.TempDir())
	until := wallNow() + 300
	chat := int64(10)
	g.SaveBackoff("b1", &chat, until)
	g.SaveBackoff("b1", nil, until) // bot-wide row uses chat_id 0
	rows := g.LoadBackoffs("b1")
	if len(rows) != 2 {
		t.Fatalf("load_backoffs = %+v, want 2 rows", rows)
	}
	// expired rows are pruned on load
	g.SaveBackoff("b2", &chat, wallNow()-1)
	if rows := g.LoadBackoffs("b2"); len(rows) != 0 {
		t.Fatalf("expired backoff not pruned: %+v", rows)
	}
}

// ── the persistent queue ───────────────────────────────────────────

func TestQueuePushPullAckOffsets(t *testing.T) {
	dir := t.TempDir()
	q, err := OpenQueue(100, "drop_oldest", 3600, filepath.Join(dir, "queue.db"), "b1", "normal", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	batch := []map[string]any{
		{"update_id": int64(1001), "message": map[string]any{"text": "first"}},
		{"update_id": int64(1002), "message": map[string]any{"text": "second"}},
	}
	if err := q.PushAll(batch); err != nil {
		t.Fatal(err)
	}
	// redelivery dedupes
	_ = q.PushAll(batch)
	if q.Depth() != 2 {
		t.Fatalf("depth after dedupe = %d, want 2", q.Depth())
	}

	got := q.Pull(nil, 0, 100, nil)
	if len(got) != 2 {
		t.Fatalf("pull = %d updates", len(got))
	}
	// Bot API offset semantics: offset = first id wanted → acks below
	got = q.Pull(ptr(int64(1002)), 0, 100, nil)
	if len(got) != 1 || idOf(got[0]) != 1002 {
		t.Fatalf("offset pull = %+v", got)
	}
	if q.Depth() != 1 {
		t.Fatalf("after offset ack depth = %d, want 1 (1002 still queued)", q.Depth())
	}
	// the next offset ack consumes it
	q.Pull(ptr(int64(1003)), 0, 100, nil)
	if q.Depth() != 0 {
		t.Fatalf("final ack depth = %d, want 0", q.Depth())
	}
}

func TestQueueRestartRestoresUnacked(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "queue.db")
	q1, err := OpenQueue(100, "drop_oldest", 3600, path, "b1", "normal", nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = q1.PushAll([]map[string]any{
		{"update_id": int64(1), "message": map[string]any{"text": "a"}},
		{"update_id": int64(2), "message": map[string]any{"text": "b"}},
	})
	// consumer saw update 1 only (ack via offset = 2 → ack <= 1)
	q1.Pull(ptr(int64(2)), 0, 100, nil)
	q1.Close()

	q2, err := OpenQueue(100, "drop_oldest", 3600, path, "b1", "normal", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer q2.Close()
	got := q2.Pull(nil, 0, 100, nil)
	if len(got) != 1 || idOf(got[0]) != 2 {
		t.Fatalf("restart must replay only unacked: %+v", got)
	}
}

func TestQueueOverflowDropOldest(t *testing.T) {
	q, _ := OpenQueue(2, "drop_oldest", 0, "", "b1", "normal", nil)
	defer q.Close()
	_ = q.PushAll([]map[string]any{{"update_id": int64(1)}, {"update_id": int64(2)}})
	_ = q.PushAll([]map[string]any{{"update_id": int64(3)}})
	stats := q.Stats()
	if stats["depth"] != 2 || stats["dropped_oldest"] != 1 {
		t.Fatalf("drop_oldest stats = %+v", stats)
	}
	got := q.Pull(nil, 0, 10, nil)
	if idOf(got[0]) != 2 {
		t.Fatal("oldest must have been dropped")
	}
}

func TestQueueOverflowRejectNew(t *testing.T) {
	q, _ := OpenQueue(2, "reject_new", 0, "", "b1", "normal", nil)
	defer q.Close()
	_ = q.PushAll([]map[string]any{{"update_id": int64(1)}, {"update_id": int64(2)}})
	if err := q.PushAll([]map[string]any{{"update_id": int64(3)}}); err == nil {
		t.Fatal("reject_new must report overflow")
	}
}

func TestQueueTTLExpiry(t *testing.T) {
	q, _ := OpenQueue(10, "drop_oldest", 0.05, "", "b1", "normal", nil)
	defer q.Close()
	_ = q.PushAll([]map[string]any{{"update_id": int64(1)}})
	time.Sleep(80 * time.Millisecond)
	_ = q.PushAll([]map[string]any{{"update_id": int64(2)}}) // triggers prune
	if got := q.Pull(nil, 0, 10, nil); len(got) != 1 || idOf(got[0]) != 2 {
		t.Fatalf("expired update must be pruned: %+v", got)
	}
}

// ── MT store ───────────────────────────────────────────────────────

func openTestMT(t *testing.T, dir string) *MT {
	t.Helper()
	m, err := OpenMT(dir, "CfR-eL8QzhZJQb22PBP1zxH7CzJMFK4LwKvFACh1pfw=", "normal", filepath.Join(dir, "backups"), health.NewStorage())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)
	return m
}

func TestMTSessionLifecycle(t *testing.T) {
	m := openTestMT(t, t.TempDir())
	if err := m.Register("s1", "710000:mt-token-abcdefghijklmnopqr"); err != nil {
		t.Fatal(err)
	}
	rows, _ := m.ListSessions()
	if len(rows) != 1 || rows[0].Alias != "s1" {
		t.Fatalf("list = %+v", rows)
	}
	if rows[0].PhoneOrToken[:5] != "71000" {
		t.Fatalf("phone_or_token preview = %q", rows[0].PhoneOrToken)
	}
	tok, ok := m.GetToken("s1")
	if !ok || tok != "710000:mt-token-abcdefghijklmnopqr" {
		t.Fatalf("token roundtrip: %q %v", tok, ok)
	}
	if err := m.SaveState("s1", MTState{PTS: 15, QTS: 3, Date: 100, Seq: 7}); err != nil {
		t.Fatal(err)
	}
	if st := m.GetState("s1"); st.PTS != 15 || st.Seq != 7 {
		t.Fatalf("state = %+v", st)
	}
}

func TestMTQueuePersistAck(t *testing.T) {
	m := openTestMT(t, t.TempDir())
	_ = m.Register("s1", "tok")
	_ = m.QueuePush("s1", 1, `{"_gw_seq":1,"message":{"text":"a"}}`)
	_ = m.QueuePush("s1", 2, `{"_gw_seq":2,"message":{"text":"b"}}`)
	if m.QueueDepth("s1") != 2 {
		t.Fatal("depth must be 2")
	}
	got := m.QueuePull("s1", 0, 100)
	if len(got) != 2 || got[0]["update_id"] != int64(1) {
		t.Fatalf("pull = %+v", got)
	}
	if err := m.QueueAck("s1", 1); err != nil {
		t.Fatal(err)
	}
	if m.QueueDepth("s1") != 1 {
		t.Fatal("ack must delete")
	}
	if got := m.QueuePull("s1", 0, 100); len(got) != 1 || idOf(got[0]) != 2 {
		t.Fatalf("post-ack pull = %+v", got)
	}
}

func TestMTDeadLetters(t *testing.T) {
	m := openTestMT(t, t.TempDir())
	_ = m.Register("s1", "tok")
	m.DeadLetter("s1", `{"weird":`, "unserializable")
	rows := m.ListDeadLetters("s1", 10)
	if len(rows) != 1 || rows[0].Reason != "unserializable" {
		t.Fatalf("dead letters = %+v", rows)
	}
	if n := m.ClearDeadLetters("s1"); n != 1 {
		t.Fatal("clear must report 1")
	}
}

func TestMTIdempotencyPerAlias(t *testing.T) {
	m := openTestMT(t, t.TempDir())
	_ = m.Register("a", "t")
	_ = m.Register("b", "t")
	if v := m.IdempotencyClaim("a", "shared"); v != "claimed" {
		t.Fatalf("a claim = %q", v)
	}
	if v := m.IdempotencyClaim("b", "shared"); v != "claimed" {
		t.Fatalf("b claim = %q — keys must be per-alias", v)
	}
	m.IdempotencyFinish("a", "shared")
	if v := m.IdempotencyClaim("a", "shared"); v != "replay" {
		t.Fatal("finished key must replay-suppress")
	}
}

// ── migration from the PYTHON-created database files ──────────────

func TestOpenPythonCreatedGatewayDB(t *testing.T) {
	if !pythonAvailable() {
		t.Skip("python venv unavailable")
	}
	dir := t.TempDir()
	if err := runPython(dir, "gateway"); err != nil {
		t.Fatalf("python fixture failed: %v", err)
	}
	g := openTestGateway(t, dir)
	bots, err := g.ListBots()
	if err != nil {
		t.Fatal(err)
	}
	if len(bots) != 1 || bots[0].Alias != "pybot" {
		t.Fatalf("python-created bots = %+v", bots)
	}
	if g.GetOffset("pybot") != 5001 {
		t.Fatalf("python offset = %d, want 5001", g.GetOffset("pybot"))
	}
	if tok, ok := g.GetToken("pybot"); !ok || tok != "720000:python-created" {
		t.Fatalf("python token decrypt failed: %q %v", tok, ok)
	}
}

func TestOpenPythonCreatedQueueDB(t *testing.T) {
	if !pythonAvailable() {
		t.Skip("python venv unavailable")
	}
	dir := t.TempDir()
	if err := runPython(dir, "queue"); err != nil {
		t.Fatalf("python fixture failed: %v", err)
	}
	q, err := OpenQueue(100, "drop_oldest", 3600, filepath.Join(dir, "queue.db"), "pybot", "normal", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	got := q.Pull(nil, 0, 100, nil)
	if len(got) != 2 {
		t.Fatalf("python queue rows = %+v", got)
	}
	if got[0]["update_id"] != float64(9001) || got[0]["message"].(map[string]any)["text"] != "from python" {
		t.Fatalf("python queue content = %+v", got)
	}
}

func ptr[T any](v T) *T { return &v }

// idOf reads an update_id across in-memory (int64) and JSON-reloaded
// (float64) representations.
func idOf(u map[string]any) int64 {
	switch n := u["update_id"].(type) {
	case int64:
		return n
	case float64:
		return int64(n)
	case int:
		return int64(n)
	}
	return -1
}
