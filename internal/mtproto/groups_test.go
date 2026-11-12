package mtproto

import (
	"encoding/json"
	"testing"

	"github.com/gotd/td/tg"

	"github.com/DovBerKaplan/tg-session-gateway/internal/store"
)

// pushTestUpdate feeds one update through the session's write-through
// path (persist + fan-out) and returns its assigned _gw_seq.
func pushTestUpdate(t *testing.T, app *App, alias string, update map[string]any) int64 {
	t.Helper()
	s := app.Mgr.Get(alias)
	if s == nil {
		t.Fatalf("no session %s", alias)
	}
	s.pushUpdate(update)
	var seq int64
	if v, ok := anyToInt64(update["_gw_seq"]); ok {
		seq = v
	}
	return seq
}

func docUpdate(chatID int64, docID int64) map[string]any {
	return map[string]any{
		"_": "UpdateNewChannelMessage",
		"message": map[string]any{
			"id":    docID,
			"chat":  map[string]any{"id": chatID, "type": "channel"},
			"media": map[string]any{"_": "MessageMediaDocument", "document": map[string]any{"id": docID}},
		},
	}
}

func textUpdate(chatID int64, msgID int64) map[string]any {
	return map[string]any{
		"_": "UpdateNewChannelMessage",
		"message": map[string]any{
			"id":   msgID,
			"chat": map[string]any{"id": chatID, "type": "channel"},
		},
	}
}

func TestReaderGroupCursors(t *testing.T) {
	fake := &fakeMTClient{}
	app, ts := newTestApp(t, fake, t.TempDir())
	base := ts.URL
	regADM(t, base, "u1")

	// three updates: doc on chan A, text on chan A, doc on chan B
	pushTestUpdate(t, app, "u1", docUpdate(111, 1))
	pushTestUpdate(t, app, "u1", textUpdate(111, 2))
	pushTestUpdate(t, app, "u1", docUpdate(222, 3))

	// register a doc-only group scoped to channel 111
	code, body := mtDo(t, "POST", base, "/v1/sessions/u1/getUpdates",
		`{"group":"scan","timeout":0,"channels":[111],"document_only":true}`, itestAPP)
	if code != 200 || body["ok"] != true {
		t.Fatalf("register+pull=%d %v", code, body)
	}
	res, _ := body["result"].([]any)
	if len(res) != 1 {
		t.Fatalf("doc-only chan-111 group got %d updates (want 1): %v", len(res), res)
	}

	// the same rows serve a plain group (no filters): all three
	code, body = mtDo(t, "POST", base, "/v1/sessions/u1/getUpdates",
		`{"group":"all","timeout":0}`, itestAPP)
	if code != 200 {
		t.Fatalf("plain group=%d %v", code, body)
	}
	res, _ = body["result"].([]any)
	if len(res) != 3 {
		t.Fatalf("plain group got %d (want 3)", len(res))
	}

	// ack: "all" advances past everything, "scan" stays — rows survive
	// until EVERY group passed them
	resp := mtRaw(t, "POST", base+"/v1/sessions/u1/getUpdates",
		`{"group":"all","offset":4,"timeout":0}`, itestAPP)
	if resp.code != 200 {
		t.Fatalf("all ack pull=%d %s", resp.code, resp.body)
	}
	if depth := app.Store.QueueDepth("u1"); depth != 3 {
		t.Fatalf("depth after all-group ack=%d (scan cursor still 0)", depth)
	}
	// scan acks its cursor → the row it never consumed is gone anyway
	// (it never matched the filter); queue empties
	resp = mtRaw(t, "POST", base+"/v1/sessions/u1/getUpdates",
		`{"group":"scan","offset":4,"timeout":0}`, itestAPP)
	if resp.code != 200 {
		t.Fatalf("scan ack pull=%d %s", resp.code, resp.body)
	}
	if depth := app.Store.QueueDepth("u1"); depth != 0 {
		t.Fatalf("depth after both acks=%d", depth)
	}

	// cursors are visible
	groups := app.Store.ListGroups("u1")
	if len(groups) != 2 {
		t.Fatalf("groups=%+v", groups)
	}
	for _, g := range groups {
		if g.AckedSeq != 3 {
			t.Fatalf("group %s cursor=%d (want 3)", g.Group, g.AckedSeq)
		}
	}
}

func TestReaderGroupIndependentReplay(t *testing.T) {
	fake := &fakeMTClient{}
	app, ts := newTestApp(t, fake, t.TempDir())
	base := ts.URL
	regADM(t, base, "u1")

	pushTestUpdate(t, app, "u1", docUpdate(111, 10))
	pushTestUpdate(t, app, "u1", docUpdate(111, 11))

	// two groups both read from 0
	for _, g := range []string{"live", "shadow"} {
		code, body := mtDo(t, "POST", base, "/v1/sessions/u1/getUpdates",
			`{"group":"`+g+`","timeout":0}`, itestAPP)
		if code != 200 {
			t.Fatalf("%s pull=%d", g, code)
		}
		res, _ := body["result"].([]any)
		if len(res) != 2 {
			t.Fatalf("%s got %d", g, len(res))
		}
	}
	// only "live" acks; "shadow" must still see both
	mtRaw(t, "POST", base+"/v1/sessions/u1/getUpdates",
		`{"group":"live","offset":12,"timeout":0}`, itestAPP)
	code, body := mtDo(t, "POST", base, "/v1/sessions/u1/getUpdates",
		`{"group":"shadow","timeout":0}`, itestAPP)
	res, _ := body["result"].([]any)
	if code != 200 || len(res) != 2 {
		t.Fatalf("shadow after live ack=%d %v", len(res), res)
	}
	// rows survive for the shadow group
	if depth := app.Store.QueueDepth("u1"); depth != 2 {
		t.Fatalf("depth=%d", depth)
	}
}

func TestGroupFilterShape(t *testing.T) {
	st := store.GroupCursor{Channels: `[111,222]`, DocumentOnly: true}
	f := groupFilter(nil, st)
	if f == nil {
		t.Fatal("filter must exist")
	}
	if !f(docUpdate(111, 1)) {
		t.Fatal("doc on listed channel must pass")
	}
	if f(textUpdate(111, 2)) {
		t.Fatal("text must fail document_only")
	}
	if f(docUpdate(333, 3)) {
		t.Fatal("unlisted channel must fail")
	}
	// no filters → nil (raw splice path)
	if groupFilter(nil, store.GroupCursor{}) != nil {
		t.Fatal("unfiltered group → nil filter")
	}
}

func TestSerializedFeedMatchesDocumentFilter(t *testing.T) {
	// the golden-path guarantee the scanner depends on: what SerializeUpdate
	// produces must satisfy the document_only group filter
	su := SerializeUpdate(&tg.UpdateNewChannelMessage{
		Message: &tg.Message{
			ID:      5,
			PeerID:  &tg.PeerChannel{ChannelID: 42},
			Message: "with a document",
			Media: &tg.MessageMediaDocument{Document: &tg.Document{
				ID: 9, MimeType: "video/mp4",
			}},
		},
		Pts: 100,
	})
	if su == nil {
		t.Fatal("serialization produced nothing")
	}
	raw, _ := json.Marshal(su)
	var back map[string]any
	_ = json.Unmarshal(raw, &back)
	f := groupFilter(nil, store.GroupCursor{DocumentOnly: true})
	if !f(back) {
		t.Fatalf("serialized doc update rejected by filter: %s", raw)
	}
}
