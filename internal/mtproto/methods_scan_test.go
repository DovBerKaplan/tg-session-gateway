package mtproto

import (
	"encoding/base64"
	"strings"
	"testing"
)

// scanProxy runs one facade method against a registered live session.
func scanProxy(t *testing.T, base, alias, method, body string) (int, map[string]any) {
	t.Helper()
	return mtDo(t, "POST", base, "/v1/sessions/"+alias+"/"+method, body, itestAPP)
}

func TestScanMethodsHappyPath(t *testing.T) {
	fake := &fakeMTClient{}
	app, ts := newTestApp(t, fake, t.TempDir())
	base := ts.URL
	regADM(t, base, "u1")
	waitLive(t, app, fake, "u1")

	// get_history: single window, messages serialized
	code, body := scanProxy(t, base, "u1", "get_history", `{"chat_id":-100123,"limit":5}`)
	if code != 200 || body["ok"] != true {
		t.Fatalf("get_history=%d %v", code, body)
	}
	result := body["result"].(map[string]any)
	msgs := result["messages"].([]any)
	if len(msgs) != 5 || result["count"] != float64(5) {
		t.Fatalf("history result=%v", result)
	}

	// get_history requires chat_id
	code, _ = scanProxy(t, base, "u1", "get_history", `{}`)
	if code != 500 { // adapter error → 500 (missing arg), not 501
		t.Fatalf("get_history no chat=%d", code)
	}

	// get_messages preserves requested order
	code, body = scanProxy(t, base, "u1", "get_messages", `{"chat_id":-100123,"message_ids":[7,3,9]}`)
	if code != 200 {
		t.Fatalf("get_messages=%d %v", code, body)
	}
	msgs = body["result"].(map[string]any)["messages"].([]any)
	if len(msgs) != 3 || msgs[0].(map[string]any)["id"] != float64(7) ||
		msgs[1].(map[string]any)["id"] != float64(3) {
		t.Fatalf("order not preserved: %v", msgs)
	}

	// get_dialogs
	code, body = scanProxy(t, base, "u1", "get_dialogs", `{"limit":10}`)
	if code != 200 {
		t.Fatalf("get_dialogs=%d %v", code, body)
	}
	dialogs := body["result"].(map[string]any)["dialogs"].([]any)
	if len(dialogs) != 3 { // 2 channels + stats row
		t.Fatalf("dialogs=%v", dialogs)
	}
	first := dialogs[0].(map[string]any)
	if first["access_hash"] != float64(11) || first["kind"] != "channel" {
		t.Fatalf("first dialog=%v", first)
	}

	// get_entity by id and by username
	code, body = scanProxy(t, base, "u1", "get_entity", `{"chat_id":42}`)
	if code != 200 || body["result"].(map[string]any)["_"] != "User" {
		t.Fatalf("get_entity user=%d %v", code, body)
	}
	code, body = scanProxy(t, base, "u1", "get_entity", `{"username":"somechan"}`)
	if code != 200 {
		t.Fatalf("get_entity username=%d %v", code, body)
	}

	// channel admin surface
	code, body = scanProxy(t, base, "u1", "get_full_channel", `{"chat_id":-100123}`)
	if code != 200 || body["result"].(map[string]any)["participants_count"] != float64(42) {
		t.Fatalf("get_full_channel=%d %v", code, body)
	}
	code, body = scanProxy(t, base, "u1", "get_participant", `{"chat_id":-100123,"user_id":5}`)
	if code != 200 || body["result"].(map[string]any)["rank"] != "scanner" {
		t.Fatalf("get_participant=%d %v", code, body)
	}
	code, body = scanProxy(t, base, "u1", "get_participants", `{"chat_id":-100123,"filter":"admins"}`)
	if code != 200 {
		t.Fatalf("get_participants=%d %v", code, body)
	}
	code, _ = scanProxy(t, base, "u1", "get_participants", `{"chat_id":-100123,"filter":"nope"}`)
	if code != 500 {
		t.Fatalf("bad filter=%d", code)
	}

	// send_media by document reference (server-side copy)
	ref := base64.StdEncoding.EncodeToString([]byte("refbytes"))
	code, body = scanProxy(t, base, "u1", "send_media",
		`{"chat_id":-100456,"doc_id":77,"access_hash":88,"file_reference":"`+ref+`","caption":"copy"}`)
	if code != 200 || body["result"].(map[string]any)["id"] != float64(5001) {
		t.Fatalf("send_media=%d %v", code, body)
	}
	fake.mu.Lock()
	if len(fake.sentDocs) != 1 || fake.sentDocs[0].ID != 77 ||
		fake.sentDocs[0].AccessHash != 88 || string(fake.sentDocs[0].FileReference) != "refbytes" {
		fake.mu.Unlock()
		t.Fatalf("sentDocs=%+v", fake.sentDocs)
	}
	fake.mu.Unlock()
	// send_media requires doc_id
	code, _ = scanProxy(t, base, "u1", "send_media", `{"chat_id":-100456}`)
	if code != 500 {
		t.Fatalf("send_media no doc=%d", code)
	}

	// channel writes
	code, body = scanProxy(t, base, "u1", "create_channel", `{"title":"store","about":"a"}`)
	if code != 200 || body["result"].(map[string]any)["title"] != "store" {
		t.Fatalf("create_channel=%d %v", code, body)
	}
	code, _ = scanProxy(t, base, "u1", "create_channel", `{}`)
	if code != 500 {
		t.Fatalf("create_channel no title=%d", code)
	}
	code, _ = scanProxy(t, base, "u1", "edit_channel",
		`{"chat_id":-100200,"signatures":true,"no_forwards":true}`)
	if code != 200 {
		t.Fatalf("edit_channel toggles=%d", code)
	}
	code, _ = scanProxy(t, base, "u1", "edit_channel", `{"chat_id":-100200}`)
	if code != 500 { // nothing to edit
		t.Fatalf("edit_channel empty=%d", code)
	}
	code, _ = scanProxy(t, base, "u1", "delete_channel", `{"chat_id":-100201}`)
	if code != 200 {
		t.Fatalf("delete_channel=%d", code)
	}
	code, _ = scanProxy(t, base, "u1", "edit_admin",
		`{"chat_id":-100202,"user_id":5,"rights":["post_messages","anonymous"],"rank":"bot"}`)
	if code != 200 {
		t.Fatalf("edit_admin=%d", code)
	}
	code, _ = scanProxy(t, base, "u1", "edit_admin", `{"chat_id":-100202,"user_id":5}`)
	if code != 500 { // no rights
		t.Fatalf("edit_admin no rights=%d", code)
	}
	code, _ = scanProxy(t, base, "u1", "join_channel", `{"chat_id":-100203}`)
	if code != 200 {
		t.Fatalf("join_channel=%d", code)
	}
	code, _ = scanProxy(t, base, "u1", "leave_channel", `{"chat_id":-100204}`)
	if code != 200 {
		t.Fatalf("leave_channel=%d", code)
	}

	// send_read_acknowledge records the ack (the ONLY read path)
	code, _ = scanProxy(t, base, "u1", "send_read_acknowledge", `{"chat_id":-100205,"max_id":55}`)
	if code != 200 {
		t.Fatalf("send_read_acknowledge=%d", code)
	}
	fake.mu.Lock()
	if len(fake.readAcks) != 1 || fake.readAcks[0] != [2]int64{-100205, 55} {
		fake.mu.Unlock()
		t.Fatalf("readAcks=%v", fake.readAcks)
	}
	fake.mu.Unlock()
}

func TestScanMethodsInReadonlyGroupAnd501(t *testing.T) {
	fake := &fakeMTClient{}
	app, ts := newTestApp(t, fake, t.TempDir())
	base := ts.URL
	regADM(t, base, "u1")
	waitLive(t, app, fake, "u1")

	// every new read method is inside the readonly group
	for _, m := range []string{"get_history", "get_messages", "get_dialogs", "get_entity", "get_full_channel", "get_participant", "get_participants"} {
		if !methodInReadonlyGroup(m) {
			t.Fatalf("%s missing from readonly group", m)
		}
	}
	for _, m := range []string{"send_media", "create_channel", "edit_channel", "delete_channel", "edit_admin", "join_channel", "leave_channel", "send_read_acknowledge"} {
		if methodInReadonlyGroup(m) {
			t.Fatalf("%s must NOT be in readonly group", m)
		}
	}

	// still 501 with the supported list for unknown methods
	code, body := scanProxy(t, base, "u1", "get_messages_batch", `{}`)
	if code != 501 {
		t.Fatalf("unknown=%d", code)
	}
	desc := body["description"].(string)
	for _, m := range []string{"get_history", "get_dialogs", "send_read_acknowledge"} {
		if !strings.Contains(desc, m) {
			t.Fatalf("501 list missing %s: %q", m, desc)
		}
	}
}

func methodInReadonlyGroup(method string) bool {
	allow, err := ExpandAllowlist([]string{"readonly"})
	if err != nil {
		return false
	}
	return allow[method]
}

func TestReadonlyClientOnScanMethods(t *testing.T) {
	fake := &fakeMTClient{}
	app, ts := newTestApp(t, fake, t.TempDir())
	base := ts.URL
	regADM(t, base, "u1")
	waitLive(t, app, fake, "u1")

	_, bearer := registerTestClient(t, base, "shadow", `["readonly"]`, `["u1"]`)
	code, _ := mtDo(t, "POST", base, "/v1/sessions/u1/get_history", `{"chat_id":-100123}`, bearer)
	if code != 200 {
		t.Fatalf("readonly get_history=%d", code)
	}
	code, _ = mtDo(t, "POST", base, "/v1/sessions/u1/send_media", `{"chat_id":-100456,"doc_id":1}`, bearer)
	if code != 403 {
		t.Fatalf("readonly send_media=%d (want 403)", code)
	}
	code, _ = mtDo(t, "POST", base, "/v1/sessions/u1/send_read_acknowledge", `{"chat_id":-100123}`, bearer)
	if code != 403 {
		t.Fatalf("readonly send_read_acknowledge=%d (want 403 — it mutates)", code)
	}
}
