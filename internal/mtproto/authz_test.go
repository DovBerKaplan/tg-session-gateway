package mtproto

import (
	"strings"
	"testing"
)

func TestExpandAllowlistFullAndEmpty(t *testing.T) {
	for _, entries := range [][]string{nil, {}, {"full"}} {
		got, err := ExpandAllowlist(entries)
		if err != nil || got != nil {
			t.Fatalf("entries=%v got=%v err=%v — full/empty must grant everything", entries, got, err)
		}
	}
}

func TestExpandAllowlistReadonly(t *testing.T) {
	got, err := ExpandAllowlist([]string{"readonly"})
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range []string{"get_me", "get_chat", "resolve_username", "get_updates"} {
		if !got[m] {
			t.Fatalf("readonly must include %s", m)
		}
	}
	for _, m := range []string{"send_message", "delete_messages", "edit_message_text"} {
		if got[m] {
			t.Fatalf("readonly must NOT include %s", m)
		}
	}
}

func TestExpandAllowlistUnknownEntry(t *testing.T) {
	if _, err := ExpandAllowlist([]string{"send_message", "sendmassage"}); err == nil {
		t.Fatal("unknown entry must error (typo protection)")
	}
}

func TestExpandAllowlistSpecificMethods(t *testing.T) {
	got, err := ExpandAllowlist([]string{"send_message", " Get_Chat "})
	if err != nil {
		t.Fatal(err)
	}
	if !got["send_message"] || !got["get_chat"] || got["get_me"] {
		t.Fatalf("got=%v", got)
	}
}

func TestCallerChecks(t *testing.T) {
	legacy := legacyCaller("")
	if !legacy.MethodAllowed("send_message") || !legacy.SessionAllowed("any") || !legacy.AllowsUpdates() {
		t.Fatal("legacy caller must be unrestricted")
	}
	allow, err := ExpandAllowlist([]string{"get_me"})
	if err != nil {
		t.Fatal(err)
	}
	c := &Caller{ID: "cl-x", allow: allow, sessions: map[string]bool{"acc1": true}}
	if c.MethodAllowed("send_message") {
		t.Fatal("send_message outside allowlist")
	}
	if !c.MethodAllowed("GET_ME") { // case-insensitive
		t.Fatal("get_me must pass case-insensitively")
	}
	if c.SessionAllowed("acc2") {
		t.Fatal("acc2 outside scope")
	}
	if c.AllowsUpdates() {
		t.Fatal("updates not in allowlist")
	}
}

func TestReadOnlyMethodsExcludesWrites(t *testing.T) {
	for _, m := range ReadOnlyMethods() {
		if strings.HasPrefix(m, "send_") || strings.HasPrefix(m, "edit_") || strings.HasPrefix(m, "delete_") ||
			strings.HasPrefix(m, "forward_") || strings.HasPrefix(m, "answer_") {
			t.Fatalf("readonly group leaked a write method: %s", m)
		}
	}
}

func TestNewClientTokenShape(t *testing.T) {
	tok, err := newClientToken()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(tok, clientTokenPrefix) || len(tok) != len(clientTokenPrefix)+48 {
		t.Fatalf("token shape=%q", tok)
	}
	id, err := newClientID()
	if err != nil || !strings.HasPrefix(id, "cl-") {
		t.Fatalf("id=%q err=%v", id, err)
	}
}
