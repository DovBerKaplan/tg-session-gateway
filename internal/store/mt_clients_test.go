package store

import (
	"bytes"
	"crypto/sha256"
	"testing"
)

func TestMTClientLifecycle(t *testing.T) {
	m := openTestMT(t, t.TempDir())
	hash := func(tok string) []byte {
		h := sha256.Sum256([]byte(tok))
		return h[:]
	}

	if err := m.RegisterClient("cl-aa", "scanner", hash("tggc_one"),
		[]string{"get_history", "get_messages"}, []string{"acc1"}, "bulk"); err != nil {
		t.Fatal(err)
	}
	if err := m.RegisterClient("cl-bb", "shadow", hash("tggc_two"),
		[]string{"readonly"}, []string{"acc1"}, ""); err != nil {
		t.Fatal(err)
	}

	rows, err := m.ListClients()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("clients = %+v", rows)
	}
	first := rows[0]
	if first.ID != "cl-aa" || first.Name != "scanner" || first.Lane != "bulk" ||
		len(first.Allowlist) != 2 || first.Sessions[0] != "acc1" || first.Revoked {
		t.Fatalf("row = %+v", first)
	}

	// hash resolution: exact match only, revoked excluded
	creds, ok := m.FindClientByHash(hash("tggc_one"))
	if !ok || creds.ID != "cl-aa" || creds.Name != "scanner" || len(creds.Sessions) != 1 {
		t.Fatalf("creds = %+v ok=%v", creds, ok)
	}
	if _, ok := m.FindClientByHash(hash("tggc_nope")); ok {
		t.Fatal("unknown hash must not resolve")
	}
	// near-miss hash (same length) must not resolve
	if _, ok := m.FindClientByHash(append(bytes.Clone(hash("tggc_one")), 0)); ok {
		t.Fatal("mismatched hash must not resolve")
	}

	// creds snapshot excludes revoked after revocation
	if ok, err := m.RevokeClient("cl-aa"); err != nil || !ok {
		t.Fatalf("revoke: %v %v", ok, err)
	}
	if ok, _ := m.RevokeClient("cl-missing"); ok {
		t.Fatal("revoking an unknown id must report false")
	}
	if _, ok := m.FindClientByHash(hash("tggc_one")); ok {
		t.Fatal("revoked client must not resolve")
	}
	snap, err := m.ListClientCreds()
	if err != nil || len(snap) != 1 || snap[0].ID != "cl-bb" {
		t.Fatalf("creds snapshot = %+v err=%v", snap, err)
	}
}

func TestMTClientUpdate(t *testing.T) {
	m := openTestMT(t, t.TempDir())
	h := sha256.Sum256([]byte("tggc_up"))
	if err := m.RegisterClient("cl-cc", "grow", h[:], []string{"get_me"}, []string{"a"}, ""); err != nil {
		t.Fatal(err)
	}

	// widen the allowlist, keep sessions and lane
	ok, err := m.UpdateClient("cl-cc", []string{"get_me", "get_chat"}, nil, nil)
	if err != nil || !ok {
		t.Fatalf("update: %v %v", ok, err)
	}
	creds, _ := m.FindClientByHash(h[:])
	if len(creds.Allowlist) != 2 {
		t.Fatalf("allowlist after update = %+v", creds)
	}

	// scope rewrite + lane
	lane := "bulk"
	if ok, _ := m.UpdateClient("cl-cc", nil, []string{"a", "b"}, &lane); !ok {
		t.Fatal("scope update failed")
	}
	creds, _ = m.FindClientByHash(h[:])
	if len(creds.Sessions) != 2 || creds.Lane != "bulk" {
		t.Fatalf("after scope update = %+v", creds)
	}

	// no-op update still reports existence
	if ok, _ := m.UpdateClient("cl-cc", nil, nil, nil); !ok {
		t.Fatal("no-op update must report the client exists")
	}
	if ok, _ := m.UpdateClient("cl-zz", []string{"get_me"}, nil, nil); ok {
		t.Fatal("unknown id must report false")
	}
}
