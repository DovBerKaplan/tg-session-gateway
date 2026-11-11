package mtproto

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/DovBerKaplan/tg-session-gateway/internal/store"
)

// clientTokenPrefix marks gateway-issued client tokens. Bot tokens keep
// their own shape (digits:AA…); a client token never reaches Telegram.
const clientTokenPrefix = "tggc_"

// Caller is the resolved consumer identity behind one request. The
// legacy GW_APP_SECRET bearer resolves to a Legacy caller with full
// access; every other bearer must be an issued client token.
type Caller struct {
	ID     string // "" for the legacy caller
	Name   string
	Legacy bool
	Lane   string // interactive|normal|bulk (milestone 4 consumes it)

	allow    map[string]bool // expanded method allowlist; nil = everything
	sessions map[string]bool // explicit session scope; nil = every session
}

// MethodAllowed reports whether the caller may dispatch this method.
func (c *Caller) MethodAllowed(method string) bool {
	if c == nil || c.allow == nil {
		return true
	}
	return c.allow[strings.ToLower(method)]
}

// SessionAllowed reports whether the caller may touch this session.
func (c *Caller) SessionAllowed(alias string) bool {
	if c == nil || c.sessions == nil {
		return true
	}
	return c.sessions[alias]
}

// AllowsUpdates reports whether the caller may pull or subscribe to the
// update stream (getUpdates / WS).
func (c *Caller) AllowsUpdates() bool {
	if c == nil {
		return false
	}
	return c.MethodAllowed("get_updates")
}

// legacyCaller is the GW_APP_SECRET identity: everything, every session.
func legacyCaller(lane string) *Caller {
	return &Caller{Name: "legacy-app-secret", Legacy: true, Lane: lane}
}

// hashToken is the stored/auth form of a client token.
func hashToken(token string) []byte {
	h := sha256.Sum256([]byte(token))
	return h[:]
}

// newClientToken mints "tggc_<48 hex chars>" (24 random bytes).
func newClientToken() (string, error) {
	raw := make([]byte, 24)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return clientTokenPrefix + hex.EncodeToString(raw), nil
}

// newClientID mints a short admin-facing id ("cl-<8 hex>").
func newClientID() (string, error) {
	raw := make([]byte, 4)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return "cl-" + hex.EncodeToString(raw), nil
}

// Allowlist groups expand to method sets at issue time, so enforcement
// stays a plain map hit. New methods join a group automatically only
// after a re-issue; validation rejects unknown entries up front.
var allowlistGroups = map[string]func() []string{
	"full":     SupportedMethods,
	"readonly": ReadOnlyMethods,
}

// ReadOnlyMethods lists the read-classified allowlist entries (the
// `readonly` group). send_read_acknowledge stays OUT: it mutates state.
func ReadOnlyMethods() []string {
	out := []string{"get_updates"}
	for _, m := range SupportedMethods() {
		if strings.HasPrefix(m, "get_") || strings.HasPrefix(m, "resolve_") {
			out = append(out, m)
		}
	}
	sort.Strings(out)
	return out
}

// ExpandAllowlist turns entries (method names or groups) into the
// enforced method set. "full" or an empty list grants everything; an
// unknown entry is a 400-grade error, not a silent no-op.
func ExpandAllowlist(entries []string) (map[string]bool, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	known := map[string]bool{}
	for _, m := range SupportedMethods() {
		known[m] = true
	}
	known["get_updates"] = true
	out := map[string]bool{}
	for _, raw := range entries {
		entry := strings.ToLower(strings.TrimSpace(raw))
		if entry == "" {
			continue
		}
		if entry == "full" {
			return nil, nil
		}
		if expand, ok := allowlistGroups[entry]; ok {
			for _, m := range expand() {
				out[m] = true
			}
			continue
		}
		if !known[entry] {
			return nil, fmt.Errorf("unknown allowlist entry %q (not a supported method or group: full, readonly)", raw)
		}
		out[entry] = true
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

// ClientIndex resolves bearers to Callers from an in-memory snapshot of
// the clients table (client counts are small; mutations reload it).
type ClientIndex struct {
	st *store.MT

	mu     sync.RWMutex
	byHash map[string]*Caller
}

// NewClientIndex wires the index against the store.
func NewClientIndex(st *store.MT) *ClientIndex {
	return &ClientIndex{st: st, byHash: map[string]*Caller{}}
}

// Reload swaps the snapshot; every admin mutation calls it.
func (ix *ClientIndex) Reload() error {
	creds, err := ix.st.ListClientCreds()
	if err != nil {
		return err
	}
	byHash := make(map[string]*Caller, len(creds))
	for _, c := range creds {
		allow, err := ExpandAllowlist(c.Allowlist)
		if err != nil {
			// stored rows were validated at issue time; skip garbage
			// rather than refuse every bearer on one bad row
			continue
		}
		scope := map[string]bool{}
		for _, a := range c.Sessions {
			scope[a] = true
		}
		byHash[hex.EncodeToString(c.TokenHash)] = &Caller{
			ID: c.ID, Name: c.Name, Lane: c.Lane,
			allow: allow, sessions: scope,
		}
	}
	ix.mu.Lock()
	ix.byHash = byHash
	ix.mu.Unlock()
	return nil
}

// Resolve maps a bearer token to its Caller (nil when unknown, revoked
// or wrongly shaped). Lookup is by SHA-256, so the raw token is never
// a key anywhere.
func (ix *ClientIndex) Resolve(token string) *Caller {
	if !strings.HasPrefix(token, clientTokenPrefix) {
		return nil
	}
	key := hex.EncodeToString(hashToken(token))
	ix.mu.RLock()
	c := ix.byHash[key]
	ix.mu.RUnlock()
	return c
}
