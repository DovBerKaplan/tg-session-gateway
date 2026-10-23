package botapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/DovBerKaplan/tg-session-gateway/internal/rateguard"
)

// gwResponse is what a proxy call produced: either gateway-generated
// JSON (jsonBody) or a verbatim upstream passthrough (raw).
type gwResponse struct {
	status   int
	jsonBody any
	raw      []byte
}

func jsonResp(status int, body any) *gwResponse { return &gwResponse{status: status, jsonBody: body} }

func (r *gwResponse) isRaw() bool { return r.raw != nil }

// Proxy is the Bot-API-compatible passthrough with Rate Guard.
type Proxy struct {
	mgr              *Manager
	client           HTTPClient
	queueWaitTimeout float64
}

// NewProxy builds the proxy around the manager/upstream client.
func NewProxy(m *Manager, client HTTPClient, queueWaitTimeout float64) *Proxy {
	return &Proxy{mgr: m, client: client, queueWaitTimeout: queueWaitTimeout}
}

// multipartChatIDRe finds the chat_id form field in a multipart body.
var multipartChatIDRe = regexp.MustCompile(`name="chat_id"\r?\n\r?\n(-?\d+)\r?\n`)

// Call routes one consumer request for a session.
func (p *Proxy) Call(ctx context.Context, s *BotSession, method string, body map[string]any, contentType string, rawBody []byte, idemKey string) *gwResponse {
	m := strings.ToLower(method)
	if rawBody != nil { // multipart/form-data — verbatim passthrough
		return p.forwardRaw(ctx, s, method, rawBody, contentType)
	}
	body = p.resolveUsernameChat(ctx, s, body)

	if gatewayOwned[m] {
		switch m {
		case "getupdates":
			return p.internalGetUpdates(ctx, s, body)
		case "deletewebhook":
			// consumers call this on boot (aiogram start_polling does);
			// the gateway already deleted the webhook when it attached.
			// Answer success so drop-in consumers don't die.
			return jsonResp(200, map[string]any{"ok": true, "result": true})
		default:
			return jsonResp(200, map[string]any{
				"ok": false, "error_code": 403,
				"description": "Conflict: gateway owns this method (spec §4.6)",
			}).withStatus(403)
		}
	}

	chatID := extractChatID(body)
	kind := rateguard.MethodKind(m)

	// ── spec E2: Idempotency-Key header (writes only) ─────────────
	idemActive := idemKey != "" && kind == "write"
	if idemActive {
		verdict := p.mgr.store.IdempotencyClaim(s.Alias, idemKey)
		if verdict == "replay" {
			return jsonResp(200, map[string]any{
				"ok": true, "result": nil, "idempotent_replay": true,
				"description": "duplicate send suppressed (Idempotency-Key already seen)",
			})
		}
	}

	// ── auto-typing bookkeeping (spec §10.4) ──────────────────────
	if m == "sendchataction" {
		action := strings.ToLower(toStr(body["action"]))
		if action == "cancel" {
			p.mgr.Typing().Stop(s, chatID)
			return jsonResp(200, map[string]any{"ok": true, "result": true})
		}
		if action == "cancel_timeout" {
			p.mgr.Typing().CancelTimeout(s, chatID)
			return jsonResp(200, map[string]any{"ok": true, "result": true})
		}
		// any other action: the consumer drives the indicator itself
		p.mgr.Typing().Stop(s, chatID)
	} else if m == "answercallbackquery" {
		// answering the button press IS the reply to that action
		p.mgr.Typing().StopByCallbackID(s, body["callback_query_id"])
	} else if kind == "write" && chatID != nil {
		p.mgr.Typing().Stop(s, chatID)
	}

	// paid lane (spec §8.2 §8): allow_paid_broadcast rides the 1000/s
	// bucket ONLY with the gateway-side double opt-in
	paidRequested := truthy(body["allow_paid_broadcast"])
	s.guard.EnterPaidLane(paidRequested)
	defer s.guard.ExitPaidLane()
	if paidRequested && !s.PaidBroadcasts {
		if idemActive {
			p.mgr.store.IdempotencyFail(s.Alias, idemKey)
		}
		return jsonResp(403, map[string]any{
			"ok": false, "error_code": 403,
			"description": "allow_paid_broadcast requires the gateway-side paid_broadcasts opt-in for this bot",
		})
	}

	resp := p.admitAndForward(ctx, s, m, method, body, chatID, kind)
	if idemActive {
		if resp.status < 400 {
			p.mgr.store.IdempotencyFinish(s.Alias, idemKey)
		} else {
			p.mgr.store.IdempotencyFail(s.Alias, idemKey)
		}
	}
	return resp
}

// admitAndForward runs admission control then forwards upstream,
// learning from real 429s (highest authority, spec §8.2 §5–6).
func (p *Proxy) admitAndForward(ctx context.Context, s *BotSession, m, method string, body map[string]any, chatID *int64, kind string) *gwResponse {
	waited := 0.0
	for !s.guard.TryAcquire(m, chatID) {
		if s.EffectiveOnLimit() == "reject" || kind != "write" {
			s.noteSynthetic429()
			retry := max(1.0, min(s.guard.WaitTime(m, chatID), 60.0))
			return jsonResp(429, map[string]any{
				"ok": false, "error_code": 429,
				"description": fmt.Sprintf("Too Many Requests: retry after %d", int(retry)),
				"parameters":  map[string]any{"retry_after": int(retry)},
			})
		}
		wait := min(s.guard.WaitTime(m, chatID), 2.0)
		if waited+wait > p.queueWaitTimeout {
			s.noteSynthetic429()
			return jsonResp(429, map[string]any{
				"ok": false, "error_code": 429,
				"description": "Queue wait exceeded GW_QUEUE_WAIT_TIMEOUT",
				"parameters":  map[string]any{"retry_after": 1},
			})
		}
		select {
		case <-ctx.Done():
			return jsonResp(429, map[string]any{
				"ok": false, "error_code": 429,
				"description": "Queue wait exceeded GW_QUEUE_WAIT_TIMEOUT",
				"parameters":  map[string]any{"retry_after": 1},
			})
		case <-time.After(time.Duration(wait * float64(time.Second))):
		}
		waited += wait
	}

	raw, err := json.Marshal(body)
	if err != nil {
		return jsonResp(500, map[string]any{"ok": false, "error_code": 500, "description": err.Error()})
	}
	resp, err := p.client.Do(&httpRequest{
		Method:  "POST",
		URL:     "/bot" + s.Token + "/" + method,
		RawBody: raw,
		ctx:     ctx,
	})
	if err != nil {
		return jsonResp(502, map[string]any{
			"ok": false, "error_code": 502, "description": "upstream: " + err.Error(),
		})
	}
	if kind == "write" {
		s.noteSend()
	}
	if resp.StatusCode == 429 {
		s.noteReal429()
		retry := 5.0
		var parsed map[string]any
		if json.Unmarshal(resp.Body, &parsed) == nil {
			if params, ok := parsed["parameters"].(map[string]any); ok {
				if v := numberOf(params["retry_after"]); v > 0 {
					retry = v
				}
			}
		}
		s.guard.Report429(chatID, retry)
		p.mgr.RecordBackoff(s, chatID, retry) // durable across restarts
		s.mu.Lock()
		s.last429 = map[string]any{
			"method": method, "chat_id": nullableInt(chatID),
			"retry_after": retry, "at": wallNow(),
		}
		s.mu.Unlock()
	}
	return &gwResponse{status: resp.StatusCode, raw: resp.Body}
}

// ForwardStream passes a pre-sized multipart body through verbatim
// WITHOUT buffering it in full: prefix (the leading bytes, already
// read so the chat_id form field can be mined) is re-joined with the
// unread rest of the body. Same admission and result handling as
// forwardRaw.
func (p *Proxy) ForwardStream(ctx context.Context, s *BotSession, method string, prefix []byte, rest io.Reader, total int64, contentType string) *gwResponse {
	return p.forwardMultipart(ctx, s, method, prefix,
		io.MultiReader(bytes.NewReader(prefix), rest), total, contentType)
}

// forwardRaw passes buffered multipart bodies through verbatim.
func (p *Proxy) forwardRaw(ctx context.Context, s *BotSession, method string, raw []byte, contentType string) *gwResponse {
	return p.forwardMultipart(ctx, s, method, raw, bytes.NewReader(raw), int64(len(raw)), contentType)
}

// forwardMultipart is the shared multipart path: Rate Guard admission
// keyed on the chat_id form field (mined from the prefix), verbatim
// forwarding (buffered or streamed), 429 learning, raw passthrough of
// the response.
func (p *Proxy) forwardMultipart(ctx context.Context, s *BotSession, method string, prefix []byte, body io.Reader, total int64, contentType string) *gwResponse {
	var chatID *int64
	if mm := multipartChatIDRe.FindSubmatch(prefix); mm != nil {
		if v, err := strconv.ParseInt(string(mm[1]), 10, 64); err == nil {
			chatID = &v
		}
	}
	// a multipart write IS the reply — stop the auto-typing loop first
	p.mgr.Typing().Stop(s, chatID)
	if !s.guard.TryAcquire(method, chatID) {
		s.noteSynthetic429()
		retry := max(1, min(s.guard.WaitTime(method, chatID), 60))
		return jsonResp(429, map[string]any{
			"ok": false, "error_code": 429,
			"description": fmt.Sprintf("Too Many Requests: retry after %d", int(retry)),
			"parameters":  map[string]any{"retry_after": int(retry)},
		})
	}
	resp, err := p.client.Do(&httpRequest{
		Method:        "POST",
		URL:           "/bot" + s.Token + "/" + method,
		Reader:        body,
		ContentLength: total,
		Header:        map[string]string{"Content-Type": contentType},
		ctx:           ctx,
	})
	if err != nil {
		return jsonResp(502, map[string]any{
			"ok": false, "error_code": 502, "description": "upstream: " + err.Error(),
		})
	}
	s.noteSend()
	if resp.StatusCode == 429 {
		s.noteReal429()
		retry := 5.0
		var parsed map[string]any
		if json.Unmarshal(resp.Body, &parsed) == nil {
			if params, ok := parsed["parameters"].(map[string]any); ok {
				if v := numberOf(params["retry_after"]); v > 0 {
					retry = v
				}
			}
		}
		s.guard.Report429(chatID, retry)
		p.mgr.RecordBackoff(s, chatID, retry)
		s.mu.Lock()
		s.last429 = map[string]any{
			"method": method, "chat_id": nullableInt(chatID),
			"retry_after": retry, "at": wallNow(),
		}
		s.mu.Unlock()
	}
	return &gwResponse{status: resp.StatusCode, raw: resp.Body}
}

// internalGetUpdates serves from the internal queue with Bot API
// long-poll semantics.
func (p *Proxy) internalGetUpdates(ctx context.Context, s *BotSession, body map[string]any) *gwResponse {
	timeout := min(numberOf(body["timeout"]), 50.0)
	limit := int(numberOf(body["limit"]))
	if limit == 0 {
		limit = 100
	}
	var offset *int64
	if v, ok := body["offset"]; ok && v != nil {
		switch n := v.(type) {
		case float64:
			i := int64(n)
			offset = &i
		case int64:
			offset = &n
		case string:
			if i, err := strconv.ParseInt(n, 10, 64); err == nil {
				offset = &i
			}
		}
	}
	wait := timeout
	if wait <= 0 {
		wait = 0.01
	}
	updates := s.queue.Pull(offset, wait, limit, ctx.Done())
	if updates == nil {
		updates = []map[string]any{}
	}
	return jsonResp(200, map[string]any{"ok": true, "result": updates})
}

// resolveUsernameChat resolves @username targets to numeric ids once
// (getChat cache, spec §8.2 §4) so they land in the right per-chat
// bucket.
func (p *Proxy) resolveUsernameChat(ctx context.Context, s *BotSession, body map[string]any) map[string]any {
	cid, ok := body["chat_id"].(string)
	if !ok || !strings.HasPrefix(cid, "@") {
		return body
	}
	s.mu.Lock()
	cached, hit := s.chatNameCache[cid]
	s.mu.Unlock()
	if hit {
		body = cloneMapAny(body)
		body["chat_id"] = cached
		return body
	}
	ctxTO, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	resp, err := p.client.Do(&httpRequest{
		Method: "POST", URL: "/bot" + s.Token + "/getChat",
		Body: map[string]any{"chat_id": cid}, ctx: ctxTO,
	})
	if err == nil {
		var parsed map[string]any
		if json.Unmarshal(resp.Body, &parsed) == nil {
			if okRes, _ := parsed["ok"].(bool); okRes {
				if result, ok := parsed["result"].(map[string]any); ok {
					if numeric := numberOf(result["id"]); numeric != 0 {
						s.mu.Lock()
						s.chatNameCache[cid] = int64(numeric)
						s.mu.Unlock()
						body = cloneMapAny(body)
						body["chat_id"] = int64(numeric)
					}
				}
			}
		}
	}
	// resolution failed — request proceeds, global bucket only
	return body
}

// extractChatID mirrors the Python _chat_id: chat_id | chat_username |
// chat; bools excluded; @usernames → nil (global bucket only).
func extractChatID(body map[string]any) *int64 {
	var raw any
	for _, k := range []string{"chat_id", "chat_username", "chat"} {
		if v, ok := body[k]; ok && v != nil && !truthyBool(v) && !isFalseBool(v) {
			raw = v
			break
		}
	}
	switch v := raw.(type) {
	case nil:
		return nil
	case float64:
		if v == float64(int64(v)) {
			i := int64(v)
			return &i
		}
	case int:
		i := int64(v)
		return &i
	case int64:
		return &v
	case string:
		if isNumeric(v) {
			i, err := strconv.ParseInt(v, 10, 64)
			if err == nil {
				return &i
			}
		}
	}
	return nil
}

func isNumeric(s string) bool {
	if s == "" {
		return false
	}
	i := 0
	if s[0] == '-' {
		i = 1
	}
	if i >= len(s) {
		return false
	}
	for ; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func truthy(v any) bool { return truthyBool(v) }

func truthyBool(v any) bool {
	b, ok := v.(bool)
	return ok && b
}

func isFalseBool(v any) bool {
	b, ok := v.(bool)
	return ok && !b
}

func nullableInt(p *int64) any {
	if p == nil {
		return nil
	}
	return *p
}

func cloneMapAny(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func (r *gwResponse) withStatus(status int) *gwResponse {
	r.status = status
	return r
}
