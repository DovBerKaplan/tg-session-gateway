// Package client is the typed Go consumer SDK for tggw, covering both
// planes over plain HTTP:
//
//	Bot API plane:  NewBotAPI(base, appSecret, botToken)
//	                methods are Bot API names (sendMessage, ...), the
//	                gateway serves getUpdates from its durable queue
//	                and accepts Idempotency-Key on writes.
//	MTProto plane:  NewMTProto(base, appSecret, alias)
//	                methods are snake_case (send_message, ...) served
//	                by the sidecar facade.
//
// Both planes answer the Telegram envelope ({"ok":true,"result":...}
// or {"ok":false,"error_code":N,...}); errors surface as *APIError
// with helpers for the cases consumers branch on (429 with
// retry_after, 403 gateway-owned, 501 unsupported).
package client

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Client is a typed consumer of one tggw gateway. Safe for
// concurrent use.
type Client struct {
	http      *http.Client
	base      string // e.g. http://tggw:8080 (no trailing slash)
	appSecret string // Bearer GW_APP_SECRET

	// exactly one of these is set
	botToken string // Bot API plane: /tgapi/bot<token>/...
	alias    string // MTProto plane: /v1/sessions/<alias>/...

	mu     sync.Mutex
	offset int64 // consumer cursor; advanced by PullUpdates
}

// NewBotAPI builds a client for the Bot API plane. botToken is the
// token registered in the gateway (alias-scoped paths exist too, but
// the token path needs no alias knowledge).
func NewBotAPI(base, appSecret, botToken string) *Client {
	return &Client{
		http:      &http.Client{},
		base:      strings.TrimRight(base, "/"),
		appSecret: appSecret,
		botToken:  botToken,
	}
}

// NewMTProto builds a client for the MTProto sidecar plane. alias is
// the registered session alias.
func NewMTProto(base, appSecret, alias string) *Client {
	return &Client{
		http:      &http.Client{},
		base:      strings.TrimRight(base, "/"),
		appSecret: appSecret,
		alias:     alias,
	}
}

// APIError is any gateway/upstream answer with ok:false.
type APIError struct {
	Code        int     // HTTP status (and error_code in the envelope)
	Description string  `json:"description"`
	RetryAfter  float64 `json:"-"` // from parameters.retry_after, when present
}

func (e *APIError) Error() string {
	s := fmt.Sprintf("tggw: %d: %s", e.Code, e.Description)
	if e.RetryAfter > 0 {
		return fmt.Sprintf("%s (retry after %.0fs)", s, e.RetryAfter)
	}
	return s
}

// ErrReplayed is returned by SendMessage when the gateway suppressed
// a duplicate Idempotency-Key: ok:true, but NOTHING was sent upstream.
var ErrReplayed = errors.New("tggw: duplicate Idempotency-Key suppressed (idempotent_replay)")

// IsRateLimited reports whether err is a 429 and, if so, its
// retry_after (0 when the gateway sent none).
func IsRateLimited(err error) (retryAfter float64, yes bool) {
	var apiErr *APIError
	if errors.As(err, &apiErr) && apiErr.Code == 429 {
		return apiErr.RetryAfter, true
	}
	return 0, false
}

// IsForbidden reports whether err is a 403 (e.g. a gateway-owned
// method, or paid broadcasts without opt-in).
func IsForbidden(err error) bool {
	var apiErr *APIError
	return errors.As(err, &apiErr) && apiErr.Code == 403
}

// IsUnsupported reports whether err is the sidecar's 501
// (method outside the adapter allowlist).
func IsUnsupported(err error) bool {
	var apiErr *APIError
	return errors.As(err, &apiErr) && apiErr.Code == 501
}

// Message is the subset of a sent message consumers actually use.
type Message struct {
	ID int64 `json:"message_id"`
	// Raw keeps the full Telegram message object verbatim.
	Raw json.RawMessage `json:"-"`
}

// Update is one queued update. Only UpdateID is typed; everything
// else stays verbatim so the SDK never lags the Bot API schema.
type Update struct {
	UpdateID int64           `json:"update_id"`
	Raw      json.RawMessage `json:"-"`
}

// Call performs an arbitrary method call and returns the raw result.
// The body is JSON-encoded as-is; nil means an empty object.
func (c *Client) Call(ctx context.Context, method string, body map[string]any) (json.RawMessage, error) {
	raw, _, err := c.do(ctx, method, body, "")
	return raw, err
}

// SendMessage sends text to chatID with an automatic Idempotency-Key
// (botapi plane): a transport-level retry of THIS call can never
// double-send. Use SendMessageIdempotent to control the key yourself.
// Returns ErrReplayed when the gateway suppressed a duplicate key.
func (c *Client) SendMessage(ctx context.Context, chatID int64, text string, extra map[string]any) (*Message, error) {
	return c.SendMessageIdempotent(ctx, newIdemKey(), chatID, text, extra)
}

// SendMessageIdempotent is SendMessage with a caller-controlled
// Idempotency-Key: the SAME key across process restarts deduplicates
// the send (the gateway stores it durably for the TTL window).
func (c *Client) SendMessageIdempotent(ctx context.Context, idemKey string, chatID int64, text string, extra map[string]any) (*Message, error) {
	body := map[string]any{"chat_id": chatID, "text": text}
	for k, v := range extra {
		body[k] = v
	}
	raw, replay, err := c.do(ctx, "sendMessage", body, idemKey)
	if err != nil {
		return nil, err
	}
	if replay {
		return nil, ErrReplayed
	}
	var m Message
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("tggw: decode message: %w", err)
	}
	m.Raw = raw
	return &m, nil
}

// PullUpdates drains the durable queue like a Telegram consumer:
// each call acks the previous cursor (the gateway persists it) and
// returns everything newer, waiting up to wait when the queue is
// empty. Across process restarts, seed the cursor with
// SetOffset(lastSeenUpdateID) — acked updates are never redelivered.
func (c *Client) PullUpdates(ctx context.Context, wait time.Duration, limit int) ([]Update, error) {
	c.mu.Lock()
	offset := c.offset
	c.mu.Unlock()
	body := map[string]any{"timeout": int(wait.Seconds()), "limit": limit}
	if offset > 0 {
		body["offset"] = offset + 1
	}
	raw, _, err := c.do(ctx, "getUpdates", body, "")
	if err != nil {
		return nil, err
	}
	var list []json.RawMessage
	if err := json.Unmarshal(raw, &list); err != nil {
		return nil, fmt.Errorf("tggw: decode updates: %w", err)
	}
	out := make([]Update, 0, len(list))
	for _, u := range list {
		var typed struct {
			UpdateID int64 `json:"update_id"`
		}
		if err := json.Unmarshal(u, &typed); err != nil {
			return nil, fmt.Errorf("tggw: decode update_id: %w", err)
		}
		out = append(out, Update{UpdateID: typed.UpdateID, Raw: u})
		if typed.UpdateID > offset {
			offset = typed.UpdateID
		}
	}
	c.mu.Lock()
	c.offset = offset
	c.mu.Unlock()
	return out, nil
}

// Offset reports the current consumer cursor (persist it yourself to
// survive restarts; see SetOffset).
func (c *Client) Offset() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.offset
}

// SetOffset seeds the consumer cursor (from persisted state).
func (c *Client) SetOffset(id int64) {
	c.mu.Lock()
	c.offset = id
	c.mu.Unlock()
}

// do is the single HTTP round trip: Telegram envelope in, typed error
// out. Returns (result, idempotentReplay, err).
func (c *Client) do(ctx context.Context, method string, body map[string]any, idemKey string) (json.RawMessage, bool, error) {
	var payload bytes.Buffer
	if err := json.NewEncoder(&payload).Encode(body); err != nil {
		return nil, false, err
	}
	url := c.methodURL(method)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, &payload)
	if err != nil {
		return nil, false, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.appSecret)
	if idemKey != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, false, fmt.Errorf("tggw: %s: %w", method, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, false, fmt.Errorf("tggw: %s: read: %w", method, err)
	}
	var envelope struct {
		Ok              bool            `json:"ok"`
		Result          json.RawMessage `json:"result"`
		ErrorCode       int             `json:"error_code"`
		Description     string          `json:"description"`
		IdempotentQuery bool            `json:"idempotent_replay"`
		Parameters      *struct {
			RetryAfter float64 `json:"retry_after"`
		} `json:"parameters"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return nil, false, fmt.Errorf("tggw: %s: bad envelope (status %d): %.200s", method, resp.StatusCode, raw)
	}
	if !envelope.Ok {
		apiErr := &APIError{Code: resp.StatusCode, Description: envelope.Description}
		if envelope.ErrorCode != 0 {
			apiErr.Code = envelope.ErrorCode
		}
		if envelope.Parameters != nil {
			apiErr.RetryAfter = envelope.Parameters.RetryAfter
		}
		return nil, false, apiErr
	}
	return envelope.Result, envelope.IdempotentQuery, nil
}

func (c *Client) methodURL(method string) string {
	if c.botToken != "" {
		return c.base + "/tgapi/bot" + c.botToken + "/" + method
	}
	return c.base + "/v1/sessions/" + c.alias + "/" + method
}

func newIdemKey() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("tggw-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}
