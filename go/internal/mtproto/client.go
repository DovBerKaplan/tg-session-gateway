// Package mtproto is the MTProto session sidecar: long-lived gotd
// clients, one per bot token, behind the same HTTP surface the
// pyrofork sidecar exposed. The Python sidecar proxied ANY Pyrogram
// method via getattr; gotd has no such layer, so every supported
// method is an explicit adapter and anything else returns a clear 501.
package mtproto

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/gotd/td/tg"
)

// SentMessage is the normalized send result (Pyrogram Message subset
// the consumers read).
type SentMessage struct {
	ID     int64 `json:"id"`
	ChatID int64 `json:"chat_id"`
}

// EditResult reports an edit outcome.
type EditResult struct {
	Ok bool `json:"ok"`
}

// Client is the Telegram-facing surface one session owns. The fake in
// tests and the gotd implementation in production both satisfy it.
type Client interface {
	Connect(ctx context.Context) error
	Disconnect(ctx context.Context) error
	IsConnected() bool
	Ping(ctx context.Context) error // active liveness (half-open detection)
	Me(ctx context.Context) (BotInfo, error)
	WarmPeers(ctx context.Context) error

	SendMessage(ctx context.Context, p SendParams) (SentMessage, error)
	EditMessageText(ctx context.Context, p EditTextParams) (EditResult, error)
	EditMessageReplyMarkup(ctx context.Context, p EditMarkupParams) (EditResult, error)
	SendChatAction(ctx context.Context, chatID int64, action string) (EditResult, error)
	DeleteMessages(ctx context.Context, chatID int64, ids []int64, revoke bool) (EditResult, error)
	ForwardMessages(ctx context.Context, p ForwardParams) ([]SentMessage, error)
	GetChat(ctx context.Context, chatID int64) (map[string]any, error)
	ResolveUsername(ctx context.Context, username string) (int64, error)
	AnswerCallbackQuery(ctx context.Context, id, text string, showAlert bool, url string, cacheTime int) (EditResult, error)
	DownloadMedia(ctx context.Context, fileID string) ([]byte, error)
	UploadMedia(ctx context.Context, kind, filename string, data []byte) (string, error)
}

// BotInfo identifies the connected account.
type BotInfo struct {
	Username string
	ID       int64
}

// SendParams is the coerced send_message surface.
type SendParams struct {
	ChatID        int64
	Text          string
	ParseMode     string // "" | html | markdown
	ReplyMarkup   map[string]any
	ReplyToMsgID  int64
	DisableNotify bool
}

// EditTextParams is edit_message_text.
type EditTextParams struct {
	ChatID      int64
	MessageID   int64
	Text        string
	ParseMode   string
	ReplyMarkup map[string]any
	NoWebpage   bool
}

// EditMarkupParams is edit_message_reply_markup.
type EditMarkupParams struct {
	ChatID      int64
	MessageID   int64
	ReplyMarkup map[string]any
}

// ForwardParams is forward_messages.
type ForwardParams struct {
	FromChatID int64
	ToChatID   int64
	IDs        []int64
	NoQuote    bool // drop the "forwarded from" header (copy semantics)
}

// UpdateHandler receives every serialized update (Pyrogram dict shape).
type UpdateHandler func(update map[string]any)

// adapter dispatches one method by name. Adapters coerce JSON args into
// typed calls on the Client and return a JSON-serializable result.
type adapter func(ctx context.Context, c Client, args map[string]any) (any, error)

// adapters is the explicit method allowlist (the plan's inventory from
// production metrics/logs). Unknown methods return ErrUnsupported.
var adapters = map[string]adapter{
	"send_message":              adaptSendMessage,
	"edit_message_text":         adaptEditText,
	"edit_message_reply_markup": adaptEditMarkup,
	"send_chat_action":          adaptChatAction,
	"delete_messages":           adaptDeleteMessages,
	"forward_messages":          adaptForwardMessages,
	"get_chat":                  adaptGetChat,
	"resolve_username":          adaptResolveUsername,
	"answer_callback_query":     adaptAnswerCallback,
	"send_photo":                adaptUploadSend("photo"),
	"send_video":                adaptUploadSend("video"),
	"send_document":             adaptUploadSend("document"),
	"send_audio":                adaptUploadSend("audio"),
	"get_me":                    adaptGetMe,
}

// ErrUnsupported marks methods outside the allowlist (HTTP 501).
var ErrUnsupported = fmt.Errorf("unsupported method (not in the adapter allowlist)")

// SupportedMethods lists the allowlist (docs/diagnostics).
func SupportedMethods() []string {
	out := make([]string, 0, len(adapters))
	for k := range adapters {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Dispatch runs one method adapter.
func Dispatch(ctx context.Context, c Client, method string, args map[string]any) (any, error) {
	a, ok := adapters[strings.ToLower(method)]
	if !ok {
		return nil, ErrUnsupported
	}
	return a(ctx, c, args)
}

func argInt(args map[string]any, keys ...string) (int64, bool) {
	for _, k := range keys {
		v, ok := args[k]
		if !ok || v == nil {
			continue
		}
		switch n := v.(type) {
		case float64:
			return int64(n), true
		case int64:
			return n, true
		case int:
			return int64(n), true
		case string:
			var i int64
			if _, err := fmt.Sscanf(n, "%d", &i); err == nil {
				return i, true
			}
		}
	}
	return 0, false
}

func argStr(args map[string]any, key string) string {
	if v, ok := args[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func argBool(args map[string]any, key string) bool {
	if v, ok := args[key]; ok {
		if b, ok := v.(bool); ok {
			return b
		}
	}
	return false
}

func argMarkup(args map[string]any) map[string]any {
	if v, ok := args["reply_markup"]; ok {
		if m, ok := v.(map[string]any); ok {
			return m
		}
	}
	return nil
}

func adaptSendMessage(ctx context.Context, c Client, args map[string]any) (any, error) {
	chatID, ok := argInt(args, "chat_id", "chat_username", "chat")
	if !ok {
		return nil, fmt.Errorf("chat_id required")
	}
	p := SendParams{
		ChatID:        chatID,
		Text:          argStr(args, "text"),
		ParseMode:     argStr(args, "parse_mode"),
		ReplyMarkup:   argMarkup(args),
		DisableNotify: argBool(args, "disable_notification"),
	}
	if rid, ok := argInt(args, "reply_to_message_id"); ok {
		p.ReplyToMsgID = rid
	}
	return c.SendMessage(ctx, p)
}

func adaptEditText(ctx context.Context, c Client, args map[string]any) (any, error) {
	chatID, _ := argInt(args, "chat_id", "chat_username", "chat")
	msgID, ok := argInt(args, "message_id")
	if !ok {
		return nil, fmt.Errorf("message_id required")
	}
	return c.EditMessageText(ctx, EditTextParams{
		ChatID: chatID, MessageID: msgID,
		Text: argStr(args, "text"), ParseMode: argStr(args, "parse_mode"),
		ReplyMarkup: argMarkup(args), NoWebpage: argBool(args, "disable_web_page_preview"),
	})
}

func adaptEditMarkup(ctx context.Context, c Client, args map[string]any) (any, error) {
	chatID, _ := argInt(args, "chat_id", "chat_username", "chat")
	msgID, ok := argInt(args, "message_id")
	if !ok {
		return nil, fmt.Errorf("message_id required")
	}
	return c.EditMessageReplyMarkup(ctx, EditMarkupParams{
		ChatID: chatID, MessageID: msgID, ReplyMarkup: argMarkup(args),
	})
}

func adaptChatAction(ctx context.Context, c Client, args map[string]any) (any, error) {
	chatID, ok := argInt(args, "chat_id", "chat_username", "chat")
	if !ok {
		return nil, fmt.Errorf("chat_id required")
	}
	return c.SendChatAction(ctx, chatID, argStr(args, "action"))
}

func adaptDeleteMessages(ctx context.Context, c Client, args map[string]any) (any, error) {
	chatID, _ := argInt(args, "chat_id")
	ids, ok := argIntSlice(args["message_ids"])
	if !ok {
		if ids, ok = argIntSlice(args["ids"]); !ok {
			return nil, fmt.Errorf("message_ids required")
		}
	}
	return c.DeleteMessages(ctx, chatID, ids, argBool(args, "revoke"))
}

func argIntSlice(v any) ([]int64, bool) {
	list, ok := v.([]any)
	if !ok {
		return nil, false
	}
	out := make([]int64, 0, len(list))
	for _, item := range list {
		switch n := item.(type) {
		case float64:
			out = append(out, int64(n))
		case int64:
			out = append(out, n)
		case int:
			out = append(out, int64(n))
		default:
			return nil, false
		}
	}
	return out, true
}

func adaptForwardMessages(ctx context.Context, c Client, args map[string]any) (any, error) {
	from, ok := argInt(args, "from_chat_id")
	if !ok {
		return nil, fmt.Errorf("from_chat_id required")
	}
	to, ok := argInt(args, "chat_id", "chat_username", "chat")
	if !ok {
		return nil, fmt.Errorf("chat_id required")
	}
	ids, ok := argIntSlice(args["message_ids"])
	if !ok {
		return nil, fmt.Errorf("message_ids required")
	}
	return c.ForwardMessages(ctx, ForwardParams{
		FromChatID: from, ToChatID: to, IDs: ids, NoQuote: argBool(args, "no_quote") || argBool(args, "copy"),
	})
}

func adaptGetChat(ctx context.Context, c Client, args map[string]any) (any, error) {
	chatID, ok := argInt(args, "chat_id", "chat_username", "chat")
	if !ok {
		return nil, fmt.Errorf("chat_id required")
	}
	return c.GetChat(ctx, chatID)
}

func adaptResolveUsername(ctx context.Context, c Client, args map[string]any) (any, error) {
	username := strings.TrimPrefix(argStr(args, "username"), "@")
	if username == "" {
		return nil, fmt.Errorf("username required")
	}
	id, err := c.ResolveUsername(ctx, username)
	if err != nil {
		return nil, err
	}
	return map[string]any{"peer_id": id, "username": username}, nil
}

func adaptAnswerCallback(ctx context.Context, c Client, args map[string]any) (any, error) {
	id := argStr(args, "callback_query_id")
	if id == "" {
		id = argStr(args, "id")
	}
	if id == "" {
		return nil, fmt.Errorf("callback_query_id required")
	}
	cacheTime := 0
	if v, ok := argInt(args, "cache_time"); ok {
		cacheTime = int(v)
	}
	return c.AnswerCallbackQuery(ctx, id, argStr(args, "text"), argBool(args, "show_alert"),
		argStr(args, "url"), cacheTime)
}

// adaptUploadSend sends local media by URL or raw bytes: consumers pass
// either file bytes (multipart upload already unwrapped to data) or an
// HTTP URL Telegram fetches itself.
func adaptUploadSend(kind string) adapter {
	return func(ctx context.Context, c Client, args map[string]any) (any, error) {
		chatID, ok := argInt(args, "chat_id", "chat_username", "chat")
		if !ok {
			return nil, fmt.Errorf("chat_id required")
		}
		caption := argStr(args, "caption")
		// media arrives as a file-id reference, an upload handle, or is
		// delegated to the dedicated upload endpoint
		fileRef := argStr(args, "file_id")
		if fileRef == "" {
			return nil, fmt.Errorf("file_id required (or POST /upload first)")
		}
		data, err := c.DownloadMedia(ctx, fileRef)
		if err != nil {
			return nil, err
		}
		newRef, err := c.UploadMedia(ctx, kind, "upload", data)
		if err != nil {
			return nil, err
		}
		p := SendParams{
			ChatID:        chatID,
			Text:          caption,
			ParseMode:     argStr(args, "parse_mode"),
			ReplyMarkup:   argMarkup(args),
			DisableNotify: argBool(args, "disable_notification"),
		}
		if rid, ok := argInt(args, "reply_to_message_id"); ok {
			p.ReplyToMsgID = rid
		}
		_ = newRef
		return c.SendMessage(ctx, p)
	}
}

func adaptGetMe(ctx context.Context, c Client, _ map[string]any) (any, error) {
	me, err := c.Me(ctx)
	if err != nil {
		return nil, err
	}
	return map[string]any{"id": me.ID, "username": me.Username, "is_bot": true}, nil
}

// peer classes for serialization helpers
var _ = tg.PeerUser{}
