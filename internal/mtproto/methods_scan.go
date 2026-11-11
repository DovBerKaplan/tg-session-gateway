package mtproto

// The scanner surface: read-side dialog/history methods plus the
// channel-admin writes, all in the same snake_case/Pyrogram-dict shape
// as the original 19. get_history NEVER marks messages as read (no
// implicit read_receipts); send_read_acknowledge is the only explicit
// read path.

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
)

// DocRef references an existing Telegram document (server-side copy:
// sendMedia with InputMediaDocument — no re-upload, no download).
type DocRef struct {
	ID            int64
	AccessHash    int64
	FileReference []byte
}

// EditChannelParams folds the channel settings toggles into one method;
// nil pointers keep the current value.
type EditChannelParams struct {
	ChatID           int64
	Title            *string
	About            *string
	Signatures       *bool
	PreHistoryHidden *bool
	NoForwards       *bool
}

// EditAdminParams grants admin rights; Rights are tg.ChatAdminRights
// flag names (change_info, post_messages, edit_messages, delete_messages,
// ban_users, invite_users, pin_messages, add_admins, anonymous,
// manage_call, other), rank is the custom title.
type EditAdminParams struct {
	ChatID int64
	UserID int64
	Rights []string
	Rank   string
}

// ParticipantsParams is get_participants (filter: recent|admins|
// kicked|banned|search, optional query for admins/search).
type ParticipantsParams struct {
	ChatID int64
	Filter string
	Query  string
	Offset int
	Limit  int
}

func argIntPtr(args map[string]any, key string) *int64 {
	if v, ok := argInt(args, key); ok {
		return &v
	}
	return nil
}

func argBoolPtr(args map[string]any, key string) *bool {
	if v, ok := args[key]; ok {
		if b, ok := v.(bool); ok {
			return &b
		}
	}
	return nil
}

func argStrPtr(args map[string]any, key string) *string {
	if v, ok := args[key]; ok {
		if s, ok := v.(string); ok {
			return &s
		}
	}
	return nil
}

func adaptGetHistory(ctx context.Context, c Client, args map[string]any) (any, error) {
	chatID, ok := argInt(args, "chat_id", "channel_id")
	if !ok {
		return nil, fmt.Errorf("chat_id required")
	}
	offsetID, _ := argInt(args, "offset_id")
	addOffset, _ := argInt(args, "add_offset")
	minID, _ := argInt(args, "min_id")
	limit, _ := argInt(args, "limit")
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	msgs, err := c.GetHistory(ctx, chatID, int(offsetID), int(addOffset), int(minID), int(limit))
	if err != nil {
		return nil, err
	}
	return map[string]any{"messages": msgs, "count": len(msgs)}, nil
}

func adaptGetMessages(ctx context.Context, c Client, args map[string]any) (any, error) {
	chatID, ok := argInt(args, "chat_id", "channel_id")
	if !ok {
		return nil, fmt.Errorf("chat_id required")
	}
	ids, ok := argIntSlice(args["message_ids"])
	if !ok || len(ids) == 0 {
		return nil, fmt.Errorf("message_ids required")
	}
	msgs, err := c.GetMessages(ctx, chatID, ids)
	if err != nil {
		return nil, err
	}
	return map[string]any{"messages": msgs, "count": len(msgs)}, nil
}

func adaptGetDialogs(ctx context.Context, c Client, args map[string]any) (any, error) {
	limit, _ := argInt(args, "limit")
	if limit <= 0 || limit > 1000 {
		limit = 500
	}
	dialogs, err := c.GetDialogs(ctx, int(limit))
	if err != nil {
		return nil, err
	}
	return map[string]any{"dialogs": dialogs, "count": len(dialogs)}, nil
}

func adaptGetEntity(ctx context.Context, c Client, args map[string]any) (any, error) {
	chatID, ok := argInt(args, "chat_id", "channel_id", "user_id")
	if !ok {
		if u := argStr(args, "username"); u != "" {
			id, err := c.ResolveUsername(ctx, strings.TrimPrefix(u, "@"))
			if err != nil {
				return nil, err
			}
			chatID = id
		} else {
			return nil, fmt.Errorf("chat_id or username required")
		}
	}
	return c.GetEntity(ctx, chatID)
}

func adaptGetFullChannel(ctx context.Context, c Client, args map[string]any) (any, error) {
	chatID, ok := argInt(args, "chat_id", "channel_id")
	if !ok {
		return nil, fmt.Errorf("chat_id required")
	}
	return c.GetFullChannel(ctx, chatID)
}

func adaptGetParticipant(ctx context.Context, c Client, args map[string]any) (any, error) {
	chatID, ok := argInt(args, "chat_id", "channel_id")
	if !ok {
		return nil, fmt.Errorf("chat_id required")
	}
	userID, ok := argInt(args, "user_id")
	if !ok {
		return nil, fmt.Errorf("user_id required")
	}
	return c.GetParticipant(ctx, chatID, userID)
}

func adaptGetParticipants(ctx context.Context, c Client, args map[string]any) (any, error) {
	chatID, ok := argInt(args, "chat_id", "channel_id")
	if !ok {
		return nil, fmt.Errorf("chat_id required")
	}
	p := ParticipantsParams{
		ChatID: chatID,
		Filter: strings.ToLower(argStr(args, "filter")),
		Query:  argStr(args, "query"),
	}
	if p.Filter == "" {
		p.Filter = "recent"
	}
	switch p.Filter {
	case "recent", "admins", "kicked", "banned", "search":
	default:
		return nil, fmt.Errorf("unknown filter %q (recent|admins|kicked|banned|search)", p.Filter)
	}
	p.Offset = int(clampOpt(argIntPtr(args, "offset"), 0, 10000))
	p.Limit = int(clampOpt(argIntPtr(args, "limit"), 1, 200))
	if p.Limit == 0 {
		p.Limit = 200
	}
	out, err := c.GetParticipants(ctx, p)
	if err != nil {
		return nil, err
	}
	return map[string]any{"participants": out, "count": len(out)}, nil
}

func clampOpt(v *int64, min, max int64) int64 {
	if v == nil || *v < min {
		return min
	}
	if *v > max {
		return max
	}
	return *v
}

// adaptSendMedia sends an existing document by reference (server-side
// copy: no download, no re-upload) — the scanner forwarder's path.
// file_reference is base64; get it from the message's media dict.
func adaptSendMedia(ctx context.Context, c Client, args map[string]any) (any, error) {
	chatID, ok := argInt(args, "chat_id")
	if !ok {
		return nil, fmt.Errorf("chat_id required")
	}
	docID, hasDoc := argInt(args, "doc_id")
	if !hasDoc {
		return nil, fmt.Errorf("doc_id required (send_photo/video/… cover uploads)")
	}
	ref := DocRef{ID: docID}
	if h, ok := argInt(args, "access_hash"); ok {
		ref.AccessHash = h
	}
	if b64 := argStr(args, "file_reference"); b64 != "" {
		raw, err := base64.StdEncoding.DecodeString(b64)
		if err != nil {
			return nil, fmt.Errorf("file_reference must be base64: %w", err)
		}
		ref.FileReference = raw
	}
	return c.SendDocument(ctx, chatID, ref, argStr(args, "caption"), argStr(args, "parse_mode"))
}

func adaptCreateChannel(ctx context.Context, c Client, args map[string]any) (any, error) {
	title := argStr(args, "title")
	if title == "" {
		return nil, fmt.Errorf("title required")
	}
	return c.CreateChannel(ctx, title, argStr(args, "about"), argBool(args, "megagroup"))
}

func adaptEditChannel(ctx context.Context, c Client, args map[string]any) (any, error) {
	chatID, ok := argInt(args, "chat_id", "channel_id")
	if !ok {
		return nil, fmt.Errorf("chat_id required")
	}
	p := EditChannelParams{
		ChatID:           chatID,
		Title:            argStrPtr(args, "title"),
		About:            argStrPtr(args, "about"),
		Signatures:       argBoolPtr(args, "signatures"),
		PreHistoryHidden: argBoolPtr(args, "pre_history_hidden"),
		NoForwards:       argBoolPtr(args, "no_forwards"),
	}
	if p.Title == nil && p.About == nil && p.Signatures == nil && p.PreHistoryHidden == nil && p.NoForwards == nil {
		return nil, fmt.Errorf("nothing to edit: pass title/about/signatures/pre_history_hidden/no_forwards")
	}
	return c.EditChannel(ctx, p)
}

func adaptDeleteChannel(ctx context.Context, c Client, args map[string]any) (any, error) {
	chatID, ok := argInt(args, "chat_id", "channel_id")
	if !ok {
		return nil, fmt.Errorf("chat_id required")
	}
	return c.DeleteChannel(ctx, chatID)
}

func adaptEditAdmin(ctx context.Context, c Client, args map[string]any) (any, error) {
	chatID, ok := argInt(args, "chat_id", "channel_id")
	if !ok {
		return nil, fmt.Errorf("chat_id required")
	}
	userID, ok := argInt(args, "user_id")
	if !ok {
		return nil, fmt.Errorf("user_id required")
	}
	rights, _ := argStrSlice(args["rights"])
	return c.EditAdmin(ctx, EditAdminParams{
		ChatID: chatID, UserID: userID, Rights: rights, Rank: argStr(args, "rank"),
	})
}

func argStrSlice(v any) ([]string, bool) {
	list, ok := v.([]any)
	if !ok {
		return nil, false
	}
	out := make([]string, 0, len(list))
	for _, item := range list {
		if s, ok := item.(string); ok {
			out = append(out, s)
		}
	}
	return out, true
}

func adaptJoinChannel(ctx context.Context, c Client, args map[string]any) (any, error) {
	chatID, ok := argInt(args, "chat_id", "channel_id")
	if !ok {
		return nil, fmt.Errorf("chat_id required")
	}
	return c.JoinChannel(ctx, chatID)
}

func adaptLeaveChannel(ctx context.Context, c Client, args map[string]any) (any, error) {
	chatID, ok := argInt(args, "chat_id", "channel_id")
	if !ok {
		return nil, fmt.Errorf("chat_id required")
	}
	return c.LeaveChannel(ctx, chatID)
}

// adaptSendReadAcknowledge is the ONLY read-receipt path — explicit by
// design, nothing else marks messages as read.
func adaptSendReadAcknowledge(ctx context.Context, c Client, args map[string]any) (any, error) {
	chatID, ok := argInt(args, "chat_id")
	if !ok {
		return nil, fmt.Errorf("chat_id required")
	}
	maxID, _ := argInt(args, "max_id")
	return c.ReadHistory(ctx, chatID, maxID)
}
