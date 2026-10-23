package mtproto

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/gotd/td/tg"
)

// Pyrogram-shaped serialization: the JSON consumers receive must keep
// the dict shapes pyrofork's to_mapOf() produced — "_" carries the
// constructor name and fields are snake_case. These are the shapes the
// existing consumers (KOT) parse; golden files recorded from the
// Python sidecar pin them at cutover.

// SerializeUpdate converts a raw gotd update into the Pyrogram dict.
// Returns nil for updates with no consumer-facing meaning (dropped,
// exactly like the Python handler dropped non-dict payloads).
func SerializeUpdate(u tg.UpdateClass) map[string]any {
	switch up := u.(type) {
	case *tg.UpdateNewMessage:
		msg := SerializeMessage(up.Message)
		if msg == nil {
			return nil
		}
		return mapOf("_", "UpdateNewMessage", "message", msg, "pts", up.Pts, "pts_count", up.PtsCount)
	case *tg.UpdateNewChannelMessage:
		msg := SerializeMessage(up.Message)
		if msg == nil {
			return nil
		}
		return mapOf("_", "UpdateNewChannelMessage", "message", msg, "pts", up.Pts, "pts_count", up.PtsCount)
	case *tg.UpdateEditMessage:
		msg := SerializeMessage(up.Message)
		if msg == nil {
			return nil
		}
		return mapOf("_", "UpdateEditMessage", "message", msg, "pts", up.Pts, "pts_count", up.PtsCount)
	case *tg.UpdateEditChannelMessage:
		msg := SerializeMessage(up.Message)
		if msg == nil {
			return nil
		}
		return mapOf("_", "UpdateEditChannelMessage", "message", msg, "pts", up.Pts, "pts_count", up.PtsCount)
	case *tg.UpdateBotCallbackQuery:
		return mapOf("_", "UpdateBotCallbackQuery",
			"query_id", up.QueryID,
			"user_id", up.UserID,
			"peer", SerializePeer(up.Peer),
			"msg_id", up.MsgID,
			"chat_instance", up.ChatInstance,
			"data", string(up.Data),
		)
	case *tg.UpdateInlineBotCallbackQuery:
		return mapOf("_", "UpdateInlineBotCallbackQuery",
			"query_id", up.QueryID,
			"user_id", up.UserID,
			"msg_data", "inline",
			"chat_instance", up.ChatInstance,
			"data", string(up.Data),
		)
	case *tg.UpdateBotCommands:
		commands := []any{}
		for _, cmd := range up.Commands {
			commands = append(commands, mapOf("_", "BotCommand", "command", cmd.Command, "description", cmd.Description))
		}
		return mapOf("_", "UpdateBotCommands", "peer", SerializePeer(up.Peer), "commands", commands)
	}
	// unknown-but-valid updates pass through with their constructor name
	// so consumers can see them instead of them vanishing
	return mapOf("_", constructorName(u))
}

func constructorName(u tg.UpdateClass) string {
	// "*tg.UpdateBotCallbackQuery" → "UpdateBotCallbackQuery"
	s := fmt.Sprintf("%T", u)
	if i := strings.LastIndexByte(s, '.'); i >= 0 {
		s = s[i+1:]
	}
	return s
}

// SerializeMessage converts a gotd message class into the Pyrogram
// Message dict subset consumers depend on (chat.id, from_user id,
// text, id, date, reply_to_message_id, entities).
func SerializeMessage(m tg.MessageClass) map[string]any {
	msg, ok := m.(*tg.Message)
	if !ok {
		return nil
	}
	out := mapOf(
		"_", "Message",
		"id", int64(msg.ID),
		"date", int64(msg.Date),
		"message", msg.Message,
	)
	if msg.PeerID != nil {
		out["chat"] = SerializeChat(msg.PeerID)
	}
	if from, ok := msg.FromID.(interface{ GetUserID() int64 }); ok {
		out["from_user_id"] = from.GetUserID()
	}
	if msg.ReplyTo != nil {
		if hdr, ok := msg.ReplyTo.(*tg.MessageReplyHeader); ok {
			out["reply_to_message_id"] = int64(hdr.ReplyToMsgID)
		}
	}
	if msg.ReplyMarkup != nil {
		out["reply_markup"] = SerializeReplyMarkup(msg.ReplyMarkup)
	}
	if len(msg.Entities) > 0 {
		entities := []any{}
		for _, e := range msg.Entities {
			if d := SerializeEntity(e); d != nil {
				entities = append(entities, d)
			}
		}
		out["entities"] = entities
	}
	if msg.Media != nil {
		if d := SerializeMedia(msg.Media); d != nil {
			out["media"] = d
		}
	}
	return out
}

// SerializePeer renders a peer as the {"_": "PeerUser", "user_id": N}
// shape.
func SerializePeer(p tg.PeerClass) map[string]any {
	switch peer := p.(type) {
	case *tg.PeerUser:
		return mapOf("_", "PeerUser", "user_id", peer.UserID)
	case *tg.PeerChat:
		return mapOf("_", "PeerChat", "chat_id", peer.ChatID)
	case *tg.PeerChannel:
		return mapOf("_", "PeerChannel", "channel_id", peer.ChannelID)
	}
	return mapOf("_", "PeerEmpty")
}

// SerializeChat renders the message's peer as the "chat" dict consumers
// read chat.id from (Pyrogram resolved full entities; the sidecar's
// consumers only rely on the id, which the peer carries).
func SerializeChat(p tg.PeerClass) map[string]any {
	switch peer := p.(type) {
	case *tg.PeerUser:
		return mapOf("_", "Chat", "id", peer.UserID, "type", "private")
	case *tg.PeerChat:
		return mapOf("_", "Chat", "id", -peer.ChatID, "type", "chat")
	case *tg.PeerChannel:
		return mapOf("_", "Chat", "id", peer.ChannelID, "type", "channel")
	}
	return mapOf("_", "Chat", "id", int64(0), "type", "empty")
}

// SerializeEntity renders message entities (the blockquote expandable
// flag rides as "collapsed").
func SerializeEntity(e tg.MessageEntityClass) map[string]any {
	base := func(name string, kv ...any) map[string]any {
		d := mapOf("_", name)
		for i := 0; i+1 < len(kv); i += 2 {
			d[kv[i].(string)] = kv[i+1]
		}
		return d
	}
	switch ent := e.(type) {
	case *tg.MessageEntityBold:
		return base("MessageEntityBold", "offset", int64(ent.Offset), "length", int64(ent.Length))
	case *tg.MessageEntityItalic:
		return base("MessageEntityItalic", "offset", int64(ent.Offset), "length", int64(ent.Length))
	case *tg.MessageEntityCode:
		return base("MessageEntityCode", "offset", int64(ent.Offset), "length", int64(ent.Length))
	case *tg.MessageEntityPre:
		return base("MessageEntityPre", "offset", int64(ent.Offset), "length", int64(ent.Length), "language", ent.Language)
	case *tg.MessageEntityTextURL:
		return base("MessageEntityTextUrl", "offset", int64(ent.Offset), "length", int64(ent.Length), "url", ent.URL)
	case *tg.MessageEntityURL:
		return base("MessageEntityUrl", "offset", int64(ent.Offset), "length", int64(ent.Length))
	case *tg.MessageEntityMention:
		return base("MessageEntityMention", "offset", int64(ent.Offset), "length", int64(ent.Length))
	case *tg.MessageEntityMentionName:
		return base("MessageEntityMentionName", "offset", int64(ent.Offset), "length", int64(ent.Length), "user_id", ent.UserID)
	case *tg.MessageEntityHashtag:
		return base("MessageEntityHashtag", "offset", int64(ent.Offset), "length", int64(ent.Length))
	case *tg.MessageEntityBotCommand:
		return base("MessageEntityBotCommand", "offset", int64(ent.Offset), "length", int64(ent.Length))
	case *tg.MessageEntityEmail:
		return base("MessageEntityEmail", "offset", int64(ent.Offset), "length", int64(ent.Length))
	case *tg.MessageEntityPhone:
		return base("MessageEntityPhone", "offset", int64(ent.Offset), "length", int64(ent.Length))
	case *tg.MessageEntityCashtag:
		return base("MessageEntityCashtag", "offset", int64(ent.Offset), "length", int64(ent.Length))
	case *tg.MessageEntityUnderline:
		return base("MessageEntityUnderline", "offset", int64(ent.Offset), "length", int64(ent.Length))
	case *tg.MessageEntityStrike:
		return base("MessageEntityStrike", "offset", int64(ent.Offset), "length", int64(ent.Length))
	case *tg.MessageEntityBlockquote:
		return base("MessageEntityBlockquote", "offset", int64(ent.Offset), "length", int64(ent.Length), "collapsed", ent.Collapsed)
	case *tg.MessageEntitySpoiler:
		return base("MessageEntitySpoiler", "offset", int64(ent.Offset), "length", int64(ent.Length))
	case *tg.MessageEntityCustomEmoji:
		return base("MessageEntityCustomEmoji", "offset", int64(ent.Offset), "length", int64(ent.Length), "document_id", ent.DocumentID)
	}
	return nil
}

// SerializeMedia renders media payloads (photo/documents carry the
// file_id as a hex string placeholder consumers use via /files).
func SerializeMedia(m tg.MessageMediaClass) map[string]any {
	switch media := m.(type) {
	case *tg.MessageMediaPhoto:
		photo, ok := media.Photo.(*tg.Photo)
		if !ok {
			return nil
		}
		sizes := []any{}
		for _, s := range photo.Sizes {
			if psz, ok := s.(*tg.PhotoSize); ok {
				sizes = append(sizes, mapOf("_", "PhotoSize", "w", int64(psz.W), "h", int64(psz.H), "size", int64(psz.Size)))
			}
		}
		return mapOf("_", "MessageMediaPhoto", "photo", mapOf(
			"_", "Photo", "id", int64(photo.ID), "sizes", sizes,
			"file_id", formatFileRef("photo", photo.ID)))
	case *tg.MessageMediaDocument:
		doc, ok := media.Document.(*tg.Document)
		if !ok {
			return nil
		}
		return mapOf("_", "MessageMediaDocument", "document", mapOf(
			"_", "Document", "id", int64(doc.ID), "mime_type", doc.MimeType,
			"size", int64(doc.Size), "file_id", formatFileRef("document", doc.ID)))
	case *tg.MessageMediaWebPage:
		return mapOf("_", "MessageMediaWebPage")
	}
	return nil
}

func formatFileRef(kind string, id int64) string {
	return kind + ":" + strconv.FormatInt(id, 10)
}

// SerializeReplyMarkup renders gotd markup back to the Pyrogram dict
// (answer side of edit_message_reply_markup / send with buttons).
func SerializeReplyMarkup(m tg.ReplyMarkupClass) map[string]any {
	switch kb := m.(type) {
	case *tg.ReplyInlineMarkup:
		rows := []any{}
		for _, row := range kb.Rows {
			r := []any{}
			for _, btn := range row.Buttons {
				r = append(r, serializeInlineButton(btn))
			}
			rows = append(rows, r)
		}
		return mapOf("_", "InlineKeyboardMarkup", "inline_keyboard", rows)
	case *tg.ReplyKeyboardMarkup:
		rows := []any{}
		for _, row := range kb.Rows {
			r := []any{}
			for _, btn := range row.Buttons {
				r = append(r, mapOf("_", "KeyboardButton", "text", btn.Text))
			}
			rows = append(rows, r)
		}
		return mapOf("_", "ReplyKeyboardMarkup", "keyboard", rows,
			"resize_keyboard", kb.Resize, "is_persistent", kb.Persistent)
	case *tg.ReplyKeyboardHide:
		return mapOf("_", "ReplyKeyboardRemove", "remove_keyboard", true)
	case *tg.ReplyKeyboardForceReply:
		return mapOf("_", "ForceReply", "force_reply", true)
	}
	return nil
}

func serializeInlineButton(btn tg.KeyboardInlineButton) map[string]any {
	out := mapOf("_", "InlineKeyboardButton", "text", btn.Text)
	switch t := btn.Type.(type) {
	case *tg.InlineButtonTypeURL:
		out["url"] = t.URL
	case *tg.InlineButtonTypeCallback:
		out["callback_data"] = string(t.Data)
	case *tg.InlineButtonTypeWebView:
		out["web_app"] = mapOf("url", t.URL)
	case *tg.InlineButtonTypeSwitchInline:
		out["switch_inline_query"] = t.Query
		out["same_peer"] = t.SamePeer
	case *tg.InlineButtonTypeBuy:
		out["buy"] = true
	}
	return out
}

func mapOf(kv ...any) map[string]any {
	out := make(map[string]any, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		if k, ok := kv[i].(string); ok {
			out[k] = kv[i+1]
		}
	}
	return out
}
