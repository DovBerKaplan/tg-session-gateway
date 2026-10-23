package mtproto

import (
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/gotd/td/tg"
)

// CoerceReplyMarkup converts a Pyrogram-serialized reply_markup dict
// into a gotd tg.ReplyMarkup. Port of mtgateway/coerce.py, including
// the buy-button union pyrofork added.
func CoerceReplyMarkup(v any) tg.ReplyMarkupClass {
	m, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	if rows, ok := m["inline_keyboard"].([]any); ok {
		return coerceInlineKeyboard(rows)
	}
	if rows, ok := m["keyboard"].([]any); ok {
		kb := &tg.ReplyKeyboardMarkup{
			Resize:     boolOr(m, "resize_keyboard", true),
			Persistent: boolOr(m, "is_persistent", false),
		}
		for _, rowAny := range rows {
			row, ok := rowAny.([]any)
			if !ok {
				continue
			}
			buttons := []tg.KeyboardButton{}
			for _, btnAny := range row {
				if btn, ok := btnAny.(map[string]any); ok {
					buttons = append(buttons, tg.KeyboardButton{Text: strOr(btn, "text")})
				}
			}
			kb.Rows = append(kb.Rows, tg.KeyboardButtonRow{Buttons: buttons})
		}
		return kb
	}
	if boolOr(m, "force_reply", false) {
		return &tg.ReplyKeyboardForceReply{}
	}
	if boolOr(m, "remove_keyboard", false) {
		return &tg.ReplyKeyboardHide{}
	}
	return nil
}

func coerceInlineKeyboard(rows []any) *tg.ReplyInlineMarkup {
	kb := &tg.ReplyInlineMarkup{}
	for _, rowAny := range rows {
		row, ok := rowAny.([]any)
		if !ok {
			continue
		}
		buttons := []tg.KeyboardInlineButton{}
		for _, btnAny := range row {
			btn, ok := btnAny.(map[string]any)
			if !ok {
				continue
			}
			buttons = append(buttons, coerceInlineButton(btn))
		}
		kb.Rows = append(kb.Rows, tg.KeyboardInlineButtonRow{Buttons: buttons})
	}
	return kb
}

// coerceInlineButton maps the Pyrogram button dict onto the unified
// KeyboardInlineButton{Text, Type} model (URL/callback/webview/buy/
// switch-inline are InlineButtonType* constructors).
func coerceInlineButton(b map[string]any) tg.KeyboardInlineButton {
	text := strOr(b, "text")
	btn := tg.KeyboardInlineButton{Text: text}
	if u := strOr(b, "url"); u != "" {
		btn.Type = &tg.InlineButtonTypeURL{URL: u}
		return btn
	}
	if cb := strOr(b, "callback_data"); cb != "" {
		btn.Type = &tg.InlineButtonTypeCallback{Data: []byte(cb)}
		return btn
	}
	if wa, ok := b["web_app"].(map[string]any); ok {
		btn.Type = &tg.InlineButtonTypeWebView{URL: strOr(wa, "url")}
		return btn
	}
	if hasKey(b, "switch_inline_query") {
		btn.Type = &tg.InlineButtonTypeSwitchInline{Query: strOr(b, "switch_inline_query")}
		return btn
	}
	if hasKey(b, "switch_inline_query_current_chat") {
		btn.Type = &tg.InlineButtonTypeSwitchInline{
			Query: strOr(b, "switch_inline_query_current_chat"), SamePeer: true,
		}
		return btn
	}
	if boolOr(b, "buy", false) { // pyrofork's InlineKeyboardButtonBuy union
		btn.Type = &tg.InlineButtonTypeBuy{}
		return btn
	}
	return btn
}

// CoerceInputMedia converts a Pyrogram-serialized InputMedia dict into
// a gotd tg.InputMediaClass (photo / video / document by `type`).
func CoerceInputMedia(v any) tg.InputMediaClass {
	m, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	kind := strOr(m, "type")
	mediaRef := strOr(m, "media")
	var input tg.InputFileClass
	if id := strOr(m, "_gw_upload_id"); id != "" {
		// uploaded through /upload earlier this request
		raw, err := base64.StdEncoding.DecodeString(id)
		if err == nil && len(raw) > 0 {
			input = &tg.InputFileBig{ID: 0} // placeholder — resolved by caller
		}
	}
	_ = input
	switch kind {
	case "photo":
		return &tg.InputMediaPhoto{
			ID:         inputPhotoOrEmpty(mediaRef),
			TTLSeconds: intOr(m, "ttl_seconds", 0),
		}
	case "video":
		im := &tg.InputMediaUploadedDocument{
			MimeType: "video/mp4",
			Attributes: []tg.DocumentAttributeClass{
				&tg.DocumentAttributeVideo{
					SupportsStreaming: true,
					Duration:          float64(intOr(m, "duration", 0)),
					W:                 intOr(m, "width", 0),
					H:                 intOr(m, "height", 0),
				},
			},
		}
		applyUploadedCommon(m, &im.File, &im.Attributes)
		return im
	case "document":
		im := &tg.InputMediaUploadedDocument{MimeType: "application/octet-stream"}
		applyUploadedCommon(m, &im.File, &im.Attributes)
		return im
	}
	return nil
}

func applyUploadedCommon(m map[string]any, file *tg.InputFileClass, attrs *[]tg.DocumentAttributeClass) {
	if id := strOr(m, "_gw_upload_id"); id != "" {
		if f, ok := decodeUploadHandle(id); ok {
			*file = f
		}
	}
}

func decodeUploadHandle(id string) (tg.InputFileClass, bool) {
	parts := strings.SplitN(id, ":", 3)
	if len(parts) != 3 {
		return nil, false
	}
	var fileID, partsN int64
	if _, err := fmt.Sscanf(parts[0]+" "+parts[1], "%d %d", &fileID, &partsN); err != nil {
		return nil, false
	}
	return &tg.InputFileBig{ID: fileID, Parts: int(partsN), Name: parts[2]}, true
}

func inputPhotoOrEmpty(ref string) tg.InputPhotoClass {
	if ref == "" {
		return &tg.InputPhotoEmpty{}
	}
	return &tg.InputPhotoEmpty{} // file-id resolution rides UploadMedia
}

func strOr(m map[string]any, k string) string {
	if v, ok := m[k]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func boolOr(m map[string]any, k string, def bool) bool {
	v, ok := m[k]
	if !ok {
		return def
	}
	if b, ok := v.(bool); ok {
		return b
	}
	return def
}

func intOr(m map[string]any, k string, def int) int {
	switch n := m[k].(type) {
	case float64:
		return int(n)
	case int:
		return n
	case int64:
		return int(n)
	}
	return def
}

func hasKey(m map[string]any, k string) bool {
	_, ok := m[k]
	return ok
}
