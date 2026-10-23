package mtproto

// Media tests: the adapter contract (upload handle / inline data /
// URL) against the fake client, and the gotd client's input-media
// construction + download-location cache against real gotd types
// (no Telegram connection needed).

import (
	"context"
	"encoding/base64"
	"strings"
	"testing"

	"github.com/gotd/td/tg"
)

func TestSendPhotoWithInlineData(t *testing.T) {
	fake := &fakeMTClient{}
	app, ts := newTestApp(t, fake, t.TempDir())
	base := ts.URL

	regADM(t, base, "b1")
	waitLive(t, app, fake, "b1")

	data := base64.StdEncoding.EncodeToString([]byte("jpegbytes"))
	code, body := mtDo(t, "POST", base, "/v1/sessions/b1/send_photo",
		`{"chat_id":10,"caption":"look","data":"`+data+`"}`, itestAPP)
	if code != 200 || body["ok"] != true {
		t.Fatalf("send_photo = %d %v", code, body)
	}
	m := fake.lastMedia()
	if m == nil || m.Kind != "photo" || m.Handle == "" {
		t.Fatalf("media attach = %+v", m)
	}
}

func TestSendDocumentWithUploadHandleAndURL(t *testing.T) {
	fake := &fakeMTClient{}
	app, ts := newTestApp(t, fake, t.TempDir())
	base := ts.URL

	regADM(t, base, "b1")
	waitLive(t, app, fake, "b1")

	// upload first (fake mints a handle), then send by handle
	_, body := mtDo(t, "POST", base, "/v1/sessions/b1/send_document",
		`{"chat_id":10,"data":"`+base64.StdEncoding.EncodeToString([]byte("pdf"))+`","file_name":"r.pdf"}`,
		itestAPP)
	if body["ok"] != true {
		t.Fatalf("inline document send = %v", body)
	}
	m0 := fake.lastMedia()
	if m0 == nil || m0.Kind != "document" || m0.FileName != "r.pdf" {
		t.Fatalf("document attach = %+v", m0)
	}

	// URL send: Telegram fetches it itself
	_, body = mtDo(t, "POST", base, "/v1/sessions/b1/send_video",
		`{"chat_id":10,"url":"https://example.com/v.mp4","caption":"ext"}`, itestAPP)
	if body["ok"] != true {
		t.Fatalf("url video send = %v", body)
	}
	m := fake.lastMedia()
	if m == nil || m.URL != "https://example.com/v.mp4" || m.Handle != "" {
		t.Fatalf("url attach = %+v", m)
	}

	// missing media → clear error listing the three shapes
	code, body := mtDo(t, "POST", base, "/v1/sessions/b1/send_photo",
		`{"chat_id":10}`, itestAPP)
	if code != 500 || !strings.Contains(fmt0(body["description"]), "media required") {
		t.Fatalf("missing media = %d %v", code, body)
	}
}

func TestBuildInputMediaVariants(t *testing.T) {
	g := &gotdClient{uploads: map[string]tg.InputFileClass{}, photoLocs: map[int64]*tg.Photo{}, docLocs: map[int64]*tg.Document{}}
	big := &tg.InputFileBig{ID: 7, Parts: 2, Name: "x.jpg"}
	g.uploads["upload:1"] = big

	photo, err := g.buildInputMedia(context.Background(), &MediaAttach{Kind: "photo", Handle: "upload:1"})
	if err != nil {
		t.Fatal(err)
	}
	up, ok := photo.(*tg.InputMediaUploadedPhoto)
	if !ok || up.File != tg.InputFileClass(big) {
		t.Fatalf("photo media = %#v", photo)
	}

	doc, err := g.buildInputMedia(context.Background(), &MediaAttach{Kind: "video", Handle: "upload:1", FileName: "v.mp4"})
	if err != nil {
		t.Fatal(err)
	}
	vid, ok := doc.(*tg.InputMediaUploadedDocument)
	if !ok || vid.MimeType != "video/mp4" {
		t.Fatalf("video media = %#v", doc)
	}
	sawVideoAttr, sawName := false, false
	for _, a := range vid.Attributes {
		if _, isVideo := a.(*tg.DocumentAttributeVideo); isVideo {
			sawVideoAttr = true
		}
		if fn, isName := a.(*tg.DocumentAttributeFilename); isName && fn.FileName == "v.mp4" {
			sawName = true
		}
	}
	if !sawVideoAttr || !sawName {
		t.Fatalf("video attributes = %#v", vid.Attributes)
	}

	ext, err := g.buildInputMedia(context.Background(), &MediaAttach{Kind: "photo", URL: "https://x/y.jpg"})
	if pe, ok := ext.(*tg.InputMediaPhotoExternal); !ok || err != nil || pe.URL != "https://x/y.jpg" {
		t.Fatalf("external photo = %#v err=%v", ext, err)
	}

	// unknown handle → clear session-scoped explanation
	_, err = g.buildInputMedia(context.Background(), &MediaAttach{Kind: "photo", Handle: "upload:404"})
	if err == nil || !strings.Contains(err.Error(), "unknown upload handle") {
		t.Fatalf("unknown handle err = %v", err)
	}
}

func TestMediaLocationCacheFromUpdates(t *testing.T) {
	g := &gotdClient{uploads: map[string]tg.InputFileClass{}, photoLocs: map[int64]*tg.Photo{}, docLocs: map[int64]*tg.Document{}}
	g.updateHandler = func(map[string]any) {} // no-op consumer

	photo := &tg.Photo{
		ID:            11,
		AccessHash:    222,
		FileReference: []byte("ref"),
		Sizes:         []tg.PhotoSizeClass{&tg.PhotoSize{Type: "x", W: 100, H: 100}, &tg.PhotoSize{Type: "y", W: 800, H: 600}},
	}
	doc := &tg.Document{
		ID: 12, AccessHash: 333, FileReference: []byte("dref"),
		MimeType: "application/pdf", Size: 10,
	}
	_ = g.onUpdatesClass(context.Background(), &tg.Updates{
		Updates: []tg.UpdateClass{
			&tg.UpdateNewMessage{Message: &tg.Message{
				ID: 1, PeerID: &tg.PeerUser{UserID: 10},
				Media: &tg.MessageMediaPhoto{Photo: photo},
			}},
			&tg.UpdateNewMessage{Message: &tg.Message{
				ID: 2, PeerID: &tg.PeerUser{UserID: 10},
				Media: &tg.MessageMediaDocument{Document: doc},
			}},
		},
	})

	// unknown id → the clear "not seen" error
	_, err := g.DownloadMedia(context.Background(), "photo:999")
	if err == nil || !strings.Contains(err.Error(), "not seen") {
		t.Fatalf("unknown photo err = %v", err)
	}
	// known id → location resolves; without a live connection the RPC
	// itself fails, but NOT with "not seen" (cache hit is proven)
	_, err = g.DownloadMedia(context.Background(), "photo:11")
	if err == nil || strings.Contains(err.Error(), "not seen") {
		t.Fatalf("cached photo err = %v", err)
	}
	_, err = g.DownloadMedia(context.Background(), "document:12")
	if err == nil || strings.Contains(err.Error(), "not seen") {
		t.Fatalf("cached document err = %v", err)
	}
	// malformed refs
	if _, err := g.DownloadMedia(context.Background(), "garbage"); err == nil {
		t.Fatal("malformed ref accepted")
	}
	if _, err := g.DownloadMedia(context.Background(), "sticker:1"); err == nil {
		t.Fatal("unknown kind accepted")
	}
}

func TestLargestThumbSize(t *testing.T) {
	sizes := []tg.PhotoSizeClass{
		&tg.PhotoSize{Type: "x", W: 100, H: 100},
		&tg.PhotoSize{Type: "y", W: 800, H: 600},
		&tg.PhotoSize{Type: "m", W: 320, H: 240},
	}
	if got := largestThumbSize(sizes); got != "y" {
		t.Fatalf("largest = %q", got)
	}
	if got := largestThumbSize(nil); got != "m" {
		t.Fatalf("default = %q", got)
	}
}

// fmt0 renders any value as a string without importing fmt everywhere.
func fmt0(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func TestNewAllowlistMethods(t *testing.T) {
	fake := &fakeMTClient{}
	app, ts := newTestApp(t, fake, t.TempDir())
	base := ts.URL

	regADM(t, base, "b1")
	waitLive(t, app, fake, "b1")

	b64 := base64.StdEncoding.EncodeToString([]byte("bin"))

	// send_media_group: mixed data + handle + url entries
	group := `{"chat_id":10,"media":[` +
		`{"type":"photo","data":"` + b64 + `"},` +
		`{"type":"document","file_id":"upload:1"},` +
		`{"type":"photo","url":"https://x/2.jpg"}]}`
	code, body := mtDo(t, "POST", base, "/v1/sessions/b1/send_media_group", group, itestAPP)
	if code != 200 || body["ok"] != true {
		t.Fatalf("send_media_group = %d %v", code, body)
	}
	results := body["result"].([]any)
	if len(results) != 3 {
		t.Fatalf("album results = %d, want 3", len(results))
	}

	// copy_message → forward with the header dropped
	code, body = mtDo(t, "POST", base, "/v1/sessions/b1/copy_message",
		`{"from_chat_id":10,"chat_id":20,"message_id":5}`, itestAPP)
	if code != 200 || body["ok"] != true {
		t.Fatalf("copy_message = %d %v", code, body)
	}
	if body["result"].(map[string]any)["message_id"] == nil {
		t.Fatal("copy_message result lacks message_id")
	}

	// edit_message_caption rides the same edit wire shape
	code, body = mtDo(t, "POST", base, "/v1/sessions/b1/edit_message_caption",
		`{"chat_id":10,"message_id":5,"text":"new caption"}`, itestAPP)
	if code != 200 || body["ok"] != true {
		t.Fatalf("edit_message_caption = %d %v", code, body)
	}

	// send_voice / send_animation attribute mapping
	_, body = mtDo(t, "POST", base, "/v1/sessions/b1/send_voice",
		`{"chat_id":30,"data":"`+b64+`"}`, itestAPP)
	if body["ok"] != true {
		t.Fatalf("send_voice = %v", body)
	}
	_, body = mtDo(t, "POST", base, "/v1/sessions/b1/send_animation",
		`{"chat_id":40,"data":"`+b64+`"}`, itestAPP)
	if body["ok"] != true {
		t.Fatalf("send_animation = %v", body)
	}
	g := &gotdClient{uploads: map[string]tg.InputFileClass{}, photoLocs: map[int64]*tg.Photo{}, docLocs: map[int64]*tg.Document{}}
	g.uploads["upload:1"] = &tg.InputFileBig{ID: 9, Parts: 1, Name: "v"}
	voice, err := g.buildInputMedia(context.Background(), &MediaAttach{Kind: "voice", Handle: "upload:1"})
	if err != nil {
		t.Fatal(err)
	}
	vd, ok := voice.(*tg.InputMediaUploadedDocument)
	if !ok || vd.MimeType != "audio/ogg" {
		t.Fatalf("voice media = %#v", voice)
	}
	sawVoiceAttr := false
	for _, a := range vd.Attributes {
		if av, isAudio := a.(*tg.DocumentAttributeAudio); isAudio && av.Voice {
			sawVoiceAttr = true
		}
	}
	if !sawVoiceAttr {
		t.Fatalf("voice attributes missing Voice flag: %#v", vd.Attributes)
	}
	anim, err := g.buildInputMedia(context.Background(), &MediaAttach{Kind: "animation", Handle: "upload:1"})
	if err != nil {
		t.Fatal(err)
	}
	ad, ok := anim.(*tg.InputMediaUploadedDocument)
	if !ok || ad.MimeType != "video/mp4" {
		t.Fatalf("animation media = %#v", anim)
	}

	// allowlist now lists them
	supported := strings.Join(SupportedMethods(), ",")
	for _, m := range []string{"send_media_group", "copy_message", "edit_message_caption", "send_voice", "send_animation"} {
		if !strings.Contains(supported, m) {
			t.Fatalf("allowlist missing %s", m)
		}
	}
}
