package mtproto

// Direct coerce.go tests (pure functions; the HTTP media paths need a
// gotd media cache that only the real client has).

import (
	"strings"
	"testing"

	"github.com/gotd/td/tg"
)

func TestCoerceInputMedia(t *testing.T) {
	if got := CoerceInputMedia("not a map"); got != nil {
		t.Fatalf("non-map must be nil, got %T", got)
	}
	if got := CoerceInputMedia(map[string]any{"type": "mystery"}); got != nil {
		t.Fatalf("unknown kind must be nil, got %T", got)
	}
	photo := CoerceInputMedia(map[string]any{"type": "photo", "media": "photo:1", "ttl_seconds": 7.0})
	if _, ok := photo.(*tg.InputMediaPhoto); !ok {
		t.Fatalf("photo shape: %T", photo)
	}
	video := CoerceInputMedia(map[string]any{"type": "video", "duration": 3, "width": 128, "height": 72})
	vm, ok := video.(*tg.InputMediaUploadedDocument)
	if !ok {
		t.Fatalf("video shape: %T", video)
	}
	if vm.MimeType != "video/mp4" {
		t.Fatalf("video mime: %s", vm.MimeType)
	}
	doc := CoerceInputMedia(map[string]any{"type": "document"})
	dm, ok := doc.(*tg.InputMediaUploadedDocument)
	if !ok || dm.MimeType != "application/octet-stream" {
		t.Fatalf("document shape: %T", doc)
	}
}

func TestDecodeUploadHandle(t *testing.T) {
	f, ok := decodeUploadHandle("12:34:name.jpg")
	if !ok {
		t.Fatal("valid handle rejected")
	}
	big, ok := f.(*tg.InputFileBig)
	if !ok || big.ID != 12 || big.Parts != 34 || big.Name != "name.jpg" {
		t.Fatalf("parsed %+v", f)
	}
	for _, bad := range []string{"", "x", "a:b:c", "1:2"} {
		if _, ok := decodeUploadHandle(bad); ok {
			t.Fatalf("bad handle %q accepted", bad)
		}
	}
	if _, ok := decodeUploadHandle(strings.Repeat("x", 3) + ":1:2"); ok {
		t.Fatal("non-numeric id must fail")
	}
}

func TestIntOrAndPhotoEmpty(t *testing.T) {
	m := map[string]any{"a": 3.0, "b": 2, "c": int64(5)}
	if intOr(m, "a", 0) != 3 || intOr(m, "b", 0) != 2 || intOr(m, "c", 0) != 5 || intOr(m, "z", 9) != 9 {
		t.Fatal("intOr type switch wrong")
	}
	if _, empty := inputPhotoOrEmpty("").(*tg.InputPhotoEmpty); !empty {
		t.Fatal("empty ref must yield InputPhotoEmpty")
	}
	if _, empty := inputPhotoOrEmpty("photo:1").(*tg.InputPhotoEmpty); !empty {
		t.Fatal("file-id resolution rides UploadMedia: placeholder expected")
	}
}
