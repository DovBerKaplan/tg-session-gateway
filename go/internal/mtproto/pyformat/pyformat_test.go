package pyformat

// Differential conformance: the expectations in testdata/cases.json
// were produced by the REAL pyrogram parser (pyrofork, the exact
// library the Python sidecar ran) across all three modes, with
// offsets/lengths converted to UTF-16 — the wire format Telegram
// requires. Regenerate:
//
//	.venv/bin/python tests/conformance_go/record_pyformat_cases.py

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/gotd/td/tg"
)

type wantEntity struct {
	Type       string `json:"type"`
	Offset     int    `json:"offset"`
	Length     int    `json:"length"`
	URL        string `json:"url"`
	Language   string `json:"language"`
	Collapsed  any    `json:"collapsed"`
	DocumentID int64  `json:"document_id"`
	UserID     any    `json:"user_id"`
}

type wantCase struct {
	Input    string       `json:"input"`
	Mode     string       `json:"mode"`
	Message  string       `json:"message"`
	Entities []wantEntity `json:"entities"`
}

func TestAgainstRealPyrogramParser(t *testing.T) {
	raw, err := os.ReadFile("testdata/cases.json")
	if err != nil {
		t.Fatalf("cases.json missing (%v) — regenerate with the recorder", err)
	}
	var cases []wantCase
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatalf("cases.json unparseable: %v", err)
	}
	if len(cases) == 0 {
		t.Fatal("empty corpus")
	}

	for _, tc := range cases {
		t.Run(tc.Mode+"/"+tc.Input, func(t *testing.T) {
			gotText, gotEntities, err := Build(tc.Input, modeOf(tc.Mode))
			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			if gotText != tc.Message {
				t.Fatalf("text:\n  pyrogram: %q\n  go:       %q", tc.Message, gotText)
			}
			if len(gotEntities) != len(tc.Entities) {
				t.Fatalf("entity count: pyrogram=%d go=%d\n  pyrogram: %+v\n  go:       %s",
					len(tc.Entities), len(gotEntities), tc.Entities, describe(gotEntities))
			}
			// compare in a stable order (pyrogram cases are sorted by offset/length/type)
			got := sortByOffset(gotEntities)
			for i, w := range tc.Entities {
				g := got[i]
				if name := typeName(g); name != w.Type {
					t.Fatalf("entity %d type: pyrogram=%s go=%s (text=%q)", i, w.Type, name, gotText)
				}
				if int(g.GetOffset()) != w.Offset || int(g.GetLength()) != w.Length {
					t.Fatalf("entity %d (%s): pyrogram off/len=%d/%d go=%d/%d (text=%q)",
						i, w.Type, w.Offset, w.Length, g.GetOffset(), g.GetLength(), gotText)
				}
				switch e := g.(type) {
				case *tg.MessageEntityTextURL:
					if e.URL != w.URL {
						t.Fatalf("entity %d url: %q vs %q", i, w.URL, e.URL)
					}
				case *tg.MessageEntityPre:
					if e.Language != w.Language {
						t.Fatalf("entity %d language: %q vs %q", i, w.Language, e.Language)
					}
				case *tg.MessageEntityBlockquote:
					wantCollapsed := w.Collapsed == true
					if e.Collapsed != wantCollapsed {
						t.Fatalf("entity %d collapsed: %v vs %v", i, wantCollapsed, e.Collapsed)
					}
				case *tg.MessageEntityCustomEmoji:
					if e.DocumentID != w.DocumentID {
						t.Fatalf("entity %d document_id: %d vs %d", i, w.DocumentID, e.DocumentID)
					}
				}
			}
		})
	}
}

func modeOf(name string) Mode {
	switch name {
	case "markdown":
		return ModeMarkdown
	case "html":
		return ModeHTML
	default:
		return ModeCombined
	}
}

func typeName(e tg.MessageEntityClass) string {
	s := fmt0(e)
	return s
}

func sortByOffset(in []tg.MessageEntityClass) []tg.MessageEntityClass {
	out := append([]tg.MessageEntityClass{}, in...)
	// stable enough for the comparison: offset, length, type name
	for i := 1; i < len(out); i++ {
		for j := i; j > 0; j-- {
			a, b := out[j-1], out[j]
			if a.GetOffset() < b.GetOffset() ||
				(a.GetOffset() == b.GetOffset() && a.GetLength() < b.GetLength()) {
				break
			}
			if a.GetOffset() == b.GetOffset() && a.GetLength() == b.GetLength() &&
				fmt0(a) <= fmt0(b) {
				break
			}
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
}

func describe(in []tg.MessageEntityClass) string {
	s := "["
	for i, e := range in {
		if i > 0 {
			s += " "
		}
		s += fmt0(e) + " " + itoa(int(e.GetOffset())) + "/" + itoa(int(e.GetLength()))
	}
	return s + "]"
}

func fmt0(e tg.MessageEntityClass) string {
	raw := ""
	switch e.(type) {
	case *tg.MessageEntityBold:
		raw = "MessageEntityBold"
	case *tg.MessageEntityItalic:
		raw = "MessageEntityItalic"
	case *tg.MessageEntityUnderline:
		raw = "MessageEntityUnderline"
	case *tg.MessageEntityStrike:
		raw = "MessageEntityStrike"
	case *tg.MessageEntityCode:
		raw = "MessageEntityCode"
	case *tg.MessageEntityPre:
		raw = "MessageEntityPre"
	case *tg.MessageEntitySpoiler:
		raw = "MessageEntitySpoiler"
	case *tg.MessageEntityTextURL:
		raw = "MessageEntityTextUrl"
	case *tg.MessageEntityBlockquote:
		raw = "MessageEntityBlockquote"
	case *tg.MessageEntityCustomEmoji:
		raw = "MessageEntityCustomEmoji"
	case *tg.InputMessageEntityMentionName:
		raw = "InputMessageEntityMentionName"
	case *tg.MessageEntityUnknown:
		raw = "MessageEntityUnknown"
	default:
		raw = "?"
	}
	return raw
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	buf := [20]byte{}
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
