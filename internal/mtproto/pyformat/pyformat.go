// Package pyformat is a faithful port of pyrogram's message parser
// (pyrogram/parser/{markdown,html}.py) — the parse_mode contract the
// Python sidecar exposed. Telegram consumers sending text without an
// explicit parse_mode got pyrogram's DEFAULT mode (combined markdown +
// HTML); the Go client previously sent plain text, silently changing
// what consumers saw in the chat.
//
// Modes mirror pyrogram.enums.ParseMode:
//
//	ModeCombined  (DEFAULT)  markdown pass, then HTML pass (tags ACTIVE)
//	ModeMarkdown             markdown pass on HTML-escaped text (strict)
//	ModeHTML                 HTML pass only
//	ModeDisabled             verbatim
//
// The markdown pass is a TEXT→HTML transform (ported line by line).
// The HTML pass tokenizes pyrogram's limited tag set into OFFSET
// spans — like pyrogram, every open entity grows while text arrives,
// so nesting produces one covering entity (Bold spans across an inner
// Italic) — and then emits gotd entity.Builder tokens segment by
// segment; the builder's Complete() computes UTF-16 offsets.
//
// Known deliberate divergence: pyrogram CRASHES on
// "![x](https://non-tg-url)" (int('https…') ValueError in its emoji
// branch). We degrade gracefully: a non-numeric emoji id is left as
// literal text instead of panicking the send.
package pyformat

import (
	"fmt"
	"html"
	"regexp"
	"sort"
	"strings"

	"github.com/gotd/td/telegram/message/entity"
	"github.com/gotd/td/tg"
)

type Mode int

const (
	ModeCombined Mode = iota
	ModeMarkdown
	ModeHTML
	ModeDisabled
)

// ModeFromName maps the sidecar's parse_mode strings (the same values
// the Python sidecar accepted verbatim).
func ModeFromName(name string) Mode {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "markdown", "md":
		return ModeMarkdown
	case "html":
		return ModeHTML
	case "disabled", "none":
		return ModeDisabled
	default: // "", "default", "combined" — pyrogram's DEFAULT
		return ModeCombined
	}
}

// Apply parses text per pyrogram semantics and writes the tokens into
// the gotd entity builder.
func Apply(b *entity.Builder, text string, mode Mode) error {
	if mode == ModeDisabled {
		b.Plain(text)
		return nil
	}
	if mode == ModeHTML {
		return applyHTML(b, text)
	}
	transformed := markdownToHTML(text, mode == ModeMarkdown)
	return applyHTML(b, transformed)
}

// Build is the send-path entrypoint: parse, complete (UTF-16 offsets)
// and merge adjacent same-type entities — the exact wire shape
// pyrogram produced (one Bold across a nested Italic, not three
// per-segment Bolds).
func Build(text string, mode Mode) (string, []tg.MessageEntityClass, error) {
	b := &entity.Builder{}
	if err := Apply(b, text, mode); err != nil {
		return "", nil, err
	}
	msg, entities := b.Complete()
	return msg, MergeAdjacent(entities), nil
}

// MergeAdjacent coalesces entities of the same type and attributes
// whose ranges are contiguous. Grouped BY SHAPE first so a nested
// entity between two segments of its parent does not break the
// parent's contiguity (Bold-across-Italic must stay one Bold — the
// exact shape pyrogram's growing entities produce).
func MergeAdjacent(in []tg.MessageEntityClass) []tg.MessageEntityClass {
	if len(in) < 2 {
		return in
	}
	groups := map[string][]tg.MessageEntityClass{}
	var order []string
	for _, e := range in {
		key := shapeKey(e)
		if _, seen := groups[key]; !seen {
			order = append(order, key)
		}
		groups[key] = append(groups[key], e)
	}
	var out []tg.MessageEntityClass
	for _, key := range order {
		g := groups[key]
		sort.SliceStable(g, func(i, j int) bool { return g[i].GetOffset() < g[j].GetOffset() })
		for _, e := range g {
			if n := len(out) - 1; n >= 0 && shapeKey(out[n]) == key &&
				out[n].GetOffset()+out[n].GetLength() == e.GetOffset() {
				setLen(out[n], out[n].GetLength()+e.GetLength())
				continue
			}
			out = append(out, e)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].GetOffset() != out[j].GetOffset() {
			return out[i].GetOffset() < out[j].GetOffset()
		}
		return out[i].GetLength() < out[j].GetLength()
	})
	return out
}

// ── markdown pass (port of pyrogram/parser/markdown.py) ────────────

const (
	boldDelim      = "**"
	italicDelim    = "__"
	underlineDelim = "--"
	strikeDelim    = "~~"
	spoilerDelim   = "||"
	codeDelim      = "`"
	preDelim       = "```"
)

// markdownRe matches any single delimiter, in pyrogram's alternation
// order (pre before code so ``` wins over `).
var markdownRe = regexp.MustCompile("(```|`|~~|--|__|\\*\\*|\\|\\|)")

// urlRe matches [text](url) and the emoji form ![emoji](tg://emoji?id=N).
var urlRe = regexp.MustCompile(`(!?)\[(.+?)\]\((.+?)\)`)

var codeTagRe = regexp.MustCompile(`<code>.*?</code>`)

func markdownToHTML(text string, strict bool) string {
	if strict {
		// MARKDOWN mode: escape HTML so tags render literally
		// (pyrogram: html.escape quote=True — the same character set)
		text = html.EscapeString(text)
	}
	text = blockquoteParser(text)

	// protect <code>…</code> sections from markdown transformation
	placeholders := map[string]string{}
	text = codeTagRe.ReplaceAllStringFunc(text, func(section string) string {
		key := fmt.Sprintf("{CODE_SECTION_%d}", len(placeholders))
		placeholders[key] = section
		return key
	})

	fixedWidth := false
	open := map[string]bool{}
	for _, m := range markdownRe.FindAllString(text, -1) {
		delim := m
		switch delim {
		case preDelim, codeDelim:
			fixedWidth = !fixedWidth
		}
		if fixedWidth && delim != preDelim && delim != codeDelim {
			continue
		}

		var tag string
		switch delim {
		case boldDelim:
			tag = "b"
		case italicDelim:
			tag = "i"
		case underlineDelim:
			tag = "u"
		case strikeDelim:
			tag = "s"
		case codeDelim:
			tag = "code"
		case preDelim:
			tag = "pre"
		case spoilerDelim:
			tag = "spoiler"
		default:
			continue
		}

		if !open[delim] {
			open[delim] = true
			if delim == preDelim {
				// pyrogram: language = the rest of the opener line after ```
				opener := preDelim
				if first := strings.Index(text, preDelim); first >= 0 {
					line := text[first:]
					if nl := strings.IndexByte(line, '\n'); nl >= 0 {
						opener = line[:nl]
					} else {
						opener = line
					}
				}
				language := strings.TrimPrefix(opener, preDelim)
				text = replaceOnceFrom(text, opener, fmt.Sprintf(`<pre language=%q>`, language), 0)
				continue
			}
			text = replaceOnce(text, delim, "<"+tag+">")
		} else {
			open[delim] = false
			text = replaceOnce(text, delim, "</"+tag+">")
		}
	}

	// [text](url) → <a href>; ![emoji](tg://emoji?id=N) → <emoji id=N>
	for _, m := range urlRe.FindAllStringSubmatch(text, -1) {
		bang, label, url := m[1], m[2], m[3]
		full := m[0]
		if bang == "" && label != "" {
			text = replaceOnce(text, full, fmt.Sprintf(`<a href=%q>%s</a>`, url, label))
			continue
		}
		if bang == "!" {
			// pyrogram crashes on a non-numeric id here; we degrade
			// gracefully (documented divergence)
			if id := strings.TrimPrefix(url, "tg://emoji?id="); id != url && isDigits(id) {
				text = replaceOnce(text, full, fmt.Sprintf(`<emoji id=%s>%s</emoji>`, id, label))
			}
		}
	}

	for key, section := range placeholders {
		text = strings.Replace(text, key, section, 1)
	}
	return text
}

func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// replaceOnce replaces the first occurrence anywhere (pyrogram's
// replace_once with start=0).
func replaceOnce(s, old, new string) string {
	return replaceOnceFrom(s, old, new, 0)
}

// replaceOnceFrom replaces the first occurrence at-or-after start;
// the prefix [:start] stays untouched (pyrogram parser/utils.replace_once).
func replaceOnceFrom(s, old, new string, start int) string {
	if start < 0 || start > len(s) {
		return s
	}
	idx := strings.Index(s[start:], old)
	if idx < 0 {
		return s
	}
	at := start + idx
	return s[:at] + new + s[at+len(old):]
}

// blockquoteParser is a line-oriented port of pyrogram's
// Markdown.blockquote_parser: '>' / '**>' line prefixes become
// <blockquote> / <blockquote expandable>, quotes run until a plain line.
func blockquoteParser(text string) string {
	// after strict escaping '>' became &gt; — restore at line starts
	// so blockquotes still parse in MARKDOWN mode (pyrogram does the same)
	text = strings.ReplaceAll(text, "\n&gt;", "\n>")
	if strings.HasPrefix(text, "&gt;") {
		text = ">" + text[len("&gt;"):]
	}
	lines := strings.Split(text, "\n")
	out := make([]string, 0, len(lines))
	inQuote := false
	for _, line := range lines {
		switch {
		case strings.HasPrefix(line, ">"):
			if !inQuote {
				line = strings.TrimPrefix(line, "> ")
				line = strings.TrimPrefix(line, ">")
				line = "<blockquote>" + line
				inQuote = true
				out = append(out, strings.TrimSpace(line))
			} else {
				out = append(out, strings.TrimSpace(line[1:]))
			}
		case strings.HasPrefix(line, "**>"):
			if !inQuote {
				line = strings.TrimPrefix(line, "**> ")
				line = strings.TrimPrefix(line, "**>")
				line = "<blockquote expandable>" + line
				inQuote = true
				out = append(out, strings.TrimSpace(line))
			} else {
				out = append(out, strings.TrimSpace(line[3:]))
			}
		default:
			if inQuote {
				line = "</blockquote>" + line
				inQuote = false
			}
			out = append(out, line)
		}
	}
	if inQuote && len(out) > 0 {
		out[len(out)-1] += "</blockquote>"
	}
	return strings.Join(out, "\n")
}

// ── HTML pass (port of pyrogram/parser/html.py semantics) ──────────

// span is one formatting entity over rune offsets of the PLAIN text
// (the builder converts to UTF-16 at Complete time).
type span struct {
	start, end int
	formatter  entity.Formatter
}

// applyHTML strips whitespace around the outer tags (pyrogram's two
// HTML.parse subs), tokenizes into offset spans, drops zero-length
// spans, and emits builder tokens segment by segment.
func applyHTML(b *entity.Builder, text string) error {
	text = stripOuterWhitespace(text)
	plain, spans := tokenizeHTML(text)
	spans = dropZeroLength(spans)
	emitTokens(b, plain, spans)
	return nil
}

// stripOuterWhitespace ports pyrogram's two HTML.parse subs: strip
// whitespace between the text edges and the outermost tags.
func stripOuterWhitespace(text string) string {
	// ^\s*(<[\w<>=\s"]*>)\s* → "\1"
	lead := 0
	for lead < len(text) && isSpace(text[lead]) {
		lead++
	}
	rest := text[lead:]
	if !strings.HasPrefix(rest, "<") {
		return rest
	}
	end := 0
	matched := false
	for end < len(rest) {
		c := rest[end]
		if c == '>' {
			end++
			matched = true
			break
		}
		if c != '<' && c != '=' && c != '"' && c != '_' && !isSpace(c) && !isWordByte(c) {
			break
		}
		end++
	}
	if matched {
		after := end
		for after < len(rest) && isSpace(rest[after]) {
			after++
		}
		text = rest[:end] + rest[after:]
	} else {
		text = rest
	}

	// \s*(</[\w</>]*>)\s*$ → "\1"
	i := len(text)
	for i > 0 && isSpace(text[i-1]) {
		i--
	}
	j := i
	for j > 0 {
		c := text[j-1]
		if c == '<' {
			break
		}
		if c != '>' && c != '/' && !isWordByte(c) {
			j = i
			break
		}
		j--
	}
	if j > 0 && strings.HasPrefix(text[j:], "</") && j < i {
		before := j
		for before > 0 && isSpace(text[before-1]) {
			before--
		}
		text = text[:before] + text[j:i]
	}
	return text
}

func isSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\v' || c == '\f'
}

func isWordByte(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

var mentionRe = regexp.MustCompile(`^tg://user\?id=(\d+)$`)

// openFrame is one open tag while tokenizing.
type openFrame struct {
	tag       string
	formatter entity.Formatter
	start     int // rune offset of the first text after the tag
	end       int // rune offset after the last text seen
	sawText   bool
}

// tokenizeHTML walks the HTML text producing the plain text (entity
// references unescaped) and pyrogram-style GROWING spans: every open
// frame extends with each text run, so a Bold surrounding an Italic
// ends up as one covering span — pyrogram's exact nesting semantics.
func tokenizeHTML(text string) (string, []span) {
	var plain strings.Builder
	var open []openFrame
	var closed []span

	runes := 0 // rune count of `plain` so far
	appendRun := func(run string) {
		base := runes
		runes += len([]rune(run))
		plain.WriteString(run)
		for k := range open {
			if !open[k].sawText {
				open[k].sawText = true
				open[k].start = base
			}
			open[k].end = runes
		}
	}

	plainStart := 0
	i := 0
	for i < len(text) {
		lt := strings.IndexByte(text[i:], '<')
		if lt < 0 {
			break
		}
		at := i + lt
		if at > plainStart {
			appendRun(html.UnescapeString(text[plainStart:at]))
		}
		tag, attrs, closing, consumed, ok := parseTag(text[at:])
		if !ok {
			appendRun("<") // malformed markup is data (pyrogram behavior)
			plainStart = at + 1
			i = at + 1
			continue
		}
		if closing {
			// pop the most recent same-tag frame (pyrogram per-tag LIFO)
			for k := len(open) - 1; k >= 0; k-- {
				if open[k].tag == tag {
					if open[k].formatter != nil && open[k].sawText {
						closed = append(closed, span{start: open[k].start, end: open[k].end, formatter: open[k].formatter})
					}
					open = append(open[:k], open[k+1:]...)
					break
				}
			}
			// unmatched closer: ignored (pyrogram logs debug and continues)
		} else {
			open = append(open, openFrame{tag: tag, formatter: formatterFor(tag, attrs)})
		}
		plainStart = at + consumed
		i = plainStart
	}
	if len(text) > plainStart {
		appendRun(html.UnescapeString(text[plainStart:]))
	}
	return plain.String(), closed
}

// formatterFor maps a supported tag+attrs to its entity formatter;
// nil for unknown tags (content kept, unformatted — pyrogram ignores
// unknown tags but keeps their text).
func formatterFor(tag string, attrs map[string]string) entity.Formatter {
	switch tag {
	case "b", "strong":
		return entity.Bold()
	case "i", "em":
		return entity.Italic()
	case "u":
		return entity.Underline()
	case "s", "del", "strike":
		return entity.Strike()
	case "blockquote":
		_, expandable := attrs["expandable"]
		return entity.Blockquote(expandable)
	case "code":
		return entity.Code()
	case "pre":
		return entity.Pre(attrs["language"])
	case "spoiler":
		return entity.Spoiler()
	case "a":
		url := attrs["href"]
		if m := mentionRe.FindStringSubmatch(url); m != nil {
			var id int64
			_, _ = fmt.Sscan(m[1], &id)
			return entity.MentionName(&tg.InputUser{UserID: id})
		}
		return entity.TextURL(url)
	case "emoji":
		var id int64
		_, _ = fmt.Sscan(attrs["id"], &id)
		return entity.CustomEmoji(id)
	default:
		return nil
	}
}

// dropZeroLength removes spans that never saw text (pyrogram drops
// zero-length entities at the end of HTML.parse).
func dropZeroLength(spans []span) []span {
	out := spans[:0]
	for _, sp := range spans {
		if sp.end > sp.start {
			out = append(out, sp)
		}
	}
	return out
}

// emitTokens cuts the plain text at every span boundary; each segment
// is written with Format() carrying every span covering it.
func emitTokens(b *entity.Builder, plain string, spans []span) {
	if plain == "" {
		return
	}
	runes := []rune(plain)
	bounds := map[int]bool{0: true, len(runes): true}
	for _, sp := range spans {
		bounds[sp.start] = true
		bounds[sp.end] = true
	}
	points := make([]int, 0, len(bounds))
	for p := range bounds {
		points = append(points, p)
	}
	sort.Ints(points)
	for i := 0; i+1 < len(points); i++ {
		from, to := points[i], points[i+1]
		if to <= from || from >= len(runes) {
			continue
		}
		seg := string(runes[from:to])
		if seg == "" {
			continue
		}
		var fs []entity.Formatter
		for _, sp := range spans {
			if sp.start <= from && to <= sp.end {
				fs = append(fs, sp.formatter)
			}
		}
		if len(fs) == 0 {
			b.Plain(seg)
		} else {
			b.Format(seg, fs...)
		}
	}
}

// setLen extends an entity's length (merge helper).
func setLen(e tg.MessageEntityClass, n int) {
	switch v := e.(type) {
	case *tg.MessageEntityBold:
		v.Length = n
	case *tg.MessageEntityItalic:
		v.Length = n
	case *tg.MessageEntityUnderline:
		v.Length = n
	case *tg.MessageEntityStrike:
		v.Length = n
	case *tg.MessageEntityCode:
		v.Length = n
	case *tg.MessageEntitySpoiler:
		v.Length = n
	case *tg.MessageEntityPre:
		v.Length = n
	case *tg.MessageEntityBlockquote:
		v.Length = n
	case *tg.MessageEntityTextURL:
		v.Length = n
	case *tg.InputMessageEntityMentionName:
		v.Length = n
	case *tg.MessageEntityCustomEmoji:
		v.Length = n
	}
}

func shapeKey(e tg.MessageEntityClass) string {
	switch v := e.(type) {
	case *tg.MessageEntityPre:
		return fmt.Sprintf("pre:%s", v.Language)
	case *tg.MessageEntityBlockquote:
		return fmt.Sprintf("quote:%v", v.Collapsed)
	case *tg.MessageEntityTextURL:
		return fmt.Sprintf("url:%s", v.URL)
	case *tg.InputMessageEntityMentionName:
		return fmt.Sprintf("mention:%d", userIDOf(v.UserID))
	case *tg.MessageEntityCustomEmoji:
		return fmt.Sprintf("emoji:%d", v.DocumentID)
	default:
		return fmt.Sprintf("%T", e)
	}
}

func userIDOf(u tg.InputUserClass) int64 {
	if user, ok := u.(*tg.InputUser); ok {
		return user.UserID
	}
	return -1
}

// parseTag parses "</name ...>" or "<name attr=...>" at the start of s.
// ok=false means this '<' is not a tag (literal data). consumed is the
// byte length including '>'.
func parseTag(s string) (name string, attrs map[string]string, closing bool, consumed int, ok bool) {
	if len(s) < 2 {
		return "", nil, false, 0, false
	}
	rest := s[1:]
	isClosing := false
	if rest[0] == '/' {
		isClosing = true
		rest = rest[1:]
	}
	end := 0
	for end < len(rest) {
		c := rest[end]
		if c == '>' || c == ' ' || c == '\t' || c == '\n' || c == '/' {
			break
		}
		end++
	}
	if end == 0 {
		return "", nil, false, 0, false
	}
	name = strings.ToLower(rest[:end])
	body := rest[end:]
	gt := strings.IndexByte(body, '>')
	if gt < 0 {
		return "", nil, false, 0, false
	}
	attrStr := body[:gt]
	slashLen := 0
	if isClosing {
		slashLen = 1
	}
	consumed = 1 + slashLen + end + gt + 1 // '<' ['/'] name body '>'
	if isClosing {
		return name, nil, true, consumed, true
	}
	return name, parseAttrs(attrStr), false, consumed, true
}

var attrRe = regexp.MustCompile(`([a-zA-Z_:][-a-zA-Z0-9_:.]*)\s*(?:=\s*("[^"]*"|'[^']*'|[^\s"'>]+))?`)

// parseAttrs mirrors Python HTMLParser: bare attributes map to "".
func parseAttrs(s string) map[string]string {
	attrs := map[string]string{}
	for _, m := range attrRe.FindAllStringSubmatch(s, -1) {
		key := strings.ToLower(m[1])
		val := m[2]
		if val == "" {
			attrs[key] = ""
			continue
		}
		if len(val) >= 2 && (val[0] == '"' || val[0] == '\'') && val[len(val)-1] == val[0] {
			val = val[1 : len(val)-1]
		}
		attrs[key] = html.UnescapeString(val)
	}
	return attrs
}
