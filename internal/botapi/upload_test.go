package botapi

// Upload-path tests: streaming multipart (byte-exact forwarding with a
// declared ContentLength), and 413 (never silent truncation) when a
// body exceeds its cap.

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestMultipartStreamsByteExact(t *testing.T) {
	fake := newFakeTelegram(nil)
	_, ts := newTestApp(t, fake, t.TempDir())
	base := ts.URL

	code, _ := post(t, base, "/admin/bots", `{"token":"`+token+`","alias":"up"}`,
		map[string]string{"Authorization": "Bearer adm-secret"})
	if code != 200 {
		t.Fatalf("register = %d", code)
	}

	// a multipart body with the chat_id field early and a big payload —
	// big enough that the streaming prefix (64KB) does NOT contain it all
	var buf bytes.Buffer
	buf.WriteString("--BOUND\r\nContent-Disposition: form-data; name=\"chat_id\"\r\n\r\n10\r\n--BOUND\r\nContent-Disposition: form-data; name=\"photo\"; filename=\"p.bin\"\r\nContent-Type: application/octet-stream\r\n\r\n")
	payload := bytes.Repeat([]byte{0xAB}, 200<<10) // 200KB
	buf.Write(payload)
	buf.WriteString("\r\n--BOUND--\r\n")
	body := buf.Bytes()

	req, _ := httpNewRequest("POST", base+"/bots/up/sendPhoto", bytes.NewReader(body))
	req.Header.Set("Content-Type", "multipart/form-data; boundary=BOUND")
	req.Header.Set("Authorization", "Bearer app-secret")
	req.ContentLength = int64(len(body)) // the streaming trigger
	resp, err := httpClientDefault.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	out, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("sendPhoto(stream) = %d: %s", resp.StatusCode, out)
	}

	// the fake recorded the EXACT bytes, and the rate guard saw chat 10
	fake.mu.Lock()
	var got []byte
	if len(fake.rawSends) > 0 {
		got = fake.rawSends[len(fake.rawSends)-1]
	}
	fake.mu.Unlock()
	if !bytes.Equal(got, body) {
		t.Fatalf("streamed body mutated: got %d bytes, want %d", len(got), len(body))
	}
}

func TestBodyOverLimitAnswers413(t *testing.T) {
	fake := newFakeTelegram(nil)
	_, ts := newTestApp(t, fake, t.TempDir())
	base := ts.URL
	code, _ := post(t, base, "/admin/bots", `{"token":"`+token+`","alias":"big"}`,
		map[string]string{"Authorization": "Bearer adm-secret"})
	if code != 200 {
		t.Fatalf("register = %d", code)
	}

	cases := []struct {
		name    string
		path    string
		ctype   string
		body    []byte
		limit   int
		withLen bool // declare ContentLength (else the buffered path)
	}{
		{"json 8MB", "/bots/big/sendMessage", "application/json",
			bytes.Repeat([]byte{'a'}, maxJSONBody+1), maxJSONBody, true},
		{"form 1MB", "/bots/big/sendMessage", "application/x-www-form-urlencoded",
			bytes.Repeat([]byte("x=1&"), (maxFormBody/4)+1), maxFormBody, true},
		{"multipart buffered 64MB", "/bots/big/sendPhoto", "multipart/form-data; boundary=B",
			bytes.Repeat([]byte{'m'}, maxMultipartBody+1), maxMultipartBody, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, _ := httpNewRequest("POST", base+tc.path, bytes.NewReader(tc.body))
			req.Header.Set("Content-Type", tc.ctype)
			req.Header.Set("Authorization", "Bearer app-secret")
			if tc.withLen {
				req.ContentLength = int64(len(tc.body))
			}
			resp, err := httpClientDefault.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = resp.Body.Close() }()
			out, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != http.StatusRequestEntityTooLarge {
				t.Fatalf("%s = %d (%s), want 413", tc.path, resp.StatusCode, strings.TrimSpace(string(out)))
			}
		})
	}
}

// TestMultipartChatIDAfterFileFallsBackToGlobal: when the file part
// arrives before chat_id, the prefix scan finds nothing — the send
// must still go through (global bucket + unresolved metric), never
// block or buffer the whole body.
func TestMultipartChatIDAfterFileFallsBackToGlobal(t *testing.T) {
	fake := newFakeTelegram(nil)
	_, ts := newTestApp(t, fake, t.TempDir())
	base := ts.URL
	code, _ := post(t, base, "/admin/bots", `{"token":"`+token+`","alias":"late"}`,
		map[string]string{"Authorization": "Bearer adm-secret"})
	if code != 200 {
		t.Fatalf("register = %d", code)
	}

	var buf bytes.Buffer
	buf.WriteString("--B\r\nContent-Disposition: form-data; name=\"photo\"; filename=\"p.bin\"\r\n\r\n")
	buf.Write(bytes.Repeat([]byte{0xCD}, 200<<10))
	buf.WriteString("\r\n--B\r\nContent-Disposition: form-data; name=\"chat_id\"\r\n\r\n10\r\n--B--\r\n")
	body := buf.Bytes()

	before := multipartChatUnresolved.Load()
	req, _ := httpNewRequest("POST", base+"/bots/late/sendPhoto", bytes.NewReader(body))
	req.Header.Set("Content-Type", "multipart/form-data; boundary=B")
	req.Header.Set("Authorization", "Bearer app-secret")
	req.ContentLength = int64(len(body))
	resp, err := httpClientDefault.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	out, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("sendPhoto(chat_id late) = %d: %s — must not block on unresolved chat", resp.StatusCode, out)
	}
	if multipartChatUnresolved.Load() <= before {
		t.Fatal("tg_gateway_multipart_chat_unresolved_total must count the fallback")
	}
	// the bytes still arrive verbatim
	fake.mu.Lock()
	var got []byte
	if len(fake.rawSends) > 0 {
		got = fake.rawSends[len(fake.rawSends)-1]
	}
	fake.mu.Unlock()
	if !bytes.Equal(got, body) {
		t.Fatalf("body mutated: %d vs %d bytes", len(got), len(body))
	}
}

// TestStreamedRequestPreservesContentLength: the fake captures the
// ContentLength the upstream request carried.
func TestStreamedRequestPreservesContentLength(t *testing.T) {
	fake := newFakeTelegram(nil)
	_, ts := newTestApp(t, fake, t.TempDir())
	base := ts.URL
	post(t, base, "/admin/bots", `{"token":"`+token+`","alias":"cl"}`,
		map[string]string{"Authorization": "Bearer adm-secret"})

	body := []byte("--B\r\nContent-Disposition: form-data; name=\"chat_id\"\r\n\r\n10\r\n--B--\r\n")
	req, _ := httpNewRequest("POST", base+"/bots/cl/sendPhoto", bytes.NewReader(body))
	req.Header.Set("Content-Type", "multipart/form-data; boundary=B")
	req.Header.Set("Authorization", "Bearer app-secret")
	req.ContentLength = int64(len(body))
	resp, err := httpClientDefault.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.ReadAll(resp.Body)
	fake.mu.Lock()
	cl := fake.lastContentLength
	fake.mu.Unlock()
	if cl != int64(len(body)) {
		t.Fatalf("upstream Content-Length = %d, want %d (preserved)", cl, int64(len(body)))
	}
}

// TestInvalidJSONIsTolerantParity: the Python gateway's _json_body
// swallows invalid JSON into {} and forwards that; the Go port keeps
// the same tolerance (documented at the site) — a request with a
// broken JSON body must reach the upstream with an empty object body,
// not 400.
func TestInvalidJSONIsTolerantParity(t *testing.T) {
	fake := newFakeTelegram(nil)
	_, ts := newTestApp(t, fake, t.TempDir())
	base := ts.URL
	code, _ := post(t, base, "/admin/bots", `{"token":"`+token+`","alias":"bad"}`,
		map[string]string{"Authorization": "Bearer adm-secret"})
	if code != 200 {
		t.Fatalf("register = %d", code)
	}
	resp, err := httpPostRaw(base+"/bots/bad/sendMessage", "application/json", []byte("{not json"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	out, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("invalid JSON = %d (%s), want 200 with upstream handling the empty body", resp.StatusCode, out)
	}
	fake.mu.Lock()
	sent := fake.sent
	fake.mu.Unlock()
	if len(sent) != 1 {
		t.Fatalf("upstream calls = %d, want 1", len(sent))
	}
	if _, ok := sent[0]["chat_id"]; ok {
		t.Fatalf("broken body must forward as {}: %v", sent[0])
	}
}

func httpPostRaw(url, ctype string, body []byte) (*http.Response, error) {
	req, err := httpNewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", ctype)
	req.Header.Set("Authorization", "Bearer app-secret")
	return httpClientDefault.Do(req)
}
