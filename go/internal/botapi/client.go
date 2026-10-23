package botapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"
)

// httpRequest is the upstream surface the gateway speaks (the base URL
// lives in the client; the URL here is a path or absolute for push).
type httpRequest struct {
	Method  string
	URL     string
	Body    map[string]any // JSON body (marshaled when RawBody is nil)
	RawBody []byte         // pre-encoded body (push mode, raw forward)
	Header  map[string]string
	ctx     context.Context
}

// httpResponse is what comes back: status + raw body bytes (the proxy
// passes upstream bodies through VERBATIM).
type httpResponse struct {
	StatusCode int
	Body       []byte
	Header     http.Header
}

// HTTPClient abstracts the upstream HTTP plane so tests can fake
// api.telegram.org and consumer push endpoints.
type HTTPClient interface {
	Do(req *httpRequest) (*httpResponse, error)
}

// UpstreamClient is the real HTTP client around a base URL.
type UpstreamClient struct {
	Base string
	HC   *http.Client
}

// NewUpstreamClient builds the api.telegram.org client.
func NewUpstreamClient(base string) *UpstreamClient {
	return &UpstreamClient{Base: base, HC: &http.Client{Timeout: 60 * time.Second}}
}

// Do sends the request (relative URLs resolve against Base).
func (u *UpstreamClient) Do(req *httpRequest) (*httpResponse, error) {
	url := req.URL
	if !bytes.HasPrefix([]byte(url), []byte("http")) {
		url = u.Base + url
	}
	var reader io.Reader
	if req.RawBody != nil {
		reader = bytes.NewReader(req.RawBody)
	} else if req.Body != nil {
		raw, err := json.Marshal(req.Body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(raw)
	} else if req.Method == "POST" {
		reader = bytes.NewReader([]byte("{}"))
	}
	hreq, err := http.NewRequest(req.Method, url, reader)
	if err != nil {
		return nil, err
	}
	if req.Header["Content-Type"] != "" {
		hreq.Header.Set("Content-Type", req.Header["Content-Type"])
	} else if req.Body != nil || req.RawBody != nil {
		hreq.Header.Set("Content-Type", "application/json")
	}
	ctx := req.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	hreq = hreq.WithContext(ctx)
	resp, err := u.HC.Do(hreq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return nil, err
	}
	return &httpResponse{StatusCode: resp.StatusCode, Body: body, Header: resp.Header}, nil
}
