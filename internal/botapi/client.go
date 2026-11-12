package botapi

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

// httpRequest is the upstream surface the gateway speaks (the base URL
// lives in the client; the URL here is a path or absolute for push).
type httpRequest struct {
	Method  string
	URL     string
	Body    map[string]any // JSON body (marshaled when RawBody is nil)
	RawBody []byte         // pre-encoded body (push mode, raw forward)
	// Reader streams a pre-sized body (multipart uploads) instead of
	// buffering. ContentLength must be set alongside it so the request
	// is sent with Content-Length preserved (a generic io.Reader alone
	// makes net/http send chunked).
	Reader        io.Reader
	ContentLength int64
	Header        map[string]string
	ctx           context.Context
}

// httpResponse is what comes back: status + raw body bytes (the proxy
// passes upstream bodies through VERBATIM). For streamed file downloads
// BodyStream is set and Body may be empty — caller must Close BodyStream.
type httpResponse struct {
	StatusCode int
	Body       []byte
	Header     http.Header
	BodyStream io.ReadCloser // optional; when set, Body is not filled
}

// HTTPClient abstracts the upstream HTTP plane so tests can fake
// api.telegram.org and consumer push endpoints.
type HTTPClient interface {
	Do(req *httpRequest) (*httpResponse, error)
}

// StreamHTTPClient optionally streams large GET responses without buffering.
type StreamHTTPClient interface {
	HTTPClient
	DoStream(req *httpRequest) (*httpResponse, error)
}

// UpstreamClient is the real HTTP client around a base URL.
type UpstreamClient struct {
	Base string
	HC   *http.Client
}

// UpstreamClientOptions configures TLS trust and timeouts.
type UpstreamClientOptions struct {
	Timeout   time.Duration
	CAFile    string // extra PEM CAs; never disables verification
	EnableH2  bool
}

// NewUpstreamClient builds the api.telegram.org client with defaults.
func NewUpstreamClient(base string) *UpstreamClient {
	return NewUpstreamClientOpts(base, UpstreamClientOptions{Timeout: 60 * time.Second, EnableH2: true})
}

// NewUpstreamClientOpts builds an upstream client with CA file and timeout.
func NewUpstreamClientOpts(base string, opts UpstreamClientOptions) *UpstreamClient {
	if opts.Timeout <= 0 {
		opts.Timeout = 60 * time.Second
	}
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ForceAttemptHTTP2:     opts.EnableH2,
	}
	if opts.CAFile != "" {
		tlsCfg, err := loadExtraCA(opts.CAFile)
		if err == nil {
			transport.TLSClientConfig = tlsCfg
		}
	}
	return &UpstreamClient{
		Base: strings.TrimRight(base, "/"),
		HC:   &http.Client{Timeout: opts.Timeout, Transport: transport},
	}
}

func loadExtraCA(path string) (*tls.Config, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("GW_UPSTREAM_CA_FILE: no certificates parsed from %s", path)
	}
	return &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}, nil
}

// SetBase updates the upstream base URL (failover).
func (u *UpstreamClient) SetBase(base string) {
	u.Base = strings.TrimRight(base, "/")
}

// Do sends the request (relative URLs resolve against Base).
// JSON/API responses are buffered (capped); use DoStream for large files.
func (u *UpstreamClient) Do(req *httpRequest) (*httpResponse, error) {
	return u.do(req, false)
}

// DoStream returns BodyStream for the caller to copy; does not buffer the body.
func (u *UpstreamClient) DoStream(req *httpRequest) (*httpResponse, error) {
	return u.do(req, true)
}

func (u *UpstreamClient) do(req *httpRequest, stream bool) (*httpResponse, error) {
	url := req.URL
	if !bytes.HasPrefix([]byte(url), []byte("http")) {
		url = u.Base + url
	}
	var reader io.Reader
	if req.Reader != nil {
		reader = req.Reader
	} else if req.RawBody != nil {
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
	if req.Reader != nil {
		hreq.ContentLength = req.ContentLength
	}
	if req.Header["Content-Type"] != "" {
		hreq.Header.Set("Content-Type", req.Header["Content-Type"])
	} else if req.Body != nil || req.RawBody != nil || req.Reader != nil {
		hreq.Header.Set("Content-Type", "application/json")
	}
	if req.Header != nil {
		for k, v := range req.Header {
			if k == "Content-Type" {
				continue
			}
			hreq.Header.Set(k, v)
		}
	}
	// Leave Accept-Encoding unset so net/http transparently negotiates gzip.
	ctx := req.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	hreq = hreq.WithContext(ctx)
	resp, err := u.HC.Do(hreq)
	if err != nil {
		return nil, err
	}
	out := &httpResponse{StatusCode: resp.StatusCode, Header: resp.Header.Clone()}
	if stream {
		out.BodyStream = resp.Body
		return out, nil
	}
	defer resp.Body.Close()
	// Cap buffered API responses at 64Mi (getFile JSON etc.). File bytes use DoStream.
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return nil, err
	}
	out.Body = body
	return out, nil
}
