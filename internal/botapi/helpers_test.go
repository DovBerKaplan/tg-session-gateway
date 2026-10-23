package botapi

import (
	"io"
	"net/http"
	"os"
	"time"
)

// test helpers around net/http so the test files stay focused on the
// behavioral assertions.

func appMux(app *App) *http.ServeMux {
	mux := http.NewServeMux()
	app.routes(mux)
	return mux
}

var httpClientDefault = &http.Client{Timeout: 30 * time.Second}

func httpNewRequest(method, url string, body io.Reader) (*http.Request, error) {
	return http.NewRequest(method, url, body)
}

func writeFile(path string, data []byte) error { return os.WriteFile(path, data, 0o644) }
