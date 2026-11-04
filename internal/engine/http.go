package engine

import (
	"context"
	"fmt"
	"io"
	"net/http"
)

// HTTPDoer sends one HTTP request per call. A transport error or a 5xx
// status is an error; the body is read to the end so that latency covers
// the whole response.
type HTTPDoer struct {
	Client *http.Client
	Method string
	URL    string
}

func (h *HTTPDoer) Do(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, h.Method, h.URL, nil)
	if err != nil {
		return err
	}
	resp, err := h.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		return err
	}
	if resp.StatusCode >= 500 {
		return fmt.Errorf("engine: %s %s: status %d", h.Method, h.URL, resp.StatusCode)
	}
	return nil
}
