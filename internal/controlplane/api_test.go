package controlplane

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/loadforge/loadforge/internal/health"
	"github.com/loadforge/loadforge/internal/runstate"
	"github.com/loadforge/loadforge/internal/store"
	"github.com/loadforge/loadforge/internal/transport"
)

func doJSON(t *testing.T, method, url string, body any, out any) int {
	t.Helper()
	var rd *bytes.Reader
	if body != nil {
		rd = bytes.NewReader(mustJSON(body))
	} else {
		rd = bytes.NewReader(nil)
	}
	req, err := http.NewRequest(method, url, rd)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			t.Fatalf("%s %s: decode: %v", method, url, err)
		}
	}
	return resp.StatusCode
}

func TestAPIRunLifecycle(t *testing.T) {
	h := newHarness(t, harnessOpts{})
	srv := httptest.NewServer(h.c.Handler())
	t.Cleanup(srv.Close)

	var created struct{ ID, State string }
	if code := doJSON(t, "POST", srv.URL+"/runs", h.request(func(r *RunRequest) { r.DurationMS = 30_000 }), &created); code != http.StatusAccepted {
		t.Fatalf("POST /runs: %d", code)
	}
	h.waitState(created.ID, runstate.Running, 10*time.Second)

	var v RunView
	if code := doJSON(t, "GET", srv.URL+"/runs/"+created.ID, nil, &v); code != http.StatusOK {
		t.Fatalf("GET: %d", code)
	}
	if v.Verdict != "" || v.Results != nil || v.Diagnostics != nil || len(v.Workers) != 2 {
		t.Errorf("running run view: %+v", v)
	}

	if code := doJSON(t, "POST", srv.URL+"/runs/"+created.ID+"/stop", nil, nil); code != http.StatusAccepted {
		t.Fatalf("stop: %d", code)
	}
	h.waitTerminal(created.ID, 15*time.Second)
	v = RunView{}
	doJSON(t, "GET", srv.URL+"/runs/"+created.ID, nil, &v)
	if v.State != string(runstate.Completed) || v.Verdict == "" {
		t.Fatalf("view %+v", v)
	}
	if v.Verdict == string(health.Invalid) {
		if v.Results != nil || v.Diagnostics == nil {
			t.Errorf("INVALID run: results %+v diagnostics %+v", v.Results, v.Diagnostics)
		}
	} else if v.Results == nil || v.Results.Verdict != v.Verdict || v.Diagnostics != nil {
		t.Errorf("results %+v diagnostics %+v", v.Results, v.Diagnostics)
	}

	if code := doJSON(t, "POST", srv.URL+"/runs/"+created.ID+"/stop", nil, nil); code != http.StatusConflict {
		t.Errorf("stop of a terminal run: %d", code)
	}
	if code := doJSON(t, "POST", srv.URL+"/runs/nope/stop", nil, nil); code != http.StatusNotFound {
		t.Errorf("stop of an unknown run: %d", code)
	}
	if code := doJSON(t, "GET", srv.URL+"/runs/nope", nil, nil); code != http.StatusNotFound {
		t.Errorf("GET unknown: %d", code)
	}
}

func TestAPIRefusals(t *testing.T) {
	h := newHarness(t, harnessOpts{})
	srv := httptest.NewServer(h.c.Handler())
	t.Cleanup(srv.Close)
	for name, tc := range map[string]struct {
		body any
		want int
	}{
		"unknown field": {map[string]any{"workers": []string{"w1"}, "bogus": 1}, http.StatusBadRequest},
		"bad shape":     {h.request(func(r *RunRequest) { r.Method = "" }), http.StatusBadRequest},
		"over the cap":  {h.request(func(r *RunRequest) { r.RatePerWorker = 1e6 }), http.StatusUnprocessableEntity},
		"unseen worker": {h.request(func(r *RunRequest) { r.Workers = []string{"ghost"} }), http.StatusConflict},
	} {
		var e map[string]string
		if code := doJSON(t, "POST", srv.URL+"/runs", tc.body, &e); code != tc.want || e["error"] == "" {
			t.Errorf("%s: %d %v, want %d", name, code, e, tc.want)
		}
	}
}

// A result row can exist before the verdict (the aggregator finalizes a
// STOPPING run first). The API must not show it until the verdict is set.
func TestAPINeverShowsResultsWithoutAVerdict(t *testing.T) {
	h := newHarness(t, harnessOpts{noStart: true})
	h.c = h.startController(h.nc, h.st, h.leases)
	prep := transport.Prepare{Workers: []string{"w1"}, Method: "GET", TargetURL: h.target, Rate: 10, DurationMS: 1000,
		MaxInFlight: 1, RequestTimeoutMS: 100, DrainTimeoutMS: 100}
	h.st.seedRun("stopping", runstate.Stopping, 900, prep)
	_, _ = h.leases.Acquire(context.Background(), store.RunLeaseKey("stopping"), 900) // keep the sweep away
	h.st.mu.Lock()
	h.st.results["stopping"] = store.ResultRow{RunID: "stopping", Percentiles: store.Percentiles{P99: 1234}}
	h.st.mu.Unlock()

	v, err := h.c.View(context.Background(), "stopping")
	if err != nil {
		t.Fatal(err)
	}
	if v.Results != nil || v.Diagnostics != nil || v.Verdict != "" {
		t.Errorf("results shown without a verdict: %+v", v)
	}

	// Once ABORTED (always INVALID) the numbers are diagnostics only.
	if err := h.st.Transition(context.Background(), store.Transition{RunID: "stopping", Fence: 900, From: runstate.Stopping,
		To: runstate.Aborted, Verdict: health.Invalid}); err != nil {
		t.Fatal(err)
	}
	v, _ = h.c.View(context.Background(), "stopping")
	if v.Results != nil || v.Diagnostics == nil || v.Diagnostics.P99US != 1234 || v.Diagnostics.Verdict != string(health.Invalid) {
		t.Errorf("INVALID view: results %+v diagnostics %+v", v.Results, v.Diagnostics)
	}
}

type fakeLive struct {
	m   map[string]string
	err error
}

func (f fakeLive) Progress(context.Context, string) (map[string]string, bool, error) {
	return f.m, f.m != nil, f.err
}

func TestAPILiveIsAlwaysProvisional(t *testing.T) {
	h := newHarness(t, harnessOpts{noStart: true})
	prep := transport.Prepare{Workers: []string{"w1"}, Method: "GET", TargetURL: h.target, Rate: 10, DurationMS: 1000,
		MaxInFlight: 1, RequestTimeoutMS: 100, DrainTimeoutMS: 100}
	h.st.seedRun("r1", runstate.Running, 900, prep)
	_, _ = h.leases.Acquire(context.Background(), store.RunLeaseKey("r1"), 900)

	get := func(live LiveCache) LiveView {
		t.Helper()
		c, err := New(h.cfg, h.st, h.leases, h.nc, live)
		if err != nil {
			t.Fatal(err)
		}
		srv := httptest.NewServer(c.Handler())
		defer srv.Close()
		var v LiveView
		if code := doJSON(t, "GET", srv.URL+"/runs/r1/live", nil, &v); code != http.StatusOK {
			t.Fatalf("live: %d", code)
		}
		return v
	}

	// From Redis; a cache that somehow carried a verdict is overridden.
	v := get(fakeLive{m: map[string]string{"ok": "7", "verdict": "VALID", "provisional": "false"}})
	if v.Source != "redis" || !v.Provisional || v.Verdict != "PENDING" || v.Progress["verdict"] != "PENDING" ||
		v.Progress["provisional"] != "true" || v.Progress["ok"] != "7" {
		t.Errorf("redis view %+v", v)
	}
	// Nothing anywhere.
	if v := get(fakeLive{}); v.Source != "none" || v.Progress != nil || v.Verdict != "PENDING" {
		t.Errorf("empty view %+v", v)
	}
	// Redis down: Postgres fallback.
	h.st.mu.Lock()
	h.st.progress["r1"] = store.Progress{Seconds: 3, Counts: store.Counts{OK: 42}}
	h.st.mu.Unlock()
	v = get(fakeLive{err: errors.New("connection refused")})
	if v.Source != "postgres" || v.Progress["ok"] != "42" || v.Progress["verdict"] != "PENDING" {
		t.Errorf("fallback view %+v", v)
	}
	if v := get(nil); v.Source != "postgres" {
		t.Errorf("no cache configured: %+v", v)
	}
}
