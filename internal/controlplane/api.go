package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/loadforge/loadforge/internal/aggregator"
	"github.com/loadforge/loadforge/internal/health"
	"github.com/loadforge/loadforge/internal/store"
)

// Handler serves the controller's HTTP API (design.md §8.5):
//
//	POST /runs             create and start a run
//	POST /runs/{id}/stop   stop it (before START, that aborts it)
//	GET  /runs/{id}        state, workers, events, and results with their verdict
//	GET  /runs/{id}/live   provisional progress, never a verdict
func (c *Controller) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /runs", c.handleCreate)
	mux.HandleFunc("POST /runs/{id}/stop", c.handleStop)
	mux.HandleFunc("GET /runs/{id}", c.handleGet)
	mux.HandleFunc("GET /runs/{id}/live", c.handleLive)
	return mux
}

func (c *Controller) handleCreate(w http.ResponseWriter, r *http.Request) {
	var req RunRequest
	dec := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "body: "+err.Error())
		return
	}
	run, err := c.CreateRun(r.Context(), req)
	if err != nil {
		c.fail(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"id": run.ID, "state": run.State})
}

func (c *Controller) handleStop(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := c.RequestStop(r.Context(), id); err != nil {
		c.fail(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"id": id, "stop_requested": true})
}

// RunView is GET /runs/{id}. Results appear only once the run has its
// verdict, and an INVALID run's numbers are under Diagnostics, never
// Results: they are kept for debugging, not as headline figures
// (CLAUDE.md rule 5, design.md §5).
type RunView struct {
	ID                string          `json:"id"`
	State             string          `json:"state"`
	CreatedAt         time.Time       `json:"created_at"`
	UpdatedAt         time.Time       `json:"updated_at"`
	Config            json.RawMessage `json:"config"`
	Thresholds        json.RawMessage `json:"thresholds"`
	StopRequested     bool            `json:"stop_requested"`
	FinalizeRequested bool            `json:"finalize_requested"`
	Workers           []WorkerView    `json:"workers"`
	Events            []EventView     `json:"events"`
	Verdict           string          `json:"verdict,omitempty"`
	VerdictDetail     json.RawMessage `json:"verdict_detail,omitempty"`
	Results           *ResultView     `json:"results,omitempty"`
	Diagnostics       *ResultView     `json:"diagnostics,omitempty"`
}

// WorkerView is a run member.
type WorkerView struct {
	WorkerID      string   `json:"worker_id"`
	PrepareStatus string   `json:"prepare_status,omitempty"`
	PrepareDetail string   `json:"prepare_detail,omitempty"`
	ClockOffsetNS *int64   `json:"clock_offset_ns"`
	CapacityRPS   *float64 `json:"capacity_rps"`
	FinalSeq      *uint64  `json:"final_seq"`
	StopDetail    string   `json:"stop_detail,omitempty"`
	Lost          bool     `json:"lost"`
}

// EventView is a run_events row.
type EventView struct {
	At         time.Time       `json:"at"`
	Kind       string          `json:"kind"`
	FenceToken uint64          `json:"fence_token"`
	From       string          `json:"from_state,omitempty"`
	To         string          `json:"to_state,omitempty"`
	WorkerID   string          `json:"worker_id,omitempty"`
	CmdSeq     uint64          `json:"cmd_seq,omitempty"`
	Detail     json.RawMessage `json:"detail,omitempty"`
}

// ResultView is a run's merged result. It always carries the verdict it
// was judged with.
type ResultView struct {
	Verdict           string `json:"verdict"`
	ExpectedSnapshots int64  `json:"expected_snapshots"`
	ReceivedSnapshots int64  `json:"received_snapshots"`
	RejectedSnapshots int64  `json:"rejected_snapshots"`
	Intended          int64  `json:"intended"`
	OK                int64  `json:"ok"`
	Err               int64  `json:"err"`
	Timeout           int64  `json:"timeout"`
	Dropped           int64  `json:"generator_dropped"`
	Unfinished        int64  `json:"unfinished"`
	Clamped           int64  `json:"clamped"`
	P50US             int64  `json:"p50_us"`
	P90US             int64  `json:"p90_us"`
	P99US             int64  `json:"p99_us"`
	P999US            int64  `json:"p999_us"`
	MaxUS             int64  `json:"max_us"`
	LagP99US          int64  `json:"dispatch_lag_p99_us"`
}

func (c *Controller) handleGet(w http.ResponseWriter, r *http.Request) {
	v, err := c.View(r.Context(), r.PathValue("id"))
	if err != nil {
		c.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, v)
}

// View assembles GET /runs/{id}.
func (c *Controller) View(ctx context.Context, id string) (RunView, error) {
	run, err := c.st.GetRun(ctx, id)
	if err != nil {
		return RunView{}, err
	}
	v := RunView{
		ID: run.ID, State: string(run.State), CreatedAt: run.CreatedAt, UpdatedAt: run.UpdatedAt,
		Config: run.Config, Thresholds: run.Thresholds, StopRequested: run.StopRequested,
		FinalizeRequested: run.FinalizeRequested, Workers: []WorkerView{}, Events: []EventView{},
	}
	workers, err := c.st.RunWorkers(ctx, id)
	if err != nil {
		return RunView{}, err
	}
	for _, w := range workers {
		v.Workers = append(v.Workers, WorkerView{WorkerID: w.WorkerID, PrepareStatus: string(w.PrepareStatus), PrepareDetail: w.PrepareDetail,
			ClockOffsetNS: w.ClockOffsetNS, CapacityRPS: w.CapacityRPS, FinalSeq: w.FinalSeq, StopDetail: w.StopDetail, Lost: w.Lost})
	}
	events, err := c.st.Events(ctx, id)
	if err != nil {
		return RunView{}, err
	}
	for _, e := range events {
		v.Events = append(v.Events, EventView{At: e.At, Kind: e.Kind, FenceToken: e.FenceToken, From: string(e.FromState),
			To: string(e.ToState), WorkerID: e.WorkerID, CmdSeq: e.CmdSeq, Detail: e.Detail})
	}
	if run.Verdict == "" {
		return v, nil // no verdict, no results
	}
	v.Verdict, v.VerdictDetail = string(run.Verdict), run.VerdictDetail
	res, err := c.st.GetResult(ctx, id)
	switch {
	case errors.Is(err, store.ErrNotFound):
		return v, nil
	case err != nil:
		return RunView{}, err
	}
	rv := &ResultView{
		Verdict: string(run.Verdict), ExpectedSnapshots: res.ExpectedSnapshots, ReceivedSnapshots: res.ReceivedSnapshots,
		RejectedSnapshots: res.RejectedSnapshots, Intended: res.Intended, OK: res.OK, Err: res.Err, Timeout: res.Timeout,
		Dropped: res.Dropped, Unfinished: res.Unfinished, Clamped: res.Clamped,
		P50US: res.P50, P90US: res.P90, P99US: res.P99, P999US: res.P999, MaxUS: res.Max, LagP99US: res.LagP99,
	}
	if run.Verdict == health.Invalid {
		v.Diagnostics = rv
	} else {
		v.Results = rv
	}
	return v, nil
}

// LiveView is GET /runs/{id}/live: always provisional, never a verdict.
type LiveView struct {
	ID          string            `json:"id"`
	State       string            `json:"state"`
	Provisional bool              `json:"provisional"`
	Verdict     string            `json:"verdict"`
	Source      string            `json:"source"` // redis, postgres or none
	Progress    map[string]string `json:"progress,omitempty"`
	Final       string            `json:"final,omitempty"` // where the verdict is, once there is one
}

func (c *Controller) handleLive(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id := r.PathValue("id")
	run, err := c.st.GetRun(ctx, id)
	if err != nil {
		c.fail(w, err)
		return
	}
	v := LiveView{ID: id, State: string(run.State), Provisional: true, Verdict: aggregator.PendingVerdict, Source: "none"}
	if run.State.Terminal() {
		v.Final = "/runs/" + id
	}
	if c.live != nil {
		m, ok, err := c.live.Progress(ctx, id)
		if err != nil {
			log.Printf("controlplane: %v; falling back to Postgres", err)
		} else if ok {
			v.Source, v.Progress = "redis", m
		}
	}
	if v.Progress == nil {
		p, ok, err := c.st.RunProgress(ctx, id)
		if err != nil {
			c.fail(w, err)
			return
		}
		if ok {
			v.Source, v.Progress = "postgres", progressFields(p)
		}
	}
	// Whatever the cache holds, the verdict is only ever read from Postgres.
	if v.Progress != nil {
		v.Progress["provisional"], v.Progress["verdict"] = "true", aggregator.PendingVerdict
	}
	writeJSON(w, http.StatusOK, v)
}

func progressFields(p store.Progress) map[string]string {
	f := aggregator.LiveProgress{Progress: p}.Fields()
	delete(f, "updated_at_unixms") // a Postgres read has no cache timestamp
	delete(f, "finalized")
	return f
}

func (c *Controller) fail(w http.ResponseWriter, err error) {
	var re *RequestError
	switch {
	case errors.As(err, &re):
		writeError(w, re.Status, re.Msg)
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, store.ErrStateConflict):
		writeError(w, http.StatusConflict, err.Error())
	case errors.Is(err, store.ErrExists):
		writeError(w, http.StatusConflict, err.Error())
	default:
		log.Printf("controlplane: api: %v", err)
		writeError(w, http.StatusServiceUnavailable, err.Error())
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg, "status": strconv.Itoa(status)})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("controlplane: api: encode: %v", err)
	}
}
