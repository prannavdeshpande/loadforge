package controlplane

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/loadforge/loadforge/internal/engine"
	"github.com/loadforge/loadforge/internal/health"
	"github.com/loadforge/loadforge/internal/transport"
)

// RunRequest is the body of POST /runs. RatePerWorker is each worker's
// rate in requests per second; the run's total is RatePerWorker ×
// len(Workers).
type RunRequest struct {
	ID               string          `json:"id,omitempty"`
	Workers          []string        `json:"workers"`
	Method           string          `json:"method"`
	TargetURL        string          `json:"target_url"`
	RatePerWorker    float64         `json:"rate_per_worker"`
	DurationMS       int64           `json:"duration_ms"`
	MaxInFlight      int             `json:"max_in_flight"`
	QueueDepth       int             `json:"queue_depth"`
	RequestTimeoutMS int64           `json:"request_timeout_ms"`
	DrainTimeoutMS   int64           `json:"drain_timeout_ms"`
	Thresholds       json.RawMessage `json:"thresholds,omitempty"`
}

// RequestError is a refused request, with the HTTP status it maps to.
type RequestError struct {
	Status int
	Msg    string
}

func (e *RequestError) Error() string { return e.Msg }

func refuse(status int, format string, args ...any) *RequestError {
	return &RequestError{Status: status, Msg: fmt.Sprintf(format, args...)}
}

// validated is a request that passed validation.
type validated struct {
	id         string
	prep       transport.Prepare
	thresholds json.RawMessage
}

// validate checks a request's shape (400), the safety limits (422) and
// that every worker is alive and idle (409). It contacts nothing but the
// worker registry.
func (c *Controller) validate(req RunRequest) (validated, error) {
	id := req.ID
	if id == "" {
		var b [8]byte
		if _, err := rand.Read(b[:]); err != nil {
			return validated{}, err
		}
		id = "run-" + hex.EncodeToString(b[:])
	}
	prep := transport.Prepare{
		Workers: req.Workers, Method: req.Method, TargetURL: req.TargetURL,
		Rate: req.RatePerWorker, DurationMS: req.DurationMS, MaxInFlight: req.MaxInFlight, QueueDepth: req.QueueDepth,
		RequestTimeoutMS: req.RequestTimeoutMS, DrainTimeoutMS: req.DrainTimeoutMS,
	}
	// The PREPARE the run will send must itself be valid.
	probe := transport.Command{Type: transport.CmdPrepare, RunID: id, CmdSeq: 1, FenceToken: 1, Deadline: time.Now(), Prepare: &prep}
	if err := probe.Validate(); err != nil {
		return validated{}, refuse(http.StatusBadRequest, "%v", err)
	}
	ecfg := engine.Config{
		Rate: prep.Rate, Duration: time.Duration(prep.DurationMS) * time.Millisecond,
		MaxInFlight: prep.MaxInFlight, QueueDepth: prep.QueueDepth,
		RequestTimeout: time.Duration(prep.RequestTimeoutMS) * time.Millisecond,
		DrainTimeout:   time.Duration(prep.DrainTimeoutMS) * time.Millisecond,
	}
	if err := ecfg.Validate(); err != nil {
		return validated{}, refuse(http.StatusBadRequest, "%v", err)
	}
	th := req.Thresholds
	if len(th) == 0 || string(th) == "null" {
		th = json.RawMessage(`{}`)
	}
	if _, err := health.ParseThresholds(th); err != nil {
		return validated{}, refuse(http.StatusBadRequest, "thresholds: %v", err)
	}

	lim := c.cfg.Limits
	if err := lim.MaxRatePerWorker.Check(prep.Rate); err != nil {
		return validated{}, refuse(http.StatusUnprocessableEntity, "%v", err)
	}
	if total := prep.Rate * float64(len(prep.Workers)); total > lim.MaxTotalRate {
		return validated{}, refuse(http.StatusUnprocessableEntity, "total rate %v req/s exceeds the limit of %v req/s", total, lim.MaxTotalRate)
	}
	if d := ecfg.Duration; d > lim.MaxDuration {
		return validated{}, refuse(http.StatusUnprocessableEntity, "duration %v exceeds the limit of %v", d, lim.MaxDuration)
	}
	if n := len(prep.Workers); n > lim.MaxWorkers {
		return validated{}, refuse(http.StatusUnprocessableEntity, "%d workers exceeds the limit of %d", n, lim.MaxWorkers)
	}

	for _, w := range prep.Workers {
		at, runID, ok := c.reg.Last(w)
		switch {
		case !ok:
			return validated{}, refuse(http.StatusConflict, "worker %s has never sent a heartbeat", w)
		case time.Since(at) >= c.cfg.WorkerSilence:
			return validated{}, refuse(http.StatusConflict, "worker %s last sent a heartbeat %v ago", w, time.Since(at).Round(time.Millisecond))
		case runID != "":
			return validated{}, refuse(http.StatusConflict, "worker %s is busy with run %s", w, runID)
		}
	}
	return validated{id: id, prep: prep, thresholds: th}, nil
}
