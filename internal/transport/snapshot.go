package transport

import (
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/loadforge/loadforge/internal/engine"
)

// SnapshotTopic carries one snapshot per worker per second (design.md §4.3).
const SnapshotTopic = "loadforge.hdr.snapshots"

// SnapshotVersion is the payload version written by Encode. Decode rejects
// any other.
const SnapshotVersion = 1

// StopReason says why a worker's stream ended. It is set only on the final
// snapshot.
type StopReason string

const (
	ReasonCompleted StopReason = "completed" // the run reached its duration
	ReasonStop      StopReason = "stop"      // STOP command
	ReasonDeadman   StopReason = "deadman"   // no controller heartbeat (§6.3)
	ReasonShutdown  StopReason = "shutdown"  // the worker process is exiting
)

func (r StopReason) valid() bool {
	switch r {
	case ReasonCompleted, ReasonStop, ReasonDeadman, ReasonShutdown:
		return true
	}
	return false
}

// Snapshot is one worker's recording interval. Histograms are in
// microseconds over the standard range (engine.NewHistogram).
//
// IntervalStart and IntervalEnd are wall-clock labels for display and
// alignment only; durations are never computed from them.
type Snapshot struct {
	RunID    string
	WorkerID string
	Seq      uint64 // contiguous from 0 for each (RunID, WorkerID)

	IntervalStart, IntervalEnd time.Time

	Latency     *hdrhistogram.Histogram // completion − intended send time
	SendDelay   *hdrhistogram.Histogram // execution start − intended send time
	DispatchLag *hdrhistogram.Histogram // release − intended send time

	Intended   int64
	OK         int64
	Errors     int64
	Timeouts   int64
	Dropped    int64 // generator_dropped
	Unfinished int64
	Clamped    int64

	Final  bool
	Reason StopReason // set if and only if Final
}

// SnapshotKey is the Kafka message key, so that each worker's snapshots go
// to one partition in order.
func SnapshotKey(runID, workerID string) []byte {
	return []byte(runID + ":" + workerID)
}

// Validate checks the invariants every snapshot must satisfy.
func (s *Snapshot) Validate() error {
	if err := ValidateID("run_id", s.RunID); err != nil {
		return err
	}
	if err := ValidateID("worker_id", s.WorkerID); err != nil {
		return err
	}
	// Compare what goes on the wire, not monotonic readings, so that the
	// decoder accepts everything the encoder does.
	if s.IntervalStart.IsZero() || s.IntervalEnd.UnixNano() < s.IntervalStart.UnixNano() {
		return fmt.Errorf("transport: snapshot interval [%v, %v] is invalid", s.IntervalStart, s.IntervalEnd)
	}
	for _, h := range []struct {
		name string
		h    *hdrhistogram.Histogram
	}{{"latency", s.Latency}, {"send_delay", s.SendDelay}, {"dispatch_lag", s.DispatchLag}} {
		if err := checkRange(h.name, h.h); err != nil {
			return err
		}
	}
	for _, c := range []int64{s.Intended, s.OK, s.Errors, s.Timeouts, s.Dropped, s.Unfinished, s.Clamped} {
		if c < 0 {
			return errors.New("transport: snapshot has a negative count")
		}
	}
	// The engine records each value together with its counter, under one
	// lock, so these hold for every interval.
	if completed := s.OK + s.Errors + s.Timeouts; s.Latency.TotalCount() != completed {
		return fmt.Errorf("transport: latency histogram has %d values for %d completed requests", s.Latency.TotalCount(), completed)
	}
	if s.SendDelay.TotalCount() != s.Latency.TotalCount() {
		return fmt.Errorf("transport: send-delay histogram has %d values, latency has %d", s.SendDelay.TotalCount(), s.Latency.TotalCount())
	}
	if s.DispatchLag.TotalCount() != s.Intended {
		return fmt.Errorf("transport: lag histogram has %d values for %d intended requests", s.DispatchLag.TotalCount(), s.Intended)
	}
	if s.Final != (s.Reason != "") {
		return errors.New("transport: a reason must be set on the final snapshot and only there")
	}
	if s.Final && !s.Reason.valid() {
		return fmt.Errorf("transport: unknown stop reason %q", s.Reason)
	}
	return nil
}

var stdHist = engine.NewHistogram()

func checkRange(name string, h *hdrhistogram.Histogram) error {
	if h == nil {
		return fmt.Errorf("transport: %s histogram is missing", name)
	}
	if h.LowestTrackableValue() != stdHist.LowestTrackableValue() ||
		h.HighestTrackableValue() != stdHist.HighestTrackableValue() ||
		h.SignificantFigures() != stdHist.SignificantFigures() {
		return fmt.Errorf("transport: %s histogram covers [%d, %d] at %d digits, want [%d, %d] at %d",
			name, h.LowestTrackableValue(), h.HighestTrackableValue(), h.SignificantFigures(),
			stdHist.LowestTrackableValue(), stdHist.HighestTrackableValue(), stdHist.SignificantFigures())
	}
	return nil
}

// Protobuf field numbers. The payload is hand-encoded with protowire, so
// this list is the schema; numbers are never reused. Timestamps and counts
// are sint64 (zigzag); version, seq and final are uint64.
const (
	fVersion       protowire.Number = 1
	fRunID         protowire.Number = 2
	fWorkerID      protowire.Number = 3
	fSeq           protowire.Number = 4
	fIntervalStart protowire.Number = 5 // unix nanoseconds
	fIntervalEnd   protowire.Number = 6
	fLatencyHist   protowire.Number = 7 // HDR V2 compressed, base64
	fLagHist       protowire.Number = 8
	fSendDelayHist protowire.Number = 9
	fOK            protowire.Number = 10
	fErr           protowire.Number = 11
	fTimeout       protowire.Number = 12
	fDropped       protowire.Number = 13
	fIntended      protowire.Number = 14
	fUnfinished    protowire.Number = 15
	fClamped       protowire.Number = 16
	fFinal         protowire.Number = 17
	fReason        protowire.Number = 18
)

// EncodeSnapshot validates s and serialises it.
func EncodeSnapshot(s *Snapshot) ([]byte, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	var b []byte
	varint := func(n protowire.Number, v uint64) {
		b = protowire.AppendTag(b, n, protowire.VarintType)
		b = protowire.AppendVarint(b, v)
	}
	sint := func(n protowire.Number, v int64) { varint(n, protowire.EncodeZigZag(v)) }
	bytes := func(n protowire.Number, v []byte) {
		b = protowire.AppendTag(b, n, protowire.BytesType)
		b = protowire.AppendBytes(b, v)
	}
	varint(fVersion, SnapshotVersion)
	bytes(fRunID, []byte(s.RunID))
	bytes(fWorkerID, []byte(s.WorkerID))
	varint(fSeq, s.Seq)
	sint(fIntervalStart, s.IntervalStart.UnixNano())
	sint(fIntervalEnd, s.IntervalEnd.UnixNano())
	for _, h := range []struct {
		n protowire.Number
		h *hdrhistogram.Histogram
	}{{fLatencyHist, s.Latency}, {fLagHist, s.DispatchLag}, {fSendDelayHist, s.SendDelay}} {
		enc, err := h.h.Encode(hdrhistogram.V2CompressedEncodingCookieBase)
		if err != nil {
			return nil, fmt.Errorf("transport: encode histogram: %w", err)
		}
		bytes(h.n, enc)
	}
	sint(fOK, s.OK)
	sint(fErr, s.Errors)
	sint(fTimeout, s.Timeouts)
	sint(fDropped, s.Dropped)
	sint(fIntended, s.Intended)
	sint(fUnfinished, s.Unfinished)
	sint(fClamped, s.Clamped)
	if s.Final {
		varint(fFinal, 1)
		bytes(fReason, []byte(s.Reason))
	}
	return b, nil
}

// DecodeSnapshot parses and validates a payload written by EncodeSnapshot.
// Unknown fields are skipped, so fields can be added without a version bump.
func DecodeSnapshot(b []byte) (*Snapshot, error) {
	var (
		s       Snapshot
		version uint64
		start   int64
		end     int64
		seen    = map[protowire.Number]bool{}
	)
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return nil, fmt.Errorf("transport: snapshot: %w", protowire.ParseError(n))
		}
		b = b[n:]
		// Known fields are singular. Unknown ones may be repeated in a
		// later version.
		if seen[num] && num >= fVersion && num <= fReason {
			return nil, fmt.Errorf("transport: snapshot: field %d repeated", num)
		}
		seen[num] = true

		var (
			v   uint64
			raw []byte
		)
		switch typ {
		case protowire.VarintType:
			v, n = protowire.ConsumeVarint(b)
		case protowire.BytesType:
			raw, n = protowire.ConsumeBytes(b)
		default:
			n = protowire.ConsumeFieldValue(num, typ, b)
		}
		if n < 0 {
			return nil, fmt.Errorf("transport: snapshot field %d: %w", num, protowire.ParseError(n))
		}
		b = b[n:]

		var err error
		switch num {
		case fVersion:
			version, err = v, wantType(num, typ, protowire.VarintType)
		case fRunID:
			s.RunID, err = string(raw), wantType(num, typ, protowire.BytesType)
		case fWorkerID:
			s.WorkerID, err = string(raw), wantType(num, typ, protowire.BytesType)
		case fSeq:
			s.Seq, err = v, wantType(num, typ, protowire.VarintType)
		case fIntervalStart:
			start, err = sint64(num, typ, v)
		case fIntervalEnd:
			end, err = sint64(num, typ, v)
		case fLatencyHist:
			s.Latency, err = decodeHist(num, typ, raw)
		case fLagHist:
			s.DispatchLag, err = decodeHist(num, typ, raw)
		case fSendDelayHist:
			s.SendDelay, err = decodeHist(num, typ, raw)
		case fOK:
			s.OK, err = sint64(num, typ, v)
		case fErr:
			s.Errors, err = sint64(num, typ, v)
		case fTimeout:
			s.Timeouts, err = sint64(num, typ, v)
		case fDropped:
			s.Dropped, err = sint64(num, typ, v)
		case fIntended:
			s.Intended, err = sint64(num, typ, v)
		case fUnfinished:
			s.Unfinished, err = sint64(num, typ, v)
		case fClamped:
			s.Clamped, err = sint64(num, typ, v)
		case fFinal:
			s.Final, err = v == 1, wantType(num, typ, protowire.VarintType)
		case fReason:
			s.Reason, err = StopReason(raw), wantType(num, typ, protowire.BytesType)
		}
		if err != nil {
			return nil, err
		}
	}
	if version != SnapshotVersion {
		return nil, fmt.Errorf("transport: snapshot version %d, want %d", version, SnapshotVersion)
	}
	if !seen[fIntervalStart] || !seen[fIntervalEnd] {
		return nil, errors.New("transport: snapshot interval is missing")
	}
	s.IntervalStart, s.IntervalEnd = time.Unix(0, start), time.Unix(0, end)
	if err := s.Validate(); err != nil {
		return nil, err
	}
	return &s, nil
}

func wantType(num protowire.Number, got, want protowire.Type) error {
	if got != want {
		return fmt.Errorf("transport: snapshot field %d has wire type %d, want %d", num, got, want)
	}
	return nil
}

func sint64(num protowire.Number, typ protowire.Type, v uint64) (int64, error) {
	if err := wantType(num, typ, protowire.VarintType); err != nil {
		return 0, err
	}
	return protowire.DecodeZigZag(v), nil
}

// decodeHist guards hdrhistogram.Decode, which panics on short input.
func decodeHist(num protowire.Number, typ protowire.Type, raw []byte) (h *hdrhistogram.Histogram, err error) {
	if err := wantType(num, typ, protowire.BytesType); err != nil {
		return nil, err
	}
	if base64.StdEncoding.DecodedLen(len(raw)) < 8 {
		return nil, fmt.Errorf("transport: snapshot field %d: histogram too short", num)
	}
	defer func() {
		if r := recover(); r != nil {
			h, err = nil, fmt.Errorf("transport: snapshot field %d: corrupt histogram: %v", num, r)
		}
	}()
	h, err = hdrhistogram.Decode(raw)
	if err != nil {
		return nil, fmt.Errorf("transport: snapshot field %d: %w", num, err)
	}
	return h, nil
}
