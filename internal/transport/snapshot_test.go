package transport

import (
	"strings"
	"testing"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/loadforge/loadforge/internal/engine"
)

func histOf(us ...int64) *hdrhistogram.Histogram {
	h := engine.NewHistogram()
	for _, v := range us {
		_ = h.RecordValue(v)
	}
	return h
}

// validSnapshot has 5 intended: 2 ok, 1 error, 1 timeout and 1 dropped.
func validSnapshot() *Snapshot {
	start := time.Date(2026, 11, 4, 12, 0, 0, 123456789, time.UTC)
	return &Snapshot{
		RunID: "run-1", WorkerID: "w_7", Seq: 42,
		IntervalStart: start, IntervalEnd: start.Add(time.Second),
		Latency:     histOf(900, 2_000_000, 1_500, 60_000_000),
		SendDelay:   histOf(10, 20, 30, 40),
		DispatchLag: histOf(1, 2, 3, 4, 5),
		Intended:    5, OK: 2, Errors: 1, Timeouts: 1, Dropped: 1, Clamped: 1,
	}
}

func roundTrip(t *testing.T, s *Snapshot) *Snapshot {
	t.Helper()
	b, err := EncodeSnapshot(s)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := DecodeSnapshot(b)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	return got
}

func TestSnapshotRoundTripKeepsEveryField(t *testing.T) {
	for _, c := range []struct {
		name string
		mut  func(*Snapshot)
	}{
		{"interval", func(*Snapshot) {}},
		{"final", func(s *Snapshot) { s.Final, s.Reason = true, ReasonDeadman }},
		{"empty", func(s *Snapshot) {
			s.Latency, s.SendDelay, s.DispatchLag = histOf(), histOf(), histOf()
			s.Intended, s.OK, s.Errors, s.Timeouts, s.Dropped, s.Clamped = 0, 0, 0, 0, 0, 0
			s.Unfinished = 3 // cut off while queued: counted, no latency
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			want := validSnapshot()
			c.mut(want)
			got := roundTrip(t, want)

			for name, h := range map[string][2]*hdrhistogram.Histogram{
				"latency": {got.Latency, want.Latency}, "send_delay": {got.SendDelay, want.SendDelay},
				"dispatch_lag": {got.DispatchLag, want.DispatchLag},
			} {
				if !h[0].Equals(h[1]) {
					t.Errorf("%s histogram changed in transit", name)
				}
			}
			got.Latency, got.SendDelay, got.DispatchLag = nil, nil, nil
			want.Latency, want.SendDelay, want.DispatchLag = nil, nil, nil
			if !got.IntervalStart.Equal(want.IntervalStart) || !got.IntervalEnd.Equal(want.IntervalEnd) {
				t.Errorf("interval = [%v, %v], want [%v, %v]", got.IntervalStart, got.IntervalEnd, want.IntervalStart, want.IntervalEnd)
			}
			got.IntervalStart, got.IntervalEnd = want.IntervalStart, want.IntervalEnd
			if *got != *want {
				t.Errorf("decoded %+v\nwant    %+v", *got, *want)
			}
		})
	}
}

func TestSnapshotDecodeSkipsUnknownFields(t *testing.T) {
	b, err := EncodeSnapshot(validSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	for range 2 { // repeated unknown fields are allowed too
		b = protowire.AppendTag(b, 99, protowire.BytesType)
		b = protowire.AppendBytes(b, []byte("from the future"))
	}
	if _, err := DecodeSnapshot(b); err != nil {
		t.Errorf("unknown field rejected: %v", err)
	}
}

func TestSnapshotDecodeRejectsBadInput(t *testing.T) {
	good, err := EncodeSnapshot(validSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	withField := func(n protowire.Number, v []byte) []byte {
		b := append([]byte(nil), good...)
		b = protowire.AppendTag(b, n, protowire.BytesType)
		return protowire.AppendBytes(b, v)
	}
	// replaceField swaps the value of a bytes field in place, so decoding
	// reaches the value rather than failing on a repeated field.
	replaceField := func(n protowire.Number, v []byte) []byte {
		var out []byte
		for b := good; len(b) > 0; {
			num, typ, tn := protowire.ConsumeTag(b)
			fn := protowire.ConsumeFieldValue(num, typ, b[tn:])
			if num == n {
				out = protowire.AppendTag(out, n, protowire.BytesType)
				out = protowire.AppendBytes(out, v)
			} else {
				out = append(out, b[:tn+fn]...)
			}
			b = b[tn+fn:]
		}
		return out
	}
	// Rewrites the version, which Encode always writes first.
	withVersion := func(v uint64) []byte {
		_, _, n := protowire.ConsumeTag(good)
		_, m := protowire.ConsumeVarint(good[n:])
		b := protowire.AppendTag(nil, fVersion, protowire.VarintType)
		b = protowire.AppendVarint(b, v)
		return append(b, good[n+m:]...)
	}
	wideHist, _ := hdrhistogram.New(1, 3_600_000_000, 3).Encode(hdrhistogram.V2CompressedEncodingCookieBase)

	for name, b := range map[string][]byte{
		"empty":                    nil,
		"truncated":                good[:len(good)/2],
		"unknown version":          withVersion(2),
		"no version":               withVersion(0),
		"repeated known field":     withField(fRunID, []byte("run-2")),
		"short histogram":          replaceField(fLatencyHist, []byte("AAAAAAAA")), // panics in hdrhistogram.Decode
		"garbage histogram":        replaceField(fLatencyHist, []byte(strings.Repeat("QUJD", 20))),
		"histogram of wrong range": replaceField(fLatencyHist, wideHist),
	} {
		_, err := DecodeSnapshot(b)
		t.Logf("%s: %v", name, err)
		if err == nil {
			t.Errorf("%s: decoded without error", name)
		}
	}
}

func TestSnapshotValidateRejectsInconsistentData(t *testing.T) {
	for name, mut := range map[string]func(*Snapshot){
		"bad run id":           func(s *Snapshot) { s.RunID = "run.1" },
		"colon in worker id":   func(s *Snapshot) { s.WorkerID = "a:b" },
		"end before start":     func(s *Snapshot) { s.IntervalEnd = s.IntervalStart.Add(-time.Nanosecond) },
		"latency count":        func(s *Snapshot) { s.OK++ },
		"lag count":            func(s *Snapshot) { s.Intended++ },
		"send delay count":     func(s *Snapshot) { _ = s.SendDelay.RecordValue(5) },
		"negative count":       func(s *Snapshot) { s.Unfinished = -1 },
		"missing histogram":    func(s *Snapshot) { s.SendDelay = nil },
		"wrong precision":      func(s *Snapshot) { s.DispatchLag = hdrhistogram.New(1, 60_000_000, 2) },
		"final without reason": func(s *Snapshot) { s.Final = true },
		"reason without final": func(s *Snapshot) { s.Reason = ReasonStop },
		"unknown reason":       func(s *Snapshot) { s.Final, s.Reason = true, "bored" },
	} {
		s := validSnapshot()
		mut(s)
		if _, err := EncodeSnapshot(s); err == nil {
			t.Errorf("%s: encoded without error", name)
		}
	}
}

func TestSnapshotKey(t *testing.T) {
	if got := string(SnapshotKey("run-1", "w_7")); got != "run-1:w_7" {
		t.Errorf("key = %q, want %q", got, "run-1:w_7")
	}
}
