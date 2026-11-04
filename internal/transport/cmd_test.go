package transport

import (
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func validPrepare() Command {
	return Command{
		Type: CmdPrepare, RunID: "run-1", CmdSeq: 1, FenceToken: 7,
		Deadline: time.Date(2026, 11, 4, 12, 0, 5, 0, time.UTC),
		Prepare: &Prepare{
			Workers: []string{"w1", "w2"},
			Method:  "GET", TargetURL: "http://target:8080/x",
			Rate: 500, DurationMS: 60_000, MaxInFlight: 100, QueueDepth: 1000,
			RequestTimeoutMS: 5000, DrainTimeoutMS: 10_000,
		},
	}
}

func TestCommandJSONRoundTrip(t *testing.T) {
	start := validPrepare()
	start.Type, start.CmdSeq, start.Prepare = CmdStart, 2, nil
	start.Start = &Start{StartAt: time.Date(2026, 11, 4, 12, 0, 7, 0, time.UTC)}
	stop := validPrepare()
	stop.Type, stop.CmdSeq, stop.Prepare = CmdStop, 3, nil

	for _, want := range []Command{validPrepare(), start, stop} {
		b, err := json.Marshal(want)
		if err != nil {
			t.Fatal(err)
		}
		var got Command
		if err := json.Unmarshal(b, &got); err != nil {
			t.Fatal(err)
		}
		if err := got.Validate(); err != nil {
			t.Errorf("%s: valid command rejected after round trip: %v", want.Type, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s: got %+v, want %+v", want.Type, got, want)
		}
	}
}

func TestCommandValidateRejects(t *testing.T) {
	for name, mut := range map[string]func(*Command){
		"run id with dot":      func(c *Command) { c.RunID = "a.b" },
		"run id wildcard":      func(c *Command) { c.RunID = "*" },
		"empty run id":         func(c *Command) { c.RunID = "" },
		"no cmd_seq":           func(c *Command) { c.CmdSeq = 0 },
		"no fence":             func(c *Command) { c.FenceToken = 0 },
		"no deadline":          func(c *Command) { c.Deadline = time.Time{} },
		"unknown type":         func(c *Command) { c.Type = "ABORT" },
		"no payload":           func(c *Command) { c.Prepare = nil },
		"two payloads":         func(c *Command) { c.Start = &Start{StartAt: c.Deadline} },
		"stop with payload":    func(c *Command) { c.Type = CmdStop },
		"no workers":           func(c *Command) { c.Prepare.Workers = nil },
		"duplicate worker":     func(c *Command) { c.Prepare.Workers = []string{"w1", "w1"} },
		"bad worker id":        func(c *Command) { c.Prepare.Workers = []string{"w:1"} },
		"no method":            func(c *Command) { c.Prepare.Method = "" },
		"relative url":         func(c *Command) { c.Prepare.TargetURL = "/x" },
		"non-http url":         func(c *Command) { c.Prepare.TargetURL = "ftp://h/x" },
		"zero rate":            func(c *Command) { c.Prepare.Rate = 0 },
		"NaN rate":             func(c *Command) { c.Prepare.Rate = math.NaN() },
		"no duration":          func(c *Command) { c.Prepare.DurationMS = 0 },
		"no request timeout":   func(c *Command) { c.Prepare.RequestTimeoutMS = 0 },
		"start without at":     func(c *Command) { c.Type, c.Prepare, c.Start = CmdStart, nil, &Start{} },
		"rate without payload": func(c *Command) { c.Type, c.Prepare, c.Rate = CmdRate, nil, &Rate{Rate: 1} },
	} {
		c := validPrepare()
		mut(&c)
		if err := c.Validate(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestPrepareAckReportsUnmeasuredAsNull(t *testing.T) {
	b, err := json.Marshal(Ack{WorkerID: "w1", RunID: "run-1", CmdSeq: 1, Status: StatusReady})
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{`"clock_offset_ns":null`, `"capacity_rps":null`} {
		if !strings.Contains(string(b), field) {
			t.Errorf("ack %s lacks %s: an unmeasured signal must be explicit", b, field)
		}
	}
}

func TestSubjects(t *testing.T) {
	for got, want := range map[string]string{
		CmdSubject("r1"):             "lf.run.r1.cmd",
		RunHeartbeatSubject("r1"):    "lf.run.r1.hb",
		WorkerHeartbeatSubject("w1"): "lf.worker.w1.hb",
	} {
		if got != want {
			t.Errorf("subject %q, want %q", got, want)
		}
	}
}
