//go:build integration && unix

package controlplane

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/loadforge/loadforge/internal/health"
	"github.com/loadforge/loadforge/internal/runstate"
	"github.com/loadforge/loadforge/internal/safety"
	"github.com/loadforge/loadforge/internal/store"
	"github.com/loadforge/loadforge/internal/timingtest"
	"github.com/loadforge/loadforge/internal/transport"
)

var (
	buildOnce sync.Once
	binDir    string
	binPath   string
	buildErr  error
)

func TestMain(m *testing.M) {
	code := m.Run()
	if binDir != "" {
		_ = os.RemoveAll(binDir)
	}
	os.Exit(code)
}

// controlplaneBinary builds cmd/controlplane once per test process.
func controlplaneBinary(t *testing.T) string {
	t.Helper()
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "loadforge-cp-bin-")
		if err != nil {
			buildErr = err
			return
		}
		binDir = dir
		binPath = filepath.Join(dir, "controlplane")
		out, err := exec.Command("go", "build", "-o", binPath, "github.com/loadforge/loadforge/cmd/controlplane").CombinedOutput()
		if err != nil {
			buildErr = fmt.Errorf("go build: %w\n%s", err, out)
		}
	})
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	return binPath
}

type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// process is a controlplane running as its own OS process.
type process struct {
	cmd *exec.Cmd
	url string
}

func (e *itEnv) startProcess(t *testing.T) *process {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	lim := itConfig().Limits
	// The binary is the one this test process just built from the repo.
	cmd := exec.Command(controlplaneBinary(t), //nolint:gosec // G204: not tainted input

		"-id", "proc-"+e.prefix, "-listen", addr, "-pg", e.dsn, "-redis", env("LF_REDIS", "localhost:6379"), "-nats", e.natsURL,
		"-max-rate-per-worker", fmt.Sprint(float64(lim.MaxRatePerWorker)), "-max-total-rate", fmt.Sprint(lim.MaxTotalRate),
		"-max-duration", lim.MaxDuration.String(), "-max-workers", fmt.Sprint(lim.MaxWorkers))
	logs := &syncBuf{}
	cmd.Stdout, cmd.Stderr = logs, logs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &process{cmd: cmd, url: "http://" + addr}
	t.Cleanup(func() {
		_ = cmd.Process.Signal(syscall.SIGCONT)
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		if t.Failed() {
			t.Logf("controlplane process output:\n%s", logs.String())
		}
	})
	until(t, 30*time.Second, "the controlplane process to serve", func() bool {
		resp, err := http.Get(p.url + "/runs/none")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusNotFound
	})
	return p
}

// startRunOn creates a 60s run through a process's API once it has seen
// the workers' heartbeats, and waits until it is RUNNING.
func (e *itEnv) startRunOn(t *testing.T, p *process, ids []string) (runID string, fence uint64) {
	t.Helper()
	var created struct{ ID string }
	until(t, 15*time.Second, "the process to accept the run", func() bool {
		return doJSON(t, "POST", p.url+"/runs", e.request(ids, func(r *RunRequest) { r.DurationMS = 60_000 }), &created) == http.StatusAccepted
	})
	until(t, 20*time.Second, "RUNNING", func() bool { return e.state(created.ID) == runstate.Running })
	r, err := e.st.GetRun(context.Background(), created.ID)
	if err != nil {
		t.Fatal(err)
	}
	return created.ID, r.FenceToken
}

// waitDeadman waits for every worker's final snapshot and checks it ended
// on the dead-man's switch while the old lease still existed, so no second
// controller could have been driving load at the same time.
func (e *itEnv) waitDeadman(t *testing.T, runID string, ids []string, since time.Time) {
	t.Helper()
	for _, w := range ids {
		var f *transport.Snapshot
		var at time.Time
		until(t, safety.LeaseTTL, "final snapshot from "+w, func() bool { f, at = e.pubs[w].final(runID); return f != nil })
		if f.Reason != transport.ReasonDeadman {
			t.Errorf("%s final reason %s, want deadman", w, f.Reason)
		}
		if el := at.Sub(since); el > safety.LeaseTTL-safety.LeaseRenewInterval {
			t.Errorf("%s stopped %v after the controller went away; the lease may already have expired", w, el)
		}
	}
	if ttl := e.rdb.PTTL(context.Background(), store.RunLeaseKey(runID)).Val(); ttl <= 0 {
		t.Errorf("old lease gone (PTTL %v) before every worker had stopped", ttl)
	}
}

func (e *itEnv) checkRecovered(t *testing.T, runID string, oldFence uint64) store.Run {
	t.Helper()
	run, err := e.st.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.State != runstate.Aborted || run.Verdict != health.Invalid || run.FenceToken <= oldFence {
		t.Fatalf("run %s: state %s verdict %s fence %d (old %d)", runID, run.State, run.Verdict, run.FenceToken, oldFence)
	}
	evs, _ := e.st.Events(context.Background(), runID)
	recovered := -1
	for i, ev := range evs {
		if ev.Kind == store.EventRecovered {
			recovered = i
		}
	}
	if recovered < 0 {
		t.Fatal("no recovered event")
	}
	for _, ev := range evs[recovered:] {
		if ev.FenceToken != run.FenceToken {
			t.Errorf("event %s after recovery carries fence %d, want %d", ev.Kind, ev.FenceToken, run.FenceToken)
		}
	}
	ws, _ := e.st.RunWorkers(context.Background(), runID)
	for _, w := range ws {
		if w.FinalSeq == nil {
			t.Errorf("%s: recovery STOP did not record final_seq", w.WorkerID)
		}
	}
	if !run.FinalizeRequested {
		t.Error("finalize not requested")
	}
	return run
}

// TestIntegrationControllerCrash SIGKILLs the controller process mid-run.
// Workers stop on their dead-man's switch before its lease can expire;
// a second controller then recovers the run under a higher fence and
// records ABORTED / INVALID, and the aggregator still finalizes it.
func TestIntegrationControllerCrash(t *testing.T) {
	// Asserts that workers stop before the lease expires, and builds and runs
	// a binary under real load: never alongside another timing test.
	timingtest.Exclusive(t)
	controlplaneBinary(t)
	e := newEnv(t)
	ids := e.addWorkers(2)
	a := e.startProcess(t)
	runID, fenceA := e.startRunOn(t, a, ids)
	time.Sleep(2 * time.Second)

	killedAt := time.Now()
	if err := a.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	e.waitDeadman(t, runID, ids, killedAt)

	b := e.controller()
	until(t, 2*safety.LeaseTTL+time.Minute, "recovery", func() bool { return e.state(runID).Terminal() })
	e.checkRecovered(t, runID, fenceA)

	e.waitResult(runID, 60*time.Second)
	v, err := b.View(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	if v.Results != nil || v.Diagnostics == nil || v.Diagnostics.Verdict != string(health.Invalid) {
		t.Errorf("results %+v diagnostics %+v", v.Results, v.Diagnostics)
	}
}

// TestIntegrationPausedControllerFenced SIGSTOPs the controller process
// mid-run. A second controller recovers the run; when the first resumes,
// every write it tries is rejected and it sends no command.
func TestIntegrationPausedControllerFenced(t *testing.T) {
	// Asserts that workers stop before the lease expires, and builds and runs
	// a binary under real load: never alongside another timing test.
	timingtest.Exclusive(t)
	controlplaneBinary(t)
	e := newEnv(t)
	ids := e.addWorkers(2)
	a := e.startProcess(t)
	runID, fenceA := e.startRunOn(t, a, ids)

	var mu sync.Mutex
	var cmds []transport.Command
	nc := e.connect()
	sub, err := nc.Subscribe(transport.CmdSubject(runID), func(m *nats.Msg) {
		var c transport.Command
		if json.Unmarshal(m.Data, &c) == nil {
			mu.Lock()
			cmds = append(cmds, c)
			mu.Unlock()
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	time.Sleep(2 * time.Second)

	pausedAt := time.Now()
	if err := a.cmd.Process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	e.waitDeadman(t, runID, ids, pausedAt)
	e.controller()
	until(t, 2*safety.LeaseTTL+time.Minute, "recovery", func() bool { return e.state(runID).Terminal() })
	run := e.checkRecovered(t, runID, fenceA)

	mu.Lock()
	before := len(cmds)
	mu.Unlock()
	if err := a.cmd.Process.Signal(syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * safety.LeaseRenewInterval) // A notices: renewal refused, writes stale

	after, _ := e.st.GetRun(context.Background(), runID)
	if after.FenceToken != run.FenceToken || after.State != runstate.Aborted {
		t.Errorf("run changed after A resumed: %+v", after)
	}
	evs, _ := e.st.Events(context.Background(), runID)
	if last := evs[len(evs)-1]; last.FenceToken != run.FenceToken {
		t.Errorf("A wrote %s with fence %d after resuming", last.Kind, last.FenceToken)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, c := range cmds[before:] {
		if c.FenceToken == fenceA {
			t.Errorf("A sent %s seq %d after resuming", c.Type, c.CmdSeq)
		}
	}
}
