// Command calibrate measures a host's generator capacity and scheduler
// jitter baseline (design.md §9), and serves the stub target.
//
//	calibrate run -host-class c6i-2xl [-out profile.json] [-pg DSN] [-target URL]
//	calibrate serve -addr :8080 -latency 1ms
//	curl -X POST 'localhost:8080/_stall?d=2s'
//
// Run calibration on an otherwise idle host: anything else using its CPU
// lowers the measured capacity.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/loadforge/loadforge/internal/calibrate"
	"github.com/loadforge/loadforge/internal/store"
	"github.com/loadforge/loadforge/internal/stubserver"
)

const usage = `usage:
  calibrate run -host-class NAME [-out FILE] [-pg DSN] [-target URL] [flags]
  calibrate serve [-addr :8080] [-latency 1ms]`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "run":
		err = run(os.Args[2:])
	case "serve":
		err = serve(os.Args[2:])
	default:
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}
	if err != nil {
		log.Fatal(err)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	def := calibrate.DefaultRamp()
	hostClass := fs.String("host-class", "", "host class this profile describes, 1-64 characters of [A-Za-z0-9_-] (required)")
	out := fs.String("out", "", "write the profile to this JSON file (load it in a worker with -profile)")
	dsn := fs.String("pg", "", "also record the profile in Postgres (calibration_profiles)")
	target := fs.String("target", "", "calibrate against this http(s) URL instead of the in-process stub")
	method := fs.String("method", http.MethodGet, "HTTP method")
	startRate := fs.Float64("start-rate", def.StartRate, "first ramp rate, req/s")
	factor := fs.Float64("factor", def.Factor, "rate multiplier between ramp steps")
	maxRate := fs.Float64("max-rate", def.MaxRate, "highest ramp rate, req/s")
	step := fs.Duration("step", def.StepDuration, "how long each ramp step lasts")
	jitterRate := fs.Float64("jitter-rate", 100, "rate of the jitter baseline, req/s")
	jitterDur := fs.Duration("jitter-duration", 20*time.Second, "length of the jitter baseline")
	maxInFlight := fs.Int("max-in-flight", 1000, "concurrency cap, as in a run")
	queueDepth := fs.Int("queue-depth", 10_000, "local queue behind the cap, as in a run")
	reqTimeout := fs.Duration("request-timeout", 2*time.Second, "per-request timeout")
	pauseDur := fs.Duration("pause", 2*time.Second, "settling time between measurements")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *hostClass == "" || (*out == "" && *dsn == "") {
		fs.Usage()
		return errors.New("calibrate: -host-class and at least one of -out and -pg are required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Fail on an unreachable database before spending minutes measuring.
	var st *store.Store
	if *dsn != "" {
		var err error
		if st, err = store.Open(ctx, *dsn); err != nil {
			return err
		}
		defer st.Close()
		if err := st.Migrate(ctx); err != nil {
			return err
		}
	}

	label, targetURL := calibrate.InProcessTarget, *target
	if targetURL == "" {
		u, stopStub, err := calibrate.StartInProcessStub()
		if err != nil {
			return err
		}
		defer stopStub()
		targetURL = u
	} else if u, err := url.Parse(targetURL); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("calibrate: -target %q must be an absolute http(s) URL", targetURL)
	} else {
		label = targetURL
	}

	ramp := calibrate.RampConfig{StartRate: *startRate, Factor: *factor, MaxRate: *maxRate, StepDuration: *step, Limits: calibrate.DefaultLimits()}
	stepper := &calibrate.EngineStepper{URL: targetURL, Method: *method, MaxInFlight: *maxInFlight, QueueDepth: *queueDepth,
		RequestTimeout: *reqTimeout, Lead: 100 * time.Millisecond}
	log.Printf("calibrate: host class %s against %s: jitter baseline %v at %v req/s, then ramp %v→%v req/s ×%v, %v per step",
		*hostClass, label, *jitterDur, *jitterRate, *startRate, *maxRate, *factor, *step)
	p, err := calibrate.Calibrate(ctx, stepper, calibrate.Options{
		HostClass: *hostClass, Target: label, JitterRate: *jitterRate, JitterDuration: *jitterDur, Ramp: ramp, Pause: *pauseDur,
	})
	if err != nil {
		return err
	}
	for _, s := range p.Steps {
		verdict := "pass"
		if !s.Passed {
			verdict = "FAIL: " + s.Failed
		}
		log.Printf("calibrate: %8.0f req/s  lag p99 %6dµs  dropped %d  cpu %5.1f%%  %s", s.RatePerSec, s.LagP99US, s.Dropped, s.CPUPercent, verdict)
	}
	log.Printf("calibrate: jitter p50/p99/p999/max = %d/%d/%d/%dµs; capacity %v req/s", p.Jitter.P50US, p.Jitter.P99US, p.Jitter.P999US, p.Jitter.MaxUS, p.CapacityRPS)
	if p.Jitter.P99US > p.Limits.LagP99US {
		log.Printf("calibrate: WARNING: the idle jitter baseline (p99 %dµs) already exceeds the VALID dispatch-lag bound (%dµs): "+
			"this host adds that much lag to every schedule, so no rate can be VALID here (a virtualised or shared host, timer resolution?)",
			p.Jitter.P99US, p.Limits.LagP99US)
	}
	if p.Steps[len(p.Steps)-1].Passed {
		log.Printf("calibrate: every step passed: capacity is at least -max-rate; raise it to measure further")
	}

	if *out != "" {
		if err := p.WriteFile(*out); err != nil {
			return err
		}
		log.Printf("calibrate: wrote %s", *out)
	}
	if st != nil {
		raw, err := json.Marshal(p)
		if err != nil {
			return err
		}
		id, err := st.InsertCalibrationProfile(ctx, store.CalibrationProfile{
			HostClass: p.HostClass, Hostname: p.Hostname, MethodVersion: p.MethodVersion, NumCPU: p.NumCPU,
			GOMAXPROCS: p.GOMAXPROCS, GoVersion: p.GoVersion, Target: p.Target, CapacityRPS: p.CapacityRPS,
			JitterP50US: p.Jitter.P50US, JitterP99US: p.Jitter.P99US, JitterP999US: p.Jitter.P999US, JitterMaxUS: p.Jitter.MaxUS,
			Profile: raw, MeasuredAt: p.MeasuredAt,
		})
		if err != nil {
			return err
		}
		log.Printf("calibrate: recorded calibration_profiles id %d", id)
	}
	return nil
}

func serve(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	addr := fs.String("addr", ":8080", "listen address")
	latency := fs.Duration("latency", time.Millisecond, "base latency of every response")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	srv := &http.Server{
		Addr:              *addr,
		Handler:           stubserver.New(*latency),
		ReadHeaderTimeout: 5 * time.Second,
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	log.Printf("calibrate: stub server on %s, latency %v, stall with POST %s?d=2s", *addr, *latency, stubserver.StallPath)

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
