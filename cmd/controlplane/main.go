// Command controlplane drives the run lifecycle, issues NATS commands and
// records verdicts (design.md §6, §8). It only parses flags and wires
// dependencies; the logic is in internal/controlplane.
//
//	controlplane -max-rate-per-worker 5000 -max-total-rate 50000 -max-duration 1h -max-workers 50
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"

	"github.com/loadforge/loadforge/internal/controlplane"
	"github.com/loadforge/loadforge/internal/safety"
	"github.com/loadforge/loadforge/internal/store"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	host, _ := os.Hostname()
	id := flag.String("id", envOr("LF_CONTROLLER_ID", host), "controller instance ID, for logs and events")
	listen := flag.String("listen", envOr("LF_LISTEN", ":8090"), "HTTP API listen address")
	dsn := flag.String("pg", envOr("LF_PG_DSN", "postgres://loadforge:loadforge@localhost:5432/loadforge?sslmode=disable"), "Postgres DSN")
	redisAddr := flag.String("redis", envOr("LF_REDIS", "localhost:6379"), "Redis address (leases and live progress)")
	natsURL := flag.String("nats", envOr("LF_NATS_URL", nats.DefaultURL), "NATS server URL")
	maxRate := flag.Float64("max-rate-per-worker", 0, "highest per-worker rate in req/s a run may ask for (required)")
	maxTotal := flag.Float64("max-total-rate", 0, "highest total rate in req/s across a run's workers (required)")
	maxDuration := flag.Duration("max-duration", 0, "longest run duration allowed (required)")
	maxWorkers := flag.Int("max-workers", 0, "most workers in one run (required)")
	shutdown := flag.Duration("shutdown-timeout", 60*time.Second, "how long SIGTERM waits for this controller's runs to be stopped and recorded ABORTED")
	flag.Parse()
	if *maxRate <= 0 || *maxTotal <= 0 || *maxDuration <= 0 || *maxWorkers <= 0 {
		flag.Usage()
		return errors.New("controlplane: -max-rate-per-worker, -max-total-rate, -max-duration and -max-workers must be positive")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st, err := store.Open(ctx, *dsn)
	if err != nil {
		return err
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		return err
	}

	rdb := redis.NewClient(&redis.Options{Addr: *redisAddr})
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return err
	}

	// While NATS is down no heartbeat reaches the workers, so their
	// dead-man's switches stop the runs; this controller then aborts them.
	nc, err := nats.Connect(*natsURL,
		nats.Name("loadforge-controlplane-"+*id),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) { log.Printf("controlplane: NATS disconnected: %v", err) }),
		nats.ReconnectHandler(func(*nats.Conn) { log.Printf("controlplane: NATS reconnected") }),
	)
	if err != nil {
		return err
	}
	defer nc.Close()

	cfg := controlplane.DefaultConfig()
	cfg.InstanceID = *id
	cfg.Limits = controlplane.Limits{
		MaxRatePerWorker: safety.RateCap(*maxRate), MaxTotalRate: *maxTotal,
		MaxDuration: *maxDuration, MaxWorkers: *maxWorkers,
	}
	c, err := controlplane.New(cfg, st, store.NewLeases(rdb), nc, controlplane.NewRedisLive(rdb))
	if err != nil {
		return err
	}
	if err := c.Start(); err != nil {
		return err
	}

	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: c.Handler(), ReadHeaderTimeout: 10 * time.Second}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()
	log.Printf("controlplane %s: listening on %s", *id, ln.Addr())

	select {
	case <-ctx.Done():
	case err := <-serveErr:
		log.Printf("controlplane: HTTP server: %v", err)
	}
	log.Printf("controlplane %s: shutting down", *id)
	closeCtx, cancel := context.WithTimeout(context.Background(), *shutdown)
	defer cancel()
	_ = srv.Shutdown(closeCtx)
	return c.Close(closeCtx)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
