// Command worker generates open-loop load and emits per-second HDR snapshots.
//
//	worker -id w1 -max-rate 5000 [-nats nats://localhost:4222] [-kafka localhost:9092]
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/loadforge/loadforge/internal/safety"
	"github.com/loadforge/loadforge/internal/worker"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	id := flag.String("id", "", "worker ID, 1-64 characters of [A-Za-z0-9_-] (required)")
	maxRate := flag.Float64("max-rate", 0, "highest per-worker rate in req/s this worker accepts (required)")
	natsURL := flag.String("nats", nats.DefaultURL, "NATS server URL")
	brokers := flag.String("kafka", "localhost:9092", "comma-separated Kafka bootstrap brokers")
	shutdown := flag.Duration("shutdown-timeout", 30*time.Second, "how long SIGTERM waits for the run to drain before aborting it")
	flag.Parse()
	if *id == "" || *maxRate <= 0 {
		flag.Usage()
		return errors.New("worker: -id and a positive -max-rate are required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pub, err := worker.NewKafkaPublisher(strings.Split(*brokers, ","))
	if err != nil {
		return err
	}
	defer pub.Close()
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	err = pub.Ping(pingCtx)
	cancel()
	if err != nil {
		return err
	}

	// While NATS is unreachable no controller heartbeat arrives, so the
	// dead-man's switch stops any run; reconnecting does not restart it.
	nc, err := nats.Connect(*natsURL,
		nats.Name("loadforge-worker-"+*id),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) { log.Printf("worker %s: NATS disconnected: %v", *id, err) }),
		nats.ReconnectHandler(func(*nats.Conn) { log.Printf("worker %s: NATS reconnected", *id) }),
	)
	if err != nil {
		return err
	}
	defer nc.Close()

	w, err := worker.New(worker.Config{ID: *id, RateCap: safety.RateCap(*maxRate)}, nc, pub)
	if err != nil {
		return err
	}
	log.Printf("worker %s: ready (NATS %s, Kafka %s, max %v req/s)", *id, *natsURL, *brokers, *maxRate)

	<-ctx.Done()
	log.Printf("worker %s: shutting down", *id)
	closeCtx, cancel := context.WithTimeout(context.Background(), *shutdown)
	defer cancel()
	return w.Close(closeCtx)
}
