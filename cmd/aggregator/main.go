// Command aggregator consumes HDR snapshots from Kafka, merges them and writes results to Postgres.
//
//	aggregator [-pg postgres://…] [-kafka localhost:9092] [-redis localhost:6379]
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/loadforge/loadforge/internal/aggregator"
	"github.com/loadforge/loadforge/internal/store"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	dsn := flag.String("pg", envOr("LF_PG_DSN", "postgres://loadforge:loadforge@localhost:5432/loadforge?sslmode=disable"), "Postgres DSN")
	brokers := flag.String("kafka", envOr("LF_KAFKA", "localhost:9092"), "comma-separated Kafka bootstrap brokers")
	redisAddr := flag.String("redis", envOr("LF_REDIS", "localhost:6379"), "Redis address for live progress; empty disables it")
	period := flag.Duration("period", time.Second, "how often to merge seconds and finalize runs")
	flag.Parse()

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

	// Redis only caches progress; it is not needed to start or to stay up.
	var prog aggregator.ProgressSink
	if *redisAddr != "" {
		rdb := redis.NewClient(&redis.Options{Addr: *redisAddr})
		defer rdb.Close()
		prog = aggregator.NewRedisProgress(rdb)
	}

	cl, err := aggregator.NewKafkaClient(strings.Split(*brokers, ","))
	if err != nil {
		return err
	}
	defer cl.Close()

	agg := aggregator.New(st, prog, aggregator.Config{Period: *period})
	log.Printf("aggregator: ready (Kafka %s, Redis %q)", *brokers, *redisAddr)

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, f := range []func(context.Context) error{
		func(ctx context.Context) error { return aggregator.Consume(ctx, cl, aggregator.NewIngester(st)) },
		agg.Run,
	} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := f(ctx); err != nil && !errors.Is(err, context.Canceled) {
				errs <- err
			}
			stop() // one half stopping stops the other
		}()
	}
	wg.Wait()
	close(errs)
	log.Printf("aggregator: stopped")
	return <-errs
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
