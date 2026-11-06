package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store is the Postgres system of record (design.md §3.1).
type Store struct {
	pool *pgxpool.Pool
}

// Open connects to Postgres and checks the connection. It does not migrate;
// call Migrate.
func Open(ctx context.Context, dsn string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("store: parse DSN: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("store: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: ping: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Pool exposes the connection pool, for Migrate and tests.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Migrate applies the embedded migrations.
func (s *Store) Migrate(ctx context.Context) error { return Migrate(ctx, s.pool, Migrations) }

// Close closes the pool.
func (s *Store) Close() { s.pool.Close() }

var (
	// ErrStaleFence means a guarded write carried a fencing token lower than
	// the one Postgres already has for the run. The writer has lost the run
	// and must give up its lease at once (CLAUDE.md rule 3).
	ErrStaleFence = errors.New("store: stale fencing token")
	// ErrNotFound means the run does not exist.
	ErrNotFound = errors.New("store: not found")
	// ErrExists means a run with that ID already exists.
	ErrExists = errors.New("store: already exists")
	// ErrStateConflict means the run is not in the state the caller
	// expected, typically because another writer moved it first.
	ErrStateConflict = errors.New("store: run is not in the expected state")
	// ErrProtocolViolation means a worker contradicted what it reported
	// before. The write was refused and a protocol_violation event logged.
	ErrProtocolViolation = errors.New("store: protocol violation")
)

// NextFenceToken draws a new fencing token from fence_seq. Tokens only ever
// increase, whatever happens to Redis (design.md §3.3).
func (s *Store) NextFenceToken(ctx context.Context) (uint64, error) {
	var t uint64
	if err := s.pool.QueryRow(ctx, `SELECT nextval('fence_seq')`).Scan(&t); err != nil {
		return 0, fmt.Errorf("store: next fence token: %w", err)
	}
	return t, nil
}

// fence locks the run's row and checks the caller's token against it. A
// lower token fails with ErrStaleFence; a higher one is recorded, so every
// older holder is rejected from then on. It returns the run's state.
//
// Every write a run lease guards calls this first, in the same
// transaction. The UPDATE also carries the predicate from design.md §3.3,
// so the check holds even if the row lock were ever dropped.
func fence(ctx context.Context, tx pgx.Tx, runID string, token uint64) (string, error) {
	if token == 0 {
		return "", fmt.Errorf("%w: token 0 is never issued", ErrStaleFence)
	}
	var (
		state string
		cur   uint64
	)
	err := tx.QueryRow(ctx, `SELECT state, fence_token FROM runs WHERE id = $1 FOR UPDATE`, runID).Scan(&state, &cur)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", fmt.Errorf("%w: run %s", ErrNotFound, runID)
	}
	if err != nil {
		return "", err
	}
	if token < cur {
		return "", fmt.Errorf("%w: run %s token %d < %d", ErrStaleFence, runID, token, cur)
	}
	if token > cur {
		tag, err := tx.Exec(ctx, `UPDATE runs SET fence_token = $2 WHERE id = $1 AND fence_token <= $2`, runID, token)
		if err != nil {
			return "", err
		}
		if tag.RowsAffected() != 1 {
			return "", fmt.Errorf("%w: run %s token %d", ErrStaleFence, runID, token)
		}
	}
	return state, nil
}

// guarded runs fn in a transaction after fencing the run.
func (s *Store) guarded(ctx context.Context, runID string, token uint64, fn func(tx pgx.Tx, state string) error) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		state, err := fence(ctx, tx, runID, token)
		if err != nil {
			return err
		}
		return fn(tx, state)
	})
}

func isUniqueViolation(err error) bool {
	var pg *pgconn.PgError
	return errors.As(err, &pg) && pg.Code == "23505"
}
