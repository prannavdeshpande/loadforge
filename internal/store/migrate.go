package store

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"regexp"
	"sort"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var embedded embed.FS

// Migrations is the schema shipped with this binary.
var Migrations fs.FS = mustSub(embedded, "migrations")

func mustSub(f fs.FS, dir string) fs.FS {
	sub, err := fs.Sub(f, dir)
	if err != nil {
		panic(err)
	}
	return sub
}

// migrateLockID serialises concurrent Migrate calls across processes
// (pg_advisory_lock). The value is arbitrary but fixed.
const migrateLockID = 0x6c66_6d69_6772 // "lfmigr"

// Migration is one numbered schema change, NNNN_name.sql.
type Migration struct {
	Version  int
	Name     string
	SQL      string
	Checksum string // sha256 of SQL, hex
}

// appliedMigration is a row of schema_migrations.
type appliedMigration struct {
	Version  int
	Checksum string
}

var migrationName = regexp.MustCompile(`^(\d{4})_([a-z0-9_]+)\.sql$`)

// LoadMigrations reads the migrations in fsys. Versions must run 1, 2, 3, …
// with no gaps or duplicates, so two branches that both add a migration
// conflict here rather than in production.
func LoadMigrations(fsys fs.FS) ([]Migration, error) {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, fmt.Errorf("store: read migrations: %w", err)
	}
	var ms []Migration
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		m := migrationName.FindStringSubmatch(e.Name())
		if m == nil {
			return nil, fmt.Errorf("store: migration file %q is not named NNNN_name.sql", e.Name())
		}
		v, _ := strconv.Atoi(m[1]) // four digits: cannot fail
		b, err := fs.ReadFile(fsys, path.Clean(e.Name()))
		if err != nil {
			return nil, fmt.Errorf("store: read migration %s: %w", e.Name(), err)
		}
		sum := sha256.Sum256(b)
		ms = append(ms, Migration{Version: v, Name: m[2], SQL: string(b), Checksum: hex.EncodeToString(sum[:])})
	}
	sort.Slice(ms, func(i, j int) bool { return ms[i].Version < ms[j].Version })
	for i, m := range ms {
		if m.Version != i+1 {
			return nil, fmt.Errorf("store: migrations must be numbered 0001, 0002, … without gaps or duplicates; found %04d at position %d", m.Version, i+1)
		}
	}
	if len(ms) == 0 {
		return nil, errors.New("store: no migrations found")
	}
	return ms, nil
}

// ErrMigrationEdited means a migration already applied to the database no
// longer matches its file. Applied migrations must never be edited
// (CLAUDE.md workflow rule 3); add a new one instead.
var ErrMigrationEdited = errors.New("store: an applied migration was edited")

// ErrSchemaTooNew means the database has migrations this binary does not
// know, so it was migrated by a newer build.
var ErrSchemaTooNew = errors.New("store: database schema is newer than this binary")

// pending checks the applied migrations against the files and returns the
// ones still to apply, in order.
func pending(files []Migration, applied []appliedMigration) ([]Migration, error) {
	byVersion := make(map[int]Migration, len(files))
	for _, m := range files {
		byVersion[m.Version] = m
	}
	done := make(map[int]bool, len(applied))
	for _, a := range applied {
		m, ok := byVersion[a.Version]
		if !ok {
			return nil, fmt.Errorf("%w: version %04d is applied but unknown", ErrSchemaTooNew, a.Version)
		}
		if m.Checksum != a.Checksum {
			return nil, fmt.Errorf("%w: %04d_%s has checksum %s, applied as %s", ErrMigrationEdited, m.Version, m.Name, m.Checksum, a.Checksum)
		}
		done[a.Version] = true
	}
	// Applied versions must be 1..k: a hole means the history diverged.
	for v := 1; v <= len(done); v++ {
		if !done[v] {
			return nil, fmt.Errorf("store: applied migrations skip %04d", v)
		}
	}
	return files[len(done):], nil
}

// Migrate applies the pending migrations in fsys, each in its own
// transaction, under an advisory lock so that concurrent starts are safe.
// It refuses to run if an applied migration was edited.
func Migrate(ctx context.Context, pool *pgxpool.Pool, fsys fs.FS) error {
	files, err := LoadMigrations(fsys)
	if err != nil {
		return err
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("store: migrate: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrateLockID); err != nil {
		return fmt.Errorf("store: migrate lock: %w", err)
	}
	defer func() {
		// Use a fresh context: the lock must be released even if ctx ended.
		_, _ = conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1)`, migrateLockID)
	}()

	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version    integer     PRIMARY KEY,
		name       text        NOT NULL,
		checksum   text        NOT NULL,
		applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return fmt.Errorf("store: create schema_migrations: %w", err)
	}

	rows, err := conn.Query(ctx, `SELECT version, checksum FROM schema_migrations ORDER BY version`)
	if err != nil {
		return fmt.Errorf("store: read schema_migrations: %w", err)
	}
	applied, err := pgx.CollectRows(rows, pgx.RowToStructByPos[appliedMigration])
	if err != nil {
		return fmt.Errorf("store: read schema_migrations: %w", err)
	}

	todo, err := pending(files, applied)
	if err != nil {
		return err
	}
	for _, m := range todo {
		err := pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, m.SQL); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version, name, checksum) VALUES ($1, $2, $3)`,
				m.Version, m.Name, m.Checksum)
			return err
		})
		if err != nil {
			return fmt.Errorf("store: apply migration %04d_%s: %w", m.Version, m.Name, err)
		}
	}
	return nil
}
