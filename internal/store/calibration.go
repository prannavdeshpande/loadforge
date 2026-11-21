package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/loadforge/loadforge/internal/transport"
)

// CalibrationProfile is a row of calibration_profiles. Profile is the
// whole profile as cmd/calibrate wrote it (package calibrate); the columns
// repeat what is queried.
type CalibrationProfile struct {
	ID            int64
	HostClass     string
	Hostname      string
	MethodVersion int
	NumCPU        int
	GOMAXPROCS    int
	GoVersion     string
	Target        string
	CapacityRPS   float64
	JitterP50US   int64
	JitterP99US   int64
	JitterP999US  int64
	JitterMaxUS   int64
	Profile       json.RawMessage
	MeasuredAt    time.Time
	RecordedAt    time.Time
}

// InsertCalibrationProfile appends a profile and returns its ID. Profiles
// are never updated: recalibrating adds a row.
func (s *Store) InsertCalibrationProfile(ctx context.Context, p CalibrationProfile) (int64, error) {
	if err := transport.ValidateID("host_class", p.HostClass); err != nil {
		return 0, err
	}
	if !json.Valid(p.Profile) || p.MeasuredAt.IsZero() {
		return 0, errors.New("store: a calibration profile needs its JSON and measured_at")
	}
	var id int64
	err := s.pool.QueryRow(ctx, `INSERT INTO calibration_profiles (host_class, hostname, method_version, num_cpu, gomaxprocs,
			go_version, target, capacity_rps, jitter_p50_us, jitter_p99_us, jitter_p999_us, jitter_max_us, profile, measured_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14) RETURNING id`,
		p.HostClass, p.Hostname, p.MethodVersion, p.NumCPU, p.GOMAXPROCS, p.GoVersion, p.Target, p.CapacityRPS,
		p.JitterP50US, p.JitterP99US, p.JitterP999US, p.JitterMaxUS, []byte(p.Profile), p.MeasuredAt).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("store: insert calibration profile for %s: %w", p.HostClass, err)
	}
	return id, nil
}

// LatestCalibrationProfile is the most recently measured profile of a host
// class, or ErrNotFound.
func (s *Store) LatestCalibrationProfile(ctx context.Context, hostClass string) (CalibrationProfile, error) {
	var p CalibrationProfile
	err := s.pool.QueryRow(ctx, `SELECT id, host_class, hostname, method_version, num_cpu, gomaxprocs, go_version, target,
			capacity_rps, jitter_p50_us, jitter_p99_us, jitter_p999_us, jitter_max_us, profile, measured_at, recorded_at
		FROM calibration_profiles WHERE host_class = $1 ORDER BY measured_at DESC, id DESC LIMIT 1`, hostClass).Scan(
		&p.ID, &p.HostClass, &p.Hostname, &p.MethodVersion, &p.NumCPU, &p.GOMAXPROCS, &p.GoVersion, &p.Target,
		&p.CapacityRPS, &p.JitterP50US, &p.JitterP99US, &p.JitterP999US, &p.JitterMaxUS, &p.Profile, &p.MeasuredAt, &p.RecordedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return CalibrationProfile{}, fmt.Errorf("%w: calibration profile for %s", ErrNotFound, hostClass)
	}
	if err != nil {
		return CalibrationProfile{}, fmt.Errorf("store: calibration profile for %s: %w", hostClass, err)
	}
	return p, nil
}
