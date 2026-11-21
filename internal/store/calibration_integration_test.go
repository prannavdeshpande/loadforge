//go:build integration

package store

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestIntegrationCalibrationProfiles(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	if _, err := s.LatestCalibrationProfile(ctx, "c6i"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("no profile yet: %v", err)
	}
	base := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	mk := func(capacity float64, at time.Time) CalibrationProfile {
		return CalibrationProfile{HostClass: "c6i", Hostname: "h", MethodVersion: 1, NumCPU: 8, GOMAXPROCS: 8,
			GoVersion: "go1.23.0", Target: "in-process", CapacityRPS: capacity, JitterP50US: 30, JitterP99US: 120,
			JitterP999US: 400, JitterMaxUS: 900, Profile: json.RawMessage(`{"capacity_rps":1}`), MeasuredAt: at}
	}
	first, err := s.InsertCalibrationProfile(ctx, mk(4000, base))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.InsertCalibrationProfile(ctx, mk(8000, base.Add(time.Hour))); err != nil {
		t.Fatal(err)
	}
	// Recorded later but measured earlier: not the latest.
	if _, err := s.InsertCalibrationProfile(ctx, mk(1000, base.Add(-time.Hour))); err != nil {
		t.Fatal(err)
	}
	p, err := s.LatestCalibrationProfile(ctx, "c6i")
	if err != nil || p.CapacityRPS != 8000 || p.JitterP999US != 400 || !p.MeasuredAt.Equal(base.Add(time.Hour)) {
		t.Fatalf("latest %+v, %v", p, err)
	}

	// Append-only, and the schema refuses what makes no sense.
	if _, err := s.pool.Exec(ctx, `UPDATE calibration_profiles SET capacity_rps = 1 WHERE id = $1`, first); err == nil {
		t.Error("profile updated")
	}
	if _, err := s.pool.Exec(ctx, `DELETE FROM calibration_profiles WHERE id = $1`, first); err == nil {
		t.Error("profile deleted")
	}
	if _, err := s.InsertCalibrationProfile(ctx, mk(-1, base)); err == nil {
		t.Error("negative capacity accepted")
	}
	bad := mk(1, base)
	bad.HostClass = "a.b"
	if _, err := s.InsertCalibrationProfile(ctx, bad); err == nil {
		t.Error("bad host class accepted")
	}
}
