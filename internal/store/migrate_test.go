package store

import (
	"errors"
	"strings"
	"testing"
	"testing/fstest"
)

// appliedChecksums pins every migration that has shipped. Applied
// migrations must never change (CLAUDE.md workflow rule 3), so editing one
// fails here before it can fail against a database. Add a line for each
// new migration; never change an existing line.
var appliedChecksums = map[int]string{
	1: "493a6c0dc0565f5d41bbf7da01221f001591c35658fedd1b803690cca2e6972d", // 0001_init
}

func TestShippedMigrationsAreUnchanged(t *testing.T) {
	ms, err := LoadMigrations(Migrations)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range ms {
		want, ok := appliedChecksums[m.Version]
		if !ok {
			t.Errorf("migration %04d_%s is not pinned in appliedChecksums: add %q", m.Version, m.Name, m.Checksum)
			continue
		}
		if m.Checksum != want {
			t.Errorf("migration %04d_%s was edited (checksum %s, shipped as %s): add a new migration instead",
				m.Version, m.Name, m.Checksum, want)
		}
	}
	if len(ms) < len(appliedChecksums) {
		t.Errorf("%d migrations embedded, %d shipped: a shipped migration was deleted", len(ms), len(appliedChecksums))
	}
}

func mapFS(files map[string]string) fstest.MapFS {
	fsys := fstest.MapFS{}
	for name, body := range files {
		fsys[name] = &fstest.MapFile{Data: []byte(body)}
	}
	return fsys
}

func TestLoadMigrationsOrdersAndChecksums(t *testing.T) {
	ms, err := LoadMigrations(mapFS(map[string]string{
		"0002_two.sql": "SELECT 2;",
		"0001_one.sql": "SELECT 1;",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if len(ms) != 2 || ms[0].Version != 1 || ms[1].Version != 2 || ms[0].Name != "one" {
		t.Fatalf("got %+v", ms)
	}
	if ms[0].Checksum == ms[1].Checksum || len(ms[0].Checksum) != 64 {
		t.Errorf("checksums %q %q", ms[0].Checksum, ms[1].Checksum)
	}
}

func TestLoadMigrationsRejectsBadSets(t *testing.T) {
	for name, files := range map[string]map[string]string{
		"gap":       {"0001_a.sql": "", "0003_c.sql": ""},
		"duplicate": {"0001_a.sql": "", "0001_b.sql": ""},
		"not first": {"0002_b.sql": ""},
		"bad name":  {"0001_a.sql": "", "2_b.sql": ""},
		"uppercase": {"0001_Init.sql": ""},
		"empty":     {},
	} {
		if _, err := LoadMigrations(mapFS(files)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestPendingAppliesOnlyWhatIsMissing(t *testing.T) {
	files, err := LoadMigrations(mapFS(map[string]string{"0001_a.sql": "A", "0002_b.sql": "B", "0003_c.sql": "C"}))
	if err != nil {
		t.Fatal(err)
	}
	applied := []appliedMigration{{1, files[0].Checksum}, {2, files[1].Checksum}}
	todo, err := pending(files, applied)
	if err != nil {
		t.Fatal(err)
	}
	if len(todo) != 1 || todo[0].Version != 3 {
		t.Fatalf("pending = %+v, want only 0003", todo)
	}
	all, err := pending(files, nil)
	if err != nil || len(all) != 3 {
		t.Fatalf("fresh database: pending = %d migrations, err %v", len(all), err)
	}
	none, err := pending(files, []appliedMigration{{1, files[0].Checksum}, {2, files[1].Checksum}, {3, files[2].Checksum}})
	if err != nil || len(none) != 0 {
		t.Fatalf("up to date: pending = %d migrations, err %v", len(none), err)
	}
}

func TestPendingRefusesEditedMigration(t *testing.T) {
	files, _ := LoadMigrations(mapFS(map[string]string{"0001_a.sql": "A edited", "0002_b.sql": "B"}))
	_, err := pending(files, []appliedMigration{{1, files[1].Checksum}})
	if !errors.Is(err, ErrMigrationEdited) {
		t.Fatalf("err = %v, want ErrMigrationEdited", err)
	}
	if !strings.Contains(err.Error(), "0001_a") {
		t.Errorf("error %q does not name the migration", err)
	}
}

func TestPendingRefusesUnknownOrHoledHistory(t *testing.T) {
	files, _ := LoadMigrations(mapFS(map[string]string{"0001_a.sql": "A", "0002_b.sql": "B"}))
	if _, err := pending(files, []appliedMigration{{1, files[0].Checksum}, {3, "x"}}); !errors.Is(err, ErrSchemaTooNew) {
		t.Errorf("unknown applied version: err = %v, want ErrSchemaTooNew", err)
	}
	if _, err := pending(files, []appliedMigration{{2, files[1].Checksum}}); err == nil {
		t.Error("history with 0002 but not 0001 accepted")
	}
}
