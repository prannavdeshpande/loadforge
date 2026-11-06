package health

// Verdict is a run's single outcome (design.md §5). Every terminal run has
// exactly one; results are never shown without it.
type Verdict string

const (
	Valid    Verdict = "VALID"    // the generator kept to its schedule
	Degraded Verdict = "DEGRADED" // usable, but the generator was strained or a signal is unmeasured
	Invalid  Verdict = "INVALID"  // the generator distorted the measurement, or the run aborted
)

// Known reports whether v is one of the three verdicts.
func (v Verdict) Known() bool { return v == Valid || v == Degraded || v == Invalid }
