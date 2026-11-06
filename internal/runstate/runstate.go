package runstate

import (
	"errors"
	"fmt"
)

// State is a run's lifecycle state, stored in runs.state.
//
//	PENDING → PREPARING → READY → RUNNING → STOPPING → COMPLETED
//	    └──────────┴─────────┴────────┴──────────┴────▶ ABORTED
type State string

const (
	Pending   State = "PENDING"
	Preparing State = "PREPARING"
	Ready     State = "READY"
	Running   State = "RUNNING"
	Stopping  State = "STOPPING"
	Completed State = "COMPLETED"
	Aborted   State = "ABORTED"
)

// All lists every state in lifecycle order.
var All = []State{Pending, Preparing, Ready, Running, Stopping, Completed, Aborted}

// next holds the forward edge out of each non-terminal state. ABORTED is
// reachable from every non-terminal state and is not listed here.
var next = map[State]State{
	Pending:   Preparing,
	Preparing: Ready,
	Ready:     Running,
	Running:   Stopping,
	Stopping:  Completed,
}

// ErrIllegalTransition is wrapped by Check for a transition the machine
// does not allow.
var ErrIllegalTransition = errors.New("runstate: illegal transition")

// Valid reports whether s is a known state.
func (s State) Valid() bool {
	switch s {
	case Pending, Preparing, Ready, Running, Stopping, Completed, Aborted:
		return true
	}
	return false
}

// Terminal reports whether s ends the run. A terminal run carries exactly
// one verdict (CLAUDE.md rule 5); the schema enforces it.
func (s State) Terminal() bool { return s == Completed || s == Aborted }

// Check reports whether a run may move from one state to another.
func Check(from, to State) error {
	if !from.Valid() || !to.Valid() {
		return fmt.Errorf("%w: unknown state in %s → %s", ErrIllegalTransition, from, to)
	}
	if from.Terminal() {
		return fmt.Errorf("%w: %s is terminal", ErrIllegalTransition, from)
	}
	if to == Aborted || next[from] == to {
		return nil
	}
	return fmt.Errorf("%w: %s → %s", ErrIllegalTransition, from, to)
}
