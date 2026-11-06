package runstate

import (
	"errors"
	"testing"
)

// legal is the full transition relation from design.md §3.1, written out
// independently of the implementation's table.
var legal = map[[2]State]bool{
	{Pending, Preparing}:  true,
	{Preparing, Ready}:    true,
	{Ready, Running}:      true,
	{Running, Stopping}:   true,
	{Stopping, Completed}: true,

	{Pending, Aborted}:   true,
	{Preparing, Aborted}: true,
	{Ready, Aborted}:     true,
	{Running, Aborted}:   true,
	{Stopping, Aborted}:  true,
}

func TestTransitionRelationIsExactlyTheDesign(t *testing.T) {
	for _, from := range All {
		for _, to := range All {
			err := Check(from, to)
			if want := legal[[2]State{from, to}]; want != (err == nil) {
				t.Errorf("%s → %s: allowed=%v, want %v (err=%v)", from, to, err == nil, want, err)
			}
			if err != nil && !errors.Is(err, ErrIllegalTransition) {
				t.Errorf("%s → %s: error %v does not wrap ErrIllegalTransition", from, to, err)
			}
		}
	}
}

func TestAbortReachableFromEveryNonTerminal(t *testing.T) {
	for _, s := range All {
		if s.Terminal() {
			continue
		}
		if err := Check(s, Aborted); err != nil {
			t.Errorf("%s cannot abort: %v", s, err)
		}
	}
}

func TestTerminalStatesHaveNoExit(t *testing.T) {
	for _, s := range []State{Completed, Aborted} {
		if !s.Terminal() {
			t.Errorf("%s is not terminal", s)
		}
		for _, to := range All {
			if Check(s, to) == nil {
				t.Errorf("%s → %s allowed out of a terminal state", s, to)
			}
		}
	}
}

func TestSelfTransitionsAndUnknownStatesRejected(t *testing.T) {
	for _, s := range All {
		if Check(s, s) == nil {
			t.Errorf("%s → %s allowed", s, s)
		}
	}
	for _, c := range [][2]State{{"", Preparing}, {Pending, "pending"}, {"BOGUS", Aborted}} {
		if !errors.Is(Check(c[0], c[1]), ErrIllegalTransition) {
			t.Errorf("%q → %q allowed", c[0], c[1])
		}
	}
}

func TestEveryNonTerminalStateCanReachCompleted(t *testing.T) {
	for _, s := range All {
		if s.Terminal() {
			continue
		}
		cur, steps := s, 0
		for cur != Completed {
			nxt, ok := next[cur]
			if !ok || Check(cur, nxt) != nil || steps > len(All) {
				t.Fatalf("%s: stuck at %s", s, cur)
			}
			cur, steps = nxt, steps+1
		}
	}
}
