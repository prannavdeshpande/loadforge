// Package runstate defines the run state machine (design.md §3.1).
//
// It is pure: it says which transitions are legal. internal/store persists
// the state in runs.state and checks every transition here, inside the same
// fenced transaction that appends the run_events row.
package runstate
