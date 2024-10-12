// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cat

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

// Trigger is an interface to a trigger on a table or view, which executes a
// trigger function in response to a pre-defined mutation of the table.
type Trigger interface {
	// Name is the name of the trigger. It is unique within a given table, and
	// cannot be qualified.
	Name() tree.Name

	// ActionTime defines whether the trigger should be fired before, after, or
	// instead of the triggering event.
	ActionTime() tree.TriggerActionTime

	// EventCount returns the number of events that the trigger is configured to
	// fire on.
	EventCount() int

	// Event returns the ith event that the trigger is configured to fire on.
	Event(i int) tree.TriggerEvent

	// NewTransitionAlias is the name to be used for the NEW transition table.
	// If no alias was specified, the result is the empty string.
	NewTransitionAlias() tree.Name

	// OldTransitionAlias is the name to be used for the OLD transition table.
	// If no alias was specified, the result is the empty string.
	OldTransitionAlias() tree.Name

	// ForEachRow returns true if the trigger function should be invoked once for
	// each row affected by the triggering event. If false, the trigger function
	// is invoked once per statement.
	ForEachRow() bool

	// WhenExpr is the optional filter expression that determines whether the
	// trigger function should be invoked. If no WHEN clause was specified, the
	// result is the empty string.
	WhenExpr() string

	// FuncID is the ID of the function that will be called when the trigger
	// fires.
	FuncID() StableID

	// FuncArgs is a list of constant string arguments for the trigger function.
	FuncArgs() tree.Datums

	// FuncBody is the set of body statements for the trigger function with
	// fully-qualified names, resolved when the trigger was created.
	FuncBody() string

	// Enabled is true if the trigger is currently enabled.
	Enabled() bool
}
