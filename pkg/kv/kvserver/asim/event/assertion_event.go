// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package event

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/assertion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/history"
)

// assertionEvent represents a single event containing assertions to be checked.
// For proper initialization, please use NewAssertionEvent constructor instead
// of direct struct literal assignment.
type assertionEvent struct {
	// assertions represents set of assertions to be checked during this event.
	assertions []assertion.SimulationAssertion
	// result represents results of executed assertions for this event. It
	// starts empty but gets populated as the event runs and assertions are
	// checked. If the event has run, its size == len(assertions).
	result *[]assertionResult
}

// assertionResult represents the outcome of a checked assertion within an
// event.
type assertionResult struct {
	// holds indicates whether the assertion passed.
	holds bool
	// reason represents the cause for the assertion failure. It is non-empty
	// only if holds is false.
	reason string
}

func (ar assertionResult) String() string {
	if ar.holds {
		return "passed"
	} else {
		return fmt.Sprintf("failed: %s", ar.reason)
	}
}

// NewAssertionEvent is assertionEvent's constructor. It ensures proper
// initialization of assertionResults, preventing panics like accessing a nil
// pointer.
func NewAssertionEvent(assertions []assertion.SimulationAssertion) assertionEvent {
	assertionResults := make([]assertionResult, 0, len(assertions))
	return assertionEvent{
		assertions: assertions,
		result:     &assertionResults,
	}
}

// String provides a string representation of an assertion event. It is called
// when the event executor summarizes the executed events in the end.
func (ag assertionEvent) String() string {
	if ag.result == nil {
		panic("unexpected nil")
	}
	if len(ag.assertions) == 0 {
		return "no assertions were registered"
	}
	if len(*ag.result) != len(ag.assertions) {
		return "assertions were not executed; likely due to short test duration."
	}

	buf := &strings.Builder{}
	buf.WriteString("assertion checking event\n")
	for i, a := range ag.assertions {
		buf.WriteString(fmt.Sprintf("\t\t\t%d. assertion=%s\n", i+1, a))
		buf.WriteString(fmt.Sprintf("\t\t\t%v", (*ag.result)[i]))
		if i != len(ag.assertions)-1 {
			buf.WriteString("\n")
		}
	}
	return buf.String()
}

// Func returns an assertion event function that runs the assertions defined in
// assertionEvent and fills the result field upon checking. It is designed to be
// invoked externally.
func (ag assertionEvent) Func() EventFunc {
	return AssertionFunc(func(ctx context.Context, t time.Time, h history.History) bool {
		if ag.result == nil {
			panic("assertionEvent.result is nil; use NewAssertionEvent for proper initialization.")
		}
		allHolds := true
		for _, eachAssert := range ag.assertions {
			holds, reason := eachAssert.Assert(ctx, h)
			*ag.result = append(*ag.result, assertionResult{
				holds, reason,
			})
			if !holds {
				allHolds = false
			}
		}
		return allHolds
	})
}
