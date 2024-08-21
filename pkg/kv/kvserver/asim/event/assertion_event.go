// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package event

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/assertion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/history"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/validator"
)

// AssertionEvent represents a single event containing assertions to be checked.
// For proper initialization, please use NewAssertionEvent constructor instead
// of direct struct literal assignment.
type AssertionEvent struct {
	prevEvent Event
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

	validationResult string
}

func (ar assertionResult) String() string {
	if ar.holds {
		return "passed"
	} else {
		return fmt.Sprintf("failed: %s\n \t%s\n", ar.reason, ar.validationResult)
	}
}

// NewAssertionEvent is AssertionEvent's constructor. It ensures proper
// initialization of assertionResults, preventing panics like accessing a nil
// pointer.
func NewAssertionEvent(e Event, assertions []assertion.SimulationAssertion) AssertionEvent {
	assertionResults := make([]assertionResult, 0, len(assertions))
	return AssertionEvent{
		prevEvent:  e,
		assertions: assertions,
		result:     &assertionResults,
	}
}

// String provides a string representation of an assertion event. It is called
// when the event executor summarizes the executed events in the end.
func (ag AssertionEvent) String() string {
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
// AssertionEvent and fills the result field upon checking. It is designed to be
// invoked externally.
func (ag AssertionEvent) Func() EventFunc {
	return AssertionFunc(func(ctx context.Context, t time.Time, h history.History, s state.State) bool {
		if ag.result == nil {
			panic("AssertionEvent.result is nil; use NewAssertionEvent for proper initialization.")
		}
		allHolds := true
		v := validator.NewValidator(s.ClusterInfo().Regions)
		for _, eachAssert := range ag.assertions {
			holds, reason := eachAssert.Assert(ctx, h)
			var validationResult string
			if !holds {
				if e, ok := ag.prevEvent.(SetSpanConfigEvent); ok {
					validationResult = fmt.Sprintln(v.ValidateEvent(e.Config))
				}
			}
			*ag.result = append(*ag.result, assertionResult{
				holds, reason, validationResult,
			})
			if !holds {
				allHolds = false
			}
		}
		return allHolds
	})
}
