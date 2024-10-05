// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package event

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/history"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/errors"
)

// Event outlines the necessary behaviours that event structs must implement.
// Some key implementations of the interface includes assertionEvent,
// SetSpanConfigEvent, AddNodeEvent, SetNodeLivenessEvent,
// SetCapacityOverrideEvent.
type Event interface {
	// Func returns a closure associated with event which could be an assertion
	// or a mutation function. Invoking Func() returns the event function that
	// facilitates the events to be executed.
	Func() EventFunc
	// String returns the string representation for events which are used. It is
	// called when the event executor summarizes the executed events in the end.
	String() string
}

// EventFunc is an interface that encapsulates varying function signatures for
// events including assertion and mutation event functions. Some key
// implementations of the interface includes AssertionFunc, MutationFunc.
type EventFunc interface {
	// GetType returns the specific type of the event function.
	GetType() eventFuncType
}

// AssertionFunc is a function type for assertion-based events. It is for
// function that evaluate assertions based on the given history and current
// time. The returned boolean indicates the outcome of the assertion.
type AssertionFunc func(context.Context, time.Time, history.History) (hold bool)

// MutationFunc is a function type for mutation-based events. It is for
// function that executes mutation events on the given state.
type MutationFunc func(context.Context, state.State)

// eventFuncType represents different types of event functions.
type eventFuncType int

const (
	AssertionType eventFuncType = iota
	MutationType
)

func (AssertionFunc) GetType() eventFuncType {
	return AssertionType
}
func (MutationFunc) GetType() eventFuncType {
	return MutationType
}

// MutationWithAssertionEvent represents a specialized event type that includes
// a mutation event and its subsequent assertion event. It ensures that changes
// introduced by the mutation are verified.
//
// Note that we expect MutationEvent to use a mutation function while
// AssertionEvent to use an assertion function. Please use Validate() to
// confirm before using.
type MutationWithAssertionEvent struct {
	MutationEvent    Event
	AssertionEvent   assertionEvent
	DurationToAssert time.Duration
}

// Validate checks whether the MutationWithAssertionEvent is correctly paired
// with both a mutation and an assertion event.
func (mae MutationWithAssertionEvent) Validate() error {
	if mae.AssertionEvent.Func().GetType() != AssertionType {
		return errors.New("MutationWithAssertionEvent.AssertionEvent is not recognized as an assertion event; " +
			"please use an assertion event with a AssertionFunc")
	}
	if mae.MutationEvent.Func().GetType() != MutationType {
		return errors.New("MutationWithAssertionEvent.MutationEvent is not recognized as a mutation event; " +
			"please use a mutation event with a MutationFunc")
	}
	return nil
}
