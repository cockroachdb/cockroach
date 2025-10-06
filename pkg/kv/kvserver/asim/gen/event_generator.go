// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gen

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/event"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/scheduled"
)

// EventGen provides a  method to generate a list of events that will apply to
// the simulated cluster. Currently, only delayed (fixed time) events are
// supported.
type EventGen interface {
	// Generate returns an eventExecutor storing a sorted list of events and
	// being ready execute events for the simulation execution.
	Generate(seed int64, settings *config.SimulationSettings) scheduled.EventExecutor
	// String returns the concise string representation of the event executor,
	// detailing the number of scheduled events.
	String() string
}

// StaticEvents implements the EventGen interface. For proper initialization,
// please use NewStaticEventsWithNoEvents constructor instead of direct struct
// literal assignment.
// TODO(kvoli): introduce conditional events.
type StaticEvents struct {
	// eventExecutor handles the registration of scheduled event and ensures
	// they are sorted chronologically.
	eventExecutor scheduled.EventExecutor
}

// NewStaticEventsWithNoEvents is StaticEvents's constructor. It ensures that
// both the eventExecutor interface and its underlying struct are initialized
// and non-nil.
func NewStaticEventsWithNoEvents() StaticEvents {
	return StaticEvents{
		eventExecutor: scheduled.NewExecutorWithNoEvents(),
	}
}

// ScheduleEvent registers the event with the eventExecutor scheduled at
// startTime.Add(delay). After registration, events remain unsorted by their
// order until Generate() is called.
func (se StaticEvents) ScheduleEvent(startTime time.Time, delay time.Duration, event event.Event) {
	if se.eventExecutor == nil {
		panic("StaticEvents.eventExecutor is a nil interface; " +
			"use NewStaticEventsWithNoEvents for proper initialization.")
	}
	se.eventExecutor.RegisterScheduledEvent(scheduled.ScheduledEvent{
		At:          startTime.Add(delay),
		TargetEvent: event,
	})
}

// ScheduleMutationWithAssertionEvent registers the mutation event with the
// eventExecutor scheduled at startTime.Add(delay) followed by the assertion
// event scheduled DurationToAssert after executing the mutation event at
// startTime.Add(delay).Add(DurationToAssert).
func (se StaticEvents) ScheduleMutationWithAssertionEvent(
	startTime time.Time, delay time.Duration, event event.MutationWithAssertionEvent,
) {
	if se.eventExecutor == nil {
		panic("StaticEvents.eventExecutor is a nil interface; " +
			"use NewStaticEventsWithNoEvents for proper initialization.")
	}
	if err := event.Validate(); err != nil {
		panic(err)
	}

	se.eventExecutor.RegisterScheduledEvent(scheduled.ScheduledEvent{
		At:          startTime.Add(delay),
		TargetEvent: event.MutationEvent,
	})

	se.eventExecutor.RegisterScheduledEvent(scheduled.ScheduledEvent{
		At:          startTime.Add(delay).Add(event.DurationToAssert),
		TargetEvent: event.AssertionEvent,
	})

}

// String returns the concise string representation of the event executor,
// detailing the number of scheduled events.
func (se StaticEvents) String() string {
	if se.eventExecutor == nil {
		panic("StaticEvents.eventExecutor is a nil interface; " +
			"use NewStaticEventsWithNoEvents for proper initialization.")
	}
	return se.eventExecutor.PrintEventSummary()
}

// Generate returns an eventExecutor populated with a sorted list of events. It
// is now prepared to execute events for the simulation execution.
func (se StaticEvents) Generate(
	seed int64, settings *config.SimulationSettings,
) scheduled.EventExecutor {
	if se.eventExecutor == nil {
		panic("StaticEvents.eventExecutor is a nil interface; " +
			"use NewStaticEventsWithNoEvents for proper initialization.")
	}
	return se.eventExecutor
}
