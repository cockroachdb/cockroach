// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scheduled

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/event"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/history"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// EventExecutor is the exported interface for eventExecutor, responsible for
// managing scheduled events, allowing event registration, tick-based
// triggering. Please use NewExecutorWithNoEvents for proper initialization.
type EventExecutor interface {
	// RegisterScheduledEvent registers an event to be executed as part of
	// eventExecutor.
	RegisterScheduledEvent(ScheduledEvent)
	// TickEvents retrieves and invokes the underlying event function from the
	// scheduled events at the given tick. It returns a boolean indicating if any
	// assertion event failed during the tick, allowing for early exit.
	TickEvents(context.Context, types.Tick, state.State, history.History) bool
	// PrintEventSummary returns a string summarizing the executed mutation and
	// assertion events.
	PrintEventSummary(tag string) string
	// PrintEventsExecuted returns a detailed string representation of executed
	// events including details of mutation events, assertion checks, and assertion
	// results.
	PrintEventsExecuted() string
}

// eventExecutor is the private implementation of the EventExecutor interface,
// maintaining a list of scheduled events and an index for the next event to be
// executed.
type eventExecutor struct {
	// scheduledEvents represent events scheduled to be executed in the
	// simulation.
	scheduledEvents ScheduledEventList
	// hasStarted represent if the eventExecutor has begun execution and whether
	// event sorting is required during TickEvents.
	hasStarted bool
	// nextEventIndex represents the index of the next event to execute in
	// scheduledEvents.
	nextEventIndex int
}

// NewExecutorWithNoEvents returns the exported interface.
func NewExecutorWithNoEvents() EventExecutor {
	return newExecutorWithNoEvents()
}

// newExecutorWithNoEvents returns the actual implementation of the
// EventExecutor interface.
func newExecutorWithNoEvents() *eventExecutor {
	return &eventExecutor{
		scheduledEvents: ScheduledEventList{},
	}
}

// PrintEventSummary returns a string summarizing the executed mutation and
// assertion events.
func (e *eventExecutor) PrintEventSummary(tag string) string {
	mutationEvents, assertionEvents := 0, 0
	var buf strings.Builder
	sort.Sort(e.scheduledEvents)
	for i, ev := range e.scheduledEvents {
		_, _ = fmt.Fprintf(&buf, "%v", ev.StringWithTag(tag))
		if i != len(e.scheduledEvents)-1 {
			_, _ = fmt.Fprintf(&buf, "\n")
		}
		if ev.IsMutationEvent() {
			mutationEvents++
		} else {
			assertionEvents++
		}
	}
	return buf.String()
}

// PrintEventsExecuted returns a detailed string representation of executed
// events including details of mutation events, assertion checks, and assertion
// results.
// For example,
// 2 events scheduled:
//
//	executed at: 2006-01-02 15:04:05
//		event: add node event
//	executed at: 2006-01-02 15:04:05
//		event: assertion checking event
//			1.assertion=
//			result=
//			2.assertion=
//			result
func (e *eventExecutor) PrintEventsExecuted() string {
	if e.scheduledEvents == nil {
		panic("unexpected")
	}
	if len(e.scheduledEvents) == 0 {
		return "no events were scheduled"
	} else {
		buf := strings.Builder{}
		buf.WriteString(fmt.Sprintf("%d events executed:\n", len(e.scheduledEvents)))
		for i, event := range e.scheduledEvents {
			_, _ = fmt.Fprintf(&buf, "%v", event.StringWithTag(""))
			if i != len(e.scheduledEvents)-1 {
				_, _ = fmt.Fprintf(&buf, "\n")
			}
		}
		return buf.String()
	}
}

// TickEvents retrieves and invokes the underlying event function from the
// scheduled events at the given tick. It returns a boolean indicating if any
// assertion event failed during the tick, allowing for early exit.
func (e *eventExecutor) TickEvents(
	ctx context.Context, tick types.Tick, state state.State, history history.History,
) (failureExists bool) {
	// Sorts the scheduled list in chronological to initiate event execution.
	if !e.hasStarted {
		sort.Sort(e.scheduledEvents)
		e.hasStarted = true
	}
	// Assume the events are in sorted order and the event list is never added
	// to.
	for e.nextEventIndex < len(e.scheduledEvents) {
		if !tick.WallTime().Before(e.scheduledEvents[e.nextEventIndex].At) {
			log.KvDistribution.Infof(ctx, "applying event (scheduled=%s tick=%s)", e.scheduledEvents[e.nextEventIndex].At, tick)
			scheduledEvent := e.scheduledEvents[e.nextEventIndex]
			fn := scheduledEvent.TargetEvent.Func()
			if scheduledEvent.IsMutationEvent() {
				mutationFn, ok := fn.(event.MutationFunc)
				if ok {
					mutationFn(ctx, state)
				} else {
					panic("expected mutation type to hold mutationFunc but found something else")
				}
			} else {
				assertionFn, ok := fn.(event.AssertionFunc)
				if ok {
					if !assertionFn(ctx, tick.WallTime(), history) && !failureExists {
						failureExists = true
					}
				} else {
					panic("expected assertion type to hold assertionFunc but found something else")
				}
			}
			e.nextEventIndex++
		} else {
			break
		}
	}
	return failureExists
}

// RegisterScheduledEvent registers an event to be executed as part of
// eventExecutor.
func (e *eventExecutor) RegisterScheduledEvent(scheduledEvent ScheduledEvent) {
	e.scheduledEvents = append(e.scheduledEvents, scheduledEvent)
}
