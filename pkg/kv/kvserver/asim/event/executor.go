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
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/history"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type delayedEventList []delayedEventInterface

// Len implements sort.Interface.
func (del delayedEventList) Len() int { return len(del) }

// Less implements sort.Interface.
func (del delayedEventList) Less(i, j int) bool {
	if del[i].getAt() == del[j].getAt() {
		return i < j
	}
	return del[i].getAt().Before(del[j].getAt())
}

// Swap implements sort.Interface.
func (del delayedEventList) Swap(i, j int) {
	del[i], del[j] = del[j], del[i]
}

type delayedEventInterface interface {
	getAt() time.Time
	getEventFn() interface{}
	printDelayedEvent() string
}

type eventFunction func(context.Context, time.Time, *state.State)
type assertionFunction func(context.Context, time.Time, history.History) (allHolds bool)

type executor struct {
	nextEventIndex int
	delayedEvents  delayedEventList
}

type Executor interface {
	PrintEventsExecuted() string
	RegisterAssertionEvents(events []AssertionsGen)
	RegisterStateChangeEvents(start time.Time, events []StateChangeEventGen)
	RegisterStateChangeEventWithAssertions(
		start time.Time, events []StateChangeEventWithAssertionGen,
	)
	Start()
	TickEvents(
		ctx context.Context, tick time.Time, state *state.State, history history.History,
	) (failureExists bool)
}

func NewExecutorWithNoEvents() Executor {
	return newExecutorWithNoEvents()
}

func newExecutorWithNoEvents() *executor {
	return &executor{
		nextEventIndex: 0,
		delayedEvents:  delayedEventList{},
	}
}

func (e *executor) Start() {
	// order delayed event
	sort.Sort(e.delayedEvents)
}

func (e *executor) PrintEventsExecuted() string {
	if e.delayedEvents == nil {
		panic("unexpected")
	}
	if len(e.delayedEvents) == 0 {
		return fmt.Sprintln("no delayed events were executed")
	} else {
		buf := strings.Builder{}
		buf.WriteString(fmt.Sprintln("delayed events executed:"))
		for _, event := range e.delayedEvents {
			buf.WriteString(fmt.Sprintln(event.printDelayedEvent()))
		}
		return buf.String()
	}
}

func (e *executor) TickEvents(
	ctx context.Context, tick time.Time, state *state.State, history history.History,
) (failureExists bool) {
	// Assume the events are in sorted order and the event list is never added
	// to.
	for e.nextEventIndex < len(e.delayedEvents) {
		if !tick.Before(e.delayedEvents[e.nextEventIndex].getAt()) {
			log.Infof(ctx, "applying event (scheduled=%s tick=%s)", e.delayedEvents[e.nextEventIndex].getAt(), tick)
			switch fn := e.delayedEvents[e.nextEventIndex].getEventFn().(type) {
			case eventFunction:
				fn(ctx, tick, state)
			case assertionFunction:
				if !fn(ctx, tick, history) {
					failureExists = true
				}
			default:
				panic("unexpected function type")
			}
			e.nextEventIndex++
		} else {
			break
		}
	}
	return failureExists
}

func (e *executor) registerStateChangeEvent(
	start time.Time, eventGen StateChangeEventGen,
) time.Time {
	eventGenerated := eventGen.generate(start)
	e.delayedEvents = append(e.delayedEvents, eventGenerated)
	return eventGenerated.at
}

func (e *executor) RegisterStateChangeEvents(start time.Time, events []StateChangeEventGen) {
	for _, event := range events {
		e.registerStateChangeEvent(start, event)
	}
}

func (e *executor) registerAssertionEvent(assertionsGen AssertionsGen) {
	eventGenerated := assertionsGen.generate()
	e.delayedEvents = append(e.delayedEvents, eventGenerated)
}

func (e *executor) RegisterAssertionEvents(events []AssertionsGen) {
	for _, event := range events {
		e.registerAssertionEvent(event)
	}
}

func (e *executor) registerStateChangeEventWithAssertions(
	start time.Time, eventGen StateChangeEventWithAssertionGen,
) {
	at := e.registerStateChangeEvent(start, eventGen.StateChangeEvent)
	e.registerAssertionEvent(
		AssertionsGen{
			At:         at.Add(eventGen.DurationToAssert),
			Assertions: eventGen.Assertions,
		},
	)
}

func (e *executor) RegisterStateChangeEventWithAssertions(
	start time.Time, events []StateChangeEventWithAssertionGen,
) {
	for _, event := range events {
		e.registerStateChangeEventWithAssertions(start, event)
	}
}
