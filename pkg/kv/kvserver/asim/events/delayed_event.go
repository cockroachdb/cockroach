// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package events

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/assertion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/history"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
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

// Executor
// Register Event
// Event can also register assertion
// Assertion output to be part of the assertion itself
// Event can register the assertion by passing in its id
// Event type struct -> event

// implement string as well
//type delayedEvent struct {
//	getAt      time.Time
//	eventFn func(context.Context, time.Time, state.State)
//}

// delayed event -> creates a pointer assertion and hold a pointer of it

// event -> register the event (get ID) -> register assertion event

type eventFunction func(context.Context, time.Time, *state.State)
type assertionFunction func(context.Context, time.Time, history.History) (allHolds bool)

type delayedEventInterface interface {
	getAt() time.Time
	getEventFn() interface{}
}

type delayedEvent struct {
	at      time.Time
	eventFn interface{}
}

func (de delayedEvent) getAt() time.Time {
	return de.at
}
func (de delayedEvent) getEventFn() interface{} {
	return de.eventFn
}

func TestNoopEventExecutor() *Executor {
	return &Executor{
		delayedEvents: delayedEventList{},
	}
}

// event: getAt, eventFn
// Event: interface1
// Event with assertion: interface2 -> stores a struct to another assertion struct type
// Sub-type events
// interface: event, event with assertions, assertions itself -> []delayedEvent
// Register event: return delayedEvent
// smaller event type struct could be called to generate this actual event struct (getAt, delayFn)
// getAt, delayedFn, assertion struct
// getAt, delayedFn, assertionOutput
// add comment on use start() before executing it
// []delayedlist
type Executor struct {
	delayedEvents delayedEventList
	//// delayed event 1. tyoe one delayed event type 2. assertion event one implements the
	//results                map[assertionCheckerID]assertionResult // assertion -> assertion result
	//assertionCheckerSeqGen assertionCheckerID
}

func (e *Executor) Start() {
	// order delayed event
	sort.Sort(e.delayedEvents)
}

func (e *Executor) TickEvents(
	ctx context.Context, tick time.Time, state *state.State, history history.History,
) (failureExists bool) {
	var idx int
	// Assume the events are in sorted order and the event list is never added
	// to.
	for i := range e.delayedEvents {
		if !tick.Before(e.delayedEvents[i].getAt()) {
			idx = i + 1
			log.Infof(ctx, "applying event (scheduled=%s tick=%s)", e.delayedEvents[i].getAt, tick)
			switch fn := e.delayedEvents[i].getEventFn().(type) {
			case eventFunction:
				fn(ctx, tick, state)
			case assertionFunction:
				if allHolds := fn(ctx, tick, history); !allHolds {
					failureExists = true
				}
			default:
				panic("unexpected function type")
			}
		} else {
			break
		}
	}
	if idx != 0 {
		e.delayedEvents = e.delayedEvents[idx:]
	}
	return failureExists
}

type StateChangeEventGen interface {
	// call back to register for assertion if needed? a
	generate(start time.Time) delayedEvent
}

var _ StateChangeEventGen = &SetSpanConfigEvent{}
var _ StateChangeEventGen = &AddNodeEvent{}
var _ StateChangeEventGen = &SetNodeLivenessEvent{}
var _ StateChangeEventGen = &SetCapacityOverrideEvent{}

type SetSpanConfigEvent struct {
	Delay  time.Duration
	Span   roachpb.Span
	Config roachpb.SpanConfig
}

func (se SetSpanConfigEvent) generate(start time.Time) delayedEvent {
	var eventFn eventFunction = func(ctx context.Context, t time.Time, s *state.State) {
		(*s).SetSpanConfig(se.Span, se.Config)
	}
	return delayedEvent{
		at:      start.Add(se.Delay),
		eventFn: eventFn,
	}
}

type AddNodeEvent struct {
	Delay          time.Duration
	NumStores      int
	LocalityString string
}

func (ae AddNodeEvent) generate(start time.Time) delayedEvent {
	var eventFn eventFunction = func(ctx context.Context, tick time.Time, s *state.State) {
		node := (*s).AddNode()
		if ae.LocalityString != "" {
			var locality roachpb.Locality
			if err := locality.Set(ae.LocalityString); err != nil {
				panic(fmt.Sprintf("unable to set node locality %s", err.Error()))
			}
			(*s).SetNodeLocality(node.NodeID(), locality)
		}
		for i := 0; i < ae.NumStores; i++ {
			if _, ok := (*s).AddStore(node.NodeID()); !ok {
				panic(fmt.Sprintf("adding store to node=%d failed", node))
			}
		}
	}
	return delayedEvent{
		at:      start.Add(ae.Delay),
		eventFn: eventFn,
	}
}

type SetNodeLivenessEvent struct {
	Delay          time.Duration
	NodeId         state.NodeID
	LivenessStatus livenesspb.NodeLivenessStatus
}

func (sne SetNodeLivenessEvent) generate(start time.Time) delayedEvent {
	var eventFn eventFunction = func(ctx context.Context, tick time.Time, s *state.State) {
		(*s).SetNodeLiveness(
			sne.NodeId,
			sne.LivenessStatus,
		)
	}
	return delayedEvent{
		at:      start.Add(sne.Delay),
		eventFn: eventFn,
	}
}

type SetCapacityOverrideEvent struct {
	Delay            time.Duration
	StoreID          state.StoreID
	CapacityOverride state.CapacityOverride
}

func (sce SetCapacityOverrideEvent) generate(start time.Time) delayedEvent {
	var eventFn eventFunction = func(ctx context.Context, tick time.Time, s *state.State) {
		log.Infof(ctx, "setting capacity override %+v", sce.CapacityOverride)
		(*s).SetCapacityOverride(sce.StoreID, sce.CapacityOverride)
	}
	return delayedEvent{
		at:      start.Add(sce.Delay),
		eventFn: eventFn,
	}
}

type SetSpanConfigEventWithAssertions struct {
	SetSpanConfigEvent
	assertions []assertion.SimulationAssertion
}

type AssertionsGen struct {
	At         time.Time
	Assertions []assertion.SimulationAssertion
}

type StateChangeEventWithAssertionGen struct {
	StateChangeEvent StateChangeEventGen
	DurationToAssert time.Duration
	Assertions       []assertion.SimulationAssertion
}

type assertionResult struct {
	holds  bool
	reason string
}

type assertionsEvent struct {
	delayedEvent
	result *[]assertionResult // populated during event
}

func (ag AssertionsGen) generate() assertionsEvent {
	assertionResults := make([]assertionResult, 0, len(ag.Assertions))
	var eventFn assertionFunction = func(ctx context.Context, t time.Time, h history.History) bool {
		allHolds := true
		for _, assertion := range ag.Assertions {
			holds, reason := assertion.Assert(ctx, h)
			assertionResults = append(assertionResults, assertionResult{
				holds, reason,
			})
			if !holds {
				allHolds = false
			}
		}
		return allHolds
	}
	return assertionsEvent{
		delayedEvent: delayedEvent{
			at:      ag.At,
			eventFn: eventFn,
		},
		result: &assertionResults,
	}
}

func (e *Executor) registerStateChangeEvent(
	start time.Time, eventGen StateChangeEventGen,
) time.Time {
	eventGenerated := eventGen.generate(start)
	e.delayedEvents = append(e.delayedEvents, eventGenerated)
	return eventGenerated.at
}

// delya
// i think we can check for failure during tickEvents and early terminate in the for loop if assertion failure exits
func (e *Executor) RegisterStateChangeEvents(start time.Time, events []StateChangeEventGen) {
	// they will in turn register assertion events as well
	// based on the struct events registered interface{}
	for _, event := range events {
		e.registerStateChangeEvent(start, event)
	}
}

func (e *Executor) OutputEventsInChronologicalOrder() string {
	return ""
}

func (e *Executor) registerAssertionEvent(assertionsGen AssertionsGen) {
	eventGenerated := assertionsGen.generate()
	e.delayedEvents = append(e.delayedEvents, eventGenerated)
}

func (e *Executor) RegisterAssertionEvents(events []AssertionsGen) {
	for _, event := range events {
		e.registerAssertionEvent(event)
	}
}
func (e *Executor) registerStateChangeEventWithAssertions(
	start time.Time, eventGen StateChangeEventWithAssertionGen,
) {
	// generate a ticket id for this event and store it in the mpa
	// they will in turn register assertion events as well
	// based on the struct events registered interface{}
	// take in assertion type and the time to check for assertion
	// save assertion output in executor

	at := e.registerStateChangeEvent(start, eventGen.StateChangeEvent)
	e.registerAssertionEvent(
		AssertionsGen{
			At:         at.Add(eventGen.DurationToAssert),
			Assertions: eventGen.Assertions,
		})
}

// i need to have some sort of data structure to couple the state change event and the assertion event in the executor to format better output
// id -> state change event, assertion event maybe just a slice is enough? but printing needs chronological order
// so the conclusion is we will order the events in chronological order but not guanranteed to be coupled together
func (e *Executor) RegisterStateChangeEventWithAssertions(
	start time.Time, events []StateChangeEventWithAssertionGen,
) {
	// they will in turn register assertion events as well
	// based on the struct events registered interface{}
	for _, event := range events {
		e.registerStateChangeEventWithAssertions(start, event)
	}
}

// On the outside, we have an event executor and register the events where mutation event can also register assertion events

// Two types of event: register events register assertions
// Events -> can register assertions -> for each assertion, I should have an output result struct
// []Assertion Output:
// Event maps to all of its assertion during registration referencing the slice (map)
// Each assertion event should populate the output result later on/
// Store one list of delayed events which mix events and assertions

// EventGen provides a  method to generate a list of events that will apply to
// the simulated cluster. Currently, only delayed (fixed time) events are
// supported.
