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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type delayedEvent struct {
	at       time.Time
	eventFn  interface{}
	eventStr string
}

func (de delayedEvent) getAt() time.Time {
	return de.at
}
func (de delayedEvent) getEventFn() interface{} {
	return de.eventFn
}
func (de delayedEvent) printDelayedEvent() string {
	return de.eventStr
}

type StateChangeEventGen interface {
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

type AddNodeEvent struct {
	Delay          time.Duration
	NumStores      int
	LocalityString string
}

type SetNodeLivenessEvent struct {
	Delay          time.Duration
	NodeId         state.NodeID
	LivenessStatus livenesspb.NodeLivenessStatus
}

type SetCapacityOverrideEvent struct {
	Delay            time.Duration
	StoreID          state.StoreID
	CapacityOverride state.CapacityOverride
}

type StateChangeEventWithAssertionGen struct {
	StateChangeEvent StateChangeEventGen
	DurationToAssert time.Duration
	Assertions       []assertion.SimulationAssertion
}

type assertionResult struct {
	assertionStr string
	holds        bool
	reason       string
}

func (ar assertionResult) string() string {
	if ar.holds {
		return fmt.Sprintf("%s passes", ar.assertionStr)
	} else {
		return fmt.Sprintf("%s fails: %s", ar.assertionStr, ar.reason)
	}
}

type assertionEvent struct {
	delayedEvent
	result *[]assertionResult // populated during event
}

type AssertionsGen struct {
	At         time.Time
	Assertions []assertion.SimulationAssertion
}

func (se SetSpanConfigEvent) generate(start time.Time) delayedEvent {
	var eventFn eventFunction = func(ctx context.Context, t time.Time, s *state.State) {
		(*s).SetSpanConfig(se.Span, se.Config)
	}
	return delayedEvent{
		at:       start.Add(se.Delay),
		eventFn:  eventFn,
		eventStr: se.string(),
	}
}

func (se SetSpanConfigEvent) string() string {
	return fmt.Sprintf("set span config event scheduled with delay=%s: span=%s, config=%s", se.Delay, se.Span.String(), se.Config.String())
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
		at:       start.Add(ae.Delay),
		eventFn:  eventFn,
		eventStr: ae.string(),
	}
}

func (ae AddNodeEvent) string() string {
	return fmt.Sprintf("add node event scheduled with delay=%s: num_of_stores=%d, locality_string=%s", ae.Delay, ae.NumStores, ae.LocalityString)
}

func (sne SetNodeLivenessEvent) generate(start time.Time) delayedEvent {
	var eventFn eventFunction = func(ctx context.Context, tick time.Time, s *state.State) {
		(*s).SetNodeLiveness(
			sne.NodeId,
			sne.LivenessStatus,
		)
	}
	return delayedEvent{
		at:       start.Add(sne.Delay),
		eventFn:  eventFn,
		eventStr: sne.string(),
	}
}

func (ae SetNodeLivenessEvent) string() string {
	return fmt.Sprintf("set node liveness event scheduled with delay=%s: nodeID=%d, liveness_status=%s", ae.Delay, ae.NodeId, ae.LivenessStatus.String())
}

func (sce SetCapacityOverrideEvent) generate(start time.Time) delayedEvent {
	var eventFn eventFunction = func(ctx context.Context, tick time.Time, s *state.State) {
		log.Infof(ctx, "setting capacity override %+v", sce.CapacityOverride)
		(*s).SetCapacityOverride(sce.StoreID, sce.CapacityOverride)
	}
	return delayedEvent{
		at:       start.Add(sce.Delay),
		eventFn:  eventFn,
		eventStr: sce.string(),
	}
}

func (sce SetCapacityOverrideEvent) string() string {
	return fmt.Sprintf("set capacity override event scheduled with delay=%s: storeID=%d, capacity_override=%s", sce.Delay, sce.StoreID, sce.CapacityOverride.String())
}

func (ag AssertionsGen) generate() assertionEvent {
	assertionResults := make([]assertionResult, 0, len(ag.Assertions))
	var eventFn assertionFunction = func(ctx context.Context, t time.Time, h history.History) bool {
		allHolds := true
		for _, eachAssert := range ag.Assertions {
			holds, reason := eachAssert.Assert(ctx, h)
			assertionResults = append(assertionResults, assertionResult{
				eachAssert.String(), holds, reason,
			})
			if !holds {
				allHolds = false
			}
		}
		return allHolds
	}
	return assertionEvent{
		delayedEvent: delayedEvent{
			at:      ag.At,
			eventFn: eventFn,
		},
		result: &assertionResults,
	}
}

func (ag assertionEvent) printDelayedEvent() string {
	if ag.delayedEvent.eventStr != "" {
		panic("unexpected assertion string format")
	}
	if ag.result == nil {
		panic("unexpected nil")
	}
	if len(*ag.result) == 0 {
		return "unexpected: assertions were not executed. could be because the test duration was too small to get to it"
	}

	buf := &strings.Builder{}
	buf.WriteString(fmt.Sprintf("assertion events scheduled at %s:\n", ag.at))
	for _, assertionResult := range *ag.result {
		buf.WriteString(fmt.Sprintf("\t%s\n", assertionResult.string()))
	}
	return buf.String()
}
