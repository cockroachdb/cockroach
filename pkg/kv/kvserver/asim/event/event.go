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
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/assertion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/history"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

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
	holds  bool
	reason string
}

type assertionsEvent struct {
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
		at:      start.Add(se.Delay),
		eventFn: eventFn,
	}
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

func (ag AssertionsGen) generate() assertionsEvent {
	assertionResults := make([]assertionResult, 0, len(ag.Assertions))
	var eventFn assertionFunction = func(ctx context.Context, t time.Time, h history.History) bool {
		allHolds := true
		for _, eachAssert := range ag.Assertions {
			holds, reason := eachAssert.Assert(ctx, h)
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
