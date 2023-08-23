// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package gen

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/events"
)

type EventGen interface {
	// Generate returns a list of events, which should be exectued at the delay specified.
	// TODO: figure out what seed is needed in gen here
	Generate(settings *config.SimulationSettings) *events.Executor
	String() string /**/
}

// StaticEvents implements the EventGen interface.
// TODO(kvoli): introduce conditional events.
type StaticEvents struct {
	StateChangeEvents               []events.StateChangeEventGen
	StateChangeWithAssertionsEvents []events.StateChangeEventWithAssertionGen
	AssertionsEvents                []events.AssertionsGen
}

func EmptyStaticEvents() StaticEvents {
	return StaticEvents{
		StateChangeEvents:               []events.StateChangeEventGen{},
		StateChangeWithAssertionsEvents: []events.StateChangeEventWithAssertionGen{},
		AssertionsEvents:                []events.AssertionsGen{},
	}
}

func (se *StaticEvents) AddStateChangeEventGen(event events.StateChangeEventGen) {
	se.StateChangeEvents = append(se.StateChangeEvents, event)
}

func (se *StaticEvents) AddStateChangeEventWithAssertionGen(
	event events.StateChangeEventWithAssertionGen,
) {
	se.StateChangeWithAssertionsEvents = append(se.StateChangeWithAssertionsEvents, event)
}

func (se *StaticEvents) AddAssertionsGen(event events.AssertionsGen) {
	se.AssertionsEvents = append(se.AssertionsEvents, event)
}

func (se StaticEvents) String() string {
	return fmt.Sprintf(
		"state change events=%d, state change events with assertions=%d, assertion events=%d",
		len(se.StateChangeEvents), len(se.StateChangeWithAssertionsEvents), len(se.AssertionsEvents))
}

// Generate returns a list of events, exactly the same as the events
// StaticEvents was created with.
func (se StaticEvents) Generate(settings *config.SimulationSettings) *events.Executor {
	executor := events.Executor{}
	executor.RegisterStateChangeEvents(settings.StartTime, se.StateChangeEvents)
	executor.RegisterStateChangeEventWithAssertions(settings.StartTime, se.StateChangeWithAssertionsEvents)
	executor.RegisterAssertionEvents(se.AssertionsEvents)
	executor.Start()
	return &executor
}
