// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

// See doc.go for a description.

package fsm

import (
	"context"
)

// State is a node in a Machine's transition graph.
type State interface {
	State()
}

// ExtendedState is extra state in a Machine that does not contribute to state
// transition decisions, but that can be affected by a state transition. The
// interface is provided as part of the Args passed to Action after being
// given to a Machine during construction.
type ExtendedState interface{}

// Event is something that happens to a Machine which may or may not trigger a
// state transition.
type Event interface {
	Event()
}

// EventPayload is extra payload on an Event that does not contribute to state
// transition decisions, but that can be affected by a state transition. The
// interface is provided as part of the Args passed to Action after being
// given to a Machine during a call to ApplyWithPayload.
type EventPayload interface{}

// Args is a structure containing the arguments passed to Transition.Action.
type Args struct {
	Ctx context.Context

	Prev     State
	Extended ExtendedState

	Event   Event
	Payload EventPayload
}

// Transition is a Machine's response to an Event applied to a State. It may
// transition the machine to a new State and it may also perform an action on
// the Machine's ExtendedState.
type Transition struct {
	Next   State
	Action func(Args) error
	// Description, if set, is reflected in the DOT diagram.
	Description string
}

// TransitionNotFoundError is returned from Machine.Apply when the Event cannot
// be applied to the current State.
type TransitionNotFoundError struct {
	State State
	Event Event
}

func (e TransitionNotFoundError) Error() string {
	return "event " + eventName(e.Event) + " inappropriate in current state " + stateName(e.State)
}

// Transitions is a set of expanded state transitions generated from a Pattern,
// forming a State graph with Events acting as the directed edges between
// different States.
//
// A Transitions graph is immutable and is only useful when used to direct a
// Machine. Because of this, multiple Machines can be instantiated using the
// same Transitions graph.
type Transitions struct {
	expanded Pattern
}

// Compile creates a set of state Transitions from a Pattern. This is relatively
// expensive so it's expected that Compile is called once for each transition
// graph and assigned to a static variable. This variable can then be given to
// MakeMachine, which is cheap.
func Compile(p Pattern) Transitions {
	return Transitions{expanded: expandPattern(p)}
}

func (t Transitions) apply(a Args) (State, error) {
	sm, ok := t.expanded[a.Prev]
	if !ok {
		return a.Prev, TransitionNotFoundError{State: a.Prev, Event: a.Event}
	}
	tr, ok := sm[a.Event]
	if !ok {
		return a.Prev, TransitionNotFoundError{State: a.Prev, Event: a.Event}
	}
	if tr.Action != nil {
		if err := tr.Action(a); err != nil {
			return a.Prev, err
		}
	}
	return tr.Next, nil
}

// Machine encapsulates a State with a set of State transitions. It reacts to
// Events, adjusting its internal State according to its Transition graph and
// perforing actions on its ExtendedState accordingly.
type Machine struct {
	t   Transitions
	cur State
	es  ExtendedState
}

// MakeMachine creates a new Machine.
func MakeMachine(t Transitions, start State, es ExtendedState) Machine {
	return Machine{t: t, cur: start, es: es}
}

// Apply applies the Event to the state Machine.
func (m *Machine) Apply(ctx context.Context, e Event) error {
	return m.ApplyWithPayload(ctx, e, nil)
}

// ApplyWithPayload applies the Event to the state Machine, passing along the
// EventPayload to the state transition's Action function.
func (m *Machine) ApplyWithPayload(ctx context.Context, e Event, b EventPayload) (err error) {
	m.cur, err = m.t.apply(Args{
		Ctx:      ctx,
		Prev:     m.cur,
		Extended: m.es,
		Event:    e,
		Payload:  b,
	})
	return err
}

// CurState returns the current state.
func (m *Machine) CurState() State {
	return m.cur
}
