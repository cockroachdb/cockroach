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

package fsm

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
)

// State is a position in a Machine's transition graph.
type State interface {
	State()
}

// Event is something that happens to a Machine which may or may not trigger a
// state transition.
type Event interface {
	Event()
}

// ExtendedState is extra state in a Machine that does not contribute to state
// transition decisions, but that can be affected by a state transition.
type ExtendedState interface{}

// Transition is a Machine's response to an Event applied to a State. It may
// transition the machine to a new State and it may also perform an action on
// the Machine's ExtendedState.
type Transition struct {
	Next   State
	Action func(context.Context, ExtendedState)
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

func (t Transitions) apply(ctx context.Context, s State, e Event, es ExtendedState) State {
	sm, ok := t.expanded[s]
	if !ok {
		panic(fmt.Sprintf("unknown state %#v", s))
	}
	tr, ok := sm[e]
	if !ok {
		panic(fmt.Sprintf("unknown state transition %#v(%#v)", s, e))
	}
	if tr.Action != nil {
		tr.Action(ctx, es)
	}
	return tr.Next
}

func (t Transitions) genReport() string {
	var stateNames []string
	var eventNames []string
	stateNameMap := make(map[string]State)
	eventNameMap := make(map[string]Event)
	for s, sm := range t.expanded {
		sName := fmt.Sprintf("%#v", s)
		stateNames = append(stateNames, sName)
		stateNameMap[sName] = s

		for e := range sm {
			eName := fmt.Sprintf("%#v", e)
			if _, ok := eventNameMap[eName]; !ok {
				eventNames = append(eventNames, eName)
				eventNameMap[eName] = e
			}
		}
	}

	sort.Strings(stateNames)
	sort.Strings(eventNames)

	var b, present, missing bytes.Buffer
	for _, sName := range stateNames {
		sm := t.expanded[stateNameMap[sName]]

		for _, eName := range eventNames {
			if _, ok := sm[eventNameMap[eName]]; ok {
				fmt.Fprintf(&present, "\t\t%s\n", eName)
			} else {
				fmt.Fprintf(&missing, "\t\t%s\n", eName)
			}
		}

		fmt.Fprintf(&b, "%s\n", sName)
		b.WriteString("\thandled events:\n")
		io.Copy(&b, &present)
		b.WriteString("\tmissing events:\n")
		io.Copy(&b, &missing)

		present.Reset()
		missing.Reset()
	}

	return b.String()
}

// PrintReport prints a report of the Transitions graph, reporting on which
// Events each State handles and which Events each state does not.
func (t Transitions) PrintReport() {
	fmt.Print(t.genReport())
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
func (m *Machine) Apply(ctx context.Context, e Event) {
	m.cur = m.t.apply(ctx, m.cur, e, m.es)
}
