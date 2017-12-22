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
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type state1 struct{}
type state2 struct{}
type state3 struct {
	Field Bool
}
type state4 struct {
	Field1 Bool
	Field2 Bool
}

func (state1) State() {}
func (state2) State() {}
func (state3) State() {}
func (state4) State() {}

type event1 struct{}
type event2 struct{}
type event3 struct {
	Field Bool
}
type event4 struct {
	Field1 Bool
	Field2 Bool
}

func (event1) Event() {}
func (event2) Event() {}
func (event3) Event() {}
func (event4) Event() {}

var noAction func(context.Context, ExtendedState)

func TestBasicTransitions(t *testing.T) {
	ctx := context.Background()
	trans := Compile(Pattern{
		state1{}: {
			event1{}: {state2{}, noAction},
			event2{}: {state1{}, noAction},
		},
		state2{}: {
			event1{}: {state1{}, noAction},
			event2{}: {state2{}, noAction},
		},
	})

	// Valid transitions.
	require.Equal(t, trans.apply(ctx, state1{}, event1{}, nil), state2{})
	require.Equal(t, trans.apply(ctx, state1{}, event2{}, nil), state1{})
	require.Equal(t, trans.apply(ctx, state2{}, event1{}, nil), state1{})
	require.Equal(t, trans.apply(ctx, state2{}, event2{}, nil), state2{})

	// Invalid transitions.
	require.Panics(t, func() { trans.apply(ctx, state3{}, event1{}, nil) })
	require.Panics(t, func() { trans.apply(ctx, state1{}, event3{}, nil) })
}

func TestTransitionActions(t *testing.T) {
	var extendedState int
	ctx := context.Background()
	trans := Compile(Pattern{
		state1{}: {
			event1{}: {state2{}, func(ctx context.Context, e ExtendedState) { *e.(*int) = 1 }},
			event2{}: {state1{}, func(ctx context.Context, e ExtendedState) { *e.(*int) = 2 }},
		},
		state2{}: {
			event1{}: {state1{}, func(ctx context.Context, e ExtendedState) { *e.(*int) = 3 }},
			event2{}: {state2{}, func(ctx context.Context, e ExtendedState) { *e.(*int) = 4 }},
		},
	})

	trans.apply(ctx, state1{}, event1{}, &extendedState)
	require.Equal(t, extendedState, 1)

	trans.apply(ctx, state1{}, event2{}, &extendedState)
	require.Equal(t, extendedState, 2)

	trans.apply(ctx, state2{}, event1{}, &extendedState)
	require.Equal(t, extendedState, 3)

	trans.apply(ctx, state2{}, event2{}, &extendedState)
	require.Equal(t, extendedState, 4)
}

func TestTransitionsWithWildcards(t *testing.T) {
	ctx := context.Background()
	trans := Compile(Pattern{
		state3{Any}: {
			event3{Any}: {state1{}, noAction},
		},
	})

	require.Equal(t, trans.apply(ctx, state3{True}, event3{True}, nil), state1{})
	require.Equal(t, trans.apply(ctx, state3{True}, event3{False}, nil), state1{})
	require.Equal(t, trans.apply(ctx, state3{False}, event3{True}, nil), state1{})
	require.Equal(t, trans.apply(ctx, state3{False}, event3{False}, nil), state1{})
}

func TestTransitionsWithVarBindings(t *testing.T) {
	ctx := context.Background()
	trans := Compile(Pattern{
		state3{Var("a")}: {
			event3{Var("b")}: {state4{Var("b"), Var("a")}, noAction},
		},
	})

	require.Equal(t, trans.apply(ctx, state3{True}, event3{True}, nil), state4{True, True})
	require.Equal(t, trans.apply(ctx, state3{True}, event3{False}, nil), state4{False, True})
	require.Equal(t, trans.apply(ctx, state3{False}, event3{True}, nil), state4{True, False})
	require.Equal(t, trans.apply(ctx, state3{False}, event3{False}, nil), state4{False, False})
}

func BenchmarkPatternCompilation(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = Compile(Pattern{
			state1{}: {
				event4{True, Any}:  {state2{}, noAction},
				event4{False, Any}: {state1{}, noAction},
			},
			state2{}: {
				event4{Any, Any}: {state2{}, noAction},
			},
			state3{True}: {
				event1{}: {state1{}, noAction},
			},
			state3{False}: {
				event3{True}:  {state2{}, noAction},
				event3{False}: {state1{}, noAction},
			},
			state4{Var("x"), Var("y")}: {
				event4{True, True}:   {state1{}, noAction},
				event4{True, False}:  {state2{}, noAction},
				event4{False, True}:  {state3{Var("x")}, noAction},
				event4{False, False}: {state4{Var("y"), Var("x")}, noAction},
			},
		})
	}
}

func BenchmarkStateTransition(b *testing.B) {
	var extendedState int
	ctx := context.Background()
	trans := Compile(Pattern{
		state1{}: {
			event1{}: {state2{}, func(ctx context.Context, e ExtendedState) { *e.(*int) = 1 }},
			event2{}: {state1{}, func(ctx context.Context, e ExtendedState) { *e.(*int) = 2 }},
		},
		state2{}: {
			event1{}: {state1{}, func(ctx context.Context, e ExtendedState) { *e.(*int) = 3 }},
			event2{}: {state2{}, func(ctx context.Context, e ExtendedState) { *e.(*int) = 4 }},
		},
		// Unused, but complicates transition graph. Demonstrates that a more
		// complicated graph does not hurt runtime performance.
		state3{True}: {
			event1{}: {state1{}, noAction},
		},
		state3{False}: {
			event3{True}:  {state2{}, noAction},
			event3{False}: {state1{}, noAction},
		},
		state4{Var("x"), Var("y")}: {
			event4{True, True}:   {state1{}, noAction},
			event4{True, False}:  {state2{}, noAction},
			event4{False, True}:  {state3{Var("x")}, noAction},
			event4{False, False}: {state4{Var("y"), Var("x")}, noAction},
		},
	})
	m := MakeMachine(trans, state1{}, &extendedState)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e := Event(event1{})
		if i%2 == 1 {
			e = event2{}
		}
		m.Apply(ctx, e)
	}
}
