// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package fsm

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMatchPattern(t *testing.T) {
	// No expansion patterns.
	p1 := expandPattern(Pattern{
		state1{}: {
			event1{}: {state2{}, noAction, ""},
		},
		state2{}: {
			event1{}: {state1{}, noAction, ""},
			event2{}: {state2{}, noAction, ""},
		},
	})
	require.Equal(t, len(p1), 2)
	require.Equal(t, len(p1[state1{}]), 1)
	require.Equal(t, len(p1[state2{}]), 2)
	require.Equal(t, len(p1[state3{}]), 0)

	// State expansion match patterns.
	p2 := expandPattern(Pattern{
		state3{True}: {
			event1{}: {state2{}, noAction, ""},
		},
	})
	require.Equal(t, len(p2), 1)
	require.Equal(t, len(p2[state1{}]), 0)
	require.Equal(t, len(p2[state3{True}]), 1)
	require.Equal(t, len(p2[state3{False}]), 0)

	p3 := expandPattern(Pattern{
		state3{False}: {
			event1{}: {state2{}, noAction, ""},
		},
	})
	require.Equal(t, len(p3), 1)
	require.Equal(t, len(p3[state1{}]), 0)
	require.Equal(t, len(p3[state3{True}]), 0)
	require.Equal(t, len(p3[state3{False}]), 1)

	p4 := expandPattern(Pattern{
		state3{Any}: {
			event1{}: {state2{}, noAction, ""},
		},
	})
	require.Equal(t, len(p4), 2)
	require.Equal(t, len(p4[state1{}]), 0)
	require.Equal(t, len(p4[state3{True}]), 1)
	require.Equal(t, len(p4[state3{False}]), 1)

	p5 := expandPattern(Pattern{
		state4{Any, True}: {
			event1{}: {state2{}, noAction, ""},
		},
	})
	require.Equal(t, len(p5), 2)
	require.Equal(t, len(p5[state1{}]), 0)
	require.Equal(t, len(p5[state3{}]), 0)
	require.Equal(t, len(p5[state4{True, True}]), 1)
	require.Equal(t, len(p5[state4{True, False}]), 0)
	require.Equal(t, len(p5[state4{False, True}]), 1)
	require.Equal(t, len(p5[state4{False, False}]), 0)

	p6 := expandPattern(Pattern{
		state4{False, Any}: {
			event1{}: {state2{}, noAction, ""},
		},
	})
	require.Equal(t, len(p6), 2)
	require.Equal(t, len(p6[state1{}]), 0)
	require.Equal(t, len(p6[state3{}]), 0)
	require.Equal(t, len(p6[state4{True, True}]), 0)
	require.Equal(t, len(p6[state4{True, False}]), 0)
	require.Equal(t, len(p6[state4{False, True}]), 1)
	require.Equal(t, len(p6[state4{False, False}]), 1)

	p7 := expandPattern(Pattern{
		state4{Any, Any}: {
			event1{}: {state2{}, noAction, ""},
		},
	})
	require.Equal(t, len(p7), 4)
	require.Equal(t, len(p7[state1{}]), 0)
	require.Equal(t, len(p7[state3{}]), 0)
	require.Equal(t, len(p7[state4{True, True}]), 1)
	require.Equal(t, len(p7[state4{True, False}]), 1)
	require.Equal(t, len(p7[state4{False, True}]), 1)
	require.Equal(t, len(p7[state4{False, False}]), 1)

	// Event expansion match patterns.
	p8 := expandPattern(Pattern{
		state1{}: {
			event3{True}: {state2{}, noAction, ""},
		},
		state2{}: {
			event3{Any}: {state2{}, noAction, ""},
		},
	})
	require.Equal(t, len(p8), 2)
	require.Equal(t, len(p8[state1{}]), 1)
	require.Equal(t, len(p8[state2{}]), 2)

	p9 := expandPattern(Pattern{
		state1{}: {
			event4{True, Any}: {state2{}, noAction, ""},
		},
		state2{}: {
			event4{Any, Any}: {state2{}, noAction, ""},
		},
	})
	require.Equal(t, len(p9), 2)
	require.Equal(t, len(p9[state1{}]), 2)
	require.Equal(t, len(p9[state2{}]), 4)

	// State and Event expansion match patterns.
	p10 := expandPattern(Pattern{
		state3{Any}: {
			event4{Any, Any}: {state2{}, noAction, ""},
		},
		state4{Any, True}: {
			event3{Any}: {state2{}, noAction, ""},
		},
	})
	require.Equal(t, len(p10), 4)
	require.Equal(t, len(p10[state3{True}]), 4)
	require.Equal(t, len(p10[state3{False}]), 4)
	require.Equal(t, len(p10[state4{True, True}]), 2)
	require.Equal(t, len(p10[state4{True, False}]), 0)
	require.Equal(t, len(p10[state4{False, True}]), 2)
	require.Equal(t, len(p10[state4{False, False}]), 0)
}

func TestMatchVariableVar(t *testing.T) {
	p := expandPattern(Pattern{
		state3{Var("a")}: {
			event3{Var("b")}: {state4{Var("b"), Var("a")}, noAction, ""},
		},
	})

	require.Equal(t, len(p), 2)
	require.Equal(t, len(p[state3{True}]), 2)
	require.Equal(t, len(p[state3{False}]), 2)
	require.Equal(t, p[state3{True}][event3{True}].Next, state4{True, True})
	require.Equal(t, p[state3{True}][event3{False}].Next, state4{False, True})
	require.Equal(t, p[state3{False}][event3{True}].Next, state4{True, False})
	require.Equal(t, p[state3{False}][event3{False}].Next, state4{False, False})
}

func TestInvalidPattern(t *testing.T) {
	// Patterns not-mutually exclusive.
	require.Panics(t, func() {
		expandPattern(Pattern{
			state3{Any}: {
				event1{}: {state2{}, noAction, ""},
			},
			state3{True}: {
				event1{}: {state2{}, noAction, ""},
			},
		})
	})
	require.Panics(t, func() {
		expandPattern(Pattern{
			state4{Any, True}: {
				event1{}: {state2{}, noAction, ""},
			},
			state4{False, Any}: {
				event1{}: {state2{}, noAction, ""},
			},
		})
	})
	require.Panics(t, func() {
		expandPattern(Pattern{
			state1{}: {
				event3{Any}:  {state2{}, noAction, ""},
				event3{True}: {state2{}, noAction, ""},
			},
		})
	})
	require.Panics(t, func() {
		expandPattern(Pattern{
			state1{}: {
				event4{Any, False}: {state2{}, noAction, ""},
				event4{True, Any}:  {state2{}, noAction, ""},
			},
		})
	})

	// Same binding multiple times in pattern.
	require.Panics(t, func() {
		expandPattern(Pattern{
			state3{Var("a")}: {
				event3{Var("a")}: {state2{}, noAction, ""},
			},
		})
	})
	require.Panics(t, func() {
		expandPattern(Pattern{
			state4{Var("a"), Var("a")}: {
				event1{}: {state2{}, noAction, ""},
			},
		})
	})
	require.Panics(t, func() {
		expandPattern(Pattern{
			state1{}: {
				event4{Var("a"), Var("a")}: {state2{}, noAction, ""},
			},
		})
	})

	// Nil states and events.
	require.Panics(t, func() {
		expandPattern(Pattern{
			nil: {
				event1{}: {state1{}, noAction, ""},
			},
		})
	})
	require.Panics(t, func() {
		expandPattern(Pattern{
			state1{}: {
				nil: {state1{}, noAction, ""},
			},
		})
	})
	require.Panics(t, func() {
		expandPattern(Pattern{
			state1{}: {
				event1{}: {nil, noAction, ""},
			},
		})
	})

	// Nil variables.
	require.Panics(t, func() {
		expandPattern(Pattern{
			state3{nil}: {
				event1{}: {state1{}, noAction, ""},
			},
		})
	})
	require.Panics(t, func() {
		expandPattern(Pattern{
			state1{}: {
				event3{nil}: {state1{}, noAction, ""},
			},
		})
	})
	require.Panics(t, func() {
		expandPattern(Pattern{
			state1{}: {
				event1{}: {state3{nil}, noAction, ""},
			},
		})
	})

	// Binding not found.
	require.Panics(t, func() {
		expandPattern(Pattern{
			state1{}: {
				event3{Var("a")}: {state3{Var("b")}, noAction, ""},
			},
		})
	})

	// Wildcard in expression.
	require.Panics(t, func() {
		expandPattern(Pattern{
			state1{}: {
				event1{}: {state3{Any}, noAction, ""},
			},
		})
	})
}

func TestFromBool(t *testing.T) {
	if !FromBool(true).Get() {
		t.Fatalf("conversion failed")
	}
	if FromBool(false).Get() {
		t.Fatalf("conversion failed")
	}
}
