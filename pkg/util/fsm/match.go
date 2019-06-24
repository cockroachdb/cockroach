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
	"fmt"
	"reflect"
)

var (
	// True is a pattern that matches true booleans.
	True Bool = b(true)
	// False is a pattern that matches false booleans.
	False Bool = b(false)
	// Any is a pattern that matches any value.
	Any = Var("")
)

// Bool represents a boolean pattern.
type Bool interface {
	bool()
	// Get returns the value of a Bool.
	Get() bool
}

// FromBool creates a Bool from a Go bool.
func FromBool(val bool) Bool {
	return b(val)
}

type b bool

// Var allows variables to be bound to names and used in the match expression.
// If the variable binding name is the empty string, it acts as a wildcard but
// does not add any variable to the expression scope.
type Var string

func (b) bool()       {}
func (x b) Get() bool { return bool(x) }
func (Var) bool()     {}

// Get is part of the Bool interface.
func (Var) Get() bool { panic("can't call get on Var") }

// Pattern is a mapping from (State,Event) pairs to Transitions. When
// unexpanded, it may contain values like wildcards and variable bindings.
type Pattern map[State]map[Event]Transition

// expandPattern expands the States and Events in a Pattern to produce a new
// Pattern with no wildcards or variable bindings. For example:
//
//   Pattern{
//       state3{Any}: {
//           event1{}: {state2{}, ...},
//       },
//       state1{}: {
//           event4{Any, Var("x")}: {state3{Var("x")}, ...},
//       },
//   }
//
// is expanded to:
//
//   Pattern{
//       state3{False}: {
//           event1{}: {state2{}, ...},
//       },
//       state3{True}: {
//           event1{}: {state2{}, ...},
//       },
//       state1{}: {
//           event4{False, False}: {state3{False}, ...},
//           event4{False, True}:  {state3{True},  ...},
//           event4{True, False}:  {state3{False}, ...},
//           event4{True, True}:   {state3{True},  ...},
//       },
//   }
//
func expandPattern(p Pattern) Pattern {
	xp := make(Pattern)
	for s, sm := range p {
		sVars := expandState(s)
		for _, sVar := range sVars {
			xs := sVar.v.Interface().(State)

			xsm := xp[xs]
			if xsm == nil {
				xsm = make(map[Event]Transition)
				xp[xs] = xsm
			}

			for e, t := range sm {
				eVars := expandEvent(e)
				for _, eVar := range eVars {
					xe := eVar.v.Interface().(Event)
					if _, ok := xsm[xe]; ok {
						panic("match patterns overlap")
					}

					scope := mergeScope(sVar.scope, eVar.scope)
					xsm[xe] = Transition{
						Next:        bindState(t.Next, scope),
						Action:      t.Action,
						Description: t.Description,
					}
				}
			}
		}
	}
	return xp
}

type bindings map[string]reflect.Value
type expandedVar struct {
	v     reflect.Value
	scope bindings
}

func expandState(s State) []expandedVar {
	if s == nil {
		panic("found nil state")
	}
	return expandVar(reflect.ValueOf(s))
}
func expandEvent(e Event) []expandedVar {
	if e == nil {
		panic("found nil event")
	}
	return expandVar(reflect.ValueOf(e))
}

// expand expands all wildcards in the provided value.
func expandVar(v reflect.Value) []expandedVar {
	for i := 0; i < v.NumField(); i++ {
		f := v.Field(i).Interface()
		switch t := f.(type) {
		case nil:
			panic("found nil field in match pattern")
		case Bool:
			switch bt := t.(type) {
			case b:
				// Can't expand.
			case Var:
				var xPats []expandedVar
				for _, xVal := range expandBool(v, i) {
					// xVal has its ith field set to a concrete value. We then
					// recurse to expand any other wildcards or bindings.
					recXPats := expandVar(xVal)
					if bt != Any {
						for j, rexXPat := range recXPats {
							recXPats[j].scope = mergeScope(
								rexXPat.scope,
								bindings{string(bt): xVal.Field(i)},
							)
						}
					}
					xPats = append(xPats, recXPats...)
				}
				return xPats
			default:
				panic("unexpected Bool variant")
			}
		default:
			// Can't expand.
		}
	}
	return []expandedVar{{v: v}}
}

func expandBool(v reflect.Value, field int) []reflect.Value {
	vTrue := reflect.New(v.Type()).Elem()
	vTrue.Set(v)
	vTrue.Field(field).Set(reflect.ValueOf(True))

	vFalse := reflect.New(v.Type()).Elem()
	vFalse.Set(v)
	vFalse.Field(field).Set(reflect.ValueOf(False))

	return []reflect.Value{vTrue, vFalse}
}

func bindState(s State, scope bindings) State {
	if s == nil {
		panic("found nil state")
	}
	xS := bindVar(reflect.ValueOf(s), scope)
	return xS.Interface().(State)
}

// bindVar binds all variables in the provided value based on the variables in
// the scope.
func bindVar(v reflect.Value, scope bindings) reflect.Value {
	newV := reflect.New(v.Type()).Elem()
	newV.Set(v)
	for i := 0; i < newV.NumField(); i++ {
		f := newV.Field(i).Interface()
		switch t := f.(type) {
		case nil:
			panic("found nil field in match expr")
		case Bool:
			switch bt := t.(type) {
			case b:
				// Nothing to bind.
			case Var:
				name := string(bt)
				if name == "" {
					panic("wildcard found in match expr")
				}
				if bv, ok := scope[name]; ok {
					newV.Field(i).Set(bv)
				} else {
					panic(fmt.Sprintf("no binding for %q", name))
				}
			default:
				panic("unexpected Bool variant")
			}
		default:
			// Nothing to bind.
		}
	}
	return newV
}

func mergeScope(a, b bindings) bindings {
	if len(a) == 0 && len(b) == 0 {
		return nil
	}
	merged := make(bindings, len(a)+len(b))
	for n, v := range a {
		merged[n] = v
	}
	for n, v := range b {
		if _, ok := merged[n]; ok {
			panic(fmt.Sprintf("multiple bindings for %q", n))
		}
		merged[n] = v
	}
	return merged
}
