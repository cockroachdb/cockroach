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
	"fmt"
	"reflect"
)

var (
	// True is a pattern that matches true booleans.
	True Bool = b(true)

	// False is a pattern that matches false booleans.
	False Bool = b(false)

	// Wildcard is a pattern that matches any value.
	Wildcard = wildcard{}
)

// Bool represents a boolean pattern.
type Bool interface {
	bool()
}

func (b) bool()        {}
func (wildcard) bool() {}
func (Binding) bool()  {}

type b bool
type wildcard struct{}

// Binding acts like Wildcard but allows variables to be bound to names and used
// in the match expression.
type Binding string

// Pattern is a mapping from (State,Event) pairs to Transitions. When
// unexpanded, it may contain values like wildcards and variable bindings.
type Pattern map[State]map[Event]Transition

func expandPattern(p Pattern) Pattern {
	xp := make(Pattern)
	for s, sm := range p {
		sVars := expandVar(reflect.ValueOf(s))
		for _, sVar := range sVars {
			xs := sVar.v.Interface().(State)

			xsm := xp[xs]
			if xsm == nil {
				xsm = make(map[Event]Transition)
				xp[xs] = xsm
			}

			for e, t := range sm {
				eVars := expandVar(reflect.ValueOf(e))
				for _, eVar := range eVars {
					xe := eVar.v.Interface().(Event)
					if _, ok := xsm[xe]; ok {
						panic("match patterns overlap")
					}

					bindings := mergeBindings(sVar.bindings, eVar.bindings)
					xNext := bindVar(reflect.ValueOf(t.Next), bindings)
					xsm[xe] = Transition{
						Next:   xNext.Interface().(State),
						Action: t.Action,
					}
				}
			}
		}
	}
	return xp
}

type bindings map[string]reflect.Value
type expandedVar struct {
	v        reflect.Value
	bindings bindings
}

// expand expands all wildcards in the provided value.
func expandVar(v reflect.Value) []expandedVar {
	for i := 0; i < v.NumField(); i++ {
		f := v.Field(i).Interface()
		switch t := f.(type) {
		case Bool:
			switch bt := t.(type) {
			case b:
				// Can't expand.
			case wildcard:
				var xPats []expandedVar
				for _, xVal := range expandBool(v, i) {
					xPats = append(xPats, expandVar(xVal)...)
				}
				return xPats
			case Binding:
				name := string(bt)
				var xPats []expandedVar
				for _, xVal := range expandBool(v, i) {
					recXPats := expandVar(xVal)
					for i, rexXPat := range recXPats {
						recXPats[i].bindings = mergeBindings(
							rexXPat.bindings,
							bindings{name: xVal.Field(i)},
						)
					}
					xPats = append(xPats, recXPats...)
				}
				return xPats
			case nil:
				panic("found nil Bool in match pattern")
			default:
				panic("unexpected Bool variant")
			}
		default:
			// Can't expand.
		}
	}
	return []expandedVar{{
		v:        v,
		bindings: map[string]reflect.Value{},
	}}
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

func bindVar(v reflect.Value, bindings bindings) reflect.Value {
	newV := reflect.New(v.Type()).Elem()
	newV.Set(v)
	for i := 0; i < newV.NumField(); i++ {
		f := newV.Field(i).Interface()
		switch t := f.(type) {
		case Bool:
			switch bt := t.(type) {
			case b:
				// Nothing to bind.
			case Binding:
				name := string(bt)
				if bv, ok := bindings[name]; ok {
					newV.Field(i).Set(bv)
				} else {
					panic(fmt.Sprintf("no binding for %q", name))
				}
			case wildcard:
				panic("either wildcard found in match expr")
			case nil:
				panic("found nil Bool in match expr")
			default:
				panic("unexpected Bool variant")
			}
		default:
			// Nothing to bind.
		}
	}
	return newV
}

func mergeBindings(a, b bindings) bindings {
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
