// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scplan

import (
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scgraph"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/errors"
)

type depMatcher struct {
	dirPredicate func(thisDir, thatDir scpb.Target_Direction) bool
	thatState    scpb.State
	predicate    interface{}
}

type decOpEdge struct {
	nextState scpb.State
	predicate interface{}
	op        interface{}
}

type targetRules struct {
	deps               targetDepRules
	forward, backwards targetOpRules
}

type targetDepRules map[scpb.State][]depMatcher

type targetOpRules map[scpb.State][]decOpEdge

var p = buildSchemaChangePlanner(rules)

type opGenFunc func(builder *scgraph.Graph, t *scpb.Target, s scpb.State, flags Params)
type depGenFunc func(g *scgraph.Graph, t *scpb.Target, s scpb.State)

type schemaChangeTargetPlanner struct {
	ops  opGenFunc
	deps depGenFunc
}

type schemaChangePlanner map[reflect.Type]schemaChangeTargetPlanner

func buildSchemaChangePlanner(m map[scpb.Element]targetRules) schemaChangePlanner {
	tp := make(map[reflect.Type]schemaChangeTargetPlanner)
	for e, r := range m {
		tp[reflect.TypeOf(e)] = schemaChangeTargetPlanner{
			ops:  buildSchemaChangeOpGenFunc(e, r.forward, r.backwards),
			deps: buildSchemaChangeDepGenFunc(e, r.deps),
		}
	}
	return tp
}

func buildSchemaChangeDepGenFunc(e scpb.Element, deps targetDepRules) depGenFunc {
	// We want to walk all of the edges and ensure that they have the proper
	// signature.
	tTyp := reflect.TypeOf(e)
	type matcher struct {
		dirPred   func(thisDir, thatDir scpb.Target_Direction) bool
		pred      func(this, that scpb.Element) bool
		thatState scpb.State
	}
	matchers := map[scpb.State]map[reflect.Type][]matcher{}
	for s, rules := range deps {
		for i, rule := range rules {
			mt := reflect.TypeOf(rule.predicate)
			if mt.NumIn() != 2 {
				panic(errors.Errorf("expected two args, got %d for (%T,%s)[%d]", mt.NumIn(), e, s, i))
			}
			if got := mt.In(0); got != tTyp {
				panic(errors.Errorf("expected %T, got %v for (%T,%s)[%d]", e, got, e, s, i))
			}
			other := mt.In(1)
			if !other.Implements(elementInterfaceType) {
				panic(errors.Errorf("expected %T to implement %v for (%T,%s)[%d]", other, elementInterfaceType, e, s, i))
			}
			if mt.NumOut() != 1 {
				panic(errors.Errorf("expected one return value, got %d for (%T,%s)[%d]", mt.NumOut(), e, s, i))
			}
			if mt.Out(0) != boolType {
				panic(errors.Errorf("expected bool return value, got %v for (%T,%s)[%d]", mt.Out(0), e, s, i))
			}
			if rule.dirPredicate == nil {
				panic(errors.Errorf("invalid missing direction predicate for (%T,%s)[%d]", e, s, i))
			}
			if matchers[s] == nil {
				matchers[s] = map[reflect.Type][]matcher{}
			}
			predV := reflect.ValueOf(rule.predicate)
			f := func(a, b scpb.Element) bool {
				out := predV.Call([]reflect.Value{
					reflect.ValueOf(a),
					reflect.ValueOf(b),
				})
				return out[0].Bool()
			}
			matchers[s][other] = append(matchers[s][other], matcher{
				dirPred:   rule.dirPredicate,
				pred:      f,
				thatState: rule.thatState,
			})
		}
	}
	return func(g *scgraph.Graph, this *scpb.Target, thisState scpb.State) {
		for t, matchers := range matchers[thisState] {
			if err := g.ForEachTarget(func(that *scpb.Target) error {
				if reflect.TypeOf(that.Element()) != t {
					return nil
				}
				for _, m := range matchers {
					if m.dirPred(this.Direction, that.Direction) &&
						m.pred(this.Element(), that.Element()) {
						g.AddDepEdge(this, thisState, that, m.thatState)
					}
				}
				return nil
			}); err != nil {
				panic(err)
			}
		}
	}
}

var (
	compileFlagsTyp      = reflect.TypeOf((*Params)(nil)).Elem()
	opsType              = reflect.TypeOf((*scop.Op)(nil)).Elem()
	boolType             = reflect.TypeOf((*bool)(nil)).Elem()
	elementInterfaceType = reflect.TypeOf((*scpb.Element)(nil)).Elem()
)

func buildSchemaChangeOpGenFunc(e scpb.Element, forward, backwards targetOpRules) opGenFunc {
	// We want to walk all of the edges and ensure that they have the proper
	// signature.
	tTyp := reflect.TypeOf(e)
	predicateTyp := reflect.FuncOf(
		[]reflect.Type{tTyp, compileFlagsTyp},
		[]reflect.Type{boolType},
		false, /* variadic */
	)
	opType := reflect.FuncOf(
		[]reflect.Type{tTyp},
		[]reflect.Type{opsType},
		false, /* variadic */
	)
	for s, rules := range forward {
		for i, rule := range rules {
			if rule.nextState == s {
				panic(errors.Errorf("detected rule into same state: %s for %T[%d]", s, e, i))
			}
			if rule.predicate != nil {
				if pt := reflect.TypeOf(rule.predicate); pt != predicateTyp {
					panic(errors.Errorf("invalid predicate with signature %v != %v for %T[%d]", pt, predicateTyp, e, i))
				}
			}
			if rule.nextState == scpb.State_UNKNOWN {
				if rule.op != nil {
					panic(errors.Errorf("invalid stopping rule with non-nil op func for %T[%d]", e, i))
				}
				continue
			}
			if rule.nextState != scpb.State_UNKNOWN && rule.op == nil {
				panic(errors.Errorf("invalid nil op with next state %s for %T[%d]", rule.nextState, e, i))
			}
			if ot := reflect.TypeOf(rule.op); ot != opType {
				panic(errors.Errorf("invalid ops with signature %v != %v %p %p for (%T, %s)[%d]", ot, opType, ot, opsType, e, s, i))
			}
		}
	}

	return func(builder *scgraph.Graph, t *scpb.Target, s scpb.State, flags Params) {
		cur := s
		tv := reflect.ValueOf(t.Element())
		flagsV := reflect.ValueOf(flags)
		predicateArgs := []reflect.Value{tv, flagsV}
		opsArgs := []reflect.Value{tv}
		var stateRules targetOpRules
		if t.Direction == scpb.Target_ADD {
			stateRules = forward
		} else {
			stateRules = backwards
		}

	outer:
		for {
			rules := stateRules[cur]
			for _, rule := range rules {
				if rule.predicate != nil {
					if out := reflect.ValueOf(rule.predicate).Call(predicateArgs); !out[0].Bool() {
						continue
					}
				}
				if rule.nextState == scpb.State_UNKNOWN {
					return
				}
				out := reflect.ValueOf(rule.op).Call(opsArgs)
				builder.AddOpEdge(t, cur, rule.nextState, out[0].Interface().(scop.Op))
				cur = rule.nextState
				continue outer
			}
			break
		}
	}
}
