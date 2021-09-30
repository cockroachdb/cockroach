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
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/errors"
)

type depMatcher struct {
	dirPredicate func(thisDir, thatDir scpb.Target_Direction) bool
	thatStatus   scpb.Status
	predicate    interface{}
}

type decOpEdge struct {
	nextStatus    scpb.Status
	predicate     interface{}
	op            interface{}
	nonRevertible bool
}

type targetRules struct {
	deps targetDepRules
}

type targetDepRules map[scpb.Status][]depMatcher

var p = buildSchemaChangePlanner(rules)

type depGenFunc func(g *scgraph.Graph, t *scpb.Target, s scpb.Status)

type schemaChangeTargetPlanner struct {
	deps depGenFunc
}

type schemaChangePlanner map[reflect.Type]schemaChangeTargetPlanner

func buildSchemaChangePlanner(m map[scpb.Element]targetRules) schemaChangePlanner {
	tp := make(map[reflect.Type]schemaChangeTargetPlanner)
	for e, r := range m {
		tp[reflect.TypeOf(e)] = schemaChangeTargetPlanner{
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
		dirPred    func(thisDir, thatDir scpb.Target_Direction) bool
		pred       func(this, that scpb.Element) bool
		thatStatus scpb.Status
	}
	matchers := map[scpb.Status]map[reflect.Type][]matcher{}
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
				dirPred:    rule.dirPredicate,
				pred:       f,
				thatStatus: rule.thatStatus,
			})
		}
	}
	return func(g *scgraph.Graph, this *scpb.Target, thisStatus scpb.Status) {
		for t, matchers := range matchers[thisStatus] {
			if err := g.ForEachTarget(func(that *scpb.Target) error {
				if reflect.TypeOf(that.Element()) != t {
					return nil
				}
				for _, m := range matchers {
					if m.dirPred(this.Direction, that.Direction) &&
						m.pred(this.Element(), that.Element()) {
						g.AddDepEdge(this, thisStatus, that, m.thatStatus)
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
	boolType             = reflect.TypeOf((*bool)(nil)).Elem()
	elementInterfaceType = reflect.TypeOf((*scpb.Element)(nil)).Elem()
)
