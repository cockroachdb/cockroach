// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package rules contains rules to:
//  - generate dependency edges for a graph which contains op edges,
//  - mark certain op-edges as no-op.
package rules

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraph"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// ApplyDepRules adds dependency edges to the graph according to the
// registered dependency rules.
func ApplyDepRules(g *scgraph.Graph) error {
	for _, dr := range registry.depRules {
		start := timeutil.Now()
		var added int
		if err := dr.q.Iterate(g.Database(), func(r rel.Result) error {
			from := r.Var(dr.from).(*screl.Node)
			to := r.Var(dr.to).(*screl.Node)
			added++
			return g.AddDepEdge(
				dr.name, dr.kind, from.Target, from.CurrentStatus, to.Target, to.CurrentStatus,
			)
		}); err != nil {
			return err
		}
		if log.V(2) {
			log.Infof(
				context.TODO(), "applying dep rule %s %d took %v",
				dr.name, added, timeutil.Since(start),
			)
		}
	}
	return nil
}

// ApplyOpRules marks op edges as no-op in a shallow copy of the graph according
// to the registered rules.
func ApplyOpRules(g *scgraph.Graph) (*scgraph.Graph, error) {
	db := g.Database()
	m := make(map[*screl.Node][]string)
	for _, rule := range registry.opRules {
		var added int
		start := timeutil.Now()
		err := rule.q.Iterate(db, func(r rel.Result) error {
			added++
			n := r.Var(rule.from).(*screl.Node)
			m[n] = append(m[n], rule.name)
			return nil
		})
		if err != nil {
			return nil, err
		}
		if log.V(2) {
			log.Infof(
				context.TODO(), "applying op rule %s %d took %v",
				rule.name, added, timeutil.Since(start),
			)
		}
	}
	// Mark any op edges from these nodes as no-op.
	ret := g.ShallowClone()
	for from, rules := range m {
		if opEdge, ok := g.GetOpEdgeFrom(from); ok {
			ret.MarkAsNoOp(opEdge, rules...)
		}
	}
	return ret, nil
}

// registry is a singleton which contains all the dep and op rules.
var registry struct {
	depRules []registeredDepRule
	opRules  []registeredOpRule
}

type registeredDepRule struct {
	name     string
	from, to rel.Var
	q        *rel.Query
	kind     scgraph.DepEdgeKind
}

type registeredOpRule struct {
	name string
	from rel.Var
	q    *rel.Query
}

// registerDepRule registers a rule from which a set of dependency edges will
// be derived in a graph.
func registerDepRule(
	ruleName string, edgeKind scgraph.DepEdgeKind, from, to rel.Var, query *rel.Query,
) {
	registry.depRules = append(registry.depRules, registeredDepRule{
		name: ruleName,
		kind: edgeKind,
		from: from,
		to:   to,
		q:    query,
	})
}

// registerOpRule adds a graph q that will label as no-op the op edge originating
// from this node. There can only be one such edge per node, as per the edge
// definitions in opgen.
func registerOpRule(ruleName string, from rel.Var, q *rel.Query) {
	registry.opRules = append(registry.opRules, registeredOpRule{
		name: ruleName,
		from: from,
		q:    q,
	})
}
