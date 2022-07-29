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
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraph"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
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
			return errors.Wrapf(err, "applying dep rule %s", dr.name)
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
	m := make(map[*screl.Node][]scgraph.RuleName)
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
			return nil, errors.Wrapf(err, "applying op rule %s", rule.name)
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
	name     scgraph.RuleName
	from, to rel.Var
	q        *rel.Query
	kind     scgraph.DepEdgeKind
}

type registeredOpRule struct {
	name scgraph.RuleName
	from rel.Var
	q    *rel.Query
}

// registerDepRule registers a rule from which a set of dependency edges will
// be derived in a graph. The edge will be formed from the node containing
// the fromEl entity to the node containing the toEl entity.
func registerDepRule(
	ruleName scgraph.RuleName,
	kind scgraph.DepEdgeKind,
	fromEl, toEl string,
	def func(from, to nodeVars) rel.Clauses,
) {
	from, to := mkNodeVars(fromEl), mkNodeVars(toEl)
	c := def(from, to)
	c = append(c, from.joinTargetNode(), to.joinTargetNode())
	registry.depRules = append(registry.depRules, registeredDepRule{
		name: ruleName,
		kind: kind,
		from: from.node,
		to:   to.node,
		q:    screl.MustQuery(c...),
	})
}

// registerOpRule adds a graph q that will label as no-op the op edge originating
// from this node. There can only be one such edge per node, as per the edge
// definitions in opgen.
func registerOpRule(rn scgraph.RuleName, from rel.Var, q *rel.Query) {
	registry.opRules = append(registry.opRules, registeredOpRule{
		name: rn,
		from: from,
		q:    q,
	})
}

// nodeVars represents three variables intended to refer to
// related element, target, and node entities.
type nodeVars struct {
	el, target, node rel.Var
}

func (v nodeVars) joinTargetNode() rel.Clause {
	return screl.JoinTargetNode(v.el, v.target, v.node)
}

func (v nodeVars) currentStatus(status ...scpb.Status) rel.Clause {
	if len(status) == 0 {
		panic(errors.AssertionFailedf("empty current status values"))
	}
	if len(status) == 1 {
		return v.node.AttrEq(screl.CurrentStatus, status[0])
	}
	in := make([]interface{}, len(status))
	for i, s := range status {
		in[i] = s
	}
	return v.node.AttrIn(screl.CurrentStatus, in...)
}

func (v nodeVars) joinTarget() rel.Clause {
	return screl.JoinTarget(v.el, v.target)
}

func (v nodeVars) targetStatus(status scpb.TargetStatus) rel.Clause {
	return v.target.AttrEq(screl.TargetStatus, status.Status())
}

// Type delegates to the element var Type method.
func (v nodeVars) Type(valuesForTypeOf ...interface{}) rel.Clause {
	if len(valuesForTypeOf) == 0 {
		panic(errors.AssertionFailedf("empty type list for %q", v.el))
	}
	return v.el.Type(valuesForTypeOf[0], valuesForTypeOf[1:]...)
}

// typeFilter returns a Type clause which binds the element var to elements of
// a specific type, filtered by the conjunction of all provided predicates.
func (v nodeVars) typeFilter(predicatesForTypeOf ...func(element scpb.Element) bool) rel.Clause {
	if len(predicatesForTypeOf) == 0 {
		panic(errors.AssertionFailedf("empty type predicate for %q", v.el))
	}
	var valuesForTypeOf []interface{}
	_ = forEachElement(func(e scpb.Element) error {
		for _, p := range predicatesForTypeOf {
			if !p(e) {
				return nil
			}
		}
		valuesForTypeOf = append(valuesForTypeOf, e)
		return nil
	})
	return v.Type(valuesForTypeOf...)
}

func mkNodeVars(elStr string) nodeVars {
	el := rel.Var(elStr)
	return nodeVars{
		el:     el,
		target: el + "-target",
		node:   el + "-node",
	}
}
