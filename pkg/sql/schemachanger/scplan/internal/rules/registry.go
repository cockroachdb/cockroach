// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package common contains shared structures / helper functions
// for implementing rules in the current and previous releases
// of cockroach. Allowing old rules to cleanly forward fit
// to newer versions via abstraction.
package rules

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraph"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"gopkg.in/yaml.v3"
)

// ApplyDepRules adds dependency edges to the graph according to the
// registered dependency rules.
func (r *Registry) ApplyDepRules(ctx context.Context, g *scgraph.Graph) error {
	for _, dr := range r.depRules {
		start := timeutil.Now()
		var added int
		if err := dr.q.Iterate(g.Database(), func(r rel.Result) error {
			// Applying the dep rules can be slow in some cases. Check for
			// cancellation when applying the rules to ensure we don't spin for
			// too long while the user is waiting for the task to exit cleanly.
			if ctx.Err() != nil {
				return ctx.Err()
			}
			from := r.Var(dr.from).(*screl.Node)
			to := r.Var(dr.to).(*screl.Node)
			added++
			return g.AddDepEdge(
				dr.name, dr.kind, from.Target, from.CurrentStatus, to.Target, to.CurrentStatus,
			)
		}); err != nil {
			return errors.Wrapf(err, "applying dep rule %s", dr.name)
		}
		if log.ExpensiveLogEnabled(ctx, 2) {
			log.Infof(
				ctx, "applying dep rule %s %d took %v",
				dr.name, added, timeutil.Since(start),
			)
		}
	}
	return nil
}

// ApplyOpRules marks op edges as no-op in a shallow copy of the graph according
// to the registered rules.
//
// Deprecated.
//
// TODO(postamar): remove once release_22_2 is also removed
func (r *Registry) ApplyOpRules(ctx context.Context, g *scgraph.Graph) (*scgraph.Graph, error) {
	db := g.Database()
	m := make(map[*screl.Node][]scgraph.RuleName)
	for _, rule := range r.opRules {
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
		if log.ExpensiveLogEnabled(ctx, 2) {
			log.Infof(
				ctx, "applying op rule %s %d took %v",
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

func (r *Registry) MarshalDepRules() (string, error) {
	s := append(([]registeredDepRule)(nil), r.depRules...)
	sort.SliceStable(s, func(i, j int) bool {
		return s[i].name < s[j].name
	})
	out, err := yaml.Marshal(s)
	if err != nil {
		return "", errors.Wrapf(err, "failed to marshal deprules")
	}
	return string(out), nil
}

func (r *Registry) MarshalOpRules() (string, error) {
	s := append(([]registeredOpRule)(nil), r.opRules...)
	sort.SliceStable(s, func(i, j int) bool {
		return s[i].name < s[j].name
	})
	out, err := yaml.Marshal(s)
	if err != nil {
		return "", errors.Wrapf(err, "failed to marshal oprules")
	}
	return string(out), nil
}

// Registry contains all the dep and op rules.
type Registry struct {
	depRules []registeredDepRule
	opRules  []registeredOpRule
}

type registeredDepRule struct {
	name     scgraph.RuleName
	from, to rel.Var
	q        *rel.Query
	kind     scgraph.DepEdgeKind
}

// Deprecated.
//
// TODO(postamar): remove once release_22_2 is also removed
type registeredOpRule struct {
	name scgraph.RuleName
	from rel.Var
	q    *rel.Query
}

func NewRegistry() *Registry {
	return &Registry{}
}

// RegisterDepRule registers a rule from which a set of dependency edges will
// be derived in a graph. The edge will be formed from the Node containing
// the fromEl entity to the Node containing the toEl entity.
func (r *Registry) RegisterDepRule(
	ruleName scgraph.RuleName,
	kind scgraph.DepEdgeKind,
	fromEl, toEl string,
	def func(from, to NodeVars) rel.Clauses,
) {
	from, to := MkNodeVars(fromEl), MkNodeVars(toEl)
	c := def(from, to)
	c = append(c, from.JoinTargetNode(), to.JoinTargetNode())
	r.depRules = append(r.depRules, registeredDepRule{
		name: ruleName,
		kind: kind,
		from: from.Node,
		to:   to.Node,
		q:    screl.MustQuery(c...),
	})
}

// RegisterOpRule adds a graph q that will label as no-op the op edge originating
// from this Node. There can only be one such edge per Node, as per the edge
// definitions in opgen.
//
// Deprecated.
//
// TODO(postamar): remove once release_22_2 is also removed
func (r *Registry) RegisterOpRule(rn scgraph.RuleName, from rel.Var, q *rel.Query) {
	r.opRules = append(r.opRules, registeredOpRule{
		name: rn,
		from: from,
		q:    q,
	})
}

// NodeVars represents three variables intended to refer to
// related element, Target, and Node entities.
type NodeVars struct {
	El, Target, Node rel.Var
}

func (v NodeVars) JoinTargetNode() rel.Clause {
	return screl.JoinTargetNode(v.El, v.Target, v.Node)
}

func (v NodeVars) CurrentStatus(status ...scpb.Status) rel.Clause {
	if len(status) == 0 {
		panic(errors.AssertionFailedf("empty current status values"))
	}
	if len(status) == 1 {
		return v.Node.AttrEq(screl.CurrentStatus, status[0])
	}
	in := make([]interface{}, len(status))
	for i, s := range status {
		in[i] = s
	}
	return v.Node.AttrIn(screl.CurrentStatus, in...)
}

func (v NodeVars) JoinTarget() rel.Clause {
	return screl.JoinTarget(v.El, v.Target)
}

func (v NodeVars) TargetStatus(status ...scpb.TargetStatus) rel.Clause {
	if len(status) == 0 {
		panic(errors.AssertionFailedf("empty current status values"))
	}
	if len(status) == 1 {
		return v.Target.AttrEq(screl.TargetStatus, status[0].Status())
	}
	in := make([]interface{}, len(status))
	for i, s := range status {
		in[i] = s.Status()
	}
	return v.Target.AttrIn(screl.TargetStatus, in...)
}

// Type delegates to the element var Type method.
func (v NodeVars) Type(valuesForTypeOf ...interface{}) rel.Clause {
	if len(valuesForTypeOf) == 0 {
		panic(errors.AssertionFailedf("empty type list for %q", v.El))
	}
	return v.El.Type(valuesForTypeOf[0], valuesForTypeOf[1:]...)
}

// TypeFilter returns a Type clause which binds the element var to elements of
// a specific type, filtered by the conjunction of all provided predicates.
func (v NodeVars) TypeFilter(
	version clusterversion.Key, predicatesForTypeOf ...func(element scpb.Element) bool,
) rel.Clause {
	if len(predicatesForTypeOf) == 0 {
		panic(errors.AssertionFailedf("empty type predicate for %q", v.El))
	}
	cv := clusterversion.ClusterVersion{Version: clusterversion.ByKey(version)}
	var valuesForTypeOf []interface{}
	_ = ForEachElementInActiveVersion(cv, func(e scpb.Element) error {
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

// DescIDEq defines a clause which will bind idVar to the DescID of the
// v's element.
func (v NodeVars) DescIDEq(idVar rel.Var) rel.Clause {
	return v.El.AttrEqVar(screl.DescID, idVar)
}

// ReferencedTypeDescIDsContain defines a clause which will bind containedIDVar
// to a descriptor ID contained in v's element's referenced type IDs.
func (v NodeVars) ReferencedTypeDescIDsContain(containedIDVar rel.Var) rel.Clause {
	return v.El.AttrContainsVar(screl.ReferencedTypeIDs, containedIDVar)
}

// ReferencedSequenceIDsContains defines a clause which will bind
// containedIDVar to a descriptor ID contained in v's element's referenced
// sequence IDs.
func (v NodeVars) ReferencedSequenceIDsContains(containedIDVar rel.Var) rel.Clause {
	return v.El.AttrContainsVar(screl.ReferencedSequenceIDs, containedIDVar)
}

// ReferencedFunctionIDsContains defines a clause which will bind
// containedIDVar to a descriptor ID contained in v's element's referenced
// function IDs.
func (v NodeVars) ReferencedFunctionIDsContains(containedIDVar rel.Var) rel.Clause {
	return v.El.AttrContainsVar(screl.ReferencedFunctionIDs, containedIDVar)
}

// ReferencedColumnIDsContains defines a clause which will bind
// containedIDVar to a descriptor ID contained in v's element's referenced
// column IDs.
func (v NodeVars) ReferencedColumnIDsContains(containedIDVar rel.Var) rel.Clause {
	return v.El.AttrContainsVar(screl.ReferencedColumnIDs, containedIDVar)
}

func MkNodeVars(elStr string) NodeVars {
	el := rel.Var(elStr)
	return NodeVars{
		El:     el,
		Target: el + "-Target",
		Node:   el + "-Node",
	}
}

func (r registeredDepRule) MarshalYAML() (interface{}, error) {
	var query yaml.Node
	if err := query.Encode(r.q.Clauses()); err != nil {
		return nil, err
	}
	return &yaml.Node{
		Kind: yaml.MappingNode,
		Content: []*yaml.Node{
			{Kind: yaml.ScalarNode, Value: "name"},
			{Kind: yaml.ScalarNode, Value: string(r.name)},
			{Kind: yaml.ScalarNode, Value: "from"},
			{Kind: yaml.ScalarNode, Value: string(r.from)},
			{Kind: yaml.ScalarNode, Value: "kind"},
			{Kind: yaml.ScalarNode, Value: r.kind.String()},
			{Kind: yaml.ScalarNode, Value: "to"},
			{Kind: yaml.ScalarNode, Value: string(r.to)},
			{Kind: yaml.ScalarNode, Value: "query"},
			&query,
		},
	}, nil
}

// Deprecated.
//
// TODO(postamar): remove once release_22_2 is also removed
func (r registeredOpRule) MarshalYAML() (interface{}, error) {
	var query yaml.Node
	if err := query.Encode(r.q.Clauses()); err != nil {
		return nil, err
	}
	return &yaml.Node{
		Kind: yaml.MappingNode,
		Content: []*yaml.Node{
			{Kind: yaml.ScalarNode, Value: "name"},
			{Kind: yaml.ScalarNode, Value: string(r.name)},
			{Kind: yaml.ScalarNode, Value: "from"},
			{Kind: yaml.ScalarNode, Value: string(r.from)},
			{Kind: yaml.ScalarNode, Value: "query"},
			&query,
		},
	}, nil
}
