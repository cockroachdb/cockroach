// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package scgraph contains utilities for describing a schema change operation
// as a graph of screl nodes.
package scgraph

import (
	"reflect"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// Graph is a graph whose nodes are *screl.Nodes. Graphs are constructed during
// schema change planning. Edges in the graph represent dependencies between
// nodes, either due to the sequencing of statuses for a single target or due to
// inter-target dependencies between statuses.
type Graph struct {

	// Targets is an interned slice of targets.
	targets []*scpb.Target

	// Interns the Node so that pointer equality can be used.
	targetNodes []map[scpb.Status]*screl.Node

	// Maps a target to its index in targetNodes.
	targetIdxMap map[*scpb.Target]targetIdx

	// depEdges stores the DepEdges to facilitate efficient lookups.
	depEdges depEdges

	// opEdges stores all OpEdges in the order they were added.
	opEdges []*OpEdge

	// opEdgesFrom maps a Node to an opEdge that proceeds
	// from it. A Node may have at most one opEdge from it.
	// opEdgesTo is the same but s/from/to/.
	opEdgesFrom, opEdgesTo map[*screl.Node]*OpEdge

	// opToOpEdge maps from an operation back to the
	// opEdge that generated it as an index.
	opToOpEdge map[scop.Op]*OpEdge

	entities *rel.Database
}

// targetIdx is the index in the targets slice where a given target resides.
type targetIdx uint32

// RuleName is the name of a rule. It exists as a type to avoid redaction and
// clarify the meaning of the string.
type RuleName string

// SafeValue makes RuleName a redact.SafeValue.
func (r RuleName) SafeValue() {}

var _ redact.SafeValue = RuleName("")

// Database returns a database of the graph's underlying entities.
func (g *Graph) Database() *rel.Database {
	return g.entities
}

// New constructs a new Graph. All initial nodes ought to correspond to distinct
// targets. If they do not, an error will be returned.
func New(cs scpb.CurrentState) (*Graph, error) {
	db, err := rel.NewDatabase(screl.Schema, []rel.Index{
		{
			Attrs:  []rel.Attr{rel.Type, screl.DescID, screl.ColumnID},
			Exists: []rel.Attr{screl.DescID},
		},
		{
			Attrs:  []rel.Attr{screl.ReferencedDescID, rel.Type},
			Exists: []rel.Attr{screl.ReferencedDescID},
		},
		{
			Attrs: []rel.Attr{screl.Element, screl.TargetStatus},
			Where: []rel.IndexWhere{
				{Attr: rel.Type, Eq: reflect.TypeOf((*scpb.Target)(nil))},
			},
		},
		{
			Attrs: []rel.Attr{screl.Target, screl.CurrentStatus},
			Where: []rel.IndexWhere{
				{Attr: rel.Type, Eq: reflect.TypeOf((*screl.Node)(nil))},
			},
		},
		{
			Attrs:    []rel.Attr{screl.ReferencedTypeIDs},
			Inverted: true,
		},
		{
			Attrs:    []rel.Attr{screl.ReferencedSequenceIDs},
			Inverted: true,
		},
		{
			Attrs:    []rel.Attr{screl.ReferencedFunctionIDs},
			Inverted: true,
		},
		{
			Attrs:    []rel.Attr{screl.ReferencedColumnIDs},
			Inverted: true,
		},
	}...)
	if err != nil {
		return nil, err
	}
	g := Graph{
		targetIdxMap: map[*scpb.Target]targetIdx{},
		opEdgesFrom:  map[*screl.Node]*OpEdge{},
		opEdgesTo:    map[*screl.Node]*OpEdge{},
		opToOpEdge:   map[scop.Op]*OpEdge{},
		entities:     db,
	}
	g.depEdges = makeDepEdges(func(n *screl.Node) targetIdx {
		return g.targetIdxMap[n.Target]
	})
	for i := range cs.Targets {
		t := &cs.Targets[i]
		var src scpb.Status
		{
			// Determine the status of the source node of the op-edge path
			// which leads to the present target node.
			if initial, current := cs.Initial[i], cs.Current[i]; initial == current {
				// This is the straightforward case, which applies in all phases except
				// the statement phase.
				src = current
			} else {
				// Here we are in the statement phase and the plan include a pre-commit
				// phase in which the element is transitioned from its current status
				// back to its initial status and then onward to the target status.
				// Those 3 corresponding nodes therefore need to be present in the
				// graph. The source node therefore needs to be either the current or
				// the initial status, depending on which is furthest from the target.
				//
				// Typically, that would be the initial status, however there are
				// legitimate cases where the initial status is also the target status
				// and in that case we need to pick the current status instead. This
				// happens in explicit transactions where a DDL statement effectively
				// undoes an earlier DDL statement in that transaction:
				//
				//     BEGIN;
				//     CREATE SCHEMA sc;
				//     DROP SCHEMA sc;
				//
				// In this example when building the plan while executing the last
				// statement, the Schema element will have:
				//   - an ABSENT target status, because we want to get rid of the
				//     newly-added schema,
				//   - an ABSENT initial status, because the schema didn't exist prior
				//     to this transaction,
				//   - a DESCRIPTOR_ADDING current status, because the previous
				//     statement has already executed its statement phase operations,
				//     in order to make the side-effects of the schema creation
				//     visible.
				//
				// The initial status has the convenient property in that it's either
				// PUBLIC or ABSENT, because we don't allow concurrent schema changes.
				// For this reason, we set the source node status based on whether
				// the target is a no-op or not.
				if initial == scpb.AsTargetStatus(t.TargetStatus).InitialStatus() {
					// In this case, either the initial status is PUBLIC and the target
					// is ABSENT, or the initial status is ABSENT and the target is
					// PUBLIC or TRANSIENT_ABSENT. This is the straightforward sub-case
					// where the current status is somewhere in between the initial
					// and target statuses on the op-edge path.
					src = initial
				} else {
					// This is the less straightforward sub-case where the target is a
					// no-op with respect to the initial status. However, since we're in
					// the statement phase, we still need to transition the element away
					// from its current status.
					src = current
				}
			}
		}
		if existing, ok := g.targetIdxMap[t]; ok {
			return nil, errors.Errorf("invalid initial state contains duplicate target: %v and %v", *t, cs.Targets[existing])
		}
		idx := len(g.targets)
		g.targetIdxMap[t] = targetIdx(idx)
		g.targets = append(g.targets, t)
		n := &screl.Node{Target: t, CurrentStatus: src}
		g.targetNodes = append(g.targetNodes, map[scpb.Status]*screl.Node{src: n})
		if err := g.entities.Insert(n); err != nil {
			return nil, err
		}
	}
	return &g, g.Validate()
}

// ShallowClone shallow copies the main graph structure, and deep copies
// any mutations / decorations on the graph.
func (g *Graph) ShallowClone() *Graph {
	// Shallow copy the base structure.
	clone := &Graph{
		targets:      g.targets,
		targetNodes:  g.targetNodes,
		targetIdxMap: g.targetIdxMap,
		opEdgesFrom:  g.opEdgesFrom,
		opEdgesTo:    g.opEdgesTo,
		depEdges:     g.depEdges,
		opEdges:      g.opEdges,
		opToOpEdge:   g.opToOpEdge,
		entities:     g.entities,
	}
	return clone
}

// GetNode returns the cached node for a given target and status.
func (g *Graph) GetNode(t *scpb.Target, s scpb.Status) (*screl.Node, bool) {
	targetStatuses := g.getTargetStatusMap(t)
	ts, ok := targetStatuses[s]
	return ts, ok
}

// Suppress the linter.
var _ = (*Graph)(nil).GetNode

func (g *Graph) getOrCreateNode(t *scpb.Target, s scpb.Status) (*screl.Node, error) {
	targetStatuses := g.getTargetStatusMap(t)
	if ts, ok := targetStatuses[s]; ok {
		return ts, nil
	}
	ts := &screl.Node{
		Target:        t,
		CurrentStatus: s,
	}
	targetStatuses[s] = ts
	if err := g.entities.Insert(ts); err != nil {
		return nil, err
	}
	return ts, nil
}

func (g *Graph) getTargetStatusMap(target *scpb.Target) map[scpb.Status]*screl.Node {
	idx, ok := g.targetIdxMap[target]
	if !ok {
		panic(errors.Errorf("target %v does not exist", target))
	}
	return g.targetNodes[idx]
}

func (g *Graph) containsTarget(target *scpb.Target) bool {
	_, exists := g.targetIdxMap[target]
	return exists
}

// Suppress the linter.
var _ = (*Graph)(nil).containsTarget

// GetOpEdgeFrom returns the unique outgoing op edge from the specified node,
// if one exists.
func (g *Graph) GetOpEdgeFrom(n *screl.Node) (*OpEdge, bool) {
	oe, ok := g.opEdgesFrom[n]
	return oe, ok
}

// GetOpEdgeTo returns the unique incoming op edge to the specified node,
// if one exists.
func (g *Graph) GetOpEdgeTo(n *screl.Node) (*OpEdge, bool) {
	oe, ok := g.opEdgesTo[n]
	return oe, ok
}

// AddOpEdges adds an op edges connecting the nodes for two statuses of a target.
func (g *Graph) AddOpEdges(
	t *scpb.Target, from, to scpb.Status, revertible, canFail bool, ops ...scop.Op,
) (err error) {
	oe := &OpEdge{
		op:         ops,
		revertible: revertible,
		canFail:    canFail,
	}
	if oe.from, err = g.getOrCreateNode(t, from); err != nil {
		return err
	}
	if oe.to, err = g.getOrCreateNode(t, to); err != nil {
		return err
	}
	if existing, exists := g.opEdgesFrom[oe.from]; exists {
		return errors.Errorf("duplicate outbound op edge %v and %v",
			oe, existing)
	}
	if existing, exists := g.opEdgesTo[oe.to]; exists {
		return errors.Errorf("duplicate outbound op edge %v and %v",
			oe, existing)
	}
	g.opEdges = append(g.opEdges, oe)
	typ := scop.MutationType
	for i, op := range ops {
		if i == 0 {
			typ = op.Type()
		} else if typ != op.Type() {
			return errors.Errorf("mixed type for opEdge %s->%s, %s != %s",
				screl.NodeString(oe.from), screl.NodeString(oe.to), typ, op.Type())
		}
	}
	oe.typ = typ
	g.opEdgesFrom[oe.from] = oe
	g.opEdgesTo[oe.to] = oe
	// Store mapping from op to Edge
	for _, op := range ops {
		g.opToOpEdge[op] = oe
	}
	return nil
}

// GetOpEdgeFromOp Gets an OpEdge from a given op.
func (g *Graph) GetOpEdgeFromOp(op scop.Op) *OpEdge {
	return g.opToOpEdge[op]
}

// AddDepEdge adds a dep edge connecting two nodes (specified by their targets
// and statuses).
func (g *Graph) AddDepEdge(
	ruleName RuleName,
	kind DepEdgeKind,
	fromTarget *scpb.Target,
	fromStatus scpb.Status,
	toTarget *scpb.Target,
	toStatus scpb.Status,
) (err error) {
	rule := Rule{Name: ruleName, Kind: kind}
	from, err := g.getOrCreateNode(fromTarget, fromStatus)
	if err != nil {
		return err
	}
	to, err := g.getOrCreateNode(toTarget, toStatus)
	if err != nil {
		return err
	}
	return g.depEdges.insertOrUpdate(rule, kind, from, to)

}

// Order returns the number of nodes in this graph.
func (g *Graph) Order() int {
	n := 0
	for _, m := range g.targetNodes {
		n = n + len(m)
	}
	return n
}

// Validate returns an error if there's a cycle in the graph.
func (g *Graph) Validate() error {
	order := g.Order()
	done := make(map[*screl.Node]bool, order)
	pred := make(map[*screl.Node]Edge, order)
	var visit func(n *screl.Node, in Edge) error
	visit = func(n *screl.Node, in Edge) error {
		if done[n] {
			return nil
		}
		if _, found := pred[n]; found {
			return errors.WithDetail(
				errors.AssertionFailedf("graph is not acyclical"),
				cycleErrorDetail(n, in, pred),
			)
		}
		pred[n] = in
		if out, ok := g.GetOpEdgeFrom(n); ok {
			if err := visit(out.To(), out); err != nil {
				return err
			}
		}
		if err := g.ForEachDepEdgeFrom(n, func(out *DepEdge) error {
			return visit(out.To(), out)
		}); err != nil {
			return err
		}
		done[n] = true
		return nil
	}
	return g.ForEachNode(func(n *screl.Node) error {
		return visit(n, nil /* in */)
	})
}

func cycleErrorDetail(target *screl.Node, edge Edge, pred map[*screl.Node]Edge) string {
	var collectCycle func(e Edge) []Edge
	collectCycle = func(e Edge) (c []Edge) {
		if e == nil {
			return nil
		}
		current := e.From()
		if current != target {
			c = collectCycle(pred[current])
		}
		return append(c, e)
	}
	var sb strings.Builder
	sb.WriteString("cycle:\n")
	for _, e := range collectCycle(edge) {
		sb.WriteString(screl.NodeString(e.From()))
		sb.WriteString(" --> ")
		if de, ok := e.(*DepEdge); ok {
			sb.WriteString(de.RuleNames().String())
		} else {
			sb.WriteString("op edge")
		}
		sb.WriteRune('\n')
	}
	sb.WriteString(screl.NodeString(target))
	sb.WriteRune('\n')
	return sb.String()
}
