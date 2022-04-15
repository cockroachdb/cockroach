// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scgraph

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/errors"
)

// Graph is a graph whose nodes are *scpb.Nodes. Graphs are constructed during
// schema change planning. Edges in the graph represent dependencies between
// nodes, either due to the sequencing of statuses for a single target or due to
// inter-target dependencies between statuses.
type Graph struct {

	// Targets is an interned slice of targets.
	targets []*scpb.Target

	// Interns the Node so that pointer equality can be used.
	targetNodes []map[scpb.Status]*screl.Node

	// Maps a target to its index in targetNodes.
	targetIdxMap map[*scpb.Target]int

	// opEdgesFrom maps a Node to an opEdge that proceeds
	// from it. A Node may have at most one opEdge from it.
	opEdgesFrom map[*screl.Node]*OpEdge

	// depEdgesFrom and depEdgesTo map a Node from and to its dependencies.
	// A Node dependency is another target node which cannot be reached before
	// reaching this node.
	depEdgesFrom, depEdgesTo *depEdgeTree

	// opToOpEdge maps from an operation back to the
	// opEdge that generated it as an index.
	opToOpEdge map[scop.Op]*OpEdge

	// noOpOpEdges that are marked optimized out, and will not generate
	// any operations.
	noOpOpEdges map[*OpEdge]map[string]struct{}

	edges []Edge

	entities *rel.Database
}

// Database returns a database of the graph's underlying entities.
func (g *Graph) Database() *rel.Database {
	return g.entities
}

// New constructs a new Graph. All initial nodes ought to correspond to distinct
// targets. If they do not, an error will be returned.
func New(cs scpb.CurrentState) (*Graph, error) {
	db, err := rel.NewDatabase(screl.Schema, [][]rel.Attr{
		{screl.DescID, rel.Type, screl.ColumnID},
		{screl.ReferencedDescID, rel.Type},
		{rel.Type, screl.Element, screl.CurrentStatus},
		{rel.Type, screl.Target, screl.TargetStatus},
	})
	if err != nil {
		return nil, err
	}
	g := Graph{
		targetIdxMap: map[*scpb.Target]int{},
		opEdgesFrom:  map[*screl.Node]*OpEdge{},
		noOpOpEdges:  map[*OpEdge]map[string]struct{}{},
		opToOpEdge:   map[scop.Op]*OpEdge{},
		entities:     db,
	}
	g.depEdgesFrom = newDepEdgeTree(fromTo, g.compareNodes)
	g.depEdgesTo = newDepEdgeTree(toFrom, g.compareNodes)
	for i, status := range cs.Current {
		t := &cs.Targets[i]
		if existing, ok := g.targetIdxMap[t]; ok {
			return nil, errors.Errorf("invalid initial state contains duplicate target: %v and %v", *t, cs.Targets[existing])
		}
		idx := len(g.targets)
		g.targetIdxMap[t] = idx
		g.targets = append(g.targets, t)
		n := &screl.Node{Target: t, CurrentStatus: status}
		g.targetNodes = append(g.targetNodes, map[scpb.Status]*screl.Node{status: n})
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
		depEdgesFrom: g.depEdgesFrom,
		depEdgesTo:   g.depEdgesTo,
		opToOpEdge:   g.opToOpEdge,
		edges:        g.edges,
		entities:     g.entities,
		noOpOpEdges:  make(map[*OpEdge]map[string]struct{}),
	}
	// Any decorations for mutations will be copied.
	for edge, noop := range g.noOpOpEdges {
		clone.noOpOpEdges[edge] = noop
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

// AddOpEdges adds an op edges connecting the nodes for two statuses of a target.
func (g *Graph) AddOpEdges(
	t *scpb.Target, from, to scpb.Status, revertible bool, minPhase scop.Phase, ops ...scop.Op,
) (err error) {

	oe := &OpEdge{
		op:         ops,
		revertible: revertible,
		minPhase:   minPhase,
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
	g.edges = append(g.edges, oe)
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
	rule string,
	kind DepEdgeKind,
	fromTarget *scpb.Target,
	fromStatus scpb.Status,
	toTarget *scpb.Target,
	toStatus scpb.Status,
) (err error) {
	de := &DepEdge{rule: rule, kind: kind}
	if de.from, err = g.getOrCreateNode(fromTarget, fromStatus); err != nil {
		return err
	}
	if de.to, err = g.getOrCreateNode(toTarget, toStatus); err != nil {
		return err
	}
	g.edges = append(g.edges, de)
	g.depEdgesFrom.insert(de)
	g.depEdgesTo.insert(de)
	return nil
}

// MarkAsNoOp marks an edge as no-op, so that no operations are emitted from
// this edge during planning.
func (g *Graph) MarkAsNoOp(edge *OpEdge, rule ...string) {
	m := make(map[string]struct{})
	for _, r := range rule {
		m[r] = struct{}{}
	}
	g.noOpOpEdges[edge] = m
}

// IsNoOp checks if an edge is marked as an edge that should emit no operations.
func (g *Graph) IsNoOp(edge *OpEdge) bool {
	if len(edge.op) == 0 {
		return true
	}
	_, isNoOp := g.noOpOpEdges[edge]
	return isNoOp
}

// NoOpRules returns the rules which caused the edge to not emit any operations.
func (g *Graph) NoOpRules(edge *OpEdge) (rules []string) {
	if !g.IsNoOp(edge) {
		return nil
	}
	m := g.noOpOpEdges[edge]
	for rule := range m {
		rules = append(rules, rule)
	}
	sort.Strings(rules)
	return rules
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
func (g *Graph) Validate() (err error) {
	marks := make(map[*screl.Node]bool, g.Order())
	var visit func(n *screl.Node)
	visit = func(n *screl.Node) {
		if err != nil {
			return
		}
		permanent, marked := marks[n]
		if marked && !permanent {
			err = errors.AssertionFailedf("graph is not acyclical")
			return
		}
		if marked && permanent {
			return
		}
		marks[n] = false
		_ = g.ForEachDepEdgeTo(n, func(de *DepEdge) error {
			visit(de.From())
			return nil
		})
		marks[n] = true
	}
	_ = g.ForEachNode(func(n *screl.Node) error {
		visit(n)
		return nil
	})
	return err
}

// compareNodes compares two nodes in a graph. A nil nodes is the minimum value.
func (g *Graph) compareNodes(a, b *screl.Node) (less, eq bool) {
	switch {
	case a == b:
		return false, true
	case a == nil:
		return true, false
	case b == nil:
		return false, false
	case a.Target == b.Target:
		return a.CurrentStatus < b.CurrentStatus, a.CurrentStatus == b.CurrentStatus
	default:
		aIdx, bIdx := g.targetIdxMap[a.Target], g.targetIdxMap[b.Target]
		return aIdx < bIdx, aIdx == bIdx
	}
}
