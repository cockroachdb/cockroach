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
	"container/list"

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

	// Statement metadata for targets.
	statements []*scpb.Statement

	// Authorization information used by the targets.
	authorization scpb.Authorization

	// Interns the Node so that pointer equality can be used.
	targetNodes []map[scpb.Status]*scpb.Node

	// Maps a target to its index in targetNodes.
	targetIdxMap map[*scpb.Target]int

	// opEdgesFrom maps a Node to an opEdge that proceeds
	// from it. A Node may have at most one opEdge from it.
	opEdgesFrom map[*scpb.Node]*OpEdge

	// depEdgesFrom maps a Node from its dependencies.
	// A Node dependency is another target node which must be
	// reached before or concurrently with this node.
	depEdgesFrom *depEdgeTree

	// sameStageDepEdgesTo maps a Node to the DepEdges with the
	// SameStage kind incident upon the indexed node.
	sameStageDepEdgesTo *depEdgeTree

	// opToNode maps from an operation back to the
	// opEdge that generated it as an index.
	opToNode map[scop.Op]*scpb.Node

	// optimizedOutOpEdges that are marked optimized out, and will not generate
	// any operations.
	optimizedOutOpEdges map[*OpEdge]bool

	edges []Edge

	entities *rel.Database
}

// Database returns a database of the graph's underlying entities.
func (g *Graph) Database() *rel.Database {
	return g.entities
}

// New constructs a new Graph. All initial nodes ought to correspond to distinct
// targets. If they do not, an error will be returned.
func New(initial scpb.State) (*Graph, error) {
	db, err := rel.NewDatabase(screl.Schema, [][]rel.Attr{
		{rel.Type, screl.DescID},
		{screl.DescID, rel.Type},
		{screl.Element},
		{screl.Target},
		// TODO(ajwerner): Decide what more predicates are needed
	})
	if err != nil {
		return nil, err
	}
	g := Graph{
		targetIdxMap:        map[*scpb.Target]int{},
		opEdgesFrom:         map[*scpb.Node]*OpEdge{},
		optimizedOutOpEdges: map[*OpEdge]bool{},
		opToNode:            map[scop.Op]*scpb.Node{},
		entities:            db,
		statements:          initial.Statements,
		authorization:       initial.Authorization,
	}
	g.depEdgesFrom = newDepEdgeTree(fromTo, g.compareNodes)
	g.sameStageDepEdgesTo = newDepEdgeTree(toFrom, g.compareNodes)
	for _, n := range initial.Nodes {
		if existing, ok := g.targetIdxMap[n.Target]; ok {
			return nil, errors.Errorf("invalid initial state contains duplicate target: %v and %v", n, initial.Nodes[existing])
		}
		idx := len(g.targets)
		g.targetIdxMap[n.Target] = idx
		g.targets = append(g.targets, n.Target)
		g.targetNodes = append(g.targetNodes, map[scpb.Status]*scpb.Node{
			n.Status: n,
		})
		if err := g.entities.Insert(n); err != nil {
			return nil, err
		}
	}
	return &g, nil
}

// ShallowClone shallow copies the main graph structure, and deep copies
// any mutations / decorations on the graph.
func (g *Graph) ShallowClone() *Graph {
	// Shallow copy the base structure.
	clone := &Graph{
		targets:             g.targets,
		statements:          g.statements,
		authorization:       g.authorization,
		targetNodes:         g.targetNodes,
		targetIdxMap:        g.targetIdxMap,
		opEdgesFrom:         g.opEdgesFrom,
		depEdgesFrom:        g.depEdgesFrom,
		sameStageDepEdgesTo: g.sameStageDepEdgesTo,
		opToNode:            g.opToNode,
		edges:               g.edges,
		entities:            g.entities,
		optimizedOutOpEdges: make(map[*OpEdge]bool),
	}
	// Any decorations for mutations will be copied.
	for edge, noop := range g.optimizedOutOpEdges {
		clone.optimizedOutOpEdges[edge] = noop
	}
	return clone
}

// GetNode returns the cached node for a given target and status.
func (g *Graph) GetNode(t *scpb.Target, s scpb.Status) (*scpb.Node, bool) {
	targetStatuses := g.getTargetStatusMap(t)
	ts, ok := targetStatuses[s]
	return ts, ok
}

// Suppress the linter.
var _ = (*Graph)(nil).GetNode

func (g *Graph) getOrCreateNode(t *scpb.Target, s scpb.Status) (*scpb.Node, error) {
	targetStatuses := g.getTargetStatusMap(t)
	if ts, ok := targetStatuses[s]; ok {
		return ts, nil
	}
	ts := &scpb.Node{
		Target: t,
		Status: s,
	}
	targetStatuses[s] = ts
	if err := g.entities.Insert(ts); err != nil {
		return nil, err
	}
	return ts, nil
}

func (g *Graph) getTargetStatusMap(target *scpb.Target) map[scpb.Status]*scpb.Node {
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
func (g *Graph) GetOpEdgeFrom(n *scpb.Node) (*OpEdge, bool) {
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
	var typ scop.Type
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
		g.opToNode[op] = oe.To()
	}
	return nil
}

// GetNodeFromOp Gets an Edge from a given op.
func (g *Graph) GetNodeFromOp(op scop.Op) *scpb.Node {
	return g.opToNode[op]
}

var _ = (*Graph)(nil).GetNodeFromOp

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
	if de.Kind() == SameStage {
		g.sameStageDepEdgesTo.insert(de)
	}
	return nil
}

// MarkOpEdgeAsOptimizedOut marks an edge as no-op, so that no operations are emitted
//during planning.
func (g *Graph) MarkOpEdgeAsOptimizedOut(edge *OpEdge) {
	g.optimizedOutOpEdges[edge] = true
}

// IsOpEdgeOptimizedOut checks if an edge is marked as an edge that should emit no
// operations.
func (g *Graph) IsOpEdgeOptimizedOut(edge *OpEdge) bool {
	return g.optimizedOutOpEdges[edge]
}

// GetMetadataFromTarget returns the metadata for a given target node.
func (g *Graph) GetMetadataFromTarget(target *scpb.Target) scpb.ElementMetadata {
	return scpb.ElementMetadata{
		TargetMetadata: scpb.TargetMetadata{
			SourceElementID: target.Metadata.SourceElementID,
			SubWorkID:       target.Metadata.SubWorkID,
			StatementID:     target.Metadata.StatementID,
		},
		Statement:    g.statements[target.Metadata.StatementID].RedactedStatement,
		StatementTag: g.statements[target.Metadata.StatementID].StatementTag,
		Username:     g.authorization.Username,
		AppName:      g.authorization.AppName,
	}
}

// GetNodeRanks fetches ranks of nodes in topological order.
func (g *Graph) GetNodeRanks() (nodeRanks map[*scpb.Node]int, err error) {
	defer func() {
		if r := recover(); r != nil {
			rAsErr, ok := r.(error)
			if !ok {
				rAsErr = errors.Errorf("panic during GetNodeRanks: %v", r)
			}
			err = rAsErr
		}
	}()
	l := list.New()
	marks := make(map[*scpb.Node]bool)
	var visit func(n *scpb.Node)
	visit = func(n *scpb.Node) {
		permanent, marked := marks[n]
		if marked && !permanent {
			panic(errors.AssertionFailedf("graph is not a dag"))
		}
		if marked && permanent {
			return
		}
		marks[n] = false
		_ = g.ForEachDepEdgeFrom(n, func(de *DepEdge) error {
			visit(de.To())
			return nil
		})
		marks[n] = true
		l.PushFront(n)
	}
	_ = g.ForEachNode(func(n *scpb.Node) error {
		visit(n)
		return nil
	})
	rank := make(map[*scpb.Node]int, l.Len())
	for i, cur := 0, l.Front(); i < l.Len(); i++ {
		rank[cur.Value.(*scpb.Node)] = i
		cur = cur.Next()
	}
	return rank, nil
}

// compareNodes compares two nodes in a graph. A nil nodes is the minimum value.
func (g *Graph) compareNodes(a, b *scpb.Node) (less, eq bool) {
	switch {
	case a == b:
		return false, true
	case a == nil:
		return true, false
	case b == nil:
		return false, false
	case a.Target == b.Target:
		return a.Status < b.Status, a.Status == b.Status
	default:
		aIdx, bIdx := g.targetIdxMap[a.Target], g.targetIdxMap[b.Target]
		return aIdx < bIdx, aIdx == bIdx
	}
}
