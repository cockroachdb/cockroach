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
	targetNodes []map[scpb.Status]*scpb.Node

	// Maps a target to its index in targetNodes.
	targetIdxMap map[*scpb.Target]int

	// nodeOpEdgesFrom maps a Node to an opEdge that proceeds
	// from it. A Node may have at most one opEdge from it.
	nodeOpEdgesFrom map[*scpb.Node]*OpEdge

	// nodeDepEdges maps a Node to its dependencies.
	// A Node dependency is another target node which must be
	// reached before or concurrently with this node.
	nodeDepEdges map[*scpb.Node][]*DepEdge

	// opToNode maps from an operation back to the
	// opEdge that generated it as an index.
	opToNode map[scop.Op]*scpb.Node

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
		targetIdxMap:    map[*scpb.Target]int{},
		nodeOpEdgesFrom: map[*scpb.Node]*OpEdge{},
		nodeDepEdges:    map[*scpb.Node][]*DepEdge{},
		opToNode:        map[scop.Op]*scpb.Node{},
		entities:        db,
	}
	for _, n := range initial {
		if existing, ok := g.targetIdxMap[n.Target]; ok {
			return nil, errors.Errorf("invalid initial state contains duplicate target: %v and %v", n, initial[existing])
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
	oe, ok := g.nodeOpEdgesFrom[n]
	return oe, ok
}

// GetDepEdgesFrom returns the unique outgoing op edge from the specified node,
// if one exists.
func (g *Graph) GetDepEdgesFrom(n *scpb.Node) ([]*DepEdge, bool) {
	de, ok := g.nodeDepEdges[n]
	return de, ok
}

// AddOpEdges adds an op edges connecting the nodes for two statuses of a target.
func (g *Graph) AddOpEdges(
	t *scpb.Target, from, to scpb.Status, revertible bool, ops ...scop.Op,
) (err error) {
	oe := &OpEdge{
		op:         ops,
		revertible: revertible,
	}
	if oe.from, err = g.getOrCreateNode(t, from); err != nil {
		return err
	}
	if oe.to, err = g.getOrCreateNode(t, to); err != nil {
		return err
	}
	if existing, exists := g.nodeOpEdgesFrom[oe.from]; exists {
		return errors.Errorf("duplicate outbound op edge %v and %v",
			oe, existing)
	}
	g.edges = append(g.edges, oe)
	g.nodeOpEdgesFrom[oe.from] = oe
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

// AddDepEdge adds a dep edge connecting two nodes (specified by their targets
// and statuses).
func (g *Graph) AddDepEdge(
	rule string,
	fromTarget *scpb.Target,
	fromStatus scpb.Status,
	toTarget *scpb.Target,
	toStatus scpb.Status,
) (err error) {
	de := &DepEdge{rule: rule}
	if de.from, err = g.getOrCreateNode(fromTarget, fromStatus); err != nil {
		return err
	}
	if de.to, err = g.getOrCreateNode(toTarget, toStatus); err != nil {
		return err
	}
	g.edges = append(g.edges, de)
	g.nodeDepEdges[de.from] = append(g.nodeDepEdges[de.from], de)
	return nil
}

// Edge represents a relationship between two Nodes.
//
// TODO(ajwerner): Consider hiding Node pointers behind an interface to clarify
// mutability.
type Edge interface {
	From() *scpb.Node
	To() *scpb.Node
}

// OpEdge represents an edge changing the state of a target with an op.
type OpEdge struct {
	from, to   *scpb.Node
	op         []scop.Op
	revertible bool
}

// From implements the Edge interface.
func (oe *OpEdge) From() *scpb.Node { return oe.from }

// To implements the Edge interface.
func (oe *OpEdge) To() *scpb.Node { return oe.to }

// Op returns the scop.Op for execution that is associated with the op edge.
func (oe *OpEdge) Op() []scop.Op { return oe.op }

// Revertible returns if the dependency edge is revertible
func (oe *OpEdge) Revertible() bool { return oe.revertible }

// DepEdge represents a dependency between two nodes. A dependency
// implies that the To() node cannot be reached before the From() node. It
// can be reached concurrently.
type DepEdge struct {
	from, to *scpb.Node

	// TODO(ajwerner): Deal with the possibility that multiple rules could
	// generate the same edge.
	rule string
}

// From implements the Edge interface.
func (de *DepEdge) From() *scpb.Node { return de.from }

// To implements the Edge interface.
func (de *DepEdge) To() *scpb.Node { return de.to }

// Name returns the name of the rule which generated this edge.
func (de *DepEdge) Name() string { return de.rule }
