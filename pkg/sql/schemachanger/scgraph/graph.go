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
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/errors"
)

// Graph is a graph whose nodes are *scpb.Nodes. Graphs are constructed during
// schema change planning. Edges in the graph represent dependencies between
// nodes, either due to the sequencing of states for a single target or due to
// inter-target dependencies between states.
type Graph struct {

	// Targets is an interned slice of targets.
	targets []*scpb.Target

	// Interns the Node so that pointer equality can be used.
	targetNodes []map[scpb.State]*scpb.Node

	// Maps a target to its index in targetNodes.
	targetIdxMap map[*scpb.Target]int

	// nodeOpEdges maps a Node to an opEdge that proceeds
	// from it. A Node may have at most one opEdge from it.
	nodeOpEdges map[*scpb.Node]*OpEdge

	// nodeDepEdges maps a Node to its dependencies.
	// A Node dependency is another target state which must be
	// reached before or concurrently with this targetState.
	nodeDepEdges map[*scpb.Node][]*DepEdge

	edges []Edge
}

// New constructs a new Graph. All initial nodes ought to correspond to distinct
// targets. If they do not, an error will be returned.
func New(initialNodes []*scpb.Node) (*Graph, error) {
	g := Graph{
		targetIdxMap: map[*scpb.Target]int{},
		nodeOpEdges:  map[*scpb.Node]*OpEdge{},
		nodeDepEdges: map[*scpb.Node][]*DepEdge{},
	}
	for _, n := range initialNodes {
		if existing, ok := g.targetIdxMap[n.Target]; ok {
			return nil, errors.Errorf("invalid initial states contains duplicate target: %v and %v", n, initialNodes[existing])
		}
		idx := len(g.targets)
		g.targetIdxMap[n.Target] = idx
		g.targets = append(g.targets, n.Target)
		g.targetNodes = append(g.targetNodes, map[scpb.State]*scpb.Node{
			n.State: n,
		})
	}
	return &g, nil
}

func (g *Graph) getNode(t *scpb.Target, s scpb.State) (*scpb.Node, bool) {
	targetStates := g.getTargetStatesMap(t)
	ts, ok := targetStates[s]
	return ts, ok
}

// Suppress the linter.
var _ = (*Graph)(nil).getNode

func (g *Graph) getOrCreateNode(t *scpb.Target, s scpb.State) *scpb.Node {
	targetStates := g.getTargetStatesMap(t)
	if ts, ok := targetStates[s]; ok {
		return ts
	}
	ts := &scpb.Node{
		Target: t,
		State:  s,
	}
	targetStates[s] = ts
	return ts
}

func (g *Graph) getTargetStatesMap(target *scpb.Target) map[scpb.State]*scpb.Node {
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
	oe, ok := g.nodeOpEdges[n]
	return oe, ok
}

// AddOpEdge adds an op edge connecting the nodes for two states of a target.
func (g *Graph) AddOpEdge(t *scpb.Target, from, to scpb.State, op scop.Op) {
	oe := &OpEdge{
		from: g.getOrCreateNode(t, from),
		to:   g.getOrCreateNode(t, to),
		op:   op,
	}
	if existing, exists := g.nodeOpEdges[oe.from]; exists {
		panic(errors.Errorf("duplicate outbound op edge %v and %v",
			oe, existing))
	}
	g.edges = append(g.edges, oe)
	g.nodeOpEdges[oe.from] = oe
}

// AddDepEdge adds a dep edge connecting two nodes (specified by their targets
// and states).
func (g *Graph) AddDepEdge(
	fromTarget *scpb.Target, fromState scpb.State, toTarget *scpb.Target, toState scpb.State,
) {
	de := &DepEdge{
		from: g.getOrCreateNode(fromTarget, fromState),
		to:   g.getOrCreateNode(toTarget, toState),
	}
	g.edges = append(g.edges, de)
	g.nodeDepEdges[de.from] = append(g.nodeDepEdges[de.from], de)
}

// Edge represents a relationship between two TargetStates.
//
// TODO(ajwerner): Consider hiding Node pointers behind an interface to clarify
// mutability.
type Edge interface {
	From() *scpb.Node
	To() *scpb.Node
}

// OpEdge represents an edge changing the state of a target with an op.
type OpEdge struct {
	from, to *scpb.Node
	op       scop.Op
}

// From implements the Edge interface.
func (oe *OpEdge) From() *scpb.Node { return oe.from }

// To implements the Edge interface.
func (oe *OpEdge) To() *scpb.Node { return oe.to }

// Op returns the scop.Op for execution that is associated with the op edge.
func (oe *OpEdge) Op() scop.Op { return oe.op }

// DepEdge represents a dependency between two target states. A dependency
// implies that the To() state cannot be reached before the From() state. It
// can be reached concurrently.
type DepEdge struct {
	from, to *scpb.Node
}

// From implements the Edge interface.
func (de *DepEdge) From() *scpb.Node { return de.from }

// To implements the Edge interface.
func (de *DepEdge) To() *scpb.Node { return de.to }
