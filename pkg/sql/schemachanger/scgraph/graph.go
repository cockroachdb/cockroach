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
	"github.com/google/btree"
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

	// Authorization information used by the targers.
	authorization scpb.Authorization

	// Interns the Node so that pointer equality can be used.
	targetNodes []map[scpb.Status]*scpb.Node

	// Maps a target to its index in targetNodes.
	targetIdxMap map[*scpb.Target]int

	// nodeOpEdgesFrom maps a Node to an opEdge that proceeds
	// from it. A Node may have at most one opEdge from it.
	nodeOpEdgesFrom map[*scpb.Node]*OpEdge

	// nodeDepEdgesFrom maps a Node from its dependencies.
	// A Node dependency is another target node which must be
	// reached before or concurrently with this node.
	nodeDepEdgesFrom *btree.BTree

	// nodeDepEdgesTo maps a Node to its dependencies.
	// A Node dependency is another target node which must be
	// reached before or concurrently with this node.
	nodeDepEdgesTo *btree.BTree

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
		targetIdxMap:     map[*scpb.Target]int{},
		nodeOpEdgesFrom:  map[*scpb.Node]*OpEdge{},
		nodeDepEdgesFrom: btree.New(2),
		nodeDepEdgesTo:   btree.New(2),
		opToNode:         map[scop.Op]*scpb.Node{},
		entities:         db,
		statements:       initial.Statements,
		authorization:    initial.Authorization,
	}
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

var _ = (*Graph)(nil).GetNodeFromOp

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
	g.nodeDepEdgesFrom.ReplaceOrInsert(&edgeTreeEntry{
		g:     g,
		edge:  de,
		order: fromTo,
	})
	g.nodeDepEdgesTo.ReplaceOrInsert(&edgeTreeEntry{
		g:     g,
		edge:  de,
		order: toFrom,
	})
	return nil
}

// GetMetadataFromTarget returns the metadata for a given target node.
func (g *Graph) GetMetadataFromTarget(target *scpb.Target) scpb.ElementMetadata {
	return scpb.ElementMetadata{
		TargetMetadata: scpb.TargetMetadata{
			SourceElementID: target.Metadata.SourceElementID,
			SubWorkID:       target.Metadata.SubWorkID,
			StatementID:     target.Metadata.StatementID,
		},
		Statement: g.statements[target.Metadata.StatementID].Statement,
		Username:  g.authorization.Username,
		AppName:   g.authorization.AppName,
	}
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
	typ        scop.Type
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

// Type returns the types of operations associated with this edge.
func (oe *OpEdge) Type() scop.Type {
	return oe.typ
}

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
	backCycleExists := func(n *scpb.Node, de *DepEdge) bool {
		var foundBack bool
		_ = g.ForEachDepEdgeFrom(de.To(), func(maybeBack *DepEdge) error {
			foundBack = foundBack || maybeBack.To() == n
			return nil
		})
		return foundBack
	}
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
			// We want to eliminate cycles caused by swaps. In that
			// case, we want to pretend that there is no edge from the
			// add to the drop, and, in that way, the drop is ordered first.
			if n.Direction == scpb.Target_ADD || !backCycleExists(n, de) {
				visit(de.To())
			}
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

// edgeTreeOrder order in which the edge tree is sorted,
// either based on from/to node indexes.
type edgeTreeOrder bool

const (
	fromTo edgeTreeOrder = true
	toFrom edgeTreeOrder = false
)

// edgeTreeEntry BTree items for tracking edges
// in an ordered manner.
type edgeTreeEntry struct {
	g     *Graph
	edge  Edge
	order edgeTreeOrder
}

// Less implements btree.Item
func (e *edgeTreeEntry) Less(other btree.Item) bool {
	o := other.(*edgeTreeEntry)
	var a1, b1, a2, b2 *scpb.Node
	switch e.order {
	case fromTo:
		a1, b1, a2, b2 = e.edge.From(), o.edge.From(), e.edge.To(), o.edge.To()
	case toFrom:
		a1, b1, a2, b2 = e.edge.To(), o.edge.To(), e.edge.From(), o.edge.From()
	}
	less, eq := compareNodes(e.g, a1, b1)
	if eq {
		less, _ = compareNodes(e.g, a2, b2)
	}
	return less
}

// compareNodes compares two nodes in a graph. A nil nodes is the minimum value.
func compareNodes(g *Graph, a, b *scpb.Node) (less, eq bool) {
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
