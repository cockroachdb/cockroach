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
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/errors"
	"github.com/google/btree"
)

// edgeTreeOrder order in which the edge tree is sorted,
// either based on from/to node indexes.
type edgeTreeOrder bool

const (
	fromTo edgeTreeOrder = true
	toFrom edgeTreeOrder = false
)

// getTargetIdxFunc is used to get the location of the node in the graph.
type getTargetIdxFunc = func(n *screl.Node) targetIdx

// depEdges is a data structure to store the set of depEdges. It offers
// in-order traversal of edges from or to any node in the graph.
type depEdges struct {
	fromTo *btree.BTree
	toFrom *btree.BTree

	getTargetIdx getTargetIdxFunc
	edgeAlloc    depEdgeAlloc
	entryAlloc   depEdgeTreeEntryAlloc
}

// makeDepEdges constructs the depEdge structure.
func makeDepEdges(getTargetIdx getTargetIdxFunc) depEdges {
	const degree = 32 // arbitrary
	return depEdges{
		fromTo:       btree.New(degree),
		toFrom:       btree.New(degree),
		getTargetIdx: getTargetIdx,
	}
}

// insertOrUpdate will insert a new dep edge if no such edge exists between
// from and to. Otherwise, it will update the edge accordingly. An error
// may be returned if the kind is incompatible with the existing kind for
// the edge.
func (et *depEdges) insertOrUpdate(rule Rule, kind DepEdgeKind, from, to *screl.Node) error {
	k := makeEdgeKey(et.getTargetIdx, from, to)
	if got, ok := et.get(k); ok {
		return updateExistingDepEdge(rule, kind, got)
	}
	de := et.edgeAlloc.new(DepEdge{
		kind:  kind,
		from:  from,
		to:    to,
		rules: []Rule{rule},
	})
	et.fromTo.ReplaceOrInsert(et.entryAlloc.new(k, fromTo, de))
	et.toFrom.ReplaceOrInsert(et.entryAlloc.new(k, toFrom, de))
	return nil
}

func updateExistingDepEdge(rule Rule, kind DepEdgeKind, got *DepEdge) error {
	if got.kind != kind && kind != Precedence {
		if got.kind != Precedence {
			return errors.AssertionFailedf(
				"inconsistent dep edge kinds: %s rule %q conflicts with %s",
				rule.Kind, rule.Name, got,
			)
		}
		got.kind = kind
	}
	got.rules = append(got.rules, rule)
	return nil
}

// iterateFromNode iterates the set of edges from the passed node.
func (et *depEdges) iterateFromNode(n *screl.Node, it DepEdgeIterator) (err error) {
	return iterateDepEdges(
		et.getTargetIdx(n), n.CurrentStatus, fromTo, et.fromTo, it,
	)
}

// iterateFromNode iterates the set of edges to the passed node.
func (et *depEdges) iterateToNode(n *screl.Node, it DepEdgeIterator) (err error) {
	return iterateDepEdges(
		et.getTargetIdx(n), n.CurrentStatus, toFrom, et.toFrom, it,
	)
}

func iterateDepEdges(
	target targetIdx, status scpb.Status, order edgeTreeOrder, t *btree.BTree, it DepEdgeIterator,
) (err error) {
	var idx int
	if order == toFrom {
		idx = 1
	}
	var qk edgeKey
	qk.targets[idx] = target
	qk.statuses[idx] = uint8(status)
	k1, k2 := getDepEdgeTreeEntry(), getDepEdgeTreeEntry()
	defer putDepEdgeTreeEntry(k1)
	defer putDepEdgeTreeEntry(k2)
	*k1 = depEdgeTreeEntry{edgeKey: qk, order: order, kind: queryStart}
	*k2 = depEdgeTreeEntry{edgeKey: qk, order: order, kind: queryEnd}
	t.AscendRange(k1, k2, func(i btree.Item) (wantMore bool) {
		err = it(i.(*depEdgeTreeEntry).edge)
		return err == nil
	})
	return iterutil.Map(err)
}

// iterates iterates all edges.
func (et *depEdges) iterate(it DepEdgeIterator) (err error) {
	et.fromTo.Ascend(func(i btree.Item) bool {
		err = it(i.(*depEdgeTreeEntry).edge)
		return err == nil
	})
	return iterutil.Map(err)
}

// get looks up a dep edge with an edgeKey.
func (et *depEdges) get(k edgeKey) (*DepEdge, bool) {
	qk := getDepEdgeTreeEntry()
	defer putDepEdgeTreeEntry(qk)
	*qk = depEdgeTreeEntry{edgeKey: k, order: fromTo}
	if got := et.fromTo.Get(qk); got != nil {
		return got.(*depEdgeTreeEntry).edge, true
	}
	return nil, false
}

// edgeKey stores two node identities in a packed structure which uses
// only two words. It also places the statuses at the end so that when
// embedded in depEdgeTreeEntry, only 4 words in total are used.
type edgeKey struct {
	targets  [2]targetIdx
	statuses [2]uint8
}

// makeEdgeKey constructs an edgeKey for two nodes.
func makeEdgeKey(getTargetIdx getTargetIdxFunc, from, to *screl.Node) edgeKey {
	return edgeKey{
		targets:  [2]targetIdx{getTargetIdx(from), getTargetIdx(to)},
		statuses: [2]uint8{uint8(from.CurrentStatus), uint8(to.CurrentStatus)},
	}
}

// depEdgeTreeEntryKind is an entry in one of the two depEdges trees.
// The order indicates the tree this entry is a member of, and how to
// perform comparisons with other entries. The kind field is used to
// determine how to order the entry in the case of queries. The edge
// points to the value of the corresponding edge.
type depEdgeTreeEntry struct {
	edgeKey
	order edgeTreeOrder
	kind  depEdgeTreeEntryKind
	edge  *DepEdge
}

// depEdgeTreeEntryKind indicates whether the entry corresponds to an edge,
// a key start value or a query end value.
type depEdgeTreeEntryKind uint8

const (
	edge depEdgeTreeEntryKind = iota
	queryStart
	queryEnd
)

func (ek *depEdgeTreeEntry) Less(other btree.Item) bool {
	o := other.(*depEdgeTreeEntry)
	less, eq := cmpEdgeTreeEntry(ek, o, true /* first */)
	if eq {
		less, _ = cmpEdgeTreeEntry(ek, o, false /* first */)
	}
	return less
}

func cmpEdgeTreeEntry(a, b *depEdgeTreeEntry, first bool) (less, eq bool) {
	idx := 0
	if a.order == fromTo && !first ||
		a.order == toFrom && first {
		idx = 1
	}
	if ta, tb := a.targets[idx], b.targets[idx]; ta != tb {
		return ta < tb, false
	}
	if sa, sb := a.statuses[idx], b.statuses[idx]; sa != sb {
		return sa < sb, false
	}
	if a.kind != b.kind {
		return a.kind == queryStart || b.kind == queryEnd, false
	}
	return false, true
}

// depEdgeTreeEntryPool pools depEdgeTreeEntry objects. This turns out to be
// important because these objects are used when querying the depEdges, which
// happens many times.
var depEdgeTreeEntryPool = sync.Pool{
	New: func() interface{} {
		return &depEdgeTreeEntry{}
	},
}

func getDepEdgeTreeEntry() (a *depEdgeTreeEntry) {
	return depEdgeTreeEntryPool.Get().(*depEdgeTreeEntry)
}

func putDepEdgeTreeEntry(a *depEdgeTreeEntry) {
	*a = depEdgeTreeEntry{}
	depEdgeTreeEntryPool.Put(a)
}
