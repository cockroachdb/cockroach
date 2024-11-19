// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
// the edge. For example, one cannot have a rule which indicates
// PreviousStagePrecedence and also SameStagePrecedence; that would be
// impossible to fulfill. Precedence is compatible with other kind of
// edge, but other kinds of edges are not compatible with each other.
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

// updateExistingDepEdge is called from insertOrUpdate when there already
// exists an edge in the graph between the two nodes. The logic asserts that
// the rule kinds are compatible, and adds the rule to the list of rules that
// this edge represents.
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
		fromTo, et.fromTo, et.getTargetIdx(n), n.CurrentStatus, it,
	)
}

// iterateFromNode iterates the set of edges to the passed node.
func (et *depEdges) iterateToNode(n *screl.Node, it DepEdgeIterator) (err error) {
	return iterateDepEdges(
		toFrom, et.toFrom, et.getTargetIdx(n), n.CurrentStatus, it,
	)
}

// iterateDepEdges iterates the dependency edges in the tree t in the requested
// order such that all edges visit match the requested target and status for
// the "first" entry in that order. If the order is fromTo, all edges from the
// (target, status) node will be visited, and if the order is toFrom, then all
// edges to that (target, status) node will be visited.
func iterateDepEdges(
	order edgeTreeOrder, t *btree.BTree, target targetIdx, status scpb.Status, it DepEdgeIterator,
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

// edgeKeyOrdinal corresponds to the ordinal in the arrays of edgeKey.
type edgeKeyOrdinal uint8

const (
	// fromOrdinal is used for the array entries corresponding to the from node.
	fromOrdinal edgeKeyOrdinal = 0
	// toOrdinal is used for the array entries corresponding to the to node.
	toOrdinal edgeKeyOrdinal = 1

	numEdgeKeyOrdinals int = 2
)

// edgeKey stores two node identities in a packed structure which uses
// only two words. It also places the statuses at the end so that when
// embedded in depEdgeTreeEntry, only 4 words in total are used.
type edgeKey struct {
	targets  [numEdgeKeyOrdinals]targetIdx
	statuses [numEdgeKeyOrdinals]uint8
}

// makeEdgeKey constructs an edgeKey for two nodes.
func makeEdgeKey(getTargetIdx getTargetIdxFunc, from, to *screl.Node) edgeKey {
	return edgeKey{
		targets: [numEdgeKeyOrdinals]targetIdx{
			fromOrdinal: getTargetIdx(from),
			toOrdinal:   getTargetIdx(to)},
		statuses: [numEdgeKeyOrdinals]uint8{
			fromOrdinal: uint8(from.CurrentStatus),
			toOrdinal:   uint8(to.CurrentStatus),
		},
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
	var ord edgeKeyOrdinal
	if a.order == fromTo && first || a.order == toFrom && !first {
		ord = fromOrdinal
	} else {
		ord = toOrdinal
	}
	if ta, tb := a.targets[ord], b.targets[ord]; ta != tb {
		return ta < tb, false
	}
	if sa, sb := a.statuses[ord], b.statuses[ord]; sa != sb {
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
