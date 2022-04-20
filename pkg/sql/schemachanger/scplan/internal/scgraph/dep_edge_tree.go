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

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/google/btree"
)

type depEdgeTree struct {
	t     *btree.BTree
	order edgeTreeOrder
	cmp   nodeCmpFn
	alloc *edgeAlloc
}

type nodeCmpFn func(a, b *screl.Node) (less, eq bool)

func newDepEdgeTree(order edgeTreeOrder, alloc *edgeAlloc, cmp nodeCmpFn) *depEdgeTree {
	const degree = 8 // arbitrary
	return &depEdgeTree{
		t:     btree.New(degree),
		order: order,
		cmp:   cmp,
		alloc: alloc,
	}
}

// edgeTreeOrder order in which the edge tree is sorted,
// either based on from/to node indexes.
type edgeTreeOrder bool

func (o edgeTreeOrder) first(e Edge) *screl.Node {
	if o == fromTo {
		return e.From()
	}
	return e.To()
}

func (o edgeTreeOrder) second(e Edge) *screl.Node {
	if o == toFrom {
		return e.From()
	}
	return e.To()
}

const (
	fromTo edgeTreeOrder = true
	toFrom edgeTreeOrder = false
)

// edgeTreeEntry BTree items for tracking edges
// in an ordered manner.
type edgeTreeEntry struct {
	t     *depEdgeTree
	alloc *edgeAlloc
	edge  *DepEdge
}

func (et *depEdgeTree) insert(edge *DepEdge) {
	e := et.alloc.edgeTreeEntry()
	e.t, e.edge = et, edge
	et.t.ReplaceOrInsert(e)
}

var edgeTreeEntryPool = sync.Pool{
	New: func() interface{} {
		return &edgeTreeEntry{edge: &DepEdge{}}
	},
}

func putEdgeTreeEntry(e *edgeTreeEntry) {
	e.t = nil
	*e.edge = DepEdge{}
	edgeTreeEntryPool.Put(e)
}

func (et *depEdgeTree) iterateSourceNode(n *screl.Node, it DepEdgeIterator) (err error) {
	e := edgeTreeEntryPool.Get().(*edgeTreeEntry)
	defer putEdgeTreeEntry(e)
	e.t = et
	if et.order == fromTo {
		e.edge.from = n
	} else {
		e.edge.to = n
	}
	et.t.AscendGreaterOrEqual(e, func(i btree.Item) (wantMore bool) {
		e := i.(*edgeTreeEntry)
		if et.order.first(e.edge) != n {
			return false
		}
		err = it(e.edge)
		return err == nil
	})
	if iterutil.Done(err) {
		err = nil
	}
	return err
}

// Less implements btree.Item.
func (e *edgeTreeEntry) Less(otherItem btree.Item) bool {
	o := otherItem.(*edgeTreeEntry)
	if less, eq := e.t.cmp(e.t.order.first(e.edge), e.t.order.first(o.edge)); !eq {
		return less
	}
	less, _ := e.t.cmp(e.t.order.second(e.edge), e.t.order.second(o.edge))
	return less
}
