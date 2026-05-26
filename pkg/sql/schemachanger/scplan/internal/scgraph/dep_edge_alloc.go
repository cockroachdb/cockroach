// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scgraph

// depEdgeAlloc is used to amortize allocation of DepEdge objects.
type depEdgeAlloc struct {
	// a is the currently unfilled slice of DepEdges. Calls to new will extend
	// this slice until it reaches its capacity. When the capacity is reached,
	// a new slice will be allocated. In this way, we amortize the cost of
	// allocating new objects, and we avoid ever copying the allocated objects.
	a []DepEdge
}

// minCap and maxCap are used to control the dynamic allocation size for
// blocks of allocations.
const minCap, maxCap = 1 << 6 /* 64 */, 1 << 14 /* 16384 */

// new constructs a new DepEdge.
func (a *depEdgeAlloc) new(de DepEdge) *DepEdge {
	if a.a == nil {
		a.a = make([]DepEdge, 0, minCap)
	} else if len(a.a) == cap(a.a) {
		newCap := 2 * cap(a.a)
		if newCap > maxCap {
			newCap = maxCap
		}
		a.a = make([]DepEdge, 0, newCap)
	}
	idx := len(a.a)
	a.a = append(a.a, de)
	return &a.a[idx]
}

// depEdgeTreeEntryAlloc is used to amortize allocation of depEdgeTreeEntry
// values.
type depEdgeTreeEntryAlloc struct {
	// a is the currently unfilled slice of depEdgeTreeEntry objects. Calls to
	// new will extend this slice until it reaches its capacity. When the
	// capacity is reached, a new slice will be allocated. In this way, we
	// amortize the cost of allocating new objects, and we avoid ever copying
	// the allocated objects.
	a []depEdgeTreeEntry
}

// new constructs a new depEdgeTreeEntry.
func (a *depEdgeTreeEntryAlloc) new(k edgeKey, order edgeTreeOrder, de *DepEdge) *depEdgeTreeEntry {
	if a.a == nil {
		a.a = make([]depEdgeTreeEntry, 0, minCap)
	} else if len(a.a) == cap(a.a) {
		newCap := 2 * cap(a.a)
		if newCap > maxCap {
			newCap = maxCap
		}
		a.a = make([]depEdgeTreeEntry, 0, newCap)
	}
	idx := len(a.a)
	a.a = append(a.a, depEdgeTreeEntry{
		edgeKey: k, order: order, kind: edge, edge: de,
	})
	return &a.a[idx]
}
