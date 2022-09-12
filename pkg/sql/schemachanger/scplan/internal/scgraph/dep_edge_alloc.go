// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scgraph

// depEdgeAlloc is used to amortize allocation of DepEdge objects.
type depEdgeAlloc struct {
	a []DepEdge
}

// minCap and maxCap are used to control the dynamic allocation size for
// blocks of allocations.
const minCap, maxCap = 64, 16384

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
