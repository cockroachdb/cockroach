// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package aug

import (
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/btree/ordered"
)

// Node represents a Node in the tree.
type Node[C ordered.Comparator[K], K, V, A any] struct {
	ref      int32
	Count    int16
	Cmp      C
	Aug      A
	Keys     [MaxEntries]K
	Values   [MaxEntries]V
	Children *[MaxEntries + 1]*Node[C, K, V, A]
}

type interiorNode[C ordered.Comparator[K], K, V, A any] struct {
	Node[C, K, V, A]
	children [MaxEntries + 1]*Node[C, K, V, A]
}

func (n *Node[C, K, V, A]) IsLeaf() bool {
	return n.Children == nil
}

// incRef acquires a reference to the Node.
func (n *Node[C, K, V, A]) incRef() {
	atomic.AddInt32(&n.ref, 1)
}

func (n *Node[C, K, V, A]) insertAt(index int, item K, value V, nd *Node[C, K, V, A]) {
	if index < int(n.Count) {
		copy(n.Keys[index+1:n.Count+1], n.Keys[index:n.Count])
		copy(n.Values[index+1:n.Count+1], n.Values[index:n.Count])
		if !n.IsLeaf() {
			copy(n.Children[index+2:n.Count+2], n.Children[index+1:n.Count+1])
		}
	}
	n.Count++
	n.Keys[index] = item
	n.Values[index] = value
	if !n.IsLeaf() {
		n.Children[index+1] = nd
	}
}

func (n *Node[C, K, V, A]) pushBack(item K, value V, nd *Node[C, K, V, A]) {
	n.Keys[n.Count] = item
	n.Values[n.Count] = value
	if !n.IsLeaf() {
		n.Children[n.Count+1] = nd
	}
	n.Count++
}

func (n *Node[C, K, V, A]) pushFront(item K, value V, nd *Node[C, K, V, A]) {
	if !n.IsLeaf() {
		copy(n.Children[1:n.Count+2], n.Children[:n.Count+1])
		n.Children[0] = nd
	}
	copy(n.Keys[1:n.Count+1], n.Keys[:n.Count])
	copy(n.Values[1:n.Count+1], n.Values[:n.Count])
	n.Keys[0] = item
	n.Values[0] = value
	n.Count++
}

// removeAt removes a value at a given index, pulling all subsequent Values
// back.
func (n *Node[C, K, V, A]) removeAt(index int) (K, V, *Node[C, K, V, A]) {
	var child *Node[C, K, V, A]
	if !n.IsLeaf() {
		child = n.Children[index+1]
		copy(n.Children[index+1:n.Count], n.Children[index+2:n.Count+1])
		n.Children[n.Count] = nil
	}
	n.Count--
	outK := n.Keys[index]
	outV := n.Values[index]
	copy(n.Keys[index:n.Count], n.Keys[index+1:n.Count+1])
	copy(n.Values[index:n.Count], n.Values[index+1:n.Count+1])
	var rk K
	var rv V
	n.Keys[n.Count] = rk
	n.Values[n.Count] = rv
	return outK, outV, child
}

// popBack removes and returns the last element in the list.
func (n *Node[C, K, V, A]) popBack() (K, V, *Node[C, K, V, A]) {
	n.Count--
	outK := n.Keys[n.Count]
	outV := n.Values[n.Count]
	var rK K
	var rV V
	n.Keys[n.Count] = rK
	n.Values[n.Count] = rV
	if n.IsLeaf() {
		return outK, outV, nil
	}
	child := n.Children[n.Count+1]
	n.Children[n.Count+1] = nil
	return outK, outV, child
}

// popFront removes and returns the first element in the list.
func (n *Node[C, K, V, A]) popFront() (K, V, *Node[C, K, V, A]) {
	n.Count--
	var child *Node[C, K, V, A]
	if !n.IsLeaf() {
		child = n.Children[0]
		copy(n.Children[:n.Count+1], n.Children[1:n.Count+2])
		n.Children[n.Count+1] = nil
	}
	outK := n.Keys[0]
	outV := n.Values[0]
	copy(n.Keys[:n.Count], n.Keys[1:n.Count+1])
	copy(n.Values[:n.Count], n.Values[1:n.Count+1])
	var rK K
	var rV V
	n.Keys[n.Count] = rK
	n.Values[n.Count] = rV
	return outK, outV, child
}

// find returns the index where the given item should be inserted into this
// list. 'found' is true if the item already exists in the list at the given
// index.
func (n *Node[C, K, V, A]) find(item K) (index int, found bool) {
	// Logic copied from sort.Search. Inlining this gave
	// an 11% speedup on BenchmarkBTreeDeleteInsert.
	i, j := 0, int(n.Count)
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		// i ≤ h < j
		v := n.Cmp.Compare(item, n.Keys[h])
		if v == 0 {
			return h, true
		} else if v > 0 {
			i = h + 1
		} else {
			j = h
		}
	}
	return i, false
}

func (n *Node[C, K, V, A]) writeString(b *strings.Builder) {
	if n.IsLeaf() {
		for i := int16(0); i < n.Count; i++ {
			if i != 0 {
				b.WriteString(",")
			}
			_, _ = fmt.Fprintf(b, "%v:%v", n.Keys[i], n.Values[i])
		}
		return
	}
	for i := int16(0); i <= n.Count; i++ {
		b.WriteString("(")
		n.Children[i].writeString(b)
		b.WriteString(")")
		if i < n.Count {
			_, _ = fmt.Fprintf(b, "%v:%v", n.Keys[i], n.Values[i])
		}
	}
}
