// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package util

import (
	"fmt"
	"math"
)

// UnionFind implements the union find structure, with union by rank and path
// compression heuristics. Find and Union operations are effectively constant
// time. The data structure uses O(N) space where N is the largest element. It
// sizes itself as necessary; there are no allocations if Union is never called.
type UnionFind struct {
	nodes []ufNode
}

// ufNode represents an element of a UnionFind data structure. The elements are
// organized as nodes in a forest; two elements are in the same group if they
// are part of the same tree.
type ufNode struct {
	// parent is the parent node. All nodes in a tree are part of the same group;
	// internally, the representative of that group is the root node. A tree root
	// has itself as parent.
	parent uint32
	// the rank is the maximum depth of the subtree rooted at this node; only used
	// for the root of each tree.
	rank uint32
	// minimum element that is part of this group; only used for the root of each
	// tree. This is the externally visible representative of the group; we do
	// this to make the structure behave predictably and simplify things like
	// checking equality of two structures.
	minElement uint32
}

// Find returns the representative of the group that n is part of; this is the
// smallest element of that group.
// Two elements a, b are in the same group iff Find(a) == Find(b).
func (f *UnionFind) Find(n int) int {
	_, minElem := f.findRoot(n)
	return minElem
}

// findRoot returns the root of the subtree containing n and the smallest
// element in this set.
func (f *UnionFind) findRoot(n int) (subtreeRoot int, minElement int) {
	if n >= len(f.nodes) {
		// n was never touched by a Union call.
		return n, n
	}

	parent := f.nodes[uint32(n)].parent
	grandparent := f.nodes[parent].parent

	// Fast path if the element is connected directly to the root of the group (or
	// is the root).
	if grandparent == parent {
		return int(parent), int(f.nodes[parent].minElement)
	}

	i := grandparent
	for {
		p := f.nodes[i].parent
		if p == i {
			break
		}
		i = p
	}

	// Compress the path.
	root := i
	for i := uint32(n); i != root; {
		i, f.nodes[i].parent = f.nodes[i].parent, root
	}
	return int(root), int(f.nodes[root].minElement)
}

// Union joins the groups to which a and b belong.
func (f *UnionFind) Union(a, b int) {
	if a > math.MaxUint32 || a < 0 || b > math.MaxUint32 || b < 0 {
		panic(fmt.Sprintf("invalid args %d, %d", a, b))
	}
	// Substitute a, b with the roots of the corresponding trees.
	a, _ = f.findRoot(a)
	b, _ = f.findRoot(b)
	if a == b {
		// Already part of the same group.
		return
	}
	// Extend the slice with single-element groups as necessary.
	for i := len(f.nodes); i <= a || i <= b; i++ {
		f.nodes = append(f.nodes, ufNode{parent: uint32(i), minElement: uint32(i)})
	}
	an := &f.nodes[a]
	bn := &f.nodes[b]
	if an.rank < bn.rank {
		// A's tree is shallower than B's tree; choose B as the root.
		an.parent = uint32(b)
		if an.minElement < bn.minElement {
			bn.minElement = an.minElement
		}
	} else {
		// Choose A as the root.
		bn.parent = uint32(a)
		if an.rank == bn.rank {
			an.rank++
		}
		if bn.minElement < an.minElement {
			an.minElement = bn.minElement
		}
	}
}

// Len returns the number of elements up to and including the last element that
// is part of a multi-element group. In other words, if X is the largest value
// which was passed to a Union operation, the length is X+1.
func (f *UnionFind) Len() int {
	return len(f.nodes)
}

// Copy returns a copy of the structure which can be modified independently.
func (f *UnionFind) Copy() UnionFind {
	return UnionFind{nodes: append([]ufNode(nil), f.nodes...)}
}

// Equals returns true if the two structures have the same sets.
func (f *UnionFind) Equals(rhs UnionFind) bool {
	if len(f.nodes) != len(rhs.nodes) {
		return false
	}
	for i := range f.nodes {
		if f.Find(i) != rhs.Find(i) {
			return false
		}
	}
	return true
}
