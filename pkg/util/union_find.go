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

type ufNode struct {
	// parent is the parent node. All nodes in a tree are part of the same group;
	// the representative of that group is the root node.
	parent int32
	// the rank is the maximum depth of the subtree rooted at this node; only used
	// for the root of each tree.
	rank int32
}

// UnionFind implements the union find structure, with union by rank and path
// compression heuristics. Find and Union operations are effectively constant
// time. The data structure uses O(N) space where N is the largest element. It
// sizes itself as necessary; there are no allocations if Union is never called.
type UnionFind struct {
	nodes []ufNode
}

// Find returns the representative of the group that n is part of.
// Two elements a, b are in the same group iff Find(a) == Find(b).
func (f *UnionFind) Find(n int32) int32 {
	if int(n) >= len(f.nodes) {
		// n was never touched by a Union call.
		return n
	}

	parent := f.nodes[n].parent
	grandparent := f.nodes[parent].parent

	// Fast path if the element is connected directly to the root of the group (or
	// is the root).
	if grandparent == parent {
		return parent
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
	for i := n; i != root; {
		i, f.nodes[i].parent = f.nodes[i].parent, root
	}
	return root
}

// Union joins the groups to which a and b belong.
func (f *UnionFind) Union(a, b int32) {
	// Substitute a, b with the roots of the corresponding trees.
	a = f.Find(a)
	b = f.Find(b)
	if a == b {
		// Already part of the same group.
		return
	}
	// Extend the slice with single-element groups as necessary.
	for i := int32(len(f.nodes)); i <= a || i <= b; i++ {
		f.nodes = append(f.nodes, ufNode{parent: i})
	}
	an := &f.nodes[a]
	bn := &f.nodes[b]
	if an.rank < bn.rank {
		// A's tree is shallower than B's tree; choose B as the root.
		an.parent = b
	} else {
		// Choose A as the root.
		bn.parent = a
		if an.rank == bn.rank {
			an.rank++
		}
	}
}
