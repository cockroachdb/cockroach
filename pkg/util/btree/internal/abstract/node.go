// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package abstract

import (
	"fmt"
	"strings"
	"sync/atomic"
)

// Node represents a Node in the tree.
type Node[K, V, A any] struct {
	ref      int32
	Count    int16
	Aug      A
	Keys     [MaxEntries]K
	Values   [MaxEntries]V
	Children *[MaxEntries + 1]*Node[K, V, A]
}

type interiorNode[K, V, A any] struct {
	Node[K, V, A]
	children [MaxEntries + 1]*Node[K, V, A]
}

func (n *Node[K, V, A]) IsLeaf() bool {
	return n.Children == nil
}

// mut creates and returns a mutable Node reference. If the Node is not shared
// with any other trees then it can be modified in place. Otherwise, it must be
// cloned to ensure unique ownership. In this way, we enforce a copy-on-write
// policy which transparently incorporates the idea of local mutations, like
// Clojure's transients or Haskell's ST monad, where nodes are only copied
// during the first time that they are modified between Clone operations.
//
// When a Node is cloned, the provided pointer will be redirected to the new
// mutable Node.
func mut[K, V, A any](np *nodePool[K, V, A], n **Node[K, V, A]) *Node[K, V, A] {
	if atomic.LoadInt32(&(*n).ref) == 1 {
		// Exclusive ownership. Can mutate in place.
		return *n
	}
	// If we do not have unique ownership over the Node then we
	// clone it to gain unique ownership. After doing so, we can
	// release our reference to the old Node. We pass recursive
	// as true because even though we just observed the Node's
	// reference Count to be greater than 1, we might be racing
	// with another call to decRef on this Node.
	c := (*n).clone(np)
	(*n).decRef(np, true /* recursive */)
	*n = c
	return *n
}

// incRef acquires a reference to the Node.
func (n *Node[K, V, A]) incRef() {
	atomic.AddInt32(&n.ref, 1)
}

// decRef releases a reference to the Node. If requested, the method
// will recurse into child nodes and decrease their refcounts as well.
func (n *Node[K, V, A]) decRef(np *nodePool[K, V, A], recursive bool) {
	if atomic.AddInt32(&n.ref, -1) > 0 {
		// Other references remain. Can't free.
		return
	}
	// Clear and release Node into memory pool.
	if n.IsLeaf() {
		np.putLeafNode(n)
	} else {
		// Release child references first, if requested.
		if recursive {
			for i := int16(0); i <= n.Count; i++ {
				n.Children[i].decRef(np, true /* recursive */)
			}
		}
		np.putInteriorNode(n)
	}
}

// clone creates a clone of the receiver with a single reference Count.
func (n *Node[K, V, A]) clone(np *nodePool[K, V, A]) *Node[K, V, A] {
	var c *Node[K, V, A]
	if n.IsLeaf() {
		c = np.getLeafNode()
	} else {
		c = np.getInteriorNode()
	}
	// NB: copy field-by-field without touching N.N.ref to avoid
	// triggering the race detector and looking like a data race.
	c.Count = n.Count
	n.Aug = c.Aug
	c.Keys = n.Keys
	if !c.IsLeaf() {
		// Copy Children and increase each refcount.
		*c.Children = *n.Children
		for i := int16(0); i <= c.Count; i++ {
			c.Children[i].incRef()
		}
	}
	return c
}

func (n *Node[K, V, A]) insertAt(index int, item K, value V, nd *Node[K, V, A]) {
	if index < int(n.Count) {
		copy(n.Keys[index+1:n.Count+1], n.Keys[index:n.Count])
		copy(n.Values[index+1:n.Count+1], n.Values[index:n.Count])
		if !n.IsLeaf() {
			copy(n.Children[index+2:n.Count+2], n.Children[index+1:n.Count+1])
		}
	}
	n.Keys[index] = item
	n.Values[index] = value
	if !n.IsLeaf() {
		n.Children[index+1] = nd
	}
	n.Count++
}

func (n *Node[K, V, A]) pushBack(item K, value V, nd *Node[K, V, A]) {
	n.Keys[n.Count] = item
	n.Values[n.Count] = value
	if !n.IsLeaf() {
		n.Children[n.Count+1] = nd
	}
	n.Count++
}

func (n *Node[K, V, A]) pushFront(item K, value V, nd *Node[K, V, A]) {
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
func (n *Node[K, V, A]) removeAt(index int) (K, V, *Node[K, V, A]) {
	var child *Node[K, V, A]
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
func (n *Node[K, V, A]) popBack() (K, V, *Node[K, V, A]) {
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
func (n *Node[K, V, A]) popFront() (K, V, *Node[K, V, A]) {
	n.Count--
	var child *Node[K, V, A]
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
func (n *Node[K, V, A]) find(cmp func(K, K) int, item K) (index int, found bool) {
	// Logic copied from sort.Search. Inlining this gave
	// an 11% speedup on BenchmarkBTreeDeleteInsert.
	i, j := 0, int(n.Count)
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		// i ≤ h < j
		v := cmp(item, n.Keys[h])
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

// split splits the given Node at the given index. The current Node shrinks,
// and this function returns the item that existed at that index and a new
// Node containing all Keys/Children after it.
//
// Before:
//
//	+-----------+
//	|   x y z   |
//	+--/-/-\-\--+
//
// After:
//
//	+-----------+
//	|     y     |
//	+----/-\----+
//	    /   \
//	   v     v
//
// +-----------+     +-----------+
// |         x |     | z         |
// +-----------+     +-----------+
func split[K, V, A any, U Updater[K, V, A]](
	n *Node[K, V, A], cfg *Config[K, V, A, U], i int,
) (K, V, *Node[K, V, A]) {
	outK := n.Keys[i]
	outV := n.Values[i]
	var next *Node[K, V, A]
	if n.IsLeaf() {
		next = cfg.np.getLeafNode()
	} else {
		next = cfg.np.getInteriorNode()
	}
	next.Count = n.Count - int16(i+1)
	copy(next.Keys[:], n.Keys[i+1:n.Count])
	copy(next.Values[:], n.Values[i+1:n.Count])
	var rK K
	var rV V
	for j := int16(i); j < n.Count; j++ {
		n.Keys[j] = rK
		n.Values[j] = rV
	}
	if !n.IsLeaf() {
		copy(next.Children[:], n.Children[i+1:n.Count+1])
		for j := int16(i + 1); j <= n.Count; j++ {
			n.Children[j] = nil
		}
	}
	n.Count = int16(i)
	cfg.Updater.UpdateDefault(next)
	cfg.Updater.UpdateOnSplit(n, next, outK)
	return outK, outV, next
}

// insert inserts an item into the suAugBTree rooted at this Node, making sure no
// nodes in the suAugBTree exceed MaxEntries Keys. Returns true if an existing item
// was replaced and false if an item was inserted. Also returns whether the
// Node's upper bound changes.
func insert[K, V, A any, U Updater[K, V, A]](
	n *Node[K, V, A], cfg *Config[K, V, A, U], item K, value V,
) (replacedK K, replacedV V, replaced, newBound bool) {
	i, found := n.find(cfg.Cmp, item)
	if found {
		replacedV = n.Values[i]
		replacedK = n.Keys[i]
		n.Keys[i] = item
		n.Values[i] = value
		return replacedK, replacedV, true, false
	}
	if n.IsLeaf() {
		n.insertAt(i, item, value, nil)
		return replacedK, replacedV, false,
			cfg.Updater.UpdateOnInsert(n, nil, item)
	}
	if n.Children[i].Count >= MaxEntries {
		splitLK, splitLV, splitNode := split(
			mut(cfg.np, &n.Children[i]), cfg, MaxEntries/2,
		)
		n.insertAt(i, splitLK, splitLV, splitNode)
		switch c := cfg.Cmp(item, n.Keys[i]); {
		case c < 0:
			// no change, we want first split Node
		case c > 0:
			i++ // we want second split Node
		default:
			replacedK, replacedV = n.Keys[i], n.Values[i]
			n.Keys[i], n.Values[i] = item, value
			return replacedK, replacedV, true, false
		}
	}
	replacedK, replacedV, replaced, newBound = insert(
		mut(cfg.np, &n.Children[i]), cfg, item, value,
	)

	if newBound {
		newBound = cfg.Updater.UpdateOnInsert(n, nil, item)
	}
	return replacedK, replacedV, replaced, newBound
}

// removeMax removes and returns the maximum item from the suAugBTree rooted at
// this Node.
func removeMax[K, V, A any, U Updater[K, V, A]](n *Node[K, V, A], cfg *Config[K, V, A, U]) (K, V) {
	if n.IsLeaf() {
		n.Count--
		outK := n.Keys[n.Count]
		outV := n.Values[n.Count]
		var rK K
		var rV V
		n.Keys[n.Count] = rK
		n.Values[n.Count] = rV
		cfg.Updater.UpdateOnRemoval(n, nil, outK)
		return outK, outV
	}
	// Recurse into max child.
	i := int(n.Count)
	if n.Children[i].Count <= MinEntries {
		// Child not large enough to remove from.
		rebalanceOrMerge(n, cfg, i)
		return removeMax(n, cfg) // redo
	}
	child := mut(cfg.np, &n.Children[i])
	outK, outV := removeMax(child, cfg)
	cfg.Updater.UpdateOnRemoval(n, nil, outK)
	return outK, outV
}

// rebalanceOrMerge grows child 'i' to ensure it has sufficient room to remove
// an item from it while keeping it at or above MinItems.
func rebalanceOrMerge[K, V, A any, U Updater[K, V, A]](
	n *Node[K, V, A], cfg *Config[K, V, A, U], i int,
) {
	switch {
	case i > 0 && n.Children[i-1].Count > MinEntries:
		// Rebalance from left sibling.
		//
		//          +-----------+
		//          |     y     |
		//          +----/-\----+
		//              /   \
		//             v     v
		// +-----------+     +-----------+
		// |         x |     |           |
		// +----------\+     +-----------+
		//             \
		//              v
		//              a
		//
		// After:
		//
		//          +-----------+
		//          |     x     |
		//          +----/-\----+
		//              /   \
		//             v     v
		// +-----------+     +-----------+
		// |           |     | y         |
		// +-----------+     +/----------+
		//                   /
		//                  v
		//                  a
		//
		left := mut(cfg.np, &n.Children[i-1])
		child := mut(cfg.np, &n.Children[i])
		xLaK, xLaV, grandChild := left.popBack()
		yLaK, yLaV := n.Keys[i-1], n.Values[i-1]
		child.pushFront(yLaK, yLaV, grandChild)
		n.Keys[i-1], n.Values[i-1] = xLaK, xLaV
		cfg.Updater.UpdateOnRemoval(left, grandChild, xLaK)
		cfg.Updater.UpdateOnInsert(child, grandChild, yLaK)

	case i < int(n.Count) && n.Children[i+1].Count > MinEntries:
		// Rebalance from right sibling.
		//
		//          +-----------+
		//          |     y     |
		//          +----/-\----+
		//              /   \
		//             v     v
		// +-----------+     +-----------+
		// |           |     | x         |
		// +-----------+     +/----------+
		//                   /
		//                  v
		//                  a
		//
		// After:
		//
		//          +-----------+
		//          |     x     |
		//          +----/-\----+
		//              /   \
		//             v     v
		// +-----------+     +-----------+
		// |         y |     |           |
		// +----------\+     +-----------+
		//             \
		//              v
		//              a
		//
		right := mut(cfg.np, &n.Children[i+1])
		child := mut(cfg.np, &n.Children[i])
		xLaK, xLaV, grandChild := right.popFront()
		yLaK, yLaV := n.Keys[i], n.Values[i]
		child.pushBack(yLaK, yLaV, grandChild)
		n.Keys[i], n.Values[i] = xLaK, xLaV
		cfg.Updater.UpdateOnRemoval(right, grandChild, xLaK)
		cfg.Updater.UpdateOnInsert(child, grandChild, yLaK)

	default:
		// Merge with either the left or right sibling.
		//
		//          +-----------+
		//          |   u y v   |
		//          +----/-\----+
		//              /   \
		//             v     v
		// +-----------+     +-----------+
		// |         x |     | z         |
		// +-----------+     +-----------+
		//
		// After:
		//
		//          +-----------+
		//          |    u v    |
		//          +-----|-----+
		//                |
		//                v
		//          +-----------+
		//          |   x y z   |
		//          +-----------+
		//
		if i >= int(n.Count) {
			i = int(n.Count - 1)
		}
		child := mut(cfg.np, &n.Children[i])
		// Make mergeChild mutable, bumping the refcounts on its Children if necessary.
		_ = mut(cfg.np, &n.Children[i+1])
		mergeLaK, mergeLaV, mergeChild := n.removeAt(i)
		child.Keys[child.Count] = mergeLaK
		child.Values[child.Count] = mergeLaV
		copy(child.Keys[child.Count+1:], mergeChild.Keys[:mergeChild.Count])
		copy(child.Values[child.Count+1:], mergeChild.Values[:mergeChild.Count])
		if !child.IsLeaf() {
			copy(child.Children[child.Count+1:], mergeChild.Children[:mergeChild.Count+1])
		}
		child.Count += mergeChild.Count + 1

		cfg.Updater.UpdateOnInsert(child, mergeChild, mergeLaK)
		mergeChild.decRef(cfg.np, false /* recursive */)
	}
}

// remove removes an item from the suAugBTree rooted at this Node. Returns the item
// that was removed or nil if no matching item was found. Also returns whether
// the Node's upper bound changes.
func remove[K, V, A any, U Updater[K, V, A]](
	n *Node[K, V, A], cfg *Config[K, V, A, U], item K,
) (outK K, outV V, found, newBound bool) {
	i, found := n.find(cfg.Cmp, item)
	if n.IsLeaf() {
		if found {
			outK, outV, _ = n.removeAt(i)
			return outK, outV, true, cfg.Updater.UpdateOnRemoval(n, nil, outK)
		}
		var rK K
		var rV V
		return rK, rV, false, false
	}
	if n.Children[i].Count <= MinEntries {
		// Child not large enough to remove from.
		rebalanceOrMerge(n, cfg, i)
		return remove(n, cfg, item) // redo
	}
	child := mut(cfg.np, &n.Children[i])
	if found {
		// Replace the item being removed with the max item in our left child.
		outK = n.Keys[i]
		outV = n.Values[i]
		n.Keys[i], n.Values[i] = removeMax(child, cfg)
		return outK, outV, true, cfg.Updater.UpdateOnRemoval(n, nil, outK)
	}
	// Latch is not in this Node and child is large enough to remove from.
	outK, outV, found, newBound = remove(child, cfg, item)
	if newBound {
		newBound = cfg.Updater.UpdateOnRemoval(n, nil, outK)
	}
	return outK, outV, found, newBound
}

func (n *Node[K, V, A]) writeString(b *strings.Builder) {
	if n.IsLeaf() {
		for i := int16(0); i < n.Count; i++ {
			if i != 0 {
				b.WriteString(",")
			}
			fmt.Fprintf(b, "%v:%v", n.Keys[i], n.Values[i])
		}
		return
	}
	for i := int16(0); i <= n.Count; i++ {
		b.WriteString("(")
		n.Children[i].writeString(b)
		b.WriteString(")")
		if i < n.Count {
			fmt.Fprintf(b, "%v:%v", n.Keys[i], n.Values[i])
		}
	}
}
