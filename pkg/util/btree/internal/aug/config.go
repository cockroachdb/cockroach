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
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/btree/ordered"
)

// Updater is used to update the augmentation of the Node when the subtree
// changes.
type Updater[C ordered.Comparator[K], K, V, A any] interface {
	~struct{}
	UpdateOnInsert(_, _ *Node[C, K, V, A], _ K) (changed bool)
	UpdateOnRemoval(_, _ *Node[C, K, V, A], _ K) (changed bool)
	UpdateOnSplit(_, _ *Node[C, K, V, A], _ K) (changed bool)
	UpdateDefault(*Node[C, K, V, A]) (changed bool)
}

type config[
	C ordered.Comparator[K],
	K, V, A any, U Updater[C, K, V, A],
] struct {
	*nodePool[C, K, V, A]
	cmp C
}

// getLeafNode sets the comparator on a node from the underlying nodePool.
func (cfg config[C, K, V, A, U]) getLeafNode() *Node[C, K, V, A] {
	n := cfg.nodePool.getLeafNode()
	n.Cmp = cfg.cmp
	return n
}

// getInteriorNode sets the comparator on a node from the underlying nodePool.
func (cfg config[C, K, V, A, U]) getInteriorNode() *Node[C, K, V, A] {
	n := cfg.nodePool.getInteriorNode()
	n.Cmp = cfg.cmp
	return n
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
func (cfg config[C, K, V, A, U]) mut(n **Node[C, K, V, A]) *Node[C, K, V, A] {
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
	c := cfg.clone(*n)
	cfg.decRef(*n, true /* recursive */)
	*n = c
	return *n
}

// decRef releases a reference to the Node. If requested, the method
// will recurse into child nodes and decrease their refcounts as well.
func (cfg config[C, K, V, A, U]) decRef(n *Node[C, K, V, A], recursive bool) {
	if atomic.AddInt32(&n.ref, -1) > 0 {
		// Other references remain. Can't free.
		return
	}
	// Clear and release Node into memory pool.
	if n.IsLeaf() {
		cfg.putLeafNode(n)
	} else {
		// Release child references first, if requested.
		if recursive {
			for i := int16(0); i <= n.Count; i++ {
				cfg.decRef(n.Children[i], true /* recursive */)
			}
		}
		cfg.putInteriorNode(n)
	}
}

// clone creates a clone of the receiver with a single reference Count.
func (cfg config[C, K, V, A, U]) clone(n *Node[C, K, V, A]) *Node[C, K, V, A] {
	var c *Node[C, K, V, A]
	if n.IsLeaf() {
		c = cfg.getLeafNode()
	} else {
		c = cfg.getInteriorNode()
	}
	// NB: copy field-by-field without touching N.N.ref to avoid
	// triggering the race detector and looking like a data race.
	c.Count = n.Count
	c.Aug = n.Aug
	c.Keys = n.Keys
	c.Values = n.Values
	if !c.IsLeaf() {
		// Copy Children and increase each refcount.
		*c.Children = *n.Children
		for i := int16(0); i <= c.Count; i++ {
			c.Children[i].incRef()
		}
	}
	return c
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
func (cfg config[C, K, V, A, U]) split(n *Node[C, K, V, A], i int) (K, V, *Node[C, K, V, A]) {
	outK := n.Keys[i]
	outV := n.Values[i]
	var next *Node[C, K, V, A]
	if n.IsLeaf() {
		next = cfg.getLeafNode()
	} else {
		next = cfg.getInteriorNode()
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
	var u U
	u.UpdateDefault(next)
	u.UpdateOnSplit(n, next, outK)
	return outK, outV, next
}

// insert inserts an item into the suAugBTree rooted at this Node, making sure no
// nodes in the suAugBTree exceed MaxEntries Keys. Returns true if an existing item
// was replaced and false if an item was inserted. Also returns whether the
// Node's upper bound changes.
func (cfg config[C, K, V, A, U]) insert(
	n *Node[C, K, V, A], item K, value V,
) (replacedK K, replacedV V, replaced, newBound bool) {
	i, found := n.find(item)
	if found {
		replacedV = n.Values[i]
		replacedK = n.Keys[i]
		n.Keys[i] = item
		n.Values[i] = value
		return replacedK, replacedV, true, false
	}
	if n.IsLeaf() {
		n.insertAt(i, item, value, nil)
		var u U
		return replacedK, replacedV, false,
			u.UpdateOnInsert(n, nil, item)
	}
	if n.Children[i].Count >= MaxEntries {
		splitLK, splitLV, splitNode := cfg.split(
			cfg.mut(&n.Children[i]), MaxEntries/2,
		)
		n.insertAt(i, splitLK, splitLV, splitNode)
		switch c := n.Cmp.Compare(item, n.Keys[i]); {
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
	replacedK, replacedV, replaced, newBound = cfg.insert(
		cfg.mut(&n.Children[i]), item, value,
	)

	if newBound {
		var u U
		newBound = u.UpdateOnInsert(n, nil, item)
	}
	return replacedK, replacedV, replaced, newBound
}

// removeMax removes and returns the maximum item from the suAugBTree rooted at
// this Node.
func (cfg config[C, K, V, A, U]) removeMax(n *Node[C, K, V, A]) (K, V) {
	if n.IsLeaf() {
		n.Count--
		outK := n.Keys[n.Count]
		outV := n.Values[n.Count]
		var rK K
		var rV V
		n.Keys[n.Count] = rK
		n.Values[n.Count] = rV
		var u U
		u.UpdateOnRemoval(n, nil, outK)
		return outK, outV
	}
	// Recurse into max child.
	i := int(n.Count)
	if n.Children[i].Count <= MinEntries {
		// Child not large enough to remove from.
		cfg.rebalanceOrMerge(n, i)
		return cfg.removeMax(n) // redo
	}
	child := cfg.mut(&n.Children[i])
	outK, outV := cfg.removeMax(child)
	var u U
	u.UpdateOnRemoval(n, nil, outK)
	return outK, outV
}

// rebalanceOrMerge grows child 'i' to ensure it has sufficient room to remove
// an item from it while keeping it at or above MinItems.
func (cfg config[C, K, V, A, U]) rebalanceOrMerge(n *Node[C, K, V, A], i int) {
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
		left := cfg.mut(&n.Children[i-1])
		child := cfg.mut(&n.Children[i])
		xLaK, xLaV, grandChild := left.popBack()
		yLaK, yLaV := n.Keys[i-1], n.Values[i-1]
		child.pushFront(yLaK, yLaV, grandChild)
		n.Keys[i-1], n.Values[i-1] = xLaK, xLaV
		var u U
		u.UpdateOnRemoval(left, grandChild, xLaK)
		u.UpdateOnInsert(child, grandChild, yLaK)

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
		right := cfg.mut(&n.Children[i+1])
		child := cfg.mut(&n.Children[i])
		xLaK, xLaV, grandChild := right.popFront()
		yLaK, yLaV := n.Keys[i], n.Values[i]
		child.pushBack(yLaK, yLaV, grandChild)
		n.Keys[i], n.Values[i] = xLaK, xLaV
		var u U
		u.UpdateOnRemoval(right, grandChild, xLaK)
		u.UpdateOnInsert(child, grandChild, yLaK)

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
		child := cfg.mut(&n.Children[i])
		// Make mergeChild mutable, bumping the refcounts on its Children if necessary.
		_ = cfg.mut(&n.Children[i+1])
		mergeLaK, mergeLaV, mergeChild := n.removeAt(i)
		child.Keys[child.Count] = mergeLaK
		child.Values[child.Count] = mergeLaV
		copy(child.Keys[child.Count+1:], mergeChild.Keys[:mergeChild.Count])
		copy(child.Values[child.Count+1:], mergeChild.Values[:mergeChild.Count])
		if !child.IsLeaf() {
			copy(child.Children[child.Count+1:], mergeChild.Children[:mergeChild.Count+1])
		}
		child.Count += mergeChild.Count + 1

		var u U
		u.UpdateOnInsert(child, mergeChild, mergeLaK)
		cfg.decRef(mergeChild, false /* recursive */)
	}
}

// remove removes an item from the suAugBTree rooted at this Node. Returns the item
// that was removed or nil if no matching item was found. Also returns whether
// the Node's upper bound changes.
func (cfg config[C, K, V, A, U]) remove(
	n *Node[C, K, V, A], item K,
) (outK K, outV V, found, newBound bool) {
	i, found := n.find(item)
	if n.IsLeaf() {
		if found {
			outK, outV, _ = n.removeAt(i)
			var u U
			return outK, outV, true, u.UpdateOnRemoval(n, nil, outK)
		}
		var rK K
		var rV V
		return rK, rV, false, false
	}
	if n.Children[i].Count <= MinEntries {
		// Child not large enough to remove from.
		cfg.rebalanceOrMerge(n, i)
		return cfg.remove(n, item) // redo
	}
	child := cfg.mut(&n.Children[i])
	if found {
		// Replace the item being removed with the max item in our left child.
		outK = n.Keys[i]
		outV = n.Values[i]
		n.Keys[i], n.Values[i] = cfg.removeMax(child)
		var u U
		return outK, outV, true, u.UpdateOnRemoval(n, nil, outK)
	}
	// Latch is not in this Node and child is large enough to remove from.
	outK, outV, found, newBound = cfg.remove(child, item)
	if newBound {
		var u U
		newBound = u.UpdateOnRemoval(n, nil, outK)
	}
	return outK, outV, found, newBound
}
