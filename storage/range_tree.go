// Copyright 2015 The Cockroach Authors.
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
//
// Author: Bram Gruneir (bram+code@cockroachlabs.com)

package storage

import (
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/engine"
	"golang.org/x/net/context"
)

// cachedNode is an in memory cache for use during range tree manipulations.
type cachedNode struct {
	node  *RangeTreeNode
	dirty bool
}

// RangeTree is used to hold the relevant context information for any
// operations on the range tree.
type treeContext struct {
	txn   *client.Txn
	tree  *RangeTree
	dirty bool
	nodes map[string]cachedNode
}

// SetupRangeTree creates a new RangeTree. This should only be called as part
// of store.BootstrapRange.
// TODO(tschottdorf): other RangeTree operations should also propagate a Context.
func SetupRangeTree(ctx context.Context, batch engine.Engine, ms *engine.MVCCStats, timestamp roachpb.Timestamp, startKey roachpb.RKey) error {
	tree := &RangeTree{
		RootKey: startKey,
	}
	node := &RangeTreeNode{
		Key:   startKey,
		Black: true,
	}
	if err := engine.MVCCPutProto(ctx, batch, ms, keys.RangeTreeRoot, timestamp, nil, tree); err != nil {
		return err
	}
	if err := engine.MVCCPutProto(ctx, batch, ms, keys.RangeTreeNodeKey(startKey), timestamp, nil, node); err != nil {
		return err
	}
	return nil
}

// flush writes all dirty nodes and the tree to the transaction.
func (tc *treeContext) flush(b *client.Batch) {
	if tc.dirty {
		b.Put(keys.RangeTreeRoot, tc.tree)
	}
	for key, cachedNode := range tc.nodes {
		if cachedNode.dirty {
			if cachedNode.node == nil {
				b.Del(keys.RangeTreeNodeKey(roachpb.RKey(key)))
			} else {
				b.Put(keys.RangeTreeNodeKey(roachpb.RKey(key)), cachedNode.node)
			}
		}
	}
}

// GetRangeTree fetches the RangeTree proto and sets up the range tree context.
func getRangeTree(txn *client.Txn) (*treeContext, *roachpb.Error) {
	tree := new(RangeTree)
	if pErr := txn.GetProto(keys.RangeTreeRoot, tree); pErr != nil {
		return nil, pErr
	}
	return &treeContext{
		txn:   txn,
		tree:  tree,
		dirty: false,
		nodes: map[string]cachedNode{},
	}, nil
}

// setRoot sets the tree root key in the cache. It also marks the root for
// writing during a flush.
func (tc *treeContext) setRootKey(key roachpb.RKey) {
	tc.tree.RootKey = key
	tc.dirty = true
}

// setNode sets the node in the cache so all subsequent reads will read this
// upated value. It also marks the node as dirty for writing during a flush.
func (tc *treeContext) setNode(node *RangeTreeNode) {
	tc.nodes[string(node.Key)] = cachedNode{
		node:  node,
		dirty: true,
	}
}

// dropNode sets a node in the cache to nil. It also marks the node as dirty for
// writing during a flush.
func (tc *treeContext) dropNode(key roachpb.RKey) {
	tc.nodes[string(key)] = cachedNode{
		node:  nil,
		dirty: true,
	}
}

// getNode returns the RangeTreeNode for the given key. If the key is nil, nil
// is returned.
func (tc *treeContext) getNode(key roachpb.RKey) (*RangeTreeNode, *roachpb.Error) {
	if key == nil {
		return nil, nil
	}

	// First check to see if we have the node cached.
	keyString := string(key)
	cached, ok := tc.nodes[keyString]
	if ok {
		return cached.node, nil
	}

	// We don't have it cached so fetch it and add it to the cache.
	node := new(RangeTreeNode)
	if pErr := tc.txn.GetProto(keys.RangeTreeNodeKey(key), node); pErr != nil {
		return nil, pErr
	}
	tc.nodes[keyString] = cachedNode{
		node:  node,
		dirty: false,
	}
	return node, nil
}

// isRed will return true only if node exists and is not set to black.
func isRed(node *RangeTreeNode) bool {
	if node == nil {
		return false
	}
	return !node.Black
}

// getSibling returns the other child of the node's parent. Returns nil if the node
// has no parent or its parent has only one child.
func (tc *treeContext) getSibling(node *RangeTreeNode) (*RangeTreeNode, *roachpb.Error) {
	if node == nil || node.ParentKey == nil {
		return nil, nil
	}
	parent, pErr := tc.getNode(node.ParentKey)
	if pErr != nil {
		return nil, pErr
	}
	if parent.LeftKey == nil || parent.RightKey == nil {
		return nil, nil
	}
	if node.Key.Equal(parent.LeftKey) {
		return tc.getNode(parent.RightKey)
	}
	return tc.getNode(parent.LeftKey)
}

// getUncle returns the node's parent's sibling. Or nil if the node doesn't have a
// grandparent or their parent has no sibling.
func (tc *treeContext) getUncle(node *RangeTreeNode) (*RangeTreeNode, *roachpb.Error) {
	if node == nil || node.ParentKey == nil {
		return nil, nil
	}
	parent, pErr := tc.getNode(node.ParentKey)
	if pErr != nil {
		return nil, pErr
	}
	return tc.getSibling(parent)
}

// replaceNode cuts a node away form its parent, substituting a new node or
// nil. The updated new node is returned. Note that this does not in fact alter
// the old node in any way, but only the old node's parent and the new node.
func (tc *treeContext) replaceNode(oldNode, newNode *RangeTreeNode) (*RangeTreeNode, *roachpb.Error) {
	if oldNode.ParentKey == nil {
		if newNode == nil {
			return nil, roachpb.NewErrorf("cannot replace the root node with nil")
		}
		// Update the root key if this was the root.
		tc.setRootKey(newNode.Key)
	} else {
		oldParent, pErr := tc.getNode(oldNode.ParentKey)
		if pErr != nil {
			return nil, pErr
		}
		if oldParent.LeftKey != nil && oldNode.Key.Equal(oldParent.LeftKey) {
			if newNode == nil {
				oldParent.LeftKey = nil
			} else {
				oldParent.LeftKey = newNode.Key
			}
		} else {
			if newNode == nil {
				oldParent.RightKey = nil
			} else {
				oldParent.RightKey = newNode.Key
			}
		}
		tc.setNode(oldParent)
	}
	if newNode != nil {
		newNode.ParentKey = oldNode.ParentKey
		tc.setNode(newNode)
	}
	return newNode, nil
}

// rotateLeft performs a left rotation around the node.
func (tc *treeContext) rotateLeft(node *RangeTreeNode) (*RangeTreeNode, *roachpb.Error) {
	right, pErr := tc.getNode(node.RightKey)
	if pErr != nil {
		return nil, pErr
	}
	right, pErr = tc.replaceNode(node, right)
	if pErr != nil {
		return nil, pErr
	}
	node.RightKey = right.LeftKey
	if right.LeftKey != nil {
		rightLeft, pErr := tc.getNode(right.LeftKey)
		if pErr != nil {
			return nil, pErr
		}
		rightLeft.ParentKey = node.Key
		tc.setNode(rightLeft)
	}
	right.LeftKey = node.Key
	node.ParentKey = right.Key
	tc.setNode(right)
	tc.setNode(node)
	return right, nil
}

// rotateRight performs a right rotation around the node.
func (tc *treeContext) rotateRight(node *RangeTreeNode) (*RangeTreeNode, *roachpb.Error) {
	left, pErr := tc.getNode(node.LeftKey)
	if pErr != nil {
		return nil, pErr
	}
	left, pErr = tc.replaceNode(node, left)
	if pErr != nil {
		return nil, pErr
	}
	node.LeftKey = left.RightKey
	if left.RightKey != nil {
		leftRight, pErr := tc.getNode(left.RightKey)
		if pErr != nil {
			return nil, pErr
		}
		leftRight.ParentKey = node.Key
		tc.setNode(leftRight)
	}
	left.RightKey = node.Key
	node.ParentKey = left.Key
	tc.setNode(left)
	tc.setNode(node)
	return left, nil
}

// InsertRange adds a new range to the RangeTree. This should only be called
// from operations that create new ranges, such as AdminSplit.
func InsertRange(txn *client.Txn, b *client.Batch, key roachpb.RKey) *roachpb.Error {
	tc, pErr := getRangeTree(txn)
	if pErr != nil {
		return pErr
	}
	newNode := &RangeTreeNode{
		Key: key,
	}
	if pErr := tc.insert(newNode); pErr != nil {
		return pErr
	}
	tc.flush(b)
	return nil
}

// insert performs the insertion of a new node into the tree. It walks the tree
// until it finds the correct location. It will fail if the node already exists
// as that case should not occur. After inserting the node, it checks all insert
// cases to ensure the tree is balanced and adjusts it if needed.
func (tc *treeContext) insert(node *RangeTreeNode) *roachpb.Error {
	if tc.tree.RootKey == nil {
		tc.setRootKey(node.Key)
	} else {
		// Walk the tree to find the right place to insert the new node.
		currentKey := tc.tree.RootKey
		for {
			currentNode, pErr := tc.getNode(currentKey)
			if pErr != nil {
				return pErr
			}
			if node.Key.Equal(currentNode.Key) {
				return roachpb.NewErrorf("key %s already exists in the range tree", node.Key)
			}
			if node.Key.Less(currentNode.Key) {
				if currentNode.LeftKey == nil {
					currentNode.LeftKey = node.Key
					tc.setNode(currentNode)
					break
				} else {
					currentKey = currentNode.LeftKey
				}
			} else {
				if currentNode.RightKey == nil {
					currentNode.RightKey = node.Key
					tc.setNode(currentNode)
					break
				} else {
					currentKey = currentNode.RightKey
				}
			}
		}
		node.ParentKey = currentKey
		tc.setNode(node)
	}
	return tc.insertCase1(node)
}

// insertCase1 handles the case when the inserted node is the root node.
func (tc *treeContext) insertCase1(node *RangeTreeNode) *roachpb.Error {
	if node.ParentKey == nil {
		node.Black = true
		tc.setNode(node)
		return nil
	}
	return tc.insertCase2(node)
}

// insertCase2 handles the case when the inserted node has a black parent.
// In this is the case, no work need to be done, the tree is already correct.
func (tc *treeContext) insertCase2(node *RangeTreeNode) *roachpb.Error {
	parent, pErr := tc.getNode(node.ParentKey)
	if pErr != nil {
		return pErr
	}
	if !isRed(parent) {
		return nil
	}
	return tc.insertCase3(node)
}

// insertCase3 handles the case in which the uncle node is red. If so, the uncle
// and parent become black and the grandparent becomes red.
func (tc *treeContext) insertCase3(node *RangeTreeNode) *roachpb.Error {
	uncle, pErr := tc.getUncle(node)
	if pErr != nil {
		return pErr
	}
	if isRed(uncle) {
		parent, pErr := tc.getNode(node.ParentKey)
		if pErr != nil {
			return pErr
		}
		parent.Black = true
		tc.setNode(parent)
		uncle.Black = true
		tc.setNode(uncle)
		grandparent, pErr := tc.getNode(parent.ParentKey)
		if pErr != nil {
			return pErr
		}
		grandparent.Black = false
		tc.setNode(grandparent)
		return tc.insertCase1(grandparent)
	}
	return tc.insertCase4(node)
}

// insertCase4 handles two mirror cases. If the node is the right child of its
// parent who is the left child of the grandparent, a left rotation around the
// parent is required. Similarly, if the node is the left child of its parent
// who is the right child of the grandparent, a right rotation around the parent
// is required. This only prepares the tree for insertCase5.
func (tc *treeContext) insertCase4(node *RangeTreeNode) *roachpb.Error {
	parent, pErr := tc.getNode(node.ParentKey)
	if pErr != nil {
		return pErr
	}
	grandparent, pErr := tc.getNode(parent.ParentKey)
	if pErr != nil {
		return pErr
	}
	if parent.RightKey != nil && grandparent.LeftKey != nil && node.Key.Equal(parent.RightKey) && parent.Key.Equal(grandparent.LeftKey) {
		if _, pErr := tc.rotateLeft(parent); pErr != nil {
			return pErr
		}
		node, pErr = tc.getNode(node.Key)
		if pErr != nil {
			return pErr
		}
		node, pErr = tc.getNode(node.LeftKey)
		if pErr != nil {
			return pErr
		}
	} else if parent.LeftKey != nil && grandparent.RightKey != nil && node.Key.Equal(parent.LeftKey) && parent.Key.Equal(grandparent.RightKey) {
		node, pErr = tc.rotateRight(parent)
		if pErr != nil {
			return pErr
		}
		node, pErr = tc.getNode(node.Key)
		if pErr != nil {
			return pErr
		}
		node, pErr = tc.getNode(node.RightKey)
		if pErr != nil {
			return pErr
		}
	}
	return tc.insertCase5(node)
}

// insertCase5 handles two mirror cases. If the node is the right child of its
// parent who is the right child of the grandparent, a right rotation around the
// grandparent is required. Similarly, if the node is the left child of its
// parent who is the left child of the grandparent, a left rotation around the
// grandparent is required.
func (tc *treeContext) insertCase5(node *RangeTreeNode) *roachpb.Error {
	parent, pErr := tc.getNode(node.ParentKey)
	if pErr != nil {
		return pErr
	}
	parent.Black = true
	tc.setNode(parent)
	grandparent, pErr := tc.getNode(parent.ParentKey)
	if pErr != nil {
		return pErr
	}
	grandparent.Black = false
	tc.setNode(grandparent)
	if parent.LeftKey != nil && node.Key.Equal(parent.LeftKey) {
		if _, pErr := tc.rotateRight(grandparent); pErr != nil {
			return pErr
		}
	} else {
		if _, pErr := tc.rotateLeft(grandparent); pErr != nil {
			return pErr
		}
	}
	return nil
}

// DeleteRange removes a range from the RangeTree. This should only be called
// from operations that remove ranges, such as AdminMerge.
func DeleteRange(txn *client.Txn, b *client.Batch, key roachpb.RKey) *roachpb.Error {
	tc, pErr := getRangeTree(txn)
	if pErr != nil {
		return pErr
	}
	node, pErr := tc.getNode(key)
	if pErr != nil {
		return pErr
	}
	if node == nil {
		return roachpb.NewErrorf("could not find range tree node with the key %s", key)
	}
	if pErr := tc.delete(node); pErr != nil {
		return pErr
	}
	tc.flush(b)
	return nil
}

// swapNodes swaps all aspects of nodes a and b except their keys. This function
// is needed in order to accommodate the in-place style delete.
func (tc *treeContext) swapNodes(a, b *RangeTreeNode) (*RangeTreeNode, *RangeTreeNode, *roachpb.Error) {
	newA := &RangeTreeNode{
		Key:       a.Key,
		Black:     b.Black,
		ParentKey: b.ParentKey,
		LeftKey:   b.LeftKey,
		RightKey:  b.RightKey,
	}
	newB := &RangeTreeNode{
		Key:       b.Key,
		Black:     a.Black,
		ParentKey: a.ParentKey,
		LeftKey:   a.LeftKey,
		RightKey:  a.RightKey,
	}

	if a.ParentKey != nil {
		// Only change a's parent's child key if the parent is not b.
		if !a.ParentKey.Equal(b.Key) {
			parent, pErr := tc.getNode(a.ParentKey)
			if pErr != nil {
				return nil, nil, pErr
			}
			if parent.LeftKey.Equal(a.Key) {
				parent.LeftKey = b.Key
			} else {
				parent.RightKey = b.Key
			}
			tc.setNode(parent)
		} else {
			newB.ParentKey = a.Key
		}
	} else {
		tc.setRootKey(b.Key)
	}

	if a.LeftKey != nil {
		if !a.LeftKey.Equal(b.Key) {
			left, pErr := tc.getNode(a.LeftKey)
			if pErr != nil {
				return nil, nil, pErr
			}
			left.ParentKey = b.Key
			tc.setNode(left)
		} else {
			newB.LeftKey = a.Key
		}
	}
	if a.RightKey != nil {
		if !a.RightKey.Equal(b.Key) {
			right, pErr := tc.getNode(a.RightKey)
			if pErr != nil {
				return nil, nil, pErr
			}
			right.ParentKey = b.Key
			tc.setNode(right)
		} else {
			newB.RightKey = a.Key
		}
	}

	if b.ParentKey != nil {
		// Only change b's parent's child key if the parent is not a.
		if !b.ParentKey.Equal(a.Key) {
			parent, pErr := tc.getNode(b.ParentKey)
			if pErr != nil {
				return nil, nil, pErr
			}
			if parent.LeftKey.Equal(b.Key) {
				parent.LeftKey = a.Key
			} else {
				parent.RightKey = a.Key
			}
			tc.setNode(parent)
		} else {
			newA.ParentKey = b.Key
		}
	} else {
		tc.setRootKey(a.Key)
	}
	if b.LeftKey != nil {
		if !b.LeftKey.Equal(a.Key) {
			left, pErr := tc.getNode(b.LeftKey)
			if pErr != nil {
				return nil, nil, pErr
			}
			left.ParentKey = a.Key
			tc.setNode(left)
		} else {
			newA.LeftKey = b.Key
		}
	}
	if b.RightKey != nil {
		if !b.RightKey.Equal(a.Key) {
			right, pErr := tc.getNode(b.RightKey)
			if pErr != nil {
				return nil, nil, pErr
			}
			right.ParentKey = a.Key
			tc.setNode(right)
		} else {
			newA.RightKey = b.Key
		}
	}

	tc.setNode(newA)
	tc.setNode(newB)
	return newA, newB, nil
}

// delete removes a range from the range tree.
// Since this tree is not stored in memory but persisted through the ranges, in
// place deletion is not possible. Instead, we use the helper function
// swapNodes above.
func (tc *treeContext) delete(node *RangeTreeNode) *roachpb.Error {
	key := node.Key
	if node.LeftKey != nil && node.RightKey != nil {
		left, pErr := tc.getNode(node.LeftKey)
		if pErr != nil {
			return pErr
		}
		predecessor, pErr := tc.getMaxNode(left)
		if pErr != nil {
			return pErr
		}
		node, _, pErr = tc.swapNodes(node, predecessor)
		if pErr != nil {
			return pErr
		}
	}

	// Node will always have at most one child.
	var child *RangeTreeNode
	var pErr *roachpb.Error
	if node.LeftKey != nil {
		if child, pErr = tc.getNode(node.LeftKey); pErr != nil {
			return pErr
		}
	} else if node.RightKey != nil {
		if child, pErr = tc.getNode(node.RightKey); pErr != nil {
			return pErr
		}
	}
	if !isRed(node) {
		// Paint the node to the color of the child node.
		node.Black = !isRed(child)
		tc.setNode(node)
		if pErr := tc.deleteCase1(node); pErr != nil {
			return pErr
		}
	}
	if _, pErr := tc.replaceNode(node, child); pErr != nil {
		return pErr
	}

	// Always set the root back to black
	if node, pErr = tc.getNode(node.Key); pErr != nil {
		return pErr
	}
	if child != nil && node.ParentKey == nil {
		if child, pErr = tc.getNode(child.Key); pErr != nil {
			return pErr
		}
		child.Black = true
		tc.setNode(child)
	}

	tc.dropNode(key)
	return nil
}

// getMaxNode walks the tree to the right until it gets to a node without a
// right child and returns that node.
func (tc *treeContext) getMaxNode(node *RangeTreeNode) (*RangeTreeNode, *roachpb.Error) {
	if node.RightKey == nil {
		return node, nil
	}
	right, pErr := tc.getNode(node.RightKey)
	if pErr != nil {
		return nil, pErr
	}
	return tc.getMaxNode(right)
}

// deleteCase1 handles the case when node is the new root. If so, there is
// nothing to do.
func (tc *treeContext) deleteCase1(node *RangeTreeNode) *roachpb.Error {
	if node.ParentKey == nil {
		return nil
	}
	return tc.deleteCase2(node)
}

// deleteCase2 handles the case when node has a red sibling.
func (tc *treeContext) deleteCase2(node *RangeTreeNode) *roachpb.Error {
	sibling, pErr := tc.getSibling(node)
	if pErr != nil {
		return pErr
	}
	if isRed(sibling) {
		parent, pErr := tc.getNode(node.ParentKey)
		if pErr != nil {
			return pErr
		}
		parent.Black = false
		sibling.Black = true
		tc.setNode(parent)
		tc.setNode(sibling)
		if parent.LeftKey.Equal(node.Key) {
			if _, pErr := tc.rotateLeft(parent); pErr != nil {
				return pErr
			}
		} else {
			if _, pErr := tc.rotateRight(parent); pErr != nil {
				return pErr
			}
		}
	}
	return tc.deleteCase3(node)
}

// deleteCase3 handles the case when node's parent, sibling and sibling's
// children are all black.
func (tc *treeContext) deleteCase3(node *RangeTreeNode) *roachpb.Error {
	// This check uses cascading ifs to limit the number of db reads.
	parent, pErr := tc.getNode(node.ParentKey)
	if pErr != nil {
		return pErr
	}
	if !isRed(parent) {
		sibling, pErr := tc.getSibling(node)
		if pErr != nil {
			return pErr
		}
		if !isRed(sibling) {
			siblingLeft, pErr := tc.getNode(sibling.LeftKey)
			if pErr != nil {
				return pErr
			}
			if !isRed(siblingLeft) {
				siblingRight, pErr := tc.getNode(sibling.RightKey)
				if pErr != nil {
					return pErr
				}
				if !isRed(siblingRight) {
					sibling.Black = false
					tc.setNode(sibling)
					return tc.deleteCase1(parent)
				}
			}
		}
	}
	return tc.deleteCase4(node)
}

// deleteCase4 handles the case when node's sibling and siblings children are
// black, but parent is red.
func (tc *treeContext) deleteCase4(node *RangeTreeNode) *roachpb.Error {
	// This check uses cascading ifs to limit the number of db reads.
	parent, pErr := tc.getNode(node.ParentKey)
	if pErr != nil {
		return pErr
	}
	if isRed(parent) {
		sibling, pErr := tc.getSibling(node)
		if pErr != nil {
			return pErr
		}
		if !isRed(sibling) {
			siblingLeft, pErr := tc.getNode(sibling.LeftKey)
			if pErr != nil {
				return pErr
			}
			if !isRed(siblingLeft) {
				siblingRight, pErr := tc.getNode(sibling.RightKey)
				if pErr != nil {
					return pErr
				}
				if !isRed(siblingRight) {
					sibling.Black = false
					tc.setNode(sibling)
					parent.Black = true
					tc.setNode(parent)
					// This corrects the tree, no need to continue.
					return nil
				}
			}
		}
	}
	return tc.deleteCase5(node)
}

// deleteCase5 handles two mirror cases. The first case is when node is the left
// child of its parent, node's sibling is black, node's sibling's right child is
// black but left child is red. Or similarly, when node is the right child of
// its parent, node's sibling is black, node's sibling's left child is black but
// right child is red. The operations here actually only prepare the tree for
// deleteCase6.
func (tc *treeContext) deleteCase5(node *RangeTreeNode) *roachpb.Error {
	parent, pErr := tc.getNode(node.ParentKey)
	if pErr != nil {
		return pErr
	}
	sibling, pErr := tc.getSibling(node)
	if pErr != nil {
		return pErr
	}
	siblingLeft, pErr := tc.getNode(sibling.LeftKey)
	if pErr != nil {
		return pErr
	}
	siblingRight, pErr := tc.getNode(sibling.RightKey)
	if pErr != nil {
		return pErr
	}
	if parent.LeftKey.Equal(node.Key) &&
		!isRed(sibling) &&
		!isRed(siblingRight) &&
		isRed(siblingLeft) {
		sibling.Black = false
		tc.setNode(sibling)
		siblingLeft.Black = true
		tc.setNode(siblingLeft)
		if _, pErr := tc.rotateRight(sibling); pErr != nil {
			return pErr
		}
	} else if parent.RightKey.Equal(node.Key) &&
		!isRed(sibling) &&
		!isRed(siblingLeft) &&
		isRed(siblingRight) {
		sibling.Black = false
		tc.setNode(sibling)
		siblingRight.Black = true
		tc.setNode(siblingRight)
		if _, pErr := tc.rotateLeft(sibling); pErr != nil {
			return pErr
		}
	}
	return tc.deleteCase6(node)
}

// deleteCase6 handles two mirror cases. The first case is when node is the left
// child of its parent, node's sibling is black and node's sibling's right child
// is red. Or similarly, when node is the right child of its parent, node's
// sibling is black  and node's sibling's left child is red. No checks are
// needed here since we have already been forced into this case by deleteCase5.
func (tc *treeContext) deleteCase6(node *RangeTreeNode) *roachpb.Error {
	parent, pErr := tc.getNode(node.ParentKey)
	if pErr != nil {
		return pErr
	}
	sibling, pErr := tc.getSibling(node)
	if pErr != nil {
		return pErr
	}
	sibling.Black = parent.Black
	tc.setNode(sibling)
	parent.Black = true
	tc.setNode(parent)
	if node.Key.Equal(parent.LeftKey) {
		siblingRight, pErr := tc.getNode(sibling.RightKey)
		if pErr != nil {
			return pErr
		}
		siblingRight.Black = true
		tc.setNode(siblingRight)
		if _, pErr := tc.rotateLeft(parent); pErr != nil {
			return pErr
		}
	} else {
		siblingLeft, pErr := tc.getNode(sibling.LeftKey)
		if pErr != nil {
			return pErr
		}
		siblingLeft.Black = true
		tc.setNode(siblingLeft)
		if _, pErr := tc.rotateRight(parent); pErr != nil {
			return pErr
		}
	}
	return nil
}
