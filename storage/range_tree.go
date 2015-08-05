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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Bram Gruneir (bram+code@cockroachlabs.com)

package storage

import (
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
)

// cachedNode is an in memory cache for use during range tree manipulations.
type cachedNode struct {
	node  *proto.RangeTreeNode
	dirty bool
}

// RangeTree is used to hold the relevant context information for any
// operations on the range tree.
type treeContext struct {
	txn   *client.Txn
	tree  *proto.RangeTree
	dirty bool
	nodes map[string]cachedNode
}

// TODO(bram): Add delete for merging ranges.

// SetupRangeTree creates a new RangeTree. This should only be called as part
// of store.BootstrapRange.
func SetupRangeTree(batch engine.Engine, ms *engine.MVCCStats, timestamp proto.Timestamp, startKey proto.Key) error {
	tree := &proto.RangeTree{
		RootKey: startKey,
	}
	node := &proto.RangeTreeNode{
		Key:   startKey,
		Black: true,
	}
	if err := engine.MVCCPutProto(batch, ms, keys.RangeTreeRoot, timestamp, nil, tree); err != nil {
		return err
	}
	if err := engine.MVCCPutProto(batch, ms, keys.RangeTreeNodeKey(startKey), timestamp, nil, node); err != nil {
		return err
	}
	return nil
}

// flush writes all dirty nodes and the tree to the transaction.
func (tc *treeContext) flush(b *client.Batch) error {
	if tc.dirty {
		b.Put(keys.RangeTreeRoot, tc.tree)
	}
	for _, cachedNode := range tc.nodes {
		if cachedNode.dirty {
			b.Put(keys.RangeTreeNodeKey(cachedNode.node.Key), cachedNode.node)
		}
	}
	return nil
}

// GetRangeTree fetches the RangeTree proto and sets up the range tree context.
func getRangeTree(txn *client.Txn) (*treeContext, error) {
	tree := new(proto.RangeTree)
	if err := txn.GetProto(keys.RangeTreeRoot, tree); err != nil {
		return nil, err
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
func (tc *treeContext) setRootKey(key proto.Key) {
	tc.tree.RootKey = key
	tc.dirty = true
}

// setNode sets the node in the cache so all subsequent reads will read this
// upated value. It also marks the node as dirty for writing during a flush.
func (tc *treeContext) setNode(node *proto.RangeTreeNode) {
	tc.nodes[string(node.Key)] = cachedNode{
		node:  node,
		dirty: true,
	}
}

// getNode returns the RangeTreeNode for the given key. If the key is nil, nil
// is returned.
func (tc *treeContext) getNode(key *proto.Key) (*proto.RangeTreeNode, error) {
	if key == nil {
		return nil, nil
	}

	// First check to see if we have the node cached.
	keyString := string(*key)
	cached, ok := tc.nodes[keyString]
	if ok {
		return cached.node, nil
	}

	// We don't have it cached so fetch it and add it to the cache.
	node := new(proto.RangeTreeNode)
	if err := tc.txn.GetProto(keys.RangeTreeNodeKey(*key), node); err != nil {
		return nil, err
	}
	tc.nodes[keyString] = cachedNode{
		node:  node,
		dirty: false,
	}
	return node, nil
}

// isRed will return true only if node exists and is not set to black.
func isRed(node *proto.RangeTreeNode) bool {
	if node == nil {
		return false
	}
	return !node.Black
}

// sibling returns the other child of the node's parent. Returns nil if the node
// has no parent or its parent has only one child.
func (tc *treeContext) sibling(node *proto.RangeTreeNode) (*proto.RangeTreeNode, error) {
	if node == nil || node.ParentKey == nil {
		return nil, nil
	}
	parent, err := tc.getNode(node.ParentKey)
	if err != nil {
		return nil, err
	}
	if parent.LeftKey == nil || parent.RightKey == nil {
		return nil, nil
	}
	if node.Key.Equal(*parent.LeftKey) {
		return tc.getNode(parent.RightKey)
	}
	return tc.getNode(parent.LeftKey)
}

// uncle returns the node's parent's sibling. Or nil if the node doesn't have a
// grandparent or their parent has no sibling.
func (tc *treeContext) uncle(node *proto.RangeTreeNode) (*proto.RangeTreeNode, error) {
	if node == nil || node.ParentKey == nil {
		return nil, nil
	}
	parent, err := tc.getNode(node.ParentKey)
	if err != nil {
		return nil, err
	}
	return tc.sibling(parent)
}

// replaceNode cuts a node away form its parent, substituting a new node or
// nil. The updated new node is returned. Note that this does not in fact alter
// the old node in any way, but only the old node's parent and the new node.
func (tc *treeContext) replaceNode(oldNode, newNode *proto.RangeTreeNode) (*proto.RangeTreeNode, error) {
	if oldNode.ParentKey == nil {
		if newNode == nil {
			return nil, util.Error("cannot replace the root node with nil")
		}
		// Update the root key if this was the root.
		tc.setRootKey(newNode.Key)
	} else {
		oldParent, err := tc.getNode(oldNode.ParentKey)
		if err != nil {
			return nil, err
		}
		if oldParent.LeftKey != nil && oldNode.Key.Equal(*oldParent.LeftKey) {
			if newNode == nil {
				oldParent.LeftKey = nil
			} else {
				oldParent.LeftKey = &newNode.Key
			}
		} else {
			if newNode == nil {
				oldParent.RightKey = nil
			} else {
				oldParent.RightKey = &newNode.Key
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
func (tc *treeContext) rotateLeft(node *proto.RangeTreeNode) (*proto.RangeTreeNode, error) {
	right, err := tc.getNode(node.RightKey)
	if err != nil {
		return nil, err
	}
	right, err = tc.replaceNode(node, right)
	if err != nil {
		return nil, err
	}
	node.RightKey = right.LeftKey
	if right.LeftKey != nil {
		rightLeft, err := tc.getNode(right.LeftKey)
		if err != nil {
			return nil, err
		}
		rightLeft.ParentKey = &node.Key
		tc.setNode(rightLeft)
	}
	right.LeftKey = &node.Key
	node.ParentKey = &right.Key
	tc.setNode(right)
	tc.setNode(node)
	return right, nil
}

// rotateRight performs a right rotation around the node.
func (tc *treeContext) rotateRight(node *proto.RangeTreeNode) (*proto.RangeTreeNode, error) {
	left, err := tc.getNode(node.LeftKey)
	if err != nil {
		return nil, err
	}
	left, err = tc.replaceNode(node, left)
	if err != nil {
		return nil, err
	}
	node.LeftKey = left.RightKey
	if left.RightKey != nil {
		leftRight, err := tc.getNode(left.RightKey)
		if err != nil {
			return nil, err
		}
		leftRight.ParentKey = &node.Key
		tc.setNode(leftRight)
	}
	left.RightKey = &node.Key
	node.ParentKey = &left.Key
	tc.setNode(left)
	tc.setNode(node)
	return left, nil
}

// InsertRange adds a new range to the RangeTree. This should only be called
// from operations that create new ranges, such as AdminSplit.
func InsertRange(txn *client.Txn, b *client.Batch, key proto.Key) error {
	tc, err := getRangeTree(txn)
	if err != nil {
		return err
	}
	newNode := &proto.RangeTreeNode{
		Key: key,
	}
	err = tc.insert(newNode)
	if err != nil {
		return err
	}
	return tc.flush(b)
}

// insert performs the insertion of a new node into the tree. It walks the tree
// until it finds the correct location. It will fail if the node already exists
// as that case should not occur. After inserting the node, it checks all insert
// cases to ensure the tree is balanced and adjusts it if needed.
func (tc *treeContext) insert(node *proto.RangeTreeNode) error {
	if tc.tree.RootKey == nil {
		tc.setRootKey(node.Key)
	} else {
		// Walk the tree to find the right place to insert the new node.
		currentKey := &tc.tree.RootKey
		for {
			currentNode, err := tc.getNode(currentKey)
			if err != nil {
				return err
			}
			if node.Key.Equal(currentNode.Key) {
				return util.Errorf("key %s already exists in the range tree", node.Key)
			}
			if node.Key.Less(currentNode.Key) {
				if currentNode.LeftKey == nil {
					currentNode.LeftKey = &node.Key
					tc.setNode(currentNode)
					break
				} else {
					currentKey = currentNode.LeftKey
				}
			} else {
				if currentNode.RightKey == nil {
					currentNode.RightKey = &node.Key
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
func (tc *treeContext) insertCase1(node *proto.RangeTreeNode) error {
	if node.ParentKey == nil {
		node.Black = true
		tc.setNode(node)
		return nil
	}
	return tc.insertCase2(node)
}

// insertCase2 handles the case when the inserted node has a black parent.
// In this is the case, no work need to be done, the tree is already correct.
func (tc *treeContext) insertCase2(node *proto.RangeTreeNode) error {
	parent, err := tc.getNode(node.ParentKey)
	if err != nil {
		return err
	}
	if !isRed(parent) {
		return nil
	}
	return tc.insertCase3(node)
}

// insertCase3 handles the case in which the uncle node is red. If so, the uncle
// and parent become black and the grandparent becomes red.
func (tc *treeContext) insertCase3(node *proto.RangeTreeNode) error {
	uncle, err := tc.uncle(node)
	if err != nil {
		return nil
	}
	if isRed(uncle) {
		parent, err := tc.getNode(node.ParentKey)
		if err != nil {
			return err
		}
		parent.Black = true
		tc.setNode(parent)
		uncle.Black = true
		tc.setNode(uncle)
		grandparent, err := tc.getNode(parent.ParentKey)
		if err != nil {
			return err
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
func (tc *treeContext) insertCase4(node *proto.RangeTreeNode) error {
	parent, err := tc.getNode(node.ParentKey)
	if err != nil {
		return err
	}
	grandparent, err := tc.getNode(parent.ParentKey)
	if err != nil {
		return err
	}
	if parent.RightKey != nil && grandparent.LeftKey != nil && node.Key.Equal(*parent.RightKey) && parent.Key.Equal(*grandparent.LeftKey) {
		_, err = tc.rotateLeft(parent)
		if err != nil {
			return err
		}
		node, err = tc.getNode(&node.Key)
		if err != nil {
			return err
		}
		node, err = tc.getNode(node.LeftKey)
		if err != nil {
			return err
		}
	} else if parent.LeftKey != nil && grandparent.RightKey != nil && node.Key.Equal(*parent.LeftKey) && parent.Key.Equal(*grandparent.RightKey) {
		node, err = tc.rotateRight(parent)
		if err != nil {
			return err
		}
		node, err = tc.getNode(&node.Key)
		if err != nil {
			return err
		}
		node, err = tc.getNode(node.RightKey)
		if err != nil {
			return err
		}
	}
	return tc.insertCase5(node)
}

// insertCase5 handles two mirror cases. If the node is the right child of its
// parent who is the right child of the grandparent, a right rotation around the
// grandparent is required. Similarly, if the node is the left child of its
// parent who is the left child of the grandparent, a left rotation around the
// grandparent is required.
func (tc *treeContext) insertCase5(node *proto.RangeTreeNode) error {
	parent, err := tc.getNode(node.ParentKey)
	if err != nil {
		return err
	}
	parent.Black = true
	tc.setNode(parent)
	grandparent, err := tc.getNode(parent.ParentKey)
	if err != nil {
		return err
	}
	grandparent.Black = false
	tc.setNode(grandparent)
	if parent.LeftKey != nil && node.Key.Equal(*parent.LeftKey) {
		_, err = tc.rotateRight(grandparent)
		if err != nil {
			return err
		}
	} else {
		_, err = tc.rotateLeft(grandparent)
		if err != nil {
			return err
		}
	}
	return nil
}
