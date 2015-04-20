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
// Author: Bram Gruneir (bram.gruneir@gmail.com)

package storage

import (
	"github.com/cockroachdb/cockroach/client"
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
// TODO(bram): Add parent keys
// TODO(bram): Add more elaborate testing.

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
	if err := engine.MVCCPutProto(batch, ms, engine.KeyRangeTreeRoot, timestamp, nil, tree); err != nil {
		return err
	}
	if err := engine.MVCCPutProto(batch, ms, engine.RangeTreeNodeKey(startKey), timestamp, nil, node); err != nil {
		return err
	}
	return nil
}

// flush writes all dirty nodes and the tree to the transaction.
func (tc *treeContext) flush() error {
	if tc.dirty {
		tc.txn.Prepare(client.PutProtoCall(engine.KeyRangeTreeRoot, tc.tree))
	}
	for _, cachedNode := range tc.nodes {
		if cachedNode.dirty {
			call := client.PutProtoCall(engine.RangeTreeNodeKey(cachedNode.node.Key), cachedNode.node)
			tc.txn.Prepare(call)
		}
	}
	return nil
}

// GetRangeTree fetches the RangeTree proto and sets up the range tree context.
func getRangeTree(txn *client.Txn) (*treeContext, error) {
	tree := &proto.RangeTree{}
	ok, _, err := txn.GetProto(engine.KeyRangeTreeRoot, tree)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, util.Errorf("Could not find the range tree:%s", engine.KeyRangeTreeRoot)
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
func (tc *treeContext) setRootKey(key *proto.Key) {
	tc.tree.RootKey = *key
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
	node := &proto.RangeTreeNode{}
	ok, _, err := tc.txn.GetProto(engine.RangeTreeNodeKey(*key), node)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, util.Errorf("Could not find the range tree node:%s", engine.RangeTreeNodeKey(*key))
	}
	tc.nodes[keyString] = cachedNode{
		node:  node,
		dirty: false,
	}
	return node, nil
}

// InsertRange adds a new range to the RangeTree. This should only be called
// from operations that create new ranges, such as range.splitTrigger.
// TODO(bram): Can we optimize this by inserting as a child of the range being
// split?
func InsertRange(txn *client.Txn, key proto.Key) error {
	tc, err := getRangeTree(txn)
	if err != nil {
		return err
	}
	root, err := tc.getNode(&tc.tree.RootKey)
	if err != nil {
		return err
	}
	root, err = tc.insert(root, key)
	if err != nil {
		return err
	}
	if !root.Black {
		// Always set the root back to black.
		root.Black = true
		tc.setNode(root)
	}
	if !tc.tree.RootKey.Equal(root.Key) {
		// If the root has changed, update the tree to point to it.
		tc.setRootKey(&root.Key)
	}
	tc.flush()
	return nil
}

// insert performs the insertion of a new range into the RangeTree. It will
// recursively call insert until it finds the correct location. It will not
// overwrite an already existing key, but that case should not occur.
func (tc *treeContext) insert(node *proto.RangeTreeNode, key proto.Key) (*proto.RangeTreeNode, error) {
	if node == nil {
		// Insert the new node here.
		node = &proto.RangeTreeNode{
			Key: key,
		}
		tc.setNode(node)
	} else if key.Less(node.Key) {
		// Walk down the tree to the left.
		left, err := tc.getNode(node.LeftKey)
		if err != nil {
			return nil, err
		}
		left, err = tc.insert(left, key)
		if err != nil {
			return nil, err
		}
		if node.LeftKey == nil || !(*node.LeftKey).Equal(left.Key) {
			node.LeftKey = &left.Key
			tc.setNode(node)
		}
	} else {
		// Walk down the tree to the right.
		right, err := tc.getNode(node.RightKey)
		if err != nil {
			return nil, err
		}
		right, err = tc.insert(right, key)
		if err != nil {
			return nil, err
		}
		if node.RightKey == nil || !(*node.RightKey).Equal(right.Key) {
			node.RightKey = &right.Key
			tc.setNode(node)
		}
	}
	return tc.walkUpRot23(node)
}

// isRed will return true only if node exists and is not set to black.
func isRed(node *proto.RangeTreeNode) bool {
	if node == nil {
		return false
	}
	return !node.Black
}

// walkUpRot23 walks up and rotates the tree using the Left Leaning Red Black
// 2-3 algorithm.
func (tc *treeContext) walkUpRot23(node *proto.RangeTreeNode) (*proto.RangeTreeNode, error) {
	// Should we rotate left?
	right, err := tc.getNode(node.RightKey)
	if err != nil {
		return nil, err
	}
	left, err := tc.getNode(node.LeftKey)
	if err != nil {
		return nil, err
	}
	if isRed(right) && !isRed(left) {
		node, err = tc.rotateLeft(node)
		if err != nil {
			return nil, err
		}
	}

	// Should we rotate right?
	left, err = tc.getNode(node.LeftKey)
	if err != nil {
		return nil, err
	}
	if left != nil {
		leftLeft, err := tc.getNode(left.LeftKey)
		if err != nil {
			return nil, err
		}
		if isRed(left) && isRed(leftLeft) {
			node, err = tc.rotateRight(node)
			if err != nil {
				return nil, err
			}
		}
	}

	// Should we flip?
	right, err = tc.getNode(node.RightKey)
	if err != nil {
		return nil, err
	}
	left, err = tc.getNode(node.LeftKey)
	if err != nil {
		return nil, err
	}
	if isRed(left) && isRed(right) {
		node, err = tc.flip(node)
		if err != nil {
			return nil, err
		}
	}
	return node, nil
}

// rotateLeft performs a left rotation around the node.
func (tc *treeContext) rotateLeft(node *proto.RangeTreeNode) (*proto.RangeTreeNode, error) {
	right, err := tc.getNode(node.RightKey)
	if err != nil {
		return nil, err
	}
	if right.Black {
		return nil, util.Error("rotating a black node")
	}
	node.RightKey = right.LeftKey
	right.LeftKey = &node.Key
	right.Black = node.Black
	node.Black = false
	tc.setNode(node)
	tc.setNode(right)
	return right, nil
}

// rotateRight performs a right rotation around the node.
func (tc *treeContext) rotateRight(node *proto.RangeTreeNode) (*proto.RangeTreeNode, error) {
	left, err := tc.getNode(node.LeftKey)
	if err != nil {
		return nil, err
	}
	if left.Black {
		return nil, util.Error("rotating a black node")
	}
	node.LeftKey = left.RightKey
	left.RightKey = &node.Key
	left.Black = node.Black
	node.Black = false
	tc.setNode(node)
	tc.setNode(left)
	return left, nil
}

// flip swaps the color of the node and both of its children. Both those
// children must exist.
func (tc *treeContext) flip(node *proto.RangeTreeNode) (*proto.RangeTreeNode, error) {
	left, err := tc.getNode(node.LeftKey)
	if err != nil {
		return nil, err
	}
	right, err := tc.getNode(node.RightKey)
	if err != nil {
		return nil, err
	}
	node.Black = !node.Black
	left.Black = !left.Black
	right.Black = !right.Black
	tc.setNode(node)
	tc.setNode(left)
	tc.setNode(right)
	return node, nil
}
