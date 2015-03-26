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

type (
	// RangeTree represents holds the metadata about the RangeTree.  There is
	// only one for the whole cluster.
	rangeTree proto.RangeTree
	// RangeTreeNode represents a node in the RangeTree, each range
	// has only a single node.
	rangeTreeNode proto.RangeTreeNode
)

// TODO(bram): Add a cache
// TODO(bram): Add parent keys
// TODO(bram): Add delete for merging ranges.
// TODO(bram): Add more elaborate testing.

// set saves the RangeTree node to the db.
func (n *rangeTreeNode) set(db *client.KV) error {
	if err := db.PutProto(engine.RangeTreeNodeKey(n.Key), (*proto.RangeTreeNode)(n)); err != nil {
		return err
	}
	return nil
}

// set saves the RangeTreeRoot to the db.
func (t *rangeTree) set(db *client.KV) error {
	if err := db.PutProto(engine.KeyRangeTreeRoot, (*proto.RangeTree)(t)); err != nil {
		return err
	}
	return nil
}

// SetupRangeTree creates a new RangeTree.  This should only be called as part of
// store.BootstrapRange.
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

// GetRangeTree fetches the RangeTree proto.
func getRangeTree(db *client.KV) (*rangeTree, error) {
	tree := &proto.RangeTree{}
	ok, _, err := db.GetProto(engine.KeyRangeTreeRoot, tree)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, util.Errorf("Could not find the range tree:%s", engine.KeyRangeTreeRoot)
	}
	return (*rangeTree)(tree), nil
}

// getRangeTreeNode return the RangeTree node for the given key.  If the key is
// nil, an empty RangeTreeNode is returned.
func getRangeTreeNode(db *client.KV, key *proto.Key) (*rangeTreeNode, error) {
	if key == nil {
		return nil, nil
	}
	node := &proto.RangeTreeNode{}
	ok, _, err := db.GetProto(engine.RangeTreeNodeKey(*key), node)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, util.Errorf("Could not find the range tree node:%s",
			engine.RangeTreeNodeKey(*key))
	}
	return (*rangeTreeNode)(node), nil
}

// getRootNode returns the RangeTree node for the root of the RangeTree.
func (t *rangeTree) getRootNode(db *client.KV) (*rangeTreeNode, error) {
	return getRangeTreeNode(db, &t.RootKey)
}

// InsertRange adds a new range to the RangeTree.  This should only be called
// from operations that create new ranges, such as range.splitTrigger.
func InsertRange(db *client.KV, key proto.Key) error {
	tree, err := getRangeTree(db)
	if err != nil {
		return err
	}
	root, err := tree.getRootNode(db)
	if err != nil {
		return err
	}
	root, err = tree.insert(db, root, key)
	if err != nil {
		return err
	}
	if !root.Black {
		// Always set the root back to black.
		root.Black = true
		if err = root.set(db); err != nil {
			return err
		}
	}
	if !tree.RootKey.Equal(root.Key) {
		// If the root has changed, update the tree to point to it.
		tree.RootKey = root.Key
		if err = tree.set(db); err != nil {
			return err
		}
	}
	return nil
}

// insert performs the insertion of a new range into the RangeTree.  It will
// recursivly call insert until it finds the correct location.  It will not
// overwite an already existing key, but that case should not occur.
func (t *rangeTree) insert(db *client.KV, node *rangeTreeNode, key proto.Key) (*rangeTreeNode, error) {
	if node == nil {
		// Insert the new node here.
		node = (*rangeTreeNode)(&proto.RangeTreeNode{
			Key: key,
		})
		if err := node.set(db); err != nil {
			return nil, err
		}
	} else if key.Less(node.Key) {
		// Walk down the tree to the left.
		left, err := getRangeTreeNode(db, node.LeftKey)
		if err != nil {
			return nil, err
		}
		left, err = t.insert(db, left, key)
		if err != nil {
			return nil, err
		}
		if node.LeftKey == nil || !(*node.LeftKey).Equal(left.Key) {
			node.LeftKey = &left.Key
			if err := node.set(db); err != nil {
				return nil, err
			}
		}
	} else {
		// Walk down the tree to the right.
		right, err := getRangeTreeNode(db, node.RightKey)
		if err != nil {
			return nil, err
		}
		right, err = t.insert(db, right, key)
		if err != nil {
			return nil, err
		}
		if node.RightKey == nil || !(*node.RightKey).Equal(right.Key) {
			node.RightKey = &right.Key
			if err := node.set(db); err != nil {
				return nil, err
			}
		}
	}
	return node.walkUpRot23(db)
}

// isRed will return true only if n exists and is not set to black.
func (n *rangeTreeNode) isRed() bool {
	if n == nil {
		return false
	}
	return !n.Black
}

// walkUpRot23 walks up and rotates the tree using the Left Leaning Red
// Black 2-3 algorithm.
func (n *rangeTreeNode) walkUpRot23(db *client.KV) (*rangeTreeNode, error) {
	// Should we rotate left?
	right, err := getRangeTreeNode(db, n.RightKey)
	if err != nil {
		return nil, err
	}
	left, err := getRangeTreeNode(db, n.LeftKey)
	if err != nil {
		return nil, err
	}
	if right.isRed() && !left.isRed() {
		n, err = n.rotateLeft(db)
		if err != nil {
			return nil, err
		}
	}

	// Should we rotate right?
	left, err = getRangeTreeNode(db, n.LeftKey)
	if err != nil {
		return nil, err
	}
	if left != nil {
		leftLeft, err := getRangeTreeNode(db, left.LeftKey)
		if err != nil {
			return nil, err
		}
		if left.isRed() && leftLeft.isRed() {
			n, err = n.rotateRight(db)
			if err != nil {
				return nil, err
			}
		}
	}

	// Should we flip?
	right, err = getRangeTreeNode(db, n.RightKey)
	if err != nil {
		return nil, err
	}
	left, err = getRangeTreeNode(db, n.LeftKey)
	if err != nil {
		return nil, err
	}
	if left.isRed() && right.isRed() {
		n, err = n.flip(db)
		if err != nil {
			return nil, err
		}
	}
	return n, nil
}

// rotateLeft performs a left rotation around the node n.
func (n *rangeTreeNode) rotateLeft(db *client.KV) (*rangeTreeNode, error) {
	right, err := getRangeTreeNode(db, n.RightKey)
	if err != nil {
		return nil, err
	}
	if right.Black {
		return nil, util.Error("rotating a black node")
	}
	n.RightKey = right.LeftKey
	right.LeftKey = &n.Key
	right.Black = n.Black
	n.Black = false
	if err = n.set(db); err != nil {
		return nil, err
	}
	if err = right.set(db); err != nil {
		return nil, err
	}
	return right, nil
}

// rotateRight performs a right rotation around the node n.
func (n *rangeTreeNode) rotateRight(db *client.KV) (*rangeTreeNode, error) {
	left, err := getRangeTreeNode(db, n.LeftKey)
	if err != nil {
		return nil, err
	}
	if left.Black {
		return nil, util.Error("rotating a black node")
	}
	n.LeftKey = left.RightKey
	left.RightKey = &n.Key
	left.Black = n.Black
	n.Black = false
	if err = n.set(db); err != nil {
		return nil, err
	}
	if err = left.set(db); err != nil {
		return nil, err
	}
	return left, nil
}

// flip swaps the color of the node n and both of its children.  Both those
// children must exist.
func (n *rangeTreeNode) flip(db *client.KV) (*rangeTreeNode, error) {
	left, err := getRangeTreeNode(db, n.LeftKey)
	if err != nil {
		return nil, err
	}
	right, err := getRangeTreeNode(db, n.RightKey)
	if err != nil {
		return nil, err
	}
	n.Black = !n.Black
	left.Black = !left.Black
	right.Black = !right.Black
	if err = n.set(db); err != nil {
		return nil, err
	}
	if err = left.set(db); err != nil {
		return nil, err
	}
	if err = right.set(db); err != nil {
		return nil, err
	}
	return n, nil
}
