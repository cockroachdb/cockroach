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

package storage_test

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

// loadNodes fetches a node and recursively all of its children.
func loadNodes(t *testing.T, db *client.DB, key roachpb.RKey, nodes map[string]storage.RangeTreeNode) {
	var node storage.RangeTreeNode
	if err := db.GetProto(keys.RangeTreeNodeKey(key), &node); err != nil {
		t.Fatal(err)
	}
	nodes[node.Key.String()] = node
	if node.LeftKey != nil {
		loadNodes(t, db, node.LeftKey, nodes)
	}
	if node.RightKey != nil {
		loadNodes(t, db, node.RightKey, nodes)
	}
}

// loadTree loads the tree root and all of its nodes. It puts all of the nodes
// into a map.
func loadTree(t *testing.T, db *client.DB) (storage.RangeTree, map[string]storage.RangeTreeNode) {
	var tree storage.RangeTree
	if err := db.GetProto(keys.RangeTreeRoot, &tree); err != nil {
		t.Fatal(err)
	}
	nodes := make(map[string]storage.RangeTreeNode)
	if tree.RootKey != nil {
		loadNodes(t, db, tree.RootKey, nodes)
	}
	return tree, nodes
}

// VerifyTree checks to ensure that the tree is indeed balanced and a correct
// red-black tree. It does so by checking each of the red-black tree properties.
// These verify functions are similar to the those found in the range_tree_test
// but these use a map of nodes instead of a tree context.
func VerifyTree(t *testing.T, tree storage.RangeTree, nodes map[string]storage.RangeTreeNode, testName string) {
	root, ok := nodes[tree.RootKey.String()]
	if !ok {
		t.Fatalf("%s: could not find root node with key %s", testName, tree.RootKey)
	}

	verifyBinarySearchTree(t, nodes, testName, root, roachpb.RKeyMin, roachpb.RKeyMax)
	// Property 1 is always correct. All nodes are already colored.
	verifyProperty2(t, testName, root)
	// Property 3 is always correct. All leaves are black.
	verifyProperty4(t, nodes, testName, root)
	verifyProperty5(t, nodes, testName, root)
}

// isRed returns true iff node is not set to black.
func isRed(node storage.RangeTreeNode) bool {
	return !node.Black
}

func getNode(t *testing.T, nodes map[string]storage.RangeTreeNode, testName string, key roachpb.RKey) (storage.RangeTreeNode, bool) {
	if key != nil {
		if node, ok := nodes[key.String()]; !ok {
			t.Errorf("%s: could not locate node with key %s", testName, key)
		} else {
			return node, ok
		}
	}
	return storage.RangeTreeNode{}, false
}

// verifyBinarySearchTree checks to ensure that all keys to the left of the root
// node are less than it, and all nodes to the right of the root node are
// greater than it. It recursively walks the tree to perform this same check.
func verifyBinarySearchTree(t *testing.T, nodes map[string]storage.RangeTreeNode, testName string, node storage.RangeTreeNode, keyMin, keyMax roachpb.RKey) {
	if !node.Key.Less(keyMax) {
		t.Errorf("%s: Failed Property BST - The key %s is not less than %s.", testName, node.Key, keyMax)
	}
	// We need the extra check since roachpb.KeyMin is actually a range start key.
	if !keyMin.Less(node.Key) && !node.Key.Equal(roachpb.RKeyMin) {
		t.Errorf("%s: Failed Property BST - The key %s is not greater than %s.", testName, node.Key, keyMin)
	}
	if left, ok := getNode(t, nodes, testName, node.LeftKey); ok {
		verifyBinarySearchTree(t, nodes, testName, left, keyMin, node.Key)
	}
	if right, ok := getNode(t, nodes, testName, node.RightKey); ok {
		verifyBinarySearchTree(t, nodes, testName, right, node.Key, keyMax)
	}
}

// verifyProperty2 ensures that the root node is black.
func verifyProperty2(t *testing.T, testName string, root storage.RangeTreeNode) {
	if e, a := false, isRed(root); e != a {
		t.Errorf("%s: Failed Property 2 - The root node is not black.", testName)
	}
}

// verifyProperty4 ensures that the parent of every red node is black.
func verifyProperty4(t *testing.T, nodes map[string]storage.RangeTreeNode, testName string, node storage.RangeTreeNode) {
	if left, ok := getNode(t, nodes, testName, node.LeftKey); ok {
		if isRed(node) {
			if e, a := false, isRed(left); e != a {
				t.Errorf("%s: Failed property 4 - Red Node %s's left child %s is also red.", testName, node.Key, left.Key)
			}
		}
		verifyProperty4(t, nodes, testName, left)
	}
	if right, ok := getNode(t, nodes, testName, node.RightKey); ok {
		if isRed(node) {
			if e, a := false, isRed(right); e != a {
				t.Errorf("%s: Failed property 4 - Red Node %s's right child %s is also red.", testName, node.Key, right.Key)
			}
		}
		verifyProperty4(t, nodes, testName, right)
	}
}

// verifyProperty5 ensures that all paths from any given node to its leaf nodes
// contain the same number of black nodes.
func verifyProperty5(t *testing.T, nodes map[string]storage.RangeTreeNode, testName string, node storage.RangeTreeNode) int {
	var leftBlackCount, rightBlackCount int
	if left, ok := getNode(t, nodes, testName, node.LeftKey); ok {
		leftBlackCount = verifyProperty5(t, nodes, testName, left)
	}
	if right, ok := getNode(t, nodes, testName, node.RightKey); ok {
		rightBlackCount = verifyProperty5(t, nodes, testName, right)
	}
	if e, a := leftBlackCount, rightBlackCount; e != a {
		t.Errorf("%s: Failed property 5 - Expected a black count of %d but instead got %d.", testName, e, a)
	}
	// Upstream doesn't need to know about local errors.
	childBlackCount := leftBlackCount
	if !isRed(node) {
		childBlackCount++
	}
	return childBlackCount
}

// TestSetupRangeTree ensures that SetupRangeTree correctly setups up the range
// tree and first node. SetupRangeTree is called via store.BootstrapRange.
func TestSetupRangeTree(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer config.TestingDisableTableSplits()()
	store, stopper, _ := createTestStore(t)
	defer stopper.Stop()
	db := store.DB()

	tree, nodes := loadTree(t, db)
	expectedTree := storage.RangeTree{
		RootKey: roachpb.RKeyMin,
	}
	if !reflect.DeepEqual(tree, expectedTree) {
		t.Fatalf("tree roots do not match - expected:%+v actual:%+v", expectedTree, tree)
	}
	VerifyTree(t, tree, nodes, "setup")
}

// TestTree is a similar to the TestTree test in range_tree_test but this one
// performs actual splits and merges.
func TestTree(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer config.TestingDisableTableSplits()()
	store, stopper, _ := createTestStore(t)
	defer stopper.Stop()
	db := store.DB()

	keys := []string{"m",
		"f", "e", "d", "c", "b", "a",
		"g", "h", "i", "j", "k", "l",
		"s", "r", "q", "p", "o", "n",
		"t", "u", "v", "w", "x", "y", "z",
	}

	for _, key := range keys {
		if err := db.AdminSplit(key); err != nil {
			t.Fatal(err)
		}
		tree, nodes := loadTree(t, db)
		VerifyTree(t, tree, nodes, key)
	}

	// To test merging, we just call AdminMerge on the lowest key to merge all
	// ranges back into a single one.
	// TODO(bdarnell): re-enable this when merging is more reliable.
	// https://github.com/cockroachdb/cockroach/issues/2433
	/*
		for i := 0; i < len(keys); i++ {
			if err := db.AdminMerge(roachpb.KeyMin); err != nil {
				t.Fatal(err)
			}
			tree, nodes := loadTree(t, db)
			VerifyTree(t, tree, nodes, fmt.Sprintf("remove %d", i))
		}
	*/
}
