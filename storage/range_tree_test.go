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
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func createTreeContext(rootKey *proto.Key, nodes []*proto.RangeTreeNode) *treeContext {
	root := &proto.RangeTree{
		RootKey: *rootKey,
	}
	tc := &treeContext{
		txn:   nil,
		tree:  root,
		dirty: false,
		nodes: map[string]cachedNode{},
	}
	for _, node := range nodes {
		if node != nil {
			// We don't use setNode here to ensure dirty is false.
			tc.nodes[string(node.Key)] = cachedNode{
				node:  node,
				dirty: false,
			}
		}
	}
	return tc
}

// TestIsRed ensures that the isRed function is correct.
func TestIsRed(t *testing.T) {
	defer leaktest.AfterTest(t)
	testCases := []struct {
		node     *proto.RangeTreeNode
		expected bool
	}{
		// normal black node
		{&proto.RangeTreeNode{Black: true}, false},
		// normal red node
		{&proto.RangeTreeNode{Black: false}, true},
		// nil
		{nil, false},
	}
	for i, test := range testCases {
		node := test.node
		if a, e := isRed(node), test.expected; a != e {
			t.Errorf("%d: %+v expected %v; got %v", i, node, e, a)
		}
	}
}

// checkTreeNode compares the node cached in the treeContext against its
// expected value. It also makes sure that the node is marked as dirty in the
// cache. If an actual value is passed it, that is compared with the value in
// the cache as well.
func checkTreeNode(t *testing.T, tc *treeContext, testNumber int, name string, key proto.Key, expected, actual *proto.RangeTreeNode) {
	if expected != nil {
		// Is the value correct?
		cached, err := tc.getNode(&expected.Key)
		if err != nil {
			t.Fatal(util.ErrorfSkipFrames(1, "%d: Could not get node %s", testNumber, expected.Key))
		}
		if !reflect.DeepEqual(cached, expected) {
			t.Error(util.ErrorfSkipFrames(1, "%d: Expected %s node is not the same as the actual.\nExpected: %+v\nActual: %+v", testNumber, name, expected, actual))
		}

		// Is there a returned value to match against the cached one?
		if actual != nil {
			if !reflect.DeepEqual(actual, cached) {
				t.Error(util.ErrorfSkipFrames(1, "%d: Cached %s node is not the same as the actual.\nExpected: %+v\nActual: %+v", testNumber, name, cached, actual))
			}
		}

		// Is the node marked as dirty?
		if !tc.nodes[string(expected.Key)].dirty {
			t.Error(util.ErrorfSkipFrames(1, "%d: Expected %s node to be dirty", testNumber, name))
		}
	} else {
		if cached := tc.nodes[string(key)].node; cached != nil {
			t.Error(util.ErrorfSkipFrames(1, "%d: Expected nil for %s node, got a cached value of: %+v", testNumber, name, cached))
		}
	}
}

// TestReplaceNode ensures that the helper function replaceNode functions
// correctly.
func TestReplaceNode(t *testing.T) {
	defer leaktest.AfterTest(t)

	keyRoot := proto.Key("ROOT")
	keyOld := proto.Key("O")
	keyOldLeft := proto.Key("OL")
	keyOldRight := proto.Key("OR")
	keyNew := proto.Key("N")
	keyNewLeft := proto.Key("NL")
	keyNewRight := proto.Key("NR")
	keyParent := proto.Key("P")
	keyParentParent := proto.Key("PP")
	keyParentLeft := proto.Key("PL")
	keyParentRight := proto.Key("PR")

	testCases := []struct {
		root           proto.Key
		parent         *proto.RangeTreeNode
		oldNode        *proto.RangeTreeNode
		newNode        *proto.RangeTreeNode
		expectedRoot   proto.Key
		expectedParent *proto.RangeTreeNode
		expectedNew    *proto.RangeTreeNode
		expectedErr    bool
	}{
		// Test Case 0: root, replace with nil, should fail
		{
			oldNode: &proto.RangeTreeNode{
				Key: keyOld,
			},
			expectedErr: true,
		},
		// Test Case 1: non-root, replace with nil, left child
		{
			root: keyRoot,
			oldNode: &proto.RangeTreeNode{
				Key:       keyOld,
				ParentKey: &keyParent,
				LeftKey:   &keyOldLeft,
				RightKey:  &keyOldRight,
			},
			parent: &proto.RangeTreeNode{
				Key:       keyParent,
				ParentKey: &keyParentParent,
				LeftKey:   &keyOld,
				RightKey:  &keyParentRight,
			},
			expectedRoot: keyRoot,
			expectedParent: &proto.RangeTreeNode{
				Key:       keyParent,
				ParentKey: &keyParentParent,
				RightKey:  &keyParentRight,
			},
		},
		// Test Case 2: non-root, replace with nil, right child
		{
			root: keyRoot,
			oldNode: &proto.RangeTreeNode{
				Key:       keyOld,
				ParentKey: &keyParent,
				LeftKey:   &keyOldLeft,
				RightKey:  &keyOldRight,
			},
			parent: &proto.RangeTreeNode{
				Key:       keyParent,
				ParentKey: &keyParentParent,
				LeftKey:   &keyParentLeft,
				RightKey:  &keyOld,
			},
			expectedRoot: keyRoot,
			expectedParent: &proto.RangeTreeNode{
				Key:       keyParent,
				ParentKey: &keyParentParent,
				LeftKey:   &keyParentLeft,
			},
		},
		// Test Case 3: non-root, replace with node, left child
		{
			root: keyRoot,
			oldNode: &proto.RangeTreeNode{
				Key:       keyOld,
				ParentKey: &keyParent,
				LeftKey:   &keyOldLeft,
				RightKey:  &keyOldRight,
			},
			parent: &proto.RangeTreeNode{
				Key:       keyParent,
				ParentKey: &keyParentParent,
				LeftKey:   &keyOld,
				RightKey:  &keyParentRight,
			},
			newNode: &proto.RangeTreeNode{
				Key:      keyNew,
				LeftKey:  &keyNewLeft,
				RightKey: &keyNewRight,
			},
			expectedRoot: keyRoot,
			expectedParent: &proto.RangeTreeNode{
				Key:       keyParent,
				ParentKey: &keyParentParent,
				LeftKey:   &keyNew,
				RightKey:  &keyParentRight,
			},
			expectedNew: &proto.RangeTreeNode{
				Key:       keyNew,
				ParentKey: &keyParent,
				LeftKey:   &keyNewLeft,
				RightKey:  &keyNewRight,
			},
		},
		// Test Case 4: non-root, replace with node, right child
		{
			root: keyRoot,
			oldNode: &proto.RangeTreeNode{
				Key:       keyOld,
				ParentKey: &keyParent,
				LeftKey:   &keyOldLeft,
				RightKey:  &keyOldRight,
			},
			parent: &proto.RangeTreeNode{
				Key:       keyParent,
				ParentKey: &keyParentParent,
				LeftKey:   &keyParentLeft,
				RightKey:  &keyOld,
			},
			newNode: &proto.RangeTreeNode{
				Key:      keyNew,
				LeftKey:  &keyNewLeft,
				RightKey: &keyNewRight,
			},
			expectedRoot: keyRoot,
			expectedParent: &proto.RangeTreeNode{
				Key:       keyParent,
				ParentKey: &keyParentParent,
				LeftKey:   &keyParentLeft,
				RightKey:  &keyNew,
			},
			expectedNew: &proto.RangeTreeNode{
				Key:       keyNew,
				ParentKey: &keyParent,
				LeftKey:   &keyNewLeft,
				RightKey:  &keyNewRight,
			},
		},
		// Test Case 5: root, replace with node
		{
			root: keyOld,
			oldNode: &proto.RangeTreeNode{
				Key:      keyOld,
				LeftKey:  &keyOldLeft,
				RightKey: &keyOldRight,
			},
			newNode: &proto.RangeTreeNode{
				Key:      keyNew,
				LeftKey:  &keyNewLeft,
				RightKey: &keyNewRight,
			},
			expectedRoot: keyNew,
			expectedNew: &proto.RangeTreeNode{
				Key:      keyNew,
				LeftKey:  &keyNewLeft,
				RightKey: &keyNewRight,
			},
		},
	}

	for i, test := range testCases {
		tc := createTreeContext(&test.root, []*proto.RangeTreeNode{
			test.parent,
			test.newNode,
			test.oldNode,
		})

		actualNewNode, err := tc.replaceNode(test.oldNode, test.newNode)
		if err != nil {
			// Did we expect the error?
			if test.expectedErr {
				continue
			}
			t.Fatal(err)
		}

		if test.expectedErr {
			t.Fatalf("%d: Error expected but didn't occur. Expected: %s", i, err)
		}

		// Compare the roots.
		if a, e := tc.tree.RootKey, test.expectedRoot; !a.Equal(e) {
			t.Errorf("%d: Roots do not match. Expected:%s Actual:%s", i, e, a)
		}

		checkTreeNode(t, tc, i, "parent", keyParent, test.expectedParent, nil)
		checkTreeNode(t, tc, i, "new", keyNew, test.expectedNew, actualNewNode)
	}
}

// TestRotateRight ensures that right rotations occur correctly.
func TestRotateRight(t *testing.T) {
	defer leaktest.AfterTest(t)

	keyNode := proto.Key("N")
	keyRight := proto.Key("R")
	keyLeft := proto.Key("L")
	keyLeftLeft := proto.Key("LL")
	keyLeftRight := proto.Key("LR")
	keyLeftRightLeft := proto.Key("LRL")
	keyLeftRightRight := proto.Key("LRR")
	keyParent := proto.Key("P")
	keyParentLeft := proto.Key("PL")
	keyParentRight := proto.Key("PR")

	testCases := []struct {
		node              *proto.RangeTreeNode
		parent            *proto.RangeTreeNode
		left              *proto.RangeTreeNode
		leftRight         *proto.RangeTreeNode
		expectedNode      *proto.RangeTreeNode
		expectedParent    *proto.RangeTreeNode
		expectedLeft      *proto.RangeTreeNode
		expectedLeftRight *proto.RangeTreeNode
	}{
		// Test Case 0: Normal Rotation, parent left
		{
			parent: &proto.RangeTreeNode{
				Key:      keyParent,
				LeftKey:  &keyNode,
				RightKey: &keyParentRight,
			},
			node: &proto.RangeTreeNode{
				Key:       keyNode,
				ParentKey: &keyParent,
				LeftKey:   &keyLeft,
				RightKey:  &keyRight,
			},
			left: &proto.RangeTreeNode{
				Key:       keyLeft,
				ParentKey: &keyNode,
				LeftKey:   &keyLeftLeft,
				RightKey:  &keyLeftRight,
			},
			leftRight: &proto.RangeTreeNode{
				Key:       keyLeftRight,
				ParentKey: &keyLeft,
				LeftKey:   &keyLeftRightLeft,
				RightKey:  &keyLeftRightRight,
			},
			expectedParent: &proto.RangeTreeNode{
				Key:      keyParent,
				LeftKey:  &keyLeft,
				RightKey: &keyParentRight,
			},
			expectedNode: &proto.RangeTreeNode{
				Key:       keyNode,
				ParentKey: &keyLeft,
				LeftKey:   &keyLeftRight,
				RightKey:  &keyRight,
			},
			expectedLeft: &proto.RangeTreeNode{
				Key:       keyLeft,
				ParentKey: &keyParent,
				LeftKey:   &keyLeftLeft,
				RightKey:  &keyNode,
			},
			expectedLeftRight: &proto.RangeTreeNode{
				Key:       keyLeftRight,
				ParentKey: &keyNode,
				LeftKey:   &keyLeftRightLeft,
				RightKey:  &keyLeftRightRight,
			},
		},
		// Test Case 1: Normal Rotation, parent right
		{
			parent: &proto.RangeTreeNode{
				Key:      keyParent,
				LeftKey:  &keyParentLeft,
				RightKey: &keyNode,
			},
			node: &proto.RangeTreeNode{
				Key:       keyNode,
				ParentKey: &keyParent,
				LeftKey:   &keyLeft,
				RightKey:  &keyRight,
			},
			left: &proto.RangeTreeNode{
				Key:       keyLeft,
				ParentKey: &keyNode,
				LeftKey:   &keyLeftLeft,
				RightKey:  &keyLeftRight,
			},
			leftRight: &proto.RangeTreeNode{
				Key:       keyLeftRight,
				ParentKey: &keyLeft,
				LeftKey:   &keyLeftRightLeft,
				RightKey:  &keyLeftRightRight,
			},
			expectedParent: &proto.RangeTreeNode{
				Key:      keyParent,
				LeftKey:  &keyParentLeft,
				RightKey: &keyLeft,
			},
			expectedNode: &proto.RangeTreeNode{
				Key:       keyNode,
				ParentKey: &keyLeft,
				LeftKey:   &keyLeftRight,
				RightKey:  &keyRight,
			},
			expectedLeft: &proto.RangeTreeNode{
				Key:       keyLeft,
				ParentKey: &keyParent,
				LeftKey:   &keyLeftLeft,
				RightKey:  &keyNode,
			},
			expectedLeftRight: &proto.RangeTreeNode{
				Key:       keyLeftRight,
				ParentKey: &keyNode,
				LeftKey:   &keyLeftRightLeft,
				RightKey:  &keyLeftRightRight,
			},
		},
		// Test Case 2: Root Rotation, no leftRight node
		{
			node: &proto.RangeTreeNode{
				Key:      keyNode,
				LeftKey:  &keyLeft,
				RightKey: &keyRight,
			},
			left: &proto.RangeTreeNode{
				Key:       keyLeft,
				ParentKey: &keyNode,
				LeftKey:   &keyLeftLeft,
			},
			expectedNode: &proto.RangeTreeNode{
				Key:       keyNode,
				ParentKey: &keyLeft,
				RightKey:  &keyRight,
			},
			expectedLeft: &proto.RangeTreeNode{
				Key:      keyLeft,
				LeftKey:  &keyLeftLeft,
				RightKey: &keyNode,
			},
		},
	}

	for i, test := range testCases {
		tc := createTreeContext(&keyNode, []*proto.RangeTreeNode{
			test.parent,
			test.node,
			test.left,
			test.leftRight,
		})

		// Perform the rotation.
		actualLeft, err := tc.rotateRight(test.node)
		if err != nil {
			t.Fatal(err)
		}

		checkTreeNode(t, tc, i, "parent", keyParent, test.expectedParent, nil)
		checkTreeNode(t, tc, i, "node", keyNode, test.expectedNode, nil)
		checkTreeNode(t, tc, i, "left", keyLeft, test.expectedLeft, actualLeft)
		checkTreeNode(t, tc, i, "leftRight", keyLeftRight, test.expectedLeftRight, nil)

		// Perform the reverse rotation and expect to get the original values
		// back.
		actualNode, err := tc.rotateLeft(actualLeft)
		if err != nil {
			t.Fatal(err)
		}

		checkTreeNode(t, tc, i, "parent-reversed", keyParent, test.parent, nil)
		checkTreeNode(t, tc, i, "node-reversed", keyNode, test.node, actualNode)
		checkTreeNode(t, tc, i, "left-reversed", keyLeft, test.left, nil)
		checkTreeNode(t, tc, i, "leftRight-reversed", keyLeftRight, test.leftRight, nil)
	}
}

// TestRotateLeft ensures that right rotations occur correctly. This is a
// mirror of TestRotateRight.
func TestRotateLeft(t *testing.T) {
	defer leaktest.AfterTest(t)

	keyNode := proto.Key("N")
	keyLeft := proto.Key("L")
	keyRight := proto.Key("R")
	keyRightLeft := proto.Key("RL")
	keyRightRight := proto.Key("RR")
	keyRightLeftRight := proto.Key("RLR")
	keyRightLeftLeft := proto.Key("RLL")
	keyParent := proto.Key("P")
	keyParentLeft := proto.Key("PL")
	keyParentRight := proto.Key("PR")

	testCases := []struct {
		node              *proto.RangeTreeNode
		parent            *proto.RangeTreeNode
		right             *proto.RangeTreeNode
		rightLeft         *proto.RangeTreeNode
		expectedNode      *proto.RangeTreeNode
		expectedParent    *proto.RangeTreeNode
		expectedRight     *proto.RangeTreeNode
		expectedRightLeft *proto.RangeTreeNode
	}{
		// Test Case 0: Normal Rotation, parent left
		{
			parent: &proto.RangeTreeNode{
				Key:      keyParent,
				LeftKey:  &keyNode,
				RightKey: &keyParentRight,
			},
			node: &proto.RangeTreeNode{
				Key:       keyNode,
				ParentKey: &keyParent,
				LeftKey:   &keyLeft,
				RightKey:  &keyRight,
			},
			right: &proto.RangeTreeNode{
				Key:       keyRight,
				ParentKey: &keyNode,
				LeftKey:   &keyRightLeft,
				RightKey:  &keyRightRight,
			},
			rightLeft: &proto.RangeTreeNode{
				Key:       keyRightLeft,
				ParentKey: &keyRight,
				LeftKey:   &keyRightLeftLeft,
				RightKey:  &keyRightLeftRight,
			},
			expectedParent: &proto.RangeTreeNode{
				Key:      keyParent,
				LeftKey:  &keyRight,
				RightKey: &keyParentRight,
			},
			expectedNode: &proto.RangeTreeNode{
				Key:       keyNode,
				ParentKey: &keyRight,
				LeftKey:   &keyLeft,
				RightKey:  &keyRightLeft,
			},
			expectedRight: &proto.RangeTreeNode{
				Key:       keyRight,
				ParentKey: &keyParent,
				LeftKey:   &keyNode,
				RightKey:  &keyRightRight,
			},
			expectedRightLeft: &proto.RangeTreeNode{
				Key:       keyRightLeft,
				ParentKey: &keyNode,
				LeftKey:   &keyRightLeftLeft,
				RightKey:  &keyRightLeftRight,
			},
		},
		// Test Case 1: Normal Rotation, parent right
		{
			parent: &proto.RangeTreeNode{
				Key:      keyParent,
				LeftKey:  &keyParentLeft,
				RightKey: &keyNode,
			},
			node: &proto.RangeTreeNode{
				Key:       keyNode,
				ParentKey: &keyParent,
				LeftKey:   &keyLeft,
				RightKey:  &keyRight,
			},
			right: &proto.RangeTreeNode{
				Key:       keyRight,
				ParentKey: &keyNode,
				LeftKey:   &keyRightLeft,
				RightKey:  &keyRightRight,
			},
			rightLeft: &proto.RangeTreeNode{
				Key:       keyRightLeft,
				ParentKey: &keyRight,
				LeftKey:   &keyRightLeftLeft,
				RightKey:  &keyRightLeftRight,
			},
			expectedParent: &proto.RangeTreeNode{
				Key:      keyParent,
				LeftKey:  &keyParentLeft,
				RightKey: &keyRight,
			},
			expectedNode: &proto.RangeTreeNode{
				Key:       keyNode,
				ParentKey: &keyRight,
				LeftKey:   &keyLeft,
				RightKey:  &keyRightLeft,
			},
			expectedRight: &proto.RangeTreeNode{
				Key:       keyRight,
				ParentKey: &keyParent,
				LeftKey:   &keyNode,
				RightKey:  &keyRightRight,
			},
			expectedRightLeft: &proto.RangeTreeNode{
				Key:       keyRightLeft,
				ParentKey: &keyNode,
				LeftKey:   &keyRightLeftLeft,
				RightKey:  &keyRightLeftRight,
			},
		},
		// Test Case 2: Root Rotation, no leftRight node
		{
			node: &proto.RangeTreeNode{
				Key:      keyNode,
				LeftKey:  &keyLeft,
				RightKey: &keyRight,
			},
			right: &proto.RangeTreeNode{
				Key:       keyRight,
				ParentKey: &keyNode,
				RightKey:  &keyRightRight,
			},
			expectedNode: &proto.RangeTreeNode{
				Key:       keyNode,
				ParentKey: &keyRight,
				LeftKey:   &keyLeft,
			},
			expectedRight: &proto.RangeTreeNode{
				Key:      keyRight,
				LeftKey:  &keyNode,
				RightKey: &keyRightRight,
			},
		},
	}

	for i, test := range testCases {
		tc := createTreeContext(&keyNode, []*proto.RangeTreeNode{
			test.parent,
			test.node,
			test.right,
			test.rightLeft,
		})

		// Perform the rotation.
		actualRight, err := tc.rotateLeft(test.node)
		if err != nil {
			t.Fatal(err)
		}

		checkTreeNode(t, tc, i, "parent", keyParent, test.expectedParent, nil)
		checkTreeNode(t, tc, i, "node", keyNode, test.expectedNode, nil)
		checkTreeNode(t, tc, i, "right", keyRight, test.expectedRight, actualRight)
		checkTreeNode(t, tc, i, "rightLeft", keyRightLeft, test.expectedRightLeft, nil)

		// Perform the reverse rotation and expect to get the original values
		// back.
		actualNode, err := tc.rotateRight(actualRight)
		if err != nil {
			t.Fatal(err)
		}

		checkTreeNode(t, tc, i, "parent-reversed", keyParent, test.parent, nil)
		checkTreeNode(t, tc, i, "node-reversed", keyNode, test.node, actualNode)
		checkTreeNode(t, tc, i, "right-reversed", keyRight, test.right, nil)
		checkTreeNode(t, tc, i, "rightLeft-reversed", keyRightLeft, test.rightLeft, nil)
	}
}

// verifyTree checks to ensure that the tree is indeed balanced and a correct
// red-black tree. It does so by checking each of the red-black tree properties.
func verifyTree(t *testing.T, tc *treeContext, testName string) {
	root, err := tc.getNode(&tc.tree.RootKey)
	if err != nil {
		t.Fatal(err)
	}

	verifyBinarySearchTree(t, tc, testName, root)
	// Property 1 is always correct. All nodes are already colored.
	verifyProperty2(t, tc, testName, root)
	// Property 3 is always correct. All leaves are black.
	verifyProperty4(t, tc, testName, root)
	pathBlackCount := new(int)
	*pathBlackCount = -1
	verifyProperty5(t, tc, testName, root, 0, pathBlackCount)
}

// verifyBinarySearchTree checks to ensure that all keys to the left of the root
// node are less than it, and all nodes to the right of the root node are
// greater than it. Performs this same check for all other nodes as well.
// Note that this test can be very expensive.
func verifyBinarySearchTree(t *testing.T, tc *treeContext, testName string, root *proto.RangeTreeNode) {
	if root.LeftKey != nil {
		left, err := tc.getNode(root.LeftKey)
		if err != nil {
			t.Fatal(err)
		}
		verifyBinarySearchTreeHelper(t, tc, testName, left, root.Key, true)
	}
	if root.RightKey != nil {
		right, err := tc.getNode(root.RightKey)
		if err != nil {
			t.Fatal(err)
		}
		verifyBinarySearchTreeHelper(t, tc, testName, right, root.Key, false)
	}
}

// verifyBinarySearchTreeHelper walks the tree and recursively calls itself
// ensures that all nodes are indeed either less or greater than the passed in
// key. It also recursively calls itself at each node.
func verifyBinarySearchTreeHelper(t *testing.T, tc *treeContext, testName string, node *proto.RangeTreeNode, key proto.Key, isLess bool) {
	if node.Key.Equal(key) {
		t.Errorf("%s: Duplicate key detected: %s", testName, key)
	}
	if e, a := isLess, node.Key.Less(key); e != a {
		if isLess {
			t.Errorf("%s: Failed Property BST - The key %s is not less than %s.", testName, node.Key, key)
		} else {
			t.Errorf("%s: Failed Property BST - The key %s is not greater than %s.", testName, node.Key, key)
		}
	}
	if node.LeftKey != nil {
		left, err := tc.getNode(node.LeftKey)
		if err != nil {
			t.Fatal(err)
		}
		// Check that all nodes to the left are less or greater than the passed
		// in key.
		verifyBinarySearchTreeHelper(t, tc, testName, left, key, isLess)
		// Check that all nodes to the left are less than the current node's
		// key.
		verifyBinarySearchTreeHelper(t, tc, testName, left, node.Key, true)
	}
	if node.RightKey != nil {
		right, err := tc.getNode(node.RightKey)
		if err != nil {
			t.Fatal(err)
		}
		// Check that all nodes to the right are less or greater than the passed
		// in key.
		verifyBinarySearchTreeHelper(t, tc, testName, right, key, isLess)
		// Check that all nodes to the right are greater than the current node's
		// key.
		verifyBinarySearchTreeHelper(t, tc, testName, right, node.Key, false)
	}
}

// verifyProperty2 ensures that the root node is black.
func verifyProperty2(t *testing.T, tc *treeContext, testName string, root *proto.RangeTreeNode) {
	if e, a := false, isRed(root); e != a {
		t.Errorf("%s: Failed Property 2 - The root node is not black.", testName)
	}
}

// verifyProperty4 ensures that the parent of every red node is black.
func verifyProperty4(t *testing.T, tc *treeContext, testName string, node *proto.RangeTreeNode) {
	if node == nil {
		return
	}
	left, err := tc.getNode(node.LeftKey)
	if err != nil {
		t.Fatal(err)
	}
	right, err := tc.getNode(node.RightKey)
	if err != nil {
		t.Fatal(err)
	}
	if isRed(node) {
		if e, a := false, isRed(left); e != a {
			t.Errorf("%s: Failed property 4 - Red Node %s's left child %s is also red.", testName, node.Key, left.Key)
		}
		if e, a := false, isRed(right); e != a {
			t.Errorf("%s: Failed property 4 - Red Node %s's right child %s is also red.", testName, node.Key, right.Key)
		}
	}
	verifyProperty4(t, tc, testName, left)
	verifyProperty4(t, tc, testName, right)
}

// verifyProperty5 ensures that all paths from any given node to its leaf nodes
// contain the same number of black nodes.
func verifyProperty5(t *testing.T, tc *treeContext, testName string, node *proto.RangeTreeNode, blackCount int, pathBlackCount *int) {
	if !isRed(node) {
		blackCount++
	}
	if node == nil {
		if *pathBlackCount == -1 {
			*pathBlackCount = blackCount
		} else {
			if e, a := *pathBlackCount, blackCount; e != a {
				t.Errorf("%s: Failed property 5 - Expected a black count of %d but instead got %d.", testName, e, a)
			}
		}
		return
	}

	left, err := tc.getNode(node.LeftKey)
	if err != nil {
		t.Fatal(err)
	}
	right, err := tc.getNode(node.RightKey)
	if err != nil {
		t.Fatal(err)
	}
	verifyProperty5(t, tc, testName, left, blackCount, pathBlackCount)
	verifyProperty5(t, tc, testName, right, blackCount, pathBlackCount)
}

// TestInsert tries inserting nodes into the range tree. The tree is verified
// after each insert.
func TestInsert(t *testing.T) {
	defer leaktest.AfterTest(t)

	keyRoot := proto.Key("m")
	tc := createTreeContext(&keyRoot, []*proto.RangeTreeNode{
		{
			Key:   keyRoot,
			Black: true,
		},
	})
	verifyTree(t, tc, "m-root")

	// This order of keys is designed to stress the tree in different ways.
	// Specifically going through each of the insert case's paths. The first row
	// always adds keys on the far left. The second row adds keys to the left of
	// the original root. The third row puts keys to the immediate right of the
	// original root and the last row puts keys on the far right of the tree.
	keys := []string{"f", "e", "d", "c", "b", "a",
		"g", "h", "i", "j", "k", "l",
		"s", "r", "q", "p", "o", "n",
		"t", "u", "v", "w", "x", "y", "z"}
	for _, key := range keys {
		node := &proto.RangeTreeNode{
			Key: proto.Key(key),
		}
		err := tc.insert(node)
		if err != nil {
			t.Fatal(err)
		}
		verifyTree(t, tc, key)
	}

	// Try adding an already added key.
	node := &proto.RangeTreeNode{
		Key: proto.Key("z"),
	}
	if err := tc.insert(node); err == nil {
		t.Fatal("inserting an already existing key should fail")
	}
	verifyTree(t, tc, "z-repeat")
}
