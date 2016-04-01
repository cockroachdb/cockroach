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
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func createTreeContext(rootKey roachpb.RKey, nodes []*RangeTreeNode) *treeContext {
	root := &RangeTree{
		RootKey: rootKey,
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
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		node     *RangeTreeNode
		expected bool
	}{
		// normal black node
		{&RangeTreeNode{Black: true}, false},
		// normal red node
		{&RangeTreeNode{Black: false}, true},
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
func checkTreeNode(t *testing.T, tc *treeContext, testNumber int, name string, key roachpb.RKey, expected, actual *RangeTreeNode) {
	if expected != nil {
		// Is the value correct?
		cached, err := tc.getNode(expected.Key)
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
	defer leaktest.AfterTest(t)()

	keyRoot := roachpb.RKey("ROOT")
	keyOld := roachpb.RKey("O")
	keyOldLeft := roachpb.RKey("OL")
	keyOldRight := roachpb.RKey("OR")
	keyNew := roachpb.RKey("N")
	keyNewLeft := roachpb.RKey("NL")
	keyNewRight := roachpb.RKey("NR")
	keyParent := roachpb.RKey("P")
	keyParentParent := roachpb.RKey("PP")
	keyParentLeft := roachpb.RKey("PL")
	keyParentRight := roachpb.RKey("PR")

	testCases := []struct {
		root           roachpb.RKey
		parent         *RangeTreeNode
		oldNode        *RangeTreeNode
		newNode        *RangeTreeNode
		expectedRoot   roachpb.RKey
		expectedParent *RangeTreeNode
		expectedNew    *RangeTreeNode
		expectedErr    bool
	}{
		// Test Case 0: root, replace with nil, should fail
		{
			oldNode: &RangeTreeNode{
				Key: keyOld,
			},
			expectedErr: true,
		},
		// Test Case 1: non-root, replace with nil, left child
		{
			root: keyRoot,
			oldNode: &RangeTreeNode{
				Key:       keyOld,
				ParentKey: keyParent,
				LeftKey:   keyOldLeft,
				RightKey:  keyOldRight,
			},
			parent: &RangeTreeNode{
				Key:       keyParent,
				ParentKey: keyParentParent,
				LeftKey:   keyOld,
				RightKey:  keyParentRight,
			},
			expectedRoot: keyRoot,
			expectedParent: &RangeTreeNode{
				Key:       keyParent,
				ParentKey: keyParentParent,
				RightKey:  keyParentRight,
			},
		},
		// Test Case 2: non-root, replace with nil, right child
		{
			root: keyRoot,
			oldNode: &RangeTreeNode{
				Key:       keyOld,
				ParentKey: keyParent,
				LeftKey:   keyOldLeft,
				RightKey:  keyOldRight,
			},
			parent: &RangeTreeNode{
				Key:       keyParent,
				ParentKey: keyParentParent,
				LeftKey:   keyParentLeft,
				RightKey:  keyOld,
			},
			expectedRoot: keyRoot,
			expectedParent: &RangeTreeNode{
				Key:       keyParent,
				ParentKey: keyParentParent,
				LeftKey:   keyParentLeft,
			},
		},
		// Test Case 3: non-root, replace with node, left child
		{
			root: keyRoot,
			oldNode: &RangeTreeNode{
				Key:       keyOld,
				ParentKey: keyParent,
				LeftKey:   keyOldLeft,
				RightKey:  keyOldRight,
			},
			parent: &RangeTreeNode{
				Key:       keyParent,
				ParentKey: keyParentParent,
				LeftKey:   keyOld,
				RightKey:  keyParentRight,
			},
			newNode: &RangeTreeNode{
				Key:      keyNew,
				LeftKey:  keyNewLeft,
				RightKey: keyNewRight,
			},
			expectedRoot: keyRoot,
			expectedParent: &RangeTreeNode{
				Key:       keyParent,
				ParentKey: keyParentParent,
				LeftKey:   keyNew,
				RightKey:  keyParentRight,
			},
			expectedNew: &RangeTreeNode{
				Key:       keyNew,
				ParentKey: keyParent,
				LeftKey:   keyNewLeft,
				RightKey:  keyNewRight,
			},
		},
		// Test Case 4: non-root, replace with node, right child
		{
			root: keyRoot,
			oldNode: &RangeTreeNode{
				Key:       keyOld,
				ParentKey: keyParent,
				LeftKey:   keyOldLeft,
				RightKey:  keyOldRight,
			},
			parent: &RangeTreeNode{
				Key:       keyParent,
				ParentKey: keyParentParent,
				LeftKey:   keyParentLeft,
				RightKey:  keyOld,
			},
			newNode: &RangeTreeNode{
				Key:      keyNew,
				LeftKey:  keyNewLeft,
				RightKey: keyNewRight,
			},
			expectedRoot: keyRoot,
			expectedParent: &RangeTreeNode{
				Key:       keyParent,
				ParentKey: keyParentParent,
				LeftKey:   keyParentLeft,
				RightKey:  keyNew,
			},
			expectedNew: &RangeTreeNode{
				Key:       keyNew,
				ParentKey: keyParent,
				LeftKey:   keyNewLeft,
				RightKey:  keyNewRight,
			},
		},
		// Test Case 5: root, replace with node
		{
			root: keyOld,
			oldNode: &RangeTreeNode{
				Key:      keyOld,
				LeftKey:  keyOldLeft,
				RightKey: keyOldRight,
			},
			newNode: &RangeTreeNode{
				Key:      keyNew,
				LeftKey:  keyNewLeft,
				RightKey: keyNewRight,
			},
			expectedRoot: keyNew,
			expectedNew: &RangeTreeNode{
				Key:      keyNew,
				LeftKey:  keyNewLeft,
				RightKey: keyNewRight,
			},
		},
	}

	for i, test := range testCases {
		tc := createTreeContext(test.root, []*RangeTreeNode{
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
	defer leaktest.AfterTest(t)()

	keyNode := roachpb.RKey("N")
	keyRight := roachpb.RKey("R")
	keyLeft := roachpb.RKey("L")
	keyLeftLeft := roachpb.RKey("LL")
	keyLeftRight := roachpb.RKey("LR")
	keyLeftRightLeft := roachpb.RKey("LRL")
	keyLeftRightRight := roachpb.RKey("LRR")
	keyParent := roachpb.RKey("P")
	keyParentLeft := roachpb.RKey("PL")
	keyParentRight := roachpb.RKey("PR")

	testCases := []struct {
		node              *RangeTreeNode
		parent            *RangeTreeNode
		left              *RangeTreeNode
		leftRight         *RangeTreeNode
		expectedNode      *RangeTreeNode
		expectedParent    *RangeTreeNode
		expectedLeft      *RangeTreeNode
		expectedLeftRight *RangeTreeNode
	}{
		// Test Case 0: Normal Rotation, parent left
		{
			parent: &RangeTreeNode{
				Key:      keyParent,
				LeftKey:  keyNode,
				RightKey: keyParentRight,
			},
			node: &RangeTreeNode{
				Key:       keyNode,
				ParentKey: keyParent,
				LeftKey:   keyLeft,
				RightKey:  keyRight,
			},
			left: &RangeTreeNode{
				Key:       keyLeft,
				ParentKey: keyNode,
				LeftKey:   keyLeftLeft,
				RightKey:  keyLeftRight,
			},
			leftRight: &RangeTreeNode{
				Key:       keyLeftRight,
				ParentKey: keyLeft,
				LeftKey:   keyLeftRightLeft,
				RightKey:  keyLeftRightRight,
			},
			expectedParent: &RangeTreeNode{
				Key:      keyParent,
				LeftKey:  keyLeft,
				RightKey: keyParentRight,
			},
			expectedNode: &RangeTreeNode{
				Key:       keyNode,
				ParentKey: keyLeft,
				LeftKey:   keyLeftRight,
				RightKey:  keyRight,
			},
			expectedLeft: &RangeTreeNode{
				Key:       keyLeft,
				ParentKey: keyParent,
				LeftKey:   keyLeftLeft,
				RightKey:  keyNode,
			},
			expectedLeftRight: &RangeTreeNode{
				Key:       keyLeftRight,
				ParentKey: keyNode,
				LeftKey:   keyLeftRightLeft,
				RightKey:  keyLeftRightRight,
			},
		},
		// Test Case 1: Normal Rotation, parent right
		{
			parent: &RangeTreeNode{
				Key:      keyParent,
				LeftKey:  keyParentLeft,
				RightKey: keyNode,
			},
			node: &RangeTreeNode{
				Key:       keyNode,
				ParentKey: keyParent,
				LeftKey:   keyLeft,
				RightKey:  keyRight,
			},
			left: &RangeTreeNode{
				Key:       keyLeft,
				ParentKey: keyNode,
				LeftKey:   keyLeftLeft,
				RightKey:  keyLeftRight,
			},
			leftRight: &RangeTreeNode{
				Key:       keyLeftRight,
				ParentKey: keyLeft,
				LeftKey:   keyLeftRightLeft,
				RightKey:  keyLeftRightRight,
			},
			expectedParent: &RangeTreeNode{
				Key:      keyParent,
				LeftKey:  keyParentLeft,
				RightKey: keyLeft,
			},
			expectedNode: &RangeTreeNode{
				Key:       keyNode,
				ParentKey: keyLeft,
				LeftKey:   keyLeftRight,
				RightKey:  keyRight,
			},
			expectedLeft: &RangeTreeNode{
				Key:       keyLeft,
				ParentKey: keyParent,
				LeftKey:   keyLeftLeft,
				RightKey:  keyNode,
			},
			expectedLeftRight: &RangeTreeNode{
				Key:       keyLeftRight,
				ParentKey: keyNode,
				LeftKey:   keyLeftRightLeft,
				RightKey:  keyLeftRightRight,
			},
		},
		// Test Case 2: Root Rotation, no leftRight node
		{
			node: &RangeTreeNode{
				Key:      keyNode,
				LeftKey:  keyLeft,
				RightKey: keyRight,
			},
			left: &RangeTreeNode{
				Key:       keyLeft,
				ParentKey: keyNode,
				LeftKey:   keyLeftLeft,
			},
			expectedNode: &RangeTreeNode{
				Key:       keyNode,
				ParentKey: keyLeft,
				RightKey:  keyRight,
			},
			expectedLeft: &RangeTreeNode{
				Key:      keyLeft,
				LeftKey:  keyLeftLeft,
				RightKey: keyNode,
			},
		},
	}

	for i, test := range testCases {
		tc := createTreeContext(keyNode, []*RangeTreeNode{
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
	defer leaktest.AfterTest(t)()

	keyNode := roachpb.RKey("N")
	keyLeft := roachpb.RKey("L")
	keyRight := roachpb.RKey("R")
	keyRightLeft := roachpb.RKey("RL")
	keyRightRight := roachpb.RKey("RR")
	keyRightLeftRight := roachpb.RKey("RLR")
	keyRightLeftLeft := roachpb.RKey("RLL")
	keyParent := roachpb.RKey("P")
	keyParentLeft := roachpb.RKey("PL")
	keyParentRight := roachpb.RKey("PR")

	testCases := []struct {
		node              *RangeTreeNode
		parent            *RangeTreeNode
		right             *RangeTreeNode
		rightLeft         *RangeTreeNode
		expectedNode      *RangeTreeNode
		expectedParent    *RangeTreeNode
		expectedRight     *RangeTreeNode
		expectedRightLeft *RangeTreeNode
	}{
		// Test Case 0: Normal Rotation, parent left
		{
			parent: &RangeTreeNode{
				Key:      keyParent,
				LeftKey:  keyNode,
				RightKey: keyParentRight,
			},
			node: &RangeTreeNode{
				Key:       keyNode,
				ParentKey: keyParent,
				LeftKey:   keyLeft,
				RightKey:  keyRight,
			},
			right: &RangeTreeNode{
				Key:       keyRight,
				ParentKey: keyNode,
				LeftKey:   keyRightLeft,
				RightKey:  keyRightRight,
			},
			rightLeft: &RangeTreeNode{
				Key:       keyRightLeft,
				ParentKey: keyRight,
				LeftKey:   keyRightLeftLeft,
				RightKey:  keyRightLeftRight,
			},
			expectedParent: &RangeTreeNode{
				Key:      keyParent,
				LeftKey:  keyRight,
				RightKey: keyParentRight,
			},
			expectedNode: &RangeTreeNode{
				Key:       keyNode,
				ParentKey: keyRight,
				LeftKey:   keyLeft,
				RightKey:  keyRightLeft,
			},
			expectedRight: &RangeTreeNode{
				Key:       keyRight,
				ParentKey: keyParent,
				LeftKey:   keyNode,
				RightKey:  keyRightRight,
			},
			expectedRightLeft: &RangeTreeNode{
				Key:       keyRightLeft,
				ParentKey: keyNode,
				LeftKey:   keyRightLeftLeft,
				RightKey:  keyRightLeftRight,
			},
		},
		// Test Case 1: Normal Rotation, parent right
		{
			parent: &RangeTreeNode{
				Key:      keyParent,
				LeftKey:  keyParentLeft,
				RightKey: keyNode,
			},
			node: &RangeTreeNode{
				Key:       keyNode,
				ParentKey: keyParent,
				LeftKey:   keyLeft,
				RightKey:  keyRight,
			},
			right: &RangeTreeNode{
				Key:       keyRight,
				ParentKey: keyNode,
				LeftKey:   keyRightLeft,
				RightKey:  keyRightRight,
			},
			rightLeft: &RangeTreeNode{
				Key:       keyRightLeft,
				ParentKey: keyRight,
				LeftKey:   keyRightLeftLeft,
				RightKey:  keyRightLeftRight,
			},
			expectedParent: &RangeTreeNode{
				Key:      keyParent,
				LeftKey:  keyParentLeft,
				RightKey: keyRight,
			},
			expectedNode: &RangeTreeNode{
				Key:       keyNode,
				ParentKey: keyRight,
				LeftKey:   keyLeft,
				RightKey:  keyRightLeft,
			},
			expectedRight: &RangeTreeNode{
				Key:       keyRight,
				ParentKey: keyParent,
				LeftKey:   keyNode,
				RightKey:  keyRightRight,
			},
			expectedRightLeft: &RangeTreeNode{
				Key:       keyRightLeft,
				ParentKey: keyNode,
				LeftKey:   keyRightLeftLeft,
				RightKey:  keyRightLeftRight,
			},
		},
		// Test Case 2: Root Rotation, no leftRight node
		{
			node: &RangeTreeNode{
				Key:      keyNode,
				LeftKey:  keyLeft,
				RightKey: keyRight,
			},
			right: &RangeTreeNode{
				Key:       keyRight,
				ParentKey: keyNode,
				RightKey:  keyRightRight,
			},
			expectedNode: &RangeTreeNode{
				Key:       keyNode,
				ParentKey: keyRight,
				LeftKey:   keyLeft,
			},
			expectedRight: &RangeTreeNode{
				Key:      keyRight,
				LeftKey:  keyNode,
				RightKey: keyRightRight,
			},
		},
	}

	for i, test := range testCases {
		tc := createTreeContext(keyNode, []*RangeTreeNode{
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

// TestSwapNodes ensures that node swap needed for deletions occur correctly.
func TestSwapNodes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	keyA := roachpb.RKey("A")
	keyAParent := roachpb.RKey("AP")
	keyAParentLeft := roachpb.RKey("APL")
	keyAParentRight := roachpb.RKey("APR")
	keyALeft := roachpb.RKey("AL")
	keyARight := roachpb.RKey("AR")
	keyB := roachpb.RKey("B")
	keyBParent := roachpb.RKey("BP")
	keyBParentLeft := roachpb.RKey("BPL")
	keyBParentRight := roachpb.RKey("BPR")
	keyBLeft := roachpb.RKey("BL")
	keyBRight := roachpb.RKey("BR")
	keyRoot := roachpb.RKey("R")

	testCases := []struct {
		root            roachpb.RKey
		a               *RangeTreeNode
		aParent         *RangeTreeNode
		aLeft           *RangeTreeNode
		aRight          *RangeTreeNode
		b               *RangeTreeNode
		bParent         *RangeTreeNode
		bLeft           *RangeTreeNode
		bRight          *RangeTreeNode
		rootExpected    roachpb.RKey
		aExpected       *RangeTreeNode
		aParentExpected *RangeTreeNode
		aLeftExpected   *RangeTreeNode
		aRightExpected  *RangeTreeNode
		bExpected       *RangeTreeNode
		bParentExpected *RangeTreeNode
		bLeftExpected   *RangeTreeNode
		bRightExpected  *RangeTreeNode
	}{
		// Test Case 0: Normal swap, two separate nodes, a's a left child, b's a left child
		// This should just swap all direct references to a and b, leaving
		// everything else intact.
		{
			root: keyRoot,
			a: &RangeTreeNode{
				Key:       keyA,
				LeftKey:   keyALeft,
				RightKey:  keyARight,
				ParentKey: keyAParent,
				Black:     true,
			},
			aParent: &RangeTreeNode{
				Key:      keyAParent,
				LeftKey:  keyA,
				RightKey: keyAParentRight,
			},
			aLeft: &RangeTreeNode{
				Key:       keyALeft,
				ParentKey: keyA,
			},
			aRight: &RangeTreeNode{
				Key:       keyARight,
				ParentKey: keyA,
			},
			b: &RangeTreeNode{
				Key:       keyB,
				LeftKey:   keyBLeft,
				RightKey:  keyBRight,
				ParentKey: keyBParent,
				Black:     false,
			},
			bParent: &RangeTreeNode{
				Key:      keyBParent,
				LeftKey:  keyB,
				RightKey: keyBParentRight,
			},
			bLeft: &RangeTreeNode{
				Key:       keyBLeft,
				ParentKey: keyB,
			},
			bRight: &RangeTreeNode{
				Key:       keyBRight,
				ParentKey: keyB,
			},
			rootExpected: keyRoot,
			aExpected: &RangeTreeNode{
				Key:       keyA,
				LeftKey:   keyBLeft,
				RightKey:  keyBRight,
				ParentKey: keyBParent,
				Black:     false,
			},
			aParentExpected: &RangeTreeNode{
				Key:      keyAParent,
				LeftKey:  keyB,
				RightKey: keyAParentRight,
			},
			aLeftExpected: &RangeTreeNode{
				Key:       keyALeft,
				ParentKey: keyB,
			},
			aRightExpected: &RangeTreeNode{
				Key:       keyARight,
				ParentKey: keyB,
			},
			bExpected: &RangeTreeNode{
				Key:       keyB,
				LeftKey:   keyALeft,
				RightKey:  keyARight,
				ParentKey: keyAParent,
				Black:     true,
			},
			bParentExpected: &RangeTreeNode{
				Key:      keyBParent,
				LeftKey:  keyA,
				RightKey: keyBParentRight,
			},
			bLeftExpected: &RangeTreeNode{
				Key:       keyBLeft,
				ParentKey: keyA,
			},
			bRightExpected: &RangeTreeNode{
				Key:       keyBRight,
				ParentKey: keyA,
			},
		},
		// Test Case 1: Normal swap, two separate nodes, a's a right child, b's a right child
		// This should just swap all direct references to a and b, leaving
		// everything else intact.
		{
			root: keyRoot,
			a: &RangeTreeNode{
				Key:       keyA,
				LeftKey:   keyALeft,
				RightKey:  keyARight,
				ParentKey: keyAParent,
				Black:     true,
			},
			aParent: &RangeTreeNode{
				Key:      keyAParent,
				LeftKey:  keyAParentLeft,
				RightKey: keyA,
			},
			aLeft: &RangeTreeNode{
				Key:       keyALeft,
				ParentKey: keyA,
			},
			aRight: &RangeTreeNode{
				Key:       keyARight,
				ParentKey: keyA,
			},
			b: &RangeTreeNode{
				Key:       keyB,
				LeftKey:   keyBLeft,
				RightKey:  keyBRight,
				ParentKey: keyBParent,
				Black:     false,
			},
			bParent: &RangeTreeNode{
				Key:      keyBParent,
				LeftKey:  keyBParentLeft,
				RightKey: keyB,
			},
			bLeft: &RangeTreeNode{
				Key:       keyBLeft,
				ParentKey: keyB,
			},
			bRight: &RangeTreeNode{
				Key:       keyBRight,
				ParentKey: keyB,
			},
			rootExpected: keyRoot,
			aExpected: &RangeTreeNode{
				Key:       keyA,
				LeftKey:   keyBLeft,
				RightKey:  keyBRight,
				ParentKey: keyBParent,
				Black:     false,
			},
			aParentExpected: &RangeTreeNode{
				Key:      keyAParent,
				LeftKey:  keyAParentLeft,
				RightKey: keyB,
			},
			aLeftExpected: &RangeTreeNode{
				Key:       keyALeft,
				ParentKey: keyB,
			},
			aRightExpected: &RangeTreeNode{
				Key:       keyARight,
				ParentKey: keyB,
			},
			bExpected: &RangeTreeNode{
				Key:       keyB,
				LeftKey:   keyALeft,
				RightKey:  keyARight,
				ParentKey: keyAParent,
				Black:     true,
			},
			bParentExpected: &RangeTreeNode{
				Key:      keyBParent,
				LeftKey:  keyBParentLeft,
				RightKey: keyA,
			},
			bLeftExpected: &RangeTreeNode{
				Key:       keyBLeft,
				ParentKey: keyA,
			},
			bRightExpected: &RangeTreeNode{
				Key:       keyBRight,
				ParentKey: keyA,
			},
		},
		// Test Case 2: b is a's right child, a is a right child
		{
			root: keyRoot,
			a: &RangeTreeNode{
				Key:       keyA,
				LeftKey:   keyALeft,
				RightKey:  keyB,
				ParentKey: keyAParent,
				Black:     true,
			},
			aParent: &RangeTreeNode{
				Key:      keyAParent,
				LeftKey:  keyAParentLeft,
				RightKey: keyA,
			},
			aLeft: &RangeTreeNode{
				Key:       keyALeft,
				ParentKey: keyA,
			},
			b: &RangeTreeNode{
				Key:       keyB,
				LeftKey:   keyBLeft,
				RightKey:  keyBRight,
				ParentKey: keyA,
				Black:     false,
			},
			bLeft: &RangeTreeNode{
				Key:       keyBLeft,
				ParentKey: keyB,
			},
			bRight: &RangeTreeNode{
				Key:       keyBRight,
				ParentKey: keyB,
			},
			rootExpected: keyRoot,
			aExpected: &RangeTreeNode{
				Key:       keyA,
				LeftKey:   keyBLeft,
				RightKey:  keyBRight,
				ParentKey: keyB,
				Black:     false,
			},
			aParentExpected: &RangeTreeNode{
				Key:      keyAParent,
				LeftKey:  keyAParentLeft,
				RightKey: keyB,
			},
			aLeftExpected: &RangeTreeNode{
				Key:       keyALeft,
				ParentKey: keyB,
			},
			bExpected: &RangeTreeNode{
				Key:       keyB,
				LeftKey:   keyALeft,
				RightKey:  keyA,
				ParentKey: keyAParent,
				Black:     true,
			},
			bLeftExpected: &RangeTreeNode{
				Key:       keyBLeft,
				ParentKey: keyA,
			},
			bRightExpected: &RangeTreeNode{
				Key:       keyBRight,
				ParentKey: keyA,
			},
		},
		// Test Case 3: b is a's left child, a is a left child
		{
			root: keyRoot,
			a: &RangeTreeNode{
				Key:       keyA,
				LeftKey:   keyB,
				RightKey:  keyARight,
				ParentKey: keyAParent,
				Black:     true,
			},
			aParent: &RangeTreeNode{
				Key:      keyAParent,
				LeftKey:  keyA,
				RightKey: keyAParentRight,
			},
			aRight: &RangeTreeNode{
				Key:       keyARight,
				ParentKey: keyA,
			},
			b: &RangeTreeNode{
				Key:       keyB,
				LeftKey:   keyBLeft,
				RightKey:  keyBRight,
				ParentKey: keyA,
				Black:     false,
			},
			bLeft: &RangeTreeNode{
				Key:       keyBLeft,
				ParentKey: keyB,
			},
			bRight: &RangeTreeNode{
				Key:       keyBRight,
				ParentKey: keyB,
			},
			rootExpected: keyRoot,
			aExpected: &RangeTreeNode{
				Key:       keyA,
				LeftKey:   keyBLeft,
				RightKey:  keyBRight,
				ParentKey: keyB,
				Black:     false,
			},
			aParentExpected: &RangeTreeNode{
				Key:      keyAParent,
				LeftKey:  keyB,
				RightKey: keyAParentRight,
			},
			aRightExpected: &RangeTreeNode{
				Key:       keyARight,
				ParentKey: keyB,
			},
			bExpected: &RangeTreeNode{
				Key:       keyB,
				LeftKey:   keyA,
				RightKey:  keyARight,
				ParentKey: keyAParent,
				Black:     true,
			},
			bLeftExpected: &RangeTreeNode{
				Key:       keyBLeft,
				ParentKey: keyA,
			},
			bRightExpected: &RangeTreeNode{
				Key:       keyBRight,
				ParentKey: keyA,
			},
		},
		// Test Case 4: b is a's right child, a is a left child
		{
			root: keyRoot,
			a: &RangeTreeNode{
				Key:       keyA,
				LeftKey:   keyALeft,
				RightKey:  keyB,
				ParentKey: keyAParent,
				Black:     true,
			},
			aParent: &RangeTreeNode{
				Key:      keyAParent,
				LeftKey:  keyA,
				RightKey: keyAParentRight,
			},
			aLeft: &RangeTreeNode{
				Key:       keyALeft,
				ParentKey: keyA,
			},
			b: &RangeTreeNode{
				Key:       keyB,
				LeftKey:   keyBLeft,
				RightKey:  keyBRight,
				ParentKey: keyA,
				Black:     false,
			},
			bLeft: &RangeTreeNode{
				Key:       keyBLeft,
				ParentKey: keyB,
			},
			bRight: &RangeTreeNode{
				Key:       keyBRight,
				ParentKey: keyB,
			},
			rootExpected: keyRoot,
			aExpected: &RangeTreeNode{
				Key:       keyA,
				LeftKey:   keyBLeft,
				RightKey:  keyBRight,
				ParentKey: keyB,
				Black:     false,
			},
			aParentExpected: &RangeTreeNode{
				Key:      keyAParent,
				LeftKey:  keyB,
				RightKey: keyAParentRight,
			},
			aLeftExpected: &RangeTreeNode{
				Key:       keyALeft,
				ParentKey: keyB,
			},
			bExpected: &RangeTreeNode{
				Key:       keyB,
				LeftKey:   keyALeft,
				RightKey:  keyA,
				ParentKey: keyAParent,
				Black:     true,
			},
			bLeftExpected: &RangeTreeNode{
				Key:       keyBLeft,
				ParentKey: keyA,
			},
			bRightExpected: &RangeTreeNode{
				Key:       keyBRight,
				ParentKey: keyA,
			},
		},
		// Test Case 5: b is a's left child, a is a right child
		{
			root: keyRoot,
			a: &RangeTreeNode{
				Key:       keyA,
				LeftKey:   keyB,
				RightKey:  keyARight,
				ParentKey: keyAParent,
				Black:     true,
			},
			aParent: &RangeTreeNode{
				Key:      keyAParent,
				LeftKey:  keyAParentLeft,
				RightKey: keyA,
			},
			aRight: &RangeTreeNode{
				Key:       keyARight,
				ParentKey: keyA,
			},
			b: &RangeTreeNode{
				Key:       keyB,
				LeftKey:   keyBLeft,
				RightKey:  keyBRight,
				ParentKey: keyA,
				Black:     false,
			},
			bLeft: &RangeTreeNode{
				Key:       keyBLeft,
				ParentKey: keyB,
			},
			bRight: &RangeTreeNode{
				Key:       keyBRight,
				ParentKey: keyB,
			},
			rootExpected: keyRoot,
			aExpected: &RangeTreeNode{
				Key:       keyA,
				LeftKey:   keyBLeft,
				RightKey:  keyBRight,
				ParentKey: keyB,
				Black:     false,
			},
			aParentExpected: &RangeTreeNode{
				Key:      keyAParent,
				LeftKey:  keyAParentLeft,
				RightKey: keyB,
			},
			aRightExpected: &RangeTreeNode{
				Key:       keyARight,
				ParentKey: keyB,
			},
			bExpected: &RangeTreeNode{
				Key:       keyB,
				LeftKey:   keyA,
				RightKey:  keyARight,
				ParentKey: keyAParent,
				Black:     true,
			},
			bLeftExpected: &RangeTreeNode{
				Key:       keyBLeft,
				ParentKey: keyA,
			},
			bRightExpected: &RangeTreeNode{
				Key:       keyBRight,
				ParentKey: keyA,
			},
		},
		// Test Case 6: a is root, b is unrelated to a
		{
			root: keyA,
			a: &RangeTreeNode{
				Key:      keyA,
				LeftKey:  keyALeft,
				RightKey: keyARight,
				Black:    true,
			},
			aLeft: &RangeTreeNode{
				Key:       keyALeft,
				ParentKey: keyA,
			},
			aRight: &RangeTreeNode{
				Key:       keyARight,
				ParentKey: keyA,
			},
			b: &RangeTreeNode{
				Key:       keyB,
				LeftKey:   keyBLeft,
				RightKey:  keyBRight,
				ParentKey: keyBParent,
				Black:     false,
			},
			bParent: &RangeTreeNode{
				Key:      keyBParent,
				LeftKey:  keyB,
				RightKey: keyBParentRight,
			},
			bLeft: &RangeTreeNode{
				Key:       keyBLeft,
				ParentKey: keyB,
			},
			bRight: &RangeTreeNode{
				Key:       keyBRight,
				ParentKey: keyB,
			},
			rootExpected: keyB,
			aExpected: &RangeTreeNode{
				Key:       keyA,
				LeftKey:   keyBLeft,
				RightKey:  keyBRight,
				ParentKey: keyBParent,
				Black:     false,
			},
			aLeftExpected: &RangeTreeNode{
				Key:       keyALeft,
				ParentKey: keyB,
			},
			aRightExpected: &RangeTreeNode{
				Key:       keyARight,
				ParentKey: keyB,
			},
			bExpected: &RangeTreeNode{
				Key:      keyB,
				LeftKey:  keyALeft,
				RightKey: keyARight,
				Black:    true,
			},
			bParentExpected: &RangeTreeNode{
				Key:      keyBParent,
				LeftKey:  keyA,
				RightKey: keyBParentRight,
			},
			bLeftExpected: &RangeTreeNode{
				Key:       keyBLeft,
				ParentKey: keyA,
			},
			bRightExpected: &RangeTreeNode{
				Key:       keyBRight,
				ParentKey: keyA,
			},
		},
		// Test Case 7: b is a's left child, a is a right child, a is root
		{
			root: keyA,
			a: &RangeTreeNode{
				Key:      keyA,
				LeftKey:  keyB,
				RightKey: keyARight,
				Black:    true,
			},
			aRight: &RangeTreeNode{
				Key:       keyARight,
				ParentKey: keyA,
			},
			b: &RangeTreeNode{
				Key:       keyB,
				LeftKey:   keyBLeft,
				ParentKey: keyA,
				Black:     false,
			},
			bLeft: &RangeTreeNode{
				Key:       keyBLeft,
				ParentKey: keyB,
			},
			rootExpected: keyB,
			aExpected: &RangeTreeNode{
				Key:       keyA,
				LeftKey:   keyBLeft,
				ParentKey: keyB,
				Black:     false,
			},
			aRightExpected: &RangeTreeNode{
				Key:       keyARight,
				ParentKey: keyB,
			},
			bExpected: &RangeTreeNode{
				Key:      keyB,
				LeftKey:  keyA,
				RightKey: keyARight,
				Black:    true,
			},
			bLeftExpected: &RangeTreeNode{
				Key:       keyBLeft,
				ParentKey: keyA,
			},
		},
	}

	for i, test := range testCases {
		tc := createTreeContext(test.root, []*RangeTreeNode{
			test.a,
			test.aRight,
			test.aLeft,
			test.aParent,
			test.b,
			test.bRight,
			test.bLeft,
			test.bParent,
		})

		// Perform the swap.
		updatedA, updatedB, err := tc.swapNodes(test.a, test.b)
		if err != nil {
			t.Fatal(err)
		}

		if e, a := test.rootExpected, tc.tree.RootKey; !e.Equal(a) {
			t.Errorf("%d: Expected root does not match actual.\nExpected: %+snActual: %s", i, e, a)
		}
		checkTreeNode(t, tc, i, "a", keyA, test.aExpected, updatedA)
		checkTreeNode(t, tc, i, "aParent", keyAParent, test.aParentExpected, nil)
		checkTreeNode(t, tc, i, "aLeft", keyALeft, test.aLeftExpected, nil)
		checkTreeNode(t, tc, i, "aRight", keyARight, test.aRightExpected, nil)
		checkTreeNode(t, tc, i, "b", keyB, test.bExpected, updatedB)
		checkTreeNode(t, tc, i, "bParent", keyBParent, test.bParentExpected, nil)
		checkTreeNode(t, tc, i, "bLeft", keyBLeft, test.bLeftExpected, nil)
		checkTreeNode(t, tc, i, "bRight", keyBRight, test.bRightExpected, nil)

		// Perform the swap again and expect to get the original values back.
		finalA, finalB, err := tc.swapNodes(updatedA, updatedB)
		if err != nil {
			t.Fatal(err)
		}

		if e, a := test.root, tc.tree.RootKey; !e.Equal(a) {
			t.Errorf("%d: Expected root does not match actual.\nExpected: %+snActual: %s", i, e, a)
		}
		checkTreeNode(t, tc, i, "a-reverse", keyA, test.a, finalA)
		checkTreeNode(t, tc, i, "aParent-reverse", keyAParent, test.aParent, nil)
		checkTreeNode(t, tc, i, "aLeft-reverse", keyALeft, test.aLeft, nil)
		checkTreeNode(t, tc, i, "aRight-reverse", keyARight, test.aRight, nil)
		checkTreeNode(t, tc, i, "b-reverse", keyB, test.b, finalB)
		checkTreeNode(t, tc, i, "bParent-reverse", keyBParent, test.bParent, nil)
		checkTreeNode(t, tc, i, "bLeft-reverse", keyBLeft, test.bLeft, nil)
		checkTreeNode(t, tc, i, "bRight-reverse", keyBRight, test.bRight, nil)
	}
}

// verifyTree checks to ensure that the tree is indeed balanced and a correct
// red-black tree. It does so by checking each of the red-black tree properties.
func verifyTree(t *testing.T, tc *treeContext, testName string) {
	root, err := tc.getNode(tc.tree.RootKey)
	if err != nil {
		t.Fatal(err)
	}

	verifyBinarySearchTree(t, tc, testName, root, roachpb.RKeyMin, roachpb.RKeyMax)
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
// greater than it. It recursively walks the tree to perform this same check.
func verifyBinarySearchTree(t *testing.T, tc *treeContext, testName string, node *RangeTreeNode, keyMin, keyMax roachpb.RKey) {
	if !node.Key.Less(keyMax) {
		t.Errorf("%s: Failed Property BST - The key %s is not less than %s.", testName, node.Key, keyMax)
	}
	if !keyMin.Less(node.Key) {
		t.Errorf("%s: Failed Property BST - The key %s is not greater than %s.", testName, node.Key, keyMin)
	}

	if node.LeftKey != nil {
		left, err := tc.getNode(node.LeftKey)
		if err != nil {
			t.Fatal(err)
		}
		verifyBinarySearchTree(t, tc, testName, left, keyMin, node.Key)
	}
	if node.RightKey != nil {
		right, err := tc.getNode(node.RightKey)
		if err != nil {
			t.Fatal(err)
		}
		verifyBinarySearchTree(t, tc, testName, right, node.Key, keyMax)
	}
}

// verifyProperty2 ensures that the root node is black.
func verifyProperty2(t *testing.T, tc *treeContext, testName string, root *RangeTreeNode) {
	if e, a := false, isRed(root); e != a {
		t.Errorf("%s: Failed Property 2 - The root node is not black.", testName)
	}
}

// verifyProperty4 ensures that the parent of every red node is black.
func verifyProperty4(t *testing.T, tc *treeContext, testName string, node *RangeTreeNode) {
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
func verifyProperty5(t *testing.T, tc *treeContext, testName string, node *RangeTreeNode, blackCount int, pathBlackCount *int) {
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

// TestTree tries both inserting nodes into and deleting node from the range
// tree. The tree is verified after each insert or delete.
func TestTree(t *testing.T) {
	defer leaktest.AfterTest(t)()

	keyRoot := roachpb.RKey("m")
	tc := createTreeContext(keyRoot, []*RangeTreeNode{
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
	keysInsert := []string{"f", "e", "d", "c", "b", "a",
		"g", "h", "i", "j", "k", "l",
		"s", "r", "q", "p", "o", "n",
		"t", "u", "v", "w", "x", "y",
		"z"}

	//keys := []string{"f", "e", "d", "z"}
	for _, key := range keysInsert {
		node := &RangeTreeNode{
			Key: roachpb.RKey(key),
		}
		err := tc.insert(node)
		if err != nil {
			t.Fatal(err)
		}
		verifyTree(t, tc, key)
	}

	// Try adding an already added key.
	node := &RangeTreeNode{
		Key: roachpb.RKey("z"),
	}
	if err := tc.insert(node); err == nil {
		t.Fatal("inserting an already existing key should fail")
	}
	verifyTree(t, tc, "z-repeat")

	// This order of keys is designed to stress the tree in different ways.
	// Specifically going through each of the delete case's paths. This order
	// was chosen experimentally.
	keysDelete := []string{"f", "e", "d", "c", "x",
		"g", "h", "i", "j", "k", "l",
		"z", "n", "o", "p", "q",
		"t", "u", "v", "w", "y",
		"r", "b", "a", "s"}

	for _, key := range keysDelete {
		{
			node, err := tc.getNode(roachpb.RKey(key))
			if err != nil {
				t.Fatal(err)
			}
			if err := tc.delete(node); err != nil {
				t.Fatal(err)
			}
		}
		{
			node, err := tc.getNode(roachpb.RKey(key))
			if err != nil {
				t.Fatal(err)
			}
			if node != nil {
				t.Fatalf("%s: node %s was not deleted", key, key)
			}
		}
		verifyTree(t, tc, key)
	}
}
