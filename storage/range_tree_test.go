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
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func createTreeContext(rootKey *proto.Key, nodes []*proto.RangeTreeNode) *treeContext {
	root := &proto.RangeTree{
		RootKey: *rootKey,
	}
	tc := &treeContext{
		tx:    nil,
		tree:  root,
		dirty: false,
		nodes: map[string]cachedNode{},
	}
	for _, node := range nodes {
		// We don't use setNode here to ensure dirty is false.
		tc.nodes[string(node.Key)] = cachedNode{
			node:  node,
			dirty: false,
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
		actual := isRed(node)
		if actual != test.expected {
			t.Errorf("%d: %+v expect %v; got %v", i, node, test.expected, actual)
		}
	}
}

// TestFlip ensures that flips function correctly.
func TestFlip(t *testing.T) {
	defer leaktest.AfterTest(t)
	testCases := []struct {
		nodeBlack          bool
		leftBlack          bool
		rightBlack         bool
		expectedNodeBlack  bool
		expectedLeftBlack  bool
		expectedRightBlack bool
	}{
		{true, true, true, false, false, false},
		{false, false, false, true, true, true},
		{true, false, true, false, true, false},
		{false, true, false, true, false, true},
	}
	keyNode := proto.Key("Node")
	keyLeft := proto.Key("Left")
	keyRight := proto.Key("Right")
	for i, test := range testCases {
		node := &proto.RangeTreeNode{
			Key:       keyNode,
			ParentKey: proto.KeyMin,
			Black:     test.nodeBlack,
			LeftKey:   &keyLeft,
			RightKey:  &keyRight,
		}
		left := &proto.RangeTreeNode{
			Key:       keyLeft,
			ParentKey: keyNode,
			Black:     test.leftBlack,
		}
		right := &proto.RangeTreeNode{
			Key:       keyRight,
			ParentKey: keyNode,
			Black:     test.rightBlack,
		}
		tc := createTreeContext(&keyNode, []*proto.RangeTreeNode{
			node,
			left,
			right,
		})

		// Perform the flip.
		actualNode, err := tc.flip(node)
		if err != nil {
			t.Fatal(err)
		}

		// Are all three nodes dirty?
		if !tc.nodes[string(keyNode)].dirty {
			t.Errorf("%d: Expected node to be dirty", i)
		}
		if !tc.nodes[string(keyLeft)].dirty {
			t.Errorf("%d: Expected left node to be dirty", i)
		}
		if !tc.nodes[string(keyRight)].dirty {
			t.Errorf("%d: Expected right node to be dirty", i)
		}

		// Is the Fetched Node the same as the retruend one?
		actualNodeFetched, err := tc.getNode(&keyNode)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(actualNodeFetched, actualNode) {
			t.Errorf("%d: Fetched Node is not the same as the returned one. Fetched: %+v Returned: %+v", i, actualNodeFetched, actualNode)
		}

		// Are the resulting nodes correctly flipped?
		if actualNode.Black != test.expectedNodeBlack {
			t.Errorf("%d: Expect node black to be %v, got %v", i, test.expectedNodeBlack, actualNode.Black)
		}
		actualLeft, err := tc.getNode(&keyLeft)
		if err != nil {
			t.Fatal(err)
		}
		if actualLeft.Black != test.expectedLeftBlack {
			t.Errorf("%d: Expect left node black to be %v, got %v", i, test.expectedLeftBlack, actualLeft.Black)
		}
		actualRight, err := tc.getNode(&keyRight)
		if err != nil {
			t.Fatal(err)
		}
		if actualRight.Black != test.expectedRightBlack {
			t.Errorf("%d: Expect right node black to be %v, got %v", i, test.expectedRightBlack, actualRight.Black)
		}
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

	testCases := []struct {
		node         *proto.RangeTreeNode
		left         *proto.RangeTreeNode
		expectedNode *proto.RangeTreeNode
		expectedLeft *proto.RangeTreeNode
		expectedErr  bool
	}{
		// Test Case 0: Normal Rotation, node is black.
		{
			node: &proto.RangeTreeNode{
				Key:       keyNode,
				ParentKey: proto.KeyMin,
				Black:     true,
				LeftKey:   &keyLeft,
				RightKey:  &keyRight,
			},
			left: &proto.RangeTreeNode{
				Key:       keyLeft,
				ParentKey: proto.KeyMin,
				Black:     false,
				LeftKey:   &keyLeftLeft,
				RightKey:  &keyLeftRight,
			},
			expectedNode: &proto.RangeTreeNode{
				Key:       keyLeft,
				ParentKey: proto.KeyMin,
				Black:     true,
				LeftKey:   &keyLeftLeft,
				RightKey:  &keyNode,
			},
			expectedLeft: &proto.RangeTreeNode{
				Key:       keyNode,
				ParentKey: proto.KeyMin,
				Black:     false,
				LeftKey:   &keyLeftRight,
				RightKey:  &keyRight,
			},
			expectedErr: false,
		},
		// Test Case 1: Normal Rotation, node is red.
		{
			node: &proto.RangeTreeNode{
				Key:       keyNode,
				ParentKey: proto.KeyMin,
				Black:     false,
				LeftKey:   &keyLeft,
				RightKey:  &keyRight,
			},
			left: &proto.RangeTreeNode{
				Key:       keyLeft,
				ParentKey: proto.KeyMin,
				Black:     false,
				LeftKey:   &keyLeftLeft,
				RightKey:  &keyLeftRight,
			},
			expectedNode: &proto.RangeTreeNode{
				Key:       keyLeft,
				ParentKey: proto.KeyMin,
				Black:     false,
				LeftKey:   &keyLeftLeft,
				RightKey:  &keyNode,
			},
			expectedLeft: &proto.RangeTreeNode{
				Key:       keyNode,
				ParentKey: proto.KeyMin,
				Black:     false,
				LeftKey:   &keyLeftRight,
				RightKey:  &keyRight,
			},
			expectedErr: false,
		},
		// Test Case 2: Failed, rotating a black node.
		{
			node: &proto.RangeTreeNode{
				Key:       keyNode,
				ParentKey: proto.KeyMin,
				Black:     false,
				LeftKey:   &keyLeft,
				RightKey:  &keyRight,
			},
			left: &proto.RangeTreeNode{
				Key:       keyLeft,
				ParentKey: proto.KeyMin,
				Black:     true,
				LeftKey:   &keyLeftLeft,
				RightKey:  &keyLeftRight,
			},
			expectedErr: true,
		},
		// Test Case 3: Normal Rotation, extraneous keys are nil.
		{
			node: &proto.RangeTreeNode{
				Key:       keyNode,
				ParentKey: proto.KeyMin,
				Black:     true,
				LeftKey:   &keyLeft,
			},
			left: &proto.RangeTreeNode{
				Key:       keyLeft,
				ParentKey: proto.KeyMin,
				Black:     false,
			},
			expectedNode: &proto.RangeTreeNode{
				Key:       keyLeft,
				ParentKey: proto.KeyMin,
				Black:     true,
				RightKey:  &keyNode,
			},
			expectedLeft: &proto.RangeTreeNode{
				Key:       keyNode,
				ParentKey: proto.KeyMin,
				Black:     false,
			},
			expectedErr: false,
		},
	}

	for i, test := range testCases {
		tc := createTreeContext(&keyNode, []*proto.RangeTreeNode{
			test.node,
			test.left,
		})

		// Perform the rotation.
		actualNode, err := tc.rotateRight(test.node)
		if err != nil {
			// Did we expect the error?
			if test.expectedErr {
				continue
			}
			t.Fatal(err)
		}

		// Are both nodes dirty?
		if !tc.nodes[string(keyNode)].dirty {
			t.Errorf("%d: Expected node to be dirty", i)
		}
		if !tc.nodes[string(keyLeft)].dirty {
			t.Errorf("%d: Expected left node to be dirty", i)
		}

		// Is the Fetched Node the same as the retruend one?
		actualNodeFetched, err := tc.getNode(&keyLeft)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(actualNodeFetched, actualNode) {
			t.Errorf("%d: Fetched Node is not the same as the returned one. Fetched: %+v Returned: %+v", i, actualNodeFetched, actualNode)
		}

		// Are the resulting nodes correctly rotated?
		if !reflect.DeepEqual(actualNode, test.expectedNode) {
			t.Errorf("%d: Expected Node is not the same as the actual.\nExpected: %+v\nActual: %+v\n", i, test.expectedNode, actualNode)
		}

		actualLeft, err := tc.getNode(&keyNode)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(actualLeft, test.expectedLeft) {
			t.Errorf("%d: Expected Left Node is not the same as the actual.\nExpected: %+v\nActual: %+v\n", i, test.expectedLeft, actualLeft)
		}
	}
}

// TestRotateLeft ensures that left rotations occur correctly.
func TestRotateLeft(t *testing.T) {
	defer leaktest.AfterTest(t)

	keyNode := proto.Key("N")
	keyRight := proto.Key("R")
	keyLeft := proto.Key("L")
	keyRightLeft := proto.Key("RL")
	keyRightRight := proto.Key("RR")

	testCases := []struct {
		node          *proto.RangeTreeNode
		right         *proto.RangeTreeNode
		expectedNode  *proto.RangeTreeNode
		expectedRight *proto.RangeTreeNode
		expectedErr   bool
	}{
		// Test Case 0: Normal Rotation, node is black.
		{
			node: &proto.RangeTreeNode{
				Key:       keyNode,
				ParentKey: proto.KeyMin,
				Black:     true,
				LeftKey:   &keyLeft,
				RightKey:  &keyRight,
			},
			right: &proto.RangeTreeNode{
				Key:       keyRight,
				ParentKey: proto.KeyMin,
				Black:     false,
				LeftKey:   &keyRightLeft,
				RightKey:  &keyRightRight,
			},
			expectedNode: &proto.RangeTreeNode{
				Key:       keyRight,
				ParentKey: proto.KeyMin,
				Black:     true,
				LeftKey:   &keyNode,
				RightKey:  &keyRightRight,
			},
			expectedRight: &proto.RangeTreeNode{
				Key:       keyNode,
				ParentKey: proto.KeyMin,
				Black:     false,
				LeftKey:   &keyLeft,
				RightKey:  &keyRightLeft,
			},
			expectedErr: false,
		},
		// Test Case 1: Normal Rotation, node is red.
		{
			node: &proto.RangeTreeNode{
				Key:       keyNode,
				ParentKey: proto.KeyMin,
				Black:     false,
				LeftKey:   &keyLeft,
				RightKey:  &keyRight,
			},
			right: &proto.RangeTreeNode{
				Key:       keyRight,
				ParentKey: proto.KeyMin,
				Black:     false,
				LeftKey:   &keyRightLeft,
				RightKey:  &keyRightRight,
			},
			expectedNode: &proto.RangeTreeNode{
				Key:       keyRight,
				ParentKey: proto.KeyMin,
				Black:     false,
				LeftKey:   &keyNode,
				RightKey:  &keyRightRight,
			},
			expectedRight: &proto.RangeTreeNode{
				Key:       keyNode,
				ParentKey: proto.KeyMin,
				Black:     false,
				LeftKey:   &keyLeft,
				RightKey:  &keyRightLeft,
			},
			expectedErr: false,
		},
		// Test Case 2: Failed, rotating a black node.
		{
			node: &proto.RangeTreeNode{
				Key:       keyNode,
				ParentKey: proto.KeyMin,
				Black:     true,
				LeftKey:   &keyLeft,
				RightKey:  &keyRight,
			},
			right: &proto.RangeTreeNode{
				Key:       keyRight,
				ParentKey: proto.KeyMin,
				Black:     true,
				LeftKey:   &keyRightLeft,
				RightKey:  &keyRightRight,
			},
			expectedErr: true,
		},
		// Test Case 3: Normal Rotation, extraneous keys are nil.
		{
			node: &proto.RangeTreeNode{
				Key:       keyNode,
				ParentKey: proto.KeyMin,
				Black:     true,
				RightKey:  &keyRight,
			},
			right: &proto.RangeTreeNode{
				Key:       keyRight,
				ParentKey: proto.KeyMin,
				Black:     false,
			},
			expectedNode: &proto.RangeTreeNode{
				Key:       keyRight,
				ParentKey: proto.KeyMin,
				Black:     true,
				LeftKey:   &keyNode,
			},
			expectedRight: &proto.RangeTreeNode{
				Key:       keyNode,
				ParentKey: proto.KeyMin,
				Black:     false,
			},
			expectedErr: false,
		},
	}

	for i, test := range testCases {
		tc := createTreeContext(&keyNode, []*proto.RangeTreeNode{
			test.node,
			test.right,
		})

		// Perform the rotation.
		actualNode, err := tc.rotateLeft(test.node)
		if err != nil {
			// Did we expect the error?
			if test.expectedErr {
				continue
			}
			t.Fatal(err)
		}

		// Are both nodes dirty?
		if !tc.nodes[string(keyNode)].dirty {
			t.Errorf("%d: Expected node to be dirty", i)
		}
		if !tc.nodes[string(keyRight)].dirty {
			t.Errorf("%d: Expected left node to be dirty", i)
		}

		// Is the Fetched Node the same as the retruend one?
		actualNodeFetched, err := tc.getNode(&keyRight)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(actualNodeFetched, actualNode) {
			t.Errorf("%d: Fetched Node is not the same as the returned one. Fetched: %+v Returned: %+v", i, actualNodeFetched, actualNode)
		}

		// Are the resulting nodes correctly rotated?
		if !reflect.DeepEqual(actualNode, test.expectedNode) {
			t.Errorf("%d: Expected Node is not the same as the actual.\nExpected: %+v\nActual: %+v\n", i, test.expectedNode, actualNode)
		}

		actualRight, err := tc.getNode(&keyNode)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(actualRight, test.expectedRight) {
			t.Errorf("%d: Expected Right Node is not the same as the actual.\nExpected: %+v\nActual: %+v\n", i, test.expectedRight, actualRight)
		}
	}
}
