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

package storage_test

import (
	"bytes"
	"reflect"
	"strconv"
	"testing"

	"github.com/biogo/store/llrb"
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
	gogoproto "github.com/gogo/protobuf/proto"
)

// TODO(bram): Increase TotalSplits drastically if performance allows.
const TotalSplits = 10

type testRangeTree struct {
	Tree  proto.RangeTree
	Nodes map[string]proto.RangeTreeNode
}

type Key proto.Key

// nodesEqual is a replacement for reflect.DeepEqual as it was having issues
// determining similarities between the types.
func nodesEqual(key proto.Key, expected, actual proto.RangeTreeNode) error {
	// Check that Key is equal.
	if !expected.Key.Equal(actual.Key) {
		return util.Errorf("Range tree node's Key is not as expected for range:%s\nexpected:%+v\nactual:%+v", key, expected, actual)
	}
	// Check that Black are equal.
	if expected.Black != actual.Black {
		return util.Errorf("Range tree node's Black is not as expected for range:%s\nexpected:%+v\nactual:%+v", key, expected, actual)
	}
	// Check that LeftKey are equal.
	if (expected.LeftKey == nil) || (actual.LeftKey == nil) {
		if !((expected.LeftKey == nil) && (actual.LeftKey == nil)) {
			return util.Errorf("Range tree node's LeftKey is not as expected for range:%s\nexpected:%+v\nactual:%+v", key, expected, actual)
		}
	} else if !(*expected.LeftKey).Equal(*actual.LeftKey) {
		return util.Errorf("Range tree node's LeftKey is not as expected for range:%s\nexpected:%+v\nactual:%+v", key, expected, actual)
	}
	// Check that RightKey are equal.
	if (expected.RightKey == nil) || (actual.RightKey == nil) {
		if !((expected.RightKey == nil) && (actual.RightKey == nil)) {
			return util.Errorf("Range tree node's RightKey is not as expected for range:%s\nexpected:%+v\nactual:%+v", key, expected, actual)
		}
	} else if !(*expected.RightKey).Equal(*actual.RightKey) {
		return util.Errorf("Range tree node's RightKey is not as expected for range:%s\nexpected:%+v\nactual:%+v", key, expected, actual)
	}
	// Check that ParentKey are equal.
	if !expected.ParentKey.Equal(actual.ParentKey) {
		return util.Errorf("Range tree node's LeftKey is not as expected for range:%s\nexpected:%+v\nactual:%+v", key, expected, actual)
	}
	return nil
}

func getProto(kv *client.KV, key proto.Key, msg gogoproto.Message) error {
	call := client.GetCall(key)
	if err := kv.Run(call); err != nil {
		return err
	}
	reply := call.Reply.(*proto.GetResponse)
	if reply.Value == nil {
		return util.Errorf("%s: no value present", key)
	}
	if reply.Value.Integer != nil {
		return util.Errorf("%s: unexpected integer value: %+v", key, reply.Value)
	}
	return gogoproto.Unmarshal(reply.Value.Bytes, msg)
}

// treeNodesEqual compares the expectedTree from the provided key to the actual
// nodes retrieved from the db.  It recursively calls itself on both left and
// right children if they exist.
func treeNodesEqual(db *client.KV, expected testRangeTree, key proto.Key) error {
	expectedNode, ok := expected.Nodes[string(key)]
	if !ok {
		return util.Errorf("Expected does not contain a node for %s", key)
	}
	actualNode := &proto.RangeTreeNode{}
	if err := getProto(db, engine.RangeTreeNodeKey(key), actualNode); err != nil {
		return err
	}
	if err := nodesEqual(key, expectedNode, *actualNode); err != nil {
		return err
	}
	if expectedNode.LeftKey != nil {
		if err := treeNodesEqual(db, expected, *expectedNode.LeftKey); err != nil {
			return err
		}
	}
	if expectedNode.RightKey != nil {
		if err := treeNodesEqual(db, expected, *expectedNode.RightKey); err != nil {
			return err
		}
	}
	return nil
}

// treesEqual compares the expectedTree and expectedNodes to the actual range
// tree stored in the db.
func treesEqual(db *client.KV, expected testRangeTree) error {
	// Compare the tree roots.
	actualTree := &proto.RangeTree{}
	if err := getProto(db, engine.KeyRangeTreeRoot, actualTree); err != nil {
		return err
	}
	if !reflect.DeepEqual(&expected.Tree, actualTree) {
		return util.Errorf("Range tree root is not as expected - expected:%+v - actual:%+v", expected.Tree, actualTree)
	}

	return treeNodesEqual(db, expected, expected.Tree.RootKey)
}

// splitRange splits whichever range contains the key on that key.
func splitRange(db *client.KV, key proto.Key) error {
	req := &proto.AdminSplitRequest{
		RequestHeader: proto.RequestHeader{
			Key: key,
		},
		SplitKey: key,
	}
	resp := &proto.AdminSplitResponse{}
	return db.Run(client.Call{Args: req, Reply: resp})
}

// TestSetupRangeTree ensures that SetupRangeTree correctly setups up the range
// tree and first node.  SetupRangeTree is called via store.BootstrapRange.
func TestSetupRangeTree(t *testing.T) {
	defer leaktest.AfterTest(t)
	store, stopper := createTestStore(t)
	defer stopper.Stop()

	// Check to make sure the range tree is stored correctly.
	tree := proto.RangeTree{
		RootKey: engine.KeyMin,
	}
	nodes := map[string]proto.RangeTreeNode{
		string(engine.KeyMin): proto.RangeTreeNode{
			Key:       engine.KeyMin,
			ParentKey: engine.KeyMin,
			Black:     true,
		},
	}
	expectedTree := testRangeTree{
		Tree:  tree,
		Nodes: nodes,
	}
	if err := treesEqual(store.DB(), expectedTree); err != nil {
		t.Fatal(err)
	}
}

// TestInsertRight tests inserting a collection of 5 nodes, forcing left
// rotations and flips.
func TestInsertRight(t *testing.T) {
	defer leaktest.AfterTest(t)
	store, stopper := createTestStore(t)
	defer stopper.Stop()
	db := store.DB()

	keyA := proto.Key("a")
	keyB := proto.Key("b")
	keyC := proto.Key("c")
	keyD := proto.Key("d")
	keyE := proto.Key("e")

	// Test single split (with a left rotation).
	tree := proto.RangeTree{
		RootKey: keyA,
	}
	nodes := map[string]proto.RangeTreeNode{
		string(keyA): proto.RangeTreeNode{
			Key:       keyA,
			ParentKey: engine.KeyMin,
			Black:     true,
			LeftKey:   &engine.KeyMin,
		},
		string(engine.KeyMin): proto.RangeTreeNode{
			Key:       engine.KeyMin,
			ParentKey: engine.KeyMin,
			Black:     false,
		},
	}
	expectedTree := testRangeTree{
		Tree:  tree,
		Nodes: nodes,
	}
	if err := splitRange(db, keyA); err != nil {
		t.Fatal(err)
	}
	if err := treesEqual(db, expectedTree); err != nil {
		t.Fatal(err)
	}

	// Test two splits (with a flip).
	tree = proto.RangeTree{
		RootKey: keyA,
	}
	nodes = map[string]proto.RangeTreeNode{
		string(keyA): proto.RangeTreeNode{
			Key:       keyA,
			ParentKey: engine.KeyMin,
			Black:     true,
			LeftKey:   &engine.KeyMin,
			RightKey:  &keyB,
		},
		string(engine.KeyMin): proto.RangeTreeNode{
			Key:       engine.KeyMin,
			ParentKey: engine.KeyMin,
			Black:     true,
		},
		string(keyB): proto.RangeTreeNode{
			Key:       keyB,
			ParentKey: engine.KeyMin,
			Black:     true,
		},
	}
	expectedTree = testRangeTree{
		Tree:  tree,
		Nodes: nodes,
	}
	if err := splitRange(db, keyB); err != nil {
		t.Fatal(err)
	}
	if err := treesEqual(db, expectedTree); err != nil {
		t.Fatal(err)
	}

	// Test three splits (with a left rotation).
	tree = proto.RangeTree{
		RootKey: keyA,
	}
	nodes = map[string]proto.RangeTreeNode{
		string(keyA): proto.RangeTreeNode{
			Key:       keyA,
			ParentKey: engine.KeyMin,
			Black:     true,
			LeftKey:   &engine.KeyMin,
			RightKey:  &keyC,
		},
		string(engine.KeyMin): proto.RangeTreeNode{
			Key:       engine.KeyMin,
			ParentKey: engine.KeyMin,
			Black:     true,
		},
		string(keyC): proto.RangeTreeNode{
			Key:       keyC,
			ParentKey: engine.KeyMin,
			Black:     true,
			LeftKey:   &keyB,
		},
		string(keyB): proto.RangeTreeNode{
			Key:       keyB,
			ParentKey: engine.KeyMin,
			Black:     false,
		},
	}
	expectedTree = testRangeTree{
		Tree:  tree,
		Nodes: nodes,
	}
	if err := splitRange(db, keyC); err != nil {
		t.Fatal(err)
	}
	if err := treesEqual(db, expectedTree); err != nil {
		t.Fatal(err)
	}

	// Test four splits (with a flip and left rotation).
	tree = proto.RangeTree{
		RootKey: keyC,
	}
	nodes = map[string]proto.RangeTreeNode{
		string(keyC): proto.RangeTreeNode{
			Key:       keyC,
			ParentKey: engine.KeyMin,
			Black:     true,
			LeftKey:   &keyA,
			RightKey:  &keyD,
		},
		string(keyA): proto.RangeTreeNode{
			Key:       keyA,
			ParentKey: engine.KeyMin,
			Black:     false,
			LeftKey:   &engine.KeyMin,
			RightKey:  &keyB,
		},
		string(engine.KeyMin): proto.RangeTreeNode{
			Key:       engine.KeyMin,
			ParentKey: engine.KeyMin,
			Black:     true,
		},
		string(keyB): proto.RangeTreeNode{
			Key:       keyB,
			ParentKey: engine.KeyMin,
			Black:     true,
		},
		string(keyD): proto.RangeTreeNode{
			Key:       keyD,
			ParentKey: engine.KeyMin,
			Black:     true,
		},
	}
	expectedTree = testRangeTree{
		Tree:  tree,
		Nodes: nodes,
	}
	if err := splitRange(db, keyD); err != nil {
		t.Fatal(err)
	}
	if err := treesEqual(db, expectedTree); err != nil {
		t.Fatal(err)
	}

	// Test four splits (with a left rotation).
	tree = proto.RangeTree{
		RootKey: keyC,
	}
	nodes = map[string]proto.RangeTreeNode{
		string(keyC): proto.RangeTreeNode{
			Key:       keyC,
			ParentKey: engine.KeyMin,
			Black:     true,
			LeftKey:   &keyA,
			RightKey:  &keyE,
		},
		string(keyA): proto.RangeTreeNode{
			Key:       keyA,
			ParentKey: engine.KeyMin,
			Black:     false,
			LeftKey:   &engine.KeyMin,
			RightKey:  &keyB,
		},
		string(engine.KeyMin): proto.RangeTreeNode{
			Key:       engine.KeyMin,
			ParentKey: engine.KeyMin,
			Black:     true,
		},
		string(keyB): proto.RangeTreeNode{
			Key:       keyB,
			ParentKey: engine.KeyMin,
			Black:     true,
		},
		string(keyE): proto.RangeTreeNode{
			Key:       keyE,
			ParentKey: engine.KeyMin,
			Black:     true,
			LeftKey:   &keyD,
		},
		string(keyD): proto.RangeTreeNode{
			Key:       keyD,
			ParentKey: engine.KeyMin,
			Black:     false,
		},
	}
	expectedTree = testRangeTree{
		Tree:  tree,
		Nodes: nodes,
	}
	if err := splitRange(db, keyE); err != nil {
		t.Fatal(err)
	}
	if err := treesEqual(db, expectedTree); err != nil {
		t.Fatal(err)
	}
}

// TestInsertLeft tests inserting a collection of 5 nodes, forcing right
// rotations, left rotations and flips.
func TestInsertLeft(t *testing.T) {
	defer leaktest.AfterTest(t)
	store, stopper := createTestStore(t)
	defer stopper.Stop()
	db := store.DB()

	keyE := proto.Key("e")
	keyD := proto.Key("d")
	keyC := proto.Key("c")
	keyB := proto.Key("b")
	keyA := proto.Key("a")

	// Test single split (with a left rotation).
	tree := proto.RangeTree{
		RootKey: keyE,
	}
	nodes := map[string]proto.RangeTreeNode{
		string(keyE): proto.RangeTreeNode{
			Key:       keyE,
			ParentKey: engine.KeyMin,
			Black:     true,
			LeftKey:   &engine.KeyMin,
		},
		string(engine.KeyMin): proto.RangeTreeNode{
			Key:       engine.KeyMin,
			ParentKey: engine.KeyMin,
			Black:     false,
		},
	}
	expectedTree := testRangeTree{
		Tree:  tree,
		Nodes: nodes,
	}
	if err := splitRange(db, keyE); err != nil {
		t.Fatal(err)
	}
	if err := treesEqual(db, expectedTree); err != nil {
		t.Fatal(err)
	}

	// Test two splits (with a left, right and flip).
	tree = proto.RangeTree{
		RootKey: keyD,
	}
	nodes = map[string]proto.RangeTreeNode{
		string(keyD): proto.RangeTreeNode{
			Key:       keyD,
			ParentKey: engine.KeyMin,
			Black:     true,
			LeftKey:   &engine.KeyMin,
			RightKey:  &keyE,
		},
		string(engine.KeyMin): proto.RangeTreeNode{
			Key:       engine.KeyMin,
			ParentKey: engine.KeyMin,
			Black:     true,
		},
		string(keyE): proto.RangeTreeNode{
			Key:       keyE,
			ParentKey: engine.KeyMin,
			Black:     true,
		},
	}
	expectedTree = testRangeTree{
		Tree:  tree,
		Nodes: nodes,
	}
	if err := splitRange(db, keyD); err != nil {
		t.Fatal(err)
	}
	if err := treesEqual(db, expectedTree); err != nil {
		t.Fatal(err)
	}

	// Test three splits (with a left rotation).
	tree = proto.RangeTree{
		RootKey: keyD,
	}
	nodes = map[string]proto.RangeTreeNode{
		string(keyD): proto.RangeTreeNode{
			Key:       keyD,
			ParentKey: engine.KeyMin,
			Black:     true,
			LeftKey:   &keyC,
			RightKey:  &keyE,
		},
		string(keyC): proto.RangeTreeNode{
			Key:       keyC,
			ParentKey: engine.KeyMin,
			Black:     true,
			LeftKey:   &engine.KeyMin,
		},
		string(engine.KeyMin): proto.RangeTreeNode{
			Key:       engine.KeyMin,
			ParentKey: engine.KeyMin,
			Black:     false,
		},

		string(keyE): proto.RangeTreeNode{
			Key:       keyE,
			ParentKey: engine.KeyMin,
			Black:     true,
		},
	}
	expectedTree = testRangeTree{
		Tree:  tree,
		Nodes: nodes,
	}
	if err := splitRange(db, keyC); err != nil {
		t.Fatal(err)
	}
	if err := treesEqual(db, expectedTree); err != nil {
		t.Fatal(err)
	}

	// Test four splits (with a flip and a right and left rotation).
	tree = proto.RangeTree{
		RootKey: keyD,
	}
	nodes = map[string]proto.RangeTreeNode{
		string(keyD): proto.RangeTreeNode{
			Key:       keyD,
			ParentKey: engine.KeyMin,
			Black:     true,
			LeftKey:   &keyB,
			RightKey:  &keyE,
		},
		string(keyB): proto.RangeTreeNode{
			Key:       keyB,
			ParentKey: engine.KeyMin,
			Black:     false,
			LeftKey:   &engine.KeyMin,
			RightKey:  &keyC,
		},
		string(engine.KeyMin): proto.RangeTreeNode{
			Key:       engine.KeyMin,
			ParentKey: engine.KeyMin,
			Black:     true,
		},
		string(keyC): proto.RangeTreeNode{
			Key:       keyC,
			ParentKey: engine.KeyMin,
			Black:     true,
		},
		string(keyE): proto.RangeTreeNode{
			Key:       keyE,
			ParentKey: engine.KeyMin,
			Black:     true,
		},
	}
	expectedTree = testRangeTree{
		Tree:  tree,
		Nodes: nodes,
	}
	if err := splitRange(db, keyB); err != nil {
		t.Fatal(err)
	}
	if err := treesEqual(db, expectedTree); err != nil {
		t.Fatal(err)
	}

	// Test four splits (with a left rotation).
	tree = proto.RangeTree{
		RootKey: keyD,
	}
	nodes = map[string]proto.RangeTreeNode{
		string(keyD): proto.RangeTreeNode{
			Key:       keyD,
			ParentKey: engine.KeyMin,
			Black:     true,
			LeftKey:   &keyB,
			RightKey:  &keyE,
		},
		string(keyB): proto.RangeTreeNode{
			Key:       keyB,
			ParentKey: engine.KeyMin,
			Black:     false,
			LeftKey:   &keyA,
			RightKey:  &keyC,
		},
		string(keyA): proto.RangeTreeNode{
			Key:       keyA,
			ParentKey: engine.KeyMin,
			Black:     true,
			LeftKey:   &engine.KeyMin,
		},
		string(engine.KeyMin): proto.RangeTreeNode{
			Key:       engine.KeyMin,
			ParentKey: engine.KeyMin,
			Black:     false,
		},
		string(keyC): proto.RangeTreeNode{
			Key:       keyC,
			ParentKey: engine.KeyMin,
			Black:     true,
		},
		string(keyE): proto.RangeTreeNode{
			Key:       keyE,
			ParentKey: engine.KeyMin,
			Black:     true,
		},
	}
	expectedTree = testRangeTree{
		Tree:  tree,
		Nodes: nodes,
	}
	if err := splitRange(db, keyA); err != nil {
		t.Fatal(err)
	}
	if err := treesEqual(db, expectedTree); err != nil {
		t.Fatal(err)
	}
}

// Compare implements the llrbComparable interface for keys.
func (k Key) Compare(b llrb.Comparable) int {
	return bytes.Compare(k, b.(Key))
}

// compareBiogoNode compares a biogo node and a range tree node to determine if both
// contain the same values in the same order.  It recursively calls itself on
// both children if they exist.
func compareBiogoNode(db *client.KV, biogoNode *llrb.Node, key *proto.Key) error {
	// Retrieve the node form the range tree.
	rtNode := &proto.RangeTreeNode{}
	if err := getProto(db, engine.RangeTreeNodeKey(*key), rtNode); err != nil {
		return err
	}

	bNode := &proto.RangeTreeNode{
		Key:       proto.Key(biogoNode.Elem.(Key)),
		ParentKey: engine.KeyMin,
		Black:     bool(biogoNode.Color),
	}
	if biogoNode.Left != nil {
		leftKey := proto.Key(biogoNode.Left.Elem.(Key))
		bNode.LeftKey = &leftKey
	}
	if biogoNode.Right != nil {
		rightKey := proto.Key(biogoNode.Right.Elem.(Key))
		bNode.RightKey = &rightKey
	}
	if err := nodesEqual(*key, *bNode, *rtNode); err != nil {
		return err
	}
	if rtNode.LeftKey != nil {
		if err := compareBiogoNode(db, biogoNode.Left, rtNode.LeftKey); err != nil {
			return err
		}
	}
	if rtNode.RightKey != nil {
		if err := compareBiogoNode(db, biogoNode.Right, rtNode.RightKey); err != nil {
			return err
		}
	}
	return nil
}

// compareBiogoTree walks both a biogo tree and the range tree to determine if both
// contain the same values in the same order.
func compareBiogoTree(db *client.KV, biogoTree *llrb.Tree) error {
	rt := &proto.RangeTree{}
	if err := getProto(db, engine.KeyRangeTreeRoot, rt); err != nil {
		return err
	}
	return compareBiogoNode(db, biogoTree.Root, &rt.RootKey)
}

// TestRandomSplits splits the keyspace a total of TotalSplits number of times.
// At the same time, a biogo LLRB tree is also maintained and at the end of the
// test, the range tree and the biogo tree are compared to ensure they are
// equal.
func TestRandomSplits(t *testing.T) {
	defer leaktest.AfterTest(t)
	store, stopper := createTestStore(t)
	defer stopper.Stop()
	db := store.DB()
	rng, seed := util.NewPseudoRand()
	t.Logf("using pseudo random number generator with seed %d", seed)

	tree := &llrb.Tree{}
	tree.Insert(Key(engine.KeyMin))

	// Test an unsplit tree.
	if err := compareBiogoTree(db, tree); err != nil {
		t.Fatalf("Unsplit trees are not equal:%v", err)
	}

	for i := 0; i < TotalSplits; i++ {
		keyInt := rng.Int31()
		keyString := strconv.Itoa(int(keyInt))
		keyProto := proto.Key(keyString)
		key := Key(keyProto)
		// Make sure we avoid collisions.
		for tree.Get(key) != nil {
			keyInt = rng.Int31()
			keyString = strconv.Itoa(int(keyInt))
			keyProto = proto.Key(keyString)
			key = Key(keyProto)
		}

		//t.Logf("Inserting %d:%d", i, keyInt)
		tree.Insert(key)

		// Split the range.
		if err := splitRange(db, keyProto); err != nil {
			t.Fatal(err)
		}
	}

	// Compare the trees
	if err := compareBiogoTree(db, tree); err != nil {
		t.Fatal(err)
	}
}
