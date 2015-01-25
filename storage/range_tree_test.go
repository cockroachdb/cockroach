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
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	gogoproto "github.com/gogo/protobuf/proto"
)

// TestSetupRangeTree ensures that SetupRangeTree correctly setups up the range tree and first node.
// SetupRangeTree is called via store.BootstrapRange.
func TestSetupRangeTree(t *testing.T) {
	store, clock := createTestStoreWithClock(t)
	defer store.Stop()

	// Check to make sure the range tree is stored correctly.
	expectedTree := &proto.RangeTree{
		RootID: 1,
	}
	treeArgs, treeReply := getArgs(engine.KeyRangeTreeRoot, 1, store.StoreID())
	if err := store.ExecuteCmd(proto.Get, treeArgs, treeReply); err != nil {
		t.Fatal(err)
	}
	actualTree := &proto.RangeTree{}
	if err := gogoproto.Unmarshal(treeReply.Value.Bytes, actualTree); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(expectedTree, actualTree) {
		t.Errorf("expected range tree and actual range tree are not equal - expected:%s actual:%s", expectedTree, actualTree)
	}

	// Check to make sure the first range tree node is stored correctly.
	expectedNode := &proto.RangeTreeNode{
		RaftID: 1,
		Black:  true,
	}
	// To read the local key, we need to use MVCCGetProto.
	actualNode := &proto.RangeTreeNode{}
	ok, err := engine.MVCCGetProto(store.Engine(), engine.RangeTreeNodeKey(1), clock.Now(), nil, actualNode)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Errorf("Could not find the first range's node:%s", engine.RangeTreeNodeKey(1))
	}
	if !reflect.DeepEqual(expectedNode, actualNode) {
		t.Errorf("expected range tree node and actual range tree node are not equal - expected:%s actual:%s", expectedNode, actualNode)
	}
}
