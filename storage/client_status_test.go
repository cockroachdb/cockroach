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

package storage_test

import (
	"reflect"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	gogoproto "github.com/gogo/protobuf/proto"
)

// compareStoreStatus ensures that the actual store status for the passed in
// store is updated correctly. It checks that the Desc.StoreID, Desc.Attrs,
// Desc.Node, Desc.Capacity.Capacity, NodeID, RangeCount, ReplicatedRangeCount
// are exactly correct and that the bytes and counts for Live, Key and Val are
// at least the expected value.
// The latest actual stats are returned.
func compareStoreStatus(t *testing.T, store *storage.Store, expectedStoreStatus *proto.StoreStatus, testNumber int) *proto.StoreStatus {
	storeStatusKey := keys.StoreStatusKey(int32(store.Ident.StoreID))
	gArgs, gReply := getArgs(storeStatusKey, 1, store.Ident.StoreID)
	if err := store.ExecuteCmd(context.Background(), client.Call{Args: gArgs, Reply: gReply}); err != nil {
		t.Fatalf("%v: failure getting store status: %s", testNumber, err)
	}
	if gReply.Value == nil {
		t.Errorf("%v: could not find store status at: %s", testNumber, storeStatusKey)
	}
	storeStatus := &proto.StoreStatus{}
	if err := gogoproto.Unmarshal(gReply.Value.GetBytes(), storeStatus); err != nil {
		t.Fatalf("%v: could not unmarshal store status: %+v", testNumber, gReply)
	}

	// Values much match exactly.
	if expectedStoreStatus.Desc.StoreID != storeStatus.Desc.StoreID {
		t.Errorf("%v: actual Desc.StoreID does not match expected\nexpected: %+v\nactual: %v\n", testNumber, expectedStoreStatus, storeStatus)
	}
	if !reflect.DeepEqual(expectedStoreStatus.Desc.Attrs, storeStatus.Desc.Attrs) {
		t.Errorf("%v: actual Desc.Attrs does not match expected\nexpected: %+v\nactual: %v\n", testNumber, expectedStoreStatus, storeStatus)
	}
	if !reflect.DeepEqual(expectedStoreStatus.Desc.Node, storeStatus.Desc.Node) {
		t.Errorf("%v: actual Desc.Attrs does not match expected\nexpected: %+v\nactual: %v\n", testNumber, expectedStoreStatus, storeStatus)
	}
	if storeStatus.Desc.Capacity.Capacity != expectedStoreStatus.Desc.Capacity.Capacity {
		t.Errorf("%v: actual Desc.Capacity.Capacity does not match expected\nexpected: %+v\nactual: %v\n", testNumber, expectedStoreStatus, storeStatus)
	}
	if expectedStoreStatus.NodeID != storeStatus.NodeID {
		t.Errorf("%v: actual node ID does not match expected\nexpected: %+v\nactual: %v\n", testNumber, expectedStoreStatus, storeStatus)
	}
	if expectedStoreStatus.RangeCount != storeStatus.RangeCount {
		t.Errorf("%v: actual RangeCount does not match expected\nexpected: %+v\nactual: %v\n", testNumber, expectedStoreStatus, storeStatus)
	}
	if expectedStoreStatus.ReplicatedRangeCount != storeStatus.ReplicatedRangeCount {
		t.Errorf("%v: actual ReplicatedRangeCount does not match expected\nexpected: %+v\nactual: %v\n", testNumber, expectedStoreStatus, storeStatus)
	}

	// Values should be >= to expected values.
	if storeStatus.Stats.LiveBytes < expectedStoreStatus.Stats.LiveBytes {
		t.Errorf("%v: actual Live Bytes is not greater or equal to expected\nexpected: %+v\nactual: %v\n", testNumber, expectedStoreStatus, storeStatus)
	}
	if storeStatus.Stats.KeyBytes < expectedStoreStatus.Stats.KeyBytes {
		t.Errorf("%v: actual Key Bytes is not greater or equal to expected\nexpected: %+v\nactual: %v\n", testNumber, expectedStoreStatus, storeStatus)
	}
	if storeStatus.Stats.ValBytes < expectedStoreStatus.Stats.ValBytes {
		t.Errorf("%v: actual Val Bytes is not greater or equal to expected\nexpected: %+v\nactual: %v\n", testNumber, expectedStoreStatus, storeStatus)
	}
	if storeStatus.Stats.LiveCount < expectedStoreStatus.Stats.LiveCount {
		t.Errorf("%v: actual Live Count is not greater or equal to expected\nexpected: %+v\nactual: %v\n", testNumber, expectedStoreStatus, storeStatus)
	}
	if storeStatus.Stats.KeyCount < expectedStoreStatus.Stats.KeyCount {
		t.Errorf("%v: actual Key Count is not greater or equal to expected\nexpected: %+v\nactual: %v\n", testNumber, expectedStoreStatus, storeStatus)
	}
	if storeStatus.Stats.ValCount < expectedStoreStatus.Stats.ValCount {
		t.Errorf("%v: actual Val Count is not greater or equal to expected\nexpected: %+v\nactual: %v\n", testNumber, expectedStoreStatus, storeStatus)
	}
	return storeStatus
}

// TestStoreStatus checks the store status after each range scan to ensure that
// it is being updated correctly.
func TestStoreStatus(t *testing.T) {
	defer leaktest.AfterTest(t)
	ctx := &storage.TestStoreContext
	ctx.ScanInterval = time.Duration(10 * time.Millisecond)
	store, stopper := createTestStoreWithEngine(t, engine.NewInMem(proto.Attributes{}, 10<<20), hlc.NewClock(hlc.NewManualClock(0).UnixNano), true, ctx)
	defer stopper.Stop()
	splitKey := proto.Key("b")
	content := proto.Key("test content")

	storeDesc, err := store.Descriptor()
	if err != nil {
		t.Fatal(err)
	}

	store.WaitForInit()
	rng := store.LookupRange([]byte("a"), nil)
	// Perform a read from the range to ensure that the raft election has
	// completed.  We do not expect a value to be present.
	gArgs, gReply := getArgs([]byte("a"), rng.Desc().RaftID, store.StoreID())
	if err := store.ExecuteCmd(context.Background(), client.Call{Args: gArgs, Reply: gReply}); err != nil {
		t.Fatal(err)
	}

	expectedStoreStatus := &proto.StoreStatus{
		Desc:                 *storeDesc,
		NodeID:               1,
		RangeCount:           1,
		LeaderRangeCount:     1,
		AvailableRangeCount:  1,
		ReplicatedRangeCount: 0,
		Stats: proto.MVCCStats{
			LiveBytes: 1,
			KeyBytes:  1,
			ValBytes:  1,
			LiveCount: 1,
			KeyCount:  1,
			ValCount:  1,
		},
	}
	// Always wait twice, to ensure a full scan has occurred.
	store.WaitForRangeScanCompletion()
	store.WaitForRangeScanCompletion()
	oldstats := compareStoreStatus(t, store, expectedStoreStatus, 0)

	// Write some values left and right of the proposed split key.
	pArgs, pReply := putArgs([]byte("a"), content, rng.Desc().RaftID, store.StoreID())
	if err := store.ExecuteCmd(context.Background(), client.Call{Args: pArgs, Reply: pReply}); err != nil {
		t.Fatal(err)
	}
	pArgs, pReply = putArgs([]byte("c"), content, rng.Desc().RaftID, store.StoreID())
	if err := store.ExecuteCmd(context.Background(), client.Call{Args: pArgs, Reply: pReply}); err != nil {
		t.Fatal(err)
	}

	expectedStoreStatus = &proto.StoreStatus{
		Desc:                 oldstats.Desc,
		NodeID:               1,
		RangeCount:           1,
		LeaderRangeCount:     1,
		AvailableRangeCount:  1,
		ReplicatedRangeCount: 0,
		Stats: proto.MVCCStats{
			LiveBytes: 1,
			KeyBytes:  1,
			ValBytes:  1,
			LiveCount: oldstats.Stats.LiveCount + 1,
			KeyCount:  oldstats.Stats.KeyCount + 1,
			ValCount:  oldstats.Stats.ValCount + 1,
		},
	}
	store.WaitForRangeScanCompletion()
	store.WaitForRangeScanCompletion()
	oldstats = compareStoreStatus(t, store, expectedStoreStatus, 1)

	// Split the range.
	args, reply := adminSplitArgs(proto.KeyMin, splitKey, rng.Desc().RaftID, store.StoreID())
	if err := store.ExecuteCmd(context.Background(), client.Call{Args: args, Reply: reply}); err != nil {
		t.Fatal(err)
	}

	expectedStoreStatus = &proto.StoreStatus{
		Desc:                 oldstats.Desc,
		NodeID:               1,
		RangeCount:           2,
		LeaderRangeCount:     2,
		AvailableRangeCount:  2,
		ReplicatedRangeCount: 0,
		Stats: proto.MVCCStats{
			LiveBytes: 1,
			KeyBytes:  1,
			ValBytes:  1,
			LiveCount: oldstats.Stats.LiveCount,
			KeyCount:  oldstats.Stats.KeyCount,
			ValCount:  oldstats.Stats.ValCount,
		},
	}
	store.WaitForRangeScanCompletion()
	store.WaitForRangeScanCompletion()
	oldstats = compareStoreStatus(t, store, expectedStoreStatus, 2)

	// Write some values left and right of the split key.
	rng = store.LookupRange([]byte("aa"), nil)
	pArgs, pReply = putArgs([]byte("aa"), content, rng.Desc().RaftID, store.StoreID())
	if err := store.ExecuteCmd(context.Background(), client.Call{Args: pArgs, Reply: pReply}); err != nil {
		t.Fatal(err)
	}
	rng2 := store.LookupRange([]byte("cc"), nil)
	pArgs, pReply = putArgs([]byte("cc"), content, rng2.Desc().RaftID, store.StoreID())
	if err := store.ExecuteCmd(context.Background(), client.Call{Args: pArgs, Reply: pReply}); err != nil {
		t.Fatal(err)
	}

	expectedStoreStatus = &proto.StoreStatus{
		Desc:                 oldstats.Desc,
		NodeID:               1,
		RangeCount:           2,
		LeaderRangeCount:     2,
		AvailableRangeCount:  2,
		ReplicatedRangeCount: 0,
		Stats: proto.MVCCStats{
			LiveBytes: 1,
			KeyBytes:  1,
			ValBytes:  1,
			LiveCount: oldstats.Stats.LiveCount + 1,
			KeyCount:  oldstats.Stats.KeyCount + 1,
			ValCount:  oldstats.Stats.ValCount + 1,
		},
	}
	store.WaitForRangeScanCompletion()
	store.WaitForRangeScanCompletion()
	compareStoreStatus(t, store, expectedStoreStatus, 3)
}
