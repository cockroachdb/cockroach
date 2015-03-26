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
	"testing"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
)

func adminMergeArgs(key []byte, subsumedRangeDesc proto.RangeDescriptor, raftID int64, storeID proto.StoreID) (*proto.AdminMergeRequest, *proto.AdminMergeResponse) {
	args := &proto.AdminMergeRequest{
		RequestHeader: proto.RequestHeader{
			Key:     key,
			RaftID:  raftID,
			Replica: proto.Replica{StoreID: storeID},
		},
		SubsumedRange: subsumedRangeDesc,
	}
	reply := &proto.AdminMergeResponse{}
	return args, reply
}

func createSplitRanges(store *storage.Store) (*proto.RangeDescriptor, *proto.RangeDescriptor, error) {
	args, reply := adminSplitArgs(engine.KeyMin, []byte("b"), 1, store.StoreID())
	if err := store.ExecuteCmd(proto.AdminSplit, args, reply); err != nil {
		return nil, nil, err
	}

	rangeA := store.LookupRange([]byte("a"), nil)
	rangeB := store.LookupRange([]byte("c"), nil)

	if bytes.Equal(rangeA.Desc().StartKey, rangeB.Desc().StartKey) {
		log.Errorf("split ranges keys are equal %q!=%q", rangeA.Desc().StartKey, rangeB.Desc().StartKey)
	}

	return rangeA.Desc(), rangeB.Desc(), nil
}

// TestStoreRangeMergeTwoEmptyRanges tries to merge two empty ranges
// together.
func TestStoreRangeMergeTwoEmptyRanges(t *testing.T) {
	defer leaktest.AfterTest(t)
	store := createTestStore(t)
	defer store.Stop()

	_, bDesc, err := createSplitRanges(store)
	if err != nil {
		t.Fatal(err)
	}

	// Merge the b range back into the a range.
	args, reply := adminMergeArgs(engine.KeyMin, *bDesc, 1, store.StoreID())
	err = store.ExecuteCmd(proto.AdminMerge, args, reply)
	if err != nil {
		t.Fatal(err)
	}

	// Verify the merge by looking up keys from both ranges.
	rangeA := store.LookupRange([]byte("a"), nil)
	rangeB := store.LookupRange([]byte("c"), nil)

	if !reflect.DeepEqual(rangeA, rangeB) {
		t.Fatalf("ranges were not merged %+v=%+v", rangeA.Desc(), rangeB.Desc())
	}
}

// TestStoreRangeMergeWithData attempts to merge two collocate ranges
// each containing data.
func TestStoreRangeMergeWithData(t *testing.T) {
	defer leaktest.AfterTest(t)
	content := proto.Key("testing!")

	store := createTestStore(t)
	defer store.Stop()

	aDesc, bDesc, err := createSplitRanges(store)
	if err != nil {
		t.Fatal(err)
	}

	// Write some values left and right of the proposed split key.
	pArgs, pReply := putArgs([]byte("aaa"), content, aDesc.RaftID, store.StoreID())
	if err := store.ExecuteCmd(proto.Put, pArgs, pReply); err != nil {
		t.Fatal(err)
	}
	pArgs, pReply = putArgs([]byte("ccc"), content, bDesc.RaftID, store.StoreID())
	if err := store.ExecuteCmd(proto.Put, pArgs, pReply); err != nil {
		t.Fatal(err)
	}

	// Confirm the values are there.
	gArgs, gReply := getArgs([]byte("aaa"), aDesc.RaftID, store.StoreID())
	if err := store.ExecuteCmd(proto.Get, gArgs, gReply); err != nil ||
		!bytes.Equal(gReply.Value.Bytes, content) {
		t.Fatal(err)
	}
	gArgs, gReply = getArgs([]byte("ccc"), bDesc.RaftID, store.StoreID())
	if err := store.ExecuteCmd(proto.Get, gArgs, gReply); err != nil ||
		!bytes.Equal(gReply.Value.Bytes, content) {
		t.Fatal(err)
	}

	// Merge the b range back into the a range.
	args, reply := adminMergeArgs(engine.KeyMin, *bDesc, 1, store.StoreID())
	if err := store.ExecuteCmd(proto.AdminMerge, args, reply); err != nil {
		t.Fatal(err)
	}

	// Verify the merge by looking up keys from both ranges.
	rangeA := store.LookupRange([]byte("a"), nil)
	rangeB := store.LookupRange([]byte("c"), nil)

	if !reflect.DeepEqual(rangeA, rangeB) {
		t.Fatalf("ranges were not merged %+v=%+v", rangeA.Desc(), rangeB.Desc())
	}
	if !bytes.Equal(rangeA.Desc().StartKey, engine.KeyMin) {
		t.Fatalf("The start key is not equal to KeyMin %q=%q", rangeA.Desc().StartKey, engine.KeyMin)
	}
	if !bytes.Equal(rangeA.Desc().EndKey, engine.KeyMax) {
		t.Fatalf("The end key is not equal to KeyMax %q=%q", rangeA.Desc().EndKey, engine.KeyMax)
	}

	// Try to get values from after the merge.
	gArgs, gReply = getArgs([]byte("aaa"), rangeA.Desc().RaftID, store.StoreID())
	if err := store.ExecuteCmd(proto.Get, gArgs, gReply); err != nil ||
		!bytes.Equal(gReply.Value.Bytes, content) {
		t.Fatal(err)
	}
	gArgs, gReply = getArgs([]byte("ccc"), rangeB.Desc().RaftID, store.StoreID())
	if err := store.ExecuteCmd(proto.Get, gArgs, gReply); err != nil ||
		!bytes.Equal(gReply.Value.Bytes, content) {
		t.Fatal(err)
	}

	// Put new values after the merge on both sides.
	pArgs, pReply = putArgs([]byte("aaaa"), content, rangeA.Desc().RaftID, store.StoreID())
	if err = store.ExecuteCmd(proto.Put, pArgs, pReply); err != nil {
		t.Fatal(err)
	}
	pArgs, pReply = putArgs([]byte("cccc"), content, rangeB.Desc().RaftID, store.StoreID())
	if err = store.ExecuteCmd(proto.Put, pArgs, pReply); err != nil {
		t.Fatal(err)
	}

	// Try to get the newly placed values.
	gArgs, gReply = getArgs([]byte("aaaa"), rangeA.Desc().RaftID, store.StoreID())
	if err := store.ExecuteCmd(proto.Get, gArgs, gReply); err != nil || !bytes.Equal(gReply.Value.Bytes, content) {
		t.Fatal(err)
	}
	gArgs, gReply = getArgs([]byte("cccc"), rangeA.Desc().RaftID, store.StoreID())
	if err := store.ExecuteCmd(proto.Get, gArgs, gReply); err != nil ||
		!bytes.Equal(gReply.Value.Bytes, content) {
		t.Fatal(err)
	}
}

// TestStoreRangeMergeFirstRange attempts to merge the first range
// which is illegal.
func TestStoreRangeMergeFirstRange(t *testing.T) {
	defer leaktest.AfterTest(t)
	store := createTestStore(t)
	defer store.Stop()

	aDesc, _, err := createSplitRanges(store)
	if err != nil {
		t.Fatal(err)
	}

	// Merge the b range back into the a range. This should fail.
	args, reply := adminMergeArgs(engine.KeyMin, *aDesc, 1, store.StoreID())
	err = store.ExecuteCmd(proto.AdminMerge, args, reply)
	if err == nil {
		t.Fatal("Should not be able to merge the first range")
	}
}

// TestStoreRangeMergeDistantRanges attempts to merge two ranges
// that are not not next to each other.
func TestStoreRangeMergeDistantRanges(t *testing.T) {
	defer leaktest.AfterTest(t)
	store := createTestStore(t)
	defer store.Stop()

	// Split into 3 ranges
	argsSplit, replySplit := adminSplitArgs(engine.KeyMin, []byte("d"), 1, store.StoreID())
	if err := store.ExecuteCmd(proto.AdminSplit, argsSplit, replySplit); err != nil {
		t.Fatalf("Can't split range %s", err)
	}
	argsSplit, replySplit = adminSplitArgs(engine.KeyMin, []byte("b"), 1, store.StoreID())
	if err := store.ExecuteCmd(proto.AdminSplit, argsSplit, replySplit); err != nil {
		t.Fatalf("Can't split range %s", err)
	}

	rangeA := store.LookupRange([]byte("a"), nil)
	rangeB := store.LookupRange([]byte("c"), nil)
	rangeC := store.LookupRange([]byte("e"), nil)

	if bytes.Equal(rangeA.Desc().StartKey, rangeB.Desc().StartKey) {
		log.Errorf("split ranges keys are equal %q!=%q", rangeA.Desc().StartKey, rangeB.Desc().StartKey)
	}
	if bytes.Equal(rangeB.Desc().StartKey, rangeC.Desc().StartKey) {
		log.Errorf("split ranges keys are equal %q!=%q", rangeB.Desc().StartKey, rangeC.Desc().StartKey)
	}
	if bytes.Equal(rangeA.Desc().StartKey, rangeC.Desc().StartKey) {
		log.Errorf("split ranges keys are equal %q!=%q", rangeA.Desc().StartKey, rangeC.Desc().StartKey)
	}

	argsMerge, replyMerge := adminMergeArgs(rangeC.Desc().StartKey, *rangeC.Desc(), 1, store.StoreID())
	rangeA.AdminMerge(argsMerge, replyMerge)
	if replyMerge.Error == nil {
		t.Fatal("Should not be able to merge two ranges that are not adjacent.")
	}
}
