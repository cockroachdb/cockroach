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
	"bytes"
	"reflect"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
)

func adminMergeArgs(key []byte, rangeID proto.RangeID, storeID proto.StoreID) proto.AdminMergeRequest {
	return proto.AdminMergeRequest{
		RequestHeader: proto.RequestHeader{
			Key:     key,
			RangeID: rangeID,
			Replica: proto.Replica{StoreID: storeID},
		},
	}
}

func createSplitRanges(store *storage.Store) (*proto.RangeDescriptor, *proto.RangeDescriptor, error) {
	args := adminSplitArgs(proto.KeyMin, []byte("b"), 1, store.StoreID())
	if _, pErr := store.ExecuteCmd(context.Background(), &args); pErr != nil {
		return nil, nil, pErr.GoError()
	}

	rangeA := store.LookupReplica([]byte("a"), nil)
	rangeB := store.LookupReplica([]byte("c"), nil)

	if bytes.Equal(rangeA.Desc().StartKey, rangeB.Desc().StartKey) {
		log.Errorf("split ranges keys are equal %q!=%q", rangeA.Desc().StartKey, rangeB.Desc().StartKey)
	}

	return rangeA.Desc(), rangeB.Desc(), nil
}

// TestStoreRangeMergeTwoEmptyRanges tries to merge two empty ranges
// together.
func TestStoreRangeMergeTwoEmptyRanges(t *testing.T) {
	defer leaktest.AfterTest(t)
	store, stopper := createTestStore(t)
	defer stopper.Stop()

	if _, _, err := createSplitRanges(store); err != nil {
		t.Fatal(err)
	}

	// Merge the b range back into the a range.
	args := adminMergeArgs(proto.KeyMin, 1, store.StoreID())
	_, err := store.ExecuteCmd(context.Background(), &args)
	if err != nil {
		t.Fatal(err)
	}

	// Verify the merge by looking up keys from both ranges.
	rangeA := store.LookupReplica([]byte("a"), nil)
	rangeB := store.LookupReplica([]byte("c"), nil)

	if !reflect.DeepEqual(rangeA, rangeB) {
		t.Fatalf("ranges were not merged %+v=%+v", rangeA.Desc(), rangeB.Desc())
	}
}

// TestStoreRangeMergeWithData attempts to merge two collocate ranges
// each containing data.
func TestStoreRangeMergeWithData(t *testing.T) {
	defer leaktest.AfterTest(t)
	content := proto.Key("testing!")

	store, stopper := createTestStore(t)
	defer stopper.Stop()

	aDesc, bDesc, err := createSplitRanges(store)
	if err != nil {
		t.Fatal(err)
	}

	// Write some values left and right of the proposed split key.
	pArgs := putArgs([]byte("aaa"), content, aDesc.RangeID, store.StoreID())
	if _, err := store.ExecuteCmd(context.Background(), &pArgs); err != nil {
		t.Fatal(err)
	}
	pArgs = putArgs([]byte("ccc"), content, bDesc.RangeID, store.StoreID())
	if _, err := store.ExecuteCmd(context.Background(), &pArgs); err != nil {
		t.Fatal(err)
	}

	// Confirm the values are there.
	gArgs := getArgs([]byte("aaa"), aDesc.RangeID, store.StoreID())
	if reply, err := store.ExecuteCmd(context.Background(), &gArgs); err != nil {
		t.Fatal(err)
	} else if gReply := reply.(*proto.GetResponse); !bytes.Equal(gReply.Value.Bytes, content) {
		t.Fatalf("actual value %q did not match expected value %q", gReply.Value.Bytes, content)
	}
	gArgs = getArgs([]byte("ccc"), bDesc.RangeID, store.StoreID())
	if reply, err := store.ExecuteCmd(context.Background(), &gArgs); err != nil {
		t.Fatal(err)
	} else if gReply := reply.(*proto.GetResponse); !bytes.Equal(gReply.Value.Bytes, content) {
		t.Fatalf("actual value %q did not match expected value %q", gReply.Value.Bytes, content)
	}

	// Merge the b range back into the a range.
	args := adminMergeArgs(proto.KeyMin, 1, store.StoreID())
	if _, err := store.ExecuteCmd(context.Background(), &args); err != nil {
		t.Fatal(err)
	}

	// Verify no intents remains on range descriptor keys.
	for _, key := range []proto.Key{keys.RangeDescriptorKey(aDesc.StartKey), keys.RangeDescriptorKey(bDesc.StartKey)} {
		if _, _, err := engine.MVCCGet(store.Engine(), key, store.Clock().Now(), true, nil); err != nil {
			t.Fatal(err)
		}
	}

	// Verify the merge by looking up keys from both ranges.
	rangeA := store.LookupReplica([]byte("a"), nil)
	rangeB := store.LookupReplica([]byte("c"), nil)

	if !reflect.DeepEqual(rangeA, rangeB) {
		t.Fatalf("ranges were not merged %+v=%+v", rangeA.Desc(), rangeB.Desc())
	}
	if !bytes.Equal(rangeA.Desc().StartKey, proto.KeyMin) {
		t.Fatalf("The start key is not equal to KeyMin %q=%q", rangeA.Desc().StartKey, proto.KeyMin)
	}
	if !bytes.Equal(rangeA.Desc().EndKey, proto.KeyMax) {
		t.Fatalf("The end key is not equal to KeyMax %q=%q", rangeA.Desc().EndKey, proto.KeyMax)
	}

	// Try to get values from after the merge.
	gArgs = getArgs([]byte("aaa"), rangeA.Desc().RangeID, store.StoreID())
	if reply, err := store.ExecuteCmd(context.Background(), &gArgs); err != nil {
		t.Fatal(err)
	} else if gReply := reply.(*proto.GetResponse); !bytes.Equal(gReply.Value.Bytes, content) {
		t.Fatalf("actual value %q did not match expected value %q", gReply.Value.Bytes, content)
	}
	gArgs = getArgs([]byte("ccc"), rangeB.Desc().RangeID, store.StoreID())
	if reply, err := store.ExecuteCmd(context.Background(), &gArgs); err != nil {
		t.Fatal(err)
	} else if gReply := reply.(*proto.GetResponse); !bytes.Equal(gReply.Value.Bytes, content) {
		t.Fatalf("actual value %q did not match expected value %q", gReply.Value.Bytes, content)
	}

	// Put new values after the merge on both sides.
	pArgs = putArgs([]byte("aaaa"), content, rangeA.Desc().RangeID, store.StoreID())
	if _, err := store.ExecuteCmd(context.Background(), &pArgs); err != nil {
		t.Fatal(err)
	}
	pArgs = putArgs([]byte("cccc"), content, rangeB.Desc().RangeID, store.StoreID())
	if _, err := store.ExecuteCmd(context.Background(), &pArgs); err != nil {
		t.Fatal(err)
	}

	// Try to get the newly placed values.
	gArgs = getArgs([]byte("aaaa"), rangeA.Desc().RangeID, store.StoreID())
	if reply, err := store.ExecuteCmd(context.Background(), &gArgs); err != nil {
		t.Fatal(err)
	} else if gReply := reply.(*proto.GetResponse); !bytes.Equal(gReply.Value.Bytes, content) {
		t.Fatalf("actual value %q did not match expected value %q", gReply.Value.Bytes, content)
	}
	gArgs = getArgs([]byte("cccc"), rangeA.Desc().RangeID, store.StoreID())
	if reply, err := store.ExecuteCmd(context.Background(), &gArgs); err != nil {
		t.Fatal(err)
	} else if gReply := reply.(*proto.GetResponse); !bytes.Equal(gReply.Value.Bytes, content) {
		t.Fatalf("actual value %q did not match expected value %q", gReply.Value.Bytes, content)
	}
}

// TestStoreRangeMergeLastRange verifies that merging the last range
// fails.
func TestStoreRangeMergeLastRange(t *testing.T) {
	defer leaktest.AfterTest(t)
	store, stopper := createTestStore(t)
	defer stopper.Stop()

	// Merge last range.
	args := adminMergeArgs(proto.KeyMin, 1, store.StoreID())
	if _, pErr := store.ExecuteCmd(context.Background(), &args); !testutils.IsError(pErr.GoError(), "cannot merge final range") {
		t.Fatalf("expected 'cannot merge final range' error; got %s", pErr)
	}
}

// TestStoreRangeMergeNonCollocated attempts to merge two ranges
// that are not on the same stores.
func TestStoreRangeMergeNonCollocated(t *testing.T) {
	defer leaktest.AfterTest(t)
	mtc := startMultiTestContext(t, 4)
	defer mtc.Stop()

	store := mtc.stores[0]

	// Split into 3 ranges
	argsSplit := adminSplitArgs(proto.KeyMin, []byte("d"), 1, store.StoreID())
	if _, err := store.ExecuteCmd(context.Background(), &argsSplit); err != nil {
		t.Fatalf("Can't split range %s", err)
	}
	argsSplit = adminSplitArgs(proto.KeyMin, []byte("b"), 1, store.StoreID())
	if _, err := store.ExecuteCmd(context.Background(), &argsSplit); err != nil {
		t.Fatalf("Can't split range %s", err)
	}

	rangeA := store.LookupReplica([]byte("a"), nil)
	rangeB := store.LookupReplica([]byte("c"), nil)
	rangeC := store.LookupReplica([]byte("e"), nil)

	if bytes.Equal(rangeA.Desc().StartKey, rangeB.Desc().StartKey) {
		log.Errorf("split ranges keys are equal %q!=%q", rangeA.Desc().StartKey, rangeB.Desc().StartKey)
	}
	if bytes.Equal(rangeB.Desc().StartKey, rangeC.Desc().StartKey) {
		log.Errorf("split ranges keys are equal %q!=%q", rangeB.Desc().StartKey, rangeC.Desc().StartKey)
	}
	if bytes.Equal(rangeA.Desc().StartKey, rangeC.Desc().StartKey) {
		log.Errorf("split ranges keys are equal %q!=%q", rangeA.Desc().StartKey, rangeC.Desc().StartKey)
	}

	// Replicate the ranges to different sets of stores. Ranges A and C
	// are collocated, but B is different.
	mtc.replicateRange(rangeA.Desc().RangeID, 0, 1, 2)
	mtc.replicateRange(rangeB.Desc().RangeID, 0, 1, 3)
	mtc.replicateRange(rangeC.Desc().RangeID, 0, 1, 2)

	// Attempt to merge.
	desc := rangeA.Desc()
	argsMerge := adminMergeArgs(desc.StartKey, 1, store.StoreID())
	if _, err := rangeA.AdminMerge(argsMerge, desc); !testutils.IsError(err, "ranges not collocated") {
		t.Fatalf("did not got expected error; got %s", err)
	}
}
