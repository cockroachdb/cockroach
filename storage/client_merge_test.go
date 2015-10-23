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
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
)

func adminMergeArgs(key []byte) roachpb.AdminMergeRequest {
	return roachpb.AdminMergeRequest{
		Span: roachpb.Span{
			Key: key,
		},
	}
}

func createSplitRanges(store *storage.Store) (*roachpb.RangeDescriptor, *roachpb.RangeDescriptor, error) {
	args := adminSplitArgs(roachpb.RKeyMin, []byte("b"))
	if _, err := client.SendWrapped(rg1(store), nil, &args); err != nil {
		return nil, nil, err
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
	args := adminMergeArgs(roachpb.RKeyMin)
	_, err := client.SendWrapped(rg1(store), nil, &args)
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

// TestStoreRangeMergeMetadataCleanup tests that all metadata of a
// subsumed range is cleaned up on merge.
func TestStoreRangeMergeMetadataCleanup(t *testing.T) {
	defer leaktest.AfterTest(t)
	store, stopper := createTestStore(t)
	defer stopper.Stop()

	scan := func(f func(roachpb.KeyValue) (bool, error)) {
		if _, err := engine.MVCCIterate(store.Engine(), roachpb.KeyMin, roachpb.KeyMax, roachpb.ZeroTimestamp, true, nil, false, f); err != nil {
			t.Fatal(err)
		}
	}
	content := roachpb.Key("testing!")

	// Write some values left of the proposed split key.
	pArgs := putArgs([]byte("aaa"), content)
	if _, err := client.SendWrapped(rg1(store), nil, &pArgs); err != nil {
		t.Fatal(err)
	}

	// Collect all the keys.
	preKeys := make(map[string]struct{})
	scan(func(kv roachpb.KeyValue) (bool, error) {
		preKeys[string(kv.Key)] = struct{}{}
		return false, nil
	})

	// Split the range.
	_, bDesc, err := createSplitRanges(store)
	if err != nil {
		t.Fatal(err)
	}

	// Write some values right of the split key.
	pArgs = putArgs([]byte("ccc"), content)
	if _, err := client.SendWrappedWith(rg1(store), nil, roachpb.Header{
		RangeID: bDesc.RangeID,
	}, &pArgs); err != nil {
		t.Fatal(err)
	}

	// Merge the b range back into the a range.
	args := adminMergeArgs(roachpb.RKeyMin)
	if _, err := client.SendWrapped(rg1(store), nil, &args); err != nil {
		t.Fatal(err)
	}

	// Collect all the keys again.
	postKeys := make(map[string]struct{})
	scan(func(kv roachpb.KeyValue) (bool, error) {
		postKeys[string(kv.Key)] = struct{}{}
		return false, nil
	})

	// Compute the new keys.
	for k := range preKeys {
		delete(postKeys, k)
	}

	// Keep only the subsumed range's local keys.
	localRangeKeyPrefix := string(keys.MakeRangeIDPrefix(bDesc.RangeID))
	for k := range postKeys {
		if !strings.HasPrefix(k, localRangeKeyPrefix) {
			delete(postKeys, k)
		}
	}

	if numKeys := len(postKeys); numKeys > 0 {
		var buf bytes.Buffer
		fmt.Fprintf(&buf, "%d keys were not cleaned up:\n", numKeys)
		for k := range postKeys {
			fmt.Fprintf(&buf, "%q\n", k)
		}
		t.Fatal(buf.String())
	}
}

// TestStoreRangeMergeWithData attempts to merge two collocate ranges
// each containing data.
func TestStoreRangeMergeWithData(t *testing.T) {
	defer leaktest.AfterTest(t)
	content := roachpb.Key("testing!")

	store, stopper := createTestStore(t)
	defer stopper.Stop()

	aDesc, bDesc, err := createSplitRanges(store)
	if err != nil {
		t.Fatal(err)
	}

	// Write some values left and right of the proposed split key.
	pArgs := putArgs([]byte("aaa"), content)
	if _, err := client.SendWrapped(rg1(store), nil, &pArgs); err != nil {
		t.Fatal(err)
	}
	pArgs = putArgs([]byte("ccc"), content)
	if _, err := client.SendWrappedWith(rg1(store), nil, roachpb.Header{
		RangeID: bDesc.RangeID,
	}, &pArgs); err != nil {
		t.Fatal(err)
	}

	// Confirm the values are there.
	gArgs := getArgs([]byte("aaa"))
	if reply, err := client.SendWrapped(rg1(store), nil, &gArgs); err != nil {
		t.Fatal(err)
	} else if gReply := reply.(*roachpb.GetResponse); !bytes.Equal(gReply.Value.GetRawBytes(), content) {
		t.Fatalf("actual value %q did not match expected value %q", gReply.Value.GetRawBytes(), content)
	}
	gArgs = getArgs([]byte("ccc"))
	if reply, err := client.SendWrappedWith(rg1(store), nil, roachpb.Header{
		RangeID: bDesc.RangeID,
	}, &gArgs); err != nil {
		t.Fatal(err)
	} else if gReply := reply.(*roachpb.GetResponse); !bytes.Equal(gReply.Value.GetRawBytes(), content) {
		t.Fatalf("actual value %q did not match expected value %q", gReply.Value.GetRawBytes(), content)
	}

	// Merge the b range back into the a range.
	args := adminMergeArgs(roachpb.RKeyMin)
	if _, err := client.SendWrapped(rg1(store), nil, &args); err != nil {
		t.Fatal(err)
	}

	// Verify no intents remains on range descriptor keys.
	for _, key := range []roachpb.Key{keys.RangeDescriptorKey(aDesc.StartKey), keys.RangeDescriptorKey(bDesc.StartKey)} {
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
	if !bytes.Equal(rangeA.Desc().StartKey, roachpb.RKeyMin) {
		t.Fatalf("The start key is not equal to KeyMin %q=%q", rangeA.Desc().StartKey, roachpb.RKeyMin)
	}
	if !bytes.Equal(rangeA.Desc().EndKey, roachpb.RKeyMax) {
		t.Fatalf("The end key is not equal to KeyMax %q=%q", rangeA.Desc().EndKey, roachpb.RKeyMax)
	}

	// Try to get values from after the merge.
	gArgs = getArgs([]byte("aaa"))
	if reply, err := client.SendWrapped(rg1(store), nil, &gArgs); err != nil {
		t.Fatal(err)
	} else if gReply := reply.(*roachpb.GetResponse); !bytes.Equal(gReply.Value.GetRawBytes(), content) {
		t.Fatalf("actual value %q did not match expected value %q", gReply.Value.GetRawBytes(), content)
	}
	gArgs = getArgs([]byte("ccc"))
	if reply, err := client.SendWrappedWith(rg1(store), nil, roachpb.Header{
		RangeID: rangeB.Desc().RangeID,
	}, &gArgs); err != nil {
		t.Fatal(err)
	} else if gReply := reply.(*roachpb.GetResponse); !bytes.Equal(gReply.Value.GetRawBytes(), content) {
		t.Fatalf("actual value %q did not match expected value %q", gReply.Value.GetRawBytes(), content)
	}

	// Put new values after the merge on both sides.
	pArgs = putArgs([]byte("aaaa"), content)
	if _, err := client.SendWrapped(rg1(store), nil, &pArgs); err != nil {
		t.Fatal(err)
	}
	pArgs = putArgs([]byte("cccc"), content)
	if _, err := client.SendWrappedWith(rg1(store), nil, roachpb.Header{
		RangeID: rangeB.Desc().RangeID,
	}, &pArgs); err != nil {
		t.Fatal(err)
	}

	// Try to get the newly placed values.
	gArgs = getArgs([]byte("aaaa"))
	if reply, err := client.SendWrapped(rg1(store), nil, &gArgs); err != nil {
		t.Fatal(err)
	} else if gReply := reply.(*roachpb.GetResponse); !bytes.Equal(gReply.Value.GetRawBytes(), content) {
		t.Fatalf("actual value %q did not match expected value %q", gReply.Value.GetRawBytes(), content)
	}
	gArgs = getArgs([]byte("cccc"))
	if reply, err := client.SendWrapped(rg1(store), nil, &gArgs); err != nil {
		t.Fatal(err)
	} else if gReply := reply.(*roachpb.GetResponse); !bytes.Equal(gReply.Value.GetRawBytes(), content) {
		t.Fatalf("actual value %q did not match expected value %q", gReply.Value.GetRawBytes(), content)
	}
}

// TestStoreRangeMergeLastRange verifies that merging the last range
// fails.
func TestStoreRangeMergeLastRange(t *testing.T) {
	defer leaktest.AfterTest(t)
	store, stopper := createTestStore(t)
	defer stopper.Stop()

	// Merge last range.
	args := adminMergeArgs(roachpb.RKeyMin)
	if _, err := client.SendWrapped(rg1(store), nil, &args); !testutils.IsError(err, "cannot merge final range") {
		t.Fatalf("expected 'cannot merge final range' error; got %s", err)
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
	argsSplit := adminSplitArgs(roachpb.RKeyMin, []byte("d"))
	if _, err := client.SendWrapped(rg1(store), nil, &argsSplit); err != nil {
		t.Fatalf("Can't split range %s", err)
	}
	argsSplit = adminSplitArgs(roachpb.RKeyMin, []byte("b"))
	if _, err := client.SendWrapped(rg1(store), nil, &argsSplit); err != nil {
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
	argsMerge := adminMergeArgs(desc.StartKey)
	if _, err := rangeA.AdminMerge(argsMerge, desc); !testutils.IsError(err, "ranges not collocated") {
		t.Fatalf("did not got expected error; got %s", err)
	}
}
