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

func adminMergeArgs(key roachpb.Key) roachpb.AdminMergeRequest {
	return roachpb.AdminMergeRequest{
		Span: roachpb.Span{
			Key: key,
		},
	}
}

func createSplitRanges(store *storage.Store) (*roachpb.RangeDescriptor, *roachpb.RangeDescriptor, error) {
	args := adminSplitArgs(roachpb.KeyMin, []byte("b"))
	if _, err := client.SendWrapped(rg1(store), nil, &args); err != nil {
		return nil, nil, err
	}

	rangeADesc := store.LookupReplica([]byte("a"), nil).Desc()
	rangeBDesc := store.LookupReplica([]byte("c"), nil).Desc()

	if bytes.Equal(rangeADesc.StartKey, rangeBDesc.StartKey) {
		log.Errorf("split ranges keys are equal %q!=%q", rangeADesc.StartKey, rangeBDesc.StartKey)
	}

	return rangeADesc, rangeBDesc, nil
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
	args := adminMergeArgs(roachpb.KeyMin)
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
	args := adminMergeArgs(roachpb.KeyMin)
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
	} else if replyBytes, err := reply.(*roachpb.GetResponse).Value.GetBytes(); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(replyBytes, content) {
		t.Fatalf("actual value %q did not match expected value %q", replyBytes, content)
	}
	gArgs = getArgs([]byte("ccc"))
	if reply, err := client.SendWrappedWith(rg1(store), nil, roachpb.Header{
		RangeID: bDesc.RangeID,
	}, &gArgs); err != nil {
		t.Fatal(err)
	} else if replyBytes, err := reply.(*roachpb.GetResponse).Value.GetBytes(); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(replyBytes, content) {
		t.Fatalf("actual value %q did not match expected value %q", replyBytes, content)
	}

	// Merge the b range back into the a range.
	args := adminMergeArgs(roachpb.KeyMin)
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
	rangeADesc := rangeA.Desc()
	rangeBDesc := rangeB.Desc()

	if !reflect.DeepEqual(rangeA, rangeB) {
		t.Fatalf("ranges were not merged %+v=%+v", rangeADesc, rangeBDesc)
	}
	if !bytes.Equal(rangeADesc.StartKey, roachpb.RKeyMin) {
		t.Fatalf("The start key is not equal to KeyMin %q=%q", rangeADesc.StartKey, roachpb.RKeyMin)
	}
	if !bytes.Equal(rangeADesc.EndKey, roachpb.RKeyMax) {
		t.Fatalf("The end key is not equal to KeyMax %q=%q", rangeADesc.EndKey, roachpb.RKeyMax)
	}

	// Try to get values from after the merge.
	gArgs = getArgs([]byte("aaa"))
	if reply, err := client.SendWrapped(rg1(store), nil, &gArgs); err != nil {
		t.Fatal(err)
	} else if replyBytes, err := reply.(*roachpb.GetResponse).Value.GetBytes(); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(replyBytes, content) {
		t.Fatalf("actual value %q did not match expected value %q", replyBytes, content)
	}
	gArgs = getArgs([]byte("ccc"))
	if reply, err := client.SendWrappedWith(rg1(store), nil, roachpb.Header{
		RangeID: rangeB.RangeID,
	}, &gArgs); err != nil {
		t.Fatal(err)
	} else if replyBytes, err := reply.(*roachpb.GetResponse).Value.GetBytes(); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(replyBytes, content) {
		t.Fatalf("actual value %q did not match expected value %q", replyBytes, content)
	}

	// Put new values after the merge on both sides.
	pArgs = putArgs([]byte("aaaa"), content)
	if _, err := client.SendWrapped(rg1(store), nil, &pArgs); err != nil {
		t.Fatal(err)
	}
	pArgs = putArgs([]byte("cccc"), content)
	if _, err := client.SendWrappedWith(rg1(store), nil, roachpb.Header{
		RangeID: rangeB.RangeID,
	}, &pArgs); err != nil {
		t.Fatal(err)
	}

	// Try to get the newly placed values.
	gArgs = getArgs([]byte("aaaa"))
	if reply, err := client.SendWrapped(rg1(store), nil, &gArgs); err != nil {
		t.Fatal(err)
	} else if replyBytes, err := reply.(*roachpb.GetResponse).Value.GetBytes(); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(replyBytes, content) {
		t.Fatalf("actual value %q did not match expected value %q", replyBytes, content)
	}
	gArgs = getArgs([]byte("cccc"))
	if reply, err := client.SendWrapped(rg1(store), nil, &gArgs); err != nil {
		t.Fatal(err)
	} else if replyBytes, err := reply.(*roachpb.GetResponse).Value.GetBytes(); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(replyBytes, content) {
		t.Fatalf("actual value %q did not match expected value %q", replyBytes, content)
	}
}

// TestStoreRangeMergeLastRange verifies that merging the last range
// fails.
func TestStoreRangeMergeLastRange(t *testing.T) {
	defer leaktest.AfterTest(t)
	store, stopper := createTestStore(t)
	defer stopper.Stop()

	// Merge last range.
	args := adminMergeArgs(roachpb.KeyMin)
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
	argsSplit := adminSplitArgs(roachpb.KeyMin, []byte("d"))
	if _, err := client.SendWrapped(rg1(store), nil, &argsSplit); err != nil {
		t.Fatalf("Can't split range %s", err)
	}
	argsSplit = adminSplitArgs(roachpb.KeyMin, []byte("b"))
	if _, err := client.SendWrapped(rg1(store), nil, &argsSplit); err != nil {
		t.Fatalf("Can't split range %s", err)
	}

	rangeA := store.LookupReplica([]byte("a"), nil)
	rangeADesc := rangeA.Desc()
	rangeB := store.LookupReplica([]byte("c"), nil)
	rangeBDesc := rangeB.Desc()
	rangeC := store.LookupReplica([]byte("e"), nil)
	rangeCDesc := rangeC.Desc()

	if bytes.Equal(rangeADesc.StartKey, rangeBDesc.StartKey) {
		log.Errorf("split ranges keys are equal %q!=%q", rangeADesc.StartKey, rangeBDesc.StartKey)
	}
	if bytes.Equal(rangeBDesc.StartKey, rangeCDesc.StartKey) {
		log.Errorf("split ranges keys are equal %q!=%q", rangeBDesc.StartKey, rangeCDesc.StartKey)
	}
	if bytes.Equal(rangeADesc.StartKey, rangeCDesc.StartKey) {
		log.Errorf("split ranges keys are equal %q!=%q", rangeADesc.StartKey, rangeCDesc.StartKey)
	}

	// Replicate the ranges to different sets of stores. Ranges A and C
	// are collocated, but B is different.
	mtc.replicateRange(rangeA.RangeID, 0, 1, 2)
	mtc.replicateRange(rangeB.RangeID, 0, 1, 3)
	mtc.replicateRange(rangeC.RangeID, 0, 1, 2)

	// Attempt to merge.
	rangeADesc = rangeA.Desc()
	argsMerge := adminMergeArgs(roachpb.Key(rangeADesc.StartKey))
	if _, err := rangeA.AdminMerge(argsMerge, rangeADesc); !testutils.IsError(err, "ranges not collocated") {
		t.Fatalf("did not got expected error; got %s", err)
	}
}
