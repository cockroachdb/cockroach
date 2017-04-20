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
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

func adminMergeArgs(key roachpb.Key) *roachpb.AdminMergeRequest {
	return &roachpb.AdminMergeRequest{
		Span: roachpb.Span{
			Key: key,
		},
	}
}

func createSplitRanges(
	store *storage.Store,
) (*roachpb.RangeDescriptor, *roachpb.RangeDescriptor, *roachpb.Error) {
	args := adminSplitArgs(roachpb.KeyMin, []byte("b"))
	if _, err := client.SendWrapped(context.Background(), rg1(store), args); err != nil {
		return nil, nil, err
	}

	rangeADesc := store.LookupReplica([]byte("a"), nil).Desc()
	rangeBDesc := store.LookupReplica([]byte("c"), nil).Desc()

	if bytes.Equal(rangeADesc.StartKey, rangeBDesc.StartKey) {
		log.Errorf(context.TODO(), "split ranges keys are equal %q!=%q", rangeADesc.StartKey, rangeBDesc.StartKey)
	}

	return rangeADesc, rangeBDesc, nil
}

// TestStoreRangeMergeTwoEmptyRanges tries to merge two empty ranges together.
func TestStoreRangeMergeTwoEmptyRanges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	storeCfg := storage.TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableSplitQueue = true
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	store := createTestStoreWithConfig(t, stopper, storeCfg)

	if _, _, err := createSplitRanges(store); err != nil {
		t.Fatal(err)
	}

	// Merge the b range back into the a range.
	args := adminMergeArgs(roachpb.KeyMin)
	_, err := client.SendWrapped(context.Background(), rg1(store), args)
	if err != nil {
		t.Fatal(err)
	}

	// Verify the merge by looking up keys from both ranges.
	replicaA := store.LookupReplica([]byte("a"), nil)
	replicaB := store.LookupReplica([]byte("c"), nil)

	if !reflect.DeepEqual(replicaA, replicaB) {
		t.Fatalf("ranges were not merged %s!=%s", replicaA, replicaB)
	}
}

// TestStoreRangeMergeMetadataCleanup tests that all metadata of a
// subsumed range is cleaned up on merge.
func TestStoreRangeMergeMetadataCleanup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	storeCfg := storage.TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableSplitQueue = true
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	store := createTestStoreWithConfig(t, stopper, storeCfg)

	scan := func(f func(roachpb.KeyValue) (bool, error)) {
		if _, err := engine.MVCCIterate(context.Background(), store.Engine(), roachpb.KeyMin, roachpb.KeyMax, hlc.Timestamp{}, true, nil, false, f); err != nil {
			t.Fatal(err)
		}
	}
	content := roachpb.Key("testing!")

	// Write some values left of the proposed split key.
	pArgs := putArgs([]byte("aaa"), content)
	if _, err := client.SendWrapped(context.Background(), rg1(store), pArgs); err != nil {
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
	if _, err := client.SendWrappedWith(context.Background(), rg1(store), roachpb.Header{
		RangeID: bDesc.RangeID,
	}, pArgs); err != nil {
		t.Fatal(err)
	}

	// Merge the b range back into the a range.
	args := adminMergeArgs(roachpb.KeyMin)
	if _, err := client.SendWrapped(context.Background(), rg1(store), args); err != nil {
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
	defer leaktest.AfterTest(t)()
	storeCfg := storage.TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableSplitQueue = true
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	store := createTestStoreWithConfig(t, stopper, storeCfg)

	content := roachpb.Key("testing!")

	aDesc, bDesc, err := createSplitRanges(store)
	if err != nil {
		t.Fatal(err)
	}

	// Write some values left and right of the proposed split key.
	pArgs := putArgs([]byte("aaa"), content)
	if _, err := client.SendWrapped(context.Background(), rg1(store), pArgs); err != nil {
		t.Fatal(err)
	}
	pArgs = putArgs([]byte("ccc"), content)
	if _, err := client.SendWrappedWith(context.Background(), rg1(store), roachpb.Header{
		RangeID: bDesc.RangeID,
	}, pArgs); err != nil {
		t.Fatal(err)
	}

	// Confirm the values are there.
	gArgs := getArgs([]byte("aaa"))
	if reply, err := client.SendWrapped(context.Background(), rg1(store), gArgs); err != nil {
		t.Fatal(err)
	} else if replyBytes, err := reply.(*roachpb.GetResponse).Value.GetBytes(); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(replyBytes, content) {
		t.Fatalf("actual value %q did not match expected value %q", replyBytes, content)
	}
	gArgs = getArgs([]byte("ccc"))
	if reply, err := client.SendWrappedWith(context.Background(), rg1(store), roachpb.Header{
		RangeID: bDesc.RangeID,
	}, gArgs); err != nil {
		t.Fatal(err)
	} else if replyBytes, err := reply.(*roachpb.GetResponse).Value.GetBytes(); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(replyBytes, content) {
		t.Fatalf("actual value %q did not match expected value %q", replyBytes, content)
	}

	// Merge the b range back into the a range.
	args := adminMergeArgs(roachpb.KeyMin)
	if _, err := client.SendWrapped(context.Background(), rg1(store), args); err != nil {
		t.Fatal(err)
	}

	// Verify no intents remains on range descriptor keys.
	for _, key := range []roachpb.Key{keys.RangeDescriptorKey(aDesc.StartKey), keys.RangeDescriptorKey(bDesc.StartKey)} {
		if _, _, err := engine.MVCCGet(context.Background(), store.Engine(), key, store.Clock().Now(), true, nil); err != nil {
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
	if reply, err := client.SendWrapped(context.Background(), rg1(store), gArgs); err != nil {
		t.Fatal(err)
	} else if replyBytes, err := reply.(*roachpb.GetResponse).Value.GetBytes(); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(replyBytes, content) {
		t.Fatalf("actual value %q did not match expected value %q", replyBytes, content)
	}
	gArgs = getArgs([]byte("ccc"))
	if reply, err := client.SendWrappedWith(context.Background(), rg1(store), roachpb.Header{
		RangeID: rangeB.RangeID,
	}, gArgs); err != nil {
		t.Fatal(err)
	} else if replyBytes, err := reply.(*roachpb.GetResponse).Value.GetBytes(); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(replyBytes, content) {
		t.Fatalf("actual value %q did not match expected value %q", replyBytes, content)
	}

	// Put new values after the merge on both sides.
	pArgs = putArgs([]byte("aaaa"), content)
	if _, err := client.SendWrapped(context.Background(), rg1(store), pArgs); err != nil {
		t.Fatal(err)
	}
	pArgs = putArgs([]byte("cccc"), content)
	if _, err := client.SendWrappedWith(context.Background(), rg1(store), roachpb.Header{
		RangeID: rangeB.RangeID,
	}, pArgs); err != nil {
		t.Fatal(err)
	}

	// Try to get the newly placed values.
	gArgs = getArgs([]byte("aaaa"))
	if reply, err := client.SendWrapped(context.Background(), rg1(store), gArgs); err != nil {
		t.Fatal(err)
	} else if replyBytes, err := reply.(*roachpb.GetResponse).Value.GetBytes(); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(replyBytes, content) {
		t.Fatalf("actual value %q did not match expected value %q", replyBytes, content)
	}
	gArgs = getArgs([]byte("cccc"))
	if reply, err := client.SendWrapped(context.Background(), rg1(store), gArgs); err != nil {
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
	defer leaktest.AfterTest(t)()
	storeCfg := storage.TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableSplitQueue = true
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	store := createTestStoreWithConfig(t, stopper, storeCfg)

	// Merge last range.
	args := adminMergeArgs(roachpb.KeyMin)
	if _, pErr := client.SendWrapped(context.Background(), rg1(store), args); !testutils.IsPError(pErr, "cannot merge final range") {
		t.Fatalf("expected 'cannot merge final range' error; got %s", pErr)
	}
}

// TestStoreRangeMergeNonCollocated attempts to merge two ranges
// that are not on the same stores.
func TestStoreRangeMergeNonCollocated(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := &multiTestContext{}
	defer mtc.Stop()
	mtc.Start(t, 4)

	store := mtc.stores[0]

	// Split into 3 ranges
	argsSplit := adminSplitArgs(roachpb.KeyMin, []byte("d"))
	if _, pErr := client.SendWrapped(context.Background(), rg1(store), argsSplit); pErr != nil {
		t.Fatalf("Can't split range %s", pErr)
	}
	argsSplit = adminSplitArgs(roachpb.KeyMin, []byte("b"))
	if _, pErr := client.SendWrapped(context.Background(), rg1(store), argsSplit); pErr != nil {
		t.Fatalf("Can't split range %s", pErr)
	}

	rangeA := store.LookupReplica([]byte("a"), nil)
	rangeADesc := rangeA.Desc()
	rangeB := store.LookupReplica([]byte("c"), nil)
	rangeBDesc := rangeB.Desc()
	rangeC := store.LookupReplica([]byte("e"), nil)
	rangeCDesc := rangeC.Desc()

	if bytes.Equal(rangeADesc.StartKey, rangeBDesc.StartKey) {
		log.Errorf(context.TODO(), "split ranges keys are equal %q!=%q", rangeADesc.StartKey, rangeBDesc.StartKey)
	}
	if bytes.Equal(rangeBDesc.StartKey, rangeCDesc.StartKey) {
		log.Errorf(context.TODO(), "split ranges keys are equal %q!=%q", rangeBDesc.StartKey, rangeCDesc.StartKey)
	}
	if bytes.Equal(rangeADesc.StartKey, rangeCDesc.StartKey) {
		log.Errorf(context.TODO(), "split ranges keys are equal %q!=%q", rangeADesc.StartKey, rangeCDesc.StartKey)
	}

	// Replicate the ranges to different sets of stores. Ranges A and C
	// are collocated, but B is different.
	mtc.replicateRange(rangeA.RangeID, 1, 2)
	mtc.replicateRange(rangeB.RangeID, 1, 3)
	mtc.replicateRange(rangeC.RangeID, 1, 2)

	// Attempt to merge.
	argsMerge := adminMergeArgs(roachpb.Key(rangeADesc.StartKey))
	if _, pErr := rangeA.AdminMerge(context.Background(), *argsMerge); !testutils.IsPError(pErr, "ranges not collocated") {
		t.Fatalf("did not got expected error; got %s", pErr)
	}
}

// TestStoreRangeMergeStats starts by splitting a range, then writing random data
// to both sides of the split. It then merges the ranges and verifies the merged
// range has stats consistent with recomputations.
func TestStoreRangeMergeStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	manual := hlc.NewManualClock(123)
	storeCfg := storage.TestStoreConfig(hlc.NewClock(manual.UnixNano, time.Nanosecond))
	storeCfg.TestingKnobs.DisableSplitQueue = true
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	store := createTestStoreWithConfig(t, stopper, storeCfg)

	// Split the range.
	aDesc, bDesc, pErr := createSplitRanges(store)
	if pErr != nil {
		t.Fatal(pErr)
	}

	// Write some values left and right of the proposed split key.
	writeRandomDataToRange(t, store, aDesc.RangeID, []byte("aaa"))
	writeRandomDataToRange(t, store, bDesc.RangeID, []byte("ccc"))

	// Get the range stats for both ranges now that we have data.
	snap := store.Engine().NewSnapshot()
	defer snap.Close()
	msA, err := engine.MVCCGetRangeStats(context.Background(), snap, aDesc.RangeID)
	if err != nil {
		t.Fatal(err)
	}
	msB, err := engine.MVCCGetRangeStats(context.Background(), snap, bDesc.RangeID)
	if err != nil {
		t.Fatal(err)
	}

	// Stats should agree with recomputation.
	if err := verifyRecomputedStats(snap, aDesc, msA, manual.UnixNano()); err != nil {
		t.Fatalf("failed to verify range A's stats before split: %v", err)
	}
	if err := verifyRecomputedStats(snap, bDesc, msB, manual.UnixNano()); err != nil {
		t.Fatalf("failed to verify range B's stats before split: %v", err)
	}

	manual.Increment(100)

	// Merge the b range back into the a range.
	args := adminMergeArgs(roachpb.KeyMin)
	if _, err := client.SendWrapped(context.Background(), rg1(store), args); err != nil {
		t.Fatal(err)
	}
	replMerged := store.LookupReplica(aDesc.StartKey, nil)

	// Get the range stats for the merged range and verify.
	snap = store.Engine().NewSnapshot()
	defer snap.Close()
	msMerged, err := engine.MVCCGetRangeStats(context.Background(), snap, replMerged.RangeID)
	if err != nil {
		t.Fatal(err)
	}

	// Merged stats should agree with recomputation.
	if err := verifyRecomputedStats(snap, replMerged.Desc(), msMerged, manual.UnixNano()); err != nil {
		t.Errorf("failed to verify range's stats after merge: %v", err)
	}
}

func BenchmarkStoreRangeMerge(b *testing.B) {
	defer tracing.Disable()()
	storeCfg := storage.TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableSplitQueue = true
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	store := createTestStoreWithConfig(b, stopper, storeCfg)

	// Perform initial split of ranges.
	sArgs := adminSplitArgs(roachpb.KeyMin, []byte("b"))
	if _, err := client.SendWrapped(context.Background(), rg1(store), sArgs); err != nil {
		b.Fatal(err)
	}

	// Write some values left and right of the proposed split key.
	aDesc := store.LookupReplica([]byte("a"), nil).Desc()
	bDesc := store.LookupReplica([]byte("c"), nil).Desc()
	writeRandomDataToRange(b, store, aDesc.RangeID, []byte("aaa"))
	writeRandomDataToRange(b, store, bDesc.RangeID, []byte("ccc"))

	// Create args to merge the b range back into the a range.
	mArgs := adminMergeArgs(roachpb.KeyMin)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Merge the ranges.
		b.StartTimer()
		if _, err := client.SendWrapped(context.Background(), rg1(store), mArgs); err != nil {
			b.Fatal(err)
		}

		// Split the range.
		b.StopTimer()
		if _, err := client.SendWrapped(context.Background(), rg1(store), sArgs); err != nil {
			b.Fatal(err)
		}
	}
}
