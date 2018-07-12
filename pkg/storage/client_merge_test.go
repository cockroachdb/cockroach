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

package storage_test

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/stateloader"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/storage/txnwait"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

func adminMergeArgs(key roachpb.Key) *roachpb.AdminMergeRequest {
	return &roachpb.AdminMergeRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: key,
		},
	}
}

func createSplitRanges(
	store *storage.Store,
) (*roachpb.RangeDescriptor, *roachpb.RangeDescriptor, *roachpb.Error) {
	args := adminSplitArgs(roachpb.Key("b"))
	if _, err := client.SendWrapped(context.Background(), store.TestSender(), args); err != nil {
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
	_, err := client.SendWrapped(context.Background(), store.TestSender(), args)
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

	scan := func() map[string]struct{} {
		t.Helper()
		kvs, err := engine.Scan(store.Engine(), engine.NilKey, engine.MVCCKeyMax, 0 /* max */)
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]struct{}{}
		for _, kv := range kvs {
			out[string(kv.Key.Key)] = struct{}{}
		}
		return out
	}

	content := roachpb.Key("testing!")

	// Write some values left of the proposed split key.
	pArgs := putArgs([]byte("aaa"), content)
	if _, err := client.SendWrapped(context.Background(), store.TestSender(), pArgs); err != nil {
		t.Fatal(err)
	}

	// Collect all the keys.
	preKeys := scan()

	// Split the range.
	_, bDesc, err := createSplitRanges(store)
	if err != nil {
		t.Fatal(err)
	}

	// Write some values right of the split key.
	pArgs = putArgs([]byte("ccc"), content)
	if _, err := client.SendWrappedWith(context.Background(), store.TestSender(), roachpb.Header{
		RangeID: bDesc.RangeID,
	}, pArgs); err != nil {
		t.Fatal(err)
	}

	// Merge the b range back into the a range.
	args := adminMergeArgs(roachpb.KeyMin)
	if _, err := client.SendWrapped(context.Background(), store.TestSender(), args); err != nil {
		t.Fatal(err)
	}

	// Collect all the keys again.
	postKeys := scan()

	// Compute the new keys.
	for k := range preKeys {
		delete(postKeys, k)
	}

	tombstoneKey := string(keys.RaftTombstoneKey(bDesc.RangeID))
	if _, ok := postKeys[tombstoneKey]; !ok {
		t.Errorf("tombstone key (%s) missing after merge", roachpb.Key(tombstoneKey))
	}
	delete(postKeys, tombstoneKey)

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
			fmt.Fprintf(&buf, "%s (%q)\n", roachpb.Key(k), k)
		}
		t.Fatal(buf.String())
	}
}

// TestStoreRangeMergeWithData attempts to merge two collocate ranges
// each containing data.
func TestStoreRangeMergeWithData(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, colocate := range []bool{false, true} {
		for _, retries := range []int64{0, 3} {
			t.Run(fmt.Sprintf("colocate=%v/retries=%d", colocate, retries), func(t *testing.T) {
				mergeWithData(t, colocate, retries)
			})
		}
	}
}

func mergeWithData(t *testing.T, colocate bool, retries int64) {
	ctx := context.Background()
	sc := storage.TestStoreConfig(nil)
	sc.TestingKnobs.DisableReplicateQueue = true

	// Maybe inject some retryable errors when the merge transaction commits.
	var mtc *multiTestContext
	sc.TestingKnobs.TestingRequestFilter = func(ba roachpb.BatchRequest) *roachpb.Error {
		for _, req := range ba.Requests {
			if et := req.GetEndTransaction(); et != nil && et.InternalCommitTrigger.GetMergeTrigger() != nil {
				if atomic.AddInt64(&retries, -1) >= 0 {
					return roachpb.NewError(roachpb.NewTransactionRetryError(roachpb.RETRY_SERIALIZABLE))
				}
			}
			if req.GetGetSnapshotForMerge() != nil {
				// Introduce targeted chaos by forcing a lease acquisition before
				// GetSnapshotForMerge can execute. This triggers an unusual code path
				// where the lease acquisition, not GetSnapshotForMerge, notices the
				// merge and installs a mergeComplete channel on the replica.
				mtc.advanceClock(ctx)
			}
		}
		return nil
	}

	mtc = &multiTestContext{storeConfig: &sc}

	var store1, store2 *storage.Store
	if colocate {
		mtc.Start(t, 1)
		store1, store2 = mtc.stores[0], mtc.stores[0]
	} else {
		mtc.Start(t, 2)
		store1, store2 = mtc.stores[0], mtc.stores[1]
	}
	defer mtc.Stop()

	aDesc, bDesc, pErr := createSplitRanges(store1)
	if pErr != nil {
		t.Fatal(pErr)
	}

	if !colocate {
		mtc.replicateRange(bDesc.RangeID, 1)
		mtc.transferLease(ctx, bDesc.RangeID, 0, 1)
		mtc.unreplicateRange(bDesc.RangeID, 0)
	}

	content := roachpb.Key("testing!")

	// Write some values left and right of the proposed split key.
	pArgs := putArgs([]byte("aaa"), content)
	if _, err := client.SendWrapped(ctx, store1.TestSender(), pArgs); err != nil {
		t.Fatal(err)
	}
	pArgs = putArgs([]byte("ccc"), content)
	if _, err := client.SendWrappedWith(ctx, store2.TestSender(), roachpb.Header{
		RangeID: bDesc.RangeID,
	}, pArgs); err != nil {
		t.Fatal(err)
	}

	// Confirm the values are there.
	gArgs := getArgs([]byte("aaa"))
	if reply, err := client.SendWrapped(ctx, store1.TestSender(), gArgs); err != nil {
		t.Fatal(err)
	} else if replyBytes, err := reply.(*roachpb.GetResponse).Value.GetBytes(); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(replyBytes, content) {
		t.Fatalf("actual value %q did not match expected value %q", replyBytes, content)
	}
	gArgs = getArgs([]byte("ccc"))
	if reply, err := client.SendWrappedWith(ctx, store2.TestSender(), roachpb.Header{
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
	if _, err := client.SendWrapped(ctx, store1.TestSender(), args); err != nil {
		t.Fatal(err)
	}

	// Verify no intents remains on range descriptor keys.
	for _, key := range []roachpb.Key{keys.RangeDescriptorKey(aDesc.StartKey), keys.RangeDescriptorKey(bDesc.StartKey)} {
		if _, _, err := engine.MVCCGet(ctx, store1.Engine(), key, store1.Clock().Now(), true, nil); err != nil {
			t.Fatal(err)
		}
	}

	// Verify the merge by looking up keys from both ranges.
	rangeA := store1.LookupReplica([]byte("a"), nil)
	rangeB := store1.LookupReplica([]byte("c"), nil)
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
	if reply, err := client.SendWrapped(ctx, store1.TestSender(), gArgs); err != nil {
		t.Fatal(err)
	} else if replyBytes, err := reply.(*roachpb.GetResponse).Value.GetBytes(); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(replyBytes, content) {
		t.Fatalf("actual value %q did not match expected value %q", replyBytes, content)
	}
	gArgs = getArgs([]byte("ccc"))
	if reply, err := client.SendWrappedWith(ctx, store1.TestSender(), roachpb.Header{
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
	if _, err := client.SendWrapped(ctx, store1.TestSender(), pArgs); err != nil {
		t.Fatal(err)
	}
	pArgs = putArgs([]byte("cccc"), content)
	if _, err := client.SendWrappedWith(ctx, store1.TestSender(), roachpb.Header{
		RangeID: rangeB.RangeID,
	}, pArgs); err != nil {
		t.Fatal(err)
	}

	// Try to get the newly placed values.
	gArgs = getArgs([]byte("aaaa"))
	if reply, err := client.SendWrapped(ctx, store1.TestSender(), gArgs); err != nil {
		t.Fatal(err)
	} else if replyBytes, err := reply.(*roachpb.GetResponse).Value.GetBytes(); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(replyBytes, content) {
		t.Fatalf("actual value %q did not match expected value %q", replyBytes, content)
	}
	gArgs = getArgs([]byte("cccc"))
	if reply, err := client.SendWrapped(ctx, store1.TestSender(), gArgs); err != nil {
		t.Fatal(err)
	} else if replyBytes, err := reply.(*roachpb.GetResponse).Value.GetBytes(); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(replyBytes, content) {
		t.Fatalf("actual value %q did not match expected value %q", replyBytes, content)
	}

	gArgs = getArgs([]byte("cccc"))
	if _, err := client.SendWrappedWith(ctx, store2, roachpb.Header{
		RangeID: bDesc.RangeID,
	}, gArgs); !testutils.IsPError(
		err, `r2 was not found`,
	) {
		t.Fatalf("expected get on rhs to fail after merge, but got err=%v", err)
	}

	if atomic.LoadInt64(&retries) >= 0 {
		t.Fatalf("%d retries remaining (expected less than zero)", retries)
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
	if _, pErr := client.SendWrapped(context.Background(), store.TestSender(), args); !testutils.IsPError(pErr, "cannot merge final range") {
		t.Fatalf("expected 'cannot merge final range' error; got %s", pErr)
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
	ctx := context.Background()

	// Split the range.
	aDesc, bDesc, pErr := createSplitRanges(store)
	if pErr != nil {
		t.Fatal(pErr)
	}

	// Write some values left and right of the proposed split key.
	writeRandomDataToRange(t, store, aDesc.RangeID, []byte("aaa"))
	writeRandomDataToRange(t, store, bDesc.RangeID, []byte("ccc"))

	// Litter some abort span records. txn1 will leave a record on the LHS, txn2
	// will leave a record on the RHS, and txn3 will leave a record on both. This
	// tests whether the merge code properly accounts for merging abort span
	// records for the same transaction.
	txn1 := client.NewTxn(store.DB(), 0 /* gatewayNodeID */, client.RootTxn)
	if err := txn1.Put(ctx, "a-txn1", "val"); err != nil {
		t.Fatal(err)
	}
	txn2 := client.NewTxn(store.DB(), 0 /* gatewayNodeID */, client.RootTxn)
	if err := txn2.Put(ctx, "c-txn2", "val"); err != nil {
		t.Fatal(err)
	}
	txn3 := client.NewTxn(store.DB(), 0 /* gatewayNodeID */, client.RootTxn)
	if err := txn3.Put(ctx, "a-txn3", "val"); err != nil {
		t.Fatal(err)
	}
	if err := txn3.Put(ctx, "c-txn3", "val"); err != nil {
		t.Fatal(err)
	}
	hiPriTxn := client.NewTxn(store.DB(), 0 /* gatewayNodeID */, client.RootTxn)
	hiPriTxn.InternalSetPriority(roachpb.MaxTxnPriority)
	for _, key := range []string{"a-txn1", "c-txn2", "a-txn3", "c-txn3"} {
		if err := hiPriTxn.Put(ctx, key, "val"); err != nil {
			t.Fatal(err)
		}
	}
	if err := hiPriTxn.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	// Leave txn1-txn3 open so that their abort span records exist during the
	// merge below.

	// Get the range stats for both ranges now that we have data.
	snap := store.Engine().NewSnapshot()
	defer snap.Close()
	msA, err := stateloader.Make(nil /* st */, aDesc.RangeID).LoadMVCCStats(ctx, snap)
	if err != nil {
		t.Fatal(err)
	}
	msB, err := stateloader.Make(nil /* st */, bDesc.RangeID).LoadMVCCStats(ctx, snap)
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
	if _, err := client.SendWrapped(ctx, store.TestSender(), args); err != nil {
		t.Fatal(err)
	}
	replMerged := store.LookupReplica(aDesc.StartKey, nil)

	// Get the range stats for the merged range and verify.
	snap = store.Engine().NewSnapshot()
	defer snap.Close()
	msMerged, err := stateloader.Make(nil /* st */, replMerged.RangeID).LoadMVCCStats(ctx, snap)
	if err != nil {
		t.Fatal(err)
	}

	// Merged stats should agree with recomputation.
	if err := verifyRecomputedStats(snap, replMerged.Desc(), msMerged, manual.UnixNano()); err != nil {
		t.Errorf("failed to verify range's stats after merge: %v", err)
	}
}

func TestStoreRangeMergeInFlightTxns(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	sc := storage.TestStoreConfig(nil)
	sc.TestingKnobs.DisableReplicateQueue = true
	mtc := &multiTestContext{storeConfig: &sc}
	mtc.Start(t, 2)
	defer mtc.Stop()
	store := mtc.stores[0]

	// Create two adjacent ranges. The left-hand range has exactly one replica,
	// on the first store, and the right-hand range has exactly one replica,
	// on the second store
	setupReplicas := func() (lhsDesc, rhsDesc *roachpb.RangeDescriptor, err error) {
		lhsDesc, rhsDesc, pErr := createSplitRanges(store)
		if pErr != nil {
			return nil, nil, pErr.GoError()
		}
		mtc.replicateRange(rhsDesc.RangeID, 1)
		mtc.transferLease(ctx, rhsDesc.RangeID, 0, 1)
		mtc.unreplicateRange(rhsDesc.RangeID, 0)
		return lhsDesc, rhsDesc, nil
	}

	// Verify that a transaction can span a merge.
	t.Run("valid", func(t *testing.T) {
		lhsDesc, _, err := setupReplicas()
		if err != nil {
			t.Fatal(err)
		}
		lhsKey, rhsKey := roachpb.Key("aa"), roachpb.Key("cc")

		txn := client.NewTxn(store.DB(), 0 /* gatewayNodeID */, client.RootTxn)
		// Put the key on the RHS side first so ownership of the transaction record
		// will need to transfer to the LHS range during the merge.
		if err := txn.Put(ctx, rhsKey, t.Name()); err != nil {
			t.Fatal(err)
		}
		if err := txn.Put(ctx, lhsKey, t.Name()); err != nil {
			t.Fatal(err)
		}
		args := adminMergeArgs(lhsDesc.StartKey.AsRawKey())
		if _, err := client.SendWrapped(ctx, store.TestSender(), args); err != nil {
			t.Fatal(err)
		}
		if err := txn.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		for _, key := range []roachpb.Key{lhsKey, rhsKey} {
			kv, err := store.DB().Get(ctx, key)
			if err != nil {
				t.Fatal(err)
			} else if string(kv.ValueBytes()) != t.Name() {
				t.Fatalf("actual value %q did not match expected value %q", kv.ValueBytes(), t.Name())
			}
		}
	})

	// Verify that a transaction's abort span records are preserved when the
	// transaction spans a merge.
	t.Run("abort-span", func(t *testing.T) {
		lhsDesc, _, err := setupReplicas()
		if err != nil {
			t.Fatal(err)
		}
		rhsKey := roachpb.Key("cc")

		// Create a transaction that will be aborted before the merge but won't
		// realize until after the merge.
		txn1 := client.NewTxn(store.DB(), 0 /* gatewayNodeID */, client.RootTxn)
		// Put the key on the RHS side so ownership of the transaction record and
		// abort span records will need to transfer to the LHS during the merge.
		if err := txn1.Put(ctx, rhsKey, t.Name()); err != nil {
			t.Fatal(err)
		}

		// Create and commit a txn that aborts txn1.
		txn2 := client.NewTxn(store.DB(), 0 /* gatewayNodeID */, client.RootTxn)
		txn2.InternalSetPriority(roachpb.MaxTxnPriority)
		if err := txn2.Put(ctx, rhsKey, "muhahahah"); err != nil {
			t.Fatal(err)
		}
		if err := txn2.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		// Complete the merge.
		args := adminMergeArgs(lhsDesc.StartKey.AsRawKey())
		if _, err := client.SendWrapped(ctx, store.TestSender(), args); err != nil {
			t.Fatal(err)
		}
		if _, err := txn1.Get(ctx, rhsKey); !testutils.IsError(err, "txn aborted") {
			t.Fatalf("expected 'txn aborted' error but got %v", err)
		}
	})

	// Verify that the transaction wait queue on the right-hand range in a merge
	// is cleared if the merge commits.
	t.Run("wait-queue", func(t *testing.T) {
		lhsDesc, rhsDesc, err := setupReplicas()
		if err != nil {
			t.Fatal(err)
		}
		rhsKey := roachpb.Key("cc")

		// Set a timeout, and set the the transaction liveness threshold to
		// something much larger than our timeout. We want transactions to get stuck
		// in the transaction wait queue and trigger the timeout if we forget to
		// clear it.
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, testutils.DefaultSucceedsSoonDuration)
		defer cancel()
		defer func(old time.Duration) { txnwait.TxnLivenessThreshold = old }(txnwait.TxnLivenessThreshold)
		txnwait.TxnLivenessThreshold = 2 * testutils.DefaultSucceedsSoonDuration

		// Create a transaction that won't complete until after the merge.
		txn1 := client.NewTxn(store.DB(), 0 /* gatewayNodeID */, client.RootTxn)
		// Put the key on the RHS side so ownership of the transaction record and
		// abort span records will need to transfer to the LHS during the merge.
		if err := txn1.Put(ctx, rhsKey, t.Name()); err != nil {
			t.Fatal(err)
		}

		// Create a txn that will conflict with txn1.
		txn2 := client.NewTxn(store.DB(), 0 /* gatewayNodeID */, client.RootTxn)
		txn2ErrCh := make(chan error)
		go func() {
			txn2ErrCh <- txn2.Put(ctx, rhsKey, "muhahahah")
		}()

		// Wait for txn2 to realize it conflicts with txn1 and enter its wait queue.
		{
			repl, err := mtc.stores[1].GetReplica(rhsDesc.RangeID)
			if err != nil {
				t.Fatal(err)
			}
			for {
				if _, ok := repl.GetTxnWaitQueue().TrackedTxns()[txn1.ID()]; ok {
					break
				}
				select {
				case <-time.After(10 * time.Millisecond):
				case <-ctx.Done():
					t.Fatal("timed out waiting for txn2 to enter wait queue")
				}
			}
		}

		// Complete the merge.
		args := adminMergeArgs(lhsDesc.StartKey.AsRawKey())
		if _, err := client.SendWrapped(ctx, store.TestSender(), args); err != nil {
			t.Fatal(err)
		}

		if err := txn1.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		kv, pErr := store.DB().Get(ctx, rhsKey)
		if pErr != nil {
			t.Fatal(pErr)
		} else if string(kv.ValueBytes()) != t.Name() {
			t.Fatalf("actual value %q did not match expected value %q", kv.ValueBytes(), t.Name())
		}

		// Now that txn1 has committed, txn2's put operation should complete.
		select {
		case err := <-txn2ErrCh:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("timed out waiting for txn2 to complete put")
		}

		if err := txn2.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	})
}

// TestStoreRangeMergeRHSLeaseExpiration verifies that, if the right-hand range
// in a merge loses its lease while a merge is in progress, the new leaseholder
// does not incorrectly serve traffic before the merge completes.
func TestStoreRangeMergeRHSLeaseExpiration(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	storeCfg := storage.TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableReplicateQueue = true

	// The synchronization in this test is tricky. The merge transaction is
	// controlled by the AdminMerge function and normally commits quite quickly,
	// but we need to ensure an expiration of the RHS's lease occurs while the
	// merge transaction is open. To do so we install various hooks to observe
	// and control requests. It's easiest to understand these hooks after you've
	// read the meat of the test.

	// Install a hook to control when the merge transaction commits.
	mergeEndTxnReceived := make(chan struct{})
	finishMerge := make(chan struct{})
	storeCfg.TestingKnobs.TestingRequestFilter = func(ba roachpb.BatchRequest) *roachpb.Error {
		for _, r := range ba.Requests {
			if et := r.GetEndTransaction(); et != nil && et.InternalCommitTrigger.GetMergeTrigger() != nil {
				mergeEndTxnReceived <- struct{}{}
				<-finishMerge
			}
		}
		return nil
	}

	// Install a hook to observe when a get request for a special key,
	// rhsSentinel, exits the command queue.
	rhsSentinel := []byte("rhs-sentinel")
	getExitedCommandQueue := make(chan struct{})
	storeCfg.TestingKnobs.OnCommandQueueAction = func(ba *roachpb.BatchRequest, action storagebase.CommandQueueAction) {
		if action == storagebase.CommandQueueBeginExecuting {
			for _, r := range ba.Requests {
				if get := r.GetGet(); get != nil && get.RequestHeader.Key.Equal(rhsSentinel) {
					getExitedCommandQueue <- struct{}{}
				}
			}
		}
	}

	mtc := &multiTestContext{storeConfig: &storeCfg}
	mtc.Start(t, 2)
	defer mtc.Stop()

	// Create the ranges to be merged. Put the LHS on only the first store. Put
	// the RHS on both stores and give the second store the lease. The LHS is
	// largely irrelevant. What matters is that the RHS exists on two stores so we
	// can transfer its lease during the merge.
	lhsDesc, rhsDesc, err := createSplitRanges(mtc.stores[0])
	if err != nil {
		t.Fatal(err)
	}
	mtc.replicateRange(rhsDesc.RangeID, 1)
	mtc.transferLease(ctx, rhsDesc.RangeID, 0, 1)

	// Launch the merge.
	mergeErr := make(chan error)
	go func() {
		args := adminMergeArgs(lhsDesc.StartKey.AsRawKey())
		_, err := client.SendWrapped(ctx, mtc.stores[0].TestSender(), args)
		mergeErr <- err.GoError()
	}()

	// Wait for the merge transaction to send its EndTransaction request. It won't
	// be able to complete just yet, thanks to the hook we installed above.
	<-mergeEndTxnReceived

	// Now's our chance to move the lease on the RHS from the second store to the
	// first. This isn't entirely straightforward. The replica on the second store
	// is aware of the merge and is refusing all traffic, so we can't just send a
	// TransferLease request. Instead, we need to expire the second store's lease,
	// then acquire the lease on the first store.

	// Turn off liveness heartbeats on the second store, then advance the clock
	// past the liveness expiration time. This expires all leases on all stores.
	mtc.nodeLivenesses[1].PauseHeartbeat(true)
	mtc.advanceClock(ctx)

	// Manually heartbeat the liveness on the first store to ensure it's
	// considered live. The automatic heartbeat might not come for a while.
	if err := mtc.heartbeatLiveness(ctx, 0); err != nil {
		t.Fatal(err)
	}

	// Send a get request directly to the first store's replica of the RHS. It
	// should notice the second store's lease is expired and try to acquire it.
	getErr := make(chan error)
	go func() {
		_, err := client.SendWrappedWith(ctx, mtc.stores[0].TestSender(), roachpb.Header{
			RangeID: rhsDesc.RangeID,
		}, getArgs(rhsSentinel))
		getErr <- err.GoError()
	}()

	// Wait for the get request to fall out of the command queue, which is as far
	// as it the request can get while the merge is in progress. Then wait a
	// little bit longer. This tests that the request really does get stuck
	// waiting for the merge to complete without depending too heavily on
	// implementation details.
	<-getExitedCommandQueue
	time.Sleep(50 * time.Millisecond)

	// Finally, allow the merge to complete. It should complete successfully.
	finishMerge <- struct{}{}
	if err := <-mergeErr; err != nil {
		t.Fatal(err)
	}

	// Because the merge completed successfully, r2 has ceased to exist. We
	// therefore *must* see a RangeNotFound error. Anything else is a consistency
	// error (or a bug in the test).
	if err := <-getErr; !testutils.IsError(err, "r2 was not found") {
		t.Fatalf("expected RangeNotFound error from get during merge, but got %v", err)
	}
}

func TestInvalidGetSnapshotForMergeRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()

	storeCfg := storage.TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableSplitQueue = true
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	store := createTestStoreWithConfig(t, stopper, storeCfg)

	// A GetSnapshotForMerge request that succeeds when it shouldn't will wedge a
	// store because it waits for a merge that is not actually in progress. Set a
	// short timeout to limit the damage.
	ctx, cancel := context.WithTimeout(context.Background(), testutils.DefaultSucceedsSoonDuration)
	defer cancel()

	aDesc, bDesc, pErr := createSplitRanges(store)
	if pErr != nil {
		t.Fatal(pErr)
	}

	getSnapArgs := roachpb.GetSnapshotForMergeRequest{
		RequestHeader: roachpb.RequestHeader{Key: bDesc.StartKey.AsRawKey()},
		LeftRange:     *aDesc,
	}

	// GetSnapshotForMerge from a non-neighboring LHS should fail.
	{
		badArgs := getSnapArgs
		badArgs.LeftRange.EndKey = append(roachpb.RKey{}, badArgs.LeftRange.EndKey...).Next()
		_, pErr = client.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{
			RangeID: bDesc.RangeID,
		}, &badArgs)
		if exp := "ranges are not adjacent"; !testutils.IsPError(pErr, exp) {
			t.Fatalf("expected %q error, but got %v", exp, pErr)
		}
	}

	// GetSnapshotForMerge without an intent on the local range descriptor should fail.
	_, pErr = client.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{
		RangeID: bDesc.RangeID,
	}, &getSnapArgs)
	if exp := "range missing intent on its local descriptor"; !testutils.IsPError(pErr, exp) {
		t.Fatalf("expected %q error, but got %v", exp, pErr)
	}

	// GetSnapshotForMerge when a non-deletion intent is present on the
	// local range descriptor should fail.
	err := store.DB().Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		if err := txn.Put(ctx, keys.RangeDescriptorKey(bDesc.StartKey), "garbage"); err != nil {
			return err
		}
		// NB: GetSnapshotForMerge intentionally takes place outside of the txn so
		// that it sees an intent rather than the value the txn just wrote.
		_, pErr := client.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{
			RangeID: bDesc.RangeID,
		}, &getSnapArgs)
		if exp := "non-deletion intent on local range descriptor"; !testutils.IsPError(pErr, exp) {
			return fmt.Errorf("expected %q error, but got %v", exp, pErr)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func BenchmarkStoreRangeMerge(b *testing.B) {
	storeCfg := storage.TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableSplitQueue = true
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	store := createTestStoreWithConfig(b, stopper, storeCfg)

	// Perform initial split of ranges.
	sArgs := adminSplitArgs(roachpb.Key("b"))
	if _, err := client.SendWrapped(context.Background(), store.TestSender(), sArgs); err != nil {
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
		if _, err := client.SendWrapped(context.Background(), store.TestSender(), mArgs); err != nil {
			b.Fatal(err)
		}

		// Split the range.
		b.StopTimer()
		if _, err := client.SendWrapped(context.Background(), store.TestSender(), sArgs); err != nil {
			b.Fatal(err)
		}
	}
}
