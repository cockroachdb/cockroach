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
	"math/rand"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/stateloader"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/storage/txnwait"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

func adminMergeArgs(key roachpb.Key) *roachpb.AdminMergeRequest {
	return &roachpb.AdminMergeRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: key,
		},
	}
}

// createSplitRanges issues an AdminSplit command for the key "b". It returns
// the descriptors for the ranges to the left and right of the split.
func createSplitRanges(
	ctx context.Context, store *storage.Store,
) (*roachpb.RangeDescriptor, *roachpb.RangeDescriptor, error) {
	args := adminSplitArgs(roachpb.Key("b"))
	if _, err := client.SendWrapped(ctx, store.TestSender(), args); err != nil {
		return nil, nil, err.GoError()
	}

	lhsDesc := store.LookupReplica(roachpb.RKey("a"), nil).Desc()
	rhsDesc := store.LookupReplica(roachpb.RKey("c"), nil).Desc()

	if bytes.Equal(lhsDesc.StartKey, rhsDesc.StartKey) {
		return nil, nil, fmt.Errorf("split ranges have the same start key: %q = %q",
			lhsDesc.StartKey, rhsDesc.StartKey)
	}

	return lhsDesc, rhsDesc, nil
}

// TestStoreRangeMergeTwoEmptyRanges tries to merge two empty ranges together.
func TestStoreRangeMergeTwoEmptyRanges(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	storeCfg := storage.TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableSplitQueue = true
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	store := createTestStoreWithConfig(t, stopper, storeCfg)

	lhsDesc, _, err := createSplitRanges(ctx, store)
	if err != nil {
		t.Fatal(err)
	}

	// Merge the RHS back into the LHS.
	args := adminMergeArgs(lhsDesc.StartKey.AsRawKey())
	_, pErr := client.SendWrapped(ctx, store.TestSender(), args)
	if pErr != nil {
		t.Fatal(pErr)
	}

	// Verify the merge by looking up keys from both ranges.
	lhsRepl := store.LookupReplica(roachpb.RKey("a"), nil)
	rhsRepl := store.LookupReplica(roachpb.RKey("c"), nil)

	if !reflect.DeepEqual(lhsRepl, rhsRepl) {
		t.Fatalf("ranges were not merged: %s != %s", lhsRepl, rhsRepl)
	}
}

// TestStoreRangeMergeMetadataCleanup tests that all metadata of a
// subsumed range is cleaned up on merge.
func TestStoreRangeMergeMetadataCleanup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	storeCfg := storage.TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableSplitQueue = true
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
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
	pArgs := putArgs(roachpb.Key("aaa"), content)
	if _, pErr := client.SendWrapped(ctx, store.TestSender(), pArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Collect all the keys.
	preKeys := scan()

	// Split the range.
	lhsDesc, rhsDesc, err := createSplitRanges(ctx, store)
	if err != nil {
		t.Fatal(err)
	}

	// Write some values right of the split key.
	pArgs = putArgs(roachpb.Key("ccc"), content)
	if _, pErr := client.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{
		RangeID: rhsDesc.RangeID,
	}, pArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Merge the b range back into the a range.
	args := adminMergeArgs(lhsDesc.StartKey.AsRawKey())
	if _, pErr := client.SendWrapped(ctx, store.TestSender(), args); pErr != nil {
		t.Fatal(pErr)
	}

	// Collect all the keys again.
	postKeys := scan()

	// Compute the new keys.
	for k := range preKeys {
		delete(postKeys, k)
	}

	tombstoneKey := string(keys.RaftTombstoneKey(rhsDesc.RangeID))
	if _, ok := postKeys[tombstoneKey]; !ok {
		t.Errorf("tombstone key (%s) missing after merge", roachpb.Key(tombstoneKey))
	}
	delete(postKeys, tombstoneKey)

	// Keep only the subsumed range's local keys.
	localRangeKeyPrefix := string(keys.MakeRangeIDPrefix(rhsDesc.RangeID))
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

// TestStoreRangeMergeWithData attempts to merge two ranges, each containing
// data.
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
	storeCfg := storage.TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableReplicateQueue = true

	// Maybe inject some retryable errors when the merge transaction commits.
	var mtc *multiTestContext
	storeCfg.TestingKnobs.TestingRequestFilter = func(ba roachpb.BatchRequest) *roachpb.Error {
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

	mtc = &multiTestContext{storeConfig: &storeCfg}

	var store1, store2 *storage.Store
	if colocate {
		mtc.Start(t, 1)
		store1, store2 = mtc.stores[0], mtc.stores[0]
	} else {
		mtc.Start(t, 2)
		store1, store2 = mtc.stores[0], mtc.stores[1]
	}
	defer mtc.Stop()

	lhsDesc, rhsDesc, pErr := createSplitRanges(ctx, store1)
	if pErr != nil {
		t.Fatal(pErr)
	}

	if !colocate {
		mtc.replicateRange(rhsDesc.RangeID, 1)
		mtc.transferLease(ctx, rhsDesc.RangeID, 0, 1)
		mtc.unreplicateRange(rhsDesc.RangeID, 0)
	}

	content := []byte("testing!")

	// Write some values left and right of the proposed split key.
	pArgs := putArgs(roachpb.Key("aaa"), content)
	if _, pErr := client.SendWrapped(ctx, store1.TestSender(), pArgs); pErr != nil {
		t.Fatal(pErr)
	}
	pArgs = putArgs(roachpb.Key("ccc"), content)
	if _, pErr := client.SendWrappedWith(ctx, store2.TestSender(), roachpb.Header{
		RangeID: rhsDesc.RangeID,
	}, pArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Confirm the values are there.
	gArgs := getArgs(roachpb.Key("aaa"))
	if reply, pErr := client.SendWrapped(ctx, store1.TestSender(), gArgs); pErr != nil {
		t.Fatal(pErr)
	} else if replyBytes, err := reply.(*roachpb.GetResponse).Value.GetBytes(); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(replyBytes, content) {
		t.Fatalf("actual value %q did not match expected value %q", replyBytes, content)
	}
	gArgs = getArgs(roachpb.Key("ccc"))
	if reply, pErr := client.SendWrappedWith(ctx, store2.TestSender(), roachpb.Header{
		RangeID: rhsDesc.RangeID,
	}, gArgs); pErr != nil {
		t.Fatal(pErr)
	} else if replyBytes, err := reply.(*roachpb.GetResponse).Value.GetBytes(); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(replyBytes, content) {
		t.Fatalf("actual value %q did not match expected value %q", replyBytes, content)
	}

	// Merge the b range back into the a range.
	args := adminMergeArgs(lhsDesc.StartKey.AsRawKey())
	if _, pErr := client.SendWrapped(ctx, store1.TestSender(), args); pErr != nil {
		t.Fatal(pErr)
	}

	// Verify no intents remains on range descriptor keys.
	for _, key := range []roachpb.Key{keys.RangeDescriptorKey(lhsDesc.StartKey), keys.RangeDescriptorKey(rhsDesc.StartKey)} {
		if _, _, err := engine.MVCCGet(ctx, store1.Engine(), key, store1.Clock().Now(), true, nil); err != nil {
			t.Fatal(err)
		}
	}

	// Verify the merge by looking up keys from both ranges.
	lhsRepl := store1.LookupReplica(roachpb.RKey("a"), nil)
	rhsRepl := store1.LookupReplica(roachpb.RKey("c"), nil)

	if lhsRepl != rhsRepl {
		t.Fatalf("ranges were not merged %+v=%+v", lhsRepl.Desc(), rhsRepl.Desc())
	}
	if startKey := lhsRepl.Desc().StartKey; !bytes.Equal(startKey, roachpb.RKeyMin) {
		t.Fatalf("The start key is not equal to KeyMin %q=%q", startKey, roachpb.RKeyMin)
	}
	if endKey := rhsRepl.Desc().EndKey; !bytes.Equal(endKey, roachpb.RKeyMax) {
		t.Fatalf("The end key is not equal to KeyMax %q=%q", endKey, roachpb.RKeyMax)
	}

	// Try to get values from after the merge.
	gArgs = getArgs(roachpb.Key("aaa"))
	if reply, pErr := client.SendWrapped(ctx, store1.TestSender(), gArgs); pErr != nil {
		t.Fatal(pErr)
	} else if replyBytes, err := reply.(*roachpb.GetResponse).Value.GetBytes(); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(replyBytes, content) {
		t.Fatalf("actual value %q did not match expected value %q", replyBytes, content)
	}
	gArgs = getArgs(roachpb.Key("ccc"))
	if reply, pErr := client.SendWrappedWith(ctx, store1.TestSender(), roachpb.Header{
		RangeID: rhsRepl.RangeID,
	}, gArgs); pErr != nil {
		t.Fatal(pErr)
	} else if replyBytes, err := reply.(*roachpb.GetResponse).Value.GetBytes(); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(replyBytes, content) {
		t.Fatalf("actual value %q did not match expected value %q", replyBytes, content)
	}

	// Put new values after the merge on both sides.
	pArgs = putArgs(roachpb.Key("aaaa"), content)
	if _, pErr := client.SendWrapped(ctx, store1.TestSender(), pArgs); pErr != nil {
		t.Fatal(pErr)
	}
	pArgs = putArgs(roachpb.Key("cccc"), content)
	if _, pErr := client.SendWrappedWith(ctx, store1.TestSender(), roachpb.Header{
		RangeID: rhsRepl.RangeID,
	}, pArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Try to get the newly placed values.
	gArgs = getArgs(roachpb.Key("aaaa"))
	if reply, pErr := client.SendWrapped(ctx, store1.TestSender(), gArgs); pErr != nil {
		t.Fatal(pErr)
	} else if replyBytes, err := reply.(*roachpb.GetResponse).Value.GetBytes(); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(replyBytes, content) {
		t.Fatalf("actual value %q did not match expected value %q", replyBytes, content)
	}
	gArgs = getArgs(roachpb.Key("cccc"))
	if reply, pErr := client.SendWrapped(ctx, store1.TestSender(), gArgs); pErr != nil {
		t.Fatal(pErr)
	} else if replyBytes, err := reply.(*roachpb.GetResponse).Value.GetBytes(); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(replyBytes, content) {
		t.Fatalf("actual value %q did not match expected value %q", replyBytes, content)
	}

	gArgs = getArgs(roachpb.Key("cccc"))
	if _, pErr := client.SendWrappedWith(ctx, store2, roachpb.Header{
		RangeID: rhsDesc.RangeID,
	}, gArgs); !testutils.IsPError(
		pErr, `r2 was not found`,
	) {
		t.Fatalf("expected get on rhs to fail after merge, but got err=%v", pErr)
	}

	if atomic.LoadInt64(&retries) >= 0 {
		t.Fatalf("%d retries remaining (expected less than zero)", retries)
	}
}

// TestStoreRangeMergeLastRange verifies that merging the last range fails.
func TestStoreRangeMergeLastRange(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	storeCfg := storage.TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableSplitQueue = true
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	store := createTestStoreWithConfig(t, stopper, storeCfg)

	// Merge last range.
	_, pErr := client.SendWrapped(ctx, store.TestSender(), adminMergeArgs(roachpb.KeyMin))
	if !testutils.IsPError(pErr, "cannot merge final range") {
		t.Fatalf("expected 'cannot merge final range' error; got %s", pErr)
	}
}

func TestStoreRangeMergeTxnFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	storeCfg := storage.TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableSplitQueue = true

	// Install a store filter that maybe injects retryable errors into a merge
	// transaction before ultimately aborting the merge.
	var retriesBeforeFailure int64
	storeCfg.TestingKnobs.TestingRequestFilter = func(ba roachpb.BatchRequest) *roachpb.Error {
		for _, req := range ba.Requests {
			if et := req.GetEndTransaction(); et != nil && et.InternalCommitTrigger.GetMergeTrigger() != nil {
				if atomic.AddInt64(&retriesBeforeFailure, -1) >= 0 {
					return roachpb.NewError(roachpb.NewTransactionRetryError(roachpb.RETRY_SERIALIZABLE))
				}
				return roachpb.NewError(errors.New("injected permafail"))
			}
		}
		return nil
	}

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	store := createTestStoreWithConfig(t, stopper, storeCfg)
	kvDB := store.DB()

	if err := kvDB.Put(ctx, "aa", "val"); err != nil {
		t.Fatal(err)
	}
	if err := kvDB.Put(ctx, "cc", "val"); err != nil {
		t.Fatal(err)
	}
	lhsDesc, rhsDesc, err := createSplitRanges(ctx, store)
	if err != nil {
		t.Fatal(err)
	}

	verifyLHSAndRHSLive := func() {
		t.Helper()
		for _, tc := range []struct {
			rangeID roachpb.RangeID
			key     roachpb.Key
		}{
			{lhsDesc.RangeID, roachpb.Key("aa")},
			{rhsDesc.RangeID, roachpb.Key("cc")},
		} {
			if reply, pErr := client.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{
				RangeID: tc.rangeID,
			}, getArgs(tc.key)); pErr != nil {
				t.Fatal(pErr)
			} else if replyBytes, err := reply.(*roachpb.GetResponse).Value.GetBytes(); err != nil {
				t.Fatal(err)
			} else if !bytes.Equal(replyBytes, []byte("val")) {
				t.Fatalf("actual value %q did not match expected value %q", replyBytes, []byte("val"))
			}
		}
	}

	attemptMerge := func() {
		t.Helper()
		args := adminMergeArgs(lhsDesc.StartKey.AsRawKey())
		_, pErr := client.SendWrapped(ctx, store.TestSender(), args)
		if exp := "injected permafail"; !testutils.IsPError(pErr, exp) {
			t.Fatalf("expected %q error, but got %q", exp, pErr)
		}
	}

	verifyLHSAndRHSLive()

	atomic.StoreInt64(&retriesBeforeFailure, 0)
	attemptMerge()
	verifyLHSAndRHSLive()
	if atomic.LoadInt64(&retriesBeforeFailure) >= 0 {
		t.Fatalf("%d retries remaining (expected less than zero)", retriesBeforeFailure)
	}

	atomic.StoreInt64(&retriesBeforeFailure, 3)
	attemptMerge()
	verifyLHSAndRHSLive()
	if atomic.LoadInt64(&retriesBeforeFailure) >= 0 {
		t.Fatalf("%d retries remaining (expected less than zero)", retriesBeforeFailure)
	}
}

// TestStoreRangeMergeStats starts by splitting a range, then writing random
// data to both sides of the split. It then merges the ranges and verifies the
// merged range has stats consistent with recomputations.
func TestStoreRangeMergeStats(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	manual := hlc.NewManualClock(123)
	storeCfg := storage.TestStoreConfig(hlc.NewClock(manual.UnixNano, time.Nanosecond))
	storeCfg.TestingKnobs.DisableSplitQueue = true
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	store := createTestStoreWithConfig(t, stopper, storeCfg)

	// Split the range.
	lhsDesc, rhsDesc, err := createSplitRanges(ctx, store)
	if err != nil {
		t.Fatal(err)
	}

	// Write some values left and right of the proposed split key.
	writeRandomDataToRange(t, store, lhsDesc.RangeID, []byte("aaa"))
	writeRandomDataToRange(t, store, rhsDesc.RangeID, []byte("ccc"))

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
	msA, err := stateloader.Make(nil /* st */, lhsDesc.RangeID).LoadMVCCStats(ctx, snap)
	if err != nil {
		t.Fatal(err)
	}
	msB, err := stateloader.Make(nil /* st */, rhsDesc.RangeID).LoadMVCCStats(ctx, snap)
	if err != nil {
		t.Fatal(err)
	}

	// Stats should agree with recomputation.
	if err := verifyRecomputedStats(snap, lhsDesc, msA, manual.UnixNano()); err != nil {
		t.Fatalf("failed to verify range A's stats before split: %v", err)
	}
	if err := verifyRecomputedStats(snap, rhsDesc, msB, manual.UnixNano()); err != nil {
		t.Fatalf("failed to verify range B's stats before split: %v", err)
	}

	manual.Increment(100)

	// Merge the b range back into the a range.
	args := adminMergeArgs(lhsDesc.StartKey.AsRawKey())
	if _, err := client.SendWrapped(ctx, store.TestSender(), args); err != nil {
		t.Fatal(err)
	}
	replMerged := store.LookupReplica(lhsDesc.StartKey, nil)

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
	storeCfg := storage.TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableReplicateQueue = true
	mtc := &multiTestContext{storeConfig: &storeCfg}
	mtc.Start(t, 2)
	defer mtc.Stop()
	store := mtc.stores[0]

	// Create two adjacent ranges. The left-hand range has exactly one replica,
	// on the first store, and the right-hand range has exactly one replica,
	// on the second store
	setupReplicas := func() (lhsDesc, rhsDesc *roachpb.RangeDescriptor, err error) {
		lhsDesc, rhsDesc, err = createSplitRanges(ctx, store)
		if err != nil {
			return nil, nil, err
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
		if _, pErr := client.SendWrapped(ctx, store.TestSender(), args); pErr != nil {
			t.Fatal(pErr)
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
		if _, pErr := client.SendWrapped(ctx, store.TestSender(), args); pErr != nil {
			t.Fatal(pErr)
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
		if _, pErr := client.SendWrapped(ctx, store.TestSender(), args); pErr != nil {
			t.Fatal(pErr)
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
	rhsSentinel := roachpb.Key("rhs-sentinel")
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
	lhsDesc, rhsDesc, err := createSplitRanges(ctx, mtc.stores[0])
	if err != nil {
		t.Fatal(err)
	}
	mtc.replicateRange(rhsDesc.RangeID, 1)
	mtc.transferLease(ctx, rhsDesc.RangeID, 0, 1)

	// Launch the merge.
	mergeErr := make(chan error)
	go func() {
		args := adminMergeArgs(lhsDesc.StartKey.AsRawKey())
		_, pErr := client.SendWrapped(ctx, mtc.stores[0].TestSender(), args)
		mergeErr <- pErr.GoError()
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
		_, pErr := client.SendWrappedWith(ctx, mtc.stores[0].TestSender(), roachpb.Header{
			RangeID: rhsDesc.RangeID,
		}, getArgs(rhsSentinel))
		getErr <- pErr.GoError()
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

// TestStoreRangeMergeConcurrentRequests tests merging ranges that are serving
// other traffic concurrently.
func TestStoreRangeMergeConcurrentRequests(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	storeCfg := storage.TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableSplitQueue = true
	storeCfg.TestingKnobs.DisableReplicateQueue = true

	var mtc *multiTestContext
	storeCfg.TestingKnobs.TestingRequestFilter = func(ba roachpb.BatchRequest) *roachpb.Error {
		if ba.IsSingleGetSnapshotForMergeRequest() && rand.Int()%4 == 0 {
			// Before every few GetSnapshotForMerge requests, expire all range leases.
			// This makes the following sequence of events quite likely:
			//
			//     1. The merge transaction begins and lays down deletion intents for
			//        the meta2 and local copies of the RHS range descriptor.
			//     2. The RHS replica loses its lease, thanks to the following call to
			//        mtc.advanceClock.
			//     3. A Get request arrives at the RHS replica and triggers a
			//        synchronous lease acquisition. The lease acquisition notices
			//        that a merge is in progress and installs a mergeComplete
			//        channel.
			//     4. The Get request blocks on the newly installed mergeComplete
			//        channel.
			//     5. The GetSnapshotForMerge request arrives.
			//
			// This scenario previously caused deadlock. The merge was not able to
			// complete until the GetSnapshotForMerge request completed, but the
			// GetSnapshotForMerge request was stuck in the command queue until the
			// Get request finished, which was itself waiting for the merge to
			// complete. Whoops!
			mtc.advanceClock(ctx)
		}
		return nil
	}

	mtc = &multiTestContext{storeConfig: &storeCfg}
	mtc.Start(t, 1)
	defer mtc.Stop()
	store := mtc.Store(0)

	keys := []roachpb.Key{
		roachpb.Key("a1"), roachpb.Key("a2"), roachpb.Key("a3"),
		roachpb.Key("c1"), roachpb.Key("c2"), roachpb.Key("c3"),
	}

	for _, k := range keys {
		if err := store.DB().Put(ctx, k, "val"); err != nil {
			t.Fatal(err)
		}
	}

	// Failures in this test often present as a deadlock. Set a short timeout to
	// limit the damage.
	ctx, cancel := context.WithTimeout(ctx, testutils.DefaultSucceedsSoonDuration)
	defer cancel()

	const numGetWorkers = 16
	const numMerges = 16

	var numGets int64
	doneCh := make(chan struct{})
	g := ctxgroup.WithContext(ctx)
	for i := 0; i < numGetWorkers; i++ {
		g.GoCtx(func(ctx context.Context) error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-doneCh:
					return nil
				default:
				}
				key := keys[rand.Intn(len(keys))]
				if kv, err := store.DB().Get(ctx, key); err != nil {
					return err
				} else if v := string(kv.ValueBytes()); v != "val" {
					return fmt.Errorf(`expected "val", but got %q`, v)
				}
				atomic.AddInt64(&numGets, 1)
			}
		})
	}

	for i := 0; i < numMerges; i++ {
		lhsDesc, _, err := createSplitRanges(ctx, store)
		if err != nil {
			t.Fatal(err)
		}
		args := adminMergeArgs(lhsDesc.StartKey.AsRawKey())
		if _, pErr := client.SendWrapped(ctx, store.TestSender(), args); pErr != nil {
			t.Fatal(pErr)
		}
	}

	close(doneCh)
	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}

	// Expect that each worker was able to issue one at least one get request
	// during every split/merge cycle. Empirical evidence suggests that this a
	// very conservative estimate that is unlikely to be flaky.
	if n := atomic.LoadInt64(&numGets); n < numGetWorkers*numMerges {
		t.Fatalf("suspiciously low numGets (expected at least %d): %d", numGetWorkers*numMerges, n)
	}
}

// TestStoreRangeMergeDuringShutdown verifies that a shutdown of a store
// containing the RHS of a merge can occur cleanly. This previously triggered
// a fatal error (#27552).
func TestStoreRangeMergeDuringShutdown(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	storeCfg := storage.TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableSplitQueue = true
	storeCfg.TestingKnobs.DisableReplicateQueue = true

	// Install a filter that triggers a shutdown when stop is non-zero and the
	// rhsDesc requests a new lease.
	var mtc *multiTestContext
	var rhsDesc *roachpb.RangeDescriptor
	var stop int32
	storeCfg.TestingKnobs.TestingPostApplyFilter = func(args storagebase.ApplyFilterArgs) *roachpb.Error {
		if atomic.LoadInt32(&stop) != 0 && args.RangeID == rhsDesc.RangeID && args.IsLeaseRequest {
			// Shut down the store. The lease acquisition will notice that a merge is
			// in progress and attempt to run a task to watch for its completion.
			// Shutting down the store before running leasePostApply will prevent that
			// task from launching. This error path would previously fatal a node
			// incorrectly (#27552).
			go mtc.Stop()
			// Sleep to give the shutdown time to propagate. The test appeared to work
			// without this sleep, but best to be somewhat robust to different
			// goroutine schedules.
			time.Sleep(10 * time.Millisecond)
		}
		return nil
	}

	mtc = &multiTestContext{storeConfig: &storeCfg}
	mtc.Start(t, 1)
	store := mtc.Store(0)
	stopper := mtc.engineStoppers[0]

	var err error
	_, rhsDesc, err = createSplitRanges(ctx, store)
	if err != nil {
		t.Fatal(err)
	}

	// Simulate a merge transaction by launching a transaction that lays down
	// intents on the two copies of the RHS range descriptor.
	txn := client.NewTxn(store.DB(), 0 /* gatewayNodeID */, client.RootTxn)
	if err := txn.Del(ctx, keys.RangeDescriptorKey(rhsDesc.StartKey)); err != nil {
		t.Fatal(err)
	}
	if err := txn.Del(ctx, keys.RangeMetaKey(rhsDesc.StartKey)); err != nil {
		t.Fatal(err)
	}

	// Indicate to the store filter installed above that the next lease
	// acquisition for the RHS should trigger a shutdown.
	atomic.StoreInt32(&stop, 1)

	// Expire all leases.
	mtc.advanceClock(ctx)

	// Send a dummy get request on the RHS to force a lease acquisition. We expect
	// this to fail, as quiescing stores cannot acquire leases.
	err = stopper.RunTaskWithErr(ctx, "test-get-rhs-key", func(ctx context.Context) error {
		_, err := store.DB().Get(ctx, "dummy-rhs-key")
		return err
	})
	if exp := "not lease holder"; !testutils.IsError(err, exp) {
		t.Fatalf("expected %q error, but got %v", err, exp)
	}
}

func TestInvalidGetSnapshotForMergeRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	storeCfg := storage.TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableSplitQueue = true
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	store := createTestStoreWithConfig(t, stopper, storeCfg)

	// A GetSnapshotForMerge request that succeeds when it shouldn't will wedge a
	// store because it waits for a merge that is not actually in progress. Set a
	// short timeout to limit the damage.
	ctx, cancel := context.WithTimeout(ctx, testutils.DefaultSucceedsSoonDuration)
	defer cancel()

	lhsDesc, rhsDesc, err := createSplitRanges(ctx, store)
	if err != nil {
		t.Fatal(err)
	}

	getSnapArgs := roachpb.GetSnapshotForMergeRequest{
		RequestHeader: roachpb.RequestHeader{Key: rhsDesc.StartKey.AsRawKey()},
		LeftRange:     *lhsDesc,
	}

	// GetSnapshotForMerge from a non-neighboring LHS should fail.
	{
		badArgs := getSnapArgs
		badArgs.LeftRange.EndKey = append(roachpb.RKey{}, badArgs.LeftRange.EndKey...).Next()
		_, pErr := client.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{
			RangeID: rhsDesc.RangeID,
		}, &badArgs)
		if exp := "ranges are not adjacent"; !testutils.IsPError(pErr, exp) {
			t.Fatalf("expected %q error, but got %v", exp, pErr)
		}
	}

	// GetSnapshotForMerge without an intent on the local range descriptor should fail.
	_, pErr := client.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{
		RangeID: rhsDesc.RangeID,
	}, &getSnapArgs)
	if exp := "range missing intent on its local descriptor"; !testutils.IsPError(pErr, exp) {
		t.Fatalf("expected %q error, but got %v", exp, pErr)
	}

	// GetSnapshotForMerge when a non-deletion intent is present on the
	// local range descriptor should fail.
	err = store.DB().Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		if err := txn.Put(ctx, keys.RangeDescriptorKey(rhsDesc.StartKey), "garbage"); err != nil {
			return err
		}
		// NB: GetSnapshotForMerge intentionally takes place outside of the txn so
		// that it sees an intent rather than the value the txn just wrote.
		_, pErr := client.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{
			RangeID: rhsDesc.RangeID,
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
	ctx := context.Background()
	storeCfg := storage.TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableSplitQueue = true
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	store := createTestStoreWithConfig(b, stopper, storeCfg)

	lhsDesc, rhsDesc, err := createSplitRanges(ctx, store)
	if err != nil {
		b.Fatal(err)
	}

	// Write some values left and right of the proposed split key.
	writeRandomDataToRange(b, store, lhsDesc.RangeID, []byte("aaa"))
	writeRandomDataToRange(b, store, rhsDesc.RangeID, []byte("ccc"))

	// Create args to merge the b range back into the a range.
	mArgs := adminMergeArgs(lhsDesc.StartKey.AsRawKey())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Merge the ranges.
		b.StartTimer()
		if _, err := client.SendWrapped(ctx, store.TestSender(), mArgs); err != nil {
			b.Fatal(err)
		}

		// Split the range.
		b.StopTimer()
		if _, _, err := createSplitRanges(ctx, store); err != nil {
			b.Fatal(err)
		}
	}
}
