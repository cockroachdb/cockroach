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
	"math"
	"math/rand"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/stateloader"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/storage/txnwait"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
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

	lhsDesc := store.LookupReplica(roachpb.RKey("a")).Desc()
	rhsDesc := store.LookupReplica(roachpb.RKey("c")).Desc()

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
	var mtc multiTestContext
	mtc.Start(t, 1)
	defer mtc.Stop()
	store := mtc.Store(0)

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
	lhsRepl := store.LookupReplica(roachpb.RKey("a"))
	rhsRepl := store.LookupReplica(roachpb.RKey("c"))

	if !reflect.DeepEqual(lhsRepl, rhsRepl) {
		t.Fatalf("ranges were not merged: %s != %s", lhsRepl, rhsRepl)
	}

	// The LHS has been split once and merged once, so it should have received
	// two generation bumps.
	if e, a := int64(2), lhsRepl.Desc().GetGeneration(); e != a {
		t.Fatalf("expected LHS to have generation %d, but got %d", e, a)
	}
}

func getEngineKeySet(t *testing.T, e engine.Engine) map[string]struct{} {
	t.Helper()
	kvs, err := engine.Scan(e, engine.NilKey, engine.MVCCKeyMax, 0 /* max */)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]struct{}{}
	for _, kv := range kvs {
		out[string(kv.Key.Key)] = struct{}{}
	}
	return out
}

// TestStoreRangeMergeMetadataCleanup tests that all metadata of a
// subsumed range is cleaned up on merge.
func TestStoreRangeMergeMetadataCleanup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	var mtc multiTestContext
	mtc.Start(t, 1)
	defer mtc.Stop()
	store := mtc.Store(0)

	content := roachpb.Key("testing!")

	// Write some values left of the proposed split key.
	pArgs := putArgs(roachpb.Key("aaa"), content)
	if _, pErr := client.SendWrapped(ctx, store.TestSender(), pArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Collect all the keys.
	preKeys := getEngineKeySet(t, store.Engine())

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
	postKeys := getEngineKeySet(t, store.Engine())

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

	for _, retries := range []int64{0, 3} {
		t.Run(fmt.Sprintf("retries=%d", retries), func(t *testing.T) {
			mergeWithData(t, retries)
		})
	}
}

func mergeWithData(t *testing.T, retries int64) {
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
	mtc.Start(t, 1)
	store1, store2 = mtc.stores[0], mtc.stores[0]
	defer mtc.Stop()

	lhsDesc, rhsDesc, pErr := createSplitRanges(ctx, store1)
	if pErr != nil {
		t.Fatal(pErr)
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
	lhsRepl := store1.LookupReplica(roachpb.RKey("a"))
	rhsRepl := store1.LookupReplica(roachpb.RKey("c"))

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
	var mtc multiTestContext
	mtc.Start(t, 1)
	defer mtc.Stop()
	store := mtc.Store(0)

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

	mtc := &multiTestContext{storeConfig: &storeCfg}
	mtc.Start(t, 1)
	defer mtc.Stop()
	store := mtc.Store(0)
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
	mtc := &multiTestContext{}
	mtc.Start(t, 1)
	defer mtc.Stop()
	store := mtc.Store(0)

	// Split the range.
	lhsDesc, rhsDesc, err := createSplitRanges(ctx, store)
	if err != nil {
		t.Fatal(err)
	}

	// Write some values left and right of the proposed split key.
	storage.WriteRandomDataToRange(t, store, lhsDesc.RangeID, []byte("aaa"))
	storage.WriteRandomDataToRange(t, store, rhsDesc.RangeID, []byte("ccc"))

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
	if err := verifyRecomputedStats(snap, lhsDesc, msA, mtc.manualClock.UnixNano()); err != nil {
		t.Fatalf("failed to verify range A's stats before split: %v", err)
	}
	if err := verifyRecomputedStats(snap, rhsDesc, msB, mtc.manualClock.UnixNano()); err != nil {
		t.Fatalf("failed to verify range B's stats before split: %v", err)
	}

	mtc.manualClock.Increment(100)

	// Merge the b range back into the a range.
	args := adminMergeArgs(lhsDesc.StartKey.AsRawKey())
	if _, err := client.SendWrapped(ctx, store.TestSender(), args); err != nil {
		t.Fatal(err)
	}
	replMerged := store.LookupReplica(lhsDesc.StartKey)

	// Get the range stats for the merged range and verify.
	snap = store.Engine().NewSnapshot()
	defer snap.Close()
	msMerged, err := stateloader.Make(nil /* st */, replMerged.RangeID).LoadMVCCStats(ctx, snap)
	if err != nil {
		t.Fatal(err)
	}

	// Merged stats should agree with recomputation.
	if err := verifyRecomputedStats(snap, replMerged.Desc(), msMerged, mtc.manualClock.UnixNano()); err != nil {
		t.Errorf("failed to verify range's stats after merge: %v", err)
	}
}

func TestStoreRangeMergeInFlightTxns(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	storeCfg := storage.TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableReplicateQueue = true
	mtc := &multiTestContext{storeConfig: &storeCfg}
	mtc.Start(t, 1)
	defer mtc.Stop()
	store := mtc.Store(0)

	// Create two adjacent ranges.
	setupReplicas := func() (lhsDesc, rhsDesc *roachpb.RangeDescriptor, err error) {
		lhsDesc, rhsDesc, err = createSplitRanges(ctx, store)
		if err != nil {
			return nil, nil, err
		}
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
			repl, err := store.GetReplica(rhsDesc.RangeID)
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

	// Create the ranges to be merged. Put both ranges on both stores, but give
	// the second store the lease on the RHS. The LHS is largely irrelevant. What
	// matters is that the RHS exists on two stores so we can transfer its lease
	// during the merge.
	lhsDesc, rhsDesc, err := createSplitRanges(ctx, mtc.stores[0])
	if err != nil {
		t.Fatal(err)
	}
	mtc.replicateRange(lhsDesc.RangeID, 1)
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
	storeCfg.TestingKnobs.TestingResponseFilter = func(
		ba roachpb.BatchRequest, _ *roachpb.BatchResponse,
	) *roachpb.Error {
		del := ba.Requests[0].GetDelete()
		if del != nil && bytes.HasSuffix(del.Key, keys.LocalRangeDescriptorSuffix) && rand.Int()%4 == 0 {
			// After every few deletions of the local range descriptor, expire all
			// range leases. This makes the following sequence of events quite
			// likely:
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
			//     5. The GetSnapshotForMerge request arrives. (Or, if the merge
			//        transaction is incorrectly pipelined, the QueryIntent request
			//        for the RHS range descriptor key that precedes the
			//        GetSnapshotForMerge request arrives.)
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

// TestStoreReplicaGCAfterMerge verifies that the replica GC queue writes the
// correct tombstone when it GCs a replica of range that has been merged away.
//
// Consider the following sequence of events observed in a real cluster:
//
//     1. Adjacent ranges Q and R are slated to be merged. Q has replicas on
//        stores S1, S2, and S3, while R has replicas on S1, S2, and S4.
//     2. To collocate Q and R, the merge queue adds a replica of R on S3 and
//        removes the replica on S4. The replica on S4 is queued for garbage
//        collection, but is not yet processed.
//     3. The merge transaction commits, deleting R's range descriptor from the
//        meta2 index.
//     4. The replica GC queue processes the former replica of R on S4. It
//        performs a consistent lookup of R's start key in the meta2 index to
//        determine whether the replica is still a member of R. Since R has been
//        deleted, the lookup returns Q's range descriptor, not R's.
//
// The replica GC queue would previously fail to notice that it had received Q's
// range descriptor, not R's. It would then proceed to call store.RemoveReplica
// with Q's descriptor, which would write a replica tombstone for Q, when in
// fact the replica tombstone needed to be written for R. Without the correct
// replica tombstone, if S4 received a slow Raft message for the now-GC'd
// replica, it would incorrectly construct an uninitialized replica and panic.
//
// This test's approach to simulating this sequence of events is based on
// TestReplicaGCRace.
func TestStoreReplicaGCAfterMerge(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	storeCfg := storage.TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableReplicateQueue = true
	storeCfg.TestingKnobs.DisableReplicaGCQueue = true
	mtc := &multiTestContext{storeConfig: &storeCfg}
	mtc.Start(t, 2)
	defer mtc.Stop()
	store0, store1 := mtc.Store(0), mtc.Store(1)

	mtc.replicateRange(roachpb.RangeID(1), 1)
	lhsDesc, rhsDesc, err := createSplitRanges(ctx, store0)
	if err != nil {
		t.Fatal(err)
	}

	mtc.unreplicateRange(lhsDesc.RangeID, 1)
	mtc.unreplicateRange(rhsDesc.RangeID, 1)

	args := adminMergeArgs(lhsDesc.StartKey.AsRawKey())
	_, pErr := client.SendWrapped(ctx, store0.TestSender(), args)
	if pErr != nil {
		t.Fatal(pErr)
	}

	for _, rangeID := range []roachpb.RangeID{lhsDesc.RangeID, rhsDesc.RangeID} {
		repl, err := store1.GetReplica(rangeID)
		if err != nil {
			t.Fatal(err)
		}
		if err := store1.ManualReplicaGC(repl); err != nil {
			t.Fatal(err)
		}
		if _, err := store1.GetReplica(rangeID); err == nil {
			t.Fatalf("replica of r%d not gc'd from s1", rangeID)
		}
	}

	rhsReplDesc0, ok := rhsDesc.GetReplicaDescriptor(store0.StoreID())
	if !ok {
		t.Fatalf("expected %s to have a replica on %s", rhsDesc, store0)
	}
	rhsReplDesc1, ok := rhsDesc.GetReplicaDescriptor(store1.StoreID())
	if !ok {
		t.Fatalf("expected %s to have a replica on %s", rhsDesc, store1)
	}

	transport0 := storage.NewRaftTransport(
		log.AmbientContext{Tracer: mtc.storeConfig.Settings.Tracer},
		cluster.MakeTestingClusterSettings(),
		nodedialer.New(mtc.rpcContext, gossip.AddressResolver(mtc.gossips[0])),
		nil, /* grpcServer */
		mtc.transportStopper,
	)
	errChan := errorChannelTestHandler(make(chan *roachpb.Error, 1))
	transport0.Listen(store0.StoreID(), errChan)

	sendHeartbeat := func(toReplDesc roachpb.ReplicaDescriptor) {
		// Try several times, as the message may be dropped (see #18355).
		for i := 0; i < 5; i++ {
			if sent := transport0.SendAsync(&storage.RaftMessageRequest{
				FromReplica: rhsReplDesc0,
				ToReplica:   toReplDesc,
				Heartbeats: []storage.RaftHeartbeat{
					{
						RangeID:       rhsDesc.RangeID,
						FromReplicaID: rhsReplDesc0.ReplicaID,
						ToReplicaID:   toReplDesc.ReplicaID,
						Commit:        42,
					},
				},
			}); !sent {
				t.Fatal("failed to send heartbeat")
			}
			select {
			case pErr := <-errChan:
				switch pErr.GetDetail().(type) {
				case *roachpb.RaftGroupDeletedError:
					return
				default:
					t.Fatalf("unexpected error type %T: %s", pErr.GetDetail(), pErr)
				}
			case <-time.After(time.Second):
			}
		}
		t.Fatal("did not get expected RaftGroupDeleted error")
	}

	// Send a heartbeat to the now-GC'd replica on store1. If the replica
	// tombstone was not written correctly when the replica was GC'd, this will
	// cause a panic.
	sendHeartbeat(rhsReplDesc1)

	// Send a heartbeat to a fictional replica on store1 with a large replica ID.
	// This tests an implementation detail: the replica tombstone that gets
	// written in this case will use the maximum possible replica ID, so store1
	// should ignore heartbeats for replicas of the range with _any_ replica ID.
	sendHeartbeat(roachpb.ReplicaDescriptor{
		NodeID:    store1.Ident.NodeID,
		StoreID:   store1.Ident.StoreID,
		ReplicaID: 123456,
	})

	// Be extra paranoid and verify the exact value of the replica tombstone.
	var rhsTombstone1 roachpb.RaftTombstone
	rhsTombstoneKey := keys.RaftTombstoneKey(rhsDesc.RangeID)
	ok, err = engine.MVCCGetProto(ctx, store1.Engine(), rhsTombstoneKey, hlc.Timestamp{},
		true /* consistent */, nil /* txn */, &rhsTombstone1)
	if err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatalf("missing raft tombstone at key %s", rhsTombstoneKey)
	}
	if e, a := roachpb.ReplicaID(math.MaxInt32), rhsTombstone1.NextReplicaID; e != a {
		t.Fatalf("expected next replica ID to be %d, but got %d", e, a)
	}
}

// TestStoreRangeMergeAddReplicaRace verifies that an add replica request that
// occurs concurrently with a merge is aborted.
//
// To see why aborting the add replica request is necessary, consider two
// adjacent and collocated ranges, Q and R. Say the replicate queue decides to
// rebalance Q onto store S4. It will initiate a ChangeReplicas command that
// will send S4 a preemptive snapshot, then launch a replica-change transaction
// to update R's range descriptor with the new replica. Now say the merge queue
// decides to merge Q and R after the preemptive snapshot of Q has been sent to
// S4 but before the replica-change transaction has started. The merge can
// succeed because the ranges are still collocated. (The new replica of Q is
// only considered added once the replica-change transaction commits.) If the
// replica-change transaction were to commit, the new replica of Q on S4 would
// have a snapshot of Q that predated the merge. In order to catch up, it would
// need to apply the merge trigger, but the merge trigger will explode because
// S4 does not have a replica of R.
//
// To avoid this scenario, ChangeReplicas commands will abort if they discover
// the range descriptor has changed between when the snapshot is sent and when
// the replica-change transaction starts.
//
// There is a particularly diabolical edge case here. Consider the same
// situation as above, except that Q and R merge together and then split at
// exactly the same key, all before the replica-change transaction starts. Q's
// range descriptor will have the same start key, end key, and next replica ID
// that it did when the preemptive snapshot started. That is, it will look
// unchanged! To protect against this, range descriptors contain a generation
// counter, which is incremented on every split or merge. The presence of this
// counter means that ChangeReplicas commands can detect and abort if any merges
// have occurred since the preemptive snapshot, even if the sequence of splits
// or merges left the keyspan of the range unchanged. This diabolical edge case
// is tested here.
func TestStoreRangeMergeAddReplicaRace(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	storeCfg := storage.TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableSplitQueue = true
	storeCfg.TestingKnobs.DisableReplicateQueue = true

	mtc := &multiTestContext{storeConfig: &storeCfg}
	mtc.Start(t, 2)
	defer mtc.Stop()
	store0, store1 := mtc.Store(0), mtc.Store(1)

	lhsDesc, rhsDesc, err := createSplitRanges(ctx, store0)
	if err != nil {
		t.Fatal(err)
	}

	mtc.transport.Listen(store1.StoreID(), RaftMessageHandlerInterceptor{
		RaftMessageHandler: store1,
		handleSnapshotFilter: func(header *storage.SnapshotRequest_Header) {
			mergeArgs := adminMergeArgs(lhsDesc.StartKey.AsRawKey())
			if _, pErr := client.SendWrapped(ctx, store0.TestSender(), mergeArgs); pErr != nil {
				// The filter is invoked by a different goroutine, so can't use t.Fatal.
				t.Error(pErr)
				return
			}
			splitArgs := adminSplitArgs(rhsDesc.StartKey.AsRawKey())
			if _, pErr := client.SendWrapped(ctx, store0.TestSender(), splitArgs); pErr != nil {
				// The filter is invoked by a different goroutine, so can't use t.Fatal.
				t.Error(pErr)
				return
			}
		},
	})

	err = mtc.replicateRangeNonFatal(lhsDesc.RangeID, 1)
	if exp := "change replicas of r1 failed: descriptor changed"; !testutils.IsError(err, exp) {
		t.Fatalf("expected error %q, got %v", exp, err)
	}
}

func TestStoreRangeMergeSlowUnabandonedFollower(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	storeCfg := storage.TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableReplicateQueue = true
	storeCfg.TestingKnobs.DisableReplicaGCQueue = true
	mtc := &multiTestContext{storeConfig: &storeCfg}
	mtc.Start(t, 3)
	defer mtc.Stop()
	store0, store2 := mtc.Store(0), mtc.Store(2)

	mtc.replicateRange(roachpb.RangeID(1), 1, 2)
	lhsDesc, rhsDesc, err := createSplitRanges(ctx, store0)
	if err != nil {
		t.Fatal(err)
	}

	// Wait for store2 to hear about the split.
	testutils.SucceedsSoon(t, func() error {
		_, err := store2.GetReplica(rhsDesc.RangeID)
		return err
	})

	// Block Raft traffic to the LHS replica on store2, by holding its raftMu, so
	// that its LHS isn't aware there's a merge in progress.
	lhsRepl2, err := store2.GetReplica(lhsDesc.RangeID)
	if err != nil {
		t.Fatal(err)
	}
	lhsRepl2.RaftLock()

	args := adminMergeArgs(lhsDesc.StartKey.AsRawKey())
	_, pErr := client.SendWrapped(ctx, store0.TestSender(), args)
	if pErr != nil {
		t.Fatal(pErr)
	}

	// Verify that store2 won't inadvertently GC the RHS before it's heard about
	// the merge. This is a tricky case for the replica GC queue, as meta2 will
	// indicate that the range has been merged away.
	rhsRepl2, err := store2.GetReplica(rhsDesc.RangeID)
	if err != nil {
		t.Fatal(err)
	}
	if err := store2.ManualReplicaGC(rhsRepl2); err != nil {
		t.Fatal(err)
	}
	if _, err := store2.GetReplica(rhsDesc.RangeID); err != nil {
		t.Fatalf("non-abandoned rhs replica unexpectedly GC'd before merge")
	}

	// Restore communication with store2. Give it the lease to force all commands
	// to be applied, including the merge trigger.
	lhsRepl2.RaftUnlock()
	mtc.transferLease(ctx, lhsDesc.RangeID, 0, 2)
}

func TestStoreRangeMergeSlowAbandonedFollower(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	storeCfg := storage.TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableReplicateQueue = true
	storeCfg.TestingKnobs.DisableReplicaGCQueue = true
	mtc := &multiTestContext{storeConfig: &storeCfg}
	mtc.Start(t, 3)
	defer mtc.Stop()
	store0, store2 := mtc.Store(0), mtc.Store(2)

	mtc.replicateRange(roachpb.RangeID(1), 1, 2)
	lhsDesc, rhsDesc, err := createSplitRanges(ctx, store0)
	if err != nil {
		t.Fatal(err)
	}

	// Wait for store2 to hear about the split.
	var rhsRepl2 *storage.Replica
	testutils.SucceedsSoon(t, func() error {
		rhsRepl2, err = store2.GetReplica(rhsDesc.RangeID)
		return err
	})

	// Block Raft traffic to the LHS replica on store2, by holding its raftMu, so
	// that its LHS isn't aware there's a merge in progress.
	lhsRepl2, err := store2.GetReplica(lhsDesc.RangeID)
	if err != nil {
		t.Fatal(err)
	}
	lhsRepl2.RaftLock()

	args := adminMergeArgs(lhsDesc.StartKey.AsRawKey())
	_, pErr := client.SendWrapped(ctx, store0.TestSender(), args)
	if pErr != nil {
		t.Fatal(pErr)
	}

	// Remove store2 from the range after the merge. It won't hear about this yet,
	// but we'll be able to commit the configuration change because we have two
	// other live members.
	mtc.unreplicateRange(lhsDesc.RangeID, 2)

	// Verify that store2 won't inadvertently GC the RHS before it's heard about
	// the merge. This is a particularly tricky case for the replica GC queue, as
	// meta2 will indicate that the range has been merged away AND that store2 is
	// not a member of the new range.
	if err := store2.ManualReplicaGC(rhsRepl2); err != nil {
		t.Fatal(err)
	}
	if _, err := store2.GetReplica(rhsDesc.RangeID); err != nil {
		t.Fatal("rhs replica on store2 destroyed before lhs applied merge")
	}

	// Flush store2's queued requests.
	lhsRepl2.RaftUnlock()

	// Ensure that the unblocked merge eventually applies and subsumes the RHS.
	testutils.SucceedsSoon(t, func() error {
		if _, err := store2.GetReplica(rhsDesc.RangeID); err == nil {
			return errors.New("rhs not yet destroyed")
		}
		return nil
	})
}

func TestStoreRangeMergeAbandonedFollowers(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	storeCfg := storage.TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableReplicateQueue = true
	storeCfg.TestingKnobs.DisableReplicaGCQueue = true
	storeCfg.TestingKnobs.DisableSplitQueue = true
	mtc := &multiTestContext{storeConfig: &storeCfg}
	mtc.Start(t, 3)
	defer mtc.Stop()
	store2 := mtc.Store(2)

	mtc.replicateRange(roachpb.RangeID(1), 1, 2)

	// Split off three ranges.
	keys := []roachpb.RKey{roachpb.RKey("a"), roachpb.RKey("b"), roachpb.RKey("c")}
	for _, key := range keys {
		splitArgs := adminSplitArgs(key.AsRawKey())
		if _, pErr := client.SendWrapped(ctx, mtc.distSenders[0], splitArgs); pErr != nil {
			t.Fatal(pErr)
		}
	}

	// Wait for store2 to hear about all three splits.
	var repls []*storage.Replica
	testutils.SucceedsSoon(t, func() error {
		repls = nil
		for _, key := range keys {
			repl := store2.LookupReplica(key) /* end */
			if repl == nil || !repl.Desc().StartKey.Equal(key) {
				return fmt.Errorf("replica for key %q is missing or has wrong start key: %s", key, repl)
			}
			repls = append(repls, repl)
		}
		return nil
	})

	// Remove all replicas from store2.
	for _, repl := range repls {
		mtc.unreplicateRange(repl.RangeID, 2)
	}

	// Merge all three ranges together. store2 won't hear about this merge.
	for i := 0; i < 2; i++ {
		if _, pErr := client.SendWrapped(ctx, mtc.distSenders[0], adminMergeArgs(roachpb.Key("a"))); pErr != nil {
			t.Fatal(pErr)
		}
	}

	// Verify that the abandoned ranges on store2 can only GC'd from left to
	// right.
	if err := store2.ManualReplicaGC(repls[2]); err != nil {
		t.Fatal(err)
	}
	if _, err := store2.GetReplica(repls[2].RangeID); err != nil {
		t.Fatal("c replica on store2 destroyed before b")
	}
	if err := store2.ManualReplicaGC(repls[1]); err != nil {
		t.Fatal(err)
	}
	if _, err := store2.GetReplica(repls[1].RangeID); err != nil {
		t.Fatal("b replica on store2 destroyed before a")
	}
	if err := store2.ManualReplicaGC(repls[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := store2.GetReplica(repls[0].RangeID); err == nil {
		t.Fatal("a replica not destroyed")
	}

	if err := store2.ManualReplicaGC(repls[2]); err != nil {
		t.Fatal(err)
	}
	if _, err := store2.GetReplica(repls[2].RangeID); err != nil {
		t.Fatal("c replica on store2 destroyed before b")
	}
	if err := store2.ManualReplicaGC(repls[1]); err != nil {
		t.Fatal(err)
	}
	if _, err := store2.GetReplica(repls[1].RangeID); err == nil {
		t.Fatal("b replica not destroyed")
	}

	if err := store2.ManualReplicaGC(repls[2]); err != nil {
		t.Fatal(err)
	}
	if _, err := store2.GetReplica(repls[2].RangeID); err == nil {
		t.Fatal("c replica not destroyed")
	}
}

func TestStoreRangeMergeDeadFollower(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	storeCfg := storage.TestStoreConfig(nil)
	mtc := &multiTestContext{storeConfig: &storeCfg}
	mtc.Start(t, 3)
	defer mtc.Stop()
	store0 := mtc.Store(0)

	mtc.replicateRange(roachpb.RangeID(1), 1, 2)
	lhsDesc, _, err := createSplitRanges(ctx, store0)
	if err != nil {
		t.Fatal(err)
	}

	mtc.stopStore(2)

	args := adminMergeArgs(lhsDesc.StartKey.AsRawKey())
	_, pErr := client.SendWrapped(ctx, store0.TestSender(), args)
	expErr := "merge of range into 1 failed: waiting for all right-hand replicas to catch up"
	if !testutils.IsPError(pErr, expErr) {
		t.Fatalf("expected %q error, but got %v", expErr, pErr)
	}
}

func TestStoreRangeMergeReadoptedBothFollowers(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	storeCfg := storage.TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableReplicateQueue = true
	storeCfg.TestingKnobs.DisableReplicaGCQueue = true
	mtc := &multiTestContext{storeConfig: &storeCfg}
	mtc.Start(t, 3)
	defer mtc.Stop()
	store0, store2 := mtc.Store(0), mtc.Store(2)

	// Create two ranges on all nodes.
	mtc.replicateRange(roachpb.RangeID(1), 1, 2)
	lhsDesc, rhsDesc, err := createSplitRanges(ctx, store0)
	if err != nil {
		t.Fatal(err)
	}

	// Wait for store2 to hear about the split.
	var lhsRepl2, rhsRepl2 *storage.Replica
	testutils.SucceedsSoon(t, func() error {
		lhsRepl2, err = store2.GetReplica(lhsDesc.RangeID)
		if err != nil {
			return err
		}
		rhsRepl2, err = store2.GetReplica(rhsDesc.RangeID)
		return err
	})

	// Abandon the two ranges on store2, but do not GC them.
	mtc.unreplicateRange(lhsDesc.RangeID, 2)
	mtc.unreplicateRange(rhsDesc.RangeID, 2)

	// Merge the two ranges together.
	args := adminMergeArgs(lhsDesc.StartKey.AsRawKey())
	_, pErr := client.SendWrapped(ctx, store0.TestSender(), args)
	if pErr != nil {
		t.Fatal(pErr)
	}

	// Attempt to re-add the merged range to store2. The operation should fail
	// because store2's LHS and RHS replicas intersect the merged range.
	lhsRepl0, err := store0.GetReplica(lhsDesc.RangeID)
	if err != nil {
		t.Fatal(err)
	}

	addLHSRepl2 := func() error {
		for r := retry.StartWithCtx(ctx, retry.Options{}); r.Next(); {
			err := lhsRepl0.ChangeReplicas(ctx, roachpb.ADD_REPLICA, roachpb.ReplicationTarget{
				NodeID:  store2.Ident.NodeID,
				StoreID: store2.Ident.StoreID,
			}, lhsRepl0.Desc(), storage.ReasonUnknown, t.Name())
			if !testutils.IsError(err, "store busy applying snapshots") {
				return err
			}
		}
		t.Fatal("unreachable")
		return nil
	}

	err = addLHSRepl2()
	if exp := "cannot apply snapshot: snapshot intersects existing range"; !testutils.IsError(err, exp) {
		t.Fatalf("expected %q error, but got %v", exp, err)
	}

	// GC the replica of the LHS on store2.
	if err := store2.ManualReplicaGC(lhsRepl2); err != nil {
		t.Fatal(err)
	}
	if _, err := store2.GetReplica(lhsDesc.RangeID); err == nil {
		t.Fatal("lhs replica not destroyed on store2")
	}

	// Attempt to re-add the merged range to store2. The operation should fail
	// again because store2's RHS still intersects the merged range.
	err = addLHSRepl2()
	if exp := "cannot apply snapshot: snapshot intersects existing range"; !testutils.IsError(err, exp) {
		t.Fatalf("expected %q error, but got %v", exp, err)
	}

	// GC the replica of the RHS on store2.
	if err := store2.ManualReplicaGC(rhsRepl2); err != nil {
		t.Fatal(err)
	}
	if _, err := store2.GetReplica(rhsDesc.RangeID); err == nil {
		t.Fatal("rhs replica not destroyed on store2")
	}

	// Attempt to re-add the merged range to store2 one last time. This time the
	// operation should succeed because there are no remaining intersecting
	// replicas.
	if err := addLHSRepl2(); err != nil {
		t.Fatal(err)
	}

	// Give store2 the lease to force all commands to be applied, including the
	// ChangeReplicas.
	mtc.transferLease(ctx, lhsDesc.RangeID, 0, 2)
}

func TestStoreRangeMergeReadoptedLHSFollower(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	storeCfg := storage.TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableReplicateQueue = true
	storeCfg.TestingKnobs.DisableReplicaGCQueue = true
	mtc := &multiTestContext{storeConfig: &storeCfg}
	mtc.Start(t, 3)
	defer mtc.Stop()
	store0, store2 := mtc.Store(0), mtc.Store(2)

	// Create two ranges on store0 and store1.
	lhsDesc, rhsDesc, err := createSplitRanges(ctx, store0)
	if err != nil {
		t.Fatal(err)
	}
	mtc.replicateRange(lhsDesc.RangeID, 1)
	mtc.replicateRange(rhsDesc.RangeID, 1)

	// Abandon a replica of the LHS on store2.
	mtc.replicateRange(lhsDesc.RangeID, 2)
	var lhsRepl2 *storage.Replica
	testutils.SucceedsSoon(t, func() error {
		lhsRepl2, err = store2.GetReplica(lhsDesc.RangeID)
		return err
	})
	mtc.unreplicateRange(lhsDesc.RangeID, 2)

	// Merge the two ranges together.
	args := adminMergeArgs(lhsDesc.StartKey.AsRawKey())
	_, pErr := client.SendWrapped(ctx, store0.TestSender(), args)
	if pErr != nil {
		t.Fatal(pErr)
	}

	// Attempt to re-add the merged range to store2. This should succeed
	// immediately because there are no overlapping replicas that would interfere
	// with the widening of the existing LHS replica.
	mtc.replicateRange(lhsDesc.RangeID, 2)

	if newLHSRepl2, err := store2.GetReplica(lhsDesc.RangeID); err != nil {
		t.Fatal(err)
	} else if lhsRepl2 != newLHSRepl2 {
		t.Fatalf("store2 created new lhs repl to receive preemptive snapshot post merge")
	}

	// Give store2 the lease to force all commands to be applied, including the
	// ChangeReplicas.
	mtc.transferLease(ctx, lhsDesc.RangeID, 0, 2)
}

// unreliableRaftHandler drops all Raft messages that are addressed to the
// specified rangeID, but lets all other messages through.
type unreliableRaftHandler struct {
	rangeID roachpb.RangeID
	storage.RaftMessageHandler
}

func (h *unreliableRaftHandler) HandleRaftRequest(
	ctx context.Context,
	req *storage.RaftMessageRequest,
	respStream storage.RaftMessageResponseStream,
) *roachpb.Error {
	if req.RangeID == h.rangeID {
		return nil
	}
	return h.RaftMessageHandler.HandleRaftRequest(ctx, req, respStream)
}

func TestStoreRangeMergeRaftSnapshot(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	storeCfg := storage.TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableReplicateQueue = true
	storeCfg.TestingKnobs.DisableReplicaGCQueue = true
	mtc := &multiTestContext{storeConfig: &storeCfg}
	mtc.Start(t, 3)
	defer mtc.Stop()
	store0, store2 := mtc.Store(0), mtc.Store(2)
	distSender := mtc.distSenders[0]

	// Create three fully-caught-up, adjacent ranges on all three stores.
	mtc.replicateRange(roachpb.RangeID(1), 1, 2)
	splitKeys := []roachpb.Key{roachpb.Key("a"), roachpb.Key("b"), roachpb.Key("c")}
	for _, key := range splitKeys {
		if _, pErr := client.SendWrapped(ctx, distSender, adminSplitArgs(key)); pErr != nil {
			t.Fatal(pErr)
		}
		if _, pErr := client.SendWrapped(ctx, distSender, incrementArgs(key, 1)); pErr != nil {
			t.Fatal(pErr)
		}
		mtc.waitForValues(key, []int64{1, 1, 1})
	}

	lhsRepl0 := store0.LookupReplica(roachpb.RKey("a"))

	// Start dropping all Raft traffic to the first range on store1.
	mtc.transport.Listen(store2.Ident.StoreID, &unreliableRaftHandler{
		rangeID:            lhsRepl0.RangeID,
		RaftMessageHandler: store2,
	})

	// Merge [a, b) into [b, c), then [a, c) into [c, /Max).
	for i := 0; i < 2; i++ {
		if _, pErr := client.SendWrapped(ctx, distSender, adminMergeArgs(roachpb.Key("a"))); pErr != nil {
			t.Fatal(pErr)
		}
	}

	// Truncate the logs of the LHS.
	{
		repl := store0.LookupReplica(roachpb.RKey("a"))
		index, err := repl.GetLastIndex()
		if err != nil {
			t.Fatal(err)
		}
		// Truncate the log at index+1 (log entries < N are removed, so this
		// includes the merge).
		truncArgs := &roachpb.TruncateLogRequest{
			RequestHeader: roachpb.RequestHeader{Key: roachpb.Key("a")},
			Index:         index,
			RangeID:       repl.RangeID,
		}
		if _, err := client.SendWrapped(ctx, mtc.distSenders[0], truncArgs); err != nil {
			t.Fatal(err)
		}
	}

	beforeRaftSnaps := store2.Metrics().RangeSnapshotsNormalApplied.Count()

	// Restore Raft traffic to the LHS on store2.
	mtc.transport.Listen(store2.Ident.StoreID, store2)

	// Wait for all replicas to catch up to the same point. Because we truncated
	// the log while store2 was unavailable, this will require a Raft snapshot.
	for _, key := range splitKeys {
		if _, pErr := client.SendWrapped(ctx, distSender, incrementArgs(key, 1)); pErr != nil {
			t.Fatal(pErr)
		}
		mtc.waitForValues(key, []int64{2, 2, 2})
	}

	afterRaftSnaps := store2.Metrics().RangeSnapshotsNormalApplied.Count()
	if afterRaftSnaps == beforeRaftSnaps {
		t.Fatal("expected store2 to apply at least 1 additional raft snapshot")
	}

	// Verify that the sets of keys in store0 and store2 are identical.
	storeKeys0 := getEngineKeySet(t, store0.Engine())
	storeKeys2 := getEngineKeySet(t, store2.Engine())
	ignoreKey := func(k string) bool {
		// Unreplicated keys for the two remaining ranges are allowed to differ.
		return strings.HasPrefix(k, string(keys.MakeRangeIDUnreplicatedPrefix(roachpb.RangeID(1)))) ||
			strings.HasPrefix(k, string(keys.MakeRangeIDUnreplicatedPrefix(lhsRepl0.RangeID)))
	}
	for k := range storeKeys0 {
		if ignoreKey(k) {
			continue
		}
		if _, ok := storeKeys2[k]; !ok {
			t.Errorf("store2 missing key %s", roachpb.Key(k))
		}
	}
	for k := range storeKeys2 {
		if ignoreKey(k) {
			continue
		}
		if _, ok := storeKeys0[k]; !ok {
			t.Errorf("store2 has extra key %s", roachpb.Key(k))
		}
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

func TestMergeQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	storeCfg := storage.TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableSplitQueue = true
	storeCfg.TestingKnobs.DisableScanner = true
	sv := &storeCfg.Settings.SV
	storage.MergeQueueEnabled.Override(sv, true)
	var mtc multiTestContext
	mtc.storeConfig = &storeCfg
	mtc.Start(t, 2)
	defer mtc.Stop()
	store := mtc.Store(0)

	split := func(t *testing.T, key roachpb.Key) {
		t.Helper()
		if _, err := client.SendWrapped(ctx, store.DB().NonTransactionalSender(), adminSplitArgs(key)); err != nil {
			t.Fatal(err)
		}
	}

	// Create two empty ranges, a - b and b - c, by splitting at a, b, and c.
	split(t, roachpb.Key("a"))
	split(t, roachpb.Key("b"))
	split(t, roachpb.Key("c"))
	lhs := func() *storage.Replica { return store.LookupReplica(roachpb.RKey("a")) }
	rhs := func() *storage.Replica { return store.LookupReplica(roachpb.RKey("b")) }
	lhsDesc := lhs().Desc()
	rhsDesc := rhs().Desc()

	// setThresholds simulates a zone config update that updates the ranges'
	// minimum and maximum sizes.
	setThresholds := func(minBytes, maxBytes int64) {
		lhs().SetByteThresholds(minBytes, maxBytes)
		rhs().SetByteThresholds(minBytes, maxBytes)
	}

	defaultZone := config.DefaultZoneConfig()

	reset := func(t *testing.T) {
		t.Helper()
		split(t, roachpb.Key("b"))
		_, pErr := client.SendWrapped(ctx, store.DB().NonTransactionalSender(), &roachpb.ClearRangeRequest{
			RequestHeader: roachpb.RequestHeader{
				Key:    roachpb.Key("a"),
				EndKey: roachpb.Key("c"),
			},
		})
		if pErr != nil {
			t.Fatal(pErr)
		}
		setThresholds(defaultZone.RangeMinBytes, defaultZone.RangeMaxBytes)
	}

	verifyMerged := func(t *testing.T) {
		t.Helper()
		repl := store.LookupReplica(rhsDesc.StartKey)
		if !repl.Desc().StartKey.Equal(lhsDesc.StartKey) {
			t.Fatalf("ranges unexpectedly unmerged")
		}
	}

	verifyUnmerged := func(t *testing.T) {
		t.Helper()
		repl := store.LookupReplica(rhsDesc.StartKey)
		if repl.Desc().StartKey.Equal(lhsDesc.StartKey) {
			t.Fatalf("ranges unexpectedly merged")
		}
	}

	t.Run("both-empty", func(t *testing.T) {
		reset(t)
		store.ForceMergeScanAndProcess()
		verifyMerged(t)
	})

	t.Run("rhs-setting-threshold", func(t *testing.T) {
		reset(t)

		if err := store.DB().Put(ctx, "b-key", "val"); err != nil {
			t.Fatal(err)
		}
		store.ForceMergeScanAndProcess()
		verifyUnmerged(t)

		storage.MergeMaxRHSSize.Override(sv, 100)
		defer storage.MergeMaxRHSSize.Override(sv, storage.MergeMaxRHSSize.Default())
		store.ForceMergeScanAndProcess()
		verifyMerged(t)
	})

	rng, _ := randutil.NewPseudoRand()

	t.Run("rhs-replica-threshold", func(t *testing.T) {
		reset(t)

		// Make the RHS cluster setting threshold irrelevant.
		storage.MergeMaxRHSSize.Override(sv, 1<<32)
		defer storage.MergeMaxRHSSize.Override(sv, storage.MergeMaxRHSSize.Default())

		bytes := randutil.RandBytes(rng, int(defaultZone.RangeMinBytes))
		if err := store.DB().Put(ctx, "b-key", bytes); err != nil {
			t.Fatal(err)
		}
		store.ForceMergeScanAndProcess()
		verifyUnmerged(t)

		setThresholds(defaultZone.RangeMinBytes*2, defaultZone.RangeMaxBytes)
		store.ForceMergeScanAndProcess()
		verifyMerged(t)
	})

	t.Run("lhs-replica-threshold", func(t *testing.T) {
		reset(t)

		bytes := randutil.RandBytes(rng, int(defaultZone.RangeMinBytes))
		if err := store.DB().Put(ctx, "a-key", bytes); err != nil {
			t.Fatal(err)
		}
		store.ForceMergeScanAndProcess()
		verifyUnmerged(t)

		setThresholds(defaultZone.RangeMinBytes*2, defaultZone.RangeMaxBytes)
		store.ForceMergeScanAndProcess()
		verifyMerged(t)
	})

	t.Run("combined-threshold", func(t *testing.T) {
		reset(t)

		storage.MergeMaxRHSSize.Override(sv, 1<<32)
		defer storage.MergeMaxRHSSize.Override(sv, storage.MergeMaxRHSSize.Default())

		// The ranges are individually beneath the minimum size threshold, but
		// together they'll exceed the maximum size threshold.
		setThresholds(200, 200)
		bytes := randutil.RandBytes(rng, 100)
		if err := store.DB().Put(ctx, "a-key", bytes); err != nil {
			t.Fatal(err)
		}
		if err := store.DB().Put(ctx, "b-key", bytes); err != nil {
			t.Fatal(err)
		}
		store.ForceMergeScanAndProcess()
		verifyUnmerged(t)

		setThresholds(200, 400)
		store.ForceMergeScanAndProcess()
		verifyMerged(t)
	})

	t.Run("non-collocated", func(t *testing.T) {
		reset(t)
		mtc.replicateRange(rhs().Desc().RangeID, 1)
		mtc.transferLease(ctx, rhs().Desc().RangeID, 0, 1)
		mtc.unreplicateRange(rhs().Desc().RangeID, 0)
		store.ForceMergeScanAndProcess()
		verifyMerged(t)
	})
}

func TestInvalidGetSnapshotForMergeRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	var mtc multiTestContext
	mtc.Start(t, 1)
	defer mtc.Stop()
	store := mtc.Store(0)

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
	var mtc multiTestContext
	mtc.Start(b, 1)
	defer mtc.Stop()
	store := mtc.Store(0)

	lhsDesc, rhsDesc, err := createSplitRanges(ctx, store)
	if err != nil {
		b.Fatal(err)
	}

	// Write some values left and right of the proposed split key.
	storage.WriteRandomDataToRange(b, store, lhsDesc.RangeID, []byte("aaa"))
	storage.WriteRandomDataToRange(b, store, rhsDesc.RangeID, []byte("ccc"))

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
