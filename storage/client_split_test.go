// Copyright 2014 The Cockroach Authors.
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
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/storage/storagebase"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/testutils/testcluster"
	"github.com/cockroachdb/cockroach/ts"
	"github.com/cockroachdb/cockroach/ts/tspb"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/protoutil"
	"github.com/cockroachdb/cockroach/util/randutil"
	"github.com/cockroachdb/cockroach/util/tracing"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/pkg/errors"
)

func adminSplitArgs(key, splitKey roachpb.Key) roachpb.AdminSplitRequest {
	return roachpb.AdminSplitRequest{
		Span: roachpb.Span{
			Key: key,
		},
		SplitKey: splitKey,
	}
}

// TestStoreRangeSplitAtIllegalKeys verifies a range cannot be split
// at illegal keys.
func TestStoreRangeSplitAtIllegalKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()
	store, stopper, _ := createTestStore(t)
	defer stopper.Stop()

	for _, key := range []roachpb.Key{
		keys.Meta1Prefix,
		testutils.MakeKey(keys.Meta1Prefix, []byte("a")),
		testutils.MakeKey(keys.Meta1Prefix, roachpb.RKeyMax),
		keys.Meta2KeyMax,
		keys.MakeTablePrefix(10 /* system descriptor ID */),
	} {
		args := adminSplitArgs(roachpb.KeyMin, key)
		_, pErr := client.SendWrapped(rg1(store), nil, &args)
		if !testutils.IsPError(pErr, "cannot split") {
			t.Errorf("%q: unexpected split error %s", key, pErr)
		}
	}
}

// TestStoreRangeSplitAtTablePrefix verifies a range can be split at
// UserTableDataMin and still gossip the SystemConfig properly.
func TestStoreRangeSplitAtTablePrefix(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sCtx := storage.TestStoreContext()
	sCtx.TestingKnobs.DisableSplitQueue = true
	store, stopper, _ := createTestStoreWithContext(t, sCtx)
	defer stopper.Stop()

	key := keys.MakeRowSentinelKey(append([]byte(nil), keys.UserTableDataMin...))
	args := adminSplitArgs(key, key)
	if _, pErr := client.SendWrapped(rg1(store), nil, &args); pErr != nil {
		t.Fatalf("%q: split unexpected error: %s", key, pErr)
	}

	var desc sqlbase.TableDescriptor
	descBytes, err := protoutil.Marshal(&desc)
	if err != nil {
		t.Fatal(err)
	}

	// Update SystemConfig to trigger gossip.
	if err := store.DB().Txn(func(txn *client.Txn) error {
		txn.SetSystemConfigTrigger()
		// We don't care about the values, just the keys.
		k := sqlbase.MakeDescMetadataKey(sqlbase.ID(keys.MaxReservedDescID + 1))
		return txn.Put(k, &desc)
	}); err != nil {
		t.Fatal(err)
	}

	successChan := make(chan struct{}, 1)
	store.Gossip().RegisterCallback(gossip.KeySystemConfig, func(_ string, content roachpb.Value) {
		contentBytes, err := content.GetBytes()
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(contentBytes, descBytes) {
			select {
			case successChan <- struct{}{}:
			default:
			}
		}
	})

	select {
	case <-time.After(time.Second):
		t.Errorf("expected a schema gossip containing %q, but did not see one", descBytes)
	case <-successChan:
	}
}

// TestStoreRangeSplitInsideRow verifies an attempt to split a range inside of
// a table row will cause a split at a boundary between rows.
func TestStoreRangeSplitInsideRow(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sCtx := storage.TestStoreContext()
	sCtx.TestingKnobs.DisableSplitQueue = true
	store, stopper, _ := createTestStoreWithContext(t, sCtx)
	defer stopper.Stop()

	// Manually create some the column keys corresponding to the table:
	//
	//   CREATE TABLE t (id STRING PRIMARY KEY, col1 INT, col2 INT)
	tableKey := keys.MakeTablePrefix(keys.MaxReservedDescID + 1)
	rowKey := roachpb.Key(encoding.EncodeVarintAscending(append([]byte(nil), tableKey...), 1))
	rowKey = encoding.EncodeStringAscending(encoding.EncodeVarintAscending(rowKey, 1), "a")
	col1Key := keys.MakeFamilyKey(append([]byte(nil), rowKey...), 1)
	col2Key := keys.MakeFamilyKey(append([]byte(nil), rowKey...), 2)

	// We don't care about the value, so just store any old thing.
	if err := store.DB().Put(col1Key, "column 1"); err != nil {
		t.Fatal(err)
	}
	if err := store.DB().Put(col2Key, "column 2"); err != nil {
		t.Fatal(err)
	}

	// Split between col1Key and col2Key by splitting before col2Key.
	args := adminSplitArgs(col2Key, col2Key)
	_, err := client.SendWrapped(rg1(store), nil, &args)
	if err != nil {
		t.Fatalf("%s: split unexpected error: %s", col1Key, err)
	}

	rng1 := store.LookupReplica(col1Key, nil)
	rng2 := store.LookupReplica(col2Key, nil)
	// Verify the two columns are still on the same range.
	if !reflect.DeepEqual(rng1, rng2) {
		t.Fatalf("%s: ranges differ: %+v vs %+v", roachpb.Key(col1Key), rng1, rng2)
	}
	// Verify we split on a row key.
	if startKey := rng1.Desc().StartKey; !startKey.Equal(rowKey) {
		t.Fatalf("%s: expected split on %s, but found %s",
			roachpb.Key(col1Key), roachpb.Key(rowKey), startKey)
	}

	// Verify the previous range was split on a row key.
	rng3 := store.LookupReplica(tableKey, nil)
	if endKey := rng3.Desc().EndKey; !endKey.Equal(rowKey) {
		t.Fatalf("%s: expected split on %s, but found %s",
			roachpb.Key(col1Key), roachpb.Key(rowKey), endKey)
	}
}

// TestStoreRangeSplitIntents executes a split of a range and verifies
// that all intents are cleared and the transaction record cleaned up.
func TestStoreRangeSplitIntents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sCtx := storage.TestStoreContext()
	sCtx.TestingKnobs.DisableSplitQueue = true
	store, stopper, _ := createTestStoreWithContext(t, sCtx)
	defer stopper.Stop()

	// First, write some values left and right of the proposed split key.
	pArgs := putArgs([]byte("c"), []byte("foo"))
	if _, pErr := client.SendWrapped(rg1(store), nil, &pArgs); pErr != nil {
		t.Fatal(pErr)
	}
	pArgs = putArgs([]byte("x"), []byte("bar"))
	if _, pErr := client.SendWrapped(rg1(store), nil, &pArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Split the range.
	splitKey := roachpb.Key("m")
	args := adminSplitArgs(roachpb.KeyMin, splitKey)
	if _, pErr := client.SendWrapped(rg1(store), nil, &args); pErr != nil {
		t.Fatal(pErr)
	}

	// Verify no intents remains on range descriptor keys.
	splitKeyAddr, err := keys.Addr(splitKey)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []roachpb.Key{keys.RangeDescriptorKey(roachpb.RKeyMin), keys.RangeDescriptorKey(splitKeyAddr)} {
		if _, _, err := engine.MVCCGet(context.Background(), store.Engine(), key, store.Clock().Now(), true, nil); err != nil {
			t.Errorf("failed to read consistent range descriptor for key %s: %s", key, err)
		}
	}

	// Verify the transaction record is gone.
	start := engine.MakeMVCCMetadataKey(keys.MakeRangeKeyPrefix(roachpb.RKeyMin))
	end := engine.MakeMVCCMetadataKey(keys.MakeRangeKeyPrefix(roachpb.RKeyMax))
	iter := store.Engine().NewIterator(false)
	defer iter.Close()
	for iter.Seek(start); iter.Valid() && iter.Less(end); iter.Next() {
		if !bytes.HasPrefix([]byte(iter.Key().Key), keys.RangeDescriptorKey(roachpb.RKeyMin)) &&
			!bytes.HasPrefix([]byte(iter.Key().Key), keys.RangeDescriptorKey(roachpb.RKey(splitKey))) {
			t.Errorf("unexpected system key: %s; txn record should have been cleaned up", iter.Key())
		}
	}
}

// TestStoreRangeSplitAtRangeBounds verifies a range cannot be split
// at its start or end keys (would create zero-length range!). This
// sort of thing might happen in the wild if two split requests
// arrived for same key. The first one succeeds and second would try
// to split at the start of the newly split range.
func TestStoreRangeSplitAtRangeBounds(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sCtx := storage.TestStoreContext()
	sCtx.TestingKnobs.DisableSplitQueue = true
	store, stopper, _ := createTestStoreWithContext(t, sCtx)
	defer stopper.Stop()

	args := adminSplitArgs(roachpb.KeyMin, []byte("a"))
	if _, err := client.SendWrapped(rg1(store), nil, &args); err != nil {
		t.Fatal(err)
	}
	// This second split will try to split at end of first split range.
	if _, err := client.SendWrapped(rg1(store), nil, &args); err == nil {
		t.Fatalf("split succeeded unexpectedly")
	}
	// Now try to split at start of new range.
	args = adminSplitArgs(roachpb.KeyMin, []byte("a"))
	if _, err := client.SendWrapped(rg1(store), nil, &args); err == nil {
		t.Fatalf("split succeeded unexpectedly")
	}
}

// TestStoreRangeSplitConcurrent verifies that concurrent range splits
// of the same range are executed serially, and all but the first fail
// because the split key is invalid after the first split succeeds.
func TestStoreRangeSplitConcurrent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sCtx := storage.TestStoreContext()
	sCtx.TestingKnobs.DisableSplitQueue = true
	store, stopper, _ := createTestStoreWithContext(t, sCtx)
	defer stopper.Stop()

	splitKey := roachpb.Key("a")
	concurrentCount := 10
	errCh := make(chan *roachpb.Error, concurrentCount)
	for i := 0; i < concurrentCount; i++ {
		go func() {
			args := adminSplitArgs(roachpb.KeyMin, splitKey)
			_, pErr := client.SendWrapped(rg1(store), nil, &args)
			errCh <- pErr
		}()
	}

	var failureCount int
	for i := 0; i < concurrentCount; i++ {
		pErr := <-errCh
		if pErr != nil {
			// There are only three expected errors from concurrent splits:
			// conflicting range descriptors if the splits are initiated
			// concurrently, the range is already split at the specified key or the
			// split key is outside of the bounds for the range.
			expected := strings.Join([]string{
				"conflict updating range descriptors",
				"range is already split at key",
				"key range .* outside of bounds of range",
			}, "|")
			if !testutils.IsError(pErr.GoError(), expected) {
				t.Fatalf("unexpected error: %v", pErr)
			}
			failureCount++
		}
	}
	if failureCount != concurrentCount-1 {
		t.Fatalf("concurrent splits succeeded unexpectedly; failureCount=%d", failureCount)
	}

	// Verify everything ended up as expected.
	if a, e := store.ReplicaCount(), 2; a != e {
		t.Fatalf("expected %d stores after concurrent splits; actual count=%d", e, a)
	}
	rngDesc := store.LookupReplica(roachpb.RKeyMin, nil).Desc()
	newRngDesc := store.LookupReplica(roachpb.RKey(splitKey), nil).Desc()
	if !bytes.Equal(newRngDesc.StartKey, splitKey) || !bytes.Equal(splitKey, rngDesc.EndKey) {
		t.Errorf("ranges mismatched, wanted %q=%q=%q", newRngDesc.StartKey, splitKey, rngDesc.EndKey)
	}
	if !bytes.Equal(newRngDesc.EndKey, roachpb.RKeyMax) || !bytes.Equal(rngDesc.StartKey, roachpb.RKeyMin) {
		t.Errorf("new ranges do not cover KeyMin-KeyMax, but only %q-%q", rngDesc.StartKey, newRngDesc.EndKey)
	}
}

// TestStoreRangeSplitIdempotency executes a split of a range and
// verifies that the resulting ranges respond to the right key ranges
// and that their stats have been properly accounted for and requests
// can't be replayed.
func TestStoreRangeSplitIdempotency(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sCtx := storage.TestStoreContext()
	sCtx.TestingKnobs.DisableSplitQueue = true
	store, stopper, _ := createTestStoreWithContext(t, sCtx)
	defer stopper.Stop()
	rangeID := roachpb.RangeID(1)
	splitKey := roachpb.Key("m")
	content := roachpb.Key("asdvb")

	// First, write some values left and right of the proposed split key.
	pArgs := putArgs([]byte("c"), content)
	if _, pErr := client.SendWrapped(rg1(store), nil, &pArgs); pErr != nil {
		t.Fatal(pErr)
	}
	pArgs = putArgs([]byte("x"), content)
	if _, pErr := client.SendWrapped(rg1(store), nil, &pArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Increments are a good way of testing idempotency. Up here, we
	// address them to the original range, then later to the one that
	// contains the key.
	txn := roachpb.NewTransaction("test", []byte("c"), 10, enginepb.SERIALIZABLE,
		store.Clock().Now(), 0)
	lIncArgs := incrementArgs([]byte("apoptosis"), 100)
	lTxn := *txn
	lTxn.Sequence++
	if _, pErr := client.SendWrappedWith(rg1(store), nil, roachpb.Header{
		Txn: &lTxn,
	}, &lIncArgs); pErr != nil {
		t.Fatal(pErr)
	}
	rIncArgs := incrementArgs([]byte("wobble"), 10)
	rTxn := *txn
	rTxn.Sequence++
	if _, pErr := client.SendWrappedWith(rg1(store), nil, roachpb.Header{
		Txn: &rTxn,
	}, &rIncArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Get the original stats for key and value bytes.
	var ms enginepb.MVCCStats
	if err := engine.MVCCGetRangeStats(context.Background(), store.Engine(), rangeID, &ms); err != nil {
		t.Fatal(err)
	}
	keyBytes, valBytes := ms.KeyBytes, ms.ValBytes

	// Split the range.
	args := adminSplitArgs(roachpb.KeyMin, splitKey)
	if _, pErr := client.SendWrapped(rg1(store), nil, &args); pErr != nil {
		t.Fatal(pErr)
	}

	// Verify no intents remains on range descriptor keys.
	splitKeyAddr, err := keys.Addr(splitKey)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []roachpb.Key{keys.RangeDescriptorKey(roachpb.RKeyMin), keys.RangeDescriptorKey(splitKeyAddr)} {
		if _, _, err := engine.MVCCGet(context.Background(), store.Engine(), key, store.Clock().Now(), true, nil); err != nil {
			t.Fatal(err)
		}
	}

	rng := store.LookupReplica(roachpb.RKeyMin, nil)
	rngDesc := rng.Desc()
	newRng := store.LookupReplica([]byte("m"), nil)
	newRngDesc := newRng.Desc()
	if !bytes.Equal(newRngDesc.StartKey, splitKey) || !bytes.Equal(splitKey, rngDesc.EndKey) {
		t.Errorf("ranges mismatched, wanted %q=%q=%q", newRngDesc.StartKey, splitKey, rngDesc.EndKey)
	}
	if !bytes.Equal(newRngDesc.EndKey, roachpb.RKeyMax) || !bytes.Equal(rngDesc.StartKey, roachpb.RKeyMin) {
		t.Errorf("new ranges do not cover KeyMin-KeyMax, but only %q-%q", rngDesc.StartKey, newRngDesc.EndKey)
	}

	// Try to get values from both left and right of where the split happened.
	gArgs := getArgs([]byte("c"))
	if reply, pErr := client.SendWrapped(rg1(store), nil, &gArgs); pErr != nil {
		t.Fatal(pErr)
	} else if replyBytes, pErr := reply.(*roachpb.GetResponse).Value.GetBytes(); pErr != nil {
		t.Fatal(pErr)
	} else if !bytes.Equal(replyBytes, content) {
		t.Fatalf("actual value %q did not match expected value %q", replyBytes, content)
	}
	gArgs = getArgs([]byte("x"))
	if reply, pErr := client.SendWrappedWith(rg1(store), nil, roachpb.Header{
		RangeID: newRng.RangeID,
	}, &gArgs); pErr != nil {
		t.Fatal(pErr)
	} else if replyBytes, err := reply.(*roachpb.GetResponse).Value.GetBytes(); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(replyBytes, content) {
		t.Fatalf("actual value %q did not match expected value %q", replyBytes, content)
	}

	// Send out an increment request copied from above (same txn/sequence)
	// which remains in the old range.
	_, pErr := client.SendWrappedWith(rg1(store), nil, roachpb.Header{
		Txn: &lTxn,
	}, &lIncArgs)
	if _, ok := pErr.GetDetail().(*roachpb.TransactionRetryError); !ok {
		t.Fatalf("unexpected idempotency failure: %v", pErr)
	}

	// Send out the same increment copied from above (same txn/sequence), but
	// now to the newly created range (which should hold that key).
	_, pErr = client.SendWrappedWith(rg1(store), nil, roachpb.Header{
		RangeID: newRng.RangeID,
		Txn:     &rTxn,
	}, &rIncArgs)
	if _, ok := pErr.GetDetail().(*roachpb.TransactionRetryError); !ok {
		t.Fatalf("unexpected idempotency failure: %v", pErr)
	}

	// Compare stats of split ranges to ensure they are non zero and
	// exceed the original range when summed.
	var left, right enginepb.MVCCStats
	if err := engine.MVCCGetRangeStats(context.Background(), store.Engine(), rangeID, &left); err != nil {
		t.Fatal(err)
	}
	lKeyBytes, lValBytes := left.KeyBytes, left.ValBytes
	if err := engine.MVCCGetRangeStats(context.Background(), store.Engine(), newRng.RangeID, &right); err != nil {
		t.Fatal(err)
	}
	rKeyBytes, rValBytes := right.KeyBytes, right.ValBytes

	if lKeyBytes == 0 || rKeyBytes == 0 {
		t.Errorf("expected non-zero key bytes; got %d, %d", lKeyBytes, rKeyBytes)
	}
	if lValBytes == 0 || rValBytes == 0 {
		t.Errorf("expected non-zero val bytes; got %d, %d", lValBytes, rValBytes)
	}
	if lKeyBytes+rKeyBytes <= keyBytes {
		t.Errorf("left + right key bytes don't match; %d + %d <= %d", lKeyBytes, rKeyBytes, keyBytes)
	}
	if lValBytes+rValBytes <= valBytes {
		t.Errorf("left + right val bytes don't match; %d + %d <= %d", lValBytes, rValBytes, valBytes)
	}
}

// TestStoreRangeSplitStats starts by splitting the system keys from user-space
// keys and verifying that the user space side of the split (which is empty),
// has all zeros for stats. It then writes random data to the user space side,
// splits it halfway and verifies the two splits have stats exactly equaling
// the pre-split.
func TestStoreRangeSplitStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sCtx := storage.TestStoreContext()
	sCtx.TestingKnobs.DisableSplitQueue = true
	store, stopper, manual := createTestStoreWithContext(t, sCtx)
	defer stopper.Stop()

	// Split the range after the last table data key.
	keyPrefix := keys.MakeTablePrefix(keys.MaxReservedDescID + 1)
	keyPrefix = keys.MakeRowSentinelKey(keyPrefix)
	args := adminSplitArgs(roachpb.KeyMin, keyPrefix)
	if _, pErr := client.SendWrapped(rg1(store), nil, &args); pErr != nil {
		t.Fatal(pErr)
	}
	// Verify empty range has empty stats.
	rng := store.LookupReplica(keyPrefix, nil)
	// NOTE that this value is expected to change over time, depending on what
	// we store in the sys-local keyspace. Update it accordingly for this test.
	empty := enginepb.MVCCStats{LastUpdateNanos: manual.UnixNano()}
	if err := verifyRangeStats(store.Engine(), rng.RangeID, empty); err != nil {
		t.Fatal(err)
	}

	// Write random data.
	midKey := writeRandomDataToRange(t, store, rng.RangeID, keyPrefix)

	// Get the range stats now that we have data.
	snap := store.Engine().NewSnapshot()
	defer snap.Close()
	var ms enginepb.MVCCStats
	if err := engine.MVCCGetRangeStats(context.Background(), snap, rng.RangeID, &ms); err != nil {
		t.Fatal(err)
	}
	if err := verifyRecomputedStats(snap, rng.Desc(), ms, manual.UnixNano()); err != nil {
		t.Fatalf("failed to verify range's stats before split: %v", err)
	}
	if inMemMS := rng.GetMVCCStats(); inMemMS != ms {
		t.Fatalf("in-memory and on-disk diverged:\n%+v\n!=\n%+v", inMemMS, ms)
	}

	manual.Increment(100)

	// Split the range at approximate halfway point.
	args = adminSplitArgs(keyPrefix, midKey)
	if _, pErr := client.SendWrappedWith(rg1(store), nil, roachpb.Header{
		RangeID: rng.RangeID,
	}, &args); pErr != nil {
		t.Fatal(pErr)
	}

	snap = store.Engine().NewSnapshot()
	defer snap.Close()
	var msLeft, msRight enginepb.MVCCStats
	if err := engine.MVCCGetRangeStats(context.Background(), snap, rng.RangeID, &msLeft); err != nil {
		t.Fatal(err)
	}
	rngRight := store.LookupReplica(midKey, nil)
	if err := engine.MVCCGetRangeStats(context.Background(), snap, rngRight.RangeID, &msRight); err != nil {
		t.Fatal(err)
	}

	// The stats should be exactly equal when added.
	expMS := enginepb.MVCCStats{
		LiveBytes:   msLeft.LiveBytes + msRight.LiveBytes,
		KeyBytes:    msLeft.KeyBytes + msRight.KeyBytes,
		ValBytes:    msLeft.ValBytes + msRight.ValBytes,
		IntentBytes: msLeft.IntentBytes + msRight.IntentBytes,
		LiveCount:   msLeft.LiveCount + msRight.LiveCount,
		KeyCount:    msLeft.KeyCount + msRight.KeyCount,
		ValCount:    msLeft.ValCount + msRight.ValCount,
		IntentCount: msLeft.IntentCount + msRight.IntentCount,
	}
	ms.SysBytes, ms.SysCount = 0, 0
	ms.LastUpdateNanos = 0
	if expMS != ms {
		t.Errorf("expected left plus right ranges to equal original, but\n %+v\n+\n %+v\n!=\n %+v", msLeft, msRight, ms)
	}

	// Stats should both have the new timestamp.
	now := manual.UnixNano()
	if lTs := msLeft.LastUpdateNanos; lTs != now {
		t.Errorf("expected left range stats to have new timestamp, want %d, got %d", now, lTs)
	}
	if rTs := msRight.LastUpdateNanos; rTs != now {
		t.Errorf("expected right range stats to have new timestamp, want %d, got %d", now, rTs)
	}

	// Stats should agree with recomputation.
	if err := verifyRecomputedStats(snap, rng.Desc(), msLeft, now); err != nil {
		t.Fatalf("failed to verify left range's stats after split: %v", err)
	}
	if err := verifyRecomputedStats(snap, rngRight.Desc(), msRight, now); err != nil {
		t.Fatalf("failed to verify right range's stats after split: %v", err)
	}
}

// TestStoreRangeSplitStatsWithMerges starts by splitting the system keys from
// user-space keys and verifying that the user space side of the split (which is empty),
// has all zeros for stats. It then issues a number of Merge requests to the user
// space side, simulating TimeSeries data. Finally, the test splits the user space
// side halfway and verifies the stats on either side of the split are equal to a
// recomputation.
//
// Note that unlike TestStoreRangeSplitStats, we do not check if the two halves of the
// split's stats are equal to the pre-split stats when added, because this will not be
// true of ranges populated with Merge requests. The reason for this is that Merge
// requests' impact on MVCCStats are only estimated. See updateStatsOnMerge.
func TestStoreRangeSplitStatsWithMerges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sCtx := storage.TestStoreContext()
	sCtx.TestingKnobs.DisableSplitQueue = true
	store, stopper, manual := createTestStoreWithContext(t, sCtx)
	defer stopper.Stop()

	// Split the range after the last table data key.
	keyPrefix := keys.MakeTablePrefix(keys.MaxReservedDescID + 1)
	keyPrefix = keys.MakeRowSentinelKey(keyPrefix)
	args := adminSplitArgs(roachpb.KeyMin, keyPrefix)
	if _, pErr := client.SendWrapped(rg1(store), nil, &args); pErr != nil {
		t.Fatal(pErr)
	}
	// Verify empty range has empty stats.
	rng := store.LookupReplica(keyPrefix, nil)
	// NOTE that this value is expected to change over time, depending on what
	// we store in the sys-local keyspace. Update it accordingly for this test.
	empty := enginepb.MVCCStats{LastUpdateNanos: manual.UnixNano()}
	if err := verifyRangeStats(store.Engine(), rng.RangeID, empty); err != nil {
		t.Fatal(err)
	}

	// Write random TimeSeries data.
	midKey := writeRandomTimeSeriesDataToRange(t, store, rng.RangeID, keyPrefix)
	manual.Increment(100)

	// Split the range at approximate halfway point.
	args = adminSplitArgs(keyPrefix, midKey)
	if _, pErr := client.SendWrappedWith(rg1(store), nil, roachpb.Header{
		RangeID: rng.RangeID,
	}, &args); pErr != nil {
		t.Fatal(pErr)
	}

	snap := store.Engine().NewSnapshot()
	defer snap.Close()
	var msLeft, msRight enginepb.MVCCStats
	if err := engine.MVCCGetRangeStats(context.Background(), snap, rng.RangeID, &msLeft); err != nil {
		t.Fatal(err)
	}
	rngRight := store.LookupReplica(midKey, nil)
	if err := engine.MVCCGetRangeStats(context.Background(), snap, rngRight.RangeID, &msRight); err != nil {
		t.Fatal(err)
	}

	// Stats should both have the new timestamp.
	now := manual.UnixNano()
	if lTs := msLeft.LastUpdateNanos; lTs != now {
		t.Errorf("expected left range stats to have new timestamp, want %d, got %d", now, lTs)
	}
	if rTs := msRight.LastUpdateNanos; rTs != now {
		t.Errorf("expected right range stats to have new timestamp, want %d, got %d", now, rTs)
	}

	// Stats should agree with recomputation.
	if err := verifyRecomputedStats(snap, rng.Desc(), msLeft, now); err != nil {
		t.Fatalf("failed to verify left range's stats after split: %v", err)
	}
	if err := verifyRecomputedStats(snap, rngRight.Desc(), msRight, now); err != nil {
		t.Fatalf("failed to verify right range's stats after split: %v", err)
	}
}

// fillRange writes keys with the given prefix and associated values
// until bytes bytes have been written or the given range has split.
func fillRange(store *storage.Store, rangeID roachpb.RangeID, prefix roachpb.Key, bytes int64, t *testing.T) {
	src := rand.New(rand.NewSource(0))
	for {
		var ms enginepb.MVCCStats
		if err := engine.MVCCGetRangeStats(context.Background(), store.Engine(), rangeID, &ms); err != nil {
			t.Fatal(err)
		}
		keyBytes, valBytes := ms.KeyBytes, ms.ValBytes
		if keyBytes+valBytes >= bytes {
			return
		}
		key := append(append([]byte(nil), prefix...), randutil.RandBytes(src, 100)...)
		key = keys.MakeRowSentinelKey(key)
		val := randutil.RandBytes(src, int(src.Int31n(1<<8)))
		pArgs := putArgs(key, val)
		_, pErr := client.SendWrappedWith(store, nil, roachpb.Header{
			RangeID: rangeID,
		}, &pArgs)
		// When the split occurs in the background, our writes may start failing.
		// We know we can stop writing when this happens.
		if _, ok := pErr.GetDetail().(*roachpb.RangeKeyMismatchError); ok {
			return
		} else if pErr != nil {
			t.Fatal(pErr)
		}
	}
}

// TestStoreZoneUpdateAndRangeSplit verifies that modifying the zone
// configuration changes range max bytes and Range.maybeSplit() takes
// max bytes into account when deciding whether to enqueue a range for
// splitting. It further verifies that the range is in fact split on
// exceeding zone's RangeMaxBytes.
func TestStoreZoneUpdateAndRangeSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	store, stopper, _ := createTestStore(t)
	config.TestingSetupZoneConfigHook(stopper)
	defer stopper.Stop()

	const maxBytes = 1 << 16
	// Set max bytes.
	descID := uint32(keys.MaxReservedDescID + 1)
	config.TestingSetZoneConfig(descID, config.ZoneConfig{RangeMaxBytes: maxBytes})

	// Trigger gossip callback.
	if err := store.Gossip().AddInfoProto(gossip.KeySystemConfig, &config.SystemConfig{}, 0); err != nil {
		t.Fatal(err)
	}

	tableBoundary := keys.MakeTablePrefix(descID)

	{
		var rng *storage.Replica

		// Wait for the range to be split along table boundaries.
		expectedRSpan := roachpb.RSpan{Key: roachpb.RKey(tableBoundary), EndKey: roachpb.RKeyMax}
		util.SucceedsSoon(t, func() error {
			rng = store.LookupReplica(tableBoundary, nil)
			if actualRSpan := rng.Desc().RSpan(); !actualRSpan.Equal(expectedRSpan) {
				return errors.Errorf("expected range %s to span %s", rng, expectedRSpan)
			}
			return nil
		})

		// Check range's max bytes settings.
		if actualMaxBytes := rng.GetMaxBytes(); actualMaxBytes != maxBytes {
			t.Fatalf("range %s max bytes mismatch, got: %d, expected: %d", rng, actualMaxBytes, maxBytes)
		}

		// Look in the range after prefix we're writing to.
		fillRange(store, rng.RangeID, tableBoundary, maxBytes, t)
	}

	// Verify that the range is in fact split.
	util.SucceedsSoon(t, func() error {
		rng := store.LookupReplica(keys.MakeTablePrefix(descID+1), nil)
		rngDesc := rng.Desc()
		rngStart, rngEnd := rngDesc.StartKey, rngDesc.EndKey
		if rngStart.Equal(tableBoundary) || !rngEnd.Equal(roachpb.RKeyMax) {
			return errors.Errorf("range %s has not yet split", rng)
		}
		return nil
	})
}

// TestStoreRangeSplitWithMaxBytesUpdate tests a scenario where a new
// zone config that updates the max bytes is set and triggers a range
// split.
func TestStoreRangeSplitWithMaxBytesUpdate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	store, stopper, _ := createTestStore(t)
	config.TestingSetupZoneConfigHook(stopper)
	defer stopper.Stop()

	origRng := store.LookupReplica(roachpb.RKeyMin, nil)

	// Set max bytes.
	const maxBytes = 1 << 16
	descID := uint32(keys.MaxReservedDescID + 1)
	config.TestingSetZoneConfig(descID, config.ZoneConfig{RangeMaxBytes: maxBytes})

	// Trigger gossip callback.
	if err := store.Gossip().AddInfoProto(gossip.KeySystemConfig, &config.SystemConfig{}, 0); err != nil {
		t.Fatal(err)
	}

	// Verify that the range is split and the new range has the correct max bytes.
	util.SucceedsSoon(t, func() error {
		newRng := store.LookupReplica(keys.MakeTablePrefix(descID), nil)
		if newRng.RangeID == origRng.RangeID {
			return errors.Errorf("expected new range created by split")
		}
		if newRng.GetMaxBytes() != maxBytes {
			return errors.Errorf("expected %d max bytes for the new range, but got %d",
				maxBytes, newRng.GetMaxBytes())
		}
		return nil
	})
}

// TestStoreRangeSystemSplits verifies that splits are based on the contents of
// the SystemConfig span.
func TestStoreRangeSystemSplits(t *testing.T) {
	defer leaktest.AfterTest(t)()
	store, stopper, _ := createTestStore(t)
	defer stopper.Stop()

	schema := sqlbase.MakeMetadataSchema()
	initialSystemValues := schema.GetInitialValues()
	var userTableMax int
	// Write the initial sql values to the system DB as well
	// as the equivalent of table descriptors for X user tables.
	// This does two things:
	// - descriptor IDs are used to determine split keys
	// - the write triggers a SystemConfig update and gossip.
	// We should end up with splits at each user table prefix.
	if err := store.DB().Txn(func(txn *client.Txn) error {
		prefix := keys.MakeTablePrefix(keys.DescriptorTableID)
		txn.SetSystemConfigTrigger()
		for i, kv := range initialSystemValues {
			if !bytes.HasPrefix(kv.Key, prefix) {
				continue
			}
			bytes, err := kv.Value.GetBytes()
			if err != nil {
				log.Info(context.TODO(), err)
				continue
			}
			if err := txn.Put(kv.Key, bytes); err != nil {
				return err
			}

			descID := keys.MaxReservedDescID + i + 1
			userTableMax = i + 1

			// We don't care about the values, just the keys.
			k := sqlbase.MakeDescMetadataKey(sqlbase.ID(descID))
			if err := txn.Put(k, bytes); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	verifySplitsAtTablePrefixes := func(maxTableID int) {
		// We expect splits at each of the user tables, but not at the system
		// tables boundaries.
		expKeys := make([]roachpb.Key, 0, maxTableID+2)

		// We can't simply set numReservedTables to schema.TableCount(), because
		// some system tables are created at cluster bootstrap time. So, before the
		// cluster bootstrap, TableCount() will return a value that's too low.
		numReservedTables := schema.MaxTableID() - keys.MaxSystemConfigDescID
		for i := 1; i <= int(numReservedTables); i++ {
			expKeys = append(expKeys,
				testutils.MakeKey(keys.Meta2Prefix,
					keys.MakeTablePrefix(keys.MaxSystemConfigDescID+uint32(i))),
			)
		}
		for i := 1; i <= maxTableID; i++ {
			expKeys = append(expKeys,
				testutils.MakeKey(keys.Meta2Prefix, keys.MakeTablePrefix(keys.MaxReservedDescID+uint32(i))),
			)
		}
		expKeys = append(expKeys, testutils.MakeKey(keys.Meta2Prefix, roachpb.RKeyMax))

		util.SucceedsSoonDepth(1, t, func() error {
			rows, err := store.DB().Scan(keys.Meta2Prefix, keys.MetaMax, 0)
			if err != nil {
				return err
			}
			keys := make([]roachpb.Key, 0, len(expKeys))
			for _, r := range rows {
				keys = append(keys, r.Key)
			}
			if !reflect.DeepEqual(keys, expKeys) {
				return errors.Errorf("expected split keys:\n%v\nbut found:\n%v", expKeys, keys)
			}
			return nil
		})
	}

	verifySplitsAtTablePrefixes(userTableMax)

	numTotalValues := keys.MaxSystemConfigDescID + server.ExpectedInitialRangeCount()

	// Write another, disjoint descriptor for a user table.
	if err := store.DB().Txn(func(txn *client.Txn) error {
		txn.SetSystemConfigTrigger()
		// This time, only write the last table descriptor. Splits
		// still occur for every intervening ID.
		// We don't care about the values, just the keys.
		k := sqlbase.MakeDescMetadataKey(sqlbase.ID(keys.MaxReservedDescID + numTotalValues))
		return txn.Put(k, &sqlbase.TableDescriptor{})
	}); err != nil {
		t.Fatal(err)
	}

	verifySplitsAtTablePrefixes(numTotalValues)
}

// setupSplitSnapshotRace engineers a situation in which a range has
// been split but node 3 hasn't processed it yet. There is a race
// depending on whether node 3 learns of the split from its left or
// right side. When this function returns most of the nodes will be
// stopped, and depending on the order in which they are restarted, we
// can arrange for both possible outcomes of the race.
//
// Range 1 is the system keyspace, located on node 0.
//
// The range containing leftKey is the left side of the split, located
// on nodes 1, 2, and 3.
//
// The range containing rightKey is the right side of the split,
// located on nodes 3, 4, and 5.
//
// Nodes 1-5 are stopped; only node 0 is running.
//
// See https://github.com/cockroachdb/cockroach/issues/1644.
func setupSplitSnapshotRace(t *testing.T) (mtc *multiTestContext, leftKey roachpb.Key, rightKey roachpb.Key) {
	mtc = startMultiTestContext(t, 6)

	leftKey = roachpb.Key("a")
	rightKey = roachpb.Key("z")

	// First, do a couple of writes; we'll use these to determine when
	// the dust has settled.
	incArgs := incrementArgs(leftKey, 1)
	if _, pErr := client.SendWrapped(rg1(mtc.stores[0]), nil, &incArgs); pErr != nil {
		t.Fatal(pErr)
	}
	incArgs = incrementArgs(rightKey, 2)
	if _, pErr := client.SendWrapped(rg1(mtc.stores[0]), nil, &incArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Split the system range from the rest of the keyspace.
	splitArgs := adminSplitArgs(roachpb.KeyMin, keys.SystemMax)
	if _, pErr := client.SendWrapped(rg1(mtc.stores[0]), nil, &splitArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Get the left range's ID. This is currently 2, but using
	// LookupReplica is more future-proof (and see below for
	// rightRangeID).
	leftRangeID := mtc.stores[0].LookupReplica(roachpb.RKey("a"), nil).RangeID

	// Replicate the left range onto nodes 1-3 and remove it from node 0.
	mtc.replicateRange(leftRangeID, 1, 2, 3)
	mtc.unreplicateRange(leftRangeID, 0)
	mtc.expireLeases()

	mtc.waitForValues(leftKey, []int64{0, 1, 1, 1, 0, 0})
	mtc.waitForValues(rightKey, []int64{0, 2, 2, 2, 0, 0})

	// Stop node 3 so it doesn't hear about the split.
	mtc.stopStore(3)
	mtc.expireLeases()

	// Split the data range.
	splitArgs = adminSplitArgs(keys.SystemMax, roachpb.Key("m"))
	if _, pErr := client.SendWrapped(mtc.distSenders[0], nil, &splitArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Get the right range's ID. Since the split was performed on node
	// 1, it is currently 11 and not 3 as might be expected.
	var rightRangeID roachpb.RangeID
	util.SucceedsSoon(t, func() error {
		rightRangeID = mtc.stores[1].LookupReplica(roachpb.RKey("z"), nil).RangeID
		if rightRangeID == leftRangeID {
			return errors.Errorf("store 1 hasn't processed split yet")
		}
		return nil
	})

	// Relocate the right range onto nodes 3-5.
	mtc.replicateRange(rightRangeID, 4, 5)
	mtc.unreplicateRange(rightRangeID, 2)
	mtc.unreplicateRange(rightRangeID, 1)

	// Perform another increment after all the replication changes. This
	// lets us ensure that all the replication changes have been
	// processed and applied on all replicas. This is necessary because
	// the range is in an unstable state at the time of the last
	// unreplicateRange call above. It has four members which means it
	// can only tolerate one failure without losing quorum. That failure
	// is store 3 which we stopped earlier. Stopping store 1 too soon
	// (before it has committed the final config change *and* propagated
	// that commit to the followers 4 and 5) would constitute a second
	// failure and render the range unable to achieve quorum after
	// restart (in the SnapshotWins branch).
	incArgs = incrementArgs(rightKey, 3)
	if _, pErr := client.SendWrapped(mtc.distSenders[0], nil, &incArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Store 3 still has the old value, but 4 and 5 are up to date.
	mtc.waitForValues(rightKey, []int64{0, 0, 0, 2, 5, 5})

	// Stop the remaining data stores.
	mtc.stopStore(1)
	mtc.stopStore(2)
	// 3 is already stopped.
	mtc.stopStore(4)
	mtc.stopStore(5)
	mtc.expireLeases()

	return mtc, leftKey, rightKey
}

// TestSplitSnapshotRace_SplitWins exercises one outcome of the
// split/snapshot race: The left side of the split propagates first,
// so the split completes before it sees a competing snapshot. This is
// the more common outcome in practice.
func TestSplitSnapshotRace_SplitWins(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc, leftKey, rightKey := setupSplitSnapshotRace(t)
	defer mtc.Stop()

	// Bring the left range up first so that the split happens before it sees a snapshot.
	for i := 1; i <= 3; i++ {
		mtc.restartStore(i)
	}

	// Perform a write on the left range and wait for it to propagate.
	incArgs := incrementArgs(leftKey, 10)
	if _, pErr := client.SendWrapped(mtc.distSenders[0], nil, &incArgs); pErr != nil {
		t.Fatal(pErr)
	}
	mtc.waitForValues(leftKey, []int64{0, 11, 11, 11, 0, 0})

	// Now wake the other stores up.
	mtc.restartStore(4)
	mtc.restartStore(5)

	// Write to the right range.
	incArgs = incrementArgs(rightKey, 20)
	if _, pErr := client.SendWrapped(mtc.distSenders[0], nil, &incArgs); pErr != nil {
		t.Fatal(pErr)
	}
	mtc.waitForValues(rightKey, []int64{0, 0, 0, 25, 25, 25})
}

// TestSplitSnapshotRace_SnapshotWins exercises one outcome of the
// split/snapshot race: The right side of the split replicates first,
// so target node sees a raft snapshot before it has processed the split,
// so it still has a conflicting range.
func TestSplitSnapshotRace_SnapshotWins(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc, leftKey, rightKey := setupSplitSnapshotRace(t)
	defer mtc.Stop()

	// Bring the right range up first.
	for i := 3; i <= 5; i++ {
		mtc.restartStore(i)
	}

	// Perform a write on the right range.
	incArgs := incrementArgs(rightKey, 20)
	if _, pErr := client.SendWrapped(mtc.distSenders[0], nil, &incArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// It immediately propagates between nodes 4 and 5, but node 3
	// remains at its old value. It can't accept the right-hand range
	// because it conflicts with its not-yet-split copy of the left-hand
	// range. This test is not completely deterministic: we want to make
	// sure that node 3 doesn't panic when it receives the snapshot, but
	// since it silently drops the message there is nothing we can wait
	// for. There is a high probability that the message will have been
	// received by the time that nodes 4 and 5 have processed their
	// update.
	mtc.waitForValues(rightKey, []int64{0, 0, 0, 2, 25, 25})

	// Wake up the left-hand range. This will allow the left-hand
	// range's split to complete and unblock the right-hand range.
	mtc.restartStore(1)
	mtc.restartStore(2)

	// Perform writes on both sides. This is not strictly necessary but
	// it helps wake up dormant ranges that would otherwise have to wait
	// for retry timeouts.
	incArgs = incrementArgs(leftKey, 10)
	if _, pErr := client.SendWrapped(mtc.distSenders[0], nil, &incArgs); pErr != nil {
		t.Fatal(pErr)
	}
	mtc.waitForValues(leftKey, []int64{0, 11, 11, 11, 0, 0})

	incArgs = incrementArgs(rightKey, 200)
	if _, pErr := client.SendWrapped(mtc.distSenders[0], nil, &incArgs); pErr != nil {
		t.Fatal(pErr)
	}
	mtc.waitForValues(rightKey, []int64{0, 0, 0, 225, 225, 225})
}

// TestStoreSplitTimestampCacheReadRace prevents regression of #3148. It begins
// a couple of read requests and lets them complete while a split is happening;
// the reads hit the right half of the split. If the split happens
// non-atomically with respect to the reads (and in particular their update of
// the timestamp cache), then some of them may not be reflected in the
// timestamp cache of the new range, in which case this test would fail.
func TestStoreSplitTimestampCacheReadRace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	splitKey := roachpb.Key("a")
	key := func(i int) roachpb.Key {
		splitCopy := append([]byte(nil), splitKey.Next()...)
		return append(splitCopy, []byte(fmt.Sprintf("%03d", i))...)
	}

	getContinues := make(chan struct{})
	var getStarted sync.WaitGroup
	sCtx := storage.TestStoreContext()
	sCtx.TestingKnobs.DisableSplitQueue = true
	sCtx.TestingKnobs.TestingCommandFilter =
		func(filterArgs storagebase.FilterArgs) *roachpb.Error {
			if et, ok := filterArgs.Req.(*roachpb.EndTransactionRequest); ok {
				st := et.InternalCommitTrigger.GetSplitTrigger()
				if st == nil || !st.LeftDesc.EndKey.Equal(splitKey) {
					return nil
				}
				close(getContinues)
			} else if filterArgs.Req.Method() == roachpb.Get &&
				bytes.HasPrefix(filterArgs.Req.Header().Key, splitKey.Next()) {
				getStarted.Done()
				<-getContinues
			}
			return nil
		}
	store, stopper, _ := createTestStoreWithContext(t, sCtx)
	defer stopper.Stop()

	now := store.Clock().Now()
	var wg sync.WaitGroup

	ts := func(i int) hlc.Timestamp {
		return now.Add(0, int32(1000+i))
	}

	const num = 10

	for i := 0; i < num; i++ {
		wg.Add(1)
		getStarted.Add(1)
		go func(i int) {
			defer wg.Done()
			args := getArgs(key(i))
			var h roachpb.Header
			h.Timestamp = ts(i)
			if _, pErr := client.SendWrappedWith(rg1(store), nil, h, &args); pErr != nil {
				t.Fatal(pErr)
			}
		}(i)
	}

	getStarted.Wait()

	wg.Add(1)
	func() {
		defer wg.Done()
		args := adminSplitArgs(roachpb.KeyMin, splitKey)
		if _, pErr := client.SendWrapped(rg1(store), nil, &args); pErr != nil {
			t.Fatal(pErr)
		}
	}()

	wg.Wait()

	for i := 0; i < num; i++ {
		var h roachpb.Header
		h.Timestamp = now
		args := putArgs(key(i), []byte("foo"))
		keyAddr, err := keys.Addr(args.Key)
		if err != nil {
			t.Fatal(err)
		}
		h.RangeID = store.LookupReplica(keyAddr, nil).RangeID
		_, respH, pErr := storage.SendWrapped(store, context.Background(), h, &args)
		if pErr != nil {
			t.Fatal(pErr)
		}
		if respH.Timestamp.Less(ts(i)) {
			t.Fatalf("%d: expected Put to be forced higher than %s by timestamp caches, but wrote at %s", i, ts(i), respH.Timestamp)
		}
	}
}

// TestStoreSplitTimestampCacheDifferentLeaseHolder prevents regression of
// #7899. When the first lease holder of the right-hand side of a Split was
// not equal to the left-hand side lease holder (at the time of the split),
// its timestamp cache would not be properly initialized, which would allow
// for writes which invalidated reads previously served by the pre-split lease.
func TestStoreSplitTimestampCacheDifferentLeaseHolder(t *testing.T) {
	defer leaktest.AfterTest(t)()

	leftKey := roachpb.Key("a")
	splitKey := roachpb.Key("b")
	rightKey := roachpb.Key("c")

	// This filter is better understood when reading the meat of the test
	// below first.
	var noLeaseForDesc atomic.Value
	var numLeases int32
	filter := func(args storagebase.FilterArgs) *roachpb.Error {
		leaseReq, argOK := args.Req.(*roachpb.RequestLeaseRequest)
		forbiddenDesc, descOK := noLeaseForDesc.Load().(*roachpb.ReplicaDescriptor)
		if !argOK || !descOK || !bytes.Equal(leaseReq.Key, splitKey) {
			return nil
		}
		atomic.AddInt32(&numLeases, 1)
		if !reflect.DeepEqual(*forbiddenDesc, leaseReq.Lease.Replica) {
			return nil
		}
		log.Infof(context.TODO(), "refusing %+v because %+v held lease for LHS of split",
			leaseReq, forbiddenDesc)
		return roachpb.NewError(&roachpb.NotLeaseHolderError{RangeID: args.Hdr.RangeID})
	}

	var args base.TestClusterArgs
	args.ReplicationMode = base.ReplicationManual
	args.ServerArgs.Knobs.Store = &storage.StoreTestingKnobs{
		TestingCommandFilter: filter,
	}

	tc := testcluster.StartTestCluster(t, 2, args)
	defer tc.Stopper().Stop()

	// Split the data range, mainly to avoid other splits getting in our way.
	for _, k := range []roachpb.Key{leftKey, rightKey} {
		if _, _, err := tc.SplitRange(k); err != nil {
			t.Fatal(errors.Wrapf(err, "split at %s", k))
		}
	}
	if _, err := tc.AddReplicas(leftKey, tc.Target(1)); err != nil {
		t.Fatal(err)
	}

	db := tc.Servers[0].DB() // irrelevant which one we use

	// Make a context tied to the Stopper. The test works without, but this
	// is cleaner since we won't properly terminate the transaction below.
	ctx := tc.Server(0).Stopper().WithCancel(context.Background())

	// This transaction will try to write "under" a served read.
	txnOld := client.NewTxn(ctx, *db)

	// Do something with txnOld so that its timestamp gets set.
	if _, err := txnOld.Scan("a", "b", 0); err != nil {
		t.Fatal(err)
	}

	// Another client comes along at a higher timestamp, touching everything on
	// the right of the (soon-to-be) split key.
	if _, err := db.Scan(splitKey, rightKey, 0); err != nil {
		t.Fatal(err)
	}

	// This block makes sure that from now on, we don't allow the current
	// lease holder of our range to extend. Any attempt of doing so will
	// catch a NotLeaseHolderError, which means a retry by DistSender (until
	// the other node gets to be the lease holder instead).
	//
	// This makes sure that once we split, we'll be in the situation described
	// in #7899 (before the fix): The first lease holder of the right hand side
	// of the Split will not be that of the pre-split Range.
	// With the fix, the right-hand lease is initialized from the left-hand
	// lease, so the lease holders are the same, and there will never be a
	// lease request for the right-hand side in this test.
	leaseHolder := func(k roachpb.Key) roachpb.ReplicaDescriptor {
		desc, err := tc.LookupRange(k)
		if err != nil {
			t.Fatal(err)
		}
		leaseHolder, err := tc.FindRangeLeaseHolder(&desc, nil)
		if err != nil {
			t.Fatal(err)
		}
		replica, found := desc.GetReplicaDescriptor(leaseHolder.StoreID)
		if !found {
			t.Fatalf("no replica on store %d found in %+v", leaseHolder.StoreID, desc)
		}
		return replica
	}
	blacklistedLeaseHolder := leaseHolder(leftKey)
	noLeaseForDesc.Store(&blacklistedLeaseHolder)

	// Pull the trigger. This actually also reads the RHS descriptor after the
	// split, so when this returns, we've got the leases set up already.
	//
	// There's a slight race here: Just above, we've settled on who must not
	// be the future lease holder. But between then and now, that lease could
	// have expired and the other Replica could have obtained it. This would
	// have given it a proper initialization of the timestamp cache, and so
	// the split trigger would populate the right hand side with a timestamp
	// cache which does not exhibit the anomaly.
	//
	// In practice, this should only be possible if second-long delays occur
	// just above this comment, and we assert against it below.
	if _, _, err := tc.SplitRange(splitKey); err != nil {
		t.Fatal(err)
	}

	if currentLHSLeaseHolder := leaseHolder(leftKey); !reflect.DeepEqual(
		currentLHSLeaseHolder, blacklistedLeaseHolder) {
		t.Fatalf("lease holder changed from %+v to %+v, should de-flake this test",
			blacklistedLeaseHolder, currentLHSLeaseHolder)
	}

	// This write (to the right-hand side of the split) should hit the
	// timestamp cache and flag the txn for a restart when we try to commit it
	// below. With the bug in #7899, the RHS of the split had an empty
	// timestamp cache and would simply let us write behind the previous read.
	if err := txnOld.Put("bb", "bump"); err != nil {
		t.Fatal(err)
	}

	if err := txnOld.Commit(); !testutils.IsError(err, "retry txn") {
		t.Fatalf("expected txn retry, got %v", err)
	}

	// As outlined above, the anomaly was fixed by giving the right-hand side
	// of the split the same lease as the left-hand side of the Split. Check
	// that that's what's happened (we actually test a little more, namely
	// that it's the same ReplicaID, which is not required but should always
	// hold).
	if rhsLease := leaseHolder(rightKey); !reflect.DeepEqual(
		rhsLease, blacklistedLeaseHolder,
	) {
		t.Errorf("expected LHS and RHS to have same lease holder")
	}
	if num := atomic.LoadInt32(&numLeases); num > 0 {
		t.Errorf("expected to see no lease requests for the right-hand side")
	}
}

// TestStoreRangeSplitRaceUninitializedRHS reproduces #7600 (before it was
// fixed). While splits are happening, we simulate incoming messages for the
// right-hand side to trigger a race between the creation of the proper replica
// and the uninitialized replica reacting to messages.
func TestStoreRangeSplitRaceUninitializedRHS(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := &multiTestContext{}
	storeCtx := storage.TestStoreContext()
	// An aggressive tick interval lets groups communicate more and thus
	// triggers test failures much more reliably. We can't go too aggressive
	// or race tests never make any progress.
	storeCtx.RaftTickInterval = 50 * time.Millisecond
	storeCtx.RaftElectionTimeoutTicks = 2
	currentTrigger := make(chan *roachpb.SplitTrigger)
	seen := make(map[storagebase.CmdIDKey]struct{})
	storeCtx.TestingKnobs.TestingCommandFilter = func(args storagebase.FilterArgs) *roachpb.Error {
		et, ok := args.Req.(*roachpb.EndTransactionRequest)
		if !ok || et.InternalCommitTrigger == nil {
			return nil
		}
		trigger := protoutil.Clone(et.InternalCommitTrigger.GetSplitTrigger()).(*roachpb.SplitTrigger)
		if trigger != nil && len(trigger.RightDesc.Replicas) == 2 && args.Hdr.Txn.Epoch == 0 && args.Sid == mtc.stores[0].StoreID() {
			if _, ok := seen[args.CmdID]; ok {
				return nil
			}
			// Without replay protection, a single reproposal locks up the
			// test.
			seen[args.CmdID] = struct{}{}
			currentTrigger <- trigger
			return roachpb.NewError(roachpb.NewReadWithinUncertaintyIntervalError(args.Hdr.Timestamp, args.Hdr.Timestamp))
		}
		return nil
	}

	mtc.storeContext = &storeCtx
	mtc.Start(t, 2)
	defer mtc.Stop()

	leftRange := mtc.stores[0].LookupReplica(roachpb.RKey("a"), nil)

	// Replicate the left range onto the second node. We don't wait since we
	// don't actually care what the second node does. All we want is that the
	// first node isn't surprised by messages from that node.
	mtc.replicateRange(leftRange.RangeID, 1)

	for i := 0; i < 10; i++ {
		var wg sync.WaitGroup
		wg.Add(2)

		// Closed when the split goroutine is done.
		splitDone := make(chan struct{})

		go func() {
			defer wg.Done()
			defer close(splitDone)

			// Split the data range. The split keys are chosen so that they move
			// towards "a" (so that the range being split is always the first
			// range).
			splitKey := roachpb.Key(encoding.EncodeVarintDescending([]byte("a"), int64(i)))
			splitArgs := adminSplitArgs(keys.SystemMax, splitKey)
			if _, pErr := client.SendWrapped(mtc.distSenders[0], nil, &splitArgs); pErr != nil {
				t.Fatal(pErr)
			}
		}()
		go func() {
			defer wg.Done()

			trigger := <-currentTrigger // our own copy
			// Make sure the first node is first for convenience.
			replicas := trigger.RightDesc.Replicas
			if replicas[0].NodeID > replicas[1].NodeID {
				tmp := replicas[1]
				replicas[1] = replicas[0]
				replicas[0] = tmp
			}

			// Send a few vote requests which look like they're from the other
			// node's right hand side of the split. This triggers a race which
			// is discussed in #7600 (briefly, the creation of the right hand
			// side in the split trigger was racing with the uninitialized
			// version for the same group, resulting in clobbered HardState).
			for term := uint64(1); ; term++ {
				if err := mtc.stores[0].HandleRaftMessage(&storage.RaftMessageRequest{
					RangeID:     trigger.RightDesc.RangeID,
					ToReplica:   replicas[0],
					FromReplica: replicas[1],
					Message: raftpb.Message{
						Type: raftpb.MsgVote,
						To:   uint64(replicas[0].ReplicaID),
						From: uint64(replicas[1].ReplicaID),
						Term: term,
					},
				}); err != nil {
					t.Error(err)
				}
				select {
				case <-splitDone:
					return
				case <-time.After(time.Microsecond):
					// If we busy-loop here, we monopolize processRaftMu and the
					// split takes a long time to complete. Sleeping reduces the
					// chance that we hit the race, but it still shows up under
					// stress.
				}
			}
		}()
		wg.Wait()
	}
}

// TestLeaderAfterSplit verifies that a raft group created by a split
// elects a leader without waiting for an election timeout.
func TestLeaderAfterSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	storeContext := storage.TestStoreContext()
	storeContext.RaftElectionTimeoutTicks = 1000000
	mtc := &multiTestContext{
		storeContext: &storeContext,
	}
	mtc.Start(t, 3)
	defer mtc.Stop()

	mtc.replicateRange(1, 1, 2)

	leftKey := roachpb.Key("a")
	splitKey := roachpb.Key("m")
	rightKey := roachpb.Key("z")

	splitArgs := adminSplitArgs(roachpb.KeyMin, splitKey)
	if _, pErr := client.SendWrapped(mtc.distSenders[0], nil, &splitArgs); pErr != nil {
		t.Fatal(pErr)
	}

	incArgs := incrementArgs(leftKey, 1)
	if _, pErr := client.SendWrapped(mtc.distSenders[0], nil, &incArgs); pErr != nil {
		t.Fatal(pErr)
	}

	incArgs = incrementArgs(rightKey, 2)
	if _, pErr := client.SendWrapped(mtc.distSenders[0], nil, &incArgs); pErr != nil {
		t.Fatal(pErr)
	}
}

func BenchmarkStoreRangeSplit(b *testing.B) {
	defer tracing.Disable()()
	sCtx := storage.TestStoreContext()
	sCtx.TestingKnobs.DisableSplitQueue = true
	store, stopper, _ := createTestStoreWithContext(b, sCtx)
	defer stopper.Stop()

	// Perform initial split of ranges.
	sArgs := adminSplitArgs(roachpb.KeyMin, []byte("b"))
	if _, err := client.SendWrapped(rg1(store), nil, &sArgs); err != nil {
		b.Fatal(err)
	}

	// Write some values left and right of the split key.
	aDesc := store.LookupReplica([]byte("a"), nil).Desc()
	bDesc := store.LookupReplica([]byte("c"), nil).Desc()
	writeRandomDataToRange(b, store, aDesc.RangeID, []byte("aaa"))
	writeRandomDataToRange(b, store, bDesc.RangeID, []byte("ccc"))

	// Merge the b range back into the a range.
	mArgs := adminMergeArgs(roachpb.KeyMin)
	if _, err := client.SendWrapped(rg1(store), nil, &mArgs); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Split the range.
		b.StartTimer()
		if _, err := client.SendWrapped(rg1(store), nil, &sArgs); err != nil {
			b.Fatal(err)
		}

		// Merge the ranges.
		b.StopTimer()
		if _, err := client.SendWrapped(rg1(store), nil, &mArgs); err != nil {
			b.Fatal(err)
		}
	}
}

func writeRandomDataToRange(
	t testing.TB,
	store *storage.Store,
	rangeID roachpb.RangeID,
	keyPrefix []byte,
) (midpoint []byte) {
	src := rand.New(rand.NewSource(0))
	for i := 0; i < 100; i++ {
		key := append([]byte(nil), keyPrefix...)
		key = append(key, randutil.RandBytes(src, int(src.Int31n(1<<7)))...)
		key = keys.MakeRowSentinelKey(key)
		val := randutil.RandBytes(src, int(src.Int31n(1<<8)))
		pArgs := putArgs(key, val)
		if _, pErr := client.SendWrappedWith(rg1(store), nil, roachpb.Header{
			RangeID: rangeID,
		}, &pArgs); pErr != nil {
			t.Fatal(pErr)
		}
	}
	// Return approximate midway point ("Z" in string "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz").
	midKey := append([]byte(nil), keyPrefix...)
	midKey = append(midKey, []byte("Z")...)
	return keys.MakeRowSentinelKey(midKey)
}

func writeRandomTimeSeriesDataToRange(
	t testing.TB,
	store *storage.Store,
	rangeID roachpb.RangeID,
	keyPrefix []byte,
) (midpoint []byte) {
	src := rand.New(rand.NewSource(0))
	r := ts.Resolution10s
	for i := 0; i < 20; i++ {
		var data []tspb.TimeSeriesData
		for j := int64(0); j <= src.Int63n(5); j++ {
			d := tspb.TimeSeriesData{
				Name:   "test.random.metric",
				Source: "cpu01",
			}
			for k := int64(0); k <= src.Int63n(10); k++ {
				d.Datapoints = append(d.Datapoints, tspb.TimeSeriesDatapoint{
					TimestampNanos: src.Int63n(200) * r.KeyDuration(),
					Value:          src.Float64(),
				})
			}
			data = append(data, d)
		}
		for _, d := range data {
			idatas, err := d.ToInternal(r.KeyDuration(), r.SampleDuration())
			if err != nil {
				t.Fatal(err)
			}
			for _, idata := range idatas {
				var value roachpb.Value
				if err := value.SetProto(&idata); err != nil {
					t.Fatal(err)
				}
				mArgs := roachpb.MergeRequest{
					Span: roachpb.Span{
						Key: encoding.EncodeVarintAscending(keyPrefix, idata.StartTimestampNanos),
					},
					Value: value,
				}
				if _, pErr := client.SendWrappedWith(rg1(store), nil, roachpb.Header{
					RangeID: rangeID,
				}, &mArgs); pErr != nil {
					t.Fatal(pErr)
				}
			}
		}
	}
	// Return approximate midway point (100 is midway between random timestamps in range [0,200)).
	midKey := append([]byte(nil), keyPrefix...)
	midKey = encoding.EncodeVarintAscending(midKey, 100*r.KeyDuration())
	return keys.MakeRowSentinelKey(midKey)
}
