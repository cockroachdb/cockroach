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
	"math"
	"math/rand"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

func adminSplitArgs(key, splitKey roachpb.Key) *roachpb.AdminSplitRequest {
	return &roachpb.AdminSplitRequest{
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	cfg := storage.TestStoreConfig(nil)
	cfg.TestingKnobs.DisableSplitQueue = true
	store := createTestStoreWithConfig(t, stopper, cfg)

	for _, key := range []roachpb.Key{
		keys.Meta1Prefix,
		testutils.MakeKey(keys.Meta1Prefix, []byte("a")),
		testutils.MakeKey(keys.Meta1Prefix, roachpb.RKeyMax),
		keys.Meta2KeyMax,
		keys.MakeTablePrefix(10 /* system descriptor ID */),
	} {
		args := adminSplitArgs(roachpb.KeyMin, key)
		_, pErr := client.SendWrapped(context.Background(), rg1(store), args)
		if !testutils.IsPError(pErr, "cannot split") {
			t.Errorf("%q: unexpected split error %s", key, pErr)
		}
	}
}

// TestStoreRangeSplitAtTablePrefix verifies a range can be split at
// UserTableDataMin and still gossip the SystemConfig properly.
func TestStoreRangeSplitAtTablePrefix(t *testing.T) {
	defer leaktest.AfterTest(t)()
	storeCfg := storage.TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableSplitQueue = true
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	store := createTestStoreWithConfig(t, stopper, storeCfg)

	key := keys.MakeRowSentinelKey(append([]byte(nil), keys.UserTableDataMin...))
	args := adminSplitArgs(key, key)
	if _, pErr := client.SendWrapped(context.Background(), rg1(store), args); pErr != nil {
		t.Fatalf("%q: split unexpected error: %s", key, pErr)
	}

	var desc sqlbase.TableDescriptor
	descBytes, err := protoutil.Marshal(&desc)
	if err != nil {
		t.Fatal(err)
	}

	// Update SystemConfig to trigger gossip.
	if err := store.DB().Txn(context.TODO(), func(ctx context.Context, txn *client.Txn) error {
		if err := txn.SetSystemConfigTrigger(); err != nil {
			return err
		}
		// We don't care about the values, just the keys.
		k := sqlbase.MakeDescMetadataKey(sqlbase.ID(keys.MaxReservedDescID + 1))
		return txn.Put(ctx, k, &desc)
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
	storeCfg := storage.TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableSplitQueue = true
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	store := createTestStoreWithConfig(t, stopper, storeCfg)

	// Manually create some the column keys corresponding to the table:
	//
	//   CREATE TABLE t (id STRING PRIMARY KEY, col1 INT, col2 INT)
	tableKey := keys.MakeTablePrefix(keys.MaxReservedDescID + 1)
	rowKey := roachpb.Key(encoding.EncodeVarintAscending(append([]byte(nil), tableKey...), 1))
	rowKey = encoding.EncodeStringAscending(encoding.EncodeVarintAscending(rowKey, 1), "a")
	col1Key := keys.MakeFamilyKey(append([]byte(nil), rowKey...), 1)
	col2Key := keys.MakeFamilyKey(append([]byte(nil), rowKey...), 2)

	// We don't care about the value, so just store any old thing.
	if err := store.DB().Put(context.TODO(), col1Key, "column 1"); err != nil {
		t.Fatal(err)
	}
	if err := store.DB().Put(context.TODO(), col2Key, "column 2"); err != nil {
		t.Fatal(err)
	}

	// Split between col1Key and col2Key by splitting before col2Key.
	args := adminSplitArgs(col2Key, col2Key)
	_, err := client.SendWrapped(context.Background(), rg1(store), args)
	if err != nil {
		t.Fatalf("%s: split unexpected error: %s", col1Key, err)
	}

	repl1 := store.LookupReplica(col1Key, nil)
	repl2 := store.LookupReplica(col2Key, nil)
	// Verify the two columns are still on the same range.
	if !reflect.DeepEqual(repl1, repl2) {
		t.Fatalf("%s: ranges differ: %+v vs %+v", roachpb.Key(col1Key), repl1, repl2)
	}
	// Verify we split on a row key.
	if startKey := repl1.Desc().StartKey; !startKey.Equal(rowKey) {
		t.Fatalf("%s: expected split on %s, but found %s",
			roachpb.Key(col1Key), roachpb.Key(rowKey), startKey)
	}

	// Verify the previous range was split on a row key.
	repl3 := store.LookupReplica(tableKey, nil)
	if endKey := repl3.Desc().EndKey; !endKey.Equal(rowKey) {
		t.Fatalf("%s: expected split on %s, but found %s",
			roachpb.Key(col1Key), roachpb.Key(rowKey), endKey)
	}
}

// TestStoreRangeSplitIntents executes a split of a range and verifies
// that all intents are cleared and the transaction record cleaned up.
func TestStoreRangeSplitIntents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	storeCfg := storage.TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableSplitQueue = true
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	store := createTestStoreWithConfig(t, stopper, storeCfg)

	// First, write some values left and right of the proposed split key.
	pArgs := putArgs([]byte("c"), []byte("foo"))
	if _, pErr := client.SendWrapped(context.Background(), rg1(store), pArgs); pErr != nil {
		t.Fatal(pErr)
	}
	pArgs = putArgs([]byte("x"), []byte("bar"))
	if _, pErr := client.SendWrapped(context.Background(), rg1(store), pArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Split the range.
	splitKey := roachpb.Key("m")
	args := adminSplitArgs(roachpb.KeyMin, splitKey)
	if _, pErr := client.SendWrapped(context.Background(), rg1(store), args); pErr != nil {
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

	txnPrefix := func(key roachpb.Key) roachpb.Key {
		rk, err := keys.Addr(key)
		if err != nil {
			t.Fatal(err)
		}
		return keys.MakeRangeKey(rk, keys.LocalTransactionSuffix, nil)
	}
	// Verify the transaction record is gone.
	start := engine.MakeMVCCMetadataKey(keys.MakeRangeKeyPrefix(roachpb.RKeyMin))
	end := engine.MakeMVCCMetadataKey(keys.MakeRangeKeyPrefix(roachpb.RKeyMax))
	iter := store.Engine().NewIterator(false)

	defer iter.Close()
	for iter.Seek(start); ; iter.Next() {
		if ok, err := iter.Valid(); err != nil {
			t.Fatal(err)
		} else if !ok || !iter.Less(end) {
			break
		}

		if bytes.HasPrefix([]byte(iter.Key().Key), txnPrefix(roachpb.KeyMin)) ||
			bytes.HasPrefix([]byte(iter.Key().Key), txnPrefix(splitKey)) {
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
	storeCfg := storage.TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableSplitQueue = true
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	store := createTestStoreWithConfig(t, stopper, storeCfg)

	args := adminSplitArgs(roachpb.KeyMin, []byte("a"))
	if _, err := client.SendWrapped(context.Background(), rg1(store), args); err != nil {
		t.Fatal(err)
	}
	// This second split will try to split at end of first split range.
	if _, err := client.SendWrapped(context.Background(), rg1(store), args); err == nil {
		t.Fatalf("split succeeded unexpectedly")
	}
	// Now try to split at start of new range.
	args = adminSplitArgs(roachpb.KeyMin, []byte("a"))
	if _, err := client.SendWrapped(context.Background(), rg1(store), args); err == nil {
		t.Fatalf("split succeeded unexpectedly")
	}
}

// TestStoreRangeSplitConcurrent verifies that concurrent range splits
// of the same range are executed serially, and all but the first fail
// because the split key is invalid after the first split succeeds.
func TestStoreRangeSplitConcurrent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	storeCfg := storage.TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableSplitQueue = true
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	store := createTestStoreWithConfig(t, stopper, storeCfg)

	splitKey := roachpb.Key("a")
	concurrentCount := 10
	errCh := make(chan *roachpb.Error, concurrentCount)
	for i := 0; i < concurrentCount; i++ {
		go func() {
			args := adminSplitArgs(roachpb.KeyMin, splitKey)
			_, pErr := client.SendWrapped(context.Background(), rg1(store), args)
			errCh <- pErr
		}()
	}

	var failureCount int
	for i := 0; i < concurrentCount; i++ {
		pErr := <-errCh
		if pErr != nil {
			// The only expected error from concurrent splits is the split key being
			// outside the bounds for the range. Note that conflicting range
			// descriptor errors are retried internally.
			if !testutils.IsError(pErr.GoError(), "key range .* outside of bounds of range") {
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
	storeCfg := storage.TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableSplitQueue = true
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	store := createTestStoreWithConfig(t, stopper, storeCfg)
	rangeID := roachpb.RangeID(1)
	splitKey := roachpb.Key("m")
	content := roachpb.Key("asdvb")

	// First, write some values left and right of the proposed split key.
	pArgs := putArgs([]byte("c"), content)
	if _, pErr := client.SendWrapped(context.Background(), rg1(store), pArgs); pErr != nil {
		t.Fatal(pErr)
	}
	pArgs = putArgs([]byte("x"), content)
	if _, pErr := client.SendWrapped(context.Background(), rg1(store), pArgs); pErr != nil {
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
	if _, pErr := client.SendWrappedWith(context.Background(), rg1(store), roachpb.Header{
		Txn: &lTxn,
	}, lIncArgs); pErr != nil {
		t.Fatal(pErr)
	}
	rIncArgs := incrementArgs([]byte("wobble"), 10)
	rTxn := *txn
	rTxn.Sequence++
	if _, pErr := client.SendWrappedWith(context.Background(), rg1(store), roachpb.Header{
		Txn: &rTxn,
	}, rIncArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Get the original stats for key and value bytes.
	ms, err := engine.MVCCGetRangeStats(context.Background(), store.Engine(), rangeID)
	if err != nil {
		t.Fatal(err)
	}
	keyBytes, valBytes := ms.KeyBytes, ms.ValBytes

	// Split the range.
	args := adminSplitArgs(roachpb.KeyMin, splitKey)
	if _, pErr := client.SendWrapped(context.Background(), rg1(store), args); pErr != nil {
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

	repl := store.LookupReplica(roachpb.RKeyMin, nil)
	rngDesc := repl.Desc()
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
	if reply, pErr := client.SendWrapped(context.Background(), rg1(store), gArgs); pErr != nil {
		t.Fatal(pErr)
	} else if replyBytes, pErr := reply.(*roachpb.GetResponse).Value.GetBytes(); pErr != nil {
		t.Fatal(pErr)
	} else if !bytes.Equal(replyBytes, content) {
		t.Fatalf("actual value %q did not match expected value %q", replyBytes, content)
	}
	gArgs = getArgs([]byte("x"))
	if reply, pErr := client.SendWrappedWith(context.Background(), rg1(store), roachpb.Header{
		RangeID: newRng.RangeID,
	}, gArgs); pErr != nil {
		t.Fatal(pErr)
	} else if replyBytes, err := reply.(*roachpb.GetResponse).Value.GetBytes(); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(replyBytes, content) {
		t.Fatalf("actual value %q did not match expected value %q", replyBytes, content)
	}

	// Send out an increment request copied from above (same txn/sequence)
	// which remains in the old range.
	_, pErr := client.SendWrappedWith(context.Background(), rg1(store), roachpb.Header{
		Txn: &lTxn,
	}, lIncArgs)
	if _, ok := pErr.GetDetail().(*roachpb.TransactionRetryError); !ok {
		t.Fatalf("unexpected idempotency failure: %v", pErr)
	}

	// Send out the same increment copied from above (same txn/sequence), but
	// now to the newly created range (which should hold that key).
	_, pErr = client.SendWrappedWith(context.Background(), rg1(store), roachpb.Header{
		RangeID: newRng.RangeID,
		Txn:     &rTxn,
	}, rIncArgs)
	if _, ok := pErr.GetDetail().(*roachpb.TransactionRetryError); !ok {
		t.Fatalf("unexpected idempotency failure: %v", pErr)
	}

	// Compare stats of split ranges to ensure they are non zero and
	// exceed the original range when summed.
	left, err := engine.MVCCGetRangeStats(context.Background(), store.Engine(), rangeID)
	if err != nil {
		t.Fatal(err)
	}
	lKeyBytes, lValBytes := left.KeyBytes, left.ValBytes
	right, err := engine.MVCCGetRangeStats(context.Background(), store.Engine(), newRng.RangeID)
	if err != nil {
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
	manual := hlc.NewManualClock(123)
	storeCfg := storage.TestStoreConfig(hlc.NewClock(manual.UnixNano, time.Nanosecond))
	storeCfg.TestingKnobs.DisableSplitQueue = true
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	store := createTestStoreWithConfig(t, stopper, storeCfg)

	// Split the range after the last table data key.
	keyPrefix := keys.MakeTablePrefix(keys.MaxReservedDescID + 1)
	keyPrefix = keys.MakeRowSentinelKey(keyPrefix)
	args := adminSplitArgs(roachpb.KeyMin, keyPrefix)
	if _, pErr := client.SendWrapped(context.Background(), rg1(store), args); pErr != nil {
		t.Fatal(pErr)
	}
	// Verify empty range has empty stats.
	repl := store.LookupReplica(keyPrefix, nil)
	// NOTE that this value is expected to change over time, depending on what
	// we store in the sys-local keyspace. Update it accordingly for this test.
	empty := enginepb.MVCCStats{LastUpdateNanos: manual.UnixNano()}
	if err := verifyRangeStats(store.Engine(), repl.RangeID, empty); err != nil {
		t.Fatal(err)
	}

	// Write random data.
	midKey := writeRandomDataToRange(t, store, repl.RangeID, keyPrefix)

	// Get the range stats now that we have data.
	snap := store.Engine().NewSnapshot()
	defer snap.Close()
	ms, err := engine.MVCCGetRangeStats(context.Background(), snap, repl.RangeID)
	if err != nil {
		t.Fatal(err)
	}
	if err := verifyRecomputedStats(snap, repl.Desc(), ms, manual.UnixNano()); err != nil {
		t.Fatalf("failed to verify range's stats before split: %v", err)
	}
	if inMemMS := repl.GetMVCCStats(); inMemMS != ms {
		t.Fatalf("in-memory and on-disk diverged:\n%+v\n!=\n%+v", inMemMS, ms)
	}

	manual.Increment(100)

	// Split the range at approximate halfway point.
	args = adminSplitArgs(keyPrefix, midKey)
	if _, pErr := client.SendWrappedWith(context.Background(), rg1(store), roachpb.Header{
		RangeID: repl.RangeID,
	}, args); pErr != nil {
		t.Fatal(pErr)
	}

	snap = store.Engine().NewSnapshot()
	defer snap.Close()
	msLeft, err := engine.MVCCGetRangeStats(context.Background(), snap, repl.RangeID)
	if err != nil {
		t.Fatal(err)
	}
	replRight := store.LookupReplica(midKey, nil)
	msRight, err := engine.MVCCGetRangeStats(context.Background(), snap, replRight.RangeID)
	if err != nil {
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
	if err := verifyRecomputedStats(snap, repl.Desc(), msLeft, now); err != nil {
		t.Fatalf("failed to verify left range's stats after split: %v", err)
	}
	if err := verifyRecomputedStats(snap, replRight.Desc(), msRight, now); err != nil {
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
	manual := hlc.NewManualClock(123)
	storeCfg := storage.TestStoreConfig(hlc.NewClock(manual.UnixNano, time.Nanosecond))
	storeCfg.TestingKnobs.DisableSplitQueue = true
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	store := createTestStoreWithConfig(t, stopper, storeCfg)

	// Split the range after the last table data key.
	keyPrefix := keys.MakeTablePrefix(keys.MaxReservedDescID + 1)
	keyPrefix = keys.MakeRowSentinelKey(keyPrefix)
	args := adminSplitArgs(roachpb.KeyMin, keyPrefix)
	if _, pErr := client.SendWrapped(context.Background(), rg1(store), args); pErr != nil {
		t.Fatal(pErr)
	}
	// Verify empty range has empty stats.
	repl := store.LookupReplica(keyPrefix, nil)
	// NOTE that this value is expected to change over time, depending on what
	// we store in the sys-local keyspace. Update it accordingly for this test.
	empty := enginepb.MVCCStats{LastUpdateNanos: manual.UnixNano()}
	if err := verifyRangeStats(store.Engine(), repl.RangeID, empty); err != nil {
		t.Fatal(err)
	}

	// Write random TimeSeries data.
	midKey := writeRandomTimeSeriesDataToRange(t, store, repl.RangeID, keyPrefix)
	manual.Increment(100)

	// Split the range at approximate halfway point.
	args = adminSplitArgs(keyPrefix, midKey)
	if _, pErr := client.SendWrappedWith(context.Background(), rg1(store), roachpb.Header{
		RangeID: repl.RangeID,
	}, args); pErr != nil {
		t.Fatal(pErr)
	}

	snap := store.Engine().NewSnapshot()
	defer snap.Close()
	msLeft, err := engine.MVCCGetRangeStats(context.Background(), snap, repl.RangeID)
	if err != nil {
		t.Fatal(err)
	}
	replRight := store.LookupReplica(midKey, nil)
	msRight, err := engine.MVCCGetRangeStats(context.Background(), snap, replRight.RangeID)
	if err != nil {
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
	if err := verifyRecomputedStats(snap, repl.Desc(), msLeft, now); err != nil {
		t.Fatalf("failed to verify left range's stats after split: %v", err)
	}
	if err := verifyRecomputedStats(snap, replRight.Desc(), msRight, now); err != nil {
		t.Fatalf("failed to verify right range's stats after split: %v", err)
	}
}

// fillRange writes keys with the given prefix and associated values
// until bytes bytes have been written or the given range has split.
func fillRange(
	store *storage.Store, rangeID roachpb.RangeID, prefix roachpb.Key, bytes int64, t *testing.T,
) {
	src := rand.New(rand.NewSource(0))
	for {
		ms, err := engine.MVCCGetRangeStats(context.Background(), store.Engine(), rangeID)
		if err != nil {
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
		_, pErr := client.SendWrappedWith(context.Background(), store, roachpb.Header{
			RangeID: rangeID,
		}, pArgs)
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	store, _ := createTestStore(t, stopper)
	config.TestingSetupZoneConfigHook(stopper)

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
		var repl *storage.Replica

		// Wait for the range to be split along table boundaries.
		expectedRSpan := roachpb.RSpan{Key: roachpb.RKey(tableBoundary), EndKey: roachpb.RKeyMax}
		testutils.SucceedsSoon(t, func() error {
			repl = store.LookupReplica(tableBoundary, nil)
			if actualRSpan := repl.Desc().RSpan(); !actualRSpan.Equal(expectedRSpan) {
				return errors.Errorf("expected range %s to span %s", repl, expectedRSpan)
			}
			return nil
		})

		// Check range's max bytes settings.
		if actualMaxBytes := repl.GetMaxBytes(); actualMaxBytes != maxBytes {
			t.Fatalf("range %s max bytes mismatch, got: %d, expected: %d", repl, actualMaxBytes, maxBytes)
		}

		// Look in the range after prefix we're writing to.
		fillRange(store, repl.RangeID, tableBoundary, maxBytes, t)
	}

	// Verify that the range is in fact split.
	testutils.SucceedsSoon(t, func() error {
		repl := store.LookupReplica(keys.MakeTablePrefix(descID+1), nil)
		rngDesc := repl.Desc()
		rngStart, rngEnd := rngDesc.StartKey, rngDesc.EndKey
		if rngStart.Equal(tableBoundary) || !rngEnd.Equal(roachpb.RKeyMax) {
			return errors.Errorf("range %s has not yet split", repl)
		}
		return nil
	})
}

// TestStoreRangeSplitWithMaxBytesUpdate tests a scenario where a new
// zone config that updates the max bytes is set and triggers a range
// split.
func TestStoreRangeSplitWithMaxBytesUpdate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	store, _ := createTestStore(t, stopper)
	config.TestingSetupZoneConfigHook(stopper)

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
	testutils.SucceedsSoon(t, func() error {
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	store, _ := createTestStore(t, stopper)

	schema := sqlbase.MakeMetadataSchema()
	initialSystemValues := schema.GetInitialValues()
	var userTableMax int
	// Write the initial sql values to the system DB as well
	// as the equivalent of table descriptors for X user tables.
	// This does two things:
	// - descriptor IDs are used to determine split keys
	// - the write triggers a SystemConfig update and gossip.
	// We should end up with splits at each user table prefix.
	if err := store.DB().Txn(context.TODO(), func(ctx context.Context, txn *client.Txn) error {
		prefix := keys.MakeTablePrefix(keys.DescriptorTableID)
		if err := txn.SetSystemConfigTrigger(); err != nil {
			return err
		}
		for i, kv := range initialSystemValues {
			if !bytes.HasPrefix(kv.Key, prefix) {
				continue
			}
			bytes, err := kv.Value.GetBytes()
			if err != nil {
				log.Info(ctx, err)
				continue
			}
			if err := txn.Put(ctx, kv.Key, bytes); err != nil {
				return err
			}

			descID := keys.MaxReservedDescID + i + 1
			userTableMax = i + 1

			// We don't care about the values, just the keys.
			k := sqlbase.MakeDescMetadataKey(sqlbase.ID(descID))
			if err := txn.Put(ctx, k, bytes); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	verifySplitsAtTablePrefixes := func(maxTableID int) {
		// We expect splits at each of the user tables and at a few fixed system
		// range boundaries, but not at system table boundaries.
		expKeys := []roachpb.Key{
			testutils.MakeKey(keys.Meta2Prefix, keys.SystemPrefix),
			testutils.MakeKey(keys.Meta2Prefix, keys.TimeseriesPrefix),
			testutils.MakeKey(keys.Meta2Prefix, keys.TimeseriesPrefix.PrefixEnd()),
			testutils.MakeKey(keys.Meta2Prefix, keys.TableDataMin),
		}
		numReservedTables := schema.SystemDescriptorCount() - schema.SystemConfigDescriptorCount()
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

		testutils.SucceedsSoonDepth(1, t, func() error {
			rows, err := store.DB().Scan(context.TODO(), keys.Meta2Prefix, keys.MetaMax, 0)
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

	// Write another, disjoint (+3) descriptor for a user table.
	numTotalValues := userTableMax + 3
	if err := store.DB().Txn(context.TODO(), func(ctx context.Context, txn *client.Txn) error {
		if err := txn.SetSystemConfigTrigger(); err != nil {
			return err
		}
		// This time, only write the last table descriptor. Splits
		// still occur for every intervening ID.
		// We don't care about the values, just the keys.
		k := sqlbase.MakeDescMetadataKey(sqlbase.ID(keys.MaxReservedDescID + numTotalValues))
		return txn.Put(ctx, k, &sqlbase.TableDescriptor{})
	}); err != nil {
		t.Fatal(err)
	}

	verifySplitsAtTablePrefixes(numTotalValues)
}

// runSetupSplitSnapshotRace engineers a situation in which a range has
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
func runSetupSplitSnapshotRace(
	t *testing.T, testFn func(*multiTestContext, roachpb.Key, roachpb.Key),
) {
	sc := storage.TestStoreConfig(nil)
	// We'll control replication by hand.
	sc.TestingKnobs.DisableReplicateQueue = true
	// Async intent resolution can sometimes lead to hangs when we stop
	// most of the stores at the end of this function.
	sc.TestingKnobs.DisableAsyncIntentResolution = true
	mtc := &multiTestContext{storeConfig: &sc}
	defer mtc.Stop()
	mtc.Start(t, 6)

	leftKey := roachpb.Key("a")
	rightKey := roachpb.Key("z")

	// First, do a couple of writes; we'll use these to determine when
	// the dust has settled.
	incArgs := incrementArgs(leftKey, 1)
	if _, pErr := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), incArgs); pErr != nil {
		t.Fatal(pErr)
	}
	incArgs = incrementArgs(rightKey, 2)
	if _, pErr := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), incArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Split the system range from the rest of the keyspace.
	splitArgs := adminSplitArgs(roachpb.KeyMin, keys.SystemMax)
	if _, pErr := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), splitArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Get the left range's ID. This is currently 2, but using
	// LookupReplica is more future-proof (and see below for
	// rightRangeID).
	leftRangeID := mtc.stores[0].LookupReplica(roachpb.RKey("a"), nil).RangeID

	// Replicate the left range onto nodes 1-3 and remove it from node 0. We have
	// to transfer the lease before unreplicating from range 0 because it isn't
	// safe (or allowed) for a leaseholder to remove itself from a cluster
	// without first giving up its lease.
	mtc.replicateRange(leftRangeID, 1, 2, 3)
	mtc.transferLease(context.TODO(), leftRangeID, 0, 1)
	mtc.unreplicateRange(leftRangeID, 0)

	mtc.waitForValues(leftKey, []int64{0, 1, 1, 1, 0, 0})
	mtc.waitForValues(rightKey, []int64{0, 2, 2, 2, 0, 0})

	// Stop node 3 so it doesn't hear about the split.
	mtc.stopStore(3)
	mtc.advanceClock(context.TODO())

	// Split the data range.
	splitArgs = adminSplitArgs(keys.SystemMax, roachpb.Key("m"))
	if _, pErr := client.SendWrapped(context.Background(), mtc.distSenders[0], splitArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Get the right range's ID. Since the split was performed on node
	// 1, it is currently 11 and not 3 as might be expected.
	var rightRangeID roachpb.RangeID
	testutils.SucceedsSoon(t, func() error {
		rightRangeID = mtc.stores[1].LookupReplica(roachpb.RKey("z"), nil).RangeID
		if rightRangeID == leftRangeID {
			return errors.Errorf("store 1 hasn't processed split yet")
		}
		return nil
	})

	// Relocate the right range onto nodes 3-5.
	mtc.replicateRange(rightRangeID, 4, 5)
	mtc.unreplicateRange(rightRangeID, 2)
	mtc.transferLease(context.TODO(), rightRangeID, 1, 4)
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
	if _, pErr := client.SendWrapped(context.Background(), mtc.distSenders[0], incArgs); pErr != nil {
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

	testFn(mtc, leftKey, rightKey)
}

// TestSplitSnapshotRace_SplitWins exercises one outcome of the
// split/snapshot race: The left side of the split propagates first,
// so the split completes before it sees a competing snapshot. This is
// the more common outcome in practice.
func TestSplitSnapshotRace_SplitWins(t *testing.T) {
	defer leaktest.AfterTest(t)()
	runSetupSplitSnapshotRace(t, func(mtc *multiTestContext, leftKey, rightKey roachpb.Key) {
		// Bring the left range up first so that the split happens before it sees a snapshot.
		for i := 1; i <= 3; i++ {
			mtc.restartStore(i)
		}

		// Perform a write on the left range and wait for it to propagate.
		incArgs := incrementArgs(leftKey, 10)
		if _, pErr := client.SendWrapped(context.Background(), mtc.distSenders[0], incArgs); pErr != nil {
			t.Fatal(pErr)
		}
		mtc.waitForValues(leftKey, []int64{0, 11, 11, 11, 0, 0})

		// Now wake the other stores up.
		mtc.restartStore(4)
		mtc.restartStore(5)

		// Write to the right range.
		incArgs = incrementArgs(rightKey, 20)
		if _, pErr := client.SendWrapped(context.Background(), mtc.distSenders[0], incArgs); pErr != nil {
			t.Fatal(pErr)
		}
		mtc.waitForValues(rightKey, []int64{0, 0, 0, 25, 25, 25})
	})
}

// TestSplitSnapshotRace_SnapshotWins exercises one outcome of the
// split/snapshot race: The right side of the split replicates first,
// so the target node sees a raft snapshot before it has processed the
// split, so it still has a conflicting range.
func TestSplitSnapshotRace_SnapshotWins(t *testing.T) {
	defer leaktest.AfterTest(t)()
	runSetupSplitSnapshotRace(t, func(mtc *multiTestContext, leftKey, rightKey roachpb.Key) {
		// Bring the right range up first.
		for i := 3; i <= 5; i++ {
			mtc.restartStore(i)
		}

		// Perform a write on the right range.
		incArgs := incrementArgs(rightKey, 20)
		if _, pErr := client.SendWrapped(context.Background(), mtc.distSenders[0], incArgs); pErr != nil {
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
		if _, pErr := client.SendWrapped(context.Background(), mtc.distSenders[0], incArgs); pErr != nil {
			t.Fatal(pErr)
		}
		mtc.waitForValues(leftKey, []int64{0, 11, 11, 11, 0, 0})

		incArgs = incrementArgs(rightKey, 200)
		if _, pErr := client.SendWrapped(context.Background(), mtc.distSenders[0], incArgs); pErr != nil {
			t.Fatal(pErr)
		}
		mtc.waitForValues(rightKey, []int64{0, 0, 0, 225, 225, 225})
	})
}

// TestStoreSplitTimestampCacheDifferentLeaseHolder prevents regression of
// #7899. When the first lease holder of the right-hand side of a Split was
// not equal to the left-hand side lease holder (at the time of the split),
// its timestamp cache would not be properly initialized, which would allow
// for writes which invalidated reads previously served by the pre-split lease.
func TestStoreSplitTimestampCacheDifferentLeaseHolder(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	leftKey := roachpb.Key("a")
	splitKey := roachpb.Key("b")
	rightKey := roachpb.Key("c")

	// This filter is better understood when reading the meat of the test
	// below first.
	var noLeaseForDesc atomic.Value
	filter := func(args storagebase.FilterArgs) *roachpb.Error {
		leaseReq, argOK := args.Req.(*roachpb.RequestLeaseRequest)
		forbiddenDesc, descOK := noLeaseForDesc.Load().(*roachpb.ReplicaDescriptor)
		if !argOK || !descOK || !bytes.Equal(leaseReq.Key, splitKey) {
			return nil
		}
		log.Infof(ctx, "received lease request (%s, %s)",
			leaseReq.Span, leaseReq.Lease)
		if !reflect.DeepEqual(*forbiddenDesc, leaseReq.Lease.Replica) {
			return nil
		}
		log.Infof(ctx,
			"refusing lease request (%s, %s) because %+v held lease for LHS of split",
			leaseReq.Span, leaseReq.Lease, forbiddenDesc)
		return roachpb.NewError(&roachpb.NotLeaseHolderError{RangeID: args.Hdr.RangeID})
	}

	var args base.TestClusterArgs
	args.ReplicationMode = base.ReplicationManual
	args.ServerArgs.Knobs.Store = &storage.StoreTestingKnobs{
		TestingEvalFilter: filter,
	}

	tc := testcluster.StartTestCluster(t, 2, args)
	defer tc.Stopper().Stop(context.TODO())

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
	ctx = tc.Server(0).Stopper().WithCancel(ctx)

	// This transaction will try to write "under" a served read.
	txnOld := client.NewTxn(db)

	// Do something with txnOld so that its timestamp gets set.
	if _, err := txnOld.Scan(ctx, "a", "b", 0); err != nil {
		t.Fatal(err)
	}

	// Another client comes along at a higher timestamp, touching everything on
	// the right of the (soon-to-be) split key.
	if _, err := db.Scan(ctx, splitKey, rightKey, 0); err != nil {
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
		lease, _, err := tc.FindRangeLease(desc, nil)
		if err != nil {
			t.Fatal(err)
		}
		leaseHolder := lease.Replica
		replica, found := desc.GetReplicaDescriptor(leaseHolder.StoreID)
		if !found {
			t.Fatalf("no replica on store %d found in %+v", leaseHolder.StoreID, desc)
		}
		return replica
	}
	blacklistedLeaseHolder := leaseHolder(leftKey)
	log.Infof(ctx, "blacklisting replica %+v for leases", blacklistedLeaseHolder)
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
	log.Infof(ctx, "splitting at %s", splitKey)
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
	if err := txnOld.Put(ctx, "bb", "bump"); err != nil {
		t.Fatal(err)
	}

	if err := txnOld.Commit(ctx); !testutils.IsError(err, "retry txn") {
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
}

// TestStoreRangeSplitRaceUninitializedRHS reproduces #7600 (before it was
// fixed). While splits are happening, we simulate incoming messages for the
// right-hand side to trigger a race between the creation of the proper replica
// and the uninitialized replica reacting to messages.
func TestStoreRangeSplitRaceUninitializedRHS(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := &multiTestContext{}
	storeCfg := storage.TestStoreConfig(nil)
	// An aggressive tick interval lets groups communicate more and thus
	// triggers test failures much more reliably. We can't go too aggressive
	// or race tests never make any progress.
	storeCfg.RaftTickInterval = 50 * time.Millisecond
	storeCfg.RaftElectionTimeoutTicks = 2
	currentTrigger := make(chan *roachpb.SplitTrigger, 1)
	var seen struct {
		syncutil.Mutex
		sids map[storagebase.CmdIDKey][2]bool
	}
	seen.sids = make(map[storagebase.CmdIDKey][2]bool)

	storeCfg.TestingKnobs.TestingEvalFilter = func(args storagebase.FilterArgs) *roachpb.Error {
		et, ok := args.Req.(*roachpb.EndTransactionRequest)
		if !ok || et.InternalCommitTrigger == nil {
			return nil
		}
		trigger := protoutil.Clone(et.InternalCommitTrigger.GetSplitTrigger()).(*roachpb.SplitTrigger)
		// The first time the trigger arrives (on each of the two stores),
		// return a transaction retry. This allows us to pass the trigger to
		// the goroutine creating faux incoming messages for the yet
		// nonexistent right-hand-side, giving it a head start. This code looks
		// fairly complicated since it wants to ensure that the two replicas
		// don't diverge.
		if trigger != nil && len(trigger.RightDesc.Replicas) == 2 && args.Hdr.Txn.Epoch == 0 {
			seen.Lock()
			defer seen.Unlock()
			sid, sl := int(args.Sid)-1, seen.sids[args.CmdID]
			if !sl[sid] {
				sl[sid] = true
				seen.sids[args.CmdID] = sl
			} else {
				return nil
			}
			select {
			case currentTrigger <- trigger:
			default:
			}
			return roachpb.NewError(
				roachpb.NewReadWithinUncertaintyIntervalError(
					args.Hdr.Timestamp, args.Hdr.Timestamp,
				))
		}
		return nil
	}

	mtc.storeConfig = &storeCfg
	defer mtc.Stop()
	mtc.Start(t, 2)

	leftRange := mtc.stores[0].LookupReplica(roachpb.RKey("a"), nil)

	// Replicate the left range onto the second node. We don't wait since we
	// don't actually care what the second node does. All we want is that the
	// first node isn't surprised by messages from that node.
	mtc.replicateRange(leftRange.RangeID, 1)

	for i := 0; i < 10; i++ {
		errChan := make(chan *roachpb.Error)

		// Closed when the split goroutine is done.
		splitDone := make(chan struct{})

		go func() {
			defer close(splitDone)

			// Split the data range. The split keys are chosen so that they move
			// towards "a" (so that the range being split is always the first
			// range).
			splitKey := roachpb.Key(encoding.EncodeVarintDescending([]byte("a"), int64(i)))
			splitArgs := adminSplitArgs(keys.SystemMax, splitKey)
			_, pErr := client.SendWrapped(context.Background(), mtc.distSenders[0], splitArgs)
			errChan <- pErr
		}()
		go func() {
			defer func() { errChan <- nil }()

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
				if err := mtc.stores[0].HandleRaftRequest(context.Background(),
					&storage.RaftMessageRequest{
						RangeID:     trigger.RightDesc.RangeID,
						ToReplica:   replicas[0],
						FromReplica: replicas[1],
						Message: raftpb.Message{
							Type: raftpb.MsgVote,
							To:   uint64(replicas[0].ReplicaID),
							From: uint64(replicas[1].ReplicaID),
							Term: term,
						},
					}, nil); err != nil {
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
		for i := 0; i < 2; i++ {
			if pErr := <-errChan; pErr != nil {
				t.Fatal(pErr)
			}
		}
	}
}

// TestLeaderAfterSplit verifies that a raft group created by a split
// elects a leader without waiting for an election timeout.
func TestLeaderAfterSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	storeConfig := storage.TestStoreConfig(nil)
	storeConfig.RaftElectionTimeoutTicks = 1000000
	mtc := &multiTestContext{
		storeConfig: &storeConfig,
	}
	defer mtc.Stop()
	mtc.Start(t, 3)

	mtc.replicateRange(1, 1, 2)

	leftKey := roachpb.Key("a")
	splitKey := roachpb.Key("m")
	rightKey := roachpb.Key("z")

	splitArgs := adminSplitArgs(roachpb.KeyMin, splitKey)
	if _, pErr := client.SendWrapped(context.Background(), mtc.distSenders[0], splitArgs); pErr != nil {
		t.Fatal(pErr)
	}

	incArgs := incrementArgs(leftKey, 1)
	if _, pErr := client.SendWrapped(context.Background(), mtc.distSenders[0], incArgs); pErr != nil {
		t.Fatal(pErr)
	}

	incArgs = incrementArgs(rightKey, 2)
	if _, pErr := client.SendWrapped(context.Background(), mtc.distSenders[0], incArgs); pErr != nil {
		t.Fatal(pErr)
	}
}

func BenchmarkStoreRangeSplit(b *testing.B) {
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

	// Write some values left and right of the split key.
	aDesc := store.LookupReplica([]byte("a"), nil).Desc()
	bDesc := store.LookupReplica([]byte("c"), nil).Desc()
	writeRandomDataToRange(b, store, aDesc.RangeID, []byte("aaa"))
	writeRandomDataToRange(b, store, bDesc.RangeID, []byte("ccc"))

	// Merge the b range back into the a range.
	mArgs := adminMergeArgs(roachpb.KeyMin)
	if _, err := client.SendWrapped(context.Background(), rg1(store), mArgs); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Split the range.
		b.StartTimer()
		if _, err := client.SendWrapped(context.Background(), rg1(store), sArgs); err != nil {
			b.Fatal(err)
		}

		// Merge the ranges.
		b.StopTimer()
		if _, err := client.SendWrapped(context.Background(), rg1(store), mArgs); err != nil {
			b.Fatal(err)
		}
	}
}

func writeRandomDataToRange(
	t testing.TB, store *storage.Store, rangeID roachpb.RangeID, keyPrefix []byte,
) (midpoint []byte) {
	src := rand.New(rand.NewSource(0))
	for i := 0; i < 100; i++ {
		key := append([]byte(nil), keyPrefix...)
		key = append(key, randutil.RandBytes(src, int(src.Int31n(1<<7)))...)
		key = keys.MakeRowSentinelKey(key)
		val := randutil.RandBytes(src, int(src.Int31n(1<<8)))
		pArgs := putArgs(key, val)
		if _, pErr := client.SendWrappedWith(context.Background(), rg1(store), roachpb.Header{
			RangeID: rangeID,
		}, pArgs); pErr != nil {
			t.Fatal(pErr)
		}
	}
	// Return approximate midway point ("Z" in string "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz").
	midKey := append([]byte(nil), keyPrefix...)
	midKey = append(midKey, []byte("Z")...)
	return keys.MakeRowSentinelKey(midKey)
}

func writeRandomTimeSeriesDataToRange(
	t testing.TB, store *storage.Store, rangeID roachpb.RangeID, keyPrefix []byte,
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
					TimestampNanos: src.Int63n(200) * r.SlabDuration(),
					Value:          src.Float64(),
				})
			}
			data = append(data, d)
		}
		for _, d := range data {
			idatas, err := d.ToInternal(r.SlabDuration(), r.SampleDuration())
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
				if _, pErr := client.SendWrappedWith(context.Background(), rg1(store), roachpb.Header{
					RangeID: rangeID,
				}, &mArgs); pErr != nil {
					t.Fatal(pErr)
				}
			}
		}
	}
	// Return approximate midway point (100 is midway between random timestamps in range [0,200)).
	midKey := append([]byte(nil), keyPrefix...)
	midKey = encoding.EncodeVarintAscending(midKey, 100*r.SlabDuration())
	return keys.MakeRowSentinelKey(midKey)
}

// TestStoreSplitBeginTxnPushMetaIntentRace prevents regression of
// #9265. It splits a range and blocks the update to the LHS range
// descriptor (and associated BeginTransaction request). Prior to the
// fix in #9287, holding up the BeginTransaction would allow the
// updates to meta addressing records to proceed. Because the test
// performs an initial split at the SystemPrefix, the dist sender ends
// up issuing a range lookup request after failing to send the entire
// txn to one range. The range lookup encounters the meta2 intents and
// trivially aborts the txn by writing an ABORTED txn record, because
// it doesn't exist. The meta2 intents are deleted. We then run a GC
// which cleans up the aborted txn. When the BeginTransaction
// proceeds, it succeeds and the rest of the txn runs to completion.
//
// This test verifies that the meta records are intact.
func TestStoreSplitBeginTxnPushMetaIntentRace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	splitKey := roachpb.RKey("a")

	var startMu struct {
		syncutil.Mutex
		time time.Time
	}

	wroteMeta2 := make(chan struct{}, 1)

	manual := hlc.NewManualClock(123)
	storeCfg := storage.TestStoreConfig(hlc.NewClock(manual.UnixNano, time.Nanosecond))
	storeCfg.TestingKnobs.DisableSplitQueue = true
	storeCfg.TestingKnobs.TestingEvalFilter = func(filterArgs storagebase.FilterArgs) *roachpb.Error {
		startMu.Lock()
		start := startMu.time
		startMu.Unlock()

		if start.IsZero() {
			return nil
		}

		if _, ok := filterArgs.Req.(*roachpb.BeginTransactionRequest); ok {
			// Return a node unavailable error for BeginTransaction request
			// in the event we haven't waited 20ms.
			if timeutil.Since(start) < 20*time.Millisecond {
				return roachpb.NewError(&roachpb.NodeUnavailableError{})
			}
			if log.V(1) {
				log.Infof(context.TODO(), "allowing BeginTransaction to proceed after 20ms")
			}
		} else if filterArgs.Req.Method() == roachpb.Put &&
			filterArgs.Req.Header().Key.Equal(keys.RangeMetaKey(splitKey)) {
			select {
			case wroteMeta2 <- struct{}{}:
			default:
			}
		}
		return nil
	}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	store := createTestStoreWithConfig(t, stopper, storeCfg)

	// Advance the clock past the transaction cleanup expiration.
	manual.Increment(storage.GetGCQueueTxnCleanupThreshold().Nanoseconds() + 1)

	// First, create a split after addressing records.
	args := adminSplitArgs(roachpb.KeyMin, keys.SystemPrefix)
	if _, pErr := client.SendWrapped(context.Background(), rg1(store), args); pErr != nil {
		t.Fatal(pErr)
	}

	// Begin blocking BeginTransaction calls and signaling meta2 writes.
	startMu.Lock()
	startMu.time = timeutil.Now()
	startMu.Unlock()

	// Initiate split at splitKey in a goroutine.
	doneSplit := make(chan *roachpb.Error, 1)
	go func() {
		if log.V(1) {
			log.Infof(context.TODO(), "splitting at %s", splitKey)
		}
		args := adminSplitArgs(keys.SystemMax, splitKey.AsRawKey())
		_, pErr := client.SendWrappedWith(context.Background(), store, roachpb.Header{
			RangeID: store.LookupReplica(splitKey, nil).RangeID,
		}, args)
		doneSplit <- pErr
	}()

	// Wait for the write to the meta2 key to be initiated.
	<-wroteMeta2

	// Wait for 5ms to allow meta2 write to finish.
	time.Sleep(5 * time.Millisecond)

	// GC the replica containing the range descriptor records.  We use a
	// SucceedsSoon because of the chance the GC is initiated before
	// the range is fully split, meaning the initial GC may fail because
	// it spans ranges.
	testutils.SucceedsSoon(t, func() error {
		return store.ManualGC(store.LookupReplica(splitKey, nil))
	})

	// Wait for the split to complete.
	if pErr := <-doneSplit; pErr != nil {
		t.Fatalf("failed split at %s: %s", splitKey, pErr)
	}

	// Now verify that the meta2/splitKey meta2 record is present; do this
	// within a SucceedSoon in order to give the intents time to resolve.
	testutils.SucceedsSoon(t, func() error {
		val, intents, err := engine.MVCCGet(context.Background(), store.Engine(),
			keys.RangeMetaKey(splitKey), hlc.MaxTimestamp, true, nil)
		if err != nil {
			return err
		}
		if val == nil {
			t.Errorf("expected meta2 record for %s", keys.RangeMetaKey(splitKey))
		}
		if len(intents) > 0 {
			t.Errorf("expected no intents; got %+v", intents)
		}
		return nil
	})
}

// TestStoreRangeGossipOnSplits verifies that the store descriptor
// is gossiped on splits up until the point where an additional
// split range doesn't exceed GossipWhenCapacityDeltaExceedsFraction.
func TestStoreRangeGossipOnSplits(t *testing.T) {
	defer leaktest.AfterTest(t)()
	storeCfg := storage.TestStoreConfig(nil)
	storeCfg.GossipWhenCapacityDeltaExceedsFraction = 0.5 // 50% for testing
	storeCfg.TestingKnobs.DisableSplitQueue = true
	storeCfg.TestingKnobs.DisableScanner = true
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	store := createTestStoreWithConfig(t, stopper, storeCfg)
	storeKey := gossip.MakeStoreKey(store.StoreID())

	// Avoid excessive logging on under-replicated ranges due to our many splits.
	config.TestingSetupZoneConfigHook(stopper)
	config.TestingSetZoneConfig(0, config.ZoneConfig{NumReplicas: 1})

	var lastSD roachpb.StoreDescriptor
	rangeCountCh := make(chan int32)
	unregister := store.Gossip().RegisterCallback(storeKey, func(_ string, val roachpb.Value) {
		var sd roachpb.StoreDescriptor
		if err := val.GetProto(&sd); err != nil {
			panic(err)
		}
		// Wait for range count to change as this callback is invoked
		// for lease count changes as well.
		if sd.Capacity.RangeCount == lastSD.Capacity.RangeCount {
			return
		}
		lastSD = sd
		rangeCountCh <- sd.Capacity.RangeCount
	})
	defer unregister()

	// Pull the first gossiped range count.
	lastRangeCount := <-rangeCountCh

	splitFunc := func(i int) *roachpb.Error {
		splitKey := roachpb.Key(fmt.Sprintf("%02d", i))
		_, pErr := store.LookupReplica(roachpb.RKey(splitKey), nil).AdminSplit(
			context.TODO(),
			roachpb.AdminSplitRequest{
				SplitKey: splitKey,
			},
		)
		return pErr
	}

	// Split until we split at least 20 ranges.
	var rangeCount int32
	for i := 0; rangeCount < 20; i++ {
		if pErr := splitFunc(i); pErr != nil {
			t.Fatal(pErr)
		}
		select {
		case rangeCount = <-rangeCountCh:
			changeCount := int32(math.Ceil(math.Max(float64(lastRangeCount)*0.5, 1)))
			diff := rangeCount - (lastRangeCount + changeCount)
			if diff < -1 || diff > 1 {
				t.Errorf("gossiped range count %d more than 1 away from expected %d", rangeCount, lastRangeCount+changeCount)
			}
			lastRangeCount = rangeCount
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// TestStorePushTxnQueueEnabledOnSplit verifies that the PushTxnQueue for
// the right hand side of the split range is enabled after a split.
func TestStorePushTxnQueueEnabledOnSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	storeCfg := storage.TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableSplitQueue = true
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	store := createTestStoreWithConfig(t, stopper, storeCfg)

	key := keys.MakeRowSentinelKey(append([]byte(nil), keys.UserTableDataMin...))
	args := adminSplitArgs(key, key)
	if _, pErr := client.SendWrapped(context.Background(), rg1(store), args); pErr != nil {
		t.Fatalf("%q: split unexpected error: %s", key, pErr)
	}

	rhsRepl := store.LookupReplica(roachpb.RKey(keys.UserTableDataMin), nil)
	if !rhsRepl.IsPushTxnQueueEnabled() {
		t.Errorf("expected RHS replica's push txn queue to be enabled post-split")
	}
}

// TestDistributedTxnCleanup verifies that distributed transactions
// cleanup their txn records after commit or abort.
func TestDistributedTxnCleanup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	storeCfg := storage.TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableSplitQueue = true
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	store := createTestStoreWithConfig(t, stopper, storeCfg)

	// Split at "a".
	lhsKey := roachpb.Key("a")
	args := adminSplitArgs(lhsKey, lhsKey)
	if _, pErr := client.SendWrapped(context.Background(), rg1(store), args); pErr != nil {
		t.Fatalf("split at %q: %s", lhsKey, pErr)
	}
	lhs := store.LookupReplica(roachpb.RKey("a"), nil)

	// Split at "b".
	rhsKey := roachpb.Key("b")
	args = adminSplitArgs(rhsKey, rhsKey)
	if _, pErr := client.SendWrappedWith(context.Background(), store, roachpb.Header{
		RangeID: lhs.RangeID,
	}, args); pErr != nil {
		t.Fatalf("split at %q: %s", rhsKey, pErr)
	}
	rhs := store.LookupReplica(roachpb.RKey("b"), nil)

	if lhs == rhs {
		t.Errorf("LHS == RHS after split: %s == %s", lhs, rhs)
	}

	// Test both commit and abort cases.
	for _, force := range []bool{true, false} {
		for _, commit := range []bool{true, false} {
			t.Run(fmt.Sprintf("force=%t,commit=%t", force, commit), func(t *testing.T) {
				// Run a distributed transaction involving the lhsKey and rhsKey.
				var txnKey roachpb.Key
				ctx := context.Background()
				txn := client.NewTxn(store.DB())
				opts := client.TxnExecOptions{
					AutoCommit: true,
					AutoRetry:  false,
				}
				if err := txn.Exec(ctx, opts, func(ctx context.Context, txn *client.Txn, _ *client.TxnExecOptions) error {
					b := txn.NewBatch()
					b.Put(fmt.Sprintf("%s.force=%t,commit=%t", string(lhsKey), force, commit), "lhsValue")
					b.Put(fmt.Sprintf("%s.force=%t,commit=%t", string(rhsKey), force, commit), "rhsValue")
					if err := txn.Run(ctx, b); err != nil {
						return err
					}
					txnKey = keys.TransactionKey(txn.Proto().Key, *txn.Proto().ID)
					// If force=true, we're force-aborting the txn out from underneath.
					// This simulates txn deadlock or a max priority txn aborting a
					// normal or min priority txn.
					if force {
						ba := roachpb.BatchRequest{}
						ba.RangeID = lhs.RangeID
						ba.Add(&roachpb.PushTxnRequest{
							Span: roachpb.Span{
								Key: txn.Proto().Key,
							},
							Now:           store.Clock().Now(),
							PusheeTxn:     txn.Proto().TxnMeta,
							PushType:      roachpb.PUSH_ABORT,
							Force:         true,
							NewPriorities: true,
						})
						_, pErr := store.Send(ctx, ba)
						if pErr != nil {
							t.Errorf("failed to abort the txn: %s", pErr)
						}
					}
					if commit {
						return nil
					}
					return errors.New("forced abort")
				}); err != nil {
					txn.CleanupOnError(ctx, err)
					if !force && commit {
						t.Fatalf("expected success with commit == true; got %v", err)
					}
				}

				// Verify that the transaction record is cleaned up.
				testutils.SucceedsSoon(t, func() error {
					kv, err := store.DB().Get(ctx, txnKey)
					if err != nil {
						return err
					}
					if kv.Value != nil {
						return errors.Errorf("expected txn record %s to have been cleaned", txnKey)
					}
					return nil
				})
			})
		}
	}
}

func TestUnsplittableRange(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	store := createTestStoreWithConfig(t, stopper, storage.TestStoreConfig(nil))
	store.ForceSplitScanAndProcess()

	// Add a single large row to /Table/14.
	tableKey := keys.MakeTablePrefix(keys.UITableID)
	row1Key := roachpb.Key(encoding.EncodeVarintAscending(append([]byte(nil), tableKey...), 1))
	col1Key := keys.MakeFamilyKey(append([]byte(nil), row1Key...), 0)
	value := bytes.Repeat([]byte("x"), 64<<10)
	if err := store.DB().Put(context.Background(), col1Key, value); err != nil {
		t.Fatal(err)
	}

	store.ForceSplitScanAndProcess()
	testutils.SucceedsSoon(t, func() error {
		repl := store.LookupReplica(tableKey, nil)
		if repl.Desc().StartKey.Equal(tableKey) {
			return nil
		}
		return errors.Errorf("waiting for split: %s", repl)
	})

	repl := store.LookupReplica(tableKey, nil)
	origMaxBytes := repl.GetMaxBytes()
	repl.SetMaxBytes(int64(len(value)))

	// Wait for an attempt to split the range which will fail because it contains
	// a single large value. The max-bytes for the range will be changed, but it
	// should not have been reset to its original value.
	store.ForceSplitScanAndProcess()
	testutils.SucceedsSoon(t, func() error {
		maxBytes := repl.GetMaxBytes()
		if maxBytes != int64(len(value)) && maxBytes < origMaxBytes {
			return nil
		}
		return errors.Errorf("expected max-bytes to be changed: %d", repl.GetMaxBytes())
	})

	// Add two more rows to the range.
	for i := int64(2); i < 4; i++ {
		rowKey := roachpb.Key(encoding.EncodeVarintAscending(append([]byte(nil), tableKey...), i))
		colKey := keys.MakeFamilyKey(append([]byte(nil), rowKey...), 0)
		if err := store.DB().Put(context.Background(), colKey, value); err != nil {
			t.Fatal(err)
		}
	}

	// Wait for the range to be split and verify that max-bytes was reset to the
	// value in the zone config.
	store.ForceSplitScanAndProcess()
	testutils.SucceedsSoon(t, func() error {
		if origMaxBytes == repl.GetMaxBytes() {
			return nil
		}
		return errors.Errorf("expected max-bytes=%d, but got max-bytes=%d",
			origMaxBytes, repl.GetMaxBytes())
	})
}

// TestPushTxnQueueDependencyCycleWithRangeSplit verifies that a range
// split which occurs while a dependency cycle is partially underway
// will cause the pending push txns to be retried such that they
// relocate to the appropriate new range.
func TestPushTxnQueueDependencyCycleWithRangeSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, read2ndPass := range []bool{false, true} {
		t.Run(fmt.Sprintf("read-2nd-pass:%t", read2ndPass), func(t *testing.T) {
			var pushCount int32
			firstPush := make(chan struct{})

			storeCfg := storage.TestStoreConfig(nil)
			storeCfg.TestingKnobs.DisableSplitQueue = true
			storeCfg.TestingKnobs.TestingEvalFilter =
				func(filterArgs storagebase.FilterArgs) *roachpb.Error {
					if _, ok := filterArgs.Req.(*roachpb.PushTxnRequest); ok {
						if atomic.AddInt32(&pushCount, 1) == 1 {
							close(firstPush)
						}
					}
					return nil
				}
			stopper := stop.NewStopper()
			defer stopper.Stop(context.TODO())
			store := createTestStoreWithConfig(t, stopper, storeCfg)

			lhsKey := roachpb.Key("a")
			rhsKey := roachpb.Key("b")

			// Split at "a".
			args := adminSplitArgs(lhsKey, lhsKey)
			if _, pErr := client.SendWrapped(context.Background(), rg1(store), args); pErr != nil {
				t.Fatalf("split at %q: %s", lhsKey, pErr)
			}
			lhs := store.LookupReplica(roachpb.RKey("a"), nil)

			var txnACount, txnBCount int32

			txnAWritesA := make(chan struct{})
			txnAProceeds := make(chan struct{})
			txnBWritesB := make(chan struct{})
			txnBProceeds := make(chan struct{})

			// Start txn to write key a.
			txnACh := make(chan error)
			go func() {
				txnACh <- store.DB().Txn(context.Background(), func(ctx context.Context, txn *client.Txn) error {
					if err := txn.Put(ctx, lhsKey, "value"); err != nil {
						return err
					}
					if atomic.LoadInt32(&txnACount) == 0 {
						close(txnAWritesA)
						<-txnAProceeds
					}
					atomic.AddInt32(&txnACount, 1)
					return txn.Put(ctx, rhsKey, "value-from-A")
				})
			}()
			<-txnAWritesA

			// Start txn to write key b.
			txnBCh := make(chan error)
			go func() {
				txnBCh <- store.DB().Txn(context.Background(), func(ctx context.Context, txn *client.Txn) error {
					if err := txn.Put(ctx, rhsKey, "value"); err != nil {
						return err
					}
					if atomic.LoadInt32(&txnBCount) == 0 {
						close(txnBWritesB)
						<-txnBProceeds
					}
					atomic.AddInt32(&txnBCount, 1)
					// Read instead of write key "a" if directed. This caused a
					// PUSH_TIMESTAMP to be issued from txn B instead of PUSH_ABORT.
					if read2ndPass {
						if _, err := txn.Get(ctx, lhsKey); err != nil {
							return err
						}
					} else {
						if err := txn.Put(ctx, lhsKey, "value-from-B"); err != nil {
							return err
						}
					}
					return nil
				})
			}()
			<-txnBWritesB

			// Now, let txnA proceed before splitting.
			close(txnAProceeds)
			// Wait for the push to occur.
			<-firstPush

			// Split at "b".
			args = adminSplitArgs(rhsKey, rhsKey)
			if _, pErr := client.SendWrappedWith(context.Background(), store, roachpb.Header{
				RangeID: lhs.RangeID,
			}, args); pErr != nil {
				t.Fatalf("split at %q: %s", rhsKey, pErr)
			}

			// Now that we've split, allow txnB to proceed.
			close(txnBProceeds)

			// Verify that both complete.
			for i, ch := range []chan error{txnACh, txnBCh} {
				if err := <-ch; err != nil {
					t.Fatalf("%d: txn failure: %v", i, err)
				}
			}
		})
	}
}
