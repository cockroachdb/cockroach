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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/randutil"
)

func adminSplitArgs(key, splitKey roachpb.Key) roachpb.AdminSplitRequest {
	return roachpb.AdminSplitRequest{
		Span: roachpb.Span{
			Key: key,
		},
		SplitKey: splitKey,
	}
}

func verifyRangeStats(eng engine.Engine, rangeID roachpb.RangeID, expMS engine.MVCCStats) error {
	var ms engine.MVCCStats
	if err := engine.MVCCGetRangeStats(eng, rangeID, &ms); err != nil {
		return err
	}
	// Clear system counts as these are expected to vary.
	ms.SysBytes, ms.SysCount = 0, 0
	if !reflect.DeepEqual(expMS, ms) {
		return util.Errorf("expected stats %+v; got %+v", expMS, ms)
	}
	return nil
}

// TestStoreRangeSplitAtIllegalKeys verifies a range cannot be split
// at illegal keys.
func TestStoreRangeSplitAtIllegalKeys(t *testing.T) {
	defer leaktest.AfterTest(t)
	store, stopper := createTestStore(t)
	defer stopper.Stop()

	for _, key := range []roachpb.Key{
		keys.Meta1Prefix,
		keys.MakeKey(keys.Meta1Prefix, []byte("a")),
		keys.MakeKey(keys.Meta1Prefix, roachpb.RKeyMax),
		keys.Meta2KeyMax,
		keys.MakeTablePrefix(10 /* system descriptor ID */),
	} {
		args := adminSplitArgs(roachpb.KeyMin, key)
		_, err := client.SendWrapped(rg1(store), nil, &args)
		if err == nil {
			t.Fatalf("%q: split succeeded unexpectedly", key)
		}
	}
}

// TestStoreRangeSplitAtTablePrefix verifies a range can be split
// at TableDataPrefix and still gossip the SystemConfig properly.
func TestStoreRangeSplitAtTablePrefix(t *testing.T) {
	defer leaktest.AfterTest(t)
	store, stopper := createTestStore(t)
	defer stopper.Stop()

	key := keys.TableDataPrefix
	args := adminSplitArgs(key, key)
	_, err := client.SendWrapped(rg1(store), nil, &args)
	if err != nil {
		t.Fatalf("%q: split unexpected error: %s", key, err)
	}

	desc := &sql.TableDescriptor{}
	descBytes, err := desc.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	// Update SystemConfig to trigger gossip.
	if err := store.DB().Txn(func(txn *client.Txn) error {
		txn.SetSystemDBTrigger()
		// We don't care about the values, just the keys.
		k := sql.MakeDescMetadataKey(sql.ID(keys.MaxReservedDescID + 1))
		return txn.Put(k, desc)
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

// TestStoreRangeSplitAtRangeBounds verifies a range cannot be split
// at its start or end keys (would create zero-length range!). This
// sort of thing might happen in the wild if two split requests
// arrived for same key. The first one succeeds and second would try
// to split at the start of the newly split range.
func TestStoreRangeSplitAtRangeBounds(t *testing.T) {
	defer leaktest.AfterTest(t)
	store, stopper := createTestStore(t)
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
	defer leaktest.AfterTest(t)
	store, stopper := createTestStore(t)
	defer stopper.Stop()

	splitKey := roachpb.Key("a")
	concurrentCount := int32(10)
	wg := sync.WaitGroup{}
	wg.Add(int(concurrentCount))
	failureCount := int32(0)
	for i := int32(0); i < concurrentCount; i++ {
		go func() {
			args := adminSplitArgs(roachpb.KeyMin, splitKey)
			_, err := client.SendWrapped(rg1(store), nil, &args)
			if err != nil {
				atomic.AddInt32(&failureCount, 1)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	if failureCount != concurrentCount-1 {
		t.Fatalf("concurrent splits succeeded unexpectedly; failureCount=%d", failureCount)
	}

	// Verify everything ended up as expected.
	if a, e := store.ReplicaCount(), 2; a != e {
		t.Fatalf("expected %d stores after concurrent splits; actual count=%d", e, a)
	}
	rng := store.LookupReplica(roachpb.RKeyMin, nil)
	newRng := store.LookupReplica(roachpb.RKey(splitKey), nil)
	if !bytes.Equal(newRng.Desc().StartKey, splitKey) || !bytes.Equal(splitKey, rng.Desc().EndKey) {
		t.Errorf("ranges mismatched, wanted %q=%q=%q", newRng.Desc().StartKey, splitKey, rng.Desc().EndKey)
	}
	if !bytes.Equal(newRng.Desc().EndKey, roachpb.RKeyMax) || !bytes.Equal(rng.Desc().StartKey, roachpb.RKeyMin) {
		t.Errorf("new ranges do not cover KeyMin-KeyMax, but only %q-%q", rng.Desc().StartKey, newRng.Desc().EndKey)
	}
}

// TestStoreRangeSplit executes a split of a range and verifies that the
// resulting ranges respond to the right key ranges and that their stats
// and sequence cache have been properly accounted for.
func TestStoreRangeSplitIdempotency(t *testing.T) {
	defer leaktest.AfterTest(t)
	store, stopper := createTestStore(t)
	defer stopper.Stop()
	rangeID := roachpb.RangeID(1)
	splitKey := roachpb.Key("m")
	content := roachpb.Key("asdvb")

	// First, write some values left and right of the proposed split key.
	pArgs := putArgs([]byte("c"), content)
	if _, err := client.SendWrapped(rg1(store), nil, &pArgs); err != nil {
		t.Fatal(err)
	}
	pArgs = putArgs([]byte("x"), content)
	if _, err := client.SendWrapped(rg1(store), nil, &pArgs); err != nil {
		t.Fatal(err)
	}

	// Increments are a good way of testing the sequence cache. Up here, we
	// address them to the original range, then later to the one that contains
	// the key.
	txn := roachpb.NewTransaction("test", []byte("c"), 10, roachpb.SERIALIZABLE,
		store.Clock().Now(), 0)
	lIncArgs := incrementArgs([]byte("apoptosis"), 100)
	if _, err := client.SendWrappedWith(rg1(store), nil, roachpb.Header{
		Txn: txn,
	}, &lIncArgs); err != nil {
		t.Fatal(err)
	}
	rIncArgs := incrementArgs([]byte("wobble"), 10)
	txn.Sequence++
	if _, err := client.SendWrappedWith(rg1(store), nil, roachpb.Header{
		Txn: txn,
	}, &rIncArgs); err != nil {
		t.Fatal(err)
	}

	// Get the original stats for key and value bytes.
	var ms engine.MVCCStats
	if err := engine.MVCCGetRangeStats(store.Engine(), rangeID, &ms); err != nil {
		t.Fatal(err)
	}
	keyBytes, valBytes := ms.KeyBytes, ms.ValBytes

	// Split the range.
	args := adminSplitArgs(roachpb.KeyMin, splitKey)
	if _, err := client.SendWrapped(rg1(store), nil, &args); err != nil {
		t.Fatal(err)
	}

	// Verify no intents remains on range descriptor keys.
	for _, key := range []roachpb.Key{keys.RangeDescriptorKey(roachpb.RKeyMin), keys.RangeDescriptorKey(keys.Addr(splitKey))} {
		if _, _, err := engine.MVCCGet(store.Engine(), key, store.Clock().Now(), true, nil); err != nil {
			t.Fatal(err)
		}
	}

	rng := store.LookupReplica(roachpb.RKeyMin, nil)
	newRng := store.LookupReplica([]byte("m"), nil)
	if !bytes.Equal(newRng.Desc().StartKey, splitKey) || !bytes.Equal(splitKey, rng.Desc().EndKey) {
		t.Errorf("ranges mismatched, wanted %q=%q=%q", newRng.Desc().StartKey, splitKey, rng.Desc().EndKey)
	}
	if !bytes.Equal(newRng.Desc().EndKey, roachpb.RKeyMax) || !bytes.Equal(rng.Desc().StartKey, roachpb.RKeyMin) {
		t.Errorf("new ranges do not cover KeyMin-KeyMax, but only %q-%q", rng.Desc().StartKey, newRng.Desc().EndKey)
	}

	// Try to get values from both left and right of where the split happened.
	gArgs := getArgs([]byte("c"))
	if reply, err := client.SendWrapped(rg1(store), nil, &gArgs); err != nil {
		t.Fatal(err)
	} else if replyBytes, err := reply.(*roachpb.GetResponse).Value.GetBytes(); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(replyBytes, content) {
		t.Fatalf("actual value %q did not match expected value %q", replyBytes, content)
	}
	gArgs = getArgs([]byte("x"))
	if reply, err := client.SendWrappedWith(rg1(store), nil, roachpb.Header{
		RangeID: newRng.Desc().RangeID,
	}, &gArgs); err != nil {
		t.Fatal(err)
	} else if replyBytes, err := reply.(*roachpb.GetResponse).Value.GetBytes(); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(replyBytes, content) {
		t.Fatalf("actual value %q did not match expected value %q", replyBytes, content)
	}

	// Send out an increment request copied from above (same txn/sequence)
	// which remains in the old range.
	_, err := client.SendWrappedWith(rg1(store), nil, roachpb.Header{
		Txn: txn,
	}, &lIncArgs)
	if _, ok := err.(*roachpb.TransactionRetryError); !ok {
		t.Fatalf("unexpected sequence cache miss: %v", err)
	}

	// Send out the same increment copied from above (same txn/sequence), but
	// now to the newly created range (which should hold that key).
	_, err = client.SendWrappedWith(rg1(store), nil, roachpb.Header{
		RangeID: newRng.Desc().RangeID,
		Txn:     txn,
	}, &rIncArgs)
	if _, ok := err.(*roachpb.TransactionRetryError); !ok {
		t.Fatalf("unexpected sequence cache miss: %v", err)
	}

	// Compare stats of split ranges to ensure they are non zero and
	// exceed the original range when summed.
	var left, right engine.MVCCStats
	if err := engine.MVCCGetRangeStats(store.Engine(), rangeID, &left); err != nil {
		t.Fatal(err)
	}
	lKeyBytes, lValBytes := left.KeyBytes, left.ValBytes
	if err := engine.MVCCGetRangeStats(store.Engine(), newRng.Desc().RangeID, &right); err != nil {
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
	defer leaktest.AfterTest(t)
	store, stopper := createTestStore(t)
	defer stopper.Stop()

	// Split the range after the last table data key.
	keyPrefix := keys.MakeTablePrefix(keys.MaxReservedDescID + 1)
	args := adminSplitArgs(roachpb.KeyMin, keyPrefix)
	if _, err := client.SendWrapped(rg1(store), nil, &args); err != nil {
		t.Fatal(err)
	}
	// Verify empty range has empty stats.
	rng := store.LookupReplica(keyPrefix, nil)
	// NOTE that this value is expected to change over time, depending on what
	// we store in the sys-local keyspace. Update it accordingly for this test.
	if err := verifyRangeStats(store.Engine(), rng.Desc().RangeID, engine.MVCCStats{}); err != nil {
		t.Fatal(err)
	}

	// Write random data.
	src := rand.New(rand.NewSource(0))
	for i := 0; i < 100; i++ {
		key := append([]byte(nil), keyPrefix...)
		key = append(key, randutil.RandBytes(src, int(src.Int31n(1<<7)))...)
		val := randutil.RandBytes(src, int(src.Int31n(1<<8)))
		pArgs := putArgs(key, val)
		if _, err := client.SendWrappedWith(rg1(store), nil, roachpb.Header{
			RangeID: rng.Desc().RangeID,
		}, &pArgs); err != nil {
			t.Fatal(err)
		}
	}
	// Get the range stats now that we have data.
	var ms engine.MVCCStats
	if err := engine.MVCCGetRangeStats(store.Engine(), rng.Desc().RangeID, &ms); err != nil {
		t.Fatal(err)
	}

	// Split the range at approximate halfway point ("Z" in string "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz").
	midKey := append([]byte(nil), keyPrefix...)
	midKey = append(midKey, []byte("Z")...)
	args = adminSplitArgs(keyPrefix, midKey)
	if _, err := client.SendWrappedWith(rg1(store), nil, roachpb.Header{
		RangeID: rng.Desc().RangeID,
	}, &args); err != nil {
		t.Fatal(err)
	}

	var msLeft, msRight engine.MVCCStats
	if err := engine.MVCCGetRangeStats(store.Engine(), rng.Desc().RangeID, &msLeft); err != nil {
		t.Fatal(err)
	}
	rngRight := store.LookupReplica(midKey, nil)
	if err := engine.MVCCGetRangeStats(store.Engine(), rngRight.Desc().RangeID, &msRight); err != nil {
		t.Fatal(err)
	}

	// The stats should be exactly equal when added.
	expMS := engine.MVCCStats{
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
	if !reflect.DeepEqual(expMS, ms) {
		t.Errorf("expected left and right ranges to equal original: %+v + %+v != %+v", msLeft, msRight, ms)
	}
}

// fillRange writes keys with the given prefix and associated values
// until bytes bytes have been written.
func fillRange(store *storage.Store, rangeID roachpb.RangeID, prefix roachpb.Key, bytes int64, t *testing.T) {
	src := rand.New(rand.NewSource(0))
	for {
		var ms engine.MVCCStats
		if err := engine.MVCCGetRangeStats(store.Engine(), rangeID, &ms); err != nil {
			t.Fatal(err)
		}
		keyBytes, valBytes := ms.KeyBytes, ms.ValBytes
		if keyBytes+valBytes >= bytes {
			return
		}
		key := append(append([]byte(nil), prefix...), randutil.RandBytes(src, 100)...)
		val := randutil.RandBytes(src, int(src.Int31n(1<<8)))
		pArgs := putArgs(key, val)
		if _, err := client.SendWrappedWith(rg1(store), nil, roachpb.Header{
			RangeID: rangeID,
		}, &pArgs); err != nil {
			t.Fatal(err)
		}
	}
}

// TestStoreZoneUpdateAndRangeSplit verifies that modifying the zone
// configuration changes range max bytes and Range.maybeSplit() takes
// max bytes into account when deciding whether to enqueue a range for
// splitting. It further verifies that the range is in fact split on
// exceeding zone's RangeMaxBytes.
func TestStoreZoneUpdateAndRangeSplit(t *testing.T) {
	defer leaktest.AfterTest(t)
	store, stopper := createTestStore(t)
	config.TestingSetupZoneConfigHook(stopper)
	defer stopper.Stop()

	maxBytes := int64(1 << 16)
	// Set max bytes.
	config.TestingSetZoneConfig(1000, &config.ZoneConfig{RangeMaxBytes: maxBytes})

	// Trigger gossip callback.
	if err := store.Gossip().AddInfoProto(gossip.KeySystemConfig, &config.SystemConfig{}, 0); err != nil {
		t.Fatal(err)
	}

	// Wait for the range to be split along table boundaries.
	originalRange := store.LookupReplica(roachpb.RKeyMin, nil)
	var rng *storage.Replica
	if err := util.IsTrueWithin(func() bool {
		rng = store.LookupReplica(keys.MakeTablePrefix(1000), nil)
		return rng.Desc().RangeID != originalRange.Desc().RangeID
	}, 50*time.Millisecond); err != nil {
		t.Fatalf("failed to notice range max bytes update: %s", err)
	}

	// Check range's max bytes settings.
	if rng.GetMaxBytes() != maxBytes {
		t.Fatalf("range max bytes mismatch, got: %d, expected: %d", rng.GetMaxBytes(), maxBytes)
	}

	// Make sure the second range goes to the end.
	if !roachpb.RKeyMax.Equal(rng.Desc().EndKey) {
		t.Fatalf("second range has split: %+v", rng.Desc())
	}

	// Look in the range after prefix we're writing to.
	fillRange(store, rng.Desc().RangeID, keys.MakeTablePrefix(1000), maxBytes, t)

	// Verify that the range is in fact split (give it a second for very slow test machines).
	var newRng *storage.Replica
	if err := util.IsTrueWithin(func() bool {
		newRng = store.LookupReplica(keys.MakeTablePrefix(2000), nil)
		return newRng.Desc().RangeID != rng.Desc().RangeID
	}, time.Second); err != nil {
		t.Errorf("expected range to split within 1s")
	}

	// Make sure the new range goes to the end.
	if !roachpb.RKeyMax.Equal(newRng.Desc().EndKey) {
		t.Fatalf("second range has split: %+v", rng.Desc())
	}
}

// TestStoreRangeSplitWithMaxBytesUpdate tests a scenario where a new
// zone config that updates the max bytes is set and triggers a range
// split.
func TestStoreRangeSplitWithMaxBytesUpdate(t *testing.T) {
	defer leaktest.AfterTest(t)
	store, stopper := createTestStore(t)
	config.TestingSetupZoneConfigHook(stopper)
	defer stopper.Stop()

	origRng := store.LookupReplica(roachpb.RKeyMin, nil)

	// Set max bytes.
	maxBytes := int64(1 << 16)
	config.TestingSetZoneConfig(1000, &config.ZoneConfig{RangeMaxBytes: maxBytes})

	// Trigger gossip callback.
	if err := store.Gossip().AddInfoProto(gossip.KeySystemConfig, &config.SystemConfig{}, 0); err != nil {
		t.Fatal(err)
	}

	// Verify that the range is split and the new range has the correct max bytes.
	util.SucceedsWithin(t, time.Second, func() error {
		newRng := store.LookupReplica(keys.MakeTablePrefix(1000), nil)
		if newRng.Desc().RangeID == origRng.Desc().RangeID {
			return util.Errorf("expected new range created by split")
		}
		if newRng.GetMaxBytes() != maxBytes {
			return util.Errorf("expected %d max bytes for the new range, but got %d",
				maxBytes, newRng.GetMaxBytes())
		}
		return nil
	})
}

// TestStoreRangeSystemSplits verifies that splits are based on the
// contents of the SystemDB span.
func TestStoreRangeSystemSplits(t *testing.T) {
	defer leaktest.AfterTest(t)
	store, stopper := createTestStore(t)
	defer stopper.Stop()

	initialSystemValues := sql.GetInitialSystemValues()
	numInitialValues := len(initialSystemValues)
	// Write the initial sql values to the system DB as well
	// as the equivalent of table descriptors for X user tables.
	// This does two things:
	// - descriptor IDs are used to determine split keys
	// - the write triggers a SystemConfig update and gossip.
	// We should end up with splits at each user table prefix.
	if err := store.DB().Txn(func(txn *client.Txn) error {
		txn.SetSystemDBTrigger()
		for i, kv := range initialSystemValues {
			bytes, err := kv.Value.GetBytes()
			if err != nil {
				log.Info(err)
				continue
			}
			if err := txn.Put(kv.Key, bytes); err != nil {
				return err
			}

			descID := keys.MaxReservedDescID + i + 1

			// We don't care about the values, just the keys.
			k := sql.MakeDescMetadataKey(sql.ID(descID))
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
		expKeys := make([]roachpb.Key, 0, maxTableID+1)
		for i := 1; i <= maxTableID; i++ {
			expKeys = append(expKeys,
				keys.MakeKey(keys.Meta2Prefix, keys.MakeTablePrefix(keys.MaxReservedDescID+uint32(i))),
			)
		}
		expKeys = append(expKeys, keys.MakeKey(keys.Meta2Prefix, roachpb.RKeyMax))

		util.SucceedsWithinDepth(1, t, 5*time.Second, func() error {
			rows, err := store.DB().Scan(keys.Meta2Prefix, keys.MetaMax, 0)
			if err != nil {
				return err
			}
			keys := make([]roachpb.Key, 0, len(expKeys))
			for _, r := range rows {
				keys = append(keys, r.Key)
			}
			if !reflect.DeepEqual(keys, expKeys) {
				return util.Errorf("expected split keys:\n%v\nbut found:\n%v", expKeys, keys)
			}
			return nil
		})
	}

	verifySplitsAtTablePrefixes(len(initialSystemValues))

	numTotalValues := numInitialValues + 5

	// Write another, disjoint descriptor for a user table.
	if err := store.DB().Txn(func(txn *client.Txn) error {
		txn.SetSystemDBTrigger()
		// This time, only write the last table descriptor. Splits
		// still occur for every intervening ID.
		// We don't care about the values, just the keys.
		k := sql.MakeDescMetadataKey(sql.ID(keys.MaxReservedDescID + numTotalValues))
		return txn.Put(k, &sql.TableDescriptor{})
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
// The range containing leftKey is the left side of the split, located on nodes 1, 2, and 3.
// The range containing rightKey is the right side of the split, located on nodes 3, 4, and 5.
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
	if _, err := client.SendWrapped(rg1(mtc.stores[0]), nil, &incArgs); err != nil {
		t.Fatal(err)
	}
	incArgs = incrementArgs(rightKey, 2)
	if _, err := client.SendWrapped(rg1(mtc.stores[0]), nil, &incArgs); err != nil {
		t.Fatal(err)
	}

	// Split the system range from the rest of the keyspace.
	splitArgs := adminSplitArgs(roachpb.KeyMin, keys.SystemMax)
	if _, err := client.SendWrapped(rg1(mtc.stores[0]), nil, &splitArgs); err != nil {
		t.Fatal(err)
	}

	// Get the left range's ID. This is currently 2, but using
	// LookupReplica is more future-proof (and see below for
	// rightRangeID).
	leftRangeID := mtc.stores[0].LookupReplica(roachpb.RKey("a"), nil).Desc().RangeID

	// Replicate the left range onto nodes 1-3 and remove it from node 0.
	mtc.replicateRange(leftRangeID, 0, 1, 2, 3)
	mtc.unreplicateRange(leftRangeID, 0, 0)
	mtc.expireLeaderLeases()

	mtc.waitForValues(leftKey, 3*time.Second, []int64{0, 1, 1, 1, 0, 0})
	mtc.waitForValues(rightKey, 3*time.Second, []int64{0, 2, 2, 2, 0, 0})

	// Stop node 3 so it doesn't hear about the split.
	mtc.stopStore(3)
	mtc.expireLeaderLeases()

	// Split the data range.
	splitArgs = adminSplitArgs(keys.SystemMax, roachpb.Key("m"))
	if _, err := client.SendWrapped(mtc.distSender, nil, &splitArgs); err != nil {
		t.Fatal(err)
	}

	// Get the right range's ID. Since the split was performed on node
	// 1, it is currently 11 and not 3 as might be expected.
	rightRangeID := mtc.stores[1].LookupReplica(roachpb.RKey("z"), nil).Desc().RangeID

	// Relocate the right range onto nodes 3-5.
	mtc.replicateRange(rightRangeID, 1, 4, 5)
	mtc.unreplicateRange(rightRangeID, 1, 2)
	mtc.unreplicateRange(rightRangeID, 1, 1)

	mtc.waitForValues(rightKey, 3*time.Second, []int64{0, 0, 0, 2, 2, 2})

	// Stop the remaining data stores.
	mtc.stopStore(1)
	mtc.stopStore(2)
	// 3 is already stopped.
	mtc.stopStore(4)
	mtc.stopStore(5)
	mtc.expireLeaderLeases()

	return mtc, leftKey, rightKey
}

// TestSplitSnapshotRace_SplitWins exercises one outcome of the
// split/snapshot race: The left side of the split propagates first,
// so the split completes before it sees a competing snapshot. This is
// the more common outcome in practice.
func TestSplitSnapshotRace_SplitWins(t *testing.T) {
	defer leaktest.AfterTest(t)
	mtc, leftKey, rightKey := setupSplitSnapshotRace(t)
	defer mtc.Stop()

	// Bring the left range up first so that the split happens before it sees a snapshot.
	for i := 1; i <= 3; i++ {
		mtc.restartStore(i)
	}

	// Perform a write on the left range and wait for it to propagate.
	incArgs := incrementArgs(leftKey, 10)
	if _, err := client.SendWrapped(mtc.distSender, nil, &incArgs); err != nil {
		t.Fatal(err)
	}
	mtc.waitForValues(leftKey, 3*time.Second, []int64{0, 11, 11, 11, 0, 0})

	// Now wake the other stores up.
	mtc.restartStore(4)
	mtc.restartStore(5)

	// Write to the right range.
	incArgs = incrementArgs(rightKey, 20)
	if _, err := client.SendWrapped(mtc.distSender, nil, &incArgs); err != nil {
		t.Fatal(err)
	}
	mtc.waitForValues(rightKey, 3*time.Second, []int64{0, 0, 0, 22, 22, 22})
}

// TestSplitSnapshotRace_SplitWins exercises one outcome of the
// split/snapshot race: The right side of the split replicates first,
// so target node sees a raft snapshot before it has processed the split,
// so it still has a conflicting range.
func TestSplitSnapshotRace_SnapshotWins(t *testing.T) {
	defer leaktest.AfterTest(t)
	t.Skip("TODO(bdarnell): https://github.com/cockroachdb/cockroach/issues/3121")
	mtc, leftKey, rightKey := setupSplitSnapshotRace(t)
	defer mtc.Stop()

	// Bring the right range up first.
	for i := 3; i <= 5; i++ {
		mtc.restartStore(i)
	}

	// Perform a write on the right range.
	incArgs := incrementArgs(rightKey, 20)
	if _, err := client.SendWrapped(mtc.distSender, nil, &incArgs); err != nil {
		t.Fatal(err)
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
	mtc.waitForValues(rightKey, 3*time.Second, []int64{0, 0, 0, 2, 22, 22})

	// Wake up the left-hand range. This will allow the left-hand
	// range's split to complete and unblock the right-hand range.
	mtc.restartStore(1)
	mtc.restartStore(2)

	// Perform writes on both sides. This is not strictly necessary but
	// it helps wake up dormant ranges that would otherwise have to wait
	// for retry timeouts.
	incArgs = incrementArgs(leftKey, 10)
	if _, err := client.SendWrapped(mtc.distSender, nil, &incArgs); err != nil {
		t.Fatal(err)
	}
	mtc.waitForValues(leftKey, 3*time.Second, []int64{0, 11, 11, 11, 0, 0})

	incArgs = incrementArgs(rightKey, 200)
	if _, err := client.SendWrapped(mtc.distSender, nil, &incArgs); err != nil {
		t.Fatal(err)
	}
	mtc.waitForValues(rightKey, 3*time.Second, []int64{0, 0, 0, 222, 222, 222})

}

// TestStoreSplitReadRace prevents regression of #3148. It begins a couple of
// read requests and lets them complete while a split is happening; the reads
// hit the second half of the split. If the split happens non-atomically with
// respect to the reads (and in particular their update of the timestamp
// cache), then some of them may not be reflected in the timestamp cache of the
// new range, in which case this test would fail.
func TestStoreSplitReadRace(t *testing.T) {
	defer leaktest.AfterTest(t)
	defer func() { storage.TestingCommandFilter = nil }()
	splitKey := roachpb.Key("a")
	key := func(i int) roachpb.Key {
		return append(splitKey.Next(), []byte(fmt.Sprintf("%03d", i))...)
	}

	getContinues := make(chan struct{})
	var getStarted sync.WaitGroup
	storage.TestingCommandFilter = func(args roachpb.Request, h roachpb.Header) error {
		if et, ok := args.(*roachpb.EndTransactionRequest); ok {
			st := et.InternalCommitTrigger.GetSplitTrigger()
			if st == nil || !st.UpdatedDesc.EndKey.Equal(splitKey) {
				return nil
			}
			close(getContinues)
		} else if args.Method() == roachpb.Get &&
			bytes.HasPrefix(args.Header().Key, splitKey.Next()) {
			getStarted.Done()
			<-getContinues
		}
		return nil
	}
	store, stopper := createTestStore(t)
	defer stopper.Stop()

	now := store.Clock().Now()
	var wg sync.WaitGroup

	ts := func(i int) roachpb.Timestamp {
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
			if _, err := client.SendWrappedWith(rg1(store), nil, h, &args); err != nil {
				t.Fatal(err)
			}
		}(i)
	}

	getStarted.Wait()

	wg.Add(1)
	func() {
		defer wg.Done()
		args := adminSplitArgs(roachpb.KeyMin, splitKey)
		if _, err := client.SendWrapped(rg1(store), nil, &args); err != nil {
			t.Fatal(err)
		}
	}()

	wg.Wait()

	for i := 0; i < num; i++ {
		var h roachpb.Header
		h.Timestamp = now
		args := putArgs(key(i), []byte("foo"))
		h.RangeID = store.LookupReplica(keys.Addr(args.Key), nil).Desc().RangeID
		reply, err := client.SendWrappedWith(store, nil, h, &args)
		if err != nil {
			t.Fatal(err)
		}
		if reply.Header().Timestamp.Less(ts(i)) {
			t.Fatalf("%d: expected Put to be forced higher than %s by timestamp caches, but wrote at %s", i, ts(i), reply.Header().Timestamp)
		}
	}
}

// TestLeaderAfterSplit verifies that a raft group created by a split
// elects a leader without waiting for an election timeout.
func TestLeaderAfterSplit(t *testing.T) {
	defer leaktest.AfterTest(t)
	storeContext := storage.TestStoreContext
	storeContext.RaftElectionTimeoutTicks = 1000000
	mtc := &multiTestContext{
		storeContext: &storeContext,
	}
	mtc.Start(t, 3)
	defer mtc.Stop()

	mtc.replicateRange(1, 0, 1, 2)

	leftKey := roachpb.Key("a")
	splitKey := roachpb.Key("m")
	rightKey := roachpb.Key("z")

	splitArgs := adminSplitArgs(roachpb.KeyMin, splitKey)
	if _, err := client.SendWrapped(mtc.distSender, nil, &splitArgs); err != nil {
		t.Fatal(err)
	}

	incArgs := incrementArgs(leftKey, 1)
	if _, err := client.SendWrapped(mtc.distSender, nil, &incArgs); err != nil {
		t.Fatal(err)
	}

	incArgs = incrementArgs(rightKey, 2)
	if _, err := client.SendWrapped(mtc.distSender, nil, &incArgs); err != nil {
		t.Fatal(err)
	}
}
