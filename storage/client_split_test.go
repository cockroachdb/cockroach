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
	"math/rand"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/randutil"
)

func adminSplitArgs(key, splitKey []byte, rangeID proto.RangeID, storeID proto.StoreID) proto.AdminSplitRequest {
	return proto.AdminSplitRequest{
		RequestHeader: proto.RequestHeader{
			Key:     key,
			RangeID: rangeID,
			Replica: proto.Replica{StoreID: storeID},
		},
		SplitKey: splitKey,
	}
}

func verifyRangeStats(eng engine.Engine, rangeID proto.RangeID, expMS engine.MVCCStats) error {
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

	for _, key := range []proto.Key{
		keys.Meta1Prefix,
		keys.MakeKey(keys.Meta1Prefix, []byte("a")),
		keys.MakeKey(keys.Meta1Prefix, proto.KeyMax),
		keys.Meta2KeyMax,
		keys.MakeTablePrefix(10 /* system descriptor ID */),
	} {
		args := adminSplitArgs(proto.KeyMin, key, 1, store.StoreID())
		_, err := store.ExecuteCmd(context.Background(), &args)
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
	args := adminSplitArgs(key, key, 1, store.StoreID())
	_, err := store.ExecuteCmd(context.Background(), &args)
	if err != nil {
		t.Fatalf("%q: split unexpected error: %s", key, err)
	}

	// Update SystemConfig to trigger gossip.
	if err := store.DB().Txn(func(txn *client.Txn) error {
		txn.SetSystemDBTrigger()
		k := sql.MakeDescMetadataKey(sql.ID(keys.MaxReservedDescID + 1))
		return txn.Put(k, 10)
	}); err != nil {
		t.Fatal(err)
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

	args := adminSplitArgs(proto.KeyMin, []byte("a"), 1, store.StoreID())
	if _, err := store.ExecuteCmd(context.Background(), &args); err != nil {
		t.Fatal(err)
	}
	// This second split will try to split at end of first split range.
	if _, err := store.ExecuteCmd(context.Background(), &args); err == nil {
		t.Fatalf("split succeeded unexpectedly")
	}
	// Now try to split at start of new range.
	args = adminSplitArgs(proto.KeyMin, []byte("a"), 2, store.StoreID())
	if _, err := store.ExecuteCmd(context.Background(), &args); err == nil {
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

	splitKey := proto.Key("a")
	concurrentCount := int32(10)
	wg := sync.WaitGroup{}
	wg.Add(int(concurrentCount))
	failureCount := int32(0)
	for i := int32(0); i < concurrentCount; i++ {
		go func() {
			args := adminSplitArgs(proto.KeyMin, splitKey, 1, store.StoreID())
			_, err := store.ExecuteCmd(context.Background(), &args)
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
	rng := store.LookupReplica(proto.KeyMin, nil)
	newRng := store.LookupReplica(splitKey, nil)
	if !bytes.Equal(newRng.Desc().StartKey, splitKey) || !bytes.Equal(splitKey, rng.Desc().EndKey) {
		t.Errorf("ranges mismatched, wanted %q=%q=%q", newRng.Desc().StartKey, splitKey, rng.Desc().EndKey)
	}
	if !bytes.Equal(newRng.Desc().EndKey, proto.KeyMax) || !bytes.Equal(rng.Desc().StartKey, proto.KeyMin) {
		t.Errorf("new ranges do not cover KeyMin-KeyMax, but only %q-%q", rng.Desc().StartKey, newRng.Desc().EndKey)
	}
}

// TestStoreRangeSplit executes a split of a range and verifies that the
// resulting ranges respond to the right key ranges and that their stats
// and response caches have been properly accounted for.
func TestStoreRangeSplit(t *testing.T) {
	defer leaktest.AfterTest(t)
	store, stopper := createTestStore(t)
	defer stopper.Stop()
	rangeID := proto.RangeID(1)
	splitKey := proto.Key("m")
	content := proto.Key("asdvb")

	// First, write some values left and right of the proposed split key.
	pArgs := putArgs([]byte("c"), content, rangeID, store.StoreID())
	if _, err := store.ExecuteCmd(context.Background(), &pArgs); err != nil {
		t.Fatal(err)
	}
	pArgs = putArgs([]byte("x"), content, rangeID, store.StoreID())
	if _, err := store.ExecuteCmd(context.Background(), &pArgs); err != nil {
		t.Fatal(err)
	}

	// Increments are a good way of testing the response cache. Up here, we
	// address them to the original range, then later to the one that contains
	// the key.
	lIncArgs := incrementArgs([]byte("apoptosis"), 100, rangeID, store.StoreID())
	lIncArgs.CmdID = proto.ClientCmdID{WallTime: 123, Random: 423}
	if _, err := store.ExecuteCmd(context.Background(), &lIncArgs); err != nil {
		t.Fatal(err)
	}
	rIncArgs := incrementArgs([]byte("wobble"), 10, rangeID, store.StoreID())
	rIncArgs.CmdID = proto.ClientCmdID{WallTime: 12, Random: 42}
	if _, err := store.ExecuteCmd(context.Background(), &rIncArgs); err != nil {
		t.Fatal(err)
	}

	// Get the original stats for key and value bytes.
	var ms engine.MVCCStats
	if err := engine.MVCCGetRangeStats(store.Engine(), rangeID, &ms); err != nil {
		t.Fatal(err)
	}
	keyBytes, valBytes := ms.KeyBytes, ms.ValBytes

	// Split the range.
	args := adminSplitArgs(proto.KeyMin, splitKey, 1, store.StoreID())
	if _, err := store.ExecuteCmd(context.Background(), &args); err != nil {
		t.Fatal(err)
	}

	// Verify no intents remains on range descriptor keys.
	for _, key := range []proto.Key{keys.RangeDescriptorKey(proto.KeyMin), keys.RangeDescriptorKey(splitKey)} {
		if _, _, err := engine.MVCCGet(store.Engine(), key, store.Clock().Now(), true, nil); err != nil {
			t.Fatal(err)
		}
	}

	rng := store.LookupReplica(proto.KeyMin, nil)
	newRng := store.LookupReplica([]byte("m"), nil)
	if !bytes.Equal(newRng.Desc().StartKey, splitKey) || !bytes.Equal(splitKey, rng.Desc().EndKey) {
		t.Errorf("ranges mismatched, wanted %q=%q=%q", newRng.Desc().StartKey, splitKey, rng.Desc().EndKey)
	}
	if !bytes.Equal(newRng.Desc().EndKey, proto.KeyMax) || !bytes.Equal(rng.Desc().StartKey, proto.KeyMin) {
		t.Errorf("new ranges do not cover KeyMin-KeyMax, but only %q-%q", rng.Desc().StartKey, newRng.Desc().EndKey)
	}

	// Try to get values from both left and right of where the split happened.
	gArgs := getArgs([]byte("c"), rangeID, store.StoreID())
	if reply, err := store.ExecuteCmd(context.Background(), &gArgs); err != nil {
		t.Fatal(err)
	} else if gReply := reply.(*proto.GetResponse); !bytes.Equal(gReply.Value.Bytes, content) {
		t.Fatalf("actual value %q did not match expected value %q", gReply.Value.Bytes, content)
	}
	gArgs = getArgs([]byte("x"), newRng.Desc().RangeID, store.StoreID())
	if reply, err := store.ExecuteCmd(context.Background(), &gArgs); err != nil {
		t.Fatal(err)
	} else if gReply := reply.(*proto.GetResponse); !bytes.Equal(gReply.Value.Bytes, content) {
		t.Fatalf("actual value %q did not match expected value %q", gReply.Value.Bytes, content)
	}

	// Send out an increment request copied from above (same ClientCmdID) which
	// remains in the old range.
	if reply, err := store.ExecuteCmd(context.Background(), &lIncArgs); err != nil {
		t.Fatal(err)
	} else if lIncReply := reply.(*proto.IncrementResponse); lIncReply.NewValue != 100 {
		t.Errorf("response cache broken in old range, expected %d but got %d", lIncArgs.Increment, lIncReply.NewValue)
	}

	// Send out the same increment copied from above (same ClientCmdID), but
	// now to the newly created range (which should hold that key).
	rIncArgs.RequestHeader.RangeID = newRng.Desc().RangeID
	if reply, err := store.ExecuteCmd(context.Background(), &rIncArgs); err != nil {
		t.Fatal(err)
	} else if rIncReply := reply.(*proto.IncrementResponse); rIncReply.NewValue != 10 {
		t.Errorf("response cache not copied correctly to new range, expected %d but got %d", rIncArgs.Increment, rIncReply.NewValue)
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
	args := adminSplitArgs(proto.KeyMin, keyPrefix, 1, store.StoreID())
	if _, err := store.ExecuteCmd(context.Background(), &args); err != nil {
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
		pArgs := putArgs(key, val, rng.Desc().RangeID, store.StoreID())
		pArgs.Timestamp = store.Clock().Now()
		if _, err := store.ExecuteCmd(context.Background(), &pArgs); err != nil {
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
	args = adminSplitArgs(keyPrefix, midKey, rng.Desc().RangeID, store.StoreID())
	if _, err := store.ExecuteCmd(context.Background(), &args); err != nil {
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
func fillRange(store *storage.Store, rangeID proto.RangeID, prefix proto.Key, bytes int64, t *testing.T) {
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
		pArgs := putArgs(key, val, rangeID, store.StoreID())
		pArgs.Timestamp = store.Clock().Now()
		if _, err := store.ExecuteCmd(context.Background(), &pArgs); err != nil {
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
	originalRange := store.LookupReplica(proto.KeyMin, nil)
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
	if !proto.KeyMax.Equal(rng.Desc().EndKey) {
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
	if !proto.KeyMax.Equal(newRng.Desc().EndKey) {
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

	origRng := store.LookupReplica(proto.KeyMin, nil)

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

	// Write the initial sql values to the system DB as well
	// as the equivalent of table descriptors for X user tables.
	// This does two things:
	// - descriptor IDs are used to determine split keys
	// - the write triggers a SystemConfig update and gossip.
	// We should end up with splits at each user table prefix.
	if err := store.DB().Txn(func(txn *client.Txn) error {
		txn.SetSystemDBTrigger()
		for _, kv := range sql.GetInitialSystemValues() {
			if errPut := txn.Put(kv.Key, kv.Value.Bytes); errPut != nil {
				return util.Errorf("Could not put KV %+v: %v", kv, errPut)
			}
		}
		for i := 1; i <= 5; i++ {
			// We don't care about the values, just the keys.
			k := sql.MakeDescMetadataKey(sql.ID(keys.MaxReservedDescID + i))
			if errPut := txn.Put(k, i); errPut != nil {
				return util.Errorf("Could not put KV %+v: %v", k, errPut)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	expKeys := []proto.Key{}
	for i := 1; i <= 5; i++ {
		expKeys = append(expKeys, keys.MakeKey(keys.Meta2Prefix,
			keys.MakeTablePrefix(uint32(keys.MaxReservedDescID+i))))
	}
	expKeys = append(expKeys, keys.MakeKey(keys.Meta2Prefix, proto.KeyMax))

	if err := util.IsTrueWithin(func() bool {
		rows, err := store.DB().Scan(keys.Meta2Prefix, keys.MetaMax, 0)
		if err != nil {
			t.Fatalf("failed to scan meta2 keys: %s", err)
		}
		var keys []proto.Key
		for _, r := range rows {
			keys = append(keys, r.Key)
		}
		return reflect.DeepEqual(keys, expKeys)
	}, 5*time.Second); err != nil {
		t.Errorf("expected splits not found: %s", err)
	}

	// Write more descriptors for user tables.
	if err := store.DB().Txn(func(txn *client.Txn) error {
		txn.SetSystemDBTrigger()
		// This time, only write the last table descriptor. Splits
		// still occur for every ID.
		// We don't care about the values, just the keys.
		k := sql.MakeDescMetadataKey(sql.ID(keys.MaxReservedDescID + 10))
		return txn.Put(k, 10)
	}); err != nil {
		t.Fatal(err)
	}

	expKeys = []proto.Key{}
	for i := 1; i <= 10; i++ {
		expKeys = append(expKeys, keys.MakeKey(keys.Meta2Prefix,
			keys.MakeTablePrefix(uint32(keys.MaxReservedDescID+i))))
	}
	expKeys = append(expKeys, keys.MakeKey(keys.Meta2Prefix, proto.KeyMax))

	if err := util.IsTrueWithin(func() bool {
		rows, err := store.DB().Scan(keys.Meta2Prefix, keys.MetaMax, 0)
		if err != nil {
			t.Fatalf("failed to scan meta2 keys: %s", err)
		}
		var keys []proto.Key
		for _, r := range rows {
			keys = append(keys, r.Key)
		}
		return reflect.DeepEqual(keys, expKeys)
	}, 5*time.Second); err != nil {
		t.Errorf("expected splits not found: %s", err)
	}
}
