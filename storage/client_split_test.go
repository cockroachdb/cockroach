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
	"regexp"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
)

func adminSplitArgs(key, splitKey []byte, raftID proto.RaftID, storeID proto.StoreID) (*proto.AdminSplitRequest, *proto.AdminSplitResponse) {
	args := &proto.AdminSplitRequest{
		RequestHeader: proto.RequestHeader{
			Key:     key,
			RaftID:  raftID,
			Replica: proto.Replica{StoreID: storeID},
		},
		SplitKey: splitKey,
	}
	reply := &proto.AdminSplitResponse{}
	return args, reply
}

func verifyRangeStats(eng engine.Engine, raftID proto.RaftID, expMS proto.MVCCStats) error {
	var ms proto.MVCCStats
	if err := engine.MVCCGetRangeStats(eng, raftID, &ms); err != nil {
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
		keys.MakeKey(keys.ConfigAccountingPrefix, []byte("a")),
		keys.MakeKey(keys.ConfigPermissionPrefix, []byte("a")),
		keys.MakeKey(keys.ConfigZonePrefix, []byte("a")),
	} {
		args, reply := adminSplitArgs(proto.KeyMin, key, 1, store.StoreID())
		err := store.ExecuteCmd(context.Background(), client.Call{Args: args, Reply: reply})
		if err == nil {
			t.Fatalf("%q: split succeeded unexpectedly", key)
		}
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

	args, reply := adminSplitArgs(proto.KeyMin, []byte("a"), 1, store.StoreID())
	if err := store.ExecuteCmd(context.Background(), client.Call{Args: args, Reply: reply}); err != nil {
		t.Fatal(err)
	}
	// This second split will try to split at end of first split range.
	if err := store.ExecuteCmd(context.Background(), client.Call{Args: args, Reply: reply}); err == nil {
		t.Fatalf("split succeeded unexpectedly")
	}
	// Now try to split at start of new range.
	args, reply = adminSplitArgs(proto.KeyMin, []byte("a"), 2, store.StoreID())
	if err := store.ExecuteCmd(context.Background(), client.Call{Args: args, Reply: reply}); err == nil {
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

	concurrentCount := int32(10)
	wg := sync.WaitGroup{}
	wg.Add(int(concurrentCount))
	failureCount := int32(0)
	for i := int32(0); i < concurrentCount; i++ {
		go func() {
			args, reply := adminSplitArgs(proto.KeyMin, []byte("a"), 1, store.StoreID())
			err := store.ExecuteCmd(context.Background(), client.Call{Args: args, Reply: reply})
			if err != nil {
				if matched, regexpErr := regexp.MatchString("range is already split at key", err.Error()); !matched || regexpErr != nil {
					t.Errorf("error %s didn't match regex %v", err, regexpErr)
				} else {
					atomic.AddInt32(&failureCount, 1)
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
	if failureCount != concurrentCount-1 {
		t.Fatalf("concurrent splits succeeded unexpectedly; failureCount=%d", failureCount)
	}
}

// TestStoreRangeSplit executes a split of a range and verifies that the
// resulting ranges respond to the right key ranges and that their stats
// and response caches have been properly accounted for.
func TestStoreRangeSplit(t *testing.T) {
	defer leaktest.AfterTest(t)
	store, stopper := createTestStore(t)
	defer stopper.Stop()
	raftID := proto.RaftID(1)
	splitKey := proto.Key("m")
	content := proto.Key("asdvb")

	// First, write some values left and right of the proposed split key.
	pArgs, pReply := putArgs([]byte("c"), content, raftID, store.StoreID())
	if err := store.ExecuteCmd(context.Background(), client.Call{Args: pArgs, Reply: pReply}); err != nil {
		t.Fatal(err)
	}
	pArgs, pReply = putArgs([]byte("x"), content, raftID, store.StoreID())
	if err := store.ExecuteCmd(context.Background(), client.Call{Args: pArgs, Reply: pReply}); err != nil {
		t.Fatal(err)
	}

	// Increments are a good way of testing the response cache. Up here, we
	// address them to the original range, then later to the one that contains
	// the key.
	lIncArgs, lIncReply := incrementArgs([]byte("apoptosis"), 100, raftID, store.StoreID())
	lIncArgs.CmdID = proto.ClientCmdID{WallTime: 123, Random: 423}
	if err := store.ExecuteCmd(context.Background(), client.Call{Args: lIncArgs, Reply: lIncReply}); err != nil {
		t.Fatal(err)
	}
	rIncArgs, rIncReply := incrementArgs([]byte("wobble"), 10, raftID, store.StoreID())
	rIncArgs.CmdID = proto.ClientCmdID{WallTime: 12, Random: 42}
	if err := store.ExecuteCmd(context.Background(), client.Call{Args: rIncArgs, Reply: rIncReply}); err != nil {
		t.Fatal(err)
	}

	// Get the original stats for key and value bytes.
	var ms proto.MVCCStats
	if err := engine.MVCCGetRangeStats(store.Engine(), raftID, &ms); err != nil {
		t.Fatal(err)
	}
	keyBytes, valBytes := ms.KeyBytes, ms.ValBytes

	// Split the range.
	args, reply := adminSplitArgs(proto.KeyMin, splitKey, 1, store.StoreID())
	if err := store.ExecuteCmd(context.Background(), client.Call{Args: args, Reply: reply}); err != nil {
		t.Fatal(err)
	}

	// Verify no intents remains on range descriptor keys.
	for _, key := range []proto.Key{keys.RangeDescriptorKey(proto.KeyMin), keys.RangeDescriptorKey(splitKey)} {
		if _, err := engine.MVCCGet(store.Engine(), key, store.Clock().Now(), true, nil); err != nil {
			t.Fatal(err)
		}
	}

	rng := store.LookupRange(proto.KeyMin, nil)
	newRng := store.LookupRange([]byte("m"), nil)
	if !bytes.Equal(newRng.Desc().StartKey, splitKey) || !bytes.Equal(splitKey, rng.Desc().EndKey) {
		t.Errorf("ranges mismatched, wanted %q=%q=%q", newRng.Desc().StartKey, splitKey, rng.Desc().EndKey)
	}
	if !bytes.Equal(newRng.Desc().EndKey, proto.KeyMax) || !bytes.Equal(rng.Desc().StartKey, proto.KeyMin) {
		t.Errorf("new ranges do not cover KeyMin-KeyMax, but only %q-%q", rng.Desc().StartKey, newRng.Desc().EndKey)
	}

	// Try to get values from both left and right of where the split happened.
	gArgs, gReply := getArgs([]byte("c"), raftID, store.StoreID())
	if err := store.ExecuteCmd(context.Background(), client.Call{Args: gArgs, Reply: gReply}); err != nil ||
		!bytes.Equal(gReply.Value.Bytes, content) {
		t.Fatal(err)
	}
	gArgs, gReply = getArgs([]byte("x"), newRng.Desc().RaftID, store.StoreID())
	if err := store.ExecuteCmd(context.Background(), client.Call{Args: gArgs, Reply: gReply}); err != nil ||
		!bytes.Equal(gReply.Value.Bytes, content) {
		t.Fatal(err)
	}

	// Send out an increment request copied from above (same ClientCmdID) which
	// remains in the old range.
	lIncReply = &proto.IncrementResponse{}
	if err := store.ExecuteCmd(context.Background(), client.Call{Args: lIncArgs, Reply: lIncReply}); err != nil {
		t.Fatal(err)
	}
	if lIncReply.NewValue != 100 {
		t.Errorf("response cache broken in old range, expected %d but got %d", lIncArgs.Increment, lIncReply.NewValue)
	}

	// Send out the same increment copied from above (same ClientCmdID), but
	// now to the newly created range (which should hold that key).
	rIncArgs.RequestHeader.RaftID = newRng.Desc().RaftID
	rIncReply = &proto.IncrementResponse{}
	if err := store.ExecuteCmd(context.Background(), client.Call{Args: rIncArgs, Reply: rIncReply}); err != nil {
		t.Fatal(err)
	}
	if rIncReply.NewValue != 10 {
		t.Errorf("response cache not copied correctly to new range, expected %d but got %d", rIncArgs.Increment, rIncReply.NewValue)
	}

	// Compare stats of split ranges to ensure they are non ero and
	// exceed the original range when summed.
	var left, right proto.MVCCStats
	if err := engine.MVCCGetRangeStats(store.Engine(), raftID, &left); err != nil {
		t.Fatal(err)
	}
	lKeyBytes, lValBytes := left.KeyBytes, left.ValBytes
	if err := engine.MVCCGetRangeStats(store.Engine(), newRng.Desc().RaftID, &right); err != nil {
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

	// Split the range at the first user key.
	args, reply := adminSplitArgs(proto.KeyMin, proto.Key("\x01"), 1, store.StoreID())
	if err := store.ExecuteCmd(context.Background(), client.Call{Args: args, Reply: reply}); err != nil {
		t.Fatal(err)
	}
	// Verify empty range has empty stats.
	rng := store.LookupRange(proto.Key("\x01"), nil)
	// NOTE that this value is expected to change over time, depending on what
	// we store in the sys-local keyspace. Update it accordingly for this test.
	if err := verifyRangeStats(store.Engine(), rng.Desc().RaftID, proto.MVCCStats{}); err != nil {
		t.Fatal(err)
	}

	// Write random data.
	src := rand.New(rand.NewSource(0))
	for i := 0; i < 100; i++ {
		key := util.RandBytes(src, int(src.Int31n(1<<7)))
		val := util.RandBytes(src, int(src.Int31n(1<<8)))
		pArgs, pReply := putArgs(key, val, rng.Desc().RaftID, store.StoreID())
		pArgs.Timestamp = store.Clock().Now()
		if err := store.ExecuteCmd(context.Background(), client.Call{Args: pArgs, Reply: pReply}); err != nil {
			t.Fatal(err)
		}
	}
	// Get the range stats now that we have data.
	var ms proto.MVCCStats
	if err := engine.MVCCGetRangeStats(store.Engine(), rng.Desc().RaftID, &ms); err != nil {
		t.Fatal(err)
	}

	// Split the range at approximate halfway point ("Z" in string "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz").
	args, reply = adminSplitArgs(proto.Key("\x01"), proto.Key("Z"), rng.Desc().RaftID, store.StoreID())
	if err := store.ExecuteCmd(context.Background(), client.Call{Args: args, Reply: reply}); err != nil {
		t.Fatal(err)
	}

	var msLeft, msRight proto.MVCCStats
	if err := engine.MVCCGetRangeStats(store.Engine(), rng.Desc().RaftID, &msLeft); err != nil {
		t.Fatal(err)
	}
	rngRight := store.LookupRange(proto.Key("Z"), nil)
	if err := engine.MVCCGetRangeStats(store.Engine(), rngRight.Desc().RaftID, &msRight); err != nil {
		t.Fatal(err)
	}

	// The stats should be exactly equal when added.
	expMS := proto.MVCCStats{
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
func fillRange(store *storage.Store, raftID proto.RaftID, prefix proto.Key, bytes int64, t *testing.T) {
	src := rand.New(rand.NewSource(0))
	for {
		var ms proto.MVCCStats
		if err := engine.MVCCGetRangeStats(store.Engine(), raftID, &ms); err != nil {
			t.Fatal(err)
		}
		keyBytes, valBytes := ms.KeyBytes, ms.ValBytes
		if keyBytes+valBytes >= bytes {
			return
		}
		key := append(append([]byte(nil), prefix...), util.RandBytes(src, 100)...)
		val := util.RandBytes(src, int(src.Int31n(1<<8)))
		pArgs, pReply := putArgs(key, val, raftID, store.StoreID())
		pArgs.Timestamp = store.Clock().Now()
		if err := store.ExecuteCmd(context.Background(), client.Call{Args: pArgs, Reply: pReply}); err != nil {
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
	defer stopper.Stop()

	maxBytes := int64(1 << 16)
	rng := store.LookupRange(proto.KeyMin, nil)
	fillRange(store, rng.Desc().RaftID, proto.Key("test"), maxBytes, t)

	// Rewrite zone config with range max bytes set to 64K.
	// This will cause the split queue to split the range in the background.
	// This must happen after fillRange() because that function is not using
	// a full-fledged client and cannot handle running concurrently with splits.
	zoneConfig := &proto.ZoneConfig{
		ReplicaAttrs: []proto.Attributes{
			{},
			{},
			{},
		},
		RangeMinBytes: 1 << 8,
		RangeMaxBytes: maxBytes,
	}
	key := keys.MakeKey(keys.ConfigZonePrefix, proto.KeyMin)
	if err := store.DB().Put(key, zoneConfig); err != nil {
		t.Fatal(err)
	}

	// See if the range's max bytes is modified via gossip callback within 50ms.
	if err := util.IsTrueWithin(func() bool {
		return rng.GetMaxBytes() == zoneConfig.RangeMaxBytes
	}, 50*time.Millisecond); err != nil {
		t.Fatalf("failed to notice range max bytes update: %s", err)
	}

	// Verify that the range is in fact split (give it a second for very slow test machines).
	if err := util.IsTrueWithin(func() bool {
		newRng := store.LookupRange(proto.Key("\xff\x00"), nil)
		return newRng != rng
	}, time.Second); err != nil {
		t.Errorf("expected range to split within 1s")
	}
}

// TestStoreRangeSplitWithMaxBytesUpdate tests a scenario where a new
// zone config that updates the max bytes is set and triggers a range
// split.
//
// TODO(kkaneda): Fix this test once Issue #1355 is addressed and a
// new range can get a new value of max bytes without another zone
// config change.
func TestStoreRangeSplitWithMaxBytesUpdate(t *testing.T) {
	defer leaktest.AfterTest(t)
	store, stopper := createTestStore(t)
	defer stopper.Stop()

	origRng := store.LookupRange(proto.KeyMin, nil)

	// Set the maxBytes and trigger a range split.
	key := keys.MakeKey(keys.ConfigZonePrefix, proto.Key("db1"))
	maxBytes := int64(1 << 16)
	zoneConfig := &proto.ZoneConfig{
		ReplicaAttrs: []proto.Attributes{
			{},
			{},
			{},
		},
		RangeMinBytes: 1 << 8,
		RangeMaxBytes: maxBytes,
	}
	if err := store.DB().Put(key, zoneConfig); err != nil {
		t.Fatal(err)
	}

	// Verify that the range is split, but the max bytes has not yet been updated.
	util.SucceedsWithin(t, time.Second, func() error {
		newRng := store.LookupRange(proto.Key("db1"), nil)
		if newRng.Desc().RaftID == origRng.Desc().RaftID {
			return util.Error("expected new range created by split")
		}
		if newRng.GetMaxBytes() != 0 {
			return util.Errorf("expected zero max bytes for the new range, but got %d",
				newRng.GetMaxBytes())
		}
		return nil
	})

	// Update the zone config to trigger the range max bytes change.
	maxBytes *= 2
	zoneConfig.RangeMaxBytes = maxBytes
	if err := store.DB().Put(key, zoneConfig); err != nil {
		t.Fatal(err)
	}

	util.SucceedsWithin(t, time.Second, func() error {
		newRng := store.LookupRange(proto.Key("db1"), nil)
		if newRng.GetMaxBytes() != maxBytes {
			return util.Errorf("expected %d max bytes for the new range, but got %d",
				maxBytes, newRng.GetMaxBytes())
		}
		return nil
	})
}

// TestStoreRangeSplitOnConfigs verifies that config changes to both
// accounting and zone configs cause ranges to be split along prefix
// boundaries.
func TestStoreRangeSplitOnConfigs(t *testing.T) {
	defer leaktest.AfterTest(t)
	store, stopper := createTestStore(t)
	defer stopper.Stop()

	acctConfig := &proto.AcctConfig{}
	zoneConfig := &proto.ZoneConfig{}

	// Write zone configs for db3 & db4.
	b := &client.Batch{}
	for _, k := range []string{"db4", "db3"} {
		b.Put(keys.MakeKey(keys.ConfigZonePrefix, proto.Key(k)), zoneConfig)
	}
	// Write accounting configs for db1 & db2.
	for _, k := range []string{"db2", "db1"} {
		b.Put(keys.MakeKey(keys.ConfigAccountingPrefix, proto.Key(k)), acctConfig)
	}
	if err := store.DB().Run(b); err != nil {
		t.Fatal(err)
	}
	log.Infof("wrote updated configs")

	// Check that we split into expected ranges in allotted time.
	expKeys := []proto.Key{
		proto.Key("\x00\x00meta2db1"),
		proto.Key("\x00\x00meta2db2"),
		proto.Key("\x00\x00meta2db3"),
		proto.Key("\x00\x00meta2db4"),
		proto.Key("\x00\x00meta2db5"),
		keys.MakeKey(proto.Key("\x00\x00meta2"), proto.KeyMax),
	}
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
	}, 500*time.Millisecond); err != nil {
		t.Errorf("expected splits not found: %s", err)
	}
}

// TestStoreRangeManySplits splits many ranges at once.
func TestStoreRangeManySplits(t *testing.T) {
	defer leaktest.AfterTest(t)
	store, stopper := createTestStore(t)
	defer stopper.Stop()

	// Write zone configs to trigger the first round of splits.
	numDbs := 20
	zoneConfig := &proto.ZoneConfig{}
	b := &client.Batch{}
	for i := 0; i < numDbs; i++ {
		key := proto.Key(fmt.Sprintf("db%02d", 20-i))
		b.Put(keys.MakeKey(keys.ConfigZonePrefix, key), zoneConfig)
	}
	if err := store.DB().Run(b); err != nil {
		t.Fatal(err)
	}

	// Check that we finish splitting in allotted time.
	expKeys := []proto.Key{}
	// Expect numDb+1 keys as the zone config for "db20" creates
	// "meta2db20" and "meta2db21" as start/end keys.
	for i := 1; i <= numDbs+1; i++ {
		if i%10 == 0 {
			expKeys = append(expKeys, proto.Key(fmt.Sprintf("\x00\x00meta2db%d:", i/10-1)))
		}
		expKeys = append(expKeys, proto.Key(fmt.Sprintf("\x00\x00meta2db%02d", i)))
	}
	expKeys = append(expKeys, keys.MakeKey(proto.Key("\x00\x00meta2"), proto.KeyMax))
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
	}, 1*time.Second); err != nil {
		t.Errorf("expected splits not found: %s", err)
	}

	// Then start the second round of splits.
	acctConfig := &proto.AcctConfig{}
	b = &client.Batch{}
	for i := 0; i < numDbs; i++ {
		key := proto.Key(fmt.Sprintf("db%02d/table", 20-i))
		b.Put(keys.MakeKey(keys.ConfigZonePrefix, key), acctConfig)
	}
	if err := store.DB().Run(b); err != nil {
		t.Fatal(err)
	}

	// Check the result of splits again.
	expKeys = []proto.Key{}
	for i := 1; i <= numDbs; i++ {
		if i%10 == 0 {
			expKeys = append(expKeys, proto.Key(fmt.Sprintf("\x00\x00meta2db%d:", i/10-1)))
		}
		expKeys = append(expKeys,
			proto.Key(fmt.Sprintf("\x00\x00meta2db%02d", i)),
			proto.Key(fmt.Sprintf("\x00\x00meta2db%02d/table", i)),
			proto.Key(fmt.Sprintf("\x00\x00meta2db%02d/tablf", i)))
	}
	expKeys = append(expKeys,
		proto.Key("\x00\x00meta2db21"),
		keys.MakeKey(proto.Key("\x00\x00meta2"), proto.KeyMax))
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
	}, 1*time.Second); err != nil {
		t.Errorf("expected splits not found: %s", err)
	}
}
