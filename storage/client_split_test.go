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
	"regexp"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

func adminSplitArgs(key, splitKey []byte, raftID int64, storeID proto.StoreID) (*proto.AdminSplitRequest, *proto.AdminSplitResponse) {
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

func verifyRangeStats(eng engine.Engine, raftID int64, expMS engine.MVCCStats, t *testing.T) {
	var ms engine.MVCCStats
	if err := engine.MVCCGetRangeStats(eng, raftID, &ms); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(expMS, ms) {
		t.Errorf("expected stats %+v; got %+v", expMS, ms)
	}
}

// TestStoreRangeSplitAtIllegalKeys verifies a range cannot be split
// at illegal keys.
func TestStoreRangeSplitAtIllegalKeys(t *testing.T) {
	store := createTestStore(t)
	defer store.Stop()

	for _, key := range []proto.Key{
		engine.KeyMeta1Prefix,
		engine.MakeKey(engine.KeyMeta1Prefix, []byte("a")),
		engine.MakeKey(engine.KeyMeta1Prefix, engine.KeyMax),
		engine.MakeKey(engine.KeyConfigAccountingPrefix, []byte("a")),
		engine.MakeKey(engine.KeyConfigPermissionPrefix, []byte("a")),
		engine.MakeKey(engine.KeyConfigZonePrefix, []byte("a")),
	} {
		args, reply := adminSplitArgs(engine.KeyMin, key, 1, store.StoreID())
		err := store.ExecuteCmd(proto.AdminSplit, args, reply)
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
	store := createTestStore(t)
	defer store.Stop()

	args, reply := adminSplitArgs(engine.KeyMin, []byte("a"), 1, store.StoreID())
	if err := store.ExecuteCmd(proto.AdminSplit, args, reply); err != nil {
		t.Fatal(err)
	}
	// This second split will try to split at end of first split range.
	if err := store.ExecuteCmd(proto.AdminSplit, args, reply); err == nil {
		t.Fatalf("split succeeded unexpectedly")
	}
	// Now try to split at start of new range.
	args, reply = adminSplitArgs(engine.KeyMin, []byte("a"), 2, store.StoreID())
	if err := store.ExecuteCmd(proto.AdminSplit, args, reply); err == nil {
		t.Fatalf("split succeeded unexpectedly")
	}
}

// TestStoreRangeSplitConcurrent verifies that concurrent range splits
// of the same range are disallowed.
func TestStoreRangeSplitConcurrent(t *testing.T) {
	store := createTestStore(t)
	defer store.Stop()

	concurrentCount := int32(10)
	wg := sync.WaitGroup{}
	wg.Add(int(concurrentCount))
	failureCount := int32(0)
	for i := int32(0); i < concurrentCount; i++ {
		go func() {
			args, reply := adminSplitArgs(engine.KeyMin, []byte("a"), 1, store.StoreID())
			err := store.ExecuteCmd(proto.AdminSplit, args, reply)
			if err != nil {
				if matched, regexpErr := regexp.MatchString(".*range 1 metadata locked", err.Error()); !matched || regexpErr != nil {
					t.Errorf("error %s didn't match: %s", err, regexpErr)
				} else {
					atomic.AddInt32(&failureCount, 1)
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
	if failureCount != concurrentCount-1 {
		t.Fatalf("concurrent splits succeeded unexpectedly")
	}
}

// TestStoreRangeSplit executes a split of a range and verifies that the
// resulting ranges respond to the right key ranges and that their stats
// and response caches have been properly accounted for.
func TestStoreRangeSplit(t *testing.T) {
	store := createTestStore(t)
	defer store.Stop()
	raftID := int64(1)
	splitKey := proto.Key("m")
	content := proto.Key("asdvb")

	// First, write some values left and right of the proposed split key.
	pArgs, pReply := putArgs([]byte("c"), content, raftID, store.StoreID())
	if err := store.ExecuteCmd(proto.Put, pArgs, pReply); err != nil {
		t.Fatal(err)
	}
	pArgs, pReply = putArgs([]byte("x"), content, raftID, store.StoreID())
	if err := store.ExecuteCmd(proto.Put, pArgs, pReply); err != nil {
		t.Fatal(err)
	}

	// Increments are a good way of testing the response cache. Up here, we
	// address them to the original range, then later to the one that contains
	// the key.
	lIncArgs, lIncReply := incrementArgs([]byte("apoptosis"), 100, raftID, store.StoreID())
	lIncArgs.CmdID = proto.ClientCmdID{WallTime: 123, Random: 423}
	if err := store.ExecuteCmd(proto.Increment, lIncArgs, lIncReply); err != nil {
		t.Fatal(err)
	}
	rIncArgs, rIncReply := incrementArgs([]byte("wobble"), 10, raftID, store.StoreID())
	rIncArgs.CmdID = proto.ClientCmdID{WallTime: 12, Random: 42}
	if err := store.ExecuteCmd(proto.Increment, rIncArgs, rIncReply); err != nil {
		t.Fatal(err)
	}

	// Get the original stats for key and value bytes.
	keyBytes, err := engine.MVCCGetRangeStat(store.Engine(), raftID, engine.StatKeyBytes)
	if err != nil {
		t.Fatal(err)
	}
	valBytes, err := engine.MVCCGetRangeStat(store.Engine(), raftID, engine.StatValBytes)
	if err != nil {
		t.Fatal(err)
	}

	// Split the range.
	args, reply := adminSplitArgs(engine.KeyMin, splitKey, 1, store.StoreID())
	if err := store.ExecuteCmd(proto.AdminSplit, args, reply); err != nil {
		t.Fatal(err)
	}

	rng := store.LookupRange(engine.KeyMin, nil)
	newRng := store.LookupRange([]byte("m"), nil)
	if !bytes.Equal(newRng.Desc().StartKey, splitKey) || !bytes.Equal(splitKey, rng.Desc().EndKey) {
		t.Errorf("ranges mismatched, wanted %q=%q=%q", newRng.Desc().StartKey, splitKey, rng.Desc().EndKey)
	}
	if !bytes.Equal(newRng.Desc().EndKey, engine.KeyMax) || !bytes.Equal(rng.Desc().StartKey, engine.KeyMin) {
		t.Errorf("new ranges do not cover KeyMin-KeyMax, but only %q-%q", rng.Desc().StartKey, newRng.Desc().EndKey)
	}

	// Try to get values from both left and right of where the split happened.
	gArgs, gReply := getArgs([]byte("c"), raftID, store.StoreID())
	if err := store.ExecuteCmd(proto.Get, gArgs, gReply); err != nil ||
		!bytes.Equal(gReply.Value.Bytes, content) {
		t.Fatal(err)
	}
	gArgs, gReply = getArgs([]byte("x"), newRng.Desc().RaftID, store.StoreID())
	if err := store.ExecuteCmd(proto.Get, gArgs, gReply); err != nil ||
		!bytes.Equal(gReply.Value.Bytes, content) {
		t.Fatal(err)
	}

	// Send out an increment request copied from above (same ClientCmdID) which
	// remains in the old range.
	lIncReply = &proto.IncrementResponse{}
	if err := store.ExecuteCmd(proto.Increment, lIncArgs, lIncReply); err != nil {
		t.Fatal(err)
	}
	if lIncReply.NewValue != 100 {
		t.Errorf("response cache broken in old range, expected %d but got %d", lIncArgs.Increment, lIncReply.NewValue)
	}

	// Send out the same increment copied from above (same ClientCmdID), but
	// now to the newly created range (which should hold that key).
	rIncArgs.RequestHeader.RaftID = newRng.Desc().RaftID
	rIncReply = &proto.IncrementResponse{}
	if err := store.ExecuteCmd(proto.Increment, rIncArgs, rIncReply); err != nil {
		t.Fatal(err)
	}
	if rIncReply.NewValue != 10 {
		t.Errorf("response cache not copied correctly to new range, expected %d but got %d", rIncArgs.Increment, rIncReply.NewValue)
	}

	// Compare stats of split ranges to ensure they are non ero and
	// exceed the original range when summed.
	lKeyBytes, err := engine.MVCCGetRangeStat(store.Engine(), raftID, engine.StatKeyBytes)
	if err != nil {
		t.Fatal(err)
	}
	lValBytes, err := engine.MVCCGetRangeStat(store.Engine(), raftID, engine.StatValBytes)
	if err != nil {
		t.Fatal(err)
	}
	rKeyBytes, err := engine.MVCCGetRangeStat(store.Engine(), newRng.Desc().RaftID, engine.StatKeyBytes)
	if err != nil {
		t.Fatal(err)
	}
	rValBytes, err := engine.MVCCGetRangeStat(store.Engine(), newRng.Desc().RaftID, engine.StatValBytes)
	if err != nil {
		t.Fatal(err)
	}

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
	store := createTestStore(t)
	defer store.Stop()

	// Split the range at the first user key.
	args, reply := adminSplitArgs(engine.KeyMin, proto.Key("\x01"), 1, store.StoreID())
	if err := store.ExecuteCmd(proto.AdminSplit, args, reply); err != nil {
		t.Fatal(err)
	}
	// Verify empty range has empty stats.
	rng := store.LookupRange(proto.Key("\x01"), nil)
	verifyRangeStats(store.Engine(), rng.Desc().RaftID, engine.MVCCStats{}, t)

	// Write random data.
	src := rand.New(rand.NewSource(0))
	for i := 0; i < 100; i++ {
		key := util.RandBytes(src, int(src.Int31n(1<<7)))
		val := util.RandBytes(src, int(src.Int31n(1<<8)))
		pArgs, pReply := putArgs(key, val, rng.Desc().RaftID, store.StoreID())
		pArgs.Timestamp = store.Clock().Now()
		if err := store.ExecuteCmd(proto.Put, pArgs, pReply); err != nil {
			t.Fatal(err)
		}
	}
	// Get the range stats now that we have data.
	var ms engine.MVCCStats
	if err := engine.MVCCGetRangeStats(store.Engine(), rng.Desc().RaftID, &ms); err != nil {
		t.Fatal(err)
	}

	// Split the range at approximate halfway point ("Z" in string "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz").
	args, reply = adminSplitArgs(proto.Key("\x01"), proto.Key("Z"), rng.Desc().RaftID, store.StoreID())
	if err := store.ExecuteCmd(proto.AdminSplit, args, reply); err != nil {
		t.Fatal(err)
	}

	var msLeft, msRight engine.MVCCStats
	if err := engine.MVCCGetRangeStats(store.Engine(), rng.Desc().RaftID, &msLeft); err != nil {
		t.Fatal(err)
	}
	rngRight := store.LookupRange(proto.Key("Z"), nil)
	if err := engine.MVCCGetRangeStats(store.Engine(), rngRight.Desc().RaftID, &msRight); err != nil {
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
	if !reflect.DeepEqual(expMS, ms) {
		t.Errorf("expected left and right ranges to equal original: %+v + %+v != %+v", msLeft, msRight, ms)
	}
}

// fillRange writes keys with the given prefix and associated values
// until bytes bytes have been written.
func fillRange(store *storage.Store, raftID int64, prefix proto.Key, bytes int64, t *testing.T) {
	src := rand.New(rand.NewSource(0))
	for {
		keyBytes, err := engine.MVCCGetRangeStat(store.Engine(), raftID, engine.StatKeyBytes)
		if err != nil {
			t.Fatal(err)
		}
		valBytes, err := engine.MVCCGetRangeStat(store.Engine(), raftID, engine.StatValBytes)
		if err != nil {
			t.Fatal(err)
		}
		if keyBytes+valBytes >= bytes {
			return
		}
		key := append(append([]byte(nil), prefix...), util.RandBytes(src, 100)...)
		val := util.RandBytes(src, int(src.Int31n(1<<8)))
		pArgs, pReply := putArgs(key, val, raftID, store.StoreID())
		pArgs.Timestamp = store.Clock().Now()
		if err := store.ExecuteCmd(proto.Put, pArgs, pReply); err != nil {
			t.Fatal(err)
		}
	}
}

// TestStoreShouldSplit verifies that shouldSplit() takes into account the
// zone configuration to figure out what the maximum size of a range is.
// It further verifies that the range is in fact split on exceeding
// zone's RangeMaxBytes.
func TestStoreShouldSplit(t *testing.T) {
	store := createTestStore(t)
	defer store.Stop()

	// Rewrite zone config with range max bytes set to 256K.
	zoneConfig := &proto.ZoneConfig{
		ReplicaAttrs: []proto.Attributes{
			{},
			{},
			{},
		},
		RangeMinBytes: 1 << 8,
		RangeMaxBytes: 1 << 16,
	}
	if err := store.DB().PutProto(engine.MakeKey(engine.KeyConfigZonePrefix, engine.KeyMin), zoneConfig); err != nil {
		t.Fatal(err)
	}

	rng := store.LookupRange(engine.KeyMin, nil)
	if ok := rng.ShouldSplit(); ok {
		t.Errorf("range should not split with no data in it")
	}

	maxBytes := zoneConfig.RangeMaxBytes
	fillRange(store, rng.Desc().RaftID, proto.Key("test"), maxBytes, t)

	// Verify that the range is in fact split (give it a second for very slow test machines).
	if err := util.IsTrueWithin(func() bool {
		newRng := store.LookupRange(engine.KeyMax[:engine.KeyMaxLength-1], nil)
		return newRng != rng
	}, time.Second); err != nil {
		t.Errorf("expected range to split in 1s")
	}
}

// TestStoreRangeSplitOnConfigs verifies that config changes to both
// accounting and zone configs cause ranges to be split along prefix
// boundaries.
func TestStoreRangeSplitOnConfigs(t *testing.T) {
	store := createTestStore(t)
	defer store.Stop()

	acctConfig := &proto.AcctConfig{}
	zoneConfig := &proto.ZoneConfig{}

	// Write zone configs for db3 & db4.
	for _, k := range []string{"db4", "db3"} {
		store.DB().PreparePutProto(engine.MakeKey(engine.KeyConfigZonePrefix, proto.Key(k)), zoneConfig)
	}
	// Write accounting configs for db1 & db2.
	for _, k := range []string{"db2", "db1"} {
		store.DB().PreparePutProto(engine.MakeKey(engine.KeyConfigAccountingPrefix, proto.Key(k)), acctConfig)
	}
	if err := store.DB().Flush(); err != nil {
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
		engine.MakeKey(proto.Key("\x00\x00meta2"), engine.KeyMax),
	}
	if err := util.IsTrueWithin(func() bool {
		resp := &proto.ScanResponse{}
		if err := store.DB().Call(proto.Scan, proto.ScanArgs(engine.KeyMeta2Prefix, engine.KeyMetaMax, 0), resp); err != nil {
			t.Fatalf("failed to scan meta2 keys: %s", err)
		}
		var keys []proto.Key
		for _, r := range resp.Rows {
			keys = append(keys, r.Key)
		}
		return reflect.DeepEqual(keys, expKeys)
	}, 500*time.Millisecond); err != nil {
		t.Errorf("expected splits not found: %s", err)
	}
}
