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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"testing"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
)

// createRangeData creates sample range data in all possible areas of
// the key space. Returns a slice of the encoded keys of all created
// data.
func createRangeData(r *Range, t *testing.T) []proto.EncodedKey {
	ts0 := proto.ZeroTimestamp
	ts := proto.Timestamp{WallTime: 1}
	keyTSs := []struct {
		key proto.Key
		ts  proto.Timestamp
	}{
		{engine.ResponseCacheKey(r.Desc.RaftID, &proto.ClientCmdID{WallTime: 1, Random: 1}), ts0},
		{engine.ResponseCacheKey(r.Desc.RaftID, &proto.ClientCmdID{WallTime: 2, Random: 2}), ts0},
		{engine.RaftLogKey(r.Desc.RaftID, 2), ts0},
		{engine.RaftLogKey(r.Desc.RaftID, 1), ts0},
		{engine.RaftStateKey(r.Desc.RaftID), ts0},
		{engine.RangeStatKey(r.Desc.RaftID, engine.StatKeyBytes), ts0},
		{engine.RangeStatKey(r.Desc.RaftID, engine.StatKeyCount), ts0},
		{engine.RangeDescriptorKey(r.Desc.StartKey), ts},
		{engine.RangeScanMetadataKey(r.Desc.StartKey), ts0},
		{engine.TransactionKey(r.Desc.StartKey, []byte("1234")), ts0},
		{engine.TransactionKey(r.Desc.StartKey.Next(), []byte("5678")), ts0},
		{engine.TransactionKey(r.Desc.EndKey.Prev(), []byte("2468")), ts0},
		{r.Desc.StartKey.Next(), ts},
		{r.Desc.EndKey.Prev(), ts},
	}

	keys := []proto.EncodedKey{}
	for _, keyTS := range keyTSs {
		if err := engine.MVCCPut(r.rm.Engine(), nil, keyTS.key, keyTS.ts, proto.Value{Bytes: []byte("value")}, nil); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, engine.MVCCEncodeKey(keyTS.key))
		if !keyTS.ts.Equal(ts0) {
			keys = append(keys, engine.MVCCEncodeVersionKey(keyTS.key, keyTS.ts))
		}
	}
	return keys
}

// TestRangeDataIterator verifies correct operation of iterator if
// a range contains no data and never has.
func TestRangeDataIteratorEmptyRange(t *testing.T) {
	tc := testContext{
		bootstrapMode: bootstrapRangeOnly,
	}
	tc.Start(t)
	defer tc.Stop()

	// Adjust the range descriptor to avoid existing data such as meta
	// records and config entries during the iteration. This is a rather
	// nasty little hack, but since it's test code, meh.
	tc.rng.Lock()
	tc.store.mu.Lock()
	newDesc := *tc.rng.Desc
	newDesc.StartKey = proto.Key("a")
	tc.rng.Desc = &newDesc
	tc.store.mu.Unlock()
	tc.rng.Unlock()

	iter := newRangeDataIterator(tc.rng, tc.rng.rm.Engine())
	defer iter.Close()
	for ; iter.Valid(); iter.Next() {
		t.Error("expected empty iteration")
	}
}

// TestRangeDataIterator creates three ranges {"a"-"b" (pre), "b"-"c"
// (main test range), "c"-"d" (post)} and fills each with data. It
// first verifies the contents of the "b"-"c" range, then deletes it
// and verifies it's empty. Finally, it verifies the pre and post
// ranges still contain the expected data.
func TestRangeDataIterator(t *testing.T) {
	tc := testContext{
		bootstrapMode: bootstrapRangeOnly,
	}
	tc.Start(t)
	defer tc.Stop()

	// See notes in EmptyRange test method for adjustment to descriptor.
	tc.rng.Lock()
	tc.store.mu.Lock()
	newDesc := *tc.rng.Desc
	newDesc.StartKey = proto.Key("b")
	newDesc.EndKey = proto.Key("c")
	tc.rng.Desc = &newDesc
	tc.store.mu.Unlock()
	tc.rng.Unlock()

	// Create two more ranges, one before the test range and one after.
	preRng := createRange(tc.store, 2, proto.Key("a"), proto.Key("b"))
	if err := tc.store.AddRange(preRng); err != nil {
		t.Fatal(err)
	}
	postRng := createRange(tc.store, 3, proto.Key("c"), proto.Key("d"))
	if err := tc.store.AddRange(postRng); err != nil {
		t.Fatal(err)
	}

	// Create range data for all three ranges.
	preKeys := createRangeData(preRng, t)
	keys := createRangeData(tc.rng, t)
	postKeys := createRangeData(postRng, t)

	iter := newRangeDataIterator(tc.rng, tc.rng.rm.Engine())
	defer iter.Close()
	for i := 0; iter.Valid(); iter.Next() {
		if err := iter.Error(); err != nil {
			t.Fatal(err)
		}
		if i >= len(keys) {
			t.Fatal("there are more keys in the iteration than expected")
		}
		if key := iter.Key(); !key.Equal(keys[i]) {
			k1, ts1, _ := engine.MVCCDecodeKey(key)
			k2, ts2, _ := engine.MVCCDecodeKey(keys[i])
			t.Errorf("%d: key mismatch %q(%d) != %q(%d)", i, k1, ts1, k2, ts2)
		}
		i++
	}

	// Destroy range and verify that its data has been completely cleared.
	if err := tc.rng.Destroy(); err != nil {
		t.Fatal(err)
	}
	iter = newRangeDataIterator(tc.rng, tc.rng.rm.Engine())
	defer iter.Close()
	if iter.Valid() {
		t.Errorf("expected empty iteration; got first key %q", iter.Key())
	}

	// Verify the keys in pre & post ranges.
	for _, test := range []struct {
		r    *Range
		keys []proto.EncodedKey
	}{
		{preRng, preKeys},
		{postRng, postKeys},
	} {
		iter = newRangeDataIterator(test.r, test.r.rm.Engine())
		defer iter.Close()
		for i := 0; iter.Valid(); iter.Next() {
			if key := iter.Key(); !key.Equal(test.keys[i]) {
				k1, ts1, _ := engine.MVCCDecodeKey(key)
				k2, ts2, _ := engine.MVCCDecodeKey(test.keys[i])
				t.Errorf("%d: key mismatch %q(%d) != %q(%d)", i, k1, ts1, k2, ts2)
			}
			i++
		}
	}
}
