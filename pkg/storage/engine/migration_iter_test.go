// Copyright 2018 The Cockroach Authors.
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

package engine

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

func scanForKeyValues(t *testing.T, it SimpleIterator, expKVs []roachpb.KeyValue) {
	t.Helper()

	it.Seek(MVCCKey{Key: keys.MinKey})
	for ; ; it.Next() {
		ok, err := it.Valid()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			if len(expKVs) > 0 {
				t.Fatalf("missing expected keys %v", expKVs)
			}
			return
		}
		if len(expKVs) == 0 {
			t.Fatalf("unexpected key %s", expKVs)
		}
		unsafeKey := it.UnsafeKey().Key
		exp := expKVs[0]
		expKVs = expKVs[1:]
		if !bytes.Equal(unsafeKey, exp.Key) {
			t.Fatalf("unexpected key %s, does not equal %s", unsafeKey, exp.Key)
		}
		unsafeVal := it.UnsafeValue()
		var meta enginepb.MVCCMetadata
		if err := protoutil.Unmarshal(unsafeVal, &meta); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(meta.RawBytes, exp.Value.RawBytes) {
			t.Fatalf("unexpected value for key %s: %b does not equal %b",
				exp.Key, meta.RawBytes, exp.Value.RawBytes)
		}
	}
}

func TestMigrationIteratorIgnoresReplicatedRaftTombstone(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	engine := createTestEngine()
	defer engine.Close()

	// Insert three range-local keys.
	const rangeID = 1
	lastGCKey := keys.MakeRangeIDPrefixBuf(rangeID).RangeLastGCKey()
	rTombKey := keys.MakeRangeIDPrefixBuf(rangeID).RaftTombstoneIncorrectLegacyKey() // should ignore
	uTombKey := keys.MakeRangeIDPrefixBuf(rangeID).RaftTombstoneKey()

	for _, key := range []roachpb.Key{lastGCKey, rTombKey, uTombKey} {
		if err := MVCCPut(ctx, engine, nil, key, hlc.Timestamp{}, value1, nil); err != nil {
			t.Fatal(err)
		}
	}

	// Iterate over keys. Without a MigrationIterator we should see the
	// replicated raft tombstone key. With a MigrationIterator, we should not.
	tests := []struct {
		withMigrationIter bool
		expKVs            []roachpb.KeyValue
	}{
		{
			withMigrationIter: false,
			expKVs: []roachpb.KeyValue{
				{Key: lastGCKey, Value: value1},
				{Key: rTombKey, Value: value1},
				{Key: uTombKey, Value: value1},
			},
		},
		{
			withMigrationIter: true,
			expKVs: []roachpb.KeyValue{
				{Key: lastGCKey, Value: value1},
				{Key: uTombKey, Value: value1},
			},
		},
	}
	for _, tc := range tests {
		name := fmt.Sprintf("migrationIter=%t", tc.withMigrationIter)
		t.Run(name, func(t *testing.T) {
			var it SimpleIterator = engine.NewIterator(false)
			if tc.withMigrationIter {
				it = NewMigrationIterator(it)
			}
			defer it.Close()

			scanForKeyValues(t, it, tc.expKVs)
		})
	}
}

func valForUInt(i uint64) roachpb.Value {
	var v roachpb.Value
	v.SetInt(int64(i))
	return v
}

func valForProto(key roachpb.Key, msg protoutil.Message) roachpb.Value {
	var v roachpb.Value
	if err := v.SetProto(msg); err != nil {
		panic(err)
	}
	v.InitChecksum(key)
	return v
}

func TestMigrationIteratorTranslatesRangeAppliedState(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	engine := createTestEngine()
	defer engine.Close()

	const rangeID = 1
	lastGCKey := keys.MakeRangeIDPrefixBuf(rangeID).RangeLastGCKey()
	applStateKey := keys.MakeRangeIDPrefixBuf(rangeID).RangeAppliedStateKey()
	raftApplKey := keys.MakeRangeIDPrefixBuf(rangeID).RaftAppliedIndexLegacyKey()
	leaseApplKey := keys.MakeRangeIDPrefixBuf(rangeID).LeaseAppliedIndexLegacyKey()
	statsKey := keys.MakeRangeIDPrefixBuf(rangeID).RangeStatsLegacyKey()
	utombKey := keys.MakeRangeIDPrefixBuf(rangeID).RaftTombstoneKey()

	// Values stored in legacy keys.
	raftAppliedIndexVal := valForUInt(25)
	leaseAppliedIndexVal := valForUInt(35)
	statsVal := valForProto(statsKey, &enginepb.MVCCStats{
		KeyBytes: 11,
		KeyCount: 12,
	})

	// Values stored in range applied state key. Overrides legacy keys, if present.
	appliedState := enginepb.RangeAppliedState{
		RaftAppliedIndex:  119,
		LeaseAppliedIndex: 120,
		RangeStats: enginepb.MVCCPersistentStats{
			KeyBytes: 111,
			KeyCount: 121,
		},
	}
	appliedStateStats := appliedState.RangeStats.ToStats()
	appliedStateVal := valForProto(applStateKey, &appliedState)

	// Insert all keys except the range applied state key.
	if err := MVCCPut(ctx, engine, nil, lastGCKey, hlc.Timestamp{}, value1, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(ctx, engine, nil, raftApplKey, hlc.Timestamp{}, raftAppliedIndexVal, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(ctx, engine, nil, leaseApplKey, hlc.Timestamp{}, leaseAppliedIndexVal, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(ctx, engine, nil, statsKey, hlc.Timestamp{}, statsVal, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(ctx, engine, nil, utombKey, hlc.Timestamp{}, value1, nil); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		withRangeAppliedStateKey bool
		withMigrationIter        bool
		expKVs                   []roachpb.KeyValue
	}{
		{
			withRangeAppliedStateKey: false,
			withMigrationIter:        false,
			expKVs: []roachpb.KeyValue{
				{Key: lastGCKey, Value: value1},
				{Key: raftApplKey, Value: raftAppliedIndexVal},
				{Key: leaseApplKey, Value: leaseAppliedIndexVal},
				{Key: statsKey, Value: statsVal},
				{Key: utombKey, Value: value1},
			},
		},
		{
			withRangeAppliedStateKey: false,
			withMigrationIter:        true,
			expKVs: []roachpb.KeyValue{
				{Key: lastGCKey, Value: value1},
				{Key: raftApplKey, Value: raftAppliedIndexVal},
				{Key: leaseApplKey, Value: leaseAppliedIndexVal},
				{Key: statsKey, Value: statsVal},
				{Key: utombKey, Value: value1},
			},
		},
		{
			withRangeAppliedStateKey: true,
			withMigrationIter:        false,
			expKVs: []roachpb.KeyValue{
				{Key: lastGCKey, Value: value1},
				{Key: applStateKey, Value: appliedStateVal},
				{Key: raftApplKey, Value: raftAppliedIndexVal},
				{Key: leaseApplKey, Value: leaseAppliedIndexVal},
				{Key: statsKey, Value: statsVal},
				{Key: utombKey, Value: value1},
			},
		},
		{
			withRangeAppliedStateKey: true,
			withMigrationIter:        true,
			expKVs: []roachpb.KeyValue{
				{Key: lastGCKey, Value: value1},
				{Key: raftApplKey, Value: valForUInt(appliedState.RaftAppliedIndex)},
				{Key: leaseApplKey, Value: valForUInt(appliedState.LeaseAppliedIndex)},
				{Key: statsKey, Value: valForProto(statsKey, &appliedStateStats)},
				{Key: utombKey, Value: value1},
			},
		},
	}
	for _, tc := range tests {
		name := fmt.Sprintf("applStateKey=%t,migrationIter=%t", tc.withRangeAppliedStateKey, tc.withMigrationIter)
		t.Run(name, func(t *testing.T) {
			if tc.withRangeAppliedStateKey {
				if err := MVCCPut(ctx, engine, nil, applStateKey, hlc.Timestamp{}, appliedStateVal, nil); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := MVCCDelete(ctx, engine, nil, applStateKey, hlc.Timestamp{}, nil); err != nil {
					t.Fatal(err)
				}
			}

			var it SimpleIterator = engine.NewIterator(false)
			if tc.withMigrationIter {
				it = NewMigrationIterator(it)
			}
			defer it.Close()

			scanForKeyValues(t, it, tc.expKVs)
		})
	}
}
