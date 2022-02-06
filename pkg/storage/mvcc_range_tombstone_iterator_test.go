// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestMVCCRangeTombstoneIterator(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	eng := NewDefaultInMemForTesting()
	defer eng.Close()

	rangeKeys := []MVCCRangeKey{
		rangeKey("b", "c", 3),
		rangeKey("e", "g", 3),
		rangeKey("d", "f", 5),
		rangeKey("d", "f", 2),
		rangeKey("a", "m", 4),
		rangeKey("m", "z", 4),
	}
	for _, rk := range rangeKeys {
		require.NoError(t, ExperimentalMVCCDeleteRangeUsingTombstone(
			ctx, eng, nil, rk.StartKey, rk.EndKey, rk.Timestamp, 0))
	}

	testcases := map[string]struct {
		opts   MVCCRangeTombstoneIterOptions
		expect []MVCCRangeKey
	}{
		"all tombstones": {
			MVCCRangeTombstoneIterOptions{},
			[]MVCCRangeKey{
				rangeKey("b", "c", 3),
				rangeKey("d", "f", 5),
				rangeKey("d", "f", 2),
				rangeKey("e", "g", 3),
				rangeKey("a", "z", 4),
			}},
		"truncated tombstones": {
			MVCCRangeTombstoneIterOptions{
				LowerBound: roachpb.Key("c"),
				UpperBound: roachpb.Key("e"),
			},
			[]MVCCRangeKey{
				rangeKey("d", "e", 5),
				rangeKey("c", "e", 4),
				rangeKey("d", "e", 2),
			}},
		"truncation between tombstone bounds": {
			MVCCRangeTombstoneIterOptions{
				LowerBound: roachpb.Key("ccc"),
				UpperBound: roachpb.Key("eee"),
			},
			[]MVCCRangeKey{
				rangeKey("d", "eee", 5),
				rangeKey("ccc", "eee", 4),
				rangeKey("e", "eee", 3),
				rangeKey("d", "eee", 2),
			}},
		"fragmented tombstones": {
			MVCCRangeTombstoneIterOptions{
				Fragmented: true,
			},
			[]MVCCRangeKey{
				rangeKey("a", "b", 4),
				rangeKey("b", "c", 4),
				rangeKey("b", "c", 3),
				rangeKey("c", "d", 4),
				rangeKey("d", "e", 5),
				rangeKey("d", "e", 4),
				rangeKey("d", "e", 2),
				rangeKey("e", "f", 5),
				rangeKey("e", "f", 4),
				rangeKey("e", "f", 3),
				rangeKey("e", "f", 2),
				rangeKey("f", "g", 4),
				rangeKey("f", "g", 3),
				rangeKey("g", "m", 4),
				rangeKey("m", "z", 4),
			}},
		"empty interval": {
			MVCCRangeTombstoneIterOptions{
				LowerBound: roachpb.Key("A"),
				UpperBound: roachpb.Key("Z"),
			},
			nil},
		"zero-length interval": {
			MVCCRangeTombstoneIterOptions{
				LowerBound: roachpb.Key("c"),
				UpperBound: roachpb.Key("c"),
			},
			nil},
		"end after start": {
			MVCCRangeTombstoneIterOptions{
				LowerBound: roachpb.Key("e"),
				UpperBound: roachpb.Key("d"),
			},
			nil},
		"min timestamp": {
			MVCCRangeTombstoneIterOptions{
				MinTimestamp: hlc.Timestamp{Logical: 3},
			},
			[]MVCCRangeKey{
				rangeKey("b", "c", 3),
				rangeKey("d", "f", 5),
				rangeKey("e", "g", 3),
				rangeKey("a", "z", 4),
			}},
		"max timestamp": {
			MVCCRangeTombstoneIterOptions{
				MaxTimestamp: hlc.Timestamp{Logical: 3},
			},
			[]MVCCRangeKey{
				rangeKey("b", "c", 3),
				rangeKey("d", "f", 2),
				rangeKey("e", "g", 3),
			}},
		"both timestamps": {
			MVCCRangeTombstoneIterOptions{
				MinTimestamp: hlc.Timestamp{Logical: 3},
				MaxTimestamp: hlc.Timestamp{Logical: 3},
			},
			[]MVCCRangeKey{
				rangeKey("b", "c", 3),
				rangeKey("e", "g", 3),
			}},
	}
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			var tombstones []MVCCRangeKey
			iter := NewMVCCRangeTombstoneIterator(eng, tc.opts)
			defer iter.Close()
			for {
				ok, err := iter.Valid()
				require.NoError(t, err)
				if !ok {
					break
				}
				tombstones = append(tombstones, iter.Key())
				iter.Next()
			}
			require.Equal(t, tc.expect, tombstones)
		})
	}
}

func rangeKey(start, end string, ts int) MVCCRangeKey {
	return MVCCRangeKey{
		StartKey:  roachpb.Key(start),
		EndKey:    roachpb.Key(end),
		Timestamp: hlc.Timestamp{Logical: int32(ts)},
	}
}
