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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestMVCCRangeKeyIterator(t *testing.T) {
	defer leaktest.AfterTest(t)()

	eng := NewDefaultInMemForTesting()
	defer eng.Close()

	rangeKeys := []MVCCRangeKeyValue{
		rangeKV("b", "c", 3, "bc3"),
		rangeKV("e", "g", 3, "eg3"),
		rangeKV("d", "f", 5, "df5"),
		rangeKV("f", "g", 5, "fg5"),
		rangeKV("d", "f", 2, "df2"),
		rangeKV("a", "m", 4, "az4"), // same value as below so these should merge into one
		rangeKV("m", "z", 4, "az4"),
	}
	for _, rk := range rangeKeys {
		require.NoError(t, eng.ExperimentalPutMVCCRangeKey(rk.Key, rk.Value))
	}

	testcases := map[string]struct {
		opts   MVCCRangeKeyIterOptions
		expect []MVCCRangeKeyValue
	}{
		"all range keys": {
			MVCCRangeKeyIterOptions{},
			[]MVCCRangeKeyValue{
				rangeKV("b", "c", 3, "bc3"),
				rangeKV("d", "f", 5, "df5"),
				rangeKV("d", "f", 2, "df2"),
				rangeKV("f", "g", 5, "fg5"),
				rangeKV("e", "g", 3, "eg3"),
				rangeKV("a", "z", 4, "az4"),
			}},
		"truncated range keys": {
			MVCCRangeKeyIterOptions{
				LowerBound: roachpb.Key("c"),
				UpperBound: roachpb.Key("e"),
			},
			[]MVCCRangeKeyValue{
				rangeKV("d", "e", 5, "df5"),
				rangeKV("c", "e", 4, "az4"),
				rangeKV("d", "e", 2, "df2"),
			}},
		"truncation between range key bounds": {
			MVCCRangeKeyIterOptions{
				LowerBound: roachpb.Key("ccc"),
				UpperBound: roachpb.Key("eee"),
			},
			[]MVCCRangeKeyValue{
				rangeKV("d", "eee", 5, "df5"),
				rangeKV("ccc", "eee", 4, "az4"),
				rangeKV("e", "eee", 3, "eg3"),
				rangeKV("d", "eee", 2, "df2"),
			}},
		"fragmented range keys": {
			MVCCRangeKeyIterOptions{
				Fragmented: true,
			},
			[]MVCCRangeKeyValue{
				rangeKV("a", "b", 4, "az4"),
				rangeKV("b", "c", 4, "az4"),
				rangeKV("b", "c", 3, "bc3"),
				rangeKV("c", "d", 4, "az4"),
				rangeKV("d", "e", 5, "df5"),
				rangeKV("d", "e", 4, "az4"),
				rangeKV("d", "e", 2, "df2"),
				rangeKV("e", "f", 5, "df5"),
				rangeKV("e", "f", 4, "az4"),
				rangeKV("e", "f", 3, "eg3"),
				rangeKV("e", "f", 2, "df2"),
				rangeKV("f", "g", 5, "fg5"),
				rangeKV("f", "g", 4, "az4"),
				rangeKV("f", "g", 3, "eg3"),
				rangeKV("g", "m", 4, "az4"),
				rangeKV("m", "z", 4, "az4"),
			}},
		"empty interval": {
			MVCCRangeKeyIterOptions{
				LowerBound: roachpb.Key("A"),
				UpperBound: roachpb.Key("Z"),
			},
			nil},
		"zero-length interval": {
			MVCCRangeKeyIterOptions{
				LowerBound: roachpb.Key("c"),
				UpperBound: roachpb.Key("c"),
			},
			nil},
		"end after start": {
			MVCCRangeKeyIterOptions{
				LowerBound: roachpb.Key("e"),
				UpperBound: roachpb.Key("d"),
			},
			nil},
		"min timestamp": {
			MVCCRangeKeyIterOptions{
				MinTimestamp: hlc.Timestamp{Logical: 3},
			},
			[]MVCCRangeKeyValue{
				rangeKV("b", "c", 3, "bc3"),
				rangeKV("d", "f", 5, "df5"),
				rangeKV("f", "g", 5, "fg5"),
				rangeKV("e", "g", 3, "eg3"),
				rangeKV("a", "z", 4, "az4"),
			}},
		"max timestamp": {
			MVCCRangeKeyIterOptions{
				MaxTimestamp: hlc.Timestamp{Logical: 3},
			},
			[]MVCCRangeKeyValue{
				rangeKV("b", "c", 3, "bc3"),
				rangeKV("d", "f", 2, "df2"),
				rangeKV("e", "g", 3, "eg3"),
			}},
		"both timestamps": {
			MVCCRangeKeyIterOptions{
				MinTimestamp: hlc.Timestamp{Logical: 3},
				MaxTimestamp: hlc.Timestamp{Logical: 3},
			},
			[]MVCCRangeKeyValue{
				rangeKV("b", "c", 3, "bc3"),
				rangeKV("e", "g", 3, "eg3"),
			}},
	}
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			opts := tc.opts
			if opts.UpperBound == nil {
				opts.UpperBound = keys.MaxKey // appease pebbleIterator
			}
			iter := NewMVCCRangeKeyIterator(eng, opts)
			defer iter.Close()

			var rangeKVs []MVCCRangeKeyValue
			for {
				ok, err := iter.Valid()
				require.NoError(t, err)
				if !ok {
					break
				}
				rangeKVs = append(rangeKVs, MVCCRangeKeyValue{
					Key:   iter.Key(),
					Value: iter.Value(),
				})
				iter.Next()
			}
			require.Equal(t, tc.expect, rangeKVs)
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

func rangeKV(start, end string, ts int, value string) MVCCRangeKeyValue {
	return MVCCRangeKeyValue{
		Key:   rangeKey(start, end, ts),
		Value: []byte(value),
	}
}
