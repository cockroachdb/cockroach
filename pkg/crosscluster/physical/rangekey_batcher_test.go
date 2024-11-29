// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package physical

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestRangeKeyBatcherSplitSST(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()

	writeSST := func(keys []storage.MVCCRangeKey) []byte {
		data := &storage.MemObject{}
		w := storage.MakeIngestionSSTWriter(ctx, st, data)
		defer w.Close()
		for _, key := range keys {
			require.NoError(t, w.PutRawMVCCRangeKey(key, []byte{}))
		}
		require.NoError(t, w.Finish())
		return data.Data()
	}

	type testCase struct {
		name  string
		start roachpb.Key
		end   roachpb.Key
		split roachpb.Key
		keys  []storage.MVCCRangeKey
		full  string
		lhs   string
		rhs   string
		err   string
	}

	testCases := []testCase{
		{
			name:  "overlapping",
			start: roachpb.Key("a"),
			end:   roachpb.Key("f"),
			split: roachpb.Key("c"),
			keys: []storage.MVCCRangeKey{
				{
					StartKey:  roachpb.Key("a"),
					EndKey:    roachpb.Key("c"),
					Timestamp: hlc.Timestamp{WallTime: 1},
				},
				{
					StartKey:  roachpb.Key("b"),
					EndKey:    roachpb.Key("f"),
					Timestamp: hlc.Timestamp{WallTime: 2},
				},
				{
					StartKey:  roachpb.Key("c"),
					EndKey:    roachpb.Key("e"),
					Timestamp: hlc.Timestamp{WallTime: 1},
				},
				{
					StartKey:  roachpb.Key("d"),
					EndKey:    roachpb.Key("f"),
					Timestamp: hlc.Timestamp{WallTime: 1},
				},
			},
			full: `{a-b}[0.000000001,0=]
{b-f}[0.000000002,0= 0.000000001,0=]`,
			lhs: `{a-b}[0.000000001,0=]
{b-c}[0.000000002,0= 0.000000001,0=]`,
			rhs: `{c-f}[0.000000002,0= 0.000000001,0=]`,
		},
		{
			name:  "split-in-gap",
			start: roachpb.Key("a"),
			end:   roachpb.Key("g"),
			split: roachpb.Key("d"),
			keys: []storage.MVCCRangeKey{
				{
					StartKey:  roachpb.Key("a"),
					EndKey:    roachpb.Key("c"),
					Timestamp: hlc.Timestamp{WallTime: 1},
				},
				{
					StartKey:  roachpb.Key("b"),
					EndKey:    roachpb.Key("c"),
					Timestamp: hlc.Timestamp{WallTime: 2},
				},
				{
					StartKey:  roachpb.Key("e"),
					EndKey:    roachpb.Key("g"),
					Timestamp: hlc.Timestamp{WallTime: 1},
				},
				{
					StartKey:  roachpb.Key("f"),
					EndKey:    roachpb.Key("g"),
					Timestamp: hlc.Timestamp{WallTime: 1},
				},
			},
			full: `{a-b}[0.000000001,0=]
{b-c}[0.000000002,0= 0.000000001,0=]
{e-g}[0.000000001,0=]`,
			lhs: `{a-b}[0.000000001,0=]
{b-c}[0.000000002,0= 0.000000001,0=]`,
			rhs: `{e-g}[0.000000001,0=]`,
		},
		{
			name:  "split-on-last",
			start: roachpb.Key("a"),
			end:   roachpb.Key("g"),
			split: roachpb.Key("f"),
			keys: []storage.MVCCRangeKey{
				{
					StartKey:  roachpb.Key("a"),
					EndKey:    roachpb.Key("c"),
					Timestamp: hlc.Timestamp{WallTime: 1},
				},
				{
					StartKey:  roachpb.Key("e"),
					EndKey:    roachpb.Key("g"),
					Timestamp: hlc.Timestamp{WallTime: 1},
				},
			},
			full: `{a-c}[0.000000001,0=]
{e-g}[0.000000001,0=]`,
			lhs: `{a-c}[0.000000001,0=]
{e-f}[0.000000001,0=]`,
			rhs: `{f-g}[0.000000001,0=]`,
		},
		{
			name:  "empty-lhs-with-split-left-of-start",
			start: roachpb.Key("b"),
			end:   roachpb.Key("g"),
			split: roachpb.Key("a"),
			keys: []storage.MVCCRangeKey{
				{
					StartKey:  roachpb.Key("b"),
					EndKey:    roachpb.Key("c"),
					Timestamp: hlc.Timestamp{WallTime: 1},
				},
				{
					StartKey:  roachpb.Key("e"),
					EndKey:    roachpb.Key("g"),
					Timestamp: hlc.Timestamp{WallTime: 1},
				},
			},
			full: `{b-c}[0.000000001,0=]
{e-g}[0.000000001,0=]`,
			lhs: ``,
			rhs: `{b-c}[0.000000001,0=]
{e-g}[0.000000001,0=]`,
		},
		{
			name:  "empty-rhs-with-split-right-of-end",
			start: roachpb.Key("a"),
			end:   roachpb.Key("g"),
			split: roachpb.Key("h"),
			keys: []storage.MVCCRangeKey{
				{
					StartKey:  roachpb.Key("a"),
					EndKey:    roachpb.Key("c"),
					Timestamp: hlc.Timestamp{WallTime: 1},
				},
				{
					StartKey:  roachpb.Key("e"),
					EndKey:    roachpb.Key("g"),
					Timestamp: hlc.Timestamp{WallTime: 1},
				},
			},
			full: `{a-c}[0.000000001,0=]
{e-g}[0.000000001,0=]`,

			lhs: `{a-c}[0.000000001,0=]
{e-g}[0.000000001,0=]`,
			rhs: ``,
		},
		{
			name:  "empty-rhs-with-split at end",
			start: roachpb.Key("a"),
			end:   roachpb.Key("g"),
			split: roachpb.Key("g"),
			keys: []storage.MVCCRangeKey{
				{
					StartKey:  roachpb.Key("a"),
					EndKey:    roachpb.Key("c"),
					Timestamp: hlc.Timestamp{WallTime: 1},
				},
				{
					StartKey:  roachpb.Key("e"),
					EndKey:    roachpb.Key("g"),
					Timestamp: hlc.Timestamp{WallTime: 1},
				},
			},
			full: `{a-c}[0.000000001,0=]
{e-g}[0.000000001,0=]`,

			lhs: `{a-c}[0.000000001,0=]
{e-g}[0.000000001,0=]`,
			rhs: ``,
		},
		{
			name:  "error-on-bad-bounds-rhs-empty",
			start: roachpb.Key("a"),
			end:   roachpb.Key("e"),
			split: roachpb.Key("d"),
			keys: []storage.MVCCRangeKey{
				{
					StartKey:  roachpb.Key("a"),
					EndKey:    roachpb.Key("c"),
					Timestamp: hlc.Timestamp{WallTime: 1},
				},
			},
			full: `{a-c}[0.000000001,0=]`,
			err:  "not found in SST",
		},
		{
			name:  "error-on-bad-bounds-lhs-empty",
			start: roachpb.Key("a"),
			end:   roachpb.Key("d"),
			split: roachpb.Key("b"),
			keys: []storage.MVCCRangeKey{
				{
					StartKey:  roachpb.Key("c"),
					EndKey:    roachpb.Key("d"),
					Timestamp: hlc.Timestamp{WallTime: 1},
				},
			},
			full: `{c-d}[0.000000001,0=]`,
			err:  "invalid SST bounds",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data := writeSST(tc.keys)
			actualStart := rangeKeysInSSTToString(t, data)
			require.Equal(t, tc.full, actualStart)
			left, right, err := splitRangeKeySSTAtKey(ctx, st, tc.start, tc.end, tc.split, data)
			if tc.err != "" {
				require.ErrorContains(t, err, tc.err)
			} else {
				require.NoError(t, err)
			}

			if tc.lhs == "" {
				require.Nil(t, left)
			} else {
				require.True(t, len(left.start) > 0, "lhs start should be non-empty")
				require.True(t, len(left.end) > 0, "lhs end should be non-empty")
				require.Equal(t, strings.TrimSpace(tc.lhs), rangeKeysInSSTToString(t, left.data))
			}

			if tc.rhs == "" {
				require.Nil(t, right)
			} else {
				require.True(t, len(right.start) > 0, "rhs start should be non-empty")
				require.True(t, len(right.end) > 0, "rhs end should be non-empty")
				require.Equal(t, strings.TrimSpace(tc.rhs), rangeKeysInSSTToString(t, right.data))
			}
		})
	}
}

func rangeKeysInSSTToString(t *testing.T, data []byte) string {
	t.Helper()
	var buf strings.Builder
	iter, err := storage.NewMemSSTIterator(data, true, storage.IterOptions{
		KeyTypes:   storage.IterKeyTypeRangesOnly,
		LowerBound: roachpb.KeyMin,
		UpperBound: roachpb.KeyMax,
	})
	require.NoError(t, err)
	defer iter.Close()

	iter.SeekGE(storage.MVCCKey{Key: roachpb.KeyMin})
	for {
		if ok, err := iter.Valid(); err != nil {
			t.Fatal(err)
		} else if !ok {
			break
		}
		fmt.Fprintf(&buf, "%v\n", iter.RangeKeys())
		iter.Next()
	}
	return strings.TrimSpace(buf.String())
}
