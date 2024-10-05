// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulk

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/storageutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestSplitSST(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	settings := cluster.MakeTestingClusterSettings()
	pointKV := storageutils.PointKV
	first := pointKV("b", 7, "foo")
	middle := pointKV("c", 6, "baz")
	last := pointKV("d", 6, "bar")
	kvs := storageutils.KVs{first, middle, last}
	sstBytes, start, end := storageutils.MakeSST(t, settings, kvs)

	sp := func(start, end roachpb.Key, keyCount int) sstSpan {
		stats := enginepb.MVCCStats{KeyCount: int64(keyCount)}
		return sstSpan{start: start, end: end, stats: stats}
	}

	iterOpts := storage.IterOptions{
		KeyTypes:   storage.IterKeyTypePointsOnly,
		LowerBound: start,
		UpperBound: end.Next(),
	}

	type testCase struct {
		name          string
		splitPoint    roachpb.Key
		errorContains string
		expectedLeft  sstSpan
		expectedRight sstSpan
	}

	for _, tc := range []testCase{
		{
			name:          "first",
			splitPoint:    first.Key.Key,
			errorContains: "original sst must be greater than than split key",
		},
		{
			name:          "middle",
			splitPoint:    middle.Key.Key,
			expectedLeft:  sp(first.Key.Key, first.Key.Key.Next(), 1),
			expectedRight: sp(middle.Key.Key, last.Key.Key.Next(), 2),
		},
		{
			name:          "last",
			splitPoint:    last.Key.Key,
			expectedLeft:  sp(first.Key.Key, middle.Key.Key.Next(), 2),
			expectedRight: sp(last.Key.Key, last.Key.Key.Next(), 1),
		},
		{
			name:          "lastNext",
			splitPoint:    last.Key.Key.Next(),
			errorContains: "after last key",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			iter, err := storage.NewMemSSTIterator(sstBytes, true, iterOpts)
			require.NoError(t, err)
			defer iter.Close()

			iter.SeekGE(storage.MVCCKey{Key: start})
			sendStart := timeutil.Now()
			stats, err := storage.ComputeStatsForIter(iter, sendStart.UnixNano())
			require.NoError(t, err)
			isValid, err := iter.Valid()
			require.NoError(t, err)
			require.False(t, isValid)

			require.Equal(t, int64(3), stats.KeyCount)
			original := &sstSpan{sstBytes: sstBytes, stats: stats}

			left, right, err := createSplitSSTable(ctx, start, tc.splitPoint, iter, settings)
			if tc.errorContains != "" {
				require.ErrorContains(t, err, tc.errorContains)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expectedLeft.start, left.start)
			require.Equal(t, tc.expectedLeft.end, left.end)

			require.Equal(t, tc.expectedRight.start, right.start)
			require.Equal(t, tc.expectedRight.end, right.end)

			require.NoError(t, addStatsToSplitTables(left, right, original, sendStart))
			require.Equal(t, tc.expectedLeft.stats.KeyCount, left.stats.KeyCount)
			require.Equal(t, tc.expectedRight.stats.KeyCount, right.stats.KeyCount)

		})
	}
}
