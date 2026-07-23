// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/lockspanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

type wrappedBatch struct {
	storage.Batch
	clearIterCount int
}

func (wb *wrappedBatch) ClearMVCCIteratorRange(
	start, end roachpb.Key, pointKeys, rangeKeys bool,
) error {
	wb.clearIterCount++
	return wb.Batch.ClearMVCCIteratorRange(start, end, pointKeys, rangeKeys)
}

// TestCmdClearRange verifies that ClearRange clears point and range keys in the
// given span, and that MVCC stats are updated correctly (both when clearing a
// complete range and just parts of it). It should clear keys using an iterator
// if under the bytes threshold, or using a Pebble range tombstone otherwise.
func TestCmdClearRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	nowNanos := int64(100e9)
	startKey := roachpb.Key("000") // NB: not 0000, different bound lengths for MVCC stats testing
	endKey := roachpb.Key("9999")
	valueStr := strings.Repeat("0123456789", 1024)
	var value roachpb.Value
	value.SetString(valueStr) // 10KiB

	halfFull := ClearRangeBytesThreshold / (2 * len(valueStr))
	overFull := ClearRangeBytesThreshold/len(valueStr) + 1
	testcases := map[string]struct {
		keyCount       int
		estimatedStats bool
		partialRange   bool
		expClearIter   bool
	}{
		"single key": {
			keyCount:     1,
			expClearIter: true,
		},
		"below threshold": {
			keyCount:     halfFull,
			expClearIter: true,
		},
		"below threshold partial range": {
			keyCount:     halfFull,
			partialRange: true,
			expClearIter: true,
		},
		"above threshold": {
			keyCount:     overFull,
			expClearIter: false,
		},
		"above threshold partial range": {
			keyCount:     overFull,
			partialRange: true,
			expClearIter: false,
		},
		"estimated stats": { // must not use iterator, since we can't trust stats
			keyCount:       1,
			estimatedStats: true,
			expClearIter:   false,
		},
		"estimated stats and partial range": { // stats get computed for partial ranges
			keyCount:       1,
			estimatedStats: true,
			partialRange:   true,
			expClearIter:   true,
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			testutils.RunTrueAndFalse(t, "spanningRangeTombstones", func(t *testing.T, spanningRangeTombstones bool) {
				ctx := context.Background()
				eng := storage.NewDefaultInMemForTesting()
				defer eng.Close()

				// Set up range descriptor. If partialRange is true, we make the range
				// wider than the cleared span, which disabled the MVCC stats fast path.
				desc := roachpb.RangeDescriptor{
					RangeID:  99,
					StartKey: roachpb.RKey(startKey),
					EndKey:   roachpb.RKey(endKey),
				}
				if tc.partialRange {
					desc.StartKey = roachpb.RKey(keys.LocalMax)
					desc.EndKey = roachpb.RKey(keys.MaxKey)
				}

				// Write some range tombstones at the bottom of the keyspace, some of
				// which straddle the clear span bounds. In particular, we need to
				// ensure MVCC stats are updated correctly for range tombstones that
				// get truncated by the ClearRange.
				//
				// If spanningRangeTombstone is true, we write very wide range
				// tombstones that engulf the entire cleared span. Otherwise, we write
				// additional range tombstones that span the start/end bounds as well as
				// some in the middle -- these will fragment the very wide range
				// tombstones, which is why we need to test both cases separately.
				rangeTombstones := []storage.MVCCRangeKey{
					{StartKey: roachpb.Key("0"), EndKey: roachpb.Key("a"), Timestamp: hlc.Timestamp{WallTime: 1e9}},
					{StartKey: roachpb.Key("0"), EndKey: roachpb.Key("a"), Timestamp: hlc.Timestamp{WallTime: 2e9}},
				}
				if !spanningRangeTombstones {
					rangeTombstones = append(rangeTombstones, []storage.MVCCRangeKey{
						{StartKey: roachpb.Key("00"), EndKey: roachpb.Key("111"), Timestamp: hlc.Timestamp{WallTime: 3e9}},
						{StartKey: roachpb.Key("2"), EndKey: roachpb.Key("4"), Timestamp: hlc.Timestamp{WallTime: 3e9}},
						{StartKey: roachpb.Key("6"), EndKey: roachpb.Key("8"), Timestamp: hlc.Timestamp{WallTime: 3e9}},
						{StartKey: roachpb.Key("999"), EndKey: roachpb.Key("aa"), Timestamp: hlc.Timestamp{WallTime: 3e9}},
					}...)
				}
				for _, rk := range rangeTombstones {
					localTS := hlc.ClockTimestamp{WallTime: rk.Timestamp.WallTime - 1e9} // give range key a value if > 0
					require.NoError(t, storage.MVCCDeleteRangeUsingTombstone(
						ctx, eng, nil, rk.StartKey, rk.EndKey, rk.Timestamp, localTS, nil, nil, false, 0, 0, nil))
				}

				// Write some random point keys within the cleared span, above the range tombstones.
				for i := 0; i < tc.keyCount; i++ {
					key := roachpb.Key(fmt.Sprintf("%04d", i))
					_, err := storage.MVCCPut(ctx, eng, key,
						hlc.Timestamp{WallTime: int64(4+i%2) * 1e9}, value, storage.MVCCWriteOptions{})
					require.NoError(t, err)
				}

				// Calculate the range stats.
				stats := computeStats(t, eng, desc.StartKey.AsRawKey(), desc.EndKey.AsRawKey(), nowNanos)
				if tc.estimatedStats {
					stats.ContainsEstimates++
				}

				// Set up the evaluation context.
				cArgs := CommandArgs{
					EvalCtx: (&MockEvalCtx{
						ClusterSettings: cluster.MakeTestingClusterSettings(),
						Desc:            &desc,
						Clock:           hlc.NewClockForTesting(nil),
						Stats:           stats,
					}).EvalContext(),
					Header: kvpb.Header{
						RangeID:   desc.RangeID,
						Timestamp: hlc.Timestamp{WallTime: nowNanos},
					},
					Args: &kvpb.ClearRangeRequest{
						RequestHeader: kvpb.RequestHeader{
							Key:    startKey,
							EndKey: endKey,
						},
					},
					Stats: &enginepb.MVCCStats{},
				}

				// Use a spanset batch to assert latching of all accesses. In
				// particular, to test the additional seeks necessary to peek for
				// adjacent range keys that we may truncate (for stats purposes) which
				// should not cross the range bounds.
				var latchSpans spanset.SpanSet
				var lockSpans lockspanset.LockSpanSet
				require.NoError(t,
					declareKeysClearRange(&desc, &cArgs.Header, cArgs.Args, &latchSpans, &lockSpans, 0),
				)
				batch := &wrappedBatch{Batch: spanset.NewBatchAt(eng.NewBatch(), &latchSpans, cArgs.Header.Timestamp)}
				defer batch.Close()

				// Run the request.
				result, err := ClearRange(ctx, batch, cArgs, &kvpb.ClearRangeResponse{})
				require.NoError(t, err)
				require.NotNil(t, result.Replicated.MVCCHistoryMutation)
				require.Equal(t, result.Replicated.MVCCHistoryMutation.Spans, []roachpb.Span{{Key: startKey, EndKey: endKey}})

				require.NoError(t, batch.Commit(true /* sync */))

				// Verify that we see the correct counts for ClearMVCCIteratorRange.
				require.Equal(t, tc.expClearIter, batch.clearIterCount == 1)

				// Ensure that the data is gone.
				iter, err := eng.NewMVCCIterator(context.Background(), storage.MVCCKeyAndIntentsIterKind, storage.IterOptions{
					KeyTypes:   storage.IterKeyTypePointsAndRanges,
					LowerBound: startKey,
					UpperBound: endKey,
				})
				if err != nil {
					t.Fatal(err)
				}
				defer iter.Close()
				iter.SeekGE(storage.MVCCKey{Key: keys.LocalMax})
				ok, err := iter.Valid()
				require.NoError(t, err)
				require.False(t, ok, "expected empty span, found key %s", iter.UnsafeKey())

				// Verify the stats delta by adding it to the original range stats and
				// comparing with the computed range stats. If we're clearing the entire
				// range then the new stats should be empty.
				newStats := stats
				newStats.ContainsEstimates, cArgs.Stats.ContainsEstimates = 0, 0
				newStats.SysBytes, cArgs.Stats.SysBytes = 0, 0
				newStats.SysCount, cArgs.Stats.SysCount = 0, 0
				newStats.AbortSpanBytes, cArgs.Stats.AbortSpanBytes = 0, 0
				newStats.Add(*cArgs.Stats)
				require.Equal(t, newStats, computeStats(t, eng, desc.StartKey.AsRawKey(), desc.EndKey.AsRawKey(), nowNanos))
				if !tc.partialRange {
					newStats.LastUpdateNanos = 0
					require.Empty(t, newStats)
				}
			})
		})
	}
}

func TestCmdClearRangeDeadline(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	var stats enginepb.MVCCStats
	startKey, endKey := roachpb.Key("0000"), roachpb.Key("9999")
	desc := roachpb.RangeDescriptor{
		RangeID: 99, StartKey: roachpb.RKey(startKey), EndKey: roachpb.RKey(endKey),
	}

	clock := hlc.NewClockForTesting(timeutil.NewManualTime(timeutil.Unix(0, 123)))

	args := kvpb.ClearRangeRequest{
		RequestHeader: kvpb.RequestHeader{Key: startKey, EndKey: endKey},
	}

	cArgs := CommandArgs{
		Header: kvpb.Header{RangeID: desc.RangeID},
		EvalCtx: (&MockEvalCtx{
			ClusterSettings: cluster.MakeTestingClusterSettings(),
			Desc:            &desc,
			Clock:           clock,
			Stats:           stats,
		}).EvalContext(),
		Stats: &enginepb.MVCCStats{},
		Args:  &args,
	}

	batch := eng.NewBatch()
	defer batch.Close()

	// no deadline
	args.Deadline = hlc.Timestamp{}
	if _, err := ClearRange(ctx, batch, cArgs, &kvpb.ClearRangeResponse{}); err != nil {
		t.Fatal(err)
	}

	// before deadline
	args.Deadline = hlc.Timestamp{WallTime: 124}
	if _, err := ClearRange(ctx, batch, cArgs, &kvpb.ClearRangeResponse{}); err != nil {
		t.Fatal(err)
	}

	// at deadline.
	args.Deadline = hlc.Timestamp{WallTime: 123}
	if _, err := ClearRange(ctx, batch, cArgs, &kvpb.ClearRangeResponse{}); err == nil {
		t.Fatal("expected deadline error")
	}

	// after deadline
	args.Deadline = hlc.Timestamp{WallTime: 122}
	if _, err := ClearRange(
		ctx, batch, cArgs, &kvpb.ClearRangeResponse{},
	); !testutils.IsError(err, "ClearRange has deadline") {
		t.Fatal("expected deadline error")
	}
}
