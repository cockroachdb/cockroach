// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/lockspanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestDeleteRangeTombstone tests DeleteRange range tombstones and predicated based DeleteRange
// directly, using only a Pebble engine.
//
// MVCC range tombstone logic is tested exhaustively in the MVCC history tests,
// this just tests the RPC plumbing.
func TestDeleteRangeTombstone(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	storage.DisableMetamorphicSimpleValueEncoding(t)

	// Initial data for each test. x is point tombstone, [] is intent,
	// o---o is range tombstone.
	//
	// 5                                 [i5]
	// 4          c4
	// 3              x
	// 2      b2      d2      o-------o
	// 1
	//    a   b   c   d   e   f   g   h   i
	//
	// We also write two range tombstones abutting the Raft range a-z at [Z-a)@100
	// and [z-|)@100. Writing a range tombstone should not merge with these.
	writeInitialData := func(t *testing.T, ctx context.Context, rw storage.ReadWriter) {
		t.Helper()
		var localTS hlc.ClockTimestamp

		txn := roachpb.MakeTransaction("test", nil /* baseKey */, isolation.Serializable, roachpb.NormalUserPriority, hlc.Timestamp{WallTime: 5e9}, 0, 0, 0, false /* omitInRangefeeds */)
		_, err := storage.MVCCPut(ctx, rw, roachpb.Key("b"), hlc.Timestamp{WallTime: 2e9}, roachpb.MakeValueFromString("b2"), storage.MVCCWriteOptions{})
		require.NoError(t, err)
		_, err = storage.MVCCPut(ctx, rw, roachpb.Key("c"), hlc.Timestamp{WallTime: 4e9}, roachpb.MakeValueFromString("c4"), storage.MVCCWriteOptions{})
		require.NoError(t, err)
		_, err = storage.MVCCPut(ctx, rw, roachpb.Key("d"), hlc.Timestamp{WallTime: 2e9}, roachpb.MakeValueFromString("d2"), storage.MVCCWriteOptions{})
		require.NoError(t, err)
		_, _, err = storage.MVCCDelete(ctx, rw, roachpb.Key("d"), hlc.Timestamp{WallTime: 3e9}, storage.MVCCWriteOptions{})
		require.NoError(t, err)
		_, err = storage.MVCCPut(ctx, rw, roachpb.Key("i"), hlc.Timestamp{WallTime: 5e9}, roachpb.MakeValueFromString("i5"), storage.MVCCWriteOptions{Txn: &txn})
		require.NoError(t, err)
		require.NoError(t, storage.MVCCDeleteRangeUsingTombstone(ctx, rw, nil, roachpb.Key("f"), roachpb.Key("h"), hlc.Timestamp{WallTime: 3e9}, localTS, nil, nil, false, 0, 0, nil))
		require.NoError(t, storage.MVCCDeleteRangeUsingTombstone(ctx, rw, nil, roachpb.Key("Z"), roachpb.Key("a"), hlc.Timestamp{WallTime: 100e9}, localTS, nil, nil, false, 0, 0, nil))
		require.NoError(t, storage.MVCCDeleteRangeUsingTombstone(ctx, rw, nil, roachpb.Key("z"), roachpb.Key("|"), hlc.Timestamp{WallTime: 100e9}, localTS, nil, nil, false, 0, 0, nil))
	}

	now := hlc.ClockTimestamp{Logical: 9}
	rangeStart, rangeEnd := roachpb.Key("a"), roachpb.Key("z")

	testcases := map[string]struct {
		start         string
		end           string
		ts            int64
		txn           bool
		inline        bool
		returnKeys    bool
		idempotent    bool
		expectNoWrite bool
		expectErr     interface{} // error type, substring, or true (any)

		// The fields below test predicate based delete range rpc plumbing.
		predicateStartTime int64 // if set, the test will only run with predicate based delete range
		onlyPointKeys      bool  // if set UsingRangeTombstone arg is set to false
		maxBatchSize       int64 // if predicateStartTime is set, then MaxBatchSize must be set
	}{
		"above points succeed": {
			start: "a",
			end:   "f",
			ts:    10e9,
		},
		"above range tombstone succeed": {
			start:     "f",
			end:       "h",
			ts:        10e9,
			expectErr: nil,
		},
		"idempotent above range tombstone does not write": {
			start:         "f",
			end:           "h",
			ts:            10e9,
			idempotent:    true,
			expectErr:     nil,
			expectNoWrite: true,
		},
		"merging succeeds": {
			start: "e",
			end:   "f",
			ts:    3e9,
		},
		"adjacent to external LHS range key": {
			start: "a",
			end:   "f",
			ts:    100e9,
		},
		"adjacent to external RHS range key": {
			start: "q",
			end:   "z",
			ts:    100e9,
		},
		"transaction errors": {
			start:     "a",
			end:       "f",
			ts:        10e9,
			txn:       true,
			expectErr: ErrTransactionUnsupported,
		},
		"inline errors": {
			start:     "a",
			end:       "f",
			ts:        10e9,
			inline:    true,
			expectErr: "Inline can't be used with range tombstones",
		},
		"returnKeys errors": {
			start:      "a",
			end:        "f",
			ts:         10e9,
			returnKeys: true,
			expectErr:  "ReturnKeys can't be used with range tombstones",
		},
		"intent errors with LockConflictError": {
			start:     "i",
			end:       "j",
			ts:        10e9,
			expectErr: &kvpb.LockConflictError{},
		},
		"below point errors with WriteTooOldError": {
			start:     "a",
			end:       "d",
			ts:        1e9,
			expectErr: &kvpb.WriteTooOldError{},
		},
		"below range tombstone errors with WriteTooOldError": {
			start:     "f",
			end:       "h",
			ts:        1e9,
			expectErr: &kvpb.WriteTooOldError{},
		},
		"predicate without UsingRangeTombstone error": {
			start:              "a",
			end:                "f",
			ts:                 10e9,
			predicateStartTime: 1,
			maxBatchSize:       maxDeleteRangeBatchBytes,
			onlyPointKeys:      true,
			expectErr:          "UseRangeTombstones must be passed with predicate based Delete Range",
		},
		"predicate maxBatchSize error": {
			start:              "a",
			end:                "f",
			ts:                 10e9,
			predicateStartTime: 1,
			maxBatchSize:       0,
			expectErr:          "MaxSpanRequestKeys must be greater than zero when using predicated based DeleteRange",
		},
	}
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			for _, runWithPredicates := range []bool{false, true} {
				if tc.predicateStartTime > 0 && !runWithPredicates {
					continue
				}
				if runWithPredicates && tc.idempotent {
					continue
				}
				t.Run(fmt.Sprintf("Predicates=%v", runWithPredicates), func(t *testing.T) {
					ctx := context.Background()
					st := cluster.MakeTestingClusterSettings()
					engine := storage.NewDefaultInMemForTesting()
					defer engine.Close()

					writeInitialData(t, ctx, engine)

					ts := hlc.Timestamp{WallTime: tc.ts}
					rangeKey := storage.MVCCRangeKey{
						StartKey:               roachpb.Key(tc.start),
						EndKey:                 roachpb.Key(tc.end),
						Timestamp:              ts,
						EncodedTimestampSuffix: storage.EncodeMVCCTimestampSuffix(ts),
					}

					// Prepare the request and environment.
					evalCtx := &MockEvalCtx{
						ClusterSettings: st,
						Desc: &roachpb.RangeDescriptor{
							StartKey: roachpb.RKey(rangeStart),
							EndKey:   roachpb.RKey(rangeEnd),
						},
					}

					h := kvpb.Header{
						Timestamp: rangeKey.Timestamp,
					}
					if tc.txn {
						txn := roachpb.MakeTransaction("txn", nil, isolation.Serializable, roachpb.NormalUserPriority, rangeKey.Timestamp, 0, 0, 0, false /* omitInRangefeeds */)
						h.Txn = &txn
					}
					var predicates kvpb.DeleteRangePredicates
					if runWithPredicates {
						predicates = kvpb.DeleteRangePredicates{
							StartTime: hlc.Timestamp{WallTime: 1},
						}
						h.MaxSpanRequestKeys = math.MaxInt64
					}
					if tc.predicateStartTime > 0 {
						predicates = kvpb.DeleteRangePredicates{
							StartTime: hlc.Timestamp{WallTime: tc.predicateStartTime},
						}
						h.MaxSpanRequestKeys = tc.maxBatchSize
					}

					req := &kvpb.DeleteRangeRequest{
						RequestHeader: kvpb.RequestHeader{
							Key:    rangeKey.StartKey,
							EndKey: rangeKey.EndKey,
						},
						UseRangeTombstone:   !tc.onlyPointKeys,
						IdempotentTombstone: tc.idempotent,
						Inline:              tc.inline,
						ReturnKeys:          tc.returnKeys,
						Predicates:          predicates,
					}

					ms := computeStats(t, engine, rangeStart, rangeEnd, rangeKey.Timestamp.WallTime)

					// Use a spanset batch to assert latching of all accesses. In particular,
					// the additional seeks necessary to check for adjacent range keys that we
					// may merge with (for stats purposes) which should not cross the range
					// bounds.
					var latchSpans spanset.SpanSet
					var lockSpans lockspanset.LockSpanSet
					require.NoError(t,
						declareKeysDeleteRange(evalCtx.Desc, &h, req, &latchSpans, &lockSpans, 0),
					)
					batch := spanset.NewBatchAt(engine.NewBatch(), &latchSpans, h.Timestamp)
					defer batch.Close()

					// Run the request.
					resp := &kvpb.DeleteRangeResponse{}
					_, err := DeleteRange(ctx, batch, CommandArgs{
						EvalCtx: evalCtx.EvalContext(),
						Stats:   &ms,
						Now:     now,
						Header:  h,
						Args:    req,
					}, resp)

					// Check the error.
					if tc.expectErr != nil {
						require.Error(t, err)
						if b, ok := tc.expectErr.(bool); ok && b {
							// any error is fine
						} else if expectMsg, ok := tc.expectErr.(string); ok {
							require.Contains(t, err.Error(), expectMsg)
						} else if e, ok := tc.expectErr.(error); ok {
							require.True(t, errors.HasType(err, e), "expected %T, got %v", e, err)
						} else {
							require.Fail(t, "invalid expectErr", "expectErr=%v", tc.expectErr)
						}
						return
					}
					require.NoError(t, err)
					require.NoError(t, batch.Commit(true))

					if runWithPredicates {
						checkPredicateDeleteRange(t, engine, rangeKey)
					} else {
						checkDeleteRangeTombstone(t, engine, rangeKey, !tc.expectNoWrite, now)
					}

					// Check that range tombstone stats were updated correctly.
					require.Equal(t, computeStats(t, engine, rangeStart, rangeEnd, rangeKey.Timestamp.WallTime), ms)
				})
			}
		})
	}
}

// checkDeleteRangeTombstone checks that the span targeted by the predicate
// based delete range operation only has point tombstones, as the size of the
// spans in this test are below rangeTombstoneThreshold
//
// the passed in rangekey contains info on the span PredicateDeleteRange
// operated on. The command should not have written an actual rangekey!
func checkPredicateDeleteRange(t *testing.T, engine storage.Reader, rKeyInfo storage.MVCCRangeKey) {

	iter, err := engine.NewMVCCIterator(context.Background(), storage.MVCCKeyAndIntentsIterKind, storage.IterOptions{
		KeyTypes:   storage.IterKeyTypePointsAndRanges,
		LowerBound: rKeyInfo.StartKey,
		UpperBound: rKeyInfo.EndKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()

	for iter.SeekGE(storage.MVCCKey{Key: rKeyInfo.StartKey}); ; iter.NextKey() {
		ok, err := iter.Valid()
		require.NoError(t, err)
		if !ok {
			break
		}
		hasPoint, hashRange := iter.HasPointAndRange()
		if !hasPoint && hashRange {
			// PredicateDeleteRange should not have written any delete tombstones;
			// therefore, any range key tombstones in the span should have been
			// written before the request was issued.
			for _, v := range iter.RangeKeys().Versions {
				require.True(t, v.Timestamp.Less(rKeyInfo.Timestamp))
			}
			continue
		}
		value, err := storage.DecodeMVCCValueAndErr(iter.UnsafeValue())
		require.NoError(t, err)
		require.True(t, value.IsTombstone())
	}
}

// checkDeleteRangeTombstone checks that the range tombstone was written successfully.
func checkDeleteRangeTombstone(
	t *testing.T,
	engine storage.Reader,
	rangeKey storage.MVCCRangeKey,
	written bool,
	now hlc.ClockTimestamp,
) {
	iter, err := engine.NewMVCCIterator(context.Background(), storage.MVCCKeyAndIntentsIterKind, storage.IterOptions{
		KeyTypes:   storage.IterKeyTypeRangesOnly,
		LowerBound: rangeKey.StartKey,
		UpperBound: rangeKey.EndKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()
	iter.SeekGE(storage.MVCCKey{Key: rangeKey.StartKey})

	var seen storage.MVCCRangeKeyValue
	for {
		ok, err := iter.Valid()
		require.NoError(t, err)
		if !ok {
			break
		}
		require.True(t, ok)
		rangeKeys := iter.RangeKeys()
		for _, v := range rangeKeys.Versions {
			if v.Timestamp.Equal(rangeKey.Timestamp) {
				if len(seen.RangeKey.StartKey) == 0 {
					seen = rangeKeys.AsRangeKeyValue(v).Clone()
				} else {
					seen.RangeKey.EndKey = rangeKeys.Bounds.EndKey.Clone()
					require.Equal(t, seen.Value, v.Value)
				}
				break
			}
		}
		iter.Next()
	}
	if written {
		require.Equal(t, rangeKey, seen.RangeKey)
		value, err := storage.DecodeMVCCValue(seen.Value)
		require.NoError(t, err)
		require.True(t, value.IsTombstone())
		require.Equal(t, now, value.LocalTimestamp)
	} else {
		require.Empty(t, seen)
	}
}

// computeStats computes MVCC stats for the given range.
//
// TODO(erikgrinaker): This, storage.computeStats(), and engineStats() should be
// moved into a testutils package, somehow avoiding import cycles with storage
// tests.
func computeStats(
	t *testing.T, reader storage.Reader, from, to roachpb.Key, nowNanos int64,
) enginepb.MVCCStats {
	t.Helper()

	if len(from) == 0 {
		from = keys.LocalMax
	}
	if len(to) == 0 {
		to = keys.MaxKey
	}
	ms, err := storage.ComputeStats(context.Background(), reader, from, to, nowNanos)
	require.NoError(t, err)
	return ms
}
