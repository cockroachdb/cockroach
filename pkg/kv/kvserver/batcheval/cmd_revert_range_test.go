// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func hashRange(t *testing.T, reader storage.Reader, start, end roachpb.Key) []byte {
	t.Helper()
	h := sha256.New()
	require.NoError(t, reader.MVCCIterate(context.Background(), start, end,
		storage.MVCCKeyAndIntentsIterKind, storage.IterKeyTypePointsOnly,
		fs.UnknownReadCategory, func(kv storage.MVCCKeyValue, _ storage.MVCCRangeKeyStack) error {
			h.Write(kv.Key.Key)
			h.Write(kv.Value)
			return nil
		}))
	return h.Sum(nil)
}

func TestCmdRevertRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	startKey := roachpb.Key("0000")
	endKey := roachpb.Key("9999")
	const keyCount = 10

	ctx := context.Background()

	// Regression test for:
	// https://github.com/cockroachdb/cockroach/pull/42386
	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	baseTime := hlc.Timestamp{WallTime: 1000}
	tsReq := hlc.Timestamp{WallTime: 10000}

	// Lay down some keys to be the starting point to which we'll revert later.
	var stats enginepb.MVCCStats
	for i := 0; i < keyCount; i++ {
		key := roachpb.Key(fmt.Sprintf("%04d", i))
		var value roachpb.Value
		value.SetString(fmt.Sprintf("%d", i))
		if _, err := storage.MVCCPut(
			ctx, eng, key, baseTime.Add(int64(i%10), 0), value, storage.MVCCWriteOptions{Stats: &stats},
		); err != nil {
			t.Fatal(err)
		}
	}

	tsA := baseTime.Add(100, 0)
	sumA := hashRange(t, eng, startKey, endKey)

	// Lay down some more keys that we'll revert later, with some of them
	// shadowing existing keys and some as new keys.
	for i := 5; i < keyCount+5; i++ {
		key := roachpb.Key(fmt.Sprintf("%04d", i))
		var value roachpb.Value
		value.SetString(fmt.Sprintf("%d-rev-a", i))
		if _, err := storage.MVCCPut(
			ctx, eng, key, tsA.Add(int64(i%5), 1), value, storage.MVCCWriteOptions{Stats: &stats},
		); err != nil {
			t.Fatal(err)
		}
	}

	sumB := hashRange(t, eng, startKey, endKey)
	tsB := tsA.Add(10, 0)

	// Lay down more keys, this time shadowing some of our earlier shadows too.
	for i := 7; i < keyCount+7; i++ {
		key := roachpb.Key(fmt.Sprintf("%04d", i))
		var value roachpb.Value
		value.SetString(fmt.Sprintf("%d-rev-b", i))
		if _, err := storage.MVCCPut(
			ctx, eng, key, tsB.Add(1, int32(i%5)), value, storage.MVCCWriteOptions{Stats: &stats},
		); err != nil {
			t.Fatal(err)
		}
	}

	sumC := hashRange(t, eng, startKey, endKey)
	tsC := tsB.Add(10, 0)

	desc := roachpb.RangeDescriptor{RangeID: 99,
		StartKey: roachpb.RKey(startKey),
		EndKey:   roachpb.RKey(endKey),
	}
	cArgs := batcheval.CommandArgs{Header: kvpb.Header{RangeID: desc.RangeID, Timestamp: tsReq, MaxSpanRequestKeys: 2}}
	evalCtx := &batcheval.MockEvalCtx{
		ClusterSettings: cluster.MakeTestingClusterSettings(),
		Desc:            &desc,
		Clock:           hlc.NewClockForTesting(nil),
		Stats:           stats,
	}
	cArgs.EvalCtx = evalCtx.EvalContext()
	afterStats, err := storage.ComputeStats(ctx, eng, keys.LocalMax, keys.MaxKey, 0)
	require.NoError(t, err)
	for _, tc := range []struct {
		name     string
		ts       hlc.Timestamp
		expected []byte
		resumes  int
		empty    bool
	}{
		{"revert revert to time A", tsA, sumA, 4, false},
		{"revert revert to time B", tsB, sumB, 4, false},
		{"revert revert to time C (nothing)", tsC, sumC, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			batch := &wrappedBatch{Batch: eng.NewBatch()}
			defer batch.Close()

			req := kvpb.RevertRangeRequest{
				RequestHeader: kvpb.RequestHeader{Key: startKey, EndKey: endKey},
				TargetTime:    tc.ts,
			}
			cArgs.Stats = &enginepb.MVCCStats{}
			cArgs.Args = &req
			var resumes int
			for {
				var reply kvpb.RevertRangeResponse
				result, err := batcheval.RevertRange(ctx, batch, cArgs, &reply)
				if err != nil {
					t.Fatal(err)
				}
				// If there's nothing to revert and the fast-path is hit,
				// MVCCHistoryMutation will be empty.
				if !tc.empty {
					require.NotNil(t, result.Replicated.MVCCHistoryMutation)
					require.Equal(t, result.Replicated.MVCCHistoryMutation.Spans,
						[]roachpb.Span{{Key: req.RequestHeader.Key, EndKey: req.RequestHeader.EndKey}})
				}
				if reply.ResumeSpan == nil {
					break
				}
				resumes++
				req.RequestHeader.Key = reply.ResumeSpan.Key
			}
			if resumes != tc.resumes {
				// NB: since ClearTimeRange buffers keys until it hits one that is not
				// going to be cleared, and thus may exceed the max batch size by up to
				// the buffer size (64) when it flushes after breaking out of the loop,
				// expected resumes isn't *quite* a simple num_cleared_keys/batch_size.
				t.Fatalf("expected %d resumes, got %d", tc.resumes, resumes)
			}
			if reverted := hashRange(t, batch, startKey, endKey); !bytes.Equal(reverted, tc.expected) {
				t.Error("expected reverted keys to match checksum")
			}
			evalStats := afterStats
			evalStats.Add(*cArgs.Stats)
			realStats, err := storage.ComputeStats(ctx, batch, keys.LocalMax, keys.MaxKey, evalStats.LastUpdateNanos)
			require.NoError(t, err)
			require.Equal(t, realStats, evalStats)
		})
	}

	txn := roachpb.MakeTransaction("test", nil, isolation.Serializable, roachpb.NormalUserPriority, tsC, 1, 1, 0, false /* omitInRangefeeds */)
	if _, err := storage.MVCCPut(
		ctx, eng, []byte("0012"), tsC, roachpb.MakeValueFromBytes([]byte("i")), storage.MVCCWriteOptions{Txn: &txn, Stats: &stats},
	); err != nil {
		t.Fatal(err)
	}

	// Lay down more revisions (skipping even keys to avoid our intent on 0012).
	for i := 7; i < keyCount+7; i += 2 {
		key := roachpb.Key(fmt.Sprintf("%04d", i))
		var value roachpb.Value
		value.SetString(fmt.Sprintf("%d-rev-b", i))
		if _, err := storage.MVCCPut(
			ctx, eng, key, tsC.Add(10, int32(i%5)), value, storage.MVCCWriteOptions{Stats: &stats},
		); err != nil {
			t.Fatalf("writing key %s: %+v", key, err)
		}
	}
	tsD := tsC.Add(100, 0)
	sumD := hashRange(t, eng, startKey, endKey)

	// Re-set EvalCtx to pick up revised stats.
	cArgs.EvalCtx = (&batcheval.MockEvalCtx{
		ClusterSettings: cluster.MakeTestingClusterSettings(),
		Desc:            &desc,
		Clock:           hlc.NewClockForTesting(nil),
		Stats:           stats,
	}).EvalContext( /* maxOffset */ )
	for _, tc := range []struct {
		name        string
		ts          hlc.Timestamp
		expectErr   bool
		expectedSum []byte
		resumes     int
	}{
		{"hit intent", tsB, true, nil, 0},
		{"hit intent exactly", tsC, true, nil, 0},
		{"clear above intent", tsC.Add(0, 1), true, nil, 0},
		{"clear nothing above intent", tsD, false, sumD, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			batch := &wrappedBatch{Batch: eng.NewBatch()}
			defer batch.Close()
			cArgs.Stats = &enginepb.MVCCStats{}
			req := kvpb.RevertRangeRequest{
				RequestHeader: kvpb.RequestHeader{Key: startKey, EndKey: endKey},
				TargetTime:    tc.ts,
			}
			cArgs.Args = &req
			var resumes int
			var err error
			for {
				var reply kvpb.RevertRangeResponse
				var result result.Result
				result, err = batcheval.RevertRange(ctx, batch, cArgs, &reply)
				if err != nil || reply.ResumeSpan == nil {
					break
				}
				require.NotNil(t, result.Replicated.MVCCHistoryMutation)
				require.Equal(t, result.Replicated.MVCCHistoryMutation.Spans,
					[]roachpb.Span{{Key: req.RequestHeader.Key, EndKey: req.RequestHeader.EndKey}})
				req.RequestHeader.Key = reply.ResumeSpan.Key
				resumes++
			}
			if resumes != tc.resumes {
				t.Fatalf("expected %d resumes, got %d", tc.resumes, resumes)
			}

			if tc.expectErr {
				if !testutils.IsError(err, "conflicting locks") {
					t.Fatalf("expected lock conflict error; got: %T %+v", err, err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if reverted := hashRange(t, batch, startKey, endKey); !bytes.Equal(reverted, tc.expectedSum) {
					t.Error("expected reverted keys to match checksum")
				}
			}
		})
	}
}

// TestCmdRevertRangeMVCCRangeTombstones tests that RevertRange reverts MVCC
// range tombstones. This is just a rudimentary test of the plumbing,
// MVCCClearTimeRange is exhaustively tested in TestMVCCHistories.
func TestCmdRevertRangeMVCCRangeTombstones(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	storage.DisableMetamorphicSimpleValueEncoding(t)

	ctx := context.Background()

	testutils.RunTrueAndFalse(t, "entireRange", func(t *testing.T, entireRange bool) {

		// Set up an engine with MVCC range tombstones a-z at time 1, 2, and 3.
		eng := storage.NewDefaultInMemForTesting()
		defer eng.Close()

		batch := eng.NewBatch()
		defer batch.Close()

		require.NoError(t, eng.PutMVCCRangeKey(rangeKey("a", "z", 1e9), storage.MVCCValue{}))
		require.NoError(t, eng.PutMVCCRangeKey(rangeKey("a", "z", 2e9), storage.MVCCValue{}))
		require.NoError(t, eng.PutMVCCRangeKey(rangeKey("a", "z", 3e9), storage.MVCCValue{}))

		// Revert section c-f back to 1. If entireRange is true, then c-f is the
		// entire Raft range. Otherwise, the Raft range is a-z.
		startKey, endKey := roachpb.Key("c"), roachpb.Key("f")
		desc := roachpb.RangeDescriptor{
			RangeID:  1,
			StartKey: roachpb.RKey("a"),
			EndKey:   roachpb.RKey("z"),
		}
		if entireRange {
			desc.StartKey, desc.EndKey = roachpb.RKey(startKey), roachpb.RKey(endKey)
		}

		var ms enginepb.MVCCStats
		cArgs := batcheval.CommandArgs{
			EvalCtx: (&batcheval.MockEvalCtx{
				ClusterSettings: cluster.MakeTestingClusterSettings(),
				Desc:            &desc,
				Clock:           hlc.NewClockForTesting(nil),
				Stats:           ms,
			}).EvalContext(),
			Header: kvpb.Header{
				RangeID:   desc.RangeID,
				Timestamp: wallTS(10e9),
			},
			Args: &kvpb.RevertRangeRequest{
				RequestHeader: kvpb.RequestHeader{Key: startKey, EndKey: endKey},
				TargetTime:    wallTS(1e9),
			},
			Stats: &ms,
		}
		_, err := batcheval.RevertRange(ctx, batch, cArgs, &kvpb.RevertRangeResponse{})
		require.NoError(t, err)
		require.NoError(t, batch.Commit(false))

		// Scan the engine results.
		require.Equal(t, kvs{
			rangeKV("a", "c", 3e9, ""),
			rangeKV("a", "c", 2e9, ""),
			rangeKV("a", "c", 1e9, ""),
			rangeKV("c", "f", 1e9, ""),
			rangeKV("f", "z", 3e9, ""),
			rangeKV("f", "z", 2e9, ""),
			rangeKV("f", "z", 1e9, ""),
		}, scanEngine(t, eng))

		// Assert evaluated stats. When we're reverting the entire range, this will
		// be considered removal of 2 range key versions, but when we're reverting a
		// portion of the range it will instead fragment the range tombstones and
		// create 2 new ones.
		ms.AgeTo(10e9)
		ms.LastUpdateNanos = 0
		if entireRange {
			// Considered removal of 2 range key versions, because the new fragments
			// are in other ranges.
			require.Equal(t, enginepb.MVCCStats{
				RangeValCount: -2,
				RangeKeyBytes: -18,
				GCBytesAge:    -127,
			}, ms)
		} else {
			// Fragmentation creates 2 new range keys with 6 new versions, then
			// removes 2 versions from the middle fragment, so net 2 new keys with 4
			// new versions.
			require.Equal(t, enginepb.MVCCStats{
				RangeKeyCount: 2,
				RangeKeyBytes: 44,
				RangeValCount: 4,
				GCBytesAge:    361,
			}, ms)
		}
	})
}
