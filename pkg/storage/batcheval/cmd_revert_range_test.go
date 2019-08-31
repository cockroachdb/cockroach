// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package batcheval

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func hashRange(t *testing.T, eng engine.Reader, start, end roachpb.Key) []byte {
	t.Helper()
	h := sha256.New()
	if err := eng.Iterate(
		engine.MVCCKey{Key: start}, engine.MVCCKey{Key: end},
		func(kv engine.MVCCKeyValue) (bool, error) {
			h.Write(kv.Key.Key)
			h.Write(kv.Value)
			return false, nil
		},
	); err != nil {
		t.Fatal(err)
	}
	return h.Sum(nil)
}

func getStats(t *testing.T, batch engine.Reader) enginepb.MVCCStats {
	t.Helper()
	iter := batch.NewIterator(engine.IterOptions{UpperBound: roachpb.KeyMax})
	defer iter.Close()
	s, err := engine.ComputeStatsGo(iter, engine.NilKey, engine.MVCCKeyMax, 1100)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	return s
}

func TestCmdRevertRange(t *testing.T) {
	defer leaktest.AfterTest(t)()

	startKey := roachpb.Key("0000")
	endKey := roachpb.Key("9999")
	const keyCount = 10

	ctx := context.Background()
	eng := engine.NewInMem(roachpb.Attributes{}, 1<<20)
	defer eng.Close()

	baseTime := hlc.Timestamp{WallTime: 1000}

	// Lay down some keys to be the starting point to which we'll revert later.
	var stats enginepb.MVCCStats
	for i := 0; i < keyCount; i++ {
		key := roachpb.Key(fmt.Sprintf("%04d", i))
		var value roachpb.Value
		value.SetString(fmt.Sprintf("%d", i))
		if err := engine.MVCCPut(ctx, eng, &stats, key, baseTime.Add(int64(i%10), 0), value, nil); err != nil {
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
		if err := engine.MVCCPut(ctx, eng, &stats, key, tsA.Add(int64(i%5), 1), value, nil); err != nil {
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
		if err := engine.MVCCPut(ctx, eng, &stats, key, tsB.Add(1, int32(i%5)), value, nil); err != nil {
			t.Fatal(err)
		}
	}

	sumC := hashRange(t, eng, startKey, endKey)
	tsC := tsB.Add(10, 0)

	desc := roachpb.RangeDescriptor{RangeID: 99,
		StartKey: roachpb.RKey(startKey),
		EndKey:   roachpb.RKey(endKey),
	}
	cArgs := CommandArgs{Header: roachpb.Header{RangeID: desc.RangeID, Timestamp: tsC}, MaxKeys: 2}
	evalCtx := &mockEvalCtx{desc: &desc, clock: hlc.NewClock(hlc.UnixNano, time.Nanosecond), stats: stats}
	cArgs.EvalCtx = evalCtx
	afterStats := getStats(t, eng)
	for _, tc := range []struct {
		name     string
		ts       hlc.Timestamp
		expected []byte
		resumes  int
	}{
		{"revert revert to time A", tsA, sumA, 4},
		{"revert revert to time B", tsB, sumB, 4},
		{"revert revert to time C (nothing)", tsC, sumC, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			batch := &wrappedBatch{Batch: eng.NewBatch()}
			defer batch.Close()

			req := roachpb.RevertRangeRequest{
				RequestHeader: roachpb.RequestHeader{Key: startKey, EndKey: endKey}, TargetTime: tc.ts,
			}
			cArgs.Stats = &enginepb.MVCCStats{}
			cArgs.Args = &req
			var resumes int
			for {
				var reply roachpb.RevertRangeResponse
				if _, err := RevertRange(ctx, batch, cArgs, &reply); err != nil {
					t.Fatal(err)
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
			if realStats := getStats(t, batch); !evalStats.Equal(evalStats) {
				t.Fatalf("stats mismatch:\npre-revert\t%+v\nevaled:\t%+v\neactual\t%+v", afterStats, evalStats, realStats)
			}
		})
	}

	t.Run("checks gc threshold", func(t *testing.T) {
		batch := &wrappedBatch{Batch: eng.NewBatch()}
		defer batch.Close()
		evalCtx.gcThreshold = tsB
		cArgs.Args = &roachpb.RevertRangeRequest{
			RequestHeader: roachpb.RequestHeader{Key: startKey, EndKey: endKey}, TargetTime: tsB,
		}
		if _, err := RevertRange(ctx, batch, cArgs, &roachpb.RevertRangeResponse{}); !testutils.IsError(err, "replica GC threshold") {
			t.Fatal(err)
		}
	})

	txn := roachpb.MakeTransaction("test", nil, roachpb.NormalUserPriority, tsC, 1)
	if err := engine.MVCCPut(
		ctx, eng, &stats, []byte("0012"), tsC, roachpb.MakeValueFromBytes([]byte("i")), &txn,
	); err != nil {
		t.Fatal(err)
	}
	sumCIntent := hashRange(t, eng, startKey, endKey)

	// Lay down more revisions (skipping even keys to avoid our intent on 0012).
	for i := 7; i < keyCount+7; i += 2 {
		key := roachpb.Key(fmt.Sprintf("%04d", i))
		var value roachpb.Value
		value.SetString(fmt.Sprintf("%d-rev-b", i))
		if err := engine.MVCCPut(ctx, eng, &stats, key, tsC.Add(10, int32(i%5)), value, nil); err != nil {
			t.Fatalf("writing key %s: %+v", key, err)
		}
	}
	tsD := tsC.Add(100, 0)
	sumD := hashRange(t, eng, startKey, endKey)

	cArgs.Header.Timestamp = tsD
	// Re-set EvalCtx to pick up revised stats.
	cArgs.EvalCtx = &mockEvalCtx{desc: &desc, clock: hlc.NewClock(hlc.UnixNano, time.Nanosecond), stats: stats}
	for _, tc := range []struct {
		name        string
		ts          hlc.Timestamp
		expectErr   bool
		expectedSum []byte
		resumes     int
	}{
		{"hit intent", tsB, true, nil, 2},
		{"hit intent exactly", tsC, false, sumCIntent, 2},
		{"clear above intent", tsC.Add(0, 1), false, sumCIntent, 2},
		{"clear nothing above intent", tsD, false, sumD, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			batch := &wrappedBatch{Batch: eng.NewBatch()}
			defer batch.Close()
			cArgs.Stats = &enginepb.MVCCStats{}
			req := roachpb.RevertRangeRequest{
				RequestHeader: roachpb.RequestHeader{Key: startKey, EndKey: endKey}, TargetTime: tc.ts,
			}
			cArgs.Args = &req
			var resumes int
			var err error
			for {
				var reply roachpb.RevertRangeResponse
				_, err = RevertRange(ctx, batch, cArgs, &reply)
				if err != nil || reply.ResumeSpan == nil {
					break
				}
				req.RequestHeader.Key = reply.ResumeSpan.Key
				resumes++
			}
			if resumes != tc.resumes {
				t.Fatalf("expected %d resumes, got %d", tc.resumes, resumes)
			}

			if tc.expectErr {
				if !testutils.IsError(err, "intents") {
					t.Fatalf("expected write intent error; got: %T %+v", err, err)
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
