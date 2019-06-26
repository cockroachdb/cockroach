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
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

type wrappedBatch struct {
	engine.Batch
	clearCount      int
	clearRangeCount int
}

func (wb *wrappedBatch) Clear(key engine.MVCCKey) error {
	wb.clearCount++
	return wb.Batch.Clear(key)
}

func (wb *wrappedBatch) ClearRange(start, end engine.MVCCKey) error {
	wb.clearRangeCount++
	return wb.Batch.ClearRange(start, end)
}

// TestCmdClearRangeBytesThreshold verifies that clear range resorts to
// clearing keys individually if under the bytes threshold and issues a
// clear range command to the batch otherwise.
func TestCmdClearRangeBytesThreshold(t *testing.T) {
	defer leaktest.AfterTest(t)()

	startKey := roachpb.Key("0000")
	endKey := roachpb.Key("9999")
	desc := roachpb.RangeDescriptor{
		RangeID:  99,
		StartKey: roachpb.RKey(startKey),
		EndKey:   roachpb.RKey(endKey),
	}
	valueStr := strings.Repeat("0123456789", 1024)
	var value roachpb.Value
	value.SetString(valueStr) // 10KiB
	halfFull := ClearRangeBytesThreshold / (2 * len(valueStr))
	overFull := ClearRangeBytesThreshold/len(valueStr) + 1
	tests := []struct {
		keyCount           int
		expClearCount      int
		expClearRangeCount int
	}{
		{
			keyCount:           1,
			expClearCount:      1,
			expClearRangeCount: 0,
		},
		// More than a single key, but not enough to use ClearRange.
		{
			keyCount:           halfFull,
			expClearCount:      halfFull,
			expClearRangeCount: 0,
		},
		// With key sizes requiring additional space, this will overshoot.
		{
			keyCount:           overFull,
			expClearCount:      0,
			expClearRangeCount: 1,
		},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			ctx := context.Background()
			eng := engine.NewInMem(roachpb.Attributes{}, 1<<20)
			defer eng.Close()

			var stats enginepb.MVCCStats
			for i := 0; i < test.keyCount; i++ {
				key := roachpb.Key(fmt.Sprintf("%04d", i))
				if err := engine.MVCCPut(ctx, eng, &stats, key, hlc.Timestamp{WallTime: int64(i % 2)}, value, nil); err != nil {
					t.Fatal(err)
				}
			}

			batch := &wrappedBatch{Batch: eng.NewBatch()}
			defer batch.Close()

			var h roachpb.Header
			h.RangeID = desc.RangeID

			cArgs := CommandArgs{Header: h}
			cArgs.EvalCtx = &mockEvalCtx{desc: &desc, clock: hlc.NewClock(hlc.UnixNano, time.Nanosecond), stats: stats}
			cArgs.Args = &roachpb.ClearRangeRequest{
				RequestHeader: roachpb.RequestHeader{
					Key:    startKey,
					EndKey: endKey,
				},
			}
			cArgs.Stats = &enginepb.MVCCStats{}

			if _, err := ClearRange(ctx, batch, cArgs, &roachpb.ClearRangeResponse{}); err != nil {
				t.Fatal(err)
			}

			// Verify cArgs.Stats is equal to the stats we wrote.
			newStats := stats
			newStats.SysBytes, newStats.SysCount = 0, 0       // ignore these values
			cArgs.Stats.SysBytes, cArgs.Stats.SysCount = 0, 0 // these too, as GC threshold is updated
			newStats.Add(*cArgs.Stats)
			newStats.AgeTo(0) // pin at LastUpdateNanos==0
			if !newStats.Equal(enginepb.MVCCStats{}) {
				t.Errorf("expected stats on original writes to be negated on clear range: %+v vs %+v", stats, *cArgs.Stats)
			}

			// Verify we see the correct counts for Clear and ClearRange.
			if a, e := batch.clearCount, test.expClearCount; a != e {
				t.Errorf("expected %d clears; got %d", e, a)
			}
			if a, e := batch.clearRangeCount, test.expClearRangeCount; a != e {
				t.Errorf("expected %d clear ranges; got %d", e, a)
			}

			// Now ensure that the data is gone, whether it was a ClearRange or individual calls to clear.
			if err := batch.Commit(true /* commit */); err != nil {
				t.Fatal(err)
			}
			if err := eng.Iterate(
				engine.MVCCKey{Key: startKey}, engine.MVCCKey{Key: endKey},
				func(kv engine.MVCCKeyValue) (bool, error) {
					return true, errors.New("expected no data in underlying engine")
				},
			); err != nil {
				t.Fatal(err)
			}
		})
	}
}

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

func TestCmdClearRangeTimeBound(t *testing.T) {
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

	tsBefore := baseTime.Add(100, 0)
	sumBefore := hashRange(t, eng, startKey, endKey)

	tsA := tsBefore.Add(100, 0)
	// Lay down some more keys that we'll revert later, with some of them
	// shadowing existing keys and some as new keys.
	for i := 5; i < keyCount+5; i++ {
		key := roachpb.Key(fmt.Sprintf("%04d", i))
		var value roachpb.Value
		value.SetString(fmt.Sprintf("%d-rev-a", i))
		if err := engine.MVCCPut(ctx, eng, &stats, key, tsA.Add(int64(i%5), 0), value, nil); err != nil {
			t.Fatal(err)
		}
	}
	sumA := hashRange(t, eng, startKey, endKey)

	tsB := tsA.Add(10, 0)
	// Lay down more keys, this time shadowing some of our earlier shadows too.
	for i := 7; i < keyCount+7; i++ {
		key := roachpb.Key(fmt.Sprintf("%04d", i))
		var value roachpb.Value
		value.SetString(fmt.Sprintf("%d-rev-b", i))
		if err := engine.MVCCPut(ctx, eng, &stats, key, tsB.Add(0, int32(i%5)), value, nil); err != nil {
			t.Fatal(err)
		}
	}
	sumB := hashRange(t, eng, startKey, endKey)

	desc := roachpb.RangeDescriptor{RangeID: 99,
		StartKey: roachpb.RKey(startKey),
		EndKey:   roachpb.RKey(endKey),
	}
	cArgs := CommandArgs{Header: roachpb.Header{RangeID: desc.RangeID}}
	cArgs.EvalCtx = &mockEvalCtx{desc: &desc, clock: hlc.NewClock(hlc.UnixNano, time.Nanosecond), stats: stats}
	afterStats := getStats(t, eng)
	for _, tc := range []struct {
		name     string
		ts       hlc.Timestamp
		expected []byte
	}{
		{"revert writes >= timeA (i.e. revert all revisions to initial)", tsA, sumBefore},
		{"revert writes >= timeB (i.e. revert to time A)", tsB, sumA},
		{"revert writes >= timeC (i.e. revert nothing)", tsB.Add(100, 0), sumB},
	} {
		t.Run(tc.name, func(t *testing.T) {
			batch := &wrappedBatch{Batch: eng.NewBatch()}
			defer batch.Close()

			cArgs.Stats = &enginepb.MVCCStats{}
			cArgs.Args = &roachpb.ClearRangeRequest{
				RequestHeader: roachpb.RequestHeader{Key: startKey, EndKey: endKey}, StartTime: tc.ts,
			}
			if _, err := ClearRange(ctx, batch, cArgs, &roachpb.ClearRangeResponse{}); err != nil {
				t.Fatal(err)
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

	tsBIntent := tsB.Add(5, 1)
	txn := roachpb.MakeTransaction("test", nil, roachpb.NormalUserPriority, tsBIntent, 1)
	if err := engine.MVCCPut(
		ctx, eng, nil, []byte("0012"), tsBIntent, roachpb.MakeValueFromBytes([]byte("i")), &txn,
	); err != nil {
		t.Fatal(err)
	}
	sumBWithIntent := hashRange(t, eng, startKey, endKey)

	tsC := tsB.Add(10, 0)
	// Lay down more revisions (skipping even keys to avoid out intent on 0012).
	for i := 7; i < keyCount+7; i += 2 {
		key := roachpb.Key(fmt.Sprintf("%04d", i))
		var value roachpb.Value
		value.SetString(fmt.Sprintf("%d-rev-b", i))
		if err := engine.MVCCPut(ctx, eng, &stats, key, tsC.Add(0, int32(i%5)), value, nil); err != nil {
			t.Fatalf("writing key %s: %+v", key, err)
		}
	}
	sumC := hashRange(t, eng, startKey, endKey)

	for _, tc := range []struct {
		name        string
		ts          hlc.Timestamp
		expectErr   bool
		expectedSum []byte
	}{
		{"hit intent", tsA, true, nil},
		{"hit intent exactly", tsBIntent, true, nil},
		{"clear above intent", tsC, false, sumBWithIntent},
		{"clear nothing above intent", tsC.Add(100, 0), false, sumC},
	} {
		t.Run(tc.name, func(t *testing.T) {
			batch := &wrappedBatch{Batch: eng.NewBatch()}
			defer batch.Close()
			cArgs.Stats = &enginepb.MVCCStats{}
			cArgs.Args = &roachpb.ClearRangeRequest{
				RequestHeader: roachpb.RequestHeader{Key: startKey, EndKey: endKey}, StartTime: tc.ts,
			}
			_, err := ClearRange(ctx, batch, cArgs, &roachpb.ClearRangeResponse{})
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
