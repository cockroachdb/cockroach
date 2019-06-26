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
			// t.Logf("%v -> %s", kv.Key, kv.Value)
			h.Write(kv.Key.Key)
			h.Write(kv.Value)
			return false, nil
		},
	); err != nil {
		t.Fatal(err)
	}
	return h.Sum([]byte{})
}

func TestCmdClearRangeTimeBound(t *testing.T) {
	defer leaktest.AfterTest(t)()

	startKey := roachpb.Key("0000")
	endKey := roachpb.Key("9999")
	const keyCount = 10

	desc := roachpb.RangeDescriptor{
		RangeID:  99,
		StartKey: roachpb.RKey(startKey),
		EndKey:   roachpb.RKey(endKey),
	}

	ctx := context.Background()
	eng := engine.NewInMem(roachpb.Attributes{}, 1<<20)
	defer eng.Close()

	before := hlc.Timestamp{WallTime: 1000}

	// lay down some keys to be the starting point to which we'll return later.
	var stats enginepb.MVCCStats
	for i := 0; i < keyCount; i++ {
		key := roachpb.Key(fmt.Sprintf("%04d", i))
		var value roachpb.Value
		value.SetString(fmt.Sprintf("%d", i))

		if err := engine.MVCCPut(ctx, eng, &stats, key, before.Add(int64(i%10), 0), value, nil); err != nil {
			t.Fatal(err)
		}
	}

	t.Log("initial values")
	beforeSum := hashRange(t, eng, startKey, endKey)

	// cutoff time has to be after every existing key's time, which are all
	// in the range [before, before+10), so before+100 is fine.
	cutoff := before.Add(100, 0)

	// lay down some keys that we'll later revert, with some shadowing existing
	// keys and some as new keys.
	for i := 5; i < keyCount+5; i++ {
		key := roachpb.Key(fmt.Sprintf("%04d", i))
		var value roachpb.Value
		value.SetString(fmt.Sprintf("%d-rev-a", i))
		if err := engine.MVCCPut(ctx, eng, &stats, key, cutoff, value, nil); err != nil {
			t.Fatal(err)
		}
	}

	t.Log("rev a values")
	revASum := hashRange(t, eng, startKey, endKey)
	if bytes.Equal(revASum, beforeSum) {
		t.Fatal("expected (over)writing some keys to change checksum")
	}

	afterCutoff := cutoff.Add(0, 1)

	// ...and again, this time shadowing some of our shadows, plus some new keys.
	for i := 7; i < keyCount+7; i++ {
		key := roachpb.Key(fmt.Sprintf("%04d", i))
		var value roachpb.Value
		value.SetString(fmt.Sprintf("%d-rev-b", i))
		if err := engine.MVCCPut(ctx, eng, &stats, key, afterCutoff, value, nil); err != nil {
			t.Fatal(err)
		}
	}

	t.Log("rev b values")
	revBSum := hashRange(t, eng, startKey, endKey)
	if bytes.Equal(revBSum, revASum) {
		t.Fatal("expected (over)writing some keys to change checksum")
	}

	t.Run("revert to before", func(t *testing.T) {
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
			StartTime: cutoff,
		}
		cArgs.Stats = &enginepb.MVCCStats{}

		if _, err := ClearRange(ctx, batch, cArgs, &roachpb.ClearRangeResponse{}); err != nil {
			t.Fatal(err)
		}
		t.Log("reverted to a values")
		if reverted := hashRange(t, batch, startKey, endKey); !bytes.Equal(reverted, beforeSum) {
			t.Error("expected reverted keys to match checksum")
		}
	})

	t.Run("revert to a", func(t *testing.T) {
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
			StartTime: afterCutoff,
		}
		cArgs.Stats = &enginepb.MVCCStats{}

		if _, err := ClearRange(ctx, batch, cArgs, &roachpb.ClearRangeResponse{}); err != nil {
			t.Fatal(err)
		}
		t.Log("reverted to a values")
		if reverted := hashRange(t, batch, startKey, endKey); !bytes.Equal(reverted, revASum) {
			t.Error("expected reverted keys to match checksum")
		}
	})

	t.Run("revert nothing", func(t *testing.T) {
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
			StartTime: cutoff.Add(1, 0),
		}
		cArgs.Stats = &enginepb.MVCCStats{}

		if _, err := ClearRange(ctx, batch, cArgs, &roachpb.ClearRangeResponse{}); err != nil {
			t.Fatal(err)
		}
		t.Log("reverted no values")
		if reverted := hashRange(t, batch, startKey, endKey); !bytes.Equal(reverted, revBSum) {
			t.Error("expected reverted keys to match checksum")
		}
	})
}
