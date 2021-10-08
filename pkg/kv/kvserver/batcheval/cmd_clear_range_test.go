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
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type wrappedBatch struct {
	storage.Batch
	clearIterCount  int
	clearRangeCount int
}

func (wb *wrappedBatch) ClearIterRange(iter storage.MVCCIterator, start, end roachpb.Key) error {
	wb.clearIterCount++
	return wb.Batch.ClearIterRange(iter, start, end)
}

func (wb *wrappedBatch) ClearMVCCRangeAndIntents(start, end roachpb.Key) error {
	wb.clearRangeCount++
	return wb.Batch.ClearMVCCRangeAndIntents(start, end)
}

// TestCmdClearRangeBytesThreshold verifies that clear range resorts to
// clearing keys individually if under the bytes threshold and issues a
// clear range command to the batch otherwise.
func TestCmdClearRangeBytesThreshold(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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
		expClearIterCount  int
		expClearRangeCount int
	}{
		{
			keyCount:           1,
			expClearIterCount:  1,
			expClearRangeCount: 0,
		},
		// More than a single key, but not enough to use ClearRange.
		{
			keyCount:           halfFull,
			expClearIterCount:  1,
			expClearRangeCount: 0,
		},
		// With key sizes requiring additional space, this will overshoot.
		{
			keyCount:           overFull,
			expClearIterCount:  0,
			expClearRangeCount: 1,
		},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			ctx := context.Background()
			eng := storage.NewDefaultInMemForTesting()
			defer eng.Close()

			var stats enginepb.MVCCStats
			for i := 0; i < test.keyCount; i++ {
				key := roachpb.Key(fmt.Sprintf("%04d", i))
				if err := storage.MVCCPut(ctx, eng, &stats, key, hlc.Timestamp{WallTime: int64(i % 2)}, value, nil); err != nil {
					t.Fatal(err)
				}
			}

			batch := &wrappedBatch{Batch: eng.NewBatch()}
			defer batch.Close()

			var h roachpb.Header
			h.RangeID = desc.RangeID

			cArgs := CommandArgs{Header: h}
			cArgs.EvalCtx = (&MockEvalCtx{Desc: &desc, Clock: hlc.NewClock(hlc.UnixNano, time.Nanosecond), Stats: stats}).EvalContext()
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
			newStats.SysBytes, newStats.SysCount, newStats.AbortSpanBytes = 0, 0, 0          // ignore these values
			cArgs.Stats.SysBytes, cArgs.Stats.SysCount, cArgs.Stats.AbortSpanBytes = 0, 0, 0 // these too, as GC threshold is updated
			newStats.Add(*cArgs.Stats)
			newStats.AgeTo(0) // pin at LastUpdateNanos==0
			if !newStats.Equal(enginepb.MVCCStats{}) {
				t.Errorf("expected stats on original writes to be negated on clear range: %+v vs %+v", stats, *cArgs.Stats)
			}

			// Verify we see the correct counts for Clear and ClearRange.
			if a, e := batch.clearIterCount, test.expClearIterCount; a != e {
				t.Errorf("expected %d iter range clears; got %d", e, a)
			}
			if a, e := batch.clearRangeCount, test.expClearRangeCount; a != e {
				t.Errorf("expected %d clear ranges; got %d", e, a)
			}

			// Now ensure that the data is gone, whether it was a ClearRange or individual calls to clear.
			if err := batch.Commit(true /* commit */); err != nil {
				t.Fatal(err)
			}
			if err := eng.MVCCIterate(startKey, endKey, storage.MVCCKeyAndIntentsIterKind, func(kv storage.MVCCKeyValue) error {
				return errors.New("expected no data in underlying engine")
			}); err != nil {
				t.Fatal(err)
			}
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

	manual := hlc.NewManualClock(123)
	clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)

	args := roachpb.ClearRangeRequest{
		RequestHeader: roachpb.RequestHeader{Key: startKey, EndKey: endKey},
	}

	cArgs := CommandArgs{
		Header:  roachpb.Header{RangeID: desc.RangeID},
		EvalCtx: (&MockEvalCtx{Desc: &desc, Clock: clock, Stats: stats}).EvalContext(),
		Stats:   &enginepb.MVCCStats{},
		Args:    &args,
	}

	batch := eng.NewBatch()
	defer batch.Close()

	// no deadline
	args.Deadline = hlc.Timestamp{}
	if _, err := ClearRange(ctx, batch, cArgs, &roachpb.ClearRangeResponse{}); err != nil {
		t.Fatal(err)
	}

	// before deadline
	args.Deadline = hlc.Timestamp{WallTime: 124}
	if _, err := ClearRange(ctx, batch, cArgs, &roachpb.ClearRangeResponse{}); err != nil {
		t.Fatal(err)
	}

	// at deadline.
	args.Deadline = hlc.Timestamp{WallTime: 123}
	if _, err := ClearRange(ctx, batch, cArgs, &roachpb.ClearRangeResponse{}); err == nil {
		t.Fatal("expected deadline error")
	}

	// after deadline
	args.Deadline = hlc.Timestamp{WallTime: 122}
	if _, err := ClearRange(
		ctx, batch, cArgs, &roachpb.ClearRangeResponse{},
	); !testutils.IsError(err, "ClearRange has deadline") {
		t.Fatal("expected deadline error")
	}
}
