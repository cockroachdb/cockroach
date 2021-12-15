// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scjob

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/stretchr/testify/require"
)

// TestBackfillTracker exercises the logic of the backfillTracker
// by using an in-memory implementation of the underlying store and
// the set of ranges.
//
// TODO(ajwerner): It's probable that datadriven test would be easier
// to read.
func TestBackfillTracker(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mkSpans := func(keys ...string) (ret []roachpb.Span) {
		for i := 1; i < len(keys); i++ {
			ret = append(ret, roachpb.Span{
				Key:    []byte(keys[i-1]),
				EndKey: []byte(keys[i]),
			})
		}
		return ret
	}
	mkBackfill := func(
		tableID descpb.ID,
		sourceIndexID descpb.IndexID,
		destIndexIDs ...descpb.IndexID,
	) scexec.Backfill {
		return scexec.Backfill{
			TableID:       tableID,
			SourceIndexID: sourceIndexID,
			DestIndexIDs:  destIndexIDs,
		}
	}

	type testData struct {
		name         string
		rangeSpans   []roachpb.Span
		origFraction float32
		progress     []scexec.BackfillProgress
	}
	t.Run("basic", func(t *testing.T) {
		tc := testData{
			name:         "foo",
			rangeSpans:   mkSpans("a", "b", "c", "d", "e", "f"),
			origFraction: .1,
			progress: []scexec.BackfillProgress{
				{
					Backfill:              mkBackfill(1, 1, 2, 3),
					MinimumWriteTimestamp: hlc.Timestamp{},
					SpansToDo: append(
						mkSpans("a", "cc"),
						mkSpans("dd", "ee")...,
					),
				},
				{
					Backfill:              mkBackfill(2, 1, 2),
					MinimumWriteTimestamp: hlc.Timestamp{},
					SpansToDo: append(
						mkSpans("a", "cc"),
						mkSpans("dd", "ee")...,
					),
				},
			},
		}
		ctx := context.Background()
		var bts backfillTrackerTestState
		bts.mu.rangeSpans = tc.rangeSpans
		tr := newBackfillTracker(bts.cfg(), tc.progress, tc.origFraction)

		t.Run("retrieving initial state", func(t *testing.T) {
			{
				pr0, err := tr.GetBackfillProgress(ctx,
					mkBackfill(1, 1, 2, 3))
				require.NoError(t, err)
				require.EqualValues(t, tc.progress[0], pr0)
			}
			{
				pr0, err := tr.GetBackfillProgress(ctx,
					// Reverse the destIndexIDs order, make sure it still works.
					mkBackfill(1, 1, 3, 2))
				require.NoError(t, err)
				require.EqualValues(t, tc.progress[0], pr0)
			}
			{
				pr1, err := tr.GetBackfillProgress(ctx,
					mkBackfill(2, 1, 2))
				require.NoError(t, err)
				require.EqualValues(t, tc.progress[1], pr1)
			}

		})
		t.Run("zero value progress for unknown backfill", func(t *testing.T) {
			bf := mkBackfill(42, 1, 3)
			pr, err := tr.GetBackfillProgress(ctx, bf)
			require.NoError(t, err)
			require.EqualValues(t, scexec.BackfillProgress{
				Backfill: bf,
			}, pr)
		})
		t.Run("GetBackfillProgress with wrong indexes fails", func(t *testing.T) {
			_, err := tr.GetBackfillProgress(ctx,
				mkBackfill(2, 1, 2, 3))
			require.EqualError(t, err,
				"backfill {2 1 [2 3]} does not match stored progress for {2 1 [2]}",
			)
		})
		t.Run("FlushCheckpoint initial does not write", func(t *testing.T) {
			require.Nil(t, bts.getCheckpoint())
			require.NoError(t, tr.FlushCheckpoint(ctx))
			require.EqualValues(t, []scexec.BackfillProgress(nil), bts.getCheckpoint())
			require.EqualValues(t, 0, bts.getCheckpointUpdatedCalls())
		})
		t.Run("WriteFraction initial", func(t *testing.T) {
			require.Equal(t, float32(0), bts.getFraction())
			require.NoError(t, tr.FlushFractionCompleted(ctx))
			// No update, so no write.
			require.EqualValues(t, float32(0), bts.getFraction())
			require.EqualValues(t, 0, bts.getFractionUpdatedCalls())
		})
		backfill2 := mkBackfill(42, 1, 3)
		t.Run("SetBackfillProgress with initial spans for new backfill", func(t *testing.T) {
			require.NoError(t, tr.SetBackfillProgress(ctx, scexec.BackfillProgress{
				Backfill:              backfill2,
				MinimumWriteTimestamp: hlc.Timestamp{WallTime: 1},
				SpansToDo:             mkSpans("a", "c"),
			}))
		})
		t.Run("Observe that there still has not been any progress", func(t *testing.T) {
			require.NoError(t, tr.FlushFractionCompleted(ctx))
			// No update, so no write.
			require.EqualValues(t, float32(0), bts.getFraction())
			require.EqualValues(t, 0, bts.getFractionUpdatedCalls())
		})
		updatedProgress2 := scexec.BackfillProgress{
			Backfill:              backfill2,
			MinimumWriteTimestamp: hlc.Timestamp{WallTime: 1},
			SpansToDo:             mkSpans("a", "b"),
		}
		t.Run("SetBackfillProgress to finish a range", func(t *testing.T) {
			require.NoError(t, tr.SetBackfillProgress(ctx, updatedProgress2))
		})
		t.Run("Observe that has been a progress update", func(t *testing.T) {
			require.NoError(t, tr.FlushFractionCompleted(ctx))
			// Each of the backfills has 2 ranges. One of the backfills now
			// is 1/2 done. That means we've finished 1/6th of the backfill
			// work. We started at 10%, so
			//
			//  .1 + .9*(1/6) = (2+3)/20 = .25
			//
			require.EqualValues(t, float32(.25), bts.getFraction())
			require.EqualValues(t, 1, bts.getFractionUpdatedCalls())
		})
		t.Run("Observe that FlushCheckpoint works", func(t *testing.T) {
			require.Nil(t, tr.FlushCheckpoint(ctx))
			require.EqualValues(t, bts.getCheckpoint(), []scexec.BackfillProgress{
				tc.progress[0],
				tc.progress[1],
				updatedProgress2,
			})
		})
		t.Run("Observe that flush without any writes does nothing", func(t *testing.T) {
			fractionUpdatedBefore := bts.getFractionUpdatedCalls()
			checkpointUpdatedBefore := bts.getCheckpointUpdatedCalls()
			require.Nil(t, tr.FlushCheckpoint(ctx))
			require.Nil(t, tr.FlushFractionCompleted(ctx))
			require.Equal(t, fractionUpdatedBefore, bts.getFractionUpdatedCalls())
			require.Equal(t, checkpointUpdatedBefore, bts.getCheckpointUpdatedCalls())
		})
	})

}

type backfillTrackerTestState struct {
	mu struct {
		syncutil.Mutex
		rangeSpans             []roachpb.Span
		fraction               float32
		fractionUpdatedCalls   int
		checkpoint             []scexec.BackfillProgress
		checkpointUpdatedCalls int
	}
}

func (bts *backfillTrackerTestState) cfg() backfillTrackerConfig {
	return backfillTrackerConfig{
		numRangesInSpans:      bts.numRangesInSpans,
		writeProgressFraction: bts.writeProgressFraction,
		writeCheckpoint:       bts.writeCheckpoint,
	}
}

func (bts *backfillTrackerTestState) numRangesInSpans(
	ctx context.Context, sp []roachpb.Span,
) (nRanges int, _ error) {
	tr := interval.NewRangeTree()
	for _, s := range sp {
		tr.Add(s.AsRange())
	}
	bts.mu.Lock()
	defer bts.mu.Unlock()
	for _, s := range bts.mu.rangeSpans {
		if tr.Encloses(s.AsRange()) {
			nRanges++
		}
	}
	return nRanges, nil
}

func (bts *backfillTrackerTestState) writeProgressFraction(
	_ context.Context, fractionProgressed float32,
) error {
	bts.mu.Lock()
	defer bts.mu.Unlock()
	bts.mu.fraction = fractionProgressed
	bts.mu.fractionUpdatedCalls++
	return nil
}

func (bts *backfillTrackerTestState) writeCheckpoint(
	_ context.Context, progresses []scexec.BackfillProgress,
) error {
	bts.mu.Lock()
	defer bts.mu.Unlock()
	bts.mu.checkpoint = progresses
	bts.mu.checkpointUpdatedCalls++
	return nil
}

func (bts *backfillTrackerTestState) getCheckpoint() []scexec.BackfillProgress {
	bts.mu.Lock()
	defer bts.mu.Unlock()
	return bts.mu.checkpoint
}

func (bts *backfillTrackerTestState) getFraction() float32 {
	bts.mu.Lock()
	defer bts.mu.Unlock()
	return bts.mu.fraction
}

func (bts *backfillTrackerTestState) getFractionUpdatedCalls() int {
	bts.mu.Lock()
	defer bts.mu.Unlock()
	return bts.mu.fractionUpdatedCalls
}

func (bts *backfillTrackerTestState) getCheckpointUpdatedCalls() interface{} {
	bts.mu.Lock()
	defer bts.mu.Unlock()
	return bts.mu.checkpointUpdatedCalls
}
