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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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
	mkSpans := func(tableID, indexID uint32, ks ...string) (ret []roachpb.Span) {
		prefix := keys.SystemSQLCodec.IndexPrefix(tableID, indexID)
		prefix = prefix[:len(prefix):len(prefix)]
		switch len(ks) {
		case 0:
			ks = []string{string(prefix), string(prefix.PrefixEnd())}
		case 1:
			ks = append(ks, string(prefix.PrefixEnd()))
		}
		for i := 1; i < len(ks); i++ {
			start := append(prefix, ks[i-1]...)
			end := append(prefix, ks[i]...)
			if ks[i] == "" {
				end = prefix.PrefixEnd()
			}
			ret = append(ret, roachpb.Span{Key: start, EndKey: end})
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
			name: "foo",
			rangeSpans: append(append(
				mkSpans(1, 1, "", "a", "b", "c", "d", ""),
				mkSpans(2, 1, "", "a", "b", "c", "d", "")...,
			), mkSpans(42, 1, "", "a", "b", "c", "d", "")...),
			progress: []scexec.BackfillProgress{
				{
					Backfill:              mkBackfill(1, 1, 2, 3),
					MinimumWriteTimestamp: hlc.Timestamp{},
					CompletedSpans: append(
						mkSpans(1, 1, "", "bb"),
						mkSpans(1, 1, "cc", "")...,
					),
				},
				{
					Backfill:              mkBackfill(2, 1, 2),
					MinimumWriteTimestamp: hlc.Timestamp{},
					CompletedSpans:        nil,
				},
			},
		}
		ctx := context.Background()
		var bts backfillTrackerTestState
		bts.mu.rangeSpans = tc.rangeSpans
		tr := newBackfillTracker(keys.SystemSQLCodec, bts.cfg(), tc.progress)

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
			// We've never had an update call, so expect the flush to no-op.
			require.EqualValues(t, 0, bts.getFraction())
			require.EqualValues(t, 0, bts.getFractionUpdatedCalls())
		})
		backfill2 := mkBackfill(42, 1, 3)
		t.Run("SetBackfillProgress with initial spans for new backfill", func(t *testing.T) {
			require.NoError(t, tr.SetBackfillProgress(ctx, scexec.BackfillProgress{
				Backfill:              backfill2,
				MinimumWriteTimestamp: hlc.Timestamp{WallTime: 1},
				CompletedSpans:        nil,
			}))
		})
		t.Run("Observe that there has been a progress update", func(t *testing.T) {
			require.NoError(t, tr.FlushFractionCompleted(ctx))
			// No we see that the denominator has changed to 3/15
			require.EqualValues(t, float32(.2), bts.getFraction())
			require.EqualValues(t, 1, bts.getFractionUpdatedCalls())
		})
		updatedProgress2 := scexec.BackfillProgress{
			Backfill:              backfill2,
			MinimumWriteTimestamp: hlc.Timestamp{WallTime: 1},
			CompletedSpans:        mkSpans(42, 1, "a", "d"),
		}
		t.Run("SetBackfillProgress to finish a range", func(t *testing.T) {
			require.NoError(t, tr.SetBackfillProgress(ctx, updatedProgress2))
		})
		t.Run("Observe that has been a progress update", func(t *testing.T) {
			require.NoError(t, tr.FlushFractionCompleted(ctx))

			require.EqualValues(t, float32(.4), bts.getFraction())
			require.EqualValues(t, 2, bts.getFractionUpdatedCalls())
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
	ctx context.Context, totalSpan roachpb.Span, containedBy roachpb.SpanGroup,
) (total, inContainedBy int, _ error) {
	bts.mu.Lock()
	defer bts.mu.Unlock()
	for _, s := range bts.mu.rangeSpans {
		if totalSpan.Overlaps(s) {
			total++
			if containedBy.Encloses(s) {
				inContainedBy++
			}
		}
	}
	return total, inContainedBy, nil
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
