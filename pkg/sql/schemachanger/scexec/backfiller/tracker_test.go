// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backfiller

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

// TestBackfillerTracker exercises the logic of the tracker
// by using an in-memory implementation of the underlying store and
// the set of ranges.
//
// TODO(ajwerner): It's probable that datadriven test would be easier
// to read.
func TestBackfillerTracker(t *testing.T) {
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
	mkMerge := func(
		tableID descpb.ID,
		sourceAndDestIDs ...descpb.IndexID,
	) (m scexec.Merge) {
		m.TableID = tableID
		var isDestID bool
		for _, id := range sourceAndDestIDs {
			if isDestID {
				m.DestIndexIDs = append(m.DestIndexIDs, id)
				isDestID = false
			} else {
				m.SourceIndexIDs = append(m.SourceIndexIDs, id)
				isDestID = true
			}
		}
		return m
	}

	type testData struct {
		name             string
		rangeSpans       []roachpb.Span
		backfillProgress []scexec.BackfillProgress
		mergeProgress    []scexec.MergeProgress
	}

	t.Run("backfill", func(t *testing.T) {
		tc := testData{
			name: "foo",
			rangeSpans: append(append(
				mkSpans(1, 1, "", "a", "b", "c", "d", ""),
				mkSpans(2, 1, "", "a", "b", "c", "d", "")...,
			), mkSpans(42, 1, "", "a", "b", "c", "d", "")...),
			backfillProgress: []scexec.BackfillProgress{
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
		var bts backfillerTrackerTestState
		bts.mu.rangeSpans = tc.rangeSpans
		tr := newTracker(keys.SystemSQLCodec, bts.cfg(), tc.backfillProgress, tc.mergeProgress)

		t.Run("retrieving initial state", func(t *testing.T) {
			{
				pr0, err := tr.GetBackfillProgress(ctx,
					mkBackfill(1, 1, 2, 3))
				require.NoError(t, err)
				require.EqualValues(t, tc.backfillProgress[0], pr0)
			}
			{
				pr0, err := tr.GetBackfillProgress(ctx,
					// Reverse the destIndexIDs order, make sure it still works.
					mkBackfill(1, 1, 3, 2))
				require.NoError(t, err)
				require.EqualValues(t, tc.backfillProgress[0], pr0)
			}
			{
				pr1, err := tr.GetBackfillProgress(ctx,
					mkBackfill(2, 1, 2))
				require.NoError(t, err)
				require.EqualValues(t, tc.backfillProgress[1], pr1)
			}

		})
		t.Run("zero value backfillProgress for unknown backfill", func(t *testing.T) {
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
				"backfill {2 1 [2 3]} does not match stored backfillProgress for {2 1 [2]}",
			)
		})
		t.Run("FlushCheckpoint initial does not write", func(t *testing.T) {
			require.Nil(t, bts.getBackfillCheckpoint())
			require.NoError(t, tr.FlushCheckpoint(ctx))
			require.EqualValues(t, []scexec.BackfillProgress(nil), bts.getBackfillCheckpoint())
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
		t.Run("Observe that there has been a backfillProgress update", func(t *testing.T) {
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
		t.Run("Observe that has been a backfillProgress update", func(t *testing.T) {
			require.NoError(t, tr.FlushFractionCompleted(ctx))

			require.EqualValues(t, float32(.4), bts.getFraction())
			require.EqualValues(t, 2, bts.getFractionUpdatedCalls())
		})
		t.Run("Observe that FlushCheckpoint works", func(t *testing.T) {
			require.Nil(t, tr.FlushCheckpoint(ctx))
			require.EqualValues(t, bts.getBackfillCheckpoint(), []scexec.BackfillProgress{
				tc.backfillProgress[0],
				tc.backfillProgress[1],
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

	t.Run("merge", func(t *testing.T) {
		tc := testData{
			name: "foo",
			rangeSpans: append(append(append(
				mkSpans(1, 2, "", "a", "b", "c", "d", ""),
				mkSpans(1, 4, "", "a", "b", "c", "d", "")...,
			), mkSpans(2, 2, "", "a", "b", "c", "d", "")...,
			), mkSpans(42, 2, "", "a", "b", "c", "d", "")...),
			mergeProgress: []scexec.MergeProgress{
				{
					Merge: mkMerge(1, 2, 1, 4, 3),
					CompletedSpans: [][]roachpb.Span{append(
						mkSpans(1, 2, "", "bb"),
						mkSpans(1, 2, "cc", "")...,
					), nil},
				},
				{
					Merge:          mkMerge(2, 2, 1),
					CompletedSpans: [][]roachpb.Span{nil},
				},
			},
		}
		ctx := context.Background()
		var bts backfillerTrackerTestState
		bts.mu.rangeSpans = tc.rangeSpans
		tr := newTracker(keys.SystemSQLCodec, bts.cfg(), tc.backfillProgress, tc.mergeProgress)

		t.Run("retrieving initial state", func(t *testing.T) {
			{
				pr0, err := tr.GetMergeProgress(ctx,
					mkMerge(1, 2, 1, 4, 3))
				require.NoError(t, err)
				require.EqualValues(t, tc.mergeProgress[0], pr0)
			}
			{
				pr0, err := tr.GetMergeProgress(ctx,
					// Reverse the destIndexIDs order, make sure it still works.
					mkMerge(1, 4, 3, 2, 1))
				require.NoError(t, err)
				require.EqualValues(t, tc.mergeProgress[0], pr0)
			}
			{
				pr1, err := tr.GetMergeProgress(ctx,
					mkMerge(2, 2, 1))
				require.NoError(t, err)
				require.EqualValues(t, tc.mergeProgress[1], pr1)
			}

		})
		t.Run("zero value mergeProgress for unknown merge", func(t *testing.T) {
			m := mkMerge(42, 3, 1)
			pr, err := tr.GetMergeProgress(ctx, m)
			require.NoError(t, err)
			require.EqualValues(t, scexec.MergeProgress{
				Merge:          m,
				CompletedSpans: [][]roachpb.Span{nil},
			}, pr)
		})
		t.Run("GetMergeProgress with wrong indexes fails", func(t *testing.T) {
			_, err := tr.GetMergeProgress(ctx,
				mkMerge(2, 2, 123))
			require.EqualError(t, err,
				"merge {2 [2] [123]} does not match stored mergeProgress for {2 [2] [1]}",
			)
		})
		t.Run("FlushCheckpoint initial does not write", func(t *testing.T) {
			require.Nil(t, bts.getMergeCheckpoint())
			require.NoError(t, tr.FlushCheckpoint(ctx))
			require.EqualValues(t, []scexec.MergeProgress(nil), bts.getMergeCheckpoint())
			require.EqualValues(t, 0, bts.getCheckpointUpdatedCalls())
		})
		t.Run("WriteFraction initial", func(t *testing.T) {
			require.Equal(t, float32(0), bts.getFraction())
			require.NoError(t, tr.FlushFractionCompleted(ctx))
			// We've never had an update call, so expect the flush to no-op.
			require.EqualValues(t, 0, bts.getFraction())
			require.EqualValues(t, 0, bts.getFractionUpdatedCalls())
		})
		merge2 := mkMerge(42, 2, 1)
		t.Run("SetMergeProgress with initial spans for new merge", func(t *testing.T) {
			require.NoError(t, tr.SetMergeProgress(ctx, scexec.MergeProgress{
				Merge:          merge2,
				CompletedSpans: [][]roachpb.Span{nil},
			}))
		})
		t.Run("Observe that there has been a mergeProgress update", func(t *testing.T) {
			require.NoError(t, tr.FlushFractionCompleted(ctx))
			// Now we see that the denominator has changed.
			require.EqualValues(t, float32(.15), bts.getFraction())
			require.EqualValues(t, 1, bts.getFractionUpdatedCalls())
		})
		updatedProgress2 := scexec.MergeProgress{
			Merge:          merge2,
			CompletedSpans: [][]roachpb.Span{mkSpans(42, 2, "a", "d")},
		}
		t.Run("SetMergeProgress to finish a range", func(t *testing.T) {
			require.NoError(t, tr.SetMergeProgress(ctx, updatedProgress2))
		})
		t.Run("Observe that has been a mergeProgress update", func(t *testing.T) {
			require.NoError(t, tr.FlushFractionCompleted(ctx))

			require.EqualValues(t, float32(.3), bts.getFraction())
			require.EqualValues(t, 2, bts.getFractionUpdatedCalls())
		})
		t.Run("Observe that FlushCheckpoint works", func(t *testing.T) {
			require.Nil(t, tr.FlushCheckpoint(ctx))
			require.EqualValues(t, bts.getMergeCheckpoint(), []scexec.MergeProgress{
				tc.mergeProgress[0],
				tc.mergeProgress[1],
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

type backfillerTrackerTestState struct {
	mu struct {
		syncutil.Mutex
		rangeSpans             []roachpb.Span
		fraction               float32
		fractionUpdatedCalls   int
		backfillCheckpoint     []scexec.BackfillProgress
		mergeCheckpoint        []scexec.MergeProgress
		checkpointUpdatedCalls int
	}
}

func (bts *backfillerTrackerTestState) cfg() trackerConfig {
	return trackerConfig{
		numRangesInSpanContainedBy: bts.numRangesInSpans,
		writeProgressFraction:      bts.writeProgressFraction,
		writeCheckpoint:            bts.writeCheckpoint,
	}
}

func (bts *backfillerTrackerTestState) numRangesInSpans(
	ctx context.Context, totalSpan roachpb.Span, containedBy []roachpb.Span,
) (total, inContainedBy int, _ error) {
	bts.mu.Lock()
	defer bts.mu.Unlock()
	var g roachpb.SpanGroup
	g.Add(containedBy...)
	for _, s := range bts.mu.rangeSpans {
		if totalSpan.Overlaps(s) {
			total++
			if g.Encloses(s.Intersect(totalSpan)) {
				inContainedBy++
			}
		}
	}
	return total, inContainedBy, nil
}

func (bts *backfillerTrackerTestState) writeProgressFraction(
	_ context.Context, fractionProgressed float32,
) error {
	bts.mu.Lock()
	defer bts.mu.Unlock()
	bts.mu.fraction = fractionProgressed
	bts.mu.fractionUpdatedCalls++
	return nil
}

func (bts *backfillerTrackerTestState) writeCheckpoint(
	_ context.Context, bps []scexec.BackfillProgress, mps []scexec.MergeProgress,
) error {
	bts.mu.Lock()
	defer bts.mu.Unlock()
	bts.mu.backfillCheckpoint = bps
	bts.mu.mergeCheckpoint = mps
	bts.mu.checkpointUpdatedCalls++
	return nil
}

func (bts *backfillerTrackerTestState) getBackfillCheckpoint() []scexec.BackfillProgress {
	bts.mu.Lock()
	defer bts.mu.Unlock()
	return bts.mu.backfillCheckpoint
}

func (bts *backfillerTrackerTestState) getMergeCheckpoint() []scexec.MergeProgress {
	bts.mu.Lock()
	defer bts.mu.Unlock()
	return bts.mu.mergeCheckpoint
}

func (bts *backfillerTrackerTestState) getFraction() float32 {
	bts.mu.Lock()
	defer bts.mu.Unlock()
	return bts.mu.fraction
}

func (bts *backfillerTrackerTestState) getFractionUpdatedCalls() int {
	bts.mu.Lock()
	defer bts.mu.Unlock()
	return bts.mu.fractionUpdatedCalls
}

func (bts *backfillerTrackerTestState) getCheckpointUpdatedCalls() interface{} {
	bts.mu.Lock()
	defer bts.mu.Unlock()
	return bts.mu.checkpointUpdatedCalls
}
