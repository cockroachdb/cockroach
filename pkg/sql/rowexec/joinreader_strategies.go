// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// joinReaderStrategy abstracts the processing of looked-up rows. The joinReader
// cooperates with a joinReaderStrategy to produce joined rows. More
// specifically, the joinReader processes rows from the input-side, passes those
// rows to the joinReaderStrategy.processLookupRows() which (usually) holds on
// to them and returns key spans to be looked up, then the joinReader
// iteratively looks up those spans and passes the resulting looked-up rows to
// joinReaderStrategy.processLookedUpRow(). The joinReaderStrategy now has rows
// from both sides of the join, and performs the actual joining, emitting output
// rows from pairs of joined rows.
//
// There are three implementations of joinReaderStrategy:
// - joinReaderNoOrderingStrategy: used when the joined rows do not need to be
//   produced in input-row order.
// - joinReaderOrderingStrategy: used when the joined rows need to be produced
//   in input-row order. As opposed to the prior strategy, this one needs to do
//   more buffering to deal with out-of-order looked-up rows.
// - joinReaderIndexJoinStrategy: used when we're performing a join between an
//   index and the table's PK. This one is the simplest and the most efficient
//   because it doesn't actually join anything - it directly emits the PK rows.
//   The joinReaderIndexJoinStrategy is used by both ordered and unordered index
//   joins; see comments on joinReaderIndexJoinStrategy for details.
type joinReaderStrategy interface {
	// getLookupRowsBatchSizeHint returns the size in bytes of the batch of lookup
	// rows.
	getLookupRowsBatchSizeHint() int64
	// getMaxLookupKeyCols returns the maximum number of key columns used to
	// lookup into the index.
	getMaxLookupKeyCols() int
	// generateRemoteSpans generates spans targeting remote nodes for the current
	// batch of input rows. Returns an error if this is not a locality optimized
	// lookup join.
	generateRemoteSpans() (roachpb.Spans, error)
	// generatedRemoteSpans returns true if generateRemoteSpans has been called on
	// the current batch of input rows.
	generatedRemoteSpans() bool
	// processLookupRows consumes the rows the joinReader has buffered and returns
	// the lookup spans.
	processLookupRows(rows []rowenc.EncDatumRow) (roachpb.Spans, error)
	// processLookedUpRow processes a looked up row. A joinReaderState is returned
	// to indicate the next state to transition to. If this next state is
	// jrPerformingLookup, processLookedUpRow will be called again if the looked
	// up rows have not been exhausted. A transition to jrStateUnknown is
	// unsupported, but if an error is returned, the joinReader will transition
	// to draining.
	processLookedUpRow(ctx context.Context, row rowenc.EncDatumRow, key roachpb.Key) (joinReaderState, error)
	// prepareToEmit informs the strategy implementation that all looked up rows
	// have been read, and that it should prepare for calls to nextRowToEmit.
	prepareToEmit(ctx context.Context)
	// nextRowToEmit gets the next row to emit from the strategy. An accompanying
	// joinReaderState is also returned, indicating a state to transition to after
	// emitting this row. A transition to jrStateUnknown is unsupported, but if an
	// error is returned, the joinReader will transition to draining.
	nextRowToEmit(ctx context.Context) (rowenc.EncDatumRow, joinReaderState, error)
	// spilled returns whether the strategy spilled to disk.
	spilled() bool
	// close releases any resources associated with the joinReaderStrategy.
	close(ctx context.Context)
}

// joinReaderNoOrderingStrategy is a joinReaderStrategy that doesn't maintain
// the input ordering: the order in which joined rows are emitted does not
// correspond to the order of the rows passed to processLookupRows(). This is
// more performant than joinReaderOrderingStrategy.
//
// Consider the following example:
// - the input side has rows (1, red), (2, blue), (3, blue), (4, red).
// - the lookup side has rows (red, x), (blue, y).
// - the join needs to produce the pairs (1, x), (2, y), (3, y), (4, x), in any
//   order.
//
// Say the joinReader looks up rows in order: (red, x), then (blue, y). Once
// (red, x) is fetched, it is handed to
// joinReaderNoOrderingStrategy.processLookedUpRow(), which will match it
// against all the corresponding input rows, and immediately emit (1, x), (4,
// x). Then the joinReader will be handed in (blue, y), for which the
// joinReaderNoOrderingStrategy will emit (2, blue) and (3, blue). Notice that
// the rows were produced in an order different from the input order but, on the
// flip side, there was no buffering of the looked-up rows. See
// joinReaderOrderingStrategy for a contrast.
type joinReaderNoOrderingStrategy struct {
	*joinerBase
	joinReaderSpanGenerator
	isPartialJoin        bool
	inputRows            []rowenc.EncDatumRow
	remoteSpansGenerated bool

	scratchMatchingInputRowIndices []int

	emitState struct {
		// processingLookupRow is an explicit boolean that specifies whether the
		// strategy is currently processing a match. This is set to true in
		// processLookedUpRow and causes nextRowToEmit to process the data in
		// emitState. If set to false, the strategy determines in nextRowToEmit
		// that no more looked up rows need processing, so unmatched input rows need
		// to be emitted.
		processingLookupRow bool

		// Used when processingLookupRow is false.
		unmatchedInputRowIndicesCursor int
		// unmatchedInputRowIndices is used only when emitting unmatched rows after
		// processing lookup results. It is populated once when first emitting
		// unmatched rows.
		unmatchedInputRowIndices            []int
		unmatchedInputRowIndicesInitialized bool

		// Used when processingLookupRow is true.
		matchingInputRowIndicesCursor int
		matchingInputRowIndices       []int
		lookedUpRow                   rowenc.EncDatumRow
	}

	groupingState *inputBatchGroupingState
}

// getLookupRowsBatchSizeHint returns the batch size for the join reader no
// ordering strategy. This number was chosen by running TPCH queries 7, 9, 10,
// and 11 with varying batch sizes and choosing the smallest batch size that
// offered a significant performance improvement. Larger batch sizes offered
// small to no marginal improvements.
func (s *joinReaderNoOrderingStrategy) getLookupRowsBatchSizeHint() int64 {
	return 2 << 20 /* 2 MiB */
}

func (s *joinReaderNoOrderingStrategy) getMaxLookupKeyCols() int {
	return s.maxLookupCols()
}

func (s *joinReaderNoOrderingStrategy) generateRemoteSpans() (roachpb.Spans, error) {
	gen, ok := s.joinReaderSpanGenerator.(*localityOptimizedSpanGenerator)
	if !ok {
		return nil, errors.AssertionFailedf("generateRemoteSpans can only be called for locality optimized lookup joins")
	}
	s.remoteSpansGenerated = true
	return gen.generateRemoteSpans(s.inputRows)
}

func (s *joinReaderNoOrderingStrategy) generatedRemoteSpans() bool {
	return s.remoteSpansGenerated
}

func (s *joinReaderNoOrderingStrategy) processLookupRows(
	rows []rowenc.EncDatumRow,
) (roachpb.Spans, error) {
	s.inputRows = rows
	s.remoteSpansGenerated = false
	s.emitState.unmatchedInputRowIndicesInitialized = false
	return s.generateSpans(s.inputRows)
}

func (s *joinReaderNoOrderingStrategy) processLookedUpRow(
	_ context.Context, row rowenc.EncDatumRow, key roachpb.Key,
) (joinReaderState, error) {
	matchingInputRowIndices := s.getMatchingRowIndices(key)
	if s.isPartialJoin {
		// In the case of partial joins, only process input rows that have not been
		// matched yet. Make a copy of the matching input row indices to avoid
		// overwriting the caller's slice.
		s.scratchMatchingInputRowIndices = s.scratchMatchingInputRowIndices[:0]
		for _, inputRowIdx := range matchingInputRowIndices {
			if !s.groupingState.getMatched(inputRowIdx) {
				s.scratchMatchingInputRowIndices = append(s.scratchMatchingInputRowIndices, inputRowIdx)
			}
		}
		matchingInputRowIndices = s.scratchMatchingInputRowIndices
	}
	s.emitState.processingLookupRow = true
	s.emitState.lookedUpRow = row
	s.emitState.matchingInputRowIndices = matchingInputRowIndices
	s.emitState.matchingInputRowIndicesCursor = 0
	return jrEmittingRows, nil
}

func (s *joinReaderNoOrderingStrategy) prepareToEmit(ctx context.Context) {}

func (s *joinReaderNoOrderingStrategy) nextRowToEmit(
	_ context.Context,
) (rowenc.EncDatumRow, joinReaderState, error) {
	if !s.emitState.processingLookupRow {
		// processLookedUpRow was not called before nextRowToEmit, which means that
		// the next unmatched row needs to be processed.
		if !shouldEmitUnmatchedRow(leftSide, s.joinType) {
			// The joinType does not require the joiner to emit unmatched rows. Move
			// on to the next batch of lookup rows.
			return nil, jrReadingInput, nil
		}

		if !s.emitState.unmatchedInputRowIndicesInitialized {
			s.emitState.unmatchedInputRowIndices = s.emitState.unmatchedInputRowIndices[:0]
			for inputRowIdx := range s.inputRows {
				if s.groupingState.isUnmatched(inputRowIdx) {
					s.emitState.unmatchedInputRowIndices = append(s.emitState.unmatchedInputRowIndices, inputRowIdx)
				}
			}
			s.emitState.unmatchedInputRowIndicesInitialized = true
			s.emitState.unmatchedInputRowIndicesCursor = 0
		}

		if s.emitState.unmatchedInputRowIndicesCursor >= len(s.emitState.unmatchedInputRowIndices) {
			// All unmatched rows have been emitted.
			return nil, jrReadingInput, nil
		}
		inputRow := s.inputRows[s.emitState.unmatchedInputRowIndices[s.emitState.unmatchedInputRowIndicesCursor]]
		s.emitState.unmatchedInputRowIndicesCursor++
		if !s.joinType.ShouldIncludeRightColsInOutput() {
			return inputRow, jrEmittingRows, nil
		}
		return s.renderUnmatchedRow(inputRow, leftSide), jrEmittingRows, nil
	}

	for s.emitState.matchingInputRowIndicesCursor < len(s.emitState.matchingInputRowIndices) {
		inputRowIdx := s.emitState.matchingInputRowIndices[s.emitState.matchingInputRowIndicesCursor]
		s.emitState.matchingInputRowIndicesCursor++
		inputRow := s.inputRows[inputRowIdx]
		if s.joinType == descpb.LeftSemiJoin && s.groupingState.getMatched(inputRowIdx) {
			// Already output a row for this group. Note that we've already excluded
			// this case when all groups are of length 1 by reading the getMatched
			// value in processLookedUpRow. But when groups can have multiple rows
			// it is possible that a group that was not matched then is by now
			// matched.
			continue
		}

		// Render the output row, this also evaluates the ON condition.
		outputRow, err := s.render(inputRow, s.emitState.lookedUpRow)
		if err != nil {
			return nil, jrStateUnknown, err
		}
		if outputRow == nil {
			// This row failed the ON condition, so it remains unmatched.
			continue
		}

		s.groupingState.setMatched(inputRowIdx)
		if !s.joinType.ShouldIncludeRightColsInOutput() {
			if s.joinType == descpb.LeftAntiJoin {
				// Skip emitting row.
				continue
			}
			return inputRow, jrEmittingRows, nil
		}
		return outputRow, jrEmittingRows, nil
	}

	// Processed all matches for a given lookup row, move to the next lookup row.
	// Set processingLookupRow to false explicitly so if the joinReader re-enters
	// nextRowToEmit, the strategy knows that no more lookup rows were processed
	// and should proceed to emit unmatched rows.
	s.emitState.processingLookupRow = false
	return nil, jrPerformingLookup, nil
}

func (s *joinReaderNoOrderingStrategy) spilled() bool { return false }

func (s *joinReaderNoOrderingStrategy) close(_ context.Context) {}

// joinReaderIndexJoinStrategy is a joinReaderStrategy that executes an index
// join. This joinReaderStrategy is very simple - it immediately emits any row
// passed to processLookedUpRow(). Since it is an index-join, it doesn't
// actually do any joining: the looked-up rows correspond to a table's PK and
// the input rows correspond to another one of the table's indexes; there's
// nothing to join.
//
// joinReaderIndexJoinStrategy does not, by itself, do anything to output rows
// in the order of the input rows (as they're ordered when passed to
// processLookupRows). But, that will be the order in which the output rows are
// produced if the looked-up rows are passed to processLookedUpRow() in the same
// order as the spans returned by processLookupRows(). In other words, if the
// spans resulting from processLookupRows() are not re-sorted, then
// joinReaderIndexJoinStrategy will produce its output rows in order. Note that
// the spans produced by processLookupRows correspond 1-1 with the input;
// there's no deduping because they're all unique (representing PKs).
type joinReaderIndexJoinStrategy struct {
	*joinerBase
	joinReaderSpanGenerator
	inputRows []rowenc.EncDatumRow

	emitState struct {
		// processingLookupRow is an explicit boolean that specifies whether the
		// strategy is currently processing a match. This is set to true in
		// processLookedUpRow and causes nextRowToEmit to process the data in
		// emitState. If set to false, the strategy determines in nextRowToEmit
		// that no more looked up rows need processing, so unmatched input rows need
		// to be emitted.
		processingLookupRow bool
		lookedUpRow         rowenc.EncDatumRow
	}
}

// getLookupRowsBatchSizeHint returns the batch size for the join reader index
// join strategy. This number was chosen by running TPCH queries 3, 4, 5, 9,
// and 19 with varying batch sizes and choosing the smallest batch size that
// offered a significant performance improvement. Larger batch sizes offered
// small to no marginal improvements.
func (s *joinReaderIndexJoinStrategy) getLookupRowsBatchSizeHint() int64 {
	return 4 << 20 /* 4 MB */
}

func (s *joinReaderIndexJoinStrategy) getMaxLookupKeyCols() int {
	return s.maxLookupCols()
}

func (s *joinReaderIndexJoinStrategy) generateRemoteSpans() (roachpb.Spans, error) {
	return nil, errors.AssertionFailedf("generateRemoteSpans called on an index join")
}

func (s *joinReaderIndexJoinStrategy) generatedRemoteSpans() bool {
	return false
}

func (s *joinReaderIndexJoinStrategy) processLookupRows(
	rows []rowenc.EncDatumRow,
) (roachpb.Spans, error) {
	s.inputRows = rows
	return s.generateSpans(s.inputRows)
}

func (s *joinReaderIndexJoinStrategy) processLookedUpRow(
	_ context.Context, row rowenc.EncDatumRow, _ roachpb.Key,
) (joinReaderState, error) {
	s.emitState.processingLookupRow = true
	s.emitState.lookedUpRow = row
	return jrEmittingRows, nil
}

func (s *joinReaderIndexJoinStrategy) prepareToEmit(ctx context.Context) {}

func (s *joinReaderIndexJoinStrategy) nextRowToEmit(
	ctx context.Context,
) (rowenc.EncDatumRow, joinReaderState, error) {
	if !s.emitState.processingLookupRow {
		return nil, jrReadingInput, nil
	}
	s.emitState.processingLookupRow = false
	return s.emitState.lookedUpRow, jrPerformingLookup, nil
}

func (s *joinReaderIndexJoinStrategy) spilled() bool {
	return false
}

func (s *joinReaderIndexJoinStrategy) close(ctx context.Context) {}

// partialJoinSentinel is used as the inputRowIdxToLookedUpRowIndices value for
// semi- and anti-joins, where we only need to know about the existence of a
// match.
var partialJoinSentinel = []int{-1}

// joinReaderOrderingStrategy is a joinReaderStrategy that maintains the input
// ordering: the order in which joined rows are emitted corresponds to the order
// of the rows passed to processLookupRows().
//
// Consider the following example:
// - the input side has rows (1, red), (2, blue), (3, blue), (4, red).
// - the lookup side has rows (red, x), (blue, y).
// - the join needs to produce the pairs (1, x), (2, y), (3, y), (4, x), in this
//   order.
//
// Say the joinReader looks up rows in order: (red, x), then (blue, y). Once
// (red, x) is fetched, it is handed to
// joinReaderOderingStrategy.processLookedUpRow(), which will match it against
// all the corresponding input rows, producing (1, x), (4, x). These two rows
// are not emitted because that would violate the input ordering (well, (1, x)
// could be emitted, but we're not smart enough). So, they are buffered until
// all joined rows are produced. Then the joinReader will hand in (blue, y), for
// which the joinReaderOrderingStrategy produces (2, y) and (3, y). Now that all
// output rows are buffered, they are re-ordered according to the input order
// and emitted.
//
// Because of the buffering required to eventually reorder the output, the
// joinReaderOrderingStrategy is more expensive than
// joinReaderNoOrderingStrategy.
type joinReaderOrderingStrategy struct {
	*joinerBase
	joinReaderSpanGenerator
	isPartialJoin bool

	inputRows            []rowenc.EncDatumRow
	remoteSpansGenerated bool

	// inputRowIdxToLookedUpRowIndices is a multimap from input row indices to
	// corresponding looked up row indices. This serves to emit rows in input
	// order even though lookups are performed out of order.
	//
	// The map is populated in the jrPerformingLookup state. For non partial joins
	// (everything but semi/anti join), the looked up rows are the rows that came
	// back from the lookup span for each input row, without checking for matches
	// with respect to the on-condition. For semi/anti join, we store at most one
	// sentinel value, indicating a matching lookup if it's present, since the
	// right side of a semi/anti join is not used.
	inputRowIdxToLookedUpRowIndices [][]int

	// lookedUpRowIdx indicates the index of the current lookup row within the
	// rows returned by a lookup batch.
	lookedUpRowIdx int
	lookedUpRows   *rowcontainer.DiskBackedNumberedRowContainer

	// emitCursor contains information about where the next row to emit is within
	// inputRowIdxToLookedUpRowIndices.
	emitCursor struct {
		// inputRowIdx contains the index into inputRowIdxToLookedUpRowIndices that
		// we're about to emit.
		inputRowIdx int
		// outputRowIdx contains the index into the inputRowIdx'th row of
		// inputRowIdxToLookedUpRowIndices that we're about to emit.
		outputRowIdx int
	}

	groupingState *inputBatchGroupingState

	// outputGroupContinuationForLeftRow is true when this join is the first
	// join in paired-joins. Note that in this case the input batches will
	// always be of size 1 (real input batching only happens when this join is
	// the second join in paired-joins).
	outputGroupContinuationForLeftRow bool
}

func (s *joinReaderOrderingStrategy) getLookupRowsBatchSizeHint() int64 {
	// TODO(asubiotto): Eventually we might want to adjust this batch size
	//  dynamically based on whether the result row container spilled or not.
	return 10 << 10 /* 10 KiB */
}

func (s *joinReaderOrderingStrategy) getMaxLookupKeyCols() int {
	return s.maxLookupCols()
}

func (s *joinReaderOrderingStrategy) generateRemoteSpans() (roachpb.Spans, error) {
	gen, ok := s.joinReaderSpanGenerator.(*localityOptimizedSpanGenerator)
	if !ok {
		return nil, errors.AssertionFailedf("generateRemoteSpans can only be called for locality optimized lookup joins")
	}
	s.remoteSpansGenerated = true
	return gen.generateRemoteSpans(s.inputRows)
}

func (s *joinReaderOrderingStrategy) generatedRemoteSpans() bool {
	return s.remoteSpansGenerated
}

func (s *joinReaderOrderingStrategy) processLookupRows(
	rows []rowenc.EncDatumRow,
) (roachpb.Spans, error) {
	// Reset s.inputRowIdxToLookedUpRowIndices. This map will be populated in
	// processedLookedUpRow(), as lookup results are received (possibly out of
	// order).
	if cap(s.inputRowIdxToLookedUpRowIndices) >= len(rows) {
		s.inputRowIdxToLookedUpRowIndices = s.inputRowIdxToLookedUpRowIndices[:len(rows)]
		for i := range s.inputRowIdxToLookedUpRowIndices {
			s.inputRowIdxToLookedUpRowIndices[i] = s.inputRowIdxToLookedUpRowIndices[i][:0]
		}
	} else {
		s.inputRowIdxToLookedUpRowIndices = make([][]int, len(rows))
	}

	s.inputRows = rows
	s.remoteSpansGenerated = false
	return s.generateSpans(s.inputRows)
}

func (s *joinReaderOrderingStrategy) processLookedUpRow(
	ctx context.Context, row rowenc.EncDatumRow, key roachpb.Key,
) (joinReaderState, error) {
	matchingInputRowIndices := s.getMatchingRowIndices(key)
	if !s.isPartialJoin {
		// Replace missing values with nulls to appease the row container.
		for i := range row {
			if row[i].IsUnset() {
				row[i].Datum = tree.DNull
			}
		}
		if _, err := s.lookedUpRows.AddRow(ctx, row); err != nil {
			return jrStateUnknown, err
		}
	}

	// Update our map from input rows to looked up rows.
	for _, inputRowIdx := range matchingInputRowIndices {
		if !s.isPartialJoin {
			s.inputRowIdxToLookedUpRowIndices[inputRowIdx] = append(
				s.inputRowIdxToLookedUpRowIndices[inputRowIdx], s.lookedUpRowIdx)
			continue
		}

		// During a SemiJoin or AntiJoin, we only output if we've seen no match
		// for this input row yet. Additionally, since we don't have to render
		// anything to output a Semi or Anti join match, we can evaluate our
		// on condition now. NB: the first join in paired-joins is never a
		// SemiJoin or AntiJoin.
		if !s.groupingState.getMatched(inputRowIdx) {
			renderedRow, err := s.render(s.inputRows[inputRowIdx], row)
			if err != nil {
				return jrStateUnknown, err
			}
			if renderedRow == nil {
				// We failed our on-condition.
				continue
			}
			s.groupingState.setMatched(inputRowIdx)
			s.inputRowIdxToLookedUpRowIndices[inputRowIdx] = partialJoinSentinel
		}
	}
	s.lookedUpRowIdx++

	return jrPerformingLookup, nil
}

func (s *joinReaderOrderingStrategy) prepareToEmit(ctx context.Context) {
	if !s.isPartialJoin {
		s.lookedUpRows.SetupForRead(ctx, s.inputRowIdxToLookedUpRowIndices)
	}
}

func (s *joinReaderOrderingStrategy) nextRowToEmit(
	ctx context.Context,
) (rowenc.EncDatumRow, joinReaderState, error) {
	if s.emitCursor.inputRowIdx >= len(s.inputRowIdxToLookedUpRowIndices) {
		log.VEventf(ctx, 1, "done emitting rows")
		// Ready for another input batch. Reset state. The groupingState,
		// which also relates to this batch, will be reset by joinReader.
		s.emitCursor.outputRowIdx = 0
		s.emitCursor.inputRowIdx = 0
		if err := s.lookedUpRows.UnsafeReset(ctx); err != nil {
			return nil, jrStateUnknown, err
		}
		s.lookedUpRowIdx = 0
		return nil, jrReadingInput, nil
	}

	inputRow := s.inputRows[s.emitCursor.inputRowIdx]
	lookedUpRows := s.inputRowIdxToLookedUpRowIndices[s.emitCursor.inputRowIdx]
	if s.emitCursor.outputRowIdx >= len(lookedUpRows) {
		// We have no more rows for the current input row. Emit an outer or anti
		// row if we didn't see a match, and bump to the next input row.
		inputRowIdx := s.emitCursor.inputRowIdx
		s.emitCursor.inputRowIdx++
		s.emitCursor.outputRowIdx = 0
		if s.groupingState.isUnmatched(inputRowIdx) {
			switch s.joinType {
			case descpb.LeftOuterJoin:
				// An outer-join non-match means we emit the input row with NULLs for
				// the right side.
				if renderedRow := s.renderUnmatchedRow(inputRow, leftSide); renderedRow != nil {
					if s.outputGroupContinuationForLeftRow {
						// This must be the first row being output for this input row.
						renderedRow = append(renderedRow, falseEncDatum)
					}
					return renderedRow, jrEmittingRows, nil
				}
			case descpb.LeftAntiJoin:
				// An anti-join non-match means we emit the input row.
				return inputRow, jrEmittingRows, nil
			}
		}
		return nil, jrEmittingRows, nil
	}

	lookedUpRowIdx := lookedUpRows[s.emitCursor.outputRowIdx]
	s.emitCursor.outputRowIdx++
	switch s.joinType {
	case descpb.LeftSemiJoin:
		// A semi-join match means we emit our input row. This is the case where
		// we used the partialJoinSentinel.
		return inputRow, jrEmittingRows, nil
	case descpb.LeftAntiJoin:
		// An anti-join match means we emit nothing. This is the case where
		// we used the partialJoinSentinel.
		return nil, jrEmittingRows, nil
	}

	lookedUpRow, err := s.lookedUpRows.GetRow(s.Ctx, lookedUpRowIdx, false /* skip */)
	if err != nil {
		return nil, jrStateUnknown, err
	}
	outputRow, err := s.render(inputRow, lookedUpRow)
	if err != nil {
		return nil, jrStateUnknown, err
	}
	if outputRow != nil {
		wasAlreadyMatched := s.groupingState.setMatched(s.emitCursor.inputRowIdx)
		if s.outputGroupContinuationForLeftRow {
			if wasAlreadyMatched {
				// Not the first row output for this input row.
				outputRow = append(outputRow, trueEncDatum)
			} else {
				// First row output for this input row.
				outputRow = append(outputRow, falseEncDatum)
			}
		}
	}
	return outputRow, jrEmittingRows, nil
}

func (s *joinReaderOrderingStrategy) spilled() bool {
	return s.lookedUpRows.Spilled()
}

func (s *joinReaderOrderingStrategy) close(ctx context.Context) {
	if s.lookedUpRows != nil {
		s.lookedUpRows.Close(ctx)
	}
}
