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
	"github.com/cockroachdb/cockroach/pkg/sql/span"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type defaultSpanGenerator struct {
	spanBuilder *span.Builder
	numKeyCols  int
	lookupCols  []uint32

	indexKeyRow rowenc.EncDatumRow
	// keyToInputRowIndices maps a lookup span key to the input row indices that
	// desire that span. This is used for joins other than index joins, for
	// de-duping spans, and to map the fetched rows to the input rows that need
	// to join with them. Index joins already have unique rows in the input that
	// generate unique spans for fetch, and simply output the fetched rows, do
	// do not use this map.
	keyToInputRowIndices map[string][]int

	scratchSpans roachpb.Spans
}

// Generate spans for a given row.
// If lookup columns are specified will use those to collect the relevant
// columns. Otherwise the first rows are assumed to correspond with the index.
// It additionally returns whether the row contains null, which is needed to
// decide whether or not to split the generated span into separate family
// specific spans.
func (g *defaultSpanGenerator) generateSpan(
	row rowenc.EncDatumRow,
) (_ roachpb.Span, containsNull bool, _ error) {
	numLookupCols := len(g.lookupCols)
	if numLookupCols > g.numKeyCols {
		return roachpb.Span{}, false, errors.Errorf(
			"%d lookup columns specified, expecting at most %d", numLookupCols, g.numKeyCols)
	}

	g.indexKeyRow = g.indexKeyRow[:0]
	for _, id := range g.lookupCols {
		g.indexKeyRow = append(g.indexKeyRow, row[id])
	}
	return g.spanBuilder.SpanFromEncDatums(g.indexKeyRow, numLookupCols)
}

func (g *defaultSpanGenerator) hasNullLookupColumn(row rowenc.EncDatumRow) bool {
	for _, colIdx := range g.lookupCols {
		if row[colIdx].IsNull() {
			return true
		}
	}
	return false
}

func (g *defaultSpanGenerator) generateSpans(rows []rowenc.EncDatumRow) (roachpb.Spans, error) {
	// This loop gets optimized to a runtime.mapclear call.
	for k := range g.keyToInputRowIndices {
		delete(g.keyToInputRowIndices, k)
	}
	// We maintain a map from index key to the corresponding input rows so we can
	// join the index results to the inputs.
	g.scratchSpans = g.scratchSpans[:0]
	for i, inputRow := range rows {
		if g.hasNullLookupColumn(inputRow) {
			continue
		}
		generatedSpan, containsNull, err := g.generateSpan(inputRow)
		if err != nil {
			return nil, err
		}
		if g.keyToInputRowIndices == nil {
			// Index join.
			g.scratchSpans = g.spanBuilder.MaybeSplitSpanIntoSeparateFamilies(
				g.scratchSpans, generatedSpan, len(g.lookupCols), containsNull)
		} else {
			inputRowIndices := g.keyToInputRowIndices[string(generatedSpan.Key)]
			if inputRowIndices == nil {
				g.scratchSpans = g.spanBuilder.MaybeSplitSpanIntoSeparateFamilies(
					g.scratchSpans, generatedSpan, len(g.lookupCols), containsNull)
			}
			g.keyToInputRowIndices[string(generatedSpan.Key)] = append(inputRowIndices, i)
		}
	}
	return g.scratchSpans, nil
}

type joinReaderStrategy interface {
	// getLookupRowsBatchSizeHint returns the size in bytes of the batch of lookup
	// rows.
	getLookupRowsBatchSizeHint() int64
	// processLookupRows consumes the rows the joinReader has buffered and should
	// return the lookup spans.
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
// the input ordering. This is more performant than joinReaderOrderingStrategy.
type joinReaderNoOrderingStrategy struct {
	*joinerBase
	defaultSpanGenerator
	isPartialJoin bool
	inputRows     []rowenc.EncDatumRow

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

func (s *joinReaderNoOrderingStrategy) processLookupRows(
	rows []rowenc.EncDatumRow,
) (roachpb.Spans, error) {
	s.inputRows = rows
	s.emitState.unmatchedInputRowIndicesInitialized = false
	return s.generateSpans(s.inputRows)
}

func (s *joinReaderNoOrderingStrategy) processLookedUpRow(
	_ context.Context, row rowenc.EncDatumRow, key roachpb.Key,
) (joinReaderState, error) {
	matchingInputRowIndices := s.keyToInputRowIndices[string(key)]
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
// join. It does not maintain the ordering.
type joinReaderIndexJoinStrategy struct {
	*joinerBase
	defaultSpanGenerator
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
// ordering. This is more expensive than joinReaderNoOrderingStrategy.
type joinReaderOrderingStrategy struct {
	*joinerBase
	defaultSpanGenerator
	isPartialJoin bool

	inputRows []rowenc.EncDatumRow

	// inputRowIdxToLookedUpRowIndices is a multimap from input row indices to
	// corresponding looked up row indices. It's populated in the
	// jrPerformingLookup state. For non partial joins (everything but semi/anti
	// join), the looked up rows are the rows that came back from the lookup
	// span for each input row, without checking for matches with respect to the
	// on-condition. For semi/anti join, we store at most one sentinel value,
	// indicating a matching lookup if it's present, since the right side of a
	// semi/anti join is not used.
	inputRowIdxToLookedUpRowIndices [][]int

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

func (s *joinReaderOrderingStrategy) processLookupRows(
	rows []rowenc.EncDatumRow,
) (roachpb.Spans, error) {
	// Maintain a map from input row index to the corresponding output rows. This
	// will allow us to preserve the order of the input in the face of multiple
	// input rows having the same lookup keyspan, or if we're doing an outer join
	// and we need to emit unmatched rows.
	if cap(s.inputRowIdxToLookedUpRowIndices) >= len(rows) {
		s.inputRowIdxToLookedUpRowIndices = s.inputRowIdxToLookedUpRowIndices[:len(rows)]
		for i := range s.inputRowIdxToLookedUpRowIndices {
			s.inputRowIdxToLookedUpRowIndices[i] = s.inputRowIdxToLookedUpRowIndices[i][:0]
		}
	} else {
		s.inputRowIdxToLookedUpRowIndices = make([][]int, len(rows))
	}

	s.inputRows = rows
	return s.generateSpans(s.inputRows)
}

func (s *joinReaderOrderingStrategy) processLookedUpRow(
	ctx context.Context, row rowenc.EncDatumRow, key roachpb.Key,
) (joinReaderState, error) {
	matchingInputRowIndices := s.keyToInputRowIndices[string(key)]
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
