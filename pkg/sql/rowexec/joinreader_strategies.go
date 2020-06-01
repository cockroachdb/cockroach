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
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/span"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type defaultSpanGenerator struct {
	spanBuilder *span.Builder
	numKeyCols  int
	lookupCols  []uint32

	indexKeyRow          sqlbase.EncDatumRow
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
	row sqlbase.EncDatumRow,
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

func (g *defaultSpanGenerator) hasNullLookupColumn(row sqlbase.EncDatumRow) bool {
	for _, colIdx := range g.lookupCols {
		if row[colIdx].IsNull() {
			return true
		}
	}
	return false
}

func (g *defaultSpanGenerator) generateSpans(rows []sqlbase.EncDatumRow) (roachpb.Spans, error) {
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
		inputRowIndices := g.keyToInputRowIndices[string(generatedSpan.Key)]
		if inputRowIndices == nil {
			g.scratchSpans = g.spanBuilder.MaybeSplitSpanIntoSeparateFamilies(
				g.scratchSpans, generatedSpan, len(g.lookupCols), containsNull)
		}
		g.keyToInputRowIndices[string(generatedSpan.Key)] = append(inputRowIndices, i)
	}
	return g.scratchSpans, nil
}

type joinReaderStrategy interface {
	// getLookupRowsBatchSizeHint returns the size in bytes of the batch of lookup
	// rows.
	getLookupRowsBatchSizeHint() int64
	// processLookupRows consumes the rows the joinReader has buffered and should
	// return the lookup spans.
	processLookupRows(rows []sqlbase.EncDatumRow) (roachpb.Spans, error)
	// processLookedUpRow processes a looked up row. A joinReaderState is returned
	// to indicate the next state to transition to. If this next state is
	// jrPerformingLookup, processLookedUpRow will be called again if the looked
	// up rows have not been exhausted. A transition to jrStateUnknown is
	// unsupported, but if an error is returned, the joinReader will transition
	// to draining.
	processLookedUpRow(ctx context.Context, row sqlbase.EncDatumRow, key roachpb.Key) (joinReaderState, error)
	// prepareToEmit informs the strategy implementation that all looked up rows
	// have been read, and that it should prepare for calls to nextRowToEmit.
	prepareToEmit(ctx context.Context)
	// nextRowToEmit gets the next row to emit from the strategy. An accompanying
	// joinReaderState is also returned, indicating a state to transition to after
	// emitting this row. A transition to jrStateUnknown is unsupported, but if an
	// error is returned, the joinReader will transition to draining.
	nextRowToEmit(ctx context.Context) (sqlbase.EncDatumRow, joinReaderState, error)
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
	inputRows     []sqlbase.EncDatumRow
	// matched[i] specifies whether inputRows[i] had a match.
	matched []bool

	scratchMatchingInputRowIndices []int

	emitState struct {
		// processingLookupRow is an explicit boolean that specifies whether the
		// strategy is currently processing a match. This is set to true in
		// processLookedUpRow and causes nextRowToEmit to process the data in
		// emitState. If set to false, the strategy determines in nextRowToEmit
		// that no more looked up rows need processing, so unmatched input rows need
		// to be emitted.
		processingLookupRow            bool
		unmatchedInputRowIndicesCursor int
		// unmatchedInputRowIndices is used only when emitting unmatched rows after
		// processing lookup results. It is populated once when first emitting
		// unmatched rows.
		unmatchedInputRowIndices      []int
		matchingInputRowIndicesCursor int
		matchingInputRowIndices       []int
		lookedUpRow                   sqlbase.EncDatumRow
	}
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
	rows []sqlbase.EncDatumRow,
) (roachpb.Spans, error) {
	s.inputRows = rows
	if cap(s.matched) < len(s.inputRows) {
		s.matched = make([]bool, len(s.inputRows))
	} else {
		s.matched = s.matched[:len(s.inputRows)]
		for i := range s.matched {
			s.matched[i] = false
		}
	}
	return s.generateSpans(s.inputRows)
}

func (s *joinReaderNoOrderingStrategy) processLookedUpRow(
	_ context.Context, row sqlbase.EncDatumRow, key roachpb.Key,
) (joinReaderState, error) {
	matchingInputRowIndices := s.keyToInputRowIndices[string(key)]
	if s.isPartialJoin {
		// In the case of partial joins, only process input rows that have not been
		// matched yet. Make a copy of the matching input row indices to avoid
		// overwriting the caller's slice.
		s.scratchMatchingInputRowIndices = s.scratchMatchingInputRowIndices[:0]
		for _, inputRowIdx := range matchingInputRowIndices {
			if !s.matched[inputRowIdx] {
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
) (sqlbase.EncDatumRow, joinReaderState, error) {
	if !s.emitState.processingLookupRow {
		// processLookedUpRow was not called before nextRowToEmit, which means that
		// the next unmatched row needs to be processed.
		if !shouldEmitUnmatchedRow(leftSide, s.joinType) {
			// The joinType does not require the joiner to emit unmatched rows. Move
			// on to the next batch of lookup rows.
			return nil, jrReadingInput, nil
		}

		if len(s.matched) != 0 {
			s.emitState.unmatchedInputRowIndices = s.emitState.unmatchedInputRowIndices[:0]
			for inputRowIdx, m := range s.matched {
				if !m {
					s.emitState.unmatchedInputRowIndices = append(s.emitState.unmatchedInputRowIndices, inputRowIdx)
				}
			}
			s.matched = s.matched[:0]
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

		// Render the output row, this also evaluates the ON condition.
		outputRow, err := s.render(inputRow, s.emitState.lookedUpRow)
		if err != nil {
			return nil, jrStateUnknown, err
		}
		if outputRow == nil {
			// This row failed the ON condition, so it remains unmatched.
			continue
		}

		s.matched[inputRowIdx] = true
		if !s.joinType.ShouldIncludeRightColsInOutput() {
			if s.joinType == sqlbase.LeftAntiJoin {
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

	inputRows []sqlbase.EncDatumRow

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
		// seenMatch is true if there was a match at the current inputRowIdx. A
		// match means that there's no need to output an outer or anti join row.
		seenMatch bool
	}
}

func (s *joinReaderOrderingStrategy) getLookupRowsBatchSizeHint() int64 {
	// TODO(asubiotto): Eventually we might want to adjust this batch size
	//  dynamically based on whether the result row container spilled or not.
	return 10 << 10 /* 10 KiB */
}

func (s *joinReaderOrderingStrategy) processLookupRows(
	rows []sqlbase.EncDatumRow,
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
	ctx context.Context, row sqlbase.EncDatumRow, key roachpb.Key,
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
		// on condition now and only buffer if we pass it.
		if len(s.inputRowIdxToLookedUpRowIndices[inputRowIdx]) == 0 {
			renderedRow, err := s.render(s.inputRows[inputRowIdx], row)
			if err != nil {
				return jrStateUnknown, err
			}
			if renderedRow == nil {
				// We failed our on-condition - don't buffer anything.
				continue
			}
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
) (sqlbase.EncDatumRow, joinReaderState, error) {
	if s.emitCursor.inputRowIdx >= len(s.inputRowIdxToLookedUpRowIndices) {
		log.VEventf(ctx, 1, "done emitting rows")
		// Ready for another input batch. Reset state.
		s.emitCursor.outputRowIdx = 0
		s.emitCursor.inputRowIdx = 0
		s.emitCursor.seenMatch = false
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
		s.emitCursor.inputRowIdx++
		s.emitCursor.outputRowIdx = 0
		seenMatch := s.emitCursor.seenMatch
		s.emitCursor.seenMatch = false
		if !seenMatch {
			switch s.joinType {
			case sqlbase.LeftOuterJoin:
				// An outer-join non-match means we emit the input row with NULLs for
				// the right side (if it passes the ON-condition).
				if renderedRow := s.renderUnmatchedRow(inputRow, leftSide); renderedRow != nil {
					return renderedRow, jrEmittingRows, nil
				}
			case sqlbase.LeftAntiJoin:
				// An anti-join non-match means we emit the input row.
				return inputRow, jrEmittingRows, nil
			}
		}
		return nil, jrEmittingRows, nil
	}

	lookedUpRowIdx := lookedUpRows[s.emitCursor.outputRowIdx]
	s.emitCursor.outputRowIdx++
	switch s.joinType {
	case sqlbase.LeftSemiJoin:
		// A semi-join match means we emit our input row.
		s.emitCursor.seenMatch = true
		return inputRow, jrEmittingRows, nil
	case sqlbase.LeftAntiJoin:
		// An anti-join match means we emit nothing.
		s.emitCursor.seenMatch = true
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
		s.emitCursor.seenMatch = true
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
