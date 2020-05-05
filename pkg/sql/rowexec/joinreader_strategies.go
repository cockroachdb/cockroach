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

	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type joinReaderStrategy interface {
	processLookupRows(rows []sqlbase.EncDatumRow)
	// processLookedUpRow processes a looked up row. A joinReaderState is returned
	// to indicate the next state to transition to. If jrPerformingLookup,
	// processLookedUpRow will be called again if the looked up rows have not been
	// exhausted. A transition to jrStateUnknown is unsupported, but if an error
	// is returned, the joinReader will transition to draining.
	processLookedUpRow(ctx context.Context, row sqlbase.EncDatumRow, matchingInputRowIndices []int) (joinReaderState, error)
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
		processingLookupRow     bool
		curUnmatchedInputRowIdx int
		// unmatchedInputRowIndices is used only when emitting unmatched rows after
		// processing lookup results. It is populated once when first emitting
		// unmatched rows.
		unmatchedInputRowIndices []int
		curInputRowIdx           int
		matchingInputRowIndices  []int
		lookedUpRow              sqlbase.EncDatumRow
	}
	toEmit []sqlbase.EncDatumRow
}

func (s *joinReaderNoOrderingStrategy) processLookupRows(rows []sqlbase.EncDatumRow) {
	s.inputRows = rows
	if cap(s.matched) < len(s.inputRows) {
		s.matched = make([]bool, len(s.inputRows))
	} else {
		s.matched = s.matched[:len(s.inputRows)]
		for i := range s.matched {
			s.matched[i] = false
		}
	}
}

func (s *joinReaderNoOrderingStrategy) processLookedUpRow(
	_ context.Context, row sqlbase.EncDatumRow, matchingInputRowIndices []int,
) (joinReaderState, error) {
	if s.isPartialJoin {
		// In the case of partial joins, only process input rows that have not been
		// matched yet. Make a copy of the matching input row indices to avoid
		// overwriting the caller's slice.
		if len(s.scratchMatchingInputRowIndices) != 0 {
			s.scratchMatchingInputRowIndices = s.scratchMatchingInputRowIndices[:0]
		}
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
	s.emitState.curInputRowIdx = 0
	return jrEmittingRows, nil
}

func (s *joinReaderNoOrderingStrategy) nextRowToEmit(
	_ context.Context,
) (sqlbase.EncDatumRow, joinReaderState, error) {
	if !s.emitState.processingLookupRow {
		// processLookedUpRow was not called before nextRowToEmit, which means that
		// the next unmatched row needs to be emitted.
		if !shouldEmitUnmatchedRow(leftSide, s.joinType) {
			// The joinType does not require the joiner to emit unmatched rows. Move
			// on to the next batch of lookup rows.
			return nil, jrReadingInput, nil
		}

		if len(s.matched) != 0 {
			if len(s.emitState.unmatchedInputRowIndices) != 0 {
				s.emitState.unmatchedInputRowIndices = s.emitState.unmatchedInputRowIndices[:0]
			}
			for inputRowIdx, m := range s.matched {
				if !m {
					s.emitState.unmatchedInputRowIndices = append(s.emitState.unmatchedInputRowIndices, inputRowIdx)
				}
			}
			s.matched = s.matched[:0]
			s.emitState.curUnmatchedInputRowIdx = 0
		}

		if s.emitState.curUnmatchedInputRowIdx >= len(s.emitState.unmatchedInputRowIndices) {
			// All unmatched rows have been emitted.
			return nil, jrReadingInput, nil
		}
		inputRow := s.inputRows[s.emitState.unmatchedInputRowIndices[s.emitState.curUnmatchedInputRowIdx]]
		s.emitState.curUnmatchedInputRowIdx++
		if !shouldIncludeRightColsInOutput(s.joinType) {
			return inputRow, jrEmittingRows, nil
		}
		return s.renderUnmatchedRow(inputRow, leftSide), jrEmittingRows, nil
	}

	for s.emitState.curInputRowIdx < len(s.emitState.matchingInputRowIndices) {
		inputRowIdx := s.emitState.matchingInputRowIndices[s.emitState.curInputRowIdx]
		s.emitState.curInputRowIdx++
		inputRow := s.inputRows[inputRowIdx]
		s.matched[inputRowIdx] = true

		// Render the output row, this also evaluates the ON condition.
		outputRow, err := s.render(inputRow, s.emitState.lookedUpRow)
		if err != nil {
			return nil, jrStateUnknown, err
		}
		if outputRow == nil {
			// This row failed the on condition, so remains unmatched.
			s.matched[inputRowIdx] = false
			continue
		}

		if !shouldIncludeRightColsInOutput(s.joinType) {
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

// joinReaderOrderingStrategy is a joinReaderStrategy that maintains the input
// ordering. This is more expensive than joinReaderNoOrderingStrategy.
type joinReaderOrderingStrategy struct {
	*joinerBase
	isPartialJoin bool

	inputRows []sqlbase.EncDatumRow

	// inputRowIdxToLookedUpRowIdx is a multimap from input row indices to
	// corresponding looked up row indices. It's populated in the
	// jrPerformingLookup state. For non partial joins (everything but semi/anti
	// join), the looked up rows are the rows that came back from the lookup
	// span for each input row, without checking for matches with respect to the
	// on-condition. For semi/anti join, we store at most one sentinel value,
	// indicating a matching lookup if it's present, since the right side of a
	// semi/anti join is not used.
	inputRowIdxToLookedUpRowIdx [][]int

	lookedUpRowIdx int
	lookedUpRows   rowcontainer.IndexedRowContainer

	// emitCursor contains information about where the next row to emit is within
	// inputRowIdxToLookedUpRowIdx.
	emitCursor struct {
		// inputRowIdx contains the index into inputRowIdxToLookedUpRowIdx that
		// we're about to emit.
		inputRowIdx int
		// outputRowIdx contains the index into the inputRowIdx'th row of
		// inputRowIdxToLookedUpRowIdx that we're about to emit.
		outputRowIdx int
		// seenMatch is true if there was a match at the current inputRowIdx. A
		// match means that there's no need to output an outer or anti join row.
		seenMatch bool
	}
}

func (s *joinReaderOrderingStrategy) processLookupRows(rows []sqlbase.EncDatumRow) {
	// Maintain a map from input row index to the corresponding output rows. This
	// will allow us to preserve the order of the input in the face of multiple
	// input rows having the same lookup keyspan, or if we're doing an outer join
	// and we need to emit unmatched rows.
	if cap(s.inputRowIdxToLookedUpRowIdx) >= len(rows) {
		s.inputRowIdxToLookedUpRowIdx = s.inputRowIdxToLookedUpRowIdx[:len(rows)]
		for i := range s.inputRowIdxToLookedUpRowIdx {
			s.inputRowIdxToLookedUpRowIdx[i] = s.inputRowIdxToLookedUpRowIdx[i][:0]
		}
	} else {
		s.inputRowIdxToLookedUpRowIdx = make([][]int, len(rows))
	}

	s.inputRows = rows
}

func (s *joinReaderOrderingStrategy) processLookedUpRow(
	ctx context.Context, row sqlbase.EncDatumRow, matchingInputRowIndices []int,
) (joinReaderState, error) {
	if !s.isPartialJoin {
		// Replace missing values with nulls to appease the row container.
		for i := range row {
			if row[i].IsUnset() {
				row[i].Datum = tree.DNull
			}
		}
		if err := s.lookedUpRows.AddRow(ctx, row); err != nil {
			return jrStateUnknown, err
		}
	}

	// Update our map from input rows to looked up rows.
	for _, inputRowIdx := range matchingInputRowIndices {
		if !s.isPartialJoin {
			s.inputRowIdxToLookedUpRowIdx[inputRowIdx] = append(
				s.inputRowIdxToLookedUpRowIdx[inputRowIdx], s.lookedUpRowIdx)
			continue
		}

		// During a SemiJoin or AntiJoin, we only output if we've seen no match
		// for this input row yet. Additionally, since we don't have to render
		// anything to output a Semi or Anti join match, we can evaluate our
		// on condition now and only buffer if we pass it.
		if len(s.inputRowIdxToLookedUpRowIdx[inputRowIdx]) == 0 {
			renderedRow, err := s.render(s.inputRows[inputRowIdx], row)
			if err != nil {
				return jrStateUnknown, err
			}
			if renderedRow == nil {
				// We failed our on-condition - don't buffer anything.
				continue
			}
			s.inputRowIdxToLookedUpRowIdx[inputRowIdx] = partialJoinSentinel
		}
	}
	s.lookedUpRowIdx++

	return jrPerformingLookup, nil
}

func (s *joinReaderOrderingStrategy) nextRowToEmit(
	ctx context.Context,
) (sqlbase.EncDatumRow, joinReaderState, error) {
	if s.emitCursor.inputRowIdx >= len(s.inputRowIdxToLookedUpRowIdx) {
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
	lookedUpRows := s.inputRowIdxToLookedUpRowIdx[s.emitCursor.inputRowIdx]
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

	lookedUpRow, err := s.lookedUpRows.GetRow(s.Ctx, lookedUpRowIdx)
	if err != nil {
		return nil, jrStateUnknown, err
	}
	outputRow, err := s.render(inputRow, lookedUpRow.(rowcontainer.IndexedRow).Row)
	if err != nil {
		return nil, jrStateUnknown, err
	}
	if outputRow != nil {
		s.emitCursor.seenMatch = true
	}
	return outputRow, jrEmittingRows, nil
}

func (s *joinReaderOrderingStrategy) spilled() bool {
	return s.lookedUpRows.(*rowcontainer.DiskBackedIndexedRowContainer).Spilled()
}

func (s *joinReaderOrderingStrategy) close(ctx context.Context) {
	if s.lookedUpRows != nil {
		s.lookedUpRows.Close(ctx)
	}
}
