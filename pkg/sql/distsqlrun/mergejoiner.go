// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package distsqlrun

import (
	"errors"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// mergeJoiner performs merge join, it has two input row sources with the same
// ordering on the columns that have equality constraints.
//
// It is guaranteed that the results preserve this ordering.
type mergeJoiner struct {
	joinerBase

	evalCtx       *tree.EvalContext
	cancelChecker *sqlbase.CancelChecker

	leftSource, rightSource RowSource
	leftRows, rightRows     []sqlbase.EncDatumRow
	leftIdx, rightIdx       int
	emitUnmatchedRight      bool
	matchedRight            util.FastIntSet
	matchedRightCount       int

	streamMerger streamMerger
}

var _ Processor = &mergeJoiner{}
var _ RowSource = &mergeJoiner{}

func newMergeJoiner(
	flowCtx *FlowCtx,
	spec *MergeJoinerSpec,
	leftSource RowSource,
	rightSource RowSource,
	post *PostProcessSpec,
	output RowReceiver,
) (*mergeJoiner, error) {
	leftEqCols := make([]uint32, 0, len(spec.LeftOrdering.Columns))
	rightEqCols := make([]uint32, 0, len(spec.RightOrdering.Columns))
	for i, c := range spec.LeftOrdering.Columns {
		if spec.RightOrdering.Columns[i].Direction != c.Direction {
			return nil, errors.New("Unmatched column orderings")
		}
		leftEqCols = append(leftEqCols, c.ColIdx)
		rightEqCols = append(rightEqCols, spec.RightOrdering.Columns[i].ColIdx)
	}

	m := &mergeJoiner{
		leftSource:  leftSource,
		rightSource: rightSource,
	}

	err := m.joinerBase.init(flowCtx,
		leftSource.OutputTypes(), rightSource.OutputTypes(),
		spec.Type, spec.OnExpr, leftEqCols, rightEqCols, 0, post, output)
	if err != nil {
		return nil, err
	}

	m.streamMerger, err = makeStreamMerger(
		leftSource,
		convertToColumnOrdering(spec.LeftOrdering),
		rightSource,
		convertToColumnOrdering(spec.RightOrdering),
		spec.NullEquality,
	)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// Run is part of the processor interface.
func (m *mergeJoiner) Run(wg *sync.WaitGroup) {
	if m.out.output == nil {
		panic("mergeJoiner output not initialized for emitting rows")
	}
	Run(m.flowCtx.Ctx, m, m.out.output)
	if wg != nil {
		wg.Done()
	}
}

func (m *mergeJoiner) close() {
	if !m.closed {
		log.VEventf(m.ctx, 2, "exiting merge joiner run")
	}
	if m.internalClose() {
		m.leftSource.ConsumerClosed()
		m.rightSource.ConsumerClosed()
	}
}

// producerMeta constructs the ProducerMetadata after consumption of rows has
// terminated, either due to being indicated by the consumer, or because the
// processor ran out of rows or encountered an error. It is ok for err to be
// nil indicating that we're done producing rows even though no error occurred.
func (m *mergeJoiner) producerMeta(err error) *ProducerMetadata {
	var meta *ProducerMetadata
	if !m.closed {
		if err != nil {
			meta = &ProducerMetadata{Err: err}
		} else if trace := getTraceData(m.ctx); trace != nil {
			meta = &ProducerMetadata{TraceData: trace}
		}
		// We need to close as soon as we send producer metadata as we're done
		// sending rows. The consumer is allowed to not call ConsumerDone().
		m.close()
	}
	return meta
}

func (m *mergeJoiner) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	if m.maybeStart("merge joiner", "MergeJoiner") {
		m.evalCtx = m.flowCtx.NewEvalCtx()
		m.cancelChecker = sqlbase.NewCancelChecker(m.ctx)
		log.VEventf(m.ctx, 2, "starting merge joiner run")
	}

	if m.closed {
		return nil, m.producerMeta(nil /* err */)
	}

	for {
		row, meta := m.nextRow()
		if meta != nil {
			return nil, meta
		}
		if row == nil {
			return nil, m.producerMeta(nil /* err */)
		}

		outRow, status, err := m.out.ProcessRow(m.ctx, row)
		if err != nil {
			return nil, m.producerMeta(err)
		}
		switch status {
		case NeedMoreRows:
			if outRow == nil && err == nil {
				continue
			}
		case DrainRequested:
			m.leftSource.ConsumerDone()
			m.rightSource.ConsumerDone()
			continue
		}
		return outRow, nil
	}
}

func (m *mergeJoiner) nextRow() (sqlbase.EncDatumRow, *ProducerMetadata) {
	// The loops below form a restartable state machine that iterates over a
	// batch of rows from the left and right side of the join. The state machine
	// returns a result for every row that should be output.

	for {
		for m.leftIdx < len(m.leftRows) {
			// We have unprocessed rows from the left-side batch.
			lrow := m.leftRows[m.leftIdx]
			for m.rightIdx < len(m.rightRows) {
				// We have unprocessed rows from the right-side batch.
				ridx := m.rightIdx
				m.rightIdx++
				renderedRow, err := m.render(lrow, m.rightRows[ridx])
				if err != nil {
					return nil, &ProducerMetadata{Err: err}
				}
				if renderedRow != nil {
					m.matchedRightCount++
					if m.joinType == sqlbase.LeftAntiJoin || m.joinType == sqlbase.ExceptAllJoin {
						break
					}
					if m.emitUnmatchedRight {
						m.matchedRight.Add(ridx)
					}
					if m.joinType == sqlbase.LeftSemiJoin || m.joinType == sqlbase.IntersectAllJoin {
						// Semi-joins and INTERSECT ALL only need to know if there is at
						// least one match, so can skip the rest of the right rows.
						m.rightIdx = len(m.rightRows)
					}
					return renderedRow, nil
				}
			}

			// Perform the cancellation check. We don't perform this on every row,
			// but once for every iteration through the right-side batch.
			if err := m.cancelChecker.Check(); err != nil {
				return nil, &ProducerMetadata{Err: err}
			}

			// We've exhausted the right-side batch. Adjust the indexes for the next
			// row from the left-side of the batch.
			m.leftIdx++
			m.rightIdx = 0

			// For INTERSECT ALL and EXCEPT ALL, adjust rightIdx to skip all
			// previously matched rows on the next right-side iteration, since we
			// don't want to match them again.
			if isSetOpJoin(m.joinType) {
				m.rightIdx = m.leftIdx
			}

			// If we didn't match any rows on the right-side of the batch and this is
			// a left outer join, full outer join, anti join, or EXCEPT ALL, emit an
			// unmatched left-side row.
			if m.matchedRightCount == 0 && shouldEmitUnmatchedRow(leftSide, m.joinType) {
				return m.renderUnmatchedRow(lrow, leftSide), nil
			}

			m.matchedRightCount = 0
		}

		// We've exhausted the left-side batch. If this is a right or full outer
		// join (and thus matchedRight!=nil), emit unmatched right-side rows.
		if m.emitUnmatchedRight {
			for m.rightIdx < len(m.rightRows) {
				ridx := m.rightIdx
				m.rightIdx++
				if m.matchedRight.Contains(ridx) {
					continue
				}
				return m.renderUnmatchedRow(m.rightRows[ridx], rightSide), nil
			}

			m.matchedRight = util.FastIntSet{}
			m.emitUnmatchedRight = false
		}

		// Retrieve the next batch of rows to process.
		var meta *ProducerMetadata
		// TODO(paul): Investigate (with benchmarks) whether or not it's
		// worthwhile to only buffer one row from the right stream per batch
		// for semi-joins.
		m.leftRows, m.rightRows, meta = m.streamMerger.NextBatch(m.evalCtx)
		if meta != nil {
			return nil, meta
		}
		if m.leftRows == nil && m.rightRows == nil {
			return nil, nil
		}

		// Prepare for processing the next batch.
		m.emitUnmatchedRight = shouldEmitUnmatchedRow(rightSide, m.joinType)
		m.leftIdx, m.rightIdx = 0, 0
	}
}

// ConsumerDone is part of the RowSource interface.
func (m *mergeJoiner) ConsumerDone() {
	m.leftSource.ConsumerDone()
	m.rightSource.ConsumerDone()
}

// ConsumerClosed is part of the RowSource interface.
func (m *mergeJoiner) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	m.close()
}
