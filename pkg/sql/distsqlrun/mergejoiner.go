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
	"context"
	"errors"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// mergeJoiner performs merge join, it has two input row sources with the same
// ordering on the columns that have equality constraints.
//
// It is guaranteed that the results preserve this ordering.
type mergeJoiner struct {
	joinerBase

	evalCtx *tree.EvalContext

	leftSource, rightSource RowSource
	leftRows, rightRows     []sqlbase.EncDatumRow
	leftIdx, rightIdx       int
	matchedRight            []bool
	matchedRightCount       int

	streamMerger streamMerger
}

var _ Processor = &mergeJoiner{}

func newMergeJoiner(
	flowCtx *FlowCtx,
	spec *MergeJoinerSpec,
	leftSource RowSource,
	rightSource RowSource,
	post *PostProcessSpec,
	output RowReceiver,
) (*mergeJoiner, error) {
	for i, c := range spec.LeftOrdering.Columns {
		if spec.RightOrdering.Columns[i].Direction != c.Direction {
			return nil, errors.New("Unmatched column orderings")
		}
	}

	m := &mergeJoiner{
		leftSource:  leftSource,
		rightSource: rightSource,
	}
	// TODO: Adapt MergeJoiner to new joinerBase constructor.
	err := m.joinerBase.init(flowCtx,
		leftSource.OutputTypes(), rightSource.OutputTypes(),
		spec.Type, spec.OnExpr, nil, nil, 0, post, output)
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
	if wg != nil {
		defer wg.Done()
	}

	ctx := log.WithLogTag(m.flowCtx.Ctx, "MergeJoiner", nil)
	ctx, span := processorSpan(ctx, "merge joiner")
	defer tracing.FinishSpan(span)
	log.VEventf(ctx, 2, "starting merge joiner run")

	cancelChecker := sqlbase.NewCancelChecker(ctx)
	m.evalCtx = m.flowCtx.NewEvalCtx()

	var err error
	for {
		if err = cancelChecker.Check(); err != nil {
			break
		}

		var row sqlbase.EncDatumRow
		row, err = m.nextRow(ctx)
		if row == nil || err != nil {
			break
		}

		var outRow sqlbase.EncDatumRow
		var status ConsumerStatus
		outRow, status, err = m.out.ProcessRow(ctx, row)
		if outRow != nil {
			if m.out.output.Push(outRow, ProducerMetadata{}) != NeedMoreRows {
				break
			}
			continue
		}
		if err != nil || status != NeedMoreRows {
			break
		}
	}

	DrainAndClose(ctx, m.out.output, err, m.leftSource, m.rightSource)
	log.VEventf(ctx, 2, "exiting merge joiner run")
}

func (m *mergeJoiner) nextRow(ctx context.Context) (sqlbase.EncDatumRow, error) {
	// The loops below form a restartable state machine that iterates over a batch
	// of rows from the left and right side of the join. The state machine
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
					return nil, err
				}
				if renderedRow != nil {
					m.matchedRightCount++
					if m.matchedRight != nil {
						m.matchedRight[ridx] = true
					}
					return renderedRow, nil
				}
			}

			// We've exhausted the right-side batch. Adjust the indexes for the next
			// row from the left-side of the batch.
			m.rightIdx = 0
			m.leftIdx++

			// If we didn't match any rows on the right-side of the batch and this is
			// a left or full outer join, emit an unmatched left-side row.
			if m.matchedRightCount == 0 && shouldEmitUnmatchedRow(leftSide, m.joinType) {
				return m.renderUnmatchedRow(lrow, leftSide), nil
			}

			m.matchedRightCount = 0
		}

		// We've exhausted the left-side batch. If this is a right or full outer
		// join (and thus matchedRight!=nil), emit unmatched right-side rows.
		if m.matchedRight != nil {
			for m.rightIdx < len(m.rightRows) {
				ridx := m.rightIdx
				m.rightIdx++
				if m.matchedRight[ridx] {
					continue
				}
				return m.renderUnmatchedRow(m.rightRows[ridx], rightSide), nil
			}

			m.matchedRight = nil
		}

		m.leftIdx, m.rightIdx = 0, 0
		if done, err := m.nextBatch(ctx); done {
			return nil, err
		}
	}
}

func (m *mergeJoiner) nextBatch(ctx context.Context) (done bool, _ error) {
	for {
		var meta *ProducerMetadata
		m.leftRows, m.rightRows, meta = m.streamMerger.NextBatch(m.evalCtx)
		if meta != nil {
			if meta.Err != nil {
				return true, meta.Err
			}
			_ = m.out.output.Push(nil /* row */, *meta)
			continue
		}
		if m.leftRows == nil && m.rightRows == nil {
			return true, nil
		}

		m.matchedRight = nil
		if shouldEmitUnmatchedRow(rightSide, m.joinType) {
			m.matchedRight = make([]bool, len(m.rightRows))
		}
		return false, nil
	}
}
