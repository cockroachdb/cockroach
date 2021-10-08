// Copyright 2016 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/cancelchecker"
	"github.com/cockroachdb/cockroach/pkg/util/optional"
	"github.com/cockroachdb/errors"
)

// mergeJoiner performs merge join, it has two input row sources with the same
// ordering on the columns that have equality constraints.
//
// It is guaranteed that the results preserve this ordering.
type mergeJoiner struct {
	joinerBase

	cancelChecker *cancelchecker.CancelChecker

	leftSource, rightSource execinfra.RowSource
	leftRows, rightRows     []rowenc.EncDatumRow
	leftIdx, rightIdx       int
	trackMatchedRight       bool
	emitUnmatchedRight      bool
	matchedRight            util.FastIntSet
	matchedRightCount       int

	streamMerger streamMerger
}

var _ execinfra.Processor = &mergeJoiner{}
var _ execinfra.RowSource = &mergeJoiner{}
var _ execinfra.OpNode = &mergeJoiner{}

const mergeJoinerProcName = "merge joiner"

func newMergeJoiner(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.MergeJoinerSpec,
	leftSource execinfra.RowSource,
	rightSource execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (*mergeJoiner, error) {
	leftEqCols := make([]uint32, 0, len(spec.LeftOrdering.Columns))
	rightEqCols := make([]uint32, 0, len(spec.RightOrdering.Columns))
	for i, c := range spec.LeftOrdering.Columns {
		if spec.RightOrdering.Columns[i].Direction != c.Direction {
			return nil, errors.New("unmatched column orderings")
		}
		leftEqCols = append(leftEqCols, c.ColIdx)
		rightEqCols = append(rightEqCols, spec.RightOrdering.Columns[i].ColIdx)
	}

	m := &mergeJoiner{
		leftSource:        leftSource,
		rightSource:       rightSource,
		trackMatchedRight: shouldEmitUnmatchedRow(rightSide, spec.Type) || spec.Type == descpb.RightSemiJoin,
	}

	if execinfra.ShouldCollectStats(flowCtx.EvalCtx.Ctx(), flowCtx) {
		m.leftSource = newInputStatCollector(m.leftSource)
		m.rightSource = newInputStatCollector(m.rightSource)
		m.ExecStatsForTrace = m.execStatsForTrace
	}

	if err := m.joinerBase.init(
		m /* self */, flowCtx, processorID, leftSource.OutputTypes(), rightSource.OutputTypes(),
		spec.Type, spec.OnExpr, leftEqCols, rightEqCols, false, /* outputContinuationColumn */
		post, output,
		execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{leftSource, rightSource},
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
				m.close()
				return nil
			},
		},
	); err != nil {
		return nil, err
	}

	m.MemMonitor = execinfra.NewMonitor(flowCtx.EvalCtx.Ctx(), flowCtx.EvalCtx.Mon, "mergejoiner-mem")

	var err error
	m.streamMerger, err = makeStreamMerger(
		m.leftSource,
		execinfrapb.ConvertToColumnOrdering(spec.LeftOrdering),
		m.rightSource,
		execinfrapb.ConvertToColumnOrdering(spec.RightOrdering),
		spec.NullEquality,
		m.MemMonitor,
	)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// Start is part of the RowSource interface.
func (m *mergeJoiner) Start(ctx context.Context) {
	ctx = m.StartInternal(ctx, mergeJoinerProcName)
	m.streamMerger.start(ctx)
	m.cancelChecker = cancelchecker.NewCancelChecker(ctx)
}

// Next is part of the Processor interface.
func (m *mergeJoiner) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for m.State == execinfra.StateRunning {
		row, meta := m.nextRow()
		if meta != nil {
			if meta.Err != nil {
				m.MoveToDraining(nil /* err */)
			}
			return nil, meta
		}
		if row == nil {
			m.MoveToDraining(nil /* err */)
			break
		}

		if outRow := m.ProcessRowHelper(row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, m.DrainHelper()
}

func (m *mergeJoiner) nextRow() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
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
				if (m.joinType == descpb.RightSemiJoin || m.joinType == descpb.RightAntiJoin) && m.matchedRight.Contains(ridx) {
					// Right semi/anti joins only need to know whether the
					// right row has a match, and we already know that for
					// ridx. Furthermore, we have already emitted this row in
					// case of right semi, so we need to skip it for
					// correctness as well.
					continue
				}
				renderedRow, err := m.render(lrow, m.rightRows[ridx])
				if err != nil {
					return nil, &execinfrapb.ProducerMetadata{Err: err}
				}
				if renderedRow != nil {
					m.matchedRightCount++
					if m.trackMatchedRight {
						m.matchedRight.Add(ridx)
					}
					if m.joinType == descpb.LeftAntiJoin || m.joinType == descpb.ExceptAllJoin {
						// We know that the current left row has a match and is
						// not included in the output, so we can stop
						// processing the right-side batch.
						break
					}
					if m.joinType == descpb.RightAntiJoin {
						// We don't emit the current right row because it has a
						// match on the left, so we move onto the next right
						// row.
						continue
					}
					if m.joinType == descpb.LeftSemiJoin || m.joinType == descpb.IntersectAllJoin {
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
				return nil, &execinfrapb.ProducerMetadata{Err: err}
			}

			// We've exhausted the right-side batch. Adjust the indexes for the next
			// row from the left-side of the batch.
			m.leftIdx++
			m.rightIdx = 0

			// For INTERSECT ALL and EXCEPT ALL, adjust rightIdx to skip all
			// previously matched rows on the next right-side iteration, since we
			// don't want to match them again.
			if m.joinType.IsSetOpJoin() {
				m.rightIdx = m.leftIdx
			}

			// If we didn't match any rows on the right-side of the batch and this is
			// a left outer join, full outer join, left anti join, or EXCEPT ALL, emit an
			// unmatched left-side row.
			if m.matchedRightCount == 0 && shouldEmitUnmatchedRow(leftSide, m.joinType) {
				return m.renderUnmatchedRow(lrow, leftSide), nil
			}

			m.matchedRightCount = 0
		}

		// We've exhausted the left-side batch. If this is a right/full outer
		// or right anti join, emit unmatched right-side rows.
		if m.emitUnmatchedRight {
			for m.rightIdx < len(m.rightRows) {
				ridx := m.rightIdx
				m.rightIdx++
				if m.matchedRight.Contains(ridx) {
					continue
				}
				return m.renderUnmatchedRow(m.rightRows[ridx], rightSide), nil
			}
			m.emitUnmatchedRight = false
		}

		// Retrieve the next batch of rows to process.
		var meta *execinfrapb.ProducerMetadata
		// TODO(paul): Investigate (with benchmarks) whether or not it's
		// worthwhile to only buffer one row from the right stream per batch
		// for semi-joins.
		m.leftRows, m.rightRows, meta = m.streamMerger.NextBatch(m.Ctx, m.EvalCtx)
		if meta != nil {
			return nil, meta
		}
		if m.leftRows == nil && m.rightRows == nil {
			return nil, nil
		}

		// Prepare for processing the next batch.
		m.emitUnmatchedRight = shouldEmitUnmatchedRow(rightSide, m.joinType)
		m.leftIdx, m.rightIdx = 0, 0
		if m.trackMatchedRight {
			m.matchedRight = util.FastIntSet{}
		}
	}
}

func (m *mergeJoiner) close() {
	if m.InternalClose() {
		m.streamMerger.close(m.Ctx)
		m.MemMonitor.Stop(m.Ctx)
	}
}

// ConsumerClosed is part of the RowSource interface.
func (m *mergeJoiner) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	m.close()
}

// execStatsForTrace implements ProcessorBase.ExecStatsForTrace.
func (m *mergeJoiner) execStatsForTrace() *execinfrapb.ComponentStats {
	lis, ok := getInputStats(m.leftSource)
	if !ok {
		return nil
	}
	ris, ok := getInputStats(m.rightSource)
	if !ok {
		return nil
	}
	return &execinfrapb.ComponentStats{
		Inputs: []execinfrapb.InputStats{lis, ris},
		Exec: execinfrapb.ExecStats{
			MaxAllocatedMem: optional.MakeUint(uint64(m.MemMonitor.MaximumBytes())),
		},
		Output: m.OutputHelper.Stats(),
	}
}

// ChildCount is part of the execinfra.OpNode interface.
func (m *mergeJoiner) ChildCount(verbose bool) int {
	if _, ok := m.leftSource.(execinfra.OpNode); ok {
		if _, ok := m.rightSource.(execinfra.OpNode); ok {
			return 2
		}
	}
	return 0
}

// Child is part of the execinfra.OpNode interface.
func (m *mergeJoiner) Child(nth int, verbose bool) execinfra.OpNode {
	switch nth {
	case 0:
		if n, ok := m.leftSource.(execinfra.OpNode); ok {
			return n
		}
		panic("left input to mergeJoiner is not an execinfra.OpNode")
	case 1:
		if n, ok := m.rightSource.(execinfra.OpNode); ok {
			return n
		}
		panic("right input to mergeJoiner is not an execinfra.OpNode")
	default:
		panic(errors.AssertionFailedf("invalid index %d", nth))
	}
}
