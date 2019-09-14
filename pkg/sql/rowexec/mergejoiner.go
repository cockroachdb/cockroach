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
	"errors"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/opentracing/opentracing-go"
)

// mergeJoiner performs merge join, it has two input row sources with the same
// ordering on the columns that have equality constraints.
//
// It is guaranteed that the results preserve this ordering.
type mergeJoiner struct {
	execinfra.JoinerBase

	cancelChecker *sqlbase.CancelChecker

	leftSource, rightSource execinfra.RowSource
	leftRows, rightRows     []sqlbase.EncDatumRow
	leftIdx, rightIdx       int
	emitUnmatchedRight      bool
	matchedRight            util.FastIntSet
	matchedRightCount       int

	streamMerger streamMerger
}

var _ execinfra.Processor = &mergeJoiner{}
var _ execinfra.RowSource = &mergeJoiner{}

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
		leftSource:  leftSource,
		rightSource: rightSource,
	}

	if sp := opentracing.SpanFromContext(flowCtx.EvalCtx.Ctx()); sp != nil && tracing.IsRecording(sp) {
		m.leftSource = execinfra.NewInputStatCollector(m.leftSource)
		m.rightSource = execinfra.NewInputStatCollector(m.rightSource)
		m.FinishTrace = m.outputStatsToTrace
	}

	if err := m.JoinerBase.Init(
		m /* self */, flowCtx, processorID, leftSource.OutputTypes(), rightSource.OutputTypes(),
		spec.Type, spec.OnExpr, leftEqCols, rightEqCols, 0, post, output,
		execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{leftSource, rightSource},
			TrailingMetaCallback: func(context.Context) []execinfrapb.ProducerMetadata {
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
func (m *mergeJoiner) Start(ctx context.Context) context.Context {
	m.streamMerger.start(ctx)
	ctx = m.StartInternal(ctx, mergeJoinerProcName)
	m.cancelChecker = sqlbase.NewCancelChecker(ctx)
	return ctx
}

// Next is part of the Processor interface.
func (m *mergeJoiner) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
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

func (m *mergeJoiner) nextRow() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
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
				renderedRow, err := m.Render(lrow, m.rightRows[ridx])
				if err != nil {
					return nil, &execinfrapb.ProducerMetadata{Err: err}
				}
				if renderedRow != nil {
					m.matchedRightCount++
					if m.JoinType == sqlbase.LeftAntiJoin || m.JoinType == sqlbase.ExceptAllJoin {
						break
					}
					if m.emitUnmatchedRight {
						m.matchedRight.Add(ridx)
					}
					if m.JoinType == sqlbase.LeftSemiJoin || m.JoinType == sqlbase.IntersectAllJoin {
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
			if m.JoinType.IsSetOpJoin() {
				m.rightIdx = m.leftIdx
			}

			// If we didn't match any rows on the right-side of the batch and this is
			// a left outer join, full outer join, anti join, or EXCEPT ALL, emit an
			// unmatched left-side row.
			if m.matchedRightCount == 0 && execinfra.ShouldEmitUnmatchedRow(execinfra.LeftSide, m.JoinType) {
				return m.RenderUnmatchedRow(lrow, execinfra.LeftSide), nil
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
				return m.RenderUnmatchedRow(m.rightRows[ridx], execinfra.RightSide), nil
			}

			m.matchedRight = util.FastIntSet{}
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
		m.emitUnmatchedRight = execinfra.ShouldEmitUnmatchedRow(execinfra.RightSide, m.JoinType)
		m.leftIdx, m.rightIdx = 0, 0
	}
}

func (m *mergeJoiner) close() {
	if m.InternalClose() {
		ctx := m.Ctx
		m.streamMerger.close(ctx)
		m.MemMonitor.Stop(ctx)
	}
}

// ConsumerClosed is part of the RowSource interface.
func (m *mergeJoiner) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	m.close()
}

var _ execinfrapb.DistSQLSpanStats = &MergeJoinerStats{}

const mergeJoinerTagPrefix = "mergejoiner."

// Stats implements the SpanStats interface.
func (mjs *MergeJoinerStats) Stats() map[string]string {
	// statsMap starts off as the left input stats map.
	statsMap := mjs.LeftInputStats.Stats(mergeJoinerTagPrefix + "left.")
	rightInputStatsMap := mjs.RightInputStats.Stats(mergeJoinerTagPrefix + "right.")
	// Merge the two input maps.
	for k, v := range rightInputStatsMap {
		statsMap[k] = v
	}
	statsMap[mergeJoinerTagPrefix+execinfra.MaxMemoryTagSuffix] = humanizeutil.IBytes(mjs.MaxAllocatedMem)
	return statsMap
}

// StatsForQueryPlan implements the DistSQLSpanStats interface.
func (mjs *MergeJoinerStats) StatsForQueryPlan() []string {
	stats := append(
		mjs.LeftInputStats.StatsForQueryPlan("left "),
		mjs.RightInputStats.StatsForQueryPlan("right ")...,
	)
	return append(stats, fmt.Sprintf("%s: %s", execinfra.MaxMemoryQueryPlanSuffix, humanizeutil.IBytes(mjs.MaxAllocatedMem)))
}

// outputStatsToTrace outputs the collected mergeJoiner stats to the trace. Will
// fail silently if the mergeJoiner is not collecting stats.
func (m *mergeJoiner) outputStatsToTrace() {
	lis, ok := execinfra.GetInputStats(m.FlowCtx, m.leftSource)
	if !ok {
		return
	}
	ris, ok := execinfra.GetInputStats(m.FlowCtx, m.rightSource)
	if !ok {
		return
	}
	if sp := opentracing.SpanFromContext(m.Ctx); sp != nil {
		tracing.SetSpanStats(
			sp,
			&MergeJoinerStats{
				LeftInputStats:  lis,
				RightInputStats: ris,
				MaxAllocatedMem: m.MemMonitor.MaximumBytes(),
			},
		)
	}
}
