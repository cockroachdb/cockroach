// Copyright 2019 The Cockroach Authors.
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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/opentracing/opentracing-go"
)

// mergeHashJoiner performs merge hash join, it has two input row sources with
// the same ordering on the subset of columns that have equality constraints.
type mergeHashJoiner struct {
	execinfra.JoinerBase

	cancelChecker *sqlbase.CancelChecker

	leftSource, rightSource         execinfra.RowSource
	leftRows, rightRows             []sqlbase.EncDatumRow
	leftHashEqCols, rightHashEqCols []uint32
	leftProbeIdx                    int
	// TODO(yuzefovich): be smarter about choosing from which side to build the
	// hash table.
	hashTable rowcontainer.HashRowContainer
	rightIter rowcontainer.RowMarkerIterator

	// nullEquality indicates that NULL = NULL should be considered true. Used
	// for INTERSECT and EXCEPT.
	// TODO(yuzefovich): use nullEquality.
	nullEquality bool

	streamMerger streamMerger
}

var _ execinfra.Processor = &mergeHashJoiner{}
var _ execinfra.RowSource = &mergeHashJoiner{}

const mergeHashJoinerProcName = "merge hash joiner"

func newMergeHashJoiner(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.MergeHashJoinerSpec,
	leftSource execinfra.RowSource,
	rightSource execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (*mergeHashJoiner, error) {
	m := &mergeHashJoiner{
		leftSource:      leftSource,
		rightSource:     rightSource,
		leftHashEqCols:  getHashEqCols(spec.LeftEqColumns, spec.LeftOrdering.Columns),
		rightHashEqCols: getHashEqCols(spec.RightEqColumns, spec.RightOrdering.Columns),
		nullEquality:    spec.NullEquality,
	}

	if sp := opentracing.SpanFromContext(flowCtx.EvalCtx.Ctx()); sp != nil && tracing.IsRecording(sp) {
		m.leftSource = execinfra.NewInputStatCollector(m.leftSource)
		m.rightSource = execinfra.NewInputStatCollector(m.rightSource)
		m.FinishTrace = m.outputStatsToTrace
	}

	if err := m.JoinerBase.Init(
		m /* self */, flowCtx, processorID, leftSource.OutputTypes(),
		rightSource.OutputTypes(),
		spec.Type, spec.OnExpr, spec.LeftEqColumns, spec.RightEqColumns,
		0 /* numMergedColumns */, post, output,
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

	m.MemMonitor = execinfra.NewMonitor(
		flowCtx.EvalCtx.Ctx(),
		flowCtx.EvalCtx.Mon,
		"mergehashjoiner-mem",
	)

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

	mrc := &rowcontainer.MemRowContainer{}
	mrc.Init(nil, m.rightSource.OutputTypes(), m.EvalCtx)
	rc := rowcontainer.MakeHashMemRowContainer(mrc)
	m.hashTable = &rc
	// TODO(yuzefovich): set shouldMark depending on the join type.
	if err := m.hashTable.Init(
		m.Ctx,
		false, /* shouldMark */
		m.rightSource.OutputTypes(),
		m.rightHashEqCols,
		m.nullEquality,
	); err != nil {
		return nil, err
	}

	return m, nil
}

func getHashEqCols(eqCols []uint32, ordCols []execinfrapb.Ordering_Column) []uint32 {
	hashEqCols := make([]uint32, 0, len(eqCols)-len(ordCols))
	for _, eqCol := range eqCols {
		isOrderingCol := false
		for _, ordCol := range ordCols {
			if eqCol == ordCol.ColIdx {
				isOrderingCol = true
				break
			}
		}
		if !isOrderingCol {
			hashEqCols = append(hashEqCols, eqCol)
		}
	}
	return hashEqCols
}

// Start is part of the RowSource interface.
func (m *mergeHashJoiner) Start(ctx context.Context) context.Context {
	m.streamMerger.start(ctx)
	ctx = m.StartInternal(ctx, mergeHashJoinerProcName)
	m.cancelChecker = sqlbase.NewCancelChecker(ctx)
	return ctx
}

// Next is part of the Processor interface.
func (m *mergeHashJoiner) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
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

func (m *mergeHashJoiner) nextRow() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	var err error
	for {
		for m.leftProbeIdx < len(m.leftRows) {
			lrow := m.leftRows[m.leftProbeIdx]
			i := m.rightIter
			if ok, err := i.Valid(); err != nil {
				return nil, &execinfrapb.ProducerMetadata{Err: err}
			} else if ok {
				rrow, err := i.Row()
				if err != nil {
					return nil, &execinfrapb.ProducerMetadata{Err: err}
				}
				i.Next()
				res, err := m.Render(lrow, rrow)
				if err != nil {
					return nil, &execinfrapb.ProducerMetadata{Err: err}
				}
				if res == nil {
					continue
				}
				return res, nil
			}

			m.leftProbeIdx++
			// TODO(yuzefovich): change this for non-INNER joins.
			m.leftProbeIdx = findRowWithoutNullsInEqCols(
				m.leftRows, m.leftProbeIdx, m.leftHashEqCols,
			)
			if m.leftProbeIdx == len(m.leftRows) {
				break
			}
			if err := m.rightIter.Reset(m.Ctx, m.leftRows[m.leftProbeIdx]); err != nil {
				return nil, &execinfrapb.ProducerMetadata{Err: err}
			}
			m.rightIter.Rewind()
		}

		// Perform the cancellation check. We don't perform this on every row,
		// but once for every group.
		if err = m.cancelChecker.Check(); err != nil {
			return nil, &execinfrapb.ProducerMetadata{Err: err}
		}

		// Retrieve the next batch of rows to process.
		m.leftRows, m.rightRows = nil, nil
		var meta *execinfrapb.ProducerMetadata
		// TODO(yuzefovich): change this for non-INNER joins.
		for m.leftRows == nil || m.rightRows == nil {
			m.leftRows, m.rightRows, meta = m.streamMerger.NextBatch(m.Ctx, m.EvalCtx)
			if meta != nil {
				return nil, meta
			}
			if m.leftRows == nil && m.rightRows == nil {
				return nil, nil
			}
		}

		// TODO(yuzefovich): change this for non-INNER joins.
		m.leftProbeIdx = findRowWithoutNullsInEqCols(
			m.leftRows, 0 /* startIdx */, m.leftHashEqCols,
		)
		if m.leftProbeIdx == len(m.leftRows) {
			continue
		}

		if err = m.hashTable.UnsafeReset(m.Ctx); err != nil {
			return nil, &execinfrapb.ProducerMetadata{Err: err}
		}
		rIdx := 0
		for {
			// Skip all rows that have NULLs in at least one equality column.
			// TODO(yuzefovich): change this for non-INNER joins.
			rIdx = findRowWithoutNullsInEqCols(m.rightRows, rIdx, m.rightHashEqCols)
			if rIdx == len(m.rightRows) {
				break
			}
			if err = m.hashTable.AddRow(m.Ctx, m.rightRows[rIdx]); err != nil {
				return nil, &execinfrapb.ProducerMetadata{Err: err}
			}
			rIdx++
		}
		if m.rightIter == nil {
			m.rightIter, err = m.hashTable.NewBucketIterator(
				m.Ctx, m.leftRows[m.leftProbeIdx], m.leftHashEqCols,
			)
			if err != nil {
				return nil, &execinfrapb.ProducerMetadata{Err: err}
			}
		} else {
			if err := m.rightIter.Reset(m.Ctx, m.leftRows[m.leftProbeIdx]); err != nil {
				return nil, &execinfrapb.ProducerMetadata{Err: err}
			}
		}
		m.rightIter.Rewind()
	}
}

// findRowWithoutNullsInEqCols searches for the first row among 'rows' starting
// from position 'startIdx' that doesn't have NULL values in the equality
// columns. It returns the position of such row or len(rows) if the search is
// not successful.
func findRowWithoutNullsInEqCols(rows sqlbase.EncDatumRows, startIdx int, eqCols []uint32) int {
	for rowIdx := startIdx; rowIdx < len(rows); rowIdx++ {
		if !hasNullInEqCol(rows[rowIdx], eqCols) {
			return rowIdx
		}
	}
	return len(rows)
}

func (m *mergeHashJoiner) close() {
	if m.InternalClose() {
		ctx := m.Ctx
		if m.rightIter != nil {
			m.rightIter.Close()
		}
		if m.hashTable != nil {
			m.hashTable.Close(ctx)
		}
		m.streamMerger.close(ctx)
		m.MemMonitor.Stop(ctx)
	}
}

// ConsumerClosed is part of the RowSource interface.
func (m *mergeHashJoiner) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	m.close()
}

var _ execinfrapb.DistSQLSpanStats = &MergeHashJoinerStats{}

const mergeHashJoinerTagPrefix = "mergehashjoiner."

// Stats implements the SpanStats interface.
func (mhjs *MergeHashJoinerStats) Stats() map[string]string {
	// statsMap starts off as the left input stats map.
	statsMap := mhjs.LeftInputStats.Stats(mergeHashJoinerTagPrefix + "left.")
	rightInputStatsMap := mhjs.RightInputStats.Stats(
		mergeHashJoinerTagPrefix + "right.",
	)
	// Merge the two input maps.
	for k, v := range rightInputStatsMap {
		statsMap[k] = v
	}
	statsMap[mergeHashJoinerTagPrefix+execinfra.MaxMemoryTagSuffix] = humanizeutil.IBytes(mhjs.MaxAllocatedMem)
	return statsMap
}

// StatsForQueryPlan implements the DistSQLSpanStats interface.
func (mhjs *MergeHashJoinerStats) StatsForQueryPlan() []string {
	stats := append(
		mhjs.LeftInputStats.StatsForQueryPlan("left "),
		mhjs.RightInputStats.StatsForQueryPlan("right ")...,
	)
	return append(stats,
		fmt.Sprintf("%s: %s",
			execinfra.MaxMemoryQueryPlanSuffix,
			humanizeutil.IBytes(mhjs.MaxAllocatedMem),
		))
}

// outputStatsToTrace outputs the collected mergeHashJoiner stats to the
// trace. Will fail silently if the mergeHashJoiner is not collecting stats.
func (m *mergeHashJoiner) outputStatsToTrace() {
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
			&MergeHashJoinerStats{
				LeftInputStats:  lis,
				RightInputStats: ris,
				MaxAllocatedMem: m.MemMonitor.MaximumBytes(),
			},
		)
	}
}
