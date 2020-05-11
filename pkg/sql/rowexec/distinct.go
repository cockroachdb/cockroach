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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stringarena"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/opentracing/opentracing-go"
)

// distinct is the physical processor implementation of the DISTINCT relational operator.
type distinct struct {
	execinfra.ProcessorBase

	input            execinfra.RowSource
	types            []*types.T
	haveLastGroupKey bool
	lastGroupKey     sqlbase.EncDatumRow
	arena            stringarena.Arena
	seen             map[string]struct{}
	orderedCols      []uint32
	distinctCols     util.FastIntSet
	memAcc           mon.BoundAccount
	datumAlloc       sqlbase.DatumAlloc
	scratch          []byte
	nullsAreDistinct bool
	nullCount        uint32
	errorOnDup       string
}

// sortedDistinct is a specialized distinct that can be used when all of the
// distinct columns are also ordered.
type sortedDistinct struct {
	distinct
}

var _ execinfra.Processor = &distinct{}
var _ execinfra.RowSource = &distinct{}
var _ execinfra.OpNode = &distinct{}

const distinctProcName = "distinct"

var _ execinfra.Processor = &sortedDistinct{}
var _ execinfra.RowSource = &sortedDistinct{}
var _ execinfra.OpNode = &sortedDistinct{}

const sortedDistinctProcName = "sorted distinct"

// newDistinct instantiates a new Distinct processor.
func newDistinct(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.DistinctSpec,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (execinfra.RowSourcedProcessor, error) {
	if len(spec.DistinctColumns) == 0 {
		return nil, errors.AssertionFailedf("0 distinct columns specified for distinct processor")
	}

	var distinctCols, orderedCols util.FastIntSet
	allSorted := true

	for _, col := range spec.OrderedColumns {
		orderedCols.Add(int(col))
	}
	for _, col := range spec.DistinctColumns {
		if !orderedCols.Contains(int(col)) {
			allSorted = false
		}
		distinctCols.Add(int(col))
	}
	if !orderedCols.SubsetOf(distinctCols) {
		return nil, errors.AssertionFailedf("ordered cols must be a subset of distinct cols")
	}

	ctx := flowCtx.EvalCtx.Ctx()
	memMonitor := execinfra.NewMonitor(ctx, flowCtx.EvalCtx.Mon, "distinct-mem")
	d := &distinct{
		input:            input,
		orderedCols:      spec.OrderedColumns,
		distinctCols:     distinctCols,
		memAcc:           memMonitor.MakeBoundAccount(),
		types:            input.OutputTypes(),
		nullsAreDistinct: spec.NullsAreDistinct,
		errorOnDup:       spec.ErrorOnDup,
	}

	var returnProcessor execinfra.RowSourcedProcessor = d
	if allSorted {
		// We can use the faster sortedDistinct processor.
		// TODO(asubiotto): We should have a distinctBase, rather than making a copy
		// of a distinct processor.
		sd := &sortedDistinct{
			distinct: distinct{
				input:            input,
				orderedCols:      spec.OrderedColumns,
				distinctCols:     distinctCols,
				memAcc:           memMonitor.MakeBoundAccount(),
				types:            input.OutputTypes(),
				nullsAreDistinct: spec.NullsAreDistinct,
				errorOnDup:       spec.ErrorOnDup,
			},
		}
		// Set d to the new distinct copy for further initialization.
		d = &sd.distinct
		returnProcessor = sd
	}

	if err := d.Init(
		d, post, d.types, flowCtx, processorID, output, memMonitor, /* memMonitor */
		execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{d.input},
			TrailingMetaCallback: func(context.Context) []execinfrapb.ProducerMetadata {
				d.close()
				return nil
			},
		}); err != nil {
		return nil, err
	}
	d.lastGroupKey = d.Out.RowAlloc.AllocRow(len(d.types))
	d.haveLastGroupKey = false
	// If we set up the arena when d is created, the pointer to the memAcc
	// will be changed because the sortedDistinct case makes a copy of d.
	// So we have to set up the account here.
	d.arena = stringarena.Make(&d.memAcc)

	if sp := opentracing.SpanFromContext(ctx); sp != nil && tracing.IsRecording(sp) {
		d.input = newInputStatCollector(d.input)
		d.FinishTrace = d.outputStatsToTrace
	}

	return returnProcessor, nil
}

// Start is part of the RowSource interface.
func (d *distinct) Start(ctx context.Context) context.Context {
	d.input.Start(ctx)
	return d.StartInternal(ctx, distinctProcName)
}

// Start is part of the RowSource interface.
func (d *sortedDistinct) Start(ctx context.Context) context.Context {
	d.input.Start(ctx)
	return d.StartInternal(ctx, sortedDistinctProcName)
}

func (d *distinct) matchLastGroupKey(row sqlbase.EncDatumRow) (bool, error) {
	if !d.haveLastGroupKey {
		return false, nil
	}
	for _, colIdx := range d.orderedCols {
		res, err := d.lastGroupKey[colIdx].Compare(
			d.types[colIdx], &d.datumAlloc, d.EvalCtx, &row[colIdx],
		)
		if res != 0 || err != nil {
			return false, err
		}

		// If null values are treated as distinct from one another, then a grouping
		// column with a NULL value means that the row should never match any other
		// row.
		if d.nullsAreDistinct && d.lastGroupKey[colIdx].IsNull() {
			return false, nil
		}
	}
	return true, nil
}

// encode appends the encoding of non-ordered columns, which we use as a key in
// our 'seen' set.
func (d *distinct) encode(appendTo []byte, row sqlbase.EncDatumRow) ([]byte, error) {
	var err error
	foundNull := false
	for i, datum := range row {
		// Ignore columns that are not in the distinctCols, as if we are
		// post-processing to strip out column Y, we cannot include it as
		// (X1, Y1) and (X1, Y2) will appear as distinct rows, but if we are
		// stripping out Y, we do not want (X1) and (X1) to be in the results.
		if !d.distinctCols.Contains(i) {
			continue
		}

		appendTo, err = datum.Fingerprint(d.types[i], &d.datumAlloc, appendTo)
		if err != nil {
			return nil, err
		}

		// If null values are treated as distinct from one another, then append
		// a unique identifier to the end of the encoding, so that the row will
		// always be in its own distinct group.
		if d.nullsAreDistinct && datum.IsNull() {
			foundNull = true
		}
	}

	if foundNull {
		appendTo = encoding.EncodeUint32Ascending(appendTo, d.nullCount)
		d.nullCount++
	}

	return appendTo, nil
}

func (d *distinct) close() {
	if d.InternalClose() {
		d.memAcc.Close(d.Ctx)
		d.MemMonitor.Stop(d.Ctx)
	}
}

// Next is part of the RowSource interface.
func (d *distinct) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for d.State == execinfra.StateRunning {
		row, meta := d.input.Next()
		if meta != nil {
			if meta.Err != nil {
				d.MoveToDraining(nil /* err */)
			}
			return nil, meta
		}
		if row == nil {
			d.MoveToDraining(nil /* err */)
			break
		}

		// If we are processing DISTINCT(x, y) and the input stream is ordered
		// by x, we define x to be our group key. Our seen set at any given time
		// is only the set of all rows with the same group key. The encoding of
		// the row is the key we use in our 'seen' set.
		encoding, err := d.encode(d.scratch, row)
		if err != nil {
			d.MoveToDraining(err)
			break
		}
		d.scratch = encoding[:0]

		// The 'seen' set is reset whenever we find consecutive rows differing on the
		// group key thus avoiding the need to store encodings of all rows.
		matched, err := d.matchLastGroupKey(row)
		if err != nil {
			d.MoveToDraining(err)
			break
		}

		if !matched {
			// Since the sorted distinct columns have changed, we know that all the
			// distinct keys in the 'seen' set will never be seen again. This allows
			// us to keep the current arena block and overwrite strings previously
			// allocated on it, which implies that UnsafeReset() is safe to call here.
			copy(d.lastGroupKey, row)
			d.haveLastGroupKey = true
			if err := d.arena.UnsafeReset(d.Ctx); err != nil {
				d.MoveToDraining(err)
				break
			}
			d.seen = make(map[string]struct{})
		}

		// Check whether row is distinct.
		if _, ok := d.seen[string(encoding)]; ok {
			if d.errorOnDup != "" {
				// Row is a duplicate input to an Upsert operation, so raise
				// an error.
				//
				// TODO(knz): errorOnDup could be passed via log.Safe() if
				// there was a guarantee that it does not contain PII. Or
				// better yet, the caller would construct an `error` object to
				// return here instead of a string.
				// See: https://github.com/cockroachdb/cockroach/issues/48166
				err = pgerror.Newf(pgcode.CardinalityViolation, "%s", d.errorOnDup)
				d.MoveToDraining(err)
				break
			}
			continue
		}
		s, err := d.arena.AllocBytes(d.Ctx, encoding)
		if err != nil {
			d.MoveToDraining(err)
			break
		}
		d.seen[s] = struct{}{}

		if outRow := d.ProcessRowHelper(row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, d.DrainHelper()
}

// Next is part of the RowSource interface.
//
// sortedDistinct is simpler than distinct. All it has to do is keep track
// of the last row it saw, emitting if the new row is different.
func (d *sortedDistinct) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for d.State == execinfra.StateRunning {
		row, meta := d.input.Next()
		if meta != nil {
			if meta.Err != nil {
				d.MoveToDraining(nil /* err */)
			}
			return nil, meta
		}
		if row == nil {
			d.MoveToDraining(nil /* err */)
			break
		}
		matched, err := d.matchLastGroupKey(row)
		if err != nil {
			d.MoveToDraining(err)
			break
		}
		if matched {
			if d.errorOnDup != "" {
				// Row is a duplicate input to an Upsert operation, so raise an error.
				// TODO(knz): errorOnDup could be passed via log.Safe() if
				// there was a guarantee that it does not contain PII.
				err = pgerror.Newf(pgcode.CardinalityViolation, "%s", d.errorOnDup)
				d.MoveToDraining(err)
				break
			}
			continue
		}

		d.haveLastGroupKey = true
		copy(d.lastGroupKey, row)

		if outRow := d.ProcessRowHelper(row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, d.DrainHelper()
}

// ConsumerClosed is part of the RowSource interface.
func (d *distinct) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	d.close()
}

var _ execinfrapb.DistSQLSpanStats = &DistinctStats{}

const distinctTagPrefix = "distinct."

// Stats implements the SpanStats interface.
func (ds *DistinctStats) Stats() map[string]string {
	inputStatsMap := ds.InputStats.Stats(distinctTagPrefix)
	inputStatsMap[distinctTagPrefix+MaxMemoryTagSuffix] = humanizeutil.IBytes(ds.MaxAllocatedMem)
	return inputStatsMap
}

// StatsForQueryPlan implements the DistSQLSpanStats interface.
func (ds *DistinctStats) StatsForQueryPlan() []string {
	stats := ds.InputStats.StatsForQueryPlan("")

	if ds.MaxAllocatedMem != 0 {
		stats = append(stats,
			fmt.Sprintf("%s: %s", MaxMemoryQueryPlanSuffix, humanizeutil.IBytes(ds.MaxAllocatedMem)))
	}

	return stats
}

// outputStatsToTrace outputs the collected distinct stats to the trace. Will
// fail silently if the Distinct processor is not collecting stats.
func (d *distinct) outputStatsToTrace() {
	is, ok := getInputStats(d.FlowCtx, d.input)
	if !ok {
		return
	}
	if sp := opentracing.SpanFromContext(d.Ctx); sp != nil {
		tracing.SetSpanStats(
			sp, &DistinctStats{InputStats: is, MaxAllocatedMem: d.MemMonitor.MaximumBytes()},
		)
	}
}

// ChildCount is part of the execinfra.OpNode interface.
func (d *distinct) ChildCount(verbose bool) int {
	if _, ok := d.input.(execinfra.OpNode); ok {
		return 1
	}
	return 0
}

// Child is part of the execinfra.OpNode interface.
func (d *distinct) Child(nth int, verbose bool) execinfra.OpNode {
	if nth == 0 {
		if n, ok := d.input.(execinfra.OpNode); ok {
			return n
		}
		panic("input to distinct is not an execinfra.OpNode")
	}
	panic(fmt.Sprintf("invalid index %d", nth))
}
