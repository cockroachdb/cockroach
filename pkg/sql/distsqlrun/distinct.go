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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stringarena"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/opentracing/opentracing-go"
)

// Distinct is the physical processor implementation of the DISTINCT relational operator.
type Distinct struct {
	ProcessorBase

	input            RowSource
	types            []types.T
	haveLastGroupKey bool
	lastGroupKey     sqlbase.EncDatumRow
	arena            stringarena.Arena
	seen             map[string]struct{}
	orderedCols      []uint32
	distinctCols     util.FastIntSet
	memAcc           mon.BoundAccount
	datumAlloc       sqlbase.DatumAlloc
	scratch          []byte
}

// SortedDistinct is a specialized distinct that can be used when all of the
// distinct columns are also ordered.
type SortedDistinct struct {
	Distinct
}

var _ Processor = &Distinct{}
var _ RowSource = &Distinct{}

const distinctProcName = "distinct"

var _ Processor = &SortedDistinct{}
var _ RowSource = &SortedDistinct{}

const sortedDistinctProcName = "sorted distinct"

// NewDistinct instantiates a new Distinct processor.
func NewDistinct(
	flowCtx *FlowCtx,
	processorID int32,
	spec *distsqlpb.DistinctSpec,
	input RowSource,
	post *distsqlpb.PostProcessSpec,
	output RowReceiver,
) (RowSourcedProcessor, error) {
	if len(spec.DistinctColumns) == 0 {
		return nil, pgerror.NewAssertionErrorf("0 distinct columns specified for distinct processor")
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
		return nil, pgerror.NewAssertionErrorf("ordered cols must be a subset of distinct cols")
	}

	ctx := flowCtx.EvalCtx.Ctx()
	memMonitor := NewMonitor(ctx, flowCtx.EvalCtx.Mon, "distinct-mem")
	d := &Distinct{
		input:        input,
		orderedCols:  spec.OrderedColumns,
		distinctCols: distinctCols,
		memAcc:       memMonitor.MakeBoundAccount(),
		types:        input.OutputTypes(),
	}

	var returnProcessor RowSourcedProcessor = d
	if allSorted {
		// We can use the faster sortedDistinct processor.
		sd := &SortedDistinct{
			Distinct: *d,
		}
		// Set d to the new distinct copy for further initialization.
		// TODO(asubiotto): We should have a distinctBase, rather than making a copy
		// of a distinct processor.
		d = &sd.Distinct
		returnProcessor = sd
	}

	if err := d.Init(
		d, post, d.types, flowCtx, processorID, output, memMonitor, /* memMonitor */
		ProcStateOpts{
			InputsToDrain: []RowSource{d.input},
			TrailingMetaCallback: func(context.Context) []ProducerMetadata {
				d.close()
				return nil
			},
		}); err != nil {
		return nil, err
	}
	d.lastGroupKey = d.out.rowAlloc.AllocRow(len(d.types))
	d.haveLastGroupKey = false

	if sp := opentracing.SpanFromContext(ctx); sp != nil && tracing.IsRecording(sp) {
		d.input = NewInputStatCollector(d.input)
		d.finishTrace = d.outputStatsToTrace
	}

	return returnProcessor, nil
}

// Start is part of the RowSource interface.
func (d *Distinct) Start(ctx context.Context) context.Context {
	d.input.Start(ctx)
	return d.StartInternal(ctx, distinctProcName)
}

// Start is part of the RowSource interface.
func (d *SortedDistinct) Start(ctx context.Context) context.Context {
	d.input.Start(ctx)
	return d.StartInternal(ctx, sortedDistinctProcName)
}

func (d *Distinct) matchLastGroupKey(row sqlbase.EncDatumRow) (bool, error) {
	if !d.haveLastGroupKey {
		return false, nil
	}
	for _, colIdx := range d.orderedCols {
		res, err := d.lastGroupKey[colIdx].Compare(
			&d.types[colIdx], &d.datumAlloc, d.evalCtx, &row[colIdx],
		)
		if res != 0 || err != nil {
			return false, err
		}
	}
	return true, nil
}

// encode appends the encoding of non-ordered columns, which we use as a key in
// our 'seen' set.
func (d *Distinct) encode(appendTo []byte, row sqlbase.EncDatumRow) ([]byte, error) {
	var err error
	for i, datum := range row {
		// Ignore columns that are not in the distinctCols, as if we are
		// post-processing to strip out column Y, we cannot include it as
		// (X1, Y1) and (X1, Y2) will appear as distinct rows, but if we are
		// stripping out Y, we do not want (X1) and (X1) to be in the results.
		if !d.distinctCols.Contains(i) {
			continue
		}

		// TODO(irfansharif): Different rows may come with different encodings,
		// e.g. if they come from different streams that were merged, in which
		// case the encodings don't match (despite having the same underlying
		// datums). We instead opt to always choose sqlbase.DatumEncoding_ASCENDING_KEY
		// but we may want to check the first row for what encodings are already
		// available.
		appendTo, err = datum.Encode(&d.types[i], &d.datumAlloc, sqlbase.DatumEncoding_ASCENDING_KEY, appendTo)
		if err != nil {
			return nil, err
		}
	}
	return appendTo, nil
}

func (d *Distinct) close() {
	if d.InternalClose() {
		d.memAcc.Close(d.Ctx)
		d.MemMonitor.Stop(d.Ctx)
	}
}

// Next is part of the RowSource interface.
func (d *Distinct) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	for d.State == StateRunning {
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

		if len(encoding) > 0 {
			if _, ok := d.seen[string(encoding)]; ok {
				continue
			}
			s, err := d.arena.AllocBytes(d.Ctx, encoding)
			if err != nil {
				d.MoveToDraining(err)
				break
			}
			d.seen[s] = struct{}{}
		}

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
func (d *SortedDistinct) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	for d.State == StateRunning {
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
func (d *Distinct) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	d.close()
}

var _ distsqlpb.DistSQLSpanStats = &DistinctStats{}

const distinctTagPrefix = "distinct."

// Stats implements the SpanStats interface.
func (ds *DistinctStats) Stats() map[string]string {
	inputStatsMap := ds.InputStats.Stats(distinctTagPrefix)
	inputStatsMap[distinctTagPrefix+maxMemoryTagSuffix] = humanizeutil.IBytes(ds.MaxAllocatedMem)
	return inputStatsMap
}

// StatsForQueryPlan implements the DistSQLSpanStats interface.
func (ds *DistinctStats) StatsForQueryPlan() []string {
	return append(
		ds.InputStats.StatsForQueryPlan(""),
		fmt.Sprintf("%s: %s", maxMemoryQueryPlanSuffix, humanizeutil.IBytes(ds.MaxAllocatedMem)),
	)
}

// outputStatsToTrace outputs the collected distinct stats to the trace. Will
// fail silently if the Distinct processor is not collecting stats.
func (d *Distinct) outputStatsToTrace() {
	is, ok := getInputStats(d.flowCtx, d.input)
	if !ok {
		return
	}
	if sp := opentracing.SpanFromContext(d.Ctx); sp != nil {
		tracing.SetSpanStats(
			sp, &DistinctStats{InputStats: is, MaxAllocatedMem: d.MemMonitor.MaximumBytes()},
		)
	}
}
