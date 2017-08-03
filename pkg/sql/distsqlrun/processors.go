// Copyright 2017 The Cockroach Authors.
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
	"math"
	"sync"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
)

// Processor is a common interface implemented by all processors, used by the
// higher-level flow orchestration code.
type Processor interface {
	// OutputTypes returns the column types of the results (that are to be fed
	// through an output router).
	OutputTypes() []sqlbase.ColumnType

	// Run is the main loop of the processor.
	// If wg is non-nil, wg.Done is called before exiting.
	Run(ctx context.Context, wg *sync.WaitGroup)
}

// ProcOutputHelper is a helper type that performs filtering and projection on
// the output of a processor.
type ProcOutputHelper struct {
	numInternalCols int
	output          RowReceiver
	rowAlloc        sqlbase.EncDatumRowAlloc

	filter *exprHelper
	// renderExprs is set if we have a rendering. Only one of renderExprs and
	// outputCols can be set.
	renderExprs []exprHelper
	// outputCols is set if we have a projection. Only one of renderExprs and
	// outputCols can be set.
	outputCols []uint32

	// outputTypes is the schema of the rows produced by the processor after
	// post-processing (i.e. the rows that are pushed through a router).
	//
	// If renderExprs is set, these types correspond to the types of those
	// expressions.
	// If outpuCols is set, these types correspond to the types of
	// those columns.
	// If neither is set, this is the internal schema of the processor.
	outputTypes []sqlbase.ColumnType

	// offset is the number of rows that are suppressed.
	offset uint64
	// maxRowIdx is the number of rows after which we can stop (offset + limit),
	// or MaxUint64 if there is no limit.
	maxRowIdx uint64

	rowIdx uint64
}

// Init sets up a ProcOutputHelper. The types describe the internal schema of
// the processor (as described for each processor core spec); they can be
// omitted if there is no filtering expression.
// Note that the types slice may be stored directly; the caller should not
// modify it.
func (h *ProcOutputHelper) Init(
	post *PostProcessSpec,
	types []sqlbase.ColumnType,
	evalCtx *parser.EvalContext,
	output RowReceiver,
) error {
	if !post.Projection && len(post.OutputColumns) > 0 {
		return errors.Errorf("post-processing has projection unset but output columns set: %s", post)
	}
	if post.Projection && len(post.RenderExprs) > 0 {
		return errors.Errorf("post-processing has both projection and rendering: %s", post)
	}
	h.output = output
	h.numInternalCols = len(types)
	if post.Filter.Expr != "" {
		h.filter = &exprHelper{}
		if err := h.filter.init(post.Filter, types, evalCtx); err != nil {
			return err
		}
	}
	if post.Projection {
		for _, col := range post.OutputColumns {
			if int(col) >= h.numInternalCols {
				return errors.Errorf("invalid output column %d (only %d available)", col, h.numInternalCols)
			}
		}
		h.outputCols = post.OutputColumns
		if h.outputCols == nil {
			// nil indicates no projection; use an empty slice.
			h.outputCols = make([]uint32, 0)
		}
		h.outputTypes = make([]sqlbase.ColumnType, len(h.outputCols))
		for i, c := range h.outputCols {
			h.outputTypes[i] = types[c]
		}
	} else if len(post.RenderExprs) > 0 {
		h.renderExprs = make([]exprHelper, len(post.RenderExprs))
		h.outputTypes = make([]sqlbase.ColumnType, len(post.RenderExprs))
		for i, expr := range post.RenderExprs {
			if err := h.renderExprs[i].init(expr, types, evalCtx); err != nil {
				return err
			}
			h.outputTypes[i] = sqlbase.DatumTypeToColumnType(h.renderExprs[i].expr.ResolvedType())
		}
	} else {
		h.outputTypes = types
	}

	h.offset = post.Offset
	if post.Limit == 0 || post.Limit >= math.MaxUint64-h.offset {
		h.maxRowIdx = math.MaxUint64
	} else {
		h.maxRowIdx = h.offset + post.Limit
	}

	return nil
}

// neededColumns calculates the set of internal processor columns that are
// actually used by the post-processing stage.
func (h *ProcOutputHelper) neededColumns() []bool {
	needed := make([]bool, h.numInternalCols)
	if h.outputCols == nil && h.renderExprs == nil {
		// No projection or rendering; all columns are needed.
		for i := range needed {
			needed[i] = true
		}
		return needed
	}
	for _, c := range h.outputCols {
		needed[c] = true
	}
	if h.filter != nil {
		for i := range needed {
			if !needed[i] {
				needed[i] = h.filter.vars.IndexedVarUsed(i)
			}
		}
	}
	if h.renderExprs != nil {
		for i := range needed {
			if !needed[i] {
				for j := range h.renderExprs {
					if h.renderExprs[j].vars.IndexedVarUsed(i) {
						needed[i] = true
						break
					}
				}
			}
		}
	}
	return needed
}

// emitHelper is a utility wrapper on top of ProcOutputHelper.EmitRow().
// It takes a row to emit and, if anything happens other than the normal
// situation where the emitting succeeds and the consumer still needs rows, both
// the (potentially many) inputs and the output are properly closed after
// potentially draining the inputs. It's allowed to not pass any inputs, in
// which case nothing will be drained (this can happen when the caller has
// already fully consumed the inputs).
//
// As opposed to EmitRow(), this also supports metadata rows which bypass the
// ProcOutputHelper and are routed directly to its output.
//
// If the consumer signals the producer to drain, the message is relayed and all
// the draining metadata is consumed and forwarded.
//
// inputs are optional.
//
// Returns true if more rows are needed, false otherwise. If false is returned
// both the inputs and the output have been properly closed.
func emitHelper(
	ctx context.Context,
	output *ProcOutputHelper,
	row sqlbase.EncDatumRow,
	meta ProducerMetadata,
	inputs ...RowSource,
) bool {
	var consumerStatus ConsumerStatus
	if !meta.Empty() {
		if row != nil {
			log.Fatalf(ctx, "both row data and metadata in the same emitHelper call. "+
				"row: %s. meta: %+v", row, meta)
		}
		// Bypass EmitRow() and send directly to output.output.
		consumerStatus = output.output.Push(nil /* row */, meta)
	} else {
		var err error
		consumerStatus, err = output.EmitRow(ctx, row)
		if err != nil {
			output.output.Push(nil /* row */, ProducerMetadata{Err: err})
			for _, input := range inputs {
				input.ConsumerClosed()
			}
			output.Close()
			return false
		}
	}
	switch consumerStatus {
	case NeedMoreRows:
		return true
	case DrainRequested:
		log.VEventf(ctx, 1, "no more rows required. drain requested.")
		DrainAndClose(ctx, output.output, nil /* cause */, inputs...)
		return false
	case ConsumerClosed:
		log.VEventf(ctx, 1, "no more rows required. Consumer shut down.")
		for _, input := range inputs {
			input.ConsumerClosed()
		}
		output.Close()
		return false
	default:
		log.Fatalf(ctx, "unexpected consumerStatus: %d", consumerStatus)
		return false
	}
}

// EmitRow sends a row through the post-processing stage. The same row can be
// reused.
//
// It returns the consumer's status that was observed when pushing this row. If
// an error is returned, it's coming from the ProcOutputHelper's filtering or
// rendering processing; the output has not been closed.
//
// Note: check out emitHelper() for a useful wrapper.
func (h *ProcOutputHelper) EmitRow(
	ctx context.Context, row sqlbase.EncDatumRow,
) (ConsumerStatus, error) {
	if h.rowIdx >= h.maxRowIdx {
		return DrainRequested, nil
	}
	if h.filter != nil {
		// Filtering.
		passes, err := h.filter.evalFilter(row)
		if err != nil {
			return ConsumerClosed, err
		}
		if !passes {
			if log.V(3) {
				log.Infof(ctx, "filtered out row %s", row)
			}
			return NeedMoreRows, nil
		}
	}
	h.rowIdx++
	if h.rowIdx <= h.offset {
		// Suppress row.
		return NeedMoreRows, nil
	}
	var outRow sqlbase.EncDatumRow
	if h.renderExprs != nil {
		// Rendering.
		outRow = h.rowAlloc.AllocRow(len(h.renderExprs))
		for i := range h.renderExprs {
			datum, err := h.renderExprs[i].eval(row)
			if err != nil {
				return ConsumerClosed, err
			}
			outRow[i] = sqlbase.DatumToEncDatum(h.outputTypes[i], datum)
		}
	} else if h.outputCols != nil {
		// Projection.
		outRow = h.rowAlloc.AllocRow(len(h.outputCols))
		for i, col := range h.outputCols {
			outRow[i] = row[col]
		}
	} else {
		// No rendering or projection.
		outRow = h.rowAlloc.AllocRow(len(row))
		copy(outRow, row)
	}
	if log.V(3) {
		log.InfofDepth(ctx, 1, "pushing row %s", outRow)
	}
	if r := h.output.Push(outRow, ProducerMetadata{}); r != NeedMoreRows {
		log.VEventf(ctx, 1, "no more rows required. drain requested: %t",
			r == DrainRequested)
		return r, nil
	}
	if h.rowIdx == h.maxRowIdx {
		log.VEventf(ctx, 1, "hit row limit; asking producer to drain")
		return DrainRequested, nil
	}
	return NeedMoreRows, nil
}

// Close signals to the output that there will be no more rows.
func (h *ProcOutputHelper) Close() {
	h.output.ProducerDone()
}

type processorBase struct {
	out ProcOutputHelper
}

// OutputTypes is part of the processor interface.
func (pb *processorBase) OutputTypes() []sqlbase.ColumnType {
	return pb.out.outputTypes
}

// noopProcessor is a processor that simply passes rows through from the
// synchronizer to the post-processing stage. It can be useful for its
// post-processing or in the last stage of a computation, where we may only
// need the synchronizer to join streams.
type noopProcessor struct {
	processorBase

	flowCtx *FlowCtx
	input   RowSource
}

var _ Processor = &noopProcessor{}

func newNoopProcessor(
	flowCtx *FlowCtx, input RowSource, post *PostProcessSpec, output RowReceiver,
) (*noopProcessor, error) {
	n := &noopProcessor{flowCtx: flowCtx, input: input}
	if err := n.out.Init(post, input.Types(), &flowCtx.EvalCtx, output); err != nil {
		return nil, err
	}
	return n, nil
}

// processorSpan creates a child span for a processor (if we are doing any
// tracing). The returned span needs to be finished using tracing.FinishSpan.
func processorSpan(ctx context.Context, name string) (context.Context, opentracing.Span) {
	parentSp := opentracing.SpanFromContext(ctx)
	if parentSp == nil || tracing.IsBlackHoleSpan(parentSp) {
		return ctx, nil
	}
	newSpan := tracing.StartChildSpan(name, parentSp, true /* separateRecording */)
	return opentracing.ContextWithSpan(ctx, newSpan), newSpan
}

// Run is part of the processor interface.
func (n *noopProcessor) Run(ctx context.Context, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}
	ctx, span := processorSpan(ctx, "noop")
	defer tracing.FinishSpan(span)

	for {
		row, meta := n.input.Next()
		if row == nil && meta.Empty() {
			sendTraceData(ctx, n.out.output)
			n.out.Close()
			return
		}
		if !emitHelper(ctx, &n.out, row, meta, n.input) {
			return
		}
	}
}

func newProcessor(
	flowCtx *FlowCtx,
	core *ProcessorCoreUnion,
	post *PostProcessSpec,
	inputs []RowSource,
	outputs []RowReceiver,
) (Processor, error) {
	if core.Noop != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		return newNoopProcessor(flowCtx, inputs[0], post, outputs[0])
	}
	if core.Values != nil {
		if err := checkNumInOut(inputs, outputs, 0, 1); err != nil {
			return nil, err
		}
		return newValuesProcessor(flowCtx, core.Values, post, outputs[0])
	}
	if core.TableReader != nil {
		if err := checkNumInOut(inputs, outputs, 0, 1); err != nil {
			return nil, err
		}
		return newTableReader(flowCtx, core.TableReader, post, outputs[0])
	}
	if core.JoinReader != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		return newJoinReader(flowCtx, core.JoinReader, inputs[0], post, outputs[0])
	}
	if core.Sorter != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		return newSorter(flowCtx, core.Sorter, inputs[0], post, outputs[0])
	}
	if core.Distinct != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		return newDistinct(flowCtx, core.Distinct, inputs[0], post, outputs[0])
	}
	if core.Aggregator != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		return newAggregator(flowCtx, core.Aggregator, inputs[0], post, outputs[0])
	}
	if core.MergeJoiner != nil {
		if err := checkNumInOut(inputs, outputs, 2, 1); err != nil {
			return nil, err
		}
		return newMergeJoiner(
			flowCtx, core.MergeJoiner, inputs[0], inputs[1], post, outputs[0],
		)
	}
	if core.HashJoiner != nil {
		if err := checkNumInOut(inputs, outputs, 2, 1); err != nil {
			return nil, err
		}
		return newHashJoiner(flowCtx, core.HashJoiner, inputs[0], inputs[1], post, outputs[0])
	}
	if core.Backfiller != nil {
		if err := checkNumInOut(inputs, outputs, 0, 1); err != nil {
			return nil, err
		}
		switch core.Backfiller.Type {
		case BackfillerSpec_Index:
			return newIndexBackfiller(flowCtx, *core.Backfiller, post, outputs[0])
		case BackfillerSpec_Column:
			return newColumnBackfiller(flowCtx, *core.Backfiller, post, outputs[0])
		}
	}
	if core.SetOp != nil {
		if err := checkNumInOut(inputs, outputs, 2, 1); err != nil {
			return nil, err
		}
		return newAlgebraicSetOp(flowCtx, core.SetOp, inputs[0], inputs[1], post, outputs[0])
	}
	if core.ReadCSV != nil {
		if err := checkNumInOut(inputs, outputs, 0, 1); err != nil {
			return nil, err
		}
		if NewReadCSVProcessor == nil {
			return nil, errors.New("ReadCSV processor unimplemented")
		}
		return NewReadCSVProcessor(flowCtx, *core.ReadCSV, outputs[0])
	}
	return nil, errors.Errorf("unsupported processor core %s", core)
}

// NewReadCSVProcessor is externally implemented.
var NewReadCSVProcessor func(*FlowCtx, ReadCSVSpec, RowReceiver) (Processor, error)

// Equals returns true if two aggregation specifiers are identical (and thus
// will always yield the same result).
func (a AggregatorSpec_Aggregation) Equals(b AggregatorSpec_Aggregation) bool {
	if a.Func != b.Func || a.Distinct != b.Distinct {
		return false
	}
	if a.FilterColIdx == nil {
		if b.FilterColIdx != nil {
			return false
		}
	} else {
		if a.FilterColIdx == nil || *a.FilterColIdx != *b.FilterColIdx {
			return false
		}
	}
	if len(a.ColIdx) != len(b.ColIdx) {
		return false
	}
	for i, c := range a.ColIdx {
		if c != b.ColIdx[i] {
			return false
		}
	}
	return true
}
