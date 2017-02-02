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
//
// Author: Radu Berinde (radu@cockroachlabs.com)

package distsqlrun

import (
	"math"
	"sync"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
)

// processor is a common interface implemented by all processors, used by the
// higher-level flow orchestration code.
type processor interface {
	// Run is the main loop of the processor.
	// If wg is non-nil, wg.Done is called before exiting.
	Run(wg *sync.WaitGroup)
}

// procOutputHelper is a helper type that performs filtering and projection on
// the output of a processor.
type procOutputHelper struct {
	numInternalCols int
	output          RowReceiver
	rowAlloc        sqlbase.EncDatumRowAlloc

	filter      *exprHelper
	renderExprs []exprHelper
	renderTypes []sqlbase.ColumnType
	outputCols  []uint32

	// offset is the number of rows that are suppressed.
	offset uint64
	// maxRowIdx is the number of rows after which we can stop (offset + limit),
	// or MaxUint64 if there is no limit.
	maxRowIdx uint64

	rowIdx      uint64
	internalErr error
}

// init sets up a procOutputHelper. The types describe the internal schema of
// the processor (as described for each processor core spec); they can be
// omitted if there is no filtering expression.
func (h *procOutputHelper) init(
	post *PostProcessSpec,
	types []sqlbase.ColumnType,
	evalCtx *parser.EvalContext,
	output RowReceiver,
) error {
	if len(post.OutputColumns) > 0 && len(post.RenderExprs) > 0 {
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
	if len(post.OutputColumns) > 0 {
		for _, col := range post.OutputColumns {
			if int(col) >= h.numInternalCols {
				return errors.Errorf("invalid output column %d (only %d available)", col, h.numInternalCols)
			}
		}
		h.outputCols = post.OutputColumns
	} else if len(post.RenderExprs) > 0 {
		h.renderExprs = make([]exprHelper, len(post.RenderExprs))
		h.renderTypes = make([]sqlbase.ColumnType, len(post.RenderExprs))
		for i, expr := range post.RenderExprs {
			if err := h.renderExprs[i].init(expr, types, evalCtx); err != nil {
				return err
			}
			h.renderTypes[i] = sqlbase.DatumTypeToColumnType(h.renderExprs[i].expr.ResolvedType())
		}
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
func (h *procOutputHelper) neededColumns() []bool {
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

// emitRow sends a row through the post-processing stage. The same row can be
// reused. Returns false if the caller should stop emitting more rows.
func (h *procOutputHelper) emitRow(ctx context.Context, row sqlbase.EncDatumRow) bool {
	if h.rowIdx >= h.maxRowIdx {
		return false
	}
	if h.filter != nil {
		// Filtering.
		passes, err := h.filter.evalFilter(row)
		if err != nil {
			h.internalErr = err
			return false
		}
		if !passes {
			if log.V(3) {
				log.Infof(ctx, "filtered out row %s", row)
			}
			return true
		}
	}
	h.rowIdx++
	if h.rowIdx <= h.offset {
		// Suppress row.
		return true
	}
	var outRow sqlbase.EncDatumRow
	if h.renderExprs != nil {
		// Rendering.
		outRow = h.rowAlloc.AllocRow(len(h.renderExprs))
		for i := range h.renderExprs {
			datum, err := h.renderExprs[i].eval(row)
			if err != nil {
				h.internalErr = err
				return false
			}
			outRow[i] = sqlbase.DatumToEncDatum(h.renderTypes[i], datum)
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
		log.Infof(ctx, "pushing row %s", outRow)
	}
	if !h.output.PushRow(outRow) {
		log.VEventf(ctx, 1, "no more rows required")
		return false
	}
	if h.rowIdx == h.maxRowIdx {
		log.VEventf(ctx, 1, "hit row limit")
		return false
	}
	return true
}

func (h *procOutputHelper) close(err error) {
	if h.internalErr != nil {
		err = h.internalErr
	}
	h.output.Close(err)
}

// noopProcessor is a processor that simply passes rows through from the
// synchronizer to the post-processing stage. It can be useful for its
// post-processing or in the last stage of a computation, where we may only
// need the synchronizer to join streams.
type noopProcessor struct {
	flowCtx *FlowCtx
	input   RowSource
	out     procOutputHelper
}

var _ processor = &noopProcessor{}

func newNoopProcessor(
	flowCtx *FlowCtx, input RowSource, post *PostProcessSpec, output RowReceiver,
) (*noopProcessor, error) {
	n := &noopProcessor{flowCtx: flowCtx, input: input}
	if err := n.out.init(post, input.Types(), flowCtx.evalCtx, output); err != nil {
		return nil, err
	}
	return n, nil
}

// Run is part of the processor interface.
func (n *noopProcessor) Run(wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}
	ctx, span := tracing.ChildSpan(n.flowCtx.Context, "noop")
	defer tracing.FinishSpan(span)

	for {
		row, err := n.input.NextRow()
		if err != nil || row == nil {
			n.out.close(err)
			return
		}
		if !n.out.emitRow(ctx, row) {
			n.input.ConsumerDone()
			n.out.close(nil)
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
) (processor, error) {
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
	return nil, errors.Errorf("unsupported processor core %s", core)
}
