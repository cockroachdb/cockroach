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
	filter          *exprHelper
	filterErr       error
	outputCols      []uint32
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
	}
	return nil
}

// neededColumns calculates the set of internal processor columns that are
// actually used by the post-processing stage.
func (h *procOutputHelper) neededColumns() []bool {
	needed := make([]bool, h.numInternalCols)
	if h.outputCols == nil {
		// No projection; all columns are needed.
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
	return needed
}

// emitRow sends a row through the post-processing stage. The same row can be
// reused. Returns false if the caller should stop emitting more rows.
func (h *procOutputHelper) emitRow(ctx context.Context, row sqlbase.EncDatumRow) bool {
	if h.filter != nil {
		passes, err := h.filter.evalFilter(row)
		if err != nil {
			h.filterErr = err
			return false
		}
		if !passes {
			if log.V(3) {
				log.Infof(ctx, "filtered out row %s", row)
			}
			return true
		}
	}
	var outRow sqlbase.EncDatumRow
	if h.outputCols == nil {
		outRow = h.rowAlloc.AllocRow(len(row))
		copy(outRow, row)
	} else {
		outRow = h.rowAlloc.AllocRow(len(h.outputCols))
		for i, col := range h.outputCols {
			outRow[i] = row[col]
		}
	}
	if log.V(3) {
		log.Infof(ctx, "pushing row %s", outRow)
	}
	if !h.output.PushRow(outRow) {
		log.VEventf(ctx, 1, "no more rows required")
		return false
	}
	return true
}

func (h *procOutputHelper) close(err error) {
	if h.filterErr != nil {
		err = h.filterErr
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
			return
		}
	}
}
