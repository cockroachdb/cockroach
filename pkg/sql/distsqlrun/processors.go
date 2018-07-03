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
	"context"
	"math"
	"sync"
	"sync/atomic"

	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
)

// processorIDTagKey is the key used for processor id tags in tracing spans.
const processorIDTagKey = tracing.TagPrefix + "processorid"

// Processor is a common interface implemented by all processors, used by the
// higher-level flow orchestration code.
type Processor interface {
	// OutputTypes returns the column types of the results (that are to be fed
	// through an output router).
	OutputTypes() []sqlbase.ColumnType

	// Run is the main loop of the processor.
	// If wg is non-nil, wg.Done is called before exiting.
	Run(_ context.Context, wg *sync.WaitGroup)
}

// ProcOutputHelper is a helper type that performs filtering and projection on
// the output of a processor.
type ProcOutputHelper struct {
	numInternalCols int
	// output can be optionally passed in for use with EmitRow and
	// emitHelper.
	// If output is nil, one can invoke ProcessRow to obtain the
	// post-processed row directly.
	output   RowReceiver
	rowAlloc sqlbase.EncDatumRowAlloc

	filter *exprHelper
	// renderExprs is set if we have a rendering. Only one of renderExprs and
	// outputCols can be set.
	renderExprs []exprHelper
	// outputCols is set if we have a projection. Only one of renderExprs and
	// outputCols can be set.
	outputCols []uint32

	outputRow sqlbase.EncDatumRow

	// outputTypes is the schema of the rows produced by the processor after
	// post-processing (i.e. the rows that are pushed through a router).
	//
	// If renderExprs is set, these types correspond to the types of those
	// expressions.
	// If outputCols is set, these types correspond to the types of
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
	post *PostProcessSpec, types []sqlbase.ColumnType, evalCtx *tree.EvalContext, output RowReceiver,
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
			colTyp, err := sqlbase.DatumTypeToColumnType(h.renderExprs[i].expr.ResolvedType())
			if err != nil {
				return err
			}
			h.outputTypes[i] = colTyp
		}
	} else {
		h.outputTypes = types
	}
	if h.outputCols != nil || h.renderExprs != nil {
		// We're rendering or projecting, so allocate an output row.
		h.outputRow = h.rowAlloc.AllocRow(len(h.outputTypes))
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
func (h *ProcOutputHelper) neededColumns() (colIdxs util.FastIntSet) {
	if h.outputCols == nil && h.renderExprs == nil {
		// No projection or rendering; all columns are needed.
		colIdxs.AddRange(0, h.numInternalCols-1)
		return colIdxs
	}

	// Add all explicit output columns.
	for _, c := range h.outputCols {
		colIdxs.Add(int(c))
	}

	for i := 0; i < h.numInternalCols; i++ {
		// See if filter requires this column.
		if h.filter != nil && h.filter.vars.IndexedVarUsed(i) {
			colIdxs.Add(i)
			continue
		}

		// See if render expressions require this column.
		if h.renderExprs != nil {
			for j := range h.renderExprs {
				if h.renderExprs[j].vars.IndexedVarUsed(i) {
					colIdxs.Add(i)
					break
				}
			}
		}
	}

	return colIdxs
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
// pushTrailingMeta is called after draining the sources and before calling
// dst.ProducerDone(). It gives the caller the opportunity to push some trailing
// metadata (e.g. tracing information and txn updates, if applicable).
//
// Returns true if more rows are needed, false otherwise. If false is returned
// both the inputs and the output have been properly closed.
func emitHelper(
	ctx context.Context,
	output *ProcOutputHelper,
	row sqlbase.EncDatumRow,
	meta *ProducerMetadata,
	pushTrailingMeta func(context.Context),
	inputs ...RowSource,
) bool {
	if output.output == nil {
		panic("output RowReceiver not initialized for emitting")
	}
	var consumerStatus ConsumerStatus
	if meta != nil {
		if row != nil {
			panic("both row data and metadata in the same emitHelper call")
		}
		// Bypass EmitRow() and send directly to output.output.
		consumerStatus = output.output.Push(nil /* row */, meta)
	} else {
		var err error
		consumerStatus, err = output.EmitRow(ctx, row)
		if err != nil {
			output.output.Push(nil /* row */, &ProducerMetadata{Err: err})
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
		DrainAndClose(ctx, output.output, nil /* cause */, pushTrailingMeta, inputs...)
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
// rendering processing; the output has not been closed and it's the caller's
// responsibility to push the error to the output.
//
// Note: check out emitHelper() for a useful wrapper.
func (h *ProcOutputHelper) EmitRow(
	ctx context.Context, row sqlbase.EncDatumRow,
) (ConsumerStatus, error) {
	if h.output == nil {
		panic("output RowReceiver not initialized for emitting rows")
	}

	outRow, ok, err := h.ProcessRow(ctx, row)
	if err != nil {
		// The status doesn't matter.
		return NeedMoreRows, err
	}
	if outRow == nil {
		if ok {
			return NeedMoreRows, nil
		}
		return DrainRequested, nil
	}

	if log.V(3) {
		log.InfofDepth(ctx, 1, "pushing row %s", outRow)
	}
	if r := h.output.Push(outRow, nil); r != NeedMoreRows {
		log.VEventf(ctx, 1, "no more rows required. drain requested: %t",
			r == DrainRequested)
		return r, nil
	}
	if h.rowIdx == h.maxRowIdx {
		log.VEventf(ctx, 1, "hit row limit; asking producer to drain")
		return DrainRequested, nil
	}
	status := NeedMoreRows
	if !ok {
		status = DrainRequested
	}
	return status, nil
}

// ProcessRow sends the invoked row through the post-processing stage and returns
// the post-processed row. Results from ProcessRow aren't safe past the next call
// to ProcessRow.
//
// The moreRowsOK retval is true if more rows can be processed, false if the
// limit has been reached (if there's a limit). Upon seeing a false value, the
// caller is expected to start draining. Note that both a row and
// moreRowsOK=false can be returned at the same time: the row that satisfies the
// limit is returned at the same time as a DrainRequested status. In that case,
// the caller is supposed to both deal with the row and start draining.
func (h *ProcOutputHelper) ProcessRow(
	ctx context.Context, row sqlbase.EncDatumRow,
) (_ sqlbase.EncDatumRow, moreRowsOK bool, _ error) {
	if h.rowIdx >= h.maxRowIdx {
		return nil, false, nil
	}

	if h.filter != nil {
		// Filtering.
		passes, err := h.filter.evalFilter(row)
		if err != nil {
			return nil, false, err
		}
		if !passes {
			if log.V(3) {
				log.Infof(ctx, "filtered out row %s", row.String(h.filter.types))
			}
			return nil, true, nil
		}
	}
	h.rowIdx++
	if h.rowIdx <= h.offset {
		// Suppress row.
		return nil, true, nil
	}

	if h.renderExprs != nil {
		// Rendering.
		for i := range h.renderExprs {
			datum, err := h.renderExprs[i].eval(row)
			if err != nil {
				return nil, false, err
			}
			h.outputRow[i] = sqlbase.DatumToEncDatum(h.outputTypes[i], datum)
		}
	} else if h.outputCols != nil {
		// Projection.
		for i, col := range h.outputCols {
			h.outputRow[i] = row[col]
		}
	} else {
		// No rendering or projection.
		return row, h.rowIdx < h.maxRowIdx, nil
	}

	// If this row satisfies the limit, the caller is told to drain.
	return h.outputRow, h.rowIdx < h.maxRowIdx, nil
}

// Close signals to the output that there will be no more rows.
func (h *ProcOutputHelper) Close() {
	h.output.ProducerDone()
}

// consumerClosed stops output of additional rows from ProcessRow.
func (h *ProcOutputHelper) consumerClosed() {
	h.rowIdx = h.maxRowIdx
}

// processorBase is supposed to be embedded by Processors. It provides
// facilities for dealing with filtering and projection (through a
// ProcOutputHelper) and for implementing the RowSource interface (draining,
// trailing metadata).
//
// If a Processor implements the RowSource interface, it's implementation is
// expected to look something like this:
//
//   // concatProcessor concatenates rows from two sources (first returns rows
//   // from the left, then from the right).
//   type concatProcessor struct {
//     processorBase
//     l, r RowSource
//
//     // leftConsumed is set once we've exhausted the left input; once set, we start
//     // consuming the right input.
//     leftConsumed bool
//   }
//
//   func newConcatProcessor(
//     flowCtx *FlowCtx, l RowSource, r RowSource, post *PostProcessSpec, output RowReceiver,
//   ) (*concatProcessor, error) {
//     p := &concatProcessor{l: l, r: r}
//     if err := p.init(
//       post, l.OutputTypes(), flowCtx, output,
//       // We pass the inputs to the helper, to be consumed by drainHelper() later.
//       procStateOpts{
//         inputsToDrain: []RowSource{l, r},
//         // If the proc needed to return any metadata at the end other than the
//         // tracing info, or if it needed to cleanup any resources other than those
//         // handled by internalClose() (say, close some memory account), it'd pass
//         // a trailingMetaCallback here.
//       },
//     ); err != nil {
//       return nil, err
//     }
//     return p, nil
//   }
//
//   // Start is part of the RowSource interface.
//   func (p *concatProcessor) Start(ctx context.Context) context.Context {
//     p.l.Start(ctx)
//     p.r.Start(ctx)
//     // Don't forget to declare a name for the proc and add it to procNameToLogTag.
//     return p.startInternal(ctx, concatProcName)
//   }
//
//   // Next is part of the RowSource interface.
//   func (p *concatProcessor) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
//     // Loop while we haven't produced a row or a metadata record. We loop around
//     // in several cases, including when the filtering rejected a row coming.
//     for p.state == stateRunning {
//       var row sqlbase.EncDatumRow
//       var meta *ProducerMetadata
//       if !p.leftConsumed {
//         row, meta = p.l.Next()
//       } else {
//         row, meta = p.r.Next()
//       }
//
//       if meta != nil {
//         // If we got an error, we need to forward it along and remember that we're
//         // draining.
//         if meta.Err != nil {
//           p.moveToDraining(nil /* err */)
//         }
//         return nil, meta
//       }
//       if row == nil {
//         if !p.leftConsumed {
//           p.leftConsumed = true
//         } else {
//           // In this case we know that both inputs are consumed, so we could
//           // transition directly to stateTrailingMeta, but implementations are
//           // encouraged to just use moveToDraining() for uniformity; drainHelper()
//           // will transition to stateTrailingMeta() quickly.
//           p.moveToDraining(nil /* err */)
//           break
//         }
//         continue
//       }
//
//       if outRow := p.processRowHelper(row); outRow != nil {
//         return outRow, nil
//       }
//     }
//     return nil, p.drainHelper()
//   }
//
//   // ConsumerDone is part of the RowSource interface.
//   func (p *concatProcessor) ConsumerDone() {
//     p.moveToDraining(nil /* err */)
//   }
//
//   // ConsumerClosed is part of the RowSource interface.
//   func (p *concatProcessor) ConsumerClosed() {
//     // The consumer is done, Next() will not be called again.
//     p.internalClose()
//   }
//
type processorBase struct {
	self RowSource

	processorID int32

	out     ProcOutputHelper
	flowCtx *FlowCtx

	// evalCtx is used for expression evaluation. It overrides the one in flowCtx.
	evalCtx *tree.EvalContext

	// memMonitor is the processor's memory monitor.
	memMonitor *mon.BytesMonitor

	// closed is set by internalClose(). Once set, the processor's tracing span
	// has been closed.
	closed bool

	// ctx and span contain the tracing state while the processor is active
	// (i.e. hasn't been closed). Initialized using flowCtx.Ctx (which should not be otherwise
	// used).
	ctx  context.Context
	span opentracing.Span
	// origCtx is the context from which ctx was derived. internalClose() resets
	// ctx to this.
	origCtx context.Context

	state procState

	// finishTrace, if set, will be called before getting the trace data from
	// the span and adding the recording to the trailing metadata. Useful for
	// adding any extra information (e.g. stats) that should be captured in a
	// trace.
	finishTrace func()

	// trailingMetaCallback, if set, will be called by moveToTrailingMeta(). The
	// callback is expected to close all inputs, do other cleanup on the processor
	// (including calling internalClose()) and generate the trailing meta that
	// needs to be returned to the consumer. As a special case,
	// moveToTrailingMeta() handles getting the tracing information into
	// trailingMeta, so the callback doesn't need to worry about that.
	//
	// If no callback is specified, internalClose() will be called automatically.
	// So, if no trailing metadata other than the trace needs to be returned (and
	// other than what has otherwise been manually put in trailingMeta) and no
	// closing other than internalClose is needed, then no callback needs to be
	// specified.
	trailingMetaCallback func() []ProducerMetadata
	// trailingMeta is scratch space where metadata is stored to be returned
	// later.
	trailingMeta []ProducerMetadata

	// inputsToDrain, if not empty, contains inputs to be drained by
	// drainHelper(). moveToDraining() calls ConsumerDone() on them,
	// internalClose() calls ConsumerClosed() on then.
	//
	// ConsumerDone() is called on all inputs at once and then inputs are drained
	// one by one (in stateDraining, inputsToDrain[0] is the one currently being
	// drained).
	inputsToDrain []RowSource
}

// procState represents the standard states that a processor can be in. These
// states are relevant when the processor is using the draining utilities in
// processorBase.
type procState int

//go:generate stringer -type=procState
const (
	// stateRunning is the common state of a processor: it's producing rows for
	// its consumer and forwarding metadata from its input. Different processors
	// might have sub-states internally.
	//
	// If the consumer calls ConsumerDone or if the ProcOutputHelper.maxRowIdx is
	// reached, then the processor will transition to stateDraining. If the input
	// is exhausted, then the processor can transition to stateTrailingMeta
	// directly, although most always go through stateDraining.
	stateRunning procState = iota

	// stateDraining is the state in which the processor is forwarding metadata
	// from its input and otherwise ignoring all rows. Once the input is
	// exhausted, the processor will transition to stateTrailingMeta.
	stateDraining

	// stateTrailingMeta is the state in which the processor is outputting final
	// metadata such as the tracing information or the TxnCoordMeta. Once all the
	// trailing metadata has been produced, the processor transitions to
	// stateExhausted.
	stateTrailingMeta

	// stateExhausted is the state of a processor that has no more rows or
	// metadata to produce.
	stateExhausted
)

// moveToDraining switches the processor to the stateDraining. Only metadata is
// returned from now on. In this state, the processor is expected to drain its
// inputs (commonly by using drainHelper()).
//
// If the processor has no input (procStateOpts.intputToDrain was not specified
// at init() time), then we move straight to the stateTrailingMeta.
//
// An error can be optionally passed. It will be the first piece of metadata
// returned by drainHelper().
func (pb *processorBase) moveToDraining(err error) {
	if pb.state != stateRunning {
		// Calling moveToDraining in any state is allowed in order to facilitate the
		// ConsumerDone() implementations that just call this unconditionally.
		// However, calling it with an error in states other than stateRunning is
		// not permitted.
		if err != nil {
			log.Fatalf(pb.ctx, "moveToDraining called in state %s with err: %s",
				pb.state, err)
		}
		return
	}

	if err != nil {
		pb.trailingMeta = append(pb.trailingMeta, ProducerMetadata{Err: err})
	}
	if len(pb.inputsToDrain) > 0 {
		// We go to stateDraining here. drainHelper() will transition to
		// stateTrailingMeta when the inputs are drained (including if the inputs
		// are already drained).
		pb.state = stateDraining
		for _, input := range pb.inputsToDrain {
			input.ConsumerDone()
		}
	} else {
		pb.moveToTrailingMeta()
	}
}

// drainHelper is supposed to be used in states draining and trailingMetadata.
// It deals with optionally draining an input and returning trailing meta. It
// also moves from stateDraining to stateTrailingMeta when appropriate.
func (pb *processorBase) drainHelper() *ProducerMetadata {
	if pb.state == stateRunning {
		log.Fatal(pb.ctx, "drain helper called in stateRunning")
	}

	// trailingMeta always has priority; it seems like a good idea because it
	// causes metadata to be sent quickly after it is produced (e.g. the error
	// passed to moveToDraining()).
	if len(pb.trailingMeta) > 0 {
		return pb.popTrailingMeta()
	}

	if pb.state != stateDraining {
		return nil
	}

	// Ignore all rows; only return meta.
	for {
		input := pb.inputsToDrain[0]

		row, meta := input.Next()
		if row == nil && meta == nil {
			pb.inputsToDrain = pb.inputsToDrain[1:]
			if len(pb.inputsToDrain) == 0 {
				pb.moveToTrailingMeta()
				return pb.popTrailingMeta()
			}
			continue
		}
		if meta != nil {
			return meta
		}
	}
}

// popTrailingMeta peels off one piece of trailing metadata or advances to
// stateExhausted if there's no more trailing metadata.
func (pb *processorBase) popTrailingMeta() *ProducerMetadata {
	if len(pb.trailingMeta) > 0 {
		meta := &pb.trailingMeta[0]
		pb.trailingMeta = pb.trailingMeta[1:]
		return meta
	}
	pb.state = stateExhausted
	return nil
}

// moveToTrailingMeta switches the processor to the "trailing meta" state: only
// trailing metadata is returned from now on. For simplicity, processors are
// encouraged to always use moveToDraining() instead of this method, even when
// there's nothing to drain. moveToDrain() or drainHelper() will internally call
// moveToTrailingMeta().
//
// trailingMetaCallback, if any, is called; it is expected to close the
// processor's inputs.
//
// This method is to be called when the processor is done producing rows and
// draining its inputs (if it wants to drain them).
func (pb *processorBase) moveToTrailingMeta() {
	if pb.state == stateTrailingMeta || pb.state == stateExhausted {
		log.Fatalf(pb.ctx, "moveToTrailingMeta called in state: %s", pb.state)
	}

	if pb.finishTrace != nil {
		pb.finishTrace()
	}

	pb.state = stateTrailingMeta
	if trace := getTraceData(pb.ctx); trace != nil {
		pb.trailingMeta = append(pb.trailingMeta, ProducerMetadata{TraceData: trace})
	}
	// trailingMetaCallback is called after reading the tracing data because it
	// generally calls internalClose, indirectly, which switches the context and
	// the span.
	if pb.trailingMetaCallback != nil {
		pb.trailingMeta = append(pb.trailingMeta, pb.trailingMetaCallback()...)
	} else {
		pb.internalClose()
	}
}

// processRowHelper is a wrapper on top of ProcOutputHelper.ProcessRow(). It
// takes care of handling errors and drain requests by moving the processor to
// stateDraining.
//
// It takes a row and returns the row after processing. The return value can be
// nil, in which case the caller shouldn't return anything to its consumer; it
// should continue processing other rows, with the awareness that the processor
// might have been transitioned to the draining phase.
func (pb *processorBase) processRowHelper(row sqlbase.EncDatumRow) sqlbase.EncDatumRow {
	outRow, ok, err := pb.out.ProcessRow(pb.ctx, row)
	if err != nil {
		pb.moveToDraining(err)
		return nil
	}
	if !ok {
		pb.moveToDraining(nil /* err */)
	}
	// Note that outRow might be nil here.
	return outRow
}

// OutputTypes is part of the processor interface.
func (pb *processorBase) OutputTypes() []sqlbase.ColumnType {
	return pb.out.outputTypes
}

// Run is part of the processor interface.
func (pb *processorBase) Run(ctx context.Context, wg *sync.WaitGroup) {
	if pb.out.output == nil {
		panic("processor output not initialized for emitting rows")
	}
	ctx = pb.self.Start(ctx)
	Run(ctx, pb.self, pb.out.output)
	if wg != nil {
		wg.Done()
	}
}

var procNameToLogTag = map[string]string{
	distinctProcName:                "distinct",
	hashAggregatorProcName:          "hashAgg",
	hashJoinerProcName:              "hashJoiner",
	indexJoinerProcName:             "indexJoiner",
	interleavedReaderJoinerProcName: "interleaveReaderJoiner",
	joinReaderProcName:              "joinReader",
	mergeJoinerProcName:             "mergeJoiner",
	metadataTestReceiverProcName:    "metaReceiver",
	metadataTestSenderProcName:      "metaSender",
	noopProcName:                    "noop",
	orderedAggregatorProcName:       "orderedAgg",
	samplerProcName:                 "sampler",
	scrubTableReaderProcName:        "scrub",
	sortAllProcName:                 "sortAll",
	sortTopKProcName:                "sortTopK",
	sortedDistinctProcName:          "sortedDistinct",
	sortChunksProcName:              "sortChunks",
	tableReaderProcName:             "tableReader",
	valuesProcName:                  "values",
	zigzagJoinerProcName:            "zigzagJoiner",
}

// procStateOpts contains fields used by the processorBase's family of functions
// that deal with draining and trailing metadata: the processorBase implements
// generic useful functionality that needs to call back into the Processor.
type procStateOpts struct {
	// trailingMetaCallback, if specified, is a callback to be called by
	// moveToTrailingMeta(). See processorBase.trailingMetaCallback.
	trailingMetaCallback func() []ProducerMetadata
	// inputsToDrain, if specified, will be drained by drainHelper().
	// moveToDraining() calls ConsumerDone() on them, internalClose() calls
	// ConsumerClosed() on them.
	inputsToDrain []RowSource
}

// init initializes the processorBase.
func (pb *processorBase) init(
	self RowSource,
	post *PostProcessSpec,
	types []sqlbase.ColumnType,
	flowCtx *FlowCtx,
	processorID int32,
	output RowReceiver,
	memMonitor *mon.BytesMonitor,
	opts procStateOpts,
) error {
	pb.self = self
	pb.flowCtx = flowCtx
	pb.processorID = processorID
	pb.memMonitor = memMonitor
	pb.evalCtx = flowCtx.NewEvalCtx()
	pb.trailingMetaCallback = opts.trailingMetaCallback
	pb.inputsToDrain = opts.inputsToDrain
	return pb.out.Init(post, types, pb.evalCtx, output)
}

// startInternal prepares the processorBase for execution. It returns the
// annotated context that's also stored in pb.ctx.
func (pb *processorBase) startInternal(ctx context.Context, name string) context.Context {
	pb.ctx = log.WithLogTag(
		ctx, fmt.Sprintf("%s/%d", procNameToLogTag[name], pb.processorID), nil,
	)

	pb.origCtx = pb.ctx
	pb.ctx, pb.span = processorSpan(pb.ctx, name)
	if pb.span != nil {
		pb.span.SetTag(tracing.TagPrefix+"processorid", pb.processorID)
	}
	pb.evalCtx.CtxProvider = tree.FixedCtxProvider{Context: pb.ctx}
	return pb.ctx
}

// internalClose helps processors implement the RowSource interface, performing
// common close functionality. Returns true iff the processor was not already
// closed.
//
// Notably, it calls ConsumerClosed() on all the inputsToDrain.
//
//   if pb.internalClose() {
//     // Perform processor specific close work.
//   }
func (pb *processorBase) internalClose() bool {
	closing := !pb.closed
	// Protection around double closing is useful for allowing ConsumerClosed() to
	// be called on processors that have already closed themselves by moving to
	// stateTrailingMeta.
	if closing {
		for _, input := range pb.inputsToDrain {
			input.ConsumerClosed()
		}

		pb.closed = true
		tracing.FinishSpan(pb.span)
		pb.span = nil
		// Reset the context so that any incidental uses after this point do not
		// access the finished span.
		pb.ctx = pb.origCtx

		// This prevents Next() from returning more rows.
		pb.out.consumerClosed()
	}
	return closing
}

// newMonitor is a utility function used by processors to create a new
// memory monitor with the given name and start it. The returned monitor must
// be closed.
func newMonitor(ctx context.Context, parent *mon.BytesMonitor, name string) *mon.BytesMonitor {
	monitor := mon.MakeMonitorInheritWithLimit(name, 0 /* limit */, parent)
	monitor.Start(ctx, parent, mon.BoundAccount{})
	return &monitor
}

// getInputStats is a utility function to check whether the given input is
// collecting stats, returning true and the stats if so. If false is returned,
// the input is not collecting stats.
func getInputStats(flowCtx *FlowCtx, input RowSource) (InputStats, bool) {
	isc, ok := input.(*InputStatCollector)
	if !ok {
		return InputStats{}, false
	}
	if flowCtx.testingKnobs.DeterministicStats {
		isc.InputStats.StallTime = 0
	}
	return isc.InputStats, true
}

// rowSourceBase provides common functionality for RowSource implementations
// that need to track consumer status. It is intended to be used by RowSource
// implementations into which data is pushed by a producer async, as opposed to
// RowSources that pull data synchronously from their inputs, which don't need
// to deal with concurrent calls to ConsumerDone() / ConsumerClosed()).
// Things like the RowChannel falls in the first category; processors generally
// fall in the latter.
type rowSourceBase struct {
	// consumerStatus is an atomic used in implementation of the
	// RowSource.Consumer{Done,Closed} methods to signal that the consumer is
	// done accepting rows or is no longer accepting data.
	consumerStatus ConsumerStatus
}

// consumerDone helps processors implement RowSource.ConsumerDone.
func (rb *rowSourceBase) consumerDone() {
	atomic.CompareAndSwapUint32((*uint32)(&rb.consumerStatus),
		uint32(NeedMoreRows), uint32(DrainRequested))
}

// consumerClosed helps processors implement RowSource.ConsumerClosed. The name
// is only used for debug messages.
func (rb *rowSourceBase) consumerClosed(name string) {
	status := ConsumerStatus(atomic.LoadUint32((*uint32)(&rb.consumerStatus)))
	if status == ConsumerClosed {
		log.Fatalf(context.Background(), "%s already closed", name)
	}
	atomic.StoreUint32((*uint32)(&rb.consumerStatus), uint32(ConsumerClosed))
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

func newProcessor(
	ctx context.Context,
	flowCtx *FlowCtx,
	processorID int32,
	core *ProcessorCoreUnion,
	post *PostProcessSpec,
	inputs []RowSource,
	outputs []RowReceiver,
) (Processor, error) {
	if core.Noop != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		return newNoopProcessor(flowCtx, processorID, inputs[0], post, outputs[0])
	}
	if core.Values != nil {
		if err := checkNumInOut(inputs, outputs, 0, 1); err != nil {
			return nil, err
		}
		return newValuesProcessor(flowCtx, processorID, core.Values, post, outputs[0])
	}
	if core.TableReader != nil {
		if err := checkNumInOut(inputs, outputs, 0, 1); err != nil {
			return nil, err
		}
		if core.TableReader.IsCheck {
			return newScrubTableReader(flowCtx, processorID, core.TableReader, post, outputs[0])
		}
		return newTableReader(flowCtx, processorID, core.TableReader, post, outputs[0])
	}
	if core.JoinReader != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		if len(core.JoinReader.LookupColumns) == 0 {
			return newIndexJoiner(
				flowCtx, processorID, core.JoinReader, inputs[0], post, outputs[0])
		}
		return newJoinReader(flowCtx, processorID, core.JoinReader, inputs[0], post, outputs[0])
	}
	if core.Sorter != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		return newSorter(ctx, flowCtx, processorID, core.Sorter, inputs[0], post, outputs[0])
	}
	if core.Distinct != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		return NewDistinct(flowCtx, processorID, core.Distinct, inputs[0], post, outputs[0])
	}
	if core.Aggregator != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		return newAggregator(flowCtx, processorID, core.Aggregator, inputs[0], post, outputs[0])
	}
	if core.MergeJoiner != nil {
		if err := checkNumInOut(inputs, outputs, 2, 1); err != nil {
			return nil, err
		}
		return newMergeJoiner(
			flowCtx, processorID, core.MergeJoiner, inputs[0], inputs[1], post, outputs[0],
		)
	}
	if core.InterleavedReaderJoiner != nil {
		if err := checkNumInOut(inputs, outputs, 0, 1); err != nil {
			return nil, err
		}
		return newInterleavedReaderJoiner(
			flowCtx, processorID, core.InterleavedReaderJoiner, post, outputs[0],
		)
	}
	if core.HashJoiner != nil {
		if err := checkNumInOut(inputs, outputs, 2, 1); err != nil {
			return nil, err
		}
		return newHashJoiner(flowCtx, processorID, core.HashJoiner, inputs[0], inputs[1], post, outputs[0])
	}
	if core.Backfiller != nil {
		if err := checkNumInOut(inputs, outputs, 0, 1); err != nil {
			return nil, err
		}
		switch core.Backfiller.Type {
		case BackfillerSpec_Index:
			return newIndexBackfiller(flowCtx, processorID, *core.Backfiller, post, outputs[0])
		case BackfillerSpec_Column:
			return newColumnBackfiller(flowCtx, processorID, *core.Backfiller, post, outputs[0])
		}
	}
	if core.Sampler != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		return newSamplerProcessor(flowCtx, processorID, core.Sampler, inputs[0], post, outputs[0])
	}
	if core.SampleAggregator != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		return newSampleAggregator(flowCtx, processorID, core.SampleAggregator, inputs[0], post, outputs[0])
	}
	if core.ReadImport != nil {
		if err := checkNumInOut(inputs, outputs, 0, 1); err != nil {
			return nil, err
		}
		if NewReadImportDataProcessor == nil {
			return nil, errors.New("ReadImportData processor unimplemented")
		}
		return NewReadImportDataProcessor(flowCtx, processorID, *core.ReadImport, outputs[0])
	}
	if core.SSTWriter != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		if NewSSTWriterProcessor == nil {
			return nil, errors.New("SSTWriter processor unimplemented")
		}
		return NewSSTWriterProcessor(flowCtx, processorID, *core.SSTWriter, inputs[0], outputs[0])
	}
	if core.CSVWriter != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		if NewCSVWriterProcessor == nil {
			return nil, errors.New("CSVWriter processor unimplemented")
		}
		return NewCSVWriterProcessor(flowCtx, processorID, *core.CSVWriter, inputs[0], outputs[0])
	}
	if core.MetadataTestSender != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		return newMetadataTestSender(flowCtx, processorID, inputs[0], post, outputs[0], core.MetadataTestSender.ID)
	}
	if core.MetadataTestReceiver != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		return newMetadataTestReceiver(
			flowCtx, processorID, inputs[0], post, outputs[0], core.MetadataTestReceiver.SenderIDs,
		)
	}
	if core.ProjectSet != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		return newProjectSetProcessor(flowCtx, processorID, core.ProjectSet, inputs[0], post, outputs[0])
	}
	return nil, errors.Errorf("unsupported processor core %s", core)
}

// NewReadImportDataProcessor is externally implemented and registered by
// ccl/sqlccl/csv.go.
var NewReadImportDataProcessor func(*FlowCtx, int32, ReadImportDataSpec, RowReceiver) (Processor, error)

// NewSSTWriterProcessor is externally implemented and registered by
// ccl/sqlccl/csv.go.
var NewSSTWriterProcessor func(*FlowCtx, int32, SSTWriterSpec, RowSource, RowReceiver) (Processor, error)

// NewCSVWriterProcessor is externally implemented.
var NewCSVWriterProcessor func(*FlowCtx, int32, CSVWriterSpec, RowSource, RowReceiver) (Processor, error)

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
		if b.FilterColIdx == nil || *a.FilterColIdx != *b.FilterColIdx {
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
