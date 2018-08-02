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

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/opentracing/opentracing-go"
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

// Output returns the output RowReciever of this ProcOutputHelper.
func (h *ProcOutputHelper) Output() RowReceiver {
	return h.output
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

// OutputTypes returns the output types of this ProcOutputHelper.
func (h *ProcOutputHelper) OutputTypes() []sqlbase.ColumnType {
	return h.outputTypes
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

// ProcessorBase is supposed to be embedded by Processors. It provides
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
//     ProcessorBase
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
//       // We pass the inputs to the helper, to be consumed by DrainHelper() later.
//       ProcStateOpts{
//         InputsToDrain: []RowSource{l, r},
//         // If the proc needed to return any metadata at the end other than the
//         // tracing info, or if it needed to cleanup any resources other than those
//         // handled by InternalClose() (say, close some memory account), it'd pass
//         // a TrailingMetaCallback here.
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
//     return p.StartInternal(ctx, concatProcName)
//   }
//
//   // Next is part of the RowSource interface.
//   func (p *concatProcessor) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
//     // Loop while we haven't produced a row or a metadata record. We loop around
//     // in several cases, including when the filtering rejected a row coming.
//     for p.State == StateRunning {
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
//           p.MoveToDraining(nil /* err */)
//         }
//         return nil, meta
//       }
//       if row == nil {
//         if !p.leftConsumed {
//           p.leftConsumed = true
//         } else {
//           // In this case we know that both inputs are consumed, so we could
//           // transition directly to stateTrailingMeta, but implementations are
//           // encouraged to just use MoveToDraining() for uniformity; DrainHelper()
//           // will transition to stateTrailingMeta() quickly.
//           p.MoveToDraining(nil /* err */)
//           break
//         }
//         continue
//       }
//
//       if outRow := p.processRowHelper(row); outRow != nil {
//         return outRow, nil
//       }
//     }
//     return nil, p.DrainHelper()
//   }
//
//   // ConsumerDone is part of the RowSource interface.
//   func (p *concatProcessor) ConsumerDone() {
//     p.MoveToDraining(nil /* err */)
//   }
//
//   // ConsumerClosed is part of the RowSource interface.
//   func (p *concatProcessor) ConsumerClosed() {
//     // The consumer is done, Next() will not be called again.
//     p.InternalClose()
//   }
//
type ProcessorBase struct {
	self RowSource

	processorID int32

	out     ProcOutputHelper
	flowCtx *FlowCtx

	// evalCtx is used for expression evaluation. It overrides the one in flowCtx.
	evalCtx *tree.EvalContext

	// MemMonitor is the processor's memory monitor.
	MemMonitor *mon.BytesMonitor

	// closed is set by InternalClose(). Once set, the processor's tracing span
	// has been closed.
	closed bool

	// Ctx and span contain the tracing state while the processor is active
	// (i.e. hasn't been closed). Initialized using flowCtx.Ctx (which should not be otherwise
	// used).
	Ctx  context.Context
	span opentracing.Span
	// origCtx is the context from which ctx was derived. InternalClose() resets
	// ctx to this.
	origCtx context.Context

	State procState

	// finishTrace, if set, will be called before getting the trace data from
	// the span and adding the recording to the trailing metadata. Useful for
	// adding any extra information (e.g. stats) that should be captured in a
	// trace.
	finishTrace func()

	// trailingMetaCallback, if set, will be called by moveToTrailingMeta(). The
	// callback is expected to close all inputs, do other cleanup on the processor
	// (including calling InternalClose()) and generate the trailing meta that
	// needs to be returned to the consumer. As a special case,
	// moveToTrailingMeta() handles getting the tracing information into
	// trailingMeta, so the callback doesn't need to worry about that.
	//
	// If no callback is specified, InternalClose() will be called automatically.
	// So, if no trailing metadata other than the trace needs to be returned (and
	// other than what has otherwise been manually put in trailingMeta) and no
	// closing other than InternalClose is needed, then no callback needs to be
	// specified.
	trailingMetaCallback func() []ProducerMetadata
	// trailingMeta is scratch space where metadata is stored to be returned
	// later.
	trailingMeta []ProducerMetadata

	// inputsToDrain, if not empty, contains inputs to be drained by
	// DrainHelper(). MoveToDraining() calls ConsumerDone() on them,
	// InternalClose() calls ConsumerClosed() on then.
	//
	// ConsumerDone() is called on all inputs at once and then inputs are drained
	// one by one (in stateDraining, inputsToDrain[0] is the one currently being
	// drained).
	inputsToDrain []RowSource
}

// procState represents the standard states that a processor can be in. These
// states are relevant when the processor is using the draining utilities in
// ProcessorBase.
type procState int

//go:generate stringer -type=procState
const (
	// StateRunning is the common state of a processor: it's producing rows for
	// its consumer and forwarding metadata from its input. Different processors
	// might have sub-states internally.
	//
	// If the consumer calls ConsumerDone or if the ProcOutputHelper.maxRowIdx is
	// reached, then the processor will transition to stateDraining. If the input
	// is exhausted, then the processor can transition to stateTrailingMeta
	// directly, although most always go through stateDraining.
	StateRunning procState = iota

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

// MoveToDraining switches the processor to the stateDraining. Only metadata is
// returned from now on. In this state, the processor is expected to drain its
// inputs (commonly by using DrainHelper()).
//
// If the processor has no input (ProcStateOpts.intputToDrain was not specified
// at init() time), then we move straight to the stateTrailingMeta.
//
// An error can be optionally passed. It will be the first piece of metadata
// returned by DrainHelper().
func (pb *ProcessorBase) MoveToDraining(err error) {
	if pb.State != StateRunning {
		// Calling MoveToDraining in any state is allowed in order to facilitate the
		// ConsumerDone() implementations that just call this unconditionally.
		// However, calling it with an error in states other than StateRunning is
		// not permitted.
		if err != nil {
			log.Fatalf(pb.Ctx, "MoveToDraining called in state %s with err: %s",
				pb.State, err)
		}
		return
	}

	if err != nil {
		pb.trailingMeta = append(pb.trailingMeta, ProducerMetadata{Err: err})
	}
	if len(pb.inputsToDrain) > 0 {
		// We go to stateDraining here. DrainHelper() will transition to
		// stateTrailingMeta when the inputs are drained (including if the inputs
		// are already drained).
		pb.State = stateDraining
		for _, input := range pb.inputsToDrain {
			input.ConsumerDone()
		}
	} else {
		pb.moveToTrailingMeta()
	}
}

// DrainHelper is supposed to be used in states draining and trailingMetadata.
// It deals with optionally draining an input and returning trailing meta. It
// also moves from stateDraining to stateTrailingMeta when appropriate.
func (pb *ProcessorBase) DrainHelper() *ProducerMetadata {
	if pb.State == StateRunning {
		log.Fatal(pb.Ctx, "drain helper called in StateRunning")
	}

	// trailingMeta always has priority; it seems like a good idea because it
	// causes metadata to be sent quickly after it is produced (e.g. the error
	// passed to MoveToDraining()).
	if len(pb.trailingMeta) > 0 {
		return pb.popTrailingMeta()
	}

	if pb.State != stateDraining {
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
func (pb *ProcessorBase) popTrailingMeta() *ProducerMetadata {
	if len(pb.trailingMeta) > 0 {
		meta := &pb.trailingMeta[0]
		pb.trailingMeta = pb.trailingMeta[1:]
		return meta
	}
	pb.State = stateExhausted
	return nil
}

// moveToTrailingMeta switches the processor to the "trailing meta" state: only
// trailing metadata is returned from now on. For simplicity, processors are
// encouraged to always use MoveToDraining() instead of this method, even when
// there's nothing to drain. moveToDrain() or DrainHelper() will internally call
// moveToTrailingMeta().
//
// trailingMetaCallback, if any, is called; it is expected to close the
// processor's inputs.
//
// This method is to be called when the processor is done producing rows and
// draining its inputs (if it wants to drain them).
func (pb *ProcessorBase) moveToTrailingMeta() {
	if pb.State == stateTrailingMeta || pb.State == stateExhausted {
		log.Fatalf(pb.Ctx, "moveToTrailingMeta called in state: %s", pb.State)
	}

	if pb.finishTrace != nil {
		pb.finishTrace()
	}

	pb.State = stateTrailingMeta
	if trace := getTraceData(pb.Ctx); trace != nil {
		pb.trailingMeta = append(pb.trailingMeta, ProducerMetadata{TraceData: trace})
	}
	// trailingMetaCallback is called after reading the tracing data because it
	// generally calls InternalClose, indirectly, which switches the context and
	// the span.
	if pb.trailingMetaCallback != nil {
		pb.trailingMeta = append(pb.trailingMeta, pb.trailingMetaCallback()...)
	} else {
		pb.InternalClose()
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
func (pb *ProcessorBase) processRowHelper(row sqlbase.EncDatumRow) sqlbase.EncDatumRow {
	outRow, ok, err := pb.out.ProcessRow(pb.Ctx, row)
	if err != nil {
		pb.MoveToDraining(err)
		return nil
	}
	if !ok {
		pb.MoveToDraining(nil /* err */)
	}
	// Note that outRow might be nil here.
	return outRow
}

// OutputTypes is part of the processor interface.
func (pb *ProcessorBase) OutputTypes() []sqlbase.ColumnType {
	return pb.out.outputTypes
}

// Run is part of the processor interface.
func (pb *ProcessorBase) Run(ctx context.Context, wg *sync.WaitGroup) {
	if pb.out.output == nil {
		panic("processor output not initialized for emitting rows")
	}
	ctx = pb.self.Start(ctx)
	Run(ctx, pb.self, pb.out.output)
	if wg != nil {
		wg.Done()
	}
}

// ProcStateOpts contains fields used by the ProcessorBase's family of functions
// that deal with draining and trailing metadata: the ProcessorBase implements
// generic useful functionality that needs to call back into the Processor.
type ProcStateOpts struct {
	// TrailingMetaCallback, if specified, is a callback to be called by
	// moveToTrailingMeta(). See ProcessorBase.TrailingMetaCallback.
	TrailingMetaCallback func() []ProducerMetadata
	// InputsToDrain, if specified, will be drained by DrainHelper().
	// MoveToDraining() calls ConsumerDone() on them, InternalClose() calls
	// ConsumerClosed() on them.
	InputsToDrain []RowSource
}

// Init initializes the ProcessorBase.
func (pb *ProcessorBase) Init(
	self RowSource,
	post *PostProcessSpec,
	types []sqlbase.ColumnType,
	flowCtx *FlowCtx,
	processorID int32,
	output RowReceiver,
	memMonitor *mon.BytesMonitor,
	opts ProcStateOpts,
) error {
	pb.self = self
	pb.flowCtx = flowCtx
	pb.processorID = processorID
	pb.MemMonitor = memMonitor
	pb.evalCtx = flowCtx.NewEvalCtx()
	pb.trailingMetaCallback = opts.TrailingMetaCallback
	pb.inputsToDrain = opts.InputsToDrain
	return pb.out.Init(post, types, pb.evalCtx, output)
}

// StartInternal prepares the ProcessorBase for execution. It returns the
// annotated context that's also stored in pb.ctx.
func (pb *ProcessorBase) StartInternal(ctx context.Context, name string) context.Context {
	pb.Ctx = ctx

	pb.origCtx = pb.Ctx
	pb.Ctx, pb.span = processorSpan(pb.Ctx, name)
	if pb.span != nil {
		pb.span.SetTag(tracing.TagPrefix+"processorid", pb.processorID)
	}
	pb.evalCtx.Context = pb.Ctx
	return pb.Ctx
}

// InternalClose helps processors implement the RowSource interface, performing
// common close functionality. Returns true iff the processor was not already
// closed.
//
// Notably, it calls ConsumerClosed() on all the inputsToDrain.
//
//   if pb.InternalClose() {
//     // Perform processor specific close work.
//   }
func (pb *ProcessorBase) InternalClose() bool {
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
		pb.Ctx = pb.origCtx

		// This prevents Next() from returning more rows.
		pb.out.consumerClosed()
	}
	return closing
}

// NewMonitor is a utility function used by processors to create a new
// memory monitor with the given name and start it. The returned monitor must
// be closed.
func NewMonitor(ctx context.Context, parent *mon.BytesMonitor, name string) *mon.BytesMonitor {
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
	localProcessors []LocalProcessor,
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
	if core.Windower != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		return newWindower(flowCtx, processorID, core.Windower, inputs[0], post, outputs[0])
	}
	if core.LocalPlanNode != nil {
		numInputs := 0
		if core.LocalPlanNode.NumInputs != nil {
			numInputs = int(*core.LocalPlanNode.NumInputs)
		}
		if err := checkNumInOut(inputs, outputs, numInputs, 1); err != nil {
			return nil, err
		}
		processor := localProcessors[*core.LocalPlanNode.RowSourceIdx]
		if numInputs == 1 {
			if err := processor.SetInput(ctx, inputs[0]); err != nil {
				return nil, err
			}
		} else if numInputs > 1 {
			return nil, errors.Errorf("invalid localPlanNode core with multiple inputs %+v", core.LocalPlanNode)
		}
		err := processor.InitWithOutput(post, outputs[0])
		return processor, err
	}
	return nil, errors.Errorf("unsupported processor core %s", core)
}

// LocalProcessor is a RowSourcedProcessor that needs to be initialized with
// its post processing spec and output row receiver. Most processors can accept
// these objects at creation time.
type LocalProcessor interface {
	RowSourcedProcessor
	// InitWithOutput initializes this processor.
	InitWithOutput(post *PostProcessSpec, output RowReceiver) error
	// SetInput initializes this LocalProcessor with an input RowSource. Not all
	// LocalProcessors need inputs, but this needs to be called if a
	// LocalProcessor expects to get its data from another RowSource.
	SetInput(ctx context.Context, input RowSource) error
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

func (spec *WindowerSpec_Frame_Mode) initFromAST(w tree.WindowFrameMode) {
	switch w {
	case tree.RANGE:
		*spec = WindowerSpec_Frame_RANGE
	case tree.ROWS:
		*spec = WindowerSpec_Frame_ROWS
	case tree.GROUPS:
		*spec = WindowerSpec_Frame_GROUPS
	default:
		panic("unexpected WindowFrameMode")
	}
}

func (spec *WindowerSpec_Frame_BoundType) initFromAST(bt tree.WindowFrameBoundType) {
	switch bt {
	case tree.UnboundedPreceding:
		*spec = WindowerSpec_Frame_UNBOUNDED_PRECEDING
	case tree.OffsetPreceding:
		*spec = WindowerSpec_Frame_OFFSET_PRECEDING
	case tree.CurrentRow:
		*spec = WindowerSpec_Frame_CURRENT_ROW
	case tree.OffsetFollowing:
		*spec = WindowerSpec_Frame_OFFSET_FOLLOWING
	case tree.UnboundedFollowing:
		*spec = WindowerSpec_Frame_UNBOUNDED_FOLLOWING
	default:
		panic("unexpected WindowFrameBoundType")
	}
}

// If offset exprs are present, we evaluate them and save the encoded results
// in the spec.
func (spec *WindowerSpec_Frame_Bounds) initFromAST(
	b tree.WindowFrameBounds, m tree.WindowFrameMode, evalCtx *tree.EvalContext,
) error {
	if b.StartBound == nil {
		return errors.Errorf("unexpected: Start Bound is nil")
	}
	spec.Start = WindowerSpec_Frame_Bound{}
	spec.Start.BoundType.initFromAST(b.StartBound.BoundType)
	if b.StartBound.HasOffset() {
		typedStartOffset := b.StartBound.OffsetExpr.(tree.TypedExpr)
		dStartOffset, err := typedStartOffset.Eval(evalCtx)
		if err != nil {
			return err
		}
		if dStartOffset == tree.DNull {
			return pgerror.NewErrorf(pgerror.CodeNullValueNotAllowedError, "frame starting offset must not be null")
		}
		switch m {
		case tree.ROWS:
			startOffset := int(tree.MustBeDInt(dStartOffset))
			if startOffset < 0 {
				return pgerror.NewErrorf(pgerror.CodeInvalidWindowFrameOffsetError, "frame starting offset must not be negative")
			}
			spec.Start.IntOffset = uint32(startOffset)
		case tree.RANGE:
			if isNegative(evalCtx, dStartOffset) {
				return pgerror.NewErrorf(pgerror.CodeInvalidWindowFrameOffsetError, "invalid preceding or following size in window function")
			}
			typ, err := sqlbase.DatumTypeToColumnType(dStartOffset.ResolvedType())
			if err != nil {
				return err
			}
			spec.Start.OffsetType = DatumInfo{Encoding: sqlbase.DatumEncoding_VALUE, Type: typ}
			var buf []byte
			var a sqlbase.DatumAlloc
			datum := sqlbase.DatumToEncDatum(typ, dStartOffset)
			buf, err = datum.Encode(&typ, &a, sqlbase.DatumEncoding_VALUE, buf)
			if err != nil {
				return err
			}
			spec.Start.TypedOffset = buf
		case tree.GROUPS:
			startOffset := int(tree.MustBeDInt(dStartOffset))
			if startOffset < 0 {
				return pgerror.NewErrorf(pgerror.CodeInvalidWindowFrameOffsetError, "frame starting offset must not be negative")
			}
			spec.Start.IntOffset = uint32(startOffset)
		}
	}

	if b.EndBound != nil {
		spec.End = &WindowerSpec_Frame_Bound{}
		spec.End.BoundType.initFromAST(b.EndBound.BoundType)
		if b.EndBound.HasOffset() {
			typedEndOffset := b.EndBound.OffsetExpr.(tree.TypedExpr)
			dEndOffset, err := typedEndOffset.Eval(evalCtx)
			if err != nil {
				return err
			}
			if dEndOffset == tree.DNull {
				return pgerror.NewErrorf(pgerror.CodeNullValueNotAllowedError, "frame ending offset must not be null")
			}
			switch m {
			case tree.ROWS:
				endOffset := int(tree.MustBeDInt(dEndOffset))
				if endOffset < 0 {
					return pgerror.NewErrorf(pgerror.CodeInvalidWindowFrameOffsetError, "frame ending offset must not be negative")
				}
				spec.End.IntOffset = uint32(endOffset)
			case tree.RANGE:
				if isNegative(evalCtx, dEndOffset) {
					return pgerror.NewErrorf(pgerror.CodeInvalidWindowFrameOffsetError, "invalid preceding or following size in window function")
				}
				typ, err := sqlbase.DatumTypeToColumnType(dEndOffset.ResolvedType())
				if err != nil {
					return err
				}
				spec.End.OffsetType = DatumInfo{Encoding: sqlbase.DatumEncoding_VALUE, Type: typ}
				var buf []byte
				var a sqlbase.DatumAlloc
				datum := sqlbase.DatumToEncDatum(typ, dEndOffset)
				buf, err = datum.Encode(&typ, &a, sqlbase.DatumEncoding_VALUE, buf)
				if err != nil {
					return err
				}
				spec.End.TypedOffset = buf
			case tree.GROUPS:
				endOffset := int(tree.MustBeDInt(dEndOffset))
				if endOffset < 0 {
					return pgerror.NewErrorf(pgerror.CodeInvalidWindowFrameOffsetError, "frame ending offset must not be negative")
				}
				spec.End.IntOffset = uint32(endOffset)
			}
		}
	}

	return nil
}

// isNegative returns whether offset is negative.
func isNegative(evalCtx *tree.EvalContext, offset tree.Datum) bool {
	switch o := offset.(type) {
	case *tree.DInt:
		return *o < 0
	case *tree.DDecimal:
		return o.Negative
	case *tree.DFloat:
		return *o < 0
	case *tree.DInterval:
		return o.Compare(evalCtx, &tree.DInterval{Duration: duration.Duration{}}) < 0
	default:
		panic("unexpected offset type")
	}
}

// InitFromAST initializes the spec based on tree.WindowFrame. It will evaluate
// offset expressions if present in the frame.
func (spec *WindowerSpec_Frame) InitFromAST(f *tree.WindowFrame, evalCtx *tree.EvalContext) error {
	spec.Mode.initFromAST(f.Mode)
	return spec.Bounds.initFromAST(f.Bounds, f.Mode, evalCtx)
}

func (spec WindowerSpec_Frame_Mode) convertToAST() tree.WindowFrameMode {
	switch spec {
	case WindowerSpec_Frame_RANGE:
		return tree.RANGE
	case WindowerSpec_Frame_ROWS:
		return tree.ROWS
	case WindowerSpec_Frame_GROUPS:
		return tree.GROUPS
	default:
		panic("unexpected WindowerSpec_Frame_Mode")
	}
}

func (spec WindowerSpec_Frame_BoundType) convertToAST() tree.WindowFrameBoundType {
	switch spec {
	case WindowerSpec_Frame_UNBOUNDED_PRECEDING:
		return tree.UnboundedPreceding
	case WindowerSpec_Frame_OFFSET_PRECEDING:
		return tree.OffsetPreceding
	case WindowerSpec_Frame_CURRENT_ROW:
		return tree.CurrentRow
	case WindowerSpec_Frame_OFFSET_FOLLOWING:
		return tree.OffsetFollowing
	case WindowerSpec_Frame_UNBOUNDED_FOLLOWING:
		return tree.UnboundedFollowing
	default:
		panic("unexpected WindowerSpec_Frame_BoundType")
	}
}

// convertToAST produces tree.WindowFrameBounds based on
// WindowerSpec_Frame_Bounds. Note that it might not be fully equivalent to
// original - if offsetExprs were present in original tree.WindowFrameBounds,
// they are not included.
func (spec WindowerSpec_Frame_Bounds) convertToAST() tree.WindowFrameBounds {
	bounds := tree.WindowFrameBounds{StartBound: &tree.WindowFrameBound{
		BoundType: spec.Start.BoundType.convertToAST(),
	}}
	if spec.End != nil {
		bounds.EndBound = &tree.WindowFrameBound{BoundType: spec.End.BoundType.convertToAST()}
	}
	return bounds
}

func (spec *WindowerSpec_Frame) convertToAST() *tree.WindowFrame {
	return &tree.WindowFrame{Mode: spec.Mode.convertToAST(), Bounds: spec.Bounds.convertToAST()}
}
