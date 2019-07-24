// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package distsqlrun

import (
	"context"
	"math"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
)

// Processor is a common interface implemented by all processors, used by the
// higher-level flow orchestration code.
type Processor interface {
	// OutputTypes returns the column types of the results (that are to be fed
	// through an output router).
	OutputTypes() []types.T

	// Run is the main loop of the processor.
	Run(context.Context)
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
	// renderExprs has length > 0 if we have a rendering. Only one of renderExprs
	// and outputCols can be set.
	renderExprs []exprHelper
	// outputCols is non-nil if we have a projection. Only one of renderExprs and
	// outputCols can be set. Note that 0-length projections are possible, in
	// which case outputCols will be 0-length but non-nil.
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
	outputTypes []types.T

	// offset is the number of rows that are suppressed.
	offset uint64
	// maxRowIdx is the number of rows after which we can stop (offset + limit),
	// or MaxUint64 if there is no limit.
	maxRowIdx uint64

	rowIdx uint64
}

// Reset resets this ProcOutputHelper, retaining allocated memory in its slices.
func (h *ProcOutputHelper) Reset() {
	*h = ProcOutputHelper{
		renderExprs: h.renderExprs[:0],
		outputTypes: h.outputTypes[:0],
	}
}

// Init sets up a ProcOutputHelper. The types describe the internal schema of
// the processor (as described for each processor core spec); they can be
// omitted if there is no filtering expression.
// Note that the types slice may be stored directly; the caller should not
// modify it.
func (h *ProcOutputHelper) Init(
	post *distsqlpb.PostProcessSpec, typs []types.T, evalCtx *tree.EvalContext, output RowReceiver,
) error {
	if !post.Projection && len(post.OutputColumns) > 0 {
		return errors.Errorf("post-processing has projection unset but output columns set: %s", post)
	}
	if post.Projection && len(post.RenderExprs) > 0 {
		return errors.Errorf("post-processing has both projection and rendering: %s", post)
	}
	h.output = output
	h.numInternalCols = len(typs)
	if post.Filter != (distsqlpb.Expression{}) {
		h.filter = &exprHelper{}
		if err := h.filter.init(post.Filter, typs, evalCtx); err != nil {
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
		nOutputCols := len(h.outputCols)
		if cap(h.outputTypes) >= nOutputCols {
			h.outputTypes = h.outputTypes[:nOutputCols]
		} else {
			h.outputTypes = make([]types.T, nOutputCols)
		}
		for i, c := range h.outputCols {
			h.outputTypes[i] = typs[c]
		}
	} else if nRenders := len(post.RenderExprs); nRenders > 0 {
		if cap(h.renderExprs) >= nRenders {
			h.renderExprs = h.renderExprs[:nRenders]
		} else {
			h.renderExprs = make([]exprHelper, nRenders)
		}
		if cap(h.outputTypes) >= nRenders {
			h.outputTypes = h.outputTypes[:nRenders]
		} else {
			h.outputTypes = make([]types.T, nRenders)
		}
		for i, expr := range post.RenderExprs {
			h.renderExprs[i] = exprHelper{}
			if err := h.renderExprs[i].init(expr, typs, evalCtx); err != nil {
				return err
			}
			h.outputTypes[i] = *h.renderExprs[i].expr.ResolvedType()
		}
	} else {
		// No rendering or projection.
		if cap(h.outputTypes) >= len(typs) {
			h.outputTypes = h.outputTypes[:len(typs)]
		} else {
			h.outputTypes = make([]types.T, len(typs))
		}
		copy(h.outputTypes, typs)
	}
	if h.outputCols != nil || len(h.renderExprs) > 0 {
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
	if h.outputCols == nil && len(h.renderExprs) == 0 {
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
		for j := range h.renderExprs {
			if h.renderExprs[j].vars.IndexedVarUsed(i) {
				colIdxs.Add(i)
				break
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
	meta *distsqlpb.ProducerMetadata,
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
		foundErr := meta.Err != nil
		consumerStatus = output.output.Push(nil /* row */, meta)
		if foundErr {
			consumerStatus = ConsumerClosed
		}
	} else {
		var err error
		consumerStatus, err = output.EmitRow(ctx, row)
		if err != nil {
			output.output.Push(nil /* row */, &distsqlpb.ProducerMetadata{Err: err})
			consumerStatus = ConsumerClosed
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

	if len(h.renderExprs) > 0 {
		// Rendering.
		for i := range h.renderExprs {
			datum, err := h.renderExprs[i].eval(row)
			if err != nil {
				return nil, false, err
			}
			h.outputRow[i] = sqlbase.DatumToEncDatum(&h.outputTypes[i], datum)
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
//       if outRow := p.ProcessRowHelper(row); outRow != nil {
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
	trailingMetaCallback func(context.Context) []distsqlpb.ProducerMetadata
	// trailingMeta is scratch space where metadata is stored to be returned
	// later.
	trailingMeta []distsqlpb.ProducerMetadata

	// inputsToDrain, if not empty, contains inputs to be drained by
	// DrainHelper(). MoveToDraining() calls ConsumerDone() on them,
	// InternalClose() calls ConsumerClosed() on then.
	//
	// ConsumerDone() is called on all inputs at once and then inputs are drained
	// one by one (in stateDraining, inputsToDrain[0] is the one currently being
	// drained).
	inputsToDrain []RowSource
}

// Reset resets this ProcessorBase, retaining allocated memory in slices.
func (pb *ProcessorBase) Reset() {
	pb.out.Reset()
	*pb = ProcessorBase{
		out:           pb.out,
		trailingMeta:  pb.trailingMeta[:0],
		inputsToDrain: pb.inputsToDrain[:0],
	}
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
	//
	// In stateDraining, processors are required to swallow
	// ReadWithinUncertaintyIntervalErrors received from its sources. We're
	// already draining, so we don't care about whatever data generated this
	// uncertainty error. Besides generally seeming like a good idea, doing this
	// allows us to offer a nice guarantee to SQL clients: a read-only query that
	// produces at most one row, run as an implicit txn, never produces retriable
	// errors, regardless of the size of the row being returned (in relation to
	// the size of the result buffer on the connection). One would naively expect
	// that to be true: either the error happens before any rows have been
	// delivered to the client, in which case the auto-retries kick in, or, if a
	// row has been delivered, then the query is done and so how can there be an
	// error? What our naive friend is ignoring is that, if it weren't for this
	// code, it'd be possible for a retriable error to sneak in after the query's
	// limit has been satisfied but while processors are still draining. Note
	// that uncertainty errors are not retried automatically by the leaf
	// TxnCoordSenders (i.e. by refresh txn interceptor).
	//
	// Other categories of errors might be safe to ignore too; however we
	// can't ignore all of them. Generally, we need to ensure that all the
	// trailing metadata (e.g. TxnCoordMeta's) make it to the gateway for
	// successful flows. If an error is telling us that some metadata might
	// have been dropped, we can't ignore that.
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
		pb.trailingMeta = append(pb.trailingMeta, distsqlpb.ProducerMetadata{Err: err})
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
func (pb *ProcessorBase) DrainHelper() *distsqlpb.ProducerMetadata {
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
			// Swallow ReadWithinUncertaintyIntervalErrors. See comments on
			// stateDraining.
			if err := meta.Err; err != nil {
				// We only look for UnhandledRetryableErrors. Local reads (which would
				// be transformed by the Root TxnCoordSender into
				// TransactionRetryWithProtoRefreshErrors) don't have any uncertainty.
				err = errors.Cause(err)
				if ure, ok := err.(*roachpb.UnhandledRetryableError); ok {
					uncertain := ure.PErr.Detail.GetReadWithinUncertaintyInterval()
					if uncertain != nil {
						continue
					}
				}
			}
			return meta
		}
	}
}

// popTrailingMeta peels off one piece of trailing metadata or advances to
// stateExhausted if there's no more trailing metadata.
func (pb *ProcessorBase) popTrailingMeta() *distsqlpb.ProducerMetadata {
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
	if pb.span != nil {
		if trace := getTraceData(pb.Ctx); trace != nil {
			pb.trailingMeta = append(pb.trailingMeta, distsqlpb.ProducerMetadata{TraceData: trace})
		}
	}
	// trailingMetaCallback is called after reading the tracing data because it
	// generally calls InternalClose, indirectly, which switches the context and
	// the span.
	if pb.trailingMetaCallback != nil {
		pb.trailingMeta = append(pb.trailingMeta, pb.trailingMetaCallback(pb.Ctx)...)
	} else {
		pb.InternalClose()
	}
}

// ProcessRowHelper is a wrapper on top of ProcOutputHelper.ProcessRow(). It
// takes care of handling errors and drain requests by moving the processor to
// stateDraining.
//
// It takes a row and returns the row after processing. The return value can be
// nil, in which case the caller shouldn't return anything to its consumer; it
// should continue processing other rows, with the awareness that the processor
// might have been transitioned to the draining phase.
func (pb *ProcessorBase) ProcessRowHelper(row sqlbase.EncDatumRow) sqlbase.EncDatumRow {
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
func (pb *ProcessorBase) OutputTypes() []types.T {
	return pb.out.outputTypes
}

// Run is part of the processor interface.
func (pb *ProcessorBase) Run(ctx context.Context) {
	if pb.out.output == nil {
		panic("processor output not initialized for emitting rows")
	}
	ctx = pb.self.Start(ctx)
	Run(ctx, pb.self, pb.out.output)
}

// ProcStateOpts contains fields used by the ProcessorBase's family of functions
// that deal with draining and trailing metadata: the ProcessorBase implements
// generic useful functionality that needs to call back into the Processor.
type ProcStateOpts struct {
	// TrailingMetaCallback, if specified, is a callback to be called by
	// moveToTrailingMeta(). See ProcessorBase.TrailingMetaCallback.
	TrailingMetaCallback func(context.Context) []distsqlpb.ProducerMetadata
	// InputsToDrain, if specified, will be drained by DrainHelper().
	// MoveToDraining() calls ConsumerDone() on them, InternalClose() calls
	// ConsumerClosed() on them.
	InputsToDrain []RowSource
}

// Init initializes the ProcessorBase.
func (pb *ProcessorBase) Init(
	self RowSource,
	post *distsqlpb.PostProcessSpec,
	types []types.T,
	flowCtx *FlowCtx,
	processorID int32,
	output RowReceiver,
	memMonitor *mon.BytesMonitor,
	opts ProcStateOpts,
) error {
	return pb.InitWithEvalCtx(
		self, post, types, flowCtx, flowCtx.NewEvalCtx(), processorID, output, memMonitor, opts,
	)
}

// InitWithEvalCtx initializes the ProcessorBase with a given EvalContext.
func (pb *ProcessorBase) InitWithEvalCtx(
	self RowSource,
	post *distsqlpb.PostProcessSpec,
	types []types.T,
	flowCtx *FlowCtx,
	evalCtx *tree.EvalContext,
	processorID int32,
	output RowReceiver,
	memMonitor *mon.BytesMonitor,
	opts ProcStateOpts,
) error {
	pb.self = self
	pb.flowCtx = flowCtx
	pb.evalCtx = evalCtx
	pb.processorID = processorID
	pb.MemMonitor = memMonitor
	pb.trailingMetaCallback = opts.TrailingMetaCallback
	pb.inputsToDrain = opts.InputsToDrain
	return pb.out.Init(post, types, pb.evalCtx, output)
}

// AddInputToDrain adds an input to drain when moving the processor to a
// draining state.
func (pb *ProcessorBase) AddInputToDrain(input RowSource) {
	pb.inputsToDrain = append(pb.inputsToDrain, input)
}

// AppendTrailingMeta appends metadata to the trailing metadata without changing
// the state to draining (as opposed to MoveToDraining).
func (pb *ProcessorBase) AppendTrailingMeta(meta distsqlpb.ProducerMetadata) {
	pb.trailingMeta = append(pb.trailingMeta, meta)
}

// StartInternal prepares the ProcessorBase for execution. It returns the
// annotated context that's also stored in pb.Ctx.
func (pb *ProcessorBase) StartInternal(ctx context.Context, name string) context.Context {
	pb.origCtx = ctx
	pb.Ctx, pb.span = processorSpan(ctx, name)
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

// ConsumerDone is part of the RowSource interface.
func (pb *ProcessorBase) ConsumerDone() {
	pb.MoveToDraining(nil /* err */)
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

func getFetcherInputStats(flowCtx *FlowCtx, f rowFetcher) (InputStats, bool) {
	rfsc, ok := f.(*rowFetcherStatCollector)
	if !ok {
		return InputStats{}, false
	}
	is, ok := getInputStats(flowCtx, rfsc.inputStatCollector)
	if !ok {
		return InputStats{}, false
	}
	// Add row fetcher start scan stall time to Next() stall time.
	if !flowCtx.testingKnobs.DeterministicStats {
		is.StallTime += rfsc.startScanStallTime
	}
	return is, true
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
		log.ReportOrPanic(context.Background(), nil, "%s already closed", log.Safe(name))
	}
	atomic.StoreUint32((*uint32)(&rb.consumerStatus), uint32(ConsumerClosed))
}

// processorSpan creates a child span for a processor (if we are doing any
// tracing). The returned span needs to be finished using tracing.FinishSpan.
func processorSpan(ctx context.Context, name string) (context.Context, opentracing.Span) {
	return tracing.ChildSpanSeparateRecording(ctx, name)
}

func newProcessor(
	ctx context.Context,
	flowCtx *FlowCtx,
	processorID int32,
	core *distsqlpb.ProcessorCoreUnion,
	post *distsqlpb.PostProcessSpec,
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
	if core.Ordinality != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		return newOrdinalityProcessor(flowCtx, processorID, core.Ordinality, inputs[0], post, outputs[0])
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
	if core.ZigzagJoiner != nil {
		if err := checkNumInOut(inputs, outputs, 0, 1); err != nil {
			return nil, err
		}
		return newZigzagJoiner(
			flowCtx, processorID, core.ZigzagJoiner, nil, post, outputs[0],
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
		case distsqlpb.BackfillerSpec_Index:
			return newIndexBackfiller(flowCtx, processorID, *core.Backfiller, post, outputs[0])
		case distsqlpb.BackfillerSpec_Column:
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
	if core.BulkRowWriter != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		return newBulkRowWriterProcessor(flowCtx, processorID, *core.BulkRowWriter, inputs[0], outputs[0])
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
		if err := processor.InitWithOutput(post, outputs[0]); err != nil {
			return nil, err
		}
		if numInputs == 1 {
			if err := processor.SetInput(ctx, inputs[0]); err != nil {
				return nil, err
			}
		} else if numInputs > 1 {
			return nil, errors.Errorf("invalid localPlanNode core with multiple inputs %+v", core.LocalPlanNode)
		}
		return processor, nil
	}
	if core.ChangeAggregator != nil {
		if err := checkNumInOut(inputs, outputs, 0, 1); err != nil {
			return nil, err
		}
		if NewChangeAggregatorProcessor == nil {
			return nil, errors.New("ChangeAggregator processor unimplemented")
		}
		return NewChangeAggregatorProcessor(flowCtx, processorID, *core.ChangeAggregator, outputs[0])
	}
	if core.ChangeFrontier != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		if NewChangeFrontierProcessor == nil {
			return nil, errors.New("ChangeFrontier processor unimplemented")
		}
		return NewChangeFrontierProcessor(flowCtx, processorID, *core.ChangeFrontier, inputs[0], outputs[0])
	}
	return nil, errors.Errorf("unsupported processor core %q", core)
}

// LocalProcessor is a RowSourcedProcessor that needs to be initialized with
// its post processing spec and output row receiver. Most processors can accept
// these objects at creation time.
type LocalProcessor interface {
	RowSourcedProcessor
	// InitWithOutput initializes this processor.
	InitWithOutput(post *distsqlpb.PostProcessSpec, output RowReceiver) error
	// SetInput initializes this LocalProcessor with an input RowSource. Not all
	// LocalProcessors need inputs, but this needs to be called if a
	// LocalProcessor expects to get its data from another RowSource.
	SetInput(ctx context.Context, input RowSource) error
}

// VectorizeAlwaysException is an object that returns whether or not execution
// should continue if experimental_vectorize=always and an error occurred when
// setting up the vectorized flow. Consider the case in which
// experimental_vectorize=always. The user must be able to unset this session
// variable without getting an error.
type VectorizeAlwaysException interface {
	// IsException returns whether this object should be an exception to the rule
	// that an inability to run this node in a vectorized flow should produce an
	// error.
	// TODO(asubiotto): This is the cleanest way I can think of to not error out
	// on SET statements when running with experimental_vectorize = always. If
	// there is a better way, we should get rid of this interface.
	IsException() bool
}

// NewReadImportDataProcessor is externally implemented and registered by
// ccl/sqlccl/csv.go.
var NewReadImportDataProcessor func(*FlowCtx, int32, distsqlpb.ReadImportDataSpec, RowReceiver) (Processor, error)

// NewSSTWriterProcessor is externally implemented and registered by
// ccl/sqlccl/csv.go.
var NewSSTWriterProcessor func(*FlowCtx, int32, distsqlpb.SSTWriterSpec, RowSource, RowReceiver) (Processor, error)

// NewCSVWriterProcessor is externally implemented.
var NewCSVWriterProcessor func(*FlowCtx, int32, distsqlpb.CSVWriterSpec, RowSource, RowReceiver) (Processor, error)

// NewChangeAggregatorProcessor is externally implemented.
var NewChangeAggregatorProcessor func(*FlowCtx, int32, distsqlpb.ChangeAggregatorSpec, RowReceiver) (Processor, error)

// NewChangeFrontierProcessor is externally implemented.
var NewChangeFrontierProcessor func(*FlowCtx, int32, distsqlpb.ChangeFrontierSpec, RowSource, RowReceiver) (Processor, error)
