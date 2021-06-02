// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execinfra

import (
	"context"
	"math"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/optional"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// Processor is a common interface implemented by all processors, used by the
// higher-level flow orchestration code.
type Processor interface {
	// OutputTypes returns the column types of the results (that are to be fed
	// through an output router).
	OutputTypes() []*types.T

	// MustBeStreaming indicates whether this processor is of "streaming" nature
	// and is expected to emit the output one row at a time (in both row-by-row
	// and the vectorized engines).
	MustBeStreaming() bool

	// Run is the main loop of the processor.
	Run(context.Context)
}

// DoesNotUseTxn is an interface implemented by some processors to mark that
// they do not use a txn. The DistSQLPlanner forbids multiple processors in a
// local flow from running in parallel if this is unknown since concurrent use
// of the RootTxn is forbidden (in a distributed flow these are leaf txns, so
// it doesn't matter).
// Implementing this interface lets the DistSQLPlanner know that it is ok to
// run this processor in an additional goroutine.
type DoesNotUseTxn interface {
	DoesNotUseTxn() bool
}

// ProcOutputHelper is a helper type that performs filtering and projection on
// the output of a processor.
type ProcOutputHelper struct {
	numInternalCols int
	RowAlloc        rowenc.EncDatumRowAlloc
	// renderExprs has length > 0 if we have a rendering. Only one of renderExprs
	// and outputCols can be set.
	renderExprs []execinfrapb.ExprHelper
	// outputCols is non-nil if we have a projection. Only one of renderExprs and
	// outputCols can be set. Note that 0-length projections are possible, in
	// which case outputCols will be 0-length but non-nil.
	outputCols []uint32

	outputRow rowenc.EncDatumRow

	// OutputTypes is the schema of the rows produced by the processor after
	// post-processing (i.e. the rows that are pushed through a router).
	//
	// If renderExprs is set, these types correspond to the types of those
	// expressions.
	// If outputCols is set, these types correspond to the types of
	// those columns.
	// If neither is set, this is the internal schema of the processor.
	OutputTypes []*types.T

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
		OutputTypes: h.OutputTypes[:0],
	}
}

// Init sets up a ProcOutputHelper. The types describe the internal schema of
// the processor (as described for each processor core spec); they can be
// omitted if there is no filtering expression.
// Note that the types slice may be stored directly; the caller should not
// modify it.
func (h *ProcOutputHelper) Init(
	post *execinfrapb.PostProcessSpec,
	coreOutputTypes []*types.T,
	semaCtx *tree.SemaContext,
	evalCtx *tree.EvalContext,
) error {
	if !post.Projection && len(post.OutputColumns) > 0 {
		return errors.Errorf("post-processing has projection unset but output columns set: %s", post)
	}
	if post.Projection && len(post.RenderExprs) > 0 {
		return errors.Errorf("post-processing has both projection and rendering: %s", post)
	}
	h.numInternalCols = len(coreOutputTypes)
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
		if cap(h.OutputTypes) >= nOutputCols {
			h.OutputTypes = h.OutputTypes[:nOutputCols]
		} else {
			h.OutputTypes = make([]*types.T, nOutputCols)
		}
		for i, c := range h.outputCols {
			h.OutputTypes[i] = coreOutputTypes[c]
		}
	} else if nRenders := len(post.RenderExprs); nRenders > 0 {
		if cap(h.renderExprs) >= nRenders {
			h.renderExprs = h.renderExprs[:nRenders]
		} else {
			h.renderExprs = make([]execinfrapb.ExprHelper, nRenders)
		}
		if cap(h.OutputTypes) >= nRenders {
			h.OutputTypes = h.OutputTypes[:nRenders]
		} else {
			h.OutputTypes = make([]*types.T, nRenders)
		}
		for i, expr := range post.RenderExprs {
			h.renderExprs[i] = execinfrapb.ExprHelper{}
			if err := h.renderExprs[i].Init(expr, coreOutputTypes, semaCtx, evalCtx); err != nil {
				return err
			}
			h.OutputTypes[i] = h.renderExprs[i].Expr.ResolvedType()
		}
	} else {
		// No rendering or projection.
		if cap(h.OutputTypes) >= len(coreOutputTypes) {
			h.OutputTypes = h.OutputTypes[:len(coreOutputTypes)]
		} else {
			h.OutputTypes = make([]*types.T, len(coreOutputTypes))
		}
		copy(h.OutputTypes, coreOutputTypes)
	}
	if h.outputCols != nil || len(h.renderExprs) > 0 {
		// We're rendering or projecting, so allocate an output row.
		h.outputRow = h.RowAlloc.AllocRow(len(h.OutputTypes))
	}

	h.offset = post.Offset
	if post.Limit == 0 || post.Limit >= math.MaxUint64-h.offset {
		h.maxRowIdx = math.MaxUint64
	} else {
		h.maxRowIdx = h.offset + post.Limit
	}

	return nil
}

// NeededColumns calculates the set of internal processor columns that are
// actually used by the post-processing stage.
func (h *ProcOutputHelper) NeededColumns() (colIdxs util.FastIntSet) {
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
		// See if render expressions require this column.
		for j := range h.renderExprs {
			if h.renderExprs[j].Vars.IndexedVarUsed(i) {
				colIdxs.Add(i)
				break
			}
		}
	}

	return colIdxs
}

// EmitRow sends a row through the post-processing stage. The same row can be
// reused.
//
// It returns the consumer's status that was observed when pushing this row. If
// an error is returned, it's coming from the ProcOutputHelper's filtering or
// rendering processing; the output has not been closed and it's the caller's
// responsibility to push the error to the output.
//
// Note: check out rowexec.emitHelper() for a useful wrapper.
func (h *ProcOutputHelper) EmitRow(
	ctx context.Context, row rowenc.EncDatumRow, output RowReceiver,
) (ConsumerStatus, error) {
	if output == nil {
		panic("output RowReceiver is not set for emitting rows")
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
		log.InfofDepth(ctx, 1, "pushing row %s", outRow.String(h.OutputTypes))
	}
	if r := output.Push(outRow, nil); r != NeedMoreRows {
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
	ctx context.Context, row rowenc.EncDatumRow,
) (_ rowenc.EncDatumRow, moreRowsOK bool, _ error) {
	if h.rowIdx >= h.maxRowIdx {
		return nil, false, nil
	}

	h.rowIdx++
	if h.rowIdx <= h.offset {
		// Suppress row.
		return nil, true, nil
	}

	if len(h.renderExprs) > 0 {
		// Rendering.
		for i := range h.renderExprs {
			datum, err := h.renderExprs[i].Eval(row)
			if err != nil {
				return nil, false, err
			}
			h.outputRow[i] = rowenc.DatumToEncDatum(h.OutputTypes[i], datum)
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

// consumerClosed stops output of additional rows from ProcessRow.
func (h *ProcOutputHelper) consumerClosed() {
	h.rowIdx = h.maxRowIdx
}

// Stats returns output statistics.
func (h *ProcOutputHelper) Stats() execinfrapb.OutputStats {
	return execinfrapb.OutputStats{
		NumTuples: optional.MakeUint(h.rowIdx),
	}
}

// ProcessorConstructor is a function that creates a Processor. It is
// abstracted away so that we could create mixed flows (i.e. a vectorized flow
// with wrapped processors) without bringing a dependency on sql/rowexec
// package into sql/colexec package.
type ProcessorConstructor func(
	ctx context.Context,
	flowCtx *FlowCtx,
	processorID int32,
	core *execinfrapb.ProcessorCoreUnion,
	post *execinfrapb.PostProcessSpec,
	inputs []RowSource,
	outputs []RowReceiver,
	localProcessors []LocalProcessor,
) (Processor, error)

// ProcessorBase is supposed to be embedded by Processors. It provides
// facilities for dealing with filtering and projection (through a
// ProcOutputHelper) and for implementing the RowSource interface (draining,
// trailing metadata).
type ProcessorBase struct {
	ProcessorBaseNoHelper

	// OutputHelper is used to handle the post-processing spec.
	OutputHelper ProcOutputHelper

	// MemMonitor is the processor's memory monitor.
	MemMonitor *mon.BytesMonitor

	// SemaCtx is used to avoid allocating a new SemaCtx during processor setup.
	SemaCtx tree.SemaContext
}

// ProcessorBaseNoHelper is slightly reduced version of ProcessorBase that
// should be used by the processors that don't need to handle the
// post-processing spec.
type ProcessorBaseNoHelper struct {
	self RowSource

	ProcessorID int32

	// Output is the consumer of the rows produced by this ProcessorBase. If
	// Output is nil, one can invoke ProcessRow to obtain the post-processed row
	// directly.
	Output RowReceiver

	FlowCtx *FlowCtx

	// EvalCtx is used for expression evaluation. It overrides the one in flowCtx.
	EvalCtx *tree.EvalContext

	// Closed is set by InternalClose(). Once set, the processor's tracing span
	// has been closed.
	Closed bool

	// Ctx and span contain the tracing state while the processor is active
	// (i.e. hasn't been closed). Initialized using flowCtx.Ctx (which should not be otherwise
	// used).
	Ctx  context.Context
	span *tracing.Span
	// origCtx is the context from which ctx was derived. InternalClose() resets
	// ctx to this.
	origCtx context.Context

	State procState

	// ExecStatsForTrace, if set, will be called before getting the trace data from
	// the span and adding the recording to the trailing metadata. The returned
	// ComponentStats are associated with the processor's span. The Component
	// field of the returned stats will be set by the calling code.
	//
	// Can return nil.
	ExecStatsForTrace func() *execinfrapb.ComponentStats
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
	trailingMetaCallback func() []execinfrapb.ProducerMetadata
	// trailingMeta is scratch space where metadata is stored to be returned
	// later.
	trailingMeta []execinfrapb.ProducerMetadata

	// inputsToDrain, if not empty, contains inputs to be drained by
	// DrainHelper(). MoveToDraining() calls ConsumerDone() on them,
	// InternalClose() calls ConsumerClosed() on then.
	//
	// ConsumerDone() is called on all inputs at once and then inputs are drained
	// one by one (in StateDraining, inputsToDrain[curInputToDrain] is the one
	// currently being drained).
	inputsToDrain []RowSource

	// curInputToDrain is the index into inputsToDrain that needs to be drained
	// next.
	curInputToDrain int
}

// MustBeStreaming implements the Processor interface.
func (pb *ProcessorBaseNoHelper) MustBeStreaming() bool {
	return false
}

// Reset resets this ProcessorBaseNoHelper, retaining allocated memory in
// slices.
func (pb *ProcessorBaseNoHelper) Reset() {
	// Deeply reset the slices so that we don't hold onto the old objects.
	for i := range pb.trailingMeta {
		pb.trailingMeta[i] = execinfrapb.ProducerMetadata{}
	}
	for i := range pb.inputsToDrain {
		pb.inputsToDrain[i] = nil
	}
	*pb = ProcessorBaseNoHelper{
		trailingMeta:  pb.trailingMeta[:0],
		inputsToDrain: pb.inputsToDrain[:0],
	}
}

// Reset resets this ProcessorBase, retaining allocated memory in slices.
func (pb *ProcessorBase) Reset() {
	pb.ProcessorBaseNoHelper.Reset()
	pb.OutputHelper.Reset()
	*pb = ProcessorBase{
		ProcessorBaseNoHelper: pb.ProcessorBaseNoHelper,
		OutputHelper:          pb.OutputHelper,
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
	// reached, then the processor will transition to StateDraining. If the input
	// is exhausted, then the processor can transition to StateTrailingMeta
	// directly, although most always go through StateDraining.
	StateRunning procState = iota

	// StateDraining is the state in which the processor is forwarding metadata
	// from its input and otherwise ignoring all rows. Once the input is
	// exhausted, the processor will transition to StateTrailingMeta.
	//
	// In StateDraining, processors are required to swallow
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
	// trailing metadata (e.g. LeafTxnFinalState's) make it to the gateway for
	// successful flows. If an error is telling us that some metadata might
	// have been dropped, we can't ignore that.
	StateDraining

	// StateTrailingMeta is the state in which the processor is outputting final
	// metadata such as the tracing information or the LeafTxnFinalState. Once all the
	// trailing metadata has been produced, the processor transitions to
	// StateExhausted.
	StateTrailingMeta

	// StateExhausted is the state of a processor that has no more rows or
	// metadata to produce.
	StateExhausted
)

// MoveToDraining switches the processor to the StateDraining. Only metadata is
// returned from now on. In this state, the processor is expected to drain its
// inputs (commonly by using DrainHelper()).
//
// If the processor has no input (ProcStateOpts.inputToDrain was not specified
// at init() time), then we move straight to the StateTrailingMeta.
//
// An error can be optionally passed. It will be the first piece of metadata
// returned by DrainHelper().
func (pb *ProcessorBaseNoHelper) MoveToDraining(err error) {
	if pb.State != StateRunning {
		// Calling MoveToDraining in any state is allowed in order to facilitate the
		// ConsumerDone() implementations that just call this unconditionally.
		// However, calling it with an error in states other than StateRunning is
		// not permitted.
		if err != nil {
			logcrash.ReportOrPanic(
				pb.Ctx,
				&pb.FlowCtx.Cfg.Settings.SV,
				"MoveToDraining called in state %s with err: %+v",
				pb.State, err)
		}
		return
	}

	if err != nil {
		pb.trailingMeta = append(pb.trailingMeta, execinfrapb.ProducerMetadata{Err: err})
	}
	if pb.curInputToDrain < len(pb.inputsToDrain) {
		// We go to StateDraining here. DrainHelper() will transition to
		// StateTrailingMeta when the inputs are drained (including if the inputs
		// are already drained).
		pb.State = StateDraining
		for _, input := range pb.inputsToDrain[pb.curInputToDrain:] {
			input.ConsumerDone()
		}
	} else {
		pb.moveToTrailingMeta()
	}
}

// DrainHelper is supposed to be used in states draining and trailingMetadata.
// It deals with optionally draining an input and returning trailing meta. It
// also moves from StateDraining to StateTrailingMeta when appropriate.
func (pb *ProcessorBaseNoHelper) DrainHelper() *execinfrapb.ProducerMetadata {
	if pb.State == StateRunning {
		logcrash.ReportOrPanic(
			pb.Ctx,
			&pb.FlowCtx.Cfg.Settings.SV,
			"drain helper called in StateRunning",
		)
	}

	// trailingMeta always has priority; it seems like a good idea because it
	// causes metadata to be sent quickly after it is produced (e.g. the error
	// passed to MoveToDraining()).
	if len(pb.trailingMeta) > 0 {
		return pb.popTrailingMeta()
	}

	if pb.State != StateDraining {
		return nil
	}

	// Ignore all rows; only return meta.
	for {
		input := pb.inputsToDrain[pb.curInputToDrain]

		row, meta := input.Next()
		if row == nil && meta == nil {
			pb.curInputToDrain++
			if pb.curInputToDrain >= len(pb.inputsToDrain) {
				pb.moveToTrailingMeta()
				return pb.popTrailingMeta()
			}
			continue
		}
		if meta != nil {
			// Swallow ReadWithinUncertaintyIntervalErrors. See comments on
			// StateDraining.
			if ShouldSwallowReadWithinUncertaintyIntervalError(meta) {
				continue
			}
			return meta
		}
	}
}

// ShouldSwallowReadWithinUncertaintyIntervalError examines meta and returns
// true if it should be swallowed and not propagated further. It is the case if
// meta contains roachpb.ReadWithinUncertaintyIntervalError.
func ShouldSwallowReadWithinUncertaintyIntervalError(meta *execinfrapb.ProducerMetadata) bool {
	if err := meta.Err; err != nil {
		// We only look for UnhandledRetryableErrors. Local reads (which would
		// be transformed by the Root TxnCoordSender into
		// TransactionRetryWithProtoRefreshErrors) don't have any uncertainty.
		if ure := (*roachpb.UnhandledRetryableError)(nil); errors.As(err, &ure) {
			if _, uncertain := ure.PErr.GetDetail().(*roachpb.ReadWithinUncertaintyIntervalError); uncertain {
				return true
			}
		}
	}
	return false
}

// popTrailingMeta peels off one piece of trailing metadata or advances to
// StateExhausted if there's no more trailing metadata.
func (pb *ProcessorBaseNoHelper) popTrailingMeta() *execinfrapb.ProducerMetadata {
	if len(pb.trailingMeta) > 0 {
		meta := &pb.trailingMeta[0]
		pb.trailingMeta = pb.trailingMeta[1:]
		return meta
	}
	pb.State = StateExhausted
	return nil
}

// ExecStatsForTraceHijacker is an interface that allows us to hijack
// ExecStatsForTrace function from the ProcessorBase.
type ExecStatsForTraceHijacker interface {
	// HijackExecStatsForTrace returns ExecStatsForTrace function, if set, and
	// sets it to nil. The caller becomes responsible for collecting and
	// propagating the execution statistics.
	HijackExecStatsForTrace() func() *execinfrapb.ComponentStats
}

var _ ExecStatsForTraceHijacker = &ProcessorBase{}

// HijackExecStatsForTrace is a part of the ExecStatsForTraceHijacker interface.
func (pb *ProcessorBase) HijackExecStatsForTrace() func() *execinfrapb.ComponentStats {
	execStatsForTrace := pb.ExecStatsForTrace
	pb.ExecStatsForTrace = nil
	return execStatsForTrace
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
func (pb *ProcessorBaseNoHelper) moveToTrailingMeta() {
	if pb.State == StateTrailingMeta || pb.State == StateExhausted {
		logcrash.ReportOrPanic(
			pb.Ctx,
			&pb.FlowCtx.Cfg.Settings.SV,
			"moveToTrailingMeta called in state: %s",
			pb.State,
		)
	}

	pb.State = StateTrailingMeta
	if pb.span != nil {
		if pb.ExecStatsForTrace != nil {
			if stats := pb.ExecStatsForTrace(); stats != nil {
				stats.Component = pb.FlowCtx.ProcessorComponentID(pb.ProcessorID)
				pb.span.RecordStructured(stats)
			}
		}
		if trace := pb.span.GetRecording(); trace != nil {
			pb.trailingMeta = append(pb.trailingMeta, execinfrapb.ProducerMetadata{TraceData: trace})
		}
	}

	if util.CrdbTestBuild && pb.Ctx == nil {
		panic(
			errors.AssertionFailedf(
				"unexpected nil ProcessorBase.Ctx when draining. Was StartInternal called?",
			),
		)
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

// ProcessRowHelper is a wrapper on top of ProcOutputHelper.ProcessRow(). It
// takes care of handling errors and drain requests by moving the processor to
// StateDraining.
//
// It takes a row and returns the row after processing. The return value can be
// nil, in which case the caller shouldn't return anything to its consumer; it
// should continue processing other rows, with the awareness that the processor
// might have been transitioned to the draining phase.
func (pb *ProcessorBase) ProcessRowHelper(row rowenc.EncDatumRow) rowenc.EncDatumRow {
	outRow, ok, err := pb.OutputHelper.ProcessRow(pb.Ctx, row)
	if err != nil {
		pb.MoveToDraining(err)
		return nil
	}
	if !ok {
		pb.MoveToDraining(nil /* err */)
	}
	// Note that outRow might be nil here.
	// TODO(yuzefovich): there is a problem with this logging when MetadataTest*
	// processors are planned - there is a mismatch between the row and the
	// output types (rendering is added to the stage of test processors and the
	// actual processors that are inputs to the test ones have an unset post
	// processing; I think that we need to set the post processing on the stages
	// of processors below the test ones).
	//if outRow != nil && log.V(3) && pb.Ctx != nil {
	//	log.InfofDepth(pb.Ctx, 1, "pushing row %s", outRow.String(pb.Out.OutputTypes))
	//}
	return outRow
}

// OutputTypes is part of the Processor interface.
func (pb *ProcessorBase) OutputTypes() []*types.T {
	return pb.OutputHelper.OutputTypes
}

// Run is part of the Processor interface.
func (pb *ProcessorBaseNoHelper) Run(ctx context.Context) {
	if pb.Output == nil {
		panic("processor output is not set for emitting rows")
	}
	pb.self.Start(ctx)
	Run(pb.Ctx, pb.self, pb.Output)
}

// ProcStateOpts contains fields used by the ProcessorBase's family of functions
// that deal with draining and trailing metadata: the ProcessorBase implements
// generic useful functionality that needs to call back into the Processor.
type ProcStateOpts struct {
	// TrailingMetaCallback, if specified, is a callback to be called by
	// moveToTrailingMeta(). See ProcessorBase.TrailingMetaCallback.
	TrailingMetaCallback func() []execinfrapb.ProducerMetadata
	// InputsToDrain, if specified, will be drained by DrainHelper().
	// MoveToDraining() calls ConsumerDone() on them, InternalClose() calls
	// ConsumerClosed() on them.
	InputsToDrain []RowSource
}

// Init initializes the ProcessorBase.
// - coreOutputTypes are the type schema of the rows output by the processor
// core (i.e. the "internal schema" of the processor, see
// execinfrapb.ProcessorSpec for more details).
func (pb *ProcessorBase) Init(
	self RowSource,
	post *execinfrapb.PostProcessSpec,
	coreOutputTypes []*types.T,
	flowCtx *FlowCtx,
	processorID int32,
	output RowReceiver,
	memMonitor *mon.BytesMonitor,
	opts ProcStateOpts,
) error {
	return pb.InitWithEvalCtx(
		self, post, coreOutputTypes, flowCtx, flowCtx.NewEvalCtx(), processorID, output, memMonitor, opts,
	)
}

// InitWithEvalCtx initializes the ProcessorBase with a given EvalContext.
// - coreOutputTypes are the type schema of the rows output by the processor
// core (i.e. the "internal schema" of the processor, see
// execinfrapb.ProcessorSpec for more details).
func (pb *ProcessorBase) InitWithEvalCtx(
	self RowSource,
	post *execinfrapb.PostProcessSpec,
	coreOutputTypes []*types.T,
	flowCtx *FlowCtx,
	evalCtx *tree.EvalContext,
	processorID int32,
	output RowReceiver,
	memMonitor *mon.BytesMonitor,
	opts ProcStateOpts,
) error {
	pb.ProcessorBaseNoHelper.Init(
		self, flowCtx, evalCtx, processorID, output, opts,
	)
	pb.MemMonitor = memMonitor

	// Hydrate all types used in the processor.
	resolver := flowCtx.TypeResolverFactory.NewTypeResolver(evalCtx.Txn)
	if err := resolver.HydrateTypeSlice(evalCtx.Context, coreOutputTypes); err != nil {
		return err
	}
	pb.SemaCtx = tree.MakeSemaContext()
	pb.SemaCtx.TypeResolver = resolver

	return pb.OutputHelper.Init(post, coreOutputTypes, &pb.SemaCtx, pb.EvalCtx)
}

// Init initializes the ProcessorBaseNoHelper.
func (pb *ProcessorBaseNoHelper) Init(
	self RowSource,
	flowCtx *FlowCtx,
	evalCtx *tree.EvalContext,
	processorID int32,
	output RowReceiver,
	opts ProcStateOpts,
) {
	pb.self = self
	pb.FlowCtx = flowCtx
	pb.EvalCtx = evalCtx
	pb.ProcessorID = processorID
	pb.Output = output
	pb.trailingMetaCallback = opts.TrailingMetaCallback
	if opts.InputsToDrain != nil {
		// Only initialize this if non-nil, because we cache the slice of inputs
		// to drain in our object pool, and overwriting the slice in Init would
		// be horribly counterproductive.
		pb.inputsToDrain = opts.InputsToDrain
	}
}

// AddInputToDrain adds an input to drain when moving the processor to a
// draining state.
func (pb *ProcessorBaseNoHelper) AddInputToDrain(input RowSource) {
	pb.inputsToDrain = append(pb.inputsToDrain, input)
}

// AppendTrailingMeta appends metadata to the trailing metadata without changing
// the state to draining (as opposed to MoveToDraining).
func (pb *ProcessorBase) AppendTrailingMeta(meta execinfrapb.ProducerMetadata) {
	pb.trailingMeta = append(pb.trailingMeta, meta)
}

// ProcessorSpan creates a child span for a processor (if we are doing any
// tracing). The returned span needs to be finished using tracing.FinishSpan.
func ProcessorSpan(ctx context.Context, name string) (context.Context, *tracing.Span) {
	return tracing.ChildSpanRemote(ctx, name)
}

// StartInternal prepares the ProcessorBase for execution. It returns the
// annotated context that's also stored in pb.Ctx.
//
// It is likely that this method is called from RowSource.Start implementation,
// and the recommended layout is the following:
//   ctx = pb.StartInternal(ctx, name)
//   < inputs >.Start(ctx) // if there are any inputs-RowSources to pb
//   < other initialization >
// so that the caller doesn't mistakenly use old ctx object.
func (pb *ProcessorBaseNoHelper) StartInternal(ctx context.Context, name string) context.Context {
	return pb.startImpl(ctx, true /* createSpan */, name)
}

// StartInternalNoSpan does the same as StartInternal except that it does not
// start a span. This is used by pass-through components whose goal is to be a
// silent translation layer for components that actually do work (e.g. a
// planNodeToRowSource wrapping an insertNode, or a columnarizer wrapping a
// rowexec flow).
func (pb *ProcessorBaseNoHelper) StartInternalNoSpan(ctx context.Context) context.Context {
	return pb.startImpl(ctx, false /* createSpan */, "")
}

func (pb *ProcessorBaseNoHelper) startImpl(
	ctx context.Context, createSpan bool, spanName string,
) context.Context {
	pb.origCtx = ctx
	if createSpan {
		pb.Ctx, pb.span = ProcessorSpan(ctx, spanName)
		if pb.span != nil && pb.span.IsVerbose() {
			pb.span.SetTag(execinfrapb.FlowIDTagKey, pb.FlowCtx.ID.String())
			pb.span.SetTag(execinfrapb.ProcessorIDTagKey, pb.ProcessorID)
		}
	} else {
		pb.Ctx = ctx
	}
	pb.EvalCtx.Context = pb.Ctx
	return pb.Ctx
}

// InternalClose helps processors implement the RowSource interface, performing
// common close functionality. Returns true iff the processor was not already
// closed.
//
// Notably, it calls ConsumerClosed() on all the inputsToDrain and updates
// pb.Ctx to the context passed into StartInternal() call.
//
//   if pb.InternalClose() {
//     // Perform processor specific close work.
//   }
func (pb *ProcessorBase) InternalClose() bool {
	closing := pb.ProcessorBaseNoHelper.InternalClose()
	if closing {
		// This prevents Next() from returning more rows.
		pb.OutputHelper.consumerClosed()
	}
	return closing
}

// InternalClose is the meat of ProcessorBase.InternalClose.
func (pb *ProcessorBaseNoHelper) InternalClose() bool {
	closing := !pb.Closed
	// Protection around double closing is useful for allowing ConsumerClosed() to
	// be called on processors that have already closed themselves by moving to
	// StateTrailingMeta.
	if closing {
		for _, input := range pb.inputsToDrain[pb.curInputToDrain:] {
			input.ConsumerClosed()
		}

		pb.Closed = true
		pb.span.Finish()
		pb.span = nil
		// Reset the context so that any incidental uses after this point do not
		// access the finished span.
		pb.Ctx = pb.origCtx
		pb.EvalCtx.Context = pb.origCtx
	}
	return closing
}

// ConsumerDone is part of the RowSource interface.
func (pb *ProcessorBaseNoHelper) ConsumerDone() {
	pb.MoveToDraining(nil /* err */)
}

// ConsumerClosed is part of the RowSource interface.
func (pb *ProcessorBaseNoHelper) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	pb.InternalClose()
}

// NewMonitor is a utility function used by processors to create a new
// memory monitor with the given name and start it. The returned monitor must
// be closed.
func NewMonitor(ctx context.Context, parent *mon.BytesMonitor, name string) *mon.BytesMonitor {
	monitor := mon.NewMonitorInheritWithLimit(name, 0 /* limit */, parent)
	monitor.Start(ctx, parent, mon.BoundAccount{})
	return monitor
}

// NewLimitedMonitor is a utility function used by processors to create a new
// limited memory monitor with the given name and start it. The returned monitor
// must be closed. The limit is determined by SessionData.WorkMemLimit (stored
// inside of the flowCtx) but overridden to 1 if
// ServerConfig.TestingKnobs.ForceDiskSpill is set or
// ServerConfig.TestingKnobs.MemoryLimitBytes if not.
func NewLimitedMonitor(
	ctx context.Context, parent *mon.BytesMonitor, flowCtx *FlowCtx, name string,
) *mon.BytesMonitor {
	limitedMon := mon.NewMonitorInheritWithLimit(name, GetWorkMemLimit(flowCtx), parent)
	limitedMon.Start(ctx, parent, mon.BoundAccount{})
	return limitedMon
}

// NewLimitedMonitorNoFlowCtx is the same as NewLimitedMonitor and should be
// used when the caller doesn't have an access to *FlowCtx.
func NewLimitedMonitorNoFlowCtx(
	ctx context.Context,
	parent *mon.BytesMonitor,
	config *ServerConfig,
	sd *sessiondata.SessionData,
	name string,
) *mon.BytesMonitor {
	// Create a fake FlowCtx populating only the required fields.
	flowCtx := &FlowCtx{
		Cfg: config,
		EvalCtx: &tree.EvalContext{
			SessionData: sd,
		},
	}
	return NewLimitedMonitor(ctx, parent, flowCtx, name)
}

// LocalProcessor is a RowSourcedProcessor that needs to be initialized with
// its post processing spec and output row receiver. Most processors can accept
// these objects at creation time.
type LocalProcessor interface {
	RowSourcedProcessor
	// InitWithOutput initializes this processor.
	InitWithOutput(flowCtx *FlowCtx, post *execinfrapb.PostProcessSpec, output RowReceiver) error
	// SetInput initializes this LocalProcessor with an input RowSource. Not all
	// LocalProcessors need inputs, but this needs to be called if a
	// LocalProcessor expects to get its data from another RowSource.
	SetInput(ctx context.Context, input RowSource) error
}
