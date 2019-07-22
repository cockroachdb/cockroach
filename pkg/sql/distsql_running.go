// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlplan"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/opentracing/opentracing-go"
)

// To allow queries to send out flow RPCs in parallel, we use a pool of workers
// that can issue the RPCs on behalf of the running code. The pool is shared by
// multiple queries.
const numRunners = 16

const clientRejectedMsg string = "client rejected when attempting to run DistSQL plan"

// runnerRequest is the request that is sent (via a channel) to a worker.
type runnerRequest struct {
	ctx        context.Context
	nodeDialer *nodedialer.Dialer
	flowReq    *distsqlpb.SetupFlowRequest
	nodeID     roachpb.NodeID
	resultChan chan<- runnerResult
}

// runnerResult is returned by a worker (via a channel) for each received
// request.
type runnerResult struct {
	nodeID roachpb.NodeID
	err    error
}

func (req runnerRequest) run() {
	res := runnerResult{nodeID: req.nodeID}

	conn, err := req.nodeDialer.Dial(req.ctx, req.nodeID)
	if err != nil {
		res.err = err
	} else {
		client := distsqlpb.NewDistSQLClient(conn)
		// TODO(radu): do we want a timeout here?
		resp, err := client.SetupFlow(req.ctx, req.flowReq)
		if err != nil {
			res.err = err
		} else {
			res.err = resp.Error.ErrorDetail(req.ctx)
		}
	}
	req.resultChan <- res
}

func (dsp *DistSQLPlanner) initRunners() {
	// This channel has to be unbuffered because we want to only be able to send
	// requests if a worker is actually there to receive them.
	dsp.runnerChan = make(chan runnerRequest)
	for i := 0; i < numRunners; i++ {
		dsp.stopper.RunWorker(context.TODO(), func(context.Context) {
			runnerChan := dsp.runnerChan
			stopChan := dsp.stopper.ShouldStop()
			for {
				select {
				case req := <-runnerChan:
					req.run()

				case <-stopChan:
					return
				}
			}
		})
	}
}

// setupFlows sets up all the flows specified in flows using the provided state.
// It will first attempt to set up all remote flows using the dsp workers if
// available or sequentially if not, and then finally set up the gateway flow,
// whose output is the DistSQLReceiver provided. This flow is then returned to
// be run.
func (dsp *DistSQLPlanner) setupFlows(
	ctx context.Context,
	evalCtx *extendedEvalContext,
	txnCoordMeta *roachpb.TxnCoordMeta,
	flows map[roachpb.NodeID]*distsqlpb.FlowSpec,
	recv *DistSQLReceiver,
	localState distsqlrun.LocalState,
) (context.Context, *distsqlrun.Flow, error) {
	thisNodeID := dsp.nodeDesc.NodeID

	evalCtxProto := distsqlpb.MakeEvalContext(evalCtx.EvalContext)
	setupReq := distsqlpb.SetupFlowRequest{
		TxnCoordMeta: txnCoordMeta,
		Version:      distsqlrun.Version,
		EvalContext:  evalCtxProto,
		TraceKV:      evalCtx.Tracing.KVTracingEnabled(),
	}

	// Start all the flows except the flow on this node (there is always a flow on
	// this node).
	var resultChan chan runnerResult
	if len(flows) > 1 {
		resultChan = make(chan runnerResult, len(flows)-1)
	}
	// First check to see whether or not to even try vectorizing the flow. The
	// goal here is to determine up front whether all of the flows can be
	// vectorized. If any of them can't, turn off the setting.
	// TODO(yuzefovich): this is a safe but quite inefficient way of setting up
	// vectorized flows since the flows will effectively be planned twice. Remove
	// this once logic of falling back to DistSQL is bullet-proof.
	if evalCtx.SessionData.Vectorize != sessiondata.VectorizeOff {
		for _, spec := range flows {
			if err := distsqlrun.SupportsVectorized(
				ctx, &distsqlrun.FlowCtx{EvalCtx: &evalCtx.EvalContext, NodeID: -1}, spec.Processors,
			); err != nil {
				// Vectorization attempt failed with an error.
				returnVectorizationSetupError := false
				if evalCtx.SessionData.Vectorize == sessiondata.VectorizeAlways {
					returnVectorizationSetupError = true
					// If running with VectorizeAlways, this check makes sure that we can
					// still run SET statements (mostly to set experimental_vectorize to
					// off) and the like.
					if len(spec.Processors) == 1 &&
						spec.Processors[0].Core.LocalPlanNode != nil {
						rsidx := spec.Processors[0].Core.LocalPlanNode.RowSourceIdx
						if rsidx != nil {
							lp := localState.LocalProcs[*rsidx]
							if z, ok := lp.(distsqlrun.VectorizeAlwaysException); ok {
								if z.IsException() {
									returnVectorizationSetupError = false
								}
							}
						}
					}
				}
				log.VEventf(ctx, 1, "failed to vectorize: %s", err)
				if returnVectorizationSetupError {
					return nil, nil, err
				}
				// Vectorization is not supported for this flow, so we override the
				// setting. Note that we need to restore the setting of evalCtx since
				// it can be used in the future, but setupReq is used only once, so we
				// don't need to restore it.
				setupReq.EvalContext.Vectorize = int32(sessiondata.VectorizeOff)
				origMode := evalCtx.SessionData.Vectorize
				evalCtx.SessionData.Vectorize = sessiondata.VectorizeOff
				defer func() { evalCtx.SessionData.Vectorize = origMode }()
				break
			}
		}
	}
	for nodeID, flowSpec := range flows {
		if nodeID == thisNodeID {
			// Skip this node.
			continue
		}
		req := setupReq
		req.Flow = *flowSpec
		runReq := runnerRequest{
			ctx:        ctx,
			nodeDialer: dsp.nodeDialer,
			flowReq:    &req,
			nodeID:     nodeID,
			resultChan: resultChan,
		}
		defer distsqlplan.ReleaseSetupFlowRequest(&req)

		// Send out a request to the workers; if no worker is available, run
		// directly.
		select {
		case dsp.runnerChan <- runReq:
		default:
			runReq.run()
		}
	}

	var firstErr error
	// Now wait for all the flows to be scheduled on remote nodes. Note that we
	// are not waiting for the flows themselves to complete.
	for i := 0; i < len(flows)-1; i++ {
		res := <-resultChan
		if firstErr == nil {
			firstErr = res.err
		}
		// TODO(radu): accumulate the flows that we failed to set up and move them
		// into the local flow.
	}
	if firstErr != nil {
		return nil, nil, firstErr
	}

	// Set up the flow on this node.
	localReq := setupReq
	localReq.Flow = *flows[thisNodeID]
	defer distsqlplan.ReleaseSetupFlowRequest(&localReq)
	ctx, flow, err := dsp.distSQLSrv.SetupLocalSyncFlow(ctx, evalCtx.Mon, &localReq, recv, localState)
	if err != nil {
		return nil, nil, err
	}

	return ctx, flow, nil
}

// Run executes a physical plan. The plan should have been finalized using
// FinalizePlan.
//
// All errors encountered are reported to the DistSQLReceiver's resultWriter.
// Additionally, if the error is a "communication error" (an error encountered
// while using that resultWriter), the error is also stored in
// DistSQLReceiver.commErr. That can be tested to see if a client session needs
// to be closed.
//
// Args:
// - txn is the transaction in which the plan will run. If nil, the different
// processors are expected to manage their own internal transactions.
// - evalCtx is the evaluation context in which the plan will run. It might be
// mutated.
// - finishedSetupFn, if non-nil, is called synchronously after all the
// processors have successfully started up.
//
// It returns a non-nil (although it can be a noop when an error is
// encountered) cleanup function that must be called in order to release the
// resources.
func (dsp *DistSQLPlanner) Run(
	planCtx *PlanningCtx,
	txn *client.Txn,
	plan *PhysicalPlan,
	recv *DistSQLReceiver,
	evalCtx *extendedEvalContext,
	finishedSetupFn func(),
) (cleanup func()) {
	ctx := planCtx.ctx

	var (
		localState   distsqlrun.LocalState
		txnCoordMeta *roachpb.TxnCoordMeta
	)
	// NB: putting part of evalCtx in localState means it might be mutated down
	// the line.
	localState.EvalContext = &evalCtx.EvalContext
	if planCtx.isLocal {
		localState.IsLocal = true
		localState.LocalProcs = plan.LocalProcessors
		localState.Txn = txn
	} else if txn != nil {
		// If the plan is not local, we will have to set up leaf txns using the
		// txnCoordMeta.
		meta, err := txn.GetTxnCoordMetaOrRejectClient(ctx)
		if err != nil {
			log.Infof(ctx, "%s: %s", clientRejectedMsg, err)
			recv.SetError(err)
			return func() {}
		}
		meta.StripRootToLeaf()
		txnCoordMeta = &meta
	}

	if err := planCtx.sanityCheckAddresses(); err != nil {
		recv.SetError(err)
		return func() {}
	}

	flows := plan.GenerateFlowSpecs(dsp.nodeDesc.NodeID /* gateway */)

	if logPlanDiagram {
		log.VEvent(ctx, 1, "creating plan diagram")
		json, url, err := distsqlpb.GeneratePlanDiagramURL(flows)
		if err != nil {
			log.Infof(ctx, "Error generating diagram: %s", err)
		} else {
			log.Infof(ctx, "Plan diagram JSON:\n%s", json)
			log.Infof(ctx, "Plan diagram URL:\n%s", url.String())
		}
	}

	log.VEvent(ctx, 1, "running DistSQL plan")

	dsp.distSQLSrv.ServerConfig.Metrics.QueryStart()
	defer dsp.distSQLSrv.ServerConfig.Metrics.QueryStop()

	recv.outputTypes = plan.ResultTypes
	recv.resultToStreamColMap = plan.PlanToStreamColMap

	ctx, flow, err := dsp.setupFlows(ctx, evalCtx, txnCoordMeta, flows, recv, localState)
	if err != nil {
		recv.SetError(err)
		return func() {}
	}

	if finishedSetupFn != nil {
		finishedSetupFn()
	}

	// TODO(radu): this should go through the flow scheduler.
	if err := flow.Run(ctx, func() {}); err != nil {
		log.Fatalf(ctx, "unexpected error from syncFlow.Start(): %s "+
			"The error should have gone to the consumer.", err)
	}

	// TODO(yuzefovich): it feels like this closing should happen after
	// PlanAndRun. We should refactor this and get rid off ignoreClose field.
	if planCtx.planner != nil && !planCtx.ignoreClose {
		// planCtx can change before the cleanup function is executed, so we make
		// a copy of the planner and bind it to the function.
		curPlan := &planCtx.planner.curPlan
		return func() {
			// We need to close the planNode tree we translated into a DistSQL plan
			// before flow.Cleanup, which closes memory accounts that expect to be
			// emptied.
			curPlan.execErr = recv.resultWriter.Err()
			curPlan.close(ctx)
			flow.Cleanup(ctx)
		}
	}

	// ignoreClose is set to true meaning that someone else will handle the
	// closing of the current plan, so we simply clean up the flow.
	return func() {
		flow.Cleanup(ctx)
	}
}

// DistSQLReceiver is a RowReceiver that writes results to a rowResultWriter.
// This is where the DistSQL execution meets the SQL Session - the RowContainer
// comes from a client Session.
//
// DistSQLReceiver also update the RangeDescriptorCache and the LeaseholderCache
// in response to DistSQL metadata about misplanned ranges.
type DistSQLReceiver struct {
	ctx context.Context

	// resultWriter is the interface which we send results to.
	resultWriter rowResultWriter

	stmtType tree.StatementType

	// outputTypes are the types of the result columns produced by the plan.
	outputTypes []types.T

	// resultToStreamColMap maps result columns to columns in the distsqlrun results
	// stream.
	resultToStreamColMap []int

	// noColsRequired indicates that the caller is only interested in the
	// existence of a single row. Used by subqueries in EXISTS mode.
	noColsRequired bool

	// discardRows is set when we want to discard rows (for testing/benchmarks).
	// See EXECUTE .. DISCARD ROWS.
	discardRows bool

	// commErr keeps track of the error received from interacting with the
	// resultWriter. This represents a "communication error" and as such is unlike
	// query execution errors: when the DistSQLReceiver is used within a SQL
	// session, such errors mean that we have to bail on the session.
	// Query execution errors are reported to the resultWriter. For some client's
	// convenience, communication errors are also reported to the resultWriter.
	//
	// Once set, no more rows are accepted.
	commErr error

	// txnAbortedErr is atomically set to an errWrap when the KV txn finishes
	// asynchronously. Further results should not be returned to the client, as
	// they risk missing seeing their own writes. Upon the next Push(), err is set
	// and ConsumerStatus is set to ConsumerClosed.
	txnAbortedErr atomic.Value

	row    tree.Datums
	status distsqlrun.ConsumerStatus
	alloc  sqlbase.DatumAlloc
	closed bool

	rangeCache *kv.RangeDescriptorCache
	leaseCache *kv.LeaseHolderCache
	tracing    *SessionTracing
	cleanup    func()

	// The transaction in which the flow producing data for this
	// receiver runs. The DistSQLReceiver updates the transaction in
	// response to RetryableTxnError's and when distributed processors
	// pass back TxnCoordMeta objects via ProducerMetas. Nil if no
	// transaction should be updated on errors (i.e. if the flow overall
	// doesn't run in a transaction).
	txn *client.Txn

	// A handler for clock signals arriving from remote nodes. This should update
	// this node's clock.
	updateClock func(observedTs hlc.Timestamp)

	// bytesRead and rowsRead track the corresponding metrics while executing the
	// statement.
	bytesRead int64
	rowsRead  int64
}

// errWrap is a container for an error, for use with atomic.Value, which
// requires that all of things stored in it must have the same concrete type.
type errWrap struct {
	err error
}

// rowResultWriter is a subset of CommandResult to be used with the
// DistSQLReceiver. It's implemented by RowResultWriter.
type rowResultWriter interface {
	// AddRow writes a result row.
	// Note that the caller owns the row slice and might reuse it.
	AddRow(ctx context.Context, row tree.Datums) error
	IncrementRowsAffected(n int)
	SetError(error)
	Err() error
}

// errOnlyResultWriter is a rowResultWriter that only supports receiving an
// error. All other functions that deal with producing results panic.
type errOnlyResultWriter struct {
	err error
}

var _ rowResultWriter = &errOnlyResultWriter{}

func (w *errOnlyResultWriter) SetError(err error) {
	w.err = err
}
func (w *errOnlyResultWriter) Err() error {
	return w.err
}

func (w *errOnlyResultWriter) AddRow(ctx context.Context, row tree.Datums) error {
	panic("AddRow not supported by errOnlyResultWriter")
}
func (w *errOnlyResultWriter) IncrementRowsAffected(n int) {
	panic("IncrementRowsAffected not supported by errOnlyResultWriter")
}

var _ distsqlrun.RowReceiver = &DistSQLReceiver{}

var receiverSyncPool = sync.Pool{
	New: func() interface{} {
		return &DistSQLReceiver{}
	},
}

// MakeDistSQLReceiver creates a DistSQLReceiver.
//
// ctx is the Context that the receiver will use throughout its
// lifetime. resultWriter is the container where the results will be
// stored. If only the row count is needed, this can be nil.
//
// txn is the transaction in which the producer flow runs; it will be updated
// on errors. Nil if the flow overall doesn't run in a transaction.
func MakeDistSQLReceiver(
	ctx context.Context,
	resultWriter rowResultWriter,
	stmtType tree.StatementType,
	rangeCache *kv.RangeDescriptorCache,
	leaseCache *kv.LeaseHolderCache,
	txn *client.Txn,
	updateClock func(observedTs hlc.Timestamp),
	tracing *SessionTracing,
) *DistSQLReceiver {
	consumeCtx, cleanup := tracing.TraceExecConsume(ctx)
	r := receiverSyncPool.Get().(*DistSQLReceiver)
	*r = DistSQLReceiver{
		ctx:          consumeCtx,
		cleanup:      cleanup,
		resultWriter: resultWriter,
		rangeCache:   rangeCache,
		leaseCache:   leaseCache,
		txn:          txn,
		updateClock:  updateClock,
		stmtType:     stmtType,
		tracing:      tracing,
	}
	// If this receiver is part of a distributed flow and isn't using the root
	// transaction, we need to sure that the flow is canceled when the root
	// transaction finishes (i.e. it is abandoned, aborted, or committed), so that
	// we don't return results to the client that might have missed seeing their
	// own writes. The committed case shouldn't happen. This isn't necessary if
	// the flow is running locally and is using the root transaction.
	//
	// TODO(andrei): Instead of doing this, should we lift this transaction
	// monitoring to connExecutor and have it cancel the SQL txn's context? Or for
	// that matter, should the TxnCoordSender cancel the context itself?
	if r.txn != nil && r.txn.Type() == client.LeafTxn {
		r.txn.OnCurrentIncarnationFinish(func(err error) {
			r.txnAbortedErr.Store(errWrap{err: err})
		})
	}
	return r
}

// Release releases this DistSQLReceiver back to the pool.
func (r *DistSQLReceiver) Release() {
	*r = DistSQLReceiver{}
	receiverSyncPool.Put(r)
}

// clone clones the receiver for running subqueries. Not all fields are cloned,
// only those required for running subqueries.
func (r *DistSQLReceiver) clone() *DistSQLReceiver {
	ret := receiverSyncPool.Get().(*DistSQLReceiver)
	*ret = DistSQLReceiver{
		ctx:         r.ctx,
		cleanup:     func() {},
		rangeCache:  r.rangeCache,
		leaseCache:  r.leaseCache,
		txn:         r.txn,
		updateClock: r.updateClock,
		stmtType:    tree.Rows,
		tracing:     r.tracing,
	}
	return ret
}

// SetError provides a convenient way for a client to pass in an error, thus
// pretending that a query execution error happened. The error is passed along
// to the resultWriter.
func (r *DistSQLReceiver) SetError(err error) {
	r.resultWriter.SetError(err)
}

// Push is part of the RowReceiver interface.
func (r *DistSQLReceiver) Push(
	row sqlbase.EncDatumRow, meta *distsqlpb.ProducerMetadata,
) distsqlrun.ConsumerStatus {
	if meta != nil {
		if meta.TxnCoordMeta != nil {
			if r.txn != nil {
				if r.txn.ID() == meta.TxnCoordMeta.Txn.ID {
					r.txn.AugmentTxnCoordMeta(r.ctx, *meta.TxnCoordMeta)
				}
			} else {
				r.resultWriter.SetError(
					errors.Errorf("received a leaf TxnCoordMeta (%s); but have no root", meta.TxnCoordMeta))
			}
		}
		if meta.Err != nil {
			// Check if the error we just received should take precedence over a
			// previous error (if any).
			if roachpb.ErrPriority(meta.Err) > roachpb.ErrPriority(r.resultWriter.Err()) {
				if r.txn != nil {
					if err, ok := errors.If(meta.Err, func(err error) (v interface{}, ok bool) {
						v, ok = err.(*roachpb.UnhandledRetryableError)
						return v, ok
					}); ok {
						retryErr := err.(*roachpb.UnhandledRetryableError)
						// Update the txn in response to remote errors. In the non-DistSQL
						// world, the TxnCoordSender handles "unhandled" retryable errors,
						// but this one is coming from a distributed SQL node, which has
						// left the handling up to the root transaction.
						meta.Err = r.txn.UpdateStateOnRemoteRetryableErr(r.ctx, &retryErr.PErr)
						// Update the clock with information from the error. On non-DistSQL
						// code paths, the DistSender does this.
						// TODO(andrei): We don't propagate clock signals on success cases
						// through DistSQL; we should. We also don't propagate them through
						// non-retryable errors; we also should.
						r.updateClock(retryErr.PErr.Now)
					}
				}
				r.resultWriter.SetError(meta.Err)
			}
		}
		if len(meta.Ranges) > 0 {
			if err := r.updateCaches(r.ctx, meta.Ranges); err != nil && r.resultWriter.Err() == nil {
				r.resultWriter.SetError(err)
			}
		}
		if len(meta.TraceData) > 0 {
			span := opentracing.SpanFromContext(r.ctx)
			if span == nil {
				r.resultWriter.SetError(
					errors.New("trying to ingest remote spans but there is no recording span set up"))
			} else if err := tracing.ImportRemoteSpans(span, meta.TraceData); err != nil {
				r.resultWriter.SetError(errors.Errorf("error ingesting remote spans: %s", err))
			}
		}
		if meta.Metrics != nil {
			r.bytesRead += meta.Metrics.BytesRead
			r.rowsRead += meta.Metrics.RowsRead
			meta.Metrics.Release()
			meta.Release()
		}
		return r.status
	}
	if r.resultWriter.Err() == nil && r.txnAbortedErr.Load() != nil {
		r.resultWriter.SetError(r.txnAbortedErr.Load().(errWrap).err)
	}
	if r.resultWriter.Err() == nil && r.ctx.Err() != nil {
		r.resultWriter.SetError(r.ctx.Err())
	}
	if r.resultWriter.Err() != nil {
		// TODO(andrei): We should drain here if we weren't canceled.
		return distsqlrun.ConsumerClosed
	}
	if r.status != distsqlrun.NeedMoreRows {
		return r.status
	}

	if r.stmtType != tree.Rows {
		// We only need the row count. planNodeToRowSource is set up to handle
		// ensuring that the last stage in the pipeline will return a single-column
		// row with the row count in it, so just grab that and exit.
		r.resultWriter.IncrementRowsAffected(int(tree.MustBeDInt(row[0].Datum)))
		return r.status
	}

	if r.discardRows {
		// Discard rows.
		return r.status
	}

	// If no columns are needed by the output, the consumer is only looking for
	// whether a single row is pushed or not, so the contents do not matter, and
	// planNodeToRowSource is not set up to handle decoding the row.
	if r.noColsRequired {
		r.row = []tree.Datum{}
		r.status = distsqlrun.ConsumerClosed
	} else {
		if r.row == nil {
			r.row = make(tree.Datums, len(r.resultToStreamColMap))
		}
		for i, resIdx := range r.resultToStreamColMap {
			err := row[resIdx].EnsureDecoded(&r.outputTypes[resIdx], &r.alloc)
			if err != nil {
				r.resultWriter.SetError(err)
				r.status = distsqlrun.ConsumerClosed
				return r.status
			}
			r.row[i] = row[resIdx].Datum
		}
	}
	r.tracing.TraceExecRowsResult(r.ctx, r.row)
	// Note that AddRow accounts for the memory used by the Datums.
	if commErr := r.resultWriter.AddRow(r.ctx, r.row); commErr != nil {
		// ErrLimitedResultClosed is not a real error, it is a
		// signal to stop distsql and return success to the client.
		if !errors.Is(commErr, ErrLimitedResultClosed) {
			// Set the error on the resultWriter too, for the convenience of some of the
			// clients. If clients don't care to differentiate between communication
			// errors and query execution errors, they can simply inspect
			// resultWriter.Err(). Also, this function itself doesn't care about the
			// distinction and just uses resultWriter.Err() to see if we're still
			// accepting results.
			r.resultWriter.SetError(commErr)

			// We don't need to shut down the connection
			// if there's a portal-related error. This is
			// definitely a layering violation, but is part
			// of some accepted technical debt (see comments on
			// sql/pgwire.limitedCommandResult.moreResultsNeeded).
			// Instead of changing the signature of AddRow, we have
			// a sentinel error that is handled specially here.
			if !errors.Is(commErr, ErrLimitedResultNotSupported) {
				r.commErr = commErr
			}
		}
		// TODO(andrei): We should drain here. Metadata from this query would be
		// useful, particularly as it was likely a large query (since AddRow()
		// above failed, presumably with an out-of-memory error).
		r.status = distsqlrun.ConsumerClosed
	}
	return r.status
}

var (
	// ErrLimitedResultNotSupported is an error produced by pgwire
	// indicating an unsupported feature of row count limits was attempted.
	ErrLimitedResultNotSupported = unimplemented.NewWithIssue(4035, "execute row count limits only partially supported")
	// ErrLimitedResultClosed is a sentinel error produced by pgwire
	// indicating the portal should be closed without error.
	ErrLimitedResultClosed = errors.New("row count limit closed")
)

// ProducerDone is part of the RowReceiver interface.
func (r *DistSQLReceiver) ProducerDone() {
	if r.txn != nil {
		r.txn.OnCurrentIncarnationFinish(nil)
	}
	if r.closed {
		panic("double close")
	}
	r.closed = true
	r.cleanup()
}

// Types is part of the RowReceiver interface.
func (r *DistSQLReceiver) Types() []types.T {
	return r.outputTypes
}

// updateCaches takes information about some ranges that were mis-planned and
// updates the range descriptor and lease-holder caches accordingly.
//
// TODO(andrei): updating these caches is not perfect: we can clobber newer
// information that someone else has populated because there's no timing info
// anywhere. We also may fail to remove stale info from the LeaseHolderCache if
// the ids of the ranges that we get are different than the ids in that cache.
func (r *DistSQLReceiver) updateCaches(ctx context.Context, ranges []roachpb.RangeInfo) error {
	// Update the RangeDescriptorCache.
	rngDescs := make([]roachpb.RangeDescriptor, len(ranges))
	for i, ri := range ranges {
		rngDescs[i] = ri.Desc
	}
	if err := r.rangeCache.InsertRangeDescriptors(ctx, rngDescs...); err != nil {
		return err
	}

	// Update the LeaseHolderCache.
	for _, ri := range ranges {
		r.leaseCache.Update(ctx, ri.Desc.RangeID, ri.Lease.Replica.StoreID)
	}
	return nil
}

// PlanAndRunSubqueries returns false if an error was encountered and sets that
// error in the provided receiver.
func (dsp *DistSQLPlanner) PlanAndRunSubqueries(
	ctx context.Context,
	planner *planner,
	evalCtxFactory func() *extendedEvalContext,
	subqueryPlans []subquery,
	recv *DistSQLReceiver,
	maybeDistribute bool,
) bool {
	for planIdx, subqueryPlan := range subqueryPlans {
		if err := dsp.planAndRunSubquery(
			ctx,
			planIdx,
			subqueryPlan,
			planner,
			evalCtxFactory(),
			subqueryPlans,
			recv,
			maybeDistribute,
		); err != nil {
			recv.SetError(err)
			return false
		}
	}

	return true
}

func (dsp *DistSQLPlanner) planAndRunSubquery(
	ctx context.Context,
	planIdx int,
	subqueryPlan subquery,
	planner *planner,
	evalCtx *extendedEvalContext,
	subqueryPlans []subquery,
	recv *DistSQLReceiver,
	maybeDistribute bool,
) error {
	subqueryMonitor := mon.MakeMonitor(
		"subquery",
		mon.MemoryResource,
		dsp.distSQLSrv.Metrics.CurBytesCount,
		dsp.distSQLSrv.Metrics.MaxBytesHist,
		-1, /* use default block size */
		noteworthyMemoryUsageBytes,
		dsp.distSQLSrv.Settings,
	)
	subqueryMonitor.Start(ctx, evalCtx.Mon, mon.BoundAccount{})
	defer subqueryMonitor.Stop(ctx)

	subqueryMemAccount := subqueryMonitor.MakeBoundAccount()
	defer subqueryMemAccount.Close(ctx)

	var subqueryPlanCtx *PlanningCtx
	var distributeSubquery bool
	if maybeDistribute {
		distributeSubquery = shouldDistributePlan(
			ctx, planner.SessionData().DistSQLMode, dsp, subqueryPlan.plan)
	}
	if distributeSubquery {
		subqueryPlanCtx = dsp.NewPlanningCtx(ctx, evalCtx, planner.txn)
	} else {
		subqueryPlanCtx = dsp.newLocalPlanningCtx(ctx, evalCtx)
	}

	subqueryPlanCtx.isLocal = !distributeSubquery
	subqueryPlanCtx.planner = planner
	subqueryPlanCtx.stmtType = tree.Rows
	// Don't close the top-level plan from subqueries - someone else will handle
	// that.
	subqueryPlanCtx.ignoreClose = true

	subqueryPhysPlan, err := dsp.createPlanForNode(subqueryPlanCtx, subqueryPlan.plan)
	if err != nil {
		return err
	}
	dsp.FinalizePlan(subqueryPlanCtx, &subqueryPhysPlan)

	// TODO(arjun): #28264: We set up a row container, wrap it in a row
	// receiver, and use it and serialize the results of the subquery. The type
	// of the results stored in the container depends on the type of the subquery.
	subqueryRecv := recv.clone()
	var typ sqlbase.ColTypeInfo
	var rows *rowcontainer.RowContainer
	if subqueryPlan.execMode == distsqlrun.SubqueryExecModeExists {
		subqueryRecv.noColsRequired = true
		typ = sqlbase.ColTypeInfoFromColTypes([]types.T{})
	} else {
		// Apply the PlanToStreamColMap projection to the ResultTypes to get the
		// final set of output types for the subquery. The reason this is necessary
		// is that the output schema of a query sometimes contains columns necessary
		// to merge the streams, but that aren't required by the final output of the
		// query. These get projected out, so we need to similarly adjust the
		// expected result types of the subquery here.
		colTypes := make([]types.T, len(subqueryPhysPlan.PlanToStreamColMap))
		for i, resIdx := range subqueryPhysPlan.PlanToStreamColMap {
			colTypes[i] = subqueryPhysPlan.ResultTypes[resIdx]
		}
		typ = sqlbase.ColTypeInfoFromColTypes(colTypes)
	}
	rows = rowcontainer.NewRowContainer(subqueryMemAccount, typ, 0)
	defer rows.Close(ctx)

	subqueryRowReceiver := NewRowResultWriter(rows)
	subqueryRecv.resultWriter = subqueryRowReceiver
	subqueryPlans[planIdx].started = true
	dsp.Run(subqueryPlanCtx, planner.txn, &subqueryPhysPlan, subqueryRecv, evalCtx, nil /* finishedSetupFn */)()
	if subqueryRecv.commErr != nil {
		return subqueryRecv.commErr
	}
	if err := subqueryRowReceiver.Err(); err != nil {
		return err
	}
	switch subqueryPlan.execMode {
	case distsqlrun.SubqueryExecModeExists:
		// For EXISTS expressions, all we want to know if there is at least one row.
		hasRows := rows.Len() != 0
		subqueryPlans[planIdx].result = tree.MakeDBool(tree.DBool(hasRows))
	case distsqlrun.SubqueryExecModeAllRows, distsqlrun.SubqueryExecModeAllRowsNormalized:
		var result tree.DTuple
		for rows.Len() > 0 {
			row := rows.At(0)
			rows.PopFirst()
			if row.Len() == 1 {
				// This seems hokey, but if we don't do this then the subquery expands
				// to a tuple of tuples instead of a tuple of values and an expression
				// like "k IN (SELECT foo FROM bar)" will fail because we're comparing
				// a single value against a tuple.
				result.D = append(result.D, row[0])
			} else {
				result.D = append(result.D, &tree.DTuple{D: row})
			}
		}

		if subqueryPlan.execMode == distsqlrun.SubqueryExecModeAllRowsNormalized {
			result.Normalize(&evalCtx.EvalContext)
		}
		subqueryPlans[planIdx].result = &result
	case distsqlrun.SubqueryExecModeOneRow:
		switch rows.Len() {
		case 0:
			subqueryPlans[planIdx].result = tree.DNull
		case 1:
			row := rows.At(0)
			switch row.Len() {
			case 1:
				subqueryPlans[planIdx].result = row[0]
			default:
				subqueryPlans[planIdx].result = &tree.DTuple{D: rows.At(0)}
			}
		default:
			return pgerror.Newf(pgcode.CardinalityViolation,
				"more than one row returned by a subquery used as an expression")
		}
	default:
		return fmt.Errorf("unexpected subqueryExecMode: %d", subqueryPlan.execMode)
	}
	return nil
}

// PlanAndRun generates a physical plan from a planNode tree and executes it. It
// assumes that the tree is supported (see CheckSupport).
//
// All errors encountered are reported to the DistSQLReceiver's resultWriter.
// Additionally, if the error is a "communication error" (an error encountered
// while using that resultWriter), the error is also stored in
// DistSQLReceiver.commErr. That can be tested to see if a client session needs
// to be closed.
//
// It returns a non-nil (although it can be a noop when an error is
// encountered) cleanup function that must be called in order to release the
// resources.
func (dsp *DistSQLPlanner) PlanAndRun(
	ctx context.Context,
	evalCtx *extendedEvalContext,
	planCtx *PlanningCtx,
	txn *client.Txn,
	plan planNode,
	recv *DistSQLReceiver,
) (cleanup func()) {
	log.VEventf(ctx, 1, "creating DistSQL plan with isLocal=%v", planCtx.isLocal)

	physPlan, err := dsp.createPlanForNode(planCtx, plan)
	if err != nil {
		recv.SetError(err)
		return func() {}
	}
	dsp.FinalizePlan(planCtx, &physPlan)
	return dsp.Run(planCtx, txn, &physPlan, recv, evalCtx, nil /* finishedSetupFn */)
}

// PlanAndRunPostqueries returns false if an error was encountered and sets
// that error in the provided receiver.
func (dsp *DistSQLPlanner) PlanAndRunPostqueries(
	ctx context.Context,
	planner *planner,
	evalCtxFactory func() *extendedEvalContext,
	postqueryPlans []postquery,
	recv *DistSQLReceiver,
	maybeDistribute bool,
) bool {
	for _, postqueryPlan := range postqueryPlans {
		if err := dsp.planAndRunPostquery(
			ctx,
			postqueryPlan,
			planner,
			evalCtxFactory(),
			recv,
			maybeDistribute,
		); err != nil {
			recv.SetError(err)
			return false
		}
	}

	return true
}

func (dsp *DistSQLPlanner) planAndRunPostquery(
	ctx context.Context,
	postqueryPlan postquery,
	planner *planner,
	evalCtx *extendedEvalContext,
	recv *DistSQLReceiver,
	maybeDistribute bool,
) error {
	postqueryMonitor := mon.MakeMonitor(
		"postquery",
		mon.MemoryResource,
		dsp.distSQLSrv.Metrics.CurBytesCount,
		dsp.distSQLSrv.Metrics.MaxBytesHist,
		-1, /* use default block size */
		noteworthyMemoryUsageBytes,
		dsp.distSQLSrv.Settings,
	)
	postqueryMonitor.Start(ctx, evalCtx.Mon, mon.BoundAccount{})
	defer postqueryMonitor.Stop(ctx)

	postqueryMemAccount := postqueryMonitor.MakeBoundAccount()
	defer postqueryMemAccount.Close(ctx)

	var postqueryPlanCtx *PlanningCtx
	var distributePostquery bool
	if maybeDistribute {
		distributePostquery = shouldDistributePlan(
			ctx, planner.SessionData().DistSQLMode, dsp, postqueryPlan.plan)
	}
	if distributePostquery {
		postqueryPlanCtx = dsp.NewPlanningCtx(ctx, evalCtx, planner.txn)
	} else {
		postqueryPlanCtx = dsp.newLocalPlanningCtx(ctx, evalCtx)
	}

	postqueryPlanCtx.isLocal = !distributePostquery
	postqueryPlanCtx.planner = planner
	postqueryPlanCtx.stmtType = tree.Rows
	postqueryPlanCtx.ignoreClose = true

	postqueryPhysPlan, err := dsp.createPlanForNode(postqueryPlanCtx, postqueryPlan.plan)
	if err != nil {
		return err
	}
	dsp.FinalizePlan(postqueryPlanCtx, &postqueryPhysPlan)

	postqueryRecv := recv.clone()
	var postqueryRowReceiver postqueryRowResultWriter
	postqueryRecv.resultWriter = &postqueryRowReceiver
	dsp.Run(postqueryPlanCtx, planner.txn, &postqueryPhysPlan, postqueryRecv, evalCtx, nil /* finishedSetupFn */)()
	if postqueryRecv.commErr != nil {
		return postqueryRecv.commErr
	}
	return postqueryRowReceiver.Err()
}

// postqueryRowResultWriter is a lightweight version of RowResultWriter that
// can only write errors. It is used only for executing postqueries and is
// sufficient for that case since those can only return errors.
type postqueryRowResultWriter struct {
	err error
}

var _ rowResultWriter = &postqueryRowResultWriter{}

func (r *postqueryRowResultWriter) AddRow(ctx context.Context, row tree.Datums) error {
	return errors.Errorf("unexpectedly AddRow is called on postqueryRowResultWriter")
}

func (r *postqueryRowResultWriter) IncrementRowsAffected(n int) {
	// TODO(yuzefovich): this probably will need to change when we support
	// cascades.
}

func (r *postqueryRowResultWriter) SetError(err error) {
	r.err = err
}

func (r *postqueryRowResultWriter) Err() error {
	return r.err
}
