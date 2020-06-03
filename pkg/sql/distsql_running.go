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
	"math"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/colflow"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	opentracing "github.com/opentracing/opentracing-go"
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
	flowReq    *execinfrapb.SetupFlowRequest
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

	conn, err := req.nodeDialer.Dial(req.ctx, req.nodeID, rpc.DefaultClass)
	if err != nil {
		res.err = err
	} else {
		client := execinfrapb.NewDistSQLClient(conn)
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
// be run. It also returns a boolean indicating whether the flow is vectorized.
func (dsp *DistSQLPlanner) setupFlows(
	ctx context.Context,
	evalCtx *extendedEvalContext,
	leafInputState *roachpb.LeafTxnInputState,
	flows map[roachpb.NodeID]*execinfrapb.FlowSpec,
	recv *DistSQLReceiver,
	localState distsql.LocalState,
	vectorizeThresholdMet bool,
) (context.Context, flowinfra.Flow, error) {
	thisNodeID := dsp.gatewayNodeID
	_, ok := flows[thisNodeID]
	if !ok {
		return nil, nil, errors.AssertionFailedf("missing gateway flow")
	}
	if localState.IsLocal && len(flows) != 1 {
		return nil, nil, errors.AssertionFailedf("IsLocal set but there's multiple flows")
	}

	evalCtxProto := execinfrapb.MakeEvalContext(&evalCtx.EvalContext)
	setupReq := execinfrapb.SetupFlowRequest{
		LeafTxnInputState: leafInputState,
		Version:           execinfra.Version,
		EvalContext:       evalCtxProto,
		TraceKV:           evalCtx.Tracing.KVTracingEnabled(),
	}

	// Start all the flows except the flow on this node (there is always a flow on
	// this node).
	var resultChan chan runnerResult
	if len(flows) > 1 {
		resultChan = make(chan runnerResult, len(flows)-1)
	}

	if evalCtx.SessionData.VectorizeMode != sessiondata.VectorizeOff {
		if !vectorizeThresholdMet && (evalCtx.SessionData.VectorizeMode == sessiondata.Vectorize201Auto || evalCtx.SessionData.VectorizeMode == sessiondata.VectorizeOn) {
			// Vectorization is not justified for this flow because the expected
			// amount of data is too small and the overhead of pre-allocating data
			// structures needed for the vectorized engine is expected to dominate
			// the execution time.
			setupReq.EvalContext.Vectorize = int32(sessiondata.VectorizeOff)
		} else {
			// Now we check to see whether or not to even try vectorizing the flow.
			// The goal here is to determine up front whether all of the flows can be
			// vectorized. If any of them can't, turn off the setting.
			// TODO(yuzefovich): this is a safe but quite inefficient way of setting
			// up vectorized flows since the flows will effectively be planned twice.
			for scheduledOnNodeID, spec := range flows {
				scheduledOnRemoteNode := scheduledOnNodeID != thisNodeID
				if _, err := colflow.SupportsVectorized(
					ctx, &execinfra.FlowCtx{
						EvalCtx: &evalCtx.EvalContext,
						Cfg: &execinfra.ServerConfig{
							DiskMonitor:    &mon.BytesMonitor{},
							Settings:       dsp.st,
							ClusterID:      &dsp.rpcCtx.ClusterID,
							VecFDSemaphore: dsp.distSQLSrv.VecFDSemaphore,
						},
						NodeID: evalCtx.NodeID,
					}, spec.Processors, localState.IsLocal, recv, scheduledOnRemoteNode,
				); err != nil {
					// Vectorization attempt failed with an error.
					returnVectorizationSetupError := false
					if evalCtx.SessionData.VectorizeMode == sessiondata.VectorizeExperimentalAlways {
						returnVectorizationSetupError = true
						// If running with VectorizeExperimentalAlways, this check makes sure
						// that we can still run SET statements (mostly to set vectorize to
						// off) and the like.
						if len(spec.Processors) == 1 &&
							spec.Processors[0].Core.LocalPlanNode != nil {
							rsidx := spec.Processors[0].Core.LocalPlanNode.RowSourceIdx
							if rsidx != nil {
								lp := localState.LocalProcs[*rsidx]
								if z, ok := lp.(colflow.VectorizeAlwaysException); ok {
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
					// setting.
					setupReq.EvalContext.Vectorize = int32(sessiondata.VectorizeOff)
					break
				}
			}
		}
	}
	for nodeID, flowSpec := range flows {
		if nodeID == thisNodeID {
			// Skip this node.
			continue
		}
		if !evalCtx.Codec.ForSystemTenant() {
			// A tenant server should never find itself distributing flows.
			// NB: we wouldn't hit this in practice but if we did the actual
			// error would be opaque.
			return nil, nil, errorutil.UnsupportedWithMultiTenancy(47900)
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
		defer physicalplan.ReleaseSetupFlowRequest(&req)

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
	defer physicalplan.ReleaseSetupFlowRequest(&localReq)
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
	txn *kv.Txn,
	plan *PhysicalPlan,
	recv *DistSQLReceiver,
	evalCtx *extendedEvalContext,
	finishedSetupFn func(),
) (cleanup func()) {
	ctx := planCtx.ctx

	var (
		localState     distsql.LocalState
		leafInputState *roachpb.LeafTxnInputState
	)
	// NB: putting part of evalCtx in localState means it might be mutated down
	// the line.
	localState.EvalContext = &evalCtx.EvalContext
	localState.Txn = txn
	if planCtx.isLocal {
		localState.IsLocal = true
		localState.LocalProcs = plan.LocalProcessors
	} else if txn != nil {
		// If the plan is not local, we will have to set up leaf txns using the
		// txnCoordMeta.
		tis, err := txn.GetLeafTxnInputStateOrRejectClient(ctx)
		if err != nil {
			log.Infof(ctx, "%s: %s", clientRejectedMsg, err)
			recv.SetError(err)
			return func() {}
		}
		leafInputState = &tis
	}

	flows := plan.GenerateFlowSpecs()
	if _, ok := flows[dsp.gatewayNodeID]; !ok {
		recv.SetError(errors.Errorf("expected to find gateway flow"))
		return func() {}
	}

	if planCtx.saveDiagram != nil {
		// Local flows might not have the UUID field set. We need it to be set to
		// distinguish statistics for processors in subqueries vs the main query vs
		// postqueries.
		if len(flows) == 1 {
			for _, f := range flows {
				if f.FlowID == (execinfrapb.FlowID{}) {
					f.FlowID.UUID = uuid.MakeV4()
				}
			}
		}
		log.VEvent(ctx, 1, "creating plan diagram")
		var stmtStr string
		if planCtx.planner != nil && planCtx.planner.stmt != nil {
			stmtStr = planCtx.planner.stmt.String()
		}
		diagram, err := execinfrapb.GeneratePlanDiagram(
			stmtStr, flows, planCtx.saveDiagramShowInputTypes,
		)
		if err != nil {
			recv.SetError(err)
			return func() {}
		}
		planCtx.saveDiagram(diagram)
	}

	if logPlanDiagram {
		log.VEvent(ctx, 1, "creating plan diagram for logging")
		var stmtStr string
		if planCtx.planner != nil && planCtx.planner.stmt != nil {
			stmtStr = planCtx.planner.stmt.String()
		}
		_, url, err := execinfrapb.GeneratePlanDiagramURL(stmtStr, flows, false /* showInputTypes */)
		if err != nil {
			log.Infof(ctx, "Error generating diagram: %s", err)
		} else {
			log.Infof(ctx, "Plan diagram URL:\n%s", url.String())
		}
	}

	log.VEvent(ctx, 1, "running DistSQL plan")

	dsp.distSQLSrv.ServerConfig.Metrics.QueryStart()
	defer dsp.distSQLSrv.ServerConfig.Metrics.QueryStop()

	recv.outputTypes = plan.ResultTypes
	recv.resultToStreamColMap = plan.PlanToStreamColMap

	vectorizedThresholdMet := plan.MaxEstimatedRowCount >= evalCtx.SessionData.VectorizeRowCountThreshold

	if len(flows) == 1 {
		// We ended up planning everything locally, regardless of whether we
		// intended to distribute or not.
		localState.IsLocal = true
	}

	ctx, flow, err := dsp.setupFlows(ctx, evalCtx, leafInputState, flows, recv, localState, vectorizedThresholdMet)
	if err != nil {
		recv.SetError(err)
		return func() {}
	}

	if finishedSetupFn != nil {
		finishedSetupFn()
	}

	if planCtx.planner != nil && flow.IsVectorized() {
		planCtx.planner.curPlan.flags.Set(planFlagVectorized)
	}

	// Check that flows that were forced to be planned locally also have no concurrency.
	// This is important, since these flows are forced to use the RootTxn (since
	// they might have mutations), and the RootTxn does not permit concurrency.
	// For such flows, we were supposed to have fused everything.
	if txn != nil && planCtx.isLocal && flow.ConcurrentExecution() {
		recv.SetError(errors.AssertionFailedf(
			"unexpected concurrency for a flow that was forced to be planned locally"))
		return func() {}
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
// DistSQLReceiver also update the RangeDescriptorCache in response to DistSQL
// metadata about misplanned ranges.
type DistSQLReceiver struct {
	ctx context.Context

	// resultWriter is the interface which we send results to.
	resultWriter rowResultWriter

	stmtType tree.StatementType

	// outputTypes are the types of the result columns produced by the plan.
	outputTypes []*types.T

	// resultToStreamColMap maps result columns to columns in the rowexec results
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

	row    tree.Datums
	status execinfra.ConsumerStatus
	alloc  sqlbase.DatumAlloc
	closed bool

	rangeCache *kvcoord.RangeDescriptorCache
	tracing    *SessionTracing
	cleanup    func()

	// The transaction in which the flow producing data for this
	// receiver runs. The DistSQLReceiver updates the transaction in
	// response to RetryableTxnError's and when distributed processors
	// pass back LeafTxnFinalState objects via ProducerMetas. Nil if no
	// transaction should be updated on errors (i.e. if the flow overall
	// doesn't run in a transaction).
	txn *kv.Txn

	// A handler for clock signals arriving from remote nodes. This should update
	// this node's clock.
	updateClock func(observedTs hlc.Timestamp)

	// bytesRead and rowsRead track the corresponding metrics while executing the
	// statement.
	bytesRead int64
	rowsRead  int64

	expectedRowsRead int64
	progressAtomic   *uint64
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

// MetadataResultWriter is used to stream metadata rather than row results in a
// DistSQL flow.
type MetadataResultWriter interface {
	AddMeta(ctx context.Context, meta *execinfrapb.ProducerMetadata)
}

// MetadataCallbackWriter wraps a rowResultWriter to stream metadata in a
// DistSQL flow. It executes a given callback when metadata is added.
type MetadataCallbackWriter struct {
	rowResultWriter
	fn func(ctx context.Context, meta *execinfrapb.ProducerMetadata) error
}

// AddMeta implements the MetadataResultWriter interface.
func (w *MetadataCallbackWriter) AddMeta(ctx context.Context, meta *execinfrapb.ProducerMetadata) {
	if err := w.fn(ctx, meta); err != nil {
		w.SetError(err)
	}
}

// NewMetadataCallbackWriter creates a new MetadataCallbackWriter.
func NewMetadataCallbackWriter(
	rowResultWriter rowResultWriter,
	metaFn func(ctx context.Context, meta *execinfrapb.ProducerMetadata) error,
) *MetadataCallbackWriter {
	return &MetadataCallbackWriter{rowResultWriter: rowResultWriter, fn: metaFn}
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

var _ execinfra.RowReceiver = &DistSQLReceiver{}

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
	rangeCache *kvcoord.RangeDescriptorCache,
	txn *kv.Txn,
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
		txn:          txn,
		updateClock:  updateClock,
		stmtType:     stmtType,
		tracing:      tracing,
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
	row sqlbase.EncDatumRow, meta *execinfrapb.ProducerMetadata,
) execinfra.ConsumerStatus {
	if meta != nil {
		if meta.LeafTxnFinalState != nil {
			if r.txn != nil {
				if r.txn.ID() == meta.LeafTxnFinalState.Txn.ID {
					if err := r.txn.UpdateRootWithLeafFinalState(r.ctx, meta.LeafTxnFinalState); err != nil {
						r.resultWriter.SetError(err)
					}
				}
			} else {
				r.resultWriter.SetError(
					errors.Errorf("received a leaf final state (%s); but have no root", meta.LeafTxnFinalState))
			}
		}
		if meta.Err != nil {
			// Check if the error we just received should take precedence over a
			// previous error (if any).
			if roachpb.ErrPriority(meta.Err) > roachpb.ErrPriority(r.resultWriter.Err()) {
				if r.txn != nil {
					if retryErr := (*roachpb.UnhandledRetryableError)(nil); errors.As(meta.Err, &retryErr) {
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
			r.rangeCache.Insert(r.ctx, meta.Ranges...)
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
			if r.progressAtomic != nil && r.expectedRowsRead != 0 {
				progress := float64(r.rowsRead) / float64(r.expectedRowsRead)
				atomic.StoreUint64(r.progressAtomic, math.Float64bits(progress))
			}
			meta.Metrics.Release()
			meta.Release()
		}
		if metaWriter, ok := r.resultWriter.(MetadataResultWriter); ok {
			metaWriter.AddMeta(r.ctx, meta)
		}
		return r.status
	}
	if r.resultWriter.Err() == nil && r.ctx.Err() != nil {
		r.resultWriter.SetError(r.ctx.Err())
	}
	if r.resultWriter.Err() != nil {
		// TODO(andrei): We should drain here if we weren't canceled.
		return execinfra.ConsumerClosed
	}
	if r.status != execinfra.NeedMoreRows {
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
		r.status = execinfra.ConsumerClosed
	} else {
		if r.row == nil {
			r.row = make(tree.Datums, len(r.resultToStreamColMap))
		}
		for i, resIdx := range r.resultToStreamColMap {
			err := row[resIdx].EnsureDecoded(r.outputTypes[resIdx], &r.alloc)
			if err != nil {
				r.resultWriter.SetError(err)
				r.status = execinfra.ConsumerClosed
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
		r.status = execinfra.ConsumerClosed
	}
	return r.status
}

var (
	// ErrLimitedResultNotSupported is an error produced by pgwire
	// indicating an unsupported feature of row count limits was attempted.
	ErrLimitedResultNotSupported = unimplemented.NewWithIssue(40195, "multiple active portals not supported")
	// ErrLimitedResultClosed is a sentinel error produced by pgwire
	// indicating the portal should be closed without error.
	ErrLimitedResultClosed = errors.New("row count limit closed")
)

// ProducerDone is part of the RowReceiver interface.
func (r *DistSQLReceiver) ProducerDone() {
	if r.closed {
		panic("double close")
	}
	r.closed = true
	r.cleanup()
}

// Types is part of the RowReceiver interface.
func (r *DistSQLReceiver) Types() []*types.T {
	return r.outputTypes
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

	var distributeSubquery bool
	if maybeDistribute {
		distributeSubquery = getPlanDistribution(
			ctx, planner.execCfg.NodeID, planner.SessionData().DistSQLMode, subqueryPlan.plan,
		).WillDistribute()
	}
	subqueryPlanCtx := dsp.NewPlanningCtx(ctx, evalCtx, planner.txn, distributeSubquery)
	subqueryPlanCtx.planner = planner
	subqueryPlanCtx.stmtType = tree.Rows
	if planner.collectBundle {
		subqueryPlanCtx.saveDiagram = func(diagram execinfrapb.FlowDiagram) {
			planner.curPlan.distSQLDiagrams = append(planner.curPlan.distSQLDiagrams, diagram)
		}
	}
	// Don't close the top-level plan from subqueries - someone else will handle
	// that.
	subqueryPlanCtx.ignoreClose = true
	subqueryPhysPlan, err := dsp.createPhysPlan(subqueryPlanCtx, subqueryPlan.plan)
	if err != nil {
		return err
	}
	dsp.FinalizePlan(subqueryPlanCtx, subqueryPhysPlan)

	// TODO(arjun): #28264: We set up a row container, wrap it in a row
	// receiver, and use it and serialize the results of the subquery. The type
	// of the results stored in the container depends on the type of the subquery.
	subqueryRecv := recv.clone()
	var typ sqlbase.ColTypeInfo
	var rows *rowcontainer.RowContainer
	if subqueryPlan.execMode == rowexec.SubqueryExecModeExists {
		subqueryRecv.noColsRequired = true
		typ = sqlbase.ColTypeInfoFromColTypes([]*types.T{})
	} else {
		// Apply the PlanToStreamColMap projection to the ResultTypes to get the
		// final set of output types for the subquery. The reason this is necessary
		// is that the output schema of a query sometimes contains columns necessary
		// to merge the streams, but that aren't required by the final output of the
		// query. These get projected out, so we need to similarly adjust the
		// expected result types of the subquery here.
		colTypes := make([]*types.T, len(subqueryPhysPlan.PlanToStreamColMap))
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
	dsp.Run(subqueryPlanCtx, planner.txn, subqueryPhysPlan, subqueryRecv, evalCtx, nil /* finishedSetupFn */)()
	if subqueryRecv.commErr != nil {
		return subqueryRecv.commErr
	}
	if err := subqueryRowReceiver.Err(); err != nil {
		return err
	}
	switch subqueryPlan.execMode {
	case rowexec.SubqueryExecModeExists:
		// For EXISTS expressions, all we want to know if there is at least one row.
		hasRows := rows.Len() != 0
		subqueryPlans[planIdx].result = tree.MakeDBool(tree.DBool(hasRows))
	case rowexec.SubqueryExecModeAllRows, rowexec.SubqueryExecModeAllRowsNormalized:
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

		if subqueryPlan.execMode == rowexec.SubqueryExecModeAllRowsNormalized {
			result.Normalize(&evalCtx.EvalContext)
		}
		subqueryPlans[planIdx].result = &result
	case rowexec.SubqueryExecModeOneRow:
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
// encountered) cleanup function that must be called once the planTop AST is no
// longer needed and can be closed. Note that this function also cleans up the
// flow which is unfortunate but is caused by the sharing of memory monitors
// between planning and execution - cleaning up the flow wants to close the
// monitor, but it cannot do so because the AST needs to live longer and still
// uses the same monitor. That's why we end up in a situation that in order to
// clean up the flow, we need to close the AST first, but we can only do that
// after PlanAndRun returns.
func (dsp *DistSQLPlanner) PlanAndRun(
	ctx context.Context,
	evalCtx *extendedEvalContext,
	planCtx *PlanningCtx,
	txn *kv.Txn,
	plan planMaybePhysical,
	recv *DistSQLReceiver,
) (cleanup func()) {
	log.VEventf(ctx, 1, "creating DistSQL plan with isLocal=%v", planCtx.isLocal)

	physPlan, err := dsp.createPhysPlan(planCtx, plan)
	if err != nil {
		recv.SetError(err)
		return func() {}
	}
	dsp.FinalizePlan(planCtx, physPlan)
	recv.expectedRowsRead = int64(physPlan.TotalEstimatedScannedRows)
	return dsp.Run(planCtx, txn, physPlan, recv, evalCtx, nil /* finishedSetupFn */)
}

// PlanAndRunCascadesAndChecks runs any cascade and check queries.
//
// Because cascades can themselves generate more cascades or check queries, this
// method can append to plan.cascades and plan.checkPlans (and all these plans
// must be closed later).
//
// Returns false if an error was encountered and sets that error in the provided
// receiver.
func (dsp *DistSQLPlanner) PlanAndRunCascadesAndChecks(
	ctx context.Context,
	planner *planner,
	evalCtxFactory func() *extendedEvalContext,
	plan *planComponents,
	recv *DistSQLReceiver,
	maybeDistribute bool,
) bool {
	if len(plan.cascades) == 0 && len(plan.checkPlans) == 0 {
		return false
	}

	prevSteppingMode := planner.Txn().ConfigureStepping(ctx, kv.SteppingEnabled)
	defer func() { _ = planner.Txn().ConfigureStepping(ctx, prevSteppingMode) }()

	// We treat plan.cascades as a queue.
	for i := 0; i < len(plan.cascades); i++ {
		// The original bufferNode is stored in c.Buffer; we can refer to it
		// directly.
		// TODO(radu): this requires keeping all previous plans "alive" until the
		// very end. We may want to make copies of the buffer nodes and clean up
		// everything else.
		buf := plan.cascades[i].Buffer.(*bufferNode)
		if buf.bufferedRows.Len() == 0 {
			// No rows were actually modified.
			continue
		}

		log.VEventf(ctx, 1, "executing cascade for constraint %s", plan.cascades[i].FKName)

		// We place a sequence point before every cascade, so
		// that each subsequent cascade can observe the writes
		// by the previous step.
		// TODO(radu): the cascades themselves can have more cascades; if any of
		// those fall back to legacy cascades code, it will disable stepping. So we
		// have to reenable stepping each time.
		_ = planner.Txn().ConfigureStepping(ctx, kv.SteppingEnabled)
		if err := planner.Txn().Step(ctx); err != nil {
			recv.SetError(err)
			return false
		}

		evalCtx := evalCtxFactory()
		execFactory := newExecFactory(planner)
		// The cascading query is allowed to autocommit only if it is the last
		// cascade and there are no check queries to run.
		if len(plan.checkPlans) > 0 || i < len(plan.cascades)-1 {
			execFactory.disableAutoCommit()
		}
		cascadePlan, err := plan.cascades[i].PlanFn(
			ctx, &planner.semaCtx, &evalCtx.EvalContext, execFactory, buf, buf.bufferedRows.Len(),
		)
		if err != nil {
			recv.SetError(err)
			return false
		}
		cp := cascadePlan.(*planTop)
		plan.cascades[i].plan = cp.main
		if len(cp.subqueryPlans) > 0 {
			recv.SetError(errors.AssertionFailedf("cascades should not have subqueries"))
			return false
		}

		// Queue any new cascades.
		if len(cp.cascades) > 0 {
			plan.cascades = append(plan.cascades, cp.cascades...)
		}

		// Collect any new checks.
		if len(cp.checkPlans) > 0 {
			plan.checkPlans = append(plan.checkPlans, cp.checkPlans...)
		}

		// In cyclical reference situations, the number of cascading operations can
		// be arbitrarily large. To avoid OOM, we enforce a limit. This is also a
		// safeguard in case we have a bug that results in an infinite cascade loop.
		if limit := evalCtx.SessionData.OptimizerFKCascadesLimit; len(plan.cascades) > limit {
			telemetry.Inc(sqltelemetry.CascadesLimitReached)
			err := pgerror.Newf(pgcode.TriggeredActionException, "cascades limit (%d) reached", limit)
			recv.SetError(err)
			return false
		}

		if err := dsp.planAndRunPostquery(
			ctx,
			cp.main,
			planner,
			evalCtx,
			recv,
			maybeDistribute,
		); err != nil {
			recv.SetError(err)
			return false
		}
	}

	if len(plan.checkPlans) == 0 {
		return true
	}

	// We place a sequence point before the checks, so that they observe the
	// writes of the main query and/or any cascades.
	// TODO(radu): the cascades themselves can have more cascades; if any of
	// those fall back to legacy cascades code, it will disable stepping. So we
	// have to reenable stepping each time.
	_ = planner.Txn().ConfigureStepping(ctx, kv.SteppingEnabled)
	if err := planner.Txn().Step(ctx); err != nil {
		recv.SetError(err)
		return false
	}

	for i := range plan.checkPlans {
		log.VEventf(ctx, 1, "executing check query %d out of %d", i+1, len(plan.checkPlans))
		if err := dsp.planAndRunPostquery(
			ctx,
			plan.checkPlans[i].plan,
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

// planAndRunPostquery runs a cascade or check query.
func (dsp *DistSQLPlanner) planAndRunPostquery(
	ctx context.Context,
	postqueryPlan planMaybePhysical,
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

	var distributePostquery bool
	if maybeDistribute {
		distributePostquery = getPlanDistribution(
			ctx, planner.execCfg.NodeID, planner.SessionData().DistSQLMode, postqueryPlan,
		).WillDistribute()
	}
	postqueryPlanCtx := dsp.NewPlanningCtx(ctx, evalCtx, planner.txn, distributePostquery)
	postqueryPlanCtx.planner = planner
	postqueryPlanCtx.stmtType = tree.Rows
	postqueryPlanCtx.ignoreClose = true
	if planner.collectBundle {
		postqueryPlanCtx.saveDiagram = func(diagram execinfrapb.FlowDiagram) {
			planner.curPlan.distSQLDiagrams = append(planner.curPlan.distSQLDiagrams, diagram)
		}
	}

	postqueryPhysPlan, err := dsp.createPhysPlan(postqueryPlanCtx, postqueryPlan)
	if err != nil {
		return err
	}
	dsp.FinalizePlan(postqueryPlanCtx, postqueryPhysPlan)

	postqueryRecv := recv.clone()
	// TODO(yuzefovich): at the moment, errOnlyResultWriter is sufficient here,
	// but it may not be the case when we support cascades through the optimizer.
	postqueryRecv.resultWriter = &errOnlyResultWriter{}
	dsp.Run(postqueryPlanCtx, planner.txn, postqueryPhysPlan, postqueryRecv, evalCtx, nil /* finishedSetupFn */)()
	if postqueryRecv.commErr != nil {
		return postqueryRecv.commErr
	}
	return postqueryRecv.resultWriter.Err()
}
