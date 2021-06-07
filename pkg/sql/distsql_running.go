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
	"time"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangecache"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/colflow"
	"github.com/cockroachdb/cockroach/pkg/sql/contention"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/ring"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
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

func (dsp *DistSQLPlanner) initRunners(ctx context.Context) {
	// This channel has to be unbuffered because we want to only be able to send
	// requests if a worker is actually there to receive them.
	dsp.runnerChan = make(chan runnerRequest)
	for i := 0; i < numRunners; i++ {
		_ = dsp.stopper.RunAsyncTask(ctx, "distsql-runner", func(context.Context) {
			runnerChan := dsp.runnerChan
			stopChan := dsp.stopper.ShouldQuiesce()
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

// To allow for canceling flows via CancelDeadFlows RPC on different nodes
// simultaneously, we use a pool of workers. It is likely that these workers
// will be less busy than SetupFlow runners, so we instantiate smaller number of
// the canceling workers.
const numCancelingWorkers = numRunners / 4

func (dsp *DistSQLPlanner) initCancelingWorkers(initCtx context.Context) {
	dsp.cancelFlowsCoordinator.workerWait = make(chan struct{}, numCancelingWorkers)
	const cancelRequestTimeout = 10 * time.Second
	for i := 0; i < numCancelingWorkers; i++ {
		workerID := i + 1
		_ = dsp.stopper.RunAsyncTask(initCtx, "distsql-canceling-worker", func(parentCtx context.Context) {
			stopChan := dsp.stopper.ShouldQuiesce()
			for {
				select {
				case <-stopChan:
					return

				case <-dsp.cancelFlowsCoordinator.workerWait:
					req, nodeID := dsp.cancelFlowsCoordinator.getFlowsToCancel()
					if req == nil {
						// There are no flows to cancel at the moment. This
						// shouldn't really happen.
						log.VEventf(parentCtx, 2, "worker %d woke up but didn't find any flows to cancel", workerID)
						continue
					}
					log.VEventf(parentCtx, 2, "worker %d is canceling at most %d flows on node %d", workerID, len(req.FlowIDs), nodeID)
					conn, err := dsp.nodeDialer.Dial(parentCtx, nodeID, rpc.DefaultClass)
					if err != nil {
						// We failed to dial the node, so we give up given that
						// our cancellation is best effort. It is possible that
						// the node is dead anyway.
						continue
					}
					client := execinfrapb.NewDistSQLClient(conn)
					_ = contextutil.RunWithTimeout(
						parentCtx,
						"cancel dead flows",
						cancelRequestTimeout,
						func(ctx context.Context) error {
							_, _ = client.CancelDeadFlows(ctx, req)
							return nil
						})
				}
			}
		})
	}
}

type deadFlowsOnNode struct {
	ids    []execinfrapb.FlowID
	nodeID roachpb.NodeID
}

// cancelFlowsCoordinator is responsible for batching up the requests to cancel
// remote flows initiated on the behalf of the current node when the local flows
// errored out.
type cancelFlowsCoordinator struct {
	mu struct {
		syncutil.Mutex
		// deadFlowsByNode is a ring of pointers to deadFlowsOnNode objects.
		deadFlowsByNode ring.Buffer
	}
	// workerWait should be used by canceling workers to block until there are
	// some dead flows to cancel.
	workerWait chan struct{}
}

// getFlowsToCancel returns a request to cancel some dead flows on a particular
// node. If there are no dead flows to cancel, it returns nil, 0. Safe for
// concurrent usage.
func (c *cancelFlowsCoordinator) getFlowsToCancel() (
	*execinfrapb.CancelDeadFlowsRequest,
	roachpb.NodeID,
) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mu.deadFlowsByNode.Len() == 0 {
		return nil, roachpb.NodeID(0)
	}
	deadFlows := c.mu.deadFlowsByNode.GetFirst().(*deadFlowsOnNode)
	c.mu.deadFlowsByNode.RemoveFirst()
	req := &execinfrapb.CancelDeadFlowsRequest{
		FlowIDs: deadFlows.ids,
	}
	return req, deadFlows.nodeID
}

// addFlowsToCancel adds all remote flows from flows map to be canceled via
// CancelDeadFlows RPC. Safe for concurrent usage.
func (c *cancelFlowsCoordinator) addFlowsToCancel(flows map[roachpb.NodeID]*execinfrapb.FlowSpec) {
	c.mu.Lock()
	for nodeID, f := range flows {
		if nodeID != f.Gateway {
			// c.mu.deadFlowsByNode.Len() is at most the number of nodes in the
			// cluster, so a linear search for the node ID should be
			// sufficiently fast.
			found := false
			for j := 0; j < c.mu.deadFlowsByNode.Len(); j++ {
				deadFlows := c.mu.deadFlowsByNode.Get(j).(*deadFlowsOnNode)
				if nodeID == deadFlows.nodeID {
					deadFlows.ids = append(deadFlows.ids, f.FlowID)
					found = true
					break
				}
			}
			if !found {
				c.mu.deadFlowsByNode.AddLast(&deadFlowsOnNode{
					ids:    []execinfrapb.FlowID{f.FlowID},
					nodeID: nodeID,
				})
			}
		}
	}
	queueLength := c.mu.deadFlowsByNode.Len()
	c.mu.Unlock()

	// Notify the canceling workers that there are some flows to cancel (we send
	// on the channel at most the length of the queue number of times in order
	// to not wake up the workers uselessly). Note that we do it in a
	// non-blocking fashion (because the workers might be busy canceling other
	// flows at the moment). Also because the channel is buffered, they won't go
	// to sleep once they are done.
	numWorkersToWakeUp := numCancelingWorkers
	if numWorkersToWakeUp > queueLength {
		numWorkersToWakeUp = queueLength
	}
	for i := 0; i < numWorkersToWakeUp; i++ {
		select {
		case c.workerWait <- struct{}{}:
		default:
			// We have filled the buffer of the channel, so there is no need to
			// try to send any more notifications.
			return
		}
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
	leafInputState *roachpb.LeafTxnInputState,
	flows map[roachpb.NodeID]*execinfrapb.FlowSpec,
	recv *DistSQLReceiver,
	localState distsql.LocalState,
	collectStats bool,
) (context.Context, flowinfra.Flow, execinfra.OpChains, error) {
	thisNodeID := dsp.gatewayNodeID
	_, ok := flows[thisNodeID]
	if !ok {
		return nil, nil, nil, errors.AssertionFailedf("missing gateway flow")
	}
	if localState.IsLocal && len(flows) != 1 {
		return nil, nil, nil, errors.AssertionFailedf("IsLocal set but there's multiple flows")
	}

	evalCtxProto := execinfrapb.MakeEvalContext(&evalCtx.EvalContext)
	setupReq := execinfrapb.SetupFlowRequest{
		LeafTxnInputState: leafInputState,
		Version:           execinfra.Version,
		EvalContext:       evalCtxProto,
		TraceKV:           evalCtx.Tracing.KVTracingEnabled(),
		CollectStats:      collectStats,
	}

	// Start all the flows except the flow on this node (there is always a flow on
	// this node).
	var resultChan chan runnerResult
	if len(flows) > 1 {
		resultChan = make(chan runnerResult, len(flows)-1)
	}

	if vectorizeMode := evalCtx.SessionData.VectorizeMode; vectorizeMode != sessiondatapb.VectorizeOff {
		// Now we determine whether the vectorized engine supports the flow
		// specs.
		for _, spec := range flows {
			if err := colflow.IsSupported(vectorizeMode, spec); err != nil {
				log.VEventf(ctx, 2, "failed to vectorize: %s", err)
				if vectorizeMode == sessiondatapb.VectorizeExperimentalAlways {
					return nil, nil, nil, err
				}
				// Vectorization is not supported for this flow, so we override the
				// setting.
				setupReq.EvalContext.SessionData.VectorizeMode = sessiondatapb.VectorizeOff
				break
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
			return nil, nil, nil, errorutil.UnsupportedWithMultiTenancy(47900)
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
		return nil, nil, nil, firstErr
	}

	// Set up the flow on this node.
	localReq := setupReq
	localReq.Flow = *flows[thisNodeID]
	var batchReceiver execinfra.BatchReceiver
	if recv.batchWriter != nil {
		// Use the DistSQLReceiver as an execinfra.BatchReceiver only if the
		// former has the corresponding writer set.
		batchReceiver = recv
	}
	return dsp.distSQLSrv.SetupLocalSyncFlow(ctx, evalCtx.Mon, &localReq, recv, batchReceiver, localState)
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
	localState.LocalProcs = plan.LocalProcessors
	// If we have access to a planner and are currently being used to plan
	// statements in a user transaction, then take the descs.Collection to resolve
	// types with during flow execution. This is necessary to do in the case of
	// a transaction that has already created or updated some types. If we do not
	// use the local descs.Collection, we would attempt to acquire a lease on
	// modified types when accessing them, which would error out.
	if planCtx.planner != nil && !planCtx.planner.isInternalPlanner {
		localState.Collection = planCtx.planner.Descriptors()
	}

	if planCtx.isLocal {
		localState.IsLocal = true
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
	defer func() {
		for _, flowSpec := range flows {
			physicalplan.ReleaseFlowSpec(flowSpec)
		}
	}()
	if _, ok := flows[dsp.gatewayNodeID]; !ok {
		recv.SetError(errors.Errorf("expected to find gateway flow"))
		return func() {}
	}

	if logPlanDiagram {
		log.VEvent(ctx, 3, "creating plan diagram for logging")
		var stmtStr string
		if planCtx.planner != nil && planCtx.planner.stmt.AST != nil {
			stmtStr = planCtx.planner.stmt.String()
		}
		_, url, err := execinfrapb.GeneratePlanDiagramURL(stmtStr, flows, execinfrapb.DiagramFlags{})
		if err != nil {
			log.Infof(ctx, "error generating diagram: %s", err)
		} else {
			log.Infof(ctx, "plan diagram URL:\n%s", url.String())
		}
	}

	log.VEvent(ctx, 2, "running DistSQL plan")

	dsp.distSQLSrv.ServerConfig.Metrics.QueryStart()
	defer dsp.distSQLSrv.ServerConfig.Metrics.QueryStop()

	recv.outputTypes = plan.GetResultTypes()
	recv.contendedQueryMetric = dsp.distSQLSrv.Metrics.ContendedQueriesCount

	if len(flows) == 1 {
		// We ended up planning everything locally, regardless of whether we
		// intended to distribute or not.
		localState.IsLocal = true
	} else {
		defer func() {
			if recv.resultWriter.Err() != nil {
				// The execution of this query encountered some error, so we
				// will eagerly cancel all scheduled flows on the remote nodes
				// (if they haven't been started yet) because they are now dead.
				// TODO(yuzefovich): consider whether augmenting
				// ConnectInboundStream to keep track of the streams that
				// initiated FlowStream RPC is worth it - the flows containing
				// such streams must have been started, so there is no point in
				// trying to cancel them this way. This will allow us to reduce
				// the size of the CancelDeadFlows request and speed up the
				// lookup on the remote node whether a particular dead flow
				// should be canceled. However, this improves the unhappy case,
				// but it'll slowdown the happy case - by introducing additional
				// tracking.
				dsp.cancelFlowsCoordinator.addFlowsToCancel(flows)
			}
		}()
	}

	ctx, flow, opChains, err := dsp.setupFlows(
		ctx, evalCtx, leafInputState, flows, recv, localState, planCtx.collectExecStats,
	)
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

	if planCtx.saveFlows != nil {
		if err := planCtx.saveFlows(flows, opChains); err != nil {
			recv.SetError(err)
			return func() {}
		}
	}

	// Check that flows that were forced to be planned locally also have no concurrency.
	// This is important, since these flows are forced to use the RootTxn (since
	// they might have mutations), and the RootTxn does not permit concurrency.
	// For such flows, we were supposed to have fused everything.
	if txn != nil && planCtx.isLocal && flow.ConcurrentTxnUse() {
		recv.SetError(errors.AssertionFailedf(
			"unexpected concurrency for a flow that was forced to be planned locally"))
		return func() {}
	}

	// TODO(radu): this should go through the flow scheduler.
	flow.Run(ctx, func() {})

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

// DistSQLReceiver is an execinfra.RowReceiver and execinfra.BatchReceiver that
// writes results to a rowResultWriter and batchResultWriter, respectively. This
// is where the DistSQL execution meets the SQL Session - the result writer
// comes from a client Session.
//
// DistSQLReceiver also update the RangeDescriptorCache in response to DistSQL
// metadata about misplanned ranges.
type DistSQLReceiver struct {
	ctx context.Context

	// These two interfaces refer to the same object, but batchWriter might be
	// unset (resultWriter is always set). These are used to send the results
	// to.
	resultWriter rowResultWriter
	batchWriter  batchResultWriter

	stmtType tree.StatementReturnType

	// outputTypes are the types of the result columns produced by the plan.
	outputTypes []*types.T

	// existsMode indicates that the caller is only interested in the existence
	// of a single row. Used by subqueries in EXISTS mode.
	existsMode bool

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
	alloc  rowenc.DatumAlloc
	closed bool

	rangeCache *rangecache.RangeCache
	tracing    *SessionTracing
	// cleanup will be called when the DistSQLReceiver is Release()'d back to
	// its sync.Pool.
	cleanup func()

	// The transaction in which the flow producing data for this
	// receiver runs. The DistSQLReceiver updates the transaction in
	// response to RetryableTxnError's and when distributed processors
	// pass back LeafTxnFinalState objects via ProducerMetas. Nil if no
	// transaction should be updated on errors (i.e. if the flow overall
	// doesn't run in a transaction).
	txn *kv.Txn

	// A handler for clock signals arriving from remote nodes. This should update
	// this node's clock.
	clockUpdater clockUpdater

	stats topLevelQueryStats

	expectedRowsRead int64
	progressAtomic   *uint64

	// contendedQueryMetric is a Counter that is incremented at most once if the
	// query produces at least one contention event.
	contendedQueryMetric *metric.Counter
	// contentionRegistry is a Registry that contention events are added to.
	contentionRegistry *contention.Registry

	testingKnobs struct {
		// pushCallback, if set, will be called every time DistSQLReceiver.Push
		// is called, with the same arguments.
		pushCallback func(rowenc.EncDatumRow, *execinfrapb.ProducerMetadata)
	}
}

// rowResultWriter is a subset of CommandResult to be used with the
// DistSQLReceiver. It's implemented by RowResultWriter.
type rowResultWriter interface {
	// AddRow writes a result row.
	// Note that the caller owns the row slice and might reuse it.
	AddRow(ctx context.Context, row tree.Datums) error
	IncrementRowsAffected(ctx context.Context, n int)
	SetError(error)
	Err() error
}

// batchResultWriter is a subset of CommandResult to be used with the
// DistSQLReceiver when the consumer can operate on columnar batches directly.
type batchResultWriter interface {
	AddBatch(context.Context, coldata.Batch) error
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

// errOnlyResultWriter is a rowResultWriter and batchResultWriter that only
// supports receiving an error. All other functions that deal with producing
// results panic.
type errOnlyResultWriter struct {
	err error
}

var _ rowResultWriter = &errOnlyResultWriter{}
var _ batchResultWriter = &errOnlyResultWriter{}

func (w *errOnlyResultWriter) SetError(err error) {
	w.err = err
}
func (w *errOnlyResultWriter) Err() error {
	return w.err
}

func (w *errOnlyResultWriter) AddRow(ctx context.Context, row tree.Datums) error {
	panic("AddRow not supported by errOnlyResultWriter")
}

func (w *errOnlyResultWriter) AddBatch(ctx context.Context, batch coldata.Batch) error {
	panic("AddBatch not supported by errOnlyResultWriter")
}

func (w *errOnlyResultWriter) IncrementRowsAffected(ctx context.Context, n int) {
	panic("IncrementRowsAffected not supported by errOnlyResultWriter")
}

var _ execinfra.RowReceiver = &DistSQLReceiver{}
var _ execinfra.BatchReceiver = &DistSQLReceiver{}

var receiverSyncPool = sync.Pool{
	New: func() interface{} {
		return &DistSQLReceiver{}
	},
}

// ClockUpdater describes an object that can be updated with an observed
// timestamp. Usually wraps an hlc.Clock.
type clockUpdater interface {
	// Update updates this ClockUpdater with the observed hlc.Timestamp.
	Update(observedTS hlc.ClockTimestamp)
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
	stmtType tree.StatementReturnType,
	rangeCache *rangecache.RangeCache,
	txn *kv.Txn,
	clockUpdater clockUpdater,
	tracing *SessionTracing,
	contentionRegistry *contention.Registry,
	testingPushCallback func(rowenc.EncDatumRow, *execinfrapb.ProducerMetadata),
) *DistSQLReceiver {
	consumeCtx, cleanup := tracing.TraceExecConsume(ctx)
	r := receiverSyncPool.Get().(*DistSQLReceiver)
	// Check whether the result writer supports pushing batches into it directly
	// without having to materialize them.
	var batchWriter batchResultWriter
	if commandResult, ok := resultWriter.(RestrictedCommandResult); ok {
		if commandResult.SupportsAddBatch() {
			batchWriter = commandResult
		}
	}
	*r = DistSQLReceiver{
		ctx:                consumeCtx,
		cleanup:            cleanup,
		resultWriter:       resultWriter,
		batchWriter:        batchWriter,
		rangeCache:         rangeCache,
		txn:                txn,
		clockUpdater:       clockUpdater,
		stmtType:           stmtType,
		tracing:            tracing,
		contentionRegistry: contentionRegistry,
	}
	r.testingKnobs.pushCallback = testingPushCallback
	return r
}

// Release releases this DistSQLReceiver back to the pool.
func (r *DistSQLReceiver) Release() {
	r.cleanup()
	*r = DistSQLReceiver{}
	receiverSyncPool.Put(r)
}

// clone clones the receiver for running sub- and post-queries. Not all fields
// are cloned. The receiver should be released when no longer needed.
func (r *DistSQLReceiver) clone() *DistSQLReceiver {
	ret := receiverSyncPool.Get().(*DistSQLReceiver)
	*ret = DistSQLReceiver{
		ctx:                r.ctx,
		cleanup:            func() {},
		rangeCache:         r.rangeCache,
		txn:                r.txn,
		clockUpdater:       r.clockUpdater,
		stmtType:           tree.Rows,
		tracing:            r.tracing,
		contentionRegistry: r.contentionRegistry,
	}
	return ret
}

// SetError provides a convenient way for a client to pass in an error, thus
// pretending that a query execution error happened. The error is passed along
// to the resultWriter.
//
// The status of DistSQLReceiver is updated accordingly.
func (r *DistSQLReceiver) SetError(err error) {
	r.resultWriter.SetError(err)
	// If we encountered an error, we will transition to draining unless we were
	// canceled.
	if r.ctx.Err() != nil {
		log.VEventf(r.ctx, 1, "encountered error (transitioning to shutting down): %v", r.ctx.Err())
		r.status = execinfra.ConsumerClosed
	} else {
		log.VEventf(r.ctx, 1, "encountered error (transitioning to draining): %v", err)
		r.status = execinfra.DrainRequested
	}
}

// pushMeta takes in non-empty metadata object and pushes it to the result
// writer. Possibly updated status is returned.
func (r *DistSQLReceiver) pushMeta(meta *execinfrapb.ProducerMetadata) execinfra.ConsumerStatus {
	if metaWriter, ok := r.resultWriter.(MetadataResultWriter); ok {
		metaWriter.AddMeta(r.ctx, meta)
	}
	if meta.LeafTxnFinalState != nil {
		if r.txn != nil {
			if r.txn.ID() == meta.LeafTxnFinalState.Txn.ID {
				if err := r.txn.UpdateRootWithLeafFinalState(r.ctx, meta.LeafTxnFinalState); err != nil {
					r.SetError(err)
				}
			}
		} else {
			r.SetError(
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
					if r.clockUpdater != nil {
						r.clockUpdater.Update(retryErr.PErr.Now)
					}
				}
			}
			r.SetError(meta.Err)
		}
	}
	if len(meta.Ranges) > 0 {
		r.rangeCache.Insert(r.ctx, meta.Ranges...)
	}
	if len(meta.TraceData) > 0 {
		if span := tracing.SpanFromContext(r.ctx); span != nil {
			span.ImportRemoteSpans(meta.TraceData)
		}
		var ev roachpb.ContentionEvent
		for i := range meta.TraceData {
			meta.TraceData[i].Structured(func(any *pbtypes.Any, _ time.Time) {
				if !pbtypes.Is(any, &ev) {
					return
				}
				if err := pbtypes.UnmarshalAny(any, &ev); err != nil {
					return
				}
				if r.contendedQueryMetric != nil {
					// Increment the contended query metric at most once
					// if the query sees at least one contention event.
					r.contendedQueryMetric.Inc(1)
					r.contendedQueryMetric = nil
				}
				r.contentionRegistry.AddContentionEvent(ev)
			})
		}
	}
	if meta.Metrics != nil {
		r.stats.bytesRead += meta.Metrics.BytesRead
		r.stats.rowsRead += meta.Metrics.RowsRead
		if r.progressAtomic != nil && r.expectedRowsRead != 0 {
			progress := float64(r.stats.rowsRead) / float64(r.expectedRowsRead)
			atomic.StoreUint64(r.progressAtomic, math.Float64bits(progress))
		}
		meta.Metrics.Release()
	}
	// Release the meta object. It is unsafe for use after this call.
	meta.Release()
	return r.status
}

// handleCommErr handles the communication error (the one returned when
// attempting to add data to the result writer).
func (r *DistSQLReceiver) handleCommErr(commErr error) {
	// ErrLimitedResultClosed and errIEResultChannelClosed are not real
	// errors, it is a signal to stop distsql and return success to the
	// client (that's why we don't set the error on the resultWriter).
	if errors.Is(commErr, ErrLimitedResultClosed) {
		log.VEvent(r.ctx, 1, "encountered ErrLimitedResultClosed (transitioning to draining)")
		r.status = execinfra.DrainRequested
	} else if errors.Is(commErr, errIEResultChannelClosed) {
		log.VEvent(r.ctx, 1, "encountered errIEResultChannelClosed (transitioning to draining)")
		r.status = execinfra.DrainRequested
	} else {
		// Set the error on the resultWriter to notify the consumer about
		// it. Most clients don't care to differentiate between
		// communication errors and query execution errors, so they can
		// simply inspect resultWriter.Err().
		r.SetError(commErr)

		// The only client that needs to know that a communication error and
		// not a query execution error has occurred is
		// connExecutor.execWithDistSQLEngine which will inspect r.commErr
		// on its own and will shut down the connection.
		//
		// We don't need to shut down the connection if there's a
		// portal-related error. This is definitely a layering violation,
		// but is part of some accepted technical debt (see comments on
		// sql/pgwire.limitedCommandResult.moreResultsNeeded). Instead of
		// changing the signature of AddRow, we have a sentinel error that
		// is handled specially here.
		if !errors.Is(commErr, ErrLimitedResultNotSupported) {
			r.commErr = commErr
		}
	}
}

// Push is part of the execinfra.RowReceiver interface.
func (r *DistSQLReceiver) Push(
	row rowenc.EncDatumRow, meta *execinfrapb.ProducerMetadata,
) execinfra.ConsumerStatus {
	if r.testingKnobs.pushCallback != nil {
		r.testingKnobs.pushCallback(row, meta)
	}
	if meta != nil {
		return r.pushMeta(meta)
	}
	if r.resultWriter.Err() == nil && r.ctx.Err() != nil {
		r.SetError(r.ctx.Err())
	}
	if r.status != execinfra.NeedMoreRows {
		return r.status
	}

	if r.stmtType != tree.Rows {
		// We only need the row count. planNodeToRowSource is set up to handle
		// ensuring that the last stage in the pipeline will return a single-column
		// row with the row count in it, so just grab that and exit.
		r.resultWriter.IncrementRowsAffected(r.ctx, int(tree.MustBeDInt(row[0].Datum)))
		return r.status
	}

	if r.discardRows {
		// Discard rows.
		return r.status
	}

	if r.existsMode {
		// In "exists" mode, the consumer is only looking for whether a single
		// row is pushed or not, so the contents do not matter.
		r.row = []tree.Datum{}
		log.VEvent(r.ctx, 2, `a row is pushed in "exists" mode, so transition to draining`)
		r.status = execinfra.DrainRequested
	} else {
		if r.row == nil {
			r.row = make(tree.Datums, len(row))
		}
		for i, encDatum := range row {
			err := encDatum.EnsureDecoded(r.outputTypes[i], &r.alloc)
			if err != nil {
				r.SetError(err)
				return r.status
			}
			r.row[i] = encDatum.Datum
		}
	}
	r.tracing.TraceExecRowsResult(r.ctx, r.row)
	if commErr := r.resultWriter.AddRow(r.ctx, r.row); commErr != nil {
		r.handleCommErr(commErr)
	}
	return r.status
}

// PushBatch is part of the execinfra.BatchReceiver interface.
func (r *DistSQLReceiver) PushBatch(
	batch coldata.Batch, meta *execinfrapb.ProducerMetadata,
) execinfra.ConsumerStatus {
	if meta != nil {
		return r.pushMeta(meta)
	}
	if r.resultWriter.Err() == nil && r.ctx.Err() != nil {
		r.SetError(r.ctx.Err())
	}
	if r.status != execinfra.NeedMoreRows {
		return r.status
	}

	if batch.Length() == 0 {
		// Nothing to do on the zero-length batch.
		return r.status
	}

	if r.stmtType != tree.Rows {
		// We only need the row count. planNodeToRowSource is set up to handle
		// ensuring that the last stage in the pipeline will return a single-column
		// row with the row count in it, so just grab that and exit.
		r.resultWriter.IncrementRowsAffected(r.ctx, int(batch.ColVec(0).Int64()[0]))
		return r.status
	}

	if r.discardRows {
		// Discard rows.
		return r.status
	}

	if r.existsMode {
		// Exists mode is only used by the subqueries which currently don't
		// support pushing batches.
		panic("unsupported exists mode for PushBatch")
	}
	r.tracing.TraceExecBatchResult(r.ctx, batch)
	if commErr := r.batchWriter.AddBatch(r.ctx, batch); commErr != nil {
		r.handleCommErr(commErr)
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

// ProducerDone is part of the execinfra.RowReceiver interface.
func (r *DistSQLReceiver) ProducerDone() {
	if r.closed {
		panic("double close")
	}
	r.closed = true
}

// PlanAndRunSubqueries returns false if an error was encountered and sets that
// error in the provided receiver. Note that if false is returned, then this
// function will have closed all the subquery plans because it assumes that the
// caller will not try to run the main plan given that the subqueries'
// evaluation failed.
// - subqueryResultMemAcc must be a non-nil memory account that the result of
//   subqueries' evaluation will be registered with. It is the caller's
//   responsibility to shrink (or close) the account accordingly, once the
//   references to those results are lost.
func (dsp *DistSQLPlanner) PlanAndRunSubqueries(
	ctx context.Context,
	planner *planner,
	evalCtxFactory func() *extendedEvalContext,
	subqueryPlans []subquery,
	recv *DistSQLReceiver,
	subqueryResultMemAcc *mon.BoundAccount,
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
			subqueryResultMemAcc,
		); err != nil {
			recv.SetError(err)
			// Usually we leave the closure of subqueries to occur when the
			// whole plan is being closed (i.e. planTop.close); however, since
			// we've encountered an error, we might never get to the point of
			// closing the whole plan, so we choose to defensively close the
			// subqueries here.
			for i := range subqueryPlans {
				subqueryPlans[i].plan.Close(ctx)
			}
			return false
		}
	}

	return true
}

// subqueryResultMemAcc must be a non-nil memory account that the result of the
// subquery's evaluation will be registered with. It is the caller's
// responsibility to shrink it (or close it) accordingly, once the references to
// those results are lost.
func (dsp *DistSQLPlanner) planAndRunSubquery(
	ctx context.Context,
	planIdx int,
	subqueryPlan subquery,
	planner *planner,
	evalCtx *extendedEvalContext,
	subqueryPlans []subquery,
	recv *DistSQLReceiver,
	subqueryResultMemAcc *mon.BoundAccount,
) error {
	subqueryMonitor := mon.NewMonitor(
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

	distributeSubquery := getPlanDistribution(
		ctx, planner, planner.execCfg.NodeID, planner.SessionData().DistSQLMode, subqueryPlan.plan,
	).WillDistribute()
	subqueryPlanCtx := dsp.NewPlanningCtx(ctx, evalCtx, planner, planner.txn, distributeSubquery)
	subqueryPlanCtx.stmtType = tree.Rows
	if planner.instrumentation.ShouldSaveFlows() {
		subqueryPlanCtx.saveFlows = subqueryPlanCtx.getDefaultSaveFlowsFunc(ctx, planner, planComponentTypeSubquery)
	}
	subqueryPlanCtx.traceMetadata = planner.instrumentation.traceMetadata
	subqueryPlanCtx.collectExecStats = planner.instrumentation.ShouldCollectExecStats()
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
	defer subqueryRecv.Release()
	var typs []*types.T
	if subqueryPlan.execMode == rowexec.SubqueryExecModeExists {
		subqueryRecv.existsMode = true
		typs = []*types.T{}
	} else {
		typs = subqueryPhysPlan.GetResultTypes()
	}
	var rows rowContainerHelper
	rows.init(typs, evalCtx, "subquery" /* opName */)
	defer rows.close(ctx)

	// TODO(yuzefovich): consider implementing batch receiving result writer.
	subqueryRowReceiver := NewRowResultWriter(&rows)
	subqueryRecv.resultWriter = subqueryRowReceiver
	subqueryPlans[planIdx].started = true
	dsp.Run(subqueryPlanCtx, planner.txn, subqueryPhysPlan, subqueryRecv, evalCtx, nil /* finishedSetupFn */)()
	if err := subqueryRowReceiver.Err(); err != nil {
		return err
	}
	switch subqueryPlan.execMode {
	case rowexec.SubqueryExecModeExists:
		// For EXISTS expressions, all we want to know if there is at least one row.
		hasRows := rows.len() != 0
		subqueryPlans[planIdx].result = tree.MakeDBool(tree.DBool(hasRows))
	case rowexec.SubqueryExecModeAllRows, rowexec.SubqueryExecModeAllRowsNormalized:
		// TODO(yuzefovich): this is unfortunate - we're materializing all
		// buffered rows into a single tuple kept in memory without any memory
		// accounting. Refactor it.
		var result tree.DTuple
		iterator := newRowContainerIterator(ctx, rows, typs)
		defer iterator.close()
		for {
			row, err := iterator.next()
			if err != nil {
				return err
			}
			if row == nil {
				break
			}
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
		switch rows.len() {
		case 0:
			subqueryPlans[planIdx].result = tree.DNull
		case 1:
			iterator := newRowContainerIterator(ctx, rows, typs)
			defer iterator.close()
			row, err := iterator.next()
			if err != nil {
				return err
			}
			if row == nil {
				return errors.AssertionFailedf("iterator didn't return a row although container len is 1")
			}
			switch row.Len() {
			case 1:
				subqueryPlans[planIdx].result = row[0]
			default:
				subqueryPlans[planIdx].result = &tree.DTuple{D: row}
			}
		default:
			return pgerror.Newf(pgcode.CardinalityViolation,
				"more than one row returned by a subquery used as an expression")
		}
	default:
		return fmt.Errorf("unexpected subqueryExecMode: %d", subqueryPlan.execMode)
	}
	// Account for the result of the subquery using the separate memory account
	// since it outlives the execution of the subquery itself.
	if err := subqueryResultMemAcc.Grow(ctx, int64(subqueryPlans[planIdx].result.Size())); err != nil {
		return err
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
	log.VEventf(ctx, 2, "creating DistSQL plan with isLocal=%v", planCtx.isLocal)

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
		buf := plan.cascades[i].Buffer
		var numBufferedRows int
		if buf != nil {
			numBufferedRows = buf.(*bufferNode).rows.rows.Len()
			if numBufferedRows == 0 {
				// No rows were actually modified.
				continue
			}
		}

		log.VEventf(ctx, 2, "executing cascade for constraint %s", plan.cascades[i].FKName)

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
		allowAutoCommit := planner.autoCommit
		if len(plan.checkPlans) > 0 || i < len(plan.cascades)-1 {
			allowAutoCommit = false
		}
		cascadePlan, err := plan.cascades[i].PlanFn(
			ctx, &planner.semaCtx, &evalCtx.EvalContext, execFactory,
			buf, numBufferedRows, allowAutoCommit,
		)
		if err != nil {
			recv.SetError(err)
			return false
		}
		cp := cascadePlan.(*planComponents)
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
		log.VEventf(ctx, 2, "executing check query %d out of %d", i+1, len(plan.checkPlans))
		if err := dsp.planAndRunPostquery(
			ctx,
			plan.checkPlans[i].plan,
			planner,
			evalCtxFactory(),
			recv,
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
) error {
	postqueryMonitor := mon.NewMonitor(
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

	distributePostquery := getPlanDistribution(
		ctx, planner, planner.execCfg.NodeID, planner.SessionData().DistSQLMode, postqueryPlan,
	).WillDistribute()
	postqueryPlanCtx := dsp.NewPlanningCtx(ctx, evalCtx, planner, planner.txn, distributePostquery)
	postqueryPlanCtx.stmtType = tree.Rows
	postqueryPlanCtx.ignoreClose = true
	if planner.instrumentation.ShouldSaveFlows() {
		postqueryPlanCtx.saveFlows = postqueryPlanCtx.getDefaultSaveFlowsFunc(ctx, planner, planComponentTypePostquery)
	}
	postqueryPlanCtx.traceMetadata = planner.instrumentation.traceMetadata
	postqueryPlanCtx.collectExecStats = planner.instrumentation.ShouldCollectExecStats()

	postqueryPhysPlan, err := dsp.createPhysPlan(postqueryPlanCtx, postqueryPlan)
	if err != nil {
		return err
	}
	dsp.FinalizePlan(postqueryPlanCtx, postqueryPhysPlan)

	postqueryRecv := recv.clone()
	defer postqueryRecv.Release()
	// TODO(yuzefovich): at the moment, errOnlyResultWriter is sufficient here,
	// but it may not be the case when we support cascades through the optimizer.
	postqueryResultWriter := &errOnlyResultWriter{}
	postqueryRecv.resultWriter = postqueryResultWriter
	postqueryRecv.batchWriter = postqueryResultWriter
	dsp.Run(postqueryPlanCtx, planner.txn, postqueryPhysPlan, postqueryRecv, evalCtx, nil /* finishedSetupFn */)()
	return postqueryRecv.resultWriter.Err()
}
