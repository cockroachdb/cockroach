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
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangecache"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/colflow"
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
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/ring"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

var settingDistSQLNumRunners = settings.RegisterIntSetting(
	settings.TenantWritable,
	"sql.distsql.num_runners",
	"determines the number of DistSQL runner goroutines used for issuing SetupFlow RPCs",
	// We use GOMAXPROCS instead of NumCPU because the former could be adjusted
	// based on cgroup limits (see cgroups.AdjustMaxProcs).
	//
	// The choice of the default multiple of 4 was made in order to get the
	// original value of 16 on machines with 4 CPUs.
	4*int64(runtime.GOMAXPROCS(0)), /* defaultValue */
	func(v int64) error {
		if v < 0 {
			return errors.Errorf("cannot be set to a negative value: %d", v)
		}
		if v > distSQLNumRunnersMax {
			return errors.Errorf("cannot be set to a value exceeding %d: %d", distSQLNumRunnersMax, v)
		}
		return nil
	},
)

// Somewhat arbitrary upper bound.
var distSQLNumRunnersMax = 256 * int64(runtime.GOMAXPROCS(0))

// runnerRequest is the request that is sent (via a channel) to a worker.
type runnerRequest struct {
	ctx           context.Context
	podNodeDialer *nodedialer.Dialer
	flowReq       *execinfrapb.SetupFlowRequest
	sqlInstanceID base.SQLInstanceID
	resultChan    chan<- runnerResult
}

// runnerResult is returned by a worker (via a channel) for each received
// request.
type runnerResult struct {
	nodeID base.SQLInstanceID
	err    error
}

// run executes the request. An error, if encountered, is both sent on the
// result channel and returned.
func (req runnerRequest) run() error {
	res := runnerResult{nodeID: req.sqlInstanceID}
	defer func() {
		req.resultChan <- res
		physicalplan.ReleaseFlowSpec(&req.flowReq.Flow)
	}()

	conn, err := req.podNodeDialer.Dial(req.ctx, roachpb.NodeID(req.sqlInstanceID), rpc.DefaultClass)
	if err != nil {
		res.err = err
		return err
	}
	client := execinfrapb.NewDistSQLClient(conn)
	// TODO(radu): do we want a timeout here?
	resp, err := client.SetupFlow(req.ctx, req.flowReq)
	if err != nil {
		res.err = err
	} else {
		res.err = resp.Error.ErrorDetail(req.ctx)
	}
	return res.err
}

type runnerCoordinator struct {
	// runnerChan is used by the DistSQLPlanner to send out requests (for
	// running SetupFlow RPCs) to a pool of workers.
	runnerChan chan runnerRequest
	// newDesiredNumWorkers is used to notify the coordinator that the size of
	// the pool of workers might have changed.
	newDesiredNumWorkers chan int64
	atomics              struct {
		// numWorkers tracks the number of workers running at the moment. This
		// needs to be accessed atomically, but only because of the usage in
		// tests.
		numWorkers int64
	}
}

func (c *runnerCoordinator) init(ctx context.Context, stopper *stop.Stopper, sv *settings.Values) {
	// This channel has to be unbuffered because we want to only be able to send
	// requests if a worker is actually there to receive them.
	c.runnerChan = make(chan runnerRequest)
	stopWorkerChan := make(chan struct{})
	worker := func(context.Context) {
		for {
			select {
			case req := <-c.runnerChan:
				_ = req.run()
			case <-stopWorkerChan:
				return
			}
		}
	}
	stopChan := stopper.ShouldQuiesce()
	// This is a buffered channel because we will be sending on it from the
	// callback when the corresponding setting changes. The buffer size of 1
	// should be sufficient, but we use a larger buffer out of caution (in case
	// the cluster setting is updated rapidly) - in order to not block the
	// goroutine that is updating the settings.
	c.newDesiredNumWorkers = make(chan int64, 4)
	// setNewNumWorkers sets the new target size of the pool of workers.
	setNewNumWorkers := func(newNumWorkers int64) {
		select {
		case c.newDesiredNumWorkers <- newNumWorkers:
		case <-stopChan:
			// If the server is quescing, then the new size of the pool doesn't
			// matter.
			return
		}
	}
	// Whenever the corresponding setting is updated, we need to notify the
	// coordinator.
	// NB: runnerCoordinator.init is called once per server lifetime so this
	// won't leak an unbounded number of OnChange callbacks.
	settingDistSQLNumRunners.SetOnChange(sv, func(ctx context.Context) {
		setNewNumWorkers(settingDistSQLNumRunners.Get(sv))
	})
	// We need to set the target pool size based on the current setting
	// explicitly since the OnChange callback won't ever be called for the
	// initial value - the setting initialization has already been performed
	// before we registered the OnChange callback.
	setNewNumWorkers(settingDistSQLNumRunners.Get(sv))
	// Spin up the coordinator goroutine.
	_ = stopper.RunAsyncTask(ctx, "distsql-runner-coordinator", func(context.Context) {
		// Make sure to stop all workers when the coordinator exits.
		defer close(stopWorkerChan)
		for {
			select {
			case newNumWorkers := <-c.newDesiredNumWorkers:
				for {
					numWorkers := atomic.LoadInt64(&c.atomics.numWorkers)
					if numWorkers == newNumWorkers {
						break
					}
					if numWorkers < newNumWorkers {
						// Need to spin another worker.
						err := stopper.RunAsyncTask(ctx, "distsql-runner", worker)
						if err != nil {
							return
						}
						atomic.AddInt64(&c.atomics.numWorkers, 1)
					} else {
						// Need to stop one of the workers.
						select {
						case stopWorkerChan <- struct{}{}:
							atomic.AddInt64(&c.atomics.numWorkers, -1)
						case <-stopChan:
							return
						}
					}
				}
			case <-stopChan:
				return
			}
		}
	})
}

// To allow for canceling flows via CancelDeadFlows RPC on different nodes
// simultaneously, we use a pool of workers.
const numCancelingWorkers = 4

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
					req, sqlInstanceID := dsp.cancelFlowsCoordinator.getFlowsToCancel()
					if req == nil {
						// There are no flows to cancel at the moment. This
						// shouldn't really happen.
						log.VEventf(parentCtx, 2, "worker %d woke up but didn't find any flows to cancel", workerID)
						continue
					}
					log.VEventf(parentCtx, 2, "worker %d is canceling at most %d flows on node %d", workerID, len(req.FlowIDs), sqlInstanceID)
					// TODO: Double check that we only ever cancel flows on SQL nodes/pods here.
					conn, err := dsp.podNodeDialer.Dial(parentCtx, roachpb.NodeID(sqlInstanceID), rpc.DefaultClass)
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
	ids           []execinfrapb.FlowID
	sqlInstanceID base.SQLInstanceID
}

// cancelFlowsCoordinator is responsible for batching up the requests to cancel
// remote flows initiated on the behalf of the current node when the local flows
// errored out.
//
// This mechanism is used in addition to the cancellation that occurs when the
// gRPC streams are closed between nodes, which happens when the query errors
// out. This mechanism works on the best-effort basis.
type cancelFlowsCoordinator struct {
	mu struct {
		syncutil.Mutex
		// deadFlowsByNode is a ring of pointers to deadFlowsOnNode objects.
		deadFlowsByNode ring.Buffer[*deadFlowsOnNode]
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
	base.SQLInstanceID,
) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mu.deadFlowsByNode.Len() == 0 {
		return nil, base.SQLInstanceID(0)
	}
	deadFlows := c.mu.deadFlowsByNode.GetFirst()
	c.mu.deadFlowsByNode.RemoveFirst()
	req := &execinfrapb.CancelDeadFlowsRequest{
		FlowIDs: deadFlows.ids,
	}
	return req, deadFlows.sqlInstanceID
}

// addFlowsToCancel adds all remote flows from flows map to be canceled via
// CancelDeadFlows RPC. Safe for concurrent usage.
func (c *cancelFlowsCoordinator) addFlowsToCancel(
	flows map[base.SQLInstanceID]*execinfrapb.FlowSpec,
) {
	c.mu.Lock()
	for sqlInstanceID, f := range flows {
		if sqlInstanceID != f.Gateway {
			// c.mu.deadFlowsByNode.Len() is at most the number of nodes in the
			// cluster, so a linear search for the node ID should be
			// sufficiently fast.
			found := false
			for j := 0; j < c.mu.deadFlowsByNode.Len(); j++ {
				deadFlows := c.mu.deadFlowsByNode.Get(j)
				if sqlInstanceID == deadFlows.sqlInstanceID {
					deadFlows.ids = append(deadFlows.ids, f.FlowID)
					found = true
					break
				}
			}
			if !found {
				c.mu.deadFlowsByNode.AddLast(&deadFlowsOnNode{
					ids:           []execinfrapb.FlowID{f.FlowID},
					sqlInstanceID: sqlInstanceID,
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
// It will first attempt to set up the gateway flow (whose output is the
// DistSQLReceiver provided) and - if successful - will proceed to setting up
// the remote flows using the dsp workers if available or sequentially if not.
//
// The gateway flow is returned to be Run(). It is the caller's responsibility
// to clean up that flow if a non-nil value is returned.
//
// The method doesn't wait for the setup of remote flows (if any) to return and,
// instead, optimistically proceeds once the corresponding SetupFlow RPCs are
// issued. A separate goroutine is spun up to listen for the RPCs to come back,
// and if the setup of a remote flow fails, then that goroutine updates the
// DistSQLReceiver with the error and cancels the gateway flow.
func (dsp *DistSQLPlanner) setupFlows(
	ctx context.Context,
	evalCtx *extendedEvalContext,
	planCtx *PlanningCtx,
	leafInputState *roachpb.LeafTxnInputState,
	flows map[base.SQLInstanceID]*execinfrapb.FlowSpec,
	recv *DistSQLReceiver,
	localState distsql.LocalState,
	statementSQL string,
) (context.Context, flowinfra.Flow, error) {
	thisNodeID := dsp.gatewaySQLInstanceID
	_, ok := flows[thisNodeID]
	if !ok {
		return nil, nil, errors.AssertionFailedf("missing gateway flow")
	}
	if localState.IsLocal && len(flows) != 1 {
		return nil, nil, errors.AssertionFailedf("IsLocal set but there's multiple flows")
	}

	const setupFlowRequestStmtMaxLength = 500
	if len(statementSQL) > setupFlowRequestStmtMaxLength {
		statementSQL = statementSQL[:setupFlowRequestStmtMaxLength]
	}
	getJobTag := func(ctx context.Context) string {
		tags := logtags.FromContext(ctx)
		if tags != nil {
			for _, tag := range tags.Get() {
				if tag.Key() == "job" {
					return tag.ValueStr()
				}
			}
		}
		return ""
	}
	setupReq := execinfrapb.SetupFlowRequest{
		// TODO(yuzefovich): avoid populating some fields of the SetupFlowRequest
		// for local plans.
		LeafTxnInputState: leafInputState,
		Version:           execinfra.Version,
		EvalContext:       execinfrapb.MakeEvalContext(&evalCtx.Context),
		TraceKV:           evalCtx.Tracing.KVTracingEnabled(),
		CollectStats:      planCtx.collectExecStats,
		StatementSQL:      statementSQL,
		JobTag:            getJobTag(ctx),
	}

	var isVectorized bool
	if vectorizeMode := evalCtx.SessionData().VectorizeMode; vectorizeMode != sessiondatapb.VectorizeOff {
		// Now we determine whether the vectorized engine supports the flow
		// specs.
		isVectorized = true
		for _, spec := range flows {
			if err := colflow.IsSupported(vectorizeMode, spec); err != nil {
				log.VEventf(ctx, 2, "failed to vectorize: %s", err)
				if vectorizeMode == sessiondatapb.VectorizeExperimentalAlways {
					return nil, nil, err
				}
				// Vectorization is not supported for this flow, so we override
				// the setting.
				setupReq.EvalContext.SessionData.VectorizeMode = sessiondatapb.VectorizeOff
				isVectorized = false
				break
			}
		}
	}
	if planCtx.planner != nil && isVectorized {
		planCtx.planner.curPlan.flags.Set(planFlagVectorized)
	}

	// First, set up the flow on this node.
	setupReq.Flow = *flows[thisNodeID]
	var batchReceiver execinfra.BatchReceiver
	if recv.resultWriterMu.batch != nil {
		// Use the DistSQLReceiver as an execinfra.BatchReceiver only if the
		// former has the corresponding writer set.
		batchReceiver = recv
	}
	origCtx := ctx
	ctx, flow, opChains, err := dsp.distSQLSrv.SetupLocalSyncFlow(ctx, evalCtx.Planner.Mon(), &setupReq, recv, batchReceiver, localState)
	if err == nil && planCtx.saveFlows != nil {
		err = planCtx.saveFlows(flows, opChains)
	}
	if len(flows) == 1 || err != nil {
		// If there are no remote flows, or we fail to set up the local flow, we
		// can just short-circuit.
		//
		// Note that we need to return the local flow even if err is non-nil so
		// that the local flow is properly cleaned up.
		return ctx, flow, err
	}

	// Start all the remote flows.
	//
	// usedWorker indicates whether we used at least one DistSQL worker
	// goroutine to issue the SetupFlow RPC.
	var usedWorker bool
	// numIssuedRequests tracks the number of the SetupFlow RPCs that were
	// issued (either by the current goroutine directly or delegated to the
	// DistSQL workers).
	var numIssuedRequests int
	if sp := tracing.SpanFromContext(origCtx); sp != nil && !sp.IsNoop() {
		setupReq.TraceInfo = sp.Meta().ToProto()
	}
	resultChan := make(chan runnerResult, len(flows)-1)
	// We use a separate context for the runnerRequests so that - in case they
	// are issued concurrently and some RPCs are actually run after the current
	// goroutine performs Flow.Cleanup() on the local flow - DistSQL workers
	// don't attempt to reuse already finished tracing span from the local flow
	// context. For the same reason we can't use origCtx (parent of the local
	// flow context).
	//
	// In particular, consider the following scenario:
	// - a runnerRequest is handled by the DistSQL worker;
	// - in runnerRequest.run() the worker dials the remote node and issues the
	//   SetupFlow RPC. However, the client-side goroutine of that RPC is not
	//   yet scheduled on the local node;
	// - the local flow runs to completion canceling the context and finishing
	//   the tracing span;
	// - now the client-side goroutine of the RPC is scheduled, and it attempts
	//   to use the span from the context, but it has already been finished.
	runnerCtx, cancelRunnerCtx := context.WithCancel(origCtx)
	var runnerSpan *tracing.Span
	// This span is necessary because it can outlive its parent.
	runnerCtx, runnerSpan = tracing.ChildSpan(runnerCtx, "setup-flow-async" /* opName */)
	// runnerCleanup can only be executed _after_ all issued RPCs are complete.
	runnerCleanup := func() {
		cancelRunnerCtx()
		runnerSpan.Finish()
	}
	// Make sure that we call runnerCleanup unless a new goroutine takes that
	// responsibility.
	var listenerGoroutineWillCleanup bool
	defer func() {
		if !listenerGoroutineWillCleanup {
			// Make sure to receive from the result channel as many times as
			// there were total SetupFlow RPCs issued, regardless of whether
			// they were executed concurrently by a DistSQL worker or serially
			// in the current goroutine. This is needed in order to block
			// finishing the runner span (in runnerCleanup) until all concurrent
			// requests are done since the runner span is used as the parent for
			// the RPC span, and, thus, the runner span can only be finished
			// when we know that all SetupFlow RPCs have already been completed.
			//
			// Note that even in case of an error in runnerRequest.run we still
			// send on the result channel.
			for i := 0; i < numIssuedRequests; i++ {
				<-resultChan
			}
			// At this point, we know that all concurrent requests (if there
			// were any) are complete, so we can safely perform the runner
			// cleanup.
			runnerCleanup()
		}
	}()
	for nodeID, flowSpec := range flows {
		if nodeID == thisNodeID {
			// Skip this node since we already handled the local flow above.
			continue
		}
		req := setupReq
		req.Flow = *flowSpec
		runReq := runnerRequest{
			ctx:           runnerCtx,
			podNodeDialer: dsp.podNodeDialer,
			flowReq:       &req,
			sqlInstanceID: nodeID,
			resultChan:    resultChan,
		}

		// Send out a request to the workers; if no worker is available, run
		// directly.
		numIssuedRequests++
		select {
		case dsp.runnerCoordinator.runnerChan <- runReq:
			usedWorker = true
		default:
			// Use the context of the local flow since we're executing this
			// SetupFlow RPC synchronously.
			runReq.ctx = ctx
			if err = runReq.run(); err != nil {
				return ctx, flow, err
			}
		}
	}

	if !usedWorker {
		// We executed all SetupFlow RPCs in the current goroutine, and all RPCs
		// succeeded.
		return ctx, flow, nil
	}

	// Some of the SetupFlow RPCs were executed concurrently, and at the moment
	// it's not clear whether the setup of the remote flows is successful, but
	// in order to not introduce an execution stall, we will proceed to run the
	// local flow assuming that all RPCs are successful. However, in case the
	// setup of a remote flow fails, we want to eagerly cancel all the flows,
	// and we do so in a separate goroutine.
	//
	// We need to synchronize the new goroutine with flow.Cleanup() being called
	// for two reasons:
	// - flow.Cleanup() is the last thing before DistSQLPlanner.Run returns at
	// which point the rowResultWriter is no longer protected by the mutex of
	// the DistSQLReceiver
	// - flow.Cancel can only be called before flow.Cleanup.
	cleanupCalledMu := struct {
		syncutil.Mutex
		called bool
	}{}
	flow.AddOnCleanupStart(func() {
		cleanupCalledMu.Lock()
		defer cleanupCalledMu.Unlock()
		cleanupCalledMu.called = true
		// Cancel any outstanding RPCs while holding the lock to protect from
		// the context canceled error (the result of the RPC) being set on the
		// DistSQLReceiver by the listener goroutine below.
		cancelRunnerCtx()
	})
	err = dsp.stopper.RunAsyncTask(origCtx, "distsql-remote-flows-setup-listener", func(ctx context.Context) {
		// Note that in the loop below we always receive from the result channel
		// as many times as there were SetupFlow RPCs issued, thus, by the time
		// this defer is executed, we are certain that all RPCs were complete,
		// and runnerCleanup() is safe to be executed.
		defer runnerCleanup()
		var seenError bool
		for i := 0; i < len(flows)-1; i++ {
			res := <-resultChan
			if res.err != nil && !seenError {
				// The setup of at least one remote flow failed.
				seenError = true
				func() {
					cleanupCalledMu.Lock()
					// Flow.Cancel cannot be called after or concurrently with
					// Flow.Cleanup.
					defer cleanupCalledMu.Unlock()
					if cleanupCalledMu.called {
						// Cleanup of the local flow has already been performed,
						// so there is nothing to do.
						return
					}
					// First, we update the DistSQL receiver with the error to
					// be returned to the client eventually.
					//
					// In order to not protect DistSQLReceiver.status with a
					// mutex, we do not update the status here and, instead,
					// rely on the DistSQLReceiver detecting the error the next
					// time an object is pushed into it.
					recv.setErrorWithoutStatusUpdate(res.err, true /* willDeferStatusUpdate */)
					// Now explicitly cancel the local flow.
					flow.Cancel()
				}()
			}
		}
	})
	if err != nil {
		return ctx, flow, err
	}
	// Now the responsibility of calling runnerCleanup is passed on to the new
	// goroutine.
	listenerGoroutineWillCleanup = true
	return ctx, flow, nil
}

const clientRejectedMsg string = "client rejected when attempting to run DistSQL plan"

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
func (dsp *DistSQLPlanner) Run(
	ctx context.Context,
	planCtx *PlanningCtx,
	txn *kv.Txn,
	plan *PhysicalPlan,
	recv *DistSQLReceiver,
	evalCtx *extendedEvalContext,
	finishedSetupFn func(),
) {
	flows := plan.GenerateFlowSpecs()
	gatewayFlowSpec, ok := flows[dsp.gatewaySQLInstanceID]
	if !ok {
		recv.SetError(errors.Errorf("expected to find gateway flow"))
		return
	}
	// Specs of the remote flows are released after performing the corresponding
	// SetupFlow RPCs. This is needed in case the local flow is canceled before
	// the SetupFlow RPCs are issued (which might happen in parallel).
	defer physicalplan.ReleaseFlowSpec(gatewayFlowSpec)

	var (
		localState     distsql.LocalState
		leafInputState *roachpb.LeafTxnInputState
	)
	// NB: putting part of evalCtx in localState means it might be mutated down
	// the line.
	localState.EvalContext = &evalCtx.Context
	localState.IsLocal = planCtx.isLocal
	localState.Txn = txn
	localState.LocalProcs = plan.LocalProcessors
	// If we have access to a planner and are currently being used to plan
	// statements in a user transaction, then take the descs.Collection to resolve
	// types with during flow execution. This is necessary to do in the case of
	// a transaction that has already created or updated some types. If we do not
	// use the local descs.Collection, we would attempt to acquire a lease on
	// modified types when accessing them, which would error out.
	if planCtx.planner != nil &&
		(!planCtx.planner.isInternalPlanner || planCtx.usePlannerDescriptorsForLocalFlow) {
		localState.Collection = planCtx.planner.Descriptors()
	}

	// noMutations indicates whether we know for sure that the plan doesn't have
	// any mutations. If we don't have the access to the planner (which can be
	// the case not on the main query execution path, i.e. BulkIO, CDC, etc),
	// then we are ignorant of the details of the execution plan, so we choose
	// to be on the safe side and mark 'noMutations' as 'false'.
	noMutations := planCtx.planner != nil && !planCtx.planner.curPlan.flags.IsSet(planFlagContainsMutation)

	if txn == nil {
		// Txn can be nil in some cases, like BulkIO flows. In such a case, we
		// cannot create a LeafTxn, so we cannot parallelize scans.
		planCtx.parallelizeScansIfLocal = false
	} else {
		if planCtx.isLocal && noMutations && planCtx.parallelizeScansIfLocal {
			// Even though we have a single flow on the gateway node, we might
			// have decided to parallelize the scans. If that's the case, we
			// will need to use the Leaf txn.
			for _, flow := range flows {
				localState.HasConcurrency = localState.HasConcurrency || execinfra.HasParallelProcessors(flow)
			}
		}
		if noMutations {
			// Even if planCtx.isLocal is false (which is the case when we think
			// it's worth distributing the query), we need to go through the
			// processors to figure out whether any of them have concurrency.
			//
			// However, the concurrency requires the usage of LeafTxns which is
			// only acceptable if we don't have any mutations in the plan.
			// TODO(yuzefovich): we could be smarter here and allow the usage of
			// the RootTxn by the mutations while still using the Streamer (that
			// gets a LeafTxn) iff the plan is such that there is no concurrency
			// between the root and the leaf txns.
			//
			// At the moment of writing, this is only relevant whenever the
			// Streamer API might be used by some of the processors. The
			// Streamer internally can have concurrency, so it expects to be
			// given a LeafTxn. In order for that LeafTxn to be created later,
			// during the flow setup, we need to populate leafInputState below,
			// so we tell the localState that there is concurrency.

			// At the moment, we disable the usage of the Streamer API for local
			// plans when non-default key locking modes are requested on some of
			// the processors. This is the case since the lock spans propagation
			// doesn't happen for the leaf txns which can result in excessive
			// contention for future reads (since the acquired locks are not
			// cleaned up properly when the txn commits).
			// TODO(yuzefovich): fix the propagation of the lock spans with the
			// leaf txns and remove this check. See #94290.
			containsNonDefaultLocking := planCtx.planner != nil && planCtx.planner.curPlan.flags.IsSet(planFlagContainsNonDefaultLocking)
			if !containsNonDefaultLocking {
				if execinfra.CanUseStreamer(dsp.st) {
					for _, proc := range plan.Processors {
						if jr := proc.Spec.Core.JoinReader; jr != nil {
							// Both index and lookup joins, with and without
							// ordering, are executed via the Streamer API that has
							// concurrency.
							localState.HasConcurrency = true
							break
						}
					}
				}
			}
		}
		if localState.MustUseLeafTxn() {
			// Set up leaf txns using the txnCoordMeta if we need to.
			tis, err := txn.GetLeafTxnInputStateOrRejectClient(ctx)
			if err != nil {
				log.Infof(ctx, "%s: %s", clientRejectedMsg, err)
				recv.SetError(err)
				return
			}
			if tis == nil {
				recv.SetError(errors.AssertionFailedf(
					"leafInputState is nil when txn is non-nil and we must use the leaf txn",
				))
				return
			}
			leafInputState = tis
		}
	}

	if !planCtx.skipDistSQLDiagramGeneration && log.ExpensiveLogEnabled(ctx, 2) {
		var stmtStr string
		if planCtx.planner != nil && planCtx.planner.stmt.AST != nil {
			stmtStr = planCtx.planner.stmt.String()
		}
		_, url, err := execinfrapb.GeneratePlanDiagramURL(stmtStr, flows, execinfrapb.DiagramFlags{})
		if err != nil {
			log.VEventf(ctx, 2, "error generating diagram: %s", err)
		} else {
			log.VEventf(ctx, 2, "plan diagram URL:\n%s", url.String())
		}
	}

	log.VEvent(ctx, 2, "running DistSQL plan")

	dsp.distSQLSrv.ServerConfig.Metrics.QueryStart()
	defer dsp.distSQLSrv.ServerConfig.Metrics.QueryStop()

	recv.outputTypes = plan.GetResultTypes()
	if multitenant.TenantRUEstimateEnabled.Get(&dsp.st.SV) &&
		dsp.distSQLSrv.TenantCostController != nil && planCtx.planner != nil {
		if instrumentation := planCtx.planner.curPlan.instrumentation; instrumentation != nil {
			// Only collect the network egress estimate for a tenant that is running
			// EXPLAIN ANALYZE, since the overhead is non-negligible.
			recv.isTenantExplainAnalyze = instrumentation.outputMode != unmodifiedOutput
		}
	}

	if len(flows) == 1 {
		// We ended up planning everything locally, regardless of whether we
		// intended to distribute or not.
		localState.IsLocal = true
	} else {
		defer func() {
			if recv.getError() != nil {
				// The execution of this query encountered some error, so we
				// will eagerly cancel all flows running on the remote nodes
				// because they are now dead.
				dsp.cancelFlowsCoordinator.addFlowsToCancel(flows)
			}
		}()
	}

	// Currently, we get the statement only if there is a planner available in
	// the planCtx which is the case only on the "main" query path (for
	// user-issued queries).
	// TODO(yuzefovich): propagate the statement in all cases.
	var statementSQL string
	if planCtx.planner != nil {
		statementSQL = planCtx.planner.stmt.StmtNoConstants
	}
	ctx, flow, err := dsp.setupFlows(
		ctx, evalCtx, planCtx, leafInputState, flows, recv, localState, statementSQL,
	)
	// Make sure that the local flow is always cleaned up if it was created.
	if flow != nil {
		defer func() {
			flow.Cleanup(ctx)
		}()
	}
	if err != nil {
		recv.SetError(err)
		return
	}

	if finishedSetupFn != nil {
		finishedSetupFn()
	}

	// Check that flows that were forced to be planned locally and didn't need
	// to have concurrency don't actually have it.
	//
	// This is important, since these flows are forced to use the RootTxn (since
	// they might have mutations), and the RootTxn does not permit concurrency.
	// For such flows, we were supposed to have fused everything.
	if txn != nil && !localState.MustUseLeafTxn() && flow.ConcurrentTxnUse() {
		recv.SetError(errors.AssertionFailedf(
			"unexpected concurrency for a flow that was forced to be planned locally"))
		return
	}

	flow.Run(ctx)
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

	resultWriterMu struct {
		// Mutex only protects SetError() and Err() methods of the
		// rowResultWriter.
		syncutil.Mutex
		// These two interfaces refer to the same object, but batch might be
		// unset (row is always set). These are used to send the results to.
		row   rowResultWriter
		batch batchResultWriter
	}
	// updateStatus, if true, indicates that a concurrent goroutine has set an
	// error on the rowResultWriter without updating status, so the main
	// goroutine needs to update the status.
	updateStatus atomic.Bool
	status       execinfra.ConsumerStatus

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
	alloc  tree.DatumAlloc
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

	stats *topLevelQueryStats

	// isTenantExplainAnalyze is used to indicate that network egress should be
	// collected in order to estimate RU consumption for a tenant that is running
	// a query with EXPLAIN ANALYZE.
	isTenantExplainAnalyze bool

	egressCounter TenantNetworkEgressCounter

	expectedRowsRead int64
	progressAtomic   *uint64

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

// TenantNetworkEgressCounter is used by tenants running EXPLAIN ANALYZE to
// measure the number of bytes that would be sent over the network if the
// query result was returned to the client. Its implementation lives in the
// pgwire package, in conn.go.
type TenantNetworkEgressCounter interface {
	// GetRowNetworkEgress estimates network egress for a row.
	GetRowNetworkEgress(ctx context.Context, row tree.Datums, typs []*types.T) int64
	// GetBatchNetworkEgress estimates network egress for a batch.
	GetBatchNetworkEgress(ctx context.Context, batch coldata.Batch) int64
}

// NewTenantNetworkEgressCounter is used to create a tenantNetworkEgressCounter.
// It hooks into pgwire code.
var NewTenantNetworkEgressCounter func() TenantNetworkEgressCounter

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

// NewMetadataOnlyMetadataCallbackWriter creates a new MetadataCallbackWriter
// that uses errOnlyResultWriter and only supports receiving
// execinfrapb.ProducerMetadata.
func NewMetadataOnlyMetadataCallbackWriter() *MetadataCallbackWriter {
	return NewMetadataCallbackWriter(
		&errOnlyResultWriter{},
		func(ctx context.Context, meta *execinfrapb.ProducerMetadata) error {
			return nil
		},
	)
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

// RowResultWriter is a thin wrapper around a RowContainer.
type RowResultWriter struct {
	rowContainer *rowContainerHelper
	rowsAffected int
	err          error
}

var _ rowResultWriter = &RowResultWriter{}

// NewRowResultWriter creates a new RowResultWriter.
func NewRowResultWriter(rowContainer *rowContainerHelper) *RowResultWriter {
	return &RowResultWriter{rowContainer: rowContainer}
}

// IncrementRowsAffected implements the rowResultWriter interface.
func (b *RowResultWriter) IncrementRowsAffected(ctx context.Context, n int) {
	b.rowsAffected += n
}

// AddRow implements the rowResultWriter interface.
func (b *RowResultWriter) AddRow(ctx context.Context, row tree.Datums) error {
	if b.rowContainer != nil {
		return b.rowContainer.AddRow(ctx, row)
	}
	return nil
}

// SetError is part of the rowResultWriter interface.
func (b *RowResultWriter) SetError(err error) {
	b.err = err
}

// Err is part of the rowResultWriter interface.
func (b *RowResultWriter) Err() error {
	return b.err
}

// CallbackResultWriter is a rowResultWriter that runs a callback function
// on AddRow.
type CallbackResultWriter struct {
	fn           func(ctx context.Context, row tree.Datums) error
	rowsAffected int
	err          error
}

var _ rowResultWriter = &CallbackResultWriter{}

// NewCallbackResultWriter creates a new CallbackResultWriter.
func NewCallbackResultWriter(
	fn func(ctx context.Context, row tree.Datums) error,
) *CallbackResultWriter {
	return &CallbackResultWriter{fn: fn}
}

// IncrementRowsAffected is part of the rowResultWriter interface.
func (c *CallbackResultWriter) IncrementRowsAffected(ctx context.Context, n int) {
	c.rowsAffected += n
}

// AddRow is part of the rowResultWriter interface.
func (c *CallbackResultWriter) AddRow(ctx context.Context, row tree.Datums) error {
	return c.fn(ctx, row)
}

// SetError is part of the rowResultWriter interface.
func (c *CallbackResultWriter) SetError(err error) {
	c.err = err
}

// Err is part of the rowResultWriter interface.
func (c *CallbackResultWriter) Err() error {
	return c.err
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
		ctx:          consumeCtx,
		cleanup:      cleanup,
		rangeCache:   rangeCache,
		txn:          txn,
		clockUpdater: clockUpdater,
		stats:        &topLevelQueryStats{},
		stmtType:     stmtType,
		tracing:      tracing,
	}
	r.resultWriterMu.row = resultWriter
	r.resultWriterMu.batch = batchWriter
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
		ctx:          r.ctx,
		cleanup:      func() {},
		rangeCache:   r.rangeCache,
		txn:          r.txn,
		clockUpdater: r.clockUpdater,
		stats:        r.stats,
		stmtType:     tree.Rows,
		tracing:      r.tracing,
	}
	return ret
}

// getError returns the error stored in the rowResultWriter (if any).
func (r *DistSQLReceiver) getError() error {
	r.resultWriterMu.Lock()
	defer r.resultWriterMu.Unlock()
	return r.resultWriterMu.row.Err()
}

// setErrorWithoutStatusUpdate sets the error in the rowResultWriter but does
// **not** update the status of the DistSQLReceiver. willDeferStatusUpdate
// indicates whether the main goroutine should update the status the next time
// it pushes something into the DistSQLReceiver.
//
// NOTE: consider using SetError() instead.
func (r *DistSQLReceiver) setErrorWithoutStatusUpdate(err error, willDeferStatusUpdate bool) {
	r.resultWriterMu.Lock()
	defer r.resultWriterMu.Unlock()
	// Check if the error we just received should take precedence over a
	// previous error (if any).
	if roachpb.ErrPriority(err) > roachpb.ErrPriority(r.resultWriterMu.row.Err()) {
		if r.txn != nil {
			if retryErr := (*roachpb.UnhandledRetryableError)(nil); errors.As(err, &retryErr) {
				// Update the txn in response to remote errors. In the
				// non-DistSQL world, the TxnCoordSender handles "unhandled"
				// retryable errors, but this one is coming from a distributed
				// SQL node, which has left the handling up to the root
				// transaction.
				err = r.txn.UpdateStateOnRemoteRetryableErr(r.ctx, &retryErr.PErr)
				// Update the clock with information from the error. On
				// non-DistSQL code paths, the DistSender does this.
				// TODO(andrei): We don't propagate clock signals on success
				// cases through DistSQL; we should. We also don't propagate
				// them through non-retryable errors; we also should.
				if r.clockUpdater != nil {
					r.clockUpdater.Update(retryErr.PErr.Now)
				}
			}
		}
		r.resultWriterMu.row.SetError(err)
		r.updateStatus.Store(willDeferStatusUpdate)
	}
}

// updateStatusAfterError updates the status of the DistSQLReceiver after it
// has received an error.
func (r *DistSQLReceiver) updateStatusAfterError(err error) {
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

// SetError provides a convenient way for a client to pass in an error, thus
// pretending that a query execution error happened. The error is passed along
// to the resultWriter.
//
// The status of DistSQLReceiver is updated accordingly.
func (r *DistSQLReceiver) SetError(err error) {
	r.setErrorWithoutStatusUpdate(err, false /* willDeferStatusUpdate */)
	r.updateStatusAfterError(err)
}

// checkConcurrentError sets the status if an error has been set by another
// goroutine that did not also update the status.
func (r *DistSQLReceiver) checkConcurrentError() {
	if r.status != execinfra.NeedMoreRows || !r.updateStatus.Load() {
		// If the status already is not NeedMoreRows, then it doesn't matter if
		// there was a concurrent error set.
		return
	}
	previousErr := r.getError()
	if previousErr == nil {
		previousErr = errors.AssertionFailedf("unexpectedly updateStatus is set but there is no error")
	}
	r.updateStatusAfterError(previousErr)
}

// pushMeta takes in non-empty metadata object and pushes it to the result
// writer. Possibly updated status is returned.
func (r *DistSQLReceiver) pushMeta(meta *execinfrapb.ProducerMetadata) execinfra.ConsumerStatus {
	if metaWriter, ok := r.resultWriterMu.row.(MetadataResultWriter); ok {
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
		r.SetError(meta.Err)
	}
	if len(meta.Ranges) > 0 {
		r.rangeCache.Insert(r.ctx, meta.Ranges...)
	}
	if len(meta.TraceData) > 0 {
		if span := tracing.SpanFromContext(r.ctx); span != nil {
			span.ImportRemoteRecording(meta.TraceData)
		}
	}
	if meta.Metrics != nil {
		r.stats.bytesRead += meta.Metrics.BytesRead
		r.stats.rowsRead += meta.Metrics.RowsRead
		r.stats.rowsWritten += meta.Metrics.RowsWritten
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
	r.checkConcurrentError()
	if r.testingKnobs.pushCallback != nil {
		r.testingKnobs.pushCallback(row, meta)
	}
	if meta != nil {
		return r.pushMeta(meta)
	}
	if r.ctx.Err() != nil && r.status != execinfra.ConsumerClosed {
		r.SetError(r.ctx.Err())
	}
	if r.status != execinfra.NeedMoreRows {
		return r.status
	}

	if r.stmtType != tree.Rows {
		n := int(tree.MustBeDInt(row[0].Datum))
		// We only need the row count. planNodeToRowSource is set up to handle
		// ensuring that the last stage in the pipeline will return a single-column
		// row with the row count in it, so just grab that and exit.
		r.resultWriterMu.row.IncrementRowsAffected(r.ctx, n)
		return r.status
	}

	ensureDecodedRow := func() error {
		if r.row == nil {
			r.row = make(tree.Datums, len(row))
		}
		for i, encDatum := range row {
			err := encDatum.EnsureDecoded(r.outputTypes[i], &r.alloc)
			if err != nil {
				return err
			}
			r.row[i] = encDatum.Datum
		}
		return nil
	}

	if r.isTenantExplainAnalyze {
		if err := ensureDecodedRow(); err != nil {
			r.SetError(err)
			return r.status
		}
		if len(r.row) != len(r.outputTypes) {
			r.SetError(errors.Errorf("expected number of columns and output types to be the same"))
			return r.status
		}
		if r.egressCounter == nil {
			r.egressCounter = NewTenantNetworkEgressCounter()
		}
		r.stats.networkEgressEstimate += r.egressCounter.GetRowNetworkEgress(r.ctx, r.row, r.outputTypes)
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
		if err := ensureDecodedRow(); err != nil {
			r.SetError(err)
			return r.status
		}
	}
	r.tracing.TraceExecRowsResult(r.ctx, r.row)
	if commErr := r.resultWriterMu.row.AddRow(r.ctx, r.row); commErr != nil {
		r.handleCommErr(commErr)
	}
	return r.status
}

// PushBatch is part of the execinfra.BatchReceiver interface.
func (r *DistSQLReceiver) PushBatch(
	batch coldata.Batch, meta *execinfrapb.ProducerMetadata,
) execinfra.ConsumerStatus {
	r.checkConcurrentError()
	if meta != nil {
		return r.pushMeta(meta)
	}
	if r.ctx.Err() != nil && r.status != execinfra.ConsumerClosed {
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
		r.resultWriterMu.row.IncrementRowsAffected(r.ctx, int(batch.ColVec(0).Int64()[0]))
		return r.status
	}

	if r.isTenantExplainAnalyze {
		if r.egressCounter == nil {
			r.egressCounter = NewTenantNetworkEgressCounter()
		}
		r.stats.networkEgressEstimate += r.egressCounter.GetBatchNetworkEgress(r.ctx, batch)
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
	if commErr := r.resultWriterMu.batch.AddBatch(r.ctx, batch); commErr != nil {
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

// PlanAndRunAll combines running the the main query, subqueries and cascades/checks.
// If an error is returned, the connection needs to stop processing queries.
// Query execution errors stored in recv; they are not returned.
func (dsp *DistSQLPlanner) PlanAndRunAll(
	ctx context.Context,
	evalCtx *extendedEvalContext,
	planCtx *PlanningCtx,
	planner *planner,
	recv *DistSQLReceiver,
	evalCtxFactory func() *extendedEvalContext,
) error {
	defer planner.curPlan.close(ctx)
	if len(planner.curPlan.subqueryPlans) != 0 {
		// Create a separate memory account for the results of the subqueries.
		// Note that we intentionally defer the closure of the account until we
		// return from this method (after the main query is executed).
		subqueryResultMemAcc := planner.Mon().MakeBoundAccount()
		defer subqueryResultMemAcc.Close(ctx)
		if !dsp.PlanAndRunSubqueries(
			ctx, planner, evalCtxFactory, planner.curPlan.subqueryPlans, recv, &subqueryResultMemAcc,
			// Skip the diagram generation since on this "main" query path we
			// can get it via the statement bundle.
			true, /* skipDistSQLDiagramGeneration */
		) {
			return recv.commErr
		}
	}
	recv.discardRows = planner.instrumentation.ShouldDiscardRows()
	dsp.PlanAndRun(
		ctx, evalCtx, planCtx, planner.txn, planner.curPlan.main, recv, nil, /* finishedSetupFn */
	)
	if recv.commErr != nil || recv.getError() != nil {
		return recv.commErr
	}

	dsp.PlanAndRunCascadesAndChecks(
		ctx, planner, evalCtxFactory, &planner.curPlan.planComponents, recv,
	)

	return recv.commErr
}

// PlanAndRunSubqueries returns false if an error was encountered and sets that
// error in the provided receiver. Note that if false is returned, then this
// function will have closed all the subquery plans because it assumes that the
// caller will not try to run the main plan given that the subqueries'
// evaluation failed.
//   - subqueryResultMemAcc must be a non-nil memory account that the result of
//     subqueries' evaluation will be registered with. It is the caller's
//     responsibility to shrink (or close) the account accordingly, once the
//     references to those results are lost.
func (dsp *DistSQLPlanner) PlanAndRunSubqueries(
	ctx context.Context,
	planner *planner,
	evalCtxFactory func() *extendedEvalContext,
	subqueryPlans []subquery,
	recv *DistSQLReceiver,
	subqueryResultMemAcc *mon.BoundAccount,
	skipDistSQLDiagramGeneration bool,
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
			skipDistSQLDiagramGeneration,
		); err != nil {
			recv.SetError(err)
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
	skipDistSQLDiagramGeneration bool,
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
	subqueryMonitor.StartNoReserved(ctx, evalCtx.Planner.Mon())
	defer subqueryMonitor.Stop(ctx)

	subqueryMemAccount := subqueryMonitor.MakeBoundAccount()
	defer subqueryMemAccount.Close(ctx)

	distributeSubquery := getPlanDistribution(
		ctx, planner, planner.execCfg.NodeInfo.NodeID, planner.SessionData().DistSQLMode, subqueryPlan.plan,
	).WillDistribute()
	distribute := DistributionType(DistributionTypeNone)
	if distributeSubquery {
		distribute = DistributionTypeAlways
	}
	subqueryPlanCtx := dsp.NewPlanningCtx(ctx, evalCtx, planner, planner.txn,
		distribute)
	subqueryPlanCtx.stmtType = tree.Rows
	subqueryPlanCtx.skipDistSQLDiagramGeneration = skipDistSQLDiagramGeneration
	if planner.instrumentation.ShouldSaveFlows() {
		subqueryPlanCtx.saveFlows = subqueryPlanCtx.getDefaultSaveFlowsFunc(ctx, planner, planComponentTypeSubquery)
	}
	subqueryPlanCtx.traceMetadata = planner.instrumentation.traceMetadata
	subqueryPlanCtx.collectExecStats = planner.instrumentation.ShouldCollectExecStats()
	subqueryPhysPlan, physPlanCleanup, err := dsp.createPhysPlan(ctx, subqueryPlanCtx, subqueryPlan.plan)
	defer physPlanCleanup()
	if err != nil {
		return err
	}
	dsp.finalizePlanWithRowCount(subqueryPlanCtx, subqueryPhysPlan, subqueryPlan.rowCount)

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
	rows.Init(ctx, typs, evalCtx, "subquery" /* opName */)
	defer rows.Close(ctx)

	// TODO(yuzefovich): consider implementing batch receiving result writer.
	subqueryRowReceiver := NewRowResultWriter(&rows)
	subqueryRecv.resultWriterMu.row = subqueryRowReceiver
	subqueryPlans[planIdx].started = true
	dsp.Run(ctx, subqueryPlanCtx, planner.txn, subqueryPhysPlan, subqueryRecv, evalCtx, nil /* finishedSetupFn */)
	if err := subqueryRowReceiver.Err(); err != nil {
		return err
	}
	var alreadyAccountedFor int64
	switch subqueryPlan.execMode {
	case rowexec.SubqueryExecModeExists:
		// For EXISTS expressions, all we want to know if there is at least one row.
		hasRows := rows.Len() != 0
		subqueryPlans[planIdx].result = tree.MakeDBool(tree.DBool(hasRows))
	case rowexec.SubqueryExecModeAllRows, rowexec.SubqueryExecModeAllRowsNormalized:
		// TODO(yuzefovich): this is unfortunate - we're materializing all
		// buffered rows into a single tuple kept in memory. Refactor it.
		var result tree.DTuple
		iterator := newRowContainerIterator(ctx, rows, typs)
		defer iterator.Close()
		for {
			row, err := iterator.Next()
			if err != nil {
				return err
			}
			if row == nil {
				break
			}
			var toAppend tree.Datum
			if row.Len() == 1 {
				// This seems hokey, but if we don't do this then the subquery expands
				// to a tuple of tuples instead of a tuple of values and an expression
				// like "k IN (SELECT foo FROM bar)" will fail because we're comparing
				// a single value against a tuple.
				toAppend = row[0]
			} else {
				toAppend = &tree.DTuple{D: row}
			}
			// Perform memory accounting for this datum. We do this in an
			// incremental fashion since we might be materializing a lot of data
			// into a single result tuple, and the memory accounting below might
			// come too late.
			size := int64(toAppend.Size())
			alreadyAccountedFor += size
			if err = subqueryResultMemAcc.Grow(ctx, size); err != nil {
				return err
			}
			result.D = append(result.D, toAppend)
		}

		if subqueryPlan.execMode == rowexec.SubqueryExecModeAllRowsNormalized {
			// During the normalization, we will remove duplicate elements which
			// we've already accounted for. That's ok because below we will
			// reconcile the incremental accounting with the final result's
			// memory footprint.
			result.Normalize(&evalCtx.Context)
		}
		subqueryPlans[planIdx].result = &result
	case rowexec.SubqueryExecModeOneRow:
		switch rows.Len() {
		case 0:
			subqueryPlans[planIdx].result = tree.DNull
		case 1:
			iterator := newRowContainerIterator(ctx, rows, typs)
			defer iterator.Close()
			row, err := iterator.Next()
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
	actualSize := int64(subqueryPlans[planIdx].result.Size())
	if actualSize >= alreadyAccountedFor {
		if err := subqueryResultMemAcc.Grow(ctx, actualSize-alreadyAccountedFor); err != nil {
			return err
		}
	} else {
		// We've accounted for more than the actual result needs. For example,
		// this could occur in rowexec.SubqueryExecModeAllRowsNormalized mode
		// with many duplicate elements.
		subqueryResultMemAcc.Shrink(ctx, alreadyAccountedFor-actualSize)
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
func (dsp *DistSQLPlanner) PlanAndRun(
	ctx context.Context,
	evalCtx *extendedEvalContext,
	planCtx *PlanningCtx,
	txn *kv.Txn,
	plan planMaybePhysical,
	recv *DistSQLReceiver,
	finishedSetupFn func(),
) {
	log.VEventf(ctx, 2, "creating DistSQL plan with isLocal=%v", planCtx.isLocal)

	physPlan, physPlanCleanup, err := dsp.createPhysPlan(ctx, planCtx, plan)
	defer physPlanCleanup()
	if err != nil {
		recv.SetError(err)
		return
	}
	dsp.finalizePlanWithRowCount(planCtx, physPlan, planCtx.planner.curPlan.mainRowCount)
	recv.expectedRowsRead = int64(physPlan.TotalEstimatedScannedRows)
	dsp.Run(ctx, planCtx, txn, physPlan, recv, evalCtx, finishedSetupFn)
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
		execFactory := newExecFactory(ctx, planner)
		// The cascading query is allowed to autocommit only if it is the last
		// cascade and there are no check queries to run.
		allowAutoCommit := planner.autoCommit
		if len(plan.checkPlans) > 0 || i < len(plan.cascades)-1 {
			allowAutoCommit = false
		}
		cascadePlan, err := plan.cascades[i].PlanFn(
			ctx, &planner.semaCtx, &evalCtx.Context, execFactory,
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
		if limit := int(evalCtx.SessionData().OptimizerFKCascadesLimit); len(plan.cascades) > limit {
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
	postqueryMonitor.StartNoReserved(ctx, evalCtx.Planner.Mon())
	defer postqueryMonitor.Stop(ctx)

	postqueryMemAccount := postqueryMonitor.MakeBoundAccount()
	defer postqueryMemAccount.Close(ctx)

	distributePostquery := getPlanDistribution(
		ctx, planner, planner.execCfg.NodeInfo.NodeID, planner.SessionData().DistSQLMode, postqueryPlan,
	).WillDistribute()
	distribute := DistributionType(DistributionTypeNone)
	if distributePostquery {
		distribute = DistributionTypeAlways
	}
	postqueryPlanCtx := dsp.NewPlanningCtx(ctx, evalCtx, planner, planner.txn, distribute)
	postqueryPlanCtx.stmtType = tree.Rows
	// Postqueries are only executed on the main query path where we skip the
	// diagram generation.
	postqueryPlanCtx.skipDistSQLDiagramGeneration = true
	if planner.instrumentation.ShouldSaveFlows() {
		postqueryPlanCtx.saveFlows = postqueryPlanCtx.getDefaultSaveFlowsFunc(ctx, planner, planComponentTypePostquery)
	}
	postqueryPlanCtx.traceMetadata = planner.instrumentation.traceMetadata
	postqueryPlanCtx.collectExecStats = planner.instrumentation.ShouldCollectExecStats()

	postqueryPhysPlan, physPlanCleanup, err := dsp.createPhysPlan(ctx, postqueryPlanCtx, postqueryPlan)
	defer physPlanCleanup()
	if err != nil {
		return err
	}
	dsp.FinalizePlan(postqueryPlanCtx, postqueryPhysPlan)

	postqueryRecv := recv.clone()
	defer postqueryRecv.Release()
	// TODO(yuzefovich): at the moment, errOnlyResultWriter is sufficient here,
	// but it may not be the case when we support cascades through the optimizer.
	postqueryResultWriter := &errOnlyResultWriter{}
	postqueryRecv.resultWriterMu.row = postqueryResultWriter
	postqueryRecv.resultWriterMu.batch = postqueryResultWriter
	dsp.Run(ctx, postqueryPlanCtx, planner.txn, postqueryPhysPlan, postqueryRecv, evalCtx, nil /* finishedSetupFn */)
	return postqueryRecv.getError()
}
