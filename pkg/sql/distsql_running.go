// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangecache"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/colflow"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execopnode"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/execversion"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/ring"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

var settingDistSQLNumRunners = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.distsql.num_runners",
	"determines the number of DistSQL runner goroutines used for issuing SetupFlow RPCs",
	// We use GOMAXPROCS instead of NumCPU because the former could be adjusted
	// based on cgroup limits (see cgroups.AdjustMaxProcs).
	//
	// The choice of the default multiple of 4 was made in order to get the
	// original value of 16 on machines with 4 CPUs.
	4*int64(runtime.GOMAXPROCS(0)), /* defaultValue */
	settings.IntInRange(0, distSQLNumRunnersMax),
)

// Somewhat arbitrary upper bound.
var distSQLNumRunnersMax = 256 * int64(runtime.GOMAXPROCS(0))

// runnerRequest is the request that is sent (via a channel) to a worker.
type runnerRequest struct {
	ctx               context.Context
	sqlInstanceDialer *nodedialer.Dialer
	flowReq           *execinfrapb.SetupFlowRequest
	sqlInstanceID     base.SQLInstanceID
	resultChan        chan<- runnerResult
}

// runnerResult is returned by a worker (via a channel) for each received
// request.
type runnerResult struct {
	nodeID base.SQLInstanceID
	err    error
}

type runnerDialErr struct {
	err error
}

func (e *runnerDialErr) Error() string {
	return e.err.Error()
}

func (e *runnerDialErr) Cause() error {
	return e.err
}

func isDialErr(err error) bool {
	return errors.HasType(err, (*runnerDialErr)(nil))
}

// run executes the request. An error, if encountered, is both sent on the
// result channel and returned.
func (req runnerRequest) run() error {
	res := runnerResult{nodeID: req.sqlInstanceID}
	defer func() {
		req.resultChan <- res
		physicalplan.ReleaseFlowSpec(&req.flowReq.Flow)
	}()

	conn, err := req.sqlInstanceDialer.Dial(req.ctx, roachpb.NodeID(req.sqlInstanceID), rpc.DefaultClass)
	if err != nil {
		// Mark this error as special runnerDialErr so that we could retry this
		// distributed query as local.
		err = &runnerDialErr{err: err}
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
					conn, err := dsp.sqlInstanceDialer.Dial(parentCtx, roachpb.NodeID(sqlInstanceID), rpc.DefaultClass)
					if err != nil {
						// We failed to dial the node, so we give up given that
						// our cancellation is best effort. It is possible that
						// the node is dead anyway.
						continue
					}
					client := execinfrapb.NewDistSQLClient(conn)
					_ = timeutil.RunWithTimeout(
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
	thisNodeSpec, ok := flows[thisNodeID]
	if !ok {
		return nil, nil, errors.AssertionFailedf("missing gateway flow")
	}
	if localState.IsLocal && len(flows) != 1 {
		return nil, nil, errors.AssertionFailedf("IsLocal set but there's multiple flows")
	}

	if buildutil.CrdbTestBuild && len(flows) > 1 {
		// Ensure that we don't have memory aliasing of some fields between the
		// local flow and the remote ones. At the time of writing only
		// TableReaderSpec.Spans is known to be problematic.
		gatewayFlowSpans := make(map[uintptr]int32)
		for _, p := range thisNodeSpec.Processors {
			if tr := p.Core.TableReader; tr != nil {
				//lint:ignore SA1019 SliceHeader is deprecated, but no clear replacement
				ptr := (*reflect.SliceHeader)(unsafe.Pointer(&tr.Spans))
				gatewayFlowSpans[ptr.Data] = p.ProcessorID
			}
		}
		for sqlInstanceID, flow := range flows {
			if sqlInstanceID == thisNodeID {
				continue
			}
			for _, p := range flow.Processors {
				if tr := p.Core.TableReader; tr != nil {
					//lint:ignore SA1019 SliceHeader is deprecated, but no clear replacement
					ptr := (*reflect.SliceHeader)(unsafe.Pointer(&tr.Spans))
					if procID, found := gatewayFlowSpans[ptr.Data]; found {
						return nil, nil, errors.AssertionFailedf(
							"found TableReaderSpec.Spans memory aliasing between local and remote flows\n"+
								"statement: %s, local proc ID %d, remote proc ID %d\n%v",
							statementSQL, procID, p.ProcessorID, flows,
						)
					}
				}
			}
		}
	}

	const setupFlowRequestStmtMaxLength = 500
	if len(statementSQL) > setupFlowRequestStmtMaxLength {
		statementSQL = statementSQL[:setupFlowRequestStmtMaxLength]
	}
	var v execversion.V
	switch {
	case dsp.st.Version.IsActive(ctx, clusterversion.V25_2):
		v = execversion.V25_2
	case dsp.st.Version.IsActive(ctx, clusterversion.V25_1):
		v = execversion.V25_1
	default:
		v = execversion.V24_3
	}
	setupReq := execinfrapb.SetupFlowRequest{
		LeafTxnInputState: leafInputState,
		Version:           v,
		TraceKV:           evalCtx.Tracing.KVTracingEnabled(),
		CollectStats:      planCtx.collectExecStats,
		StatementSQL:      statementSQL,
	}
	if localState.IsLocal {
		// VectorizeMode is the only field that the setup code expects to be set
		// in the local flows.
		setupReq.EvalContext.SessionData.VectorizeMode = evalCtx.SessionData().VectorizeMode
	} else {
		// In distributed plans populate some extra state.
		setupReq.EvalContext = execinfrapb.MakeEvalContext(&evalCtx.Context)
		if jobTag, ok := logtags.FromContext(ctx).GetTag("job"); ok {
			setupReq.JobTag = jobTag.ValueStr()
		}
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
	if !planCtx.subOrPostQuery && planCtx.planner != nil && isVectorized {
		// Only set the vectorized flag for the main query (to be consistent
		// with the 'vectorized' attribute of the EXPLAIN output).
		planCtx.planner.curPlan.flags.Set(planFlagVectorized)
	}

	// First, set up the flow on this node.
	setupReq.Flow = *thisNodeSpec
	var batchReceiver execinfra.BatchReceiver
	if recv.batchWriter != nil {
		// Use the DistSQLReceiver as an execinfra.BatchReceiver only if the
		// former has the corresponding writer set.
		batchReceiver = recv
	}
	origCtx := ctx
	parentMonitor := evalCtx.Planner.Mon()
	if planCtx.OverridePlannerMon != nil {
		parentMonitor = planCtx.OverridePlannerMon
	}
	ctx, flow, opChains, err := dsp.distSQLSrv.SetupLocalSyncFlow(ctx, parentMonitor, &setupReq, recv, batchReceiver, localState)
	if err == nil && planCtx.saveFlows != nil {
		err = planCtx.saveFlows(flows, opChains, planCtx.infra.LocalProcessors, isVectorized)
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
	if sp := tracing.SpanFromContext(origCtx); sp != nil {
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
			ctx:               runnerCtx,
			sqlInstanceDialer: dsp.sqlInstanceDialer,
			flowReq:           &req,
			sqlInstanceID:     nodeID,
			resultChan:        resultChan,
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
	// since flow.Cleanup() is the last thing before DistSQLPlanner.Run returns
	// at which point the rowResultWriter is no longer protected by the mutex of
	// the DistSQLReceiver.
	// TODO(yuzefovich): think through this comment - we no longer have mutex
	// protection in place, so perhaps this can be simplified.
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
					defer cleanupCalledMu.Unlock()
					if cleanupCalledMu.called {
						// Cleanup of the local flow has already been performed,
						// so there is nothing to do.
						return
					}
					// First, we send the error to the DistSQL receiver to be
					// returned to the client eventually (which will happen on
					// the next Push or PushBatch call).
					recv.concurrentErrorCh <- res.err
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
const executingParallelAndSerialChecks = "executing %d checks concurrently and %d checks serially"

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
// - txn is the root transaction in which the plan will run (or will be used to
// derive leaf transactions if the plan has concurrency). If nil, then different
// processors are expected to manage their own internal transactions.
// - evalCtx is the evaluation context in which the plan will run. It might be
// mutated.
// - finishedSetupFn, if non-nil, is called synchronously after all the
// processors have been created but haven't started running yet.
func (dsp *DistSQLPlanner) Run(
	ctx context.Context,
	planCtx *PlanningCtx,
	txn *kv.Txn,
	plan *PhysicalPlan,
	recv *DistSQLReceiver,
	evalCtx *extendedEvalContext,
	finishedSetupFn func(localFlow flowinfra.Flow),
) {
	// Ignore the cleanup function since we will release each spec separately.
	flows, _ := plan.GenerateFlowSpecs()
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
	localState.MustUseLeaf = planCtx.mustUseLeafTxn
	localState.Txn = txn
	localState.LocalProcs = plan.LocalProcessors
	localState.LocalVectorSources = plan.LocalVectorSources
	if planCtx.planner != nil {
		// Note that the planner's collection will only be used for local plans.
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
		for _, flow := range flows {
			localState.HasConcurrency = localState.HasConcurrency || execinfra.HasParallelProcessors(flow)
		}
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

			// At the moment, we disable the usage of the Streamer API for local plans
			// when locking is used by any of the processors. This is the case since
			// the lock spans propagation doesn't happen for the leaf txns which can
			// result in excessive contention for future reads (since the acquired
			// locks are not cleaned up properly when the txn commits).
			// TODO(yuzefovich): fix the propagation of the lock spans with the leaf
			// txns and remove this check. See #94290.
			containsLocking := planCtx.planner != nil && planCtx.planner.curPlan.flags.IsSet(planFlagContainsLocking)

			// We also currently disable the usage of the Streamer API whenever
			// we have a wrapped planNode. This is done to prevent scenarios
			// where some of planNodes will use the RootTxn (via the internal
			// executor) which prohibits the usage of the LeafTxn for this flow.
			//
			// Note that we're disallowing the Streamer API in more cases than
			// strictly necessary (i.e. there are planNodes that don't use the
			// txn at all), but auditing each planNode implementation to see
			// which are using the internal executor is error-prone, so we just
			// disable the Streamer API for the "super-set" of problematic
			// cases.
			mustUseRootTxn := func() bool {
				for _, p := range plan.Processors {
					if p.Spec.Core.LocalPlanNode != nil {
						return true
					}
				}
				return false
			}()
			// We disable the usage of the Streamer API whenever usage of
			// DistSQL was prohibited with an error. The thinking behind it is
			// that we might have a plan where some expression (e.g. a cast to
			// an Oid type) uses the planner's txn (which is the RootTxn), so
			// it'd be illegal to use LeafTxns for a part of such plan.
			// TODO(yuzefovich): this check is both excessive and insufficient.
			// For example:
			// - it disables the usage of the Streamer when a subquery has an
			// Oid type, but that would have no impact on usage of the Streamer
			// in the main query;
			// - it might allow the usage of the Streamer even when the internal
			// executor is used by a part of the plan, and the IE would use the
			// RootTxn. Arguably, this would be a bug in not prohibiting the
			// DistSQL altogether.
			if !containsLocking && !mustUseRootTxn && planCtx.distSQLProhibitedErr == nil {
				if evalCtx.SessionData().StreamerEnabled {
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
			tis, err := txn.GetLeafTxnInputState(ctx)
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
		if planCtx.stmtForDistSQLDiagram != "" {
			stmtStr = planCtx.stmtForDistSQLDiagram
		} else if planCtx.planner != nil && planCtx.planner.stmt.AST != nil {
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

	dsp.distSQLSrv.ServerConfig.Metrics.RunStart(len(flows) > 1 /* distributed */)
	defer dsp.distSQLSrv.ServerConfig.Metrics.RunStop()

	recv.outputTypes = plan.GetResultTypes()
	if execinfra.IncludeRUEstimateInExplainAnalyze.Get(&dsp.st.SV) &&
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
		// Concurrent errors are only possible during the distributed execution.
		recv.skipConcurrentErrorCheck = true
	} else {
		defer func() {
			if recv.resultWriter.Err() != nil {
				// The execution of this query encountered some error, so we
				// will eagerly cancel all flows running on the remote nodes
				// because they are now dead.
				dsp.cancelFlowsCoordinator.addFlowsToCancel(flows)
			}
		}()
		if planCtx.planner != nil {
			planCtx.planner.curPlan.flags.Set(planFlagDistributedExecution)
		}
	}

	// Currently, we get the statement only if there is a planner available in
	// the planCtx which is the case only on the "main" query path (for
	// user-issued queries).
	// TODO(yuzefovich): propagate the statement in all cases.
	var statementSQL string
	if planCtx.planner != nil {
		statementSQL = planCtx.planner.stmt.StmtNoConstants
	}

	var flow flowinfra.Flow
	var err error
	if i := planCtx.getPortalPauseInfo(); i != nil && i.resumableFlow.flow != nil {
		flow = i.resumableFlow.flow
	} else {
		ctx, flow, err = dsp.setupFlows(
			ctx, evalCtx, planCtx, leafInputState, flows, recv, localState, statementSQL,
		)
		if i != nil {
			// TODO(yuzefovich): add a check that this flow runs in a single goroutine.
			i.resumableFlow.flow = flow
			i.resumableFlow.outputTypes = plan.GetResultTypes()
		}
	}

	if flow != nil {
		// Make sure that the local flow is always cleaned up if it was created.
		// If the flow is not for retained portal, we clean the flow up here.
		// Otherwise, we delay the clean up via portalPauseInfo.flowCleanup until
		// the portal is closed.
		if planCtx.getPortalPauseInfo() == nil {
			defer func() {
				flow.Cleanup(ctx)
			}()
		}
	}
	if err != nil {
		recv.SetError(err)
		return
	}

	if finishedSetupFn != nil {
		finishedSetupFn(flow)
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
	if buildutil.CrdbTestBuild && txn != nil && localState.MustUseLeafTxn() && flow.GetFlowCtx().Txn.Type() != kv.LeafTxn {
		recv.SetError(errors.AssertionFailedf("unexpected root txn used when leaf txn expected"))
		return
	}

	noWait := planCtx.getPortalPauseInfo() != nil
	flow.Run(ctx, noWait)
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

	// concurrentErrorCh is a buffered channel that allows for concurrent
	// goroutines to tell the main execution goroutine that there is an error.
	// The main goroutine will read from this channel on the next call to Push
	// or PushBatch.
	//
	// This channel is needed since rowResultWriter is not thread-safe and will
	// only be sent on during distributed plan execution.
	concurrentErrorCh chan error
	// skipConcurrentErrorCheck is set when concurrent errors aren't possible,
	// so there is no need to poll concurrentErrorCh.
	skipConcurrentErrorCheck bool

	status   execinfra.ConsumerStatus
	stmtType tree.StatementReturnType

	// outputTypes are the types of the result columns produced by the plan.
	outputTypes []*types.T

	// existsMode indicates that the caller is only interested in the existence
	// of a single row. Used by subqueries in EXISTS mode.
	existsMode bool

	// discardRows is set when we want to discard rows (for testing/benchmarks).
	// See EXECUTE .. DISCARD ROWS.
	discardRows bool

	// dataPushed is set once at least one row, one batch, or one non-error
	// piece of metadata is pushed to the result writer.
	dataPushed bool

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

	stats topLevelQueryStats

	// isTenantExplainAnalyze is used to indicate that network egress should be
	// collected in order to estimate RU consumption for a tenant that is running
	// a query with EXPLAIN ANALYZE.
	isTenantExplainAnalyze bool

	// If set, client time will be measured (result will be stored in
	// stats.clientTime).
	measureClientTime bool

	egressCounter TenantNetworkEgressCounter

	expectedRowsRead int64
	progressAtomic   *uint64

	testingKnobs struct {
		// pushCallback, if set, will be called every time DistSQLReceiver.Push
		// or DistSQLReceiver.PushBatch is called, with the same arguments.
		// Possibly updated arguments are returned.
		pushCallback func(rowenc.EncDatumRow, coldata.Batch, *execinfrapb.ProducerMetadata) (rowenc.EncDatumRow, coldata.Batch, *execinfrapb.ProducerMetadata)
	}
}

// rowResultWriter is a subset of CommandResult to be used with the
// DistSQLReceiver. It's implemented by RowResultWriter.
type rowResultWriter interface {
	// AddRow writes a result row.
	// Note that the caller owns the row slice and might reuse it.
	AddRow(ctx context.Context, row tree.Datums) error
	SetRowsAffected(ctx context.Context, n int)
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

func (w *errOnlyResultWriter) SetRowsAffected(ctx context.Context, n int) {
	panic("SetRowsAffected not supported by errOnlyResultWriter")
}

// droppingResultWriter drops all rows that are added to it. It only tracks
// errors with the SetError and Err functions.
type droppingResultWriter struct {
	err error
}

var _ rowResultWriter = &droppingResultWriter{}
var _ batchResultWriter = &droppingResultWriter{}

// AddRow is part of the rowResultWriter interface.
func (d *droppingResultWriter) AddRow(ctx context.Context, row tree.Datums) error {
	return nil
}

func (d *droppingResultWriter) AddBatch(ctx context.Context, batch coldata.Batch) error {
	return nil
}

// SetRowsAffected is part of the rowResultWriter interface.
func (d *droppingResultWriter) SetRowsAffected(ctx context.Context, n int) {}

// SetError is part of the rowResultWriter interface.
func (d *droppingResultWriter) SetError(err error) {
	d.err = err
}

// Err is part of the rowResultWriter interface.
func (d *droppingResultWriter) Err() error {
	return d.err
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

// SetRowsAffected implements the rowResultWriter interface.
func (b *RowResultWriter) SetRowsAffected(ctx context.Context, n int) {
	b.rowsAffected = n
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

// SetRowsAffected is part of the rowResultWriter interface.
func (c *CallbackResultWriter) SetRowsAffected(ctx context.Context, n int) {
	c.rowsAffected = n
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
		resultWriter: resultWriter,
		batchWriter:  batchWriter,
		// At the time of writing, there is only one concurrent goroutine that
		// might send at most one error.
		concurrentErrorCh: make(chan error, 1),
		cleanup:           cleanup,
		rangeCache:        rangeCache,
		txn:               txn,
		clockUpdater:      clockUpdater,
		stmtType:          stmtType,
		tracing:           tracing,
	}
	return r
}

// resetForLocalRerun prepares the DistSQLReceiver to be used again for
// executing the plan - that encountered an error when run in the distributed
// fashion - locally.
func (r *DistSQLReceiver) resetForLocalRerun(stats topLevelQueryStats) {
	r.resultWriter.SetError(nil)
	// Note that DistSQLPlanner.Run will also set r.skipConcurrentErrorCheck to
	// true, so r.concurrentErrorCh won't be pulled during the local rerun. Here
	// we do so for clarity.
	r.skipConcurrentErrorCheck = true
	r.status = execinfra.NeedMoreRows
	r.dataPushed = false
	r.closed = false
	r.stats = stats
	r.egressCounter = nil
	if r.progressAtomic != nil {
		atomic.StoreUint64(r.progressAtomic, math.Float64bits(0))
	}
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
		ctx:               r.ctx,
		concurrentErrorCh: make(chan error, 1),
		cleanup:           func() {},
		rangeCache:        r.rangeCache,
		txn:               r.txn,
		clockUpdater:      r.clockUpdater,
		stmtType:          tree.Rows,
		tracing:           r.tracing,
	}
	return ret
}

// SetError passes the given error to the resultWriter and updates the status of
// the DistSQLReceiver accordingly.
//
// Note that this method is **not** concurrency safe and needs to be called only
// from the main execution goroutine.
func (r *DistSQLReceiver) SetError(err error) {
	// Check if the error we just received should take precedence over a
	// previous error (if any).
	if kvpb.ErrPriority(err) > kvpb.ErrPriority(r.resultWriter.Err()) {
		if r.txn != nil {
			if retryErr := (*kvpb.UnhandledRetryableError)(nil); errors.As(err, &retryErr) {
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
		r.resultWriter.SetError(err)
		// If we encountered an error, we will transition to draining unless we
		// were canceled.
		if r.ctx.Err() != nil {
			log.VEventf(r.ctx, 1, "encountered error (transitioning to shutting down): %v", r.ctx.Err())
			r.status = execinfra.ConsumerClosed
		} else {
			log.VEventf(r.ctx, 1, "encountered error (transitioning to draining): %v", err)
			r.status = execinfra.DrainRequested
		}
	}
}

// checkConcurrentError checks whether a concurrent goroutine encountered an
// error that should trigger the shutdown of the execution pipeline. The status
// of the DistSQLReceiver is updated accordingly.
func (r *DistSQLReceiver) checkConcurrentError() {
	if r.skipConcurrentErrorCheck || r.status != execinfra.NeedMoreRows {
		// If the status already is not NeedMoreRows, then it doesn't matter if
		// there was a concurrent error set.
		if buildutil.CrdbTestBuild {
			// SwitchToAnotherPortal is only reachable with local execution when
			// skipConcurrentErrorCheck should be set to true.
			if !r.skipConcurrentErrorCheck && r.status == execinfra.SwitchToAnotherPortal {
				r.SetError(errors.AssertionFailedf("DistSQLReceiver's status is SwitchToAnotherPortal when skipConcurrentErrorCheck is false"))
			}
		}
		return
	}
	select {
	case err := <-r.concurrentErrorCh:
		r.SetError(err)
	default:
	}
}

// pushMeta takes in non-empty metadata object and pushes it to the result
// writer. Possibly updated status is returned.
func (r *DistSQLReceiver) pushMeta(meta *execinfrapb.ProducerMetadata) execinfra.ConsumerStatus {
	if metaWriter, ok := r.resultWriter.(MetadataResultWriter); ok {
		metaWriter.AddMeta(r.ctx, meta)
		if meta.Err == nil {
			r.dataPushed = true
		}
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
	} else if errors.Is(commErr, ErrPortalLimitHasBeenReached) {
		r.status = execinfra.SwitchToAnotherPortal
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
		if !errors.Is(commErr, ErrLimitedResultNotSupported) && !errors.Is(commErr, ErrStmtNotSupportedForPausablePortal) {
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
		row, _, meta = r.testingKnobs.pushCallback(row, nil /* batch */, meta)
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
		r.resultWriter.SetRowsAffected(r.ctx, n)
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
	if r.measureClientTime {
		defer func(start time.Time) {
			r.stats.clientTime += timeutil.Since(start)
		}(timeutil.Now())
	}
	if commErr := r.resultWriter.AddRow(r.ctx, r.row); commErr != nil {
		r.handleCommErr(commErr)
	}
	r.dataPushed = true
	return r.status
}

// PushBatch is part of the execinfra.BatchReceiver interface.
func (r *DistSQLReceiver) PushBatch(
	batch coldata.Batch, meta *execinfrapb.ProducerMetadata,
) execinfra.ConsumerStatus {
	r.checkConcurrentError()
	if r.testingKnobs.pushCallback != nil {
		_, batch, meta = r.testingKnobs.pushCallback(nil /* row */, batch, meta)
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

	if batch.Length() == 0 {
		// Nothing to do on the zero-length batch.
		return r.status
	}

	if r.stmtType != tree.Rows {
		// We only need the row count. planNodeToRowSource is set up to handle
		// ensuring that the last stage in the pipeline will return a single-column
		// row with the row count in it, so just grab that and exit.
		r.resultWriter.SetRowsAffected(r.ctx, int(batch.ColVec(0).Int64()[0]))
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
	if r.measureClientTime {
		defer func(start time.Time) {
			r.stats.clientTime += timeutil.Since(start)
		}(timeutil.Now())
	}
	if commErr := r.batchWriter.AddBatch(r.ctx, batch); commErr != nil {
		r.handleCommErr(commErr)
	}
	r.dataPushed = true
	return r.status
}

var (
	// ErrLimitedResultNotSupported is an error produced by pgwire
	// indicating the user attempted to have multiple active portals but
	// either without setting session variable multiple_active_portals_enabled to
	// true or the underlying query does not satisfy the restriction.
	ErrLimitedResultNotSupported = unimplemented.NewWithIssue(
		40195,
		"multiple active portals is in preview, "+
			"please set session variable multiple_active_portals_enabled to true to enable them",
	)
	// ErrStmtNotSupportedForPausablePortal is returned when the user have set
	// session variable multiple_active_portals_enabled to true but set an unsupported
	// statement for a portal.
	ErrStmtNotSupportedForPausablePortal = unimplemented.NewWithIssue(
		98911,
		"the statement for a pausable portal must be a read-only SELECT query"+
			" with no sub-queries or post-queries",
	)
	// ErrLimitedResultClosed is a sentinel error produced by pgwire
	// indicating the portal should be closed without error.
	ErrLimitedResultClosed       = errors.New("row count limit closed")
	ErrPortalLimitHasBeenReached = errors.New("limit has been reached")
)

// ProducerDone is part of the execinfra.RowReceiver interface.
func (r *DistSQLReceiver) ProducerDone() {
	if r.closed {
		panic("double close")
	}
	r.closed = true
}

// getFinishedSetupFn returns a function to be passed into
// DistSQLPlanner.PlanAndRun or DistSQLPlanner.Run when running an "outer" plan
// that might create "inner" plans (e.g. apply join iterations). The returned
// function updates the passed-in planner to make sure that the "inner" plans
// use the LeafTxns if the "outer" plan happens to have concurrency. It also
// returns a non-nil cleanup function that must be called once all plans (the
// "outer" as well as all "inner" ones) are done. The returned functions should
// only be called once.
func getFinishedSetupFn(planner *planner) (finishedSetupFn func(flowinfra.Flow), cleanup func()) {
	finishedSetupFn = func(localFlow flowinfra.Flow) {
		if localFlow.GetFlowCtx().Txn.Type() == kv.LeafTxn {
			atomic.AddInt32(&planner.atomic.innerPlansMustUseLeafTxn, 1)
		}
	}
	cleanup = func() {
		atomic.AddInt32(&planner.atomic.innerPlansMustUseLeafTxn, -1)
	}
	return finishedSetupFn, cleanup
}

// PlanAndRunAll combines running the main query, subqueries and cascades/checks.
// If an error is returned, the connection needs to stop processing queries.
// Query execution errors stored in recv; they are not returned.
// NB: the plan (in planner.curPlan) is not closed, so it is the caller's
// responsibility to do so.
func (dsp *DistSQLPlanner) PlanAndRunAll(
	ctx context.Context,
	evalCtx *extendedEvalContext,
	planCtx *PlanningCtx,
	planner *planner,
	recv *DistSQLReceiver,
	evalCtxFactory func(usedConcurrently bool) *extendedEvalContext,
) (retErr error) {
	defer func() {
		if ppInfo := planCtx.getPortalPauseInfo(); ppInfo != nil && !ppInfo.resumableFlow.cleanup.isComplete {
			ppInfo.resumableFlow.cleanup.isComplete = true
		}
		if retErr != nil && planCtx.getPortalPauseInfo() != nil {
			planCtx.getPortalPauseInfo().resumableFlow.cleanup.run(ctx)
		}
	}()
	if len(planner.curPlan.subqueryPlans) != 0 {
		// Create a separate memory account for the results of the subqueries.
		// Note that we intentionally defer the closure of the account until we
		// return from this method (after the main query is executed).
		subqueryResultMemAcc := planner.Mon().MakeBoundAccount()
		defer subqueryResultMemAcc.Close(ctx)
		if !dsp.PlanAndRunSubqueries(
			ctx,
			planner,
			func() *extendedEvalContext { return evalCtxFactory(false /* usedConcurrently */) },
			planner.curPlan.subqueryPlans,
			recv,
			&subqueryResultMemAcc,
			// Skip the diagram generation since on this "main" query path we
			// can get it via the statement bundle.
			true,  /* skipDistSQLDiagramGeneration */
			false, /* mustUseLeafTxn */
		) {
			return recv.commErr
		}
	}
	recv.discardRows = planner.instrumentation.ShouldDiscardRows()
	func() {
		finishedSetupFn, cleanup := getFinishedSetupFn(planner)
		defer cleanup()
		dsp.PlanAndRun(
			ctx, evalCtx, planCtx, planner.txn, planner.curPlan.main, recv, finishedSetupFn,
		)
	}()

	if p := planCtx.getPortalPauseInfo(); p != nil {
		if buildutil.CrdbTestBuild && p.resumableFlow.flow == nil {
			checkErr := errors.AssertionFailedf("flow for portal %s cannot be found", planner.pausablePortal.Name)
			if recv.commErr != nil {
				recv.commErr = errors.CombineErrors(recv.commErr, checkErr)
			} else {
				return checkErr
			}
		}
		if !p.resumableFlow.cleanup.isComplete {
			p.resumableFlow.cleanup.appendFunc(func(ctx context.Context) {
				p.resumableFlow.flow.Cleanup(ctx)
			})
		}
	}

	if recv.commErr != nil || recv.resultWriter.Err() != nil {
		return recv.commErr
	}

	if knobs := evalCtx.ExecCfg.DistSQLRunTestingKnobs; knobs != nil {
		if fn := knobs.RunBeforeCascadesAndChecks; fn != nil {
			fn(planner.Txn().ID())
		}
	}

	dsp.PlanAndRunPostQueries(
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
	mustUseLeafTxn bool,
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
			mustUseLeafTxn,
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
	mustUseLeafTxn bool,
) error {
	subqueryDistribution, distSQLProhibitedErr := getPlanDistribution(
		ctx, planner.Descriptors().HasUncommittedTypes(),
		planner.SessionData(), subqueryPlan.plan, &planner.distSQLVisitor,
	)
	distribute := DistributionType(LocalDistribution)
	if subqueryDistribution.WillDistribute() {
		distribute = FullDistribution
	}
	subqueryPlanCtx := dsp.NewPlanningCtx(ctx, evalCtx, planner, planner.txn, distribute)
	subqueryPlanCtx.distSQLProhibitedErr = distSQLProhibitedErr
	subqueryPlanCtx.stmtType = tree.Rows
	subqueryPlanCtx.skipDistSQLDiagramGeneration = skipDistSQLDiagramGeneration
	subqueryPlanCtx.subOrPostQuery = true
	subqueryPlanCtx.mustUseLeafTxn = mustUseLeafTxn
	if planner.instrumentation.ShouldSaveFlows() {
		subqueryPlanCtx.saveFlows = getDefaultSaveFlowsFunc(ctx, planner, planComponentTypeSubquery)
	}
	subqueryPlanCtx.associateNodeWithComponents = planner.instrumentation.getAssociateNodeWithComponentsFn()
	subqueryPlanCtx.collectExecStats = planner.instrumentation.ShouldCollectExecStats()
	subqueryPhysPlan, physPlanCleanup, err := dsp.createPhysPlan(ctx, subqueryPlanCtx, subqueryPlan.plan)
	defer physPlanCleanup()
	if err != nil {
		return err
	}
	finalizePlanWithRowCount(ctx, subqueryPlanCtx, subqueryPhysPlan, subqueryPlan.rowCount)

	// TODO(arjun): #28264: We set up a row container, wrap it in a row
	// receiver, and use it and serialize the results of the subquery. The type
	// of the results stored in the container depends on the type of the subquery.
	subqueryRecv := recv.clone()
	defer subqueryRecv.Release()
	defer recv.stats.add(&subqueryRecv.stats)
	var typs []*types.T
	switch subqueryPlan.execMode {
	case rowexec.SubqueryExecModeExists:
		subqueryRecv.existsMode = true
		typs = []*types.T{}
	case rowexec.SubqueryExecModeDiscardAllRows:
	default:
		typs = subqueryPhysPlan.GetResultTypes()
	}
	var rows rowContainerHelper
	if subqueryPlan.execMode != rowexec.SubqueryExecModeDiscardAllRows {
		rows.Init(ctx, typs, evalCtx, "subquery" /* opName */)
		defer rows.Close(ctx)
	}

	// TODO(yuzefovich): consider implementing batch receiving result writer.
	var subqueryRowReceiver rowResultWriter
	if subqueryPlan.execMode == rowexec.SubqueryExecModeDiscardAllRows {
		// Use a row receiver that ignores all results except for errors.
		subqueryRowReceiver = &droppingResultWriter{}
	} else {
		subqueryRowReceiver = NewRowResultWriter(&rows)
	}
	subqueryRecv.resultWriter = subqueryRowReceiver
	subqueryPlans[planIdx].started = true
	finishedSetupFn, cleanup := getFinishedSetupFn(planner)
	defer cleanup()
	dsp.Run(ctx, subqueryPlanCtx, planner.txn, subqueryPhysPlan, subqueryRecv, evalCtx, finishedSetupFn)
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
		iterator := newRowContainerIterator(ctx, rows)
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
				toAppend = planner.datumAlloc.NewDTuple(tree.DTuple{D: row})
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
			result.Normalize(ctx, &evalCtx.Context)
		}
		subqueryPlans[planIdx].result = &result
	case rowexec.SubqueryExecModeOneRow:
		switch rows.Len() {
		case 0:
			subqueryPlans[planIdx].result = tree.DNull
		case 1:
			iterator := newRowContainerIterator(ctx, rows)
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
	case rowexec.SubqueryExecModeDiscardAllRows:
		subqueryPlans[planIdx].result = tree.DNull
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

var distributedQueryRerunAsLocalEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.distsql.distributed_query_rerun_locally.enabled",
	"determines whether the distributed plans can be rerun locally for some errors",
	true,
)

// PlanAndRun generates a physical plan from a planNode tree and executes it. It
// assumes that the tree is supported (see checkSupportForPlanNode).
//
// All errors encountered are reported to the DistSQLReceiver's resultWriter.
// Additionally, if the error is a "communication error" (an error encountered
// while using that resultWriter), the error is also stored in
// DistSQLReceiver.commErr. That can be tested to see if a client session needs
// to be closed.
//
// An allow-list of errors that are encountered during the distributed query
// execution are transparently retried by re-planning and re-running the query
// as local (as long as no data has been communicated to the result writer).
//
// - finishedSetupFn, if non-nil, is called synchronously after all the local
// processors have been created but haven't started running yet. If the query is
// re-planned as local after having encountered an error during distributed
// execution, then finishedSetupFn will be called twice.
func (dsp *DistSQLPlanner) PlanAndRun(
	ctx context.Context,
	evalCtx *extendedEvalContext,
	planCtx *PlanningCtx,
	txn *kv.Txn,
	plan planMaybePhysical,
	recv *DistSQLReceiver,
	finishedSetupFn func(localFlow flowinfra.Flow),
) {
	log.VEventf(ctx, 2, "creating DistSQL plan with isLocal=%v", planCtx.isLocal)
	// Copy query-level stats before executing this plan in case we need to
	// re-run it as local.
	subqueriesStats := recv.stats
	physPlan, physPlanCleanup, err := dsp.createPhysPlan(ctx, planCtx, plan)
	defer physPlanCleanup()
	if err != nil {
		recv.SetError(err)
	} else {
		finalizePlanWithRowCount(ctx, planCtx, physPlan, planCtx.planner.curPlan.mainRowCount)
		recv.expectedRowsRead = int64(physPlan.TotalEstimatedScannedRows)
		dsp.Run(ctx, planCtx, txn, physPlan, recv, evalCtx, finishedSetupFn)
	}
	if planCtx.isLocal {
		// If the plan was local, then we're done regardless of whether an error
		// was encountered or not.
		return
	}
	if distributedErr := recv.resultWriter.Err(); distributedErr != nil &&
		distributedQueryRerunAsLocalEnabled.Get(&dsp.st.SV) {
		// If we had a distributed plan which resulted in an error, we want to
		// retry this query as local in some cases. In particular, this retry
		// mechanism allows us to hide transient network problems, and - more
		// importantly - in the multi-tenant model it allows us to go around the
		// problem when "not ready" SQL instance is being used for DistSQL
		// planning (e.g. the instance might have been brought down, but the
		// cache on top of the system table hasn't been updated accordingly).
		//
		// The rationale for why it is ok to do so is that we'll create
		// brand-new processors that aren't affiliated to the distributed plan
		// that was just cleaned up. It's worth mentioning that the planNode
		// tree couldn't have been reused in this way, but if we needed to
		// execute any planNodes directly, then we would have to run such a plan
		// in a local fashion. In other words, the fact that we had a
		// distributed plan initially guarantees that we don't have any
		// planNodes to be concerned about.
		// TODO(yuzefovich): consider introducing this retry mechanism to sub-
		// and post-queries too.
		if recv.dataPushed || plan.isPhysicalPlan() {
			// If some data has already been pushed to the result writer, we
			// cannot retry. Also, we cannot re-plan locally if we used the
			// experimental DistSQL spec planning factory.
			return
		}
		if recv.commErr != nil || ctx.Err() != nil {
			// For communication errors, we don't try to rerun the query since
			// the connection is toast. We also give up if the context
			// cancellation has already occurred.
			return
		}
		if !sqlerrors.IsDistSQLRetryableError(distributedErr) &&
			!pgerror.IsSQLRetryableError(distributedErr) &&
			!flowinfra.IsFlowRetryableError(distributedErr) &&
			!isDialErr(distributedErr) {
			// Only re-run the query if we think there is a high chance of a
			// successful local execution.
			return
		}
		log.VEventf(ctx, 1, "encountered an error when running the distributed plan, re-running it as local: %v", distributedErr)
		recv.resetForLocalRerun(subqueriesStats)
		telemetry.Inc(sqltelemetry.DistributedErrorLocalRetryAttempt)
		dsp.distSQLSrv.ServerConfig.Metrics.DistErrorLocalRetryAttempts.Inc(1)
		defer func() {
			if recv.resultWriter.Err() != nil {
				telemetry.Inc(sqltelemetry.DistributedErrorLocalRetryFailure)
				dsp.distSQLSrv.ServerConfig.Metrics.DistErrorLocalRetryFailures.Inc(1)
			}
		}()
		// Note that since we're going to execute the query locally now, there
		// is no point in providing the locality filter since it will be ignored
		// anyway, so we don't use NewPlanningCtxWithOracle constructor.
		localPlanCtx := dsp.NewPlanningCtx(
			ctx, evalCtx, planCtx.planner, evalCtx.Txn, LocalDistribution,
		)
		localPlanCtx.setUpForMainQuery(ctx, planCtx.planner, recv)
		localPhysPlan, localPhysPlanCleanup, err := dsp.createPhysPlan(ctx, localPlanCtx, plan)
		defer localPhysPlanCleanup()
		if err != nil {
			recv.SetError(err)
			return
		}
		finalizePlanWithRowCount(ctx, localPlanCtx, localPhysPlan, localPlanCtx.planner.curPlan.mainRowCount)
		recv.expectedRowsRead = int64(localPhysPlan.TotalEstimatedScannedRows)
		// We already called finishedSetupFn in the previous call to Run, since we
		// only got here if we got a distributed error, not an error during setup.
		dsp.Run(ctx, localPlanCtx, txn, localPhysPlan, recv, evalCtx, nil /* finishedSetupFn */)
	}
}

// PlanAndRunPostQueries runs any cascade, check, and trigger queries.
//
// Because cascades and triggers can themselves generate more cascades, check,
// or trigger queries, this method can append to plan.cascades, plan.checkPlans,
// and plan.triggers (and all these plans must be closed later).
//
// Returns false if an error was encountered and sets that error in the provided
// receiver.
func (dsp *DistSQLPlanner) PlanAndRunPostQueries(
	ctx context.Context,
	planner *planner,
	evalCtxFactory func(usedConcurrently bool) *extendedEvalContext,
	plan *planComponents,
	recv *DistSQLReceiver,
) bool {
	if len(plan.cascades) == 0 && len(plan.checkPlans) == 0 && len(plan.triggers) == 0 {
		return false
	}

	prevSteppingMode := planner.Txn().ConfigureStepping(ctx, kv.SteppingEnabled)
	defer func() { _ = planner.Txn().ConfigureStepping(ctx, prevSteppingMode) }()

	defaultGetSaveFlowsFunc := func() SaveFlowsFunc {
		return getDefaultSaveFlowsFunc(ctx, planner, planComponentTypePostquery)
	}

	checksContainLocking := planner.curPlan.flags.IsSet(planFlagCheckContainsLocking)

	// We treat plan.cascades, plan.checkPlans, and plan.triggers as queues.
	// Cascades and triggers can queue more cascades, checks, and triggers.
	// Cascades are executed first, then checks, and then triggers. Then, the
	// cycle begins again with newly queued post-queries.
	var cascadesIdx, checksIdx, triggersIdx int
	for cascadesIdx < len(plan.cascades) || checksIdx < len(plan.checkPlans) || triggersIdx < len(plan.triggers) {
		// Run cascades first.
		numCascades := len(plan.cascades)
		for ; cascadesIdx < numCascades; cascadesIdx++ {
			hasBuffer, numBufferedRows := checkPostQueryBuffer(plan.cascades[cascadesIdx])
			if hasBuffer && numBufferedRows == 0 {
				// No rows were actually modified.
				continue
			}

			log.VEventf(ctx, 2, "executing cascade for constraint %s",
				plan.cascades[cascadesIdx].FKConstraint.Name())

			// We place a sequence point before every cascade, so that each subsequent
			// cascade can observe the writes by the previous step. However, The
			// external read timestamp is not allowed to advance, since the checks are
			// run as part of the same statement as the corresponding mutations.
			if err := planner.Txn().Step(ctx, false /* allowReadTimestampStep */); err != nil {
				recv.SetError(err)
				return false
			}

			// The cascading query is allowed to autocommit only if it is the last
			// cascade and there are no check queries to run.
			//
			// Note that even if it's the last cascade, we still might not be able
			// to autocommit in case there are more checks to run during or after
			// this cascade. Such scenario is handled in the execbuilder where we
			// need to explicitly enable autocommit on each mutation planNode. In
			// other words, allowAutoCommit = true here means that the plan _might_
			// autocommit but doesn't guarantee that.
			allowAutoCommit := planner.autoCommit
			if len(plan.checkPlans) > 0 || cascadesIdx < len(plan.cascades)-1 {
				allowAutoCommit = false
			}
			evalCtx := evalCtxFactory(false /* usedConcurrently */)
			ok, newChecksContainLocking := dsp.planAndRunCascadeOrTrigger(
				ctx, planner, evalCtx, plan, recv, allowAutoCommit, numBufferedRows,
				&plan.cascades[cascadesIdx], defaultGetSaveFlowsFunc,
			)
			if !ok {
				return false
			}
			checksContainLocking = checksContainLocking || newChecksContainLocking
		}

		if len(plan.triggers) == 0 && cascadesIdx < len(plan.cascades) {
			// If there are more (newly queued) cascades to run and no triggers, execute
			// the cascades before any checks. This is an optimization to batch the
			// execution of checks.
			//
			// We don't do this when there are triggers to ensure that the correct
			// order of mutations is observed.
			continue
		}

		// Next, run checks.
		if checksIdx < len(plan.checkPlans) {
			// We place a sequence point before the checks, so that they observe the
			// writes of the main query and/or any cascades. However, The external
			// read timestamp is not allowed to advance, since the checks are run as
			// part of the same statement as the corresponding mutations.
			if err := planner.Txn().Step(ctx, false /* allowReadTimestampStep */); err != nil {
				recv.SetError(err)
				return false
			}

			// We'll run the checks in parallel if the parallelization is enabled, we have
			// multiple checks to run, none of the checks have non-default locking, and
			// we're likely to have quota to do so.
			runParallelChecks := parallelizeChecks.Get(&dsp.st.SV) &&
				(len(plan.checkPlans)-checksIdx) > 1 && !checksContainLocking &&
				dsp.parallelChecksSem.ApproximateQuota() > 0
			if runParallelChecks {
				// At the moment, we rely on not using the newer DistSQL spec factory to
				// enable parallelization.
				// TODO(yuzefovich): the planObserver logic in
				// planAndRunChecksInParallel will need to be adjusted when we switch to
				// using the DistSQL spec factory.
				for i := checksIdx; i < len(plan.checkPlans); i++ {
					if plan.checkPlans[i].plan.isPhysicalPlan() {
						runParallelChecks = false
						break
					}
				}
			}
			if runParallelChecks {
				checksToRun := plan.checkPlans[checksIdx:]
				if err := dsp.planAndRunChecksInParallel(ctx, checksToRun, planner, evalCtxFactory, recv); err != nil {
					recv.SetError(err)
					return false
				}
			} else {
				if (len(plan.checkPlans) - checksIdx) > 1 {
					log.VEventf(ctx, 2, "executing %d checks serially", len(plan.checkPlans))
				}
				for i := checksIdx; i < len(plan.checkPlans); i++ {
					log.VEventf(ctx, 2, "executing check query %d out of %d", i+1, len(plan.checkPlans))
					if err := dsp.planAndRunPostquery(
						ctx,
						plan.checkPlans[i].plan,
						planner,
						evalCtxFactory(false /* usedConcurrently */),
						recv,
						false, /* parallelCheck */
						defaultGetSaveFlowsFunc,
						planner.instrumentation.getAssociateNodeWithComponentsFn(),
						recv.stats.add,
					); err != nil {
						recv.SetError(err)
						return false
					}
				}
			}
			// Now that the current batch of checks has been run, reset
			// checksContainLocking for the next batch (if any).
			checksContainLocking = false
			checksIdx = len(plan.checkPlans)
		}

		// Finally, run triggers.
		numTriggers := len(plan.triggers)
		for ; triggersIdx < numTriggers; triggersIdx++ {
			trigger := &plan.triggers[triggersIdx]
			hasBuffer, numBufferedRows := checkPostQueryBuffer(plan.triggers[triggersIdx])
			if hasBuffer && numBufferedRows == 0 {
				// No rows were actually modified.
				continue
			}
			if log.ExpensiveLogEnabled(ctx, 2) {
				names := make([]string, len(trigger.Triggers))
				for j := range trigger.Triggers {
					names[j] = string(trigger.Triggers[j].Name())
				}
				log.VEventf(ctx, 2, "executing triggers %s",
					strings.Join(names, ","))
			}
			const allowAutoCommit = false
			evalCtx := evalCtxFactory(false /* usedConcurrently */)
			ok, newChecksContainLocking := dsp.planAndRunCascadeOrTrigger(
				ctx, planner, evalCtx, plan, recv, allowAutoCommit, numBufferedRows,
				&plan.triggers[triggersIdx], defaultGetSaveFlowsFunc,
			)
			if !ok {
				return false
			}
			checksContainLocking = checksContainLocking || newChecksContainLocking
		}
	}
	return true
}

// checkPostQueryBuffer checks whether the given post-query has an input buffer
// node, and returns the number of buffered rows.
func checkPostQueryBuffer(postQuery postQueryMetadata) (hasBuffer bool, numBufferedRows int) {
	// The original bufferNode is stored in c.Buffer; we can refer to it
	// directly.
	// TODO(radu): this requires keeping all previous plans "alive" until the
	// very end. We may want to make copies of the buffer nodes and clean up
	// everything else.
	buf := postQuery.Buffer
	if buf == nil {
		return false, 0
	}
	return true, buf.(*bufferNode).rows.rows.Len()
}

// planAndRunCascadeOrTrigger builds the plan for a cascade or trigger query and
// runs it. It returns false if an error was encountered and sets that error in
// the provided receiver. It also returns checksContainLocking, which is true if
// any new checks were generated that will take locks.
//
// NOTE: planAndRunCascadeOrTrigger has the following side effects:
//   - It may append new cascades, checks, and triggers to "plan".
//   - It will populate the built plan in the given postQueryMetadata.
func (dsp *DistSQLPlanner) planAndRunCascadeOrTrigger(
	ctx context.Context,
	planner *planner,
	evalCtx *extendedEvalContext,
	plan *planComponents,
	recv *DistSQLReceiver,
	allowAutoCommit bool,
	numBufferedRows int,
	pq *postQueryMetadata,
	defaultGetSaveFlowsFunc func() SaveFlowsFunc,
) (ok, checksContainLocking bool) {
	execFactory := newExecFactory(ctx, planner)
	cascadePlan, err := pq.PlanFn(
		ctx, &planner.semaCtx, &evalCtx.Context, execFactory,
		pq.Buffer, numBufferedRows, allowAutoCommit,
	)
	if err != nil {
		recv.SetError(err)
		return false, false
	}
	cp := cascadePlan.(*planComponents)
	pq.plan = cp.main
	if len(cp.subqueryPlans) > 0 {
		recv.SetError(errors.AssertionFailedf("post-query should not have subqueries"))
		return false, false
	}
	checksContainLocking = cp.flags.IsSet(planFlagCheckContainsLocking)

	// add any newly generated post-queries to the queue.
	addPostQueriesFromPlan(evalCtx, recv, cp, plan)

	if err := dsp.planAndRunPostquery(
		ctx,
		cp.main,
		planner,
		evalCtx,
		recv,
		false, /* parallelCheck */
		defaultGetSaveFlowsFunc,
		planner.instrumentation.getAssociateNodeWithComponentsFn(),
		recv.stats.add,
	); err != nil {
		recv.SetError(err)
		return false, false
	}
	return true, checksContainLocking
}

// addPostQueriesFromPlan queues any cascades, checks, and triggers from the
// given "fromPlan" to the given "toPlan". It returns false if an error was
// encountered and sets that error in the provided receiver, otherwise true.
func addPostQueriesFromPlan(
	evalCtx *extendedEvalContext, recv *DistSQLReceiver, fromPlan, toPlan *planComponents,
) bool {

	// Collect any new checks.
	if len(fromPlan.checkPlans) > 0 {
		toPlan.checkPlans = append(toPlan.checkPlans, fromPlan.checkPlans...)
	}

	// Queue any new cascades.
	if len(fromPlan.cascades) > 0 {
		toPlan.cascades = append(toPlan.cascades, fromPlan.cascades...)
	}

	// Queue any new triggers.
	if len(fromPlan.triggers) > 0 {
		toPlan.triggers = append(toPlan.triggers, fromPlan.triggers...)
	}

	// In cyclical reference situations, the number of cascading operations can
	// be arbitrarily large. To avoid OOM, we enforce a limit. This is also a
	// safeguard in case we have a bug that results in an infinite cascade loop.
	if limit := int(evalCtx.SessionData().OptimizerFKCascadesLimit); len(toPlan.cascades) > limit {
		telemetry.Inc(sqltelemetry.CascadesLimitReached)
		err := pgerror.Newf(pgcode.TriggeredActionException, "cascades limit (%d) reached", limit)
		recv.SetError(err)
		return false
	}
	return true
}

var parallelizeChecks = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.distsql.parallelize_checks.enabled",
	"determines whether FOREIGN KEY and UNIQUE constraint checks are performed in parallel",
	true,
)

// parallelChecksConcurrencyLimit controls the maximum number of additional
// goroutines that can be used to run checks in parallel.
var parallelChecksConcurrencyLimit = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.distsql.parallelize_checks.concurrency_limit",
	"maximum number of additional goroutines to run checks in parallel",
	// The default here is picked somewhat arbitrarily - the thinking is that we
	// want it to be proportional to the number of CPUs we have, yet this limit
	// should probably be smaller than kvcoord.senderConcurrencyLimit (which is
	// 64 x CPUs).
	int64(16*runtime.GOMAXPROCS(0)),
	settings.NonNegativeInt,
)

// planAndRunPostquery runs a cascade or check query. Can be safe for concurrent
// use if parallelCheck is true.
//
// - parallelCheck indicates whether this is a check query that runs in parallel
// with other check queries. If parallelCheck is true, then getSaveFlowsFunc,
// associateNodeWithComponents, and addTopLevelQueryStats must be
// concurrency-safe (if non-nil).
// - getSaveFlowsFunc will only be called if
// planner.instrumentation.ShouldSaveFlows() returns true.
func (dsp *DistSQLPlanner) planAndRunPostquery(
	ctx context.Context,
	postqueryPlan planMaybePhysical,
	planner *planner,
	evalCtx *extendedEvalContext,
	recv *DistSQLReceiver,
	parallelCheck bool,
	getSaveFlowsFunc func() SaveFlowsFunc,
	associateNodeWithComponents func(exec.Node, execComponents),
	addTopLevelQueryStats func(stats *topLevelQueryStats),
) error {
	postqueryDistribution, distSQLProhibitedErr := getPlanDistribution(
		ctx, planner.Descriptors().HasUncommittedTypes(),
		planner.SessionData(), postqueryPlan, &planner.distSQLVisitor,
	)
	distribute := DistributionType(LocalDistribution)
	if postqueryDistribution.WillDistribute() {
		distribute = FullDistribution
	}
	postqueryPlanCtx := dsp.NewPlanningCtx(ctx, evalCtx, planner, planner.txn, distribute)
	postqueryPlanCtx.distSQLProhibitedErr = distSQLProhibitedErr
	postqueryPlanCtx.stmtType = tree.Rows
	// Postqueries are only executed on the main query path where we skip the
	// diagram generation.
	postqueryPlanCtx.skipDistSQLDiagramGeneration = true
	postqueryPlanCtx.subOrPostQuery = true
	if planner.instrumentation.ShouldSaveFlows() {
		postqueryPlanCtx.saveFlows = getSaveFlowsFunc()
	}
	postqueryPlanCtx.associateNodeWithComponents = associateNodeWithComponents
	postqueryPlanCtx.collectExecStats = planner.instrumentation.ShouldCollectExecStats()
	postqueryPlanCtx.mustUseLeafTxn = parallelCheck

	postqueryPhysPlan, physPlanCleanup, err := dsp.createPhysPlan(ctx, postqueryPlanCtx, postqueryPlan)
	defer physPlanCleanup()
	if err != nil {
		return err
	}
	FinalizePlan(ctx, postqueryPlanCtx, postqueryPhysPlan)

	postqueryRecv := recv.clone()
	defer postqueryRecv.Release()
	defer addTopLevelQueryStats(&postqueryRecv.stats)
	postqueryResultWriter := &droppingResultWriter{}
	postqueryRecv.resultWriter = postqueryResultWriter
	postqueryRecv.batchWriter = postqueryResultWriter
	finishedSetupFn, cleanup := getFinishedSetupFn(planner)
	defer cleanup()
	dsp.Run(ctx, postqueryPlanCtx, planner.txn, postqueryPhysPlan, postqueryRecv, evalCtx, finishedSetupFn)
	return postqueryRecv.resultWriter.Err()
}

// planAndRunChecksInParallel executes all checkPlans in parallel. The function
// blocks until all checks that start executing return (i.e. when this function
// returns, it is guaranteed that the txn is no longer used by the checks).
//
// Note that it is assumed that all check plans use the old planNode
// representation.
func (dsp *DistSQLPlanner) planAndRunChecksInParallel(
	ctx context.Context,
	checkPlans []checkPlan,
	planner *planner,
	evalCtxFactory func(usedConcurrently bool) *extendedEvalContext,
	recv *DistSQLReceiver,
) error {
	ctx, cancelCtx := context.WithCancel(ctx)
	defer cancelCtx()

	// We need to synchronize many operations for the parallel checks, and we
	// use a single mutex for that. This seems acceptable given that these
	// operations are pretty quick and occur at different points throughout the
	// checks' execution, so there should be effectively no mutex contention.
	var mu syncutil.Mutex
	// For parallel checks we must make all `scanBufferNode`s in the plans
	// concurrency-safe. (The need to be able to walk the planNode tree is why
	// we currently disable the usage of the new DistSQL spec factory.)
	observer := planObserver{
		enterNode: func(_ context.Context, _ string, plan planNode) (bool, error) {
			if s, ok := plan.(*scanBufferNode); ok {
				s.makeConcurrencySafe(&mu)
			}
			return true, nil
		},
	}
	for i := range checkPlans {
		if checkPlans[i].plan.isPhysicalPlan() {
			return errors.AssertionFailedf("unexpectedly physical plan is used for a parallel CHECK")
		}
		// Ignore the error since our observer never returns an error.
		_ = walkPlan(
			ctx,
			checkPlans[i].plan.planNode,
			observer,
		)
	}
	var getSaveFlowsFunc func() SaveFlowsFunc
	if planner.instrumentation.ShouldSaveFlows() {
		// getDefaultSaveFlowsFunc returns a concurrency-unsafe function, so we
		// need to explicitly protect calls to it. Allocate this function only
		// when necessary.
		getSaveFlowsFunc = func() SaveFlowsFunc {
			fn := getDefaultSaveFlowsFunc(ctx, planner, planComponentTypePostquery)
			return func(flowSpec map[base.SQLInstanceID]*execinfrapb.FlowSpec, opChains execopnode.OpChains, localProcessors []execinfra.LocalProcessor, vectorized bool) error {
				mu.Lock()
				defer mu.Unlock()
				return fn(flowSpec, opChains, localProcessors, vectorized)
			}
		}
	}
	var associateNodeWithComponents func(exec.Node, execComponents)
	if fn := planner.instrumentation.getAssociateNodeWithComponentsFn(); fn != nil {
		// Since fn is not safe for concurrent use, we have to introduce the
		// synchronization on top of it.
		associateNodeWithComponents = func(node exec.Node, components execComponents) {
			mu.Lock()
			defer mu.Unlock()
			fn(node, components)
		}
	}
	// addTopLevelQueryStats is always allocated since it runs unconditionally
	// at the end of the postquery execution.
	addTopLevelQueryStats := func(other *topLevelQueryStats) {
		mu.Lock()
		defer mu.Unlock()
		recv.stats.add(other)
	}

	// We track errors according to the corresponding check plans. This is
	// needed in order to return the error for the "earliest" plan (which makes
	// the tests deterministic when multiple checks fail).
	errs := make([]error, len(checkPlans))
	runCheck := func(ctx context.Context, checkPlanIdx int) {
		log.VEventf(ctx, 3, "begin check %d", checkPlanIdx)
		errs[checkPlanIdx] = dsp.planAndRunPostquery(
			ctx, checkPlans[checkPlanIdx].plan,
			planner,
			evalCtxFactory(true /* usedConcurrently */),
			recv,
			true, /* parallelCheck */
			getSaveFlowsFunc,
			associateNodeWithComponents,
			addTopLevelQueryStats,
		)
		log.VEventf(ctx, 3, "end check %d", checkPlanIdx)
	}

	// Determine the concurrency we're allowed to use based on the node-wide
	// semaphore. We will always run at least one check in the current
	// goroutine.
	numParallelChecks := len(checkPlans) - 1
	if quota := int(dsp.parallelChecksSem.ApproximateQuota()); numParallelChecks > quota {
		numParallelChecks = quota
	}
	var alloc *quotapool.IntAlloc
	for numParallelChecks > 0 {
		var err error
		alloc, err = dsp.parallelLocalScansSem.TryAcquire(ctx, uint64(numParallelChecks))
		if err == nil {
			break
		}
		numParallelChecks--
	}
	if alloc != nil {
		defer alloc.Release()
	}

	log.VEventf(
		ctx, 2, executingParallelAndSerialChecks, numParallelChecks, len(checkPlans)-numParallelChecks,
	)

	// Set up a wait group so that the main (current) goroutine can block until
	// all concurrent checks return. We cannot short-circuit if one of the
	// checks results in a quick error in order to let all the planning infra
	// cleanup to be performed for each check, before we attempt to clean up the
	// whole plan.
	var wg sync.WaitGroup
	// Execute first numParallelChecks concurrently.
	for i := range checkPlans[:numParallelChecks] {
		checkPlanIdx := i
		wg.Add(1)
		if err := dsp.stopper.RunAsyncTaskEx(
			ctx,
			stop.TaskOpts{
				TaskName: "parallel-check-runner",
				SpanOpt:  stop.ChildSpan,
			},
			func(ctx context.Context) {
				defer wg.Done()
				runCheck(ctx, checkPlanIdx)
			}); err != nil {
			// The server is quiescing, so we just make sure to wait for all
			// already started checks to complete after canceling them.
			cancelCtx()
			// The task didn't start, so it won't be able to decrement the wait
			// group.
			wg.Done()
			wg.Wait()
			return err
		}
	}
	// Execute all other checks serially in the current goroutine.
	for checkPlanIdx := numParallelChecks; checkPlanIdx < len(checkPlans); checkPlanIdx++ {
		runCheck(ctx, checkPlanIdx)
	}
	// Wait for all concurrent checks to complete and return the error from the
	// earliest check (if there were any errors).
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}
