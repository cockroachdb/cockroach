// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colflow

import (
	"context"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colflow/colrpc"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/optional"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/marusama/semaphore"
)

// countingSemaphore is a semaphore that keeps track of the semaphore count from
// its perspective.
// Note that it effectively implements the execinfra.Releasable interface but
// due to the method name conflict doesn't.
type countingSemaphore struct {
	semaphore.Semaphore
	globalCount *metric.Gauge
	count       int64
}

var countingSemaphorePool = sync.Pool{
	New: func() interface{} {
		return &countingSemaphore{}
	},
}

func newCountingSemaphore(sem semaphore.Semaphore, globalCount *metric.Gauge) *countingSemaphore {
	s := countingSemaphorePool.Get().(*countingSemaphore)
	s.Semaphore = sem
	s.globalCount = globalCount
	return s
}

func (s *countingSemaphore) Acquire(ctx context.Context, n int) error {
	if err := s.Semaphore.Acquire(ctx, n); err != nil {
		return err
	}
	atomic.AddInt64(&s.count, int64(n))
	s.globalCount.Inc(int64(n))
	return nil
}

func (s *countingSemaphore) TryAcquire(n int) bool {
	success := s.Semaphore.TryAcquire(n)
	if !success {
		return false
	}
	atomic.AddInt64(&s.count, int64(n))
	s.globalCount.Inc(int64(n))
	return success
}

func (s *countingSemaphore) Release(n int) int {
	atomic.AddInt64(&s.count, int64(-n))
	s.globalCount.Dec(int64(n))
	return s.Semaphore.Release(n)
}

// ReleaseToPool should be named Release and should implement the
// execinfra.Releasable interface, but that would lead to a conflict with
// semaphore.Semaphore.Release method.
func (s *countingSemaphore) ReleaseToPool() {
	if unreleased := atomic.LoadInt64(&s.count); unreleased != 0 {
		colexecerror.InternalError(errors.Newf("unexpectedly %d count on the semaphore when releasing it to the pool", unreleased))
	}
	*s = countingSemaphore{}
	countingSemaphorePool.Put(s)
}

type vectorizedFlow struct {
	*flowinfra.FlowBase

	// creator is the object that created this flow. It must be cleaned up in
	// order to shut down the memory monitoring infrastructure and should be
	// released back to the pool.
	creator *vectorizedFlowCreator

	// batchFlowCoordinator will be set if the flow is pushing coldata.Batches
	// to the consumer.
	batchFlowCoordinator *BatchFlowCoordinator

	// countingSemaphore is a wrapper over a semaphore.Semaphore that keeps track
	// of the number of resources held in a semaphore.Semaphore requested from the
	// context of this flow so that these can be released unconditionally upon
	// Cleanup.
	countingSemaphore *countingSemaphore

	tempStorage struct {
		syncutil.Mutex
		// path is the path to this flow's temporary storage directory. If
		// it is an empty string, then it hasn't been computed yet nor the
		// directory has been created.
		path string
	}

	testingInfo struct {
		// numClosers is the number of components in the flow that implement
		// Close. This is used for testing assertions.
		numClosers int32
		// numClosed is a pointer to an int32 that is updated atomically when a
		// component's Close method is called. This is used for testing
		// assertions.
		numClosed *int32
	}

	testingKnobs struct {
		// onSetupFlow is a testing knob that is called before calling
		// creator.setupFlow with the given creator.
		onSetupFlow func(*vectorizedFlowCreator)
	}
}

var _ flowinfra.Flow = &vectorizedFlow{}
var _ execinfra.Releasable = &vectorizedFlow{}

var vectorizedFlowPool = sync.Pool{
	New: func() interface{} {
		return &vectorizedFlow{}
	},
}

// NewVectorizedFlow creates a new vectorized flow given the flow base.
func NewVectorizedFlow(base *flowinfra.FlowBase) flowinfra.Flow {
	vf := vectorizedFlowPool.Get().(*vectorizedFlow)
	vf.FlowBase = base
	return vf
}

// Setup is part of the flowinfra.Flow interface.
func (f *vectorizedFlow) Setup(
	ctx context.Context, spec *execinfrapb.FlowSpec, opt flowinfra.FuseOpt,
) (context.Context, execinfra.OpChains, error) {
	var err error
	ctx, _, err = f.FlowBase.Setup(ctx, spec, opt)
	if err != nil {
		return ctx, nil, err
	}
	log.VEvent(ctx, 2, "setting up vectorized flow")
	recordingStats := false
	if execinfra.ShouldCollectStats(ctx, &f.FlowCtx) {
		recordingStats = true
	}
	helper := newVectorizedFlowCreatorHelper(f.FlowBase)

	diskQueueCfg := colcontainer.DiskQueueCfg{
		FS:             f.Cfg.TempFS,
		DistSQLMetrics: f.Cfg.Metrics,
		GetPather:      f,
	}
	if err := diskQueueCfg.EnsureDefaults(); err != nil {
		return ctx, nil, err
	}
	f.countingSemaphore = newCountingSemaphore(f.Cfg.VecFDSemaphore, f.Cfg.Metrics.VecOpenFDs)
	flowCtx := f.GetFlowCtx()
	f.creator = newVectorizedFlowCreator(
		helper,
		vectorizedRemoteComponentCreator{},
		recordingStats,
		f.Gateway,
		f.GetWaitGroup(),
		f.GetRowSyncFlowConsumer(),
		f.GetBatchSyncFlowConsumer(),
		flowCtx.Cfg.PodNodeDialer,
		f.GetID(),
		diskQueueCfg,
		f.countingSemaphore,
		flowCtx.NewTypeResolver(flowCtx.EvalCtx.Txn),
		f.FlowBase.GetAdmissionInfo(),
	)
	if f.testingKnobs.onSetupFlow != nil {
		f.testingKnobs.onSetupFlow(f.creator)
	}
	opChains, batchFlowCoordinator, err := f.creator.setupFlow(ctx, flowCtx, spec.Processors, f.GetLocalProcessors(), opt)
	if err != nil {
		// It is (theoretically) possible that some of the memory monitoring
		// infrastructure was created even in case of an error, and we need to
		// clean that up.
		f.creator.cleanup(ctx)
		f.creator.Release()
		log.VEventf(ctx, 1, "failed to vectorize: %v", err)
		return ctx, nil, err
	}
	f.batchFlowCoordinator = batchFlowCoordinator
	f.testingInfo.numClosers = f.creator.numClosers
	f.testingInfo.numClosed = &f.creator.numClosed
	f.SetStartedGoroutines(f.creator.operatorConcurrency)
	log.VEventf(ctx, 2, "vectorized flow setup succeeded")
	if !f.IsLocal() {
		// For distributed flows set opChains to nil, per the contract of
		// flowinfra.Flow.Setup.
		opChains = nil
	}
	return ctx, opChains, nil
}

// Run is part of the Flow interface.
func (f *vectorizedFlow) Run(ctx context.Context, doneFn func()) {
	if f.batchFlowCoordinator == nil {
		// If we didn't create a BatchFlowCoordinator, then we have a processor
		// as the root, so we run this flow with the default implementation.
		f.FlowBase.Run(ctx, doneFn)
		return
	}

	defer f.Wait()

	if err := f.StartInternal(ctx, nil /* processors */, doneFn); err != nil {
		f.GetRowSyncFlowConsumer().Push(nil /* row */, &execinfrapb.ProducerMetadata{Err: err})
		f.GetRowSyncFlowConsumer().ProducerDone()
		return
	}

	log.VEvent(ctx, 1, "running the batch flow coordinator in the flow's goroutine")
	f.batchFlowCoordinator.Run(ctx)
}

var _ colcontainer.GetPather = &vectorizedFlow{}

// GetPath returns the path of the temporary directory for
// disk-spilling components of the flow. The directory is created on the first
// call to this method.
func (f *vectorizedFlow) GetPath(ctx context.Context) string {
	f.tempStorage.Lock()
	defer f.tempStorage.Unlock()
	if f.tempStorage.path != "" {
		// The temporary directory has already been created.
		return f.tempStorage.path
	}
	// We haven't created this flow's temporary directory yet, so we do so now.
	// The directory name is the flow's ID.
	tempDirName := f.GetID().String()
	f.tempStorage.path = filepath.Join(f.Cfg.TempStoragePath, tempDirName)
	log.VEventf(ctx, 1, "flow %s spilled to disk, stack trace: %s", f.ID, util.GetSmallTrace(2))
	if err := f.Cfg.TempFS.MkdirAll(f.tempStorage.path); err != nil {
		colexecerror.InternalError(errors.Wrap(err, "unable to create temporary storage directory"))
	}
	return f.tempStorage.path
}

// IsVectorized is part of the flowinfra.Flow interface.
func (f *vectorizedFlow) IsVectorized() bool {
	return true
}

// ConcurrentTxnUse is part of the flowinfra.Flow interface. It is conservative
// in that it returns that there is concurrent txn use as soon as any operator
// concurrency is detected. This should be inconsequential for local flows that
// use the RootTxn (which are the cases in which we care about this return
// value), because only unordered synchronizers introduce operator concurrency
// at the time of writing.
func (f *vectorizedFlow) ConcurrentTxnUse() bool {
	return f.creator.operatorConcurrency || f.FlowBase.ConcurrentTxnUse()
}

// Release implements the execinfra.Releasable interface.
func (f *vectorizedFlow) Release() {
	f.creator.Release()
	f.countingSemaphore.ReleaseToPool()
	*f = vectorizedFlow{}
	vectorizedFlowPool.Put(f)
}

// Cleanup is part of the flowinfra.Flow interface.
func (f *vectorizedFlow) Cleanup(ctx context.Context) {
	// This cleans up all the memory and disk monitoring of the vectorized flow.
	f.creator.cleanup(ctx)

	if buildutil.CrdbTestBuild && f.FlowBase.Started() {
		// Check that all closers have been closed. Note that we don't check
		// this in case the flow was never started in the first place (it is ok
		// to not check this since closers haven't allocated any resources in
		// such a case).
		if numClosed := atomic.LoadInt32(f.testingInfo.numClosed); numClosed != f.testingInfo.numClosers {
			colexecerror.InternalError(errors.AssertionFailedf("expected %d components to be closed, but found that only %d were", f.testingInfo.numClosers, numClosed))
		}
	}

	f.tempStorage.Lock()
	created := f.tempStorage.path != ""
	f.tempStorage.Unlock()
	if created {
		if err := f.Cfg.TempFS.RemoveAll(f.GetPath(ctx)); err != nil {
			// Log error as a Warning but keep on going to close the memory
			// infrastructure.
			log.Warningf(
				ctx,
				"unable to remove flow %s's temporary directory at %s, files may be left over: %v",
				f.GetID().Short(),
				f.GetPath(ctx),
				err,
			)
		}
	}
	// Release any leftover temporary storage file descriptors from this flow.
	if unreleased := atomic.LoadInt64(&f.countingSemaphore.count); unreleased > 0 {
		f.countingSemaphore.Release(int(unreleased))
	}
	f.FlowBase.Cleanup(ctx)
	f.Release()
}

// wrapWithVectorizedStatsCollectorBase creates a new
// colexecop.VectorizedStatsCollector that wraps op and connects the newly
// created wrapper with those corresponding to operators in inputs (the latter
// must have already been wrapped).
func (s *vectorizedFlowCreator) wrapWithVectorizedStatsCollectorBase(
	op *colexecargs.OpWithMetaInfo,
	kvReader colexecop.KVReader,
	columnarizer colexecop.VectorizedStatsCollector,
	inputs []colexecargs.OpWithMetaInfo,
	component execinfrapb.ComponentID,
	monitors []*mon.BytesMonitor,
) error {
	inputWatch := timeutil.NewStopWatch()
	var memMonitors, diskMonitors []*mon.BytesMonitor
	for _, m := range monitors {
		if m.Resource() == mon.DiskResource {
			diskMonitors = append(diskMonitors, m)
		} else {
			memMonitors = append(memMonitors, m)
		}
	}
	inputStatsCollectors := make([]childStatsCollector, len(inputs))
	for i, input := range inputs {
		sc, ok := input.Root.(childStatsCollector)
		if !ok {
			return errors.New("unexpectedly an input is not collecting stats")
		}
		inputStatsCollectors[i] = sc
	}
	vsc := newVectorizedStatsCollector(
		op.Root, kvReader, columnarizer, component, inputWatch,
		memMonitors, diskMonitors, inputStatsCollectors,
	)
	op.Root = vsc
	op.StatsCollectors = append(op.StatsCollectors, vsc)
	maybeAddStatsInvariantChecker(op)
	return nil
}

// wrapWithNetworkVectorizedStatsCollector creates a new
// colexecop.VectorizedStatsCollector that wraps op.
func (s *vectorizedFlowCreator) wrapWithNetworkVectorizedStatsCollector(
	op *colexecargs.OpWithMetaInfo,
	inbox *colrpc.Inbox,
	component execinfrapb.ComponentID,
	latency time.Duration,
) {
	inputWatch := timeutil.NewStopWatch()
	nvsc := newNetworkVectorizedStatsCollector(op.Root, component, inputWatch, inbox, latency)
	op.Root = nvsc
	op.StatsCollectors = []colexecop.VectorizedStatsCollector{nvsc}
	maybeAddStatsInvariantChecker(op)
}

// makeGetStatsFnForOutbox creates a function that will retrieve all execution
// statistics that the outbox is responsible for, nil is returned if stats are
// not being collected.
func (s *vectorizedFlowCreator) makeGetStatsFnForOutbox(
	flowCtx *execinfra.FlowCtx,
	statsCollectors []colexecop.VectorizedStatsCollector,
	originSQLInstanceID base.SQLInstanceID,
) func() []*execinfrapb.ComponentStats {
	if !s.recordingStats {
		return nil
	}
	return func() []*execinfrapb.ComponentStats {
		lastOutboxOnRemoteNode := atomic.AddInt32(&s.numOutboxesDrained, 1) == atomic.LoadInt32(&s.numOutboxes) && !s.isGatewayNode
		numResults := len(statsCollectors)
		if lastOutboxOnRemoteNode {
			numResults++
		}
		result := make([]*execinfrapb.ComponentStats, 0, numResults)
		for _, s := range statsCollectors {
			result = append(result, s.GetStats())
		}
		if lastOutboxOnRemoteNode {
			// At the last outbox, we can accurately retrieve stats for the
			// whole flow from parent monitors. These stats are added to a
			// flow-level span.
			result = append(result, &execinfrapb.ComponentStats{
				Component: execinfrapb.FlowComponentID(originSQLInstanceID, flowCtx.ID),
				FlowStats: execinfrapb.FlowStats{
					MaxMemUsage:  optional.MakeUint(uint64(flowCtx.EvalCtx.Mon.MaximumBytes())),
					MaxDiskUsage: optional.MakeUint(uint64(flowCtx.DiskMonitor.MaximumBytes())),
				},
			})
		}
		return result
	}
}

type runFn func(_ context.Context, flowCtxCancel context.CancelFunc)

// flowCreatorHelper contains all the logic needed to add the vectorized
// infrastructure to be run asynchronously as well as to perform some sanity
// checks.
type flowCreatorHelper interface {
	execinfra.Releasable
	// addStreamEndpoint stores information about an inbound stream.
	addStreamEndpoint(execinfrapb.StreamID, *colrpc.Inbox, *sync.WaitGroup)
	// checkInboundStreamID checks that the provided stream ID has not been seen
	// yet.
	checkInboundStreamID(execinfrapb.StreamID) error
	// accumulateAsyncComponent stores a component (either a router or an outbox)
	// to be run asynchronously.
	accumulateAsyncComponent(runFn)
	// addFlowCoordinator adds the FlowCoordinator to the flow. This is only
	// done on the gateway node.
	addFlowCoordinator(coordinator *FlowCoordinator)
	// getCtxDone returns done channel of the context of this flow.
	getFlowCtxDone() <-chan struct{}
	// getCancelFlowFn returns a flow cancellation function.
	getCancelFlowFn() context.CancelFunc
}

type admissionOptions struct {
	admissionQ    *admission.WorkQueue
	admissionInfo admission.WorkInfo
}

// remoteComponentCreator is an interface that abstracts the constructors for
// several components in a remote flow. Mostly for testing purposes.
type remoteComponentCreator interface {
	newOutbox(
		allocator *colmem.Allocator,
		input colexecargs.OpWithMetaInfo,
		typs []*types.T,
		getStats func() []*execinfrapb.ComponentStats,
	) (*colrpc.Outbox, error)
	newInbox(
		allocator *colmem.Allocator,
		typs []*types.T,
		streamID execinfrapb.StreamID,
		flowCtxDone <-chan struct{},
		admissionOpts admissionOptions,
	) (*colrpc.Inbox, error)
}

type vectorizedRemoteComponentCreator struct{}

func (vectorizedRemoteComponentCreator) newOutbox(
	allocator *colmem.Allocator,
	input colexecargs.OpWithMetaInfo,
	typs []*types.T,
	getStats func() []*execinfrapb.ComponentStats,
) (*colrpc.Outbox, error) {
	return colrpc.NewOutbox(allocator, input, typs, getStats)
}

func (vectorizedRemoteComponentCreator) newInbox(
	allocator *colmem.Allocator,
	typs []*types.T,
	streamID execinfrapb.StreamID,
	flowCtxDone <-chan struct{},
	admissionOpts admissionOptions,
) (*colrpc.Inbox, error) {
	return colrpc.NewInboxWithAdmissionControl(
		allocator, typs, streamID, flowCtxDone,
		admissionOpts.admissionQ, admissionOpts.admissionInfo,
	)
}

// vectorizedFlowCreator performs all the setup of vectorized flows. Depending
// on embedded flowCreatorHelper, it can either do the actual setup in order
// to run the flow or do the setup needed to check that the flow is supported
// through the vectorized engine.
type vectorizedFlowCreator struct {
	flowCreatorHelper
	remoteComponentCreator

	// rowReceiver is always set.
	rowReceiver execinfra.RowReceiver
	// batchReceiver might be set if the consumer supports pushing of
	// coldata.Batches.
	batchReceiver execinfra.BatchReceiver
	// batchFlowCoordinator, if set, indicates that the vectorized flow should
	// not use the default FlowBase.Run implementation.
	batchFlowCoordinator *BatchFlowCoordinator

	streamIDToInputOp map[execinfrapb.StreamID]colexecargs.OpWithMetaInfo
	streamIDToSpecIdx map[execinfrapb.StreamID]int
	recordingStats    bool
	isGatewayNode     bool
	waitGroup         *sync.WaitGroup
	nodeDialer        *nodedialer.Dialer
	flowID            execinfrapb.FlowID
	exprHelper        *colexecargs.ExprHelper
	typeResolver      descs.DistSQLTypeResolver
	admissionInfo     admission.WorkInfo

	// numOutboxes counts how many colrpc.Outbox'es have been set up on this
	// node. It must be accessed atomically.
	numOutboxes int32
	// numOutboxesDrained is an atomic that keeps track of how many outboxes
	// have been drained. When numOutboxesDrained equals numOutboxes, flow-level
	// metadata is added to a flow-level span on the non-gateway nodes.
	numOutboxesDrained int32

	// procIdxQueue is a queue of indices into processorSpecs (the argument to
	// setupFlow), for topologically ordered processing.
	procIdxQueue []int
	// opChains accumulates all operators that have no further outputs on the
	// current node, for the purposes of EXPLAIN output.
	opChains execinfra.OpChains
	// operatorConcurrency is set if any operators are executed in parallel.
	operatorConcurrency bool
	// releasables contains all components that should be released back to their
	// pools during the flow cleanup.
	releasables []execinfra.Releasable

	monitorRegistry colexecargs.MonitorRegistry
	diskQueueCfg    colcontainer.DiskQueueCfg
	fdSemaphore     semaphore.Semaphore

	// numClosers and numClosed are used to assert during testing that the
	// expected number of components are closed.
	numClosers int32
	numClosed  int32
}

var _ execinfra.Releasable = &vectorizedFlowCreator{}

var vectorizedFlowCreatorPool = sync.Pool{
	New: func() interface{} {
		return &vectorizedFlowCreator{
			streamIDToInputOp: make(map[execinfrapb.StreamID]colexecargs.OpWithMetaInfo),
			streamIDToSpecIdx: make(map[execinfrapb.StreamID]int),
			exprHelper:        colexecargs.NewExprHelper(),
		}
	},
}

func newVectorizedFlowCreator(
	helper flowCreatorHelper,
	componentCreator remoteComponentCreator,
	recordingStats bool,
	isGatewayNode bool,
	waitGroup *sync.WaitGroup,
	rowSyncFlowConsumer execinfra.RowReceiver,
	batchSyncFlowConsumer execinfra.BatchReceiver,
	nodeDialer *nodedialer.Dialer,
	flowID execinfrapb.FlowID,
	diskQueueCfg colcontainer.DiskQueueCfg,
	fdSemaphore semaphore.Semaphore,
	typeResolver descs.DistSQLTypeResolver,
	admissionInfo admission.WorkInfo,
) *vectorizedFlowCreator {
	creator := vectorizedFlowCreatorPool.Get().(*vectorizedFlowCreator)
	*creator = vectorizedFlowCreator{
		flowCreatorHelper:      helper,
		remoteComponentCreator: componentCreator,
		streamIDToInputOp:      creator.streamIDToInputOp,
		streamIDToSpecIdx:      creator.streamIDToSpecIdx,
		recordingStats:         recordingStats,
		isGatewayNode:          isGatewayNode,
		waitGroup:              waitGroup,
		rowReceiver:            rowSyncFlowConsumer,
		batchReceiver:          batchSyncFlowConsumer,
		nodeDialer:             nodeDialer,
		flowID:                 flowID,
		exprHelper:             creator.exprHelper,
		typeResolver:           typeResolver,
		admissionInfo:          admissionInfo,
		procIdxQueue:           creator.procIdxQueue,
		opChains:               creator.opChains,
		releasables:            creator.releasables,
		monitorRegistry:        creator.monitorRegistry,
		diskQueueCfg:           diskQueueCfg,
		fdSemaphore:            fdSemaphore,
	}
	return creator
}

func (s *vectorizedFlowCreator) cleanup(ctx context.Context) {
	s.monitorRegistry.Close(ctx)
}

// Release implements the execinfra.Releasable interface.
func (s *vectorizedFlowCreator) Release() {
	for k := range s.streamIDToInputOp {
		delete(s.streamIDToInputOp, k)
	}
	for k := range s.streamIDToSpecIdx {
		delete(s.streamIDToSpecIdx, k)
	}
	s.flowCreatorHelper.Release()
	for _, r := range s.releasables {
		r.Release()
	}
	// Deeply reset slices that might point to the objects of non-trivial size
	// so that the old references don't interfere with the objects being
	// garbage-collected.
	for i := range s.opChains {
		s.opChains[i] = nil
	}
	for i := range s.releasables {
		s.releasables[i] = nil
	}
	if s.exprHelper != nil {
		s.exprHelper.SemaCtx = nil
	}
	s.monitorRegistry.Reset()
	*s = vectorizedFlowCreator{
		streamIDToInputOp: s.streamIDToInputOp,
		streamIDToSpecIdx: s.streamIDToSpecIdx,
		exprHelper:        s.exprHelper,
		// procIdxQueue is a slice of ints, so it's ok to just slice up to 0 to
		// prime it for reuse.
		procIdxQueue:    s.procIdxQueue[:0],
		opChains:        s.opChains[:0],
		releasables:     s.releasables[:0],
		monitorRegistry: s.monitorRegistry,
	}
	vectorizedFlowCreatorPool.Put(s)
}

// setupRemoteOutputStream sets up an Outbox that will operate according to
// the given StreamEndpointSpec. It will also drain all MetadataSources in the
// metadataSources.
// NOTE: The caller must not reuse the metadataSources and toClose.
func (s *vectorizedFlowCreator) setupRemoteOutputStream(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	op colexecargs.OpWithMetaInfo,
	outputTyps []*types.T,
	stream *execinfrapb.StreamEndpointSpec,
	factory coldata.ColumnFactory,
	getStats func() []*execinfrapb.ComponentStats,
) (execinfra.OpNode, error) {
	outbox, err := s.remoteComponentCreator.newOutbox(
		colmem.NewAllocator(ctx, s.monitorRegistry.NewStreamingMemAccount(flowCtx), factory),
		op, outputTyps, getStats,
	)
	if err != nil {
		return nil, err
	}

	atomic.AddInt32(&s.numOutboxes, 1)
	run := func(ctx context.Context, flowCtxCancel context.CancelFunc) {
		outbox.Run(
			ctx,
			s.nodeDialer,
			stream.TargetNodeID,
			s.flowID,
			stream.StreamID,
			flowCtxCancel,
			flowinfra.SettingFlowStreamTimeout.Get(&flowCtx.Cfg.Settings.SV),
		)
	}
	s.accumulateAsyncComponent(run)
	return outbox, nil
}

// setupRouter sets up a vectorized hash router according to the output router
// spec. If the outputs are local, these are added to s.streamIDToInputOp to be
// used as inputs in further planning. metadataSources is passed along to any
// outboxes created to be drained, or stored in streamIDToInputOp for any local
// outputs to pass that responsibility along. In any case, metadataSources will
// always be fully consumed.
// NOTE: This method supports only BY_HASH routers. Callers should handle
// PASS_THROUGH routers separately.
// NOTE: The caller must not reuse the metadataSources and toClose.
func (s *vectorizedFlowCreator) setupRouter(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	input colexecargs.OpWithMetaInfo,
	outputTyps []*types.T,
	output *execinfrapb.OutputRouterSpec,
	factory coldata.ColumnFactory,
) error {
	if output.Type != execinfrapb.OutputRouterSpec_BY_HASH {
		return errors.Errorf("vectorized output router type %s unsupported", output.Type)
	}

	// HashRouter memory monitor names are the concatenated output stream IDs.
	streamIDs := make([]string, len(output.Streams))
	for i, s := range output.Streams {
		streamIDs[i] = strconv.Itoa(int(s.StreamID))
	}
	mmName := "hash-router-[" + strings.Join(streamIDs, ",") + "]"

	hashRouterMemMonitor, accounts := s.monitorRegistry.CreateUnlimitedMemAccounts(ctx, flowCtx, mmName, len(output.Streams))
	allocators := make([]*colmem.Allocator, len(output.Streams))
	for i := range allocators {
		allocators[i] = colmem.NewAllocator(ctx, accounts[i], factory)
	}
	diskMon, diskAccounts := s.monitorRegistry.CreateDiskAccounts(ctx, flowCtx, mmName, len(output.Streams))
	router, outputs := NewHashRouter(
		allocators, input, outputTyps, output.HashColumns, execinfra.GetWorkMemLimit(flowCtx),
		s.diskQueueCfg, s.fdSemaphore, diskAccounts,
	)
	runRouter := func(ctx context.Context, _ context.CancelFunc) {
		router.Run(logtags.AddTag(ctx, "hashRouterID", strings.Join(streamIDs, ",")))
	}
	s.accumulateAsyncComponent(runRouter)

	foundLocalOutput := false
	for i, op := range outputs {
		if buildutil.CrdbTestBuild {
			op = colexec.NewInvariantsChecker(op)
		}
		stream := &output.Streams[i]
		switch stream.Type {
		case execinfrapb.StreamEndpointSpec_SYNC_RESPONSE:
			return errors.Errorf("unexpected sync response output when setting up router")
		case execinfrapb.StreamEndpointSpec_REMOTE:
			// Note that here we pass in nil 'toClose' slice because hash
			// router is responsible for closing all of the idempotent closers.
			if _, err := s.setupRemoteOutputStream(
				ctx, flowCtx, colexecargs.OpWithMetaInfo{
					Root:            op,
					MetadataSources: colexecop.MetadataSources{op},
				}, outputTyps, stream, factory, nil, /* getStats */
			); err != nil {
				return err
			}
		case execinfrapb.StreamEndpointSpec_LOCAL:
			foundLocalOutput = true
			opWithMetaInfo := colexecargs.OpWithMetaInfo{
				Root:            op,
				MetadataSources: colexecop.MetadataSources{op},
				// ToClose will be closed by the hash router.
				ToClose: nil,
			}
			if s.recordingStats {
				mons := []*mon.BytesMonitor{hashRouterMemMonitor, diskMon}
				// Wrap local outputs with vectorized stats collectors when recording
				// stats. This is mostly for compatibility but will provide some useful
				// information (e.g. output stall time).
				if err := s.wrapWithVectorizedStatsCollectorBase(
					&opWithMetaInfo, nil /* kvReader */, nil, /* columnarizer */
					nil /* inputs */, flowCtx.StreamComponentID(stream.StreamID), mons,
				); err != nil {
					return err
				}
			}
			s.streamIDToInputOp[stream.StreamID] = opWithMetaInfo
		}
	}
	if !foundLocalOutput {
		// No local output means that our router is a root of its operator
		// chain.
		s.opChains = append(s.opChains, router)
	}
	return nil
}

// setupInput sets up one or more input operators (local or remote) and a
// synchronizer to expose these separate streams as one exec.Operator which is
// returned. If s.recordingStats is true, these inputs and synchronizer are
// wrapped in stats collectors if not done so, although these stats are not
// exposed as of yet. Inboxes that are created are also returned as
// []colexecop.MetadataSource so that any remote metadata can be read through
// calling DrainMeta.
func (s *vectorizedFlowCreator) setupInput(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	input execinfrapb.InputSyncSpec,
	opt flowinfra.FuseOpt,
	factory coldata.ColumnFactory,
) (colexecargs.OpWithMetaInfo, error) {
	inputStreamOps := make([]colexecargs.OpWithMetaInfo, 0, len(input.Streams))
	// Before we can safely use types we received over the wire in the
	// operators, we need to make sure they are hydrated. In row execution
	// engine it is done during the processor initialization, but operators
	// don't do that.
	if err := s.typeResolver.HydrateTypeSlice(ctx, input.ColumnTypes); err != nil {
		return colexecargs.OpWithMetaInfo{}, err
	}

	for _, inputStream := range input.Streams {
		switch inputStream.Type {
		case execinfrapb.StreamEndpointSpec_LOCAL:
			in := s.streamIDToInputOp[inputStream.StreamID]
			inputStreamOps = append(inputStreamOps, in)
		case execinfrapb.StreamEndpointSpec_REMOTE:
			// If the input is remote, the input operator does not exist in
			// streamIDToInputOp. Create an inbox.
			if err := s.checkInboundStreamID(inputStream.StreamID); err != nil {
				return colexecargs.OpWithMetaInfo{}, err
			}

			// Retrieve the latency from the origin node (the one that has the
			// outbox).
			latency, err := s.nodeDialer.Latency(roachpb.NodeID(inputStream.OriginNodeID))
			if err != nil {
				// If an error occurred, latency's nil value of 0 is used. If latency is
				// 0, it is not included in the displayed stats for EXPLAIN ANALYZE
				// diagrams.
				latency = 0
				log.VEventf(ctx, 1, "an error occurred during vectorized planning while getting latency: %v", err)
			}
			inbox, err := s.remoteComponentCreator.newInbox(
				colmem.NewAllocator(ctx, s.monitorRegistry.NewStreamingMemAccount(flowCtx), factory),
				input.ColumnTypes,
				inputStream.StreamID,
				s.flowCreatorHelper.getFlowCtxDone(),
				admissionOptions{
					admissionQ:    flowCtx.Cfg.SQLSQLResponseAdmissionQ,
					admissionInfo: s.admissionInfo,
				})

			if err != nil {
				return colexecargs.OpWithMetaInfo{}, err
			}
			s.addStreamEndpoint(inputStream.StreamID, inbox, s.waitGroup)
			op := colexecop.Operator(inbox)
			ms := colexecop.MetadataSource(inbox)
			if buildutil.CrdbTestBuild {
				op = colexec.NewInvariantsChecker(op)
				ms = op.(colexecop.MetadataSource)
			}
			opWithMetaInfo := colexecargs.OpWithMetaInfo{
				Root:            op,
				MetadataSources: colexecop.MetadataSources{ms},
			}
			if s.recordingStats {
				// Note: we can't use flowCtx.StreamComponentID because the stream does
				// not originate from this node (we are the target node).
				compID := execinfrapb.StreamComponentID(
					inputStream.OriginNodeID, flowCtx.ID, inputStream.StreamID,
				)
				s.wrapWithNetworkVectorizedStatsCollector(&opWithMetaInfo, inbox, compID, latency)
			}
			inputStreamOps = append(inputStreamOps, opWithMetaInfo)
		default:
			return colexecargs.OpWithMetaInfo{}, errors.Errorf("unsupported input stream type %s", inputStream.Type)
		}
	}
	opWithMetaInfo := inputStreamOps[0]
	if len(inputStreamOps) > 1 {
		statsInputs := inputStreamOps
		if input.Type == execinfrapb.InputSyncSpec_ORDERED {
			os := colexec.NewOrderedSynchronizer(
				colmem.NewAllocator(ctx, s.monitorRegistry.NewStreamingMemAccount(flowCtx), factory),
				execinfra.GetWorkMemLimit(flowCtx), inputStreamOps,
				input.ColumnTypes, execinfrapb.ConvertToColumnOrdering(input.Ordering),
			)
			opWithMetaInfo = colexecargs.OpWithMetaInfo{
				Root:            os,
				MetadataSources: colexecop.MetadataSources{os},
				ToClose:         colexecop.Closers{os},
			}
		} else if input.Type == execinfrapb.InputSyncSpec_SERIAL_UNORDERED || opt == flowinfra.FuseAggressively {
			sync := colexec.NewSerialUnorderedSynchronizer(inputStreamOps)
			opWithMetaInfo = colexecargs.OpWithMetaInfo{
				Root:            sync,
				MetadataSources: colexecop.MetadataSources{sync},
				ToClose:         colexecop.Closers{sync},
			}
		} else {
			// Note that if we have opt == flowinfra.FuseAggressively, then we
			// must use the serial unordered sync above in order to remove any
			// concurrency.
			sync := colexec.NewParallelUnorderedSynchronizer(inputStreamOps, s.waitGroup)
			sync.LocalPlan = flowCtx.Local
			opWithMetaInfo = colexecargs.OpWithMetaInfo{
				Root:            sync,
				MetadataSources: colexecop.MetadataSources{sync},
				ToClose:         colexecop.Closers{sync},
			}
			s.operatorConcurrency = true
			// Don't use the unordered synchronizer's inputs for stats collection
			// given that they run concurrently. The stall time will be collected
			// instead.
			statsInputs = nil
		}
		if buildutil.CrdbTestBuild {
			opWithMetaInfo.Root = colexec.NewInvariantsChecker(opWithMetaInfo.Root)
			opWithMetaInfo.MetadataSources[0] = opWithMetaInfo.Root.(colexecop.MetadataSource)
		}
		if s.recordingStats {
			statsInputsAsOps := make([]colexecargs.OpWithMetaInfo, len(statsInputs))
			for i := range statsInputs {
				statsInputsAsOps[i].Root = statsInputs[i].Root
			}
			// TODO(asubiotto): Once we have IDs for synchronizers, plumb them into
			// this stats collector to display stats.
			if err := s.wrapWithVectorizedStatsCollectorBase(
				&opWithMetaInfo, nil /* kvReader */, nil, /* columnarizer */
				statsInputsAsOps, execinfrapb.ComponentID{}, nil, /* monitors */
			); err != nil {
				return colexecargs.OpWithMetaInfo{}, err
			}
		}
	}
	return opWithMetaInfo, nil
}

// setupOutput sets up any necessary infrastructure according to the output spec
// of pspec. The metadataSources and toClose slices are fully consumed by either
// passing them to an outbox or HashRouter to be drained/closed, or storing them
// in streamIDToInputOp with the given op to be processed later.
// NOTE: The caller must not reuse the metadataSources and toClose.
func (s *vectorizedFlowCreator) setupOutput(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	pspec *execinfrapb.ProcessorSpec,
	opWithMetaInfo colexecargs.OpWithMetaInfo,
	opOutputTypes []*types.T,
	factory coldata.ColumnFactory,
) error {
	output := &pspec.Output[0]
	if output.Type != execinfrapb.OutputRouterSpec_PASS_THROUGH {
		return s.setupRouter(
			ctx,
			flowCtx,
			opWithMetaInfo,
			opOutputTypes,
			output,
			factory,
		)
	}

	if len(output.Streams) != 1 {
		return errors.Errorf("unsupported multi outputstream proc (%d streams)", len(output.Streams))
	}
	outputStream := &output.Streams[0]
	switch outputStream.Type {
	case execinfrapb.StreamEndpointSpec_LOCAL:
		s.streamIDToInputOp[outputStream.StreamID] = opWithMetaInfo
	case execinfrapb.StreamEndpointSpec_REMOTE:
		// Set up an Outbox.
		outbox, err := s.setupRemoteOutputStream(
			ctx, flowCtx, opWithMetaInfo, opOutputTypes, outputStream, factory,
			s.makeGetStatsFnForOutbox(flowCtx, opWithMetaInfo.StatsCollectors, outputStream.OriginNodeID),
		)
		if err != nil {
			return err
		}
		// An outbox is a root of its operator chain: there's nothing that sees
		// it as an input on this node.
		s.opChains = append(s.opChains, outbox)
	case execinfrapb.StreamEndpointSpec_SYNC_RESPONSE:
		// Check whether the root of the chain is a columnarizer - if so, we can
		// avoid creating the materializer.
		input := colbuilder.MaybeRemoveRootColumnarizer(opWithMetaInfo)
		if input == nil && s.batchReceiver != nil {
			// We can create a batch flow coordinator and avoid materializing
			// the batches.
			s.batchFlowCoordinator = NewBatchFlowCoordinator(
				flowCtx,
				pspec.ProcessorID,
				opWithMetaInfo,
				s.batchReceiver,
				s.getCancelFlowFn(),
			)
			// The flow coordinator is a root of its operator chain.
			s.opChains = append(s.opChains, s.batchFlowCoordinator)
			s.releasables = append(s.releasables, s.batchFlowCoordinator)
		} else {
			// We need to use the row receiving output.
			if input != nil {
				// We successfully removed the columnarizer.
				if buildutil.CrdbTestBuild {
					// That columnarizer was added as a closer, so we need to
					// decrement the number of expected closers.
					s.numClosers--
				}
			} else {
				input = colexec.NewMaterializerNoEvalCtxCopy(
					flowCtx,
					pspec.ProcessorID,
					opWithMetaInfo,
					opOutputTypes,
				)
			}
			// Make the FlowCoordinator, which will write to the given row
			// receiver.
			f := NewFlowCoordinator(
				flowCtx,
				pspec.ProcessorID,
				input,
				s.rowReceiver,
				s.getCancelFlowFn(),
			)
			// The flow coordinator is a root of its operator chain.
			s.opChains = append(s.opChains, f)
			// NOTE: we don't append f to s.releasables because addFlowCoordinator
			// adds the FlowCoordinator to FlowBase.processors, which ensures that
			// it is later released in FlowBase.Cleanup.
			s.addFlowCoordinator(f)
		}

	default:
		return errors.Errorf("unsupported output stream type %s", outputStream.Type)
	}
	return nil
}

// callbackCloser is a utility struct that implements the Closer interface by
// calling the provided callback.
type callbackCloser struct {
	closeCb func(context.Context) error
}

var _ colexecop.Closer = &callbackCloser{}

// Close implements the Closer interface.
func (c *callbackCloser) Close(ctx context.Context) error {
	return c.closeCb(ctx)
}

func (s *vectorizedFlowCreator) setupFlow(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorSpecs []execinfrapb.ProcessorSpec,
	localProcessors []execinfra.LocalProcessor,
	opt flowinfra.FuseOpt,
) (opChains execinfra.OpChains, batchFlowCoordinator *BatchFlowCoordinator, err error) {
	if vecErr := colexecerror.CatchVectorizedRuntimeError(func() {
		// The column factory will not change the eval context, so we can use
		// the one we have in the flow context, without making a copy.
		factory := coldataext.NewExtendedColumnFactory(flowCtx.EvalCtx)
		for i := range processorSpecs {
			hasLocalInput := false
			for j := range processorSpecs[i].Input {
				input := &processorSpecs[i].Input[j]
				for k := range input.Streams {
					stream := &input.Streams[k]
					s.streamIDToSpecIdx[stream.StreamID] = i
					if stream.Type != execinfrapb.StreamEndpointSpec_REMOTE {
						hasLocalInput = true
					}
				}
			}
			if hasLocalInput {
				continue
			}
			// Queue all processors with either no inputs or remote inputs.
			s.procIdxQueue = append(s.procIdxQueue, i)
		}

		for procIdxQueuePos := 0; procIdxQueuePos < len(processorSpecs); procIdxQueuePos++ {
			pspec := &processorSpecs[s.procIdxQueue[procIdxQueuePos]]
			if len(pspec.Output) > 1 {
				err = errors.Errorf("unsupported multi-output proc (%d outputs)", len(pspec.Output))
				return
			}

			inputs := make([]colexecargs.OpWithMetaInfo, len(pspec.Input))
			for i := range pspec.Input {
				inputs[i], err = s.setupInput(ctx, flowCtx, pspec.Input[i], opt, factory)
				if err != nil {
					return
				}
			}

			// Before we can safely use types we received over the wire in the
			// operators, we need to make sure they are hydrated.
			if err = s.typeResolver.HydrateTypeSlice(ctx, pspec.ResultTypes); err != nil {
				return
			}

			args := &colexecargs.NewColOperatorArgs{
				Spec:                 pspec,
				Inputs:               inputs,
				StreamingMemAccount:  s.monitorRegistry.NewStreamingMemAccount(flowCtx),
				ProcessorConstructor: rowexec.NewProcessor,
				LocalProcessors:      localProcessors,
				DiskQueueCfg:         s.diskQueueCfg,
				FDSemaphore:          s.fdSemaphore,
				ExprHelper:           s.exprHelper,
				Factory:              factory,
				MonitorRegistry:      &s.monitorRegistry,
			}
			numOldMonitors := len(s.monitorRegistry.GetMonitors())
			if args.ExprHelper.SemaCtx == nil {
				args.ExprHelper.SemaCtx = flowCtx.NewSemaContext(flowCtx.EvalCtx.Txn)
			}
			var result *colexecargs.NewColOperatorResult
			result, err = colbuilder.NewColOperator(ctx, flowCtx, args)
			if result != nil {
				s.releasables = append(s.releasables, result)
			}
			if err != nil {
				err = errors.Wrapf(err, "unable to vectorize execution plan")
				return
			}
			if flowCtx.EvalCtx.SessionData().TestingVectorizeInjectPanics {
				result.Root = newPanicInjector(result.Root)
			}
			if buildutil.CrdbTestBuild {
				toCloseCopy := append(colexecop.Closers{}, result.ToClose...)
				for i := range toCloseCopy {
					func(idx int) {
						closed := false
						result.ToClose[idx] = &callbackCloser{closeCb: func(ctx context.Context) error {
							if !closed {
								closed = true
								atomic.AddInt32(&s.numClosed, 1)
							}
							return toCloseCopy[idx].Close(ctx)
						}}
					}(i)
				}
				s.numClosers += int32(len(result.ToClose))
			}

			if s.recordingStats {
				newMonitors := s.monitorRegistry.GetMonitors()[numOldMonitors:]
				if err := s.wrapWithVectorizedStatsCollectorBase(
					&result.OpWithMetaInfo, result.KVReader, result.Columnarizer, inputs,
					flowCtx.ProcessorComponentID(pspec.ProcessorID), newMonitors,
				); err != nil {
					return
				}
			}

			if err = s.setupOutput(
				ctx, flowCtx, pspec, result.OpWithMetaInfo, result.ColumnTypes, factory,
			); err != nil {
				return
			}

			// Now queue all outputs from this op whose inputs are already all
			// populated.
		NEXTOUTPUT:
			for i := range pspec.Output {
				for j := range pspec.Output[i].Streams {
					outputStream := &pspec.Output[i].Streams[j]
					if outputStream.Type != execinfrapb.StreamEndpointSpec_LOCAL {
						continue
					}
					procIdx, ok := s.streamIDToSpecIdx[outputStream.StreamID]
					if !ok {
						err = errors.Errorf("couldn't find stream %d", outputStream.StreamID)
						return
					}
					outputSpec := &processorSpecs[procIdx]
					for k := range outputSpec.Input {
						for l := range outputSpec.Input[k].Streams {
							inputStream := outputSpec.Input[k].Streams[l]
							if inputStream.Type == execinfrapb.StreamEndpointSpec_REMOTE {
								// Remote streams are not present in streamIDToInputOp. The
								// Inboxes that consume these streams are created at the same time
								// as the operator that needs them, so skip the creation check for
								// this input.
								continue
							}
							if _, ok := s.streamIDToInputOp[inputStream.StreamID]; !ok {
								continue NEXTOUTPUT
							}
						}
					}
					// We found an input op for every single stream in this output. Queue
					// it for processing.
					s.procIdxQueue = append(s.procIdxQueue, procIdx)
				}
			}
		}
	}); vecErr != nil {
		return s.opChains, s.batchFlowCoordinator, vecErr
	}
	return s.opChains, s.batchFlowCoordinator, err
}

type vectorizedInboundStreamHandler struct {
	*colrpc.Inbox
}

var _ flowinfra.InboundStreamHandler = vectorizedInboundStreamHandler{}

// Run is part of the flowinfra.InboundStreamHandler interface.
func (s vectorizedInboundStreamHandler) Run(
	ctx context.Context,
	stream execinfrapb.DistSQL_FlowStreamServer,
	_ *execinfrapb.ProducerMessage,
	_ *flowinfra.FlowBase,
) error {
	return s.RunWithStream(ctx, stream)
}

// Timeout is part of the flowinfra.InboundStreamHandler interface.
func (s vectorizedInboundStreamHandler) Timeout(err error) {
	s.Inbox.Timeout(err)
}

// vectorizedFlowCreatorHelper is a flowCreatorHelper that sets up all the
// vectorized infrastructure to be actually run.
type vectorizedFlowCreatorHelper struct {
	f          *flowinfra.FlowBase
	processors []execinfra.Processor
}

var _ flowCreatorHelper = &vectorizedFlowCreatorHelper{}

var vectorizedFlowCreatorHelperPool = sync.Pool{
	New: func() interface{} {
		return &vectorizedFlowCreatorHelper{
			processors: make([]execinfra.Processor, 0, 1),
		}
	},
}

func newVectorizedFlowCreatorHelper(f *flowinfra.FlowBase) *vectorizedFlowCreatorHelper {
	helper := vectorizedFlowCreatorHelperPool.Get().(*vectorizedFlowCreatorHelper)
	helper.f = f
	return helper
}

func (r *vectorizedFlowCreatorHelper) addStreamEndpoint(
	streamID execinfrapb.StreamID, inbox *colrpc.Inbox, wg *sync.WaitGroup,
) {
	r.f.AddRemoteStream(streamID, flowinfra.NewInboundStreamInfo(
		vectorizedInboundStreamHandler{inbox},
		wg,
	))
}

func (r *vectorizedFlowCreatorHelper) checkInboundStreamID(sid execinfrapb.StreamID) error {
	return r.f.CheckInboundStreamID(sid)
}

func (r *vectorizedFlowCreatorHelper) accumulateAsyncComponent(run runFn) {
	r.f.AddStartable(
		flowinfra.StartableFn(func(ctx context.Context, wg *sync.WaitGroup, flowCtxCancel context.CancelFunc) {
			if wg != nil {
				wg.Add(1)
			}
			go func() {
				run(ctx, flowCtxCancel)
				if wg != nil {
					wg.Done()
				}
			}()
		}))
}

func (r *vectorizedFlowCreatorHelper) addFlowCoordinator(f *FlowCoordinator) {
	r.processors = append(r.processors, f)
	r.f.SetProcessors(r.processors)
}

func (r *vectorizedFlowCreatorHelper) getFlowCtxDone() <-chan struct{} {
	return r.f.GetCtxDone()
}

func (r *vectorizedFlowCreatorHelper) getCancelFlowFn() context.CancelFunc {
	return r.f.GetCancelFlowFn()
}

func (r *vectorizedFlowCreatorHelper) Release() {
	// Note that processors here can only be of 0 or 1 length, but always of
	// 1 capacity (only the flow coordinator can be appended to this slice).
	// Unset the slot so that we don't keep the reference to the old flow
	// coordinator.
	if len(r.processors) == 1 {
		r.processors[0] = nil
	}
	*r = vectorizedFlowCreatorHelper{
		processors: r.processors[:0],
	}
	vectorizedFlowCreatorHelperPool.Put(r)
}

// noopFlowCreatorHelper is a flowCreatorHelper that only performs sanity
// checks.
type noopFlowCreatorHelper struct {
	inboundStreams map[execinfrapb.StreamID]struct{}
}

var _ flowCreatorHelper = &noopFlowCreatorHelper{}

var noopFlowCreatorHelperPool = sync.Pool{
	New: func() interface{} {
		return &noopFlowCreatorHelper{
			inboundStreams: make(map[execinfrapb.StreamID]struct{}),
		}
	},
}

func newNoopFlowCreatorHelper() *noopFlowCreatorHelper {
	return noopFlowCreatorHelperPool.Get().(*noopFlowCreatorHelper)
}

func (r *noopFlowCreatorHelper) addStreamEndpoint(
	streamID execinfrapb.StreamID, _ *colrpc.Inbox, _ *sync.WaitGroup,
) {
	r.inboundStreams[streamID] = struct{}{}
}

func (r *noopFlowCreatorHelper) checkInboundStreamID(sid execinfrapb.StreamID) error {
	if _, found := r.inboundStreams[sid]; found {
		return errors.Errorf("inbound stream %d already exists in map", sid)
	}
	return nil
}

func (r *noopFlowCreatorHelper) accumulateAsyncComponent(runFn) {}

func (r *noopFlowCreatorHelper) addFlowCoordinator(coordinator *FlowCoordinator) {}

func (r *noopFlowCreatorHelper) getFlowCtxDone() <-chan struct{} {
	return nil
}

func (r *noopFlowCreatorHelper) getCancelFlowFn() context.CancelFunc {
	return nil
}

func (r *noopFlowCreatorHelper) Release() {
	for k := range r.inboundStreams {
		delete(r.inboundStreams, k)
	}
	noopFlowCreatorHelperPool.Put(r)
}

// IsSupported returns whether a flow specified by spec can be vectorized.
func IsSupported(mode sessiondatapb.VectorizeExecMode, spec *execinfrapb.FlowSpec) error {
	for pIdx := range spec.Processors {
		if err := colbuilder.IsSupported(mode, &spec.Processors[pIdx]); err != nil {
			return err
		}
		for _, procOutput := range spec.Processors[pIdx].Output {
			switch procOutput.Type {
			case execinfrapb.OutputRouterSpec_PASS_THROUGH,
				execinfrapb.OutputRouterSpec_BY_HASH:
			default:
				return errors.New("only pass-through and hash routers are supported")
			}
		}
	}
	return nil
}
