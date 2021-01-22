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
	"math"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colflow/colrpc"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/optional"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
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
) (context.Context, error) {
	var err error
	ctx, err = f.FlowBase.Setup(ctx, spec, opt)
	if err != nil {
		return ctx, err
	}
	if log.V(1) {
		log.Infof(ctx, "setting up vectorize flow %s", f.ID.Short())
	}
	recordingStats := false
	if sp := tracing.SpanFromContext(ctx); sp != nil && sp.IsVerbose() {
		recordingStats = true
	}
	helper := newVectorizedFlowCreatorHelper(f.FlowBase)

	diskQueueCfg := colcontainer.DiskQueueCfg{
		FS:        f.Cfg.TempFS,
		GetPather: f,
	}
	if err := diskQueueCfg.EnsureDefaults(); err != nil {
		return ctx, err
	}
	f.countingSemaphore = newCountingSemaphore(f.Cfg.VecFDSemaphore, f.Cfg.Metrics.VecOpenFDs)
	flowCtx := f.GetFlowCtx()
	f.creator = newVectorizedFlowCreator(
		helper,
		vectorizedRemoteComponentCreator{},
		recordingStats,
		f.Gateway,
		f.GetWaitGroup(),
		f.GetSyncFlowConsumer(),
		flowCtx.Cfg.NodeDialer,
		f.GetID(),
		diskQueueCfg,
		f.countingSemaphore,
		flowCtx.TypeResolverFactory.NewTypeResolver(flowCtx.EvalCtx.Txn),
	)
	if f.testingKnobs.onSetupFlow != nil {
		f.testingKnobs.onSetupFlow(f.creator)
	}
	_, err = f.creator.setupFlow(ctx, flowCtx, spec.Processors, f.GetLocalProcessors(), opt)
	if err == nil {
		f.testingInfo.numClosers = f.creator.numClosers
		f.testingInfo.numClosed = &f.creator.numClosed
		if log.V(1) {
			log.Info(ctx, "vectorized flow setup succeeded")
		}
		return ctx, nil
	}
	// It is (theoretically) possible that some of the memory monitoring
	// infrastructure was created even in case of an error, and we need to clean
	// that up.
	f.creator.cleanup(ctx)
	f.creator.Release()
	if log.V(1) {
		log.Infof(ctx, "failed to vectorize: %s", err)
	}
	return ctx, err
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
		colexecerror.InternalError(errors.Errorf("unable to create temporary storage directory: %v", err))
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

	if f.Cfg.TestingKnobs.CheckVectorizedFlowIsClosedCorrectly {
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
// colexec.VectorizedStatsCollectorBase that wraps op and connects the newly
// created wrapper with those corresponding to operators in inputs (the latter
// must have already been wrapped).
func (s *vectorizedFlowCreator) wrapWithVectorizedStatsCollectorBase(
	op colexecbase.Operator,
	kvReader execinfra.KVReader,
	inputs []colexecbase.Operator,
	component execinfrapb.ComponentID,
	monitors []*mon.BytesMonitor,
) (colexec.VectorizedStatsCollector, error) {
	inputWatch := timeutil.NewStopWatch()
	var memMonitors, diskMonitors []*mon.BytesMonitor
	for _, m := range monitors {
		if m.Resource() == mon.DiskResource {
			diskMonitors = append(diskMonitors, m)
		} else {
			memMonitors = append(memMonitors, m)
		}
	}
	inputStatsCollectors := make([]colexec.ChildStatsCollector, len(inputs))
	for i, input := range inputs {
		sc, ok := input.(colexec.ChildStatsCollector)
		if !ok {
			return nil, errors.New("unexpectedly an input is not collecting stats")
		}
		inputStatsCollectors[i] = sc
	}
	vsc := colexec.NewVectorizedStatsCollector(
		op, kvReader, component, inputWatch,
		memMonitors, diskMonitors, inputStatsCollectors,
	)
	s.vectorizedStatsCollectorsQueue = append(s.vectorizedStatsCollectorsQueue, vsc)
	return vsc, nil
}

// wrapWithNetworkVectorizedStatsCollector creates a new
// colexec.NetworkVectorizedStatsCollector that wraps op.
func (s *vectorizedFlowCreator) wrapWithNetworkVectorizedStatsCollector(
	inbox *colrpc.Inbox, component execinfrapb.ComponentID, latency time.Duration,
) (colexec.VectorizedStatsCollector, error) {
	inputWatch := timeutil.NewStopWatch()
	op := colexecbase.Operator(inbox)
	networkReader := colexec.NetworkReader(inbox)
	nvsc := colexec.NewNetworkVectorizedStatsCollector(op, component, inputWatch, networkReader, latency)
	s.vectorizedStatsCollectorsQueue = append(s.vectorizedStatsCollectorsQueue, nvsc)
	return nvsc, nil
}

// finishVectorizedStatsCollectors finishes the given stats collectors and
// outputs their stats to the trace contained in the ctx's span.
func finishVectorizedStatsCollectors(
	ctx context.Context, vectorizedStatsCollectors []colexec.VectorizedStatsCollector,
) {
	for _, vsc := range vectorizedStatsCollectors {
		vsc.OutputStats(ctx)
	}
}

type runFn func(context.Context, context.CancelFunc)

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
	// addMaterializer adds a materializer to the flow.
	addMaterializer(*colexec.Materializer)
	// getCancelFlowFn returns a flow cancellation function.
	getCancelFlowFn() context.CancelFunc
}

// opDAGWithMetaSources is a helper struct that stores an operator DAG as well
// as the metadataSources and closers in this DAG that need to be drained and
// closed.
type opDAGWithMetaSources struct {
	rootOperator    colexecbase.Operator
	metadataSources []execinfrapb.MetadataSource
	toClose         []colexecbase.Closer
}

// remoteComponentCreator is an interface that abstracts the constructors for
// several components in a remote flow. Mostly for testing purposes.
type remoteComponentCreator interface {
	newOutbox(
		allocator *colmem.Allocator,
		input colexecbase.Operator,
		typs []*types.T,
		metadataSources []execinfrapb.MetadataSource,
		toClose []colexecbase.Closer,
	) (*colrpc.Outbox, error)
	newInbox(ctx context.Context, allocator *colmem.Allocator, typs []*types.T, streamID execinfrapb.StreamID) (*colrpc.Inbox, error)
}

type vectorizedRemoteComponentCreator struct{}

func (vectorizedRemoteComponentCreator) newOutbox(
	allocator *colmem.Allocator,
	input colexecbase.Operator,
	typs []*types.T,
	metadataSources []execinfrapb.MetadataSource,
	toClose []colexecbase.Closer,
) (*colrpc.Outbox, error) {
	return colrpc.NewOutbox(allocator, input, typs, metadataSources, toClose)
}

func (vectorizedRemoteComponentCreator) newInbox(
	ctx context.Context, allocator *colmem.Allocator, typs []*types.T, streamID execinfrapb.StreamID,
) (*colrpc.Inbox, error) {
	return colrpc.NewInbox(ctx, allocator, typs, streamID)
}

// vectorizedFlowCreator performs all the setup of vectorized flows. Depending
// on embedded flowCreatorHelper, it can either do the actual setup in order
// to run the flow or do the setup needed to check that the flow is supported
// through the vectorized engine.
type vectorizedFlowCreator struct {
	flowCreatorHelper
	remoteComponentCreator

	streamIDToInputOp              map[execinfrapb.StreamID]opDAGWithMetaSources
	streamIDToSpecIdx              map[execinfrapb.StreamID]int
	recordingStats                 bool
	isGatewayNode                  bool
	vectorizedStatsCollectorsQueue []colexec.VectorizedStatsCollector
	waitGroup                      *sync.WaitGroup
	syncFlowConsumer               execinfra.RowReceiver
	nodeDialer                     *nodedialer.Dialer
	flowID                         execinfrapb.FlowID
	exprHelper                     *colexec.ExprHelper
	typeResolver                   descs.DistSQLTypeResolver

	// numOutboxes counts how many exec.Outboxes have been set up on this node.
	// It must be accessed atomically.
	numOutboxes       int32
	materializerAdded bool

	// numOutboxesExited is an atomic that keeps track of how many outboxes have exited.
	// When numOutboxesExited equals numOutboxes, the cancellation function for the flow
	// is called.
	numOutboxesExited int32
	// numOutboxesDrained is an atomic that keeps track of how many outboxes have
	// been drained. When numOutboxesDrained equals numOutboxes, flow-level metadata is
	// added to a flow-level span.
	numOutboxesDrained int32

	// procIdxQueue is a queue of indices into processorSpecs (the argument to
	// setupFlow), for topologically ordered processing.
	procIdxQueue []int
	// leaves accumulates all operators that have no further outputs on the
	// current node, for the purposes of EXPLAIN output.
	leaves []execinfra.OpNode
	// operatorConcurrency is set if any operators are executed in parallel.
	operatorConcurrency bool
	// monitors contains all monitors (for both memory and disk usage) of the
	// components in the vectorized flow.
	monitors []*mon.BytesMonitor
	// accounts contains all monitors (for both memory and disk usage) of the
	// components in the vectorized flow.
	accounts []*mon.BoundAccount
	// releasables contains all components that should be released back to their
	// pools during the flow cleanup.
	releasables []execinfra.Releasable

	diskQueueCfg colcontainer.DiskQueueCfg
	fdSemaphore  semaphore.Semaphore

	// numClosers and numClosed are used to assert during testing that the
	// expected number of components are closed.
	numClosers int32
	numClosed  int32

	inputsScratch []colexecbase.Operator
}

var _ execinfra.Releasable = &vectorizedFlowCreator{}

var vectorizedFlowCreatorPool = sync.Pool{
	New: func() interface{} {
		return &vectorizedFlowCreator{
			streamIDToInputOp: make(map[execinfrapb.StreamID]opDAGWithMetaSources),
			streamIDToSpecIdx: make(map[execinfrapb.StreamID]int),
			exprHelper:        colexec.NewExprHelper(),
		}
	},
}

func newVectorizedFlowCreator(
	helper flowCreatorHelper,
	componentCreator remoteComponentCreator,
	recordingStats bool,
	isGatewayNode bool,
	waitGroup *sync.WaitGroup,
	syncFlowConsumer execinfra.RowReceiver,
	nodeDialer *nodedialer.Dialer,
	flowID execinfrapb.FlowID,
	diskQueueCfg colcontainer.DiskQueueCfg,
	fdSemaphore semaphore.Semaphore,
	typeResolver descs.DistSQLTypeResolver,
) *vectorizedFlowCreator {
	creator := vectorizedFlowCreatorPool.Get().(*vectorizedFlowCreator)
	*creator = vectorizedFlowCreator{
		flowCreatorHelper:              helper,
		remoteComponentCreator:         componentCreator,
		streamIDToInputOp:              creator.streamIDToInputOp,
		streamIDToSpecIdx:              creator.streamIDToSpecIdx,
		recordingStats:                 recordingStats,
		vectorizedStatsCollectorsQueue: creator.vectorizedStatsCollectorsQueue,
		waitGroup:                      waitGroup,
		syncFlowConsumer:               syncFlowConsumer,
		nodeDialer:                     nodeDialer,
		flowID:                         flowID,
		exprHelper:                     creator.exprHelper,
		typeResolver:                   typeResolver,
		procIdxQueue:                   creator.procIdxQueue,
		leaves:                         creator.leaves,
		monitors:                       creator.monitors,
		accounts:                       creator.accounts,
		releasables:                    creator.releasables,
		diskQueueCfg:                   diskQueueCfg,
		fdSemaphore:                    fdSemaphore,
		inputsScratch:                  creator.inputsScratch,
	}
	return creator
}

func (s *vectorizedFlowCreator) cleanup(ctx context.Context) {
	for _, acc := range s.accounts {
		acc.Close(ctx)
	}
	for _, mon := range s.monitors {
		mon.Stop(ctx)
	}
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
	*s = vectorizedFlowCreator{
		streamIDToInputOp:              s.streamIDToInputOp,
		streamIDToSpecIdx:              s.streamIDToSpecIdx,
		vectorizedStatsCollectorsQueue: s.vectorizedStatsCollectorsQueue[:0],
		exprHelper:                     s.exprHelper,
		procIdxQueue:                   s.procIdxQueue[:0],
		leaves:                         s.leaves[:0],
		monitors:                       s.monitors[:0],
		accounts:                       s.accounts[:0],
		releasables:                    s.releasables[:0],
		inputsScratch:                  s.inputsScratch[:0],
	}
	vectorizedFlowCreatorPool.Put(s)
}

// createBufferingUnlimitedMemMonitor instantiates an unlimited memory monitor.
// These should only be used when spilling to disk and an operator is made aware
// of a memory usage limit separately.
// The receiver is updated to have a reference to the unlimited memory monitor.
// TODO(asubiotto): This identical to the helper function in
//  NewColOperatorResult, meaning that we should probably find a way to refactor
//  this.
func (s *vectorizedFlowCreator) createBufferingUnlimitedMemMonitor(
	ctx context.Context, flowCtx *execinfra.FlowCtx, name string,
) *mon.BytesMonitor {
	bufferingOpUnlimitedMemMonitor := execinfra.NewMonitor(
		ctx, flowCtx.EvalCtx.Mon, name+"-unlimited",
	)
	s.monitors = append(s.monitors, bufferingOpUnlimitedMemMonitor)
	return bufferingOpUnlimitedMemMonitor
}

// createDiskAccounts instantiates an unlimited disk monitor and disk accounts
// to be used for disk spilling infrastructure in vectorized engine.
// TODO(azhng): consolidate all allocation monitors/account management into one
// place after branch cut for 20.1.
func (s *vectorizedFlowCreator) createDiskAccounts(
	ctx context.Context, flowCtx *execinfra.FlowCtx, name string, numAccounts int,
) (*mon.BytesMonitor, []*mon.BoundAccount) {
	diskMonitor := execinfra.NewMonitor(ctx, flowCtx.Cfg.DiskMonitor, name)
	s.monitors = append(s.monitors, diskMonitor)
	diskAccounts := make([]*mon.BoundAccount, numAccounts)
	for i := range diskAccounts {
		diskAcc := diskMonitor.MakeBoundAccount()
		diskAccounts[i] = &diskAcc
	}
	s.accounts = append(s.accounts, diskAccounts...)
	return diskMonitor, diskAccounts
}

// newStreamingMemAccount creates a new memory account bound to the monitor in
// flowCtx and accumulates it into streamingMemAccounts slice.
func (s *vectorizedFlowCreator) newStreamingMemAccount(
	flowCtx *execinfra.FlowCtx,
) *mon.BoundAccount {
	streamingMemAccount := flowCtx.EvalCtx.Mon.MakeBoundAccount()
	s.accounts = append(s.accounts, &streamingMemAccount)
	return &streamingMemAccount
}

// setupRemoteOutputStream sets up an Outbox that will operate according to
// the given StreamEndpointSpec. It will also drain all MetadataSources in the
// metadataSourcesQueue.
// NOTE: The caller must not reuse the metadataSourcesQueue and toClose.
func (s *vectorizedFlowCreator) setupRemoteOutputStream(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	op colexecbase.Operator,
	outputTyps []*types.T,
	stream *execinfrapb.StreamEndpointSpec,
	metadataSourcesQueue []execinfrapb.MetadataSource,
	toClose []colexecbase.Closer,
	factory coldata.ColumnFactory,
) (execinfra.OpNode, error) {
	outbox, err := s.remoteComponentCreator.newOutbox(
		colmem.NewAllocator(ctx, s.newStreamingMemAccount(flowCtx), factory),
		op, outputTyps, metadataSourcesQueue, toClose,
	)
	if err != nil {
		return nil, err
	}

	atomic.AddInt32(&s.numOutboxes, 1)
	run := func(ctx context.Context, cancelFn context.CancelFunc) {
		// cancelFn is the cancellation function of the context of the whole
		// flow, and we want to call it only when the last outbox exits, so we
		// derive a separate child context for each outbox.
		var outboxCancelFn context.CancelFunc
		ctx, outboxCancelFn = context.WithCancel(ctx)
		outbox.Run(
			ctx,
			s.nodeDialer,
			stream.TargetNodeID,
			s.flowID,
			stream.StreamID,
			outboxCancelFn,
			flowinfra.SettingFlowStreamTimeout.Get(&flowCtx.Cfg.Settings.SV),
		)
		// When the last Outbox on this node exits, we want to make sure that
		// everything is shutdown; namely, we need to call cancelFn if:
		// - it is the last Outbox
		// - there is no root materializer on this node (if it were, it would take
		// care of the cancellation itself)
		// - cancelFn is non-nil (it can be nil in tests).
		// Calling cancelFn will cancel the context that all infrastructure on this
		// node is listening on, so it will shut everything down.
		if atomic.AddInt32(&s.numOutboxesExited, 1) == atomic.LoadInt32(&s.numOutboxes) && !s.materializerAdded && cancelFn != nil {
			cancelFn()
		}
	}
	s.accumulateAsyncComponent(run)
	return outbox, nil
}

// setupRouter sets up a vectorized hash router according to the output router
// spec. If the outputs are local, these are added to s.streamIDToInputOp to be
// used as inputs in further planning. metadataSourcesQueue is passed along to
// any outboxes created to be drained, or stored in streamIDToInputOp for any
// local outputs to pass that responsibility along. In any case,
// metadataSourcesQueue will always be fully consumed.
// NOTE: This method supports only BY_HASH routers. Callers should handle
// PASS_THROUGH routers separately.
// NOTE: The caller must not reuse the metadataSourcesQueue and toClose.
func (s *vectorizedFlowCreator) setupRouter(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	input colexecbase.Operator,
	outputTyps []*types.T,
	output *execinfrapb.OutputRouterSpec,
	metadataSourcesQueue []execinfrapb.MetadataSource,
	toClose []colexecbase.Closer,
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

	hashRouterMemMonitor := s.createBufferingUnlimitedMemMonitor(ctx, flowCtx, mmName)
	allocators := make([]*colmem.Allocator, len(output.Streams))
	for i := range allocators {
		acc := hashRouterMemMonitor.MakeBoundAccount()
		allocators[i] = colmem.NewAllocator(ctx, &acc, factory)
		s.accounts = append(s.accounts, &acc)
	}
	limit := execinfra.GetWorkMemLimit(flowCtx.Cfg)
	if flowCtx.Cfg.TestingKnobs.ForceDiskSpill {
		limit = 1
	}
	diskMon, diskAccounts := s.createDiskAccounts(ctx, flowCtx, mmName, len(output.Streams))
	router, outputs := colexec.NewHashRouter(allocators, input, outputTyps, output.HashColumns, limit, s.diskQueueCfg, s.fdSemaphore, diskAccounts, metadataSourcesQueue, toClose)
	runRouter := func(ctx context.Context, _ context.CancelFunc) {
		router.Run(logtags.AddTag(ctx, "hashRouterID", strings.Join(streamIDs, ",")))
	}
	s.accumulateAsyncComponent(runRouter)

	foundLocalOutput := false
	for i, op := range outputs {
		stream := &output.Streams[i]
		switch stream.Type {
		case execinfrapb.StreamEndpointSpec_SYNC_RESPONSE:
			return errors.Errorf("unexpected sync response output when setting up router")
		case execinfrapb.StreamEndpointSpec_REMOTE:
			// Note that here we pass in nil 'toClose' slice because hash
			// router is responsible for closing all of the idempotent closers.
			if _, err := s.setupRemoteOutputStream(
				ctx, flowCtx, op, outputTyps, stream, []execinfrapb.MetadataSource{op}, nil /* toClose */, factory,
			); err != nil {
				return err
			}
		case execinfrapb.StreamEndpointSpec_LOCAL:
			foundLocalOutput = true
			localOp := colexecbase.Operator(op)
			if s.recordingStats {
				mons := []*mon.BytesMonitor{hashRouterMemMonitor, diskMon}
				// Wrap local outputs with vectorized stats collectors when recording
				// stats. This is mostly for compatibility but will provide some useful
				// information (e.g. output stall time).
				var err error
				localOp, err = s.wrapWithVectorizedStatsCollectorBase(
					op, nil /* kvReader */, nil, /* inputs */
					flowCtx.StreamComponentID(stream.StreamID), mons,
				)
				if err != nil {
					return err
				}
			}
			s.streamIDToInputOp[stream.StreamID] = opDAGWithMetaSources{
				rootOperator:    localOp,
				metadataSources: []execinfrapb.MetadataSource{op},
				// toClose will be closed by the HashRouter.
				toClose: nil,
			}
		}
	}
	if !foundLocalOutput {
		// No local output means that our router is a leaf node.
		s.leaves = append(s.leaves, router)
	}
	return nil
}

// setupInput sets up one or more input operators (local or remote) and a
// synchronizer to expose these separate streams as one exec.Operator which is
// returned. If s.recordingStats is true, these inputs and synchronizer are
// wrapped in stats collectors if not done so, although these stats are not
// exposed as of yet. Inboxes that are created are also returned as
// []distqlpb.MetadataSource so that any remote metadata can be read through
// calling DrainMeta.
func (s *vectorizedFlowCreator) setupInput(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	input execinfrapb.InputSyncSpec,
	opt flowinfra.FuseOpt,
	factory coldata.ColumnFactory,
) (colexecbase.Operator, []execinfrapb.MetadataSource, []colexecbase.Closer, error) {
	inputStreamOps := make([]colexec.SynchronizerInput, 0, len(input.Streams))
	// Before we can safely use types we received over the wire in the
	// operators, we need to make sure they are hydrated. In row execution
	// engine it is done during the processor initialization, but operators
	// don't do that.
	if err := s.typeResolver.HydrateTypeSlice(ctx, input.ColumnTypes); err != nil {
		return nil, nil, nil, err
	}

	for _, inputStream := range input.Streams {
		switch inputStream.Type {
		case execinfrapb.StreamEndpointSpec_LOCAL:
			in := s.streamIDToInputOp[inputStream.StreamID]
			inputStreamOps = append(inputStreamOps, colexec.SynchronizerInput{
				Op:              in.rootOperator,
				MetadataSources: in.metadataSources,
				ToClose:         in.toClose,
			})
		case execinfrapb.StreamEndpointSpec_REMOTE:
			// If the input is remote, the input operator does not exist in
			// streamIDToInputOp. Create an inbox.
			if err := s.checkInboundStreamID(inputStream.StreamID); err != nil {
				return nil, nil, nil, err
			}

			latency, err := s.nodeDialer.Latency(inputStream.TargetNodeID)
			if err != nil {
				// If an error occurred, latency's nil value of 0 is used. If latency is
				// 0, it is not included in the displayed stats for EXPLAIN ANALYZE
				// diagrams.
				latency = 0
				if log.V(1) {
					log.Infof(ctx, "an error occurred during vectorized planning while getting latency: %v", err)
				}
			}

			inbox, err := s.remoteComponentCreator.newInbox(
				ctx, colmem.NewAllocator(ctx, s.newStreamingMemAccount(flowCtx), factory), input.ColumnTypes, inputStream.StreamID,
			)

			if err != nil {
				return nil, nil, nil, err
			}
			s.addStreamEndpoint(inputStream.StreamID, inbox, s.waitGroup)
			op := colexecbase.Operator(inbox)
			if s.recordingStats {
				op, err = s.wrapWithNetworkVectorizedStatsCollector(
					inbox, flowCtx.StreamComponentID(inputStream.StreamID), latency,
				)
				if err != nil {
					return nil, nil, nil, err
				}
			}
			inputStreamOps = append(inputStreamOps, colexec.SynchronizerInput{Op: op, MetadataSources: []execinfrapb.MetadataSource{inbox}})
		default:
			return nil, nil, nil, errors.Errorf("unsupported input stream type %s", inputStream.Type)
		}
	}
	op := inputStreamOps[0].Op
	metaSources := inputStreamOps[0].MetadataSources
	toClose := inputStreamOps[0].ToClose
	if len(inputStreamOps) > 1 {
		statsInputs := inputStreamOps
		if input.Type == execinfrapb.InputSyncSpec_ORDERED {
			os, err := colexec.NewOrderedSynchronizer(
				colmem.NewAllocator(ctx, s.newStreamingMemAccount(flowCtx), factory),
				inputStreamOps, input.ColumnTypes, execinfrapb.ConvertToColumnOrdering(input.Ordering),
			)
			if err != nil {
				return nil, nil, nil, err
			}
			op = os
			metaSources = []execinfrapb.MetadataSource{os}
			toClose = []colexecbase.Closer{os}
		} else {
			if opt == flowinfra.FuseAggressively {
				sync := colexec.NewSerialUnorderedSynchronizer(inputStreamOps)
				op = sync
				metaSources = []execinfrapb.MetadataSource{sync}
				toClose = []colexecbase.Closer{sync}
			} else {
				sync := colexec.NewParallelUnorderedSynchronizer(inputStreamOps, s.waitGroup)
				op = sync
				metaSources = []execinfrapb.MetadataSource{sync}
				// toClose is set to nil because the ParallelUnorderedSynchronizer takes
				// care of closing these components itself since they need to be closed
				// from the same goroutine as Next.
				toClose = nil
				s.operatorConcurrency = true
			}
			// Don't use the unordered synchronizer's inputs for stats collection
			// given that they run concurrently. The stall time will be collected
			// instead.
			statsInputs = nil
		}
		if s.recordingStats {
			statsInputsAsOps := make([]colexecbase.Operator, len(statsInputs))
			for i := range statsInputs {
				statsInputsAsOps[i] = statsInputs[i].Op
			}
			// TODO(asubiotto): Once we have IDs for synchronizers, plumb them into
			// this stats collector to display stats.
			var err error
			op, err = s.wrapWithVectorizedStatsCollectorBase(
				op, nil /* kvReader */, statsInputsAsOps, execinfrapb.ComponentID{}, nil, /* monitors */
			)
			if err != nil {
				return nil, nil, nil, err
			}
		}
	}
	return op, metaSources, toClose, nil
}

// setupOutput sets up any necessary infrastructure according to the output
// spec of pspec. The metadataSourcesQueue and toClose slices are fully consumed
// by either passing them to an outbox or HashRouter to be drained/closed, or
// storing them in streamIDToInputOp with the given op to be processed later.
// NOTE: The caller must not reuse the metadataSourcesQueue and toClose.
func (s *vectorizedFlowCreator) setupOutput(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	pspec *execinfrapb.ProcessorSpec,
	op colexecbase.Operator,
	opOutputTypes []*types.T,
	metadataSourcesQueue []execinfrapb.MetadataSource,
	toClose []colexecbase.Closer,
	factory coldata.ColumnFactory,
) error {
	output := &pspec.Output[0]
	if output.Type != execinfrapb.OutputRouterSpec_PASS_THROUGH {
		return s.setupRouter(
			ctx,
			flowCtx,
			op,
			opOutputTypes,
			output,
			metadataSourcesQueue,
			toClose,
			factory,
		)
	}

	if len(output.Streams) != 1 {
		return errors.Errorf("unsupported multi outputstream proc (%d streams)", len(output.Streams))
	}
	outputStream := &output.Streams[0]
	switch outputStream.Type {
	case execinfrapb.StreamEndpointSpec_LOCAL:
		s.streamIDToInputOp[outputStream.StreamID] = opDAGWithMetaSources{
			rootOperator: op, metadataSources: metadataSourcesQueue, toClose: toClose,
		}
	case execinfrapb.StreamEndpointSpec_REMOTE:
		// Set up an Outbox.
		if s.recordingStats {
			// If recording stats, we add a metadata source that will generate all
			// stats data as metadata for the stats collectors created so far.
			vscs := append([]colexec.VectorizedStatsCollector(nil), s.vectorizedStatsCollectorsQueue...)
			s.vectorizedStatsCollectorsQueue = s.vectorizedStatsCollectorsQueue[:0]
			metadataSourcesQueue = append(
				metadataSourcesQueue,
				execinfrapb.CallbackMetadataSource{
					DrainMetaCb: func(ctx context.Context) []execinfrapb.ProducerMetadata {
						// Start a separate recording so that GetRecording will return
						// the recordings for only the child spans containing stats.
						ctx, span := tracing.ChildSpanRemote(ctx, "")
						if atomic.AddInt32(&s.numOutboxesDrained, 1) == atomic.LoadInt32(&s.numOutboxes) && !s.isGatewayNode {
							// At the last outbox, we can accurately retrieve stats for the
							// whole flow from parent monitors. These stats are added to a
							// flow-level span.
							span.SetTag(execinfrapb.FlowIDTagKey, flowCtx.ID)
							span.SetSpanStats(&execinfrapb.ComponentStats{
								Component: execinfrapb.FlowComponentID(outputStream.OriginNodeID, flowCtx.ID),
								FlowStats: execinfrapb.FlowStats{
									MaxMemUsage: optional.MakeUint(uint64(flowCtx.EvalCtx.Mon.MaximumBytes())),
								},
							})
						}
						finishVectorizedStatsCollectors(ctx, vscs)
						span.Finish()
						return []execinfrapb.ProducerMetadata{{TraceData: span.GetRecording()}}
					},
				},
			)
		}
		outbox, err := s.setupRemoteOutputStream(
			ctx, flowCtx, op, opOutputTypes, outputStream, metadataSourcesQueue, toClose, factory,
		)
		if err != nil {
			return err
		}
		// An outbox is a leaf: there's nothing that sees it as an input on this
		// node.
		s.leaves = append(s.leaves, outbox)
	case execinfrapb.StreamEndpointSpec_SYNC_RESPONSE:
		// Make the materializer, which will write to the given receiver.
		var outputStatsToTrace func() *execinfrapb.ComponentStats
		if s.recordingStats {
			// Make a copy given that vectorizedStatsCollectorsQueue is reset and
			// appended to.
			vscq := append([]colexec.VectorizedStatsCollector(nil), s.vectorizedStatsCollectorsQueue...)
			outputStatsToTrace = func() *execinfrapb.ComponentStats {
				// TODO(radu): this is a sketchy way to use this infrastructure. We
				// aren't actually returning any stats, but we are creating and closing
				// child spans with stats.
				finishVectorizedStatsCollectors(ctx, vscq)
				return nil
			}
		}
		proc, err := colexec.NewMaterializer(
			flowCtx,
			pspec.ProcessorID,
			op,
			opOutputTypes,
			s.syncFlowConsumer,
			metadataSourcesQueue,
			toClose,
			outputStatsToTrace,
			s.getCancelFlowFn,
		)
		if err != nil {
			return err
		}
		s.vectorizedStatsCollectorsQueue = s.vectorizedStatsCollectorsQueue[:0]
		// A materializer is a leaf.
		s.leaves = append(s.leaves, proc)
		s.addMaterializer(proc)
		s.materializerAdded = true
	default:
		return errors.Errorf("unsupported output stream type %s", outputStream.Type)
	}
	return nil
}

func (s *vectorizedFlowCreator) setupFlow(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorSpecs []execinfrapb.ProcessorSpec,
	localProcessors []execinfra.LocalProcessor,
	opt flowinfra.FuseOpt,
) (leaves []execinfra.OpNode, err error) {
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

			// metadataSourcesQueue contains all the MetadataSources that need to be
			// drained. If in a given loop iteration no component that can drain
			// metadata from these sources is found, the metadataSourcesQueue should be
			// added as part of one of the last unconnected inputDAGs in
			// streamIDToInputOp. This is to avoid cycles.
			var metadataSourcesQueue []execinfrapb.MetadataSource
			// toClose is similar to metadataSourcesQueue with the difference that these
			// components do not produce metadata and should be Closed even during
			// non-graceful termination.
			var toClose []colexecbase.Closer
			inputs := s.inputsScratch[:0]
			for i := range pspec.Input {
				input, metadataSources, closers, localErr := s.setupInput(ctx, flowCtx, pspec.Input[i], opt, factory)
				if localErr != nil {
					err = localErr
					return
				}
				metadataSourcesQueue = append(metadataSourcesQueue, metadataSources...)
				toClose = append(toClose, closers...)
				inputs = append(inputs, input)
			}

			// Before we can safely use types we received over the wire in the
			// operators, we need to make sure they are hydrated.
			if err = s.typeResolver.HydrateTypeSlice(ctx, pspec.ResultTypes); err != nil {
				return
			}

			args := &colexec.NewColOperatorArgs{
				Spec:                 pspec,
				Inputs:               inputs,
				StreamingMemAccount:  s.newStreamingMemAccount(flowCtx),
				ProcessorConstructor: rowexec.NewProcessor,
				LocalProcessors:      localProcessors,
				DiskQueueCfg:         s.diskQueueCfg,
				FDSemaphore:          s.fdSemaphore,
				ExprHelper:           s.exprHelper,
				Factory:              factory,
			}
			var result *colexec.NewColOperatorResult
			result, err = colbuilder.NewColOperator(ctx, flowCtx, args)
			if result != nil {
				// Even when err is non-nil, it is possible that the buffering memory
				// monitor and account have been created, so we always want to accumulate
				// them for a proper cleanup.
				s.monitors = append(s.monitors, result.OpMonitors...)
				s.accounts = append(s.accounts, result.OpAccounts...)
				s.releasables = append(s.releasables, result)
			}
			if err != nil {
				err = errors.Wrapf(err, "unable to vectorize execution plan")
				return
			}
			if flowCtx.Cfg != nil && flowCtx.Cfg.TestingKnobs.EnableVectorizedInvariantsChecker {
				result.Op = colexec.NewInvariantsChecker(result.Op)
			}
			if flowCtx.EvalCtx.SessionData.TestingVectorizeInjectPanics {
				result.Op = colexec.NewPanicInjector(result.Op)
			}
			metadataSourcesQueue = append(metadataSourcesQueue, result.MetadataSources...)
			if flowCtx.Cfg != nil && flowCtx.Cfg.TestingKnobs.CheckVectorizedFlowIsClosedCorrectly {
				for _, closer := range result.ToClose {
					func(c colexecbase.Closer) {
						closed := false
						toClose = append(toClose, &colexec.CallbackCloser{CloseCb: func(ctx context.Context) error {
							if !closed {
								closed = true
								atomic.AddInt32(&s.numClosed, 1)
							}
							return c.Close(ctx)
						}})
					}(closer)
				}
				s.numClosers += int32(len(result.ToClose))
			} else {
				toClose = append(toClose, result.ToClose...)
			}

			op := result.Op
			if s.recordingStats {
				// Note: if the original op is a Columnarizer, this will result in two
				// sets of stats for the same processor. The code that processes stats
				// is prepared to union the stats.
				// TODO(radu): find a way to clean this up.
				op, err = s.wrapWithVectorizedStatsCollectorBase(
					op, result.KVReader, inputs, flowCtx.ProcessorComponentID(pspec.ProcessorID),
					result.OpMonitors,
				)
				if err != nil {
					return
				}
			}

			if err = s.setupOutput(
				ctx, flowCtx, pspec, op, result.ColumnTypes, metadataSourcesQueue, toClose, factory,
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
		if len(s.vectorizedStatsCollectorsQueue) > 0 {
			colexecerror.InternalError(errors.AssertionFailedf("not all vectorized stats collectors have been processed"))
		}
	}); vecErr != nil {
		return s.leaves, vecErr
	}
	return s.leaves, err
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
		flowinfra.StartableFn(func(ctx context.Context, wg *sync.WaitGroup, cancelFn context.CancelFunc) {
			if wg != nil {
				wg.Add(1)
			}
			go func() {
				run(ctx, cancelFn)
				if wg != nil {
					wg.Done()
				}
			}()
		}))
}

func (r *vectorizedFlowCreatorHelper) addMaterializer(m *colexec.Materializer) {
	r.processors = append(r.processors, m)
	r.f.SetProcessors(r.processors)
}

func (r *vectorizedFlowCreatorHelper) getCancelFlowFn() context.CancelFunc {
	return r.f.GetCancelFlowFn()
}

func (r *vectorizedFlowCreatorHelper) Release() {
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

func (r *noopFlowCreatorHelper) addMaterializer(*colexec.Materializer) {}

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
	}
	return nil
}

// ConvertToVecTree converts the flow to a tree of vectorized operators
// returning a list of the leap operators or an error if the flow vectorization
// is not supported. Note that it does so by setting up the full flow without
// running the components asynchronously, so it is pretty expensive.
// It also returns a non-nil cleanup function that releases all
// execinfra.Releasable objects which can *only* be performed once leaves are
// no longer needed.
func ConvertToVecTree(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	flow *execinfrapb.FlowSpec,
	localProcessors []execinfra.LocalProcessor,
	isPlanLocal bool,
) (leaves []execinfra.OpNode, cleanup func(), err error) {
	if !isPlanLocal && len(localProcessors) > 0 {
		return nil, func() {}, errors.AssertionFailedf("unexpectedly non-empty LocalProcessors when plan is not local")
	}
	fuseOpt := flowinfra.FuseNormally
	if isPlanLocal {
		fuseOpt = flowinfra.FuseAggressively
	}
	creator := newVectorizedFlowCreator(
		newNoopFlowCreatorHelper(), vectorizedRemoteComponentCreator{}, false, false,
		nil, &execinfra.RowChannel{}, nil, execinfrapb.FlowID{}, colcontainer.DiskQueueCfg{},
		flowCtx.Cfg.VecFDSemaphore, flowCtx.TypeResolverFactory.NewTypeResolver(flowCtx.EvalCtx.Txn),
	)
	// We create an unlimited memory account because we're interested whether the
	// flow is supported via the vectorized engine in general (without paying
	// attention to the memory since it is node-dependent in the distributed
	// case).
	memoryMonitor := mon.NewMonitor(
		"convert-to-vec-tree",
		mon.MemoryResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment */
		math.MaxInt64, /* noteworthy */
		flowCtx.Cfg.Settings,
	)
	memoryMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
	defer memoryMonitor.Stop(ctx)
	defer creator.cleanup(ctx)
	leaves, err = creator.setupFlow(ctx, flowCtx, flow.Processors, localProcessors, fuseOpt)
	return leaves, creator.Release, err
}
