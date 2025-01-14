// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colflow

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
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
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execopnode"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execreleasable"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/execstats"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/optional"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
	"github.com/marusama/semaphore"
)

// fdCountingSemaphore is a semaphore that keeps track of the number of file
// descriptors currently used by the vectorized engine.
//
// Note that it effectively implements the execreleasable.Releasable interface
// but due to the method name conflict doesn't.
type fdCountingSemaphore struct {
	semaphore.Semaphore
	globalCount       *metric.Gauge
	count             int64
	acquireMaxRetries int
}

var fdCountingSemaphorePool = sync.Pool{
	New: func() interface{} {
		return &fdCountingSemaphore{}
	},
}

func newFDCountingSemaphore(
	sem semaphore.Semaphore, globalCount *metric.Gauge, sv *settings.Values,
) *fdCountingSemaphore {
	s := fdCountingSemaphorePool.Get().(*fdCountingSemaphore)
	s.Semaphore = sem
	s.globalCount = globalCount
	s.acquireMaxRetries = int(fdCountingSemaphoreMaxRetries.Get(sv))
	return s
}

var errAcquireTimeout = pgerror.New(
	pgcode.ConfigurationLimitExceeded,
	"acquiring of file descriptors timed out, consider increasing "+
		"COCKROACH_VEC_MAX_OPEN_FDS environment variable",
)

var fdCountingSemaphoreMaxRetries = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.distsql.acquire_vec_fds.max_retries",
	"determines the number of retries performed during the acquisition of "+
		"file descriptors needed for disk-spilling operations, set to 0 for "+
		"unlimited retries",
	8,
	settings.NonNegativeInt,
)

func (s *fdCountingSemaphore) Acquire(ctx context.Context, n int) error {
	if s.TryAcquire(n) {
		return nil
	}
	// Currently there is not enough capacity in the semaphore to acquire the
	// desired number, so we set up a retry loop that exponentially backs off,
	// until either the semaphore opens up or we time out (most likely due to a
	// deadlock).
	//
	// The latter situation is possible when multiple queries already hold some
	// of the quota and each of them needs more to proceed resulting in a
	// deadlock. We get out of such a deadlock by randomly erroring out one of
	// the queries (which would release some quota back to the semaphore) making
	// it possible for other queries to proceed.
	//
	// Note that we've already tried to acquire the quota above (which failed),
	// so the initial backoff time of 100ms seems ok (we are spilling to disk
	// after all, so the query is likely to experience significant latency). The
	// current choice of options is such that we'll spend on the order of 25s
	// in the retry loop before timing out with the default value of the
	// 'sql.distsql.acquire_vec_fds.max_retries' cluster settings.
	opts := retry.Options{
		InitialBackoff:      100 * time.Millisecond,
		Multiplier:          2.0,
		RandomizationFactor: 0.25,
		MaxRetries:          s.acquireMaxRetries,
	}
	for r := retry.StartWithCtx(ctx, opts); r.Next(); {
		if s.TryAcquire(n) {
			return nil
		}
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	log.Warning(ctx, "acquiring of file descriptors for disk-spilling timed out")
	return errAcquireTimeout
}

func (s *fdCountingSemaphore) TryAcquire(n int) bool {
	success := s.Semaphore.TryAcquire(n)
	if !success {
		return false
	}
	atomic.AddInt64(&s.count, int64(n))
	s.globalCount.Inc(int64(n))
	return success
}

func (s *fdCountingSemaphore) Release(n int) int {
	atomic.AddInt64(&s.count, int64(-n))
	s.globalCount.Dec(int64(n))
	return s.Semaphore.Release(n)
}

// ReleaseToPool should be named Release and should implement the
// execinfra.Releasable interface, but that would lead to a conflict with
// semaphore.Semaphore.Release method.
func (s *fdCountingSemaphore) ReleaseToPool() {
	if unreleased := atomic.LoadInt64(&s.count); unreleased != 0 {
		colexecerror.InternalError(errors.AssertionFailedf("unexpectedly %d count on the semaphore when releasing it to the pool", unreleased))
	}
	*s = fdCountingSemaphore{}
	fdCountingSemaphorePool.Put(s)
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
	countingSemaphore *fdCountingSemaphore

	tempStorage struct {
		syncutil.Mutex
		// path is the path to this flow's temporary storage directory. If
		// it is an empty string, then it hasn't been computed yet nor the
		// directory has been created.
		path string
	}

	testingKnobs struct {
		// onSetupFlow is a testing knob that is called before calling
		// creator.setupFlow with the given creator.
		onSetupFlow func(*vectorizedFlowCreator)
	}
}

var _ flowinfra.Flow = &vectorizedFlow{}
var _ execreleasable.Releasable = &vectorizedFlow{}

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
) (context.Context, execopnode.OpChains, error) {
	var err error
	ctx, _, err = f.FlowBase.Setup(ctx, spec, opt)
	if err != nil {
		return ctx, nil, err
	}
	log.VEvent(ctx, 2, "setting up vectorized flow")
	recordingStats := false
	if execstats.ShouldCollectStats(ctx, f.FlowCtx.CollectStats) {
		recordingStats = true
	}

	diskQueueCfg := colcontainer.DiskQueueCfg{
		FS:                  f.Cfg.TempFS,
		GetPather:           f,
		SpilledBytesWritten: f.Cfg.Metrics.SpilledBytesWritten,
		SpilledBytesRead:    f.Cfg.Metrics.SpilledBytesRead,
	}
	if err := diskQueueCfg.EnsureDefaults(); err != nil {
		return ctx, nil, err
	}
	flowCtx := f.GetFlowCtx()
	f.countingSemaphore = newFDCountingSemaphore(
		f.Cfg.VecFDSemaphore, f.Cfg.Metrics.VecOpenFDs, &flowCtx.Cfg.Settings.SV,
	)
	f.creator = newVectorizedFlowCreator(
		f.FlowBase,
		nil, /* componentCreator */
		recordingStats,
		diskQueueCfg,
		f.countingSemaphore,
	)
	if f.testingKnobs.onSetupFlow != nil {
		f.testingKnobs.onSetupFlow(f.creator)
	}
	opChains, batchFlowCoordinator, err := f.creator.setupFlow(ctx, spec.Processors, opt)
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
	f.SetStartedGoroutines(f.creator.operatorConcurrency)
	log.VEventf(ctx, 2, "vectorized flow setup succeeded")
	if !f.IsLocal() {
		// For distributed flows set opChains to nil, per the contract of
		// flowinfra.Flow.Setup.
		opChains = nil
	}
	return ctx, opChains, nil
}

// Resume is part of the Flow interface.
func (f *vectorizedFlow) Resume(recv execinfra.RowReceiver) {
	if f.batchFlowCoordinator != nil {
		// Resume is expected to be called only for pausable portals, for which
		// we must be using limitedCommandResult which currently doesn't
		// implement the execinfra.BatchReceiver interface, so we shouldn't have
		// a batch flow coordinator here.
		recv.Push(
			nil, /* row */
			&execinfrapb.ProducerMetadata{
				Err: errors.AssertionFailedf(
					"batchFlowCoordinator should be nil for vectorizedFlow",
				)})
		recv.ProducerDone()
		return
	}
	f.FlowBase.Resume(recv)
}

// Run is part of the Flow interface.
func (f *vectorizedFlow) Run(ctx context.Context, noWait bool) {
	if f.batchFlowCoordinator == nil {
		// If we didn't create a BatchFlowCoordinator, then we have a processor
		// as the root, so we run this flow with the default implementation.
		f.FlowBase.Run(ctx, noWait)
		return
	}

	if !noWait {
		defer f.Wait()
	}

	if err := f.StartInternal(ctx, nil /* processors */, nil /* outputs */); err != nil {
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
	if err := f.Cfg.TempFS.MkdirAll(f.tempStorage.path, os.ModePerm); err != nil {
		colexecerror.InternalError(errors.Wrap(err, "unable to create temporary storage directory"))
	}
	// We have just created the temporary directory which will be used for all
	// disk-spilling operations of this flow, thus, it is a convenient place to
	// increment the counter of the number of queries spilled - this code won't
	// be executed for this flow in the future since we short-circuit above.
	f.Cfg.Metrics.QueriesSpilled.Inc(1)
	return f.tempStorage.path
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

const (
	vectorizedFlowOverhead        = int64(unsafe.Sizeof(vectorizedFlow{}))
	vectorizedFlowCreatorOverhead = int64(unsafe.Sizeof(vectorizedFlowCreator{}))
	fdCountingSemaphoreOverhead   = int64(unsafe.Sizeof(fdCountingSemaphore{}))
)

// MemUsage is part of the flowinfra.Flow interface.
func (f *vectorizedFlow) MemUsage() int64 {
	return f.FlowBase.MemUsage() + vectorizedFlowOverhead +
		vectorizedFlowCreatorOverhead + fdCountingSemaphoreOverhead
}

// Cleanup is part of the flowinfra.Flow interface.
func (f *vectorizedFlow) Cleanup(ctx context.Context) {
	startCleanup, endCleanup := f.FlowBase.GetOnCleanupFns()
	startCleanup()
	defer endCleanup(ctx)

	// This cleans up all the memory and disk monitoring of the vectorized flow
	// as well as closes all the closers.
	f.creator.cleanup(ctx)

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
	inputWatch := timeutil.NewStopWatchWithCPU()
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
			return errors.AssertionFailedf("unexpectedly an input is not collecting stats")
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
	flowCtx *execinfra.FlowCtx, statsCollectors []colexecop.VectorizedStatsCollector,
) func(context.Context) []*execinfrapb.ComponentStats {
	if !s.recordingStats {
		return nil
	}
	return func(ctx context.Context) []*execinfrapb.ComponentStats {
		lastOutboxOnRemoteNode := atomic.AddInt32(&s.numOutboxesDrained, 1) == s.numOutboxes && !s.f.Gateway
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
				Component: flowCtx.FlowComponentID(),
				FlowStats: execinfrapb.FlowStats{
					MaxMemUsage:  optional.MakeUint(uint64(flowCtx.Mon.MaximumBytes())),
					MaxDiskUsage: optional.MakeUint(uint64(flowCtx.DiskMonitor.MaximumBytes())),
					ConsumedRU:   optional.MakeUint(uint64(flowCtx.TenantCPUMonitor.EndCollection(ctx))),
				},
			})
		}
		return result
	}
}

type runFn func(_ context.Context, flowCtxCancel context.CancelFunc)

type admissionOptions struct {
	admissionQ    *admission.WorkQueue
	admissionInfo admission.WorkInfo
}

// remoteComponentCreator is an interface that abstracts the constructors for
// several components in a remote flow. Mostly for testing purposes.
type remoteComponentCreator interface {
	newOutbox(
		flowCtx *execinfra.FlowCtx,
		processorID int32,
		allocator *colmem.Allocator,
		converterMemAcc *mon.BoundAccount,
		input colexecargs.OpWithMetaInfo,
		typs []*types.T,
		getStats func(context.Context) []*execinfrapb.ComponentStats,
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
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	allocator *colmem.Allocator,
	converterMemAcc *mon.BoundAccount,
	input colexecargs.OpWithMetaInfo,
	typs []*types.T,
	getStats func(context.Context) []*execinfrapb.ComponentStats,
) (*colrpc.Outbox, error) {
	return colrpc.NewOutbox(flowCtx, processorID, allocator, converterMemAcc, input, typs, getStats)
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
	remoteComponentCreator

	// This field should not be accessed directly - instead, the embedded
	// remoteComponentCreator interface above should be used. This struct is
	// embedded in order to avoid allocations on the main code path.
	rcCreator vectorizedRemoteComponentCreator

	f          *flowinfra.FlowBase
	processors [1]execinfra.Processor
	outputs    [1]execinfra.RowReceiver

	// batchFlowCoordinator, if set, indicates that the vectorized flow should
	// not use the default FlowBase.Run implementation.
	batchFlowCoordinator *BatchFlowCoordinator

	streamIDToInputOp map[execinfrapb.StreamID]colexecargs.OpWithMetaInfo
	streamIDToSpecIdx map[execinfrapb.StreamID]int
	semaCtx           *tree.SemaContext
	typeResolver      descs.DistSQLTypeResolver

	// numOutboxes counts how many colrpc.Outbox'es have been set up on this
	// node. Note that unlike numOutboxesDrained, numOutboxes doesn't need to be
	// accessed atomically because we create all outboxes serially in the flow
	// goroutine, during the flow setup (unlike draining which happens inside
	// the outboxes goroutines).
	numOutboxes int32
	// numOutboxesDrained is an atomic that keeps track of how many outboxes
	// have been drained. When numOutboxesDrained equals numOutboxes, flow-level
	// metadata is added to a flow-level span on the non-gateway nodes.
	// NB: must be accessed atomically.
	numOutboxesDrained int32

	// procIdxQueue is a queue of indices into processorSpecs (the argument to
	// setupFlow), for topologically ordered processing.
	procIdxQueue []int
	// opChains accumulates all operators that have no further outputs on the
	// current node, for the purposes of EXPLAIN output.
	opChains execopnode.OpChains
	// operatorConcurrency is set if any operators are executed in parallel.
	operatorConcurrency bool
	recordingStats      bool
	// releasables contains all components that should be released back to their
	// pools during the flow cleanup.
	releasables []execreleasable.Releasable

	monitorRegistry *colexecargs.MonitorRegistry
	// closerRegistry will be closed during the flow cleanup. It is safe to do
	// so in the main flow goroutine since all other goroutines that might have
	// used these objects must have exited by the time Cleanup() is called -
	// Flow.Wait() ensures that.
	closerRegistry *colexecargs.CloserRegistry
	diskQueueCfg   colcontainer.DiskQueueCfg
	fdSemaphore    semaphore.Semaphore
}

var _ execreleasable.Releasable = &vectorizedFlowCreator{}

var vectorizedFlowCreatorPool = sync.Pool{
	New: func() interface{} {
		return &vectorizedFlowCreator{
			streamIDToInputOp: make(map[execinfrapb.StreamID]colexecargs.OpWithMetaInfo),
			streamIDToSpecIdx: make(map[execinfrapb.StreamID]int),
			monitorRegistry:   &colexecargs.MonitorRegistry{},
			closerRegistry:    &colexecargs.CloserRegistry{},
		}
	},
}

// newVectorizedFlowCreator returns a new vectorizedFlowCreator.
//
// flowBase must be non-nil while componentCreator can be nil in which case the
// embedded struct is used.
func newVectorizedFlowCreator(
	flowBase *flowinfra.FlowBase,
	componentCreator remoteComponentCreator,
	recordingStats bool,
	diskQueueCfg colcontainer.DiskQueueCfg,
	fdSemaphore semaphore.Semaphore,
) *vectorizedFlowCreator {
	typeResolver := flowBase.NewTypeResolver(flowBase.Txn)
	semaCtx := tree.MakeSemaContext(typeResolver)
	creator := vectorizedFlowCreatorPool.Get().(*vectorizedFlowCreator)
	*creator = vectorizedFlowCreator{
		f:                 flowBase,
		streamIDToInputOp: creator.streamIDToInputOp,
		streamIDToSpecIdx: creator.streamIDToSpecIdx,
		semaCtx:           &semaCtx,
		typeResolver:      typeResolver,
		procIdxQueue:      creator.procIdxQueue,
		opChains:          creator.opChains,
		recordingStats:    recordingStats,
		releasables:       creator.releasables,
		monitorRegistry:   creator.monitorRegistry,
		closerRegistry:    creator.closerRegistry,
		diskQueueCfg:      diskQueueCfg,
		fdSemaphore:       fdSemaphore,
	}
	if componentCreator == nil {
		// On the main code path, use the embedded component creator.
		creator.remoteComponentCreator = creator.rcCreator
	} else {
		creator.remoteComponentCreator = componentCreator
	}
	return creator
}

func (s *vectorizedFlowCreator) cleanup(ctx context.Context) {
	s.closerRegistry.Close(ctx)
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
	s.monitorRegistry.Reset()
	s.closerRegistry.Reset()
	*s = vectorizedFlowCreator{
		streamIDToInputOp: s.streamIDToInputOp,
		streamIDToSpecIdx: s.streamIDToSpecIdx,
		// procIdxQueue is a slice of ints, so it's ok to just slice up to 0 to
		// prime it for reuse.
		procIdxQueue:    s.procIdxQueue[:0],
		opChains:        s.opChains[:0],
		releasables:     s.releasables[:0],
		monitorRegistry: s.monitorRegistry,
		closerRegistry:  s.closerRegistry,
	}
	vectorizedFlowCreatorPool.Put(s)
}

// accumulateAsyncComponent stores a component (either a router or an outbox)
// to be run asynchronously. Note that this component doesn't run until the flow
// is started.
func (s *vectorizedFlowCreator) accumulateAsyncComponent(run runFn) {
	s.f.AddStartable(
		flowinfra.StartableFn(func(ctx context.Context, wg *sync.WaitGroup, flowCtxCancel context.CancelFunc) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				run(ctx, flowCtxCancel)
			}()
		}))
}

// addFlowCoordinator adds the FlowCoordinator to the flow. This is only done on
// the gateway node.
func (s *vectorizedFlowCreator) addFlowCoordinator(f *FlowCoordinator) {
	s.processors[0] = f
	s.outputs[0] = s.f.GetRowSyncFlowConsumer()
	_ = s.f.SetProcessorsAndOutputs(s.processors[:1], s.outputs[:1])
}

// setupRemoteOutputStream sets up a colrpc.Outbox that will operate according
// to the given execinfrapb.StreamEndpointSpec. It will also drain all
// MetadataSources in op.
// NOTE: The caller must not reuse the metadata sources.
func (s *vectorizedFlowCreator) setupRemoteOutputStream(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	op colexecargs.OpWithMetaInfo,
	outputTyps []*types.T,
	stream *execinfrapb.StreamEndpointSpec,
	factory coldata.ColumnFactory,
	getStats func(context.Context) []*execinfrapb.ComponentStats,
) (execopnode.OpNode, error) {
	outbox, err := s.remoteComponentCreator.newOutbox(
		flowCtx, processorID, colmem.NewAllocator(ctx, s.monitorRegistry.NewStreamingMemAccount(flowCtx), factory),
		s.monitorRegistry.NewStreamingMemAccount(flowCtx),
		op, outputTyps, getStats,
	)
	if err != nil {
		return nil, err
	}

	s.numOutboxes++
	run := func(ctx context.Context, flowCtxCancel context.CancelFunc) {
		outbox.Run(
			ctx,
			s.f.Cfg.SQLInstanceDialer,
			stream.TargetNodeID,
			stream.StreamID,
			flowCtxCancel,
			flowinfra.SettingFlowStreamTimeout.Get(&flowCtx.Cfg.Settings.SV),
		)
	}
	s.accumulateAsyncComponent(run)
	return outbox, nil
}

// setupRouter sets up a vectorized hash router according to the output router
// spec. The router takes the responsibility of draining the metadata sources
// from input.MetadataSources.
// NOTE: This method supports only BY_HASH routers. Callers should handle
// PASS_THROUGH routers separately.
// NOTE: The caller must not reuse the metadata sources.
func (s *vectorizedFlowCreator) setupRouter(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	input colexecargs.OpWithMetaInfo,
	outputTyps []*types.T,
	output *execinfrapb.OutputRouterSpec,
	factory coldata.ColumnFactory,
) error {
	if output.Type != execinfrapb.OutputRouterSpec_BY_HASH {
		return errors.AssertionFailedf("vectorized output router type %s unsupported", output.Type)
	}

	// HashRouter memory monitor names are the concatenated output stream IDs.
	var sb strings.Builder
	for i, s := range output.Streams {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(s.StreamID.String())
	}
	streamIDs := redact.SafeString(sb.String())
	mmName := "hash-router-[" + streamIDs + "]"

	numOutputs := len(output.Streams)
	// We need to create two memory accounts for each output (one for the
	// allocator and another one for the converter).
	hashRouterMemMonitor, accounts := s.monitorRegistry.CreateUnlimitedMemAccountsWithName(
		ctx, flowCtx, mmName, 2*numOutputs,
	)
	allocatorAccounts, converterAccounts := accounts[:numOutputs], accounts[numOutputs:]
	allocators := make([]*colmem.Allocator, numOutputs)
	for i := range allocators {
		allocators[i] = colmem.NewAllocator(ctx, allocatorAccounts[i], factory)
	}
	diskMon, diskAccounts := s.monitorRegistry.CreateDiskAccounts(ctx, flowCtx, mmName, numOutputs)
	router, outputs := NewHashRouter(
		flowCtx, processorID, allocators, input, outputTyps, output.HashColumns, execinfra.GetWorkMemLimit(flowCtx),
		s.diskQueueCfg, s.fdSemaphore, diskAccounts, converterAccounts,
	)
	runRouter := func(ctx context.Context, _ context.CancelFunc) {
		router.Run(logtags.AddTag(ctx, "hashRouterID", streamIDs))
	}
	s.accumulateAsyncComponent(runRouter)

	foundLocalOutput := false
	for i, op := range outputs {
		s.closerRegistry.AddCloser(op)
		if buildutil.CrdbTestBuild {
			op = colexec.NewInvariantsChecker(op)
		}
		stream := &output.Streams[i]
		switch stream.Type {
		case execinfrapb.StreamEndpointSpec_SYNC_RESPONSE:
			return errors.AssertionFailedf("unexpected sync response output when setting up router")
		case execinfrapb.StreamEndpointSpec_REMOTE:
			if _, err := s.setupRemoteOutputStream(
				ctx, flowCtx, processorID, colexecargs.OpWithMetaInfo{
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
	processorID int32,
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
			if err := s.f.CheckInboundStreamID(inputStream.StreamID); err != nil {
				return colexecargs.OpWithMetaInfo{}, err
			}

			// Retrieve the latency from the origin node (the one that has the
			// outbox).
			latency, err := s.f.Cfg.SQLInstanceDialer.Latency(roachpb.NodeID(inputStream.OriginNodeID))
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
				s.f.GetCtxDone(),
				admissionOptions{
					admissionQ:    flowCtx.Cfg.SQLSQLResponseAdmissionQ,
					admissionInfo: s.f.GetAdmissionInfo(),
				})

			if err != nil {
				return colexecargs.OpWithMetaInfo{}, err
			}
			s.f.AddRemoteStream(inputStream.StreamID, flowinfra.NewInboundStreamInfo(
				vectorizedInboundStreamHandler{inbox},
				s.f.GetWaitGroup(),
			))
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
			return colexecargs.OpWithMetaInfo{}, errors.AssertionFailedf("unsupported input stream type %s", inputStream.Type)
		}
	}
	opWithMetaInfo := inputStreamOps[0]
	if len(inputStreamOps) > 1 {
		statsInputs := inputStreamOps
		allocator := colmem.NewAllocator(ctx, s.monitorRegistry.NewStreamingMemAccount(flowCtx), factory)
		if input.Type == execinfrapb.InputSyncSpec_ORDERED {
			os := colexec.NewOrderedSynchronizer(
				flowCtx, processorID, allocator, execinfra.GetWorkMemLimit(flowCtx),
				inputStreamOps, input.ColumnTypes,
				execinfrapb.ConvertToColumnOrdering(input.Ordering), 0, /* tuplesToMerge */
			)
			opWithMetaInfo = colexecargs.OpWithMetaInfo{
				Root:            os,
				MetadataSources: colexecop.MetadataSources{os},
			}
			s.closerRegistry.AddCloser(os)
		} else if input.Type == execinfrapb.InputSyncSpec_SERIAL_UNORDERED || opt == flowinfra.FuseAggressively {
			var err error
			if input.EnforceHomeRegionError != nil {
				err = input.EnforceHomeRegionError.ErrorDetail(ctx)
				if flowCtx.EvalCtx.SessionData().EnforceHomeRegionFollowerReadsEnabled {
					err = execinfra.NewDynamicQueryHasNoHomeRegionError(err)
				}
			}
			sync := colexec.NewSerialUnorderedSynchronizer(
				flowCtx, processorID, allocator, input.ColumnTypes, inputStreamOps,
				input.EnforceHomeRegionStreamExclusiveUpperBound, err,
			)
			opWithMetaInfo = colexecargs.OpWithMetaInfo{
				Root:            sync,
				MetadataSources: colexecop.MetadataSources{sync},
			}
			s.closerRegistry.AddCloser(sync)
		} else {
			// Note that if we have opt == flowinfra.FuseAggressively, then we
			// must use the serial unordered sync above in order to remove any
			// concurrency.
			sync := colexec.NewParallelUnorderedSynchronizer(
				flowCtx, processorID, allocator, input.ColumnTypes, inputStreamOps, s.f.GetWaitGroup(),
			)
			opWithMetaInfo = colexecargs.OpWithMetaInfo{
				Root:            sync,
				MetadataSources: colexecop.MetadataSources{sync},
			}
			s.closerRegistry.AddCloser(sync)
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
// of pspec. The metadata sources are fully consumed by either passing them to
// an outbox or HashRouter to be drained, or storing them in streamIDToInputOp
// with the given op to be processed later.
// NOTE: The caller must not reuse the metadata sources.
func (s *vectorizedFlowCreator) setupOutput(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	pspec *execinfrapb.ProcessorSpec,
	opWithMetaInfo colexecargs.OpWithMetaInfo,
	opOutputTypes []*types.T,
	factory coldata.ColumnFactory,
	streamingMemAccount *mon.BoundAccount,
) error {
	output := &pspec.Output[0]
	if output.Type != execinfrapb.OutputRouterSpec_PASS_THROUGH {
		return s.setupRouter(
			ctx,
			flowCtx,
			pspec.ProcessorID,
			opWithMetaInfo,
			opOutputTypes,
			output,
			factory,
		)
	}

	if len(output.Streams) != 1 {
		return errors.AssertionFailedf("unsupported multi outputstream proc (%d streams)", len(output.Streams))
	}
	outputStream := &output.Streams[0]
	switch outputStream.Type {
	case execinfrapb.StreamEndpointSpec_LOCAL:
		s.streamIDToInputOp[outputStream.StreamID] = opWithMetaInfo
	case execinfrapb.StreamEndpointSpec_REMOTE:
		// Set up an Outbox.
		outbox, err := s.setupRemoteOutputStream(
			ctx, flowCtx, pspec.ProcessorID, opWithMetaInfo, opOutputTypes, outputStream, factory,
			s.makeGetStatsFnForOutbox(flowCtx, opWithMetaInfo.StatsCollectors),
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
		if input == nil && s.f.GetBatchSyncFlowConsumer() != nil {
			// We can create a batch flow coordinator and avoid materializing
			// the batches.
			s.batchFlowCoordinator = NewBatchFlowCoordinator(
				flowCtx,
				pspec.ProcessorID,
				opWithMetaInfo,
				s.f.GetBatchSyncFlowConsumer(),
				s.f.GetCancelFlowFn(),
			)
			// The flow coordinator is a root of its operator chain.
			s.opChains = append(s.opChains, s.batchFlowCoordinator)
			s.releasables = append(s.releasables, s.batchFlowCoordinator)
		} else {
			// We need to use the row receiving output.
			if input == nil {
				// We couldn't remove the columnarizer.
				input = colexec.NewMaterializer(
					streamingMemAccount,
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
				s.f.GetCancelFlowFn(),
			)
			// The flow coordinator is a root of its operator chain.
			s.opChains = append(s.opChains, f)
			// NOTE: we don't append f to s.releasables because addFlowCoordinator
			// adds the FlowCoordinator to FlowBase.processors, which ensures that
			// it is later released in FlowBase.Cleanup.
			s.addFlowCoordinator(f)
		}

	default:
		return errors.AssertionFailedf("unsupported output stream type %s", outputStream.Type)
	}
	return nil
}

func (s *vectorizedFlowCreator) setupFlow(
	ctx context.Context, processorSpecs []execinfrapb.ProcessorSpec, opt flowinfra.FuseOpt,
) (opChains execopnode.OpChains, batchFlowCoordinator *BatchFlowCoordinator, err error) {
	flowCtx := &s.f.FlowCtx
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
				err = errors.AssertionFailedf("unsupported multi-output proc (%d outputs)", len(pspec.Output))
				return
			}

			inputs := make([]colexecargs.OpWithMetaInfo, len(pspec.Input))
			for i := range pspec.Input {
				inputs[i], err = s.setupInput(ctx, flowCtx, pspec.ProcessorID, pspec.Input[i], opt, factory)
				if err != nil {
					return
				}
			}

			// Before we can safely use types we received over the wire in the
			// operators, we need to make sure they are hydrated.
			if err = s.typeResolver.HydrateTypeSlice(ctx, pspec.ResultTypes); err != nil {
				return
			}

			streamingMemAccount := s.monitorRegistry.NewStreamingMemAccount(flowCtx)
			args := &colexecargs.NewColOperatorArgs{
				Spec:                 pspec,
				Inputs:               inputs,
				StreamingMemAccount:  streamingMemAccount,
				ProcessorConstructor: rowexec.NewProcessor,
				LocalProcessors:      s.f.GetLocalProcessors(),
				LocalVectorSources:   s.f.GetLocalVectorSources(),
				DiskQueueCfg:         s.diskQueueCfg,
				FDSemaphore:          s.fdSemaphore,
				SemaCtx:              s.semaCtx,
				Factory:              factory,
				MonitorRegistry:      s.monitorRegistry,
				CloserRegistry:       s.closerRegistry,
				TypeResolver:         &s.typeResolver,
			}
			numOldMonitors := len(s.monitorRegistry.GetMonitors())
			var result *colexecargs.NewColOperatorResult
			result, err = colbuilder.NewColOperator(ctx, flowCtx, args)
			if result != nil {
				s.releasables = append(s.releasables, result)
			}
			if err != nil {
				if log.ExpensiveLogEnabled(ctx, 1) {
					err = errors.Wrapf(err, "unable to vectorize execution plan")
				}
				return
			}
			if flowCtx.EvalCtx.SessionData().TestingVectorizeInjectPanics {
				result.Root = newPanicInjector(result.Root)
			}

			if s.recordingStats {
				newMonitors := s.monitorRegistry.GetMonitors()[numOldMonitors:]
				if err = s.wrapWithVectorizedStatsCollectorBase(
					&result.OpWithMetaInfo, result.KVReader, result.Columnarizer, inputs,
					flowCtx.ProcessorComponentID(pspec.ProcessorID), newMonitors,
				); err != nil {
					return
				}
			}

			if err = s.setupOutput(
				ctx, flowCtx, pspec, result.OpWithMetaInfo, result.ColumnTypes, factory, streamingMemAccount,
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
						err = errors.AssertionFailedf("couldn't find stream %d", outputStream.StreamID)
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
				return errors.AssertionFailedf("only pass-through and hash routers are supported")
			}
		}
	}
	return nil
}
