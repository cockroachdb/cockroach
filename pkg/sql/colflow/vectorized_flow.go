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
	"fmt"
	"math"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/settings"
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
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/marusama/semaphore"
	opentracing "github.com/opentracing/opentracing-go"
)

// countingSemaphore is a semaphore that keeps track of the semaphore count from
// its perspective.
type countingSemaphore struct {
	semaphore.Semaphore
	globalCount *metric.Gauge
	count       int64
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

type vectorizedFlow struct {
	*flowinfra.FlowBase
	// operatorConcurrency is set if any operators are executed in parallel.
	operatorConcurrency bool

	// countingSemaphore is a wrapper over a semaphore.Semaphore that keeps track
	// of the number of resources held in a semaphore.Semaphore requested from the
	// context of this flow so that these can be released unconditionally upon
	// Cleanup.
	countingSemaphore *countingSemaphore

	// streamingMemAccounts are the memory accounts that are tracking the static
	// memory usage of the whole vectorized flow as well as all dynamic memory of
	// the streaming components.
	streamingMemAccounts []*mon.BoundAccount

	// monitors are the monitors (of both memory and disk usage) of the
	// buffering components.
	monitors []*mon.BytesMonitor
	// accounts are the accounts that are tracking the dynamic memory and disk
	// usage of the buffering components.
	accounts []*mon.BoundAccount

	tempStorage struct {
		// path is the path to this flow's temporary storage directory.
		path           string
		createdStateMu struct {
			syncutil.Mutex
			// created is a protected boolean that is true when the flow's temporary
			// storage directory has been created.
			created bool
		}
	}

	testingKnobs struct {
		// onSetupFlow is a testing knob that is called before calling
		// creator.setupFlow with the given creator.
		onSetupFlow func(*vectorizedFlowCreator)
	}
}

var _ flowinfra.Flow = &vectorizedFlow{}

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

// VectorizeTestingBatchSize is a testing cluster setting that sets the default
// batch size used by the vectorized execution engine. A low batch size is
// useful to test batch reuse.
var VectorizeTestingBatchSize = settings.RegisterValidatedIntSetting(
	"sql.testing.vectorize.batch_size",
	fmt.Sprintf("the size of a batch of rows in the vectorized engine (0=default, value must be less than %d)", coldata.MaxBatchSize),
	0,
	func(newBatchSize int64) error {
		if newBatchSize > coldata.MaxBatchSize {
			return pgerror.Newf(pgcode.InvalidParameterValue, "batch size %d may not be larger than %d", newBatchSize, coldata.MaxBatchSize)
		}
		return nil
	},
)

// Setup is part of the flowinfra.Flow interface.
func (f *vectorizedFlow) Setup(
	ctx context.Context, spec *execinfrapb.FlowSpec, opt flowinfra.FuseOpt,
) (context.Context, error) {
	var err error
	ctx, err = f.FlowBase.Setup(ctx, spec, opt)
	if err != nil {
		return ctx, err
	}
	log.VEventf(ctx, 1, "setting up vectorize flow %s", f.ID.Short())
	recordingStats := false
	if sp := opentracing.SpanFromContext(ctx); sp != nil && tracing.IsRecording(sp) {
		recordingStats = true
	}
	helper := &vectorizedFlowCreatorHelper{f: f.FlowBase}

	testingBatchSize := int64(0)
	if f.FlowCtx.Cfg.Settings != nil {
		testingBatchSize = VectorizeTestingBatchSize.Get(&f.FlowCtx.Cfg.Settings.SV)
	}
	if testingBatchSize != 0 {
		if err := coldata.SetBatchSizeForTests(int(testingBatchSize)); err != nil {
			return ctx, err
		}
	} else {
		coldata.ResetBatchSizeForTests()
	}

	// Create a name for this flow's temporary directory. Note that this directory
	// is lazily created when necessary and cleaned up in Cleanup(). The directory
	// name is the flow's ID in most cases apart from when the flow's ID is unset
	// (in the case of local flows). In this case the directory will be prefixed
	// with "local-flow" and a uuid is generated on the spot to provide a unique
	// name.
	tempDirName := f.GetID().String()
	if f.GetID().Equal(uuid.Nil) {
		tempDirName = "local-flow" + uuid.FastMakeV4().String()
	}
	f.tempStorage.path = filepath.Join(f.Cfg.TempStoragePath, tempDirName)
	diskQueueCfg := colcontainer.DiskQueueCfg{
		FS:   f.Cfg.TempFS,
		Path: f.tempStorage.path,
		OnNewDiskQueueCb: func() {
			f.tempStorage.createdStateMu.Lock()
			defer f.tempStorage.createdStateMu.Unlock()
			if f.tempStorage.createdStateMu.created {
				// The temporary storage directory has already been created.
				return
			}
			log.VEventf(ctx, 1, "flow %s spilled to disk, stack trace: %s", f.ID, util.GetSmallTrace(2))
			if err := f.Cfg.TempFS.MkdirAll(f.tempStorage.path); err != nil {
				colexecerror.InternalError(errors.Errorf("unable to create temporary storage directory: %v", err))
			}
			f.tempStorage.createdStateMu.created = true
		},
	}
	if err := diskQueueCfg.EnsureDefaults(); err != nil {
		return ctx, err
	}
	f.countingSemaphore = &countingSemaphore{Semaphore: f.Cfg.VecFDSemaphore, globalCount: f.Cfg.Metrics.VecOpenFDs}
	creator := newVectorizedFlowCreator(
		helper,
		vectorizedRemoteComponentCreator{},
		recordingStats,
		f.GetWaitGroup(),
		f.GetSyncFlowConsumer(),
		f.GetFlowCtx().Cfg.NodeDialer,
		f.GetID(),
		diskQueueCfg,
		f.countingSemaphore,
		false, /* forceExprDeserialization */
	)
	if f.testingKnobs.onSetupFlow != nil {
		f.testingKnobs.onSetupFlow(creator)
	}
	_, err = creator.setupFlow(ctx, f.GetFlowCtx(), spec.Processors, opt)
	if err == nil {
		f.operatorConcurrency = creator.operatorConcurrency
		f.streamingMemAccounts = append(f.streamingMemAccounts, creator.streamingMemAccounts...)
		f.monitors = append(f.monitors, creator.monitors...)
		f.accounts = append(f.accounts, creator.accounts...)
		log.VEventf(ctx, 1, "vectorized flow setup succeeded")
		return ctx, nil
	}
	// It is (theoretically) possible that some of the memory monitoring
	// infrastructure was created even in case of an error, and we need to clean
	// that up.
	for _, acc := range creator.streamingMemAccounts {
		acc.Close(ctx)
	}
	for _, acc := range creator.accounts {
		acc.Close(ctx)
	}
	for _, mon := range creator.monitors {
		mon.Stop(ctx)
	}
	log.VEventf(ctx, 1, "failed to vectorize: %s", err)
	return ctx, err
}

// IsVectorized is part of the flowinfra.Flow interface.
func (f *vectorizedFlow) IsVectorized() bool {
	return true
}

// ConcurrentExecution is part of the flowinfra.Flow interface.
func (f *vectorizedFlow) ConcurrentExecution() bool {
	return f.operatorConcurrency || f.FlowBase.ConcurrentExecution()
}

// Release releases this vectorizedFlow back to the pool.
func (f *vectorizedFlow) Release() {
	*f = vectorizedFlow{}
	vectorizedFlowPool.Put(f)
}

// Cleanup is part of the flowinfra.Flow interface.
func (f *vectorizedFlow) Cleanup(ctx context.Context) {
	// This cleans up all the memory and disk monitoring of the vectorized flow.
	for _, acc := range f.streamingMemAccounts {
		acc.Close(ctx)
	}
	for _, acc := range f.accounts {
		acc.Close(ctx)
	}
	for _, mon := range f.monitors {
		mon.Stop(ctx)
	}

	f.tempStorage.createdStateMu.Lock()
	created := f.tempStorage.createdStateMu.created
	f.tempStorage.createdStateMu.Unlock()
	if created {
		if err := f.Cfg.TempFS.RemoveAll(f.tempStorage.path); err != nil {
			// Log error as a Warning but keep on going to close the memory
			// infrastructure.
			log.Warningf(
				ctx,
				"unable to remove flow %s's temporary directory at %s, files may be left over: %v",
				f.GetID().Short(),
				f.tempStorage.path,
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

// wrapWithVectorizedStatsCollector creates a new
// colexec.VectorizedStatsCollector that wraps op and connects the newly
// created wrapper with those corresponding to operators in inputs (the latter
// must have already been wrapped).
func (s *vectorizedFlowCreator) wrapWithVectorizedStatsCollector(
	op colexecbase.Operator,
	inputs []colexecbase.Operator,
	id int32,
	idTagKey string,
	monitors []*mon.BytesMonitor,
) (*colexec.VectorizedStatsCollector, error) {
	inputWatch := timeutil.NewStopWatch()
	var memMonitors, diskMonitors []*mon.BytesMonitor
	for _, m := range monitors {
		if m.Resource() == mon.DiskResource {
			diskMonitors = append(diskMonitors, m)
		} else {
			memMonitors = append(memMonitors, m)
		}
	}
	vsc := colexec.NewVectorizedStatsCollector(
		op, id, idTagKey, len(inputs) == 0, inputWatch, memMonitors, diskMonitors,
	)
	for _, input := range inputs {
		sc, ok := input.(*colexec.VectorizedStatsCollector)
		if !ok {
			return nil, errors.New("unexpectedly an input is not collecting stats")
		}
		sc.SetOutputWatch(inputWatch)
	}
	s.vectorizedStatsCollectorsQueue = append(s.vectorizedStatsCollectorsQueue, vsc)
	return vsc, nil
}

// finishVectorizedStatsCollectors finishes the given stats collectors and
// outputs their stats to the trace contained in the ctx's span.
func finishVectorizedStatsCollectors(
	ctx context.Context,
	flowID execinfrapb.FlowID,
	deterministicStats bool,
	vectorizedStatsCollectors []*colexec.VectorizedStatsCollector,
) {
	flowIDString := flowID.String()
	for _, vsc := range vectorizedStatsCollectors {
		vsc.OutputStats(ctx, flowIDString, deterministicStats)
	}
}

type runFn func(context.Context, context.CancelFunc)

// flowCreatorHelper contains all the logic needed to add the vectorized
// infrastructure to be run asynchronously as well as to perform some sanity
// checks.
type flowCreatorHelper interface {
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
	toClose         []colexec.IdempotentCloser
}

// remoteComponentCreator is an interface that abstracts the constructors for
// several components in a remote flow. Mostly for testing purposes.
type remoteComponentCreator interface {
	newOutbox(
		allocator *colmem.Allocator,
		input colexecbase.Operator,
		typs []*types.T,
		metadataSources []execinfrapb.MetadataSource,
		toClose []colexec.IdempotentCloser,
	) (*colrpc.Outbox, error)
	newInbox(allocator *colmem.Allocator, typs []*types.T, streamID execinfrapb.StreamID) (*colrpc.Inbox, error)
}

type vectorizedRemoteComponentCreator struct{}

func (vectorizedRemoteComponentCreator) newOutbox(
	allocator *colmem.Allocator,
	input colexecbase.Operator,
	typs []*types.T,
	metadataSources []execinfrapb.MetadataSource,
	toClose []colexec.IdempotentCloser,
) (*colrpc.Outbox, error) {
	return colrpc.NewOutbox(allocator, input, typs, metadataSources, toClose)
}

func (vectorizedRemoteComponentCreator) newInbox(
	allocator *colmem.Allocator, typs []*types.T, streamID execinfrapb.StreamID,
) (*colrpc.Inbox, error) {
	return colrpc.NewInbox(allocator, typs, streamID)
}

// vectorizedFlowCreator performs all the setup of vectorized flows. Depending
// on embedded flowCreatorHelper, it can either do the actual setup in order
// to run the flow or do the setup needed to check that the flow is supported
// through the vectorized engine.
type vectorizedFlowCreator struct {
	flowCreatorHelper
	remoteComponentCreator

	streamIDToInputOp              map[execinfrapb.StreamID]opDAGWithMetaSources
	recordingStats                 bool
	vectorizedStatsCollectorsQueue []*colexec.VectorizedStatsCollector
	waitGroup                      *sync.WaitGroup
	syncFlowConsumer               execinfra.RowReceiver
	nodeDialer                     *nodedialer.Dialer
	flowID                         execinfrapb.FlowID
	exprHelper                     colexec.ExprHelper

	// numOutboxes counts how many exec.Outboxes have been set up on this node.
	// It must be accessed atomically.
	numOutboxes       int32
	materializerAdded bool

	// leaves accumulates all operators that have no further outputs on the
	// current node, for the purposes of EXPLAIN output.
	leaves []execinfra.OpNode
	// operatorConcurrency is set if any operators are executed in parallel.
	operatorConcurrency bool
	// streamingMemAccounts contains all memory accounts of the non-buffering
	// components in the vectorized flow.
	streamingMemAccounts []*mon.BoundAccount
	// monitors contains all monitors (for both memory and disk usage) of the
	// buffering components in the vectorized flow.
	monitors []*mon.BytesMonitor
	// accounts contains all monitors (for both memory and disk usage) of the
	// buffering components in the vectorized flow.
	accounts []*mon.BoundAccount

	diskQueueCfg colcontainer.DiskQueueCfg
	fdSemaphore  semaphore.Semaphore
}

func newVectorizedFlowCreator(
	helper flowCreatorHelper,
	componentCreator remoteComponentCreator,
	recordingStats bool,
	waitGroup *sync.WaitGroup,
	syncFlowConsumer execinfra.RowReceiver,
	nodeDialer *nodedialer.Dialer,
	flowID execinfrapb.FlowID,
	diskQueueCfg colcontainer.DiskQueueCfg,
	fdSemaphore semaphore.Semaphore,
	forceExprDeserialization bool,
) *vectorizedFlowCreator {
	return &vectorizedFlowCreator{
		flowCreatorHelper:              helper,
		remoteComponentCreator:         componentCreator,
		streamIDToInputOp:              make(map[execinfrapb.StreamID]opDAGWithMetaSources),
		recordingStats:                 recordingStats,
		vectorizedStatsCollectorsQueue: make([]*colexec.VectorizedStatsCollector, 0, 2),
		waitGroup:                      waitGroup,
		syncFlowConsumer:               syncFlowConsumer,
		nodeDialer:                     nodeDialer,
		flowID:                         flowID,
		diskQueueCfg:                   diskQueueCfg,
		fdSemaphore:                    fdSemaphore,
		exprHelper:                     colexec.NewExprHelper(forceExprDeserialization),
	}
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
	s.streamingMemAccounts = append(s.streamingMemAccounts, &streamingMemAccount)
	return &streamingMemAccount
}

// setupRemoteOutputStream sets up an Outbox that will operate according to
// the given StreamEndpointSpec. It will also drain all MetadataSources in the
// metadataSourcesQueue.
func (s *vectorizedFlowCreator) setupRemoteOutputStream(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	op colexecbase.Operator,
	outputTyps []*types.T,
	stream *execinfrapb.StreamEndpointSpec,
	metadataSourcesQueue []execinfrapb.MetadataSource,
	toClose []colexec.IdempotentCloser,
	factory coldata.ColumnFactory,
) (execinfra.OpNode, error) {
	// TODO(yuzefovich): we should collect some statistics on the outbox (e.g.
	// number of bytes sent).
	outbox, err := s.remoteComponentCreator.newOutbox(
		colmem.NewAllocator(ctx, s.newStreamingMemAccount(flowCtx), factory),
		op, outputTyps, metadataSourcesQueue, toClose,
	)
	if err != nil {
		return nil, err
	}
	atomic.AddInt32(&s.numOutboxes, 1)
	run := func(ctx context.Context, cancelFn context.CancelFunc) {
		outbox.Run(ctx, s.nodeDialer, stream.TargetNodeID, s.flowID, stream.StreamID, cancelFn)
		currentOutboxes := atomic.AddInt32(&s.numOutboxes, -1)
		// When the last Outbox on this node exits, we want to make sure that
		// everything is shutdown; namely, we need to call cancelFn if:
		// - it is the last Outbox
		// - there is no root materializer on this node (if it were, it would take
		// care of the cancellation itself)
		// - cancelFn is non-nil (it can be nil in tests).
		// Calling cancelFn will cancel the context that all infrastructure on this
		// node is listening on, so it will shut everything down.
		if currentOutboxes == 0 && !s.materializerAdded && cancelFn != nil {
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
func (s *vectorizedFlowCreator) setupRouter(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	input colexecbase.Operator,
	outputTyps []*types.T,
	output *execinfrapb.OutputRouterSpec,
	metadataSourcesQueue []execinfrapb.MetadataSource,
	toClose []colexec.IdempotentCloser,
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
		logtags.AddTag(ctx, "hashRouterID", mmName)
		router.Run(ctx)
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
				localOp, err = s.wrapWithVectorizedStatsCollector(
					op, nil /* inputs */, int32(stream.StreamID),
					execinfrapb.StreamIDTagKey, mons,
				)
				if err != nil {
					return err
				}
			}
			s.streamIDToInputOp[stream.StreamID] = opDAGWithMetaSources{
				rootOperator: localOp, metadataSources: []execinfrapb.MetadataSource{op}, toClose: toClose,
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
) (colexecbase.Operator, []execinfrapb.MetadataSource, error) {
	inputStreamOps := make([]colexec.SynchronizerInput, 0, len(input.Streams))
	// Before we can safely use types we received over the wire in the
	// operators, we need to make sure they are hydrated. In row execution
	// engine it is done during the processor initialization, but operators
	// don't do that. However, all operators (apart from the colBatchScan) get
	// their types from InputSyncSpec, so this is a convenient place to do the
	// hydration so that all operators get the valid types.
	if err := execinfrapb.HydrateTypeSlice(flowCtx.EvalCtx, input.ColumnTypes); err != nil {
		return nil, nil, err
	}
	for _, inputStream := range input.Streams {
		switch inputStream.Type {
		case execinfrapb.StreamEndpointSpec_LOCAL:
			in := s.streamIDToInputOp[inputStream.StreamID]
			inputStreamOps = append(inputStreamOps, colexec.SynchronizerInput{
				Op:              in.rootOperator,
				MetadataSources: in.metadataSources,
			})
		case execinfrapb.StreamEndpointSpec_REMOTE:
			// If the input is remote, the input operator does not exist in
			// streamIDToInputOp. Create an inbox.
			if err := s.checkInboundStreamID(inputStream.StreamID); err != nil {
				return nil, nil, err
			}
			inbox, err := s.remoteComponentCreator.newInbox(
				colmem.NewAllocator(ctx, s.newStreamingMemAccount(flowCtx), factory),
				input.ColumnTypes, inputStream.StreamID,
			)
			if err != nil {
				return nil, nil, err
			}
			s.addStreamEndpoint(inputStream.StreamID, inbox, s.waitGroup)
			op := colexecbase.Operator(inbox)
			if s.recordingStats {
				op, err = s.wrapWithVectorizedStatsCollector(
					inbox, nil /* inputs */, int32(inputStream.StreamID),
					execinfrapb.StreamIDTagKey, nil, /* monitors */
				)
				if err != nil {
					return nil, nil, err
				}
			}
			inputStreamOps = append(inputStreamOps, colexec.SynchronizerInput{Op: op, MetadataSources: []execinfrapb.MetadataSource{inbox}})
		default:
			return nil, nil, errors.Errorf("unsupported input stream type %s", inputStream.Type)
		}
	}
	op := inputStreamOps[0].Op
	metaSources := inputStreamOps[0].MetadataSources
	if len(inputStreamOps) > 1 {
		statsInputs := inputStreamOps
		if input.Type == execinfrapb.InputSyncSpec_ORDERED {
			os, err := colexec.NewOrderedSynchronizer(
				colmem.NewAllocator(ctx, s.newStreamingMemAccount(flowCtx), factory),
				inputStreamOps, input.ColumnTypes, execinfrapb.ConvertToColumnOrdering(input.Ordering),
			)
			if err != nil {
				return nil, nil, err
			}
			op = os
			metaSources = []execinfrapb.MetadataSource{os}
		} else {
			if opt == flowinfra.FuseAggressively {
				sync := colexec.NewSerialUnorderedSynchronizer(inputStreamOps)
				op = sync
				metaSources = []execinfrapb.MetadataSource{sync}
			} else {
				sync := colexec.NewParallelUnorderedSynchronizer(inputStreamOps, s.waitGroup)
				op = sync
				metaSources = []execinfrapb.MetadataSource{sync}
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
			op, err = s.wrapWithVectorizedStatsCollector(
				op, statsInputsAsOps, -1 /* id */, "" /* idTagKey */, nil, /* monitors */
			)
			if err != nil {
				return nil, nil, err
			}
		}
	}
	return op, metaSources, nil
}

// setupOutput sets up any necessary infrastructure according to the output
// spec of pspec. The metadataSourcesQueue is fully consumed by either
// connecting it to a component that can drain these MetadataSources (root
// materializer or outbox) or storing it in streamIDToInputOp with the given op
// to be processed later.
// NOTE: The caller must not reuse the metadataSourcesQueue.
func (s *vectorizedFlowCreator) setupOutput(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	pspec *execinfrapb.ProcessorSpec,
	op colexecbase.Operator,
	opOutputTypes []*types.T,
	metadataSourcesQueue []execinfrapb.MetadataSource,
	toClose []colexec.IdempotentCloser,
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
			// Pass in a copy of the queue to reset metadataSourcesQueue for
			// further appends without overwriting.
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
		// Set up an Outbox. Note that we pass in a copy of metadataSourcesQueue
		// so that we can reset it below and keep on writing to it.
		if s.recordingStats {
			// If recording stats, we add a metadata source that will generate all
			// stats data as metadata for the stats collectors created so far.
			vscs := append([]*colexec.VectorizedStatsCollector(nil), s.vectorizedStatsCollectorsQueue...)
			s.vectorizedStatsCollectorsQueue = s.vectorizedStatsCollectorsQueue[:0]
			metadataSourcesQueue = append(
				metadataSourcesQueue,
				execinfrapb.CallbackMetadataSource{
					DrainMetaCb: func(ctx context.Context) []execinfrapb.ProducerMetadata {
						// TODO(asubiotto): Who is responsible for the recording of the
						// parent context?
						// Start a separate recording so that GetRecording will return
						// the recordings for only the child spans containing stats.
						ctx, span := tracing.ChildSpanSeparateRecording(ctx, "")
						finishVectorizedStatsCollectors(
							ctx, flowCtx.ID, flowCtx.Cfg.TestingKnobs.DeterministicStats, vscs,
						)
						return []execinfrapb.ProducerMetadata{{TraceData: tracing.GetRecording(span)}}
					},
				},
			)
		}
		outbox, err :=
			s.setupRemoteOutputStream(ctx, flowCtx, op, opOutputTypes, outputStream, metadataSourcesQueue, toClose, factory)
		if err != nil {
			return err
		}
		// An outbox is a leaf: there's nothing that sees it as an input on this
		// node.
		s.leaves = append(s.leaves, outbox)
	case execinfrapb.StreamEndpointSpec_SYNC_RESPONSE:
		if s.syncFlowConsumer == nil {
			return errors.New("syncFlowConsumer unset, unable to create materializer")
		}
		// Make the materializer, which will write to the given receiver.
		columnTypes := s.syncFlowConsumer.Types()
		if err := assertTypesMatch(columnTypes, opOutputTypes); err != nil {
			return err
		}
		var outputStatsToTrace func()
		if s.recordingStats {
			// Make a copy given that vectorizedStatsCollectorsQueue is reset and
			// appended to.
			vscq := append([]*colexec.VectorizedStatsCollector(nil), s.vectorizedStatsCollectorsQueue...)
			outputStatsToTrace = func() {
				finishVectorizedStatsCollectors(
					ctx, flowCtx.ID, flowCtx.Cfg.TestingKnobs.DeterministicStats, vscq,
				)
			}
		}
		proc, err := colexec.NewMaterializer(
			flowCtx,
			pspec.ProcessorID,
			op,
			columnTypes,
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
	opt flowinfra.FuseOpt,
) (leaves []execinfra.OpNode, err error) {
	streamIDToSpecIdx := make(map[execinfrapb.StreamID]int)
	factory := coldataext.NewExtendedColumnFactory(flowCtx.NewEvalCtx())
	// queue is a queue of indices into processorSpecs, for topologically
	// ordered processing.
	queue := make([]int, 0, len(processorSpecs))
	for i := range processorSpecs {
		hasLocalInput := false
		for j := range processorSpecs[i].Input {
			input := &processorSpecs[i].Input[j]
			for k := range input.Streams {
				stream := &input.Streams[k]
				streamIDToSpecIdx[stream.StreamID] = i
				if stream.Type != execinfrapb.StreamEndpointSpec_REMOTE {
					hasLocalInput = true
				}
			}
		}
		if hasLocalInput {
			continue
		}
		// Queue all processors with either no inputs or remote inputs.
		queue = append(queue, i)
	}

	inputs := make([]colexecbase.Operator, 0, 2)
	for len(queue) > 0 {
		pspec := &processorSpecs[queue[0]]
		queue = queue[1:]
		if len(pspec.Output) > 1 {
			return nil, errors.Errorf("unsupported multi-output proc (%d outputs)", len(pspec.Output))
		}

		// metadataSourcesQueue contains all the MetadataSources that need to be
		// drained. If in a given loop iteration no component that can drain
		// metadata from these sources is found, the metadataSourcesQueue should be
		// added as part of one of the last unconnected inputDAGs in
		// streamIDToInputOp. This is to avoid cycles.
		metadataSourcesQueue := make([]execinfrapb.MetadataSource, 0, 1)
		// toClose is similar to metadataSourcesQueue with the difference that these
		// components do not produce metadata and should be Closed even during
		// non-graceful termination.
		toClose := make([]colexec.IdempotentCloser, 0, 1)
		inputs = inputs[:0]
		for i := range pspec.Input {
			input, metadataSources, err := s.setupInput(ctx, flowCtx, pspec.Input[i], opt, factory)
			if err != nil {
				return nil, err
			}
			metadataSourcesQueue = append(metadataSourcesQueue, metadataSources...)
			inputs = append(inputs, input)
		}

		args := colexec.NewColOperatorArgs{
			Spec:                 pspec,
			Inputs:               inputs,
			StreamingMemAccount:  s.newStreamingMemAccount(flowCtx),
			ProcessorConstructor: rowexec.NewProcessor,
			DiskQueueCfg:         s.diskQueueCfg,
			FDSemaphore:          s.fdSemaphore,
			ExprHelper:           s.exprHelper,
		}
		result, err := colbuilder.NewColOperator(ctx, flowCtx, args)
		// Even when err is non-nil, it is possible that the buffering memory
		// monitor and account have been created, so we always want to accumulate
		// them for a proper cleanup.
		s.monitors = append(s.monitors, result.OpMonitors...)
		s.accounts = append(s.accounts, result.OpAccounts...)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to vectorize execution plan")
		}
		if flowCtx.Cfg != nil && flowCtx.Cfg.TestingKnobs.EnableVectorizedInvariantsChecker {
			result.Op = colexec.NewInvariantsChecker(result.Op)
		}
		if flowCtx.EvalCtx.SessionData.VectorizeMode == sessiondata.Vectorize201Auto &&
			!result.IsStreaming {
			return nil, errors.Errorf("non-streaming operator encountered when vectorize=201auto")
		}
		// We created a streaming memory account when calling NewColOperator above,
		// so there is definitely at least one memory account, and it doesn't
		// matter which one we grow.
		if err = s.streamingMemAccounts[0].Grow(ctx, int64(result.InternalMemUsage)); err != nil {
			return nil, errors.Wrapf(err, "not enough memory to setup vectorized plan")
		}
		metadataSourcesQueue = append(metadataSourcesQueue, result.MetadataSources...)
		toClose = append(toClose, result.ToClose...)

		op := result.Op
		if s.recordingStats {
			op, err = s.wrapWithVectorizedStatsCollector(
				op, inputs, pspec.ProcessorID, execinfrapb.ProcessorIDTagKey, result.OpMonitors,
			)
			if err != nil {
				return nil, err
			}
		}

		if (flowCtx.EvalCtx.SessionData.VectorizeMode == sessiondata.Vectorize201Auto) &&
			pspec.Output[0].Type == execinfrapb.OutputRouterSpec_BY_HASH {
			// colexec.HashRouter is not supported when vectorize=auto since it can
			// buffer an unlimited number of tuples, even though it falls back to
			// disk. vectorize=on does support this.
			return nil, errors.Errorf("hash router encountered when vectorize=201auto")
		}
		if err = s.setupOutput(
			ctx, flowCtx, pspec, op, result.ColumnTypes, metadataSourcesQueue, toClose, factory,
		); err != nil {
			return nil, err
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
				procIdx, ok := streamIDToSpecIdx[outputStream.StreamID]
				if !ok {
					return nil, errors.Errorf("couldn't find stream %d", outputStream.StreamID)
				}
				outputSpec := &processorSpecs[procIdx]
				for k := range outputSpec.Input {
					for l := range outputSpec.Input[k].Streams {
						inputStream := outputSpec.Input[k].Streams[l]
						if inputStream.StreamID == outputStream.StreamID {
							if err := assertTypesMatch(outputSpec.Input[k].ColumnTypes, result.ColumnTypes); err != nil {
								return nil, err
							}
						}
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
				queue = append(queue, procIdx)
			}
		}
	}

	if len(s.vectorizedStatsCollectorsQueue) > 0 {
		colexecerror.InternalError("not all vectorized stats collectors have been processed")
	}
	return s.leaves, nil
}

// assertTypesMatch checks whether expected types match with actual types and
// returns an error if not.
func assertTypesMatch(expected []*types.T, actual []*types.T) error {
	for i := range expected {
		if !expected[i].Identical(actual[i]) {
			return errors.Errorf("mismatched types at index %d: expected %v\tactual %v ",
				i, expected, actual,
			)
		}
	}
	return nil
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
	f *flowinfra.FlowBase
}

var _ flowCreatorHelper = &vectorizedFlowCreatorHelper{}

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
	processors := make([]execinfra.Processor, 1)
	processors[0] = m
	r.f.SetProcessors(processors)
}

func (r *vectorizedFlowCreatorHelper) getCancelFlowFn() context.CancelFunc {
	return r.f.GetCancelFlowFn()
}

// noopFlowCreatorHelper is a flowCreatorHelper that only performs sanity
// checks.
type noopFlowCreatorHelper struct {
	inboundStreams map[execinfrapb.StreamID]struct{}
}

var _ flowCreatorHelper = &noopFlowCreatorHelper{}

func newNoopFlowCreatorHelper() *noopFlowCreatorHelper {
	return &noopFlowCreatorHelper{
		inboundStreams: make(map[execinfrapb.StreamID]struct{}),
	}
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

// SupportsVectorized checks whether flow is supported by the vectorized engine
// and returns an error if it isn't. Note that it does so by setting up the
// full flow without running the components asynchronously.
// It returns a list of the leaf operators of all flows for the purposes of
// EXPLAIN output.
// Note that passed-in output can be nil, but if it is non-nil, only Types()
// method on it might be called (nothing will actually get Push()'ed into it).
// - scheduledOnRemoteNode indicates whether the flow that processorSpecs
// represent is scheduled to be run on a remote node (different from the one
// performing this check).
func SupportsVectorized(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorSpecs []execinfrapb.ProcessorSpec,
	isPlanLocal bool,
	output execinfra.RowReceiver,
	scheduledOnRemoteNode bool,
) (leaves []execinfra.OpNode, err error) {
	if output == nil {
		output = &execinfra.RowChannel{}
	}
	fuseOpt := flowinfra.FuseNormally
	if isPlanLocal {
		fuseOpt = flowinfra.FuseAggressively
	}
	// We want to force the expression deserialization if this flow is actually
	// scheduled to be on the remote node in order to make sure that during
	// actual execution the remote node will be able to deserialize the
	// expressions without an error.
	forceExprDeserialization := scheduledOnRemoteNode
	creator := newVectorizedFlowCreator(
		newNoopFlowCreatorHelper(), vectorizedRemoteComponentCreator{}, false,
		nil, output, nil, execinfrapb.FlowID{}, colcontainer.DiskQueueCfg{},
		flowCtx.Cfg.VecFDSemaphore, forceExprDeserialization,
	)
	// We create an unlimited memory account because we're interested whether the
	// flow is supported via the vectorized engine in general (without paying
	// attention to the memory since it is node-dependent in the distributed
	// case).
	memoryMonitor := mon.MakeMonitor(
		"supports-vectorized",
		mon.MemoryResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment */
		math.MaxInt64, /* noteworthy */
		flowCtx.Cfg.Settings,
	)
	memoryMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
	defer memoryMonitor.Stop(ctx)
	defer func() {
		for _, acc := range creator.streamingMemAccounts {
			acc.Close(ctx)
		}
		for _, acc := range creator.accounts {
			acc.Close(ctx)
		}
		for _, mon := range creator.monitors {
			mon.Stop(ctx)
		}
	}()
	if vecErr := colexecerror.CatchVectorizedRuntimeError(func() {
		leaves, err = creator.setupFlow(ctx, flowCtx, processorSpecs, fuseOpt)
	}); vecErr != nil {
		return leaves, vecErr
	}
	return leaves, err
}

// VectorizeAlwaysException is an object that returns whether or not execution
// should continue if vectorize=experimental_always and an error occurred when
// setting up the vectorized flow. Consider the case in which
// vectorize=experimental_always. The user must be able to unset this session
// variable without getting an error.
type VectorizeAlwaysException interface {
	// IsException returns whether this object should be an exception to the rule
	// that an inability to run this node in a vectorized flow should produce an
	// error.
	// TODO(asubiotto): This is the cleanest way I can think of to not error out
	// on SET statements when running with vectorize = experimental_always. If
	// there is a better way, we should get rid of this interface.
	IsException() bool
}
