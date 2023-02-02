// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package flowinfra

import (
	"context"
	"sync"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execopnode"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execreleasable"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/cancelchecker"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/optional"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

type flowStatus int

// Flow status indicators.
const (
	flowNotStarted flowStatus = iota
	flowRunning
	flowFinished
)

// Startable is any component that can be started (a router or an outbox).
type Startable interface {
	Start(ctx context.Context, wg *sync.WaitGroup, flowCtxCancel context.CancelFunc)
}

// StartableFn is an adapter when a customer function (i.e. a custom goroutine)
// needs to become Startable.
type StartableFn func(context.Context, *sync.WaitGroup, context.CancelFunc)

// Start is a part of the Startable interface.
func (f StartableFn) Start(
	ctx context.Context, wg *sync.WaitGroup, flowCtxCancel context.CancelFunc,
) {
	f(ctx, wg, flowCtxCancel)
}

// FuseOpt specifies options for processor fusing at Flow.Setup() time.
type FuseOpt bool

const (
	// FuseNormally means fuse what you can, but don't serialize unordered input
	// synchronizers.
	FuseNormally FuseOpt = false
	// FuseAggressively means serialize unordered input synchronizers.
	// This is useful for flows that might have mutations which can't have any
	// concurrency.
	FuseAggressively = true
)

// Flow represents a flow which consists of processors and streams.
type Flow interface {
	// Setup sets up all the infrastructure for the flow as defined by the flow
	// spec. The flow will then need to be started or run. A new context (along
	// with a context cancellation function) is derived. The new context must be
	// used when running the flow so that all components running in their own
	// goroutines could listen for a cancellation on the same context.
	//
	// The second return argument contains all operator chains planned on the
	// gateway node if the flow is vectorized and the physical plan is fully
	// local (in all other cases the second return argument is nil).
	Setup(ctx context.Context, spec *execinfrapb.FlowSpec, opt FuseOpt) (context.Context, execopnode.OpChains, error)

	// SetTxn is used to provide the transaction in which the flow will run.
	// It needs to be called after Setup() and before Start/Run.
	SetTxn(*kv.Txn)

	// Start starts the flow. Processors run asynchronously in their own
	// goroutines. Wait() needs to be called to wait for the flow to finish.
	// See Run() for a synchronous version.
	//
	// If errors are encountered during the setup part, they're returned.
	Start(context.Context) error

	// Run runs the flow to completion. The last processor is run in the current
	// goroutine; others may run in different goroutines depending on how the
	// flow was configured.
	//
	// f.Wait() is called internally, so the call blocks until all the flow's
	// goroutines are done.
	//
	// It is assumed that rowSyncFlowConsumer is set, so all errors encountered
	// when running this flow are sent to it.
	//
	// The caller needs to call f.Cleanup().
	Run(context.Context)

	// Wait waits for all the goroutines for this flow to exit. If the context gets
	// canceled before all goroutines exit, it calls f.cancel().
	Wait()

	// IsLocal returns whether this flow is being run as part of a local-only
	// query.
	IsLocal() bool

	// StatementSQL is the SQL statement for which this flow is executing. It is
	// populated on a best effort basis (only available for user-issued queries
	// that are also not like BulkIO/CDC related).
	StatementSQL() string

	// GetFlowCtx returns the flow context of this flow.
	GetFlowCtx() *execinfra.FlowCtx

	// AddStartable accumulates a Startable object.
	AddStartable(Startable)

	// GetID returns the flow ID.
	GetID() execinfrapb.FlowID

	// MemUsage returns the estimated memory footprint of this Flow object. Note
	// that this ignores all the memory usage of the components that are created
	// on behalf of this Flow.
	MemUsage() int64

	// Cancel cancels the flow by canceling its context. Safe to be called from
	// any goroutine but **cannot** be called after (or concurrently with)
	// Cleanup.
	Cancel()

	// AddOnCleanupStart adds a callback to be executed at the very beginning of
	// Cleanup.
	AddOnCleanupStart(fn func())

	// GetOnCleanupFns returns a couple of functions that should be called at
	// the very beginning and the very end of Cleanup, respectively. Both will
	// be non-nil.
	GetOnCleanupFns() (startCleanup, endCleanup func())

	// Cleanup must be called whenever the flow is done (meaning it either
	// completes gracefully after all processors and mailboxes exited or an
	// error is encountered that stops the flow from making progress). The
	// implementations must be safe to execute in case the Flow is never Run()
	// or Start()ed.
	Cleanup(context.Context)

	// ConcurrentTxnUse returns true if multiple processors/operators in the flow
	// will execute concurrently (i.e. if not all of them have been fused) and
	// more than one goroutine will be using a txn.
	// Can only be called after Setup().
	ConcurrentTxnUse() bool
}

// FlowBase is the shared logic between row based and vectorized flows. It
// implements Flow interface for convenience and for usage in tests, but if
// FlowBase.Setup is called, it'll panic.
type FlowBase struct {
	execinfra.FlowCtx

	flowRegistry *FlowRegistry

	// processors contains a subset of the processors in the flow - the ones that
	// run in their own goroutines. Some processors that implement RowSource are
	// scheduled to run in their consumer's goroutine; those are not present here.
	processors []execinfra.Processor
	// startables are entities that must be started when the flow starts;
	// currently these are outboxes and routers.
	startables []Startable
	// rowSyncFlowConsumer is a special execinfra.RowReceiver which, instead of
	// sending rows to another host (as the outboxes do), returns them directly
	// (to the local host). It is always set.
	rowSyncFlowConsumer execinfra.RowReceiver
	// batchSyncFlowConsumer, if set, provides an alternative interface for
	// pushing coldata.Batches to locally.
	batchSyncFlowConsumer execinfra.BatchReceiver

	localProcessors []execinfra.LocalProcessor

	// startedGoroutines specifies whether this flow started any goroutines. This
	// is used in Wait() to avoid the overhead of waiting for non-existent
	// goroutines.
	startedGoroutines bool

	// inboundStreams are streams that receive data from other hosts; this map
	// is to be passed to FlowRegistry.RegisterFlow. This map is populated in
	// Flow.Setup(), so it is safe to lookup into concurrently later.
	inboundStreams map[execinfrapb.StreamID]*InboundStreamInfo

	// waitGroup is used to wait for async components of the flow:
	//  - processors
	//  - inbound streams
	//  - outboxes
	waitGroup sync.WaitGroup

	// onCleanupStart and onCleanupEnd will be called in the very beginning and
	// the very end of Cleanup(), respectively.
	onCleanupStart func()
	onCleanupEnd   func()

	statementSQL string

	status flowStatus

	// Cancel function for ctx. Call this to cancel the flow (safe to be called
	// multiple times).
	ctxCancel context.CancelFunc
	ctxDone   <-chan struct{}

	// sp is the span that this Flow runs in. Can be nil if no span was created
	// for the flow. Flow.Cleanup() finishes it.
	sp *tracing.Span

	// spec is the request that produced this flow. Only used for debugging.
	spec *execinfrapb.FlowSpec

	admissionInfo admission.WorkInfo
}

// Setup is part of the Flow interface.
func (f *FlowBase) Setup(
	ctx context.Context, spec *execinfrapb.FlowSpec, _ FuseOpt,
) (context.Context, execopnode.OpChains, error) {
	ctx, f.ctxCancel = contextutil.WithCancel(ctx)
	f.ctxDone = ctx.Done()
	f.spec = spec
	return ctx, nil, nil
}

// SetTxn is part of the Flow interface.
func (f *FlowBase) SetTxn(txn *kv.Txn) {
	f.FlowCtx.Txn = txn
	f.EvalCtx.Txn = txn
}

// ConcurrentTxnUse is part of the Flow interface.
func (f *FlowBase) ConcurrentTxnUse() bool {
	numProcessorsThatMightUseTxn := 0
	for _, proc := range f.processors {
		if txnUser, ok := proc.(execinfra.DoesNotUseTxn); !ok || !txnUser.DoesNotUseTxn() {
			numProcessorsThatMightUseTxn++
			if numProcessorsThatMightUseTxn > 1 {
				return true
			}
		}
	}
	return false
}

// SetStartedGoroutines sets FlowBase.startedGoroutines to the passed in value.
// This allows notifying the FlowBase about the concurrent goroutines which are
// started outside of the FlowBase.StartInternal machinery.
func (f *FlowBase) SetStartedGoroutines(val bool) {
	f.startedGoroutines = val
}

// Started returns true if f has either been Run() or Start()ed.
func (f *FlowBase) Started() bool {
	return f.status != flowNotStarted
}

var _ Flow = &FlowBase{}

// NewFlowBase creates a new FlowBase.
//
// sp, if not nil, is the Span corresponding to the flow. The flow takes
// ownership; Cleanup() will finish it.
func NewFlowBase(
	flowCtx execinfra.FlowCtx,
	sp *tracing.Span,
	flowReg *FlowRegistry,
	rowSyncFlowConsumer execinfra.RowReceiver,
	batchSyncFlowConsumer execinfra.BatchReceiver,
	localProcessors []execinfra.LocalProcessor,
	onFlowCleanupEnd func(),
	statementSQL string,
) *FlowBase {
	// We are either in a single tenant cluster, or a SQL node in a multi-tenant
	// cluster, where the SQL node is single tenant. The tenant below is used
	// within SQL (not KV), so using an arbitrary tenant is ok -- we choose to
	// use SystemTenantID since it is already defined.
	admissionInfo := admission.WorkInfo{TenantID: roachpb.SystemTenantID}
	if flowCtx.Txn == nil {
		admissionInfo.Priority = admissionpb.NormalPri
		admissionInfo.CreateTime = timeutil.Now().UnixNano()
	} else {
		h := flowCtx.Txn.AdmissionHeader()
		admissionInfo.Priority = admissionpb.WorkPriority(h.Priority)
		admissionInfo.CreateTime = h.CreateTime
	}
	return &FlowBase{
		FlowCtx:               flowCtx,
		sp:                    sp,
		flowRegistry:          flowReg,
		rowSyncFlowConsumer:   rowSyncFlowConsumer,
		batchSyncFlowConsumer: batchSyncFlowConsumer,
		localProcessors:       localProcessors,
		admissionInfo:         admissionInfo,
		onCleanupEnd:          onFlowCleanupEnd,
		status:                flowNotStarted,
		statementSQL:          statementSQL,
	}
}

// StatementSQL is part of the Flow interface.
func (f *FlowBase) StatementSQL() string {
	return f.statementSQL
}

// GetFlowCtx is part of the Flow interface.
func (f *FlowBase) GetFlowCtx() *execinfra.FlowCtx {
	return &f.FlowCtx
}

// AddStartable is part of the Flow interface.
func (f *FlowBase) AddStartable(s Startable) {
	f.startables = append(f.startables, s)
}

// GetID is part of the Flow interface.
func (f *FlowBase) GetID() execinfrapb.FlowID {
	return f.ID
}

// CheckInboundStreamID takes a stream ID and returns an error if an inbound
// stream already exists with that ID in the inbound streams map, creating the
// inbound streams map if it is nil.
func (f *FlowBase) CheckInboundStreamID(sid execinfrapb.StreamID) error {
	if _, found := f.inboundStreams[sid]; found {
		return errors.Errorf("inbound stream %d already exists in map", sid)
	}
	if f.inboundStreams == nil {
		f.inboundStreams = make(map[execinfrapb.StreamID]*InboundStreamInfo)
	}
	return nil
}

// GetWaitGroup returns the wait group of this flow.
func (f *FlowBase) GetWaitGroup() *sync.WaitGroup {
	return &f.waitGroup
}

// GetCtxDone returns done channel of the context of this flow.
func (f *FlowBase) GetCtxDone() <-chan struct{} {
	return f.ctxDone
}

// GetCancelFlowFn returns the context cancellation function of the context of
// this flow.
func (f *FlowBase) GetCancelFlowFn() context.CancelFunc {
	return f.ctxCancel
}

// SetProcessors overrides the current f.processors with the provided
// processors. This is used to set up the vectorized flow.
func (f *FlowBase) SetProcessors(processors []execinfra.Processor) {
	f.processors = processors
}

// AddRemoteStream adds a remote stream to this flow.
func (f *FlowBase) AddRemoteStream(streamID execinfrapb.StreamID, streamInfo *InboundStreamInfo) {
	f.inboundStreams[streamID] = streamInfo
}

// GetRowSyncFlowConsumer returns the special rowSyncFlowConsumer outbox.
func (f *FlowBase) GetRowSyncFlowConsumer() execinfra.RowReceiver {
	return f.rowSyncFlowConsumer
}

// GetBatchSyncFlowConsumer returns the special batchSyncFlowConsumer outbox.
// Will return nil if the consumer cannot receive batches.
func (f *FlowBase) GetBatchSyncFlowConsumer() execinfra.BatchReceiver {
	return f.batchSyncFlowConsumer
}

// GetLocalProcessors return the execinfra.LocalProcessors of this flow.
func (f *FlowBase) GetLocalProcessors() []execinfra.LocalProcessor {
	return f.localProcessors
}

// GetAdmissionInfo returns the information to use for admission control on
// responses received from a remote flow.
func (f *FlowBase) GetAdmissionInfo() admission.WorkInfo {
	return f.admissionInfo
}

// StartInternal starts the flow. All processors are started, each in their own
// goroutine. The caller must forward any returned error to rowSyncFlowConsumer if
// set.
func (f *FlowBase) StartInternal(ctx context.Context, processors []execinfra.Processor) error {
	log.VEventf(
		ctx, 1, "starting (%d processors, %d startables) asynchronously", len(processors), len(f.startables),
	)

	// Only register the flow if it is a part of the distributed plan. This is
	// needed to satisfy two different use cases:
	// 1. there are inbound stream connections that need to look up this flow in
	// the flow registry. This can only happen if the plan is not fully local
	// (since those inbound streams originate on different nodes).
	// 2. when the node is draining, the flow registry can cancel all running
	// non-fully local flows if they don't finish on their own during the grace
	// period. Cancellation of local flows occurs by cancelling the connections
	// that the local flows were spinned up for.
	if !f.IsLocal() {
		// Once we call RegisterFlow, the inbound streams become accessible; we
		// must set up the WaitGroup counter before.
		// The counter will be further incremented below to account for the
		// processors.
		f.waitGroup.Add(len(f.inboundStreams))

		if err := f.flowRegistry.RegisterFlow(
			ctx, f.ID, f, f.inboundStreams, SettingFlowStreamTimeout.Get(&f.FlowCtx.Cfg.Settings.SV),
		); err != nil {
			return err
		}
	}

	f.status = flowRunning

	if multitenant.TenantRUEstimateEnabled.Get(&f.Cfg.Settings.SV) &&
		!f.Gateway && f.CollectStats {
		// Remote flows begin collecting CPU usage here, and finish when the last
		// outbox finishes. Gateway flows are handled by the connExecutor.
		f.FlowCtx.TenantCPUMonitor.StartCollection(ctx, f.Cfg.TenantCostController)
	}

	if log.V(1) {
		log.Infof(ctx, "registered flow %s", f.ID.Short())
	}
	for _, s := range f.startables {
		s.Start(ctx, &f.waitGroup, f.ctxCancel)
	}
	for i := 0; i < len(processors); i++ {
		f.waitGroup.Add(1)
		go func(i int) {
			processors[i].Run(ctx)
			f.waitGroup.Done()
		}(i)
	}
	// Note that we might have already set f.startedGoroutines to true if it is
	// a vectorized flow with a parallel unordered synchronizer. That component
	// starts goroutines on its own, so we need to preserve that fact so that we
	// correctly wait in Wait().
	f.startedGoroutines = f.startedGoroutines || len(f.startables) > 0 || len(processors) > 0 || len(f.inboundStreams) > 0
	return nil
}

// IsLocal returns whether this flow is being run as part of a local-only query.
func (f *FlowBase) IsLocal() bool {
	return f.Local
}

// Start is part of the Flow interface.
func (f *FlowBase) Start(ctx context.Context) error {
	return f.StartInternal(ctx, f.processors)
}

// Run is part of the Flow interface.
func (f *FlowBase) Run(ctx context.Context) {
	defer f.Wait()

	// We'll take care of the last processor in particular.
	var headProc execinfra.Processor
	if len(f.processors) == 0 {
		f.rowSyncFlowConsumer.Push(nil /* row */, &execinfrapb.ProducerMetadata{Err: errors.AssertionFailedf("no processors in flow")})
		f.rowSyncFlowConsumer.ProducerDone()
		return
	}
	headProc = f.processors[len(f.processors)-1]
	otherProcs := f.processors[:len(f.processors)-1]

	var err error
	if err = f.StartInternal(ctx, otherProcs); err != nil {
		f.rowSyncFlowConsumer.Push(nil /* row */, &execinfrapb.ProducerMetadata{Err: err})
		f.rowSyncFlowConsumer.ProducerDone()
		return
	}
	log.VEventf(ctx, 1, "running %T in the flow's goroutine", headProc)
	headProc.Run(ctx)
}

// Wait is part of the Flow interface.
func (f *FlowBase) Wait() {
	if !f.startedGoroutines {
		return
	}

	var panicVal interface{}
	if panicVal = recover(); panicVal != nil {
		// If Wait is called as part of stack unwinding during a panic, the flow
		// context must be canceled to ensure that all asynchronous goroutines get
		// the message that they must exit (otherwise we will wait indefinitely).
		f.ctxCancel()
	}
	waitChan := make(chan struct{})

	go func() {
		f.waitGroup.Wait()
		close(waitChan)
	}()

	select {
	case <-f.ctxDone:
		f.cancel()
		<-waitChan
	case <-waitChan:
		// Exit normally
	}
	if panicVal != nil {
		panic(panicVal)
	}
}

const flowBaseOverhead = int64(unsafe.Sizeof(FlowBase{}))

// MemUsage is part of the Flow interface.
func (f *FlowBase) MemUsage() int64 {
	return flowBaseOverhead + int64(len(f.statementSQL))
}

// Cancel is part of the Flow interface.
func (f *FlowBase) Cancel() {
	f.ctxCancel()
}

// AddOnCleanupStart is part of the Flow interface.
func (f *FlowBase) AddOnCleanupStart(fn func()) {
	if f.onCleanupStart != nil {
		oldOnCleanupStart := f.onCleanupStart
		f.onCleanupStart = func() {
			fn()
			oldOnCleanupStart()
		}
	} else {
		f.onCleanupStart = fn
	}
}

var noopFn = func() {}

// GetOnCleanupFns is part of the Flow interface.
func (f *FlowBase) GetOnCleanupFns() (startCleanup, endCleanup func()) {
	onCleanupStart, onCleanupEnd := f.onCleanupStart, f.onCleanupEnd
	if onCleanupStart == nil {
		onCleanupStart = noopFn
	}
	if onCleanupEnd == nil {
		onCleanupEnd = noopFn
	}
	return onCleanupStart, onCleanupEnd
}

// Cleanup is part of the Flow interface.
// NOTE: this implements only the shared clean up logic between row-based and
// vectorized flows.
func (f *FlowBase) Cleanup(ctx context.Context) {
	if f.status == flowFinished {
		panic("flow cleanup called twice")
	}

	// Release any descriptors accessed by this flow.
	if f.Descriptors != nil && f.IsDescriptorsCleanupRequired {
		f.Descriptors.ReleaseAll(ctx)
	}

	if f.sp != nil {
		defer f.sp.Finish()
		if f.Gateway && f.CollectStats {
			// If this is the gateway node and we're collecting execution stats,
			// output the maximum memory usage to the flow span. Note that
			// non-gateway nodes use the last outbox to send this information
			// over.
			f.sp.RecordStructured(&execinfrapb.ComponentStats{
				Component: execinfrapb.FlowComponentID(f.NodeID.SQLInstanceID(), f.FlowCtx.ID),
				FlowStats: execinfrapb.FlowStats{
					MaxMemUsage:  optional.MakeUint(uint64(f.FlowCtx.Mon.MaximumBytes())),
					MaxDiskUsage: optional.MakeUint(uint64(f.FlowCtx.DiskMonitor.MaximumBytes())),
				},
			})
		}
	}

	// This closes the disk monitor opened in newFlowContext as well as the
	// memory monitor opened in ServerImpl.setupFlow.
	if r := recover(); r != nil {
		f.DiskMonitor.EmergencyStop(ctx)
		f.Mon.EmergencyStop(ctx)
		panic(r)
	} else {
		f.DiskMonitor.Stop(ctx)
		f.Mon.Stop(ctx)
	}
	for _, p := range f.processors {
		if d, ok := p.(execreleasable.Releasable); ok {
			d.Release()
		}
	}
	if log.V(1) {
		log.Infof(ctx, "cleaning up")
	}
	// Local flows do not get registered.
	if !f.IsLocal() && f.Started() {
		f.flowRegistry.UnregisterFlow(f.ID)
	}
	f.status = flowFinished
	f.ctxCancel()
}

// cancel cancels all unconnected streams of this flow. This function is called
// in Wait() after the associated context has been canceled. In order to cancel
// a flow, call f.ctxCancel() instead of this function.
//
// For a detailed description of the distsql query cancellation mechanism,
// read docs/RFCS/query_cancellation.md.
func (f *FlowBase) cancel() {
	if len(f.inboundStreams) == 0 {
		return
	}
	// Pending streams have yet to be started; send an error to its receivers
	// and prevent them from being connected.
	f.flowRegistry.cancelPendingStreams(f.ID, cancelchecker.QueryCanceledError)
}
