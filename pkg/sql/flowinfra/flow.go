// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package flowinfra

import (
	"context"
	"sync"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execopnode"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execreleasable"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/cancelchecker"
	"github.com/cockroachdb/cockroach/pkg/util/ctxlog"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/optional"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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
	// noWait is set true when the flow is bound to a pausable portal. With it set,
	// the function returns without waiting the all goroutines to finish. For a
	// pausable portal we will persist this flow and reuse it when re-executing
	// the portal. The flow will be cleaned when the portal is closed, rather than
	// when each portal execution finishes.
	//
	// The caller needs to call f.Cleanup().
	Run(ctx context.Context, noWait bool)

	// Resume continues running the Flow after it has been paused with the new
	// output receiver. The Flow is expected to have exactly one processor.
	// It is called when resuming a paused portal.
	// The lifecycle of a flow for a pausable portal is:
	// - flow.Run(ctx, true /* noWait */) (only once)
	// - flow.Resume() (for all re-executions of the portal)
	// - flow.Cleanup() (only once)
	Resume(recv execinfra.RowReceiver)

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
	// any goroutine.
	Cancel()

	// AddOnCleanupStart adds a callback to be executed at the very beginning of
	// Cleanup.
	AddOnCleanupStart(fn func())

	// GetOnCleanupFns returns a couple of functions that should be called at
	// the very beginning and the very end of Cleanup, respectively. Both will
	// be non-nil.
	GetOnCleanupFns() (startCleanup func(), endCleanup func(context.Context))

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

	// resumeCtx is only captured for using inside of Flow.Resume() implementations.
	resumeCtx context.Context

	flowRegistry *FlowRegistry

	// processors contains a subset of the processors in the flow - the ones that
	// run in their own goroutines. Some processors that implement RowSource are
	// scheduled to run in their consumer's goroutine; those are not present here.
	processors []execinfra.Processor
	// outputs contains an output for each execinfra.Processor in processors.
	outputs []execinfra.RowReceiver
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

	localProcessors    []execinfra.LocalProcessor
	localVectorSources map[int32]any

	// startedGoroutines specifies whether this flow started any goroutines. This
	// is used in Wait() to avoid the overhead of waiting for non-existent
	// goroutines.
	startedGoroutines bool

	// headProcStarted tracks whether Start was called on the "head" processor
	// in Run.
	headProcStarted bool

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
	onCleanupEnd   func(context.Context)

	statementSQL string

	mu struct {
		syncutil.Mutex
		status flowStatus
		// Cancel function for ctx. Call this to cancel the flow (safe to be
		// called multiple times).
		//
		// NB: must be used with care as this function should **not** be called
		// once the Flow has been cleaned up. Consider using Flow.Cancel
		// instead when unsure.
		ctxCancel context.CancelFunc
	}

	ctxDone <-chan struct{}

	// sp is the span that this Flow runs in. Can be nil if no span was created
	// for the flow. Flow.Cleanup() finishes it.
	sp *tracing.Span

	// spec is the request that produced this flow. Only used for debugging.
	spec *execinfrapb.FlowSpec

	admissionInfo admission.WorkInfo
}

func (f *FlowBase) getStatus() flowStatus {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.mu.status
}

func (f *FlowBase) setStatus(status flowStatus) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mu.status = status
}

// Setup is part of the Flow interface.
func (f *FlowBase) Setup(
	ctx context.Context, spec *execinfrapb.FlowSpec, _ FuseOpt,
) (context.Context, execopnode.OpChains, error) {
	ctx, f.mu.ctxCancel = ctxlog.WithCancel(ctx)
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
	return f.getStatus() != flowNotStarted
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
	localVectorSources map[int32]any,
	onFlowCleanupEnd func(context.Context),
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
		localVectorSources:    localVectorSources,
		admissionInfo:         admissionInfo,
		onCleanupEnd:          onFlowCleanupEnd,
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
// this flow. The returned function is only safe to be used before Flow.Cleanup
// has been called.
func (f *FlowBase) GetCancelFlowFn() context.CancelFunc {
	return f.mu.ctxCancel
}

// SetProcessorsAndOutputs overrides the current f.processors and f.outputs with
// the provided slices.
func (f *FlowBase) SetProcessorsAndOutputs(
	processors []execinfra.Processor, outputs []execinfra.RowReceiver,
) error {
	if len(processors) != len(outputs) {
		return errors.AssertionFailedf(
			"processors and outputs don't match: %d processors, %d outputs",
			len(processors), len(outputs),
		)
	}
	f.processors = processors
	f.outputs = outputs
	return nil
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

// GetLocalVectorSources return the LocalVectorSources of this flow.
func (f *FlowBase) GetLocalVectorSources() map[int32]any {
	return f.localVectorSources
}

// GetAdmissionInfo returns the information to use for admission control on
// responses received from a remote flow.
func (f *FlowBase) GetAdmissionInfo() admission.WorkInfo {
	return f.admissionInfo
}

// StartInternal starts the flow. All processors are started, each in their own
// goroutine. The caller must forward any returned error to rowSyncFlowConsumer if
// set.
func (f *FlowBase) StartInternal(
	ctx context.Context, processors []execinfra.Processor, outputs []execinfra.RowReceiver,
) error {
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

	f.setStatus(flowRunning)

	if execinfra.IncludeRUEstimateInExplainAnalyze.Get(&f.Cfg.Settings.SV) &&
		!f.Gateway && f.CollectStats {
		// Remote flows begin collecting CPU usage here, and finish when the last
		// outbox finishes. Gateway flows are handled by the connExecutor.
		f.FlowCtx.TenantCPUMonitor.StartCollection(ctx, f.Cfg.TenantCostController)
	}

	if log.V(1) {
		log.Infof(ctx, "registered flow %s", f.ID.Short())
	}
	for _, s := range f.startables {
		// Note that it is safe to pass the context cancellation function
		// directly since the main goroutine of the Flow will block until all
		// startable goroutines exit.
		s.Start(ctx, &f.waitGroup, f.mu.ctxCancel)
	}
	for i := 0; i < len(processors); i++ {
		f.waitGroup.Add(1)
		go func(i int) {
			processors[i].Run(ctx, outputs[i])
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
	return f.StartInternal(ctx, f.processors, f.outputs)
}

// Resume is part of the Flow interface.
func (f *FlowBase) Resume(recv execinfra.RowReceiver) {
	if len(f.processors) != 1 || len(f.outputs) != 1 {
		recv.Push(
			nil, /* row */
			&execinfrapb.ProducerMetadata{
				Err: errors.AssertionFailedf(
					"length of both the processor and the output must be 1",
				)})
		recv.ProducerDone()
		return
	}

	f.outputs[0] = recv
	log.VEventf(f.resumeCtx, 1, "resuming %T in the flow's goroutine", f.processors[0])
	f.processors[0].Resume(recv)
}

// Run is part of the Flow interface.
func (f *FlowBase) Run(ctx context.Context, noWait bool) {
	if !noWait {
		defer f.Wait()
	}

	if len(f.processors) == 0 {
		f.rowSyncFlowConsumer.Push(nil /* row */, &execinfrapb.ProducerMetadata{Err: errors.AssertionFailedf("no processors in flow")})
		f.rowSyncFlowConsumer.ProducerDone()
		return
	}
	// We'll take care of the last processor in particular.
	headProc := f.processors[len(f.processors)-1]
	headOutput := f.outputs[len(f.outputs)-1]
	otherProcs := f.processors[:len(f.processors)-1]
	otherOutputs := f.outputs[:len(f.outputs)-1]

	var err error
	if err = f.StartInternal(ctx, otherProcs, otherOutputs); err != nil {
		f.rowSyncFlowConsumer.Push(nil /* row */, &execinfrapb.ProducerMetadata{Err: err})
		f.rowSyncFlowConsumer.ProducerDone()
		return
	}
	f.resumeCtx = ctx
	log.VEventf(ctx, 1, "running %T in the flow's goroutine", headProc)
	f.headProcStarted = true
	headProc.Run(ctx, headOutput)
}

// Wait is part of the Flow interface.
func (f *FlowBase) Wait() {
	if !f.startedGoroutines {
		return
	}

	var panicVal interface{}
	if panicVal = recover(); panicVal != nil {
		// If Wait is called as part of stack unwinding during a panic, the flow
		// context must be canceled to ensure that all asynchronous goroutines
		// get the message that they must exit (otherwise we will wait
		// indefinitely).
		//
		// Cleanup is only called _after_ Wait, so it's safe to use ctxCancel
		// directly.
		f.mu.ctxCancel()
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
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.mu.status == flowFinished {
		// The Flow is already done, nothing to cancel.
		return
	}
	f.mu.ctxCancel()
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
var noopCtxFn = func(context.Context) {}

// GetOnCleanupFns is part of the Flow interface.
func (f *FlowBase) GetOnCleanupFns() (startCleanup func(), endCleanup func(context.Context)) {
	onCleanupStart, onCleanupEnd := f.onCleanupStart, f.onCleanupEnd
	if onCleanupStart == nil {
		onCleanupStart = noopFn
	}
	if onCleanupEnd == nil {
		onCleanupEnd = noopCtxFn
	}
	return onCleanupStart, onCleanupEnd
}

// Cleanup is part of the Flow interface.
// NOTE: this implements only the shared cleanup logic between row-based and
// vectorized flows.
func (f *FlowBase) Cleanup(ctx context.Context) {
	if buildutil.CrdbTestBuild {
		if f.getStatus() == flowFinished {
			panic("flow cleanup called twice")
		}
	}

	// Ensure that all processors are closed. Usually this is done automatically
	// (when a processor is exhausted or at the end of execinfra.Run loop), but
	// in edge cases we need to do it here. Close can be called multiple times.
	//
	// Note that Close is not thread-safe, but at this point if the processor
	// wasn't fused and ran in its own goroutine, that goroutine must have
	// exited since Cleanup is called after having waited for all started
	// goroutines to exit.
	for _, proc := range f.processors {
		proc.Close(ctx)
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
				Component: f.FlowCtx.FlowComponentID(),
				FlowStats: execinfrapb.FlowStats{
					MaxMemUsage:  optional.MakeUint(uint64(f.FlowCtx.Mon.MaximumBytes())),
					MaxDiskUsage: optional.MakeUint(uint64(f.FlowCtx.DiskMonitor.MaximumBytes())),
				},
			})
		}
	}

	// This closes the monitors opened in ServerImpl.setupFlow.
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
	// Importantly, we must mark the Flow as finished before f.sp is finished in
	// the defer above.
	f.setStatus(flowFinished)
	f.mu.ctxCancel()
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
