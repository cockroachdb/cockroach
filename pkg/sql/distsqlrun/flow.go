// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package distsqlrun

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/opentracing/opentracing-go"
)

type flowStatus int

// Flow status indicators.
const (
	FlowNotStarted flowStatus = iota
	FlowRunning
	FlowFinished
)

type startable interface {
	start(ctx context.Context, wg *sync.WaitGroup, ctxCancel context.CancelFunc)
}

type startableFn func(context.Context, *sync.WaitGroup, context.CancelFunc)

func (f startableFn) start(ctx context.Context, wg *sync.WaitGroup, ctxCancel context.CancelFunc) {
	f(ctx, wg, ctxCancel)
}

// Flow represents a flow which consists of processors and streams.
type Flow struct {
	distsql.FlowCtx

	flowRegistry *flowRegistry
	// isVectorized indicates whether it is a vectorized flow.
	isVectorized bool
	// processors contains a subset of the processors in the flow - the ones that
	// run in their own goroutines. Some processors that implement RowSource are
	// scheduled to run in their consumer's goroutine; those are not present here.
	processors []distsql.Processor
	// startables are entities that must be started when the flow starts;
	// currently these are outboxes and routers.
	startables []startable
	// syncFlowConsumer is a special outbox which instead of sending rows to
	// another host, returns them directly (as a result to a SetupSyncFlow RPC,
	// or to the local host).
	syncFlowConsumer distsql.RowReceiver

	localProcessors []distsql.LocalProcessor

	// startedGoroutines specifies whether this flow started any goroutines. This
	// is used in Wait() to avoid the overhead of waiting for non-existent
	// goroutines.
	startedGoroutines bool

	localStreams map[distsqlpb.StreamID]distsql.RowReceiver

	// inboundStreams are streams that receive data from other hosts; this map
	// is to be passed to flowRegistry.RegisterFlow.
	inboundStreams map[distsqlpb.StreamID]*inboundStreamInfo

	// waitGroup is used to wait for async components of the flow:
	//  - processors
	//  - inbound streams
	//  - outboxes
	waitGroup sync.WaitGroup

	doneFn func()

	status flowStatus

	// Cancel function for ctx. Call this to cancel the flow (safe to be called
	// multiple times).
	ctxCancel context.CancelFunc
	ctxDone   <-chan struct{}

	// spec is the request that produced this flow. Only used for debugging.
	spec *distsqlpb.FlowSpec
}

func newFlow(
	flowCtx distsql.FlowCtx,
	flowReg *flowRegistry,
	syncFlowConsumer distsql.RowReceiver,
	localProcessors []distsql.LocalProcessor,
	isVectorized bool,
) *Flow {
	f := &Flow{
		FlowCtx:          flowCtx,
		flowRegistry:     flowReg,
		isVectorized:     isVectorized,
		syncFlowConsumer: syncFlowConsumer,
		localProcessors:  localProcessors,
	}
	f.status = FlowNotStarted
	return f
}

// checkInboundStreamID takes a stream ID and returns an error if an inbound
// stream already exists with that ID in the inbound streams map, creating the
// inbound streams map if it is nil.
func (f *Flow) checkInboundStreamID(sid distsqlpb.StreamID) error {
	if _, found := f.inboundStreams[sid]; found {
		return errors.Errorf("inbound stream %d already exists in map", sid)
	}
	if f.inboundStreams == nil {
		f.inboundStreams = make(map[distsqlpb.StreamID]*inboundStreamInfo)
	}
	return nil
}

// setupInboundStream adds a stream to the stream map (inboundStreams or
// localStreams).
func (f *Flow) setupInboundStream(
	ctx context.Context, spec distsqlpb.StreamEndpointSpec, receiver distsql.RowReceiver,
) error {
	sid := spec.StreamID
	switch spec.Type {
	case distsqlpb.StreamEndpointSpec_SYNC_RESPONSE:
		return errors.Errorf("inbound stream of type SYNC_RESPONSE")

	case distsqlpb.StreamEndpointSpec_REMOTE:
		if err := f.checkInboundStreamID(sid); err != nil {
			return err
		}
		if log.V(2) {
			log.Infof(ctx, "set up inbound stream %d", sid)
		}
		f.inboundStreams[sid] = &inboundStreamInfo{receiver: rowInboundStreamHandler{receiver}, waitGroup: &f.waitGroup}

	case distsqlpb.StreamEndpointSpec_LOCAL:
		if _, found := f.localStreams[sid]; found {
			return errors.Errorf("local stream %d has multiple consumers", sid)
		}
		if f.localStreams == nil {
			f.localStreams = make(map[distsqlpb.StreamID]distsql.RowReceiver)
		}
		f.localStreams[sid] = receiver

	default:
		return errors.Errorf("invalid stream type %d", spec.Type)
	}

	return nil
}

// setupOutboundStream sets up an output stream; if the stream is local, the
// RowChannel is looked up in the localStreams map; otherwise an outgoing
// mailbox is created.
func (f *Flow) setupOutboundStream(spec distsqlpb.StreamEndpointSpec) (distsql.RowReceiver, error) {
	sid := spec.StreamID
	switch spec.Type {
	case distsqlpb.StreamEndpointSpec_SYNC_RESPONSE:
		return f.syncFlowConsumer, nil

	case distsqlpb.StreamEndpointSpec_REMOTE:
		outbox := newOutbox(&f.FlowCtx, spec.TargetNodeID, f.ID, sid)
		f.startables = append(f.startables, outbox)
		return outbox, nil

	case distsqlpb.StreamEndpointSpec_LOCAL:
		rowChan, found := f.localStreams[sid]
		if !found {
			return nil, errors.Errorf("unconnected inbound stream %d", sid)
		}
		// Once we "connect" a stream, we set the value in the map to nil.
		if rowChan == nil {
			return nil, errors.Errorf("stream %d has multiple connections", sid)
		}
		f.localStreams[sid] = nil
		return rowChan, nil
	default:
		return nil, errors.Errorf("invalid stream type %d", spec.Type)
	}
}

// setupRouter initializes a router and the outbound streams.
//
// Pass-through routers are not supported; they should be handled separately.
func (f *Flow) setupRouter(spec *distsqlpb.OutputRouterSpec) (router, error) {
	streams := make([]distsql.RowReceiver, len(spec.Streams))
	for i := range spec.Streams {
		var err error
		streams[i], err = f.setupOutboundStream(spec.Streams[i])
		if err != nil {
			return nil, err
		}
	}
	return makeRouter(spec, streams)
}

func checkNumInOut(
	inputs []distsql.RowSource, outputs []distsql.RowReceiver, numIn, numOut int,
) error {
	if len(inputs) != numIn {
		return errors.Errorf("expected %d input(s), got %d", numIn, len(inputs))
	}
	if len(outputs) != numOut {
		return errors.Errorf("expected %d output(s), got %d", numOut, len(outputs))
	}
	return nil
}

type copyingRowReceiver struct {
	distsql.RowReceiver
	alloc sqlbase.EncDatumRowAlloc
}

func (r *copyingRowReceiver) Push(
	row sqlbase.EncDatumRow, meta *distsqlpb.ProducerMetadata,
) distsql.ConsumerStatus {
	if row != nil {
		row = r.alloc.CopyRow(row)
	}
	return r.RowReceiver.Push(row, meta)
}

func (f *Flow) makeProcessor(
	ctx context.Context, ps *distsqlpb.ProcessorSpec, inputs []distsql.RowSource,
) (distsql.Processor, error) {
	if len(ps.Output) != 1 {
		return nil, errors.Errorf("only single-output processors supported")
	}
	var output distsql.RowReceiver
	spec := &ps.Output[0]
	if spec.Type == distsqlpb.OutputRouterSpec_PASS_THROUGH {
		// There is no entity that corresponds to a pass-through router - we just
		// use its output stream directly.
		if len(spec.Streams) != 1 {
			return nil, errors.Errorf("expected one stream for passthrough router")
		}
		var err error
		output, err = f.setupOutboundStream(spec.Streams[0])
		if err != nil {
			return nil, err
		}
	} else {
		r, err := f.setupRouter(spec)
		if err != nil {
			return nil, err
		}
		output = r
		f.startables = append(f.startables, r)
	}

	// No output router or channel is safe to push rows to, unless the row won't
	// be modified later by the thing that created it. No processor creates safe
	// rows, either. So, we always wrap our outputs in copyingRowReceivers. These
	// outputs aren't used at all if they are processors that get fused to their
	// upstreams, though, which means that copyingRowReceivers are only used on
	// non-fused processors like the output routers.

	output = &copyingRowReceiver{RowReceiver: output}

	outputs := []distsql.RowReceiver{output}
	proc, err := newProcessor(ctx, &f.FlowCtx, ps.ProcessorID, &ps.Core, &ps.Post, inputs, outputs, f.localProcessors)
	if err != nil {
		return nil, err
	}

	// Initialize any routers (the setupRouter case above) and outboxes.
	types := proc.OutputTypes()
	rowRecv := output.(*copyingRowReceiver).RowReceiver
	switch o := rowRecv.(type) {
	case router:
		o.init(ctx, &f.FlowCtx, types)
	case *outbox:
		o.init(types)
	}
	return proc, nil
}

// setupInputSyncs populates a slice of input syncs, one for each Processor in
// f.Spec, each containing one RowSource for each input to that Processor.
func (f *Flow) setupInputSyncs(ctx context.Context) ([][]distsql.RowSource, error) {
	inputSyncs := make([][]distsql.RowSource, len(f.spec.Processors))
	for pIdx, ps := range f.spec.Processors {
		for _, is := range ps.Input {
			if len(is.Streams) == 0 {
				return nil, errors.Errorf("input sync with no streams")
			}
			var sync distsql.RowSource
			switch is.Type {
			case distsqlpb.InputSyncSpec_UNORDERED:
				mrc := &distsql.RowChannel{}
				mrc.InitWithNumSenders(is.ColumnTypes, len(is.Streams))
				for _, s := range is.Streams {
					if err := f.setupInboundStream(ctx, s, mrc); err != nil {
						return nil, err
					}
				}
				sync = mrc
			case distsqlpb.InputSyncSpec_ORDERED:
				// Ordered synchronizer: create a RowChannel for each input.
				streams := make([]distsql.RowSource, len(is.Streams))
				for i, s := range is.Streams {
					rowChan := &distsql.RowChannel{}
					rowChan.InitWithNumSenders(is.ColumnTypes, 1 /* numSenders */)
					if err := f.setupInboundStream(ctx, s, rowChan); err != nil {
						return nil, err
					}
					streams[i] = rowChan
				}
				var err error
				sync, err = makeOrderedSync(distsqlpb.ConvertToColumnOrdering(is.Ordering), f.EvalCtx, streams)
				if err != nil {
					return nil, err
				}

			default:
				return nil, errors.Errorf("unsupported input sync type %s", is.Type)
			}
			inputSyncs[pIdx] = append(inputSyncs[pIdx], sync)
		}
	}
	return inputSyncs, nil
}

// setupProcessors creates processors for each spec in f.spec, fusing processors
// together when possible (when an upstream processor implements RowSource, only
// has one output, and that output is a simple PASS_THROUGH output), and
// populates f.processors with all created processors that weren't fused to and
// thus need their own goroutine.
func (f *Flow) setupProcessors(ctx context.Context, inputSyncs [][]distsql.RowSource) error {
	f.processors = make([]distsql.Processor, 0, len(f.spec.Processors))

	// Populate f.processors: see which processors need their own goroutine and
	// which are fused with their consumer.
	for i := range f.spec.Processors {
		pspec := &f.spec.Processors[i]
		p, err := f.makeProcessor(ctx, pspec, inputSyncs[i])
		if err != nil {
			return err
		}

		// fuse will return true if we managed to fuse p, false otherwise.
		fuse := func() bool {
			// If the processor implements RowSource try to hook it up directly to the
			// input of a later processor.
			source, ok := p.(distsql.RowSource)
			if !ok {
				return false
			}
			if len(pspec.Output) != 1 {
				// The processor has more than one output, use the normal routing
				// machinery.
				return false
			}
			ospec := &pspec.Output[0]
			if ospec.Type != distsqlpb.OutputRouterSpec_PASS_THROUGH {
				// The output is not pass-through and thus is being sent through a
				// router.
				return false
			}
			if len(ospec.Streams) != 1 {
				// The output contains more than one stream.
				return false
			}

			for pIdx, ps := range f.spec.Processors {
				if pIdx <= i {
					// Skip processors which have already been created.
					continue
				}
				for inIdx, in := range ps.Input {
					// Look for "simple" inputs: an unordered input (which, by definition,
					// doesn't require an ordered synchronizer), with a single input stream
					// (which doesn't require a multiplexed RowChannel).
					if in.Type != distsqlpb.InputSyncSpec_UNORDERED {
						continue
					}
					if len(in.Streams) != 1 {
						continue
					}
					if in.Streams[0].StreamID != ospec.Streams[0].StreamID {
						continue
					}
					// We found a consumer to fuse our proc to.
					inputSyncs[pIdx][inIdx] = source
					return true
				}
			}
			return false
		}
		if !fuse() {
			f.processors = append(f.processors, p)
		}
	}
	return nil
}

func (f *Flow) setup(ctx context.Context, spec *distsqlpb.FlowSpec) error {
	f.spec = spec
	if f.isVectorized {
		log.VEventf(ctx, 1, "setting up vectorize flow %s", f.ID.Short())
		acc := f.EvalCtx.Mon.MakeBoundAccount()
		f.VectorizedBoundAccount = &acc
		err := f.setupVectorizedFlow(ctx, f.VectorizedBoundAccount)
		if err == nil {
			log.VEventf(ctx, 1, "vectorized flow setup succeeded")
			return nil
		}
		log.VEventf(ctx, 1, "failed to vectorize: %s", err)
		return err
	}

	// First step: setup the input synchronizers for all processors.
	inputSyncs, err := f.setupInputSyncs(ctx)
	if err != nil {
		return err
	}

	// Then, populate f.processors.
	return f.setupProcessors(ctx, inputSyncs)
}

// startInternal starts the flow. All processors are started, each in their own
// goroutine. The caller must forward any returned error to syncFlowConsumer if
// set. A new context is derived and returned, and it must be used when this
// method returns so that all components running in their own goroutines could
// listen for a cancellation on the same context.
func (f *Flow) startInternal(ctx context.Context, doneFn func()) (context.Context, error) {
	f.doneFn = doneFn
	log.VEventf(
		ctx, 1, "starting (%d processors, %d startables)", len(f.processors), len(f.startables),
	)

	ctx, f.ctxCancel = contextutil.WithCancel(ctx)
	f.ctxDone = ctx.Done()

	// Only register the flow if there will be inbound stream connections that
	// need to look up this flow in the flow registry.
	if !f.isLocal() {
		// Once we call RegisterFlow, the inbound streams become accessible; we must
		// set up the WaitGroup counter before.
		// The counter will be further incremented below to account for the
		// processors.
		f.waitGroup.Add(len(f.inboundStreams))

		if err := f.flowRegistry.RegisterFlow(
			ctx, f.ID, f, f.inboundStreams, settingFlowStreamTimeout.Get(&f.FlowCtx.Cfg.Settings.SV),
		); err != nil {
			return ctx, err
		}
	}

	f.status = FlowRunning

	if log.V(1) {
		log.Infof(ctx, "registered flow %s", f.ID.Short())
	}
	for _, s := range f.startables {
		s.start(ctx, &f.waitGroup, f.ctxCancel)
	}
	for i := 0; i < len(f.processors); i++ {
		f.waitGroup.Add(1)
		go func(i int) {
			f.processors[i].Run(ctx)
			f.waitGroup.Done()
		}(i)
	}
	f.startedGoroutines = len(f.startables) > 0 || len(f.processors) > 0 || !f.isLocal()
	return ctx, nil
}

// isLocal returns whether this flow does not have any remote execution.
func (f *Flow) isLocal() bool {
	return len(f.inboundStreams) == 0
}

// Start starts the flow. Processors run asynchronously in their own goroutines.
// Wait() needs to be called to wait for the flow to finish.
// See Run() for a synchronous version.
//
// Generally if errors are encountered during the setup part, they're returned.
// But if the flow is a synchronous one, then no error is returned; instead the
// setup error is pushed to the syncFlowConsumer. In this case, a subsequent
// call to f.Wait() will not block.
func (f *Flow) Start(ctx context.Context, doneFn func()) error {
	if _, err := f.startInternal(ctx, doneFn); err != nil {
		// For sync flows, the error goes to the consumer.
		if f.syncFlowConsumer != nil {
			f.syncFlowConsumer.Push(nil /* row */, &distsqlpb.ProducerMetadata{Err: err})
			f.syncFlowConsumer.ProducerDone()
			return nil
		}
		return err
	}
	return nil
}

// Run runs the flow to completion. The last processor is run in the current
// goroutine; others may run in different goroutines depending on how the flow
// was configured.
// f.Wait() is called internally, so the call blocks until all the flow's
// goroutines are done.
// The caller needs to call f.Cleanup().
func (f *Flow) Run(ctx context.Context, doneFn func()) error {
	defer f.Wait()

	// We'll take care of the last processor in particular.
	var headProc distsql.Processor
	if len(f.processors) == 0 {
		return errors.AssertionFailedf("no processors in flow")
	}
	headProc = f.processors[len(f.processors)-1]
	f.processors = f.processors[:len(f.processors)-1]

	var err error
	if ctx, err = f.startInternal(ctx, doneFn); err != nil {
		// For sync flows, the error goes to the consumer.
		if f.syncFlowConsumer != nil {
			f.syncFlowConsumer.Push(nil /* row */, &distsqlpb.ProducerMetadata{Err: err})
			f.syncFlowConsumer.ProducerDone()
			return nil
		}
		return err
	}
	headProc.Run(ctx)
	return nil
}

// Wait waits for all the goroutines for this flow to exit. If the context gets
// canceled before all goroutines exit, it calls f.cancel().
func (f *Flow) Wait() {
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

// Releasable is an interface for objects than can be Released back into a
// memory pool when finished.
type Releasable interface {
	// Release allows this object to be returned to a memory pool. Objects must
	// not be used after Release is called.
	Release()
}

// Cleanup should be called when the flow completes (after all processors and
// mailboxes exited).
func (f *Flow) Cleanup(ctx context.Context) {
	if f.status == FlowFinished {
		panic("flow cleanup called twice")
	}

	if f.VectorizedBoundAccount != nil {
		f.VectorizedBoundAccount.Close(ctx)
	}

	// This closes the monitor opened in ServerImpl.setupFlow.
	f.EvalCtx.Stop(ctx)
	for _, p := range f.processors {
		if d, ok := p.(Releasable); ok {
			d.Release()
		}
	}
	if log.V(1) {
		log.Infof(ctx, "cleaning up")
	}
	sp := opentracing.SpanFromContext(ctx)
	// Local flows do not get registered.
	if !f.isLocal() && f.status != FlowNotStarted {
		f.flowRegistry.UnregisterFlow(f.ID)
	}
	f.status = FlowFinished
	f.ctxCancel()
	f.doneFn()
	f.doneFn = nil
	sp.Finish()
}

// cancel iterates through all unconnected streams of this flow and marks them canceled.
// This function is called in Wait() after the associated context has been canceled.
// In order to cancel a flow, call f.ctxCancel() instead of this function.
//
// For a detailed description of the distsql query cancellation mechanism,
// read docs/RFCS/query_cancellation.md.
func (f *Flow) cancel() {
	// If the flow is local, there are no inbound streams to cancel.
	if f.isLocal() {
		return
	}
	f.flowRegistry.Lock()
	timedOutReceivers := f.flowRegistry.cancelPendingStreamsLocked(f.ID)
	f.flowRegistry.Unlock()

	for _, receiver := range timedOutReceivers {
		go func(receiver inboundStreamHandler) {
			// Stream has yet to be started; send an error to its
			// receiver and prevent it from being connected.
			receiver.timeout(sqlbase.QueryCanceledError)
		}(receiver)
	}
}
