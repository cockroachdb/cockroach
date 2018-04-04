// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package distsqlrun

import (
	"context"
	"sync"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// StreamID identifies a stream; it may be local to a flow or it may cross
// machine boundaries. The identifier can only be used in the context of a
// specific flow.
type StreamID int

// FlowID identifies a flow. It is most importantly used when setting up streams
// between nodes.
type FlowID struct {
	uuid.UUID
}

// FlowCtx encompasses the contexts needed for various flow components.
type FlowCtx struct {
	log.AmbientContext

	// Context used for all execution within the flow.
	// Created in Start(), canceled in Cleanup().
	Ctx context.Context

	Settings *cluster.Settings

	stopper *stop.Stopper

	// id is a unique identifier for a flow.
	id FlowID
	// EvalCtx is used by all the processors in the flow to evaluate expressions.
	// Processors that intend to evaluate expressions with this EvalCtx should
	// get a copy with NewEvalCtx instead of storing a pointer to this one
	// directly.
	EvalCtx tree.EvalContext
	// rpcCtx is used by the Outboxes that may be present in the flow for
	// connecting to other nodes.
	rpcCtx *rpc.Context
	// The transaction in which kv operations performed by processors in the flow
	// must be performed. Processors in the Flow will use this txn concurrently.
	// This field is generally not nil, except for flows that don't run in a
	// higher-level txn (like backfills).
	txn *client.Txn
	// clientDB is a handle to the cluster. Used for performing requests outside
	// of the transaction in which the flow's query is running.
	clientDB *client.DB
	// executor provides access to the SQL executor.
	executor sqlutil.InternalExecutor

	// nodeID is the ID of the node on which the processors using this FlowCtx
	// run.
	nodeID       roachpb.NodeID
	testingKnobs TestingKnobs

	// TempStorage is used by some DistSQL processors to store Rows when the
	// working set is larger than can be stored in memory.
	// This is not supposed to be used as a general engine.Engine and thus
	// one should sparingly use the set of features offered by it.
	TempStorage engine.Engine
	// diskMonitor is used to monitor temporary storage disk usage.
	diskMonitor *mon.BytesMonitor

	// JobRegistry is used during backfill to load jobs which keep state.
	JobRegistry *jobs.Registry
}

// NewEvalCtx returns a modifiable copy of the FlowCtx's EvalContext.
// Processors should use this method any time they need to store a pointer to
// the EvalContext, since processors may mutate the EvalContext. Specifically,
// every processor that runs ProcOutputHelper.Init must pass in a modifiable
// EvalContext, since it stores that EvalContext in its exprHelpers and mutates
// them at runtime to ensure expressions are evaluated with the correct indexed
// var context.
func (ctx *FlowCtx) NewEvalCtx() *tree.EvalContext {
	evalCtx := ctx.EvalCtx
	return &evalCtx
}

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

// Flow represents a flow which consists of processors and streams.
type Flow struct {
	FlowCtx

	flowRegistry *flowRegistry
	// processors contains a subset of the processors in the flow - the ones that
	// run in their own goroutines. Some processors that implement RowSource are
	// scheduled to run in their consumer's goroutine; those are not present here.
	processors []Processor
	// startables are entities that must be started when the flow starts;
	// currently these are outboxes and routers.
	startables []startable
	// syncFlowConsumer is a special outbox which instead of sending rows to
	// another host, returns them directly (as a result to a SetupSyncFlow RPC,
	// or to the local host).
	syncFlowConsumer RowReceiver

	localStreams map[StreamID]RowReceiver

	// inboundStreams are streams that receive data from other hosts; this map
	// is to be passed to flowRegistry.RegisterFlow.
	inboundStreams map[StreamID]*inboundStreamInfo

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

	// spec is the request that produced this flow. Only used for debugging.
	spec *FlowSpec
}

func newFlow(flowCtx FlowCtx, flowReg *flowRegistry, syncFlowConsumer RowReceiver) *Flow {
	f := &Flow{
		FlowCtx:          flowCtx,
		flowRegistry:     flowReg,
		syncFlowConsumer: syncFlowConsumer,
	}
	f.status = FlowNotStarted
	return f
}

// setupInboundStream adds a stream to the stream map (inboundStreams or
// localStreams).
func (f *Flow) setupInboundStream(
	ctx context.Context, spec StreamEndpointSpec, receiver RowReceiver,
) error {
	if spec.TargetAddr != "" {
		return errors.Errorf("inbound stream has target address set: %s", spec.TargetAddr)
	}
	sid := spec.StreamID
	switch spec.Type {
	case StreamEndpointSpec_SYNC_RESPONSE:
		return errors.Errorf("inbound stream of type SYNC_RESPONSE")

	case StreamEndpointSpec_REMOTE:
		if _, found := f.inboundStreams[sid]; found {
			return errors.Errorf("inbound stream %d has multiple consumers", sid)
		}
		if f.inboundStreams == nil {
			f.inboundStreams = make(map[StreamID]*inboundStreamInfo)
		}
		if log.V(2) {
			log.Infof(ctx, "set up inbound stream %d", sid)
		}
		f.inboundStreams[sid] = &inboundStreamInfo{receiver: receiver, waitGroup: &f.waitGroup}

	case StreamEndpointSpec_LOCAL:
		if _, found := f.localStreams[sid]; found {
			return errors.Errorf("local stream %d has multiple consumers", sid)
		}
		if f.localStreams == nil {
			f.localStreams = make(map[StreamID]RowReceiver)
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
func (f *Flow) setupOutboundStream(spec StreamEndpointSpec) (RowReceiver, error) {
	sid := spec.StreamID
	switch spec.Type {
	case StreamEndpointSpec_SYNC_RESPONSE:
		return f.syncFlowConsumer, nil

	case StreamEndpointSpec_REMOTE:
		outbox := newOutbox(&f.FlowCtx, spec.TargetAddr, f.id, sid)
		f.startables = append(f.startables, outbox)
		return outbox, nil

	case StreamEndpointSpec_LOCAL:
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
func (f *Flow) setupRouter(spec *OutputRouterSpec) (router, error) {
	streams := make([]RowReceiver, len(spec.Streams))
	for i := range spec.Streams {
		var err error
		streams[i], err = f.setupOutboundStream(spec.Streams[i])
		if err != nil {
			return nil, err
		}
	}
	return makeRouter(spec, streams)
}

func checkNumInOut(inputs []RowSource, outputs []RowReceiver, numIn, numOut int) error {
	if len(inputs) != numIn {
		return errors.Errorf("expected %d input(s), got %d", numIn, len(inputs))
	}
	if len(outputs) != numOut {
		return errors.Errorf("expected %d output(s), got %d", numOut, len(outputs))
	}
	return nil
}

func (f *Flow) makeProcessor(ps *ProcessorSpec, inputs []RowSource) (Processor, error) {
	if len(ps.Output) != 1 {
		return nil, errors.Errorf("only single-output processors supported")
	}
	outputs := make([]RowReceiver, len(ps.Output))
	for i := range ps.Output {
		spec := &ps.Output[i]
		if spec.Type == OutputRouterSpec_PASS_THROUGH {
			// There is no entity that corresponds to a pass-through router - we just
			// use its output stream directly.
			if len(spec.Streams) != 1 {
				return nil, errors.Errorf("expected one stream for passthrough router")
			}
			var err error
			outputs[i], err = f.setupOutboundStream(spec.Streams[0])
			if err != nil {
				return nil, err
			}
			continue
		}

		r, err := f.setupRouter(spec)
		if err != nil {
			return nil, err
		}
		outputs[i] = r
		f.startables = append(f.startables, r)
	}
	proc, err := newProcessor(&f.FlowCtx, &ps.Core, &ps.Post, inputs, outputs)
	if err != nil {
		return nil, err
	}
	// Initialize any routers (the setupRouter case above) and outboxes.
	types := proc.OutputTypes()
	for _, o := range outputs {
		switch o := o.(type) {
		case router:
			o.init(&f.FlowCtx, types)
		case *outbox:
			o.init(types)
		}
	}
	return proc, nil
}

func (f *Flow) setup(ctx context.Context, spec *FlowSpec) error {
	f.spec = spec

	// First step: setup the input synchronizers for all processors.
	inputSyncs := make([][]RowSource, len(spec.Processors))
	for pIdx, ps := range spec.Processors {
		for _, is := range ps.Input {
			if len(is.Streams) == 0 {
				return errors.Errorf("input sync with no streams")
			}
			var sync RowSource
			switch is.Type {
			case InputSyncSpec_UNORDERED:
				if len(is.Streams) == 1 {
					rowChan := &RowChannel{}
					rowChan.Init(is.ColumnTypes)
					if err := f.setupInboundStream(ctx, is.Streams[0], rowChan); err != nil {
						return err
					}
					sync = rowChan
				} else {
					mrc := &MultiplexedRowChannel{}
					mrc.Init(len(is.Streams), is.ColumnTypes)
					for _, s := range is.Streams {
						if err := f.setupInboundStream(ctx, s, mrc); err != nil {
							return err
						}
					}
					sync = mrc
				}
			case InputSyncSpec_ORDERED:
				// Ordered synchronizer: create a RowChannel for each input.
				streams := make([]RowSource, len(is.Streams))
				for i, s := range is.Streams {
					rowChan := &RowChannel{}
					rowChan.Init(is.ColumnTypes)
					if err := f.setupInboundStream(ctx, s, rowChan); err != nil {
						return err
					}
					streams[i] = rowChan
				}
				var err error
				sync, err = makeOrderedSync(convertToColumnOrdering(is.Ordering), &f.EvalCtx, streams)
				if err != nil {
					return err
				}

			default:
				return errors.Errorf("unsupported input sync type %s", is.Type)
			}
			inputSyncs[pIdx] = append(inputSyncs[pIdx], sync)
		}
	}

	f.processors = make([]Processor, 0, len(spec.Processors))

	// Populate f.processors: see which processors need their own goroutine and
	// which are fused with their consumer.
	for i := range spec.Processors {
		pspec := &spec.Processors[i]
		p, err := f.makeProcessor(pspec, inputSyncs[i])
		if err != nil {
			return err
		}

		// fuse will return true if we managed to fuse p, false otherwise.
		fuse := func() bool {
			// If the processor implements RowSource try to hook it up directly to the
			// input of a later processor.
			source, ok := p.(RowSource)
			if !ok {
				return false
			}
			if len(pspec.Output) != 1 {
				// The processor has more than one output, use the normal routing
				// machinery.
				return false
			}
			ospec := &pspec.Output[0]
			if ospec.Type != OutputRouterSpec_PASS_THROUGH {
				// The output is not pass-through and thus is being sent through a
				// router.
				return false
			}
			if len(ospec.Streams) != 1 {
				// The output contains more than one stream.
				return false
			}

			for pIdx, ps := range spec.Processors {
				if pIdx <= i {
					// Skip processors which have already been created.
					continue
				}
				for inIdx, in := range ps.Input {
					// Look for "simple" inputs: an unordered input (which, by definition,
					// doesn't require an ordered synchronizer), with a single input stream
					// (which doesn't require a MultiplexedRowChannel).
					if in.Type != InputSyncSpec_UNORDERED {
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

// Start starts the flow (each processor runs in their own goroutine).
//
// Generally if errors are encountered during the setup part, they're returned.
// But if the flow is a synchronous one, then no error is returned; instead the
// setup error is pushed to the syncFlowConsumer. In this case, a subsequent
// call to f.Wait() will not block.
func (f *Flow) Start(ctx context.Context, doneFn func()) error {
	f.doneFn = doneFn
	log.VEventf(
		ctx, 1, "starting (%d processors, %d startables)", len(f.processors), len(f.startables),
	)

	f.Ctx, f.ctxCancel = contextutil.WithCancel(ctx)

	// Once we call RegisterFlow, the inbound streams become accessible; we must
	// set up the WaitGroup counter before.
	// The counter will be further incremented below to account for the
	// processors.
	f.waitGroup.Add(len(f.inboundStreams))

	if err := f.flowRegistry.RegisterFlow(
		f.Ctx, f.id, f, f.inboundStreams, flowStreamDefaultTimeout,
	); err != nil {
		if f.syncFlowConsumer != nil {
			// For sync flows, the error goes to the consumer.
			f.syncFlowConsumer.Push(nil /* row */, &ProducerMetadata{Err: err})
			f.syncFlowConsumer.ProducerDone()
			return nil
		}
		return err
	}

	f.status = FlowRunning

	if log.V(1) {
		log.Infof(f.Ctx, "registered flow %s", f.id.Short())
	}
	for _, s := range f.startables {
		s.start(f.Ctx, &f.waitGroup, f.ctxCancel)
	}
	for _, p := range f.processors {
		f.waitGroup.Add(1)
		go p.Run(&f.waitGroup)
	}
	return nil
}

// Wait waits for all the goroutines for this flow to exit. If the context gets
// canceled before all goroutines exit, it calls f.cancel().
func (f *Flow) Wait() {
	waitChan := make(chan struct{})

	go func() {
		f.waitGroup.Wait()
		close(waitChan)
	}()

	select {
	case <-f.Ctx.Done():
		f.cancel()
		<-waitChan
	case <-waitChan:
		// Exit normally
	}
}

// Cleanup should be called when the flow completes (after all processors and
// mailboxes exited).
func (f *Flow) Cleanup(ctx context.Context) {
	if f.status == FlowFinished {
		panic("flow cleanup called twice")
	}
	// This closes the account and monitor opened in ServerImpl.setupFlow.
	f.EvalCtx.ActiveMemAcc.Close(ctx)
	f.EvalCtx.Stop(ctx)
	if log.V(1) {
		log.Infof(ctx, "cleaning up")
	}
	sp := opentracing.SpanFromContext(ctx)
	sp.Finish()
	if f.status != FlowNotStarted {
		f.flowRegistry.UnregisterFlow(f.id)
	}
	f.status = FlowFinished
	f.ctxCancel()
	f.doneFn()
	f.doneFn = nil
}

// cancel iterates through all unconnected streams of this flow and marks them canceled.
// This function is called in Wait() after the associated context has been canceled.
// In order to cancel a flow, call f.ctxCancel() instead of this function.
//
// For a detailed description of the distsql query cancellation mechanism,
// read docs/RFCS/query_cancellation.md.
func (f *Flow) cancel() {
	f.flowRegistry.Lock()
	defer f.flowRegistry.Unlock()

	entry := f.flowRegistry.flows[f.id]
	for streamID, is := range entry.inboundStreams {
		// Connected, non-finished inbound streams will get an error
		// returned in ProcessInboundStream(). Non-connected streams
		// are handled below.
		if !is.connected && !is.finished {
			is.canceled = true
			// Stream has yet to be started; send an error to its
			// receiver and prevent it from being connected.
			is.receiver.Push(
				nil, /* row */
				&ProducerMetadata{Err: sqlbase.QueryCanceledError})
			is.receiver.ProducerDone()
			f.flowRegistry.finishInboundStreamLocked(f.id, streamID)
		}
	}
}
