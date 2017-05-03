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
//
// Author: Radu Berinde (radu@cockroachlabs.com)
// Author: Irfan Sharif (irfansharif@cockroachlabs.com)

package distsqlrun

import (
	"sync"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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

	// id is a unique identifier for a flow.
	id FlowID
	// evalCtx is used by all the processors in the flow to evaluate expressions.
	evalCtx parser.EvalContext
	// rpcCtx is used by the Outboxes that may be present in the flow for
	// connecting to other nodes.
	rpcCtx *rpc.Context
	// txnProto is the transaction in which kv operations performed by processors
	// in the flow must be performed.
	txnProto *roachpb.Transaction
	// clientDB is a handle to the cluster. Used for performing requests outside
	// of the transaction in which the flow's query is running.
	clientDB *client.DB
	// remoteTxnDB is a handle to the cluster that bypasses the local
	// TxnCoordSender. Used via setupTxn() for running requests on behalf of the
	// query's transaction.
	remoteTxnDB *client.DB
	// nodeID is the ID of the node on which the processors using this FlowCtx
	// run.
	nodeID       roachpb.NodeID
	testingKnobs TestingKnobs
}

func (flowCtx *FlowCtx) setupTxn() *client.Txn {
	txn := client.NewTxnWithProto(flowCtx.remoteTxnDB, *flowCtx.txnProto)
	// DistSQL transactions get retryable errors that would otherwise be handled
	// by the TxnCoordSender.
	txn.AcceptUnhandledRetryableErrors()
	return txn
}

type flowStatus int

// Flow status indicators.
const (
	FlowNotStarted flowStatus = iota
	FlowRunning
	FlowFinished
)

// Flow represents a flow which consists of processors and streams.
type Flow struct {
	FlowCtx

	flowRegistry *flowRegistry
	processors   []processor
	outboxes     []*outbox
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
		f.outboxes = append(f.outboxes, outbox)
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

func (f *Flow) setupRouter(spec *OutputRouterSpec) (RowReceiver, error) {
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

func (f *Flow) makeProcessor(ps *ProcessorSpec, inputs []RowSource) (processor, error) {
	if len(ps.Output) != 1 {
		return nil, errors.Errorf("only single-output processors supported")
	}
	outputs := make([]RowReceiver, len(ps.Output))
	for i := range ps.Output {
		var err error
		outputs[i], err = f.setupRouter(&ps.Output[i])
		if err != nil {
			return nil, err
		}
	}
	return newProcessor(&f.FlowCtx, &ps.Core, &ps.Post, inputs, outputs)
}

func (f *Flow) setupFlow(ctx context.Context, spec *FlowSpec) error {
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
				sync, err = makeOrderedSync(convertToColumnOrdering(is.Ordering), &f.evalCtx, streams)
				if err != nil {
					return err
				}

			default:
				return errors.Errorf("unsupported input sync type %s", is.Type)
			}
			inputSyncs[pIdx] = append(inputSyncs[pIdx], sync)
		}
	}

	f.processors = make([]processor, len(spec.Processors))

	for i := range spec.Processors {
		var err error
		f.processors[i], err = f.makeProcessor(&spec.Processors[i], inputSyncs[i])
		if err != nil {
			return err
		}
	}
	return nil
}

// Start starts the flow (each processor runs in their own goroutine).
func (f *Flow) Start(ctx context.Context, doneFn func()) {
	f.doneFn = doneFn
	log.VEventf(
		ctx, 1, "starting (%d processors, %d outboxes)", len(f.outboxes), len(f.processors),
	)
	f.status = FlowRunning

	// Once we call RegisterFlow, the inbound streams become accessible; we must
	// set up the WaitGroup counter before.
	f.waitGroup.Add(len(f.inboundStreams) + len(f.outboxes) + len(f.processors))

	f.flowRegistry.RegisterFlow(ctx, f.id, f, f.inboundStreams, flowStreamDefaultTimeout)
	if log.V(1) {
		log.Infof(ctx, "registered flow %s", f.id.Short())
	}
	for _, o := range f.outboxes {
		o.start(ctx, &f.waitGroup)
	}
	for _, p := range f.processors {
		go p.Run(ctx, &f.waitGroup)
	}
}

// Wait waits for all the goroutines for this flow to exit.
func (f *Flow) Wait() {
	f.waitGroup.Wait()
}

// Cleanup should be called when the flow completes (after all processors and
// mailboxes exited).
func (f *Flow) Cleanup(ctx context.Context) {
	if f.status == FlowFinished {
		panic("flow cleanup called twice")
	}
	// This closes the account and monitor opened in ServerImpl.setupFlow
	f.evalCtx.Mon.Stop(ctx)
	if log.V(1) {
		log.Infof(ctx, "cleaning up")
	}
	sp := opentracing.SpanFromContext(ctx)
	sp.Finish()
	if f.status != FlowNotStarted {
		f.flowRegistry.UnregisterFlow(f.id)
	}
	f.status = FlowFinished
	f.doneFn()
	f.doneFn = nil
}

// RunSync runs the processors in the flow in order (serially), in the same
// context (no goroutines are spawned).
func (f *Flow) RunSync(ctx context.Context) {
	for _, p := range f.processors {
		p.Run(ctx, nil)
	}
	f.Cleanup(ctx)
}

var _ = (*Flow).RunSync
