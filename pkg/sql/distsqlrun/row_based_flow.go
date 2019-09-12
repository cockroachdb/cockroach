// Copyright 2019 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

type rowBasedFlow struct {
	execinfra.FlowBase

	localStreams map[execinfrapb.StreamID]execinfra.RowReceiver
}

var _ execinfra.Flow = &rowBasedFlow{}

func (f *rowBasedFlow) Setup(ctx context.Context, spec *execinfrapb.FlowSpec) error {
	f.SetSpec(spec)
	// First step: setup the input synchronizers for all processors.
	inputSyncs, err := f.setupInputSyncs(ctx)
	if err != nil {
		return err
	}

	// Then, populate processors.
	return f.setupProcessors(ctx, inputSyncs)
}

// setupProcessors creates processors for each spec in f.spec, fusing processors
// together when possible (when an upstream processor implements RowSource, only
// has one output, and that output is a simple PASS_THROUGH output), and
// populates f.processors with all created processors that weren't fused to and
// thus need their own goroutine.
func (f *rowBasedFlow) setupProcessors(
	ctx context.Context, inputSyncs [][]execinfra.RowSource,
) error {
	spec := f.GetFlowSpec()
	processors := make([]execinfra.Processor, 0, len(spec.Processors))

	// Populate processors: see which processors need their own goroutine and
	// which are fused with their consumer.
	for i := range spec.Processors {
		pspec := &spec.Processors[i]
		p, err := f.makeProcessor(ctx, pspec, inputSyncs[i])
		if err != nil {
			return err
		}

		// fuse will return true if we managed to fuse p, false otherwise.
		fuse := func() bool {
			// If the processor implements RowSource try to hook it up directly to the
			// input of a later processor.
			source, ok := p.(execinfra.RowSource)
			if !ok {
				return false
			}
			if len(pspec.Output) != 1 {
				// The processor has more than one output, use the normal routing
				// machinery.
				return false
			}
			ospec := &pspec.Output[0]
			if ospec.Type != execinfrapb.OutputRouterSpec_PASS_THROUGH {
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
					// (which doesn't require a multiplexed RowChannel).
					if in.Type != execinfrapb.InputSyncSpec_UNORDERED {
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
			processors = append(processors, p)
		}
	}
	f.SetProcessors(processors)
	return nil
}

func (f *rowBasedFlow) makeProcessor(
	ctx context.Context, ps *execinfrapb.ProcessorSpec, inputs []execinfra.RowSource,
) (execinfra.Processor, error) {
	if len(ps.Output) != 1 {
		return nil, errors.Errorf("only single-output processors supported")
	}
	var output execinfra.RowReceiver
	spec := &ps.Output[0]
	if spec.Type == execinfrapb.OutputRouterSpec_PASS_THROUGH {
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
		f.AddStartable(r)
	}

	// No output router or channel is safe to push rows to, unless the row won't
	// be modified later by the thing that created it. No processor creates safe
	// rows, either. So, we always wrap our outputs in copyingRowReceivers. These
	// outputs aren't used at all if they are processors that get fused to their
	// upstreams, though, which means that copyingRowReceivers are only used on
	// non-fused processors like the output routers.

	output = &copyingRowReceiver{RowReceiver: output}

	outputs := []execinfra.RowReceiver{output}
	proc, err := newProcessor(
		ctx,
		&f.FlowCtx,
		ps.ProcessorID,
		&ps.Core,
		&ps.Post,
		inputs,
		outputs,
		f.GetLocalProcessors(),
	)
	if err != nil {
		return nil, err
	}

	// Initialize any routers (the setupRouter case above) and outboxes.
	types := proc.OutputTypes()
	rowRecv := output.(*copyingRowReceiver).RowReceiver
	switch o := rowRecv.(type) {
	case router:
		o.init(ctx, &f.FlowCtx, types)
	case *execinfra.Outbox:
		o.Init(types)
	}
	return proc, nil
}

// setupInputSyncs populates a slice of input syncs, one for each Processor in
// f.Spec, each containing one RowSource for each input to that Processor.
func (f *rowBasedFlow) setupInputSyncs(ctx context.Context) ([][]execinfra.RowSource, error) {
	spec := f.GetFlowSpec()
	inputSyncs := make([][]execinfra.RowSource, len(spec.Processors))
	for pIdx, ps := range spec.Processors {
		for _, is := range ps.Input {
			if len(is.Streams) == 0 {
				return nil, errors.Errorf("input sync with no streams")
			}
			var sync execinfra.RowSource
			switch is.Type {
			case execinfrapb.InputSyncSpec_UNORDERED:
				mrc := &execinfra.RowChannel{}
				mrc.InitWithNumSenders(is.ColumnTypes, len(is.Streams))
				for _, s := range is.Streams {
					if err := f.setupInboundStream(ctx, s, mrc); err != nil {
						return nil, err
					}
				}
				sync = mrc
			case execinfrapb.InputSyncSpec_ORDERED:
				// Ordered synchronizer: create a RowChannel for each input.
				streams := make([]execinfra.RowSource, len(is.Streams))
				for i, s := range is.Streams {
					rowChan := &execinfra.RowChannel{}
					rowChan.InitWithNumSenders(is.ColumnTypes, 1 /* numSenders */)
					if err := f.setupInboundStream(ctx, s, rowChan); err != nil {
						return nil, err
					}
					streams[i] = rowChan
				}
				var err error
				sync, err = makeOrderedSync(execinfrapb.ConvertToColumnOrdering(is.Ordering), f.EvalCtx, streams)
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

// setupInboundStream adds a stream to the stream map (inboundStreams or
// localStreams).
func (f *rowBasedFlow) setupInboundStream(
	ctx context.Context, spec execinfrapb.StreamEndpointSpec, receiver execinfra.RowReceiver,
) error {
	sid := spec.StreamID
	switch spec.Type {
	case execinfrapb.StreamEndpointSpec_SYNC_RESPONSE:
		return errors.Errorf("inbound stream of type SYNC_RESPONSE")

	case execinfrapb.StreamEndpointSpec_REMOTE:
		if err := f.CheckInboundStreamID(sid); err != nil {
			return err
		}
		if log.V(2) {
			log.Infof(ctx, "set up inbound stream %d", sid)
		}
		f.AddRemoteRowBasedStream(sid, receiver)

	case execinfrapb.StreamEndpointSpec_LOCAL:
		if _, found := f.localStreams[sid]; found {
			return errors.Errorf("local stream %d has multiple consumers", sid)
		}
		if f.localStreams == nil {
			f.localStreams = make(map[execinfrapb.StreamID]execinfra.RowReceiver)
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
func (f *rowBasedFlow) setupOutboundStream(
	spec execinfrapb.StreamEndpointSpec,
) (execinfra.RowReceiver, error) {
	sid := spec.StreamID
	switch spec.Type {
	case execinfrapb.StreamEndpointSpec_SYNC_RESPONSE:
		return f.GetSyncFlowConsumer(), nil

	case execinfrapb.StreamEndpointSpec_REMOTE:
		outbox := execinfra.NewOutbox(&f.FlowCtx, spec.TargetNodeID, f.ID, sid)
		f.AddStartable(outbox)
		return outbox, nil

	case execinfrapb.StreamEndpointSpec_LOCAL:
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
func (f *rowBasedFlow) setupRouter(spec *execinfrapb.OutputRouterSpec) (router, error) {
	streams := make([]execinfra.RowReceiver, len(spec.Streams))
	for i := range spec.Streams {
		var err error
		streams[i], err = f.setupOutboundStream(spec.Streams[i])
		if err != nil {
			return nil, err
		}
	}
	return makeRouter(spec, streams)
}

type copyingRowReceiver struct {
	execinfra.RowReceiver
	alloc sqlbase.EncDatumRowAlloc
}

func (r *copyingRowReceiver) Push(
	row sqlbase.EncDatumRow, meta *execinfrapb.ProducerMetadata,
) execinfra.ConsumerStatus {
	if row != nil {
		row = r.alloc.CopyRow(row)
	}
	return r.RowReceiver.Push(row, meta)
}
