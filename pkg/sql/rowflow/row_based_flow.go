// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowflow

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

type rowBasedFlow struct {
	*flowinfra.FlowBase

	localStreams map[execinfrapb.StreamID]execinfra.RowReceiver
}

var _ flowinfra.Flow = &rowBasedFlow{}

// NewRowBasedFlow returns a row based flow using base as its FlowBase.
func NewRowBasedFlow(base *flowinfra.FlowBase) flowinfra.Flow {
	return &rowBasedFlow{FlowBase: base}
}

// Setup if part of the flowinfra.Flow interface.
func (f *rowBasedFlow) Setup(
	ctx context.Context, spec *execinfrapb.FlowSpec, opt flowinfra.FuseOpt,
) error {
	f.SetSpec(spec)
	// First step: setup the input synchronizers for all processors.
	inputSyncs, err := f.setupInputSyncs(ctx, spec, opt)
	if err != nil {
		return err
	}

	// Then, populate processors.
	return f.setupProcessors(ctx, spec, inputSyncs)
}

// setupProcessors creates processors for each spec in f.spec, fusing processors
// together when possible (when an upstream processor implements RowSource, only
// has one output, and that output is a simple PASS_THROUGH output), and
// populates f.processors with all created processors that weren't fused to and
// thus need their own goroutine.
func (f *rowBasedFlow) setupProcessors(
	ctx context.Context, spec *execinfrapb.FlowSpec, inputSyncs [][]execinfra.RowSource,
) error {
	processors := make([]execinfra.Processor, 0, len(spec.Processors))

	// Populate processors: see which processors need their own goroutine and
	// which are fused with their consumer.
	for i := range spec.Processors {
		pspec := &spec.Processors[i]
		p, err := f.makeProcessor(ctx, pspec, inputSyncs[i])
		if err != nil {
			return err
		}

		// fuse will return true if we managed to fuse p with its consumer.
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
					if len(in.Streams) == 1 {
						if in.Streams[0].StreamID != ospec.Streams[0].StreamID {
							continue
						}
						// We found a consumer to fuse our proc to.
						inputSyncs[pIdx][inIdx] = source
						return true
					}
					// ps has an input with multiple streams. This can be either a
					// multiplexed RowChannel (in case of some unordered synchronizers)
					// or an orderedSynchronizer (for other unordered synchronizers or
					// ordered synchronizers). If it's a multiplexed RowChannel,
					// then its inputs run in parallel, so there's no fusing with them.
					// If it's an orderedSynchronizer, then we look inside it to see if
					// the processor we're trying to fuse feeds into it.
					orderedSync, ok := inputSyncs[pIdx][inIdx].(*orderedSynchronizer)
					if !ok {
						continue
					}
					// See if we can find a stream attached to the processor we're
					// trying to fuse.
					for sIdx, sspec := range in.Streams {
						input := findProcByOutputStreamID(spec, sspec.StreamID)
						if input == nil {
							continue
						}
						if input.ProcessorID != pspec.ProcessorID {
							continue
						}
						// Fuse the processor with this orderedSynchronizer.
						orderedSync.sources[sIdx].src = source
						return true
					}
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

// findProcByOutputStreamID looks in spec for a processor that has a
// pass-through output router connected to the specified stream. Returns nil if
// such a processor is not found.
func findProcByOutputStreamID(
	spec *execinfrapb.FlowSpec, streamID execinfrapb.StreamID,
) *execinfrapb.ProcessorSpec {
	for i := range spec.Processors {
		pspec := &spec.Processors[i]
		if len(pspec.Output) > 1 {
			// We don't have any processors with more than one output. But if we
			// didn't, we couldn't fuse them, so ignore.
			continue
		}
		ospec := &pspec.Output[0]
		if ospec.Type != execinfrapb.OutputRouterSpec_PASS_THROUGH {
			// The output is not pass-through and thus is being sent through a
			// router.
			continue
		}
		if len(ospec.Streams) != 1 {
			panic(fmt.Sprintf("pass-through router with %d streams", len(ospec.Streams)))
		}
		if ospec.Streams[0].StreamID == streamID {
			return pspec
		}
	}
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
	proc, err := rowexec.NewProcessor(
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
	case *flowinfra.Outbox:
		o.Init(types)
	}
	return proc, nil
}

// setupInputSyncs populates a slice of input syncs, one for each Processor in
// f.Spec, each containing one RowSource for each input to that Processor.
func (f *rowBasedFlow) setupInputSyncs(
	ctx context.Context, spec *execinfrapb.FlowSpec, opt flowinfra.FuseOpt,
) ([][]execinfra.RowSource, error) {
	inputSyncs := make([][]execinfra.RowSource, len(spec.Processors))
	for pIdx, ps := range spec.Processors {
		for _, is := range ps.Input {
			if len(is.Streams) == 0 {
				return nil, errors.Errorf("input sync with no streams")
			}
			var sync execinfra.RowSource
			if is.Type != execinfrapb.InputSyncSpec_UNORDERED &&
				is.Type != execinfrapb.InputSyncSpec_ORDERED {
				return nil, errors.Errorf("unsupported input sync type %s", is.Type)
			}

			if is.Type == execinfrapb.InputSyncSpec_UNORDERED {
				if opt == flowinfra.FuseNormally || len(is.Streams) == 1 {
					// Unordered synchronizer: create a RowChannel for each input.

					mrc := &execinfra.RowChannel{}
					mrc.InitWithNumSenders(is.ColumnTypes, len(is.Streams))
					for _, s := range is.Streams {
						if err := f.setupInboundStream(ctx, s, mrc); err != nil {
							return nil, err
						}
					}
					sync = mrc
				}
			}
			if sync == nil {
				// We have an ordered synchronizer or an unordered one that we really
				// want to fuse because of the FuseAggressively option. We'll create a
				// RowChannel for each input for now, but the inputs might be fused with
				// the orderedSynchronizer later (in which case the RowChannels will be
				// dropped).
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
				ordering := sqlbase.NoOrdering
				if is.Type == execinfrapb.InputSyncSpec_ORDERED {
					ordering = execinfrapb.ConvertToColumnOrdering(is.Ordering)
				}
				sync, err = makeOrderedSync(ordering, f.EvalCtx, streams)
				if err != nil {
					return nil, err
				}
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
		f.AddRemoteStream(sid, flowinfra.NewInboundStreamInfo(
			flowinfra.RowInboundStreamHandler{RowReceiver: receiver},
			f.GetWaitGroup(),
		))

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
		outbox := flowinfra.NewOutbox(&f.FlowCtx, spec.TargetNodeID, f.ID, sid)
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
