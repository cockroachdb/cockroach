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
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colflow/colrpc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/opentracing/opentracing-go"
)

type vectorizedFlow struct {
	*flowinfra.FlowBase
	// operatorConcurrency is set if any operators are executed in parallel.
	operatorConcurrency bool
}

var _ flowinfra.Flow = &vectorizedFlow{}

// NewVectorizedFlow creates a new vectorized flow given the flow base.
func NewVectorizedFlow(base *flowinfra.FlowBase) flowinfra.Flow {
	return &vectorizedFlow{FlowBase: base}
}

// Setup is part of the flowinfra.Flow interface.
func (f *vectorizedFlow) Setup(
	ctx context.Context, spec *execinfrapb.FlowSpec, opt flowinfra.FuseOpt,
) error {
	f.SetSpec(spec)
	log.VEventf(ctx, 1, "setting up vectorize flow %s", f.ID.Short())
	acc := f.EvalCtx.Mon.MakeBoundAccount()
	f.VectorizedBoundAccount = &acc
	recordingStats := false
	if sp := opentracing.SpanFromContext(ctx); sp != nil && tracing.IsRecording(sp) {
		recordingStats = true
	}
	helper := &vectorizedFlowCreatorHelper{f: f.FlowBase}
	creator := newVectorizedFlowCreator(
		helper,
		vectorizedRemoteComponentCreator{},
		recordingStats,
		f.GetWaitGroup(),
		f.GetSyncFlowConsumer(),
		f.GetFlowCtx().Cfg.NodeDialer,
		f.GetID(),
	)
	_, err := creator.setupFlow(ctx, f.GetFlowCtx(), spec.Processors, &acc, opt)
	if err == nil {
		log.VEventf(ctx, 1, "vectorized flow setup succeeded")
		return nil
	}
	f.operatorConcurrency = creator.operatorConcurrency
	log.VEventf(ctx, 1, "failed to vectorize: %s", err)
	return err
}

// ConcurrentExecution is part of the Flow interface.
func (f *vectorizedFlow) ConcurrentExecution() bool {
	return f.operatorConcurrency || f.FlowBase.ConcurrentExecution()
}

// wrapWithVectorizedStatsCollector creates a new exec.VectorizedStatsCollector
// that wraps op and connects the newly created wrapper with those
// corresponding to operators in inputs (the latter must have already been
// wrapped).
func wrapWithVectorizedStatsCollector(
	op colexec.Operator, inputs []colexec.Operator, pspec *execinfrapb.ProcessorSpec,
) (*colexec.VectorizedStatsCollector, error) {
	inputWatch := timeutil.NewStopWatch()
	vsc := colexec.NewVectorizedStatsCollector(op, pspec.ProcessorID, len(inputs) == 0, inputWatch)
	for _, input := range inputs {
		sc, ok := input.(*colexec.VectorizedStatsCollector)
		if !ok {
			return nil, errors.New("unexpectedly an input is not collecting stats")
		}
		sc.SetOutputWatch(inputWatch)
	}
	return vsc, nil
}

// finishVectorizedStatsCollectors finishes the given stats collectors and
// outputs their stats to the trace contained in the ctx's span.
func finishVectorizedStatsCollectors(
	ctx context.Context,
	deterministicStats bool,
	vectorizedStatsCollectors []*colexec.VectorizedStatsCollector,
	procIDs []int32,
) {
	spansByProcID := make(map[int32]opentracing.Span)
	for _, pid := range procIDs {
		// We're creating a new span for every processor setting the
		// appropriate tag so that it is displayed correctly on the flow
		// diagram.
		// TODO(yuzefovich): these spans are created and finished right
		// away which is not the way they are supposed to be used, so this
		// should be fixed.
		_, spansByProcID[pid] = tracing.ChildSpan(ctx, fmt.Sprintf("operator for processor %d", pid))
		spansByProcID[pid].SetTag(execinfrapb.ProcessorIDTagKey, pid)
	}
	for _, vsc := range vectorizedStatsCollectors {
		// TODO(yuzefovich): I'm not sure whether there are cases when
		// multiple operators correspond to a single processor. We might
		// need to do some aggregation here in that case.
		vsc.FinalizeStats()
		if deterministicStats {
			vsc.VectorizedStats.Time = 0
		}
		if vsc.ID < 0 {
			// Ignore stats collectors not associated with a processor.
			continue
		}
		tracing.SetSpanStats(spansByProcID[vsc.ID], &vsc.VectorizedStats)
	}
	for _, sp := range spansByProcID {
		sp.Finish()
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
// as the metadataSources in this DAG that need to be drained.
type opDAGWithMetaSources struct {
	rootOperator    colexec.Operator
	metadataSources []execinfrapb.MetadataSource
}

// remoteComponentCreator is an interface that abstracts the constructors for
// several components in a remote flow. Mostly for testing purposes.
type remoteComponentCreator interface {
	newOutbox(input colexec.Operator, typs []coltypes.T, metadataSources []execinfrapb.MetadataSource) (*colrpc.Outbox, error)
	newInbox(typs []coltypes.T, streamID execinfrapb.StreamID) (*colrpc.Inbox, error)
}

type vectorizedRemoteComponentCreator struct{}

func (vectorizedRemoteComponentCreator) newOutbox(
	input colexec.Operator, typs []coltypes.T, metadataSources []execinfrapb.MetadataSource,
) (*colrpc.Outbox, error) {
	return colrpc.NewOutbox(input, typs, metadataSources)
}

func (vectorizedRemoteComponentCreator) newInbox(
	typs []coltypes.T, streamID execinfrapb.StreamID,
) (*colrpc.Inbox, error) {
	return colrpc.NewInbox(typs, streamID)
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
	procIDs                        []int32
	waitGroup                      *sync.WaitGroup
	syncFlowConsumer               execinfra.RowReceiver
	nodeDialer                     *nodedialer.Dialer
	flowID                         execinfrapb.FlowID

	// numOutboxes counts how many exec.Outboxes have been set up on this node.
	// It must be accessed atomically.
	numOutboxes       int32
	materializerAdded bool

	// leaves accumulates all operators that have no further outputs on the
	// current node, for the purposes of EXPLAIN output.
	leaves []execinfra.OpNode
	// operatorConcurrency is set if any operators are executed in parallel.
	operatorConcurrency bool
}

func newVectorizedFlowCreator(
	helper flowCreatorHelper,
	componentCreator remoteComponentCreator,
	recordingStats bool,
	waitGroup *sync.WaitGroup,
	syncFlowConsumer execinfra.RowReceiver,
	nodeDialer *nodedialer.Dialer,
	flowID execinfrapb.FlowID,
) *vectorizedFlowCreator {
	return &vectorizedFlowCreator{
		flowCreatorHelper:              helper,
		remoteComponentCreator:         componentCreator,
		streamIDToInputOp:              make(map[execinfrapb.StreamID]opDAGWithMetaSources),
		recordingStats:                 recordingStats,
		vectorizedStatsCollectorsQueue: make([]*colexec.VectorizedStatsCollector, 0, 2),
		procIDs:                        make([]int32, 0, 2),
		waitGroup:                      waitGroup,
		syncFlowConsumer:               syncFlowConsumer,
		nodeDialer:                     nodeDialer,
		flowID:                         flowID,
	}
}

// setupRemoteOutputStream sets up an Outbox that will operate according to
// the given StreamEndpointSpec. It will also drain all MetadataSources in the
// metadataSourcesQueue.
func (s *vectorizedFlowCreator) setupRemoteOutputStream(
	op colexec.Operator,
	outputTyps []coltypes.T,
	stream *execinfrapb.StreamEndpointSpec,
	metadataSourcesQueue []execinfrapb.MetadataSource,
) (execinfra.OpNode, error) {
	outbox, err := s.remoteComponentCreator.newOutbox(op, outputTyps, metadataSourcesQueue)
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
	input colexec.Operator,
	outputTyps []coltypes.T,
	output *execinfrapb.OutputRouterSpec,
	metadataSourcesQueue []execinfrapb.MetadataSource,
) error {
	if output.Type != execinfrapb.OutputRouterSpec_BY_HASH {
		return errors.Errorf("vectorized output router type %s unsupported", output.Type)
	}

	// TODO(asubiotto): Change hashRouter's hashCols to be uint32s.
	hashCols := make([]int, len(output.HashColumns))
	for i := range hashCols {
		hashCols[i] = int(output.HashColumns[i])
	}
	router, outputs := colexec.NewHashRouter(input, outputTyps, hashCols, len(output.Streams))
	runRouter := func(ctx context.Context, _ context.CancelFunc) {
		router.Run(ctx)
	}
	s.accumulateAsyncComponent(runRouter)

	// Append the router to the metadata sources.
	metadataSourcesQueue = append(metadataSourcesQueue, router)

	foundLocalOutput := false
	for i, op := range outputs {
		stream := &output.Streams[i]
		switch stream.Type {
		case execinfrapb.StreamEndpointSpec_SYNC_RESPONSE:
			return errors.Errorf("unexpected sync response output when setting up router")
		case execinfrapb.StreamEndpointSpec_REMOTE:
			if _, err := s.setupRemoteOutputStream(op, outputTyps, stream, metadataSourcesQueue); err != nil {
				return err
			}
		case execinfrapb.StreamEndpointSpec_LOCAL:
			foundLocalOutput = true
			if s.recordingStats {
				// Wrap local outputs with vectorized stats collectors when recording
				// stats. This is mostly for compatibility but will provide some useful
				// information (e.g. output stall time).
				var err error
				op, err = wrapWithVectorizedStatsCollector(
					op, nil /* inputs */, &execinfrapb.ProcessorSpec{ProcessorID: -1},
				)
				if err != nil {
					return err
				}
			}
			s.streamIDToInputOp[stream.StreamID] = opDAGWithMetaSources{rootOperator: op, metadataSources: metadataSourcesQueue}
		}
		// Either the metadataSourcesQueue will be drained by an outbox or we
		// created an opDAGWithMetaSources to pass along these metadataSources. We don't need to
		// worry about metadata sources for following iterations of the loop.
		metadataSourcesQueue = nil
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
	input execinfrapb.InputSyncSpec, opt flowinfra.FuseOpt,
) (op colexec.Operator, _ []execinfrapb.MetadataSource, memUsed int, _ error) {
	inputStreamOps := make([]colexec.Operator, 0, len(input.Streams))
	metaSources := make([]execinfrapb.MetadataSource, 0, len(input.Streams))
	for _, inputStream := range input.Streams {
		switch inputStream.Type {
		case execinfrapb.StreamEndpointSpec_LOCAL:
			in := s.streamIDToInputOp[inputStream.StreamID]
			inputStreamOps = append(inputStreamOps, in.rootOperator)
			metaSources = append(metaSources, in.metadataSources...)
		case execinfrapb.StreamEndpointSpec_REMOTE:
			// If the input is remote, the input operator does not exist in
			// streamIDToInputOp. Create an inbox.
			if err := s.checkInboundStreamID(inputStream.StreamID); err != nil {
				return nil, nil, memUsed, err
			}
			typs, err := typeconv.FromColumnTypes(input.ColumnTypes)
			if err != nil {
				return nil, nil, memUsed, err
			}
			inbox, err := s.remoteComponentCreator.newInbox(typs, inputStream.StreamID)
			if err != nil {
				return nil, nil, memUsed, err
			}
			s.addStreamEndpoint(inputStream.StreamID, inbox, s.waitGroup)
			metaSources = append(metaSources, inbox)
			op = inbox
			memUsed += op.(colexec.StaticMemoryOperator).EstimateStaticMemoryUsage()
			if s.recordingStats {
				op, err = wrapWithVectorizedStatsCollector(
					inbox,
					nil, /* inputs */
					// TODO(asubiotto): Vectorized stats collectors currently expect a
					// processor ID. These stats will not be shown until we extend stats
					// collectors to take in a stream ID.
					&execinfrapb.ProcessorSpec{
						ProcessorID: -1,
					},
				)
				if err != nil {
					return nil, nil, memUsed, err
				}
			}
			inputStreamOps = append(inputStreamOps, op)
		default:
			return nil, nil, memUsed, errors.Errorf("unsupported input stream type %s", inputStream.Type)
		}
	}
	op = inputStreamOps[0]
	if len(inputStreamOps) > 1 {
		statsInputs := inputStreamOps
		typs, err := typeconv.FromColumnTypes(input.ColumnTypes)
		if err != nil {
			return nil, nil, memUsed, err
		}
		if input.Type == execinfrapb.InputSyncSpec_ORDERED {
			op = colexec.NewOrderedSynchronizer(
				inputStreamOps, typs, execinfrapb.ConvertToColumnOrdering(input.Ordering),
			)
			memUsed += op.(colexec.StaticMemoryOperator).EstimateStaticMemoryUsage()
		} else {
			if opt == flowinfra.FuseAggressively {
				op = colexec.NewSerialUnorderedSynchronizer(inputStreamOps, typs)
			} else {
				op = colexec.NewParallelUnorderedSynchronizer(inputStreamOps, typs, s.waitGroup)
				s.operatorConcurrency = true
			}
			// Don't use the unordered synchronizer's inputs for stats collection
			// given that they run concurrently. The stall time will be collected
			// instead.
			statsInputs = nil
		}
		if s.recordingStats {
			// TODO(asubiotto): Once we have IDs for synchronizers, plumb them into
			// this stats collector to display stats.
			var err error
			op, err = wrapWithVectorizedStatsCollector(op, statsInputs, &execinfrapb.ProcessorSpec{ProcessorID: -1})
			if err != nil {
				return nil, nil, memUsed, err
			}
		}
	}
	return op, metaSources, memUsed, nil
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
	op colexec.Operator,
	opOutputTypes []coltypes.T,
	metadataSourcesQueue []execinfrapb.MetadataSource,
) error {
	output := &pspec.Output[0]
	if output.Type != execinfrapb.OutputRouterSpec_PASS_THROUGH {
		return s.setupRouter(
			op,
			opOutputTypes,
			output,
			// Pass in a copy of the queue to reset metadataSourcesQueue for
			// further appends without overwriting.
			metadataSourcesQueue,
		)
	}

	if len(output.Streams) != 1 {
		return errors.Errorf("unsupported multi outputstream proc (%d streams)", len(output.Streams))
	}
	outputStream := &output.Streams[0]
	switch outputStream.Type {
	case execinfrapb.StreamEndpointSpec_LOCAL:
		s.streamIDToInputOp[outputStream.StreamID] = opDAGWithMetaSources{rootOperator: op, metadataSources: metadataSourcesQueue}
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
						finishVectorizedStatsCollectors(ctx, flowCtx.Cfg.TestingKnobs.DeterministicStats, vscs, s.procIDs)
						return []execinfrapb.ProducerMetadata{{TraceData: tracing.GetRecording(span)}}
					},
				},
			)
		}
		outbox, err := s.setupRemoteOutputStream(op, opOutputTypes, outputStream, metadataSourcesQueue)
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
		var outputStatsToTrace func()
		if s.recordingStats {
			// Make a copy given that vectorizedStatsCollectorsQueue is reset and
			// appended to.
			vscq := append([]*colexec.VectorizedStatsCollector(nil), s.vectorizedStatsCollectorsQueue...)
			outputStatsToTrace = func() {
				finishVectorizedStatsCollectors(
					ctx, flowCtx.Cfg.TestingKnobs.DeterministicStats, vscq, s.procIDs,
				)
			}
		}
		proc, err := colexec.NewMaterializer(
			flowCtx,
			pspec.ProcessorID,
			op,
			columnTypes,
			&execinfrapb.PostProcessSpec{},
			s.syncFlowConsumer,
			metadataSourcesQueue,
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
	acc *mon.BoundAccount,
	opt flowinfra.FuseOpt,
) (leaves []execinfra.OpNode, err error) {
	streamIDToSpecIdx := make(map[execinfrapb.StreamID]int)
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

	inputs := make([]colexec.Operator, 0, 2)
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
		inputs = inputs[:0]
		for i := range pspec.Input {
			input, metadataSources, memUsed, err := s.setupInput(pspec.Input[i], opt)
			if err != nil {
				return nil, err
			}
			if err = acc.Grow(ctx, int64(memUsed)); err != nil {
				return nil, errors.Wrapf(err, "not enough memory to setup vectorized plan")
			}
			metadataSourcesQueue = append(metadataSourcesQueue, metadataSources...)
			inputs = append(inputs, input)
		}

		result, err := colexec.NewColOperator(ctx, flowCtx, pspec, inputs)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to vectorize execution plan")
		}
		if flowCtx.EvalCtx.SessionData.VectorizeMode == sessiondata.VectorizeAuto &&
			!result.IsStreaming {
			return nil, errors.Errorf("non-streaming operator encountered when vectorize=auto")
		}
		if err = acc.Grow(ctx, int64(result.MemUsage)); err != nil {
			return nil, errors.Wrapf(err, "not enough memory to setup vectorized plan")
		}
		metadataSourcesQueue = append(metadataSourcesQueue, result.MetadataSources...)

		op := result.Op
		if s.recordingStats {
			vsc, err := wrapWithVectorizedStatsCollector(op, inputs, pspec)
			if err != nil {
				return nil, err
			}
			s.vectorizedStatsCollectorsQueue = append(s.vectorizedStatsCollectorsQueue, vsc)
			s.procIDs = append(s.procIDs, pspec.ProcessorID)
			op = vsc
		}

		if flowCtx.EvalCtx.SessionData.VectorizeMode == sessiondata.VectorizeAuto &&
			pspec.Output[0].Type == execinfrapb.OutputRouterSpec_BY_HASH {
			// exec.HashRouter can do unlimited buffering, and it is present in the
			// flow, so we don't want to run such a flow via the vectorized engine
			// when vectorize=auto.
			return nil, errors.Errorf("hash router encountered when vectorize=auto")
		}
		opOutputTypes, err := typeconv.FromColumnTypes(result.ColumnTypes)
		if err != nil {
			return nil, err
		}
		if err = s.setupOutput(
			ctx, flowCtx, pspec, op, opOutputTypes, metadataSourcesQueue,
		); err != nil {
			return nil, err
		}

		// Now queue all outputs from this op whose inputs are already all
		// populated.
	NEXTOUTPUT:
		for i := range pspec.Output {
			for j := range pspec.Output[i].Streams {
				stream := &pspec.Output[i].Streams[j]
				if stream.Type != execinfrapb.StreamEndpointSpec_LOCAL {
					continue
				}
				procIdx, ok := streamIDToSpecIdx[stream.StreamID]
				if !ok {
					return nil, errors.Errorf("couldn't find stream %d", stream.StreamID)
				}
				outputSpec := &processorSpecs[procIdx]
				for k := range outputSpec.Input {
					for l := range outputSpec.Input[k].Streams {
						stream := outputSpec.Input[k].Streams[l]
						if stream.Type == execinfrapb.StreamEndpointSpec_REMOTE {
							// Remote streams are not present in streamIDToInputOp. The
							// Inboxes that consume these streams are created at the same time
							// as the operator that needs them, so skip the creation check for
							// this input.
							continue
						}
						if _, ok := s.streamIDToInputOp[stream.StreamID]; !ok {
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
		execerror.VectorizedInternalPanic("not all vectorized stats collectors have been processed")
	}
	return s.leaves, nil
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
func SupportsVectorized(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorSpecs []execinfrapb.ProcessorSpec,
	fuseOpt flowinfra.FuseOpt,
) (leaves []execinfra.OpNode, err error) {
	creator := newVectorizedFlowCreator(
		newNoopFlowCreatorHelper(),
		vectorizedRemoteComponentCreator{},
		false,                   /* recordingStats */
		nil,                     /* waitGroup */
		&execinfra.RowChannel{}, /* syncFlowConsumer */
		nil,                     /* nodeDialer */
		execinfrapb.FlowID{},
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
	acc := memoryMonitor.MakeBoundAccount()
	defer acc.Close(ctx)
	if vecErr := execerror.CatchVectorizedRuntimeError(func() {
		leaves, err = creator.setupFlow(ctx, flowCtx, processorSpecs, &acc, fuseOpt)
	}); vecErr != nil {
		return leaves, vecErr
	}
	return leaves, err
}
