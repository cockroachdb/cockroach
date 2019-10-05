// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package distsql

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/colflow"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/rowflow"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/opentracing/opentracing-go"
)

// minFlowDrainWait is the minimum amount of time a draining server allows for
// any incoming flows to be registered. It acts as a grace period in which the
// draining server waits for its gossiped draining state to be received by other
// nodes.
const minFlowDrainWait = 1 * time.Second

var noteworthyMemoryUsageBytes = envutil.EnvOrDefaultInt64("COCKROACH_NOTEWORTHY_DISTSQL_MEMORY_USAGE", 1024*1024 /* 1MB */)

// ServerImpl implements the server for the distributed SQL APIs.
type ServerImpl struct {
	execinfra.ServerConfig
	flowRegistry  *flowinfra.FlowRegistry
	flowScheduler *flowinfra.FlowScheduler
	memMonitor    mon.BytesMonitor
	regexpCache   *tree.RegexpCache
}

var _ execinfrapb.DistSQLServer = &ServerImpl{}

// NewServer instantiates a DistSQLServer.
func NewServer(ctx context.Context, cfg execinfra.ServerConfig) *ServerImpl {
	ds := &ServerImpl{
		ServerConfig:  cfg,
		regexpCache:   tree.NewRegexpCache(512),
		flowRegistry:  flowinfra.NewFlowRegistry(cfg.NodeID.Get()),
		flowScheduler: flowinfra.NewFlowScheduler(cfg.AmbientContext, cfg.Stopper, cfg.Settings, cfg.Metrics),
		memMonitor: mon.MakeMonitor(
			"distsql",
			mon.MemoryResource,
			cfg.Metrics.CurBytesCount,
			cfg.Metrics.MaxBytesHist,
			-1, /* increment: use default block size */
			noteworthyMemoryUsageBytes,
			cfg.Settings,
		),
	}
	ds.memMonitor.Start(ctx, cfg.ParentMemoryMonitor, mon.BoundAccount{})
	return ds
}

// Start launches workers for the server.
func (ds *ServerImpl) Start() {
	// Gossip the version info so that other nodes don't plan incompatible flows
	// for us.
	if err := ds.ServerConfig.Gossip.AddInfoProto(
		gossip.MakeDistSQLNodeVersionKey(ds.ServerConfig.NodeID.Get()),
		&execinfrapb.DistSQLVersionGossipInfo{
			Version:            execinfra.Version,
			MinAcceptedVersion: execinfra.MinAcceptedVersion,
		},
		0, // ttl - no expiration
	); err != nil {
		panic(err)
	}

	if err := ds.setDraining(false); err != nil {
		panic(err)
	}

	ds.flowScheduler.Start()
}

// Drain changes the node's draining state through gossip and drains the
// server's flowRegistry. See flowRegistry.Drain for more details.
func (ds *ServerImpl) Drain(ctx context.Context, flowDrainWait time.Duration) {
	if err := ds.setDraining(true); err != nil {
		log.Warningf(ctx, "unable to gossip distsql draining state: %s", err)
	}

	flowWait := flowDrainWait
	minWait := minFlowDrainWait
	if ds.ServerConfig.TestingKnobs.DrainFast {
		flowWait = 0
		minWait = 0
	} else if len(ds.Gossip.Outgoing()) == 0 {
		// If there is only one node in the cluster (us), there's no need to
		// wait a minimum time for the draining state to be gossiped.
		minWait = 0
	}
	ds.flowRegistry.Drain(flowWait, minWait)
}

// Undrain changes the node's draining state through gossip and undrains the
// server's flowRegistry. See flowRegistry.Undrain for more details.
func (ds *ServerImpl) Undrain(ctx context.Context) {
	ds.flowRegistry.Undrain()
	if err := ds.setDraining(false); err != nil {
		log.Warningf(ctx, "unable to gossip distsql draining state: %s", err)
	}
}

// setDraining changes the node's draining state through gossip to the provided
// state.
func (ds *ServerImpl) setDraining(drain bool) error {
	return ds.ServerConfig.Gossip.AddInfoProto(
		gossip.MakeDistSQLDrainingKey(ds.ServerConfig.NodeID.Get()),
		&execinfrapb.DistSQLDrainingInfo{
			Draining: drain,
		},
		0, // ttl - no expiration
	)
}

// FlowVerIsCompatible checks a flow's version is compatible with this node's
// DistSQL version.
func FlowVerIsCompatible(
	flowVer, minAcceptedVersion, serverVersion execinfrapb.DistSQLVersion,
) bool {
	return flowVer >= minAcceptedVersion && flowVer <= serverVersion
}

// setupFlow creates a Flow.
//
// Args:
// localState: Specifies if the flow runs entirely on this node and, if it does,
//   specifies the txn and other attributes.
//
// Note: unless an error is returned, the returned context contains a span that
// must be finished through Flow.Cleanup.
func (ds *ServerImpl) setupFlow(
	ctx context.Context,
	parentSpan opentracing.Span,
	parentMonitor *mon.BytesMonitor,
	req *execinfrapb.SetupFlowRequest,
	syncFlowConsumer execinfra.RowReceiver,
	localState LocalState,
) (context.Context, flowinfra.Flow, error) {
	if !FlowVerIsCompatible(req.Version, execinfra.MinAcceptedVersion, execinfra.Version) {
		err := errors.Errorf(
			"version mismatch in flow request: %d; this node accepts %d through %d",
			req.Version, execinfra.MinAcceptedVersion, execinfra.Version,
		)
		log.Warning(ctx, err)
		return ctx, nil, err
	}
	nodeID := ds.ServerConfig.NodeID.Get()
	if nodeID == 0 {
		return nil, nil, errors.AssertionFailedf("setupFlow called before the NodeID was resolved")
	}

	const opName = "flow"
	var sp opentracing.Span
	if parentSpan == nil {
		sp = ds.Tracer.(*tracing.Tracer).StartRootSpan(
			opName, logtags.FromContext(ctx), tracing.NonRecordableSpan)
	} else if localState.IsLocal {
		// If we're a local flow, we don't need a "follows from" relationship: we're
		// going to run this flow synchronously.
		// TODO(andrei): localState.IsLocal is not quite the right thing to use.
		//  If that field is unset, we might still want to create a child span if
		//  this flow is run synchronously.
		sp = tracing.StartChildSpan(opName, parentSpan, logtags.FromContext(ctx), false /* separateRecording */)
	} else {
		// We use FollowsFrom because the flow's span outlives the SetupFlow request.
		// TODO(andrei): We should use something more efficient than StartSpan; we
		// should use AmbientContext.AnnotateCtxWithSpan() but that interface
		// doesn't currently support FollowsFrom relationships.
		sp = ds.Tracer.StartSpan(
			opName,
			opentracing.FollowsFrom(parentSpan.Context()),
			tracing.LogTagsFromCtx(ctx),
		)
	}
	// sp will be Finish()ed by Flow.Cleanup().
	ctx = opentracing.ContextWithSpan(ctx, sp)

	// The monitor opened here are closed in Flow.Cleanup().
	monitor := mon.MakeMonitor(
		"flow",
		mon.MemoryResource,
		ds.Metrics.CurBytesCount,
		ds.Metrics.MaxBytesHist,
		-1, /* use default block size */
		noteworthyMemoryUsageBytes,
		ds.Settings,
	)
	monitor.Start(ctx, parentMonitor, mon.BoundAccount{})

	var evalCtx *tree.EvalContext
	if localState.EvalContext != nil {
		evalCtx = localState.EvalContext
		evalCtx.Mon = &monitor
	} else {
		location, err := timeutil.TimeZoneStringToLocation(req.EvalContext.Location)
		if err != nil {
			tracing.FinishSpan(sp)
			return ctx, nil, err
		}

		var be sessiondata.BytesEncodeFormat
		switch req.EvalContext.BytesEncodeFormat {
		case execinfrapb.BytesEncodeFormat_HEX:
			be = sessiondata.BytesEncodeHex
		case execinfrapb.BytesEncodeFormat_ESCAPE:
			be = sessiondata.BytesEncodeEscape
		case execinfrapb.BytesEncodeFormat_BASE64:
			be = sessiondata.BytesEncodeBase64
		default:
			return nil, nil, errors.AssertionFailedf("unknown byte encode format: %s",
				errors.Safe(req.EvalContext.BytesEncodeFormat))
		}
		sd := &sessiondata.SessionData{
			ApplicationName: req.EvalContext.ApplicationName,
			Database:        req.EvalContext.Database,
			User:            req.EvalContext.User,
			SearchPath:      sessiondata.MakeSearchPath(req.EvalContext.SearchPath),
			SequenceState:   sessiondata.NewSequenceState(),
			DataConversion: sessiondata.DataConversionConfig{
				Location:          location,
				BytesEncodeFormat: be,
				ExtraFloatDigits:  int(req.EvalContext.ExtraFloatDigits),
			},
		}
		// Enable better compatibility with PostgreSQL date math.
		if req.Version >= 22 {
			sd.DurationAdditionMode = duration.AdditionModeCompatible
		} else {
			sd.DurationAdditionMode = duration.AdditionModeLegacy
		}
		ie := &lazyInternalExecutor{
			newInternalExecutor: func() tree.SessionBoundInternalExecutor {
				return ds.SessionBoundInternalExecutorFactory(ctx, sd)
			},
		}

		evalCtx = &tree.EvalContext{
			Settings:    ds.ServerConfig.Settings,
			SessionData: sd,
			ClusterID:   ds.ServerConfig.ClusterID.Get(),
			ClusterName: ds.ServerConfig.ClusterName,
			NodeID:      nodeID,
			ReCache:     ds.regexpCache,
			Mon:         &monitor,
			// TODO(andrei): This is wrong. Each processor should override Ctx with its
			// own context.
			Context:          ctx,
			Planner:          &sqlbase.DummyEvalPlanner{},
			SessionAccessor:  &sqlbase.DummySessionAccessor{},
			Sequence:         &sqlbase.DummySequenceOperators{},
			InternalExecutor: ie,
		}
		evalCtx.SetStmtTimestamp(timeutil.Unix(0 /* sec */, req.EvalContext.StmtTimestampNanos))
		evalCtx.SetTxnTimestamp(timeutil.Unix(0 /* sec */, req.EvalContext.TxnTimestampNanos))
		var haveSequences bool
		for _, seq := range req.EvalContext.SeqState.Seqs {
			evalCtx.SessionData.SequenceState.RecordValue(seq.SeqID, seq.LatestVal)
			haveSequences = true
		}
		if haveSequences {
			evalCtx.SessionData.SequenceState.SetLastSequenceIncremented(
				*req.EvalContext.SeqState.LastSeqIncremented)
		}
	}
	// TODO(radu): we should sanity check some of these fields.
	flowCtx := execinfra.FlowCtx{
		AmbientContext: ds.AmbientContext,
		Cfg:            &ds.ServerConfig,
		ID:             req.Flow.FlowID,
		EvalCtx:        evalCtx,
		NodeID:         nodeID,
		TraceKV:        req.TraceKV,
		Local:          localState.IsLocal,
	}
	// req always contains the desired vectorize mode, regardless of whether we
	// have non-nil localState.EvalContext. We don't want to update EvalContext
	// itself when the vectorize mode needs to be changed because we would need
	// to restore the original value which can have data races under stress.
	isVectorized := sessiondata.VectorizeExecMode(req.EvalContext.Vectorize) != sessiondata.VectorizeOff
	f := newFlow(flowCtx, ds.flowRegistry, syncFlowConsumer, localState.LocalProcs, isVectorized)
	opt := flowinfra.FuseNormally
	if localState.IsLocal {
		// If there's no remote flows, fuse everything. This is needed in order for
		// us to be able to use the RootTxn for the flow 's execution; the RootTxn
		// doesn't allow for concurrent operations. Local flows with mutations need
		// to use the RootTxn.
		opt = flowinfra.FuseAggressively
	}
	if err := f.Setup(ctx, &req.Flow, opt); err != nil {
		log.Errorf(ctx, "error setting up flow: %s", err)
		tracing.FinishSpan(sp)
		ctx = opentracing.ContextWithSpan(ctx, nil)
		return ctx, nil, err
	}
	if !f.IsLocal() {
		flowCtx.AddLogTag("f", f.GetFlowCtx().ID.Short())
		flowCtx.AnnotateCtx(ctx)
		telemetry.Inc(sqltelemetry.DistSQLExecCounter)
	}
	if f.IsVectorized() {
		telemetry.Inc(sqltelemetry.VecExecCounter)
	}

	// Figure out what txn the flow needs to run in, if any. For gateway flows
	// that have no remote flows and also no concurrency, the txn comes from
	// localState.Txn. Otherwise, we create a txn based on the request's
	// TxnCoordMeta.
	var txn *client.Txn
	if localState.IsLocal && !f.ConcurrentExecution() {
		txn = localState.Txn
	} else {
		if meta := req.TxnCoordMeta; meta != nil {
			if meta.Txn.Status != roachpb.PENDING {
				return nil, nil, errors.Errorf("cannot create flow in non-PENDING txn: %s",
					meta.Txn)
			}
			// The flow will run in a LeafTxn because we do not want each distributed
			// Txn to heartbeat the transaction.
			txn = client.NewTxnWithCoordMeta(ctx, ds.FlowDB, req.Flow.Gateway, client.LeafTxn, *meta)
		}
	}
	f.SetTxn(txn)

	return ctx, f, nil
}

func newFlow(
	flowCtx execinfra.FlowCtx,
	flowReg *flowinfra.FlowRegistry,
	syncFlowConsumer execinfra.RowReceiver,
	localProcessors []execinfra.LocalProcessor,
	isVectorized bool,
) flowinfra.Flow {
	base := flowinfra.NewFlowBase(flowCtx, flowReg, syncFlowConsumer, localProcessors, isVectorized)
	if isVectorized {
		return colflow.NewVectorizedFlow(base)
	}
	return rowflow.NewRowBasedFlow(base)
}

// SetupSyncFlow sets up a synchronous flow, connecting the sync response
// output stream to the given RowReceiver. The flow is not started. The flow
// will be associated with the given context.
// Note: the returned context contains a span that must be finished through
// Flow.Cleanup.
func (ds *ServerImpl) SetupSyncFlow(
	ctx context.Context,
	parentMonitor *mon.BytesMonitor,
	req *execinfrapb.SetupFlowRequest,
	output execinfra.RowReceiver,
) (context.Context, flowinfra.Flow, error) {
	ctx, f, err := ds.setupFlow(ds.AnnotateCtx(ctx), opentracing.SpanFromContext(ctx), parentMonitor,
		req, output, LocalState{})
	if err != nil {
		return nil, nil, err
	}
	return ctx, f, err
}

// LocalState carries information that is required to set up a flow with wrapped
// planNodes.
type LocalState struct {
	EvalContext *tree.EvalContext

	// IsLocal is set if the flow is running on the gateway and there are no
	// remote flows.
	IsLocal bool

	// Txn is filled in on the gateway only. It is the RootTxn that the query is running in.
	// This will be used directly by the flow if the flow has no concurrency and IsLocal is set.
	// If there is concurrency, a LeafTxn will be created.
	Txn *client.Txn

	/////////////////////////////////////////////
	// Fields below are empty if IsLocal == false
	/////////////////////////////////////////////

	// LocalProcs is an array of planNodeToRowSource processors. It's in order and
	// will be indexed into by the RowSourceIdx field in LocalPlanNodeSpec.
	LocalProcs []execinfra.LocalProcessor
}

// SetupLocalSyncFlow sets up a synchronous flow on the current (planning) node.
// It's used by the gateway node to set up the flows local to it.
// It's the same as SetupSyncFlow except it takes the localState.
func (ds *ServerImpl) SetupLocalSyncFlow(
	ctx context.Context,
	parentMonitor *mon.BytesMonitor,
	req *execinfrapb.SetupFlowRequest,
	output execinfra.RowReceiver,
	localState LocalState,
) (context.Context, flowinfra.Flow, error) {
	ctx, f, err := ds.setupFlow(ctx, opentracing.SpanFromContext(ctx), parentMonitor, req, output,
		localState)
	if err != nil {
		return nil, nil, err
	}
	return ctx, f, err
}

// RunSyncFlow is part of the DistSQLServer interface.
func (ds *ServerImpl) RunSyncFlow(stream execinfrapb.DistSQL_RunSyncFlowServer) error {
	// Set up the outgoing mailbox for the stream.
	mbox := flowinfra.NewOutboxSyncFlowStream(stream)

	firstMsg, err := stream.Recv()
	if err != nil {
		return err
	}
	if firstMsg.SetupFlowRequest == nil {
		return errors.AssertionFailedf("first message in RunSyncFlow doesn't contain SetupFlowRequest")
	}
	req := firstMsg.SetupFlowRequest
	ctx, f, err := ds.SetupSyncFlow(stream.Context(), &ds.memMonitor, req, mbox)
	if err != nil {
		return err
	}
	mbox.SetFlowCtx(f.GetFlowCtx())

	if err := ds.Stopper.RunTask(ctx, "distsql.ServerImpl: sync flow", func(ctx context.Context) {
		ctx, ctxCancel := contextutil.WithCancel(ctx)
		defer ctxCancel()
		f.AddStartable(mbox)
		ds.Metrics.FlowStart()
		if err := f.Run(ctx, func() {}); err != nil {
			log.Fatalf(ctx, "unexpected error from syncFlow.Start(): %s "+
				"The error should have gone to the consumer.", err)
		}
		f.Cleanup(ctx)
		ds.Metrics.FlowStop()
	}); err != nil {
		return err
	}
	return mbox.Err()
}

// SetupFlow is part of the DistSQLServer interface.
func (ds *ServerImpl) SetupFlow(
	ctx context.Context, req *execinfrapb.SetupFlowRequest,
) (*execinfrapb.SimpleResponse, error) {
	log.VEventf(ctx, 1, "received SetupFlow request from n%v for flow %v", req.Flow.Gateway, req.Flow.FlowID)
	parentSpan := opentracing.SpanFromContext(ctx)

	// Note: the passed context will be canceled when this RPC completes, so we
	// can't associate it with the flow.
	ctx = ds.AnnotateCtx(context.Background())
	ctx, f, err := ds.setupFlow(ctx, parentSpan, &ds.memMonitor, req, nil /* syncFlowConsumer */, LocalState{})
	if err == nil {
		err = ds.flowScheduler.ScheduleFlow(ctx, f)
	}
	if err != nil {
		// We return flow deployment errors in the response so that they are
		// packaged correctly over the wire. If we return them directly to this
		// function, they become part of an rpc error.
		return &execinfrapb.SimpleResponse{Error: execinfrapb.NewError(ctx, err)}, nil
	}
	return &execinfrapb.SimpleResponse{}, nil
}

func (ds *ServerImpl) flowStreamInt(
	ctx context.Context, stream execinfrapb.DistSQL_FlowStreamServer,
) error {
	// Receive the first message.
	msg, err := stream.Recv()
	if err != nil {
		if err == io.EOF {
			return errors.AssertionFailedf("missing header message")
		}
		return err
	}
	if msg.Header == nil {
		return errors.AssertionFailedf("no header in first message")
	}
	flowID := msg.Header.FlowID
	streamID := msg.Header.StreamID
	if log.V(1) {
		log.Infof(ctx, "connecting inbound stream %s/%d", flowID.Short(), streamID)
	}
	f, streamStrategy, cleanup, err := ds.flowRegistry.ConnectInboundStream(
		ctx, flowID, streamID, stream, flowinfra.SettingFlowStreamTimeout.Get(&ds.Settings.SV),
	)
	if err != nil {
		return err
	}
	defer cleanup()
	log.VEventf(ctx, 1, "connected inbound stream %s/%d", flowID.Short(), streamID)
	return streamStrategy.Run(f.AnnotateCtx(ctx), stream, msg, f)
}

// FlowStream is part of the DistSQLServer interface.
func (ds *ServerImpl) FlowStream(stream execinfrapb.DistSQL_FlowStreamServer) error {
	ctx := ds.AnnotateCtx(stream.Context())
	err := ds.flowStreamInt(ctx, stream)
	if err != nil {
		log.Error(ctx, err)
	}
	return err
}

// lazyInternalExecutor is a tree.SessionBoundInternalExecutor that initializes
// itself only on the first call to QueryRow.
type lazyInternalExecutor struct {
	// Set when an internal executor has been initialized.
	tree.SessionBoundInternalExecutor

	// Used for initializing the internal executor exactly once.
	once sync.Once

	// newInternalExecutor must be set when instantiating a lazyInternalExecutor,
	// it provides an internal executor to use when necessary.
	newInternalExecutor func() tree.SessionBoundInternalExecutor
}

var _ tree.SessionBoundInternalExecutor = &lazyInternalExecutor{}

func (ie *lazyInternalExecutor) QueryRow(
	ctx context.Context, opName string, txn *client.Txn, stmt string, qargs ...interface{},
) (tree.Datums, error) {
	ie.once.Do(func() {
		ie.SessionBoundInternalExecutor = ie.newInternalExecutor()
	})
	return ie.SessionBoundInternalExecutor.QueryRow(ctx, opName, txn, stmt, qargs...)
}
