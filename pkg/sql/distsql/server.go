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
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec"
	"github.com/cockroachdb/cockroach/pkg/sql/colflow"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/faketreeeval"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/rowflow"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// minFlowDrainWait is the minimum amount of time a draining server allows for
// any incoming flows to be registered. It acts as a grace period in which the
// draining server waits for its gossiped draining state to be received by other
// nodes.
const minFlowDrainWait = 1 * time.Second

// MultiTenancyIssueNo is the issue tracking DistSQL's Gossip and
// NodeID dependencies.
//
// See https://github.com/cockroachdb/cockroach/issues/47900.
const MultiTenancyIssueNo = 47900

var noteworthyMemoryUsageBytes = envutil.EnvOrDefaultInt64("COCKROACH_NOTEWORTHY_DISTSQL_MEMORY_USAGE", 1024*1024 /* 1MB */)

// ServerImpl implements the server for the distributed SQL APIs.
type ServerImpl struct {
	execinfra.ServerConfig
	flowRegistry  *flowinfra.FlowRegistry
	flowScheduler *flowinfra.FlowScheduler
	memMonitor    *mon.BytesMonitor
	regexpCache   *tree.RegexpCache
}

var _ execinfrapb.DistSQLServer = &ServerImpl{}

// NewServer instantiates a DistSQLServer.
func NewServer(
	ctx context.Context, cfg execinfra.ServerConfig, flowScheduler *flowinfra.FlowScheduler,
) *ServerImpl {
	ds := &ServerImpl{
		ServerConfig:  cfg,
		regexpCache:   tree.NewRegexpCache(512),
		flowRegistry:  flowinfra.NewFlowRegistry(),
		flowScheduler: flowScheduler,
		memMonitor: mon.NewMonitor(
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
	// We have to initialize the flow scheduler at the same time we're creating
	// the DistSQLServer because the latter will be registered as a gRPC service
	// right away, so the RPCs might start coming in pretty much right after the
	// current method returns. See #66330.
	ds.flowScheduler.Init(ds.Metrics)

	colexec.HashAggregationDiskSpillingEnabled.SetOnChange(&cfg.Settings.SV, func(ctx context.Context) {
		if !colexec.HashAggregationDiskSpillingEnabled.Get(&cfg.Settings.SV) {
			telemetry.Inc(sqltelemetry.HashAggregationDiskSpillingDisabled)
		}
	})

	return ds
}

// Start launches workers for the server.
//
// Note that the initialization of the server required for performing the
// incoming RPCs needs to go into NewServer above because once that method
// returns, the server is registered as a gRPC service and needs to be fully
// initialized. For example, the initialization of the flow scheduler has to
// happen in NewServer.
func (ds *ServerImpl) Start() {
	// Gossip the version info so that other nodes don't plan incompatible flows
	// for us.
	if g, ok := ds.ServerConfig.Gossip.Optional(MultiTenancyIssueNo); ok {
		if nodeID, ok := ds.ServerConfig.NodeID.OptionalNodeID(); ok {
			if err := g.AddInfoProto(
				gossip.MakeDistSQLNodeVersionKey(nodeID),
				&execinfrapb.DistSQLVersionGossipInfo{
					Version:            execinfra.Version,
					MinAcceptedVersion: execinfra.MinAcceptedVersion,
				},
				0, // ttl - no expiration
			); err != nil {
				panic(err)
			}
		}
	}

	if err := ds.setDraining(false); err != nil {
		panic(err)
	}

	ds.flowScheduler.Start()
}

// NumRemoteFlowsInQueue returns the number of remote flows scheduled to run on
// this server which are currently in the queue of the flow scheduler.
func (ds *ServerImpl) NumRemoteFlowsInQueue() int {
	return ds.flowScheduler.NumFlowsInQueue()
}

// NumRemoteRunningFlows returns the number of remote flows currently running on
// this server.
func (ds *ServerImpl) NumRemoteRunningFlows() int {
	return ds.flowScheduler.NumRunningFlows()
}

// SetCancelDeadFlowsCallback sets a testing callback that will be executed by
// the flow scheduler at the end of CancelDeadFlows call. The callback must be
// concurrency-safe.
func (ds *ServerImpl) SetCancelDeadFlowsCallback(cb func(int)) {
	ds.flowScheduler.TestingKnobs.CancelDeadFlowsCallback = cb
}

// Drain changes the node's draining state through gossip and drains the
// server's flowRegistry. See flowRegistry.Drain for more details.
func (ds *ServerImpl) Drain(
	ctx context.Context, flowDrainWait time.Duration, reporter func(int, redact.SafeString),
) {
	if err := ds.setDraining(true); err != nil {
		log.Warningf(ctx, "unable to gossip distsql draining state: %s", err)
	}

	flowWait := flowDrainWait
	minWait := minFlowDrainWait
	if ds.ServerConfig.TestingKnobs.DrainFast {
		flowWait = 0
		minWait = 0
	} else if g, ok := ds.Gossip.Optional(MultiTenancyIssueNo); !ok || len(g.Outgoing()) == 0 {
		// If there is only one node in the cluster (us), there's no need to
		// wait a minimum time for the draining state to be gossiped.
		minWait = 0
	}
	ds.flowRegistry.Drain(flowWait, minWait, reporter)
}

// setDraining changes the node's draining state through gossip to the provided
// state.
func (ds *ServerImpl) setDraining(drain bool) error {
	nodeID, ok := ds.ServerConfig.NodeID.OptionalNodeID()
	if !ok {
		// Ignore draining requests when running on behalf of a tenant.
		// NB: intentionally swallow the error or the server will fatal.
		_ = MultiTenancyIssueNo // related issue
		return nil
	}
	if g, ok := ds.ServerConfig.Gossip.Optional(MultiTenancyIssueNo); ok {
		return g.AddInfoProto(
			gossip.MakeDistSQLDrainingKey(nodeID),
			&execinfrapb.DistSQLDrainingInfo{
				Draining: drain,
			},
			0, // ttl - no expiration
		)
	}
	return nil
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
	parentSpan *tracing.Span,
	parentMonitor *mon.BytesMonitor,
	req *execinfrapb.SetupFlowRequest,
	rowSyncFlowConsumer execinfra.RowReceiver,
	batchSyncFlowConsumer execinfra.BatchReceiver,
	localState LocalState,
) (context.Context, flowinfra.Flow, execinfra.OpChains, error) {
	if !FlowVerIsCompatible(req.Version, execinfra.MinAcceptedVersion, execinfra.Version) {
		err := errors.Errorf(
			"version mismatch in flow request: %d; this node accepts %d through %d",
			req.Version, execinfra.MinAcceptedVersion, execinfra.Version,
		)
		log.Warningf(ctx, "%v", err)
		return ctx, nil, nil, err
	}

	const opName = "flow"
	var sp *tracing.Span // will be Finish()ed by Flow.Cleanup()
	if parentSpan == nil {
		ctx, sp = ds.Tracer.StartSpanCtx(ctx, opName)
	} else if localState.IsLocal {
		// If we're a local flow, we don't need a "follows from" relationship: we're
		// going to run this flow synchronously.
		// TODO(andrei): localState.IsLocal is not quite the right thing to use.
		//  If that field is unset, we might still want to create a child span if
		//  this flow is run synchronously.
		ctx, sp = ds.Tracer.StartSpanCtx(ctx, opName, tracing.WithParentAndAutoCollection(parentSpan))
	} else {
		// We use FollowsFrom because the flow's span outlives the SetupFlow request.
		ctx, sp = ds.Tracer.StartSpanCtx(
			ctx,
			opName,
			tracing.WithParentAndAutoCollection(parentSpan),
			tracing.WithFollowsFrom(),
		)
	}

	// The monitor opened here is closed in Flow.Cleanup().
	monitor := mon.NewMonitor(
		"flow",
		mon.MemoryResource,
		ds.Metrics.CurBytesCount,
		ds.Metrics.MaxBytesHist,
		-1, /* use default block size */
		noteworthyMemoryUsageBytes,
		ds.Settings,
	)
	monitor.Start(ctx, parentMonitor, mon.BoundAccount{})

	makeLeaf := func(req *execinfrapb.SetupFlowRequest) (*kv.Txn, error) {
		tis := req.LeafTxnInputState
		if tis == nil {
			// This must be a flow running for some bulk-io operation that doesn't use
			// a txn.
			return nil, nil
		}
		if tis.Txn.Status != roachpb.PENDING {
			return nil, errors.AssertionFailedf("cannot create flow in non-PENDING txn: %s",
				tis.Txn)
		}
		// The flow will run in a LeafTxn because we do not want each distributed
		// Txn to heartbeat the transaction.
		return kv.NewLeafTxn(ctx, ds.DB, req.Flow.Gateway, tis), nil
	}

	var evalCtx *tree.EvalContext
	var leafTxn *kv.Txn
	if localState.EvalContext != nil {
		evalCtx = localState.EvalContext
		evalCtx.Mon = monitor
	} else {
		if localState.IsLocal {
			return nil, nil, nil, errors.AssertionFailedf(
				"EvalContext expected to be populated when IsLocal is set")
		}

		sd, err := sessiondata.UnmarshalNonLocal(req.EvalContext.SessionData)
		if err != nil {
			sp.Finish()
			return ctx, nil, nil, err
		}
		ie := &lazyInternalExecutor{
			newInternalExecutor: func() sqlutil.InternalExecutor {
				return ds.SessionBoundInternalExecutorFactory(ctx, sd)
			},
		}

		// It's important to populate evalCtx.Txn early. We'll write it again in the
		// f.SetTxn() call below, but by then it will already have been captured by
		// processors.
		leafTxn, err = makeLeaf(req)
		if err != nil {
			return nil, nil, nil, err
		}
		evalCtx = &tree.EvalContext{
			Settings:    ds.ServerConfig.Settings,
			SessionData: sd,
			ClusterID:   ds.ServerConfig.ClusterID.Get(),
			ClusterName: ds.ServerConfig.ClusterName,
			NodeID:      ds.ServerConfig.NodeID,
			Codec:       ds.ServerConfig.Codec,
			ReCache:     ds.regexpCache,
			Mon:         monitor,
			// Most processors will override this Context with their own context in
			// ProcessorBase. StartInternal().
			Context:            ctx,
			Planner:            &faketreeeval.DummyEvalPlanner{},
			PrivilegedAccessor: &faketreeeval.DummyPrivilegedAccessor{},
			SessionAccessor:    &faketreeeval.DummySessionAccessor{},
			ClientNoticeSender: &faketreeeval.DummyClientNoticeSender{},
			Sequence:           &faketreeeval.DummySequenceOperators{},
			Tenant:             &faketreeeval.DummyTenantOperator{},
			InternalExecutor:   ie,
			Txn:                leafTxn,
			SQLLivenessReader:  ds.ServerConfig.SQLLivenessReader,
			SQLStatsResetter:   ds.ServerConfig.SQLStatsResetter,
		}
		evalCtx.SetStmtTimestamp(timeutil.Unix(0 /* sec */, req.EvalContext.StmtTimestampNanos))
		evalCtx.SetTxnTimestamp(timeutil.Unix(0 /* sec */, req.EvalContext.TxnTimestampNanos))
	}

	// Create the FlowCtx for the flow.
	flowCtx := ds.newFlowContext(
		ctx, req.Flow.FlowID, evalCtx, req.TraceKV, req.CollectStats, localState, req.Flow.Gateway == roachpb.NodeID(ds.NodeID.SQLInstanceID()),
	)

	// req always contains the desired vectorize mode, regardless of whether we
	// have non-nil localState.EvalContext. We don't want to update EvalContext
	// itself when the vectorize mode needs to be changed because we would need
	// to restore the original value which can have data races under stress.
	isVectorized := req.EvalContext.SessionData.VectorizeMode != sessiondatapb.VectorizeOff
	f := newFlow(
		flowCtx, ds.flowRegistry, rowSyncFlowConsumer, batchSyncFlowConsumer,
		localState.LocalProcs, isVectorized,
	)
	opt := flowinfra.FuseNormally
	if localState.IsLocal {
		// If there's no remote flows, fuse everything. This is needed in order for
		// us to be able to use the RootTxn for the flow 's execution; the RootTxn
		// doesn't allow for concurrent operations. Local flows with mutations need
		// to use the RootTxn.
		opt = flowinfra.FuseAggressively
	}

	var opChains execinfra.OpChains
	var err error
	ctx, opChains, err = f.Setup(ctx, &req.Flow, opt)
	if err != nil {
		log.Errorf(ctx, "error setting up flow: %s", err)
		// Flow.Cleanup will not be called, so we have to close the memory monitor
		// and finish the span manually.
		monitor.Stop(ctx)
		sp.Finish()
		ctx = tracing.ContextWithSpan(ctx, nil)
		return ctx, nil, nil, err
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
	// LeafTxnInputState.
	var txn *kv.Txn
	if localState.IsLocal && !f.ConcurrentTxnUse() {
		txn = localState.Txn
	} else {
		// If I haven't created the leaf already, do it now.
		if leafTxn == nil {
			var err error
			leafTxn, err = makeLeaf(req)
			if err != nil {
				return nil, nil, nil, err
			}
		}
		txn = leafTxn
	}
	// TODO(andrei): We're about to overwrite f.EvalCtx.Txn, but the existing
	// field has already been captured by various processors and operators that
	// have already made a copy of the EvalCtx. In case this is not the gateway,
	// we had already set the LeafTxn on the EvalCtx above, so it's OK. In case
	// this is the gateway, if we're running with the RootTxn, then again it was
	// set above so it's fine. If we're using a LeafTxn on the gateway, though,
	// then the processors have erroneously captured the Root. See #41992.
	f.SetTxn(txn)

	return ctx, f, opChains, nil
}

// newFlowContext creates a new FlowCtx that can be used during execution of
// a flow.
func (ds *ServerImpl) newFlowContext(
	ctx context.Context,
	id execinfrapb.FlowID,
	evalCtx *tree.EvalContext,
	traceKV bool,
	collectStats bool,
	localState LocalState,
	isGatewayNode bool,
) execinfra.FlowCtx {
	// TODO(radu): we should sanity check some of these fields.
	flowCtx := execinfra.FlowCtx{
		AmbientContext: ds.AmbientContext,
		Cfg:            &ds.ServerConfig,
		ID:             id,
		EvalCtx:        evalCtx,
		NodeID:         ds.ServerConfig.NodeID,
		TraceKV:        traceKV,
		CollectStats:   collectStats,
		Local:          localState.IsLocal,
		Gateway:        isGatewayNode,
		// The flow disk monitor is a child of the server's and is closed on
		// Cleanup.
		DiskMonitor: execinfra.NewMonitor(
			ctx, ds.ParentDiskMonitor, "flow-disk-monitor",
		),
	}

	if localState.IsLocal && localState.Collection != nil {
		// If we were passed a descs.Collection to use, then take it. In this case,
		// the caller will handle releasing the used descriptors, so we don't need
		// to cleanup the descriptors when cleaning up the flow.
		flowCtx.TypeResolverFactory = &descs.DistSQLTypeResolverFactory{
			Descriptors: localState.Collection,
			CleanupFunc: func(ctx context.Context) {},
		}
	} else {
		// If we weren't passed a descs.Collection, then make a new one. We are
		// responsible for cleaning it up and releasing any accessed descriptors
		// on flow cleanup.
		collection := descs.NewCollection(
			ds.ServerConfig.Settings,
			ds.ServerConfig.LeaseManager.(*lease.Manager),
			ds.ServerConfig.HydratedTables,
			ds.ServerConfig.VirtualSchemas,
		)
		flowCtx.TypeResolverFactory = &descs.DistSQLTypeResolverFactory{
			Descriptors: collection,
			CleanupFunc: func(ctx context.Context) {
				collection.ReleaseAll(ctx)
			},
		}
	}
	return flowCtx
}

func newFlow(
	flowCtx execinfra.FlowCtx,
	flowReg *flowinfra.FlowRegistry,
	rowSyncFlowConsumer execinfra.RowReceiver,
	batchSyncFlowConsumer execinfra.BatchReceiver,
	localProcessors []execinfra.LocalProcessor,
	isVectorized bool,
) flowinfra.Flow {
	base := flowinfra.NewFlowBase(flowCtx, flowReg, rowSyncFlowConsumer, batchSyncFlowConsumer, localProcessors)
	if isVectorized {
		return colflow.NewVectorizedFlow(base)
	}
	return rowflow.NewRowBasedFlow(base)
}

// LocalState carries information that is required to set up a flow with wrapped
// planNodes.
type LocalState struct {
	EvalContext *tree.EvalContext

	// Collection is set if this flow is running on the gateway as part of user
	// SQL session. It is the current descs.Collection of the planner executing
	// the flow.
	Collection *descs.Collection

	// IsLocal is set if the flow is running on the gateway and there are no
	// remote flows.
	IsLocal bool

	// Txn is filled in on the gateway only. It is the RootTxn that the query is running in.
	// This will be used directly by the flow if the flow has no concurrency and IsLocal is set.
	// If there is concurrency, a LeafTxn will be created.
	Txn *kv.Txn

	// LocalProcs is an array of planNodeToRowSource processors. It's in order and
	// will be indexed into by the RowSourceIdx field in LocalPlanNodeSpec.
	LocalProcs []execinfra.LocalProcessor
}

// SetupLocalSyncFlow sets up a synchronous flow on the current (planning) node,
// connecting the sync response output stream to the given RowReceiver. It's
// used by the gateway node to set up the flows local to it. The flow is not
// started. The flow will be associated with the given context.
// Note: the returned context contains a span that must be finished through
// Flow.Cleanup.
func (ds *ServerImpl) SetupLocalSyncFlow(
	ctx context.Context,
	parentMonitor *mon.BytesMonitor,
	req *execinfrapb.SetupFlowRequest,
	output execinfra.RowReceiver,
	batchOutput execinfra.BatchReceiver,
	localState LocalState,
) (context.Context, flowinfra.Flow, execinfra.OpChains, error) {
	ctx, f, opChains, err := ds.setupFlow(
		ctx, tracing.SpanFromContext(ctx), parentMonitor, req, output, batchOutput, localState,
	)
	if err != nil {
		return nil, nil, nil, err
	}
	return ctx, f, opChains, err
}

// SetupFlow is part of the execinfrapb.DistSQLServer interface.
func (ds *ServerImpl) SetupFlow(
	ctx context.Context, req *execinfrapb.SetupFlowRequest,
) (*execinfrapb.SimpleResponse, error) {
	log.VEventf(ctx, 1, "received SetupFlow request from n%v for flow %v", req.Flow.Gateway, req.Flow.FlowID)
	parentSpan := tracing.SpanFromContext(ctx)

	// Note: the passed context will be canceled when this RPC completes, so we
	// can't associate it with the flow.
	ctx = ds.AnnotateCtx(context.Background())
	ctx, f, _, err := ds.setupFlow(
		ctx, parentSpan, ds.memMonitor, req, nil, /* rowSyncFlowConsumer */
		nil /* batchSyncFlowConsumer */, LocalState{},
	)
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

// CancelDeadFlows is part of the execinfrapb.DistSQLServer interface.
func (ds *ServerImpl) CancelDeadFlows(
	_ context.Context, req *execinfrapb.CancelDeadFlowsRequest,
) (*execinfrapb.SimpleResponse, error) {
	ds.flowScheduler.CancelDeadFlows(req)
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

// FlowStream is part of the execinfrapb.DistSQLServer interface.
func (ds *ServerImpl) FlowStream(stream execinfrapb.DistSQL_FlowStreamServer) error {
	ctx := ds.AnnotateCtx(stream.Context())
	err := ds.flowStreamInt(ctx, stream)
	if err != nil && log.V(2) {
		// flowStreamInt may return an error during normal operation (e.g. a flow
		// was canceled as part of a graceful teardown). Log this error at the INFO
		// level behind a verbose flag for visibility.
		log.Infof(ctx, "%v", err)
	}
	return err
}

// lazyInternalExecutor is a tree.InternalExecutor that initializes
// itself only on the first call to QueryRow.
type lazyInternalExecutor struct {
	// Set when an internal executor has been initialized.
	sqlutil.InternalExecutor

	// Used for initializing the internal executor exactly once.
	once sync.Once

	// newInternalExecutor must be set when instantiating a lazyInternalExecutor,
	// it provides an internal executor to use when necessary.
	newInternalExecutor func() sqlutil.InternalExecutor
}

var _ sqlutil.InternalExecutor = &lazyInternalExecutor{}

func (ie *lazyInternalExecutor) QueryRowEx(
	ctx context.Context,
	opName string,
	txn *kv.Txn,
	opts sessiondata.InternalExecutorOverride,
	stmt string,
	qargs ...interface{},
) (tree.Datums, error) {
	ie.once.Do(func() {
		ie.InternalExecutor = ie.newInternalExecutor()
	})
	return ie.InternalExecutor.QueryRowEx(ctx, opName, txn, opts, stmt, qargs...)
}

func (ie *lazyInternalExecutor) QueryRow(
	ctx context.Context, opName string, txn *kv.Txn, stmt string, qargs ...interface{},
) (tree.Datums, error) {
	ie.once.Do(func() {
		ie.InternalExecutor = ie.newInternalExecutor()
	})
	return ie.InternalExecutor.QueryRow(ctx, opName, txn, stmt, qargs...)
}
