// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package distsql

import (
	"context"
	"io"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catsessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/colflow"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execopnode"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/execversion"
	"github.com/cockroachdb/cockroach/pkg/sql/faketreeeval"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/rowflow"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/pprofutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tochar"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/grpcinterceptor"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
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

// ServerImpl implements the server for the distributed SQL APIs.
type ServerImpl struct {
	execinfra.ServerConfig
	flowRegistry      *flowinfra.FlowRegistry
	remoteFlowRunner  *flowinfra.RemoteFlowRunner
	memMonitor        *mon.BytesMonitor
	regexpCache       *tree.RegexpCache
	toCharFormatCache *tochar.FormatCache
}

var _ execinfrapb.DistSQLServer = &ServerImpl{}

// NewServer instantiates a DistSQLServer.
func NewServer(
	ctx context.Context, cfg execinfra.ServerConfig, remoteFlowRunner *flowinfra.RemoteFlowRunner,
) *ServerImpl {
	ds := &ServerImpl{
		ServerConfig:      cfg,
		regexpCache:       tree.NewRegexpCache(512),
		toCharFormatCache: tochar.NewFormatCache(512),
		flowRegistry:      flowinfra.NewFlowRegistry(),
		remoteFlowRunner:  remoteFlowRunner,
		memMonitor: mon.NewMonitor(mon.Options{
			Name: mon.MakeMonitorName("distsql"),
			// Note that we don't use 'sql.mem.distsql.*' metrics here since
			// that would double count them with the 'flow' monitor in
			// setupFlow.
			CurCount:   nil,
			MaxHist:    nil,
			Settings:   cfg.Settings,
			LongLiving: true,
		}),
	}
	ds.memMonitor.StartNoReserved(ctx, cfg.ParentMemoryMonitor)
	// We have to initialize the flow runner at the same time we're creating
	// the DistSQLServer because the latter will be registered as a gRPC service
	// right away, so the RPCs might start coming in pretty much right after the
	// current method returns. See #66330.
	ds.remoteFlowRunner.Init(ds.Metrics)

	return ds
}

// Start launches workers for the server.
//
// Note that the initialization of the server required for performing the
// incoming RPCs needs to go into NewServer above because once that method
// returns, the server is registered as a gRPC service and needs to be fully
// initialized. For example, the initialization of the flow runner has to
// happen in NewServer.
func (ds *ServerImpl) Start() {
	if err := ds.setDraining(false); err != nil {
		panic(err)
	}
}

// NumRemoteRunningFlows returns the number of remote flows currently running on
// this server.
func (ds *ServerImpl) NumRemoteRunningFlows() int {
	return ds.remoteFlowRunner.NumRunningFlows()
}

// Drain changes the node's draining state through gossip and drains the
// server's flowRegistry. See flowRegistry.Drain for more details.
func (ds *ServerImpl) Drain(
	ctx context.Context, flowDrainWait time.Duration, reporter func(int, redact.SafeString),
) {
	if err := ds.setDraining(true); err != nil {
		log.Warningf(ctx, "unable to gossip distsql draining state: %v", err)
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
			gossip.MakeDistSQLDrainingKey(base.SQLInstanceID(nodeID)),
			&execinfrapb.DistSQLDrainingInfo{
				Draining: drain,
			},
			0, // ttl - no expiration
		)
	}
	return nil
}

// setupFlow creates a Flow.
//
//   - reserved: specifies the upfront memory reservation that the flow takes
//     ownership of. This account is already closed if an error is returned or
//     will be closed through Flow.Cleanup.
//
//   - localState: specifies if the flow runs entirely on this node and, if it
//     does, specifies the txn and other attributes.
//
// Note: unless an error is returned, the returned context contains a span that
// must be finished through Flow.Cleanup.
func (ds *ServerImpl) setupFlow(
	ctx context.Context,
	parentSpan *tracing.Span,
	parentMonitor *mon.BytesMonitor,
	reserved *mon.BoundAccount,
	req *execinfrapb.SetupFlowRequest,
	rowSyncFlowConsumer execinfra.RowReceiver,
	batchSyncFlowConsumer execinfra.BatchReceiver,
	localState LocalState,
) (retCtx context.Context, _ flowinfra.Flow, _ execopnode.OpChains, retErr error) {
	var sp *tracing.Span                       // will be Finish()ed by Flow.Cleanup()
	var monitor, diskMonitor *mon.BytesMonitor // will be closed in Flow.Cleanup()
	var onFlowCleanupEnd func(context.Context) // will be called at the very end of Flow.Cleanup()
	// Make sure that we clean up all resources (which in the happy case are
	// cleaned up in Flow.Cleanup()) if an error is encountered.
	defer func() {
		if retErr != nil {
			if monitor != nil {
				monitor.Stop(ctx)
			}
			if diskMonitor != nil {
				diskMonitor.Stop(ctx)
			}
			if onFlowCleanupEnd != nil {
				onFlowCleanupEnd(ctx)
			} else {
				reserved.Close(ctx)
			}
			// We finish the span after performing other cleanup in case that
			// cleanup accesses the context with the span.
			if sp != nil {
				sp.Finish()
			}
			retCtx = tracing.ContextWithSpan(ctx, nil)
		}
	}()

	if req.Version < execversion.MinAccepted || req.Version > execversion.Latest {
		err := errors.Errorf(
			"version mismatch in flow request: %d; this node accepts %d through %d",
			req.Version, execversion.MinAccepted, execversion.Latest,
		)
		log.Warningf(ctx, "%v", err)
		return ctx, nil, nil, err
	}
	ctx = execversion.WithVersion(ctx, req.Version)

	const opName = "flow"
	if parentSpan == nil {
		ctx, sp = ds.Tracer.StartSpanCtx(ctx, opName)
	} else if localState.IsLocal {
		// If we're a local flow, we don't need a "follows from" relationship: we're
		// going to run this flow synchronously.
		// TODO(andrei): localState.IsLocal is not quite the right thing to use.
		//  If that field is unset, we might still want to create a child span if
		//  this flow is run synchronously.
		ctx, sp = ds.Tracer.StartSpanCtx(ctx, opName, tracing.WithParent(parentSpan))
	} else {
		// We use FollowsFrom because the flow's span outlives the SetupFlow request.
		ctx, sp = ds.Tracer.StartSpanCtx(
			ctx,
			opName,
			tracing.WithParent(parentSpan),
			tracing.WithFollowsFrom(),
		)
	}

	monitor = mon.NewMonitor(mon.Options{
		Name:     mon.MakeMonitorNameWithID("flow ", req.Flow.FlowID.Short()),
		CurCount: ds.Metrics.CurBytesCount,
		MaxHist:  ds.Metrics.MaxBytesHist,
		Settings: ds.Settings,
	})
	monitor.Start(ctx, parentMonitor, reserved)
	diskMonitor = execinfra.NewMonitor(ctx, ds.ParentDiskMonitor, "flow-disk-monitor")

	makeLeaf := func(ctx context.Context) (*kv.Txn, error) {
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
		return kv.NewLeafTxn(ctx, ds.DB.KV(), roachpb.NodeID(req.Flow.Gateway), tis), nil
	}

	var evalCtx *eval.Context
	var leafTxn *kv.Txn
	if localState.EvalContext != nil {
		// If we're running on the gateway, then we'll reuse already existing
		// eval context. This is the case even if the query is distributed -
		// this allows us to avoid an unnecessary deserialization of the eval
		// context proto.
		evalCtx = localState.EvalContext
		// We create an eval context variable scoped to this block and reference
		// it in the onFlowCleanupEnd closure. If the closure referenced
		// evalCtx, then the pointer would be heap allocated because it is
		// modified in the other branch of the conditional, and Go's escape
		// analysis cannot determine that the capture and modification are
		// mutually exclusive.
		localEvalCtx := evalCtx
		// We're about to mutate the evalCtx and we want to restore its original
		// state once the flow cleans up. Note that we could have made a copy of
		// the whole evalContext, but that isn't free, so we choose to restore
		// the original state in order to avoid performance regressions.
		origTxn := localEvalCtx.Txn
		onFlowCleanupEnd = func(ctx context.Context) {
			localEvalCtx.Txn = origTxn
			reserved.Close(ctx)
		}
		if localState.MustUseLeafTxn() {
			var err error
			leafTxn, err = makeLeaf(ctx)
			if err != nil {
				return nil, nil, nil, err
			}
			// Update the Txn field early (before f.SetTxn() below) since some
			// processors capture the field in their constructor (see #41992).
			localEvalCtx.Txn = leafTxn
		}
	} else {
		onFlowCleanupEnd = func(ctx context.Context) {
			reserved.Close(ctx)
		}
		if localState.IsLocal {
			return nil, nil, nil, errors.AssertionFailedf(
				"EvalContext expected to be populated when IsLocal is set")
		}

		sd, err := sessiondata.UnmarshalNonLocal(req.EvalContext.SessionData)
		if err != nil {
			return ctx, nil, nil, err
		}

		// It's important to populate evalCtx.Txn early. We'll write it again in the
		// f.SetTxn() call below, but by then it will already have been captured by
		// processors.
		leafTxn, err = makeLeaf(ctx)
		if err != nil {
			return nil, nil, nil, err
		}
		evalCtx = &eval.Context{
			Settings:                  ds.ServerConfig.Settings,
			SessionDataStack:          sessiondata.NewStack(sd),
			ClusterID:                 ds.ServerConfig.LogicalClusterID.Get(),
			ClusterName:               ds.ServerConfig.ClusterName,
			NodeID:                    ds.ServerConfig.NodeID,
			Codec:                     ds.ServerConfig.Codec,
			ReCache:                   ds.regexpCache,
			ToCharFormatCache:         ds.toCharFormatCache,
			Locality:                  ds.ServerConfig.Locality,
			OriginalLocality:          ds.ServerConfig.Locality,
			Tracer:                    ds.ServerConfig.Tracer,
			Planner:                   &faketreeeval.DummyEvalPlanner{Monitor: monitor},
			StreamManagerFactory:      &faketreeeval.DummyStreamManagerFactory{},
			PrivilegedAccessor:        &faketreeeval.DummyPrivilegedAccessor{},
			SessionAccessor:           &faketreeeval.DummySessionAccessor{},
			ClientNoticeSender:        &faketreeeval.DummyClientNoticeSender{},
			Sequence:                  &faketreeeval.DummySequenceOperators{},
			Tenant:                    &faketreeeval.DummyTenantOperator{},
			Regions:                   &faketreeeval.DummyRegionOperator{},
			Txn:                       leafTxn,
			SQLLivenessReader:         ds.ServerConfig.SQLLivenessReader,
			SQLStatsController:        ds.ServerConfig.SQLStatsController,
			SchemaTelemetryController: ds.ServerConfig.SchemaTelemetryController,
			IndexUsageStatsController: ds.ServerConfig.IndexUsageStatsController,
			RangeStatsFetcher:         ds.ServerConfig.RangeStatsFetcher,
		}
		evalCtx.SetStmtTimestamp(timeutil.Unix(0 /* sec */, req.EvalContext.StmtTimestampNanos))
		evalCtx.SetTxnTimestamp(timeutil.Unix(0 /* sec */, req.EvalContext.TxnTimestampNanos))
	}

	// Create the FlowCtx for the flow.
	flowCtx := ds.newFlowContext(
		ctx, req.Flow.FlowID, evalCtx, monitor, diskMonitor, makeLeaf, req.TraceKV,
		req.CollectStats, localState, req.Flow.Gateway == ds.NodeID.SQLInstanceID(),
	)

	// req always contains the desired vectorize mode, regardless of whether we
	// have non-nil localState.EvalContext. We don't want to update EvalContext
	// itself when the vectorize mode needs to be changed because we would need
	// to restore the original value which can have data races under stress.
	isVectorized := req.EvalContext.SessionData.VectorizeMode != sessiondatapb.VectorizeOff
	f := newFlow(
		flowCtx, sp, ds.flowRegistry, rowSyncFlowConsumer, batchSyncFlowConsumer,
		localState.LocalProcs, localState.LocalVectorSources, isVectorized,
		onFlowCleanupEnd, req.StatementSQL,
	)
	opt := flowinfra.FuseNormally
	if !localState.MustUseLeafTxn() {
		// If there are no remote flows and the local flow doesn't have any
		// concurrency, fuse everything. This is needed in order for us to be
		// able to use the RootTxn for the flow 's execution; the RootTxn
		// doesn't allow for concurrent operations. Local flows with mutations
		// need to use the RootTxn.
		opt = flowinfra.FuseAggressively
	}

	if !f.IsLocal() {
		bld := logtags.BuildBuffer()
		bld.Add("f", flowCtx.ID.Short().String())
		if req.JobTag != "" {
			bld.Add("job", req.JobTag)
		}
		if req.StatementSQL != "" {
			bld.Add("distsql.stmt", req.StatementSQL)
		}
		bld.Add("distsql.gateway", req.Flow.Gateway)
		if req.EvalContext.SessionData.ApplicationName != "" {
			bld.Add("distsql.appname", req.EvalContext.SessionData.ApplicationName)
		}
		if leafTxn != nil {
			// TODO(radu): boxing the UUID requires an allocation.
			bld.Add("distsql.txn", leafTxn.ID())
		}
		flowCtx.AmbientContext.AddLogTags(bld.Finish())
		ctx = flowCtx.AmbientContext.AnnotateCtx(ctx)
		telemetry.Inc(sqltelemetry.DistSQLExecCounter)
	}

	var opChains execopnode.OpChains
	var err error
	ctx, opChains, err = f.Setup(ctx, &req.Flow, opt)
	if err != nil {
		log.Errorf(ctx, "error setting up flow: %s", err)
		return ctx, nil, nil, err
	}
	if isVectorized {
		telemetry.Inc(sqltelemetry.VecExecCounter)
	}

	// Figure out what txn the flow needs to run in, if any. For gateway flows
	// that have no remote flows and also no concurrency, the (root) txn comes
	// from localState.Txn if we haven't already created a leaf txn. Otherwise,
	// we create, if necessary, a txn based on the request's LeafTxnInputState.
	var txn *kv.Txn
	if localState.IsLocal && !f.ConcurrentTxnUse() && leafTxn == nil {
		txn = localState.Txn
	} else {
		// If I haven't created the leaf already, do it now.
		if leafTxn == nil {
			leafTxn, err = makeLeaf(ctx)
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
	evalCtx *eval.Context,
	monitor, diskMonitor *mon.BytesMonitor,
	makeLeafTxn func(context.Context) (*kv.Txn, error),
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
		Mon:            monitor,
		Txn:            evalCtx.Txn,
		MakeLeafTxn:    makeLeafTxn,
		NodeID:         ds.ServerConfig.NodeID,
		TraceKV:        traceKV,
		CollectStats:   collectStats,
		Local:          localState.IsLocal,
		Gateway:        isGatewayNode,
		DiskMonitor:    diskMonitor,
	}

	if localState.IsLocal && localState.Collection != nil {
		// If we were passed a descs.Collection to use, then take it. In this
		// case, the caller will handle releasing the used descriptors, so we
		// don't need to clean up the descriptors when cleaning up the flow.
		flowCtx.Descriptors = localState.Collection
	} else {
		// If we weren't passed a descs.Collection, then make a new one. We are
		// responsible for cleaning it up and releasing any accessed descriptors
		// on flow cleanup.
		dsdp := catsessiondata.NewDescriptorSessionDataStackProvider(evalCtx.SessionDataStack)
		flowCtx.Descriptors = ds.CollectionFactory.NewCollection(
			ctx, descs.WithDescriptorSessionDataProvider(dsdp),
		)
		flowCtx.IsDescriptorsCleanupRequired = true
		flowCtx.EvalCatalogBuiltins.Init(evalCtx.Codec, evalCtx.Txn, flowCtx.Descriptors)
		evalCtx.CatalogBuiltins = &flowCtx.EvalCatalogBuiltins
	}
	return flowCtx
}

func newFlow(
	flowCtx execinfra.FlowCtx,
	sp *tracing.Span,
	flowReg *flowinfra.FlowRegistry,
	rowSyncFlowConsumer execinfra.RowReceiver,
	batchSyncFlowConsumer execinfra.BatchReceiver,
	localProcessors []execinfra.LocalProcessor,
	localVectorSources map[int32]any,
	isVectorized bool,
	onFlowCleanupEnd func(context.Context),
	statementSQL string,
) flowinfra.Flow {
	base := flowinfra.NewFlowBase(flowCtx, sp, flowReg, rowSyncFlowConsumer, batchSyncFlowConsumer, localProcessors, localVectorSources, onFlowCleanupEnd, statementSQL)
	if isVectorized {
		return colflow.NewVectorizedFlow(base)
	}
	return rowflow.NewRowBasedFlow(base)
}

// LocalState carries information that is required to set up a flow with wrapped
// planNodes.
type LocalState struct {
	EvalContext *eval.Context

	// Collection is set if this flow is running on the gateway as part of user
	// SQL session. It is the current descs.Collection of the planner executing
	// the flow.
	Collection *descs.Collection

	// IsLocal is set if the flow is running on the gateway and there are no
	// remote flows.
	IsLocal bool

	// HasConcurrency indicates whether the local flow uses multiple goroutines.
	HasConcurrency bool

	// MustUseLeaf indicates whether the local flow must use the LeafTxn even if
	// there is no concurrency in the flow on its own because there would be
	// concurrency with other flows which prohibits the usage of the RootTxn.
	MustUseLeaf bool

	// Txn is filled in on the gateway only. It is the RootTxn that the query is running in.
	// This will be used directly by the flow if the flow has no concurrency and IsLocal is set.
	// If there is concurrency, a LeafTxn will be created.
	Txn *kv.Txn

	// LocalProcs is an array of planNodeToRowSource processors. It's in order and
	// will be indexed into by the RowSourceIdx field in LocalPlanNodeSpec.
	LocalProcs []execinfra.LocalProcessor

	// LocalVectorSources is a map of local vector sources for Insert operator
	// mapping to coldata.Batch, use any to avoid injecting new
	// dependencies.
	LocalVectorSources map[int32]any
}

// MustUseLeafTxn returns true if a LeafTxn must be used. It is valid to call
// this method only after IsLocal and HasConcurrency have been set correctly.
func (l LocalState) MustUseLeafTxn() bool {
	return !l.IsLocal || l.HasConcurrency || l.MustUseLeaf
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
) (context.Context, flowinfra.Flow, execopnode.OpChains, error) {
	return ds.setupFlow(
		ctx, tracing.SpanFromContext(ctx), parentMonitor, &mon.BoundAccount{}, /* reserved */
		req, output, batchOutput, localState,
	)
}

// setupSpanForIncomingRPC creates a span for a SetupFlow RPC. The caller must
// finish the returned span.
//
// For most other RPCs, there's a gRPC server interceptor that opens spans based
// on trace info passed as gRPC metadata. But the SetupFlow RPC is common and so
// we have a more efficient implementation based on tracing information being
// passed in the request proto.
func (ds *ServerImpl) setupSpanForIncomingRPC(
	ctx context.Context, req *execinfrapb.SetupFlowRequest,
) (context.Context, *tracing.Span) {
	tr := ds.ServerConfig.AmbientContext.Tracer
	parentSpan := tracing.SpanFromContext(ctx)
	if parentSpan != nil {
		// It's not expected to have a span in the context since the gRPC server
		// interceptor that generally opens spans exempts this particular RPC. Note
		// that this method is not called for flows local to the gateway.
		return tr.StartSpanCtx(ctx, grpcinterceptor.SetupFlowMethodName,
			tracing.WithParent(parentSpan),
			tracing.WithServerSpanKind)
	}

	if !req.TraceInfo.Empty() {
		return tr.StartSpanCtx(ctx, grpcinterceptor.SetupFlowMethodName,
			tracing.WithRemoteParentFromTraceInfo(req.TraceInfo),
			tracing.WithServerSpanKind)
	}
	// For backwards compatibility with 21.2, if tracing info was passed as
	// gRPC metadata, we use it.
	remoteParent, err := grpcinterceptor.ExtractSpanMetaFromGRPCCtx(ctx, tr)
	if err != nil {
		log.Warningf(ctx, "error extracting tracing info from gRPC: %s", err)
	}
	return tr.StartSpanCtx(ctx, grpcinterceptor.SetupFlowMethodName,
		tracing.WithRemoteParentFromSpanMeta(remoteParent),
		tracing.WithServerSpanKind)
}

// SetupFlow is part of the execinfrapb.DistSQLServer interface.
func (ds *ServerImpl) SetupFlow(
	ctx context.Context, req *execinfrapb.SetupFlowRequest,
) (*execinfrapb.SimpleResponse, error) {
	log.VEventf(ctx, 1, "received SetupFlow request from n%v for flow %v", req.Flow.Gateway, req.Flow.FlowID)
	_, rpcSpan := ds.setupSpanForIncomingRPC(ctx, req)
	defer rpcSpan.Finish()

	rpcCtx := ctx
	// Note: the passed context will be canceled when this RPC completes, so we
	// can't associate it with the flow since it outlives the RPC.
	ctx = ds.AnnotateCtx(context.Background())
	if err := func() error {
		// Reserve some memory for this remote flow which is a poor man's
		// admission control based on the RAM usage.
		reserved := ds.memMonitor.MakeBoundAccount()
		err := reserved.Grow(ctx, mon.DefaultPoolAllocationSize)
		if err != nil {
			return err
		}
		var f flowinfra.Flow
		ctx, f, _, err = ds.setupFlow(
			ctx, rpcSpan, ds.memMonitor, &reserved, req, nil, /* rowSyncFlowConsumer */
			nil /* batchSyncFlowConsumer */, LocalState{},
		)
		// Check whether the RPC context has been canceled indicating that we
		// actually don't need to run this flow. This can happen when the
		// context is canceled after Dial() but before issuing SetupFlow RPC in
		// runnerRequest.run().
		if err == nil {
			err = rpcCtx.Err()
		}
		if err != nil {
			// Make sure to clean up the flow if it was created.
			if f != nil {
				f.Cleanup(ctx)
			}
			return err
		}
		var undo func()
		ctx, undo = pprofutil.SetProfilerLabelsFromCtxTags(ctx)
		defer undo()
		if cb := ds.TestingKnobs.SetupFlowCb; cb != nil {
			if err = cb(ctx, ds.ServerConfig.NodeID.SQLInstanceID(), req); err != nil {
				f.Cleanup(ctx)
				return err
			}
		}
		return ds.remoteFlowRunner.RunFlow(ctx, f)
	}(); err != nil {
		// We return flow deployment errors in the response so that they are
		// packaged correctly over the wire. If we return them directly to this
		// function, they become part of an rpc error.
		return &execinfrapb.SimpleResponse{Error: execinfrapb.NewError(ctx, err)}, nil
	}
	return &execinfrapb.SimpleResponse{}, nil
}

// CancelDeadFlows is part of the execinfrapb.DistSQLServer interface.
func (ds *ServerImpl) CancelDeadFlows(
	ctx context.Context, req *execinfrapb.CancelDeadFlowsRequest,
) (*execinfrapb.SimpleResponse, error) {
	ctx = ds.AnnotateCtx(ctx)
	ds.remoteFlowRunner.CancelDeadFlows(ctx, req)
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
		log.VEventf(ctx, 2, "FlowStream (server) error while receiving header: %v", err)
		return err
	}
	if msg.Header == nil {
		return errors.AssertionFailedf("no header in first message")
	}
	log.VEvent(ctx, 2, "FlowStream (server) received header")
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
	ctx = f.AmbientContext.AnnotateCtx(ctx)
	ctx, undo := pprofutil.SetProfilerLabelsFromCtxTags(ctx)
	defer undo()
	return streamStrategy.Run(ctx, stream, msg, f)
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
