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
	"io"
	time "time"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// DistSQLVersion identifies DistSQL engine versions.
type DistSQLVersion uint32

// Version identifies the distsqlrun protocol version.
//
// This version is separate from the main CockroachDB version numbering; it is
// only changed when the distsqlrun API changes.
//
// The planner populates the version in SetupFlowRequest.
// A server only accepts requests with versions in the range MinAcceptedVersion
// to Version.
//
// Is is possible used to provide a "window" of compatibility when new features are
// added. Example:
//  - we start with Version=1; distsqlrun servers with version 1 only accept
//    requests with version 1.
//  - a new distsqlrun feature is added; Version is bumped to 2. The
//    planner does not yet use this feature by default; it still issues
//    requests with version 1.
//  - MinAcceptedVersion is still 1, i.e. servers with version 2
//    accept both versions 1 and 2.
//  - after an upgrade cycle, we can enable the feature in the planner,
//    requiring version 2.
//  - at some later point, we can choose to deprecate version 1 and have
//    servers only accept versions >= 2 (by setting
//    MinAcceptedVersion to 2).
//
// ATTENTION: When updating these fields, add to version_history.txt explaining
// what changed.
const Version DistSQLVersion = 11

// MinAcceptedVersion is the oldest version that the server is
// compatible with; see above.
const MinAcceptedVersion DistSQLVersion = 6

// minFlowDrainWait is the minimum amount of time a draining server allows for
// any incoming flows to be registered. It acts as a grace period in which the
// draining server waits for its gossiped draining state to be received by other
// nodes.
const minFlowDrainWait = 1 * time.Second

var settingUseTempStorageSorts = settings.RegisterBoolSetting(
	"sql.distsql.temp_storage.sorts",
	"set to true to enable use of disk for distributed sql sorts",
	true,
)

var settingUseTempStorageJoins = settings.RegisterBoolSetting(
	"sql.distsql.temp_storage.joins",
	"set to true to enable use of disk for distributed sql joins",
	true,
)

var settingWorkMemBytes = settings.RegisterByteSizeSetting(
	"sql.distsql.temp_storage.workmem",
	"maximum amount of memory in bytes a processor can use before falling back to temp storage",
	64*1024*1024, /* 64MB */
)

var noteworthyMemoryUsageBytes = envutil.EnvOrDefaultInt64("COCKROACH_NOTEWORTHY_DISTSQL_MEMORY_USAGE", 1024*1024 /* 1MB */)

// ServerConfig encompasses the configuration required to create a
// DistSQLServer.
type ServerConfig struct {
	log.AmbientContext

	Settings *cluster.Settings

	// DB is a handle to the cluster.
	DB       *client.DB
	Executor sqlutil.InternalExecutor

	// FlowDB is the DB that flows should use for interacting with the database.
	// This DB has to be set such that it bypasses the local TxnCoordSender. We
	// want only the TxnCoordSender on the gateway to be involved with requests
	// performed by DistSQL.
	FlowDB       *client.DB
	RPCContext   *rpc.Context
	Stopper      *stop.Stopper
	TestingKnobs TestingKnobs

	ParentMemoryMonitor *mon.BytesMonitor

	// TempStorage is used by some DistSQL processors to store rows when the
	// working set is larger than can be stored in memory.
	TempStorage engine.Engine
	// DiskMonitor is used to monitor temporary storage disk usage. Actual disk
	// space used will be a small multiple (~1.1) of this because of RocksDB
	// space amplification.
	DiskMonitor *mon.BytesMonitor

	Metrics *DistSQLMetrics

	// NodeID is the id of the node on which this Server is running.
	NodeID    *base.NodeIDContainer
	ClusterID uuid.UUID

	// JobRegistry manages jobs being used by this Server.
	JobRegistry *jobs.Registry

	// A handle to gossip used to broadcast the node's DistSQL version and
	// draining state.
	Gossip *gossip.Gossip
}

// ServerImpl implements the server for the distributed SQL APIs.
type ServerImpl struct {
	ServerConfig
	flowRegistry  *flowRegistry
	flowScheduler *flowScheduler
	memMonitor    mon.BytesMonitor
	regexpCache   *tree.RegexpCache
}

var _ DistSQLServer = &ServerImpl{}

// NewServer instantiates a DistSQLServer.
func NewServer(ctx context.Context, cfg ServerConfig) *ServerImpl {
	ds := &ServerImpl{
		ServerConfig:  cfg,
		regexpCache:   tree.NewRegexpCache(512),
		flowRegistry:  makeFlowRegistry(cfg.NodeID.Get()),
		flowScheduler: newFlowScheduler(cfg.AmbientContext, cfg.Stopper, cfg.Metrics),
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
		&DistSQLVersionGossipInfo{
			Version:            Version,
			MinAcceptedVersion: MinAcceptedVersion,
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
		&DistSQLDrainingInfo{
			Draining: drain,
		},
		0, // ttl - no expiration
	)
}

// FlowVerIsCompatible checks a flow's version is compatible with this node's
// DistSQL version.
func FlowVerIsCompatible(flowVer, minAcceptedVersion, serverVersion DistSQLVersion) bool {
	return flowVer >= minAcceptedVersion && flowVer <= serverVersion
}

// simpleCtxProvider always returns the context that it holds.
type simpleCtxProvider struct {
	ctx context.Context
}

func (s simpleCtxProvider) Ctx() context.Context {
	return s.ctx
}

// Note: unless an error is returned, the returned context contains a span that
// must be finished through Flow.Cleanup.
func (ds *ServerImpl) setupFlow(
	ctx context.Context,
	parentSpan opentracing.Span,
	req *SetupFlowRequest,
	syncFlowConsumer RowReceiver,
) (context.Context, *Flow, error) {
	if !FlowVerIsCompatible(req.Version, MinAcceptedVersion, Version) {
		err := errors.Errorf(
			"version mismatch in flow request: %d; this node accepts %d through %d",
			req.Version, MinAcceptedVersion, Version,
		)
		log.Warning(ctx, err)
		return ctx, nil, err
	}
	nodeID := ds.ServerConfig.NodeID.Get()
	if nodeID == 0 {
		return nil, nil, errors.Errorf("setupFlow called before the NodeID was resolved")
	}

	const opName = "flow"
	var sp opentracing.Span
	if parentSpan == nil {
		sp = ds.Tracer.StartSpan(opName)
	} else {
		// We use FollowsFrom because the flow's span outlives the SetupFlow request.
		sp = ds.Tracer.StartSpan(opName, opentracing.FollowsFrom(parentSpan.Context()))
	}
	ctx = opentracing.ContextWithSpan(ctx, sp)

	// The monitor and account opened here are closed in Flow.Cleanup().
	monitor := mon.MakeMonitor(
		"flow",
		mon.MemoryResource,
		ds.Metrics.CurBytesCount,
		ds.Metrics.MaxBytesHist,
		-1, /* use default block size */
		noteworthyMemoryUsageBytes,
		ds.Settings,
	)
	monitor.Start(ctx, &ds.memMonitor, mon.BoundAccount{})
	acc := monitor.MakeBoundAccount()

	var txn *client.Txn
	if req.Txn != nil {
		// The flow will run in a Txn that specifies child=true because we
		// do not want each distributed Txn to heartbeat the transaction.
		txn = client.NewTxnWithProto(ds.FlowDB, req.Flow.Gateway, client.LeafTxn, *req.Txn)
	}

	location, err := timeutil.TimeZoneStringToLocation(req.EvalContext.Location)
	if err != nil {
		tracing.FinishSpan(sp)
		return ctx, nil, err
	}
	evalCtx := tree.EvalContext{
		Settings: ds.ServerConfig.Settings,
		SessionData: &sessiondata.SessionData{
			Location:   location,
			Database:   req.EvalContext.Database,
			User:       req.EvalContext.User,
			SearchPath: sessiondata.MakeSearchPath(req.EvalContext.SearchPath),
		},
		ClusterID:    ds.ServerConfig.ClusterID,
		NodeID:       nodeID,
		ReCache:      ds.regexpCache,
		Mon:          &monitor,
		ActiveMemAcc: &acc,
		// TODO(andrei): This is wrong. Each processor should override Ctx with its
		// own context.
		CtxProvider: simpleCtxProvider{ctx: ctx},
		Txn:         txn,
		Planner:     &dummyEvalPlanner{},
		Sequence:    &dummySequenceOperators{},
	}
	evalCtx.SetStmtTimestamp(timeutil.Unix(0 /* sec */, req.EvalContext.StmtTimestampNanos))
	evalCtx.SetTxnTimestamp(timeutil.Unix(0 /* sec */, req.EvalContext.TxnTimestampNanos))
	evalCtx.SessionData.SequenceState = sessiondata.NewSequenceState()
	var haveSequences bool
	for _, seq := range req.EvalContext.SeqState.Seqs {
		evalCtx.SessionData.SequenceState.RecordValue(seq.SeqID, seq.LatestVal)
	}
	if haveSequences {
		evalCtx.SessionData.SequenceState.SetLastSequenceIncremented(
			*req.EvalContext.SeqState.LastSeqIncremented)
	}

	// TODO(radu): we should sanity check some of these fields (especially
	// txnProto).
	flowCtx := FlowCtx{
		Settings:       ds.Settings,
		AmbientContext: ds.AmbientContext,
		stopper:        ds.Stopper,
		id:             req.Flow.FlowID,
		EvalCtx:        evalCtx,
		rpcCtx:         ds.RPCContext,
		txn:            txn,
		clientDB:       ds.DB,
		executor:       ds.Executor,
		testingKnobs:   ds.TestingKnobs,
		nodeID:         nodeID,
		TempStorage:    ds.TempStorage,
		diskMonitor:    ds.DiskMonitor,
		JobRegistry:    ds.ServerConfig.JobRegistry,
	}

	ctx = flowCtx.AnnotateCtx(ctx)

	f := newFlow(flowCtx, ds.flowRegistry, syncFlowConsumer)
	flowCtx.AddLogTagStr("f", f.id.Short())
	if err := f.setup(ctx, &req.Flow); err != nil {
		log.Errorf(ctx, "error setting up flow: %s", err)
		tracing.FinishSpan(sp)
		ctx = opentracing.ContextWithSpan(ctx, nil)
		return ctx, nil, err
	}
	return ctx, f, nil
}

// SetupSyncFlow sets up a synchronous flow, connecting the sync response
// output stream to the given RowReceiver. The flow is not started. The flow
// will be associated with the given context.
// Note: the returned context contains a span that must be finished through
// Flow.Cleanup.
func (ds *ServerImpl) SetupSyncFlow(
	ctx context.Context, req *SetupFlowRequest, output RowReceiver,
) (context.Context, *Flow, error) {
	return ds.setupFlow(ds.AnnotateCtx(ctx), opentracing.SpanFromContext(ctx), req, output)
}

// RunSyncFlow is part of the DistSQLServer interface.
func (ds *ServerImpl) RunSyncFlow(stream DistSQL_RunSyncFlowServer) error {
	// Set up the outgoing mailbox for the stream.
	mbox := newOutboxSyncFlowStream(stream)

	firstMsg, err := stream.Recv()
	if err != nil {
		return err
	}
	if firstMsg.SetupFlowRequest == nil {
		return errors.Errorf("first message in RunSyncFlow doesn't contain SetupFlowRequest")
	}
	req := firstMsg.SetupFlowRequest
	ctx, f, err := ds.SetupSyncFlow(stream.Context(), req, mbox)
	if err != nil {
		return err
	}
	mbox.setFlowCtx(&f.FlowCtx)

	if err := ds.Stopper.RunTask(ctx, "distsqlrun.ServerImpl: sync flow", func(ctx context.Context) {
		ctx, ctxCancel := contextutil.WithCancel(ctx)
		defer ctxCancel()
		mbox.start(ctx, &f.waitGroup, ctxCancel)
		ds.Metrics.FlowStart()
		if err := f.Start(ctx, func() {}); err != nil {
			log.Fatalf(ctx, "unexpected error from syncFlow.Start(): %s "+
				"The error should have gone to the consumer.", err)
		}
		f.Wait()
		f.Cleanup(ctx)
		ds.Metrics.FlowStop()
	}); err != nil {
		return err
	}
	return mbox.err
}

// SetupFlow is part of the DistSQLServer interface.
func (ds *ServerImpl) SetupFlow(
	ctx context.Context, req *SetupFlowRequest,
) (*SimpleResponse, error) {
	parentSpan := opentracing.SpanFromContext(ctx)

	// Note: the passed context will be canceled when this RPC completes, so we
	// can't associate it with the flow.
	ctx = ds.AnnotateCtx(context.Background())
	ctx, f, err := ds.setupFlow(ctx, parentSpan, req, nil /* syncFlowConsumer */)
	if err == nil {
		err = ds.flowScheduler.ScheduleFlow(ctx, f)
	}
	if err != nil {
		// We return flow deployment errors in the response so that they are
		// packaged correctly over the wire. If we return them directly to this
		// function, they become part of an rpc error.
		return &SimpleResponse{Error: NewError(err)}, nil
	}
	return &SimpleResponse{}, nil
}

func (ds *ServerImpl) flowStreamInt(ctx context.Context, stream DistSQL_FlowStreamServer) error {
	// Receive the first message.
	msg, err := stream.Recv()
	if err != nil {
		if err == io.EOF {
			return errors.Errorf("missing header message")
		}
		return err
	}
	if msg.Header == nil {
		return errors.Errorf("no header in first message")
	}
	flowID := msg.Header.FlowID
	streamID := msg.Header.StreamID
	if log.V(1) {
		log.Infof(ctx, "connecting inbound stream %s/%d", flowID.Short(), streamID)
	}
	f, receiver, cleanup, err := ds.flowRegistry.ConnectInboundStream(
		ctx, flowID, streamID, stream, flowStreamDefaultTimeout)
	if err != nil {
		return err
	}
	defer cleanup()
	log.VEventf(ctx, 1, "connected inbound stream %s/%d", flowID.Short(), streamID)
	return ProcessInboundStream(f.AnnotateCtx(ctx), stream, msg, receiver, f)
}

// FlowStream is part of the DistSQLServer interface.
func (ds *ServerImpl) FlowStream(stream DistSQL_FlowStreamServer) error {
	ctx := ds.AnnotateCtx(stream.Context())
	err := ds.flowStreamInt(ctx, stream)
	if err != nil {
		log.Error(ctx, err)
	}
	return err
}

// TestingKnobs are the testing knobs.
type TestingKnobs struct {
	// RunBeforeBackfillChunk is called before executing each chunk of a
	// backfill during a schema change operation. It is called with the
	// current span and returns an error which eventually is returned to the
	// caller of SchemaChanger.exec(). It is called at the start of the
	// backfill function passed into the transaction executing the chunk.
	RunBeforeBackfillChunk func(sp roachpb.Span) error

	// RunAfterBackfillChunk is called after executing each chunk of a
	// backfill during a schema change operation. It is called just before
	// returning from the backfill function passed into the transaction
	// executing the chunk. It is always called even when the backfill
	// function returns an error, or if the table has already been dropped.
	RunAfterBackfillChunk func()

	// MemoryLimitBytes specifies a maximum amount of working memory that a
	// processor that supports falling back to disk can use. Must be >= 1 to
	// enable. Once this limit is hit, processors employ their on-disk
	// implementation regardless of applicable cluster settings.
	MemoryLimitBytes int64

	// DrainFast, if enabled, causes the server to not wait for any currently
	// running flows to complete or give a grace period of minFlowDrainWait
	// to incoming flows to register.
	DrainFast bool

	// MetadataTestLevel controls whether or not additional metadata test
	// processors are planned, which send additional "RowNum" metadata that is
	// checked by a test receiver on the gateway.
	MetadataTestLevel MetadataTestLevel
}

// MetadataTestLevel represents the types of queries where metadata test
// processors are planned.
type MetadataTestLevel int

const (
	// Off represents that no metadata test processors are planned.
	Off MetadataTestLevel = iota
	// NoExplain represents that metadata test processors are planned for all
	// queries except EXPLAIN (DISTSQL) statements.
	NoExplain
	// On represents that metadata test processors are planned for all queries.
	On
)

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*TestingKnobs) ModuleTestingKnobs() {}

var errEvalPlanner = errors.New("cannot backfill such evaluated expression")

// Implements the tree.EvalPlanner interface by returning errors.
type dummyEvalPlanner struct {
}

// Implements the tree.EvalPlanner interface.
func (ep *dummyEvalPlanner) QueryRow(
	ctx context.Context, sql string, args ...interface{},
) (tree.Datums, error) {
	return nil, errEvalPlanner
}

// Implements the tree.EvalDatabase interface.
func (ep *dummyEvalPlanner) ParseQualifiedTableName(
	ctx context.Context, sql string,
) (*tree.TableName, error) {
	return nil, errEvalPlanner
}

// Implements the tree.EvalDatabase interface.
func (ep *dummyEvalPlanner) ResolveTableName(ctx context.Context, tn *tree.TableName) error {
	return errEvalPlanner
}

// Implements the tree.EvalPlanner interface.
func (ep *dummyEvalPlanner) ParseType(sql string) (coltypes.CastTargetType, error) {
	return nil, errEvalPlanner
}

// Implements the tree.EvalPlanner interface.
func (ep *dummyEvalPlanner) EvalSubquery(expr *tree.Subquery) (tree.Datum, error) {
	return nil, errEvalPlanner
}

var errSequenceOperators = errors.New("cannot backfill such sequence operation")

// Implements the tree.SequenceOperators interface by returning errors.
type dummySequenceOperators struct {
}

// Implements the tree.EvalDatabase interface.
func (so *dummySequenceOperators) ParseQualifiedTableName(
	ctx context.Context, sql string,
) (*tree.TableName, error) {
	return nil, errSequenceOperators
}

// Implements the tree.EvalDatabase interface.
func (so *dummySequenceOperators) ResolveTableName(ctx context.Context, tn *tree.TableName) error {
	return errEvalPlanner
}

// Implements the tree.SequenceOperators interface.
func (so *dummySequenceOperators) IncrementSequence(
	ctx context.Context, seqName *tree.TableName,
) (int64, error) {
	return 0, errSequenceOperators
}

// Implements the tree.SequenceOperators interface.
func (so *dummySequenceOperators) GetLatestValueInSessionForSequence(
	ctx context.Context, seqName *tree.TableName,
) (int64, error) {
	return 0, errSequenceOperators
}

// Implements the tree.SequenceOperators interface.
func (so *dummySequenceOperators) SetSequenceValue(
	ctx context.Context, seqName *tree.TableName, newVal int64, isCalled bool,
) error {
	return errSequenceOperators
}
