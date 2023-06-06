// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/rsa"
	"crypto/x509/pkix"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/keyvisstorage"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangestats"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/diagnostics/diagnosticspb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/contention"
	"github.com/cockroachdb/cockroach/pkg/sql/contentionpb"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/insights"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	gwruntime "github.com/grpc-ecosystem/grpc-gateway/runtime"
	raft "go.etcd.io/raft/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// Default Maximum number of log entries returned.
	defaultMaxLogEntries = 1000

	// statusPrefix is the root of the cluster statistics and metrics API.
	statusPrefix = "/_status/"

	// statusVars exposes prometheus metrics for monitoring consumption.
	statusVars = statusPrefix + "vars"

	// loadStatusVars exposes prometheus metrics for instant monitoring of CPU load.
	loadStatusVars = statusPrefix + "load"

	// raftStateDormant is used when there is no known raft state.
	raftStateDormant = "StateDormant"

	// maxConcurrentRequests is the maximum number of RPC fan-out requests
	// that will be made at any point of time.
	maxConcurrentRequests = 100

	// maxConcurrentPaginatedRequests is the maximum number of RPC fan-out
	// requests that will be made at any point of time for a row-limited /
	// paginated request. This should be much lower than maxConcurrentRequests
	// as too much concurrency here can result in wasted results.
	maxConcurrentPaginatedRequests = 4
)

var (
	// Pattern for local used when determining the node ID.
	localRE = regexp.MustCompile(`(?i)local`)

	// Counter to count accesses to the prometheus vars endpoint /_status/vars .
	telemetryPrometheusVars = telemetry.GetCounterOnce("monitoring.prometheus.vars")

	// Counter to count accesses to the health check endpoint /health .
	telemetryHealthCheck = telemetry.GetCounterOnce("monitoring.health.details")
)

type metricMarshaler interface {
	json.Marshaler
	PrintAsText(io.Writer) error
	ScrapeIntoPrometheus(pm *metric.PrometheusExporter)
}

// baseStatusServer implements functionality shared by the tenantStatusServer
// and the full statusServer.
type baseStatusServer struct {
	// Embedding the UnimplementedStatusServer lets us easily support
	// treating the tenantStatusServer as implementing the StatusServer
	// interface. We'd return an unimplemented error for the methods we
	// didn't require anyway.
	serverpb.UnimplementedStatusServer

	log.AmbientContext
	privilegeChecker   *adminPrivilegeChecker
	sessionRegistry    *sql.SessionRegistry
	closedSessionCache *sql.ClosedSessionCache
	remoteFlowRunner   *flowinfra.RemoteFlowRunner
	st                 *cluster.Settings
	sqlServer          *SQLServer
	rpcCtx             *rpc.Context
	stopper            *stop.Stopper
	serverIterator     ServerIterator
	clock              *hlc.Clock
}

func isInternalAppName(app string) bool {
	return strings.HasPrefix(app, catconstants.InternalAppNamePrefix)
}

// getLocalSessions returns a list of local sessions on this node. Note that the
// NodeID field is unset.
func (b *baseStatusServer) getLocalSessions(
	ctx context.Context, req *serverpb.ListSessionsRequest,
) ([]serverpb.Session, error) {
	ctx = forwardSQLIdentityThroughRPCCalls(ctx)
	ctx = b.AnnotateCtx(ctx)

	sessionUser, isAdmin, err := b.privilegeChecker.getUserAndRole(ctx)
	if err != nil {
		return nil, serverError(ctx, err)
	}

	hasViewActivityRedacted, err := b.privilegeChecker.hasRoleOption(ctx, sessionUser, roleoption.VIEWACTIVITYREDACTED)
	if err != nil {
		return nil, serverError(ctx, err)
	}

	hasViewActivity, err := b.privilegeChecker.hasRoleOption(ctx, sessionUser, roleoption.VIEWACTIVITY)
	if err != nil {
		return nil, serverError(ctx, err)
	}

	reqUsername, err := username.MakeSQLUsernameFromPreNormalizedStringChecked(req.Username)
	if err != nil {
		return nil, serverError(ctx, err)
	}

	if !isAdmin && !hasViewActivity && !hasViewActivityRedacted {
		// For non-superusers, requests with an empty username is
		// implicitly a request for the client's own sessions.
		if reqUsername.Undefined() {
			reqUsername = sessionUser
		}

		// Non-superusers are not allowed to query sessions others than their own.
		if sessionUser != reqUsername {
			return nil, status.Errorf(
				codes.PermissionDenied,
				"client user %q does not have permission to view sessions from user %q",
				sessionUser, reqUsername)
		}
	}

	// The empty username means "all sessions".
	showAll := reqUsername.Undefined()
	showInternal := SQLStatsShowInternal.Get(&b.st.SV) || req.IncludeInternal

	sessions := b.sessionRegistry.SerializeAll()
	var closedSessions []serverpb.Session
	var closedSessionIDs map[uint128.Uint128]struct{}
	if !req.ExcludeClosedSessions {
		closedSessions = b.closedSessionCache.GetSerializedSessions()
		closedSessionIDs = make(map[uint128.Uint128]struct{}, len(closedSessions))
		for _, closedSession := range closedSessions {
			closedSessionIDs[uint128.FromBytes(closedSession.ID)] = struct{}{}
		}
	}

	reqUserNameNormalized := reqUsername.Normalized()

	userSessions := make([]serverpb.Session, 0, len(sessions)+len(closedSessions))
	addUserSession := func(session serverpb.Session) {
		// We filter based on the session name instead of the executor type because we
		// may want to surface certain internal sessions, such as those executed by
		// the SQL over HTTP api, as non-internal.
		if (reqUserNameNormalized != session.Username && !showAll) ||
			(!showInternal && isInternalAppName(session.ApplicationName)) {
			return
		}

		if !isAdmin && hasViewActivityRedacted && (reqUserNameNormalized != session.Username) {
			// Remove queries with constants if user doesn't have correct privileges.
			// Note that users can have both VIEWACTIVITYREDACTED and VIEWACTIVITY,
			// with the former taking precedence.
			for idx := range session.ActiveQueries {
				session.ActiveQueries[idx].Sql = session.ActiveQueries[idx].SqlNoConstants
			}
			session.LastActiveQuery = session.LastActiveQueryNoConstants
		}
		userSessions = append(userSessions, session)
	}
	for _, session := range sessions {
		// The same session can appear as both open and closed because reading the
		// open and closed sessions is not synchronized. Prefer the closed session
		// over the open one if the same session appears as both because it was
		// closed in between reading the open sessions and reading the closed ones.
		_, ok := closedSessionIDs[uint128.FromBytes(session.ID)]
		if ok {
			continue
		}
		addUserSession(session)
	}
	for _, session := range closedSessions {
		addUserSession(session)
	}

	sort.Slice(userSessions, func(i, j int) bool {
		return userSessions[i].Start.Before(userSessions[j].Start)
	})

	return userSessions, nil
}

// checkCancelPrivilege returns nil if the user has the necessary cancel action
// privileges for a session. This function returns a proper gRPC error status.
func (b *baseStatusServer) checkCancelPrivilege(
	ctx context.Context, reqUsername username.SQLUsername, sessionUsername username.SQLUsername,
) error {
	ctx = forwardSQLIdentityThroughRPCCalls(ctx)
	ctx = b.AnnotateCtx(ctx)

	ctxUsername, isCtxAdmin, err := b.privilegeChecker.getUserAndRole(ctx)
	if err != nil {
		return serverError(ctx, err)
	}
	if reqUsername.Undefined() {
		reqUsername = ctxUsername
	} else if reqUsername != ctxUsername && !isCtxAdmin {
		// When CANCEL QUERY is run as a SQL statement, sessionUser is always root
		// and the user who ran the statement is passed as req.Username.
		return errRequiresAdmin
	}

	// A user can always cancel their own sessions/queries.
	if sessionUsername == reqUsername {
		return nil
	}

	// If reqUsername equals ctxUsername then isReqAdmin is isCtxAdmin and was
	// checked inside getUserAndRole above.
	isReqAdmin := isCtxAdmin
	if reqUsername != ctxUsername {
		isReqAdmin, err = b.privilegeChecker.hasAdminRole(ctx, reqUsername)
		if err != nil {
			return serverError(ctx, err)
		}
	}

	// Admin users can cancel sessions/queries.
	if isReqAdmin {
		return nil
	}

	// Must have CANCELQUERY privilege to cancel other users'
	// sessions/queries.
	hasGlobalCancelQuery, err := b.privilegeChecker.hasGlobalPrivilege(ctx, reqUsername, privilege.CANCELQUERY)
	if err != nil {
		return serverError(ctx, err)
	}
	if !hasGlobalCancelQuery {
		hasRoleCancelQuery, err := b.privilegeChecker.hasRoleOption(ctx, reqUsername, roleoption.CANCELQUERY)
		if err != nil {
			return serverError(ctx, err)
		}
		if !hasRoleCancelQuery {
			return errRequiresRoleOption(roleoption.CANCELQUERY)
		}
	}

	// Non-admins cannot cancel admins' sessions/queries.
	isSessionAdmin, err := b.privilegeChecker.hasAdminRole(ctx, sessionUsername)
	if err != nil {
		return serverError(ctx, err)
	}
	if isSessionAdmin {
		return status.Error(
			codes.PermissionDenied, "permission denied to cancel admin session")
	}

	return nil
}

// ListLocalContentionEvents returns a list of contention events on this node.
func (b *baseStatusServer) ListLocalContentionEvents(
	ctx context.Context, _ *serverpb.ListContentionEventsRequest,
) (*serverpb.ListContentionEventsResponse, error) {
	ctx = forwardSQLIdentityThroughRPCCalls(ctx)
	ctx = b.AnnotateCtx(ctx)

	if err := b.privilegeChecker.requireViewActivityOrViewActivityRedactedPermission(ctx); err != nil {
		// NB: not using serverError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	return &serverpb.ListContentionEventsResponse{
		Events: b.sqlServer.execCfg.ContentionRegistry.Serialize(),
	}, nil
}

func (b *baseStatusServer) ListLocalDistSQLFlows(
	ctx context.Context, _ *serverpb.ListDistSQLFlowsRequest,
) (*serverpb.ListDistSQLFlowsResponse, error) {
	ctx = forwardSQLIdentityThroughRPCCalls(ctx)
	ctx = b.AnnotateCtx(ctx)

	if err := b.privilegeChecker.requireViewActivityOrViewActivityRedactedPermission(ctx); err != nil {
		// NB: not using serverError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	nodeIDOrZero, _ := b.sqlServer.sqlIDContainer.OptionalNodeID()

	flows := b.remoteFlowRunner.Serialize()
	response := &serverpb.ListDistSQLFlowsResponse{
		Flows: make([]serverpb.DistSQLRemoteFlows, 0, len(flows)),
	}
	for _, f := range flows {
		response.Flows = append(response.Flows, serverpb.DistSQLRemoteFlows{
			FlowID: f.FlowID,
			Infos: []serverpb.DistSQLRemoteFlows_Info{{
				NodeID:    nodeIDOrZero,
				Timestamp: f.Timestamp,
				Status:    serverpb.DistSQLRemoteFlows_RUNNING,
				Stmt:      f.StatementSQL,
			}},
		})
	}
	// Per the contract of serverpb.ListDistSQLFlowsResponse, sort the flows
	// lexicographically by FlowID.
	sort.Slice(response.Flows, func(i, j int) bool {
		return bytes.Compare(response.Flows[i].FlowID.GetBytes(), response.Flows[j].FlowID.GetBytes()) < 0
	})
	return response, nil
}

func (b *baseStatusServer) localExecutionInsights(
	ctx context.Context,
) (*serverpb.ListExecutionInsightsResponse, error) {
	var response serverpb.ListExecutionInsightsResponse

	reader := b.sqlServer.pgServer.SQLServer.GetInsightsReader()
	reader.IterateInsights(ctx, func(ctx context.Context, insight *insights.Insight) {
		if insight == nil {
			return
		}

		// Versions <=22.2.6 expects that Statement is not null when building the exec insights virtual table.
		insightWithStmt := *insight
		insightWithStmt.Statement = &insights.Statement{}

		response.Insights = append(response.Insights, insightWithStmt)
	})

	return &response, nil
}

func (b *baseStatusServer) localTxnIDResolution(
	req *serverpb.TxnIDResolutionRequest,
) *serverpb.TxnIDResolutionResponse {
	txnIDCache := b.sqlServer.pgServer.SQLServer.GetTxnIDCache()

	unresolvedTxnIDs := make(map[uuid.UUID]struct{}, len(req.TxnIDs))
	for _, txnID := range req.TxnIDs {
		unresolvedTxnIDs[txnID] = struct{}{}
	}

	resp := &serverpb.TxnIDResolutionResponse{
		ResolvedTxnIDs: make([]contentionpb.ResolvedTxnID, 0, len(req.TxnIDs)),
	}

	for i := range req.TxnIDs {
		if txnFingerprintID, found := txnIDCache.Lookup(req.TxnIDs[i]); found {
			resp.ResolvedTxnIDs = append(resp.ResolvedTxnIDs, contentionpb.ResolvedTxnID{
				TxnID:            req.TxnIDs[i],
				TxnFingerprintID: txnFingerprintID,
			})
		}
	}

	// If we encounter any transaction ID that we cannot resolve, we tell the
	// txnID cache to drain its write buffer (note: The .DrainWriteBuffer() call
	// is asynchronous). The client of this RPC will perform retries.
	if len(unresolvedTxnIDs) > 0 {
		txnIDCache.DrainWriteBuffer()
	}

	return resp
}

func (b *baseStatusServer) localTransactionContentionEvents(
	shouldRedactContendingKey bool,
) *serverpb.TransactionContentionEventsResponse {
	registry := b.sqlServer.execCfg.ContentionRegistry

	resp := &serverpb.TransactionContentionEventsResponse{
		Events: make([]contentionpb.ExtendedContentionEvent, 0),
	}
	// Ignore error returned by ForEachEvent() since if our own callback doesn't
	// return error, ForEachEvent() also doesn't return error.
	_ = registry.ForEachEvent(func(event *contentionpb.ExtendedContentionEvent) error {
		if shouldRedactContendingKey {
			event.BlockingEvent.Key = []byte{}
		}
		resp.Events = append(resp.Events, *event)
		return nil
	})

	return resp
}

// A statusServer provides a RESTful status API.
type statusServer struct {
	*baseStatusServer

	cfg                      *base.Config
	db                       *kv.DB
	metricSource             metricMarshaler
	si                       systemInfoOnce
	stmtDiagnosticsRequester StmtDiagnosticsRequester
	internalExecutor         *sql.InternalExecutor

	// cancelSemaphore is a semaphore that limits the number of
	// concurrent calls to the pgwire query cancellation endpoint. This
	// is needed to avoid the risk of a DoS attack by malicious users
	// that attempts to cancel random queries by spamming the request.
	//
	// See CancelQueryByKey() for details.
	//
	// The semaphore is initialized with a hard-coded limit of 256
	// concurrent pgwire cancel requests (per node). We also add a
	// 1-second penalty for failed cancellation requests in
	// CancelQueryByKey, meaning that an attacker needs 1 second per
	// guess. With an attacker randomly guessing a 32-bit secret, it
	// would take 2^24 seconds to hit one query. If we suppose there are
	// 256 concurrent queries actively running on a node, then it would
	// take 2^16 seconds (18 hours) to hit any one of them.
	cancelSemaphore *quotapool.IntPool
}

// systemStatusServer is an extension of the standard
// statusServer defined above with a few extra fields
// that support additional implementations of APIs that
// require system tenant access. This includes endpoints
// that require gossip, or information about nodes and
// stores.
// In general, add new RPC implementations to the base
// statusServer to ensure feature parity with system and
// app tenants.
type systemStatusServer struct {
	*statusServer

	gossip             *gossip.Gossip
	storePool          *storepool.StorePool
	stores             *kvserver.Stores
	nodeLiveness       *liveness.NodeLiveness
	spanConfigReporter spanconfig.Reporter
	distSender         *kvcoord.DistSender
	rangeStatsFetcher  *rangestats.Fetcher
	node               *Node
}

// StmtDiagnosticsRequester is the interface into *stmtdiagnostics.Registry
// used by AdminUI endpoints.
type StmtDiagnosticsRequester interface {
	// InsertRequest adds an entry to system.statement_diagnostics_requests for
	// tracing a query with the given fingerprint. Once this returns, calling
	// stmtdiagnostics.ShouldCollectDiagnostics() on the current node will
	// return true depending on the parameters below.
	// - samplingProbability controls how likely we are to try and collect a
	//  diagnostics report for a given execution. The semantics with
	//  minExecutionLatency are as follows:
	//  - If samplingProbability is zero, we're always sampling. This is for
	//    compatibility with pre-22.2 versions where this parameter was not
	//    available.
	//  - If samplingProbability is non-zero, minExecutionLatency must be
	//    non-zero. We'll sample stmt executions with the given probability
	//    until:
	//    (a) we capture one that exceeds minExecutionLatency, or
	//    (b) we hit the expiresAfter point.
	// - minExecutionLatency, if non-zero, determines the minimum execution
	//   latency of a query that satisfies the request. In other words, queries
	//   that ran faster than minExecutionLatency do not satisfy the condition
	//   and the bundle is not generated for them.
	// - expiresAfter, if non-zero, indicates for how long the request should
	//   stay active.
	InsertRequest(
		ctx context.Context,
		stmtFingerprint string,
		samplingProbability float64,
		minExecutionLatency time.Duration,
		expiresAfter time.Duration,
	) error
	// CancelRequest updates an entry in system.statement_diagnostics_requests
	// for tracing a query with the given fingerprint to be expired (thus,
	// canceling any new tracing for it).
	CancelRequest(ctx context.Context, requestID int64) error
}

// newStatusServer allocates and returns a statusServer.
func newStatusServer(
	ambient log.AmbientContext,
	st *cluster.Settings,
	cfg *base.Config,
	adminAuthzCheck *adminPrivilegeChecker,
	db *kv.DB,
	metricSource metricMarshaler,
	rpcCtx *rpc.Context,
	stopper *stop.Stopper,
	sessionRegistry *sql.SessionRegistry,
	closedSessionCache *sql.ClosedSessionCache,
	remoteFlowRunner *flowinfra.RemoteFlowRunner,
	internalExecutor *sql.InternalExecutor,
	serverIterator ServerIterator,
	clock *hlc.Clock,
) *statusServer {
	ambient.AddLogTag("status", nil)
	if !rpcCtx.TenantID.IsSystem() {
		ambient.AddLogTag("tenant", rpcCtx.TenantID)
	}

	server := &statusServer{
		baseStatusServer: &baseStatusServer{
			AmbientContext:     ambient,
			privilegeChecker:   adminAuthzCheck,
			sessionRegistry:    sessionRegistry,
			closedSessionCache: closedSessionCache,
			remoteFlowRunner:   remoteFlowRunner,
			st:                 st,
			rpcCtx:             rpcCtx,
			stopper:            stopper,
			serverIterator:     serverIterator,
			clock:              clock,
		},
		cfg:              cfg,
		db:               db,
		metricSource:     metricSource,
		internalExecutor: internalExecutor,

		// See the docstring on cancelSemaphore for details about this initialization.
		cancelSemaphore: quotapool.NewIntPool("pgwire-cancel", 256),
	}

	return server
}

// newSystemStatusServer allocates and returns a statusServer.
func newSystemStatusServer(
	ambient log.AmbientContext,
	st *cluster.Settings,
	cfg *base.Config,
	adminAuthzCheck *adminPrivilegeChecker,
	db *kv.DB,
	gossip *gossip.Gossip,
	metricSource metricMarshaler,
	nodeLiveness *liveness.NodeLiveness,
	storePool *storepool.StorePool,
	rpcCtx *rpc.Context,
	stores *kvserver.Stores,
	stopper *stop.Stopper,
	sessionRegistry *sql.SessionRegistry,
	closedSessionCache *sql.ClosedSessionCache,
	remoteFlowRunner *flowinfra.RemoteFlowRunner,
	internalExecutor *sql.InternalExecutor,
	serverIterator ServerIterator,
	spanConfigReporter spanconfig.Reporter,
	clock *hlc.Clock,
	distSender *kvcoord.DistSender,
	rangeStatsFetcher *rangestats.Fetcher,
	node *Node,
) *systemStatusServer {
	server := newStatusServer(
		ambient,
		st,
		cfg,
		adminAuthzCheck,
		db,
		metricSource,
		rpcCtx,
		stopper,
		sessionRegistry,
		closedSessionCache,
		remoteFlowRunner,
		internalExecutor,
		serverIterator,
		clock,
	)

	return &systemStatusServer{
		statusServer:       server,
		gossip:             gossip,
		storePool:          storePool,
		stores:             stores,
		nodeLiveness:       nodeLiveness,
		spanConfigReporter: spanConfigReporter,
		distSender:         distSender,
		rangeStatsFetcher:  rangeStatsFetcher,
		node:               node,
	}
}

// setStmtDiagnosticsRequester is used to provide a StmtDiagnosticsRequester to
// the status server. This cannot be done at construction time because the
// implementation of StmtDiagnosticsRequester depends on an executor which in
// turn depends on the statusServer.
func (s *statusServer) setStmtDiagnosticsRequester(sr StmtDiagnosticsRequester) {
	s.stmtDiagnosticsRequester = sr
}

// RegisterService registers the GRPC service.
func (s *statusServer) RegisterService(g *grpc.Server) {
	serverpb.RegisterStatusServer(g, s)
}

// RegisterGateway starts the gateway (i.e. reverse
// proxy) that proxies HTTP requests to the appropriate gRPC endpoints.
func (s *statusServer) RegisterGateway(
	ctx context.Context, mux *gwruntime.ServeMux, conn *grpc.ClientConn,
) error {
	ctx = s.AnnotateCtx(ctx)
	return serverpb.RegisterStatusHandler(ctx, mux, conn)
}

// RegisterService registers the GRPC service.
func (s *systemStatusServer) RegisterService(g *grpc.Server) {
	serverpb.RegisterStatusServer(g, s)
}

func (s *statusServer) parseNodeID(nodeIDParam string) (roachpb.NodeID, bool, error) {
	id, local, err := s.serverIterator.parseServerID(nodeIDParam)
	return roachpb.NodeID(id), local, err
}

func (s *statusServer) dialNode(
	ctx context.Context, nodeID roachpb.NodeID,
) (serverpb.StatusClient, error) {
	conn, err := s.serverIterator.dialNode(ctx, serverID(nodeID))
	if err != nil {
		return nil, err
	}
	return serverpb.NewStatusClient(conn), nil
}

// Gossip returns gossip network status. It is implemented
// in the systemStatusServer since the system tenant has
// access to gossip.
func (s *systemStatusServer) Gossip(
	ctx context.Context, req *serverpb.GossipRequest,
) (*gossip.InfoStatus, error) {
	ctx = forwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, err := s.privilegeChecker.requireAdminUser(ctx); err != nil {
		// NB: not using serverError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if local {
		infoStatus := s.gossip.GetInfoStatus()
		return &infoStatus, nil
	}
	status, err := s.dialNode(ctx, nodeID)
	if err != nil {
		return nil, serverError(ctx, err)
	}
	return status.Gossip(ctx, req)
}

func (s *systemStatusServer) EngineStats(
	ctx context.Context, req *serverpb.EngineStatsRequest,
) (*serverpb.EngineStatsResponse, error) {
	ctx = forwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, err := s.privilegeChecker.requireAdminUser(ctx); err != nil {
		// NB: not using serverError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if !local {
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, serverError(ctx, err)
		}
		return status.EngineStats(ctx, req)
	}

	resp := new(serverpb.EngineStatsResponse)
	err = s.stores.VisitStores(func(store *kvserver.Store) error {
		engineStatsInfo := serverpb.EngineStatsInfo{
			StoreID:              store.Ident.StoreID,
			TickersAndHistograms: nil,
			EngineType:           store.TODOEngine().Type(),
		}

		resp.Stats = append(resp.Stats, engineStatsInfo)
		return nil
	})
	if err != nil {
		return nil, serverError(ctx, err)
	}
	return resp, nil
}

// Allocator returns simulated allocator info for the ranges on the given node.
func (s *systemStatusServer) Allocator(
	ctx context.Context, req *serverpb.AllocatorRequest,
) (*serverpb.AllocatorResponse, error) {
	ctx = forwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.requireViewClusterMetadataPermission(ctx); err != nil {
		// NB: not using serverError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if !local {
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, serverError(ctx, err)
		}
		return status.Allocator(ctx, req)
	}

	output := new(serverpb.AllocatorResponse)
	err = s.stores.VisitStores(func(store *kvserver.Store) error {
		// All ranges requested:
		if len(req.RangeIDs) == 0 {
			var err error
			store.VisitReplicas(
				func(rep *kvserver.Replica) bool {
					if !rep.OwnsValidLease(ctx, store.Clock().NowAsClockTimestamp()) {
						return true // continue.
					}
					var allocatorSpans tracingpb.Recording
					allocatorSpans, err = store.ReplicateQueueDryRun(ctx, rep)
					if err != nil {
						return false // break and bubble up the error.
					}
					output.DryRuns = append(output.DryRuns, &serverpb.AllocatorDryRun{
						RangeID: rep.RangeID,
						Events:  recordedSpansToTraceEvents(allocatorSpans),
					})
					return true // continue.
				},
				kvserver.WithReplicasInOrder(),
			)
			return err
		}

		// Specific ranges requested:
		for _, rid := range req.RangeIDs {
			rep, err := store.GetReplica(rid)
			if err != nil {
				// Not found: continue.
				continue
			}
			if !rep.OwnsValidLease(ctx, store.Clock().NowAsClockTimestamp()) {
				continue
			}
			allocatorSpans, err := store.ReplicateQueueDryRun(ctx, rep)
			if err != nil {
				return err
			}
			output.DryRuns = append(output.DryRuns, &serverpb.AllocatorDryRun{
				RangeID: rep.RangeID,
				Events:  recordedSpansToTraceEvents(allocatorSpans),
			})
		}
		return nil
	})
	if err != nil {
		return nil, serverError(ctx, err)
	}
	return output, nil
}

func recordedSpansToTraceEvents(spans []tracingpb.RecordedSpan) []*serverpb.TraceEvent {
	var output []*serverpb.TraceEvent
	for _, sp := range spans {
		for _, entry := range sp.Logs {
			event := &serverpb.TraceEvent{
				Time:    entry.Time,
				Message: entry.Msg().StripMarkers(),
			}
			output = append(output, event)
		}
	}
	return output
}

// CriticalNodes retrieves nodes that are considered critical. A critical node
// is one whose unexpected termination could result in data loss. A node is
// considered critical if any of its replicas are unavailable or
// under-replicated. The response includes a list of node descriptors that are
// considered critical, and the corresponding SpanConfigConformanceReport that
// includes details of non-conforming ranges contributing to the criticality.
func (s *systemStatusServer) CriticalNodes(
	ctx context.Context, req *serverpb.CriticalNodesRequest,
) (*serverpb.CriticalNodesResponse, error) {
	ctx = s.AnnotateCtx(ctx)
	if _, err := s.privilegeChecker.requireAdminUser(ctx); err != nil {
		return nil, err
	}
	conformance, err := s.node.SpanConfigConformance(
		ctx, &roachpb.SpanConfigConformanceRequest{
			Spans: []roachpb.Span{keys.EverythingSpan},
		})
	if err != nil {
		return nil, err
	}

	critical := make(map[roachpb.NodeID]bool)
	for _, r := range conformance.Report.UnderReplicated {
		for _, desc := range r.RangeDescriptor.Replicas().Descriptors() {
			critical[desc.NodeID] = true
		}
	}
	for _, r := range conformance.Report.Unavailable {
		for _, desc := range r.RangeDescriptor.Replicas().Descriptors() {
			critical[desc.NodeID] = true
		}
	}

	res := &serverpb.CriticalNodesResponse{
		CriticalNodes: nil,
		Report:        conformance.Report,
	}
	for nodeID := range critical {
		ns, err := s.nodeStatus(ctx, &serverpb.NodeRequest{NodeId: nodeID.String()})
		if err != nil {
			return nil, err
		}
		res.CriticalNodes = append(res.CriticalNodes, ns.Desc)
	}
	return res, nil
}

// AllocatorRange returns simulated allocator info for the requested range.
func (s *systemStatusServer) AllocatorRange(
	ctx context.Context, req *serverpb.AllocatorRangeRequest,
) (*serverpb.AllocatorRangeResponse, error) {
	ctx = forwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	err := s.privilegeChecker.requireViewClusterMetadataPermission(ctx)
	if err != nil {
		return nil, err
	}

	isLiveMap := s.nodeLiveness.GetIsLiveMap()
	type nodeResponse struct {
		nodeID roachpb.NodeID
		resp   *serverpb.AllocatorResponse
		err    error
	}
	responses := make(chan nodeResponse)
	// TODO(bram): consider abstracting out this repeated pattern.
	for nodeID := range isLiveMap {
		nodeID := nodeID
		if err := s.stopper.RunAsyncTask(
			ctx,
			"server.statusServer: requesting remote Allocator simulation",
			func(ctx context.Context) {
				_ = contextutil.RunWithTimeout(ctx, "allocator range", 3*time.Second, func(ctx context.Context) error {
					status, err := s.dialNode(ctx, nodeID)
					var allocatorResponse *serverpb.AllocatorResponse
					if err == nil {
						allocatorRequest := &serverpb.AllocatorRequest{
							RangeIDs: []roachpb.RangeID{roachpb.RangeID(req.RangeId)},
						}
						allocatorResponse, err = status.Allocator(ctx, allocatorRequest)
					}
					response := nodeResponse{
						nodeID: nodeID,
						resp:   allocatorResponse,
						err:    err,
					}

					select {
					case responses <- response:
						// Response processed.
					case <-ctx.Done():
						// Context completed, response no longer needed.
					}
					return nil
				})
			}); err != nil {
			return nil, serverError(ctx, err)
		}
	}

	errs := make(map[roachpb.NodeID]error)
	for remainingResponses := len(isLiveMap); remainingResponses > 0; remainingResponses-- {
		select {
		case resp := <-responses:
			if resp.err != nil {
				errs[resp.nodeID] = resp.err
				continue
			}
			if len(resp.resp.DryRuns) > 0 {
				return &serverpb.AllocatorRangeResponse{
					NodeID: resp.nodeID,
					DryRun: resp.resp.DryRuns[0],
				}, nil
			}
		case <-ctx.Done():
			return nil, status.Errorf(codes.DeadlineExceeded, "request timed out")
		}
	}

	// We didn't get a valid simulated Allocator run. Just return whatever errors
	// we got instead. If we didn't even get any errors, then there is no active
	// leaseholder for the range.
	if len(errs) > 0 {
		var buf bytes.Buffer
		for nodeID, err := range errs {
			if buf.Len() > 0 {
				buf.WriteByte('\n')
			}
			fmt.Fprintf(&buf, "n%d: %s", nodeID, err)
		}
		return nil, serverErrorf(ctx, "%v", buf)
	}
	return &serverpb.AllocatorRangeResponse{}, nil
}

// Certificates returns the x509 certificates.
func (s *statusServer) Certificates(
	ctx context.Context, req *serverpb.CertificatesRequest,
) (*serverpb.CertificatesResponse, error) {
	ctx = forwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, err := s.privilegeChecker.requireAdminUser(ctx); err != nil {
		// NB: not using serverError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if s.cfg.Insecure {
		return nil, status.Errorf(codes.Unavailable, "server is in insecure mode, cannot examine certificates")
	}

	if !local {
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, serverError(ctx, err)
		}
		return status.Certificates(ctx, req)
	}

	cm, err := s.rpcCtx.GetCertificateManager()
	if err != nil {
		return nil, serverError(ctx, err)
	}

	// The certificate manager gives us a list of CertInfo objects to avoid
	// making security depend on serverpb.
	certs, err := cm.ListCertificates()
	if err != nil {
		return nil, serverError(ctx, err)
	}

	cr := &serverpb.CertificatesResponse{}
	for _, cert := range certs {
		details := serverpb.CertificateDetails{}
		switch cert.FileUsage {
		case security.CAPem:
			details.Type = serverpb.CertificateDetails_CA
		case security.ClientCAPem:
			details.Type = serverpb.CertificateDetails_CLIENT_CA
		case security.UICAPem:
			details.Type = serverpb.CertificateDetails_UI_CA
		case security.NodePem:
			details.Type = serverpb.CertificateDetails_NODE
		case security.UIPem:
			details.Type = serverpb.CertificateDetails_UI
		case security.ClientPem:
			details.Type = serverpb.CertificateDetails_CLIENT
		default:
			return nil, serverErrorf(ctx, "unknown certificate type %v for file %s", cert.FileUsage, cert.Filename)
		}

		if cert.Error == nil {
			if err := extractCertFields(cert.FileContents, &details); err != nil {
				details.ErrorMessage = err.Error()
			}
		} else {
			details.ErrorMessage = cert.Error.Error()
		}
		cr.Certificates = append(cr.Certificates, details)
	}

	return cr, nil
}

func formatCertNames(p pkix.Name) string {
	return fmt.Sprintf("CommonName=%s, Organization=%s", p.CommonName, strings.Join(p.Organization, ","))
}

func extractCertFields(contents []byte, details *serverpb.CertificateDetails) error {
	certs, err := security.PEMContentsToX509(contents)
	if err != nil {
		return err
	}

	for _, c := range certs {
		addresses := c.DNSNames
		for _, ip := range c.IPAddresses {
			addresses = append(addresses, ip.String())
		}

		extKeyUsage := make([]string, len(c.ExtKeyUsage))
		for i, eku := range c.ExtKeyUsage {
			extKeyUsage[i] = security.ExtKeyUsageToString(eku)
		}

		var pubKeyInfo string
		if rsaPub, ok := c.PublicKey.(*rsa.PublicKey); ok {
			pubKeyInfo = fmt.Sprintf("%d bit RSA", rsaPub.N.BitLen())
		} else if ecdsaPub, ok := c.PublicKey.(*ecdsa.PublicKey); ok {
			pubKeyInfo = fmt.Sprintf("%d bit ECDSA", ecdsaPub.Params().BitSize)
		} else {
			// go's x509 library does not support other types (so far).
			pubKeyInfo = fmt.Sprintf("unknown key type %T", c.PublicKey)
		}

		details.Fields = append(details.Fields, serverpb.CertificateDetails_Fields{
			Issuer:             formatCertNames(c.Issuer),
			Subject:            formatCertNames(c.Subject),
			ValidFrom:          c.NotBefore.UnixNano(),
			ValidUntil:         c.NotAfter.UnixNano(),
			Addresses:          addresses,
			SignatureAlgorithm: c.SignatureAlgorithm.String(),
			PublicKey:          pubKeyInfo,
			KeyUsage:           security.KeyUsageToString(c.KeyUsage),
			ExtendedKeyUsage:   extKeyUsage,
		})
	}
	return nil
}

// Details returns node details.
func (s *statusServer) Details(
	ctx context.Context, req *serverpb.DetailsRequest,
) (*serverpb.DetailsResponse, error) {
	ctx = forwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, err := s.privilegeChecker.requireAdminUser(ctx); err != nil {
		// NB: not using serverError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	if !local {
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, serverError(ctx, err)
		}
		return status.Details(ctx, req)
	}

	remoteNodeID := roachpb.NodeID(s.serverIterator.getID())
	resp := &serverpb.DetailsResponse{
		NodeID:     remoteNodeID,
		BuildInfo:  build.GetInfo(),
		SystemInfo: s.si.systemInfo(ctx),
	}
	if addr, err := s.serverIterator.getServerIDAddress(ctx, serverID(remoteNodeID)); err == nil {
		resp.Address = *addr
	}
	if addr, err := s.serverIterator.getServerIDSQLAddress(ctx, serverID(remoteNodeID)); err == nil {
		resp.SQLAddress = *addr
	}

	return resp, nil
}

// GetFiles returns a list of files of type defined in the request.
func (s *statusServer) GetFiles(
	ctx context.Context, req *serverpb.GetFilesRequest,
) (*serverpb.GetFilesResponse, error) {
	ctx = forwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, err := s.privilegeChecker.requireAdminUser(ctx); err != nil {
		// NB: not using serverError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	if !local {
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, serverError(ctx, err)
		}
		return status.GetFiles(ctx, req)
	}

	return getLocalFiles(req, s.sqlServer.cfg.HeapProfileDirName, s.sqlServer.cfg.GoroutineDumpDirName)
}

// checkFilePattern checks if a pattern is acceptable for the GetFiles call.
// Only patterns to match filenames are acceptable, not more general paths.
func checkFilePattern(pattern string) error {
	if strings.Contains(pattern, string(os.PathSeparator)) {
		return errors.New("invalid pattern: cannot have path seperators")
	}
	return nil
}

// LogFilesList returns a list of available log files.
//
// Note that even though the FileInfo struct does not store the path
// to the log file(s), each file can be mapped back to its directory
// reliably via LogFile(), thanks to the unique file group names in
// the log configuration. For example, consider the following config:
//
// file-groups:
//
//	groupA:
//	  dir: dir1
//	groupB:
//	  dir: dir2
//
// The result of ListLogFiles on this config will return the list
// {cockroach-groupA.XXX.log, cockroach-groupB.XXX.log}, without
// directory information. This can be mapped back to dir1 and dir2 via the
// configuration. We know that groupA files cannot be in dir2 because
// the group names are unique under file-groups and so there cannot be
// two different groups with the same name and different directories.
func (s *statusServer) LogFilesList(
	ctx context.Context, req *serverpb.LogFilesListRequest,
) (*serverpb.LogFilesListResponse, error) {
	ctx = forwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, err := s.privilegeChecker.requireAdminUser(ctx); err != nil {
		// NB: not using serverError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	if !local {
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, serverError(ctx, err)
		}
		return status.LogFilesList(ctx, req)
	}
	log.Flush()
	logFiles, err := log.ListLogFiles()
	if err != nil {
		return nil, serverError(ctx, err)
	}
	return &serverpb.LogFilesListResponse{Files: logFiles}, nil
}

// LogFile returns a single log file.
//
// See the comment on LogfilesList() to understand why+how log file
// names are mapped to their full path.
func (s *statusServer) LogFile(
	ctx context.Context, req *serverpb.LogFileRequest,
) (*serverpb.LogEntriesResponse, error) {
	ctx = forwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, err := s.privilegeChecker.requireAdminUser(ctx); err != nil {
		// NB: not using serverError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	if !local {
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, serverError(ctx, err)
		}
		return status.LogFile(ctx, req)
	}

	// Determine how to redact.
	inputEditMode := log.SelectEditMode(req.Redact, log.KeepRedactable)

	// Ensure that the latest log entries are available in files.
	log.Flush()

	// Read the logs.
	reader, err := log.GetLogReader(req.File)
	if err != nil {
		return nil, serverError(ctx, errors.Wrapf(err, "log file %q could not be opened", req.File))
	}
	defer reader.Close()

	var resp serverpb.LogEntriesResponse
	decoder, err := log.NewEntryDecoder(reader, inputEditMode)
	if err != nil {
		return nil, serverError(ctx, err)
	}
	// Unless we're the system tenant, clients should only be able
	// to view logs that pertain to their own tenant. Set the filter
	// accordingly.
	tenantIDFilter := ""
	if s.rpcCtx.TenantID != roachpb.SystemTenantID {
		tenantIDFilter = s.rpcCtx.TenantID.String()
	}
	for {
		var entry logpb.Entry
		if err := decoder.Decode(&entry); err != nil {
			if err == io.EOF {
				break
			}
			return nil, serverError(ctx, err)
		}
		if tenantIDFilter != "" && entry.TenantID != tenantIDFilter {
			continue
		}
		resp.Entries = append(resp.Entries, entry)
	}

	return &resp, nil
}

// parseInt64WithDefault attempts to parse the passed in string. If an empty
// string is supplied or parsing results in an error the default value is
// returned.  If an error does occur during parsing, the error is returned as
// well.
func parseInt64WithDefault(s string, defaultValue int64) (int64, error) {
	if len(s) == 0 {
		return defaultValue, nil
	}
	result, err := strconv.ParseInt(s, 10, 0)
	if err != nil {
		return defaultValue, err
	}
	return result, nil
}

// Logs returns the log entries parsed from the log files stored on
// the server. Log entries are returned in reverse chronological order. The
// following options are available:
//   - "starttime" query parameter filters the log entries to only ones that
//     occurred on or after the "starttime". Defaults to a day ago.
//   - "endtime" query parameter filters the log entries to only ones that
//     occurred before on on the "endtime". Defaults to the current time.
//   - "pattern" query parameter filters the log entries by the provided regexp
//     pattern if it exists. Defaults to nil.
//   - "max" query parameter is the hard limit of the number of returned log
//     entries. Defaults to defaultMaxLogEntries.
//
// To filter the log messages to only retrieve messages from a given level,
// use a pattern that excludes all messages at the undesired levels.
// (e.g. "^[^IW]" to only get errors, fatals and panics). An exclusive
// pattern is better because panics and some other errors do not use
// a prefix character.
func (s *statusServer) Logs(
	ctx context.Context, req *serverpb.LogsRequest,
) (*serverpb.LogEntriesResponse, error) {
	ctx = forwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, err := s.privilegeChecker.requireAdminUser(ctx); err != nil {
		// NB: not using serverError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	if !local {
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, serverError(ctx, err)
		}
		return status.Logs(ctx, req)
	}

	// Determine how to redact.
	inputEditMode := log.SelectEditMode(req.Redact, log.KeepRedactable)

	// Select the time interval.
	startTimestamp, err := parseInt64WithDefault(
		req.StartTime,
		timeutil.Now().AddDate(0, 0, -1).UnixNano())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "StartTime could not be parsed: %s", err)
	}

	endTimestamp, err := parseInt64WithDefault(req.EndTime, timeutil.Now().UnixNano())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "EndTime could not be parsed: %s", err)
	}

	if startTimestamp > endTimestamp {
		return nil, status.Errorf(codes.InvalidArgument, "StartTime: %d should not be greater than endtime: %d", startTimestamp, endTimestamp)
	}

	maxEntries, err := parseInt64WithDefault(req.Max, defaultMaxLogEntries)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Max could not be parsed: %s", err)
	}
	if maxEntries < 1 {
		return nil, status.Errorf(codes.InvalidArgument, "Max: %d should be set to a value greater than 0", maxEntries)
	}

	var regex *regexp.Regexp
	if len(req.Pattern) > 0 {
		if regex, err = regexp.Compile(req.Pattern); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "regex pattern could not be compiled: %s", err)
		}
	}

	// Ensure that the latest log entries are available in files.
	log.Flush()

	// Read the logs.
	entries, err := log.FetchEntriesFromFiles(
		startTimestamp, endTimestamp, int(maxEntries), regex, inputEditMode)
	if err != nil {
		return nil, serverError(ctx, err)
	}

	out := &serverpb.LogEntriesResponse{}
	// Unless we're the system tenant, clients should only be able
	// to view logs that pertain to their own tenant. Set the filter
	// accordingly.
	tenantIDFilter := ""
	if s.rpcCtx.TenantID != roachpb.SystemTenantID {
		tenantIDFilter = s.rpcCtx.TenantID.String()
	}
	for _, e := range entries {
		if tenantIDFilter != "" && e.TenantID != tenantIDFilter {
			continue
		}
		out.Entries = append(out.Entries, e)
	}

	return out, nil
}

// Stacks returns goroutine or thread stack traces.
func (s *statusServer) Stacks(
	ctx context.Context, req *serverpb.StacksRequest,
) (*serverpb.JSONResponse, error) {
	ctx = forwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, err := s.privilegeChecker.requireAdminUser(ctx); err != nil {
		// NB: not using serverError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if !local {
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, serverError(ctx, err)
		}
		return status.Stacks(ctx, req)
	}

	return stacksLocal(req)
}

// TODO(tschottdorf): significant overlap with /debug/pprof/heap, except that
// this one allows querying by NodeID.
//
// Profile returns a heap profile. This endpoint is used by the
// `pprofui` package to satisfy local and remote pprof requests.
func (s *statusServer) Profile(
	ctx context.Context, req *serverpb.ProfileRequest,
) (*serverpb.JSONResponse, error) {
	ctx = forwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, err := s.privilegeChecker.requireAdminUser(ctx); err != nil {
		// NB: not using serverError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if !local {
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, serverError(ctx, err)
		}
		return status.Profile(ctx, req)
	}

	return profileLocal(ctx, req, s.st)
}

// Regions implements the serverpb.StatusServer interface.
func (s *systemStatusServer) Regions(
	ctx context.Context, req *serverpb.RegionsRequest,
) (*serverpb.RegionsResponse, error) {
	resp, _, err := s.nodesHelper(ctx, 0 /* limit */, 0 /* offset */)
	if err != nil {
		return nil, serverError(ctx, err)
	}
	return regionsResponseFromNodesResponse(resp), nil
}

func regionsResponseFromNodesResponse(nr *serverpb.NodesResponse) *serverpb.RegionsResponse {
	regionsToZones := make(map[string]map[string]struct{})
	for _, node := range nr.Nodes {
		var region string
		var zone string
		for _, tier := range node.Desc.Locality.Tiers {
			switch tier.Key {
			case "region":
				region = tier.Value
			case "zone", "availability-zone", "az":
				zone = tier.Value
			}
		}
		if region == "" {
			continue
		}
		if _, ok := regionsToZones[region]; !ok {
			regionsToZones[region] = make(map[string]struct{})
		}
		if zone != "" {
			regionsToZones[region][zone] = struct{}{}
		}
	}
	ret := &serverpb.RegionsResponse{
		Regions: make(map[string]*serverpb.RegionsResponse_Region, len(regionsToZones)),
	}
	for region, zones := range regionsToZones {
		zonesArr := make([]string, 0, len(zones))
		for z := range zones {
			zonesArr = append(zonesArr, z)
		}
		sort.Strings(zonesArr)
		ret.Regions[region] = &serverpb.RegionsResponse_Region{
			Zones: zonesArr,
		}
	}
	return ret
}

// NodesList returns a list of nodes with their corresponding addresses.
func (s *statusServer) NodesList(
	ctx context.Context, _ *serverpb.NodesListRequest,
) (*serverpb.NodesListResponse, error) {
	ctx = forwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	// The node status contains details about the command line, network
	// addresses, env vars etc which are admin-only.
	if _, err := s.privilegeChecker.requireAdminUser(ctx); err != nil {
		// NB: not using serverError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}
	return s.serverIterator.nodesList(ctx)
}

// Nodes returns all node statuses.
//
// Do not use this method inside the server code! Use
// ListNodesInternal() instead.
// This method here is the one exposed to network clients over HTTP.
//
// The LivenessByNodeID in the response returns the known liveness
// information according to gossip. Nodes for which there is no gossip
// information will not have an entry. Clients can exploit the fact
// that status "UNKNOWN" has value 0 (the default) when accessing the
// map.
func (s *systemStatusServer) Nodes(
	ctx context.Context, req *serverpb.NodesRequest,
) (*serverpb.NodesResponse, error) {
	ctx = forwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	err := s.privilegeChecker.requireViewClusterMetadataPermission(ctx)
	if err != nil {
		return nil, err
	}

	resp, _, err := s.nodesHelper(ctx, 0 /* limit */, 0 /* offset */)
	if err != nil {
		return nil, serverError(ctx, err)
	}
	return resp, nil
}

// NodesUI on the tenant, delegates to the storage layer's endpoint after
// checking local SQL permissions. The tenant connector will require
// additional tenant capabilities in order to let the request through,
// but the `NodesTenant` endpoint will do no additional permissioning on
// the system-tenant side.
func (s *statusServer) NodesUI(
	ctx context.Context, req *serverpb.NodesRequest,
) (*serverpb.NodesResponseExternal, error) {
	ctx = s.AnnotateCtx(ctx)

	hasViewClusterMetadata := false
	err := s.privilegeChecker.requireViewClusterMetadataPermission(ctx)
	if err != nil {
		if !grpcutil.IsAuthError(err) {
			return nil, err
		}
	} else {
		hasViewClusterMetadata = true
	}

	internalResp, err := s.sqlServer.tenantConnect.Nodes(ctx, req)
	if err != nil {
		return nil, serverError(ctx, err)
	}

	resp := &serverpb.NodesResponseExternal{
		Nodes:            make([]serverpb.NodeResponse, len(internalResp.Nodes)),
		LivenessByNodeID: internalResp.LivenessByNodeID,
	}
	for i, nodeStatus := range internalResp.Nodes {
		resp.Nodes[i] = nodeStatusToResp(&nodeStatus, hasViewClusterMetadata)
	}

	return resp, nil
}

func (s *systemStatusServer) NodesUI(
	ctx context.Context, req *serverpb.NodesRequest,
) (*serverpb.NodesResponseExternal, error) {
	ctx = forwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	hasViewClusterMetadata := false
	err := s.privilegeChecker.requireViewClusterMetadataPermission(ctx)
	if err != nil {
		if !grpcutil.IsAuthError(err) {
			return nil, err
		}
	} else {
		hasViewClusterMetadata = true
	}

	internalResp, _, err := s.nodesHelper(ctx, 0 /* limit */, 0 /* offset */)
	if err != nil {
		return nil, serverError(ctx, err)
	}
	resp := &serverpb.NodesResponseExternal{
		Nodes:            make([]serverpb.NodeResponse, len(internalResp.Nodes)),
		LivenessByNodeID: internalResp.LivenessByNodeID,
	}
	for i, nodeStatus := range internalResp.Nodes {
		resp.Nodes[i] = nodeStatusToResp(&nodeStatus, hasViewClusterMetadata)
	}

	return resp, nil
}

func nodeStatusToResp(n *statuspb.NodeStatus, hasViewClusterMetadata bool) serverpb.NodeResponse {
	tiers := make([]serverpb.Tier, len(n.Desc.Locality.Tiers))
	for j, t := range n.Desc.Locality.Tiers {
		tiers[j] = serverpb.Tier{
			Key:   t.Key,
			Value: t.Value,
		}
	}

	activity := make(map[roachpb.NodeID]serverpb.NodeResponse_NetworkActivity, len(n.Activity))
	for k, v := range n.Activity {
		activity[k] = serverpb.NodeResponse_NetworkActivity{
			Latency: v.Latency,
		}
	}

	nodeDescriptor := serverpb.NodeDescriptor{
		NodeID:  n.Desc.NodeID,
		Address: util.UnresolvedAddr{},
		Attrs:   roachpb.Attributes{},
		Locality: serverpb.Locality{
			Tiers: tiers,
		},
		ServerVersion: serverpb.Version{
			Major:    n.Desc.ServerVersion.Major,
			Minor:    n.Desc.ServerVersion.Minor,
			Patch:    n.Desc.ServerVersion.Patch,
			Internal: n.Desc.ServerVersion.Internal,
		},
		BuildTag:        n.Desc.BuildTag,
		StartedAt:       n.Desc.StartedAt,
		LocalityAddress: nil,
		ClusterName:     n.Desc.ClusterName,
		SQLAddress:      util.UnresolvedAddr{},
	}

	statuses := make([]serverpb.StoreStatus, len(n.StoreStatuses))
	for i, ss := range n.StoreStatuses {
		statuses[i] = serverpb.StoreStatus{
			Desc: serverpb.StoreDescriptor{
				StoreID:  ss.Desc.StoreID,
				Attrs:    ss.Desc.Attrs,
				Node:     nodeDescriptor,
				Capacity: ss.Desc.Capacity,

				Properties: roachpb.StoreProperties{
					ReadOnly:  ss.Desc.Properties.ReadOnly,
					Encrypted: ss.Desc.Properties.Encrypted,
				},
			},
			Metrics: ss.Metrics,
		}
		if fsprops := ss.Desc.Properties.FileStoreProperties; fsprops != nil {
			sfsprops := &roachpb.FileStoreProperties{
				FsType: fsprops.FsType,
			}
			if hasViewClusterMetadata {
				sfsprops.Path = fsprops.Path
				sfsprops.BlockDevice = fsprops.BlockDevice
				sfsprops.MountPoint = fsprops.MountPoint
				sfsprops.MountOptions = fsprops.MountOptions
			}
			statuses[i].Desc.Properties.FileStoreProperties = sfsprops
		}
	}

	resp := serverpb.NodeResponse{
		Desc:              nodeDescriptor,
		BuildInfo:         n.BuildInfo,
		StartedAt:         n.StartedAt,
		UpdatedAt:         n.UpdatedAt,
		Metrics:           n.Metrics,
		StoreStatuses:     statuses,
		Args:              nil,
		Env:               nil,
		Latencies:         n.Latencies,
		Activity:          activity,
		TotalSystemMemory: n.TotalSystemMemory,
		NumCpus:           n.NumCpus,
	}

	if hasViewClusterMetadata {
		resp.Args = n.Args
		resp.Env = n.Env
		resp.Desc.Attrs = n.Desc.Attrs
		resp.Desc.Address = n.Desc.Address
		resp.Desc.LocalityAddress = n.Desc.LocalityAddress
		resp.Desc.SQLAddress = n.Desc.SQLAddress
		for _, n := range resp.StoreStatuses {
			n.Desc.Node = resp.Desc
		}
	}

	return resp
}

// ListNodesInternal is a helper function for the benefit of SQL exclusively.
// It skips the privilege check, assuming that SQL is doing privilege checking already.
//
// Note that the function returns plain errors, and it is the caller's
// responsibility to convert them to serverErrors.
func (s *systemStatusServer) ListNodesInternal(
	ctx context.Context, req *serverpb.NodesRequest,
) (*serverpb.NodesResponse, error) {
	resp, _, err := s.nodesHelper(ctx, 0 /* limit */, 0 /* offset */)
	return resp, err
}

// Note that the function returns plain errors, and it is the caller's
// responsibility to convert them to serverErrors.
func getNodeStatuses(
	ctx context.Context, db *kv.DB, limit, offset int,
) (statuses []statuspb.NodeStatus, next int, _ error) {
	startKey := keys.StatusNodePrefix
	endKey := startKey.PrefixEnd()

	b := &kv.Batch{}
	b.Scan(startKey, endKey)
	if err := db.Run(ctx, b); err != nil {
		return nil, 0, err
	}

	var rows []kv.KeyValue
	if len(b.Results[0].Rows) > 0 {
		var rowsInterface interface{}
		rowsInterface, next = simplePaginate(b.Results[0].Rows, limit, offset)
		rows = rowsInterface.([]kv.KeyValue)
	}

	statuses = make([]statuspb.NodeStatus, len(rows))
	for i, row := range rows {
		if err := row.ValueProto(&statuses[i]); err != nil {
			return nil, 0, err
		}
	}
	return statuses, next, nil
}

// Note that the function returns plain errors, and it is the caller's
// responsibility to convert them to serverErrors.
func (s *systemStatusServer) nodesHelper(
	ctx context.Context, limit, offset int,
) (*serverpb.NodesResponse, int, error) {
	ctx = forwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	statuses, next, err := getNodeStatuses(ctx, s.db, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	resp := serverpb.NodesResponse{
		Nodes: statuses,
	}

	clock := s.clock
	resp.LivenessByNodeID, err = getLivenessStatusMap(ctx, s.nodeLiveness, clock.Now().GoTime(), s.st)
	if err != nil {
		return nil, 0, err
	}
	return &resp, next, nil
}

// handleNodeStatus handles GET requests for a single node's status.
func (s *statusServer) Node(
	ctx context.Context, req *serverpb.NodeRequest,
) (*statuspb.NodeStatus, error) {
	ctx = forwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	// The node status contains details about the command line, network
	// addresses, env vars etc which are admin-only.
	if _, err := s.privilegeChecker.requireAdminUser(ctx); err != nil {
		// NB: not using serverError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	// NB: not using serverError() here since nodeStatus
	// already returns a proper gRPC error status.
	return s.nodeStatus(ctx, req)
}

func (s *statusServer) nodeStatus(
	ctx context.Context, req *serverpb.NodeRequest,
) (*statuspb.NodeStatus, error) {
	nodeID, _, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	key := keys.NodeStatusKey(nodeID)
	b := &kv.Batch{}
	b.Get(key)
	if err := s.db.Run(ctx, b); err != nil {
		return nil, serverError(ctx, err)
	}

	var nodeStatus statuspb.NodeStatus
	if err := b.Results[0].Rows[0].ValueProto(&nodeStatus); err != nil {
		err = errors.Wrapf(err, "could not unmarshal NodeStatus from %s", key)
		return nil, serverError(ctx, err)
	}
	return &nodeStatus, nil
}

func (s *statusServer) NodeUI(
	ctx context.Context, req *serverpb.NodeRequest,
) (*serverpb.NodeResponse, error) {
	ctx = forwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	// The node status contains details about the command line, network
	// addresses, env vars etc which are admin-only.
	_, isAdmin, err := s.privilegeChecker.getUserAndRole(ctx)
	if err != nil {
		return nil, serverError(ctx, err)
	}

	nodeStatus, err := s.nodeStatus(ctx, req)
	if err != nil {
		// NB: not using serverError() here since nodeStatus
		// already returns a proper gRPC error status.
		return nil, err
	}
	resp := nodeStatusToResp(nodeStatus, isAdmin)
	return &resp, nil
}

// Metrics return metrics information for the server specified.
func (s *statusServer) Metrics(
	ctx context.Context, req *serverpb.MetricsRequest,
) (*serverpb.JSONResponse, error) {
	ctx = forwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if !local {
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, serverError(ctx, err)
		}
		return status.Metrics(ctx, req)
	}
	j, err := marshalJSONResponse(s.metricSource)
	if err != nil {
		return nil, serverError(ctx, err)
	}
	return j, nil
}

// RaftDebug returns raft debug information for all known nodes.
func (s *systemStatusServer) RaftDebug(
	ctx context.Context, req *serverpb.RaftDebugRequest,
) (*serverpb.RaftDebugResponse, error) {
	ctx = forwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.requireViewClusterMetadataPermission(ctx); err != nil {
		// NB: not using serverError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	nodes, err := s.ListNodesInternal(ctx, nil)
	if err != nil {
		return nil, serverError(ctx, err)
	}

	mu := struct {
		syncutil.Mutex
		resp serverpb.RaftDebugResponse
	}{
		resp: serverpb.RaftDebugResponse{
			Ranges: make(map[roachpb.RangeID]serverpb.RaftRangeStatus),
		},
	}

	// Parallelize fetching of ranges to minimize total time.
	var wg sync.WaitGroup
	for _, node := range nodes.Nodes {
		wg.Add(1)
		nodeID := node.Desc.NodeID
		go func() {
			defer wg.Done()
			ranges, err := s.Ranges(ctx, &serverpb.RangesRequest{NodeId: nodeID.String(), RangeIDs: req.RangeIDs})

			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				err := errors.Wrapf(err, "failed to get ranges from %d", nodeID)
				mu.resp.Errors = append(mu.resp.Errors, serverpb.RaftRangeError{Message: err.Error()})
				return
			}

			for _, rng := range ranges.Ranges {
				rangeID := rng.State.Desc.RangeID
				status, ok := mu.resp.Ranges[rangeID]
				if !ok {
					status = serverpb.RaftRangeStatus{
						RangeID: rangeID,
					}
				}
				status.Nodes = append(status.Nodes, serverpb.RaftRangeNode{
					NodeID: nodeID,
					Range:  rng,
				})
				mu.resp.Ranges[rangeID] = status
			}
		}()
	}
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()

	// Check for errors.
	for i, rng := range mu.resp.Ranges {
		for j, node := range rng.Nodes {
			desc := node.Range.State.Desc
			// Check for whether replica should be GCed.
			containsNode := false
			for _, replica := range desc.Replicas().Descriptors() {
				if replica.NodeID == node.NodeID {
					containsNode = true
				}
			}
			if !containsNode {
				rng.Errors = append(rng.Errors, serverpb.RaftRangeError{
					Message: fmt.Sprintf("node %d not in range descriptor and should be GCed", node.NodeID),
				})
			}

			// Check for replica descs not matching.
			if j > 0 {
				prevDesc := rng.Nodes[j-1].Range.State.Desc
				if !desc.Equal(prevDesc) {
					prevNodeID := rng.Nodes[j-1].NodeID
					rng.Errors = append(rng.Errors, serverpb.RaftRangeError{
						Message: fmt.Sprintf("node %d range descriptor does not match node %d", node.NodeID, prevNodeID),
					})
				}
			}
			mu.resp.Ranges[i] = rng
		}
	}
	return &mu.resp, nil
}

type varsHandler struct {
	metricSource metricMarshaler
	st           *cluster.Settings
}

func (h varsHandler) handleVars(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	w.Header().Set(httputil.ContentTypeHeader, httputil.PlaintextContentType)
	err := h.metricSource.PrintAsText(w)
	if err != nil {
		log.Errorf(ctx, "%v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	telemetry.Inc(telemetryPrometheusVars)
}

// Ranges returns range info for the specified node.
func (s *systemStatusServer) Ranges(
	ctx context.Context, req *serverpb.RangesRequest,
) (*serverpb.RangesResponse, error) {
	resp, next, err := s.rangesHelper(ctx, req, int(req.Limit), int(req.Offset))
	if resp != nil {
		resp.Next = int32(next)
	}
	return resp, err
}

// Ranges returns range info for the specified node.
func (s *systemStatusServer) rangesHelper(
	ctx context.Context, req *serverpb.RangesRequest, limit, offset int,
) (*serverpb.RangesResponse, int, error) {
	ctx = forwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	err := s.privilegeChecker.requireViewClusterMetadataPermission(ctx)
	if err != nil {
		return nil, 0, err
	}

	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, 0, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if !local {
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, 0, err
		}
		resp, err := status.Ranges(ctx, req)
		if resp != nil && len(resp.Ranges) > 0 {
			resultInterface, next := simplePaginate(resp.Ranges, limit, offset)
			resp.Ranges = resultInterface.([]serverpb.RangeInfo)
			return resp, next, err
		}
		return resp, 0, err
	}

	output := serverpb.RangesResponse{
		Ranges: make([]serverpb.RangeInfo, 0, s.stores.GetStoreCount()),
	}

	convertRaftStatus := func(raftStatus *raft.Status) serverpb.RaftState {
		if raftStatus == nil {
			return serverpb.RaftState{
				State: raftStateDormant,
			}
		}

		state := serverpb.RaftState{
			ReplicaID:      raftStatus.ID,
			HardState:      raftStatus.HardState,
			Applied:        raftStatus.Applied,
			Lead:           raftStatus.Lead,
			State:          raftStatus.RaftState.String(),
			Progress:       make(map[uint64]serverpb.RaftState_Progress),
			LeadTransferee: raftStatus.LeadTransferee,
		}

		for id, progress := range raftStatus.Progress {
			state.Progress[id] = serverpb.RaftState_Progress{
				Match:           progress.Match,
				Next:            progress.Next,
				Paused:          progress.IsPaused(),
				PendingSnapshot: progress.PendingSnapshot,
				State:           progress.State.String(),
			}
		}

		return state
	}

	constructRangeInfo := func(
		rep *kvserver.Replica, storeID roachpb.StoreID, metrics kvserver.ReplicaMetrics,
	) serverpb.RangeInfo {
		raftStatus := rep.RaftStatus()
		raftState := convertRaftStatus(raftStatus)
		leaseHistory := rep.GetLeaseHistory()
		var span serverpb.PrettySpan
		desc := rep.Desc()
		span.StartKey = desc.StartKey.String()
		span.EndKey = desc.EndKey.String()
		state := rep.State(ctx)
		var topKLocksByWaiters []serverpb.RangeInfo_LockInfo
		for _, lm := range metrics.LockTableMetrics.TopKLocksByWaiters {
			if lm.Key == nil {
				break
			}
			topKLocksByWaiters = append(topKLocksByWaiters, serverpb.RangeInfo_LockInfo{
				PrettyKey:      lm.Key.String(),
				Key:            lm.Key,
				Held:           lm.Held,
				Waiters:        lm.Waiters,
				WaitingReaders: lm.WaitingReaders,
				WaitingWriters: lm.WaitingWriters,
			})
		}

		loadStats := rep.LoadStats()
		locality := serverpb.Locality{}
		for _, tier := range rep.GetNodeLocality().Tiers {
			locality.Tiers = append(locality.Tiers, serverpb.Tier{
				Key:   tier.Key,
				Value: tier.Value,
			})
		}
		return serverpb.RangeInfo{
			Span:          span,
			RaftState:     raftState,
			State:         state,
			SourceNodeID:  nodeID,
			SourceStoreID: storeID,
			LeaseHistory:  leaseHistory,
			Stats: serverpb.RangeStatistics{
				QueriesPerSecond:    loadStats.QueriesPerSecond,
				RequestsPerSecond:   loadStats.RequestsPerSecond,
				WritesPerSecond:     loadStats.WriteKeysPerSecond,
				ReadsPerSecond:      loadStats.ReadKeysPerSecond,
				WriteBytesPerSecond: loadStats.WriteBytesPerSecond,
				ReadBytesPerSecond:  loadStats.ReadBytesPerSecond,
				CPUTimePerSecond:    loadStats.RaftCPUNanosPerSecond + loadStats.RequestCPUNanosPerSecond,
			},
			Problems: serverpb.RangeProblems{
				Unavailable:            metrics.Unavailable,
				LeaderNotLeaseHolder:   metrics.Leader && metrics.LeaseValid && !metrics.Leaseholder,
				NoRaftLeader:           !kvserver.HasRaftLeader(raftStatus) && !metrics.Quiescent,
				Underreplicated:        metrics.Underreplicated,
				Overreplicated:         metrics.Overreplicated,
				NoLease:                metrics.Leader && !metrics.LeaseValid && !metrics.Quiescent,
				QuiescentEqualsTicking: raftStatus != nil && metrics.Quiescent == metrics.Ticking,
				RaftLogTooLarge:        metrics.RaftLogTooLarge,
				CircuitBreakerError:    len(state.CircuitBreakerError) > 0,
				PausedFollowers:        metrics.PausedFollowerCount > 0,
			},
			LeaseStatus:                 metrics.LeaseStatus,
			Quiescent:                   metrics.Quiescent,
			Ticking:                     metrics.Ticking,
			ReadLatches:                 metrics.LatchMetrics.ReadCount,
			WriteLatches:                metrics.LatchMetrics.WriteCount,
			Locks:                       metrics.LockTableMetrics.Locks,
			LocksWithWaitQueues:         metrics.LockTableMetrics.LocksWithWaitQueues,
			LockWaitQueueWaiters:        metrics.LockTableMetrics.Waiters,
			TopKLocksByWaitQueueWaiters: topKLocksByWaiters,
			Locality:                    &locality,
			IsLeaseholder:               metrics.Leaseholder,
			LeaseValid:                  metrics.LeaseValid,
		}
	}

	isLiveMap := s.nodeLiveness.GetIsLiveMap()
	clusterNodes := s.storePool.ClusterNodeCount()

	// There are two possibilities for ordering of ranges in the results:
	// it could either be determined by the RangeIDs in the request (if specified),
	// or be in RangeID order if not (as we pass in the
	// VisitReplicasInSortedOrder option to store.VisitReplicas below). The latter
	// is already sorted in a stable fashion, as far as pagination is concerned.
	// The former case requires sorting.
	if len(req.RangeIDs) > 0 {
		sort.Slice(req.RangeIDs, func(i, j int) bool {
			return req.RangeIDs[i] < req.RangeIDs[j]
		})
	}

	err = s.stores.VisitStores(func(store *kvserver.Store) error {
		now := store.Clock().NowAsClockTimestamp()
		if len(req.RangeIDs) == 0 {
			// All ranges requested.
			store.VisitReplicas(
				func(rep *kvserver.Replica) bool {
					output.Ranges = append(output.Ranges,
						constructRangeInfo(
							rep,
							store.Ident.StoreID,
							rep.Metrics(ctx, now, isLiveMap, clusterNodes),
						))
					return true // continue.
				},
				kvserver.WithReplicasInOrder(),
			)
			return nil
		}

		// Specific ranges requested:
		for _, rid := range req.RangeIDs {
			rep, err := store.GetReplica(rid)
			if err != nil {
				// Not found: continue.
				continue
			}
			output.Ranges = append(output.Ranges,
				constructRangeInfo(
					rep,
					store.Ident.StoreID,
					rep.Metrics(ctx, now, isLiveMap, clusterNodes),
				))
		}
		return nil
	})
	if err != nil {
		return nil, 0, status.Errorf(codes.Internal, err.Error())
	}
	var next int
	if limit > 0 {
		var outputInterface interface{}
		outputInterface, next = simplePaginate(output.Ranges, limit, offset)
		output.Ranges = outputInterface.([]serverpb.RangeInfo)
	}
	return &output, next, nil
}

func (t *statusServer) TenantRanges(
	ctx context.Context, req *serverpb.TenantRangesRequest,
) (*serverpb.TenantRangesResponse, error) {
	ctx = forwardSQLIdentityThroughRPCCalls(ctx)
	ctx = t.AnnotateCtx(ctx)

	// The tenant range report contains replica metadata which is admin-only.
	if _, err := t.privilegeChecker.requireAdminUser(ctx); err != nil {
		return nil, err
	}

	return t.sqlServer.tenantConnect.TenantRanges(ctx, req)
}

func (s *systemStatusServer) TenantRanges(
	ctx context.Context, req *serverpb.TenantRangesRequest,
) (*serverpb.TenantRangesResponse, error) {
	forwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)
	if _, err := s.privilegeChecker.requireAdminUser(ctx); err != nil {
		return nil, err
	}

	tID, ok := roachpb.ClientTenantFromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Internal, "no tenant ID found in context")
	}

	tenantPrefix := keys.MakeTenantPrefix(tID)
	tenantKeySpan := roachpb.Span{
		Key:    tenantPrefix,
		EndKey: tenantPrefix.PrefixEnd(),
	}

	// rangeIDs contains all the `roachpb.RangeID`s found to exist within the
	// tenant's keyspace.
	rangeIDs := make([]roachpb.RangeID, 0)
	// replicaNodeIDs acts as a set of `roachpb.NodeID`'s. These `NodeID`s
	// represent all nodes with a store containing a replica for the tenant.
	replicaNodeIDs := make(map[roachpb.NodeID]struct{})
	if err := s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		rangeKVs, err := kvclient.ScanMetaKVs(ctx, txn, tenantKeySpan)
		if err != nil {
			return err
		}

		for _, rangeKV := range rangeKVs {
			var desc roachpb.RangeDescriptor
			if err := rangeKV.ValueProto(&desc); err != nil {
				return err
			}
			rangeIDs = append(rangeIDs, desc.RangeID)
			for _, rep := range desc.Replicas().Descriptors() {
				_, ok := replicaNodeIDs[rep.NodeID]
				if !ok {
					replicaNodeIDs[rep.NodeID] = struct{}{}
				}
			}
		}
		return nil
	}); err != nil {
		return nil, status.Error(
			codes.Internal,
			errors.Wrap(err, "there was a problem with the initial fetch of range IDs").Error())
	}

	nodeResults := make([][]serverpb.RangeInfo, 0, len(replicaNodeIDs))
	var next int
	for nodeID := range replicaNodeIDs {
		nodeIDString := nodeID.String()
		_, local, err := s.parseNodeID(nodeIDString)
		if err != nil {
			return nil, status.Errorf(codes.Internal, err.Error())
		}

		nodeReq := &serverpb.RangesRequest{
			NodeId:   nodeIDString,
			RangeIDs: rangeIDs,
			Limit:    req.Limit,
			Offset:   req.Offset,
		}

		var resp *serverpb.RangesResponse
		if local {
			resp, next, err = s.rangesHelper(ctx, nodeReq, int(req.Limit), int(req.Offset))
			if err != nil {
				return nil, err
			}
		} else {
			statusServer, err := s.dialNode(ctx, nodeID)
			if err != nil {
				return nil, serverError(ctx, err)
			}

			resp, err = statusServer.Ranges(ctx, nodeReq)
			if err != nil {
				return nil, err
			}
		}

		nodeResults = append(nodeResults, resp.Ranges)
	}

	transformTenantRange := func(
		rep serverpb.RangeInfo,
	) (string, *serverpb.TenantRangeInfo) {
		topKLocksByWaiters := make([]serverpb.TenantRangeInfo_LockInfo, 0, len(rep.TopKLocksByWaitQueueWaiters))
		for _, lm := range rep.TopKLocksByWaitQueueWaiters {
			topKLocksByWaiters = append(topKLocksByWaiters, serverpb.TenantRangeInfo_LockInfo{
				PrettyKey:      lm.Key.String(),
				Key:            lm.Key,
				Held:           lm.Held,
				Waiters:        lm.Waiters,
				WaitingReaders: lm.WaitingReaders,
				WaitingWriters: lm.WaitingWriters,
			})
		}
		azKey := "az"
		localityKey := "locality-unset"
		for _, tier := range rep.Locality.Tiers {
			if tier.Key == azKey {
				localityKey = tier.Value
			}
		}
		return localityKey, &serverpb.TenantRangeInfo{
			RangeID:                     rep.State.Desc.RangeID,
			Span:                        rep.Span,
			Locality:                    rep.Locality,
			IsLeaseholder:               rep.IsLeaseholder,
			LeaseValid:                  rep.LeaseValid,
			RangeStats:                  rep.Stats,
			MvccStats:                   rep.State.Stats,
			ReadLatches:                 rep.ReadLatches,
			WriteLatches:                rep.WriteLatches,
			Locks:                       rep.Locks,
			LocksWithWaitQueues:         rep.LocksWithWaitQueues,
			LockWaitQueueWaiters:        rep.LockWaitQueueWaiters,
			TopKLocksByWaitQueueWaiters: topKLocksByWaiters,
		}
	}

	resp := &serverpb.TenantRangesResponse{
		RangesByLocality: make(map[string]serverpb.TenantRangesResponse_TenantRangeList),
		Next:             int32(next),
	}

	for _, rangeMetas := range nodeResults {
		for _, rangeMeta := range rangeMetas {
			localityKey, rangeInfo := transformTenantRange(rangeMeta)
			rangeList, ok := resp.RangesByLocality[localityKey]
			if !ok {
				rangeList = serverpb.TenantRangesResponse_TenantRangeList{
					Ranges: make([]serverpb.TenantRangeInfo, 0),
				}
			}
			rangeList.Ranges = append(rangeList.Ranges, *rangeInfo)
			resp.RangesByLocality[localityKey] = rangeList
		}
	}

	return resp, nil
}

// HotRanges returns the hottest ranges on each store on the requested node(s).
func (s *systemStatusServer) HotRanges(
	ctx context.Context, req *serverpb.HotRangesRequest,
) (*serverpb.HotRangesResponse, error) {
	ctx = forwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.requireViewClusterMetadataPermission(ctx); err != nil {
		// NB: not using serverError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	response := &serverpb.HotRangesResponse{
		NodeID:            roachpb.NodeID(s.serverIterator.getID()),
		HotRangesByNodeID: make(map[roachpb.NodeID]serverpb.HotRangesResponse_NodeResponse),
	}

	if len(req.NodeID) > 0 {
		requestedNodeID, local, err := s.parseNodeID(req.NodeID)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, err.Error())
		}

		// Only hot ranges from the local node.
		if local {
			response.HotRangesByNodeID[requestedNodeID] = s.localHotRanges(ctx, roachpb.TenantID{})
			return response, nil
		}

		// Only hot ranges from one non-local node.
		status, err := s.dialNode(ctx, requestedNodeID)
		if err != nil {
			return nil, serverError(ctx, err)
		}
		return status.HotRanges(ctx, req)
	}

	// Hot ranges from all nodes.
	dialFn := func(ctx context.Context, nodeID roachpb.NodeID) (interface{}, error) {
		client, err := s.dialNode(ctx, nodeID)
		return client, err
	}
	remoteRequest := serverpb.HotRangesRequest{NodeID: "local"}
	nodeFn := func(ctx context.Context, client interface{}, _ roachpb.NodeID) (interface{}, error) {
		status := client.(serverpb.StatusClient)
		return status.HotRanges(ctx, &remoteRequest)
	}
	responseFn := func(nodeID roachpb.NodeID, resp interface{}) {
		hotRangesResp := resp.(*serverpb.HotRangesResponse)
		response.HotRangesByNodeID[nodeID] = hotRangesResp.HotRangesByNodeID[nodeID]
	}
	errorFn := func(nodeID roachpb.NodeID, err error) {
		response.HotRangesByNodeID[nodeID] = serverpb.HotRangesResponse_NodeResponse{
			ErrorMessage: err.Error(),
		}
	}

	if err := s.iterateNodes(ctx, "hot ranges", dialFn, nodeFn, responseFn, errorFn); err != nil {
		return nil, serverError(ctx, err)
	}

	return response, nil
}

type tableMeta struct {
	dbName     string
	tableName  string
	schemaName string
	indexName  string
}

func (t *statusServer) HotRangesV2(
	ctx context.Context, req *serverpb.HotRangesRequest,
) (*serverpb.HotRangesResponseV2, error) {
	ctx = t.AnnotateCtx(ctx)

	err := t.privilegeChecker.requireViewClusterMetadataPermission(ctx)
	if err != nil {
		return nil, err
	}

	return t.sqlServer.tenantConnect.HotRangesV2(ctx, req)
}

// HotRangesV2 returns hot ranges from all stores on requested node or all nodes for specified tenant
// in case request message doesn't include specific node ID.
func (s *systemStatusServer) HotRangesV2(
	ctx context.Context, req *serverpb.HotRangesRequest,
) (*serverpb.HotRangesResponseV2, error) {
	ctx = s.AnnotateCtx(forwardSQLIdentityThroughRPCCalls(ctx))

	err := s.privilegeChecker.requireViewClusterMetadataPermission(ctx)
	if err != nil {
		return nil, err
	}

	var tenantID roachpb.TenantID
	if len(req.TenantID) > 0 {
		tenantID, err = roachpb.TenantIDFromString(req.TenantID)
		if err != nil {
			return nil, err
		}
	}

	size := int(req.PageSize)
	start := paginationState{}

	if len(req.PageToken) > 0 {
		if err := start.UnmarshalText([]byte(req.PageToken)); err != nil {
			return nil, err
		}
	}

	tableMetaCache := sync.Map{}

	response := &serverpb.HotRangesResponseV2{
		ErrorsByNodeID: make(map[roachpb.NodeID]string),
	}

	var requestedNodes []roachpb.NodeID
	if len(req.NodeID) > 0 {
		requestedNodeID, local, err := s.parseNodeID(req.NodeID)
		if err != nil {
			return nil, err
		}
		if local {
			resp := s.localHotRanges(ctx, tenantID)
			var ranges []*serverpb.HotRangesResponseV2_HotRange
			for _, store := range resp.Stores {
				for _, r := range store.HotRanges {
					var (
						dbName, tableName, indexName, schemaName string
						replicaNodeIDs                           []roachpb.NodeID
					)
					rangeID := uint32(r.Desc.RangeID)
					for _, repl := range r.Desc.Replicas().Descriptors() {
						replicaNodeIDs = append(replicaNodeIDs, repl.NodeID)
					}
					if maybeIndexPrefix, tableID, ok := decodeTableID(s.sqlServer.execCfg.Codec, r.Desc.StartKey.AsRawKey()); !ok {
						dbName = "system"
						tableName = r.Desc.StartKey.String()
					} else if meta, ok := tableMetaCache.Load(rangeID); ok {
						dbName = meta.(tableMeta).dbName
						tableName = meta.(tableMeta).tableName
						schemaName = meta.(tableMeta).schemaName
						indexName = meta.(tableMeta).indexName
					} else {
						if err = s.sqlServer.distSQLServer.DB.DescsTxn(
							ctx, func(ctx context.Context, txn descs.Txn) error {
								col := txn.Descriptors()
								desc, err := col.ByID(txn.KV()).WithoutNonPublic().Get().Table(ctx, descpb.ID(tableID))
								if err != nil {
									return errors.Wrapf(err, "cannot get table descriptor with tableID: %d, %s", tableID, r.Desc)
								}
								tableName = desc.GetName()

								if !maybeIndexPrefix.Equal(roachpb.KeyMin) {
									if _, _, idxID, err := s.sqlServer.execCfg.Codec.DecodeIndexPrefix(r.Desc.StartKey.AsRawKey()); err != nil {
										log.Warningf(ctx, "cannot decode index prefix for range descriptor: %s: %v", r.Desc, err)
									} else {
										if index := catalog.FindIndexByID(desc, descpb.IndexID(idxID)); index == nil {
											log.Warningf(ctx, "cannot get index name for range descriptor: %s: index with ID %d not found", r.Desc, idxID)
										} else {
											indexName = index.GetName()
										}
									}
								}

								if dbDesc, err := col.ByID(txn.KV()).WithoutNonPublic().Get().Database(ctx, desc.GetParentID()); err != nil {
									log.Warningf(ctx, "cannot get database by descriptor ID: %s: %v", r.Desc, err)
								} else {
									dbName = dbDesc.GetName()
								}

								if schemaDesc, err := col.ByID(txn.KV()).WithoutNonPublic().Get().Schema(ctx, desc.GetParentSchemaID()); err != nil {
									log.Warningf(ctx, "cannot get schema name for range descriptor: %s: %v", r.Desc, err)
								} else {
									schemaName = schemaDesc.GetName()
								}
								return nil
							}); err != nil {
							log.Warningf(ctx, "failed to get table info for %s: %v", r.Desc, err)
							continue
						}

						tableMetaCache.Store(rangeID, tableMeta{
							dbName:     dbName,
							tableName:  tableName,
							schemaName: schemaName,
							indexName:  indexName,
						})
					}

					ranges = append(ranges, &serverpb.HotRangesResponseV2_HotRange{
						RangeID:             r.Desc.RangeID,
						NodeID:              requestedNodeID,
						QPS:                 r.QueriesPerSecond,
						WritesPerSecond:     r.WritesPerSecond,
						ReadsPerSecond:      r.ReadsPerSecond,
						WriteBytesPerSecond: r.WriteBytesPerSecond,
						ReadBytesPerSecond:  r.ReadBytesPerSecond,
						CPUTimePerSecond:    r.CPUTimePerSecond,
						TableName:           tableName,
						SchemaName:          schemaName,
						DatabaseName:        dbName,
						IndexName:           indexName,
						ReplicaNodeIds:      replicaNodeIDs,
						LeaseholderNodeID:   r.LeaseholderNodeID,
						StoreID:             store.StoreID,
					})
				}
			}
			response.Ranges = ranges
			response.ErrorsByNodeID[requestedNodeID] = resp.ErrorMessage
			return response, nil
		}
		requestedNodes = []roachpb.NodeID{requestedNodeID}
	}

	dialFn := func(ctx context.Context, nodeID roachpb.NodeID) (interface{}, error) {
		client, err := s.dialNode(ctx, nodeID)
		return client, err
	}
	remoteRequest := serverpb.HotRangesRequest{NodeID: "local", TenantID: req.TenantID}
	nodeFn := func(ctx context.Context, client interface{}, nodeID roachpb.NodeID) (interface{}, error) {
		status := client.(serverpb.StatusClient)
		nodeResp, err := status.HotRangesV2(ctx, &remoteRequest)
		if err != nil {
			return nil, err
		}
		return nodeResp.Ranges, nil
	}
	responseFn := func(nodeID roachpb.NodeID, resp interface{}) {
		if resp == nil {
			return
		}
		hotRanges := resp.([]*serverpb.HotRangesResponseV2_HotRange)
		response.Ranges = append(response.Ranges, hotRanges...)
	}
	errorFn := func(nodeID roachpb.NodeID, err error) {
		response.ErrorsByNodeID[nodeID] = err.Error()
	}

	next, err := s.paginatedIterateNodes(
		ctx, "hotRanges", size, start, requestedNodes, dialFn,
		nodeFn, responseFn, errorFn)

	if err != nil {
		return nil, err
	}
	var nextBytes []byte
	if nextBytes, err = next.MarshalText(); err != nil {
		return nil, err
	}
	response.NextPageToken = string(nextBytes)
	return response, nil
}

func decodeTableID(codec keys.SQLCodec, key roachpb.Key) (roachpb.Key, uint32, bool) {
	remaining, tableID, err := codec.DecodeTablePrefix(key)
	if err != nil {
		return nil, 0, false
	}
	// Validate that tableID doesn't belong to system or pseudo table.
	if key.Equal(roachpb.KeyMin) ||
		tableID <= keys.SystemDatabaseID ||
		keys.IsPseudoTableID(tableID) ||
		bytes.HasPrefix(key, keys.Meta1Prefix) ||
		bytes.HasPrefix(key, keys.Meta2Prefix) ||
		bytes.HasPrefix(key, keys.SystemPrefix) {
		return nil, 0, false
	}
	return remaining, tableID, true
}

func (s *systemStatusServer) localHotRanges(
	ctx context.Context, tenantID roachpb.TenantID,
) serverpb.HotRangesResponse_NodeResponse {
	var resp serverpb.HotRangesResponse_NodeResponse
	err := s.stores.VisitStores(func(store *kvserver.Store) error {
		var ranges []kvserver.HotReplicaInfo
		if tenantID.IsSet() {
			ranges = store.HottestReplicasByTenant(tenantID)
		} else {
			ranges = store.HottestReplicas()
		}
		storeResp := &serverpb.HotRangesResponse_StoreResponse{
			StoreID:   store.StoreID(),
			HotRanges: make([]serverpb.HotRangesResponse_HotRange, len(ranges)),
		}
		for i, r := range ranges {
			replica, err := store.GetReplica(r.Desc.GetRangeID())
			if err == nil {
				storeResp.HotRanges[i].LeaseholderNodeID = replica.State(ctx).Lease.Replica.NodeID
			}
			storeResp.HotRanges[i].Desc = *r.Desc
			storeResp.HotRanges[i].QueriesPerSecond = r.QPS
			storeResp.HotRanges[i].RequestsPerSecond = r.RequestsPerSecond
			storeResp.HotRanges[i].WritesPerSecond = r.WriteKeysPerSecond
			storeResp.HotRanges[i].ReadsPerSecond = r.ReadKeysPerSecond
			storeResp.HotRanges[i].WriteBytesPerSecond = r.WriteBytesPerSecond
			storeResp.HotRanges[i].ReadBytesPerSecond = r.ReadBytesPerSecond
			storeResp.HotRanges[i].CPUTimePerSecond = r.CPUTimePerSecond
		}
		resp.Stores = append(resp.Stores, storeResp)
		return nil
	})
	if err != nil {
		return serverpb.HotRangesResponse_NodeResponse{ErrorMessage: err.Error()}
	}
	return resp
}

func (s *statusServer) KeyVisSamples(
	ctx context.Context, req *serverpb.KeyVisSamplesRequest,
) (*serverpb.KeyVisSamplesResponse, error) {

	if _, err := s.privilegeChecker.requireAdminUser(ctx); err != nil {
		return nil, err
	}

	uniqueKeys, err := keyvisstorage.ReadKeys(ctx, s.internalExecutor)
	if err != nil {
		return nil, err
	}

	prettyForKeyString := make(map[string]string)
	prettyForUUID := make(map[string]string)
	sorted := make([]string, 0)
	sortedPretty := make([]string, 0)

	for keyUUID, keyBytes := range uniqueKeys {
		s := string(keyBytes)
		pretty := keyBytes.String()
		prettyForKeyString[s] = pretty
		sorted = append(sorted, s)
		prettyForUUID[keyUUID] = pretty
	}

	sort.Strings(sorted)
	for _, s := range sorted {
		sortedPretty = append(sortedPretty, prettyForKeyString[s])
	}

	// read samples
	samples, err := keyvisstorage.ReadSamples(ctx, s.internalExecutor)
	if err != nil {
		return nil, err
	}

	return &serverpb.KeyVisSamplesResponse{
		PrettyKeyForUuid: prettyForUUID,
		SortedPrettyKeys: sortedPretty,
		Samples:          samples,
	}, nil
}

// Range returns rangeInfos for all nodes in the cluster about a specific
// range. It also returns the range history for that range as well.
func (s *statusServer) Range(
	ctx context.Context, req *serverpb.RangeRequest,
) (*serverpb.RangeResponse, error) {
	ctx = forwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.requireViewClusterMetadataPermission(ctx); err != nil {
		// NB: not using serverError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	response := &serverpb.RangeResponse{
		RangeID:           roachpb.RangeID(req.RangeId),
		NodeID:            roachpb.NodeID(s.serverIterator.getID()),
		ResponsesByNodeID: make(map[roachpb.NodeID]serverpb.RangeResponse_NodeResponse),
	}

	rangesRequest := &serverpb.RangesRequest{
		RangeIDs: []roachpb.RangeID{roachpb.RangeID(req.RangeId)},
	}

	dialFn := func(ctx context.Context, nodeID roachpb.NodeID) (interface{}, error) {
		client, err := s.dialNode(ctx, nodeID)
		return client, err
	}
	nodeFn := func(ctx context.Context, client interface{}, _ roachpb.NodeID) (interface{}, error) {
		status := client.(serverpb.StatusClient)
		return status.Ranges(ctx, rangesRequest)
	}
	nowNanos := timeutil.Now().UnixNano()
	responseFn := func(nodeID roachpb.NodeID, resp interface{}) {
		rangesResp := resp.(*serverpb.RangesResponse)
		// Age the MVCCStats to a consistent current timestamp. An age that is
		// not up to date is less useful.
		for i := range rangesResp.Ranges {
			rangesResp.Ranges[i].State.Stats.AgeTo(nowNanos)
		}
		response.ResponsesByNodeID[nodeID] = serverpb.RangeResponse_NodeResponse{
			Response: true,
			Infos:    rangesResp.Ranges,
		}
	}
	errorFn := func(nodeID roachpb.NodeID, err error) {
		response.ResponsesByNodeID[nodeID] = serverpb.RangeResponse_NodeResponse{
			ErrorMessage: err.Error(),
		}
	}

	if err := s.iterateNodes(
		ctx, fmt.Sprintf("details about range %d", req.RangeId), dialFn, nodeFn, responseFn, errorFn,
	); err != nil {
		return nil, serverError(ctx, err)
	}
	return response, nil
}

// ListLocalSessions returns a list of SQL sessions on this node.
func (s *statusServer) ListLocalSessions(
	ctx context.Context, req *serverpb.ListSessionsRequest,
) (*serverpb.ListSessionsResponse, error) {
	sessions, err := s.getLocalSessions(ctx, req)
	if err != nil {
		// NB: not using serverError() here since getLocalSessions
		// already returns a proper gRPC error status.
		return nil, err
	}
	for i := range sessions {
		sessions[i].NodeID = roachpb.NodeID(s.serverIterator.getID())
	}
	return &serverpb.ListSessionsResponse{Sessions: sessions}, nil
}

// iterateNodes iterates nodeFn over all non-removed nodes concurrently.
// It then calls nodeResponse for every valid result of nodeFn, and
// nodeError on every error result.
func (s *statusServer) iterateNodes(
	ctx context.Context,
	errorCtx string,
	dialFn func(ctx context.Context, nodeID roachpb.NodeID) (interface{}, error),
	nodeFn func(ctx context.Context, client interface{}, nodeID roachpb.NodeID) (interface{}, error),
	responseFn func(nodeID roachpb.NodeID, resp interface{}),
	errorFn func(nodeID roachpb.NodeID, nodeFnError error),
) error {
	nodeStatuses, err := s.serverIterator.getAllNodes(ctx)
	if err != nil {
		return err
	}

	// channels for responses and errors.
	type nodeResponse struct {
		nodeID   roachpb.NodeID
		response interface{}
		err      error
	}

	numNodes := len(nodeStatuses)
	responseChan := make(chan nodeResponse, numNodes)

	nodeQuery := func(ctx context.Context, nodeID roachpb.NodeID) {
		var client interface{}
		err := contextutil.RunWithTimeout(ctx, "dial node", base.DialTimeout, func(ctx context.Context) error {
			var err error
			client, err = dialFn(ctx, nodeID)
			return err
		})
		if err != nil {
			err = errors.Wrapf(err, "failed to dial into node %d (%s)",
				nodeID, nodeStatuses[serverID(nodeID)])
			responseChan <- nodeResponse{nodeID: nodeID, err: err}
			return
		}

		res, err := nodeFn(ctx, client, nodeID)
		if err != nil {
			err = errors.Wrapf(err, "error requesting %s from node %d (%s)",
				errorCtx, nodeID, nodeStatuses[serverID(nodeID)])
		}
		responseChan <- nodeResponse{nodeID: nodeID, response: res, err: err}
	}

	// Issue the requests concurrently.
	sem := quotapool.NewIntPool("node status", maxConcurrentRequests)
	ctx, cancel := s.stopper.WithCancelOnQuiesce(ctx)
	defer cancel()
	for nodeID := range nodeStatuses {
		nodeID := nodeID // needed to ensure the closure below captures a copy.
		if err := s.stopper.RunAsyncTaskEx(
			ctx,
			stop.TaskOpts{
				TaskName:   fmt.Sprintf("server.statusServer: requesting %s", errorCtx),
				Sem:        sem,
				WaitForSem: true,
			},
			func(ctx context.Context) { nodeQuery(ctx, roachpb.NodeID(nodeID)) },
		); err != nil {
			return err
		}
	}

	var resultErr error
	for numNodes > 0 {
		select {
		case res := <-responseChan:
			if res.err != nil {
				errorFn(res.nodeID, res.err)
			} else {
				responseFn(res.nodeID, res.response)
			}
		case <-ctx.Done():
			resultErr = errors.Errorf("request of %s canceled before completion", errorCtx)
		}
		numNodes--
	}
	return resultErr
}

// paginatedIterateNodes iterates nodeFn over all non-removed nodes
// sequentially.  It then calls nodeResponse for every valid result of nodeFn,
// and nodeError on every error result. It returns the next `limit` results
// after `start`. If `requestedNodes` is specified and non-empty, iteration is
// only done on that subset of nodes in addition to any nodes already in pagState.
func (s *statusServer) paginatedIterateNodes(
	ctx context.Context,
	errorCtx string,
	limit int,
	pagState paginationState,
	requestedNodes []roachpb.NodeID,
	dialFn func(ctx context.Context, nodeID roachpb.NodeID) (interface{}, error),
	nodeFn func(ctx context.Context, client interface{}, nodeID roachpb.NodeID) (interface{}, error),
	responseFn func(nodeID roachpb.NodeID, resp interface{}),
	errorFn func(nodeID roachpb.NodeID, nodeFnError error),
) (next paginationState, err error) {
	if limit == 0 {
		return paginationState{}, s.iterateNodes(ctx, errorCtx, dialFn, nodeFn, responseFn, errorFn)
	}
	nodeStatuses, err := s.serverIterator.getAllNodes(ctx)
	if err != nil {
		return paginationState{}, err
	}

	numNodes := len(nodeStatuses)
	nodeIDs := make([]roachpb.NodeID, 0, numNodes)
	if len(requestedNodes) > 0 {
		nodeIDs = append(nodeIDs, requestedNodes...)
	} else {
		for nodeID := range nodeStatuses {
			nodeIDs = append(nodeIDs, roachpb.NodeID(nodeID))
		}
	}
	// Sort all nodes by IDs, as this is what mergeNodeIDs expects.
	sort.Slice(nodeIDs, func(i, j int) bool {
		return nodeIDs[i] < nodeIDs[j]
	})
	pagState.mergeNodeIDs(nodeIDs)
	// Remove any node that have already been queried.
	nodeIDs = nodeIDs[:0]
	if pagState.inProgress != 0 {
		nodeIDs = append(nodeIDs, pagState.inProgress)
	}
	nodeIDs = append(nodeIDs, pagState.nodesToQuery...)

	paginator := &rpcNodePaginator{
		limit:        limit,
		numNodes:     len(nodeIDs),
		errorCtx:     errorCtx,
		pagState:     pagState,
		nodeStatuses: nodeStatuses,
		dialFn:       dialFn,
		nodeFn:       nodeFn,
		responseFn:   responseFn,
		errorFn:      errorFn,
	}

	paginator.init()
	// Issue the requests concurrently.
	sem := quotapool.NewIntPool("node status", maxConcurrentPaginatedRequests)
	ctx, cancel := s.stopper.WithCancelOnQuiesce(ctx)
	defer cancel()
	for idx, nodeID := range nodeIDs {
		nodeID := nodeID // needed to ensure the closure below captures a copy.
		idx := idx
		if err := s.stopper.RunAsyncTaskEx(
			ctx,
			stop.TaskOpts{
				TaskName:   fmt.Sprintf("server.statusServer: requesting %s", errorCtx),
				Sem:        sem,
				WaitForSem: true,
			},
			func(ctx context.Context) { paginator.queryNode(ctx, nodeID, idx) },
		); err != nil {
			return pagState, err
		}
	}

	return paginator.processResponses(ctx)
}

func (s *statusServer) listSessionsHelper(
	ctx context.Context, req *serverpb.ListSessionsRequest, limit int, start paginationState,
) (*serverpb.ListSessionsResponse, paginationState, error) {
	response := &serverpb.ListSessionsResponse{
		Sessions:              make([]serverpb.Session, 0),
		Errors:                make([]serverpb.ListSessionsError, 0),
		InternalAppNamePrefix: catconstants.InternalAppNamePrefix,
	}

	dialFn := func(ctx context.Context, nodeID roachpb.NodeID) (interface{}, error) {
		client, err := s.dialNode(ctx, nodeID)
		return client, err
	}
	nodeFn := func(ctx context.Context, client interface{}, _ roachpb.NodeID) (interface{}, error) {
		statusClient := client.(serverpb.StatusClient)
		resp, err := statusClient.ListLocalSessions(ctx, req)
		if resp != nil && err == nil {
			if len(resp.Errors) > 0 {
				return nil, errors.Errorf("%s", resp.Errors[0].Message)
			}
			sort.Slice(resp.Sessions, func(i, j int) bool {
				return resp.Sessions[i].Start.Before(resp.Sessions[j].Start)
			})
			return resp.Sessions, nil
		}
		return nil, err
	}
	responseFn := func(_ roachpb.NodeID, nodeResp interface{}) {
		if nodeResp == nil {
			return
		}
		sessions := nodeResp.([]serverpb.Session)
		response.Sessions = append(response.Sessions, sessions...)
		sort.Slice(response.Sessions, func(i, j int) bool {
			return response.Sessions[i].Start.Before(response.Sessions[j].Start)
		})
	}
	errorFn := func(nodeID roachpb.NodeID, err error) {
		errResponse := serverpb.ListSessionsError{NodeID: nodeID, Message: err.Error()}
		response.Errors = append(response.Errors, errResponse)
	}

	var err error
	var pagState paginationState
	if pagState, err = s.paginatedIterateNodes(
		ctx, "session list", limit, start, nil, dialFn, nodeFn, responseFn, errorFn); err != nil {
		err := serverpb.ListSessionsError{Message: err.Error()}
		response.Errors = append(response.Errors, err)
	}
	return response, pagState, nil
}

// ListSessions returns a list of SQL sessions on all nodes in the cluster.
func (s *statusServer) ListSessions(
	ctx context.Context, req *serverpb.ListSessionsRequest,
) (*serverpb.ListSessionsResponse, error) {
	ctx = forwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, _, err := s.privilegeChecker.getUserAndRole(ctx); err != nil {
		// NB: not using serverError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	resp, _, err := s.listSessionsHelper(ctx, req, 0 /* limit */, paginationState{})
	if err != nil {
		return nil, serverError(ctx, err)
	}
	return resp, nil
}

// CancelSession responds to a session cancellation request by canceling the
// target session's associated context.
func (s *statusServer) CancelSession(
	ctx context.Context, req *serverpb.CancelSessionRequest,
) (*serverpb.CancelSessionResponse, error) {
	ctx = forwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	sessionIDBytes := req.SessionID
	if len(sessionIDBytes) != 16 {
		return &serverpb.CancelSessionResponse{
			Error: fmt.Sprintf("session ID %v malformed", sessionIDBytes),
		}, nil
	}
	sessionID := clusterunique.IDFromBytes(sessionIDBytes)
	nodeID := sessionID.GetNodeID()
	local := nodeID == int32(s.serverIterator.getID())
	if !local {
		status, err := s.dialNode(ctx, roachpb.NodeID(nodeID))
		if err != nil {
			if errors.Is(err, sqlinstance.NonExistentInstanceError) {
				return &serverpb.CancelSessionResponse{
					Error: fmt.Sprintf("session ID %s not found", sessionID),
				}, nil
			}
			return nil, serverError(ctx, err)
		}
		return status.CancelSession(ctx, req)
	}

	reqUsername, err := username.MakeSQLUsernameFromPreNormalizedStringChecked(req.Username)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	session, ok := s.sessionRegistry.GetSessionByID(sessionID)
	if !ok {
		return &serverpb.CancelSessionResponse{
			Error: fmt.Sprintf("session ID %s not found", sessionID),
		}, nil
	}

	if err := s.checkCancelPrivilege(ctx, reqUsername, session.SessionUser()); err != nil {
		// NB: not using serverError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	session.CancelSession()
	return &serverpb.CancelSessionResponse{Canceled: true}, nil
}

// CancelQuery responds to a query cancellation request, and cancels
// the target query's associated context and sets a cancellation flag.
func (s *statusServer) CancelQuery(
	ctx context.Context, req *serverpb.CancelQueryRequest,
) (*serverpb.CancelQueryResponse, error) {
	ctx = forwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	queryID, err := clusterunique.IDFromString(req.QueryID)
	if err != nil {
		return &serverpb.CancelQueryResponse{
			Error: errors.Wrapf(err, "query ID %s malformed", queryID).Error(),
		}, nil
	}

	nodeID := queryID.GetNodeID()
	local := nodeID == int32(s.serverIterator.getID())
	if !local {
		// This request needs to be forwarded to another node.
		status, err := s.dialNode(ctx, roachpb.NodeID(nodeID))
		if err != nil {
			if errors.Is(err, sqlinstance.NonExistentInstanceError) {
				return &serverpb.CancelQueryResponse{
					Error: fmt.Sprintf("query ID %s not found", queryID),
				}, nil
			}
			return nil, serverError(ctx, err)
		}
		return status.CancelQuery(ctx, req)
	}

	reqUsername, err := username.MakeSQLUsernameFromPreNormalizedStringChecked(req.Username)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	session, ok := s.sessionRegistry.GetSessionByQueryID(queryID)
	if !ok {
		return &serverpb.CancelQueryResponse{
			Error: fmt.Sprintf("query ID %s not found", queryID),
		}, nil
	}

	if err := s.checkCancelPrivilege(ctx, reqUsername, session.SessionUser()); err != nil {
		// NB: not using serverError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	isCanceled := session.CancelQuery(queryID)
	return &serverpb.CancelQueryResponse{
		Canceled: isCanceled,
	}, nil
}

// CancelQueryByKey responds to a pgwire query cancellation request, and cancels
// the target query's associated context and sets a cancellation flag. This
// endpoint is rate-limited by a semaphore.
func (s *statusServer) CancelQueryByKey(
	ctx context.Context, req *serverpb.CancelQueryByKeyRequest,
) (resp *serverpb.CancelQueryByKeyResponse, retErr error) {
	local := req.SQLInstanceID == s.sqlServer.SQLInstanceID()

	// Acquiring the semaphore here helps protect both the source and destination
	// nodes. The source node is protected against an attacker causing too much
	// inter-node network traffic by spamming cancel requests. The destination
	// node is protected so that if an attacker spams all the nodes in the cluster
	// with requests that all go to the same node, this semaphore will prevent
	// them from having too many guesses.
	// More concretely, suppose we have a 100-node cluster. If we limit the
	// ingress only, then each node is limited to processing X requests per
	// second. But if an attacker crafts the CancelRequests to all target the
	// same SQLInstance, then that one instance would have to process 100*X
	// requests per second.
	alloc, err := s.cancelSemaphore.TryAcquire(ctx, 1)
	if err != nil {
		return nil, status.Errorf(codes.ResourceExhausted, "exceeded rate limit of pgwire cancellation requests")
	}
	defer func() {
		// If we acquired the semaphore but the cancellation request failed, then
		// hold on to the semaphore for longer. This helps mitigate a DoS attack
		// of random cancellation requests.
		if err != nil || (resp != nil && !resp.Canceled) {
			time.Sleep(1 * time.Second)
		}
		alloc.Release()
	}()

	if local {
		cancelQueryKey := req.CancelQueryKey
		session, ok := s.sessionRegistry.GetSessionByCancelKey(cancelQueryKey)
		if !ok {
			return &serverpb.CancelQueryByKeyResponse{
				Error: fmt.Sprintf("session for cancel key %d not found", cancelQueryKey),
			}, nil
		}

		isCanceled := session.CancelActiveQueries()
		return &serverpb.CancelQueryByKeyResponse{
			Canceled: isCanceled,
		}, nil
	}

	// This request needs to be forwarded to another node.
	ctx = forwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)
	client, err := s.dialNode(ctx, roachpb.NodeID(req.SQLInstanceID))
	if err != nil {
		return nil, err
	}
	return client.CancelQueryByKey(ctx, req)
}

// ListContentionEvents returns a list of contention events on all nodes in the
// cluster.
func (s *statusServer) ListContentionEvents(
	ctx context.Context, req *serverpb.ListContentionEventsRequest,
) (*serverpb.ListContentionEventsResponse, error) {
	ctx = forwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	// Check permissions early to avoid fan-out to all nodes.
	if err := s.privilegeChecker.requireViewActivityOrViewActivityRedactedPermission(ctx); err != nil {
		// NB: not using serverError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	var response serverpb.ListContentionEventsResponse
	dialFn := func(ctx context.Context, nodeID roachpb.NodeID) (interface{}, error) {
		client, err := s.dialNode(ctx, nodeID)
		return client, err
	}
	nodeFn := func(ctx context.Context, client interface{}, _ roachpb.NodeID) (interface{}, error) {
		statusClient := client.(serverpb.StatusClient)
		resp, err := statusClient.ListLocalContentionEvents(ctx, req)
		if err != nil {
			return nil, err
		}
		if len(resp.Errors) > 0 {
			return nil, errors.Errorf("%s", resp.Errors[0].Message)
		}
		return resp, nil
	}
	responseFn := func(_ roachpb.NodeID, nodeResp interface{}) {
		if nodeResp == nil {
			return
		}
		events := nodeResp.(*serverpb.ListContentionEventsResponse).Events
		response.Events = contention.MergeSerializedRegistries(response.Events, events)
	}
	errorFn := func(nodeID roachpb.NodeID, err error) {
		errResponse := serverpb.ListActivityError{NodeID: nodeID, Message: err.Error()}
		response.Errors = append(response.Errors, errResponse)
	}

	if err := s.iterateNodes(ctx, "contention events list", dialFn, nodeFn, responseFn, errorFn); err != nil {
		return nil, serverError(ctx, err)
	}
	return &response, nil
}

func (s *statusServer) ListDistSQLFlows(
	ctx context.Context, request *serverpb.ListDistSQLFlowsRequest,
) (*serverpb.ListDistSQLFlowsResponse, error) {
	ctx = forwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	// Check permissions early to avoid fan-out to all nodes.
	if err := s.privilegeChecker.requireViewActivityOrViewActivityRedactedPermission(ctx); err != nil {
		// NB: not using serverError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	var response serverpb.ListDistSQLFlowsResponse
	dialFn := func(ctx context.Context, nodeID roachpb.NodeID) (interface{}, error) {
		client, err := s.dialNode(ctx, nodeID)
		return client, err
	}
	nodeFn := func(ctx context.Context, client interface{}, _ roachpb.NodeID) (interface{}, error) {
		statusClient := client.(serverpb.StatusClient)
		resp, err := statusClient.ListLocalDistSQLFlows(ctx, request)
		if err != nil {
			return nil, err
		}
		if len(resp.Errors) > 0 {
			return nil, errors.Errorf("%s", resp.Errors[0].Message)
		}
		return resp, nil
	}
	responseFn := func(_ roachpb.NodeID, nodeResp interface{}) {
		if nodeResp == nil {
			return
		}
		flows := nodeResp.(*serverpb.ListDistSQLFlowsResponse).Flows
		response.Flows = mergeDistSQLRemoteFlows(response.Flows, flows)
	}
	errorFn := func(nodeID roachpb.NodeID, err error) {
		errResponse := serverpb.ListActivityError{NodeID: nodeID, Message: err.Error()}
		response.Errors = append(response.Errors, errResponse)
	}

	if err := s.iterateNodes(ctx, "distsql flows list", dialFn, nodeFn, responseFn, errorFn); err != nil {
		return nil, serverError(ctx, err)
	}
	return &response, nil
}

// mergeDistSQLRemoteFlows takes in two slices of DistSQL remote flows (that
// satisfy the contract of serverpb.ListDistSQLFlowsResponse) and merges them
// together while adhering to the same contract.
//
// It is assumed that if serverpb.DistSQLRemoteFlows for a particular FlowID
// appear in both arguments - let's call them flowsA and flowsB for a and b,
// respectively - then there are no duplicate NodeIDs among flowsA and flowsB.
func mergeDistSQLRemoteFlows(a, b []serverpb.DistSQLRemoteFlows) []serverpb.DistSQLRemoteFlows {
	maxLength := len(a)
	if len(b) > len(a) {
		maxLength = len(b)
	}
	result := make([]serverpb.DistSQLRemoteFlows, 0, maxLength)
	aIter, bIter := 0, 0
	for aIter < len(a) && bIter < len(b) {
		cmp := bytes.Compare(a[aIter].FlowID.GetBytes(), b[bIter].FlowID.GetBytes())
		if cmp < 0 {
			result = append(result, a[aIter])
			aIter++
		} else if cmp > 0 {
			result = append(result, b[bIter])
			bIter++
		} else {
			r := a[aIter]
			// No need to perform any kind of de-duplication because a
			// particular flow will be reported at most once by each node in the
			// cluster.
			r.Infos = append(r.Infos, b[bIter].Infos...)
			sort.Slice(r.Infos, func(i, j int) bool {
				return r.Infos[i].NodeID < r.Infos[j].NodeID
			})
			result = append(result, r)
			aIter++
			bIter++
		}
	}
	if aIter < len(a) {
		result = append(result, a[aIter:]...)
	}
	if bIter < len(b) {
		result = append(result, b[bIter:]...)
	}
	return result
}

func (s *statusServer) ListExecutionInsights(
	ctx context.Context, req *serverpb.ListExecutionInsightsRequest,
) (*serverpb.ListExecutionInsightsResponse, error) {
	ctx = forwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	// Check permissions early to avoid fan-out to all nodes.
	if err := s.privilegeChecker.requireViewActivityOrViewActivityRedactedPermission(ctx); err != nil {
		// NB: not using serverError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	localRequest := serverpb.ListExecutionInsightsRequest{NodeID: "local"}

	if len(req.NodeID) > 0 {
		requestedNodeID, local, err := s.parseNodeID(req.NodeID)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, err.Error())
		}
		if local {
			return s.localExecutionInsights(ctx)
		}
		statusClient, err := s.dialNode(ctx, requestedNodeID)
		if err != nil {
			return nil, serverError(ctx, err)
		}
		return statusClient.ListExecutionInsights(ctx, &localRequest)
	}

	var response serverpb.ListExecutionInsightsResponse

	dialFn := func(ctx context.Context, nodeID roachpb.NodeID) (interface{}, error) {
		return s.dialNode(ctx, nodeID)
	}
	nodeFn := func(ctx context.Context, client interface{}, nodeID roachpb.NodeID) (interface{}, error) {
		statusClient := client.(serverpb.StatusClient)
		resp, err := statusClient.ListExecutionInsights(ctx, &localRequest)
		if err != nil {
			return nil, err
		}
		return resp, nil
	}
	responseFn := func(nodeID roachpb.NodeID, nodeResponse interface{}) {
		if nodeResponse == nil {
			return
		}
		insightsResponse := nodeResponse.(*serverpb.ListExecutionInsightsResponse)
		response.Insights = append(response.Insights, insightsResponse.Insights...)
	}
	errorFn := func(nodeID roachpb.NodeID, err error) {
		response.Errors = append(response.Errors, errors.EncodeError(ctx, err))
	}

	if err := s.iterateNodes(ctx, "execution insights list", dialFn, nodeFn, responseFn, errorFn); err != nil {
		return nil, serverError(ctx, err)
	}
	return &response, nil
}

// SpanStats requests the total statistics stored on a node for a given key
// span, which may include multiple ranges.
func (s *statusServer) SpanStats(
	ctx context.Context, req *roachpb.SpanStatsRequest,
) (*roachpb.SpanStatsResponse, error) {
	ctx = s.AnnotateCtx(ctx)
	if _, err := s.privilegeChecker.requireAdminUser(ctx); err != nil {
		// NB: not using serverError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}
	return s.sqlServer.tenantConnect.SpanStats(ctx, req)
}

func (s *systemStatusServer) SpanStats(
	ctx context.Context, req *roachpb.SpanStatsRequest,
) (*roachpb.SpanStatsResponse, error) {
	ctx = forwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)
	if _, err := s.privilegeChecker.requireAdminUser(ctx); err != nil {
		// NB: not using serverError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	// If the cluster's active version is less than 23.1 return a mixed version error.
	if !s.st.Version.IsActive(ctx, clusterversion.V23_1) {
		return nil, errors.New(MixedVersionErr)
	}

	// If we receive a request using the old format.
	if isLegacyRequest(req) {
		// We want to force 23.1 callers to use the new format (e.g. Spans field).
		if req.NodeID == "0" {
			return nil, errors.New(UnexpectedLegacyRequest)
		}
		// We want to error if we receive a legacy request from a 22.2
		// node (e.g. during a mixed-version fanout).
		return nil, errors.New(MixedVersionErr)
	}
	if len(req.Spans) > int(roachpb.SpanStatsBatchLimit.Get(&s.st.SV)) {
		return nil, errors.Newf(exceedSpanLimitPlaceholder, len(req.Spans), int(roachpb.SpanStatsBatchLimit.Get(&s.st.SV)))
	}

	return s.getSpanStatsInternal(ctx, req)
}

// Diagnostics returns an anonymized diagnostics report.
func (s *statusServer) Diagnostics(
	ctx context.Context, req *serverpb.DiagnosticsRequest,
) (*diagnosticspb.DiagnosticReport, error) {
	ctx = forwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)
	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if !local {
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, serverError(ctx, err)
		}
		return status.Diagnostics(ctx, req)
	}

	return s.sqlServer.diagnosticsReporter.CreateReport(ctx, telemetry.ReadOnly), nil
}

// Stores returns details for each store.
func (s *systemStatusServer) Stores(
	ctx context.Context, req *serverpb.StoresRequest,
) (*serverpb.StoresResponse, error) {
	ctx = forwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.requireViewClusterMetadataPermission(ctx); err != nil {
		// NB: not using serverError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if !local {
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, serverError(ctx, err)
		}
		return status.Stores(ctx, req)
	}

	resp := &serverpb.StoresResponse{}
	err = s.stores.VisitStores(func(store *kvserver.Store) error {
		storeDetails := serverpb.StoreDetails{
			StoreID: store.Ident.StoreID,
		}

		envStats, err := store.TODOEngine().GetEnvStats()
		if err != nil {
			return err
		}

		if len(envStats.EncryptionStatus) > 0 {
			storeDetails.EncryptionStatus = envStats.EncryptionStatus
		}
		storeDetails.TotalFiles = envStats.TotalFiles
		storeDetails.TotalBytes = envStats.TotalBytes
		storeDetails.ActiveKeyFiles = envStats.ActiveKeyFiles
		storeDetails.ActiveKeyBytes = envStats.ActiveKeyBytes

		resp.Stores = append(resp.Stores, storeDetails)

		return nil
	})
	if err != nil {
		return nil, serverError(ctx, err)
	}
	return resp, nil
}

// jsonWrapper provides a wrapper on any slice data type being
// marshaled to JSON. This prevents a security vulnerability
// where a phishing attack can trick a user's browser into
// requesting a document from Cockroach as an executable script,
// allowing the contents of the fetched document to be treated
// as executable javascript. More details here:
// http://haacked.com/archive/2009/06/25/json-hijacking.aspx/
type jsonWrapper struct {
	Data interface{} `json:"d"`
}

// marshalToJSON marshals the given value into nicely indented JSON. If the
// value is an array or slice it is wrapped in jsonWrapper and then marshaled.
func marshalToJSON(value interface{}) ([]byte, error) {
	switch reflect.ValueOf(value).Kind() {
	case reflect.Array, reflect.Slice:
		value = jsonWrapper{Data: value}
	}
	body, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return nil, errors.Wrapf(err, "unable to marshal %+v to json", value)
	}
	return body, nil
}

// marshalJSONResponse converts an arbitrary value into a JSONResponse protobuf
// that can be sent via grpc.
func marshalJSONResponse(value interface{}) (*serverpb.JSONResponse, error) {
	data, err := marshalToJSON(value)
	if err != nil {
		return nil, err
	}
	return &serverpb.JSONResponse{Data: data}, nil
}

type systemInfoOnce struct {
	once sync.Once
	info serverpb.SystemInfo
}

func (si *systemInfoOnce) systemInfo(ctx context.Context) serverpb.SystemInfo {
	// We only want to attempt to populate the si.info once. If an error occurs
	// it is logged but the corresponding field in the returned struct is just
	// left empty as there isn't anything to do with an error from this function.
	si.once.Do(func() {
		// Don't use CommandContext because uname ought to not run for too long and
		// if the status request were canceled, we don't want future requests to
		// get blank information because we bailed out of this Once.
		cmd := exec.Command("uname", "-a")
		var errBuf bytes.Buffer
		cmd.Stderr = &errBuf
		output, err := cmd.Output()
		if err != nil {
			log.Warningf(ctx, "failed to get system information: %v\nstderr: %v",
				err, errBuf.String())
			return
		}
		si.info.SystemInfo = string(bytes.TrimSpace(output))
		cmd = exec.Command("uname", "-r")
		errBuf.Reset()
		cmd.Stderr = &errBuf
		output, err = cmd.Output()
		if err != nil {
			log.Warningf(ctx, "failed to get kernel information: %v\nstderr: %v",
				err, errBuf.String())
			return
		}
		si.info.KernelInfo = string(bytes.TrimSpace(output))
	})
	return si.info
}

// JobRegistryStatus returns details about the jobs running on the registry at a
// particular node.
func (s *statusServer) JobRegistryStatus(
	ctx context.Context, req *serverpb.JobRegistryStatusRequest,
) (*serverpb.JobRegistryStatusResponse, error) {
	ctx = forwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, err := s.privilegeChecker.requireAdminUser(ctx); err != nil {
		// NB: not using serverError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	if !local {
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, serverError(ctx, err)
		}
		return status.JobRegistryStatus(ctx, req)
	}

	remoteNodeID := roachpb.NodeID(s.serverIterator.getID())
	resp := &serverpb.JobRegistryStatusResponse{
		NodeID: remoteNodeID,
	}
	for _, jID := range s.sqlServer.jobRegistry.CurrentlyRunningJobs() {
		job := serverpb.JobRegistryStatusResponse_Job{
			Id: int64(jID),
		}
		resp.RunningJobs = append(resp.RunningJobs, &job)
	}
	return resp, nil
}

// JobStatus returns details about the jobs running on the registry at a
// particular node.
func (s *statusServer) JobStatus(
	ctx context.Context, req *serverpb.JobStatusRequest,
) (*serverpb.JobStatusResponse, error) {
	ctx = s.AnnotateCtx(forwardSQLIdentityThroughRPCCalls(ctx))

	if _, err := s.privilegeChecker.requireAdminUser(ctx); err != nil {
		// NB: not using serverError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	j, err := s.sqlServer.jobRegistry.LoadJob(ctx, jobspb.JobID(req.JobId))
	if err != nil {
		if je := (*jobs.JobNotFoundError)(nil); errors.As(err, &je) {
			return nil, status.Errorf(codes.NotFound, "%v", err)
		}
		return nil, serverError(ctx, err)
	}
	res := &jobspb.Job{
		Payload:  &jobspb.Payload{},
		Progress: &jobspb.Progress{},
	}
	res.Id = j.ID()
	// j is not escaping this function and hence is immutable so a shallow copy
	// is fine. Also we can't really clone the field Payload as it may contain
	// types that are not supported by Clone().
	// See https://github.com/cockroachdb/cockroach/issues/46049.
	*res.Payload = j.Payload()
	*res.Progress = j.Progress()

	return &serverpb.JobStatusResponse{Job: res}, nil
}

func (s *statusServer) TxnIDResolution(
	ctx context.Context, req *serverpb.TxnIDResolutionRequest,
) (*serverpb.TxnIDResolutionResponse, error) {
	ctx = s.AnnotateCtx(forwardSQLIdentityThroughRPCCalls(ctx))
	if _, err := s.privilegeChecker.requireAdminUser(ctx); err != nil {
		return nil, err
	}

	requestedNodeID, local, err := s.parseNodeID(req.CoordinatorID)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	if local {
		return s.localTxnIDResolution(req), nil
	}

	statusClient, err := s.dialNode(ctx, requestedNodeID)
	if err != nil {
		return nil, err
	}

	return statusClient.TxnIDResolution(ctx, req)
}

func (s *statusServer) TransactionContentionEvents(
	ctx context.Context, req *serverpb.TransactionContentionEventsRequest,
) (*serverpb.TransactionContentionEventsResponse, error) {
	ctx = s.AnnotateCtx(forwardSQLIdentityThroughRPCCalls(ctx))

	if err := s.privilegeChecker.requireViewActivityOrViewActivityRedactedPermission(ctx); err != nil {
		return nil, err
	}

	user, isAdmin, err := s.privilegeChecker.getUserAndRole(ctx)
	if err != nil {
		return nil, serverError(ctx, err)
	}

	shouldRedactContendingKey := false
	if !isAdmin {
		shouldRedactContendingKey, err =
			s.privilegeChecker.hasRoleOption(ctx, user, roleoption.VIEWACTIVITYREDACTED)
		if err != nil {
			return nil, serverError(ctx, err)
		}
	}

	if roachpb.NodeID(s.serverIterator.getID()) == 0 {
		return nil, status.Errorf(codes.Unavailable, "nodeID not set")
	}

	if len(req.NodeID) > 0 {
		requestedNodeID, local, err := s.parseNodeID(req.NodeID)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, err.Error())
		}
		if local {
			return s.localTransactionContentionEvents(shouldRedactContendingKey), nil
		}

		statusClient, err := s.dialNode(ctx, requestedNodeID)
		if err != nil {
			return nil, err
		}
		return statusClient.TransactionContentionEvents(ctx, req)
	}

	dialFn := func(ctx context.Context, nodeID roachpb.NodeID) (interface{}, error) {
		statusClient, err := s.dialNode(ctx, nodeID)
		return statusClient, err
	}

	rpcCallFn := func(ctx context.Context, client interface{}, _ roachpb.NodeID) (interface{}, error) {
		statusClient := client.(serverpb.StatusClient)
		return statusClient.TransactionContentionEvents(ctx, &serverpb.TransactionContentionEventsRequest{
			NodeID: "local",
		})
	}

	resp := &serverpb.TransactionContentionEventsResponse{
		Events: make([]contentionpb.ExtendedContentionEvent, 0),
	}

	if err := s.iterateNodes(ctx, "txn contention events for node",
		dialFn,
		rpcCallFn,
		func(nodeID roachpb.NodeID, nodeResp interface{}) {
			txnContentionEvents := nodeResp.(*serverpb.TransactionContentionEventsResponse)
			resp.Events = append(resp.Events, txnContentionEvents.Events...)
		},
		func(nodeID roachpb.NodeID, nodeFnError error) {
		},
	); err != nil {
		return nil, err
	}

	sort.Slice(resp.Events, func(i, j int) bool {
		return resp.Events[i].CollectionTs.Before(resp.Events[j].CollectionTs)
	})

	return resp, nil
}
