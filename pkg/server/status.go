// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/keyvisstorage"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangestats"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	raft "github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/apiconstants"
	"github.com/cockroachdb/cockroach/pkg/server/apiutil"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/debug"
	"github.com/cockroachdb/cockroach/pkg/server/diagnostics/diagnosticspb"
	"github.com/cockroachdb/cockroach/pkg/server/privchecker"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/contention"
	"github.com/cockroachdb/cockroach/pkg/sql/contentionpb"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/insights"
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
	"github.com/cockroachdb/redact"
	"github.com/google/pprof/profile"
	gwruntime "github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/prometheus/common/expfmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// Default Maximum number of log entries returned.
	defaultMaxLogEntries = 1000

	// RaftStateDormant is used when there is no known raft state.
	RaftStateDormant = "StateDormant"
)

var (
	// Pattern for local used when determining the node ID.
	localRE = regexp.MustCompile(`(?i)local`)

	// Counter to count accesses to the prometheus vars endpoint /_status/vars .
	telemetryPrometheusVars = telemetry.GetCounterOnce("monitoring.prometheus.vars")

	// Counter to count accesses to the health check endpoint /health .
	telemetryHealthCheck = telemetry.GetCounterOnce("monitoring.health.details")

	// redactedMarker is redacted marker string for fields to be redacted in API response
	redactedMarker = string(redact.RedactedMarker())
)

const (
	updateTableMetadataCachePermissionErrMsg = "only admin users can trigger table metadata cache updates"
)

type metricMarshaler interface {
	json.Marshaler
	PrintAsText(io.Writer, expfmt.Format) error
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
	privilegeChecker   privchecker.CheckerForRPCHandlers
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
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = b.AnnotateCtx(ctx)

	sessionUser, isAdmin, err := b.privilegeChecker.GetUserAndRole(ctx)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	hasViewActivityRedacted, err := b.privilegeChecker.HasPrivilegeOrRoleOption(ctx, sessionUser, privilege.VIEWACTIVITYREDACTED)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	hasViewActivity, err := b.privilegeChecker.HasPrivilegeOrRoleOption(ctx, sessionUser, privilege.VIEWACTIVITY)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	reqUsername, err := username.MakeSQLUsernameFromPreNormalizedStringChecked(req.Username)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
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

	// At this point, we have decided if we are going to show all sessions so we
	// can set the username to the session user if it is undefined.
	if reqUsername.Undefined() {
		reqUsername = sessionUser
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
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = b.AnnotateCtx(ctx)

	ctxUsername, isCtxAdmin, err := b.privilegeChecker.GetUserAndRole(ctx)
	if err != nil {
		return srverrors.ServerError(ctx, err)
	}
	if reqUsername.Undefined() {
		reqUsername = ctxUsername
	} else if reqUsername != ctxUsername && !isCtxAdmin {
		// When CANCEL QUERY is run as a SQL statement, sessionUser is always root
		// and the user who ran the statement is passed as req.Username.
		return privchecker.ErrRequiresAdmin
	}

	// A user can always cancel their own sessions/queries.
	if sessionUsername == reqUsername {
		return nil
	}

	// If reqUsername equals ctxUsername then isReqAdmin is isCtxAdmin and was
	// checked inside getUserAndRole above.
	isReqAdmin := isCtxAdmin
	if reqUsername != ctxUsername {
		isReqAdmin, err = b.privilegeChecker.HasAdminRole(ctx, reqUsername)
		if err != nil {
			return srverrors.ServerError(ctx, err)
		}
	}

	// Admin users can cancel sessions/queries.
	if isReqAdmin {
		return nil
	}

	// Must have CANCELQUERY privilege to cancel other users'
	// sessions/queries.
	hasGlobalCancelQuery, err := b.privilegeChecker.HasGlobalPrivilege(ctx, reqUsername, privilege.CANCELQUERY)
	if err != nil {
		return srverrors.ServerError(ctx, err)
	}
	if !hasGlobalCancelQuery {
		hasRoleCancelQuery, err := b.privilegeChecker.HasRoleOption(ctx, reqUsername, roleoption.CANCELQUERY)
		if err != nil {
			return srverrors.ServerError(ctx, err)
		}
		if !hasRoleCancelQuery {
			return privchecker.ErrRequiresRoleOption(roleoption.CANCELQUERY)
		}
	}

	// Non-admins cannot cancel admins' sessions/queries.
	isSessionAdmin, err := b.privilegeChecker.HasAdminRole(ctx, sessionUsername)
	if err != nil {
		return srverrors.ServerError(ctx, err)
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
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = b.AnnotateCtx(ctx)

	if err := b.privilegeChecker.RequireViewActivityOrViewActivityRedactedPermission(ctx); err != nil {
		// NB: not using srverrors.ServerError() here since the priv checker
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
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = b.AnnotateCtx(ctx)

	if err := b.privilegeChecker.RequireViewActivityOrViewActivityRedactedPermission(ctx); err != nil {
		// NB: not using srverrors.ServerError() here since the priv checker
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

		insightsCopy := *insight
		// Copy statements slice - these insights objects can be read concurrently.
		insightsCopy.Statements = make([]*insights.Statement, len(insight.Statements))
		copy(insightsCopy.Statements, insight.Statements)

		response.Insights = append(response.Insights, insightsCopy)
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

	// updateTableMetadataJobSignal is used to signal the updateTableMetadataCacheJob
	// to execute.
	updateTableMetadataJobSignal chan struct{}

	knobs *TestingKnobs
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
	engines            *Engines
	nodeLiveness       *liveness.NodeLiveness
	spanConfigReporter spanconfig.Reporter
	rangeStatsFetcher  *rangestats.Fetcher
	node               *Node
	knobs              *TestingKnobs
}

// StmtDiagnosticsRequester is the interface into *stmtdiagnostics.Registry
// used by AdminUI endpoints.
type StmtDiagnosticsRequester interface {
	// InsertRequest adds an entry to system.statement_diagnostics_requests for
	// tracing a query with the given fingerprint. Once this returns, calling
	// stmtdiagnostics.ShouldCollectDiagnostics() on the current node will
	// return true depending on the parameters below.
	// - planGist, when set, indicates a particular plan that we want collect
	//   diagnostics for. This can be useful when a single fingerprint can
	//   result in multiple plans.
	//  - There is a caveat to using this filtering: since the plan gist for a
	//    running query is only available after the optimizer has done its part,
	//    the trace will only include things after the optimizer is done.
	//  - if antiPlanGist is true, then any plan not matching the gist will do.
	// - samplingProbability controls how likely we are to try and collect a
	//   diagnostics report for a given execution. The semantics with
	//   minExecutionLatency are as follows:
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
	// - redacted, if true, indicates that the redacted bundle is requested.
	InsertRequest(
		ctx context.Context,
		stmtFingerprint string,
		planGist string,
		antiPlanGist bool,
		samplingProbability float64,
		minExecutionLatency time.Duration,
		expiresAfter time.Duration,
		redacted bool,
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
	adminAuthzCheck privchecker.CheckerForRPCHandlers,
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
	knobs *TestingKnobs,
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
		cancelSemaphore:              quotapool.NewIntPool("pgwire-cancel", 256),
		updateTableMetadataJobSignal: make(chan struct{}),
		knobs:                        knobs,
	}

	return server
}

// newSystemStatusServer allocates and returns a statusServer.
func newSystemStatusServer(
	ambient log.AmbientContext,
	st *cluster.Settings,
	cfg *base.Config,
	adminAuthzCheck privchecker.CheckerForRPCHandlers,
	db *kv.DB,
	gossip *gossip.Gossip,
	metricSource metricMarshaler,
	nodeLiveness *liveness.NodeLiveness,
	storePool *storepool.StorePool,
	rpcCtx *rpc.Context,
	stores *kvserver.Stores,
	engines *Engines,
	stopper *stop.Stopper,
	sessionRegistry *sql.SessionRegistry,
	closedSessionCache *sql.ClosedSessionCache,
	remoteFlowRunner *flowinfra.RemoteFlowRunner,
	internalExecutor *sql.InternalExecutor,
	serverIterator ServerIterator,
	spanConfigReporter spanconfig.Reporter,
	clock *hlc.Clock,
	rangeStatsFetcher *rangestats.Fetcher,
	node *Node,
	knobs *TestingKnobs,
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
		knobs,
	)

	return &systemStatusServer{
		statusServer:       server,
		gossip:             gossip,
		storePool:          storePool,
		stores:             stores,
		engines:            engines,
		nodeLiveness:       nodeLiveness,
		spanConfigReporter: spanConfigReporter,
		rangeStatsFetcher:  rangeStatsFetcher,
		node:               node,
		knobs:              knobs,
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
	if s.knobs != nil && s.knobs.DialNodeCallback != nil {
		if err := s.knobs.DialNodeCallback(ctx, nodeID); err != nil {
			return nil, err
		}
	}
	conn, err := s.serverIterator.dialNode(ctx, serverID(nodeID))
	if err != nil {
		return nil, err
	}
	return serverpb.NewStatusClient(conn), nil
}

// Gossip returns current state of gossip information on the given node
// which is crucial for monitoring and debugging the gossip protocol in
// CockroachDB cluster.
func (t *statusServer) Gossip(
	ctx context.Context, req *serverpb.GossipRequest,
) (*gossip.InfoStatus, error) {
	ctx = t.AnnotateCtx(ctx)

	if err := t.privilegeChecker.RequireViewClusterMetadataPermission(ctx); err != nil {
		return nil, err
	}

	return t.sqlServer.tenantConnect.Gossip(ctx, req)
}

// Gossip returns gossip network status. It is implemented
// in the systemStatusServer since the system tenant has
// access to gossip.
func (s *systemStatusServer) Gossip(
	ctx context.Context, req *serverpb.GossipRequest,
) (*gossip.InfoStatus, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.RequireViewClusterMetadataPermission(ctx); err != nil {
		// NB: not using srverrors.ServerError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if local {
		infoStatus := s.gossip.GetInfoStatus()
		return &infoStatus, nil
	}
	status, err := s.dialNode(ctx, nodeID)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}
	gossipData, err := status.Gossip(ctx, req)
	if err != nil {
		return nil, err
	}

	if req.Redact && DebugZipRedactAddressesEnabled.Get(&s.st.SV) {
		gossipData = s.redactGossipResponse(gossipData)
	}

	return gossipData, err
}

// TODO: Enhance with redaction middleware, refer: https://github.com/cockroachdb/cockroach/issues/109594
func (s *statusServer) redactGossipResponse(resp *gossip.InfoStatus) *gossip.InfoStatus {
	for i := range resp.Server.ConnStatus {
		resp.Server.ConnStatus[i].Address = redactedMarker
	}

	return resp
}

func (s *systemStatusServer) EngineStats(
	ctx context.Context, req *serverpb.EngineStatsRequest,
) (*serverpb.EngineStatsResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.RequireViewClusterMetadataPermission(ctx); err != nil {
		// NB: not using srverrors.ServerError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if !local {
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}
		return status.EngineStats(ctx, req)
	}

	stats, err := debug.GetLSMStats(*s.engines)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	return &serverpb.EngineStatsResponse{
		StatsByStoreId: stats,
	}, nil
}

// Allocator returns simulated allocator info for the ranges on the given node.
func (s *systemStatusServer) Allocator(
	ctx context.Context, req *serverpb.AllocatorRequest,
) (*serverpb.AllocatorResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.RequireViewClusterMetadataPermission(ctx); err != nil {
		// NB: not using srverrors.ServerError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if !local {
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, srverrors.ServerError(ctx, err)
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
		return nil, srverrors.ServerError(ctx, err)
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
	if err := s.privilegeChecker.RequireViewClusterMetadataPermission(ctx); err != nil {
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
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	err := s.privilegeChecker.RequireViewClusterMetadataPermission(ctx)
	if err != nil {
		return nil, err
	}

	isLiveMap := s.nodeLiveness.ScanNodeVitalityFromCache()
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
				_ = timeutil.RunWithTimeout(ctx, "allocator range", 3*time.Second, func(ctx context.Context) error {
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
			return nil, srverrors.ServerError(ctx, err)
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
		return nil, srverrors.ServerErrorf(ctx, "%v", buf)
	}
	return &serverpb.AllocatorRangeResponse{}, nil
}

// Certificates returns the x509 certificates.
func (s *statusServer) Certificates(
	ctx context.Context, req *serverpb.CertificatesRequest,
) (*serverpb.CertificatesResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.RequireViewClusterMetadataPermission(ctx); err != nil {
		// NB: not using srverrors.ServerError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if s.cfg.Insecure {
		return nil, status.Errorf(codes.Unavailable, "server is in insecure mode, cannot examine certificates")
	}

	if !local {
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}
		return status.Certificates(ctx, req)
	}

	cm, err := s.rpcCtx.GetCertificateManager()
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	// The certificate manager gives us a list of CertInfo objects to avoid
	// making security depend on serverpb.
	certs, err := cm.ListCertificates()
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
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
			return nil, srverrors.ServerErrorf(ctx, "unknown certificate type %v for file %s", cert.FileUsage, cert.Filename)
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
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.RequireViewClusterMetadataPermission(ctx); err != nil {
		// NB: not using srverrors.ServerError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if !local {
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}
		detailsResponse, err := status.Details(ctx, req)
		if err != nil {
			return nil, err
		}

		if req.Redact && DebugZipRedactAddressesEnabled.Get(&s.st.SV) {
			detailsResponse = s.redactDetailsResponse(detailsResponse)
		}

		return detailsResponse, err
	}

	remoteNodeID := roachpb.NodeID(s.serverIterator.getID())
	resp := &serverpb.DetailsResponse{
		NodeID:     remoteNodeID,
		BuildInfo:  build.GetInfo(),
		SystemInfo: s.si.systemInfo(ctx),
	}
	if addr, _, err := s.serverIterator.getServerIDAddress(ctx, serverID(remoteNodeID)); err == nil {
		resp.Address = *addr
	}
	if addr, _, err := s.serverIterator.getServerIDSQLAddress(ctx, serverID(remoteNodeID)); err == nil {
		resp.SQLAddress = *addr
	}

	if req.Redact && DebugZipRedactAddressesEnabled.Get(&s.st.SV) {
		resp = s.redactDetailsResponse(resp)
	}

	return resp, nil
}

// TODO: Enhance with redaction middleware, refer: https://github.com/cockroachdb/cockroach/issues/109594
func (s *statusServer) redactDetailsResponse(
	resp *serverpb.DetailsResponse,
) *serverpb.DetailsResponse {
	resp.SQLAddress.AddressField = redactedMarker
	resp.Address.AddressField = redactedMarker
	resp.SystemInfo.SystemInfo = redactedMarker
	return resp
}

// GetFiles returns a list of files of type defined in the request.
func (s *statusServer) GetFiles(
	ctx context.Context, req *serverpb.GetFilesRequest,
) (*serverpb.GetFilesResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.RequireViewClusterMetadataPermission(ctx); err != nil {
		// NB: not using srverrors.ServerError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if !local {
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}
		return status.GetFiles(ctx, req)
	}

	cfg := s.sqlServer.cfg
	return getLocalFiles(
		req, cfg.HeapProfileDirName, cfg.GoroutineDumpDirName,
		cfg.CPUProfileDirName, os.Stat, os.ReadFile,
	)
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
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.RequireViewClusterMetadataPermission(ctx); err != nil {
		// NB: not using srverrors.ServerError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if !local {
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}
		return status.LogFilesList(ctx, req)
	}
	log.FlushFiles()
	logFiles, err := log.ListLogFiles()
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
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
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.RequireViewClusterMetadataPermission(ctx); err != nil {
		// NB: not using srverrors.ServerError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if !local {
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}
		return status.LogFile(ctx, req)
	}

	// Determine how to redact.
	inputEditMode := log.SelectEditMode(req.Redact, log.KeepRedactable)

	// Ensure that the latest log entries are available in files.
	log.FlushFiles()

	// Read the logs.
	reader, err := log.GetLogReader(req.File)
	if err != nil {
		return nil, srverrors.ServerError(ctx, errors.Wrapf(err, "log file %q could not be opened", req.File))
	}
	defer reader.Close()

	var resp serverpb.LogEntriesResponse
	decoder, err := log.NewEntryDecoder(reader, inputEditMode)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
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
			if errors.Is(err, log.ErrMalformedLogEntry) {
				resp.ParseErrors = append(resp.ParseErrors, err.Error())
				//Append log generated from malformed line.
				resp.Entries = append(resp.Entries, entry)
				// Proceed decoding next entry, as we want to retrieve as much logs
				// as possible.
				continue
			}
			return nil, srverrors.ServerError(ctx, err)
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
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.RequireViewClusterMetadataPermission(ctx); err != nil {
		// NB: not using srverrors.ServerError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if !local {
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, srverrors.ServerError(ctx, err)
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
	log.FlushFiles()

	// Read the logs.
	entries, err := log.FetchEntriesFromFiles(
		startTimestamp, endTimestamp, int(maxEntries), regex, inputEditMode)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
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
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.RequireViewClusterMetadataPermission(ctx); err != nil {
		// NB: not using srverrors.ServerError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if !local {
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}
		return status.Stacks(ctx, req)
	}

	return stacksLocal(req)
}

// Nodes returns all node statuses for a secondary tenant with
// `can_view_node_info` capability.
//
// Do not use this method inside the server code! Use ListNodesInternal()
// instead. This method here is the one exposed to network clients over HTTP.
func (s *statusServer) Nodes(
	ctx context.Context, req *serverpb.NodesRequest,
) (*serverpb.NodesResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	err := s.privilegeChecker.RequireViewClusterMetadataPermission(ctx)
	if err != nil {
		return nil, err
	}

	return s.sqlServer.tenantConnect.Nodes(ctx, req)
}

func (s *statusServer) processRawGoroutines(
	_ context.Context, response profDataResponse,
) ([]byte, error) {
	res := bytes.NewBuffer(nil)
	for nodeID, pd := range response.profDataByNodeID {
		if len(pd.data) == 0 && pd.err == nil {
			res.WriteString(fmt.Sprintf("No goroutines collected for node %d\n", nodeID))
			continue // skipped node
		}

		if pd.err != nil {
			res.WriteString(fmt.Sprintf("Failed to collect goroutines for node %d: %v\n", nodeID, pd.err))
			continue
		}

		res.Write(pd.data)
	}
	return res.Bytes(), nil
}

func (s *statusServer) processProfileProtoGoroutines(
	_ context.Context, response profDataResponse,
) ([]byte, error) {
	profileErrs := make([]string, 0)
	res := bytes.NewBuffer(nil)
	profs := make([]*profile.Profile, 0, len(response.profDataByNodeID))
	for nodeID, pd := range response.profDataByNodeID {
		if len(pd.data) == 0 && pd.err == nil {
			profileErrs = append(profileErrs, fmt.Sprintf("No goroutines collected for node %d", nodeID))
			continue // skipped node
		}

		if pd.err != nil {
			profileErrs = append(profileErrs, fmt.Sprintf("Failed to collect goroutines for node %d: %v", nodeID, pd.err))
			continue
		}

		p, err := profile.ParseData(pd.data)
		if err != nil {
			return nil, err
		}
		p.Comments = append(p.Comments, fmt.Sprintf("n%d", nodeID))
		profs = append(profs, p)
	}

	errMsg := "Errors while collecting profiles:\n"
	for _, pErr := range profileErrs {
		errMsg += fmt.Sprintf("%s\n", pErr)
	}
	if len(profs) == 0 {
		return nil, errors.Newf("no profiles could be collected: %s", errMsg)
	}

	mergedProfiles, err := profile.Merge(profs)
	if err != nil {
		return nil, errors.Wrap(err, "failed to merge profiles")
	}
	if len(profileErrs) > 0 {
		mergedProfiles.Comments = append(mergedProfiles.Comments, errMsg)
	}

	if err := mergedProfiles.Write(res); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return res.Bytes(), nil
}

func (s *statusServer) processGoroutineProfilesFromAllNodes(
	ctx context.Context, request *serverpb.ProfileRequest, response profDataResponse,
) ([]byte, error) {
	if request.Labels {
		return s.processRawGoroutines(ctx, response)
	}
	return s.processProfileProtoGoroutines(ctx, response)
}

func (s *statusServer) processCPUProfilesFromAllNodes(
	_ context.Context, response profDataResponse,
) ([]byte, error) {
	profileErrs := make([]string, 0)
	profs := make([]*profile.Profile, 0, len(response.profDataByNodeID))
	for nodeID, pd := range response.profDataByNodeID {
		if len(pd.data) == 0 && pd.err == nil {
			profileErrs = append(profileErrs, fmt.Sprintf("No profile collected for node %d", nodeID))
			continue // skipped node
		}

		if pd.err != nil {
			profileErrs = append(profileErrs, fmt.Sprintf("Failed to collect profile for node %d: %v", nodeID, pd.err))
			continue
		}

		p, err := profile.ParseData(pd.data)
		if err != nil {
			return nil, err
		}
		p.Comments = append(p.Comments, fmt.Sprintf("n%d", nodeID))
		profs = append(profs, p)
	}

	errMsg := "Errors while collecting profiles:\n"
	for _, pErr := range profileErrs {
		errMsg += fmt.Sprintf("%s\n", pErr)
	}
	if len(profs) == 0 {
		return nil, errors.Newf("no profiles could be collected: %s", errMsg)
	}
	mergedProfiles, err := profile.Merge(profs)
	if err != nil {
		return nil, errors.Wrap(err, "failed to merge profiles")
	}
	if len(profileErrs) > 0 {
		mergedProfiles.Comments = append(mergedProfiles.Comments, errMsg)
	}

	var buf bytes.Buffer
	if err := mergedProfiles.Write(&buf); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return buf.Bytes(), nil
}

type profData struct {
	data []byte
	err  error
}
type profDataResponse struct {
	profDataByNodeID map[roachpb.NodeID]*profData
}

// fetchProfileFromAllNodes fetches the profile from all live nodes in the
// cluster and merges the samples across all profiles.
func (s *statusServer) fetchProfileFromAllNodes(
	ctx context.Context, req *serverpb.ProfileRequest,
) (*serverpb.JSONResponse, error) {
	response := profDataResponse{profDataByNodeID: make(map[roachpb.NodeID]*profData)}

	resp, err := s.Node(ctx, &serverpb.NodeRequest{NodeId: "local"})
	if err != nil {
		return nil, err
	}
	senderServerVersion := resp.Desc.ServerVersion

	opName := redact.Sprintf("fetch cluster-wide %s profile", req.Type)
	nodeFn := func(ctx context.Context, statusClient serverpb.StatusClient, nodeID roachpb.NodeID) (*profData, error) {
		var pd *profData
		err := timeutil.RunWithTimeout(ctx, opName, 1*time.Minute, func(ctx context.Context) error {
			resp, err := statusClient.Profile(ctx, &serverpb.ProfileRequest{
				NodeId:              fmt.Sprintf("%d", nodeID),
				Type:                req.Type,
				Seconds:             req.Seconds,
				Labels:              req.Labels,
				LabelFilter:         req.LabelFilter,
				SenderServerVersion: &senderServerVersion,
			})
			if err != nil {
				return err
			}
			pd = &profData{data: resp.Data}
			return nil
		})
		return pd, err
	}
	responseFn := func(nodeID roachpb.NodeID, profResp *profData) {
		response.profDataByNodeID[nodeID] = profResp
	}
	errorFn := func(nodeID roachpb.NodeID, err error) {
		response.profDataByNodeID[nodeID] = &profData{err: err}
	}
	if err := iterateNodes(
		ctx, s.serverIterator, s.stopper, opName, noTimeout, s.dialNode, nodeFn, responseFn, errorFn,
	); err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}
	var data []byte
	switch req.Type {
	case serverpb.ProfileRequest_CPU:
		data, err = s.processCPUProfilesFromAllNodes(ctx, response)
	case serverpb.ProfileRequest_GOROUTINE:
		data, err = s.processGoroutineProfilesFromAllNodes(ctx, req, response)
	default:
		return nil, errors.Newf("cluster-wide collection of %s is unsupported", req.Type)
	}
	if err != nil {
		return nil, err
	}
	return &serverpb.JSONResponse{Data: data}, nil
}

// TODO(tschottdorf): significant overlap with /debug/pprof/heap, except that
// this one allows querying by NodeID.
//
// Profile returns a heap profile. This endpoint is used by the
// `pprofui` package to satisfy local and remote pprof requests.
func (s *statusServer) Profile(
	ctx context.Context, req *serverpb.ProfileRequest,
) (*serverpb.JSONResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.RequireViewClusterMetadataPermission(ctx); err != nil {
		// NB: not using srverrors.ServerError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	// If the request is for "all" nodes then we collect profiles from all nodes
	// in the cluster and process them before returning to the user.
	if req.NodeId == "all" {
		return s.fetchProfileFromAllNodes(ctx, req)
	}

	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if !local {
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}
		return status.Profile(ctx, req)
	}

	// If the request has a SenderVersion, then ensure the current node has the
	// same server version before collecting a profile.
	if req.SenderServerVersion != nil {
		serverVersion := s.st.Version.LatestVersion()
		if !serverVersion.Equal(*req.SenderServerVersion) {
			return nil, errors.Newf("server version of the node being profiled %s != sender version %s",
				serverVersion.String(), req.SenderServerVersion.String())
		}
	}
	return profileLocal(ctx, req, s.st, nodeID)
}

// Regions implements the serverpb.StatusServer interface.
func (s *systemStatusServer) Regions(
	ctx context.Context, req *serverpb.RegionsRequest,
) (*serverpb.RegionsResponse, error) {
	resp, _, err := s.nodesHelper(ctx, 0 /* limit */, 0 /* offset */)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}
	return regionsResponseFromNodesResponse(resp), nil
}

// NodesList returns a list of nodes with their corresponding addresses.
func (s *statusServer) NodesList(
	ctx context.Context, request *serverpb.NodesListRequest,
) (*serverpb.NodesListResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	// The node status contains details about the command line, network
	// addresses, env vars etc which are privileged information.
	if err := s.privilegeChecker.RequireViewClusterMetadataPermission(ctx); err != nil {
		// NB: not using srverrors.ServerError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}
	nodeListResponse, err := s.serverIterator.nodesList(ctx)

	if err != nil {
		return nil, err
	}

	if request != nil && request.Redact && DebugZipRedactAddressesEnabled.Get(&s.st.SV) {
		nodeListResponse = s.redactNodeListResponse(nodeListResponse)
	}

	return nodeListResponse, nil
}

// TODO: Enhance with redaction middleware, refer: https://github.com/cockroachdb/cockroach/issues/109594
func (s *statusServer) redactNodeListResponse(
	nodeListResponse *serverpb.NodesListResponse,
) *serverpb.NodesListResponse {
	for i := range nodeListResponse.Nodes {
		nodeListResponse.Nodes[i].Address.AddressField = redactedMarker
		nodeListResponse.Nodes[i].SQLAddress.AddressField = redactedMarker
	}

	return nodeListResponse
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
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	err := s.privilegeChecker.RequireViewClusterMetadataPermission(ctx)
	if err != nil {
		return nil, err
	}

	resp, _, err := s.nodesHelper(ctx, 0 /* limit */, 0 /* offset */)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	if req != nil && req.Redact && DebugZipRedactAddressesEnabled.Get(&s.st.SV) {
		resp = s.redactNodesResponse(resp)
	}

	return resp, nil
}

// TODO: Enhance with redaction middleware, refer: https://github.com/cockroachdb/cockroach/issues/109594
func (s *statusServer) redactNodesResponse(resp *serverpb.NodesResponse) *serverpb.NodesResponse {
	for i := range resp.Nodes {
		resp.Nodes[i].Desc.Address.AddressField = redactedMarker
		resp.Nodes[i].Desc.SQLAddress.AddressField = redactedMarker
		resp.Nodes[i].Desc.HTTPAddress.AddressField = redactedMarker

		for j := range resp.Nodes[i].Desc.Locality.Tiers {
			resp.Nodes[i].Desc.Locality.Tiers[j].Value = redactedMarker
		}

		for j := range resp.Nodes[i].StoreStatuses {
			resp.Nodes[i].StoreStatuses[j].Desc.Node.Address.AddressField = redactedMarker
			resp.Nodes[i].StoreStatuses[j].Desc.Node.SQLAddress.AddressField = redactedMarker
			resp.Nodes[i].StoreStatuses[j].Desc.Node.HTTPAddress.AddressField = redactedMarker

			for k := range resp.Nodes[i].StoreStatuses[j].Desc.Node.Locality.Tiers {
				resp.Nodes[i].StoreStatuses[j].Desc.Node.Locality.Tiers[k].Value = redactedMarker
			}
		}
	}

	return resp
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
	err := s.privilegeChecker.RequireViewClusterMetadataPermission(ctx)
	if err != nil {
		if !grpcutil.IsAuthError(err) {
			return nil, err
		}
	} else {
		hasViewClusterMetadata = true
	}

	internalResp, err := s.sqlServer.tenantConnect.Nodes(ctx, req)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
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
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	hasViewClusterMetadata := false
	err := s.privilegeChecker.RequireViewClusterMetadataPermission(ctx)
	if err != nil {
		if !grpcutil.IsAuthError(err) {
			return nil, err
		}
	} else {
		hasViewClusterMetadata = true
	}

	internalResp, _, err := s.nodesHelper(ctx, 0 /* limit */, 0 /* offset */)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
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

// ListNodesInternal is a helper function for the benefit of SQL exclusively.
// It skips the privilege check, assuming that SQL is doing privilege checking already.
//
// Note that the function returns plain errors, and it is the caller's
// responsibility to convert them to srverrors.ServerErrors.
func (s *systemStatusServer) ListNodesInternal(
	ctx context.Context, req *serverpb.NodesRequest,
) (*serverpb.NodesResponse, error) {
	resp, _, err := s.nodesHelper(ctx, 0 /* limit */, 0 /* offset */)
	return resp, err
}

// Note that the function returns plain errors, and it is the caller's
// responsibility to convert them to srverrors.ServerErrors.
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
		rows, next = simplePaginate(b.Results[0].Rows, limit, offset)
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
// responsibility to convert them to srverrors.ServerErrors.
func (s *systemStatusServer) nodesHelper(
	ctx context.Context, limit, offset int,
) (*serverpb.NodesResponse, int, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	statuses, next, err := getNodeStatuses(ctx, s.db, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	resp := serverpb.NodesResponse{
		Nodes: statuses,
	}

	nodeStatusMap, err := s.nodeLiveness.ScanNodeVitalityFromKV(ctx)
	if err != nil {
		return nil, 0, err
	}
	// TODO(baptist): Consider returning something better than LivenessStatus. It
	// is an unfortunate mix of values.
	resp.LivenessByNodeID = make(map[roachpb.NodeID]livenesspb.NodeLivenessStatus, len(nodeStatusMap))
	for nodeID, status := range nodeStatusMap {
		resp.LivenessByNodeID[nodeID] = status.LivenessStatus()
	}
	return &resp, next, nil
}

// handleNodeStatus handles GET requests for a single node's status.
func (s *statusServer) Node(
	ctx context.Context, req *serverpb.NodeRequest,
) (*statuspb.NodeStatus, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	// The node status contains details about the command line, network
	// addresses, env vars etc which are privileged information..
	if err := s.privilegeChecker.RequireViewClusterMetadataPermission(ctx); err != nil {
		// NB: not using srverrors.ServerError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	// NB: not using srverrors.ServerError() here since nodeStatus
	// already returns a proper gRPC error status.
	return s.nodeStatus(ctx, req)
}

func (s *statusServer) nodeStatus(
	ctx context.Context, req *serverpb.NodeRequest,
) (*statuspb.NodeStatus, error) {
	nodeID, _, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	key := keys.NodeStatusKey(nodeID)
	b := &kv.Batch{}
	b.Get(key)
	if err := s.db.Run(ctx, b); err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	var nodeStatus statuspb.NodeStatus
	if err := b.Results[0].Rows[0].ValueProto(&nodeStatus); err != nil {
		err = errors.Wrapf(err, "could not unmarshal NodeStatus from %s", key)
		return nil, srverrors.ServerError(ctx, err)
	}

	if req != nil && req.Redact && DebugZipRedactAddressesEnabled.Get(&s.st.SV) {
		nodeStatus = *s.redactNodeStatusResponse(&nodeStatus)
	}

	return &nodeStatus, nil
}

// TODO: Enhance with redaction middleware, refer: https://github.com/cockroachdb/cockroach/issues/109594
func (s *statusServer) redactNodeStatusResponse(
	nodeStatus *statuspb.NodeStatus,
) *statuspb.NodeStatus {
	nodeStatus.Desc.SQLAddress.AddressField = redactedMarker
	nodeStatus.Desc.Address.AddressField = redactedMarker
	return nodeStatus
}

func (s *statusServer) NodeUI(
	ctx context.Context, req *serverpb.NodeRequest,
) (*serverpb.NodeResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	// The node status contains details about the command line, network
	// addresses, env vars etc which are admin-only.
	_, isAdmin, err := s.privilegeChecker.GetUserAndRole(ctx)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	nodeStatus, err := s.nodeStatus(ctx, req)
	if err != nil {
		// NB: not using srverrors.ServerError() here since nodeStatus
		// already returns a proper gRPC error status.
		return nil, err
	}
	resp := nodeStatusToResp(nodeStatus, isAdmin)
	return &resp, nil
}

// NetworkConnectivity collects info about connection statuses across all nodes.
// This isn't tenant-specific information; it's about networking activity across
// all tenants between nodes. It's accessible via the system tenant, and here
// made available to secondary tenants with the `can_debug_process` capability.
// This works well for shared-process mode, but in external-process mode, this
// endpoint won't give a complete picture of network connectivity since the SQL
// server might run entirely outside the KV node. We might need to extend this
// endpoint or create a new one for SQL-SQL servers and SQL server to KV nodes.
// This work is for the future. Currently, this endpoint only shows KV-KV nodes
// network connectivity. So, it's not ready for external-process mode and should
// only be enabled for shared-process mode. There's nothing enforcing this, but
// it shouldn't be a problem. See issue #138156
func (t *statusServer) NetworkConnectivity(
	ctx context.Context, req *serverpb.NetworkConnectivityRequest,
) (*serverpb.NetworkConnectivityResponse, error) {
	ctx = t.AnnotateCtx(ctx)

	err := t.privilegeChecker.RequireViewClusterMetadataPermission(ctx)
	if err != nil {
		return nil, err
	}

	return t.sqlServer.tenantConnect.NetworkConnectivity(ctx, req)
}

// NetworkConnectivity collects information about connections statuses across all nodes.
func (s *systemStatusServer) NetworkConnectivity(
	ctx context.Context, req *serverpb.NetworkConnectivityRequest,
) (*serverpb.NetworkConnectivityResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	response := &serverpb.NetworkConnectivityResponse{
		Connections:    map[roachpb.NodeID]serverpb.NetworkConnectivityResponse_Connectivity{},
		ErrorsByNodeID: map[roachpb.NodeID]string{},
	}

	if len(req.NodeID) > 0 {
		sourceNodeID, local, err := s.parseNodeID(req.NodeID)
		if err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}

		if !local {
			statusClient, err := s.dialNode(ctx, sourceNodeID)
			if err != nil {
				return nil, srverrors.ServerError(ctx, err)
			}
			return statusClient.NetworkConnectivity(ctx, req)
		}

		// "local" specified, so collect the local results and return them back.
		peers := map[roachpb.NodeID]serverpb.NetworkConnectivityResponse_Peer{}
		var nodeIDs []roachpb.NodeID
		err = s.gossip.IterateInfos(gossip.KeyNodeDescPrefix, func(k string, info gossip.Info) error {
			nodeIDs = append(nodeIDs, info.NodeID)
			return nil
		})
		if err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}

		latencies := s.rpcCtx.RemoteClocks.AllLatencies()

		for _, targetNodeId := range nodeIDs {
			if sourceNodeID == targetNodeId {
				continue
			}
			peer := serverpb.NetworkConnectivityResponse_Peer{}
			peer.Latency = latencies[targetNodeId]

			node, err := s.gossip.GetNodeDescriptor(targetNodeId)
			if err != nil {
				peer.Status = serverpb.NetworkConnectivityResponse_UNKNOWN
				peer.Error = errors.UnwrapAll(err).Error()
				continue
			}
			addr, _, err := s.gossip.GetNodeIDAddress(targetNodeId)
			if err != nil {
				peer.Status = serverpb.NetworkConnectivityResponse_UNKNOWN
				peer.Error = errors.UnwrapAll(err).Error()
				continue
			}
			if err = s.rpcCtx.ConnHealth(addr.String(), targetNodeId, rpc.SystemClass); err != nil {
				if errors.Is(rpc.ErrNotHeartbeated, err) {
					peer.Status = serverpb.NetworkConnectivityResponse_ESTABLISHING
				} else {
					peer.Status = serverpb.NetworkConnectivityResponse_ERROR
				}
				peer.Error = errors.UnwrapAll(err).Error()
			} else {
				peer.Status = serverpb.NetworkConnectivityResponse_ESTABLISHED
			}
			peer.Address = addr.String()
			peer.Locality = &node.Locality

			peers[targetNodeId] = peer
		}

		response.Connections[sourceNodeID] = serverpb.NetworkConnectivityResponse_Connectivity{
			Peers: peers,
		}
		return response, nil
	}

	// No NodeID parameter specified, so fan-out to all nodes and collect results.
	remoteRequest := serverpb.NetworkConnectivityRequest{NodeID: "local"}
	nodeFn := func(ctx context.Context, statusClient serverpb.StatusClient, _ roachpb.NodeID) (*serverpb.NetworkConnectivityResponse, error) {
		return statusClient.NetworkConnectivity(ctx, &remoteRequest)
	}
	responseFn := func(nodeID roachpb.NodeID, r *serverpb.NetworkConnectivityResponse) {
		response.Connections[nodeID] = r.Connections[nodeID]
	}
	errorFn := func(nodeID roachpb.NodeID, err error) {
		response.ErrorsByNodeID[nodeID] = err.Error()
	}

	if err := iterateNodes(ctx, s.serverIterator, s.stopper, "network connectivity",
		noTimeout,
		s.dialNode,
		nodeFn,
		responseFn,
		errorFn,
	); err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	return response, nil
}

// Metrics return metrics information for the server specified.
func (s *statusServer) Metrics(
	ctx context.Context, req *serverpb.MetricsRequest,
) (*serverpb.JSONResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if !local {
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}
		return status.Metrics(ctx, req)
	}
	j, err := marshalJSONResponse(s.metricSource)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}
	return j, nil
}

// RaftDebug returns raft debug information for all known nodes.
func (s *systemStatusServer) RaftDebug(
	ctx context.Context, req *serverpb.RaftDebugRequest,
) (*serverpb.RaftDebugResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.RequireViewClusterMetadataPermission(ctx); err != nil {
		// NB: not using srverrors.ServerError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	nodes, err := s.ListNodesInternal(ctx, nil)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
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

	contentType := expfmt.Negotiate(r.Header)
	w.Header().Set(httputil.ContentTypeHeader, string(contentType))
	err := h.metricSource.PrintAsText(w, contentType)
	if err != nil {
		log.Errorf(ctx, "%v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	telemetry.Inc(telemetryPrometheusVars)
}

// Ranges returns the current tenant's range information for the specified node.
//
// Note: If the current server is running in external process mode and
// RangesRequest.NodeID is `local`, the resulting value could be returned by any
// of the KV nodes via the tenant connector. This is because the system SQL
// server may not be colocated with the current server, making the result
// non-deterministic.
func (t *statusServer) Ranges(
	ctx context.Context, req *serverpb.RangesRequest,
) (*serverpb.RangesResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = t.AnnotateCtx(ctx)

	// Response contains replica metadata which is privileged.
	if err := t.privilegeChecker.RequireViewClusterMetadataPermission(ctx); err != nil {
		return nil, err
	}

	return t.sqlServer.tenantConnect.Ranges(ctx, req)
}

// Ranges returns range info for the specified node.
func (s *systemStatusServer) Ranges(
	ctx context.Context, req *serverpb.RangesRequest,
) (*serverpb.RangesResponse, error) {
	resp, next, err := s.rangesHelper(ctx, req, int(req.Limit), int(req.Offset))
	if resp != nil {
		resp.Next = int32(next)
	}

	if req.Redact && DebugZipRedactAddressesEnabled.Get(&s.st.SV) {
		resp = s.redactRangesResponse(resp)
	}

	return resp, err
}

// TODO: Enhance with redaction middleware, refer: https://github.com/cockroachdb/cockroach/issues/109594
func (s *statusServer) redactRangesResponse(
	resp *serverpb.RangesResponse,
) *serverpb.RangesResponse {
	for i := range resp.Ranges {
		for j := range resp.Ranges[i].Locality.Tiers {
			resp.Ranges[i].Locality.Tiers[j].Value = redactedMarker
		}
	}

	return resp
}

// Ranges returns range info for the specified node.
func (s *systemStatusServer) rangesHelper(
	ctx context.Context, req *serverpb.RangesRequest, limit, offset int,
) (*serverpb.RangesResponse, int, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	err := s.privilegeChecker.RequireViewClusterMetadataPermission(ctx)
	if err != nil {
		return nil, 0, err
	}

	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, 0, status.Error(codes.InvalidArgument, err.Error())
	}

	if !local {
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, 0, err
		}
		resp, err := status.Ranges(ctx, req)
		var next int
		if resp != nil && len(resp.Ranges) > 0 {
			resp.Ranges, next = simplePaginate(resp.Ranges, limit, offset)
		}
		return resp, next, err
	}

	tID, ok := roachpb.ClientTenantFromContext(ctx)
	if !ok {
		tID = roachpb.SystemTenantID
	}

	tenantKeySpan := keys.MakeTenantSpan(tID)

	output := serverpb.RangesResponse{
		Ranges: make([]serverpb.RangeInfo, 0, s.stores.GetStoreCount()),
	}

	convertRaftStatus := func(raftStatus *raft.Status) serverpb.RaftState {
		if raftStatus == nil {
			return serverpb.RaftState{State: RaftStateDormant}
		}

		state := serverpb.RaftState{
			ReplicaID:        uint64(raftStatus.ID),
			HardState:        raftStatus.HardState,
			Applied:          raftStatus.Applied,
			Lead:             raftStatus.Lead,
			State:            raftStatus.RaftState.String(),
			Progress:         make(map[uint64]serverpb.RaftState_Progress),
			LeadTransferee:   raftStatus.LeadTransferee,
			LeadSupportUntil: raftStatus.LeadSupportUntil,
		}

		for id, progress := range raftStatus.Progress {
			state.Progress[uint64(id)] = serverpb.RaftState_Progress{
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
		// TODO(pav-kv): lock once, instead of doing it in every call below (some
		// doing it more than once).
		raftStatus := rep.RaftStatus()
		leaseHistory := rep.GetLeaseHistory()
		desc := rep.Desc()
		rac2Status := rep.RACv2Status()
		span := serverpb.PrettySpan{StartKey: desc.StartKey.String(), EndKey: desc.EndKey.String()}
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
		quiescentOrAsleep := metrics.Quiescent || metrics.Asleep
		return serverpb.RangeInfo{
			Span:          span,
			RaftState:     convertRaftStatus(raftStatus),
			RACStatus:     rac2Status,
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
				QuiescentEqualsTicking: raftStatus != nil && quiescentOrAsleep == metrics.Ticking,
				RaftLogTooLarge:        metrics.RaftLogTooLarge,
				RangeTooLarge:          metrics.RangeTooLarge,
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

	isLiveMap := s.nodeLiveness.ScanNodeVitalityFromCache()
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
		appendRangeInfo := func(rep *kvserver.Replica) {
			if !tID.IsSystem() {
				rangeReplicaSpan := rep.Desc().RSpan().AsRawSpanWithNoLocals()
				if !tenantKeySpan.Contains(rangeReplicaSpan) {
					return
				}
			}

			output.Ranges = append(output.Ranges, constructRangeInfo(
				rep,
				store.Ident.StoreID,
				rep.Metrics(ctx, now, isLiveMap, clusterNodes),
			))
		}

		if len(req.RangeIDs) == 0 {
			// All ranges requested.
			store.VisitReplicas(func(r *kvserver.Replica) bool {
				appendRangeInfo(r)
				return true // continue.
			}, kvserver.WithReplicasInOrder())
			return nil
		}

		// Specific ranges requested:
		for _, rid := range req.RangeIDs {
			rep, err := store.GetReplica(rid)
			if err != nil {
				// Not found: continue.
				continue
			}
			appendRangeInfo(rep)
		}
		return nil
	})
	if err != nil {
		return nil, 0, status.Error(codes.Internal, err.Error())
	}
	var next int
	if limit > 0 {
		output.Ranges, next = simplePaginate(output.Ranges, limit, offset)
	}
	return &output, next, nil
}

func (t *statusServer) TenantRanges(
	ctx context.Context, req *serverpb.TenantRangesRequest,
) (*serverpb.TenantRangesResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = t.AnnotateCtx(ctx)

	// The tenant range report contains replica metadata which is privileged.
	if err := t.privilegeChecker.RequireViewClusterMetadataPermission(ctx); err != nil {
		return nil, err
	}

	return t.sqlServer.tenantConnect.TenantRanges(ctx, req)
}

func (s *systemStatusServer) TenantRanges(
	ctx context.Context, req *serverpb.TenantRangesRequest,
) (*serverpb.TenantRangesResponse, error) {
	authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)
	if err := s.privilegeChecker.RequireViewClusterMetadataPermission(ctx); err != nil {
		return nil, err
	}

	tID, ok := roachpb.ClientTenantFromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Internal, "no tenant ID found in context")
	}

	tenantKeySpan := keys.MakeTenantSpan(tID)

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
			return nil, status.Error(codes.Internal, err.Error())
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
				return nil, srverrors.ServerError(ctx, err)
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
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.RequireViewClusterMetadataPermission(ctx); err != nil {
		// NB: not using srverrors.ServerError() here since the priv checker
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
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}

		// Only hot ranges from the local node.
		if local {
			response.HotRangesByNodeID[requestedNodeID] = s.localHotRanges(ctx, roachpb.TenantID{})
			return response, nil
		}

		// Only hot ranges from one non-local node.
		status, err := s.dialNode(ctx, requestedNodeID)
		if err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}
		return status.HotRanges(ctx, req)
	}

	// Hot ranges from all nodes.
	remoteRequest := serverpb.HotRangesRequest{NodeID: "local"}
	nodeFn := func(ctx context.Context, status serverpb.StatusClient, _ roachpb.NodeID) (*serverpb.HotRangesResponse, error) {
		return status.HotRanges(ctx, &remoteRequest)
	}
	responseFn := func(nodeID roachpb.NodeID, hotRangesResp *serverpb.HotRangesResponse) {
		response.HotRangesByNodeID[nodeID] = hotRangesResp.HotRangesByNodeID[nodeID]
	}
	errorFn := func(nodeID roachpb.NodeID, err error) {
		response.HotRangesByNodeID[nodeID] = serverpb.HotRangesResponse_NodeResponse{
			ErrorMessage: err.Error(),
		}
	}

	if err := iterateNodes(ctx, s.serverIterator, s.stopper, "hot ranges",
		noTimeout,
		s.dialNode,
		nodeFn,
		responseFn,
		errorFn,
	); err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	return response, nil
}

func (t *statusServer) HotRangesV2(
	ctx context.Context, req *serverpb.HotRangesRequest,
) (*serverpb.HotRangesResponseV2, error) {
	ctx = t.AnnotateCtx(ctx)

	err := t.privilegeChecker.RequireViewClusterMetadataPermission(ctx)
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
	ctx = s.AnnotateCtx(authserver.ForwardSQLIdentityThroughRPCCalls(ctx))

	err := s.privilegeChecker.RequireViewClusterMetadataPermission(ctx)
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
			var rangeIndexMappings map[roachpb.RangeID]apiutil.IndexNamesList
			for _, store := range resp.Stores {
				rangeDescriptors := []roachpb.RangeDescriptor{}
				for _, r := range store.HotRanges {
					rangeDescriptors = append(rangeDescriptors, r.Desc)
				}
				if err = s.sqlServer.distSQLServer.DB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
					databases, err := txn.Descriptors().GetAllDatabaseDescriptorsMap(ctx, txn.KV())
					if err != nil {
						return err
					}
					rangeIndexMappings, err = apiutil.GetRangeIndexMapping(ctx, txn, s.sqlServer.execCfg.Codec, databases, rangeDescriptors)
					return err
				}); err != nil {
					return nil, err
				}
				for _, r := range store.HotRanges {
					var replicaNodeIDs []roachpb.NodeID

					for _, repl := range r.Desc.Replicas().Descriptors() {
						replicaNodeIDs = append(replicaNodeIDs, repl.NodeID)
					}

					databases, tables, indexes := rangeIndexMappings[r.Desc.RangeID].ToOutput()

					ranges = append(ranges, &serverpb.HotRangesResponseV2_HotRange{
						RangeID:             r.Desc.RangeID,
						NodeID:              requestedNodeID,
						QPS:                 r.QueriesPerSecond,
						WritesPerSecond:     r.WritesPerSecond,
						ReadsPerSecond:      r.ReadsPerSecond,
						WriteBytesPerSecond: r.WriteBytesPerSecond,
						ReadBytesPerSecond:  r.ReadBytesPerSecond,
						CPUTimePerSecond:    r.CPUTimePerSecond,
						ReplicaNodeIds:      replicaNodeIDs,
						LeaseholderNodeID:   r.LeaseholderNodeID,
						StoreID:             store.StoreID,
						Databases:           databases,
						Tables:              tables,
						Indexes:             indexes,
					})
				}
			}
			response.Ranges = ranges
			response.ErrorsByNodeID[requestedNodeID] = resp.ErrorMessage
			return response, nil
		}
		requestedNodes = []roachpb.NodeID{requestedNodeID}
	}

	remoteRequest := serverpb.HotRangesRequest{NodeID: "local", TenantID: req.TenantID}
	nodeFn := func(ctx context.Context, status serverpb.StatusClient, nodeID roachpb.NodeID) ([]*serverpb.HotRangesResponseV2_HotRange, error) {
		nodeResp, err := status.HotRangesV2(ctx, &remoteRequest)
		if err != nil {
			return nil, err
		}
		return nodeResp.Ranges, nil
	}
	responseFn := func(nodeID roachpb.NodeID, hotRanges []*serverpb.HotRangesResponseV2_HotRange) {
		if len(hotRanges) == 0 {
			return
		}
		response.Ranges = append(response.Ranges, hotRanges...)
	}
	errorFn := func(nodeID roachpb.NodeID, err error) {
		response.ErrorsByNodeID[nodeID] = err.Error()
	}

	timeout := HotRangesRequestNodeTimeout.Get(&s.st.SV)
	next, err := paginatedIterateNodes(
		ctx, s.statusServer, "hotRanges", size, start, requestedNodes, timeout,
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
				lease, _ := replica.GetLease()
				storeResp.HotRanges[i].LeaseholderNodeID = lease.Replica.NodeID
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

	if err := s.privilegeChecker.RequireViewClusterMetadataPermission(ctx); err != nil {
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
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.RequireViewClusterMetadataPermission(ctx); err != nil {
		// NB: not using srverrors.ServerError() here since the priv checker
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

	nodeFn := func(ctx context.Context, status serverpb.StatusClient, _ roachpb.NodeID) (*serverpb.RangesResponse, error) {
		return status.Ranges(ctx, rangesRequest)
	}
	nowNanos := timeutil.Now().UnixNano()
	responseFn := func(nodeID roachpb.NodeID, rangesResp *serverpb.RangesResponse) {
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

	if err := iterateNodes(
		ctx, s.serverIterator, s.stopper, redact.Sprintf("details about range %d", req.RangeId), noTimeout,
		s.dialNode, nodeFn, responseFn, errorFn,
	); err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}
	return response, nil
}

// ListLocalSessions returns a list of SQL sessions on this node.
func (s *statusServer) ListLocalSessions(
	ctx context.Context, req *serverpb.ListSessionsRequest,
) (*serverpb.ListSessionsResponse, error) {
	sessions, err := s.getLocalSessions(ctx, req)
	if err != nil {
		// NB: not using srverrors.ServerError() here since getLocalSessions
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
func iterateNodes[Client, Result any](
	ctx context.Context,
	iter ServerIterator,
	stopper *stop.Stopper,
	errorCtx redact.RedactableString,
	nodeFnTimeout time.Duration,
	dialFn func(ctx context.Context, nodeID roachpb.NodeID) (Client, error),
	nodeFn func(ctx context.Context, client Client, nodeID roachpb.NodeID) (Result, error),
	responseFn func(nodeID roachpb.NodeID, resp Result),
	errorFn func(nodeID roachpb.NodeID, nodeFnError error),
) error {
	nodeStatuses, err := iter.getAllNodes(ctx)
	if err != nil {
		return err
	}

	// channels for responses and errors.
	type nodeResponse struct {
		nodeID   roachpb.NodeID
		response Result
		err      error
	}

	numNodes := len(nodeStatuses)
	responseChan := make(chan nodeResponse, numNodes)

	nodeQuery := func(ctx context.Context, nodeID roachpb.NodeID) {
		var client Client
		err := timeutil.RunWithTimeout(ctx, "dial node", base.DialTimeout, func(ctx context.Context) error {
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

		var res Result
		if nodeFnTimeout == noTimeout {
			res, err = nodeFn(ctx, client, nodeID)
		} else {
			err = timeutil.RunWithTimeout(ctx, "iterate-nodes-fn",
				nodeFnTimeout, func(ctx context.Context) error {
					var _err error
					res, _err = nodeFn(ctx, client, nodeID)
					return _err
				})
		}

		if err != nil {
			err = errors.Wrapf(err, "error requesting %s from node %d (%s)",
				errorCtx, nodeID, nodeStatuses[serverID(nodeID)])
		}
		responseChan <- nodeResponse{nodeID: nodeID, response: res, err: err}
	}

	// Issue the requests concurrently.
	sem := quotapool.NewIntPool("node status", apiconstants.MaxConcurrentRequests)
	ctx, cancel := stopper.WithCancelOnQuiesce(ctx)
	defer cancel()
	for nodeID := range nodeStatuses {
		nodeID := nodeID // needed to ensure the closure below captures a copy.
		if err := stopper.RunAsyncTaskEx(
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
			resultErr = errors.Wrapf(ctx.Err(), "request of %s canceled before completion", errorCtx)
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
// If non-zero, nodeFn will run with a timeout specified by nodeFnTimeout.
func paginatedIterateNodes[Result any](
	ctx context.Context,
	s *statusServer,
	errorCtx redact.RedactableString,
	limit int,
	pagState paginationState,
	requestedNodes []roachpb.NodeID,
	nodeFnTimeout time.Duration,
	nodeFn func(ctx context.Context, client serverpb.StatusClient, nodeID roachpb.NodeID) ([]Result, error),
	responseFn func(nodeID roachpb.NodeID, resp []Result),
	errorFn func(nodeID roachpb.NodeID, nodeFnError error),
) (next paginationState, err error) {
	if limit == 0 {
		return paginationState{}, iterateNodes(ctx, s.serverIterator, s.stopper, errorCtx, noTimeout,
			s.dialNode, nodeFn, responseFn, errorFn)
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

	paginator := &rpcNodePaginator[serverpb.StatusClient, Result]{
		limit:        limit,
		numNodes:     len(nodeIDs),
		errorCtx:     errorCtx,
		pagState:     pagState,
		nodeStatuses: nodeStatuses,
		dialFn:       s.dialNode,
		nodeFn:       nodeFn,
		responseFn:   responseFn,
		errorFn:      errorFn,
	}

	paginator.init()
	// Issue the requests concurrently.
	sem := quotapool.NewIntPool("node status", apiconstants.MaxConcurrentPaginatedRequests)
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
			func(ctx context.Context) {
				paginator.queryNode(ctx, nodeID, idx, nodeFnTimeout)
			},
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

	nodeFn := func(ctx context.Context, statusClient serverpb.StatusClient, _ roachpb.NodeID) ([]serverpb.Session, error) {
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
	responseFn := func(_ roachpb.NodeID, sessions []serverpb.Session) {
		if len(sessions) == 0 {
			return
		}
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
	if pagState, err = paginatedIterateNodes(
		ctx, s, "session list", limit, start, nil, noTimeout, nodeFn, responseFn, errorFn); err != nil {
		err := serverpb.ListSessionsError{Message: err.Error()}
		response.Errors = append(response.Errors, err)
	}
	return response, pagState, nil
}

// ListSessions returns a list of SQL sessions on all nodes in the cluster.
func (s *statusServer) ListSessions(
	ctx context.Context, req *serverpb.ListSessionsRequest,
) (*serverpb.ListSessionsResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, _, err := s.privilegeChecker.GetUserAndRole(ctx); err != nil {
		// NB: not using srverrors.ServerError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	resp, _, err := s.listSessionsHelper(ctx, req, 0 /* limit */, paginationState{})
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}
	return resp, nil
}

// CancelSession responds to a session cancellation request by canceling the
// target session's associated context.
func (s *statusServer) CancelSession(
	ctx context.Context, req *serverpb.CancelSessionRequest,
) (*serverpb.CancelSessionResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
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
			return nil, srverrors.ServerError(ctx, err)
		}
		return status.CancelSession(ctx, req)
	}

	reqUsername, err := username.MakeSQLUsernameFromPreNormalizedStringChecked(req.Username)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	session, ok := s.sessionRegistry.GetSessionByID(sessionID)
	if !ok {
		return &serverpb.CancelSessionResponse{
			Error: fmt.Sprintf("session ID %s not found", sessionID),
		}, nil
	}

	if err := s.checkCancelPrivilege(ctx, reqUsername, session.SessionUser()); err != nil {
		// NB: not using srverrors.ServerError() here since the priv checker
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
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
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
			return nil, srverrors.ServerError(ctx, err)
		}
		return status.CancelQuery(ctx, req)
	}

	reqUsername, err := username.MakeSQLUsernameFromPreNormalizedStringChecked(req.Username)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	session, ok := s.sessionRegistry.GetSessionByQueryID(queryID)
	if !ok {
		return &serverpb.CancelQueryResponse{
			Error: fmt.Sprintf("query ID %s not found", queryID),
		}, nil
	}

	if err := s.checkCancelPrivilege(ctx, reqUsername, session.SessionUser()); err != nil {
		// NB: not using srverrors.ServerError() here since the priv checker
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
) (*serverpb.CancelQueryByKeyResponse, error) {
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
	defer alloc.Release()

	resp, retErr := func() (*serverpb.CancelQueryByKeyResponse, error) {
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
		ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
		ctx = s.AnnotateCtx(ctx)
		client, err := s.dialNode(ctx, roachpb.NodeID(req.SQLInstanceID))
		if err != nil {
			return nil, err
		}
		return client.CancelQueryByKey(ctx, req)
	}()
	// If the cancellation request failed, then hold on to the semaphore for
	// longer. This helps mitigate a DoS attack of random cancellation requests.
	if retErr != nil || (resp != nil && !resp.Canceled) {
		time.Sleep(1 * time.Second)
	}

	return resp, retErr
}

// ListContentionEvents returns a list of contention events on all nodes in the
// cluster.
func (s *statusServer) ListContentionEvents(
	ctx context.Context, req *serverpb.ListContentionEventsRequest,
) (*serverpb.ListContentionEventsResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	// Check permissions early to avoid fan-out to all nodes.
	if err := s.privilegeChecker.RequireViewActivityOrViewActivityRedactedPermission(ctx); err != nil {
		// NB: not using srverrors.ServerError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	var response serverpb.ListContentionEventsResponse
	nodeFn := func(ctx context.Context, statusClient serverpb.StatusClient, _ roachpb.NodeID) (*serverpb.ListContentionEventsResponse, error) {
		resp, err := statusClient.ListLocalContentionEvents(ctx, req)
		if err != nil {
			return nil, err
		}
		if len(resp.Errors) > 0 {
			return nil, errors.Errorf("%s", resp.Errors[0].Message)
		}
		return resp, nil
	}
	responseFn := func(_ roachpb.NodeID, resp *serverpb.ListContentionEventsResponse) {
		if resp == nil {
			return
		}
		events := resp.Events
		response.Events = contention.MergeSerializedRegistries(response.Events, events)
	}
	errorFn := func(nodeID roachpb.NodeID, err error) {
		errResponse := serverpb.ListActivityError{NodeID: nodeID, Message: err.Error()}
		response.Errors = append(response.Errors, errResponse)
	}

	if err := iterateNodes(ctx, s.serverIterator, s.stopper, "contention events list", noTimeout,
		s.dialNode,
		nodeFn,
		responseFn, errorFn); err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}
	return &response, nil
}

func (s *statusServer) ListDistSQLFlows(
	ctx context.Context, request *serverpb.ListDistSQLFlowsRequest,
) (*serverpb.ListDistSQLFlowsResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	// Check permissions early to avoid fan-out to all nodes.
	if err := s.privilegeChecker.RequireViewActivityOrViewActivityRedactedPermission(ctx); err != nil {
		// NB: not using srverrors.ServerError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	var response serverpb.ListDistSQLFlowsResponse
	nodeFn := func(ctx context.Context, statusClient serverpb.StatusClient, _ roachpb.NodeID) (*serverpb.ListDistSQLFlowsResponse, error) {
		resp, err := statusClient.ListLocalDistSQLFlows(ctx, request)
		if err != nil {
			return nil, err
		}
		if len(resp.Errors) > 0 {
			return nil, errors.Errorf("%s", resp.Errors[0].Message)
		}
		return resp, nil
	}
	responseFn := func(_ roachpb.NodeID, nodeResp *serverpb.ListDistSQLFlowsResponse) {
		if nodeResp == nil {
			return
		}
		response.Flows = mergeDistSQLRemoteFlows(response.Flows, nodeResp.Flows)
	}
	errorFn := func(nodeID roachpb.NodeID, err error) {
		errResponse := serverpb.ListActivityError{NodeID: nodeID, Message: err.Error()}
		response.Errors = append(response.Errors, errResponse)
	}

	if err := iterateNodes(ctx, s.serverIterator, s.stopper, "distsql flows list", noTimeout,
		s.dialNode, nodeFn,
		responseFn, errorFn); err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}
	return &response, nil
}

func (s *statusServer) ListExecutionInsights(
	ctx context.Context, req *serverpb.ListExecutionInsightsRequest,
) (*serverpb.ListExecutionInsightsResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	// Check permissions early to avoid fan-out to all nodes.
	if err := s.privilegeChecker.RequireViewActivityOrViewActivityRedactedPermission(ctx); err != nil {
		// NB: not using srverrors.ServerError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	localRequest := serverpb.ListExecutionInsightsRequest{NodeID: "local"}

	if len(req.NodeID) > 0 {
		requestedNodeID, local, err := s.parseNodeID(req.NodeID)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if local {
			return s.localExecutionInsights(ctx)
		}
		statusClient, err := s.dialNode(ctx, requestedNodeID)
		if err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}
		return statusClient.ListExecutionInsights(ctx, &localRequest)
	}

	var response serverpb.ListExecutionInsightsResponse

	nodeFn := func(ctx context.Context, statusClient serverpb.StatusClient, nodeID roachpb.NodeID) (*serverpb.ListExecutionInsightsResponse, error) {
		resp, err := statusClient.ListExecutionInsights(ctx, &localRequest)
		if err != nil {
			return nil, err
		}
		return resp, nil
	}
	responseFn := func(nodeID roachpb.NodeID, resp *serverpb.ListExecutionInsightsResponse) {
		if resp == nil {
			return
		}
		response.Insights = append(response.Insights, resp.Insights...)
	}
	errorFn := func(nodeID roachpb.NodeID, err error) {
		response.Errors = append(response.Errors, errors.EncodeError(ctx, err))
	}

	if err := iterateNodes(ctx, s.serverIterator, s.stopper, "execution insights list", noTimeout,
		s.dialNode, nodeFn,
		responseFn, errorFn); err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}
	return &response, nil
}

// SpanStats requests the total statistics stored on a node for a given key
// span, which may include multiple ranges.
func (s *statusServer) SpanStats(
	ctx context.Context, req *roachpb.SpanStatsRequest,
) (*roachpb.SpanStatsResponse, error) {
	ctx = s.AnnotateCtx(ctx)
	if err := s.privilegeChecker.RequireViewClusterMetadataPermission(ctx); err != nil {
		// NB: not using srverrors.ServerError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	if err := verifySpanStatsRequest(ctx, req, s.st.Version); err != nil {
		return nil, err
	}

	batchSize := int(builtins.SpanStatsBatchLimit.Get(&s.st.SV))
	return batchedSpanStats(ctx, req, s.sqlServer.tenantConnect.SpanStats, batchSize)
}

func (s *systemStatusServer) SpanStats(
	ctx context.Context, req *roachpb.SpanStatsRequest,
) (*roachpb.SpanStatsResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)
	if err := s.privilegeChecker.RequireViewClusterMetadataPermission(ctx); err != nil {
		// NB: not using srverrors.ServerError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	if err := verifySpanStatsRequest(ctx, req, s.st.Version); err != nil {
		return nil, err
	}

	batchSize := int(builtins.SpanStatsBatchLimit.Get(&s.st.SV))
	return batchedSpanStats(ctx, req, s.getSpanStatsInternal, batchSize)
}

func (s *systemStatusServer) TenantServiceStatus(
	ctx context.Context, req *serverpb.TenantServiceStatusRequest,
) (*serverpb.TenantServiceStatusResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	resp := &serverpb.TenantServiceStatusResponse{
		StatusByNodeID: make(map[roachpb.NodeID]mtinfopb.SQLInfo),
		ErrorsByNodeID: make(map[roachpb.NodeID]string),
	}
	if len(req.NodeID) > 0 {
		reqNodeID, local, err := s.parseNodeID(req.NodeID)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}

		if local {
			info, err := s.localTenantServiceStatus(ctx, req)
			if err != nil {
				return nil, err
			}
			resp.StatusByNodeID[reqNodeID] = info
			return resp, nil
		}
		return nil, errors.AssertionFailedf("requesting service status on a specific node is not supported yet")
	}

	// Send TenantStatusService request to all stores on all nodes.
	remoteRequest := serverpb.TenantServiceStatusRequest{NodeID: "local", TenantID: req.TenantID}
	nodeFn := func(ctx context.Context, status serverpb.StatusClient, _ roachpb.NodeID) (*serverpb.TenantServiceStatusResponse, error) {
		return status.TenantServiceStatus(ctx, &remoteRequest)
	}
	responseFn := func(nodeID roachpb.NodeID, remoteResp *serverpb.TenantServiceStatusResponse) {
		resp.StatusByNodeID[nodeID] = remoteResp.StatusByNodeID[nodeID]
	}
	errorFn := func(nodeID roachpb.NodeID, err error) {
		resp.ErrorsByNodeID[nodeID] = err.Error()
	}

	if err := iterateNodes(ctx, s.serverIterator, s.stopper, "tenant service status",
		noTimeout,
		s.dialNode,
		nodeFn,
		responseFn,
		errorFn,
	); err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	return resp, nil
}

func (s *systemStatusServer) localTenantServiceStatus(
	ctx context.Context, req *serverpb.TenantServiceStatusRequest,
) (mtinfopb.SQLInfo, error) {
	ret := mtinfopb.SQLInfo{}
	r, err := s.sqlServer.execCfg.TenantCapabilitiesReader.Get("tenant service status")
	if err != nil {
		return ret, err
	}
	tid, err := roachpb.MakeTenantID(req.TenantID)
	if err != nil {
		return ret, err
	}
	entry, _, found := r.GetInfo(tid)
	if !found {
		return ret, nil
	}
	ret.ID = entry.TenantID.ToUint64()
	ret.Name = entry.Name
	ret.DataState = entry.DataState
	ret.ServiceMode = entry.ServiceMode
	return ret, nil
}

// Diagnostics returns an anonymized diagnostics report.
func (s *statusServer) Diagnostics(
	ctx context.Context, req *serverpb.DiagnosticsRequest,
) (*diagnosticspb.DiagnosticReport, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)
	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if !local {
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}
		return status.Diagnostics(ctx, req)
	}

	return s.sqlServer.diagnosticsReporter.CreateReport(ctx, telemetry.ReadOnly), nil
}

// Stores returns details for each store.
func (s *systemStatusServer) Stores(
	ctx context.Context, req *serverpb.StoresRequest,
) (*serverpb.StoresResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.RequireViewClusterMetadataPermission(ctx); err != nil {
		// NB: not using srverrors.ServerError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if !local {
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}
		return status.Stores(ctx, req)
	}

	resp := &serverpb.StoresResponse{}
	err = s.stores.VisitStores(func(store *kvserver.Store) error {
		eng := store.TODOEngine()
		envStats, err := eng.GetEnvStats()
		if err != nil {
			return err
		}
		props := eng.Properties()
		storeDetails := serverpb.StoreDetails{
			StoreID:          store.Ident.StoreID,
			NodeID:           nodeID,
			EncryptionStatus: envStats.EncryptionStatus,
			TotalFiles:       envStats.TotalFiles,
			TotalBytes:       envStats.TotalBytes,
			ActiveKeyFiles:   envStats.ActiveKeyFiles,
			ActiveKeyBytes:   envStats.ActiveKeyBytes,
			Dir:              props.Dir,
		}
		if props.WalFailoverPath != nil {
			storeDetails.WalFailoverPath = *props.WalFailoverPath
		}
		resp.Stores = append(resp.Stores, storeDetails)
		return nil
	})
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
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
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.RequireViewClusterMetadataPermission(ctx); err != nil {
		// NB: not using srverrors.ServerError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if !local {
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, srverrors.ServerError(ctx, err)
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
	ctx = s.AnnotateCtx(authserver.ForwardSQLIdentityThroughRPCCalls(ctx))

	if err := s.privilegeChecker.RequireViewClusterMetadataPermission(ctx); err != nil {
		// NB: not using srverrors.ServerError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	j, err := s.sqlServer.jobRegistry.LoadJob(ctx, jobspb.JobID(req.JobId))
	if err != nil {
		if je := (*jobs.JobNotFoundError)(nil); errors.As(err, &je) {
			return nil, status.Errorf(codes.NotFound, "%v", err)
		}
		return nil, srverrors.ServerError(ctx, err)
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
	ctx = s.AnnotateCtx(authserver.ForwardSQLIdentityThroughRPCCalls(ctx))
	if err := s.privilegeChecker.RequireViewClusterMetadataPermission(ctx); err != nil {
		return nil, err
	}

	requestedNodeID, local, err := s.parseNodeID(req.CoordinatorID)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
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
	ctx = s.AnnotateCtx(authserver.ForwardSQLIdentityThroughRPCCalls(ctx))

	if err := s.privilegeChecker.RequireViewActivityOrViewActivityRedactedPermission(ctx); err != nil {
		return nil, err
	}

	user, isAdmin, err := s.privilegeChecker.GetUserAndRole(ctx)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	shouldRedactContendingKey := false
	if !isAdmin {
		shouldRedactContendingKey, err =
			s.privilegeChecker.HasRoleOption(ctx, user, roleoption.VIEWACTIVITYREDACTED)
		if err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}
	}

	if roachpb.NodeID(s.serverIterator.getID()) == 0 {
		return nil, status.Errorf(codes.Unavailable, "nodeID not set")
	}

	if len(req.NodeID) > 0 {
		requestedNodeID, local, err := s.parseNodeID(req.NodeID)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
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

	rpcCallFn := func(ctx context.Context, statusClient serverpb.StatusClient, _ roachpb.NodeID) (*serverpb.TransactionContentionEventsResponse, error) {
		return statusClient.TransactionContentionEvents(ctx, &serverpb.TransactionContentionEventsRequest{
			NodeID: "local",
		})
	}

	resp := &serverpb.TransactionContentionEventsResponse{
		Events: make([]contentionpb.ExtendedContentionEvent, 0),
	}

	if err := iterateNodes(ctx, s.serverIterator, s.stopper, "txn contention events for node",
		noTimeout,
		s.dialNode,
		rpcCallFn,
		func(nodeID roachpb.NodeID, nodeResp *serverpb.TransactionContentionEventsResponse) {
			resp.Events = append(resp.Events, nodeResp.Events...)
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

// GetJobProfilerExecutionDetails reads all the stored execution details for a
// given job ID.
func (s *statusServer) GetJobProfilerExecutionDetails(
	ctx context.Context, req *serverpb.GetJobProfilerExecutionDetailRequest,
) (*serverpb.GetJobProfilerExecutionDetailResponse, error) {
	ctx = s.AnnotateCtx(ctx)
	err := s.privilegeChecker.RequireViewClusterMetadataPermission(ctx)
	if err != nil {
		return nil, err
	}

	jobID := jobspb.JobID(req.JobId)
	execCfg := s.sqlServer.execCfg
	var data []byte
	if err := execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		data, err = jobs.ReadExecutionDetailFile(ctx, req.Filename, txn, jobID)
		return err
	}); err != nil {
		return nil, err
	}
	return &serverpb.GetJobProfilerExecutionDetailResponse{Data: data}, nil
}

// ListJobProfilerExecutionDetails lists all the stored execution details for a
// given job ID.
func (s *statusServer) ListJobProfilerExecutionDetails(
	ctx context.Context, req *serverpb.ListJobProfilerExecutionDetailsRequest,
) (*serverpb.ListJobProfilerExecutionDetailsResponse, error) {
	ctx = s.AnnotateCtx(ctx)
	err := s.privilegeChecker.RequireViewClusterMetadataPermission(ctx)
	if err != nil {
		return nil, err
	}

	jobID := jobspb.JobID(req.JobId)
	execCfg := s.sqlServer.execCfg
	files, err := jobs.ListExecutionDetailFiles(ctx, execCfg.InternalDB, jobID)
	if err != nil {
		return nil, err
	}
	return &serverpb.ListJobProfilerExecutionDetailsResponse{Files: files}, nil
}

func (s *statusServer) localUpdateTableMetadataCache() (
	*serverpb.UpdateTableMetadataCacheResponse,
	error,
) {
	select {
	case s.updateTableMetadataJobSignal <- struct{}{}:
	default:
		return nil, status.Errorf(codes.Aborted, "update table metadata cache job is not ready to start execution")
	}
	return &serverpb.UpdateTableMetadataCacheResponse{}, nil
}

func (s *statusServer) UpdateTableMetadataCache(
	ctx context.Context, req *serverpb.UpdateTableMetadataCacheRequest,
) (*serverpb.UpdateTableMetadataCacheResponse, error) {
	_, isAdmin, _ := s.privilegeChecker.GetUserAndRole(ctx)
	if !isAdmin {
		return nil, status.Error(codes.PermissionDenied, updateTableMetadataCachePermissionErrMsg)
	}
	if req.Local {
		return s.localUpdateTableMetadataCache()
	}
	ctx = s.AnnotateCtx(ctx)

	// Get the node id for the job.
	row, err := s.internalExecutor.QueryRow(ctx, "get-node-id", nil, `
SELECT claim_instance_id
FROM system.jobs
WHERE id = $1
`, jobs.UpdateTableMetadataCacheJobID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s", err.Error())
	}
	if row == nil {
		return nil, status.Error(codes.FailedPrecondition, "no job record found")
	}
	if row[0] == tree.DNull {
		return nil, status.Error(codes.Unavailable, "update table metadata cache job is unclaimed")
	}

	nodeID := roachpb.NodeID(*row[0].(*tree.DInt))
	statusClient, err := s.dialNode(ctx, nodeID)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}
	return statusClient.UpdateTableMetadataCache(ctx, &serverpb.UpdateTableMetadataCacheRequest{
		Local: true,
	})
}

// GetUpdateTableMetadataCacheSignal returns the signal channel used
// in the UpdateTableMetadataCache rpc.
func (s *statusServer) GetUpdateTableMetadataCacheSignal() chan struct{} {
	return s.updateTableMetadataJobSignal
}

func (s *statusServer) GetThrottlingMetadata(
	ctx context.Context, req *serverpb.GetThrottlingMetadataRequest,
) (*serverpb.GetThrottlingMetadataResponse, error) {

	if len(req.NodeID) > 0 {
		requestedNodeID, local, err := s.parseNodeID(req.NodeID)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if local {
			return s.getThrottlingMetadataLocal(ctx)
		}

		statusClient, err := s.dialNode(ctx, requestedNodeID)
		if err != nil {
			return nil, err
		}
		return statusClient.GetThrottlingMetadata(ctx, req)
	}

	rpcCallFn := func(ctx context.Context, statusClient serverpb.StatusClient, _ roachpb.NodeID) (*serverpb.GetThrottlingMetadataResponse, error) {
		return statusClient.GetThrottlingMetadata(ctx, &serverpb.GetThrottlingMetadataRequest{
			NodeID: "local",
		})
	}

	resp := &serverpb.GetThrottlingMetadataResponse{}

	if err := iterateNodes(ctx, s.serverIterator, s.stopper, "throttling metadata for node",
		noTimeout,
		s.dialNode,
		rpcCallFn,
		func(nodeID roachpb.NodeID, nodeResp *serverpb.GetThrottlingMetadataResponse) {
			if !resp.Throttled && nodeResp.Throttled {
				resp.Throttled = true
				resp.ThrottleExplanation = nodeResp.ThrottleExplanation
			}
			if !resp.HasGracePeriod && nodeResp.HasGracePeriod {
				resp.HasGracePeriod = true
				resp.GracePeriodEndSeconds = nodeResp.GracePeriodEndSeconds
			}
			if !resp.HasTelemetryDeadline && nodeResp.HasTelemetryDeadline {
				resp.HasTelemetryDeadline = true
				resp.TelemetryDeadlineSeconds = nodeResp.TelemetryDeadlineSeconds
				resp.LastTelemetryReceivedSeconds = nodeResp.LastTelemetryReceivedSeconds
			}
			// Telemetry deadlines can vary from node to node based on timing
			// issues or firewalls (as opposed to license expiry which is a
			// global cluster state). For that reason we will accumulate all
			// nodeIDs which haven't sent telemetry in a day.
			if nodeResp.HasTelemetryDeadline && nodeResp.LastTelemetryReceivedSeconds < timeutil.Now().Add(-24*time.Hour).Unix() {
				resp.NodeIdsWithTelemetryProblems = append(resp.NodeIdsWithTelemetryProblems, nodeID.String())
			}
		},
		func(nodeID roachpb.NodeID, nodeFnError error) {
		},
	); err != nil {
		return nil, err
	}

	return resp, nil
}

func (s *statusServer) getThrottlingMetadataLocal(
	ctx context.Context,
) (*serverpb.GetThrottlingMetadataResponse, error) {
	e := s.sqlServer.execCfg.LicenseEnforcer

	// We use 100 here as a number that's definitely more than what's
	// allowed to trigger the throttling error if possible.
	// TODO(davidh): consider saving the pg notice here as an explanation if err is nil
	_, err := e.MaybeFailIfThrottled(ctx, 100 /* txnsOpened */)
	throttleExplanation := ""
	if err != nil {
		throttleExplanation = err.Error()
	}
	gpe, okGrace := e.GetGracePeriodEndTS()
	deadline, lastPing, okTelem := e.GetTelemetryDeadline()
	return &serverpb.GetThrottlingMetadataResponse{
			Throttled:                    err != nil,
			ThrottleExplanation:          throttleExplanation,
			HasGracePeriod:               okGrace,
			GracePeriodEndSeconds:        gpe.Unix(),
			TelemetryDeadlineSeconds:     deadline.Unix(),
			LastTelemetryReceivedSeconds: lastPing.Unix(),
			HasTelemetryDeadline:         okTelem,
		},
		nil
}
