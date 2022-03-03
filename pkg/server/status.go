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
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/diagnostics/diagnosticspb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/contention"
	"github.com/cockroachdb/cockroach/pkg/sql/contentionpb"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirecancel"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	gwruntime "github.com/grpc-ecosystem/grpc-gateway/runtime"
	raft "go.etcd.io/etcd/raft/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
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
}

func propagateGatewayMetadata(ctx context.Context) context.Context {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		return metadata.NewOutgoingContext(ctx, md)
	}
	return ctx
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
	privilegeChecker *adminPrivilegeChecker
	sessionRegistry  *sql.SessionRegistry
	flowScheduler    *flowinfra.FlowScheduler
	st               *cluster.Settings
	sqlServer        *SQLServer
	rpcCtx           *rpc.Context
	stopper          *stop.Stopper
}

// getLocalSessions returns a list of local sessions on this node. Note that the
// NodeID field is unset.
func (b *baseStatusServer) getLocalSessions(
	ctx context.Context, req *serverpb.ListSessionsRequest,
) ([]serverpb.Session, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = b.AnnotateCtx(ctx)

	sessionUser, isAdmin, err := b.privilegeChecker.getUserAndRole(ctx)
	if err != nil {
		return nil, serverError(ctx, err)
	}

	reqUsername, err := security.MakeSQLUsernameFromPreNormalizedStringChecked(req.Username)
	if err != nil {
		return nil, serverError(ctx, err)
	}

	errViewActivity := b.privilegeChecker.requireViewActivityOrViewActivityRedactedPermission(ctx)
	// TODO(knz): The following check on errViewActivity is incorrect, it
	// does not properly handle non-privilege errors.
	// See: https://github.com/cockroachdb/cockroach/issues/76288
	if !isAdmin && errViewActivity != nil {
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

	hasViewActivityRedacted, err := b.privilegeChecker.hasRoleOption(ctx, sessionUser, roleoption.VIEWACTIVITYREDACTED)
	if err != nil {
		return nil, serverError(ctx, err)
	}

	// The empty username means "all sessions".
	showAll := reqUsername.Undefined()

	registry := b.sessionRegistry
	sessions := registry.SerializeAll()
	userSessions := make([]serverpb.Session, 0, len(sessions))

	for _, session := range sessions {
		if reqUsername.Normalized() != session.Username && !showAll {
			continue
		}

		if !isAdmin && hasViewActivityRedacted && (sessionUser != reqUsername) {
			// Remove queries with constants if user doesn't have correct privileges.
			// Note that users can have both VIEWACTIVITYREDACTED and VIEWACTIVITY,
			// with the former taking precedence.
			for _, query := range session.ActiveQueries {
				query.Sql = ""
			}
			session.LastActiveQuery = ""
		}

		userSessions = append(userSessions, session)
	}
	return userSessions, nil
}

type sessionFinder func(sessions []serverpb.Session) (serverpb.Session, error)

func findSessionBySessionID(sessionID []byte) sessionFinder {
	return func(sessions []serverpb.Session) (serverpb.Session, error) {
		var session serverpb.Session
		for _, s := range sessions {
			if bytes.Equal(sessionID, s.ID) {
				session = s
				break
			}
		}
		if len(session.ID) == 0 {
			return session, fmt.Errorf("session ID %s not found", sql.BytesToClusterWideID(sessionID))
		}
		return session, nil
	}
}

func findSessionByQueryID(queryID string) sessionFinder {
	return func(sessions []serverpb.Session) (serverpb.Session, error) {
		var session serverpb.Session
		for _, s := range sessions {
			for _, q := range s.ActiveQueries {
				if queryID == q.ID {
					session = s
					break
				}
			}
		}
		if len(session.ID) == 0 {
			return session, fmt.Errorf("query ID %s not found", queryID)
		}
		return session, nil
	}
}

func (b *baseStatusServer) checkCancelPrivilege(
	ctx context.Context, username security.SQLUsername, findSession sessionFinder,
) error {
	ctx = propagateGatewayMetadata(ctx)
	ctx = b.AnnotateCtx(ctx)
	// reqUser is the user who made the cancellation request.
	var reqUser security.SQLUsername
	{
		sessionUser, isAdmin, err := b.privilegeChecker.getUserAndRole(ctx)
		if err != nil {
			return err
		}
		if username.Undefined() || username == sessionUser {
			reqUser = sessionUser
		} else {
			// When CANCEL QUERY is run as a SQL statement, sessionUser is always root
			// and the user who ran the statement is passed as req.Username.
			if !isAdmin {
				return errRequiresAdmin
			}
			reqUser = username
		}
	}

	hasAdmin, err := b.privilegeChecker.hasAdminRole(ctx, reqUser)
	if err != nil {
		return serverError(ctx, err)
	}

	if !hasAdmin {
		// Check if the user has permission to see the session.
		session, err := findSession(b.sessionRegistry.SerializeAll())
		if err != nil {
			return serverError(ctx, err)
		}

		sessionUser := security.MakeSQLUsernameFromPreNormalizedString(session.Username)
		if sessionUser != reqUser {
			// Must have CANCELQUERY privilege to cancel other users'
			// sessions/queries.
			ok, err := b.privilegeChecker.hasRoleOption(ctx, reqUser, roleoption.CANCELQUERY)
			if err != nil {
				return serverError(ctx, err)
			}
			if !ok {
				return errRequiresRoleOption(roleoption.CANCELQUERY)
			}
			// Non-admins cannot cancel admins' sessions/queries.
			isAdminSession, err := b.privilegeChecker.hasAdminRole(ctx, sessionUser)
			if err != nil {
				return serverError(ctx, err)
			}
			if isAdminSession {
				return status.Error(
					codes.PermissionDenied, "permission denied to cancel admin session")
			}
		}
	}

	return nil
}

// ListLocalContentionEvents returns a list of contention events on this node.
func (b *baseStatusServer) ListLocalContentionEvents(
	ctx context.Context, _ *serverpb.ListContentionEventsRequest,
) (*serverpb.ListContentionEventsResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
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
	ctx = propagateGatewayMetadata(ctx)
	ctx = b.AnnotateCtx(ctx)

	if err := b.privilegeChecker.requireViewActivityOrViewActivityRedactedPermission(ctx); err != nil {
		// NB: not using serverError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	nodeIDOrZero, _ := b.sqlServer.sqlIDContainer.OptionalNodeID()

	running, queued := b.flowScheduler.Serialize()
	response := &serverpb.ListDistSQLFlowsResponse{
		Flows: make([]serverpb.DistSQLRemoteFlows, 0, len(running)+len(queued)),
	}
	for _, f := range running {
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
	for _, f := range queued {
		response.Flows = append(response.Flows, serverpb.DistSQLRemoteFlows{
			FlowID: f.FlowID,
			Infos: []serverpb.DistSQLRemoteFlows_Info{{
				NodeID:    nodeIDOrZero,
				Timestamp: f.Timestamp,
				Status:    serverpb.DistSQLRemoteFlows_QUEUED,
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
	admin                    *adminServer
	db                       *kv.DB
	gossip                   *gossip.Gossip
	metricSource             metricMarshaler
	nodeLiveness             *liveness.NodeLiveness
	storePool                *kvserver.StorePool
	stores                   *kvserver.Stores
	si                       systemInfoOnce
	stmtDiagnosticsRequester StmtDiagnosticsRequester
	internalExecutor         *sql.InternalExecutor
}

// StmtDiagnosticsRequester is the interface into *stmtdiagnostics.Registry
// used by AdminUI endpoints.
type StmtDiagnosticsRequester interface {
	// InsertRequest adds an entry to system.statement_diagnostics_requests for
	// tracing a query with the given fingerprint. Once this returns, calling
	// stmtdiagnostics.ShouldCollectDiagnostics() on the current node will
	// return true for the given fingerprint.
	// - minExecutionLatency, if non-zero, determines the minimum execution
	// latency of a query that satisfies the request. In other words, queries
	// that ran faster than minExecutionLatency do not satisfy the condition
	// and the bundle is not generated for them.
	// - expiresAfter, if non-zero, indicates for how long the request should
	// stay active.
	InsertRequest(
		ctx context.Context,
		stmtFingerprint string,
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
	adminServer *adminServer,
	db *kv.DB,
	gossip *gossip.Gossip,
	metricSource metricMarshaler,
	nodeLiveness *liveness.NodeLiveness,
	storePool *kvserver.StorePool,
	rpcCtx *rpc.Context,
	stores *kvserver.Stores,
	stopper *stop.Stopper,
	sessionRegistry *sql.SessionRegistry,
	flowScheduler *flowinfra.FlowScheduler,
	internalExecutor *sql.InternalExecutor,
) *statusServer {
	ambient.AddLogTag("status", nil)
	server := &statusServer{
		baseStatusServer: &baseStatusServer{
			AmbientContext:   ambient,
			privilegeChecker: adminAuthzCheck,
			sessionRegistry:  sessionRegistry,
			flowScheduler:    flowScheduler,
			st:               st,
			rpcCtx:           rpcCtx,
			stopper:          stopper,
		},
		cfg:              cfg,
		admin:            adminServer,
		db:               db,
		gossip:           gossip,
		metricSource:     metricSource,
		nodeLiveness:     nodeLiveness,
		storePool:        storePool,
		stores:           stores,
		internalExecutor: internalExecutor,
	}

	return server
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

func (s *statusServer) parseNodeID(nodeIDParam string) (roachpb.NodeID, bool, error) {
	return parseNodeID(s.gossip, nodeIDParam)
}

func parseNodeID(
	gossip *gossip.Gossip, nodeIDParam string,
) (nodeID roachpb.NodeID, isLocal bool, err error) {
	// No parameter provided or set to local.
	if len(nodeIDParam) == 0 || localRE.MatchString(nodeIDParam) {
		return gossip.NodeID.Get(), true, nil
	}

	id, err := strconv.ParseInt(nodeIDParam, 0, 32)
	if err != nil {
		return 0, false, errors.Wrap(err, "node ID could not be parsed")
	}
	nodeID = roachpb.NodeID(id)
	return nodeID, nodeID == gossip.NodeID.Get(), nil
}

func (s *statusServer) dialNode(
	ctx context.Context, nodeID roachpb.NodeID,
) (serverpb.StatusClient, error) {
	addr, err := s.gossip.GetNodeIDAddress(nodeID)
	if err != nil {
		return nil, err
	}
	conn, err := s.rpcCtx.GRPCDialNode(addr.String(), nodeID,
		rpc.DefaultClass).Connect(ctx)
	if err != nil {
		return nil, err
	}
	return serverpb.NewStatusClient(conn), nil
}

// Gossip returns gossip network status.
func (s *statusServer) Gossip(
	ctx context.Context, req *serverpb.GossipRequest,
) (*gossip.InfoStatus, error) {
	ctx = propagateGatewayMetadata(ctx)
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

func (s *statusServer) EngineStats(
	ctx context.Context, req *serverpb.EngineStatsRequest,
) (*serverpb.EngineStatsResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
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
			EngineType:           store.Engine().Type(),
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
func (s *statusServer) Allocator(
	ctx context.Context, req *serverpb.AllocatorRequest,
) (*serverpb.AllocatorResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
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
					var allocatorSpans tracing.Recording
					allocatorSpans, err = store.AllocatorDryRun(ctx, rep)
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
			allocatorSpans, err := store.AllocatorDryRun(ctx, rep)
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

// AllocatorRange returns simulated allocator info for the requested range.
func (s *statusServer) AllocatorRange(
	ctx context.Context, req *serverpb.AllocatorRangeRequest,
) (*serverpb.AllocatorRangeResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, err := s.privilegeChecker.requireAdminUser(ctx); err != nil {
		// NB: not using serverError() here since the priv checker
		// already returns a proper gRPC error status.
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
				_ = contextutil.RunWithTimeout(ctx, "allocator range", base.NetworkTimeout, func(ctx context.Context) error {
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
	ctx = propagateGatewayMetadata(ctx)
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
			details.Data = cert.FileContents
			if err := extractCertFields(details.Data, &details); err != nil {
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
	ctx = propagateGatewayMetadata(ctx)
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

	remoteNodeID := s.gossip.NodeID.Get()
	resp := &serverpb.DetailsResponse{
		NodeID:     remoteNodeID,
		BuildInfo:  build.GetInfo(),
		SystemInfo: s.si.systemInfo(ctx),
	}
	if addr, err := s.gossip.GetNodeIDAddress(remoteNodeID); err == nil {
		resp.Address = *addr
	}
	if addr, err := s.gossip.GetNodeIDSQLAddress(remoteNodeID); err == nil {
		resp.SQLAddress = *addr
	}

	return resp, nil
}

// GetFiles returns a list of files of type defined in the request.
func (s *statusServer) GetFiles(
	ctx context.Context, req *serverpb.GetFilesRequest,
) (*serverpb.GetFilesResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
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
//    groupA:
//      dir: dir1
//    groupB:
//      dir: dir2
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
	ctx = propagateGatewayMetadata(ctx)
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
	ctx = propagateGatewayMetadata(ctx)
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
	for {
		var entry logpb.Entry
		if err := decoder.Decode(&entry); err != nil {
			if err == io.EOF {
				break
			}
			return nil, serverError(ctx, err)
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
// * "starttime" query parameter filters the log entries to only ones that
//   occurred on or after the "starttime". Defaults to a day ago.
// * "endtime" query parameter filters the log entries to only ones that
//   occurred before on on the "endtime". Defaults to the current time.
// * "pattern" query parameter filters the log entries by the provided regexp
//   pattern if it exists. Defaults to nil.
// * "max" query parameter is the hard limit of the number of returned log
//   entries. Defaults to defaultMaxLogEntries.
// To filter the log messages to only retrieve messages from a given level,
// use a pattern that excludes all messages at the undesired levels.
// (e.g. "^[^IW]" to only get errors, fatals and panics). An exclusive
// pattern is better because panics and some other errors do not use
// a prefix character.
func (s *statusServer) Logs(
	ctx context.Context, req *serverpb.LogsRequest,
) (*serverpb.LogEntriesResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
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

	return &serverpb.LogEntriesResponse{Entries: entries}, nil
}

// Stacks returns goroutine or thread stack traces.
func (s *statusServer) Stacks(
	ctx context.Context, req *serverpb.StacksRequest,
) (*serverpb.JSONResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
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
	ctx = propagateGatewayMetadata(ctx)
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

// Regions implements the serverpb.Status interface.
func (s *statusServer) Regions(
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
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	// The node status contains details about the command line, network
	// addresses, env vars etc which are admin-only.
	if _, err := s.privilegeChecker.requireAdminUser(ctx); err != nil {
		// NB: not using serverError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}
	statuses, _, err := s.getNodeStatuses(ctx, 0 /* limit */, 0 /* offset */)
	if err != nil {
		return nil, serverError(ctx, err)
	}
	resp := &serverpb.NodesListResponse{
		Nodes: make([]serverpb.NodeDetails, len(statuses)),
	}
	for i, status := range statuses {
		resp.Nodes[i].NodeID = int32(status.Desc.NodeID)
		resp.Nodes[i].Address = status.Desc.Address
		resp.Nodes[i].SQLAddress = status.Desc.SQLAddress
	}
	return resp, nil
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
func (s *statusServer) Nodes(
	ctx context.Context, req *serverpb.NodesRequest,
) (*serverpb.NodesResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	// The node status contains details about the command line, network
	// addresses, env vars etc which are admin-only.
	if _, err := s.privilegeChecker.requireAdminUser(ctx); err != nil {
		// NB: not using serverError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	resp, _, err := s.nodesHelper(ctx, 0 /* limit */, 0 /* offset */)
	if err != nil {
		return nil, serverError(ctx, err)
	}
	return resp, nil
}

func (s *statusServer) NodesUI(
	ctx context.Context, req *serverpb.NodesRequest,
) (*serverpb.NodesResponseExternal, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	_, isAdmin, err := s.privilegeChecker.getUserAndRole(ctx)
	if err != nil {
		return nil, serverError(ctx, err)
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
		resp.Nodes[i] = nodeStatusToResp(&nodeStatus, isAdmin)
	}

	return resp, nil
}

func nodeStatusToResp(n *statuspb.NodeStatus, isAdmin bool) serverpb.NodeResponse {
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
			Incoming: v.Incoming,
			Outgoing: v.Outgoing,
			Latency:  v.Latency,
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
			if isAdmin {
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

	if isAdmin {
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
func (s *statusServer) ListNodesInternal(
	ctx context.Context, req *serverpb.NodesRequest,
) (*serverpb.NodesResponse, error) {
	resp, _, err := s.nodesHelper(ctx, 0 /* limit */, 0 /* offset */)
	return resp, err
}

// Note that the function returns plain errors, and it is the caller's
// responsibility to convert them to serverErrors.
func (s *statusServer) getNodeStatuses(
	ctx context.Context, limit, offset int,
) (statuses []statuspb.NodeStatus, next int, _ error) {
	startKey := keys.StatusNodePrefix
	endKey := startKey.PrefixEnd()

	b := &kv.Batch{}
	b.Scan(startKey, endKey)
	if err := s.db.Run(ctx, b); err != nil {
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
func (s *statusServer) nodesHelper(
	ctx context.Context, limit, offset int,
) (*serverpb.NodesResponse, int, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	statuses, next, err := s.getNodeStatuses(ctx, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	resp := serverpb.NodesResponse{
		Nodes: statuses,
	}

	clock := s.admin.server.clock
	resp.LivenessByNodeID = getLivenessStatusMap(s.nodeLiveness, clock.Now().GoTime(), s.st)
	return &resp, next, nil
}

// nodesStatusWithLiveness is like Nodes but for internal
// use within this package.
//
// Note that the function returns plain errors, and it is the caller's
// responsibility to convert them to serverErrors.
func (s *statusServer) nodesStatusWithLiveness(
	ctx context.Context,
) (map[roachpb.NodeID]nodeStatusWithLiveness, error) {
	nodes, err := s.ListNodesInternal(ctx, nil)
	if err != nil {
		return nil, err
	}
	clock := s.admin.server.clock
	statusMap := getLivenessStatusMap(s.nodeLiveness, clock.Now().GoTime(), s.st)
	ret := make(map[roachpb.NodeID]nodeStatusWithLiveness)
	for _, node := range nodes.Nodes {
		nodeID := node.Desc.NodeID
		livenessStatus := statusMap[nodeID]
		if livenessStatus == livenesspb.NodeLivenessStatus_DECOMMISSIONED {
			// Skip over removed nodes.
			continue
		}
		ret[nodeID] = nodeStatusWithLiveness{
			NodeStatus:     node,
			livenessStatus: livenessStatus,
		}
	}
	return ret, nil
}

// nodeStatusWithLiveness combines a NodeStatus with a NodeLivenessStatus.
type nodeStatusWithLiveness struct {
	statuspb.NodeStatus
	livenessStatus livenesspb.NodeLivenessStatus
}

// handleNodeStatus handles GET requests for a single node's status.
func (s *statusServer) Node(
	ctx context.Context, req *serverpb.NodeRequest,
) (*statuspb.NodeStatus, error) {
	ctx = propagateGatewayMetadata(ctx)
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
	ctx = propagateGatewayMetadata(ctx)
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
	ctx = propagateGatewayMetadata(ctx)
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
func (s *statusServer) RaftDebug(
	ctx context.Context, req *serverpb.RaftDebugRequest,
) (*serverpb.RaftDebugResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, err := s.privilegeChecker.requireAdminUser(ctx); err != nil {
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
func (s *statusServer) Ranges(
	ctx context.Context, req *serverpb.RangesRequest,
) (*serverpb.RangesResponse, error) {
	resp, _, err := s.rangesHelper(ctx, req, 0, 0)
	return resp, err
}

// Ranges returns range info for the specified node.
func (s *statusServer) rangesHelper(
	ctx context.Context, req *serverpb.RangesRequest, limit, offset int,
) (*serverpb.RangesResponse, int, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, err := s.privilegeChecker.requireAdminUser(ctx); err != nil {
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
		qps, _ := rep.QueriesPerSecond()
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
				QueriesPerSecond: qps,
				WritesPerSecond:  rep.WritesPerSecond(),
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
	if len(req.RangeIDs) > 0 {
		var outputInterface interface{}
		outputInterface, next = simplePaginate(output.Ranges, limit, offset)
		output.Ranges = outputInterface.([]serverpb.RangeInfo)
	}
	return &output, next, nil
}

func (s *statusServer) TenantRanges(
	ctx context.Context, _ *serverpb.TenantRangesRequest,
) (*serverpb.TenantRangesResponse, error) {
	propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)
	if _, err := s.privilegeChecker.requireAdminUser(ctx); err != nil {
		return nil, err
	}

	tID, ok := roachpb.TenantFromContext(ctx)
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
	for nodeID := range replicaNodeIDs {
		nodeIDString := nodeID.String()
		_, local, err := s.parseNodeID(nodeIDString)
		if err != nil {
			return nil, status.Errorf(codes.Internal, err.Error())
		}

		req := &serverpb.RangesRequest{
			NodeId:   nodeIDString,
			RangeIDs: rangeIDs,
		}

		var resp *serverpb.RangesResponse
		if local {
			resp, _, err = s.rangesHelper(ctx, req, 0, 0)
			if err != nil {
				return nil, err
			}
		} else {
			statusServer, err := s.dialNode(ctx, nodeID)
			if err != nil {
				return nil, serverError(ctx, err)
			}

			resp, err = statusServer.Ranges(ctx, req)
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
func (s *statusServer) HotRanges(
	ctx context.Context, req *serverpb.HotRangesRequest,
) (*serverpb.HotRangesResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, err := s.privilegeChecker.requireAdminUser(ctx); err != nil {
		// NB: not using serverError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	response := &serverpb.HotRangesResponse{
		NodeID:            s.gossip.NodeID.Get(),
		HotRangesByNodeID: make(map[roachpb.NodeID]serverpb.HotRangesResponse_NodeResponse),
	}

	if len(req.NodeID) > 0 {
		requestedNodeID, local, err := s.parseNodeID(req.NodeID)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, err.Error())
		}

		// Only hot ranges from the local node.
		if local {
			response.HotRangesByNodeID[requestedNodeID] = s.localHotRanges(ctx)
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

type hotRangeReportMeta struct {
	dbName         string
	tableName      string
	schemaName     string
	indexNames     map[uint32]string
	parentID       uint32
	schemaParentID uint32
}

// HotRangesV2 returns hot ranges from all stores on requested node or all nodes in case
// request message doesn't include specific node ID.
func (s *statusServer) HotRangesV2(
	ctx context.Context, req *serverpb.HotRangesRequest,
) (*serverpb.HotRangesResponseV2, error) {
	if _, err := s.privilegeChecker.requireAdminUser(ctx); err != nil {
		return nil, err
	}

	size := int(req.PageSize)
	start := paginationState{}

	if len(req.PageToken) > 0 {
		if err := start.UnmarshalText([]byte(req.PageToken)); err != nil {
			return nil, err
		}
	}

	rangeReportMetas := make(map[uint32]hotRangeReportMeta)
	var descrs []catalog.Descriptor
	var err error
	if err := s.sqlServer.distSQLServer.CollectionFactory.Txn(
		ctx, s.sqlServer.internalExecutor, s.db,
		func(ctx context.Context, txn *kv.Txn, descriptors *descs.Collection) error {
			all, err := descriptors.GetAllDescriptors(ctx, txn)
			if err != nil {
				return err
			}
			descrs = all.OrderedDescriptors()
			return nil
		}); err != nil {
		return nil, err
	}

	for _, desc := range descrs {
		id := uint32(desc.GetID())
		meta := hotRangeReportMeta{
			indexNames: map[uint32]string{},
		}
		switch desc := desc.(type) {
		case catalog.TableDescriptor:
			meta.tableName = desc.GetName()
			meta.parentID = uint32(desc.GetParentID())
			meta.schemaParentID = uint32(desc.GetParentSchemaID())
			for _, idx := range desc.AllIndexes() {
				meta.indexNames[uint32(idx.GetID())] = idx.GetName()
			}
		case catalog.SchemaDescriptor:
			meta.schemaName = desc.GetName()
		case catalog.DatabaseDescriptor:
			meta.dbName = desc.GetName()
		}
		rangeReportMetas[id] = meta
	}

	response := &serverpb.HotRangesResponseV2{
		ErrorsByNodeID: make(map[roachpb.NodeID]string),
	}

	var requestedNodes []roachpb.NodeID
	if len(req.NodeID) > 0 {
		requestedNodeID, _, err := s.parseNodeID(req.NodeID)
		if err != nil {
			return nil, err
		}
		requestedNodes = []roachpb.NodeID{requestedNodeID}
	}

	dialFn := func(ctx context.Context, nodeID roachpb.NodeID) (interface{}, error) {
		client, err := s.dialNode(ctx, nodeID)
		return client, err
	}
	remoteRequest := serverpb.HotRangesRequest{NodeID: "local"}
	nodeFn := func(ctx context.Context, client interface{}, nodeID roachpb.NodeID) (interface{}, error) {
		status := client.(serverpb.StatusClient)
		resp, err := status.HotRanges(ctx, &remoteRequest)
		if err != nil || resp == nil {
			return nil, err
		}
		var ranges []*serverpb.HotRangesResponseV2_HotRange
		for nodeID, hr := range resp.HotRangesByNodeID {
			for _, store := range hr.Stores {
				for _, r := range store.HotRanges {
					var (
						dbName, tableName, indexName, schemaName string
						replicaNodeIDs                           []roachpb.NodeID
					)
					_, tableID, err := s.sqlServer.execCfg.Codec.DecodeTablePrefix(r.Desc.StartKey.AsRawKey())
					if err != nil {
						log.Warningf(ctx, "cannot decode tableID for range descriptor: %s. %s", r.Desc.String(), err.Error())
						continue
					}
					parent := rangeReportMetas[tableID].parentID
					if parent != 0 {
						tableName = rangeReportMetas[tableID].tableName
						dbName = rangeReportMetas[parent].dbName
					} else {
						dbName = rangeReportMetas[tableID].dbName
					}
					schemaParent := rangeReportMetas[tableID].schemaParentID
					schemaName = rangeReportMetas[schemaParent].schemaName
					_, _, idxID, err := s.sqlServer.execCfg.Codec.DecodeIndexPrefix(r.Desc.StartKey.AsRawKey())
					if err == nil {
						indexName = rangeReportMetas[tableID].indexNames[idxID]
					}
					for _, repl := range r.Desc.Replicas().Descriptors() {
						replicaNodeIDs = append(replicaNodeIDs, repl.NodeID)
					}
					ranges = append(ranges, &serverpb.HotRangesResponseV2_HotRange{
						RangeID:           r.Desc.RangeID,
						NodeID:            nodeID,
						QPS:               r.QueriesPerSecond,
						TableName:         tableName,
						SchemaName:        schemaName,
						DatabaseName:      dbName,
						IndexName:         indexName,
						ReplicaNodeIds:    replicaNodeIDs,
						LeaseholderNodeID: r.LeaseholderNodeID,
						StoreID:           store.StoreID,
					})
				}
			}
		}
		return ranges, nil
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

func (s *statusServer) localHotRanges(ctx context.Context) serverpb.HotRangesResponse_NodeResponse {
	var resp serverpb.HotRangesResponse_NodeResponse
	err := s.stores.VisitStores(func(store *kvserver.Store) error {
		ranges := store.HottestReplicas()
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
		}
		resp.Stores = append(resp.Stores, storeResp)
		return nil
	})
	if err != nil {
		return serverpb.HotRangesResponse_NodeResponse{ErrorMessage: err.Error()}
	}
	return resp
}

// Range returns rangeInfos for all nodes in the cluster about a specific
// range. It also returns the range history for that range as well.
func (s *statusServer) Range(
	ctx context.Context, req *serverpb.RangeRequest,
) (*serverpb.RangeResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, err := s.privilegeChecker.requireAdminUser(ctx); err != nil {
		// NB: not using serverError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	response := &serverpb.RangeResponse{
		RangeID:           roachpb.RangeID(req.RangeId),
		NodeID:            s.gossip.NodeID.Get(),
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
		sessions[i].NodeID = s.gossip.NodeID.Get()
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
	nodeStatuses, err := s.nodesStatusWithLiveness(ctx)
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
		err := contextutil.RunWithTimeout(ctx, "dial node", base.NetworkTimeout, func(ctx context.Context) error {
			var err error
			client, err = dialFn(ctx, nodeID)
			return err
		})
		if err != nil {
			err = errors.Wrapf(err, "failed to dial into node %d (%s)",
				nodeID, nodeStatuses[nodeID].livenessStatus)
			responseChan <- nodeResponse{nodeID: nodeID, err: err}
			return
		}

		res, err := nodeFn(ctx, client, nodeID)
		if err != nil {
			err = errors.Wrapf(err, "error requesting %s from node %d (%s)",
				errorCtx, nodeID, nodeStatuses[nodeID].livenessStatus)
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
			func(ctx context.Context) { nodeQuery(ctx, nodeID) },
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
	nodeStatuses, err := s.nodesStatusWithLiveness(ctx)
	if err != nil {
		return paginationState{}, err
	}

	numNodes := len(nodeStatuses)
	nodeIDs := make([]roachpb.NodeID, 0, numNodes)
	if len(requestedNodes) > 0 {
		nodeIDs = append(nodeIDs, requestedNodes...)
	} else {
		for nodeID := range nodeStatuses {
			nodeIDs = append(nodeIDs, nodeID)
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
	ctx = propagateGatewayMetadata(ctx)
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
	ctx = propagateGatewayMetadata(ctx)
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
		return status.CancelSession(ctx, req)
	}

	reqUsername, err := security.MakeSQLUsernameFromPreNormalizedStringChecked(req.Username)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if err := s.checkCancelPrivilege(ctx, reqUsername, findSessionBySessionID(req.SessionID)); err != nil {
		// NB: not using serverError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	r, err := s.sessionRegistry.CancelSession(req.SessionID)
	if err != nil {
		return nil, serverError(ctx, err)
	}
	return r, nil
}

// CancelQuery responds to a query cancellation request, and cancels
// the target query's associated context and sets a cancellation flag.
func (s *statusServer) CancelQuery(
	ctx context.Context, req *serverpb.CancelQueryRequest,
) (*serverpb.CancelQueryResponse, error) {
	nodeID, local, err := s.parseNodeID(req.NodeId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	if !local {
		// This request needs to be forwarded to another node.
		ctx = propagateGatewayMetadata(ctx)
		ctx = s.AnnotateCtx(ctx)
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, serverError(ctx, err)
		}
		return status.CancelQuery(ctx, req)
	}

	reqUsername, err := security.MakeSQLUsernameFromPreNormalizedStringChecked(req.Username)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if err := s.checkCancelPrivilege(ctx, reqUsername, findSessionByQueryID(req.QueryID)); err != nil {
		// NB: not using serverError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	output := &serverpb.CancelQueryResponse{}
	output.Canceled, err = s.sessionRegistry.CancelQuery(req.QueryID)
	if err != nil {
		output.Error = err.Error()
	}
	return output, nil
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
	alloc, err := pgwirecancel.CancelSemaphore.TryAcquire(ctx, 1)
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
		resp = &serverpb.CancelQueryByKeyResponse{}
		resp.Canceled, err = s.sessionRegistry.CancelQueryByKey(req.CancelQueryKey)
		if err != nil {
			resp.Error = err.Error()
		}
		return resp, nil
	}

	// This request needs to be forwarded to another node.
	ctx = propagateGatewayMetadata(ctx)
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
	ctx = propagateGatewayMetadata(ctx)
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
	ctx = propagateGatewayMetadata(ctx)
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

// SpanStats requests the total statistics stored on a node for a given key
// span, which may include multiple ranges.
func (s *statusServer) SpanStats(
	ctx context.Context, req *serverpb.SpanStatsRequest,
) (*serverpb.SpanStatsResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, err := s.privilegeChecker.requireAdminUser(ctx); err != nil {
		// NB: not using serverError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	nodeID, local, err := s.parseNodeID(req.NodeID)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	if !local {
		status, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return nil, serverError(ctx, err)
		}
		return status.SpanStats(ctx, req)
	}

	output := &serverpb.SpanStatsResponse{}
	err = s.stores.VisitStores(func(store *kvserver.Store) error {
		result, err := store.ComputeStatsForKeySpan(req.StartKey.Next(), req.EndKey)
		if err != nil {
			return err
		}
		output.TotalStats.Add(result.MVCC)
		output.RangeCount += int32(result.ReplicaCount)
		output.ApproximateDiskBytes += result.ApproximateDiskBytes
		return nil
	})
	if err != nil {
		return nil, serverError(ctx, err)
	}

	return output, nil
}

// Diagnostics returns an anonymized diagnostics report.
func (s *statusServer) Diagnostics(
	ctx context.Context, req *serverpb.DiagnosticsRequest,
) (*diagnosticspb.DiagnosticReport, error) {
	ctx = propagateGatewayMetadata(ctx)
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

	return s.admin.server.sqlServer.diagnosticsReporter.CreateReport(ctx, telemetry.ReadOnly), nil
}

// Stores returns details for each store.
func (s *statusServer) Stores(
	ctx context.Context, req *serverpb.StoresRequest,
) (*serverpb.StoresResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
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
		return status.Stores(ctx, req)
	}

	resp := &serverpb.StoresResponse{}
	err = s.stores.VisitStores(func(store *kvserver.Store) error {
		storeDetails := serverpb.StoreDetails{
			StoreID: store.Ident.StoreID,
		}

		envStats, err := store.Engine().GetEnvStats()
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

func userFromContext(ctx context.Context) (res security.SQLUsername, err error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		// If the incoming context has metadata but no attached web session user,
		// it's a gRPC / internal SQL connection which has root on the cluster.
		return security.RootUserName(), nil
	}
	usernames, ok := md[webSessionUserKeyStr]
	if !ok {
		// If the incoming context has metadata but no attached web session user,
		// it's a gRPC / internal SQL connection which has root on the cluster.
		return security.RootUserName(), nil
	}
	if len(usernames) != 1 {
		log.Warningf(ctx, "context's incoming metadata contains unexpected number of usernames: %+v ", md)
		return res, fmt.Errorf(
			"context's incoming metadata contains unexpected number of usernames: %+v ", md)
	}
	// At this point the user is already logged in, so we can assume
	// the username has been normalized already.
	username := security.MakeSQLUsernameFromPreNormalizedString(usernames[0])
	return username, nil
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
	ctx = propagateGatewayMetadata(ctx)
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

	remoteNodeID := s.gossip.NodeID.Get()
	resp := &serverpb.JobRegistryStatusResponse{
		NodeID: remoteNodeID,
	}
	for _, jID := range s.admin.server.sqlServer.jobRegistry.CurrentlyRunningJobs() {
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
	ctx = s.AnnotateCtx(propagateGatewayMetadata(ctx))

	if _, err := s.privilegeChecker.requireAdminUser(ctx); err != nil {
		// NB: not using serverError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	j, err := s.admin.server.sqlServer.jobRegistry.LoadJob(ctx, jobspb.JobID(req.JobId))
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
	ctx = s.AnnotateCtx(propagateGatewayMetadata(ctx))
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
	ctx = s.AnnotateCtx(propagateGatewayMetadata(ctx))

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

	if s.gossip.NodeID.Get() == 0 {
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
