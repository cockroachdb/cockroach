// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// TODO(azhng): The implementation for tenantStatusServer here will need to be updated
//  once we have pod-to-pod communication implemented. After all dependencies that are
//  unavailable to tenants have been removed, we can likely remove tenant status server
//  entirely and use the normal status server.

package server

import (
	"context"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/contention"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirecancel"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	gwruntime "github.com/grpc-ecosystem/grpc-gateway/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// tenantStatusServer is an implementation of a SQLStatusServer that is
// available to tenants. The full statusServer implementation is unavailable to
// tenants due to its use of gossip and other unavailable subsystems.
// The tenantStatusServer implementation is local only. This is enough for
// Phase 2 requirements that there can only be at most one live SQL pod per
// tenant.
type tenantStatusServer struct {
	baseStatusServer // embeds UnimplementedStatusServer
}

// We require that `tenantStatusServer` implement
// `serverpb.StatusServer` even though we only have partial
// implementation, in order to serve some endpoints on tenants.
var _ serverpb.StatusServer = &tenantStatusServer{}

func (t *tenantStatusServer) RegisterService(g *grpc.Server) {
	serverpb.RegisterStatusServer(g, t)
}

func (t *tenantStatusServer) RegisterGateway(
	ctx context.Context, mux *gwruntime.ServeMux, conn *grpc.ClientConn,
) error {
	ctx = t.AnnotateCtx(ctx)
	return serverpb.RegisterStatusHandler(ctx, mux, conn)
}

var _ grpcGatewayServer = &tenantStatusServer{}

func newTenantStatusServer(
	ambient log.AmbientContext,
	privilegeChecker *adminPrivilegeChecker,
	sessionRegistry *sql.SessionRegistry,
	flowScheduler *flowinfra.FlowScheduler,
	st *cluster.Settings,
	sqlServer *SQLServer,
	rpcCtx *rpc.Context,
	stopper *stop.Stopper,
) *tenantStatusServer {
	ambient.AddLogTag("tenant-status", nil)
	return &tenantStatusServer{
		baseStatusServer: baseStatusServer{
			AmbientContext:   ambient,
			privilegeChecker: privilegeChecker,
			sessionRegistry:  sessionRegistry,
			flowScheduler:    flowScheduler,
			st:               st,
			sqlServer:        sqlServer,
			rpcCtx:           rpcCtx,
			stopper:          stopper,
		},
	}
}

// dialCallback used to dial specific pods when
// iterating nodes.
func (t *tenantStatusServer) dialCallback(
	ctx context.Context, instanceID base.SQLInstanceID, addr string,
) (interface{}, error) {
	client, err := t.dialPod(ctx, instanceID, addr)
	return client, err
}

func (t *tenantStatusServer) ListSessions(
	ctx context.Context, req *serverpb.ListSessionsRequest,
) (*serverpb.ListSessionsResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = t.AnnotateCtx(ctx)

	if err := t.privilegeChecker.requireViewActivityOrViewActivityRedactedPermission(ctx); err != nil {
		// NB: not using serverError() here since the priv checker already
		// returns a proper gRPC error status.
		return nil, err
	}
	if t.sqlServer.SQLInstanceID() == 0 {
		return nil, status.Errorf(codes.Unavailable, "instanceID not set")
	}

	response := &serverpb.ListSessionsResponse{
		InternalAppNamePrefix: catconstants.InternalAppNamePrefix,
	}
	nodeStatement := func(ctx context.Context, client interface{}, instanceID base.SQLInstanceID) (interface{}, error) {
		statusClient := client.(serverpb.StatusClient)
		localResponse, err := statusClient.ListLocalSessions(ctx, req)
		if localResponse == nil {
			log.Errorf(ctx, "listing local sessions on %d produced a nil result with error %v",
				instanceID,
				err)
		}
		if err != nil {
			return nil, serverError(ctx, err)
		}
		return localResponse, nil
	}
	if err := t.iteratePods(ctx, "sessions for nodes",
		t.dialCallback,
		nodeStatement,
		func(instanceID base.SQLInstanceID, resp interface{}) {
			sessionResp := resp.(*serverpb.ListSessionsResponse)
			response.Sessions = append(response.Sessions, sessionResp.Sessions...)
			response.Errors = append(response.Errors, sessionResp.Errors...)
		},
		func(instanceID base.SQLInstanceID, err error) {
			// Log any errors related to the failures.
			log.Warningf(ctx, "fan out statements request recorded error from node %d: %v", instanceID, err)
			response.Errors = append(response.Errors,
				serverpb.ListSessionsError{
					Message: err.Error(),
					NodeID:  roachpb.NodeID(instanceID),
				})
		},
	); err != nil {
		return nil, serverError(ctx, err)
	}
	return response, nil
}

func (t *tenantStatusServer) ListLocalSessions(
	ctx context.Context, request *serverpb.ListSessionsRequest,
) (*serverpb.ListSessionsResponse, error) {
	sessions, err := t.getLocalSessions(ctx, request)
	if err != nil {
		// NB: not using serverError() here since the getLocalSessions
		// already returns a proper gRPC error status.
		return nil, err
	}

	if t.sqlServer.SQLInstanceID() == 0 {
		return nil, status.Errorf(codes.Unavailable, "instanceID not set")
	}

	return &serverpb.ListSessionsResponse{
		Sessions:              sessions,
		InternalAppNamePrefix: catconstants.InternalAppNamePrefix,
	}, nil
}

func (t *tenantStatusServer) CancelQuery(
	ctx context.Context, request *serverpb.CancelQueryRequest,
) (*serverpb.CancelQueryResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = t.AnnotateCtx(ctx)

	// Check permissions early to avoid fan-out to all nodes.
	reqUsername := security.MakeSQLUsernameFromPreNormalizedString(request.Username)
	if err := t.checkCancelPrivilege(ctx, reqUsername, findSessionByQueryID(request.QueryID)); err != nil {
		// NB: not using serverError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}
	if t.sqlServer.SQLInstanceID() == 0 {
		return nil, status.Errorf(codes.Unavailable, "instanceID not set")
	}

	response := serverpb.CancelQueryResponse{}
	distinctErrorMessages := map[string]struct{}{}

	if err := t.iteratePods(
		ctx,
		fmt.Sprintf("cancel query ID %s", request.QueryID),
		t.dialCallback,
		func(ctx context.Context, client interface{}, _ base.SQLInstanceID) (interface{}, error) {
			return client.(serverpb.StatusClient).CancelLocalQuery(ctx, request)
		},
		func(_ base.SQLInstanceID, nodeResp interface{}) {
			nodeCancelQueryResponse := nodeResp.(*serverpb.CancelQueryResponse)
			if nodeCancelQueryResponse.Canceled {
				response.Canceled = true
			}
			distinctErrorMessages[nodeCancelQueryResponse.Error] = struct{}{}
		},
		func(_ base.SQLInstanceID, err error) {
			distinctErrorMessages[err.Error()] = struct{}{}
		},
	); err != nil {
		return nil, serverError(ctx, err)
	}

	if !response.Canceled {
		var errorMessages []string
		for errorMessage := range distinctErrorMessages {
			errorMessages = append(errorMessages, errorMessage)
		}
		response.Error = strings.Join(errorMessages, ", ")
	}

	return &response, nil
}

func (t *tenantStatusServer) CancelLocalQuery(
	ctx context.Context, request *serverpb.CancelQueryRequest,
) (*serverpb.CancelQueryResponse, error) {
	reqUsername := security.MakeSQLUsernameFromPreNormalizedString(request.Username)
	if err := t.checkCancelPrivilege(ctx, reqUsername, findSessionByQueryID(request.QueryID)); err != nil {
		// NB: not using serverError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}
	var (
		output = &serverpb.CancelQueryResponse{}
		err    error
	)
	output.Canceled, err = t.sessionRegistry.CancelQuery(request.QueryID)
	if err != nil {
		output.Error = err.Error()
	}
	return output, nil
}

// CancelQueryByKey responds to a pgwire query cancellation request, and cancels
// the target query's associated context and sets a cancellation flag. This
// endpoint is rate-limited by a semaphore.
func (t *tenantStatusServer) CancelQueryByKey(
	ctx context.Context, req *serverpb.CancelQueryByKeyRequest,
) (resp *serverpb.CancelQueryByKeyResponse, retErr error) {
	// We are interpreting the `NodeID` in the request as an `InstanceID` since
	// we are executing in the context of a tenant.
	local := req.SQLInstanceID == t.sqlServer.SQLInstanceID()
	resp = &serverpb.CancelQueryByKeyResponse{}

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
		resp.Canceled, err = t.sessionRegistry.CancelQueryByKey(req.CancelQueryKey)
		if err != nil {
			resp.Error = err.Error()
		}
		return resp, nil
	}

	// This request needs to be forwarded to another node.
	ctx = propagateGatewayMetadata(ctx)
	ctx = t.AnnotateCtx(ctx)
	instance, err := t.sqlServer.sqlInstanceProvider.GetInstance(ctx, req.SQLInstanceID)
	if err != nil {
		return nil, err
	}
	statusClient, err := t.dialPod(ctx, req.SQLInstanceID, instance.InstanceAddr)
	if err != nil {
		return nil, err
	}
	return statusClient.CancelQueryByKey(ctx, req)
}

func (t *tenantStatusServer) CancelSession(
	ctx context.Context, request *serverpb.CancelSessionRequest,
) (*serverpb.CancelSessionResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = t.AnnotateCtx(ctx)

	// Check permissions early to avoid fan-out to all nodes.
	reqUsername := security.MakeSQLUsernameFromPreNormalizedString(request.Username)
	if err := t.checkCancelPrivilege(ctx, reqUsername, findSessionBySessionID(request.SessionID)); err != nil {
		// NB: not using serverError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	if t.sqlServer.SQLInstanceID() == 0 {
		return nil, status.Errorf(codes.Unavailable, "instanceID not set")
	}

	response := serverpb.CancelSessionResponse{}
	distinctErrorMessages := map[string]struct{}{}

	if err := t.iteratePods(
		ctx,
		fmt.Sprintf("cancel session ID %s", hex.EncodeToString(request.SessionID)),
		t.dialCallback,
		func(ctx context.Context, client interface{}, _ base.SQLInstanceID) (interface{}, error) {
			return client.(serverpb.StatusClient).CancelLocalSession(ctx, request)
		},
		func(_ base.SQLInstanceID, nodeResp interface{}) {
			nodeCancelSessionResp := nodeResp.(*serverpb.CancelSessionResponse)
			if nodeCancelSessionResp.Canceled {
				response.Canceled = true
			}
			distinctErrorMessages[nodeCancelSessionResp.Error] = struct{}{}
		},
		func(_ base.SQLInstanceID, err error) {
			distinctErrorMessages[err.Error()] = struct{}{}
		},
	); err != nil {
		return nil, serverError(ctx, err)
	}

	if !response.Canceled {
		var errorMessages []string
		for errorMessage := range distinctErrorMessages {
			errorMessages = append(errorMessages, errorMessage)
		}
		response.Error = strings.Join(errorMessages, ", ")
	}

	return &response, nil
}

func (t *tenantStatusServer) CancelLocalSession(
	ctx context.Context, request *serverpb.CancelSessionRequest,
) (*serverpb.CancelSessionResponse, error) {
	reqUsername := security.MakeSQLUsernameFromPreNormalizedString(request.Username)
	if err := t.checkCancelPrivilege(ctx, reqUsername, findSessionBySessionID(request.SessionID)); err != nil {
		// NB: not using serverError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}
	return t.sessionRegistry.CancelSession(request.SessionID)
}

func (t *tenantStatusServer) ListContentionEvents(
	ctx context.Context, req *serverpb.ListContentionEventsRequest,
) (*serverpb.ListContentionEventsResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = t.AnnotateCtx(ctx)

	// Check permissions early to avoid fan-out to all nodes.
	if err := t.privilegeChecker.requireViewActivityOrViewActivityRedactedPermission(ctx); err != nil {
		// NB: not using serverError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	if t.sqlServer.SQLInstanceID() == 0 {
		return nil, status.Errorf(codes.Unavailable, "instanceID not set")
	}

	var response serverpb.ListContentionEventsResponse

	podFn := func(ctx context.Context, client interface{}, _ base.SQLInstanceID) (interface{}, error) {
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
	responseFn := func(_ base.SQLInstanceID, nodeResp interface{}) {
		if nodeResp == nil {
			return
		}
		events := nodeResp.(*serverpb.ListContentionEventsResponse).Events
		response.Events = contention.MergeSerializedRegistries(response.Events, events)
	}
	errorFn := func(instanceID base.SQLInstanceID, err error) {
		errResponse := serverpb.ListActivityError{
			NodeID:  roachpb.NodeID(instanceID),
			Message: err.Error(),
		}
		response.Errors = append(response.Errors, errResponse)
	}

	if err := t.iteratePods(
		ctx,
		"contention events list",
		t.dialCallback,
		podFn,
		responseFn,
		errorFn,
	); err != nil {
		return nil, err
	}
	return &response, nil
}

func (t *tenantStatusServer) ListLocalContentionEvents(
	ctx context.Context, req *serverpb.ListContentionEventsRequest,
) (*serverpb.ListContentionEventsResponse, error) {
	if t.sqlServer.SQLInstanceID() == 0 {
		return nil, status.Errorf(codes.Unavailable, "instanceID not set")
	}
	return t.baseStatusServer.ListLocalContentionEvents(ctx, req)
}

func (t *tenantStatusServer) ResetSQLStats(
	ctx context.Context, req *serverpb.ResetSQLStatsRequest,
) (*serverpb.ResetSQLStatsResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = t.AnnotateCtx(ctx)

	if _, err := t.privilegeChecker.requireAdminUser(ctx); err != nil {
		return nil, err
	}

	if t.sqlServer.SQLInstanceID() == 0 {
		return nil, status.Errorf(codes.Unavailable, "instanceID not set")
	}

	response := &serverpb.ResetSQLStatsResponse{}
	controller := t.sqlServer.pgServer.SQLServer.GetSQLStatsController()

	// If we need to reset persisted stats, we delegate to SQLStatsController,
	// which will trigger a system table truncation and RPC fanout under the hood.
	if req.ResetPersistedStats {
		if err := controller.ResetClusterSQLStats(ctx); err != nil {
			return nil, err
		}

		return response, nil
	}

	localReq := &serverpb.ResetSQLStatsRequest{
		NodeID: "local",
		// Only the top level RPC handler handles the reset persisted stats.
		ResetPersistedStats: false,
	}

	if len(req.NodeID) > 0 {
		parsedInstanceID, local, err := t.parseInstanceID(req.NodeID)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, err.Error())
		}
		if local {
			controller.ResetLocalSQLStats(ctx)
			return response, nil
		}

		instance, err := t.sqlServer.sqlInstanceProvider.GetInstance(ctx, parsedInstanceID)
		if err != nil {
			return nil, err
		}
		statusClient, err := t.dialPod(ctx, parsedInstanceID, instance.InstanceAddr)
		if err != nil {
			return nil, err
		}
		return statusClient.ResetSQLStats(ctx, localReq)
	}

	nodeResetFn := func(
		ctx context.Context,
		client interface{},
		instanceID base.SQLInstanceID,
	) (interface{}, error) {
		statusClient := client.(serverpb.StatusClient)
		return statusClient.ResetSQLStats(ctx, localReq)
	}

	var fanoutError error

	if err := t.iteratePods(ctx, "reset SQL statistics",
		t.dialCallback,
		nodeResetFn,
		func(instanceID base.SQLInstanceID, resp interface{}) {
			// Nothing to do here.
		},
		func(instanceID base.SQLInstanceID, err error) {
			if err != nil {
				fanoutError = errors.CombineErrors(fanoutError, err)
			}
		},
	); err != nil {
		return nil, err
	}

	return response, fanoutError
}

func (t *tenantStatusServer) CombinedStatementStats(
	ctx context.Context, req *serverpb.CombinedStatementsStatsRequest,
) (*serverpb.StatementsResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = t.AnnotateCtx(ctx)

	if err := t.privilegeChecker.requireViewActivityOrViewActivityRedactedPermission(ctx); err != nil {
		return nil, err
	}

	if t.sqlServer.SQLInstanceID() == 0 {
		return nil, status.Errorf(codes.Unavailable, "instanceID not set")
	}

	return getCombinedStatementStats(ctx, req, t.sqlServer.pgServer.SQLServer.GetSQLStatsProvider(),
		t.sqlServer.internalExecutor, t.st, t.sqlServer.execCfg.SQLStatsTestingKnobs)
}

func (t *tenantStatusServer) StatementDetails(
	ctx context.Context, req *serverpb.StatementDetailsRequest,
) (*serverpb.StatementDetailsResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = t.AnnotateCtx(ctx)

	if err := t.privilegeChecker.requireViewActivityOrViewActivityRedactedPermission(ctx); err != nil {
		return nil, err
	}

	if t.sqlServer.SQLInstanceID() == 0 {
		return nil, status.Errorf(codes.Unavailable, "instanceID not set")
	}

	return getStatementDetails(ctx, req, t.sqlServer.internalExecutor, t.st, t.sqlServer.execCfg.SQLStatsTestingKnobs)
}

// Statements implements the relevant endpoint on the StatusServer by
// fanning out a request to all pods on the current tenant via gRPC to collect
// in-memory statistics and append them together for the caller.
//
// The implementation is based on the one in statements.go but differs
// by leaning on the InstanceID subsystem to implement the fan-out. If
// the InstanceID and NodeID subsystems can be unified in some way,
// these implementations could be merged.
func (t *tenantStatusServer) Statements(
	ctx context.Context, req *serverpb.StatementsRequest,
) (*serverpb.StatementsResponse, error) {
	if req.Combined {
		combinedRequest := serverpb.CombinedStatementsStatsRequest{
			Start: req.Start,
			End:   req.End,
		}
		return t.CombinedStatementStats(ctx, &combinedRequest)
	}

	ctx = propagateGatewayMetadata(ctx)
	ctx = t.AnnotateCtx(ctx)

	if err := t.privilegeChecker.requireViewActivityOrViewActivityRedactedPermission(ctx); err != nil {
		return nil, err
	}

	if t.sqlServer.SQLInstanceID() == 0 {
		return nil, status.Errorf(codes.Unavailable, "instanceID not set")
	}

	response := &serverpb.StatementsResponse{
		Statements:            []serverpb.StatementsResponse_CollectedStatementStatistics{},
		LastReset:             timeutil.Now(),
		InternalAppNamePrefix: catconstants.InternalAppNamePrefix,
	}

	localReq := &serverpb.StatementsRequest{
		NodeID: "local",
	}

	if len(req.NodeID) > 0 {
		// We are interpreting the `NodeID` in the request as an `InstanceID` since
		// we are executing in the context of a tenant.
		parsedInstanceID, local, err := t.parseInstanceID(req.NodeID)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, err.Error())
		}
		if local {
			return statementsLocal(
				ctx,
				roachpb.NodeID(t.sqlServer.SQLInstanceID()),
				t.sqlServer,
				req.FetchMode)
		}
		instance, err := t.sqlServer.sqlInstanceProvider.GetInstance(ctx, parsedInstanceID)
		if err != nil {
			return nil, err
		}
		statusClient, err := t.dialPod(ctx, parsedInstanceID, instance.InstanceAddr)
		if err != nil {
			return nil, err
		}
		return statusClient.Statements(ctx, localReq)
	}

	nodeStatement := func(ctx context.Context, client interface{}, instanceID base.SQLInstanceID) (interface{}, error) {
		statusClient := client.(serverpb.StatusClient)
		localResponse, err := statusClient.Statements(ctx, localReq)
		if localResponse == nil {
			log.Errorf(ctx, "listing statements on %d produced a nil result with err: %v",
				instanceID,
				err)
		}
		return localResponse, err
	}

	if err := t.iteratePods(ctx, "statement statistics",
		t.dialCallback,
		nodeStatement,
		func(instanceID base.SQLInstanceID, resp interface{}) {
			statementsResp := resp.(*serverpb.StatementsResponse)
			response.Statements = append(response.Statements, statementsResp.Statements...)
			response.Transactions = append(response.Transactions, statementsResp.Transactions...)
			if response.LastReset.After(statementsResp.LastReset) {
				response.LastReset = statementsResp.LastReset
			}
		},
		func(instanceID base.SQLInstanceID, err error) {
			// We log warnings when fanout returns error, but proceed with
			// constructing a response from whoever returns a good one.
			log.Warningf(ctx, "fan out statements request recorded error from node %d: %v", instanceID, err)
		},
	); err != nil {
		return nil, err
	}

	return response, nil
}

// parseInstanceID is based on status.parseNodeID
func (t *tenantStatusServer) parseInstanceID(
	instanceIDParam string,
) (instanceID base.SQLInstanceID, isLocal bool, err error) {
	// No parameter provided or set to local.
	if len(instanceIDParam) == 0 || localRE.MatchString(instanceIDParam) {
		return t.sqlServer.SQLInstanceID(), true /* isLocal */, nil /* err */
	}

	id, err := strconv.ParseInt(instanceIDParam, 0, 32)
	if err != nil {
		return 0 /* instanceID */, false /* isLocal */, errors.Wrap(err, "instance ID could not be parsed")
	}
	instanceID = base.SQLInstanceID(id)
	return instanceID, instanceID == t.sqlServer.SQLInstanceID() /* isLocal */, nil
}

func (t *tenantStatusServer) dialPod(
	ctx context.Context, instanceID base.SQLInstanceID, addr string,
) (serverpb.StatusClient, error) {
	conn, err := t.rpcCtx.GRPCDialPod(addr, instanceID, rpc.DefaultClass).Connect(ctx)
	if err != nil {
		return nil, err
	}

	// nb: The server on the pods doesn't implement all the methods of the
	// `StatusService`. It is up to the caller of `dialPod` to only call
	// methods that are implemented on the tenant server.
	return serverpb.NewStatusClient(conn), nil
}

// iteratePods is based on the implementation of `iterateNodes in the
// status server. The two implementations have not been unified into one
// because there are some deep differences since we use `InstanceInfo`
// instead of `NodeID`. Since the eventual plan is to deprecate
// `tenant_status.go` altogether, we're leaving this code-as is.
//
// TODO(davidh): unify with `status.iterateNodes` once this server is
// deprecated
func (t *tenantStatusServer) iteratePods(
	ctx context.Context,
	errorCtx string,
	dialFn func(ctx context.Context, instanceID base.SQLInstanceID, addr string) (interface{}, error),
	instanceFn func(ctx context.Context, client interface{}, instanceID base.SQLInstanceID) (interface{}, error),
	responseFn func(instanceID base.SQLInstanceID, resp interface{}),
	errorFn func(instanceID base.SQLInstanceID, nodeFnError error),
) error {
	liveTenantInstances, err := t.sqlServer.sqlInstanceProvider.GetAllInstances(ctx)
	if err != nil {
		return err
	}

	type instanceResponse struct {
		instanceID base.SQLInstanceID
		response   interface{}
		err        error
	}

	numInstances := len(liveTenantInstances)
	responseChan := make(chan instanceResponse, numInstances)

	instanceQuery := func(ctx context.Context, instance sqlinstance.InstanceInfo) {
		var client interface{}
		err := contextutil.RunWithTimeout(ctx, "dial instance", base.NetworkTimeout, func(ctx context.Context) error {
			var err error
			client, err = dialFn(ctx, instance.InstanceID, instance.InstanceAddr)
			return err
		})

		instanceID := instance.InstanceID
		if err != nil {
			err = errors.Wrapf(err, "failed to dial into node %d",
				instanceID)
			responseChan <- instanceResponse{instanceID: instanceID, err: err}
			return
		}

		res, err := instanceFn(ctx, client, instanceID)
		if err != nil {
			err = errors.Wrapf(err, "error requesting %s from instance %d",
				errorCtx, instanceID)
			responseChan <- instanceResponse{instanceID: instanceID, err: err}
			return
		}
		responseChan <- instanceResponse{instanceID: instanceID, response: res}
	}

	sem := quotapool.NewIntPool("instance status", maxConcurrentRequests)
	ctx, cancel := t.stopper.WithCancelOnQuiesce(ctx)
	defer cancel()

	for _, instance := range liveTenantInstances {
		instance := instance
		if err := t.stopper.RunAsyncTaskEx(
			ctx, stop.TaskOpts{
				TaskName:   fmt.Sprintf("server.tenantStatusServer: requesting %s", errorCtx),
				Sem:        sem,
				WaitForSem: true,
			},
			func(ctx context.Context) {
				instanceQuery(ctx, instance)
			}); err != nil {
			return err
		}
	}

	var resultErr error
	for numInstances > 0 {
		select {
		case res := <-responseChan:
			if res.err != nil {
				errorFn(res.instanceID, res.err)
			} else {
				responseFn(res.instanceID, res.response)
			}
		case <-ctx.Done():
			resultErr = errors.Errorf("request of %s canceled before completion", errorCtx)
		}
		numInstances--
	}
	return resultErr
}

func (t *tenantStatusServer) ListDistSQLFlows(
	ctx context.Context, request *serverpb.ListDistSQLFlowsRequest,
) (*serverpb.ListDistSQLFlowsResponse, error) {
	if t.sqlServer.SQLInstanceID() == 0 {
		return nil, status.Errorf(codes.Unavailable, "instanceID not set")
	}

	return t.ListLocalDistSQLFlows(ctx, request)
}

func (t *tenantStatusServer) ListLocalDistSQLFlows(
	ctx context.Context, request *serverpb.ListDistSQLFlowsRequest,
) (*serverpb.ListDistSQLFlowsResponse, error) {
	if t.sqlServer.SQLInstanceID() == 0 {
		return nil, status.Errorf(codes.Unavailable, "instanceID not set")
	}

	return t.baseStatusServer.ListLocalDistSQLFlows(ctx, request)
}

// Profile implements the profiling endpoint by delegating the request
// to the local handler. If the requested node_id is not the same as
// the current instance ID, it performs an RPC call to fetch the profile
// data for the requested SQL instance.
func (t *tenantStatusServer) Profile(
	ctx context.Context, request *serverpb.ProfileRequest,
) (*serverpb.JSONResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = t.AnnotateCtx(ctx)

	if _, err := t.privilegeChecker.requireAdminUser(ctx); err != nil {
		return nil, err
	}
	if t.sqlServer.SQLInstanceID() == 0 {
		return nil, status.Errorf(codes.Unavailable, "instanceID not set")
	}

	instanceID, local, err := t.parseInstanceID(request.NodeId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	if !local {
		instance, err := t.sqlServer.sqlInstanceProvider.GetInstance(ctx, instanceID)
		if err != nil {
			return nil, err
		}
		status, err := t.dialPod(ctx, instanceID, instance.InstanceAddr)
		if err != nil {
			return nil, err
		}
		return status.Profile(ctx, request)
	}
	return profileLocal(ctx, request, t.st)
}

func (t *tenantStatusServer) Stacks(
	ctx context.Context, request *serverpb.StacksRequest,
) (*serverpb.JSONResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = t.AnnotateCtx(ctx)

	if _, err := t.privilegeChecker.requireAdminUser(ctx); err != nil {
		// NB: not using serverError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}
	if t.sqlServer.SQLInstanceID() == 0 {
		return nil, status.Errorf(codes.Unavailable, "instanceID not set")
	}

	instanceID, local, err := t.parseInstanceID(request.NodeId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	if !local {
		instance, err := t.sqlServer.sqlInstanceProvider.GetInstance(ctx, instanceID)
		if err != nil {
			return nil, err
		}
		status, err := t.dialPod(ctx, instanceID, instance.InstanceAddr)
		if err != nil {
			return nil, err
		}
		return status.Stacks(ctx, request)
	}
	return stacksLocal(request)
}

func (t *tenantStatusServer) IndexUsageStatistics(
	ctx context.Context, req *serverpb.IndexUsageStatisticsRequest,
) (*serverpb.IndexUsageStatisticsResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = t.AnnotateCtx(ctx)

	if err := t.privilegeChecker.requireViewActivityOrViewActivityRedactedPermission(ctx); err != nil {
		return nil, err
	}

	if t.sqlServer.SQLInstanceID() == 0 {
		return nil, status.Errorf(codes.Unavailable, "instanceID not set")
	}

	localReq := &serverpb.IndexUsageStatisticsRequest{
		NodeID: "local",
	}

	if len(req.NodeID) > 0 {
		parsedInstanceID, local, err := t.parseInstanceID(req.NodeID)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if local {
			statsReader := t.sqlServer.pgServer.SQLServer.GetLocalIndexStatistics()
			return indexUsageStatsLocal(statsReader)
		}

		instance, err := t.sqlServer.sqlInstanceProvider.GetInstance(ctx, parsedInstanceID)
		if err != nil {
			return nil, err
		}
		statusClient, err := t.dialPod(ctx, parsedInstanceID, instance.InstanceAddr)
		if err != nil {
			return nil, err
		}

		// We issue a localReq instead of the incoming req to other nodes. This is
		// to instruct other nodes to only return us their node-local stats and
		// do not further propagates the RPC call.
		return statusClient.IndexUsageStatistics(ctx, localReq)
	}

	fetchIndexUsageStats := func(ctx context.Context, client interface{}, _ base.SQLInstanceID) (interface{}, error) {
		statusClient := client.(serverpb.StatusClient)
		return statusClient.IndexUsageStatistics(ctx, localReq)
	}

	resp := &serverpb.IndexUsageStatisticsResponse{}
	aggFn := func(_ base.SQLInstanceID, nodeResp interface{}) {
		stats := nodeResp.(*serverpb.IndexUsageStatisticsResponse)
		resp.Statistics = append(resp.Statistics, stats.Statistics...)
	}

	var combinedError error
	errFn := func(_ base.SQLInstanceID, nodeFnError error) {
		combinedError = errors.CombineErrors(combinedError, nodeFnError)
	}

	if err := t.iteratePods(ctx, "requesting index usage stats",
		t.dialCallback,
		fetchIndexUsageStats,
		aggFn,
		errFn,
	); err != nil {
		return nil, err
	}

	// Append last reset time.
	resp.LastReset = t.sqlServer.pgServer.SQLServer.GetLocalIndexStatistics().GetLastReset()

	return resp, nil
}

// ResetIndexUsageStats is the gRPC handler for resetting index usage stats.
// This endpoint resets index usage statistics for all tables.
func (t *tenantStatusServer) ResetIndexUsageStats(
	ctx context.Context, req *serverpb.ResetIndexUsageStatsRequest,
) (*serverpb.ResetIndexUsageStatsResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = t.AnnotateCtx(ctx)

	if _, err := t.privilegeChecker.requireAdminUser(ctx); err != nil {
		return nil, err
	}

	if t.sqlServer.SQLInstanceID() == 0 {
		return nil, status.Errorf(codes.Unavailable, "instanceID not set")
	}

	localReq := &serverpb.ResetIndexUsageStatsRequest{
		NodeID: "local",
	}
	resp := &serverpb.ResetIndexUsageStatsResponse{}

	if len(req.NodeID) > 0 {
		parsedInstanceID, local, err := t.parseInstanceID(req.NodeID)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if local {
			t.sqlServer.pgServer.SQLServer.GetLocalIndexStatistics().Reset()
			return resp, nil
		}

		instance, err := t.sqlServer.sqlInstanceProvider.GetInstance(ctx, parsedInstanceID)
		if err != nil {
			return nil, err
		}
		statusClient, err := t.dialPod(ctx, parsedInstanceID, instance.InstanceAddr)
		if err != nil {
			return nil, err
		}

		return statusClient.ResetIndexUsageStats(ctx, localReq)
	}

	resetIndexUsageStats := func(ctx context.Context, client interface{}, _ base.SQLInstanceID) (interface{}, error) {
		statusClient := client.(serverpb.StatusClient)
		return statusClient.ResetIndexUsageStats(ctx, localReq)
	}

	var combinedError error

	if err := t.iteratePods(ctx, "Resetting index usage stats for instance",
		t.dialCallback,
		resetIndexUsageStats,
		func(instanceID base.SQLInstanceID, resp interface{}) {
			// Nothing to do here.
		},
		func(_ base.SQLInstanceID, err error) {
			combinedError = errors.CombineErrors(combinedError, err)
		},
	); err != nil {
		return nil, err
	}

	return resp, nil
}

// TableIndexStats is the gRPC handler for retrieving index usage statistics
// by table. This function reads index usage statistics directly from the
// database and is meant for external usage.
func (t *tenantStatusServer) TableIndexStats(
	ctx context.Context, req *serverpb.TableIndexStatsRequest,
) (*serverpb.TableIndexStatsResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = t.AnnotateCtx(ctx)

	if err := t.privilegeChecker.requireViewActivityOrViewActivityRedactedPermission(ctx); err != nil {
		return nil, err
	}

	if t.sqlServer.SQLInstanceID() == 0 {
		return nil, status.Errorf(codes.Unavailable, "instanceID not set")
	}

	return getTableIndexUsageStats(ctx, req, t.sqlServer.pgServer.SQLServer.GetLocalIndexStatistics(),
		t.sqlServer.internalExecutor)
}

// Details returns information for a given instance ID such as
// the instance address and build info.
func (t *tenantStatusServer) Details(
	ctx context.Context, req *serverpb.DetailsRequest,
) (*serverpb.DetailsResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = t.AnnotateCtx(ctx)

	if err := t.privilegeChecker.requireViewActivityOrViewActivityRedactedPermission(ctx); err != nil {
		return nil, err
	}

	if t.sqlServer.SQLInstanceID() == 0 {
		return nil, status.Errorf(codes.Unavailable, "instanceID not set")
	}

	instanceID, local, err := t.parseInstanceID(req.NodeId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	if !local {
		instance, err := t.sqlServer.sqlInstanceProvider.GetInstance(ctx, instanceID)
		if err != nil {
			return nil, err
		}
		status, err := t.dialPod(ctx, instanceID, instance.InstanceAddr)
		if err != nil {
			return nil, err
		}
		return status.Details(ctx, req)
	}
	localInstance, err := t.sqlServer.sqlInstanceProvider.GetInstance(ctx, t.sqlServer.SQLInstanceID())
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "local instance unavailable")
	}
	resp := &serverpb.DetailsResponse{
		NodeID:     roachpb.NodeID(instanceID),
		BuildInfo:  build.GetInfo(),
		SQLAddress: util.MakeUnresolvedAddr("tcp", localInstance.InstanceAddr),
	}

	return resp, nil
}

func (t *tenantStatusServer) NodesList(
	ctx context.Context, req *serverpb.NodesListRequest,
) (*serverpb.NodesListResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = t.AnnotateCtx(ctx)

	// The node list contains details about the network addresses which are admin-only.
	if _, err := t.privilegeChecker.requireAdminUser(ctx); err != nil {
		return nil, err
	}
	instances, err := t.sqlServer.sqlInstanceProvider.GetAllInstances(ctx)
	if err != nil {
		return nil, err
	}
	var resp serverpb.NodesListResponse
	for _, instance := range instances {
		// For SQL only servers, the (RPC) Address and SQL address is the same.
		// TODO(#76175): We should split the instance address into SQL and RPC addresses.
		nodeDetails := serverpb.NodeDetails{
			NodeID:     int32(instance.InstanceID),
			Address:    util.MakeUnresolvedAddr("tcp", instance.InstanceAddr),
			SQLAddress: util.MakeUnresolvedAddr("tcp", instance.InstanceAddr),
		}
		resp.Nodes = append(resp.Nodes, nodeDetails)
	}
	return &resp, err
}

func (t *tenantStatusServer) TxnIDResolution(
	ctx context.Context, req *serverpb.TxnIDResolutionRequest,
) (*serverpb.TxnIDResolutionResponse, error) {
	ctx = t.AnnotateCtx(propagateGatewayMetadata(ctx))
	if _, err := t.privilegeChecker.requireAdminUser(ctx); err != nil {
		return nil, err
	}

	instanceID, local, err := t.parseInstanceID(req.CoordinatorID)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	if local {
		return t.localTxnIDResolution(req), nil
	}

	instance, err := t.sqlServer.sqlInstanceProvider.GetInstance(ctx, instanceID)
	if err != nil {
		return nil, err
	}
	statusClient, err := t.dialPod(ctx, instanceID, instance.InstanceAddr)
	if err != nil {
		return nil, err
	}

	return statusClient.TxnIDResolution(ctx, req)
}

func (t *tenantStatusServer) TenantRanges(
	ctx context.Context, req *serverpb.TenantRangesRequest,
) (*serverpb.TenantRangesResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = t.AnnotateCtx(ctx)

	// The tenant range report contains replica metadata which is admin-only.
	if _, err := t.privilegeChecker.requireAdminUser(ctx); err != nil {
		return nil, err
	}

	return t.sqlServer.tenantConnect.TenantRanges(ctx, req)
}

// GetFiles returns a list of files of type defined in the request.
func (t *tenantStatusServer) GetFiles(
	ctx context.Context, req *serverpb.GetFilesRequest,
) (*serverpb.GetFilesResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = t.AnnotateCtx(ctx)

	if _, err := t.privilegeChecker.requireAdminUser(ctx); err != nil {
		return nil, err
	}

	instanceID, local, err := t.parseInstanceID(req.NodeId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	if !local {
		instance, err := t.sqlServer.sqlInstanceProvider.GetInstance(ctx, instanceID)
		if err != nil {
			return nil, err
		}
		status, err := t.dialPod(ctx, instanceID, instance.InstanceAddr)
		if err != nil {
			return nil, err
		}
		return status.GetFiles(ctx, req)
	}

	return getLocalFiles(req, t.sqlServer.cfg.HeapProfileDirName, t.sqlServer.cfg.GoroutineDumpDirName)
}

func (t *tenantStatusServer) TransactionContentionEvents(
	ctx context.Context, req *serverpb.TransactionContentionEventsRequest,
) (*serverpb.TransactionContentionEventsResponse, error) {
	ctx = t.AnnotateCtx(propagateGatewayMetadata(ctx))

	if err := t.privilegeChecker.requireViewActivityOrViewActivityRedactedPermission(ctx); err != nil {
		return nil, err
	}

	user, isAdmin, err := t.privilegeChecker.getUserAndRole(ctx)
	if err != nil {
		return nil, serverError(ctx, err)
	}

	shouldRedactContendingKey := false
	if !isAdmin {
		shouldRedactContendingKey, err =
			t.privilegeChecker.hasRoleOption(ctx, user, roleoption.VIEWACTIVITYREDACTED)
		if err != nil {
			return nil, serverError(ctx, err)
		}
	}

	if t.sqlServer.SQLInstanceID() == 0 {
		return nil, status.Errorf(codes.Unavailable, "instanceID not set")
	}

	resp := &serverpb.TransactionContentionEventsResponse{}

	if len(req.NodeID) > 0 {
		parsedInstanceID, local, err := t.parseInstanceID(req.NodeID)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if local {
			return t.localTransactionContentionEvents(shouldRedactContendingKey), nil
		}

		instance, err := t.sqlServer.sqlInstanceProvider.GetInstance(ctx, parsedInstanceID)
		if err != nil {
			return nil, err
		}
		statusClient, err := t.dialPod(ctx, parsedInstanceID, instance.InstanceAddr)
		if err != nil {
			return nil, err
		}

		return statusClient.TransactionContentionEvents(ctx, req)
	}

	rpcCallFn := func(ctx context.Context, client interface{}, _ base.SQLInstanceID) (interface{}, error) {
		statusClient := client.(serverpb.StatusClient)
		return statusClient.TransactionContentionEvents(ctx, &serverpb.TransactionContentionEventsRequest{
			NodeID: "local",
		})
	}

	if err := t.iteratePods(ctx, "txn contention events for instance",
		t.dialCallback,
		rpcCallFn,
		func(instanceID base.SQLInstanceID, nodeResp interface{}) {
			txnContentionEvents := nodeResp.(*serverpb.TransactionContentionEventsResponse)
			resp.Events = append(resp.Events, txnContentionEvents.Events...)
		},
		func(_ base.SQLInstanceID, err error) {
		},
	); err != nil {
		return nil, err
	}

	sort.Slice(resp.Events, func(i, j int) bool {
		return resp.Events[i].CollectionTs.Before(resp.Events[j].CollectionTs)
	})

	return resp, nil
}
