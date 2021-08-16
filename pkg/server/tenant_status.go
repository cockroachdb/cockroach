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
	"fmt"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/contention"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance"
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
	contentionRegistry *contention.Registry,
	flowScheduler *flowinfra.FlowScheduler,
	st *cluster.Settings,
	sqlServer *SQLServer,
	rpcCtx *rpc.Context,
	stopper *stop.Stopper,
) *tenantStatusServer {
	ambient.AddLogTag("tenant-status", nil)
	return &tenantStatusServer{
		baseStatusServer: baseStatusServer{
			AmbientContext:     ambient,
			privilegeChecker:   privilegeChecker,
			sessionRegistry:    sessionRegistry,
			contentionRegistry: contentionRegistry,
			flowScheduler:      flowScheduler,
			st:                 st,
			sqlServer:          sqlServer,
			rpcCtx:             rpcCtx,
			stopper:            stopper,
		},
	}
}

func (t *tenantStatusServer) ListSessions(
	ctx context.Context, request *serverpb.ListSessionsRequest,
) (*serverpb.ListSessionsResponse, error) {
	return t.ListLocalSessions(ctx, request)
}

func (t *tenantStatusServer) ListLocalSessions(
	ctx context.Context, request *serverpb.ListSessionsRequest,
) (*serverpb.ListSessionsResponse, error) {
	sessions, err := t.getLocalSessions(ctx, request)
	if err != nil {
		return nil, err
	}
	return &serverpb.ListSessionsResponse{Sessions: sessions}, nil
}

func (t *tenantStatusServer) CancelQuery(
	ctx context.Context, request *serverpb.CancelQueryRequest,
) (*serverpb.CancelQueryResponse, error) {
	reqUsername := security.MakeSQLUsernameFromPreNormalizedString(request.Username)
	if err := t.checkCancelPrivilege(ctx, reqUsername, findSessionByQueryID(request.QueryID)); err != nil {
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

func (t *tenantStatusServer) CancelSession(
	ctx context.Context, request *serverpb.CancelSessionRequest,
) (*serverpb.CancelSessionResponse, error) {
	reqUsername := security.MakeSQLUsernameFromPreNormalizedString(request.Username)
	if err := t.checkCancelPrivilege(ctx, reqUsername, findSessionBySessionID(request.SessionID)); err != nil {
		return nil, err
	}
	return t.sessionRegistry.CancelSession(request.SessionID)
}

func (t *tenantStatusServer) ListContentionEvents(
	ctx context.Context, request *serverpb.ListContentionEventsRequest,
) (*serverpb.ListContentionEventsResponse, error) {
	return t.ListLocalContentionEvents(ctx, request)
}

func (t *tenantStatusServer) ResetSQLStats(
	ctx context.Context, _ *serverpb.ResetSQLStatsRequest,
) (*serverpb.ResetSQLStatsResponse, error) {
	controller := t.sqlServer.pgServer.SQLServer.GetSQLStatsController()
	controller.ResetLocalSQLStats(ctx)
	return &serverpb.ResetSQLStatsResponse{}, nil
}

func (t *tenantStatusServer) CombinedStatementStats(
	ctx context.Context, req *serverpb.CombinedStatementsStatsRequest,
) (*serverpb.StatementsResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = t.AnnotateCtx(ctx)

	if _, err := t.privilegeChecker.requireViewActivityPermission(ctx); err != nil {
		return nil, err
	}

	if t.sqlServer.SQLInstanceID() == 0 {
		return nil, status.Errorf(codes.Unavailable, "instanceID not set")
	}

	return getCombinedStatementStats(ctx, req, t.sqlServer.pgServer.SQLServer.GetSQLStatsProvider(),
		t.sqlServer.internalExecutor)
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
	ctx = propagateGatewayMetadata(ctx)
	ctx = t.AnnotateCtx(ctx)

	if _, err := t.privilegeChecker.requireViewActivityPermission(ctx); err != nil {
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
			return statementsLocal(ctx, roachpb.NodeID(t.sqlServer.SQLInstanceID()), t.sqlServer)
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

	dialFn := func(ctx context.Context, instanceID base.SQLInstanceID, addr string) (interface{}, error) {
		client, err := t.dialPod(ctx, instanceID, addr)
		return client, err
	}
	nodeStatement := func(ctx context.Context, client interface{}, _ base.SQLInstanceID) (interface{}, error) {
		statusClient := client.(serverpb.StatusClient)
		return statusClient.Statements(ctx, localReq)
	}

	if err := t.iteratePods(ctx, fmt.Sprintf("statement statistics for node %s", req.NodeID),
		dialFn,
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
	return t.ListLocalDistSQLFlows(ctx, request)
}

func (t *tenantStatusServer) IndexUsageStatistics(
	ctx context.Context, request *serverpb.IndexUsageStatisticsRequest,
) (*serverpb.IndexUsageStatisticsResponse, error) {
	if _, err := t.privilegeChecker.requireViewActivityPermission(ctx); err != nil {
		return nil, err
	}

	idxUsageStats := t.sqlServer.pgServer.SQLServer.GetLocalIndexStatistics()
	return indexUsageStatsLocal(idxUsageStats)
}
