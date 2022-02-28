// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// TODO(azhng): The implementation for tenantAdminServer here will need to be updated
//  once we have pod-to-pod communication implemented. After all dependencies that are
//  unavailable to tenants have been removed, we can likely remove tenant admin server
//  entirely and use the normal admin server.

package server

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	gwruntime "github.com/grpc-ecosystem/grpc-gateway/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type tenantAdminServer struct {
	log.AmbientContext
	serverpb.UnimplementedAdminServer
	sqlServer *SQLServer
	drain     *drainServer

	// TODO(knz): find a way to avoid using status here,
	// for example by lifting the services used from admin into
	// a separate object.
	status *tenantStatusServer
}

// We require that `tenantAdminServer` implement
// `serverpb.AdminServer` even though we only have partial
// implementation, in order to serve some endpoints on tenants.
var _ serverpb.AdminServer = &tenantAdminServer{}

func (t *tenantAdminServer) RegisterService(g *grpc.Server) {
	serverpb.RegisterAdminServer(g, t)
}

func (t *tenantAdminServer) RegisterGateway(
	ctx context.Context, mux *gwruntime.ServeMux, conn *grpc.ClientConn,
) error {
	ctx = t.AnnotateCtx(ctx)
	return serverpb.RegisterAdminHandler(ctx, mux, conn)
}

var _ grpcGatewayServer = &tenantAdminServer{}

func newTenantAdminServer(
	ambientCtx log.AmbientContext,
	sqlServer *SQLServer,
	status *tenantStatusServer,
	drain *drainServer,
) *tenantAdminServer {
	return &tenantAdminServer{
		AmbientContext: ambientCtx,
		sqlServer:      sqlServer,
		drain:          drain,
		status:         status,
	}
}

// Health returns liveness for the node target of the request.
//
// See the docstring for HealthRequest for more details about
// what this function precisely reports.
//
// Note: Health is non-privileged and non-authenticated and thus
// must not report privileged information.
func (t *tenantAdminServer) Health(
	ctx context.Context, req *serverpb.HealthRequest,
) (*serverpb.HealthResponse, error) {
	telemetry.Inc(telemetryHealthCheck)

	resp := &serverpb.HealthResponse{}
	// If Ready is not set, the client doesn't want to know whether this node is
	// ready to receive client traffic.
	if !req.Ready {
		return resp, nil
	}

	if !t.sqlServer.isReady.Get() {
		return nil, status.Errorf(codes.Unavailable, "node is not accepting SQL clients")
	}

	return resp, nil
}

func (t *tenantAdminServer) dialPod(
	ctx context.Context, instanceID base.SQLInstanceID, addr string,
) (serverpb.AdminClient, error) {
	conn, err := t.sqlServer.execCfg.RPCContext.GRPCDialPod(addr, instanceID, rpc.DefaultClass).Connect(ctx)
	if err != nil {
		return nil, err
	}

	// nb: The server on the pods doesn't implement all the methods of the
	// `AdminService`. It is up to the caller of `dialPod` to only call
	// methods that are implemented on the tenant server.
	return serverpb.NewAdminClient(conn), nil
}

// Drain puts the node into the specified drain mode(s) and optionally
// instructs the process to terminate.
// This method is part of the serverpb.AdminClient interface.
func (t *tenantAdminServer) Drain(
	req *serverpb.DrainRequest, stream serverpb.Admin_DrainServer,
) error {
	ctx := stream.Context()
	ctx = t.AnnotateCtx(ctx)

	// Which node is this request for?
	parsedInstanceID, local, err := t.status.parseInstanceID(req.NodeId)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, err.Error())
	}
	if !local {
		instance, err := t.sqlServer.sqlInstanceProvider.GetInstance(ctx, parsedInstanceID)
		if err != nil {
			return err
		}
		// This request is for another node. Forward it.
		// In contrast to many RPC calls we implement around
		// the server package, the Drain RPC is a *streaming*
		// RPC. This means that it may have more than one
		// response. We must forward all of them.

		// Connect to the target node.
		client, err := t.dialPod(ctx, parsedInstanceID, instance.InstanceAddr)
		if err != nil {
			return serverError(ctx, err)
		}
		return delegateDrain(ctx, req, client, stream)
	}

	return t.drain.handleDrain(ctx, req, stream)
}
