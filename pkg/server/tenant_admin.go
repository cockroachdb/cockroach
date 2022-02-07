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

func newTenantAdminServer(ambientCtx log.AmbientContext, sqlServer *SQLServer) *tenantAdminServer {
	return &tenantAdminServer{
		AmbientContext: ambientCtx,
		sqlServer:      sqlServer,
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

// TODO(knz): add Drain implementation here.
