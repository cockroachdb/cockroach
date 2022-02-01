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
	"github.com/cockroachdb/cockroach/pkg/util/log"
	gwruntime "github.com/grpc-ecosystem/grpc-gateway/runtime"
	"google.golang.org/grpc"
)

type tenantAdminServer struct {
	log.AmbientContext
	serverpb.UnimplementedAdminServer
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

func newTenantAdminServer(ambientCtx log.AmbientContext) *tenantAdminServer {
	return &tenantAdminServer{AmbientContext: ambientCtx}
}

// TODO(knz): add Drain implementation here.
// TODO(rima): add Nodes implementation here for debug zip.
