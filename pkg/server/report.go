// Copyright 2017 The Cockroach Authors.
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

package server

import (
	gwruntime "github.com/grpc-ecosystem/grpc-gateway/runtime"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// A reportServer is a grpc server that provides detailed reports.
type reportServer struct {
	log.AmbientContext

	nodeLiveness *storage.NodeLiveness
	rpcCtx       *rpc.Context
	status       *statusServer
	stopper      *stop.Stopper
}

// newReportServer allocates and returns a reportServer.
func newReportServer(
	ambient log.AmbientContext,
	nodeLiveness *storage.NodeLiveness,
	rpcCtx *rpc.Context,
	status *statusServer,
	stopper *stop.Stopper,
) *reportServer {
	ambient.AddLogTag("report", nil)
	return &reportServer{
		AmbientContext: ambient,
		nodeLiveness:   nodeLiveness,
		rpcCtx:         rpcCtx,
		status:         status,
		stopper:        stopper,
	}
}

// RegisterService registers the GRPC service.
func (s *reportServer) RegisterService(g *grpc.Server) {
	serverpb.RegisterReportServer(g, s)
}

// RegisterGateway starts the gateway (i.e. reverse
// proxy) that proxies HTTP requests to the appropriate gRPC endpoints.
func (s *reportServer) RegisterGateway(
	ctx context.Context, mux *gwruntime.ServeMux, conn *grpc.ClientConn,
) error {
	ctx = s.AnnotateCtx(ctx)
	return serverpb.RegisterReportHandler(ctx, mux, conn)
}
