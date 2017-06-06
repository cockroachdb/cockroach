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
//
// Author: Adam Gee (adamgee@gmail.com)

package server

import (
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"google.golang.org/grpc"
	"golang.org/x/net/context"
)

type initServer struct {
	server     *Server
}

func newInitServer(s *Server) *initServer {
	return &initServer{server: s}
}

// RegisterService registers the GRPC service.
func (s *initServer) RegisterService(g *grpc.Server) {
	serverpb.RegisterInitServer(g, s)
}

func (s *initServer) Bootstrap(
	ctx context.Context,
	request *serverpb.BootstrapRequest,
) (response *serverpb.BootstrapResponse, err error) {
	log.Info(ctx, "Bootstrap")
	return &serverpb.BootstrapResponse{}, nil
}

