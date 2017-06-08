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
	"errors"
	"net"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"golang.org/x/net/context"
)

type initServer struct {
	server       *Server
	bootstrapped chan struct{}
	mux          sync.Mutex
}

func newInitServer(s *Server) *initServer {
	return &initServer{server: s, bootstrapped: make(chan struct{})}
}

func (s *initServer) startAndAwait(ctx context.Context, ln net.Listener) error {
	grpcServer := s.server.grpc
	serverpb.RegisterInitServer(grpcServer, s)

	s.server.stopper.RunWorker(ctx, func(context.Context) {
		netutil.FatalIfUnexpected(grpcServer.Serve(ln))
	})

	select {
	case <-s.server.node.storeCfg.Gossip.Connected:
	case <-s.bootstrapped:
	case <-s.server.stopper.ShouldStop():
		return errors.New("Stop called while waiting to bootstrap")
	}

	return nil
}

func (s *initServer) Bootstrap(ctx context.Context, request *serverpb.BootstrapRequest,
) (response *serverpb.BootstrapResponse, err error) {
	s.mux.Lock() // Mux bootstrap
	defer s.mux.Unlock()
	if err := s.server.node.bootstrap(ctx, s.server.engines); err != nil {
		log.Error(ctx, "Node bootstrap failed: ", err)
		return nil, err
	}

	close(s.bootstrapped)
	return &serverpb.BootstrapResponse{}, nil
}
