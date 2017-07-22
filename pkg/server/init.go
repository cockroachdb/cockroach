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

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"golang.org/x/net/context"
)

type initServer struct {
	server       *Server
	bootstrapped chan struct{}
	mu           syncutil.Mutex
	waiting      bool
}

func newInitServer(s *Server) *initServer {
	return &initServer{server: s, bootstrapped: make(chan struct{}), waiting: false}
}

func (s *initServer) serve(ctx context.Context, ln net.Listener) {
	grpcServer := s.server.grpc
	serverpb.RegisterInitServer(grpcServer, s)

	s.server.stopper.RunWorker(ctx, func(context.Context) {
		netutil.FatalIfUnexpected(grpcServer.Serve(ln))
	})
}

func (s *initServer) awaitBootstrap() error {
	s.mu.Lock()
	s.waiting = true
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.waiting = false
		s.mu.Unlock()
	}()

	select {
	case <-s.server.node.storeCfg.Gossip.Connected:
	case <-s.bootstrapped:
	case <-s.server.stopper.ShouldStop():
		return errors.New("Stop called while waiting to bootstrap")
	}

	return nil
}

func (s *initServer) Bootstrap(
	ctx context.Context, request *serverpb.BootstrapRequest,
) (response *serverpb.BootstrapResponse, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.waiting {
		return nil, errors.New("Init not expecting Bootstrap")
	}

	if err := s.server.node.bootstrap(ctx, s.server.engines); err != nil {
		log.Error(ctx, "Node bootstrap failed: ", err)
		return nil, err
	}

	close(s.bootstrapped)
	return &serverpb.BootstrapResponse{}, nil
}
