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
	"io"
	"net"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type initServer struct {
	server       *Server
	bootstrapped int32
	didBootstrap chan struct{}
	mux          sync.Mutex
}

func newInitServer(s *Server) *initServer {
	return &initServer{server: s, bootstrapped: 0, didBootstrap: make(chan struct{})}
}

func (s *initServer) startAndAwait(ctx context.Context, ln net.Listener) error {
	grpcServer := grpc.NewServer()
	serverpb.RegisterInitServer(grpcServer, s)

	s.server.stopper.RunWorker(ctx, func(context.Context) {
		netutil.FatalIfUnexpected(grpcServer.Serve(ln))
	})

	select {
	case <-s.server.node.storeCfg.Gossip.Connected:
	case <-s.didBootstrap:
	case <-s.server.stopper.ShouldStop():
		return errors.New("Stop called while waiting to bootstrap")
	}

	// Set the bootstrapped bit to stop accepting connections.
	if !atomic.CompareAndSwapInt32(&s.bootstrapped, 0, 1) {
		return errors.New("Failed to set bootstrapped")
	}

	// NOTE(adamgee): Stopping our private grpc server somehow affects the shared cmux connection.
	//grpcServer.GracefulStop()
	return nil
}

func (s *initServer) Bootstrap(ctx context.Context, request *serverpb.BootstrapRequest,
) (response *serverpb.BootstrapResponse, err error) {
	log.Info(ctx, "Bootstrap", request)
	s.mux.Lock()
	defer s.mux.Unlock()

	if err := s.server.node.bootstrap(ctx, s.server.engines); err != nil {
		log.Error(ctx, "Node bootstrap failed: ", err)
		return nil, err
	}

	log.Info(ctx, "Closing bootstrap")
	close(s.didBootstrap)
	log.Info(ctx, "Closed")
	return &serverpb.BootstrapResponse{}, nil
}

func (s *initServer) acceptConnection(_ io.Reader) bool {
	return atomic.LoadInt32(&s.bootstrapped) == 0
}
