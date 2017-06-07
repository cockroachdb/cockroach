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
	"fmt"
	"net"
	
	"golang.org/x/net/context"
	
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
)

type initServer struct {
	server       *Server
	ln           *net.Listener
	bootstrapped chan struct{}
}

func newInitServer(s *Server, ln *net.Listener) *initServer {
	return &initServer{server: s, ln: ln, bootstrapped: make(chan struct{})}
}

type stayOpenListener struct {
	net.Listener
}

func (l stayOpenListener) Close() error {
	return nil
}

func (s *initServer) startAndAwait(ctx context.Context) error {
	serverpb.RegisterInitServer(s.server.grpc, s)
	
	s.server.stopper.RunWorker(ctx, func(context.Context) {
		log.Info(ctx, "Starting dedicated grpc server for Init")
		netutil.FatalIfUnexpected(s.server.grpc.Serve(stayOpenListener{*s.ln}))
	})

	select {
	case <- s.server.node.storeCfg.Gossip.Connected:
	case <- s.bootstrapped:
	case <- s.server.stopper.ShouldStop():
		return errors.New("Stop called while waiting to bootstrap")
	}

	log.Info(ctx, "Stopping dedicated grpc server for Init")
	s.server.grpc.GracefulStop()
	log.Info(ctx, "grpc Stopped")
	return nil
}

func (s *initServer) Bootstrap(
	ctx context.Context,
	request *serverpb.BootstrapRequest,
) (response *serverpb.BootstrapResponse, err error) {
	log.Info(ctx, "Bootstrap", request)

	if err := s.server.node.bootstrap(ctx, s.server.engines); err != nil {
		log.Error(ctx, "Node bootstrap failed: ", err)
		return &serverpb.BootstrapResponse{}, err
	}

	log.Info(ctx, "Closing bootstrap")
	close(s.bootstrapped)
	log.Info(ctx, "Closed")
	return &serverpb.BootstrapResponse{}, nil
}

