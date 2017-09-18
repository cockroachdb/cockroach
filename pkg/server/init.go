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
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

// noopInitServer implements the Init server interface for a cluster that has
// already been initialized. Its purpose is simply to provide better error
// messages if someone tries to initialize a cluster that's already been
// initialized.
type noopInitServer struct {
	clusterID func() uuid.UUID
}

func (s *noopInitServer) Bootstrap(
	_ context.Context, _ *serverpb.BootstrapRequest,
) (*serverpb.BootstrapResponse, error) {
	return nil, grpc.Errorf(
		codes.AlreadyExists, "cluster has already been initialized with ID %s", s.clusterID())
}

// initListener wraps a net.Listener and turns its Close() method into
// a no-op. This is used so that the initServer's grpc.Server can be
// stopped without closing the listener (which it shares with the main
// grpc.Server).
type initListener struct {
	net.Listener
}

func (initListener) Close() error {
	return nil
}

// initServer manages the temporary init server used during
// bootstrapping.
type initServer struct {
	server       *Server
	grpc         *grpc.Server
	bootstrapped chan struct{}
	mu           struct {
		syncutil.Mutex
		awaitDone bool
	}
}

func newInitServer(s *Server) *initServer {
	return &initServer{server: s, bootstrapped: make(chan struct{})}
}

func (s *initServer) serve(ctx context.Context, ln net.Listener) {
	s.grpc = rpc.NewServer(s.server.rpcContext)
	serverpb.RegisterInitServer(s.grpc, s)

	s.server.stopper.RunWorker(ctx, func(context.Context) {
		netutil.FatalIfUnexpected(s.grpc.Serve(initListener{ln}))
	})
}

func (s *initServer) awaitBootstrap() error {
	select {
	case <-s.server.node.storeCfg.Gossip.Connected:
	case <-s.bootstrapped:
	case <-s.server.stopper.ShouldStop():
		return errors.New("stop called while waiting to bootstrap")
	}
	s.mu.Lock()
	s.mu.awaitDone = true
	s.mu.Unlock()
	s.grpc.GracefulStop()

	return nil
}

func (s *initServer) Bootstrap(
	ctx context.Context, request *serverpb.BootstrapRequest,
) (response *serverpb.BootstrapResponse, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.awaitDone {
		return nil, errors.New("bootstrap called after cluster already initialized")
	}

	if err := s.server.node.bootstrap(ctx, s.server.engines, s.server.cfg.Settings.Version.BootstrapVersion()); err != nil {
		log.Error(ctx, "node bootstrap failed: ", err)
		return nil, err
	}

	close(s.bootstrapped)
	return &serverpb.BootstrapResponse{}, nil
}
