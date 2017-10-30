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
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

// initServer manages the temporary init server used during
// bootstrapping.
type initServer struct {
	server       *Server
	bootstrapped chan struct{}
	mu           struct {
		syncutil.Mutex
		awaitDone bool
	}
}

func newInitServer(s *Server) *initServer {
	return &initServer{server: s, bootstrapped: make(chan struct{})}
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

	return nil
}

func (s *initServer) Bootstrap(
	ctx context.Context, request *serverpb.BootstrapRequest,
) (response *serverpb.BootstrapResponse, err error) {
	if !isInitMode(s.server) {
		return nil, grpc.Errorf(
			codes.AlreadyExists, "cluster has already been initialized with ID %s", s.server.ClusterID())
	}

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
