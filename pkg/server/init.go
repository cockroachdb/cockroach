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
	"context"

	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type semaphore chan struct{}

func newSemaphore() semaphore {
	ch := make(semaphore, 1)
	ch <- struct{}{}
	return ch
}

func (s semaphore) acquire() {
	<-s
}

func (s semaphore) release() {
	s <- struct{}{}
}

// initServer manages the temporary init server used during
// bootstrapping.
type initServer struct {
	server       *Server
	bootstrapped chan struct{}
	semaphore
}

func newInitServer(s *Server) *initServer {
	return &initServer{
		server:       s,
		semaphore:    newSemaphore(),
		bootstrapped: make(chan struct{}),
	}
}

func (s *initServer) awaitBootstrap() error {
	select {
	case <-s.server.node.storeCfg.Gossip.Connected:
	case <-s.bootstrapped:
	case <-s.server.stopper.ShouldStop():
		return errors.New("stop called while waiting to bootstrap")
	}

	return nil
}

func (s *initServer) Bootstrap(
	ctx context.Context, request *serverpb.BootstrapRequest,
) (response *serverpb.BootstrapResponse, err error) {
	s.semaphore.acquire()
	defer s.semaphore.release()

	if err := s.server.node.bootstrap(ctx, s.server.engines, s.server.cfg.Settings.Version.BootstrapVersion()); err != nil {
		if _, ok := err.(*duplicateBootstrapError); ok {
			return nil, status.Errorf(codes.AlreadyExists, err.Error())
		}
		log.Error(ctx, "node bootstrap failed: ", err)
		return nil, err
	}

	close(s.bootstrapped)
	return &serverpb.BootstrapResponse{}, nil
}
