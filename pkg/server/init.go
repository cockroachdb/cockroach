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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// initServer manages the temporary init server used during
// bootstrapping.
type initServer struct {
	mu struct {
		syncutil.Mutex
		// If set, a Bootstrap() call is rejected with this error.
		rejectErr error
	}
	bootstrapReqCh chan struct{}
	connected      <-chan struct{}
	shouldStop     <-chan struct{}
}

func newInitServer(connected <-chan struct{}, shouldStop <-chan struct{}) *initServer {
	return &initServer{
		bootstrapReqCh: make(chan struct{}),
		connected:      connected,
		shouldStop:     shouldStop,
	}
}

// setRejectErr sets the error that future Bootstrap() calls will get.
func (s *initServer) setRejectErr(err error) {
	s.mu.Lock()
	s.mu.rejectErr = err
	s.mu.Unlock()
}

// compareAndSwapErr calls setRejectErr() unless a reject error was already set,
// in which case it returns the one that was already set. If no error had
// previously been set, returns nil.
func (s *initServer) compareAndSwapErr(err error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.rejectErr != nil {
		return s.mu.rejectErr
	}
	s.mu.rejectErr = err
	return nil
}

// awaitBootstrap blocks until the connected channel is closed or a Bootstrap()
// call is received. It returns true if a Bootstrap() call is received,
// instructing the caller to perform cluster bootstrap. It returns false if the
// connected channel is closed, telling the caller that someone else
// bootstrapped the cluster. Assuming that the connected channel comes from
// Gossip, this means that the cluster ID is now available in gossip.
func (s *initServer) awaitBootstrap() (bool, error) {
	select {
	case <-s.connected:
		_ = s.compareAndSwapErr(fmt.Errorf("already connected to cluster"))
		return false, nil
	case <-s.bootstrapReqCh:
		return true, nil
	case <-s.shouldStop:
		err := fmt.Errorf("stop called while waiting to bootstrap")
		_ = s.compareAndSwapErr(err)
		return false, err
	}
}

// Bootstrap unblocks an awaitBootstrap() call. If awaitBootstrap() hasn't been
// called yet, it will not block the next time it's called.
func (s *initServer) Bootstrap(
	ctx context.Context, request *serverpb.BootstrapRequest,
) (response *serverpb.BootstrapResponse, err error) {
	if err := s.compareAndSwapErr(fmt.Errorf("already bootstrapped")); err != nil {
		return nil, err
	}
	close(s.bootstrapReqCh)
	return &serverpb.BootstrapResponse{}, nil
}
