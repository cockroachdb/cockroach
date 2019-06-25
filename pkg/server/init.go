// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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

// testOrSetRejectErr set the reject error unless a reject error was already
// set, in which case it returns the one that was already set. If no error had
// previously been set, returns nil.
func (s *initServer) testOrSetRejectErr(err error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.rejectErr != nil {
		return s.mu.rejectErr
	}
	s.mu.rejectErr = err
	return nil
}

type initServerResult int

const (
	invalidInitResult initServerResult = iota
	connectedToCluster
	needBootstrap
)

// awaitBootstrap blocks until the connected channel is closed or a Bootstrap()
// call is received. It returns true if a Bootstrap() call is received,
// instructing the caller to perform cluster bootstrap. It returns false if the
// connected channel is closed, telling the caller that someone else
// bootstrapped the cluster. Assuming that the connected channel comes from
// Gossip, this means that the cluster ID is now available in gossip.
func (s *initServer) awaitBootstrap() (initServerResult, error) {
	select {
	case <-s.connected:
		_ = s.testOrSetRejectErr(fmt.Errorf("already connected to cluster"))
		return connectedToCluster, nil
	case <-s.bootstrapReqCh:
		return needBootstrap, nil
	case <-s.shouldStop:
		err := fmt.Errorf("stop called while waiting to bootstrap")
		_ = s.testOrSetRejectErr(err)
		return invalidInitResult, err
	}
}

// Bootstrap unblocks an awaitBootstrap() call. If awaitBootstrap() hasn't been
// called yet, it will not block the next time it's called.
//
// TODO(andrei): There's a race between gossip connecting and this initServer
// getting a Bootstrap request that allows both to succeed: there's no
// synchronization between gossip and this server and so gossip can succeed in
// propagating one cluster ID while this call succeeds in telling the Server to
// bootstrap and created a new cluster ID. We should fix it somehow by tangling
// the gossip.Server with this initServer such that they serialize access to a
// clusterID and decide among themselves a single winner for the race.
func (s *initServer) Bootstrap(
	ctx context.Context, request *serverpb.BootstrapRequest,
) (response *serverpb.BootstrapResponse, err error) {
	if err := s.testOrSetRejectErr(errClusterInitialized); err != nil {
		return nil, err
	}
	close(s.bootstrapReqCh)
	return &serverpb.BootstrapResponse{}, nil
}

var errClusterInitialized = fmt.Errorf("cluster has already been initialized")
