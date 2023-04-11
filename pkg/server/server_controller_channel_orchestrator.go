// Copyright 2023 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// channelOrchestrator is an implementation of serverOrchestrator
// whose serverState uses go channels to coordinate the server's
// lifecycle.
type channelOrchestrator struct {
	parentStopper *stop.Stopper
	serverFactory serverFactoryForOrchestration
}

var _ serverOrchestrator = (*channelOrchestrator)(nil)

func newChannelOrchestrator(
	parentStopper *stop.Stopper, serverFactory serverFactoryForOrchestration,
) *channelOrchestrator {
	return &channelOrchestrator{
		parentStopper: parentStopper,
		serverFactory: serverFactory,
	}
}

// serverStateUsingChannels coordinates the lifecycle of a tenant
// server. It ensures sane concurrent behavior between:
// - requests to start a server manually, e.g. via TestServer;
// - async changes to the tenant service mode;
// - quiescence of the outer stopper;
// - RPC drain requests on the tenant server;
// - server startup errors if any.
//
// Generally, the lifecycle is as follows:
//  1. a request to start a server will cause a serverEntry to be added
//     to the server controller, in the state "not yet started".
//  2. the "managed-tenant-server" async task starts, via
//     StartControlledServer()
//  3. the async task attempts to start the server (with retries and
//     backoff delay as needed), or cancels the startup if
//     a request to stop is received asynchronously.
//  4. after the server is started, the async task waits for a shutdown
//     request.
//  5. once a shutdown request is received the async task
//     stops the server.
//
// The async task is also responsible for reporting the server
// start/stop events in the event log.
type serverStateUsingChannels struct {
	// nc holds a shared reference to the current name of the
	// tenant. If the tenant's name is updated, the `Set` method on
	// nameContainer should be called in order to update any subscribers
	// within the tenant. These are typically observability-related
	// features that label data with the current tenant name.
	nc *roachpb.TenantNameContainer

	// server is the server that is being controlled.
	server orchestratedServer

	// startedOrStopped is closed when the server has either started or
	// stopped. This can be used to wait for a server start.
	startedOrStoppedCh <-chan struct{}

	// startErr, once startedOrStopped is closed, reports the error
	// during server creation if any.
	startErr error

	// started is marked true when the server has started. This can
	// be used to observe the start state without waiting.
	started syncutil.AtomicBool

	// requestStop can be called to request a server to stop.
	// It can be called multiple times.
	requestStop func()

	// stopped is closed when the server has stopped.
	stoppedCh <-chan struct{}
}

var _ serverState = (*serverStateUsingChannels)(nil)

// getServer is part of the serverState interface.
func (s *serverStateUsingChannels) getServer() (orchestratedServer, bool) {
	return s.server, s.started.Get()
}

// nameContainer is part of the serverState interface.
func (s *serverStateUsingChannels) nameContainer() *roachpb.TenantNameContainer {
	return s.nc
}

// getLastStartupError is part of the serverState interface.
func (s *serverStateUsingChannels) getLastStartupError() error {
	return s.startErr
}

// requestGracefulShutdown is part of the serverState interface.
func (s *serverStateUsingChannels) requestGracefulShutdown(ctx context.Context) {
	// TODO(knz): this is the code that was originally implemented, and
	// it is incorrect because it does not obey the incoming context's
	// cancellation.
	s.requestStop()
}

// requestImmediateShutdown is part of the serverState interface.
func (s *serverStateUsingChannels) requestImmediateShutdown(ctx context.Context) {
	// TODO(knz): this is the code that was originally implemented,
	// and it is incorrect; this should strigger a stop on
	// the tenant stopper.
	//
	// Luckly, this implementation error happens to be innocuous because
	// the only call to reqquestImmediateShutdown() happens after the
	// parent stopper quiescence, and the control loop is already
	// sensitive to that.
	//
	// We should refactor this to remove the potential for confusion.
	s.requestStop()
}

// stopped is part of the serverState interface.
func (s *serverStateUsingChannels) stopped() <-chan struct{} {
	return s.stoppedCh
}

// startedOrStopped is part of the serverState interface.
func (s *serverStateUsingChannels) startedOrStopped() <-chan struct{} {
	return s.startedOrStoppedCh
}

// makeServerStateForSystemTenant is part of the orchestrator interface.
func (o *channelOrchestrator) makeServerStateForSystemTenant(
	nc *roachpb.TenantNameContainer, systemSrv orchestratedServer,
) serverState {
	// We make the serverState for the system mock the regular
	// lifecycle. It starts with an already-closed `startedOrStopped`
	// channel; and it properly reacts to a call to requestStop()
	// by closing its stopped channel -- albeit with no other side effects.
	closedChan := make(chan struct{})
	close(closedChan)
	closeCtx, cancelFn := context.WithCancel(context.Background())
	st := &serverStateUsingChannels{
		nc:                 nc,
		server:             systemSrv,
		startedOrStoppedCh: closedChan,
		requestStop:        cancelFn,
		stoppedCh:          closeCtx.Done(),
	}

	st.started.Set(true)
	return st
}
