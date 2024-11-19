// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"runtime/pprof"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

// channelOrchestrator is an implementation of serverOrchestrator
// whose serverState uses go channels to coordinate the server's
// lifecycle.
type channelOrchestrator struct {
	parentStopper *stop.Stopper
	serverFactory serverFactoryForOrchestration
}

// serverFactoryForOrchestration provides the method that instantiates tenant servers.
type serverFactoryForOrchestration interface {
	// newServerForOrchestrator returns a new tenant server.
	newServerForOrchestrator(ctx context.Context, nc *roachpb.TenantNameContainer, tenantStopper *stop.Stopper) (orchestratedServer, error)
}

func newChannelOrchestrator(
	parentStopper *stop.Stopper, serverFactory serverFactoryForOrchestration,
) *channelOrchestrator {
	return &channelOrchestrator{
		parentStopper: parentStopper,
		serverFactory: serverFactory,
	}
}

// serverState coordinates the lifecycle of a tenant server. It
// ensures sane concurrent behavior between:
//
// - requests to start a server manually, e.g. via testServer;
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
type serverState struct {
	// nc holds a shared reference to the current name of the
	// tenant. If the tenant's name is updated, the `Set` method on
	// nameContainer should be called in order to update any subscribers
	// within the tenant. These are typically observability-related
	// features that label data with the current tenant name.
	nc *roachpb.TenantNameContainer

	startedMu struct {
		syncutil.Mutex

		// server is the server that is being controlled.
		// This is only set when the corresponding orchestratedServer
		// instance is ready.
		server orchestratedServer
	}

	// startedOrStopped is closed when the server has either started or
	// stopped. This can be used to wait for a server start.
	startedOrStoppedCh <-chan struct{}

	// startErr, once startedOrStopped is closed, reports the error
	// during server creation if any.
	startErr error

	// requestImmediateStop can be called to request a server to stop
	// ungracefully.
	// It can be called multiple times.
	requestImmediateStop func()

	// requestGracefulStop can be called to request a server to stop gracefully.
	// It can be called multiple times.
	requestGracefulStop func()

	// stopped is closed when the server has stopped.
	stoppedCh <-chan struct{}
}

// getServer is part of the serverState interface.
func (s *serverState) getServer() (orchestratedServer, bool) {
	s.startedMu.Lock()
	defer s.startedMu.Unlock()
	return s.startedMu.server, s.startedMu.server != nil
}

// nameContainer is part of the serverState interface.
func (s *serverState) nameContainer() *roachpb.TenantNameContainer {
	return s.nc
}

// getLastStartupError is part of the serverState interface.
func (s *serverState) getLastStartupError() error {
	return s.startErr
}

// requestGracefulShutdown is part of the serverState interface.
func (s *serverState) requestGracefulShutdown(ctx context.Context) {
	// TODO(knz): this is incorrect because it does not obey the
	// incoming context's cancellation.
	s.requestGracefulStop()
}

// requestImmediateShutdown is part of the serverState interface.
func (s *serverState) requestImmediateShutdown(ctx context.Context) {
	s.requestImmediateStop()
}

// stopped is part of the serverState interface.
func (s *serverState) stopped() <-chan struct{} {
	return s.stoppedCh
}

// startedOrStopped is part of the serverState interface.
func (s *serverState) startedOrStopped() <-chan struct{} {
	return s.startedOrStoppedCh
}

// makeServerStateForSystemTenant is part of the orchestrator interface.
func (o *channelOrchestrator) makeServerStateForSystemTenant(
	nc *roachpb.TenantNameContainer, systemSrv orchestratedServer,
) *serverState {
	// We make the serverState for the system mock the regular
	// lifecycle. It starts with an already-closed `startedOrStopped`
	// channel; and it properly reacts to a call to requestStop()
	// by closing its stopped channel -- albeit with no other side effects.
	closedChan := make(chan struct{})
	close(closedChan)
	closeCtx, cancelFn := context.WithCancel(context.Background())
	st := &serverState{
		nc:                   nc,
		startedOrStoppedCh:   closedChan,
		requestImmediateStop: cancelFn,
		requestGracefulStop:  cancelFn,
		stoppedCh:            closeCtx.Done(),
	}

	st.startedMu.server = systemSrv
	return st
}

// startControlledServer is part of the serverOrchestrator interface.
func (o *channelOrchestrator) startControlledServer(
	ctx context.Context,
	// tenantName is the name of the tenant for which a server should
	// be created.
	tenantName roachpb.TenantName,
	// finalizeFn is called when the server is fully stopped.
	// This is always called, even if there is a server startup error.
	finalizeFn func(ctx context.Context, tenantName roachpb.TenantName),
	// startErrorFn is called every time there is an error starting
	// the server. Suggested use is for logging. To synchronize on the
	// server's state, use the resulting serverState instead.
	startErrorFn func(ctx context.Context, tenantName roachpb.TenantName, err error),
	// startCompleteFn is called when the server has started
	// successfully and is accepting clients. Suggested use is for
	// logging. To synchronize on the server's state, use the
	// resulting serverState instead.
	startCompleteFn func(ctx context.Context, tenantName roachpb.TenantName, tid roachpb.TenantID, sid base.SQLInstanceID),
	// serverStoppingFn is called when the server is shutting down
	// after a successful start. Suggested use is for logging. To
	// synchronize on the server's state, use the resulting
	// serverState instead.
	serverStoppingFn func(ctx context.Context, tenantName roachpb.TenantName, tid roachpb.TenantID, sid base.SQLInstanceID),
) (*serverState, error) {
	var immediateStopRequest sync.Once
	immediateStopRequestCh := make(chan struct{})
	immediateStopFn := func() {
		immediateStopRequest.Do(func() {
			close(immediateStopRequestCh)
		})
	}
	var gracefulStopRequest sync.Once
	gracefulStopRequestCh := make(chan struct{})
	gracefulStopFn := func() {
		gracefulStopRequest.Do(func() {
			close(gracefulStopRequestCh)
		})
	}

	stoppedCh := make(chan struct{})
	startedOrStoppedCh := make(chan struct{})

	state := &serverState{
		nc:                   roachpb.NewTenantNameContainer(tenantName),
		startedOrStoppedCh:   startedOrStoppedCh,
		requestImmediateStop: immediateStopFn,
		requestGracefulStop:  gracefulStopFn,
		stoppedCh:            stoppedCh,
	}

	topCtx := ctx

	// Use a different context for the tasks below, because the tenant
	// stopper will have its own tracer which is incompatible with the
	// tracer attached to the incoming context.
	tenantCtx := logtags.WithTags(context.Background(), logtags.FromContext(ctx))
	tenantCtx = logtags.AddTag(tenantCtx, "tenant-orchestration", nil)
	tenantCtx = logtags.AddTag(tenantCtx, "tenant", tenantName)

	// ctlStopper is a stopper uniquely responsible for the control
	// loop. It is separate from the tenantStopper defined below so
	// that we can retry the server instantiation if it fails.
	ctlStopper := stop.NewStopper()

	// useGracefulDrainDuringTenantShutdown defined whether a graceful
	// drain is requested on the tenant server by orchestration.
	useGracefulDrainDuringTenantShutdown := make(chan bool, 1)
	markDrainMode := func(graceful bool) {
		select {
		case useGracefulDrainDuringTenantShutdown <- graceful:
		default:
			// Avoid blocking write.
		}
	}

	// Ensure that if the surrounding server requests shutdown, we
	// propagate it to the new server.
	if err := o.parentStopper.RunAsyncTask(ctx, "propagate-close", func(ctx context.Context) {
		defer log.Infof(ctx, "propagate-close task terminating")
		select {
		case <-stoppedCh:
			// Server control loop is terminating prematurely before a
			// request was made to terminate it.
			log.Infof(ctx, "tenant %q terminating", tenantName)

		case <-o.parentStopper.ShouldQuiesce():
			// Surrounding server is stopping; propagate the stop to the
			// control goroutine below.
			// Note: we can't do a graceful drain in that case because
			// the RPC service in the surrounding server may already
			// be unavailable.
			log.Infof(ctx, "server terminating; telling tenant %q to terminate", tenantName)
			markDrainMode(false)
			ctlStopper.Stop(tenantCtx)

		case <-gracefulStopRequestCh:
			// Someone requested a graceful shutdown.
			log.Infof(ctx, "received request for tenant %q to terminate gracefully", tenantName)
			markDrainMode(true)
			ctlStopper.Stop(tenantCtx)

		case <-immediateStopRequestCh:
			// Someone requested a graceful shutdown.
			log.Infof(ctx, "received request for tenant %q to terminate immediately", tenantName)
			markDrainMode(false)
			ctlStopper.Stop(tenantCtx)

		case <-topCtx.Done():
			// Someone requested a shutdown - probably a test.
			// Note: we can't do a graceful drain in that case because
			// the RPC service in the surrounding server may already
			// be unavailable.
			log.Infof(ctx, "startup context cancelled; telling tenant %q to terminate", tenantName)
			markDrainMode(false)
			ctlStopper.Stop(tenantCtx)
		}
	}); err != nil {
		// The goroutine above is responsible for stopping the ctlStopper.
		// If it fails to stop, we stop it here to avoid leaking a
		// stopper.
		ctlStopper.Stop(ctx)
		return nil, err
	}

	if err := o.parentStopper.RunAsyncTask(ctx, "managed-tenant-server", func(_ context.Context) {
		startedOrStoppedChAlreadyClosed := false
		defer func() {
			// We may be returning early due to an error in the server initialization
			// not otherwise caused by a server shutdown. In that case, we don't have
			// a guarantee that the tenantStopper.Stop() call will ever be called
			// and we could get a goroutine leak for the above task.
			//
			// To prevent this, we call requestImmediateStop() which tells
			// the goroutine above to call tenantStopper.Stop() and
			// terminate.
			state.requestImmediateStop()
			func() {
				state.startedMu.Lock()
				defer state.startedMu.Unlock()
				state.startedMu.server = nil
			}()
			close(stoppedCh)
			if !startedOrStoppedChAlreadyClosed {
				state.startErr = errors.New("server stop before successful start")
				close(startedOrStoppedCh)
			}

			// Call the finalizer.
			if finalizeFn != nil {
				finalizeFn(ctx, tenantName)
			}
		}()

		// We use our detached tenantCtx, the incoming ctx given by
		// RunAsyncTask, because this stopper will be assigned its own
		// different tracer.
		ctx := tenantCtx

		// Set the pprof label to more easily identify
		// goroutines related to this virtual cluster. The
		// calls here are exactly what pprof.Do does.
		defer pprof.SetGoroutineLabels(ctx)
		ctx = pprof.WithLabels(ctx, pprof.Labels("cluster", string(tenantName)))
		pprof.SetGoroutineLabels(ctx)

		// We want a context that gets cancelled when the server is
		// shutting down, for the possible few cases in
		// newServerInternal/preStart/acceptClients which are not looking at the
		// tenantStopper.ShouldQuiesce() channel but are sensitive to context
		// cancellation.
		var cancel func()
		ctx, cancel = ctlStopper.WithCancelOnQuiesce(ctx)
		defer cancel()

		// Stop retrying startup/initialization if we are being shut
		// down early.
		retryOpts := retry.Options{
			Closer: ctlStopper.ShouldQuiesce(),
		}

		// tenantStopper is the stopper specific to one tenant server
		// instance. We define a new tenantStopper on every attempt to
		// instantiate the tenant server below. It is then linked to
		// ctlStopper below once the instantiation and start have
		// succeeded.
		var tenantStopper *stop.Stopper

		var tenantServer orchestratedServer
		for retry := retry.StartWithCtx(ctx, retryOpts); retry.Next(); {
			tenantStopper = stop.NewStopper()

			// Task that is solely responsible for propagating ungraceful exits.
			if err := o.propagateUngracefulStopAsync(ctx, ctlStopper, tenantStopper, immediateStopRequestCh); err != nil {
				tenantStopper.Stop(ctx)
				return
			}

			// Try to create the server.
			s, err := func() (orchestratedServer, error) {
				s, err := o.serverFactory.newServerForOrchestrator(ctx, state.nc, tenantStopper)
				if err != nil {
					return nil, errors.Wrap(err, "while creating server")
				}

				// Note: we make preStart() below derive from ctx, which is
				// cancelled on shutdown of the outer server. This is necessary
				// to ensure preStart() properly stops prematurely in that case.
				startCtx := s.annotateCtx(ctx)
				startCtx = logtags.AddTag(startCtx, "start-server", nil)
				log.Infof(startCtx, "starting tenant server")
				if err := s.preStart(startCtx); err != nil {
					return nil, errors.Wrap(err, "while starting server")
				}

				// Task that propagates graceful shutdowns.
				// This can only start after the server has started successfully.
				if err := o.propagateGracefulDrainAsync(ctx,
					tenantCtx, ctlStopper, tenantStopper,
					useGracefulDrainDuringTenantShutdown, s); err != nil {
					return nil, errors.Wrap(err, "while starting graceful drain propagation task")
				}

				return s, errors.Wrap(s.acceptClients(startCtx), "while accepting clients")
			}()
			if err != nil {
				// Creation failed. We stop the tenant stopper here, which also
				// takes care of terminating the async task we've just started above.
				tenantStopper.Stop(ctx)
				if startErrorFn != nil {
					startErrorFn(ctx, tenantName, err)
				}
				log.Warningf(ctx,
					"unable to start server for tenant %q (attempt %d, will retry): %v",
					tenantName, retry.CurrentAttempt(), err)
				state.startErr = err
				continue
			}
			tenantServer = s
			break
		}
		if tenantServer == nil {
			// Retry loop exited before the server could start. This
			// indicates that there was an async request to abandon the
			// server startup. This is OK; just terminate early. The defer
			// will take care of cleaning up.
			return
		}

		// Log the start event and ensure the stop event is logged eventually.
		tid, iid := tenantServer.getTenantID(), tenantServer.getInstanceID()
		if startCompleteFn != nil {
			startCompleteFn(ctx, tenantName, tid, iid)
		}
		if serverStoppingFn != nil {
			tenantStopper.AddCloser(stop.CloserFn(func() {
				serverStoppingFn(ctx, tenantName, tid, iid)
			}))
		}

		// Indicate the server has started.
		startedOrStoppedChAlreadyClosed = true
		func() {
			state.startedMu.Lock()
			defer state.startedMu.Unlock()
			state.startedMu.server = tenantServer
		}()
		close(startedOrStoppedCh)

		// Wait for a request to shut down.
		for {
			select {
			case <-tenantStopper.ShouldQuiesce():
				log.Infof(ctx, "tenant %q finishing their own control loop", tenantName)
				return

			case shutdownRequest := <-tenantServer.shutdownRequested():
				log.Infof(ctx, "tenant %q requesting their own shutdown: %v",
					tenantName, shutdownRequest.ShutdownCause())
				// Make the async stop goroutine above pick up the task of shutting down.
				if shutdownRequest.TerminateUsingGracefulDrain() {
					state.requestGracefulStop()
				} else {
					state.requestImmediateStop()
				}
			}
		}
	}); err != nil {
		// Clean up the task we just started before.
		state.requestImmediateStop()
		return nil, err
	}

	return state, nil
}

// propagateUngracefulStopAsync propagates ungraceful stop requests
// from the surrounding KV node into the tenant server.
func (o *channelOrchestrator) propagateUngracefulStopAsync(
	ctx context.Context,
	ctlStopper, tenantStopper *stop.Stopper,
	immediateStopRequestCh <-chan struct{},
) error {
	return ctlStopper.RunAsyncTask(ctx, "propagate-ungraceful-stop", func(ctx context.Context) {
		defer log.Infof(ctx, "propagate-ungraceful-stop task terminating")
		select {
		case <-tenantStopper.ShouldQuiesce():
			// Tenant server shutting down on its own.
			return
		case <-immediateStopRequestCh:
			// An immediate stop request is catching up with the
			// graceful drain above.
			tenantStopper.Stop(ctx)
		case <-o.parentStopper.ShouldQuiesce():
			// Expedited shutdown of the surrounding KV node.
			tenantStopper.Stop(ctx)
		}
	})
}

// propagateGracefulDrainAsync propagates graceful drain requests
// from the surrounding KV node into the tenant server.
func (o *channelOrchestrator) propagateGracefulDrainAsync(
	ctx, tenantCtx context.Context,
	ctlStopper, tenantStopper *stop.Stopper,
	gracefulDrainRequestCh <-chan bool,
	tenantServer orchestratedServer,
) error {
	return ctlStopper.RunAsyncTask(ctx, "propagate-graceful-drain", func(ctx context.Context) {
		defer log.Infof(ctx, "propagate-graceful-drain task terminating")
		select {
		case <-tenantStopper.ShouldQuiesce():
			// Tenant server shutting down on its own.
			return
		case <-ctlStopper.ShouldQuiesce():
			gracefulDrainRequested := <-gracefulDrainRequestCh
			if gracefulDrainRequested {
				// Ensure that the graceful drain for the tenant server aborts
				// early if the Stopper for the surrounding server is
				// prematurely shutting down. This is because once the surrounding node
				// starts quiescing tasks, it won't be able to process KV requests
				// by the tenant server any more.
				//
				// Beware: we use tenantCtx here, not ctx, because the
				// latter has been linked to ctlStopper.Quiesce already
				// -- and in this select branch that context has been
				// canceled already.
				drainCtx, cancel := o.parentStopper.WithCancelOnQuiesce(tenantCtx)
				defer cancel()
				// If an immediate drain catches up with the graceful drain, make
				// the former cancel the ctx too.
				var cancel2 func()
				drainCtx, cancel2 = tenantStopper.WithCancelOnQuiesce(drainCtx)
				defer cancel2()

				log.Infof(drainCtx, "starting graceful drain")
				// Call the drain service on that tenant's server. This may take a
				// while as it needs to wait for clients to disconnect and SQL
				// activity to clear up, possibly waiting for various configurable
				// timeouts.
				CallDrainServerSide(drainCtx, tenantServer.gracefulDrain)
			}
			tenantStopper.Stop(ctx)
		}
	})
}
