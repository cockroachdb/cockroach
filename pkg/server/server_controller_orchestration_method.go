// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverctl"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/redact"
)

// serverState is the interface that the server controller uses to
// interact with one of the orchestration methods.
type serverState interface {
	// requestGracefulShutdown requests a graceful shutdown of the
	// server, via the drain logic. The function returns immediately.
	// The drain is performed in the background. To wait on actual
	// server shutdown, the caller should also wait on the stopped()
	// channel.
	//
	// We are generally assuming the graceful drain will eventually
	// succeed, although it may take a while due to various
	// user-configurable timeouts. The caller can forcefully cancel this
	// by either cancelling the context argument; or by quiescing the
	// stopper that this serverState was instantiated from.
	// (There is not need to make the context argument hang off
	// the stopper using WithCancelOnDrain.)
	//
	// Beware that the context passed as argument should remain active
	// for as long as the (background) shutdown takes. The caller should
	// not cancel it immediately.
	requestGracefulShutdown(ctx context.Context)

	// requestImmediateShutdown requests an immediate (and ungraceful)
	// shutdown of the server. The function returns immediately,
	// possibly before all server tasks are terminated. The caller can
	// wait on the actual shutdown by waiting on the stopped() channel.
	requestImmediateShutdown(ctx context.Context)

	// stopped returns a channel that is closed when the server is fully stopped
	// (no tasks remaining).
	stopped() <-chan struct{}

	// startedOrStopped returns a channel that is closed when the server
	// is fully started or fully stopped.
	startedOrStopped() <-chan struct{}

	// getServer retrieves the orchestrated server and whether the server is ready.
	//
	// The first return value is only guaranteed to be non-nil when the isReady value is true.
	getServer() (server orchestratedServer, isReady bool)

	// getLastStartupError returns the last known startup error.
	getLastStartupError() error

	// nameContainer returns a shared reference to the current name of the
	// tenant. If the tenant's name is updated, the `Set` method on
	// nameContainer should be called in order to update any subscribers
	// within the tenant. These are typically observability-related
	// features that label data with the current tenant name.
	nameContainer() *roachpb.TenantNameContainer
}

// orchestratedServer is the subset of the onDemandServer interface
// that is sufficient to orchestrate the lifecycle of a server.
type orchestratedServer interface {
	// annotateCtx annotates the context with server-specific logging tags.
	annotateCtx(context.Context) context.Context

	// preStart activates background tasks and initializes subsystems
	// but does not yet accept incoming connections.
	// Graceful drain becomes possible after preStart() returns.
	// Note that there may be background tasks remaining even if preStart
	// returns an error.
	preStart(context.Context) error

	// acceptClients starts accepting incoming connections.
	acceptClients(context.Context) error

	// shutdownRequested returns the shutdown request channel,
	// which is triggered when the server encounters an internal
	// condition or receives an external RPC that requires it to shut down.
	shutdownRequested() <-chan serverctl.ShutdownRequest

	// gracefulDrain drains the server. It should be called repeatedly
	// until the first value reaches zero.
	gracefulDrain(ctx context.Context, verbose bool) (uint64, redact.RedactableString, error)

	// getTenantID returns the tenant ID.
	getTenantID() roachpb.TenantID

	// getInstanceID returns the instance ID. This is not well-defined
	// until preStart() returns successfully.
	getInstanceID() base.SQLInstanceID
}

// serverOrchestrator abstracts over the orchestration method for tenant
// servers. This interface allows us to decouple the orchestration
// logic from the implementation of server controller.
type serverOrchestrator interface {
	// makeServerStateForSystemTenant returns a serverState for the
	// system tenant.
	makeServerStateForSystemTenant(nc *roachpb.TenantNameContainer, systemSrv orchestratedServer) serverState

	// startControlledServer starts a tenant server.
	startControlledServer(ctx context.Context,
		// tenantName is the name of the tenant for which a server should
		// be created.
		tenantName roachpb.TenantName,
		// finalizeFn is called when the server is fully stopped.
		// This is always called, even if there is a server startup error.
		// It is also called asynchronously, possibly after
		// startControlledServer has returned.
		finalizeFn func(ctx context.Context, tenantName roachpb.TenantName),
		// startErrorFn is called every time there is an error starting
		// the server. Suggested use is for logging. To synchronize on the
		// server's state, use the resulting serverState instead.
		startErrorFn func(ctx context.Context, tenantName roachpb.TenantName, err error),
		// serverStartedFn is called when the server has started
		// successfully and is accepting clients. Suggested use is for
		// logging. To synchronize on the server's state, use the
		// resulting serverState instead.
		serverStartedFn func(ctx context.Context, tenantName roachpb.TenantName, tid roachpb.TenantID, sid base.SQLInstanceID),
		// serverStoppingFn is called when the server is shutting down
		// after a successful start. Suggested use is for logging. To
		// synchronize on the server's state, use the resulting
		// serverState instead.
		serverStoppingFn func(ctx context.Context, tenantName roachpb.TenantName, tid roachpb.TenantID, sid base.SQLInstanceID),
	) (state serverState, err error)
}

// serverFactoryForOrchestration provides the method that instantiates tenant servers.
type serverFactoryForOrchestration interface {
	// newServerForOrchestrator returns a new tenant server.
	newServerForOrchestrator(ctx context.Context, nc *roachpb.TenantNameContainer, tenantStopper *stop.Stopper) (orchestratedServer, error)
}

// newServerOrchestrator returns the orchestration method to use.
func newServerOrchestrator(
	parentStopper *stop.Stopper, serverFactory serverFactoryForOrchestration,
) serverOrchestrator {
	// TODO(knz): make this configurable for testing.
	return newChannelOrchestrator(parentStopper, serverFactory)
}
