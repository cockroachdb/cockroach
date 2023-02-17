// Copyright 2022 The Cockroach Authors.
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
	"net"
	"net/http"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirecancel"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// onDemandServer represents a server that can be started on demand.
type onDemandServer interface {
	stop(context.Context)

	// getHTTPHandlerFn retrieves the function that can serve HTTP
	// requests for this server.
	getHTTPHandlerFn() http.HandlerFunc

	// handleCancel processes a SQL async cancel query.
	handleCancel(ctx context.Context, cancelKey pgwirecancel.BackendKeyData) error

	// serveConn handles an incoming SQL connection.
	serveConn(ctx context.Context, conn net.Conn, status pgwire.PreServeStatus) error

	// getSQLAddr returns the SQL address for this server.
	getSQLAddr() string

	// getRPCAddr() returns the RPC address for this server.
	getRPCAddr() string

	// getTenantID returns the TenantID for this server.
	getTenantID() roachpb.TenantID

	// getInstanceID returns the SQLInstanceID for this server.
	getInstanceID() base.SQLInstanceID

	// shutdownRequested returns the shutdown request channel.
	shutdownRequested() <-chan ShutdownRequest
}

type serverEntry struct {
	// server is the actual server.
	// This is only defined once state.started is true.
	server onDemandServer

	// nameContainer holds a shared reference to the current
	// name of the tenant within this serverEntry. If the
	// tenant's name is updated, the `Set` method on
	// nameContainer should be called in order to update
	// any subscribers within the tenant. These are typically
	// observability-related features that label data with
	// the current tenant name.
	nameContainer *roachpb.TenantNameContainer

	// state coordinates the server's lifecycle.
	state serverState
}

// serverController manages a fleet of multiple servers side-by-side.
// They are instantiated on demand the first time they are accessed.
// Instantiation can fail, e.g. if the target tenant doesn't exist or
// is not active.
type serverController struct {
	// nodeID is the node ID of the server where the controller
	// is running. This is used for logging only.
	nodeID *base.NodeIDContainer

	// logger is used to report server start/stop events.
	logger nodeEventLogger

	// tenantServerCreator instantiates tenant servers.
	tenantServerCreator tenantServerCreator

	// stopper is the parent stopper.
	stopper *stop.Stopper

	// st refers to the applicable cluster settings.
	st *cluster.Settings

	// testArgs is used when creating new tenant servers.
	testArgs map[roachpb.TenantName]base.TestSharedProcessTenantArgs

	// sendSQLRoutingError is a callback to use to report
	// a tenant routing error to the incoming client.
	sendSQLRoutingError func(ctx context.Context, conn net.Conn, tenantName roachpb.TenantName)

	mu struct {
		syncutil.Mutex

		// servers maps tenant names to the server for that tenant.
		//
		// TODO(knz): Detect when the mapping of name to tenant ID has
		// changed, and invalidate the entry.
		servers map[roachpb.TenantName]*serverEntry

		// nextServerIdx is the index to provide to the next call to
		// newServerFn.
		nextServerIdx int
	}
}

func newServerController(
	ctx context.Context,
	logger nodeEventLogger,
	parentNodeID *base.NodeIDContainer,
	parentStopper *stop.Stopper,
	st *cluster.Settings,
	tenantServerCreator tenantServerCreator,
	systemServer onDemandServer,
	systemTenantNameContainer *roachpb.TenantNameContainer,
	sendSQLRoutingError func(ctx context.Context, conn net.Conn, tenantName roachpb.TenantName),
) *serverController {
	c := &serverController{
		nodeID:              parentNodeID,
		logger:              logger,
		st:                  st,
		testArgs:            make(map[roachpb.TenantName]base.TestSharedProcessTenantArgs),
		stopper:             parentStopper,
		tenantServerCreator: tenantServerCreator,
		sendSQLRoutingError: sendSQLRoutingError,
	}

	// We make the serverState for the system mock the regular
	// lifecycle. It starts with an already-closed `startedOrStopped`
	// channel; and it properly reacts to a call to requestStop()
	// by closing its stopped channel -- albeit with no other side effects.
	closedChan := make(chan struct{})
	close(closedChan)
	closeCtx, cancelFn := context.WithCancel(context.Background())
	entry := &serverEntry{
		server:        systemServer,
		nameContainer: systemTenantNameContainer,
		state: serverState{
			startedOrStopped: closedChan,
			requestStop:      cancelFn,
			stopped:          closeCtx.Done(),
		},
	}
	entry.state.started.Set(true)

	c.mu.servers = map[roachpb.TenantName]*serverEntry{
		catconstants.SystemTenantName: entry,
	}
	parentStopper.AddCloser(c)
	return c
}

// DefaultTenantSelectSettingName is the name of the setting that
// configures the default tenant to use when a client does not specify
// a specific tenant.
var DefaultTenantSelectSettingName = "server.controller.default_tenant"

var defaultTenantSelect = settings.RegisterStringSetting(
	settings.SystemOnly,
	DefaultTenantSelectSettingName,
	"name of the tenant to use to serve requests when clients don't specify a tenant",
	catconstants.SystemTenantName,
).WithPublic()

// tenantServerWrapper implements the onDemandServer interface for SQLServerWrapper.
type tenantServerWrapper struct {
	stopper *stop.Stopper
	server  *SQLServerWrapper
}

var _ onDemandServer = (*tenantServerWrapper)(nil)

func (t *tenantServerWrapper) stop(ctx context.Context) {
	ctx = t.server.AnnotateCtx(ctx)
	t.stopper.Stop(ctx)
}

func (t *tenantServerWrapper) getHTTPHandlerFn() http.HandlerFunc {
	return t.server.http.baseHandler
}

func (t *tenantServerWrapper) getSQLAddr() string {
	return t.server.sqlServer.cfg.SQLAdvertiseAddr
}

func (t *tenantServerWrapper) getRPCAddr() string {
	return t.server.sqlServer.cfg.AdvertiseAddr
}

func (t *tenantServerWrapper) getTenantID() roachpb.TenantID {
	return t.server.sqlCfg.TenantID
}

func (s *tenantServerWrapper) getInstanceID() base.SQLInstanceID {
	return s.server.sqlServer.sqlIDContainer.SQLInstanceID()
}

func (s *tenantServerWrapper) shutdownRequested() <-chan ShutdownRequest {
	return s.server.sqlServer.ShutdownRequested()
}

// systemServerWrapper implements the onDemandServer interface for Server.
//
// (We can imagine a future where the SQL service for the system
// tenant is served using the same code path as any other secondary
// tenant, in which case systemServerWrapper can disappear and we use
// tenantServerWrapper everywhere, but we're not there yet.)
//
// We do not implement the onDemandServer interface methods on *Server
// directly so as to not add noise to its go documentation.
type systemServerWrapper struct {
	server *Server
}

var _ onDemandServer = (*systemServerWrapper)(nil)

func (s *systemServerWrapper) stop(ctx context.Context) {
	// No-op: the SQL service for the system tenant never shuts down.
}

func (t *systemServerWrapper) getHTTPHandlerFn() http.HandlerFunc {
	return t.server.http.baseHandler
}

func (s *systemServerWrapper) getSQLAddr() string {
	return s.server.sqlServer.cfg.SQLAdvertiseAddr
}

func (s *systemServerWrapper) getRPCAddr() string {
	return s.server.sqlServer.cfg.AdvertiseAddr
}

func (s *systemServerWrapper) getTenantID() roachpb.TenantID {
	return s.server.cfg.TenantID
}

func (s *systemServerWrapper) getInstanceID() base.SQLInstanceID {
	// In system tenant, node ID == instance ID.
	return base.SQLInstanceID(s.server.nodeIDContainer.Get())
}

func (s *systemServerWrapper) shutdownRequested() <-chan ShutdownRequest {
	return nil
}
