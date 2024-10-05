// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"net"
	"net/http"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitieswatcher"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverctl"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirecancel"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil/singleflight"
	"github.com/cockroachdb/redact"
)

// onDemandServer represents a server that can be started on demand.
type onDemandServer interface {
	orchestratedServer

	// getHTTPHandlerFn retrieves the function that can serve HTTP
	// requests for this server.
	getHTTPHandlerFn() http.HandlerFunc

	// handleCancel processes a SQL async cancel query.
	handleCancel(ctx context.Context, cancelKey pgwirecancel.BackendKeyData)

	// serveConn handles an incoming SQL connection.
	serveConn(ctx context.Context, conn net.Conn, status pgwire.PreServeStatus) error

	// getSQLAddr returns the SQL address for this server.
	getSQLAddr() string

	// getRPCAddr() returns the RPC address for this server.
	getRPCAddr() string
}

// serverController manages a fleet of multiple servers side-by-side.
// They are instantiated on demand the first time they are accessed.
// Instantiation can fail, e.g. if the target tenant doesn't exist or
// is not active.
type serverController struct {
	log.AmbientContext

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

	// sendSQLRoutingError is a callback to use to report
	// a tenant routing error to the incoming client.
	sendSQLRoutingError func(ctx context.Context, conn net.Conn, tenantName roachpb.TenantName)

	tenantWaiter *singleflight.Group

	// draining is set when the surrounding server starts draining, and
	// prevents further creation of new tenant servers.
	draining syncutil.AtomicBool
	drainCh  chan struct{}

	// orchestrator is the orchestration method to use.
	orchestrator serverOrchestrator

	watcher *tenantcapabilitieswatcher.Watcher

	disableTLSForHTTP bool

	mu struct {
		syncutil.RWMutex

		// servers maps tenant names to the server for that tenant.
		//
		// TODO(knz): Detect when the mapping of name to tenant ID has
		// changed, and invalidate the entry.
		servers map[roachpb.TenantName]serverState

		// testArgs is used when creating new tenant servers.
		testArgs map[roachpb.TenantName]base.TestSharedProcessTenantArgs

		// nextServerIdx is the index to provide to the next call to
		// newServerFn.
		nextServerIdx int

		// newServerCh is closed anytime a server is added to
		// the servers list.
		newServerCh chan struct{}
	}
}

func newServerController(
	ctx context.Context,
	ambientCtx log.AmbientContext,
	logger nodeEventLogger,
	parentNodeID *base.NodeIDContainer,
	parentStopper *stop.Stopper,
	st *cluster.Settings,
	tenantServerCreator tenantServerCreator,
	systemServer onDemandServer,
	systemTenantNameContainer *roachpb.TenantNameContainer,
	sendSQLRoutingError func(ctx context.Context, conn net.Conn, tenantName roachpb.TenantName),
	watcher *tenantcapabilitieswatcher.Watcher,
	disableTLSForHTTP bool,
) *serverController {
	c := &serverController{
		AmbientContext:      ambientCtx,
		nodeID:              parentNodeID,
		logger:              logger,
		st:                  st,
		stopper:             parentStopper,
		tenantServerCreator: tenantServerCreator,
		sendSQLRoutingError: sendSQLRoutingError,
		watcher:             watcher,
		tenantWaiter:        singleflight.NewGroup("tenant server poller", "poll"),
		drainCh:             make(chan struct{}),
		disableTLSForHTTP:   disableTLSForHTTP,
	}
	c.orchestrator = newServerOrchestrator(parentStopper, c)
	c.mu.servers = map[roachpb.TenantName]serverState{
		catconstants.SystemTenantName: c.orchestrator.makeServerStateForSystemTenant(systemTenantNameContainer, systemServer),
	}
	c.mu.testArgs = make(map[roachpb.TenantName]base.TestSharedProcessTenantArgs)
	c.mu.newServerCh = make(chan struct{})
	parentStopper.AddCloser(c)
	return c
}

func (s *serverController) SetDraining() {
	if !s.draining.Get() {
		s.draining.Set(true)
		close(s.drainCh)
	}
}

// tenantServerWrapper implements the onDemandServer interface for SQLServerWrapper.
type tenantServerWrapper struct {
	stopper *stop.Stopper
	server  *SQLServerWrapper
}

var _ onDemandServer = (*tenantServerWrapper)(nil)

func (t *tenantServerWrapper) annotateCtx(ctx context.Context) context.Context {
	return t.server.AnnotateCtx(ctx)
}

func (t *tenantServerWrapper) preStart(ctx context.Context) error {
	return t.server.PreStart(ctx)
}

func (t *tenantServerWrapper) acceptClients(ctx context.Context) error {
	if err := t.server.AcceptClients(ctx); err != nil {
		return err
	}
	// Show the tenant details in logs.
	// TODO(knz): Remove this once we can use a single listener.
	return t.server.reportTenantInfo(ctx)
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

func (s *tenantServerWrapper) shutdownRequested() <-chan serverctl.ShutdownRequest {
	return s.server.sqlServer.ShutdownRequested()
}

func (s *tenantServerWrapper) gracefulDrain(
	ctx context.Context, verbose bool,
) (uint64, redact.RedactableString, error) {
	ctx = s.server.AnnotateCtx(ctx)
	return s.server.Drain(ctx, verbose)
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
	server *topLevelServer
}

var _ onDemandServer = (*systemServerWrapper)(nil)

func (s *systemServerWrapper) annotateCtx(ctx context.Context) context.Context {
	return s.server.AnnotateCtx(ctx)
}

func (s *systemServerWrapper) preStart(ctx context.Context) error {
	// No-op: the SQL service for the system tenant is started elsewhere.
	return nil
}

func (s *systemServerWrapper) acceptClients(ctx context.Context) error {
	// No-op: the SQL service for the system tenant is started elsewhere.
	return nil
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

func (s *systemServerWrapper) shutdownRequested() <-chan serverctl.ShutdownRequest {
	return nil
}

func (s *systemServerWrapper) gracefulDrain(
	ctx context.Context, verbose bool,
) (uint64, redact.RedactableString, error) {
	// The controller is not responsible for draining the system tenant.
	return 0, "", nil
}
