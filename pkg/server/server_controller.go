// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitieswatcher"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverctl"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirecancel"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/startup"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil/singleflight"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
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
	draining atomic.Bool
	drainCh  chan struct{}

	// disableSQLServer disables starting the SQL service.
	disableSQLServer bool

	// orchestrator is the orchestration method to use.
	orchestrator *channelOrchestrator

	watcher *tenantcapabilitieswatcher.Watcher

	disableTLSForHTTP bool

	mu struct {
		syncutil.RWMutex

		// servers maps tenant names to the server for that tenant.
		//
		// TODO(knz): Detect when the mapping of name to tenant ID has
		// changed, and invalidate the entry.
		servers map[roachpb.TenantName]*serverState

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
	disableSQLServer bool,
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
		disableSQLServer:    disableSQLServer,
		disableTLSForHTTP:   disableTLSForHTTP,
	}
	c.orchestrator = newChannelOrchestrator(parentStopper, c)
	c.mu.servers = map[roachpb.TenantName]*serverState{
		catconstants.SystemTenantName: c.orchestrator.makeServerStateForSystemTenant(systemTenantNameContainer, systemServer),
	}
	c.mu.testArgs = make(map[roachpb.TenantName]base.TestSharedProcessTenantArgs)
	c.mu.newServerCh = make(chan struct{})
	parentStopper.AddCloser(c)
	return c
}

func (s *serverController) SetDraining() {
	if !s.draining.Load() {
		s.draining.Store(true)
		close(s.drainCh)
	}
}

// start monitors changes to the service mode and updates
// the running servers accordingly.
func (c *serverController) start(ctx context.Context, ie isql.Executor) error {
	// If the SQL server is disabled there are no initial secondary tenants to
	// start.
	if !c.disableSQLServer {
		// We perform one round of updates synchronously, to ensure that
		// any tenants already in service mode SHARED get a chance to boot
		// up before we signal readiness.
		if err := c.startInitialSecondaryTenantServers(ctx, ie); err != nil {
			return err
		}
	}

	// Run the detection of which servers should be started or stopped.
	return c.stopper.RunAsyncTask(ctx, "mark-tenant-services", func(ctx context.Context) {
		// We receieve updates from the tenantcapabilities
		// watcher, but we also refresh our state at a fixed
		// interval to account for:
		//
		//  - A rapid stop & start in which the start is
		//    initially ignored because the server is still
		//    stopping.
		//
		//  - Startup failures that we want to retry.
		const watchInterval = 1 * time.Second
		ctx, cancel := c.stopper.WithCancelOnQuiesce(ctx)
		defer cancel()

		var timer timeutil.Timer
		defer timer.Stop()

		for {
			allTenants, updateCh := c.watcher.GetAllTenants()
			if err := c.startMissingServers(ctx, allTenants); err != nil {
				log.Warningf(ctx, "cannot update running tenant services: %v", err)
			}

			timer.Reset(watchInterval)
			select {
			case <-updateCh:
			case <-timer.C:
				timer.Read = true
			case <-c.stopper.ShouldQuiesce():
				// Expedited server shutdown of outer server.
				return
			case <-c.drainCh:
				// The outer server has started a graceful drain: stop
				// picking up new servers.
				return
			}
		}
	})
}

// startInitialSecondaryTenantServers starts the servers for secondary tenants
// that should be started during server initialization.
func (c *serverController) startInitialSecondaryTenantServers(
	ctx context.Context, ie isql.Executor,
) error {
	// The list of tenants that should have a running server.
	reqTenants, err := startup.RunIdempotentWithRetryEx(ctx,
		c.stopper.ShouldQuiesce(),
		"get expected running tenants",
		func(ctx context.Context) ([]roachpb.TenantName, error) {
			return c.getExpectedRunningTenants(ctx, ie)
		})
	if err != nil {
		return err
	}
	for _, name := range reqTenants {
		if name == catconstants.SystemTenantName {
			// We already pre-initialize the entry for the system tenant.
			continue
		}
		if _, err := c.startAndWaitForRunningServer(ctx, name); err != nil {
			return err
		}
	}
	return nil
}

func (c *serverController) startMissingServers(
	ctx context.Context, tenants []tenantcapabilities.Entry,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, t := range tenants {
		if t.Name == "" {
			continue
		}

		if t.DataState != mtinfopb.DataStateReady {
			continue
		}

		if t.ServiceMode != mtinfopb.ServiceModeShared {
			continue
		}

		name := t.Name
		if _, ok := c.mu.servers[name]; !ok {
			log.Infof(ctx, "tenant %q has changed service mode, should now start", name)
			// Mark the server for async creation.
			if _, err := c.createServerEntryLocked(ctx, name); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *serverController) createServerEntryLocked(
	ctx context.Context, tenantName roachpb.TenantName,
) (*serverState, error) {
	if c.draining.Load() {
		return nil, errors.New("server is draining")
	}

	// finalizeFn is called when the server is fully stopped.
	// It is called from a different goroutine than the caller of
	// startControlledServer, and so needs to lock c.mu itself.
	finalizeFn := func(ctx context.Context, tenantName roachpb.TenantName) {
		c.mu.Lock()
		defer c.mu.Unlock()
		delete(c.mu.servers, tenantName)
	}
	// startErrorFn is called every time there is an error starting
	// the server.
	startErrorFn := func(ctx context.Context, tenantName roachpb.TenantName, err error) {
		c.logStartEvent(ctx, roachpb.TenantID{}, 0, tenantName, false /* success */, err)
	}
	// serverStartedFn is called when the server has started
	// successfully and is accepting clients.
	serverStartedFn := func(ctx context.Context, tenantName roachpb.TenantName, tid roachpb.TenantID, sid base.SQLInstanceID) {
		c.logStartEvent(ctx, tid, sid, tenantName, true /* success */, nil)
	}
	// serverStoppingFn is called when the server is shutting down
	// after a successful start.
	serverStoppingFn := func(ctx context.Context, tenantName roachpb.TenantName, tid roachpb.TenantID, sid base.SQLInstanceID) {
		c.logStopEvent(ctx, tid, sid, tenantName)
	}

	entry, err := c.orchestrator.startControlledServer(ctx, tenantName,
		finalizeFn, startErrorFn, serverStartedFn, serverStoppingFn)
	if err != nil {
		return nil, err
	}

	c.mu.servers[tenantName] = entry
	close(c.mu.newServerCh)
	c.mu.newServerCh = make(chan struct{})
	return entry, nil
}

// getExpectedRunningTenants retrieves the tenant IDs that should
// be running right now.
func (c *serverController) getExpectedRunningTenants(
	ctx context.Context, ie isql.Executor,
) (tenantNames []roachpb.TenantName, resErr error) {
	rowIter, err := ie.QueryIterator(ctx, "list-tenants", nil, /* txn */
		`SELECT name FROM system.tenants
WHERE service_mode = $1
  AND data_state = $2
  AND name IS NOT NULL
ORDER BY name`, mtinfopb.ServiceModeShared, mtinfopb.DataStateReady)
	if err != nil {
		return nil, err
	}
	defer func() { resErr = errors.CombineErrors(resErr, rowIter.Close()) }()

	var hasNext bool
	for hasNext, err = rowIter.Next(ctx); hasNext && err == nil; hasNext, err = rowIter.Next(ctx) {
		row := rowIter.Cur()
		tenantName := tree.MustBeDString(row[0])
		tenantNames = append(tenantNames, roachpb.TenantName(tenantName))
	}
	return tenantNames, err
}

// startAndWaitForRunningServer either waits for an existing server to
// have started already for the given tenant, or starts and wait for a
// new server.
func (c *serverController) startAndWaitForRunningServer(
	ctx context.Context, tenantName roachpb.TenantName,
) (onDemandServer, error) {
	entry, err := func() (*serverState, error) {
		c.mu.Lock()
		defer c.mu.Unlock()
		if entry, ok := c.mu.servers[tenantName]; ok {
			return entry, nil
		}
		return c.createServerEntryLocked(ctx, tenantName)
	}()
	if err != nil {
		return nil, err
	}

	select {
	case <-entry.startedOrStopped():
		s, isReady := entry.getServer()
		if isReady {
			return s.(onDemandServer), nil
		}
		return nil, entry.getLastStartupError()
	case <-c.stopper.ShouldQuiesce():
		return nil, errors.New("server stopping")
	case <-ctx.Done():
		return nil, errors.WithStack(ctx.Err())
	}
}

// newServerForOrchestrator implements the
// serverFactoryForOrchestration interface.
func (c *serverController) newServerForOrchestrator(
	ctx context.Context, nameContainer *roachpb.TenantNameContainer, tenantStopper *stop.Stopper,
) (orchestratedServer, error) {
	tenantName := nameContainer.Get()
	var testArgs base.TestSharedProcessTenantArgs
	func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		testArgs = c.mu.testArgs[tenantName]
	}()

	// Server does not exist yet: instantiate and start it.
	idx := func() int {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.mu.nextServerIdx++
		return c.mu.nextServerIdx
	}()
	return c.tenantServerCreator.newTenantServer(ctx, nameContainer, tenantStopper, idx, testArgs)
}

// Close implements the stop.Closer interface.
func (c *serverController) Close() {
	ctx := c.AnnotateCtx(context.Background())
	log.Infof(ctx, "server controller shutting down")
	entries := c.getAllEntries()
	// Request immediate shutdown. This is probably not needed; the
	// server should already be sensitive to the parent stopper
	// quiescing.
	for _, e := range entries {
		e.requestImmediateShutdown(ctx)
	}

	log.Infof(ctx, "waiting for tenant servers to report stopped")
	for _, e := range entries {
		<-e.stopped()
	}
}

func (c *serverController) drain(ctx context.Context) (stillRunning int) {
	entries := c.getAllEntries()
	// Request shutdown for all servers.
	for _, e := range entries {
		e.requestGracefulShutdown(ctx)
	}

	// How many entries are _not_ stopped yet?
	notStopped := 0
	for _, e := range entries {
		select {
		case <-e.stopped():
		default:
			log.Infof(ctx, "server for tenant %q still running", e.nameContainer())
			notStopped++
		}
	}
	return notStopped
}

func (c *serverController) getAllEntries() (res map[roachpb.TenantName]*serverState) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	res = make(map[roachpb.TenantName]*serverState, len(c.mu.servers))
	for name, e := range c.mu.servers {
		res[name] = e
	}
	return res
}

type nodeEventLogger interface {
	logStructuredEvent(ctx context.Context, event logpb.EventPayload)
}

func (c *serverController) logStartEvent(
	ctx context.Context,
	tid roachpb.TenantID,
	instanceID base.SQLInstanceID,
	tenantName roachpb.TenantName,
	success bool,
	opError error,
) {
	ev := &eventpb.TenantSharedServiceStart{OK: success}
	if opError != nil {
		ev.ErrorText = redact.Sprint(opError)
	}
	sharedDetails := &ev.CommonSharedServiceEventDetails
	sharedDetails.NodeID = int32(c.nodeID.Get())
	if tid.IsSet() {
		sharedDetails.TenantID = tid.ToUint64()
	}
	sharedDetails.InstanceID = int32(instanceID)
	sharedDetails.TenantName = string(tenantName)

	c.logger.logStructuredEvent(ctx, ev)
}

func (c *serverController) logStopEvent(
	ctx context.Context,
	tid roachpb.TenantID,
	instanceID base.SQLInstanceID,
	tenantName roachpb.TenantName,
) {
	ev := &eventpb.TenantSharedServiceStop{}
	sharedDetails := &ev.CommonSharedServiceEventDetails
	sharedDetails.NodeID = int32(c.nodeID.Get())
	if tid.IsSet() {
		sharedDetails.TenantID = tid.ToUint64()
	}
	sharedDetails.InstanceID = int32(instanceID)
	sharedDetails.TenantName = string(tenantName)

	c.logger.logStructuredEvent(ctx, ev)
}

// getServer retrieves a reference to the current server for the given
// tenant name. The returned channel is closed if a new tenant is
// added to the running tenants or if the requested tenant becomes
// available. It can be used by the caller to wait for the server to
// become available.
func (c *serverController) getServer(
	ctx context.Context, tenantName roachpb.TenantName,
) (onDemandServer, <-chan struct{}, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if e, ok := c.mu.servers[tenantName]; ok {
		if so, isReady := e.getServer(); isReady {
			return so.(onDemandServer), c.mu.newServerCh, nil
		} else {
			// If we have a server but it isn't ready yet,
			// return the startedOrStopped entry for the
			// channel for the caller to poll on.
			return nil, e.startedOrStopped(), errors.Mark(errors.Newf("server for tenant %q not ready", tenantName), errNoTenantServerRunning)
		}
	}
	return nil, c.mu.newServerCh, errors.Mark(errors.Newf("no server for tenant %q", tenantName), errNoTenantServerRunning)
}

type noTenantServerRunning struct{}

func (noTenantServerRunning) Error() string { return "no server for tenant" }

var errNoTenantServerRunning error = noTenantServerRunning{}

// getServers retrieves all the currently instantiated and running
// in-memory servers.
func (c *serverController) getServers() (res []onDemandServer) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, e := range c.mu.servers {
		so, isReady := e.getServer()
		if !isReady {
			continue
		}
		res = append(res, so.(onDemandServer))
	}
	return res
}

// getCurrentTenantNames returns the names of the tenants for which we
// already have a tenant server running.
func (c *serverController) getCurrentTenantNames() []roachpb.TenantName {
	var serverNames []roachpb.TenantName
	c.mu.RLock()
	defer c.mu.RUnlock()
	for name, e := range c.mu.servers {
		if _, isReady := e.getServer(); !isReady {
			continue
		}
		serverNames = append(serverNames, name)
	}
	return serverNames
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
