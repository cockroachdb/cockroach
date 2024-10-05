// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/startup"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// start monitors changes to the service mode and updates
// the running servers accordingly.
func (c *serverController) start(ctx context.Context, ie isql.Executor) error {
	// We perform one round of updates synchronously, to ensure that
	// any tenants already in service mode SHARED get a chance to boot
	// up before we signal readiness.
	if err := c.startInitialSecondaryTenantServers(ctx, ie); err != nil {
		return err
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

		timer := timeutil.NewTimer()
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
) (serverState, error) {
	if c.draining.Get() {
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
	if !c.st.Version.IsActive(ctx, clusterversion.V23_1TenantNamesStateAndServiceMode) {
		// Cluster not yet upgraded - we know there is no secondary tenant
		// with a name yet.
		return []roachpb.TenantName{catconstants.SystemTenantName}, nil
	}

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
	entry, err := func() (serverState, error) {
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

func (c *serverController) getAllEntries() (res map[roachpb.TenantName]serverState) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	res = make(map[roachpb.TenantName]serverState, len(c.mu.servers))
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
