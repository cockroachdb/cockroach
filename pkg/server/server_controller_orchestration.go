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
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// serverState coordinates the lifecycle of a tenant server. It ensures
// sane concurrent behavior between:
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
type serverState struct {
	// startedOrStopped is closed when the server has either started or
	// stopped. This can be used to wait for a server start.
	startedOrStopped <-chan struct{}

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
	stopped <-chan struct{}
}

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
		const watchInterval = time.Second
		ctx, cancel := c.stopper.WithCancelOnQuiesce(ctx)
		defer cancel()
		for {
			select {
			case <-time.After(watchInterval):
			case <-c.stopper.ShouldQuiesce():
				return
			}

			if err := c.scanTenantsForRunnableServices(ctx, ie); err != nil {
				log.Warningf(ctx, "cannot update running tenant services: %v", err)
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
	reqTenants, err := c.getExpectedRunningTenants(ctx, ie)
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

// scanTenantsForRunnableServices checks which tenants need to be
// started/stopped and queues the necessary server lifecycle changes.
func (c *serverController) scanTenantsForRunnableServices(
	ctx context.Context, ie isql.Executor,
) error {
	// The list of tenants that should have a running server.
	reqTenants, err := c.getExpectedRunningTenants(ctx, ie)
	if err != nil {
		return err
	}

	// Create a lookup map for the first loop below.
	nameLookup := make(map[roachpb.TenantName]struct{}, len(reqTenants))
	for _, name := range reqTenants {
		nameLookup[name] = struct{}{}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// First check if there are any servers that shouldn't be running
	// right now.
	for name, srv := range c.mu.servers {
		if _, ok := nameLookup[name]; !ok {
			log.Infof(ctx, "tenant %q has changed service mode, should now stop", name)
			// Mark the server for async shutdown.
			srv.state.requestStop()
		}
	}

	// Now add all the missing servers.
	for _, name := range reqTenants {
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
) (*serverEntry, error) {
	entry, err := c.startControlledServer(ctx, tenantName)
	if err != nil {
		return nil, err
	}
	c.mu.servers[tenantName] = entry
	return entry, nil
}

// startControlledServer starts the orchestration task that starts,
// then shuts down, the server for the given tenant.
func (c *serverController) startControlledServer(
	ctx context.Context, tenantName roachpb.TenantName,
) (*serverEntry, error) {
	var stoppedChClosed syncutil.AtomicBool
	stopRequestCh := make(chan struct{})
	stoppedCh := make(chan struct{})
	startedOrStoppedCh := make(chan struct{})
	entry := &serverEntry{
		nameContainer: roachpb.NewTenantNameContainer(tenantName),
		state: serverState{
			startedOrStopped: startedOrStoppedCh,
			requestStop: func() {
				if !stoppedChClosed.Swap(true) {
					close(stopRequestCh)
				}
			},
			stopped: stoppedCh,
		},
	}
	if err := c.stopper.RunAsyncTask(ctx, "managed-tenant-server", func(ctx context.Context) {
		startedOrStoppedChAlreadyClosed := false
		defer func() {
			entry.state.started.Set(false)
			close(stoppedCh)
			if !startedOrStoppedChAlreadyClosed {
				entry.state.startErr = errors.New("server stop before start")
				close(startedOrStoppedCh)
			}

			// Remove the entry from the server map.
			c.mu.Lock()
			defer c.mu.Unlock()
			delete(c.mu.servers, tenantName)
		}()

		retryOpts := retry.Options{
			Closer: c.stopper.ShouldQuiesce(),
		}

		var s onDemandServer
		for retry := retry.StartWithCtx(ctx, retryOpts); retry.Next(); {
			select {
			case <-stopRequestCh:
				entry.state.startErr = errors.Newf("tenant %q: stop requested before start succeeded", tenantName)
				log.Infof(ctx, "%v", entry.state.startErr)
				return
			default:
			}
			var err error
			s, err = c.startServerInternal(ctx, entry.nameContainer)
			if err != nil {
				c.logStartEvent(ctx, roachpb.TenantID{}, 0,
					entry.nameContainer.Get(), false /* success */, err)
				log.Warningf(ctx,
					"unable to start server for tenant %q (attempt %d, will retry): %v",
					tenantName, retry.CurrentAttempt(), err)
				entry.state.startErr = err
				continue
			}
			break
		}
		if s == nil {
			// Retry loop exited before the server could start. This
			// indicates that there was an async request to abandon the
			// server startup. This is OK; just terminate early. The defer
			// will take care of cleaning up.
			return
		}

		tid, iid := s.getTenantID(), s.getInstanceID()
		c.logStartEvent(ctx, tid, iid, tenantName, true /* success */, nil)

		// Indicate the server has started.
		entry.server = s
		startedOrStoppedChAlreadyClosed = true
		entry.state.started.Set(true)
		close(startedOrStoppedCh)

		// Wait for a request to shut down.
		select {
		case <-c.stopper.ShouldQuiesce():
		case <-stopRequestCh:
		case shutdownRequest := <-s.shutdownRequested():
			log.Infof(ctx, "tenant %q requesting their own shutdown: %v",
				tenantName, shutdownRequest.ShutdownCause())
		}
		log.Infof(ctx, "stop requested for tenant %q", tenantName)

		// Stop the server.
		// We use context.Background() here so that the process of
		// stopping the tenant does not get cancelled when shutting
		// down the outer server.
		s.stop(context.Background())
		c.logStopEvent(ctx, tid, iid, tenantName)

		// The defer on the return path will take care of the rest.
	}); err != nil {
		return nil, err
	}

	return entry, nil
}

// getExpectedRunningTenants retrieves the tenant IDs that should
// be running right now.
// TODO(knz): Use a watcher here.
// Probably as followup to https://github.com/cockroachdb/cockroach/pull/95657.
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
	entry, err := func() (*serverEntry, error) {
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
	case <-entry.state.startedOrStopped:
		return entry.server, entry.state.startErr
	case <-c.stopper.ShouldQuiesce():
		return nil, errors.New("server stopping")
	case <-ctx.Done():
		return nil, errors.WithStack(ctx.Err())
	}
}

func (c *serverController) startServerInternal(
	ctx context.Context, nameContainer *roachpb.TenantNameContainer,
) (onDemandServer, error) {
	tenantName := nameContainer.Get()
	testArgs := c.testArgs[tenantName]

	// Server does not exist yet: instantiate and start it.
	idx := func() int {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.mu.nextServerIdx++
		return c.mu.nextServerIdx
	}()
	return c.tenantServerCreator.newTenantServer(ctx, nameContainer, idx, testArgs)
}

// Close implements the stop.Closer interface.
func (c *serverController) Close() {
	entries := func() (res []*serverEntry) {
		c.mu.Lock()
		defer c.mu.Unlock()
		res = make([]*serverEntry, 0, len(c.mu.servers))
		for _, e := range c.mu.servers {
			res = append(res, e)
		}
		return res
	}()

	// Request shutdown for all servers.
	for _, e := range entries {
		e.state.requestStop()
	}

	// Wait for shutdown for all servers.
	for _, e := range entries {
		<-e.state.stopped
	}
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
