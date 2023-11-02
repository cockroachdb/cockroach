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
	"net"
	"time"

	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirecancel"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

// sqlMux redirects incoming SQL connections to the server selected
// by the client-provided SQL parameters.
// If no tenant is specifeid, the default tenant is used.
func (c *serverController) sqlMux(
	ctx context.Context, conn net.Conn, status pgwire.PreServeStatus,
) error {
	switch status.State {
	case pgwire.PreServeCancel:
		// Cancel requests do not contain enough data for routing; we
		// simply broadcast them to all servers. One of the servers will
		// pick it up.
		servers := c.getServers()
		for i := range servers {
			s := servers[i]
			// We dispatch the request concurrently to all the servers.
			//
			// This concurrency is needed for UX. If there is more than 1
			// server, even if one server accepts the cancel request, at
			// least another will fail the cancel request and wait. If we
			// dispatch sequentially, and the server that fails is called
			// before the one that succeeds in the sequence, the client
			// would need to wait that extra delay to see their query
			// effectively cancelled. We don't want this extra wait for UX.
			//
			// The concurrent dispatch gives a chance to the succeeding
			// servers to see and process the cancel at approximately the
			// same time as every other.
			//
			// Cancel requests are unauthenticated so run the cancel async to prevent
			// the client from deriving any info about the cancel based on how long it
			// takes.
			if err := c.stopper.RunAsyncTask(ctx, "cancel", func(ctx context.Context) {
				s.handleCancel(ctx, status.CancelKey)
			}); err != nil {
				return err
			}
		}
		return nil

	case pgwire.PreServeReady:
		tenantName := roachpb.TenantName(status.GetTenantName())
		if tenantName == "" {
			tenantName = roachpb.TenantName(multitenant.DefaultTenantSelect.Get(&c.st.SV))
		}

		s, err := c.getServer(ctx, tenantName)
		if err != nil && c.shouldWaitForTenantServer(tenantName) {
			s, err = c.waitForTenantServer(ctx, tenantName)
		}
		if err != nil {
			log.Warningf(ctx, "unable to find server for tenant %q: %v", tenantName, err)
			c.sendSQLRoutingError(ctx, conn, tenantName)
			return err
		}

		return s.serveConn(ctx, conn, status)

	default:
		return errors.AssertionFailedf("programming error: missing case %v", status.State)
	}
}

// shouldWaitForTenantServer returns true if the serverController
// should wait for the tenant to become active when routing a
// connection.
func (c *serverController) shouldWaitForTenantServer(name roachpb.TenantName) bool {
	// We never wait for the system tenant because if it isn't
	// available, something is very wrong.
	if name == catconstants.SystemTenantName {
		return false
	}

	// For now, we only ever wait on connections to the default
	// tenant. The default tenant name is validated at the time it
	// is set. While it is possible that this was deleted before,
	// that would have to happen from an authorized user.
	//
	// TODO(ssd): We can remove this restriction once we plumb the
	// tenant watcher into the server controller. That will allow
	// us to query the known set of tenants without hitting the
	// DB.
	if name != roachpb.TenantName(multitenant.DefaultTenantSelect.Get(&c.st.SV)) {
		return false
	}

	return multitenant.WaitForClusterStart.Get(&c.st.SV)
}

func (c *serverController) waitForTenantServer(
	ctx context.Context, name roachpb.TenantName,
) (onDemandServer, error) {
	// TODO(ssd): This polling will happen for every inbound
	// connection that happens during tenant startup. Ideally, we
	// could avoid this polling by having the tenant controller
	// hand us a channel that it will close when the tenant
	// becomes available. Further, it might be nice to just have 1
	// thing doing the waiting so if we get a flood of new
	// connections for the same tenant we have have 1 waiter per
	// tenant.
	maxWait := multitenant.WaitForClusterStartTimeout.Get(&c.st.SV)
	retryCfg := retry.Options{
		InitialBackoff: 50 * time.Millisecond,
		MaxBackoff:     1 * time.Second,
	}

	ctx, cancel := context.WithTimeout(ctx, maxWait)
	defer cancel()
	var (
		err error
		s   onDemandServer
	)

	for i := retry.StartWithCtx(ctx, retryCfg); i.Next(); {
		s, err = c.getServer(ctx, name)
		if err == nil {
			break
		}
	}
	return s, err
}

func (t *systemServerWrapper) handleCancel(
	ctx context.Context, cancelKey pgwirecancel.BackendKeyData,
) {
	pgCtx := t.server.sqlServer.AnnotateCtx(context.Background())
	pgCtx = logtags.AddTags(pgCtx, logtags.FromContext(ctx))
	t.server.sqlServer.pgServer.HandleCancel(pgCtx, cancelKey)
}

func (t *systemServerWrapper) serveConn(
	ctx context.Context, conn net.Conn, status pgwire.PreServeStatus,
) error {
	pgCtx := t.server.sqlServer.AnnotateCtx(context.Background())
	pgCtx = logtags.AddTags(pgCtx, logtags.FromContext(ctx))
	return t.server.sqlServer.pgServer.ServeConn(pgCtx, conn, status)
}

func (t *tenantServerWrapper) handleCancel(
	ctx context.Context, cancelKey pgwirecancel.BackendKeyData,
) {
	pgCtx := t.server.sqlServer.AnnotateCtx(context.Background())
	pgCtx = logtags.AddTags(pgCtx, logtags.FromContext(ctx))
	t.server.sqlServer.pgServer.HandleCancel(pgCtx, cancelKey)
}

func (t *tenantServerWrapper) serveConn(
	ctx context.Context, conn net.Conn, status pgwire.PreServeStatus,
) error {
	pgCtx := t.server.sqlServer.AnnotateCtx(context.Background())
	pgCtx = logtags.AddTags(pgCtx, logtags.FromContext(ctx))
	return t.server.sqlServer.pgServer.ServeConn(pgCtx, conn, status)
}
