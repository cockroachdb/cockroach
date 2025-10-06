// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"net"

	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirecancel"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil/singleflight"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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

		s, _, err := c.getServer(ctx, tenantName)
		if err != nil && c.shouldWaitForTenantServer(tenantName) {
			s, err = c.waitForTenantServer(ctx, tenantName)
		}
		if err != nil {
			c.sendSQLRoutingError(ctx, conn, tenantName)
			// Avoid logging this error since it could
			// just be the result of user error (the wrong
			// tenant name) or an expected failure (such
			// as while the server is draining).
			//
			// TODO(ssd): In the draining case the node
			// should know we are draining and just stop
			// accepting SQL clients even when waiting for
			// application tenants to stop.
			if errors.Is(err, errNoTenantServerRunning) {
				return nil
			}
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

	return multitenant.WaitForClusterStartTimeout.Get(&c.st.SV) > 0
}

func (c *serverController) waitForTenantServer(
	ctx context.Context, name roachpb.TenantName,
) (onDemandServer, error) {
	// Note that requests that come in after the first request may time out
	// in less time than the WaitForClusterStartTimeout. This seems fine for
	// now since cluster startup should be relatively quick and if it isn't,
	// waiting longer isn't going to help.
	opts := singleflight.DoOpts{Stop: c.stopper, InheritCancelation: false}
	futureRes, _ := c.tenantWaiter.DoChan(ctx, string(name), opts, func(ctx context.Context) (interface{}, error) {
		var t timeutil.Timer
		defer t.Stop()
		t.Reset(multitenant.WaitForClusterStartTimeout.Get(&c.st.SV))
		for {
			s, waitCh, err := c.getServer(ctx, name)
			if err == nil {
				return s, nil
			}
			log.Infof(ctx, "waiting for server for %s to become available", name)
			select {
			case <-waitCh:
			case <-t.C:
				t.Read = true
				log.Infof(ctx, "timed out waiting for server for %s to become available", name)
				return nil, err
			}
		}
	})
	res := futureRes.WaitForResult(ctx)
	if res.Err != nil {
		return nil, res.Err
	}
	return res.Val.(onDemandServer), nil
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
