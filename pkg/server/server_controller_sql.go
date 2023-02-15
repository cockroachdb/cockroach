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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirecancel"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
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
		errCh := make(chan error, len(servers))
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
			if err := c.stopper.RunAsyncTask(ctx, "cancel", func(ctx context.Context) {
				errCh <- s.server.handleCancel(ctx, status.CancelKey)
			}); err != nil {
				return err
			}
		}
		// Wait for the cancellation to be processed.
		return c.stopper.RunAsyncTask(ctx, "wait-cancel", func(ctx context.Context) {
			var err error
			sawSuccess := false
			for i := 0; i < len(servers); i++ {
				select {
				case thisErr := <-errCh:
					err = errors.CombineErrors(err, thisErr)
					sawSuccess = sawSuccess || thisErr == nil
				case <-c.stopper.ShouldQuiesce():
					return
				}
			}
			if !sawSuccess {
				// We don't want to log a warning if cancellation has succeeded.
				_, rateLimited := errors.If(err, func(err error) (interface{}, bool) {
					if respStatus := grpcstatus.Convert(err); respStatus.Code() == codes.ResourceExhausted {
						return nil, true
					}
					return nil, false
				})
				if rateLimited {
					log.Sessions.Warningf(ctx, "unexpected while handling pgwire cancellation request: %v", err)
				}
			}
		})

	case pgwire.PreServeReady:
		tenantName := roachpb.TenantName(status.GetTenantName())
		if tenantName == "" {
			tenantName = roachpb.TenantName(defaultTenantSelect.Get(&c.st.SV))
		}

		s, err := c.getServer(ctx, tenantName)
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

func (t *systemServerWrapper) handleCancel(
	ctx context.Context, cancelKey pgwirecancel.BackendKeyData,
) error {
	pgCtx := t.server.sqlServer.AnnotateCtx(context.Background())
	pgCtx = logtags.AddTags(pgCtx, logtags.FromContext(ctx))
	return t.server.sqlServer.pgServer.HandleCancel(pgCtx, cancelKey)
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
) error {
	pgCtx := t.server.sqlServer.AnnotateCtx(context.Background())
	pgCtx = logtags.AddTags(pgCtx, logtags.FromContext(ctx))
	return t.server.sqlServer.pgServer.HandleCancel(pgCtx, cancelKey)
}

func (t *tenantServerWrapper) serveConn(
	ctx context.Context, conn net.Conn, status pgwire.PreServeStatus,
) error {
	pgCtx := t.server.sqlServer.AnnotateCtx(context.Background())
	pgCtx = logtags.AddTags(pgCtx, logtags.FromContext(ctx))
	return t.server.sqlServer.pgServer.ServeConn(pgCtx, conn, status)
}
