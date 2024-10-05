// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

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
