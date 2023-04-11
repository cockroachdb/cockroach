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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

// getServer retrieves a reference to the current server for the given
// tenant name.
func (c *serverController) getServer(
	ctx context.Context, tenantName roachpb.TenantName,
) (onDemandServer, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.mu.servers[tenantName]; ok {
		if so, isReady := e.getServer(); isReady {
			return so.(onDemandServer), nil
		}
	}
	return nil, errors.Mark(errors.Newf("no server for tenant %q", tenantName), errNoTenantServerRunning)
}

type noTenantServerRunning struct{}

func (noTenantServerRunning) Error() string { return "no server for tenant" }

var errNoTenantServerRunning error = noTenantServerRunning{}

// getServers retrieves all the currently instantiated and running
// in-memory servers.
func (c *serverController) getServers() (res []onDemandServer) {
	c.mu.Lock()
	defer c.mu.Unlock()
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
	c.mu.Lock()
	defer c.mu.Unlock()
	for name, e := range c.mu.servers {
		if _, isReady := e.getServer(); !isReady {
			continue
		}
		serverNames = append(serverNames, name)
	}
	return serverNames
}
