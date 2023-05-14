// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package acl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

type lookupTenantFunc func(ctx context.Context, tenantID roachpb.TenantID) (*tenant.Tenant, error)

// PrivateEndpoints represents the controller used to manage ACL rules for
// private connections. A connection is assumed to be private if it includes
// the EndpointID field, which gets populated through the PROXY headers.
type PrivateEndpoints struct {
	LookupTenantFn lookupTenantFunc
}

var _ AccessController = &PrivateEndpoints{}

// CheckConnection implements the AccessController interface.
func (p *PrivateEndpoints) CheckConnection(ctx context.Context, conn ConnectionTags) error {
	tenantObj, err := p.LookupTenantFn(ctx, conn.TenantID)
	if err != nil {
		return err
	}

	// Public connections. This ACL is only responsible for private endpoints.
	if conn.EndpointID == "" {
		return nil
	}

	// Cluster allows private connections, so we'll check allowed endpoints.
	if (tenantObj.ConnectivityType & tenant.ALLOW_PRIVATE) != 0 {
		for _, endpoints := range tenantObj.PrivateEndpoints {
			// A matching endpointID was found.
			if endpoints == conn.EndpointID {
				return nil
			}
		}
	}

	// By default, connections are rejected if the cluster does not allow private
	// connections, or there are no endpoints defined.
	return errors.Newf(
		"connection to '%s' denied: cluster does not allow this private connection",
		conn.TenantID.String(),
	)
}
