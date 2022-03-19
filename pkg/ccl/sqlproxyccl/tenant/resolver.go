// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package tenant

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// Resolver is an interface for the tenant directory.
//
// TODO(jaylim-crl): Rename this to Directory, and the current tenant.Directory
// to tenant.directory.
type Resolver interface {
	// EnsureTenantAddr returns an IP address of one of the given tenant's SQL
	// processes based on the tenantID and clusterName fields. This should block
	// until the process associated with the IP is ready.
	//
	// If no matching pods are found (e.g. cluster name mismatch, or tenant was
	// deleted), this will return a GRPC NotFound error.
	EnsureTenantAddr(
		ctx context.Context,
		tenantID roachpb.TenantID,
		clusterName string,
	) (string, error)

	// LookupTenantAddrs returns the IP addresses for all available SQL
	// processes for the given tenant. It returns a GRPC NotFound error if the
	// tenant does not exist.
	//
	// Unlike EnsureTenantAddr which blocks until there is an associated
	// process, LookupTenantAddrs will just return an empty set if no processes
	// are available for the tenant.
	LookupTenantAddrs(ctx context.Context, tenantID roachpb.TenantID) ([]string, error)

	// ReportFailure is used to indicate to the resolver that a connection
	// attempt to connect to a particular SQL tenant pod have failed.
	ReportFailure(ctx context.Context, tenantID roachpb.TenantID, addr string) error
}
