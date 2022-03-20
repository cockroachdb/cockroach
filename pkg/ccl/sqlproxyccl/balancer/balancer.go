// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package balancer

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// Balancer handles load balancing of SQL connections within the proxy.
type Balancer struct {
	// directory corresponds to the tenant directory that stores information
	// about SQL pods for each tenant.
	directory tenant.Directory
}

// NewBalancer constructs a new Balancer instance that is responsible for
// load balancing SQL connections within the proxy.
func NewBalancer(directory tenant.Directory) *Balancer {
	return &Balancer{directory: directory}
}

// ChoosePodAddr returns the IP address of one of this tenant's available pods.
// This applies a weighted load balancing algorithm when selecting a pod from a
// list. If the tenant is suspended (i.e. list is empty initially), and no pods
// are available, this will trigger resumption of the tenant, and return the IP
// address of the new pod. If the tenant cannot be resumed, this may return a
// GRPC FailedPrecondition error.
//
// If clusterName is non-empty, then a GRPC NotFound error is returned if no
// pods match the cluster's name. This can be used to ensure that the
// incoming SQL connection "knows" some additional information about the
// tenant, such as the name of the cluster, before being allowed to connect.
// Similarly, if the tenant does not exist (e.g. because it was deleted),
// this returns a GRPC NotFound error.
//
// Note that resuming a tenant requires directory server calls, so this can
// block for some time, until the resumption process is complete.
//
// TODO(jaylim-crl): Remap GRPC NotFound and FailedPrecondition errors to a
// concrete error type. These GRPC errors should be internal to the service
// directory.
func (b *Balancer) ChoosePodAddr(
	ctx context.Context, tenantID roachpb.TenantID, clusterName string,
) (string, error) {
	// TODO(jaylim-crl): We currently choose the pod's address within
	// selectTenantPod, which is called by EnsureTenantAddr. That logic has to
	// be extracted out into the balancer. EnsureTenantAddr should only block
	// until there is at least 1 pod, so the balancer would first call
	// EnsureTenantAddr, followed by LookupTenantAddrs to retrieve a list of
	// SQL pods. Finally, the balancer would apply the weighted load balancing
	// algorithm on the list of pods. The tenant directory should just be
	// responsible for reporting a list of SQL pods, and allowing callers to
	// resume SQL pods (e.g. EnsureTenantAddr).
	return b.directory.EnsureTenantAddr(ctx, tenantID, clusterName)
}

// ListPodAddrs returns the IP addresses of all available SQL pods for the given
// tenant. This returns a GRPC NotFound error if the tenant does not exist (e.g.
// it has not yet been created) or if it has not yet been fetched into the
// directory's cache (ListPodAddrs will never attempt to fetch it). Unlike
// ChoosePodAddr, if no SQL pods are available for the tenant, ListPodAddrs
// will return the empty set.
//
// TODO(jaylim-crl): Remap GRPC NotFound to a concrete error type. The GRPC
// NotFound error should be internal to the directory. This GRPC error should be
// internal to the service directory.
func (b *Balancer) ListPodAddrs(ctx context.Context, tenantID roachpb.TenantID) ([]string, error) {
	return b.directory.LookupTenantAddrs(ctx, tenantID)
}

// ReportFailure reports a connection failure to the balancer. This will inform
// the balancer that the process at addr is unhealthy, so that it is less likely
// to be chosen. It will also refresh the directory's internal cache.
func (b *Balancer) ReportFailure(
	ctx context.Context, tenantID roachpb.TenantID, addr string,
) error {
	return b.directory.ReportFailure(ctx, tenantID, addr)
}
