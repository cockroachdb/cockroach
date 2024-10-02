// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenantdirsvr

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/gogo/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

// TestSimpleDirectoryServer is a directory server that returns a single
// pre-defined address for all tenants. It is expected that a SQL pod is
// listening on that address.
//
// The metadata of such tenants will not have a clusterName returned, so
// validation of cluster names through the directory cache will be skipped.
type TestSimpleDirectoryServer struct {
	// podAddr refers to the address of the SQL pod, which consists of both the
	// host and port (e.g. "127.0.0.1:26257").
	podAddr string

	mu struct {
		syncutil.Mutex

		// deleted indicates that the tenant has been deleted. A NotFound
		// error will be returned when trying to resume a SQL pod, or read the
		// tenant's metadata.
		deleted map[roachpb.TenantID]struct{}
	}
}

var _ tenant.DirectoryServer = &TestSimpleDirectoryServer{}

// NewTestSimpleDirectoryServer constructs a new simple directory server.
func NewTestSimpleDirectoryServer(podAddr string) (*TestSimpleDirectoryServer, *grpc.Server) {
	dir := &TestSimpleDirectoryServer{podAddr: podAddr}
	dir.mu.deleted = make(map[roachpb.TenantID]struct{})
	grpcServer := grpc.NewServer()
	tenant.RegisterDirectoryServer(grpcServer, dir)
	return dir, grpcServer
}

// ListPods returns a list with a single RUNNING pod. The address of the pod
// will be the same regardless of tenant ID. If the tenant has been deleted, no
// pods will be returned.
//
// ListPods implements the tenant.DirectoryServer interface.
func (d *TestSimpleDirectoryServer) ListPods(
	ctx context.Context, req *tenant.ListPodsRequest,
) (*tenant.ListPodsResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.mu.deleted[roachpb.MustMakeTenantID(req.TenantID)]; ok {
		return &tenant.ListPodsResponse{}, nil
	}
	return &tenant.ListPodsResponse{
		Pods: []*tenant.Pod{
			{
				TenantID:       req.TenantID,
				Addr:           d.podAddr,
				State:          tenant.RUNNING,
				StateTimestamp: timeutil.Now(),
			},
		},
	}, nil
}

// WatchPods is a no-op for the simple directory.
//
// WatchPods implements the tenant.DirectoryServer interface.
func (d *TestSimpleDirectoryServer) WatchPods(
	req *tenant.WatchPodsRequest, server tenant.Directory_WatchPodsServer,
) error {
	// Insted of returning right away, we block until context is done.
	// This prevents the proxy server from constantly trying to establish
	// a watch in test environments, causing spammy logs.
	<-server.Context().Done()
	return nil
}

// EnsurePod is a no-op for the simple directory since it assumes that a SQL
// pod is actively listening at the associated pod address. However, if the
// tenant has been deleted, a GRPC NotFound error will be returned. This would
// mimic the behavior that we have in the actual tenant directory.
//
// EnsurePod implements the tenant.DirectoryServer interface.
func (d *TestSimpleDirectoryServer) EnsurePod(
	ctx context.Context, req *tenant.EnsurePodRequest,
) (*tenant.EnsurePodResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.mu.deleted[roachpb.MustMakeTenantID(req.TenantID)]; ok {
		return nil, status.Errorf(codes.NotFound, "tenant has been deleted")
	}
	return &tenant.EnsurePodResponse{}, nil
}

// GetTenant returns an empty response regardless of tenants. However, if the
// tenant has been deleted, a GRPC NotFound error will be returned.
//
// GetTenant implements the tenant.DirectoryServer interface.
func (d *TestSimpleDirectoryServer) GetTenant(
	ctx context.Context, req *tenant.GetTenantRequest,
) (*tenant.GetTenantResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.mu.deleted[roachpb.MustMakeTenantID(req.TenantID)]; ok {
		return nil, status.Errorf(codes.NotFound, "tenant has been deleted")
	}
	return &tenant.GetTenantResponse{
		Tenant: &tenant.Tenant{
			TenantID: req.TenantID,
			// Note that we do not return a ClusterName field here. Doing this
			// skips the clusterName validation in the directory cache which
			// makes testing easier.
			//
			// If we hardcoded a cluster name here, all connection strings will
			// need to be updated to use that cluster name, including the one
			// used by the ORM tests, which is currently hardcoded to
			// "prancing-pony": https://github.com/cockroachdb/cockroach-go/blob/e1659d1d/testserver/tenant.go#L244
			ClusterName:       "",
			AllowedCIDRRanges: []string{"0.0.0.0/0"},
		},
	}, nil
}

// WatchTenants is a no-op for the simple directory.
//
// WatchTenants implements the tenant.DirectoryServer interface.
func (d *TestSimpleDirectoryServer) WatchTenants(
	req *tenant.WatchTenantsRequest, server tenant.Directory_WatchTenantsServer,
) error {
	// Insted of returning right away, we block until context is done.
	// This prevents the proxy server from constantly trying to establish
	// a watch in test environments, causing spammy logs.
	<-server.Context().Done()
	return nil
}

// DeleteTenant marks the given tenant as deleted, so that a NotFound error
// will be returned for certain directory server endpoints. This also changes
// the behavior of ListPods so no pods are returned.
func (d *TestSimpleDirectoryServer) DeleteTenant(tenantID roachpb.TenantID) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.mu.deleted[tenantID] = struct{}{}
}
