package migrationcluster

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
)

// TenantCluster is an implementation of migration.Cluster that doesn't do
// anything to track the set of nodes in the cluster. In the fullness of time
// a secondary tenant cluster may care about providing a true barrier between
// code versions. As of writing, there is only a single pod running and it
// is assumed to be of the appropriate version.
type TenantCluster struct {
	db *kv.DB
}

// NewTenantCluster returns a new TenantCluster.
func NewTenantCluster(db *kv.DB) *TenantCluster {
	return &TenantCluster{db: db}
}

// DB is part of the migration.Cluster interface.
func (t *TenantCluster) DB() *kv.DB {
	return t.db
}

// ForEveryNode is part of the migration.Cluster interface.
func (t *TenantCluster) ForEveryNode(
	ctx context.Context, op string, fn func(context.Context, serverpb.MigrationClient) error,
) error {
	return nil
}

// UntilClusterStable is part of the migration.Cluster interface.
func (t TenantCluster) UntilClusterStable(ctx context.Context, fn func() error) error {
	return nil
}

// IterateRangeDescriptors is part of the migration.Cluster interface.
func (t TenantCluster) IterateRangeDescriptors(
	ctx context.Context, size int, init func(), f func(descriptors ...roachpb.RangeDescriptor) error,
) error {
	return nil
}
