// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestTenantResourceGroupsTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterversion.SkipWhenMinSupportedVersionIsAtLeast(t, clusterversion.V26_3_AddTenantResourceGroupsTable)

	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					ClusterVersionOverride:         clusterversion.MinSupported.Version(),
				},
			},
		},
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, clusterArgs)
	defer tc.Stopper().Stop(ctx)
	s, sqlDB := tc.Server(0), tc.ServerConn(0)

	require.True(t, s.ExecutorConfig().(sql.ExecutorConfig).Codec.ForSystemTenant())

	_, err := sqlDB.Exec("SELECT * FROM system.tenant_resource_groups")
	require.Error(t, err)

	upgrades.Upgrade(t, sqlDB, clusterversion.V26_3_AddTenantResourceGroupsTable, nil, false)

	_, err = sqlDB.Exec(
		"SELECT tenant_id, id, name, config FROM system.tenant_resource_groups")
	require.NoError(t, err)

	// A tenant may write any of its resource groups, including ids in the
	// reserved (< 16) range that built-in groups will eventually occupy. The
	// host-side table is a passive aggregation; the source table's CHECK is
	// what gates the reserved range, not this one.
	_, err = sqlDB.Exec(
		`INSERT INTO system.tenant_resource_groups (tenant_id, id, name, config)
		 VALUES (1, 1, 'default', b'')`)
	require.NoError(t, err)
	_, err = sqlDB.Exec(
		`INSERT INTO system.tenant_resource_groups (tenant_id, id, name, config)
		 VALUES (1, 17, 'high', b'')`)
	require.NoError(t, err)

	// (tenant_id, id) is the primary key — duplicates are rejected.
	_, err = sqlDB.Exec(
		`INSERT INTO system.tenant_resource_groups (tenant_id, id, name, config)
		 VALUES (1, 17, 'duplicate', b'')`)
	require.Error(t, err)

	// Names are tenant-local: different tenants may reuse the same name.
	_, err = sqlDB.Exec(
		`INSERT INTO system.tenant_resource_groups (tenant_id, id, name, config)
		 VALUES (2, 17, 'high', b'')`)
	require.NoError(t, err)

	// And the same tenant may also reuse a name across ids: no UNIQUE on name.
	// (Tenants are expected to enforce per-tenant name uniqueness in
	// system.resource_groups, but the host table doesn't.)
	_, err = sqlDB.Exec(
		`INSERT INTO system.tenant_resource_groups (tenant_id, id, name, config)
		 VALUES (1, 18, 'high', b'')`)
	require.NoError(t, err)

	var n int
	require.NoError(t,
		sqlDB.QueryRow(
			`SELECT count(*) FROM system.tenant_resource_groups WHERE tenant_id = 1`).
			Scan(&n))
	require.Equal(t, 3, n)

	// The migration must also have created the singleton reconciliation
	// job at the static job ID, since this is a versioned upgrade and
	// existing tenants would otherwise miss it.
	var jobCount int
	require.NoError(t,
		sqlDB.QueryRow(
			`SELECT count(*) FROM system.jobs WHERE id = $1`,
			int64(jobs.ResourceGroupReconciliationJobID)).Scan(&jobCount))
	require.Equal(t, 1, jobCount)
}
