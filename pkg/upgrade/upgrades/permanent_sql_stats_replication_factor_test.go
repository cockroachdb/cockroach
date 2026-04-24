// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSQLStatsReplicationFactorBootstrap verifies that newly-bootstrapped
// clusters and tenants pick up num_replicas = 3 on the SQL stats tables via
// the permanent bootstrap step.
func TestSQLStatsReplicationFactorBootstrap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
		},
	})
	defer tc.Stopper().Stop(ctx)
	systemDB := tc.ServerConn(0)
	defer systemDB.Close()

	ts, err := tc.Server(0).TenantController().StartTenant(ctx, base.TestTenantArgs{
		TenantID:   serverutils.TestTenantID(),
		TenantName: "tenant",
	})
	require.NoError(t, err)
	tenantDB := ts.SQLConn(t)
	defer tenantDB.Close()

	tables := []string{
		"system.public.statement_statistics",
		"system.public.transaction_statistics",
	}
	testCases := []struct {
		name  string
		dbCon *gosql.DB
	}{
		{"system", systemDB},
		{"tenant", tenantDB},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			for _, table := range tables {
				var target, rawConfigSQL string
				row := tc.dbCon.QueryRow(fmt.Sprintf("SHOW ZONE CONFIGURATION FROM TABLE %s", table))
				require.NoError(t, row.Scan(&target, &rawConfigSQL))
				assert.Equal(t, fmt.Sprintf("TABLE %s", table), target)
				assert.Contains(t, rawConfigSQL, "num_replicas = 3")
			}
		})
	}
}

// TestSQLStatsReplicationFactorUpgrade verifies that the version-gated upgrade
// applies num_replicas = 3 on existing clusters that pre-date the change.
func TestSQLStatsReplicationFactorUpgrade(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterversion.SkipWhenMinSupportedVersionIsAtLeast(t, clusterversion.V26_3_ReduceSQLStatsReplicationFactor)

	skip.UnderRace(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					ClusterVersionOverride:         clusterversion.MinSupported.Version(),
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)
	sqlDB := tc.ServerConn(0)
	defer sqlDB.Close()

	tables := []string{
		"system.public.statement_statistics",
		"system.public.transaction_statistics",
	}

	upgrades.Upgrade(t, sqlDB, clusterversion.V26_3_ReduceSQLStatsReplicationFactor, nil, false)

	for _, table := range tables {
		var target, rawConfigSQL string
		row := sqlDB.QueryRow(fmt.Sprintf("SHOW ZONE CONFIGURATION FROM TABLE %s", table))
		require.NoError(t, row.Scan(&target, &rawConfigSQL))
		assert.Equal(t, fmt.Sprintf("TABLE %s", table), target)
		assert.Contains(t, rawConfigSQL, "num_replicas = 3")
	}
}
