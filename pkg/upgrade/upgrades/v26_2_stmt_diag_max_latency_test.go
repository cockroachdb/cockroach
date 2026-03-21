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
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestStmtDiagnosticsMaxLatencyMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderDeadlock(t, "slow under deadlock+race")
	skip.UnderRace(t, "slow under race")

	clusterversion.SkipWhenMinSupportedVersionIsAtLeast(t, clusterversion.V26_2_StmtDiagnosticsMaxLatency)

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

	sqlDB := tc.ServerConn(0)
	defer sqlDB.Close()

	validateSchemaExists := func(shouldExist bool) {
		var colExists bool
		err := sqlDB.QueryRow(`
			SELECT count(*) > 0
			FROM [SHOW COLUMNS FROM system.statement_diagnostics_requests]
			WHERE column_name = 'max_execution_latency'
		`).Scan(&colExists)
		require.NoError(t, err)
		require.Equal(t, shouldExist, colExists)
	}

	// Verify column doesn't exist before migration.
	validateSchemaExists(false)

	// Run the upgrade.
	upgrades.Upgrade(
		t, sqlDB,
		clusterversion.V26_2_StmtDiagnosticsMaxLatency,
		nil,   /* done */
		false, /* expectError */
	)

	// Verify column exists after migration.
	validateSchemaExists(true)
}
