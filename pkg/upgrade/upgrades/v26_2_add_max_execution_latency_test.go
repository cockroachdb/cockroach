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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestAddMaxExecutionLatencyToStmtDiag(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterversion.SkipWhenMinSupportedVersionIsAtLeast(t,
		clusterversion.V26_2_AddMaxExecutionLatencyToStmtDiag)

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

	var (
		ctx   = context.Background()
		tc    = testcluster.StartTestCluster(t, 1, clusterArgs)
		sqlDB = tc.ServerConn(0)
	)
	defer tc.Stopper().Stop(ctx)

	// Verify the column does not exist before upgrade.
	_, err := sqlDB.Exec(`SELECT max_execution_latency FROM system.statement_diagnostics_requests LIMIT 0`)
	require.Error(t, err, "max_execution_latency should not exist before upgrade")

	_, err = sqlDB.Exec(`SELECT max_execution_latency FROM system.transaction_diagnostics_requests LIMIT 0`)
	require.Error(t, err, "max_execution_latency should not exist before upgrade")

	// Run the upgrade.
	upgrades.Upgrade(
		t,
		sqlDB,
		clusterversion.V26_2_AddMaxExecutionLatencyToStmtDiag,
		nil,   /* done */
		false, /* expectError */
	)

	// Verify the column now exists on both tables.
	_, err = sqlDB.Exec(`SELECT max_execution_latency FROM system.statement_diagnostics_requests LIMIT 0`)
	require.NoError(t, err, "max_execution_latency should exist after upgrade on statement_diagnostics_requests")

	_, err = sqlDB.Exec(`SELECT max_execution_latency FROM system.transaction_diagnostics_requests LIMIT 0`)
	require.NoError(t, err, "max_execution_latency should exist after upgrade on transaction_diagnostics_requests")

	// Verify we can insert a row with the new column.
	_, err = sqlDB.Exec(`
		INSERT INTO system.statement_diagnostics_requests
			(id, completed, statement_fingerprint, requested_at)
		VALUES (99999, false, 'SELECT 1', now())
	`)
	require.NoError(t, err)

	// Verify max_execution_latency defaults to NULL.
	var isNull bool
	err = sqlDB.QueryRow(`
		SELECT max_execution_latency IS NULL
		FROM system.statement_diagnostics_requests
		WHERE id = 99999
	`).Scan(&isNull)
	require.NoError(t, err)
	require.True(t, isNull, "max_execution_latency should default to NULL")

	// Verify the upgrade is idempotent.
	upgrades.Upgrade(
		t,
		sqlDB,
		clusterversion.V26_2_AddMaxExecutionLatencyToStmtDiag,
		nil,   /* done */
		false, /* expectError */
	)

	// Verify the table schemas match the expected final schema.
	_ = systemschema.StatementDiagnosticsRequestsTable
	_ = systemschema.TransactionDiagnosticsRequestsTable
}
