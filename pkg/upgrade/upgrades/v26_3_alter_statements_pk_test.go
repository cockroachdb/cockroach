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
	"github.com/cockroachdb/cockroach/pkg/obs/statementstore"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestAlterStatementsTablePKMigration verifies that V26_3_AlterStatementsTablePK
// migrates an existing system.statements table from PK(id) with a unique
// secondary index on fingerprint_id to PK(fingerprint_id) with the id column
// dropped. The pre-migration shape is established by injecting the legacy
// descriptor over the bootstrap-time descriptor.
func TestAlterStatementsTablePKMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderDeadlock(t, "slow under deadlock+race")
	skip.UnderRace(t, "slow under race")

	clusterversion.SkipWhenMinSupportedVersionIsAtLeast(t, clusterversion.V26_3_AlterStatementsTablePK)

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
	s := tc.Server(0)

	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	// First upgrade to the version that creates the table.
	upgrades.Upgrade(
		t, tc.ServerConn(0),
		clusterversion.V26_2_AddSystemStatementsTable,
		nil,   /* done */
		false, /* expectError */
	)

	// Replace the bootstrap-time descriptor with the legacy (pre-V26_3) shape so
	// the migration has something to migrate.
	upgrades.InjectLegacyTable(ctx, t, s, systemschema.StatementsTable,
		func() *descpb.TableDescriptor {
			return statementstore.GetOldStatementsDescriptor(descpb.InvalidID)
		})

	hasColumn := func(name string) bool {
		var exists bool
		sqlDB.QueryRow(t, `
			SELECT count(*) > 0
			FROM [SHOW COLUMNS FROM system.statements]
			WHERE column_name = $1
		`, name).Scan(&exists)
		return exists
	}
	hasIndex := func(name string) bool {
		var exists bool
		sqlDB.QueryRow(t, `
			SELECT count(*) > 0
			FROM [SHOW INDEXES FROM system.statements]
			WHERE index_name = $1
		`, name).Scan(&exists)
		return exists
	}
	primaryKeyColumns := func() []string {
		rows := sqlDB.QueryStr(t, `
			SELECT column_name
			FROM [SHOW INDEXES FROM system.statements]
			WHERE index_name = 'primary' AND storing = false
			ORDER BY seq_in_index
		`)
		out := make([]string, 0, len(rows))
		for _, r := range rows {
			out = append(out, r[0])
		}
		return out
	}

	// Pre-migration: id is the PK, redundant unique index exists.
	require.True(t, hasColumn("id"), "expected id column before migration")
	require.True(t, hasIndex("statements_fingerprint_id_key"),
		"expected statements_fingerprint_id_key index before migration")
	require.Equal(t, []string{"id"}, primaryKeyColumns(),
		"expected id to be the primary key before migration")

	// Run the upgrade.
	upgrades.Upgrade(
		t, tc.ServerConn(0),
		clusterversion.V26_3_AlterStatementsTablePK,
		nil,   /* done */
		false, /* expectError */
	)

	// Post-migration: id column gone, redundant unique index gone, PK on fingerprint_id.
	require.False(t, hasColumn("id"), "expected id column to be dropped after migration")
	require.False(t, hasIndex("statements_fingerprint_id_key"),
		"expected statements_fingerprint_id_key index to be dropped after migration")
	require.Equal(t, []string{"fingerprint_id"}, primaryKeyColumns(),
		"expected fingerprint_id to be the primary key after migration")

	// The fingerprint secondary index should still exist.
	require.True(t, hasIndex("statements_fingerprint_idx"),
		"expected statements_fingerprint_idx to still exist after migration")

	// Confirm the table is still writable using the production INSERT pattern
	// from statementstore (ON CONFLICT against fingerprint_id).
	sqlDB.Exec(t, `INSERT INTO system.statements
		(fingerprint_id, fingerprint, summary, db, metadata)
		VALUES (b'\x00\x00\x00\x00\x00\x00\x00\x02', 'SELECT 2', 'SELECT 2', 'defaultdb', '{}')
		ON CONFLICT (fingerprint_id) DO UPDATE SET last_upserted = now()`)
	sqlDB.Exec(t, `INSERT INTO system.statements
		(fingerprint_id, fingerprint, summary, db, metadata)
		VALUES (b'\x00\x00\x00\x00\x00\x00\x00\x02', 'SELECT 2', 'SELECT 2', 'defaultdb', '{}')
		ON CONFLICT (fingerprint_id) DO UPDATE SET last_upserted = now()`)
}
