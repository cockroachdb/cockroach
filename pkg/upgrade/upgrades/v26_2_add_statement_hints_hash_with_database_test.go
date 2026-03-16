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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/hints"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestStatementHintsHashWithDatabaseMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterversion.SkipWhenMinSupportedVersionIsAtLeast(t,
		clusterversion.V26_2_StatementHintsHashWithDatabaseColumn)

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
		s     = tc.Server(0)
		sqlDB = tc.ServerConn(0)
	)
	defer tc.Stopper().Stop(ctx)

	// Inject the descriptor without the hash_with_database column.
	upgrades.InjectLegacyTable(ctx, t, s, systemschema.StatementHintsTable,
		func() *descpb.TableDescriptor {
			return hints.GetStatementHintsDescriptorWithoutHashWithDatabase(descpb.InvalidID)
		})

	// Get the dynamic table ID for statement_hints after injection.
	var tableID descpb.ID
	err := sqlDB.QueryRow("SELECT 'system.statement_hints'::REGCLASS::OID").Scan(&tableID)
	require.NoError(t, err)

	validationStmts := []string{
		`SELECT hash_with_database FROM system.statement_hints LIMIT 0`,
	}
	validationSchemas := []upgrades.Schema{
		{Name: "hash_with_database", ValidationFn: upgrades.ColumnExists},
		{Name: "hash_with_database_idx", ValidationFn: upgrades.IndexExists},
	}

	validateNewSchemaExists := func(expectExists bool) {
		upgrades.ValidateSchemaExists(
			ctx, t, s, sqlDB, tableID,
			systemschema.StatementHintsTable,
			validationStmts, validationSchemas,
			expectExists,
		)
	}

	// Verify the hash_with_database column and index don't exist yet.
	validateNewSchemaExists(false)

	// Insert rows before the upgrade: one with database, one without.
	_, err = sqlDB.Exec(`
		INSERT INTO system.statement_hints (fingerprint, hint, hint_type)
		VALUES ('SELECT 1', 'hint1'::BYTES, 'rewrite_inline_hints')
	`)
	require.NoError(t, err)
	_, err = sqlDB.Exec(`
		INSERT INTO system.statement_hints (fingerprint, hint, hint_type, database)
		VALUES ('SELECT 1', 'hint2'::BYTES, 'rewrite_inline_hints', 'mydb')
	`)
	require.NoError(t, err)

	// Run the upgrade.
	upgrades.Upgrade(
		t, sqlDB,
		clusterversion.V26_2_StatementHintsHashWithDatabaseColumn,
		nil,   /* done */
		false, /* expectError */
	)

	// Verify the column and index now exist.
	validateNewSchemaExists(true)

	// Verify hash_with_database values are correct.
	var hashNullDB, hashWithDB int64
	err = sqlDB.QueryRow(`
		SELECT hash_with_database FROM system.statement_hints
		WHERE fingerprint = 'SELECT 1' AND database IS NULL
	`).Scan(&hashNullDB)
	require.NoError(t, err)

	err = sqlDB.QueryRow(`
		SELECT hash_with_database FROM system.statement_hints
		WHERE fingerprint = 'SELECT 1' AND database = 'mydb'
	`).Scan(&hashWithDB)
	require.NoError(t, err)

	// The two hash_with_database values should differ since one has database='mydb'.
	require.NotEqual(t, hashNullDB, hashWithDB,
		"hash_with_database should differ when database differs")

	// Verify that hash_with_database with NULL database matches the old hash column.
	var oldHash int64
	err = sqlDB.QueryRow(`
		SELECT hash FROM system.statement_hints
		WHERE fingerprint = 'SELECT 1' AND database IS NULL
	`).Scan(&oldHash)
	require.NoError(t, err)
	require.Equal(t, oldHash, hashNullDB,
		"hash_with_database with NULL database should equal hash (fnv64 skips NULL args)")
}
