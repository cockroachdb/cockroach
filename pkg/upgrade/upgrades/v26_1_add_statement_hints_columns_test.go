// Copyright 2025 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/hintpb"
	"github.com/cockroachdb/cockroach/pkg/sql/hints"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestStatementHintsColumnsMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterversion.SkipWhenMinSupportedVersionIsAtLeast(t,
		clusterversion.V26_1_StatementHintsTypeNameEnabledColumnsAdded)

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

	// The statement_hints table already exists at MinSupported (V25_4).
	_, err := sqlDB.Exec(`SELECT 1 FROM system.statement_hints LIMIT 0`)
	require.NoError(t, err, "system.statement_hints table should exist at MinSupported (V25_4)")

	var (
		validationStmts = []string{
			`SELECT hint_type FROM system.statement_hints LIMIT 0`,
			`SELECT hint_name FROM system.statement_hints LIMIT 0`,
			`SELECT enabled FROM system.statement_hints LIMIT 0`,
			`SELECT where_expr FROM system.statement_hints LIMIT 0`,
		}
		validationSchemas = []upgrades.Schema{
			{Name: "hint_type", ValidationFn: upgrades.HasColumn},
			{Name: "hint_name", ValidationFn: upgrades.HasColumn},
			{Name: "enabled", ValidationFn: upgrades.HasColumn},
			{Name: "where_expr", ValidationFn: upgrades.HasColumn},
		}
	)

	// Inject the old copy of the descriptor (before the new columns were added).
	// Use descpb.InvalidID since InjectLegacyTable will set the ID.
	upgrades.InjectLegacyTable(ctx, t, s, systemschema.StatementHintsTable,
		func() *descpb.TableDescriptor { return hints.GetOldStatementHintsDescriptor(descpb.InvalidID) })

	// Get the dynamic table ID for statement_hints after injection.
	var tableID descpb.ID
	err = sqlDB.QueryRow("SELECT 'system.statement_hints'::REGCLASS::OID").Scan(&tableID)
	require.NoError(t, err, "failed to get statement_hints table ID")

	validateSchemaExists := func(expectExists bool) {
		upgrades.ValidateSchemaExists(
			ctx,
			t,
			s,
			sqlDB,
			tableID,
			systemschema.StatementHintsTable,
			validationStmts,
			validationSchemas,
			expectExists,
		)
	}

	// Verify that the statement_hints table has the old schema (no new columns).
	validateSchemaExists(false)

	// Insert a row with the old schema (before new columns exist).
	_, err = sqlDB.Exec(`
		INSERT INTO system.statement_hints (fingerprint, hint)
		VALUES ('test_fingerprint', 'test_hint'::BYTES)
	`)
	require.NoError(t, err, "should be able to insert row with old schema")

	// Run the upgrade to add the new columns.
	upgrades.Upgrade(
		t,
		sqlDB,
		clusterversion.V26_1_StatementHintsTypeNameEnabledColumnsAdded,
		nil,   /* done */
		false, /* expectError */
	)

	// Verify that the table now has the new schema (with the new columns).
	validateSchemaExists(true)

	// Verify that the old row has NULL for hint_type (before backfill) and
	// enabled=true.
	var hintType *string
	var enabled bool
	err = sqlDB.QueryRow(`
		SELECT hint_type, enabled FROM system.statement_hints WHERE fingerprint = 'test_fingerprint'
	`).Scan(&hintType, &enabled)
	require.NoError(t, err)
	require.Nil(t, hintType, "hint_type should be NULL before backfill")
	require.True(t, enabled, "enabled should be true (from default)")

	// Run the backfill migration.
	upgrades.Upgrade(
		t,
		sqlDB,
		clusterversion.V26_1_StatementHintsTypeColumnBackfilled,
		nil,   /* done */
		false, /* expectError */
	)

	// Verify that the old row now has the backfilled hint_type.
	err = sqlDB.QueryRow(`
		SELECT hint_type FROM system.statement_hints WHERE fingerprint = 'test_fingerprint'
	`).Scan(&hintType)
	require.NoError(t, err)
	require.NotNil(t, hintType, "hint_type should not be NULL after backfill")
	require.Equal(t, hintpb.HintTypeRewriteInlineHints, *hintType, "hint_type should be backfilled")

	// Test that we can insert a new row with explicit hint_type.
	_, err = sqlDB.Exec(`
		INSERT INTO system.statement_hints
		(fingerprint, hint, hint_type, hint_name, enabled, where_expr)
		VALUES ('test_fingerprint_2', 'test_hint'::BYTES, 'optimization', 'test_hint_name', true, 'current_database() = ''mydb''')
	`)
	require.NoError(t, err, "should be able to insert row with new columns")
}
