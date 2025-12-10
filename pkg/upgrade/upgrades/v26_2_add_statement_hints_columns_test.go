// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades_test

import (
	"context"
	"fmt"
	"testing"
	"time"

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
		clusterversion.V26_2_StatementHintsTypeNameEnabledColumnsAdded)

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
			`SELECT database FROM system.statement_hints LIMIT 0`,
		}
		validationSchemas = []upgrades.Schema{
			{Name: "hint_type", ValidationFn: upgrades.HasColumn},
			{Name: "hint_name", ValidationFn: upgrades.HasColumn},
			{Name: "enabled", ValidationFn: upgrades.HasColumn},
			{Name: "database", ValidationFn: upgrades.HasColumn},
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

	validateNewSchemaExists := func(expectExists bool) {
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
	validateNewSchemaExists(false)

	// Insert a row with the old schema (before new columns exist).
	_, err = sqlDB.Exec(`
		INSERT INTO system.statement_hints (fingerprint, hint)
		VALUES ('test_fingerprint', 'test_hint'::BYTES)
	`)
	require.NoError(t, err, "should be able to insert row with old schema")

	// Helper to validate the columns after both migrations.
	validateColumns := func() {
		// Verify that the old row has hint_type and enabled set correctly.
		var hintType string
		var enabled bool
		err := sqlDB.QueryRow(`
			SELECT hint_type, enabled FROM system.statement_hints WHERE fingerprint = 'test_fingerprint'
		`).Scan(&hintType, &enabled)
		require.NoError(t, err)
		require.Equal(t, hintpb.HintTypeRewriteInlineHints, hintType, "hint_type should be set")
		require.True(t, enabled, "enabled should be true (from default)")

		// Verify we can insert a new row with explicit hint_type.
		_, err = sqlDB.Exec(`
			INSERT INTO system.statement_hints
			(fingerprint, hint, hint_type, hint_name, enabled, database)
			VALUES ($1, 'test_hint'::BYTES, 'optimization', 'test_hint_name', true, 'mydb')
		`, fmt.Sprintf("test_fp_%d", time.Now().UnixNano()))
		require.NoError(t, err, "should be able to insert row with explicit hint_type")
	}

	// Run the upgrade to add the new columns.
	upgrades.Upgrade(
		t,
		sqlDB,
		clusterversion.V26_2_StatementHintsTypeNameEnabledColumnsAdded,
		nil,   /* done */
		false, /* expectError */
	)
	validateColumns()

	// Run the migration to remove the default value from hint_type.
	upgrades.Upgrade(
		t,
		sqlDB,
		clusterversion.V26_2_StatementHintsTypeColumnBackfilled,
		nil,   /* done */
		false, /* expectError */
	)
	validateColumns()

	// Verify that the table now has the final schema (after default removed).
	validateNewSchemaExists(true)

	// Verify that new rows without explicit hint_type will fail (NOT NULL
	// constraint remains, only default was removed).
	_, err = sqlDB.Exec(`
		INSERT INTO system.statement_hints (fingerprint, hint)
		VALUES ('test_fingerprint_no_type', 'test_hint'::BYTES)
	`)
	require.Error(t, err, "should fail to insert without hint_type after default removed")
}
