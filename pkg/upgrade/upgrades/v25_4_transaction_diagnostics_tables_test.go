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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestTransactionDiagnosticsTableMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterversion.SkipWhenMinSupportedVersionIsAtLeast(t, clusterversion.V25_4)

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

	var (
		validationStmts = []string{
			`SELECT "transaction_diagnostics_id" FROM system.statement_diagnostics@transaction_diagnostics_id_idx LIMIT 0`,
		}
		validationSchemas = []upgrades.Schema{
			{Name: "transaction_diagnostics_id", ValidationFn: upgrades.HasColumn},
			{Name: "transaction_diagnostics_id_idx", ValidationFn: upgrades.HasIndex},
		}
	)

	_, err := sqlDB.Exec("SELECT 1 FROM system.transaction_diagnostics_requests LIMIT 0")
	require.Error(t, err, "system.transaction_diagnostics_requests table should not exist")
	_, err = sqlDB.Exec("SELECT 1 FROM system.transaction_diagnostics LIMIT 0")
	require.Error(t, err, "system.transaction_diagnostics table should not exist")
	// Inject the old copy of the descriptor.
	upgrades.InjectLegacyTable(ctx, t, s, systemschema.StatementDiagnosticsTable,
		getOldStatementDiagnosticsDescriptor)
	validateSchemaExists := func(expectExists bool) {
		upgrades.ValidateSchemaExists(
			ctx,
			t,
			s,
			sqlDB,
			keys.StatementDiagnosticsTableID,
			systemschema.StatementDiagnosticsTable,
			validationStmts,
			validationSchemas,
			expectExists,
		)
	}
	// Validate that the eventlog table has the old
	// schema.
	validateSchemaExists(false)
	// Run the upgrade.
	upgrades.Upgrade(
		t,
		sqlDB,
		clusterversion.V25_4_TransactionDiagnosticsSupport,
		nil,   /* done */
		false, /* expectError */
	)
	_, err = sqlDB.Exec(`SELECT 1 FROM system.transaction_diagnostics_requests LIMIT 0`)
	require.NoError(t, err, "system.transaction_diagnostics_requests table should exist")
	_, err = sqlDB.Exec(`SELECT 1 FROM system.transaction_diagnostics LIMIT 0`)
	require.NoError(t, err, "system.transaction_diagnostics table should exist")
	// Validate that the table has new schema.
	validateSchemaExists(true)
}

func getOldStatementDiagnosticsDescriptor() *descpb.TableDescriptor {
	uniqueRowIDString := "unique_rowid()"
	return &descpb.TableDescriptor{
		Name:                    string(catconstants.StatementDiagnosticsTableName),
		ID:                      keys.StatementDiagnosticsTableID,
		ParentID:                keys.SystemDatabaseID,
		UnexposedParentSchemaID: keys.PublicSchemaID,
		Version:                 1,
		Columns: []descpb.ColumnDescriptor{
			{Name: "id", ID: 1, Type: types.Int, DefaultExpr: &uniqueRowIDString, Nullable: false},
			{Name: "statement_fingerprint", ID: 2, Type: types.String, Nullable: false},
			{Name: "statement", ID: 3, Type: types.String, Nullable: false},
			{Name: "collected_at", ID: 4, Type: types.TimestampTZ, Nullable: false},
			{Name: "trace", ID: 5, Type: types.Jsonb, Nullable: true},
			{Name: "bundle_chunks", ID: 6, Type: types.IntArray, Nullable: true},
			{Name: "error", ID: 7, Type: types.String, Nullable: true},
		},
		NextColumnID: 8,
		Families: []descpb.ColumnFamilyDescriptor{
			{
				Name: "primary",
				ColumnNames: []string{"id", "statement_fingerprint", "statement",
					"collected_at", "trace", "bundle_chunks", "error"},
				ColumnIDs: []descpb.ColumnID{1, 2, 3, 4, 5, 6, 7},
			},
		},
		NextFamilyID: 2,
		PrimaryIndex: descpb.IndexDescriptor{
			Name:                "primary",
			ID:                  1,
			Unique:              true,
			KeyColumnNames:      []string{"id"},
			KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
			KeyColumnIDs:        []descpb.ColumnID{1},
			ConstraintID:        1,
		},
		NextIndexID:      2,
		Privileges:       catpb.NewCustomSuperuserPrivilegeDescriptor(privilege.ReadWriteData, username.NodeUserName()),
		NextMutationID:   1,
		FormatVersion:    3,
		NextConstraintID: 3,
	}
}
