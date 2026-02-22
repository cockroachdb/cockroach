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
)

// TestStmtDiagnosticsRequestIDMigration verifies that the request_id column
// and its index are added to system.statement_diagnostics table during upgrade.
func TestStmtDiagnosticsRequestIDMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterversion.SkipWhenMinSupportedVersionIsAtLeast(t, clusterversion.V26_2_StmtDiagnosticsRequestID)

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

	// Inject old descriptor for the statement_diagnostics table.
	upgrades.InjectLegacyTable(ctx, t, s, systemschema.StatementDiagnosticsTable,
		getOldStatementDiagnosticsDescriptor)

	// Validate statement_diagnostics table (request_id column and index).
	validationStmts := []string{
		`SELECT request_id FROM system.statement_diagnostics LIMIT 0`,
	}
	validationSchemas := []upgrades.Schema{
		{Name: "request_id", ValidationFn: upgrades.HasColumn},
		{Name: "request_id_idx", ValidationFn: upgrades.HasIndex},
	}

	validateSchemaExists := func(expectExists bool) {
		upgrades.ValidateSchemaExists(
			ctx, t, s, sqlDB,
			keys.StatementDiagnosticsTableID,
			systemschema.StatementDiagnosticsTable,
			validationStmts, validationSchemas, expectExists,
		)
	}
	validateSchemaExists(false)

	// Run the upgrade.
	upgrades.Upgrade(
		t, sqlDB,
		clusterversion.V26_2_StmtDiagnosticsRequestID,
		nil,   /* done */
		false, /* expectError */
	)

	validateSchemaExists(true)
}

// getOldStatementDiagnosticsDescriptor returns the system.statement_diagnostics
// table descriptor that was being used before adding the request_id column.
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
			{Name: "transaction_diagnostics_id", ID: 8, Type: types.Int, Nullable: true},
			// Note: request_id column (ID 9) is intentionally omitted for the old descriptor
		},
		NextColumnID: 9,
		Families: []descpb.ColumnFamilyDescriptor{
			{
				Name: "primary",
				ID:   0,
				ColumnNames: []string{
					"id", "statement_fingerprint", "statement", "collected_at",
					"trace", "bundle_chunks", "error", "transaction_diagnostics_id",
					// Note: request_id is intentionally omitted for the old descriptor
				},
				ColumnIDs: []descpb.ColumnID{1, 2, 3, 4, 5, 6, 7, 8},
			},
		},
		NextFamilyID: 1,
		PrimaryIndex: descpb.IndexDescriptor{
			Name:                "primary",
			ID:                  1,
			Unique:              true,
			KeyColumnNames:      []string{"id"},
			KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
			KeyColumnIDs:        []descpb.ColumnID{1},
			ConstraintID:        1,
		},
		Indexes: []descpb.IndexDescriptor{
			{
				Name:                "transaction_diagnostics_id_idx",
				ID:                  2,
				Unique:              false,
				KeyColumnNames:      []string{"transaction_diagnostics_id"},
				KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
				KeyColumnIDs:        []descpb.ColumnID{8},
				KeySuffixColumnIDs:  []descpb.ColumnID{1},
				Version:             descpb.StrictIndexColumnIDGuaranteesVersion,
			},
			// Note: request_id_idx is intentionally omitted for the old descriptor
		},
		NextIndexID:      3,
		Privileges:       catpb.NewCustomSuperuserPrivilegeDescriptor(privilege.ReadWriteData, username.NodeUserName()),
		NextMutationID:   1,
		FormatVersion:    3,
		NextConstraintID: 2,
	}
}
