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
// and its index are added to system.statement_diagnostics table, and the
// collections_remaining column is added to system.statement_diagnostics_requests
// during upgrade.
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

	// Inject old descriptors for both tables.
	upgrades.InjectLegacyTable(ctx, t, s, systemschema.StatementDiagnosticsTable,
		getOldStatementDiagnosticsDescriptor)
	upgrades.InjectLegacyTable(ctx, t, s, systemschema.StatementDiagnosticsRequestsTable,
		getOldStatementDiagnosticsRequestsDescriptor)

	// Validate statement_diagnostics table (request_id column and index).
	{
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

	// Validate statement_diagnostics_requests table (collections_remaining
	// column and updated completed_idx_v2 index).
	{
		validationStmts := []string{
			`SELECT collections_remaining FROM system.statement_diagnostics_requests LIMIT 0`,
		}
		validationSchemas := []upgrades.Schema{
			{Name: "collections_remaining", ValidationFn: upgrades.HasColumn},
			{Name: "completed_idx_v2", ValidationFn: upgrades.HasIndex},
		}

		upgrades.ValidateSchemaExists(
			ctx, t, s, sqlDB,
			keys.StatementDiagnosticsRequestsTableID,
			systemschema.StatementDiagnosticsRequestsTable,
			validationStmts, validationSchemas, true,
		)
	}
}

// getOldStatementDiagnosticsRequestsDescriptor returns the
// system.statement_diagnostics_requests table descriptor that was being used
// before adding the collections_remaining column.
func getOldStatementDiagnosticsRequestsDescriptor() *descpb.TableDescriptor {
	uniqueRowIDString := "unique_rowid()"
	falseBoolString := "false"
	emptyString := "'':::STRING"
	return &descpb.TableDescriptor{
		Name:                    string(catconstants.StatementDiagnosticsRequestsTableName),
		ID:                      keys.StatementDiagnosticsRequestsTableID,
		ParentID:                keys.SystemDatabaseID,
		UnexposedParentSchemaID: keys.PublicSchemaID,
		Version:                 1,
		Columns: []descpb.ColumnDescriptor{
			{Name: "id", ID: 1, Type: types.Int, DefaultExpr: &uniqueRowIDString, Nullable: false},
			{Name: "completed", ID: 2, Type: types.Bool, Nullable: false, DefaultExpr: &falseBoolString},
			{Name: "statement_fingerprint", ID: 3, Type: types.String, Nullable: false},
			{Name: "statement_diagnostics_id", ID: 4, Type: types.Int, Nullable: true},
			{Name: "requested_at", ID: 5, Type: types.TimestampTZ, Nullable: false},
			{Name: "min_execution_latency", ID: 6, Type: types.Interval, Nullable: true},
			{Name: "expires_at", ID: 7, Type: types.TimestampTZ, Nullable: true},
			{Name: "sampling_probability", ID: 8, Type: types.Float, Nullable: true},
			{Name: "plan_gist", ID: 9, Type: types.String, Nullable: true},
			{Name: "anti_plan_gist", ID: 10, Type: types.Bool, Nullable: true},
			{Name: "redacted", ID: 11, Type: types.Bool, Nullable: false, DefaultExpr: &falseBoolString},
			{Name: "username", ID: 12, Type: types.String, Nullable: false, DefaultExpr: &emptyString},
			// Note: collections_remaining column (ID 13) is intentionally omitted for the old descriptor
		},
		NextColumnID: 13,
		Families: []descpb.ColumnFamilyDescriptor{
			{
				Name: "primary",
				ID:   0,
				ColumnNames: []string{
					"id", "completed", "statement_fingerprint", "statement_diagnostics_id",
					"requested_at", "min_execution_latency", "expires_at", "sampling_probability",
					"plan_gist", "anti_plan_gist", "redacted", "username",
				},
				ColumnIDs: []descpb.ColumnID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
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
				Name:                "completed_idx_v2",
				ID:                  2,
				Unique:              false,
				KeyColumnNames:      []string{"completed", "id"},
				StoreColumnNames:    []string{"statement_fingerprint", "min_execution_latency", "expires_at", "sampling_probability", "plan_gist", "anti_plan_gist", "redacted", "username"},
				KeyColumnIDs:        []descpb.ColumnID{2, 1},
				KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC, catenumpb.IndexColumn_ASC},
				StoreColumnIDs:      []descpb.ColumnID{3, 6, 7, 8, 9, 10, 11, 12},
				Version:             descpb.StrictIndexColumnIDGuaranteesVersion,
			},
		},
		NextIndexID: 3,
		Checks: []*descpb.TableDescriptor_CheckConstraint{{
			Name:         "check_sampling_probability",
			Expr:         "sampling_probability BETWEEN 0.0:::FLOAT8 AND 1.0:::FLOAT8",
			ColumnIDs:    []descpb.ColumnID{8},
			ConstraintID: 2,
		}},
		Privileges:       catpb.NewCustomSuperuserPrivilegeDescriptor(privilege.ReadWriteData, username.NodeUserName()),
		NextMutationID:   1,
		FormatVersion:    3,
		NextConstraintID: 3,
	}
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
