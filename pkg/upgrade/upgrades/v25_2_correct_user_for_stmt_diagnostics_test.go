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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestStmtDiagAddUsernameMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterversion.SkipWhenMinSupportedVersionIsAtLeast(t, clusterversion.V25_2)

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
			`SELECT username FROM system.statement_diagnostics_requests LIMIT 0`,
			`SELECT username FROM system.statement_diagnostics_requests@completed_idx_v2 LIMIT 0`,
		}
		validationSchemas = []upgrades.Schema{
			{Name: "username", ValidationFn: upgrades.HasColumn},
			{Name: "primary", ValidationFn: upgrades.HasColumnFamily},
			{Name: "completed_idx_v2", ValidationFn: upgrades.HasIndex},
			{Name: "completed_idx", ValidationFn: upgrades.DoesNotHaveIndex},
		}
	)

	// Inject the old copy of the descriptor.
	upgrades.InjectLegacyTable(ctx, t, s, systemschema.StatementDiagnosticsRequestsTable,
		getOldStmtDiagReqsDescriptor)
	validateSchemaExists := func(expectExists bool) {
		upgrades.ValidateSchemaExists(
			ctx,
			t,
			s,
			sqlDB,
			keys.StatementDiagnosticsRequestsTableID,
			systemschema.StatementDiagnosticsRequestsTable,
			validationStmts,
			validationSchemas,
			expectExists,
		)
	}
	// Validate that the statement_diagnostics_requests table has the old
	// schema.
	validateSchemaExists(false)
	// Run the upgrade.
	upgrades.Upgrade(
		t,
		sqlDB,
		clusterversion.V25_2_AddUsernameToStmtDiagRequest,
		nil,   /* done */
		false, /* expectError */
	)
	// Validate that the table has new schema.
	validateSchemaExists(true)
}

// getOldStmtDiagReqsDescriptor returns the
// system.statement_diagnostics_requests table descriptor that was being used
// before adding the username column to the current version.
func getOldStmtDiagReqsDescriptor() *descpb.TableDescriptor {
	uniqueRowIDString := "unique_rowid()"
	falseBoolString := "false"

	return &descpb.TableDescriptor{
		Name:                    "statement_diagnostics_requests",
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
		},
		NextColumnID: 12,
		Families: []descpb.ColumnFamilyDescriptor{
			{
				Name:        "primary",
				ColumnNames: []string{"id", "completed", "statement_fingerprint", "statement_diagnostics_id", "requested_at", "min_execution_latency", "expires_at", "sampling_probability", "plan_gist", "anti_plan_gist", "redacted"},
				ColumnIDs:   []descpb.ColumnID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
			},
		},
		NextFamilyID: 1,
		PrimaryIndex: descpb.IndexDescriptor{
			Name:                tabledesc.PrimaryKeyIndexName("statement_diagnostics_requests"),
			ID:                  1,
			Unique:              true,
			KeyColumnNames:      []string{"id"},
			KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
			KeyColumnIDs:        []descpb.ColumnID{1},
			ConstraintID:        1,
		},
		Indexes: []descpb.IndexDescriptor{
			{
				Name:                "completed_idx",
				ID:                  2,
				Unique:              false,
				KeyColumnNames:      []string{"completed", "id"},
				StoreColumnNames:    []string{"statement_fingerprint", "min_execution_latency", "expires_at", "sampling_probability", "plan_gist", "anti_plan_gist", "redacted"},
				KeyColumnIDs:        []descpb.ColumnID{2, 1},
				KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC, catenumpb.IndexColumn_ASC},
				StoreColumnIDs:      []descpb.ColumnID{3, 6, 7, 8, 9, 10, 11},
				Version:             descpb.StrictIndexColumnIDGuaranteesVersion,
				ConstraintID:        2,
			},
		},
		NextIndexID:      4,
		Privileges:       catpb.NewCustomSuperuserPrivilegeDescriptor(privilege.ReadWriteData, username.NodeUserName()),
		NextMutationID:   1,
		FormatVersion:    3,
		NextConstraintID: 3,
	}
}
