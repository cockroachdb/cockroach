// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migrations_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/migration/migrations"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestAlterSystemStmtDiagReqs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride:          clusterversion.ByKey(clusterversion.AlterSystemStmtDiagReqs - 1),
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
			`SELECT min_execution_latency, expires_at FROM system.statement_diagnostics_requests LIMIT 0`,
			`SELECT min_execution_latency, expires_at FROM system.statement_diagnostics_requests@completed_idx_v2 LIMIT 0`,
		}
		validationSchemas = []migrations.Schema{
			{Name: "min_execution_latency", ValidationFn: migrations.HasColumn},
			{Name: "expires_at", ValidationFn: migrations.HasColumn},
		}
	)

	// Inject the old copy of the descriptor.
	migrations.InjectLegacyTable(ctx, t, s, systemschema.StatementDiagnosticsRequestsTable,
		getDeprecatedStmtDiagReqsDescriptor)
	validateSchemaExists := func(expectExists bool) {
		migrations.ValidateSchemaExists(
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
	// Run the migration.
	migrations.Migrate(
		t,
		sqlDB,
		clusterversion.AlterSystemStmtDiagReqs,
		nil,   /* done */
		false, /* expectError */
	)
	// Validate that the table has new schema.
	validateSchemaExists(true)
}

// getDeprecatedStmtDiagReqsDescriptor returns the
// system.statement_diagnostics_requests table descriptor that was being used
// before adding two new indexes in the current version.
func getDeprecatedStmtDiagReqsDescriptor() *descpb.TableDescriptor {
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
		},
		NextColumnID: 6,
		Families: []descpb.ColumnFamilyDescriptor{
			{
				Name:        "primary",
				ColumnNames: []string{"id", "completed", "statement_fingerprint", "statement_diagnostics_id", "requested_at"},
				ColumnIDs:   []descpb.ColumnID{1, 2, 3, 4, 5},
			},
		},
		NextFamilyID: 1,
		PrimaryIndex: descpb.IndexDescriptor{
			Name:                tabledesc.PrimaryKeyIndexName("statement_diagnostics_requests"),
			ID:                  1,
			Unique:              true,
			KeyColumnNames:      []string{"id"},
			KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
			KeyColumnIDs:        []descpb.ColumnID{1},
		},
		Indexes: []descpb.IndexDescriptor{
			{
				Name:                "completed_idx",
				ID:                  2,
				Unique:              false,
				KeyColumnNames:      []string{"completed", "id"},
				StoreColumnNames:    []string{"statement_fingerprint"},
				KeyColumnIDs:        []descpb.ColumnID{2, 1},
				KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC, descpb.IndexDescriptor_ASC},
				StoreColumnIDs:      []descpb.ColumnID{3},
				Version:             descpb.StrictIndexColumnIDGuaranteesVersion,
			},
		},
		NextIndexID:    3,
		Privileges:     catpb.NewCustomSuperuserPrivilegeDescriptor(privilege.ReadWriteData, security.NodeUserName()),
		NextMutationID: 1,
		FormatVersion:  3,
	}
}
