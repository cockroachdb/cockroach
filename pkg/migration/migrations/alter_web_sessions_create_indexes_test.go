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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestAlterSystemWebSessionsTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: 1,
					BinaryVersionOverride: clusterversion.ByKey(
						clusterversion.AlterSystemWebSessionsCreateIndexes - 1),
				},
			},
		},
	}

	var (
		ctx = context.Background()

		tc    = testcluster.StartTestCluster(t, 1, clusterArgs)
		s     = tc.Server(0)
		sqlDB = tc.ServerConn(0)
	)
	defer tc.Stopper().Stop(ctx)

	var (
		validationStmts = []string{
			`SELECT 1 from system.web_sessions@"web_sessions_revokedAt_idx" LIMIT 0`,
			`SELECT 1 from system.web_sessions@"web_sessions_lastUsedAt_idx" LIMIT 0`,
		}
		validationSchemas = []migrations.Schema{
			{Name: "web_sessions_revokedAt_idx", ValidationFn: migrations.HasIndex},
			{Name: "web_sessions_lastUsedAt_idx", ValidationFn: migrations.HasIndex},
		}
	)

	// Inject the old copy of the descriptor.
	migrations.InjectLegacyTable(ctx, t, s, systemschema.WebSessionsTable, getDeprecatedWebSessionsDescriptor)
	// Validate that the web sessions table has the old schema.
	migrations.ValidateSchemaExists(
		ctx,
		t,
		s,
		sqlDB,
		keys.WebSessionsTableID,
		systemschema.WebSessionsTable,
		validationStmts,
		validationSchemas,
		false, /* expectExists */
	)
	// Run the migration.
	migrations.Migrate(
		t,
		sqlDB,
		clusterversion.AlterSystemWebSessionsCreateIndexes,
		nil,   /* done */
		false, /* expectError */
	)
	// Validate that the table has new schema.
	migrations.ValidateSchemaExists(
		ctx,
		t,
		s,
		sqlDB,
		keys.WebSessionsTableID,
		systemschema.WebSessionsTable,
		validationStmts,
		validationSchemas,
		true, /* expectExists */
	)
}

// getDeprecatedWebSessionsDescriptor returns the system.web_sessions table descriptor that was being used
// before adding two new indexes in the current version.
func getDeprecatedWebSessionsDescriptor() *descpb.TableDescriptor {
	uniqueRowIDString := "unique_rowid()"
	nowString := "now():::TIMESTAMP"

	return &descpb.TableDescriptor{
		Name:                    "web_sessions",
		ID:                      keys.WebSessionsTableID,
		ParentID:                keys.SystemDatabaseID,
		UnexposedParentSchemaID: keys.PublicSchemaID,
		Version:                 1,
		Columns: []descpb.ColumnDescriptor{
			{Name: "id", ID: 1, Type: types.Int, DefaultExpr: &uniqueRowIDString},
			{Name: "hashedSecret", ID: 2, Type: types.Bytes},
			{Name: "username", ID: 3, Type: types.String},
			{Name: "createdAt", ID: 4, Type: types.Timestamp, DefaultExpr: &nowString},
			{Name: "expiresAt", ID: 5, Type: types.Timestamp},
			{Name: "revokedAt", ID: 6, Type: types.Timestamp, Nullable: true},
			{Name: "lastUsedAt", ID: 7, Type: types.Timestamp, DefaultExpr: &nowString},
			{Name: "auditInfo", ID: 8, Type: types.String, Nullable: true},
		},
		NextColumnID: 9,
		Families: []descpb.ColumnFamilyDescriptor{
			{
				Name: "fam_0_id_hashedSecret_username_createdAt_expiresAt_revokedAt_lastUsedAt_auditInfo",
				ID:   0,
				ColumnNames: []string{
					"id",
					"hashedSecret",
					"username",
					"createdAt",
					"expiresAt",
					"revokedAt",
					"lastUsedAt",
					"auditInfo",
				},
				ColumnIDs: []descpb.ColumnID{1, 2, 3, 4, 5, 6, 7, 8},
			},
		},
		NextFamilyID: 1,
		PrimaryIndex: descpb.IndexDescriptor{
			Name:                tabledesc.PrimaryKeyIndexName,
			ID:                  1,
			Unique:              true,
			KeyColumnNames:      []string{"id"},
			KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
			KeyColumnIDs:        []descpb.ColumnID{1},
		},
		Indexes: []descpb.IndexDescriptor{
			{
				Name:                "web_sessions_expiresAt_idx",
				ID:                  2,
				Unique:              false,
				KeyColumnNames:      []string{"expiresAt"},
				KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
				KeyColumnIDs:        []descpb.ColumnID{5},
				KeySuffixColumnIDs:  []descpb.ColumnID{1},
				Version:             descpb.StrictIndexColumnIDGuaranteesVersion,
			},
			{
				Name:                "web_sessions_createdAt_idx",
				ID:                  3,
				Unique:              false,
				KeyColumnNames:      []string{"createdAt"},
				KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
				KeyColumnIDs:        []descpb.ColumnID{4},
				KeySuffixColumnIDs:  []descpb.ColumnID{1},
				Version:             descpb.StrictIndexColumnIDGuaranteesVersion,
			},
		},
		NextIndexID:    4,
		Privileges:     descpb.NewCustomSuperuserPrivilegeDescriptor(privilege.ReadWriteData, security.NodeUserName()),
		NextMutationID: 1,
		FormatVersion:  3,
	}
}
