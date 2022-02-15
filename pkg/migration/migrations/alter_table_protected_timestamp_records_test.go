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
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestAlterSystemProtectedTimestampRecordsTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride: clusterversion.ByKey(
						clusterversion.AlterSystemProtectedTimestampAddColumn - 1),
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
		validationSchemas = []migrations.Schema{
			{Name: "target", ValidationFn: migrations.HasColumn},
		}
	)

	// Inject the old copy of the descriptor.
	migrations.InjectLegacyTable(ctx, t, s, systemschema.ProtectedTimestampsRecordsTable, getDeprecatedProtectedTimestampRecordsDescriptor)
	// Validate that the protected timestamp records table has the old schema.
	migrations.ValidateSchemaExists(
		ctx,
		t,
		s,
		sqlDB,
		keys.ProtectedTimestampsRecordsTableID,
		systemschema.ProtectedTimestampsRecordsTable,
		[]string{},
		validationSchemas,
		false, /* expectExists */
	)
	// Run the migration.
	migrations.Migrate(
		t,
		sqlDB,
		clusterversion.AlterSystemProtectedTimestampAddColumn,
		nil,   /* done */
		false, /* expectError */
	)
	// Validate that the table has new schema.
	migrations.ValidateSchemaExists(
		ctx,
		t,
		s,
		sqlDB,
		keys.ProtectedTimestampsRecordsTableID,
		systemschema.ProtectedTimestampsRecordsTable,
		[]string{},
		validationSchemas,
		true, /* expectExists */
	)
}

// getDeprecatedProtectedTimestampRecordsDescriptor returns the
// system.pts_records table descriptor that was being used before adding a new
// column in the current version.
func getDeprecatedProtectedTimestampRecordsDescriptor() *descpb.TableDescriptor {
	falseBoolString := "false"

	return &descpb.TableDescriptor{
		Name:                    "protected_ts_records",
		ID:                      keys.ProtectedTimestampsRecordsTableID,
		ParentID:                keys.SystemDatabaseID,
		UnexposedParentSchemaID: keys.PublicSchemaID,
		Version:                 1,
		Columns: []descpb.ColumnDescriptor{
			{Name: "id", ID: 1, Type: types.Uuid},
			{Name: "ts", ID: 2, Type: types.Decimal},
			{Name: "meta_type", ID: 3, Type: types.String},
			{Name: "meta", ID: 4, Type: types.Bytes, Nullable: true},
			{Name: "num_spans", ID: 5, Type: types.Int},
			{Name: "spans", ID: 6, Type: types.Bytes},
			{Name: "verified", ID: 7, Type: types.Bool, DefaultExpr: &falseBoolString},
		},
		NextColumnID: 8,
		Families: []descpb.ColumnFamilyDescriptor{
			{
				Name:        "primary",
				ColumnNames: []string{"id", "ts", "meta_type", "meta", "num_spans", "spans", "verified"},
				ColumnIDs:   []descpb.ColumnID{1, 2, 3, 4, 5, 6, 7},
			},
		},
		NextFamilyID: 1,
		PrimaryIndex: descpb.IndexDescriptor{
			Name:           "primary",
			ID:             1,
			Unique:         true,
			KeyColumnNames: []string{"id"},
			KeyColumnIDs:   []descpb.ColumnID{1},
			KeyColumnDirections: []descpb.IndexDescriptor_Direction{
				descpb.IndexDescriptor_ASC,
			},
		},
		NextIndexID:    2,
		Privileges:     catpb.NewCustomSuperuserPrivilegeDescriptor(privilege.ReadWriteData, security.NodeUserName()),
		NextMutationID: 1,
		FormatVersion:  3,
	}
}
