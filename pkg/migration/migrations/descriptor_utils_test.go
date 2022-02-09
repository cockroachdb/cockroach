// Copyright 2022 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/migration/migrations"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestCreateSystemTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	fakeTableSchema := `CREATE TABLE public.fake_table (
	id UUID NOT NULL,
	CONSTRAINT "primary" PRIMARY KEY (id ASC)
)`
	fakeTable := descpb.TableDescriptor{
		Name:                    "fake_table",
		ParentID:                keys.SystemDatabaseID,
		UnexposedParentSchemaID: keys.PublicSchemaID,
		Columns: []descpb.ColumnDescriptor{
			{Name: "id", ID: 1, Type: types.Uuid, Nullable: false},
		},
		NextColumnID: 2,
		Families: []descpb.ColumnFamilyDescriptor{
			{
				Name:            "primary",
				ID:              0,
				ColumnNames:     []string{"id", "secret", "expiration"},
				ColumnIDs:       []descpb.ColumnID{1, 2, 3},
				DefaultColumnID: 0,
			},
		},
		NextFamilyID: 1,
		PrimaryIndex: descpb.IndexDescriptor{
			Name:           tabledesc.LegacyPrimaryKeyIndexName,
			ID:             1,
			Unique:         true,
			KeyColumnNames: []string{"id"},
			KeyColumnDirections: []descpb.IndexDescriptor_Direction{
				descpb.IndexDescriptor_ASC,
			},
			KeyColumnIDs: []descpb.ColumnID{1},
		},
		NextIndexID: 2,
		Privileges: catpb.NewCustomSuperuserPrivilegeDescriptor(
			privilege.ReadData,
			security.NodeUserName(),
		),
	}

	table := tabledesc.NewBuilder(&fakeTable).BuildCreatedMutable().(catalog.TableDescriptor)

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	// Verify that the keys were not written.
	checkEntries := func(t *testing.T) [][]string {
		return sqlDB.QueryStr(t, `
SELECT *
  FROM system.namespace
 WHERE "parentID" = $1 AND "parentSchemaID" = $2 AND name = $3`,
			table.GetParentID(), table.GetParentSchemaID(), table.GetName())
	}
	require.Len(t, checkEntries(t), 0)
	require.NoError(t, migrations.CreateSystemTable(
		ctx, tc.Server(0).DB(), keys.SystemSQLCodec, table,
	))
	require.Len(t, checkEntries(t), 1)
	sqlDB.CheckQueryResults(t,
		"SELECT create_statement FROM [SHOW CREATE TABLE system.fake_table]",
		[][]string{{fakeTableSchema}})

	// Make sure it's idempotent.
	require.NoError(t, migrations.CreateSystemTable(
		ctx, tc.Server(0).DB(), keys.SystemSQLCodec, table,
	))
	require.Len(t, checkEntries(t), 1)

}
