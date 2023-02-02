// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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

func TestUpdateTenantsTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride: clusterversion.ByKey(
						clusterversion.V23_1TenantNamesStateAndServiceMode - 1),
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
		validationSchemas = []upgrades.Schema{
			{Name: "name", ValidationFn: upgrades.HasColumn},
			{Name: "data_state", ValidationFn: upgrades.HasColumn},
			{Name: "service_mode", ValidationFn: upgrades.HasColumn},
			{Name: "tenants_name_idx", ValidationFn: upgrades.HasIndex},
			{Name: "tenants_service_mode_idx", ValidationFn: upgrades.HasIndex},
		}
	)

	// Clear the initial KV pairs set up for the system tenant entry. We
	// need to do this because the bootstrap keyspace is initialized in
	// the new version and that includes the latest system tenant entry.
	// The proper way to do this is to initialize the keyspace in the
	// pre-migration state.
	// TODO(sql-schema): Bootstrap in the old version so that this
	// DelRange is not necessary.
	_, err := s.DB().DelRange(ctx,
		keys.SystemSQLCodec.TablePrefix(keys.TenantsTableID),
		keys.SystemSQLCodec.TablePrefix(keys.TenantsTableID).PrefixEnd(),
		false, /* returnKeys */
	)
	require.NoError(t, err)

	// Inject the old copy of the descriptor.
	upgrades.InjectLegacyTable(ctx, t, s, systemschema.TenantsTable, getDeprecatedTenantsDescriptor)
	// Validate that the table sql_instances has the old schema.
	upgrades.ValidateSchemaExists(
		ctx,
		t,
		s,
		sqlDB,
		keys.TenantsTableID,
		systemschema.TenantsTable,
		[]string{},
		validationSchemas,
		false, /* expectExists */
	)
	// Run the upgrade.
	upgrades.Upgrade(
		t,
		sqlDB,
		clusterversion.V23_1TenantNamesStateAndServiceMode,
		nil,   /* done */
		false, /* expectError */
	)
	// Validate that the table has new schema.
	upgrades.ValidateSchemaExists(
		ctx,
		t,
		s,
		sqlDB,
		keys.TenantsTableID,
		systemschema.TenantsTable,
		[]string{},
		validationSchemas,
		true, /* expectExists */
	)

	// Verify that we can do simple operations with the new schema.
	_, err = sqlDB.Exec("CREATE TENANT foo")
	require.NoError(t, err)
	_, err = sqlDB.Exec("ALTER TENANT foo START SERVICE SHARED")
	require.NoError(t, err)
}

// getDeprecatedTenantsDescriptor returns the system.tenants
// table descriptor that was being used before adding a new column in the
// current version.
func getDeprecatedTenantsDescriptor() *descpb.TableDescriptor {
	trueBoolString := "true"
	return &descpb.TableDescriptor{
		Name:                    string(catconstants.TenantsTableName),
		ID:                      keys.TenantsTableID,
		ParentID:                keys.SystemDatabaseID,
		UnexposedParentSchemaID: keys.PublicSchemaID,
		Version:                 1,
		Columns: []descpb.ColumnDescriptor{
			{Name: "id", ID: 1, Type: types.Int},
			{Name: "active", ID: 2, Type: types.Bool, DefaultExpr: &trueBoolString},
			{Name: "info", ID: 3, Type: types.Bytes, Nullable: true},
		},
		NextColumnID: 4,
		Families: []descpb.ColumnFamilyDescriptor{
			{
				Name:        "primary",
				ID:          0,
				ColumnNames: []string{"id", "active", "info"},
				ColumnIDs:   []descpb.ColumnID{1, 2, 3},
			},
		},
		NextFamilyID: 1,
		PrimaryIndex: descpb.IndexDescriptor{
			Name:                "id",
			ID:                  1,
			ConstraintID:        1,
			Unique:              true,
			KeyColumnNames:      []string{"id"},
			KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
			KeyColumnIDs:        []descpb.ColumnID{1},
		},
		NextIndexID:      2,
		Privileges:       catpb.NewCustomSuperuserPrivilegeDescriptor(privilege.ReadWriteData, username.NodeUserName()),
		NextMutationID:   1,
		NextConstraintID: 2,
		FormatVersion:    descpb.InterleavedFormatVersion,
	}
}
