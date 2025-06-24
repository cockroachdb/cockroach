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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestUsersLastLoginTimeTableMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterversion.SkipWhenMinSupportedVersionIsAtLeast(t, clusterversion.V25_3)

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
			`SELECT estimated_last_login_time FROM system.users LIMIT 0`,
		}
		validationSchemas = []upgrades.Schema{
			{Name: "estimated_last_login_time", ValidationFn: upgrades.HasColumn},
		}
	)

	// Inject the old copy of the descriptor.
	upgrades.InjectLegacyTable(ctx, t, s, systemschema.UsersTable,
		getOldUsersDescriptor)
	validateSchemaExists := func(expectExists bool) {
		upgrades.ValidateSchemaExists(
			ctx,
			t,
			s,
			sqlDB,
			keys.UsersTableID,
			systemschema.UsersTable,
			validationStmts,
			validationSchemas,
			expectExists,
		)
	}
	// Validate that the users table has the old
	// schema.
	validateSchemaExists(false)
	// Run the upgrade.
	upgrades.Upgrade(
		t,
		sqlDB,
		clusterversion.V25_3_AddEstimatedLastLoginTime,
		nil,   /* done */
		false, /* expectError */
	)
	// Validate that the table has new schema.
	validateSchemaExists(true)
}

// getOldUsersDescriptor returns the
// system.users table descriptor that was being used
// before adding the last login time column to the current version.
func getOldUsersDescriptor() *descpb.TableDescriptor {
	falseBoolString := "false"
	return &descpb.TableDescriptor{
		Name:                    string(catconstants.UsersTableName),
		ID:                      keys.UsersTableID,
		ParentID:                keys.SystemDatabaseID,
		UnexposedParentSchemaID: keys.PublicSchemaID,
		Version:                 1,
		Columns: []descpb.ColumnDescriptor{
			{Name: "username", ID: 1, Type: types.String},
			{Name: "hashedPassword", ID: 2, Type: types.Bytes, Nullable: true},
			{Name: "isRole", ID: 3, Type: types.Bool, DefaultExpr: &falseBoolString},
			{Name: "user_id", ID: 4, Type: types.Oid},
		},
		NextColumnID: 5,
		Families: []descpb.ColumnFamilyDescriptor{
			{Name: "primary", ID: 0, ColumnNames: []string{"username", "user_id"}, ColumnIDs: []descpb.ColumnID{1, 4}, DefaultColumnID: 4},
			{Name: "fam_2_hashedPassword", ID: 2, ColumnNames: []string{"hashedPassword"}, ColumnIDs: []descpb.ColumnID{2}, DefaultColumnID: 2},
			{Name: "fam_3_isRole", ID: 3, ColumnNames: []string{"isRole"}, ColumnIDs: []descpb.ColumnID{3}, DefaultColumnID: 3},
		},
		NextFamilyID: 4,
		PrimaryIndex: descpb.IndexDescriptor{
			Name:                tabledesc.LegacyPrimaryKeyIndexName,
			ID:                  1,
			Unique:              true,
			KeyColumnNames:      []string{"username"},
			KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
			KeyColumnIDs:        []descpb.ColumnID{1},
			ConstraintID:        1,
		},
		Indexes: []descpb.IndexDescriptor{
			{
				Name:                "users_user_id_idx",
				ID:                  2,
				Unique:              true,
				KeyColumnNames:      []string{"user_id"},
				KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
				KeyColumnIDs:        []descpb.ColumnID{4},
				Version:             descpb.StrictIndexColumnIDGuaranteesVersion,
				ConstraintID:        2,
			},
		},
		NextIndexID:      3,
		Privileges:       catpb.NewCustomSuperuserPrivilegeDescriptor(privilege.ReadWriteData, username.NodeUserName()),
		NextMutationID:   1,
		FormatVersion:    3,
		NextConstraintID: 3,
	}
}
