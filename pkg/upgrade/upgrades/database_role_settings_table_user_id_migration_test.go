// Copyright 2023 The Cockroach Authors.
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
	"fmt"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestDatabaseRoleSettingsUserIDMigrationNoUsers(t *testing.T) {
	runTestDatabaseRoleSettingsUserIDMigration(t, 0)
}

func TestDatabaseRoleSettingsUserIDMigration10Users(t *testing.T) {
	runTestDatabaseRoleSettingsUserIDMigration(t, 10)
}

func TestDatabaseRoleSettingsUserIDMigration1500Users(t *testing.T) {
	skip.UnderRace(t)
	skip.UnderStress(t)
	runTestDatabaseRoleSettingsUserIDMigration(t, 1500)
}

func runTestDatabaseRoleSettingsUserIDMigration(t *testing.T, numUsers int) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	settings := cluster.MakeTestingClusterSettingsWithVersions(
		clusterversion.TestingBinaryVersion,
		clusterversion.ByKey(clusterversion.V23_1DatabaseRoleSettingsHasRoleIDColumn-1),
		false, /* initializeVersion */
	)
	tc := testcluster.StartTestCluster(t, 1 /* nodes */, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Settings: settings,
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride:          clusterversion.ByKey(clusterversion.V23_1DatabaseRoleSettingsHasRoleIDColumn - 1),
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)
	defer db.Close()
	tdb := sqlutils.MakeSQLRunner(db)
	s := tc.Server(0)

	// Inject the descriptor for the system.database_role_settings table from
	// before the role_id column was added.
	upgrades.InjectLegacyTable(ctx, t, s, systemschema.DatabaseRoleSettingsTable, getTableDescForDatabaseRoleSettingsTableBeforeRoleIDCol)

	// Create test users.
	upgrades.ExecForCountInTxns(ctx, t, db, numUsers, 100 /* txCount */, func(txRunner *sqlutils.SQLRunner, i int) {
		txRunner.Exec(t, fmt.Sprintf("CREATE USER testuser%d", i))
		txRunner.Exec(t, fmt.Sprintf(`ALTER USER testuser%d SET application_name = 'roach sql'`, i))
	})
	tdb.CheckQueryResults(t, "SELECT count(*) FROM system.database_role_settings", [][]string{{strconv.Itoa(numUsers)}})

	// Run migrations.
	_, err := tc.Conns[0].ExecContext(ctx, `SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.V23_1DatabaseRoleSettingsHasRoleIDColumn).String())
	require.NoError(t, err)
	_, err = tc.Conns[0].ExecContext(ctx, `SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.V23_1DatabaseRoleSettingsRoleIDColumnBackfilled).String())
	require.NoError(t, err)

	// Verify that the final schema matches the expected one.
	expectedSchema := `CREATE TABLE public.database_role_settings (
	database_id OID NOT NULL,
	role_name STRING NOT NULL,
	settings STRING[] NOT NULL,
	role_id OID NOT NULL,
	CONSTRAINT "primary" PRIMARY KEY (database_id ASC, role_name ASC),
	UNIQUE INDEX database_role_settings_database_id_role_id_key (database_id ASC, role_id ASC) STORING (settings)
)`
	r := tdb.QueryRow(t, "SELECT create_statement FROM [SHOW CREATE TABLE system.database_role_settings]")
	var actualSchema string
	r.Scan(&actualSchema)
	require.Equal(t, expectedSchema, actualSchema)

	// Check that the backfill was successful and correct.
	tdb.CheckQueryResults(t, "SELECT * FROM system.database_role_settings WHERE role_id IS NULL", [][]string{})
	tdb.CheckQueryResults(t, "SELECT count(*) FROM system.database_role_settings", [][]string{{strconv.Itoa(numUsers)}})
	tdb.CheckQueryResults(t, "SELECT count(*) FROM system.database_role_settings AS a JOIN system.users AS b ON a.role_name = b.username AND a.role_id <> b.user_id", [][]string{{"0"}})
}

func getTableDescForDatabaseRoleSettingsTableBeforeRoleIDCol() *descpb.TableDescriptor {
	return &descpb.TableDescriptor{
		Name:                    string(catconstants.DatabaseRoleSettingsTableName),
		ID:                      keys.DatabaseRoleSettingsTableID,
		ParentID:                keys.SystemDatabaseID,
		UnexposedParentSchemaID: keys.PublicSchemaID,
		Version:                 1,
		Columns: []descpb.ColumnDescriptor{
			{Name: "database_id", ID: 1, Type: types.Oid, Nullable: false},
			{Name: "role_name", ID: 2, Type: types.String, Nullable: false},
			{Name: "settings", ID: 3, Type: types.StringArray, Nullable: false},
		},
		NextColumnID: 4,
		Families: []descpb.ColumnFamilyDescriptor{
			{
				Name:            "primary",
				ID:              0,
				ColumnNames:     []string{"database_id", "role_name", "settings"},
				ColumnIDs:       []descpb.ColumnID{1, 2, 3},
				DefaultColumnID: 3,
			},
		},
		NextFamilyID: 1,
		PrimaryIndex: descpb.IndexDescriptor{
			Name:           "primary",
			ID:             1,
			Unique:         true,
			KeyColumnNames: []string{"database_id", "role_name"},
			KeyColumnDirections: []catenumpb.IndexColumn_Direction{
				catenumpb.IndexColumn_ASC, catenumpb.IndexColumn_ASC,
			},
			KeyColumnIDs: []descpb.ColumnID{1, 2},
		},
		NextIndexID:      2,
		FormatVersion:    descpb.InterleavedFormatVersion,
		NextMutationID:   1,
		NextConstraintID: 1,
	}
}
