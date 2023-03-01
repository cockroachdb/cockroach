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
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestExternalConnectionsUserIDMigrationNoUsers(t *testing.T) {
	runTestExternalConnectionsUserIDMigration(t, 0)
}

func TestExternalConnectionsUserIDMigration10Users(t *testing.T) {
	runTestExternalConnectionsUserIDMigration(t, 10)
}

func TestExternalConnectionsUserIDMigration1500Users(t *testing.T) {
	skip.UnderRace(t)
	skip.UnderStress(t)
	runTestExternalConnectionsUserIDMigration(t, 1500)
}

func runTestExternalConnectionsUserIDMigration(t *testing.T, numUsers int) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	settings := cluster.MakeTestingClusterSettingsWithVersions(
		clusterversion.TestingBinaryVersion,
		clusterversion.ByKey(clusterversion.V23_1ExternalConnectionsTableHasOwnerIDColumn-1),
		false, /* initializeVersion */
	)

	tc := testcluster.StartTestCluster(t, 1 /* nodes */, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Settings: settings,
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride:          clusterversion.ByKey(clusterversion.V23_1ExternalConnectionsTableHasOwnerIDColumn - 1),
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)
	defer db.Close()
	tdb := sqlutils.MakeSQLRunner(db)
	s := tc.Server(0)

	// Inject the descriptor for system.external_connections table from before
	// the owner_id column was added.
	upgrades.InjectLegacyTable(ctx, t, s, systemschema.SystemExternalConnectionsTable,
		getTableDescForExternalConnectionsTableBeforeOwnerIDCol)

	// Create test users.
	tx, err := db.BeginTx(ctx, nil /* opts */)
	require.NoError(t, err)
	txRunner := sqlutils.MakeSQLRunner(tx)
	for i := 0; i < numUsers; i++ {
		// Group statements into transactions of 100 users to speed up creation.
		if i != 0 && i%100 == 0 {
			err := tx.Commit()
			require.NoError(t, err)
			tx, err = db.BeginTx(ctx, nil /* opts */)
			require.NoError(t, err)
			txRunner = sqlutils.MakeSQLRunner(tx)
		}
		txRunner.Exec(t, fmt.Sprintf("CREATE USER testuser%d", i))

		// Simulate the INSERT that happens when an external connection is created.
		txRunner.Exec(t, fmt.Sprintf(`
INSERT INTO system.external_connections (connection_name, connection_type, connection_details, owner)
VALUES (
	'connection%[1]d',
	'STORAGE',
	'\x0801121b0a196e6f64656c6f63616c3a2f2f312f636f6e6e656374696f6e30',
	'testuser%[1]d'
)
`, i))
	}
	err = tx.Commit()
	require.NoError(t, err)
	tdb.CheckQueryResults(t, "SELECT count(*) FROM system.external_connections", [][]string{{strconv.Itoa(numUsers)}})

	// Run migrations.
	_, err = tc.Conns[0].ExecContext(ctx, `SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.V23_1ExternalConnectionsTableHasOwnerIDColumn).String())
	require.NoError(t, err)
	_, err = tc.Conns[0].ExecContext(ctx, `SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.V23_1ExternalConnectionsTableOwnerIDColumnBackfilled).String())
	require.NoError(t, err)

	// Verify that the final schema matches the expected one.
	expectedSchema := `CREATE TABLE public.external_connections (
	connection_name STRING NOT NULL,
	created TIMESTAMP NOT NULL DEFAULT now():::TIMESTAMP,
	updated TIMESTAMP NOT NULL DEFAULT now():::TIMESTAMP,
	connection_type STRING NOT NULL,
	connection_details BYTES NOT NULL,
	owner STRING NOT NULL,
	owner_id OID NULL,
	CONSTRAINT "primary" PRIMARY KEY (connection_name ASC)
)`
	r := tdb.QueryRow(t, "SELECT create_statement FROM [SHOW CREATE TABLE system.external_connections]")
	var actualSchema string
	r.Scan(&actualSchema)
	require.Equal(t, expectedSchema, actualSchema)

	// Check that the backfill was successful and correct.
	tdb.CheckQueryResults(t, "SELECT * FROM system.external_connections WHERE owner_id IS NULL", [][]string{})
	tdb.CheckQueryResults(t, "SELECT count(*) FROM system.external_connections", [][]string{{strconv.Itoa(numUsers)}})
	tdb.CheckQueryResults(t, "SELECT count(*) FROM system.external_connections JOIN system.users ON owner = username AND owner_id <> user_id", [][]string{{"0"}})
}

func getTableDescForExternalConnectionsTableBeforeOwnerIDCol() *descpb.TableDescriptor {
	nowString := "now():::TIMESTAMP"
	return &descpb.TableDescriptor{
		Name:                    string(catconstants.SystemExternalConnectionsTableName),
		ID:                      descpb.InvalidID,
		ParentID:                keys.SystemDatabaseID,
		UnexposedParentSchemaID: keys.PublicSchemaID,
		Version:                 1,
		Columns: []descpb.ColumnDescriptor{
			{Name: "connection_name", ID: 1, Type: types.String},
			{Name: "created", ID: 2, Type: types.Timestamp, DefaultExpr: &nowString},
			{Name: "updated", ID: 3, Type: types.Timestamp, DefaultExpr: &nowString},
			{Name: "connection_type", ID: 4, Type: types.String},
			{Name: "connection_details", ID: 5, Type: types.Bytes},
			{Name: "owner", ID: 6, Type: types.String},
		},
		NextColumnID: 7,
		Families: []descpb.ColumnFamilyDescriptor{
			{
				Name:        "primary",
				ID:          0,
				ColumnNames: []string{"connection_name", "created", "updated", "connection_type", "connection_details", "owner"},
				ColumnIDs:   []descpb.ColumnID{1, 2, 3, 4, 5, 6},
			},
		},
		NextFamilyID: 1,
		PrimaryIndex: descpb.IndexDescriptor{
			Name:                "primary",
			ID:                  1,
			Unique:              true,
			KeyColumnNames:      []string{"connection_name"},
			KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
			KeyColumnIDs:        []descpb.ColumnID{1},
		},
		NextIndexID:      2,
		Privileges:       catpb.NewCustomSuperuserPrivilegeDescriptor(privilege.ReadWriteData, username.NodeUserName()),
		FormatVersion:    descpb.InterleavedFormatVersion,
		NextMutationID:   1,
		NextConstraintID: 1,
	}
}
