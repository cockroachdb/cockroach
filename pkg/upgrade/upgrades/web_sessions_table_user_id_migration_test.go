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

func TestWebSessionsUserIDMigrationNoUsers(t *testing.T) {
	runTestWebSessionsUserIDMigration(t, 0)
}

func TestWebSessionsUserIDMigration10Users(t *testing.T) {
	runTestWebSessionsUserIDMigration(t, 10)
}

func TestWebSessionsUserIDMigration1500Users(t *testing.T) {
	skip.UnderRace(t)
	skip.UnderStress(t)
	runTestWebSessionsUserIDMigration(t, 1500)
}

func runTestWebSessionsUserIDMigration(t *testing.T, numUsers int) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	settings := cluster.MakeTestingClusterSettingsWithVersions(
		clusterversion.TestingBinaryVersion,
		clusterversion.ByKey(clusterversion.V23_1WebSessionsTableHasUserIDColumn-1),
		false, /* initializeVersion */
	)

	tc := testcluster.StartTestCluster(t, 1 /* nodes */, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Settings: settings,
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride:          clusterversion.ByKey(clusterversion.V23_1WebSessionsTableHasUserIDColumn - 1),
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)
	defer db.Close()
	tdb := sqlutils.MakeSQLRunner(db)
	s := tc.Server(0)

	// Inject the descriptor for the system.web_sessions table from before the
	// user_id column was added.
	upgrades.InjectLegacyTable(ctx, t, s, systemschema.WebSessionsTable, getTableDescForSystemWebSessionsTableBeforeUserIDCol)

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
		txRunner.Exec(t, fmt.Sprintf(`
INSERT INTO system.web_sessions ("hashedSecret", username, "createdAt", "expiresAt", "lastUsedAt")
VALUES (
	'\xe77edd369fc3a955a129d2ced69d97717eeee49912e4369a2b687a4d8c36f798',
	'testuser%d',
	'2023-02-14 20:56:30.699447',
	'2023-02-21 20:56:30.699242',
	'2023-02-14 20:56:30.699447'
)
`, i))
	}
	err = tx.Commit()
	require.NoError(t, err)
	tdb.CheckQueryResults(t, "SELECT count(*) FROM system.web_sessions", [][]string{{strconv.Itoa(numUsers)}})

	// Run migrations.
	_, err = tc.Conns[0].ExecContext(ctx, `SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.V23_1WebSessionsTableHasUserIDColumn).String())
	require.NoError(t, err)
	_, err = tc.Conns[0].ExecContext(ctx, `SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.V23_1WebSessionsTableUserIDColumnBackfilled).String())
	require.NoError(t, err)

	// Verify that the final schema matches the expected one.
	expectedSchema := `CREATE TABLE public.web_sessions (
	id INT8 NOT NULL DEFAULT unique_rowid(),
	"hashedSecret" BYTES NOT NULL,
	username STRING NOT NULL,
	"createdAt" TIMESTAMP NOT NULL DEFAULT now():::TIMESTAMP,
	"expiresAt" TIMESTAMP NOT NULL,
	"revokedAt" TIMESTAMP NULL,
	"lastUsedAt" TIMESTAMP NOT NULL DEFAULT now():::TIMESTAMP,
	"auditInfo" STRING NULL,
	user_id OID NULL,
	CONSTRAINT "primary" PRIMARY KEY (id ASC),
	INDEX "web_sessions_expiresAt_idx" ("expiresAt" ASC),
	INDEX "web_sessions_createdAt_idx" ("createdAt" ASC),
	INDEX "web_sessions_revokedAt_idx" ("revokedAt" ASC),
	INDEX "web_sessions_lastUsedAt_idx" ("lastUsedAt" ASC),
	FAMILY "fam_0_id_hashedSecret_username_createdAt_expiresAt_revokedAt_lastUsedAt_auditInfo" (id, "hashedSecret", username, "createdAt", "expiresAt", "revokedAt", "lastUsedAt", "auditInfo", user_id)
)`
	r := tdb.QueryRow(t, "SELECT create_statement FROM [SHOW CREATE TABLE system.web_sessions]")
	var actualSchema string
	r.Scan(&actualSchema)
	require.Equal(t, expectedSchema, actualSchema)

	// Check that the backfill was successful and correct.
	tdb.CheckQueryResults(t, "SELECT * FROM system.web_sessions WHERE user_id IS NULL", [][]string{})
	tdb.CheckQueryResults(t, "SELECT count(*) FROM system.web_sessions", [][]string{{strconv.Itoa(numUsers)}})
	tdb.CheckQueryResults(t, "SELECT count(*) FROM system.web_sessions AS a JOIN system.users AS b ON a.username = b.username AND a.user_id <> b.user_id", [][]string{{"0"}})
}

func getTableDescForSystemWebSessionsTableBeforeUserIDCol() *descpb.TableDescriptor {
	uniqueRowIDString := "unique_rowid()"
	nowString := "now():::TIMESTAMP"
	return &descpb.TableDescriptor{
		Name:                    string(catconstants.WebSessionsTableName),
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
			Name:                "primary",
			ID:                  1,
			Unique:              true,
			KeyColumnNames:      []string{"id"},
			KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
			KeyColumnIDs:        []descpb.ColumnID{1},
		},
		Indexes: []descpb.IndexDescriptor{
			{
				Name:                "web_sessions_expiresAt_idx",
				ID:                  2,
				Unique:              false,
				KeyColumnNames:      []string{"expiresAt"},
				KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
				KeyColumnIDs:        []descpb.ColumnID{5},
				KeySuffixColumnIDs:  []descpb.ColumnID{1},
				Version:             descpb.StrictIndexColumnIDGuaranteesVersion,
			},
			{
				Name:                "web_sessions_createdAt_idx",
				ID:                  3,
				Unique:              false,
				KeyColumnNames:      []string{"createdAt"},
				KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
				KeyColumnIDs:        []descpb.ColumnID{4},
				KeySuffixColumnIDs:  []descpb.ColumnID{1},
				Version:             descpb.StrictIndexColumnIDGuaranteesVersion,
			},
			{
				Name:                "web_sessions_revokedAt_idx",
				ID:                  4,
				Unique:              false,
				KeyColumnNames:      []string{"revokedAt"},
				KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
				KeyColumnIDs:        []descpb.ColumnID{6},
				KeySuffixColumnIDs:  []descpb.ColumnID{1},
				Version:             descpb.StrictIndexColumnIDGuaranteesVersion,
			},
			{
				Name:                "web_sessions_lastUsedAt_idx",
				ID:                  5,
				Unique:              false,
				KeyColumnNames:      []string{"lastUsedAt"},
				KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
				KeyColumnIDs:        []descpb.ColumnID{7},
				KeySuffixColumnIDs:  []descpb.ColumnID{1},
				Version:             descpb.StrictIndexColumnIDGuaranteesVersion,
			},
		},
		NextIndexID:      6,
		Privileges:       catpb.NewCustomSuperuserPrivilegeDescriptor(privilege.ReadWriteData, username.NodeUserName()),
		FormatVersion:    descpb.InterleavedFormatVersion,
		NextMutationID:   1,
		NextConstraintID: 1,
	}
}
