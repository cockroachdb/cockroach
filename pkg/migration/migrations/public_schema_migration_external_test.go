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
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func publicSchemaMigrationTest(t *testing.T, ctx context.Context, numTables int) {
	settings := cluster.MakeTestingClusterSettingsWithVersions(
		clusterversion.TestingBinaryVersion,
		clusterversion.ByKey(clusterversion.PublicSchemasWithDescriptors-1),
		false,
	)
	// 2048 KiB batch size - 4x the public schema migration's minBatchSizeInBytes.
	const maxCommandSize = 1 << 22
	kvserver.MaxCommandSize.Override(ctx, &settings.SV, maxCommandSize)
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Settings: settings,
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride:          clusterversion.ByKey(clusterversion.PublicSchemasWithDescriptors - 1),
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)
	defer db.Close()
	tdb := sqlutils.MakeSQLRunner(db)

	// We bootstrap the cluster on the older version where databases are
	// created without public schemas. The namespace before upgrading looks like:
	/*
		51 0 public 29
		50 0 public 29
		0 0 defaultdb 50
		0 0 postgres 51
		50 29 t 52
		50 29 typ 53
		50 29 _typ 54
	*/
	tdb.Exec(t, `CREATE TABLE defaultdb.public.t(x INT)`)
	tdb.Exec(t, `INSERT INTO defaultdb.public.t VALUES (1), (2), (3)`)
	tdb.Exec(t, `CREATE TYPE defaultdb.public.typ AS ENUM()`)
	// Ensure the migration works if we have UDS in the database.
	tdb.Exec(t, `CREATE SCHEMA defaultdb.s`)
	tdb.Exec(t, `CREATE TABLE defaultdb.s.table_in_uds(x INT)`)
	tdb.Exec(t, `INSERT INTO defaultdb.s.table_in_uds VALUES (1), (2), (3)`)

	// Create large descriptors to ensure we're batching descriptors.
	// The name of the table is approx 1000 bytes.
	// Thus, we create approximately 5000 KiB of descriptors in this database.
	// This is also larger than the 2048 KiB max command size we set.
	// The batch size in the migration is 512 KiB so this ensures we have at
	// least two batches.
	for i := 0; i < numTables; i++ {
		tdb.Exec(t, fmt.Sprintf(`CREATE TABLE defaultdb.t%s%d()`, strings.Repeat("x", 10000), i))
	}

	{
		_, err := tc.Conns[0].ExecContext(ctx, `SET CLUSTER SETTING version = $1`,
			clusterversion.ByKey(clusterversion.PublicSchemasWithDescriptors).String())
		require.NoError(t, err)
	}

	// Verify that defaultdb and postgres have public schemas with IDs that
	// are not 29.
	const selectDatabaseByName = `
SELECT id FROM system.namespace WHERE name = $1 and "parentID" = 0`
	const selectDatabasePublicSchemaID = `SELECT id
  FROM system.namespace
 WHERE name = 'public'
       AND "parentID" IN (` + selectDatabaseByName + `)`
	var defaultDBPublicSchemaID int
	tdb.QueryRow(t, selectDatabasePublicSchemaID, "defaultdb").
		Scan(&defaultDBPublicSchemaID)
	require.NotEqual(t, defaultDBPublicSchemaID, keys.PublicSchemaID)

	var postgresPublicSchemaID int
	tdb.QueryRow(t, selectDatabasePublicSchemaID, "postgres").
		Scan(&postgresPublicSchemaID)
	require.NotEqual(t, postgresPublicSchemaID, keys.PublicSchemaID)

	// Verify that table "t" and type "typ" and "_typ" are have parent schema id
	// defaultDBPublicSchemaID.
	var tParentSchemaID, typParentSchemaID, typArrParentSchemaID int
	const selectPublicSchemaIDWithNameAndParent = `
SELECT "parentSchemaID"
  FROM system.namespace
 WHERE "parentID" IN (` + selectDatabaseByName + `) AND name = $2`
	tdb.QueryRow(t, selectPublicSchemaIDWithNameAndParent, "defaultdb", "t").
		Scan(&tParentSchemaID)
	require.Equal(t, tParentSchemaID, defaultDBPublicSchemaID)
	tdb.QueryRow(t, selectPublicSchemaIDWithNameAndParent, "defaultdb", "typ").
		Scan(&typParentSchemaID)
	require.Equal(t, typParentSchemaID, defaultDBPublicSchemaID)
	tdb.QueryRow(t, selectPublicSchemaIDWithNameAndParent, "defaultdb", "_typ").
		Scan(&typArrParentSchemaID)
	require.Equal(t, typArrParentSchemaID, defaultDBPublicSchemaID)

	// Verify that the public role has the correct permissions on the public schema.
	for _, expectedPrivType := range []string{"CREATE", "USAGE"} {
		var privType string
		tdb.QueryRow(t, `
SELECT privilege_type FROM [SHOW GRANTS ON SCHEMA defaultdb.public]
WHERE grantee = 'public' AND privilege_type = $1`,
			expectedPrivType).
			Scan(&privType)
		require.Equal(t, expectedPrivType, privType)
		tdb.QueryRow(t, `
SELECT privilege_type FROM [SHOW GRANTS ON SCHEMA postgres.public]
WHERE grantee = 'public' AND privilege_type = $1`,
			expectedPrivType).
			Scan(&privType)
		require.Equal(t, expectedPrivType, privType)
	}

	tdb.Exec(t, `INSERT INTO t VALUES (4)`)

	tdb.CheckQueryResults(t, `SELECT * FROM defaultdb.t ORDER BY x`,
		[][]string{{"1"}, {"2"}, {"3"}, {"4"}})

	// Verify that we can use type "typ".
	tdb.Exec(t, `CREATE TABLE t2(x typ)`)

	// Verify that we can use the typ / enum.
	tdb.Exec(t, `ALTER TYPE typ ADD VALUE 'hello'`)

	tdb.Exec(t, `INSERT INTO t2 VALUES ('hello')`)

	var helloStr string
	tdb.QueryRow(t, `SELECT * FROM t2`).Scan(&helloStr)
	require.Equal(t, "hello", helloStr)

	// Verify that we can query table defaultdb.s.table_in_uds (table in a UDS).
	tdb.CheckQueryResults(t, `SELECT * FROM defaultdb.s.table_in_uds ORDER BY x`,
		[][]string{{"1"}, {"2"}, {"3"}})

	// Verify that the tables with large descriptor sizes have parentSchemaIDs
	// that are not 29.
	const oldPublicSchemaID = 29
	var parentSchemaID int
	for i := 0; i < numTables; i++ {
		tdb.QueryRow(t, fmt.Sprintf(`
SELECT "parentSchemaID" FROM system.namespace WHERE name = 't%s%d'
`, strings.Repeat("x", 10000), i)).
			Scan(&parentSchemaID)
		require.NotEqual(t, parentSchemaID, descpb.InvalidID)
		require.NotEqual(t, oldPublicSchemaID, parentSchemaID)
	}
}

func TestPublicSchemaMigration500Tables(t *testing.T) {
	skip.UnderRace(t, "takes >1min under race")
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	publicSchemaMigrationTest(t, ctx, 500)
}

func TestPublicSchemaMigration10Tables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	publicSchemaMigrationTest(t, ctx, 10)
}
