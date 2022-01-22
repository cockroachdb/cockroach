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
					DisableAutomaticVersionUpgrade: 1,
					BinaryVersionOverride:          clusterversion.ByKey(clusterversion.PublicSchemasWithDescriptors - 1),
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)
	defer db.Close()

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
	_, err := db.Exec(`CREATE TABLE defaultdb.public.t(x INT)`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO defaultdb.public.t VALUES (1), (2), (3)`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TYPE defaultdb.public.typ AS ENUM()`)
	require.NoError(t, err)
	// Ensure the migration works if we have UDS in the database.
	_, err = db.Exec(`CREATE SCHEMA defaultdb.s`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE defaultdb.s.table_in_uds(x INT)`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO defaultdb.s.table_in_uds VALUES (1), (2), (3)`)
	require.NoError(t, err)

	// Create large descriptors to ensure we're batching descriptors.
	// The name of the table is approx 1000 bytes.
	// Thus, we create approximately 5000 KiB of descriptors in this database.
	// This is also larger than the 2048 KiB max command size we set.
	// The batch size in the migration is 512 KiB so this ensures we have at
	// least two batches.
	for i := 0; i < numTables; i++ {
		_, err = db.Exec(fmt.Sprintf(`CREATE TABLE defaultdb.t%s%d()`, strings.Repeat("x", 10000), i))
		require.NoError(t, err)
	}

	_, err = tc.Conns[0].ExecContext(ctx, `SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.PublicSchemasWithDescriptors).String())
	require.NoError(t, err)

	// Verify that defaultdb and postgres have public schemas with IDs that
	// are not 29.
	row := db.QueryRow(`SELECT id FROM system.namespace WHERE name='public' AND "parentID"=50`)
	require.NotNil(t, row)
	var defaultDBPublicSchemaID int
	err = row.Scan(&defaultDBPublicSchemaID)
	require.NoError(t, err)

	require.NotEqual(t, defaultDBPublicSchemaID, keys.PublicSchemaID)

	row = db.QueryRow(`SELECT id FROM system.namespace WHERE name='public' AND "parentID"=51`)
	require.NotNil(t, row)
	var postgresPublicSchemaID int
	err = row.Scan(&postgresPublicSchemaID)
	require.NoError(t, err)

	require.NotEqual(t, postgresPublicSchemaID, keys.PublicSchemaID)

	// Verify that table "t" and type "typ" and "_typ" are have parent schema id
	// defaultDBPublicSchemaID.
	var tParentSchemaID, typParentSchemaID, typArrParentSchemaID int
	row = db.QueryRow(`SELECT "parentSchemaID" FROM system.namespace WHERE name='t' AND "parentID"=50`)
	err = row.Scan(&tParentSchemaID)
	require.NoError(t, err)

	require.Equal(t, tParentSchemaID, defaultDBPublicSchemaID)

	row = db.QueryRow(`SELECT "parentSchemaID" FROM system.namespace WHERE name='typ' AND "parentID"=50`)
	err = row.Scan(&typParentSchemaID)
	require.NoError(t, err)

	require.Equal(t, typParentSchemaID, defaultDBPublicSchemaID)

	row = db.QueryRow(`SELECT "parentSchemaID" FROM system.namespace WHERE name='_typ' AND "parentID"=50`)
	err = row.Scan(&typArrParentSchemaID)
	require.NoError(t, err)

	require.Equal(t, typArrParentSchemaID, defaultDBPublicSchemaID)

	// Verify that the public role has the correct permissions on the public schema.
	for _, expectedPrivType := range []string{"CREATE", "USAGE"} {
		var privType string
		err = db.QueryRow(`
SELECT privilege_type FROM [SHOW GRANTS ON SCHEMA defaultdb.public]
WHERE grantee = 'public' AND privilege_type = $1`,
			expectedPrivType).
			Scan(&privType)
		require.NoError(t, err)
		require.Equal(t, expectedPrivType, privType)
		err = db.QueryRow(`
SELECT privilege_type FROM [SHOW GRANTS ON SCHEMA postgres.public]
WHERE grantee = 'public' AND privilege_type = $1`,
			expectedPrivType).
			Scan(&privType)
		require.NoError(t, err)
		require.Equal(t, expectedPrivType, privType)
	}

	_, err = db.Exec(`INSERT INTO t VALUES (4)`)
	require.NoError(t, err)

	rows, err := db.Query(`SELECT * FROM defaultdb.t ORDER BY x`)
	require.NoError(t, err)
	defer rows.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Verify that we can query table t.
	var x int
	for i := 1; i < 5; i++ {
		rows.Next()
		require.NoError(t, err)
		err = rows.Scan(&x)
		require.NoError(t, err)
		require.Equal(t, x, i)
	}

	// Verify that we can use type "typ".
	_, err = db.Exec(`CREATE TABLE t2(x typ)`)
	require.NoError(t, err)

	// Verify that we can use the typ / enum.
	_, err = db.Exec(`ALTER TYPE typ ADD VALUE 'hello'`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO t2 VALUES ('hello')`)
	require.NoError(t, err)

	row = db.QueryRow(`SELECT * FROM t2`)
	require.NotNil(t, row)

	var helloStr string
	err = row.Scan(&helloStr)
	require.NoError(t, err)

	require.Equal(t, "hello", helloStr)

	rows, err = db.Query(`SELECT * FROM defaultdb.s.table_in_uds ORDER BY x`)
	require.NoError(t, err)

	// Verify that we can query table defaultdb.s.table_in_uds (table in a UDS).
	for i := 1; i < 4; i++ {
		rows.Next()
		require.NoError(t, err)
		err = rows.Scan(&x)
		require.NoError(t, err)
		require.Equal(t, x, i)
	}

	// Verify that the tables with large descriptor sizes have parentSchemaIDs
	// that are not 29.
	const oldPublicSchemaID = 29
	var parentSchemaID int
	for i := 0; i < numTables; i++ {
		row = db.QueryRow(fmt.Sprintf(`SELECT "parentSchemaID" FROM system.namespace WHERE name = 't%s%d'`, strings.Repeat("x", 10000), i))
		err = row.Scan(&parentSchemaID)
		require.NoError(t, err)
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
