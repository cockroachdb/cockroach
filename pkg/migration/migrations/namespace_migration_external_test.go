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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestNamespaceMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: 1,
					BinaryVersionOverride:          clusterversion.ByKey(clusterversion.NamespaceTableWithSchemasMigration - 1),
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	tableName := func(i int) string {
		return fmt.Sprintf("tbl%04d", i)
	}
	dbName := func(i int) string {
		return fmt.Sprintf("db%04d", i)
	}
	const (
		tableNameRegex = `tbl[0-9]{4}`
		dbNameRegex    = `db[0-9]{4}`
	)
	// Chosen so that the number of descriptors (databases and tables) will
	// exceed the batch limit of 1000.
	numDBsInOldNamespaceTable := 750
	numDBsInBothNamespaceTables := 10
	numDBsInNewNamespaceTable := 10
	if util.RaceEnabled {
		numDBsInOldNamespaceTable = 10
		numDBsInBothNamespaceTables = 10
		numDBsInNewNamespaceTable = 10
	}
	numTotalDatabases := numDBsInOldNamespaceTable + numDBsInBothNamespaceTables + numDBsInNewNamespaceTable

	dbIDs := make([]descpb.ID, numTotalDatabases)
	tableIDs := make([]descpb.ID, numTotalDatabases)

	// Create tables and databases, and get their IDs.
	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	for i := 0; i < numTotalDatabases; i++ {
		tdb.Exec(t, fmt.Sprintf(`CREATE DATABASE %s`, dbName(i)))
		tdb.Exec(t, fmt.Sprintf(`CREATE TABLE %s.%s()`, dbName(i), tableName(i)))
	}

	var parentID int
	var name string
	for i, rows := 0, tdb.Query(t,
		`SELECT "parentID", name, id FROM system.namespace WHERE name ~ $1 ORDER BY name`,
		dbNameRegex,
	); rows.Next(); i++ {
		require.NoError(t, rows.Scan(&parentID, &name, &dbIDs[i]))
		require.EqualValues(t, 0, parentID)
		require.EqualValues(t, dbName(i), name)
	}
	for i, rows := 0, tdb.Query(t,
		`SELECT "parentID", name, id FROM system.namespace WHERE name ~ $1 ORDER BY name`,
		tableNameRegex,
	); rows.Next(); i++ {
		require.NoError(t, rows.Scan(&parentID, &name, &tableIDs[i]))
		require.EqualValues(t, dbIDs[i], parentID)
		require.EqualValues(t, tableName(i), name)
	}

	// Move or copy some namespace entries to the old namespace table.
	kvDB := tc.Servers[0].DB()
	require.NoError(t, kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		b := txn.NewBatch()
		for i := 0; i < numDBsInOldNamespaceTable; i++ {
			newDBKey := catalogkeys.NewDatabaseKey(dbName(i))
			newPublicSchemaKey := catalogkeys.NewPublicSchemaKey(dbIDs[i])
			newTableKey := catalogkeys.NewTableKey(dbIDs[i], keys.PublicSchemaID, tableName(i))
			b.Del(
				newDBKey.Key(keys.SystemSQLCodec),
				newPublicSchemaKey.Key(keys.SystemSQLCodec),
				newTableKey.Key(keys.SystemSQLCodec),
			)
		}
		for i := 0; i < numDBsInOldNamespaceTable+numDBsInBothNamespaceTables; i++ {
			oldDBKey := catalogkeys.NewDeprecatedDatabaseKey(dbName(i))
			oldTableKey := catalogkeys.NewDeprecatedTableKey(dbIDs[i], tableName(i))
			b.CPut(oldDBKey.Key(keys.SystemSQLCodec), dbIDs[i], nil)
			b.CPut(oldTableKey.Key(keys.SystemSQLCodec), tableIDs[i], nil)
		}
		return txn.Run(ctx, b)
	}))

	checkNumOldNamespaceEntries := func() {
		var n int
		tdb.QueryRow(t, `SELECT count(*) FROM [2 AS old_namespace]`).Scan(&n)
		require.EqualValues(t, (numDBsInOldNamespaceTable+numDBsInBothNamespaceTables)*2, n)
	}
	checkNumNewNamespaceEntries := func(expected int) {
		var n int
		tdb.QueryRow(t,
			`SELECT count(*) FROM [30 AS new_namespace] WHERE name ~ $1 OR name ~ $2`,
			dbNameRegex, tableNameRegex,
		).Scan(&n)
		require.EqualValues(t, expected, n)
	}
	checkShowDatabases := func() {
		var n int
		tdb.QueryRow(t, `SELECT count(*) FROM [SHOW DATABASES] WHERE database_name ~ $1`, dbNameRegex).
			Scan(&n)
		require.EqualValues(t, numTotalDatabases, n)
	}
	checkShowTables := func() {
		for _, dbIndex := range []int{
			0,
			numDBsInOldNamespaceTable,
			numDBsInOldNamespaceTable + numDBsInBothNamespaceTables,
		} {
			var sn, tn string
			tdb.QueryRow(t,
				fmt.Sprintf(`SELECT schema_name, table_name FROM [SHOW TABLES FROM %s]`, dbName(dbIndex)),
			).Scan(&sn, &tn)
			require.Equal(t, "public", sn)
			require.Equal(t, tableName(dbIndex), tn)
		}
	}

	checkNumOldNamespaceEntries()
	checkNumNewNamespaceEntries((numDBsInBothNamespaceTables + numDBsInNewNamespaceTable) * 2)
	checkShowDatabases()
	checkShowTables()

	tdb.Exec(t, `SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.NamespaceTableWithSchemasMigration).String())

	checkNumOldNamespaceEntries()
	checkNumNewNamespaceEntries(numTotalDatabases * 2)
	checkShowDatabases()
	checkShowTables()
}
