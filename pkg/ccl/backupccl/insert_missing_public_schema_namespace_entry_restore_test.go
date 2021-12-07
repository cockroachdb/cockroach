// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestPublicSchemaMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	dir, cleanup := testutils.TempDir(t)
	defer cleanup()
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			ExternalIODir: dir,
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: 1,
					BinaryVersionOverride:          clusterversion.ByKey(clusterversion.InsertPublicSchemaNamespaceEntryOnRestore - 1),
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)
	defer db.Close()
	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])

	// Mimic a restore where the public schema system.namespace entries are
	// missing.
	sqlDB.Exec(t, `CREATE DATABASE db1`)
	sqlDB.Exec(t, `CREATE TABLE db1.t()`)
	sqlDB.Exec(t, `CREATE SCHEMA db1.s`)
	sqlDB.Exec(t, `CREATE DATABASE db2`)
	sqlDB.Exec(t, `CREATE TABLE db2.t(x INT)`)
	sqlDB.Exec(t, `INSERT INTO db2.t VALUES (1), (2)`)
	sqlDB.Exec(t, `CREATE SCHEMA db2.s`)
	sqlDB.Exec(t, `CREATE TABLE db2.s.t(x INT)`)
	sqlDB.Exec(t, `INSERT INTO db2.s.t VALUES (1), (2)`)

	// Give root the ability to delete from system.namespace.
	sqlDB.Exec(t, `INSERT INTO system.users VALUES ('node', '', false);`)
	sqlDB.Exec(t, `GRANT node TO root`)

	var db1ID, db2ID int
	row := sqlDB.QueryRow(t, `SELECT id FROM system.namespace WHERE name = 'db1'`)
	row.Scan(&db1ID)
	row = sqlDB.QueryRow(t, `SELECT id FROM system.namespace WHERE name = 'db2'`)
	row.Scan(&db2ID)

	// Remove public schema namespace entries from system.namespace.
	sqlDB.Exec(t, fmt.Sprintf(`DELETE FROM system.namespace WHERE name='public' AND id=29 AND "parentID"=%d`, db1ID))
	sqlDB.Exec(t, fmt.Sprintf(`DELETE FROM system.namespace WHERE name='public' AND id=29 AND "parentID"=%d`, db2ID))

	// Verify that there are no system.namespace entries for the public schema for
	// the two databases.
	sqlDB.CheckQueryResults(t, fmt.Sprintf(`SELECT id FROM system.namespace WHERE name = 'public' AND "parentID"=%d`, db1ID), [][]string{})
	sqlDB.CheckQueryResults(t, fmt.Sprintf(`SELECT id FROM system.namespace WHERE name = 'public' AND "parentID"=%d`, db2ID), [][]string{})

	// Kick off migration by upgrading to the new version.
	_ = sqlDB.Exec(t, `SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.InsertPublicSchemaNamespaceEntryOnRestore).String())

	sqlDB.CheckQueryResults(t, fmt.Sprintf(`SELECT id FROM system.namespace WHERE name = 'public' AND "parentID"=%d`, db1ID), [][]string{{"29"}})
	sqlDB.CheckQueryResults(t, fmt.Sprintf(`SELECT id FROM system.namespace WHERE name = 'public' AND "parentID"=%d`, db2ID), [][]string{{"29"}})

}
