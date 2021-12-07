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
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
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

	/*
		This backup was created by executing the following commands on a v21.1.1
		cluster and performing a full cluster backup.
			CREATE DATABASE db1;
			CREATE TABLE db1.t();
			CREATE SCHEMA db1.s;
			CREATE DATABASE db2;
			CREATE TABLE db2.t(x INT);
			INSERT INTO db2.t VALUES (1), (2);
			CREATE SCHEMA db2.s;
			CREATE TABLE db2.s.t(x INT);
			INSERT INTO db2.s.t VALUES (1), (2);
	*/
	publicSchemaDir, err := filepath.Abs("./testdata/restore_old_versions/missing-public-schema-namespace/v21.1.1")
	require.NoError(t, err)
	err = os.Symlink(publicSchemaDir, filepath.Join(dir, "foo"))
	require.NoError(t, err)

	localFoo := "nodelocal://0/foo"

	_ = sqlDB.Exec(t, fmt.Sprintf("RESTORE DATABASE db1 FROM '%s'", localFoo))
	_ = sqlDB.Exec(t, fmt.Sprintf("RESTORE DATABASE db2 FROM '%s'", localFoo))

	var db1ID, db2ID int
	row := sqlDB.QueryRow(t, `SELECT id FROM system.namespace WHERE name = 'db1'`)
	row.Scan(&db1ID)
	row = sqlDB.QueryRow(t, `SELECT id FROM system.namespace WHERE name = 'db2'`)
	row.Scan(&db2ID)

	// Restore is bugged, the public schemas will not have entries in
	// system.namespace.
	sqlDB.CheckQueryResults(t, fmt.Sprintf(`SELECT id FROM system.namespace WHERE name = 'public' AND "parentID"=%d`, db1ID), [][]string{})
	sqlDB.CheckQueryResults(t, fmt.Sprintf(`SELECT id FROM system.namespace WHERE name = 'public' AND "parentID"=%d`, db2ID), [][]string{})

	// Migrate to the new version.
	_ = sqlDB.Exec(t, `SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.InsertPublicSchemaNamespaceEntryOnRestore).String())
	require.NoError(t, err)

	sqlDB.CheckQueryResults(t, fmt.Sprintf(`SELECT id FROM system.namespace WHERE name = 'public' AND "parentID"=%d`, db1ID), [][]string{{"29"}})
	sqlDB.CheckQueryResults(t, fmt.Sprintf(`SELECT id FROM system.namespace WHERE name = 'public' AND "parentID"=%d`, db2ID), [][]string{{"29"}})

}
