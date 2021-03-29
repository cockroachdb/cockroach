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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestSequenceMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	// Start up a cluster in 20.2 and create some tables/views/sequences.
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: 1,
					BinaryVersionOverride:          clusterversion.ByKey(clusterversion.SequencesRegclass - 1),
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	// Create descriptors in 20.2.
	tdb.Exec(t, `CREATE SEQUENCE s`)
	tdb.Exec(t, `SET serial_normalization = 'sql_sequence'`)
	tdb.Exec(t, `CREATE TABLE t (i SERIAL PRIMARY KEY, j INT NOT NULL DEFAULT nextval(52::regclass))`)
	tdb.Exec(t, `INSERT INTO t VALUES (default, default)`)
	tdb.Exec(t, `CREATE TABLE t2 (i INT NOT NULL DEFAULT nextval('defaultdb.s'))`)
	tdb.Exec(t, `CREATE VIEW v AS (SELECT nextval('s'))`)
	tdb.Exec(t, `CREATE VIEW v2 AS (SELECT a FROM (SELECT nextval('s'::regclass) + currval('s') AS a))`)

	// This is known to fail (cross-database sequence reference).
	//tdb.Exec(t, `CREATE DATABASE db`)
	//tdb.Exec(t, `SET DATABASE = db`)
	//tdb.Exec(t, `CREATE TABLE t (i INT NOT NULL DEFAULT nextval('s'))`)

	// Upgrade cluster.
	tdb.Exec(t, `SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.SequenceMigration).String())

	// Rename the sequence, verify tables are not corrupted and dependencies are fine.
	tdb.Exec(t, `ALTER SEQUENCE s RENAME TO s2`)
	tdb.Exec(t, `ALTER SEQUENCE t_i_seq RENAME TO t_i_seq2`)
	tdb.CheckQueryResults(t, `SELECT * from v`, [][]string{{"2"}})
	tdb.CheckQueryResults(t, `SELECT * from v2`, [][]string{{"6"}})
	tdb.Exec(t, `INSERT INTO t VALUES (default, default)`)
	tdb.CheckQueryResults(t, `SELECT * FROM t ORDER BY i`, [][]string{
		{"1", "1"},
		{"2", "4"},
	})
	tdb.Exec(t, `INSERT INTO t2 VALUES (default)`)
	tdb.CheckQueryResults(t, `SELECT * FROM t2`, [][]string{{"5"}})
	tdb.ExpectErr(t, "pq: cannot drop sequence s2 because other objects depend on it", `DROP SEQUENCE s2`)
	tdb.ExpectErr(t, "pq: cannot drop sequence t_i_seq2 because other objects depend on it", `DROP SEQUENCE t_i_seq2`)

	// Verify we can rename dependent objects even if referenced by fully qualified name.
	tdb.Exec(t, `SET sql_safe_updates = false`)
	tdb.Exec(t, `ALTER DATABASE defaultdb RENAME TO other_db`)
	tdb.Exec(t, `SET DATABASE = other_db`)
}
