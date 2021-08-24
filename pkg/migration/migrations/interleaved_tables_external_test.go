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
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestInterleavedTableMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: 1,
					BinaryVersionOverride:          clusterversion.ByKey(clusterversion.PreventNewInterleavedTables - 1),
				},
			},
		},
	})

	defer tc.Stopper().Stop(ctx)
	db := tc.ServerConn(0)
	tdb := sqlutils.MakeSQLRunner(db)
	tdb.ExecSucceedsSoon(t, "CREATE TABLE customers (id INT PRIMARY KEY, name STRING(50));")
	tdb.ExecSucceedsSoon(t, `CREATE TABLE orders (
   customer INT,
   id INT,
   total DECIMAL(20, 5),
   PRIMARY KEY (customer, id),
   CONSTRAINT fk_customer FOREIGN KEY (customer) REFERENCES customers
 ) INTERLEAVE IN PARENT customers (customer);`)
	// Migration to the second phase should fail, since the pre-condition was violated.
	tdb.ExpectErr(t,
		"pq: verifying precondition for version 21.1-140: interleaved tables are no longer supported at this version, please drop or uninterleave any tables visible using crdb_internal.interleaved.",
		"SET CLUSTER SETTING version = $1",
		clusterversion.ByKey(clusterversion.EnsureNoInterleavedTables).String())
	// Migration to the first phase should be fine, since it only blocks creating
	// new interleaved tables.
	tdb.ExecSucceedsSoon(t,
		"SET CLUSTER SETTING version = $1",
		clusterversion.ByKey(clusterversion.PreventNewInterleavedTables).String())
	// Migration to the next phase without interleaved tables should fail.
	tdb.ExpectErr(t,
		"pq: verifying precondition for version 21.1-140: interleaved tables are no longer supported at this version, please drop or uninterleave any tables visible using crdb_internal.interleaved.",
		"SET CLUSTER SETTING version = $1",
		clusterversion.ByKey(clusterversion.EnsureNoInterleavedTables).String())
	// Next drop the old descriptor and wait for the jobs to complete.
	_, err := db.Exec(`ALTER RANGE default CONFIGURE ZONE USING gc.ttlseconds=1;`)
	require.NoError(t, err)
	tdb.ExecSucceedsSoon(t, `ALTER TABLE ORDERS ALTER PRIMARY KEY USING COLUMNS (customer, id);`)
	testutils.SucceedsSoon(t, func() error {
		row := tdb.QueryRow(t,
			`SELECT count(*) from [show jobs]  where status not in ('succeeded', 'failed', 'aborted')`)
		count := 0
		row.Scan(&count)
		if count != 0 {
			return errors.New("Waiting for GC jobs to complete")
		}
		return nil
	})
	// Check that creation of interleaved tables is fully disabled.
	tdb.Exec(t, "CREATE TABLE customers2 (id INT PRIMARY KEY, name STRING(50));")
	tdb.Exec(t,
		`CREATE TABLE orders2 (
   customer INT,
   id INT,
   total DECIMAL(20, 5),
   PRIMARY KEY (customer, id),
   CONSTRAINT fk_customer FOREIGN KEY (customer) REFERENCES customers2
 ) INTERLEAVE IN PARENT customers2 (customer);`)
	// Check that no interleaved tables were created and the syntax is now a no-op.
	tdb.CheckQueryResults(t,
		"SELECT EXISTS(SELECT * FROM crdb_internal.interleaved);",
		[][]string{
			{"false"},
		})
	// Migration to next phase should succeed.
	tdb.ExecSucceedsSoon(t, "SET CLUSTER SETTING version = $1",
		clusterversion.ByKey(clusterversion.EnsureNoInterleavedTables).String())
	// Check that creation of interleaved tables is a no-op.
	tdb.Exec(t,
		`CREATE TABLE orders3 (
   customer INT,
   id INT,
   total DECIMAL(20, 5),
   PRIMARY KEY (customer, id),
   CONSTRAINT fk_customer FOREIGN KEY (customer) REFERENCES customers2
 ) INTERLEAVE IN PARENT customers2 (customer);`)
	// Check that no interleaved tables were created and the syntax is now a no-op.
	tdb.CheckQueryResults(t,
		"SELECT EXISTS(SELECT * FROM crdb_internal.interleaved);",
		[][]string{
			{"false"},
		})
}
