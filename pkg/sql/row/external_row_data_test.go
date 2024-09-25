// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package row_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// TestExternalRowData is a sanity test that external row data (as configured by
// the External field of the table descriptor) is accessed correctly. It does so
// by creating two tables with one pointing to the other at a specific point in
// time.
func TestExternalRowData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	// Ensure that we always get the same connection in the SQL runner.
	sqlDB.SetMaxOpenConns(1)

	r := sqlutils.MakeSQLRunner(sqlDB)
	r.Exec(t, `CREATE TABLE t (k INT PRIMARY KEY, v1 INT, v2 INT, INDEX (v1))`)
	r.Exec(t, `CREATE TABLE t_copy (k INT PRIMARY KEY, v1 INT, v2 INT, INDEX (v1))`)

	// Insert some data into the original table, then record AOST, and insert
	// more data that shouldn't be visible via the external copy.
	r.Exec(t, `INSERT INTO t SELECT i, i, -i FROM generate_series(1, 3) AS g(i)`)
	asOf := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	r.Exec(t, `INSERT INTO t SELECT i, i, -i FROM generate_series(4, 6) AS g(i)`)

	// Modify the table descriptor for 't_copy' to have external row data from
	// 't'.
	var tableID int
	row := r.QueryRow(t, `SELECT 't'::REGCLASS::OID`)
	row.Scan(&tableID)
	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	require.NoError(t, execCfg.InternalDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		descriptors := txn.Descriptors()
		tn := tree.MakeTableNameWithSchema("defaultdb", "public", "t_copy")
		_, mut, err := descs.PrefixAndMutableTable(ctx, descriptors.MutableByName(txn.KV()), &tn)
		if err != nil {
			return err
		}
		require.NotNil(t, mut)
		mut.External = &descpb.ExternalRowData{
			AsOf:     asOf,
			TenantID: execCfg.Codec.TenantID,
			TableID:  descpb.ID(tableID),
		}
		return descriptors.WriteDesc(ctx, false /* kvTrace */, mut, txn.KV())
	}))

	// Try both execution engines since they have different fetcher
	// implementations.
	for _, vectorize := range []string{"on", "off"} {
		r.Exec(t, `SET vectorize = `+vectorize)
		for _, tc := range []struct {
			query    string
			expected [][]string
		}{
			{ // ScanRequest
				query:    `SELECT * FROM t_copy`,
				expected: [][]string{{"1", "1", "-1"}, {"2", "2", "-2"}, {"3", "3", "-3"}},
			},
			{ // ReverseScanRequest
				query:    `SELECT * FROM t_copy ORDER BY k DESC`,
				expected: [][]string{{"3", "3", "-3"}, {"2", "2", "-2"}, {"1", "1", "-1"}},
			},
			{ // GetRequests
				query:    `SELECT * FROM t_copy WHERE k = 2 OR k = 5`,
				expected: [][]string{{"2", "2", "-2"}},
			},
			{ // lookup join which might be served via the Streamer
				query:    `SELECT t_copy.k FROM t INNER LOOKUP JOIN t_copy ON t.k = t_copy.k`,
				expected: [][]string{{"1"}, {"2"}, {"3"}},
			},
			{ // index join which might be served via the Streamer
				query:    `SELECT * FROM t_copy WHERE v1 = 2 OR v1 = 5`,
				expected: [][]string{{"2", "2", "-2"}},
			},
		} {

			require.Equal(t, tc.expected, r.QueryStrMeta(
				t, fmt.Sprintf("vectorize=%v", vectorize), tc.query,
			))
		}
	}
}

// TestExternalRowDataDistSQL tests that the DistSQL physical planner can
// correctly place flows reading from external row data.
func TestExternalRowDataDistSQL(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// start a 7-node cluster
	// place data on nodes 2, 4, 6
	// check that eventually distsql plans place reads on these nodes

	// inspired from TestDistSQLRangeCachesIntegrationTest

	tc := serverutils.StartCluster(t, 7, /* numNodes */
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				UseDatabase: "defaultdb",
			},
		})
	defer tc.Stopper().Stop(context.Background())

	db0 := tc.ServerConn(0)
	var err error
	/*
		_, err := db0.Exec(`CREATE DATABASE test; USE test`)
		if err != nil {
			t.Fatal(err)
		}
	*/
	_, err = db0.Exec(`CREATE TABLE t (k INT PRIMARY KEY, v1 INT, v2 INT)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db0.Exec(`CREATE TABLE t_copy (k INT PRIMARY KEY, v1 INT, v2 INT)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db0.Exec(`INSERT INTO t VALUES (1), (2), (3)`)
	if err != nil {
		t.Fatal(err)
	}
	asOf := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	_, err = db0.Exec(`ALTER TABLE t SPLIT AT VALUES (1), (2), (3)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db0.Exec(`ALTER TABLE t SCATTER`)
	if err != nil {
		t.Fatal(err)
	}
	/*
		_, err = db0.Exec(fmt.Sprintf(`
		ALTER TABLE t EXPERIMENTAL_RELOCATE VALUES (ARRAY[%d], 1), (ARRAY[%d], 2), (ARRAY[%d], 3);
		`,
			tc.Server(1).GetFirstStoreID(),
			tc.Server(0).GetFirstStoreID(),
			tc.Server(2).GetFirstStoreID()))
		if err != nil {
			t.Fatal(err)
		}
	*/

	// does this warm up the range cache?
	res, err := db0.Query(`SELECT range_id, lease_holder FROM [SHOW RANGES FROM TABLE t WITH DETAILS]`)
	if err != nil {
		t.Fatal(err)
	}

	for res.Next() {
		var rangeID, leaseHolder int
		err := res.Scan(&rangeID, &leaseHolder)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Println(rangeID, leaseHolder)
	}

	// Modify the table descriptor for 't_copy' to have external row data from 't'.
	var tableID int
	row := db0.QueryRow(`SELECT 't'::REGCLASS::OID`)
	if err := row.Err(); err != nil {
		t.Fatal(err)
	}
	err = row.Scan(&tableID)
	if err != nil {
		t.Fatal(err)
	}

	execCfg0 := tc.ApplicationLayer(0).ExecutorConfig().(sql.ExecutorConfig)
	require.NoError(t, execCfg0.InternalDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		descriptors := txn.Descriptors()
		tn := tree.MakeTableNameWithSchema("defaultdb", "public", "t_copy")
		_, mut, err := descs.PrefixAndMutableTable(ctx, descriptors.MutableByName(txn.KV()), &tn)
		if err != nil {
			return err
		}
		require.NotNil(t, mut)
		mut.External = &descpb.ExternalRowData{
			AsOf:     asOf,
			TenantID: execCfg0.Codec.TenantID,
			TableID:  descpb.ID(tableID),
		}
		return descriptors.WriteDesc(ctx, false /* kvtrace */, mut, txn.KV())
	}))

	// now check query plans for both t and t_copy

	_, err = db0.Exec(`SET distsql = always`)
	if err != nil {
		t.Fatal(err)
	}

	res, err = db0.Query(`EXPLAIN (DISTSQL, JSON) SELECT count(*) FROM t`)
	if err != nil {
		t.Fatal(err)
	}
	for res.Next() {
		var line string
		err := res.Scan(&line)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Println(line)
	}

	res, err = db0.Query(`EXPLAIN (DISTSQL, JSON) SELECT count(*) FROM t_copy`)
	if err != nil {
		t.Fatal(err)
	}
	for res.Next() {
		var line string
		err := res.Scan(&line)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Println(line)
	}

	/*




		srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
		defer srv.Stopper().Stop(ctx)
		s := srv.ApplicationLayer()

		// Ensure that we always get the same connection in the SQL runner.
		sqlDB.SetMaxOpenConns(1)

		r := sqlutils.MakeSQLRunner(sqlDB)
		r.Exec(t, `CREATE TABLE t (k INT PRIMARY KEY, v1 INT, v2 INT, INDEX (v1))`)
		r.Exec(t, `CREATE TABLE t_copy (k INT PRIMARY KEY, v1 INT, v2 INT, INDEX (v1))`)

		// Insert some data into the original table, then record AOST, and insert
		// more data that shouldn't be visible via the external copy.
		r.Exec(t, `INSERT INTO t SELECT i, i, -i FROM generate_series(1, 3) AS g(i)`)
		asOf := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
		r.Exec(t, `INSERT INTO t SELECT i, i, -i FROM generate_series(4, 6) AS g(i)`)

		// Modify the table descriptor for 't_copy' to have external row data from
		// 't'.
		var tableID int
		row := r.QueryRow(t, `SELECT 't'::REGCLASS::OID`)
		row.Scan(&tableID)
		execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
		require.NoError(t, execCfg.InternalDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
			descriptors := txn.Descriptors()
			tn := tree.MakeTableNameWithSchema("defaultdb", "public", "t_copy")
			_, mut, err := descs.PrefixAndMutableTable(ctx, descriptors.MutableByName(txn.KV()), &tn)
			if err != nil {
				return err
			}
			require.NotNil(t, mut)
			mut.External = &descpb.ExternalRowData{
				AsOf:     asOf,
				TenantID: execCfg.Codec.TenantID,
				TableID:  descpb.ID(tableID),
			}
			return descriptors.WriteDesc(ctx, false kvTrace, mut, txn.KV())
		}))

		// Try both execution engines since they have different fetcher
		// implementations.
		for _, vectorize := range []string{"on", "off"} {
			r.Exec(t, `SET vectorize = `+vectorize)
			for _, tc := range []struct {
				query    string
				expected [][]string
			}{
				{ // ScanRequest
					query:    `SELECT * FROM t_copy`,
					expected: [][]string{{"1", "1", "-1"}, {"2", "2", "-2"}, {"3", "3", "-3"}},
				},
				{ // ReverseScanRequest
					query:    `SELECT * FROM t_copy ORDER BY k DESC`,
					expected: [][]string{{"3", "3", "-3"}, {"2", "2", "-2"}, {"1", "1", "-1"}},
				},
				{ // GetRequests
					query:    `SELECT * FROM t_copy WHERE k = 2 OR k = 5`,
					expected: [][]string{{"2", "2", "-2"}},
				},
				{ // lookup join which might be served via the Streamer
					query:    `SELECT t_copy.k FROM t INNER LOOKUP JOIN t_copy ON t.k = t_copy.k`,
					expected: [][]string{{"1"}, {"2"}, {"3"}},
				},
				{ // index join which might be served via the Streamer
					query:    `SELECT * FROM t_copy WHERE v1 = 2 OR v1 = 5`,
					expected: [][]string{{"2", "2", "-2"}},
				},
			} {

				require.Equal(t, tc.expected, r.QueryStrMeta(
					t, fmt.Sprintf("vectorize=%v", vectorize), tc.query,
				))
			}
		}
	*/
}
