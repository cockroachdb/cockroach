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
			require.Equal(t, tc.expected, r.QueryStr(t, tc.query))
		}
	}
}
