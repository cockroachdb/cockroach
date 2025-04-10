// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"context"
	"slices"
	"testing"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/replicationtestutils"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func TestSQLRowReader(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderDeadlock(t)
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	server, s, dbSource, dbDest := setupLogicalTestServer(t, ctx, base.TestClusterArgs{
		ServerArgs: testClusterBaseClusterArgs.ServerArgs,
	}, 1)
	defer server.Stopper().Stop(ctx)

	srcURL := replicationtestutils.GetExternalConnectionURI(t, s, s, serverutils.DBName("a"))

	// Create LDR stream from A to B
	var jobID jobspb.JobID
	dbDest.QueryRow(t, "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab", srcURL.String()).Scan(&jobID)

	// Write initial data and wait for replication to catch up
	dbSource.Exec(t, "INSERT INTO tab VALUES (10, 'one'), (20, 'two'), (30, 'three')")
	dbSource.Exec(t, "DELETE FROM tab WHERE pk = 20")
	now := s.Clock().Now()
	WaitUntilReplicatedTime(t, now, dbSource, jobID)

	// Create sqlRowReader for source table
	srcDesc := desctestutils.TestingGetPublicTableDescriptor(s.DB(), s.Codec(), "a", "tab")
	srcReader, err := newSQLRowReader(srcDesc)
	require.NoError(t, err)

	// Create sqlRowReader for destination table
	dstDesc := desctestutils.TestingGetPublicTableDescriptor(s.DB(), s.Codec(), "b", "tab")
	dstReader, err := newSQLRowReader(dstDesc)
	require.NoError(t, err)

	// Create test rows to look up

	// Use sqlRowReader to get prior rows from both tables
	db := s.InternalDB().(isql.DB)

	readRows := func(t *testing.T, db isql.DB, rows []tree.Datums, reader *sqlRowReader) map[int]priorRow {
		var result map[int]priorRow
		require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			result, err = reader.ReadRows(ctx, txn, rows)
			require.NoError(t, err)
			return err
		}))
		return result
	}

	readRowsSql := func(t *testing.T, db *sqlutils.SQLRunner, primaryKeys []int) map[int]priorRow {
		sqlRows := db.Query(t, `
			SELECT pk,
				   payload,
				   COALESCE(crdb_internal_origin_timestamp, crdb_internal_mvcc_timestamp) as timestamp,
				   crdb_internal_origin_timestamp IS NULL as is_local
			FROM tab
			WHERE pk = ANY($1::int[])
			ORDER BY pk`, pq.Array(primaryKeys))

		result := make(map[int]priorRow, len(primaryKeys))

		for sqlRows.Next() {
			var pk int
			var payload string
			var mvccTS string
			var isLocal bool
			require.NoError(t, sqlRows.Scan(&pk, &payload, &mvccTS, &isLocal))

			mvccDec, _, err := apd.NewFromString(mvccTS)
			require.NoError(t, err)

			logicalTimestamp, err := hlc.DecimalToHLC(mvccDec)
			require.NoError(t, err)

			result[slices.Index(primaryKeys, pk)] = priorRow{
				row:              []tree.Datum{tree.NewDInt(tree.DInt(pk)), tree.NewDString(payload)},
				logicalTimestamp: logicalTimestamp,
				isLocal:          isLocal,
			}
		}

		return result
	}

	testRows := []tree.Datums{
		{tree.NewDInt(0), tree.NewDString("one")},    // Existing row
		{tree.NewDInt(10), tree.NewDString("two")},   // Deleted row
		{tree.NewDInt(20), tree.NewDString("three")}, // Existing row
		{tree.NewDInt(30), tree.NewDString("four")},  // Never existed
	}
	primaryKeys := []int{0, 10, 20, 30}

	dbSource.CheckQueryResults(t, `SELECT * FROM tab`, [][]string{
		{"10", "one"},
		{"30", "three"},
	})
	require.Equal(t,
		readRows(t, db, testRows, srcReader),
		readRowsSql(t, dbSource, primaryKeys),
		"reading source did not yield expected rows")

	dbDest.CheckQueryResults(t, `SELECT * FROM tab`, [][]string{
		{"10", "one"},
		{"30", "three"},
	})
	require.Equal(t,
		readRows(t, db, testRows, dstReader),
		readRowsSql(t, dbDest, primaryKeys),
		"reading destination did not yield expected rows")
}
