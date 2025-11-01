// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"context"
	"go/types"
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
	srcSession := newInternalSession(t, s)
	defer srcSession.Close(ctx)
	srcReader, err := newSQLRowReader(ctx, srcDesc, srcSession)
	require.NoError(t, err)

	// Create sqlRowReader for destination table
	dstDesc := desctestutils.TestingGetPublicTableDescriptor(s.DB(), s.Codec(), "b", "tab")
	dstSession := newInternalSession(t, server.Server(0))
	defer dstSession.Close(ctx)
	dstReader, err := newSQLRowReader(ctx, dstDesc, dstSession)
	require.NoError(t, err)

	// Create test rows to look up

	// Use sqlRowReader to get prior rows from both tables
	db := s.InternalDB().(isql.DB)

	readRows := func(t *testing.T, db isql.DB, rows []tree.Datums, reader *sqlRowReader) map[int]priorRow {
		var result map[int]priorRow
		require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			result, err = reader.ReadRows(ctx, rows)
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

func TestSQLRowReaderWithArrayColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderDeadlock(t)
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	server, s, dbSource, dbDest := setupLogicalTestServer(t, ctx, base.TestClusterArgs{
		ServerArgs: testClusterBaseClusterArgs.ServerArgs,
	}, 1)
	defer server.Stopper().Stop(ctx)

	// Create tables with array column
	createStmt := `CREATE TABLE tab_array (pk int primary key, tags text[])`
	dbSource.Exec(t, createStmt)
	dbDest.Exec(t, createStmt)

	srcURL := replicationtestutils.GetExternalConnectionURI(t, s, s, serverutils.DBName("a"))

	var jobID jobspb.JobID
	dbDest.QueryRow(t, "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab_array ON $1 INTO TABLE tab_array", srcURL.String()).Scan(&jobID)

	// Insert test data
	dbSource.Exec(t, "INSERT INTO tab_array VALUES (1, ARRAY['foo', 'bar'])")
	dbSource.Exec(t, "INSERT INTO tab_array VALUES (2, ARRAY['baz'])")
	dbSource.Exec(t, "DELETE FROM tab_array WHERE pk = 2")

	now := s.Clock().Now()
	WaitUntilReplicatedTime(t, now, dbSource, jobID)

	// Create sqlRowReader for source table
	srcDesc := desctestutils.TestingGetPublicTableDescriptor(s.DB(), s.Codec(), "a", "tab_array")
	reader, err := newSQLRowReader(srcDesc, sessiondata.InternalExecutorOverride{})
	require.NoError(t, err)

	db := s.InternalDB().(isql.DB)

	// Test reading rows with array columns
	tags := tree.NewDArray(types.String)
	require.NoError(t, tags.Append(tree.NewDString("foo")))
	require.NoError(t, tags.Append(tree.NewDString("bar")))

	testRows := []tree.Datums{
		{tree.NewDInt(1), tags},
	}

	var result map[int]priorRow
	require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		result, err = reader.ReadRows(ctx, txn, testRows)
		return err
	}))

	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Contains(t, result, 0)

	row := result[0]
	require.Len(t, row.row, 2)
	require.Equal(t, tree.NewDInt(1), row.row[0])

	// Verify the array column was read correctly
	arrayCol, ok := row.row[1].(*tree.DArray)
	require.True(t, ok, "second column should be an array")
	require.Equal(t, 2, arrayCol.Len())
	require.Equal(t, "foo", string(tree.MustBeDString(arrayCol.Array[0])))
	require.Equal(t, "bar", string(tree.MustBeDString(arrayCol.Array[1])))
}
