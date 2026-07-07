// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/replicationtestutils"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
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

	readRows := func(t *testing.T, db isql.DB, rows []tree.Datums, reader sqlRowReader) map[int]priorRow {
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
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	runner := sqlutils.MakeSQLRunner(sqlDB)

	// Create tables with array column
	createStmt := `CREATE TABLE tab_array (pk int[] primary key, value int)`
	runner.Exec(t, createStmt)

	// Insert test data
	runner.Exec(t, "INSERT INTO tab_array VALUES (ARRAY['1', '2'], 10), (ARRAY['1'], 20)")

	// Create sqlRowReader for source table
	desc := desctestutils.TestingGetPublicTableDescriptor(s.DB(), s.Codec(), "defaultdb", "tab_array")
	session := newInternalSession(t, s)
	defer session.Close(ctx)
	reader, err := newSQLRowReader(ctx, desc, session)
	require.NoError(t, err)

	db := s.InternalDB().(isql.DB)

	testRows := []tree.Datums{
		{
			tree.NewDArrayFromDatums(types.Int, tree.Datums{tree.NewDInt(1), tree.NewDInt(2)}),
			tree.NewDInt(0),
		},
		{
			tree.NewDArrayFromDatums(types.Int, tree.Datums{tree.NewDInt(1)}),
			tree.NewDInt(0),
		},
	}

	var result map[int]priorRow
	require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		result, err = reader.ReadRows(ctx, testRows)
		return err
	}))

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result, 2)
	require.Equal(t, result[0].row[1], tree.NewDInt(10))
	require.Equal(t, result[1].row[1], tree.NewDInt(20))
}

// TestSQLRowReaderResumeDropsOriginTimestamp is a regression test for a bug
// where the KV streamer drops ReturnRawMVCCValues when a Get is deferred into a
// resume batch. The reader selects crdb_internal_origin_timestamp, which is
// recovered from the raw MVCC value header; if the resumed Get returns the
// value without its MVCC header the origin timestamp decodes as NULL and the row
// is misreported as locally written (isLocal=true). The txn writer's refresh
// path then compares against the local row's mvcc timestamp instead of its
// (older) origin timestamp, so the local row wins last-write-wins and the more
// recent incoming write loses and is silently dropped.
//
// The resume is triggered by the streamer's cold response-size estimator: the
// first batch of a fresh streamer targets len(keys) * initial_avg_response_size
// (default 1KiB/key, see kvstreamer.DefaultInitialAvgResponseSize), with no
// schema awareness. With multiple keys in a single range whose rows each exceed
// 1KiB, the head-of-line Get consumes the whole batch budget and the remaining
// Get(s) are deferred to a resume batch. Each ReadRows call spins up a fresh
// streamer, so the estimator resets every refresh and this pagination is
// reliable for LDR-sized rows. On buggy code the assertions below fail because
// the origin timestamp is lost on the resumed Get.
func TestSQLRowReaderResumeDropsOriginTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderDeadlock(t)
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	// Large SQL memory pool so the streamer isn't hitting the root memory budget.
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		SQLMemoryPoolSize: 1 << 30, /* 1GiB */
	})
	defer s.Stopper().Stop(ctx)
	sqlRunner := sqlutils.MakeSQLRunner(sqlDB)
	sqlRunner.Exec(t, "CREATE TABLE defaultdb.tab (pk INT PRIMARY KEY, payload STRING)")

	session := newInternalSession(t, s)
	defer session.Close(ctx)

	desc := desctestutils.TestingGetPublicTableDescriptor(s.DB(), s.Codec(), "defaultdb", "tab")
	writer, err := newSQLRowWriter(ctx, desc, session)
	require.NoError(t, err)

	// Write several rows, each with an origin timestamp and a payload well above
	// the streamer's 1KiB/key cold estimate. The rows share a single range so
	// they coalesce into one batch, guaranteeing that the head-of-line Get
	// exhausts the budget and the rest are resumed.
	const payloadSize = 4 << 10 // 4KiB, comfortably over the 1KiB estimate.
	const numRows = 4
	payload := strings.Repeat("a", payloadSize)

	originTS := s.Clock().Now()
	pks := make([]int, 0, numRows)
	testRows := make([]tree.Datums, 0, numRows)
	for i := 0; i < numRows; i++ {
		pk := 10 + i
		require.NoError(t, writer.InsertRow(ctx, originTS,
			tree.Datums{tree.NewDInt(tree.DInt(pk)), tree.NewDString(payload)}))
		pks = append(pks, pk)
		testRows = append(testRows, tree.Datums{tree.NewDInt(tree.DInt(pk)), tree.DNull})
	}

	// Populate the range cache.
	sqlRunner.Exec(t, "SELECT count(*) FROM defaultdb.tab")

	reader, err := newSQLRowReader(ctx, desc, session)
	require.NoError(t, err)

	db := s.InternalDB().(isql.DB)
	var result map[int]priorRow
	require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		result, err = reader.ReadRows(ctx, testRows)
		return err
	}))
	require.Len(t, result, numRows)

	// Every row was written with an origin timestamp, so none should be
	// reported as locally written and each should carry the origin timestamp.
	for i, pk := range pks {
		pr, ok := result[i]
		require.Truef(t, ok, "row for pk %d missing from result", pk)
		require.Falsef(t, pr.isLocal, "row for pk %d wrongly reported as local", pk)
		require.Equalf(t, originTS, pr.logicalTimestamp,
			"row for pk %d lost its origin timestamp across a streamer resume", pk)
	}
}
