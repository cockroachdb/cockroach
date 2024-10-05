// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestTableRollback(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, sqlDB, kv := serverutils.StartServer(t, base.TestServerArgs{UseDatabase: "test"})
	defer s.Stopper().Stop(context.Background())
	tt := s.ApplicationLayer()
	codec, sv := tt.Codec(), &tt.ClusterSettings().SV
	execCfg := tt.ExecutorConfig().(sql.ExecutorConfig)

	db := sqlutils.MakeSQLRunner(sqlDB)
	db.Exec(t, `CREATE DATABASE IF NOT EXISTS test`)
	db.Exec(t, `CREATE TABLE test (k INT PRIMARY KEY, rev INT DEFAULT 0, INDEX (rev))`)

	// Fill a table with some rows plus some revisions to those rows.
	const numRows = 1000
	db.Exec(t, `INSERT INTO test (k) SELECT generate_series(1, $1)`, numRows)
	db.Exec(t, `UPDATE test SET rev = 1 WHERE k % 3 = 0`)
	db.Exec(t, `DELETE FROM test WHERE k % 10 = 0`)
	db.Exec(t, `ALTER TABLE test SPLIT AT VALUES (30), (300), (501), (700)`)

	var ts string
	var before int
	db.QueryRow(t, `SELECT cluster_logical_timestamp(), xor_agg(k # rev) FROM test`).Scan(&ts, &before)
	targetTime, err := hlc.ParseHLC(ts)
	require.NoError(t, err)

	beforeNumRows := db.QueryStr(t, `SELECT count(*) FROM test`)

	// Make some more edits: delete some rows and edit others, insert into some of
	// the gaps made between previous rows, edit a large swath of rows and add a
	// large swath of new rows as well.
	db.Exec(t, `DELETE FROM test WHERE k % 5 = 2`)
	db.Exec(t, `INSERT INTO test (k, rev) SELECT generate_series(10, $1, 10), 10`, numRows)
	db.Exec(t, `INSERT INTO test (k, rev) SELECT generate_series($1+1, $1+500, 1), 500`, numRows)

	t.Run("simple-revert", func(t *testing.T) {

		const ignoreGC = false
		db.Exec(t, `UPDATE test SET rev = 2 WHERE k % 4 = 0`)
		db.Exec(t, `UPDATE test SET rev = 4 WHERE k > 150 and k < 350`)

		var edited, aost int
		db.QueryRow(t, `SELECT xor_agg(k # rev) FROM test`).Scan(&edited)
		require.NotEqual(t, before, edited)
		db.QueryRow(t, fmt.Sprintf(`SELECT xor_agg(k # rev) FROM test AS OF SYSTEM TIME %s`, ts)).Scan(&aost)
		require.Equal(t, before, aost)

		// Revert the table to ts.
		desc := desctestutils.TestingGetPublicTableDescriptor(kv, codec, "test", "test")
		desc.TableDesc().State = descpb.DescriptorState_OFFLINE // bypass the offline check.
		require.NoError(t, sql.RevertTables(ctx, kv, &execCfg, []catalog.TableDescriptor{desc}, targetTime, ignoreGC, 10))

		var reverted int
		db.QueryRow(t, `SELECT xor_agg(k # rev) FROM test`).Scan(&reverted)
		require.Equal(t, before, reverted, "expected reverted table after edits to match before")

		db.CheckQueryResults(t, `SELECT count(*) FROM test`, beforeNumRows)
	})

	t.Run("simple-delete-range-predicate", func(t *testing.T) {

		// Delete all keys with values after the targetTime
		desc := desctestutils.TestingGetPublicTableDescriptor(kv, codec, "test", "test")

		predicates := kvpb.DeleteRangePredicates{StartTime: targetTime}
		require.NoError(t, sql.DeleteTableWithPredicate(
			ctx, kv, codec, sv, execCfg.DistSender, desc, predicates, 10))

		db.CheckQueryResults(t, `SELECT count(*) FROM test`, beforeNumRows)
	})
}

// TestTableRollbackMultiTable is a regression test for a previous bug
// in RevertTables in which passing two tables to the function would
// cause a log.Fatal error from KV.
func TestTableRollbackMultiTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{UseDatabase: "test"})
	defer s.Stopper().Stop(context.Background())
	tt := s.ApplicationLayer()
	codec, kvDB := tt.Codec(), tt.DB()
	execCfg := tt.ExecutorConfig().(sql.ExecutorConfig)

	db := sqlutils.MakeSQLRunner(sqlDB)
	db.Exec(t, "CREATE DATABASE IF NOT EXISTS test")
	db.Exec(t, "CREATE TABLE test (k INT PRIMARY KEY)")
	db.Exec(t, "CREATE TABLE test2 (k INT PRIMARY KEY)")

	desc1 := desctestutils.TestingGetPublicTableDescriptor(kvDB, codec, "test", "test")
	desc2 := desctestutils.TestingGetPublicTableDescriptor(kvDB, codec, "test", "test2")

	tableID1 := desc1.GetID()
	tableID2 := desc2.GetID()

	const numRows = 1000
	db.Exec(t, `INSERT INTO test (k) SELECT generate_series(1, $1)`, numRows)
	db.Exec(t, `INSERT INTO test2 (k) SELECT generate_series(1, $1)`, numRows)

	before1, ts := fingerprintTableNoHistory(t, db, tableID1, "")
	before2, _ := fingerprintTableNoHistory(t, db, tableID2, ts)

	targetTime, err := hlc.ParseHLC(ts)
	require.NoError(t, err)

	// Delete some rows
	db.Exec(t, `DELETE FROM test WHERE k % 5 = 2`)
	db.Exec(t, `DELETE FROM test2 WHERE k % 5 = 2`)

	afterDelete1, _ := fingerprintTableNoHistory(t, db, tableID1, "")
	afterDelete2, _ := fingerprintTableNoHistory(t, db, tableID2, "")
	require.NotEqual(t, before1, afterDelete1)
	require.NotEqual(t, before2, afterDelete2)

	aost1, _ := fingerprintTableNoHistory(t, db, tableID1, ts)
	aost2, _ := fingerprintTableNoHistory(t, db, tableID2, ts)
	require.Equal(t, before1, aost1)
	require.Equal(t, before2, aost2)

	const ignoreGC = false

	// Bypass the offline check.
	desc1.TableDesc().State = descpb.DescriptorState_OFFLINE
	desc2.TableDesc().State = descpb.DescriptorState_OFFLINE

	require.NoError(t, sql.RevertTables(context.Background(), kvDB, &execCfg, []catalog.TableDescriptor{desc1, desc2}, targetTime, ignoreGC, 10))

	reverted1, _ := fingerprintTableNoHistory(t, db, tableID1, "")
	reverted2, _ := fingerprintTableNoHistory(t, db, tableID2, "")
	require.Equal(t, before1, reverted1, "expected reverted table after edits to match before")
	require.Equal(t, before2, reverted2, "expected reverted table after edits to match before")
}

// TestRevertSpansFanout tests RevertSpansFanout using the same
// sequence of modifications used in TestTableRollback.
//
// We use more splits and scatter the ranges.
func TestRevertSpansFanout(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{UseDatabase: "test"},
	})
	defer tc.Stopper().Stop(context.Background())
	s := tc.ApplicationLayer(0)
	sqlDB := tc.Conns[0]

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)

	db := sqlutils.MakeSQLRunner(sqlDB)
	db.Exec(t, "CREATE DATABASE IF NOT EXISTS test")
	db.Exec(t, "CREATE TABLE test (k INT PRIMARY KEY, rev INT DEFAULT 0, INDEX (rev))")

	kvDB := s.DB()
	desc := desctestutils.TestingGetPublicTableDescriptor(kvDB, execCfg.Codec, "test", "test")
	span := desc.TableSpan(execCfg.Codec)
	tableID := desc.GetID()

	// Fill a table with some rows plus some revisions to those rows.
	const (
		initRows  = 1000
		extraRows = 500
		splits    = 100
	)

	db.Exec(t, "INSERT INTO test (k) SELECT generate_series(1, $1)", initRows)
	db.Exec(t, "UPDATE test SET rev = 1 WHERE k % 3 = 0")
	db.Exec(t, "DELETE FROM test WHERE k % 10 = 0")
	before, ts := fingerprintTableNoHistory(t, db, tableID, "")
	targetTime, err := hlc.ParseHLC(ts)
	require.NoError(t, err)

	beforeNumRows := db.QueryStr(t, "SELECT count(*) FROM test")

	// Make some more edits: delete some rows and edit others, insert into some of
	// the gaps made between previous rows, edit a large swath of rows and add a
	// large swath of new rows as well.
	db.Exec(t, "DELETE FROM test WHERE k % 5 = 2")
	db.Exec(t, "INSERT INTO test (k, rev) SELECT generate_series(10, $1, 10), 10", initRows)
	db.Exec(t, "INSERT INTO test (k, rev) SELECT generate_series($1+1, $1+$2, 1), $2", initRows, extraRows)
	db.Exec(t, "UPDATE test SET rev = 2 WHERE k % 4 = 0")
	db.Exec(t, "UPDATE test SET rev = 4 WHERE k > 150 and k < 350")

	var splitPointsBuf strings.Builder
	splitIncrement := (initRows + extraRows) / splits
	for i := 0; i < 100; i++ {
		splitPoint := splitIncrement * i
		if i == 0 {
			fmt.Fprintf(&splitPointsBuf, "(%d)", splitPoint)
		} else {
			fmt.Fprintf(&splitPointsBuf, ", (%d)", splitPoint)
		}
	}

	// Split and scatter the table.
	db.Exec(t, fmt.Sprintf("ALTER TABLE test SPLIT AT VALUES %s", splitPointsBuf.String()))
	db.Exec(t, "ALTER TABLE test SCATTER")

	rsCtx, close := sql.MakeJobExecContext(ctx, "revert-spans", username.RootUserName(), &sql.MemoryMetrics{}, &execCfg)
	defer close()

	verifyRevert := func() {
		reverted, _ := fingerprintTableNoHistory(t, db, tableID, "")
		require.Equal(t, before, reverted, "expected reverted table after edits to match before")
		db.CheckQueryResults(t, "SELECT count(*) FROM test", beforeNumRows)
	}

	t.Run("revert-fanout-with-callback", func(t *testing.T) {
		require.NoError(t,
			sql.RevertSpansFanout(ctx,
				kvDB, rsCtx, []roachpb.Span{span}, targetTime, false, 10,
				func(context.Context, roachpb.Span) error {
					return nil
				}))
		verifyRevert()
	})
	t.Run("revert-without-callback", func(t *testing.T) {
		db.Exec(t, "DELETE FROM test WHERE k % 5 = 2")
		require.NoError(t,
			sql.RevertSpansFanout(ctx,
				kvDB, rsCtx, []roachpb.Span{span}, targetTime, false, 10, nil))
		verifyRevert()
	})
	t.Run("revert-with-1-worker", func(t *testing.T) {
		db.Exec(t, "DELETE FROM test WHERE k % 5 = 2")
		db.Exec(t, "SET CLUSTER SETTING sql.revert.max_span_parallelism = 1")
		defer db.Exec(t, "RESET CLUSTER SETTING sql.revert.max_span_parallelism")
		require.NoError(t,
			sql.RevertSpansFanout(ctx,
				kvDB, rsCtx, []roachpb.Span{span}, targetTime, false, 10,
				func(context.Context, roachpb.Span) error {
					return nil
				}))
		verifyRevert()

	})
	t.Run("revert-with-failing-callback", func(t *testing.T) {
		require.Error(t,
			sql.RevertSpansFanout(ctx,
				kvDB, rsCtx, []roachpb.Span{span}, targetTime, false, 10,
				func(context.Context, roachpb.Span) error {
					return errors.New("callback failed")
				}), "callbackFailed")
		// No use verifying, the revert failed and we didn't
		// make any changes anyway.
	})
}

// fingerprintTableNoHistory returns the fingerprint of the given
// table as well as the time the fingerprint was taken at.
//
// If a non-empty ts string is specified, the fingerprint will be
// taken at that timestamp.
func fingerprintTableNoHistory(
	t *testing.T, db *sqlutils.SQLRunner, tableID catid.DescID, ts string,
) (int, string) {
	t.Helper()
	query := "SELECT cluster_logical_timestamp(), * FROM crdb_internal.fingerprint(crdb_internal.table_span($1), 0::timestamptz, false)"
	if ts != "" {
		query = fmt.Sprintf("%s AS OF SYSTEM TIME %s", query, ts)
	}

	var fingerprint int
	var timestamp string
	db.QueryRow(t, query, tableID).Scan(&timestamp, &fingerprint)
	return fingerprint, timestamp
}

func TestRevertGCThreshold(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	codec := srv.ApplicationLayer().Codec()

	req := &kvpb.RevertRangeRequest{
		RequestHeader: kvpb.RequestHeader{Key: bootstrap.TestingUserTableDataMin(codec), EndKey: codec.TenantEndKey()},
		TargetTime:    hlc.Timestamp{WallTime: -1},
	}
	_, pErr := kv.SendWrapped(ctx, kvDB.NonTransactionalSender(), req)
	if !testutils.IsPError(pErr, "must be after replica GC threshold") {
		t.Fatalf(`expected "must be after replica GC threshold" error got: %+v`, pErr)
	}
	req.IgnoreGcThreshold = true
	_, pErr = kv.SendWrapped(ctx, kvDB.NonTransactionalSender(), req)
	require.Nil(t, pErr)
}
