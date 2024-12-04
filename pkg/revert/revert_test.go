// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revert

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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
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
			RevertSpansFanout(ctx,
				kvDB, rsCtx, []roachpb.Span{span}, targetTime, false, 10,
				func(context.Context, roachpb.Span) error {
					return nil
				}))
		verifyRevert()
	})
	t.Run("revert-without-callback", func(t *testing.T) {
		db.Exec(t, "DELETE FROM test WHERE k % 5 = 2")
		require.NoError(t,
			RevertSpansFanout(ctx,
				kvDB, rsCtx, []roachpb.Span{span}, targetTime, false, 10, nil))
		verifyRevert()
	})
	t.Run("revert-with-1-worker", func(t *testing.T) {
		db.Exec(t, "DELETE FROM test WHERE k % 5 = 2")
		db.Exec(t, "SET CLUSTER SETTING sql.revert.max_span_parallelism = 1")
		defer db.Exec(t, "RESET CLUSTER SETTING sql.revert.max_span_parallelism")
		require.NoError(t,
			RevertSpansFanout(ctx,
				kvDB, rsCtx, []roachpb.Span{span}, targetTime, false, 10,
				func(context.Context, roachpb.Span) error {
					return nil
				}))
		verifyRevert()

	})
	t.Run("revert-with-failing-callback", func(t *testing.T) {
		require.Error(t,
			RevertSpansFanout(ctx,
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

	req1 := kvpb.RevertRangeRequest{
		RequestHeader: kvpb.RequestHeader{Key: bootstrap.TestingUserTableDataMin(codec), EndKey: codec.TenantEndKey()},
		TargetTime:    hlc.Timestamp{WallTime: -1},
	}
	req2 := req1
	req2.IgnoreGcThreshold = true

	_, pErr := kv.SendWrapped(ctx, kvDB.NonTransactionalSender(), &req1)
	if !testutils.IsPError(pErr, "must be after replica GC threshold") {
		t.Fatalf(`expected "must be after replica GC threshold" error got: %+v`, pErr)
	}

	_, pErr = kv.SendWrapped(ctx, kvDB.NonTransactionalSender(), &req2)
	require.Nil(t, pErr)
}
