// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestRevertTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	s, sqlDB, kv := serverutils.StartServer(
		t, base.TestServerArgs{UseDatabase: "test"})
	defer s.Stopper().Stop(context.Background())
	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)

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
	targetTime, err := sql.ParseHLC(ts)
	require.NoError(t, err)

	const ignoreGC = false

	t.Run("simple", func(t *testing.T) {
		// Make some more edits: delete some rows and edit others, insert into some of
		// the gaps made between previous rows, edit a large swath of rows and add a
		// large swath of new rows as well.
		db.Exec(t, `UPDATE test SET rev = 2 WHERE k % 4 = 0`)
		db.Exec(t, `DELETE FROM test WHERE k % 5 = 2`)
		db.Exec(t, `INSERT INTO test (k, rev) SELECT generate_series(10, $1, 10), 10`, numRows)
		db.Exec(t, `UPDATE test SET rev = 4 WHERE k > 150 and k < 350`)
		db.Exec(t, `INSERT INTO test (k, rev) SELECT generate_series($1+1, $1+500, 1), 500`, numRows)

		var edited, aost int
		db.QueryRow(t, `SELECT xor_agg(k # rev) FROM test`).Scan(&edited)
		require.NotEqual(t, before, edited)
		db.QueryRow(t, fmt.Sprintf(`SELECT xor_agg(k # rev) FROM test AS OF SYSTEM TIME %s`, ts)).Scan(&aost)
		require.Equal(t, before, aost)

		// Revert the table to ts.
		desc := catalogkv.TestingGetTableDescriptor(kv, keys.SystemSQLCodec, "test", "test")
		desc.TableDesc().State = descpb.DescriptorState_OFFLINE // bypass the offline check.
		require.NoError(t, sql.RevertTables(context.Background(), kv, &execCfg, []catalog.TableDescriptor{desc}, targetTime, ignoreGC, 10))

		var reverted int
		db.QueryRow(t, `SELECT xor_agg(k # rev) FROM test`).Scan(&reverted)
		require.Equal(t, before, reverted, "expected reverted table after edits to match before")
	})

	t.Run("interleaved", func(t *testing.T) {
		db.Exec(t, `CREATE TABLE child (a INT, b INT, rev INT DEFAULT 0, INDEX (rev), PRIMARY KEY (a, b)) INTERLEAVE IN PARENT test (a)`)
		db.Exec(t, `INSERT INTO child (a, b) SELECT generate_series(1, $1, 2), generate_series(2, $1, 2)`, numRows)
		db.Exec(t, `UPDATE child SET rev = 1 WHERE a % 3 = 0`)

		db.QueryRow(t, `SELECT cluster_logical_timestamp() FROM test`).Scan(&ts)
		targetTime, err = sql.ParseHLC(ts)
		require.NoError(t, err)

		var beforeChild int
		db.QueryRow(t, `SELECT xor_agg(a # b # rev) FROM child`).Scan(&beforeChild)

		db.Exec(t, `UPDATE child SET rev = 2 WHERE a % 5 = 0`)
		db.Exec(t, `UPDATE child SET rev = 3 WHERE a > 450 and a < 700`)
		db.Exec(t, `DELETE FROM child WHERE a % 7 = 0`)

		// Revert the table to ts.
		desc := catalogkv.TestingGetTableDescriptor(kv, keys.SystemSQLCodec, "test", "test")
		desc.TableDesc().State = descpb.DescriptorState_OFFLINE
		child := catalogkv.TestingGetTableDescriptor(kv, keys.SystemSQLCodec, "test", "child")
		child.TableDesc().State = descpb.DescriptorState_OFFLINE
		t.Run("reject only parent", func(t *testing.T) {
			require.Error(t, sql.RevertTables(ctx, kv, &execCfg, []catalog.TableDescriptor{desc}, targetTime, ignoreGC, 10))
		})
		t.Run("reject only child", func(t *testing.T) {
			require.Error(t, sql.RevertTables(ctx, kv, &execCfg, []catalog.TableDescriptor{child}, targetTime, ignoreGC, 10))
		})

		t.Run("rollback parent and child", func(t *testing.T) {
			require.NoError(t, sql.RevertTables(ctx, kv, &execCfg, []catalog.TableDescriptor{desc, child}, targetTime, ignoreGC, sql.RevertTableDefaultBatchSize))

			var reverted, revertedChild int
			db.QueryRow(t, `SELECT xor_agg(k # rev) FROM test`).Scan(&reverted)
			require.Equal(t, before, reverted, "expected reverted table after edits to match before")
			db.QueryRow(t, `SELECT xor_agg(a # b # rev) FROM child`).Scan(&revertedChild)
			require.Equal(t, beforeChild, revertedChild, "expected reverted table after edits to match before")
		})
	})
}

func TestRevertGCThreshold(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	kvDB := tc.Server(0).DB()

	req := &roachpb.RevertRangeRequest{
		RequestHeader:                       roachpb.RequestHeader{Key: keys.UserTableDataMin, EndKey: keys.MaxKey},
		TargetTime:                          hlc.Timestamp{WallTime: -1},
		EnableTimeBoundIteratorOptimization: true,
	}
	_, pErr := kv.SendWrapped(ctx, kvDB.NonTransactionalSender(), req)
	if !testutils.IsPError(pErr, "must be after replica GC threshold") {
		t.Fatalf(`expected "must be after replica GC threshold" error got: %+v`, pErr)
	}
	req.IgnoreGcThreshold = true
	_, pErr = kv.SendWrapped(ctx, kvDB.NonTransactionalSender(), req)
	require.Nil(t, pErr)
}
