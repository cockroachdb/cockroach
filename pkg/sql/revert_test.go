// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
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
	db.Exec(t, `INSERT INTO test (k, rev) SELECT generate_series(10, $1, 10), 10`, numRows)
	db.Exec(t, `INSERT INTO test (k, rev) SELECT generate_series($1+1, $1+500, 1), 500`, numRows)

	// Delete all keys with values after the targetTime
	desc := desctestutils.TestingGetPublicTableDescriptor(kv, codec, "test", "test")

	predicates := kvpb.DeleteRangePredicates{StartTime: targetTime}
	require.NoError(t, sql.DeleteTableWithPredicate(
		ctx, kv, codec, sv, execCfg.DistSender, desc.GetID(), predicates, 10))

	db.CheckQueryResults(t, `SELECT count(*) FROM test`, beforeNumRows)

}
