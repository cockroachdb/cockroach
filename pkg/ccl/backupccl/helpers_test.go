// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"context"
	gosql "database/sql"
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/workload/bank"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadsql"
	"github.com/cockroachdb/errors"
	"github.com/kr/pretty"
)

const (
	singleNode                  = 1
	MultiNode                   = 3
	backupRestoreDefaultRanges  = 10
	backupRestoreRowPayloadSize = 100
	LocalFoo                    = "nodelocal://0/foo"
)

func backupRestoreTestSetupWithParams(
	t testing.TB,
	clusterSize int,
	numAccounts int,
	init func(tc *testcluster.TestCluster),
	params base.TestClusterArgs,
) (
	ctx context.Context,
	tc *testcluster.TestCluster,
	sqlDB *sqlutils.SQLRunner,
	tempDir string,
	cleanup func(),
) {
	ctx = context.Background()

	dir, dirCleanupFn := testutils.TempDir(t)
	params.ServerArgs.ExternalIODir = dir
	params.ServerArgs.UseDatabase = "data"
	tc = testcluster.StartTestCluster(t, clusterSize, params)
	init(tc)

	const payloadSize = 100
	splits := 10
	if numAccounts == 0 {
		splits = 0
	}
	bankData := bank.FromConfig(numAccounts, numAccounts, payloadSize, splits)

	sqlDB = sqlutils.MakeSQLRunner(tc.Conns[0])
	sqlDB.Exec(t, `CREATE DATABASE data`)
	l := workloadsql.InsertsDataLoader{BatchSize: 1000, Concurrency: 4}
	if _, err := workloadsql.Setup(ctx, sqlDB.DB.(*gosql.DB), bankData, l); err != nil {
		t.Fatalf("%+v", err)
	}

	if err := tc.WaitForFullReplication(); err != nil {
		t.Fatal(err)
	}

	cleanupFn := func() {
		tc.Stopper().Stop(ctx) // cleans up in memory storage's auxiliary dirs
		dirCleanupFn()         // cleans up dir, which is the nodelocal:// storage
	}

	return ctx, tc, sqlDB, dir, cleanupFn
}

func BackupRestoreTestSetup(
	t testing.TB, clusterSize int, numAccounts int, init func(*testcluster.TestCluster),
) (
	ctx context.Context,
	tc *testcluster.TestCluster,
	sqlDB *sqlutils.SQLRunner,
	tempDir string,
	cleanup func(),
) {
	return backupRestoreTestSetupWithParams(t, clusterSize, numAccounts, init, base.TestClusterArgs{})
}

func backupRestoreTestSetupEmpty(
	t testing.TB, clusterSize int, tempDir string, init func(*testcluster.TestCluster),
) (ctx context.Context, tc *testcluster.TestCluster, sqlDB *sqlutils.SQLRunner, cleanup func()) {
	return backupRestoreTestSetupEmptyWithParams(t, clusterSize, tempDir, init, base.TestClusterArgs{})
}

func verifyBackupRestoreStatementResult(
	t *testing.T, sqlDB *sqlutils.SQLRunner, query string, args ...interface{},
) error {
	t.Helper()
	rows := sqlDB.Query(t, query, args...)

	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	if e, a := columns, []string{
		"job_id", "status", "fraction_completed", "rows", "index_entries", "bytes",
	}; !reflect.DeepEqual(e, a) {
		return errors.Errorf("unexpected columns:\n%s", strings.Join(pretty.Diff(e, a), "\n"))
	}

	type job struct {
		id                int64
		status            string
		fractionCompleted float32
	}

	var expectedJob job
	var actualJob job
	var unused int64

	if !rows.Next() {
		return errors.New("zero rows in result")
	}
	if err := rows.Scan(
		&actualJob.id, &actualJob.status, &actualJob.fractionCompleted, &unused, &unused, &unused,
	); err != nil {
		return err
	}
	if rows.Next() {
		return errors.New("more than one row in result")
	}

	sqlDB.QueryRow(t,
		`SELECT job_id, status, fraction_completed FROM crdb_internal.jobs WHERE job_id = $1`, actualJob.id,
	).Scan(
		&expectedJob.id, &expectedJob.status, &expectedJob.fractionCompleted,
	)

	if e, a := expectedJob, actualJob; !reflect.DeepEqual(e, a) {
		return errors.Errorf("result does not match system.jobs:\n%s",
			strings.Join(pretty.Diff(e, a), "\n"))
	}

	return nil
}

func generateInterleavedData(
	sqlDB *sqlutils.SQLRunner, t *testing.T, numAccounts int,
) (int, []string) {
	_ = sqlDB.Exec(t, `SET CLUSTER SETTING kv.range_merge.queue_enabled = false`)
	// TODO(dan): The INTERLEAVE IN PARENT clause currently doesn't allow the
	// `db.table` syntax. Fix that and use it here instead of `SET DATABASE`.
	_ = sqlDB.Exec(t, `SET DATABASE = data`)
	_ = sqlDB.Exec(t, `CREATE TABLE strpk (id string, v int, primary key (id, v)) PARTITION BY LIST (id) ( PARTITION ab VALUES IN (('a'), ('b')), PARTITION xy VALUES IN (('x'), ('y')) );`)
	_ = sqlDB.Exec(t, `ALTER PARTITION ab OF TABLE strpk CONFIGURE ZONE USING gc.ttlseconds = 60`)
	_ = sqlDB.Exec(t, `INSERT INTO strpk VALUES ('a', 1), ('a', 2), ('x', 100), ('y', 101)`)
	const numStrPK = 4
	_ = sqlDB.Exec(t, `CREATE TABLE strpkchild (a string, b int, c int, primary key (a, b, c)) INTERLEAVE IN PARENT strpk (a, b)`)
	// i0 interleaves in parent with a, and has a multi-col PK of its own b, c
	_ = sqlDB.Exec(t, `CREATE TABLE i0 (a INT, b INT, c INT, PRIMARY KEY (a, b, c)) INTERLEAVE IN PARENT bank (a)`)
	// Split at at a _strict prefix_ of the cols in i_0's PK
	_ = sqlDB.Exec(t, `ALTER TABLE i0 SPLIT AT VALUES (1, 1)`)
	// i0_0 interleaves into i0.
	_ = sqlDB.Exec(t, `CREATE TABLE i0_0 (a INT, b INT, c INT, d INT, PRIMARY KEY (a, b, c, d)) INTERLEAVE IN PARENT i0 (a, b, c)`)
	_ = sqlDB.Exec(t, `CREATE TABLE i1 (a INT, b CHAR, PRIMARY KEY (a, b)) INTERLEAVE IN PARENT bank (a)`)
	_ = sqlDB.Exec(t, `CREATE TABLE i2 (a INT, b CHAR, PRIMARY KEY (a, b)) INTERLEAVE IN PARENT bank (a)`)
	// The bank table has numAccounts accounts, put 2x that in i0, 3x in i0_0,
	// and 4x in i1.
	totalRows := numAccounts + numStrPK
	for i := 0; i < numAccounts; i++ {
		_ = sqlDB.Exec(t, `INSERT INTO i0 VALUES ($1, 1, 1), ($1, 2, 2)`, i)
		totalRows += 2
		_ = sqlDB.Exec(t, `INSERT INTO i0_0 VALUES ($1, 1, 1, 1), ($1, 2, 2, 2), ($1, 3, 3, 3)`, i)
		totalRows += 3
		_ = sqlDB.Exec(t, `INSERT INTO i1 VALUES ($1, 'a'), ($1, 'b'), ($1, 'c'), ($1, 'd')`, i)
		totalRows += 4
		_ = sqlDB.Exec(t, `INSERT INTO i2 VALUES ($1, 'e'), ($1, 'f'), ($1, 'g'), ($1, 'h')`, i)
		totalRows += 4
	}
	// Split some rows to attempt to exercise edge conditions in the key rewriter.
	_ = sqlDB.Exec(t, `ALTER TABLE i0 SPLIT AT SELECT * from i0 where a % 2 = 0 LIMIT $1`, numAccounts)
	_ = sqlDB.Exec(t, `ALTER TABLE i0_0 SPLIT AT SELECT * from i0_0 LIMIT $1`, numAccounts)
	_ = sqlDB.Exec(t, `ALTER TABLE i1 SPLIT AT SELECT * from i1 WHERE a % 3 = 0`)
	_ = sqlDB.Exec(t, `ALTER TABLE i2 SPLIT AT SELECT * from i2 WHERE a % 5 = 0`)
	// Truncate will allocate a new ID for i1. At that point the splits we created
	// above will still exist, but will contain the old table ID. Since the table
	// does not exist anymore, it will not be in the backup, so the rewriting will
	// not have a configured rewrite for that part of those splits, but we still
	// expect RESTORE to succeed.
	_ = sqlDB.Exec(t, `TRUNCATE i1`)
	for i := 0; i < numAccounts; i++ {
		_ = sqlDB.Exec(t, `INSERT INTO i1 VALUES ($1, 'a'), ($1, 'b'), ($1, 'c'), ($1, 'd')`, i)
	}
	tableNames := []string{
		"strpk",
		"strpkchild",
		"i0",
		"i0_0",
		"i1",
		"i2",
	}
	return totalRows, tableNames
}

func backupRestoreTestSetupEmptyWithParams(
	t testing.TB,
	clusterSize int,
	dir string,
	init func(tc *testcluster.TestCluster),
	params base.TestClusterArgs,
) (ctx context.Context, tc *testcluster.TestCluster, sqlDB *sqlutils.SQLRunner, cleanup func()) {
	ctx = context.Background()

	params.ServerArgs.ExternalIODir = dir
	tc = testcluster.StartTestCluster(t, clusterSize, params)
	init(tc)

	sqlDB = sqlutils.MakeSQLRunner(tc.Conns[0])

	cleanupFn := func() {
		tc.Stopper().Stop(ctx) // cleans up in memory storage's auxiliary dirs
	}

	return ctx, tc, sqlDB, cleanupFn
}

func createEmptyCluster(
	t testing.TB, clusterSize int,
) (sqlDB *sqlutils.SQLRunner, tempDir string, cleanup func()) {
	ctx := context.Background()

	dir, dirCleanupFn := testutils.TempDir(t)
	params := base.TestClusterArgs{}
	params.ServerArgs.ExternalIODir = dir
	tc := testcluster.StartTestCluster(t, clusterSize, params)

	sqlDB = sqlutils.MakeSQLRunner(tc.Conns[0])

	cleanupFn := func() {
		tc.Stopper().Stop(ctx) // cleans up in memory storage's auxiliary dirs
		dirCleanupFn()         // cleans up dir, which is the nodelocal:// storage
	}

	return sqlDB, dir, cleanupFn
}
