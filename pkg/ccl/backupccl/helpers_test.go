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
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	roachpb "github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/workload/bank"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadsql"
	"github.com/cockroachdb/errors"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
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
	if len(params.ServerArgsPerNode) > 0 {
		for i := range params.ServerArgsPerNode {
			param := params.ServerArgsPerNode[i]
			param.ExternalIODir = dir
			param.UseDatabase = "data"
			params.ServerArgsPerNode[i] = param
		}
	}

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
	t testing.TB,
	clusterSize int,
	tempDir string,
	init func(*testcluster.TestCluster),
	params base.TestClusterArgs,
) (ctx context.Context, tc *testcluster.TestCluster, sqlDB *sqlutils.SQLRunner, cleanup func()) {
	return backupRestoreTestSetupEmptyWithParams(t, clusterSize, tempDir, init, params)
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
		if err := rows.Err(); err != nil {
			return err
		}
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
	if len(params.ServerArgsPerNode) > 0 {
		for i := range params.ServerArgsPerNode {
			param := params.ServerArgsPerNode[i]
			param.ExternalIODir = dir
			params.ServerArgsPerNode[i] = param
		}
	}
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

// getStatsQuery returns a SQL query that will return the properties of the
// statistics on a table that are expected to remain the same after being
// restored on a new cluster.
func getStatsQuery(tableName string) string {
	return fmt.Sprintf(`SELECT
	  statistics_name,
	  column_names,
	  row_count,
	  distinct_count,
	  null_count
	FROM [SHOW STATISTICS FOR TABLE %s]`, tableName)
}

// injectStats directly injects some arbitrary statistic into a given table for
// a specified column.
// See injectStatsWithRowCount.
func injectStats(
	t *testing.T, sqlDB *sqlutils.SQLRunner, tableName string, columnName string,
) [][]string {
	return injectStatsWithRowCount(t, sqlDB, tableName, columnName, 100 /* rowCount */)
}

// injectStatsWithRowCount directly injects some statistics specifying some row
// count for a column in the given table.
// N.B. This should be used in backup testing over CREATE STATISTICS since it
// ensures that the stats cache will be up to date during a subsequent BACKUP.
func injectStatsWithRowCount(
	t *testing.T, sqlDB *sqlutils.SQLRunner, tableName string, columnName string, rowCount int,
) [][]string {
	sqlDB.Exec(t, fmt.Sprintf(`ALTER TABLE %s INJECT STATISTICS '[
	{
		"columns": ["%s"],
		"created_at": "2018-01-01 1:00:00.00000+00:00",
		"row_count": %d,
		"distinct_count": %d
	}
	]'`, tableName, columnName, rowCount, rowCount))
	return sqlDB.QueryStr(t, getStatsQuery(tableName))
}

func makeInsecureHTTPServer(t *testing.T) (string, func()) {
	t.Helper()

	const badHeadResponse = "bad-head-response"

	tmp, dirCleanup := testutils.TempDir(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		localfile := filepath.Join(tmp, filepath.Base(r.URL.Path))
		switch r.Method {
		case "PUT":
			f, err := os.Create(localfile)
			if err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			defer f.Close()
			if _, err := io.Copy(f, r.Body); err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			w.WriteHeader(201)
		case "GET", "HEAD":
			if filepath.Base(localfile) == badHeadResponse {
				http.Error(w, "HEAD not implemented", 500)
				return
			}
			http.ServeFile(w, r, localfile)
		case "DELETE":
			if err := os.Remove(localfile); err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			w.WriteHeader(204)
		default:
			http.Error(w, "unsupported method "+r.Method, 400)
		}
	}))

	cleanup := func() {
		srv.Close()
		dirCleanup()
	}

	t.Logf("Mock HTTP Storage %q", srv.URL)
	uri, err := url.Parse(srv.URL)
	if err != nil {
		srv.Close()
		t.Fatal(err)
	}
	uri.Path = filepath.Join(uri.Path, "testing")
	return uri.String(), cleanup
}

// thresholdBlocker is a small wrapper around channels that are commonly used to
// block operations during testing.
// For example, it can be used in conjection with the RunBeforeBackfillChunk and
// BulkAdderFlushesEveryBatch cluster settings. The SQLSchemaChanger knob can be
// used to control the chunk size.
type thresholdBlocker struct {
	threshold        int
	reachedThreshold chan struct{}
	canProceed       chan struct{}
}

func (t thresholdBlocker) maybeBlock(count int) {
	if count == t.threshold {
		close(t.reachedThreshold)
		<-t.canProceed
	}
}

func (t thresholdBlocker) waitUntilBlocked() {
	<-t.reachedThreshold
}

func (t thresholdBlocker) allowToProceed() {
	close(t.canProceed)
}

func makeThresholdBlocker(threshold int) thresholdBlocker {
	return thresholdBlocker{
		threshold:        threshold,
		reachedThreshold: make(chan struct{}),
		canProceed:       make(chan struct{}),
	}
}

// getSpansFromManifest returns the spans that describe the data included in a
// given backup.
func getSpansFromManifest(t *testing.T, backupPath string) roachpb.Spans {
	backupManifestBytes, err := ioutil.ReadFile(backupPath + "/" + backupManifestName)
	require.NoError(t, err)
	var backupManifest BackupManifest
	decompressedBytes, err := decompressData(backupManifestBytes)
	require.NoError(t, err)
	require.NoError(t, protoutil.Unmarshal(decompressedBytes, &backupManifest))
	spans := make([]roachpb.Span, 0, len(backupManifest.Files))
	for _, file := range backupManifest.Files {
		spans = append(spans, file.Span)
	}
	mergedSpans, _ := roachpb.MergeSpans(&spans)
	return mergedSpans
}

func getKVCount(ctx context.Context, kvDB *kv.DB, dbName, tableName string) (int, error) {
	tableDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, dbName, tableName)
	tablePrefix := keys.SystemSQLCodec.TablePrefix(uint32(tableDesc.GetID()))
	tableEnd := tablePrefix.PrefixEnd()
	kvs, err := kvDB.Scan(ctx, tablePrefix, tableEnd, 0)
	return len(kvs), err
}

// uriFmtStringAndArgs returns format strings like "$1" or "($1, $2, $3)" and
// an []interface{} of URIs for the BACKUP/RESTORE queries.
func uriFmtStringAndArgs(uris []string) (string, []interface{}) {
	urisForFormat := make([]interface{}, len(uris))
	var fmtString strings.Builder
	if len(uris) > 1 {
		fmtString.WriteString("(")
	}
	for i, uri := range uris {
		if i > 0 {
			fmtString.WriteString(", ")
		}
		fmtString.WriteString(fmt.Sprintf("$%d", i+1))
		urisForFormat[i] = uri
	}
	if len(uris) > 1 {
		fmtString.WriteString(")")
	}
	return fmtString.String(), urisForFormat
}
