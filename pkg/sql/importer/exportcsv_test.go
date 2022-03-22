// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package importer_test

import (
	"bytes"
	"compress/gzip"
	"context"
	gosql "database/sql"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/workload/bank"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadsql"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
)

const exportFilePattern = "export*-n*.0.csv"

func setupExportableBank(t *testing.T, nodes, rows int) (*sqlutils.SQLRunner, string, func()) {
	ctx := context.Background()
	dir, cleanupDir := testutils.TempDir(t)

	tc := testcluster.StartTestCluster(t, nodes,
		base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				ExternalIODir:      dir,
				UseDatabase:        "test",
				DisableSpanConfigs: true,
			},
		},
	)
	conn := tc.Conns[0]
	db := sqlutils.MakeSQLRunner(conn)
	db.Exec(t, "CREATE DATABASE test")

	wk := bank.FromRows(rows)
	l := workloadsql.InsertsDataLoader{BatchSize: 100, Concurrency: 3}
	if _, err := workloadsql.Setup(ctx, conn, wk, l); err != nil {
		t.Fatal(err)
	}

	config.TestingSetupZoneConfigHook(tc.Stopper())
	v, err := tc.Servers[0].DB().Get(context.Background(), keys.SystemSQLCodec.DescIDSequenceKey())
	if err != nil {
		t.Fatal(err)
	}
	last := config.SystemTenantObjectID(v.ValueInt())
	zoneConfig := zonepb.DefaultZoneConfig()
	zoneConfig.RangeMaxBytes = proto.Int64(5000)
	config.TestingSetZoneConfig(last+1, zoneConfig)
	db.Exec(t, "ALTER TABLE bank SCATTER")
	db.Exec(t, "SELECT 'force a scan to repopulate range cache' FROM [SELECT count(*) FROM bank]")

	return db, dir, func() {
		tc.Stopper().Stop(ctx)
		cleanupDir()
	}
}

func TestExportImportBank(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	db, _, cleanup := setupExportableBank(t, 3, 100)
	defer cleanup()

	// Add some unicode to prove FmtExport works as advertised.
	db.Exec(t, "UPDATE bank SET payload = payload || '✅' WHERE id = 5")
	db.Exec(t, "UPDATE bank SET payload = NULL WHERE id % 2 = 0")

	chunkSize := 13
	baseExportDir := "userfile:///t/"
	for _, null := range []string{"", "NULL"} {
		nullAs := fmt.Sprintf(", nullas = '%s'", null)
		nullIf := fmt.Sprintf(", nullif = '%s'", null)

		t.Run("null="+null, func(t *testing.T) {
			exportDir := filepath.Join(baseExportDir, t.Name())

			var asOf string
			db.QueryRow(t, "SELECT cluster_logical_timestamp()").Scan(&asOf)

			db.Exec(t,
				fmt.Sprintf(`EXPORT INTO CSV $1
				WITH chunk_rows = $2, delimiter = '|' %s
				FROM SELECT * FROM bank AS OF SYSTEM TIME %s`, nullAs, asOf), exportDir, chunkSize,
			)

			schema := bank.FromRows(1).Tables()[0].Schema
			exportedFiles := filepath.Join(exportDir, "*")
			db.Exec(t, fmt.Sprintf("CREATE TABLE bank2 %s", schema))
			db.Exec(t, fmt.Sprintf(`IMPORT INTO bank2 CSV DATA ($1) WITH delimiter = '|'%s`, nullIf), exportedFiles)

			db.CheckQueryResults(t,
				fmt.Sprintf(`SELECT * FROM bank AS OF SYSTEM TIME %s ORDER BY id`, asOf), db.QueryStr(t, `SELECT * FROM bank2 ORDER BY id`),
			)
			db.CheckQueryResults(t,
				`SELECT fingerprint FROM [SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE bank2]`,
				db.QueryStr(t, `SELECT fingerprint FROM [SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE bank]`),
			)
			db.Exec(t, "DROP TABLE bank2")
		})
	}
}

// Tests if user does not specify nullas option and imports null data, an error is raised.
func TestExportNullWithEmptyNullAs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	dir, cleanup := testutils.TempDir(t)
	defer cleanup()

	tc := testcluster.StartTestCluster(
		t, 1, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: dir}})
	defer tc.Stopper().Stop(ctx)

	conn := tc.Conns[0]
	db := sqlutils.MakeSQLRunner(conn)

	// Set up dummy accounts table with NULL value
	db.Exec(t, `
		CREATE TABLE accounts (id INT PRIMARY KEY, balance INT);
		INSERT INTO accounts VALUES (1, NULL), (2, 8);
	`)

	// Case when `nullas` option is unspecified: expect error
	const stmtWithoutNullas = "EXPORT INTO CSV 'nodelocal://0/t' FROM SELECT * FROM accounts"
	db.ExpectErr(t, "NULL value encountered during EXPORT, "+
		"use `WITH nullas` to specify the string representation of NULL", stmtWithoutNullas)

	// Case when `nullas` option is specified: operation is successful and NULLs are encoded to "None"
	const stmtWithNullas = `EXPORT INTO CSV 'nodelocal://0/t' WITH nullas="None" FROM SELECT * FROM accounts`
	db.Exec(t, stmtWithNullas)
	contents := readFileByGlob(t, filepath.Join(dir, "t", exportFilePattern))
	require.Equal(t, "1,None\n2,8\n", string(contents))

	// Verify successful IMPORT statement `WITH nullif="None"` to complete round trip
	const importStmt = `IMPORT INTO accounts2 CSV DATA ('nodelocal://0/t/export*-n*.0.csv') WITH nullif="None"`
	db.Exec(t, `CREATE TABLE accounts2(id INT PRIMARY KEY, balance INT)`)
	db.Exec(t, importStmt)
	db.CheckQueryResults(t,
		"SELECT * FROM accounts2", db.QueryStr(t, "SELECT * FROM accounts"),
	)
}

func TestMultiNodeExportStmt(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	nodes := 5
	exportRows := 100
	db, _, cleanup := setupExportableBank(t, nodes, exportRows*2)
	defer cleanup()

	maxTries := 10
	// we might need to retry if our table didn't actually scatter enough.
	for tries := 0; tries < maxTries; tries++ {
		chunkSize := 13
		rows := db.Query(t,
			`EXPORT INTO CSV 'nodelocal://0/t' WITH chunk_rows = $3 FROM SELECT * FROM bank WHERE id >= $1 and id < $2`,
			10, 10+exportRows, chunkSize,
		)

		files, totalRows, totalBytes := 0, 0, 0
		nodesSeen := make(map[string]bool)
		for rows.Next() {
			filename, count, bytes := "", 0, 0
			if err := rows.Scan(&filename, &count, &bytes); err != nil {
				t.Fatal(err)
			}
			files++
			if count > chunkSize {
				t.Fatalf("expected no chunk larger than %d, got %d", chunkSize, count)
			}
			totalRows += count
			totalBytes += bytes
			nodesSeen[strings.SplitN(filename, ".", 2)[0]] = true
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("unexpected error during export: %s", err.Error())
		}
		if totalRows != exportRows {
			t.Fatalf("expected %d rows, got %d", exportRows, totalRows)
		}
		if expected := exportRows / chunkSize; files < expected {
			t.Fatalf("expected at least %d files, got %d", expected, files)
		}
		if len(nodesSeen) < 2 {
			// table isn't as scattered as we expected, but we can try again.
			if tries < maxTries {
				continue
			}
			t.Fatalf("expected files from %d nodes, got %d: %v", 2, len(nodesSeen), nodesSeen)
		}
		break
	}
}

func TestExportJoin(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `CREATE TABLE t AS VALUES (1, 2)`)
	sqlDB.Exec(t, `EXPORT INTO CSV 'nodelocal://0/join' FROM SELECT * FROM t, t as u`)
}

func readFileByGlob(t *testing.T, pattern string) []byte {
	paths, err := filepath.Glob(pattern)
	require.NoError(t, err)

	require.Equal(t, 1, len(paths))

	result, err := ioutil.ReadFile(paths[0])
	require.NoError(t, err)

	return result
}

func TestExportOrder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `CREATE TABLE foo (i INT PRIMARY KEY, x INT, y INT, z INT, INDEX (y))`)
	sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 12, 3, 14), (2, 22, 2, 24), (3, 32, 1, 34)`)

	sqlDB.Exec(t, `EXPORT INTO CSV 'nodelocal://0/order' FROM SELECT * FROM foo ORDER BY y ASC LIMIT 2`)
	content := readFileByGlob(t, filepath.Join(dir, "order", exportFilePattern))

	if expected, got := "3,32,1,34\n2,22,2,24\n", string(content); expected != got {
		t.Fatalf("expected %q, got %q", expected, got)
	}
}

func TestExportUniqueness(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `CREATE TABLE foo (i INT PRIMARY KEY, x INT, y INT, z INT, INDEX (y))`)
	sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 12, 3, 14), (2, 22, 2, 24), (3, 32, 1, 34)`)

	const stmt = `EXPORT INTO CSV 'nodelocal://0/' WITH chunk_rows=$1 FROM SELECT * FROM foo`

	sqlDB.Exec(t, stmt, 2)
	dir1, err := ioutil.ReadDir(dir)
	require.NoError(t, err)

	sqlDB.Exec(t, stmt, 2)
	dir2, err := ioutil.ReadDir(dir)
	require.NoError(t, err)

	require.Equal(t, 2*len(dir1), len(dir2), "second export did not double the number of files")
}

func TestExportUserDefinedTypes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	dir, cleanup := testutils.TempDir(t)
	defer cleanup()

	tc := testcluster.StartTestCluster(
		t, 1, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: dir}})
	defer tc.Stopper().Stop(ctx)

	conn := tc.Conns[0]
	sqlDB := sqlutils.MakeSQLRunner(conn)

	// Set up some initial state for the tests.
	sqlDB.Exec(t, `
CREATE TYPE greeting AS ENUM ('hello', 'hi');
CREATE TABLE greeting_table (x greeting, y greeting);
INSERT INTO greeting_table VALUES ('hello', 'hello'), ('hi', 'hi');
`)
	tests := []struct {
		stmt     string
		expected string
	}{
		{
			stmt:     "EXPORT INTO CSV 'nodelocal://0/%s/' FROM (SELECT 'hello':::greeting, 'hi':::greeting)",
			expected: "hello,hi\n",
		},
		{
			stmt:     "EXPORT INTO CSV 'nodelocal://0/%s/' FROM TABLE greeting_table",
			expected: "hello,hello\nhi,hi\n",
		},
		{
			stmt:     "EXPORT INTO CSV 'nodelocal://0/%s/' FROM (SELECT x, y, enum_first(x) FROM greeting_table)",
			expected: "hello,hello,hello\nhi,hi,hello\n",
		},
	}
	for i, test := range tests {
		path := fmt.Sprintf("test%d", i)
		stmt := fmt.Sprintf(test.stmt, path)

		sqlDB.Exec(t, stmt)

		// Read the dumped file.
		contents := readFileByGlob(t, filepath.Join(dir, path, exportFilePattern))

		require.Equal(t, test.expected, string(contents))
	}
}

func TestExportOrderCompressed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	close := func(c io.Closer) {
		if err := c.Close(); err != nil {
			t.Fatalf("failed to close stream, got error %s", err)
		}
	}

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `CREATE TABLE foo (i INT PRIMARY KEY, x INT, y INT, z INT, INDEX (y))`)
	sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 12, 3, 14), (2, 22, 2, 24), (3, 32, 1, 34)`)

	sqlDB.Exec(t, `EXPORT INTO CSV 'nodelocal://0/order' with compression = gzip from select * from foo order by y asc limit 2`)
	compressed := readFileByGlob(t, filepath.Join(dir, "order", exportFilePattern+".gz"))

	gzipReader, err := gzip.NewReader(bytes.NewReader(compressed))
	defer close(gzipReader)

	require.NoError(t, err)

	content, err := ioutil.ReadAll(gzipReader)
	require.NoError(t, err)

	if expected, got := "3,32,1,34\n2,22,2,24\n", string(content); expected != got {
		t.Fatalf("expected %q, got %q", expected, got)
	}
}

func TestExportShow(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir})
	defer srv.Stopper().Stop(context.Background())

	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `EXPORT INTO CSV 'nodelocal://0/show' FROM SELECT database_name, owner FROM [SHOW DATABASES] ORDER BY database_name`)
	content := readFileByGlob(t, filepath.Join(dir, "show", exportFilePattern))

	if expected, got := "defaultdb,"+security.RootUser+"\npostgres,"+security.RootUser+"\nsystem,"+
		security.NodeUser+"\n", string(content); expected != got {
		t.Fatalf("expected %q, got %q", expected, got)
	}
}

// TestExportVectorized makes sure that SupportsVectorized check doesn't panic
// on CSVWriter processor.
func TestExportVectorized(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `CREATE TABLE t(a INT PRIMARY KEY)`)
	sqlDB.Exec(t, `EXPORT INTO CSV 'http://0.1:37957/exp_1' FROM TABLE t`)
}

// TestExportFeatureFlag tests the feature flag logic that allows the EXPORT
// command to be toggled off via cluster settings.
func TestExportFeatureFlag(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	// Feature flag is off — test that EXPORT surfaces error.
	sqlDB.Exec(t, `SET CLUSTER SETTING feature.export.enabled = FALSE`)
	sqlDB.Exec(t, `CREATE TABLE feature_flags (a INT PRIMARY KEY)`)
	sqlDB.ExpectErr(t, `feature EXPORT was disabled by the database administrator`,
		`EXPORT INTO CSV 'nodelocal://0/%s/' FROM TABLE feature_flags`)

	// Feature flag is on — test that EXPORT does not error.
	sqlDB.Exec(t, `SET CLUSTER SETTING feature.export.enabled = TRUE`)
	sqlDB.Exec(t, `EXPORT INTO CSV 'nodelocal://0/%s/' FROM TABLE feature_flags`)
}

func TestExportPrivileges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE USER testuser`)
	sqlDB.Exec(t, `CREATE TABLE privs (a INT)`)

	pgURL, cleanup := sqlutils.PGUrl(t, srv.ServingSQLAddr(),
		"TestExportPrivileges-testuser", url.User("testuser"))
	defer cleanup()
	startTestUser := func() *gosql.DB {
		testuser, err := gosql.Open("postgres", pgURL.String())
		require.NoError(t, err)
		return testuser
	}
	testuser := startTestUser()
	_, err := testuser.Exec(`EXPORT INTO CSV 'nodelocal://0/privs' FROM TABLE privs`)
	require.True(t, testutils.IsError(err, "testuser does not have SELECT privilege"))

	dest := "nodelocal://0/privs_placeholder"
	_, err = testuser.Exec(`EXPORT INTO CSV $1 FROM TABLE privs`, dest)
	require.True(t, testutils.IsError(err, "testuser does not have SELECT privilege"))
	testuser.Close()

	// Grant SELECT privilege.
	sqlDB.Exec(t, `GRANT SELECT ON TABLE privs TO testuser`)

	// The above SELECT GRANT hangs if we leave the user conn open. Thus, we need
	// to reinitialize it here.
	testuser = startTestUser()
	defer testuser.Close()

	_, err = testuser.Exec(`EXPORT INTO CSV 'nodelocal://0/privs' FROM TABLE privs`)
	require.True(t, testutils.IsError(err,
		"only users with the admin role are allowed to EXPORT to the specified URI"))
	_, err = testuser.Exec(`EXPORT INTO CSV $1 FROM TABLE privs`, dest)
	require.True(t, testutils.IsError(err,
		"only users with the admin role are allowed to EXPORT to the specified URI"))

	sqlDB.Exec(t, `GRANT ADMIN TO testuser`)

	_, err = testuser.Exec(`EXPORT INTO CSV 'nodelocal://0/privs' FROM TABLE privs`)
	require.NoError(t, err)
	_, err = testuser.Exec(`EXPORT INTO CSV $1 FROM TABLE privs`, dest)
	require.NoError(t, err)
}

func TestExportTargetFileSizeSetting(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	dir, cleanup := testutils.TempDir(t)
	defer cleanup()

	tc := testcluster.StartTestCluster(
		t, 1, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: dir}})
	defer tc.Stopper().Stop(ctx)

	conn := tc.Conns[0]
	sqlDB := sqlutils.MakeSQLRunner(conn)

	sqlDB.Exec(t, `EXPORT INTO CSV 'nodelocal://0/foo' WITH chunk_size='10KB' FROM select i, gen_random_uuid() from generate_series(1, 4000) as i;`)
	files, err := ioutil.ReadDir(filepath.Join(dir, "foo"))
	require.NoError(t, err)
	require.Equal(t, 14, len(files))

	sqlDB.Exec(t, `EXPORT INTO CSV 'nodelocal://0/foo-compressed' WITH chunk_size='10KB',compression='gzip' FROM select i, gen_random_uuid() from generate_series(1, 4000) as i;`)
	zipFiles, err := ioutil.ReadDir(filepath.Join(dir, "foo-compressed"))
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(zipFiles), 6)
}
