// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

// importerccl is a package where we can write tests of IMPORT that require that
// ccl-only functionality be enabled as well.
package importerccl

import (
	"context"
	gosql "database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/kvccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/multiregionccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/multiregionccl/multiregionccltestutils"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/partitionccl"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/impl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func sharedTestdata(t *testing.T) string {
	testdataDir := "../../sql/importer/testdata/"
	if bazel.BuiltWithBazel() {
		runfile, err := bazel.Runfile("pkg/sql/importer/testdata/")
		if err != nil {
			t.Fatal(err)
		}
		testdataDir = runfile
	}
	return testdataDir
}

func TestImportMultiRegion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	baseDir := sharedTestdata(t)
	tc, sqlDB, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(
		t, 2 /* numServers */, base.TestingKnobs{}, multiregionccltestutils.WithBaseDirectory(baseDir),
	)
	defer cleanup()

	// Set up a hook which we can set to run during the import.
	// Importantly this happens before the final descriptors have been published.
	var duringImportFunc atomic.Value
	noopDuringImportFunc := func() error { return nil }
	duringImportFunc.Store(noopDuringImportFunc)
	for i := 0; i < tc.NumServers(); i++ {
		tc.Server(i).JobRegistry().(*jobs.Registry).
			TestingResumerCreationKnobs = map[jobspb.Type]func(jobs.Resumer) jobs.Resumer{
			jobspb.TypeImport: func(resumer jobs.Resumer) jobs.Resumer {
				resumer.(interface {
					TestingSetAfterImportKnob(fn func(summary roachpb.RowCount) error)
				}).TestingSetAfterImportKnob(func(summary roachpb.RowCount) error {
					return duringImportFunc.Load().(func() error)()
				})
				return resumer
			},
		}
	}

	tdb := sqlutils.MakeSQLRunner(sqlDB)
	tdb.Exec(t, `SET CLUSTER SETTING kv.bulk_ingest.batch_size = '10KB'`)

	// Create the databases
	tdb.Exec(t, `CREATE DATABASE foo`)
	tdb.Exec(t, `CREATE DATABASE multi_region PRIMARY REGION "us-east1"`)

	simpleOcf := fmt.Sprintf("nodelocal://0/avro/%s", "simple.ocf")

	var data string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			_, _ = w.Write([]byte(data))
		}
	}))
	defer srv.Close()

	viewsAndSequencesTestCases := []struct {
		desc      string
		importSQL string
		expected  map[string]string
	}{
		{
			desc:      "pgdump",
			importSQL: `IMPORT PGDUMP 'nodelocal://0/pgdump/views_and_sequences.sql' WITH ignore_unsupported_statements`,
			expected: map[string]string{
				"tbl": "REGIONAL BY TABLE IN PRIMARY REGION",
				"s":   "REGIONAL BY TABLE IN PRIMARY REGION",
				// views are ignored.
			},
		},
		{
			desc:      "mysqldump",
			importSQL: `IMPORT MYSQLDUMP 'nodelocal://0/mysqldump/views_and_sequences.sql'`,
			expected: map[string]string{
				"tbl":          "REGIONAL BY TABLE IN PRIMARY REGION",
				"tbl_auto_inc": "REGIONAL BY TABLE IN PRIMARY REGION",
				// views are ignored.
			},
		},
	}

	for _, tc := range viewsAndSequencesTestCases {
		t.Run(tc.desc, func(t *testing.T) {
			tdb.Exec(t, `USE multi_region`)
			defer tdb.Exec(t, `
DROP TABLE IF EXISTS tbl;
DROP SEQUENCE IF EXISTS s;
DROP SEQUENCE IF EXISTS table_auto_inc;
DROP VIEW IF EXISTS v`,
			)

			tdb.Exec(t, tc.importSQL)
			rows := tdb.Query(t, "SELECT table_name, locality FROM [SHOW TABLES] ORDER BY table_name")
			results := make(map[string]string)
			for rows.Next() {
				var tableName, locality string
				require.NoError(t, rows.Scan(&tableName, &locality))
				results[tableName] = locality
			}
			require.NoError(t, rows.Err())
			require.Equal(t, tc.expected, results)
		})
	}

	t.Run("avro", func(t *testing.T) {
		tests := []struct {
			name      string
			db        string
			table     string
			sql       string
			create    string
			args      []interface{}
			errString string
			data      string
			during    string
		}{
			{
				name:   "import-create-using-multi-region-regional-by-table-to-multi-region-database",
				db:     "multi_region",
				table:  "simple",
				sql:    "IMPORT INTO simple AVRO DATA ($1)",
				create: "CREATE TABLE simple (i integer PRIMARY KEY, s text, b bytea) LOCALITY REGIONAL BY TABLE",
				args:   []interface{}{simpleOcf},
			},
			{
				name:   "import-into-multi-region-regional-by-row-default-col-to-multi-region-database",
				db:     "multi_region",
				table:  "mr_regional_by_row",
				create: "CREATE TABLE mr_regional_by_row (i INT8 PRIMARY KEY, s text, b bytea) LOCALITY REGIONAL BY ROW",
				sql:    "IMPORT INTO mr_regional_by_row AVRO DATA ($1)",
				args:   []interface{}{simpleOcf},
			},
			{
				name:   "import-into-multi-region-regional-by-row-to-multi-region-database",
				db:     "multi_region",
				table:  "mr_regional_by_row",
				create: "CREATE TABLE mr_regional_by_row (i INT8 PRIMARY KEY, s text, b bytea) LOCALITY REGIONAL BY ROW",
				sql:    "IMPORT INTO mr_regional_by_row (i, s, b, crdb_region) CSV DATA ($1)",
				args:   []interface{}{srv.URL},
				data:   "1,\"foo\",NULL,us-east1\n",
			},
			{
				name:   "import-into-multi-region-regional-by-row-to-multi-region-database-concurrent-table-add",
				db:     "multi_region",
				table:  "mr_regional_by_row",
				create: "CREATE TABLE mr_regional_by_row (i INT8 PRIMARY KEY, s text, b bytea) LOCALITY REGIONAL BY ROW",
				during: "CREATE TABLE mr_regional_by_row2 (i INT8 PRIMARY KEY) LOCALITY REGIONAL BY ROW",
				sql:    "IMPORT INTO mr_regional_by_row (i, s, b, crdb_region) CSV DATA ($1)",
				args:   []interface{}{srv.URL},
				data:   "1,\"foo\",NULL,us-east1\n",
			},
			{
				name:   "import-into-multi-region-regional-by-row-to-multi-region-database-concurrent-add-region",
				db:     "multi_region",
				table:  "mr_regional_by_row",
				create: "CREATE TABLE mr_regional_by_row (i INT8 PRIMARY KEY, s text, b bytea) LOCALITY REGIONAL BY ROW",
				sql:    "IMPORT INTO mr_regional_by_row (i, s, b, crdb_region) CSV DATA ($1)",
				during: `ALTER DATABASE multi_region ADD REGION "us-east2"`,
				errString: `type descriptor "crdb_internal_region" \(\d+\) has been ` +
					`modified, potentially incompatibly, since import planning; ` +
					`aborting to avoid possible corruption`,
				args: []interface{}{srv.URL},
				data: "1,\"foo\",NULL,us-east1\n",
			},
			{
				name:  "import-into-multi-region-regional-by-row-with-enum-which-has-been-modified",
				db:    "multi_region",
				table: "mr_regional_by_row",
				create: `
CREATE TYPE typ AS ENUM ('a');
CREATE TABLE mr_regional_by_row (i INT8 PRIMARY KEY, s typ, b bytea) LOCALITY REGIONAL BY ROW;
`,
				sql:    "IMPORT INTO mr_regional_by_row (i, s, b, crdb_region) CSV DATA ($1)",
				during: `ALTER TYPE typ ADD VALUE 'b'`,
				errString: `type descriptor "typ" \(\d+\) has been ` +
					`modified, potentially incompatibly, since import planning; ` +
					`aborting to avoid possible corruption`,
				args: []interface{}{srv.URL},
				data: "1,\"a\",NULL,us-east1\n",
			},
			{
				name:      "import-into-multi-region-regional-by-row-to-multi-region-database-wrong-value",
				db:        "multi_region",
				table:     "mr_regional_by_row",
				create:    "CREATE TABLE mr_regional_by_row (i INT8 PRIMARY KEY, s text, b bytea) LOCALITY REGIONAL BY ROW",
				sql:       "IMPORT INTO mr_regional_by_row (i, s, b, crdb_region) CSV DATA ($1)",
				args:      []interface{}{srv.URL},
				data:      "1,\"foo\",NULL,us-west1\n",
				errString: "invalid input value for enum crdb_internal_region",
			},
			{
				name:   "import-into-multi-region-global-to-multi-region-database",
				db:     "multi_region",
				table:  "mr_global",
				create: "CREATE TABLE mr_global (i INT8 PRIMARY KEY, s text, b bytea) LOCALITY GLOBAL",
				sql:    "IMPORT INTO mr_global AVRO DATA ($1)",
				args:   []interface{}{simpleOcf},
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				defer duringImportFunc.Store(noopDuringImportFunc)
				if test.during != "" {
					duringImportFunc.Store(func() error {
						q := fmt.Sprintf(`SET DATABASE = %q; %s`, test.db, test.during)
						_, err := sqlDB.Exec(q)
						return err
					})
				}
				tdb.Exec(t, fmt.Sprintf(`SET DATABASE = %q`, test.db))
				tdb.Exec(t, fmt.Sprintf("DROP TABLE IF EXISTS %q CASCADE", test.table))

				if test.data != "" {
					data = test.data
				}

				if test.create != "" {
					tdb.Exec(t, test.create)
				}

				_, err := sqlDB.ExecContext(context.Background(), test.sql, test.args...)
				if test.errString != "" {
					require.Regexp(t, test.errString, err)
				} else {
					require.NoError(t, err)
					var numRows int
					tdb.QueryRow(
						t, fmt.Sprintf("SELECT count(*) FROM %q", test.table),
					).Scan(&numRows)
					if numRows == 0 {
						t.Error("expected some rows after import")
					}
				}
			})
		}
	})
}

// There are two goals of this testcase:
//
// 1) Ensure that we can properly export from REGIONAL BY ROW tables (that the
//    hidden row stays hidden, unless explicitly requested).
// 2) That we can import the exported data both into a non-RBR table, as well
//    as a table which we can later convert to RBR, while preserving the
//    crdb_region column data.
func TestMultiRegionExportImportRoundTrip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	validateNumRows := func(sqlDB *gosql.DB, tableName string, expected int) {
		res := sqlDB.QueryRow(fmt.Sprintf(`SELECT count(*) FROM %s`, tableName))
		require.NoError(t, res.Err())

		var numRows int
		err := res.Scan(&numRows)
		require.NoError(t, err)

		if numRows != expected {
			t.Errorf("expected %d rows after import, found %d", expected, numRows)
		}
	}

	baseDir, dirCleanup := testutils.TempDir(t)
	defer dirCleanup()

	_, sqlDB, clusterCleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(
		t, 3 /* numServers */, base.TestingKnobs{}, multiregionccltestutils.WithBaseDirectory(baseDir))
	defer clusterCleanup()

	_, err := sqlDB.Exec(`SET CLUSTER SETTING kv.bulk_ingest.batch_size = '10KB'`)
	require.NoError(t, err)

	// Create the database.
	_, err = sqlDB.Exec(`CREATE DATABASE multi_region PRIMARY REGION "us-east1" REGIONS "us-east2", "us-east3";
USE multi_region;`)
	require.NoError(t, err)

	// Create the tables
	_, err = sqlDB.Exec(`CREATE TABLE original_rbr (i int) LOCALITY REGIONAL BY ROW;
CREATE TABLE destination (i int);
CREATE TABLE destination_fake_rbr (crdb_region public.crdb_internal_region NOT NULL, i int);`)
	require.NoError(t, err)

	// Insert some data to the original table.
	_, err = sqlDB.Exec(`INSERT INTO original_rbr values (1),(2),(3),(4),(5)`)
	require.NoError(t, err)

	// Export the data.
	_, err = sqlDB.Exec(`EXPORT INTO CSV 'nodelocal://0/original_rbr_full'
 FROM SELECT crdb_region, i from original_rbr;`)
	require.NoError(t, err)

	_, err = sqlDB.Exec(`EXPORT INTO CSV 'nodelocal://0/original_rbr_default'
FROM TABLE original_rbr;`)
	require.NoError(t, err)

	// Import the data back into the destination table.
	_, err = sqlDB.Exec(`IMPORT into destination (i) CSV DATA
 ('nodelocal://0/original_rbr_default/export*.csv')`)
	require.NoError(t, err)
	validateNumRows(sqlDB, `destination`, 5)

	// Import the full export to the fake RBR table.
	_, err = sqlDB.Exec(`IMPORT into destination_fake_rbr (crdb_region, i) CSV DATA
 ('nodelocal://0/original_rbr_full/export*.csv')`)
	require.NoError(t, err)
	validateNumRows(sqlDB, `destination_fake_rbr`, 5)

	// Convert fake table to full RBR table. This test is only required until we
	// support IMPORT directly to RBR tables. The thinking behind this test is
	// that this _could_ be one way that customers work-around the limitation of
	// not supporting IMPORT to RBR tables in 21.1. Note that right now we can't
	// make this column hidden (#62892).
	_, err = sqlDB.Exec(`ALTER TABLE destination_fake_rbr ALTER COLUMN
 crdb_region SET DEFAULT default_to_database_primary_region(gateway_region())::public.crdb_internal_region;`)
	require.NoError(t, err)

	_, err = sqlDB.Exec(`ALTER TABLE destination_fake_rbr SET LOCALITY
 REGIONAL BY ROW AS crdb_region;`)
	require.NoError(t, err)

	// Insert some more rows and ensure that the default values get generated.
	_, err = sqlDB.Exec(`INSERT INTO destination_fake_rbr (i) values (6),(7),(3),(9),(10)`)
	require.NoError(t, err)
	validateNumRows(sqlDB, `destination_fake_rbr`, 10)
}

func TestExportInsideTenant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	srv, _, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir})
	defer srv.Stopper().Stop(context.Background())

	_, conn10 := serverutils.StartTenant(t, srv, base.TestTenantArgs{TenantID: roachpb.MakeTenantID(10)})
	defer conn10.Close()
	tenant10 := sqlutils.MakeSQLRunner(conn10)

	tenant10.Exec(t, `CREATE TABLE foo (i INT PRIMARY KEY, x INT, y INT, z INT, INDEX (y))`)
	tenant10.Exec(t, `INSERT INTO foo VALUES (1, 12, 3, 14), (2, 22, 2, 24), (3, 32, 1, 34)`)

	tenant10.Exec(t, `EXPORT INTO CSV 'userfile:///order' FROM SELECT * FROM foo ORDER BY y ASC LIMIT 2`)
	csvFileIDs := tenant10.QueryStr(t, `SELECT file_id FROM userfiles_root_upload_files WHERE filename LIKE '%csv'`)
	require.Len(t, csvFileIDs, 1)
	filePayloads := tenant10.QueryStr(t, `
SELECT payload FROM userfiles_root_upload_payload WHERE file_id = $1`, csvFileIDs[0][0])
	require.Len(t, filePayloads, 1)

	require.Equal(t, filePayloads[0][0], "3,32,1,34\n2,22,2,24\n")
}

func TestImportInTenant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	baseDir := sharedTestdata(t)
	args := base.TestServerArgs{ExternalIODir: baseDir}
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{ServerArgs: args})
	defer tc.Stopper().Stop(ctx)
	conn := tc.Conns[0]
	sqlDB := sqlutils.MakeSQLRunner(conn)

	// Setup a few tenants, each with a different table.
	_, conn10 := serverutils.StartTenant(t, tc.Server(0), base.TestTenantArgs{TenantID: roachpb.MakeTenantID(10)})
	defer conn10.Close()
	t10 := sqlutils.MakeSQLRunner(conn10)

	// Setup a few tenants, each with a different table.
	_, conn11 := serverutils.StartTenant(t, tc.Server(0), base.TestTenantArgs{TenantID: roachpb.MakeTenantID(11)})
	defer conn11.Close()
	t11 := sqlutils.MakeSQLRunner(conn11)

	const userfileURI = "userfile://defaultdb.public.root/test.csv"
	const createStmt = "CREATE TABLE foo (k INT PRIMARY KEY, v INT)"
	const importStmt = "IMPORT INTO foo CSV DATA ($1)"

	// Upload different files to same userfile name on each of host and tenants.
	require.NoError(t, putUserfile(ctx, conn, security.RootUserName(), userfileURI, []byte("1,2\n3,4")))
	require.NoError(t, putUserfile(ctx, conn10, security.RootUserName(), userfileURI, []byte("10,2")))
	require.NoError(t, putUserfile(ctx, conn11, security.RootUserName(), userfileURI, []byte("11,22\n33,44\n55,66")))

	sqlDB.Exec(t, createStmt)
	sqlDB.Exec(t, importStmt, userfileURI)
	sqlDB.CheckQueryResults(t, "SELECT * FROM foo", [][]string{{"1", "2"}, {"3", "4"}})

	t10.Exec(t, createStmt)
	t10.Exec(t, importStmt, userfileURI)
	t10.CheckQueryResults(t, "SELECT * FROM foo", [][]string{{"10", "2"}})

	t11.Exec(t, createStmt)
	t11.Exec(t, importStmt, userfileURI)
	t11.CheckQueryResults(t, "SELECT * FROM foo", [][]string{{"11", "22"}, {"33", "44"}, {"55", "66"}})
}

// TestImportInMultiServerTenant tests that import is successful in a tenant
// with multiple SQL pods.
// Currently, verification that the import is distributed needs to be done
// manually, and can be done by running the test with logging enabled and
// checking that the log contains the message "starting read import" for both
// instance nsql1 and nsql 2.
func TestImportInMultiServerTenant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	baseDir := testutils.TestDataPath(t)
	args := base.TestServerArgs{ExternalIODir: baseDir}
	tc := serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{ServerArgs: args})
	defer tc.Stopper().Stop(ctx)

	// Setup a SQL server on a tenant.
	_, conn1 := serverutils.StartTenant(t, tc.Server(0), base.TestTenantArgs{
		TenantID: roachpb.MakeTenantID(10),
	})
	defer conn1.Close()
	t1 := sqlutils.MakeSQLRunner(conn1)

	// Setup another SQL server on the same tenant.
	_, conn2 := serverutils.StartTenant(t, tc.Server(0), base.TestTenantArgs{
		TenantID: roachpb.MakeTenantID(10),
		Existing: true,
	})
	defer conn2.Close()
	t2 := sqlutils.MakeSQLRunner(conn2)

	const userfileURI = "userfile://defaultdb.public.root/test.csv"
	const userfile2URI = "userfile://defaultdb.public.root/test2.csv"
	const createStmt = "CREATE TABLE foo (k INT PRIMARY KEY, v INT)"
	const importStmt = "IMPORT INTO foo CSV DATA ($1, $2)"

	// Upload files.
	require.NoError(t, putUserfile(ctx, conn1, security.RootUserName(), userfileURI, []byte("10,2")))
	require.NoError(t, putUserfile(ctx, conn2, security.RootUserName(), userfile2URI, []byte("11,22\n33,44\n55,66")))

	t1.Exec(t, createStmt)
	// TODO(harding): Verify that the import is distributed to both pods.
	t1.Exec(t, importStmt, userfileURI, userfile2URI)
	t1.CheckQueryResults(t, "SELECT * FROM foo", [][]string{{"10", "2"}, {"11", "22"}, {"33", "44"}, {"55", "66"}})
	t2.CheckQueryResults(t, "SELECT * FROM foo", [][]string{{"10", "2"}, {"11", "22"}, {"33", "44"}, {"55", "66"}})
}

func putUserfile(
	ctx context.Context, conn *gosql.DB, user security.SQLUsername, uri string, content []byte,
) error {
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(sql.CopyInFileStmt(uri, sql.CrdbInternalName, sql.UserFileUploadTable))
	if err != nil {
		return err
	}

	var sent int
	for sent < len(content) {
		chunk := 1024
		if sent+chunk >= len(content) {
			chunk = len(content) - sent
		}
		_, err = stmt.Exec(string(content[sent : sent+chunk]))
		if err != nil {
			return err
		}
		sent += chunk
	}
	if err := stmt.Close(); err != nil {
		return err
	}

	return tx.Commit()
}
