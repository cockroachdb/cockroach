// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl_test

import (
	"bytes"
	"compress/gzip"
	"context"
	gosql "database/sql"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/workload/bank"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadsql"
	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
)

const exportFilePattern = "export*-n*.0.csv"
const parquetExportFilePattern = "export*-n*.0.parquet"

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

// parquetTest provides information to validate a test of EXPORT PARQUET. All
// fields below name and stmt validate some aspect of the exported parquet file.
// If a validation field is empty, then that field will not be used in the test.
type parquetTest struct {
	// name is the name of the test.
	name string

	// prep contains sql commands that will execute before the stmt.
	prep []string

	// stmt contains the EXPORT PARQUET statement.
	stmt string

	// colNames provides the expected column names for the parquet file.
	colNames []string

	// colFieldRepType provides the expected parquet column type of each column in
	// the parquet file.
	colFieldRepType []parquet.FieldRepetitionType

	// vals provides the expected values of the parquet file.
	vals [][]interface{}
}

// validateParquetFile reads the parquet file, conducts a test related to each
// non nil field in the parquetTest struct
func validateParquetFile(t *testing.T, file string, test parquetTest) error {
	r, err := os.Open(file)
	if err != nil {
		return err
	}
	defer r.Close()

	fr, err := goparquet.NewFileReader(r)
	if err != nil {
		return err
	}
	t.Logf("Schema: %s", fr.GetSchemaDefinition())

	cols := fr.SchemaReader.GetSchemaDefinition().RootColumn.Children

	if test.colNames != nil {
		for i, col := range cols {
			require.Equal(t, col.SchemaElement.Name, test.colNames[i])
		}
	}
	if test.colFieldRepType != nil {
		for i, col := range cols {
			require.Equal(t, *col.SchemaElement.RepetitionType, test.colFieldRepType[i])
		}
	}
	if test.vals != nil {
		require.Equal(t, len(cols), len(test.vals[0]))
		require.Equal(t, int(fr.NumRows()), len(test.vals))
		count := 0
		for {
			row, err := fr.NextRow()
			if err == io.EOF {
				break
			}
			if err != nil {
				return fmt.Errorf("reading record failed: %w", err)
			}

			t.Logf("\n Record %v:", count)
			for i := 0; i < len(cols); i++ {
				if test.vals[count][i] == nil {
					// If we expect a null value, the row created by the parquet reader
					// will not have the associated column.
					_, ok := row[cols[i].SchemaElement.Name]
					require.Equal(t, ok, false)
					continue
				}
				v := row[cols[i].SchemaElement.Name]
				decodedV := decodeEl(t, v)
				t.Logf("\t %v", decodedV)
				require.Equal(t, test.vals[count][i], decodedV)
			}
			count++
		}
	}
	return nil
}

func decodeEl(t *testing.T, v interface{}) interface{} {
	var decodedV interface{}
	switch vv := v.(type) {
	case []byte:
		// The parquet exporter encodes native go strings as []byte, so an extra
		// step is required here.

		// TODO (MB): as we add more type support, this switch statement will be
		// insufficient: many go native types are encoded as []byte, so in the
		// future, each column will have to call it's own custom decoder ( this is
		// how IMPORT Parquet will work).
		decodedV = string(vv)
	case int64, float64, bool:
		decodedV = vv
	case map[string]interface{}:
		var arrDecodedV []interface{}
		b := vv["list"].([]map[string]interface{})

		// Array values are stored in an array of maps: []map[string]interface{},
		// where the ith map contains a single key value pair. The key is always "element"
		// and the value is the ith value in the array.

		// If the array of maps only contains an empty map, the array is empty. This
		// occurs IFF "element" is not in the map.

		// NB: there's a bug in the fraugster-parquet vendor library around reading
		// an ARRAY[NULL]. See the "arrays" test case for more info. Ideally, once
		// the bug gets fixed, ARRAY[NULL] will get read as the kvp {"element":interface{}} while
		// ARRAY[] will continue to get read as an empty map.
		if _, nonEmpty := b[0]["element"]; !nonEmpty {
			arrDecodedV = []interface{}{}
			if len(b) > 1 {
				t.Fatalf("array is empty, it shouldn't have a length greater than 1")
			}
		} else {
			// For non-empty arrays
			for _, elMap := range b {
				arrDecodedV = append(arrDecodedV, decodeEl(t, elMap["element"]))
			}
		}
		decodedV = arrDecodedV
	default:
		t.Fatalf("unexepected type: %T", vv)
	}
	return decodedV
}

// TestBasicParquetTypes exports a relation with bool, int, float and string
// values to a parquet file, and then asserts that the parquet exporter properly
// encoded the values of the crdb relation.
func TestBasicParquetTypes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `CREATE TABLE foo (i INT PRIMARY KEY, x STRING, y INT, z FLOAT NOT NULL, a BOOL, 
INDEX (y))`)
	sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'Alice', 3, 14.3, true), (2, 'Bob', 2, 24.1, 
false),(3, 'Carl', 1, 34.214,true),(4, 'Alex', 3, 14.3, NULL), (5, 'Bobby', 2, 3.4,false),
(6, NULL, NULL, 4.5, NULL)`)

	tests := []parquetTest{
		{
			name: "basic",
			stmt: `EXPORT INTO PARQUET 'nodelocal://0/basic' FROM SELECT *
							FROM foo WHERE y IS NOT NULL ORDER BY y ASC LIMIT 2 `,
			colNames: []string{"i", "x", "y", "z", "a"},
			vals: [][]interface{}{{int64(3), "Carl", int64(1), 34.214, true},
				{int64(2), "Bob", int64(2), 24.1, false}},
		},
		{
			name: "null_vals",
			stmt: `EXPORT INTO PARQUET 'nodelocal://0/null_vals' FROM SELECT *
							FROM foo ORDER BY x ASC LIMIT 2`,
			vals: [][]interface{}{
				{int64(6), nil, nil, 4.5, nil},
				{int64(4), "Alex", int64(3), 14.3, nil}},
		},
		{
			name: "colname",
			stmt: `EXPORT INTO PARQUET 'nodelocal://0/colname' FROM SELECT avg(z), min(y) AS baz
							FROM foo`,
			colNames: []string{"avg", "baz"},
		},
		{
			name: "nullable",
			stmt: `EXPORT INTO PARQUET 'nodelocal://0/nullable' FROM SELECT y,z,x
							FROM foo`,
			colFieldRepType: []parquet.FieldRepetitionType{
				parquet.FieldRepetitionType_OPTIONAL,
				parquet.FieldRepetitionType_REQUIRED,
				parquet.FieldRepetitionType_OPTIONAL},
		},
		{
			// TODO (mb): switch one of the values in the array to NULL once the
			// vendor's parquet file reader bug resolves.
			// https://github.com/fraugster/parquet-go/issues/60
			//
			// I already verified that the vendor's parquet writer can write arrays
			// with null values just fine, so EXPORT PARQUET is bug free; however this
			// roundtrip test would fail.
			name: "arrays",
			prep: []string{"CREATE TABLE atable (i INT PRIMARY KEY, x INT[])",
				"INSERT INTO atable VALUES (1, ARRAY[1,2]), (2, ARRAY[2]), (3,ARRAY[1,13,5]),(4, NULL),(5, ARRAY[])"},
			stmt:     `EXPORT INTO PARQUET 'nodelocal://0/arrays' FROM SELECT * FROM atable`,
			colNames: []string{"i", "x"},
			vals: [][]interface{}{
				{int64(1), []interface{}{int64(1), int64(2)}},
				{int64(2), []interface{}{int64(2)}},
				{int64(3), []interface{}{int64(1), int64(13), int64(5)}},
				{int64(4), nil},
				{int64(5), []interface{}{}},
			},
		},
	}

	for _, test := range tests {
		t.Logf("Test %s", test.name)
		if test.prep != nil {
			for _, cmd := range test.prep {
				sqlDB.Exec(t, cmd)
			}
		}
		sqlDB.Exec(t, test.stmt)
		paths, err := filepath.Glob(filepath.Join(dir, test.name,
			parquetExportFilePattern))
		require.NoError(t, err)

		require.Equal(t, 1, len(paths))

		err = validateParquetFile(t, paths[0], test)
		require.NoError(t, err)

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

	var close = func(c io.Closer) {
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
