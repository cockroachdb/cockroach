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
	"github.com/cockroachdb/cockroach/pkg/ccl/importccl"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
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
// fields below the stmt validate some aspect of the exported parquet file.
type parquetTest struct {
	// filePrefix provides the parquet file name in front of the parquetExportFilePattern.
	filePrefix string

	// fileSuffix provides the compression type, if any, of the parquet file.
	fileSuffix string

	// dir is the temp directory the parquet file will be in.
	dir string

	// dbName is the name of the exported table's database.
	dbName string

	// prep contains sql commands that will execute before the stmt.
	prep []string

	// stmt contains the EXPORT PARQUET sql statement to test.
	stmt string

	// cols provides the expected column name and type
	cols colinfo.ResultColumns

	// colFieldRepType provides the expected parquet repetition type of each column in
	// the parquet file.
	colFieldRepType []parquet.FieldRepetitionType

	// datums provides the expected values of the parquet file.
	datums []tree.Datums
}

// validateParquetFile reads the parquet file and validates various aspects of
// the parquet file.
func validateParquetFile(
	t *testing.T, ctx context.Context, ie *sql.InternalExecutor, test parquetTest,
) error {
	paths, err := filepath.Glob(filepath.Join(test.dir, test.filePrefix, parquetExportFilePattern+test.fileSuffix))
	require.NoError(t, err)

	require.Equal(t, 1, len(paths))
	r, err := os.Open(paths[0])
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

	if test.colFieldRepType != nil {
		for i, col := range cols {
			require.Equal(t, *col.SchemaElement.RepetitionType, test.colFieldRepType[i])
		}
	}
	// Get the datums returned by the SELECT statement called in the EXPORT
	// PARQUET statement to validate the data in the parquet file.
	validationStmt := strings.SplitN(test.stmt, "FROM ", 2)[1]
	test.datums, test.cols, err = ie.QueryBufferedExWithCols(
		ctx,
		"",
		nil,
		sessiondata.InternalExecutorOverride{
			User:     security.RootUserName(),
			Database: test.dbName},
		validationStmt)
	require.NoError(t, err)

	for j, col := range cols {
		require.Equal(t, col.SchemaElement.Name, test.cols[j].Name)
	}
	i := 0
	for {
		row, err := fr.NextRow()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("reading record failed: %w", err)
		}
		for j := 0; j < len(cols); j++ {
			if test.datums[i][j].ResolvedType() == types.Unknown {
				// If we expect a null value, the row created by the parquet reader
				// will not have the associated column.
				_, ok := row[cols[j].SchemaElement.Name]
				require.Equal(t, ok, false)
				continue
			}
			parquetCol, err := importccl.NewParquetColumn(test.cols[j].Typ, "", false)
			if err != nil {
				return err
			}
			datum, err := parquetCol.DecodeFn(row[cols[j].SchemaElement.Name])
			if err != nil {
				return err
			}

			tester := test.datums[i][j]
			switch tester.ResolvedType().Family() {
			case types.DateFamily:
				// pgDate.orig property doesn't matter and can cause the test to fail
				require.Equal(t, tester.(*tree.DDate).Date.UnixEpochDays(),
					datum.(*tree.DDate).Date.UnixEpochDays())
			case types.JsonFamily:
				// Only the value of the json object matters, not that additional properties
				require.Equal(t, tester.(*tree.DJSON).JSON.String(),
					datum.(*tree.DJSON).JSON.String())
			case types.FloatFamily:
				if tester.(*tree.DFloat).String() == "NaN" {
					// NaN != NaN, therefore stringify the comparison.
					require.Equal(t, "NaN", datum.(*tree.DFloat).String())
					continue
				}
				require.Equal(t, tester, datum)
			default:
				require.Equal(t, tester, datum)
			}
		}
		i++
	}
	return nil
}

func TestRandomParquetExports(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()
	dbName := "rand"

	params := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			UseDatabase:   dbName,
			ExternalIODir: dir,
		},
	}
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3, params)
	defer tc.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])

	sqlDB.Exec(t, fmt.Sprintf("CREATE DATABASE %s", dbName))

	tableName := "table_1"
	// TODO (butler): Randomly generate additional table(s) using tools from PR
	// #75677. The function below only creates a deterministic table with a bunch
	// of interesting corner case values.
	err := randgen.GenerateRandInterestingTable(tc.Conns[0], dbName, tableName)
	require.NoError(t, err)

	s0 := tc.Server(0)
	ie := s0.ExecutorConfig().(sql.ExecutorConfig).InternalExecutor
	{
		// Ensure table only contains columns supported by EXPORT Parquet
		_, cols, err := ie.QueryRowExWithCols(
			ctx,
			"",
			nil,
			sessiondata.InternalExecutorOverride{
				User:     security.RootUserName(),
				Database: dbName},
			fmt.Sprintf("SELECT * FROM %s LIMIT 1", tableName))
		require.NoError(t, err)

		for _, col := range cols {
			_, err := importccl.NewParquetColumn(col.Typ, "", false)
			if err != nil {
				t.Logf("Column type %s not supported in parquet, dropping", col.Typ.String())
				sqlDB.Exec(t, fmt.Sprintf(`ALTER TABLE %s DROP COLUMN %s`, tableName, col.Name))
			}
		}
	}

	// TODO (butler): iterate over random select statements
	test := parquetTest{
		filePrefix: tableName,
		dbName:     dbName,
		dir:        dir,
		stmt: fmt.Sprintf("EXPORT INTO PARQUET 'nodelocal://0/%s' FROM SELECT * FROM %s",
			tableName, tableName),
	}
	sqlDB.Exec(t, test.stmt)
	err = validateParquetFile(t, ctx, ie, test)
	require.NoError(t, err)
}

// TestBasicParquetTypes exports a variety of relations into parquet files, and
// then asserts that the parquet exporter properly encoded the values of the
// crdb relations.
func TestBasicParquetTypes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()
	dbName := "baz"
	params := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			UseDatabase:   dbName,
			ExternalIODir: dir,
		},
	}
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3, params)
	defer tc.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])
	sqlDB.Exec(t, fmt.Sprintf("CREATE DATABASE %s", dbName))

	// instantiating an internal executor to easily get datums from the table
	s0 := tc.Server(0)
	ie := s0.ExecutorConfig().(sql.ExecutorConfig).InternalExecutor

	sqlDB.Exec(t, `CREATE TABLE foo (i INT PRIMARY KEY, x STRING, y INT, z FLOAT NOT NULL, a BOOL, 
INDEX (y))`)
	sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'Alice', 3, 14.3, true), (2, 'Bob', 2, 24.1, 
false),(3, 'Carl', 1, 34.214,true),(4, 'Alex', 3, 14.3, NULL), (5, 'Bobby', 2, 3.4,false),
(6, NULL, NULL, 4.5, NULL)`)

	tests := []parquetTest{
		{
			filePrefix: "basic",
			stmt: `EXPORT INTO PARQUET 'nodelocal://0/basic' FROM SELECT *
							FROM foo WHERE y IS NOT NULL ORDER BY y ASC LIMIT 2 `,
		},
		{
			filePrefix: "null_vals",
			stmt: `EXPORT INTO PARQUET 'nodelocal://0/null_vals' FROM SELECT *
							FROM foo ORDER BY x ASC LIMIT 2`,
		},
		{
			filePrefix: "colname",
			stmt: `EXPORT INTO PARQUET 'nodelocal://0/colname' FROM SELECT avg(z), min(y) AS baz
							FROM foo`,
		},
		{
			filePrefix: "nullable",
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

			filePrefix: "arrays",
			prep: []string{
				"CREATE TABLE atable (i INT PRIMARY KEY, x INT[])",
				"INSERT INTO atable VALUES (1, ARRAY[1,2]), (2, ARRAY[2]), (3,ARRAY[1,13,5]),(4, NULL),(5, ARRAY[])"},
			stmt: `EXPORT INTO PARQUET 'nodelocal://0/arrays' FROM SELECT * FROM atable`,
		},
		{
			filePrefix: "user_types",
			prep: []string{
				"CREATE TYPE greeting AS ENUM ('hello', 'hi')",
				"CREATE TABLE greeting_table (x greeting, y greeting)",
				"INSERT INTO greeting_table VALUES ('hello', 'hello'), ('hi', 'hi')",
			},
			stmt: `EXPORT INTO PARQUET 'nodelocal://0/user_types' FROM SELECT * FROM greeting_table`,
		},
		{
			filePrefix: "collate",
			prep: []string{
				"CREATE TABLE de_names (name STRING COLLATE de PRIMARY KEY)",
				"INSERT INTO de_names VALUES ('Backhaus' COLLATE de), ('Bär' COLLATE de), ('Baz' COLLATE de)",
			},
			stmt: `EXPORT INTO PARQUET 'nodelocal://0/collate' FROM SELECT * FROM de_names ORDER BY name`,
		},
		{
			filePrefix: "ints_floats",
			prep: []string{
				"CREATE TABLE nums (int_2 INT2, int_4 INT4, int_8 INT8, real_0 REAL, double_0 DOUBLE PRECISION)",
				"INSERT INTO nums VALUES (2, 2, 2, 3.2, 3.2)",
			},
			stmt: `EXPORT INTO PARQUET 'nodelocal://0/ints_floats' FROM SELECT * FROM nums`,
		},
		{
			filePrefix: "compress_gzip",
			fileSuffix: ".gz",
			stmt: `EXPORT INTO PARQUET 'nodelocal://0/compress_gzip' WITH compression = gzip
							FROM SELECT * FROM foo`,
		},
		{
			filePrefix: "compress_snappy",
			fileSuffix: ".snappy",
			stmt: `EXPORT INTO PARQUET 'nodelocal://0/compress_snappy' WITH compression = snappy
							FROM SELECT * FROM foo `,
		},
		{
			filePrefix: "uncompress",
			stmt: `EXPORT INTO PARQUET 'nodelocal://0/uncompress'
							FROM SELECT * FROM foo `,
		},
	}

	for _, test := range tests {
		t.Logf("Test %s", test.filePrefix)
		if test.prep != nil {
			for _, cmd := range test.prep {
				sqlDB.Exec(t, cmd)
			}
		}

		sqlDB.Exec(t, test.stmt)
		test.dir = dir
		test.dbName = dbName
		err := validateParquetFile(t, ctx, ie, test)
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
