// Copyright 2022 The Cockroach Authors.
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
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/importer"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/stretchr/testify/require"
)

const parquetExportFilePattern = "export*-n*.0.parquet"

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

	cols := fr.GetSchemaDefinition().RootColumn.Children

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
			Database: test.dbName,
		},
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
			parquetCol, err := importer.NewParquetColumn(test.cols[j].Typ, "", false)
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
				Database: dbName,
			},
			fmt.Sprintf("SELECT * FROM %s LIMIT 1", tableName))
		require.NoError(t, err)

		for _, col := range cols {
			_, err := importer.NewParquetColumn(col.Typ, "", false)
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
				parquet.FieldRepetitionType_OPTIONAL,
			},
		},
		{
			// TODO (mb): switch one of the values in the array to NULL once the
			// vendor's parquet file reader bug resolves.
			// https://github.com/fraugster/parquet-go/issues/60

			filePrefix: "arrays",
			prep: []string{
				"CREATE TABLE atable (i INT PRIMARY KEY, x INT[])",
				"INSERT INTO atable VALUES (1, ARRAY[1,2]), (2, ARRAY[2]), (3,ARRAY[1,13,5]),(4, NULL),(5, ARRAY[])",
			},
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
