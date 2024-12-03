// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package importer_test

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/importer"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/parquet"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
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

	// datums provides the expected values of the parquet file.
	datums []tree.Datums
}

// validateParquetFile reads the parquet file and validates various aspects of
// the parquet file.
func validateParquetFile(
	t *testing.T, ctx context.Context, ie isql.Executor, test parquetTest,
) error {
	paths, err := filepath.Glob(filepath.Join(test.dir, test.filePrefix, parquetExportFilePattern+test.fileSuffix))
	require.NoError(t, err)

	require.Equal(t, 1, len(paths))

	// Get the datums returned by the SELECT statement called in the EXPORT
	// PARQUET statement to validate the data in the parquet file.
	validationStmt := strings.SplitN(test.stmt, "FROM ", 2)[1]
	test.datums, test.cols, err = ie.QueryBufferedExWithCols(
		ctx,
		"",
		nil,
		sessiondata.InternalExecutorOverride{
			User:     username.NodeUserName(),
			Database: test.dbName,
		},
		validationStmt)
	require.NoError(t, err)

	// It's possible to have a set of rows where each row has no columns.
	// Ex.    CREATE TABLE public.tabl͛e1 (
	//                rowid INT8 NOT VISIBLE NOT NULL DEFAULT unique_rowid(),
	//                CONSTRAINT tabl͛e1_pkey PRIMARY KEY (rowid ASC)
	//        )
	// In this case, the parquet file will simply contain no datums, but
	// the SELECT above returns a slice of empty slices [[], [], []...].
	// Thus, we override the array of test datums as an empty slice [].
	if len(test.datums) > 0 && len(test.datums[0]) == 0 {
		test.datums = make([]tree.Datums, 0)
	}

	meta, readDatums, err := parquet.ReadFile(paths[0])
	require.NoError(t, err)

	require.Equal(t, len(test.cols), meta.NumCols)
	require.Equal(t, len(test.datums), meta.NumRows)

	i := 0
	for _, row := range readDatums {
		if err != nil {
			return fmt.Errorf("reading record failed: %w", err)
		}
		for j := 0; j < len(test.cols); j++ {
			// If we're encoding a DOidWrapper, then we want to cast the wrapped
			// datum. Note that we don't use eval.UnwrapDatum since we're not
			// interested in evaluating the placeholders.
			// TODO(#104278): use ValidateDatum from util/parquet.
			validateDatum(t, tree.UnwrapDOidWrapper(test.datums[i][j]), tree.UnwrapDOidWrapper(row[j]), test.cols[j].Typ)
		}
		i++
	}
	return nil
}

func validateDatum(t *testing.T, expected tree.Datum, actual tree.Datum, typ *types.T) {
	switch expected.ResolvedType().Family() {
	case types.ArrayFamily:
		eArr := expected.(*tree.DArray)
		aArr := actual.(*tree.DArray)
		for i := 0; i < eArr.Len(); i++ {
			validateDatum(t, tree.UnwrapDOidWrapper(eArr.Array[i]),
				tree.UnwrapDOidWrapper(aArr.Array[i]), typ.ArrayContents())
		}
	case types.DateFamily:
		// pgDate.orig property doesn't matter and can cause the test to fail
		require.Equal(t, expected.(*tree.DDate).Date.UnixEpochDays(),
			actual.(*tree.DDate).Date.UnixEpochDays())
	case types.JsonFamily:
		// Only the value of the json object matters, not that additional properties
		require.Equal(t, expected.(*tree.DJSON).JSON.String(),
			actual.(*tree.DJSON).JSON.String())
	case types.EnumFamily:
		// Only the value of the enum string matters, not that additional properties
		require.Equal(t, expected.(*tree.DEnum).LogicalRep,
			actual.(*tree.DEnum).LogicalRep)
	case types.CollatedStringFamily:
		// Only the value of the collated string matters, not that additional properties
		require.Equal(t, expected.(*tree.DCollatedString).Contents,
			actual.(*tree.DCollatedString).Contents)
	case types.FloatFamily:
		if typ.Equal(types.Float4) && expected.(*tree.DFloat).String() != "NaN" {
			// CRDB currently doesn't truncate non NAN float4's correctly, so this
			// test does it manually :(
			// https://github.com/cockroachdb/cockroach/issues/73743
			e := float32(*expected.(*tree.DFloat))
			a := float32(*expected.(*tree.DFloat))
			require.Equal(t, e, a)
		} else {
			require.Equal(t, expected.String(), actual.String())
		}
	case types.DecimalFamily:
		require.Equal(t, expected.String(), actual.String())

	default:
		require.Equal(t, expected, actual)
	}
}

func TestRandomParquetExports(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer ccl.TestingEnableEnterprise()() // allow usage of partitions

	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()
	dbName := "rand"
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		UseDatabase:   dbName,
		ExternalIODir: dir,
	})
	ctx := context.Background()
	defer srv.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)
	rng, _ := randutil.NewTestRand()
	sqlDB.Exec(t, fmt.Sprintf("CREATE DATABASE %s", dbName))

	var tableName string
	idb := srv.ApplicationLayer().ExecutorConfig().(sql.ExecutorConfig).InternalDB
	// Try at most 10 times to populate a random table with at least 10 rows.
	{
		var (
			i           int
			success     bool
			tablePrefix = "table"
			numTables   = 20
		)

		stmts := randgen.RandCreateTables(
			ctx, rng, tablePrefix, numTables, randgen.TableOptNone,
			randgen.PartialIndexMutator, randgen.ForeignKeyMutator,
		)

		var sb strings.Builder
		for _, stmt := range stmts {
			sb.WriteString(tree.SerializeForDisplay(stmt))
			sb.WriteString(";\n")
		}
		sqlDB.Exec(t, sb.String())

		for i = 0; i < numTables; i++ {
			tableName = string(stmts[i].(*tree.CreateTable).Table.ObjectName)
			numRows, err := randgen.PopulateTableWithRandData(rng, db, tableName, 20, nil)
			require.NoError(t, err)
			if numRows > 5 {
				// Ensure the table only contains columns supported by EXPORT Parquet. If an
				// unsupported column cannot be dropped, try populating another table
				if err := func() error {
					_, cols, err := idb.Executor().QueryRowExWithCols(
						ctx,
						"",
						nil,
						sessiondata.InternalExecutorOverride{
							User:     username.NodeUserName(),
							Database: dbName},
						fmt.Sprintf("SELECT * FROM %s LIMIT 1", tree.NameString(tableName)))
					require.NoError(t, err)

					for _, col := range cols {
						// TODO(#104278): don't call this function to check if a type is supported.
						// We should explicitly use the ones supported by  util/parquet).
						_, err := parquet.NewSchema([]string{"test"}, []*types.T{col.Typ})
						if err != nil {
							_, err = sqlDB.DB.ExecContext(ctx, fmt.Sprintf(`ALTER TABLE %s DROP COLUMN %s`, tree.NameString(tableName), tree.NameString(col.Name)))
							if err != nil {
								return err
							}
						}
					}
					return nil
				}(); err != nil {
					continue
				}
				success = true
				break
			}
		}
		require.Equal(t, true, success, "test flake: failed to create a random table")
	}
	t.Logf("exporting as parquet from random table with schema: \n %s", sqlDB.QueryStr(t, `SHOW CREATE TABLE `+tree.NameString(tableName)))
	// TODO (msbutler): iterate over random select statements
	test := parquetTest{
		filePrefix: "outputfile",
		dbName:     dbName,
		dir:        dir,
		stmt: fmt.Sprintf("EXPORT INTO PARQUET 'nodelocal://1/outputfile' FROM SELECT * FROM %s",
			tree.NameString(tableName)),
	}
	sqlDB.Exec(t, test.stmt)
	err := validateParquetFile(t, ctx, idb.Executor(), test)
	require.NoError(t, err, "failed to validate parquet file")
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
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		UseDatabase:   dbName,
		ExternalIODir: dir,
	})
	ctx := context.Background()
	defer srv.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, fmt.Sprintf("CREATE DATABASE %s", dbName))

	// Instantiating an internal executor to easily get datums from the table.
	ie := srv.ApplicationLayer().ExecutorConfig().(sql.ExecutorConfig).InternalDB.Executor()

	sqlDB.Exec(t, `CREATE TABLE foo (i INT PRIMARY KEY, x STRING, y INT, z FLOAT NOT NULL, a BOOL, 
INDEX (y))`)
	sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'Alice', 3, 0.5032135844230652, true), (2, 'Bob',
	2, CAST('nan' AS FLOAT),false),(3, 'Carl', 1, 0.5032135844230652,true),(4, 'Alex', 3, 14.3, NULL), (5, 
'Bobby', 2, 3.4,false), (6, NULL, NULL, 4.5, NULL)`)

	tests := []parquetTest{
		{
			filePrefix: "basic",
			stmt: `EXPORT INTO PARQUET 'nodelocal://1/basic' FROM SELECT *
							FROM foo WHERE y IS NOT NULL ORDER BY y ASC LIMIT 2 `,
		},
		{
			filePrefix: "null_vals",
			stmt: `EXPORT INTO PARQUET 'nodelocal://1/null_vals' FROM SELECT *
							FROM foo ORDER BY x ASC LIMIT 2`,
		},
		{
			filePrefix: "colname",
			stmt: `EXPORT INTO PARQUET 'nodelocal://1/colname' FROM SELECT avg(z), min(y) AS baz
							FROM foo`,
		},
		{
			filePrefix: "nullable",
			stmt: `EXPORT INTO PARQUET 'nodelocal://1/nullable' FROM SELECT y,z,x
							FROM foo`,
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
			stmt: `EXPORT INTO PARQUET 'nodelocal://1/arrays' FROM SELECT * FROM atable`,
		},
		{
			filePrefix: "user_types",
			prep: []string{
				"CREATE TYPE greeting AS ENUM ('hello', 'hi')",
				"CREATE TABLE greeting_table (x greeting, y greeting)",
				"INSERT INTO greeting_table VALUES ('hello', 'hello'), ('hi', 'hi')",
			},
			stmt: `EXPORT INTO PARQUET 'nodelocal://1/user_types' FROM SELECT * FROM greeting_table`,
		},
		{
			filePrefix: "collate",
			prep: []string{
				"CREATE TABLE de_names (name STRING COLLATE de PRIMARY KEY)",
				"INSERT INTO de_names VALUES ('Backhaus' COLLATE de), ('Bär' COLLATE de), ('Baz' COLLATE de)",
			},
			stmt: `EXPORT INTO PARQUET 'nodelocal://1/collate' FROM SELECT * FROM de_names ORDER BY name`,
		},
		{
			filePrefix: "ints_floats",
			prep: []string{
				"CREATE TABLE nums (int_2 INT2, int_4 INT4, int_8 INT8, real_0 FLOAT4, double_0 FLOAT8)",
				"INSERT INTO nums VALUES (2, 2, 2, 2.107109308242798, 2.107109308242798)",
			},
			stmt: `EXPORT INTO PARQUET 'nodelocal://1/ints_floats' FROM SELECT * FROM nums`,
		},
		{
			filePrefix: "compress_gzip",
			fileSuffix: ".gz",
			stmt: `EXPORT INTO PARQUET 'nodelocal://1/compress_gzip' WITH compression = gzip
							FROM SELECT * FROM foo`,
		},
		{
			filePrefix: "compress_snappy",
			fileSuffix: ".snappy",
			stmt: `EXPORT INTO PARQUET 'nodelocal://1/compress_snappy' WITH compression = snappy
							FROM SELECT * FROM foo `,
		},
		{
			filePrefix: "uncompress",
			stmt: `EXPORT INTO PARQUET 'nodelocal://1/uncompress'
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
		require.NoError(t, err, "failed to validate parquet file")
	}
}

func TestMemoryMonitor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Arrange for a small memory budget.
	budget := int64(4096)
	mm := mon.NewMonitor(mon.Options{
		Name:      mon.MakeMonitorName("test-mm"),
		Limit:     budget,
		Increment: 128, /* small allocation increment */
		Settings:  cluster.MakeTestingClusterSettings(),
	})
	mm.Start(context.Background(), nil, mon.NewStandaloneBudget(budget))

	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		ExternalIODir: dir,
		Knobs: base.TestingKnobs{
			DistSQL: &execinfra.TestingKnobs{
				Export: &importer.ExportTestingKnobs{
					MemoryMonitor: mm,
				},
			},
		},
	})
	ctx := context.Background()
	cleanup := func() {
		s.Stopper().Stop(ctx)
	}
	defer cleanup()

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE TABLE foo(key INT PRIMARY KEY DEFAULT unique_rowid(), val INT)`)
	sqlDB.Exec(t, `INSERT INTO foo (val) SELECT * FROM generate_series(1, 100)`)
	sqlDB.ExpectErr(t, "memory budget exceeded", `EXPORT INTO PARQUET 'nodelocal://1/foo' FROM SELECT * FROM foo`)
}
