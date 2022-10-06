package changefeedccl

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/importer"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/stretchr/testify/require"
)

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

	validationStmt string

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
	t *testing.T, ctx context.Context, ie *sql.InternalExecutor, test parquetTest, path string,
) error {
	r, err := os.Open(path)
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
	test.datums, test.cols, err = ie.QueryBufferedExWithCols(
		ctx,
		"",
		nil,
		sessiondata.InternalExecutorOverride{
			User:     username.RootUserName(),
			Database: test.dbName,
		},
		test.validationStmt)
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

			// If we're encoding a DOidWrapper, then we want to cast the wrapped datum.
			// Note that we pass in nil as the first argument since we're not interested
			// in evaluating the placeholders.
			validateDatum(t, eval.UnwrapDatum(nil, test.datums[i][j]), eval.UnwrapDatum(nil, datum),
				test.cols[j].Typ)
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
			validateDatum(t, eArr.Array[i], aArr.Array[i], typ.ArrayContents())
		}
	case types.DateFamily:
		// pgDate.orig property doesn't matter and can cause the test to fail
		require.Equal(t, expected.(*tree.DDate).Date.UnixEpochDays(),
			actual.(*tree.DDate).Date.UnixEpochDays())
	case types.JsonFamily:
		// Only the value of the json object matters, not that additional properties
		require.Equal(t, expected.(*tree.DJSON).JSON.String(),
			actual.(*tree.DJSON).JSON.String())
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

func TestBasicParquetTypes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()
	dbName := "baz"
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		// Test fails when run within a test tenant. More
		// investigation is required. Tracked with #76378.
		DisableDefaultTestTenant: true,
		UseDatabase:              dbName,
		ExternalIODir:            dir,
	})
	ctx := context.Background()
	defer srv.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.ExecMultiple(t, strings.Split(serverSetupStatements, ";")...)

	sqlDB.Exec(t, fmt.Sprintf("CREATE DATABASE %s", dbName))

	// instantiating an internal executor to easily get datums from the table
	ie := srv.ExecutorConfig().(sql.ExecutorConfig).InternalExecutor

	sqlDB.Exec(t, `CREATE TABLE foo (id INT PRIMARY KEY, city STRING)`)
	sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'Berlin')`)
	// sqlDB.Exec(t, `CREATE TABLE foo (i INT PRIMARY KEY, x STRING, y INT, z FLOAT NOT NULL, a BOOL,
	// INDEX (y))`)
	// sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'Alice', 3, 0.5032135844230652, true), (2, 'Bob',
	// 2, CAST('nan' AS FLOAT),false),(3, 'Carl', 1, 0.5032135844230652,true),(4, 'Alex', 3, 14.3, NULL), (5,
	// 'Bobby', 2, 3.4,false), (6, NULL, NULL, 4.5, NULL)`)

	tests := []parquetTest{
		{
			filePrefix:     "basic",
			stmt:           `CREATE CHANGEFEED for foo into 'nodelocal://0/basic' with initial_scan='only',format='parquet',key_in_value`,
			validationStmt: `select * from foo`,
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
		time.Sleep(3 * time.Second)
		test.dir = dir
		test.dbName = dbName
		paths, err := filepath.Glob(filepath.Join(test.dir, test.filePrefix))
		require.NoError(t, err)
		require.Equal(t, 1, len(paths))
		walkFn := func(path string, info fs.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				return nil
			}
			if strings.HasSuffix(path, `parquet`) {
				fmt.Printf("xkcd: got file: %s\n", path)
				return validateParquetFile(t, ctx, ie, test, path)
			} else {
				fmt.Printf("xkcd: file %s not have parquet\n", path)
			}
			return nil
		}
		err = filepath.Walk(paths[0], walkFn)
		require.NoError(t, err, "failed to validate parquet file")
	}
}
