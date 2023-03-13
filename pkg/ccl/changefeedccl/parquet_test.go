// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	"math"
	"os"
	"testing"

	"github.com/apache/arrow/go/v11/parquet"
	"github.com/apache/arrow/go/v11/parquet/file"
	"github.com/apache/arrow/go/v11/parquet/schema"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParquetTypes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Rangefeed reader can time out under stress.
	skip.UnderStress(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		// TODO(#98816): cdctest.GetHydratedTableDescriptor does not work with tenant dbs.
		// Once it is fixed, this flag can be removed.
		DisableDefaultTestTenant: true,
	})
	defer s.Stopper().Stop(ctx)

	maxRowGroupSize := int64(2)

	MaxParquetRowGroupSize.Override(context.Background(), &s.ClusterSettings().SV, maxRowGroupSize)

	sqlDB := sqlutils.MakeSQLRunner(db)

	for _, tc := range []struct {
		testName    string
		createTable string
		inserts     []string
	}{
		{
			testName:    "bool-family",
			createTable: `CREATE TABLE foo (id int primary key, booleanColumn boolean)`,
			inserts:     []string{`INSERT INTO foo VALUES(0, false)`, `INSERT INTO foo VALUES(1, NULL)`},
		},
		{
			testName:    "string-family",
			createTable: `CREATE TABLE foo (id int primary key, char16Col CHAR(7), charCol CHAR, varChar20Col VARCHAR(20), strVal STRING)`,
			inserts: []string{
				`INSERT INTO foo VALUES(0, 'charVal', 'c', 'varCharVal', 'stringVal')`,
				`INSERT INTO foo VALUES(1, NULL, NULL, NULL, NULL)`,
			},
		},
		{
			testName:    "timestamp-family",
			createTable: `CREATE TABLE foo (id int primary key, tsCol TIMESTAMP)`,
			inserts:     []string{`INSERT INTO foo VALUES(0, now())`, `INSERT INTO foo VALUES(1, NULL)`},
		},
		{
			testName:    "decimal-family",
			createTable: `CREATE TABLE foo (id int primary key, dec122Col DECIMAL(12,2), dec44Col DECIMAL(4,4), dec100Col DECIMAL(10, 0))`,
			inserts: []string{
				`INSERT INTO foo VALUES(0, 12.00, -0.2222, 0.00000001)`,
				`INSERT INTO foo VALUES(1, NULL, NULL, NULL)`,
				`INSERT INTO foo VALUES(2, 'inf', NULL, NULL)`,
			},
		},
		{
			testName:    "uuid-family",
			createTable: `CREATE TABLE foo (id int primary key, uidCol UUID)`,
			inserts: []string{
				`INSERT INTO foo VALUES(0, gen_random_uuid())`,
				`INSERT INTO foo VALUES(1, NULL)`,
				`INSERT INTO foo VALUES(2, gen_random_uuid())`,
			},
		},
		{
			testName: "mixed",
			createTable: `CREATE TABLE foo (
    	int32Col INT4 PRIMARY KEY,
      varCharCol VARCHAR(16) ,
      charCol CHAR(2),
      tsCol TIMESTAMP ,
      stringCol STRING ,
      decimalCOl DECIMAL(12,2),
      uuidCol UUID
  )`,
			inserts: []string{
				`INSERT INTO foo values (0, 'zero', 'CA', now(), 'oiwjfoijsdjif', 'inf', gen_random_uuid())`,
				`INSERT INTO foo values (1, 'one', 'NY', now(), 'sdi9fu90d', '-1.90', gen_random_uuid())`,
				`INSERT INTO foo values (2, 'two', 'WA', now(), 'sd9fid9fuj', '0.01', gen_random_uuid())`,
				`INSERT INTO foo values (3, 'three', 'ON', now(), 'sadklfhkdlsjf', '1.2', gen_random_uuid())`,
				`INSERT INTO foo values (4, 'four', 'NS', now(), '123123', '-11222221.2', gen_random_uuid())`,
				`INSERT INTO foo values (5, 'five', 'BC', now(), 'sadklfhkdlsjf', '1.2', gen_random_uuid())`,
				`INSERT INTO foo values (6, 'siz', 'AB', now(), '123123', '-11222221.2', gen_random_uuid())`,
			},
		},
	} {
		t.Run(tc.testName, func(t *testing.T) {
			sqlDB.Exec(t, tc.createTable)
			defer func() {
				sqlDB.Exec(t, "DROP TABLE foo")
			}()

			popRow, cleanup, decoder := makeRangefeedReaderAndDecoder(t, s)
			defer cleanup()

			fileName := "TestParquetTypes"
			var writer *ParquetWriter
			var numCols int
			f, err := os.CreateTemp(os.TempDir(), fileName)
			require.NoError(t, err)

			numRows := len(tc.inserts)
			for _, insertStmt := range tc.inserts {
				sqlDB.Exec(t, insertStmt)
			}

			datums := make([][]tree.Datum, numRows)
			for i := 0; i < numRows; i++ {
				v := popRow(t)

				updatedRow, err := decoder.DecodeKV(
					ctx, roachpb.KeyValue{Key: v.Key, Value: v.Value}, cdcevent.CurrentRow, v.Timestamp(), false)
				require.NoError(t, err)

				prevRow, err := decoder.DecodeKV(
					ctx, roachpb.KeyValue{Key: v.Key, Value: v.PrevValue}, cdcevent.PrevRow, v.Timestamp(), false)
				require.NoError(t, err)

				if writer == nil {
					writer, err = NewCDCParquetWriterFromRow(updatedRow, f, s.ClusterSettings())
					if err != nil {
						t.Fatalf(err.Error())
					}
					numCols = len(writer.sch.cols)
				}

				idx := 0
				datumRow := make([]tree.Datum, numCols)
				err = updatedRow.ForEachColumn().Datum(func(d tree.Datum, _ cdcevent.ResultColumn) error {
					datumRow[idx] = d
					idx += 1
					return nil
				})
				datumRow[idx] = getEventTypeDatum(updatedRow, prevRow)
				require.NoError(t, err)
				datums[i] = datumRow

				err = writer.AddData(updatedRow, prevRow)
				require.NoError(t, err)
			}

			err = writer.Close()
			require.NoError(t, err)

			numRowGroups := int(math.Ceil(float64(numRows) / float64(maxRowGroupSize)))
			readFileAndVerifyDatums(t, f.Name(), numRows, numCols, numRowGroups, datums)
		})
	}
}

func makeRangefeedReaderAndDecoder(
	t *testing.T, s serverutils.TestServerInterface,
) (func(t *testing.T) *kvpb.RangeFeedValue, func(), cdcevent.Decoder) {
	tableDesc := cdctest.GetHydratedTableDescriptor(t, s.ExecutorConfig(), "foo")
	popRow, cleanup := cdctest.MakeRangeFeedValueReader(t, s.ExecutorConfig(), tableDesc)
	targets := changefeedbase.Targets{}
	targets.Add(changefeedbase.Target{
		Type:       jobspb.ChangefeedTargetSpecification_PRIMARY_FAMILY_ONLY,
		TableID:    tableDesc.GetID(),
		FamilyName: "primary",
	})
	sqlExecCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	ctx := context.Background()
	decoder, err := cdcevent.NewEventDecoder(ctx, &sqlExecCfg, targets, false, false)
	require.NoError(t, err)
	return popRow, cleanup, decoder
}

// readFileAndVerifyDatums reads the parquet file and asserts its contents match the provided
// metadata and datums.
func readFileAndVerifyDatums(
	t *testing.T,
	parquetFileName string,
	numRows int,
	numCols int,
	numRowGroups int,
	writtenDatums [][]tree.Datum,
) {
	f, err := os.Open(parquetFileName)
	require.NoError(t, err)

	reader, err := file.NewParquetReader(f)
	require.NoError(t, err)

	assert.Equal(t, reader.NumRows(), int64(numRows))
	assert.Equal(t, reader.MetaData().Schema.NumColumns(), numCols)
	assert.Equal(t, reader.MetaData().Version().String(), "v2.6")
	assert.EqualValues(t, numRowGroups, reader.NumRowGroups())

	readDatums := make([][]tree.Datum, numRows)
	for i := 0; i < numRows; i++ {
		readDatums[i] = make([]tree.Datum, numCols)
	}

	startingRowIdx := 0
	for rg := 0; rg < reader.NumRowGroups(); rg++ {
		rgr := reader.RowGroup(rg)
		rowsInRowGroup := rgr.NumRows()
		defLevels := make([]int16, rowsInRowGroup)

		for colIdx := 0; colIdx < numCols; colIdx++ {
			col, err := rgr.Column(colIdx)
			require.NoError(t, err)
			switch col.Type() {
			case parquet.Types.Boolean:
				colReader := col.(*file.BooleanColumnChunkReader)
				values := make([]bool, rowsInRowGroup)
				numRead, _, err := colReader.ReadBatch(rowsInRowGroup, values, defLevels, nil)
				require.NoError(t, err)
				require.Equal(t, rowsInRowGroup, numRead)

				decodeValuesIntoDatumsHelper(t, readDatums, colIdx, startingRowIdx, parquetBoolDecoder, values, defLevels)
			case parquet.Types.Int32:
				colReader := col.(*file.Int32ColumnChunkReader)
				values := make([]int32, numRows)
				numRead, _, err := colReader.ReadBatch(rowsInRowGroup, values, defLevels, nil)
				require.NoError(t, err)
				require.Equal(t, rowsInRowGroup, numRead)
				decodeValuesIntoDatumsHelper(t, readDatums, colIdx, startingRowIdx, parquetInt32Decoder, values, defLevels)
			case parquet.Types.Int64:
				values := make([]int64, rowsInRowGroup)
				colReader := col.(*file.Int64ColumnChunkReader)
				numRead, _, err := colReader.ReadBatch(rowsInRowGroup, values, defLevels, nil)
				require.NoError(t, err)
				require.Equal(t, rowsInRowGroup, numRead)
				var dec parquetDecodeFn
				switch typ := col.Descriptor().LogicalType().(type) {
				case *schema.TimestampLogicalType:
					dec = parquetTimestampDecoder
				case *schema.IntLogicalType:
					dec = parquetInt64Decoder
				default:
					panic(errors.Newf("unimplemented logical type %s", typ))
				}
				decodeValuesIntoDatumsHelper(t, readDatums, colIdx, startingRowIdx, dec, values, defLevels)
			case parquet.Types.Int96:
				panic("unimplemented")
			case parquet.Types.Float:
				panic("unimplemented")
			case parquet.Types.Double:
				panic("unimplemented")
			case parquet.Types.ByteArray:
				colReader := col.(*file.ByteArrayColumnChunkReader)
				values := make([]parquet.ByteArray, rowsInRowGroup)
				numRead, _, err := colReader.ReadBatch(rowsInRowGroup, values, defLevels, nil)
				require.NoError(t, err)
				require.Equal(t, rowsInRowGroup, numRead)

				var dec parquetDecodeFn
				switch typ := col.Descriptor().LogicalType().(type) {
				case *schema.DecimalLogicalType:
					dec = parquetDecimalDecoder
				case schema.StringLogicalType:
					dec = parquetStringDecoder
				default:
					panic(errors.Newf("unimplemented logical type %s", typ))
				}
				decodeValuesIntoDatumsHelper(t, readDatums, colIdx, startingRowIdx, dec, values, defLevels)
			case parquet.Types.FixedLenByteArray:
				colReader := col.(*file.FixedLenByteArrayColumnChunkReader)
				values := make([]parquet.FixedLenByteArray, rowsInRowGroup)
				numRead, _, err := colReader.ReadBatch(rowsInRowGroup, values, defLevels, nil)
				require.NoError(t, err)
				require.Equal(t, rowsInRowGroup, numRead)
				var dec parquetDecodeFn
				switch typ := col.Descriptor().LogicalType().(type) {
				case schema.UUIDLogicalType:
					dec = parquetUUIDDecoder
				default:
					panic(errors.Newf("unimplemented logical type %s", typ))
				}
				decodeValuesIntoDatumsHelper(t, readDatums, colIdx, startingRowIdx, dec, values, defLevels)
			}
		}
		startingRowIdx += int(rowsInRowGroup)
	}
	require.NoError(t, reader.Close())

	for i := 0; i < numRows; i++ {
		for j := 0; j < numCols; j++ {
			assert.Equal(t, writtenDatums[i][j], readDatums[i][j])
		}
	}
}

func decodeValuesIntoDatumsHelper[V bool | int32 | int64 | parquet.ByteArray | parquet.FixedLenByteArray](
	t *testing.T,
	datums [][]tree.Datum,
	colIdx int,
	startingRowIdx int,
	dec parquetDecodeFn,
	values []V,
	defLevels []int16,
) {
	var err error
	// If the defLevel of a datum is 0, parquet will not write it to the column.
	// Use valueReadIdx to only read from the front of the values array, where datums are defined.
	valueReadIdx := 0
	for rowOffset, defLevel := range defLevels {
		d := tree.DNull
		if defLevel != 0 {
			d, err = dec(values[valueReadIdx])
			require.NoError(t, err)
			valueReadIdx++
		}
		datums[startingRowIdx+rowOffset][colIdx] = d
	}
}
