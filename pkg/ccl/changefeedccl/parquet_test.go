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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
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

	// TODO this does not work with tenants.
	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		DisableDefaultTestTenant: true,
	})
	defer s.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(db)

	for _, tc := range []struct {
		testName    string
		createTable string
		inserts     []string
	}{
		{
			testName:    "boolean",
			createTable: `CREATE TABLE foo (id int primary key, booleanColumn boolean)`,
			inserts:     []string{`INSERT INTO foo VALUES(0, false)`},
		},
		{
			testName:    "char",
			createTable: `CREATE TABLE foo (id int primary key, char16Col CHAR(7))`,
			inserts:     []string{`INSERT INTO foo VALUES(0, 'charVal')`},
		},
		{
			testName:    "varchar",
			createTable: `CREATE TABLE foo (id int primary key, varChar20Col VARCHAR(20))`,
			inserts:     []string{`INSERT INTO foo VALUES(0, 'varCharVal')`},
		},
		{
			testName:    "timestamp",
			createTable: `CREATE TABLE foo (id int primary key, tsCol TIMESTAMP)`,
			inserts:     []string{`INSERT INTO foo VALUES(0, now())`},
		},
		{
			testName:    "nulls",
			createTable: `CREATE TABLE foo (id int primary key, intCol int, boolCol bool)`,
			inserts:     []string{`INSERT INTO foo VALUES(0, NULL, NULL)`},
		},
		{
			testName: "tpcc",
			createTable: `CREATE TABLE foo (
      c_id INT8 NOT NULL,
      c_d_id INT8 NOT NULL,
      c_w_id INT8 NOT NULL,
      c_first VARCHAR(16) NOT NULL,
      c_middle CHAR(2) NOT NULL,
      c_last VARCHAR(16) NOT NULL,
      c_street_1 VARCHAR(20) NOT NULL,
      c_street_2 VARCHAR(20) NOT NULL,
      c_city VARCHAR(20) NOT NULL,
      c_state CHAR(2) NOT NULL,
      c_zip CHAR(9) NOT NULL,
      c_phone CHAR(16) NOT NULL,
      c_since TIMESTAMP NOT NULL,
      c_credit CHAR(2) NOT NULL,
      c_credit_lim DECIMAL(12,2) NOT NULL,
      c_discount DECIMAL(4,4) NOT NULL,
      c_balance DECIMAL(12,2) NOT NULL,
      c_ytd_payment DECIMAL(12,2) NOT NULL,
      c_payment_cnt INT8 NOT NULL,
      c_delivery_cnt INT8 NOT NULL,
      c_data VARCHAR(500) NOT NULL,
      CONSTRAINT customer_pkey PRIMARY KEY (c_w_id ASC, c_d_id ASC, c_id ASC)
  )
`,
			inserts: []string{`INSERT INTO foo values (1, 1, 1, 'first', 'oe', 'last', '53', 'beck dr', 'new york',                    
        'CA', '100011001', '1231231231231231', '2006-01-02 15:04:05', 'GC', 'inf', 0.1569, -10.00,10.00,  1, 0, 
        'BahdejZrKB2O3Hzk13xWSP8P9fwb2ZjtZAs3NbYdihFxFime6B6Adnt5jrXvRR7OGYhlpdljbDvShaRF4E9zNHsJ7ZvyiJ3n2X1f4fJoMgn5buTDyUmQupcYMoPylHqYo89SqHqQ4HFVNpmnIWHyIowzQN2r4uSQJ8PYVLLLZk9Epp6cNEnaVrN3JXcrBCOuRRSlC0zvh9lctkhRvAvE5H6TtiDNPEJrcjAUOegvQ1Ol7SuF7jPf275wNDlEbdC58hrunlPfhoY1dORoIgb0VnxqkqbEWTXujHUOvCRfqCdVyc8gRGMfAd4nWB1rXYANQ0fa6ZQJJI2uTeFFazaVwxnN13XunKGV6AwCKxhJQVgXWaljKLJ7r175FAuGYFLyxJvnAUXEp2waty')`,
				`INSERT INTO foo values (2, 2, 2, 'first', 'oe', 'last', '53', 'beck dr', 'new york',                    
        'CA', '100011001', '1231231231231231', '2006-01-02 15:04:05', 'GC', 50000.00, 0.1569, -10.00,10.00,  1, 0, 
        'BahdejZrKB2O3Hzk13xWSP8P9fwb2ZjtZAs3NbYdihFxFime6B6Adnt5jrXvRR7OGYhlpdljbDvShaRF4E9zNHsJ7ZvyiJ3n2X1f4fJoMgn5buTDyUmQupcYMoPylHqYo89SqHqQ4HFVNpmnIWHyIowzQN2r4uSQJ8PYVLLLZk9Epp6cNEnaVrN3JXcrBCOuRRSlC0zvh9lctkhRvAvE5H6TtiDNPEJrcjAUOegvQ1Ol7SuF7jPf275wNDlEbdC58hrunlPfhoY1dORoIgb0VnxqkqbEWTXujHUOvCRfqCdVyc8gRGMfAd4nWB1rXYANQ0fa6ZQJJI2uTeFFazaVwxnN13XunKGV6AwCKxhJQVgXWaljKLJ7r175FAuGYFLyxJvnAUXEp2waty')`,
				`INSERT INTO foo values (3, 3, 3, 'first', 'oe', 'last', '53', 'beck dr', 'new york',                    
        'CA', '100011001', '1231231231231231', '2006-01-02 15:04:05', 'GC', 50000.00, 0.1569, -10.00,10.00,  1, 0, 
        'BahdejZrKB2O3Hzk13xWSP8P9fwb2ZjtZAs3NbYdihFxFime6B6Adnt5jrXvRR7OGYhlpdljbDvShaRF4E9zNHsJ7ZvyiJ3n2X1f4fJoMgn5buTDyUmQupcYMoPylHqYo89SqHqQ4HFVNpmnIWHyIowzQN2r4uSQJ8PYVLLLZk9Epp6cNEnaVrN3JXcrBCOuRRSlC0zvh9lctkhRvAvE5H6TtiDNPEJrcjAUOegvQ1Ol7SuF7jPf275wNDlEbdC58hrunlPfhoY1dORoIgb0VnxqkqbEWTXujHUOvCRfqCdVyc8gRGMfAd4nWB1rXYANQ0fa6ZQJJI2uTeFFazaVwxnN13XunKGV6AwCKxhJQVgXWaljKLJ7r175FAuGYFLyxJvnAUXEp2waty')`,
				`INSERT INTO foo values (4, 4, 4, 'first', 'oe', 'last', '53', 'beck dr', 'new york',                    
        'CA', '100011001', '1231231231231231', '2006-01-02 15:04:05', 'GC', 50000.00, 0.1569, -10.00,10.00,  1, 0, 
        'BahdejZrKB2O3Hzk13xWSP8P9fwb2ZjtZAs3NbYdihFxFime6B6Adnt5jrXvRR7OGYhlpdljbDvShaRF4E9zNHsJ7ZvyiJ3n2X1f4fJoMgn5buTDyUmQupcYMoPylHqYo89SqHqQ4HFVNpmnIWHyIowzQN2r4uSQJ8PYVLLLZk9Epp6cNEnaVrN3JXcrBCOuRRSlC0zvh9lctkhRvAvE5H6TtiDNPEJrcjAUOegvQ1Ol7SuF7jPf275wNDlEbdC58hrunlPfhoY1dORoIgb0VnxqkqbEWTXujHUOvCRfqCdVyc8gRGMfAd4nWB1rXYANQ0fa6ZQJJI2uTeFFazaVwxnN13XunKGV6AwCKxhJQVgXWaljKLJ7r175FAuGYFLyxJvnAUXEp2waty')`,
			},
		},
	} {
		t.Run(tc.testName, func(t *testing.T) {
			sqlDB.Exec(t, tc.createTable)
			defer func() {
				sqlDB.Exec(t, "DROP TABLE foo")
			}()

			tableDesc := cdctest.GetHydratedTableDescriptor(t, s.ExecutorConfig(), "foo")
			popRow, cleanup := cdctest.MakeRangeFeedValueReader(t, s.ExecutorConfig(), tableDesc)
			defer cleanup()

			targets := changefeedbase.Targets{}
			targets.Add(changefeedbase.Target{
				Type:       jobspb.ChangefeedTargetSpecification_PRIMARY_FAMILY_ONLY,
				TableID:    tableDesc.GetID(),
				FamilyName: "primary",
			})
			sqlExecCfg := s.ExecutorConfig().(sql.ExecutorConfig)
			ctx := context.Background()
			decoder, err := cdcevent.NewEventDecoder(ctx, &sqlExecCfg, targets, false, false)
			if err != nil {
				t.Fatalf(err.Error())
			}

			fileName := "./../../../TestParquetTypes.txt"
			var writer *ParquetWriter
			var numCols int
			f, err := os.Create(fileName)
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
					writer, err = NewCDCParquetWriterFromRow(updatedRow, f)
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

			require.True(t, writer.CurrentSize() > 0)

			f, err = os.Open(fileName)
			require.NoError(t, err)

			reader, err := file.NewParquetReader(f)
			require.NoError(t, err)

			assert.Equal(t, reader.NumRows(), int64(numRows))
			assert.Equal(t, reader.MetaData().Schema.NumColumns(), numCols)
			assert.Equal(t, reader.MetaData().Version().String(), "v2.6")
			assert.EqualValues(t, 1, reader.NumRowGroups())

			read(t, reader, numRows, numCols, datums)
		})
	}
}

func read(t *testing.T, reader *file.Reader, numRows int, numCols int, datums [][]tree.Datum) {
	readDatums := make([][]tree.Datum, numRows)
	for i := 0; i < numRows; i++ {
		readDatums[i] = make([]tree.Datum, numCols)
	}

	defLevels := make([]int16, numRows)
	for r := 0; r < reader.NumRowGroups(); r++ {
		rgr := reader.RowGroup(r)

		for i := 0; i < numCols; i++ {
			col, err := rgr.Column(i)
			require.NoError(t, err)
			switch col.Type() {
			case parquet.Types.Boolean:
				colReader := col.(*file.BooleanColumnChunkReader)
				values := make([]bool, numRows)
				for colReader.HasNext() {
					_, _, err := colReader.ReadBatch(int64(numRows), values, defLevels, nil)
					require.NoError(t, err)
				}
				for rowIdx, v := range values {
					d := tree.DNull
					if defLevels[rowIdx] != 0 {
						d, err = parquetBoolDecoder(v)
						require.NoError(t, err)
					}
					readDatums[rowIdx][i] = d
				}
			case parquet.Types.Int32:
				colReader := col.(*file.Int32ColumnChunkReader)
				values := make([]int32, numRows)
				for colReader.HasNext() {
					_, _, err := colReader.ReadBatch(int64(numRows), values, defLevels, nil)
					require.NoError(t, err)
				}
				for rowIdx, v := range values {
					d := tree.DNull
					if defLevels[rowIdx] != 0 {
						d, err = parquetInt32Decoder(v)
						require.NoError(t, err)
					}
					readDatums[rowIdx][i] = d
				}
			case parquet.Types.Int64:
				values := make([]int64, numRows)
				colReader := col.(*file.Int64ColumnChunkReader)
				for colReader.HasNext() {
					_, _, err := colReader.ReadBatch(int64(numRows), values, defLevels, nil)
					require.NoError(t, err)
				}
				var dec decodeFn
				switch typ := col.Descriptor().LogicalType().(type) {
				case *schema.TimestampLogicalType:
					dec = parquetTimestampDecoder
				case *schema.IntLogicalType:
					dec = parquetInt64Decoder
				default:
					panic(errors.Newf("unimplemented logical type %s", typ))
				}
				for rowIdx, v := range values {
					d := tree.DNull
					if defLevels[rowIdx] != 0 {
						d, err = dec(v)
						require.NoError(t, err)
					}
					readDatums[rowIdx][i] = d
				}
			case parquet.Types.Int96:
				panic("unimplemented")
			case parquet.Types.Float:
				panic("unimplemented")
			case parquet.Types.Double:
				panic("unimplemented")
			case parquet.Types.ByteArray:
				colReader := col.(*file.ByteArrayColumnChunkReader)
				values := make([]parquet.ByteArray, numRows)
				for colReader.HasNext() {
					_, _, err := colReader.ReadBatch(int64(numRows), values, defLevels, nil)
					require.NoError(t, err)
				}
				var dec decodeFn
				switch typ := col.Descriptor().LogicalType().(type) {
				case *schema.DecimalLogicalType:
					dec = parquetDecimalDecoder
				case schema.StringLogicalType:
					dec = parquetStringDecoder
				default:
					panic(errors.Newf("unimplemented logical type %s", typ))
				}
				for rowIdx, v := range values {
					d := tree.DNull
					if defLevels[rowIdx] != 0 {
						d, err = dec(v)
						require.NoError(t, err)
					}
					readDatums[rowIdx][i] = d
				}
			case parquet.Types.FixedLenByteArray:
				colReader := col.(*file.FixedLenByteArrayColumnChunkReader)
				values := make([]parquet.FixedLenByteArray, numRows)
				for colReader.HasNext() {
					_, _, err := colReader.ReadBatch(int64(numRows), values, defLevels, nil)
					require.NoError(t, err)
				}
				var dec decodeFn
				switch typ := col.Descriptor().LogicalType().(type) {
				case *schema.UUIDLogicalType:
					dec = parquetUUIDDecoder
				default:
					panic(errors.Newf("unimplemented logical type %s", typ))
				}
				for rowIdx, v := range values {
					d := tree.DNull
					if defLevels[rowIdx] != 0 {
						d, err = dec(v)
						require.NoError(t, err)
					}
					readDatums[rowIdx][i] = d
				}
			}
		}
	}

	require.NoError(t, reader.Close())

	for i := 0; i < numRows; i++ {
		for j := 0; j < numCols; j++ {
			assert.Equal(t, datums[i][j], readDatums[i][j])
		}
	}
}
