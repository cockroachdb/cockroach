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
	"fmt"
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
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
	} {
		t.Run(tc.testName, func(t *testing.T) {
			sqlDB.Exec(t, tc.createTable)

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

			numInserts := len(tc.inserts)
			for _, insertStmt := range tc.inserts {
				sqlDB.Exec(t, insertStmt)
			}
			for i := 0; i < numInserts; i++ {
				v := popRow(t)

				updatedRow, err := decoder.DecodeKV(
					ctx, roachpb.KeyValue{Key: v.Key, Value: v.Value}, cdcevent.CurrentRow, v.Timestamp(), false)
				if err != nil {
					t.Fatalf(err.Error())
				}

				sch, err := NewParquetSchema(updatedRow)
				if err != nil {
					t.Fatalf(err.Error())
				}

				f, err := os.Create("./blah.txt")
				require.NoError(t, err)

				writer := NewParquetWriter(sch, f)
				err = writer.AddData(updatedRow)
				require.NoError(t, err)
			}
		})
	}

}

func TestParquetGolden(t *testing.T) {
	fileName := "./../../../PARQUETFILE.txt"
	f, err := os.Create(fileName)
	if err != nil {
		t.Fatal(err)
	}

	numCols := 10
	numRowGroups := 5

	fields := make([]schema.Node, numCols)
	for i := 0; i < numCols; i++ {
		name := fmt.Sprintf("column_%d", i)
		fields[i], _ = schema.NewPrimitiveNode(name, parquet.Repetitions.Optional, parquet.Types.ByteArray, -1, 12)
	}
	node, _ := schema.NewGroupNode("schema", parquet.Repetitions.Required, fields, -1)
	sch := schema.NewSchema(node)

	opts := []parquet.WriterProperty{parquet.WithCreatedBy("cockroachdb")}

	props := parquet.NewWriterProperties(opts...)

	writer := file.NewParquetWriter(f, sch.Root(), file.WithWriterProps(props))

	for rg := 0; rg < numRowGroups; rg++ {
		rgw := writer.AppendBufferedRowGroup()
		for col := 0; col < numCols; col++ {
			cw, err := rgw.Column(col)
			if err != nil {
				t.Fatal(err)
			}
			switch w := cw.(type) {
			case *file.ByteArrayColumnChunkWriter:
				_, err := w.WriteBatch([]parquet.ByteArray{parquet.ByteArray(fmt.Sprintf("rg-%d-col-%d", rg, col))}, []int16{int16(1)}, nil)
				if err != nil {
					t.Fatal(err)
				}
			default:
				if err != nil {
					t.Fatal("non int32")
				}
			}
			err = cw.Close()
			if err != nil {
				t.Fatal("non int32")
			}
		}
		err := rgw.Close()
		if err != nil {
			t.Fatal("non int32")
		}
	}

	err = writer.Close()
	require.NoError(t, err)

	// reader

	f, err = os.Open(fileName)
	require.NoError(t, err)

	reader, err := file.NewParquetReader(f)
	require.NoError(t, err)

	assert.EqualValues(t, numRowGroups, reader.NumRowGroups())
	valueCount := int64(1)
	valuesOut := make([]parquet.ByteArray, valueCount)

	for r := 0; r < reader.NumRowGroups(); r++ {
		rgr := reader.RowGroup(r)
		assert.EqualValues(t, numCols, rgr.NumColumns())
		assert.EqualValues(t, valueCount, rgr.NumRows())

		for col := 0; col < numCols; col++ {
			var totalRead int64
			col, err := rgr.Column(0)
			assert.NoError(t, err)
			colReader := col.(*file.ByteArrayColumnChunkReader)
			for colReader.HasNext() {
				total, _, _ := colReader.ReadBatch(valueCount-totalRead, valuesOut[totalRead:], nil, nil)
				totalRead += total
			}
			fmt.Println(valuesOut)
		}
	}
}
