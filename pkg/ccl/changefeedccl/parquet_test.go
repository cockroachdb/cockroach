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

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/parquet"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

// TestParquetRows tests that the parquetWriter correctly writes datums. It does
// this by setting up a rangefeed on a table wih data and verifying the writer
// writes the correct datums the parquet file.
func TestParquetRows(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Rangefeed reader can time out under stress.
	skip.UnderStress(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		// TODO(#98816): cdctest.GetHydratedTableDescriptor does not work with tenant dbs.
		// Once it is fixed, this flag can be removed.
		DefaultTestTenant: base.TODOTestTenantDisabled,
	})
	defer s.Stopper().Stop(ctx)

	maxRowGroupSize := int64(2)

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, "SET CLUSTER SETTING kv.rangefeed.enabled = true")

	for _, tc := range []struct {
		testName          string
		createTable       string
		stmts             []string
		expectedDatumRows [][]tree.Datum
	}{
		{
			testName: "mixed",
			createTable: `
				CREATE TABLE foo (
				int32Col INT4 PRIMARY KEY,
				stringCol STRING,
				uuidCol UUID
				)
		    `,
			stmts: []string{
				`INSERT INTO foo VALUES (0, 'a1', '2fec7a4b-0a78-40ce-92e0-d1c0fac70436')`,
				`INSERT INTO foo VALUES (1,   'b1', '0ce43188-e4a9-4b73-803b-a253abc57e6b')`,
				`INSERT INTO foo VALUES (2,   'c1', '5a02bd48-ba64-4134-9199-844c1517f722')`,
				`UPDATE foo SET stringCol = 'changed' WHERE int32Col = 1`,
				`DELETE FROM foo WHERE int32Col = 0`,
			},
			expectedDatumRows: [][]tree.Datum{
				{tree.NewDInt(0), tree.NewDString("a1"),
					&tree.DUuid{UUID: uuid.FromStringOrNil("2fec7a4b-0a78-40ce-92e0-d1c0fac70436")},
					parquetEventTypeDatumStringMap[parquetEventInsert]},
				{tree.NewDInt(1), tree.NewDString("b1"),
					&tree.DUuid{UUID: uuid.FromStringOrNil("0ce43188-e4a9-4b73-803b-a253abc57e6b")},
					parquetEventTypeDatumStringMap[parquetEventInsert]},
				{tree.NewDInt(2), tree.NewDString("c1"),
					&tree.DUuid{UUID: uuid.FromStringOrNil("5a02bd48-ba64-4134-9199-844c1517f722")},
					parquetEventTypeDatumStringMap[parquetEventInsert]},
				{tree.NewDInt(1), tree.NewDString("changed"),
					&tree.DUuid{UUID: uuid.FromStringOrNil("0ce43188-e4a9-4b73-803b-a253abc57e6b")},
					parquetEventTypeDatumStringMap[parquetEventUpdate]},
				{tree.NewDInt(0), tree.DNull, tree.DNull, parquetEventTypeDatumStringMap[parquetEventDelete]},
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

			fileName := "TestParquetRows"
			var writer *parquetWriter
			var numCols int
			f, err := os.CreateTemp(os.TempDir(), fileName)
			require.NoError(t, err)
			defer func() {
				if t.Failed() {
					t.Logf("leaving %s for inspection", f.Name())
				} else {
					if err := os.Remove(f.Name()); err != nil {
						t.Logf("could not cleanup temp file %s: %s", f.Name(), err)
					}
				}
			}()

			numRows := len(tc.stmts)
			for _, insertStmt := range tc.stmts {
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

				encodingOpts := changefeedbase.EncodingOptions{}

				if writer == nil {
					writer, err = newParquetWriterFromRow(updatedRow, f, encodingOpts, parquet.WithMaxRowGroupLength(maxRowGroupSize),
						parquet.WithCompressionCodec(parquet.CompressionGZIP))
					if err != nil {
						t.Fatalf(err.Error())
					}
					numCols = len(updatedRow.ResultColumns()) + 1
				}

				err = writer.addData(updatedRow, prevRow, hlc.Timestamp{}, hlc.Timestamp{})
				require.NoError(t, err)

				datums[i] = tc.expectedDatumRows[i]
			}

			err = writer.close()
			require.NoError(t, err)

			meta, readDatums, err := parquet.ReadFile(f.Name())
			require.NoError(t, err)
			require.Equal(t, meta.NumRows, numRows)
			require.Equal(t, meta.NumCols, numCols)
			// NB: Rangefeeds have per-key ordering, so the rows in the parquet
			// file may not match the order we insert them. To accommodate for
			// this, sort the expected and actual datums by the primary key.
			slices.SortFunc(datums, func(a []tree.Datum, b []tree.Datum) bool {
				return a[0].Compare(&eval.Context{}, b[0]) == -1
			})
			slices.SortFunc(readDatums, func(a []tree.Datum, b []tree.Datum) bool {
				return a[0].Compare(&eval.Context{}, b[0]) == -1
			})
			for r := 0; r < numRows; r++ {
				for c := 0; c < numCols; c++ {
					parquet.ValidateDatum(t, datums[r][c], readDatums[r][c])
				}
			}
		})
	}
}

func makeRangefeedReaderAndDecoder(
	t *testing.T, s serverutils.TestServerInterface,
) (func(t testing.TB) *kvpb.RangeFeedValue, func(), cdcevent.Decoder) {
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

// TestParquetResolvedTimestamps runs tests a changefeed with format=parquet and
// resolved timestamps enabled.
func TestParquetResolvedTimestamps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)

		foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH format=parquet, resolved='10ms'`)
		defer closeFeed(t, foo)

		firstResolved, _ := expectResolvedTimestamp(t, foo)
		testutils.SucceedsSoon(t, func() error {
			nextResolved, _ := expectResolvedTimestamp(t, foo)
			if !firstResolved.Less(nextResolved) {
				return errors.AssertionFailedf(
					"expected resolved timestamp %s to eventually exceed timestamp %s",
					nextResolved, firstResolved)
			}
			return nil
		})
	}

	cdcTest(t, testFn, feedTestForceSink("cloudstorage"))
}
