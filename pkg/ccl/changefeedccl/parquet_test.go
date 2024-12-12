// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"slices"
	"testing"

	"github.com/cockroachdb/apd/v3"
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

	maxRowGroupSize := int64(4)

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, "SET CLUSTER SETTING kv.rangefeed.enabled = true")

	newDecimal := func(s string) *tree.DDecimal {
		d, _, _ := apd.NewFromString(s)
		return &tree.DDecimal{Decimal: *d}
	}

	for _, tc := range []struct {
		testName          string
		createTable       string
		stmts             []string
		expectedDatumRows [][]tree.Datum
	}{
		{
			testName: "decimal",
			createTable: `
				CREATE TABLE foo (
				i INT PRIMARY KEY,
				d DECIMAL(18,9)
				)
				`,
			stmts: []string{
				`INSERT INTO foo VALUES (0, 0)`,
				`DELETE FROM foo WHERE d = 0.0`,
				`INSERT INTO foo VALUES (1, 1.000000000)`,
				`UPDATE foo SET d = 2.000000000 WHERE d = 1.000000000`,
				`INSERT INTO foo VALUES (2, 3.14)`,
				`INSERT INTO foo VALUES (3, 1.234567890123456789)`,
				`INSERT INTO foo VALUES (4, '-Inf'::DECIMAL)`,
				`INSERT INTO foo VALUES (5, 'Inf'::DECIMAL)`,
				`INSERT INTO foo VALUES (6, 'NaN'::DECIMAL)`,
			},
			expectedDatumRows: [][]tree.Datum{
				{tree.NewDInt(0), newDecimal("0.000000000"), parquetEventTypeDatumStringMap[parquetEventInsert]},
				{tree.NewDInt(0), tree.DNull, parquetEventTypeDatumStringMap[parquetEventDelete]},
				{tree.NewDInt(1), newDecimal("1.000000000"), parquetEventTypeDatumStringMap[parquetEventInsert]},
				{tree.NewDInt(1), newDecimal("2.000000000"), parquetEventTypeDatumStringMap[parquetEventUpdate]},
				{tree.NewDInt(2), newDecimal("3.140000000"), parquetEventTypeDatumStringMap[parquetEventInsert]},
				{tree.NewDInt(3), newDecimal("1.234567890"), parquetEventTypeDatumStringMap[parquetEventInsert]},
				{tree.NewDInt(4), tree.DNegInfDecimal, parquetEventTypeDatumStringMap[parquetEventInsert]},
				{tree.NewDInt(5), tree.DPosInfDecimal, parquetEventTypeDatumStringMap[parquetEventInsert]},
				{tree.NewDInt(6), tree.DNaNDecimal, parquetEventTypeDatumStringMap[parquetEventInsert]},
			},
		},
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
				`INSERT INTO foo VALUES (3,   'd1', '5a02bd48-ba64-4134-9199-844c1517f723')`,
				`INSERT INTO foo VALUES (4,   'e1', '5a02bd48-ba64-4134-9199-844c1517f724')`,
				`INSERT INTO foo VALUES (5,   'f1', '5a02bd48-ba64-4134-9199-844c1517f725')`,
				`INSERT INTO foo VALUES (6,   'g1', '5a02bd48-ba64-4134-9199-844c1517f726')`,
				`INSERT INTO foo VALUES (7,   'h1', '5a02bd48-ba64-4134-9199-844c1517f727')`,
				`UPDATE foo SET stringCol = 'changed' WHERE int32Col = 3`,
				`INSERT INTO foo VALUES (9,   'j1', '5a02bd48-ba64-4134-9199-844c1517f729')`,
				`INSERT INTO foo VALUES (10,   'k1', '5a02bd48-ba64-4134-9199-844c1517f712')`,
				`INSERT INTO foo VALUES (11,   'l1', '5a02bd48-ba64-4134-9199-844c1517f713')`,
				`DELETE FROM foo WHERE int32Col = 4`,
				`INSERT INTO foo VALUES (12,   'm1', '5a02bd48-ba64-4134-9199-844c1517f714')`,
				`INSERT INTO foo VALUES (13,   'n1', '5a02bd48-ba64-4134-9199-844c1517f715')`,
				`INSERT INTO foo VALUES (14,   'o1', '5a02bd48-ba64-4134-9199-844c1517f716')`,
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
				{tree.NewDInt(3), tree.NewDString("d1"),
					&tree.DUuid{UUID: uuid.FromStringOrNil("5a02bd48-ba64-4134-9199-844c1517f723")}, parquetEventTypeDatumStringMap[parquetEventInsert]},
				{tree.NewDInt(4), tree.NewDString("e1"),
					&tree.DUuid{UUID: uuid.FromStringOrNil("5a02bd48-ba64-4134-9199-844c1517f724")}, parquetEventTypeDatumStringMap[parquetEventInsert]},
				{tree.NewDInt(5), tree.NewDString("f1"),
					&tree.DUuid{UUID: uuid.FromStringOrNil("5a02bd48-ba64-4134-9199-844c1517f725")}, parquetEventTypeDatumStringMap[parquetEventInsert]},
				{tree.NewDInt(6), tree.NewDString("g1"),
					&tree.DUuid{UUID: uuid.FromStringOrNil("5a02bd48-ba64-4134-9199-844c1517f726")}, parquetEventTypeDatumStringMap[parquetEventInsert]},
				{tree.NewDInt(7), tree.NewDString("h1"),
					&tree.DUuid{UUID: uuid.FromStringOrNil("5a02bd48-ba64-4134-9199-844c1517f727")}, parquetEventTypeDatumStringMap[parquetEventInsert]},
				{tree.NewDInt(3), tree.NewDString("changed"),
					&tree.DUuid{UUID: uuid.FromStringOrNil("5a02bd48-ba64-4134-9199-844c1517f723")}, parquetEventTypeDatumStringMap[parquetEventUpdate]},
				{tree.NewDInt(9), tree.NewDString("j1"),
					&tree.DUuid{UUID: uuid.FromStringOrNil("5a02bd48-ba64-4134-9199-844c1517f729")}, parquetEventTypeDatumStringMap[parquetEventInsert]},
				{tree.NewDInt(10), tree.NewDString("k1"),
					&tree.DUuid{UUID: uuid.FromStringOrNil("5a02bd48-ba64-4134-9199-844c1517f712")}, parquetEventTypeDatumStringMap[parquetEventInsert]},
				{tree.NewDInt(11), tree.NewDString("l1"),
					&tree.DUuid{UUID: uuid.FromStringOrNil("5a02bd48-ba64-4134-9199-844c1517f713")}, parquetEventTypeDatumStringMap[parquetEventInsert]},
				{tree.NewDInt(4), tree.DNull, tree.DNull, parquetEventTypeDatumStringMap[parquetEventDelete]},
				{tree.NewDInt(12), tree.NewDString("m1"),
					&tree.DUuid{UUID: uuid.FromStringOrNil("5a02bd48-ba64-4134-9199-844c1517f714")}, parquetEventTypeDatumStringMap[parquetEventInsert]},
				{tree.NewDInt(13), tree.NewDString("n1"),
					&tree.DUuid{UUID: uuid.FromStringOrNil("5a02bd48-ba64-4134-9199-844c1517f715")}, parquetEventTypeDatumStringMap[parquetEventInsert]},
				{tree.NewDInt(14), tree.NewDString("o1"),
					&tree.DUuid{UUID: uuid.FromStringOrNil("5a02bd48-ba64-4134-9199-844c1517f716")}, parquetEventTypeDatumStringMap[parquetEventInsert]},
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
						t.Fatal(err)
					}
					numCols = len(updatedRow.ResultColumns()) + 1
				}

				err = writer.addData(updatedRow, prevRow, hlc.Timestamp{}, hlc.Timestamp{})
				require.NoError(t, err)

				// Flush every 3 rows on average.
				if rand.Float32() < 0.33 {
					require.NoError(t, writer.flush())
				}

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
			sortFn := func(a []tree.Datum, b []tree.Datum) int {
				cmp, err := a[0].Compare(ctx, &eval.Context{}, b[0])
				require.NoError(t, err)
				return cmp
			}
			slices.SortStableFunc(datums, sortFn)
			slices.SortStableFunc(readDatums, sortFn)
			for r := 0; r < numRows; r++ {
				t.Logf("comparing row expected: %s to actual: %s\n", datums[r], readDatums[r])
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

func TestParquetDuplicateColumns(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE t (id INT8 PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO t VALUES (1)`)
		foo := feed(t, f, `CREATE CHANGEFEED WITH format=parquet,initial_scan='only' AS SELECT id FROM t`)
		defer closeFeed(t, foo)

		// Test that this should not fail with this error:
		// `Number of datums in parquet output row doesn't match number of distinct
		// columns, Expected: %d, Recieved: %d`.
		assertPayloads(t, foo, []string{
			`t: [1]->{"id": 1}`,
		})
	}

	cdcTest(t, testFn, feedTestForceSink("cloudstorage"))
}

func TestParquetSpecifiedDuplicateQueryColumns(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE t (id INT8 PRIMARY KEY, a INT8)`)
		sqlDB.Exec(t, `INSERT INTO t VALUES (1, 9)`)
		foo := feed(t, f, `CREATE CHANGEFEED WITH format=parquet,initial_scan='only' AS SELECT a, a, id, id FROM t`)
		defer closeFeed(t, foo)

		// Test that this should not fail with this error:
		// `Number of datums in parquet output row doesn't match number of distinct
		// columns, Expected: %d, Recieved: %d`.
		assertPayloads(t, foo, []string{
			`t: [1]->{"a": 9, "a_1": 9, "id": 1, "id_1": 1}`,
		})
	}

	cdcTest(t, testFn, feedTestForceSink("cloudstorage"))
}

func TestParquetNoUserDefinedPrimaryKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE t (id INT8)`)
		var rowId int
		sqlDB.QueryRow(t, `INSERT INTO t VALUES (0) RETURNING rowid`).Scan(&rowId)
		foo := feed(t, f, `CREATE CHANGEFEED WITH format=parquet,initial_scan='only' AS SELECT id FROM t`)
		defer closeFeed(t, foo)

		// The parquet output always includes the primary key.
		assertPayloads(t, foo, []string{
			fmt.Sprintf(`t: [%d]->{"id": 0}`, rowId),
		})
	}

	cdcTest(t, testFn, feedTestForceSink("cloudstorage"))
}
