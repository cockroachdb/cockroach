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
	gosql "database/sql"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach-go/v2/crdb"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/parquet"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParquetRows(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Rangefeed reader can time out under stress.
	skip.UnderStress(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		// TODO(#98816): cdctest.GetHydratedTableDescriptor does not work with tenant dbs.
		// Once it is fixed, this flag can be removed.
		DefaultTestTenant: base.TestTenantDisabled,
	})
	defer s.Stopper().Stop(ctx)

	maxRowGroupSize := int64(2)

	sqlDB := sqlutils.MakeSQLRunner(db)

	for _, tc := range []struct {
		testName    string
		createTable string
		inserts     []string
	}{
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

				encodingOpts := changefeedbase.EncodingOptions{}

				if writer == nil {
					writer, err = newParquetWriterFromRow(updatedRow, f, encodingOpts, &TestingKnobs{EnableParquetMetadata: true}, parquet.WithMaxRowGroupLength(maxRowGroupSize),
						parquet.WithCompressionCodec(parquet.CompressionGZIP))
					if err != nil {
						t.Fatalf(err.Error())
					}
					numCols = len(updatedRow.ResultColumns()) + 1
				}

				err = writer.addData(updatedRow, prevRow, hlc.Timestamp{}, hlc.Timestamp{})
				require.NoError(t, err)

				// Save a copy of the datums we wrote.
				datumRow := make([]tree.Datum, writer.schemaDef.NumColumns())
				err = populateDatums(updatedRow, prevRow, encodingOpts, hlc.Timestamp{}, hlc.Timestamp{}, datumRow)
				require.NoError(t, err)
				datums[i] = datumRow
			}

			err = writer.close()
			require.NoError(t, err)

			parquet.ReadFileAndVerifyDatums(t, f.Name(), numRows, numCols, writer.inner, datums)
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

func TestChangefeedUpdatedTimestamps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		ctx := context.Background()
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (0)`)

		foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH updated, resolved, format=parquet`)
		defer closeFeed(t, foo)

		// Grab the first non resolved-timestamp row.
		var row0 *cdctest.TestFeedMessage
		for {
			var err error
			row0, err = foo.Next()
			assert.NoError(t, err)
			if len(row0.Value) > 0 {
				break
			}
		}

		// If this changefeed uses jobs (and thus stores a ChangefeedDetails), get
		// the statement timestamp from row0 and verify that they match. Otherwise,
		// just skip the row.
		if jf, ok := foo.(cdctest.EnterpriseTestFeed); ok {
			d, err := jf.Details()
			assert.NoError(t, err)
			expected := `{"after": {"a": 0}, "updated": "` + d.StatementTime.AsOfSystemTime() + `"}`
			assert.Equal(t, expected, string(row0.Value))
		}

		// Assert the remaining key using assertPayloads, since we know the exact
		// timestamp expected.
		var ts1 string
		if err := crdb.ExecuteTx(ctx, s.DB, nil /* txopts */, func(tx *gosql.Tx) error {
			return tx.QueryRow(
				`INSERT INTO foo VALUES (1) RETURNING cluster_logical_timestamp()`,
			).Scan(&ts1)
		}); err != nil {
			t.Fatal(err)
		}
		assertPayloads(t, foo, []string{
			`foo: [1]->{"after": {"a": 1}, "updated": "` + ts1 + `"}`,
		})

		// Check that we eventually get a resolved timestamp greater than ts1.
		parsed := parseTimeToHLC(t, ts1)
		for {
			if resolved, _ := expectResolvedTimestamp(t, foo); parsed.Less(resolved) {
				break
			}
		}
	}

	cdcTest(t, testFn, feedTestForceSink("cloudstorage"))
}
