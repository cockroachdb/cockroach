// Copyright 2019 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestConverterFlushesBatches(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Reset batch size setting upon test completion.
	defer row.TestingSetDatumRowConverterBatchSize(0)()

	// Returns a map of file id -> URI for the specified test inputs.
	makeInputsSpec := func(inputs ...string) map[int32]string {
		base, err := os.Getwd()
		if err != nil {
			t.Fatalf("Failed to get working dir: %v", err)
		}
		res := make(map[int32]string)
		for id, path := range inputs {
			if res[int32(id)], err = cloud.MakeLocalStorageURI(filepath.Join(base, path)); err != nil {
				t.Fatalf("Failed to make local uri: %v", err)
			}
		}
		return res
	}

	// Helper to create external storage
	externalStorage := func(ctx context.Context, dest roachpb.ExternalStorage) (cloud.ExternalStorage, error) {
		return cloud.MakeExternalStorage(ctx, dest, nil)
	}

	// Returns an import spec for the specified "create table" statement.
	makeTablesSpec :=
		func(t *testing.T, createStmt string) map[string]*execinfrapb.ReadImportDataSpec_ImportTable {
			descr := descForTable(t, createStmt, 10, 20, NoFKs)
			targetCols := make([]string, len(descr.Columns))
			numCols := 0
			for i, col := range descr.Columns {
				if !col.Hidden {
					targetCols[i] = col.Name
					numCols++
				}
			}
			assert.True(t, numCols > 0)
			return map[string]*execinfrapb.ReadImportDataSpec_ImportTable{
				"simple": {Desc: descr, TargetCols: targetCols[0:numCols]},
			}
		}

		// Helper to generate test name.
	testName := func(inputFormat roachpb.IOFileFormat_FileFormat, batchSize int) string {
		switch batchSize {
		case 0:
			return fmt.Sprintf("%s-default-batch-size", inputFormat)
		case 1:
			return fmt.Sprintf("%s-always-flush", inputFormat)
		default:
			return fmt.Sprintf("%s-flush-%d-records", inputFormat, batchSize)
		}
	}

	ctx := context.Background()
	evalCtx := tree.MakeTestingEvalContext(nil)

	tests := []struct {
		inputFormat roachpb.IOFileFormat_FileFormat
		inputs      map[int32]string
		tables      map[string]*execinfrapb.ReadImportDataSpec_ImportTable
	}{
		{
			inputFormat: roachpb.IOFileFormat_CSV,
			inputs:      makeInputsSpec("testdata/csv/data-0"),
			tables:      makeTablesSpec(t, `CREATE TABLE simple (i INT, s text)`),
		},
		{
			inputFormat: roachpb.IOFileFormat_Mysqldump,
			inputs:      makeInputsSpec("testdata/mysqldump/simple.sql"),
			tables:      makeTablesSpec(t, `CREATE TABLE simple (i INT, s text, b bytes default null)`),
		},
		{
			inputFormat: roachpb.IOFileFormat_PgDump,
			inputs:      makeInputsSpec("testdata/pgdump/simple.sql"),
			tables:      makeTablesSpec(t, `CREATE TABLE simple (i INT, s text, b bytes default null)`),
		},
	}

	const endBatchSize = -1

	for _, testCase := range tests {
		expectedNumRecords := 0
		expectedNumBatches := 0
		pgDumpOptions := roachpb.PgDumpOptions{MaxRowSize: 64 * 1024}
		converterSpec := &execinfrapb.ReadImportDataSpec{
			Format:            roachpb.IOFileFormat{Format: testCase.inputFormat, PgDump: pgDumpOptions},
			Tables:            testCase.tables,
			Uri:               testCase.inputs,
			ReaderParallelism: 1, // Make tests deterministic
		}

		// Run multiple tests, increasing batch size until it exceeds the total number of records.
		// When batch size is 0, we run converters with the default batch size, and use that run
		// to figure out the expected number of records and batches for the subsequent run.
		for batchSize := 0; batchSize != endBatchSize; {
			t.Run(testName(testCase.inputFormat, batchSize), func(t *testing.T) {
				if batchSize > 0 {
					row.TestingSetDatumRowConverterBatchSize(batchSize)
				}

				kvCh := make(chan row.KVBatch, batchSize)
				conv, err := makeInputConverter(converterSpec, &evalCtx, kvCh)

				if err != nil {
					t.Fatalf("makeInputConverter() error = %v", err)
				}

				group := ctxgroup.WithContext(ctx)

				group.Go(func() error {
					defer conv.inputFinished(ctx)
					return conv.readFiles(ctx, testCase.inputs, converterSpec.Format, externalStorage)
				})

				conv.start(group)

				lastBatch := 0
				testNumRecords := 0
				testNumBatches := 0

				// Read from the channel; we expect batches of testCase.batchSize size, with the exception of the last batch.
				for batch := range kvCh {
					if batchSize > 0 {
						assert.True(t, lastBatch == 0 || lastBatch == batchSize)
					}
					lastBatch = len(batch.KVs)
					testNumRecords = testNumRecords + lastBatch
					testNumBatches++
				}

				if err := group.Wait(); err != nil {
					t.Fatalf("Conversion failed: %v", err)
				}

				if batchSize == 0 {
					expectedNumRecords = testNumRecords
					// Next batch: flush every record.
					batchSize = 1
					expectedNumBatches = expectedNumRecords
				} else if batchSize > expectedNumRecords {
					// Done with this test case.
					batchSize = endBatchSize
					return
				} else {
					// Number of records and batches ought to be correct.
					assert.Equal(t, expectedNumRecords, testNumRecords)
					assert.Equal(t, expectedNumBatches, testNumBatches)

					// Progressively increase the batch size.
					batchSize = batchSize + (batchSize << 2)
					expectedNumBatches = int(math.Ceil(float64(expectedNumRecords) / float64(batchSize)))
				}
			})
		}
	}
}
