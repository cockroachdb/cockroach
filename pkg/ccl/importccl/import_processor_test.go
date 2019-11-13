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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
)

type testSpec struct {
	inputFormat roachpb.IOFileFormat_FileFormat
	inputs      map[int32]string
	tables      map[string]*execinfrapb.ReadImportDataSpec_ImportTable
}

// Given test spec returns ReadImportDataSpec suitable creating input converter.
func (spec *testSpec) getConverterSpec() *execinfrapb.ReadImportDataSpec {
	pgDumpOptions := roachpb.PgDumpOptions{MaxRowSize: 64 * 1024}
	return &execinfrapb.ReadImportDataSpec{
		Format:            roachpb.IOFileFormat{Format: spec.inputFormat, PgDump: pgDumpOptions},
		Tables:            spec.tables,
		Uri:               spec.inputs,
		ReaderParallelism: 1, // Make tests deterministic
	}
}

func TestConverterFlushesBatches(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Reset batch size setting upon test completion.
	defer row.TestingSetDatumRowConverterBatchSize(0)()

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

	tests := []testSpec{
		newTestSpec(t, roachpb.IOFileFormat_CSV, "testdata/csv/data-0"),
		newTestSpec(t, roachpb.IOFileFormat_Mysqldump, "testdata/mysqldump/simple.sql"),
		newTestSpec(t, roachpb.IOFileFormat_PgDump, "testdata/pgdump/simple.sql"),
	}

	const endBatchSize = -1

	for _, testCase := range tests {
		expectedNumRecords := 0
		expectedNumBatches := 0
		converterSpec := testCase.getConverterSpec()

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
					defer close(kvCh)
					return conv.readFiles(ctx, testCase.inputs, converterSpec.Format, externalStorageFactory)
				})

				lastBatch := 0
				testNumRecords := 0
				testNumBatches := 0

				// Read from the channel; we expect batches of testCase.batchSize size, with the exception of the last batch.
				for batch := range kvCh {
					if batchSize > 0 {
						assert.True(t, lastBatch == 0 || lastBatch == batchSize)
					}
					lastBatch = len(batch.KVs)
					testNumRecords += lastBatch
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
					batchSize += (batchSize << 2)
					expectedNumBatches = int(math.Ceil(float64(expectedNumRecords) / float64(batchSize)))
				}
			})
		}
	}
}

// A RowReceiver implementation which fails the test if it receives an error.
type errorReportingRowReceiver struct {
	t *testing.T
}

var _ execinfra.RowReceiver = &errorReportingRowReceiver{}

func (r *errorReportingRowReceiver) Push(
	row sqlbase.EncDatumRow, meta *execinfrapb.ProducerMetadata,
) execinfra.ConsumerStatus {
	if r.t.Failed() || (meta != nil && meta.Err != nil) {
		if !r.t.Failed() {
			r.t.Fail()
		}
		r.t.Logf("Row receiver got an error: %v", meta.Err)
		return execinfra.ConsumerClosed
	}
	return execinfra.NeedMoreRows
}

func (r *errorReportingRowReceiver) ProducerDone() {}
func (r *errorReportingRowReceiver) Types() []types.T {
	return nil
}

// A do nothing BulkAdder implementation
type doNothingBulkAdder struct{}

var doNothingAdder storagebase.BulkAdder = &doNothingBulkAdder{}

func (*doNothingBulkAdder) Add(_ context.Context, _ roachpb.Key, _ []byte) error { return nil }
func (*doNothingBulkAdder) Flush(_ context.Context) error                        { return nil }
func (*doNothingBulkAdder) IsEmpty() bool                                        { return true }
func (*doNothingBulkAdder) CurrentBufferFill() float32                           { return 0 }
func (*doNothingBulkAdder) GetSummary() roachpb.BulkOpSummary                    { return roachpb.BulkOpSummary{} }
func (*doNothingBulkAdder) Close(_ context.Context)                              {}
func (*doNothingBulkAdder) SetOnFlush(func())                                    {}

func doNothingBulkAdderFactory(
	_ context.Context, _ *client.DB, _ hlc.Timestamp, _ storagebase.BulkAdderOptions,
) (storagebase.BulkAdder, error) {
	return doNothingAdder, nil
}

var eofOffset int64 = math.MaxInt64

func TestImportIgnoresProcessedFiles(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	testServer, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer testServer.Stopper().Stop(ctx)

	evalCtx := tree.MakeTestingEvalContext(nil)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings:        testServer.ClusterSettings(),
			ExternalStorage: externalStorageFactory,
			BulkAdder:       doNothingBulkAdderFactory,
		},
	}

	// In this test, we'll specify import files that do not exist, but mark those files fully processed.
	// The converters should not attempt to even open these files (and if they do, we should report a test failure)
	tests := []struct {
		name         string
		spec         testSpec
		inputOffsets []int64 // List of file ids that were fully processed
	}{
		{
			"csv-two-invalid",
			newTestSpec(t, roachpb.IOFileFormat_CSV, "__invalid__", "testdata/csv/data-0", "/_/missing/_"),
			[]int64{eofOffset, 0, eofOffset},
		},
		{
			"csv-all-invalid",
			newTestSpec(t, roachpb.IOFileFormat_CSV, "__invalid__", "../../&"),
			[]int64{eofOffset, eofOffset},
		},
		{
			"csv-all-valid",
			newTestSpec(t, roachpb.IOFileFormat_CSV, "testdata/csv/data-0"),
			[]int64{0},
		},
		{
			"mysql-one-invalid",
			newTestSpec(t, roachpb.IOFileFormat_Mysqldump, "testdata/mysqldump/simple.sql", "/_/missing/_"),
			[]int64{0, eofOffset},
		},
		{
			"pgdump-one-input",
			newTestSpec(t, roachpb.IOFileFormat_PgDump, "testdata/pgdump/simple.sql"),
			[]int64{0},
		},
	}

	// Configures import spec to have appropriate input offsets set.
	setInputOffsets := func(
		t *testing.T, spec *execinfrapb.ReadImportDataSpec, offsets []int64,
	) *execinfrapb.ReadImportDataSpec {
		if len(spec.Uri) != len(offsets) {
			t.Fatal("Expected matching number of input offsets")
		}
		spec.ResumePos = make(map[int32]int64)
		for id, offset := range offsets {
			if offset > 0 {
				spec.ResumePos[int32(id)] = offset
			}
		}
		return spec
	}

	for _, testCase := range tests {
		t.Run(fmt.Sprintf("processes-files-once-%s", testCase.name), func(t *testing.T) {
			spec := setInputOffsets(t, testCase.spec.getConverterSpec(), testCase.inputOffsets)

			processor, err := newReadImportDataProcessor(flowCtx, 0, *spec, &errorReportingRowReceiver{t})

			if err != nil {
				t.Fatalf("Could not create data processor: %v", err)
			}

			processor.Run(ctx)
		})
	}
}

// External storage factory needed to run converters.
func externalStorageFactory(
	ctx context.Context, dest roachpb.ExternalStorage,
) (cloud.ExternalStorage, error) {
	workdir, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	return cloud.MakeExternalStorage(ctx, dest, nil, blobs.TestBlobServiceClient(workdir))
}

// Helper to create and initialize testSpec.
func newTestSpec(
	t *testing.T, inputFormat roachpb.IOFileFormat_FileFormat, inputs ...string,
) testSpec {
	spec := testSpec{
		inputFormat: inputFormat,
		inputs:      make(map[int32]string),
	}

	// Initialize table descriptor for import.
	// We need valid descriptor to run converters, even though we don't actually import anything in this test.
	var descr *sqlbase.TableDescriptor
	switch inputFormat {
	case roachpb.IOFileFormat_CSV:
		descr = descForTable(t, "CREATE TABLE simple (i INT, s text)", 10, 20, NoFKs)
	case roachpb.IOFileFormat_Mysqldump, roachpb.IOFileFormat_PgDump:
		descr = descForTable(t, "CREATE TABLE simple (i INT, s text, b bytes default null)", 10, 20, NoFKs)
	default:
		t.Fatalf("Unsupported input format: %v", inputFormat)
	}

	targetCols := make([]string, len(descr.Columns))
	numCols := 0
	for i, col := range descr.Columns {
		if !col.Hidden {
			targetCols[i] = col.Name
			numCols++
		}
	}
	assert.True(t, numCols > 0)

	spec.tables = map[string]*execinfrapb.ReadImportDataSpec_ImportTable{
		"simple": {Desc: descr, TargetCols: targetCols[0:numCols]},
	}

	for id, path := range inputs {
		spec.inputs[int32(id)] = cloud.MakeLocalStorageURI(path)
	}

	return spec
}
