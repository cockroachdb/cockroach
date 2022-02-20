// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package importer

import (
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"net/url"
	"os"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/nodelocal"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testSpec struct {
	format roachpb.IOFileFormat
	inputs map[int32]string
	tables map[string]*execinfrapb.ReadImportDataSpec_ImportTable
}

// Given test spec returns ReadImportDataSpec suitable creating input converter.
func (spec *testSpec) getConverterSpec() *execinfrapb.ReadImportDataSpec {
	return &execinfrapb.ReadImportDataSpec{
		Format:            spec.format,
		Tables:            spec.tables,
		Uri:               spec.inputs,
		ReaderParallelism: 1, // Make tests deterministic
	}
}

func TestConverterFlushesBatches(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// Reset batch size setting upon test completion.
	defer row.TestingSetDatumRowConverterBatchSize(0)()

	// Helper to generate test name.
	testName := func(format roachpb.IOFileFormat, batchSize int) string {
		switch batchSize {
		case 0:
			return fmt.Sprintf("%s-default-batch-size", format.Format)
		case 1:
			return fmt.Sprintf("%s-always-flush", format.Format)
		default:
			return fmt.Sprintf("%s-flush-%d-records", format.Format, batchSize)
		}
	}

	ctx := context.Background()
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())

	tests := []testSpec{
		newTestSpec(ctx, t, csvFormat(), "testdata/csv/data-0"),
		newTestSpec(ctx, t, mysqlDumpFormat(), "testdata/mysqldump/simple.sql"),
		newTestSpec(ctx, t, pgDumpFormat(), "testdata/pgdump/simple.sql"),
		newTestSpec(ctx, t, avroFormat(t, roachpb.AvroOptions_OCF), "testdata/avro/simple.ocf"),
	}

	const endBatchSize = -1

	for _, testCase := range tests {
		expectedNumRecords := 0
		expectedNumBatches := 0
		converterSpec := testCase.getConverterSpec()

		// Run multiple tests, increasing batch size until it exceeds the
		// total number of records. When batch size is 0, we run converters
		// with the default batch size, and use that run to figure out the
		// expected number of records and batches for the subsequent run.
		for batchSize := 0; batchSize != endBatchSize; {
			t.Run(testName(testCase.format, batchSize), func(t *testing.T) {
				if batchSize > 0 {
					row.TestingSetDatumRowConverterBatchSize(batchSize)
				}

				kvCh := make(chan row.KVBatch, batchSize)
				semaCtx := tree.MakeSemaContext()
				conv, err := makeInputConverter(ctx, &semaCtx, converterSpec, &evalCtx, kvCh,
					nil /* seqChunkProvider */)
				if err != nil {
					t.Fatalf("makeInputConverter() error = %v", err)
				}

				group := ctxgroup.WithContext(ctx)
				group.Go(func() error {
					defer close(kvCh)
					return conv.readFiles(ctx, testCase.inputs, nil, converterSpec.Format,
						externalStorageFactory, security.RootUserName())
				})

				lastBatch := 0
				testNumRecords := 0
				testNumBatches := 0

				// Read from the channel; we expect batches of testCase.batchSize
				// size, with the exception of the last batch.
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
	row rowenc.EncDatumRow, meta *execinfrapb.ProducerMetadata,
) execinfra.ConsumerStatus {
	if r.t.Failed() || (meta != nil && meta.Err != nil) {
		if !r.t.Failed() {
			r.t.Fail()
		}
		r.t.Logf("receiver got an error: %v", meta.Err)
		return execinfra.ConsumerClosed
	}
	return execinfra.NeedMoreRows
}

func (r *errorReportingRowReceiver) ProducerDone() {}

// A do nothing bulk adder implementation.
type doNothingKeyAdder struct {
	onKeyAdd func(key roachpb.Key)
	onFlush  func(summary roachpb.BulkOpSummary)
}

var _ kvserverbase.BulkAdder = &doNothingKeyAdder{}

func (a *doNothingKeyAdder) Add(_ context.Context, k roachpb.Key, _ []byte) error {
	if a.onKeyAdd != nil {
		a.onKeyAdd(k)
	}
	return nil
}

func (a *doNothingKeyAdder) Flush(_ context.Context) error {
	if a.onFlush != nil {
		a.onFlush(roachpb.BulkOpSummary{})
	}
	return nil
}

func (*doNothingKeyAdder) IsEmpty() bool                                { return true }
func (*doNothingKeyAdder) CurrentBufferFill() float32                   { return 0 }
func (*doNothingKeyAdder) GetSummary() roachpb.BulkOpSummary            { return roachpb.BulkOpSummary{} }
func (*doNothingKeyAdder) Close(_ context.Context)                      {}
func (a *doNothingKeyAdder) SetOnFlush(f func(_ roachpb.BulkOpSummary)) { a.onFlush = f }

var eofOffset int64 = math.MaxInt64

func TestImportIgnoresProcessedFiles(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings:        &cluster.Settings{},
			ExternalStorage: externalStorageFactory,
			BulkAdder: func(
				_ context.Context, _ *kv.DB, _ hlc.Timestamp,
				_ kvserverbase.BulkAdderOptions) (kvserverbase.BulkAdder, error) {
				return &doNothingKeyAdder{}, nil
			},
		},
	}

	// In this test, we'll specify import files that do not exist, but mark
	// those files fully processed. The converters should not attempt to even
	// open these files (and if they do, we should report a test failure)
	tests := []struct {
		name         string
		spec         testSpec
		inputOffsets []int64 // List of file ids that were fully processed
	}{
		{
			"csv-two-invalid",
			newTestSpec(ctx, t, csvFormat(), "__invalid__", "testdata/csv/data-0", "/_/missing/_"),
			[]int64{eofOffset, 0, eofOffset},
		},
		{
			"csv-all-invalid",
			newTestSpec(ctx, t, csvFormat(), "__invalid__", "../../&"),
			[]int64{eofOffset, eofOffset},
		},
		{
			"csv-all-valid",
			newTestSpec(ctx, t, csvFormat(), "testdata/csv/data-0"),
			[]int64{0},
		},
		{
			"mysql-one-invalid",
			newTestSpec(ctx, t, mysqlDumpFormat(), "testdata/mysqldump/simple.sql", "/_/missing/_"),
			[]int64{0, eofOffset},
		},
		{
			"pgdump-one-input",
			newTestSpec(ctx, t, pgDumpFormat(), "testdata/pgdump/simple.sql"),
			[]int64{0},
		},
		{
			"avro-one-invalid",
			newTestSpec(ctx, t, avroFormat(t, roachpb.AvroOptions_OCF), "__invalid__", "testdata/avro/simple.ocf"),
			[]int64{eofOffset, 0},
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
			post := execinfrapb.PostProcessSpec{}

			processor, err := newReadImportDataProcessor(flowCtx, 0, *spec, &post, &errorReportingRowReceiver{t})
			if err != nil {
				t.Fatalf("Could not create data processor: %v", err)
			}

			processor.Run(ctx)
		})
	}
}

type observedKeys struct {
	syncutil.Mutex
	keys []roachpb.Key
}

func TestImportHonorsResumePosition(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	batchSize := 13
	defer row.TestingSetDatumRowConverterBatchSize(batchSize)()

	pkBulkAdder := &doNothingKeyAdder{}
	ctx := context.Background()

	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings:        &cluster.Settings{},
			ExternalStorage: externalStorageFactory,
			BulkAdder: func(
				_ context.Context, _ *kv.DB, _ hlc.Timestamp,
				opts kvserverbase.BulkAdderOptions) (kvserverbase.BulkAdder, error) {
				if opts.Name == "pkAdder" {
					return pkBulkAdder, nil
				}
				return &doNothingKeyAdder{}, nil
			},
			TestingKnobs: execinfra.TestingKnobs{
				BulkAdderFlushesEveryBatch: true,
			},
		},
	}

	// In this test, we'll specify various resume positions for
	// different input formats. We expect that the rows before resume
	// position will be skipped.
	// NB: We assume that the (external) test files are sorted and
	// contain sufficient number of rows.
	testSpecs := []testSpec{
		newTestSpec(ctx, t, csvFormat(), "testdata/csv/data-0"),
		newTestSpec(ctx, t, mysqlDumpFormat(), "testdata/mysqldump/simple.sql"),
		newTestSpec(ctx, t, mysqlOutFormat(), "testdata/mysqlout/csv-ish/simple.txt"),
		newTestSpec(ctx, t, pgCopyFormat(), "testdata/pgcopy/default/test.txt"),
		newTestSpec(ctx, t, pgDumpFormat(), "testdata/pgdump/simple.sql"),
		newTestSpec(ctx, t, avroFormat(t, roachpb.AvroOptions_JSON_RECORDS), "testdata/avro/simple-sorted.json"),
	}

	resumes := []int64{0, 10, 64, eofOffset}

	for _, testCase := range testSpecs {
		spec := testCase.getConverterSpec()
		keys := &observedKeys{keys: make([]roachpb.Key, 0, 1000)}
		numKeys := 0

		for _, resumePos := range resumes {
			spec.ResumePos = map[int32]int64{0: resumePos}
			if resumePos == 0 {
				// We use 0 resume position to record the set of keys in the input file.
				pkBulkAdder.onKeyAdd = func(k roachpb.Key) {
					keys.Lock()
					keys.keys = append(keys.keys, k)
					keys.Unlock()
				}
			} else {
				if resumePos != eofOffset && resumePos > int64(numKeys) {
					t.Logf("test skipped: resume position %d > number of keys %d", resumePos, numKeys)
					continue
				}

				// For other resume positions, we want to ensure that
				// the key we add is not among [0 - resumePos) keys.
				pkBulkAdder.onKeyAdd = func(k roachpb.Key) {
					maxKeyIdx := int(resumePos)
					if resumePos == eofOffset {
						maxKeyIdx = numKeys
					}
					keys.Lock()
					idx := sort.Search(maxKeyIdx, func(i int) bool { return keys.keys[i].Compare(k) == 0 })
					if idx < maxKeyIdx {
						t.Errorf("failed to skip key[%d]=%s", idx, k)
					}
					keys.Unlock()
				}
			}

			t.Run(fmt.Sprintf("resume-%v-%v", spec.Format.Format, resumePos), func(t *testing.T) {
				rp := resumePos
				progCh := make(chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress)
				defer close(progCh)

				// Setup progress consumer.
				go func() {
					// Consume progress reports. Since we expect every batch to be flushed
					// (BulkAdderFlushesEveryBatch), then the progress resport must be emitted every
					// batchSize rows (possibly out of order), starting from our initial resumePos
					for prog := range progCh {
						if !t.Failed() && prog.ResumePos[0] < (rp+int64(batchSize)) {
							t.Logf("unexpected progress resume pos: %d", prog.ResumePos[0])
							t.Fail()
						}
					}
				}()

				_, err := runImport(ctx, flowCtx, spec, progCh, nil /* seqChunkProvider */)
				if err != nil {
					t.Fatal(err)
				}
			})

			if resumePos == 0 {
				// Even though the input is assumed to be sorted, we may still observe
				// bulk adder keys arriving out of order.  We need to sort the keys.
				keys.Lock()
				sort.Slice(keys.keys, func(i int, j int) bool {
					return keys.keys[i].Compare(keys.keys[j]) < 0
				})
				numKeys = len(keys.keys)
				keys.Unlock()
			}
		}
	}
}

type duplicateKeyErrorAdder struct {
	doNothingKeyAdder
}

var _ kvserverbase.BulkAdder = &duplicateKeyErrorAdder{}

func (a *duplicateKeyErrorAdder) Add(_ context.Context, k roachpb.Key, v []byte) error {
	return &kvserverbase.DuplicateKeyError{Key: k, Value: v}
}

func TestImportHandlesDuplicateKVs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	batchSize := 13
	defer row.TestingSetDatumRowConverterBatchSize(batchSize)()
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings:        &cluster.Settings{},
			ExternalStorage: externalStorageFactory,
			BulkAdder: func(
				_ context.Context, _ *kv.DB, _ hlc.Timestamp,
				opts kvserverbase.BulkAdderOptions) (kvserverbase.BulkAdder, error) {
				return &duplicateKeyErrorAdder{}, nil
			},
			TestingKnobs: execinfra.TestingKnobs{
				BulkAdderFlushesEveryBatch: true,
			},
		},
	}

	// In this test, we'll attempt to import different input formats.
	// All imports produce a DuplicateKeyError, which we expect to be propagated.
	testSpecs := []testSpec{
		newTestSpec(ctx, t, csvFormat(), "testdata/csv/data-0"),
		newTestSpec(ctx, t, mysqlDumpFormat(), "testdata/mysqldump/simple.sql"),
		newTestSpec(ctx, t, mysqlOutFormat(), "testdata/mysqlout/csv-ish/simple.txt"),
		newTestSpec(ctx, t, pgCopyFormat(), "testdata/pgcopy/default/test.txt"),
		newTestSpec(ctx, t, pgDumpFormat(), "testdata/pgdump/simple.sql"),
		newTestSpec(ctx, t, avroFormat(t, roachpb.AvroOptions_JSON_RECORDS), "testdata/avro/simple-sorted.json"),
	}

	for _, testCase := range testSpecs {
		spec := testCase.getConverterSpec()

		t.Run(fmt.Sprintf("duplicate-key-%v", spec.Format.Format), func(t *testing.T) {
			progCh := make(chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress)
			defer close(progCh)
			go func() {
				for range progCh {
				}
			}()

			_, err := runImport(ctx, flowCtx, spec, progCh, nil /* seqChunkProvider */)
			require.True(t, errors.HasType(err, &kvserverbase.DuplicateKeyError{}))
		})
	}
}

// syncBarrier allows 2 threads (a controller and a worker) to
// synchronize between themselves. A controller portion of the
// barrier waits until worker starts running, and then notifies
// worker to proceed. The worker is the opposite: notifies controller
// that it started running, and waits for the proceed signal.
type syncBarrier interface {
	// Enter blocks the barrier, and returns a function
	// that, when executed, unblocks the other thread.
	Enter() func()
}

type barrier struct {
	read       <-chan struct{}
	write      chan<- struct{}
	controller bool
}

// Returns controller/worker barriers.
func newSyncBarrier() (syncBarrier, syncBarrier) {
	p1 := make(chan struct{})
	p2 := make(chan struct{})
	return &barrier{p1, p2, true}, &barrier{p2, p1, false}
}

func (b *barrier) Enter() func() {
	if b.controller {
		b.write <- struct{}{}
		return func() { <-b.read }
	}

	<-b.read
	return func() { b.write <- struct{}{} }
}

// A special jobs.Resumer that, instead of finishing
// the job successfully, forces the job to be paused.
var _ jobs.Resumer = &cancellableImportResumer{}

type cancellableImportResumer struct {
	ctx              context.Context
	jobIDCh          chan jobspb.JobID
	jobID            jobspb.JobID
	onSuccessBarrier syncBarrier
	wrapped          *importResumer
}

func (r *cancellableImportResumer) Resume(ctx context.Context, execCtx interface{}) error {
	r.jobID = r.wrapped.job.ID()
	r.jobIDCh <- r.jobID
	if err := r.wrapped.Resume(r.ctx, execCtx); err != nil {
		return err
	}
	if r.onSuccessBarrier != nil {
		defer r.onSuccessBarrier.Enter()()
	}
	return errors.New("job succeed, but we're forcing it to be paused")
}

func (r *cancellableImportResumer) OnFailOrCancel(ctx context.Context, execCtx interface{}) error {
	// This callback is invoked when an error or cancellation occurs
	// during the import. Since our Resume handler returned an
	// error (after pausing the job), we need to short-circuits
	// jobs machinery so that this job is not marked as failed.
	return errors.New("bail out")
}

func setImportReaderParallelism(parallelism int32) func() {
	factory := rowexec.NewReadImportDataProcessor
	rowexec.NewReadImportDataProcessor = func(
		flowCtx *execinfra.FlowCtx, processorID int32,
		spec execinfrapb.ReadImportDataSpec, post *execinfrapb.PostProcessSpec,
		output execinfra.RowReceiver) (execinfra.Processor, error) {
		spec.ReaderParallelism = parallelism
		return factory(flowCtx, processorID, spec, post, output)
	}

	return func() {
		rowexec.NewReadImportDataProcessor = factory
	}
}

// Queries the status and the import progress of the job.
type jobState struct {
	err    error
	status jobs.Status
	prog   jobspb.ImportProgress
}

func queryJob(db sqlutils.DBHandle, jobID jobspb.JobID) (js jobState) {
	js = jobState{
		err:    nil,
		status: "",
		prog:   jobspb.ImportProgress{},
	}
	var progressBytes, payloadBytes []byte
	js.err = db.QueryRowContext(
		context.Background(), "SELECT status, payload, progress FROM system.jobs WHERE id = $1", jobID).Scan(
		&js.status, &payloadBytes, &progressBytes)
	if js.err != nil {
		return
	}

	if js.status == jobs.StatusFailed {
		payload := &jobspb.Payload{}
		js.err = protoutil.Unmarshal(payloadBytes, payload)
		if js.err == nil {
			js.err = errors.Newf("%s", payload.Error)
		}
		return
	}

	progress := &jobspb.Progress{}
	if js.err = protoutil.Unmarshal(progressBytes, progress); js.err != nil {
		return
	}
	js.prog = *(progress.Details.(*jobspb.Progress_Import).Import)
	return
}

// Repeatedly queries job status/progress until specified function returns true.
func queryJobUntil(
	t *testing.T, db sqlutils.DBHandle, jobID jobspb.JobID, isDone func(js jobState) bool,
) (js jobState) {
	t.Helper()
	for r := retry.Start(base.DefaultRetryOptions()); r.Next(); {
		js = queryJob(db, jobID)
		if js.err != nil || isDone(js) {
			break
		}
	}
	if js.err != nil {
		t.Fatal(js.err)
	}
	return
}

func TestCSVImportCanBeResumed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer setImportReaderParallelism(1)()
	const batchSize = 5
	defer TestingSetParallelImporterReaderBatchSize(batchSize)()
	defer row.TestingSetDatumRowConverterBatchSize(2 * batchSize)()

	s, db, _ := serverutils.StartServer(t,
		base.TestServerArgs{
			Knobs: base.TestingKnobs{
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
				DistSQL: &execinfra.TestingKnobs{
					BulkAdderFlushesEveryBatch: true,
				},
			},
		})
	registry := s.JobRegistry().(*jobs.Registry)
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, "CREATE TABLE t (id INT, data STRING)")
	defer sqlDB.Exec(t, `DROP TABLE t`)

	jobCtx, cancelImport := context.WithCancel(ctx)
	jobIDCh := make(chan jobspb.JobID)
	var jobID jobspb.JobID = -1
	var importSummary roachpb.RowCount

	registry.TestingResumerCreationKnobs = map[jobspb.Type]func(raw jobs.Resumer) jobs.Resumer{
		// Arrange for our special job resumer to be
		// returned the very first time we start the import.
		jobspb.TypeImport: func(raw jobs.Resumer) jobs.Resumer {
			resumer := raw.(*importResumer)
			resumer.testingKnobs.alwaysFlushJobProgress = true
			resumer.testingKnobs.afterImport = func(summary roachpb.RowCount) error {
				importSummary = summary
				return nil
			}
			if jobID == -1 {
				return &cancellableImportResumer{
					ctx:     jobCtx,
					jobIDCh: jobIDCh,
					wrapped: resumer,
				}
			}
			return resumer
		},
	}

	testBarrier, csvBarrier := newSyncBarrier()
	csv1 := newCsvGenerator(0, 10*batchSize+1, &intGenerator{}, &strGenerator{})
	csv1.addBreakpoint(7*batchSize, func() (bool, error) {
		defer csvBarrier.Enter()()
		return false, nil
	})

	// Convince distsql to use our "external" storage implementation.
	storage := newGeneratedStorage(csv1)
	s.DistSQLServer().(*distsql.ServerImpl).ServerConfig.ExternalStorage = storage.externalStorageFactory()

	// Execute import; ignore any errors returned
	// (since we're aborting the first import run.).
	go func() {
		_, _ = sqlDB.DB.ExecContext(ctx,
			`IMPORT INTO t (id, data) CSV DATA ($1)`, storage.getGeneratorURIs()[0])
	}()

	// Wait for the job to start running
	jobID = <-jobIDCh

	// Wait until we are blocked handling breakpoint.
	unblockImport := testBarrier.Enter()
	// Wait until we have recorded some job progress.
	js := queryJobUntil(t, sqlDB.DB, jobID, func(js jobState) bool { return js.prog.ResumePos[0] > 0 })

	// Pause the job;
	if err := registry.PauseRequested(ctx, nil, jobID, ""); err != nil {
		t.Fatal(err)
	}
	// Send cancellation and unblock breakpoint.
	cancelImport()
	unblockImport()

	// Get updated resume position counter.
	js = queryJobUntil(t, sqlDB.DB, jobID, func(js jobState) bool { return jobs.StatusPaused == js.status })
	resumePos := js.prog.ResumePos[0]
	t.Logf("Resume pos: %v\n", js.prog.ResumePos[0])

	// Unpause the job and wait for it to complete.
	if err := registry.Unpause(ctx, nil, jobID); err != nil {
		t.Fatal(err)
	}
	js = queryJobUntil(t, sqlDB.DB, jobID, func(js jobState) bool { return jobs.StatusSucceeded == js.status })

	// Verify that the import proceeded from the resumeRow position.
	assert.Equal(t, importSummary.Rows, int64(csv1.numRows)-resumePos)

	sqlDB.CheckQueryResults(t, `SELECT id FROM t ORDER BY id`,
		sqlDB.QueryStr(t, `SELECT generate_series(0, $1)`, csv1.numRows-1),
	)
}

func TestCSVImportMarksFilesFullyProcessed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	const batchSize = 5
	defer TestingSetParallelImporterReaderBatchSize(batchSize)()
	defer row.TestingSetDatumRowConverterBatchSize(2 * batchSize)()

	s, db, _ := serverutils.StartServer(t,
		base.TestServerArgs{
			Knobs: base.TestingKnobs{
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
				DistSQL: &execinfra.TestingKnobs{
					BulkAdderFlushesEveryBatch: true,
				},
			},
		})
	registry := s.JobRegistry().(*jobs.Registry)
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, "CREATE TABLE t (id INT, data STRING)")
	defer sqlDB.Exec(t, `DROP TABLE t`)

	jobIDCh := make(chan jobspb.JobID)
	controllerBarrier, importBarrier := newSyncBarrier()

	var jobID jobspb.JobID = -1
	var importSummary roachpb.RowCount

	registry.TestingResumerCreationKnobs = map[jobspb.Type]func(raw jobs.Resumer) jobs.Resumer{
		// Arrange for our special job resumer to be
		// returned the very first time we start the import.
		jobspb.TypeImport: func(raw jobs.Resumer) jobs.Resumer {
			resumer := raw.(*importResumer)
			resumer.testingKnobs.alwaysFlushJobProgress = true
			resumer.testingKnobs.afterImport = func(summary roachpb.RowCount) error {
				importSummary = summary
				return nil
			}
			if jobID == -1 {
				return &cancellableImportResumer{
					ctx:              ctx,
					jobIDCh:          jobIDCh,
					onSuccessBarrier: importBarrier,
					wrapped:          resumer,
				}
			}
			return resumer
		},
	}

	csv1 := newCsvGenerator(0, 10*batchSize+1, &intGenerator{}, &strGenerator{})
	csv2 := newCsvGenerator(0, 20*batchSize-1, &intGenerator{}, &strGenerator{})
	csv3 := newCsvGenerator(0, 1, &intGenerator{}, &strGenerator{})

	// Convince distsql to use our "external" storage implementation.
	storage := newGeneratedStorage(csv1, csv2, csv3)
	s.DistSQLServer().(*distsql.ServerImpl).ServerConfig.ExternalStorage = storage.externalStorageFactory()

	// Execute import; ignore any errors returned
	// (since we're aborting the first import run).
	go func() {
		_, _ = sqlDB.DB.ExecContext(ctx,
			`IMPORT INTO t (id, data) CSV DATA ($1, $2, $3)`, storage.getGeneratorURIs()...)
	}()

	// Wait for the job to start running
	jobID = <-jobIDCh

	// Tell importer that it can continue with it's onSuccess
	proceedImport := controllerBarrier.Enter()

	// Pause the job;
	if err := registry.PauseRequested(ctx, nil, jobID, ""); err != nil {
		t.Fatal(err)
	}

	// All files should have been processed,
	// and the resume position set to maxInt64.
	js := queryJobUntil(t, sqlDB.DB, jobID, func(js jobState) bool { return jobs.StatusPaused == js.status })
	for _, pos := range js.prog.ResumePos {
		assert.True(t, pos == math.MaxInt64)
	}

	// Send cancellation and unblock import.
	proceedImport()

	// Unpause the job and wait for it to complete.
	if err := registry.Unpause(ctx, nil, jobID); err != nil {
		t.Fatal(err)
	}
	js = queryJobUntil(t, sqlDB.DB, jobID, func(js jobState) bool { return jobs.StatusSucceeded == js.status })

	// Verify that after resume we have not processed any additional rows.
	assert.Zero(t, importSummary.Rows)
}

func TestImportWithPartialIndexesErrs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t,
		base.TestServerArgs{
			Knobs: base.TestingKnobs{
				DistSQL: &execinfra.TestingKnobs{
					BulkAdderFlushesEveryBatch: true,
				},
			},
		})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, "CREATE TABLE t (id INT, data STRING, INDEX (data) WHERE id > 0)")
	defer sqlDB.Exec(t, `DROP TABLE t`)

	sqlDB.ExpectErr(t, "cannot import into table with partial indexes", `IMPORT INTO t (id, data) CSV DATA ('https://foo.bar')`)
}

func (ses *generatedStorage) externalStorageFactory() cloud.ExternalStorageFactory {
	return func(_ context.Context, es roachpb.ExternalStorage) (cloud.ExternalStorage, error) {
		uri, err := url.Parse(es.HttpPath.BaseUri)
		if err != nil {
			return nil, err
		}
		id, ok := ses.nameIDMap[uri.Path]
		if !ok {
			id = ses.nextID
			ses.nextID++
			ses.nameIDMap[uri.Path] = id
		}
		return &generatorExternalStorage{conf: es, gen: ses.generators[id]}, nil
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
	return cloud.MakeExternalStorage(ctx, dest, base.ExternalIODirConfig{},
		nil, blobs.TestBlobServiceClient(workdir), nil, nil)
}

// Helper to create and initialize testSpec.
func newTestSpec(
	ctx context.Context, t *testing.T, format roachpb.IOFileFormat, inputs ...string,
) testSpec {
	spec := testSpec{
		format: format,
		inputs: make(map[int32]string),
	}

	// Initialize table descriptor for import. We need valid descriptor to run
	// converters, even though we don't actually import anything in this test.
	var descr *tabledesc.Mutable
	switch format.Format {
	case roachpb.IOFileFormat_CSV:
		descr = descForTable(ctx, t,
			"CREATE TABLE simple (i INT PRIMARY KEY, s text )", 100, 150, 200, NoFKs)
	case
		roachpb.IOFileFormat_Mysqldump,
		roachpb.IOFileFormat_MysqlOutfile,
		roachpb.IOFileFormat_PgDump,
		roachpb.IOFileFormat_PgCopy,
		roachpb.IOFileFormat_Avro:
		descr = descForTable(ctx, t,
			"CREATE TABLE simple (i INT PRIMARY KEY, s text, b bytea default null)", 100, 150, 200, NoFKs)
	default:
		t.Fatalf("Unsupported input format: %v", format)
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

	fullTableName := "simple"
	if format.Format == roachpb.IOFileFormat_PgDump {
		fullTableName = "public.simple"
	}
	spec.tables = map[string]*execinfrapb.ReadImportDataSpec_ImportTable{
		fullTableName: {Desc: descr.TableDesc(), TargetCols: targetCols[0:numCols]},
	}

	for id, path := range inputs {
		spec.inputs[int32(id)] = nodelocal.MakeLocalStorageURI(path)
	}

	return spec
}

func pgDumpFormat() roachpb.IOFileFormat {
	return roachpb.IOFileFormat{
		Format: roachpb.IOFileFormat_PgDump,
		PgDump: roachpb.PgDumpOptions{
			MaxRowSize:        64 * 1024,
			IgnoreUnsupported: true,
		},
	}
}

func pgCopyFormat() roachpb.IOFileFormat {
	return roachpb.IOFileFormat{
		Format: roachpb.IOFileFormat_PgCopy,
		PgCopy: roachpb.PgCopyOptions{
			Delimiter:  '\t',
			Null:       `\N`,
			MaxRowSize: 4096,
		},
	}
}

func mysqlDumpFormat() roachpb.IOFileFormat {
	return roachpb.IOFileFormat{
		Format: roachpb.IOFileFormat_Mysqldump,
	}
}

func mysqlOutFormat() roachpb.IOFileFormat {
	return roachpb.IOFileFormat{
		Format: roachpb.IOFileFormat_MysqlOutfile,
		MysqlOut: roachpb.MySQLOutfileOptions{
			FieldSeparator: ',',
			RowSeparator:   '\n',
			HasEscape:      true,
			Escape:         '\\',
			Enclose:        roachpb.MySQLOutfileOptions_Always,
			Encloser:       '"',
		},
	}
}

func csvFormat() roachpb.IOFileFormat {
	return roachpb.IOFileFormat{
		Format: roachpb.IOFileFormat_CSV,
	}
}

func avroFormat(t *testing.T, format roachpb.AvroOptions_Format) roachpb.IOFileFormat {
	avro := roachpb.AvroOptions{
		Format:     format,
		StrictMode: false,
	}

	if format != roachpb.AvroOptions_OCF {
		// Need to load schema for record specific inputs.
		bytes, err := ioutil.ReadFile(testutils.TestDataPath(t, "avro", "simple-schema.json"))
		require.NoError(t, err)
		avro.SchemaJSON = string(bytes)
		avro.RecordSeparator = '\n'
	}

	return roachpb.IOFileFormat{
		Format: roachpb.IOFileFormat_Avro,
		Avro:   avro,
	}
}
