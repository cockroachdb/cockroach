// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ttljob

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/require"
)

// NOTE: This test is for functions in ttljob_processor.go. We already have
// ttljob_processor_test.go, but that is part of the ttljob_test package.
// This test is specifically part of the ttljob package to access non-exported
// functions and structs. Hence, the name '_internal_' in the file to signify
// that it access internal functions.

// mockDeleteBuilder is a DeleteBuilder implementation that allows gives you
// control over what errors are returned for each batch attempt.
type mockDeleteBuilder struct {
	batchSize      int
	callCountIndex int
	errorsPerCall  []error
}

// mockDeleteBuilder is an implementation of the DeleteBuilder interface.
func (m *mockDeleteBuilder) Run(_ context.Context, _ isql.Txn, rows []tree.Datums) (int64, error) {
	i := m.callCountIndex
	m.callCountIndex++
	if i < len(m.errorsPerCall) {
		if m.errorsPerCall[i] != nil {
			return 0, m.errorsPerCall[i]
		}
	}
	return int64(len(rows)), nil
}

// BuildQuery is an implementation of the DeleteBuilder interface.
func (m *mockDeleteBuilder) BuildQuery(numRows int) string {
	return ""
}

// GetBatchSize is an implementation of the DeleteBuilder interface.
func (m *mockDeleteBuilder) GetBatchSize() int {
	return m.batchSize
}

// mockSelectBuilder is a mock implementation of the SelectBuilder interface.
// It allows you to control the data that is returned instead of retrieving data
// from a SELECT query on a table.
type mockSelectBuilder struct {
	data []tree.Datums
}

// Run is an implementation of the SelectBuilder interface.
func (m *mockSelectBuilder) Run(
	ctx context.Context, ie isql.Executor,
) (_ []tree.Datums, hasNext bool, _ error) {
	return m.data, false, nil
}

// BuildQuery is an implementation of the SelectBuilder interface.
func (m *mockSelectBuilder) BuildQuery() (string, error) {
	return "", nil
}

func TestRetryDeleteBatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	s := srv.ApplicationLayer()

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	flowCtx := execinfra.FlowCtx{
		Cfg: &execinfra.ServerConfig{
			DB:       s.InternalDB().(descs.DB),
			Settings: s.ClusterSettings(),
			Codec:    s.Codec(),
		},
		EvalCtx: &eval.Context{
			Codec:    s.Codec(),
			Settings: s.ClusterSettings(),
		},
	}

	// We need to create a dummy table so that we have a table descriptor. The
	// table descriptor is used to populate a valid ID and version for the TTL
	// processor.
	runner := sqlutils.MakeSQLRunner(sqlDB)
	runner.Exec(t, "CREATE DATABASE db")
	runner.Exec(t, "CREATE SCHEMA db.sc")
	runner.Exec(t, "CREATE TABLE db.sc.ttl_tester ()")
	var tableDesc catalog.TableDescriptor
	require.NoError(t, sql.DescsTxn(ctx, &execCfg, func(
		ctx context.Context, txn isql.Txn, descriptors *descs.Collection,
	) error {
		db, err := descriptors.ByName(txn.KV()).Get().Database(ctx, "db")
		if err != nil {
			return err
		}
		schema, err := descriptors.ByName(txn.KV()).Get().Schema(ctx, db, "sc")
		if err != nil {
			return err
		}
		tableDesc, err = descriptors.ByName(txn.KV()).Get().Table(ctx, db, schema, "ttl_tester")
		return err
	}))

	// We will use the same SelectBuilder for each test case. It will mock the
	// data returned to delete.
	sb := &mockSelectBuilder{}
	const selectSize = 250
	for i := 0; i < selectSize; i++ {
		sb.data = append(sb.data, tree.Datums{tree.NewDInt(tree.DInt(i))})
	}
	const batchSize = 7

	// The two kinds of errors we will see in the test
	nonRetryableErr := errors.New("not retried")
	retryableErr := kv.ErrAutoRetryLimitExhausted

	testCases := []struct {
		// desc is the description of the test case
		desc string
		// batchErrs is the list of errors that occur during the delete operation.
		// It is an ordered list of errors. The size of this slice does not have to
		// match the number of batch attempts. If an attempt is made that is larger
		// than the slice, we assume the attempt completes successfully.
		batchErrs []error
		// expectedResult is the expected error returned by the function. If nil,
		// then we assume the entire batch was completed.
		expectedResult error
		// expectedRetryCount is the expected number of times that we will retry
		// the delete operation with a smaller batch size.
		expectedRetryCount int64
	}{
		{desc: "no errors", batchErrs: nil, expectedResult: nil, expectedRetryCount: 0},
		{desc: "non retryable error", batchErrs: []error{nonRetryableErr}, expectedResult: nonRetryableErr, expectedRetryCount: 0},
		{desc: "one retryable error", batchErrs: []error{retryableErr}, expectedResult: nil, expectedRetryCount: 1},
		{desc: "two retryable error", batchErrs: []error{retryableErr, retryableErr},
			expectedResult: nil, expectedRetryCount: 2},
		{desc: "three retryable error", batchErrs: []error{retryableErr, retryableErr, retryableErr},
			expectedResult: kv.ErrAutoRetryLimitExhausted, expectedRetryCount: 2},
		{desc: "one retryable and one terminal error", batchErrs: []error{retryableErr, nonRetryableErr},
			expectedResult: nonRetryableErr, expectedRetryCount: 1},
		{desc: "intermittent retries",
			batchErrs: []error{
				nil,          // first batch of 7 rows is fine
				retryableErr, // error in the first attempt of this batch
				nil,          // retry the batch with 3 rows, which is fine
				nil,          // continue with batch size of 3 and succeed
				nil,          // final 1 row to complete the batch of 7 rows
				retryableErr, // error in the first of this new batch
				nil,          // retry using 3 rows is fine
				retryableErr, // error in the next 3 rows fails
				nil,          // retry using 1 row is fine
				nil,          // next batch is 1 row
				nil,          // next batch is 1 row
				nil,          // next batch is 1 row, completes the batch of 7
				retryableErr, // error in the fourth batch. All remaining attempts are successful.
			},
			expectedResult: nil, expectedRetryCount: 4},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			m := makeRowLevelTTLAggMetrics(time.Second)
			metrics := m.(*RowLevelTTLAggMetrics).loadMetrics(false /* labelMetrics */, "test" /* relationName */)

			mockTTLProc := ttlProcessor{
				ttlSpec: execinfrapb.TTLSpec{
					RowLevelTTLDetails: jobspb.RowLevelTTLDetails{
						TableID:      tableDesc.GetID(),
						TableVersion: tableDesc.GetVersion(),
					},
				},
				ProcessorBase: execinfra.ProcessorBase{
					ProcessorBaseNoHelper: execinfra.ProcessorBaseNoHelper{
						FlowCtx: &flowCtx,
					},
				},
			}
			db := &mockDeleteBuilder{
				batchSize:     batchSize,
				errorsPerCall: tc.batchErrs,
			}
			rowCount, err := mockTTLProc.runTTLOnQueryBounds(ctx, metrics, sb, db)
			if tc.expectedResult != nil {
				require.Error(t, err)
				require.True(t, errors.Is(err, tc.expectedResult))
				require.Equal(t, 0, int(rowCount))
				require.Equal(t, int64(0), metrics.RowDeletions.Value())
			} else {
				require.NoError(t, err)
				require.Equal(t, selectSize, int(rowCount))
				require.Equal(t, int64(selectSize), metrics.RowDeletions.Value())
			}
			// Check the metric to ensure we actually did the expected number of retries.
			require.Equal(t, tc.expectedRetryCount, metrics.NumDeleteBatchRetries.Value())
		})
	}
}

// metadataCache is a RowReceiver that caches any metadata it receives for later
// inspection.
type metadataCache struct {
	bufferedMeta []execinfrapb.ProducerMetadata
	pushResult   execinfra.ConsumerStatus
}

var _ execinfra.RowReceiver = &metadataCache{}

// Push is part of the execinfra.RowReceiver interface.
func (m *metadataCache) Push(
	row rowenc.EncDatumRow, meta *execinfrapb.ProducerMetadata,
) execinfra.ConsumerStatus {
	if meta != nil {
		m.bufferedMeta = append(m.bufferedMeta, *meta)
	}
	return m.pushResult
}

// ProducerDone is part of the execinfra.RowReceiver interface.
func (m *metadataCache) ProducerDone() {}

func (m *metadataCache) GetLatest() *execinfrapb.ProducerMetadata {
	if len(m.bufferedMeta) == 0 {
		return nil
	}
	return &m.bufferedMeta[len(m.bufferedMeta)-1]
}

func mockProcessor(processorID int32, nodeID roachpb.NodeID, totalSpanCount int64) *ttlProcessor {
	var c base.NodeIDContainer
	if nodeID != 0 {
		c.Set(context.Background(), nodeID)
	}
	flowCtx := &execinfra.FlowCtx{
		Cfg:    &execinfra.ServerConfig{},
		NodeID: base.NewSQLIDContainerForNode(&c),
		ID:     execinfrapb.FlowID{UUID: uuid.MakeV4()},
	}
	processor := &ttlProcessor{}
	processor.progressUpdater = &coordinatorStreamUpdater{proc: processor}
	processor.progressUpdater.InitProgress(totalSpanCount)
	processor.ProcessorBase = execinfra.ProcessorBase{
		ProcessorBaseNoHelper: execinfra.ProcessorBaseNoHelper{
			ProcessorID: processorID,
			FlowCtx:     flowCtx,
		},
	}
	return processor
}

func TestSendProgressMeta(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		// desc is the description of the test case
		desc             string
		outputResult     execinfra.ConsumerStatus
		expectedErrRegEx string
		nodeID           roachpb.NodeID
		deletedRowCount  int64
		spansCompleted   int
		totalSpanCount   int64
	}{
		{desc: "output fails", outputResult: execinfra.ConsumerClosed, expectedErrRegEx: "ConsumerClosed"},
		{desc: "output succeeds", outputResult: execinfra.NeedMoreRows, nodeID: 1, deletedRowCount: 18, spansCompleted: 1, totalSpanCount: 5},
		{desc: "last push", outputResult: execinfra.NeedMoreRows, nodeID: 1, deletedRowCount: 50, spansCompleted: 5, totalSpanCount: 5},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			processor := mockProcessor(42, tc.nodeID, tc.totalSpanCount)
			mockRowReceiver := metadataCache{pushResult: tc.outputResult}
			spans := makeFakeSpans(tc.spansCompleted)
			for i, span := range spans {
				deletedRowCount := int64(0)
				if i == 0 {
					deletedRowCount = tc.deletedRowCount
				}
				processor.progressUpdater.OnSpanProcessed(span, deletedRowCount)
			}
			err := processor.progressUpdater.UpdateProgress(context.Background(), &mockRowReceiver)

			if tc.expectedErrRegEx != "" {
				require.Regexp(t, tc.expectedErrRegEx, err.Error())
				return
			}
			require.NoError(t, err)
			require.Len(t, mockRowReceiver.bufferedMeta, 1)
			md := mockRowReceiver.bufferedMeta[0]
			require.NotNil(t, md.BulkProcessorProgress)
			require.Equal(t, processor.FlowCtx.NodeID.SQLInstanceID(), md.BulkProcessorProgress.NodeID)
			var ttlProgress jobspb.RowLevelTTLProcessorProgress
			require.NoError(t, pbtypes.UnmarshalAny(&md.BulkProcessorProgress.ProgressDetails, &ttlProgress))
			require.Equal(t, tc.totalSpanCount, ttlProgress.TotalSpanCount)
			require.Equal(t, int64(tc.spansCompleted), ttlProgress.ProcessedSpanCount)
			require.Equal(t, tc.deletedRowCount, ttlProgress.DeletedRowCount)
		})
	}
}

// asyncSerializingReceiver is an execinfra.RowReceiver that simulates the
// behavior of gRPC where serialization happens asynchronously after Push
// returns.
type asyncSerializingReceiver struct {
	pushResult execinfra.ConsumerStatus
	// serializeWg is used to wait for all async serialization goroutines.
	serializeWg sync.WaitGroup
	// writerReady is closed when the writer goroutine is ready to start writing.
	writerReady chan struct{}
	// readerStarted is closed when the reader goroutine has started reading.
	readerStarted chan struct{}
}

var _ execinfra.RowReceiver = &asyncSerializingReceiver{}

// Push is part of the execinfra.RowReceiver interface. It spawns a goroutine
// to serialize the metadata asynchronously, simulating gRPC behavior.
func (r *asyncSerializingReceiver) Push(
	_ rowenc.EncDatumRow, meta *execinfrapb.ProducerMetadata,
) execinfra.ConsumerStatus {
	if meta != nil && meta.BulkProcessorProgress != nil {
		// Capture the pointer to CompletedSpans - this is what gRPC would do.
		completedSpans := meta.BulkProcessorProgress.CompletedSpans
		r.serializeWg.Go(func() {
			// Wait for writer to be ready, then signal we're about to read.
			<-r.writerReady
			close(r.readerStarted)
			// Simulate serialization by reading the spans.
			for i := 0; i < len(completedSpans); i++ {
				_ = completedSpans[i].Key
				_ = completedSpans[i].EndKey
			}
		})
	}
	return r.pushResult
}

// ProducerDone is part of the execinfra.RowReceiver interface.
func (r *asyncSerializingReceiver) ProducerDone() {}

// TestProgressUpdateDataRace is a regression test for a data race during
// progress updates between serializing an old update and populating a new one.
func TestProgressUpdateDataRace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const totalSpans = 100

	processor := mockProcessor(1 /* processorID */, 1 /* nodeID */, totalSpans)
	receiver := &asyncSerializingReceiver{
		pushResult:    execinfra.NeedMoreRows,
		writerReady:   make(chan struct{}),
		readerStarted: make(chan struct{}),
	}

	ctx := context.Background()

	// Add some spans so the slice has capacity.
	spans := makeFakeSpans(5)
	for _, span := range spans {
		processor.progressUpdater.OnSpanProcessed(span, 0 /* deletedRowCount */)
	}

	// Call UpdateProgress - this creates metadata with a reference to the
	// slice, then resets the slice with [:0]. After this call returns,
	// the metadata still holds a reference to the (now reset) slice.
	err := processor.progressUpdater.UpdateProgress(ctx, receiver)
	require.NoError(t, err)

	// Signal to the reader that we're ready to write, then wait for reader to
	// start.
	close(receiver.writerReady)
	<-receiver.readerStarted

	// Now write new spans concurrently with the reader. Since the slice was
	// reset with [:0], this append reuses the same underlying array,
	// overwriting the data that the reader goroutine is reading.
	moreSpans := makeFakeSpans(5)
	for _, span := range moreSpans {
		processor.progressUpdater.OnSpanProcessed(span, 0)
	}

	// Wait for serialization to complete.
	receiver.serializeWg.Wait()
}
