// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ttljob

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
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
	// onRun, if set, is invoked with the transaction each batch runs in.
	onRun func(txn isql.Txn)
}

// mockDeleteBuilder is an implementation of the DeleteBuilder interface.
func (m *mockDeleteBuilder) Run(
	_ context.Context, txn isql.Txn, rows []tree.Datums,
) (int64, error) {
	if m.onRun != nil {
		m.onRun(txn)
	}
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
func (m *mockSelectBuilder) BuildQuery() string {
	return ""
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
			rowCount, err := mockTTLProc.runTTLOnQueryBounds(ctx, metrics, sb, db, unlimitedRateLimiter())
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

func unlimitedRateLimiter() *quotapool.RateLimiter {
	return quotapool.NewRateLimiter("", quotapool.Inf(), math.MaxInt64)
}

// TestDeleteRateLimiterAcquiredBeforeTxn verifies that a delete batch waits
// for rate limiter tokens before its transaction is opened. If the wait
// happened inside the transaction, the transaction would be as old as the wait
// by the time it issued its first write, and a lease transfer or timestamp
// cache rotation on the anchor range during the wait would abort it.
func TestDeleteRateLimiterAcquiredBeforeTxn(t *testing.T) {
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

	runner := sqlutils.MakeSQLRunner(sqlDB)
	runner.Exec(t, "CREATE TABLE defaultdb.public.ttl_tester ()")
	var tableDesc catalog.TableDescriptor
	require.NoError(t, sql.DescsTxn(ctx, &execCfg, func(
		ctx context.Context, txn isql.Txn, descriptors *descs.Collection,
	) error {
		db, err := descriptors.ByName(txn.KV()).Get().Database(ctx, "defaultdb")
		if err != nil {
			return err
		}
		schema, err := descriptors.ByName(txn.KV()).Get().Schema(ctx, db, "public")
		if err != nil {
			return err
		}
		tableDesc, err = descriptors.ByName(txn.KV()).Get().Table(ctx, db, schema, "ttl_tester")
		return err
	}))

	// Three batches of five rows against a limiter that refills two rows per
	// second with a burst of five: the first batch proceeds immediately and each
	// of the next two must wait about 2.5 seconds for tokens.
	const batchSize = 5
	const numBatches = 3
	sb := &mockSelectBuilder{}
	for i := 0; i < batchSize*numBatches; i++ {
		sb.data = append(sb.data, tree.Datums{tree.NewDInt(tree.DInt(i))})
	}
	limiter := quotapool.NewRateLimiter("", quotapool.Limit(2), batchSize)

	// Record how old each batch's transaction is when the DELETE runs. The read
	// timestamp is assigned when the transaction is created, so this measures
	// the time between opening the transaction and reaching the DELETE.
	var txnAges []time.Duration
	db := &mockDeleteBuilder{
		batchSize: batchSize,
		onRun: func(txn isql.Txn) {
			txnAges = append(txnAges, timeutil.Since(txn.KV().ReadTimestamp().GoTime()))
		},
	}

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

	start := timeutil.Now()
	rowCount, err := mockTTLProc.runTTLOnQueryBounds(ctx, metrics, sb, db, limiter)
	require.NoError(t, err)
	require.Equal(t, batchSize*numBatches, int(rowCount))
	require.Len(t, txnAges, numBatches)

	// The rate limiter must actually have throttled the run; otherwise the
	// assertion below would pass trivially.
	require.Greater(t, timeutil.Since(start), 4*time.Second)
	for i, age := range txnAges {
		require.Less(t, age, time.Second, "batch %d transaction aged %s before its DELETE", i, age)
	}
}
