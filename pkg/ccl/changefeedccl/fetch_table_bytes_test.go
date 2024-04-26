// Copyright 2024 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/mocks"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFetchChangefeedUsageBytesBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	fx := newUsageFx(ctx, t)
	defer fx.close()

	fx.query("CREATE TABLE testdb.test as SELECT generate_series(1, 1000) AS id")
	row := fx.query(`CREATE CHANGEFEED FOR TABLE testdb.test INTO 'null://' WITH initial_scan='no';`)
	feedJobId := int64(tree.MustBeDInt(row[0]))

	payload, err := fx.getChangefeedPayload(ctx, catpb.JobID(feedJobId))
	require.NoError(t, err)
	res, err := changefeedccl.FetchChangefeedUsageBytes(ctx, &fx.execCfg, payload)
	require.NoError(t, err)
	assert.NotZero(t, res)
}

func TestFetchChangefeedUsageBytes(t *testing.T) {
	t.Cleanup(leaktest.AfterTest(t))
	t.Cleanup(func() { log.Scope(t).Close(t) })

	type testState struct {
		*UsageFx
		tss       *mocks.MockTenantStatusServer
		feedJobId catpb.JobID
	}

	ctx := context.Background()
	setup := func(t *testing.T, alters []string, spanSizes map[string][]int64, createFeed string) *testState {
		usageFx := newUsageFx(ctx, t)
		t.Cleanup(usageFx.close)

		usageFx.query("CREATE TABLE test as SELECT generate_series(1, 1000) AS id")
		usageFx.query("CREATE TABLE test2 (id int primary key)")

		stmt := `CREATE CHANGEFEED FOR TABLE test, test2 INTO 'null://' WITH initial_scan='no';`
		if createFeed != "" {
			stmt = createFeed
		}
		feedJobId := int64(tree.MustBeDInt(usageFx.query(stmt)[0]))

		for _, alter := range alters {
			usageFx.query(alter)
		}

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		// Populate response.
		resp := &roachpb.SpanStatsResponse{SpanToStats: map[string]*roachpb.SpanStats{}}
		for tableName, spSz := range spanSizes {
			tableSpans := usageFx.getTableSpans(ctx, tableName)
			for i, sp := range tableSpans { // assumption: one span per table/index. should be true for the small tables we're using
				resp.SpanToStats[sp.String()] = &roachpb.SpanStats{ApproximateTotalStats: enginepb.MVCCStats{LiveBytes: spSz[i]}}
			}
		}

		var reqMatcher fnMatcher = func(arg any) bool {
			req := arg.(*roachpb.SpanStatsRequest)
			require.Equal(t, "0", req.NodeID)
			require.Len(t, req.Spans, len(resp.SpanToStats))
			return true
		}

		tss := mocks.NewMockTenantStatusServer(ctrl)
		tss.EXPECT().
			SpanStats(gomock.Any(), reqMatcher).
			Return(resp, nil).Times(1)
		usageFx.execCfg.TenantStatusServer = tss

		return &testState{
			UsageFx:   usageFx,
			tss:       tss,
			feedJobId: catpb.JobID(feedJobId),
		}
	}

	cases := []struct {
		name       string
		createFeed string
		alters     []string
		spanSizes  map[string][]int64
		size       int64
	}{
		{
			name:      "basic",
			spanSizes: map[string][]int64{"test": {1000}, "test2": {100}},
			size:      1100,
		},

		{
			name:      "with another index",
			alters:    []string{"CREATE INDEX ON test (id)"},
			spanSizes: map[string][]int64{"test": {1000, 1000}, "test2": {100}},
			size:      2100,
		},

		{
			name:       "with queries",
			createFeed: `CREATE CHANGEFEED INTO 'null://' WITH initial_scan='no' AS SELECT * FROM test WHERE id < 10;`,
			alters:     []string{"CREATE INDEX ON test (id)"},
			spanSizes:  map[string][]int64{"test": {1000, 1000}},
			size:       2000,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ts := setup(t, tc.alters, tc.spanSizes, tc.createFeed)
			payload, err := ts.getChangefeedPayload(ctx, ts.feedJobId)
			require.NoError(t, err)
			res, err := changefeedccl.FetchChangefeedUsageBytes(ctx, &ts.execCfg, payload)
			require.NoError(t, err)
			assert.Equal(t, res, tc.size)
		})
	}
}

func TestFetchChangefeedUsageBytesE2E(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	fx := newUsageFx(ctx, t)
	defer fx.close()

	fx.query("CREATE TABLE test as SELECT generate_series(1, 1000) AS id")
	fx.query("CREATE TABLE test2 as SELECT generate_series(1, 500) AS id2")

	res := fx.query(`CREATE CHANGEFEED FOR TABLE test INTO 'null://' WITH initial_scan='no';`)
	feedJobId := int64(tree.MustBeDInt(res[0]))

	// Wait for the first run to complete.
	fx.tableBytesTracker.WaitForIncrease()

	// Spin up another feed, and see that the metric gets updated.
	res = fx.query(`CREATE CHANGEFEED FOR TABLE test2 INTO 'null://' WITH initial_scan='no';`)
	feedJobId2 := int64(tree.MustBeDInt(res[0]))

	fx.tableBytesTracker.WaitForIncrease()

	// Pause one of the changefeeds. This should result in the metric reducing again.
	fx.query(`PAUSE JOB $1`, feedJobId)
	fx.tableBytesTracker.WaitForDecrease()

	// Shut down the other changefeed. This should result in the metric getting
	// zeroed out. Note that we can't cancel the job we paused without waiting
	// for it to actually get paused, else the stmt will error.
	fx.query(`CANCEL JOB $1`, feedJobId2)
	fx.tableBytesTracker.WaitForZero()

	// Unpause the first feed, and see that the metric gets updated again. Need
	// to wait for it to go from `pause-requested` to `paused` first, otherwise
	// the RESUME statement errors.
	fx.waitForPausedJob(feedJobId)
	fx.query(`RESUME JOB $1`, feedJobId)
	fx.tableBytesTracker.WaitForIncrease()

	// Test that altering feeds doesn't mess things up. Need to pause it first though.
	fx.query(`PAUSE JOB $1`, feedJobId)
	fx.waitForPausedJob(feedJobId)
	fx.query(`ALTER CHANGEFEED $1 ADD test2 WITH initial_scan='yes'`, feedJobId)
	fx.query(`RESUME JOB $1`, feedJobId)

	fx.tableBytesTracker.WaitForIncrease()

	// Despite all the churn, the error count should still be zero, as we don't count context.Canceled (or similar).
	require.Zero(t, fx.metrics.UsageErrorCount.Count())
}

func TestFetchChangefeedUsageBytesE2EFamilies(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	fx := newUsageFx(ctx, t)
	defer fx.close()

	fx.query(`CREATE TABLE testfam (id PRIMARY KEY FAMILY ids, value FAMILY values) as SELECT generate_series(1, 100) AS id1, generate_series(101, 200) AS value`)

	// Create a feed using split_column_families.
	row := fx.query(`CREATE CHANGEFEED FOR TABLE testfam INTO 'null://' WITH initial_scan='no', split_column_families;`)
	feedJobId := int64(tree.MustBeDInt(row[0]))

	// Wait for the first run to complete.
	fx.tableBytesTracker.WaitForIncrease()

	// Add another family to the table and see the bytes increase.
	fx.query(`ALTER TABLE testfam ADD COLUMN name UUID default gen_random_uuid() CREATE IF NOT EXISTS FAMILY uuids`)
	fx.tableBytesTracker.WaitForIncrease()

	// Cancel this job and wait for shutdown.
	fx.query(`CANCEL JOB $1`, feedJobId)
	fx.tableBytesTracker.WaitForZero()

	// Make a new feed, but manually specifying families.
	row = fx.query(`CREATE CHANGEFEED FOR TABLE testfam FAMILY ids INTO 'null://' WITH initial_scan='no';`)
	feedJobId = int64(tree.MustBeDInt(row[0]))

	fx.tableBytesTracker.WaitForIncrease()

	// Pause, add a family to the feed, and resume.
	fx.query(`PAUSE JOB $1`, feedJobId)
	fx.waitForPausedJob(feedJobId)
	fx.query(`ALTER CHANGEFEED $1 ADD testfam FAMILY values`, feedJobId)
	fx.query(`RESUME JOB $1`, feedJobId)

	fx.tableBytesTracker.WaitForIncrease()

	require.Zero(t, fx.metrics.UsageErrorCount.Count())
}

func TestFetchChangefeedUsageBytesE2EErrorCount(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tss := mocks.NewMockTenantStatusServer(ctrl)
	tss.EXPECT().SpanStats(gomock.Any(), gomock.Any()).Return(nil, errors.New("boom")).MinTimes(1)

	fx := newUsageFxWithMockTss(ctx, t, tss)
	defer fx.close()

	fx.query(`CREATE TABLE test AS SELECT generate_series(1, 1000) AS id`)

	fx.query(`CREATE CHANGEFEED FOR TABLE test INTO 'null://'`)

	fx.errorCountTracker.WaitForIncrease()
}

func TestFetchChangefeedUsageBytesE2EDisabledByDefault(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Create a mock with no expectations. It will error if it's called.
	ctrl := gomock.NewController(t)
	tss := mocks.NewMockTenantStatusServer(ctrl)

	fx := newUsageFxWithMockTss(ctx, t, tss)
	defer fx.close()

	changefeedccl.EnableCloudBillingAccounting = false
	defer func() { changefeedccl.EnableCloudBillingAccounting = true }()

	fx.query(`CREATE TABLE test as SELECT generate_series(1, 1000) AS id`)
	fx.query(`CREATE CHANGEFEED FOR TABLE test INTO 'null://'`)

	// Give it a chance to make that call. TODO: This isn't a very good test but
	// I'm not sure of a low-touch way to improve it.
	err := testutils.SucceedsWithinError(func() error {
		return errors.New("not called")
	}, 5*time.Second)
	require.Error(t, err)
}

type fnMatcher func(arg any) bool

func (f fnMatcher) Matches(x any) bool {
	return f(x)
}

func (f fnMatcher) String() string {
	return "matching function"
}

var _ gomock.Matcher = fnMatcher(nil)

type metricValueTracker struct {
	t             *testing.T
	name          string
	observeValue  func() int64
	observedValue int64
}

func (m *metricValueTracker) WaitForZero() {
	testutils.SucceedsSoon(m.t, func() error {
		if m.observedValue = m.observeValue(); m.observedValue == 0 {
			return nil
		}
		return errors.Newf("metric %q not updated in time. expected 0, got %d", m.name, m.observedValue)
	})
}

func (m *metricValueTracker) WaitForIncrease() {
	testutils.SucceedsSoon(m.t, func() error {
		prevValue := m.observedValue
		if m.observedValue = m.observeValue(); m.observedValue > prevValue {
			return nil
		}
		return errors.Newf("metric %q not updated in time. expected an increase over %d, got %d", m.name, prevValue, m.observedValue)
	})
}

func (m *metricValueTracker) WaitForDecrease() {
	testutils.SucceedsSoon(m.t, func() error {
		prevValue := m.observedValue
		if m.observedValue = m.observeValue(); m.observedValue < prevValue {
			return nil
		}
		return errors.Newf("metric %q not updated in time. expected a decrease from %d, got %d", m.name, prevValue, m.observedValue)
	})
}

type UsageFx struct {
	t       *testing.T
	execCfg sql.ExecutorConfig
	query   func(string, ...any) tree.Datums
	metrics *changefeedccl.JobScopedUsageMetrics

	tableBytesTracker *metricValueTracker
	errorCountTracker *metricValueTracker

	close func()
}

func newUsageFx(ctx context.Context, t *testing.T) *UsageFx {
	return newUsageFxWithMockTss(ctx, t, nil)
}

func newUsageFxWithMockTss(ctx context.Context, t *testing.T, mockTss *mocks.MockTenantStatusServer) *UsageFx {
	changefeedccl.EnableCloudBillingAccounting = true

	params := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			DistSQL:          &execinfra.TestingKnobs{Changefeed: &changefeedccl.TestingKnobs{SkipFirstUsageMetricsReportingWait: true}},
		},
	}
	s := serverutils.StartServerOnly(t, params)

	// TODO: this is a data race. how can we do it properly?
	// TODO: also saw this error, presumably due to this:
	// mvcc_statistics_update_job.go:107: Unexpected call to *mocks.MockTenantStatusServer.SpanStats([context.Background.WithValue(type logtags.contextLogTagsKey, val nsql1).WithValue(type serverident.ServerIdentificationContextKey, val <not Stringer>).WithCancel.WithValue(type multitenant.exemptCtxValueType, val <not Stringer>).WithValue(type tracing.activeSpanKey, val job-104).WithCancel.WithValue(type tracing.activeSpanKey, val MVCC STATISTICS UPDATE-104).WithValue(type logtags.contextLogTagsKey, val nsql1,job=MVCC STATISTICS UPDATE id=104).WithValue(type pprof.labelContextKey, val {"job":"MVCC STATISTICS UPDATE id=104", "n":"sql1"}) node_id:"0" spans:<key:"\376\222" end_key:"\376\223" > ]) at github.com/cockroachdb/cockroach/pkg/sql/mvcc_statistics_update_job.go:107 because: there are no expected calls of the method "SpanStats" for that receiver
	if mockTss != nil {
		s.DistSQLServer().(*distsql.ServerImpl).ExecutorConfig.(*sql.ExecutorConfig).TenantStatusServer = mockTss
	}

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	metrics := execCfg.JobRegistry.MetricsStruct().Changefeed.(*changefeedccl.Metrics).UsageMetrics

	// Set the reporting interval to really long so we only get one run per job.
	// This relies on the fact that we do our first run immediately on startup.
	changefeedbase.UsageMetricsReportingInterval.Override(ctx, execCfg.SV(), 10*time.Hour)

	stmt := "SET CLUSTER SETTING kv.rangefeed.enabled = true"
	_, err := s.SystemLayer().ExecutorConfig().(sql.ExecutorConfig).InternalDB.Executor().Exec(ctx, "test", nil, stmt)
	require.NoError(t, err)

	ie := s.InternalExecutor().(*sql.InternalExecutor)
	sd := sessiondata.InternalExecutorOverride{User: username.NodeUserName()}
	query := func(stmt string, args ...any) tree.Datums {
		res, err := ie.QueryRowEx(ctx, "test", nil, sd, stmt, args...)
		require.NoError(t, err)
		return res
	}
	query("CREATE DATABASE testdb")
	sd.Database = "testdb"

	return &UsageFx{
		t:                 t,
		execCfg:           execCfg,
		query:             query,
		metrics:           metrics,
		tableBytesTracker: &metricValueTracker{t: t, name: "table_bytes", observeValue: metrics.UsageTableBytes.Value},
		errorCountTracker: &metricValueTracker{t: t, name: "error_count", observeValue: metrics.UsageErrorCount.Count},
		close:             func() { s.Stopper().Stop(ctx) },
	}
}

func (fx *UsageFx) getTableSpans(ctx context.Context, name string) (spans roachpb.Spans) {
	require.NoError(fx.t, fx.execCfg.InternalDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		_, td, err := descs.PrefixAndTable(ctx, txn.Descriptors().ByName(txn.KV()).Get(), tree.NewTableNameWithSchema("testdb", "public", tree.Name(name)))
		require.NoError(fx.t, err)
		spans = td.AllIndexSpans(fx.execCfg.Codec)
		return nil
	}))
	return spans
}

const changefeedDetailsQuery = `
       SELECT ji.value
       FROM system.jobs j JOIN system.job_info ji ON j.id = ji.job_id
       WHERE job_type = 'CHANGEFEED'
               AND info_key = '` + jobs.LegacyPayloadKey + `'
               AND j.id = $1
       LIMIT 1
`

func (fx *UsageFx) getChangefeedPayload(ctx context.Context, jobID catpb.JobID) (jobspb.Payload, error) {
	var payload jobspb.Payload
	f := func(ctx context.Context, txn descs.Txn) error {
		row, err := txn.QueryRowEx(ctx, "test", txn.KV(), sessiondata.NodeUserSessionDataOverride, changefeedDetailsQuery, jobID)
		if err != nil {
			return err
		}
		// This should not happen but better to error than panic.
		if len(row) == 0 {
			return errors.Newf("job payload not found: %d", jobID)
		}
		payloadBs := tree.MustBeDBytes(row[0])
		if err := payload.Unmarshal([]byte(payloadBs)); err != nil {
			return errors.WithDetailf(err, "failed to unmarshal payload")
		}
		return err
	}
	if err := fx.execCfg.InternalDB.DescsTxn(ctx, f); err != nil {
		return jobspb.Payload{}, err
	}
	return payload, nil
}

func (fx *UsageFx) waitForPausedJob(jobID int64) {
	require.NoError(fx.t, testutils.SucceedsSoonError(func() error {
		res := fx.query(`WITH js AS (SHOW CHANGEFEED JOBS) SELECT status FROM js WHERE job_id = $1`, jobID)
		status := string(tree.MustBeDString(res[0]))
		if status == "paused" {
			return nil
		}
		return errors.Newf("feed not paused yet: %s", status)
	}))
}
