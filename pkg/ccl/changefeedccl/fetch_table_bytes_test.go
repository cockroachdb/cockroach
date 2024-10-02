// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/mocks"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFetchChangefeedUsageBytesBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	fx := newUsageFixture(ctx, t)
	defer fx.close()

	fx.db.Exec(t, "CREATE TABLE testdb.test as SELECT generate_series(1, 1000) AS id")
	row := fx.db.QueryRow(t, `CREATE CHANGEFEED FOR TABLE testdb.test INTO 'null://' WITH initial_scan='no';`)
	var feedJobId int64
	row.Scan(&feedJobId)

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
		*UsageFixture
		tss       *mocks.MockTenantStatusServer
		feedJobId catpb.JobID
	}

	ctx := context.Background()
	setup := func(t *testing.T, alters []string, spanSizes map[string]int64, createFeed string) *testState {
		usageFx := newUsageFixture(ctx, t)
		t.Cleanup(usageFx.close)

		usageFx.db.Exec(t, "CREATE TABLE test as SELECT generate_series(1, 1000) AS id")
		usageFx.db.Exec(t, "CREATE TABLE test2 (id int primary key)")

		stmt := `CREATE CHANGEFEED FOR TABLE test, test2 INTO 'null://' WITH initial_scan='no';`
		if createFeed != "" {
			stmt = createFeed
		}
		row := usageFx.db.QueryRow(t, stmt)
		var feedJobId int64
		row.Scan(&feedJobId)

		for _, alter := range alters {
			usageFx.db.Exec(t, alter)
		}

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		// Populate response.
		resp := &roachpb.SpanStatsResponse{SpanToStats: map[string]*roachpb.SpanStats{}}
		for tableName, spSz := range spanSizes {
			span := usageFx.getPrimaryTableSpan(ctx, tableName)
			resp.SpanToStats[span.String()] = &roachpb.SpanStats{TotalStats: enginepb.MVCCStats{LiveBytes: spSz}}
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
			UsageFixture: usageFx,
			tss:          tss,
			feedJobId:    catpb.JobID(feedJobId),
		}
	}

	cases := []struct {
		name       string
		createFeed string
		alters     []string
		spanSizes  map[string]int64
		size       int64
	}{
		{
			name:      "basic",
			spanSizes: map[string]int64{"test": 1000, "test2": 100},
			size:      1100,
		},

		{
			name:      "with another index (has no effect on result, and not queried for)",
			alters:    []string{"CREATE INDEX ON test (id)"},
			spanSizes: map[string]int64{"test": 1000, "test2": 100},
			size:      1100,
		},

		{
			name:       "with queries",
			createFeed: `CREATE CHANGEFEED INTO 'null://' WITH initial_scan='no' AS SELECT * FROM test WHERE id < 10;`,
			alters:     []string{"CREATE INDEX ON test (id)"},
			spanSizes:  map[string]int64{"test": 1000},
			size:       1000,
		},

		{
			name:       "with queries on a subset of cols and with a non primary index",
			createFeed: `CREATE CHANGEFEED INTO 'null://' WITH initial_scan='no' AS SELECT id FROM test WHERE id < 10;`,
			alters:     []string{"ALTER TABLE test ADD COLUMN name STRING", "CREATE INDEX ON test (name)"},
			spanSizes:  map[string]int64{"test": 1000},
			size:       1000,
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
	fx := newUsageFixture(ctx, t)
	defer fx.close()

	fx.db.Exec(t, "CREATE TABLE test as SELECT generate_series(1, 1000) AS id")
	fx.db.Exec(t, "CREATE TABLE test2 as SELECT generate_series(1, 500) AS id2")

	res := fx.db.QueryRow(t, `CREATE CHANGEFEED FOR TABLE test INTO 'null://' WITH initial_scan='no';`)
	var feedJobId int64
	res.Scan(&feedJobId)

	// Wait for the first run to complete.
	fx.tableBytesTracker.WaitForIncrease()
	fx.queryDurationTracker.WaitForIncrease() // the query duration metric should be updated

	// Spin up another feed, and see that the metric gets updated.
	res = fx.db.QueryRow(t, `CREATE CHANGEFEED FOR TABLE test2 INTO 'null://' WITH initial_scan='no';`)
	var feedJobId2 int64
	res.Scan(&feedJobId2)

	fx.tableBytesTracker.WaitForIncrease()

	// Pause one of the changefeeds. This should result in the metric reducing again.
	fx.db.Exec(t, `PAUSE JOB $1`, feedJobId)
	fx.tableBytesTracker.WaitForDecrease()

	// Shut down the other changefeed. This should result in the metric getting
	// zeroed out. Note that we can't cancel the job we paused without waiting
	// for it to actually get paused, else the stmt will error.
	fx.db.Exec(t, `CANCEL JOB $1`, feedJobId2)
	fx.tableBytesTracker.WaitForZero()

	// Unpause the first feed, and see that the metric gets updated again. Need
	// to wait for it to go from `pause-requested` to `paused` first, otherwise
	// the RESUME statement errors.
	fx.waitForPausedJob(feedJobId)
	fx.db.Exec(t, `RESUME JOB $1`, feedJobId)
	fx.tableBytesTracker.WaitForIncrease()

	// Test that altering feeds doesn't mess things up. Need to pause it first though.
	fx.db.Exec(t, `PAUSE JOB $1`, feedJobId)
	fx.waitForPausedJob(feedJobId)
	fx.db.Exec(t, `ALTER CHANGEFEED $1 ADD test2 WITH initial_scan='yes'`, feedJobId)
	fx.db.Exec(t, `RESUME JOB $1`, feedJobId)

	fx.tableBytesTracker.WaitForIncrease()

	// Despite all the churn, the error count should still be zero, as we don't count context.Canceled (or similar).
	require.Zero(t, fx.metrics.UsageErrorCount.Count())
}

func TestFetchChangefeedUsageBytesE2EFamilies(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	fx := newUsageFixture(ctx, t)
	defer fx.close()

	fx.db.Exec(t, `CREATE TABLE testfam (id PRIMARY KEY FAMILY ids, value FAMILY values) as SELECT generate_series(1, 100) AS id1, generate_series(101, 200) AS value`)

	// Create a feed using split_column_families.
	row := fx.db.QueryRow(t, `CREATE CHANGEFEED FOR TABLE testfam INTO 'null://' WITH initial_scan='no', split_column_families;`)
	var feedJobId int64
	row.Scan(&feedJobId)

	// Wait for the first run to complete.
	fx.tableBytesTracker.WaitForIncrease()

	// Add another family to the table and see the bytes increase.
	fx.db.Exec(t, `ALTER TABLE testfam ADD COLUMN name UUID default gen_random_uuid() CREATE IF NOT EXISTS FAMILY uuids`)
	fx.tableBytesTracker.WaitForIncrease()

	// Cancel this job and wait for shutdown.
	fx.db.Exec(t, `CANCEL JOB $1`, feedJobId)
	fx.tableBytesTracker.WaitForZero()

	// Make a new feed, but manually specifying families.
	row = fx.db.QueryRow(t, `CREATE CHANGEFEED FOR TABLE testfam FAMILY ids INTO 'null://' WITH initial_scan='no';`)
	row.Scan(&feedJobId)

	fx.tableBytesTracker.WaitForIncrease()

	// Pause, add a family to the feed, and resume.
	fx.db.Exec(t, `PAUSE JOB $1`, feedJobId)
	fx.waitForPausedJob(feedJobId)
	fx.db.Exec(t, `ALTER CHANGEFEED $1 ADD testfam FAMILY values`, feedJobId)
	fx.db.Exec(t, `RESUME JOB $1`, feedJobId)

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

	fx := newUsageFixtureWithMockTSS(ctx, t, tss)
	defer fx.close()

	fx.db.Exec(t, `CREATE TABLE test AS SELECT generate_series(1, 1000) AS id`)

	fx.db.Exec(t, `CREATE CHANGEFEED FOR TABLE test INTO 'null://'`)

	fx.errorCountTracker.WaitForIncrease()
}

func TestFetchChangefeedUsageBytesE2EDisabledByDefault(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Create a mock with no expectations. It will error if it's called.
	ctrl := gomock.NewController(t)
	tss := mocks.NewMockTenantStatusServer(ctrl)

	fx := newUsageFixtureWithMockTSS(ctx, t, tss)
	defer fx.close()

	changefeedccl.EnableCloudBillingAccounting = false
	defer func() { changefeedccl.EnableCloudBillingAccounting = true }()

	fx.db.Exec(t, `CREATE TABLE test as SELECT generate_series(1, 1000) AS id`)
	fx.db.Exec(t, `CREATE CHANGEFEED FOR TABLE test INTO 'null://'`)

	// It should not wait on the timer.
	err := testutils.SucceedsWithinError(func() error {
		require.Empty(t, fx.ts.Timers())
		return errors.New("did not find a timer")
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
	ts            *timeutil.ManualTime
	name          string
	observeValue  func() int64
	observedValue int64
}

func (m *metricValueTracker) AssertNoChange() {
	m.t.Helper()
	prevValue := m.observedValue
	m.ts.Advance(time.Hour)
	require.Equal(m.t, prevValue, m.observeValue(), "metric %q changed unexpectedly", m.name)
}

func (m *metricValueTracker) WaitForZero() {
	m.t.Helper()
	testutils.SucceedsSoon(m.t, func() error {
		m.ts.Advance(time.Hour)
		if m.observedValue = m.observeValue(); m.observedValue == 0 {
			return nil
		}
		return errors.Newf("metric %q not updated in time. expected 0, got %d", m.name, m.observedValue)
	})
}

func (m *metricValueTracker) WaitForIncrease() {
	m.t.Helper()
	testutils.SucceedsSoon(m.t, func() error {
		m.ts.Advance(time.Hour)
		prevValue := m.observedValue
		if m.observedValue = m.observeValue(); m.observedValue > prevValue {
			return nil
		}
		return errors.Newf("metric %q not updated in time. expected an increase over %d, got %d", m.name, prevValue, m.observedValue)
	})
}

func (m *metricValueTracker) WaitForDecrease() {
	m.t.Helper()
	testutils.SucceedsSoon(m.t, func() error {
		m.ts.Advance(time.Hour)
		prevValue := m.observedValue
		if m.observedValue = m.observeValue(); m.observedValue < prevValue {
			return nil
		}
		return errors.Newf("metric %q not updated in time. expected a decrease from %d, got %d", m.name, prevValue, m.observedValue)
	})
}

// UsageFixture is a test fixture for testing the usage metrics of changefeeds.
type UsageFixture struct {
	t       *testing.T
	ts      *timeutil.ManualTime
	execCfg sql.ExecutorConfig
	db      *sqlutils.SQLRunner
	metrics *changefeedccl.JobScopedUsageMetrics

	tableBytesTracker    *metricValueTracker
	errorCountTracker    *metricValueTracker
	queryDurationTracker *metricValueTracker

	close func()
}

func newUsageFixture(ctx context.Context, t *testing.T) *UsageFixture {
	return newUsageFixtureWithMockTSS(ctx, t, nil)
}

func newUsageFixtureWithMockTSS(
	ctx context.Context, t *testing.T, mockTSS *mocks.MockTenantStatusServer,
) *UsageFixture {
	changefeedccl.EnableCloudBillingAccounting = true

	ts := timeutil.NewManualTime(time.Now())

	knobs := base.TestingKnobs{
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		DistSQL: &execinfra.TestingKnobs{Changefeed: &changefeedccl.TestingKnobs{
			TimeSource: ts,
		}},
		Server: &server.TestingKnobs{},
	}

	if mockTSS != nil {
		knobs.DistSQL.(*execinfra.TestingKnobs).Changefeed.(*changefeedccl.TestingKnobs).OverrideExecCfg = func(execCfg *sql.ExecutorConfig) *sql.ExecutorConfig {
			newExecCfg := *execCfg
			newExecCfg.TenantStatusServer = mockTSS
			return &newExecCfg
		}
	}

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{Knobs: knobs})

	rootDB := sqlutils.MakeSQLRunner(db)

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	metrics := execCfg.JobRegistry.MetricsStruct().Changefeed.(*changefeedccl.Metrics).UsageMetrics

	// Must be executed on the system layer, not the tenant, if we're a tenant (metamorphic).
	_, err := s.SystemLayer().InternalDB().(isql.DB).Executor().ExecEx(ctx, "kvrf", nil, sessiondata.InternalExecutorOverride{User: username.NodeUserName()}, "SET CLUSTER SETTING kv.rangefeed.enabled = true")
	require.NoError(t, err)

	rootDB.Exec(t, "CREATE DATABASE testdb")
	rootDB.Exec(t, "USE testdb")

	metrics.UsageQueryDuration.WindowedSnapshot().Total()

	return &UsageFixture{
		t:                    t,
		ts:                   ts,
		execCfg:              execCfg,
		db:                   rootDB,
		metrics:              metrics,
		tableBytesTracker:    &metricValueTracker{t: t, ts: ts, name: "table_bytes", observeValue: metrics.UsageTableBytes.Value},
		errorCountTracker:    &metricValueTracker{t: t, ts: ts, name: "error_count", observeValue: metrics.UsageErrorCount.Count},
		queryDurationTracker: &metricValueTracker{t: t, ts: ts, name: "query_duration", observeValue: func() int64 { c, _ := metrics.UsageQueryDuration.CumulativeSnapshot().Total(); return c }},
		close:                func() { s.Stopper().Stop(ctx) },
	}
}

func (fx *UsageFixture) getPrimaryTableSpan(ctx context.Context, name string) (span roachpb.Span) {
	require.NoError(fx.t, fx.execCfg.InternalDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		_, td, err := descs.PrefixAndTable(ctx, txn.Descriptors().ByName(txn.KV()).Get(), tree.NewTableNameWithSchema("testdb", "public", tree.Name(name)))
		require.NoError(fx.t, err)
		span = td.PrimaryIndexSpan(fx.execCfg.Codec)
		return nil
	}))
	return span
}

const changefeedDetailsQuery = `
       SELECT ji.value
       FROM system.jobs j JOIN system.job_info ji ON j.id = ji.job_id
       WHERE job_type = 'CHANGEFEED'
               AND info_key = '` + jobs.LegacyPayloadKey + `'
               AND j.id = $1
       LIMIT 1
`

func (fx *UsageFixture) getChangefeedPayload(
	ctx context.Context, jobID catpb.JobID,
) (jobspb.Payload, error) {
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
		payloadBs := []byte(tree.MustBeDBytes(row[0]))
		if err := protoutil.Unmarshal(payloadBs, &payload); err != nil {
			return errors.WithDetailf(err, "failed to unmarshal payload")
		}
		return err
	}
	if err := fx.execCfg.InternalDB.DescsTxn(ctx, f); err != nil {
		return jobspb.Payload{}, err
	}
	return payload, nil
}

func (fx *UsageFixture) waitForPausedJob(jobID int64) {
	require.NoError(fx.t, testutils.SucceedsSoonError(func() error {
		res := fx.db.QueryRow(fx.t, `WITH js AS (SHOW CHANGEFEED JOBS) SELECT status FROM js WHERE job_id = $1`, jobID)
		var status string
		res.Scan(&status)
		if status == "paused" {
			return nil
		}
		return errors.Newf("feed not paused yet: %s", status)
	}))
}
