// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	gosql "database/sql"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedpb"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobstest"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs/schedulebase"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/require"
)

const allSchedules = 0

// testHelper starts a server, and arranges for job scheduling daemon to
// use jobstest.JobSchedulerTestEnv.
// This helper also arranges for the manual override of scheduling logic
// via executeSchedules callback.
type execSchedulesFn = func(ctx context.Context, maxSchedules int64) error
type testHelper struct {
	env              *jobstest.JobSchedulerTestEnv
	cfg              *scheduledjobs.JobExecutionConfig
	db               *gosql.DB
	sqlDB            *sqlutils.SQLRunner
	server           serverutils.TestServerInterface
	executeSchedules func() error
	createdSchedules []jobspb.ScheduleID
}

func newTestHelper(t *testing.T) (*testHelper, func()) {
	sh := &testHelper{
		env: jobstest.NewJobSchedulerTestEnv(
			jobstest.UseSystemTables, timeutil.Now(), tree.ScheduledChangefeedExecutor),
	}

	s, db, stopServer := startTestFullServer(t, makeOptions(withSchedulerHelper(sh)))
	sh.db = db
	sh.sqlDB = sqlutils.MakeSQLRunner(db)
	sh.server = s
	return sh, stopServer
}

// withScheduleHelper returns an option to configure test server with
// settings required to make scheduled jobs system function in tests.
func withSchedulerHelper(sh *testHelper) func(opts *feedTestOptions) {
	return func(opts *feedTestOptions) {
		opts.knobsFn = func(knobs *base.TestingKnobs) {
			jobsKnobs := knobs.JobsTestingKnobs.(*jobs.TestingKnobs)
			jobsKnobs.JobSchedulerEnv = sh.env
			jobsKnobs.TakeOverJobsScheduling = func(fn execSchedulesFn) {
				sh.executeSchedules = func() error {
					defer sh.server.JobRegistry().(*jobs.Registry).TestingNudgeAdoptionQueue()
					return fn(context.Background(), allSchedules)
				}
			}
			jobsKnobs.CaptureJobExecutionConfig = func(config *scheduledjobs.JobExecutionConfig) {
				sh.cfg = config
			}
		}
	}
}

func (h *testHelper) loadSchedule(t *testing.T, scheduleID jobspb.ScheduleID) *jobs.ScheduledJob {
	t.Helper()

	loaded, err := jobs.ScheduledJobDB(h.internalDB()).
		Load(context.Background(), h.env, scheduleID)
	require.NoError(t, err)
	return loaded
}

func (h *testHelper) clearSchedules(t *testing.T) {
	t.Helper()

	var sb strings.Builder
	sep := ""
	for _, id := range h.createdSchedules {
		sb.WriteString(sep)
		sb.WriteString(strconv.FormatInt(int64(id), 10))
		sep = ", "
	}
	h.sqlDB.Exec(t, fmt.Sprintf("DELETE FROM %s WHERE schedule_id IN (%s)",
		h.env.ScheduledJobsTableName(), sb.String()))
}

func (h *testHelper) waitForSuccessfulScheduledJob(
	t *testing.T, scheduleID jobspb.ScheduleID,
) int64 {
	query := "SELECT id FROM " + h.env.SystemJobsTableName() +
		" WHERE status=$1 AND created_by_type=$2 AND created_by_id=$3"
	var jobID int64

	testutils.SucceedsSoon(t, func() error {
		// Force newly created job to be adopted and verify it succeeds.
		h.server.JobRegistry().(*jobs.Registry).TestingNudgeAdoptionQueue()

		return h.sqlDB.DB.QueryRowContext(context.Background(),
			query, jobs.StateSucceeded, jobs.CreatedByScheduledJobs, scheduleID).Scan(&jobID)
	})

	return jobID
}

// createChangefeedSchedule executes specified "CREATE SCHEDULE FOR CHANGEFEED"
// query, with the provided arguments.  Returns created schedule.
func (h *testHelper) createChangefeedSchedule(
	t *testing.T, query string, args ...interface{},
) (*jobs.ScheduledJob, error) {
	var id int64
	var unusedStr string
	var unusedTS *time.Time
	if err := h.sqlDB.DB.QueryRowContext(context.Background(), query, args...).Scan(
		&id, &unusedStr, &unusedStr, &unusedTS, &unusedStr, &unusedStr,
	); err != nil {
		return nil, err
	}
	// Query system.scheduled_job table and load those schedules.
	datums, cols, err := h.cfg.DB.Executor().QueryRowExWithCols(
		context.Background(), "sched-load", nil,
		sessiondata.NodeUserSessionDataOverride,
		"SELECT * FROM system.scheduled_jobs WHERE schedule_id = $1",
		id,
	)
	require.NoError(t, err)
	require.NotNil(t, datums)

	sj := jobs.NewScheduledJob(h.env)
	require.NoError(t, sj.InitFromDatums(datums, cols))

	h.createdSchedules = append(h.createdSchedules, sj.ScheduleID())
	return sj, nil
}

func getScheduledChangefeedStatement(t *testing.T, arg *jobspb.ExecutionArguments) string {
	var scheduledChangefeed changefeedpb.ScheduledChangefeedExecutionArgs
	require.NoError(t, pbtypes.UnmarshalAny(arg.Args, &scheduledChangefeed))
	return scheduledChangefeed.ChangefeedStatement
}

func (h *testHelper) internalDB() descs.DB {
	return h.server.InternalDB().(descs.DB)
}

// This test examines serialized representation of changefeed schedule arguments
// when the scheduled changefeed statement executes.  This test does not concern
// itself with the actual scheduling and the execution of those changefeeds.
func TestSerializesScheduledChangefeedExecutionArgs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	th, cleanup := newTestHelper(t)
	defer cleanup()

	th.sqlDB.Exec(t, `CREATE TABLE foo (a INT, b STRING)`)

	type expectedSchedule struct {
		nameRe         string
		changefeedStmt string
		period         time.Duration
		runsNow        bool
		shownStmt      string
	}

	testCases := []struct {
		name      string
		query     string
		queryArgs []interface{}
		es        expectedSchedule
		errMsg    string
	}{
		{
			name:  "changefeed",
			query: "CREATE SCHEDULE FOR CHANGEFEED d.public.foo INTO 'webhook-https://0/changefeed?AWS_SECRET_ACCESS_KEY=nevershown' WITH initial_scan='only' RECURRING '@hourly'",
			es: expectedSchedule{
				nameRe:         "CHANGEFEED .+",
				changefeedStmt: "CREATE CHANGEFEED FOR TABLE d.public.foo INTO 'webhook-https://0/changefeed?AWS_SECRET_ACCESS_KEY=nevershown' WITH OPTIONS (initial_scan = 'only')",
				shownStmt:      "CREATE CHANGEFEED FOR TABLE d.public.foo INTO 'webhook-https://0/changefeed?AWS_SECRET_ACCESS_KEY=redacted' WITH OPTIONS (initial_scan = 'only')",
				period:         time.Hour,
			},
		},
		{
			name:  "changefeed-qualifies-names",
			query: "CREATE SCHEDULE FOR CHANGEFEED foo INTO 'webhook-https://0/changefeed' WITH initial_scan = 'only' RECURRING '@hourly'",
			es: expectedSchedule{
				nameRe:         "CHANGEFEED .+",
				changefeedStmt: "CREATE CHANGEFEED FOR TABLE d.public.foo INTO 'webhook-https://0/changefeed' WITH OPTIONS (initial_scan = 'only')",
				period:         time.Hour,
			},
		},
		{
			name: "changefeed-with-options",
			query: `
		CREATE SCHEDULE 'foo-changefeed'
		FOR CHANGEFEED  foo
		INTO 'webhook-https://0/changefeed?AWS_SECRET_ACCESS_KEY=nevershown'
		WITH format='JSON'
		RECURRING '@hourly' WITH SCHEDULE OPTIONS on_execution_failure = 'pause', first_run=$1
		`,
			queryArgs: []interface{}{th.env.Now()},
			es: expectedSchedule{
				nameRe:         "foo-changefeed",
				changefeedStmt: "CREATE CHANGEFEED FOR TABLE d.public.foo INTO 'webhook-https://0/changefeed?AWS_SECRET_ACCESS_KEY=nevershown' WITH OPTIONS (format = 'JSON', initial_scan = 'only')",
				shownStmt:      "CREATE CHANGEFEED FOR TABLE d.public.foo INTO 'webhook-https://0/changefeed?AWS_SECRET_ACCESS_KEY=redacted' WITH OPTIONS (format = 'JSON', initial_scan = 'only')",
				period:         time.Hour,
				runsNow:        true,
			},
		},
		{
			name: "changefeed-expressions",
			query: `
		CREATE SCHEDULE 'foo-changefeed'
		FOR CHANGEFEED INTO 'webhook-https://0/changefeed?AWS_SECRET_ACCESS_KEY=nevershown'
		WITH format='JSON', initial_scan = 'only', schema_change_policy='stop'
		AS SELECT * FROM foo
		RECURRING '@hourly' WITH SCHEDULE OPTIONS on_execution_failure = 'pause', first_run=$1
		`,
			queryArgs: []interface{}{th.env.Now()},
			es: expectedSchedule{
				nameRe:         "foo-changefeed",
				changefeedStmt: "CREATE CHANGEFEED INTO 'webhook-https://0/changefeed?AWS_SECRET_ACCESS_KEY=nevershown' WITH OPTIONS (format = 'JSON', initial_scan = 'only', schema_change_policy = 'stop') AS SELECT * FROM d.public.foo",
				shownStmt:      "CREATE CHANGEFEED INTO 'webhook-https://0/changefeed?AWS_SECRET_ACCESS_KEY=redacted' WITH OPTIONS (format = 'JSON', initial_scan = 'only', schema_change_policy = 'stop') AS SELECT * FROM d.public.foo",
				period:         time.Hour,
				runsNow:        true,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			defer th.clearSchedules(t)

			sj, err := th.createChangefeedSchedule(t, tc.query, tc.queryArgs...)
			if len(tc.errMsg) > 0 {
				require.True(t, testutils.IsError(err, tc.errMsg),
					"expected error to match %q, found %q instead", tc.errMsg, err.Error())
				return
			}

			require.NoError(t, err)

			expectedSchedule := tc.es
			changefeedStmt := getScheduledChangefeedStatement(t, sj.ExecutionArgs())
			require.Equal(t, expectedSchedule.changefeedStmt, changefeedStmt)

			frequency, err := sj.Frequency()
			require.NoError(t, err)
			require.EqualValues(t, expectedSchedule.period, frequency, expectedSchedule)

			if expectedSchedule.runsNow {
				require.EqualValues(t, th.env.Now().Round(time.Microsecond), sj.ScheduledRunTime())
			}

			var shownStmt string
			th.sqlDB.QueryRow(t, `SELECT command->>'changefeed_statement' FROM [SHOW SCHEDULE $1]`,
				sj.ScheduleID()).Scan(&shownStmt)

			if expectedSchedule.shownStmt != "" {
				require.Equal(t, expectedSchedule.shownStmt, shownStmt)
			} else {
				require.Equal(t, expectedSchedule.changefeedStmt, shownStmt)
			}
		})
	}
}

// TestCreateChangefeedScheduleChecksPermissionsDuringDryRun verifies
// that we perform a dry run of creating the changefeed (performs
// permissions checks).
func TestCreateChangefeedScheduleChecksPermissionsDuringDryRun(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TODOTestTenantDisabled,
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			DistSQL: &execinfra.TestingKnobs{
				Changefeed: &TestingKnobs{
					WrapSink: func(s Sink, _ jobspb.JobID) Sink {
						if _, ok := s.(*externalConnectionKafkaSink); ok {
							return s
						}
						return &externalConnectionKafkaSink{sink: s}
					},
				},
			},
		},
	})
	defer s.Stopper().Stop(ctx)
	rootDB := sqlutils.MakeSQLRunner(db)
	rootDB.Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)
	enableEnterprise := utilccl.TestingDisableEnterprise()
	enableEnterprise()

	rootDB.Exec(t, `CREATE TABLE table_a (i int)`)
	rootDB.Exec(t, `CREATE USER testuser WITH PASSWORD 'test'`)

	pgURL := url.URL{
		Scheme: "postgres",
		User:   url.UserPassword("testuser", "test"),
		Host:   s.SQLAddr(),
	}
	db2, err := gosql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	userDB := sqlutils.MakeSQLRunner(db2)

	userDB.ExpectErr(t, "Failed to dry run create changefeed: user testuser requires the CHANGEFEED privilege on all target tables to be able to run an enterprise changefeed",
		"CREATE SCHEDULE FOR CHANGEFEED TABLE table_a INTO 'somewhere' WITH initial_scan = 'only' RECURRING '@daily'")
}

// TestCreateChangefeedScheduleIfNotExists: checks if adding IF NOT EXISTS will
// create the schedule only if the schedule label doesn't already exist.
func TestCreateChangefeedScheduleIfNotExists(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	th, cleanup := newTestHelper(t)
	defer cleanup()

	th.sqlDB.Exec(t, "CREATE TABLE t1 (a INT)")

	const scheduleLabel = "foo"
	const createQuery = "CREATE SCHEDULE IF NOT EXISTS '%s' FOR CHANGEFEED TABLE t1 INTO 'null://' WITH initial_scan = 'only' RECURRING '@daily'"

	th.sqlDB.Exec(t, fmt.Sprintf(createQuery, scheduleLabel))

	// no op expected
	th.sqlDB.Exec(t, fmt.Sprintf(createQuery, scheduleLabel))

	const selectQuery = "SELECT label FROM [SHOW SCHEDULES FOR CHANGEFEED]"

	rows, err := th.cfg.DB.Executor().QueryBufferedEx(
		context.Background(), "check-sched", nil,
		sessiondata.NodeUserSessionDataOverride,
		selectQuery)

	require.NoError(t, err)
	require.Equal(t, 1, len(rows))

	// the 'bar' schedule should get scheduled
	const newScheduleLabel = "bar"

	th.sqlDB.Exec(t, fmt.Sprintf(createQuery, newScheduleLabel))

	rows, err = th.cfg.DB.Executor().QueryBufferedEx(
		context.Background(), "check-sched2", nil,
		sessiondata.NodeUserSessionDataOverride,
		selectQuery)

	require.NoError(t, err)
	require.Equal(t, 2, len(rows))
}

func TestCreateChangefeedScheduleInExplicitTxnRollback(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	th, cleanup := newTestHelper(t)
	defer cleanup()

	th.sqlDB.Exec(t, "CREATE TABLE t1 (a INT)")
	res := th.sqlDB.Query(t, "SELECT id FROM [SHOW SCHEDULES FOR CHANGEFEED]")
	require.False(t, res.Next())
	require.NoError(t, res.Err())

	th.sqlDB.Exec(t, "BEGIN;")
	th.sqlDB.Exec(t, "SET LOCAL autocommit_before_ddl = false;")
	th.sqlDB.Exec(t, "CREATE SCHEDULE FOR CHANGEFEED TABLE t1 INTO 'null://' WITH initial_scan = 'only' RECURRING '@daily';")
	th.sqlDB.Exec(t, "ROLLBACK;")

	res = th.sqlDB.Query(t, "SELECT id FROM [SHOW SCHEDULES FOR CHANGEFEED]")
	require.False(t, res.Next())
	require.NoError(t, res.Err())
}

// TestScheduledChangefeed schedules a changefeed and verifies (for 1 run) the
// changefeed gets executed properly.
func TestScheduledChangefeed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	th, cleanup := newTestHelper(t)
	defer cleanup()

	th.sqlDB.Exec(t, `
CREATE TABLE t1(a INT PRIMARY KEY, b STRING);
INSERT INTO t1 values (1, 'one'), (10, 'ten'), (100, 'hundred');

CREATE TABLE t2(b INT PRIMARY KEY, c STRING);
INSERT INTO t2 VALUES (3, 'three'), (2, 'two'), (1, 'one');
`)

	getFeed := func(envelopeType changefeedbase.EnvelopeType, db *gosql.DB) (string, *webhookFeed, func()) {
		cert, _, err := cdctest.NewCACertBase64Encoded()
		require.NoError(t, err)
		sinkDest, err := cdctest.StartMockWebhookSink(cert)
		require.NoError(t, err)

		dummyWrapper := func(s Sink) Sink {
			return s
		}
		// NB: This is a partially initialized webhookFeed.
		// You must call th.waitForSuccessfulScheduledJob prior to reading
		// feed messages. If messages are tried to access before they are present,
		// this feed will panic because sink synchronizer is not initialized.
		feed := &webhookFeed{
			seenTrackerMap: make(map[string]struct{}),
			mockSink:       sinkDest,
			envelopeType:   envelopeType,
			jobFeed:        newJobFeed(db, dummyWrapper),
		}

		sinkURI := fmt.Sprintf("webhook-%s?insecure_tls_skip_verify=true", sinkDest.URL())
		return sinkURI, feed, sinkDest.Close
	}

	testCases := []struct {
		name            string
		scheduleStmt    string
		expectedPayload []string
		envelopeType    changefeedbase.EnvelopeType
	}{
		{
			name:         "one-table",
			scheduleStmt: "CREATE SCHEDULE FOR changefeed TABLE t1 INTO $1 RECURRING '@hourly'",
			expectedPayload: []string{
				`t1: [1]->{"after": {"a": 1, "b": "one"}}`,
				`t1: [10]->{"after": {"a": 10, "b": "ten"}}`,
				`t1: [100]->{"after": {"a": 100, "b": "hundred"}}`,
			},
			envelopeType: changefeedbase.OptEnvelopeWrapped,
		},
		{
			name:         "two-table",
			scheduleStmt: "CREATE SCHEDULE FOR CHANGEFEED TABLE t1, t2 INTO $1 WITH initial_scan='only' RECURRING '@hourly'",
			expectedPayload: []string{
				`t1: [1]->{"after": {"a": 1, "b": "one"}}`,
				`t1: [10]->{"after": {"a": 10, "b": "ten"}}`,
				`t1: [100]->{"after": {"a": 100, "b": "hundred"}}`,
				`t2: [3]->{"after": {"b": 3, "c": "three"}}`,
				`t2: [2]->{"after": {"b": 2, "c": "two"}}`,
				`t2: [1]->{"after": {"b": 1, "c": "one"}}`,
			},
			envelopeType: changefeedbase.OptEnvelopeWrapped,
		},
		{
			name: "changefeed-expressions",
			// key_in_value is only need here to make the webhookFeed work.
			scheduleStmt: "CREATE SCHEDULE FOR CHANGEFEED INTO $1 WITH key_in_value, topic_in_value, initial_scan='only', schema_change_policy='stop' AS SELECT b FROM t1 RECURRING '@hourly'",
			expectedPayload: []string{
				`t1: [1]->{"b": "one"}`,
				`t1: [10]->{"b": "ten"}`,
				`t1: [100]->{"b": "hundred"}`,
			},
			envelopeType: changefeedbase.OptEnvelopeBare,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			sinkURI, feed, cleanup := getFeed(test.envelopeType, th.db)
			defer cleanup()

			sj, err := th.createChangefeedSchedule(
				t, test.scheduleStmt, sinkURI)
			require.NoError(t, err)
			defer th.clearSchedules(t)

			// Force the schedule to execute.
			th.env.SetTime(sj.NextRun().Add(time.Second))
			require.NoError(t, th.executeSchedules())

			jobID := th.waitForSuccessfulScheduledJob(t, sj.ScheduleID())
			// We need to set the job ID here explicitly because the webhookFeed.Next
			// function calls jobFeed.Details, which uses the job ID to get the
			// changefeed details from the system.jobs table.
			feed.jobFeed.jobID = jobspb.JobID(jobID)
			assertPayloads(t, feed, test.expectedPayload)
		})
	}
}

// TestPauseScheduledChangefeedOnNewClusterID schedules a changefeed and verifies the changefeed pauses
// if it is running on a cluster with a different ID than is stored in the schedule details
func TestPauseScheduledChangefeedOnNewClusterID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	th, cleanup := newTestHelper(t)
	defer cleanup()

	th.sqlDB.Exec(t, `
CREATE TABLE t1(a INT PRIMARY KEY, b STRING);
INSERT INTO t1 values (1, 'one'), (10, 'ten'), (100, 'hundred');

CREATE TABLE t2(b INT PRIMARY KEY, c STRING);
INSERT INTO t2 VALUES (3, 'three'), (2, 'two'), (1, 'one');
`)

	getFeed := func(envelopeType changefeedbase.EnvelopeType, db *gosql.DB) (string, *webhookFeed, func()) {
		cert, _, err := cdctest.NewCACertBase64Encoded()
		require.NoError(t, err)
		sinkDest, err := cdctest.StartMockWebhookSink(cert)
		require.NoError(t, err)

		dummyWrapper := func(s Sink) Sink {
			return s
		}
		// NB: This is a partially initialized webhookFeed.
		// You must call th.waitForSuccessfulScheduledJob prior to reading
		// feed messages. If messages are tried to access before they are present,
		// this feed will panic because sink synchronizer is not initialized.
		feed := &webhookFeed{
			seenTrackerMap: make(map[string]struct{}),
			mockSink:       sinkDest,
			envelopeType:   envelopeType,
			jobFeed:        newJobFeed(db, dummyWrapper),
		}

		sinkURI := fmt.Sprintf("webhook-%s?insecure_tls_skip_verify=true", sinkDest.URL())
		return sinkURI, feed, sinkDest.Close
	}

	testCase := struct {
		name            string
		scheduleStmt    string
		expectedPayload []string
		envelopeType    changefeedbase.EnvelopeType
	}{
		name:         "one-table",
		scheduleStmt: "CREATE SCHEDULE FOR changefeed TABLE t1 INTO $1 RECURRING '@hourly'",
		expectedPayload: []string{
			`t1: [1]->{"after": {"a": 1, "b": "one"}}`,
			`t1: [10]->{"after": {"a": 10, "b": "ten"}}`,
			`t1: [100]->{"after": {"a": 100, "b": "hundred"}}`,
		},
		envelopeType: changefeedbase.OptEnvelopeWrapped,
	}

	t.Run(testCase.name, func(t *testing.T) {
		sinkURI, feed, cleanup := getFeed(testCase.envelopeType, th.db)
		defer cleanup()

		sj, err := th.createChangefeedSchedule(
			t, testCase.scheduleStmt, sinkURI)
		require.NoError(t, err)
		defer th.clearSchedules(t)

		// Get schedule DB to update the schedule with new clusterID
		scheduleStorage := jobs.ScheduledJobDB(th.internalDB())

		details := sj.ScheduleDetails()
		currentClusterID := details.ClusterID
		require.NotZero(t, currentClusterID)

		// Modify the scheduled clusterID and update the job
		details.ClusterID = jobstest.DummyClusterID
		sj.SetScheduleDetails(*details)
		th.env.SetTime(sj.NextRun().Add(time.Second))
		require.NoError(t, scheduleStorage.Update(context.Background(), sj))
		require.NoError(t, th.executeSchedules())

		// Schedule is expected to be paused
		testutils.SucceedsSoon(t, func() error {
			expectPausedSchedule := th.loadSchedule(t, sj.ScheduleID())
			if !expectPausedSchedule.IsPaused() {
				return errors.New("schedule has not paused yet")
			}
			// The cluster ID should have been reset.
			require.Equal(t, currentClusterID, expectPausedSchedule.ScheduleDetails().ClusterID)
			return nil
		})

		th.sqlDB.Exec(t, "RESUME SCHEDULE $1", sj.ScheduleID())
		resumedSchedule := th.loadSchedule(t, sj.ScheduleID())
		require.False(t, resumedSchedule.IsPaused())
		th.env.SetTime(resumedSchedule.NextRun().Add(time.Second))
		require.NoError(t, th.executeSchedules())
		jobID := th.waitForSuccessfulScheduledJob(t, sj.ScheduleID())
		// We need to set the job ID here explicitly because the webhookFeed.Next
		// function calls jobFeed.Details, which uses the job ID to get the
		// changefeed details from the system.jobs table.
		feed.jobFeed.jobID = jobspb.JobID(jobID)
		assertPayloads(t, feed, testCase.expectedPayload)
	})
}

// TestScheduledChangefeedErrors tests cases where a schedule changefeed statement will return an error.
func TestScheduledChangefeedErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	th, cleanup := newTestHelper(t)
	defer cleanup()

	th.sqlDB.Exec(t, `
		CREATE TABLE t1(a INT PRIMARY KEY, b STRING);
	`)

	testCases := []struct {
		name    string
		stmt    string
		errRE   string
		hintStr string
	}{
		{
			name:    "implicit-conflict-with-initial-scan-only",
			stmt:    "CREATE SCHEDULE test_schedule FOR CHANGEFEED t1 INTO 'null://sink' WITH resolved RECURRING '@daily';",
			errRE:   "cannot specify both initial_scan='only' and resolved",
			hintStr: "scheduled changefeeds implicitly pass the option initial_scan='only'",
		},
		{
			name:  "explicit-conflict-with-initial-scan-only",
			stmt:  "CREATE SCHEDULE test_schedule FOR CHANGEFEED t1 INTO 'null://sink' WITH resolved, initial_scan='only' RECURRING '@daily';",
			errRE: "cannot specify both initial_scan='only' and resolved",
		},
		{
			name:  "explicit-conflict-with-initial-scan-only",
			stmt:  "CREATE SCHEDULE test_schedule FOR CHANGEFEED t1 INTO 'null://sink' WITH resolved, initial_scan='no' RECURRING '@daily';",
			errRE: "initial_scan must be `only` for scheduled changefeeds",
		},
		{
			name:  "explicit-conflict-with-initial-scan-only",
			stmt:  "CREATE SCHEDULE test_schedule FOR CHANGEFEED t1 INTO 'null://sink' WITH initial_scan='no', initial_scan_only RECURRING '@daily';",
			errRE: "cannot specify both initial_scan and initial_scan_only",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			hint := tc.hintStr
			if hint == "" {
				// Assert there is no hint when there shouldn't be by passing the regex for an empty string.
				hint = "^$"
			}
			th.sqlDB.ExpectErrWithHint(t, tc.errRE, tc.hintStr, tc.stmt)
		})
	}
}

func extractChangefeedNode(sj *jobs.ScheduledJob) (*tree.CreateChangefeed, error) {
	args := &changefeedpb.ScheduledChangefeedExecutionArgs{}
	if err := pbtypes.UnmarshalAny(sj.ExecutionArgs().Args, args); err != nil {
		return nil, errors.Wrap(err, "un-marshaling args")
	}

	node, err := parser.ParseOne(args.ChangefeedStatement)
	if err != nil {
		return nil, errors.Wrap(err, "parsing changefeed statement")
	}

	if changefeedStmt, ok := node.AST.(*tree.CreateChangefeed); ok {
		return changefeedStmt, nil
	}

	return nil, errors.Newf("unexpect node type %T", node)
}

func constructExpectedScheduledChangefeedNode(
	t *testing.T, sj *jobs.ScheduledJob, recurrence string,
) (*tree.ScheduledChangefeed, error) {
	t.Helper()

	changefeedNode, err := extractChangefeedNode(sj)
	require.NoError(t, err)

	wait, err := schedulebase.ParseOnPreviousRunningOption(sj.ScheduleDetails().Wait)
	if err != nil {
		return nil, err
	}

	onError, err := schedulebase.ParseOnErrorOption(sj.ScheduleDetails().OnError)
	if err != nil {
		return nil, err
	}

	scheduleOptions := tree.KVOptions{
		tree.KVOption{
			Key:   optOnExecFailure,
			Value: tree.NewDString(onError),
		},
		tree.KVOption{
			Key:   optOnPreviousRunning,
			Value: tree.NewDString(wait),
		},
	}

	return &tree.ScheduledChangefeed{
		CreateChangefeed: changefeedNode,
		ScheduleLabelSpec: tree.LabelSpec{
			IfNotExists: false,
			Label:       tree.NewDString(sj.ScheduleLabel()),
		},
		Recurrence:      tree.NewDString(recurrence),
		ScheduleOptions: scheduleOptions,
	}, nil

}

func TestShowCreateScheduleStatement(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	th, cleanup := newTestHelper(t)
	defer cleanup()

	th.sqlDB.Exec(t, "CREATE TABLE t1 (a INT)")

	testCases := []struct {
		name       string
		query      string
		recurrence string
	}{
		{
			name:       "changefeed",
			query:      `CREATE SCHEDULE simple FOR CHANGEFEED TABLE t1 INTO '%s' WITH initial_scan = 'only' RECURRING '@hourly'`,
			recurrence: "@hourly",
		},
		{
			name:       "changefeed-with-options",
			query:      `CREATE SCHEDULE with_opts FOR CHANGEFEED TABLE t1 INTO '%s' WITH format='json', initial_scan = 'only' RECURRING '@hourly' WITH SCHEDULE OPTIONS on_execution_failure = 'pause'`,
			recurrence: "@hourly",
		},
		{
			name: "changefeed-expressions",
			query: `
		CREATE SCHEDULE 'foo-changefeed'
		FOR CHANGEFEED INTO '%s'
		WITH format='JSON', initial_scan = 'only', schema_change_policy='stop'
		AS SELECT * FROM t1
		RECURRING '@hourly' WITH SCHEDULE OPTIONS on_execution_failure = 'pause'`,
			recurrence: "@hourly",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			destination := "webhook-https://0/" + tc.name
			createScheduleQuery := fmt.Sprintf(tc.query, destination)
			schedule, err := th.createChangefeedSchedule(t, createScheduleQuery)
			require.NoError(t, err)

			expectedScheduleNode, err := constructExpectedScheduledChangefeedNode(t, schedule, tc.recurrence)
			require.NoError(t, err)

			t.Run("show-create-all-schedules", func(t *testing.T) {
				rows := th.sqlDB.QueryStr(t, "SELECT * FROM [ SHOW CREATE ALL SCHEDULES ] WHERE create_statement LIKE '%FOR CHANGEFEED%'")
				cols, err := th.sqlDB.Query(t, "SHOW CREATE ALL SCHEDULES").Columns()
				require.NoError(t, err)
				// The number of rows returned should be equal to the number of
				// schedules created
				require.Equal(t, 1, len(rows), rows)
				require.Equal(t, cols, []string{"schedule_id", "create_statement"})

				for _, row := range rows {
					// Ensure that each row has schedule_id, create_stmt.
					require.Len(t, row, 2)
					showCreateScheduleStmt := row[1]
					require.Equal(t, tree.AsString(expectedScheduleNode), showCreateScheduleStmt)
				}
			})

			t.Run("show-create-schedule-by-id", func(t *testing.T) {
				rows := th.sqlDB.QueryStr(t, fmt.Sprintf("SHOW CREATE SCHEDULE %d", schedule.ScheduleID()))
				require.Equal(t, 1, len(rows))
				cols, err := th.sqlDB.Query(t, fmt.Sprintf("SHOW CREATE SCHEDULE %d", schedule.ScheduleID())).Columns()
				require.NoError(t, err)
				require.Equal(t, cols, []string{"schedule_id", "create_statement"})
				require.Equal(t, tree.AsString(expectedScheduleNode), rows[0][1])
			})

			th.clearSchedules(t)
		})
	}
}

func TestCheckScheduleAlreadyExists(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	th, cleanup := newTestHelper(t)
	defer cleanup()

	th.sqlDB.Exec(t, `CREATE TABLE demo (id int)`)
	th.sqlDB.Exec(t, `CREATE SCHEDULE simple FOR CHANGEFEED TABLE demo INTO 'webhook-https://0/temp' WITH initial_scan = 'only' RECURRING '@hourly'`)
	execCfg := th.server.ExecutorConfig().(sql.ExecutorConfig)

	ctx := context.Background()

	sd := sql.NewInternalSessionData(ctx, execCfg.Settings, "test")
	sd.Database = "d"
	p, cleanup := sql.NewInternalPlanner("test",
		execCfg.DB.NewTxn(ctx, "test-planner"),
		username.NodeUserName(), &sql.MemoryMetrics{}, &execCfg,
		sd,
	)
	defer cleanup()

	present, err := schedulebase.CheckScheduleAlreadyExists(ctx, p.(sql.PlanHookState), "simple")
	require.NoError(t, err)
	require.Equal(t, present, true)

	present, err = schedulebase.CheckScheduleAlreadyExists(ctx, p.(sql.PlanHookState), "not-existing")
	require.NoError(t, err)
	require.Equal(t, present, false)
}

func TestFullyQualifyTables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	th, cleanup := newTestHelper(t)
	defer cleanup()

	ctx := context.Background()
	execCfg := th.server.ExecutorConfig().(sql.ExecutorConfig)

	th.sqlDB.Exec(t, "CREATE DATABASE ocean;")
	th.sqlDB.Exec(t, "USE ocean;")
	th.sqlDB.Exec(t, "create table islands (id int);")
	stmt, err := parser.ParseOne(`create changefeed for islands;`)
	require.NoError(t, err)
	createChangeFeedStmt := stmt.AST.(*tree.CreateChangefeed)

	sd := sql.NewInternalSessionData(ctx, execCfg.Settings, "test")
	sd.Database = "ocean"
	p, cleanupPlanHook := sql.NewInternalPlanner("test",
		execCfg.DB.NewTxn(ctx, "test-planner"),
		username.NodeUserName(), &sql.MemoryMetrics{}, &execCfg,
		sd,
	)
	defer cleanupPlanHook()

	tablePatterns := make([]tree.TablePattern, 0)
	for _, target := range createChangeFeedStmt.Targets {
		tablePatterns = append(tablePatterns, target.TableName)
	}

	qualifiedPatterns, err := schedulebase.FullyQualifyTables(ctx, p.(sql.PlanHookState), tablePatterns)
	require.NoError(t, err)
	require.Equal(t, "[ocean.public.islands]", fmt.Sprint(qualifiedPatterns))
}
