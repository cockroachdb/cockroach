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
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedpb"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobstest"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs/schedulebase"
	"github.com/cockroachdb/cockroach/pkg/security/username"
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
	sqlDB            *sqlutils.SQLRunner
	server           serverutils.TestServerInterface
	executeSchedules func() error
	createdSchedules []int64
}

func newTestHelper(t *testing.T) (*testHelper, func()) {
	sh := &testHelper{
		env: jobstest.NewJobSchedulerTestEnv(
			jobstest.UseSystemTables, timeutil.Now(), tree.ScheduledChangefeedExecutor),
	}

	s, db, stopServer := startTestFullServer(t, makeOptions(withSchedulerHelper(sh)))
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
					//return sh.cfg.DB.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
					//	return fn(ctx, allSchedules)
					//})
				}
			}
			jobsKnobs.CaptureJobExecutionConfig = func(config *scheduledjobs.JobExecutionConfig) {
				sh.cfg = config
			}
			// Revisit: we no longer have support for asof. so we can remove this. we no longer need
			// this. need confirmation.

			// We'll be manipulating schedule time via th.env, but we can't fool actual changefeed
			// when it comes to AsOf time.  So, override AsOf changefeed clause to be the current time.
			//jobsKnobs.OverrideAsOfClause = func(clause *tree.AsOfClause) {
			//	expr, err := tree.MakeDTimestampTZ(sh.cfg.DB.Clock().PhysicalTime(), time.Microsecond)
			//	if err != nil {
			//		panic("unexpected failure to parse physical time.")
			//	}
			//	clause.Expr = expr
			//}
		}
	}
}

func (h *testHelper) clearSchedules(t *testing.T) {
	t.Helper()

	var sb strings.Builder
	sep := ""
	for _, id := range h.createdSchedules {
		sb.WriteString(sep)
		sb.WriteString(strconv.FormatInt(id, 10))
		sep = ", "
	}
	h.sqlDB.Exec(t, fmt.Sprintf("DELETE FROM %s WHERE schedule_id IN (%s)",
		h.env.ScheduledJobsTableName(), sb.String()))
}

func (h *testHelper) waitForSuccessfulScheduledJob(t *testing.T, scheduleID int64) int64 {
	query := "SELECT id FROM " + h.env.SystemJobsTableName() +
		" WHERE status=$1 AND created_by_type=$2 AND created_by_id=$3"
	var jobID int64

	testutils.SucceedsSoon(t, func() error {
		// Force newly created job to be adopted and verify it succeeds.
		h.server.JobRegistry().(*jobs.Registry).TestingNudgeAdoptionQueue()

		return h.sqlDB.DB.QueryRowContext(context.Background(),
			query, jobs.StatusSucceeded, jobs.CreatedByScheduledJobs, scheduleID).Scan(&jobID)
	})

	return jobID
}

// createChangefeedSchedule executes specified "CREATE SCHEDULE FOR CHANGEFEED" query, with
// the provided arguments.  Returns created schedule.
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
	datums, cols, err := h.cfg.InternalExecutor.QueryRowExWithCols(
		context.Background(), "sched-load", nil,
		sessiondata.InternalExecutorOverride{User: username.RootUserName()},
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
	return scheduledChangefeed.ScheduledChangefeedStatement
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
				changefeedStmt: "CREATE CHANGEFEED FOR TABLE d.public.foo INTO 'webhook-https://0/changefeed?AWS_SECRET_ACCESS_KEY=nevershown' WITH initial_scan = 'only'",
				shownStmt:      "CREATE CHANGEFEED FOR TABLE d.public.foo INTO 'webhook-https://0/changefeed?AWS_SECRET_ACCESS_KEY=redacted' WITH initial_scan = 'only'",
				period:         time.Hour,
			},
		},
		{
			name:  "changefeed-qualifies-names",
			query: "CREATE SCHEDULE FOR CHANGEFEED foo INTO 'webhook-https://0/changefeed' WITH initial_scan = 'only' RECURRING '@hourly'",
			es: expectedSchedule{
				nameRe:         "CHANGEFEED .+",
				changefeedStmt: "CREATE CHANGEFEED FOR TABLE d.public.foo INTO 'webhook-https://0/changefeed' WITH initial_scan = 'only'",
				period:         time.Hour,
			},
		},
		{
			name: "changefeed-with-options",
			query: `
CREATE SCHEDULE 'foo-changefeed' 
FOR CHANGEFEED  foo
INTO 'webhook-https://0/changefeed?AWS_SECRET_ACCESS_KEY=nevershown'
WITH format='JSON', initial_scan = 'only'
RECURRING '@hourly' WITH SCHEDULE OPTIONS on_execution_failure = 'pause', first_run=$1
`,
			queryArgs: []interface{}{th.env.Now()},
			es: expectedSchedule{
				nameRe: "foo-changefeed",
				// AS OF TIME clause ignored when creating changefeed schedules, but a notice warning
				// is produced.
				changefeedStmt: "CREATE CHANGEFEED FOR TABLE d.public.foo INTO 'webhook-https://0/changefeed?AWS_SECRET_ACCESS_KEY=nevershown' WITH format = 'JSON', initial_scan = 'only'",
				shownStmt:      "CREATE CHANGEFEED FOR TABLE d.public.foo INTO 'webhook-https://0/changefeed?AWS_SECRET_ACCESS_KEY=redacted' WITH format = 'JSON', initial_scan = 'only'",
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
				changefeedStmt: "CREATE CHANGEFEED INTO 'webhook-https://0/changefeed?AWS_SECRET_ACCESS_KEY=nevershown' WITH format = 'JSON', initial_scan = 'only', schema_change_policy = 'stop' AS SELECT * FROM d.public.foo",
				shownStmt:      "CREATE CHANGEFEED INTO 'webhook-https://0/changefeed?AWS_SECRET_ACCESS_KEY=redacted' WITH format = 'JSON', initial_scan = 'only', schema_change_policy = 'stop' AS SELECT * FROM d.public.foo",
				period:         time.Hour,
				runsNow:        true,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf(tc.name), func(t *testing.T) {
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
			th.sqlDB.QueryRow(t, `SELECT command->>'scheduled_changefeed_statement' FROM [SHOW SCHEDULE $1]`,
				sj.ScheduleID()).Scan(&shownStmt)

			if expectedSchedule.shownStmt != "" {
				require.Equal(t, expectedSchedule.shownStmt, shownStmt)
			} else {
				require.Equal(t, expectedSchedule.changefeedStmt, shownStmt)
			}
		})
	}
}

func TestCreateChangefeedScheduleRequiresAdminRole(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	th, cleanup := newTestHelper(t)
	defer cleanup()

	th.sqlDB.Exec(t, `CREATE USER testuser`)
	pgURL, cleanupFunc := sqlutils.PGUrl(
		t, th.server.ServingSQLAddr(),
		"TestCreateSchedule-testuser", url.User("testuser"),
	)
	defer cleanupFunc()

	testuser, err := gosql.Open("postgres", pgURL.String())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, testuser.Close())
	}()

	_, err = testuser.Exec(
		"CREATE SCHEDULE FOR CHANGEFEED TABLE system.jobs INTO 'somewhere' WITH initial_scan = 'only' RECURRING '@daily'")
	require.Regexp(t, "only users with the admin role", err)
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
	const createQuery = "CREATE SCHEDULE IF NOT EXISTS '%s' FOR CHANGEFEED TABLE t1 INTO 's3://bucket?AUTH=implicit' WITH initial_scan = 'only' RECURRING '@daily'"

	th.sqlDB.Exec(t, fmt.Sprintf(createQuery, scheduleLabel))

	// no op expected
	th.sqlDB.Exec(t, fmt.Sprintf(createQuery, scheduleLabel))

	const selectQuery = "SELECT label FROM [SHOW SCHEDULES FOR CHANGEFEED]"

	rows, err := th.cfg.InternalExecutor.QueryBufferedEx(
		context.Background(), "check-sched", nil,
		sessiondata.InternalExecutorOverride{User: username.RootUserName()},
		selectQuery)

	require.NoError(t, err)
	require.Equal(t, 1, len(rows))

	// the 'bar' schedule should get scheduled
	const newScheduleLabel = "bar"

	th.sqlDB.Exec(t, fmt.Sprintf(createQuery, newScheduleLabel))

	rows, err = th.cfg.InternalExecutor.QueryBufferedEx(
		context.Background(), "check-sched2", nil,
		sessiondata.InternalExecutorOverride{User: username.RootUserName()},
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
	th.sqlDB.Exec(t, "CREATE SCHEDULE FOR CHANGEFEED TABLE t1 INTO 's3://bucket?AUTH=implicit' WITH initial_scan = 'only' RECURRING '@daily';")
	th.sqlDB.Exec(t, "ROLLBACK;")

	res = th.sqlDB.Query(t, "SELECT id FROM [SHOW SCHEDULES FOR CHANGEFEED]")
	require.False(t, res.Next())
	require.NoError(t, res.Err())
}

// webhookFeedForSchedules is present so that Next function can be overloaded
// for the purposes of testing scheduled changefeeds.
type webhookFeedForSchedules struct {
	*webhookFeed
}

// Next implements TestFeed interface. Its overloaded here for webhookFeed Next function, specifically for testing
// scheduled changefeeds.
func (f *webhookFeedForSchedules) Next() (*cdctest.TestFeedMessage, error) {
	for {
		msg := f.mockSink.Pop()
		if msg != "" {
			m := &cdctest.TestFeedMessage{}
			if msg != "" {
				resolved, err := isResolvedTimestamp([]byte(msg))
				if err != nil {
					return nil, err
				}
				if resolved {
					m.Resolved = []byte(msg)
				} else {
					wrappedValue, err := extractValueFromJSONMessage([]byte(msg))
					if err != nil {
						return nil, err
					}
					if m.Key, m.Value, err = extractKeyFromJSONValue(f.isBare, wrappedValue); err != nil {
						return nil, err
					}
					if m.Topic, m.Value, err = extractTopicFromJSONValue(f.isBare, m.Value); err != nil {
						return nil, err
					}
					if isNew := f.markSeen(m); !isNew {
						continue
					}
				}
				return m, nil
			}
			m.Key, m.Value = nil, nil
			return m, nil
		}
		return nil, errors.Errorf("Sink does not have any more messages")
	}
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

	getFeed := func(isBare bool) (string, cdctest.TestFeed, func()) {
		cert, _, err := cdctest.NewCACertBase64Encoded()
		require.NoError(t, err)
		sinkDest, err := cdctest.StartMockWebhookSink(cert)
		require.NoError(t, err)

		// NB: This is a partially initialized webhookFeed.
		// You must call th.waitForSuccessfulScheduledJob prior to reading
		// feed messages. If messages are tried to access before they are present,
		// this feed will error out.
		// This feed has support only for json format.
		feed := &webhookFeedForSchedules{
			&webhookFeed{
				seenTrackerMap: make(map[string]struct{}),
				mockSink:       sinkDest,
				isBare:         isBare,
			},
		}

		sinkURI := fmt.Sprintf("webhook-%s?insecure_tls_skip_verify=true", sinkDest.URL())
		return sinkURI, feed, sinkDest.Close
	}

	testCases := []struct {
		name            string
		scheduleStmt    string
		expectedPayload []string
		isBare          bool
	}{
		{
			name:         "one-table",
			scheduleStmt: "CREATE SCHEDULE FOR changefeed TABLE t1 INTO $1 with initial_scan='only' RECURRING '@hourly'",
			expectedPayload: []string{
				`t1: [1]->{"after": {"a": 1, "b": "one"}}`,
				`t1: [10]->{"after": {"a": 10, "b": "ten"}}`,
				`t1: [100]->{"after": {"a": 100, "b": "hundred"}}`,
			},
			isBare: false,
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
			isBare: false,
		},
		{
			name: "changefeed-expressions",
			// key_in_value is only need here to make the webhookFeed work.
			scheduleStmt: "CREATE SCHEDULE FOR CHANGEFEED INTO $1 WITH key_in_value, initial_scan='only', schema_change_policy='stop' AS SELECT b FROM t1 RECURRING '@hourly'",
			expectedPayload: []string{
				`t1: [1]->{"b": "one"}`,
				`t1: [10]->{"b": "ten"}`,
				`t1: [100]->{"b": "hundred"}`,
			},
			isBare: true,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			sinkURI, feed, cleanup := getFeed(test.isBare)
			defer cleanup()

			sj, err := th.createChangefeedSchedule(
				t, test.scheduleStmt, sinkURI)
			require.NoError(t, err)
			defer th.clearSchedules(t)

			// Force the schedule to execute.
			th.env.SetTime(sj.NextRun().Add(time.Second))
			require.NoError(t, th.executeSchedules())

			th.waitForSuccessfulScheduledJob(t, sj.ScheduleID())
			assertPayloads(t, feed, test.expectedPayload)
		})
	}

}

func extractChangefeedNode(sj *jobs.ScheduledJob) (*tree.CreateChangefeed, error) {
	args := &changefeedpb.ScheduledChangefeedExecutionArgs{}
	if err := pbtypes.UnmarshalAny(sj.ExecutionArgs().Args, args); err != nil {
		return nil, errors.Wrap(err, "un-marshaling args")
	}

	node, err := parser.ParseOne(args.ScheduledChangefeedStatement)
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

	firstRunTime := sj.ScheduledRunTime()
	firstRun, err := tree.MakeDTimestampTZ(firstRunTime, time.Microsecond)
	if err != nil {
		return nil, err
	}

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
		tree.KVOption{
			Key:   optFirstRun,
			Value: firstRun,
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
				// The number of rows returned should be equal to the number of schedules created
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
