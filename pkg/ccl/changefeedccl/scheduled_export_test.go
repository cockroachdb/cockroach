// Copyright 2021 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs/schedulebase"
	"github.com/cockroachdb/cockroach/pkg/security"
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
type execSchedulesFn = func(ctx context.Context, maxSchedules int64, txn *kv.Txn) error
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
			jobstest.UseSystemTables, timeutil.Now(), tree.ScheduledExportExecutor),
	}

	s, db, stopServer := startTestServer(t, makeOptions(withSchedulerHelper(sh)))
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
					return sh.cfg.DB.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
						return fn(ctx, allSchedules, txn)
					})
				}
			}
			jobsKnobs.CaptureJobExecutionConfig = func(config *scheduledjobs.JobExecutionConfig) {
				sh.cfg = config
			}
			// We'll be manipulating schedule time via th.env, but we can't fool actual export
			// when it comes to AsOf time.  So, override AsOf export clause to be the current time.
			jobsKnobs.OverrideAsOfClause = func(clause *tree.AsOfClause) {
				expr, err := tree.MakeDTimestampTZ(sh.cfg.DB.Clock().PhysicalTime(), time.Microsecond)
				if err != nil {
					panic("unexpected failure to parse physical time.")
				}
				clause.Expr = expr
			}
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

func (h *testHelper) waitForSuccessfulScheduledJob(t *testing.T, scheduleID int64) {
	query := "SELECT id FROM " + h.env.SystemJobsTableName() +
		" WHERE status=$1 AND created_by_type=$2 AND created_by_id=$3"

	testutils.SucceedsSoon(t, func() error {
		// Force newly created job to be adopted and verify it succeeds.
		h.server.JobRegistry().(*jobs.Registry).TestingNudgeAdoptionQueue()
		var unused int64
		return h.sqlDB.DB.QueryRowContext(context.Background(),
			query, jobs.StatusSucceeded, jobs.CreatedByScheduledJobs, scheduleID).Scan(&unused)
	})
}

// createExportSchedule executes specified "CREATE SCHEDULE FOR EXPORT" query, with
// the provided arguments.  Returns created schedule.
func (h *testHelper) createExportSchedule(
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
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
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

func getScheduledExportStatement(t *testing.T, arg *jobspb.ExecutionArguments) string {
	var export changefeedpb.ScheduledExportExecutionArgs
	require.NoError(t, pbtypes.UnmarshalAny(arg.Args, &export))
	return export.ExportStatement
}

// This test examines serialized representation of export schedule arguments
// when the scheduled export statement executes.  This test does not concern
// itself with the actual scheduling and the execution of those exports.
func TestSerializesScheduledExportExecutionArgs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	th, cleanup := newTestHelper(t)
	defer cleanup()

	th.sqlDB.Exec(t, `CREATE TABLE foo (a INT, b STRING)`)

	type expectedSchedule struct {
		nameRe     string
		exportStmt string
		period     time.Duration
		runsNow    bool
		shownStmt  string
	}

	testCases := []struct {
		name      string
		query     string
		queryArgs []interface{}
		es        expectedSchedule
		errMsg    string
	}{
		{
			name:  "export",
			query: "CREATE SCHEDULE FOR EXPORT TABLE d.public.foo INTO 'webhook-https://0/export?AWS_SECRET_ACCESS_KEY=nevershown' RECURRING '@hourly'",
			es: expectedSchedule{
				nameRe:     "EXPORT .+",
				exportStmt: "CREATE EXPORT CHANGEFEED FOR TABLE d.public.foo INTO 'webhook-https://0/export?AWS_SECRET_ACCESS_KEY=nevershown'",
				shownStmt:  "CREATE EXPORT CHANGEFEED FOR TABLE d.public.foo INTO 'webhook-https://0/export?AWS_SECRET_ACCESS_KEY=redacted'",
				period:     time.Hour,
			},
		},
		{
			name:  "export-qualifies-names",
			query: "CREATE SCHEDULE FOR EXPORT TABLE foo INTO 'webhook-https://0/export' RECURRING '@hourly'",
			es: expectedSchedule{
				nameRe:     "EXPORT .+",
				exportStmt: "CREATE EXPORT CHANGEFEED FOR TABLE d.public.foo INTO 'webhook-https://0/export'",
				period:     time.Hour,
			},
		},
		{
			name: "export-with-options",
			query: `
CREATE SCHEDULE 'foo-export' 
FOR EXPORT TABLE foo AS OF SYSTEM TIME 'sometime' 
INTO 'webhook-https://0/export?AWS_SECRET_ACCESS_KEY=nevershown'
WITH format='JSON'
RECURRING '@hourly' WITH SCHEDULE OPTIONS on_execution_failure = 'pause', first_run=$1
`,
			queryArgs: []interface{}{th.env.Now()},
			es: expectedSchedule{
				nameRe: "foo-export",
				// AS OF TIME clause ignored when creating export schedules, but a notice warning
				// is produced.
				exportStmt: "CREATE EXPORT CHANGEFEED FOR TABLE d.public.foo INTO 'webhook-https://0/export?AWS_SECRET_ACCESS_KEY=nevershown' WITH format = 'JSON'",
				shownStmt:  "CREATE EXPORT CHANGEFEED FOR TABLE d.public.foo INTO 'webhook-https://0/export?AWS_SECRET_ACCESS_KEY=redacted' WITH format = 'JSON'",
				period:     time.Hour,
				runsNow:    true,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf(tc.name), func(t *testing.T) {
			defer th.clearSchedules(t)

			sj, err := th.createExportSchedule(t, tc.query, tc.queryArgs...)
			if len(tc.errMsg) > 0 {
				require.True(t, testutils.IsError(err, tc.errMsg),
					"expected error to match %q, found %q instead", tc.errMsg, err.Error())
				return
			}

			require.NoError(t, err)

			expectedSchedule := tc.es
			exportStmt := getScheduledExportStatement(t, sj.ExecutionArgs())
			require.Equal(t, expectedSchedule.exportStmt, exportStmt)

			frequency, err := sj.Frequency()
			require.NoError(t, err)
			require.EqualValues(t, expectedSchedule.period, frequency, expectedSchedule)

			if expectedSchedule.runsNow {
				require.EqualValues(t, th.env.Now().Round(time.Microsecond), sj.ScheduledRunTime())
			}

			var shownStmt string
			th.sqlDB.QueryRow(t, `SELECT command->>'export_statement' FROM [SHOW SCHEDULE $1]`,
				sj.ScheduleID()).Scan(&shownStmt)

			if expectedSchedule.shownStmt != "" {
				require.Equal(t, expectedSchedule.shownStmt, shownStmt)
			} else {
				require.Equal(t, expectedSchedule.exportStmt, shownStmt)
			}
		})
	}
}

func TestScheduledExport(t *testing.T) {
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

	getFeed := func() (string, cdctest.TestFeed, func()) {
		cert, _, err := cdctest.NewCACertBase64Encoded()
		require.NoError(t, err)
		sinkDest, err := cdctest.StartMockWebhookSink(cert)
		require.NoError(t, err)

		// NB: This is a partially initialized webhookFeed.  It doesn't have feed
		// synchronizer (so it would panic if it ever needs to wait for a message).
		// You must call th.waitForSuccessfulScheduledJob prior to reading
		// feed messages.
		feed := &webhookFeed{
			seenTrackerMap: make(map[string]struct{}),
			mockSink:       sinkDest,
		}
		return sinkDest.TrustedEndpoint(), feed, sinkDest.Close
	}

	t.Run("one-table", func(t *testing.T) {
		sinkURI, feed, cleanup := getFeed()
		defer cleanup()

		sj, err := th.createExportSchedule(
			t, "CREATE SCHEDULE FOR EXPORT TABLE t1 INTO $1 RECURRING '@hourly'", sinkURI)
		require.NoError(t, err)
		defer th.clearSchedules(t)

		// Force the schedule to execute.
		th.env.SetTime(sj.NextRun().Add(time.Second))
		require.NoError(t, th.executeSchedules())

		th.waitForSuccessfulScheduledJob(t, sj.ScheduleID())
		assertPayloads(t, feed, []string{
			`t1: [1]->{"after": {"a": 1, "b": "one"}}`,
			`t1: [10]->{"after": {"a": 10, "b": "ten"}}`,
			`t1: [100]->{"after": {"a": 100, "b": "hundred"}}`,
		})
	})

	t.Run("two-tables", func(t *testing.T) {
		sinkURI, feed, cleanup := getFeed()
		defer cleanup()

		sj, err := th.createExportSchedule(
			t, "CREATE SCHEDULE FOR EXPORT TABLE t1, t2 INTO $1 RECURRING '@hourly'", sinkURI)
		require.NoError(t, err)
		defer th.clearSchedules(t)

		// Force the schedule to execute.
		th.env.SetTime(sj.NextRun().Add(time.Second))
		require.NoError(t, th.executeSchedules())

		th.waitForSuccessfulScheduledJob(t, sj.ScheduleID())
		assertPayloads(t, feed, []string{
			`t1: [1]->{"after": {"a": 1, "b": "one"}}`,
			`t1: [10]->{"after": {"a": 10, "b": "ten"}}`,
			`t1: [100]->{"after": {"a": 100, "b": "hundred"}}`,
			`t2: [3]->{"after": {"b": 3, "c": "three"}}`,
			`t2: [2]->{"after": {"b": 2, "c": "two"}}`,
			`t2: [1]->{"after": {"b": 1, "c": "one"}}`,
		})
	})
}

func TestCreateExportScheduleRequiresAdminRole(t *testing.T) {
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
		"CREATE SCHEDULE FOR EXPORT TABLE system.jobs INTO 'somewhere' RECURRING '@daily'")
	require.Regexp(t, "only users with the admin role", err)
}

// TestCreateExportScheduleIfNotExists: checks if adding IF NOT EXISTS will
// create the schedule only if the schedule label doesn't already exist.
func TestCreateExportScheduleIfNotExists(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	th, cleanup := newTestHelper(t)
	defer cleanup()

	th.sqlDB.Exec(t, "CREATE TABLE t1 (a INT)")

	const scheduleLabel = "foo"
	const createQuery = "CREATE SCHEDULE IF NOT EXISTS '%s' FOR EXPORT TABLE t1 INTO 's3://bucket?AUTH=implicit' RECURRING '@daily'"

	th.sqlDB.Exec(t, fmt.Sprintf(createQuery, scheduleLabel))

	// no op expected
	th.sqlDB.Exec(t, fmt.Sprintf(createQuery, scheduleLabel))

	const selectQuery = "SELECT label FROM [SHOW SCHEDULES FOR EXPORT]"

	rows, err := th.cfg.InternalExecutor.QueryBufferedEx(
		context.Background(), "check-sched", nil,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		selectQuery)

	require.NoError(t, err)
	require.Equal(t, 1, len(rows))

	// the 'bar' schedule should get scheduled
	const newScheduleLabel = "bar"

	th.sqlDB.Exec(t, fmt.Sprintf(createQuery, newScheduleLabel))

	rows, err = th.cfg.InternalExecutor.QueryBufferedEx(
		context.Background(), "check-sched2", nil,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		selectQuery)

	require.NoError(t, err)
	require.Equal(t, 2, len(rows))
}

func TestCreateExportScheduleInExplicitTxnRollback(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	th, cleanup := newTestHelper(t)
	defer cleanup()

	th.sqlDB.Exec(t, "CREATE TABLE t1 (a INT)")
	res := th.sqlDB.Query(t, "SELECT id FROM [SHOW SCHEDULES FOR EXPORT]")
	require.False(t, res.Next())
	require.NoError(t, res.Err())

	th.sqlDB.Exec(t, "BEGIN;")
	th.sqlDB.Exec(t, "CREATE SCHEDULE FOR EXPORT TABLE t1 INTO 's3://bucket?AUTH=implicit' RECURRING '@daily';")
	th.sqlDB.Exec(t, "ROLLBACK;")

	res = th.sqlDB.Query(t, "SELECT id FROM [SHOW SCHEDULES FOR EXPORT]")
	require.False(t, res.Next())
	require.NoError(t, res.Err())
}

func extractExportNode(sj *jobs.ScheduledJob) (*tree.CreateChangefeed, error) {
	args := &changefeedpb.ScheduledExportExecutionArgs{}
	if err := pbtypes.UnmarshalAny(sj.ExecutionArgs().Args, args); err != nil {
		return nil, errors.Wrap(err, "un-marshaling args")
	}

	node, err := parser.ParseOne(args.ExportStatement)
	if err != nil {
		return nil, errors.Wrap(err, "parsing export statement")
	}

	if exportStmt, ok := node.AST.(*tree.CreateChangefeed); ok {
		return exportStmt, nil
	}

	return nil, errors.Newf("unexpect node type %T", node)
}

func constructExpectedScheduledExportNode(
	t *testing.T, sj *jobs.ScheduledJob, recurrence string,
) *tree.ScheduledExport {
	t.Helper()
	args := &changefeedpb.ScheduledExportExecutionArgs{}
	err := pbtypes.UnmarshalAny(sj.ExecutionArgs().Args, args)
	require.NoError(t, err)

	exportNode, err := extractExportNode(sj)
	require.NoError(t, err)

	opts := schedulebase.CommonScheduleOptions{
		FirstRun: sj.ScheduledRunTime(),
		OnError:  sj.ScheduleDetails().OnError,
		Wait:     sj.ScheduleDetails().Wait,
	}
	scheduleOptions, err := opts.KVOptions()
	require.NoError(t, err)

	return &tree.ScheduledExport{
		CreateChangefeed: *exportNode,
		ScheduleLabelSpec: tree.ScheduleLabelSpec{
			IfNotExists: false,
			Label:       tree.NewDString(sj.ScheduleLabel())},
		Recurrence:      tree.NewDString(recurrence),
		ScheduleOptions: scheduleOptions,
	}
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
			name:       "export",
			query:      `CREATE SCHEDULE simple FOR EXPORT TABLE t1 INTO '%s' RECURRING '@hourly'`,
			recurrence: "@hourly",
		},
		{
			name:       "export-with-options",
			query:      `CREATE SCHEDULE with_opts FOR EXPORT TABLE t1 INTO '%s' WITH format='json' RECURRING '@hourly' WITH SCHEDULE OPTIONS on_execution_failure = 'pause'`,
			recurrence: "@hourly",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			destination := "webhook-https://0/" + tc.name
			createScheduleQuery := fmt.Sprintf(tc.query, destination)
			schedule, err := th.createExportSchedule(t, createScheduleQuery)
			require.NoError(t, err)

			expectedScheduleNode := constructExpectedScheduledExportNode(t, schedule, tc.recurrence)

			t.Run("show-create-all-schedules", func(t *testing.T) {
				rows := th.sqlDB.QueryStr(t, "SELECT * FROM [ SHOW CREATE ALL SCHEDULES ] WHERE create_statement LIKE '%FOR EXPORT%'")
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
