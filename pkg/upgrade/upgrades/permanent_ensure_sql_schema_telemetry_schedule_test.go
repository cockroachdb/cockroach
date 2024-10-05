// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades_test

import (
	"context"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobstest"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schematelemetry/schematelemetrycontroller"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/builtinconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

func TestSchemaTelemetrySchedule(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// We want to ensure that the migration will succeed when run again.
	// To ensure that it will, we inject a failure when trying to mark
	// the upgrade as complete when forceRetry is true.
	testutils.RunTrueAndFalse(t, "force-retry", func(t *testing.T, forceRetry bool) {
		defer log.Scope(t).Close(t)

		ctx := context.Background()
		var args base.TestServerArgs
		var injectedFailure syncutil.AtomicBool
		// The statement which writes the completion of the migration will
		// match the below regexp.
		completeRegexp := regexp.MustCompile(`INSERT\s+INTO\s+system.migrations`)
		jobKnobs := jobs.NewTestingKnobsWithShortIntervals()
		jobKnobs.JobSchedulerEnv = jobstest.NewJobSchedulerTestEnv(
			jobstest.UseSystemTables,
			timeutil.Now(),
			tree.ScheduledSchemaTelemetryExecutor,
		)
		args.Knobs.JobsTestingKnobs = jobKnobs
		args.Knobs.SQLExecutor = &sql.ExecutorTestingKnobs{
			BeforePrepare: func(ctx context.Context, stmt string, txn *kv.Txn) error {
				if forceRetry && !injectedFailure.Get() && completeRegexp.MatchString(stmt) {
					injectedFailure.Set(true)
					return errors.New("boom")
				}
				return nil
			},
		}
		aostDuration := time.Nanosecond
		args.Knobs.SchemaTelemetry = &sql.SchemaTelemetryTestingKnobs{
			AOSTDuration: &aostDuration,
		}

		tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{ServerArgs: args})
		defer tc.Stopper().Stop(ctx)
		tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))

		qExists := fmt.Sprintf(`
    SELECT recurrence, count(*)
      FROM [SHOW SCHEDULES]
      WHERE label = '%s'
      GROUP BY recurrence`,
			schematelemetrycontroller.SchemaTelemetryScheduleName)

		qJob := fmt.Sprintf(`SELECT %s()`,
			builtinconstants.CreateSchemaTelemetryJobBuiltinName)

		clusterID := tc.Server(0).ExecutorConfig().(sql.ExecutorConfig).NodeInfo.
			LogicalClusterID()

		// Check that the schedule exists and that jobs can be created.
		tdb.Exec(t, qJob)
		exp := scheduledjobs.MaybeRewriteCronExpr(clusterID, "@weekly")
		tdb.CheckQueryResultsRetry(t, qExists, [][]string{{exp, "1"}})

		// Check that the schedule can have its recurrence altered.
		tdb.Exec(t, fmt.Sprintf(`SET CLUSTER SETTING %s = '* * * * *'`,
			schematelemetrycontroller.SchemaTelemetryRecurrence.Name()))
		tdb.CheckQueryResultsRetry(t, qExists, [][]string{{"* * * * *", "1"}})
		exp = scheduledjobs.MaybeRewriteCronExpr(clusterID, "@daily")
		tdb.Exec(t, fmt.Sprintf(`SET CLUSTER SETTING %s = '@daily'`,
			schematelemetrycontroller.SchemaTelemetryRecurrence.Name()))
		tdb.CheckQueryResultsRetry(t, qExists, [][]string{{exp, "1"}})
	})

}
