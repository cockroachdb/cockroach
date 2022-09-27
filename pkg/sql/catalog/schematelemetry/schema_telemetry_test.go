// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schematelemetry_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobstest"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schematelemetry/schematelemetrycontroller"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/builtinconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func makeTestServerArgs() (args base.TestServerArgs) {
	args.Knobs.JobsTestingKnobs = &jobs.TestingKnobs{
		JobSchedulerEnv: jobstest.NewJobSchedulerTestEnv(
			jobstest.UseSystemTables,
			timeutil.Now(),
			tree.ScheduledSchemaTelemetryExecutor,
		),
	}
	aostDuration := time.Nanosecond
	args.Knobs.SchemaTelemetry = &sql.SchemaTelemetryTestingKnobs{
		AOSTDuration: &aostDuration,
	}
	return args
}

var (
	qExists = fmt.Sprintf(`
    SELECT recurrence, count(*)
      FROM [SHOW SCHEDULES]
      WHERE label = '%s'
        AND schedule_status = 'ACTIVE'
      GROUP BY recurrence`,
		schematelemetrycontroller.SchemaTelemetryScheduleName)

	qID = fmt.Sprintf(`
    SELECT id
      FROM [SHOW SCHEDULES]
      WHERE label = '%s'
        AND schedule_status = 'ACTIVE'`,
		schematelemetrycontroller.SchemaTelemetryScheduleName)

	qSet = fmt.Sprintf(`SET CLUSTER SETTING %s = '* * * * *'`,
		schematelemetrycontroller.SchemaTelemetryRecurrence.Key())

	qJob = fmt.Sprintf(`SELECT %s()`,
		builtinconstants.CreateSchemaTelemetryJobBuiltinName)
)

const qHasJob = `SELECT count(*) FROM crdb_internal.jobs WHERE job_type = 'AUTO SCHEMA TELEMETRY' AND status = 'succeeded'`

func TestSchemaTelemetrySchedule(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, makeTestServerArgs())
	defer s.Stopper().Stop(ctx)
	tdb := sqlutils.MakeSQLRunner(db)

	clusterID := s.ExecutorConfig().(sql.ExecutorConfig).NodeInfo.
		LogicalClusterID()
	exp := schematelemetrycontroller.MaybeRewriteCronExpr(clusterID, "@weekly")
	tdb.CheckQueryResultsRetry(t, qExists, [][]string{{exp, "1"}})
	tdb.ExecSucceedsSoon(t, qSet)
	tdb.CheckQueryResultsRetry(t, qExists, [][]string{{"* * * * *", "1"}})
}

func TestSchemaTelemetryJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, makeTestServerArgs())
	defer s.Stopper().Stop(ctx)
	tdb := sqlutils.MakeSQLRunner(db)
	tdb.Exec(t, `SET CLUSTER SETTING server.eventlog.enabled = true`)

	// Pause the existing schema telemetry schedule so that it doesn't interfere.
	res := tdb.QueryStr(t, qID)
	require.NotEmpty(t, res)
	require.NotEmpty(t, res[0])
	id := res[0][0]
	tdb.ExecSucceedsSoon(t, fmt.Sprintf("PAUSE SCHEDULE %s", id))
	tdb.CheckQueryResults(t, qHasJob, [][]string{{"0"}})
	// Run a schema telemetry job and wait for it to succeed.
	tdb.Exec(t, qJob)
	tdb.CheckQueryResultsRetry(t, qHasJob, [][]string{{"1"}})
}
