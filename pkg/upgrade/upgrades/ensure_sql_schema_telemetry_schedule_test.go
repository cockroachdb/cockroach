// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package upgrades_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobstest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schematelemetry/schematelemetrycontroller"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/builtinconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func TestSchemaTelemetrySchedule(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var args base.TestServerArgs
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
	args.Knobs.Server = &server.TestingKnobs{
		DisableAutomaticVersionUpgrade: make(chan struct{}),
		BinaryVersionOverride:          clusterversion.ByKey(clusterversion.SQLSchemaTelemetryScheduledJobs - 1),
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

	// Check that there is no schema telemetry schedule and that creating schema
	// telemetry jobs is not possible.
	tdb.CheckQueryResults(t, qExists, [][]string{})
	tdb.ExpectErr(t, schematelemetrycontroller.ErrVersionGate.Error(), qJob)

	// Upgrade the cluster.
	tdb.Exec(t, `SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.SQLSchemaTelemetryScheduledJobs).String())

	// Check that the schedule now exists and that jobs can be created.
	tdb.Exec(t, qJob)
	tdb.CheckQueryResultsRetry(t, qExists, [][]string{{"@weekly", "1"}})

	// Check that the schedule can have its recurrence altered.
	tdb.Exec(t, fmt.Sprintf(`SET CLUSTER SETTING %s = '* * * * *'`,
		schematelemetrycontroller.SchemaTelemetryRecurrence.Key()))
	tdb.CheckQueryResultsRetry(t, qExists, [][]string{{"* * * * *", "1"}})
	tdb.Exec(t, fmt.Sprintf(`SET CLUSTER SETTING %s = '@daily'`,
		schematelemetrycontroller.SchemaTelemetryRecurrence.Key()))
	tdb.CheckQueryResultsRetry(t, qExists, [][]string{{"@daily", "1"}})

}
