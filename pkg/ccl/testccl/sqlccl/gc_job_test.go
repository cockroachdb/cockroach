// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package sqlccl

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
)

// TestGCJobGetsMarkedIdle is an integration test of sorts to ensure that a
// gc job which is waiting for its data to expire is marked idle in the job
// registry.
func TestGCJobGetsMarkedIdle(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	params, _ := tests.CreateTestServerParams()
	s, mainDB, _ := serverutils.StartServer(t, params)
	sqltestutils.SetShortRangeFeedIntervals(t, mainDB)
	defer s.Stopper().Stop(ctx)
	tenant, tenantDB := serverutils.StartTenant(t, s, tests.CreateTestTenantParams(serverutils.TestTenantID()))
	defer tenant.Stopper().Stop(ctx)
	defer tenantDB.Close()

	sqlutils.MakeSQLRunner(mainDB).Exec(t,
		"ALTER TENANT ALL SET CLUSTER SETTING sql.gc_job.idle_wait_duration = '10ms'",
	)

	tdb := sqlutils.MakeSQLRunner(tenantDB)
	tdb.CheckQueryResultsRetry(t,
		"SHOW CLUSTER SETTING sql.gc_job.idle_wait_duration",
		[][]string{{"00:00:00.01"}},
	)
	tdb.Exec(t, "CREATE TABLE foo (i INT PRIMARY KEY)")
	tdb.Exec(t, "DROP TABLE foo")
	jrm := tenant.JobRegistry().(*jobs.Registry).MetricsStruct()
	testutils.SucceedsSoon(t, func() error {
		gcJobMetrics := jrm.JobMetrics[jobspb.TypeSchemaChangeGC]
		if got := gcJobMetrics.CurrentlyRunning.Value(); got != 1 {
			return errors.Errorf("expected 1 running gc-job, got %d", got)
		}
		if got := gcJobMetrics.CurrentlyIdle.Value(); got != 1 {
			return errors.Errorf("expected 1 idle gc-job, got %d", got)
		}
		if got := jrm.RunningNonIdleJobs.Value(); got > 0 {
			return errors.Errorf("expected 0 non-idle jobs, got %d", got)
		}
		return nil
	})
}
