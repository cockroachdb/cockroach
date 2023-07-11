// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/impl" // register cloud storage providers
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprofiler"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprofiler/profilerconstants"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// fakeExecResumer calls optional callbacks during the job lifecycle.
type fakeExecResumer struct {
	OnResume     func(context.Context) error
	FailOrCancel func(context.Context) error
}

var _ jobs.Resumer = fakeExecResumer{}

func (d fakeExecResumer) Resume(ctx context.Context, execCtx interface{}) error {
	if d.OnResume != nil {
		if err := d.OnResume(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (d fakeExecResumer) OnFailOrCancel(ctx context.Context, _ interface{}, _ error) error {
	if d.FailOrCancel != nil {
		return d.FailOrCancel(ctx)
	}
	return nil
}

// checkForPlanDiagram is a method used in tests to wait for the existence of a
// DSP diagram for the provided jobID.
func checkForPlanDiagrams(
	ctx context.Context, t *testing.T, db isql.DB, jobID jobspb.JobID, expectedNumDiagrams int,
) {
	testutils.SucceedsSoon(t, func() error {
		return db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			infoStorage := jobs.InfoStorageForJob(txn, jobID)
			var found int
			err := infoStorage.Iterate(ctx, profilerconstants.DSPDiagramInfoKeyPrefix,
				func(infoKey string, value []byte) error {
					found++
					return nil
				})
			if err != nil {
				return err
			}
			if found != expectedNumDiagrams {
				return errors.Newf("found %d diagrams, expected %d", found, expectedNumDiagrams)
			}
			return nil
		})
	})
}

// TestJobsExecutionDetails tests that a job's execution details are retrieved
// and rendered correctly.
func TestJobsExecutionDetails(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Timeout the test in a few minutes if it hasn't succeeded.
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Minute*2)
	defer cancel()

	params, _ := tests.CreateTestServerParams()
	params.Knobs.JobsTestingKnobs = jobs.NewTestingKnobsWithShortIntervals()
	defer jobs.ResetConstructors()()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	runner := sqlutils.MakeSQLRunner(sqlDB)

	jobs.RegisterConstructor(jobspb.TypeImport, func(j *jobs.Job, _ *cluster.Settings) jobs.Resumer {
		return fakeExecResumer{
			OnResume: func(ctx context.Context) error {
				p := sql.PhysicalPlan{}
				infra := physicalplan.NewPhysicalInfrastructure(uuid.FastMakeV4(), base.SQLInstanceID(1))
				p.PhysicalInfrastructure = infra
				jobsprofiler.StorePlanDiagram(ctx, s.Stopper(), &p, s.InternalDB().(isql.DB), j.ID())
				checkForPlanDiagrams(ctx, t, s.InternalDB().(isql.DB), j.ID(), 1)
				return nil
			},
		}
	}, jobs.UsesTenantCostControl)

	runner.Exec(t, `CREATE TABLE t (id INT)`)
	runner.Exec(t, `INSERT INTO t SELECT generate_series(1, 100)`)
	var importJobID int
	runner.QueryRow(t, `IMPORT INTO t CSV DATA ('nodelocal://1/foo') WITH DETACHED`).Scan(&importJobID)
	jobutils.WaitForJobToSucceed(t, runner, jobspb.JobID(importJobID))

	var count int
	runner.QueryRow(t, `SELECT count(*) FROM [SHOW JOB $1 WITH EXECUTION DETAILS] WHERE plan_diagram IS NOT NULL`, importJobID).Scan(&count)
	require.NotZero(t, count)
	runner.CheckQueryResults(t, `SELECT count(*) FROM [SHOW JOBS WITH EXECUTION DETAILS] WHERE plan_diagram IS NOT NULL`, [][]string{{"1"}})
}
