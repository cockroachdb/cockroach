// Copyright 2023 The Cockroach Authors.
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
	gosql "database/sql"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestUpgradeSchemaChangerElements(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	const (
		WaitForSchemaChangeCompletion = 1
		ForceJobFirst                 = 2
		NoWaitRequired                = 0
	)
	testCases := []struct {
		name       string
		after      func(t *testing.T, sqlDB *gosql.DB) error
		run        func(t *testing.T, sqlDB *gosql.DB) error
		unpauseJob bool
		waitType   int
	}{
		{
			name: "running schema change that will have a deprecated waitForSchemaChangerElementMigration element",
			run: func(t *testing.T, sqlDB *gosql.DB) error {
				_, err := sqlDB.Exec("SET sql_safe_updates=off")
				require.NoError(t, err)
				_, err = sqlDB.Exec("ALTER TABLE t DROP COLUMN b")
				require.ErrorContains(t, err, "was paused before it completed with reason: pause point \"newschemachanger.before.exec\" hit")
				return nil
			},
			unpauseJob: true,
			waitType:   WaitForSchemaChangeCompletion,
		},
		{
			"running schema change that will have no deprecated elements",
			nil,
			func(t *testing.T, sqlDB *gosql.DB) error {
				_, err := sqlDB.Exec("SET sql_safe_updates=off")
				require.NoError(t, err)
				_, err = sqlDB.Exec("ALTER TABLE t ADD COLUMN j int")
				require.ErrorContains(t, err, "was paused before it completed with reason: pause point \"newschemachanger.before.exec\" hit")
				return nil
			},
			true,
			ForceJobFirst | WaitForSchemaChangeCompletion, // ADD COLUMN will not have any deprecated elements, so no one will wait for the job.
		},
		{
			"running schema change that will have a deprecated waitForSchemaChangerElementMigration element but is paused",
			func(t *testing.T, sqlDB *gosql.DB) error {
				_, err := sqlDB.Exec("SET CLUSTER SETTING jobs.debug.pausepoints=''")
				require.NoError(t, err)
				_, err = sqlDB.Exec("RESUME JOB (SELECT job_id FROM crdb_internal.jobs WHERE description LIKE 'ALTER TABLE%' AND status='paused' FETCH FIRST 1 ROWS ONLY);")
				require.NoError(t, err)
				testutils.SucceedsSoon(t, func() error {
					row := sqlDB.QueryRow("SELECT job_id FROM crdb_internal.jobs WHERE description LIKE 'ALTER TABLE%' AND status='succeeded' FETCH FIRST 1 ROWS ONLY;")
					var id int64
					if err = row.Scan(&id); err != nil {
						return err
					}
					return nil
				})
				return nil
			},
			func(t *testing.T, sqlDB *gosql.DB) error {
				_, err := sqlDB.Exec("SET sql_safe_updates=off")
				require.NoError(t, err)
				_, err = sqlDB.Exec("ALTER TABLE t DROP COLUMN b")
				require.ErrorContains(t, err, "was paused before it completed with reason: pause point \"newschemachanger.before.exec\" hit")
				return nil
			},
			false,
			NoWaitRequired,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			var params base.TestServerArgs
			params.Knobs.Server = &server.TestingKnobs{
				DisableAutomaticVersionUpgrade: make(chan struct{}),
				BinaryVersionOverride:          clusterversion.ByKey(clusterversion.V23_1_SchemaChangerDeprecatedIndexPredicates - 1),
			}

			var (
				schemaChangeAllowedToComplete chan struct{}
				waitedForJob                  chan struct{}
				jobIsPaused                   chan struct{}
				jobStarted                    chan struct{}
				readyToQuery                  int64 = 0
			)
			scJobID := int64(jobspb.InvalidJobID)
			params.Knobs.SQLDeclarativeSchemaChanger = &scexec.TestingKnobs{
				BeforeStage: func(p scplan.Plan, stageIdx int) error {
					if waitedForJob == nil {
						return nil
					}
					if p.Stages[stageIdx].Phase == scop.PreCommitPhase {
						atomic.StoreInt64(&scJobID, int64(p.JobID))
					} else if p.Stages[stageIdx].Phase > scop.PreCommitPhase {
						if tc.unpauseJob {
							jobStarted <- struct{}{}
						}
						<-waitedForJob
						waitedForJob = nil
						schemaChangeAllowedToComplete <- struct{}{}
					}
					return nil
				},
			}
			jobKnobs := jobs.NewTestingKnobsWithShortIntervals()
			jobKnobs.IntervalOverrides.WaitForJobsInitialDelay = shortInterval()
			jobKnobs.IntervalOverrides.WaitForJobsMaxDelay = shortInterval()
			jobKnobs.BeforeWaitForJobsQuery = func(jobs []jobspb.JobID) {
				if targetJobID := jobspb.JobID(atomic.LoadInt64(&scJobID)); targetJobID != jobspb.InvalidJobID {
					for _, j := range jobs {
						// The upgrade is waiting for the job...
						if j == targetJobID && atomic.CompareAndSwapInt64(&readyToQuery, 1, 0) {
							atomic.StoreInt64(&scJobID, int64(jobspb.InvalidJobID))
							waitedForJob <- struct{}{}
						}
					}

				}
			}
			params.Knobs.JobsTestingKnobs = jobKnobs
			s, sqlDB, _ := serverutils.StartServer(t, params)
			defer s.Stopper().Stop(ctx)
			// Make sure our cluster is up
			_, err := sqlDB.Exec("SET CLUSTER SETTING version=$1",
				clusterversion.ByKey(clusterversion.V23_1_SchemaChangerDeprecatedIndexPredicates-1).String())
			require.NoError(t, err)
			_, err = sqlDB.Exec("CREATE TABLE t (pk INT PRIMARY KEY, b INT)")
			require.NoError(t, err)
			_, err = sqlDB.Exec("CREATE UNIQUE INDEX ON t (b) WHERE pk > 0")
			require.NoError(t, err)
			_, err = sqlDB.Exec("SET CLUSTER SETTING jobs.debug.pausepoints='newschemachanger.before.exec'")
			require.NoError(t, err)
			ctx := context.Background()
			g := ctxgroup.WithContext(ctx)

			schemaChangeAllowedToComplete = make(chan struct{})
			waitedForJob = make(chan struct{})
			jobIsPaused = make(chan struct{})
			jobStarted = make(chan struct{})
			g.GoCtx(func(ctx context.Context) error {
				err := tc.run(t, sqlDB)
				jobIsPaused <- struct{}{}
				return err
			})

			g.GoCtx(func(ctx context.Context) error {
				<-jobIsPaused
				atomic.StoreInt64(&readyToQuery, 1)
				if tc.unpauseJob {
					_, err = sqlDB.Exec("SET CLUSTER SETTING jobs.debug.pausepoints=''")
					require.NoError(t, err)
					_, err = sqlDB.Exec("RESUME JOB $1", jobspb.JobID(atomic.LoadInt64(&scJobID)))
					require.NoError(t, err)
					// Wait for the job to start after the unpause,
					// we will let it get stuck during execution next.
					// We don't want a race condition with the migration
					// job that will pause it again.
					<-jobStarted
				}
				_, err = sqlDB.Exec("SET CLUSTER SETTING version = $1",
					clusterversion.ByKey(clusterversion.V23_1_SchemaChangerDeprecatedIndexPredicates).String())
				return err
			})

			if tc.waitType&ForceJobFirst > 0 {
				// Pretend that we waited for the job, if
				// we actually wait again a hang will occur.
				waitedForJob <- struct{}{}
			}
			if tc.waitType&WaitForSchemaChangeCompletion > 0 {
				<-schemaChangeAllowedToComplete
			}
			require.NoError(t, g.Wait())
			if tc.after != nil {
				// Prevent any unintended waits on schema changes.
				waitedForJob = nil
				require.NoError(t, tc.after(t, sqlDB))
			}
		})
	}
}

func shortInterval() *time.Duration {
	shortInterval := 10 * time.Millisecond
	if util.RaceEnabled {
		shortInterval *= 5
	}
	return &shortInterval
}
