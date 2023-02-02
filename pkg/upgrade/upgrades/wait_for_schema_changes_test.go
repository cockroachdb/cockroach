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
	gosql "database/sql"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestWaitForSchemaChangeMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.WithIssue(t, 95530, "bump minBinary to 22.2. Skip 22.2 mixed-version tests for future cleanup")

	ctx := context.Background()
	testCases := []struct {
		name  string
		setup func(t *testing.T, sqlDB *gosql.DB) error
		run   func(t *testing.T, sqlDB *gosql.DB) error
	}{
		{
			"running schema change that succeeds waits for success",
			nil,
			func(t *testing.T, sqlDB *gosql.DB) error {
				_, err := sqlDB.Exec("CREATE INDEX ON t (b)")
				return err
			},
		},
		{
			"running schema change that fails waits for failure",
			nil,
			func(t *testing.T, sqlDB *gosql.DB) error {
				if _, err := sqlDB.Exec("INSERT INTO t VALUES (1, 1), (2, 1)"); err != nil {
					return err
				}

				if _, err := sqlDB.Exec("CREATE UNIQUE INDEX ON t (b)"); err == nil {
					return errors.New("expected failure to create unique index but error was nil")
				}
				return nil
			},
		},
		{
			"running schema change that pauses should not block upgrade",
			nil,
			func(t *testing.T, sqlDB *gosql.DB) error {
				if _, err := sqlDB.Exec("SET CLUSTER SETTING jobs.debug.pausepoints = 'indexbackfill.before_flow'"); err != nil {
					return err
				}
				if _, err := sqlDB.Exec("CREATE INDEX ON t (b)"); err == nil {
					return errors.New("expected failure because of pausepoint but error was nil")
				}
				return nil
			},
		},
		{
			"previously successful schema change does block migration",
			func(t *testing.T, sqlDB *gosql.DB) error {
				_, err := sqlDB.Exec("CREATE UNIQUE INDEX ON t (b)")
				return err
			},
			nil,
		},
		{
			"previously failed schema change does block migration",
			func(t *testing.T, sqlDB *gosql.DB) error {
				if _, err := sqlDB.Exec("INSERT INTO t VALUES (1, 1), (2, 1)"); err != nil {
					return err
				}
				if _, err := sqlDB.Exec("CREATE UNIQUE INDEX ON t (b)"); err == nil {
					return errors.New("expected failure to create unique index but error was nil")
				}
				return nil
			},
			nil,
		},
		{
			"previously paused schema should not block upgrade",
			func(t *testing.T, sqlDB *gosql.DB) error {
				if _, err := sqlDB.Exec("SET CLUSTER SETTING jobs.debug.pausepoints = 'indexbackfill.before_flow'"); err != nil {
					return err
				}
				if _, err := sqlDB.Exec("CREATE INDEX ON t (b)"); err == nil {
					return errors.New("expected failure because of pausepoint but error was nil")
				}
				return nil
			},
			nil,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			params, _ := tests.CreateTestServerParams()
			params.Knobs.Server = &server.TestingKnobs{
				DisableAutomaticVersionUpgrade: make(chan struct{}),
				BinaryVersionOverride:          clusterversion.ByKey(clusterversion.TODODelete_V22_2NoNonMVCCAddSSTable - 1),
			}

			var (
				scStartedChan     chan struct{}
				scAllowResumeChan chan struct{}
				secondWaitChan    chan struct{}
			)

			params.Knobs.SQLSchemaChanger = &sql.SchemaChangerTestingKnobs{
				RunBeforeResume: func(_ jobspb.JobID) error {
					if scStartedChan != nil {
						close(scStartedChan)
					}
					if scAllowResumeChan != nil {
						<-scAllowResumeChan
					}
					return nil
				},
			}
			jobKnobs := jobs.NewTestingKnobsWithShortIntervals()
			jobKnobs.IntervalOverrides.WaitForJobsInitialDelay = shortInterval()
			jobKnobs.IntervalOverrides.WaitForJobsMaxDelay = shortInterval()

			var waitCount int32
			jobKnobs.BeforeWaitForJobsQuery = func(_ []jobspb.JobID) {
				if secondWaitChan != nil {
					if atomic.AddInt32(&waitCount, 1) == 2 {
						close(secondWaitChan)
					}
				}
			}
			params.Knobs.JobsTestingKnobs = jobKnobs

			s, sqlDB, _ := serverutils.StartServer(t, params)
			defer s.Stopper().Stop(ctx)
			_, err := sqlDB.Exec("CREATE TABLE t (pk INT PRIMARY KEY, b INT)")
			require.NoError(t, err)
			if tc.setup != nil {
				require.NoError(t, tc.setup(t, sqlDB))
			}

			ctx := context.Background()
			g := ctxgroup.WithContext(ctx)

			if tc.run != nil {
				scStartedChan = make(chan struct{})
				scAllowResumeChan = make(chan struct{})
				secondWaitChan = make(chan struct{})
				g.GoCtx(func(ctx context.Context) error {
					return tc.run(t, sqlDB)
				})
			}
			g.GoCtx(func(ctx context.Context) error {
				if scStartedChan != nil {
					<-scStartedChan
				}
				_, err = sqlDB.Exec("SET CLUSTER SETTING version = crdb_internal.node_executable_version()")
				return err
			})
			if tc.run != nil {
				<-scStartedChan
				<-secondWaitChan
				close(scAllowResumeChan)
			}
			require.NoError(t, g.Wait())
		})
	}
}

func TestWaitForSchemaChangeMigrationSynthetic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.WithIssue(t, 95530, "bump minBinary to 22.2. Skip 22.2 mixed-version tests for future cleanup")

	ctx := context.Background()

	upsertJob := func(sqlDB *gosql.DB, typ string, status string) error {
		var details jobspb.Details
		switch typ {
		case "SCHEMA CHANGE":
			details = jobspb.SchemaChangeDetails{}
		case "AUTO CREATE STATS":
			details = jobspb.CreateStatsDetails{Name: "__auto__"}
		default:
			return errors.Newf("job type not support in this test: %s", typ)
		}

		payload, err := protoutil.Marshal(&jobspb.Payload{
			UsernameProto: username.RootUserName().EncodeProto(),
			Details:       jobspb.WrapPayloadDetails(details),
		})
		if err != nil {
			return err
		}

		_, err = sqlDB.Exec(`UPSERT INTO system.jobs (id, status, payload) VALUES ($1, $2, $3)`,
			1, status, payload,
		)
		return err
	}

	terminalStates := []jobs.Status{
		jobs.StatusSucceeded,
		jobs.StatusFailed,
		jobs.StatusCanceled,
		jobs.StatusRevertFailed,
		jobs.StatusPaused,
	}
	nonTerminalStates := []jobs.Status{
		jobs.StatusPending,
		jobs.StatusRunning,
		jobs.StatusReverting,
		jobs.StatusCancelRequested,
		jobs.StatusPauseRequested,
	}

	testMigrate := func(jobType string, startingState string, nextState string) {
		name := fmt.Sprintf("%s_%s", jobType, startingState)
		if nextState != "" {
			name = fmt.Sprintf("%s_%s", name, nextState)
		}

		t.Run(name, func(t *testing.T) {
			params, _ := tests.CreateTestServerParams()
			params.Knobs.Server = &server.TestingKnobs{
				DisableAutomaticVersionUpgrade: make(chan struct{}),
				BinaryVersionOverride:          clusterversion.ByKey(clusterversion.TODODelete_V22_2NoNonMVCCAddSSTable - 1),
			}

			var waitCount int32
			var secondWaitChan chan struct{}
			params.Knobs.JobsTestingKnobs = &jobs.TestingKnobs{
				BeforeWaitForJobsQuery: func(_ []jobspb.JobID) {
					if secondWaitChan != nil {
						if atomic.AddInt32(&waitCount, 1) == 2 {
							close(secondWaitChan)
						}
					}
				},
				IntervalOverrides: jobs.TestingIntervalOverrides{
					WaitForJobsInitialDelay: shortInterval(),
					WaitForJobsMaxDelay:     shortInterval(),
				},
			}
			s, sqlDB, _ := serverutils.StartServer(t, params)
			defer s.Stopper().Stop(ctx)

			require.NoError(t, upsertJob(sqlDB, jobType, startingState))

			// This test expects all of the cases will eventually
			// pass the migration. If not, we timeout.
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			g := ctxgroup.WithContext(ctx)
			if nextState != "" {
				secondWaitChan = make(chan struct{})
			}

			g.GoCtx(func(ctx context.Context) error {
				_, err := sqlDB.Exec("SET CLUSTER SETTING version = crdb_internal.node_executable_version()")
				return err
			})

			if nextState != "" {
				<-secondWaitChan
				require.NoError(t, upsertJob(sqlDB, jobType, nextState))
			}
			require.NoError(t, g.Wait())
		})
	}

	for _, state := range nonTerminalStates {
		testMigrate("AUTO CREATE STATS", string(state), "")

	}
	for _, state := range terminalStates {
		testMigrate("AUTO CREATE STATS", string(state), "")
		testMigrate("SCHEMA CHANGE", string(state), "")
	}
	for _, startingState := range nonTerminalStates {
		for _, endingState := range terminalStates {
			testMigrate("SCHEMA CHANGE", string(startingState), string(endingState))
		}
	}
}

func shortInterval() *time.Duration {
	shortInterval := 10 * time.Millisecond
	if util.RaceEnabled {
		shortInterval *= 5
	}
	return &shortInterval
}
