// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"regexp"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobfrontier"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilitiespb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestInspectJobImplicitTxnSemantics validates how inspect jobs behave when
// triggered by statements that run in implicit transactions. It verifies that the job
// starts correctly, that errors or timeouts propagate to the user, and that
// client-visible semantics (like statement timeout or job failure) behave as expected.
func TestInspectJobImplicitTxnSemantics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var onInspectErrorToReturn atomic.Pointer[error]
	var pauseJobStart atomic.Bool
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Inspect: &sql.InspectTestingKnobs{
				OnInspectJobStart: func() error {
					// Use a timeout so we aren't stuck in here forever in case something bad happens.
					const maxPause = 30 * time.Second
					deadline := time.After(maxPause)
					for {
						if !pauseJobStart.Load() {
							break
						}
						select {
						case <-deadline:
							return errors.Newf("test timed out after %s while waiting for pause to clear", maxPause)
						default:
							time.Sleep(10 * time.Millisecond)
						}
					}
					if errPtr := onInspectErrorToReturn.Load(); errPtr != nil {
						return *errPtr
					}
					return nil
				},
			},
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	defer s.Stopper().Stop(context.Background())
	runner := sqlutils.MakeSQLRunner(db)

	runner.Exec(t, `
		CREATE DATABASE db;
		CREATE TABLE db.t (
			id INT PRIMARY KEY,
			val INT
		);
		CREATE INDEX i1 on db.t (val);
		INSERT INTO db.t VALUES (1, 2), (2,3);`)

	for _, tc := range []struct {
		desc              string
		setupSQL          string
		tearDownSQL       string
		pauseAtStart      bool
		onStartError      error
		expectedErrRegex  string
		expectedJobStatus string
		skipUnderRace     bool
	}{
		{desc: "inspect success", expectedJobStatus: "succeeded"},
		{desc: "inspect failure", onStartError: errors.Newf("inspect validation error"),
			expectedErrRegex: "inspect validation error", expectedJobStatus: "failed"},
		// Note: avoiding small statement timeouts, as this can impact the ability to reset.
		{desc: "statement timeout", setupSQL: "SET statement_timeout = '1s'", tearDownSQL: "RESET statement_timeout",
			pauseAtStart: true, expectedErrRegex: "canceled", expectedJobStatus: "succeeded", skipUnderRace: true},
	} {
		if tc.skipUnderRace {
			skip.UnderRace(t, "timing dependent")
		}
		t.Run(tc.desc, func(t *testing.T) {
			// Run in a closure so that we run teardown before verifying job status
			func() {
				if tc.setupSQL != "" {
					runner.Exec(t, tc.setupSQL)
				}
				if tc.tearDownSQL != "" {
					defer func() { runner.Exec(t, tc.tearDownSQL) }()
				}
				if tc.pauseAtStart {
					pauseJobStart.Store(true)
				}
				if tc.onStartError != nil {
					onInspectErrorToReturn.Store(&tc.onStartError)
					defer func() { onInspectErrorToReturn.Store(nil) }()
				}
				_, err := db.Exec("INSPECT TABLE db.t AS OF SYSTEM TIME '-1us'")
				pauseJobStart.Store(false)
				if tc.expectedErrRegex != "" {
					require.Error(t, err)
					re := regexp.MustCompile(tc.expectedErrRegex)
					match := re.MatchString(err.Error())
					require.True(t, match, "Error text %q doesn't match the expected regexp of %q",
						err.Error(), tc.expectedErrRegex)
				} else {
					require.NoError(t, err)
				}
			}()

			// Wait for the job to finish.
			var jobID int64
			var status string
			var fractionCompleted float64
			testutils.SucceedsSoon(t, func() error {
				row := db.QueryRow(`SELECT job_id, status, fraction_completed FROM [SHOW JOBS] WHERE job_type = 'INSPECT' ORDER BY job_id DESC LIMIT 1`)
				if err := row.Scan(&jobID, &status, &fractionCompleted); err != nil {
					return err
				}
				if status == "succeeded" || status == "failed" {
					return nil
				}
				return errors.Newf("job is not in the succeeded or failed state: %q", status)
			})
			require.Equal(t, tc.expectedJobStatus, status)
			if tc.expectedJobStatus == "succeeded" {
				require.InEpsilon(t, 1.0, fractionCompleted, 1e-9, "expected fraction_completed ≈ 1.0")
			}
		})
	}
}

// TestInspectJobProtectedTimestamp verifies that INSPECT jobs properly create
// and clean up protected timestamp records when using AS OF SYSTEM TIME.
func TestInspectJobProtectedTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, tc := range []struct {
		desc              string
		forceJobFailure   bool
		expectedJobStatus string
		expectError       bool
	}{
		{
			desc:              "job success with cleanup",
			forceJobFailure:   false,
			expectedJobStatus: "succeeded",
			expectError:       false,
		},
		{
			desc:              "job failure with cleanup",
			forceJobFailure:   true,
			expectedJobStatus: "failed",
			expectError:       true,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			var blockInspectExecution atomic.Bool
			var protectedTimestampCreated atomic.Bool

			ctx := context.Background()
			s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Inspect: &sql.InspectTestingKnobs{
						OnInspectAfterProtectedTimestamp: func() error {
							protectedTimestampCreated.Store(true)
							// Block execution until we've verified the protected timestamp
							for blockInspectExecution.Load() {
								time.Sleep(10 * time.Millisecond)
							}
							if tc.forceJobFailure {
								return errors.New("forced job failure for testing")
							}
							return nil
						},
					},
					JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
				},
			})
			defer s.Stopper().Stop(ctx)

			runner := sqlutils.MakeSQLRunner(db)
			runner.Exec(t, `
				CREATE DATABASE db;
				CREATE TABLE db.t (
					id INT PRIMARY KEY,
					val INT
				);
				CREATE INDEX i1 on db.t (val);
				INSERT INTO db.t VALUES (1, 2), (2, 3);`)

			// Start blocking inspection execution
			blockInspectExecution.Store(true)

			// Start INSPECT job with AS OF timestamp in a goroutine
			errCh := make(chan error, 1)
			go func() {
				_, err := db.Exec("INSPECT TABLE db.t AS OF SYSTEM TIME '-1us'")
				errCh <- err
			}()

			// Wait for the protected timestamp hook to be called
			testutils.SucceedsSoon(t, func() error {
				if !protectedTimestampCreated.Load() {
					return errors.New("protected timestamp hook not called yet")
				}
				return nil
			})

			// Get the job ID
			var jobID int64
			runner.QueryRow(t, `
				SELECT id
				FROM crdb_internal.system_jobs
				WHERE job_type = 'INSPECT' AND status = 'running'
				ORDER BY created DESC
				LIMIT 1
			`).Scan(&jobID)

			// Load the job and get protected timestamp record
			execCfg := s.ApplicationLayer().ExecutorConfig().(sql.ExecutorConfig)
			job, err := execCfg.JobRegistry.LoadJob(ctx, jobspb.JobID(jobID))
			require.NoError(t, err)

			details := job.Details().(jobspb.InspectDetails)
			require.NotNil(t, details.ProtectedTimestampRecord, "protected timestamp record should be set")
			protectedTSID := *details.ProtectedTimestampRecord

			// Check that the protected timestamp record actually exists in the system
			var recordExists bool
			require.NoError(t, execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
				pts := execCfg.ProtectedTimestampProvider.WithTxn(txn)
				_, err := pts.GetRecord(ctx, protectedTSID)
				if err != nil {
					if errors.Is(err, protectedts.ErrNotExists) {
						recordExists = false
						return nil
					}
					return err
				}
				recordExists = true
				return nil
			}))
			require.True(t, recordExists, "protected timestamp record should exist in the system")

			// Allow the job to complete
			blockInspectExecution.Store(false)

			// Wait for job to complete
			select {
			case err := <-errCh:
				if tc.expectError {
					require.Error(t, err, "INSPECT job should fail due to forced error")
				} else {
					require.NoError(t, err, "INSPECT job should complete successfully")
				}
			case <-time.After(30 * time.Second):
				t.Fatal("INSPECT job did not complete within timeout")
			}

			// Verify job status
			var status string
			runner.QueryRow(t, `
				SELECT status
				FROM crdb_internal.system_jobs
				WHERE id = $1
			`, jobID).Scan(&status)
			require.Equal(t, tc.expectedJobStatus, status, "job should have expected status")

			// Verify protected timestamp record is cleaned up
			testutils.SucceedsSoon(t, func() error {
				return execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
					pts := execCfg.ProtectedTimestampProvider.WithTxn(txn)
					_, err := pts.GetRecord(ctx, protectedTSID)
					if err != nil {
						if errors.Is(err, protectedts.ErrNotExists) {
							return nil // This is what we want
						}
						return err
					}
					return errors.New("protected timestamp record still exists")
				})
			})
		})
	}
}

// TestInspectProgressWithMultiRangeTable is a regression test for the bug where
// INSPECT progress could exceed 100% when a table had many ranges. This test
// creates a multi-node cluster with a multi-range table and verifies that:
// 1. The progress never exceeds 100%
// 2. The final progress is exactly 100% when the job completes
// 3. The total check count is based on partitioned spans, not PK spans
func TestInspectProgressWithMultiRangeTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderShort(t)

	ctx := context.Background()
	const numNodes = 3
	tc := serverutils.StartCluster(t, numNodes, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)
	runner := sqlutils.MakeSQLRunner(db)

	// Create a table and split it into multiple ranges.
	runner.Exec(t, `
		CREATE DATABASE testdb;
		USE testdb;
		CREATE TABLE multi_range_table (
			id INT PRIMARY KEY,
			val1 INT,
			val2 STRING,
			INDEX idx_val1 (val1),
			INDEX idx_val2 (val2)
		);
	`)

	// Insert data to create multiple ranges. We'll insert enough data and then
	// manually split to ensure multiple ranges.
	runner.Exec(t, `INSERT INTO multi_range_table SELECT i, i*2, md5(i::STRING) FROM generate_series(1, 1000) AS g(i)`)
	for i := 100; i <= 900; i += 100 {
		runner.Exec(t, `ALTER TABLE multi_range_table SPLIT AT VALUES ($1)`, i)
	}
	if tc.DefaultTenantDeploymentMode().IsExternal() {
		tc.GrantTenantCapabilities(ctx, t, serverutils.TestTenantID(),
			map[tenantcapabilitiespb.ID]string{tenantcapabilitiespb.CanAdminRelocateRange: "true"})
	}
	splitVals := []int{100, 200, 300, 400, 500, 600, 700, 800, 900}
	for i, splitVal := range splitVals {
		nodeIdx := (i % numNodes) + 1
		stmt := fmt.Sprintf(
			`ALTER TABLE multi_range_table EXPERIMENTAL_RELOCATE LEASE VALUES (%d, %d)`,
			nodeIdx, splitVal)
		testutils.SucceedsSoon(t, func() error {
			_, err := db.ExecContext(ctx, stmt)
			return err
		})
	}

	// Start the INSPECT job.
	t.Log("Starting INSPECT job on multi-range table with leases distributed across nodes")
	_, err := db.Exec(`
		COMMIT;
		INSPECT TABLE multi_range_table`)
	require.NoError(t, err)

	var jobID int64
	var status string
	var fractionCompleted float64
	runner.QueryRow(t, `
		SELECT job_id, status, fraction_completed
		FROM [SHOW JOBS]
		WHERE job_type = 'INSPECT'
		ORDER BY created DESC
		LIMIT 1
	`).Scan(&jobID, &status, &fractionCompleted)
	t.Logf("Job %d: status=%s, fraction_completed=%.4f", jobID, status, fractionCompleted)

	require.Equal(t, "succeeded", status, "INSPECT job should succeed")
	require.InEpsilon(t, 1.0, fractionCompleted, 0.01,
		"progress should be ~100%% at completion, got %.2f%%", fractionCompleted*100)
}

func TestInspectJobResumeOnAllSpansCompleted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	var s serverutils.TestServerInterface
	var db *gosql.DB

	s, db, _ = serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Inspect: &sql.InspectTestingKnobs{
				OnInspectJobStart: func() error {
					// Mark all spans as completed immediately.
					execCfg := s.ApplicationLayer().ExecutorConfig().(sql.ExecutorConfig)

					var jobID int64
					row := db.QueryRow(`
						SELECT job_id
						FROM [SHOW JOBS]
						WHERE job_type = 'INSPECT'
						ORDER BY created DESC
						LIMIT 1
					`)
					if err := row.Scan(&jobID); err != nil {
						return err
					}

					// Mark the primary key span as completed.
					var spans []roachpb.Span
					err := execCfg.InternalDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
						j, err := execCfg.JobRegistry.LoadJob(ctx, jobspb.JobID(jobID))
						if err != nil {
							return err
						}
						details := j.Details().(jobspb.InspectDetails)
						if len(details.Checks) == 0 {
							return errors.New("no checks in job details")
						}
						tableID := details.Checks[0].TableID

						desc, err := txn.Descriptors().ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Table(ctx, tableID)
						if err != nil {
							return err
						}
						spans = []roachpb.Span{desc.PrimaryIndexSpan(execCfg.Codec)}
						return nil
					})
					if err != nil {
						return err
					}

					// And store the completed spans in the job's frontier.
					return execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
						frontier, err := span.MakeFrontier(spans...)
						if err != nil {
							return err
						}
						defer frontier.Release()

						return jobfrontier.Store(ctx, txn, jobspb.JobID(jobID), "inspect_completed_spans", frontier)
					})
				},
			},
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	defer s.Stopper().Stop(ctx)

	runner := sqlutils.MakeSQLRunner(db)

	runner.Exec(t, `
		CREATE DATABASE db;
		CREATE TABLE db.t (
			id INT PRIMARY KEY,
			val INT
		);
		CREATE INDEX i1 on db.t (val);
		INSERT INTO db.t VALUES (1, 2), (2, 3);`)

	// The checks on the spans will be bypassed.
	_, err := db.Exec("INSPECT TABLE db.t AS OF SYSTEM TIME '-1us'")
	require.NoError(t, err, "INSPECT job should complete successfully")

	var jobID int64
	var status string
	var fractionCompleted float64
	testutils.SucceedsSoon(t, func() error {
		row := db.QueryRow(`
			SELECT job_id, status, fraction_completed
			FROM [SHOW JOBS]
			WHERE job_type = 'INSPECT'
			ORDER BY created DESC
			LIMIT 1
		`)
		if err := row.Scan(&jobID, &status, &fractionCompleted); err != nil {
			return err
		}
		if status != "succeeded" {
			return errors.Newf("job not complete: status=%s", status)
		}
		return nil
	})

	require.Equal(t, "succeeded", status, "job should succeed")
	require.InEpsilon(t, 1.0, fractionCompleted, 1e-9, "job should be completed")

	var totalChecks, completedChecks int64
	row := db.QueryRow(`
		SELECT
			(crdb_internal.pb_to_json('cockroach.sql.jobs.jobspb.Progress', value)->'inspect'->>'jobTotalCheckCount')::INT,
			(crdb_internal.pb_to_json('cockroach.sql.jobs.jobspb.Progress', value)->'inspect'->>'jobCompletedCheckCount')::INT
		FROM system.job_info
		WHERE job_id = $1 AND info_key = 'legacy_progress'
	`, jobID)
	require.NoError(t, row.Scan(&totalChecks, &completedChecks))
	require.Equal(t, totalChecks, int64(1), "job should have counted cluster checks")
	require.Equal(t, totalChecks, completedChecks, "all checks (including cluster checks) should be marked complete")
}
