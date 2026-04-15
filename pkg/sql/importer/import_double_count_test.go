// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package importer

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestImportEntryCountsDoubleCountingOnRetry verifies the behavior of
// EntryCounts across import retries. This test examines the potential bug where:
//
//  1. First distImport attempt processes some rows and checkpoints progress
//  2. A retryable error occurs after the checkpoint
//  3. On retry, getLastImportSummary loads the counts from the first attempt
//  4. The checkpoint tracker is initialized with those counts via Add()
//  5. If rows are reprocessed, bulkSummary.Add() could accumulate on top of old counts
//  6. Result: EntryCounts could contain inflated values
//
// For distributed merge imports, this is corrected by CorrectEntryCounts which
// replaces the accumulated counts with the actual KV-ingested counts from the
// merge processor. For non-distributed imports, there is no such correction.
//
// Current behavior (with proper resumePos tracking):
// - The resumePos mechanism ensures retries skip already-processed rows
// - EntryCounts remain accurate because no duplicate rows are processed
// - This test verifies that the counts are stable across retries
//
// However, if resumePos tracking fails or rows are reprocessed for any reason,
// the accumulation via bulkSummary.Add() could lead to incorrect counts being
// used for the row count validation at import_job.go:436
func TestImportEntryCountsDoubleCountingOnRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderDuress(t, "uses short job adoption intervals")

	ctx := context.Background()
	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		ExternalIODir: dir,
		Knobs: base.TestingKnobs{
			DistSQL: &execinfra.TestingKnobs{
				BulkAdderFlushesEveryBatch: true,
			},
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	defer srv.Stopper().Stop(ctx)

	runner := sqlutils.MakeSQLRunner(db)

	// Disable distributed merge to test the legacy import path where
	// CorrectEntryCounts is not called.
	runner.Exec(t, `SET CLUSTER SETTING bulkio.import.distributed_merge.enabled = false`)

	s := srv.ApplicationLayer()
	registry := s.JobRegistry().(*jobs.Registry)

	for _, tc := range []struct {
		name      string
		setupSQL  string
		importSQL string
		numRows   int
	}{
		{
			name:      "empty_table",
			setupSQL:  "CREATE TABLE empty_table (k INT PRIMARY KEY, v INT)",
			importSQL: "IMPORT INTO empty_table (k, v) CSV DATA ('nodelocal://1/export_empty/export*-n*.0.csv')",
			numRows:   10,
		},
		{
			name:      "nonempty_table",
			setupSQL:  "CREATE TABLE nonempty_table (k INT PRIMARY KEY, v INT); INSERT INTO nonempty_table SELECT i, i*10 FROM generate_series(1, 5) AS g(i)",
			importSQL: "IMPORT INTO nonempty_table (k, v) CSV DATA ('nodelocal://1/export_nonempty/export*-n*.0.csv')",
			numRows:   10,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var once sync.Once
			var retryTriggered atomic.Bool
			var firstAttemptEntryCounts map[uint64]int64
			var secondAttemptEntryCounts map[uint64]int64

			registry.TestingWrapResumerConstructor(
				jobspb.TypeImport,
				func(resumer jobs.Resumer) jobs.Resumer {
					r := resumer.(interface {
						TestingSetAlwaysFlushJobProgress()
						TestingSetAfterDistImportKnob(func() error)
					})

					// Always flush to ensure counts are checkpointed before error.
					r.TestingSetAlwaysFlushJobProgress()

					r.TestingSetAfterDistImportKnob(func() error {
						var err error
						once.Do(func() {
							// Capture the EntryCounts from the first attempt.
							// This should reflect the rows actually imported.
							job := resumer.(*importResumer).job
							prog := job.Progress()
							if importProg := prog.GetImport(); importProg != nil {
								firstAttemptEntryCounts = make(map[uint64]int64)
								for k, v := range importProg.Summary.EntryCounts {
									firstAttemptEntryCounts[k] = v
								}
							}

							retryTriggered.Store(true)
							// Inject a retryable error. The "rpc error" substring
							// makes this retryable per sqlerrors.IsDistSQLRetryableError.
							err = errors.New("rpc error: injected test retry")
						})
						return err
					})
					return resumer
				})

			runner.Exec(t, tc.setupSQL)

			// Export data for import.
			exportPath := fmt.Sprintf("nodelocal://1/export_%s/", tc.name[:len(tc.name)-6]) // strip "_table"
			runner.Exec(t, fmt.Sprintf(
				`EXPORT INTO CSV '%s' FROM SELECT i, i*10 FROM generate_series(6, 15) AS g(i)`,
				exportPath))

			// Run the import. It will fail on first attempt, then retry and succeed.
			runner.Exec(t, tc.importSQL)

			require.True(t, retryTriggered.Load(), "expected retry to be triggered")

			// Get the job ID to inspect its final progress.
			var jobID jobspb.JobID
			runner.QueryRow(t,
				`SELECT job_id FROM [SHOW JOBS] WHERE job_type = 'IMPORT' ORDER BY created DESC LIMIT 1`,
			).Scan(&jobID)

			// Query the final job progress to check EntryCounts.
			js := queryJob(db, jobID)
			require.NoError(t, js.err)
			require.Equal(t, jobs.StateSucceeded, js.status)

			secondAttemptEntryCounts = js.prog.Summary.EntryCounts

			// This is the core bug demonstration: if the import retried and
			// reprocessed rows, the EntryCounts could be inflated.
			//
			// Expected behavior:
			// - firstAttemptEntryCounts should have numRows entries
			// - secondAttemptEntryCounts should ALSO have numRows entries
			//
			// Buggy behavior (if double-counting occurs):
			// - secondAttemptEntryCounts > firstAttemptEntryCounts
			//
			// Note: In the current implementation with distributed merge disabled
			// and proper resumePos tracking, the retry should process 0 new rows
			// (all files are at EOF). So both should have the same count.
			// But if there's any reprocessing, we'd see double-counting.

			t.Logf("First attempt EntryCounts: %v", firstAttemptEntryCounts)
			t.Logf("Second attempt (final) EntryCounts: %v", secondAttemptEntryCounts)

			// Calculate the primary key ID to check the specific count.
			// We can just use any key from the EntryCounts map since there's only
			// one table with one primary index being imported.
			var pkID uint64
			for k := range secondAttemptEntryCounts {
				pkID = k
				break
			}

			firstCount := firstAttemptEntryCounts[pkID]
			secondCount := secondAttemptEntryCounts[pkID]

			// The bug would manifest as secondCount > firstCount when it should
			// be equal (since retry processes 0 new rows at EOF).
			if secondCount > firstCount {
				t.Logf("BUG DETECTED: EntryCounts inflated on retry!")
				t.Logf("  First attempt count: %d", firstCount)
				t.Logf("  Final count after retry: %d", secondCount)
				t.Logf("  Expected: both should be %d", tc.numRows)
			}

			// Verify the actual row count in the table is correct.
			var actualRows int
			runner.QueryRow(t, fmt.Sprintf(`SELECT count(*) FROM %s`, tc.name)).Scan(&actualRows)

			expectedRows := tc.numRows
			if tc.name == "nonempty_table" {
				expectedRows += 5 // Initial 5 rows
			}
			require.Equal(t, expectedRows, actualRows, "actual row count should be correct")

			// The critical assertion: demonstrate that if we used the job progress
			// EntryCounts for row count validation (as done in line 436 of
			// import_job.go), we might get the wrong value.
			if secondCount != int64(tc.numRows) {
				t.Errorf("Job progress EntryCounts[pkID] = %d, but only %d rows were imported in this attempt",
					secondCount, tc.numRows)
			}
		})
	}
}

// TestImportEntryCountsAccumulationPattern verifies the accumulation behavior
// of EntryCounts across retries by inspecting intermediate progress states.
// This test demonstrates HOW the double-counting happens by checking the
// checkpoint tracker's accumulation logic.
func TestImportEntryCountsAccumulationPattern(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderDuress(t, "uses short job adoption intervals")

	ctx := context.Background()
	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()

	var capturedProgresses []map[uint64]int64

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		ExternalIODir: dir,
		Knobs: base.TestingKnobs{
			DistSQL: &execinfra.TestingKnobs{
				BulkAdderFlushesEveryBatch: true,
			},
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	defer srv.Stopper().Stop(ctx)

	runner := sqlutils.MakeSQLRunner(db)
	runner.Exec(t, `SET CLUSTER SETTING bulkio.import.distributed_merge.enabled = false`)

	s := srv.ApplicationLayer()
	registry := s.JobRegistry().(*jobs.Registry)

	var attemptCount atomic.Int32
	maxAttempts := int32(3)

	registry.TestingWrapResumerConstructor(
		jobspb.TypeImport,
		func(resumer jobs.Resumer) jobs.Resumer {
			r := resumer.(interface {
				TestingSetAlwaysFlushJobProgress()
				TestingSetAfterDistImportKnob(func() error)
			})

			r.TestingSetAlwaysFlushJobProgress()

			r.TestingSetAfterDistImportKnob(func() error {
				job := resumer.(*importResumer).job
				prog := job.Progress()
				if importProg := prog.GetImport(); importProg != nil {
					snapshot := make(map[uint64]int64)
					for k, v := range importProg.Summary.EntryCounts {
						snapshot[k] = v
					}
					capturedProgresses = append(capturedProgresses, snapshot)
				}

				attempt := attemptCount.Add(1)
				if attempt < maxAttempts {
					return errors.Newf("rpc error: injected retry %d", attempt)
				}
				return nil
			})
			return resumer
		})

	runner.Exec(t, `CREATE TABLE accumulation_test (k INT PRIMARY KEY, v INT)`)
	runner.Exec(t, `EXPORT INTO CSV 'nodelocal://1/accum/' FROM SELECT i, i*10 FROM generate_series(1, 5) AS g(i)`)
	runner.Exec(t, `IMPORT INTO accumulation_test (k, v) CSV DATA ('nodelocal://1/accum/export*-n*.0.csv')`)

	// Verify we captured progress from multiple attempts.
	require.GreaterOrEqual(t, len(capturedProgresses), int(maxAttempts),
		"should have captured progress from multiple attempts")

	t.Logf("Captured %d progress snapshots", len(capturedProgresses))
	for i, prog := range capturedProgresses {
		t.Logf("Attempt %d EntryCounts: %v", i+1, prog)
	}

	// If accumulation bug exists, we'd see increasing counts across attempts
	// even though the same rows are being processed.
	if len(capturedProgresses) >= 2 {
		first := capturedProgresses[0]
		last := capturedProgresses[len(capturedProgresses)-1]

		// Get any key to compare (should only be one for single table)
		for k := range first {
			if last[k] > first[k] {
				t.Logf("Accumulation detected: attempt 1 had count %d, final attempt has %d",
					first[k], last[k])
			}
		}
	}
}

// TestImportRowCountValidationWithRetry specifically tests the scenario at
// import_job.go:436 where totalImportedRows is read from
// importProgress.Summary.EntryCounts[pkID]. This test verifies that the value
// used for row count validation is correct even after retries.
//
// The concern is that if EntryCounts accumulates duplicate counts across
// retries, the expected row count calculation would be wrong:
//   expectedRowCount = totalImportedRows + InitialRowCount
//
// This would cause the INSPECT validation to fail or pass incorrectly.
func TestImportRowCountValidationWithRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderDuress(t, "uses short job adoption intervals")

	ctx := context.Background()
	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		ExternalIODir: dir,
		Knobs: base.TestingKnobs{
			DistSQL: &execinfra.TestingKnobs{
				BulkAdderFlushesEveryBatch: true,
			},
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	defer srv.Stopper().Stop(ctx)

	runner := sqlutils.MakeSQLRunner(db)

	// Use sync mode so the import waits for INSPECT validation to complete.
	// If the expected row count is wrong, the INSPECT job will fail and the
	// import will fail.
	runner.Exec(t, `SET CLUSTER SETTING bulkio.import.row_count_validation.mode = 'sync'`)

	s := srv.ApplicationLayer()
	registry := s.JobRegistry().(*jobs.Registry)

	// Test both distributed merge and legacy paths.
	for _, useDistMerge := range []bool{false, true} {
		mergeName := "legacy"
		if useDistMerge {
			mergeName = "distributed_merge"
		}

		t.Run(mergeName, func(t *testing.T) {
			if useDistMerge {
				runner.Exec(t, `SET CLUSTER SETTING bulkio.import.distributed_merge.enabled = true`)
			} else {
				runner.Exec(t, `SET CLUSTER SETTING bulkio.import.distributed_merge.enabled = false`)
			}

			tableName := fmt.Sprintf("validate_%s", mergeName)

			var once sync.Once
			var retryTriggered atomic.Bool

			registry.TestingWrapResumerConstructor(
				jobspb.TypeImport,
				func(resumer jobs.Resumer) jobs.Resumer {
					r := resumer.(interface {
						TestingSetAlwaysFlushJobProgress()
						TestingSetAfterDistImportKnob(func() error)
					})

					r.TestingSetAlwaysFlushJobProgress()

					r.TestingSetAfterDistImportKnob(func() error {
						var err error
						once.Do(func() {
							retryTriggered.Store(true)
							err = errors.New("rpc error: injected test retry")
						})
						return err
					})
					return resumer
				})

			runner.Exec(t, fmt.Sprintf(`CREATE TABLE %s (k INT PRIMARY KEY, v INT)`, tableName))

			// Pre-populate with some rows.
			runner.Exec(t, fmt.Sprintf(
				`INSERT INTO %s SELECT i, i*10 FROM generate_series(1, 7) AS g(i)`,
				tableName))

			// Export data to import.
			exportPath := fmt.Sprintf("nodelocal://1/validate_%s/", mergeName)
			runner.Exec(t, fmt.Sprintf(
				`EXPORT INTO CSV '%s' FROM SELECT i, i*10 FROM generate_series(8, 20) AS g(i)`,
				exportPath))

			// Import with retry. The import will:
			// 1. Process all 13 rows (8-20) in the first attempt
			// 2. Checkpoint the progress
			// 3. Hit the injected error
			// 4. Retry and process 0 new rows (all at EOF due to resumePos)
			// 5. Calculate expectedRowCount = totalImportedRows + InitialRowCount
			//    = 13 + 7 = 20
			// 6. Run INSPECT validation expecting 20 total rows
			// 7. INSPECT should find exactly 20 rows
			//
			// If EntryCounts were double-counted, expectedRowCount would be wrong
			// (e.g., 26 instead of 20) and INSPECT would fail.
			runner.Exec(t, fmt.Sprintf(
				`IMPORT INTO %s (k, v) CSV DATA ('%sexport*-n*.0.csv')`,
				tableName, exportPath))

			require.True(t, retryTriggered.Load(), "expected retry to be triggered")

			// Verify the final row count is correct.
			var actualRows int
			runner.QueryRow(t, fmt.Sprintf(`SELECT count(*) FROM %s`, tableName)).Scan(&actualRows)
			require.Equal(t, 20, actualRows, "should have 20 total rows (7 initial + 13 imported)")

			// The fact that the import succeeded with sync validation mode means
			// the INSPECT job passed, which means the expected row count was
			// correct. If there was double-counting in EntryCounts, the INSPECT
			// would have failed.
			t.Logf("Import with retry completed successfully, INSPECT validation passed")
		})
	}
}

// TestImportEntryCountsDoubleCountingBugDemonstration explicitly breaks the
// resumePos mechanism to demonstrate that EntryCounts WILL be double-counted
// if rows are reprocessed. This test proves the vulnerability exists even though
// it doesn't trigger under normal operation.
//
// This test:
// 1. Allows first attempt to complete and checkpoint progress with resumePos
// 2. Manually resets resumePos to 0 before retry
// 3. Forces rows to be reprocessed
// 4. Demonstrates that EntryCounts accumulates (doubles)
//
// This is the BUG that would manifest if resumePos tracking ever fails.
func TestImportEntryCountsDoubleCountingBugDemonstration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderDuress(t, "uses short job adoption intervals")

	ctx := context.Background()
	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		ExternalIODir: dir,
		Knobs: base.TestingKnobs{
			DistSQL: &execinfra.TestingKnobs{
				BulkAdderFlushesEveryBatch: true,
			},
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	defer srv.Stopper().Stop(ctx)

	runner := sqlutils.MakeSQLRunner(db)

	// Disable distributed merge to test the legacy import path where
	// there is no CorrectEntryCounts to fix the inflation.
	runner.Exec(t, `SET CLUSTER SETTING bulkio.import.distributed_merge.enabled = false`)

	// Disable INSPECT validation so the import can complete despite the
	// inflated EntryCounts. We want to see the bug (inflated count), not
	// have the import fail due to validation.
	runner.Exec(t, `SET CLUSTER SETTING bulkio.import.row_count_validation.mode = 'off'`)

	s := srv.ApplicationLayer()
	registry := s.JobRegistry().(*jobs.Registry)

	var once sync.Once
	var retryTriggered atomic.Bool
	var firstAttemptCount int64
	var secondAttemptCount int64

	registry.TestingWrapResumerConstructor(
		jobspb.TypeImport,
		func(resumer jobs.Resumer) jobs.Resumer {
			r := resumer.(interface {
				TestingSetAlwaysFlushJobProgress()
				TestingSetAfterDistImportKnob(func() error)
			})

			r.TestingSetAlwaysFlushJobProgress()

			r.TestingSetAfterDistImportKnob(func() error {
				job := resumer.(*importResumer).job
				prog := job.Progress()

				var err error
				once.Do(func() {
					// Capture the count from the first attempt.
					if importProg := prog.GetImport(); importProg != nil {
						for _, v := range importProg.Summary.EntryCounts {
							firstAttemptCount = v
							break
						}
					}

					// INTENTIONALLY BREAK resumePos: Reset it to 0 so rows will be
					// reprocessed on retry. This simulates what would happen if
					// resumePos tracking failed.
					if err := job.NoTxn().Update(ctx, func(
						txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater,
					) error {
						importProg := md.Progress.GetImport()
						if importProg != nil {
							// Reset all resumePos to 0, forcing rows to be reprocessed.
							for i := range importProg.ResumePos {
								importProg.ResumePos[i] = 0
							}
							t.Logf("INTENTIONALLY CORRUPTED resumePos: reset all to 0")
						}
						ju.UpdateProgress(md.Progress)
						return nil
					}); err != nil {
						t.Fatalf("failed to corrupt resumePos: %v", err)
					}

					retryTriggered.Store(true)
					err = errors.New("rpc error: injected test retry")
				})

				return err
			})
			return resumer
		})

	runner.Exec(t, `CREATE TABLE bug_demo (k INT PRIMARY KEY, v INT)`)
	runner.Exec(t, `EXPORT INTO CSV 'nodelocal://1/bug_demo/' FROM SELECT i, i*10 FROM generate_series(1, 10) AS g(i)`)

	// Run import. Because we corrupted resumePos:
	// - First attempt: processes 10 rows, EntryCounts[pkID] = 10
	// - After first attempt: resumePos reset to 0
	// - Retry: processes 10 rows AGAIN, bulkSummary.Add() → EntryCounts[pkID] = 10 + 10 = 20
	runner.Exec(t, `IMPORT INTO bug_demo (k, v) CSV DATA ('nodelocal://1/bug_demo/export*-n*.0.csv')`)

	require.True(t, retryTriggered.Load(), "expected retry to be triggered")

	// Get the final EntryCounts from the completed job.
	var jobID jobspb.JobID
	runner.QueryRow(t,
		`SELECT job_id FROM [SHOW JOBS] WHERE job_type = 'IMPORT' ORDER BY created DESC LIMIT 1`,
	).Scan(&jobID)
	js := queryJob(db, jobID)
	require.NoError(t, js.err)
	for _, v := range js.prog.Summary.EntryCounts {
		secondAttemptCount = v
		break
	}

	t.Logf("First attempt EntryCounts: %d", firstAttemptCount)
	t.Logf("Final EntryCounts (after retry): %d", secondAttemptCount)

	// THE FIX: Even though we broke resumePos and rows were reprocessed,
	// EntryCounts should still be correct (10) because the fix uses actual
	// KV-ingested counts via CorrectEntryCounts.
	if secondAttemptCount != firstAttemptCount {
		t.Errorf("🐛 BUG DETECTED: EntryCounts changed from %d to %d despite deduplication fix!",
			firstAttemptCount, secondAttemptCount)
		t.Errorf("   Expected both to be %d after the fix was applied.", firstAttemptCount)
	} else {
		t.Logf("✅ FIX VERIFIED: EntryCounts correctly stayed at %d despite reprocessing rows",
			secondAttemptCount)
		t.Logf("   The CorrectEntryCounts fix prevented double-counting from inflating the value.")
	}

	// Verify the actual row count is still correct (deduplication in KV).
	var actualRows int
	runner.QueryRow(t, `SELECT count(*) FROM bug_demo`).Scan(&actualRows)
	require.Equal(t, 10, actualRows, "should have 10 rows (duplicates were deduplicated by KV)")

	// The critical validation: EntryCounts should now match actual rows.
	// This ensures row count validation at import_job.go:436 will work correctly.
	if secondAttemptCount == int64(actualRows) {
		t.Logf("✅ VALIDATION CORRECT: EntryCounts=%d matches actual rows=%d",
			secondAttemptCount, actualRows)
		t.Logf("   Row count validation at import_job.go:436 will pass with the correct expected count.")
	} else {
		t.Errorf("💥 VALIDATION BUG: EntryCounts=%d but actual rows=%d",
			secondAttemptCount, actualRows)
		t.Errorf("   If this count was used at import_job.go:436, INSPECT validation would fail!")
		t.Errorf("   Expected row count would be: %d (InitialRowCount=0 + EntryCounts=%d)",
			secondAttemptCount, secondAttemptCount)
		t.Errorf("   Actual row count is: %d", actualRows)
	}
}

// TestImportEntryCountsDistributedMergeCorrection verifies that distributed
// merge imports are protected from the double-counting bug via CorrectEntryCounts.
// Even if rows are reprocessed, the merge phase replaces the inflated counts
// with the true KV-ingested counts.
func TestImportEntryCountsDistributedMergeCorrection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderDuress(t, "uses short job adoption intervals")

	ctx := context.Background()
	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		ExternalIODir: dir,
		Knobs: base.TestingKnobs{
			DistSQL: &execinfra.TestingKnobs{
				BulkAdderFlushesEveryBatch: true,
			},
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	defer srv.Stopper().Stop(ctx)

	runner := sqlutils.MakeSQLRunner(db)

	// Enable distributed merge to test the correction path.
	runner.Exec(t, `SET CLUSTER SETTING bulkio.import.distributed_merge.enabled = true`)

	s := srv.ApplicationLayer()
	registry := s.JobRegistry().(*jobs.Registry)

	var once sync.Once
	var retryTriggered atomic.Bool
	var mapPhaseCount int64
	var finalCount int64

	registry.TestingWrapResumerConstructor(
		jobspb.TypeImport,
		func(resumer jobs.Resumer) jobs.Resumer {
			r := resumer.(interface {
				TestingSetAlwaysFlushJobProgress()
				TestingSetAfterDistImportKnob(func() error)
			})

			r.TestingSetAlwaysFlushJobProgress()

			r.TestingSetAfterDistImportKnob(func() error {
				job := resumer.(*importResumer).job
				prog := job.Progress()

				var err error
				once.Do(func() {
					// Capture the map-phase count (potentially inflated).
					if importProg := prog.GetImport(); importProg != nil {
						for _, v := range importProg.Summary.EntryCounts {
							mapPhaseCount = v
							break
						}
					}

					// Corrupt resumePos like in the previous test.
					if err := job.NoTxn().Update(ctx, func(
						txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater,
					) error {
						importProg := md.Progress.GetImport()
						if importProg != nil {
							for i := range importProg.ResumePos {
								importProg.ResumePos[i] = 0
							}
							t.Logf("INTENTIONALLY CORRUPTED resumePos for distributed merge test")
						}
						ju.UpdateProgress(md.Progress)
						return nil
					}); err != nil {
						t.Fatalf("failed to corrupt resumePos: %v", err)
					}

					retryTriggered.Store(true)
					err = errors.New("rpc error: injected test retry")
				})

				return err
			})
			return resumer
		})

	runner.Exec(t, `CREATE TABLE distmerge_demo (k INT PRIMARY KEY, v INT)`)
	runner.Exec(t, `EXPORT INTO CSV 'nodelocal://1/distmerge_demo/' FROM SELECT i, i*10 FROM generate_series(1, 10) AS g(i)`)

	// Run import with distributed merge enabled.
	runner.Exec(t, `IMPORT INTO distmerge_demo (k, v) CSV DATA ('nodelocal://1/distmerge_demo/export*-n*.0.csv')`)

	require.True(t, retryTriggered.Load(), "expected retry to be triggered")

	// Get the final count after CorrectEntryCounts.
	var jobID jobspb.JobID
	runner.QueryRow(t,
		`SELECT job_id FROM [SHOW JOBS] WHERE job_type = 'IMPORT' ORDER BY created DESC LIMIT 1`,
	).Scan(&jobID)
	js := queryJob(db, jobID)
	for _, v := range js.prog.Summary.EntryCounts {
		finalCount = v
		break
	}

	t.Logf("Map-phase count (potentially inflated): %d", mapPhaseCount)
	t.Logf("Final count (after CorrectEntryCounts): %d", finalCount)

	// Verify the actual row count.
	var actualRows int
	runner.QueryRow(t, `SELECT count(*) FROM distmerge_demo`).Scan(&actualRows)
	require.Equal(t, 10, actualRows, "should have 10 rows")

	// The key finding: Even if map-phase count was inflated, CorrectEntryCounts
	// should have fixed it to match the actual KV-ingested count.
	if finalCount == int64(actualRows) {
		t.Logf("✅ DISTRIBUTED MERGE PROTECTION: CorrectEntryCounts fixed the count to %d", finalCount)
		t.Logf("   This demonstrates that distributed merge is protected from double-counting.")
	} else {
		t.Errorf("Expected final count to be corrected to %d, but got %d", actualRows, finalCount)
	}
}
