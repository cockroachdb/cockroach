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
	"math"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgradebase"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type schemaChangeTestCase struct {
	// Test identifier.
	name string
	// Job status when the job is intercepted while transitioning to the intercepted status.
	query string
	// Whether the schema-change job should wait for the migration to restart
	// after failure before proceeding.
	waitForMigrationRestart bool
	// Cancel the intercepted schema-change to inject a failure during migration.
	cancelSchemaJob bool
	// Expected number of schema-changes that are skipped during migration.
	expectedSkipped int
}

// TestMigrationWithFailures tests modification of a table during
// migration with different failures. It tests the system behavior with failure
// combinations of the migration job and schema-change jobs at different stages
// in their progress.
//
// This test was originally written in support of the migration which added
// exponential backoff to the system.jobs table, but was retrofitted to prevent
// regressions.
func TestMigrationWithFailures(t *testing.T) {
	const createTableBefore = `
CREATE TABLE test.test_table (
	id                INT8      DEFAULT unique_rowid() PRIMARY KEY,
	status            STRING    NOT NULL,
	created           TIMESTAMP NOT NULL DEFAULT now(),
	payload           BYTES     NOT NULL,
	progress          BYTES,
	created_by_type   STRING,
	created_by_id     INT,
	claim_session_id  BYTES,
	claim_instance_id INT8,
	INDEX (status, created),
	INDEX (created_by_type, created_by_id) STORING (status),
	FAMILY fam_0_id_status_created_payload (id, status, created, payload, created_by_type, created_by_id),
	FAMILY progress (progress),
	FAMILY claim (claim_session_id, claim_instance_id)
);
`
	const createTableAfter = `
CREATE TABLE test.test_table (
	id                INT8      DEFAULT unique_rowid() PRIMARY KEY,
	status            STRING    NOT NULL,
	created           TIMESTAMP NOT NULL DEFAULT now(),
	payload           BYTES     NOT NULL,
	progress          BYTES,
	created_by_type   STRING,
	created_by_id     INT,
	claim_session_id  BYTES,
	claim_instance_id INT8,
	num_runs          INT8,
	last_run          TIMESTAMP,
	INDEX (status, created),
	INDEX (created_by_type, created_by_id) STORING (status),
	INDEX jobs_run_stats_idx (
    claim_session_id,
    status,
    created
  ) STORING(last_run, num_runs, claim_instance_id)
    WHERE ` + systemschema.JobsRunStatsIdxPredicate + `,
	FAMILY fam_0_id_status_created_payload (id, status, created, payload, created_by_type, created_by_id),
	FAMILY progress (progress),
	FAMILY claim (claim_session_id, claim_instance_id, num_runs, last_run)
);
`

	testCases := []schemaChangeTestCase{
		{
			name:                    "adding columns",
			query:                   upgrades.TestingAddColsQuery,
			waitForMigrationRestart: false, // Does not matter.
			cancelSchemaJob:         false, // Does not matter.
			expectedSkipped:         0,     // Will be ignored.
		},
		{
			name:                    "adding index",
			query:                   upgrades.TestingAddIndexQuery,
			waitForMigrationRestart: false, // Does not matter.
			cancelSchemaJob:         false, // Does not matter.
			expectedSkipped:         0,     // Will be ignored.
		},
		{
			name:                    "fail adding columns",
			query:                   upgrades.TestingAddColsQuery,
			waitForMigrationRestart: true, // Need to wait to observe failing schema change.
			cancelSchemaJob:         true, // To fail adding columns.
			expectedSkipped:         0,
		},
		{
			name:                    "fail adding index",
			query:                   upgrades.TestingAddIndexQuery,
			waitForMigrationRestart: true, // Need to wait to observe failing schema change.
			cancelSchemaJob:         true, // To fail adding index.
			expectedSkipped:         1,    // Columns must not be added again.
		},
		{
			name:                    "skip none",
			query:                   upgrades.TestingAddColsQuery,
			waitForMigrationRestart: true, // Need to wait to observe schema change and have correct expectedSkipped count.
			cancelSchemaJob:         true, // To fail adding index and skip adding column.
			expectedSkipped:         0,    // Both columns and index must be added.
		},
		{
			name:                    "skip adding columns",
			query:                   upgrades.TestingAddIndexQuery,
			waitForMigrationRestart: true, // Need to wait to observe schema change and have correct expectedSkipped count.
			cancelSchemaJob:         true, // To fail adding index and skip adding column.
			expectedSkipped:         1,    // Columns must not be added again.
		},
		{
			name:                    "skip adding columns and index",
			query:                   upgrades.TestingAddIndexQuery,
			waitForMigrationRestart: true,  // Need to wait to observe schema change and have correct expectedSkipped count.
			cancelSchemaJob:         false, // To fail adding index and skip adding column.
			expectedSkipped:         2,     // Both columns and index must not be added again.
		},
	}

	testMigrationWithFailures(t, createTableBefore, createTableAfter, upgrades.MakeFakeMigrationForTestMigrationWithFailures, testCases)
}

// TestMigrationWithFailuresMultipleAltersOnSameColumn tests a migration that
// alters a column in a table multiple times with failures at different stages
// of the migration.
func TestMigrationWithFailuresMultipleAltersOnSameColumn(t *testing.T) {

	const createTableBefore = `
CREATE TABLE test.test_table (
   username STRING NOT NULL
);
`

	const createTableAfter = `
CREATE TABLE test.test_table (
	username STRING NOT NULL,
	user_id OID NOT NULL
);
`

	testCases := []schemaChangeTestCase{
		{
			name:                    "add column",
			query:                   upgrades.TestingAddNewColStmt,
			waitForMigrationRestart: false,
			cancelSchemaJob:         false,
			expectedSkipped:         0,
		},
		{
			name:                    "alter column",
			query:                   upgrades.TestingAlterNewColStmt,
			waitForMigrationRestart: false,
			cancelSchemaJob:         false,
			expectedSkipped:         0,
		},
		{
			name:                    "skip none",
			query:                   upgrades.TestingAddNewColStmt,
			waitForMigrationRestart: true,
			cancelSchemaJob:         true,
			expectedSkipped:         0,
		},
		{
			name:                    "skip adding column",
			query:                   upgrades.TestingAlterNewColStmt,
			waitForMigrationRestart: true,
			cancelSchemaJob:         true,
			expectedSkipped:         1,
		},
		{
			name:                    "skip adding column and altering column",
			query:                   upgrades.TestingAlterNewColStmt,
			waitForMigrationRestart: true,
			cancelSchemaJob:         false,
			expectedSkipped:         2,
		},
	}

	testMigrationWithFailures(t, createTableBefore, createTableAfter, upgrades.MakeFakeMigrationForTestMigrationWithFailuresMultipleAltersOnSameColumn, testCases)
}

// testMigrationWithFailures tests a migration that alters the schema of a
// table with failures injected at multiple points within the migration.
// The table should be named test.test_table.
func testMigrationWithFailures(
	t *testing.T,
	createTableBefore string,
	createTableAfter string,
	testMigrationFunc upgrades.SchemaChangeTestMigrationFunc,
	testCases []schemaChangeTestCase,
) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "very slow")

	// We're going to be migrating from the minimum supported version to the
	// "next" version. We'll be injecting the migration for the next version.
	startKey := clusterversion.BinaryMinSupportedVersionKey
	startCV := clusterversion.ByKey(startKey)
	endCV := startCV
	endCV.Internal += 2

	// The tests follows the following procedure.
	//
	// Inject the old table descriptor and ensure that the system is using the
	// deprecated jobs-table.
	//
	// Start migration, which initiates two schema-change jobs one by one. Test
	// the system for each schema-change job separately. Later on, we inject
	// failure in this migration, causing it to fail.
	//
	// Depending on the test setting, intercept the target schema-change job,
	// preventing the job from progressing. We may cancel this schema-change or
	// let it succeed to test different scenarios.
	//
	// Cancel the migration, causing the migration to revert and fail.
	//
	// Wait for the canceled migration-job to finish, expecting its failure. The
	// schema-change job is still not progressing to control what the restarted
	// migration will observe.
	//
	// Restart the migration, expecting it to succeed. Depending on the test setting,
	// the intercepted schema-change job may wail for the migration job to resume.
	// If it does, the migration job is expected to observe the ongoing schema-change.
	// The ongoing schema-change is canceled or not, depending on the test case.
	// In either case, we expect the correct number of mutations to be skipped
	// during the migration.
	//
	// If we canceled the schema-job, expect it to rerun
	// as part of the migration. Otherwise, expect the schema-change to be ignored
	// during the migration.
	//
	// Finally, we validate that the schema changes are in effect by reading the new
	// columns and the index, and by running a job that is failed and retried to
	// practice exponential-backoff machinery.

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			scope := log.Scope(t)
			defer scope.Close(t)

			type updateEvent struct {
				orig, updated jobs.JobMetadata
				errChan       chan error
			}

			ctx := context.Background()
			cancelCtx, cancel := context.WithCancel(ctx)
			// To intercept the schema-change and the migration job.
			updateEventChan := make(chan updateEvent)
			var enableUpdateEventCh syncutil.AtomicBool
			enableUpdateEventCh.Set(false)
			beforeUpdate := func(orig, updated jobs.JobMetadata) error {
				if !enableUpdateEventCh.Get() {
					return nil
				}
				ue := updateEvent{
					orig:    orig,
					updated: updated,
					errChan: make(chan error),
				}
				select {
				case updateEventChan <- ue:
				case <-cancelCtx.Done():
					return cancelCtx.Err()
				}
				select {
				case err := <-ue.errChan:
					return err
				case <-cancelCtx.Done():
					return cancelCtx.Err()
				}
			}
			var schemaEvent updateEvent
			migrationWaitCh := make(chan struct{})

			// Number of schema-change jobs that are skipped.
			settings := cluster.MakeTestingClusterSettingsWithVersions(
				endCV, startCV, false, /* initializeVersion */
			)
			require.NoError(t, clusterversion.Initialize(
				ctx, startCV, &settings.SV,
			))
			jobsKnobs := jobs.NewTestingKnobsWithShortIntervals()
			jobsKnobs.BeforeUpdate = beforeUpdate
			migrationFunc, expectedDescriptor := testMigrationFunc()
			clusterArgs := base.TestClusterArgs{
				ServerArgs: base.TestServerArgs{
					Settings: settings,
					Knobs: base.TestingKnobs{
						Server: &server.TestingKnobs{
							DisableAutomaticVersionUpgrade: make(chan struct{}),
							BinaryVersionOverride:          startCV,
							BootstrapVersionKeyOverride:    startKey,
						},
						JobsTestingKnobs: jobsKnobs,
						SQLExecutor: &sql.ExecutorTestingKnobs{
							BeforeExecute: func(ctx context.Context, stmt string, descriptors *descs.Collection) {
								if stmt == upgrades.WaitForJobStatement {
									select {
									case migrationWaitCh <- struct{}{}:
									case <-ctx.Done():
									}
								}
							},
						},
						UpgradeManager: &upgradebase.TestingKnobs{
							ListBetweenOverride: func(from, to roachpb.Version) []roachpb.Version {
								return []roachpb.Version{
									endCV,
								}
							},
							RegistryOverride: func(cv roachpb.Version) (upgradebase.Upgrade, bool) {
								if cv.Equal(endCV) {
									return upgrade.NewTenantUpgrade("testing",
										endCV,
										upgrade.NoPrecondition,
										migrationFunc,
									), true
								}
								panic("unexpected version")
							}},
					},
				},
			}
			tc := testcluster.StartTestCluster(t, 1, clusterArgs)
			defer tc.Stopper().Stop(ctx)
			defer cancel()
			s := tc.Server(0)
			sqlDB := tc.ServerConn(0)
			tdb := sqlutils.MakeSQLRunner(sqlDB)

			// Build the expected table descriptor, inject it into the
			// migration function, drop it, and then add the descriptor
			// in the pre-migration state.
			tdb.Exec(t, "CREATE DATABASE test")
			tdb.Exec(t, createTableAfter)
			var desc catalog.TableDescriptor
			require.NoError(t, s.InternalDB().(descs.DB).DescsTxn(ctx, func(
				ctx context.Context, txn descs.Txn,
			) (err error) {
				tn := tree.MakeTableNameWithSchema("test", "public", "test_table")
				_, desc, err = descs.PrefixAndTable(ctx, txn.Descriptors().ByName(txn.KV()).Get(), &tn)
				return err
			}))
			tdb.Exec(t, "DROP TABLE test.test_table")
			tdb.Exec(t, createTableBefore)
			expectedDescriptor.Store(desc)
			enableUpdateEventCh.Set(true)

			// Run the migration, expecting failure.
			t.Log("trying migration, expecting to fail")
			// Channel to wait for the migration job to complete.
			finishChan := make(chan struct{})
			go upgrades.UpgradeToVersion(
				t, sqlDB, endCV, finishChan, true, /* expectError */
			)

			var migJobID jobspb.JobID
			// Intercept the target schema-change job and get migration-job's ID.
			t.Log("intercepting the schema job")
			for {
				e := <-updateEventChan
				// The migration job creates schema-change jobs. Therefore, we are guaranteed
				// to get the migration-job's ID before canceling the job later on.
				if e.orig.Payload.Type() == jobspb.TypeMigration {
					migJobID = e.orig.ID
					e.errChan <- nil
					continue
				}
				schemaQuery := strings.Replace(e.orig.Payload.Description, "test.public.test_table", "test.test_table", -1)
				testQuery := removeSpaces(test.query)
				testQuery = strings.ReplaceAll(testQuery, ":::STRING", "")
				if testQuery == schemaQuery {
					// Intercepted the target schema-change.
					schemaEvent = e
					t.Logf("intercepted schema change job: %v", e.orig.ID)
					break
				}
				// Ignore all other job updates.
				e.errChan <- nil
			}
			// Cancel the migration job.
			t.Log("canceling the migration job")
			go cancelJob(t, ctx, s, migJobID)

			// Wait for the migration job to finish while preventing the intercepted
			// schema-change job from progressing.
			t.Log("waiting for the migration job to finish.")
			testutils.SucceedsSoon(t, func() error {
				for {
					select {
					case <-finishChan:
						return nil
					case e := <-updateEventChan:
						e.errChan <- nil
					default:
						return errors.Errorf("waiting for the migration job to finish.")
					}
				}
			})

			// Let all jobs to continue until test's completion, except the intercepted
			// schema-change job that we resume later on.
			go func() {
				for {
					var e updateEvent
					select {
					case e = <-updateEventChan:
						close(e.errChan)
					case <-cancelCtx.Done():
						return
					}
				}
			}()

			// Restart the migration job.
			t.Log("retrying migration, expecting to succeed")
			go upgrades.UpgradeToVersion(t, sqlDB, endCV, finishChan, false /* expectError */)

			// Wait until the new migration job observes an existing mutation job.
			if test.waitForMigrationRestart {
				t.Log("waiting for the migration job to observe a mutation")
				<-migrationWaitCh
			}

			t.Log("resuming the schema change job")
			// If configured so, mark the schema-change job to cancel.
			if test.cancelSchemaJob {
				cancelJob(t, ctx, s, schemaEvent.orig.ID)
			}
			// Resume the schema-change job and all other jobs.
			schemaEvent.errChan <- nil

			// If canceled the job, wait for the job to finish.
			if test.cancelSchemaJob {
				t.Log("waiting for the schema job to reach the cancel status")
				waitUntilState(t, tdb, schemaEvent.orig.ID, jobs.StatusCanceled)
			}
			// Ensure all migrations complete.
			go func() {
				for {
					select {
					case <-migrationWaitCh:
					case <-cancelCtx.Done():
						return
					}
				}
			}()

			// Wait for the migration to complete, expecting success.
			t.Logf("waiting for the new migration job to complete.")
			testutils.SucceedsSoon(t, func() error {
				select {
				case <-finishChan:
					return nil
				default:
				}
				return errors.Errorf("waiting for the migration job to finish.")
			})
			if test.waitForMigrationRestart {
				// Ensure that we have observed the expected number of ignored schema change jobs.
				log.FlushFileSinks()
				entries, err := log.FetchEntriesFromFiles(
					0, math.MaxInt64, 10000,
					regexp.MustCompile("skipping.*operation as the schema change already exists."),
					log.WithFlattenedSensitiveData,
				)
				require.NoError(t, err)
				require.Len(t, entries, test.expectedSkipped)
			}
		})
	}
}

// cancelJob marks the given job as cancel-requested, leading the job to be
// canceled.
func cancelJob(
	t *testing.T, ctx context.Context, s serverutils.TestServerInterface, jobID jobspb.JobID,
) {
	err := s.InternalDB().(isql.DB).Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		// Using this way of canceling because the migration job us non-cancelable.
		// Canceling in this way skips the check.
		return s.JobRegistry().(*jobs.Registry).UpdateJobWithTxn(
			ctx, jobID, txn, false /* useReadLock */, func(
				txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater,
			) error {
				ju.UpdateStatus(jobs.StatusCancelRequested)
				return nil
			})
	})
	assert.NoError(t, err)
}

// waitUntilState waits until the specified job reaches to given state.
func waitUntilState(
	t *testing.T, tdb *sqlutils.SQLRunner, jobID jobspb.JobID, expectedStatus jobs.Status,
) {
	testutils.SucceedsSoon(t, func() error {
		var status jobs.Status
		tdb.QueryRow(t,
			"SELECT status FROM system.jobs WHERE id = $1", jobID,
		).Scan(&status)
		if status == expectedStatus {
			return nil
		}
		return errors.Errorf(
			"waiting for job %v to reach status %v, current status is %v",
			jobID, expectedStatus, status)
	})
}

func removeSpaces(stmt string) string {
	stmt = strings.TrimSpace(regexp.MustCompile(`(\s+|;+)`).ReplaceAllString(stmt, " "))
	stmt = strings.ReplaceAll(stmt, "( ", "(")
	stmt = strings.ReplaceAll(stmt, " )", ")")
	return stmt
}
