// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migrations_test

import (
	"context"
	gosql "database/sql"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/migration/migrations"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestExponentialBackoffMigration tests modification of system.jobs table
// during migration. It does not test the migration success during failures.
func TestExponentialBackoffMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: 1,
					BinaryVersionOverride: clusterversion.ByKey(
						clusterversion.RetryJobsWithExponentialBackoff - 1),
				},
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			},
		},
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, clusterArgs)
	defer tc.Stopper().Stop(ctx)
	s := tc.Server(0)
	sqlDB := tc.ServerConn(0)
	tdb := sqlutils.MakeSQLRunner(sqlDB)

	// Inject the old copy of the descriptor.
	injectLegacyTable(t, ctx, s)
	// Validate that the jobs table has old schema.
	validateSchemaExists(t, ctx, s, sqlDB, false)
	// Run the migration.
	migrate(t, sqlDB, false, nil)
	// Validate that the jobs table has new schema.
	validateSchemaExists(t, ctx, s, sqlDB, true)
	// Make sure that jobs work by running a job.
	runGcJob(t, tdb)
}

type updateEvent struct {
	orig, updated jobs.JobMetadata
	errChan       chan error
}

// TestMigrationWithFailures tests modification of system.jobs table during
// migration with different failures. It tests the system behavior with failure
// combinations of the migration job and schema-change jobs at different stages
// in their progress.
func TestMigrationWithFailures(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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

	for _, test := range []struct {
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
	}{
		{
			name:                    "adding columns",
			query:                   migrations.AddColsQuery,
			waitForMigrationRestart: false, // Does not matter.
			cancelSchemaJob:         false, // Does not matter.
			expectedSkipped:         0,     // Will be ignored.
		},
		{
			name:                    "adding index",
			query:                   migrations.AddIndexQuery,
			waitForMigrationRestart: false, // Does not matter.
			cancelSchemaJob:         false, // Does not matter.
			expectedSkipped:         0,     // Will be ignored.
		},
		{
			name:                    "fail adding columns",
			query:                   migrations.AddColsQuery,
			waitForMigrationRestart: true, // Need to wait to observe failing schema change.
			cancelSchemaJob:         true, // To fail adding columns.
			expectedSkipped:         0,
		},
		{
			name:                    "fail adding index",
			query:                   migrations.AddIndexQuery,
			waitForMigrationRestart: true, // Need to wait to observe failing schema change.
			cancelSchemaJob:         true, // To fail adding index.
			expectedSkipped:         1,    // Columns must not be added again.
		},
		{
			name:                    "skip none",
			query:                   migrations.AddColsQuery,
			waitForMigrationRestart: true, // Need to wait to observe schema change and have correct expectedSkipped count.
			cancelSchemaJob:         true, // To fail adding index and skip adding column.
			expectedSkipped:         0,    // Both columns and index must be added.
		},
		{
			name:                    "skip adding columns",
			query:                   migrations.AddIndexQuery,
			waitForMigrationRestart: true, // Need to wait to observe schema change and have correct expectedSkipped count.
			cancelSchemaJob:         true, // To fail adding index and skip adding column.
			expectedSkipped:         1,    // Columns must not be added again.
		},
		{
			name:                    "skip adding columns and index",
			query:                   migrations.AddIndexQuery,
			waitForMigrationRestart: true,  // Need to wait to observe schema change and have correct expectedSkipped count.
			cancelSchemaJob:         false, // To fail adding index and skip adding column.
			expectedSkipped:         2,     // Both columns and index must not be added again.
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			// To intercept the schema-change and the migration job.
			updateEventChan := make(chan updateEvent)
			beforeUpdate := func(orig, updated jobs.JobMetadata) error {
				ue := updateEvent{
					orig:    orig,
					updated: updated,
					errChan: make(chan error),
				}
				updateEventChan <- ue
				return <-ue.errChan
			}

			var schemaEvent updateEvent
			migrationWaitCh := make(chan struct{})
			beforeMutationWait := func(jobID jobspb.JobID) {
				if !test.waitForMigrationRestart || jobID != schemaEvent.orig.ID {
					return
				}
				migrationWaitCh <- struct{}{}
			}

			// Number of schema-change jobs that are skipped.
			skippedCnt := int32(0)
			ignoredMutationObserver := func() {
				atomic.AddInt32(&skippedCnt, 1)
			}

			shortInterval := 2 * time.Millisecond
			clusterArgs := base.TestClusterArgs{
				ServerArgs: base.TestServerArgs{
					Knobs: base.TestingKnobs{
						Server: &server.TestingKnobs{
							DisableAutomaticVersionUpgrade: 1,
							BinaryVersionOverride: clusterversion.ByKey(
								clusterversion.RetryJobsWithExponentialBackoff - 1),
						},
						JobsTestingKnobs: &jobs.TestingKnobs{
							IntervalOverrides: jobs.TestingIntervalOverrides{
								Adopt:  &shortInterval,
								Cancel: &shortInterval,
							},
							BeforeUpdate: beforeUpdate,
						},
						MigrationManager: &migration.TestingKnobs{
							BeforeWaitInRetryJobsWithExponentialBackoffMigration: beforeMutationWait,
							SkippedMutation: ignoredMutationObserver,
						},
					},
				},
			}
			tc := testcluster.StartTestCluster(t, 1, clusterArgs)
			defer tc.Stopper().Stop(ctx)
			s := tc.Server(0)
			sqlDB := tc.ServerConn(0)
			tdb := sqlutils.MakeSQLRunner(sqlDB)

			tdb.Exec(t, "SET CLUSTER SETTING jobs.registry.interval.gc = '2ms'")
			// Inject the old copy of the descriptor.
			injectLegacyTable(t, ctx, s)
			// Validate that the jobs-table has old schema.
			validateSchemaExists(t, ctx, s, sqlDB, false)
			// Run the migration, expecting failure.
			t.Log("trying migration, expecting to fail")
			// Channel to wait for the migration job to complete.
			finishChan := make(chan struct{})
			go migrate(t, sqlDB, true, finishChan)

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
				schemaQuery := strings.Replace(e.orig.Payload.Description, "system.public.jobs", "system.jobs", -1)
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

			// Channel to finish the goroutine when the test completes.
			done := make(chan struct{})
			// Let all jobs to continue until test's completion, except the intercepted
			// schema-change job that we resume later on.
			go func() {
				for {
					select {
					case e := <-updateEventChan:
						e.errChan <- nil
					case <-done:
						return
					}
				}
			}()

			// Restart the migration job.
			t.Log("retrying migration, expecting to succeed")
			go migrate(t, sqlDB, false, finishChan)

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
				require.Equal(t, int32(test.expectedSkipped), atomic.LoadInt32(&skippedCnt))
			}

			// Validate that the jobs table has new schema.
			validateSchemaExists(t, ctx, s, sqlDB, true)
			done <- struct{}{}
			validateJobRetries(t, tdb, updateEventChan)
		})
	}
}

// cancelJob marks the given job as cancel-requested, leading the job to be
// canceled.
func cancelJob(
	t *testing.T, ctx context.Context, s serverutils.TestServerInterface, jobID jobspb.JobID,
) {
	err := s.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// Using this way of canceling because the migration job us non-cancelable.
		// Canceling in this way skips the check.
		return s.JobRegistry().(*jobs.Registry).UpdateJobWithTxn(
			ctx, jobID, txn, false /* useReadLock */, func(txn *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater,
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

// migrate runs cluster migration by changing the 'version' cluster setting.
func migrate(t *testing.T, sqlDB *gosql.DB, expectError bool, done chan struct{}) {
	defer func() {
		if done != nil {
			done <- struct{}{}
		}
	}()
	_, err := sqlDB.Exec(`SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.RetryJobsWithExponentialBackoff).String())
	if expectError {
		assert.Error(t, err)
		return
	}
	assert.NoError(t, err)
}

// injectLegacyTable overwrites the existing table descriptor with the previous
// table descriptor.
func injectLegacyTable(t *testing.T, ctx context.Context, s serverutils.TestServerInterface) {
	err := s.CollectionFactory().(*descs.CollectionFactory).Txn(
		ctx,
		s.InternalExecutor().(sqlutil.InternalExecutor),
		s.DB(),
		func(ctx context.Context, txn *kv.Txn, descriptors *descs.Collection) error {
			id := systemschema.JobsTable.GetID()
			tab, err := descriptors.GetMutableTableByID(ctx, txn, id, tree.ObjectLookupFlagsWithRequired())
			if err != nil {
				return err
			}
			builder := tabledesc.NewBuilder(deprecatedDescriptor())
			require.NoError(t, builder.RunPostDeserializationChanges(ctx, nil))
			tab.TableDescriptor = builder.BuildCreatedMutableTable().TableDescriptor
			tab.Version = tab.ClusterVersion.Version + 1
			return descriptors.WriteDesc(ctx, false, tab, txn)
		})
	require.NoError(t, err)
}

// validateSchemaExists validates whether the schema-changes of the system.jobs
// table exist or not.
func validateSchemaExists(
	t *testing.T,
	ctx context.Context,
	s serverutils.TestServerInterface,
	sqlDB *gosql.DB,
	expectExists bool,
) {
	// First validate by reading the columns and the index.
	for _, stmt := range []string{
		"SELECT last_run, num_runs FROM system.jobs LIMIT 0",
		"SELECT num_runs, last_run, claim_instance_id from system.jobs@jobs_run_stats_idx LIMIT 0",
	} {
		_, err := sqlDB.Exec(stmt)
		if expectExists {
			require.NoError(
				t, err, "expected schema to exist, but unable to query it, using statement: %s", stmt,
			)
		} else {
			require.Error(
				t, err, "expected schema to not exist, but queried it successfully, using statement: %s", stmt,
			)
		}
	}

	// Manually verify the table descriptor.
	storedTable := getJobsTable(t, ctx, s)
	jTable := systemschema.JobsTable
	str := "not have"
	if expectExists {
		str = "have"
	}
	for _, schema := range [...]struct {
		name         string
		validationFn func(catalog.TableDescriptor, catalog.TableDescriptor, string) (bool, error)
	}{
		{"num_runs", migrations.HasBackoffCols},
		{"last_run", migrations.HasBackoffCols},
		{"jobs_run_stats_idx", migrations.HasBackoffIndex},
	} {
		updated, err := schema.validationFn(storedTable, jTable, schema.name)
		require.NoError(t, err)
		require.Equal(t, expectExists, updated,
			"expected jobs table to %s %s", str, schema)
	}
}

// getJobsTable returns system.jobs table descriptor, reading it from storage.
func getJobsTable(
	t *testing.T, ctx context.Context, s serverutils.TestServerInterface,
) catalog.TableDescriptor {
	var table catalog.TableDescriptor
	// Retrieve the jobs table.
	err := s.CollectionFactory().(*descs.CollectionFactory).Txn(
		ctx,
		s.InternalExecutor().(sqlutil.InternalExecutor),
		s.DB(),
		func(ctx context.Context, txn *kv.Txn, descriptors *descs.Collection) (err error) {
			table, err = descriptors.GetImmutableTableByID(ctx, txn, keys.JobsTableID, tree.ObjectLookupFlags{
				CommonLookupFlags: tree.CommonLookupFlags{
					AvoidCached: true,
					Required:    true,
				},
			})
			return err
		})
	require.NoError(t, err)
	return table
}

// runGcJob creates and alters a dummy table to trigger jobs machinery,
// which validates its working.
func runGcJob(t *testing.T, tdb *sqlutils.SQLRunner) {
	tdb.Exec(t, "CREATE TABLE foo (i INT PRIMARY KEY)")
	tdb.Exec(t, "ALTER TABLE foo CONFIGURE ZONE USING gc.ttlseconds = 1;")
	tdb.Exec(t, "DROP TABLE foo CASCADE;")
	var jobID int64
	tdb.QueryRow(t, `
SELECT job_id
  FROM [SHOW JOBS]
 WHERE job_type = 'SCHEMA CHANGE GC' AND description LIKE '%foo%';`,
	).Scan(&jobID)
	var status jobs.Status
	tdb.QueryRow(t,
		"SELECT status FROM [SHOW JOB WHEN COMPLETE $1]", jobID,
	).Scan(&status)
	require.Equal(t, jobs.StatusSucceeded, status)
}

func validateJobRetries(t *testing.T, tdb *sqlutils.SQLRunner, eventCh chan updateEvent) {
	tdb.Exec(t, "SET CLUSTER SETTING jobs.registry.retry.initial_delay = '2ms'")
	tdb.Exec(t, "SET CLUSTER SETTING jobs.registry.retry.max_delay = '10ms'")
	done := make(chan struct{})
	go func() {
		// Fail a GC job once and then let it succeed.
		var failed atomic.Value
		failed.Store(false)
		var ev updateEvent
		for {
			// eventCh receives events in the BeforeUpdate hook.
			select {
			case ev = <-eventCh:
			case <-done:
				return
			}
			// If not a schema-change GC job, let it run.
			if ev.orig.Payload.Type() != jobspb.TypeSchemaChangeGC {
				ev.errChan <- nil
				continue
			}
			if ev.updated.Status == jobs.StatusSucceeded {
				// If the job is succeeding, it must have been retried once, and the
				// the number of retries is populated from the jobs table.
				assert.Equal(t, 1, ev.orig.RunStats.NumRuns)
			}
			if failed.Load().(bool) ||
				ev.updated.Status != jobs.StatusRunning {
				ev.errChan <- nil
				continue
			}
			failed.Store(true)
			ev.errChan <- jobs.MarkAsRetryJobError(errors.New("failing job to retry"))
		}
	}()
	runGcJob(t, tdb)
	done <- struct{}{}
}

func removeSpaces(stmt string) string {
	stmt = strings.TrimSpace(regexp.MustCompile(`(\s+|;+)`).ReplaceAllString(stmt, " "))
	stmt = strings.ReplaceAll(stmt, "( ", "(")
	stmt = strings.ReplaceAll(stmt, " )", ")")
	return stmt
}

// deprecatedDescriptor returns the system.jobs table descriptor that was being used
// before adding two new columns and an index in the current version.
func deprecatedDescriptor() *descpb.TableDescriptor {
	uniqueRowIDString := "unique_rowid()"
	nowString := "now():::TIMESTAMP"
	pk := func(name string) descpb.IndexDescriptor {
		return descpb.IndexDescriptor{
			Name:                tabledesc.PrimaryKeyIndexName,
			ID:                  1,
			Unique:              true,
			KeyColumnNames:      []string{name},
			KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
			KeyColumnIDs:        []descpb.ColumnID{1},
		}
	}

	return &descpb.TableDescriptor{
		Name:                    "jobs",
		ID:                      keys.JobsTableID,
		ParentID:                keys.SystemDatabaseID,
		UnexposedParentSchemaID: keys.PublicSchemaID,
		Version:                 1,
		Columns: []descpb.ColumnDescriptor{
			{Name: "id", ID: 1, Type: types.Int, DefaultExpr: &uniqueRowIDString},
			{Name: "status", ID: 2, Type: types.String},
			{Name: "created", ID: 3, Type: types.Timestamp, DefaultExpr: &nowString},
			{Name: "payload", ID: 4, Type: types.Bytes},
			{Name: "progress", ID: 5, Type: types.Bytes, Nullable: true},
			{Name: "created_by_type", ID: 6, Type: types.String, Nullable: true},
			{Name: "created_by_id", ID: 7, Type: types.Int, Nullable: true},
			{Name: "claim_session_id", ID: 8, Type: types.Bytes, Nullable: true},
			{Name: "claim_instance_id", ID: 9, Type: types.Int, Nullable: true},
		},
		NextColumnID: 10,
		Families: []descpb.ColumnFamilyDescriptor{
			{
				Name:        "fam_0_id_status_created_payload",
				ID:          0,
				ColumnNames: []string{"id", "status", "created", "payload", "created_by_type", "created_by_id"},
				ColumnIDs:   []descpb.ColumnID{1, 2, 3, 4, 6, 7},
			},
			{
				Name:            "progress",
				ID:              1,
				ColumnNames:     []string{"progress"},
				ColumnIDs:       []descpb.ColumnID{5},
				DefaultColumnID: 5,
			},
			{
				Name:        "claim",
				ID:          2,
				ColumnNames: []string{"claim_session_id", "claim_instance_id"},
				ColumnIDs:   []descpb.ColumnID{8, 9},
			},
		},
		NextFamilyID: 3,
		PrimaryIndex: pk("id"),
		Indexes: []descpb.IndexDescriptor{
			{
				Name:                "jobs_status_created_idx",
				ID:                  2,
				Unique:              false,
				KeyColumnNames:      []string{"status", "created"},
				KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC, descpb.IndexDescriptor_ASC},
				KeyColumnIDs:        []descpb.ColumnID{2, 3},
				KeySuffixColumnIDs:  []descpb.ColumnID{1},
				Version:             descpb.StrictIndexColumnIDGuaranteesVersion,
			},
			{
				Name:                "jobs_created_by_type_created_by_id_idx",
				ID:                  3,
				Unique:              false,
				KeyColumnNames:      []string{"created_by_type", "created_by_id"},
				KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC, descpb.IndexDescriptor_ASC},
				KeyColumnIDs:        []descpb.ColumnID{6, 7},
				StoreColumnIDs:      []descpb.ColumnID{2},
				StoreColumnNames:    []string{"status"},
				KeySuffixColumnIDs:  []descpb.ColumnID{1},
				Version:             descpb.StrictIndexColumnIDGuaranteesVersion,
			},
		},
		NextIndexID:    4,
		Privileges:     descpb.NewCustomSuperuserPrivilegeDescriptor(privilege.ReadWriteData, security.NodeUserName()),
		FormatVersion:  descpb.InterleavedFormatVersion,
		NextMutationID: 1,
	}
}
