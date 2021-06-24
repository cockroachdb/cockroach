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
	"fmt"
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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
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
	validateJobExec(t, tdb)
}

// TestMigrationWithFailures tests modification of system.jobs table during
// migration with different failures. It tests the system behavior with failure
// combinations of the migration job and schema-change jobs at different stages
// in their progress. Particularly, it injects migration job failure when the
// schema-change job moves to running state and succeeded state. Both scenarios
// are tested with and without the schema-change job to fail, combined with
// the scenarios that the restarted migration job observes the ongoing schema-change
// or not.
func TestMigrationWithFailures(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Start migration, which create schema change jobs. Intercept a schema change
	// jobs, pause the job, cancel the migration, restart the migration, ensure
	// that the migration is waiting, resume the schema-change job, leading to
	// test completion with successful migration.

	// The tests follows the following procedure.
	//
	// Inject the old table descriptor and ensure that the system is using the
	// deprecated jobs-table.
	//
	// Start migration, which initiates three schema-change jobs one by one. Test
	// the system for each schema-change job separately. We will cancel this
	// migration later on, causing it to fail.
	//
	// Intercept the target schema-change job, preventing the job from progressing.
	// We are performing three changes in the table, adding two columns and one index.
	// The interception can be at the beginning of the job, when it moves to
	// 'running' state, or at the end, when it moves to 'succeeded' state. The idea
	// is that we want to fail the job and the migration at different stages of
	// jobs-table descriptor update.
	//
	// Cancel the migration job, causing the migration to revert and fail.
	//
	// Wait for the cancelled migration-job to finish, expecting its failure.
	//
	// Depending on the test config, wait for the migration job, which we start
	// later on, to reach to its running state. If we wait, the migration job is
	// expected to observe the ongoing schema-change job and wait for the job to
	// complete.
	//
	// Depending on the test config, cancel the ongoing schema-change job, causing
	// it to practice a schema-change job failure.
	//
	// Let all jobs to freely progress from this point on, expecting the migration
	// to complete without errors. If we cancelled the schema-job, expect it to rerun
	// as part of the migration. Otherwise, expect the schema-change to be ignored
	// during the migration. We verify the number of ignored schema-change jobs
	// in the test.
	//
	// Validate that the schema changes are in effect by reading the new columns
	// and the index.
	//
	// Validate that the jobs subsystem is running without errors by running a
	// sample job.

	type testConf struct {
		// Test identifier.
		name string
		// Job status when the job is intercepted while transitioning to the intercepted status.
		status jobs.Status
		// Target schema-change in the jobs table, which will be intercepted.
		query string
		// Whether to wait for the migration to restart after failure before releasing
		// the schema-change job.
		waitForMigrationRestart bool
		// Whether to cancel the intercepted schema-change job or not.
		cancelSchemaJob bool
		// Expected number of schema-change jobs to be skipped during migration.
		skippedMutations int
	}

	var tests []testConf
	for _, waitForMigration := range []bool{true, false} {
		for _, cancelSchemaJob := range []bool{true, false} {
			for _, interceptAtStatus := range []jobs.Status{jobs.StatusRunning, jobs.StatusSucceeded} {
				for runOrder, schemaQuery := range []string{
					// Match this order with the order of schema-change commands in the
					// the migration.
					migrations.NumRunsQuery,
					migrations.LastRunQuery,
					migrations.AddIndexQuery,
				} {
					// The number of schema-changes skipped depends on the number of already
					// completed schema changes, which depends on the order of their execution.
					skipCnt := runOrder
					// if we do not cancel the ongoing schema-change job, the schema-change
					// will succeed and the restarted migration will skip that schema-change.
					if !cancelSchemaJob {
						skipCnt++
					}
					tests = append(tests, testConf{
						fmt.Sprintf(
							"%v-%v-%v-%v",
							waitForMigration, cancelSchemaJob, interceptAtStatus, runOrder),
						interceptAtStatus,
						schemaQuery,
						waitForMigration,
						cancelSchemaJob,
						skipCnt,
					})
				}
			}
		}
	}

	for _, test := range tests {
		ctx := context.Background()
		log.Infof(ctx, "running test %s", test.name)
		test.query = migrations.NeutralizeSQL(test.query)
		type updateEvent struct {
			orig, updated jobs.JobMetadata
			errChan       chan error
		}
		// To intercept the schema-change and the migration job.
		updateEventChan := make(chan updateEvent)
		beforeUpdate := func(orig, updated jobs.JobMetadata) error {
			ue := updateEvent{
				orig:    orig,
				updated: updated,
				errChan: make(chan error),
			}
			updateEventChan <- ue
			err := <-ue.errChan
			return err
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

		shortInterval := 10 * time.Millisecond
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
						BeforeUpdate:     beforeUpdate,
						DisableForUpdate: true,
					},
					MigrationManager: &migration.TestingKnobs{
						BeforeWaitingForMutation: beforeMutationWait,
						SkippedMutation:          ignoredMutationObserver,
					},
				},
			},
		}
		tc := testcluster.StartTestCluster(t, 1, clusterArgs)
		defer tc.Stopper().Stop(ctx)
		s := tc.Server(0)
		sqlDB := tc.ServerConn(0)
		tdb := sqlutils.MakeSQLRunner(sqlDB)

		// Inject the old copy of the descriptor.
		injectLegacyTable(t, ctx, s)
		// Validate that the jobs-table has old schema.
		validateSchemaExists(t, ctx, s, sqlDB, false)
		// Run the migration, expecting failure.
		log.Info(ctx, "trying migration, expecting to fail")
		// Channel to wait for the migration job to complete.
		finishChan := make(chan struct{})
		go migrate(t, sqlDB, true, finishChan)

		var migJobID jobspb.JobID
		// Intercept the target schema-change job and get migration-job's ID.
		log.Info(ctx, "intercepting the schema job")
		for {
			e := <-updateEventChan
			// The migration job creates schema-change jobs. Therefore, we are guaranteed
			// to get the migration-job's ID before cancelling the job later on.
			if e.orig.Payload.Type() == jobspb.TypeMigration {
				migJobID = e.orig.ID
			} else if e.orig.Payload.Description == test.query {
				// Intercepted the target schema-change.
				schemaEvent = e
				log.Infof(ctx, "intercepted schema change job: %v", e.orig.ID)
				break
			}
			// Ignore all other job updates.
			e.errChan <- nil
		}
		// Cancel the migration job.
		log.Info(ctx, "cancelling the migration job")
		go cancelJob(t, ctx, s, migJobID)

		// Wait for the migration job to finish while preventing the intercepted
		// schema-change job from progressing.
		log.Info(ctx, "waiting for the migration job to finish.")
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
		log.Info(ctx, "retrying migration, expecting to succeed")
		go migrate(t, sqlDB, false, finishChan)

		// Wait until the new migration job observes an existing mutation job.
		if test.waitForMigrationRestart {
			log.Info(ctx, "waiting for the migration job to observe a mutation")
			<-migrationWaitCh
		}

		log.Info(ctx, "resuming the schema change job")
		// If configured so, mark the schema-change job to cancel.
		if test.cancelSchemaJob {
			cancelJob(t, ctx, s, schemaEvent.orig.ID)
		}
		// Resume the schema-change job and all other jobs.
		schemaEvent.errChan <- nil

		// If cancelled the job, wait for the job to finish.
		if test.cancelSchemaJob {
			log.Info(ctx, "waiting for the schema job to reach the cancel status")
			waitUntilState(t, tdb, schemaEvent.orig.ID, jobs.StatusCanceled)
		}

		// Wait for the migration to complete, expecting success.
		log.Infof(ctx, "waiting for the new migration job to complete.")
		testutils.SucceedsSoon(t, func() error {
			select {
			case <-finishChan:
				return nil
			default:
			}
			return errors.Errorf("aiting for the migration job to finish.")
		})

		// Validate that the jobs table has new schema.
		validateSchemaExists(t, ctx, s, sqlDB, true)

		// Make sure that jobs work by running a job.
		validateJobExec(t, tdb)

		// Make sure that we have observed the expected number of ignored schema change
		// jobs.
		require.Equal(t, int32(test.skippedMutations), atomic.LoadInt32(&skippedCnt))
		done <- struct{}{}
	}
}

// cancelJob marks the given job as cancel-requested, leading the job to be
// cancelled.
func cancelJob(
	t *testing.T, ctx context.Context, s serverutils.TestServerInterface, jobID jobspb.JobID,
) {
	err := s.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		log.Infof(ctx, "cancelling job: %v", jobID)
		// Using this way of cancelling because the migration job us non-cancellable.
		// Cancelling in this way skips the check.
		return s.JobRegistry().(*jobs.Registry).UpdateJobWithTxn(
			ctx, jobID, txn, func(txn *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater,
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
	err := descs.Txn(
		ctx,
		s.ClusterSettings(),
		s.LeaseManager().(*lease.Manager),
		s.InternalExecutor().(sqlutil.InternalExecutor),
		s.DB(),
		func(
			ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
		) error {
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
	table := jobsTable(t, ctx, s)
	str := "not have"
	if expectExists {
		str = "have"
	}
	for _, schema := range [...]struct {
		name         string
		validationFn func(catalog.TableDescriptor, string) (bool, error)
	}{
		// TODO(sajjad): To discuss: Shouldn't we use an independnt way to verify
		// instead of reusing HasBackoffCols and HasBackoffIndex functions from
		// the migration itself? If not, I think we should at least write a unit
		// test to validate the correctness of these functions.
		{"num_runs", migrations.HasBackoffCols},
		{"last_run", migrations.HasBackoffCols},
		{"jobs_run_stats_idx", migrations.HasBackoffIndex},
	} {
		updated, err := schema.validationFn(table, schema.name)
		require.NoError(t, err)
		require.Equal(t, expectExists, updated,
			"expected jobs table to %s %s", str, schema)
	}
}

// jobsTable returns the system.jobs table descriptor, reading it from the
// storage.
func jobsTable(
	t *testing.T, ctx context.Context, s serverutils.TestServerInterface,
) catalog.TableDescriptor {
	var table catalog.TableDescriptor
	// Retrieve the jobs table.
	err := descs.Txn(ctx,
		s.ClusterSettings(),
		s.LeaseManager().(*lease.Manager),
		s.InternalExecutor().(sqlutil.InternalExecutor),
		s.DB(),
		func(
			ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
		) (err error) {
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

// validateJobExec creates and alters a dummy table to trigger jobs machinery,
// which validates its working.
func validateJobExec(t *testing.T, tdb *sqlutils.SQLRunner) {
	// TODO(sajjad): To discuss: The following job do not use the exponential
	// backoff parameters. Should we run a job that initially fails and then
	// retried, which will practice using the new columns, index, and exponential
	// backoff delays?
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
		NextIndexID: 4,
		Privileges: descpb.NewCustomSuperuserPrivilegeDescriptor(
			descpb.SystemAllowedPrivileges[keys.JobsTableID], security.NodeUserName()),
		FormatVersion:  descpb.InterleavedFormatVersion,
		NextMutationID: 1,
	}
}
