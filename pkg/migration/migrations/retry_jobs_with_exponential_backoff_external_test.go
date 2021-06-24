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
	"github.com/cockroachdb/cockroach/pkg/migration/migrations"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
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

func TestMigrationWithFailures(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, test := range []struct {
		name   string
		status jobs.Status
		query  string
	}{
		{
			"fail num_run before update",
			jobs.StatusRunning,
			migrations.NumRunsQuery,
		},
		{
			"fail num_run after update",
			jobs.StatusSucceeded,
			migrations.NumRunsQuery,
		},
		{
			"fail last_run before update",
			jobs.StatusRunning,
			migrations.LastRunQuery,
		},
		{
			"fail last_run after update",
			jobs.StatusSucceeded,
			migrations.LastRunQuery,
		},
		{
			"fail index before update",
			jobs.StatusRunning,
			migrations.AddIndexQuery,
		},
		{
			"fail index after update",
			jobs.StatusSucceeded,
			migrations.AddIndexQuery,
		},
	} {
		ctx := context.Background()
		var s serverutils.TestServerInterface
		var injectedError atomic.Value
		injectedError.Store(false)
		beforeUpdate := func(orig, updated jobs.JobMetadata) error {
			fmt.Println("[SR] BU: ", orig.ID, orig.Status, orig.Payload.Type(), updated.Status)
			if orig.Payload.Type() == jobspb.TypeMigration {
				//migrationJob = orig.ID
				return nil
			}
			if orig.Payload.Description != migrations.NeutralizeSQL(test.query) ||
				injectedError.Load().(bool) ||
				updated.Status != test.status {
				return nil
			}
			injectedError.Store(true)
			fmt.Println("[SR] injecting error ...")
			assert.NoError(t, s.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				if err := txn.SetUserPriority(roachpb.MaxUserPriority); err != nil {
					return err
				}
				return s.JobRegistry().(*jobs.Registry).CancelRequested(ctx, txn, orig.ID)
			}))
			return nil
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
						BeforeUpdate: beforeUpdate,
					},
				},
			},
		}
		tc := testcluster.StartTestCluster(t, 1, clusterArgs)
		defer tc.Stopper().Stop(ctx)
		s = tc.Server(0)
		sqlDB := tc.ServerConn(0)
		tdb := sqlutils.MakeSQLRunner(sqlDB)

		// Inject the old copy of the descriptor.
		injectLegacyTable(t, ctx, s)
		// Validate that the jobs table has old schema.
		validateSchemaExists(t, ctx, s, sqlDB, false)
		// Run the migration, expecting failure.
		migrate(t, sqlDB, true, nil)
		// Run the migration again, expecting success.
		migrate(t, sqlDB, false, nil)
		// Validate that the jobs table has new schema.
		validateSchemaExists(t, ctx, s, sqlDB, true)
		// Make sure that jobs work by running a job.
		validateJobExec(t, tdb)
	}
}

func TestWaitForMutations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Test to ensure that the migration waits for existing mutation jobs to
	// reach a terminal state before it performs the migration.
	//
	// Start migration, which create schema change jobs. Intercept a schema change
	// jobs, pause the job, cancel the migration, restart the migration, ensure
	// that the migration is waiting, resume the schema-change job, leading to
	// test completion with successful migration.

	for _, test := range []struct {
		name                    string
		status                  jobs.Status
		query                   string
		waitForMigrationRestart bool
		cancelSchemaJob         bool
	}{
		{
			"fail index after update",
			jobs.StatusRunning,
			migrations.NumRunsQuery,
			true,
			false,
		},
	} {
		test.query = migrations.NeutralizeSQL(test.query)
		type updateEvent struct {
			orig, updated jobs.JobMetadata
			errChan       chan error
		}
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
						BeforeUpdate: beforeUpdate,
					},
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
		// Run the migration, expecting failure.
		log.Info(ctx, "[SR] Trying migration, expecting to fail")
		finishChan := make(chan struct{})
		go migrate(t, sqlDB, true, finishChan)

		var migJobID jobspb.JobID
		var schemaEvent updateEvent
		// Intercept the target schema-change job and get migration-job's ID.
		log.Info(ctx, "[SR] Intercepting the schema job")
		for {
			e := <-updateEventChan
			// The migration job creates schema-change jobs. Therefore, we are guaranteed
			// to get the migration-job's ID before cancelling the job later on.
			if e.orig.Payload.Type() == jobspb.TypeMigration {
				migJobID = e.orig.ID
				log.Infof(ctx, "[SR] Migration job ID: %v", migJobID)
			} else if e.orig.Payload.Description == test.query {
				schemaEvent = e
				log.Infof(ctx, "[SR] Intercepted schema change job: %v", e.orig.ID)
				break
			}
			e.errChan <- nil
		}
		// Cancel the migration job
		log.Info(ctx, "[SR] cancelling the migration job")
		cancelJob(t, ctx, s, migJobID)

		// several cases after canceling the migration
		// 1. Wait for the migration to resume and then continue
		// 2. Wait for the migration to resume and then fail
		// 3. continue without waiting for the migration to resume
		// 4. fail without waiting for the migration to resume

		// Wait for the migration job to finish.
		log.Info(ctx, "[SR] waiting for the migration job to finish.")
		testutils.SucceedsSoon(t, func() error {
			select {
			case <-finishChan:
				return nil
			case e := <-updateEventChan:
				e.errChan <- nil
			default:
			}
			return errors.Errorf("Waiting for the migration job to finish.")
		})

		// Restart the migration job.
		log.Info(ctx, "[SR] restarting the migration job")
		go migrate(t, sqlDB, true, finishChan)

		// Wait until the migration job restarts.
		log.Info(ctx, "[SR] retrying migration, expecting to succeed")
		if test.waitForMigrationRestart {
			for {
				e := <-updateEventChan
				e.errChan <- nil
				if e.orig.Payload.Type() == jobspb.TypeMigration && e.updated.Status == jobs.StatusRunning {
					break
				}
			}
		}

		// Resume the schema change.
		log.Info(ctx, "[SR] resuming the schema change jobs")
		schemaEvent.errChan <- nil
		done := make(chan struct{})
		go func() {
			for {
				select {
				case e := <-updateEventChan:
					e.errChan <- nil
				case <-done:
					break
				}
			}
		}()

		// Let all jobs to finish.
		log.Infof(ctx, "[SR] waiting for the new migration job to complete.")
		testutils.SucceedsSoon(t, func() error {
			select {
			case <-finishChan:
				return nil
			default:
			}
			return errors.Errorf("Waiting for the migration job to finish.")
		})

		// Validate that the jobs table has new schema.
		validateSchemaExists(t, ctx, s, sqlDB, true)
		// Make sure that jobs work by running a job.
		validateJobExec(t, tdb)
		done <- struct{}{}
	}
}

func cancelJob(
	t *testing.T, ctx context.Context, s serverutils.TestServerInterface, jobID jobspb.JobID,
) {
	assert.NoError(t, s.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		if err := txn.SetUserPriority(roachpb.MaxUserPriority); err != nil {
			return err
		}
		log.Infof(ctx, "[SR] cancelling job: %v", jobID)
		//return s.JobRegistry().(*jobs.Registry).CancelRequested(ctx, txn, jobID)
		return s.JobRegistry().(*jobs.Registry).UpdateJobWithTxn(
			ctx, jobID, txn, func(txn *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater,
			) error {
				ju.UpdateStatus(jobs.StatusCancelRequested)
				log.Infof(ctx, "[SR] updated the status to cancel-requested for job %v", md.ID)
				return nil
			})
	}))
}

func migrate(t *testing.T, sqlDB *gosql.DB, expectError bool, finished chan struct{}) {
	defer func() {
		fmt.Println("[SR] migration finished")
		finished <- struct{}{}
	}()
	fmt.Println("[SR] Expecting migration to fail: ", expectError)
	_, err := sqlDB.Exec(`SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.RetryJobsWithExponentialBackoff).String())
	if expectError {
		require.Error(t, err)
		return
	}
	require.NoError(t, err)
}

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

/**
FIXME [SR]
- Test failing the jobs before they start, using BeforeResume in schemachanger
- Test failing the jobs after they succeed, using BeforeUpdate in jobs
- Test failing a migration but succeeding schema changes
- Test do we wait for a mutation job to complete or not
- Test whether the schema changes are idempotent or not
*/

func validateSchemaExists(
	t *testing.T,
	ctx context.Context,
	s serverutils.TestServerInterface,
	sqlDB *gosql.DB,
	expectExists bool,
) {
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

	table := jobsTable(t, ctx, s)
	str := "not have"
	if expectExists {
		str = "have"
	}
	for _, schema := range [...]struct {
		name         string
		validationFn func(catalog.TableDescriptor, string) (bool, error)
	}{
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
