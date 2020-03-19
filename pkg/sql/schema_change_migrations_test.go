// Copyright 2020 The Cockroach Authors.
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
	gosql "database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/sqlmigrations"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type BlockState int

// These are the states that we want to block the 19.2 style schema change and
// ensure that it can be migrated properly when it is in that state.
const (
	BeforeBackfill BlockState = iota
	AfterBackfill
	AfterReversingMutations // Only used if the job was canceled.
	WaitingForGC            // Only applies to DROP INDEX, DROP TABLE, TRUNCATE TABLE.
)

type SchemaChangeType int

const (
	AddColumn SchemaChangeType = iota
	DropColumn
	AddIndex
	DropIndex
	AddConstraint
	DropConstraint
	CreateTable
	DropTable
	TruncateTable
)

const setup = `
CREATE DATABASE t;
USE t;
CREATE TABLE test (k INT PRIMARY KEY, v INT, INDEX k_idx (k), CONSTRAINT k_cons CHECK (k > 0));
INSERT INTO test VALUES (1, 2);
`

// runsBackfill is a set of schema change types that run a backfill.
var runsBackfill = map[SchemaChangeType]struct{}{
	AddColumn:  {},
	DropColumn: {},
	AddIndex:   {},
	DropIndex:  {},
}

func isDeletingTable(schemaChangeType SchemaChangeType) bool {
	return schemaChangeType == TruncateTable || schemaChangeType == DropTable
}

func checkBlockedSchemaChange(
	t *testing.T,
	runner *sqlutils.SQLRunner,
	shouldCancel bool,
	schemaChange string,
	schemaChangeType SchemaChangeType,
	blockState BlockState,
) {
	if blockState == WaitingForGC {
		// Earlier we turned the 20.1 GC job into a 19.2 schema change job. Delete
		// the original schema change job which is now succeeded, to avoid having
		// special cases later, since we rely heavily on the index of the job row in
		// the jobs table when verifying a job.
		//
		// First, though, we have to actually wait for the original job to become
		// Succeeded.
		runner.CheckQueryResultsRetry(t,
			"SELECT count(*) FROM [SHOW JOBS] WHERE job_type = 'SCHEMA CHANGE' AND status = 'succeeded'",
			[][]string{{"1"}},
		)
		rows := runner.QueryStr(
			t,
			"SELECT * FROM [SHOW JOBS] WHERE job_type = 'SCHEMA CHANGE' AND status = 'succeeded'",
		)
		jobID, _ := strconv.Atoi(rows[0][0])
		runner.Exec(t, "DELETE FROM system.jobs WHERE id = $1", jobID)
	}

	oldVersion := jobutils.GetJobFormatVersion(t, runner)
	require.Equal(t, jobspb.BaseFormatVersion, oldVersion)
	expStatus := jobs.StatusRunning
	if shouldCancel {
		expStatus = jobs.StatusReverting
	}
	if err := jobutils.VerifySystemJob(t, runner, 0, jobspb.TypeSchemaChange, expStatus, jobs.Record{
		Description:   schemaChange,
		Username:      security.RootUser,
		DescriptorIDs: getTableIDsUnderTest(schemaChangeType),
	}); err != nil {
		t.Fatal(err)
	}

	if !hadJobInOldVersion(schemaChangeType) {
		// Delete the job if it didn't have a schema change before.
		rows := runner.QueryStr(t, "SELECT * FROM [SHOW JOBS] WHERE job_type = 'SCHEMA CHANGE'")
		for _, job := range rows {
			jobID, _ := strconv.Atoi(job[0])
			runner.Exec(t, "DELETE FROM system.jobs WHERE id = $1", jobID)
		}
	}
}

// testSchemaChangeMigrations tests that a schema change can be migrated after
// being blocked in a certain state.
//
// 1. Create a 20.1 schema change.
// 2. Block the schema change at a certain point in its execution.
// 3. Mutate the job descriptor and table descriptor such that it appears as a
// 19.2 format job. These jobs will not be resumed anymore as 20.1 will refuse
// to run 19.2 jobs.
// 4. Verify that the job has been marked as a 19.2 job and is blocked.
// 5. Run the migration and wait for the migration to complete.
// 6. Ensure that the schema change completes.
func testSchemaChangeMigrations(
	t *testing.T,
	blockState BlockState,
	schemaChangeType SchemaChangeType,
	shouldCancel bool,
	schemaChange string,
) {
	ctx := context.Background()
	blockCh := make(chan struct{})
	migrationDoneCh := make(chan struct{})
	shouldBlockMigration := false
	blockFnErrChan := make(chan error, 1)
	runner, sqlDB, tc := setupServerAndStartSchemaChange(
		t,
		blockFnErrChan,
		blockState,
		schemaChangeType,
		shouldCancel,
		schemaChange,
		&shouldBlockMigration,
		blockCh,
		migrationDoneCh,
	)
	defer tc.Stopper().Stop(context.TODO())
	defer disableGCTTLStrictEnforcement(t, sqlDB)()

	log.Info(ctx, "waiting for all schema changes to block")
	<-blockCh
	log.Info(ctx, "all schema changes have blocked")

	close(blockFnErrChan)
	for err := range blockFnErrChan {
		if err != nil {
			t.Fatalf("%+v", err)
		}
	}

	checkBlockedSchemaChange(t, runner, shouldCancel, schemaChange, schemaChangeType, blockState)

	// Start the migrations.
	log.Info(ctx, "starting job migration")
	shouldBlockMigration = true
	migMgr := tc.Server(0).MigrationManager().(*sqlmigrations.Manager)
	if err := migMgr.StartSchemaChangeJobMigration(ctx); err != nil {
		t.Fatal(err)
	}

	log.Info(ctx, "waiting for migration to complete")
	<-migrationDoneCh
	shouldBlockMigration = false

	// TODO(pbardea): SHOW JOBS WHEN COMPLETE SELECT does not work on some schema
	// changes when canceling jobs, but querying until there are no jobs works.
	//runner.Exec(t, "SHOW JOBS WHEN COMPLETE SELECT job_id FROM [SHOW JOBS] WHERE (job_type = 'SCHEMA CHANGE' OR job_type = 'SCHEMA CHANGE GC')")
	// Wait until there are no more running schema changes.
	log.Info(ctx, "waiting for new schema change jobs to complete")
	runner.CheckQueryResultsRetry(t, "SELECT * FROM [SHOW JOBS] WHERE (job_type = 'SCHEMA CHANGE' OR job_type = 'SCHEMA CHANGE GC') AND NOT (status = 'succeeded' OR status = 'canceled')", [][]string{})
	log.Info(ctx, "done running new schema change jobs")

	verifySchemaChangeJobRan(t, runner, schemaChange, schemaChangeType, blockState, shouldCancel)
}

func setupServerAndStartSchemaChange(
	t *testing.T,
	errCh chan error,
	blockState BlockState,
	schemaChangeType SchemaChangeType,
	shouldCancel bool,
	schemaChange string,
	shouldBlockMigration *bool,
	blockCh, migrationDoneCh chan struct{},
) (*sqlutils.SQLRunner, *gosql.DB, serverutils.TestClusterInterface) {
	clusterSize := 3
	params, _ := tests.CreateTestServerParams()
	var runner *sqlutils.SQLRunner
	var kvDB *kv.DB
	var registry *jobs.Registry
	blockSchemaChanges := int32(0)
	setupTestingKnobs(t, errCh, &kvDB, blockState, schemaChangeType, shouldCancel, &registry, &params, &runner, &blockSchemaChanges, shouldBlockMigration, blockCh, migrationDoneCh)

	tc := serverutils.StartTestCluster(t, clusterSize,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs:      params,
		})
	sqlDB := tc.ServerConn(0)
	kvDB = tc.Server(0).DB()
	runner = sqlutils.MakeSQLRunner(sqlDB)
	registry = tc.Server(0).JobRegistry().(*jobs.Registry)

	ctx, cancel := context.WithCancel(context.Background())

	if _, err := sqlDB.Exec(setup); err != nil {
		t.Fatal(err)
	}

	runner.CheckQueryResultsRetry(t, "SELECT count(*) FROM [SHOW JOBS] WHERE job_type = 'SCHEMA CHANGE' AND NOT (status = 'succeeded' OR status = 'canceled')", [][]string{{"0"}})
	atomic.StoreInt32(&blockSchemaChanges, 1)

	bg := ctxgroup.WithContext(ctx)
	bg.Go(func() error {
		if _, err := sqlDB.ExecContext(ctx, schemaChange); err != nil {
			cancel()
			return err
		}
		return nil
	})
	// TODO(pbardea): Remove this magic 53.
	if _, err := addImmediateGCZoneConfig(sqlDB, sqlbase.ID(53)); err != nil {
		t.Fatal(err)
	}
	return runner, sqlDB, tc
}

// hasJobInOldVersion returns if a given schema change had a job in 19.2.
// Therefore these jobs could not be canceled in 19.2
func hadJobInOldVersion(schemaChangeType SchemaChangeType) bool {
	return schemaChangeType != CreateTable
}

// canBlockIfCanceled returns if a certain state (where we want to block the
// schema change) will be reached given if the job was canceled or not.
func canBlockIfCanceled(blockState BlockState, shouldCancel bool) bool {
	// States that are only valid when the job is canceled.
	if blockState == WaitingForGC {
		return !shouldCancel
	}
	if blockState == AfterReversingMutations {
		return shouldCancel
	}
	return true
}

// migrateJobToOldFormat updates the state of a job and table descriptor from
// it's 20.1 to its 19.2 representation. There is a separate implementation for
// GC jobs.
func migrateJobToOldFormat(
	kvDB *kv.DB, registry *jobs.Registry, jobID int64, schemaChangeType SchemaChangeType,
) error {
	ctx := context.Background()

	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")
	if schemaChangeType == CreateTable {
		tableDesc = sqlbase.GetTableDescriptor(kvDB, "t", "new_table")
	}

	if err := kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		job, err := registry.LoadJobWithTxn(ctx, jobID, txn)
		if err != nil {
			return err
		}
		return job.WithTxn(txn).Update(ctx, func(txn *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
			details := job.Details().(jobspb.SchemaChangeDetails)
			// Explicitly zero out these fields as they will be set to their 0 value
			// on 19.2 nodes.
			details.TableID = 0
			details.MutationID = 0
			details.FormatVersion = jobspb.BaseFormatVersion
			if isDeletingTable(schemaChangeType) {
				details.DroppedTables = []jobspb.DroppedTableDetails{
					{
						Name:   tableDesc.Name,
						ID:     tableDesc.ID,
						Status: jobspb.Status_DRAINING_NAMES,
					},
				}
			}

			progress := job.Progress()
			// TODO(pbardea): Probably want to change this to check on block state
			// being draining names.
			if isDeletingTable(schemaChangeType) {
				progress.RunningStatus = string(sql.RunningStatusDrainingNames)
			}

			md.Payload.Lease = nil
			md.Payload.Details = jobspb.WrapPayloadDetails(details)
			md.Progress = &progress
			ju.UpdatePayload(md.Payload)
			ju.UpdateProgress(md.Progress)
			return nil
		})
	}); err != nil {
		return err
	}

	// Update the table descriptor.
	tableDesc.Lease = &sqlbase.TableDescriptor_SchemaChangeLease{
		ExpirationTime: timeutil.Now().UnixNano(),
		NodeID:         roachpb.NodeID(0),
	}
	if schemaChangeType == TruncateTable {
		tableDesc.DropJobID = jobID
		// TODO(pbardea): When is drop time populated?
	}

	// Write the table descriptor back.
	if err := kvDB.Put(ctx, sqlbase.MakeDescMetadataKey(tableDesc.GetID()), sqlbase.WrapDescriptor(tableDesc)); err != nil {
		return err
	}
	return nil
}

// migrateGCJobToOldFormat converts a GC job created in 20.1 into a 19.2-style
// schema change job that is waiting for GC. This involves changing the type of
// the job details and progress.
//
// We could have gone back and set the original schema change job to Running,
// but then we'd have to update that job from inside the GC job testing knob
// function, which seems risky since we have no way of controlling that schema
// change job once it's eligible to be adopted.
func migrateGCJobToOldFormat(
	kvDB *kv.DB, registry *jobs.Registry, jobID int64, schemaChangeType SchemaChangeType,
) error {
	ctx := context.Background()

	if err := kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		job, err := registry.LoadJobWithTxn(ctx, jobID, txn)
		if err != nil {
			return err
		}
		return job.WithTxn(txn).Update(ctx, func(txn *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
			// Replace the details with an entirely new SchemaChangeDetails.
			details := jobspb.SchemaChangeDetails{
				FormatVersion: jobspb.BaseFormatVersion,
			}
			if isDeletingTable(schemaChangeType) {
				details.DroppedTables = []jobspb.DroppedTableDetails{
					{
						// TODO (lucy): Stop hard-coding these if possible. We can't get
						// these values from the table descriptor if we're dropping the
						// table, since at this point the table descriptor would have been
						// deleted.
						Name:   "test",
						ID:     53,
						Status: jobspb.Status_WAIT_FOR_GC_INTERVAL,
					},
				}
			}

			progress := jobspb.Progress{
				Details:       jobspb.WrapProgressDetails(jobspb.SchemaChangeProgress{}),
				RunningStatus: string(sql.RunningStatusWaitingGC),
			}

			md.Payload.Lease = nil
			md.Payload.Description = strings.TrimPrefix(md.Payload.Description, "GC for ")
			md.Payload.Details = jobspb.WrapPayloadDetails(details)
			md.Progress = &progress
			ju.UpdatePayload(md.Payload)
			ju.UpdateProgress(md.Progress)
			return nil
		})
	}); err != nil {
		return err
	}

	switch schemaChangeType {
	case DropTable:
		// There's no table descriptor to update, so we're done.
		return nil

	case DropIndex:
		// Since we write the GC job in a separate transaction from finalizing the
		// mutations on the table descriptor, there's a chance that we might see the
		// table descriptor before it's even been updated with the new GCMutations.
		// This seems like a potential problem. For now, we'll just hackily retry
		// until the new descriptor appears.
		var tableDesc *sql.TableDescriptor
		for r := retry.Start(base.DefaultRetryOptions()); r.Next(); {
			tableDesc = sqlbase.GetTableDescriptor(kvDB, "t", "test")
			if l := len(tableDesc.GCMutations); l == 1 {
				break
			}
		}

		// Update the table descriptor.
		tableDesc.Lease = &sqlbase.TableDescriptor_SchemaChangeLease{
			ExpirationTime: timeutil.Now().UnixNano(),
			NodeID:         roachpb.NodeID(0),
		}

		tableDesc.GCMutations[0].JobID = jobID
		tableDesc.GCMutations[0].DropTime = timeutil.Now().UnixNano()

		// Write the table descriptor back.
		if err := kvDB.Put(ctx, sqlbase.MakeDescMetadataKey(tableDesc.GetID()), sqlbase.WrapDescriptor(tableDesc)); err != nil {
			return err
		}

	default:
		return errors.Errorf("invalid schema change type: %d", schemaChangeType)
	}

	return nil
}

// Set up server testing args such that knobs are set to block and abandon any
// given schema change at a certain point. The "blocked" channel will be
// signaled when the schema change gets abandoned.
// The runner should only be used inside callback closures.
func setupTestingKnobs(
	t *testing.T,
	errCh chan error,
	db **kv.DB,
	blockState BlockState,
	schemaChangeType SchemaChangeType,
	shouldCancel bool,
	registryAddr **jobs.Registry,
	args *base.TestServerArgs,
	runnerAddr **sqlutils.SQLRunner,
	blockSchemaChanges *int32,
	blockMigration *bool,
	blockCh, migrationDoneCh chan struct{},
) {
	numJobs := 1
	if schemaChangeType == CreateTable {
		numJobs = 2
	}
	migratedCount := 0
	doneReverseMigration := false
	mu := syncutil.Mutex{}

	ranCancelCommand := false
	hasCanceled := int32(0)

	blockFn := func(jobID int64) error {
		mu.Lock()
		defer mu.Unlock()
		// These are instantiated between the time that the knob is set and when the
		// knob is run.
		runner := *runnerAddr
		if runner == nil {
			// Server hasn't started yet. This will be the case when running "updating
			// privilege" schema changes during startup.
			return nil
		}
		if atomic.LoadInt32(blockSchemaChanges) == 0 {
			return nil
		}
		kvDB := *db
		registry := *registryAddr

		// In the case we're canceling the job, this blockFn should only be called
		// after the OnFailOrCancel hook is called. At this point we know that the
		// job is actually canceled.
		atomic.StoreInt32(&hasCanceled, 1)

		if doneReverseMigration {
			// Already migrated all the jobs that we want to migrate to 19.2.
			// New jobs created after we migrated the original batch should be allowed
			// to continue.
			return nil
		} else {
			if blockState == WaitingForGC {
				if err := migrateGCJobToOldFormat(kvDB, registry, jobID, schemaChangeType); err != nil {
					errCh <- err
				}
			} else {
				if err := migrateJobToOldFormat(kvDB, registry, jobID, schemaChangeType); err != nil {
					errCh <- err
				}
			}
			migratedCount++
		}

		if migratedCount == numJobs {
			doneReverseMigration = true
			blockCh <- struct{}{}
		}

		// Return a retryable error so that the job doesn't make any progress past
		// this point. It should not get adopted since it has been marked as a 19.2
		// job.
		return jobs.NewRetryJobError("stop this job until cluster upgrade")
	}

	cancelFn := func(jobID int64) error {
		mu.Lock()
		defer mu.Unlock()
		// These are instantiated between the time that the knob is set and when the
		// knob is run.
		runner := *runnerAddr
		if runner == nil {
			return nil
		}

		if atomic.LoadInt32(&hasCanceled) == 1 {
			// The job has already been successfully canceled.
			return nil
		}

		if !ranCancelCommand {
			runner.Exec(t, `CANCEL JOB (
					SELECT job_id FROM [SHOW JOBS]
					WHERE
						job_id = $1
				)`, jobID)
			ranCancelCommand = true
		}

		// Don't allow the job to progress further than this knob until it has
		// actually been canceled	.
		return jobs.NewRetryJobError("retry until canceled")
	}

	knobs := &sql.SchemaChangerTestingKnobs{}
	gcKnobs := &sql.GCJobTestingKnobs{}

	if shouldCancel {
		if _, ok := runsBackfill[schemaChangeType]; ok {
			knobs.RunAfterBackfill = cancelFn
		} else {
			knobs.RunBeforeResume = cancelFn
		}
	}

	switch blockState {
	case BeforeBackfill:
		if shouldCancel {
			knobs.RunBeforeOnFailOrCancel = blockFn
		} else {
			knobs.RunBeforeResume = blockFn
		}
	case AfterBackfill:
		if shouldCancel {
			knobs.RunAfterOnFailOrCancel = blockFn
		} else {
			knobs.RunAfterBackfill = blockFn
		}
	case AfterReversingMutations:
		if !shouldCancel {
			t.Fatal("can only block after reversing mutations if the job is expected to be canceled")
		}
		knobs.RunAfterBackfill = cancelFn
		knobs.RunAfterMutationReversal = blockFn
	case WaitingForGC:
		if shouldCancel {
			t.Fatal("cannot block on waiting for GC if the job should also be canceled")
		}
		gcKnobs.RunBeforeResume = blockFn
	}

	args.Knobs.SQLSchemaChanger = knobs
	args.Knobs.SQLMigrationManager = &sqlmigrations.MigrationManagerTestingKnobs{
		AfterJobMigration: func() {
			if *blockMigration {
				migrationDoneCh <- struct{}{}
			}
		},
		AlwaysRunJobMigration: true,
	}
	args.Knobs.GCJob = gcKnobs
}

func getTestName(schemaChange SchemaChangeType, blockState BlockState, shouldCancel bool) string {
	stateNames := map[BlockState]string{
		BeforeBackfill:          "before-backfill",
		AfterBackfill:           "after-backfill",
		AfterReversingMutations: "after-reversing-mutations",
		WaitingForGC:            "waiting-for-gc",
	}
	schemaChangeName := map[SchemaChangeType]string{
		AddColumn:      "add-column",
		DropColumn:     "drop-column",
		AddIndex:       "add-index",
		DropIndex:      "drop-index",
		AddConstraint:  "add-constraint",
		DropConstraint: "drop-constraint",
		CreateTable:    "create-table",
		TruncateTable:  "truncate-table",
		DropTable:      "drop-table",
	}

	testName := fmt.Sprintf("%s-blocked-at-%s", schemaChangeName[schemaChange], stateNames[blockState])
	if shouldCancel {
		testName += "-canceled"
	}
	return testName
}

func verifySchemaChangeJobRan(
	t *testing.T,
	runner *sqlutils.SQLRunner,
	schemaChange string,
	schemaChangeType SchemaChangeType,
	blockState BlockState,
	didCancel bool,
) {
	expStatus := jobs.StatusSucceeded
	description := schemaChange
	if didCancel {
		expStatus = jobs.StatusCanceled
	}
	if schemaChangeType == CreateTable {
		description = "adding table 54"
	}
	if schemaChangeType != CreateTable {
		if err := jobutils.VerifySystemJob(t, runner, 0, jobspb.TypeSchemaChange, expStatus, jobs.Record{
			Description:   description,
			Username:      security.RootUser,
			DescriptorIDs: getTableIDsUnderTest(schemaChangeType),
		}); err != nil {
			t.Fatal(err)
		}
	}

	// Verify that the GC job exists and is in the correct state, if applicable.
	if blockState == WaitingForGC {
		if err := jobutils.VerifySystemJob(t, runner, 0, jobspb.TypeSchemaChangeGC, jobs.StatusSucceeded, jobs.Record{
			Description:   "GC for " + description,
			Username:      security.RootUser,
			DescriptorIDs: getTableIDsUnderTest(schemaChangeType),
		}); err != nil {
			t.Fatal(err)
		}
	} else {
		// For non-GC jobs, verify that the schema change job format version was
		// updated.
		newVersion := jobutils.GetJobFormatVersion(t, runner)
		require.Equal(t, jobspb.JobResumerFormatVersion, newVersion)
	}

	var expected [][]string
	switch schemaChangeType {
	case AddColumn:
		if didCancel {
			expected = [][]string{{"1", "2"}}
		} else {
			expected = [][]string{{"1", "2", "NULL"}}
		}
		rows := runner.QueryStr(t, "SELECT * FROM t.test")
		require.Equal(t, expected, rows)
	case DropColumn:
		if didCancel {
			expected = [][]string{{"1", "NULL"}}
		} else {
			expected = [][]string{{"1"}}
		}
		rows := runner.QueryStr(t, "SELECT * FROM t.test")
		require.Equal(t, expected, rows)
	case AddIndex:
		if didCancel {
			expected = [][]string{{"primary"}, {"k_idx"}}
		} else {
			expected = [][]string{{"primary"}, {"k_idx"}, {"v_idx"}}
		}
		rows := runner.QueryStr(t, "SELECT DISTINCT index_name FROM [SHOW INDEXES FROM t.test]")
		require.Equal(t, expected, rows)
	case DropIndex:
		if didCancel {
			expected = [][]string{{"primary"}, {"k_idx"}}
		} else {
			expected = [][]string{{"primary"}}
		}
		rows := runner.QueryStr(t, "SELECT DISTINCT index_name FROM [SHOW INDEXES FROM t.test]")
		require.Equal(t, expected, rows)
	case AddConstraint:
		if didCancel {
			expected = [][]string{{"k_cons"}, {"primary"}}
		} else {
			expected = [][]string{{"k_cons"}, {"primary"}, {"v_unq"}}
		}
		rows := runner.QueryStr(t, "SELECT constraint_name FROM [SHOW CONSTRAINTS FROM t.test] ORDER BY constraint_name")
		require.Equal(t, expected, rows)
	case DropConstraint:
		if didCancel {
			expected = [][]string{{"k_cons"}, {"primary"}}
		} else {
			expected = [][]string{{"primary"}}
		}
		rows := runner.QueryStr(t, "SELECT constraint_name FROM [SHOW CONSTRAINTS FROM t.test] ORDER BY constraint_name")
		require.Equal(t, expected, rows)
	case CreateTable:
		if didCancel {
			t.Fatal("cannot cancel create table")
		} else {
			expected = [][]string{{"new_table"}, {"test"}}
		}
		rows := runner.QueryStr(t, "SHOW TABLES FROM t")
		require.Equal(t, expected, rows)
	case TruncateTable:
		if didCancel {
			expected = [][]string{{"0"}}
		} else {
			expected = [][]string{{"0"}}
		}
		rows := runner.QueryStr(t, "SELECT count(*) FROM t.test")
		require.Equal(t, expected, rows)
	case DropTable:
		// Canceling after the backfill has no effect.
		expected = [][]string{}
		rows := runner.QueryStr(t, "SHOW TABLES FROM t")
		require.Equal(t, expected, rows)
	}
}

func getTableIDsUnderTest(schemaChangeType SchemaChangeType) []sqlbase.ID {
	tableID := sqlbase.ID(53)
	if schemaChangeType == CreateTable {
		tableID = sqlbase.ID(54)
	}
	return []sqlbase.ID{tableID}
}

func TestMigrateCreateTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer setTestJobsAdoptInterval()()

	schemaChange := "CREATE TABLE t.public.new_table (k INT8, FOREIGN KEY (k) REFERENCES t.public.test (k))"
	schemaChangeType := CreateTable

	blockStates := []BlockState{
		BeforeBackfill,
	}

	for _, blockState := range blockStates {
		blockState := blockState
		shouldCancel := false

		if !canBlockIfCanceled(blockState, shouldCancel) {
			continue
		}

		t.Run(getTestName(schemaChangeType, blockState, shouldCancel), func(t *testing.T) {
			testSchemaChangeMigrations(t, blockState, schemaChangeType, shouldCancel, schemaChange)
		})
	}
}

func TestMigrateAddColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer setTestJobsAdoptInterval()()

	schemaChange := "ALTER TABLE t.public.test ADD COLUMN foo INT8"
	schemaChangeType := AddColumn

	blockStates := []BlockState{
		BeforeBackfill,
		AfterBackfill,
		AfterReversingMutations,
	}

	for _, blockState := range blockStates {
		for _, shouldCancel := range []bool{true, false} {
			blockState := blockState
			shouldCancel := shouldCancel

			if !canBlockIfCanceled(blockState, shouldCancel) {
				continue
			}

			t.Run(getTestName(schemaChangeType, blockState, shouldCancel), func(t *testing.T) {
				testSchemaChangeMigrations(t, blockState, schemaChangeType, shouldCancel, schemaChange)
			})
		}
	}
}

func TestMigrateDropColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer setTestJobsAdoptInterval()()

	schemaChange := "ALTER TABLE t.public.test DROP COLUMN v"
	schemaChangeType := DropColumn

	blockStates := []BlockState{
		BeforeBackfill,
		AfterBackfill,
		AfterReversingMutations,
	}

	for _, blockState := range blockStates {
		for _, shouldCancel := range []bool{true, false} {
			blockState := blockState
			shouldCancel := shouldCancel

			if !canBlockIfCanceled(blockState, shouldCancel) {
				continue
			}

			t.Run(getTestName(schemaChangeType, blockState, shouldCancel), func(t *testing.T) {
				testSchemaChangeMigrations(t, blockState, schemaChangeType, shouldCancel, schemaChange)
			})
		}
	}
}

func TestMigrateAddIndex(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer setTestJobsAdoptInterval()()

	schemaChange := "CREATE INDEX v_idx ON t.public.test (v)"
	schemaChangeType := AddIndex

	blockStates := []BlockState{
		BeforeBackfill,
		AfterReversingMutations,
	}

	for _, blockState := range blockStates {
		for _, shouldCancel := range []bool{true, false} {
			blockState := blockState
			shouldCancel := shouldCancel

			if !canBlockIfCanceled(blockState, shouldCancel) {
				continue
			}

			t.Run(getTestName(schemaChangeType, blockState, shouldCancel), func(t *testing.T) {
				testSchemaChangeMigrations(t, blockState, schemaChangeType, shouldCancel, schemaChange)
			})
		}
	}
}

func TestMigrateDropIndex(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer setTestJobsAdoptInterval()()

	schemaChange := "DROP INDEX t.public.test@k_idx"
	schemaChangeType := DropIndex

	blockStates := []BlockState{
		BeforeBackfill,
		AfterReversingMutations,
		WaitingForGC,
	}

	for _, blockState := range blockStates {
		for _, shouldCancel := range []bool{true, false} {
			blockState := blockState
			shouldCancel := shouldCancel

			if !canBlockIfCanceled(blockState, shouldCancel) {
				continue
			}

			t.Run(getTestName(schemaChangeType, blockState, shouldCancel), func(t *testing.T) {
				testSchemaChangeMigrations(t, blockState, schemaChangeType, shouldCancel, schemaChange)
			})
		}
	}
}

func TestMigrateAddConstraint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer setTestJobsAdoptInterval()()

	schemaChange := "ALTER TABLE t.public.test ADD CONSTRAINT v_unq UNIQUE (v)"
	schemaChangeType := AddConstraint

	blockStates := []BlockState{
		BeforeBackfill,
		AfterReversingMutations,
	}

	for _, blockState := range blockStates {
		for _, shouldCancel := range []bool{false, true} {
			blockState := blockState
			shouldCancel := shouldCancel

			if !canBlockIfCanceled(blockState, shouldCancel) {
				continue
			}

			t.Run(getTestName(schemaChangeType, blockState, shouldCancel), func(t *testing.T) {
				testSchemaChangeMigrations(t, blockState, schemaChangeType, shouldCancel, schemaChange)
			})
		}
	}
}

func TestMigrateDropConstraint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer setTestJobsAdoptInterval()()

	schemaChange := "ALTER TABLE t.public.test DROP CONSTRAINT k_cons"
	schemaChangeType := DropConstraint

	blockStates := []BlockState{
		BeforeBackfill,
		AfterReversingMutations,
	}

	for _, blockState := range blockStates {
		for _, shouldCancel := range []bool{true, false} {
			blockState := blockState
			shouldCancel := shouldCancel

			if !canBlockIfCanceled(blockState, shouldCancel) {
				continue
			}

			t.Run(getTestName(schemaChangeType, blockState, shouldCancel), func(t *testing.T) {
				testSchemaChangeMigrations(t, blockState, schemaChangeType, shouldCancel, schemaChange)
			})
		}
	}
}

func TestMigrateTruncateTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer setTestJobsAdoptInterval()()

	schemaChange := "TRUNCATE TABLE t.public.test"
	schemaChangeType := TruncateTable

	blockStates := []BlockState{
		BeforeBackfill,
		AfterReversingMutations,
	}

	for _, blockState := range blockStates {
		for _, shouldCancel := range []bool{true, false} {
			blockState := blockState
			shouldCancel := shouldCancel

			if !canBlockIfCanceled(blockState, shouldCancel) {
				continue
			}

			t.Run(getTestName(schemaChangeType, blockState, shouldCancel), func(t *testing.T) {
				testSchemaChangeMigrations(t, blockState, schemaChangeType, shouldCancel, schemaChange)
			})
		}
	}
}

func TestMigrateDropTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer setTestJobsAdoptInterval()()

	schemaChange := "DROP TABLE t.public.test"
	schemaChangeType := DropTable

	blockStates := []BlockState{
		BeforeBackfill,
		AfterReversingMutations,
		WaitingForGC,
	}

	for _, blockState := range blockStates {
		for _, shouldCancel := range []bool{true, false} {
			blockState := blockState
			shouldCancel := shouldCancel

			if !canBlockIfCanceled(blockState, shouldCancel) {
				continue
			}

			t.Run(getTestName(schemaChangeType, blockState, shouldCancel), func(t *testing.T) {
				testSchemaChangeMigrations(t, blockState, schemaChangeType, shouldCancel, schemaChange)
			})
		}
	}
}
