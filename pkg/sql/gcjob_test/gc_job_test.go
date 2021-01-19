// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package gcjob_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/gcjob"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TODO(pbardea): Add more testing around the timer calculations.
func TestSchemaChangeGCJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer jobs.TestingSetAdoptAndCancelIntervals(100*time.Millisecond, 100*time.Millisecond)()

	type DropItem int
	const (
		INDEX = iota
		TABLE
		DATABASE
	)

	type TTLTime int
	const (
		PAST   = iota // An item was supposed to be GC already.
		SOON          // An item will be GC'd soon.
		FUTURE        // An item should not be GC'd during this test.
	)

	for _, dropItem := range []DropItem{INDEX, TABLE, DATABASE} {
		for _, ttlTime := range []TTLTime{PAST, SOON, FUTURE} {
			s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
			ctx := context.Background()
			defer s.Stopper().Stop(ctx)
			sqlDB := sqlutils.MakeSQLRunner(db)

			jobRegistry := s.JobRegistry().(*jobs.Registry)

			sqlDB.Exec(t, "CREATE DATABASE my_db")
			sqlDB.Exec(t, "USE my_db")
			sqlDB.Exec(t, "CREATE TABLE my_table (a int primary key, b int, index (b))")
			sqlDB.Exec(t, "CREATE TABLE my_other_table (a int primary key, b int, index (b))")
			if ttlTime == SOON {
				sqlDB.Exec(t, "ALTER TABLE my_table CONFIGURE ZONE USING gc.ttlseconds = 1")
				sqlDB.Exec(t, "ALTER TABLE my_other_table CONFIGURE ZONE USING gc.ttlseconds = 1")
			}
			myDBID := descpb.ID(keys.MinUserDescID + 2)
			myTableID := descpb.ID(keys.MinUserDescID + 3)
			myOtherTableID := descpb.ID(keys.MinUserDescID + 4)

			var myTableDesc *tabledesc.Mutable
			var myOtherTableDesc *tabledesc.Mutable
			if err := kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				myDesc, err := catalogkv.GetDescriptorByID(ctx, txn, keys.SystemSQLCodec, myTableID,
					catalogkv.Mutable, catalogkv.TableDescriptorKind, true /* required */)
				if err != nil {
					return err
				}
				myTableDesc = myDesc.(*tabledesc.Mutable)
				myOtherDesc, err := catalogkv.GetDescriptorByID(ctx, txn, keys.SystemSQLCodec, myOtherTableID,
					catalogkv.Mutable, catalogkv.TableDescriptorKind, true /* required */)
				if err != nil {
					return err
				}
				myOtherTableDesc = myOtherDesc.(*tabledesc.Mutable)
				return nil
			}); err != nil {
				t.Fatal(err)
			}

			// Start the job that drops an index.
			dropTime := timeutil.Now().UnixNano()
			if ttlTime == PAST {
				dropTime = 1
			}
			var details jobspb.SchemaChangeGCDetails
			switch dropItem {
			case INDEX:
				details = jobspb.SchemaChangeGCDetails{
					Indexes: []jobspb.SchemaChangeGCDetails_DroppedIndex{
						{
							IndexID:  descpb.IndexID(2),
							DropTime: dropTime,
						},
					},
					ParentID: myTableID,
				}
				myTableDesc.SetPublicNonPrimaryIndexes([]descpb.IndexDescriptor{})
				myTableDesc.GCMutations = append(myTableDesc.GCMutations, descpb.TableDescriptor_GCDescriptorMutation{
					IndexID: descpb.IndexID(2),
				})
			case TABLE:
				details = jobspb.SchemaChangeGCDetails{
					Tables: []jobspb.SchemaChangeGCDetails_DroppedID{
						{
							ID:       myTableID,
							DropTime: dropTime,
						},
					},
				}
				myTableDesc.State = descpb.DescriptorState_DROP
				myTableDesc.DropTime = dropTime
			case DATABASE:
				details = jobspb.SchemaChangeGCDetails{
					Tables: []jobspb.SchemaChangeGCDetails_DroppedID{
						{
							ID:       myTableID,
							DropTime: dropTime,
						},
						{
							ID:       myOtherTableID,
							DropTime: dropTime,
						},
					},
					ParentID: myDBID,
				}
				myTableDesc.State = descpb.DescriptorState_DROP
				myTableDesc.DropTime = dropTime
				myOtherTableDesc.State = descpb.DescriptorState_DROP
				myOtherTableDesc.DropTime = dropTime
			}

			if err := kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				b := txn.NewBatch()
				descKey := catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, myTableID)
				descDesc := myTableDesc.DescriptorProto()
				b.Put(descKey, descDesc)
				descKey2 := catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, myOtherTableID)
				descDesc2 := myOtherTableDesc.DescriptorProto()
				b.Put(descKey2, descDesc2)
				return txn.Run(ctx, b)
			}); err != nil {
				t.Fatal(err)
			}

			jobRecord := jobs.Record{
				Description:   "GC test",
				Username:      security.TestUserName(),
				DescriptorIDs: descpb.IDs{myTableID},
				Details:       details,
				Progress:      jobspb.SchemaChangeGCProgress{},
				RunningStatus: sql.RunningStatusWaitingGC,
				NonCancelable: true,
			}

			// The job record that will be used to lookup this job.
			lookupJR := jobs.Record{
				Description:   "GC test",
				Username:      security.TestUserName(),
				DescriptorIDs: descpb.IDs{myTableID},
				Details:       details,
			}

			resultsCh := make(chan tree.Datums)
			job, _, err := jobRegistry.CreateAndStartJob(ctx, resultsCh, jobRecord)
			if err != nil {
				t.Fatal(err)
			}

			// Check that the job started.
			jobIDStr := strconv.Itoa(int(*job.ID()))
			if err := jobutils.VerifyRunningSystemJob(t, sqlDB, 0, jobspb.TypeSchemaChangeGC, sql.RunningStatusWaitingGC, lookupJR); err != nil {
				t.Fatal(err)
			}

			if ttlTime == FUTURE {
				time.Sleep(500 * time.Millisecond)
			} else {
				sqlDB.CheckQueryResultsRetry(t, fmt.Sprintf("SELECT status FROM [SHOW JOBS] WHERE job_id = %s", jobIDStr), [][]string{{"succeeded"}})
				if err := jobutils.VerifySystemJob(t, sqlDB, 0, jobspb.TypeSchemaChangeGC, jobs.StatusSucceeded, lookupJR); err != nil {
					t.Fatal(err)
				}
			}

			if err := kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				myDesc, err := catalogkv.GetDescriptorByID(ctx, txn, keys.SystemSQLCodec, myTableID,
					catalogkv.Mutable, catalogkv.TableDescriptorKind, true /* required */)
				if ttlTime != FUTURE && (dropItem == TABLE || dropItem == DATABASE) {
					// We dropped the table, so expect it to not be found.
					require.EqualError(t, err, "descriptor not found")
					return nil
				}
				if err != nil {
					return err
				}
				myTableDesc = myDesc.(*tabledesc.Mutable)
				myOtherDesc, err := catalogkv.GetDescriptorByID(ctx, txn, keys.SystemSQLCodec, myOtherTableID,
					catalogkv.Mutable, catalogkv.TableDescriptorKind, true /* required */)
				if ttlTime != FUTURE && dropItem == DATABASE {
					// We dropped the entire database, so expect none of the tables to be found.
					require.EqualError(t, err, "descriptor not found")
					return nil
				}
				if err != nil {
					return err
				}
				myOtherTableDesc = myOtherDesc.(*tabledesc.Mutable)
				return nil
			}); err != nil {
				t.Fatal(err)
			}

			switch dropItem {
			case INDEX:
				if ttlTime == FUTURE {
					require.Equal(t, 1, len(myTableDesc.GCMutations))
				} else {
					require.Equal(t, 0, len(myTableDesc.GCMutations))
				}
			case TABLE:
			case DATABASE:
				// Already handled the case where the TTL was lowered, since we expect
				// to not find the descriptor.
				// If the TTL was not lowered, we just expect to have not found an error
				// when fetching the TTL.
			}
		}
	}
}

func TestSchemaChangeGCJobTableGCdWhileWaitingForExpiration(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defer jobs.TestingSetAdoptAndCancelIntervals(100*time.Millisecond, 100*time.Millisecond)()

	// We're going to drop a table then manually delete it, then update the
	// database zone config and ensure the job finishes successfully.
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)

	// Note: this is to avoid a common failure during shutdown when a range
	// merge runs concurrently with node shutdown leading to a panic due to
	// pebble already being closed. See #51544.
	sqlDB.Exec(t, "SET CLUSTER SETTING kv.range_merge.queue_enabled = false")

	sqlDB.Exec(t, "CREATE DATABASE db")
	sqlDB.Exec(t, "CREATE TABLE db.foo ()")
	var dbID, tableID descpb.ID
	sqlDB.QueryRow(t, `
SELECT parent_id, table_id
  FROM crdb_internal.tables
 WHERE database_name = $1 AND name = $2;
`, "db", "foo").Scan(&dbID, &tableID)
	sqlDB.Exec(t, "DROP TABLE db.foo")

	// Now we should be able to find our GC job
	var jobID int64
	var status jobs.Status
	var runningStatus jobs.RunningStatus
	sqlDB.QueryRow(t, `
SELECT job_id, status, running_status
  FROM crdb_internal.jobs
 WHERE description LIKE 'GC for DROP TABLE db.public.foo';
`).Scan(&jobID, &status, &runningStatus)
	require.Equal(t, jobs.StatusRunning, status)
	require.Equal(t, sql.RunningStatusWaitingGC, runningStatus)

	// Manually delete the table.
	require.NoError(t, kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		nameKey := catalogkeys.MakeNameMetadataKey(keys.SystemSQLCodec, dbID, keys.PublicSchemaID, "foo")
		if err := txn.Del(ctx, nameKey); err != nil {
			return err
		}
		descKey := catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, tableID)
		return txn.Del(ctx, descKey)
	}))
	// Update the GC TTL to tickle the job to refresh the status and discover that
	// it has been removed. Use a SucceedsSoon to deal with races between setting
	// the zone config and when the job subscribes to the zone config.
	var i int
	testutils.SucceedsSoon(t, func() error {
		i++
		sqlDB.Exec(t, "ALTER DATABASE db CONFIGURE ZONE USING gc.ttlseconds = 60 * 60 * 25 + $1", i)
		var status jobs.Status
		sqlDB.QueryRow(t, "SELECT status FROM [SHOW JOB $1]", jobID).Scan(&status)
		if status != jobs.StatusSucceeded {
			return errors.Errorf("job status %v != %v", status, jobs.StatusSucceeded)
		}
		return nil
	})
}

// TestGCTenant is lightweight test that tests the branching logic in Resume
// depending if the job is GC for tenant or tables/indexes and also the GC
// logic for GC-ing tenant.
func TestGCResumer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer jobs.TestingSetAdoptAndCancelIntervals(100*time.Millisecond, 100*time.Millisecond)()
	defer log.Scope(t).Close(t)
	defer jobs.ResetConstructors()()
	gcjob.SetSmallMaxGCIntervalForTest()

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	execCfg := srv.ExecutorConfig().(sql.ExecutorConfig)
	jobRegistry := execCfg.JobRegistry
	defer srv.Stopper().Stop(ctx)

	t.Run("tenant GC job past", func(t *testing.T) {
		const tenID = 10
		record := jobs.Record{
			Details: jobspb.SchemaChangeGCDetails{
				Tenant: &jobspb.SchemaChangeGCDetails_DroppedTenant{
					ID:       tenID,
					DropTime: 1, // guarantees the tenant will expire immediately.
				},
			},
			Progress: jobspb.SchemaChangeGCProgress{},
		}

		resultsCh := make(chan tree.Datums)
		sj, errCh, err := jobRegistry.CreateAndStartJob(ctx, resultsCh, record)
		require.NoError(t, err)
		require.NoError(t, <-errCh)
		job, err := jobRegistry.LoadJob(ctx, *sj.ID())
		require.NoError(t, err)
		st, err := job.CurrentStatus(ctx)
		require.NoError(t, err)
		require.Equal(t, jobs.StatusSucceeded, st)
		_, err = sql.GetTenantRecord(ctx, &execCfg, nil /* txn */, tenID)
		require.EqualError(t, err, `tenant "10" does not exist`)
		progress := job.Progress()
		require.Equal(t, jobspb.SchemaChangeGCProgress_DELETED, progress.GetSchemaChangeGC().Tenant.Status)
	})

	t.Run("tenant GC job soon", func(t *testing.T) {
		const tenID = 10
		record := jobs.Record{
			Details: jobspb.SchemaChangeGCDetails{
				Tenant: &jobspb.SchemaChangeGCDetails_DroppedTenant{
					ID:       tenID,
					DropTime: timeutil.Now().UnixNano(),
				},
			},
			Progress: jobspb.SchemaChangeGCProgress{},
		}

		resultsCh := make(chan tree.Datums)
		sj, errCh, err := jobRegistry.CreateAndStartJob(ctx, resultsCh, record)
		require.NoError(t, err)

		_, err = sqlDB.Exec("ALTER RANGE tenants CONFIGURE ZONE USING gc.ttlseconds = 1;")
		require.NoError(t, err)
		require.NoError(t, <-errCh)

		job, err := jobRegistry.LoadJob(ctx, *sj.ID())
		require.NoError(t, err)
		st, err := job.CurrentStatus(ctx)
		require.NoError(t, err)
		require.Equal(t, jobs.StatusSucceeded, st)
		_, err = sql.GetTenantRecord(ctx, &execCfg, nil /* txn */, tenID)
		require.EqualError(t, err, `tenant "10" does not exist`)
		progress := job.Progress()
		require.Equal(t, jobspb.SchemaChangeGCProgress_DELETED, progress.GetSchemaChangeGC().Tenant.Status)
	})

	t.Run("no tenant and tables in same GC job", func(t *testing.T) {
		gcDetails := jobspb.SchemaChangeGCDetails{
			Tenant: &jobspb.SchemaChangeGCDetails_DroppedTenant{
				ID:       10,
				DropTime: 1, // guarantees the tenant will expire immediately.
			},
		}
		gcDetails.Tables = append(gcDetails.Tables, jobspb.SchemaChangeGCDetails_DroppedID{
			ID:       100,
			DropTime: 1,
		})
		record := jobs.Record{
			Details:  gcDetails,
			Progress: jobspb.SchemaChangeGCProgress{},
		}

		resultsCh := make(chan tree.Datums)
		_, errCh, err := jobRegistry.CreateAndStartJob(ctx, resultsCh, record)
		require.NoError(t, err)
		require.Error(t, <-errCh)
	})
}
