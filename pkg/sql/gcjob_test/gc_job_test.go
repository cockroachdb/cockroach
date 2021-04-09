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
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/gcjob"
	"github.com/cockroachdb/cockroach/pkg/sql/gcjob/gcjobnotifier"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TODO(pbardea): Add more testing around the timer calculations.
func TestSchemaChangeGCJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.WithIssue(t, 60664, "flaky test")

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
			blockGC := make(chan struct{}, 1)
			params := base.TestServerArgs{}
			params.Knobs.JobsTestingKnobs = jobs.NewTestingKnobsWithShortIntervals()
			params.Knobs.GCJob = &sql.GCJobTestingKnobs{
				RunBeforePerformGC: func(_ jobspb.JobID) error {
					<-blockGC
					return nil
				},
			}
			s, db, kvDB := serverutils.StartServer(t, params)
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
			myDBID := descpb.ID(bootstrap.TestingUserDescID(2))
			myTableID := descpb.ID(bootstrap.TestingUserDescID(3))
			myOtherTableID := descpb.ID(bootstrap.TestingUserDescID(4))

			var myTableDesc *tabledesc.Mutable
			var myOtherTableDesc *tabledesc.Mutable
			if err := sql.TestingDescsTxn(ctx, s, func(ctx context.Context, txn *kv.Txn, col *descs.Collection) error {
				myImm, err := col.Direct().MustGetTableDescByID(ctx, txn, myTableID)
				if err != nil {
					return err
				}
				myTableDesc = tabledesc.NewBuilder(myImm.TableDesc()).BuildExistingMutableTable()
				myOtherImm, err := col.Direct().MustGetTableDescByID(ctx, txn, myOtherTableID)
				if err != nil {
					return err
				}
				myOtherTableDesc = tabledesc.NewBuilder(myOtherImm.TableDesc()).BuildExistingMutableTable()
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
			var expectedRunningStatus string
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
				expectedRunningStatus = "performing garbage collection on index 2"
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
				expectedRunningStatus = fmt.Sprintf("performing garbage collection on table %d", myTableID)
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
				expectedRunningStatus = fmt.Sprintf("performing garbage collection on tables %d, %d", myTableID, myOtherTableID)
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
				Username:      username.TestUserName(),
				DescriptorIDs: descpb.IDs{myTableID},
				Details:       details,
				Progress:      jobspb.SchemaChangeGCProgress{},
				RunningStatus: sql.RunningStatusWaitingGC,
				NonCancelable: true,
			}

			// The job record that will be used to lookup this job.
			lookupJR := jobs.Record{
				Description:   "GC test",
				Username:      username.TestUserName(),
				DescriptorIDs: descpb.IDs{myTableID},
				Details:       details,
			}

			job, err := jobs.TestingCreateAndStartJob(ctx, jobRegistry, kvDB, jobRecord)
			if err != nil {
				t.Fatal(err)
			}

			// Check that the job started.
			jobIDStr := strconv.Itoa(int(job.ID()))
			if err := jobutils.VerifyRunningSystemJob(t, sqlDB, 0, jobspb.TypeSchemaChangeGC, sql.RunningStatusWaitingGC, lookupJR); err != nil {
				t.Fatal(err)
			}

			if ttlTime != FUTURE {
				// Check that the job eventually blocks right before performing GC, due to the testing knob.
				sqlDB.CheckQueryResultsRetry(
					t,
					fmt.Sprintf("SELECT status, running_status FROM [SHOW JOBS] WHERE job_id = %s", jobIDStr),
					[][]string{{"running", expectedRunningStatus}})
			}
			blockGC <- struct{}{}

			if ttlTime == FUTURE {
				time.Sleep(500 * time.Millisecond)
			} else {
				sqlDB.CheckQueryResultsRetry(t, fmt.Sprintf("SELECT status FROM [SHOW JOBS] WHERE job_id = %s", jobIDStr), [][]string{{"succeeded"}})
				if err := jobutils.VerifySystemJob(t, sqlDB, 0, jobspb.TypeSchemaChangeGC, jobs.StatusSucceeded, lookupJR); err != nil {
					t.Fatal(err)
				}
			}

			if err := sql.TestingDescsTxn(ctx, s, func(ctx context.Context, txn *kv.Txn, col *descs.Collection) error {
				myImm, err := col.Direct().MustGetTableDescByID(ctx, txn, myTableID)
				if ttlTime != FUTURE && (dropItem == TABLE || dropItem == DATABASE) {
					// We dropped the table, so expect it to not be found.
					require.EqualError(t, err, "descriptor not found")
					return nil
				}
				if err != nil {
					return err
				}
				myTableDesc = tabledesc.NewBuilder(myImm.TableDesc()).BuildExistingMutableTable()
				myOtherImm, err := col.Direct().MustGetTableDescByID(ctx, txn, myOtherTableID)
				if ttlTime != FUTURE && dropItem == DATABASE {
					// We dropped the entire database, so expect none of the tables to be found.
					require.EqualError(t, err, "descriptor not found")
					return nil
				}
				if err != nil {
					return err
				}
				myOtherTableDesc = tabledesc.NewBuilder(myOtherImm.TableDesc()).BuildExistingMutableTable()
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestSchemaChangeGCJobTableGCdWhileWaitingForExpiration(t *testing.T) {
	defer leaktest.AfterTest(t)()

	args := base.TestServerArgs{Knobs: base.TestingKnobs{JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals()}}

	// We're going to drop a table then manually delete it, then update the
	// database zone config and ensure the job finishes successfully.
	s, db, kvDB := serverutils.StartServer(t, args)
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
	var jobID jobspb.JobID
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
		nameKey := catalogkeys.MakePublicObjectNameKey(keys.SystemSQLCodec, dbID, "foo")
		if _, err := txn.Del(ctx, nameKey); err != nil {
			return err
		}
		descKey := catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, tableID)
		_, err := txn.Del(ctx, descKey)
		return err
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

func TestGCJobRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	var failed atomic.Value
	failed.Store(false)
	params := base.TestServerArgs{}
	params.Knobs.JobsTestingKnobs = jobs.NewTestingKnobsWithShortIntervals()
	params.Knobs.Store = &kvserver.StoreTestingKnobs{
		TestingRequestFilter: func(ctx context.Context, request roachpb.BatchRequest) *roachpb.Error {
			_, ok := request.GetArg(roachpb.ClearRange)
			if !ok {
				return nil
			}
			if failed.Load().(bool) {
				return nil
			}
			failed.Store(true)
			return roachpb.NewError(&roachpb.BatchTimestampBeforeGCError{
				Timestamp: hlc.Timestamp{},
				Threshold: hlc.Timestamp{},
			})
		},
	}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)
	tdb := sqlutils.MakeSQLRunner(db)
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

// TestGCTenant is lightweight test that tests the branching logic in Resume
// depending on if the job is GC for tenant or tables/indexes.
func TestGCResumer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer jobs.ResetConstructors()()
	gcjob.SetSmallMaxGCIntervalForTest()

	ctx := context.Background()
	args := base.TestServerArgs{Knobs: base.TestingKnobs{JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals()}}
	srv, sqlDB, kvDB := serverutils.StartServer(t, args)
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

		sj, err := jobs.TestingCreateAndStartJob(ctx, jobRegistry, kvDB, record)
		require.NoError(t, err)
		require.NoError(t, sj.AwaitCompletion(ctx))
		job, err := jobRegistry.LoadJob(ctx, sj.ID())
		require.NoError(t, err)
		require.Equal(t, jobs.StatusSucceeded, job.Status())
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

		sj, err := jobs.TestingCreateAndStartJob(ctx, jobRegistry, kvDB, record)
		require.NoError(t, err)

		_, err = sqlDB.Exec("ALTER RANGE tenants CONFIGURE ZONE USING gc.ttlseconds = 1;")
		require.NoError(t, err)
		require.NoError(t, sj.AwaitCompletion(ctx))

		job, err := jobRegistry.LoadJob(ctx, sj.ID())
		require.NoError(t, err)
		require.Equal(t, jobs.StatusSucceeded, job.Status())
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

		sj, err := jobs.TestingCreateAndStartJob(ctx, jobRegistry, kvDB, record)
		require.NoError(t, err)
		require.Error(t, sj.AwaitCompletion(ctx))
	})
}

// TestGCTenant tests the `gcTenant` method responsible for clearing the
// tenant's data.
func TestGCTenant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer jobs.ResetConstructors()()

	ctx := context.Background()
	srv, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	execCfg := srv.ExecutorConfig().(sql.ExecutorConfig)
	defer srv.Stopper().Stop(ctx)

	gcClosure := func(tenID uint64, progress *jobspb.SchemaChangeGCProgress) error {
		return gcjob.TestingGCTenant(ctx, &execCfg, tenID, progress)
	}

	const (
		activeTenID      = 10
		dropTenID        = 11
		nonexistentTenID = 12
	)
	require.NoError(t, sql.CreateTenantRecord(
		ctx, &execCfg, nil, /* txn */
		&descpb.TenantInfoWithUsage{
			TenantInfo: descpb.TenantInfo{ID: activeTenID},
		}),
	)
	require.NoError(t, sql.CreateTenantRecord(
		ctx, &execCfg, nil, /* txn */
		&descpb.TenantInfoWithUsage{
			TenantInfo: descpb.TenantInfo{ID: dropTenID, State: descpb.TenantInfo_DROP},
		}),
	)

	t.Run("unexpected progress state", func(t *testing.T) {
		progress := &jobspb.SchemaChangeGCProgress{
			Tenant: &jobspb.SchemaChangeGCProgress_TenantProgress{
				Status: jobspb.SchemaChangeGCProgress_WAITING_FOR_GC,
			},
		}
		require.EqualError(
			t,
			gcClosure(10, progress),
			"Tenant id 10 is expired and should not be in state WAITING_FOR_GC",
		)
		require.Equal(t, jobspb.SchemaChangeGCProgress_WAITING_FOR_GC, progress.Tenant.Status)
	})

	t.Run("non-existent tenant deleting progress", func(t *testing.T) {
		progress := &jobspb.SchemaChangeGCProgress{
			Tenant: &jobspb.SchemaChangeGCProgress_TenantProgress{
				Status: jobspb.SchemaChangeGCProgress_DELETING,
			},
		}
		require.NoError(t, gcClosure(nonexistentTenID, progress))
		require.Equal(t, jobspb.SchemaChangeGCProgress_DELETED, progress.Tenant.Status)
	})

	t.Run("existent tenant deleted progress", func(t *testing.T) {
		progress := &jobspb.SchemaChangeGCProgress{
			Tenant: &jobspb.SchemaChangeGCProgress_TenantProgress{
				Status: jobspb.SchemaChangeGCProgress_DELETED,
			},
		}
		require.EqualError(
			t,
			gcClosure(dropTenID, progress),
			"GC state for tenant id:11 state:DROP is DELETED yet the tenant row still exists",
		)
	})

	t.Run("active tenant GC", func(t *testing.T) {
		progress := &jobspb.SchemaChangeGCProgress{
			Tenant: &jobspb.SchemaChangeGCProgress_TenantProgress{
				Status: jobspb.SchemaChangeGCProgress_DELETING,
			},
		}
		require.EqualError(
			t,
			gcClosure(activeTenID, progress),
			"gc tenant 10: clear tenant: tenant 10 is not in state DROP", activeTenID,
		)
	})

	t.Run("drop tenant GC", func(t *testing.T) {
		progress := &jobspb.SchemaChangeGCProgress{
			Tenant: &jobspb.SchemaChangeGCProgress_TenantProgress{
				Status: jobspb.SchemaChangeGCProgress_DELETING,
			},
		}

		descKey := catalogkeys.MakeDescMetadataKey(
			keys.MakeSQLCodec(roachpb.MakeTenantID(dropTenID)), keys.NamespaceTableID,
		)
		require.NoError(t, kvDB.Put(ctx, descKey, "foo"))

		r, err := kvDB.Get(ctx, descKey)
		require.NoError(t, err)
		val, err := r.Value.GetBytes()
		require.NoError(t, err)
		require.Equal(t, []byte("foo"), val)

		require.NoError(t, gcClosure(dropTenID, progress))
		require.Equal(t, jobspb.SchemaChangeGCProgress_DELETED, progress.Tenant.Status)
		_, err = sql.GetTenantRecord(ctx, &execCfg, nil /* txn */, dropTenID)
		require.EqualError(t, err, `tenant "11" does not exist`)
		require.NoError(t, gcClosure(dropTenID, progress))

		r, err = kvDB.Get(ctx, descKey)
		require.NoError(t, err)
		require.True(t, nil == r.Value)
	})
}

// TestGCJobNoSystemConfig tests that the GC job is robust to running with
// no system config provided by the SystemConfigProvider. It is a regression
// test for a panic which could occur due to a slow systemconfigwatcher
// initialization.
func TestGCJobNoSystemConfig(t *testing.T) {
	defer leaktest.AfterTest(t)()

	provider := fakeSystemConfigProvider{}
	settings := cluster.MakeTestingClusterSettings()
	stopper := stop.NewStopper()
	ctx := context.Background()

	gcKnobs := &sql.GCJobTestingKnobs{}
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		Settings: settings,
		Stopper:  stopper,
		Knobs: base.TestingKnobs{
			GCJob: gcKnobs,
		},
	})
	defer stopper.Stop(ctx)
	codec := s.ExecutorConfig().(sql.ExecutorConfig).Codec
	n := gcjobnotifier.New(settings, &provider, codec, stopper)
	n.Start(ctx)
	gcKnobs.Notifier = n

	tdb := sqlutils.MakeSQLRunner(sqlDB)
	tdb.Exec(t, "CREATE TABLE foo (i INT PRIMARY KEY)")
	tdb.Exec(t, "SET CLUSTER SETTING kv.protectedts.poll_interval = '10ms'")
	var id uint32
	tdb.QueryRow(t, "SELECT 'foo'::regclass::int").Scan(&id)
	tdb.Exec(t, "DROP TABLE foo")
	// We want to make sure there's a notifyee and that the job attempted
	// to read the status twice. We expect it once for the notifier and
	// once for the job itself.
	testutils.SucceedsSoon(t, func() error {
		if n := provider.numNotifyees(); n == 0 {
			return errors.Errorf("expected 1 notifyee, got %d", n)
		}
		if n := provider.numCalls(); n < 2 {
			return errors.Errorf("expected at least 2 calls, got %d", n)
		}
		return nil
	})
	cfgProto := &zonepb.ZoneConfig{
		GC: &zonepb.GCPolicy{TTLSeconds: 0},
	}
	cfg := config.NewSystemConfig(cfgProto)
	descKV, err := kvDB.Get(ctx, codec.DescMetadataKey(id))
	require.NoError(t, err)
	var zoneKV roachpb.KeyValue
	zoneKV.Key = config.MakeZoneKey(codec, descpb.ID(id))
	require.NoError(t, zoneKV.Value.SetProto(cfgProto))
	defaultKV := zoneKV
	defaultKV.Key = config.MakeZoneKey(codec, 0)
	// We need to put in an entry for the descriptor both so that the notifier
	// fires and so that we don't think the descriptor is missing. We also
	// need a zone config KV to make the delta filter happy.
	cfg.Values = []roachpb.KeyValue{
		{Key: descKV.Key, Value: *descKV.Value},
		defaultKV,
		zoneKV,
	}

	provider.setConfig(cfg)
	tdb.CheckQueryResultsRetry(t, `
SELECT status
  FROM crdb_internal.jobs
 WHERE description = 'GC for DROP TABLE defaultdb.public.foo'`,
		[][]string{{"succeeded"}})
}

type fakeSystemConfigProvider struct {
	mu struct {
		syncutil.Mutex

		calls     int
		n         int
		config    *config.SystemConfig
		notifyees map[int]chan struct{}
	}
}

func (f *fakeSystemConfigProvider) GetSystemConfig() *config.SystemConfig {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mu.calls++
	return f.mu.config
}

func (f *fakeSystemConfigProvider) RegisterSystemConfigChannel() (
	_ <-chan struct{},
	unregister func(),
) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ch := make(chan struct{}, 1)
	n := f.mu.n
	f.mu.n++
	if f.mu.notifyees == nil {
		f.mu.notifyees = map[int]chan struct{}{}
	}
	f.mu.notifyees[n] = ch
	return ch, func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		delete(f.mu.notifyees, n)
	}
}

func (f *fakeSystemConfigProvider) setConfig(c *config.SystemConfig) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mu.config = c
	for _, ch := range f.mu.notifyees {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (f *fakeSystemConfigProvider) numNotifyees() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.mu.notifyees)
}

func (f *fakeSystemConfigProvider) numCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.mu.calls
}

var _ config.SystemConfigProvider = (*fakeSystemConfigProvider)(nil)
