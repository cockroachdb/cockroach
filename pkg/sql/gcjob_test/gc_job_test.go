// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gcjob_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
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
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

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

// TODO(pbardea): Add more testing around the timer calculations.
func TestSchemaChangeGCJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, dropItem := range []DropItem{INDEX, TABLE, DATABASE} {
		for _, ttlTime := range []TTLTime{PAST, SOON, FUTURE} {
			t.Run(fmt.Sprintf("dropItem=%d/ttlTime=%d", dropItem, ttlTime), func(t *testing.T) {
				// NB: The inner body of this loop has been extracted into a function to
				// ensure defer statements (namely testserver.Stop) is executed at the
				// end of each loop rather than the end of each test.
				doTestSchemaChangeGCJob(t, dropItem, ttlTime)
			})
		}
	}
}

func doTestSchemaChangeGCJob(t *testing.T, dropItem DropItem, ttlTime TTLTime) {
	blockGC := make(chan struct{}, 1)
	params := base.TestServerArgs{}
	params.ScanMaxIdleTime = time.Millisecond
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
	// The deferred call to unblock the GC job needs to run before the deferred
	// call to stop the TestServer. Otherwise, the quiesce step of shutting down
	// can hang forever waiting for the GC job.
	defer close(blockGC)
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `SET CLUSTER SETTING sql.gc_job.wait_for_gc.interval = '1s';`)
	// Refresh protected timestamp cache immediately to make MVCC GC queue to
	// process GC immediately.
	sqlDB.Exec(t, `SET CLUSTER SETTING kv.protectedts.poll_interval = '1s';`)

	jobRegistry := s.JobRegistry().(*jobs.Registry)

	sqlDB.Exec(t, "CREATE DATABASE my_db")
	sqlDB.Exec(t, "USE my_db")
	sqlDB.Exec(t, "CREATE TABLE my_table (a int primary key, b int, index (b))")
	sqlDB.Exec(t, "CREATE TABLE my_other_table (a int primary key, b int, index (b))")
	if ttlTime == SOON {
		sqlDB.Exec(t, "ALTER TABLE my_table CONFIGURE ZONE USING gc.ttlseconds = 1")
		sqlDB.Exec(t, "ALTER TABLE my_other_table CONFIGURE ZONE USING gc.ttlseconds = 1")
	}

	myDBID := descpb.ID(bootstrap.TestingUserDescID(4))
	myTableID := descpb.ID(bootstrap.TestingUserDescID(6))
	myOtherTableID := descpb.ID(bootstrap.TestingUserDescID(7))

	var myTableDesc *tabledesc.Mutable
	var myOtherTableDesc *tabledesc.Mutable
	if err := sqltestutils.TestingDescsTxn(ctx, s, func(ctx context.Context, txn isql.Txn, col *descs.Collection) error {
		myImm, err := col.ByIDWithoutLeased(txn.KV()).Get().Table(ctx, myTableID)
		if err != nil {
			return err
		}
		myTableDesc = tabledesc.NewBuilder(myImm.TableDesc()).BuildExistingMutableTable()
		myOtherImm, err := col.ByIDWithoutLeased(txn.KV()).Get().Table(ctx, myOtherTableID)
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
	var expectedStatusMessage string
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
		expectedStatusMessage = "deleting data"
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
		expectedStatusMessage = "deleting data"
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
		expectedStatusMessage = "deleting data"
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
		StatusMessage: sql.StatusWaitingGC,
		NonCancelable: true,
	}

	// The job record that will be used to lookup this job.
	lookupJR := jobs.Record{
		Description:   "GC test",
		Username:      username.TestUserName(),
		DescriptorIDs: descpb.IDs{myTableID},
		Details:       details,
	}

	job, err := jobs.TestingCreateAndStartJob(ctx, jobRegistry, s.InternalDB().(isql.DB), jobRecord)
	if err != nil {
		t.Fatal(err)
	}

	// Check that the job started.
	jobIDStr := strconv.Itoa(int(job.ID()))
	testutils.SucceedsSoon(t, func() error {
		if err := jobutils.VerifyRunningSystemJob(
			t, sqlDB, 0, jobspb.TypeSchemaChangeGC, sql.StatusWaitingGC, lookupJR,
		); err != nil {
			// Since the intervals are set very low, the GC TTL job may have already
			// started. If so, the status will be "deleting data" since "waiting for
			// GC TTL" will have completed already.
			if testutils.IsError(err, "expected status waiting for GC TTL, got deleting data") {
				return nil
			}
			return err
		}
		return nil
	})

	if ttlTime != FUTURE {
		// Check that the job eventually blocks right before performing GC, due to the testing knob.
		sqlDB.CheckQueryResultsRetry(
			t,
			fmt.Sprintf("SELECT status, running_status FROM [SHOW JOBS] WHERE job_id = %s", jobIDStr),
			[][]string{{"running", expectedStatusMessage}})
	}
	blockGC <- struct{}{}

	if ttlTime == FUTURE {
		time.Sleep(500 * time.Millisecond)
	} else {
		sqlDB.CheckQueryResultsRetry(t, fmt.Sprintf("SELECT status FROM [SHOW JOBS] WHERE job_id = %s", jobIDStr), [][]string{{"succeeded"}})
		if err := jobutils.VerifySystemJob(t, sqlDB, 0, jobspb.TypeSchemaChangeGC, jobs.StateSucceeded, lookupJR); err != nil {
			t.Fatal(err)
		}
	}

	if err := sqltestutils.TestingDescsTxn(ctx, s, func(ctx context.Context, txn isql.Txn, col *descs.Collection) error {
		myImm, err := col.ByIDWithoutLeased(txn.KV()).Get().Table(ctx, myTableID)
		if err != nil {
			if ttlTime != FUTURE && (dropItem == TABLE || dropItem == DATABASE) {
				// We dropped the table, so expect it to not be found.
				require.EqualError(t, err, fmt.Sprintf(`relation "[%d]" does not exist`, myTableID))
				return nil
			}
			return err
		}
		myTableDesc = tabledesc.NewBuilder(myImm.TableDesc()).BuildExistingMutableTable()
		myOtherImm, err := col.ByIDWithoutLeased(txn.KV()).Get().Table(ctx, myOtherTableID)
		if err != nil {
			if ttlTime != FUTURE && dropItem == DATABASE {
				// We dropped the entire database, so expect none of the tables to be found.
				require.EqualError(t, err, fmt.Sprintf(`relation "[%d]" does not exist`, myOtherTableID))
				return nil
			}
			return err
		}
		myOtherTableDesc = tabledesc.NewBuilder(myOtherImm.TableDesc()).BuildExistingMutableTable()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestGCJobRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	var failed atomic.Value
	failed.Store(false)
	cs := cluster.MakeTestingClusterSettings()
	gcjob.EmptySpanPollInterval.Override(ctx, &cs.SV, 100*time.Millisecond)
	params := base.TestServerArgs{Settings: cs}
	params.Knobs.JobsTestingKnobs = jobs.NewTestingKnobsWithShortIntervals()
	params.Knobs.Store = &kvserver.StoreTestingKnobs{
		TestingRequestFilter: func(ctx context.Context, request *kvpb.BatchRequest) *kvpb.Error {
			r, ok := request.GetArg(kvpb.DeleteRange)
			if !ok || !r.(*kvpb.DeleteRangeRequest).UseRangeTombstone {
				return nil
			}
			if failed.Load().(bool) {
				return nil
			}
			failed.Store(true)
			return kvpb.NewError(&kvpb.BatchTimestampBeforeGCError{
				Timestamp: hlc.Timestamp{},
				Threshold: hlc.Timestamp{},
			})
		},
	}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)
	tdb := sqlutils.MakeSQLRunner(db)
	tdb.Exec(t, "CREATE TABLE foo (i INT PRIMARY KEY)")
	tdb.Exec(t, "INSERT INTO foo SELECT generate_series(1, 1000)")
	tdb.Exec(t, "DROP TABLE foo")
	var jobID string
	tdb.QueryRow(t, `
SELECT job_id
  FROM [SHOW JOBS]
 WHERE job_type = 'SCHEMA CHANGE GC' AND description LIKE '%foo%';`,
	).Scan(&jobID)

	const expectedStatusMessage = string(sql.StatusWaitingForMVCCGC)
	testutils.SucceedsSoon(t, func() error {
		var state, statusMessage, jobErr gosql.NullString
		tdb.QueryRow(t, fmt.Sprintf(`
SELECT status, running_status, error
FROM crdb_internal.jobs
WHERE job_id = %s`, jobID)).Scan(&state, &statusMessage, &jobErr)

		t.Logf(`details about SCHEMA CHANGE GC job: {state: %#v, status: %#v, error: %#v}`,
			state, statusMessage, jobErr)

		if !statusMessage.Valid {
			return errors.Newf(`status is NULL but expected %q`, expectedStatusMessage)
		}

		if actualStatus := statusMessage.String; actualStatus != expectedStatusMessage {
			return errors.Newf(`status %q does not match expected status %q`,
				actualStatus, expectedStatusMessage)
		}

		return nil
	})
}

// TestGCTenant is lightweight test that tests the branching logic in Resume
// depending on if the job is GC for tenant or tables/indexes.
func TestGCResumer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer jobs.ResetConstructors()()
	defer gcjob.SetSmallMaxGCIntervalForTest()()

	ctx := context.Background()
	args := base.TestServerArgs{Knobs: base.TestingKnobs{JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals()}}
	srv, sqlDB, _ := serverutils.StartServer(t, args)
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
			Username: username.TestUserName(),
		}

		sj, err := jobs.TestingCreateAndStartJob(ctx, jobRegistry, execCfg.InternalDB, record)
		require.NoError(t, err)
		require.NoError(t, sj.AwaitCompletion(ctx))
		job, err := jobRegistry.LoadJob(ctx, sj.ID())
		require.NoError(t, err)
		require.Equal(t, jobs.StateSucceeded, job.State())
		err = execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			_, err := sql.GetTenantRecordByID(ctx, txn, roachpb.MustMakeTenantID(tenID), execCfg.Settings)
			return err
		})

		require.EqualError(t, err, `tenant "10" does not exist`)
		progress := job.Progress()
		require.Equal(t, jobspb.SchemaChangeGCProgress_CLEARED, progress.GetSchemaChangeGC().Tenant.Status)
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
			Username: username.TestUserName(),
		}

		sj, err := jobs.TestingCreateAndStartJob(ctx, jobRegistry, execCfg.InternalDB, record)
		require.NoError(t, err)

		_, err = sqlDB.Exec("ALTER RANGE tenants CONFIGURE ZONE USING gc.ttlseconds = 1;")
		require.NoError(t, err)
		require.NoError(t, sj.AwaitCompletion(ctx))

		job, err := jobRegistry.LoadJob(ctx, sj.ID())
		require.NoError(t, err)
		require.Equal(t, jobs.StateSucceeded, job.State())
		err = execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			_, err := sql.GetTenantRecordByID(ctx, txn, roachpb.MustMakeTenantID(tenID), execCfg.Settings)
			return err
		})
		require.EqualError(t, err, `tenant "10" does not exist`)
		progress := job.Progress()
		require.Equal(t, jobspb.SchemaChangeGCProgress_CLEARED, progress.GetSchemaChangeGC().Tenant.Status)
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
			Username: username.TestUserName(),
		}

		sj, err := jobs.TestingCreateAndStartJob(ctx, jobRegistry, execCfg.InternalDB, record)
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
	require.NoError(t, execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		_, err := sql.CreateTenantRecord(
			ctx, execCfg.Codec, execCfg.Settings,
			txn,
			execCfg.SpanConfigKVAccessor.WithISQLTxn(ctx, txn),
			&mtinfopb.TenantInfoWithUsage{
				SQLInfo: mtinfopb.SQLInfo{ID: activeTenID},
			},
			execCfg.DefaultZoneConfig,
			false, /* ifNotExists */
			execCfg.TenantTestingKnobs,
		)
		return err
	}))

	require.NoError(t, execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		_, err := sql.CreateTenantRecord(
			ctx, execCfg.Codec, execCfg.Settings,
			txn,
			execCfg.SpanConfigKVAccessor.WithISQLTxn(ctx, txn),
			&mtinfopb.TenantInfoWithUsage{
				SQLInfo: mtinfopb.SQLInfo{
					ID:        dropTenID,
					DataState: mtinfopb.DataStateDrop,
				},
			},
			execCfg.DefaultZoneConfig,
			false, /* ifNotExists */
			execCfg.TenantTestingKnobs,
		)
		return err
	}))

	t.Run("unexpected progress state", func(t *testing.T) {
		progress := &jobspb.SchemaChangeGCProgress{
			Tenant: &jobspb.SchemaChangeGCProgress_TenantProgress{
				Status: jobspb.SchemaChangeGCProgress_WAITING_FOR_CLEAR,
			},
		}
		require.EqualError(
			t,
			gcClosure(10, progress),
			"tenant ID 10 is expired and should not be in state WAITING_FOR_CLEAR",
		)
		require.Equal(t, jobspb.SchemaChangeGCProgress_WAITING_FOR_CLEAR, progress.Tenant.Status)
	})

	t.Run("non-existent tenant deleting progress", func(t *testing.T) {
		progress := &jobspb.SchemaChangeGCProgress{
			Tenant: &jobspb.SchemaChangeGCProgress_TenantProgress{
				Status: jobspb.SchemaChangeGCProgress_CLEARING,
			},
		}
		require.NoError(t, gcClosure(nonexistentTenID, progress))
		require.Equal(t, jobspb.SchemaChangeGCProgress_CLEARED, progress.Tenant.Status)
	})

	t.Run("existent tenant deleted progress", func(t *testing.T) {
		progress := &jobspb.SchemaChangeGCProgress{
			Tenant: &jobspb.SchemaChangeGCProgress_TenantProgress{
				Status: jobspb.SchemaChangeGCProgress_CLEARED,
			},
		}
		require.ErrorContains(
			t,
			gcClosure(dropTenID, progress),
			`GC state for tenant is DELETED yet the tenant row still exists`,
		)
	})

	t.Run("active tenant GC", func(t *testing.T) {
		progress := &jobspb.SchemaChangeGCProgress{
			Tenant: &jobspb.SchemaChangeGCProgress_TenantProgress{
				Status: jobspb.SchemaChangeGCProgress_CLEARING,
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
				Status: jobspb.SchemaChangeGCProgress_CLEARING,
			},
		}

		descKey := catalogkeys.MakeDescMetadataKey(
			keys.MakeSQLCodec(roachpb.MustMakeTenantID(dropTenID)), keys.NamespaceTableID,
		)
		require.NoError(t, kvDB.Put(ctx, descKey, "foo"))

		r, err := kvDB.Get(ctx, descKey)
		require.NoError(t, err)
		val, err := r.Value.GetBytes()
		require.NoError(t, err)
		require.Equal(t, []byte("foo"), val)

		require.NoError(t, gcClosure(dropTenID, progress))
		require.Equal(t, jobspb.SchemaChangeGCProgress_CLEARED, progress.Tenant.Status)
		err = execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			_, err := sql.GetTenantRecordByID(ctx, txn, roachpb.MustMakeTenantID(dropTenID), execCfg.Settings)
			return err
		})
		require.EqualError(t, err, `tenant "11" does not exist`)
		require.NoError(t, gcClosure(dropTenID, progress))

		r, err = kvDB.Get(ctx, descKey)
		require.NoError(t, err)
		require.True(t, nil == r.Value)
	})
}

// This test exercises code whereby an GC job is running, and, in the
// meantime, the descriptor is removed. We want to ensure that the GC job
// finishes without an error. We want to test this both for index drops
// and for table drops.
func TestDropWithDeletedDescriptor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	runTest := func(t *testing.T, dropIndex bool, beforeDelRange bool) {
		ctx, cancel := context.WithCancel(context.Background())
		gcJobID := make(chan jobspb.JobID)
		knobs := base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			GCJob: &sql.GCJobTestingKnobs{
				RunBeforeResume: func(jobID jobspb.JobID) error {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case gcJobID <- jobID:
						return nil
					}
				},
				SkipWaitingForMVCCGC: true,
			},
		}
		delRangeChan := make(chan chan struct{})
		var tablePrefix atomic.Value
		tablePrefix.Store(roachpb.Key{})
		// If not running beforeDelRange, we want to delete the descriptor during
		// the DeleteRange operation. To do this, we install the below testing knob.
		if !beforeDelRange {
			knobs.Store = &kvserver.StoreTestingKnobs{
				TestingRequestFilter: func(
					ctx context.Context, request *kvpb.BatchRequest,
				) *kvpb.Error {
					req, ok := request.GetArg(kvpb.DeleteRange)
					if !ok {
						return nil
					}
					dr := req.(*kvpb.DeleteRangeRequest)
					if !dr.UseRangeTombstone {
						return nil
					}
					k := tablePrefix.Load().(roachpb.Key)
					if len(k) == 0 {
						return nil
					}
					ch := make(chan struct{})
					select {
					case delRangeChan <- ch:
					case <-ctx.Done():
					}
					select {
					case <-ch:
					case <-ctx.Done():
					}
					return nil
				},
			}
		}
		s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{
			Knobs: knobs,
		})
		defer s.Stopper().Stop(ctx)
		defer cancel()
		tdb := sqlutils.MakeSQLRunner(sqlDB)

		// Create the table and index to be dropped.
		tdb.Exec(t, "CREATE TABLE foo (i INT PRIMARY KEY, j INT, INDEX(j, i))")
		// Store the relevant IDs to make it easy to intercept the DelRange.
		var tableID catid.DescID
		var indexID catid.IndexID
		tdb.QueryRow(t, `
SELECT descriptor_id, index_id
  FROM crdb_internal.table_indexes
 WHERE descriptor_name = 'foo'
   AND index_name = 'foo_j_i_idx';`).Scan(&tableID, &indexID)
		// Drop the index.
		if dropIndex {
			tdb.Exec(t, "DROP INDEX foo@foo_j_i_idx")
		} else {
			tdb.Exec(t, "DROP TABLE foo")
		}

		codec := s.ExecutorConfig().(sql.ExecutorConfig).Codec
		tablePrefix.Store(codec.TablePrefix(uint32(tableID)))

		deleteDescriptor := func(t *testing.T) {
			t.Helper()
			k := catalogkeys.MakeDescMetadataKey(codec, tableID)
			_, err := kvDB.Del(ctx, k)
			require.NoError(t, err)
		}

		// Delete the descriptor either before the initial job run, or after
		// the job has started, but during the sending of DeleteRange requests.
		var jobID jobspb.JobID
		if beforeDelRange {
			deleteDescriptor(t)
			jobID = <-gcJobID
		} else {
			jobID = <-gcJobID
			ch := <-delRangeChan
			deleteDescriptor(t)
			close(ch)
		}
		// Ensure that the job completes successfully in either case.
		jr := s.JobRegistry().(*jobs.Registry)
		require.NoError(t, jr.WaitForJobs(ctx, []jobspb.JobID{jobID}))
	}

	// The way the GC job works is that it initially clears the index
	// data, then it waits for the background MVCC GC to run and remove
	// the underlying tombstone, and then finally it removes any relevant
	// zone configurations for the index from system.zones. In the first
	// and final phases, the job resolves the descriptor. This test ensures
	// that the code is robust to the descriptor being removed both before
	// the initial DelRange, and after, when going to remove the zone config.
	testutils.RunTrueAndFalse(t, "before DelRange", func(
		t *testing.T, beforeDelRange bool,
	) {
		testutils.RunTrueAndFalse(t, "drop index", func(t *testing.T, dropIndex bool) {
			runTest(t, dropIndex, beforeDelRange)
		})
	})
}
