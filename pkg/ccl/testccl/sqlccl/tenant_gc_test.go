// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package sqlccl

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/gcjob"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

// TestGCTenantRemovesSpanConfigs ensures that GC-ing a tenant removes all
// span/system span configs installed by it.
func TestGCTenantRemovesSpanConfigs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	ts, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SpanConfig: &spanconfig.TestingKnobs{
				// Disable the system tenant's reconciliation process so that we can
				// make assertions on the total number of span configurations in the
				// system.
				ManagerDisableJobCreation: true,
			},
		},
	})
	defer ts.Stopper().Stop(ctx)
	execCfg := ts.ExecutorConfig().(sql.ExecutorConfig)
	scKVAccessor := ts.SpanConfigKVAccessor().(spanconfig.KVAccessor)

	gcClosure := func(tenID uint64, progress *jobspb.SchemaChangeGCProgress) error {
		return gcjob.TestingGCTenant(ctx, &execCfg, tenID, progress)
	}

	tenantID := roachpb.MakeTenantID(10)

	tt, err := ts.StartTenant(ctx, base.TestTenantArgs{
		TenantID: tenantID,
		TestingKnobs: base.TestingKnobs{
			SpanConfig: &spanconfig.TestingKnobs{
				// Disable the tenant's span config reconciliation process, we'll
				// instead manually add system span configs via the KVAccessor.
				ManagerDisableJobCreation: true,
			},
		},
	})
	require.NoError(t, err)

	tenantKVAccessor := tt.SpanConfigKVAccessor().(spanconfig.KVAccessor)
	// Write a system span config, set by the tenant, targeting its entire
	// keyspace.
	systemTarget, err := spanconfig.MakeTenantKeyspaceTarget(tenantID, tenantID)
	require.NoError(t, err)
	rec, err := spanconfig.MakeRecord(spanconfig.MakeTargetFromSystemTarget(systemTarget), roachpb.SpanConfig{})
	require.NoError(t, err)
	err = tenantKVAccessor.UpdateSpanConfigRecords(ctx, nil /* toDelete */, []spanconfig.Record{rec})
	require.NoError(t, err)

	// Ensure there are 2 configs for the tenant -- one that spans its entire
	// keyspace, installed on creation, and of course the system span config we
	// inserted above.
	tenPrefix := keys.MakeTenantPrefix(tenantID)
	records, err := tenantKVAccessor.GetSpanConfigRecords(ctx, spanconfig.Targets{
		spanconfig.MakeTargetFromSpan(roachpb.Span{Key: tenPrefix, EndKey: tenPrefix.PrefixEnd()}),
		spanconfig.MakeTargetFromSystemTarget(systemTarget),
	})
	require.NoError(t, err)
	require.Equal(t, 2, len(records))

	// Get the entire span config state, from the system tenant's perspective,
	// which we'll use to compare against once the tenant is GC-ed.
	records, err = scKVAccessor.GetSpanConfigRecords(
		ctx, spanconfig.TestingEntireSpanConfigurationStateTargets(),
	)
	require.NoError(t, err)
	beforeDelete := len(records)

	// Mark the tenant as dropped by updating its record.
	require.NoError(t, sql.TestingUpdateTenantRecord(
		ctx, &execCfg, nil, /* txn */
		&descpb.TenantInfo{ID: tenantID.ToUint64(), State: descpb.TenantInfo_DROP},
	))

	// Run GC on the tenant.
	progress := &jobspb.SchemaChangeGCProgress{
		Tenant: &jobspb.SchemaChangeGCProgress_TenantProgress{
			Status: jobspb.SchemaChangeGCProgress_DELETING,
		},
	}
	require.NoError(t, gcClosure(tenantID.ToUint64(), progress))
	require.Equal(t, jobspb.SchemaChangeGCProgress_DELETED, progress.Tenant.Status)

	// Ensure the tenant's span configs and system span configs have been deleted.
	records, err = scKVAccessor.GetSpanConfigRecords(
		ctx, spanconfig.TestingEntireSpanConfigurationStateTargets(),
	)
	require.NoError(t, err)
	require.Equal(t, len(records), beforeDelete-2)
}

func TestGCTenantJobWaitsForProtectedTimestamps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	gcjob.SetSmallMaxGCIntervalForTest()

	ctx := context.Background()
	args := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	}
	srv, sqlDBRaw, kvDB := serverutils.StartServer(t, args)
	sqlDB := sqlutils.MakeSQLRunner(sqlDBRaw)

	sqlDB.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'")

	execCfg := srv.ExecutorConfig().(sql.ExecutorConfig)
	jobRegistry := execCfg.JobRegistry
	defer srv.Stopper().Stop(ctx)

	ptp := execCfg.ProtectedTimestampProvider
	mkRecordAndProtect := func(ts hlc.Timestamp, target *ptpb.Target) *ptpb.Record {
		recordID := uuid.MakeV4()
		rec := jobsprotectedts.MakeRecord(recordID, int64(1), ts, nil, /* deprecatedSpans */
			jobsprotectedts.Jobs, target)
		require.NoError(t, execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			return ptp.Protect(ctx, txn, rec)
		}))
		return rec
	}

	checkGCBlockedByPTS := func(sj *jobs.StartableJob, tenID uint64) {
		log.Flush()
		entries, err := log.FetchEntriesFromFiles(0, math.MaxInt64, 1,
			regexp.MustCompile(fmt.Sprintf("GC TTL for dropped tenant %d has expired, "+
				"but protected timestamprecord(s) on the tenant keyspace are preventing GC", tenID)),
			log.WithFlattenedSensitiveData)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) > 0 {
			t.Fatal("expected protected timestamp record to hold up GC")
		}

		// Sanity check that the GC job is running.
		job, err := jobRegistry.LoadJob(ctx, sj.ID())
		require.NoError(t, err)
		require.Equal(t, jobs.StatusRunning, job.Status())
	}

	checkTenantGCed := func(sj *jobs.StartableJob, tenID roachpb.TenantID) {
		// Await job completion, and check that the tenant has been GC'ed once the
		// PTS record has been released.
		require.NoError(t, sj.AwaitCompletion(ctx))
		job, err := jobRegistry.LoadJob(ctx, sj.ID())
		require.NoError(t, err)
		require.Equal(t, jobs.StatusSucceeded, job.Status())
		_, err = sql.GetTenantRecord(ctx, &execCfg, nil /* txn */, tenID.ToUint64())
		require.EqualError(t, err, fmt.Sprintf(`tenant "%d" does not exist`, tenID.ToUint64()))
		progress := job.Progress()
		require.Equal(t, jobspb.SchemaChangeGCProgress_DELETED, progress.GetSchemaChangeGC().Tenant.Status)
	}

	// PTS record protecting secondary tenant should block tenant GC.
	t.Run("protect tenant before drop time", func(t *testing.T) {
		const tenID = 11
		dropTime := 2
		record := jobs.Record{
			Details: jobspb.SchemaChangeGCDetails{
				Tenant: &jobspb.SchemaChangeGCDetails_DroppedTenant{
					ID:       tenID,
					DropTime: int64(dropTime), // guarantees the tenant will expire immediately.
				},
			},
			Progress: jobspb.SchemaChangeGCProgress{},
		}

		tenantTarget := ptpb.MakeTenantsTarget([]roachpb.TenantID{roachpb.MakeTenantID(tenID)})
		rec := mkRecordAndProtect(hlc.Timestamp{WallTime: int64(dropTime - 1)}, tenantTarget)
		sj, err := jobs.TestingCreateAndStartJob(ctx, jobRegistry, kvDB, record)
		require.NoError(t, err)

		checkGCBlockedByPTS(sj, tenID)

		// Release the record.
		require.NoError(t, execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			require.NoError(t, ptp.Release(ctx, txn, rec.ID.GetUUID()))
			return nil
		}))

		checkTenantGCed(sj, roachpb.MakeTenantID(tenID))
	})

	t.Run("protect at and after drop time", func(t *testing.T) {
		const tenID = 12
		dropTime := 2
		record := jobs.Record{
			Details: jobspb.SchemaChangeGCDetails{
				Tenant: &jobspb.SchemaChangeGCDetails_DroppedTenant{
					ID:       tenID,
					DropTime: int64(dropTime), // guarantees the tenant will expire immediately.
				},
			},
			Progress: jobspb.SchemaChangeGCProgress{},
		}

		// Protect after drop time, so it should not block GC.
		clusterTarget := ptpb.MakeClusterTarget()
		clusterRec := mkRecordAndProtect(hlc.Timestamp{WallTime: int64(dropTime + 1)}, clusterTarget)

		// Protect at drop time, so it should not block GC.
		tenantTarget := ptpb.MakeTenantsTarget([]roachpb.TenantID{roachpb.MakeTenantID(tenID)})
		tenantRec := mkRecordAndProtect(hlc.Timestamp{WallTime: int64(dropTime)}, tenantTarget)

		sj, err := jobs.TestingCreateAndStartJob(ctx, jobRegistry, kvDB, record)
		require.NoError(t, err)

		checkTenantGCed(sj, roachpb.MakeTenantID(tenID))

		// Cleanup.
		require.NoError(t, execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			require.NoError(t, ptp.Release(ctx, txn, clusterRec.ID.GetUUID()))
			require.NoError(t, ptp.Release(ctx, txn, tenantRec.ID.GetUUID()))
			return nil
		}))
	})

	t.Run("tenant set PTS records dont affect tenant GC", func(t *testing.T) {
		tenID := roachpb.MakeTenantID(10)
		sqlDB.Exec(t, "ALTER RANGE tenants CONFIGURE ZONE USING gc.ttlseconds = 1;")

		ten, conn11 := serverutils.StartTenant(t, srv,
			base.TestTenantArgs{TenantID: tenID, Stopper: srv.Stopper()})
		defer conn11.Close()

		// Write a cluster PTS record as the tenant.
		tenPtp := ten.ExecutorConfig().(sql.ExecutorConfig).ProtectedTimestampProvider
		clusterTarget := ptpb.MakeClusterTarget()
		recordID := uuid.MakeV4()
		rec := jobsprotectedts.MakeRecord(recordID, int64(1),
			hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}, nil, /* deprecatedSpans */
			jobsprotectedts.Jobs, clusterTarget)
		require.NoError(t, execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			return tenPtp.Protect(ctx, txn, rec)
		}))

		sqlDB.Exec(t, fmt.Sprintf(`SELECT crdb_internal.destroy_tenant(%d)`,
			tenID.ToUint64()))

		sqlDB.CheckQueryResultsRetry(
			t,
			"SELECT status FROM [SHOW JOBS] WHERE description = 'GC for tenant 10'",
			[][]string{{"succeeded"}},
		)
		_, err := sql.GetTenantRecord(ctx, &execCfg, nil /* txn */, tenID.ToUint64())
		require.EqualError(t, err, `tenant "10" does not exist`)

		// PTS record protecting system tenant cluster should block tenant GC.
		t.Run("protect system tenant cluster before drop time", func(t *testing.T) {
			const tenID = 13
			dropTime := 2
			record := jobs.Record{
				Details: jobspb.SchemaChangeGCDetails{
					Tenant: &jobspb.SchemaChangeGCDetails_DroppedTenant{
						ID:       tenID,
						DropTime: int64(dropTime), // guarantees the tenant will expire immediately.
					},
				},
				Progress: jobspb.SchemaChangeGCProgress{},
			}

			clusterTarget := ptpb.MakeClusterTarget()
			rec := mkRecordAndProtect(hlc.Timestamp{WallTime: int64(dropTime - 1)}, clusterTarget)
			sj, err := jobs.TestingCreateAndStartJob(ctx, jobRegistry, kvDB, record)
			require.NoError(t, err)

			checkGCBlockedByPTS(sj, tenID)

			// Release the record.
			require.NoError(t, execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				require.NoError(t, ptp.Release(ctx, txn, rec.ID.GetUUID()))
				return nil
			}))

			checkTenantGCed(sj, roachpb.MakeTenantID(tenID))
		})
	})
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
