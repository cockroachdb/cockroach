// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/gcjob"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestGCTenantRemovesSpanConfigs ensures that GC-ing a tenant removes all
// span/system span configs installed by it.
func TestGCTenantRemovesSpanConfigs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	ts := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
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

	sys := ts.SystemLayer()

	execCfg := sys.ExecutorConfig().(sql.ExecutorConfig)
	scKVAccessor := sys.SpanConfigKVAccessor().(spanconfig.KVAccessor)

	gcClosure := func(tenID uint64, progress *jobspb.SchemaChangeGCProgress) error {
		return gcjob.TestingGCTenant(ctx, &execCfg, tenID, progress)
	}

	tenantID := roachpb.MustMakeTenantID(10)

	tt, err := ts.TenantController().StartTenant(ctx, base.TestTenantArgs{
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
	err = tenantKVAccessor.UpdateSpanConfigRecords(
		ctx, nil, []spanconfig.Record{rec}, hlc.MinTimestamp, hlc.MaxTimestamp,
	)
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

	require.NoError(t, sys.InternalDB().(isql.DB).Txn(ctx, func(
		ctx context.Context, txn isql.Txn,
	) error {
		return sql.TestingUpdateTenantRecord(
			ctx, sys.ClusterSettings(), txn,
			&mtinfopb.TenantInfo{
				SQLInfo: mtinfopb.SQLInfo{
					ID:          tenantID.ToUint64(),
					ServiceMode: mtinfopb.ServiceModeNone,
					DataState:   mtinfopb.DataStateDrop,
				}},
		)
	}))

	// Run GC on the tenant.
	progress := &jobspb.SchemaChangeGCProgress{
		Tenant: &jobspb.SchemaChangeGCProgress_TenantProgress{
			Status: jobspb.SchemaChangeGCProgress_CLEARING,
		},
	}
	require.NoError(t, gcClosure(tenantID.ToUint64(), progress))
	require.Equal(t, jobspb.SchemaChangeGCProgress_CLEARED, progress.Tenant.Status)

	// Ensure the tenant's span configs and system span configs have been deleted.
	records, err = scKVAccessor.GetSpanConfigRecords(
		ctx, spanconfig.TestingEntireSpanConfigurationStateTargets(),
	)
	require.NoError(t, err)
	require.Equal(t, len(records), beforeDelete-2)
}

// TestGCTenantJobWaitsForProtectedTimestamps tests that the GC job responsible
// for clearing a dropped tenant's data respects protected timestamp records
// written by the system tenant on the secondary tenant's keyspace.
func TestGCTenantJobWaitsForProtectedTimestamps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	defer gcjob.SetSmallMaxGCIntervalForTest()()

	ctx := context.Background()
	args := base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	}
	srv, sqlDBRaw, _ := serverutils.StartServer(t, args)
	sqlDB := sqlutils.MakeSQLRunner(sqlDBRaw)

	sqlDB.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'")

	execCfg := srv.ExecutorConfig().(sql.ExecutorConfig)
	jobRegistry := execCfg.JobRegistry
	defer srv.Stopper().Stop(ctx)

	insqlDB := execCfg.InternalDB
	ptp := execCfg.ProtectedTimestampProvider
	mkRecordAndProtect := func(ts hlc.Timestamp, target *ptpb.Target) *ptpb.Record {
		recordID := uuid.MakeV4()
		rec := jobsprotectedts.MakeRecord(recordID, int64(1), ts, nil, /* deprecatedSpans */
			jobsprotectedts.Jobs, target)
		require.NoError(t, insqlDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			return ptp.WithTxn(txn).Protect(ctx, rec)
		}))
		return rec
	}

	checkGCBlockedByPTS := func(t *testing.T, sj *jobs.StartableJob, tenID uint64) {
		testutils.SucceedsSoon(t, func() error {
			log.FlushFiles()
			entries, err := log.FetchEntriesFromFiles(0, math.MaxInt64, 1,
				regexp.MustCompile(fmt.Sprintf("GC TTL for dropped tenant %d has expired, but protected timestamp record\\(s\\)", tenID)),
				log.WithFlattenedSensitiveData)
			if err != nil {
				return err
			}
			if len(entries) == 0 {
				return errors.New("expected protected timestamp record to hold up GC")
			}

			// Sanity check that the GC job is running.
			job, err := jobRegistry.LoadJob(ctx, sj.ID())
			if err != nil {
				return err
			}
			if job.State() != jobs.StateRunning {
				return errors.Newf("expected job to have StatusRunning but found %+v", job.State())
			}
			return nil
		})
	}

	checkTenantGCed := func(t *testing.T, sj *jobs.StartableJob, tenID roachpb.TenantID) {
		// Await job completion, and check that the tenant has been GC'ed once the
		// PTS record has been released.
		require.NoError(t, sj.AwaitCompletion(ctx))
		job, err := jobRegistry.LoadJob(ctx, sj.ID())
		require.NoError(t, err)
		require.Equal(t, jobs.StateSucceeded, job.State())
		err = insqlDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			_, err = sql.GetTenantRecordByID(ctx, txn, tenID, srv.ClusterSettings())
			return err
		})
		require.EqualError(t, err, fmt.Sprintf(`tenant "%d" does not exist`, tenID.ToUint64()))
		progress := job.Progress()
		require.Equal(t, jobspb.SchemaChangeGCProgress_CLEARED, progress.GetSchemaChangeGC().Tenant.Status)
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
			Username: username.RootUserName(),
		}

		tenantTarget := ptpb.MakeTenantsTarget([]roachpb.TenantID{roachpb.MustMakeTenantID(tenID)})
		rec := mkRecordAndProtect(hlc.Timestamp{WallTime: int64(dropTime - 1)}, tenantTarget)
		sj, err := jobs.TestingCreateAndStartJob(ctx, jobRegistry, insqlDB, record)
		require.NoError(t, err)

		checkGCBlockedByPTS(t, sj, tenID)

		// Release the record.
		require.NoError(t, insqlDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			require.NoError(t, ptp.WithTxn(txn).Release(ctx, rec.ID.GetUUID()))
			return nil
		}))

		checkTenantGCed(t, sj, roachpb.MustMakeTenantID(tenID))
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
			Username: username.RootUserName(),
		}

		// Protect after drop time, so it should not block GC.
		clusterTarget := ptpb.MakeClusterTarget()
		clusterRec := mkRecordAndProtect(hlc.Timestamp{WallTime: int64(dropTime + 1)}, clusterTarget)

		// Protect at drop time, so it should not block GC.
		tenantTarget := ptpb.MakeTenantsTarget([]roachpb.TenantID{roachpb.MustMakeTenantID(tenID)})
		tenantRec := mkRecordAndProtect(hlc.Timestamp{WallTime: int64(dropTime)}, tenantTarget)

		sj, err := jobs.TestingCreateAndStartJob(ctx, jobRegistry, insqlDB, record)
		require.NoError(t, err)

		checkTenantGCed(t, sj, roachpb.MustMakeTenantID(tenID))

		// Cleanup.
		require.NoError(t, insqlDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			ptps := ptp.WithTxn(txn)
			require.NoError(t, ptps.Release(ctx, clusterRec.ID.GetUUID()))
			require.NoError(t, ptps.Release(ctx, tenantRec.ID.GetUUID()))
			return nil
		}))
	})

	t.Run("tenant set PTS records dont affect tenant GC", func(t *testing.T) {
		tenID := roachpb.MustMakeTenantID(10)
		sqlDB.Exec(t, "ALTER RANGE tenants CONFIGURE ZONE USING gc.ttlseconds = 1;")

		tenantStopper := stop.NewStopper()
		defer tenantStopper.Stop(ctx) // in case the test fails prematurely.

		ten, conn10 := serverutils.StartTenant(t, srv,
			base.TestTenantArgs{TenantID: tenID, Stopper: tenantStopper})
		defer conn10.Close()

		// Write a cluster PTS record as the tenant.
		tenPtp := ten.ExecutorConfig().(sql.ExecutorConfig).ProtectedTimestampProvider
		clusterTarget := ptpb.MakeClusterTarget()
		recordID := uuid.MakeV4()
		rec := jobsprotectedts.MakeRecord(recordID, int64(1),
			hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}, nil, /* deprecatedSpans */
			jobsprotectedts.Jobs, clusterTarget)
		tenInsqlDB := ten.ExecutorConfig().(sql.ExecutorConfig).InternalDB
		require.NoError(t, tenInsqlDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			return tenPtp.WithTxn(txn).Protect(ctx, rec)
		}))

		// Ensure the secondary tenant is not running any more tasks.
		tenantStopper.Stop(ctx)

		// Drop the record.
		sqlDB.Exec(t, `ALTER TENANT [$1] STOP SERVICE`, tenID.ToUint64())
		sqlDB.Exec(t, `DROP TENANT [$1]`, tenID.ToUint64())

		sqlDB.CheckQueryResultsRetry(
			t,
			"SELECT status FROM [SHOW JOBS] WHERE description = 'GC for tenant 10'",
			[][]string{{"succeeded"}},
		)
		err := insqlDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			_, err := sql.GetTenantRecordByID(ctx, txn, tenID, execCfg.Settings)
			return err
		})
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
				Username: username.RootUserName(),
			}

			clusterTarget := ptpb.MakeClusterTarget()
			rec := mkRecordAndProtect(hlc.Timestamp{WallTime: int64(dropTime - 1)}, clusterTarget)
			sj, err := jobs.TestingCreateAndStartJob(ctx, jobRegistry, insqlDB, record)
			require.NoError(t, err)

			checkGCBlockedByPTS(t, sj, tenID)

			// Release the record.
			require.NoError(t, insqlDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
				require.NoError(t, ptp.WithTxn(txn).Release(ctx, rec.ID.GetUUID()))
				return nil
			}))

			checkTenantGCed(t, sj, roachpb.MustMakeTenantID(tenID))
		})
	})
}
