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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/gcjob"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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

// TestGCTablesWaitsForProtectedTimestamps is an integration test to ensure that
// the GC job responsible for clearing ranging a dropped table's data respects
// protected timestamps. Sketch:
// Setup of PTS-es on system span configs:
// Entire keyspace:  (ts@8, ts@23, ts@30)
// Host on secondary tenant: (ts@6, ts@28, ts@35)
// Tenant on tenant: (ts@2, ts@40)
// Host on another secondary tenant not being tested: (ts@3, ts@4, ts@5)
// We drop the table at ts@10. We then start updating system span configs or
// entirely removing them to unblock GC in the order above. For the host, we
// ensure only the PTS from the entire keyspace target applies. For the
// secondary tenant variant, we ensure that the first 3 system span configs
// all have effect.
func TestGCTablesWaitsForProtectedTimestamps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer gcjob.SetSmallMaxGCIntervalForTest()

	var mu struct {
		syncutil.Mutex

		isSet       bool
		isProtected bool
	}

	ctx := context.Background()
	ts, sqlDBRaw, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			GCJob: &sql.GCJobTestingKnobs{
				RunAfterIsProtectedCheck: func(isProtected bool) {
					mu.Lock()
					defer mu.Unlock()

					mu.isSet = true
					mu.isProtected = isProtected
				},
			},
		},
	})
	defer ts.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(sqlDBRaw)

	sqlDB.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'")
	tsKVAccessor := ts.SpanConfigKVAccessor().(spanconfig.KVAccessor)

	tenantID := roachpb.MakeTenantID(10)

	tt, ttSQLDBRaw := serverutils.StartTenant(
		t, ts, base.TestTenantArgs{
			TenantID: tenantID,
			TestingKnobs: base.TestingKnobs{
				GCJob: &sql.GCJobTestingKnobs{
					RunAfterIsProtectedCheck: func(isProtected bool) {
						mu.Lock()
						defer mu.Unlock()

						mu.isSet = true
						mu.isProtected = isProtected
					},
				},
			},
			Stopper: ts.Stopper(),
		},
	)
	ttSQLDB := sqlutils.MakeSQLRunner(ttSQLDBRaw)
	ttKVAccessor := ts.SpanConfigKVAccessor().(spanconfig.KVAccessor)

	makeGCJobRecord := func(dropTime int64, id descpb.ID) jobs.Record {
		return jobs.Record{
			Details: jobspb.SchemaChangeGCDetails{
				Tables: []jobspb.SchemaChangeGCDetails_DroppedID{
					{
						ID:       id,
						DropTime: dropTime,
					},
				},
			},
			Progress: jobspb.SchemaChangeGCProgress{},
		}
	}

	resetTestingKnob := func() {
		mu.Lock()
		defer mu.Unlock()
		mu.isSet = false
	}

	makeSystemSpanConfig := func(nanosTimestamps ...int) roachpb.SpanConfig {
		var protectionPolicies []roachpb.ProtectionPolicy
		for _, nanos := range nanosTimestamps {
			protectionPolicies = append(protectionPolicies, roachpb.ProtectionPolicy{
				ProtectedTimestamp: hlc.Timestamp{
					WallTime: int64(nanos),
				},
			})
		}
		return roachpb.SpanConfig{
			GCPolicy: roachpb.GCPolicy{
				ProtectionPolicies: protectionPolicies,
			},
		}
	}

	ensureGCBlockedByPTS := func(t *testing.T, registry *jobs.Registry, sj *jobs.StartableJob) {
		testutils.SucceedsSoon(t, func() error {
			job, err := registry.LoadJob(ctx, sj.ID())
			require.NoError(t, err)
			require.Equal(t, jobs.StatusRunning, job.Status())

			mu.Lock()
			defer mu.Unlock()
			if !mu.isSet {
				return errors.Newf("waiting for isProtected status to be set")
			}
			require.Equal(t, true, mu.isProtected)
			return nil
		})
	}

	ensureGCSucceeded := func(t *testing.T, registry *jobs.Registry, sj *jobs.StartableJob) {
		// Await job completion, ensure protection status was checked, and it was
		// determined safe to GC.
		require.NoError(t, sj.AwaitCompletion(ctx))
		job, err := registry.LoadJob(ctx, sj.ID())
		require.NoError(t, err)
		require.Equal(t, jobs.StatusSucceeded, job.Status())
		progress := job.Progress()
		require.Equal(t, jobspb.SchemaChangeGCProgress_DELETED, progress.GetSchemaChangeGC().Tables[0].Status)

		mu.Lock()
		defer mu.Unlock()
		require.Equal(t, false, mu.isProtected)
	}

	testutils.RunTrueAndFalse(t, "secondary-tenant", func(t *testing.T, forSecondaryTenant bool) {
		resetTestingKnob()
		if !forSecondaryTenant {
			return
		}
		registry := ts.JobRegistry().(*jobs.Registry)
		execCfg := ts.ExecutorConfig().(sql.ExecutorConfig)
		sqlRunner := sqlDB
		if forSecondaryTenant {
			registry = tt.JobRegistry().(*jobs.Registry)
			execCfg = tt.ExecutorConfig().(sql.ExecutorConfig)
			sqlRunner = ttSQLDB
		}
		sqlRunner.Exec(t, "CREATE TABLE t()")
		var tableID int
		sqlRunner.QueryRow(t, "SELECT id FROM system.namespace WHERE name = 't'").Scan(&tableID)
		// Drop the table. We'll also create another GC job for this table with
		// an injected drop time.
		sqlRunner.Exec(t, "DROP TABLE t")

		// Write a PTS over the entire keyspace. We'll include multiple
		// timestamps, one of which is before the drop time.
		r1, err := spanconfig.MakeRecord(
			spanconfig.MakeTargetFromSystemTarget(spanconfig.MakeEntireKeyspaceTarget()),
			makeSystemSpanConfig(8, 23, 30),
		)

		// We do something similar for a PTS set by the host over the tenant's
		// keyspace.
		hostOnTenant, err := spanconfig.MakeTenantKeyspaceTarget(roachpb.SystemTenantID, tenantID)
		require.NoError(t, err)
		r2, err := spanconfig.MakeRecord(
			spanconfig.MakeTargetFromSystemTarget(hostOnTenant),
			makeSystemSpanConfig(6, 28, 35),
		)

		// Lastly, we'll also add a PTS set by the host over a different
		// secondary tenant's keyspace. This should have no bearing on our test.
		hostOnTenant20, err := spanconfig.MakeTenantKeyspaceTarget(
			roachpb.SystemTenantID, roachpb.MakeTenantID(20),
		)
		require.NoError(t, err)
		r3, err := spanconfig.MakeRecord(
			spanconfig.MakeTargetFromSystemTarget(hostOnTenant20),
			makeSystemSpanConfig(3, 4, 5),
		)

		require.NoError(t, err)
		err = tsKVAccessor.UpdateSpanConfigRecords(ctx, nil /*toDelete */, []spanconfig.Record{r1, r2, r3})
		require.NoError(t, err)

		// One more, this time set by the tenant itself on its entire keyspace.
		// Note that we must do this upsert using the tenant's KVAccessor.
		tenantOnTenant, err := spanconfig.MakeTenantKeyspaceTarget(tenantID, tenantID)
		require.NoError(t, err)
		r4, err := spanconfig.MakeRecord(
			spanconfig.MakeTargetFromSystemTarget(tenantOnTenant),
			makeSystemSpanConfig(2, 40),
		)
		require.NoError(t, err)
		err = ttKVAccessor.UpdateSpanConfigRecords(ctx, nil /*toDelete */, []spanconfig.Record{r4})
		require.NoError(t, err)

		sj, err := jobs.TestingCreateAndStartJob(
			ctx, registry, execCfg.DB, makeGCJobRecord(10, descpb.ID(tableID)),
		)
		require.NoError(t, err)

		ensureGCBlockedByPTS(t, registry, sj)

		// Update PTS state that applies to the entire keyspace -- we only
		// remove the timestamp before the drop time. We should expect GC to
		// succeed on if we're not testing for the system tenant at this point.
		r1, err = spanconfig.MakeRecord(
			spanconfig.MakeTargetFromSystemTarget(spanconfig.MakeEntireKeyspaceTarget()),
			makeSystemSpanConfig(23, 30),
		)
		require.NoError(t, err)
		err = tsKVAccessor.UpdateSpanConfigRecords(ctx, nil /*toDelete */, []spanconfig.Record{r1})
		require.NoError(t, err)

		resetTestingKnob()

		if !forSecondaryTenant {
			ensureGCSucceeded(t, registry, sj)
			return
		}

		// Next, we'll remove the host set system span config on our secondary
		// tenant.
		r1, err = spanconfig.MakeRecord(
			spanconfig.MakeTargetFromSystemTarget(spanconfig.MakeEntireKeyspaceTarget()),
			makeSystemSpanConfig(23, 30),
		)
		require.NoError(t, err)
		err = tsKVAccessor.UpdateSpanConfigRecords(ctx, []spanconfig.Target{r2.GetTarget()}, nil /* ToUpdate */)
		require.NoError(t, err)

		resetTestingKnob()
		ensureGCBlockedByPTS(t, registry, sj)

		// The only remaining PTS is from the system span config applied by
		// the secondary tenant itself.
		err = ttKVAccessor.UpdateSpanConfigRecords(ctx, []spanconfig.Target{r4.GetTarget()}, nil /* ToUpdate */)
		require.NoError(t, err)

		// At this point, GC should succeed.
		resetTestingKnob()
		ensureGCSucceeded(t, registry, sj)
	})
}

// TestGCTenantJobWaitsForProtectedTimestamps tests that the GC job responsible
// for clearing a dropped tenant's data respects protected timestamp records
// written by the system tenant on the secondary tenant's keyspace.
func TestGCTenantJobWaitsForProtectedTimestamps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer gcjob.SetSmallMaxGCIntervalForTest()

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

	checkGCBlockedByPTS := func(t *testing.T, sj *jobs.StartableJob, tenID uint64) {
		testutils.SucceedsSoon(t, func() error {
			log.Flush()
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
			if job.Status() != jobs.StatusRunning {
				return errors.Newf("expected job to have StatusRunning but found %+v", job.Status())
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

		checkGCBlockedByPTS(t, sj, tenID)

		// Release the record.
		require.NoError(t, execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			require.NoError(t, ptp.Release(ctx, txn, rec.ID.GetUUID()))
			return nil
		}))

		checkTenantGCed(t, sj, roachpb.MakeTenantID(tenID))
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

		checkTenantGCed(t, sj, roachpb.MakeTenantID(tenID))

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

		ten, conn10 := serverutils.StartTenant(t, srv,
			base.TestTenantArgs{TenantID: tenID, Stopper: srv.Stopper()})
		defer conn10.Close()

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

			checkGCBlockedByPTS(t, sj, tenID)

			// Release the record.
			require.NoError(t, execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				require.NoError(t, ptp.Release(ctx, txn, rec.ID.GetUUID()))
				return nil
			}))

			checkTenantGCed(t, sj, roachpb.MakeTenantID(tenID))
		})
	})
}
