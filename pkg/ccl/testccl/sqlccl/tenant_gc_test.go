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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/gcjob"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
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

	tenantID := roachpb.MustMakeTenantID(10)

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

	require.NoError(t, ts.InternalDB().(isql.DB).Txn(ctx, func(
		ctx context.Context, txn isql.Txn,
	) error {
		return sql.TestingUpdateTenantRecord(
			ctx, ts.ClusterSettings(), txn,
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

// TestGCTableOrIndexWaitsForProtectedTimestamps is an integration test to
// ensure that the GC job responsible for clear ranging a dropped table or
// index's data respects protected timestamps. We run two variants -- one for
// tables and another for indexes. Moreover, these 2 variants are run for both
// the host tenant and a secondary tenant. The sketch is as follows:
// Setup of PTS-es on system span configs:
// Entire keyspace:  (ts@8, ts@23, ts@30)
// Host on secondary tenant: (ts@6, ts@28, ts@35)
// Tenant on tenant: (ts@2, ts@40)
// Host on another secondary tenant not being tested: (ts@3, ts@4, ts@5)
// We drop the object at ts@10. We then start updating system span configs or
// entirely removing them to unblock GC in the order above. For the host, we
// ensure only the PTS from the entire keyspace target applies. For the
// secondary tenant variant, we ensure that the first 3 system span configs
// all have effect.
func TestGCTableOrIndexWaitsForProtectedTimestamps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.WithIssue(t, 85876)
	defer gcjob.SetSmallMaxGCIntervalForTest()()

	var mu struct {
		syncutil.Mutex

		isSet       bool
		isProtected bool
		jobID       jobspb.JobID
	}

	ctx := context.Background()
	ts, sqlDBRaw, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			GCJob: &sql.GCJobTestingKnobs{
				RunAfterIsProtectedCheck: func(jobID jobspb.JobID, isProtected bool) {
					mu.Lock()
					defer mu.Unlock()

					if jobID != mu.jobID { // not the job we're interested in
						return
					}
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

	tenantID := roachpb.MustMakeTenantID(10)

	tt, ttSQLDBRaw := serverutils.StartTenant(
		t, ts, base.TestTenantArgs{
			TenantID: tenantID,
			TestingKnobs: base.TestingKnobs{
				GCJob: &sql.GCJobTestingKnobs{
					RunAfterIsProtectedCheck: func(jobID jobspb.JobID, isProtected bool) {
						mu.Lock()
						defer mu.Unlock()

						if jobID != mu.jobID { // not the job we're interested in
							return
						}
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

	makeIndexGCJobRecord := func(dropTime int64, indexID descpb.IndexID, parentID descpb.ID) jobs.Record {
		return jobs.Record{
			Details: jobspb.SchemaChangeGCDetails{
				Indexes: []jobspb.SchemaChangeGCDetails_DroppedIndex{
					{
						IndexID:  indexID,
						DropTime: dropTime,
					},
				},
				ParentID: parentID,
			},
			Progress: jobspb.SchemaChangeGCProgress{},
			Username: username.TestUserName(),
		}
	}
	makeTableGCJobRecord := func(dropTime int64, id descpb.ID) jobs.Record {
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
			Username: username.TestUserName(),
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
			require.True(t, mu.isProtected)
			return nil
		})
	}

	ensureGCSucceeded := func(t *testing.T, gcIndex bool, registry *jobs.Registry, sj *jobs.StartableJob) {
		// Await job completion, ensure protection status was checked, and it was
		// determined safe to GC.
		require.NoError(t, sj.AwaitCompletion(ctx))
		job, err := registry.LoadJob(ctx, sj.ID())
		require.NoError(t, err)
		require.Equal(t, jobs.StatusSucceeded, job.Status())
		progress := job.Progress()

		if gcIndex {
			require.Equal(t, jobspb.SchemaChangeGCProgress_CLEARED, progress.GetSchemaChangeGC().Indexes[0].Status)
		} else {
			require.Equal(t, jobspb.SchemaChangeGCProgress_CLEARED, progress.GetSchemaChangeGC().Tables[0].Status)
		}

		mu.Lock()
		defer mu.Unlock()
		require.Equal(t, false, mu.isProtected)
	}

	testutils.RunTrueAndFalse(t, "gc-index", func(t *testing.T, gcIndex bool) {
		testutils.RunTrueAndFalse(t, "secondary-tenant", func(t *testing.T, forSecondaryTenant bool) {
			resetTestingKnob()
			registry := ts.JobRegistry().(*jobs.Registry)
			execCfg := ts.ExecutorConfig().(sql.ExecutorConfig)
			sqlRunner := sqlDB
			if forSecondaryTenant {
				registry = tt.JobRegistry().(*jobs.Registry)
				execCfg = tt.ExecutorConfig().(sql.ExecutorConfig)
				sqlRunner = ttSQLDB
			}
			sqlRunner.Exec(t, "DROP DATABASE IF EXISTS db") // start with a clean slate
			sqlRunner.Exec(t, "CREATE DATABASE db")
			sqlRunner.Exec(t, "CREATE TABLE db.t(i INT, j INT)")
			sqlRunner.Exec(t, "CREATE INDEX t_idx ON db.t(j)")

			tableDesc := desctestutils.TestingGetTableDescriptor(execCfg.DB, execCfg.Codec, "db", "public", "t")
			tableID := tableDesc.GetID()
			idx, err := catalog.MustFindIndexByName(tableDesc, "t_idx")
			require.NoError(t, err)
			indexID := idx.GetID()

			// Depending on which version of the test we're running (GC-ing an index
			// or GC-ing a table), DROP the index or the table. We'll also create
			// another GC job for the table or index with an injected drop time (so
			// that it's considered "expired").
			if gcIndex {
				sqlRunner.Exec(t, "DROP INDEX db.t@t_idx")
			} else {
				sqlRunner.Exec(t, "DROP TABLE db.t")
			}

			// Write a PTS over the entire keyspace. We'll include multiple
			// timestamps, one of which is before the drop time.
			r1, err := spanconfig.MakeRecord(
				spanconfig.MakeTargetFromSystemTarget(spanconfig.MakeEntireKeyspaceTarget()),
				makeSystemSpanConfig(8, 23, 30),
			)
			require.NoError(t, err)

			// We do something similar for a PTS set by the host over the tenant's
			// keyspace.
			hostOnTenant, err := spanconfig.MakeTenantKeyspaceTarget(roachpb.SystemTenantID, tenantID)
			require.NoError(t, err)
			r2, err := spanconfig.MakeRecord(
				spanconfig.MakeTargetFromSystemTarget(hostOnTenant),
				makeSystemSpanConfig(6, 28, 35),
			)
			require.NoError(t, err)

			// Lastly, we'll also add a PTS set by the host over a different
			// secondary tenant's keyspace. This should have no bearing on our test.
			hostOnTenant20, err := spanconfig.MakeTenantKeyspaceTarget(
				roachpb.SystemTenantID, roachpb.MustMakeTenantID(20),
			)
			require.NoError(t, err)
			r3, err := spanconfig.MakeRecord(
				spanconfig.MakeTargetFromSystemTarget(hostOnTenant20),
				makeSystemSpanConfig(3, 4, 5),
			)

			require.NoError(t, err)
			err = tsKVAccessor.UpdateSpanConfigRecords(
				ctx, nil, []spanconfig.Record{r1, r2, r3}, hlc.MinTimestamp, hlc.MaxTimestamp,
			)
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
			err = ttKVAccessor.UpdateSpanConfigRecords(
				ctx, nil, []spanconfig.Record{r4}, hlc.MinTimestamp, hlc.MaxTimestamp,
			)
			require.NoError(t, err)

			// Create a GC-job for the table/index depending on which version of the
			// test we're running. We inject a very low drop time so that the object's
			// TTL is considered expired.
			var record jobs.Record
			if gcIndex {
				record = makeIndexGCJobRecord(10, indexID, tableID)
			} else {
				record = makeTableGCJobRecord(10, tableID)
			}
			jobID := registry.MakeJobID()
			mu.Lock()
			mu.jobID = jobID
			mu.Unlock()
			sj, err := jobs.TestingCreateAndStartJob(
				ctx, registry, execCfg.InternalDB, record, jobs.WithJobID(jobID),
			)
			require.NoError(t, err)

			ensureGCBlockedByPTS(t, registry, sj)

			// Update PTS state that applies to the entire keyspace -- we only
			// remove the timestamp before the drop time. We should expect GC to
			// succeed if we're testing for the system tenant at this point.
			r1, err = spanconfig.MakeRecord(
				spanconfig.MakeTargetFromSystemTarget(spanconfig.MakeEntireKeyspaceTarget()),
				makeSystemSpanConfig(23, 30),
			)
			require.NoError(t, err)
			err = tsKVAccessor.UpdateSpanConfigRecords(
				ctx, nil, []spanconfig.Record{r1}, hlc.MinTimestamp, hlc.MaxTimestamp,
			)
			require.NoError(t, err)

			resetTestingKnob()

			// GC should succeed on the system tenant. Exit the test early, we're done.
			if !forSecondaryTenant {
				ensureGCSucceeded(t, gcIndex, registry, sj)
				return
			}

			// Ensure GC is still blocked for secondary tenants.
			ensureGCBlockedByPTS(t, registry, sj)

			// Next, we'll remove the host set system span config on our secondary
			// tenant.
			err = tsKVAccessor.UpdateSpanConfigRecords(
				ctx, []spanconfig.Target{r2.GetTarget()}, nil, hlc.MinTimestamp, hlc.MaxTimestamp,
			)
			require.NoError(t, err)

			resetTestingKnob()
			ensureGCBlockedByPTS(t, registry, sj)

			// The only remaining PTS is from the system span config applied by
			// the secondary tenant itself.
			err = ttKVAccessor.UpdateSpanConfigRecords(
				ctx, []spanconfig.Target{r4.GetTarget()}, nil, hlc.MinTimestamp, hlc.MaxTimestamp,
			)
			require.NoError(t, err)

			// At this point, GC should succeed.
			resetTestingKnob()
			ensureGCSucceeded(t, gcIndex, registry, sj)
		})
	})
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
		// Disable the implicit default test tenant so that we can start our own.
		DisableDefaultTestTenant: true,
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
			log.FlushFileSinks()
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
