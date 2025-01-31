// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrademanager_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvtenantccl"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/settingswatcher"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/protoreflect"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgradebase"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrademanager"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// TestAlreadyRunningJobsAreHandledProperly is a relatively low-level test to
// ensure that the behavior to detect running jobs is sane. The test intercepts
// and blocks an upgrade that it first runs. It then duplicates the job to
// break the single-running job invariant. It then ensures that that invariant
// violation is detected. After that errant job is finished, it ensures that
// concurrent attempts to bump the cluster version detect the already running
// upgrade and wait.
func TestAlreadyRunningJobsAreHandledProperly(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// clusterversion.V23_1StopWritingPayloadAndProgressToSystemJobs was chosen
	// specifically so that all the migrations that introduce and backfill the new
	// `system.job_info` have run by this point. In the future this startCV should
	// be changed to V23_2Start and updated to the next Start key everytime the
	// compatability window moves forward.
	startCV := clusterversion.V23_1StopWritingPayloadAndProgressToSystemJobs
	endCV := startCV + 1

	ch := make(chan chan error)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 2, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
				Server: &server.TestingKnobs{
					BootstrapVersionKeyOverride:    clusterversion.BinaryMinSupportedVersionKey,
					BinaryVersionOverride:          clusterversion.ByKey(startCV),
					DisableAutomaticVersionUpgrade: make(chan struct{}),
				},
				DistSQL: &execinfra.TestingKnobs{
					// See the TODO below for why we need this.
					ProcessorNoTracingSpan: true,
				},
				UpgradeManager: &upgradebase.TestingKnobs{
					RegistryOverride: func(v roachpb.Version) (upgradebase.Upgrade, bool) {
						if v != clusterversion.ByKey(endCV) {
							return nil, false
						}
						return upgrade.NewTenantUpgrade("test", v, upgrade.NoPrecondition, func(
							ctx context.Context, version clusterversion.ClusterVersion, deps upgrade.TenantDeps,
						) error {
							canResume := make(chan error)
							select {
							case ch <- canResume:
							case <-ctx.Done():
								return ctx.Err()
							}
							select {
							case err := <-canResume:
								return err
							case <-ctx.Done():
								return ctx.Err()
							}
						}), true
					},
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	// At this point the test cluster has run all the migrations until startCV.

	upgrade1Err := make(chan error, 1)
	go func() {
		_, err := tc.ServerConn(0).ExecContext(ctx, `SET CLUSTER SETTING version = $1`, endCV.String())
		upgrade1Err <- err
	}()
	unblock := <-ch

	var firstID jobspb.JobID
	var firstPayload, firstProgress []byte
	require.NoError(t, tc.ServerConn(0).QueryRow(`
   SELECT id, payload, progress FROM crdb_internal.system_jobs WHERE (
                    crdb_internal.pb_to_json(
                        'cockroach.sql.jobs.jobspb.Payload',
                        payload
                    )->'migration'
                   ) IS NOT NULL AND status = 'running'
`).Scan(&firstID, &firstPayload, &firstProgress))

	// Inject a second job for the same upgrade and ensure that that causes
	// an error. This is pretty gnarly.
	var secondID jobspb.JobID
	require.NoError(t, tc.ServerConn(0).QueryRow(`
   INSERT
     INTO system.jobs
          (
            SELECT
                    unique_rowid(),
                    status,
                    created,
                    NULL,
                    NULL,
                    created_by_type,
                    created_by_id,
                    claim_session_id,
                    claim_instance_id,
                    0,
                    NULL,
                    job_type
              FROM crdb_internal.system_jobs
             WHERE id = $1
          )
RETURNING id;`, firstID).Scan(&secondID))
	// Insert the job payload and progress into the `system.job_info` table.
	err := tc.Server(0).InternalDB().(isql.DB).Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		infoStorage := jobs.InfoStorageForJob(txn, secondID)
		if err := infoStorage.WriteLegacyPayload(ctx, firstPayload); err != nil {
			return err
		}
		return infoStorage.WriteLegacyProgress(ctx, firstProgress)
	})
	require.NoError(t, err)

	// Make sure that the second job gets run in a timely manner.
	runErr := make(chan error)
	go func() {
		runErr <- tc.Server(0).JobRegistry().(*jobs.Registry).
			Run(ctx, []jobspb.JobID{secondID})
	}()
	fakeJobBlockChan := <-ch

	// Ensure that we see the assertion error.
	_, err = tc.Conns[0].ExecContext(ctx, `SET CLUSTER SETTING version = $1`, endCV.String())
	require.Regexp(t, "found multiple non-terminal jobs for version", err)

	// Let the fake, erroneous job finish with an error.
	fakeJobBlockChan <- jobs.MarkAsPermanentJobError(errors.New("boom"))
	require.Regexp(t, "boom", <-runErr)

	// See the TODO below for why we need this.
	_, err = tc.Conns[0].ExecContext(ctx, `SET CLUSTER SETTING sql.txn_stats.sample_rate = 0`)
	require.NoError(t, err)

	// Launch a second upgrade which later we'll ensure does not kick off
	// another job. We'll make sure this happens by polling the trace to see
	// the log line indicating what we want.
	tr := tc.Server(0).TracerI().(*tracing.Tracer)
	recCtx, sp := tr.StartSpanCtx(ctx, "test", tracing.WithRecording(tracingpb.RecordingVerbose))
	defer sp.Finish()
	upgrade2Err := make(chan error, 1)
	go func() {
		// Use an internal executor to get access to the trace as it happens.
		_, err := tc.Server(0).InternalExecutor().(isql.Executor).Exec(
			recCtx, "test", nil /* txn */, `SET CLUSTER SETTING version = $1`, endCV.String())
		upgrade2Err <- err
	}()

	testutils.SucceedsSoon(t, func() error {
		// TODO(yuzefovich): this check is quite unfortunate since it relies on the
		// assumption that all recordings from the child spans are imported into the
		// tracer. However, this is not the case for the DistSQL processors whose
		// recordings require explicit importing. This only happens when the
		// execution flow is drained which cannot happen until we close the
		// 'unblock' channel, and this we cannot do until we see the expected
		// message in the trace.
		//
		// At the moment it works in a very fragile manner by making sure that
		// no processors actually create their own spans.
		//
		// Instead, a different way to observe the status of the upgrade manager
		// should be introduced and should be used here.
		rec := sp.GetConfiguredRecording()
		if tracing.FindMsgInRecording(rec, "found existing migration job") > 0 {
			return nil
		}
		return errors.Errorf("waiting for job to be discovered: %v", rec)
	})
	close(unblock)
	require.NoError(t, <-upgrade1Err)
	require.NoError(t, <-upgrade2Err)
}

// TestPostJobInfoTableQueryDuplicateJobInfo tests that the
// PostJobInfoTableQuery returns 1 row for a migration even when the
// job_info table has 2 payloads for the relevant migration.
//
// This condition shouldn't really be possible for the migrations that
// this query will be used against since it is only possible for jobs
// that exist during the job_info backfill. By definition, any job we
// are looking for with this query should be ones started after the
// query.
//
// But, in case we are wrong about that reasoning, we handle it and
// test it here.
func TestPostJobInfoTableQueryDuplicateJobInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	targetCV := clusterversion.V23_1StopWritingPayloadAndProgressToSystemJobs + 1
	targetCVJSON, err := protoreflect.MessageToJSON(&clusterversion.ClusterVersion{Version: clusterversion.ByKey(targetCV)},
		protoreflect.FmtFlags{EmitDefaults: false})
	require.NoError(t, err)

	settingsForUpgrade := func() *cluster.Settings {
		settings := cluster.MakeTestingClusterSettingsWithVersions(
			clusterversion.TestingBinaryVersion,
			clusterversion.TestingBinaryMinSupportedVersion,
			false, // initializeVersion
		)
		require.NoError(t, clusterversion.Initialize(ctx,
			clusterversion.ByKey(clusterversion.BinaryMinSupportedVersionKey), &settings.SV))
		return settings
	}

	upgradeStarted := make(chan chan struct{})
	registryOverrideHook := func(v roachpb.Version) (upgradebase.Upgrade, bool) {
		if v != clusterversion.ByKey(targetCV) {
			return nil, false
		}
		return upgrade.NewTenantUpgrade("test", v, upgrade.NoPrecondition, func(
			ctx context.Context, version clusterversion.ClusterVersion, deps upgrade.TenantDeps,
		) error {
			canResume := make(chan struct{})
			upgradeStarted <- canResume
			select {
			case <-canResume:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}), true
	}

	ts, systemSQLDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		DisableDefaultTestTenant: true,
		Settings:                 settingsForUpgrade(),
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			Server: &server.TestingKnobs{
				BinaryVersionOverride:          clusterversion.TestingBinaryMinSupportedVersion,
				DisableAutomaticVersionUpgrade: make(chan struct{}),
			},
			UpgradeManager: &upgradebase.TestingKnobs{
				RegistryOverride: registryOverrideHook,
			},
		},
	})
	defer ts.Stopper().Stop(ctx)

	runTestForDB := func(t *testing.T, sqlDB *gosql.DB) {
		upgradeErr := make(chan error, 1)
		go func() {
			t.Logf("setting cluster version to %s", targetCV.String())
			_, err := sqlDB.ExecContext(ctx, `SET CLUSTER SETTING version = $1`, targetCV.String())
			upgradeErr <- err
		}()
		canResume := <-upgradeStarted

		var jobID jobspb.JobID
		require.NoError(t,
			sqlDB.QueryRow(`
SELECT id
FROM system.jobs WHERE job_type = 'MIGRATION' AND status = 'running'`).Scan(&jobID))

		verifyJobInfoQuery := func() {
			rows, err := sqlDB.Query(upgrademanager.PostJobInfoTableQuery, targetCVJSON.String())
			require.NoError(t, err)
			defer rows.Close()

			require.True(t, rows.Next(), "one row required")
			require.False(t, rows.Next(), "more than one row returned")
			require.NoError(t, rows.Err())
		}
		verifyJobInfoQuery()
		t.Logf("inserting row")
		res, err := sqlDB.Exec(`
INSERT INTO system.job_info (
SELECT job_id, info_key, now(), value
FROM system.job_info WHERE job_id = $1 AND info_key = 'legacy_payload')`, jobID)
		require.NoError(t, err)
		rowsInserted, err := res.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int64(1), rowsInserted)

		verifyJobInfoQuery()

		close(canResume)
		require.NoError(t, <-upgradeErr)
	}

	t.Run("system", func(t *testing.T) {
		runTestForDB(t, systemSQLDB)
	})
	t.Run("tenant", func(t *testing.T) {
		_, tenantSQLDB := serverutils.StartTenant(t, ts, base.TestTenantArgs{
			TenantID: roachpb.MustMakeTenantID(10),
			TestingKnobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					BinaryVersionOverride:          clusterversion.TestingBinaryMinSupportedVersion,
					DisableAutomaticVersionUpgrade: make(chan struct{}),
				},
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
				UpgradeManager: &upgradebase.TestingKnobs{
					RegistryOverride: registryOverrideHook,
				},
			},
			Settings: settingsForUpgrade(),
		})
		require.NoError(t, err)
		runTestForDB(t, tenantSQLDB)
	})
}

func TestMigrateUpdatesReplicaVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// We're going to be migrating from startCV to endCV.
	startCVKey := clusterversion.V22_2
	startCV := clusterversion.ByKey(startCVKey)
	endCVKey := startCVKey + 1
	endCV := clusterversion.ByKey(endCVKey)

	var desc roachpb.RangeDescriptor
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 2, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					BootstrapVersionKeyOverride:    clusterversion.BinaryMinSupportedVersionKey,
					BinaryVersionOverride:          startCV,
					DisableAutomaticVersionUpgrade: make(chan struct{}),
				},
				UpgradeManager: &upgradebase.TestingKnobs{
					ListBetweenOverride: func(from, to roachpb.Version) []roachpb.Version {
						return []roachpb.Version{from, to}
					},
					RegistryOverride: func(cv roachpb.Version) (upgradebase.Upgrade, bool) {
						if cv != endCV {
							return nil, false
						}
						return upgrade.NewSystemUpgrade("test", cv, func(
							ctx context.Context, version clusterversion.ClusterVersion, d upgrade.SystemDeps,
						) error {
							return d.DB.KV().Migrate(ctx, desc.StartKey, desc.EndKey, cv)
						}), true
					},
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)
	// RegisterKVMigration the below raft upgrade.
	unregisterKVMigration := batcheval.TestingRegisterMigrationInterceptor(endCV, func() {})
	defer unregisterKVMigration()

	// We'll take a specific range, still running at startCV, generate an
	// outgoing snapshot and then suspend it temporarily. We'll then bump the
	// cluster version on all the stores, as part of the upgrade process, and
	// then resume the snapshot process. Seeing as how the snapshot was
	// generated pre-version bump, off of a version of the range that hadn't
	// observed the upgrade corresponding to the latest cluster version, we
	// expect the store to reject it.

	key := tc.ScratchRange(t)
	require.NoError(t, tc.WaitForSplitAndInitialization(key))
	var err error
	desc, err = tc.LookupRange(key)
	require.NoError(t, err)
	rangeID := desc.RangeID

	// Enqueue the replica in the raftsnapshot queue. We use SucceedsSoon
	// because it may take a bit for raft to figure out that we need to be
	// generating a snapshot.
	store := tc.GetFirstStoreFromServer(t, 0)
	repl, err := store.GetReplica(rangeID)
	require.NoError(t, err)

	if got := repl.Version(); got != startCV {
		t.Fatalf("got replica version %s, expected %s", got, startCV)
	}

	// Wait until all nodes have are considered live.
	nl := tc.Server(0).NodeLiveness().(*liveness.NodeLiveness)
	testutils.SucceedsSoon(t, func() error {
		for _, s := range tc.Servers {
			id := s.NodeID()
			live, err := nl.IsLive(id)
			if err != nil {
				return err
			}
			if !live {
				return errors.Newf("n%s not live yet", id)
			}
		}
		return nil
	})

	// Kick off the upgrade process.
	_, err = tc.Conns[0].ExecContext(ctx, `SET CLUSTER SETTING version = $1`, endCV.String())
	require.NoError(t, err)

	if got := repl.Version(); got != endCV {
		t.Fatalf("got replica version %s, expected %s", got, endCV)
	}
}

// TestConcurrentMigrationAttempts ensures that concurrent attempts to run
// upgrades over a number of versions exhibits reasonable behavior. Namely,
// that each upgrade gets run one time and that upgrades do not get run
// again.
func TestConcurrentMigrationAttempts(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// We're going to be migrating from the BinaryMinSupportedVersion to imaginary future versions.
	current := clusterversion.TestingBinaryMinSupportedVersion
	versions := []roachpb.Version{current}
	for i := int32(1); i <= 4; i++ {
		v := current
		v.Internal += i * 2
		versions = append(versions, v)
	}

	// RegisterKVMigration the upgrades to update the map with run counts.
	// There should definitely not be any concurrency of execution, so the race
	// detector should not fire.
	migrationRunCounts := make(map[clusterversion.ClusterVersion]int)

	ctx := context.Background()
	var active int32 // used to detect races
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Settings: cluster.MakeTestingClusterSettingsWithVersions(
				versions[len(versions)-1],
				versions[0],
				false,
			),
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					BinaryVersionOverride:          versions[0],
					DisableAutomaticVersionUpgrade: make(chan struct{}),
				},
				UpgradeManager: &upgradebase.TestingKnobs{
					ListBetweenOverride: func(from, to roachpb.Version) []roachpb.Version {
						return versions
					},
					RegistryOverride: func(cv roachpb.Version) (upgradebase.Upgrade, bool) {
						return upgrade.NewSystemUpgrade("test", cv, func(
							ctx context.Context, version clusterversion.ClusterVersion, d upgrade.SystemDeps,
						) error {
							if atomic.AddInt32(&active, 1) != 1 {
								t.Error("unexpected concurrency")
							}
							time.Sleep(time.Millisecond)
							atomic.AddInt32(&active, -1)
							migrationRunCounts[version]++
							return nil
						}), true
					},
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	// Run N instances of the upgrade concurrently on different connections.
	// They should all eventually succeed; some may internally experience a
	// serializable restart but cockroach will handle that transparently.
	// Afterwards we'll ensure that no upgrade was run more than once.
	N := 25
	if util.RaceEnabled {
		N = 5
	}
	db := tc.ServerConn(0)
	db.SetMaxOpenConns(N)
	conns := make([]*gosql.Conn, N)
	for i := range conns {
		var err error
		conns[i], err = db.Conn(ctx)
		require.NoError(t, err)
	}
	var g errgroup.Group
	for i := 0; i < N; i++ {
		conn := conns[i]
		g.Go(func() error {
			_, err := conn.ExecContext(ctx, `SET CLUSTER SETTING version = $1`,
				versions[len(versions)-1].String())
			return err
		})
	}
	require.Nil(t, g.Wait())
	for k, c := range migrationRunCounts {
		require.Equalf(t, 1, c, "version: %v", k)
	}
	require.Len(t, migrationRunCounts, len(versions))
}

// TestPauseMigration ensures that upgrades can indeed be paused and that
// concurrent attempts to perform an upgrade will block on the existing,
// paused job.
func TestPauseMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// clusterversion.V23_1StopWritingPayloadAndProgressToSystemJobs was chosen
	// specifically so that all the migrations that introduce and backfill the new
	// `system.job_info` have run by this point. In the future this startCV should
	// be changed to V23_2Start and updated to the next Start key everytime the
	// compatability window moves forward.
	startCV := clusterversion.V23_1StopWritingPayloadAndProgressToSystemJobs
	endCV := startCV + 1

	type migrationEvent struct {
		unblock  chan<- error
		canceled <-chan struct{}
	}
	ch := make(chan migrationEvent)
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 2, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
				Server: &server.TestingKnobs{
					BinaryVersionOverride:          clusterversion.ByKey(startCV),
					BootstrapVersionKeyOverride:    clusterversion.BinaryMinSupportedVersionKey,
					DisableAutomaticVersionUpgrade: make(chan struct{}),
				},
				UpgradeManager: &upgradebase.TestingKnobs{
					RegistryOverride: func(cv roachpb.Version) (upgradebase.Upgrade, bool) {
						if cv != clusterversion.ByKey(endCV) {
							return nil, false
						}
						return upgrade.NewTenantUpgrade("test", cv, upgrade.NoPrecondition, func(
							ctx context.Context, version clusterversion.ClusterVersion, deps upgrade.TenantDeps,
						) error {
							canResume := make(chan error)
							ch <- migrationEvent{
								unblock:  canResume,
								canceled: ctx.Done(),
							}
							select {
							case <-ctx.Done():
								return ctx.Err()
							case err := <-canResume:
								return err
							}
						}), true
					},
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	sqlDB := tc.ServerConn(0)
	upgrade1Err := make(chan error, 1)
	go func() {
		_, err := sqlDB.ExecContext(ctx, `SET CLUSTER SETTING version = $1`, endCV.String())
		upgrade1Err <- err
	}()
	ev := <-ch

	tdb := sqlutils.MakeSQLRunner(sqlDB)
	var id int64
	tdb.QueryRow(t, `
SELECT id
  FROM crdb_internal.system_jobs
 WHERE (
        crdb_internal.pb_to_json(
            'cockroach.sql.jobs.jobspb.Payload',
            payload
        )->'migration'
       ) IS NOT NULL AND status = 'running';`).
		Scan(&id)
	tdb.Exec(t, "PAUSE JOB $1", id)

	<-ev.canceled
	// Kick off another upgrade
	upgrade2Err := make(chan error, 1)
	go func() {
		_, err := sqlDB.ExecContext(ctx, `SET CLUSTER SETTING version = $1`, endCV.String())
		upgrade2Err <- err
	}()

	// The upgrade should not be done.
	select {
	case err := <-upgrade1Err:
		require.Error(t, err)
		require.Contains(t, err.Error(), "paused before it completed")
	case err := <-upgrade2Err:
		require.Error(t, err)
		require.Contains(t, err.Error(), "paused before it completed")
	case <-ch:
		t.Fatalf("did not expect the job to run again")
	case <-time.After(10 * time.Millisecond):
	}
	// Wait for the job to actually be paused as opposed to waiting in
	// pause-requested. There's a separate issue to make PAUSE wait for
	// the job to be paused, but that's a behavior change is better than nothing.
	tdb.CheckQueryResultsRetry(t, fmt.Sprintf(
		`SELECT status FROM crdb_internal.jobs WHERE job_id = %d`, id,
	), [][]string{{"paused"}})
	tdb.Exec(t, "RESUME JOB $1", id)
	ev = <-ch
	close(ev.unblock)
	_, err := sqlDB.ExecContext(ctx, `SET CLUSTER SETTING version = $1`, endCV.String())
	require.NoError(t, err)
}

// Test that the precondition prevents upgrades from being run.
func TestPrecondition(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderStress(t, "flaky")

	// Start by running v0. We want the precondition of v1 to prevent
	// us from reaching v1 (or v2). We want the precondition to not be
	// run when migrating from v1 to v2.
	next := func(version roachpb.Version) roachpb.Version {
		version.Internal += 2
		return version
	}
	// In cases where the tenant migration fails at the migration step, the
	// active version will remain at the fence version, which was set before
	// the migration was attempted.
	fence := func(version roachpb.Version) roachpb.Version {
		version.Internal += 1
		return version
	}
	v0 := clusterversion.ByKey(clusterversion.TODODelete_V22_1)
	v0_fence := fence(v0)
	v1 := next(v0)
	v1_fence := fence(v1)
	v2 := next(v1)
	versions := []roachpb.Version{v0, v1, v2}
	var migrationRun, preconditionRun int64
	var preconditionErr, migrationErr atomic.Value
	preconditionErr.Store(true)
	migrationErr.Store(true)
	cf := func(run *int64, err *atomic.Value) upgrade.TenantUpgradeFunc {
		return func(
			context.Context, clusterversion.ClusterVersion, upgrade.TenantDeps,
		) error {
			atomic.AddInt64(run, 1)
			if err.Load().(bool) {
				return jobs.MarkAsPermanentJobError(errors.New("boom"))
			}
			return nil
		}
	}
	knobs := base.TestingKnobs{
		Server: &server.TestingKnobs{
			DisableAutomaticVersionUpgrade: make(chan struct{}),
			BinaryVersionOverride:          v0,
		},
		// Inject an upgrade which would run to upgrade the cluster.
		// We'll validate that we never create a job for this upgrade.
		UpgradeManager: &upgradebase.TestingKnobs{
			ListBetweenOverride: func(from, to roachpb.Version) []roachpb.Version {
				start := sort.Search(len(versions), func(i int) bool { return from.Less(versions[i]) })
				end := sort.Search(len(versions), func(i int) bool { return to.Less(versions[i]) })
				return versions[start:end]
			},
			RegistryOverride: func(cv roachpb.Version) (upgradebase.Upgrade, bool) {
				switch cv {
				case v1:
					return upgrade.NewTenantUpgrade("v1", cv,
						upgrade.PreconditionFunc(func(
							ctx context.Context, cv clusterversion.ClusterVersion, td upgrade.TenantDeps,
						) error {
							return cf(&preconditionRun, &preconditionErr)(ctx, cv, td)
						}),
						cf(&migrationRun, &migrationErr),
					), true
				case v2:
					return upgrade.NewTenantUpgrade("v2", cv,
						upgrade.NoPrecondition,
						cf(&migrationRun, &migrationErr),
					), true
				default:
					return nil, false
				}
			},
		},
	}
	ctx := context.Background()
	args := func() base.TestServerArgs {
		return base.TestServerArgs{
			Knobs: knobs,
			Settings: cluster.MakeTestingClusterSettingsWithVersions(
				v2,    // binaryVersion
				v0,    // binaryMinSupportedVersion
				false, // initializeVersion
			),
		}
	}
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ServerArgsPerNode: map[int]base.TestServerArgs{
			0: args(),
			1: args(),
			2: args(),
		},
	})
	checkActiveVersion := func(t *testing.T, exp roachpb.Version) {
		for i := 0; i < tc.NumServers(); i++ {
			got := tc.Server(i).ClusterSettings().Version.ActiveVersion(ctx).Version
			require.Equalf(t, exp, got, "server %d", i)
		}
	}

	defer tc.Stopper().Stop(ctx)
	sqlDB := tc.ServerConn(0)
	{
		_, err := sqlDB.Exec("SET CLUSTER SETTING version = $1", v2.String())
		require.Regexp(t, "boom", err)
		require.Equal(t, int64(1), atomic.LoadInt64(&preconditionRun))
		require.Equal(t, int64(0), atomic.LoadInt64(&migrationRun))
		checkActiveVersion(t, v0)
	}
	preconditionErr.Store(false)
	{
		_, err := sqlDB.Exec("SET CLUSTER SETTING version = $1", v2.String())
		require.Regexp(t, "boom", err)
		require.Equal(t, int64(2), atomic.LoadInt64(&preconditionRun))
		require.Equal(t, int64(1), atomic.LoadInt64(&migrationRun))
		checkActiveVersion(t, v0_fence)
	}
	migrationErr.Store(false)
	{
		_, err := sqlDB.Exec("SET CLUSTER SETTING version = $1", v1.String())
		require.NoError(t, err)
		require.Equal(t, int64(3), atomic.LoadInt64(&preconditionRun))
		require.Equal(t, int64(2), atomic.LoadInt64(&migrationRun))
		checkActiveVersion(t, v1)
	}
	migrationErr.Store(true)
	{
		_, err := sqlDB.Exec("SET CLUSTER SETTING version = $1", v2.String())
		require.Regexp(t, "boom", err)
		// Note that there is no precondition for this second upgrade. This
		// case will fail at the migration step.
		require.Equal(t, int64(3), atomic.LoadInt64(&preconditionRun))
		require.Equal(t, int64(3), atomic.LoadInt64(&migrationRun))
		checkActiveVersion(t, v1_fence)
	}
	migrationErr.Store(false)
	{
		_, err := sqlDB.Exec("SET CLUSTER SETTING version = $1", v2.String())
		require.NoError(t, err)
		require.Equal(t, int64(3), atomic.LoadInt64(&preconditionRun))
		require.Equal(t, int64(4), atomic.LoadInt64(&migrationRun))
		checkActiveVersion(t, v2)
	}
}

func TestMigrationFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Configure the range of versions used by the test
	startVersionKey := clusterversion.BinaryMinSupportedVersionKey
	startVersion := clusterversion.ByKey(startVersionKey)
	endVersionKey := clusterversion.BinaryVersionKey
	endVersion := clusterversion.ByKey(endVersionKey)

	// Pick a random version in to fail at
	versions := clusterversion.ListBetween(startVersion, endVersion)
	failVersion := versions[rand.Intn(len(versions))]
	fenceVersion := upgrade.FenceVersionFor(ctx, clusterversion.ClusterVersion{Version: failVersion}).Version
	t.Logf("test will fail at version: %s", failVersion.String())

	// Create a storage cluster for the tenant
	testCluster := serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DisableDefaultTestTenant: true,
			Knobs: base.TestingKnobs{
				SQLEvalContext: &eval.TestingKnobs{
					TenantLogicalVersionKeyOverride: startVersionKey,
				},
			},
		},
	})
	defer testCluster.Stopper().Stop(ctx)

	// Set the version override so that the tenant is able to upgrade. If this is
	// not set, the tenant treats the storage cluster as if it had the oldest
	// supported binary version.
	s := testCluster.Server(0)
	goDB := serverutils.OpenDBConn(t, s.ServingSQLAddr(), "system", false, s.Stopper())
	_, err := goDB.Exec(`ALTER TENANT ALL SET CLUSTER SETTING version = $1`, endVersion.String())
	require.NoError(t, err)

	// setting failUpgrade to false disables the upgrade error logic.
	var failUpgrade atomic.Bool
	failUpgrade.Store(true)

	// Create a tenant cluster at the oldest supported binary version. Configure
	// upgrade manager to fail the upgrade at a random version.
	tenantSettings := cluster.MakeTestingClusterSettingsWithVersions(
		endVersion,
		startVersion,
		false,
	)
	require.NoError(t, clusterversion.Initialize(ctx, startVersion, &tenantSettings.SV))
	tenant, db := serverutils.StartTenant(t, testCluster.Server(0), base.TestTenantArgs{
		TenantID: roachpb.MustMakeTenantID(10),
		Settings: tenantSettings,
		TestingKnobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				DisableAutomaticVersionUpgrade: make(chan struct{}),
				BootstrapVersionKeyOverride:    startVersionKey,
				BinaryVersionOverride:          startVersion,
			},
			UpgradeManager: &upgradebase.TestingKnobs{
				DontUseJobs: true,
				RegistryOverride: func(cv roachpb.Version) (upgradebase.Upgrade, bool) {
					if failUpgrade.Load() && cv == failVersion {
						errorUpgrade := func(ctx context.Context, version clusterversion.ClusterVersion, deps upgrade.TenantDeps) error {
							return errors.New("the upgrade failed with some error!")
						}
						return upgrade.NewTenantUpgrade("test", cv, nil, errorUpgrade), true
					}
					return upgrades.GetUpgrade(cv)
				},
			},
		},
	})

	// Wait for the storage cluster version to propogate
	watcher := tenant.SettingsWatcher().(*settingswatcher.SettingsWatcher)
	testutils.SucceedsSoon(t, func() error {
		storageVersion := watcher.GetStorageClusterActiveVersion()
		if !storageVersion.IsActive(endVersionKey) {
			return errors.Newf("expected storage version of at least %s found %s", endVersion.PrettyPrint(), storageVersion.PrettyPrint())
		}
		return nil
	})

	checkActiveVersion := func(t *testing.T, exp roachpb.Version) {
		got := tenant.ClusterSettings().Version.ActiveVersion(ctx).Version
		require.Equal(t, exp, got)
	}
	checkSettingVersion := func(t *testing.T, exp roachpb.Version) {
		var settingVersion clusterversion.ClusterVersion
		require.NoError(t, tenant.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			var err error
			settingVersion, err = watcher.GetClusterVersionFromStorage(ctx, txn)
			return err
		}))
		require.Equal(t, exp, settingVersion.Version)
	}

	checkActiveVersion(t, startVersion)
	checkSettingVersion(t, startVersion)

	// Try to finalize
	_, err = db.Exec(`SET CLUSTER SETTING version = $1`, endVersion.String())
	require.Error(t, err)
	checkActiveVersion(t, fenceVersion)
	// Note: we don't check the setting version here because the fence setting
	// is only materialized on the first attempt.

	// Try to finalize again.
	_, err = db.Exec(`SET CLUSTER SETTING version = $1`, endVersion.String())
	require.Error(t, err)
	checkActiveVersion(t, fenceVersion)
	checkSettingVersion(t, fenceVersion)

	// Try to finalize a final time, allowing the upgrade to succeed.
	failUpgrade.Store(false)
	_, err = db.Exec(`SET CLUSTER SETTING version = $1`, endVersion.String())
	require.NoError(t, err)
	checkActiveVersion(t, endVersion)
	checkSettingVersion(t, endVersion)
}
