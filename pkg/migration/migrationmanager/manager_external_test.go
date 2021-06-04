// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migrationmanager_test

import (
	"context"
	gosql "database/sql"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/migration/migrationmanager"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// TestAlreadyRunningJobsAreHandledProperly is a relatively low-level test to
// ensure that the behavior to detect running jobs is sane. The test intercepts
// and blocks a migration that it first runs. It then duplicates the job to
// break the single-running job invariant. It then ensures that that invariant
// violation is detected. After that errant job is finished, it ensures that
// concurrent attempts to bump the cluster version detect the already running
// migration and wait.
func TestAlreadyRunningJobsAreHandledProperly(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// We're going to be migrating from startCV to endCV.
	startCV := clusterversion.ClusterVersion{Version: roachpb.Version{Major: 41}}
	endCV := clusterversion.ClusterVersion{Version: roachpb.Version{Major: 42}}

	ch := make(chan chan error)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 2, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Settings: cluster.MakeTestingClusterSettingsWithVersions(endCV.Version, startCV.Version, false),
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					BinaryVersionOverride:          startCV.Version,
					DisableAutomaticVersionUpgrade: 1,
				},
				MigrationManager: &migrationmanager.TestingKnobs{
					ListBetweenOverride: func(from, to clusterversion.ClusterVersion) []clusterversion.ClusterVersion {
						return []clusterversion.ClusterVersion{to}
					},
					RegistryOverride: func(cv clusterversion.ClusterVersion) (migration.Migration, bool) {
						if cv != endCV {
							return nil, false
						}
						return migration.NewTenantMigration("test", cv, func(
							ctx context.Context, version clusterversion.ClusterVersion, deps migration.TenantDeps,
						) error {
							canResume := make(chan error)
							ch <- canResume
							return <-canResume
						}), true
					},
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	upgrade1Err := make(chan error, 1)
	go func() {
		_, err := tc.ServerConn(0).ExecContext(ctx, `SET CLUSTER SETTING version = $1`, endCV.String())
		upgrade1Err <- err
	}()
	unblock := <-ch

	// Inject a second job for the same migration and ensure that that causes
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
                    payload,
                    progress,
                    created_by_type,
                    created_by_id,
                    claim_session_id,
                    claim_instance_id
              FROM system.jobs
             WHERE (
                    crdb_internal.pb_to_json(
                        'cockroach.sql.jobs.jobspb.Payload',
                        payload
                    )->'migration'
                   ) IS NOT NULL
          )
RETURNING id;`).Scan(&secondID))

	// Make sure that the second job gets run in a timely manner.
	runErr := make(chan error)
	go func() {
		runErr <- tc.Server(0).JobRegistry().(*jobs.Registry).
			Run(
				ctx,
				tc.Server(0).InternalExecutor().(sqlutil.InternalExecutor),
				[]jobspb.JobID{secondID},
			)
	}()
	fakeJobBlockChan := <-ch

	// Ensure that we see the assertion error.
	_, err := tc.Conns[0].ExecContext(ctx, `SET CLUSTER SETTING version = $1`, endCV.String())
	require.Regexp(t, "found multiple non-terminal jobs for version", err)

	// Let the fake, erroneous job finish with an error.
	fakeJobBlockChan <- errors.New("boom")
	require.Regexp(t, "boom", <-runErr)

	// Launch a second migration which later we'll ensure does not kick off
	// another job. We'll make sure this happens by polling the trace to see
	// the log line indicating what we want.
	tr := tc.Server(0).Tracer().(*tracing.Tracer)
	recCtx, getRecording, cancel := tracing.ContextWithRecordingSpan(ctx, tr, "test")
	defer cancel()
	upgrade2Err := make(chan error, 1)
	go func() {
		// Use an internal executor to get access to the trace as it happens.
		_, err := tc.Server(0).InternalExecutor().(sqlutil.InternalExecutor).Exec(
			recCtx, "test", nil /* txn */, `SET CLUSTER SETTING version = $1`, endCV.String())
		upgrade2Err <- err
	}()

	testutils.SucceedsSoon(t, func() error {
		// TODO(yuzefovich): this check is quite unfortunate since it relies on
		// the assumption that all recordings from the child spans are imported
		// into the tracer. However, this is not the case for the DistSQL
		// processors where child spans are created with
		// WithParentAndManualCollection option which requires explicitly
		// importing the recordings from the children. This only happens when
		// the execution flow is drained which cannot happen until we close
		// the 'unblock' channel, and this we cannot do until we see the
		// expected message in the trace.
		//
		// At the moment it works in a very fragile manner (by making sure that
		// no processors actually create their own spans). Instead, a different
		// way to observe the status of the migration manager should be
		// introduced and should be used here.
		if tracing.FindMsgInRecording(getRecording(), "found existing migration job") > 0 {
			return nil
		}
		return errors.Errorf("waiting for job to be discovered: %v", getRecording())
	})
	close(unblock)
	require.NoError(t, <-upgrade1Err)
	require.NoError(t, <-upgrade2Err)
}

func TestMigrateUpdatesReplicaVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// We're going to be migrating from startCV to endCV.
	startCV := clusterversion.ClusterVersion{Version: roachpb.Version{Major: 41}}
	endCV := clusterversion.ClusterVersion{Version: roachpb.Version{Major: 42}}

	var desc roachpb.RangeDescriptor
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 2, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Settings: cluster.MakeTestingClusterSettingsWithVersions(endCV.Version, startCV.Version, false),
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					BinaryVersionOverride:          startCV.Version,
					DisableAutomaticVersionUpgrade: 1,
				},
				MigrationManager: &migrationmanager.TestingKnobs{
					ListBetweenOverride: func(from, to clusterversion.ClusterVersion) []clusterversion.ClusterVersion {
						return []clusterversion.ClusterVersion{from, to}
					},
					RegistryOverride: func(cv clusterversion.ClusterVersion) (migration.Migration, bool) {
						if cv != endCV {
							return nil, false
						}
						return migration.NewSystemMigration("test", cv, func(
							ctx context.Context, version clusterversion.ClusterVersion, c migration.Cluster,
						) error {
							return c.DB().Migrate(ctx, desc.StartKey, desc.EndKey, cv.Version)
						}), true
					},
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)
	// RegisterKVMigration the below raft migration.
	unregisterKVMigration := batcheval.TestingRegisterMigrationInterceptor(endCV.Version, func() {})
	defer unregisterKVMigration()

	// We'll take a specific range, still running at startCV, generate an
	// outgoing snapshot and then suspend it temporarily. We'll then bump the
	// cluster version on all the stores, as part of the migration process, and
	// then resume the snapshot process. Seeing as how the snapshot was
	// generated pre-version bump, off of a version of the range that hadn't
	// observed the migration corresponding to the latest cluster version, we
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

	if got := repl.Version(); got != startCV.Version {
		t.Fatalf("got replica version %s, expected %s", got, startCV.Version)
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

	// Kick off the migration process.
	_, err = tc.Conns[0].ExecContext(ctx, `SET CLUSTER SETTING version = $1`, endCV.String())
	require.NoError(t, err)

	if got := repl.Version(); got != endCV.Version {
		t.Fatalf("got replica version %s, expected %s", got, endCV.Version)
	}
}

// TestConcurrentMigrationAttempts ensures that concurrent attempts to run
// migrations over a number of versions exhibits reasonable behavior. Namely,
// that each migration gets run one time and that migrations do not get run
// again.
func TestConcurrentMigrationAttempts(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// We're going to be migrating from startKey to endKey. We end up needing
	// to use real versions because the ListBetween uses the keys compiled into
	// the clusterversion package.
	const (
		startMajor = 42
		endMajor   = 48
	)
	migrationRunCounts := make(map[clusterversion.ClusterVersion]int)

	// RegisterKVMigration the migrations to update the map with run counts.
	// There should definitely not be any concurrency of execution, so the race
	// detector should not fire.
	var versions []clusterversion.ClusterVersion

	for major := int32(startMajor); major <= endMajor; major++ {
		versions = append(versions, clusterversion.ClusterVersion{
			Version: roachpb.Version{
				Major: major,
			},
		})
	}
	ctx := context.Background()
	var active int32 // used to detect races
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Settings: cluster.MakeTestingClusterSettingsWithVersions(
				versions[len(versions)-1].Version,
				versions[0].Version,
				false,
			),
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					BinaryVersionOverride:          versions[0].Version,
					DisableAutomaticVersionUpgrade: 1,
				},
				MigrationManager: &migrationmanager.TestingKnobs{
					ListBetweenOverride: func(from, to clusterversion.ClusterVersion) []clusterversion.ClusterVersion {
						return versions
					},
					RegistryOverride: func(cv clusterversion.ClusterVersion) (migration.Migration, bool) {
						return migration.NewSystemMigration("test", cv, func(
							ctx context.Context, version clusterversion.ClusterVersion, c migration.Cluster,
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

	// Run N instances of the migration concurrently on different connections.
	// They should all eventually succeed; some may internally experience a
	// serializable restart but cockroach will handle that transparently.
	// Afterwards we'll ensure that no migration was run more than once.
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

// TestPauseMigration ensures that migrations can indeed be paused and that
// concurrent attempts to perform a migration will block on the existing,
// paused job.
func TestPauseMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// We're going to be migrating from startCV to endCV.
	startCV := clusterversion.ClusterVersion{Version: roachpb.Version{Major: 41}}
	endCV := clusterversion.ClusterVersion{Version: roachpb.Version{Major: 42}}

	type migrationEvent struct {
		unblock  chan<- error
		canceled <-chan struct{}
	}
	ch := make(chan migrationEvent)
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 2, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Settings: cluster.MakeTestingClusterSettingsWithVersions(endCV.Version, startCV.Version, false),
			Knobs: base.TestingKnobs{
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
				Server: &server.TestingKnobs{
					BinaryVersionOverride:          startCV.Version,
					DisableAutomaticVersionUpgrade: 1,
				},
				MigrationManager: &migrationmanager.TestingKnobs{
					ListBetweenOverride: func(from, to clusterversion.ClusterVersion) []clusterversion.ClusterVersion {
						return []clusterversion.ClusterVersion{to}
					},
					RegistryOverride: func(cv clusterversion.ClusterVersion) (migration.Migration, bool) {
						if cv != endCV {
							return nil, false
						}
						return migration.NewTenantMigration("test", cv, func(
							ctx context.Context, version clusterversion.ClusterVersion, deps migration.TenantDeps,
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
  FROM system.jobs
 WHERE (
        crdb_internal.pb_to_json(
            'cockroach.sql.jobs.jobspb.Payload',
            payload
        )->'migration'
       ) IS NOT NULL;`).
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
		t.Fatalf("did not expect the first upgrade to finish: %v", err)
	case err := <-upgrade2Err:
		t.Fatalf("did not expect the second upgrade to finish: %v", err)
	case <-ch:
		t.Fatalf("did not expect the job to run again")
	case <-time.After(10 * time.Millisecond):
	}
	tdb.Exec(t, "RESUME JOB $1", id)
	ev = <-ch
	close(ev.unblock)
	require.NoError(t, <-upgrade1Err)
	require.NoError(t, <-upgrade2Err)
}
