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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
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
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	ch := make(chan chan struct{})
	defer migration.TestingRegisterMigrationInterceptor(endCV, func(
		ctx context.Context, cv clusterversion.ClusterVersion, h migration.Cluster,
	) error {
		canResume := make(chan struct{})
		ch <- canResume
		<-canResume
		return nil
	})()
	upgrade1Err := make(chan error, 1)
	go func() {
		_, err := tc.ServerConn(0).ExecContext(ctx, `SET CLUSTER SETTING version = $1`, endCV.String())
		upgrade1Err <- err
	}()
	unblock := <-ch

	// Inject a second job for the same migration and ensure that that causes
	// an error. This is pretty gnarly.
	var secondID int64
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
                    )->'longRunningMigration'
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
				[]int64{secondID},
			)
	}()
	fakeJobBlockChan := <-ch

	// Ensure that we see the assertion error.
	_, err := tc.Conns[0].ExecContext(ctx, `SET CLUSTER SETTING version = $1`, endCV.String())
	require.Regexp(t, "found multiple non-terminal jobs for version", err)

	// Let the fake, erroneous job finish.
	close(fakeJobBlockChan)
	require.NoError(t, <-runErr)

	// Launch a second migration which later we'll ensure does not kick off
	// another job. We'll make sure this happens by polling the trace to see
	// the log line indicating what we want.
	recCtx, getRecording, cancel := tracing.ContextWithRecordingSpan(ctx, "test")
	defer cancel()
	upgrade2Err := make(chan error, 1)
	go func() {
		// Use an internal executor to get access to the trace as it happens.
		_, err := tc.Server(0).InternalExecutor().(sqlutil.InternalExecutor).Exec(
			recCtx, "test", nil /* txn */, `SET CLUSTER SETTING version = $1`, endCV.String())
		upgrade2Err <- err
	}()

	testutils.SucceedsSoon(t, func() error {
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
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	// We'll take a specific range, still running at startCV, generate an
	// outgoing snapshot and then suspend it temporarily. We'll then bump the
	// cluster version on all the stores, as part of the migration process, and
	// then resume the snapshot process. Seeing as how the snapshot was
	// generated pre-version bump, off of a version of the range that hadn't
	// observed the migration corresponding to the latest cluster version, we
	// expect the store to reject it.

	key := tc.ScratchRange(t)
	require.NoError(t, tc.WaitForSplitAndInitialization(key))
	desc, err := tc.LookupRange(key)
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

	// Register the below raft migration.
	unregisterKVMigration := batcheval.TestingRegisterMigrationInterceptor(endCV.Version, func() {})
	defer unregisterKVMigration()

	// Register the top-level migration.
	unregister := migration.TestingRegisterMigrationInterceptor(endCV, func(
		ctx context.Context, cv clusterversion.ClusterVersion, c migration.Cluster,
	) error {
		return c.DB().Migrate(ctx, desc.StartKey, desc.EndKey, cv.Version)
	})
	defer unregister()

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
