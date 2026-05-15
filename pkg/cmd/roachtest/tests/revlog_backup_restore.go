// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	apd "github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func registerRevlogBackupRestore(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:              "revlog-backup-restore/roundtrip",
		Timeout:           4 * time.Hour,
		Owner:             registry.OwnerDisasterRecovery,
		Cluster:           r.MakeClusterSpec(4, spec.WorkloadNode()),
		EncryptionSupport: registry.EncryptionMetamorphic,
		CompatibleClouds:  registry.Clouds(spec.GCE, spec.Local),
		Suites:            registry.Suites(registry.Nightly),
		Randomized:        true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			continuousBackupRestoreRoundTrip(ctx, t, c)
		},
	})

	r.Add(registry.TestSpec{
		Name:              "revlog-backup-restore/node-restart",
		Timeout:           4 * time.Hour,
		Owner:             registry.OwnerDisasterRecovery,
		Cluster:           r.MakeClusterSpec(4, spec.WorkloadNode()),
		EncryptionSupport: registry.EncryptionMetamorphic,
		CompatibleClouds:  registry.Clouds(spec.GCE, spec.Local),
		Suites:            registry.Suites(registry.Nightly),
		Randomized:        true,
		Skip:              "revlog job does not recover after node restart",
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			revlogNodeRestart(ctx, t, c)
		},
	})
}

// continuousBackupRestoreRoundTrip tests that a backup created with
// `WITH REVISION STREAM` (continuous backup) can be restored to an
// arbitrary point in time within the backup chain and the restored
// data matches the original.
func continuousBackupRestoreRoundTrip(ctx context.Context, t test.Test, c cluster.Cluster) {
	testRNG, seed := randutil.NewLockedPseudoRand()
	workloadSeed := testRNG.Int63()
	t.L().Printf("random seed: %d; workload seed: %d", seed, workloadSeed)

	startOpts := roachtestutil.MaybeUseMemoryBudget(t, 50)
	c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), c.CRDBNodes())

	// Enable rangefeeds, required for the revlog sibling job.
	conn := c.Conn(ctx, t.L(), 1)
	defer conn.Close()
	_, err := conn.ExecContext(ctx, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)
	require.NoError(t, err)

	testUtils, err := setupBackupRestoreTestUtils(
		ctx, t, c, testRNG, withCompaction(true),
	)
	require.NoError(t, err)
	defer testUtils.CloseConnections()

	// NB: schemachange is excluded because it triggers DeleteRange
	// events that the revlog rangefeed does not yet handle.
	dbs := []string{"bank", "tpcc"}
	d, runBackgroundWorkload, _, err := createDriversForBackupRestore(
		ctx, t, c, testRNG, workloadSeed, testUtils, dbs, []string{"schemachange"},
	)
	require.NoError(t, err)

	stopWorkloads, err := runBackgroundWorkload()
	require.NoError(t, err)
	defer stopWorkloads()

	allNodes := labeledNodes{
		Nodes:   c.CRDBNodes(),
		Version: clusterupgrade.CurrentVersion().String(),
	}
	bspec := backupSpec{Plan: allNodes, Execute: allNodes}

	builder := d.NewCollectionBuilder(
		t.L(), t, testRNG, "continuous-roundtrip",
		bspec, bspec, true /* internalSystemJobs */, false, /* isMultitenant */
		WithRevisionStream(),
	)

	_, err = builder.TakeFullSync(ctx)
	require.NoError(t, err)

	for range 2 {
		d.randomWait(t.L(), testRNG)
		_, err = builder.TakeIncSync(ctx)
		require.NoError(t, err)
	}

	_, err = builder.TakeCompactedSync(ctx, 1, 2)
	require.NoError(t, err)

	for range 2 {
		d.randomWait(t.L(), testRNG)
		_, err = builder.TakeIncSync(ctx)
		require.NoError(t, err)
	}

	collectionURI, err := builder.CollectionURI()
	require.NoError(t, err)

	fingerprintTime, err := pickRandomTimeBetweenBackups(ctx, testUtils, testRNG, collectionURI)
	require.NoError(t, err)
	t.L().Printf("chosen fingerprint time: %s", fingerprintTime)

	t.L().Printf("waiting for revlog resolved timestamp to pass fingerprint time")
	err = waitForRevlogResolvedPast(ctx, testUtils, testRNG, fingerprintTime)
	require.NoError(t, err)

	builder.SetFingerprintTime(fingerprintTime)
	collection, err := builder.Finalize(ctx)
	require.NoError(t, err)

	if _, ok := collection.btype.(*clusterBackup); ok {
		stopWorkloads()
		testUtils.takeDebugZip(ctx, t.L())
		err = testUtils.resetCluster(
			ctx, t.L(), clusterupgrade.CurrentVersion(), nil, nil,
		)
		require.NoError(t, err)
	}

	restoreJob, err := d.RestoreSync(
		ctx, t.L(), testRNG, collection,
		false /* checkFiles */, true /* internalSystemJobs */, nil, /* mvHelper */
	)
	require.NoError(t, err)
	require.NoError(t, restoreJob.ValidateRestore(ctx))
}

// revlogNodeRestart tests that the revlog producer pipeline survives
// a node restart. The test takes a backup with revision stream, runs
// workloads, kills and restarts a node, takes more incrementals, and
// verifies the restored data matches the original.
func revlogNodeRestart(ctx context.Context, t test.Test, c cluster.Cluster) {
	testRNG, seed := randutil.NewLockedPseudoRand()
	workloadSeed := testRNG.Int63()
	t.L().Printf("random seed: %d; workload seed: %d", seed, workloadSeed)

	startOpts := roachtestutil.MaybeUseMemoryBudget(t, 50)
	c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), c.CRDBNodes())

	conn := c.Conn(ctx, t.L(), 1)
	defer conn.Close()
	_, err := conn.ExecContext(ctx, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)
	require.NoError(t, err)

	testUtils, err := setupBackupRestoreTestUtils(
		ctx, t, c, testRNG, withCompaction(true),
	)
	require.NoError(t, err)
	defer testUtils.CloseConnections()

	dbs := []string{"bank", "tpcc"}
	d, runBackgroundWorkload, _, err := createDriversForBackupRestore(
		ctx, t, c, testRNG, workloadSeed, testUtils, dbs, []string{"schemachange"},
	)
	require.NoError(t, err)

	stopWorkloads, err := runBackgroundWorkload()
	require.NoError(t, err)
	defer stopWorkloads()

	allNodes := labeledNodes{
		Nodes:   c.CRDBNodes(),
		Version: clusterupgrade.CurrentVersion().String(),
	}
	bspec := backupSpec{Plan: allNodes, Execute: allNodes}

	builder := d.NewCollectionBuilder(
		t.L(), t, testRNG, "revlog-node-restart",
		bspec, bspec, true /* internalSystemJobs */, false, /* isMultitenant */
		WithRevisionStream(),
	)

	_, err = builder.TakeFullSync(ctx)
	require.NoError(t, err)

	d.randomWait(t.L(), testRNG)
	_, err = builder.TakeIncSync(ctx)
	require.NoError(t, err)

	collectionURI, err := builder.CollectionURI()
	require.NoError(t, err)

	t.L().Printf("waiting for revlog to resolve past first incremental")
	incEndTime, err := latestBackupEndTime(ctx, testUtils, testRNG, collectionURI)
	require.NoError(t, err)
	err = waitForRevlogResolvedPast(ctx, testUtils, testRNG, incEndTime)
	require.NoError(t, err)

	// Restart node 3 (not node 1, which holds nodelocal storage).
	t.L().Printf("stopping node 3")
	c.Stop(ctx, t.L(), option.DefaultStopOpts(), c.Node(3))
	time.Sleep(10 * time.Second)

	t.L().Printf("restarting node 3")
	c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), c.Node(3))
	time.Sleep(15 * time.Second)

	d.randomWait(t.L(), testRNG)
	_, err = builder.TakeIncSync(ctx)
	require.NoError(t, err)

	t.L().Printf("waiting for revlog to resolve past second incremental")
	postRestartEndTime, err := latestBackupEndTime(ctx, testUtils, testRNG, collectionURI)
	require.NoError(t, err)
	err = waitForRevlogResolvedPast(ctx, testUtils, testRNG, postRestartEndTime)
	require.NoError(t, err)

	fingerprintTime, err := pickRandomTimeBetweenBackups(ctx, testUtils, testRNG, collectionURI)
	require.NoError(t, err)
	t.L().Printf("chosen fingerprint time: %s", fingerprintTime)

	builder.SetFingerprintTime(fingerprintTime)
	collection, err := builder.Finalize(ctx)
	require.NoError(t, err)

	if _, ok := collection.btype.(*clusterBackup); ok {
		stopWorkloads()
		testUtils.takeDebugZip(ctx, t.L())
		err = testUtils.resetCluster(
			ctx, t.L(), clusterupgrade.CurrentVersion(), nil, nil,
		)
		require.NoError(t, err)
	}

	restoreJob, err := d.RestoreSync(
		ctx, t.L(), testRNG, collection,
		false /* checkFiles */, true /* internalSystemJobs */, nil, /* mvHelper */
	)
	require.NoError(t, err)
	require.NoError(t, restoreJob.ValidateRestore(ctx))
}

// pickRandomTimeBetweenBackups queries the backup collection for its
// backup times and picks a random HLC timestamp strictly between the
// earliest and latest backup.
func pickRandomTimeBetweenBackups(
	ctx context.Context, testUtils *CommonTestUtils, rng *rand.Rand, collectionURI string,
) (string, error) {
	_, db := testUtils.RandomDB(rng, testUtils.roachNodes)
	if _, err := db.ExecContext(ctx, `SET use_backups_with_ids = true`); err != nil {
		return "", errors.Wrap(err, "setting use_backups_with_ids")
	}

	var minTime, maxTime time.Time
	if err := db.QueryRowContext(ctx,
		fmt.Sprintf(
			`SELECT min(backup_time), max(backup_time) FROM [SHOW BACKUPS IN '%s']`,
			collectionURI,
		),
	).Scan(&minTime, &maxTime); err != nil {
		return "", errors.Wrap(err, "querying backup times")
	}

	minWall := minTime.UnixNano()
	maxWall := maxTime.UnixNano()
	if minWall == maxWall {
		return "", errors.New("need at least 2 backups with distinct times")
	}

	offset := 1 + rng.Int63n(maxWall-minWall-1)
	chosen := hlc.Timestamp{WallTime: minWall + offset}

	return chosen.AsOfSystemTime(), nil
}

// latestBackupEndTime returns the end time of the most recent backup
// in the collection as an HLC decimal string.
func latestBackupEndTime(
	ctx context.Context, testUtils *CommonTestUtils, rng *rand.Rand, collectionURI string,
) (string, error) {
	_, db := testUtils.RandomDB(rng, testUtils.roachNodes)
	if _, err := db.ExecContext(ctx, `SET use_backups_with_ids = true`); err != nil {
		return "", errors.Wrap(err, "setting use_backups_with_ids")
	}

	var t time.Time
	if err := db.QueryRowContext(ctx,
		fmt.Sprintf(
			`SELECT backup_time FROM [SHOW BACKUPS IN '%s'] LIMIT 1`,
			collectionURI,
		),
	).Scan(&t); err != nil {
		return "", errors.Wrap(err, "querying latest backup time")
	}

	return hlc.Timestamp{WallTime: t.UnixNano()}.AsOfSystemTime(), nil
}

// waitForRevlogResolvedPast finds the revlog sibling job and polls
// until its resolved timestamp passes the given target time.
func waitForRevlogResolvedPast(
	ctx context.Context, testUtils *CommonTestUtils, rng *rand.Rand, targetAOST string,
) error {
	_, db := testUtils.RandomDB(rng, testUtils.roachNodes)

	// Find the revlog sibling job. Its description starts with "REVLOG:".
	var jobID int
	err := db.QueryRowContext(ctx,
		`SELECT job_id FROM [SHOW JOBS]
		 WHERE description LIKE 'REVLOG:%'
		 ORDER BY created DESC LIMIT 1`,
	).Scan(&jobID)
	if err != nil {
		return errors.Wrap(err, "finding revlog job")
	}

	parseAOST := func(aost string) (hlc.Timestamp, error) {
		d, _, err := apd.NewFromString(aost)
		if err != nil {
			return hlc.Timestamp{}, err
		}
		return hlc.DecimalToHLC(d)
	}

	targetTS, err := parseAOST(targetAOST)
	if err != nil {
		return errors.Wrapf(err, "parsing target AOST %s", targetAOST)
	}

	return testutils.SucceedsWithinError(func() error {
		var status string
		var resolvedAOST *string
		err := db.QueryRowContext(ctx,
			fmt.Sprintf(
				`SELECT status, resolved_timestamp::STRING
				 FROM [SHOW JOB %d WITH RESOLVED TIMESTAMP]`, jobID),
		).Scan(&status, &resolvedAOST)
		if err != nil {
			return errors.Wrapf(err, "querying resolved timestamp for revlog job %d", jobID)
		}
		if status != "running" {
			return errors.Newf("revlog job %d is %s, expected running", jobID, status)
		}
		if resolvedAOST == nil {
			return errors.Newf("revlog job %d has no resolved timestamp yet (target %s)", jobID, targetAOST)
		}

		resolvedTS, err := parseAOST(*resolvedAOST)
		if err != nil {
			return errors.Wrapf(err, "parsing resolved timestamp %s", *resolvedAOST)
		}
		if resolvedTS.Less(targetTS) {
			return errors.Newf(
				"revlog job %d resolved timestamp %s has not passed target %s",
				jobID, *resolvedAOST, targetAOST,
			)
		}
		return nil
	}, 3*time.Minute)
}
