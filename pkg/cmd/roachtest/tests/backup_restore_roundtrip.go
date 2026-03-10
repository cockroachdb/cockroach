// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/task"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/failureinjection/failures"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

var (
	// maxRangeSizeBytes defines the possible non default (default is 512 MiB) maximum range
	// sizes that may get set for all user databases.
	maxRangeSizeBytes = []int64{4 << 20 /* 4 MiB*/, 32 << 20 /* 32 MiB */, 128 << 20}

	// clusterSettingsValuesBoundOnRangeSize defines the cluster settings that
	// should scale in proportion to the range size. For example, if the range
	// size is halved, all the values of these cluster settings should also be
	// halved.
	clusterSettingsScaledOnRangeSize = []string{
		"backup.restore_span.target_size",
		"bulkio.backup.file_size",
		"kv.bulk_sst.target_size",
	}
)

func handleSchemaChangeWorkloadError(err error) error {
	// If the UNEXPECTED ERROR detail appears, the workload likely flaked.
	// Otherwise, the workload could have failed due to other reasons like a node
	// crash.
	if err != nil {
		flattenedErr := errors.FlattenDetails(err)
		if strings.Contains(flattenedErr, "workload run error: ***") || strings.Contains(flattenedErr, "UNEXPECTED ERROR") || strings.Contains(flattenedErr, "UNEXPECTED COMMIT ERROR") {
			return registry.ErrorWithOwner(registry.OwnerSQLFoundations, errors.Wrapf(err, "schema change workload failed"))
		}
	}
	return err
}

const numFullBackups = 3

type roundTripSpecs struct {
	name                 string
	metamorphicRangeSize bool
	onlineRestore        bool
	mock                 bool
	skip                 string
}

func registerBackupRestoreRoundTrip(r registry.Registry) {
	for _, sp := range []roundTripSpecs{
		{
			name:                 "backup-restore/round-trip",
			metamorphicRangeSize: false,
		},
		{
			name:                 "backup-restore/small-ranges",
			metamorphicRangeSize: true,
		},
		{
			name:                 "backup-restore/online-restore",
			metamorphicRangeSize: false,
			onlineRestore:        true,
		},
		{
			name: "backup-restore/mock",
			mock: true,
			skip: "used only for debugging",
		},
	} {
		sp := sp
		r.Add(registry.TestSpec{
			Name:              sp.name,
			Timeout:           4 * time.Hour,
			Owner:             registry.OwnerDisasterRecovery,
			Cluster:           r.MakeClusterSpec(4, spec.WorkloadNode()),
			EncryptionSupport: registry.EncryptionMetamorphic,
			NativeLibs:        registry.LibGEOS,
			// See https://github.com/cockroachdb/cockroach/issues/105968
			CompatibleClouds:           registry.Clouds(spec.GCE, spec.Local),
			Suites:                     registry.Suites(registry.Nightly),
			TestSelectionOptOutSuites:  registry.Suites(registry.Nightly),
			Randomized:                 true,
			Skip:                       sp.skip,
			RequiresDeprecatedWorkload: true, // uses schemachange
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				backupRestoreRoundTrip(ctx, t, c, sp)
			},
		})
	}

	r.Add(registry.TestSpec{
		Name:                       "backup-restore/chaos",
		Timeout:                    4 * time.Hour,
		Owner:                      registry.OwnerDisasterRecovery,
		Cluster:                    r.MakeClusterSpec(4, spec.WorkloadNode()),
		EncryptionSupport:          registry.EncryptionMetamorphic,
		NativeLibs:                 registry.LibGEOS,
		CompatibleClouds:           registry.Clouds(spec.GCE, spec.AWS, spec.Azure, spec.Local),
		Suites:                     registry.Suites(registry.Nightly),
		TestSelectionOptOutSuites:  registry.Suites(registry.Nightly),
		Randomized:                 true,
		RequiresDeprecatedWorkload: true, // uses schemachange
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			backupRestoreChaos(ctx, t, c)
		},
	})
}

// backup-restore/round-trip tests that a round trip of creating a backup and
// restoring the created backup create the same objects.
func backupRestoreRoundTrip(
	ctx context.Context, t test.Test, c cluster.Cluster, sp roundTripSpecs,
) {
	pauseProbability := 0.2
	testRNG, seed := randutil.NewLockedPseudoRand()

	// Workload can only take a positive int as a seed, but seed could be a
	// negative int. Ensure the seed passed to workload is an int.
	workloadSeed := testRNG.Int63()
	t.L().Printf("random seed: %d; workload seed: %d", seed, workloadSeed)

	envOption := install.EnvOption([]string{
		"COCKROACH_MIN_RANGE_MAX_BYTES=1",
	})

	startOpts := roachtestutil.MaybeUseMemoryBudget(t, 50)
	startOpts.RoachprodOpts.ExtraArgs = []string{"--vmodule=split_queue=3,cloud_logging_transport=1"}
	c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(envOption), c.CRDBNodes())
	m := c.NewDeprecatedMonitor(ctx, c.CRDBNodes())

	m.Go(func(ctx context.Context) error {
		testUtils, err := setupBackupRestoreTestUtils(
			ctx, t, c, c.CRDBNodes(), c.CRDBNodes(), testRNG,
			withMock(sp.mock), withOnlineRestore(sp.onlineRestore), withCompaction(!sp.onlineRestore),
		)
		if err != nil {
			return err
		}
		defer testUtils.CloseConnections()

		dbs := []string{"bank", "tpcc", schemaChangeDB}
		d, runBackgroundWorkload, _, err := createDriversForBackupRestore(
			ctx, t, c, c.CRDBNodes(), testRNG, workloadSeed, testUtils, dbs,
		)
		if err != nil {
			return err
		}

		if sp.metamorphicRangeSize {
			if err := testUtils.setMaxRangeSizeAndDependentSettings(ctx, t, testRNG, dbs); err != nil {
				return err
			}
		}
		stopBackgroundCommands, err := runBackgroundWorkload()
		if err != nil {
			return err
		}
		defer func() {
			if stopBackgroundCommands != nil {
				// Since stopBackgroundCommands may get reassigned below, call
				// stopBackgroundCommands within an anonymous function to ensure the up
				// to date assignment gets called on defer.
				stopBackgroundCommands()
			}
		}()

		for i := 0; i < numFullBackups; i++ {
			allNodes := labeledNodes{Nodes: c.CRDBNodes(), Version: clusterupgrade.CurrentVersion().String()}
			bspec := backupSpec{
				PauseProbability: pauseProbability,
				Plan:             allNodes,
				Execute:          allNodes,
			}

			// Run backups.
			t.L().Printf("starting backup %d", i+1)
			collection, err := d.createBackupCollection(
				ctx, t.L(), t, testRNG, bspec, bspec, "round-trip-test-backup",
				true /* internalSystemsJobs */, false, /* isMultitenant */
			)
			if err != nil {
				return err
			}

			// If we're running a cluster backup, we need to reset the cluster
			// to restore it. We also intentionally stop background commands so
			// the workloads don't report errors.
			if _, ok := collection.btype.(*clusterBackup); ok {
				t.L().Printf("resetting cluster before verifying full cluster backup %d", i+1)
				stopBackgroundCommands()
				expectDeathsFn := func(n int) {
					m.ExpectDeaths(int32(n))
				}

				// Between each reset grab a debug zip from the cluster.
				zipPath := fmt.Sprintf("debug-%d.zip", timeutil.Now().Unix())
				if err := testUtils.cluster.FetchDebugZip(ctx, t.L(), zipPath); err != nil {
					t.L().Printf("failed to fetch a debug zip: %v", err)
				}
				if err := testUtils.resetCluster(ctx, t.L(), clusterupgrade.CurrentVersion(), expectDeathsFn, []install.ClusterSettingOption{}); err != nil {
					return err
				}
			}

			t.L().Printf("verifying backup %d", i+1)
			// Verify content in backups.
			err = d.verifyBackupCollection(
				ctx, t.L(), testRNG, collection,
				true /* checkFiles */, true /* internalSystemJobs */, nil, /* mvHelper */
			)
			if err != nil {
				return err
			}

			// Restart background commands after verifying full cluster backups.
			if _, ok := collection.btype.(*clusterBackup); ok {
				t.L().Printf("resuming workloads after verifying full cluster backup %d", i+1)
				stopBackgroundCommands, err = runBackgroundWorkload()
				if err != nil {
					return err
				}
			}
		}
		return nil
	})

	m.Wait()
}

func backupRestoreChaos(ctx context.Context, t test.Test, c cluster.Cluster) {
	testRNG, seed := randutil.NewLockedPseudoRand()
	t.L().Printf("random seed: %d", seed)

	workloadSeed := testRNG.Int63()
	t.L().Printf("workload seed: %d", workloadSeed)

	startOpts := roachtestutil.MaybeUseMemoryBudget(t, 50)
	startOpts.RoachprodOpts.ExtraArgs = []string{"--vmodule=split_queue=3,cloud_logging_transport=1"}
	c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), c.CRDBNodes())

	grp := t.NewGroup()
	grp.Go(func(ctx context.Context, l *logger.Logger) error {
		const numToKill = 1
		failureNodes := c.CRDBNodes()[:numToKill]
		liveNodes := c.CRDBNodes()[numToKill:] // The set of nodes that will not be failure injected
		isGraceful := testRNG.Intn(2) == 0
		l.Printf("process kill failure isGraceful: %t", isGraceful)
		// To clarify, the grace period refers to the amount of time we wait when
		// draining gracefully before sending a SIGKILL and is unrelated to the wait
		// in injectAndRecoverFailure.
		failer, args, err := roachtestutil.MakeProcessKillFailer(
			l, c, failureNodes, isGraceful, 5*time.Minute, /* gracePeriod */
		)
		require.NoError(t, err)
		require.NoError(t, failer.Setup(ctx, l, args))
		defer func() {
			if err := failer.Cleanup(ctx, l); err != nil {
				l.Printf("failed to clean up failure: %v", err)
			}
		}()

		doOnlineRestore := testRNG.Intn(2) == 0
		// TODO (kev-cao): Running this test with withMock(true) causes the
		// backups to complete too quickly, but the default workload sizes also take
		// quite a while to backup. Considering the goal of this test, it'd be good
		// to add some options to provide the caller with more flexibility over the
		// workload.
		testUtils, err := setupBackupRestoreTestUtils(
			ctx, t, c, liveNodes, c.CRDBNodes(), testRNG,
			withCompaction(true), withOnlineRestore(doOnlineRestore),
		)
		require.NoError(t, err)
		defer testUtils.CloseConnections()

		dbs := []string{"bank", "tpcc", schemaChangeDB}
		d, runWorkloads, _, err := createDriversForBackupRestore(
			ctx, t, c, liveNodes, testRNG, workloadSeed, testUtils, dbs,
		)
		require.NoError(t, err)
		stopWorkloads, err := runWorkloads()
		require.NoError(t, err)
		defer stopWorkloads()

		bspec := backupSpec{
			// We query for job progress via plannodes, so we don't want to use nodes
			// subject to failure injection in the Plan spec.
			Plan:    labeledNodes{Nodes: liveNodes, Version: clusterupgrade.CurrentVersion().String()},
			Execute: labeledNodes{Nodes: c.CRDBNodes(), Version: clusterupgrade.CurrentVersion().String()},
		}
		builder := d.NewCollectionBuilder(
			l, t, testRNG, "backup-restore-chaos", bspec, bspec, true /* internalSystemsJobs */, false, /* isMultitenant */
			WithClusterScope(),
		)
		jobID, err := builder.TakeFull(ctx)
		require.NoError(t, err)
		require.NoError(t, injectAndRecoverFailure(
			ctx, t, l, testUtils, testRNG, jobID, failer, args,
			randFloatBetween(testRNG, 0.15, 0.65),
			// We use a number greater than 1.0 so that there is a chance of a node
			// failing until the backup is complete.
			randFloatBetween(testRNG, 0.65, 1.1),
		))
		require.NoError(t, builder.WaitForLastJob(ctx))

		for i := 0; i < 3; i++ {
			jobID, err = builder.TakeInc(ctx)
			require.NoError(t, err)
			require.NoError(t, injectAndRecoverFailure(
				ctx, t, l, testUtils, testRNG, jobID, failer, args,
				randFloatBetween(testRNG, 0.15, 0.65),
				randFloatBetween(testRNG, 0.65, 1.1),
			))
			require.NoError(t, builder.WaitForLastJob(ctx))
		}

		jobID, err = builder.TakeCompacted(ctx, 1, 3)
		require.NoError(t, err)
		require.NoError(t, injectAndRecoverFailure(
			ctx, t, l, testUtils, testRNG, jobID, failer, args,
			randFloatBetween(testRNG, 0.15, 0.65),
			randFloatBetween(testRNG, 0.65, 1.1),
		))
		require.NoError(t, builder.WaitForLastJob(ctx))
		collection, err := builder.Finalize(ctx)
		require.NoError(t, err)

		stopWorkloads()
		testUtils.takeDebugZip(ctx, l)
		err = testUtils.resetCluster(ctx, l, clusterupgrade.CurrentVersion(), nil, nil)
		require.NoError(t, err)

		// TODO (kev-cao): Make restores asynchronous in backup restore driver so
		// that we can inject failures there as well.
		err = d.verifyBackupCollection(
			ctx, t.L(), testRNG, collection,
			true /* checkFiles */, true /* internalSystemJobs */, nil, /* mvHelper */
		)
		require.NoError(t, err)
		return nil
	})

	grp.Wait()
}

// injectAndRecoverFailure waits for the job to progress past a specified
// percentage, then injects the specified failure, waits for the failure to
// propagate, waits for the job to progress past another specified percentage,
// and then recovers from the failure. It synchronously waits until the job is
// completed before returning.
// NB: If the job fails before the recovery threshold, we skip recovery; the
// test should fail on the returned error.
func injectAndRecoverFailure(
	ctx context.Context,
	t test.Test,
	l *logger.Logger,
	testUtils *CommonTestUtils,
	rng *rand.Rand,
	jobID int,
	failer *failures.Failer,
	args failures.FailureArgs,
	killPercentage float64,
	recoverPercentage float64,
) error {
	// User may pass in recovery percentage > 1 to indicate that recovery should
	// only occur after the job has completed. So we need to cap it here.
	recoverPercentage = min(recoverPercentage, killPercentage+0.1)

	grp := t.NewErrorGroup(task.WithContext(ctx))
	cancelFailer := grp.GoWithCancel(func(ctx context.Context, l *logger.Logger) error {
		l.Printf(
			"waiting for job %d to progress past %.2f%% before injecting failure",
			jobID, killPercentage*100,
		)
		if err := testUtils.waitForJobProgressPercentage(ctx, l, rng, jobID, killPercentage); err != nil {
			return err
		}
		l.Printf("injecting failure")
		if err := failer.Inject(ctx, l, args); err != nil {
			return err
		}
		if err := failer.WaitForFailureToPropagate(ctx, l); err != nil {
			return err
		}
		l.Printf(
			"failure propagated, waiting for job %d to progress past %.2f%% before recovering",
			jobID, recoverPercentage*100,
		)
		if err := testUtils.waitForJobProgressPercentage(ctx, l, rng, jobID, recoverPercentage); err != nil {
			return err
		}
		l.Printf("job %d progressed past %.2f%%, recovering from failure", jobID, recoverPercentage*100)

		if err := failer.Recover(ctx, l); err != nil {
			return err
		}
		return failer.WaitForFailureToRecover(ctx, l)
	}, task.Name(fmt.Sprintf("inject-failure-and-recovery-job-%d", jobID)))

	grp.Go(func(ctx context.Context, l *logger.Logger) error {
		l.Printf("waiting for job %d to succeed", jobID)
		if err := testUtils.waitForJobSuccess(ctx, l, rng, jobID, true /* internalSystemJobs */); err != nil {
			// If the job fails, the test fails, so no need to attempt to recover.
			cancelFailer()
			return err
		}
		return nil
	}, task.Name(fmt.Sprintf("wait-for-job-%d-success", jobID)))

	return grp.WaitE()
}

// initBackgroundWorkloads returns a function that starts a TPCC, bank, and a
// system table workload in the background.
func initBackgroundWorkloads(
	ctx context.Context,
	t test.Test,
	l *logger.Logger,
	c cluster.Cluster,
	testRNG *rand.Rand,
	seed int64,
	roachNodes, workloadNode option.NodeListOption,
	testUtils *CommonTestUtils,
	dbs []string,
) (func() (func(), error), error) {
	// numWarehouses is picked as a number that provides enough work
	// for the cluster used in this test without overloading it,
	// which can make the backups take much longer to finish.
	numWarehouses := 100
	if testUtils.mock {
		numWarehouses = 10
	}
	tpccInit, tpccRun := tpccWorkloadCmd(l, testRNG, seed, numWarehouses, roachNodes)
	bankInit, bankRun := bankWorkloadCmd(l, testRNG, seed, roachNodes, testUtils.mock)
	scInit, scRun := schemaChangeWorkloadCmd(l, testRNG, seed, roachNodes, testUtils.mock)

	initGroup := t.NewErrorGroup(task.WithContext(ctx))
	initGroup.Go(func(ctx context.Context, l *logger.Logger) error {
		return c.RunE(ctx, option.WithNodes(workloadNode), bankInit.String())
	}, task.Name("init-bank"))
	initGroup.Go(func(ctx context.Context, l *logger.Logger) error {
		return c.RunE(ctx, option.WithNodes(workloadNode), tpccInit.String())
	}, task.Name("init-tpcc"))
	initGroup.Go(func(ctx context.Context, l *logger.Logger) (err error) {
		defer func() {
			if err != nil {
				err = handleSchemaChangeWorkloadError(err)
			}
		}()
		if err := prepSchemaChangeWorkload(ctx, workloadNode, testUtils, testRNG); err != nil {
			return err
		}
		return c.RunE(ctx, option.WithNodes(workloadNode), scInit.String())
	}, task.Name("init-schemachange"))
	if err := initGroup.WaitE(); err != nil {
		return nil, err
	}

	runWorkloadTasks := func() (func(), error) {
		tables, err := testUtils.loadTablesForDBs(ctx, l, testRNG, dbs...)
		if err != nil {
			return nil, err
		}

		runGroup := t.NewGroup()
		runGroup.Go(func(ctx context.Context, l *logger.Logger) error {
			return c.RunE(ctx, option.WithNodes(workloadNode), bankRun.String())
		}, task.Name("run-bank"))
		runGroup.Go(func(ctx context.Context, l *logger.Logger) error {
			return c.RunE(ctx, option.WithNodes(workloadNode), tpccRun.String())
		}, task.Name("run-tpcc"))
		runGroup.Go(func(ctx context.Context, l *logger.Logger) error {
			return handleSchemaChangeWorkloadError(
				c.RunE(ctx, option.WithNodes(workloadNode), scRun.String()),
			)
		}, task.Name("run-schemachange"))
		runGroup.Go(func(ctx context.Context, l *logger.Logger) error {
			// We use a separate RNG for the system table writer to avoid
			// non-determinism of the RNG usage due to the time-based nature of
			// the system writer workload. See
			// https://github.com/cockroachdb/cockroach/blob/master/pkg/cmd/roachtest/roachtestutil/mixedversion/README.md#note-non-deterministic-use-of-the-randrand-instance
			systemTableRNG := rand.New(rand.NewSource(testRNG.Int63()))
			return testUtils.systemTableWriter(ctx, l, systemTableRNG, dbs, tables)
		}, task.Name("run-system-table-writer"))

		return runGroup.Cancel, nil
	}

	return runWorkloadTasks, nil
}

// Connect makes a database handle to the node.
func (u *CommonTestUtils) Connect(node int) *gosql.DB {
	u.connCache.mu.Lock()
	defer u.connCache.mu.Unlock()
	return u.connCache.cache[node]
}

// RandomNode returns a random nodeID in the cluster.
func (u *CommonTestUtils) RandomNode(rng *rand.Rand, nodes option.NodeListOption) int {
	return nodes[rng.Intn(len(nodes))]
}

func (u *CommonTestUtils) RandomDB(rng *rand.Rand, nodes option.NodeListOption) (int, *gosql.DB) {
	node := u.RandomNode(rng, nodes)
	return node, u.Connect(node)
}

func (u *CommonTestUtils) Exec(
	ctx context.Context, rng *rand.Rand, query string, args ...interface{},
) error {
	_, db := u.RandomDB(rng, u.queryNodes)
	_, err := db.ExecContext(ctx, query, args...)
	return err
}

// QueryRow executes a query that is expected to return at most one row on a
// random node.
func (u *CommonTestUtils) QueryRow(
	ctx context.Context, rng *rand.Rand, query string, args ...interface{},
) *gosql.Row {
	_, db := u.RandomDB(rng, u.queryNodes)
	return db.QueryRowContext(ctx, query, args...)
}

func (u *CommonTestUtils) now() string {
	return hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}.AsOfSystemTime()
}

func (u *CommonTestUtils) CloseConnections() {
	u.connCache.mu.Lock()
	defer u.connCache.mu.Unlock()

	for _, db := range u.connCache.cache {
		if db != nil {
			_ = db.Close()
		}
	}
}

// setupBackupRestoreTestUtils sets up a CommonTestUtils instance for backup and
// restore tests and initializes some useful settings.
//
// NB: queryNodes should exclude any nodes that can potentially be taken down by
// failure injection, as they are used to perform queries.
func setupBackupRestoreTestUtils(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	roachNodes option.NodeListOption,
	queryNodes option.NodeListOption,
	rng *rand.Rand,
	testOpts ...commonTestOption,
) (*CommonTestUtils, error) {
	connectFunc := func(node int) (*gosql.DB, error) {
		conn, err := c.ConnE(ctx, t.L(), node)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to node %d: %w", node, err)
		}

		return conn, err
	}
	// TODO (msbutler): enable compaction for online restore test once inc layer limit is increased.
	testUtils, err := newCommonTestUtils(ctx, t, c, connectFunc, roachNodes, queryNodes, testOpts...)
	if err != nil {
		return nil, err
	}
	if err := testUtils.setShortJobIntervals(ctx, rng); err != nil {
		return nil, err
	}
	if err := testUtils.setClusterSettings(ctx, t.L(), c, rng); err != nil {
		return nil, err
	}
	return testUtils, err
}

// createDriversForBackupRestore creates a BackupRestoreTestDriver for backup
// and restore tests, a handler to trigger background workloads, and the tables
// that are used in the test. The tables are mapped to the databases that were
// passed in.
//
// NB: queryNodes should exclude any nodes that can potentially be taken down by
// failure injection. The workloads run against these nodes and expect them to be
// alive, as well as the test driver, which uses these nodes to perform queries.
func createDriversForBackupRestore(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	queryNodes option.NodeListOption,
	rng *rand.Rand,
	workloadSeed int64,
	testUtils *CommonTestUtils,
	dbs []string,
) (*BackupRestoreTestDriver, func() (func(), error), [][]string, error) {
	runBackgroundWorkload, err := initBackgroundWorkloads(
		ctx, t, t.L(), c, rng, workloadSeed, queryNodes, c.WorkloadNode(), testUtils, dbs,
	)
	if err != nil {
		return nil, nil, nil, err
	}
	tables, err := testUtils.loadTablesForDBs(ctx, t.L(), rng, dbs...)
	if err != nil {
		return nil, nil, nil, err
	}
	d, err := newBackupRestoreTestDriver(ctx, t, c, testUtils, queryNodes, dbs, tables)
	if err != nil {
		return nil, nil, nil, err
	}
	return d, runBackgroundWorkload, tables, nil
}
