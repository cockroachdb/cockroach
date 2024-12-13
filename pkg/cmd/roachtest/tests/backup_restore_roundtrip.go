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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
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
}

// backup-restore/round-trip tests that a round trip of creating a backup and
// restoring the created backup create the same objects.
func backupRestoreRoundTrip(
	ctx context.Context, t test.Test, c cluster.Cluster, sp roundTripSpecs,
) {
	pauseProbability := 0.2
	testRNG, seed := randutil.NewLockedPseudoRand()
	t.L().Printf("random seed: %d", seed)

	envOption := install.EnvOption([]string{
		"COCKROACH_MIN_RANGE_MAX_BYTES=1",
	})

	c.Start(ctx, t.L(), roachtestutil.MaybeUseMemoryBudget(t, 50), install.MakeClusterSettings(envOption), c.CRDBNodes())
	m := c.NewMonitor(ctx, c.CRDBNodes())

	m.Go(func(ctx context.Context) error {
		connectFunc := func(node int) (*gosql.DB, error) {
			conn, err := c.ConnE(ctx, t.L(), node)
			if err != nil {
				return nil, fmt.Errorf("failed to connect to node %d: %w", node, err)
			}

			return conn, err
		}
		testUtils, err := newCommonTestUtils(ctx, t, c, connectFunc, c.CRDBNodes(), sp.mock, sp.onlineRestore)
		if err != nil {
			return err
		}
		defer testUtils.CloseConnections()

		dbs := []string{"bank", "tpcc", schemaChangeDB}
		runBackgroundWorkload, err := startBackgroundWorkloads(ctx, t.L(), c, m, testRNG, c.CRDBNodes(), c.WorkloadNode(), testUtils, dbs)
		if err != nil {
			return err
		}
		tables, err := testUtils.loadTablesForDBs(ctx, t.L(), testRNG, dbs...)
		if err != nil {
			return err
		}
		d, err := newBackupRestoreTestDriver(ctx, t, c, testUtils, c.CRDBNodes(), dbs, tables)
		if err != nil {
			return err
		}
		if err := testUtils.setShortJobIntervals(ctx, testRNG); err != nil {
			return err
		}
		if err := testUtils.setClusterSettings(ctx, t.L(), c, testRNG); err != nil {
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
		defer stopBackgroundCommands()

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
			err = collection.verifyBackupCollection(ctx, t.L(), testRNG, d, true /* checkFiles */, true /* internalSystemJobs */)
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
		stopBackgroundCommands()
		return nil
	})

	m.Wait()
}

// startBackgroundWorkloads starts a TPCC, bank, and a system table workload in
// the background.
func startBackgroundWorkloads(
	ctx context.Context,
	l *logger.Logger,
	c cluster.Cluster,
	m cluster.Monitor,
	testRNG *rand.Rand,
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
	tpccInit, tpccRun := tpccWorkloadCmd(l, testRNG, numWarehouses, roachNodes)
	bankInit, bankRun := bankWorkloadCmd(l, testRNG, roachNodes, testUtils.mock)
	scInit, scRun := schemaChangeWorkloadCmd(l, testRNG, roachNodes, testUtils.mock)
	if err := prepSchemaChangeWorkload(ctx, workloadNode, testUtils, testRNG); err != nil {
		return nil, err
	}

	err := c.RunE(ctx, option.WithNodes(workloadNode), bankInit.String())
	if err != nil {
		return nil, err
	}

	err = c.RunE(ctx, option.WithNodes(workloadNode), tpccInit.String())
	if err != nil {
		return nil, err
	}

	handleChemaChangeError := func(err error) error {
		// If the UNEXPECTED ERROR detail appears, the workload likely flaked.
		// Otherwise, the workload could have failed due to other reasons like a node
		// crash.
		if err != nil && strings.Contains(errors.FlattenDetails(err), "UNEXPECTED ERROR") {
			return registry.ErrorWithOwner(registry.OwnerSQLFoundations, errors.Wrapf(err, "schema change workload failed"))
		}
		return err
	}
	err = c.RunE(ctx, option.WithNodes(workloadNode), scInit.String())
	if err != nil {
		return nil, handleChemaChangeError(err)
	}

	run := func() (func(), error) {
		tables, err := testUtils.loadTablesForDBs(ctx, l, testRNG, dbs...)
		if err != nil {
			return nil, err
		}
		stopBank := workloadWithCancel(m, func(ctx context.Context) error {
			return c.RunE(ctx, option.WithNodes(workloadNode), bankRun.String())
		})

		stopTPCC := workloadWithCancel(m, func(ctx context.Context) error {
			return c.RunE(ctx, option.WithNodes(workloadNode), tpccRun.String())
		})
		stopSC := workloadWithCancel(m, func(ctx context.Context) error {
			if err := c.RunE(ctx, option.WithNodes(workloadNode), scRun.String()); err != nil {
				return handleChemaChangeError(err)
			}
			return nil
		})

		stopSystemWriter := workloadWithCancel(m, func(ctx context.Context) error {
			// We use a separate RNG for the system table writer to avoid
			// non-determinism of the RNG usage due to the time-based nature of
			// the system writer workload. See
			// https://github.com/cockroachdb/cockroach/blob/master/pkg/cmd/roachtest/roachtestutil/mixedversion/README.md#note-non-deterministic-use-of-the-randrand-instance
			systemTableRNG := rand.New(rand.NewSource(testRNG.Int63()))
			return testUtils.systemTableWriter(ctx, l, systemTableRNG, dbs, tables)
		})

		stopBackgroundCommands := func() {
			stopBank()
			stopTPCC()
			stopSC()
			stopSystemWriter()
		}

		return stopBackgroundCommands, nil
	}

	return run, nil
}

// Connect makes a database handle to the node.
func (u *CommonTestUtils) Connect(node int) *gosql.DB {
	u.connCache.mu.Lock()
	defer u.connCache.mu.Unlock()
	return u.connCache.cache[node-1]
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
	_, db := u.RandomDB(rng, u.roachNodes)
	_, err := db.ExecContext(ctx, query, args...)
	return err
}

// QueryRow executes a query that is expected to return at most one row on a
// random node.
func (u *CommonTestUtils) QueryRow(
	ctx context.Context, rng *rand.Rand, query string, args ...interface{},
) *gosql.Row {
	_, db := u.RandomDB(rng, u.roachNodes)
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

func workloadWithCancel(m cluster.Monitor, fn func(ctx context.Context) error) func() {

	cancelWorkload := m.GoWithCancel(func(ctx context.Context) error {
		err := fn(ctx)
		if ctx.Err() != nil {
			// Workload context was cancelled as a normal part of test shutdown.
			return nil
		}
		return err
	})
	return cancelWorkload
}
