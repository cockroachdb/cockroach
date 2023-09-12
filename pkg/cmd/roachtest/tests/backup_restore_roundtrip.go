// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	gosql "database/sql"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"math/rand"
	"time"
)

func registerBackupRestoreRoundTrip(r registry.Registry) {
	// backup-restore/round-trip tests that a round trip of creating a backup and
	// restoring the created backup create the same objects.
	r.Add(registry.TestSpec{
		Name:              "backup-restore/round-trip",
		Timeout:           8 * time.Hour,
		Owner:             registry.OwnerDisasterRecovery,
		Cluster:           r.MakeClusterSpec(5),
		EncryptionSupport: registry.EncryptionMetamorphic,
		RequiresLicense:   true,
		Run:               backupRestoreRoundTrip,
	})
}

func backupRestoreRoundTrip(ctx context.Context, t test.Test, c cluster.Cluster) {
	if c.Spec().Cloud != spec.GCE {
		t.Skip("uses gs://cockroachdb-backup-testing; see https://github.com/cockroachdb/cockroach/issues/105968")
	}

	pauseProbability := 0.2
	roachNodes := c.Range(1, c.Spec().NodeCount-1)
	workloadNode := c.Node(c.Spec().NodeCount)
	testRNG, seed := randutil.NewLockedPseudoRand()
	t.L().Printf("random seed: %d", seed)

	// Upload binaries and start cluster.
	uploadVersion(ctx, t, c, workloadNode, clusterupgrade.MainVersion)
	uploadVersion(ctx, t, c, roachNodes, clusterupgrade.MainVersion)

	c.Start(ctx, t.L(), option.DefaultStartOptsNoBackups(), install.MakeClusterSettings(install.SecureOption(true)))
	m := c.NewMonitor(ctx, roachNodes)

	m.Go(func(ctx context.Context) error {
		testUtils, err := newCommonTestUtils(ctx, t, c, roachNodes)
		if err != nil {
			return err
		}
		defer testUtils.CloseConnections()

		dbs := []string{"bank", "tpcc"}
		stopBackgroundCommands, err := startBackgroundWorkloads(ctx, t.L(), c, m, testRNG, roachNodes, workloadNode, testUtils, dbs)
		if err != nil {
			return err
		}

		tables, err := testUtils.loadTablesForDBs(ctx, t.L(), testRNG, dbs...)
		if err != nil {
			return err
		}

		d, err := newBackupRestoreTestDriver(ctx, t, c, testUtils, roachNodes, dbs, tables)
		if err != nil {
			return err
		}

		if err := testUtils.setShortJobIntervals(ctx, testRNG); err != nil {
			return err
		}
		if err := testUtils.setClusterSettings(ctx, t.L(), testRNG); err != nil {
			return err
		}

		// Run backups.
		allNodes := labeledNodes{Nodes: roachNodes, Version: clusterupgrade.MainVersion}
		bspec := backupSpec{
			PauseProbability: pauseProbability,
			Plan:             allNodes,
			Execute:          allNodes,
		}
		collection, err := d.createBackupCollection(ctx, t.L(), testRNG, bspec, bspec, "round-trip-test-backup", true)
		if err != nil {
			return err
		}

		stopBackgroundCommands()

		if _, ok := collection.btype.(*clusterBackup); ok {
			expectDeathsFn := func(n int) {
				m.ExpectDeaths(int32(n))
			}

			if err := testUtils.resetCluster(ctx, t.L(), clusterupgrade.MainVersion, expectDeathsFn); err != nil {
				return err
			}
		}

		// Verify content in backups.
		return collection.verifyBackupCollection(ctx, t.L(), testRNG, d, true /* checkFiles */, true /* internalSystemJobs */)
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
) (func(), error) {
	// numWarehouses is picked as a number that provides enough work
	// for the cluster used in this test without overloading it,
	// which can make the backups take much longer to finish.
	const numWarehouses = 100
	tpccInit, tpccRun := tpccWorkloadCmd(numWarehouses, roachNodes)
	bankInit, bankRun := bankWorkloadCmd(testRNG, roachNodes)

	err := c.RunE(ctx, workloadNode, bankInit.String())
	if err != nil {
		return nil, err
	}

	err = c.RunE(ctx, workloadNode, tpccInit.String())
	if err != nil {
		return nil, err
	}

	tables, err := testUtils.loadTablesForDBs(ctx, l, testRNG, dbs...)
	if err != nil {
		return nil, err
	}

	stopBank := m.GoWithCancel(func(ctx context.Context) error {
		return c.RunE(ctx, workloadNode, bankRun.String())
	})
	stopTPCC := m.GoWithCancel(func(ctx context.Context) error {
		return c.RunE(ctx, workloadNode, tpccRun.String())
	})

	stopSystemWriter := m.GoWithCancel(func(ctx context.Context) error {
		return testUtils.systemTableWriter(ctx, l, testRNG, dbs, tables)
	})

	stopBackgroundCommands := func() {
		stopBank()
		stopTPCC()
		stopSystemWriter()
	}

	return stopBackgroundCommands, nil
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

func (u *CommonTestUtils) RandomDB(
	rng *rand.Rand, nodes option.NodeListOption,
) (int, *gosql.DB) {
	node := u.RandomNode(rng, nodes)
	return node, u.Connect(node)
}

func (u *CommonTestUtils) Exec(ctx context.Context, rng *rand.Rand, query string, args ...interface{}) error {
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
