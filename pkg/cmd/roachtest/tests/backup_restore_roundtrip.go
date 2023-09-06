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
	"fmt"
	"math/rand"
	"strings"
	"time"

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
	"github.com/cockroachdb/errors"
)

// possibleNumIncrementalBackups are the possible lengths of the incremental
// backup chain for the backup in the round trip test.
var possibleNumIncrementalBackups = []int{
	1,
	5,
	20,
}

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
		d, err := newBackupRestoreTestDriver(ctx, t, c, roachNodes, "bank", "tpcc")
		if err != nil {
			return err
		}

		stopBackgroundCommands, err := startBackgroundWorkloads(ctx, t.L(), c, testRNG, workloadNode, d)
		if err != nil {
			return err
		}

		if err := d.loadTables(ctx, t.L(), testRNG); err != nil {
			return err
		}
		if err := d.setShortJobIntervals(testRNG); err != nil {
			return err
		}
		if err := d.setClusterSettings(t.L(), testRNG); err != nil {
			return err
		}

		// Run backups.
		if err := runBackupsAndSaveContents(ctx, t.L(), testRNG, roachNodes, pauseProbability, d); err != nil {
			return err
		}

		if err := stopBackgroundCommands(); err != nil {
			return err
		}

		expectDeathsFn := func(n int) {
			m.ExpectDeaths(int32(n))
		}

		// Verify content in backups.
		if err := d.verifyAllBackupsOnVersions(ctx, t.L(), testRNG, expectDeathsFn, clusterupgrade.MainVersion); err != nil {
			return err
		}
		return nil
	})

	m.Wait()
}

func runBackupsAndSaveContents(
	ctx context.Context,
	l *logger.Logger,
	rng *rand.Rand,
	nodes option.NodeListOption,
	pauseProbability float64,
	driver *BackupRestoreTestDriver,
) error {
	var collection backupCollection
	var timestamp string

	numIncs := possibleNumIncrementalBackups[rng.Intn(len(possibleNumIncrementalBackups))]
	l.Printf("creating backup with %d incremental backups", numIncs)

	// Create full backup.
	l.Printf("creating full backup")
	if err := driver.runJobOnOneOf(ctx, l, nodes, func() error {
		var err error
		scope := driver.newBackupScope(rng)
		collection, _, err = driver.runBackup(ctx, l, rng, nodes, pauseProbability, fullBackup{name: "round-trip-test-backup", scope: scope})
		return err
	}); err != nil {
		return err
	}

	// Create incremental backups.
	for incNum := 0; incNum < numIncs; incNum++ {
		l.Printf("creating incremental backup number %d", incNum+1)
		if err := driver.runJobOnOneOf(ctx, l, nodes, func() error {
			var err error
			collection, timestamp, err = driver.runBackup(ctx, l, rng, nodes, pauseProbability, incrementalBackup{collection: collection})
			return err
		}); err != nil {
			return err
		}
	}

	l.Printf("saving contents of backup for %s", collection.name)
	if err := driver.saveContents(ctx, l, rng, &collection, timestamp); err != nil {
		return err
	}

	return nil
}

func runInBackground(ctx context.Context, fn func(context.Context) error) func() error {
	cmdDone := make(chan error, 1)
	ctx, cancel := context.WithCancel(ctx)

	go func() {
		cmdDone <- fn(ctx)
	}()

	cleanup := func() error {
		select {
		case err := <-cmdDone:
			return errors.Wrapf(err, "command prematurely exited")
		default:
		}
		cancel()
		return nil
	}

	return cleanup
}

// startBackgroundWorkloads starts a TPCC, bank, and a system table workload in
// the background.
func startBackgroundWorkloads(
	ctx context.Context,
	l *logger.Logger,
	c cluster.Cluster,
	testRNG *rand.Rand,
	workloadNode option.NodeListOption,
	driver *BackupRestoreTestDriver,
) (func() error, error) {
	// numWarehouses is picked as a number that provides enough work
	// for the cluster used in this test without overloading it,
	// which can make the backups take much longer to finish.
	const numWarehouses = 100
	tpccInit, tpccRun := driver.tpccWorkloadCmd(numWarehouses)
	bankInit, bankRun := driver.bankWorkloadCmd(testRNG)

	err := c.RunE(ctx, workloadNode, bankInit.String())
	if err != nil {
		return nil, err
	}

	err = c.RunE(ctx, workloadNode, tpccInit.String())
	if err != nil {
		return nil, err
	}

	stopBank := runInBackground(ctx, func(ctx context.Context) error {
		return c.RunE(ctx, workloadNode, bankRun.String())
	})
	stopTPCC := runInBackground(ctx, func(ctx context.Context) error {
		return c.RunE(ctx, workloadNode, tpccRun.String())
	})

	stopSystemWriter := runInBackground(ctx, func(ctx context.Context) error {
		return driver.systemTableWriter(ctx, l, testRNG)
	})

	stopBackgroundCommands := func() error {
		var stopErrors []error
		if err := stopBank(); err != nil {
			stopErrors = append(stopErrors, errors.Wrapf(err, "bank workload"))
		}

		if err := stopTPCC(); err != nil {
			stopErrors = append(stopErrors, errors.Wrapf(err, "tpcc workload"))
		}
		if err := stopSystemWriter(); err != nil {
			stopErrors = append(stopErrors, errors.Wrapf(err, "system writer workload"))
		}

		if len(stopErrors) > 0 {
			msgs := make([]string, 0, len(stopErrors))
			for i, stopErr := range stopErrors {
				msgs = append(msgs, fmt.Sprintf("%d: %s", i, stopErr.Error()))
			}

			return errors.Newf("%d errors while stopping background commands: %s", len(stopErrors), strings.Join(msgs, "\n"))
		}

		return nil
	}

	return stopBackgroundCommands, nil
}

// Connect makes a database handle to the node.
func (d *BackupRestoreTestDriver) Connect(node int) *gosql.DB {
	d.connCache.mu.Lock()
	defer d.connCache.mu.Unlock()
	return d.connCache.cache[node-1]
}

// RandomNode returns a random nodeID in the cluster.
func (d *BackupRestoreTestDriver) RandomNode(rng *rand.Rand, nodes option.NodeListOption) int {
	return nodes[rng.Intn(len(nodes))]
}

func (d *BackupRestoreTestDriver) RandomDB(
	rng *rand.Rand, nodes option.NodeListOption,
) (int, *gosql.DB) {
	node := d.RandomNode(rng, nodes)
	return node, d.Connect(node)
}

func (d *BackupRestoreTestDriver) Exec(rng *rand.Rand, query string, args ...interface{}) error {
	_, db := d.RandomDB(rng, d.roachNodes)
	// TODO: add ctx to driver
	_, err := db.ExecContext(d.ctx, query, args...)
	return err
}

// QueryRow executes a query that is expected to return at most one row on a
// random node.
func (d *BackupRestoreTestDriver) QueryRow(
	rng *rand.Rand, query string, args ...interface{},
) *gosql.Row {
	_, db := d.RandomDB(rng, d.roachNodes)
	return db.QueryRowContext(d.ctx, query, args...)
}

func (d *BackupRestoreTestDriver) now() string {
	return hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}.AsOfSystemTime()
}
