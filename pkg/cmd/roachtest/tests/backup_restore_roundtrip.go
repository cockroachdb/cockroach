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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/mixedversion"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/version"
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

	h, err := newBackupTestHelper(ctx, t.L(), c, roachNodes, m)
	if err != nil {
		t.Fatal(err)
	}
	defer h.closeConnections()

	m.Go(func(ctx context.Context) error {
		// Use an instance of mixedVersionBackup for its backup related machinery.
		mvb := newMixedVersionBackup(t, c, roachNodes, "bank", "tpcc")
		stopBackgroundCommands, err := startBackgroundWorkloads(ctx, t.L(), c, testRNG, roachNodes, workloadNode, mvb, h)
		if err != nil {
			return err
		}

		if err := mvb.loadTables(ctx, t.L(), testRNG, h); err != nil {
			return err
		}

		if err := mvb.setShortJobIntervals(ctx, t.L(), testRNG, h); err != nil {
			return err
		}
		if err := mvb.setClusterSettings(ctx, t.L(), testRNG, h); err != nil {
			return err
		}

		// Run backups.
		if err := runBackupsAndSaveContents(ctx, t.L(), testRNG, roachNodes, pauseProbability, h, mvb); err != nil {
			return err
		}

		if err := stopBackgroundCommands(); err != nil {
			return err
		}

		// Verify content in backups.
		if err := verifyBackupContents(ctx, t.L(), testRNG, h, mvb); err != nil {
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
	h mixedversion.CommonTestHelper,
	mvb *mixedVersionBackup,
) error {
	var collection backupCollection
	var timestamp string

	numIncs := possibleNumIncrementalBackups[rng.Intn(len(possibleNumIncrementalBackups))]
	l.Printf("creating backup with %d incremental backups", numIncs)

	// Create full backup.
	l.Printf("creating full backup")
	if err := mvb.runJobOnOneOf(ctx, l, nodes, h, func() error {
		var err error
		collection, _, err = runBackup(ctx, l, rng, nodes, pauseProbability, h, fullBackup{}, mvb)
		return err
	}); err != nil {
		return err
	}

	// Create incremental backups.
	for incNum := 0; incNum < numIncs; incNum++ {
		l.Printf("creating incremental backup number %d", incNum+1)
		if err := mvb.runJobOnOneOf(ctx, l, nodes, h, func() error {
			var err error
			collection, timestamp, err = runBackup(ctx, l, rng, nodes, pauseProbability, h, incrementalBackup{collection: collection}, mvb)
			return err
		}); err != nil {
			return err
		}
	}

	l.Printf("saving contents of backup for %s", collection.name)
	if err := mvb.saveContents(ctx, l, rng, &collection, timestamp, h); err != nil {
		return err
	}

	return nil
}

func runBackup(
	ctx context.Context,
	l *logger.Logger,
	rng *rand.Rand,
	nodes option.NodeListOption,
	pauseProbability float64,
	h mixedversion.CommonTestHelper,
	bType fmt.Stringer,
	mvb *mixedVersionBackup,
) (backupCollection, string, error) {
	pauseAfter := 1024 * time.Hour // infinity
	var pauseResumeDB *gosql.DB
	if rng.Float64() < pauseProbability {
		possibleDurations := []time.Duration{
			10 * time.Second, 30 * time.Second, 2 * time.Minute,
		}
		pauseAfter = possibleDurations[rng.Intn(len(possibleDurations))]

		var node int
		node, pauseResumeDB = h.RandomDB(rng, nodes)
		l.Printf("attempting pauses in %s through node %d", pauseAfter, node)
	}

	var collection backupCollection
	var latest string

	switch b := bType.(type) {
	case fullBackup:
		btype := mvb.newBackupType(rng, h)
		name := "round_trip_backup"
		createOptions := newBackupOptions(rng)
		collection = newBackupCollection(name, btype, createOptions)
		l.Printf("creating full backup for %s", collection.name)
	case incrementalBackup:
		collection = b.collection
		latest = " LATEST IN"
		l.Printf("creating incremental backup for %s", b.collection.name)
	}

	// NB: we need to run with the `detached` option + poll the
	// `system.jobs` table because we are intentionally disabling job
	// adoption in some nodes in the mixed-version test. Running the job
	// without the `detached` option will cause a `node liveness` error
	// to be returned when running the `BACKUP` statement.
	options := []string{"detached"}

	for _, opt := range collection.options {
		options = append(options, opt.String())
	}

	backupTime := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}.AsOfSystemTime()
	node, db := h.RandomDB(rng, nodes)

	stmt := fmt.Sprintf(
		"%s INTO%s '%s' AS OF SYSTEM TIME '%s' WITH %s",
		collection.btype.BackupCommand(),
		latest,
		collection.uri(),
		backupTime,
		strings.Join(options, ", "),
	)
	l.Printf("creating backup via node %d: %s", node, stmt)
	var jobID int
	if err := db.QueryRowContext(ctx, stmt).Scan(&jobID); err != nil {
		return backupCollection{}, "", fmt.Errorf("error while creating backup %s: %w", collection.name, err)
	}

	backupErr := make(chan error)
	go func() {
		defer close(backupErr)
		l.Printf("waiting for job %d (%s)", jobID, collection.name)
		if err := mvb.waitForJobSuccess(ctx, l, rng, h, jobID); err != nil {
			backupErr <- err
		}
	}()

	var numPauses int
	for {
		select {
		case err := <-backupErr:
			if err != nil {
				return backupCollection{}, "", err
			}
			return collection, backupTime, nil

		case <-time.After(pauseAfter):
			if numPauses >= maxPauses {
				continue
			}

			pauseDur := 5 * time.Second
			l.Printf("pausing job %d for %s", jobID, pauseDur)
			if _, err := pauseResumeDB.Exec(fmt.Sprintf("PAUSE JOB %d", jobID)); err != nil {
				// We just log the error if pausing the job fails since we
				// cannot guarantee the job is still running by the time we
				// attempt to pause it. If that's the case, the next iteration
				// of the loop should read from the backupErr channel.
				l.Printf("error pausing job %d: %s", jobID, err)
				continue
			}

			time.Sleep(pauseDur)

			l.Printf("resuming job %d", jobID)
			if _, err := pauseResumeDB.Exec(fmt.Sprintf("RESUME JOB %d", jobID)); err != nil {
				return backupCollection{}, "", fmt.Errorf("error resuming job %d: %w", jobID, err)
			}

			numPauses++
		}
	}
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

type backupTestHelper struct {
	connCache struct {
		mu    syncutil.Mutex
		cache []*gosql.DB
	}
	nodes          option.NodeListOption
	ctx            context.Context
	m              cluster.Monitor
	clusterVersion *version.Version
}

var _ mixedversion.CommonTestHelper = &backupTestHelper{}

func newBackupTestHelper(
	ctx context.Context,
	l *logger.Logger,
	c cluster.Cluster,
	nodes option.NodeListOption,
	m cluster.Monitor,
) (*backupTestHelper, error) {
	cc := make([]*gosql.DB, len(nodes))
	for _, node := range nodes {
		conn, err := c.ConnE(ctx, l, node)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to node %d: %w", node, err)
		}

		cc[node-1] = conn
	}

	if len(cc) == 0 {
		return nil, errors.New("cannot test using 0 nodes")
	}

	v, err := clusterupgrade.ClusterVersion(ctx, cc[0])
	if err != nil {
		return nil, err
	}

	cv := version.MustParse(fmt.Sprintf("v%d.%d.%d", v.Major, v.Minor, v.Patch))

	h := &backupTestHelper{
		nodes:          nodes,
		m:              m,
		ctx:            ctx,
		clusterVersion: cv,
	}
	h.connCache.cache = cc

	return h, nil
}

func (b *backupTestHelper) Connect(node int) *gosql.DB {
	b.connCache.mu.Lock()
	defer b.connCache.mu.Unlock()
	return b.connCache.cache[node-1]
}

func (b *backupTestHelper) Exec(rng *rand.Rand, query string, args ...interface{}) error {
	_, db := b.RandomDB(rng, b.nodes)
	_, err := db.ExecContext(b.ctx, query, args...)
	return err
}

func (b *backupTestHelper) RandomNode(prng *rand.Rand, nodes option.NodeListOption) int {
	return nodes[prng.Intn(len(nodes))]
}

func (b *backupTestHelper) RandomDB(prng *rand.Rand, nodes option.NodeListOption) (int, *gosql.DB) {
	node := b.RandomNode(prng, nodes)
	return node, b.Connect(node)
}

func (b *backupTestHelper) QueryRow(rng *rand.Rand, query string, args ...interface{}) *gosql.Row {
	_, db := b.RandomDB(rng, b.nodes)
	return db.QueryRowContext(b.ctx, query, args...)
}

func (b *backupTestHelper) closeConnections() {
	b.connCache.mu.Lock()
	defer b.connCache.mu.Unlock()

	for _, db := range b.connCache.cache {
		if db != nil {
			_ = db.Close()
		}
	}
}

func (b *backupTestHelper) ExpectDeath() {
	b.m.ExpectDeath()
}

func (b *backupTestHelper) ExpectDeaths(n int) {
	b.m.ExpectDeaths(int32(n))
}

func (b *backupTestHelper) LowestBinaryVersion() *version.Version {
	return b.clusterVersion
}

// startBackgroundWorkloads starts a TPCC, bank, and a system table workload in
// the background.
func startBackgroundWorkloads(
	ctx context.Context,
	l *logger.Logger,
	c cluster.Cluster,
	testRNG *rand.Rand,
	roachNodes, workloadNode option.NodeListOption,
	mvb *mixedVersionBackup,
	h mixedversion.CommonTestHelper,
) (func() error, error) {
	// numWarehouses is picked as a number that provides enough work
	// for the cluster used in this test without overloading it,
	// which can make the backups take much longer to finish.
	// TODO: uncomment
	//const numWarehouses = 100
	const numWarehouses = 1

	tpccInit := roachtestutil.NewCommand("./cockroach workload init tpcc").
		Arg("{pgurl%s}", roachNodes).
		Flag("warehouses", numWarehouses)
	tpccRun := roachtestutil.NewCommand("./cockroach workload run tpcc").
		Arg("{pgurl%s}", roachNodes).
		Flag("warehouses", numWarehouses).
		Option("tolerate-errors")

	bankPayload := bankPossiblePayloadBytes[testRNG.Intn(len(bankPossiblePayloadBytes))]
	bankRows := bankPossibleRows[testRNG.Intn(len(bankPossibleRows))]

	for bankPayload == largeBankPayload && bankRows == fewBankRows {
		bankPayload = bankPossiblePayloadBytes[testRNG.Intn(len(bankPossiblePayloadBytes))]
		bankRows = bankPossibleRows[testRNG.Intn(len(bankPossibleRows))]
	}

	bankInit := roachtestutil.NewCommand("./cockroach workload init bank").
		Flag("rows", bankRows).
		MaybeFlag(bankPayload != 0, "payload-bytes", bankPayload).
		Flag("ranges", 0).
		Arg("{pgurl%s}", roachNodes)
	bankRun := roachtestutil.NewCommand("./cockroach workload run bank").
		Arg("{pgurl%s}", roachNodes).
		Option("tolerate-errors")

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
		return mvb.systemTableWriter(ctx, l, testRNG, h)
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

func verifyBackupContents(
	ctx context.Context,
	l *logger.Logger,
	rng *rand.Rand,
	h mixedversion.CommonTestHelper,
	mvb *mixedVersionBackup,
) error {
	var restoreErrors []error
	for _, collection := range mvb.collections {
		l.Printf("verifying backup collection %s", collection.name)
		if err := mvb.verifyBackupCollection(ctx, l, rng, h, collection, ""); err != nil {
			l.Printf("restore error: %v", err)
			// Attempt to collect logs and debug.zip at the time of this
			// restore failure; if we can't, log the error encountered and
			// move on.
			restoreErr, collectionErr := mvb.collectFailureArtifacts(ctx, l, err, len(restoreErrors)+1)
			if collectionErr != nil {
				l.Printf("could not collect failure artifacts: %v", collectionErr)
			}
			restoreErrors = append(restoreErrors, restoreErr)
		}
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	if len(restoreErrors) > 0 {
		if len(restoreErrors) == 1 {
			// Simplify error reporting if only one error was found.
			return restoreErrors[0]
		}

		msgs := make([]string, 0, len(restoreErrors))
		for j, err := range restoreErrors {
			msgs = append(msgs, fmt.Sprintf("%d: %s", j+1, err.Error()))
		}
		return fmt.Errorf("%d errors during restore:\n%s", len(restoreErrors), strings.Join(msgs, "\n"))
	}

	return nil
}
