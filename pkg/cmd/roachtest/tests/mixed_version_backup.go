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
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/mixedversion"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

const (
	// probability that we will attempt to restore a backup in
	// mixed-version state.
	mixedVersionRestoreProbability = 0.5

	// string label added to the names of backups taken while the cluster is
	// upgrading.
	finalizingLabel = "_finalizing"

	// These constants are used to define the scope of the cluster
	// settings used in this test.
	systemOnly       = "system-only"
	applicationLevel = "application-level"
)

var (
	invalidVersionRE = regexp.MustCompile(`[^a-zA-Z0-9\.]`)
)

func systemSetting(values ...string) metamorphicSetting {
	return metamorphicSetting{Scope: systemOnly, Values: values}
}

func tenantSetting(values ...string) metamorphicSetting {
	return metamorphicSetting{Scope: applicationLevel, Values: values}
}

func (s metamorphicSetting) IsSystemOnly() bool {
	return s.Scope == systemOnly
}

// sanitizeVersionForBackup takes the string representation of a
// version and removes any characters that would not be allowed in a
// backup destination.
func sanitizeVersionForBackup(v *clusterupgrade.Version) string {
	return invalidVersionRE.ReplaceAllString(v.String(), "")
}

// mixedVersionBackup is the struct that contains all the necessary
// state involved in the mixed-version backup test.
type mixedVersionBackup struct {
	cluster    cluster.Cluster
	t          test.Test
	roachNodes option.NodeListOption
	// backup collections that are created along the test
	collections []*backupCollection

	// databases where user data is being inserted
	dbs          []string
	tables       [][]string
	tablesLoaded *atomic.Bool

	// stopBackground can be called to stop any background functions
	// (including workloads) started in this test. Useful when restoring
	// cluster backups, as we don't want a stream of errors in the
	// these functions due to the nodes stopping.
	stopBackground mixedversion.StopFunc

	backupRestoreTestDriver *BackupRestoreTestDriver

	// commonTestUtils contains test utilities that can be shared between tests.
	// Do not use this field directly, use the CommonTestUtils method instead.
	commonTestUtils *CommonTestUtils
	utilsOnce       sync.Once
}

func newMixedVersionBackup(
	t test.Test, c cluster.Cluster, roachNodes option.NodeListOption, dbs []string,
) (*mixedVersionBackup, error) {
	var tablesLoaded atomic.Bool
	tablesLoaded.Store(false)

	return &mixedVersionBackup{
		t: t, cluster: c, roachNodes: roachNodes, tablesLoaded: &tablesLoaded, dbs: dbs,
	}, nil
}

func (mvb *mixedVersionBackup) initBackupRestoreTestDriver(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper,
) error {
	u, err := mvb.CommonTestUtils(ctx, h)
	if err != nil {
		return err
	}
	tables, err := u.loadTablesForDBs(ctx, l, rng, mvb.dbs...)
	if err != nil {
		return err
	}
	mvb.tables = tables
	mvb.tablesLoaded.Store(true)

	mvb.backupRestoreTestDriver, err = newBackupRestoreTestDriver(ctx, mvb.t, mvb.cluster, u, mvb.roachNodes, mvb.dbs, tables)
	if err != nil {
		return err
	}

	return nil
}

func (mvb *mixedVersionBackup) setShortJobIntervals(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper,
) error {
	u, err := mvb.CommonTestUtils(ctx, h)
	if err != nil {
		return err
	}
	return u.setShortJobIntervals(ctx, rng)
}

func (mvb *mixedVersionBackup) systemTableWriter(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper,
) error {
	for !mvb.tablesLoaded.Load() {
		l.Printf("waiting for user tables to be loaded...")
		time.Sleep(10 * time.Second)
	}
	l.Printf("user tables loaded, starting random inserts")

	u, err := mvb.CommonTestUtils(ctx, h)
	if err != nil {
		return err
	}

	return u.systemTableWriter(ctx, l, rng, mvb.dbs, mvb.backupRestoreTestDriver.tables)
}

func (mvb *mixedVersionBackup) setClusterSettings(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper,
) error {
	u, err := mvb.CommonTestUtils(ctx, h)
	if err != nil {
		return err
	}
	return u.setClusterSettings(ctx, l, mvb.cluster, rng)
}

// waitForDBs waits until every database in the `dbs` field
// exists. Useful in case a mixed-version hook is called concurrently
// with the process of actually creating the tables (e.g., workload
// initialization).
func (mvb *mixedVersionBackup) waitForDBs(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper,
) error {
	retryOptions := retry.Options{
		InitialBackoff: 10 * time.Second,
		MaxBackoff:     1 * time.Minute,
		Multiplier:     1.5,
		MaxRetries:     20,
	}

	for _, dbName := range mvb.dbs {
		r := retry.StartWithCtx(ctx, retryOptions)
		var err error
		for r.Next() {
			q := "SELECT 1 FROM [SHOW DATABASES] WHERE database_name = $1"
			var n int
			if err = h.QueryRow(rng, q, dbName).Scan(&n); err == nil {
				break
			}

			l.Printf("waiting for DB %s (err: %v)", dbName, err)
		}

		if err != nil {
			return fmt.Errorf("failed to wait for DB %s (last error: %w)", dbName, err)
		}

		// For bank and tpcc databases, also ensure at least one table exists.
		if dbName == "bank" || dbName == "tpcc" {
			r := retry.StartWithCtx(ctx, retryOptions)
			for r.Next() {
				q := fmt.Sprintf("SELECT count(*) FROM [SHOW TABLES FROM %s]", dbName)
				var tableCount int
				if err = h.QueryRow(rng, q).Scan(&tableCount); err == nil && tableCount > 0 {
					l.Printf("DB %s has %d table(s)", dbName, tableCount)
					break
				}

				l.Printf("waiting for tables in DB %s (count: %d, err: %v)", dbName, tableCount, err)
			}

			if err != nil {
				return fmt.Errorf("failed to wait for tables in DB %s (last error: %w)", dbName, err)
			}
		}
	}

	// After every database exists, wait for a small amount of time to
	// make sure *some* data exists (the workloads could be inserting
	// data concurrently).
	time.Sleep(1 * time.Minute)
	return nil
}

// maybeTakePreviousVersionBackup creates a backup collection (full +
// incremental), and is supposed to be called before any nodes are
// upgraded. This ensures that we are able to restore this backup
// later, when we are in mixed version, and also after the upgrade is
// finalized.
func (mvb *mixedVersionBackup) maybeTakePreviousVersionBackup(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper,
) error {
	// Wait here to allow the workloads (which are initializing
	// concurrently with this step) to store some data in the cluster by
	// the time the backup is taken.
	if err := mvb.waitForDBs(ctx, l, rng, h); err != nil {
		return err
	}

	if err := mvb.initBackupRestoreTestDriver(ctx, l, rng, h); err != nil {
		return err
	}

	previousVersion := h.Context().FromVersion
	label := fmt.Sprintf("before upgrade in %s", sanitizeVersionForBackup(previousVersion))
	allPrevVersionNodes := labeledNodes{Nodes: mvb.roachNodes, Version: previousVersion.String()}
	executeOnAllNodesSpec := backupSpec{PauseProbability: neverPause, Plan: allPrevVersionNodes, Execute: allPrevVersionNodes}
	return mvb.createBackupCollection(ctx, l, rng, executeOnAllNodesSpec, executeOnAllNodesSpec, h, label)
}

// backupNamePrefix returns a descriptive prefix for the name of a backup
// depending on the state of the test we are in. The given label is also used to
// provide more context. Example: '22.2.4-to-current_final'
func (mvb *mixedVersionBackup) backupNamePrefix(h *mixedversion.Helper, label string) string {
	var finalizing string
	if h.IsFinalizing() {
		finalizing = finalizingLabel
	}

	fromVersion := sanitizeVersionForBackup(h.Context().FromVersion)
	toVersion := sanitizeVersionForBackup(h.Context().ToVersion)
	sanitizedLabel := strings.ReplaceAll(label, " ", "-")

	return fmt.Sprintf(
		"%s-to-%s_%s%s",
		fromVersion, toVersion, sanitizedLabel, finalizing,
	)
}

func (mvb *mixedVersionBackup) createBackupCollection(
	ctx context.Context,
	l *logger.Logger,
	rng *rand.Rand,
	fullBackupSpec backupSpec,
	incBackupSpec backupSpec,
	h *mixedversion.Helper,
	labelOverride string,
) error {
	label := backupCollectionDesc(fullBackupSpec, incBackupSpec)
	if labelOverride != "" {
		label = labelOverride
	}
	backupNamePrefix := mvb.backupNamePrefix(h, label)
	n, db := h.System.RandomDB(rng)
	l.Printf("checking existence of crdb_internal.system_jobs via node %d", n)
	internalSystemJobs, err := hasInternalSystemJobs(ctx, l, rng, db)
	if err != nil {
		return err
	}

	collection, err := mvb.backupRestoreTestDriver.createBackupCollection(
		ctx, l, h, rng, fullBackupSpec, incBackupSpec, backupNamePrefix,
		internalSystemJobs, h.IsMultitenant(),
	)
	if err != nil {
		return err
	}

	mvb.collections = append(mvb.collections, collection)
	return nil
}

// planAndRunBackups is the function that can be passed to the
// mixed-version test's `InMixedVersion` function. If the cluster is
// in mixed-binary state, 3 (`numCollections`) backup collections are
// created (see possible setups in `collectionSpecs`). If all nodes
// are running the next version (which is different from the cluster
// version), then a backup collection is created in an arbitrary node.
func (mvb *mixedVersionBackup) planAndRunBackups(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper,
) error {
	upgradingService := h.DefaultService()
	if upgradingService.Stage == mixedversion.UpgradingSystemStage {
		upgradingService = h.System
	}

	onPrevious := labeledNodes{
		Nodes: upgradingService.NodesInPreviousVersion(), Version: sanitizeVersionForBackup(h.Context().FromVersion),
	}
	onNext := labeledNodes{
		Nodes: upgradingService.NodesInNextVersion(), Version: sanitizeVersionForBackup(h.Context().ToVersion),
	}
	onRandom := labeledNodes{Nodes: mvb.roachNodes, Version: "random node"}
	defaultPauseProbability := 0.2

	collectionSpecs := [][2]backupSpec{
		// Case 1: plan backups    -> previous node
		//         execute backups -> next node
		{
			{Plan: onPrevious, Execute: onNext, PauseProbability: defaultPauseProbability}, // full
			{Plan: onPrevious, Execute: onNext, PauseProbability: defaultPauseProbability}, // incremental
		},
		// Case 2: plan backups    -> next node
		//         execute backups -> previous node
		{
			{Plan: onNext, Execute: onPrevious, PauseProbability: defaultPauseProbability}, // full
			{Plan: onNext, Execute: onPrevious, PauseProbability: defaultPauseProbability}, // incremental
		},
		// Case 3: plan & execute full backup        -> previous node
		//         plan & execute incremental backup -> next node
		{
			{Plan: onPrevious, Execute: onPrevious, PauseProbability: defaultPauseProbability}, // full
			{Plan: onNext, Execute: onNext, PauseProbability: defaultPauseProbability},         // incremental
		},
		// Case 4: plan & execute full backup        -> next node
		//         plan & execute incremental backup -> previous node
		{
			{Plan: onNext, Execute: onNext, PauseProbability: defaultPauseProbability},         // full
			{Plan: onPrevious, Execute: onPrevious, PauseProbability: defaultPauseProbability}, // incremental
		},
		// Case 5: plan backups    -> random node
		//         execute backups -> random node (with pause + resume)
		{
			{Plan: onRandom, Execute: onRandom, PauseProbability: alwaysPause}, // full
			{Plan: onRandom, Execute: onRandom, PauseProbability: alwaysPause}, // incremental
		},
	}

	if h.Context().MixedBinary() {
		const numCollections = 2
		rng.Shuffle(len(collectionSpecs), func(i, j int) {
			collectionSpecs[i], collectionSpecs[j] = collectionSpecs[j], collectionSpecs[i]
		})

		for _, specPair := range collectionSpecs[:numCollections] {
			fullSpec, incSpec := specPair[0], specPair[1]
			l.Printf("planning backup: %s", backupCollectionDesc(fullSpec, incSpec))
			if err := mvb.createBackupCollection(ctx, l, rng, fullSpec, incSpec, h, ""); err != nil {
				return err
			}
		}
		return nil
	}

	l.Printf("all nodes running next version, running backup on arbitrary node")
	fullSpec := backupSpec{Plan: onNext, Execute: onNext, PauseProbability: defaultPauseProbability}
	incSpec := fullSpec
	return mvb.createBackupCollection(ctx, l, rng, fullSpec, incSpec, h, "")
}

// verifySomeBackups is supposed to be called in mixed-version
// state. It will randomly pick a sample of the backups taken by the
// test so far (according to `mixedVersionRestoreProbability`), and
// validate the restore.
func (mvb *mixedVersionBackup) verifySomeBackups(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper,
) error {
	var toBeRestored []*backupCollection
	for _, collection := range mvb.collections {
		if _, isCluster := collection.btype.(*clusterBackup); isCluster {
			continue
		}
		if rng.Float64() < mixedVersionRestoreProbability {
			toBeRestored = append(toBeRestored, collection)
		}
	}

	l.Printf("verifying %d out of %d backups in mixed version", len(toBeRestored), len(mvb.collections))
	checkFiles, err := supportsCheckFiles(rng, h)
	if err != nil {
		return err
	}
	if !checkFiles {
		l.Printf("skipping check_files as it is not supported")
	}

	n, db := h.System.RandomDB(rng)
	l.Printf("checking existence of crdb_internal.system_jobs via node %d", n)
	internalSystemJobs, err := hasInternalSystemJobs(ctx, l, rng, db)
	if err != nil {
		return err
	}

	for _, collection := range toBeRestored {
		l.Printf("mixed-version: verifying %s", collection.name)
		if err := mvb.backupRestoreTestDriver.verifyBackupCollection(
			ctx, l, rng, collection, checkFiles, internalSystemJobs, h,
		); err != nil {
			return errors.Wrap(err, "mixed-version")
		}
	}

	return nil
}

// verifyAllBackups cycles through all the backup collections created
// for the duration of the test, and verifies that restoring the
// backups results in the same data as when the backup was taken. We
// attempt to restore all backups taken both in the previous version,
// as well as in the current (latest) version, returning all errors
// found in the process.
func (mvb *mixedVersionBackup) verifyAllBackups(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper,
) error {
	isFinalUpgrade := h.Context().ToVersion.IsCurrent()

	if isFinalUpgrade {
		l.Printf("stopping background functions and workloads")
		mvb.stopBackground()
	}

	u, err := mvb.CommonTestUtils(ctx, h)
	if err != nil {
		return err
	}

	var restoreErrors []error
	verify := func(v *clusterupgrade.Version) {
		l.Printf("%s: verifying %d collections created during this test", v, len(mvb.collections))

		for _, collection := range mvb.collections {
			if v.Equal(h.Context().FromVersion) && strings.Contains(collection.name, finalizingLabel) {
				// Do not attempt to restore, in the previous version, a
				// backup that was taken while the cluster was finalizing, as
				// that will most likely fail (the backup version will be past
				// the cluster version).
				continue
			}

			_, isClusterBackup := collection.btype.(*clusterBackup)
			if isClusterBackup && !isFinalUpgrade {
				// We only verify cluster backups once we upgraded all the way
				// to the final version in this test. Wiping and restarting
				// nodes does not work well with the mixedversion framework.
				continue
			}

			if isClusterBackup {
				// The mixedversion framework uses the global test monitor, which
				// already handles marking node deaths as expected for simple restarts.
				expectDeaths := func(numNodes int) {}
				err := u.resetCluster(ctx, l, v, expectDeaths, []install.ClusterSettingOption{})
				if err != nil {
					err := errors.Wrapf(err, "%s", v)
					l.Printf("error resetting cluster: %v", err)
					restoreErrors = append(restoreErrors, err)
					continue
				}
			}

			checkFiles, err := supportsCheckFiles(rng, h)
			if err != nil {
				restoreErrors = append(restoreErrors, fmt.Errorf("error checking for check_files support: %w", err))
				return
			}
			if !checkFiles {
				l.Printf("skipping check_files as it is not supported")
			}

			n, db := h.System.RandomDB(rng)
			l.Printf("checking existence of crdb_internal.system_jobs via node %d", n)
			internalSystemJobs, err := hasInternalSystemJobs(ctx, l, rng, db)
			if err != nil {
				restoreErrors = append(restoreErrors, fmt.Errorf("error checking for internal system jobs: %w", err))
				return
			}

			if err := mvb.backupRestoreTestDriver.verifyBackupCollection(
				ctx, l, rng, collection, checkFiles, internalSystemJobs, h,
			); err != nil {
				err := errors.Wrapf(err, "%s", v)
				l.Printf("restore error: %v", err)
				// Attempt to collect logs and debug.zip at the time of this
				// restore failure; if we can't, log the error encountered and
				// move on.
				restoreErr, collectionErr := u.collectFailureArtifacts(ctx, l, err, len(restoreErrors)+1)
				if collectionErr != nil {
					l.Printf("could not collect failure artifacts: %v", collectionErr)
				}
				restoreErrors = append(restoreErrors, restoreErr)
			}
		}
	}

	if h.Context().FromVersion.AtLeast(mixedversion.OldestSupportedVersion) {
		verify(h.Context().FromVersion)
	}
	verify(h.Context().ToVersion)

	// If the context was canceled (most likely due to a test timeout),
	// return early. In these cases, it's likely that `restoreErrors`
	// will have a number of "restore failures" that all happened
	// because the underlying context was canceled, so proceeding with
	// the error reporting logic below is confusing, as it makes it look
	// like multiple failures occurred. It also makes the actually
	// important "timed out" message less prominent.
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

	// Reset collections -- if this test run is performing multiple
	// upgrades, we just want to test restores from the previous version
	// to the current one.
	//
	// TODO(renato): it would be nice if this automatically followed
	// `binaryMinSupportedVersion` instead.
	mvb.collections = nil
	return nil
}

func registerBackupMixedVersion(r registry.Registry) {
	// backup/mixed-version tests different states of backup in a mixed
	// version cluster. The actual state of the cluster when a backup is
	// executed is randomized, so each run of the test will exercise a
	// different set of events. Reusing the same seed will produce the
	// same test.
	r.Add(registry.TestSpec{
		Name:              "backup-restore/mixed-version",
		Timeout:           8 * time.Hour,
		Owner:             registry.OwnerDisasterRecovery,
		Cluster:           r.MakeClusterSpec(5, spec.WorkloadNode()),
		EncryptionSupport: registry.EncryptionMetamorphic,
		NativeLibs:        registry.LibGEOS,
		// Uses gs://cockroach-fixtures-us-east1. See:
		// https://github.com/cockroachdb/cockroach/issues/105968
		CompatibleClouds:          registry.Clouds(spec.GCE, spec.Local),
		Suites:                    registry.Suites(registry.MixedVersion, registry.Nightly),
		TestSelectionOptOutSuites: registry.Suites(registry.Nightly),
		Monitor:                   true,
		Randomized:                true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			enabledDeploymentModes := []mixedversion.DeploymentMode{
				mixedversion.SystemOnlyDeployment,
				mixedversion.SharedProcessDeployment,
			}
			// Separate process deployments do not have node local storage.
			if !c.IsLocal() {
				enabledDeploymentModes = append(enabledDeploymentModes, mixedversion.SeparateProcessDeployment)
			}

			mvt := mixedversion.NewTest(
				ctx, t, t.L(), c, c.CRDBNodes(),
				// We use a longer upgrade timeout in this test to give the
				// migrations enough time to finish considering all the data
				// that might exist in the cluster by the time the upgrade is
				// attempted.
				mixedversion.WithWorkloadNodes(c.WorkloadNode()),
				mixedversion.UpgradeTimeout(30*time.Minute),
				mixedversion.AlwaysUseLatestPredecessors,
				mixedversion.EnabledDeploymentModes(enabledDeploymentModes...),
				// We disable cluster setting mutators because this test
				// resets the cluster to older versions when verifying cluster
				// backups. This makes the mixed-version context inaccurate
				// and leads to flakes.
				//
				// TODO(renato): don't disable these mutators when the
				// framework exposes some utility to provide mutual exclusion
				// of concurrent steps.
				mixedversion.DisableAllClusterSettingMutators(),
			)
			testRNG := mvt.RNG()

			// Workload can only take a positive int as a seed, but seed could be a
			// negative int. Ensure the seed passed to workload is an int.
			workloadSeed := testRNG.Int63()

			dbs := []string{"bank", "tpcc"}
			backupTest, err := newMixedVersionBackup(t, c, c.CRDBNodes(), dbs)
			if err != nil {
				t.Fatal(err)
			}

			defer func() {
				err := backupTest.cleanUp(ctx)
				if err != nil {
					t.L().Printf("encountered error while cleaning up: %v", err)
				}
			}()

			// numWarehouses is picked as a number that provides enough work
			// for the cluster used in this test without overloading it,
			// which can make the backups take much longer to finish.
			const numWarehouses = 100
			bankInit, bankRun := bankWorkloadCmd(t.L(), testRNG, workloadSeed, c.CRDBNodes(), false)
			tpccInit, tpccRun := tpccWorkloadCmd(t.L(), testRNG, workloadSeed, numWarehouses, c.CRDBNodes())

			mvt.OnStartup("set short job interval", backupTest.setShortJobIntervals)
			mvt.OnStartup("take backup in previous version", backupTest.maybeTakePreviousVersionBackup)
			mvt.OnStartup("maybe set custom cluster settings", backupTest.setClusterSettings)

			// We start two workloads in this test:
			// - bank: the main purpose of this workload is to test some
			//   edge cases, such as columns with small or large payloads,
			//   keys with long revision history, etc.
			// - tpcc: tpcc is a much more complex workload, and should keep
			//   the cluster relatively busy while the backup and restores
			//   take place. Its schema is also more complex, and the
			//   operations more closely resemble a customer workload.
			stopBank := mvt.Workload("bank", c.WorkloadNode(), bankInit, bankRun)
			stopTPCC := mvt.Workload("tpcc", c.WorkloadNode(), tpccInit, tpccRun)
			stopSystemWriter := mvt.BackgroundFunc("system table writer", backupTest.systemTableWriter)

			mvt.InMixedVersion("plan and run backups", backupTest.planAndRunBackups)
			mvt.InMixedVersion("verify some backups", backupTest.verifySomeBackups)
			mvt.AfterUpgradeFinalized("maybe verify all backups", backupTest.verifyAllBackups)

			backupTest.stopBackground = func() {
				stopBank()
				stopTPCC()
				stopSystemWriter()
			}
			mvt.Run()
		},
	})
}

func (mvb *mixedVersionBackup) CommonTestUtils(
	ctx context.Context, h *mixedversion.Helper,
) (*CommonTestUtils, error) {
	var err error
	mvb.utilsOnce.Do(func() {
		connectFunc := func(node int) (*gosql.DB, error) { return h.Connect(node), nil }
		mvb.commonTestUtils, err = newCommonTestUtils(
			ctx, mvb.t, mvb.cluster, connectFunc, mvb.roachNodes, mvb.roachNodes,
		)
	})
	return mvb.commonTestUtils, err
}

func (mvb *mixedVersionBackup) cleanUp(ctx context.Context) error {
	if mvb.commonTestUtils == nil {
		return nil
	}

	// The helper should not be necessary if we already set up a
	// `commonTestUtils`.
	u, err := mvb.CommonTestUtils(ctx, nil /* helper */)
	if err != nil {
		return err
	}

	u.CloseConnections()
	return nil
}
