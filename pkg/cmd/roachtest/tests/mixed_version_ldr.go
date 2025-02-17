// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/mixedversion"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/task"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/replicationtestutils"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

func registerLDRMixedVersions(r registry.Registry) {

	sp := multiClusterSpec{
		leftNodes:  4,
		rightNodes: 4,
	}

	r.Add(registry.TestSpec{
		Name:             "ldr/mixed-version",
		Owner:            registry.OwnerDisasterRecovery,
		Cluster:          r.MakeClusterSpec(sp.leftNodes+sp.rightNodes+1, spec.WorkloadNode()),
		CompatibleClouds: registry.OnlyGCE,
		Suites:           registry.Suites(registry.MixedVersion, registry.Nightly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runLDRMixedVersions(ctx, t, c, sp)
		},
	})
}

func runLDRMixedVersions(ctx context.Context, t test.Test, c cluster.Cluster, sp multiClusterSpec) {
	lm := InitLDRMixed(ctx, t, c, sp)
	lm.SetupHook(ctx)
	lm.WorkloadHook(ctx)
	lm.LatencyHook(ctx)
	lm.UpdateHook(ctx)
	lm.Run(t)
}

func InitLDRMixed(
	ctx context.Context, t test.Test, c cluster.Cluster, sp multiClusterSpec,
) *ldrMixed {

	expectedMajorUpgrades := 1

	// TODO(msbutler): allow shared service mode once this test stabilizes.
	serviceMode := mixedversion.SystemOnlyDeployment

	leftMvt := mixedversion.NewTest(ctx, t, t.L(), c, sp.LeftNodesList(),
		mixedversion.AlwaysUseLatestPredecessors,
		mixedversion.EnabledDeploymentModes(serviceMode),
		mixedversion.NumUpgrades(expectedMajorUpgrades),
		mixedversion.WithTag("left"),
		mixedversion.WithSkipVersionProbability(0),
	)

	rightMvt := mixedversion.NewTest(ctx, t, t.L(), c, sp.RightNodesList(),
		mixedversion.AlwaysUseLatestPredecessors,
		mixedversion.EnabledDeploymentModes(serviceMode),
		mixedversion.NumUpgrades(expectedMajorUpgrades),
		mixedversion.WithTag("right"),
		mixedversion.WithSkipVersionProbability(0),
	)

	return &ldrMixed{
		leftMvt:         leftMvt,
		rightMvt:        rightMvt,
		leftStartedChan: make(chan struct{}),
		sp:              sp,
		t:               t,
		c:               c,
	}
}

func workloadInitCmd(nodes option.NodeListOption, initRows int) *roachtestutil.Command {
	return roachtestutil.NewCommand(`./cockroach workload init kv`).
		MaybeFlag(initRows > 0, "insert-count", initRows).
		// Only set the max block byte values for the init command if we
		// actually need to insert rows.
		MaybeFlag(initRows > 0, "max-block-bytes", 1024).
		MaybeFlag(initRows > 0, "splits", 100). // Ensure LDR stream with initial scan uses multiple source nodes.
		MaybeOption(initRows > 0, "scatter").
		Arg("{pgurl%s}", nodes).
		WithEqualsSyntax()
}

func workloadRunCmd(nodes option.NodeListOption) *roachtestutil.Command {
	return roachtestutil.NewCommand(`./cockroach workload run kv`).
		Option("tolerate-errors").
		Flag("read-percent", 0).
		Flag("max-rate", 100). // Set this very low to avoid overload.
		Arg("{pgurl%s}", nodes).
		WithEqualsSyntax()
}

type ldrMixed struct {
	leftMvt, rightMvt *mixedversion.Test

	// leftStartedSchan ensures the left cluster is started before the
	// right cluster is started. The left must be created before the right
	// due to a limitation in roachprod #129318.
	leftStartedChan chan struct{}

	sp multiClusterSpec
	t  test.Test
	c  cluster.Cluster
	// midUpgradeCatchupMu _attempts_ to prevent the source from upgrading while
	// the destination is waiting for the stream to catch up in some mixed version
	// state.
	midUpgradeCatchupMu syncutil.Mutex

	leftJobID            int
	rightJobID           int
	leftWorkloadStopper  mixedversion.StopFunc
	rightWorkloadStopper mixedversion.StopFunc
}

func (lm *ldrMixed) commonSetup(
	ctx context.Context,
	l *logger.Logger,
	r *rand.Rand,
	h *mixedversion.Helper,
	initCmd *roachtestutil.Command,
) (string, error) {
	l.Printf("enabling rangefeeds")
	if err := h.System.Exec(r, "SET CLUSTER SETTING kv.rangefeed.enabled = true"); err != nil {
		return "", errors.Wrap(err, "failed to enable rangefeeds")
	}

	l.Printf("init tables")
	if err := lm.c.RunE(ctx, option.WithNodes(lm.c.WorkloadNode()), initCmd.String()); err != nil {
		return "", errors.Wrap(err, "failed to init workload")
	}

	node, _ := h.RandomDB(r)
	settings := install.MakeClusterSettings()
	addr, err := lm.c.ExternalPGUrl(ctx, l, lm.c.Node(node), roachprod.PGURLOptions{
		VirtualClusterName: h.DefaultService().Descriptor.Name,
	})
	if err != nil {
		return "", err
	}

	pgURL, err := copyPGCertsAndMakeURL(ctx, lm.t, lm.c, lm.c.Node(node), settings.PGUrlCertsDir, addr[0])
	if err != nil {
		return "", err
	}
	return pgURL.String(), nil
}

var ldrCmd = "CREATE LOGICAL REPLICATION STREAM FROM TABLE kv.kv ON $1 INTO TABLE kv.kv"
var externalConnCmd = "CREATE EXTERNAL CONNECTION IF NOT EXISTS '%s' AS '%s'"

// The following should happen before upgrades:
// - Create tables, get pgurl on both left and right side.
// - Import some data on left.
// - Begin stream from right to left (empty init scan)
// - Wait for this initial scan to complete
// - Begin stream from left to right (with init scan)
// - Wait for this initial scan to complete
func (lm *ldrMixed) SetupHook(ctx context.Context) {
	rightPGURLChan := make(chan string)
	leftPGURLChan := make(chan string)
	righInitialScanComplete := make(chan struct{})

	lm.leftMvt.OnStartup("setup",
		func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
			close(lm.leftStartedChan)

			initCmd := workloadInitCmd(lm.sp.LeftNodesList(), 1000)
			leftPGURL, err := lm.commonSetup(ctx, l, r, h, initCmd)
			if err != nil {
				return err
			}
			var rightPGURL string
			select {
			case rightPGURL = <-rightPGURLChan:
			case <-ctx.Done():
				return ctx.Err()
			}
			if err := h.Exec(r, fmt.Sprintf(externalConnCmd, rightExternalConn.Host, rightPGURL)); err != nil {
				return err
			}
			l.Printf("setting up stream from right to left (no initial scan)")
			if err := h.QueryRow(r, ldrCmd, rightExternalConn.String()).Scan(&lm.leftJobID); err != nil {
				return err
			}
			if err := lm.WaitForReplicatedTime(ctx, timeutil.Now(), h, r, 5*time.Minute, lm.leftJobID); err != nil {
				return err
			}

			l.Printf("empty init scan complete. sending pgurl to right")
			leftPGURLChan <- leftPGURL

			l.Printf("waiting for reverse stream to complete initial scan")
			chanReadCtx(ctx, righInitialScanComplete)
			l.Printf("done")
			return nil
		},
	)

	lm.rightMvt.OnStartup("setup",
		func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
			initCmd := workloadInitCmd(lm.sp.RightNodesList(), 0)
			rightPGURL, err := lm.commonSetup(ctx, l, r, h, initCmd)
			if err != nil {
				return err
			}
			rightPGURLChan <- rightPGURL

			var leftPGURL string
			select {
			case leftPGURL = <-leftPGURLChan:
			case <-ctx.Done():
				return ctx.Err()
			}

			if err := h.Exec(r, fmt.Sprintf(externalConnCmd, leftExternalConn.Host, leftPGURL)); err != nil {
				return err
			}

			l.Printf("setting stream from left to right (with initial scan)")
			if err := h.QueryRow(r, ldrCmd, leftExternalConn.String()).Scan(&lm.rightJobID); err != nil {
				return err
			}
			if err := lm.WaitForReplicatedTime(ctx, timeutil.Now(), h, r, 10*time.Minute, lm.rightJobID); err != nil {
				return err
			}
			close(righInitialScanComplete)
			return nil

		})
}

func (lm *ldrMixed) WorkloadHook(ctx context.Context) {
	leftWorkloadCmd := workloadRunCmd(lm.sp.LeftNodesList())
	lm.leftWorkloadStopper = lm.leftMvt.Workload("kv", lm.c.WorkloadNode(), nil, leftWorkloadCmd)

	rightWorkloadCmd := workloadRunCmd(lm.sp.RightNodesList())
	lm.rightWorkloadStopper = lm.rightMvt.Workload("kv", lm.c.WorkloadNode(), nil, rightWorkloadCmd)
}

func (lm *ldrMixed) LatencyHook(ctx context.Context) {
	lm.leftMvt.BackgroundFunc("latency verifier", func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
		return runLatencyVerifier(ctx, l, r, h, lm.leftJobID)
	})
	lm.rightMvt.BackgroundFunc("latency verifier", func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
		return runLatencyVerifier(ctx, l, r, h, lm.rightJobID)
	})
}

func runLatencyVerifier(
	ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper, jobID int,
) error {
	lv := makeLatencyVerifier("ldr", 0, 0, l,
		getLogicalDataReplicationJobInfo, func(args ...interface{}) { l.Printf(fmt.Sprintln(args...)) }, true)
	defer lv.maybeLogLatencyHist()
	_, db := h.RandomDB(r)

	// The latency verify doesn't need a stopper, as ctx cancellation will stop it.
	dummyCh := make(chan struct{})
	if err := lv.pollLatencyUntilJobSucceeds(ctx, db, jobID, time.Second*5, dummyCh); ctx.Err() == nil {
		// The ctx is cancelled when the background func is successfully stopped,
		// therefore, don't return a context cancellation error.
		return errors.Wrapf(err, "latency verifier failed")
	}
	return nil
}

// UpdateHook registers a few mixed version hooks that conduct the following:
// - during random right side mixed version states, the upgrade processes
// will wait for the LDR replicated time to catch up, validating the stream can
// advance in a mixed version state.
// - After both clusters have upgraded to their final version, they will cut the
// workload, fingerprint the replicating table and assert fingerprint equality.
func (lm *ldrMixed) UpdateHook(ctx context.Context) {

	lm.rightMvt.InMixedVersion("maybe wait for replicated time",
		func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
			if r.Intn(3) == 0 {
				lm.midUpgradeCatchupMu.Lock()
				defer lm.midUpgradeCatchupMu.Unlock()
				return lm.WaitForReplicatedTime(ctx, nowLess30Seconds(), h, r, 5*time.Minute, lm.rightJobID)
			}
			return nil
		})

	lm.leftMvt.InMixedVersion(
		"maybe stall upgrade until right catches up",
		func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
			// If we have to wait for the lock, that implies the right cluster is
			// waiting for the replication time to catch up in some mixed version
			// state.
			//
			// NB: this lock is a best effort attempt to pause the left side upgrade
			// process from ocurring while the right is waiting for the replication
			// time to catch up. This lock is best effort because the left could
			// acquire and release the lock right before the right acquires it,
			// allowing the left side upgrade step to proceed. Furthermore, the left
			// does not acquire this lock on every node restart step-- rather it is
			// acquired on each InMixedVersion call, which occurs up to 4 times for
			// each major upgrade.
			lm.midUpgradeCatchupMu.Lock()
			l.Printf("acquired mid upgrade lock") // nolint:deferunlockcheck
			lm.midUpgradeCatchupMu.Unlock()
			return nil
		},
	)

	workloadFinishedTimeCh := make(chan time.Time)
	rightFingerprintCh := make(chan string)

	lm.rightMvt.AfterUpgradeFinalized(
		"fingerprint",
		func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
			version := h.Context().ToVersion
			final := clusterupgrade.CurrentVersion()
			if version.Equal(final) {
				lm.leftWorkloadStopper()
				lm.rightWorkloadStopper()

				// Give the workload a moment to stop.
				time.Sleep(2 * time.Second)
				now := timeutil.Now()
				workloadFinishedTimeCh <- now
				if err := lm.WaitForReplicatedTime(ctx, now, h, r, 5*time.Minute, lm.rightJobID); err != nil {
					return err
				}
				l.Printf("computing fingerprint")
				rightFingerprint, err := ComputeFingerprint(ctx, r, h)
				if err != nil {
					return err
				}
				l.Printf("computed right fingerprint %s", rightFingerprint)
				rightFingerprintCh <- rightFingerprint

				return nil
			}
			return nil
		},
	)

	lm.leftMvt.AfterUpgradeFinalized(
		"fingerprint",
		func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
			version := h.Context().ToVersion
			final := clusterupgrade.CurrentVersion()
			if version.Equal(final) {
				var now time.Time
				select {
				case now = <-workloadFinishedTimeCh:
				case <-ctx.Done():
					return ctx.Err()
				}
				if err := lm.WaitForReplicatedTime(ctx, now, h, r, 5*time.Minute, lm.leftJobID); err != nil {
					return err
				}
				l.Printf("computing fingerprint")
				leftFingerprint, err := ComputeFingerprint(ctx, r, h)
				if err != nil {
					return err
				}
				l.Printf("computed left fingerprint %s", leftFingerprint)
				select {
				case rightFingerprint := <-rightFingerprintCh:
					if leftFingerprint != rightFingerprint {
						return errors.Newf("fingerprints do not match: left %s, right %s", leftFingerprint, rightFingerprint)
					}
				case <-ctx.Done():
					return ctx.Err()
				}
				return nil
			}
			return nil
		},
	)
}

func ComputeFingerprint(ctx context.Context, r *rand.Rand, h *mixedversion.Helper) (string, error) {
	node, _ := h.RandomDB(r)
	db := h.Connect(node)
	if err := replicationtestutils.CheckEmptyDLQs(ctx, db, "kv"); err != nil {
		return "", err
	}
	return roachtestutil.Fingerprint(ctx, db, "kv", "kv")
}

func (lm *ldrMixed) Run(t task.Tasker) {
	var wg sync.WaitGroup
	wg.Add(2)

	t.Go(func(_ context.Context, l *logger.Logger) error {
		defer func() {
			if r := recover(); r != nil {
				l.Printf("left cluster upgrade failed: %v", r)
			}
		}()
		defer wg.Done()
		lm.leftMvt.Run()
		return nil
	})

	t.Go(func(taskCtx context.Context, l *logger.Logger) error {
		defer func() {
			if r := recover(); r != nil {
				l.Printf("right cluster upgrade failed: %v", r)
			}
		}()
		defer wg.Done()
		chanReadCtx(taskCtx, lm.leftStartedChan)
		lm.rightMvt.Run()
		return nil
	})
	wg.Wait()
}

func (lm *ldrMixed) WaitForReplicatedTime(
	ctx context.Context,
	targetTime time.Time,
	h *mixedversion.Helper,
	r *rand.Rand,
	timeout time.Duration,
	jobID int,
) error {
	lm.t.L().Printf("waiting for replicated time to advance past %s", targetTime)
	return testutils.SucceedsWithinError(func() error {
		query := "SELECT replicated_time FROM [SHOW LOGICAL REPLICATION JOBS] WHERE job_id = $1"
		var replicatedTime gosql.NullTime
		_, db := h.RandomDB(r)
		if err := db.QueryRowContext(ctx, query, jobID).Scan(&replicatedTime); err != nil {
			return err
		}
		if !(replicatedTime.Valid && replicatedTime.Time.After(targetTime)) {
			return errors.Newf("replicated time %s not yet at %s", replicatedTime, targetTime)
		}
		lm.t.L().Printf("replicated time is now %s, past %s", replicatedTime, targetTime)

		return nil
	}, timeout)
}
