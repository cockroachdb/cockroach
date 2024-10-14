// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"math/rand"
	"net/url"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/mixedversion"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

func registerC2CMixedVersions(r registry.Registry) {

	sp := replicationSpec{
		srcNodes:                  4,
		dstNodes:                  4,
		timeout:                   30 * time.Minute,
		additionalDuration:        0 * time.Minute,
		cutover:                   30 * time.Second,
		skipNodeDistributionCheck: true,
		clouds:                    registry.AllClouds,
		suites:                    registry.Suites(registry.Nightly),
	}

	r.Add(registry.TestSpec{
		Name:             "c2c/mixed-version",
		Owner:            registry.OwnerDisasterRecovery,
		Cluster:          r.MakeClusterSpec(sp.dstNodes+sp.srcNodes+1, spec.WorkloadNode()),
		CompatibleClouds: sp.clouds,
		Suites:           registry.Suites(registry.Nightly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runC2CMixedVersions(ctx, t, c, sp)
		},
	})
}

const (
	expectedMajorUpgrades = 1
	destTenantName        = "dest"
	replicationJobType    = "REPLICATION STREAM INGESTION"
	fingerprintQuery      = `SELECT fingerprint FROM [SHOW EXPERIMENTAL_FINGERPRINTS FROM VIRTUAL CLUSTER $1 WITH START TIMESTAMP = '%s'] AS OF SYSTEM TIME '%s'`
)

func runC2CMixedVersions(ctx context.Context, t test.Test, c cluster.Cluster, sp replicationSpec) {
	cm := InitC2CMixed(ctx, t, c, sp)
	cm.SetupHook(ctx)
	cm.WorkloadHook(ctx)
	cm.LatencyHook(ctx)
	cm.UpdateHook(ctx)
	cm.Run(ctx, c)
}

func InitC2CMixed(
	ctx context.Context, t test.Test, c cluster.Cluster, sp replicationSpec,
) *c2cMixed {
	// TODO(msbutler): allow for version skipping and multiple upgrades.
	sourceMvt := mixedversion.NewTest(ctx, t, t.L(), c, c.Range(1, sp.srcNodes),
		mixedversion.AlwaysUseLatestPredecessors,
		mixedversion.NumUpgrades(expectedMajorUpgrades),
		mixedversion.EnabledDeploymentModes(mixedversion.SharedProcessDeployment),
		mixedversion.WithTag("source"),
		mixedversion.DisableSkipVersionUpgrades,
		mixedversion.EnableWaitForReplication, // see #130384
	)

	destMvt := mixedversion.NewTest(ctx, t, t.L(), c, c.Range(sp.srcNodes+1, sp.srcNodes+sp.dstNodes),
		mixedversion.AlwaysUseLatestPredecessors,
		mixedversion.NumUpgrades(expectedMajorUpgrades),
		mixedversion.EnabledDeploymentModes(mixedversion.SystemOnlyDeployment),
		mixedversion.WithTag("dest"),
		mixedversion.DisableSkipVersionUpgrades,
		mixedversion.EnableWaitForReplication, // see #130384
	)

	return &c2cMixed{
		sourceMvt:           sourceMvt,
		destMvt:             destMvt,
		sourceStartedChan:   make(chan struct{}),
		destStartedChan:     make(chan struct{}),
		sp:                  sp,
		t:                   t,
		c:                   c,
		fingerprintArgsChan: make(chan fingerprintArgs, 1),
		fingerprintChan:     make(chan int64, 1),
	}
}

type sourceTenantInfo struct {
	name  string
	pgurl *url.URL
}

type fingerprintArgs struct {
	retainedTime hlc.Timestamp
	cutoverTime  hlc.Timestamp
}

type c2cMixed struct {
	sourceMvt, destMvt *mixedversion.Test
	// sourceStartedChan ensures the source cluster is started before the
	// destination cluster is started. The source must be created before the dest
	// due to a limitation in roachprod #129318.
	sourceStartedChan chan struct{}
	// destStartedChan prevents the source from starting upgrading until PCR has
	// completed its initial scan during dest startup. In the future, we may relax
	// this guardrail.
	destStartedChan chan struct{}
	sp              replicationSpec
	t               test.Test
	c               cluster.Cluster
	// fingerprintArgsChan sends information from dest to source about the correct
	// arguments to use for fingerprinting. This channel is buffered so the dest
	// can begin fingerprinting if the source is not ready to fingerprint.
	fingerprintArgsChan chan fingerprintArgs
	fingerprintChan     chan int64
	// midUpgradeCatchupMu _attempts_ to prevent the source from upgrading while
	// the destination is waiting for the stream to catch up in some mixed version
	// state.
	midUpgradeCatchupMu syncutil.Mutex

	ingestionJobID  catpb.JobID
	workloadStopper mixedversion.StopFunc
}

func (cm *c2cMixed) SetupHook(ctx context.Context) {
	// sourceInfoChan provides the destination with source cluster info generated
	// during source startup. The channel is buffered so the source runner can
	// buffer the information and proceed with the upgrade process even if the
	// destination is not ready to receive the information.
	sourceInfoChan := make(chan sourceTenantInfo, 1)

	cm.sourceMvt.OnStartup(
		"generate pgurl",
		func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
			// Enable rangefeeds, required for PCR to work.
			l.Printf("enabling rangefeeds")
			if err := h.System.Exec(r, "SET CLUSTER SETTING kv.rangefeed.enabled = true"); err != nil {
				return errors.Wrap(err, "failed to enable rangefeeds")
			}
			close(cm.sourceStartedChan)

			l.Printf("generating pgurl")
			srcNode := cm.c.Node(1)
			srcClusterSetting := install.MakeClusterSettings()
			addr, err := cm.c.ExternalPGUrl(ctx, l, srcNode, roachprod.PGURLOptions{
				VirtualClusterName: install.SystemInterfaceName,
			})
			if err != nil {
				return err
			}

			pgURL, err := copyPGCertsAndMakeURL(ctx, cm.t, cm.c, srcNode, srcClusterSetting.PGUrlCertsDir, addr[0])
			if err != nil {
				return err
			}

			sourceInfoChan <- sourceTenantInfo{name: h.Tenant.Descriptor.Name, pgurl: pgURL}

			// TODO(msbutler): once we allow upgrades during initial scan, remove the
			// destStartedChan.
			l.Printf("waiting for destination tenant to be created and replication stream to begin")
			chanReadCtx(ctx, cm.destStartedChan)
			l.Printf("done")

			return nil
		},
	)

	cm.destMvt.OnStartup("create destination tenant on standby",
		func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
			l.Printf("waiting to hear from source cluster")
			sourceInfo := chanReadCtx(ctx, sourceInfoChan)

			if err := h.Exec(r, fmt.Sprintf(
				"CREATE TENANT %q FROM REPLICATION OF %q ON $1",
				destTenantName, sourceInfo.name,
			), sourceInfo.pgurl.String()); err != nil {
				return errors.Wrap(err, "creating destination tenant")
			}

			if err := h.QueryRow(r,
				"SELECT job_id FROM [SHOW JOBS] WHERE job_type = $1",
				replicationJobType,
			).Scan(&cm.ingestionJobID); err != nil {
				return errors.Wrap(err, "querying ingestion job ID")
			}

			l.Printf("replication job: %d. Let initial scan complete", cm.ingestionJobID)
			// TODO(msbutler): relax requirement that initial scan completes before upgrades.
			if err := cm.WaitForReplicatedTime(ctx, timeutil.Now(), h, r, 5*time.Minute); err != nil {
				return err
			}
			close(cm.destStartedChan)
			return nil
		})
}

func (cm *c2cMixed) WorkloadHook(ctx context.Context) {
	tpccInitCmd := roachtestutil.NewCommand("./cockroach workload init tpcc").
		Arg("{pgurl%s}", cm.c.Range(1, cm.sp.srcNodes)).
		Flag("warehouses", 10)
	tpccRunCmd := roachtestutil.NewCommand("./cockroach workload run tpcc").
		Arg("{pgurl%s}", cm.c.Range(1, cm.sp.srcNodes)).
		Option("tolerate-errors").
		Flag("warehouses", 500)
	cm.workloadStopper = cm.sourceMvt.Workload("tpcc", cm.c.WorkloadNode(), tpccInitCmd, tpccRunCmd)
}

func (cm *c2cMixed) LatencyHook(ctx context.Context) {
	cm.destMvt.BackgroundFunc("latency verifier", func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
		lv := makeLatencyVerifier("stream-ingestion", 0, cm.sp.maxAcceptedLatency, l,
			getStreamIngestionJobInfo, func(args ...interface{}) { l.Printf(fmt.Sprintln(args...)) }, true)
		defer lv.maybeLogLatencyHist()
		_, db := h.RandomDB(r)

		// The latency verify doesn't need a stopper, as ctx cancellation will stop it.
		dummyCh := make(chan struct{})
		if err := lv.pollLatencyUntilJobSucceeds(ctx, db, int(cm.ingestionJobID), time.Second*5, dummyCh); ctx.Err() == nil {
			// The ctx is cancelled when the background func is successfully stopped,
			// therefore, don't return a context cancellation error.
			return errors.Wrapf(err, "latency verifier failed")
		}
		return nil
	})
}

// UpdateHook registers a few mixed version hooks that ensure that the upgrading
// clusters obey several invariants, which include:
// - the destination cluster must be the same or at most one major version ahead
// of the source cluster. This implies that for a given major upgrade, the
// destination cluster finalizes before the source cluster.
// - the app tenant must finalize after the system tenant (baked into the mixed
// version framework).
//
// The hooks also conduct the following:
// - during random destination side mixed version states, the upgrade processes
// will wait for the PCR replicated time to catch up, validating the stream can
// advance in a mixed version state.
// - After the destination has upgraded to its final version, it will issue a
// cutover command and fingerprint the app tenant key space.
// - The source will also run a fingerprint command using the same timestamps
// and ensure the fingerprints are the same.
func (cm *c2cMixed) UpdateHook(ctx context.Context) {
	destFinalized := make(chan struct{}, 1)

	cm.destMvt.InMixedVersion("maybe wait for replicated time",
		func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
			if r.Intn(3) == 0 {
				cm.midUpgradeCatchupMu.Lock()
				defer cm.midUpgradeCatchupMu.Unlock()
				return cm.WaitForReplicatedTime(ctx, nowLess30Seconds(), h, r, 5*time.Minute)
			}
			return nil
		})

	cm.sourceMvt.InMixedVersion(
		"wait for dest to finalize if source is ready to finalize upgrade",
		func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {

			// If we have to wait for the lock, that implies the destination is
			// waiting for the replication time to catch up in some mixed version
			// state.
			//
			// NB: this lock is a best effort attempt to pause the source side upgrade
			// process from ocurring while the dest is waiting for the replication
			// time to catch up. Specifically, when the source side is restarting
			// nodes, it prevents the replication stream from advancing. This lock is
			// best effort because the source could acquire and release the lock right
			// before the dest acquires it, allowing the source side upgrade step to
			// proceed. Furthermore, the source does not acquire this lock on every
			// node restart step-- rather it is acquired on each InMixedVersion call,
			// which occurs up to 4 times for each major upgrade.
			cm.midUpgradeCatchupMu.Lock()
			l.Printf("acquired mid upgrade lock") // nolint:deferunlockcheck
			cm.midUpgradeCatchupMu.Unlock()
			if h.Context().Stage == mixedversion.LastUpgradeStage {
				l.Printf("waiting for destination cluster to finalize upgrade")
				chanReadCtx(ctx, destFinalized)
			} else {
				l.Printf("no need to wait for dest: not ready to finalize")
			}
			return nil
		},
	)

	destMajorUpgradeCount := 0
	cm.destMvt.AfterUpgradeFinalized(
		"cutover and allow source to finalize",
		func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
			// Ensure the source always waits to finalize until after the dest finalizes.
			// NB: the cutover may happen while the source is still in some mixed version state.
			destFinalized <- struct{}{}
			destMajorUpgradeCount++
			if destMajorUpgradeCount == expectedMajorUpgrades {
				return cm.destCutoverAndFingerprint(ctx, l, r, h)
			}
			return nil
		},
	)

	sourceMajorUpgradeCount := 0
	cm.sourceMvt.AfterUpgradeFinalized(
		"fingerprint source",
		func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
			sourceMajorUpgradeCount++
			if sourceMajorUpgradeCount == expectedMajorUpgrades {
				return cm.sourceFingerprintAndCompare(ctx, l, r, h)
			}
			return nil
		},
	)
}

func (cm *c2cMixed) destCutoverAndFingerprint(
	ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper,
) error {

	// Wait for destination to catch up before cutover.
	//
	// TODO(msbutler): test cutting over to a time when the app tenant is a
	// different version.
	if err := cm.WaitForReplicatedTime(ctx, nowLess30Seconds(), h, r, 5*time.Minute); err != nil {
		return err
	}

	var retainedTime time.Time
	if err := h.QueryRow(r,
		`SELECT retained_time FROM [SHOW TENANT $1 WITH REPLICATION STATUS]`, destTenantName).Scan(&retainedTime); err != nil {
		return err
	}
	retainedHLCTime := hlc.Timestamp{WallTime: retainedTime.UnixNano()}

	var cutoverStr string
	if err := h.QueryRow(r, "ALTER TENANT $1 COMPLETE REPLICATION TO LATEST", destTenantName).Scan(&cutoverStr); err != nil {
		return err
	}
	cutover, err := hlc.ParseHLC(cutoverStr)
	if err != nil {
		return err
	}
	_, db := h.RandomDB(r)
	if err := WaitForSucceeded(ctx, db, cm.ingestionJobID, time.Minute); err != nil {
		return err
	}

	l.Printf("Retained time %s; cutover time %s", retainedHLCTime.GoTime(), cutover.GoTime())
	// The fingerprint args are sent over to the source before the dest begins
	// fingerprinting merely so both clusters can run the fingerprint commands in
	// parallel.
	cm.fingerprintArgsChan <- fingerprintArgs{
		retainedTime: retainedHLCTime,
		cutoverTime:  cutover,
	}
	var destFingerprint int64
	if err := h.QueryRow(r,
		fmt.Sprintf(fingerprintQuery, retainedHLCTime.AsOfSystemTime(), cutover.AsOfSystemTime()),
		destTenantName,
	).Scan(&destFingerprint); err != nil {
		return err
	}
	cm.fingerprintChan <- destFingerprint
	// TODO(msbutler): we could spin up the workload for a bit on the destination,
	// just to check that it works after cutover.
	return nil
}

func (cm *c2cMixed) sourceFingerprintAndCompare(
	ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper,
) error {
	args := chanReadCtx(ctx, cm.fingerprintArgsChan)
	cm.workloadStopper()
	var sourceFingerprint int64
	if err := h.System.QueryRow(r,
		fmt.Sprintf(fingerprintQuery, args.retainedTime.AsOfSystemTime(), args.cutoverTime.AsOfSystemTime()),
		h.Tenant.Descriptor.Name,
	).Scan(&sourceFingerprint); err != nil {
		return err
	}

	destFingerprint := chanReadCtx(ctx, cm.fingerprintChan)
	if sourceFingerprint != destFingerprint {
		return errors.Newf("source fingerprint %d does not match dest fingerprint %d", sourceFingerprint, destFingerprint)
	}
	return nil
}

func (cm *c2cMixed) Run(ctx context.Context, c cluster.Cluster) {
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				cm.t.L().Printf("source cluster upgrade failed: %v", r)
			}
		}()
		defer wg.Done()
		cm.sourceMvt.Run()
	}()

	go func() {
		defer func() {
			if r := recover(); r != nil {
				cm.t.L().Printf("destination cluster upgrade failed: %v", r)
			}
		}()
		defer wg.Done()

		chanReadCtx(ctx, cm.sourceStartedChan)
		cm.destMvt.Run()
	}()

	wg.Wait()
}

func (cm *c2cMixed) WaitForReplicatedTime(
	ctx context.Context,
	targetTime time.Time,
	h *mixedversion.Helper,
	r *rand.Rand,
	timeout time.Duration,
) error {
	cm.t.L().Printf("waiting for replicated time to advance past %s", targetTime)
	return testutils.SucceedsWithinError(func() error {
		query := "SELECT replicated_time FROM [SHOW TENANT $1 WITH REPLICATION STATUS]"
		var replicatedTime time.Time
		_, db := h.RandomDB(r)
		if err := db.QueryRowContext(ctx, query, destTenantName).Scan(&replicatedTime); err != nil {
			return err
		}
		if !replicatedTime.After(targetTime) {
			return errors.Newf("replicated time %s not yet at %s", replicatedTime, targetTime)
		}
		cm.t.L().Printf("replicated time is now %s, past %s", replicatedTime, targetTime)
		return nil
	}, timeout)
}

func nowLess30Seconds() time.Time {
	return timeutil.Now().Add(-30 * time.Second)
}

func chanReadCtx[T any](ctx context.Context, ch <-chan T) T {
	select {
	case v := <-ch:
		return v
	case <-ctx.Done():
		var zero T
		return zero
	}
}
