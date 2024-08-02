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
	"fmt"
	"math/rand"
	"net/url"
	"sync"
	"time"

	apd "github.com/cockroachdb/apd/v3"
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
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

func registerC2CMixedVersions(r registry.Registry) {

	sp := replicationSpec{
		srcNodes:                  4,
		dstNodes:                  4,
		timeout:                   10 * time.Minute,
		additionalDuration:        0 * time.Minute,
		cutover:                   30 * time.Second,
		skipNodeDistributionCheck: true,
		clouds:                    registry.AllExceptAzure,
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
	minSupportedVersion   = "v23.2.0"
	replicationJobType    = "REPLICATION STREAM INGESTION"
	fingerprintQuery      = `SELECT fingerprint FROM [SHOW EXPERIMENTAL_FINGERPRINTS FROM VIRTUAL CLUSTER $1 WITH START TIMESTAMP = '%s'] AS OF SYSTEM TIME '%s'`
)

// TODO (msbutler): schedule upgrades during initial scan and cutover.
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
	sourceMvt := mixedversion.NewTest(ctx, t, t.L(), c, c.Range(1, sp.srcNodes),
		mixedversion.MinimumSupportedVersion(minSupportedVersion),
		mixedversion.AlwaysUseLatestPredecessors,
		mixedversion.NumUpgrades(expectedMajorUpgrades),
		mixedversion.EnabledDeploymentModes(mixedversion.SharedProcessDeployment),
		mixedversion.WithTag("source"),
		mixedversion.DisableSkipVersionUpgrades,
	)

	destMvt := mixedversion.NewTest(ctx, t, t.L(), c, c.Range(sp.srcNodes+1, sp.srcNodes+sp.dstNodes),
		mixedversion.MinimumSupportedVersion(minSupportedVersion),
		mixedversion.AlwaysUseLatestPredecessors,
		mixedversion.NumUpgrades(expectedMajorUpgrades),
		mixedversion.EnabledDeploymentModes(mixedversion.SystemOnlyDeployment),
		mixedversion.WithTag("dest"),
		mixedversion.DisableSkipVersionUpgrades,
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
	sourceMvt, destMvt  *mixedversion.Test
	sourceStartedChan   chan struct{}
	destStartedChan     chan struct{}
	sp                  replicationSpec
	t                   test.Test
	c                   cluster.Cluster
	fingerprintArgsChan chan fingerprintArgs
	fingerprintChan     chan int64

	startTime       hlc.Timestamp
	ingestionJobID  catpb.JobID
	workloadStopper mixedversion.StopFunc
}

func (cm *c2cMixed) SetupHook(ctx context.Context) {
	sourceChan := make(chan sourceTenantInfo, 1)

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

			sourceChan <- sourceTenantInfo{name: h.Tenant.Descriptor.Name, pgurl: pgURL}

			l.Printf("waiting for destination tenant to be created")
			chanReadCtx(ctx, cm.destStartedChan)
			l.Printf("done")

			return nil
		},
	)

	cm.destMvt.OnStartup("create destination tenant on standby",
		func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
			l.Printf("waiting to hear from source cluster")
			sourceInfo := chanReadCtx(ctx, sourceChan)

			if err := h.Exec(r, fmt.Sprintf(
				"CREATE TENANT %q FROM REPLICATION OF %q ON '%s'",
				destTenantName, sourceInfo.name, sourceInfo.pgurl.String(),
			)); err != nil {
				return errors.Wrap(err, "creating destination tenant")
			}

			if err := h.QueryRow(r, fmt.Sprintf(
				"SELECT job_id FROM [SHOW JOBS] WHERE job_type = '%s'",
				replicationJobType,
			)).Scan(&cm.ingestionJobID); err != nil {
				return errors.Wrap(err, "querying ingestion job ID")
			}

			l.Printf("replication job: %d", cm.ingestionJobID)
			// Debugging... let initial scan complete
			if err := cm.WaitForReplicatedTime(ctx, timeutil.Now(), h, r); err != nil {
				return err
			}
			close(cm.destStartedChan)
			return nil
		})
}

func (cm *c2cMixed) WorkloadHook(ctx context.Context) {
	warehouses := 100
	tpccInitCmd := roachtestutil.NewCommand("./cockroach workload init tpcc").
		Arg("{pgurl%s}", cm.c.Range(1, cm.sp.srcNodes)).
		Flag("warehouses", warehouses)
	tpccRunCmd := roachtestutil.NewCommand("./cockroach workload run tpcc").
		Arg("{pgurl%s}", cm.c.Range(1, cm.sp.srcNodes)).
		Option("tolerate-errors").
		Flag("warehouses", warehouses)
	cm.workloadStopper = cm.sourceMvt.Workload("tpcc", cm.c.WorkloadNode(), tpccInitCmd, tpccRunCmd)
}

func (cm *c2cMixed) LatencyHook(ctx context.Context) {
	cm.destMvt.BackgroundFunc("latency verifier", func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
		lv := makeLatencyVerifier("stream-ingestion", 0, cm.sp.maxAcceptedLatency, l,
			getStreamIngestionJobInfo, nil, true)
		defer lv.maybeLogLatencyHist()
		_, db := h.RandomDB(r)
		defer db.Close()

		// The latency verify doesn't need a stopper, as ctx cancellation will stop it.
		dummyCh := make(chan struct{})
		if err := lv.pollLatencyUntilJobSucceeds(ctx, db, int(cm.ingestionJobID), time.Second*5, dummyCh); ctx.Err() == nil {
			// The ctx is cancelled when the background func is successfully stopped,
			// therefore, don't return a context cancellation error.
			return err
		}
		return nil
	})
}

func (cm *c2cMixed) UpdateHook(ctx context.Context) {
	destFinalized := make(chan struct{}, 1)

	// For a given major version upgrade, this can be called three times: upgrade,
	// downgrade, upgrade again
	cm.sourceMvt.InMixedVersion(
		"wait for dest to finalize",
		func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
			if h.Context().Stage == mixedversion.LastUpgradeStage {
				l.Printf("waiting for destination cluster to finalize upgrade")
				chanReadCtx(ctx, destFinalized)
			}

			return nil
		},
	)

	// Called at the end of each major version upgrade.
	destMajorUpgradeCount := 0
	cm.destMvt.AfterUpgradeFinalized(
		"cutover and allow source to finalize",
		func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
			// Ensure the source always waits to finalize until after the dest finalizes.
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
				return cm.sourceFingerprint(ctx, l, r, h)
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
	// TODO(msbutler): test cutting over a time when the app tenant is a different
	// version.
	if err := cm.WaitForReplicatedTime(ctx, timeutil.Now(), h, r); err != nil {
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
	cutover, err := stringToHLC(cutoverStr)
	if err != nil {
		return err
	}
	_, db := h.RandomDB(r)
	defer db.Close()
	if err := WaitForSucceed(ctx, db, cm.ingestionJobID, time.Minute); err != nil {
		return err
	}

	l.Printf("Retained time %s; cutover time %s", retainedHLCTime.GoTime(), cutover.GoTime())
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
	return nil
}

func (cm *c2cMixed) sourceFingerprint(
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
	ctx context.Context, targetTime time.Time, h *mixedversion.Helper, r *rand.Rand,
) error {
	return testutils.SucceedsWithinError(func() error {
		node, db := h.RandomDB(r)
		defer db.Close()
		cm.t.L().Printf("waiting for stream ingestion job progress on node %d", node)
		info, err := getStreamIngestionJobInfo(db, int(cm.ingestionJobID))
		if err != nil {
			return err
		}
		cm.t.L().Printf("current replicated time %s, job status %s, job error %s", info.GetHighWater().Format(time.RFC3339), info.GetStatus(), info.GetError())
		if info.GetHighWater().Before(targetTime) {
			return errors.Newf("waiting for stream ingestion job progress %s to advance beyond %s",
				info.GetHighWater(), targetTime)
		}
		cm.t.L().Printf("reached replicated time %s", info.GetHighWater().Format(time.RFC3339))
		return nil
	}, time.Minute*2)
}

func stringToHLC(s string) (hlc.Timestamp, error) {
	d, _, err := apd.NewFromString(s)
	if err != nil {
		return hlc.Timestamp{}, err
	}
	ts, err := hlc.DecimalToHLC(d)
	return ts, err
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
