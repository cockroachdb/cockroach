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
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

func registerOnlineRestore(r registry.Registry) {
	// This driver creates a variety of roachtests to benchmark online restore
	// performance with the prefix
	// restore/{online,offline}/workload={true,false}/<workload/scale>). For each
	// online restore roachtest (prefix restore/online/*), the driver creates a
	// corresponding roachtest that runs a conventional restore over the same
	// cluster topology and workload in order to measure post restore query
	// latency relative to online restore (prefix restore/control/*). Further, the
	// driver creates an additional roachtest variant where
	// `/workload={true,false}` determines if the driver will run a foreground
	// workload after the restore completes.
	for _, sp := range []restoreSpecs{
		{
			// 400GB tpce Online Restore
			hardware:               makeHardwareSpecs(hardwareSpecs{ebsThroughput: 250 /* MB/s */, workloadNode: true}),
			backup:                 makeRestoringBackupSpecs(backupSpecs{nonRevisionHistory: true, version: "v23.1.11"}),
			timeout:                5 * time.Hour,
			clouds:                 registry.AllClouds,
			suites:                 registry.Suites(registry.Nightly),
			tags:                   registry.Tags("aws"),
			restoreUptoIncremental: 1,
			skip:                   "used for ad hoc experiments",
		},
		{
			// 8TB tpce Online Restore
			hardware: makeHardwareSpecs(hardwareSpecs{nodes: 10, volumeSize: 2000,
				ebsThroughput: 250 /* MB/s */}),
			backup: makeRestoringBackupSpecs(backupSpecs{
				nonRevisionHistory: true,
				version:            "v23.1.11",
				workload:           tpceRestore{customers: 500000}}),
			timeout:                5 * time.Hour,
			clouds:                 registry.AllClouds,
			suites:                 registry.Suites(registry.Nightly),
			tags:                   registry.Tags("aws"),
			restoreUptoIncremental: 1,
			skip:                   "used for ad hoc experiments",
		},
	} {
		for _, runOnline := range []bool{true, false} {
			for _, runWorkload := range []bool{true, false} {
				sp := sp
				runOnline := runOnline
				runWorkload := runWorkload

				if runOnline {
					sp.namePrefix = "online/"
				} else {
					sp.namePrefix = "offline/"
					sp.skip = "used for ad hoc experiments"
				}
				sp.namePrefix = sp.namePrefix + fmt.Sprintf("workload=%t", runWorkload)

				sp.initTestName()
				r.Add(registry.TestSpec{
					Name:      sp.testName,
					Owner:     registry.OwnerDisasterRecovery,
					Benchmark: true,
					Cluster:   sp.hardware.makeClusterSpecs(r, sp.backup.cloud),
					Timeout:   sp.timeout,
					// These tests measure performance. To ensure consistent perf,
					// disable metamorphic encryption.
					EncryptionSupport: registry.EncryptionAlwaysDisabled,
					CompatibleClouds:  sp.clouds,
					Suites:            sp.suites,
					Tags:              sp.tags,
					Skip:              sp.skip,
					Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {

						testStartTime := timeutil.Now()

						rd := makeRestoreDriver(t, c, sp)
						rd.prepareCluster(ctx)

						m := c.NewMonitor(ctx, sp.hardware.getCRDBNodes())
						m.Go(func(ctx context.Context) error {
							db, err := rd.c.ConnE(ctx, rd.t.L(), rd.c.Node(1)[0])
							if err != nil {
								return err
							}
							defer db.Close()
							if _, err := db.Exec("SET CLUSTER SETTING kv.queue.process.guaranteed_time_budget='1h'"); err != nil {
								return err
							}
							if _, err := db.Exec("SET CLUSTER SETTING kv.snapshot_receiver.excise.enabled=true"); err != nil {
								return err
							}
							opts := ""
							if runOnline {
								opts = "WITH EXPERIMENTAL DEFERRED COPY"
							}
							restoreCmd := rd.restoreCmd("DATABASE tpce", opts)
							t.L().Printf("Running %s", restoreCmd)
							if _, err = db.ExecContext(ctx, restoreCmd); err != nil {
								return err
							}
							return nil
						})
						m.Wait()

						workloadCtx, workloadCancel := context.WithCancel(ctx)
						mDownload := c.NewMonitor(workloadCtx, sp.hardware.getCRDBNodes())
						// TODO(msbutler): add foreground query latency tracker

						mDownload.Go(func(ctx context.Context) error {
							if !runWorkload {
								fmt.Printf("roachtest configured to skip running the foreground workload")
								return nil
							}
							err := sp.backup.workload.run(ctx, t, c, sp.hardware)
							// We expect the workload to return a context cancelled error because
							// the roachtest driver cancels the monitor's context after the download job completes
							if err != nil && ctx.Err() == nil {
								// Implies the workload context was not cancelled and the workload cmd returned a
								// different error.
								return errors.Wrapf(err, `Workload context was not cancelled. Error returned by workload cmd`)
							}
							rd.t.L().Printf("workload successfully finished")
							return nil
						})
						mDownload.Go(func(ctx context.Context) error {
							defer workloadCancel()
							if runOnline {
								return waitForDownloadJob(ctx, c, t.L())
							}
							if runWorkload {
								// If we just completed an offline restore and are running the
								// workload, run the workload until we're at most 15 minutes
								// away from timing out.
								testRuntime := timeutil.Since(testStartTime)
								workloadDuration := sp.timeout - testRuntime
								if workloadDuration > time.Minute*15 {
									workloadDuration = workloadDuration - time.Minute*15
								}
								fmt.Printf("let workload run for %.2f minutes", workloadDuration.Minutes())
								time.Sleep(workloadDuration)
							}
							return nil
						})
						mDownload.Wait()
					},
				})
			}
		}
	}
}

func waitForDownloadJob(ctx context.Context, c cluster.Cluster, l *logger.Logger) error {
	l.Printf(`Begin tracking online restore download phase completion`)
	// Wait for the job to succeed.
	succeededJobTick := time.NewTicker(time.Minute * 1)
	defer succeededJobTick.Stop()
	done := ctx.Done()
	conn, err := c.ConnE(ctx, l, c.Node(1)[0])
	if err != nil {
		return err
	}
	defer conn.Close()
	for {
		select {
		case <-done:
			return ctx.Err()
		case <-succeededJobTick.C:
			var status string
			if err := conn.QueryRow(`SELECT status FROM [SHOW JOBS] WHERE job_type = 'RESTORE' ORDER BY created DESC LIMIT 1`).Scan(&status); err != nil {
				return err
			}
			if status == string(jobs.StatusSucceeded) {
				return nil
			} else if status == string(jobs.StatusRunning) {
				l.Printf("Download job still running")
			} else {
				return errors.Newf("job unexpectedly found in %s state", status)
			}
		}
	}
}
