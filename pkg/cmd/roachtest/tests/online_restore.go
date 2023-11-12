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
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func registerOnlineRestore(r registry.Registry) {
	durationGauge := r.PromFactory().NewGaugeVec(prometheus.GaugeOpts{Namespace: registry.
		PrometheusNameSpace, Subsystem: "online_restore", Name: "duration"}, []string{"test_name"})

	for _, sp := range []restoreSpecs{
		{
			namePrefix:             "online",
			hardware:               makeHardwareSpecs(hardwareSpecs{ebsThroughput: 250 /* MB/s */}),
			backup:                 makeRestoringBackupSpecs(backupSpecs{nonRevisionHistory: true, version: "v23.1.11"}),
			timeout:                5 * time.Hour,
			clouds:                 registry.AllClouds,
			suites:                 registry.Suites(registry.Nightly),
			tags:                   registry.Tags("aws"),
			restoreUptoIncremental: 1,
			skip:                   "used for ad hoc experiments",
		},
	} {
		sp := sp
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

				rd := makeRestoreDriver(t, c, sp)
				rd.prepareCluster(ctx)

				m := c.NewMonitor(ctx)
				dul := roachtestutil.NewDiskUsageLogger(t, c)
				m.Go(dul.Runner)
				m.Go(func(ctx context.Context) error {
					defer dul.Done()
					t.Status(`running setup statements`)
					db, err := rd.c.ConnE(ctx, rd.t.L(), rd.c.Node(1)[0])
					if err != nil {
						return errors.Wrapf(err, "failure to run setup statements")
					}
					defer db.Close()

					t.Status(`Running online restore: linking phase`)
					metricCollector := rd.initRestorePerfMetrics(ctx, durationGauge)
					restoreCmd := rd.restoreCmd("DATABASE tpce", "WITH EXPERIMENTAL DEFERRED COPY")
					t.L().Printf("Running %s", restoreCmd)
					if _, err = db.ExecContext(ctx, restoreCmd); err != nil {
						return err
					}
					metricCollector()
					return nil
				})
				m.Wait()

				t.Status(`Running online restore: download phase`)
				workloadCtx, workloadCancel := context.WithCancel(ctx)
				mDownload := c.NewMonitor(workloadCtx)
				defer func() {
					workloadCancel()
					mDownload.Wait()
				}()
				// TODO(msbutler): add foreground query latency tracker

				mDownload.Go(func(ctx context.Context) error {
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
					// Wait for the job to succeed.
					succeededJobTick := time.NewTicker(time.Minute * 1)
					defer succeededJobTick.Stop()
					done := ctx.Done()
					conn, err := c.ConnE(ctx, t.L(), c.Node(1)[0])
					require.NoError(t, err)
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
								rd.t.L().Printf("Download job still running")
							} else {
								return errors.Newf("job unexpectedly found in %s state", status)
							}
						}
					}
				})
				mDownload.Wait()
			},
		})
	}
}
