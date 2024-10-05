// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/grafana"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
)

// This test sets up a 3-node CRDB cluster on 8vCPU machines running
// 1000-warehouse TPC-C with an aggressive (every 20m) full backup schedule.
// We've observed latency spikes during backups because of its CPU-heavy nature
// -- it can elevate CPU scheduling latencies which in turn translates to an
// increase in foreground latency. In #86638 we introduced admission control
// mechanisms to dynamically pace such work while maintaining acceptable CPU
// scheduling latencies (sub millisecond p99s). This roachtest exercises that
// machinery.
//
// TODO(irfansharif): Add libraries to automatically spit out the degree to
// which {CPU-scheduler,foreground} latencies are protected and track this data
// in roachperf.
func registerElasticControlForBackups(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             "admission-control/elastic-backup",
		Owner:            registry.OwnerAdmissionControl,
		Benchmark:        true,
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Weekly),
		Cluster:          r.MakeClusterSpec(4, spec.CPU(8)),
		Leases:           registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			if c.Spec().NodeCount < 4 {
				t.Fatalf("expected at least 4 nodes, found %d", c.Spec().NodeCount)
			}

			crdbNodes := c.Spec().NodeCount - 1
			workloadNode := crdbNodes + 1
			numWarehouses, workloadDuration, estimatedSetupTime := 1000, 90*time.Minute, 10*time.Minute
			if c.IsLocal() {
				numWarehouses, workloadDuration, estimatedSetupTime = 1, time.Minute, 2*time.Minute
			}

			promCfg := &prometheus.Config{}
			promCfg.WithPrometheusNode(c.Node(workloadNode).InstallNodes()[0]).
				WithNodeExporter(c.Range(1, c.Spec().NodeCount-1).InstallNodes()).
				WithCluster(c.Range(1, c.Spec().NodeCount-1).InstallNodes()).
				WithGrafanaDashboardJSON(grafana.BackupAdmissionControlGrafanaJSON).
				WithScrapeConfigs(
					prometheus.MakeWorkloadScrapeConfig("workload", "/",
						makeWorkloadScrapeNodes(
							c.Node(workloadNode).InstallNodes()[0],
							[]workloadInstance{{nodes: c.Node(workloadNode)}},
						),
					),
				)

			if t.SkipInit() {
				t.Status(fmt.Sprintf("running tpcc for %s (<%s)", workloadDuration, estimatedSetupTime))
			} else {
				t.Status(fmt.Sprintf("initializing + running tpcc for %s (<%s)", workloadDuration, estimatedSetupTime))
			}

			runTPCC(ctx, t, c, tpccOptions{
				Warehouses:                    numWarehouses,
				Duration:                      workloadDuration,
				SetupType:                     usingImport,
				EstimatedSetupTime:            estimatedSetupTime,
				SkipPostRunCheck:              true,
				ExtraSetupArgs:                "--checks=false",
				PrometheusConfig:              promCfg,
				DisableDefaultScheduledBackup: true,
				During: func(ctx context.Context) error {
					db := c.Conn(ctx, t.L(), crdbNodes)
					defer db.Close()

					t.Status(fmt.Sprintf("during: enabling admission control (<%s)", 30*time.Second))
					setAdmissionControl(ctx, t, c, true)

					m := c.NewMonitor(ctx, c.Range(1, crdbNodes))
					m.Go(func(ctx context.Context) error {
						t.Status(fmt.Sprintf("during: creating full backup schedule to run every 20m (<%s)", time.Minute))
						gcsBackupTestingBucket := backupTestingBucket
						_, err := db.ExecContext(ctx,
							`CREATE SCHEDULE FOR BACKUP INTO $1 RECURRING '*/20 * * * *' FULL BACKUP ALWAYS WITH SCHEDULE OPTIONS ignore_existing_backups;`,
							"gs://"+gcsBackupTestingBucket+"/"+c.Name()+"?AUTH=implicit",
						)
						return err
					})
					m.Wait()

					t.Status(fmt.Sprintf("during: waiting for workload to finish (<%s)", workloadDuration))
					return nil
				},
			})
		},
	})
}
