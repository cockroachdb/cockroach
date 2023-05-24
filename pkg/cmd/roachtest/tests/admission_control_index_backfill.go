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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/util/version"
)

func registerIndexBackfill(r registry.Registry) {
	clusterSpec := r.MakeClusterSpec(
		10, /* nodeCount */
		spec.CPU(8),
		spec.Zones("us-east1-b"),
		spec.VolumeSize(500),
		spec.Cloud(spec.GCE),
	)
	clusterSpec.InstanceType = "n2-standard-8"
	clusterSpec.GCEMinCPUPlatform = "Intel Ice Lake"
	clusterSpec.GCEVolumeType = "pd-ssd"

	// XXX: Subsume https://github.com/cockroachdb/cockroach/pull/90005/files
	r.Add(registry.TestSpec{
		Name:    "admission-control/index-backfill",
		Timeout: 6 * time.Hour,
		Owner:   registry.OwnerAdmissionControl,
		// TODO(irfansharif): Reduce to weekly cadence once stabilized.
		// Tags:            registry.Tags(`weekly`),
		Cluster:         clusterSpec,
		RequiresLicense: true,
		SnapshotPrefix:  "tpce-100k",
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			crdbNodes := c.Spec().NodeCount - 1
			workloadNode := c.Spec().NodeCount

			promCfg := &prometheus.Config{}
			promCfg.WithPrometheusNode(c.Node(workloadNode).InstallNodes()[0]).
				WithNodeExporter(c.Range(1, crdbNodes).InstallNodes()).
				WithCluster(c.Range(1, crdbNodes).InstallNodes()).
				WithGrafanaDashboard("https://go.crdb.dev/p/index-admission-control-grafana").
				WithScrapeConfigs(
					prometheus.MakeWorkloadScrapeConfig("workload", "/",
						makeWorkloadScrapeNodes(
							c.Node(workloadNode).InstallNodes()[0],
							[]workloadInstance{{nodes: c.Node(workloadNode)}},
						),
					),
				)

			snapshots, err := c.ListSnapshots(ctx, vm.VolumeSnapshotListOpts{
				// TODO(irfansharif): Search by taking in the other parts of the
				// snapshot fingerprint, i.e. the node count, the version, etc.
				Name: t.SnapshotPrefix(),
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(snapshots) == 0 {
				t.L().Printf("no existing snapshots found for %s (%s), doing pre-work",
					t.Name(), t.SnapshotPrefix())

				// Set up TPC-E with 100k customers. Do so using a published
				// CRDB release, since we'll use this state to generate disk
				// snapshots.
				runTPCE(ctx, t, c, tpceOptions{
					start: func(ctx context.Context, t test.Test, c cluster.Cluster) {
						pred, err := version.PredecessorVersion(*t.BuildVersion())
						if err != nil {
							t.Fatal(err)
						}

						path, err := clusterupgrade.UploadVersion(ctx, t, t.L(), c, c.All(), pred)
						if err != nil {
							t.Fatal(err)
						}

						// Copy over the binary to ./cockroach and run it from
						// there. This test captures disk snapshots, which are
						// fingerprinted using the binary version found in this
						// path. The reason it can't just poke at the running
						// crdb process is because when grabbing snapshots, crdb
						// is not running.
						c.Run(ctx, c.All(), fmt.Sprintf("cp %s ./cockroach", path))
						settings := install.MakeClusterSettings(install.NumRacksOption(crdbNodes))
						if err := c.StartE(ctx, t.L(), option.DefaultStartOptsNoBackups(), settings, c.Range(1, crdbNodes)); err != nil {
							t.Fatal(err)
						}
					},
					customers:          100_000,
					disablePrometheus:  true,
					setupType:          usingTPCEInit,
					estimatedSetupTime: 4 * time.Hour,
					nodes:              crdbNodes,
					owner:              registry.OwnerAdmissionControl,
					cpus:               clusterSpec.CPUs,
					ssds:               1,
					onlySetup:          true,
				})

				// Stop all nodes before capturing cluster snapshots.
				c.Stop(ctx, t.L(), option.DefaultStopOpts())

				// Create the aforementioned snapshots.
				if err := c.CreateSnapshot(ctx, t.SnapshotPrefix()); err != nil {
					t.Fatal(err)
				}
				snapshots, err = c.ListSnapshots(ctx, vm.VolumeSnapshotListOpts{
					Name: t.SnapshotPrefix(),
				})
				if err != nil {
					t.Fatal(err)
				}
				t.L().Printf("using %d newly created snapshot(s) with prefix %q", len(snapshots), t.SnapshotPrefix())
			} else {
				t.L().Printf("using %d pre-existing snapshot(s) with prefix %q", len(snapshots), t.SnapshotPrefix())
			}

			if err := c.ApplySnapshots(ctx, snapshots); err != nil {
				t.Fatal(err)
			}

			// Run the foreground TPC-E workload. Run a large index backfill
			// while it's running.
			runTPCE(ctx, t, c, tpceOptions{
				start: func(ctx context.Context, t test.Test, c cluster.Cluster) {
					settings := install.MakeClusterSettings(install.NumRacksOption(crdbNodes))
					c.Put(ctx, t.Cockroach(), "./cockroach", c.All())
					for i := 1; i <= crdbNodes; i++ {
						c.Start(ctx, t.L(), option.DefaultStartOptsNoBackups(), settings, c.Node(i))
					}
				},
				customers:        100_000,
				activeCustomers:  20_000,
				threads:          400,
				skipCleanup:      true,
				ssds:             1,
				setupType:        usingExistingTPCEData,
				nodes:            clusterSpec.NodeCount - 1,
				owner:            registry.OwnerAdmissionControl,
				cpus:             clusterSpec.CPUs,
				prometheusConfig: promCfg,
				during: func(ctx context.Context) error {
					duration := 5 * time.Minute
					t.Status(fmt.Sprintf("recording baseline performance (<%s)", duration))
					time.Sleep(duration)

					// Choose an index creation that takes ~10-12 minutes.
					t.Status(fmt.Sprintf("starting index creation (<%s)", duration*6))

					db := c.Conn(ctx, t.L(), 1)
					defer db.Close()

					if _, err := db.ExecContext(ctx,
						fmt.Sprintf("CREATE INDEX index_%s ON tpce.cash_transaction (ct_dts)",
							time.Now().Format("20060102_T150405"),
						),
					); err != nil {
						t.Fatalf("failed to create index: %v", err)
					}

					t.Status("index creation complete, waiting for workload to finish")
					return nil
				},
				workloadDuration: time.Hour,
			})
		},
	})
}
