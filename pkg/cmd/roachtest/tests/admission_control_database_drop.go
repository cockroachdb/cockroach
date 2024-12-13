// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/testutils/release"
)

func registerDatabaseDrop(r registry.Registry) {
	clusterSpec := r.MakeClusterSpec(
		10, /* nodeCount */
		spec.CPU(8),
		spec.WorkloadNode(),
		spec.WorkloadNodeCPU(8),
		spec.VolumeSize(500),
		spec.GCEVolumeType("pd-ssd"),
		spec.GCEMachineType("n2-standard-8"),
		spec.GCEZones("us-east1-b"),
	)

	r.Add(registry.TestSpec{
		Name:             "admission-control/database-drop",
		Timeout:          15 * time.Hour,
		Owner:            registry.OwnerAdmissionControl,
		Benchmark:        true,
		CompatibleClouds: registry.OnlyGCE,
		Suites:           registry.Suites(registry.Weekly),
		Cluster:          clusterSpec,
		SnapshotPrefix:   "droppable-database-tpce-100k",
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			snapshots, err := c.ListSnapshots(ctx, vm.VolumeSnapshotListOpts{
				NamePrefix: t.SnapshotPrefix(),
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(snapshots) == 0 {
				t.L().Printf("no existing snapshots found for %s (%s), doing pre-work",
					t.Name(), t.SnapshotPrefix())

				pred, err := release.LatestPredecessor(t.BuildVersion())
				if err != nil {
					t.Fatal(err)
				}
				path, err := clusterupgrade.UploadCockroach(
					ctx, t, t.L(), c, c.All(), clusterupgrade.MustParseVersion(pred),
				)
				if err != nil {
					t.Fatal(err)
				}
				// Copy over the binary to ./cockroach and run it from there.
				// This test captures disk snapshots, which are fingerprinted
				// using the binary version found in this path. The reason it
				// can't just poke at the running CRDB process is because when
				// grabbing snapshots, CRDB is not running.
				//
				// TODO(irfansharif): Make this versioning business a bit more
				// explicit throughout. It works, but too tacitly.
				c.Run(ctx, option.WithNodes(c.All()), fmt.Sprintf("cp %s ./cockroach", path))

				// Set up TPC-E with 100k customers.
				//
				// TODO(irfansharif): We can't simply re-use the snapshots
				// already created by admission_control_index_backfill.go,
				// though that'd be awfully convenient. When the two tests run
				// simultaneously, the two clusters end up using the same
				// cluster ID and there's cross-talk from persisted gossip state
				// where we record IP addresses.

				runTPCE(ctx, t, c, tpceOptions{
					start: func(ctx context.Context, t test.Test, c cluster.Cluster) {
						settings := install.MakeClusterSettings(install.NumRacksOption(len(c.CRDBNodes())))
						startOpts := option.NewStartOpts(option.NoBackupSchedule)
						roachtestutil.SetDefaultSQLPort(c, &startOpts.RoachprodOpts)
						if err := c.StartE(ctx, t.L(), startOpts, settings, c.CRDBNodes()); err != nil {
							t.Fatal(err)
						}
					},
					customers:          100_000,
					disablePrometheus:  true,
					setupType:          usingTPCEInit,
					estimatedSetupTime: 4 * time.Hour,
					nodes:              len(c.CRDBNodes()),
					cpus:               clusterSpec.CPUs,
					ssds:               1,
					onlySetup:          true,
				})

				// We have to create the droppable database next. We're going to
				// do it by renaming the existing TPC-E database and re-creating
				// another one using the same init process above.
				//
				// TODO(irfansharif): The TPC-E workload hardcodes the name of
				// the database it uses, hence this renaming dance. We could
				// change that. We could also load up some other data heavy
				// fixture, like TPC-C. All we're going to do is drop it. We
				// could also use CREATE TABLE AS.

				db := c.Conn(ctx, t.L(), 1)
				defer db.Close()

				if _, err := db.ExecContext(ctx,
					"ALTER DATABASE tpce RENAME TO droppable_tpce;",
				); err != nil {
					t.Fatal(err)
				}

				// Set up TPC-E with 100k customers, again.
				runTPCE(ctx, t, c, tpceOptions{
					start: func(ctx context.Context, t test.Test, c cluster.Cluster) {
						// Things are already staged/setup above. There's
						// nothing to do here.
					},
					customers:          100_000,
					disablePrometheus:  true,
					setupType:          usingTPCEInit,
					estimatedSetupTime: 4 * time.Hour,
					nodes:              len(c.CRDBNodes()),
					cpus:               clusterSpec.CPUs,
					ssds:               1,
					onlySetup:          true,
				})

				// Stop all nodes before capturing cluster snapshots.
				c.Stop(ctx, t.L(), option.DefaultStopOpts())

				// Create the aforementioned snapshots.
				snapshots, err = c.CreateSnapshot(ctx, t.SnapshotPrefix())
				if err != nil {
					t.Fatal(err)
				}
				t.L().Printf("created %d new snapshot(s) with prefix %q, using this state",
					len(snapshots), t.SnapshotPrefix())
			} else {
				t.L().Printf("using %d pre-existing snapshot(s) with prefix %q",
					len(snapshots), t.SnapshotPrefix())

				if !t.SkipInit() {
					// Creating and attaching volumes takes some time. This test
					// is written to be re-entrant. Assume the user passing in
					// --skip-init knows this particular test behavior.
					if err := c.ApplySnapshots(ctx, snapshots); err != nil {
						t.Fatal(err)
					}
				}
			}

			promCfg := &prometheus.Config{}
			promCfg.WithPrometheusNode(c.WorkloadNode().InstallNodes()[0]).
				WithNodeExporter(c.CRDBNodes().InstallNodes()).
				WithCluster(c.CRDBNodes().InstallNodes()).
				WithGrafanaDashboard("https://go.crdb.dev/p/index-admission-control-grafana").
				WithScrapeConfigs(
					prometheus.MakeWorkloadScrapeConfig("workload", "/",
						makeWorkloadScrapeNodes(
							c.WorkloadNode().InstallNodes()[0],
							[]workloadInstance{{nodes: c.WorkloadNode()}},
						),
					),
				)

			// Run the foreground TPC-E workload. Drop a full database while
			// it's running.
			//
			// TODO(irfansharif): This doesn't show much of a foreground
			// latency/throughput impact. Likely because TPC-E is very read
			// heavy and in the short spurt where we there's a burst of
			// compactions, we're not using disk bandwidth sufficiently to need
			// something like disk bandwidth tokens. Find a more appropriate
			// foreground load to run and refresh this test. Look at the
			// clearrange tests -- it seems more appropriate and there's already
			// sustained IO token exhaustion, so perhaps just cargocult that
			// test and use disk snapshots?
			runTPCE(ctx, t, c, tpceOptions{
				start: func(ctx context.Context, t test.Test, c cluster.Cluster) {
					startOpts := option.NewStartOpts(option.NoBackupSchedule)
					roachtestutil.SetDefaultSQLPort(c, &startOpts.RoachprodOpts)
					roachtestutil.SetDefaultAdminUIPort(c, &startOpts.RoachprodOpts)
					settings := install.MakeClusterSettings(install.NumRacksOption(len(c.CRDBNodes())))
					if err := c.StartE(ctx, t.L(), startOpts, settings, c.CRDBNodes()); err != nil {
						t.Fatal(err)
					}
				},
				customers:        100_000,
				activeCustomers:  20_000,
				threads:          400,
				skipCleanup:      true,
				ssds:             1,
				setupType:        usingExistingTPCEData,
				nodes:            clusterSpec.NodeCount - 1,
				cpus:             clusterSpec.CPUs,
				prometheusConfig: promCfg,
				workloadDuration: 30 * time.Minute,
				during: func(ctx context.Context) error {
					db := c.Conn(ctx, t.L(), 1)
					defer db.Close()

					t.Status(fmt.Sprintf("recording baseline performance (<%s)", 5*time.Minute))
					time.Sleep(5 * time.Minute)

					t.Status(fmt.Sprintf("issuing drop statements (happens asynchronously within <%s)", 15*time.Minute))
					for _, dropStatement := range []string{
						`DROP TABLE droppable_tpce.trade CASCADE`,
						`DROP TABLE droppable_tpce.cash_transaction CASCADE`,
						`DROP TABLE droppable_tpce.trade_history CASCADE`,
						`DROP TABLE droppable_tpce.settlement CASCADE`,

						// TODO(irfansharif): We'd rather drop the entire
						// database of course, but there's a real bug:
						// https://github.com/cockroachdb/cockroach/issues/104044.
						// Change once fixed. The individual tables above amount
						// to ~4TiB of data dropped. It exercises the same code
						// paths as indexes being dropped.
						//
						// `DROP DATABASE droppable_tpce CASCADE`,
					} {
						if _, err := db.ExecContext(ctx,
							dropStatement,
						); err != nil {
							return err
						}
					}

					// TODO(irfansharif): Wait for "GC for DROP ..." jobs to
					// finish explicitly.
					t.Status(fmt.Sprintf("waiting for workload to finish (<%s)", 20*time.Minute))
					return nil
				},
			})
		},
	})
}
