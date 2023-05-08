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

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
)

// This test sets up a 3-node CRDB cluster on 8vCPU machines running
// 1000-warehouse TPC-C, and kicks off a few changefeed backfills concurrently.
// We've observed latency spikes during backfills because of its CPU/scan-heavy
// nature -- it can elevate CPU scheduling latencies which in turn translates to
// an increase in foreground latency.
func registerIndexBackfill(r registry.Registry) {
	clusterSpec := r.MakeClusterSpec(
		1, /* nodeCount */
		spec.CPU(8),
		spec.Zones("us-east1-b"),
		spec.VolumeSize(500),
		spec.Cloud(spec.GCE),
	)
	clusterSpec.InstanceType = "n2-standard-8"
	clusterSpec.GCEMinCPUPlatform = "Intel Ice Lake"
	clusterSpec.GCEVolumeType = "pd-ssd"

	r.Add(registry.TestSpec{
		Name:  "admission-control/index-backfill",
		Owner: registry.OwnerAdmissionControl,
		// TODO(irfansharif): Reduce to weekly cadence once stabilized.
		// Tags:            registry.Tags(`weekly`),
		Cluster:         clusterSpec,
		RequiresLicense: true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			if err := c.SnapshotVolume(ctx, "admission-control-index-backfill"); err != nil {
				t.Fatal(err)
			}
			// set ZONE us-east1-b
			// set STORAGELOCATION us-east1
			// set SOURCE irfansharif-1666654285-01-n10cpu8
			// set NODES 10
			//
			// roachprod create $SOURCE --nodes $NODES --gce-machine-type=n2-standard-8 -l 12h \
			// --gce-min-cpu-platform="Intel Ice Lake" --gce-pd-volume-type=pd-ssd \
			// --gce-pd-volume-size=500 --clouds=gce --gce-zones=$ZONE --local-ssd=false
			//
			// roachprod sync # fight annoying resolution issues
			// gw ssh -- '
			// cd /home/irfansharif/go/src/github.com/cockroachdb/cockroach
			// git fetch --all
			// git checkout 221024.index-backfill-latency
			// git reset --hard origin/221024.index-backfill-latency
			// ./dev build
			// '
			// gw scp \
			// 'gceworker-irfansharif:/home/irfansharif/go/src/github.com/cockroachdb/cockroach/cockroach' \
			// $CRDB/artifacts/cockroach
			//
			// roachprod stop $SOURCE:1-9
			// roachprod put $SOURCE:1-9 $CRDB/artifacts/cockroach ./cockroach
			// roachprod start $SOURCE:1-9
			// roachprod install $SOURCE:10 docker
			// roachprod grafana-start $SOURCE
			// roachprod run $SOURCE:10 -- sudo docker run cockroachdb/tpc-e:latest --customers=100000 --racks=9 --init # init step; takes ~4h; run through GCE worker perhaps
			//
			// for i in ( seq -f "%04g" 1 $NODES)
			// gcloud compute snapshots create $CLUSTER-$i --source-disk $CLUSTER-$i-1  --source-disk-zone $ZONE --storage-location $STORAGELOCATION &
			// 	end
			// wait
			//
			// roachprod destroy $SOURCE
			// XXX:
			//opts := tpceOptions{
			//	customers: 100_000,
			//	nodes:     10,
			//	cpus:      8,
			//	ssds:      1,
			//	tags:      registry.Tags("weekly"),
			//	timeout:   36 * time.Hour,
			//}
			//runTPCE(ctx, t, c, opts)

			//crdbNodes := c.Spec().NodeCount - 1
			//workloadNode := crdbNodes + 1
			//numWarehouses, workloadDuration, estimatedSetupTime := 1000, 60*time.Minute, 10*time.Minute
			//if c.IsLocal() {
			//	numWarehouses, workloadDuration, estimatedSetupTime = 1, time.Minute, 2*time.Minute
			//}
			//
			//promCfg := &prometheus.Config{}
			//promCfg.WithPrometheusNode(c.Node(workloadNode).InstallNodes()[0]).
			//	WithNodeExporter(c.Range(1, c.Spec().NodeCount-1).InstallNodes()).
			//	WithCluster(c.Range(1, c.Spec().NodeCount-1).InstallNodes()).
			//	WithGrafanaDashboardJSON(grafana.ChangefeedAdmissionControlGrafana).
			//	WithScrapeConfigs(
			//		prometheus.MakeWorkloadScrapeConfig("workload", "/",
			//			makeWorkloadScrapeNodes(
			//				c.Node(workloadNode).InstallNodes()[0],
			//				[]workloadInstance{{nodes: c.Node(workloadNode)}},
			//			),
			//		),
			//	)
			//
			//if t.SkipInit() {
			//	t.Status(fmt.Sprintf("running tpcc for %s (<%s)", workloadDuration, time.Minute))
			//} else {
			//	t.Status(fmt.Sprintf("initializing + running tpcc for %s (<%s)", workloadDuration, 10*time.Minute))
			//}
			//
			//padDuration, err := time.ParseDuration(ifLocal(c, "5s", "5m"))
			//if err != nil {
			//	t.Fatal(err)
			//}
			//stopFeedsDuration, err := time.ParseDuration(ifLocal(c, "5s", "1m"))
			//if err != nil {
			//	t.Fatal(err)
			//}
			//
			//runTPCC(ctx, t, c, tpccOptions{
			//	Warehouses:                    numWarehouses,
			//	Duration:                      workloadDuration,
			//	SetupType:                     usingImport,
			//	EstimatedSetupTime:            estimatedSetupTime,
			//	SkipPostRunCheck:              true,
			//	ExtraSetupArgs:                "--checks=false",
			//	PrometheusConfig:              promCfg,
			//	DisableDefaultScheduledBackup: true,
			//	During: func(ctx context.Context) error {
			//		db := c.Conn(ctx, t.L(), crdbNodes)
			//		defer db.Close()
			//
			//		t.Status(fmt.Sprintf("configuring cluster (<%s)", 30*time.Second))
			//		{
			//			setAdmissionControl(ctx, t, c, true)
			//
			//			// Changefeeds depend on rangefeeds being enabled.
			//			if _, err := db.Exec("SET CLUSTER SETTING kv.rangefeed.enabled = true"); err != nil {
			//				return err
			//			}
			//			// By default, each changefeed can use up to 512MB of memory from root monitor.
			//			// We will start 10 changefeeds, and potentially, we can reserve 5GB from main
			//			// memory monitor.  That's a bit too much when running this test on smaller, 8G
			//			// machines.  Lower the memory allowance.
			//			if _, err := db.Exec("SET CLUSTER SETTING changefeed.memory.per_changefeed_limit = '128MB'"); err != nil {
			//				return err
			//			}
			//		}
			//
			//		stopFeeds(db) // stop stray feeds (from repeated runs against the same cluster for ex.)
			//		defer stopFeeds(db)
			//
			//		m := c.NewMonitor(ctx, c.Range(1, crdbNodes))
			//		m.Go(func(ctx context.Context) error {
			//			const iters, changefeeds = 5, 10
			//			for i := 0; i < iters; i++ {
			//				if i == 0 {
			//					t.Status(fmt.Sprintf("setting performance baseline (<%s)", padDuration))
			//				}
			//				time.Sleep(padDuration) // each iteration lasts long enough to observe effects in metrics
			//
			//				t.Status(fmt.Sprintf("during: round %d: stopping extant changefeeds (<%s)", i, stopFeedsDuration))
			//				stopFeeds(db)
			//				time.Sleep(stopFeedsDuration) // buffer for cancellations to take effect/show up in metrics
			//
			//				t.Status(fmt.Sprintf("during: round %d: creating %d changefeeds (<%s)", i, changefeeds, time.Minute))
			//
			//				for j := 0; j < changefeeds; j++ {
			//					var createChangefeedStmt string
			//					if i%2 == 0 {
			//						createChangefeedStmt = fmt.Sprintf(`
			//						CREATE CHANGEFEED FOR tpcc.order_line, tpcc.stock, tpcc.customer
			//						INTO 'null://' WITH cursor = '-%ds'
			//					`, int64(float64(i+1)*padDuration.Seconds())) // scanning as far back as possible (~ when the workload started)
			//					} else {
			//						createChangefeedStmt = "CREATE CHANGEFEED FOR tpcc.order_line, tpcc.stock, tpcc.customer " +
			//							"INTO 'null://' WITH initial_scan = 'only'"
			//					}
			//
			//					if _, err := db.ExecContext(ctx, createChangefeedStmt); err != nil {
			//						return err
			//					}
			//				}
			//			}
			//			return nil
			//		})
			//
			//		t.Status(fmt.Sprintf("waiting for workload to finish (<%s)", workloadDuration))
			//		m.Wait()
			//
			//		return nil
			//	},
			//})
		},
	})
}
