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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
	"github.com/stretchr/testify/require"
)

// This test sets up a 1 node CRDB cluster on an 32vCPU machine, runs low
// priority kv0 that will overload the compaction throughput (out of L0) of
// the store. With admission control subjecting this low priority load to
// elastic IO tokens, the overload is limited.
func registerDiskBandwidth(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:      "admission-control/disk-bandwidth",
		Owner:     registry.OwnerAdmissionControl,
		Timeout:   time.Hour,
		Benchmark: true,
		// TODO(sumeer): Reduce to weekly after working well.
		// Tags:      registry.Tags(`weekly`),
		Cluster:         r.MakeClusterSpec(3, spec.CPU(32)),
		RequiresLicense: true,
		Leases:          registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			if c.IsLocal() {
				t.Skip("not meant to run locally")
			}
			if c.Spec().NodeCount != 3 {
				t.Fatalf("expected 3 nodes, found %d", c.Spec().NodeCount)
			}
			crdbNodes := 1
			promNode := c.Spec().NodeCount
			regularNode, elasticNode := c.Spec().NodeCount, c.Spec().NodeCount-1

			promCfg := &prometheus.Config{}
			promCfg.WithPrometheusNode(c.Node(promNode).InstallNodes()[0]).
				WithNodeExporter(c.Range(1, c.Spec().NodeCount-1).InstallNodes()).
				WithCluster(c.Range(1, c.Spec().NodeCount-1).InstallNodes()).
				WithGrafanaDashboard("https://go.crdb.dev/p/index-admission-control-grafana")
			require.NoError(t, c.StartGrafana(ctx, t.L(), promCfg))

			c.Put(ctx, t.Cockroach(), "./cockroach", c.Range(1, crdbNodes))
			c.Put(ctx, t.DeprecatedWorkload(), "./workload", c.Node(regularNode))
			c.Put(ctx, t.DeprecatedWorkload(), "./workload", c.Node(elasticNode))

			startOpts := option.DefaultStartOptsNoBackups()
			startOpts.RoachprodOpts.ExtraArgs = append(startOpts.RoachprodOpts.ExtraArgs,
				"--vmodule=io_load_listener=2")
			settings := install.MakeClusterSettings()
			c.Start(ctx, t.L(), startOpts, settings, c.Range(1, crdbNodes))

			setAdmissionControl(ctx, t, c, true)

			{ // Activate disk bandwidth controls.
				db := c.Conn(ctx, t.L(), crdbNodes)
				defer db.Close()

				if _, err := db.ExecContext(
					ctx, "SET CLUSTER SETTING kvadmission.store.provisioned_bandwidth = '250MiB'"); err != nil {
					t.Fatalf("failed to set kvadmission.store.provisioned_bandwidth: %v", err)
				}
			}

			duration := 30 * time.Minute
			t.Status("running workloads")
			m := c.NewMonitor(ctx, c.Range(1, crdbNodes))

			m.Go(func(ctx context.Context) error {
				// Regular traffic: consumes 40-50% of the disk bandwidth (note
				// the low concurrency=2, since regular traffic does not cause
				// any disk bandwidth controls to be activated -- so we've
				// explicitly set it up to leave significant unused bandwidth).
				dur := " --duration=" + duration.String()
				url := fmt.Sprintf(" {pgurl:1-%d}", crdbNodes)
				cmd := "./workload run kv --init --histograms=perf/stats.json --concurrency=2 " +
					"--splits=1000 --read-percent=0 --min-block-bytes=4096 --max-block-bytes=4096 " +
					"--background-qos=false --tolerate-errors" + dur + url
				c.Run(ctx, c.Node(regularNode), cmd)
				return nil
			})

			m.Go(func(ctx context.Context) error {
				// Then add elastic traffic with a high concurrency=1024 (this
				// is more than enough to blow past the provisioned limit if
				// there was no disk bandwidth control). The throughput of
				// regular traffic stays stable.
				time.Sleep(5 * time.Minute)

				dur := " --duration=" + duration.String()
				url := fmt.Sprintf(" {pgurl:1-%d}", crdbNodes)
				cmd := "./workload run kv --init --histograms=perf/stats.json --concurrency=1024 " +
					"--splits=1000 --read-percent=0 --min-block-bytes=4096 --max-block-bytes=4096 " +
					"--background-qos=true --tolerate-errors" + dur + url
				c.Run(ctx, c.Node(elasticNode), cmd)
				return nil
			})

			m.Wait()
		},
	})
}
