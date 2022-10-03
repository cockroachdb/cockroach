// Copyright 2022 The Cockroach Authors.
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
	"github.com/stretchr/testify/assert"
)

// This test sets up a 3-node CRDB cluster on 8vCPU machines, loads it up with a
// large TPC-C dataset, and sets up a foreground load of kv50/1b. It then
// attempts to create a several "dummy" secondary index on the table while the
// workload is running to measure the impact. The indexes will not be used by
// any of the queries, but the intent is to measure the impact of the index
// creation.
func registerIndexOverload(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:  "admission-control/index-overload",
		Owner: registry.OwnerAdmissionControl,
		// TODO(baptist): After two weeks of nightly baking time, reduce
		// this to a weekly cadence. This is a long-running test and serves only
		// as a coarse-grained benchmark.
		// 	Tags:    []string{`weekly`},
		Cluster: r.MakeClusterSpec(4, spec.CPU(8)),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {

			crdbNodes := c.Spec().NodeCount - 1
			workloadNode := c.Spec().NodeCount

			c.Put(ctx, t.Cockroach(), "./cockroach", c.All())
			c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.Range(1, crdbNodes))

			duration, err := time.ParseDuration(ifLocal(c, "20s", "10m"))
			assert.NoError(t, err)
			testDuration := 3 * duration

			db := c.Conn(ctx, t.L(), crdbNodes)
			defer db.Close()

			if !t.SkipInit() {
				// Initialize the kv database with replicas on all nodes but
				// leaseholders on just n1.
				t.Status(fmt.Sprintf("initializing kv dataset (<%s)", time.Minute))
				splits := ifLocal(c, " --splits=3", " --splits=100")
				c.Run(ctx, c.Node(workloadNode), "./cockroach workload init kv "+splits+" {pgurl:1}")

				// We need a big enough size so index creation will take enough time.
				t.Status(fmt.Sprintf("initializing tpcc dataset (<%s)", duration))
				warehouses := ifLocal(c, " --warehouses=1", " --warehouses=2000")
				c.Run(ctx, c.Node(workloadNode), "./cockroach workload fixtures import tpcc --checks=false"+warehouses+" {pgurl:1}")

				// Setting this low allows us to hit overload. In a larger cluster with
				// more nodes and larger tables, it will hit the unmodified 1000 limit.
				t.Status(fmt.Sprintf("decreasing L0 overload threshold (%s)", duration))
				if _, err := db.ExecContext(ctx,
					"SET CLUSTER SETTING admission.l0_file_count_overload_threshold=10",
				); err != nil {
					t.Fatalf("failed to alter cluster setting: %v", err)
				}
			}

			t.Status(fmt.Sprintf("starting kv workload thread to run for %s (<%s)", testDuration, time.Minute))
			m := c.NewMonitor(ctx, c.Range(1, crdbNodes))
			m.Go(func(ctx context.Context) error {
				testDurationStr := " --duration=" + testDuration.String()
				concurrency := ifLocal(c, "  --concurrency=8", " --concurrency=2048")
				c.Run(ctx, c.Node(crdbNodes+1),
					"./cockroach workload run kv --read-percent=50 --max-rate=1000 --max-block-bytes=4096"+
						testDurationStr+concurrency+fmt.Sprintf(" {pgurl:1-%d}", crdbNodes),
				)
				return nil
			})

			t.Status(fmt.Sprintf("recording baseline performance (%s)", duration))
			time.Sleep(duration)

			// Choose an index creation that takes ~10-12 minutes.
			t.Status(fmt.Sprintf("starting index creation (%s)", duration))
			if _, err := db.ExecContext(ctx,
				"CREATE INDEX test_index ON tpcc.stock(s_quantity)",
			); err != nil {
				t.Fatalf("failed to create index: %v", err)
			}

			// Drop the index to leave the system in its original state. This makes it
			// easier to rerun without resetting the cluster.
			t.Status("index creation complete - dropping now")

			if _, err := db.ExecContext(ctx,
				"DROP INDEX tpcc.test_index",
			); err != nil {
				t.Fatalf("failed to drop index: %v", err)
			}

			t.Status("drop complete - waiting for workload to finish (~%s)", duration)
			m.Wait()
		},
	})
}
