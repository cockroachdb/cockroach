// Copyright 2018 The Cockroach Authors.
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
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
)

func registerNIndexes(r registry.Registry, secondaryIndexes int) {
	const nodes = 6
	geoZones := []string{"us-east1-b", "us-west1-b", "europe-west2-b"}
	if r.MakeClusterSpec(1).Cloud == spec.AWS {
		geoZones = []string{"us-east-2b", "us-west-1a", "eu-west-1a"}
	}
	geoZonesStr := strings.Join(geoZones, ",")
	r.Add(registry.TestSpec{
		Name:    fmt.Sprintf("indexes/%d/nodes=%d/multi-region", secondaryIndexes, nodes),
		Owner:   registry.OwnerKV,
		Cluster: r.MakeClusterSpec(nodes+1, spec.CPU(16), spec.Geo(), spec.Zones(geoZonesStr)),
		// Uses CONFIGURE ZONE USING ... COPY FROM PARENT syntax.
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			firstAZ := geoZones[0]
			roachNodes := c.Range(1, nodes)
			gatewayNodes := c.Range(1, nodes/3)
			loadNode := c.Node(nodes + 1)

			c.Put(ctx, t.Cockroach(), "./cockroach", roachNodes)
			c.Put(ctx, t.DeprecatedWorkload(), "./workload", loadNode)
			c.Start(ctx, roachNodes)
			conn := c.Conn(ctx, 1)

			t.Status("running workload")
			m := c.NewMonitor(ctx, roachNodes)
			m.Go(func(ctx context.Context) error {
				secondary := " --secondary-indexes=" + strconv.Itoa(secondaryIndexes)
				initCmd := "./workload init indexes" + secondary + " {pgurl:1}"
				c.Run(ctx, loadNode, initCmd)

				// Set lease preferences so that all leases for the table are
				// located in the availability zone with the load generator.
				if !c.IsLocal() {
					t.L().Printf("setting lease preferences")
					if _, err := conn.ExecContext(ctx, fmt.Sprintf(`
						ALTER TABLE indexes.indexes
						CONFIGURE ZONE USING
						constraints = COPY FROM PARENT,
						lease_preferences = '[[+zone=%s]]'`,
						firstAZ,
					)); err != nil {
						return err
					}

					// Wait for ranges to rebalance across all three regions.
					t.L().Printf("checking replica balance")
					retryOpts := retry.Options{MaxBackoff: 15 * time.Second}
					for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
						WaitForUpdatedReplicationReport(ctx, t, conn)

						var ok bool
						if err := conn.QueryRowContext(ctx, `
							SELECT count(*) = 0
							FROM system.replication_critical_localities
							WHERE at_risk_ranges > 0
							AND locality LIKE '%region%'`,
						).Scan(&ok); err != nil {
							return err
						} else if ok {
							break
						}

						t.L().Printf("replicas still rebalancing...")
					}

					// Wait for leases to adhere to preferences, if they aren't
					// already.
					t.L().Printf("checking lease preferences")
					for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
						var ok bool
						if err := conn.QueryRowContext(ctx, `
							SELECT lease_holder <= $1
							FROM crdb_internal.ranges
							WHERE table_name = 'indexes'`,
							nodes/3,
						).Scan(&ok); err != nil {
							return err
						} else if ok {
							break
						}

						t.L().Printf("leases still rebalancing...")
					}
				}

				// Set the DistSender concurrency setting high enough so that no
				// requests get throttled. Add 2x headroom on top of this.
				conc := 16 * len(gatewayNodes)
				parallelWrites := (secondaryIndexes + 1) * conc
				distSenderConc := 2 * parallelWrites
				if _, err := conn.ExecContext(ctx, `
					SET CLUSTER SETTING kv.dist_sender.concurrency_limit = $1`,
					distSenderConc,
				); err != nil {
					return err
				}

				payload := " --payload=64"
				concurrency := ifLocal(c, "", " --concurrency="+strconv.Itoa(conc))
				duration := " --duration=" + ifLocal(c, "10s", "10m")
				runCmd := fmt.Sprintf("./workload run indexes --histograms="+t.PerfArtifactsDir()+"/stats.json"+
					payload+concurrency+duration+" {pgurl%s}", gatewayNodes)
				c.Run(ctx, loadNode, runCmd)
				return nil
			})
			m.Wait()
		},
	})
}

func registerIndexes(r registry.Registry) {
	registerNIndexes(r, 2)
}

func registerIndexesBench(r registry.Registry) {
	for i := 0; i <= 100; i++ {
		registerNIndexes(r, i)
	}
}
