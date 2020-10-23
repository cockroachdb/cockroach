// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/retry"
)

func registerNIndexes(r *testRegistry, secondaryIndexes int) {
	const nodes = 6
	geoZones := []string{"us-east1-b", "us-west1-b", "europe-west2-b"}
	if cloud == aws {
		geoZones = []string{"us-east-2b", "us-west-1a", "eu-west-1a"}
	}
	geoZonesStr := strings.Join(geoZones, ",")
	r.Add(testSpec{
		Name:    fmt.Sprintf("indexes/%d/nodes=%d/multi-region", secondaryIndexes, nodes),
		Owner:   OwnerKV,
		Cluster: makeClusterSpec(nodes+1, cpu(16), geo(), zones(geoZonesStr)),
		// Uses CONFIGURE ZONE USING ... COPY FROM PARENT syntax.
		MinVersion: `v19.1.0`,
		Run: func(ctx context.Context, t *test, c *cluster) {
			firstAZ := geoZones[0]
			roachNodes := c.Range(1, nodes)
			gatewayNodes := c.Range(1, nodes/3)
			loadNode := c.Node(nodes + 1)

			c.Put(ctx, cockroach, "./cockroach", roachNodes)
			c.Put(ctx, workload, "./workload", loadNode)
			c.Start(ctx, t, roachNodes)
			conn := c.Conn(ctx, 1)

			t.Status("running workload")
			m := newMonitor(ctx, c, roachNodes)
			m.Go(func(ctx context.Context) error {
				secondary := " --secondary-indexes=" + strconv.Itoa(secondaryIndexes)
				initCmd := "./workload init indexes" + secondary + " {pgurl:1}"
				c.Run(ctx, loadNode, initCmd)

				// Set lease preferences so that all leases for the table are
				// located in the availability zone with the load generator.
				if !local {
					t.l.Printf("setting lease preferences")
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
					t.l.Printf("checking replica balance")
					retryOpts := retry.Options{MaxBackoff: 15 * time.Second}
					for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
						waitForUpdatedReplicationReport(ctx, t, conn)

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

						t.l.Printf("replicas still rebalancing...")
					}

					// Wait for leases to adhere to preferences, if they aren't
					// already.
					t.l.Printf("checking lease preferences")
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

						t.l.Printf("leases still rebalancing...")
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
				concurrency := ifLocal("", " --concurrency="+strconv.Itoa(conc))
				duration := " --duration=" + ifLocal("10s", "10m")
				runCmd := fmt.Sprintf("./workload run indexes --histograms="+perfArtifactsDir+"/stats.json"+
					payload+concurrency+duration+" {pgurl%s}", gatewayNodes)
				c.Run(ctx, loadNode, runCmd)
				return nil
			})
			m.Wait()
		},
	})
}

func registerIndexes(r *testRegistry) {
	registerNIndexes(r, 2)
}

func registerIndexesBench(r *testRegistry) {
	for i := 0; i <= 100; i++ {
		registerNIndexes(r, i)
	}
}
