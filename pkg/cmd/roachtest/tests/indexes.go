// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
)

func registerNIndexes(r registry.Registry, secondaryIndexes int) {
	const nodes = 6
	gceGeoZones := []string{"us-east1-b", "us-west1-b", "europe-west2-b"}
	awsGeoZones := []string{"us-east-2b", "us-west-1a", "eu-west-1a"}
	r.Add(registry.TestSpec{
		Name:      fmt.Sprintf("indexes/%d/nodes=%d/multi-region", secondaryIndexes, nodes),
		Owner:     registry.OwnerKV,
		Benchmark: true,
		Cluster: r.MakeClusterSpec(
			nodes+1,
			spec.CPU(16),
			spec.WorkloadNode(),
			spec.WorkloadNodeCPU(16),
			spec.Geo(),
			spec.GCEZones(strings.Join(gceGeoZones, ",")),
			spec.AWSZones(strings.Join(awsGeoZones, ",")),
		),
		// TODO(radu): enable this test on AWS.
		CompatibleClouds:           registry.OnlyGCE,
		Suites:                     registry.Suites(registry.Nightly),
		RequiresDeprecatedWorkload: true, // uses indexes
		// Uses CONFIGURE ZONE USING ... COPY FROM PARENT syntax.
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			firstAZ := gceGeoZones[0]
			if c.Cloud() == spec.AWS {
				firstAZ = awsGeoZones[0]
			}
			gatewayNodes := c.Range(1, nodes/3)

			c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.CRDBNodes())
			conn := c.Conn(ctx, t.L(), 1)

			t.Status("running workload")
			m := c.NewMonitor(ctx, c.CRDBNodes())
			m.Go(func(ctx context.Context) error {
				secondary := " --secondary-indexes=" + strconv.Itoa(secondaryIndexes)
				initCmd := "./workload init indexes" + secondary + " {pgurl:1}"
				c.Run(ctx, option.WithNodes(c.WorkloadNode()), initCmd)

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
						roachtestutil.WaitForUpdatedReplicationReport(ctx, t, conn)

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
							FROM [SHOW RANGES FROM TABLE indexes.indexes WITH DETAILS]`,
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
				concurrency := roachtestutil.IfLocal(c, "", " --concurrency="+strconv.Itoa(conc))
				duration := " --duration=" + roachtestutil.IfLocal(c, "10s", "10m")
				labels := map[string]string{
					"concurrency":     fmt.Sprintf("%d", conc),
					"parallel_writes": fmt.Sprintf("%d", parallelWrites),
				}
				runCmd := fmt.Sprintf("./workload run indexes %s %s %s %s {pgurl%s}", roachtestutil.GetWorkloadHistogramArgs(t, c, labels), payload, concurrency, duration, gatewayNodes)
				c.Run(ctx, option.WithNodes(c.WorkloadNode()), runCmd)
				return nil
			})
			m.Wait()
		},
	})
}

func registerIndexes(r registry.Registry) {
	registerNIndexes(r, 2)
}
