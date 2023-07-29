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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func registerRowLevelTTLDuringTPCC(r registry.Registry) {
	const nodes = 7
	var clusterSpec = spec.CPU(4)
	const warehouses = 1500
	const activeWarehouses = 100
	const duration = 30 * time.Minute
	r.Add(makeRowLevelTTLDuringTPCC(r.MakeClusterSpec(nodes, clusterSpec), warehouses, activeWarehouses, duration, false /* expiredRows */))
	r.Add(makeRowLevelTTLDuringTPCC(r.MakeClusterSpec(nodes, clusterSpec), warehouses, activeWarehouses, duration, true /* expiredRows */))
}

func makeRowLevelTTLDuringTPCC(
	spec spec.ClusterSpec, warehouses, activeWarehouses int, length time.Duration, expiredRows bool,
) registry.TestSpec {
	return registry.TestSpec{
		Name:      fmt.Sprintf("row-level-ttl/during/tpcc/expired-rows=%t", expiredRows),
		Owner:     registry.OwnerSQLFoundations,
		Benchmark: true,
		Cluster:   spec,
		Leases:    registry.MetamorphicLeases,
		Timeout:   length * 3,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runTPCC(ctx, t, c, tpccOptions{
				Warehouses: warehouses,
				// We limit the number of workers because the default results in a lot
				// of connections which can lead to OOM issues (see #40566).
				ExtraRunArgs: fmt.Sprintf("--wait=false --tolerate-errors --max-rate=100 --active-warehouses=%d --workers=%d", activeWarehouses, warehouses),
				// The expired-rows test will delete rows from the order_line table, so
				// the post run checks are expected to fail.
				SkipPostRunCheck: expiredRows,
				During: func(ctx context.Context) error {
					nowMinute := timeutil.Now().Minute()
					scheduledMinute := (nowMinute + 10) % 60
					var expirationExpr string
					if expiredRows {
						expirationExpr = `'((ol_delivery_d::TIMESTAMP) + INTERVAL ''1 days'') AT TIME ZONE ''UTC'''`
					} else {
						// The TPCC fixtures have dates from 2006 for the ol_delivery_d column.
						expirationExpr = `'((ol_delivery_d::TIMESTAMP) + INTERVAL ''1000 years'') AT TIME ZONE ''UTC'''`
					}
					ttlStatement := fmt.Sprintf(`
					ALTER TABLE tpcc.public.order_line SET (
					    ttl_expiration_expression=%s,
					    ttl_job_cron='%d * * * *'
					);`, expirationExpr, scheduledMinute,
					)

					if err := runAndLogStmts(ctx, t, c, "enable-ttl", []string{ttlStatement}); err != nil {
						return err
					}
					return nil
				},
				Duration:  length,
				SetupType: usingImport,
			})
		},
	}
}
