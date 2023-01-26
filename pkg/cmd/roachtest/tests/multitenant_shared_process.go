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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
)

func registerMultiTenantSharedProcess(r registry.Registry) {
	crdbNodeCount := 4

	r.Add(registry.TestSpec{
		Name:    "multitenant/shared-process/basic",
		Owner:   registry.OwnerMultiTenant,
		Cluster: r.MakeClusterSpec(crdbNodeCount + 1),
		Timeout: 1 * time.Hour,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			var (
				appTenantID    = 2
				appTenantName  = "app"
				tpccWarehouses = 500
				crdbNodes      = c.Range(1, crdbNodeCount)
				workloadNode   = c.Node(crdbNodeCount + 1)
			)
			t.Status(`set up Unified Architecture Cluster`)
			c.Put(ctx, t.Cockroach(), "./cockroach", crdbNodes)
			c.Put(ctx, t.DeprecatedWorkload(), "./workload", workloadNode)
			c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), crdbNodes)

			sysConn := c.Conn(ctx, t.L(), crdbNodes.RandNode()[0])
			sysSQL := sqlutils.MakeSQLRunner(sysConn)

			sysSQL.Exec(t, fmt.Sprintf(`CREATE TENANT %s`, appTenantName))

			// Currently, a tenant has by default a 10m RU burst limit, which can be
			// reached during this test. To prevent RU limit throttling, add 10B RUs to
			// the tenant.
			sysSQL.Exec(t, `SELECT crdb_internal.update_tenant_resource_limits($1, 10000000000, 0,
		10000000000, now(), 0);`, appTenantID)

			t.Status(`initialize tpcc workload`)
			initCmd := fmt.Sprintf(`./workload init tpcc --data-loader import --warehouses %d {pgurl%s:%s}`,
				tpccWarehouses, crdbNodes, appTenantName)
			c.Run(ctx, workloadNode, initCmd)

			t.Status(`run tpcc workload`)
			runCmd := fmt.Sprintf(`./workload run tpcc --warehouses %d --tolerate-errors --duration 10m {pgurl%s:%s}`,
				tpccWarehouses, crdbNodes, appTenantName)
			c.Run(ctx, workloadNode, runCmd)
		},
	})
}
