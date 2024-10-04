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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
)

const appTenantName = "app"

func registerMultiTenantSharedProcess(r registry.Registry) {
	crdbNodeCount := 4

	r.Add(registry.TestSpec{
		Name:             "multitenant/shared-process/basic",
		Owner:            registry.OwnerDisasterRecovery,
		Cluster:          r.MakeClusterSpec(crdbNodeCount+1, spec.WorkloadNode()),
		Leases:           registry.MetamorphicLeases,
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		Timeout:          1 * time.Hour,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			tpccWarehouses := 500
			t.Status(`set up Unified Architecture Cluster`)

			// In order to observe the app tenant's db console, create a secure
			// cluster and add Admin roles to the system and app tenant.
			clusterSettings := install.MakeClusterSettings()
			c.Start(ctx, t.L(), option.DefaultStartOpts(), clusterSettings, c.CRDBNodes())

			startOpts := option.StartSharedVirtualClusterOpts(appTenantName)
			c.StartServiceForVirtualCluster(ctx, t.L(), startOpts, clusterSettings)

			t.Status(`initialize tpcc workload`)
			initCmd := fmt.Sprintf(`./cockroach workload init tpcc --data-loader import --warehouses %d {pgurl%s:%s}`,
				tpccWarehouses, c.CRDBNodes(), appTenantName)
			c.Run(ctx, option.WithNodes(c.WorkloadNode()), initCmd)

			t.Status(`run tpcc workload`)
			runCmd := fmt.Sprintf(`./cockroach workload run tpcc --warehouses %d --tolerate-errors --duration 10m {pgurl%s:%s}`,
				tpccWarehouses, c.CRDBNodes(), appTenantName)
			c.Run(ctx, option.WithNodes(c.WorkloadNode()), runCmd)
		},
	})
}
