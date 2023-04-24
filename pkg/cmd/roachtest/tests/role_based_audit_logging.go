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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
)

func registerRoleBasedAuditLogging(r registry.Registry) {
	const warehouses = 100
	const length = time.Minute * 5

	r.Add(registry.TestSpec{
		Name:            "role-based-audit-logging/benchmark-empty-config",
		Owner:           registry.OwnerClusterObs,
		Cluster:         r.MakeClusterSpec(4, spec.CPU(16)),
		RequiresLicense: true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			c.Put(ctx, t.Cockroach(), "./cockroach")
			c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.Range(1, c.Spec().NodeCount-1))
			c.Put(ctx, t.DeprecatedWorkload(), "./workload", c.Node(c.Spec().NodeCount))

			setRoleBasedAuditConfig(ctx, t, c, 0)
			runTPCC(ctx, t, c, tpccOptions{
				Warehouses: warehouses,
				// We limit the number of workers because the default results in a lot
				// of connections which can lead to OOM issues (see #40566).
				ExtraRunArgs:                  fmt.Sprintf("--wait=false --workers=%d", warehouses),
				Duration:                      length,
				SetupType:                     usingImport,
				DisableDefaultScheduledBackup: true,
				DisablePrometheus:             true,
			})
		},
	})
	r.Add(registry.TestSpec{
		Name:            "role-based-audit-logging/benchmark-small-config-size",
		Owner:           registry.OwnerClusterObs,
		Cluster:         r.MakeClusterSpec(4, spec.CPU(16)),
		RequiresLicense: true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			c.Put(ctx, t.Cockroach(), "./cockroach")
			c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.Range(1, c.Spec().NodeCount-1))
			c.Put(ctx, t.DeprecatedWorkload(), "./workload", c.Node(c.Spec().NodeCount))

			setRoleBasedAuditConfig(ctx, t, c, 50)
			runTPCC(ctx, t, c, tpccOptions{
				Warehouses: warehouses,
				// We limit the number of workers because the default results in a lot
				// of connections which can lead to OOM issues (see #40566).
				ExtraRunArgs:                  fmt.Sprintf("--wait=false --workers=%d", warehouses),
				Duration:                      length,
				SetupType:                     usingImport,
				DisableDefaultScheduledBackup: true,
				DisablePrometheus:             true,
			})
		},
	})
	r.Add(registry.TestSpec{
		Name:            "role-based-audit-logging/benchmark-large-config-size",
		Owner:           registry.OwnerClusterObs,
		Cluster:         r.MakeClusterSpec(4, spec.CPU(16)),
		RequiresLicense: true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			c.Put(ctx, t.Cockroach(), "./cockroach")
			c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.Range(1, c.Spec().NodeCount-1))
			c.Put(ctx, t.DeprecatedWorkload(), "./workload", c.Node(c.Spec().NodeCount))

			setRoleBasedAuditConfig(ctx, t, c, 200)
			runTPCC(ctx, t, c, tpccOptions{
				Warehouses: warehouses,
				// We limit the number of workers because the default results in a lot
				// of connections which can lead to OOM issues (see #40566).
				ExtraRunArgs:                  fmt.Sprintf("--wait=false --workers=%d", warehouses),
				Duration:                      length,
				SetupType:                     usingImport,
				DisableDefaultScheduledBackup: true,
				DisablePrometheus:             true,
			})
		},
	})
}

func setRoleBasedAuditConfig(ctx context.Context, t test.Test, c cluster.Cluster, numSettings int) {
	db := c.Conn(ctx, t.L(), 1)
	defer db.Close()

	configString := ""
	if numSettings > 0 {
		var setting string
		// Create numSettings - 1 audit settings.
		for i := 1; i < numSettings; i++ {
			setting = fmt.Sprintf("test_role_%d ALL\n", i)
			configString += setting
		}
		// Create a final catch-all audit setting. This will apply for any user, so we don't need to grant any role.
		// We add this setting at the end to test the worst case scenario on the linear match.
		configString += "ALL ALL\n"
	}
	if _, err := db.Exec(fmt.Sprintf("SET CLUSTER SETTING sql.log.user_audit = '%s'", configString)); err != nil {
		t.Fatal(err)
	}
}
