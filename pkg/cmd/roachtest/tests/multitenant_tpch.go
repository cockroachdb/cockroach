// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/workload/tpch"
)

// runMultiTenantTPCH runs TPCH queries on a cluster that is first used as a
// single-tenant deployment followed by a run of all queries in a multi-tenant
// deployment with a single SQL instance.
func runMultiTenantTPCH(
	ctx context.Context, t test.Test, c cluster.Cluster, enableDirectScans bool, sharedProcess bool,
) {
	start := func() {
		c.Start(ctx, t.L(), option.NewStartOpts(option.NoBackupSchedule), install.MakeClusterSettings(), c.All())
	}
	start()

	setupNames := []string{"single-tenant", "multi-tenant"}
	const numRunsPerQuery = 3
	perfHelper := newTpchVecPerfHelper(setupNames)

	// runTPCH runs all TPCH queries on a single setup. It first restores the
	// TPCH dataset using the provided connection and then runs each TPCH query
	// one at a time (using the given url as a parameter to the 'workload run'
	// command). The runtimes are accumulated in the perf helper.
	runTPCH := func(node int, virtualClusterName string, setupIdx int) {
		conn := c.Conn(ctx, t.L(), node, option.VirtualClusterName(virtualClusterName))
		setting := fmt.Sprintf("SET CLUSTER SETTING sql.distsql.direct_columnar_scans.enabled = %t", enableDirectScans)
		t.Status(setting)
		if _, err := conn.Exec(setting); err != nil {
			t.Fatal(err)
		}
		t.Status("restoring TPCH dataset for Scale Factor 1 in ", setupNames[setupIdx])
		if err := loadTPCHDataset(
			ctx, t, c, conn, 1 /* sf */, c.NewMonitor(ctx), c.All(), false, /* disableMergeQueue */
		); err != nil {
			t.Fatal(err)
		}
		for queryNum := 1; queryNum <= tpch.NumQueries; queryNum++ {
			cmd := roachtestutil.NewCommand("%s workload run tpch", test.DefaultCockroachPath).
				Flag("concurrency", 1).
				Flag("db", "tpch").
				Flag("max-ops", numRunsPerQuery).
				Flag("queries", queryNum).
				Arg("{pgurl:%d:%s}", node, virtualClusterName)
			result, err := c.RunWithDetailsSingleNode(ctx, t.L(), option.WithNodes(c.Node(1)), cmd.String())
			workloadOutput := result.Stdout + result.Stderr
			t.L().Printf(workloadOutput)
			if err != nil {
				t.Fatal(err)
			}
			perfHelper.parseQueryOutput(t, []byte(workloadOutput), setupIdx)
		}
	}

	systemConn := c.Conn(ctx, t.L(), 1)
	defer systemConn.Close()

	// First, use the cluster as a single tenant deployment.
	gatewayNode := c.All().RandNode()[0]
	// Disable merge queue in the system tenant.
	if _, err := systemConn.Exec("SET CLUSTER SETTING kv.range_merge.queue_enabled = false;"); err != nil {
		t.Fatal(err)
	}
	runTPCH(gatewayNode, install.SystemInterfaceName, 0 /* setupIdx */)

	// Restart and wipe the cluster to remove advantage of the second TPCH run.
	c.Wipe(ctx)
	start()
	// Disable merge queue in the system tenant.
	if _, err := systemConn.Exec("SET CLUSTER SETTING kv.range_merge.queue_enabled = false;"); err != nil {
		t.Fatal(err)
	}

	// Now we create a tenant and run all TPCH queries within it.
	startOpts := option.StartSharedVirtualClusterOpts(appTenantName)
	var separateProcessNode option.NodeListOption
	if !sharedProcess {
		separateProcessNode = c.All().RandNode()
		gatewayNode = separateProcessNode[0]
		startOpts = option.StartVirtualClusterOpts(appTenantName, separateProcessNode)
	}
	c.StartServiceForVirtualCluster(
		ctx, t.L(), startOpts, install.MakeClusterSettings(),
	)

	// Allow the tenant to be able to split tables. We need to run a dummy
	// split in order to make sure the capability is propagated before
	// starting the test, otherwise importing tpch may fail.
	if _, err := systemConn.Exec(
		`ALTER TENANT $1 SET CLUSTER SETTING sql.split_at.allow_for_secondary_tenant.enabled=true`, appTenantName,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := systemConn.Exec(
		`ALTER TENANT $1 GRANT CAPABILITY can_admin_split=true`, appTenantName,
	); err != nil {
		t.Fatal(err)
	}

	virtualClusterConn := c.Conn(
		ctx, t.L(), gatewayNode, option.VirtualClusterName(appTenantName),
	)
	defer virtualClusterConn.Close()

	testutils.SucceedsSoon(t, func() error {
		if _, err := virtualClusterConn.Exec(`CREATE TABLE IF NOT EXISTS dummysplit (a INT)`); err != nil {
			return err
		}
		_, err := virtualClusterConn.Exec(`ALTER TABLE dummysplit SPLIT AT VALUES (0)`)
		return err
	})

	runTPCH(gatewayNode, appTenantName, 1 /* setupIdx */)

	// Analyze the runtimes of both setups.
	perfHelper.compareSetups(t, numRunsPerQuery, nil /* timesCallback */)
}

func registerMultiTenantTPCH(r registry.Registry) {
	for _, sharedProcess := range []bool{false, true} {
		for _, enableDirectScans := range []bool{false, true} {
			name := "multitenant/tpch"
			if sharedProcess {
				name += "/shared_process"
			} else {
				name += "/separate_process"
			}
			if enableDirectScans {
				name += "/enable_direct_scans"
			}
			r.Add(registry.TestSpec{
				Name:      name,
				Owner:     registry.OwnerSQLQueries,
				Benchmark: true,
				Cluster:   r.MakeClusterSpec(1 /* nodeCount */),
				// Uses gs://cockroach-fixtures-us-east1. See:
				// https://github.com/cockroachdb/cockroach/issues/105968
				CompatibleClouds: registry.Clouds(spec.GCE, spec.Local),
				Suites:           registry.Suites(registry.Nightly),
				Leases:           registry.MetamorphicLeases,
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
					runMultiTenantTPCH(ctx, t, c, enableDirectScans, sharedProcess)
				},
			})
		}
	}
}
