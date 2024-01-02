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
	gosql "database/sql"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
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
	secure := true
	c.Put(ctx, t.DeprecatedWorkload(), "./workload", c.Node(1))
	start := func() {
		c.Start(ctx, t.L(), option.DefaultStartOptsNoBackups(), install.MakeClusterSettings(install.SecureOption(secure)), c.All())
	}
	start()

	setupNames := []string{"single-tenant", "multi-tenant"}
	const numRunsPerQuery = 3
	perfHelper := newTpchVecPerfHelper(setupNames)

	// runTPCH runs all TPCH queries on a single setup. It first restores the
	// TPCH dataset using the provided connection and then runs each TPCH query
	// one at a time (using the given url as a parameter to the 'workload run'
	// command). The runtimes are accumulated in the perf helper.
	runTPCH := func(conn *gosql.DB, url string, setupIdx int) {
		setting := fmt.Sprintf("SET CLUSTER SETTING sql.distsql.direct_columnar_scans.enabled = %t", enableDirectScans)
		t.Status(setting)
		if _, err := conn.Exec(setting); err != nil {
			t.Fatal(err)
		}
		t.Status("restoring TPCH dataset for Scale Factor 1 in ", setupNames[setupIdx])
		if err := loadTPCHDataset(
			ctx, t, c, conn, 1 /* sf */, c.NewMonitor(ctx), c.All(), false /* disableMergeQueue */, secure,
		); err != nil {
			t.Fatal(err)
		}
		for queryNum := 1; queryNum <= tpch.NumQueries; queryNum++ {
			cmd := fmt.Sprintf("./workload run tpch %s --secure "+
				"--concurrency=1 --db=tpch --max-ops=%d --queries=%d {pgurl:1}",
				url, numRunsPerQuery, queryNum)
			result, err := c.RunWithDetailsSingleNode(ctx, t.L(), c.Node(1), cmd)
			workloadOutput := result.Stdout + result.Stderr
			t.L().Printf(workloadOutput)
			if err != nil {
				t.Fatal(err)
			}
			perfHelper.parseQueryOutput(t, []byte(workloadOutput), setupIdx)
		}
	}

	// First, use the cluster as a single tenant deployment. It is important to
	// not create the tenant yet so that the certs directory is not overwritten.
	singleTenantConn := c.Conn(ctx, t.L(), 1)
	// Disable merge queue in the system tenant.
	if _, err := singleTenantConn.Exec("SET CLUSTER SETTING kv.range_merge.queue_enabled = false;"); err != nil {
		t.Fatal(err)
	}
	runTPCH(singleTenantConn, "" /* url */, 0 /* setupIdx */)

	// Restart and wipe the cluster to remove advantage of the second TPCH run.
	c.Stop(ctx, t.L(), option.DefaultStopOpts())
	c.Wipe(ctx, secure)
	start()
	singleTenantConn = c.Conn(ctx, t.L(), 1)
	// Disable merge queue in the system tenant.
	if _, err := singleTenantConn.Exec("SET CLUSTER SETTING kv.range_merge.queue_enabled = false;"); err != nil {
		t.Fatal(err)
	}

	// Now we create a tenant and run all TPCH queries within it.
	if sharedProcess {
		db := createInMemoryTenantWithConn(ctx, t, c, appTenantName, c.All(), true /* secure */)
		defer db.Close()
		url := fmt.Sprintf("{pgurl:1:%s}", appTenantName)
		runTPCH(db, url, 1 /* setupIdx */)
	} else {
		const (
			tenantID       = 123
			tenantHTTPPort = 8081
			tenantSQLPort  = 30258
			tenantNode     = 1
		)
		_, err := singleTenantConn.Exec(`SELECT crdb_internal.create_tenant($1::INT)`, tenantID)
		if err != nil {
			t.Fatal(err)
		}
		tenant := createTenantNode(ctx, t, c, c.All(), tenantID, tenantNode, tenantHTTPPort, tenantSQLPort)
		tenant.start(ctx, t, c, "./cockroach")
		multiTenantConn, err := gosql.Open("postgres", tenant.pgURL)
		if err != nil {
			t.Fatal(err)
		}
		// Allow the tenant to be able to split tables. We need to run a dummy
		// split in order to make sure the capability is propagated before
		// starting the test, otherwise importing tpch may fail.
		_, err = singleTenantConn.Exec(
			`ALTER TENANT [$1] SET CLUSTER SETTING sql.split_at.allow_for_secondary_tenant.enabled=true`, tenantID)
		if err != nil {
			t.Fatal(err)
		}
		_, err = singleTenantConn.Exec(`ALTER TENANT [$1] GRANT CAPABILITY can_admin_split=true`, tenantID)
		if err != nil {
			t.Fatal(err)
		}
		testutils.SucceedsSoon(t, func() error {
			if _, err := multiTenantConn.Exec(`CREATE TABLE IF NOT EXISTS dummysplit (a INT)`); err != nil {
				return err
			}
			_, err = multiTenantConn.Exec(`ALTER TABLE dummysplit SPLIT AT VALUES (0)`)
			return err
		})
		runTPCH(multiTenantConn, "'"+tenant.secureURL()+"'", 1 /* setupIdx */)
	}

	// Analyze the runtimes of both setups.
	perfHelper.compareSetups(t, numRunsPerQuery, nil /* timesCallback */)
}

func registerMultiTenantTPCH(r registry.Registry) {
	for _, sharedProcess := range []bool{false, true} {
		for _, enableDirectScans := range []bool{false, true} {
			sharedProcess, enableDirectScans := sharedProcess, enableDirectScans
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
				Name:             name,
				Owner:            registry.OwnerSQLQueries,
				Benchmark:        true,
				Cluster:          r.MakeClusterSpec(1 /* nodeCount */),
				CompatibleClouds: registry.AllExceptAWS,
				Suites:           registry.Suites(registry.Nightly),
				Leases:           registry.MetamorphicLeases,
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
					runMultiTenantTPCH(ctx, t, c, enableDirectScans, sharedProcess)
				},
			})
		}
	}
}
