// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/stretchr/testify/require"
)

func registerUATests(r registry.Registry) {
	{
		r.Add(registry.TestSpec{
			Name:             "ua/migration",
			Owner:            registry.OwnerSQLFoundations,
			Cluster:          r.MakeClusterSpec(3),
			CompatibleClouds: registry.AllClouds,
			Suites:           registry.Suites(registry.Nightly),
			Leases:           registry.MetamorphicLeases,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runUAMigration(ctx, t, c)
			},
		})
		r.Add(registry.TestSpec{
			Name:             "ua/cluster-with-t2-system-tenant",
			Owner:            registry.OwnerSQLFoundations,
			Cluster:          r.MakeClusterSpec(3),
			CompatibleClouds: registry.AllClouds,
			Suites:           registry.Suites(registry.Nightly),
			Leases:           registry.MetamorphicLeases,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runSystemTenantTwo(ctx, t, c)
			},
		})
	}
}

// - Start a cluster
// - Bootstrap TenantTwo
// - Restart a node with environment variable
// - Keyspace validations (nothing should be there other than /Tenant/2/Table/*)
func runUAMigration(ctx context.Context, t test.Test, c cluster.Cluster) {
	opts := option.DefaultStartOpts()
	settings := install.MakeClusterSettings()
	c.Start(ctx, t.L(), opts, settings, c.All())

	dbOne := c.Conn(ctx, t.L(), 1)
	defer dbOne.Close()

	// Create TenantTwo
	query := `select crdb_internal.create_tenant('{"id": 2, "name": "system2", "service_mode": "shared"}'::jsonb);`
	_, err := dbOne.ExecContext(ctx, query)
	require.NoError(t, err, "cannot create TenantTwo")

	nodeTwo := c.Nodes(2)

	// Stop node 2
	stopOpts := option.DefaultStopOpts()
	c.Stop(ctx, t.L(), stopOpts, nodeTwo)

	// Start node 2
	settings.Env = append(settings.Env, "COCKROACH_EXPERIMENTAL_UA=true")
	c.Start(ctx, t.L(), opts, settings, nodeTwo)

	numWarehouses := 10

	// nodeOne := c.Nodes(1)

	// TODO(shubham): run workloads in parallel too

	// Run workloads
	workloadNode := nodeTwo
	init := roachtestutil.NewCommand("./cockroach workload init tpcc --split").
		Arg("{pgurl%s}", workloadNode).
		Flag("warehouses", numWarehouses)

	runDuration := 1 * time.Minute

	run := roachtestutil.NewCommand("./cockroach workload run tpcc").
		Arg("{pgurl%s}", workloadNode).
		Flag("warehouses", numWarehouses).
		Flag("duration", runDuration).
		Option("tolerate-errors")
	err = c.RunE(ctx, option.WithNodes(workloadNode), init.String())
	require.NoError(t, err, "failed to run tpcc init")

	err = c.RunE(ctx, option.WithNodes(workloadNode), run.String())
	require.NoError(t, err, "failed to run tpcc run")

	dbTwo := c.Conn(ctx, t.L(), 2)
	defer dbTwo.Close()

	// Verify if we are able to create a virtual cluster on TenantTwo as its a
	// system tenant
	query = `CREATE VIRTUAL CLUSTER 'foo'`
	_, err = dbTwo.ExecContext(ctx, query)
	require.NoError(t, err, "cannot create tenant foo")

	// Print the content of tenants system table in TenantTwo
	query = `SELECT id, name FROM system.tenants`
	rows, err := dbTwo.QueryContext(ctx, query)
	require.NoError(t, err, "failed to query system.tenants")
	defer rows.Close()
	for rows.Next() {
		var id int
		var name string
		err := rows.Scan(&id, &name)
		require.NoError(t, err, "couldn't do SCAN")
		t.L().Printf("tenant : %d, %s", id, name)
	}
	require.NoError(t, rows.Err(), "some failure while reading result")

	// Verify we didn't write to anywhere in TenantTwo's keyspace except its
	// table keyspace
	query = `SELECT count(1) FROM crdb_internal.scan(
		crdb_internal.tenant_span(2)[1], crdb_internal.table_span(1)[1]);`
	rows, err = dbTwo.QueryContext(ctx, query)
	require.NoError(t, err, "failed to run crdb_internal.scan")
	defer rows.Close()
	for rows.Next() {
		var id int
		err := rows.Scan(&id)
		require.NoError(t, err, "couldn't SCAN count")
		require.Equal(t, 0, id)
	}
	require.NoError(t, rows.Err(), "some failure while reading")
}

// Run workload with tenant {2} as system tenant
func runSystemTenantTwo(ctx context.Context, t test.Test, c cluster.Cluster) {
	opts := option.DefaultStartOpts()
	settings := install.MakeClusterSettings()
	settings.Env = append(settings.Env, "COCKROACH_EXPERIMENTAL_UA=true")

	c.Start(ctx, t.L(), opts, settings, c.All())
	db := c.Conn(ctx, t.L(), 1)
	defer db.Close()

	// Workload settings
	roachNodes, workloadNodes := c.Nodes(2), c.Nodes(1)
	numWarehouses := 10
	duration := time.Minute * 10

	init := roachtestutil.NewCommand("./cockroach workload init tpcc").
		Arg("{pgurl%s}", roachNodes).
		Option("split").
		Flag("warehouses", numWarehouses)
	err := c.RunE(ctx, option.WithNodes(workloadNodes), init.String())
	require.NoError(t, err, "failed to run tpcc init")

	run := roachtestutil.NewCommand("./cockroach workload run tpcc").
		Arg("{pgurl%s}", roachNodes).
		Flag("duration", duration).
		Flag("warehouses", numWarehouses).
		Option("tolerate-errors")
	err = c.RunE(ctx, option.WithNodes(workloadNodes), run.String())
	require.NoError(t, err, "failed to run tpcc run")
}
