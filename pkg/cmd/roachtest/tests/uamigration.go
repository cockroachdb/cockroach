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
		r.Add(registry.TestSpec{
			Name:             "ua/span-config-conflict",
			Owner:            registry.OwnerSQLFoundations,
			Cluster:          r.MakeClusterSpec(3),
			CompatibleClouds: registry.AllClouds,
			Suites:           registry.Suites(registry.Nightly),
			Leases:           registry.MetamorphicLeases,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runSpanConfigConflictsTest(ctx, t, c)
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

	db := c.Conn(ctx, t.L(), 1)
	defer db.Close()

	query := `select crdb_internal.create_tenant('{"id": 2, "name": "system2", "service_mode": "shared"}'::jsonb);`
	_, err := db.ExecContext(ctx, query)
	require.NoError(t, err, "cannot create TenantTwo")

	stopOpts := option.DefaultStopOpts()
	c.Stop(ctx, t.L(), stopOpts, c.Nodes(2))

	settings.Env = append(settings.Env, "COCKROACH_EXPERIMENTAL_UA=true")
	c.Start(ctx, t.L(), opts, settings, c.Nodes(2))

	numWarehouses := 10

	roachNodes := c.Nodes(2)
	workloadNode := c.Nodes(2)
	init := roachtestutil.NewCommand("./cockroach workload init tpcc --split").
		Arg("{pgurl%s}", roachNodes).
		Flag("warehouses", numWarehouses)
	run := roachtestutil.NewCommand("./cockroach workload run tpcc --duration 10m").
		Arg("{pgurl%s}", roachNodes).
		Flag("warehouses", numWarehouses).
		Option("tolerate-errors")
	err = c.RunE(ctx, option.WithNodes(workloadNode), init.String())
	require.NoError(t, err, "failed to run tpcc init")

	err = c.RunE(ctx, option.WithNodes(workloadNode), run.String())
	require.NoError(t, err, "failed to run tpcc run")

	query = `CREATE VIRTUAL CLUSTER 'foo'`
	_, err = db.ExecContext(ctx, query)
	require.NoError(t, err, "cannot create foo")

	query = `SELECT id, name FROM system.tenants`
	rows, err := db.QueryContext(ctx, query)
	require.NoError(t, err, "failed to query system.tenants")
	defer rows.Close()
	for rows.Next() {
		var id int
		var name string
		err := rows.Scan(&id, &name)
		require.NoError(t, err, "couldn't do SCAN")
		t.L().Printf("SYSTEM.TENANTS : %d, %s", id, name)
	}
	require.NoError(t, rows.Err(), "some failure while reading")

	dbTwo := c.Conn(ctx, t.L(), 2)
	defer dbTwo.Close()
	query = `select count(1) from crdb_internal.scan(crdb_internal.tenant_span(2)[1], crdb_internal.table_span(1)[1]);`
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

func runSpanConfigConflictsTest(ctx context.Context, t test.Test, c cluster.Cluster) {
	opts := option.DefaultStartOpts()
	settings := install.MakeClusterSettings()
	c.Start(ctx, t.L(), opts, settings, c.All())

	// Connection to Node1
	db := c.Conn(ctx, t.L(), 1)
	defer db.Close()

	// Create TenantTwo
	query := `select crdb_internal.create_tenant('{"id": 2, "name": "system2", "service_mode": "shared"}'::jsonb);`
	_, err := db.ExecContext(ctx, query)
	require.NoError(t, err, "cannot create TenantTwo")

	// Stop Node2
	// stopOpts := option.DefaultStopOpts()
	// c.Stop(ctx, t.L(), stopOpts, c.Nodes(2))

	// Reconfigure Node2 to use TenantTwo as system tenant
	// settings.Env = append(settings.Env, "COCKROACH_EXPERIMENTAL_UA=true")
	// c.Start(ctx, t.L(), opts, settings, c.Nodes(2))

	numWarehouses := 10
	roachNodes, workloadNode := c.Nodes(2), c.Nodes(2)
	duration := time.Minute * 10

	// Start workload on Node2 running with TenantTwo as system tenant
	init := roachtestutil.NewCommand("./cockroach workload init tpcc").
		Arg("{pgurl%s}", roachNodes).
		Option("split").
		Flag("warehouses", numWarehouses)
	run := roachtestutil.NewCommand("./cockroach workload run tpcc").
		Arg("{pgurl%s}", roachNodes).
		Flag("duration", duration).
		Flag("warehouses", numWarehouses).
		Option("tolerate-errors")

	err = c.RunE(ctx, option.WithNodes(workloadNode), init.String())
	require.NoError(t, err, "failed to run tpcc init")

	err = c.RunE(ctx, option.WithNodes(workloadNode), run.String())
	require.NoError(t, err, "failed to run tpcc run")
}
