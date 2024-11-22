// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/stretchr/testify/require"
)

func registerUATests(r registry.Registry) {
	{
		r.Add(registry.TestSpec{
			Name:             "uamigration",
			Owner:            registry.OwnerSQLFoundations,
			Cluster:          r.MakeClusterSpec(3),
			CompatibleClouds: registry.AllClouds,
			Suites:           registry.Suites(registry.Nightly),
			Leases:           registry.MetamorphicLeases,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runUAMigration(ctx, t, c)
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
	c.Stop(ctx, t.L(), stopOpts, c.All())

	settings.Env = append(settings.Env, "COCKROACH_EXPERIMENTAL_UA=true")
	c.Start(ctx, t.L(), opts, settings, c.All())

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
}
