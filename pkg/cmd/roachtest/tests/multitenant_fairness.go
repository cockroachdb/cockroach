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
	"sync"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/stretchr/testify/require"
)

func registerMultiTenantFairness(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:              "multitenant/kv-fairness",
		Cluster:           r.MakeClusterSpec(5),
		Owner:             registry.OwnerSQLQueries,
		NonReleaseBlocker: false,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runMultiTenantKVFairness(ctx, t, c, *t.BuildVersion())
		},
	})
	r.Add(registry.TestSpec{
		Name:              "multitenant/store-fairness",
		Cluster:           r.MakeClusterSpec(5),
		Owner:             registry.OwnerSQLQueries,
		NonReleaseBlocker: false,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runMultiTenantStoreFairness(ctx, t, c, *t.BuildVersion())
		},
	})
}

// Test that the kvserver fairly distributes CPU token on a highly concurrent 4 MT read only
// workload.
func runMultiTenantKVFairness(ctx context.Context, t test.Test, c cluster.Cluster, v version.Version) {
	c.Put(ctx, t.Cockroach(), "./cockroach")
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.Node(1))

	kvAddrs, err := c.ExternalAddr(ctx, t.L(), c.Node(1))
	require.NoError(t, err)

	const (
		tenantBaseID       = 10
		tenantBaseHTTPPort = 8080
		tenantBaseSQLPort  = 26282
	)

	fmt.Println("starting pods...")
	numSQLPods := 4
	pgurls := make([]string, numSQLPods)
	for i := 1; i <= numSQLPods; i++ {
		fmt.Println("create tenant id")
		_, err := c.Conn(ctx, t.L(), 1).Exec(`SELECT crdb_internal.create_tenant($1)`, tenantBaseID+i)
		require.NoError(t, err)

		fmt.Println("createTenantNode")
		tenant := createTenantNode(kvAddrs, tenantBaseID+i, i+1, tenantBaseHTTPPort+i, tenantBaseSQLPort+i)

		fmt.Println("tenant.start")
		tenant.start(ctx, t, c, "./cockroach")
		pgurls[i-1] = tenant.pgURL

		fmt.Printf("started pod %d:%s\n", i, pgurls[i-1])
	}

	fmt.Println("init workload...")
	{
		var wg sync.WaitGroup
		wg.Add(numSQLPods)
		for i := 0; i < numSQLPods; i++ {
			go func(pgurl string) {
				defer wg.Done()
				cmd := fmt.Sprintf("./cockroach workload run kv --init --batch 1000 --duration=10s %s", pgurl)
				c.Run(ctx, c.Node(1), cmd)
			}(pgurls[i])
		}
		wg.Wait()
	}

	fmt.Println("running workload...")
	{
		var wg sync.WaitGroup
		wg.Add(numSQLPods)
		for i := 0; i < numSQLPods; i++ {
			go func(pgurl string) {
				defer wg.Done()
				cmd := fmt.Sprintf("./cockroach workload run kv --read-percent=100 --duration=10s %s", pgurl)
				c.Run(ctx, c.Node(1), cmd)
			}(pgurls[i])
		}
		wg.Wait()
	}

	fmt.Println("querying...")
	for i := 0; i < numSQLPods; i++ {
		db, err := gosql.Open("postgres", pgurls[i])
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		tdb := sqlutils.MakeSQLRunner(db)
		/*
			{
				rows := tdb.Query(t, `select statistics -> 'statistics' from crdb_internal.statement_statistics where app_name = 'kv' AND metadata @> '{"querySummary": "SELECT k, v FROM kv"}' AND metadata @> '{"failed":false}'`)
				for rows.Next() {
					var stats string
					rows.Scan(&stats)
					fmt.Println("stats:", stats)
				}
			}*/
		rows := tdb.Query(t, `select  statistics -> 'statistics' -> 'cnt', statistics -> 'statistics' -> 'runLat' -> 'mean' from crdb_internal.statement_statistics where app_name = 'kv' AND metadata @> '{"querySummary": "SELECT k, v FROM kv"}' AND metadata @> '{"failed":false}'`)
		var count int64
		var meanLatency float64
		for rows.Next() {
			err := rows.Scan(&count, &meanLatency)
			fmt.Println(i, "err:", err, "count:", count, "\nmeanLatency:", meanLatency)
		}
	}

	// should we do store fairness testing here?
}

// Test that the kvserver fairly distributes store tokens on a highly concurrent 4 MT write only
// workload.
func runMultiTenantStoreFairness(ctx context.Context, t test.Test, c cluster.Cluster, v version.Version) {
}
