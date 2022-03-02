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
	"math"
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
		Name:              "multitenant/admission-control/kv-fairness",
		Cluster:           r.MakeClusterSpec(1),
		Owner:             registry.OwnerSQLQueries,
		NonReleaseBlocker: false,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runMultiTenantFairness(ctx, t, c, *t.BuildVersion(),
				10,  // init duration
				10,  // run duration
				100, // read percent
				1,   // min block size
				1,   // max block size
				"SELECT k, v FROM kv")
		},
	})
	r.Add(registry.TestSpec{
		Name:              "multitenant/admission-control/store-fairness",
		Cluster:           r.MakeClusterSpec(1),
		Owner:             registry.OwnerSQLQueries,
		NonReleaseBlocker: false,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runMultiTenantFairness(ctx, t, c, *t.BuildVersion(),
				5,    // init duration
				60,   // runDuration
				0,    // read percent
				1000, // min block size
				1000, // max block size
				"UPSERT INTO kv(k, v)")
		},
	})
}

// Test that the kvserver fairly distributes CPU token on a highly concurrent 4 MT workload.
func runMultiTenantFairness(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	v version.Version,
	initDuration int,
	runDuration int,
	readPercent int,
	minBlockSize int,
	maxBlockSize int,
	query string,
) {
	c.Put(ctx, t.Cockroach(), "./cockroach")
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.Node(1))

	kvAddrs, err := c.ExternalAddr(ctx, t.L(), c.Node(1))
	require.NoError(t, err)

	const (
		tenantBaseID       = 10
		tenantBaseHTTPPort = 8080
		tenantBaseSQLPort  = 26282
	)

	// Create the tenants.
	numSQLPods := 4
	pgurls := make([]string, numSQLPods)
	for i := 1; i <= numSQLPods; i++ {
		_, err := c.Conn(ctx, t.L(), 1).Exec(`SELECT crdb_internal.create_tenant($1)`, tenantBaseID+i)
		require.NoError(t, err)
		tenant := createTenantNode(kvAddrs, tenantBaseID+i, 1, tenantBaseHTTPPort+i, tenantBaseSQLPort+i)
		tenant.start(ctx, t, c, "./cockroach")
		pgurls[i-1] = tenant.pgURL
	}

	// Init kv on each tenant.
	var wg sync.WaitGroup
	wg.Add(numSQLPods)
	for i := 0; i < numSQLPods; i++ {
		go func(pgurl string) {
			defer wg.Done()
			cmd := fmt.Sprintf("./cockroach workload run kv  --min-block-bytes %d --max-block-bytes %d --splits 10 --init --batch 100 --duration=%ds %s", minBlockSize, maxBlockSize, initDuration, pgurl)
			c.Run(ctx, c.Node(1), cmd)
		}(pgurls[i])
	}
	wg.Wait()

	// Run workload.
	wg.Add(numSQLPods)
	for i := 0; i < numSQLPods; i++ {
		go func(pgurl string) {
			defer wg.Done()
			cmd := fmt.Sprintf("./cockroach workload run kv  --min-block-bytes %d --max-block-bytes %d --batch 100 --duration=%ds --read-percent=%d %s", minBlockSize, maxBlockSize, runDuration, readPercent, pgurl)
			c.Run(ctx, c.Node(1), cmd)
		}(pgurls[i])
	}
	wg.Wait()

	// Pull workload performance from crdb_internal.statement_statistics.
	counts := make([]float64, numSQLPods)
	meanLatencies := make([]float64, numSQLPods)
	for i := 0; i < numSQLPods; i++ {
		db, err := gosql.Open("postgres", pgurls[i])
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		tdb := sqlutils.MakeSQLRunner(db)
		querySelector := fmt.Sprintf(`{"querySummary": "%s"}`, query)
		rows := tdb.Query(t, `select statistics -> 'statistics' -> 'cnt', statistics -> 'statistics' -> 'runLat' -> 'mean' from crdb_internal.statement_statistics where app_name = 'kv' AND metadata @> $1 AND metadata @> '{"failed":false}'`, querySelector)
		for rows.Next() {
			err := rows.Scan(&counts[i], &meanLatencies[i])
			require.NoError(t, err)
		}
	}
	ok, maxDeltaPercentage := floatsWithinPercentage(counts, .1)
	require.Truef(t, ok, "Throughput not within expectations: %v", counts)
	fmt.Printf("Max throughput delta: %d%% %v\n", int(maxDeltaPercentage*100), counts)
	ok, maxDeltaPercentage = floatsWithinPercentage(meanLatencies, .1)
	require.Truef(t, ok, "Latency not within expectations: %v", meanLatencies)
	fmt.Printf("Max latency delta: %d%% %v\n", int(maxDeltaPercentage*100), meanLatencies)

	// TODO: Can we do anything to verify that admission control kicked in?  Check some stats?
}

func floatsWithinPercentage(values []float64, percent float64) (bool, float64) {
	average := 0.0
	for _, v := range values {
		average += v
	}
	average = average / float64(len(values))
	limit := average * percent
	maxDelta := 0.0
	for _, v := range values {
		delta := math.Abs(average - v)
		if delta > limit {
			return false, 0.0
		}
		if delta > maxDelta {
			maxDelta = delta
		}
	}
	// make a percentage
	maxDelta = 1.0 - (average-maxDelta)/average
	return true, maxDelta
}
