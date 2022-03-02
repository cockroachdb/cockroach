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
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/stretchr/testify/require"
)

type mtFairnessSpec struct {
	name           string
	acEnabled      bool
	readPercent    int
	minBlockSize   int
	maxBlockSize   int
	warmUpDuration func(int) time.Duration
	concurrency    func(int) int
}

func registerMultiTenantFairness(r registry.Registry) {
	acStr := map[bool]string{
		true:  "on",
		false: "off",
	}
	for _, acEnabled := range []bool{true, false} {
		kvSpecs := []mtFairnessSpec{
			{
				name:           "same",
				warmUpDuration: func(int) time.Duration { return 5 * time.Minute },
				concurrency:    func(int) int { return 32 },
			},
			{
				name:           "concurrency-skew",
				readPercent:    100,
				minBlockSize:   1,
				maxBlockSize:   10,
				warmUpDuration: func(i int) time.Duration { return 5 * time.Minute },
				concurrency:    func(i int) int { return i * 8 },
			},
			{
				name:           "size-skew",
				readPercent:    100,
				minBlockSize:   1,
				maxBlockSize:   10,
				warmUpDuration: func(i int) time.Duration { return time.Duration(i*2) * time.Minute },
				concurrency:    func(_ int) int { return 32 },
			},
		}
		for _, s := range kvSpecs {
			s.readPercent = 100
			s.acEnabled = acEnabled
			s.minBlockSize = 1
			s.maxBlockSize = 10
			r.Add(registry.TestSpec{
				Name:              fmt.Sprintf("multitenant/admission-control-%s/kv-fairness/%s", acStr[s.acEnabled], s.name),
				Cluster:           r.MakeClusterSpec(5),
				Owner:             registry.OwnerSQLQueries,
				NonReleaseBlocker: false,
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
					runMultiTenantFairness(ctx, t, c, s, "SELECT k, v FROM kv")
				},
			})
		}
		storeSpecs := []mtFairnessSpec{
			{
				name:           "same",
				warmUpDuration: func(i int) time.Duration { return 5 * time.Minute },
				concurrency:    func(i int) int { return 32 },
			},
			{
				name:           "concurrency-skew",
				warmUpDuration: func(i int) time.Duration { return 5 * time.Minute },
				concurrency:    func(i int) int { return i * 8 },
			},
			{
				name:           "size-skew",
				warmUpDuration: func(i int) time.Duration { return time.Duration(i*2) * time.Minute },
				concurrency:    func(i int) int { return 32 },
			},
		}
		for _, s := range storeSpecs {
			s.readPercent = 0
			s.acEnabled = acEnabled
			s.minBlockSize = 1000
			s.maxBlockSize = 10000
			r.Add(registry.TestSpec{
				Name:              fmt.Sprintf("multitenant/admission-control-%s/store-fairness/%s", acStr[s.acEnabled], s.name),
				Cluster:           r.MakeClusterSpec(5),
				Owner:             registry.OwnerSQLQueries,
				NonReleaseBlocker: false,
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
					runMultiTenantFairness(ctx, t, c, s, "UPSERT INTO kv(k, v)")
				},
			})
		}
	}
}

// Test that the kvserver fairly distributes CPU token on a highly concurrent 4 sql pod workload.
func runMultiTenantFairness(
	ctx context.Context, t test.Test, c cluster.Cluster, s mtFairnessSpec, query string,
) {
	numTenants := 4
	duration := 10 * time.Minute

	// For quick local testing.
	if c.IsLocal() {
		duration = time.Second
		s.warmUpDuration = func(i int) time.Duration { return time.Second }
		s.concurrency = func(i int) int { return 4 }
	}

	c.Put(ctx, t.Cockroach(), "./cockroach")
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.Node(1))
	SetAdmissionControl(ctx, t, c, s.acEnabled)

	m := c.NewMonitor(ctx, c.Node(1))
	kvAddrs, err := c.ExternalAddr(ctx, t.L(), c.Node(1))
	require.NoError(t, err)

	const (
		tenantBaseID       = 11
		tenantBaseHTTPPort = 8081
		tenantBaseSQLPort  = 26283
	)

	// Create the tenants.
	t.L().Printf("initializing %d tenants", numTenants)

	pgurls := make([]string, numTenants)
	for i := 0; i < numTenants; i++ {
		node := i + 2
		_, err := c.Conn(ctx, t.L(), 1).Exec(`SELECT crdb_internal.create_tenant($1)`, tenantBaseID+i)
		require.NoError(t, err)
		tenant := createTenantNode(kvAddrs, tenantBaseID+i, node, tenantBaseHTTPPort+i, tenantBaseSQLPort+i)
		tenant.start(ctx, t, c, "./cockroach")
		pgurls[i] = tenant.pgURL

		// Init kv on each tenant.
		cmd := fmt.Sprintf("./cockroach workload init kv %s --db kv", tenant.pgURL)
		err = c.RunE(ctx, c.Node(node), cmd)
		require.NoError(t, err)
	}

	// Prime workload.
	t.L().Printf("doing warmup data load")

	for i := 0; i < numTenants; i++ {
		node := i + 2
		pgurl := pgurls[i]
		m.Go(func(ctx context.Context) error {
			cmd := fmt.Sprintf("./cockroach workload run kv %s --splits 10 --min-block-bytes %d --max-block-bytes %d --batch 1000 --duration=%s --read-percent=0",
				pgurl, s.minBlockSize, s.maxBlockSize, s.warmUpDuration(node-1))
			err := c.RunE(ctx, c.Node(node), cmd)
			return err
		})
	}
	m.Wait()

	// Reset all the stats, not sure this does anything
	_, err = c.Conn(ctx, t.L(), 1).Exec(`SELECT crdb_internal.reset_sql_stats()`)
	require.NoError(t, err)

	m = c.NewMonitor(ctx, c.Node(1))

	t.L().Printf("running main workload")
	for i := 0; i < numTenants; i++ {
		node := i + 2
		pgurl := pgurls[i]
		m.Go(func(ctx context.Context) error {
			cmd := fmt.Sprintf("./cockroach workload run kv %s --min-block-bytes %d --max-block-bytes %d --batch 100 --duration=%s --read-percent=%d --concurrency=%d",
				pgurl, s.minBlockSize, s.maxBlockSize, duration, s.readPercent, s.concurrency(node-1))
			err := c.RunE(ctx, c.Node(node), cmd)
			return err
		})
	}
	m.Wait()

	// Pull workload performance from crdb_internal.statement_statistics.
	counts := make([]float64, numTenants)
	meanLatencies := make([]float64, numTenants)
	for i := 0; i < numTenants; i++ {
		db, err := gosql.Open("postgres", pgurls[i])
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		tdb := sqlutils.MakeSQLRunner(db)
		querySelector := fmt.Sprintf(`{"querySummary": "%s"}`, query)
		// TODO: should we check that count of failed queries is smallish?
		rows := tdb.Query(t, `select statistics -> 'statistics' -> 'cnt', statistics -> 'statistics' -> 'runLat' -> 'mean' from crdb_internal.statement_statistics where metadata @> '{"db":"kv", "failed":false}' AND metadata @> $1`, querySelector)

		if rows.Next() {
			err := rows.Scan(&counts[i], &meanLatencies[i])
			require.NoError(t, err)
		}
		// Because of how query field works we can get two entries like this:
		// UPSERT INTO kv(k, v) VALUES ($1, $2), (__more90__)
		// UPSERT INTO kv(k, v) VALUES ($1, $2), (__more900__)
		// One is from warm up and one from workload, just pick the one with the higher count.
		if rows.Next() {
			var cnt, lat float64
			err := rows.Scan(&cnt, &lat)
			require.NoError(t, err)
			if cnt > counts[i] {
				counts[i] = cnt
				meanLatencies[i] = lat
			}
		}
	}
	ok, maxDeltaPercentage := floatsWithinPercentage(counts, .3)
	t.L().Printf("Max throughput delta: %d%% %v\n", int(maxDeltaPercentage*100), counts)
	ok, maxDeltaPercentage = floatsWithinPercentage(meanLatencies, .3)
	t.L().Printf("Max latency delta: %d%% %v\n", int(maxDeltaPercentage*100), meanLatencies)

	if s.acEnabled {
		require.Truef(t, ok, "Latency not within expectations: %v", meanLatencies)
		throughput := make([]float64, numTenants)
		for i, count := range counts {
			throughput[i] = count / float64(duration)
		}
		require.Truef(t, ok, "Throughput not within expectations: %v", throughput)
	}
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
			return false, 1.0 - (average-delta)/average
		}
		if delta > maxDelta {
			maxDelta = delta
		}
	}
	// make a percentage
	maxDelta = 1.0 - (average-maxDelta)/average
	return true, maxDelta
}
