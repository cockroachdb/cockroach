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
	name        string
	acEnabled   bool
	readPercent int
	blockSizes  func(int) (int, int)
	duration    time.Duration
	concurrency func(int) int
}

func registerMultiTenantFairness(r registry.Registry) {
	acStr := map[bool]string{
		true:  "on",
		false: "off",
	}
	for _, acEnabled := range []bool{true, false} {
		kvSpecs := []mtFairnessSpec{
			{
				name:        "same",
				concurrency: func(int) int { return 80 },
				blockSizes:  func(int) (int, int) { return 4, 8 },
			},
			{
				name:        "concurrency-skew",
				concurrency: func(i int) int { return i * 80 },
				blockSizes:  func(int) (int, int) { return 4, 8 },
			},
		}
		for i := range kvSpecs {
			s := kvSpecs[i]
			s.readPercent = 95
			s.acEnabled = acEnabled
			s.duration = 5 * time.Minute

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
				name:        "same",
				concurrency: func(i int) int { return 20 },
				blockSizes:  func(int) (int, int) { return 9000, 10000 },
			},
			{
				name:        "concurrency-skew",
				concurrency: func(i int) int { return i * 10 },
				blockSizes:  func(int) (int, int) { return 9000, 10000 },
			},
		}
		for i := range storeSpecs {
			s := storeSpecs[i]
			s.readPercent = 5
			s.acEnabled = acEnabled
			s.duration = 20 * time.Minute

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
	duration := 15 * time.Minute
	// This is taken away from duration
	warmUpDuration := 5 * time.Minute

	// For quick local testing.
	if c.IsLocal() {
		duration = 30 * time.Second
		warmUpDuration = 5 * time.Second
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

	// NB: we're using --tolerate-errors because of sql liveness errors like this:
	// ERROR: liveness session expired 571.043163ms before transaction
	// dialed back batch to 100 so we don't need.

	m = c.NewMonitor(ctx, c.Node(1))

	t.L().Printf("running main workload")
	for i := 0; i < numTenants; i++ {
		node := i + 2
		pgurl := pgurls[i]
		m.Go(func(ctx context.Context) error {
			min, max := s.blockSizes(node - 1)
			cmd := fmt.Sprintf("./cockroach workload run kv %s --min-block-bytes %d --max-block-bytes %d --batch 20 --duration=%s --read-percent=%d --concurrency=%d",
				pgurl, min, max, duration, s.readPercent, s.concurrency(node-1))
			err := c.RunE(ctx, c.Node(node), cmd)
			return err
		})
	}

	time.Sleep(warmUpDuration)

	t.L().Printf("warmup done")

	// Reset all the stats, not sure this does anything
	_, err = c.Conn(ctx, t.L(), 1).Exec(`SELECT crdb_internal.reset_sql_stats()`)
	require.NoError(t, err)

	m.Wait()

	// Pull workload performance from crdb_internal.statement_statistics. Alternatively we could pull these from
	// workload but this seemed most straightforward.
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
		// Because of how query field works with batches we can get two entries like this:
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

	failThreshold := .3

	throughput := make([]float64, numTenants)
	ok, maxThroughputDelta := floatsWithinPercentage(counts, failThreshold)
	for i, count := range counts {
		throughput[i] = count / float64(duration.Seconds())
	}

	if s.acEnabled && !ok {
		t.L().Printf("Throughput not within expectations: %f > %f %v", maxThroughputDelta, failThreshold, throughput)
	}

	t.L().Printf("Max throughput delta: %d%% %d %v\n", int(maxThroughputDelta*100), average(throughput), counts)

	ok, maxLatencyDelta := floatsWithinPercentage(meanLatencies, failThreshold)
	t.L().Printf("Max latency delta: %d%% %v\n", int(maxLatencyDelta*100), meanLatencies)

	if s.acEnabled && !ok {
		t.L().Printf("Latency not within expectations: %f > %f %v", maxLatencyDelta, failThreshold, meanLatencies)
	}

	c.Run(ctx, c.Node(1), "mkdir", t.PerfArtifactsDir())
	results := fmt.Sprintf(`{ "max_tput_delta": %f, "max_latency_delta": %f, "max_tput": %f, "min_tput": %f, "max_latency": %f, "min_latency": %f}`,
		maxThroughputDelta, maxLatencyDelta, maxFloat(throughput), minFloat(throughput), maxFloat(meanLatencies), minFloat(meanLatencies))
	t.L().Printf("reporting perf results: %s", results)
	cmd := fmt.Sprintf(`echo '%s' > %s/stats.json`, results, t.PerfArtifactsDir())
	c.Run(ctx, c.Node(1), cmd)

	// get cluster timeseries data into artifacts
	c.FetchTimeseriesData(ctx, t)
}

func average(values []float64) int {
	average := 0
	for _, v := range values {
		average += int(v)
	}
	average /= len(values)
	return average
}

func minFloat(values []float64) float64 {
	min := values[0]
	for _, v := range values {
		if v < min {
			min = v
		}
	}
	return min
}

func maxFloat(values []float64) float64 {
	max := values[0]
	for _, v := range values {
		if v > max {
			max = v
		}
	}
	return max
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
