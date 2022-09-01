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
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/stretchr/testify/require"
)

type mtFairnessSpec struct {
	name        string
	acEnabled   bool
	readPercent int
	blockSize   int
	duration    time.Duration
	concurrency func(int) int
	batchSize   int
	maxLoadOps  int
}

func registerMultiTenantFairness(r registry.Registry) {
	acStr := map[bool]string{
		true:  "admission",
		false: "no-admission",
	}
	for _, acEnabled := range []bool{true, false} {
		kvSpecs := []mtFairnessSpec{
			{
				name:        "same",
				concurrency: func(int) int { return 250 },
			},
			{
				name:        "concurrency-skew",
				concurrency: func(i int) int { return i * 250 },
			},
		}
		for i := range kvSpecs {
			s := kvSpecs[i]
			s.blockSize = 5
			s.readPercent = 95
			s.acEnabled = acEnabled
			s.duration = 5 * time.Minute
			s.batchSize = 100
			s.maxLoadOps = 100_000

			r.Add(registry.TestSpec{
				Skip:              "#83994",
				Name:              fmt.Sprintf("multitenant/fairness/kv/%s/%s", s.name, acStr[s.acEnabled]),
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
				concurrency: func(i int) int { return 50 },
			},
			{
				name:        "concurrency-skew",
				concurrency: func(i int) int { return i * 50 },
			},
		}
		for i := range storeSpecs {
			s := storeSpecs[i]
			s.blockSize = 50_000
			s.readPercent = 5
			s.acEnabled = acEnabled
			s.duration = 10 * time.Minute
			s.batchSize = 1
			s.maxLoadOps = 1000

			r.Add(registry.TestSpec{
				Skip:              "#83994",
				Name:              fmt.Sprintf("multitenant/fairness/store/%s/%s", s.name, acStr[s.acEnabled]),
				Cluster:           r.MakeClusterSpec(5),
				Owner:             registry.OwnerSQLQueries,
				NonReleaseBlocker: false,
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
					runMultiTenantFairness(ctx, t, c, s, "UPSERT INTO kv(k, v)")
				},
			})
		}
	}
	if buildutil.CrdbTestBuild {
		quick := mtFairnessSpec{
			duration:    1,
			acEnabled:   false,
			readPercent: 95,
			name:        "quick",
			concurrency: func(i int) int { return 1 },
			blockSize:   2,
			batchSize:   10,
			maxLoadOps:  1000,
		}
		r.Add(registry.TestSpec{
			Name:              "multitenant/fairness/quick",
			Cluster:           r.MakeClusterSpec(2),
			Owner:             registry.OwnerSQLQueries,
			NonReleaseBlocker: false,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runMultiTenantFairness(ctx, t, c, quick, "SELECT k, v FROM kv")
			},
		})
	}
}

// Test that the kvserver fairly distributes CPU token on a highly concurrent 4 sql pod workload.
func runMultiTenantFairness(
	ctx context.Context, t test.Test, c cluster.Cluster, s mtFairnessSpec, query string,
) {
	numTenants := c.Spec().NodeCount - 1
	duration := s.duration

	// For quick local testing.
	quick := c.IsLocal() || s.name == "quick"
	if quick {
		duration = 30 * time.Second
		s.concurrency = func(i int) int { return 4 }
	}

	c.Put(ctx, t.Cockroach(), "./cockroach")
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(install.SecureOption(true)), c.Node(1))
	SetAdmissionControl(ctx, t, c, s.acEnabled)

	setRateLimit := func(ctx context.Context, val int, node int) {
		db := c.Conn(ctx, t.L(), node)
		defer db.Close()
		if _, err := db.ExecContext(
			ctx, fmt.Sprintf("SET CLUSTER SETTING kv.tenant_rate_limiter.rate_limit = '%d'", val)); err != nil {
			t.Fatalf("failed to set kv.tenant_rate_limiter.rate_limit: %v", err)
		}
	}

	setRateLimit(ctx, 1000000, 1)

	const (
		tenantBaseID       = 11
		tenantBaseHTTPPort = 8081
		tenantBaseSQLPort  = 26259
	)

	tenantHTTPPort := func(offset int) int {
		if c.IsLocal() {
			return tenantBaseHTTPPort + offset
		}
		return tenantBaseHTTPPort
	}
	tenantSQLPort := func(offset int) int {
		if c.IsLocal() {
			return tenantBaseSQLPort + offset
		}
		return tenantBaseSQLPort
	}

	// Create the tenants.
	t.L().Printf("initializing %d tenants", numTenants)
	tenantIDs := make([]int, 0, numTenants)
	for i := 0; i < numTenants; i++ {
		tenantIDs = append(tenantIDs, tenantBaseID+i)
	}

	tenants := make([]*tenantNode, numTenants)
	for i := 0; i < numTenants; i++ {
		node := i + 2
		_, err := c.Conn(ctx, t.L(), 1).Exec(`SELECT crdb_internal.create_tenant($1)`, tenantBaseID+i)
		require.NoError(t, err)
		tenant := createTenantNode(ctx, t, c, c.Node(1), tenantBaseID+i, node, tenantHTTPPort(i), tenantSQLPort(i), createTenantOtherTenantIDs(tenantIDs))
		defer tenant.stop(ctx, t, c)
		tenant.start(ctx, t, c, "./cockroach")
		tenants[i] = tenant

		// Init kv on each tenant.
		cmd := fmt.Sprintf("./cockroach workload init kv '%s' --secure", tenant.secureURL())
		err = c.RunE(ctx, c.Node(node), cmd)
		require.NoError(t, err)

		// This doesn't work on tenant, have to do it on kvserver
		//setRateLimit(ctx, 1000000, node)
		//  failed to set range_max_bytes: pq: unimplemented: operation is unsupported in multi-tenancy mode
		//setMaxRangeBytes(ctx, 1<<18, node)
	}

	m := c.NewMonitor(ctx, c.Node(1))

	// NB: we're using --tolerate-errors because of sql liveness errors like this:
	// ERROR: liveness session expired 571.043163ms before transaction
	// dialed back batch to 20 so we don't need.

	t.L().Printf("running dataload 	")
	for i := 0; i < numTenants; i++ {
		tid := tenantBaseID + i
		node := i + 2
		pgurl := tenants[i].secureURL()
		m.Go(func(ctx context.Context) error {
			cmd := fmt.Sprintf(
				"./cockroach workload run kv '%s' --secure --min-block-bytes %d --max-block-bytes %d "+
					"--batch %d --max-ops %d --concurrency=100",
				pgurl, s.blockSize, s.blockSize, s.batchSize, s.maxLoadOps)
			err := c.RunE(ctx, c.Node(node), cmd)
			t.L().Printf("dataload for tenant %d done", tid)
			return err
		})
	}

	m.Wait()
	t.L().Printf("running main workloads")
	m = c.NewMonitor(ctx, c.Node(1))

	for i := 0; i < numTenants; i++ {
		tid := tenantBaseID + i
		node := i + 2
		pgurl := tenants[i].secureURL()
		m.Go(func(ctx context.Context) error {
			cmd := fmt.Sprintf(
				"./cockroach workload run kv '%s' --write-seq=%s --secure --min-block-bytes %d "+
					"--max-block-bytes %d --batch %d --duration=%s --read-percent=%d --concurrency=%d",
				pgurl, fmt.Sprintf("R%d", s.maxLoadOps*s.batchSize), s.blockSize, s.blockSize, s.batchSize,
				duration, s.readPercent, s.concurrency(node-1))
			err := c.RunE(ctx, c.Node(node), cmd)
			t.L().Printf("workload for tenant %d done", tid)
			return err
		})
	}

	m.Wait()
	t.L().Printf("workloads done")

	// Pull workload performance from crdb_internal.statement_statistics. Alternatively we could pull these from
	// workload but this seemed most straightforward.
	counts := make([]float64, numTenants)
	meanLatencies := make([]float64, numTenants)
	for i := 0; i < numTenants; i++ {
		db, err := gosql.Open("postgres", tenants[i].pgURL)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		tdb := sqlutils.MakeSQLRunner(db)
		tdb.Exec(t, "USE kv")
		querySelector := fmt.Sprintf(`{"querySummary": "%s"}`, query)
		// TODO: should we check that count of failed queries is smallish?
		rows := tdb.Query(t, `
			SELECT
				sum((statistics -> 'statistics' -> 'cnt')::INT),
				avg((statistics -> 'statistics' -> 'runLat' -> 'mean')::FLOAT)
			FROM crdb_internal.statement_statistics
			WHERE metadata @> '{"db":"kv","failed":false}' AND metadata @> $1`, querySelector)

		if rows.Next() {
			var cnt, lat float64
			err := rows.Scan(&cnt, &lat)
			require.NoError(t, err)
			counts[i] = cnt
			meanLatencies[i] = lat
		} else {
			t.Fatal("no query results")
		}
	}

	failThreshold := .3

	throughput := make([]float64, numTenants)
	ok, maxThroughputDelta := floatsWithinPercentage(counts, failThreshold)
	for i, count := range counts {
		throughput[i] = count / duration.Seconds()
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

	c.Run(ctx, c.Node(1), "mkdir", "-p", t.PerfArtifactsDir())
	results := fmt.Sprintf(`{ "max_tput_delta": %f, "max_tput": %f, "min_tput": %f, "max_latency": %f, "min_latency": %f}`,
		maxThroughputDelta, maxFloat(throughput), minFloat(throughput), maxFloat(meanLatencies), minFloat(meanLatencies))
	t.L().Printf("reporting perf results: %s", results)
	cmd := fmt.Sprintf(`echo '%s' > %s/stats.json`, results, t.PerfArtifactsDir())
	c.Run(ctx, c.Node(1), cmd)

	// get cluster timeseries data into artifacts
	err := c.FetchTimeseriesData(ctx, t.L())
	require.NoError(t, err)
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
