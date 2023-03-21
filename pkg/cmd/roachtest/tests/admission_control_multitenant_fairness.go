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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/grafana"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/stretchr/testify/require"
)

// This test sets up a single-node CRDB cluster on a 4vCPU machine, and 4
// separate tenant pods also running on their own 4vCPU machines. Each tenant is
// also running a workload on the same node the SQL pod is running on[1]. It
// tests that KV fairly proportions CPU and IO use across the four tenants. Each
// tenant drives the workload generator, k95 and kv5 for {read,CPU}-heavy and
// {write,IO}-heavy variants. Another thing that's varied is the relative
// concurrency of each tenant workload -- they're either even across all tenants
// or skewed multiples of one another.
//
// [1]: Co-locating the SQL pod and the workload generator is a bit funky, but
// it works fine enough as written and saves us from using another 4 nodes
// per test.
func registerMultiTenantFairness(r registry.Registry) {
	specs := []multiTenantFairnessSpec{
		{
			name:        "read-heavy/even",
			concurrency: func(int) int { return 250 },
			blockSize:   5,
			readPercent: 95,
			duration:    20 * time.Minute,
			batch:       100,
			maxOps:      100_000,
			query:       "SELECT k, v FROM kv",
		},
		{
			name:        "read-heavy/skewed",
			concurrency: func(i int) int { return i * 250 },
			blockSize:   5,
			readPercent: 95,
			duration:    20 * time.Minute,
			batch:       100,
			maxOps:      100_000,
			query:       "SELECT k, v FROM kv",
		},
		{
			name:        "write-heavy/even",
			concurrency: func(i int) int { return 50 },
			blockSize:   50_000,
			readPercent: 5,
			duration:    20 * time.Minute,
			batch:       1,
			maxOps:      1000,
			query:       "UPSERT INTO kv(k, v)",
		},
		{
			name:        "write-heavy/skewed",
			concurrency: func(i int) int { return i * 50 },
			blockSize:   50_000,
			readPercent: 5,
			duration:    20 * time.Minute,
			batch:       1,
			maxOps:      1000,
			query:       "UPSERT INTO kv(k, v)",
		},
	}

	for _, s := range specs {
		s := s
		r.Add(registry.TestSpec{
			Name:              fmt.Sprintf("admission-control/multitenant-fairness/%s", s.name),
			Cluster:           r.MakeClusterSpec(5),
			Owner:             registry.OwnerAdmissionControl,
			NonReleaseBlocker: false,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runMultiTenantFairness(ctx, t, c, s)
			},
		})
	}
}

type multiTenantFairnessSpec struct {
	name  string
	query string // query for which we'll check statistics for

	readPercent int           // --read-percent
	blockSize   int           // --min-block-bytes, --max-block-bytes
	duration    time.Duration // --duration
	concurrency func(int) int // --concurrency
	batch       int           // --batch
	maxOps      int           // --max-ops
}

func runMultiTenantFairness(
	ctx context.Context, t test.Test, c cluster.Cluster, s multiTenantFairnessSpec,
) {
	if c.Spec().NodeCount < 5 {
		t.Fatalf("expected at least 5 nodes, found %d", c.Spec().NodeCount)
	}

	numTenants := 4
	crdbNodeID := 1
	crdbNode := c.Node(crdbNodeID)
	if c.IsLocal() {
		s.duration = 30 * time.Second
		s.concurrency = func(i int) int { return 4 }
		if s.maxOps > 10000 {
			s.maxOps = 10000
		}
		if s.batch > 10 {
			s.batch = 10
		}
		if s.blockSize > 2 {
			s.batch = 2
		}
	}

	t.L().Printf("starting cockroach securely (<%s)", time.Minute)
	c.Put(ctx, t.Cockroach(), "./cockroach")
	c.Start(ctx, t.L(),
		option.DefaultStartOptsNoBackups(),
		install.MakeClusterSettings(install.SecureOption(true)),
		crdbNode,
	)

	promNode := c.Node(c.Spec().NodeCount)
	promCfg := &prometheus.Config{}
	promCfg.WithPrometheusNode(promNode.InstallNodes()[0])
	promCfg.WithNodeExporter(crdbNode.InstallNodes())
	promCfg.WithCluster(crdbNode.InstallNodes())
	promCfg.WithGrafanaDashboardJSON(grafana.MultiTenantFairnessGrafanaJSON)

	setRateLimit := func(ctx context.Context, val int) {
		db := c.Conn(ctx, t.L(), crdbNodeID)
		defer db.Close()

		if _, err := db.ExecContext(
			ctx, fmt.Sprintf("SET CLUSTER SETTING kv.tenant_rate_limiter.rate_limit = '%d'", val)); err != nil {
			t.Fatalf("failed to set tenant rate limiter limit: %v", err)
		}
	}

	setRateLimit(ctx, 1_000_000)

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
	tenantID := func(offset int) int {
		return tenantBaseID + offset
	}
	setTenantResourceLimits := func(tenantID int) {
		db := c.Conn(ctx, t.L(), crdbNodeID)
		defer db.Close()
		if _, err := db.ExecContext(
			ctx, fmt.Sprintf(
				"SELECT crdb_internal.update_tenant_resource_limits(%[1]d, 1000000000, 10000, 1000000, now(), 0)", tenantID)); err != nil {
			t.Fatalf("failed to update tenant resource limits: %v", err)
		}
	}
	tenantNodeID := func(idx int) int {
		return idx + 2
	}

	t.L().Printf("enabling child metrics (<%s)", 30*time.Second)
	_, err := c.Conn(ctx, t.L(), crdbNodeID).Exec(`SET CLUSTER SETTING server.child_metrics.enabled = true`)
	require.NoError(t, err)

	// Create the tenants.
	t.L().Printf("initializing %d tenants (<%s)", numTenants, 5*time.Minute)
	tenantIDs := make([]int, 0, numTenants)
	for i := 0; i < numTenants; i++ {
		tenantIDs = append(tenantIDs, tenantID(i))
	}

	tenants := make([]*tenantNode, numTenants)
	for i := 0; i < numTenants; i++ {
		if !t.SkipInit() {
			_, err := c.Conn(ctx, t.L(), 1).Exec(`SELECT crdb_internal.create_tenant($1::INT)`, tenantID(i))
			require.NoError(t, err)
		}

		tenant := createTenantNode(ctx, t, c,
			crdbNode, tenantID(i), tenantNodeID(i), tenantHTTPPort(i), tenantSQLPort(i),
			createTenantOtherTenantIDs(tenantIDs))
		defer tenant.stop(ctx, t, c)

		tenants[i] = tenant
		tenant.start(ctx, t, c, "./cockroach")
		setTenantResourceLimits(tenantID(i))

		tenantNode := c.Node(tenantNodeID(i))

		// Init kv on each tenant.
		cmd := fmt.Sprintf("./cockroach workload init kv '%s'", tenant.secureURL())
		require.NoError(t, c.RunE(ctx, tenantNode, cmd))

		promCfg.WithTenantPod(tenantNode.InstallNodes()[0], tenantID(i))
		promCfg.WithScrapeConfigs(
			prometheus.MakeWorkloadScrapeConfig(fmt.Sprintf("workload-tenant-%d", i),
				"/", makeWorkloadScrapeNodes(
					tenantNode.InstallNodes()[0],
					[]workloadInstance{
						{
							nodes:          c.Node(tenantNodeID(i)),
							prometheusPort: 2112,
						},
					})),
		)
	}

	t.Status(fmt.Sprintf("setting up prometheus/grafana (<%s)", 2*time.Minute))
	_, cleanupFunc := setupPrometheusForRoachtest(ctx, t, c, promCfg, nil)
	defer cleanupFunc()

	t.L().Printf("loading per-tenant data (<%s)", 10*time.Minute)
	m1 := c.NewMonitor(ctx, crdbNode)
	for i := 0; i < numTenants; i++ {
		if t.SkipInit() {
			continue
		}

		i := i
		pgurl := tenants[i].secureURL()
		m1.Go(func(ctx context.Context) error {
			// TODO(irfansharif): Occasionally we see SQL liveness errors of the
			// following form. See #78691, #97448.
			//
			// 	ERROR: liveness session expired 571.043163ms before transaction
			//
			// Why do these errors occur? We started using high-pri for tenant
			// sql liveness work as of #98785, so this TODO might be stale. If
			// it persists, consider extending the default lease duration from
			// 40s to something higher, or retrying internally if the sql
			// session gets renewed shortly (within some jitter). We don't want
			// to --tolerate-errors here and below because we'd see total
			// throughput collapse.
			cmd := fmt.Sprintf(
				"./cockroach workload run kv '%s' --secure --min-block-bytes %d --max-block-bytes %d "+
					"--batch %d --max-ops %d --concurrency=25",
				pgurl, s.blockSize, s.blockSize, s.batch, s.maxOps)
			err := c.RunE(ctx, c.Node(tenantNodeID(i)), cmd)
			t.L().Printf("loaded data for tenant %d", tenantID(i))
			return err
		})
	}
	m1.Wait()

	if !t.SkipInit() {
		t.L().Printf("loaded data for all tenants, sleeping (<%s)", 2*time.Minute)
		time.Sleep(2 * time.Minute)
	}

	t.L().Printf("running per-tenant workloads (<%s)", s.duration+time.Minute)
	m2 := c.NewMonitor(ctx, crdbNode)
	for i := 0; i < numTenants; i++ {
		i := i
		pgurl := tenants[i].secureURL()
		m2.Go(func(ctx context.Context) error {
			cmd := fmt.Sprintf(
				"./cockroach workload run kv '%s' --write-seq=%s --secure --min-block-bytes %d "+
					"--max-block-bytes %d --batch %d --duration=%s --read-percent=%d --concurrency=%d",
				pgurl, fmt.Sprintf("R%d", s.maxOps*s.batch), s.blockSize, s.blockSize, s.batch,
				s.duration, s.readPercent, s.concurrency(tenantNodeID(i)-1))

			err := c.RunE(ctx, c.Node(tenantNodeID(i)), cmd)
			t.L().Printf("ran workload for tenant %d", tenantID(i))
			return err
		})
	}
	m2.Wait()

	// Pull workload performance from crdb_internal.statement_statistics. We
	// could alternatively get these from the workload itself but this was
	// easier.
	//
	// TODO(irfansharif): Worth using clusterstats for this directly against a
	// prometheus instance pointed to each tenant's workload generator.
	// TODO(irfansharif): Make sure that count of failed queries is small/zero.
	// TODO(irfansharif): Aren't these stats getting polluted by the data-load
	// step?
	t.L().Printf("computing workload statistics (%s)", 30*time.Second)
	counts := make([]float64, numTenants)
	meanLatencies := make([]float64, numTenants)
	for i := 0; i < numTenants; i++ {
		i := i
		db, err := gosql.Open("postgres", tenants[i].pgURL)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()

		tdb := sqlutils.MakeSQLRunner(db)
		tdb.Exec(t, "USE kv")

		rows := tdb.Query(t, `
			SELECT
				sum((statistics -> 'statistics' -> 'cnt')::INT),
				avg((statistics -> 'statistics' -> 'runLat' -> 'mean')::FLOAT)
			FROM crdb_internal.statement_statistics
			WHERE metadata @> '{"db":"kv","failed":false}' AND metadata @> $1`,
			fmt.Sprintf(`{"querySummary": "%s"}`, s.query))

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
		throughput[i] = count / s.duration.Seconds()
	}
	t.L().Printf("max-throughput-delta=%d%% average-throughput=%f total-ops-per-tenant=%v\n", int(maxThroughputDelta*100), averageFloat(throughput), counts)
	if !ok {
		// TODO(irfansharif): This is a weak assertion. Variation occurs when
		// there are workload differences during periods where AC is not
		// inducing any queuing. Remove?
		t.L().Printf("throughput not within expectations: %f > %f %v", maxThroughputDelta, failThreshold, throughput)
	}

	ok, maxLatencyDelta := floatsWithinPercentage(meanLatencies, failThreshold)
	t.L().Printf("max-latency-delta=d%% mean-latency-per-tenant=%v\n", int(maxLatencyDelta*100), meanLatencies)
	if !ok {
		// TODO(irfansharif): Same as above -- this is a weak assertion.
		t.L().Printf("latency not within expectations: %f > %f %v", maxLatencyDelta, failThreshold, meanLatencies)
	}

	c.Run(ctx, crdbNode, "mkdir", "-p", t.PerfArtifactsDir())
	results := fmt.Sprintf(`{ "max_tput_delta": %f, "max_tput": %f, "min_tput": %f, "max_latency": %f, "min_latency": %f}`,
		maxThroughputDelta, maxFloat(throughput), minFloat(throughput), maxFloat(meanLatencies), minFloat(meanLatencies))
	c.Run(ctx, crdbNode, fmt.Sprintf(`echo '%s' > %s/stats.json`, results, t.PerfArtifactsDir()))
}

func averageFloat(values []float64) float64 {
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

func minFloat(values []float64) float64 {
	min := values[0]
	for _, v := range values {
		min = math.Min(v, min)
	}
	return min
}

func maxFloat(values []float64) float64 {
	max := values[0]
	for _, v := range values {
		max = math.Max(v, max)
	}
	return max
}

func floatsWithinPercentage(values []float64, percent float64) (bool, float64) {
	avg := averageFloat(values)
	limit := avg * percent
	maxDelta := 0.0
	for _, v := range values {
		delta := math.Abs(avg - v)
		if delta > limit {
			return false, 1.0 - (avg-delta)/avg
		}
		if delta > maxDelta {
			maxDelta = delta
		}
	}
	maxDelta = 1.0 - (avg-maxDelta)/avg // make a percentage
	return true, maxDelta
}
