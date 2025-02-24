// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/grafana"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
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
//
// TODO(sumeer): Now that we are counting actual CPU for inter-tenant
// fairness, alter the read-heavy workloads to perform different sized work,
// and evaluate fairness.
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
			Name:             fmt.Sprintf("admission-control/multitenant-fairness/%s", s.name),
			Cluster:          r.MakeClusterSpec(5),
			Owner:            registry.OwnerAdmissionControl,
			Benchmark:        true,
			Leases:           registry.MetamorphicLeases,
			CompatibleClouds: registry.CloudsWithServiceRegistration,
			Suites:           registry.Suites(registry.Weekly),
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
	crdbNode := c.Node(1)
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

	t.L().Printf("starting cockroach (<%s)", time.Minute)
	c.Start(ctx, t.L(),
		option.NewStartOpts(option.NoBackupSchedule),
		install.MakeClusterSettings(),
		crdbNode,
	)

	promNode := c.Node(c.Spec().NodeCount)
	promCfg := &prometheus.Config{}
	promCfg.WithPrometheusNode(promNode.InstallNodes()[0])
	promCfg.WithNodeExporter(crdbNode.InstallNodes())
	promCfg.WithCluster(crdbNode.InstallNodes())
	promCfg.WithGrafanaDashboardJSON(grafana.MultiTenantFairnessGrafanaJSON)

	systemConn := c.Conn(ctx, t.L(), crdbNode[0])
	defer systemConn.Close()

	const rateLimit = 1_000_000

	if _, err := systemConn.ExecContext(
		ctx, fmt.Sprintf("SET CLUSTER SETTING kv.tenant_rate_limiter.rate_limit = '%d'", rateLimit),
	); err != nil {
		t.Fatalf("failed to set tenant rate limiter limit: %v", err)
	}

	t.L().Printf("enabling child metrics (<%s)", 30*time.Second)
	_, err := systemConn.ExecContext(ctx, `SET CLUSTER SETTING server.child_metrics.enabled = true`)
	require.NoError(t, err)

	virtualClusters := map[string]option.NodeListOption{
		"app-fairness-n2": c.Node(2),
		"app-fairness-n3": c.Node(3),
		"app-fairness-n4": c.Node(4),
		"app-fairness-n5": c.Node(5),
	}

	virtualClusterNames := maps.Keys(virtualClusters)
	sort.Strings(virtualClusterNames)

	t.L().Printf("initializing %d virtual clusters (<%s)", len(virtualClusters), 5*time.Minute)
	for j, name := range virtualClusterNames {
		node := virtualClusters[name]
		c.StartServiceForVirtualCluster(
			ctx, t.L(),
			option.StartVirtualClusterOpts(name, node),
			install.MakeClusterSettings(),
		)

		t.L().Printf("virtual cluster %q started on n%d", name, node[0])
		_, err := systemConn.ExecContext(
			ctx, fmt.Sprintf("SELECT crdb_internal.update_tenant_resource_limits('%s', 1000000000, 10000, 1000000)", name),
		)
		require.NoError(t, err)

		promCfg.WithTenantPod(node.InstallNodes()[0], j+1)
		promCfg.WithScrapeConfigs(
			prometheus.MakeWorkloadScrapeConfig(fmt.Sprintf("workload-tenant-%d", j+1),
				"/", makeWorkloadScrapeNodes(
					node.InstallNodes()[0],
					[]workloadInstance{
						{
							nodes:          node,
							prometheusPort: 2112,
						},
					})),
		)

		initKV := fmt.Sprintf(
			"%s workload init kv {pgurl:%d:%s}",
			test.DefaultCockroachPath, node[0], name,
		)

		c.Run(ctx, option.WithNodes(node), initKV)
	}

	t.L().Printf("setting up prometheus/grafana (<%s)", 2*time.Minute)
	_, cleanupFunc := setupPrometheusForRoachtest(ctx, t, c, promCfg, nil)
	defer cleanupFunc()

	t.L().Printf("loading per-tenant data (<%s)", 10*time.Minute)
	m1 := c.NewMonitor(ctx, c.All())
	for name, node := range virtualClusters {
		pgurl := fmt.Sprintf("{pgurl:%d:%s}", node[0], name)
		name := name
		node := node
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
			cmd := roachtestutil.NewCommand("%s workload run kv", test.DefaultCockroachPath).
				Option("secure").
				Flag("min-block-bytes", s.blockSize).
				Flag("max-block-bytes", s.blockSize).
				Flag("batch", s.batch).
				Flag("max-ops", s.maxOps).
				Flag("concurrency", 25).
				Arg("%s", pgurl)

			if err := c.RunE(ctx, option.WithNodes(node), cmd.String()); err != nil {
				return err
			}

			t.L().Printf("loaded data for virtual cluster %q", name)
			return nil
		})
	}
	m1.Wait()

	waitDur := 2 * time.Minute
	t.L().Printf("loaded data for all tenants, sleeping (<%s)", waitDur)
	time.Sleep(waitDur)

	t.L().Printf("running virtual cluster workloads (<%s)", s.duration+time.Minute)
	m2 := c.NewMonitor(ctx, crdbNode)
	var n int
	for name, node := range virtualClusters {
		pgurl := fmt.Sprintf("{pgurl:%d:%s}", node[0], name)
		n++

		name := name
		node := node
		m2.Go(func(ctx context.Context) error {
			cmd := roachtestutil.NewCommand("%s workload run kv", test.DefaultCockroachPath).
				Option("secure").
				Flag("write-seq", fmt.Sprintf("R%d", s.maxOps*s.batch)).
				Flag("min-block-bytes", s.blockSize).
				Flag("max-block-bytes", s.blockSize).
				Flag("batch", s.batch).
				Flag("duration", s.duration).
				Flag("read-percent", s.readPercent).
				Flag("concurrency", s.concurrency(n)).
				Arg("%s", pgurl)

			if err := c.RunE(ctx, option.WithNodes(node), cmd.String()); err != nil {
				return err
			}

			t.L().Printf("ran workload for virtual cluster %q", name)
			return nil
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
	counts := make([]float64, len(virtualClusters))
	meanLatencies := make([]float64, len(virtualClusters))
	for j, name := range virtualClusterNames {
		node := virtualClusters[name]

		vcdb := c.Conn(ctx, t.L(), node[0], option.VirtualClusterName(name))
		//nolint:deferloop TODO(#137605)
		defer vcdb.Close()

		_, err := vcdb.ExecContext(ctx, "USE kv")
		// Retry once, since this can fail sometimes due the cluster running hot.
		if err != nil {
			_, err = vcdb.ExecContext(ctx, "USE kv")
		}
		require.NoError(t, err)

		// TODO(aaditya): We no longer have the ability to filter for stats by
		// successful queries, and include ones for failed queries. Maybe consider
		// finding a way to do this?
		// See https://github.com/cockroachdb/cockroach/pull/121120.
		rows, err := vcdb.QueryContext(ctx, `
			SELECT
				sum((statistics -> 'statistics' -> 'cnt')::INT),
				avg((statistics -> 'statistics' -> 'runLat' -> 'mean')::FLOAT)
			FROM crdb_internal.statement_statistics
			WHERE metadata @> '{"db":"kv"}' AND metadata @> $1`,
			fmt.Sprintf(`{"querySummary": "%s"}`, s.query))
		require.NoError(t, err)

		if rows.Next() {
			var cnt, lat float64
			err := rows.Scan(&cnt, &lat)
			require.NoError(t, err)
			counts[j] = cnt
			meanLatencies[j] = lat
		} else {
			t.Fatal("no query results")
		}

		require.NoError(t, rows.Err())
	}

	failThreshold := .3
	throughput := make([]float64, len(virtualClusters))
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
	t.L().Printf("max-latency-delta=%d% mean-latency-per-tenant=%v\n", int(maxLatencyDelta*100), meanLatencies)
	if !ok {
		// TODO(irfansharif): Same as above -- this is a weak assertion.
		t.L().Printf("latency not within expectations: %f > %f %v", maxLatencyDelta, failThreshold, meanLatencies)
	}

	c.Run(ctx, option.WithNodes(crdbNode), "mkdir", "-p", t.PerfArtifactsDir())
	results := fmt.Sprintf(`{ "max_tput_delta": %f, "max_tput": %f, "min_tput": %f, "max_latency": %f, "min_latency": %f}`,
		maxThroughputDelta, maxFloat(throughput), minFloat(throughput), maxFloat(meanLatencies), minFloat(meanLatencies))
	c.Run(ctx, option.WithNodes(crdbNode), fmt.Sprintf(`echo '%s' > %s/stats.json`, results, t.PerfArtifactsDir()))
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
