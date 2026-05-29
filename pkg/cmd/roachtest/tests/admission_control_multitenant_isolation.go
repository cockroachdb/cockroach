// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	gosql "database/sql"
	"fmt"
	"regexp"
	"time"

	rpgrafana "github.com/cockroachdb/cockroach/pkg/cmd/roachprod/grafana"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// This test sets up a 3-node cluster -- one KV node (4 vCPUs) and two 32-vCPU
// machines each running a SQL pod and workload generator co-located, one
// "quiet" and one "noisy". The SQL/workload nodes are oversized so they aren't
// the bottleneck -- today CPU time token AC only runs in the KV layer, not the
// SQL layer. It tests that the quiet tenant's latency is isolated from a noisy
// neighbor driving heavy load. The test runs both tenants concurrently and
// asserts the quiet tenant's avg latency stays below a configured ceiling. It
// also verifies that CPU utilization is in the 70-90% range.
func registerMultiTenantIsolation(r registry.Registry) {
	specs := []multiTenantIsolationSpec{
		// Simplest KV test: the quiet tenant's concurrency is so small it
		// should always be able to burst (see cpu_time_token_granter.go).
		//
		// KV tests focus on reads to ensure CPU is the bottleneck resource,
		// not LSM or disk.
		{
			name:            "kv-tiny/noisy-neighbor",
			quietLatencyMax: 10 * time.Millisecond,
			kv: &multiTenantIsolationKVSpec{
				readPercent:      95,
				blockSize:        20,
				batch:            100,
				maxOps:           100_000,
				quietConcurrency: 1,
				noisyConcurrency: 3500,
				query:            "SELECT k, v FROM kv",
			},
		},
		// Larger quiet workload that can't burst, but is still much smaller
		// than the noisy tenant so shouldn't be queued excessively.
		{
			name:            "kv-read/noisy-neighbor",
			quietLatencyMax: 150 * time.Millisecond,
			kv: &multiTenantIsolationKVSpec{
				readPercent:      95,
				blockSize:        20,
				batch:            100,
				maxOps:           100_000,
				quietConcurrency: 25,
				noisyConcurrency: 3500,
				query:            "SELECT k, v FROM kv",
			},
		},
		// TPCC exercises a more complex workload at the KV level than
		// simple point reads/writes.
		{
			name:            "tpcc/noisy-neighbor",
			quietLatencyMax: 20 * time.Millisecond,
			tpcc: &multiTenantIsolationTPCCSpec{
				quietWarehouses: 1,
				noisyWarehouses: 200,
			},
		},
	}
	for _, s := range specs {
		s := s
		r.Add(registry.TestSpec{
			Name: fmt.Sprintf("admission-control/multitenant-isolation/%s", s.name),
			// Node 1: KV (4 vCPUs), Nodes 2-3: SQL pod + workload generator
			// (32 vCPUs each, oversized so they aren't the bottleneck -- today
			// CPU time token AC only runs in the KV layer, not the SQL layer).
			Cluster: r.MakeClusterSpec(
				3, spec.CPU(4), spec.VolumeSize(4096), spec.DisableLocalSSD(),
				spec.WorkloadNodeCount(2), spec.WorkloadNodeCPU(32),
			),
			Owner:            registry.OwnerAdmissionControl,
			Benchmark:        true,
			CompatibleClouds: registry.CloudsWithServiceRegistration,
			// TODO(joshimhoff): Consider moving to Weekly once tests are proven stable.
			Suites:  registry.Suites(registry.Nightly),
			Timeout: 60 * time.Minute,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runMultiTenantIsolation(ctx, t, c, s)
			},
		})
	}
}

type multiTenantIsolationSpec struct {
	name            string
	kv              *multiTenantIsolationKVSpec
	tpcc            *multiTenantIsolationTPCCSpec
	quietLatencyMax time.Duration // max avg latency for quiet tenant during noisy period
}

type multiTenantIsolationKVSpec struct {
	readPercent      int    // --read-percent
	blockSize        int    // --min-block-bytes, --max-block-bytes
	batch            int    // --batch
	maxOps           int    // --max-ops
	quietConcurrency int    // --concurrency for quiet tenant
	noisyConcurrency int    // --concurrency for noisy tenant
	query            string // querySummary filter for statement_statistics
}

type multiTenantIsolationTPCCSpec struct {
	quietWarehouses int // --warehouses for quiet tenant
	noisyWarehouses int // --warehouses for noisy tenant
}

func runMultiTenantIsolation(
	ctx context.Context, t test.Test, c cluster.Cluster, s multiTenantIsolationSpec,
) {
	annotatePhase := func(text string) {
		if err := c.AddGrafanaAnnotation(
			ctx, t.L(), rpgrafana.AddAnnotationRequest{Text: text},
		); err != nil {
			t.L().Printf("annotation error: %v", err)
		}
	}

	logCPUTimeTokenDashboardLink(t, c)

	kvNode := c.Node(1)
	quietSQLNode := c.Node(2)
	noisySQLNode := c.Node(3)

	t.L().Printf("starting cockroach")
	c.Start(ctx, t.L(),
		option.NewStartOpts(option.NoBackupSchedule),
		install.MakeClusterSettings(),
		kvNode,
	)

	systemConn := c.Conn(ctx, t.L(), kvNode[0])
	defer systemConn.Close()

	if _, err := systemConn.ExecContext(
		ctx, `SET CLUSTER SETTING admission.cpu_time_tokens.enabled = true`,
	); err != nil {
		t.Fatalf("failed to enable cpu time tokens: %v", err)
	}
	if _, err := systemConn.ExecContext(
		ctx, `SET CLUSTER SETTING server.child_metrics.enabled = true`,
	); err != nil {
		t.Fatalf("failed to enable child metrics: %v", err)
	}
	// Set effectively unlimited rate limiter so CPU time tokens are the
	// binding constraint.
	if _, err := systemConn.ExecContext(
		ctx, `SET CLUSTER SETTING kv.tenant_rate_limiter.rate_limit = '1000000'`,
	); err != nil {
		t.Fatalf("failed to set tenant rate limiter limit: %v", err)
	}

	// Start virtual clusters.
	quietName := "app-quiet"
	noisyName := "app-noisy"

	type vcInfo struct {
		name       string
		node       option.NodeListOption
		warehouses int // TPCC only
	}
	var vcs []vcInfo
	if s.tpcc != nil {
		vcs = []vcInfo{
			{quietName, quietSQLNode, s.tpcc.quietWarehouses},
			{noisyName, noisySQLNode, s.tpcc.noisyWarehouses},
		}
	} else {
		vcs = []vcInfo{
			{quietName, quietSQLNode, 0},
			{noisyName, noisySQLNode, 0},
		}
	}

	for _, vc := range vcs {
		c.StartServiceForVirtualCluster(
			ctx, t.L(),
			option.StartVirtualClusterOpts(vc.name, vc.node, option.NoBackupSchedule),
			install.MakeClusterSettings(),
		)
		t.L().Printf("virtual cluster %q started on n%d", vc.name, vc.node[0])

		// Set generous resource limits.
		_, err := systemConn.ExecContext(
			ctx, fmt.Sprintf(
				"SELECT crdb_internal.update_tenant_resource_limits('%s', 1000000000, 10000, 1000000)",
				vc.name,
			),
		)
		require.NoError(t, err)
	}

	// Initialize workload schema and load data in parallel across tenants.
	t.L().Printf("initializing and loading data for all tenants")
	annotatePhase("loading data")
	m1 := c.NewDeprecatedMonitor(ctx, c.All())
	for _, vc := range vcs {
		vc := vc
		m1.Go(func(ctx context.Context) error {
			if s.tpcc != nil {
				initCmd := fmt.Sprintf(
					"%s workload init tpcc --warehouses=%d {pgurl:%d:%s}",
					test.DefaultCockroachPath, vc.warehouses, vc.node[0], vc.name,
				)
				return c.RunE(ctx, option.WithNodes(vc.node), initCmd)
			}
			initCmd := fmt.Sprintf(
				"%s workload init kv {pgurl:%d:%s}",
				test.DefaultCockroachPath, vc.node[0], vc.name,
			)
			if err := c.RunE(ctx, option.WithNodes(vc.node), initCmd); err != nil {
				return err
			}
			pgurl := fmt.Sprintf("{pgurl:%d:%s}", vc.node[0], vc.name)
			loadCmd := roachtestutil.NewCommand("%s workload run kv", test.DefaultCockroachPath).
				Option("secure").
				Flag("min-block-bytes", s.kv.blockSize).
				Flag("max-block-bytes", s.kv.blockSize).
				Flag("batch", s.kv.batch).
				Flag("max-ops", s.kv.maxOps).
				Flag("concurrency", 25).
				Arg("%s", pgurl)
			return c.RunE(ctx, option.WithNodes(vc.node), loadCmd.String())
		})
	}
	m1.Wait()

	waitDur := 2 * time.Minute
	t.L().Printf("loaded data, sleeping %s", waitDur)
	annotatePhase("data loaded")
	time.Sleep(waitDur)

	// Reset SQL stats on quiet tenant before baseline measurement.
	quietConn := c.Conn(ctx, t.L(), quietSQLNode[0], option.VirtualClusterName(quietName))
	defer quietConn.Close()
	_, err := quietConn.ExecContext(ctx, "SELECT crdb_internal.reset_sql_stats()")
	require.NoError(t, err)

	// Phase 1: run only the quiet tenant workload to establish a baseline.
	phase1Duration := 5 * time.Minute
	t.L().Printf("phase 1: running quiet tenant only (%s)", phase1Duration)
	annotatePhase("phase 1: quiet tenant only")
	quietPgurl := fmt.Sprintf("{pgurl:%d:%s}", quietSQLNode[0], quietName)
	if s.tpcc != nil {
		quietCmd := roachtestutil.NewCommand("%s workload run tpcc", test.DefaultCockroachPath).
			Option("secure").
			Flag("warehouses", s.tpcc.quietWarehouses).
			Flag("duration", phase1Duration).
			Arg("%s", quietPgurl)
		c.Run(ctx, option.WithNodes(quietSQLNode), quietCmd.String())
	} else {
		quietCmd := roachtestutil.NewCommand("%s workload run kv", test.DefaultCockroachPath).
			Option("secure").
			Flag("write-seq", fmt.Sprintf("R%d", s.kv.maxOps*s.kv.batch)).
			Flag("min-block-bytes", s.kv.blockSize).
			Flag("max-block-bytes", s.kv.blockSize).
			Flag("batch", s.kv.batch).
			Flag("duration", phase1Duration).
			Flag("read-percent", s.kv.readPercent).
			Flag("concurrency", s.kv.quietConcurrency).
			Arg("%s", quietPgurl)
		c.Run(ctx, option.WithNodes(quietSQLNode), quietCmd.String())
	}

	// Collect baseline latency from statement statistics.
	dbName := "kv"
	if s.tpcc != nil {
		dbName = "tpcc"
	}
	_, err = quietConn.ExecContext(ctx, "USE "+dbName)
	require.NoError(t, err)

	baselineLatency, err := fetchAvgQuietLatency(ctx, quietConn, s)
	require.NoError(t, err)
	t.L().Printf("phase 1 baseline latency: %f", baselineLatency)

	// Reset SQL stats on quiet tenant before phase 2.
	_, err = quietConn.ExecContext(ctx, "SELECT crdb_internal.reset_sql_stats()")
	require.NoError(t, err)

	// Phase 2: run both tenants concurrently.
	phase2Duration := 15 * time.Minute
	t.L().Printf("phase 2: running both tenants (%s)", phase2Duration)
	annotatePhase("phase 2: both tenants")
	workloadStart := timeutil.Now()
	m2 := c.NewDeprecatedMonitor(ctx, c.All())

	// Quiet tenant workload.
	m2.Go(func(ctx context.Context) error {
		var cmd *roachtestutil.Command
		if s.tpcc != nil {
			cmd = roachtestutil.NewCommand("%s workload run tpcc", test.DefaultCockroachPath).
				Option("secure").
				Flag("warehouses", s.tpcc.quietWarehouses).
				Flag("duration", phase2Duration).
				Arg("%s", quietPgurl)
		} else {
			cmd = roachtestutil.NewCommand("%s workload run kv", test.DefaultCockroachPath).
				Option("secure").
				Flag("write-seq", fmt.Sprintf("R%d", s.kv.maxOps*s.kv.batch)).
				Flag("min-block-bytes", s.kv.blockSize).
				Flag("max-block-bytes", s.kv.blockSize).
				Flag("batch", s.kv.batch).
				Flag("duration", phase2Duration).
				Flag("read-percent", s.kv.readPercent).
				Flag("concurrency", s.kv.quietConcurrency).
				Arg("%s", quietPgurl)
		}
		return c.RunE(ctx, option.WithNodes(quietSQLNode), cmd.String())
	})

	// Run noisy tenant workload.
	noisyPgurl := fmt.Sprintf("{pgurl:%d:%s}", noisySQLNode[0], noisyName)
	m2.Go(func(ctx context.Context) error {
		var cmd *roachtestutil.Command
		if s.tpcc != nil {
			cmd = roachtestutil.NewCommand("%s workload run tpcc", test.DefaultCockroachPath).
				Option("secure").
				Option("tolerate-errors").
				Flag("warehouses", s.tpcc.noisyWarehouses).
				Flag("wait", 0).
				Flag("duration", phase2Duration).
				Arg("%s", noisyPgurl)
		} else {
			cmd = roachtestutil.NewCommand("%s workload run kv", test.DefaultCockroachPath).
				Option("secure").
				Option("tolerate-errors").
				Flag("write-seq", fmt.Sprintf("R%d", s.kv.maxOps*s.kv.batch)).
				Flag("min-block-bytes", s.kv.blockSize).
				Flag("max-block-bytes", s.kv.blockSize).
				Flag("batch", s.kv.batch).
				Flag("duration", phase2Duration).
				Flag("read-percent", s.kv.readPercent).
				Flag("concurrency", s.kv.noisyConcurrency).
				Arg("%s", noisyPgurl)
		}
		return c.RunE(ctx, option.WithNodes(noisySQLNode), cmd.String())
	})

	m2.Wait()
	workloadEnd := timeutil.Now()
	annotatePhase("workload complete")

	// Verify CPU utilization is near the target.
	verifyCPUUtilization(ctx, c, t, kvNode, workloadStart, workloadEnd)

	// Collect phase 2 latency from statement statistics.
	quietLatency, err := fetchAvgQuietLatency(ctx, quietConn, s)
	require.NoError(t, err)
	quietLatencyDur := time.Duration(quietLatency * float64(time.Second))
	baselineDur := time.Duration(baselineLatency * float64(time.Second))

	ratioStr := "n/a"
	if baselineLatency > 0 {
		ratioStr = fmt.Sprintf("%.2f", quietLatency/baselineLatency)
	}
	t.L().Printf("phase 2 quiet latency: %s (baseline: %s, ratio: %s)",
		quietLatencyDur, baselineDur, ratioStr)

	if quietLatencyDur > s.quietLatencyMax {
		t.Fatalf("quiet tenant latency %s exceeds max %s", quietLatencyDur, s.quietLatencyMax)
	}
}

// fetchAvgQuietLatency returns the avg run latency (in seconds) recorded for
// the quiet tenant's workload-fingerprint statements in the current SQL stats
// window. It returns an error if no matching statements are recorded so the
// caller fails with a descriptive message rather than the cryptic
// "converting NULL to float64" Scan error.
func fetchAvgQuietLatency(
	ctx context.Context, conn *gosql.DB, s multiTenantIsolationSpec,
) (float64, error) {
	var (
		row    *gosql.Row
		filter string
	)
	if s.kv != nil {
		filter = fmt.Sprintf(`db=kv, querySummary=%q`, s.kv.query)
		row = conn.QueryRowContext(ctx, `
			SELECT avg((statistics -> 'statistics' -> 'runLat' -> 'mean')::FLOAT)
			FROM crdb_internal.statement_statistics
			WHERE metadata @> '{"db":"kv"}' AND metadata @> $1`,
			fmt.Sprintf(`{"querySummary": "%s"}`, s.kv.query),
		)
	} else {
		filter = `db=tpcc`
		row = conn.QueryRowContext(ctx, `
			SELECT avg((statistics -> 'statistics' -> 'runLat' -> 'mean')::FLOAT)
			FROM crdb_internal.statement_statistics
			WHERE metadata @> '{"db":"tpcc"}'`,
		)
	}
	var avg gosql.NullFloat64
	if err := row.Scan(&avg); err != nil {
		return 0, errors.Wrapf(err, "scanning avg quiet latency (%s)", filter)
	}
	if !avg.Valid {
		return 0, errors.Newf("no statement statistics matched filter %s", filter)
	}
	return avg.Float64, nil
}

// cpuRampUpSamples is the number of leading 1-minute datapoints dropped from
// the CPU average to exclude workload ramp-up bias. The combined CPU
// timeseries is sampled at one-minute intervals, so 2 corresponds to ~2
// minutes of warm-up.
const cpuRampUpSamples = 2

// verifyCPUUtilization queries the combined CPU utilization metric over the
// given time window and asserts that the average is in the expected band.
// adminNode must designate a single KV node: the response is summed across
// sources, so multi-node lists would aggregate per-node fractions.
func verifyCPUUtilization(
	ctx context.Context,
	c cluster.Cluster,
	t test.Test,
	adminNode option.NodeListOption,
	start, end time.Time,
) {
	adminUIAddrs, err := c.ExternalAdminUIAddr(ctx, t.L(), adminNode)
	if err != nil {
		t.Fatal(err)
	}
	adminURL := adminUIAddrs[0]
	response := mustGetMetrics(
		ctx, c, t, adminURL, install.SystemInterfaceName, start, end, []tsQuery{
			{
				name:      "cr.node.sys.cpu.combined.percent-normalized",
				queryType: total,
			},
		})

	datapoints := response.Results[0].Datapoints
	// Drop ramp-up samples so the average reflects steady-state behavior
	// rather than the cold-start window.
	if len(datapoints) > cpuRampUpSamples {
		datapoints = datapoints[cpuRampUpSamples:]
	}
	if len(datapoints) == 0 {
		t.Fatal("not enough CPU utilization datapoints")
	}

	var sum float64
	for _, dp := range datapoints {
		sum += dp.Value
	}
	avgCPU := sum / float64(len(datapoints))

	t.L().Printf("average CPU utilization: %.2f%% (%d samples after %d ramp-up)",
		avgCPU*100, len(datapoints), cpuRampUpSamples)

	// The lower bound guards test validity: if CPU stays below the target
	// band, the workload isn't actually saturating the cluster and the
	// isolation/fairness signal isn't being exercised. The upper bound
	// guards AC behavior: CPU above the band means AC is failing to throttle
	// to its target utilization (default 0.8). The two failures have
	// different root causes so they get distinct messages.
	switch {
	case avgCPU < 0.70:
		t.Fatalf("workload did not saturate cluster: average CPU %.2f%% < 70%%", avgCPU*100)
	case avgCPU > 0.90:
		t.Fatalf("AC may be under-throttling: average CPU %.2f%% > 90%%", avgCPU*100)
	}
}

// dashboardLinkRe extracts the test_run_id from a roachtest cluster name of
// the form "<test_run_id>-NN-...".
var dashboardLinkRe = regexp.MustCompile(`^(.+)-\d{2}-`)

// logCPUTimeTokenDashboardLink logs a link to the CPU time token admission
// control dashboard on the shared Grafana instance. Best-effort: cluster
// names that do not match the expected pattern (e.g. local runs where
// c.Name() is "local") skip the link rather than failing the test.
func logCPUTimeTokenDashboardLink(t test.Test, c cluster.Cluster) {
	m := dashboardLinkRe.FindStringSubmatch(c.Name())
	if m == nil {
		t.L().Printf("skipping Grafana dashboard link: cluster name %q does not match expected pattern", c.Name())
		return
	}
	t.L().Printf(
		"Grafana dashboard: https://grafana.testeng.crdb.io/d/cpu-time-tokens/cpu-time-token-admission-control"+
			"?orgId=1&var-DS_PROMETHEUS=v9Zz2K6nz&var-test_run_id=%s&var-node=All",
		m[1],
	)
}
