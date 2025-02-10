// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type tpceSpec struct {
	loadNode         option.NodeListOption
	roachNodes       option.NodeListOption
	roachNodeIPFlags []string
	portFlag         string
}

type tpceCmdOptions struct {
	customers       int
	activeCustomers int
	racks           int
	duration        time.Duration
	threads         int
	skipCleanup     bool
	connectionOpts  tpceConnectionOpts
}

type tpceConnectionOpts struct {
	fixtureBucket string
	user          string
	password      string
}

const (
	defaultFixtureBucket = "gs://cockroach-fixtures-us-east1/tpce-csv"
	defaultUser          = install.DefaultUser
	defaultPassword      = install.DefaultPassword
)

func defaultTPCEConnectionOpts() tpceConnectionOpts {
	return tpceConnectionOpts{
		fixtureBucket: defaultFixtureBucket,
		user:          defaultUser,
		password:      defaultPassword,
	}
}

func (to tpceCmdOptions) AddCommandOptions(cmd *roachtestutil.Command) {
	cmd.MaybeFlag(to.customers != 0, "customers", to.customers)
	cmd.MaybeFlag(to.activeCustomers != 0, "active-customers", to.activeCustomers)
	cmd.MaybeFlag(to.racks != 0, "racks", to.racks)
	cmd.MaybeFlag(to.duration != 0, "duration", to.duration)
	cmd.MaybeFlag(to.threads != 0, "threads", to.threads)
	cmd.MaybeFlag(to.skipCleanup, "skip-cleanup", "")
	cmd.MaybeFlag(to.connectionOpts.fixtureBucket != "", "bucket", to.connectionOpts.fixtureBucket)
	cmd.MaybeFlag(to.connectionOpts.user != "", "pg-user", to.connectionOpts.user)
	cmd.MaybeFlag(to.connectionOpts.password != "", "pg-password", to.connectionOpts.password)
}

func initTPCESpec(ctx context.Context, l *logger.Logger, c cluster.Cluster) (*tpceSpec, error) {
	l.Printf("Installing docker")
	if err := c.Install(ctx, l, c.WorkloadNode(), "docker"); err != nil {
		return nil, err
	}
	roachNodeIPs, err := c.InternalIP(ctx, l, c.CRDBNodes())
	if err != nil {
		return nil, err
	}
	roachNodeIPFlags := make([]string, len(roachNodeIPs))
	for i, ip := range roachNodeIPs {
		roachNodeIPFlags[i] = fmt.Sprintf("--hosts=%s", ip)
	}
	ports, err := c.SQLPorts(ctx, l, c.CRDBNodes(), "" /* tenant */, 0 /* sqlInstance */)
	if err != nil {
		return nil, err
	}
	port := fmt.Sprintf("--pg-port=%d", ports[0])
	return &tpceSpec{
		loadNode:         c.WorkloadNode(),
		roachNodes:       c.CRDBNodes(),
		roachNodeIPFlags: roachNodeIPFlags,
		portFlag:         port,
	}, nil
}

func (ts *tpceSpec) newCmd(o tpceCmdOptions) *roachtestutil.Command {
	cmd := roachtestutil.NewCommand(`sudo docker run us-east1-docker.pkg.dev/crl-ci-images/cockroach/tpc-e:master`)
	o.AddCommandOptions(cmd)
	return cmd
}

// init initializes an empty cluster with a tpce database. This includes a bulk
// import of the data and schema creation.
func (ts *tpceSpec) init(ctx context.Context, t test.Test, c cluster.Cluster, o tpceCmdOptions) {
	cmd := ts.newCmd(o).Option("init")
	c.Run(ctx, option.WithNodes(ts.loadNode), fmt.Sprintf("%s %s %s", cmd, ts.portFlag, ts.roachNodeIPFlags[0]))
}

// run runs the tpce workload on cluster that has been initialized with the tpce schema.
func (ts *tpceSpec) run(
	ctx context.Context, t test.Test, c cluster.Cluster, o tpceCmdOptions,
) (install.RunResultDetails, error) {
	cmd := fmt.Sprintf("%s %s %s", ts.newCmd(o), ts.portFlag, strings.Join(ts.roachNodeIPFlags, " "))
	return c.RunWithDetailsSingleNode(ctx, t.L(), option.WithNodes(ts.loadNode), cmd)
}

type tpceOptions struct {
	// start is called to stage+start cockroach. If not specified, it defaults
	// to uploading the default binary to all nodes, and starting it on all but
	// the last node.
	start func(context.Context, test.Test, cluster.Cluster)

	customers int // --customers
	nodes     int // use to determine where the workload is run from, how data is partitioned, and workload concurrency
	cpus      int // used to determine workload concurrency
	ssds      int // used during cluster init

	// Promethues specific flags.
	//
	prometheusConfig  *prometheus.Config // overwrites the default prometheus config settings
	disablePrometheus bool               // forces prometheus to not start up

	// TPC-E init specific flags.
	//
	setupType          tpceSetupType
	estimatedSetupTime time.Duration // used only for logging

	// Workload specific flags.
	//
	onlySetup        bool                            // doesn't run the workload
	workloadDuration time.Duration                   // how long the workload runs for
	activeCustomers  int                             // --active-customers in the workload
	threads          int                             // overrides the number of threads used
	skipCleanup      bool                            // passes --skip-cleanup to the tpc-e workload
	exportMetrics    bool                            // exports metrics to stats.json used by roachperf
	during           func(ctx context.Context) error // invoked concurrently with the workload
}
type tpceSetupType int

const (
	usingTPCEInit         tpceSetupType = iota
	usingExistingTPCEData               // skips import
)

func runTPCE(ctx context.Context, t test.Test, c cluster.Cluster, opts tpceOptions) {
	racks := opts.nodes

	if opts.start == nil {
		opts.start = func(ctx context.Context, t test.Test, c cluster.Cluster) {
			t.Status("installing cockroach")
			startOpts := option.DefaultStartOpts()
			startOpts.RoachprodOpts.StoreCount = opts.ssds
			settings := install.MakeClusterSettings(install.NumRacksOption(racks))
			c.Start(ctx, t.L(), startOpts, settings, c.CRDBNodes())
		}
	}
	opts.start(ctx, t, c)

	tpceSpec, err := initTPCESpec(ctx, t.L(), c)
	require.NoError(t, err)

	// Configure to increase the speed of the import.
	{
		db := c.Conn(ctx, t.L(), 1)
		defer db.Close()
		if _, err := db.ExecContext(
			ctx, "SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false",
		); err != nil {
			t.Fatal(err)
		}
	}

	if !opts.disablePrometheus {
		// TODO(irfansharif): Move this after the import step? The statistics
		// during import itself is uninteresting and pollutes actual workload
		// data.
		var cleanupFunc func()
		_, cleanupFunc = setupPrometheusForRoachtest(ctx, t, c, opts.prometheusConfig, nil)
		defer cleanupFunc()
	}

	if opts.setupType == usingTPCEInit && !t.SkipInit() {
		m := c.NewMonitor(ctx, c.CRDBNodes())
		m.Go(func(ctx context.Context) error {
			estimatedSetupTimeStr := ""
			if opts.estimatedSetupTime != 0 {
				estimatedSetupTimeStr = fmt.Sprintf(" (<%s)", opts.estimatedSetupTime)
			}
			t.Status(fmt.Sprintf("initializing %d tpc-e customers%s", opts.customers, estimatedSetupTimeStr))
			tpceSpec.init(ctx, t, c, tpceCmdOptions{
				customers:      opts.customers,
				racks:          racks,
				connectionOpts: defaultTPCEConnectionOpts(),
			})
			return nil
		})
		m.Wait() // for init
	} else {
		t.Status("skipping tpc-e init")
	}
	if opts.onlySetup {
		return
	}

	m := c.NewMonitor(ctx, c.CRDBNodes())
	m.Go(func(ctx context.Context) error {
		t.Status("running workload")
		workloadDuration := opts.workloadDuration
		if workloadDuration == 0 {
			workloadDuration = 2 * time.Hour
		}
		runOptions := tpceCmdOptions{
			customers:      opts.customers,
			racks:          racks,
			duration:       workloadDuration,
			threads:        opts.nodes * opts.cpus,
			connectionOpts: defaultTPCEConnectionOpts(),
		}
		if opts.activeCustomers != 0 {
			runOptions.activeCustomers = opts.activeCustomers
		}
		if opts.threads != 0 {
			runOptions.threads = opts.threads
		}
		if opts.skipCleanup {
			runOptions.skipCleanup = opts.skipCleanup
		}
		result, err := tpceSpec.run(ctx, t, c, runOptions)
		if err != nil {
			t.Fatal(err.Error())
		}
		t.L().Printf("workload output:\n%s\n", result.Stdout)
		if strings.Contains(result.Stdout, "Reported tpsE :    --   (not between 80% and 100%)") {
			return errors.New("invalid tpsE fraction")
		}

		if opts.exportMetrics {
			return exportTPCEResults(t, c, result.Stdout)
		}
		return nil
	})
	if opts.during != nil {
		m.Go(opts.during)
	}
	m.Wait()
}

func registerTPCE(r registry.Registry) {
	// Nightly, small scale configuration.
	smallNightly := tpceOptions{
		customers:     5_000,
		nodes:         3,
		cpus:          4,
		ssds:          1,
		exportMetrics: true,
	}
	r.Add(registry.TestSpec{
		Name:             fmt.Sprintf("tpce/c=%d/nodes=%d", smallNightly.customers, smallNightly.nodes),
		Owner:            registry.OwnerTestEng,
		Benchmark:        true,
		Timeout:          4 * time.Hour,
		Cluster:          r.MakeClusterSpec(smallNightly.nodes+1, spec.CPU(smallNightly.cpus), spec.WorkloadNode(), spec.WorkloadNodeCPU(smallNightly.cpus), spec.SSD(smallNightly.ssds)),
		CompatibleClouds: registry.OnlyGCE,
		Suites:           registry.Suites(registry.Nightly),
		// Never run with runtime assertions as this makes this test take
		// too long to complete.
		CockroachBinary: registry.StandardCockroach,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runTPCE(ctx, t, c, smallNightly)
		},
	})

	// Weekly, large scale configuration.
	largeWeekly := tpceOptions{
		customers:     100_000,
		nodes:         5,
		cpus:          32,
		ssds:          2,
		exportMetrics: true,
	}
	r.Add(registry.TestSpec{
		Name:             fmt.Sprintf("tpce/c=%d/nodes=%d", largeWeekly.customers, largeWeekly.nodes),
		Owner:            registry.OwnerTestEng,
		Benchmark:        true,
		CompatibleClouds: registry.OnlyGCE,
		Suites:           registry.Suites(registry.Weekly),
		Timeout:          8 * time.Hour,
		Cluster:          r.MakeClusterSpec(largeWeekly.nodes+1, spec.CPU(largeWeekly.cpus), spec.WorkloadNode(), spec.WorkloadNodeCPU(largeWeekly.cpus), spec.SSD(largeWeekly.ssds)),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runTPCE(ctx, t, c, largeWeekly)
		},
	})
}

type tpceMetrics struct {
	AvgLatency  string `json:"AvgLatency"`
	P50Latency  string `json:"p50Latency"`
	P90Latency  string `json:"p90Latency"`
	P99Latency  string `json:"p99Latency"`
	PMaxLatency string `json:"pMaxLatency"`
}

// exportTPCEResults parses the output of `tpce` into a JSON file
// and writes it to the perf directory that roachperf expects. TPCE
// is an open-loop workload with a fixed rate of transactions, so
// we want roachperf to plot the latency. There are too many transaction
// types to plot, so we isolate just `TradeResult`:
//
// The Measured Throughput is computed as the total number of Valid Trade-Result Transactions
// within the Measurement Interval divided by the duration of the Measurement Interval in seconds.
func exportTPCEResults(t test.Test, c cluster.Cluster, result string) error {
	// Filter out everything but the TradeResult transaction metrics.
	//
	// Example output of TPCE:
	// _Transaction_______|__pass_mix___pass_lat__|______txns______txns/s_______mix__________avg__________p50__________p90__________p99_________pMax
	//  ...
	//  TradeResult       |     false       true  |     72114       10.02    9.926%     41.918ms     37.796ms     57.525ms    104.819ms    299.538ms
	s := bufio.NewScanner(strings.NewReader(result))
	for s.Scan() {
		if !strings.HasPrefix(s.Text(), " TradeResult") {
			continue
		}

		fields := strings.Fields(s.Text())
		if len(fields) != 13 {
			return errors.Errorf("exportTPCEResults: unexpected format of metrics")
		}

		removeUnits := func(s string) string {
			s, _ = strings.CutSuffix(s, "ms")
			return s
		}

		metrics := tpceMetrics{
			AvgLatency:  removeUnits(fields[8]),
			P50Latency:  removeUnits(fields[9]),
			P90Latency:  removeUnits(fields[10]),
			P99Latency:  removeUnits(fields[11]),
			PMaxLatency: removeUnits(fields[12]),
		}

		var metricBytes []byte
		var err error
		fileName := roachtestutil.GetBenchmarkMetricsFileName(t)
		if t.ExportOpenmetrics() {
			labels := map[string]string{
				"workload": "tpce",
			}
			labelString := roachtestutil.GetOpenmetricsLabelString(t, c, labels)
			metricBytes = getOpenMetrics(metrics, fields[5], labelString)
		} else {
			metricBytes, err = json.Marshal(metrics)
		}

		if err != nil {
			return err
		}

		// Copy the metrics to the artifacts directory, so it can be exported to roachperf.
		// Assume single node artifacts, since the metrics we get are aggregated amongst the cluster.
		perfDir := fmt.Sprintf("%s/1.perf", t.ArtifactsDir())
		if err = os.MkdirAll(perfDir, 0755); err != nil {
			return err
		}

		return os.WriteFile(fmt.Sprintf("%s/%s", perfDir, fileName), metricBytes, 0666)
	}
	return errors.Errorf("exportTPCEResults: found no lines starting with TradeResult")
}

func getOpenMetrics(metrics tpceMetrics, countOfLatencies string, labelString string) []byte {

	var buffer bytes.Buffer
	now := timeutil.Now().Unix()

	buffer.WriteString("# TYPE tpce_latency summary\n")
	buffer.WriteString("# HELP tpce_latency Latency metrics for TPC-E transactions\n")
	buffer.WriteString(fmt.Sprintf("tpce_latency{%s,quantile=\"0.5\"} %s %d\n", labelString, metrics.P50Latency, now))
	buffer.WriteString(fmt.Sprintf("tpce_latency{%s,quantile=\"0.9\"} %s %d\n", labelString, metrics.P90Latency, now))
	buffer.WriteString(fmt.Sprintf("tpce_latency{%s,quantile=\"0.99\"} %s %d\n", labelString, metrics.P99Latency, now))
	buffer.WriteString(fmt.Sprintf("tpce_latency{%s,quantile=\"1.0\"} %s %d\n", labelString, metrics.PMaxLatency, now))
	buffer.WriteString(fmt.Sprintf("tpce_latency_sum{%s} %d %d\n", labelString, 0, now))
	buffer.WriteString(fmt.Sprintf("tpce_latency_count{%s} %s %d\n", labelString, countOfLatencies, now))
	buffer.WriteString("# EOF")

	metricsBytes := buffer.Bytes()
	return metricsBytes
}
