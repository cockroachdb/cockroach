// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"io"
	"maps"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/mixedversion"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/util/search"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram/exporter"
	"github.com/cockroachdb/cockroach/pkg/workload/tpcc"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/ttycolor"
	"github.com/codahale/hdrhistogram"
	"github.com/lib/pq"
	promapi "github.com/prometheus/client_golang/api"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/stretchr/testify/require"
)

type tpccSetupType int

const (
	usingImport tpccSetupType = iota
	usingInit
	usingExistingData // skips import
)

// rampDuration returns the default durations passed to the `ramp`
// option when running a tpcc workload in these tests.
func rampDuration(isLocal bool) time.Duration {
	if isLocal {
		return 30 * time.Second
	}

	return 5 * time.Minute
}

type tpccOptions struct {
	DB                 string // database name
	Warehouses         int
	WorkloadCmd        string // defaults to tpcc if empty
	ExtraRunArgs       string
	ExtraSetupArgs     string
	Chaos              func() Chaos // for late binding of stopper
	ExpectedDeaths     int
	During             func(context.Context) error // for running a function during the test
	Duration           time.Duration               // if zero, TPCC is not invoked
	SetupType          tpccSetupType
	EstimatedSetupTime time.Duration
	// PrometheusConfig, if set, overwrites the default prometheus config settings.
	PrometheusConfig *prometheus.Config
	// DisablePrometheus will force prometheus to not start up.
	DisablePrometheus bool
	// WorkloadInstances contains a list of instances for
	// workloads to run against.
	// If unset, it will run one workload which talks to
	// all cluster nodes.
	WorkloadInstances []workloadInstance
	// tpccChaosEventProcessor processes chaos events if specified.
	ChaosEventsProcessor func(
		prometheusNode option.NodeListOption,
		workloadInstances []workloadInstance,
	) (tpccChaosEventProcessor, error)
	// If specified, called to stage+start cockroach. If not
	// specified, defaults to uploading the default binary to
	// all nodes, and starting it on all but the last node.
	Start func(context.Context, test.Test, cluster.Cluster)
	// If specified, assigned to StartOpts.ExtraArgs when starting cockroach.
	ExtraStartArgs                []string
	DisableDefaultScheduledBackup bool
	// SkipPostRunCheck, if set, skips post TPC-C run checks.
	SkipPostRunCheck bool
	// SkipSetup if set, skips the setup step.
	SkipSetup bool
	// ExpensiveChecks, if set, runs expensive post TPC-C run checks.
	ExpensiveChecks bool
	// DisableIsolationLevels will cause the workload to not attempt to
	// enable the corresponding cluster settings for the different
	// isolation levels in the database. Useful if we are not testing
	// these isolation levels and we are bootstrapping the cluster in an
	// older version, where they are not supported.
	DisableIsolationLevels bool
	// DisableHistogram will determine if the histogram argument should
	// be passed in.
	DisableHistogram bool
}

func (t tpccOptions) getWorkloadCmd() string {
	if t.WorkloadCmd == "" {
		return "tpcc"
	}
	return t.WorkloadCmd
}

type workloadInstance struct {
	// nodes dictates the nodes workload should run against.
	nodes option.NodeListOption
	// prometheusPort is the port on the workload which runs
	// prometheus.
	prometheusPort int
	// extraRunArgs dictates unique arguments to use for the workload.
	extraRunArgs string
}

const workloadPProfStartPort = 33333

// tpccImportCmd see tpccImportCmdWithCockroachBinary, this variant is set up to
// invoke the default tpcc subcommand
func tpccImportCmd(db string, warehouses int, extraArgs ...string) string {
	return tpccImportCmdWithCockroachBinary(test.DefaultCockroachPath, db, "tpcc", warehouses, extraArgs...)
}

// tpccImportCmdWithCockroachBinary generates the command string to load tpcc data
// for the specified warehouse count into a cluster.
//
// The command uses `cockroach workload` instead of `workload` so the tpcc
// workload-versions match on release branches. Similarly, the command does not
// specify pgurl to ensure that it is run on a node with a running cockroach
// instance to ensure that the workload version matches the gateway version in a
// mixed version cluster. The subcommand used can be specified for variants of
// tpcc.
func tpccImportCmdWithCockroachBinary(
	crdbBinary string, db string, workloadCmd string, warehouses int, extraArgs ...string,
) string {
	return roachtestutil.NewCommand("%s workload fixtures import %s", crdbBinary, workloadCmd).
		MaybeFlag(db != "", "db", db).
		Flag("warehouses", warehouses).
		Arg("%s", strings.Join(extraArgs, " ")).
		String()
}

func setupTPCC(
	ctx context.Context, t test.Test, l *logger.Logger, c cluster.Cluster, opts tpccOptions,
) {
	// If setup should be skipped, then nothing to o here.
	if opts.SkipSetup {
		return
	}

	if c.IsLocal() {
		opts.Warehouses = 1
	}

	if opts.Start == nil {
		opts.Start = func(ctx context.Context, t test.Test, c cluster.Cluster) {
			settings := install.MakeClusterSettings()
			if c.IsLocal() {
				settings.Env = append(settings.Env, "COCKROACH_SCAN_INTERVAL=200ms")
				settings.Env = append(settings.Env, "COCKROACH_SCAN_MAX_IDLE_TIME=5ms")
			}
			startOpts := option.DefaultStartOpts()
			startOpts.RoachprodOpts.ExtraArgs = opts.ExtraStartArgs
			startOpts.RoachprodOpts.ScheduleBackups = !opts.DisableDefaultScheduledBackup
			c.Start(ctx, l, startOpts, settings, c.CRDBNodes())
		}
	}

	func() {
		opts.Start(ctx, t, c)
		db := c.Conn(ctx, l, 1)
		defer db.Close()

		if t.SkipInit() {
			return
		}

		if !opts.DisableIsolationLevels {
			require.NoError(t, enableIsolationLevels(ctx, t, db))
		}

		require.NoError(t, roachtestutil.WaitFor3XReplication(ctx, l, db))

		estimatedSetupTimeStr := ""
		if opts.EstimatedSetupTime != 0 {
			estimatedSetupTimeStr = fmt.Sprintf(" (<%s)", opts.EstimatedSetupTime)
		}

		switch opts.SetupType {
		case usingExistingData:
			// Do nothing.
		case usingImport:
			t.Status("loading fixture" + estimatedSetupTimeStr)
			c.Run(ctx, option.WithNodes(c.Node(1)), tpccImportCmdWithCockroachBinary(test.DefaultCockroachPath, opts.DB, opts.getWorkloadCmd(), opts.Warehouses, opts.ExtraSetupArgs, "{pgurl:1}"))
		case usingInit:
			l.Printf("initializing tables" + estimatedSetupTimeStr)
			extraArgs := opts.ExtraSetupArgs
			cmd := roachtestutil.NewCommand("%s workload init %s", test.DefaultCockroachPath, opts.getWorkloadCmd()).
				MaybeFlag(opts.DB != "", "db", opts.DB).
				Flag("warehouses", opts.Warehouses).
				Arg("%s", extraArgs).
				Arg("%s", "{pgurl:1}")

			c.Run(ctx, option.WithNodes(c.WorkloadNode()), cmd.String())
		default:
			t.Fatal("unknown tpcc setup type")
		}
		l.Printf("finished tpc-c setup")
	}()
}

func runTPCC(
	ctx context.Context, t test.Test, l *logger.Logger, c cluster.Cluster, opts tpccOptions,
) {
	workloadInstances := opts.WorkloadInstances
	if len(workloadInstances) == 0 {
		workloadInstances = append(
			workloadInstances,
			workloadInstance{
				nodes:          c.CRDBNodes(),
				prometheusPort: 2112,
			},
		)
	}
	var pgURLs []string
	for _, workloadInstance := range workloadInstances {
		pgURLs = append(pgURLs, fmt.Sprintf("{pgurl%s}", workloadInstance.nodes.String()))
	}

	var ep *tpccChaosEventProcessor
	var promCfg *prometheus.Config
	if !opts.DisablePrometheus && !t.SkipInit() {
		// TODO(irfansharif): Move this after the import step. The statistics
		// during import itself is uninteresting and pollutes actual workload
		// data.
		var cleanupFunc func()
		promCfg, cleanupFunc = setupPrometheusForRoachtest(ctx, t, c, opts.PrometheusConfig, workloadInstances)
		defer cleanupFunc()
	}
	if opts.ChaosEventsProcessor != nil {
		if promCfg == nil {
			t.Skip("skipping test as prometheus is needed, but prometheus does not yet work locally")
			return
		}
		cep, err := opts.ChaosEventsProcessor(
			c.Nodes(int(promCfg.PrometheusNode)),
			workloadInstances,
		)
		if err != nil {
			t.Fatal(err)
		}
		cep.listen(ctx, t, l)
		ep = &cep
	}

	if c.IsLocal() {
		opts.Warehouses = 1
		if opts.Duration > time.Minute {
			opts.Duration = time.Minute
		}
	}
	setupTPCC(ctx, t, l, c, opts)
	m := c.NewMonitor(ctx, c.CRDBNodes())
	m.ExpectDeaths(int32(opts.ExpectedDeaths))
	rampDur := rampDuration(c.IsLocal())
	for i := range workloadInstances {
		// Make a copy of i for the goroutine.
		i := i
		m.Go(func(ctx context.Context) error {
			// Only prefix stats file with workload_i_ if we have multiple workloads,
			// in case other processes relied on previous behavior.
			var statsPrefix string
			if len(workloadInstances) > 1 {
				statsPrefix = fmt.Sprintf("workload_%d.", i)
			}
			l.Printf("running tpcc worker=%d warehouses=%d ramp=%s duration=%s on %s (<%s)",
				i, opts.Warehouses, rampDur, opts.Duration, pgURLs[i], time.Minute)

			fileName := roachtestutil.GetBenchmarkMetricsFileName(t)
			histogramsPath := fmt.Sprintf("%s/%s%s", t.PerfArtifactsDir(), statsPrefix, fileName)
			var labelsMap map[string]string
			if t.ExportOpenmetrics() {
				labelsMap = getTpccLabels(opts.Warehouses, rampDur, opts.Duration, map[string]string{"database": opts.DB})
			}
			cmd := roachtestutil.NewCommand("%s workload run %s", test.DefaultCockroachPath, opts.getWorkloadCmd()).
				MaybeFlag(opts.DB != "", "db", opts.DB).
				Flag("warehouses", opts.Warehouses).
				MaybeFlag(!opts.DisableHistogram, "histograms", histogramsPath).
				MaybeFlag(!opts.DisableHistogram && t.ExportOpenmetrics(), "histogram-export-format", "openmetrics").
				MaybeFlag(!opts.DisableHistogram && t.ExportOpenmetrics(), "openmetrics-labels", roachtestutil.GetOpenmetricsLabelString(t, c, labelsMap)).
				Flag("ramp", rampDur).
				Flag("duration", opts.Duration).
				Flag("prometheus-port", workloadInstances[i].prometheusPort).
				Flag("pprofport", workloadPProfStartPort+i).
				Arg("%s", opts.ExtraRunArgs).
				Arg("%s", workloadInstances[i].extraRunArgs).
				Arg("%s", pgURLs[i])

			err := c.RunE(ctx, option.WithNodes(c.WorkloadNode()), cmd.String())
			// Don't fail the test if we are running the workload throughout
			// the entire test. Canceling the context just means that the
			// test already failed, or that the test finished (and therefore
			// this function should not return an error).
			if ctx.Err() != nil && opts.Duration == 0 {
				return nil
			}

			return err
		})
	}
	if opts.Chaos != nil {
		chaos := opts.Chaos()
		m.Go(chaos.Runner(c, t, m))
	}
	if opts.During != nil {
		m.Go(opts.During)
	}
	m.Wait()

	if !opts.SkipPostRunCheck {
		cmd := roachtestutil.NewCommand("%s workload check %s", test.DefaultCockroachPath, opts.getWorkloadCmd()).
			MaybeFlag(opts.DB != "", "db", opts.DB).
			MaybeOption(opts.ExpensiveChecks, "expensive-checks").
			Flag("warehouses", opts.Warehouses).
			Arg("{pgurl:1}")

		c.Run(ctx, option.WithNodes(c.WorkloadNode()), cmd.String())
	}

	// Check no errors from metrics.
	if ep != nil {
		if err := ep.err(); err != nil {
			t.Fatal(errors.Wrap(err, "error detected during DRT"))
		}
	}
}

// tpccSupportedWarehouses returns our claim for the maximum number of tpcc
// warehouses we support for a given hardware configuration.
//
// These should be added to periodically. Ideally when tpccbench finds major
// performance movement, but at the least for every major release.
var tpccSupportedWarehouses = []struct {
	hardware   string
	v          *version.Version
	warehouses int
}{
	// TODO(darrylwong): these numbers are old, with azure and n5 cluster values being copied
	// from other configs. We should take another pass to figure out more current numbers.

	// We append "-0" to the version so that we capture all prereleases of the
	// specified version. Otherwise, "v2.1.0" would compare greater than
	// "v2.1.0-alpha.x".
	{hardware: "gce-n4cpu16", v: version.MustParse(`v2.1.0-0`), warehouses: 1300},
	{hardware: "gce-n4cpu16", v: version.MustParse(`v19.1.0-0`), warehouses: 1250},
	{hardware: "aws-n4cpu16", v: version.MustParse(`v19.1.0-0`), warehouses: 2100},
	{hardware: "azure-n4cpu16", v: version.MustParse(`v19.1.0-0`), warehouses: 1300},

	// TODO(tbg): this number is copied from gce-n4cpu16. The real number should be a
	// little higher, find out what it is.
	{hardware: "gce-n5cpu16", v: version.MustParse(`v19.1.0-0`), warehouses: 1300},
	{hardware: "aws-n5cpu16", v: version.MustParse(`v19.1.0-0`), warehouses: 2100},
	{hardware: "azure-n5cpu16", v: version.MustParse(`v19.1.0-0`), warehouses: 1300},
	// Ditto.
	{hardware: "gce-n5cpu16", v: version.MustParse(`v2.1.0-0`), warehouses: 1300},
}

// tpccMaxRate calculates the max rate of the workload given a number of warehouses.
func tpccMaxRate(warehouses int) int {
	const txnsPerWarehousePerSecond = 12.8 * (23.0 / 10.0) * (1.0 / 60.0) // max_tpmC/warehouse * all_txns/new_order_txns * minutes/seconds
	rateAtExpected := txnsPerWarehousePerSecond * float64(warehouses)
	maxRate := int(rateAtExpected / 2)
	return maxRate
}

func maxSupportedTPCCWarehouses(
	buildVersion version.Version, cloud spec.Cloud, nodes spec.ClusterSpec,
) int {
	if cloud == spec.Local {
		// Arbitrary number since the limit depends on the machine, local TPCC runs
		// are usually used for dry runs and not actual performance testing.
		return 15
	}

	var v *version.Version
	var warehouses int
	hardware := fmt.Sprintf(`%s-%s`, cloud, &nodes)
	for _, x := range tpccSupportedWarehouses {
		if x.hardware != hardware {
			continue
		}
		if buildVersion.AtLeast(x.v) && (v == nil || buildVersion.AtLeast(v)) {
			v = x.v
			warehouses = x.warehouses
		}
	}
	if v == nil {
		panic(fmt.Sprintf(`could not find max tpcc warehouses for %s`, hardware))
	}
	return warehouses
}

// runTPCCMixedHeadroom runs a mixed-version test that imports a large
// `bank` dataset, and runs multiple database upgrades while a TPCC
// workload is running. The number of database upgrades is randomized
// by the mixed-version framework which chooses a random predecessor version
// and upgrades until it reaches the current version.
func runTPCCMixedHeadroom(ctx context.Context, t test.Test, c cluster.Cluster) {
	maxWarehouses := maxSupportedTPCCWarehouses(*t.BuildVersion(), c.Cloud(), c.Spec())
	headroomWarehouses := int(float64(maxWarehouses) * 0.7)

	// NB: this results in ~100GB of (actual) disk usage per node once things
	// have settled down, and ~7.5k ranges. The import takes ~40 minutes.
	// The full 6.5m import ran into out of disk errors (on 250gb machines),
	// hence division by two.
	bankRows := 65104166 / 2
	if c.IsLocal() {
		bankRows = 1000
	}

	mvt := mixedversion.NewTest(
		ctx, t, t.L(), c, c.CRDBNodes(),
		// We test only upgrades from 23.2 in this test because it uses
		// the `workload fixtures import` command, which is only supported
		// reliably multi-tenant mode starting from that version.
		mixedversion.MinimumSupportedVersion("v23.2.0"),
		// We limit the total number of plan steps to 70, which is roughly 80% of all plan lengths.
		// See #138014 for more details.
		mixedversion.MaxNumPlanSteps(70),
	)

	tenantFeaturesEnabled := make(chan struct{})
	enableTenantFeatures := func(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper) error {
		defer close(tenantFeaturesEnabled)
		return enableTenantSplitScatter(l, rng, h)
	}

	importTPCC := func(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper) error {
		l.Printf("waiting for tenant features to be enabled")
		<-tenantFeaturesEnabled

		randomNode := c.Node(c.CRDBNodes().SeededRandNode(rng)[0])
		cmd := tpccImportCmdWithCockroachBinary(test.DefaultCockroachPath, "", "tpcc", headroomWarehouses, fmt.Sprintf("{pgurl%s}", randomNode))
		return c.RunE(ctx, option.WithNodes(randomNode), cmd)
	}

	// Add a lot of cold data to this cluster. This further stresses the version
	// upgrade machinery, in which a) all ranges are touched and b) work proportional
	// to the amount data may be carried out.
	importLargeBank := func(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper) error {
		l.Printf("waiting for tenant features to be enabled")
		<-tenantFeaturesEnabled

		randomNode := c.Node(c.CRDBNodes().SeededRandNode(rng)[0])
		cmd := roachtestutil.NewCommand("%s workload fixtures import bank", test.DefaultCockroachPath).
			Arg("{pgurl%s}", randomNode).
			Flag("payload-bytes", 10240).
			Flag("rows", bankRows).
			Flag("seed", 4).
			Flag("db", "bigbank").
			String()
		return c.RunE(ctx, option.WithNodes(randomNode), cmd)
	}

	// We don't run this in the background using the Workload() wrapper. We want
	// it to block and wait for the workload to ramp up before attempting to upgrade
	// the cluster version. If we start the migrations immediately after launching
	// the tpcc workload, they could finish "too quickly", before the workload had
	// a chance to pick up the pace (starting all the workers, range merge/splits,
	// compactions, etc). By waiting here, we increase the concurrency exposed to
	// the upgrade migrations, and increase the chances of exposing bugs (such as #83079).
	runTPCCWorkload := func(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper) error {
		workloadDur := 10 * time.Minute
		rampDur := rampDuration(c.IsLocal())
		// If migrations are running we want to ramp up the workload faster in order
		// to expose them to more concurrent load. In a similar goal, we also let the
		// TPCC workload run longer.
		if h.IsFinalizing() && !c.IsLocal() {
			rampDur = 1 * time.Minute
			if h.Context().ToVersion.IsCurrent() {
				workloadDur = 100 * time.Minute
			}
		}
		histogramsPath := fmt.Sprintf("%s/%s", t.PerfArtifactsDir(), roachtestutil.GetBenchmarkMetricsFileName(t))
		var labelsMap map[string]string
		if t.ExportOpenmetrics() {
			labelsMap = getTpccLabels(headroomWarehouses, rampDur, workloadDur/time.Millisecond, nil)
		}
		cmd := roachtestutil.NewCommand("./cockroach workload run tpcc").
			Arg("{pgurl%s}", c.CRDBNodes()).
			Flag("duration", workloadDur).
			Flag("warehouses", headroomWarehouses).
			Flag("histograms", histogramsPath).
			MaybeFlag(t.ExportOpenmetrics(), "histogram-export-format", "openmetrics").
			MaybeFlag(t.ExportOpenmetrics(), "openmetrics-labels", roachtestutil.GetOpenmetricsLabelString(t, c, labelsMap)).
			Flag("ramp", rampDur).
			Flag("prometheus-port", 2112).
			Flag("pprofport", workloadPProfStartPort).
			String()
		return c.RunE(ctx, option.WithNodes(c.WorkloadNode()), cmd)
	}

	checkTPCCWorkload := func(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper) error {
		cmd := roachtestutil.NewCommand("%s workload check tpcc", test.DefaultCockroachPath).
			Arg("{pgurl:1}").
			Flag("warehouses", headroomWarehouses).
			String()
		return c.RunE(ctx, option.WithNodes(c.WorkloadNode()), cmd)
	}

	mvt.OnStartup("maybe enable tenant features", enableTenantFeatures)
	mvt.OnStartup("load TPCC dataset", importTPCC)
	mvt.OnStartup("load bank dataset", importLargeBank)
	mvt.InMixedVersion("TPCC workload", runTPCCWorkload)
	mvt.AfterUpgradeFinalized("check TPCC workload", checkTPCCWorkload)
	mvt.Run()
}

func registerTPCC(r registry.Registry) {
	headroomSpec := r.MakeClusterSpec(4, spec.CPU(16), spec.WorkloadNode(), spec.RandomlyUseZfs())
	r.Add(registry.TestSpec{
		// w=headroom runs tpcc for a semi-extended period with some amount of
		// headroom, more closely mirroring a real production deployment than
		// running with the max supported warehouses.
		Name:              "tpcc/headroom/" + headroomSpec.String(),
		Owner:             registry.OwnerTestEng,
		Benchmark:         true,
		CompatibleClouds:  registry.AllClouds,
		Suites:            registry.Suites(registry.Nightly, registry.ReleaseQualification),
		Cluster:           headroomSpec,
		Timeout:           4 * time.Hour,
		EncryptionSupport: registry.EncryptionMetamorphic,
		Leases:            registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			maxWarehouses := maxSupportedTPCCWarehouses(*t.BuildVersion(), c.Cloud(), c.Spec())
			headroomWarehouses := int(float64(maxWarehouses) * 0.7)
			t.L().Printf("computed headroom warehouses of %d\n", headroomWarehouses)
			runTPCC(ctx, t, t.L(), c, tpccOptions{
				Warehouses: headroomWarehouses,
				Duration:   120 * time.Minute,
				SetupType:  usingImport,
			})
		},
	})
	r.Add(registry.TestSpec{
		Name:              "tpcc/headroom/isolation-level=read-committed/" + headroomSpec.String(),
		Owner:             registry.OwnerTestEng,
		Benchmark:         true,
		CompatibleClouds:  registry.AllExceptAWS,
		Suites:            registry.Suites(registry.Nightly),
		Cluster:           headroomSpec,
		Timeout:           4 * time.Hour,
		EncryptionSupport: registry.EncryptionMetamorphic,
		Leases:            registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			maxWarehouses := maxSupportedTPCCWarehouses(*t.BuildVersion(), c.Cloud(), c.Spec())
			headroomWarehouses := int(float64(maxWarehouses) * 0.7)
			t.L().Printf("computed headroom warehouses of %d\n", headroomWarehouses)
			runTPCC(ctx, t, t.L(), c, tpccOptions{
				Warehouses:   headroomWarehouses,
				ExtraRunArgs: "--isolation-level=read_committed --txn-retries=false",
				Duration:     120 * time.Minute,
				SetupType:    usingImport,
				// Increase the vmodule level around transaction pushes so that if we do
				// see a transaction retry error, we can debug it. This may affect perf,
				// so we should not use this as a performance test.
				ExtraStartArgs: []string{"--vmodule=cmd_push_txn=2,queue=2"},
			})
		},
	})

	mixedHeadroomSpec := r.MakeClusterSpec(5, spec.CPU(16), spec.WorkloadNode(), spec.RandomlyUseZfs())
	r.Add(registry.TestSpec{
		// mixed-headroom is similar to w=headroom, but with an additional
		// node and on a mixed version cluster which runs its long-running
		// migrations while TPCC runs. It simulates a real production
		// deployment in the middle of the migration into a new cluster version.
		Name:    "tpcc/mixed-headroom/" + mixedHeadroomSpec.String(),
		Timeout: 7 * time.Hour,
		Owner:   registry.OwnerTestEng,
		// TODO(tbg): add release_qualification tag once we know the test isn't
		// buggy.
		CompatibleClouds:  registry.AllExceptAWS,
		Suites:            registry.Suites(registry.MixedVersion, registry.Nightly),
		Cluster:           mixedHeadroomSpec,
		EncryptionSupport: registry.EncryptionMetamorphic,
		Randomized:        true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runTPCCMixedHeadroom(ctx, t, c)
		},
	})

	r.Add(registry.TestSpec{
		Name:              "tpcc-nowait/nodes=3/w=1",
		Owner:             registry.OwnerTestEng,
		Benchmark:         true,
		Cluster:           r.MakeClusterSpec(4, spec.CPU(16), spec.WorkloadNode()),
		CompatibleClouds:  registry.AllExceptAWS,
		Suites:            registry.Suites(registry.Nightly),
		EncryptionSupport: registry.EncryptionMetamorphic,
		Leases:            registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runTPCC(ctx, t, t.L(), c, tpccOptions{
				Warehouses:      1,
				Duration:        10 * time.Minute,
				ExtraRunArgs:    "--wait=false",
				SetupType:       usingImport,
				ExpensiveChecks: true,
			})
		},
	})
	r.Add(registry.TestSpec{
		Name:              "tpcc-nowait/isolation-level=snapshot/nodes=3/w=1",
		Owner:             registry.OwnerTestEng,
		Benchmark:         true,
		Cluster:           r.MakeClusterSpec(4, spec.CPU(16), spec.WorkloadNode()),
		CompatibleClouds:  registry.AllExceptAWS,
		Suites:            registry.Suites(registry.Nightly),
		EncryptionSupport: registry.EncryptionMetamorphic,
		Leases:            registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runTPCC(ctx, t, t.L(), c, tpccOptions{
				Warehouses:      1,
				Duration:        10 * time.Minute,
				ExtraRunArgs:    "--wait=false --isolation-level=snapshot",
				SetupType:       usingImport,
				ExpensiveChecks: true,
			})
		},
	})
	r.Add(registry.TestSpec{
		Name:              "tpcc-nowait/isolation-level=read-committed/nodes=3/w=1",
		Owner:             registry.OwnerTestEng,
		Benchmark:         true,
		Cluster:           r.MakeClusterSpec(4, spec.CPU(16), spec.WorkloadNode()),
		CompatibleClouds:  registry.AllExceptAWS,
		Suites:            registry.Suites(registry.Nightly),
		EncryptionSupport: registry.EncryptionMetamorphic,
		Leases:            registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runTPCC(ctx, t, t.L(), c, tpccOptions{
				Warehouses:      1,
				Duration:        10 * time.Minute,
				ExtraRunArgs:    "--wait=false --isolation-level=read_committed --txn-retries=false",
				SetupType:       usingImport,
				ExpensiveChecks: true,
				// Increase the vmodule level around transaction pushes so that if we do
				// see a transaction retry error, we can debug it. This may affect perf,
				// so we should not use this as a performance test.
				ExtraStartArgs: []string{"--vmodule=cmd_push_txn=2,queue=2,transaction=2"},
			})
		},
	})
	r.Add(registry.TestSpec{
		Name:              "tpcc-nowait/isolation-level=mixed/nodes=3/w=1",
		Owner:             registry.OwnerTestEng,
		Benchmark:         true,
		Cluster:           r.MakeClusterSpec(4, spec.CPU(16), spec.WorkloadNode()),
		CompatibleClouds:  registry.AllExceptAWS,
		Suites:            registry.Suites(registry.Nightly),
		EncryptionSupport: registry.EncryptionMetamorphic,
		Leases:            registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runTPCC(ctx, t, t.L(), c, tpccOptions{
				Warehouses:      5,
				Duration:        10 * time.Minute,
				ExtraRunArgs:    "--wait=false",
				SetupType:       usingImport,
				ExpensiveChecks: true,
				WorkloadInstances: func() (ret []workloadInstance) {
					isoLevels := []string{"read_uncommitted", "read_committed", "repeatable_read", "snapshot", "serializable"}
					for i, isoLevel := range isoLevels {
						args := "--isolation-level=" + isoLevel
						switch isoLevel {
						case "read_uncommitted", "read_committed":
							// Disable retries for read uncommitted and read committed. These
							// isolation levels are weak enough that we don't expect 40001
							// "serialization_failure" errors which would necessitate a
							// transaction retry loop. If we do see a 40001 error when running
							// at one of these isolation levels, fail the test.
							args += " --txn-retries=false"
						case "serializable":
							// Enable durable locking for serializable transactions. This
							// ensures that we do not run into issues with best-effort locks
							// acquired by SELECT FOR UPDATE being lost and creating lock
							// order inversions which lead to transaction deadlocks.
							args += " --conn-vars=enable_durable_locking_for_serializable=true"
						}
						ret = append(ret, workloadInstance{
							nodes:          c.CRDBNodes(),
							prometheusPort: 2112 + i,
							extraRunArgs:   args,
						})
					}
					return ret
				}(),
			})
		},
	})

	r.Add(registry.TestSpec{
		Name:             "weekly/tpcc/headroom",
		Owner:            registry.OwnerTestEng,
		Benchmark:        true,
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Weekly),
		Cluster:          r.MakeClusterSpec(4, spec.CPU(16), spec.WorkloadNode()),
		// Give the test a generous extra 10 hours to load the dataset and
		// slowly ramp up the load.
		Timeout:           4*24*time.Hour + 10*time.Hour,
		EncryptionSupport: registry.EncryptionMetamorphic,
		Leases:            registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			warehouses := 1000
			runTPCC(ctx, t, t.L(), c, tpccOptions{
				Warehouses: warehouses,
				Duration:   4 * 24 * time.Hour,
				SetupType:  usingImport,
			})
		},
	})

	// Setup multi-region tests.
	{
		mrSetup := []struct {
			region string
			zones  string
		}{
			{region: "us-east1", zones: "us-east1-b"},
			{region: "us-west1", zones: "us-west1-b"},
			{region: "europe-west2", zones: "europe-west2-b"},
		}
		zs := []string{}
		// NOTE: region is currently only used for number of regions.
		regions := []string{}
		for _, s := range mrSetup {
			regions = append(regions, s.region)
			zs = append(zs, s.zones)
		}
		const nodesPerRegion = 3
		const warehousesPerRegion = 20

		multiRegionTests := []struct {
			desc         string
			name         string
			survivalGoal string

			chaosTarget       func(iter int) option.NodeListOption
			workloadInstances []workloadInstance
		}{
			{
				desc:         "test zone survivability works when single nodes are down",
				name:         "tpcc/multiregion/survive=zone/chaos=true",
				survivalGoal: "zone",
				chaosTarget: func(iter int) option.NodeListOption {
					return option.NodeListOption{(iter % (len(regions) * nodesPerRegion)) + 1}
				},
				workloadInstances: func() []workloadInstance {
					const prometheusPortStart = 2110
					ret := []workloadInstance{}
					for i := 0; i < len(regions)*nodesPerRegion; i++ {
						ret = append(
							ret,
							workloadInstance{
								nodes:          option.NodeListOption{i + 1}, // 1-indexed
								prometheusPort: prometheusPortStart + i,
								extraRunArgs:   fmt.Sprintf("--partition-affinity=%d", i/nodesPerRegion), // 0-indexed
							},
						)
					}
					return ret
				}(),
			},
			{
				desc:         "test region survivability works when regions going down",
				name:         "tpcc/multiregion/survive=region/chaos=true",
				survivalGoal: "region",
				chaosTarget: func(iter int) option.NodeListOption {
					regionIdx := iter % len(regions)
					return option.NewNodeListOptionRange(
						(nodesPerRegion*regionIdx)+1,
						(nodesPerRegion * (regionIdx + 1)),
					)
				},
				workloadInstances: func() []workloadInstance {
					// We run two sets of workloads:
					// * for each region, have a workload that speaks SQL which only affects data
					//   that is partitioned in the same region.
					// * for each region, have a workload that speaks SQL which affects data
					//   that is partitioned into a different region.
					const prometheusLocalPortStart = 2110
					const prometheusRemotePortStart = 2120
					ret := []workloadInstance{}
					for i := 0; i < len(regions); i++ {
						// Data partitioned in the same region.
						// e.g. nodes 1-3, partition-affinity=0, prometheus port 2111 means
						// we talk to nodes 1-3, with nodes partitioned in nodes 1-3 (affinity 0).
						ret = append(
							ret,
							workloadInstance{
								nodes:          option.NewNodeListOptionRange((i*nodesPerRegion)+1, ((i + 1) * nodesPerRegion)), // 1-indexed
								prometheusPort: prometheusLocalPortStart + i,
								extraRunArgs:   fmt.Sprintf("--partition-affinity=%d", i), // 0-indexed
							},
						)
						// Data partitioned in a different region.
						// e.g. nodes 1-3, partition-affinity=1, prometheus port 2121 means
						// we talk to nodes 1-3, with nodes partitioned in nodes 3-6 (affinity 1).
						ret = append(
							ret,
							workloadInstance{
								nodes:          option.NewNodeListOptionRange((i*nodesPerRegion)+1, ((i + 1) * nodesPerRegion)), // 1-indexed
								prometheusPort: prometheusRemotePortStart + i,
								extraRunArgs:   fmt.Sprintf("--partition-affinity=%d", (i+1)%len(regions)), // 0-indexed
							},
						)
					}
					return ret
				}(),
			},
		}

		for i := range multiRegionTests {
			tc := multiRegionTests[i]
			r.Add(registry.TestSpec{
				Name:  tc.name,
				Owner: registry.OwnerSQLFoundations,
				// Add an extra node which serves as the workload nodes.
				Cluster:           r.MakeClusterSpec(len(regions)*nodesPerRegion+1, spec.WorkloadNode(), spec.Geo(), spec.GCEZones(strings.Join(zs, ","))),
				CompatibleClouds:  registry.OnlyGCE,
				Suites:            registry.Suites(registry.Nightly),
				EncryptionSupport: registry.EncryptionMetamorphic,
				Leases:            registry.MetamorphicLeases,
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
					t.Status(tc.desc)
					duration := 90 * time.Minute
					partitionArgs := fmt.Sprintf(
						`--survival-goal=%s --regions=%s --partitions=%d`,
						tc.survivalGoal,
						strings.Join(regions, ","),
						len(regions),
					)
					iter := 0
					chaosEventCh := make(chan ChaosEvent)
					runTPCC(ctx, t, t.L(), c, tpccOptions{
						Warehouses:     len(regions) * warehousesPerRegion,
						Duration:       duration,
						ExtraSetupArgs: partitionArgs,
						ExtraRunArgs:   `--method=simple --wait=false --tolerate-errors ` + partitionArgs,
						Chaos: func() Chaos {
							return Chaos{
								Timer: Periodic{
									Period:   240 * time.Second,
									DownTime: 240 * time.Second,
								},
								Target: func() option.NodeListOption {
									ret := tc.chaosTarget(iter)
									iter++
									return ret
								},
								Stopper:      time.After(duration),
								DrainAndQuit: false,
								ChaosEventCh: chaosEventCh,
							}
						},
						ChaosEventsProcessor: func(
							prometheusNode option.NodeListOption,
							workloadInstances []workloadInstance,
						) (tpccChaosEventProcessor, error) {
							prometheusNodeIP, err := c.ExternalIP(ctx, t.L(), prometheusNode)
							if err != nil {
								return tpccChaosEventProcessor{}, err
							}
							client, err := promapi.NewClient(promapi.Config{
								Address: fmt.Sprintf("http://%s:9090", prometheusNodeIP[0]),
							})
							if err != nil {
								return tpccChaosEventProcessor{}, err
							}
							// We see a slow trickle of errors after a server has been force shutdown due
							// to queries before the shutdown not fully completing. You can inspect this
							// by looking at the workload logs and corresponding the errors with the
							// prometheus graphs.
							// The errors seen can be of the form:
							// * ERROR: inbox communication error: rpc error: code = Canceled
							//   desc = context canceled (SQLSTATE 58C01)
							// Setting this allows some errors to occur.
							allowedErrorsMultiplier := 5
							if tc.survivalGoal == "region" {
								// REGION failures last a bit longer after a region has gone down.
								allowedErrorsMultiplier *= 20
							}
							maxErrorsDuringUptime := warehousesPerRegion * tpcc.NumWorkersPerWarehouse * allowedErrorsMultiplier

							return tpccChaosEventProcessor{
								workloadInstances: workloadInstances,
								workloadNodeIP:    prometheusNodeIP[0],
								ops: []string{
									"newOrder",
									"delivery",
									"payment",
									"orderStatus",
									"stockLevel",
								},
								ch:                    chaosEventCh,
								promClient:            promv1.NewAPI(client),
								maxErrorsDuringUptime: maxErrorsDuringUptime,
								// "delivery" does not trigger often.
								allowZeroSuccessDuringUptime: true,
							}, nil
						},
						SetupType:         usingInit,
						WorkloadInstances: tc.workloadInstances,
						// Increase the log verbosity to help debug future failures.
						ExtraStartArgs: []string{
							"--vmodule=store=2,store_rebalancer=2,liveness=2,raft_log_queue=3,replica_range_lease=3,raft=3"},
					})
				},
			})
		}
	}

	r.Add(registry.TestSpec{
		Name:              "tpcc/w=100/nodes=3/chaos=true",
		Owner:             registry.OwnerTestEng,
		Cluster:           r.MakeClusterSpec(4, spec.WorkloadNode()),
		CompatibleClouds:  registry.AllExceptAWS,
		Suites:            registry.Suites(registry.Nightly),
		EncryptionSupport: registry.EncryptionMetamorphic,
		Leases:            registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			duration := 30 * time.Minute
			runTPCC(ctx, t, t.L(), c, tpccOptions{
				Warehouses: 100,
				Duration:   duration,
				// For chaos tests, we don't want to use the default method because it
				// involves preparing statements on all connections (see #51785).
				ExtraRunArgs: "--method=simple --wait=false --tolerate-errors",
				Chaos: func() Chaos {
					return Chaos{
						Timer: Periodic{
							Period:   45 * time.Second,
							DownTime: 10 * time.Second,
						},
						Target:       func() option.NodeListOption { return c.Node(1 + rand.Intn(c.Spec().NodeCount-1)) },
						Stopper:      time.After(duration),
						DrainAndQuit: false,
					}
				},
				SetupType: usingImport,
				// Increase the log verbosity to help debug future failures.
				ExtraStartArgs: []string{
					"--vmodule=store=2,store_rebalancer=2,liveness=2,raft_log_queue=3,replica_range_lease=3,raft=3"}})
		},
	})

	// Run a few representative tpccbench specs in CI.
	registerTPCCBenchSpec(r, tpccBenchSpec{
		Nodes: 3,
		CPUs:  4,

		LoadWarehousesGCE:   1000,
		LoadWarehousesAWS:   1000,
		LoadWarehousesAzure: 1000,
		EstimatedMaxGCE:     750,
		EstimatedMaxAWS:     900,
		EstimatedMaxAzure:   900,

		Clouds: registry.OnlyGCE,
		Suites: registry.Suites(registry.Nightly),
	})
	registerTPCCBenchSpec(r, tpccBenchSpec{
		Nodes: 3,
		CPUs:  4,

		LoadWarehousesGCE:   1000,
		LoadWarehousesAWS:   1000,
		LoadWarehousesAzure: 1000,
		EstimatedMaxGCE:     750,
		EstimatedMaxAWS:     900,
		EstimatedMaxAzure:   900,
		SharedProcessMT:     true,

		Clouds: registry.OnlyGCE,
		Suites: registry.Suites(registry.Nightly),
	})
	registerTPCCBenchSpec(r, tpccBenchSpec{
		Nodes: 3,
		CPUs:  4,

		EnableDefaultScheduledBackup: true,
		LoadWarehousesGCE:            1000,
		LoadWarehousesAWS:            1000,
		LoadWarehousesAzure:          1000,
		EstimatedMaxGCE:              750,
		EstimatedMaxAWS:              900,
		EstimatedMaxAzure:            900,

		Clouds: registry.OnlyGCE,
		Suites: registry.Suites(registry.Nightly),
	})
	registerTPCCBenchSpec(r, tpccBenchSpec{
		Nodes: 3,
		CPUs:  16,

		LoadWarehousesGCE:   3500,
		LoadWarehousesAWS:   3900,
		LoadWarehousesAzure: 3900,
		EstimatedMaxGCE:     3100,
		EstimatedMaxAWS:     3600,
		EstimatedMaxAzure:   3600,
		Clouds:              registry.AllClouds,
		Suites:              registry.Suites(registry.Nightly),
	})
	registerTPCCBenchSpec(r, tpccBenchSpec{
		Nodes: 3,
		CPUs:  16,

		LoadWarehousesGCE:   3500,
		LoadWarehousesAWS:   3900,
		LoadWarehousesAzure: 3900,
		EstimatedMaxGCE:     2900,
		EstimatedMaxAWS:     3400,
		EstimatedMaxAzure:   3400,
		Clouds:              registry.AllClouds,
		Suites:              registry.Suites(registry.Nightly),
		SharedProcessMT:     true,
	})
	registerTPCCBenchSpec(r, tpccBenchSpec{
		Nodes: 12,
		CPUs:  16,

		LoadWarehousesGCE:   11500,
		LoadWarehousesAWS:   11500,
		LoadWarehousesAzure: 11500,
		EstimatedMaxGCE:     10000,
		EstimatedMaxAWS:     10000,
		EstimatedMaxAzure:   10000,

		Clouds: registry.OnlyGCE,
		Suites: registry.Suites(registry.Weekly),
	})
	registerTPCCBenchSpec(r, tpccBenchSpec{
		Nodes:        6,
		CPUs:         16,
		Distribution: multiZone,

		LoadWarehousesGCE:   6500,
		LoadWarehousesAWS:   6500,
		LoadWarehousesAzure: 6500,
		EstimatedMaxGCE:     6300,
		EstimatedMaxAWS:     6300,
		EstimatedMaxAzure:   6300,

		Clouds: registry.OnlyGCE,
		Suites: registry.Suites(registry.Nightly),
	})
	registerTPCCBenchSpec(r, tpccBenchSpec{
		Nodes:        9,
		CPUs:         4,
		HighMem:      true, // can OOM otherwise: https://github.com/cockroachdb/cockroach/issues/73376
		Distribution: multiRegion,
		LoadConfig:   multiLoadgen,

		LoadWarehousesGCE:   3000,
		LoadWarehousesAWS:   3000,
		LoadWarehousesAzure: 3000,
		EstimatedMaxGCE:     2500,
		EstimatedMaxAWS:     2500,
		EstimatedMaxAzure:   2500,

		Clouds: registry.OnlyGCE,
		Suites: registry.Suites(registry.Nightly),
	})
	registerTPCCBenchSpec(r, tpccBenchSpec{
		Nodes:      9,
		CPUs:       4,
		Chaos:      true,
		LoadConfig: singlePartitionedLoadgen,

		LoadWarehousesGCE:   2000,
		LoadWarehousesAWS:   2000,
		LoadWarehousesAzure: 2000,
		EstimatedMaxGCE:     1700,
		EstimatedMaxAWS:     1700,
		EstimatedMaxAzure:   1700,

		Clouds: registry.OnlyGCE,
		Suites: registry.Suites(registry.Nightly),
	})

	// Encryption-At-Rest benchmarks. These are duplicates of variants above,
	// using encrypted stores.
	registerTPCCBenchSpec(r, tpccBenchSpec{
		Nodes: 3,
		CPUs:  4,

		LoadWarehousesGCE:   1000,
		LoadWarehousesAWS:   1000,
		LoadWarehousesAzure: 1000,
		EstimatedMaxGCE:     750,
		EstimatedMaxAWS:     900,
		EstimatedMaxAzure:   900,
		EncryptionEnabled:   true,

		Clouds: registry.OnlyGCE,
		Suites: registry.Suites(registry.Nightly),
	})
	registerTPCCBenchSpec(r, tpccBenchSpec{
		Nodes: 3,
		CPUs:  16,

		LoadWarehousesGCE:   3500,
		LoadWarehousesAWS:   3900,
		LoadWarehousesAzure: 3900,
		EstimatedMaxGCE:     3100,
		EstimatedMaxAWS:     3600,
		EstimatedMaxAzure:   3600,
		EncryptionEnabled:   true,
		Clouds:              registry.AllClouds,
		Suites:              registry.Suites(registry.Nightly),
	})
	registerTPCCBenchSpec(r, tpccBenchSpec{
		Nodes: 12,
		CPUs:  16,

		LoadWarehousesGCE:   11500,
		LoadWarehousesAWS:   11500,
		LoadWarehousesAzure: 11500,
		EstimatedMaxGCE:     10000,
		EstimatedMaxAWS:     10000,
		EstimatedMaxAzure:   10000,
		EncryptionEnabled:   true,

		Clouds: registry.OnlyGCE,
		Suites: registry.Suites(registry.Weekly),
	})

	// Expiration lease benchmarks. These are duplicates of variants above.
	registerTPCCBenchSpec(r, tpccBenchSpec{
		Nodes: 3,
		CPUs:  4,

		LoadWarehousesGCE:   1000,
		LoadWarehousesAWS:   1000,
		LoadWarehousesAzure: 1000,
		EstimatedMaxGCE:     750,
		EstimatedMaxAWS:     900,
		EstimatedMaxAzure:   900,
		ExpirationLeases:    true,

		Clouds: registry.OnlyGCE,
		Suites: registry.Suites(registry.Nightly),
	})
	registerTPCCBenchSpec(r, tpccBenchSpec{
		Nodes: 3,
		CPUs:  16,

		LoadWarehousesGCE:   3500,
		LoadWarehousesAWS:   3900,
		LoadWarehousesAzure: 3900,
		EstimatedMaxGCE:     3100,
		EstimatedMaxAWS:     3600,
		EstimatedMaxAzure:   3600,
		ExpirationLeases:    true,
		Clouds:              registry.AllClouds,
		Suites:              registry.Suites(registry.Nightly),
	})
	registerTPCCBenchSpec(r, tpccBenchSpec{
		Nodes: 12,
		CPUs:  16,

		LoadWarehousesGCE:   11500,
		LoadWarehousesAWS:   11500,
		LoadWarehousesAzure: 11500,
		EstimatedMaxGCE:     10000,
		EstimatedMaxAWS:     10000,
		EstimatedMaxAzure:   10000,
		ExpirationLeases:    true,

		Clouds: registry.OnlyGCE,
		Suites: registry.Suites(registry.Weekly),
	})
}

func valueForCloud(cloud spec.Cloud, gce, aws, azure int) int {
	switch cloud {
	case spec.AWS:
		return aws
	case spec.GCE:
		return gce
	case spec.Azure:
		return azure
	default:
		panic(fmt.Sprintf("unknown cloud %s", cloud))
	}
}

// tpccBenchDistribution represents a distribution of nodes in a tpccbench
// cluster.
type tpccBenchDistribution int

const (
	// All nodes are within the same zone.
	singleZone tpccBenchDistribution = iota
	// Nodes are distributed across 3 zones, all in the same region.
	multiZone
	// Nodes are distributed across 3 regions.
	multiRegion
)

func (d tpccBenchDistribution) zones() []string {
	switch d {
	case singleZone:
		return []string{"us-central1-b"}
	case multiZone:
		// NB: us-central1-a has been causing issues, see:
		// https://github.com/cockroachdb/cockroach/issues/66184
		return []string{"us-central1-f", "us-central1-b", "us-central1-c"}
	case multiRegion:
		return []string{"us-east1-b", "us-west1-b", "europe-west2-b"}
	default:
		panic("unexpected")
	}
}

// tpccBenchLoadConfig represents configurations of load generators in a
// tpccbench spec.
type tpccBenchLoadConfig int

const (
	// A single load generator is run.
	singleLoadgen tpccBenchLoadConfig = iota
	// A single load generator is run with partitioning enabled.
	singlePartitionedLoadgen
	// A load generator is run in each zone.
	multiLoadgen
)

// numLoadNodes returns the number of load generator nodes that the load
// configuration requires for the given node distribution.
func (l tpccBenchLoadConfig) numLoadNodes(d tpccBenchDistribution) int {
	switch l {
	case singleLoadgen:
		return 1
	case singlePartitionedLoadgen:
		return 1
	case multiLoadgen:
		return len(d.zones())
	default:
		panic("unexpected")
	}
}

type tpccBenchSpec struct {
	Owner        registry.Owner // defaults to Test-Eng
	Nodes        int
	CPUs         int
	HighMem      bool
	Chaos        bool
	Distribution tpccBenchDistribution
	LoadConfig   tpccBenchLoadConfig

	// The number of warehouses to load into the cluster before beginning
	// benchmarking. Should be larger than EstimatedMax and should be a
	// value that is unlikely to be achievable.
	LoadWarehousesGCE   int
	LoadWarehousesAWS   int
	LoadWarehousesAzure int
	// An estimate of the maximum number of warehouses achievable in the
	// cluster config. The closer this is to the actual max achievable
	// warehouse count, the faster the benchmark will be in producing a
	// result. This can be adjusted over time as performance characteristics
	// change (i.e. CockroachDB gets faster!).
	EstimatedMaxGCE   int
	EstimatedMaxAWS   int
	EstimatedMaxAzure int

	Clouds registry.CloudSet
	Suites registry.SuiteSet
	// EncryptionEnabled determines if the benchmark uses encrypted stores (i.e.
	// Encryption-At-Rest / EAR).
	EncryptionEnabled bool
	// ExpirationLeases enables use of expiration-based leases.
	ExpirationLeases bool
	// TODO(nvanbenschoten): add a leader lease variant.
	EnableDefaultScheduledBackup bool
	// SharedProcessMT, if true, indicates that the cluster should run in
	// shared-process mode of multi-tenancy.
	SharedProcessMT bool
}

func (s tpccBenchSpec) EstimatedMax(cloud spec.Cloud) int {
	return valueForCloud(cloud, s.EstimatedMaxGCE, s.EstimatedMaxAWS, s.EstimatedMaxAzure)
}

func (s tpccBenchSpec) LoadWarehouses(cloud spec.Cloud) int {
	return valueForCloud(cloud, s.LoadWarehousesGCE, s.LoadWarehousesAWS, s.LoadWarehousesAzure)
}

// partitions returns the number of partitions specified to the load generator.
func (s tpccBenchSpec) partitions() int {
	switch s.LoadConfig {
	case singleLoadgen:
		return 0
	case singlePartitionedLoadgen:
		return s.Nodes / 3
	case multiLoadgen:
		return len(s.Distribution.zones())
	default:
		panic("unexpected")
	}
}

// startOpts returns any extra start options that the spec requires.
func (s tpccBenchSpec) startOpts() (option.StartOpts, install.ClusterSettings) {
	startOpts := option.DefaultStartOpts()
	startOpts.RoachprodOpts.ScheduleBackups = s.EnableDefaultScheduledBackup
	settings := install.MakeClusterSettings()
	// Facilitate diagnosing out-of-memory conditions in tpccbench runs.
	// See https://github.com/cockroachdb/cockroach/issues/75071.
	settings.Env = append(settings.Env, "COCKROACH_MEMPROF_INTERVAL=15s")
	if s.LoadConfig == singlePartitionedLoadgen {
		settings.NumRacks = s.partitions()
	}
	return startOpts, settings
}

func registerTPCCBenchSpec(r registry.Registry, b tpccBenchSpec) {
	owner := registry.OwnerTestEng
	if b.Owner != "" {
		owner = b.Owner
	}
	nameParts := []string{
		"tpccbench",
		fmt.Sprintf("nodes=%d", b.Nodes),
		fmt.Sprintf("cpu=%d", b.CPUs),
	}
	if b.Chaos {
		nameParts = append(nameParts, "chaos")
	}
	if b.EnableDefaultScheduledBackup {
		nameParts = append(nameParts, "backups=true")
	}

	opts := []spec.Option{spec.CPU(b.CPUs)}
	if b.HighMem {
		opts = append(opts, spec.Mem(spec.High))
	}
	switch b.Distribution {
	case singleZone:
		// No specifier.
	case multiZone:
		nameParts = append(nameParts, "multi-az")
		opts = append(opts, spec.Geo(), spec.GCEZones(strings.Join(b.Distribution.zones(), ",")))
	case multiRegion:
		nameParts = append(nameParts, "multi-region")
		opts = append(opts, spec.Geo(), spec.GCEZones(strings.Join(b.Distribution.zones(), ",")))
	default:
		panic("unexpected")
	}

	switch b.LoadConfig {
	case singleLoadgen:
		// No specifier.
	case singlePartitionedLoadgen:
		nameParts = append(nameParts, "partition")
	case multiLoadgen:
		// No specifier.
	default:
		panic("unexpected")
	}

	encryptionSupport := registry.EncryptionAlwaysDisabled
	if b.EncryptionEnabled {
		encryptionSupport = registry.EncryptionAlwaysEnabled
		nameParts = append(nameParts, "enc=true")
	}

	leases := registry.DefaultLeases
	if b.ExpirationLeases {
		leases = registry.ExpirationLeases
		nameParts = append(nameParts, "lease=expiration")
	}

	if b.SharedProcessMT {
		nameParts = append(nameParts, "mt-shared-process")
	}

	name := strings.Join(nameParts, "/")

	numNodes := b.Nodes + b.LoadConfig.numLoadNodes(b.Distribution)
	nodes := r.MakeClusterSpec(numNodes, opts...)

	r.Add(registry.TestSpec{
		Name:              name,
		Owner:             owner,
		Benchmark:         true,
		Cluster:           nodes,
		Timeout:           7 * time.Hour,
		CompatibleClouds:  b.Clouds,
		Suites:            b.Suites,
		EncryptionSupport: encryptionSupport,
		Leases:            leases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runTPCCBench(ctx, t, c, b)
		},
	})
}

// loadTPCCBench loads a TPCC dataset for the specific benchmark spec. The
// function is idempotent and first checks whether a compatible dataset exists,
// performing an expensive dataset restore only if it doesn't.
func loadTPCCBench(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	db *gosql.DB,
	b tpccBenchSpec,
	roachNodes, loadNode option.NodeListOption,
) error {
	// Check if the dataset already exists and is already large enough to
	// accommodate this benchmarking. If so, we can skip the fixture RESTORE.
	if _, err := db.ExecContext(ctx, `USE tpcc`); err == nil {
		t.L().Printf("found existing tpcc database\n")

		var curWarehouses int
		if err := db.QueryRowContext(ctx,
			`SELECT count(*) FROM tpcc.warehouse`,
		).Scan(&curWarehouses); err != nil {
			return err
		}
		if curWarehouses >= b.LoadWarehouses(c.Cloud()) {
			// The cluster has enough warehouses. Nothing to do.
			return nil
		}

		// If the dataset exists but is not large enough, wipe the cluster
		// before restoring.
		c.Wipe(ctx, roachNodes)
		startOpts, settings := b.startOpts()
		c.Start(ctx, t.L(), startOpts, settings, roachNodes)
	} else if pqErr := (*pq.Error)(nil); !(errors.As(err, &pqErr) &&
		pgcode.MakeCode(string(pqErr.Code)) == pgcode.InvalidCatalogName) {
		return err
	}

	var loadArgs string
	var rebalanceWait time.Duration
	loadWarehouses := b.LoadWarehouses(c.Cloud())
	switch b.LoadConfig {
	case singleLoadgen:
		loadArgs = `--checks=false`
		rebalanceWait = time.Duration(loadWarehouses/250) * time.Minute
	case singlePartitionedLoadgen:
		loadArgs = fmt.Sprintf(`--checks=false --partitions=%d`, b.partitions())
		rebalanceWait = time.Duration(loadWarehouses/125) * time.Minute
	case multiLoadgen:
		loadArgs = fmt.Sprintf(`--checks=false --partitions=%d --zones="%s"`,
			b.partitions(), strings.Join(b.Distribution.zones(), ","))
		rebalanceWait = time.Duration(loadWarehouses/50) * time.Minute
	default:
		panic("unexpected")
	}

	// Load the corresponding fixture.
	t.L().Printf("restoring tpcc fixture\n")
	err := roachtestutil.WaitFor3XReplication(ctx, t.L(), db)
	require.NoError(t, err)
	var pgurl string
	if b.SharedProcessMT {
		pgurl = fmt.Sprintf("{pgurl%s:%s}", roachNodes[:1], appTenantName)
	} else {
		pgurl = "{pgurl:1}"
	}
	cmd := tpccImportCmd("", loadWarehouses, loadArgs, pgurl)
	if err = c.RunE(ctx, option.WithNodes(roachNodes[:1]), cmd); err != nil {
		return err
	}
	if rebalanceWait == 0 || len(roachNodes) <= 3 {
		return nil
	}

	t.L().Printf("waiting %v for rebalancing\n", rebalanceWait)
	_, err = db.ExecContext(ctx, `SET CLUSTER SETTING kv.snapshot_rebalance.max_rate='128MiB'`)
	if err != nil {
		return err
	}

	// Split and scatter the tables. Ramp up to the half of the expected load in
	// the desired distribution. This should allow for load-based rebalancing to
	// help distribute load. Optionally pass some load configuration-specific
	// flags.
	maxRate := tpccMaxRate(b.EstimatedMax(c.Cloud()))
	rampTime := (1 * rebalanceWait) / 4
	loadTime := (3 * rebalanceWait) / 4
	var tenantSuffix string
	if b.SharedProcessMT {
		tenantSuffix = fmt.Sprintf(":%s", appTenantName)
	}
	cmd = fmt.Sprintf("./cockroach workload run tpcc --warehouses=%d --workers=%d --max-rate=%d "+
		"--wait=false --ramp=%s --duration=%s --scatter --tolerate-errors {pgurl%s%s}",
		b.LoadWarehouses(c.Cloud()), b.LoadWarehouses(c.Cloud()), maxRate, rampTime, loadTime, roachNodes, tenantSuffix)
	if _, err := c.RunWithDetailsSingleNode(ctx, t.L(), option.WithNodes(loadNode), cmd); err != nil {
		return err
	}

	_, err = db.ExecContext(ctx, `SET CLUSTER SETTING kv.snapshot_rebalance.max_rate='2MiB'`)
	return err
}

// tpccbench is a suite of benchmarking tools that run TPC-C against CockroachDB
// clusters in different configurations. The tools search for the maximum number
// of warehouses that a load generator can run TPC-C against while still
// maintaining a minimum acceptable throughput. This maximum warehouse value is
// directly comparable to other runs of the tool in the same cluster config, and
// expresses how well CockroachDB performance scales.
//
// In order to run a benchmark spec, the tool must first load a TPC-C dataset
// large enough to accommodate it. This can take a while, so it is recommended
// to use a combination of `--cluster=<cluster>` and `--wipe=false` flags to
// limit the loading phase to the first run of the tool. Subsequent runs will be
// able to avoid the dataset restore as long as they are not wiped. This allows
// for quick iteration on experimental changes.
//
// It can also be useful to omit the `--cluster` flag during the first run of
// the tool to allow roachtest to create the correct set of VMs required by the
// test. The `--wipe` flag will prevent this cluster from being destroyed, so it
// can then be used during future runs.
func runTPCCBench(ctx context.Context, t test.Test, c cluster.Cluster, b tpccBenchSpec) {
	// Determine the nodes in each load group. A load group consists of a set of
	// Cockroach nodes and a single load generator.
	numLoadGroups := b.LoadConfig.numLoadNodes(b.Distribution)
	numZones := len(b.Distribution.zones())
	loadGroups := roachtestutil.MakeLoadGroups(c, numZones, b.Nodes, numLoadGroups)
	roachNodes := loadGroups.RoachNodes()
	loadNodes := loadGroups.LoadNodes()
	// Don't encrypt in tpccbench tests.
	startOpts, settings := b.startOpts()
	c.Start(ctx, t.L(), startOpts, settings, roachNodes)

	var db *gosql.DB
	if b.SharedProcessMT {
		startOpts = option.StartSharedVirtualClusterOpts(appTenantName)
		c.StartServiceForVirtualCluster(ctx, t.L(), startOpts, settings)
		db = c.Conn(ctx, t.L(), 1, option.VirtualClusterName(appTenantName))
	} else {
		db = c.Conn(ctx, t.L(), 1)
	}

	useHAProxy := b.Chaos
	const restartWait = 15 * time.Second
	{
		// Wait after restarting nodes before applying load. This lets
		// things settle down to avoid unexpected cluster states.
		time.Sleep(restartWait)
		if useHAProxy {
			if len(loadNodes) > 1 {
				t.Fatal("distributed chaos benchmarking not supported")
			}
			t.Status("installing haproxy")
			if err := c.Install(ctx, t.L(), loadNodes, "haproxy"); err != nil {
				t.Fatal(err)
			}
			c.Run(ctx, option.WithNodes(loadNodes), "./cockroach gen haproxy --url {pgurl:1}")
			// Increase the maximum connection limit to ensure that no TPC-C
			// load gen workers get stuck during connection initialization.
			// 10k warehouses requires at least 20,000 connections, so add a
			// bit of breathing room and check the warehouse count.
			c.Run(ctx, option.WithNodes(loadNodes), "sed -i 's/maxconn [0-9]\\+/maxconn 21000/' haproxy.cfg")
			if b.LoadWarehouses(c.Cloud()) > 1e4 {
				t.Fatal("HAProxy config supports up to 10k warehouses")
			}
			c.Run(ctx, option.WithNodes(loadNodes), "haproxy -f haproxy.cfg -D")
		}

		m := c.NewMonitor(ctx, roachNodes)
		m.Go(func(ctx context.Context) error {
			t.Status("setting up dataset")
			return loadTPCCBench(ctx, t, c, db, b, roachNodes, c.Node(loadNodes[0]))
		})
		m.Wait()
	}

	// Search between 1 and b.LoadWarehouses for the largest number of
	// warehouses that can be operated on while sustaining a throughput
	// threshold, set to a fraction of max tpmC.
	precision := int(math.Max(1.0, float64(b.LoadWarehouses(c.Cloud())/200)))
	initStepSize := precision

	// Create a temp directory to store the local copy of results from the
	// workloads.
	resultsDir, err := os.MkdirTemp("", "roachtest-tpcc")
	if err != nil {
		t.Fatal(errors.Wrap(err, "failed to create temp dir"))
	}
	defer func() { _ = os.RemoveAll(resultsDir) }()

	restart := func(ctx context.Context) {
		c.Stop(ctx, t.L(), option.DefaultStopOpts(), roachNodes)
		startOpts, settings := b.startOpts()
		c.Start(ctx, t.L(), startOpts, settings, roachNodes)
	}

	s := search.NewLineSearcher(1, b.LoadWarehouses(c.Cloud()), b.EstimatedMax(c.Cloud()), initStepSize, precision)
	iteration := 0
	if res, err := s.Search(func(warehouses int) (bool, error) {
		iteration++
		t.L().Printf("initializing cluster for %d warehouses (search attempt: %d)", warehouses, iteration)

		restart(ctx)

		time.Sleep(restartWait)

		// Set up the load generation configuration.
		rampDur := 5 * time.Minute
		loadDur := 10 * time.Minute
		loadDone := make(chan time.Time, numLoadGroups)

		// NB: for goroutines in this monitor, handle errors via `t.Fatal` to
		// *abort* the line search and whole tpccbench run. Return the errors
		// to indicate that the specific warehouse count failed, but that the
		// line search ought to continue.
		m := c.NewMonitor(ctx, roachNodes)

		// If we're running chaos in this configuration, modify this config.
		if b.Chaos {
			// Kill one node at a time.
			ch := Chaos{
				Timer:   Periodic{Period: 90 * time.Second, DownTime: 5 * time.Second},
				Target:  roachNodes.RandNode,
				Stopper: loadDone,
			}
			m.Go(ch.Runner(c, t, m))
		}
		if b.Distribution == multiRegion {
			rampDur = 3 * time.Minute
			loadDur = 15 * time.Minute
		}

		// If we're running multiple load generators, run them in parallel and then
		// aggregate resultChan. In order to process the results we need to copy
		// over the histograms. Create a temp dir which will contain the fetched
		// data.
		resultChan := make(chan *tpcc.Result, numLoadGroups)
		for groupIdx, group := range loadGroups {
			// Copy for goroutine
			groupIdx := groupIdx
			group := group
			m.Go(func(ctx context.Context) error {
				sqlGateways := group.RoachNodes
				if useHAProxy {
					sqlGateways = group.LoadNodes
				}

				extraFlags := ""
				switch b.LoadConfig {
				case singleLoadgen:
					// Nothing.
				case singlePartitionedLoadgen:
					extraFlags = fmt.Sprintf(` --partitions=%d`, b.partitions())
				case multiLoadgen:
					extraFlags = fmt.Sprintf(` --partitions=%d --partition-affinity=%d`,
						b.partitions(), groupIdx)
				default:
					// Abort the whole test.
					t.Fatalf("unimplemented LoadConfig %v", b.LoadConfig)
				}
				if b.Chaos {
					// For chaos tests, we don't want to use the default method because it
					// involves preparing statements on all connections (see #51785).
					extraFlags += " --method=simple"
				}
				t.Status(fmt.Sprintf("running benchmark, warehouses=%d", warehouses))
				histogramsPath := fmt.Sprintf("%s/warehouses=%d/stats.json", t.PerfArtifactsDir(), warehouses)
				var tenantSuffix string
				if b.SharedProcessMT {
					tenantSuffix = fmt.Sprintf(":%s", appTenantName)
				}
				cmd := fmt.Sprintf("./cockroach workload run tpcc --warehouses=%d --active-warehouses=%d "+
					"--tolerate-errors --ramp=%s --duration=%s%s --histograms=%s {pgurl%s%s}",
					b.LoadWarehouses(c.Cloud()), warehouses, rampDur,
					loadDur, extraFlags, histogramsPath, sqlGateways, tenantSuffix)
				err := c.RunE(ctx, option.WithNodes(group.LoadNodes), cmd)
				loadDone <- timeutil.Now()
				if err != nil {
					// NB: this will let the line search continue at a lower warehouse
					// count.
					return errors.Wrapf(err, "error running tpcc load generator")
				}

				roachtestHistogramsPath := filepath.Join(resultsDir, fmt.Sprintf("%d.%d-stats.json", warehouses, groupIdx))
				if err := c.Get(
					ctx, t.L(), histogramsPath, roachtestHistogramsPath, group.LoadNodes,
				); err != nil {
					// NB: this will let the line search continue. The reason we do this
					// is because it's conceivable that we made it here, but a VM just
					// froze up on us. The next search iteration will handle this state.
					return err
				}
				snapshots, err := histogram.DecodeSnapshots(roachtestHistogramsPath)
				if err != nil {
					// If we got this far, and can't decode data, it's not a case of
					// overload but something that deserves failing the whole test.
					t.Fatal(err)
				}
				result := tpcc.NewResultWithSnapshots(warehouses, 0, snapshots)

				// This roachtest uses the stats.json emitted from hdr histogram to compute Tpmc and show it in the run log
				// Since directly emitting openmetrics and computing Tpmc from it is not supported, it is better to convert the
				// stats.json emitted to openmetrics in the test itself and upload it to the cluster
				if t.ExportOpenmetrics() {
					// Creating a prefix
					statsFilePrefix := fmt.Sprintf("warehouses=%d/", warehouses)

					// Create buffer for performance metrics
					perfBuf := bytes.NewBuffer([]byte{})
					exporter := roachtestutil.CreateWorkloadHistogramExporterWithLabels(t, c, map[string]string{"warehouses": fmt.Sprintf("%d", warehouses)})
					writer := io.Writer(perfBuf)
					exporter.Init(&writer)
					defer roachtestutil.CloseExporter(ctx, exporter, t, c, perfBuf, group.LoadNodes, statsFilePrefix)

					if err := exportOpenMetrics(exporter, snapshots); err != nil {
						return errors.Wrapf(err, "error converting histogram to openmetrics")
					}
				}
				resultChan <- result
				return nil
			})
		}
		failErr := m.WaitE()
		close(resultChan)

		var res *tpcc.Result
		if failErr != nil {
			if t.Failed() {
				// Someone called `t.Fatal` in a monitored goroutine,
				// meaning that something went sideways in a way that
				// indicates a general problem (i.e. not just that the
				// current warehouse count overloaded the cluster.
				// Abort the whole test.
				return false, err
			}
			// A goroutine returned an error, but this means only
			// that the given warehouse count did not run to completion,
			// presumably because it overloaded the cluster. We thus
			// "achieved" zero TpmC, but will continue the search.
			//
			// Note that it's also possible that we get here due to an
			// actual bug in CRDB (for example a node crashing due to
			// getting into an invalid state); we cannot distinguish
			// those here and so tpccbench isn't a good test to rely
			// on to catch crash-causing bugs.
			res = &tpcc.Result{
				ActiveWarehouses: warehouses,
			}
		} else {
			// We managed to run TPCC, which means that we may or may
			// not have "passed" TPCC.
			var results []*tpcc.Result
			for partial := range resultChan {
				results = append(results, partial)
			}
			res = tpcc.MergeResults(results...)
			failErr = res.FailureError()
		}

		// Print the result.
		if failErr == nil {
			ttycolor.Stdout(ttycolor.Green)
			t.L().Printf("--- SEARCH ITER PASS: TPCC %d resulted in %.1f tpmC (%.1f%% of max tpmC)\n\n",
				warehouses, res.TpmC(), res.Efficiency())
		} else {
			ttycolor.Stdout(ttycolor.Red)
			t.L().Printf("--- SEARCH ITER FAIL: TPCC %d resulted in %.1f tpmC and failed due to %v",
				warehouses, res.TpmC(), failErr)
		}
		ttycolor.Stdout(ttycolor.Reset)
		return failErr == nil, nil
	}); err != nil {
		t.Fatal(err)
	} else {
		// The last iteration may have been a failing run that overloaded
		// nodes to the point of them crashing. Make roachtest happy by
		// restarting the cluster so that it can run consistency checks.
		restart(ctx)
		ttycolor.Stdout(ttycolor.Green)
		t.L().Printf("------\nMAX WAREHOUSES = %d\n------\n\n", res)
		ttycolor.Stdout(ttycolor.Reset)
	}
}

// makeWorkloadScrapeNodes creates a ScrapeNode for every workloadInstance.
func makeWorkloadScrapeNodes(
	workloadNode install.Node, workloadInstances []workloadInstance,
) []prometheus.ScrapeNode {
	workloadScrapeNodes := make([]prometheus.ScrapeNode, len(workloadInstances))
	for i, workloadInstance := range workloadInstances {
		workloadScrapeNodes[i] = prometheus.ScrapeNode{
			Node: workloadNode,
			Port: workloadInstance.prometheusPort,
		}
	}
	return workloadScrapeNodes
}

// setupPrometheusForRoachtest initializes prometheus to run against the provided
// PrometheusConfig. If no PrometheusConfig is provided, it creates a prometheus
// scraper for all CockroachDB nodes in the TPC-C setup, as well as one for
// each workloadInstance.
// Returns the created PrometheusConfig if prometheus is initialized, as well
// as a cleanup function which should be called in a defer statement.
func setupPrometheusForRoachtest(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	promCfg *prometheus.Config,
	workloadInstances []workloadInstance,
) (*prometheus.Config, func()) {
	cfg := promCfg
	if cfg == nil {
		// Avoid setting prometheus automatically up for local clusters.
		if c.IsLocal() {
			return nil, func() {}
		}
		cfg = &prometheus.Config{}
		workloadNode := c.WorkloadNode().InstallNodes()[0]
		cfg.WithPrometheusNode(workloadNode)
		cfg.WithNodeExporter(c.CRDBNodes().InstallNodes())
		cfg.WithCluster(c.CRDBNodes().InstallNodes())
		if len(workloadInstances) > 0 {
			cfg.ScrapeConfigs = append(cfg.ScrapeConfigs, prometheus.MakeWorkloadScrapeConfig("workload",
				"/", makeWorkloadScrapeNodes(workloadNode, workloadInstances)))
		}
	}
	if c.IsLocal() {
		t.Status("ignoring prometheus setup given --local was specified")
		return nil, func() {}
	}

	t.Status(fmt.Sprintf("setting up prometheus/grafana (<%s)", 2*time.Minute))

	quietLogger, err := t.L().ChildLogger("start-grafana", logger.QuietStdout, logger.QuietStderr)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.StartGrafana(ctx, quietLogger, cfg); err != nil {
		t.Fatal(err)
	}
	cleanupFunc := func() {
		if t.IsDebug() {
			return // nothing to do
		}
		if err := c.StopGrafana(ctx, quietLogger, t.ArtifactsDir()); err != nil {
			t.L().ErrorfCtx(ctx, "error(s) shutting down prom/grafana: %s", err)
		}
	}
	return cfg, cleanupFunc
}

func getTpccLabels(
	warehouses int, rampDur time.Duration, duration time.Duration, extraLabels map[string]string,
) map[string]string {
	labels := map[string]string{
		"warehouses": fmt.Sprintf("%d", warehouses),
		"duration":   duration.String(),
		"ramp":       rampDur.String(),
	}

	if extraLabels != nil {
		maps.Copy(labels, extraLabels)
	}

	return labels
}

// This function converts exporter.SnapshotTick to openmetrics into a buffer
func exportOpenMetrics(
	exporter exporter.Exporter, snapshots map[string][]exporter.SnapshotTick,
) error {
	for _, snaps := range snapshots {
		for _, s := range snaps {
			h := hdrhistogram.Import(s.Hist)
			if err := exporter.SnapshotAndWrite(h, s.Now, s.Elapsed, &s.Name); err != nil {
				return errors.Wrapf(err, "failed to write snapshot for histogram %q", s.Name)
			}
		}
	}

	return nil
}
