// Copyright 2018 The Cockroach Authors.
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
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/testutils/release"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/search"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/cockroach/pkg/workload/tpcc"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/ttycolor"
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
	Warehouses         int
	ExtraRunArgs       string
	ExtraSetupArgs     string
	Chaos              func() Chaos                // for late binding of stopper
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
	//
	// TODO(tbg): for better coverage at scale of the migration process, we should
	// also be doing a rolling-restart into the new binary while the cluster
	// is running, but that feels like jamming too much into the tpcc setup.
	Start func(context.Context, test.Test, cluster.Cluster)
	// SkipPostRunCheck, if set, skips post TPC-C run checks.
	SkipPostRunCheck              bool
	DisableDefaultScheduledBackup bool
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

// tpccImportCmd generates the command string to load tpcc data for the
// specified warehouse count into a cluster.
//
// The command uses `cockroach workload` instead of `workload` so the tpcc
// workload-versions match on release branches. Similarly, the command does not
// specify pgurl to ensure that it is run on a node with a running cockroach
// instance to ensure that the workload version matches the gateway version in a
// mixed version cluster.
func tpccImportCmd(warehouses int, extraArgs ...string) string {
	return tpccImportCmdWithCockroachBinary("./cockroach", warehouses, extraArgs...)
}

func tpccImportCmdWithCockroachBinary(
	crdbBinary string, warehouses int, extraArgs ...string,
) string {
	return fmt.Sprintf("./%s workload fixtures import tpcc --warehouses=%d %s",
		crdbBinary, warehouses, strings.Join(extraArgs, " "))
}

func setupTPCC(
	ctx context.Context, t test.Test, c cluster.Cluster, opts tpccOptions,
) (crdbNodes, workloadNode option.NodeListOption) {
	// Randomize starting with encryption-at-rest enabled.
	crdbNodes = c.Range(1, c.Spec().NodeCount-1)
	workloadNode = c.Node(c.Spec().NodeCount)

	if c.IsLocal() {
		opts.Warehouses = 1
	}

	if opts.Start == nil {
		opts.Start = func(ctx context.Context, t test.Test, c cluster.Cluster) {
			// NB: workloadNode also needs ./cockroach because
			// of `./cockroach workload` for usingImport.
			c.Put(ctx, t.Cockroach(), "./cockroach", c.All())
			settings := install.MakeClusterSettings()
			if c.IsLocal() {
				settings.Env = append(settings.Env, "COCKROACH_SCAN_INTERVAL=200ms")
				settings.Env = append(settings.Env, "COCKROACH_SCAN_MAX_IDLE_TIME=5ms")
			}
			startOpts := option.DefaultStartOpts()
			startOpts.RoachprodOpts.ScheduleBackups = !opts.DisableDefaultScheduledBackup
			c.Start(ctx, t.L(), startOpts, settings, crdbNodes)
		}
	}

	func() {
		opts.Start(ctx, t, c)
		db := c.Conn(ctx, t.L(), 1)
		defer db.Close()

		if t.SkipInit() {
			return
		}

		require.NoError(t, WaitFor3XReplication(ctx, t, c.Conn(ctx, t.L(), crdbNodes[0])))

		estimatedSetupTimeStr := ""
		if opts.EstimatedSetupTime != 0 {
			estimatedSetupTimeStr = fmt.Sprintf(" (<%s)", opts.EstimatedSetupTime)
		}

		switch opts.SetupType {
		case usingExistingData:
			// Do nothing.
		case usingImport:
			t.Status("loading fixture" + estimatedSetupTimeStr)
			c.Run(ctx, crdbNodes[:1], tpccImportCmd(opts.Warehouses, opts.ExtraSetupArgs))
		case usingInit:
			t.Status("initializing tables" + estimatedSetupTimeStr)
			extraArgs := opts.ExtraSetupArgs
			if !t.BuildVersion().AtLeast(version.MustParse("v20.2.0")) {
				extraArgs += " --deprecated-fk-indexes"
			}
			cmd := fmt.Sprintf(
				"./cockroach workload init tpcc --warehouses=%d %s {pgurl:1}",
				opts.Warehouses, extraArgs,
			)
			c.Run(ctx, workloadNode, cmd)
		default:
			t.Fatal("unknown tpcc setup type")
		}
		t.Status("finished tpc-c setup")
	}()
	return crdbNodes, workloadNode
}

func runTPCC(ctx context.Context, t test.Test, c cluster.Cluster, opts tpccOptions) {
	workloadInstances := opts.WorkloadInstances
	if len(workloadInstances) == 0 {
		workloadInstances = append(
			workloadInstances,
			workloadInstance{
				nodes:          c.Range(1, c.Spec().NodeCount-1),
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
			c.Nodes(int(promCfg.PrometheusNode[0])),
			workloadInstances,
		)
		if err != nil {
			t.Fatal(err)
		}
		cep.listen(ctx, t.L())
		ep = &cep
	}

	if c.IsLocal() {
		opts.Warehouses = 1
		if opts.Duration > time.Minute {
			opts.Duration = time.Minute
		}
	}
	crdbNodes, workloadNode := setupTPCC(ctx, t, c, opts)
	m := c.NewMonitor(ctx, crdbNodes)
	rampDur := rampDuration(c.IsLocal())
	for i := range workloadInstances {
		// Make a copy of i for the goroutine.
		i := i
		m.Go(func(ctx context.Context) error {
			// Only prefix stats.json with workload_i_ if we have multiple workloads,
			// in case other processes relied on previous behavior.
			var statsPrefix string
			if len(workloadInstances) > 1 {
				statsPrefix = fmt.Sprintf("workload_%d.", i)
			}
			t.WorkerStatus(fmt.Sprintf("running tpcc worker=%d warehouses=%d ramp=%s duration=%s on %s (<%s)",
				i, opts.Warehouses, rampDur, opts.Duration, pgURLs[i], time.Minute))
			cmd := fmt.Sprintf(
				"./cockroach workload run tpcc --warehouses=%d --histograms="+t.PerfArtifactsDir()+"/%sstats.json "+
					opts.ExtraRunArgs+" --ramp=%s --duration=%s --prometheus-port=%d --pprofport=%d %s %s",
				opts.Warehouses,
				statsPrefix,
				rampDur,
				opts.Duration,
				workloadInstances[i].prometheusPort,
				workloadPProfStartPort+i,
				workloadInstances[i].extraRunArgs,
				pgURLs[i],
			)
			return c.RunE(ctx, workloadNode, cmd)
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
		c.Run(ctx, workloadNode, fmt.Sprintf(
			"./cockroach workload check tpcc --warehouses=%d {pgurl:1}", opts.Warehouses))
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
	// We append "-0" to the version so that we capture all prereleases of the
	// specified version. Otherwise, "v2.1.0" would compare greater than
	// "v2.1.0-alpha.x".
	{hardware: "gce-n4cpu16", v: version.MustParse(`v2.1.0-0`), warehouses: 1300},
	{hardware: "gce-n4cpu16", v: version.MustParse(`v19.1.0-0`), warehouses: 1250},
	{hardware: "aws-n4cpu16", v: version.MustParse(`v19.1.0-0`), warehouses: 2100},

	// TODO(tbg): this number is copied from gce-n4cpu16. The real number should be a
	// little higher, find out what it is.
	{hardware: "gce-n5cpu16", v: version.MustParse(`v19.1.0-0`), warehouses: 1300},
	{hardware: "aws-n5cpu16", v: version.MustParse(`v19.1.0-0`), warehouses: 2100},
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
	buildVersion version.Version, cloud string, nodes spec.ClusterSpec,
) int {
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

type backgroundFn func(ctx context.Context, u *versionUpgradeTest) error

// A backgroundStepper is a tool to run long-lived commands while a cluster is
// going through a sequence of version upgrade operations.
// It exposes a `launch` step that launches the method carrying out long-running
// work (in the background) and a `stop` step collecting any errors.
type backgroundStepper struct {
	// This is the operation that will be launched in the background. When the
	// context gets canceled, it should shut down and return without an error.
	// The way to typically get this is:
	//
	//  err := doSomething(ctx)
	//  ctx.Err() != nil {
	//    return nil
	//  }
	//  return err
	run backgroundFn
	// When not nil, called with the error within `.stop()`. The interceptor
	// gets a chance to ignore the error or produce a different one (via t.Fatal).
	onStop func(context.Context, test.Test, *versionUpgradeTest, error)
	nodes  option.NodeListOption // nodes to monitor, defaults to c.All()

	// Internal.
	m cluster.Monitor
}

// launch spawns the function the background step was initialized with.
func (s *backgroundStepper) launch(ctx context.Context, t test.Test, u *versionUpgradeTest) {
	nodes := s.nodes
	if nodes == nil {
		nodes = u.c.All()
	}
	s.m = u.c.NewMonitor(ctx, nodes)
	s.m.Go(func(ctx context.Context) error {
		return s.run(ctx, u)
	})
}

func (s *backgroundStepper) wait(ctx context.Context, t test.Test, u *versionUpgradeTest) {
	// We don't care about the workload failing since we only use it to produce a
	// few `RESTORE` jobs. And indeed workload will fail because it does not
	// tolerate pausing of its jobs.
	err := s.m.WaitE()
	if s.onStop != nil {
		s.onStop(ctx, t, u, err)
	} else if err != nil {
		t.Fatal(err)
	}
}

// runTPCCMixedHeadroom runs a mixed-version test that imports a large
// `bank` dataset, and runs one or multiple database upgrades while a
// TPCC workload is running. The number of database upgrades is
// controlled by the `versionsToUpgrade` parameter.
func runTPCCMixedHeadroom(
	ctx context.Context, t test.Test, c cluster.Cluster, cloud string, versionsToUpgrade int,
) {
	crdbNodes := c.Range(1, c.Spec().NodeCount-1)
	workloadNode := c.Node(c.Spec().NodeCount)

	maxWarehouses := maxSupportedTPCCWarehouses(*t.BuildVersion(), cloud, c.Spec())
	headroomWarehouses := int(float64(maxWarehouses) * 0.7)
	if c.IsLocal() {
		headroomWarehouses = 10
	}

	// We'll need this below.
	tpccBackgroundStepper := func(duration time.Duration) backgroundStepper {
		return backgroundStepper{
			nodes: crdbNodes,
			run: func(ctx context.Context, u *versionUpgradeTest) error {
				t.L().Printf("running background TPCC workload for %s", duration)
				runTPCC(ctx, t, c, tpccOptions{
					Warehouses: headroomWarehouses,
					Duration:   duration,
					SetupType:  usingExistingData,
					Start: func(ctx context.Context, t test.Test, c cluster.Cluster) {
						// Noop - we don't let tpcc upload or start binaries in this test.
					},
				})
				return nil
			}}
	}

	randomCRDBNode := func() int { return crdbNodes.RandNode()[0] }
	const mainBinary = ""

	// NB: this results in ~100GB of (actual) disk usage per node once things
	// have settled down, and ~7.5k ranges. The import takes ~40 minutes.
	// The full 6.5m import ran into out of disk errors (on 250gb machines),
	// hence division by two.
	bankRows := 65104166 / 2
	if c.IsLocal() {
		bankRows = 1000
	}

	rng, seed := randutil.NewLockedPseudoRand()
	t.L().Printf("using random seed %d", seed)
	history, err := release.RandomPredecessorHistory(rng, t.BuildVersion(), versionsToUpgrade)
	if err != nil {
		t.Fatal(err)
	}
	sep := " -> "
	t.L().Printf("testing upgrade: %s%scurrent", strings.Join(history, sep), sep)
	history = append(history, mainBinary)

	waitForWorkloadToRampUp := sleepStep(rampDuration(c.IsLocal()))
	logStep := func(format string, args ...interface{}) versionStep {
		return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
			t.L().Printf(format, args...)
		}
	}

	oldestVersion := history[0]
	setupSteps := []versionStep{
		logStep("starting from fixture at version %s", oldestVersion),
		uploadAndStartFromCheckpointFixture(crdbNodes, oldestVersion),
		waitForUpgradeStep(crdbNodes),               // let oldest version settle (gossip etc)
		uploadVersionStep(workloadNode, mainBinary), // for tpccBackgroundStepper's workload

		// Load TPCC dataset, don't run TPCC yet. We do this while in the
		// version we are starting with to load some data and hopefully
		// create some state that will need work by long-running
		// migrations.
		importTPCCStep(oldestVersion, headroomWarehouses, crdbNodes),
		// Add a lot of cold data to this cluster. This further stresses the version
		// upgrade machinery, in which a) all ranges are touched and b) work proportional
		// to the amount data may be carried out.
		importLargeBankStep(oldestVersion, bankRows, crdbNodes),
	}

	// upgradeToVersionSteps returns the list of steps to be performed
	// when upgrading to the given version.
	upgradeToVersionSteps := func(crdbVersion string) []versionStep {
		duration := 10 * time.Minute
		versionString := crdbVersion
		if crdbVersion == mainBinary {
			duration = 100 * time.Minute
			versionString = "current"
		}
		tpccWorkload := tpccBackgroundStepper(duration)

		return []versionStep{
			logStep("upgrading to version %q", versionString),
			preventAutoUpgradeStep(randomCRDBNode()),
			// Upload and restart cluster into the new
			// binary (stays at previous cluster version).
			binaryUpgradeStep(crdbNodes, crdbVersion),
			// Now start running TPCC in the background.
			tpccWorkload.launch,
			// Wait for the workload to ramp up before attemping to
			// upgrade the cluster version. If we start the migrations
			// immediately after launching the tpcc workload above, they
			// could finish "too quickly", before the workload had a
			// chance to pick up the pace (starting all the workers, range
			// merge/splits, compactions, etc). By waiting here, we
			// increase the concurrency exposed to the upgrade migrations,
			// and increase the chances of exposing bugs (such as #83079).
			waitForWorkloadToRampUp,
			// While tpcc is running in the background, bump the cluster
			// version manually. We do this over allowing automatic upgrades
			// to get a better idea of what errors come back here, if any.
			// This will block until the long-running migrations have run.
			allowAutoUpgradeStep(randomCRDBNode()),
			waitForUpgradeStep(crdbNodes),
			// Wait until TPCC background run terminates
			// and fail if it reports an error.
			tpccWorkload.wait,
		}
	}

	// Test steps consist of the setup steps + the upgrade steps for
	// each upgrade being carried out here.
	testSteps := append([]versionStep{}, setupSteps...)
	for _, nextVersion := range history[1:] {
		testSteps = append(testSteps, upgradeToVersionSteps(nextVersion)...)
	}

	newVersionUpgradeTest(c, testSteps...).run(ctx, t)
}

func registerTPCC(r registry.Registry) {
	cloud := r.MakeClusterSpec(1).Cloud
	headroomSpec := r.MakeClusterSpec(4, spec.CPU(16), spec.RandomlyUseZfs())
	r.Add(registry.TestSpec{
		// w=headroom runs tpcc for a semi-extended period with some amount of
		// headroom, more closely mirroring a real production deployment than
		// running with the max supported warehouses.
		Name:              "tpcc/headroom/" + headroomSpec.String(),
		Owner:             registry.OwnerTestEng,
		Tags:              registry.Tags(`default`, `release_qualification`, `aws`),
		Cluster:           headroomSpec,
		EncryptionSupport: registry.EncryptionMetamorphic,
		Leases:            registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			maxWarehouses := maxSupportedTPCCWarehouses(*t.BuildVersion(), cloud, t.Spec().(*registry.TestSpec).Cluster)
			headroomWarehouses := int(float64(maxWarehouses) * 0.7)
			t.L().Printf("computed headroom warehouses of %d\n", headroomWarehouses)
			runTPCC(ctx, t, c, tpccOptions{
				Warehouses: headroomWarehouses,
				Duration:   120 * time.Minute,
				SetupType:  usingImport,
			})
		},
	})
	mixedHeadroomSpec := r.MakeClusterSpec(5, spec.CPU(16), spec.RandomlyUseZfs())

	r.Add(registry.TestSpec{
		// mixed-headroom is similar to w=headroom, but with an additional
		// node and on a mixed version cluster which runs its long-running
		// migrations while TPCC runs. It simulates a real production
		// deployment in the middle of the migration into a new cluster version.
		Name:    "tpcc/mixed-headroom/" + mixedHeadroomSpec.String(),
		Timeout: 5 * time.Hour,
		Owner:   registry.OwnerTestEng,
		// TODO(tbg): add release_qualification tag once we know the test isn't
		// buggy.
		Tags:              registry.Tags(`default`),
		Cluster:           mixedHeadroomSpec,
		EncryptionSupport: registry.EncryptionMetamorphic,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runTPCCMixedHeadroom(ctx, t, c, cloud, 1)
		},
	})

	// N.B. Multiple upgrades may require a released version < 22.2.x, which wasn't built for ARM64.
	mixedHeadroomMultiUpgradesSpec := r.MakeClusterSpec(5, spec.CPU(16), spec.RandomlyUseZfs(), spec.Arch(vm.ArchAMD64))

	r.Add(registry.TestSpec{
		// run the same mixed-headroom test, but going back two versions
		Name:              "tpcc/mixed-headroom/multiple-upgrades/" + mixedHeadroomMultiUpgradesSpec.String(),
		Timeout:           5 * time.Hour,
		Owner:             registry.OwnerTestEng,
		Tags:              registry.Tags(`default`),
		Cluster:           mixedHeadroomMultiUpgradesSpec,
		EncryptionSupport: registry.EncryptionMetamorphic,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runTPCCMixedHeadroom(ctx, t, c, cloud, 2)
		},
	})
	r.Add(registry.TestSpec{
		Name:              "tpcc-nowait/nodes=3/w=1",
		Owner:             registry.OwnerTestEng,
		Cluster:           r.MakeClusterSpec(4, spec.CPU(16)),
		EncryptionSupport: registry.EncryptionMetamorphic,
		Leases:            registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runTPCC(ctx, t, c, tpccOptions{
				Warehouses:   1,
				Duration:     10 * time.Minute,
				ExtraRunArgs: "--wait=false",
				SetupType:    usingImport,
			})
		},
	})
	r.Add(registry.TestSpec{
		Name:    "weekly/tpcc/headroom",
		Owner:   registry.OwnerTestEng,
		Tags:    registry.Tags(`weekly`),
		Cluster: r.MakeClusterSpec(4, spec.CPU(16)),
		// Give the test a generous extra 10 hours to load the dataset and
		// slowly ramp up the load.
		Timeout:           4*24*time.Hour + 10*time.Hour,
		EncryptionSupport: registry.EncryptionMetamorphic,
		Leases:            registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			warehouses := 1000
			runTPCC(ctx, t, c, tpccOptions{
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
				Cluster:           r.MakeClusterSpec(len(regions)*nodesPerRegion+1, spec.Geo(), spec.Zones(strings.Join(zs, ","))),
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
					runTPCC(ctx, t, c, tpccOptions{
						Warehouses:     len(regions) * warehousesPerRegion,
						Duration:       duration,
						ExtraSetupArgs: partitionArgs,
						ExtraRunArgs:   `--method=simple --wait=false --tolerate-errors ` + partitionArgs,
						Chaos: func() Chaos {
							return Chaos{
								Timer: Periodic{
									Period:   300 * time.Second,
									DownTime: 300 * time.Second,
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
					})
				},
			})
		}
	}

	r.Add(registry.TestSpec{
		Name:              "tpcc/w=100/nodes=3/chaos=true",
		Owner:             registry.OwnerTestEng,
		Cluster:           r.MakeClusterSpec(4),
		EncryptionSupport: registry.EncryptionMetamorphic,
		Leases:            registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			duration := 30 * time.Minute
			runTPCC(ctx, t, c, tpccOptions{
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
			})
		},
	})

	// Run a few representative tpccbench specs in CI.
	registerTPCCBenchSpec(r, tpccBenchSpec{
		Nodes: 3,
		CPUs:  4,

		LoadWarehouses: 1000,
		EstimatedMax:   gceOrAws(cloud, 750, 900),
	})
	registerTPCCBenchSpec(r, tpccBenchSpec{
		Nodes: 3,
		CPUs:  4,

		LoadWarehouses:  1000,
		EstimatedMax:    gceOrAws(cloud, 750, 900),
		SharedProcessMT: true,
	})
	registerTPCCBenchSpec(r, tpccBenchSpec{
		Nodes:                        3,
		CPUs:                         4,
		EnableDefaultScheduledBackup: true,
		LoadWarehouses:               1000,
		EstimatedMax:                 gceOrAws(cloud, 750, 900),
	})
	registerTPCCBenchSpec(r, tpccBenchSpec{
		Nodes: 3,
		CPUs:  16,

		LoadWarehouses: gceOrAws(cloud, 3500, 3900),
		EstimatedMax:   gceOrAws(cloud, 2900, 3500),
		Tags:           registry.Tags(`aws`),
	})
	registerTPCCBenchSpec(r, tpccBenchSpec{
		Nodes: 3,
		CPUs:  16,

		LoadWarehouses:  gceOrAws(cloud, 3500, 3900),
		EstimatedMax:    gceOrAws(cloud, 2900, 3500),
		Tags:            registry.Tags(`aws`),
		SharedProcessMT: true,
	})
	registerTPCCBenchSpec(r, tpccBenchSpec{
		Nodes: 12,
		CPUs:  16,

		LoadWarehouses: gceOrAws(cloud, 11500, 11500),
		EstimatedMax:   gceOrAws(cloud, 10000, 10000),

		Tags: registry.Tags(`weekly`),
	})
	registerTPCCBenchSpec(r, tpccBenchSpec{
		Nodes:        6,
		CPUs:         16,
		Distribution: multiZone,

		LoadWarehouses: 6500,
		EstimatedMax:   5000,
	})
	registerTPCCBenchSpec(r, tpccBenchSpec{
		Nodes:        9,
		CPUs:         4,
		HighMem:      true, // can OOM otherwise: https://github.com/cockroachdb/cockroach/issues/73376
		Distribution: multiRegion,
		LoadConfig:   multiLoadgen,

		LoadWarehouses: 3000,
		EstimatedMax:   2000,
	})
	registerTPCCBenchSpec(r, tpccBenchSpec{
		Nodes:      9,
		CPUs:       4,
		Chaos:      true,
		LoadConfig: singlePartitionedLoadgen,

		LoadWarehouses: 2000,
		EstimatedMax:   900,
	})

	// Encryption-At-Rest benchmarks. These are duplicates of variants above,
	// using encrypted stores.
	registerTPCCBenchSpec(r, tpccBenchSpec{
		Nodes: 3,
		CPUs:  4,

		LoadWarehouses:    1000,
		EstimatedMax:      gceOrAws(cloud, 750, 900),
		EncryptionEnabled: true,
	})
	registerTPCCBenchSpec(r, tpccBenchSpec{
		Nodes: 3,
		CPUs:  16,

		LoadWarehouses:    gceOrAws(cloud, 3500, 3900),
		EstimatedMax:      gceOrAws(cloud, 2900, 3500),
		EncryptionEnabled: true,
		Tags:              registry.Tags(`aws`),
	})
	registerTPCCBenchSpec(r, tpccBenchSpec{
		Nodes: 12,
		CPUs:  16,

		LoadWarehouses:    gceOrAws(cloud, 11500, 11500),
		EstimatedMax:      gceOrAws(cloud, 10000, 10000),
		EncryptionEnabled: true,

		Tags: registry.Tags(`weekly`),
	})

	// Expiration lease benchmarks. These are duplicates of variants above.
	registerTPCCBenchSpec(r, tpccBenchSpec{
		Nodes: 3,
		CPUs:  4,

		LoadWarehouses:   1000,
		EstimatedMax:     gceOrAws(cloud, 750, 900),
		ExpirationLeases: true,
	})
	registerTPCCBenchSpec(r, tpccBenchSpec{
		Nodes: 3,
		CPUs:  16,

		LoadWarehouses:   gceOrAws(cloud, 3500, 3900),
		EstimatedMax:     gceOrAws(cloud, 2900, 3500),
		ExpirationLeases: true,
		Tags:             registry.Tags(`aws`),
	})
	registerTPCCBenchSpec(r, tpccBenchSpec{
		Nodes: 12,
		CPUs:  16,

		LoadWarehouses:   gceOrAws(cloud, 11500, 11500),
		EstimatedMax:     gceOrAws(cloud, 10000, 10000),
		ExpirationLeases: true,

		Tags: registry.Tags(`weekly`),
	})
}

func gceOrAws(cloud string, gce, aws int) int {
	if cloud == "aws" {
		return aws
	}
	return gce
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
	LoadWarehouses int
	// An estimate of the maximum number of warehouses achievable in the
	// cluster config. The closer this is to the actual max achievable
	// warehouse count, the faster the benchmark will be in producing a
	// result. This can be adjusted over time as performance characteristics
	// change (i.e. CockroachDB gets faster!).
	EstimatedMax int

	// Tags to pass to testRegistryImpl.Add.
	Tags map[string]struct{}
	// EncryptionEnabled determines if the benchmark uses encrypted stores (i.e.
	// Encryption-At-Rest / EAR).
	EncryptionEnabled bool
	// ExpirationLeases enables use of expiration-based leases.
	ExpirationLeases             bool
	EnableDefaultScheduledBackup bool
	// SharedProcessMT, if true, indicates that the cluster should run in
	// shared-process mode of multi-tenancy.
	SharedProcessMT bool
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
		opts = append(opts, spec.Geo(), spec.Zones(strings.Join(b.Distribution.zones(), ",")))
	case multiRegion:
		nameParts = append(nameParts, "multi-region")
		opts = append(opts, spec.Geo(), spec.Zones(strings.Join(b.Distribution.zones(), ",")))
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
		Tags:              b.Tags,
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
		if curWarehouses >= b.LoadWarehouses {
			// The cluster has enough warehouses. Nothing to do.
			return nil
		}

		// If the dataset exists but is not large enough, wipe the cluster
		// before restoring.
		c.Wipe(ctx, false /* preserveCerts */, roachNodes)
		startOpts, settings := b.startOpts()
		c.Start(ctx, t.L(), startOpts, settings, roachNodes)
	} else if pqErr := (*pq.Error)(nil); !(errors.As(err, &pqErr) &&
		pgcode.MakeCode(string(pqErr.Code)) == pgcode.InvalidCatalogName) {
		return err
	}

	var loadArgs string
	var rebalanceWait time.Duration
	switch b.LoadConfig {
	case singleLoadgen:
		loadArgs = `--checks=false`
		rebalanceWait = time.Duration(b.LoadWarehouses/250) * time.Minute
	case singlePartitionedLoadgen:
		loadArgs = fmt.Sprintf(`--checks=false --partitions=%d`, b.partitions())
		rebalanceWait = time.Duration(b.LoadWarehouses/125) * time.Minute
	case multiLoadgen:
		loadArgs = fmt.Sprintf(`--checks=false --partitions=%d --zones="%s"`,
			b.partitions(), strings.Join(b.Distribution.zones(), ","))
		rebalanceWait = time.Duration(b.LoadWarehouses/50) * time.Minute
	default:
		panic("unexpected")
	}

	// Load the corresponding fixture.
	t.L().Printf("restoring tpcc fixture\n")
	err := WaitFor3XReplication(ctx, t, db)
	require.NoError(t, err)
	var pgurl string
	if b.SharedProcessMT {
		pgurl = fmt.Sprintf("{pgurl%s:%s}", roachNodes[:1], appTenantName)
	}
	cmd := tpccImportCmd(b.LoadWarehouses, loadArgs, pgurl)
	if err = c.RunE(ctx, roachNodes[:1], cmd); err != nil {
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
	maxRate := tpccMaxRate(b.EstimatedMax)
	rampTime := (1 * rebalanceWait) / 4
	loadTime := (3 * rebalanceWait) / 4
	var tenantSuffix string
	if b.SharedProcessMT {
		tenantSuffix = fmt.Sprintf(":%s", appTenantName)
	}
	cmd = fmt.Sprintf("./cockroach workload run tpcc --warehouses=%d --workers=%d --max-rate=%d "+
		"--wait=false --ramp=%s --duration=%s --scatter --tolerate-errors {pgurl%s%s}",
		b.LoadWarehouses, b.LoadWarehouses, maxRate, rampTime, loadTime, roachNodes, tenantSuffix)
	if _, err := c.RunWithDetailsSingleNode(ctx, t.L(), loadNode, cmd); err != nil {
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
	loadGroups := makeLoadGroups(c, numZones, b.Nodes, numLoadGroups)
	roachNodes := loadGroups.roachNodes()
	loadNodes := loadGroups.loadNodes()
	c.Put(ctx, t.Cockroach(), "./cockroach", roachNodes)
	// Fixture import needs './cockroach workload' on loadNodes[0],
	// and if we use haproxy (see below) we need it on the others
	// as well.
	c.Put(ctx, t.Cockroach(), "./cockroach", loadNodes)
	// Don't encrypt in tpccbench tests.
	startOpts, settings := b.startOpts()
	c.Start(ctx, t.L(), startOpts, settings, roachNodes)

	var db *gosql.DB
	if b.SharedProcessMT {
		db = createInMemoryTenant(ctx, t, c, appTenantName, roachNodes, false /* secure */)
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
			c.Run(ctx, loadNodes, "./cockroach gen haproxy --insecure --url {pgurl:1}")
			// Increase the maximum connection limit to ensure that no TPC-C
			// load gen workers get stuck during connection initialization.
			// 10k warehouses requires at least 20,000 connections, so add a
			// bit of breathing room and check the warehouse count.
			c.Run(ctx, loadNodes, "sed -i 's/maxconn [0-9]\\+/maxconn 21000/' haproxy.cfg")
			if b.LoadWarehouses > 1e4 {
				t.Fatal("HAProxy config supports up to 10k warehouses")
			}
			c.Run(ctx, loadNodes, "haproxy -f haproxy.cfg -D")
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
	precision := int(math.Max(1.0, float64(b.LoadWarehouses/200)))
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

	s := search.NewLineSearcher(1, b.LoadWarehouses, b.EstimatedMax, initStepSize, precision)
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
				sqlGateways := group.roachNodes
				if useHAProxy {
					sqlGateways = group.loadNodes
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
					b.LoadWarehouses, warehouses, rampDur,
					loadDur, extraFlags, histogramsPath, sqlGateways, tenantSuffix)
				err := c.RunE(ctx, group.loadNodes, cmd)
				loadDone <- timeutil.Now()
				if err != nil {
					// NB: this will let the line search continue at a lower warehouse
					// count.
					return errors.Wrapf(err, "error running tpcc load generator")
				}
				roachtestHistogramsPath := filepath.Join(resultsDir, fmt.Sprintf("%d.%d-stats.json", warehouses, groupIdx))
				if err := c.Get(
					ctx, t.L(), histogramsPath, roachtestHistogramsPath, group.loadNodes,
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
		workloadNode := c.Node(c.Spec().NodeCount).InstallNodes()[0]
		cfg.WithPrometheusNode(workloadNode)
		cfg.WithNodeExporter(c.Range(1, c.Spec().NodeCount-1).InstallNodes())
		cfg.WithCluster(c.Range(1, c.Spec().NodeCount-1).InstallNodes())
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
