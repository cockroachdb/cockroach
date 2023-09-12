// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package mixedversion implements a framework for testing
// functionality when a cluster is in a mixed-version state.
//
// Mixed-version is defined as points in time when some (potentially
// not all) nodes have been upgraded to a certain binary version, or
// the state when all nodes have been upgraded, but the cluster itself
// is in the process of upgrading to that version (i.e., running
// migrations). This also applies to clusters that have upgraded
// binaries on all nodes and then later decided to downgrade to a
// previous version (which is permitted if the `preserve_downgrade_option`
// cluster setting is set accordingly).
//
// The goal of the framework is to let test writers focus on the
// functionality they want to test in a mixed-version state, rather
// than having to worry about how to set that up, stopping and
// restarting nodes, etc.
//
// In the most common use-case, callers will just call the
// `InMixedVersion` function on the test object defining what they
// want to test in a mixed-version state. The framework will take care
// of exercising that functionality in different mixed-version states,
// randomizing the order of events in each run. A detailed test plan
// is logged *before* the test runs; it facilitates debugging a test
// failure by summarizing all executed steps.
//
// Typical usage:
//
//			mvt, err := mixedversion.NewTest(...)
//			mvt.InMixedVersion("test my feature", func(
//		  ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper,
//		 ) error {
//			    l.Printf("testing feature X")
//			    node, db := h.RandomDB(rng, c.All())
//			    l.Printf("running query on node %d", node)
//			    _, err := db.ExecContext(ctx, "SELECT * FROM test")
//			    return err
//			})
//			mvt.InMixedVersion("test another feature", func(
//	     ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper,
//	   ) error {
//			    l.Printf("testing feature Y")
//			    node, db := h.RandomDB(rng, c.All())
//			    l.Printf("running query on node %d", node)
//			    _, err := db.ExecContext(ctx, "SELECT * FROM test2")
//			    return err
//			})
//			mvt.Run()
//
// Functions passed to `InMixedVersion` will be called at arbitrary
// points during an upgrade/downgrade process. They may also be called
// sequentially or concurrently. This information will be included in
// the test plan. Any messages logged during the execution of the hook
// should use the Logger instance passed as a parameter to it. This
// logger writes to a `mixed-version-test/{ID}.log` file, allowing
// easy cross referencing between a test step and its corresponding
// output.
//
// The random seed used to generate a test plan is logged; reusing the
// same seed will lead to the exact same test plan being generated. To
// set a specific seed when running a mixed-version test, one can set
// the `COCKROACH_RANDOM_SEED` environment variable.
package mixedversion

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/testutils/release"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/pkg/errors"
)

const (
	logPrefix         = "mixed-version-test"
	startupLabel      = "run startup hooks"
	backgroundLabel   = "start background hooks"
	mixedVersionLabel = "run mixed-version hooks"
	afterTestLabel    = "run after test hooks"

	// runWhileMigratingProbability is the probability that a
	// mixed-version hook will run after all nodes in the cluster have
	// been upgraded to the same binary version, and the cluster version
	// is allowed to be upgraded; this typically nvolves the execution
	// of migration steps before the new cluster version can be
	// finalized.
	runWhileMigratingProbability = 0.5

	// rollbackIntermediateUpgradesProbability is the probability that
	// an "intermediate" upgrade (i.e., an upgrade to a version older
	// than the one being tested) will also go through a rollback during
	// a test run.
	rollbackIntermediateUpgradesProbability = 0.3

	// numNodesInFixtures is the number of nodes expected to exist in a
	// cluster that can use the test fixtures in
	// `pkg/cmd/roachtest/fixtures`.
	numNodesInFixtures = 4
)

var (
	// possibleDelaysMs lists the possible delays to be added to
	// concurrent steps
	possibleDelaysMs = []int{
		0, 50, 100, 200, 500,
	}

	// defaultClusterSettings is the set of cluster settings always
	// passed to `clusterupgrade.StartWithSettings` when (re)starting
	// nodes in a cluster.
	defaultClusterSettings = []install.ClusterSettingOption{
		install.SecureOption(true),
	}

	// minSupportedARM64Version is the minimum version for which there
	// is a published ARM64 build. If we are running a mixedversion test
	// on ARM64, older versions will be skipped even if the test
	// requested a certain number of upgrades.
	minSupportedARM64Version = version.MustParse("v22.2.0")

	defaultTestOptions = testOptions{
		// We use fixtures more often than not as they are more likely to
		// detect bugs, especially in migrations.
		useFixturesProbability: 0.7,
		upgradeTimeout:         clusterupgrade.DefaultUpgradeTimeout,
		minUpgrades:            1,
		maxUpgrades:            3,
		predecessorFunc:        release.RandomPredecessorHistory,
	}
)

type (
	// Context wraps the context passed to predicate functions that
	// dictate when a mixed-version hook will run during a test
	Context struct {
		// FromVersion is the string representation of the version the
		// nodes are migrating from
		FromVersion string
		// FromVersionNodes are the nodes that are currently running
		// `FromVersion`
		FromVersionNodes option.NodeListOption
		// ToVersion is the string representation of the version the nodes
		// are migrating to
		ToVersion string
		// ToVersionNodes are the nodes that are currently running
		// `ToVersion`
		ToVersionNodes option.NodeListOption
		// Finalizing indicates whether the cluster version is in the
		// process of upgrading (i.e., all nodes in the cluster have been
		// upgraded to a certain version, and the migrations are being
		// executed).
		Finalizing bool
	}

	// userFunc is the signature for user-provided functions that run at
	// various points in the test (synchronously or in the background).
	// These functions run on the test runner node itself; i.e., any
	// commands they wish to execute on the cluster need to go through
	// the `cluster` methods as usual (cluster.RunE, cluster.PutE,
	// etc). In addition, these functions should prefer returning an
	// error over calling `t.Fatal` directly. The error is handled by
	// the mixedversion framework and better error messages are produced
	// as a result.
	userFunc      func(context.Context, *logger.Logger, *rand.Rand, *Helper) error
	predicateFunc func(Context) bool

	// versionUpgradeHook is a hook that can be called at any time
	// during an upgrade/downgrade process. The `predicate` is used
	// during test planning to determine when it is called: currently,
	// it is always auto-generated, but in the future, this package
	// could expose an API that would allow callers to have control of
	// when their hooks should run.
	versionUpgradeHook struct {
		name      string
		predicate predicateFunc
		fn        userFunc
	}

	// testStep is an opaque reference to one step of a mixed-version
	// test. It can be a singleStep (see below), or a "meta-step",
	// meaning that it combines other steps in some way (for instance, a
	// series of steps to be run sequentially or concurrently).
	testStep interface{}

	// singleStep represents steps that implement the pieces on top of
	// which a mixed-version test is built. In other words, they are not
	// composed by other steps and hence can be directly executed.
	singleStep interface {
		// ID returns a unique ID associated with the step, making it easy
		// to reference test output with the exact step it relates to
		ID() int
		// Description is a string representation of the step, intended
		// for human-consumption. Displayed when pretty-printing the test
		// plan.
		Description() string
		// Background returns a channel that controls the execution of a
		// background step: when that channel is closed, the context
		// associated with the step will be canceled. Returning `nil`
		// indicates that the step should not be run in the background.
		// When a step is *not* run in the background, the test will wait
		// for it to finish before moving on. When a background step
		// fails, the entire test fails.
		Background() shouldStop
		// Run implements the actual functionality of the step.
		Run(context.Context, *logger.Logger, cluster.Cluster, *Helper) error
	}

	hooks []versionUpgradeHook

	// testHooks groups hooks associated with a mixed-version test in
	// its different stages: startup, mixed-version, and after-test.
	testHooks struct {
		startup               hooks
		background            hooks
		mixedVersion          hooks
		afterUpgradeFinalized hooks

		prng      *rand.Rand
		crdbNodes option.NodeListOption
	}

	// testOptions contains some options that can be changed by the user
	// that expose some control over the generated test plan and behaviour.
	testOptions struct {
		useFixturesProbability float64
		upgradeTimeout         time.Duration
		minUpgrades            int
		maxUpgrades            int
		predecessorFunc        predecessorFunc
	}

	customOption func(*testOptions)

	predecessorFunc func(*rand.Rand, *version.Version, int) ([]string, error)

	// Test is the main struct callers of this package interact with.
	Test struct {
		ctx       context.Context
		cancel    context.CancelFunc
		cluster   cluster.Cluster
		logger    *logger.Logger
		crdbNodes option.NodeListOption

		options testOptions

		rt    test.Test
		prng  *rand.Rand
		seed  int64
		hooks *testHooks

		// bgChans collects all channels that control the execution of
		// background steps in the test (created with `BackgroundFunc`,
		// `Workload`, et al.). These channels are passsed to the test
		// runner, and the test author can stop a background function by
		// closing the channel. When that happens, the test runner will
		// cancel the underlying context.
		//
		// Invariant: there is exactly one channel in `bgChans` for each
		// hook in `Test.hooks.background`.
		bgChans []shouldStop

		// predecessorFunc computes the predecessor versions to be used in
		// a test run. By default, random predecessors are used, but tests
		// may choose to always use the latest predecessor as well.
		predecessorFunc predecessorFunc

		// test-only field, allowing us to avoid passing a test.Test
		// implementation in the tests
		_buildVersion *version.Version
		// test-only field, allowing tests to simulate cluster
		// architectures without passing a cluster.Cluster implementation.
		_arch *vm.CPUArch
	}

	shouldStop chan struct{}

	// StopFunc is the signature of the function returned by calls that
	// create background steps. StopFuncs are meant to be called by test
	// authors when they want to stop a background step as part of test
	// logic itself, without causing the test to fail.
	StopFunc func()
)

// NeverUseFixtures is an option that can be passed to `NewTest` to
// disable the use of fixtures in the test. Necessary if the test
// wants to use a number of cockroach nodes other than 4.
func NeverUseFixtures(opts *testOptions) {
	opts.useFixturesProbability = 0
}

// AlwaysUseFixtures is an option that can be passed to `NewTest` to
// force the test to always start the cluster from the fixtures in
// `pkg/cmd/roachtest/fixtures`. Necessary if the test makes
// assertions that rely on the existence of data present in the
// fixtures.
func AlwaysUseFixtures(opts *testOptions) {
	opts.useFixturesProbability = 1
}

// UpgradeTimeout allows test authors to provide a different timeout
// to apply when waiting for an upgrade to finish.
func UpgradeTimeout(timeout time.Duration) customOption {
	return func(opts *testOptions) {
		opts.upgradeTimeout = timeout
	}
}

// MinUpgrades allows callers to set a minimum number of upgrades each
// test run should exercise.
func MinUpgrades(n int) customOption {
	return func(opts *testOptions) {
		opts.minUpgrades = n
	}
}

// MaxUpgrades allows callers to set a maximum number of upgrades to
// be performed during a test run.
func MaxUpgrades(n int) customOption {
	return func(opts *testOptions) {
		opts.maxUpgrades = n
	}
}

// NumUpgrades allows callers to specify the exact number of upgrades
// every test run should perform.
func NumUpgrades(n int) customOption {
	return func(opts *testOptions) {
		opts.minUpgrades = n
		opts.maxUpgrades = n
	}
}

// AlwaysUseLatestPredecessors allows test authors to opt-out of
// testing upgrades from random predecessor patch releases. The
// default is to pick a random (non-withdrawn) patch release for
// predecessors used in the test, as that better reflects upgrades
// performed by customers. However, teams have the option to always
// use the latest predecessor to avoid the burden of having to update
// tests to account for issues that have already been fixed in
// subsequent patch releases. If possible, this option should be
// avoided, but it might be necessary in certain cases to reduce noise
// in case the test is more susceptible to fail due to known bugs.
func AlwaysUseLatestPredecessors() customOption {
	return func(opts *testOptions) {
		opts.predecessorFunc = func(_ *rand.Rand, v *version.Version, n int) ([]string, error) {
			return release.LatestPredecessorHistory(v, n)
		}
	}
}

// NewTest creates a Test struct that users can use to create and run
// a mixed-version roachtest.
func NewTest(
	ctx context.Context,
	t test.Test,
	l *logger.Logger,
	c cluster.Cluster,
	crdbNodes option.NodeListOption,
	options ...customOption,
) *Test {
	testLogger, err := prefixedLogger(l, logPrefix)
	if err != nil {
		t.Fatal(err)
	}

	opts := defaultTestOptions
	for _, fn := range options {
		fn(&opts)
	}

	prng, seed := randutil.NewLockedPseudoRand()
	testLogger.Printf("mixed-version random seed: %d", seed)

	testCtx, cancel := context.WithCancel(ctx)
	test := &Test{
		ctx:             testCtx,
		cancel:          cancel,
		cluster:         c,
		logger:          testLogger,
		crdbNodes:       crdbNodes,
		options:         opts,
		rt:              t,
		prng:            prng,
		seed:            seed,
		hooks:           &testHooks{prng: prng, crdbNodes: crdbNodes},
		predecessorFunc: opts.predecessorFunc,
	}

	assertValidTest(test, t.Fatal)
	return test
}

// RNG returns the underlying random number generator used by the
// mixedversion framework to generate a test plan. This rng can be
// used to make random decisions during test setup.
//
// Do NOT use the rng returned by this function in mixedversion hooks
// (functions passed to `InMixedVersion` and similar). Instead, use
// the rng instance directly passed as argument to those functions.
func (t *Test) RNG() *rand.Rand {
	return t.prng
}

// InMixedVersion adds a new mixed-version hook to the test. The
// functionality in the function passed as argument to this function
// will be tested in arbitrary mixed-version states. If multiple
// InMixedVersion hooks are passed, they will be executed
// concurrently.
func (t *Test) InMixedVersion(desc string, fn userFunc) {
	lastFromVersion := "invalid-version"
	var numUpgradedNodes int
	predicate := func(testContext Context) bool {
		// If the cluster is finalizing an upgrade, run this hook
		// according to the probability defined in the package.
		if testContext.Finalizing {
			return t.prng.Float64() < runWhileMigratingProbability
		}

		// This check makes sure we only schedule a mixed-version hook
		// once while upgrading from one version to another. The number of
		// nodes we wait to be running the new version is determined when
		// the version changes for the first time.
		if testContext.FromVersion != lastFromVersion {
			lastFromVersion = testContext.FromVersion
			numUpgradedNodes = t.prng.Intn(len(t.crdbNodes)) + 1
		}

		return len(testContext.ToVersionNodes) == numUpgradedNodes
	}

	t.hooks.AddMixedVersion(versionUpgradeHook{name: desc, predicate: predicate, fn: fn})
}

// OnStartup registers a callback that is run once the cluster is
// initialized (i.e., all nodes in the cluster start running CRDB at a
// certain previous version, potentially from existing fixtures). If
// multiple OnStartup hooks are passed, they will be executed
// concurrently.
func (t *Test) OnStartup(desc string, fn userFunc) {
	// Since the callbacks here are only referenced in the setup steps
	// of the planner, there is no need to have a predicate function
	// gating them.
	t.hooks.AddStartup(versionUpgradeHook{name: desc, fn: fn})
}

// AfterUpgradeFinalized registers a callback that is run once the
// mixed-version test has brought the cluster to the latest version,
// and allowed the upgrade to finalize successfully. If multiple such
// hooks are passed, they will be executed concurrently.
func (t *Test) AfterUpgradeFinalized(desc string, fn userFunc) {
	t.hooks.AddAfterUpgradeFinalized(versionUpgradeHook{name: desc, fn: fn})
}

// BackgroundFunc runs the function passed as argument in the
// background during the test. Background functions are kicked off
// once the cluster has been initialized (i.e., after all startup
// steps have finished). If the `userFunc` returns an error, it will
// cause the test to fail. These functions can run indefinitely but
// should respect the context passed to them, which will be canceled
// when the test terminates (successfully or not). Returns a function
// that can be called to terminate the step, which will cancel the
// context passed to `userFunc`.
func (t *Test) BackgroundFunc(desc string, fn userFunc) StopFunc {
	t.hooks.AddBackground(versionUpgradeHook{name: desc, fn: fn})

	ch := make(shouldStop)
	t.bgChans = append(t.bgChans, ch)
	var closeOnce sync.Once
	// Make sure to only close the background channel once, allowing the
	// caller to call the StopFunc multiple times (subsequent calls will
	// be no-ops).
	return func() { closeOnce.Do(func() { close(ch) }) }
}

// BackgroundCommand is a convenience wrapper around `BackgroundFunc`
// that runs the command passed once the cluster is initialized. The
// node where the command runs is picked randomly.
//
// TODO: unfortunately, `cluster.Run()` does not allow the caller to
// pass a logger instance. It would be convenient if the output of the
// command itself lived within the `mixed-version/*.log` files.
func (t *Test) BackgroundCommand(
	desc string, nodes option.NodeListOption, cmd *roachtestutil.Command,
) StopFunc {
	return t.BackgroundFunc(desc, t.runCommandFunc(nodes, cmd.String()))
}

// Workload is a convenience wrapper that allows callers to run
// workloads in the background during a mixed-version test. `initCmd`,
// if passed, is the command run to initialize the workload; it is run
// synchronously as a regular startup function. `runCmd` is the
// command to actually run the command; it is run in the background.
func (t *Test) Workload(
	name string, node option.NodeListOption, initCmd, runCmd *roachtestutil.Command,
) StopFunc {
	seed := uint64(t.prng.Int63())
	addSeed := func(cmd *roachtestutil.Command) {
		if !cmd.HasFlag("seed") {
			cmd.Flag("seed", seed)
		}
	}

	if initCmd != nil {
		addSeed(initCmd)
		t.OnStartup(fmt.Sprintf("initialize %s workload", name), t.runCommandFunc(node, initCmd.String()))
	}

	addSeed(runCmd)
	return t.BackgroundCommand(fmt.Sprintf("%s workload", name), node, runCmd)
}

// Run runs the mixed-version test. It should be called once all
// startup, mixed-version, and after-test hooks have been declared. A
// test plan will be generated (and logged), and the test will be
// carried out.
func (t *Test) Run() {
	plan, err := t.plan()
	if err != nil {
		t.rt.Fatal(fmt.Errorf("error creating test plan: %w", err))
	}

	t.logger.Printf(plan.PrettyPrint())

	if err := t.run(plan); err != nil {
		t.rt.Fatal(err)
	}
}

func (t *Test) run(plan *TestPlan) error {
	return newTestRunner(
		t.ctx, t.cancel, plan, t.logger, t.cluster, t.crdbNodes, t.seed,
	).run()
}

func (t *Test) plan() (*TestPlan, error) {
	previousReleases, err := t.choosePreviousReleases()
	if err != nil {
		return nil, err
	}

	planner := testPlanner{
		versions:  append(previousReleases, clusterupgrade.MainVersion),
		options:   t.options,
		rt:        t.rt,
		crdbNodes: t.crdbNodes,
		hooks:     t.hooks,
		prng:      t.prng,
		bgChans:   t.bgChans,
	}

	return planner.Plan(), nil
}

func (t *Test) buildVersion() *version.Version {
	if t._buildVersion != nil {
		return t._buildVersion // test-only
	}

	return t.rt.BuildVersion()
}

func (t *Test) clusterArch() vm.CPUArch {
	if t._arch != nil {
		return *t._arch // test-only
	}

	return t.cluster.Architecture()
}

func (t *Test) runCommandFunc(nodes option.NodeListOption, cmd string) userFunc {
	return func(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *Helper) error {
		l.Printf("running command `%s` on nodes %v", cmd, nodes)
		return t.cluster.RunE(ctx, nodes, cmd)
	}
}

// choosePreviousReleases returns a list of predecessor releases
// relative to the current build version. It uses the
// `predecessorFunc` field to compute the actual list of
// predecessors. Special care is taken to avoid using releases that
// are not available under a certain cluster architecture.
// Specifically, ARM64 builds are only available on v22.2.0+.
func (t *Test) choosePreviousReleases() ([]string, error) {
	history, err := t.predecessorFunc(t.prng, t.buildVersion(), t.numUpgrades())
	if err != nil {
		return nil, err
	}

	if t.clusterArch() != vm.ArchARM64 {
		return history, nil
	}

	var releases []string
	for _, r := range history {
		if version.MustParse("v" + r).AtLeast(minSupportedARM64Version) {
			releases = append(releases, r)
		}
	}

	if len(releases) != len(history) {
		t.logger.Printf("WARNING: skipping upgrades as ARM64 is only supported on %s+", minSupportedARM64Version)
	}

	return releases, nil
}

// numUpgrades returns the number of upgrades that will be performed
// in this test run. Returns a number in the [minUpgrades, maxUpgrades]
// range.
func (t *Test) numUpgrades() int {
	return t.prng.Intn(
		t.options.maxUpgrades-t.options.minUpgrades+1,
	) + t.options.minUpgrades
}

// installFixturesStep is the step that copies the fixtures from
// `pkg/cmd/roachtest/fixtures` for a specific version into the nodes'
// store dir.
type installFixturesStep struct {
	id        int
	version   string
	crdbNodes option.NodeListOption
}

func (s installFixturesStep) ID() int                { return s.id }
func (s installFixturesStep) Background() shouldStop { return nil }

func (s installFixturesStep) Description() string {
	return fmt.Sprintf("install fixtures for version %q", s.version)
}

func (s installFixturesStep) Run(
	ctx context.Context, l *logger.Logger, c cluster.Cluster, h *Helper,
) error {
	return clusterupgrade.InstallFixtures(ctx, l, c, s.crdbNodes, s.version)
}

// startStep is the step that starts the cluster from a specific
// `version`.
type startStep struct {
	id        int
	rt        test.Test
	version   string
	crdbNodes option.NodeListOption
}

func (s startStep) ID() int                { return s.id }
func (s startStep) Background() shouldStop { return nil }

func (s startStep) Description() string {
	return fmt.Sprintf("start cluster at version %q", s.version)
}

// Run uploads the binary associated with the given version and starts
// the cockroach binary on the nodes.
func (s startStep) Run(ctx context.Context, l *logger.Logger, c cluster.Cluster, h *Helper) error {
	binaryPath, err := clusterupgrade.UploadVersion(ctx, s.rt, l, c, s.crdbNodes, s.version)
	if err != nil {
		return err
	}

	startOpts := option.DefaultStartOptsNoBackups()
	startOpts.RoachprodOpts.Sequential = false
	clusterSettings := append(
		append([]install.ClusterSettingOption{}, defaultClusterSettings...),
		install.BinaryOption(binaryPath),
	)
	return clusterupgrade.StartWithSettings(ctx, l, c, s.crdbNodes, startOpts, clusterSettings...)
}

// waitForStableClusterVersionStep implements the process of waiting
// for the `version` cluster setting being the same on all nodes of
// the cluster and equal to the binary version of the first node in
// the `nodes` field.
type waitForStableClusterVersionStep struct {
	id      int
	nodes   option.NodeListOption
	timeout time.Duration
}

func (s waitForStableClusterVersionStep) ID() int                { return s.id }
func (s waitForStableClusterVersionStep) Background() shouldStop { return nil }

func (s waitForStableClusterVersionStep) Description() string {
	return fmt.Sprintf(
		"wait for nodes %v to all have the same cluster version (same as binary version of node %d)",
		s.nodes, s.nodes[0],
	)
}

func (s waitForStableClusterVersionStep) Run(
	ctx context.Context, l *logger.Logger, c cluster.Cluster, h *Helper,
) error {
	return clusterupgrade.WaitForClusterUpgrade(ctx, l, s.nodes, h.Connect, s.timeout)
}

// preserveDowngradeOptionStep sets the `preserve_downgrade_option`
// cluster setting to the binary version running in `node`.
type preserveDowngradeOptionStep struct {
	id        int
	crdbNodes option.NodeListOption
	prng      *rand.Rand
}

func (s preserveDowngradeOptionStep) ID() int                { return s.id }
func (s preserveDowngradeOptionStep) Background() shouldStop { return nil }

func (s preserveDowngradeOptionStep) Description() string {
	return "prevent auto-upgrades by setting `preserve_downgrade_option`"
}

func (s preserveDowngradeOptionStep) Run(
	ctx context.Context, l *logger.Logger, c cluster.Cluster, h *Helper,
) error {
	node, db := h.RandomDB(s.prng, s.crdbNodes)
	l.Printf("checking binary version (via node %d)", node)
	bv, err := clusterupgrade.BinaryVersion(db)
	if err != nil {
		return err
	}

	node, db = h.RandomDB(s.prng, s.crdbNodes)
	downgradeOption := bv.String()
	l.Printf("setting `preserve_downgrade_option` to %s (via node %d)", downgradeOption, node)
	_, err = db.ExecContext(ctx, "SET CLUSTER SETTING cluster.preserve_downgrade_option = $1", downgradeOption)
	return err
}

// restartWithNewBinaryStep restarts a certain `node` with a new
// cockroach binary. Any existing `cockroach` process will be stopped,
// then the new binary will be uploaded and the `cockroach` process
// will restart using the new binary.
type restartWithNewBinaryStep struct {
	id      int
	version string
	rt      test.Test
	node    int
}

func (s restartWithNewBinaryStep) ID() int                { return s.id }
func (s restartWithNewBinaryStep) Background() shouldStop { return nil }

func (s restartWithNewBinaryStep) Description() string {
	return fmt.Sprintf("restart node %d with binary version %s", s.node, versionMsg(s.version))
}

func (s restartWithNewBinaryStep) Run(
	ctx context.Context, l *logger.Logger, c cluster.Cluster, h *Helper,
) error {
	h.ExpectDeath()
	return clusterupgrade.RestartNodesWithNewBinary(
		ctx,
		s.rt,
		l,
		c,
		c.Node(s.node),
		// Disable regular backups in mixed-version tests, as some tests
		// check for running jobs and the scheduled backup may make
		// things non-deterministic. In the future, we should change the
		// default and add an API for tests to opt-out of the default
		// scheduled backup if necessary.
		option.DefaultStartOptsNoBackups(),
		s.version,
		defaultClusterSettings...,
	)
}

// finalizeUpgradeStep resets the `preserve_downgrade_option` cluster
// setting, allowing the upgrade migrations to run and the cluster
// version to eventually reach the binary version on the nodes.
type finalizeUpgradeStep struct {
	id        int
	crdbNodes option.NodeListOption
	prng      *rand.Rand
}

func (s finalizeUpgradeStep) ID() int                { return s.id }
func (s finalizeUpgradeStep) Background() shouldStop { return nil }

func (s finalizeUpgradeStep) Description() string {
	return "finalize upgrade by resetting `preserve_downgrade_option`"
}

func (s finalizeUpgradeStep) Run(
	ctx context.Context, l *logger.Logger, c cluster.Cluster, h *Helper,
) error {
	node, db := h.RandomDB(s.prng, s.crdbNodes)
	l.Printf("resetting preserve_downgrade_option (via node %d)", node)
	_, err := db.ExecContext(ctx, "RESET CLUSTER SETTING cluster.preserve_downgrade_option")
	return err
}

// runHookStep is a step used to run a user-provided hook (i.e.,
// callbacks passed to `OnStartup`, `InMixedVersion`, or `AfterTest`).
type runHookStep struct {
	id          int
	testContext Context
	prng        *rand.Rand
	hook        versionUpgradeHook
	stopChan    shouldStop
}

func (s runHookStep) ID() int                { return s.id }
func (s runHookStep) Background() shouldStop { return s.stopChan }

func (s runHookStep) Description() string {
	return fmt.Sprintf("run %q", s.hook.name)
}

func (s runHookStep) Run(
	ctx context.Context, l *logger.Logger, c cluster.Cluster, h *Helper,
) error {
	h.SetContext(&s.testContext)
	return s.hook.fn(ctx, l, s.prng, h)
}

// sequentialRunStep is a "meta-step" that indicates that a sequence
// of steps are to be executed sequentially. The default test runner
// already runs steps sequentially. This meta-step exists primarily as
// a way to group related steps so that a test plan is easier to
// understand for a human.
type sequentialRunStep struct {
	label string
	steps []testStep
}

func (s sequentialRunStep) Description() string {
	return s.label
}

// delayedStep is a thin wrapper around a test step that marks steps
// with a random delay that should be honored before the step starts
// executing. This is useful in steps that are to be executed
// concurrently to avoid biases of certain execution orders. While the
// Go scheduler is non-deterministic, in practice schedules are not
// uniformly distributed, and the delay is inteded to expose scenarios
// where a specific ordering of events leads to a failure.
type delayedStep struct {
	delay time.Duration
	step  testStep
}

// concurrentRunStep is a "meta-step" that groups multiple test steps
// that are meant to be executed concurrently. A random delay is added
// before each step starts, see notes on `delayedStep`.
type concurrentRunStep struct {
	label        string
	delayedSteps []testStep
}

func newConcurrentRunStep(label string, steps []testStep, rng *rand.Rand) concurrentRunStep {
	delayedSteps := make([]testStep, 0, len(steps))
	for _, step := range steps {
		delayedSteps = append(delayedSteps, delayedStep{delay: randomDelay(rng), step: step})
	}

	return concurrentRunStep{label: label, delayedSteps: delayedSteps}
}

func (s concurrentRunStep) Description() string {
	return fmt.Sprintf("%s concurrently", s.label)
}

// prefixedLogger returns a logger instance off of the given `l`
// parameter, and adds a prefix to everything logged by the retured
// logger.
func prefixedLogger(l *logger.Logger, prefix string) (*logger.Logger, error) {
	fileName := strings.ReplaceAll(prefix, " ", "-")
	formattedPrefix := fmt.Sprintf("[%s] ", fileName)
	return l.ChildLogger(fileName, logger.LogPrefix(formattedPrefix))
}

func (h hooks) Filter(testContext Context) hooks {
	var result hooks
	for _, hook := range h {
		if hook.predicate(testContext) {
			result = append(result, hook)
		}
	}

	return result
}

// AsSteps transforms the sequence of hooks into a corresponding test
// step. If there is only one hook, the corresponding `runHookStep` is
// returned. Otherwise, a `concurrentRunStep` is returned, where every
// hook is run concurrently. `stopChans` should either be `nil` for
// steps that are not meant to be run in the background, or contain
// one stop channel (`shouldStop`) for each hook.
func (h hooks) AsSteps(
	label string, idGen func() int, prng *rand.Rand, testContext Context, stopChans []shouldStop,
) []testStep {
	steps := make([]testStep, 0, len(h))
	stopChanFor := func(j int) shouldStop {
		if len(stopChans) == 0 {
			return nil
		}
		return stopChans[j]
	}

	for j, hook := range h {
		hookPrng := rngFromRNG(prng)
		steps = append(steps, runHookStep{
			id:          idGen(),
			prng:        hookPrng,
			hook:        hook,
			stopChan:    stopChanFor(j),
			testContext: testContext,
		})
	}

	if len(steps) <= 1 {
		return steps
	}

	return []testStep{newConcurrentRunStep(label, steps, prng)}
}

func (th *testHooks) AddStartup(hook versionUpgradeHook) {
	th.startup = append(th.startup, hook)
}

func (th *testHooks) AddBackground(hook versionUpgradeHook) {
	th.background = append(th.background, hook)
}

func (th *testHooks) AddMixedVersion(hook versionUpgradeHook) {
	th.mixedVersion = append(th.mixedVersion, hook)
}

func (th *testHooks) AddAfterUpgradeFinalized(hook versionUpgradeHook) {
	th.afterUpgradeFinalized = append(th.afterUpgradeFinalized, hook)
}

func (th *testHooks) StartupSteps(idGen func() int, testContext Context) []testStep {
	return th.startup.AsSteps(startupLabel, idGen, th.prng, testContext, nil)
}

func (th *testHooks) BackgroundSteps(
	idGen func() int, testContext Context, stopChans []shouldStop,
) []testStep {
	return th.background.AsSteps(backgroundLabel, idGen, th.prng, testContext, stopChans)
}

func (th *testHooks) MixedVersionSteps(testContext Context, idGen func() int) []testStep {
	return th.mixedVersion.
		Filter(testContext).
		AsSteps(mixedVersionLabel, idGen, th.prng, testContext, nil)
}

func (th *testHooks) AfterUpgradeFinalizedSteps(idGen func() int, testContext Context) []testStep {
	return th.afterUpgradeFinalized.AsSteps(afterTestLabel, idGen, th.prng, testContext, nil)
}

func randomDelay(rng *rand.Rand) time.Duration {
	idx := rng.Intn(len(possibleDelaysMs))
	return time.Duration(possibleDelaysMs[idx]) * time.Millisecond
}

func rngFromRNG(rng *rand.Rand) *rand.Rand {
	return rand.New(rand.NewSource(rng.Int63()))
}

func versionMsg(version string) string {
	return clusterupgrade.VersionMsg(version)
}

func assertValidTest(test *Test, fatalFunc func(...interface{})) {
	if test.options.useFixturesProbability > 0 && len(test.crdbNodes) != numNodesInFixtures {
		err := fmt.Errorf(
			"invalid cluster: use of fixtures requires %d cockroach nodes, got %d (%v)",
			numNodesInFixtures, len(test.crdbNodes), test.crdbNodes,
		)
		fatalFunc(errors.Wrap(err, "mixedversion.NewTest"))
	}

	if test.options.minUpgrades > test.options.maxUpgrades {
		err := fmt.Errorf(
			"invalid test options: maxUpgrades (%d) must be greater than minUpgrades (%d)",
			test.options.maxUpgrades, test.options.minUpgrades,
		)
		fatalFunc(errors.Wrap(err, "mixedversion.NewTest"))
	}
}
