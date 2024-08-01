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
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/testutils/release"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
)

const (
	logPrefix         = "mixed-version-test"
	startupLabel      = "run startup hooks"
	backgroundLabel   = "start background hooks"
	mixedVersionLabel = "run mixed-version hooks"
	afterTestLabel    = "run after test hooks"
	genericLabel      = "run following steps" // used by mutators to group steps

	unreachable = "internal error: unreachable"

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

	// rollbackFinalUpgradeProbability is the probability that we will
	// attempt to rollback the upgrade to the "current" version. We
	// should apply extra scrutiny to this upgrade which is why we
	// perform the rollback on most test runs.
	rollbackFinalUpgradeProbability = 0.9

	// numNodesInFixtures is the number of nodes expected to exist in a
	// cluster that can use the test fixtures in
	// `pkg/cmd/roachtest/fixtures`.
	numNodesInFixtures = 4

	// These `*Deployment` constants are used to indicate different
	// deployment modes that a test may choose to enable/disable.
	SystemOnlyDeployment    = DeploymentMode("system-only")
	SharedProcessDeployment = DeploymentMode("shared-process")
)

var (
	// possibleDelays lists the possible delays to be added to
	// concurrent steps.
	possibleDelays = []time.Duration{
		0,
		100 * time.Millisecond,
		500 * time.Millisecond,
		5 * time.Second,
		30 * time.Second,
		3 * time.Minute,
	}

	// defaultClusterSettings is the set of cluster settings always
	// passed to `clusterupgrade.StartWithSettings` when (re)starting
	// nodes in a cluster.
	defaultClusterSettings = []install.ClusterSettingOption{}

	// minSupportedARM64Version is the minimum version for which there
	// is a published ARM64 build. If we are running a mixedversion test
	// on ARM64, older versions will be skipped even if the test
	// requested a certain number of upgrades.
	minSupportedARM64Version = clusterupgrade.MustParseVersion("v22.2.0")

	allDeploymentModes = []DeploymentMode{
		SystemOnlyDeployment,
		SharedProcessDeployment,
	}

	// OldestSupportedVersion is the oldest cockroachdb version
	// officially supported. If we are performing upgrades from versions
	// older than this, we don't run user-provided hooks to avoid
	// creating flakes in unsupported versions or rediscovering
	// already-fixed bugs. This variable should be updated periodically
	// as releases reach end of life.
	//
	// Note that this is different from cockroach's MinSupportedVersion.
	// The version defined here has to do with our support window (see
	// [1]) and is branch agnostic, whereas MinSupportedVersion encodes
	// our backwards compatibility and is specific to each release
	// branch.
	//
	// [1] https://www.cockroachlabs.com/docs/releases/release-support-policy#current-supported-releases
	OldestSupportedVersion = clusterupgrade.MustParseVersion("v23.1.0")

	// OldestSupportedVersionVC is similar to `OldestSupportedVersion`,
	// but applies only to virtual cluster deployments. The reason it is
	// different for now is that v23.1.5 is the first version to ship
	// with the ability to change the default target cluster, a core
	// component of tests generated by the framework.
	//
	// TODO(testeng): remove this constant when 23.1 reaches EOL.
	OldestSupportedVersionVC = clusterupgrade.MustParseVersion("v23.1.5")

	// TenantsAndSystemAlignedSettingsVersion is the version in which
	// some work was done to align virtual clusters and the storage
	// cluster settings. This means that shared-process virtual clusters
	// get all capabilities by default, and some settings have their
	// values switched to enabled by default (such as the ability to
	// split and scatter tables).
	TenantsAndSystemAlignedSettingsVersion = clusterupgrade.MustParseVersion("v24.1.0")

	// tenantSettingsVersionOverrideFixVersion is the lowest version
	// after which bad data in the system.tenant_settings table is
	// supposed to be automatically deleted during the upgrade. See
	// #125702 for more details.
	tenantSettingsVersionOverrideFixVersion = clusterupgrade.MustParseVersion("v24.2.0-alpha.00000000")

	// tenantSupportsAutoUpgradeVersion is the minimum version after
	// which tenants start fully supporting auto upgrades. Prior to
	// this, resetting a cluster setting that impacts auto upgrades
	// would *not* make the tenant auto upgrade.
	tenantSupportsAutoUpgradeVersion = clusterupgrade.MustParseVersion("v24.2.0-alpha.00000000")

	// Catch divergences between `stepFunc` and `Run`'s signature in
	// `singleStepProtocol` at compile time.
	_ = func() stepFunc {
		var step singleStepProtocol
		return step.Run
	}
)

type (
	// stepFunc is the signature of functions that run in each
	// individual step in the test (user-provided or implemented by the
	// framework). These functions run at various points in the test
	// (synchronously or in the background) on the test runner node
	// itself; i.e., any commands they wish to execute on the cluster
	// need to go through the `cluster` methods as usual (cluster.RunE,
	// cluster.PutE, etc). These functions should prefer returning an
	// error over calling `t.Fatal` directly. The error is handled by
	// the mixedversion framework and better error messages are produced
	// as a result.
	stepFunc      func(context.Context, *logger.Logger, *rand.Rand, *Helper) error
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
		fn        stepFunc
	}

	// testStep is an opaque reference to one step of a mixed-version
	// test. It can be a singleStep (see below), or a "meta-step",
	// meaning that it combines other steps in some way (for instance, a
	// series of steps to be run sequentially or concurrently).
	testStep interface{}

	// singleStepProtocol is the set of functions that single step
	// implementations need to provide.
	singleStepProtocol interface {
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
		// Run implements the actual functionality of the step. This
		// signature should remain in sync with `stepFunc`.
		Run(context.Context, *logger.Logger, *rand.Rand, *Helper) error
	}

	// singleStep represents steps that implement the pieces on top of
	// which a mixed-version test is built. In other words, they are not
	// composed by other steps and hence can be directly executed.
	singleStep struct {
		context Context            // the context the step runs in
		rng     *rand.Rand         // the RNG to be used when running this step
		ID      int                // unique ID associated with the step
		impl    singleStepProtocol // the concrete implementation of the step
	}

	hooks []versionUpgradeHook

	// testHooks groups hooks associated with a mixed-version test in
	// its different stages: startup, mixed-version, and after-test.
	testHooks struct {
		startup               hooks
		background            hooks
		mixedVersion          hooks
		afterUpgradeFinalized hooks
		crdbNodes             option.NodeListOption
	}

	// testOptions contains some options that can be changed by the user
	// that expose some control over the generated test plan and behaviour.
	testOptions struct {
		useFixturesProbability         float64
		upgradeTimeout                 time.Duration
		minUpgrades                    int
		maxUpgrades                    int
		minimumSupportedVersion        *clusterupgrade.Version
		predecessorFunc                predecessorFunc
		skipVersionProbability         float64
		settings                       []install.ClusterSettingOption
		enabledDeploymentModes         []DeploymentMode
		tag                            string
		overriddenMutatorProbabilities map[string]float64
	}

	CustomOption func(*testOptions)

	predecessorFunc func(*rand.Rand, *clusterupgrade.Version, *clusterupgrade.Version) (*clusterupgrade.Version, error)

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

		// predecessorFunc computes the predecessor of a particular
		// release. By default, random predecessors are used, but tests
		// may choose to always use the latest predecessor as well.
		predecessorFunc predecessorFunc

		// the following are test-only fields, allowing tests to simulate
		// cluster properties without passing a cluster.Cluster
		// implementation.
		_arch    *vm.CPUArch
		_isLocal *bool
	}

	shouldStop chan struct{}

	// StopFunc is the signature of the function returned by calls that
	// create background steps. StopFuncs are meant to be called by test
	// authors when they want to stop a background step as part of test
	// logic itself, without causing the test to fail.
	StopFunc func()

	DeploymentMode string
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
func UpgradeTimeout(timeout time.Duration) CustomOption {
	return func(opts *testOptions) {
		opts.upgradeTimeout = timeout
	}
}

// MinUpgrades allows callers to set a minimum number of upgrades each
// test run should exercise.
func MinUpgrades(n int) CustomOption {
	return func(opts *testOptions) {
		opts.minUpgrades = n
	}
}

// MaxUpgrades allows callers to set a maximum number of upgrades to
// be performed during a test run.
func MaxUpgrades(n int) CustomOption {
	return func(opts *testOptions) {
		opts.maxUpgrades = n
	}
}

// NumUpgrades allows callers to specify the exact number of upgrades
// every test run should perform.
func NumUpgrades(n int) CustomOption {
	return func(opts *testOptions) {
		opts.minUpgrades = n
		opts.maxUpgrades = n
	}
}

// MinimumSupportedVersion allows tests to specify that the
// mixed-version hooks passed to the test are meant to be run only
// after the cluster is running at least the version `v` passed. For
// example, if `v` is v23.1.0, mixed-version hooks will only be
// scheduled when upgrading from a version in the 23.1 releases series
// or above.
func MinimumSupportedVersion(v string) CustomOption {
	return func(opts *testOptions) {
		opts.minimumSupportedVersion = clusterupgrade.MustParseVersion(v)
	}
}

// ClusterSettingOption adds a cluster setting option, in addition to the
// default set of options.
func ClusterSettingOption(opt ...install.ClusterSettingOption) CustomOption {
	return func(opts *testOptions) {
		opts.settings = append(opts.settings, opt...)
	}
}

// EnabledDeploymentModes restricts the test to only run in the
// provided deployment modes, in case the test is incompatible with a
// certain mode. Each test run will pick a random deployment mode
// within the list of enabled modes.
func EnabledDeploymentModes(modes ...DeploymentMode) CustomOption {
	return func(opts *testOptions) {
		opts.enabledDeploymentModes = modes
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
func AlwaysUseLatestPredecessors(opts *testOptions) {
	opts.predecessorFunc = latestPredecessor
}

// WithMutatorProbability allows tests to override the default
// probability that a mutator will be applied to a test plan.
func WithMutatorProbability(name string, probability float64) CustomOption {
	return func(opts *testOptions) {
		opts.overriddenMutatorProbabilities[name] = probability
	}
}

// DisableMutators disables all mutators with the names passed.
func DisableMutators(names ...string) CustomOption {
	return func(opts *testOptions) {
		for _, name := range names {
			WithMutatorProbability(name, 0)(opts)
		}
	}
}

// WithTag allows callers give the mixedversion test instance a
// `tag`. The tag is used as prefix in the log messages emitted by
// this upgrade test. This is only useful when running multiple
// concurrent mixedversion test runs in a single roachtest.
func WithTag(tag string) CustomOption {
	return func(opts *testOptions) {
		opts.tag = tag
	}
}

// minSupportedSkipVersionUpgrade is the minimum version after which
// "skip version" upgrades are supported (i.e., upgrading two major
// releases in a single upgrade).
var minSupportedSkipVersionUpgrade = clusterupgrade.MustParseVersion("v24.1.0")

func defaultTestOptions() testOptions {
	return testOptions{
		// We use fixtures more often than not as they are more likely to
		// detect bugs, especially in migrations.
		useFixturesProbability:         0.7,
		upgradeTimeout:                 clusterupgrade.DefaultUpgradeTimeout,
		minUpgrades:                    1,
		maxUpgrades:                    4,
		minimumSupportedVersion:        OldestSupportedVersion,
		predecessorFunc:                randomPredecessor,
		enabledDeploymentModes:         allDeploymentModes,
		skipVersionProbability:         0.5,
		overriddenMutatorProbabilities: make(map[string]float64),
	}
}

// DisableSkipVersionUpgrades can be used by callers to disable the
// experimental "skip version" upgrade testing. Useful if a test is
// verifying something specific to an upgrade path from the previous
// release to the current one.
func DisableSkipVersionUpgrades(opts *testOptions) {
	withSkipVersionProbability(0)(opts)
}

// withSkipVersionProbability allows callers to set the specific
// probability under which skip-version upgrades will happen in a test
// run. Only used for testing at the moment.
func withSkipVersionProbability(p float64) CustomOption {
	return func(opts *testOptions) {
		opts.skipVersionProbability = p
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
	options ...CustomOption,
) *Test {
	opts := defaultTestOptions()
	for _, fn := range options {
		fn(&opts)
	}

	testLogger, err := prefixedLogger(l, filepath.Join(opts.tag, logPrefix))
	if err != nil {
		t.Fatal(err)
	}

	prng, seed := randutil.NewLockedPseudoRand()
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
		hooks:           &testHooks{crdbNodes: crdbNodes},
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
// InMixedVersion hooks are passed, they may be executed
// concurrently.
func (t *Test) InMixedVersion(desc string, fn stepFunc) {
	var prevUpgradeStage UpgradeStage
	var numUpgradedNodes int
	predicate := func(testContext Context) bool {
		// If the cluster is finalizing an upgrade, run this hook
		// according to the probability defined in the package.
		if testContext.Finalizing() {
			return t.prng.Float64() < runWhileMigratingProbability
		}

		// This check makes sure we only schedule a mixed-version hook
		// once while upgrading from one version to another. The number of
		// nodes we wait to be running the new version is determined when
		// the version changes for the first time.
		if testContext.DefaultService().Stage != prevUpgradeStage {
			prevUpgradeStage = testContext.DefaultService().Stage
			numUpgradedNodes = t.prng.Intn(len(t.crdbNodes)) + 1
		}

		return len(testContext.NodesInNextVersion()) == numUpgradedNodes
	}

	t.hooks.AddMixedVersion(versionUpgradeHook{name: desc, predicate: predicate, fn: fn})
}

// OnStartup registers a callback that is run once the cluster is
// initialized (i.e., all nodes in the cluster start running CRDB at a
// certain previous version, potentially from existing fixtures). If
// multiple OnStartup hooks are passed, they will be executed
// concurrently.
func (t *Test) OnStartup(desc string, fn stepFunc) {
	// Since the callbacks here are only referenced in the setup steps
	// of the planner, there is no need to have a predicate function
	// gating them.
	t.hooks.AddStartup(versionUpgradeHook{name: desc, fn: fn})
}

// AfterUpgradeFinalized registers a callback that is run once the
// mixed-version test has brought the cluster to the latest version,
// and allowed the upgrade to finalize successfully. If multiple such
// hooks are passed, they will be executed concurrently.
func (t *Test) AfterUpgradeFinalized(desc string, fn stepFunc) {
	t.hooks.AddAfterUpgradeFinalized(versionUpgradeHook{name: desc, fn: fn})
}

// BackgroundFunc runs the function passed as argument in the
// background during the test. Background functions are kicked off
// once the cluster has been initialized (i.e., after all startup
// steps have finished). If the `stepFunc` returns an error, it will
// cause the test to fail. These functions can run indefinitely but
// should respect the context passed to them, which will be canceled
// when the test terminates (successfully or not). Returns a function
// that can be called to terminate the step, which will cancel the
// context passed to `stepFunc`.
func (t *Test) BackgroundFunc(desc string, fn stepFunc) StopFunc {
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
		t.rt.Fatal(err)
	}

	t.logger.Printf("mixed-version test:\n%s", plan.PrettyPrint())

	if err := t.run(plan); err != nil {
		t.rt.Fatal(err)
	}
}

func (t *Test) run(plan *TestPlan) error {
	return newTestRunner(t.ctx, t.cancel, plan, t.options.tag, t.logger, t.cluster).run()
}

func (t *Test) plan() (plan *TestPlan, retErr error) {
	defer func() {
		if retErr != nil {
			// Planning failures are always internal framework errors, so
			// they should be sent to TestEng.
			retErr = registry.ErrorWithOwner(
				registry.OwnerTestEng,
				fmt.Errorf("error creating test plan: %w", retErr),
			)
		}
	}()

	// Pick a random deployment mode to use in this test run among the
	// list of enabled deployment modes enabled for this test.
	deploymentMode := t.options.enabledDeploymentModes[t.prng.Intn(len(t.options.enabledDeploymentModes))]
	t.updateOptionsForDeploymentMode(deploymentMode)

	previousReleases, err := t.choosePreviousReleases()
	if err != nil {
		return nil, err
	}

	tenantDescriptor := t.tenantDescriptor(deploymentMode)
	initialRelease := previousReleases[0]

	planner := testPlanner{
		versions:       append(previousReleases, clusterupgrade.CurrentVersion()),
		deploymentMode: deploymentMode,
		seed:           t.seed,
		currentContext: newInitialContext(initialRelease, t.crdbNodes, tenantDescriptor),
		options:        t.options,
		rt:             t.rt,
		isLocal:        t.isLocal(),
		hooks:          t.hooks,
		prng:           t.prng,
		bgChans:        t.bgChans,
	}

	return planner.Plan(), nil
}

func (t *Test) clusterArch() vm.CPUArch {
	if t._arch != nil {
		return *t._arch // test-only
	}

	return t.cluster.Architecture()
}

func (t *Test) isLocal() bool {
	if t._isLocal != nil {
		return *t._isLocal
	}

	return t.cluster.IsLocal()
}

func (t *Test) runCommandFunc(nodes option.NodeListOption, cmd string) stepFunc {
	return func(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *Helper) error {
		l.Printf("running command `%s` on nodes %v", cmd, nodes)
		return t.cluster.RunE(ctx, option.WithNodes(nodes), cmd)
	}
}

// choosePreviousReleases returns a list of predecessor releases
// relative to the current build version. It uses the `predecessorFunc`
// field to compute the actual list of predecessors. This function
// may also choose to skip releases when supported. Special care is
// taken to avoid using releases that are not available under a
// certain cluster architecture. Specifically, ARM64 builds are
// only available on v22.2.0+.
func (t *Test) choosePreviousReleases() ([]*clusterupgrade.Version, error) {
	skipVersions := t.prng.Float64() < t.options.skipVersionProbability
	isAvailable := func(v *clusterupgrade.Version) bool {
		if t.clusterArch() != vm.ArchARM64 {
			return true
		}

		return v.AtLeast(minSupportedARM64Version)
	}

	var numSkips int
	// possiblePredecessorsFor returns a list of possible predecessors
	// for the given release `v`. If skip-version is enabled and
	// supported, this function will return both the immediate
	// predecessor along with the predecessor's predecessor. This is the
	// function to change in case the rules around what upgrades are
	// possible in CRDB change.
	possiblePredecessorsFor := func(v *clusterupgrade.Version) ([]*clusterupgrade.Version, error) {
		pred, err := t.predecessorFunc(t.prng, v, t.options.minimumSupportedVersion)
		if err != nil {
			return nil, err
		}

		if !isAvailable(pred) {
			return nil, nil
		}

		// If skip-version upgrades are not enabled, the only possible
		// predecessor is the immediate predecessor release. If the
		// predecessor doesn't support skip versions, then its predecessor
		// won't either. Don't attempt to find it.
		if !skipVersions || !pred.AtLeast(minSupportedSkipVersionUpgrade) {
			return []*clusterupgrade.Version{pred}, nil
		}

		predPred, err := t.predecessorFunc(t.prng, pred, t.options.minimumSupportedVersion)
		if err != nil {
			return nil, err
		}

		if predPred.AtLeast(minSupportedSkipVersionUpgrade) {
			// If the predecessor's predecessor supports skip-version
			// upgrades and we haven't performed a skip-version upgrade yet,
			// do it. This logic makes sure that, when skip-version upgrades
			// are enabled, it happens when upgrading to the current
			// release, which is the most important upgrade to be tested on
			// any release branch.
			if numSkips == 0 {
				numSkips++
				return []*clusterupgrade.Version{predPred}, nil
			}

			// If we already performed a skip-version upgrade on this test
			// plan, we can choose to do another one or not.
			return []*clusterupgrade.Version{pred, predPred}, nil
		}

		// If the predecessor is too old and does not support skip-version
		// upgrades, it's the only possible predecessor.
		return []*clusterupgrade.Version{pred}, nil
	}

	currentVersion := clusterupgrade.CurrentVersion()
	var upgradePath []*clusterupgrade.Version
	numUpgrades := t.numUpgrades()

	for j := 0; j < numUpgrades; j++ {
		predecessors, err := possiblePredecessorsFor(currentVersion)
		if err != nil {
			return nil, err
		}

		// If there are no valid predecessors, it means some release is
		// not available for the cluster architecture. We log a warning
		// below in case we have a shorter upgrade path than requested
		// because of this.
		if len(predecessors) == 0 {
			break
		}

		chosenPredecessor := predecessors[t.prng.Intn(len(predecessors))]
		upgradePath = append(upgradePath, chosenPredecessor)
		currentVersion = chosenPredecessor
	}

	if len(upgradePath) < numUpgrades {
		t.logger.Printf("WARNING: skipping upgrades as ARM64 is only supported on %s+", minSupportedARM64Version)
	}

	// The upgrade path to be returned is from oldest to newest release.
	slices.Reverse(upgradePath)
	return upgradePath, nil
}

// numUpgrades returns the number of upgrades that will be performed
// in this test run. Returns a number in the [minUpgrades, maxUpgrades]
// range.
func (t *Test) numUpgrades() int {
	return t.prng.Intn(
		t.options.maxUpgrades-t.options.minUpgrades+1,
	) + t.options.minUpgrades
}

// latestPredecessor is an implementation of `predecessorFunc` that
// always picks the latest predecessor for the given release version,
// ignoring the minimum supported version declared by the test.
func latestPredecessor(
	_ *rand.Rand, v, minSupported *clusterupgrade.Version,
) (*clusterupgrade.Version, error) {
	predecessor, err := release.LatestPredecessor(&v.Version)
	if err != nil {
		return nil, err
	}

	return clusterupgrade.MustParseVersion(predecessor), nil
}

// randomPredecessor is an implementation of `predecessorFunc` that
// picks a random predecessor for the given release version. If we are
// choosing a predecessor in the same series as the minimum supported
// version, special care is taken to select a random predecessor that
// is more recent that the minimum supported version.
func randomPredecessor(
	rng *rand.Rand, v, minSupported *clusterupgrade.Version,
) (*clusterupgrade.Version, error) {
	predecessor, err := release.RandomPredecessor(rng, &v.Version)
	if err != nil {
		return nil, err
	}

	// If the minimum supported version is from a different release
	// series, we can pick any random patch release.
	predV := clusterupgrade.MustParseVersion(predecessor)
	if predV.Series() != minSupported.Series() {
		return predV, nil
	}

	// If the latest release of a series is a pre-release, we validate
	// whether the minimum supported version is valid.
	if predV.PreRelease() != "" && !predV.AtLeast(minSupported) {
		return nil, fmt.Errorf(
			"latest release for %s (%s) is not sufficient for minimum supported version (%s)",
			predV.Series(), predV, minSupported.Version,
		)
	}

	// If the patch version of `minSupporrted` is 0, it means that we
	// can choose any patch release in the predecessor series. It is
	// also safe to return `predV` here if the `minSupported` version is
	// a pre-release: we already validated that `predV`is at least
	// `minSupported` in check above.
	if minSupported.Patch() == 0 {
		return predV, nil
	}

	latestPred, err := latestPredecessor(rng, v, minSupported)
	if err != nil {
		return nil, err
	}

	var supportedPatchReleases []*clusterupgrade.Version
	for j := minSupported.Patch(); j <= latestPred.Patch(); j++ {
		supportedV := clusterupgrade.MustParseVersion(
			fmt.Sprintf("v%d.%d.%d", predV.Major(), predV.Minor(), j),
		)

		isWithdrawn, err := release.IsWithdrawn(&supportedV.Version)
		if err != nil {
			return nil, err
		}

		if !isWithdrawn {
			supportedPatchReleases = append(supportedPatchReleases, supportedV)
		}
	}

	return supportedPatchReleases[rng.Intn(len(supportedPatchReleases))], nil
}

func (t *Test) updateOptionsForDeploymentMode(mode DeploymentMode) {
	if mode == SystemOnlyDeployment {
		return
	}

	// Make sure to enforce the minimum supported version for virtual
	// clusters.
	if v := OldestSupportedVersionVC; !t.options.minimumSupportedVersion.AtLeast(v) {
		t.options.minimumSupportedVersion = v
	}
}

func (t *Test) tenantDescriptor(deploymentMode DeploymentMode) *ServiceDescriptor {
	switch deploymentMode {
	case SystemOnlyDeployment:
		return nil

	case SharedProcessDeployment:
		return &ServiceDescriptor{
			Name:  virtualClusterName(t.prng),
			Nodes: t.crdbNodes,
		}
	}

	panic(unreachable)
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
	rng          *rand.Rand
	delayedSteps []testStep
}

func newConcurrentRunStep(
	label string, steps []testStep, rng *rand.Rand, isLocal bool,
) concurrentRunStep {
	delayedSteps := make([]testStep, 0, len(steps))
	for _, step := range steps {
		delayedSteps = append(delayedSteps, delayedStep{
			delay: randomConcurrencyDelay(rng, isLocal), step: step,
		})
	}

	return concurrentRunStep{label: label, delayedSteps: delayedSteps, rng: rng}
}

func (s concurrentRunStep) Description() string {
	return fmt.Sprintf("%s concurrently", s.label)
}

// newSingleStep creates a `singleStep` struct for the implementation
// passed, making sure to copy the context so that any modifications
// made to it do not affect this step's view of the context.
func newSingleStep(context *Context, impl singleStepProtocol, rng *rand.Rand) *singleStep {
	return &singleStep{context: context.clone(), impl: impl, rng: rng}
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
	label string, prng *rand.Rand, testContext *Context, stopChans []shouldStop, isLocal bool,
) []testStep {
	steps := make([]testStep, 0, len(h))
	stopChanFor := func(j int) shouldStop {
		if len(stopChans) == 0 {
			return nil
		}
		return stopChans[j]
	}

	for j, hook := range h {
		steps = append(steps, newSingleStep(testContext, runHookStep{
			hook:     hook,
			stopChan: stopChanFor(j),
		}, rngFromRNG(prng)))
	}

	if len(steps) <= 1 {
		return steps
	}

	return []testStep{newConcurrentRunStep(label, steps, prng, isLocal)}
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

func (th *testHooks) StartupSteps(testContext *Context, rng *rand.Rand, isLocal bool) []testStep {
	return th.startup.AsSteps(startupLabel, rng, testContext, nil, isLocal)
}

func (th *testHooks) BackgroundSteps(
	testContext *Context, stopChans []shouldStop, rng *rand.Rand, isLocal bool,
) []testStep {
	testContext.SetStage(BackgroundStage)
	return th.background.AsSteps(backgroundLabel, rng, testContext, stopChans, isLocal)
}

func (th *testHooks) MixedVersionSteps(
	testContext *Context, rng *rand.Rand, isLocal bool,
) []testStep {
	return th.mixedVersion.
		Filter(*testContext).
		AsSteps(mixedVersionLabel, rng, testContext, nil, isLocal)
}

func (th *testHooks) AfterUpgradeFinalizedSteps(
	testContext *Context, rng *rand.Rand, isLocal bool,
) []testStep {
	return th.afterUpgradeFinalized.AsSteps(afterTestLabel, rng, testContext, nil, isLocal)
}

// pickRandomDelay chooses a random duration from the list passed,
// reducing it in local runs, as some tests run as part of CI and we
// don't want to spend too much time waiting in that context.
func pickRandomDelay(rng *rand.Rand, isLocal bool, durations []time.Duration) time.Duration {
	dur := durations[rng.Intn(len(durations))]
	if isLocal {
		dur = dur / 10
	}

	return dur
}

func randomConcurrencyDelay(rng *rand.Rand, isLocal bool) time.Duration {
	return pickRandomDelay(rng, isLocal, possibleDelays)
}

func rngFromRNG(rng *rand.Rand) *rand.Rand {
	return rand.New(rand.NewSource(rng.Int63()))
}

// virtualClusterName returns a random name that can be used as a
// virtual cluster name in a test.
func virtualClusterName(rng *rand.Rand) string {
	return strings.ToLower(
		fmt.Sprintf(
			"mixed-version-tenant-%s",
			randutil.RandString(rng, 5, randutil.PrintableKeyAlphabet),
		),
	)
}

func assertValidTest(test *Test, fatalFunc func(...interface{})) {
	fail := func(err error) {
		fatalFunc(errors.Wrap(err, "mixedversion.NewTest"))
	}

	if test.options.useFixturesProbability > 0 && len(test.crdbNodes) != numNodesInFixtures {
		fail(
			fmt.Errorf(
				"invalid cluster: use of fixtures requires %d cockroach nodes, got %d (%v)",
				numNodesInFixtures, len(test.crdbNodes), test.crdbNodes,
			),
		)
	}

	if test.options.minUpgrades > test.options.maxUpgrades {
		fail(
			fmt.Errorf(
				"invalid test options: maxUpgrades (%d) must be greater than minUpgrades (%d)",
				test.options.maxUpgrades, test.options.minUpgrades,
			),
		)
	}

	currentVersion := clusterupgrade.CurrentVersion()
	msv := test.options.minimumSupportedVersion
	// The minimum supported version should be from an older major
	// version or, if from the same major version, from an older minor
	// version.
	validVersion := msv.Major() < currentVersion.Major() ||
		(msv.Major() == currentVersion.Major() && msv.Minor() < currentVersion.Minor())

	if !validVersion {
		fail(
			fmt.Errorf(
				"invalid test options: minimum supported version (%s) should be from an older release series than current version (%s)",
				msv.Version.String(), currentVersion.Version.String(),
			),
		)
	}

	if len(test.options.enabledDeploymentModes) == 0 {
		fail(fmt.Errorf("invalid test options: no deployment modes enabled"))
	}

	validDeploymentMode := func(mode DeploymentMode) bool {
		for _, deploymentMode := range allDeploymentModes {
			if mode == deploymentMode {
				return true
			}
		}

		return false
	}

	// Validate that every possible deployment mode for this test is
	// valid (i.e., part of the `allDeploymentModes` list). This stops
	// us from having to implement a `default` branch with an error when
	// switching on deployment mode.
	for _, dm := range test.options.enabledDeploymentModes {
		if !validDeploymentMode(dm) {
			fail(fmt.Errorf("invalid test options: unknown deployment mode %q", dm))
		}
	}
}
