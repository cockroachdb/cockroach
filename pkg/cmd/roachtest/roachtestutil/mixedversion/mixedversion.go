// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/failureinjection/failures"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/testutils/release"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/version"
)

const (
	logPrefix                  = "mixed-version-test"
	beforeClusterStartLabel    = "run before cluster start hooks"
	startupLabel               = "run startup hooks"
	backgroundLabel            = "start background hooks"
	mixedVersionLabel          = "run mixed-version hooks"
	afterTestLabel             = "run after test hooks"
	upgradeStorageClusterLabel = "upgrade storage cluster"
	upgradeTenantLabel         = "upgrade tenant"
	genericLabel               = "run following steps" // used by mutators to group steps

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
	SystemOnlyDeployment      = DeploymentMode("system-only")
	SharedProcessDeployment   = DeploymentMode("shared-process")
	SeparateProcessDeployment = DeploymentMode("separate-process")
)

// These env vars are used by the planner to generate plans with
// certain specs regardless of the seed. This can be useful for
// forcing certain plans to be generated for debugging without needing
// trial and error.
const (
	// deploymentModeOverrideEnv overrides the deployment mode used.
	// 	- MVT_DEPLOYMENT_MODE=system
	deploymentModeOverrideEnv = "MVT_DEPLOYMENT_MODE"

	// upgradePathOverrideEnv is parsed to override the upgrade path used.
	// Specifying a release series uses the latest patch release.
	// 	- MVT_UPGRADE_PATH=24.1.5,24.2.0,current
	// 	- MVT_UPGRADE_PATH=24.1,24.2,current
	upgradePathOverrideEnv = "MVT_UPGRADE_PATH"

	// Dump the test plan and return, i.e., skipping the actual execution of the test.
	dryRunEnv = "MVT_DRY_RUN"
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

	// MinBootstrapSupportedVersion is the _minimum_ version for any supported upgrade path.
	// For practical reasons, testing upgrade paths from even older versions is a point of diminishing returns.
	MinBootstrapSupportedVersion = clusterupgrade.MustParseVersion("v22.2.0")

	// MinBootstrapSupportedVersionS390x is the _minimum_ version for any supported upgrade path running on S390x.
	MinBootstrapSupportedVersionS390x = clusterupgrade.MustParseVersion("v25.3.0")

	allDeploymentModes = []DeploymentMode{
		SystemOnlyDeployment,
		SharedProcessDeployment,
		SeparateProcessDeployment,
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

	// OldestSupportedVersionSP is similar to `OldestSupportedVersion`,
	// but applies only to shared-process virtual cluster deployments.
	// The reason it is different for now is that v23.1.5 is the first
	// version to ship with the ability to change the default target
	// cluster, a core component of tests generated by the framework.
	//
	// TODO(testeng): remove this constant when 23.1 reaches EOL.
	OldestSupportedVersionSP = clusterupgrade.MustParseVersion("v23.1.5")

	// TenantsAndSystemAlignedSettingsVersion is the version in which
	// some work was done to align shared-process virtual clusters and
	// the storage cluster settings. This means that shared-process
	// virtual clusters get all capabilities by default, and some
	// settings have their values switched to enabled by default (such
	// as the ability to split and scatter tables).
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

	// updateTenantResourceLimitsDeprecatedArgsVersion is the lowest version
	// after which the "as_of" and "as_of_consumed_tokens" arguments were removed when
	// using the `tenant_name` overload.
	updateTenantResourceLimitsDeprecatedArgsVersion = clusterupgrade.MustParseVersion("v24.2.1")

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
		id        string // unique and non-empty ID of the hook, generated by the framework
		name      string // user-specified name or description, not necessarily unique or non-empty
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
		Description(debug bool) string
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
		// ConcurrencyDisabled returns true if the step should not be run
		// concurrently with other steps. This is the case for any steps
		// that involve restarting a node, as they may attempt to connect
		// to an unavailable node.
		ConcurrencyDisabled() bool
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
		beforeClusterStart    hooks
		startup               hooks
		background            hooks
		mixedVersion          hooks
		afterUpgradeFinalized hooks
		crdbNodes             option.NodeListOption
	}

	// testOptions contains some options that can be changed by the user
	// that expose some control over the generated test plan and behaviour.
	testOptions struct {
		useFixturesProbability  float64
		upgradeTimeout          time.Duration
		maxNumPlanSteps         int
		minUpgrades             int
		maxUpgrades             int
		minimumSupportedVersion *clusterupgrade.Version
		// N.B. If unset, then there is no minimum bootstrap version enforced.
		// We do this over, e.g. setting it to the oldest version we have release data
		// for, because unit tests can use fake release data much older than that.
		minimumBootstrapVersion *clusterupgrade.Version
		// predecessorFunc computes the predecessor of a particular
		// release. By default, random predecessors are used, but tests
		// may choose to always use the latest predecessor as well.
		predecessorFunc                predecessorFunc
		useLatestPredecessors          bool
		waitForReplication             bool
		skipVersionProbability         float64
		sameSeriesUpgradeProbability   float64
		settings                       []install.ClusterSettingOption
		enabledDeploymentModes         []DeploymentMode
		tag                            string
		overriddenMutatorProbabilities map[string]float64
		hooksSupportFailureInjection   bool
		workloadNodes                  option.NodeListOption
		enableUpReplication            bool
	}

	CustomOption func(*testOptions)

	predecessorFunc func(*rand.Rand, *clusterupgrade.Version) (*clusterupgrade.Version, error)

	// Test is the main struct callers of this package interact with.
	Test struct {
		ctx       context.Context
		cancel    context.CancelFunc
		cluster   cluster.Cluster
		logger    *logger.Logger
		crdbNodes option.NodeListOption

		options testOptions

		rt      test.Test
		prng    *rand.Rand
		seed    int64
		hooks   *testHooks
		hookIds map[string]bool

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

		// The following are test-only fields, e.g., allowing tests to simulate
		// cluster properties without passing a cluster.Cluster
		// implementation.
		_arch      *vm.CPUArch
		_isLocal   *bool
		_getFailer func(name string) (*failures.Failer, error)
	}

	shouldStop chan struct{}

	// StopFunc is the signature of the function returned by calls that
	// create background steps. StopFuncs are meant to be called by test
	// authors when they want to stop a background step as part of test
	// logic itself, without causing the test to fail.
	StopFunc func()

	DeploymentMode string
)

// EnableHooksDuringFailureInjection is an option that can be passed to
// `NewTest` to enable the use of mixed-version hooks during failure injections.
func EnableHooksDuringFailureInjection(opts *testOptions) {
	opts.hooksSupportFailureInjection = true
}

// EnableUpReplication is an option that can be passed to `NewTest` to enable
// up-replication to 5X before panicking a node during failure injection tests.
func EnableUpReplication(opts *testOptions) {
	opts.enableUpReplication = true
}

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

// MaxNumPlanSteps allows callers to set a maximum number of steps to
// be performed during a test run.
func MaxNumPlanSteps(n int) CustomOption {
	return func(opts *testOptions) {
		opts.maxNumPlanSteps = n
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

// MinimumBootstrapVersion allows tests to specify that the
// cluster created should only be bootstrapped on a version
// `v` or above. May override MaxUpgrades if the minimum bootstrap
// version does not support that many upgrades.
func MinimumBootstrapVersion(v string) CustomOption {
	return func(opts *testOptions) {
		opts.minimumBootstrapVersion = clusterupgrade.MustParseVersion(v)
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
	opts.useLatestPredecessors = true
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

// DisableAllMutators will disable all available mutators.
func DisableAllMutators() CustomOption {
	return func(opts *testOptions) {
		names := []string{}
		for _, m := range planMutators {
			names = append(names, m.Name())
		}
		DisableMutators(names...)(opts)
	}
}

// DisableAllClusterSettingMutators will disable all available cluster setting mutators.
func DisableAllClusterSettingMutators() CustomOption {
	return func(opts *testOptions) {
		names := []string{}
		for _, m := range clusterSettingMutators {
			names = append(names, m.Name())
		}
		DisableMutators(names...)(opts)
	}
}

// DisableAllFailureInjectionMutators will disable all available failure injection mutators.
func DisableAllFailureInjectionMutators() CustomOption {
	return func(opts *testOptions) {
		names := []string{}
		for _, m := range failureInjectionMutators {
			names = append(names, m.Name())
		}
		DisableMutators(names...)(opts)
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

// WithWorkloadNodes tells the mixedversion framework that this test's cluster
// includes workload node(s) so the framework can stage all the cockroach
// binaries included in the upgrade plan on the workload node so the test can
// use versioned workload commands without the test itself having to stage
// those binaries.
func WithWorkloadNodes(nodes option.NodeListOption) CustomOption {
	return func(opts *testOptions) {
		opts.workloadNodes = nodes
	}
}

// supportsSkipUpgradeTo returns true if the given version supports skipping the
// previous major version during upgrade. For example, 24.3 supports upgrade
// directly from 24.1, but 25.1 only supports upgrade from 24.3.
//
// Checks whether a skip upgrade step, pred->v, is feasible,
// - pred is not of the same release series as msv (prevents skipping of user-specified hooks)
// - v supports skip upgrades
// - there are multiple supported previous versions to skip over
func (t *Test) supportsSkipUpgradeTo(pred, v *clusterupgrade.Version) bool {
	if t.options.minimumSupportedVersion.Series() == pred.Series() {
		return false
	}

	// If there's only one supported previous version for the current version, we can't skip.
	// This happens during the version bump process when MinSupported == PreviousRelease.
	r := clusterversion.Latest.ReleaseSeries()
	currentMajor := version.MajorVersion{Year: int(r.Major), Ordinal: int(r.Minor)}
	if currentMajor.Equals(v.Version.Major()) && len(clusterversion.SupportedPreviousReleases()) <= 1 {
		return false
	}

	series := v.Version.Major()
	switch {
	case series.Year < 24:
		return false
	case series.Year == 24:
		// v24.3 is the first version which officially supports the skip upgrade.
		return series.Ordinal == 3
	default:
		// The current plan for 2025+ is for .1 and .3 to be skippable innovation
		// releases and thus allow skip upgrades to 25.2 and 25.4.
		return series.Ordinal == 2 || series.Ordinal == 4
	}
}

func defaultTestOptions() testOptions {
	return testOptions{
		// We use fixtures more often than not as they are more likely to
		// detect bugs, especially in migrations.
		useFixturesProbability: 0.7,
		upgradeTimeout:         clusterupgrade.DefaultUpgradeTimeout,
		// N.B. The default is unlimited since a priori we don't know anything
		// about the test plan.
		maxNumPlanSteps:         math.MaxInt,
		minUpgrades:             1,
		maxUpgrades:             4,
		minimumSupportedVersion: OldestSupportedVersion,
		// We've seen tests flake due to overload right after restarting a
		// node (#130384). Waiting for 3X replication between each node restart
		// appears to help, but we should be cautious of tests that create a lot
		// of ranges as this may add significant delay.
		waitForReplication:             true,
		enabledDeploymentModes:         allDeploymentModes,
		skipVersionProbability:         0.5,
		sameSeriesUpgradeProbability:   0,
		overriddenMutatorProbabilities: make(map[string]float64),
	}
}

// DisableSkipVersionUpgrades can be used by callers to disable "skip
// version" upgrades. Useful if a test is verifying something specific
// to an upgrade path from the previous release to the current one;
// otherwise, this option should not be used, as every feature should
// work when skip-version upgrades are performed.
func DisableSkipVersionUpgrades(opts *testOptions) {
	WithSkipVersionProbability(0)(opts)
}

// WithSkipVersionProbability allows callers to set the specific
// probability under which skip-version upgrades will happen in a test
// run.
func WithSkipVersionProbability(p float64) CustomOption {
	return func(opts *testOptions) {
		opts.skipVersionProbability = p
	}
}

// WithSameSeriesUpgradeProbability allows callers to set the probability
// that same-series upgrades (e.g., 24.3.5 -> 24.3.12) are enabled for a
// given test run. When enabled, the number of same-series insertions is
// chosen randomly (bounded by maxUpgrades). Same-series upgrades test
// patch-level upgrades within the same major.minor release series.
func WithSameSeriesUpgradeProbability(p float64) CustomOption {
	return func(opts *testOptions) {
		opts.sameSeriesUpgradeProbability = p
	}
}

// DisableSameSeriesUpgrades can be used by callers to disable
// same-series upgrades. Useful if a test is verifying something
// specific to cross-series upgrades only.
func DisableSameSeriesUpgrades(opts *testOptions) {
	WithSameSeriesUpgradeProbability(0)(opts)
}

// DisableWaitForReplication disables the wait for 3x replication
// after each node restart in a mixedversion test.
func DisableWaitForReplication(opts *testOptions) {
	opts.waitForReplication = false
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
	if !t.Spec().(*registry.TestSpec).Monitor {
		t.Fatal("mixedversion tests require enabling the global test monitor in the test spec")
	}

	opts := defaultTestOptions()
	for _, fn := range options {
		fn(&opts)
	}
	opts.enabledDeploymentModes = validDeploymentModesForCloud(c.Cloud(), opts.enabledDeploymentModes)

	testLogger, err := prefixedLogger(l, filepath.Join(opts.tag, logPrefix))
	if err != nil {
		t.Fatal(err)
	}

	prng, seed := randutil.NewLockedPseudoRand()
	testCtx, cancel := context.WithCancel(ctx)

	test := &Test{
		ctx:       testCtx,
		cancel:    cancel,
		cluster:   c,
		logger:    testLogger,
		crdbNodes: crdbNodes,
		options:   opts,
		rt:        t,
		prng:      prng,
		seed:      seed,
		hooks:     &testHooks{crdbNodes: crdbNodes},
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
// will be tested in arbitrary mixed-version states; specifically, it
// can be called up to four times during each major upgrade
// performed:
//
// 1. when the cluster upgrades to the new binary (`preserve_downgrade_option` set)
// 2. when the cluster downgrades to the old binary
// 3. when the cluster upgrades to the new binary again
// 4. when the cluster is finalizing
//
// Note that not every major upgrade performs a downgrade. In those
// cases, the InMixedVersion hook would only be called up to two times
// (when the cluster upgrades to the new binary, and when the cluster
// is finalizing the upgrade). Callers can use `h.Context().Stage` to
// find out the stage in the upgrade in which the hook is being
// called.
//
// If multiple InMixedVersion hooks are passed, they may be executed
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

		upgradingService := testContext.DefaultService()
		if upgradingService.Stage == UpgradingSystemStage {
			// This condition represents the situation where we are
			// upgrading the storage cluster in a separate-process
			// deployment. In that case, we want to look at the storage
			// cluster when checking the number of nodes in the
			// previous/next versions.
			upgradingService = testContext.System
		}

		// This check makes sure we only schedule a mixed-version hook
		// once while upgrading (or downgrading) from one version to
		// another. The number of nodes we wait to be running the new
		// version is determined when the version changes for the first
		// time.
		if upgradingService.Stage != prevUpgradeStage {
			prevUpgradeStage = upgradingService.Stage
			numUpgradedNodes = t.prng.Intn(len(t.crdbNodes)) + 1
		}

		return len(upgradingService.NodesInNextVersion()) == numUpgradedNodes
	}

	t.hooks.AddMixedVersion(versionUpgradeHook{id: genId(t), name: desc, predicate: predicate, fn: fn})
}

// BeforeClusterStart registers a callback that is run before cluster
// initialization. In the case of multitenant deployments, hooks
// will be run for both the system and tenant cluster startup. If
// only one of the two is desired, the caller can check the upgrade
// stage.
func (t *Test) BeforeClusterStart(desc string, fn stepFunc) {
	// Since the callbacks here are only referenced in the setup steps
	// of the planner, there is no need to have a predicate function
	// gating them.
	t.hooks.AddBeforeClusterStart(versionUpgradeHook{id: genId(t), name: desc, fn: fn})
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
	t.hooks.AddStartup(versionUpgradeHook{id: genId(t), name: desc, fn: fn})
}

// AfterUpgradeFinalized registers a callback that is run once per
// major upgrade performed in a test, after the upgrade is finalized
// successfully.  If multiple such hooks are passed, they may be
// executed concurrently.
func (t *Test) AfterUpgradeFinalized(desc string, fn stepFunc) {
	t.hooks.AddAfterUpgradeFinalized(versionUpgradeHook{id: genId(t), name: desc, fn: fn})
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
	t.hooks.AddBackground(versionUpgradeHook{id: genId(t), name: desc, fn: fn})

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
//
// By default, the binary used to run the command(s) will be the same as the
// the current version of the cluster at the time this hook is executed.
// We do this because we assume the binary is not backwards compatible.
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
		t.OnStartup(fmt.Sprintf("initialize %s workload", name), func(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *Helper) error {
			initCmd.Binary = h.VersionedCockroachPath(t.rt)
			l.Printf("running command `%s` on nodes %v", initCmd.String(), node)
			return t.cluster.RunE(ctx, option.WithNodes(node), initCmd.String())
		})
	}

	addSeed(runCmd)
	return t.BackgroundFunc(fmt.Sprintf("%s workload", name), func(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *Helper) error {
		runCmd.Binary = h.VersionedCockroachPath(t.rt)
		l.Printf("running command `%s` on nodes %v", runCmd.String(), node)
		return t.cluster.RunE(ctx, option.WithNodes(node), runCmd.String())
	})
}

// Run is like RunE, except it fatals the test if any error occurs.
func (t *Test) Run() {
	_, err := t.RunE()
	if err != nil {
		t.rt.Fatal(err)
	}
}

// RunE runs the mixed-version test. It should be called once all
// startup, mixed-version, and after-test hooks have been declared. A
// test plan will be generated (and logged), and the test will be
// carried out. A non-nil plan will be returned unless planning fails.
func (t *Test) RunE() (*TestPlan, error) {
	plan, err := t.plan()
	if err != nil {
		return nil, err
	}

	t.logger.Printf("mbv=%s,msv=%s,minUpgrades=%d,maxUpgrades=%d", t.options.minimumBootstrapVersion, t.options.minimumSupportedVersion, t.options.minUpgrades, t.options.maxUpgrades)
	t.logger.Printf("mixed-version test:\n%s", plan.PrettyPrint())

	if override := os.Getenv(dryRunEnv); override != "" {
		t.logger.Printf("skipping test run in dry-run mode")
		return plan, nil
	}

	// Mark the deployment mode and versions, so they show up in the github issue. This makes
	// it easier to group failures together without having to dig into the test logs.
	t.rt.AddParam("mvtDeploymentMode", string(plan.deploymentMode))
	t.rt.AddParam("mvtVersions", formatVersions(plan.Versions()))

	if err := t.run(plan); err != nil {
		return plan, err
	}

	return plan, nil
}

func (t *Test) run(plan *TestPlan) error {
	return newTestRunner(t.ctx, t.cancel, plan, t.rt, t.options.tag, t.logger, t.cluster).run()
}

func (t *Test) plan() (plan *TestPlan, retErr error) {
	defer func() {
		if r := recover(); r != nil {
			retErr = errors.Newf("panic during test planning: %v", r)
		}
		if retErr != nil {
			// Planning failures are always internal framework errors, so
			// they should be sent to TestEng.
			retErr = registry.ErrorWithOwner(
				registry.OwnerTestEng,
				errors.Wrap(retErr, "error creating test plan"),
			)
		}
	}()
	var retries int
	var numMajorUpgrades, numSameSeriesUpgrades int
	var skipVersions bool
	// In case the length of the test plan exceeds `opts.maxNumPlanSteps`, retry up to 100 times.
	// N.B. Statistically, the expected number of retries is miniscule; see #138014 for more info.
	for ; retries < 100; retries++ {

		// Pick a random deployment mode to use in this test run among the
		// list of enabled deployment modes enabled for this test.
		deploymentMode := t.deploymentMode()
		t.updateOptionsForDeploymentMode(deploymentMode)

		enableSameSeriesUpgrades := t.options.sameSeriesUpgradeProbability > 0 && t.prng.Float64() < t.options.sameSeriesUpgradeProbability
		numMajorUpgrades, numSameSeriesUpgrades = t.numUpgrades(enableSameSeriesUpgrades)
		skipVersions = t.prng.Float64() < t.options.skipVersionProbability
		upgradePath, err := t.chooseUpgradePath(numMajorUpgrades, numSameSeriesUpgrades, skipVersions)
		if err != nil {
			return nil, err
		}

		if override := os.Getenv(upgradePathOverrideEnv); override != "" {
			upgradePath, err = parseUpgradePathOverride(override)
			if err != nil {
				return nil, err
			}
			t.logger.Printf("%s override set: %s", upgradePathOverrideEnv, upgradePath)
		}

		tenantDescriptor := t.tenantDescriptor(deploymentMode)
		initialRelease := upgradePath[0]

		planner := testPlanner{
			versions:       upgradePath,
			deploymentMode: deploymentMode,
			seed:           t.seed,
			currentContext: newInitialContext(initialRelease, t.crdbNodes, tenantDescriptor),
			options:        t.options,
			rt:             t.rt,
			isLocal:        t.isLocal(),
			hooks:          t.hooks,
			prng:           t.prng,
			bgChans:        t.bgChans,
			logger:         t.logger,
			cluster:        t.cluster,
			_getFailer:     t._getFailer,
		}
		// Let's find a plan.
		plan, err = planner.Plan()
		if err != nil {
			return nil, errors.Wrapf(err, "error generating test plan")
		}
		if plan.length <= t.options.maxNumPlanSteps {
			break
		}
	}
	if plan.length > t.options.maxNumPlanSteps {
		return nil, errors.Newf("unable to generate a test plan with at most %d steps", t.options.maxNumPlanSteps)
	}
	if retries > 0 {
		t.logger.Printf("WARNING: generated a smaller (%d) test plan after %d retries", plan.length, retries)
	}
	if err := t.assertPlanValid(plan, numMajorUpgrades, numSameSeriesUpgrades, skipVersions); err != nil {
		t.logger.Errorf("invalid plan: %v\n%s", err, plan.prettyPrintInternal(true))
		return nil, err
	}
	// If we got this far, we have a (valid) plan!
	return plan, nil
}

func (t *Test) clusterArch() vm.CPUArch {
	if t._arch != nil {
		return *t._arch // test-only
	}
	return t.cluster.Architecture()
}

func (t *Test) assertPlanValid(
	plan *TestPlan, numMajorUpgrades, numSameSeriesUpgrades int, skipVersions bool,
) error {
	if len(plan.upgrades) == 0 {
		return errors.New("internal error: plan has no upgrades")
	}
	if err := t.assertNumUpgrades(plan, numMajorUpgrades, numSameSeriesUpgrades); err != nil {
		return err
	}
	if err := t.assertMbvAndMsv(plan); err != nil {
		return err
	}
	if err := t.assertSkipUpgrade(plan, skipVersions); err != nil {
		return err
	}
	// Finally, check that user-specified hooks occur in the plan.
	if err := t.assertExpectedUserHooks(plan); err != nil {
		return err
	}
	return nil
}

// assertNumUpgrades validates that the total number of upgrades in
// the plan matches the expected count derived from
// expectedPredecessorSteps (the number of predecessor versions
// selected by chooseUpgradePath, which may include natural
// same-series steps from the predecessor chain) and
// expectedSameSeriesInsertions (the number of additional same-series
// steps inserted by chooseSameSeriesUpgrades).
//
// The total number of upgrades in the plan should equal
// expectedPredecessorSteps + expectedSameSeriesInsertions, with the
// following tolerances:
//
//   - Skip version upgrades may reduce the total by 1.
//   - Same-series insertions may fall short if the randomly chosen
//     predecessor versions are all .0 or dev releases (which have no
//     older patches available for insertion). If the total is short,
//     we verify this is justified.
func (t *Test) assertNumUpgrades(
	plan *TestPlan, expectedPredecessorSteps, expectedSameSeriesInsertions int,
) error {
	upgrades := plan.allUpgrades()
	totalUpgrades := len(upgrades)
	expectedTotal := expectedPredecessorSteps + expectedSameSeriesInsertions

	// Upper bound: the plan should never have more upgrades than expected.
	if totalUpgrades > expectedTotal {
		return errors.Newf(
			"expected at most %d total upgrades (%d predecessor + %d same-series), got %d",
			expectedTotal, expectedPredecessorSteps, expectedSameSeriesInsertions, totalUpgrades,
		)
	}

	// Lower bound: skip-version upgrades may reduce the count by 1.
	minExpected := expectedTotal
	if t.options.skipVersionProbability > 0 {
		minExpected--
	}

	// Same-series insertions may also fall short if the chosen
	// predecessor versions have no older patches available.
	minExpected -= expectedSameSeriesInsertions

	if totalUpgrades < minExpected {
		return errors.Newf(
			"expected at least %d total upgrades, got %d",
			minExpected, totalUpgrades,
		)
	}

	// If the total is short and same-series insertions were expected,
	// verify the shortfall is justified: every cross-series upgrade
	// target should be a .0 release, the current (dev) version, or
	// have all older patches filtered out by msv/mbv constraints.
	if totalUpgrades < expectedTotal && expectedSameSeriesInsertions > 0 {
		mbv := t.options.minimumBootstrapVersion
		msv := t.options.minimumSupportedVersion
		for _, u := range upgrades {
			if u.from.Series() == u.to.Series() {
				continue
			}
			if u.to.Patch() == 0 || u.to.IsCurrent() {
				continue
			}
			// If msv or mbv is in the same series as u.to and all
			// older patches fall below the constraint, insertion was
			// not possible.
			if msv != nil && u.to.Series() == msv.Series() && u.to.Patch() <= msv.Patch() {
				continue
			}
			if mbv != nil && u.to.Series() == mbv.Series() && u.to.Patch() <= mbv.Patch() {
				continue
			}
			return errors.Newf(
				"expected %d total upgrades but got %d; "+
					"cross-series upgrade target %s has patch > 0 and is not the current version, "+
					"so a same-series insertion should have been possible",
				expectedTotal, totalUpgrades, u.to,
			)
		}
	}

	return nil
}

// Check the following,
// -- mbv <= msv <= finalVersion.
//
//	N.B. mbv <= msv already follows from assertValidTest which is a precondition for _any_ plan.
//
// -- mbv <= initialVersion.
// -- any upgrade version that's in the same release series as msb or msv must be greater than or equal to
// the respective version.
func (t *Test) assertMbvAndMsv(plan *TestPlan) error {
	allVersions := plan.Versions()
	msv := t.options.minimumSupportedVersion
	mbv := t.options.minimumBootstrapVersion
	msvSeries := msv.Series()
	if allVersions[len(allVersions)-1].LessThan(msv) {
		return errors.Newf("minimum supported version %s is not <= final version %s", msv, allVersions[len(allVersions)-1])
	}
	if mbv != nil {
		if allVersions[0].LessThan(mbv) {
			return errors.Newf("minimum bootstrap version %s is not <= initial version %s", mbv, allVersions[0])
		}
	}

	for _, v := range allVersions {
		if v.Series() == msvSeries {
			if v.LessThan(msv) {
				return errors.Newf("minimum supported version is not <= %s", msv, v)
			}
		}
		// N.B. mbvSeries <= msvSeries, thus the above implies mbvSeries <= v.Series().
	}
	return nil
}

// Check the following properties,
// -- upgrade path satisfies the `<` version ordering.
// -- skip upgrade when the upgrade path allows skip upgrade(s) and skipVersions is true.
// -- no skip upgrade when skipVersions is false.
// -- msv <= "from" version of a skip upgrade with same release series as msv.
// -- if skip upgrades are present, then the skip to final version is present, if supported.
func (t *Test) assertSkipUpgrade(plan *TestPlan, skipVersions bool) error {
	upgrades := plan.allUpgrades()
	lastUpgrade := upgrades[len(upgrades)-1]
	numSkips := 0
	numSkipsToFinal := 0
	hasEligibleSkips := false
	{
		if n, err := release.MajorReleasesBetween(&t.options.minimumSupportedVersion.Version, &lastUpgrade.to.Version); err != nil {
			return errors.NewAssertionErrorWithWrappedErrf(err, "unexpected error in MajorReleasesBetween")
		} else {
			// If msv is 2 major releases from final version, and final version supports skip upgrades, then we expect a skip.
			hasEligibleSkips = t.supportsSkipUpgradeTo(lastUpgrade.from, lastUpgrade.to) && n >= 2
		}
	}
	var prev *upgradePlan

	for i := 0; i < len(upgrades); i++ {
		if !upgrades[i].from.Version.LessThan(upgrades[i].to.Version) {
			return errors.Newf("invalid upgrade: %s -> %s", upgrades[i].from, upgrades[i].to)
		}
		if prev != nil {
			if !prev.to.Version.Equals(upgrades[i].from.Version) {
				return errors.Newf("invalid upgrade path: %s -> %s -> %s -> %s", prev.from, prev.to, upgrades[i].from, upgrades[i].to)
			}
		}
		if n, err := release.MajorReleasesBetween(&upgrades[i].from.Version, &upgrades[i].to.Version); err != nil {
			return errors.NewAssertionErrorWithWrappedErrf(err, "unexpected error in MajorReleasesBetween")
		} else if n >= 2 {
			numSkips++
			if i == len(upgrades)-1 {
				numSkipsToFinal++
			}
			if !skipVersions {
				return errors.Newf("upgrade path: %s -> %s has a skip while skipProbability=%.2f", upgrades[i].from, upgrades[i].to, t.options.skipVersionProbability)
			}
			if upgrades[i].from.Series() == t.options.minimumSupportedVersion.Series() && upgrades[i].from.LessThan(t.options.minimumSupportedVersion) {
				return errors.Newf("minimum supported version %s cannot be between %s and %s", t.options.minimumSupportedVersion, upgrades[i].from, upgrades[i].to)
			}
		}
		prev = upgrades[i]
	}
	if skipVersions && hasEligibleSkips && numSkips == 0 {
		return errors.Newf("expected at least one skip upgrade, got none")
	}
	// Check that the last cross-series upgrade is a skip when expected.
	// Same-series upgrades (e.g., v25.2.4 → v25.2.10) are never skips, so
	// we walk backwards to find the last cross-series upgrade and check
	// whether it is a skip.
	if numSkips > 0 {
		for i := len(upgrades) - 1; i >= 0; i-- {
			if upgrades[i].from.Series() == upgrades[i].to.Series() {
				continue
			}
			// Found the last cross-series upgrade.
			n, err := release.MajorReleasesBetween(&upgrades[i].from.Version, &upgrades[i].to.Version)
			if err != nil {
				return errors.NewAssertionErrorWithWrappedErrf(err, "unexpected error in MajorReleasesBetween")
			}
			isSkip := n >= 2
			if !isSkip && t.supportsSkipUpgradeTo(upgrades[i].from, upgrades[i].to) {
				return errors.Newf("expected at least one skip upgrade to the final version, got none")
			}
			break
		}
	}
	return nil
}

// assertExpectedUserHooks checks that the user-specified hooks occur in the plan.
// Each eligible upgrade in the plan is checked for an occurrence of corresponding
// user-specified hooks. If a hook is not found, an error is returned.
//
// Note that some hooks _may_ have multiple occurrences per upgrade. We only check for one.
func (t *Test) assertExpectedUserHooks(plan *TestPlan) error {
	// N.B. this check is disabled if the plan has `remove_user_hooks_mutator`; this is used only for testing plan
	// mutators. (See testdata/planner/conflicting_mutator)
	for _, m := range plan.enabledMutators {
		if m.Name() == "remove_user_hooks_mutator" {
			return nil
		}
	}

	var startHooks, runOnceVersionHooks, otherVersionHooks hooks
	startHooks = t.hooks.beforeClusterStart
	runOnceVersionHooks = append(runOnceVersionHooks, t.hooks.startup...)
	runOnceVersionHooks = append(runOnceVersionHooks, t.hooks.background...)
	otherVersionHooks = append(otherVersionHooks, t.hooks.mixedVersion...)
	otherVersionHooks = append(otherVersionHooks, t.hooks.afterUpgradeFinalized...)

	runOnceVersionHooksAdded := false
	// Maps "from" version to a set of expected user hooks.
	userHooks := map[string]map[string]bool{}
	// Gather all user hooks that are expected to occur in the plan.
	allVersions := plan.Versions()
	// Skip the final version; since there are no other upgrades _from_ it, no user hooks are expected.
	for i := 0; i < len(allVersions)-1; i++ {
		v := allVersions[i]
		from := v.String()
		// startHooks are only run when the cluster is starting up. They are not subject to minimumSupportedVersion.
		if i == 0 {
			for _, h := range startHooks {
				if userHooks[from] == nil {
					userHooks[from] = make(map[string]bool)
				}
				// N.B. combine name and id for debugging purposes; i.e., yields more descriptive error messages.
				userHooks[from][h.name+"_"+h.id] = true
			}
		}
		// N.B. user hooks are invoked only from versions that are at least minimumSupportedVersion.
		if !v.AtLeast(t.options.minimumSupportedVersion) {
			continue
		}
		// runOnceVersionHooks are run only once we reach an upgrade which satisfies minimumSupportedVersion.
		if !runOnceVersionHooksAdded {
			for _, h := range runOnceVersionHooks {
				if userHooks[from] == nil {
					userHooks[from] = make(map[string]bool)
				}
				userHooks[from][h.name+"_"+h.id] = true
				runOnceVersionHooksAdded = true
			}
		}
		for _, h := range otherVersionHooks {
			if userHooks[from] == nil {
				userHooks[from] = make(map[string]bool)
			}
			userHooks[from][h.name+"_"+h.id] = true
		}
	}
	if len(userHooks) == 0 && len(runOnceVersionHooks)+len(otherVersionHooks) > 0 {
		return errors.Newf("user hooks are not executable due to minimum supported version %s > all upgrade versions", t.options.minimumSupportedVersion)
	}

	// Visit each step and remove the corresponding	user hook.
	plan.iterateSingleSteps(func(ss *singleStep, _ bool) {
		from := ss.context.FromVersion().String()
		if hook, ok := ss.impl.(runHookStep); ok {
			delete(userHooks[from], hook.hook.name+"_"+hook.hook.id)
		}
	})
	// Check that no unused hooks remain. If so, that's likely a bug in the planner, so we return an error.
	for from := range userHooks {
		for hookName := range userHooks[from] {
			return errors.Newf("unused user hook: %q in version: %q", hookName, from)
		}
	}
	return nil
}

// genId generates a unique ID for the hook. The ID is of the form filename:line{_i}, where filename:line
// corresponds to where in the user code, the hook is created.
//
// Since filename is relative, i.e., not fully qualified package path, it may collide with another user hook. In that
// case, we add a suffix _i, where i is the insertion position of the hook.
// Note, it's assumed genId is called immediately after the corresponding hook is created; otherwise, filename:line
// will denote an _internal_ caller.
func genId(t *Test) string {
	id := ""
	// Skip the first two frames, which are the current function and its caller, e.g., InMixedVersion.
	if _, file, line, ok := runtime.Caller(2); ok {
		// Strip the path to just the filename for brevity.
		filename := filepath.Base(file)
		id = fmt.Sprintf("%s:%d", filename, line)
	}
	// N.B. `hookIds` is lazily allocated since some tests may initialize the `Test` struct differently.
	if t.hookIds == nil {
		t.hookIds = make(map[string]bool)
	}
	// Check if the id is already used or empty.
	if _, ok := t.hookIds[id]; ok || id == "" {
		// Append unique suffix.
		id = fmt.Sprintf("%s_%d", id, len(t.hookIds))
	}
	t.hookIds[id] = true

	return id
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

// chooseUpgradePath returns a valid upgrade path for the test to
// take. An upgrade path is a list of predecessor versions that can
// be upgraded into each other, ending at the current build version.
// It uses the `predecessorFunc` field to compute the actual list of
// predecessors. This function may also choose to skip releases when
// supported. Special care is taken to avoid using releases that are
// not available under a certain cluster architecture. Specifically,
// ARM64 builds are only available on v22.2.0+.
func (t *Test) chooseUpgradePath(
	numMajorUpgrades, numSameSeriesUpgrades int, skipVersions bool,
) ([]*clusterupgrade.Version, error) {
	mbv := t.options.minimumBootstrapVersion
	msv := t.options.minimumSupportedVersion
	// N.B. both mbv and msv are assumed to be non-nil; see assertValidTest.
	predecessors := []*clusterupgrade.Version{}
	v := clusterupgrade.CurrentVersion()
	// Checks whether a predecessor is feasible which amounts to being >= mbv.
	isAvailable := func(pred *clusterupgrade.Version) bool {
		return pred.AtLeast(mbv)
	}
	// Constructs the _longest_ feasible list of predecessors by iterating peredecessorFunc from the current version.
	// Every predecessor in the resulting list is in [mbv, currentVersion).
	for {
		pred, err := t.options.predecessorFunc(t.prng, v)
		if err != nil {
			return nil, err
		}
		pred, err = maybeClampMsbMsv(t.prng, pred, mbv, msv)
		if err != nil {
			return nil, err
		}
		if !isAvailable(pred) {
			// We've reached the _ beginning _ of a feasible upgrade path.
			break
		}
		predecessors = append(predecessors, pred)
		// N.B. Owing to maybeClampMsbMsv, we have msv <= pred.
		// However, if msv and pred are of the same release series, then it's possible that a _chosen_ upgrade path may end
		// up skipping over msv, thereby bypassing user-specified hooks. As an example, consider the following case,
		// msv=24.3.1 and upgrade path=24.1.1->24.2.2->24.3.11->25.1.1
		//
		// The first predecessor, i.e., pred(v)=24.3.11, satisfies msv <= pred. Thus, the upgrade step 24.3.11->25.1.1 will
		// execute user-specified hooks. However, consider a slightly different upgrade path where we chose to _skip_ over
		// the release series 24.3: 24.1.1->24.2.2->25.1.1
		// Now, the upgrade step 24.2.2->25.1.1 will _not_ execute user-specified hooks since 24.2.2 is below msv.
		// Consequently, we must ensure that the predecessor with the same release series as msv is _never_ skipped.
		// (See `isSkipAvailable(pred, v)` which ensures that pred can be skipped and v supports skip upgrades.)
		//
		v = pred
	}
	if skipVersions {
		// Choose a subset with skip upgrade steps.
		predecessors = t.chooseSkips(predecessors, numMajorUpgrades)
	}
	// N.B. len(predecessors) is the longest possible number of upgrade steps _modulo_ the result of `chooseSkips`.
	if len(predecessors) < numMajorUpgrades {
		warnMsg := "WARNING: %d upgrades requested but found plan with only %d upgrades"
		if skipVersions {
			t.logger.Printf("%s: prioritizing running at least one skip version upgrade", warnMsg)
		} else {
			return nil, errors.Newf("unable to find a valid upgrade path with %d upgrades", numMajorUpgrades)
		}
	}
	// The chosen upgrade path is bounded by numMajorUpgrades.
	availableUpgrades := min(len(predecessors), numMajorUpgrades)
	upgradePath := predecessors[0:availableUpgrades]
	// The upgrade path to be returned is from oldest to newest release.
	slices.Reverse(upgradePath)
	// Insert the predetermined number of same-series upgrades (e.g., 24.3.5 -> 24.3.12).
	// This is done before appending the current version to ensure the
	// current version remains the final step in the upgrade path.
	upgradePath = t.chooseSameSeriesUpgrades(upgradePath, numSameSeriesUpgrades)
	// Complete the upgrade path by appending the current version.
	upgradePath = append(upgradePath, clusterupgrade.CurrentVersion())

	return upgradePath, nil
}

// chooseSameSeriesUpgrades inserts up to numInsertions same-series upgrade
// steps into the upgrade path using rejection sampling. Multiple insertions
// can target the same version, producing chains like
// v24.2.1 → v24.2.3 → v24.2.5 → v24.2.8, which models realistic
// patch-level upgrade paths.
//
// For each insertion, the function picks a random version from the path and
// attempts to insert an older patch from the same release series. If a
// version already has insertions, the new patch must be older than the
// oldest patch already inserted (to maintain ordering). Insertion stops
// after numInsertions successful inserts or after exhausting retry attempts.
func (t *Test) chooseSameSeriesUpgrades(
	upgradePath []*clusterupgrade.Version, numInsertions int,
) []*clusterupgrade.Version {
	if numInsertions == 0 {
		return upgradePath
	}

	mbv := t.options.minimumBootstrapVersion
	msv := t.options.minimumSupportedVersion

	const maxRetries = 100
	// Each index can have multiple patches, ordered from newest to oldest
	// (order of insertion). When reconstructed, they are reversed so the
	// oldest patch comes first in the upgrade path.
	insertions := make(map[int][]*clusterupgrade.Version)
	for inserted, retries := 0, 0; inserted < numInsertions && retries < maxRetries; retries++ {
		idx := t.prng.Intn(len(upgradePath))

		// Determine the reference version: if we've already inserted
		// patches at this index, the next patch must be older than the
		// oldest one inserted so far (to maintain ordering).
		referenceVersion := upgradePath[idx]
		if patches := insertions[idx]; len(patches) > 0 {
			referenceVersion = patches[len(patches)-1]
		}

		olderPatches, err := release.OlderPatchReleases(&referenceVersion.Version)
		if err != nil || len(olderPatches) == 0 {
			continue
		}

		// Filter patches that respect mbv and msv constraints.
		var validPatches []*clusterupgrade.Version
		for _, patchStr := range olderPatches {
			patchV := clusterupgrade.MustParseVersion(patchStr)
			if mbv != nil && patchV.LessThan(mbv) {
				continue
			}
			if msv != nil && patchV.Series() == msv.Series() && patchV.LessThan(msv) {
				continue
			}
			validPatches = append(validPatches, patchV)
		}

		if len(validPatches) == 0 {
			continue
		}

		chosen := validPatches[t.prng.Intn(len(validPatches))]
		insertions[idx] = append(insertions[idx], chosen)
		inserted++
	}

	// Reconstruct the path with insertions.
	totalInserted := 0
	for _, patches := range insertions {
		totalInserted += len(patches)
	}
	result := make([]*clusterupgrade.Version, 0, len(upgradePath)+totalInserted)
	for i, v := range upgradePath {
		if patches, ok := insertions[i]; ok {
			// Patches were inserted newest-first; reverse to get oldest-first
			// so the upgrade path is monotonically increasing.
			for j := len(patches) - 1; j >= 0; j-- {
				result = append(result, patches[j])
			}
		}
		result = append(result, v)
	}

	return result
}

// chooseSkips returns a subset of predecessors (from newest to oldest) by skipping
// zero or more predecessors, with probability=1/2.
// The first predecessor is skipped with probability=1 if it's eligible for skipping.
//
// E.g., given predecessors=[24.3.11,24.2.2,24.1.1], CurrentVersion=25.1.1, and msv=24.3.1, the resulting subsets
// are one of the following,
// - [24.3.11,24.2.2,24.1.1] (no skips)
// - [24.3.11,24.1.1] (skipped 24.2.2)
// N.B. 24.3.11 cannot be skipped since it's of the same release series as msv.
func (t *Test) chooseSkips(
	predecessors []*clusterupgrade.Version, numUpgrades int,
) []*clusterupgrade.Version {
	if len(predecessors) <= 1 {
		// No (more) skips are possible when we have at most one upgrade step.
		return predecessors
	}
	res := []*clusterupgrade.Version{}
	v := clusterupgrade.CurrentVersion()
	numSkips := 0

	for i, pred := range predecessors {
		exclude := false
		// Exclude a predecessor when,
		// - it's the first predecessor (i.e., wrt current version) and is eligible for skipping
		// - it's any other predecessor that is eligible for skipping, with probability 1/2
		if i == 0 && t.supportsSkipUpgradeTo(pred, v) {
			// We prioritize the skip to the _current_ release--the most important upgrade to be tested on any release branch.
			exclude = true
		}
		if t.prng.Intn(2) == 0 && t.supportsSkipUpgradeTo(pred, v) {
			exclude = true
		}
		if !exclude || len(predecessors)-numSkips < numUpgrades {
			// Keep this predecessor; it's not eligible for skipping.
			res = append(res, pred)
		} else {
			numSkips++
		}
		v = pred
	}
	return res
}

// numUpgrades returns the number of major (cross-series) upgrades and
// same-series (patch-level) upgrades that will be performed in this
// test run. majorUpgrades is picked from [minUpgrades, maxUpgrades].
// When enableSameSeriesUpgrades is true, sameSeriesUpgrades is picked
// from [0, maxUpgrades - majorUpgrades]; otherwise it is 0. The total
// number of upgrades is bounded by maxUpgrades.
func (t *Test) numUpgrades(enableSameSeriesUpgrades bool) (majorUpgrades, sameSeriesUpgrades int) {
	majorUpgrades = t.prng.Intn(
		t.options.maxUpgrades-t.options.minUpgrades+1,
	) + t.options.minUpgrades
	if enableSameSeriesUpgrades {
		remainingUpgrades := t.options.maxUpgrades - majorUpgrades
		sameSeriesUpgrades = t.prng.Intn(remainingUpgrades + 1)
	}
	return majorUpgrades, sameSeriesUpgrades
}

func (t *Test) deploymentMode() DeploymentMode {
	deploymentMode := t.options.enabledDeploymentModes[t.prng.Intn(len(t.options.enabledDeploymentModes))]
	if deploymentModeOverride := os.Getenv(deploymentModeOverrideEnv); deploymentModeOverride != "" {
		deploymentMode = DeploymentMode(deploymentModeOverride)
		t.logger.Printf("%s override set: %s", deploymentModeOverrideEnv, deploymentModeOverride)
	}
	return deploymentMode
}

// latestPredecessor is an implementation of `predecessorFunc` that
// always picks the latest predecessor for the given release version.
func latestPredecessor(_ *rand.Rand, v *clusterupgrade.Version) (*clusterupgrade.Version, error) {
	predecessor, err := release.LatestPredecessor(&v.Version)
	if err != nil {
		return nil, err
	}

	return clusterupgrade.MustParseVersion(predecessor), nil
}

// maybeClampMsbMsv attempts to clamp version patch of given version `v` such that
// it satisfies minimum bootstrap (msb) and minimum supported (msv) versions.
//
// If neither msb nor msv is set, it returns the original version `v`.
// If neither msb nor msv is of the same release series as `v`, it returns the original version `v`.
// Otherwise, the returned version (`newV`) is of the same release series as `v` such that
// its patch version satisfies: msv <= newV and msb <= newV, relative to the release series; i.e., if release
// series are different, <= is trivially satisfied.
//
// E.g., if v is v23.2.1, minBootstrap is v23.1.13, and minSupported is v23.2.5, then
// the result is v23.2.x, where x is between 5 and 21, inclusive. (Note, the latest patch version of
// v23.2 is 21.)
//
// N.B. This function is typically used in conjunction with predecessorFunc to ensure that the chosen predecessor
// satisfies msv <= newV and msb <= newV. This in turn ensures that user-specified hooks can be run from predecessor.
func maybeClampMsbMsv(
	rng *rand.Rand, v, minBootstrap, minSupported *clusterupgrade.Version,
) (*clusterupgrade.Version, error) {
	minVersion := minSupported
	var minBootstrapSeries string
	if minBootstrap != nil {
		minBootstrapSeries = minBootstrap.Series()
	}
	// Owing to `assertValidTest`, we know that mbv <= msv.
	// Thus, to establish msv <= newV and msb <= newV, we must consider the following 3 cases:
	//
	// Case 1. v's series is a different series than the mbv and msv series, pick any random patch release.
	// Case 2. v's series is the same as the msv series, validate against the msv.
	// Case 3. v's series is the same as the mbv series, validate against the mbv.
	//
	// Since mbv <= msv, if v's series is the same as _both_ mbv and msv, then it suffices to handle Case 2.
	//
	// For example, consider mbv of v23.1.3 and msv of v24.1.5. The framework may pick an upgrade path of:
	// v23.1 -> v23.2 -> v24.1 -> 24.2. When clamping the patch version of:
	//
	//		v23.1: This is the same series as our mbv, so the new version needs to be
	//		at least v23.1.3 or the plan will not be valid. In this case, minVersion is
	//    set to the mbv. We can ignore the msv here since it is an older series so
	//		no patch release could satisfy it.
	//
	//		v23.2: This isn't the same series as either the msv or mbv, so we can ignore
	//		minVersion and pick any random patch release. Note that mbv <= v23.2 and msv <= v23.2 (since
	//		the release series are different.)
	//
	//		v24.1: This is the same series as our msv, so the new version needs to be
	//		at least v24.1.5. In this case, minVersion is set to the msv.
	//
	if v.Series() != minSupported.Series() && v.Series() != minBootstrapSeries {
		// Case 1 above.
		return v, nil
	} else if v.Series() == minSupported.Series() {
		// Case 2 above.
		minVersion = minSupported
	} else {
		// Case 3 above.
		minVersion = minBootstrap
	}

	// If the latest release of a series is a pre-release, we validate
	// whether the minimum supported version is valid.
	if v.IsPrerelease() && !v.AtLeast(minVersion) {
		return nil, fmt.Errorf(
			"latest release for %s (%s) is not sufficient for minimum supported version (%s)", v.Series(), v, minVersion.Version,
		)
	}
	// N.B. After the above case analysis, minVersion and v are of the same series.

	// Check if minVersion <= v, then we're done.
	if minVersion.Patch() <= v.Patch() {
		return v, nil
	}
	// Otherwise, pick a random patch from [minVersion.Patch, minVersion.LatestPatch].
	latestPatch, err := release.LatestPatch(v.Series())
	if err != nil {
		return nil, err
	}
	latestPatchV := clusterupgrade.MustParseVersion(latestPatch)

	var supportedPatchReleases []*clusterupgrade.Version
	for i := minVersion.Patch(); i <= latestPatchV.Patch(); i++ {
		supportedV := clusterupgrade.MustParseVersion(
			fmt.Sprintf("%s.%d", v.Major().String(), i),
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

// randomPredecessor is an implementation of `predecessorFunc` that
// picks a random predecessor for the given release version.
func randomPredecessor(rng *rand.Rand, v *clusterupgrade.Version) (*clusterupgrade.Version, error) {
	predecessor, err := release.RandomPredecessor(rng, &v.Version)
	if err != nil {
		return nil, err
	}

	return clusterupgrade.MustParseVersion(predecessor), nil
}

func (t *Test) updateOptionsForDeploymentMode(mode DeploymentMode) {
	if mode == SharedProcessDeployment {
		if v := OldestSupportedVersionSP; !t.options.minimumSupportedVersion.AtLeast(v) {
			t.options.minimumSupportedVersion = v
		}
	}

	if t.options.predecessorFunc == nil {
		switch mode {
		case SeparateProcessDeployment:
			// We use latest predecessors in separate-process deployments since separate-process
			// is more prone to flake (due to the relative lack of testing historically). In
			// addition, production separate-process deployments (Serverless) run in much more
			// controlled environments than self-hosted and are generally running the latest
			// patch releases. Same-series upgrades are also disabled since they would
			// reintroduce older patches, contradicting the intent of using latest predecessors.
			t.options.predecessorFunc = latestPredecessor
			t.options.sameSeriesUpgradeProbability = 0
		default:
			t.options.predecessorFunc = randomPredecessor
		}
	}
}

func (t *Test) tenantDescriptor(deploymentMode DeploymentMode) *ServiceDescriptor {
	switch deploymentMode {
	case SystemOnlyDeployment:
		return nil

	case SharedProcessDeployment, SeparateProcessDeployment:
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
// parameter. The path and prefix are the same.
func prefixedLogger(l *logger.Logger, prefix string) (*logger.Logger, error) {
	return prefixedLoggerWithFilename(l, prefix, prefix)
}

// prefixedLoggerWithFilename returns a logger instance with the given
// prefix. The logger will write to a file on the given `path`,
// relative to the logger `l`'s location.
func prefixedLoggerWithFilename(l *logger.Logger, prefix, path string) (*logger.Logger, error) {
	formattedPrefix := fmt.Sprintf("[%s] ", sanitizePath(prefix))
	return l.ChildLogger(sanitizePath(path), logger.LogPrefix(formattedPrefix))
}

func sanitizePath(s string) string {
	return strings.ReplaceAll(s, " ", "-")
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

// AsSteps transforms the sequence of hooks into a sequence of
// `*singleStep` structs to be run in some way by the planner
// (sequentially, concurrently, etc). `stopChans` should either be
// `nil` for steps that are not meant to be run in the background, or
// contain one stop channel (`shouldStop`) for each hook.
func (h hooks) AsSteps(prng *rand.Rand, testContext *Context, stopChans []shouldStop) []testStep {
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

	return steps
}

func (th *testHooks) AddBeforeClusterStart(hook versionUpgradeHook) {
	th.beforeClusterStart = append(th.beforeClusterStart, hook)
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

func (th *testHooks) BeforeClusterStartSteps(testContext *Context, rng *rand.Rand) []testStep {
	return th.beforeClusterStart.AsSteps(rng, testContext, nil)
}

func (th *testHooks) StartupSteps(testContext *Context, rng *rand.Rand) []testStep {
	return th.startup.AsSteps(rng, testContext, nil)
}

func (th *testHooks) BackgroundSteps(
	testContext *Context, stopChans []shouldStop, rng *rand.Rand,
) []testStep {
	testContext.System.Stage = BackgroundStage
	if testContext.Tenant != nil {
		testContext.Tenant.Stage = BackgroundStage
	}

	return th.background.AsSteps(rng, testContext, stopChans)
}

func (th *testHooks) MixedVersionSteps(testContext *Context, rng *rand.Rand) []testStep {
	return th.mixedVersion.
		Filter(*testContext).
		AsSteps(rng, testContext, nil)
}

func (th *testHooks) AfterUpgradeFinalizedSteps(testContext *Context, rng *rand.Rand) []testStep {
	return th.afterUpgradeFinalized.AsSteps(rng, testContext, nil)
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

// validDeploymentModesForCloud computes a subset of the given
// parameter `modes` containing the deployment modes that can be run
// on the given cloud. Specifically, the only rule enforced at the
// moment is that separate-process deployments are only possible
// locally or on GCE, since they require service registration.
func validDeploymentModesForCloud(cloud spec.Cloud, modes []DeploymentMode) []DeploymentMode {
	if cloud == spec.GCE || cloud == spec.Local {
		return modes
	}

	var validModes []DeploymentMode
	for _, m := range modes {
		if m != SeparateProcessDeployment {
			validModes = append(validModes, m)
		}
	}

	return validModes
}

// assertValidTest checks that the test configuration is _sound_ but not necessarily _complete_.
// N.B. Soundness follows by ensuring that the specified options do not contradict or violate framework invariants.
// The following checks are performed,
// 1. If using fixtures, the cluster must have exactly numNodesInFixtures nodes.
// 2. maxUpgrades must be >= minUpgrades.
// 3. minimumSupportedVersion must be from an older release series than the current version. (This ensures that at least one upgrade is possible.)
// 4. At least one valid deployment mode must be enabled.
// 5. If SeparateProcessDeployment is enabled, the cluster must have at least 3 nodes.
// 6. If minimumBootstrapVersion is set, it must be from an older release series than the current version.
// 7. If minimumBootstrapVersion is set, it must allow for at least minUpgrades to the current version.
// 8. If minimumBootstrapVersion is set, minimumSupportedVersion must be at least as new as minimumBootstrapVersion.
//
// If any of the above conditions are violated, the test is considered invalid and the fatalFunc is invoked.
//
// Completeness is a stronger property which requires searching for a valid upgrade path. E.g., based on the specified
// minimumSupportedVersion, minimumBootstrapVersion, number of upgrades, cluster architecture, etc., there may not
// exist a valid upgrade path.
func assertValidTest(test *Test, fatalFunc func(...interface{})) {
	fail := func(err error) {
		fatalFunc(errors.Wrap(err, "mixedversion.NewTest"))
	}
	minUpgrades := test.options.minUpgrades
	maxUpgrades := test.options.maxUpgrades

	if test.options.useFixturesProbability > 0 && len(test.crdbNodes) != numNodesInFixtures {
		fail(
			fmt.Errorf(
				"invalid cluster: use of fixtures requires %d cockroach nodes, got %d (%v)",
				numNodesInFixtures, len(test.crdbNodes), test.crdbNodes,
			),
		)
	}

	if minUpgrades > maxUpgrades {
		fail(
			fmt.Errorf(
				"invalid test options: maxUpgrades (%d) must be >= minUpgrades (%d)",
				maxUpgrades, minUpgrades,
			),
		)
	}

	currentVersion := clusterupgrade.CurrentVersion()
	msv := test.options.minimumSupportedVersion
	// The current version should be at least one release series newer from
	// the minimum supported version.
	validVersion := currentVersion.CompareSeries(msv.Version) == 1

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
	var supportsSeparateProcess bool
	for _, dm := range test.options.enabledDeploymentModes {
		if !validDeploymentMode(dm) {
			fail(fmt.Errorf("invalid test options: unknown deployment mode %q", dm))
		}

		if dm == SeparateProcessDeployment {
			supportsSeparateProcess = true
		}
	}

	// In separate process deployments, we need to make sure the storage
	// cluster is still functional while a node is restarting.
	// Otherwise, the tenant could fail to keep its sqllivenes record
	// active, causing it to voluntarily shutdown.
	const minSeparateProcessNodes = 3
	if supportsSeparateProcess && len(test.crdbNodes) < minSeparateProcessNodes {
		fail(fmt.Errorf(
			"invalid test options: %s deployments require cluster with at least %d nodes",
			SeparateProcessDeployment, minSeparateProcessNodes,
		))
	}

	// If minimumBootstrapVersion is unspecified, set a default.
	if test.options.minimumBootstrapVersion == nil {
		if test.clusterArch() == vm.ArchS390x {
			test.options.minimumBootstrapVersion = MinBootstrapSupportedVersionS390x
		} else {
			test.options.minimumBootstrapVersion = MinBootstrapSupportedVersion
		}
	}
	// Validate the minimum bootstrap version.
	minBootstrapVersion := test.options.minimumBootstrapVersion

	// The minimum bootstrap version should be from an older major version.
	validVersion = minBootstrapVersion.Major().LessThan(currentVersion.Major())
	if !validVersion {
		fail(
			fmt.Errorf(
				"invalid test options: minimum bootstrap version (%s) should be from an older release series than current version (%s)",
				minBootstrapVersion.Version.String(), currentVersion.Version.String(),
			),
		)
	}

	// The minimum bootstrap version should be compatible with the min and max upgrades.
	maxUpgradesFromBootstrapVersion, err := release.MajorReleasesBetween(&minBootstrapVersion.Version, &currentVersion.Version)
	if err != nil {
		fail(err)
	}
	if maxUpgradesFromBootstrapVersion < minUpgrades {
		fail(errors.Newf(
			"invalid test options: minimum bootstrap version (%s) does not allow for min %d upgrades to %s, max is %d",
			minBootstrapVersion, minUpgrades, currentVersion, maxUpgradesFromBootstrapVersion,
		))
	}
	// Override the max upgrades if the minimum bootstrap version does not allow for that
	// many upgrades.
	if maxUpgrades > maxUpgradesFromBootstrapVersion {
		test.logger.Printf("WARN: overriding maxUpgrades, minimum bootstrap version (%s) allows for at most %d upgrades", minBootstrapVersion, maxUpgradesFromBootstrapVersion)
		test.options.maxUpgrades = maxUpgradesFromBootstrapVersion
	}

	if msv.LessThan(minBootstrapVersion) {
		test.logger.Printf("WARN: overriding minSupportedVersion, cannot be older than minimum bootstrap version (%s)", minBootstrapVersion)
		test.options.minimumSupportedVersion = minBootstrapVersion
	}

	if test.options.useLatestPredecessors && test.options.sameSeriesUpgradeProbability > 0 {
		fail(fmt.Errorf(
			"invalid test options: same-series upgrades (probability=%.2f) cannot be used with AlwaysUseLatestPredecessors",
			test.options.sameSeriesUpgradeProbability,
		))
	}
}

// parseUpgradePathOverride parses the upgrade path override and returns it as a list
// of versions for the framework to use instead of generating a path based on
// the seed. It assumes the user knows what it's doing and forgoes validation
// of legal upgrade paths.
func parseUpgradePathOverride(override string) ([]*clusterupgrade.Version, error) {
	versions := strings.Split(override, ",")
	var upgradePath []*clusterupgrade.Version
	for _, v := range versions {
		// Special case for the current version, as the current version on
		// master is usually a long prerelease.
		if v == "current" || v == "<current>" {
			upgradePath = append(upgradePath, clusterupgrade.CurrentVersion())
			continue
		}

		parsedVersion, err := clusterupgrade.ParseVersion(v)
		if err == nil {
			upgradePath = append(upgradePath, parsedVersion)
			continue
		}

		// If the supplied version is invalid, it might be a release series.
		// Support parsing release series as well since the user might not
		// care about the exact patch version.
		parsedVersion, err = clusterupgrade.LatestPatchRelease(v)
		if err != nil {
			return nil, errors.Newf("unable to parse version: %s", v)
		}

		upgradePath = append(upgradePath, parsedVersion)
	}

	return upgradePath, nil
}
