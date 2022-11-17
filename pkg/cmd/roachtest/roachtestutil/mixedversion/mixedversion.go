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
// is logged before the test runs, and it's easy to understand at
// which point the test failed when debugging issues.
//
// Typical usage:
//
//	mvt, err := NewMixedVersionTest(...)
//	mvt.InMixedVersion("test my feature", func(l *logger.Logger, db *gosql.DB) error {
//	    l.Printf("testing feature X")
//	    _, err := db.ExecContext(ctx, "SELECT * FROM test")
//	    return err
//	})
//	mvt.InMixedVersion("test another feature", func(l *logger.Logger, db *gosql.DB) error {
//	    l.Printf("testing feature Y")
//	    _, err := db.ExecContext(ctx, "SELECT * FROM test2")
//	    return err
//	})
//	mvt.Run()
//
// Fuctions passed to `InMixedVersion` will be called at arbitrary
// points during an upgrade/downgrade process. They may also be called
// separately or concurrently. This information will be included in
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
	gosql "database/sql"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/version"
)

const (
	logPrefix         = "mixed-version-test"
	startupLabel      = "run startup hooks"
	mixedVersionLabel = "run mixed-version hooks"
	afterTestLabel    = "run after test hooks"

	// runWhileMigratingProbability is the probability that a
	// mixed-version hook will also run while all nodes in the cluster
	// have been upgraded and the cluster is allowed to upgrade.
	runWhileMigratingProbability = 0.5

	noDBNeeded = -1
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

	hookFunc      func(*logger.Logger, *gosql.DB) error
	predicateFunc func(Context) bool

	// versionUpgradeHook is a hook that can be called at any time
	// during an upgrade/downgrade process. The `predicate` is used
	// during test planning to determine when it is called.
	versionUpgradeHook struct {
		name      string
		predicate predicateFunc
		fn        hookFunc
	}

	// testStep is an opaque reference to one step of a mixed-version
	// test. It can be a runnableStep (see below), or a "meta-step",
	// meaning that it combines other steps in some way (for instance, a
	// series of steps to run in sequence or concurrently).
	testStep interface{}

	// runnableStep represents steps that implement the pieces on top of
	// which a mixed-version test is built.
	runnableStep interface {
		// ID returns a unique ID associated with the step, making it easy
		// to reference test output with the exact step it relates to
		ID() int
		// DBNode returns the database node that that step connects to
		// during its execution. If the step does not require a database
		// connection, this function should return the `noDBNeeded`
		// constant
		DBNode() int
		// Description is a string representation of the step, intended
		// for human-consumption. Displayed when pretty-printing the test
		// plan.
		Description() string
		// Run implements the actual functionality of the step.
		Run(context.Context, *logger.Logger, cluster.Cluster, func(int) *gosql.DB) error
	}

	hooks []versionUpgradeHook

	// testHooks groups hooks associated with a mixed-version test in
	// its different stages: startup, mixed-version, and after-test.
	testHooks struct {
		startup      hooks
		mixedVersion hooks
		afterTest    hooks

		prng      *rand.Rand
		crdbNodes option.NodeListOption
	}

	// testOptions groups options typically linked with the `test.Test`
	// struct that we need to pass to this package separately.
	testOptions struct {
		crdbNodes              option.NodeListOption
		currentCockroachPath   string
		versionsBinaryOverride map[string]string
	}

	// Test is the main struct callers of this package interact with.
	Test struct {
		ctx          context.Context
		cluster      cluster.Cluster
		logger       *logger.Logger
		buildVersion version.Version

		opts  *testOptions
		prng  *rand.Rand
		seed  int64
		hooks *testHooks
	}
)

// NewTest creates a Test struct that users can use to create and run
// a mixed-version test. Most of the parameters come from the
// `test.Test` struct, which can't be directly passed here due to
// import cycles.
func NewTest(
	ctx context.Context,
	l *logger.Logger,
	c cluster.Cluster,
	buildVersion version.Version,
	crdbNodes option.NodeListOption,
	cockroachPath string,
	versionsBinaryOverride map[string]string,
) (*Test, error) {
	testLogger, err := prefixedLogger(l, logPrefix)
	if err != nil {
		return nil, err
	}

	prng, seed := randutil.NewPseudoRand()
	testLogger.Printf("random seed: %d", seed)

	return &Test{
		ctx:     ctx,
		cluster: c,
		logger:  testLogger,
		opts: &testOptions{
			crdbNodes:              crdbNodes,
			currentCockroachPath:   cockroachPath,
			versionsBinaryOverride: versionsBinaryOverride,
		},
		buildVersion: buildVersion,
		prng:         prng,
		seed:         seed,
		hooks:        &testHooks{prng: prng, crdbNodes: crdbNodes},
	}, nil
}

// InMixedVersion adds a new mixed-version hook to the test. The
// functionality in the function passed as argument to this function
// will be tested in arbitrary mixed-version states.
func (t *Test) InMixedVersion(desc string, fn hookFunc) {
	lastFromVersion := "invalid-version"
	var upgradedNodes int
	predicate := func(testContext Context) bool {
		// If the cluster is finalizing an upgrade, run this hook
		// according with the probability defined in the package.
		if testContext.Finalizing {
			return t.prng.Float64() < runWhileMigratingProbability
		}

		// This check makes sure we only schedule a mixed-version hook
		// once while upgrading from one version to another. The number of
		// nodes we wait to be running the new version is determined when
		// the version changes for the first time.
		if testContext.FromVersion != lastFromVersion {
			lastFromVersion = testContext.FromVersion
			upgradedNodes = t.prng.Intn(len(t.opts.crdbNodes)) + 1
		}

		return len(testContext.ToVersionNodes) == upgradedNodes
	}

	t.hooks.AddMixedVersion(versionUpgradeHook{desc, predicate, fn})
}

// OnStartup registers a callback that is run once the cluster is
// initialized (i.e., all nodes in the cluster start running CRDB at a
// certain previous version, potentially from existing fixtures).
func (t *Test) OnStartup(desc string, fn hookFunc) {
	// Since the callbacks here are only referenced in the setup steps
	// of the planner, there is no need to have a predicate function
	// gating them.
	t.hooks.AddStartup(versionUpgradeHook{desc, nil, fn})
}

// AfterTest registers a callback that is run once the mixed-version
// test has started from a previous version, performed an upgrade, and
// waited for the cluster version to upgrade on all nodes.
func (t *Test) AfterTest(desc string, fn hookFunc) {
	t.hooks.AddAfterTest(versionUpgradeHook{desc, nil, fn})
}

// Run runs the mixed-version test. It should be called once all
// startup, mixed-version, and after-test hooks have been declared. A
// test plan will be generated (and logged), and the test will be
// carried out.
func (t *Test) Run() error {
	plan, err := t.plan()
	if err != nil {
		return err
	}

	return t.run(plan)
}

func (t *Test) run(plan *TestPlan) error {
	return newTestRunner(
		t.ctx, plan, t.logger, t.cluster, t.opts.crdbNodes, t.seed,
	).run()
}

func (t *Test) plan() (*TestPlan, error) {
	previousRelease, err := clusterupgrade.PredecessorVersion(t.buildVersion)
	if err != nil {
		return nil, err
	}

	planner := testPlanner{
		initialVersion: previousRelease,
		opts:           t.opts,
		hooks:          t.hooks,
		prng:           t.prng,
	}

	return planner.Plan(), nil
}

// startFromCheckpointStep is the step that starts the cluster from a
// specific `version`, using checked-in fixtures.
type startFromCheckpointStep struct {
	id      int
	version string
	opts    *testOptions
}

func (s startFromCheckpointStep) ID() int     { return s.id }
func (s startFromCheckpointStep) DBNode() int { return noDBNeeded }

func (s startFromCheckpointStep) Description() string {
	return fmt.Sprintf("starting cluster from fixtures for version %q", s.version)
}

// Run will copy the fixtures to all database nodes in the cluster,
// upload the binary associated with that given version, and finally
// start the cockroach binary on these nodes.
func (s startFromCheckpointStep) Run(
	ctx context.Context, l *logger.Logger, c cluster.Cluster, dbFunc func(int) *gosql.DB,
) error {
	if err := clusterupgrade.InstallFixtures(ctx, l, c, s.opts.crdbNodes, s.version); err != nil {
		return err
	}

	binaryPath, err := clusterupgrade.UploadVersion(
		ctx, l, c, s.opts.crdbNodes, s.version, s.opts.currentCockroachPath, s.opts.versionsBinaryOverride,
	)
	if err != nil {
		return err
	}

	clusterupgrade.StartWithBinary(ctx, l, c, s.opts.crdbNodes, binaryPath, false /* sequential */)
	return nil
}

// waitForStableClusterVersionStep implements the process of waiting
// for the `version` cluster setting being the same on all nodes of
// the cluster and equal to the binary version of the first node in
// the `nodes` field.
type waitForStableClusterVersionStep struct {
	id    int
	nodes option.NodeListOption
}

func (s waitForStableClusterVersionStep) ID() int     { return s.id }
func (s waitForStableClusterVersionStep) DBNode() int { return noDBNeeded }

func (s waitForStableClusterVersionStep) Description() string {
	return fmt.Sprintf(
		"wait for nodes %v to all have the same cluster version (same as binary version of node %d)",
		s.nodes, s.nodes[0],
	)
}

func (s waitForStableClusterVersionStep) Run(
	ctx context.Context, l *logger.Logger, c cluster.Cluster, dbFunc func(int) *gosql.DB,
) error {
	return clusterupgrade.WaitForClusterUpgrade(ctx, l, s.nodes, dbFunc)
}

// preserveDowngradeOptionStep sets the `preserve_downgrade_option`
// cluster setting to the binary version running in `node`.
type preserveDowngradeOptionStep struct {
	id   int
	node int
}

func (s preserveDowngradeOptionStep) ID() int     { return s.id }
func (s preserveDowngradeOptionStep) DBNode() int { return s.node }

func (s preserveDowngradeOptionStep) Description() string {
	return "preventing auto-upgrades by setting `preserve_downgrade_option`"
}

func (s preserveDowngradeOptionStep) Run(
	ctx context.Context, l *logger.Logger, c cluster.Cluster, dbFunc func(int) *gosql.DB,
) error {
	db := dbFunc(s.node)

	l.Printf("checking binary version on node %d", s.node)
	bv, err := clusterupgrade.BinaryVersion(ctx, db)
	if err != nil {
		return err
	}

	downgradeOption := bv.String()
	l.Printf("setting `preserve_downgrade_option` to %s", downgradeOption)
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
	opts    *testOptions
	node    int
}

func (s restartWithNewBinaryStep) ID() int     { return s.id }
func (s restartWithNewBinaryStep) DBNode() int { return noDBNeeded }

func (s restartWithNewBinaryStep) Description() string {
	return fmt.Sprintf("restart node %d with binary version %s", s.node, versionMsg(s.version))
}

func (s restartWithNewBinaryStep) Run(
	ctx context.Context, l *logger.Logger, c cluster.Cluster, dbFunc func(int) *gosql.DB,
) error {
	return clusterupgrade.RestartNodesWithNewBinary(
		ctx,
		l,
		c,
		c.Node(s.node),
		option.DefaultStartOpts(),
		s.version,
		s.opts.currentCockroachPath,
		s.opts.versionsBinaryOverride,
	)
}

// finalizeUpgradeStep resets the `preserve_downgrade_option` cluster
// setting, allowing the upgrade migrations to run and the cluster
// version to eventually reach the binary version on the nodes.
type finalizeUpgradeStep struct {
	id   int
	node int
}

func (s finalizeUpgradeStep) ID() int     { return s.id }
func (s finalizeUpgradeStep) DBNode() int { return s.node }

func (s finalizeUpgradeStep) Description() string {
	return "finalize upgrade by resetting `preserve_downgrade_option`"
}

func (s finalizeUpgradeStep) Run(
	ctx context.Context, l *logger.Logger, c cluster.Cluster, dbFunc func(int) *gosql.DB,
) error {
	db := dbFunc(s.node)
	l.Printf("resetting preserve_downgrade_option")
	_, err := db.ExecContext(ctx, "RESET CLUSTER SETTING cluster.preserve_downgrade_option")
	return err
}

// runHookStep is a step used to run a user-provided hook (i.e.,
// callbacks passed to `OnStartup`, `InMixedVersion`, or `AfterTest`).
type runHookStep struct {
	id   int
	node int
	hook versionUpgradeHook
}

func (s runHookStep) ID() int     { return s.id }
func (s runHookStep) DBNode() int { return s.node }

func (s runHookStep) Description() string {
	return fmt.Sprintf("run %q", s.hook.name)
}

func (s runHookStep) Run(
	ctx context.Context, l *logger.Logger, c cluster.Cluster, dbFunc func(int) *gosql.DB,
) error {
	return s.hook.fn(l, dbFunc(s.node))
}

// sequentialRunStep is a "meta-step" that indicates that a sequence
// of steps are to be executed in sequence. The default test runner
// already runs steps sequentially, and this meta-step exists
// primarily as a way to visually group related steps so that a test
// plan is easier to understand to a human.
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
// concurrently to avoid biases on a certain execution orders. While
// the Go scheduler is non-deterministic, in practice schedules are not
// uniformly distributed (as that's not the point of the scheduler),
// and the delay is inteded to expose scenarios where a specific
// ordering of events (unlikely happen if all concurrent steps were
// spawned immediately) leads to a failure.
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
		delay := time.Duration(rng.Intn(1000)+1) * time.Millisecond // delays vary from 1ms to 1s
		delayedSteps = append(delayedSteps, delayedStep{delay: delay, step: step})
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

func randomNode(rng *rand.Rand, nodes option.NodeListOption) int {
	idx := rng.Intn(len(nodes))
	return nodes[idx]
}

// Filter filters out hooks where the predicate does not return `true`
// for the test context passed.
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
// hook is run concurrently.
func (h hooks) AsSteps(
	label string, idGen func() int, rng *rand.Rand, nodes option.NodeListOption,
) []testStep {
	steps := make([]testStep, 0, len(h))
	for _, hook := range h {
		rhs := runHookStep{id: idGen(), node: randomNode(rng, nodes), hook: hook}
		steps = append(steps, rhs)
	}

	if len(steps) <= 1 {
		return steps
	}

	return []testStep{newConcurrentRunStep(label, steps, rng)}
}

func (th *testHooks) AddStartup(hook versionUpgradeHook) {
	th.startup = append(th.startup, hook)
}

func (th *testHooks) AddMixedVersion(hook versionUpgradeHook) {
	th.mixedVersion = append(th.mixedVersion, hook)
}

func (th *testHooks) AddAfterTest(hook versionUpgradeHook) {
	th.afterTest = append(th.afterTest, hook)
}

func (th *testHooks) StartupSteps(idGen func() int) []testStep {
	return th.startup.AsSteps(startupLabel, idGen, th.prng, th.crdbNodes)
}

func (th *testHooks) MixedVersionSteps(testContext Context, idGen func() int) []testStep {
	return th.mixedVersion.Filter(testContext).AsSteps(mixedVersionLabel, idGen, th.prng, th.crdbNodes)
}

func (th *testHooks) AfterTestSteps(idGen func() int) []testStep {
	return th.afterTest.AsSteps(afterTestLabel, idGen, th.prng, th.crdbNodes)
}

func versionMsg(version string) string {
	return clusterupgrade.VersionMsg(version)
}
