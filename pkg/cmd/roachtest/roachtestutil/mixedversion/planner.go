// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package mixedversion

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
)

type (
	// TestPlan is the output of planning a mixed-version test.
	TestPlan struct {
		// setup groups together steps that setup the cluster for a test
		// run. This involves starting the cockroach process in the
		// initial version, installing fixtures, running initial upgrades,
		// etc.
		setup testSetup
		// initSteps is the sequence of user-provided steps to be
		// performed when the test starts (i.e., the cluster is running in
		// a supported version).
		initSteps []testStep
		// startClusterID is the step ID after which the cluster should be
		// ready to receive connections.
		startClusterID int
		// upgrades is the list of upgrade plans to be performed during
		// the run encoded by this plan. These upgrades happen *after*
		// every update in `setup` is finished.
		upgrades []*upgradePlan
	}

	// testSetup includes the sequence of steps that run to setup the
	// cluster before any user-provided hooks are scheduled. There will
	// always be some `clusterSetup` steps, and the cluster might also
	// go through a few upgrades (from older, unsupported versions)
	// before the test logic runs.
	testSetup struct {
		clusterSetup []testStep
		upgrades     []*upgradePlan
	}

	// testPlanner wraps the state and the logic involved in generating
	// a test plan from the given rng and user-provided hooks.
	testPlanner struct {
		versions       []*clusterupgrade.Version
		currentContext *Context
		crdbNodes      option.NodeListOption
		rt             test.Test
		isLocal        bool
		options        testOptions
		hooks          *testHooks
		prng           *rand.Rand
		bgChans        []shouldStop
	}

	// UpgradeStage encodes in what part of an upgrade a test step is in
	// (i.e., before upgrade, doing initial upgrade, rolling back, etc.)
	UpgradeStage int

	upgradePlan struct {
		from           *clusterupgrade.Version
		to             *clusterupgrade.Version
		sequentialStep sequentialRunStep
	}
)

const (
	branchString       = "├──"
	nestedBranchString = "│   "
	lastBranchString   = "└──"
	lastBranchPadding  = "   "
)

const (
	// Upgrade stages are defined in the order they happen during test
	// runs, so that we are able to select, for example, "stages after
	// rollback" by doing `stage > RollbackUpgrade`.  Note that
	// `BackgroundStage` is special in the sense that it may continue to
	// run across stage changes, so doing direct stage comparisons as
	// mentioned above doesn't make sense for background functions.
	ClusterSetupStage = UpgradeStage(iota)
	OnStartupStage
	BackgroundStage
	InitUpgradeStage
	TemporaryUpgradeStage
	RollbackUpgradeStage
	LastUpgradeStage
	RunningUpgradeMigrationsStage
	AfterUpgradeFinalizedStage
)

// Plan returns the TestPlan used to upgrade the cluster from the
// first to the final version in the `versions` field. The test plan
// roughly translates to the sequence of steps below, annotated with
// the respective `UpgradeStage` that they run in:
//
//  1. ClusterSetupStage: start all nodes in the cluster at the initial version,
//     maybe using fixtures.
//  2. OnStartupStage: run startup hooks.
//  3. for each cluster upgrade:
//     - InitUpgradeStage: set `preserve_downgrade_option`.
//     - TemporaryUpgradeStage: upgrade all nodes to the next cockroach version
//     (running mixed-version hooks at times determined by the planner). This
//     stage only applies if the planner decides to rollback.
//     - RollbackUpgradeStage: downgrade all nodes back to the previous
//     version (running mixed-version hooks again). This stage may not happen.
//     - LastUpgradeStage: upgrade all nodes to the next version (running
//     mixed-version hooks). The upgrade will not be rolled back.
//     - RunningUpgradeMigrationsStage: reset `preserve_downgrade_option`,
//     allowing the cluster version to advance. Mixed-version hooks may be
//     executed while this is happening.
//     - AfterUpgradeFinalizedStage: run after-upgrade hooks.
func (p *testPlanner) Plan() *TestPlan {
	setup := testSetup{clusterSetup: p.clusterSetupSteps()}

	var testUpgrades []*upgradePlan
	for prevVersionIdx := 0; prevVersionIdx+1 < len(p.versions); prevVersionIdx++ {
		fromVersion := p.versions[prevVersionIdx]
		toVersion := p.versions[prevVersionIdx+1]

		// Only arrange for user-hooks to run if we are upgrading from a
		// supported version.
		scheduleHooks := fromVersion.AtLeast(p.options.minimumSupportedVersion)

		plan := newUpgradePlan(fromVersion, toVersion)
		p.currentContext.startUpgrade(toVersion)
		plan.Add(p.initUpgradeSteps())
		if p.shouldRollback(toVersion) {
			// previous -> next
			plan.Add(p.upgradeSteps(TemporaryUpgradeStage, fromVersion, toVersion, scheduleHooks))
			// next -> previous (rollback)
			plan.Add(p.downgradeSteps(toVersion, fromVersion, scheduleHooks))
		}
		// previous -> next
		plan.Add(p.upgradeSteps(LastUpgradeStage, fromVersion, toVersion, scheduleHooks))

		// finalize -- i.e., run upgrade migrations.
		plan.Add(p.finalizeUpgradeSteps(fromVersion, toVersion, scheduleHooks))

		// run after upgrade steps, if any,
		plan.Add(p.afterUpgradeSteps(fromVersion, toVersion, scheduleHooks))

		if scheduleHooks {
			testUpgrades = append(testUpgrades, plan)
		} else {
			setup.upgrades = append(setup.upgrades, plan)
		}
	}

	testPlan := &TestPlan{
		setup:     setup,
		initSteps: p.testStartSteps(),
		upgrades:  testUpgrades,
	}

	testPlan.assignIDs()
	return testPlan
}

func (p *testPlanner) longRunningContext() *Context {
	return newLongRunningContext(
		p.versions[0], p.versions[len(p.versions)-1], p.crdbNodes, p.currentContext.Stage,
	)
}

func (p *testPlanner) clusterSetupSteps() []testStep {
	p.currentContext.Stage = ClusterSetupStage
	initialVersion := p.versions[0]

	var steps []testStep
	if p.prng.Float64() < p.options.useFixturesProbability {
		steps = []testStep{
			p.newSingleStep(
				installFixturesStep{version: initialVersion, crdbNodes: p.crdbNodes},
			),
		}
	}

	return append(steps,
		p.newSingleStep(startStep{
			version:   initialVersion,
			rt:        p.rt,
			crdbNodes: p.crdbNodes,
			settings:  p.clusterSettings(),
		}),
		p.newSingleStep(waitForStableClusterVersionStep{
			nodes: p.crdbNodes, timeout: p.options.upgradeTimeout,
		}),
	)
}

// startupSteps returns the list of steps that should be executed once
// the test (as defined by user-provided functions) is ready to start.
func (p *testPlanner) startupSteps() []testStep {
	p.currentContext.Stage = OnStartupStage
	return p.hooks.StartupSteps(p.longRunningContext())
}

// testStartSteps are the user-provided steps that should run when the
// test is starting i.e., when the cluster is running and at a
// supported version.
func (p *testPlanner) testStartSteps() []testStep {
	return append(
		p.startupSteps(),
		p.hooks.BackgroundSteps(p.longRunningContext(), p.bgChans)...,
	)
}

// initUpgradeSteps returns the sequence of steps that should be
// executed before we start changing binaries on nodes in the process
// of upgrading/downgrading.
func (p *testPlanner) initUpgradeSteps() []testStep {
	p.currentContext.Stage = InitUpgradeStage
	return []testStep{
		p.newSingleStep(
			preserveDowngradeOptionStep{prng: p.newRNG(), crdbNodes: p.crdbNodes},
		),
	}
}

// afterUpgradeSteps are the steps to be run once the nodes have been
// upgraded. It will wait for the cluster version on all nodes to be
// the same and then run any after-finalization hooks the user may
// have provided.
func (p *testPlanner) afterUpgradeSteps(
	fromVersion, toVersion *clusterupgrade.Version, scheduleHooks bool,
) []testStep {
	p.currentContext.Finalizing = false
	p.currentContext.Stage = AfterUpgradeFinalizedStage
	if scheduleHooks {
		return p.hooks.AfterUpgradeFinalizedSteps(p.currentContext)
	}

	// Currently, we only schedule user-provided hooks after the upgrade
	// is finalized; if we are not scheduling hooks, return a nil slice.
	return nil
}

func (p *testPlanner) upgradeSteps(
	stage UpgradeStage, from, to *clusterupgrade.Version, scheduleHooks bool,
) []testStep {
	p.currentContext.Stage = stage
	msg := fmt.Sprintf("upgrade nodes %v from %q to %q", p.crdbNodes, from.String(), to.String())
	return p.changeVersionSteps(from, to, msg, scheduleHooks)
}

func (p *testPlanner) downgradeSteps(
	from, to *clusterupgrade.Version, scheduleHooks bool,
) []testStep {
	p.currentContext.Stage = RollbackUpgradeStage
	msg := fmt.Sprintf("downgrade nodes %v from %q to %q", p.crdbNodes, from.String(), to.String())
	return p.changeVersionSteps(from, to, msg, scheduleHooks)
}

// changeVersionSteps returns the sequence of steps to be performed
// when we are changing the version of cockroach in the nodes of a
// cluster (either upgrading to a new binary, or downgrading to a
// predecessor version). If `scheduleHooks` is true, user-provided
// mixed-version hooks will be scheduled randomly during the
// upgrade/downgrade process.
func (p *testPlanner) changeVersionSteps(
	from, to *clusterupgrade.Version, label string, scheduleHooks bool,
) []testStep {
	// copy `crdbNodes` here so that shuffling won't mutate that array
	previousVersionNodes := append(option.NodeListOption{}, p.crdbNodes...)

	// change the binary version on the nodes in random order
	p.prng.Shuffle(len(previousVersionNodes), func(i, j int) {
		previousVersionNodes[i], previousVersionNodes[j] = previousVersionNodes[j], previousVersionNodes[i]
	})

	// waitProbability is the probability that we will wait and allow
	// the cluster to stay in the current state for a while, in case we
	// are not scheduling user hooks.
	const waitProbability = 0.5
	waitIndex := -1
	if !scheduleHooks && p.prng.Float64() < waitProbability {
		waitIndex = p.prng.Intn(len(previousVersionNodes))
	}

	var steps []testStep
	for j, node := range previousVersionNodes {
		steps = append(steps, p.newSingleStep(
			restartWithNewBinaryStep{version: to, node: node, rt: p.rt, settings: p.clusterSettings()},
		))
		p.currentContext.changeVersion(node, to)
		if scheduleHooks {
			steps = append(steps, p.hooks.MixedVersionSteps(p.currentContext)...)
		} else if j == waitIndex {
			// If we are not scheduling user-provided hooks, we wait a short
			// while in this state to allow some time for background
			// operations to run.
			possibleWaitMinutes := []int{1, 5, 10}
			waitDur := time.Duration(possibleWaitMinutes[p.prng.Intn(len(possibleWaitMinutes))]) * time.Minute
			if p.isLocal {
				// Reduce wait duration in local runs, as some tests run as
				// part of CI and we can't to spend too much time waiting in
				// that context.
				waitDur = waitDur / 10
			}
			steps = append(steps, p.newSingleStep(waitStep{dur: waitDur}))
		}
	}

	return []testStep{sequentialRunStep{label: label, steps: steps}}
}

// finalizeUpgradeSteps finalizes the upgrade by resetting the
// `preserve_downgrade_option` and potentially running mixed-version
// hooks while the cluster version is changing. At the end of this
// process, we wait for all the migrations to finish running.
func (p *testPlanner) finalizeUpgradeSteps(
	fromVersion, toVersion *clusterupgrade.Version, scheduleHooks bool,
) []testStep {
	p.currentContext.Finalizing = true
	p.currentContext.Stage = RunningUpgradeMigrationsStage
	runMigrations := p.newSingleStep(
		finalizeUpgradeStep{prng: p.newRNG(), crdbNodes: p.crdbNodes},
	)
	var mixedVersionStepsDuringMigrations []testStep
	if scheduleHooks {
		mixedVersionStepsDuringMigrations = p.hooks.MixedVersionSteps(p.currentContext)
	}
	waitForMigrations := p.newSingleStep(
		waitForStableClusterVersionStep{nodes: p.crdbNodes, timeout: p.options.upgradeTimeout},
	)

	return append(
		append(
			[]testStep{runMigrations}, mixedVersionStepsDuringMigrations...,
		),
		waitForMigrations,
	)
}

// shouldRollback returns whether the test will attempt a rollback. If
// we are upgrading to the current version being tested, we always
// rollback, as we want to expose that upgrade to complex upgrade
// scenarios. If this is an intermediate upgrade in a multi-upgrade
// test, then rollback with a probability. This stops tests from
// having excessively long running times.
func (p *testPlanner) shouldRollback(toVersion *clusterupgrade.Version) bool {
	if toVersion.IsCurrent() {
		return p.prng.Float64() < rollbackFinalUpgradeProbability
	}

	return p.prng.Float64() < rollbackIntermediateUpgradesProbability
}

func (p *testPlanner) newSingleStep(impl singleStepProtocol) *singleStep {
	return newSingleStep(p.currentContext, impl)
}

func (p *testPlanner) clusterSettings() []install.ClusterSettingOption {
	cs := []install.ClusterSettingOption{}
	cs = append(cs, defaultClusterSettings...)
	cs = append(cs, p.options.settings...)
	return cs
}

func (p *testPlanner) newRNG() *rand.Rand {
	return rngFromRNG(p.prng)
}

func newUpgradePlan(from, to *clusterupgrade.Version) *upgradePlan {
	return &upgradePlan{
		from: from,
		to:   to,
		sequentialStep: sequentialRunStep{
			label: fmt.Sprintf("upgrade cluster from %q to %q", from.String(), to.String()),
		},
	}
}

func (up *upgradePlan) Add(steps []testStep) {
	up.sequentialStep.steps = append(up.sequentialStep.steps, steps...)
}

// assignIDs iterates over each `singleStep` in the test plan, and
// assigns them a unique numeric ID. These IDs are not necessary for
// correctness, but are nice to have when debugging failures and
// matching output from a step to where it happens in the test plan.
func (plan *TestPlan) assignIDs() {
	var currentID int
	nextID := func() int {
		currentID++
		return currentID
	}

	var assignIDsToSteps func([]testStep)
	assignIDsToSteps = func(steps []testStep) {
		for _, step := range steps {
			switch s := step.(type) {
			case sequentialRunStep:
				assignIDsToSteps(s.steps)
			case concurrentRunStep:
				assignIDsToSteps(s.delayedSteps)
			case delayedStep:
				assignIDsToSteps([]testStep{s.step})
			default:
				ss := s.(*singleStep)
				stepID := nextID()
				if _, ok := ss.impl.(startStep); ok && plan.startClusterID == 0 {
					plan.startClusterID = stepID
				}

				ss.ID = stepID
			}
		}
	}

	assignIDsToSteps(plan.setup.clusterSetup)
	for _, upgrade := range plan.setup.upgrades {
		assignIDsToSteps(upgrade.sequentialStep.steps)
	}

	assignIDsToSteps(plan.initSteps)
	for _, upgrade := range plan.upgrades {
		assignIDsToSteps(upgrade.sequentialStep.steps)
	}
}

// allUpgrades returns a list of all upgrades encoded in this test
// plan, including the ones that are run as part of test setup (i.e.,
// before any user-provided test logic is scheduled).
func (plan *TestPlan) allUpgrades() []*upgradePlan {
	return append(
		append([]*upgradePlan{}, plan.setup.upgrades...),
		plan.upgrades...,
	)
}

// Steps returns a list of all steps involved in carrying out the
// entire test plan (comprised of one or multiple upgrades).
func (plan *TestPlan) Steps() []testStep {
	// 1. Set up the cluster (install fixtures, start binaries, etc)
	steps := append([]testStep{}, plan.setup.clusterSetup...)

	// 2. Run through any setup upgrades, if any.
	for _, upgrade := range plan.setup.upgrades {
		steps = append(steps, upgrade.sequentialStep)
	}

	// 3. Run user provided initial steps.
	steps = append(steps, plan.initSteps...)

	// 4. Run upgrades with user-provided hooks.
	for _, upgrade := range plan.upgrades {
		steps = append(steps, upgrade.sequentialStep)
	}

	return steps
}

// Versions returns the list of versions the cluster goes through in
// the process of running this mixed-version test.
func (plan *TestPlan) Versions() []*clusterupgrade.Version {
	upgrades := plan.allUpgrades()
	result := []*clusterupgrade.Version{upgrades[0].from}
	for _, upgrade := range upgrades {
		result = append(result, upgrade.to)
	}

	return result
}

// PrettyPrint displays a tree-like view of the mixed-version test
// plan, useful when debugging mixed-version test failures. Each step
// is assigned an ID, making it easy to correlate the step that
// failed with the point in the test where the failure occurred. See
// testdata/ for examples of the output of this function.
func (plan *TestPlan) PrettyPrint() string {
	return plan.prettyPrintInternal(false /* debug */)
}

func (plan *TestPlan) prettyPrintInternal(debug bool) string {
	var out strings.Builder
	allSteps := plan.Steps()
	for i, step := range allSteps {
		plan.prettyPrintStep(&out, step, treeBranchString(i, len(allSteps)), debug)
	}

	versions := plan.Versions()
	formattedVersions := make([]string, 0, len(versions))
	for _, v := range versions {
		formattedVersions = append(formattedVersions, fmt.Sprintf("%q", v.String()))
	}

	return fmt.Sprintf(
		"mixed-version test plan for upgrading from %s:\n%s",
		strings.Join(formattedVersions, " to "), out.String(),
	)
}

func (plan *TestPlan) prettyPrintStep(
	out *strings.Builder, step testStep, prefix string, debug bool,
) {
	writeNested := func(label string, steps []testStep) {
		out.WriteString(fmt.Sprintf("%s %s\n", prefix, label))
		for i, subStep := range steps {
			nestedPrefix := strings.ReplaceAll(prefix, branchString, nestedBranchString)
			nestedPrefix = strings.ReplaceAll(nestedPrefix, lastBranchString, lastBranchPadding)
			subPrefix := fmt.Sprintf("%s%s", nestedPrefix, treeBranchString(i, len(steps)))
			plan.prettyPrintStep(out, subStep, subPrefix, debug)
		}
	}

	// writeSingle is the function that generates the description for
	// a singleStep. It can include extra information, such as whether
	// there's a delay associated with the step (in the case of
	// concurrent execution), and what database node the step is
	// connecting to.
	writeSingle := func(ss *singleStep, extraContext ...string) {
		var extras string
		if contextStr := strings.Join(extraContext, ", "); contextStr != "" {
			extras = ", " + contextStr
		}

		// Include debug information if we are in debug mode.
		var debugInfo string
		if debug {
			debugInfo = fmt.Sprintf(" [stage=%s]", ss.context.Stage)
		}

		out.WriteString(fmt.Sprintf(
			"%s %s%s (%d)%s\n", prefix, ss.impl.Description(), extras, ss.ID, debugInfo,
		))
	}

	switch s := step.(type) {
	case sequentialRunStep:
		writeNested(s.Description(), s.steps)
	case concurrentRunStep:
		writeNested(s.Description(), s.delayedSteps)
	case delayedStep:
		delayStr := fmt.Sprintf("after %s delay", s.delay)
		writeSingle(s.step.(*singleStep), delayStr)
	default:
		writeSingle(s.(*singleStep))
	}
}

func treeBranchString(idx, sliceLen int) string {
	if idx == sliceLen-1 {
		return lastBranchString
	}

	return branchString
}

func (u UpgradeStage) String() string {
	switch u {
	case ClusterSetupStage:
		return "cluster-setup"
	case OnStartupStage:
		return "on-startup"
	case BackgroundStage:
		return "background"
	case InitUpgradeStage:
		return "init"
	case TemporaryUpgradeStage:
		return "temporary-upgrade"
	case RollbackUpgradeStage:
		return "rollback-upgrade"
	case LastUpgradeStage:
		return "last-upgrade"
	case RunningUpgradeMigrationsStage:
		return "running-upgrade-migrations"
	case AfterUpgradeFinalizedStage:
		return "after-upgrade-finished"
	default:
		return fmt.Sprintf("invalid upgrade stage (%d)", u)
	}
}
