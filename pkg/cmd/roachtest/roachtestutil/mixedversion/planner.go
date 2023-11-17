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

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
)

type (
	// TestPlan is the output of planning a mixed-version test.
	TestPlan struct {
		// initSteps is the sequence of steps to be performed before any
		// upgrade takes place. This involves starting the cockroach
		// process in the initial version, starting user-provided
		// background functions, etc.
		initSteps []testStep
		// startClusterID is the step ID after which the cluster should be
		// ready to receive connections.
		startClusterID int
		// upgrades is the list of upgrade plans to be performed during
		// the run encoded by this plan.
		upgrades []*upgradePlan
	}

	// testPlanner wraps the state and the logic involved in generating
	// a test plan from the given rng and user-provided hooks.
	testPlanner struct {
		stepCount      int
		startClusterID int
		versions       []*clusterupgrade.Version
		currentContext *Context
		crdbNodes      option.NodeListOption
		rt             test.Test
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
	// rollback" by doing `stage > RollbackUpgrade`.
	ClusterSetupStage = UpgradeStage(iota)
	OnStartupStage
	BackgroundStage
	InitUpgradeStage
	InitialUpgradeStage
	RollbackUpgradeStage
	FinalUpgradeStage
	RunningUpgradeMigrationsStage
	AfterUpgradeFinalizedStage
)

// Plan returns the TestPlan used to upgrade the cluster from the
// first to the final version in the `versions` field. The test plan
// roughly translates to the sequence of steps below:
//
//  1. start all nodes in the cluster at the initial version, maybe
//     using fixtures.
//  2. run startup hooks.
//  3. for each cluster upgrade:
//     - set `preserve_downgrade_option`.
//     - upgrade all nodes to the next cockroach version (running
//     mixed-version hooks at times determined by the planner).
//     - maybe downgrade all nodes back to the previous version
//     (running mixed-version hooks again).
//     - if a rollback was performed, upgrade all nodes back to the
//     next version one more time (running mixed-version hooks).
//     - reset `preserve_downgrade_option`, allowing the cluster
//     to upgrade. Mixed-version hooks may be executed while
//     this is happening.
//     - run after-upgrade hooks.
func (p *testPlanner) Plan() *TestPlan {
	initSteps := append([]testStep{}, p.testSetupSteps()...)
	initSteps = append(initSteps, p.hooks.BackgroundSteps(p.nextID, p.longRunningContext(), p.bgChans)...)

	var upgrades []*upgradePlan
	for prevVersionIdx := 0; prevVersionIdx+1 < len(p.versions); prevVersionIdx++ {
		fromVersion := p.versions[prevVersionIdx]
		toVersion := p.versions[prevVersionIdx+1]

		plan := newUpgradePlan(fromVersion, toVersion)
		p.currentContext.startUpgrade(toVersion)
		plan.Add(p.initUpgradeSteps())

		if p.shouldRollback(toVersion) {
			// previous -> next
			plan.Add(p.upgradeSteps(InitialUpgradeStage, fromVersion, toVersion))
			// next -> previous (rollback)
			plan.Add(p.downgradeSteps(toVersion, fromVersion))
			// previous -> next
			plan.Add(p.upgradeSteps(FinalUpgradeStage, fromVersion, toVersion))
		} else {
			// previous -> next
			plan.Add(p.upgradeSteps(FinalUpgradeStage, fromVersion, toVersion))
		}

		// finalize
		plan.Add(p.finalizeUpgradeSteps(fromVersion, toVersion))

		// wait for upgrade to finalize
		plan.Add(p.finalUpgradeSteps(fromVersion, toVersion))
		upgrades = append(upgrades, plan)
	}

	return &TestPlan{
		initSteps:      initSteps,
		startClusterID: p.startClusterID,
		upgrades:       upgrades,
	}
}

func (p *testPlanner) longRunningContext() *Context {
	return newLongRunningContext(
		p.versions[0], p.versions[len(p.versions)-1], p.crdbNodes, p.currentContext.Stage,
	)
}

func (p *testPlanner) testSetupSteps() []testStep {
	p.currentContext.Stage = ClusterSetupStage
	initialVersion := p.versions[0]

	var steps []testStep
	if p.prng.Float64() < p.options.useFixturesProbability {
		steps = []testStep{
			p.newSingleStep(
				installFixturesStep{id: p.nextID(), version: initialVersion, crdbNodes: p.crdbNodes},
			),
		}
	}

	p.startClusterID = p.nextID()
	steps = append(steps,
		p.newSingleStep(startStep{
			id:        p.startClusterID,
			version:   initialVersion,
			rt:        p.rt,
			crdbNodes: p.crdbNodes,
			settings:  p.clusterSettings(),
		}),
		p.newSingleStep(waitForStableClusterVersionStep{
			id: p.nextID(), nodes: p.crdbNodes, timeout: p.options.upgradeTimeout,
		}),
	)

	p.currentContext.Stage = OnStartupStage
	return append(
		steps,
		p.hooks.StartupSteps(p.nextID, p.longRunningContext())...,
	)
}

// initUpgradeSteps returns the sequence of steps that should be
// executed before we start changing binaries on nodes in the process
// of upgrading/downgrading.
func (p *testPlanner) initUpgradeSteps() []testStep {
	p.currentContext.Stage = InitUpgradeStage
	return []testStep{
		p.newSingleStep(
			preserveDowngradeOptionStep{id: p.nextID(), prng: p.newRNG(), crdbNodes: p.crdbNodes},
		),
	}
}

// finalUpgradeSteps are the steps to be run once the nodes have been
// upgraded/downgraded. It will wait for the cluster version on all
// nodes to be the same and then run any after-finalization hooks the
// user may have provided.
func (p *testPlanner) finalUpgradeSteps(fromVersion, toVersion *clusterupgrade.Version) []testStep {
	waitForUpgrade := []testStep{
		p.newSingleStep(
			waitForStableClusterVersionStep{id: p.nextID(), nodes: p.crdbNodes, timeout: p.options.upgradeTimeout},
		),
	}

	p.currentContext.Finalizing = false
	p.currentContext.Stage = AfterUpgradeFinalizedStage
	return append(
		waitForUpgrade,
		p.hooks.AfterUpgradeFinalizedSteps(p.nextID, p.currentContext)...,
	)
}

func (p *testPlanner) upgradeSteps(
	stage UpgradeStage, from, to *clusterupgrade.Version,
) []testStep {
	p.currentContext.Stage = stage
	msg := fmt.Sprintf("upgrade nodes %v from %q to %q", p.crdbNodes, from.String(), to.String())
	return p.changeVersionSteps(from, to, msg)
}

func (p *testPlanner) downgradeSteps(from, to *clusterupgrade.Version) []testStep {
	p.currentContext.Stage = RollbackUpgradeStage
	msg := fmt.Sprintf("downgrade nodes %v from %q to %q", p.crdbNodes, from.String(), to.String())
	return p.changeVersionSteps(from, to, msg)
}

// changeVersionSteps returns the sequence of steps to be performed
// when we are changing the version of cockroach in the nodes of a
// cluster (either upgrading to a new binary, or downgrading to a
// predecessor version).
func (p *testPlanner) changeVersionSteps(
	from, to *clusterupgrade.Version, label string,
) []testStep {
	// copy `crdbNodes` here so that shuffling won't mutate that array
	previousVersionNodes := append(option.NodeListOption{}, p.crdbNodes...)

	// change the binary version on the nodes in random order
	p.prng.Shuffle(len(previousVersionNodes), func(i, j int) {
		previousVersionNodes[i], previousVersionNodes[j] = previousVersionNodes[j], previousVersionNodes[i]
	})

	var steps []testStep
	for _, node := range previousVersionNodes {
		steps = append(steps, p.newSingleStep(
			restartWithNewBinaryStep{id: p.nextID(), version: to, node: node, rt: p.rt, settings: p.clusterSettings()},
		))
		p.currentContext.changeVersion(node, to)
		steps = append(steps, p.hooks.MixedVersionSteps(p.currentContext, p.nextID)...)
	}

	return []testStep{sequentialRunStep{label: label, steps: steps}}
}

// finalizeUpgradeSteps finalizes the upgrade by resetting the
// `preserve_downgrade_option` and potentially running mixed-version
// hooks while the cluster version is changing.
func (p *testPlanner) finalizeUpgradeSteps(
	fromVersion, toVersion *clusterupgrade.Version,
) []testStep {
	p.currentContext.Finalizing = true
	p.currentContext.Stage = RunningUpgradeMigrationsStage
	return append([]testStep{
		p.newSingleStep(
			finalizeUpgradeStep{id: p.nextID(), prng: p.newRNG(), crdbNodes: p.crdbNodes},
		),
	}, p.hooks.MixedVersionSteps(p.currentContext, p.nextID)...)
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

func (p *testPlanner) newSingleStep(impl singleStepProtocol) singleStep {
	return newSingleStep(p.currentContext, impl)
}

func (p *testPlanner) nextID() int {
	p.stepCount++
	return p.stepCount
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

// Steps returns a list of all steps involved in carrying out the
// entire test plan (comprised of one or multiple upgrades).
func (plan *TestPlan) Steps() []testStep {
	steps := append([]testStep{}, plan.initSteps...)
	for _, upgrade := range plan.upgrades {
		steps = append(steps, upgrade.sequentialStep)
	}

	return steps
}

// Versions returns the list of versions the cluster goes through in
// the process of running this mixed-version test.
func (plan *TestPlan) Versions() []*clusterupgrade.Version {
	result := []*clusterupgrade.Version{plan.upgrades[0].from}
	for _, upgrade := range plan.upgrades {
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
	writeSingle := func(ss singleStep, extraContext ...string) {
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
			"%s %s%s (%d)%s\n", prefix, ss.impl.Description(), extras, ss.impl.ID(), debugInfo,
		))
	}

	switch s := step.(type) {
	case sequentialRunStep:
		writeNested(s.Description(), s.steps)
	case concurrentRunStep:
		writeNested(s.Description(), s.delayedSteps)
	case delayedStep:
		delayStr := fmt.Sprintf("after %s delay", s.delay)
		writeSingle(s.step.(singleStep), delayStr)
	default:
		writeSingle(s.(singleStep))
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
	case InitialUpgradeStage:
		return "initial-upgrade"
	case RollbackUpgradeStage:
		return "rollback-upgrade"
	case FinalUpgradeStage:
		return "final-upgrade"
	case RunningUpgradeMigrationsStage:
		return "running-upgrade-migrations"
	case AfterUpgradeFinalizedStage:
		return "after-upgrade-finished"
	default:
		return fmt.Sprintf("invalid upgrade stage (%d)", u)
	}
}
