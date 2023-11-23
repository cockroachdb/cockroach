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
		// enabledMutators is a list of `mutator` implementations that
		// were applied when generating this test plan.
		enabledMutators []mutator
		// isLocal indicates if this test plan is generated for a `local`
		// run.
		isLocal bool
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

	// mutator describes the interface to be implemented by different
	// `mutators`.
	//
	// Mutators exist as a way to introduce optional behaviour to
	// upgrade tests in order to increase coverage of certain areas of
	// the database and of the upgrade machinery itself.
	mutator interface {
		// Name returns the unique name of this mutator. This allows the
		// test runner to log which mutators are active and test authors
		// to opt-out of mutators that are incompatible with a test, if
		// necessary.
		Name() string
		// Probability returns the probability that this mutator will run
		// in any given test run. Every mutator should run under some
		// probability. Making this an explicit part of the interface
		// makes that more prominent. Note that tests are able to override
		// this probability for specific mutator implementations as
		// needed.
		Probability() float64
		// Generate takes a test plan and a RNG and returns the list of
		// mutations that should be applied to the plan.
		Generate(*rand.Rand, *TestPlan) []mutation
	}

	// mutationOp encodes the type of mutation and controls how the
	// mutation is applied to the test plan.
	mutationOp int

	// mutation describes a change to an upgrade test plan. The change
	// is relative to a `reference` step (i.e., a step that exists in
	// the original plan). `op` encodes the operation to be
	// performed. If a new step is being added to the plan, `impl`
	// includes its implementation.
	mutation struct {
		reference *singleStep
		impl      singleStepProtocol
		op        mutationOp
	}

	// stepSelector provides a high level API for mutator
	// implementations to select the steps in the test plan that they
	// wish to mutate.
	stepSelector []*singleStep

	// singleStepInfo is the information we keep about single steps in
	// `stepIndex`. `isConcurrent` indicates whether the step is part of
	// a `concurrentRunStep`.
	singleStepInfo struct {
		step         *singleStep
		isConcurrent bool
	}

	// stepIndex contains a sequence of `singleStep`s present in a plan,
	// along with some step metadata, providing functionality to
	// determine the test context when inserting new steps in a plan.
	stepIndex []singleStepInfo
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
	ClusterSetupStage UpgradeStage = iota
	OnStartupStage
	BackgroundStage
	InitUpgradeStage
	TemporaryUpgradeStage
	RollbackUpgradeStage
	LastUpgradeStage
	RunningUpgradeMigrationsStage
	AfterUpgradeFinalizedStage

	mutationInsertBefore mutationOp = iota
	mutationInsertAfter
	mutationInsertConcurrent
	mutationRemove
)

// planMutators includes a list of all known `mutator`
// implementations. A subset of these mutations might be enabled in
// any mixedversion test plan.
var planMutators = []mutator{
	preserveDowngradeOptionRandomizerMutator{},
}

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
		isLocal:   p.isLocal,
	}

	// Probabilistically enable some of of the mutators on the base test
	// plan generated above.
	for _, mut := range planMutators {
		if p.mutatorEnabled(mut) {
			mutations := mut.Generate(p.prng, testPlan)
			testPlan.applyMutations(p.prng, mutations)
			testPlan.enabledMutators = append(testPlan.enabledMutators, mut)
		}
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
				installFixturesStep{version: initialVersion},
			),
		}
	}

	return append(steps,
		p.newSingleStep(startStep{
			version:  initialVersion,
			rt:       p.rt,
			settings: p.clusterSettings(),
		}),
		p.newSingleStep(waitForStableClusterVersionStep{
			nodes:          p.crdbNodes,
			timeout:        p.options.upgradeTimeout,
			desiredVersion: versionToClusterVersion(initialVersion),
		}),
	)
}

// startupSteps returns the list of steps that should be executed once
// the test (as defined by user-provided functions) is ready to start.
func (p *testPlanner) startupSteps() []testStep {
	p.currentContext.Stage = OnStartupStage
	return p.hooks.StartupSteps(p.longRunningContext(), p.prng, p.isLocal)
}

// testStartSteps are the user-provided steps that should run when the
// test is starting i.e., when the cluster is running and at a
// supported version.
func (p *testPlanner) testStartSteps() []testStep {
	return append(
		p.startupSteps(),
		p.hooks.BackgroundSteps(p.longRunningContext(), p.bgChans, p.prng, p.isLocal)...,
	)
}

// initUpgradeSteps returns the sequence of steps that should be
// executed before we start changing binaries on nodes in the process
// of upgrading/downgrading.
func (p *testPlanner) initUpgradeSteps() []testStep {
	p.currentContext.Stage = InitUpgradeStage
	return []testStep{p.newSingleStep(preserveDowngradeOptionStep{})}
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
		return p.hooks.AfterUpgradeFinalizedSteps(p.currentContext, p.prng, p.isLocal)
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
			steps = append(steps, p.hooks.MixedVersionSteps(p.currentContext, p.prng, p.isLocal)...)
		} else if j == waitIndex {
			// If we are not scheduling user-provided hooks, we wait a short
			// while in this state to allow some time for background
			// operations to run.
			possibleWaitDurations := []time.Duration{1 * time.Minute, 5 * time.Minute, 10 * time.Minute}
			steps = append(steps, p.newSingleStep(waitStep{
				dur: pickRandomDelay(p.prng, p.isLocal, possibleWaitDurations),
			}))
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
	runMigrations := p.newSingleStep(allowUpgradeStep{})
	var mixedVersionStepsDuringMigrations []testStep
	if scheduleHooks {
		mixedVersionStepsDuringMigrations = p.hooks.MixedVersionSteps(p.currentContext, p.prng, p.isLocal)
	}

	waitForMigrations := p.newSingleStep(
		waitForStableClusterVersionStep{
			nodes:          p.crdbNodes,
			timeout:        p.options.upgradeTimeout,
			desiredVersion: versionToClusterVersion(toVersion),
		},
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
	return newSingleStep(p.currentContext, impl, p.newRNG())
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

func (p *testPlanner) mutatorEnabled(mut mutator) bool {
	probability := mut.Probability()
	if p, ok := p.options.overriddenMutatorProbabilities[mut.Name()]; ok {
		probability = p
	}

	return p.prng.Float64() < probability
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

// mapSingleSteps iterates over every step in the test plan and calls
// the given function `f` for every `singleStep` (i.e., every step
// that actually performs an action). The function should return a
// list of testSteps that replace the given step in the plan.
func (plan *TestPlan) mapSingleSteps(f func(*singleStep, bool) []testStep) {
	var mapStep func(testStep, bool) []testStep
	mapStep = func(step testStep, isConcurrent bool) []testStep {
		switch s := step.(type) {
		case sequentialRunStep:
			var newSteps []testStep
			for _, seqStep := range s.steps {
				newSteps = append(newSteps, mapStep(seqStep, false)...)
			}
			s.steps = newSteps
			return []testStep{s}
		case concurrentRunStep:
			var newSteps []testStep
			for _, concurrentStep := range s.delayedSteps {
				ds := concurrentStep.(delayedStep)
				for _, ss := range mapStep(ds.step, true) {
					// If the function returned the original step, don't
					// generate a new delay for it.
					if ss == ds.step {
						newSteps = append(newSteps, delayedStep{delay: ds.delay, step: ss})
					} else {
						newSteps = append(newSteps, delayedStep{
							delay: randomConcurrencyDelay(s.rng, plan.isLocal), step: ss},
						)
					}
				}
			}

			// If, after mapping, our concurrentRunStep only has one step,
			// flatten it to that step alone. While it's harmless, from a
			// test execution standpoint, to leave this as-is, it's silly to
			// have a "concurrent run" of a single step, so this
			// simplification makes the test plan more understandable.
			if s.label == genericLabel && len(newSteps) == 1 {
				return []testStep{newSteps[0].(delayedStep).step}
			}

			s.delayedSteps = newSteps
			return []testStep{s}
		default:
			ss := s.(*singleStep)
			return f(ss, isConcurrent)
		}
	}

	mapSteps := func(steps []testStep) []testStep {
		var newSteps []testStep
		for _, s := range steps {
			newSteps = append(newSteps, mapStep(s, false)...)
		}

		return newSteps
	}

	mapUpgrades := func(upgrades []*upgradePlan) []*upgradePlan {
		var newUpgrades []*upgradePlan
		for _, upgrade := range upgrades {
			newUpgrades = append(newUpgrades, &upgradePlan{
				from: upgrade.from,
				to:   upgrade.to,
				sequentialStep: sequentialRunStep{
					label: upgrade.sequentialStep.label,
					steps: mapSteps(upgrade.sequentialStep.steps),
				},
			})
		}

		return newUpgrades
	}

	plan.setup.clusterSetup = mapSteps(plan.setup.clusterSetup)
	plan.setup.upgrades = mapUpgrades(plan.setup.upgrades)
	plan.initSteps = mapSteps(plan.initSteps)
	plan.upgrades = mapUpgrades(plan.upgrades)
}

func newStepIndex(plan *TestPlan) stepIndex {
	var index stepIndex

	plan.mapSingleSteps(func(ss *singleStep, isConcurrent bool) []testStep {
		index = append(index, singleStepInfo{
			step:         ss,
			isConcurrent: isConcurrent,
		})
		return []testStep{ss}
	})

	return index
}

// ContextForInsertion returns a `Context` to be used when applying
// the given `mutationOp` relative to the step passed as argument
// (which is expected to exist in the underlying step sequence).
//
// The logic in this function relies on the assumption (withheld by
// `mutator` implementations) that steps inserted by mutations do not
// change the `Context` they run in (in other words, they don't
// restart nodes with different binaries). In that case, when
// inserting a step before a given step `s`, the inserted step should
// have the same context as `s`; when inserting a step after `s`, then
// it should have the same context as the `singleStep` that runs after
// `s`.
func (si stepIndex) ContextForInsertion(step *singleStep, op mutationOp) Context {
	for j, info := range si {
		if info.step == step {
			// If this is the first or last singleStep in the plan, the
			// context is inherited from `info.step`. We also inherit the
			// same context if we are inserting the new step relative to a
			// step that is part of a concurrent group.
			if j == 0 || j == len(si)-1 || op == mutationInsertConcurrent {
				return info.step.context.clone()
			}

			var idx int
			switch op {
			case mutationInsertBefore:
				idx = j
			case mutationInsertAfter:
				idx = j + 1
			default:
				panic(fmt.Errorf("internal error: ContextForInsertion: unexpected operation %d", op))
			}

			return si[idx].step.context.clone()
		}
	}

	panic(fmt.Errorf("internal error: could not find step %#v", *step))
}

// IsConcurrent returns whether the step passed is part of a
// `concurrentRunStep`.
func (si stepIndex) IsConcurrent(step *singleStep) bool {
	for _, info := range si {
		if info.step == step {
			return info.isConcurrent
		}
	}

	panic(fmt.Errorf("internal error: could not find step %#v", *step))
}

// singleSteps returns a list of all `singleStep`s in the test plan.
func (plan *TestPlan) singleSteps() []*singleStep {
	var result []*singleStep
	plan.mapSingleSteps(func(ss *singleStep, _ bool) []testStep {
		result = append(result, ss)
		return []testStep{ss}
	})

	return result
}

// newStepSelector creates a `stepSelector` instance that can be used
// by mutators to find a specific step or set of steps. The returned
// selector will match every singleStep in the test plan.
func (plan *TestPlan) newStepSelector() stepSelector {
	return plan.singleSteps()
}

// Filter returns a new selector that only applies to steps that match
// the predicate given.
func (ss stepSelector) Filter(predicate func(*singleStep) bool) stepSelector {
	var result stepSelector
	for _, s := range ss {
		if predicate(s) {
			result = append(result, s)
		}
	}

	return result
}

// RandomStep returns a new selector that selects a single step,
// randomly chosen from the list of selected steps in the original
// selector.
func (ss stepSelector) RandomStep(rng *rand.Rand) stepSelector {
	if len(ss) > 0 {
		chosenStep := ss[rng.Intn(len(ss))]
		return stepSelector{chosenStep}
	}

	return ss
}

func (ss stepSelector) insert(impl singleStepProtocol, opGen func() mutationOp) []mutation {
	var mutations []mutation
	for _, s := range ss {
		mutations = append(mutations, mutation{
			reference: s,
			impl:      impl,
			op:        opGen(),
		})
	}

	return mutations
}

// Insert creates mutations to insert a step with the given
// implementation randomly around each selected step (before, after,
// or concurrently).
func (ss stepSelector) Insert(rng *rand.Rand, impl singleStepProtocol) []mutation {
	possibleInserts := []mutationOp{
		mutationInsertBefore,
		mutationInsertAfter,
		mutationInsertConcurrent,
	}

	return ss.insert(impl, func() mutationOp {
		return possibleInserts[rng.Intn(len(possibleInserts))]
	})
}

// InsertSequential creates mutations to insert a step with the given
// implementation sequentially relative to each selected step. The new
// step might be inserted before or after selected steps.
func (ss stepSelector) InsertSequential(rng *rand.Rand, impl singleStepProtocol) []mutation {
	possibleInserts := []mutationOp{
		mutationInsertBefore,
		mutationInsertAfter,
	}

	return ss.insert(impl, func() mutationOp {
		return possibleInserts[rng.Intn(len(possibleInserts))]
	})
}

func (ss stepSelector) InsertBefore(impl singleStepProtocol) []mutation {
	return ss.insert(impl, func() mutationOp {
		return mutationInsertBefore
	})
}

func (ss stepSelector) InsertAfter(impl singleStepProtocol) []mutation {
	return ss.insert(impl, func() mutationOp {
		return mutationInsertAfter
	})
}

func (ss stepSelector) InsertConcurrent(impl singleStepProtocol) []mutation {
	return ss.insert(impl, func() mutationOp {
		return mutationInsertConcurrent
	})
}

// Remove creates mutations to remove the steps currently selected by
// the selector.
func (ss stepSelector) Remove() []mutation {
	var mutations []mutation
	for _, s := range ss {
		mutations = append(mutations, mutation{
			reference: s,
			op:        mutationRemove,
		})
	}

	return mutations
}

// applyMutations applies each mutation passed to the test plan,
// making in place updates.
func (plan *TestPlan) applyMutations(rng *rand.Rand, mutations []mutation) {
	for _, mut := range mutationApplicationOrder(mutations) {
		plan.mapSingleSteps(func(ss *singleStep, isConcurrent bool) []testStep {
			index := newStepIndex(plan)

			// If the mutation is not relative to this step, move on.
			if ss != mut.reference {
				return []testStep{ss}
			}

			// If we are inserting a new step via this mutation, create the
			// `singleStep` here so that it is used accordingly in the
			// `switch` below.
			var newSingleStep *singleStep
			if mut.op == mutationInsertBefore ||
				mut.op == mutationInsertAfter ||
				mut.op == mutationInsertConcurrent {
				newSingleStep = &singleStep{
					context: index.ContextForInsertion(ss, mut.op),
					impl:    mut.impl,
					rng:     rngFromRNG(rng),
				}
			}

			switch mut.op {
			case mutationInsertBefore:
				return []testStep{newSingleStep, ss}
			case mutationInsertAfter:
				return []testStep{ss, newSingleStep}
			case mutationInsertConcurrent:
				steps := []testStep{ss, newSingleStep}

				// If the reference step is already part of a
				// `concurrentRunStep`, return the existing and new steps in
				// sequence; they will already be running concurrently in this
				// case, and nothing else is needed.
				if index.IsConcurrent(ss) {
					return steps
				}

				// Otherwise, create a new `concurrentRunStep` for the two
				// steps.
				return []testStep{
					newConcurrentRunStep(genericLabel, steps, rng, plan.isLocal),
				}
			case mutationRemove:
				return nil
			default:
				panic(fmt.Errorf("internal error: unknown mutation type (%d)", mut.op))
			}
		})
	}
}

// mutationApplicationOrder rearranges the collection of mutations
// passed so that all insertions happen before any removal. This is to
// avoid the situation where an insertion references a step that is
// removed by a previous mutation. This is a safe operation because
// mutators generate mutations based on a fixed view of the test plan;
// in other words, a mutation is never relative to a step created by
// a previous mutation.
func mutationApplicationOrder(mutations []mutation) []mutation {
	var insertions []mutation
	var removals []mutation

	for _, mut := range mutations {
		switch mut.op {
		case mutationRemove:
			removals = append(removals, mut)
		default:
			insertions = append(insertions, mut)
		}
	}

	return append(insertions, removals...)
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

	plan.mapSingleSteps(func(ss *singleStep, _ bool) []testStep {
		stepID := nextID()
		if _, ok := ss.impl.(startStep); ok && plan.startClusterID == 0 {
			plan.startClusterID = stepID
		}
		ss.ID = stepID
		return []testStep{ss}
	})
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

	var mutatorsDesc string
	if len(plan.enabledMutators) > 0 {
		mutatorNames := make([]string, 0, len(plan.enabledMutators))
		for _, mut := range plan.enabledMutators {
			mutatorNames = append(mutatorNames, mut.Name())
		}

		mutatorsDesc = fmt.Sprintf(" with mutators {%s}", strings.Join(mutatorNames, ", "))
	}

	return fmt.Sprintf(
		"mixed-version test plan for upgrading from %s%s:\n%s",
		strings.Join(formattedVersions, " to "), mutatorsDesc, out.String(),
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

func versionToClusterVersion(v *clusterupgrade.Version) string {
	if v.IsCurrent() {
		return clusterupgrade.CurrentVersionString
	}
	return fmt.Sprintf("'%d.%d'", v.Major(), v.Minor())
}
