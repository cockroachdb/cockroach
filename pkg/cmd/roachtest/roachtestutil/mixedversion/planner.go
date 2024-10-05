// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mixedversion

import (
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

type (
	// TestPlan is the output of planning a mixed-version test.
	TestPlan struct {
		// seed is the seed used to generate this test plan.
		seed int64
		// services is the list of services being upgraded as part of this
		// test plan.
		services []*ServiceDescriptor
		// setup groups together steps that setup the cluster for a test
		// run. This involves starting the cockroach process in the
		// initial version, installing fixtures, running initial upgrades,
		// etc.
		setup testSetup
		// initSteps is the sequence of user-provided steps to be
		// performed when the test starts (i.e., the cluster is running in
		// a supported version).
		initSteps []testStep
		// startSystemID is the step ID after which the system tenant
		// should be ready to receive connections.
		startSystemID int
		// startTenantID is the step ID after which the secondary tenant,
		// if any, should be ready to receive connections.
		startTenantID int
		// upgrades is the list of upgrade plans to be performed during
		// the run encoded by this plan. These upgrades happen *after*
		// every update in `setup` is finished.
		upgrades []*upgradePlan
		// deploymentMode is the associated deployment mode used when
		// generating this test plan.
		deploymentMode DeploymentMode
		// enabledMutators is a list of `mutator` implementations that
		// were applied when generating this test plan.
		enabledMutators []mutator
		// isLocal indicates if this test plan is generated for a `local`
		// run.
		isLocal bool
	}

	// serviceSetup encapsulates the steps to setup a service in the
	// test (system or tenant). The `steps` field encode how to start
	// the service on the cluster. The list of `upgrades` contains setup
	// upgrades that may run after the service is setup (and before the
	// cluster reaches the desired minimum version).
	serviceSetup struct {
		steps    []testStep
		upgrades []*upgradePlan
	}

	// testSetup includes the sequence of steps that run to setup the
	// cluster before any user-provided hooks are scheduled. These are
	// divided into `systemSetup` (sets up the storage cluster, always
	// runs first) and `tenantSetup` (creates tenants in multitenant
	// deployments).
	testSetup struct {
		systemSetup *serviceSetup
		tenantSetup *serviceSetup
	}

	// testPlanner wraps the state and the logic involved in generating
	// a test plan from the given rng and user-provided hooks.
	testPlanner struct {
		versions       []*clusterupgrade.Version
		deploymentMode DeploymentMode
		seed           int64
		currentContext *Context
		rt             test.Test
		isLocal        bool
		options        testOptions
		hooks          *testHooks
		prng           *rand.Rand
		bgChans        []shouldStop

		// State variables updated as the test plan is generated.
		usingFixtures bool
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
	SystemSetupStage UpgradeStage = iota
	TenantSetupStage
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
	newClusterSettingMutator(
		"kv.expiration_leases_only.enabled",
		[]bool{true, false},
		clusterSettingMinimumVersion("v23.1.0"),
	),
}

// Plan returns the TestPlan used to upgrade the cluster from the
// first to the final version in the `versions` field. The test plan
// roughly translates to the sequence of steps below, annotated with
// the respective `UpgradeStage` that they run in:
//
//  1. SystemSetupStage: start all nodes in the cluster at the initial version,
//     maybe using fixtures. Some upgrades may take place here from older
//     versions, as we make our way to the test's minimum supported version.
//  2. TenantSetupStage: creates tenants (if running in a multitenant
//     deployment mode). May also run some setup upgrades if the cluster
//     is not yet at the minimum supported version.
//  3. OnStartupStage: run startup hooks.
//  4. for each cluster upgrade:
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
	setup, testUpgrades := p.setupTest()

	planUpgrade := func(upgrade *upgradePlan, virtualClusterSetup, scheduleHooks bool) {
		for _, s := range p.services() {
			s.startUpgrade(upgrade.to)
		}
		upgrade.Add(p.initUpgradeSteps(virtualClusterSetup))
		if p.shouldRollback(upgrade.to) {
			// previous -> next
			upgrade.Add(p.upgradeSteps(
				TemporaryUpgradeStage, upgrade.from, upgrade.to, scheduleHooks, virtualClusterSetup,
			))
			// next -> previous (rollback)
			upgrade.Add(p.downgradeSteps(
				upgrade.to, upgrade.from, scheduleHooks, virtualClusterSetup,
			))
		}
		// previous -> next
		upgrade.Add(p.upgradeSteps(
			LastUpgradeStage, upgrade.from, upgrade.to, scheduleHooks, virtualClusterSetup,
		))

		// finalize -- i.e., run upgrade migrations.
		upgrade.Add(p.finalizeUpgradeSteps(upgrade.from, upgrade.to, scheduleHooks, virtualClusterSetup))

		// run after upgrade steps, if any,
		upgrade.Add(p.afterUpgradeSteps(upgrade.from, upgrade.to, scheduleHooks))
	}

	for _, upgrade := range setup.systemSetup.upgrades {
		planUpgrade(
			upgrade,
			// We are performing setup upgrades after the system service is
			// started; at this point, there is not virtual cluster configured.
			false,
			// These are setup upgrades; user-provided functions do not run.
			false,
		)
	}

	if p.isMultitenant() {
		for _, upgrade := range setup.tenantSetup.upgrades {
			planUpgrade(
				upgrade,
				// These setup upgrades happen after our tenant service is
				// started, so they should upgrade the tenants as well.
				true,
				// These are setup upgrades; user-provided functions do not run.
				false,
			)
		}
	}

	for _, upgrade := range testUpgrades {
		planUpgrade(upgrade,
			// These are actual tested upgrades; both system and tenant
			// should be operating at this point.
			p.isMultitenant(),
			// We want to run user-provided functions during these upgrades.
			true,
		)
	}

	// Use first tested upgrade version in the context passed to startup
	// functions; the setup upgrades that happened before should be
	// invisible to them.
	firstTestedUpgradeVersion := testUpgrades[0].from

	testPlan := &TestPlan{
		seed:           p.seed,
		services:       p.serviceDescriptors(),
		setup:          setup,
		initSteps:      p.testStartSteps(firstTestedUpgradeVersion),
		upgrades:       testUpgrades,
		deploymentMode: p.deploymentMode,
		isLocal:        p.isLocal,
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

// nonUpgradeContext builds a mixed-version context to be used during
// steps that happen outside of the context of a particular
// upgrade. In this situation, the `ToVersion` in the context is
// undefined; the passed `fromVersion`, however, should represent the
// version running when the corresponding functions starts running.
func (p *testPlanner) nonUpgradeContext(
	fromVersion *clusterupgrade.Version, stage UpgradeStage,
) *Context {
	var tenantDescriptor *ServiceDescriptor
	if p.currentContext.Tenant != nil {
		tenantDescriptor = p.currentContext.Tenant.Descriptor
	}

	return newContext(
		fromVersion, p.versions[len(p.versions)-1],
		stage,
		p.currentContext.DefaultService().Descriptor.Nodes,
		tenantDescriptor,
	)
}

// buildUpgrades constructs a list of `upgradePlan` structs for each
// upgrade being performed during this test run (whether the upgrade
// is a part of setup or not). The actual steps to be performed during
// each upgrade are computed later.
func (p *testPlanner) buildUpgrades() []*upgradePlan {
	var upgrades []*upgradePlan
	for prevVersionIdx := 0; prevVersionIdx+1 < len(p.versions); prevVersionIdx++ {
		fromVersion := p.versions[prevVersionIdx]
		toVersion := p.versions[prevVersionIdx+1]

		upgrades = append(upgrades, newUpgradePlan(fromVersion, toVersion))
	}

	return upgrades
}

// setupTest generates a `testSetup` struct, encapsulating the
// sequence of steps to be performed to setup the cluster, before any
// user-provided function runs. The list of `upgradePlan` returned
// corresponds to the upgrades that *are* part of the test (i.e., the
// upgrades where user functions will run).
//
// The setup process is comprised of, at a high level:
//
// * start (storage) cluster at initial version.
// * perform setup upgrades from initial version to some version v (optional).
// * start tenant cluster (at version v).
// * perform more setup upgrades from v to the minimum supported version (optional).
func (p *testPlanner) setupTest() (testSetup, []*upgradePlan) {
	allUpgrades := p.buildUpgrades()

	// Find which upgrades are part of setup and which upgrades will be
	// tested, based on the test's minimum supported version.
	idx := slices.IndexFunc(allUpgrades, func(upgrade *upgradePlan) bool {
		return upgrade.from.AtLeast(p.options.minimumSupportedVersion)
	})
	setupUpgrades := allUpgrades[:idx]
	testUpgrades := allUpgrades[idx:]

	// By default, we perform all setup upgrades after the storage
	// cluster is created.
	upgradesAfterSystemSetup := setupUpgrades
	var upgradesAfterTenantSetup []*upgradePlan
	tenantBootstrapVersion := allUpgrades[0].from

	if len(setupUpgrades) > 0 && p.isMultitenant() {
		// If we are in a multi-tenant deployment and we have some setup
		// upgrades to run, we find the first setup upgrade before which a
		// tenant can be started.
		firstSupportedUpgradeIdx := slices.IndexFunc(setupUpgrades, func(upgrade *upgradePlan) bool {
			return upgrade.from.AtLeast(OldestSupportedVersionVC)
		})

		if firstSupportedUpgradeIdx == -1 {
			// If there is no such upgrade, it means that the tenant will be
			// created after the last setup upgrade.
			tenantBootstrapVersion = setupUpgrades[len(setupUpgrades)-1].to
		} else {
			// If there *is* a setup upgrade where we could create a tenant,
			// we pick a random point during the setup upgrades in which we
			// will create our tenant.
			upgradesAfterSystemSetupIdx := randutil.RandIntInRange(
				p.prng, firstSupportedUpgradeIdx, len(setupUpgrades),
			)

			// We then split the setup upgrades based on whether they run
			// after the storage cluster is created, or after the tenant is
			// created.
			upgradesAfterSystemSetup = setupUpgrades[:upgradesAfterSystemSetupIdx]
			upgradesAfterTenantSetup = setupUpgrades[upgradesAfterSystemSetupIdx:]
			tenantBootstrapVersion = setupUpgrades[upgradesAfterSystemSetupIdx].from
		}
	}

	systemSetup := &serviceSetup{
		steps:    p.systemSetupSteps(),
		upgrades: upgradesAfterSystemSetup,
	}

	var tenantSetup *serviceSetup
	if p.isMultitenant() {
		tenantSetup = &serviceSetup{
			steps:    p.tenantSetupSteps(tenantBootstrapVersion),
			upgrades: upgradesAfterTenantSetup,
		}
	}

	return testSetup{systemSetup, tenantSetup}, testUpgrades
}

func (p *testPlanner) systemSetupSteps() []testStep {
	initialVersion := p.versions[0]

	var steps []testStep
	p.usingFixtures = p.prng.Float64() < p.options.useFixturesProbability
	if p.usingFixtures {
		steps = []testStep{
			p.newSingleStep(
				installFixturesStep{version: initialVersion},
			),
		}
	}

	setupContext := p.nonUpgradeContext(initialVersion, SystemSetupStage)
	return append(steps,
		p.newSingleStepWithContext(setupContext, startStep{
			version:    initialVersion,
			rt:         p.rt,
			initTarget: p.currentContext.System.Descriptor.Nodes[0],
			settings:   p.clusterSettingsForSystem(),
		}),
		p.newSingleStepWithContext(setupContext, waitForStableClusterVersionStep{
			nodes:              p.currentContext.System.Descriptor.Nodes,
			timeout:            p.options.upgradeTimeout,
			desiredVersion:     versionToClusterVersion(initialVersion),
			virtualClusterName: install.SystemInterfaceName,
		}),
	)
}

// tenantSetupSteps returns the series of steps needed to create the
// tenant that will be used during this test run. The version `v`
// passed is the version in which the tenant is created.
func (p *testPlanner) tenantSetupSteps(v *clusterupgrade.Version) []testStep {
	setupContext := p.nonUpgradeContext(v, TenantSetupStage)
	shouldGrantCapabilities := p.deploymentMode == SharedProcessDeployment &&
		!v.AtLeast(TenantsAndSystemAlignedSettingsVersion)

	// In case we are starting a shared-process virtual cluster, create
	// it, wait for the cluster version to match the expected version,
	// set it as the default cluster, and give it all capabilities if
	// necessary.
	steps := []testStep{
		p.newSingleStepWithContext(setupContext, startSharedProcessVirtualClusterStep{
			name:       p.tenantName(),
			initTarget: p.currentContext.Tenant.Descriptor.Nodes[0],
			settings:   p.clusterSettingsForTenant(),
		}),
		p.newSingleStepWithContext(setupContext, waitForStableClusterVersionStep{
			nodes:              p.currentContext.Tenant.Descriptor.Nodes,
			timeout:            p.options.upgradeTimeout,
			desiredVersion:     versionToClusterVersion(v),
			virtualClusterName: p.tenantName(),
		}),
		p.newSingleStepWithContext(setupContext, setClusterSettingStep{
			name:               defaultTenantClusterSetting(v),
			value:              p.tenantName(),
			virtualClusterName: install.SystemInterfaceName,
		}),
	}

	if shouldGrantCapabilities {
		steps = append(steps, p.newSingleStepWithContext(setupContext, setClusterSettingStep{
			name:               "server.secondary_tenants.authorization.mode",
			value:              "allow-all",
			virtualClusterName: install.SystemInterfaceName,
		}))
	}

	// If we are using fixtures, the system.tenant_settings table will
	// have bad data (`version` override for all tenants.) Only
	// versions 24.2+ are currently equipped to handle this situation,
	// so we manually delete the bad data for now.
	if p.usingFixtures && !v.AtLeast(tenantSettingsVersionOverrideFixVersion) {
		steps = append(steps, p.newSingleStepWithContext(
			setupContext, deleteAllTenantsVersionOverrideStep{},
		))
	}

	return steps
}

// startupSteps returns the list of steps that should be executed once
// the test (as defined by user-provided functions) is ready to start.
func (p *testPlanner) startupSteps(firstUpgradeVersion *clusterupgrade.Version) []testStep {
	return p.hooks.StartupSteps(
		p.nonUpgradeContext(firstUpgradeVersion, OnStartupStage), p.prng, p.isLocal,
	)
}

// testStartSteps are the user-provided steps that should run when the
// test is starting i.e., when the cluster is running and at a
// supported version.
func (p *testPlanner) testStartSteps(firstUpgradeVersion *clusterupgrade.Version) []testStep {
	return append(
		p.startupSteps(firstUpgradeVersion),
		p.hooks.BackgroundSteps(
			p.nonUpgradeContext(firstUpgradeVersion, OnStartupStage), p.bgChans, p.prng, p.isLocal,
		)...,
	)
}

// initUpgradeSteps returns the sequence of steps that should be
// executed before we start changing binaries on nodes in the process
// of upgrading/downgrading.
func (p *testPlanner) initUpgradeSteps(virtualClusterSetup bool) []testStep {
	p.currentContext.SetStage(InitUpgradeStage)

	preserveDowngradeForService := func(name string) *singleStep {
		return p.newSingleStep(preserveDowngradeOptionStep{
			virtualClusterName: name,
		})
	}

	steps := []testStep{preserveDowngradeForService(install.SystemInterfaceName)}

	switch p.deploymentMode {
	case SharedProcessDeployment:
		if virtualClusterSetup {
			steps = append(steps, preserveDowngradeForService(p.tenantName()))
		}
	}

	return steps
}

// afterUpgradeSteps are the steps to be run once the nodes have been
// upgraded. It will wait for the cluster version on all nodes to be
// the same and then run any after-finalization hooks the user may
// have provided.
func (p *testPlanner) afterUpgradeSteps(
	fromVersion, toVersion *clusterupgrade.Version, scheduleHooks bool,
) []testStep {
	p.currentContext.SetFinalizing(false)
	p.currentContext.SetStage(AfterUpgradeFinalizedStage)
	if scheduleHooks {
		return p.hooks.AfterUpgradeFinalizedSteps(p.currentContext, p.prng, p.isLocal)
	}

	// Currently, we only schedule user-provided hooks after the upgrade
	// is finalized; if we are not scheduling hooks, return a nil slice.
	return nil
}

func (p *testPlanner) upgradeSteps(
	stage UpgradeStage, from, to *clusterupgrade.Version, scheduleHooks, virtualClusterSetup bool,
) []testStep {
	p.currentContext.SetStage(stage)
	nodes := p.currentContext.System.Descriptor.Nodes
	msg := fmt.Sprintf("upgrade nodes %v from %q to %q", nodes, from.String(), to.String())
	return p.changeVersionSteps(from, to, msg, scheduleHooks, virtualClusterSetup)
}

func (p *testPlanner) downgradeSteps(
	from, to *clusterupgrade.Version, scheduleHooks, virtualClusterSetup bool,
) []testStep {
	p.currentContext.SetStage(RollbackUpgradeStage)
	nodes := p.currentContext.System.Descriptor.Nodes
	msg := fmt.Sprintf("downgrade nodes %v from %q to %q", nodes, from.String(), to.String())
	return p.changeVersionSteps(from, to, msg, scheduleHooks, virtualClusterSetup)
}

// changeVersionSteps returns the sequence of steps to be performed
// when we are changing the version of cockroach in the nodes of a
// cluster (either upgrading to a new binary, or downgrading to a
// predecessor version). If `scheduleHooks` is true, user-provided
// mixed-version hooks will be scheduled randomly during the
// upgrade/downgrade process.
func (p *testPlanner) changeVersionSteps(
	from, to *clusterupgrade.Version, label string, scheduleHooks, virtualClusterSetup bool,
) []testStep {
	nodes := p.currentContext.System.Descriptor.Nodes
	// copy system `Nodes` here so that shuffling won't mutate that array
	previousVersionNodes := append(option.NodeListOption{}, nodes...)

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
			restartWithNewBinaryStep{
				version:              to,
				node:                 node,
				rt:                   p.rt,
				settings:             p.clusterSettingsForSystem(),
				sharedProcessStarted: virtualClusterSetup,
				initTarget:           p.currentContext.System.Descriptor.Nodes[0],
			},
		))
		for _, s := range p.services() {
			handleInternalError(s.changeVersion(node, to))
		}

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
	fromVersion, toVersion *clusterupgrade.Version, scheduleHooks, virtualClusterSetup bool,
) []testStep {
	p.currentContext.SetFinalizing(true)
	p.currentContext.SetStage(RunningUpgradeMigrationsStage)
	runSystemMigrations := p.newSingleStep(allowUpgradeStep{
		virtualClusterName: install.SystemInterfaceName,
	})

	steps := []testStep{runSystemMigrations}

	var mixedVersionStepsDuringTenantMigrations []testStep
	if scheduleHooks {
		mixedVersionSteps := p.hooks.MixedVersionSteps(p.currentContext, p.prng, p.isLocal)

		switch p.deploymentMode {
		case SystemOnlyDeployment:
			steps = append(steps, mixedVersionSteps...)

		case SharedProcessDeployment:
			// If we are in a multi-tenant deployment, we run some
			// mixed-version hooks while the system is finalizing, and the
			// remaining hooks while the tenant is finalizing.
			if len(mixedVersionSteps) > 0 {
				idx := p.prng.Intn(len(mixedVersionSteps))

				mixedVersionStepsDuringSystemMigrations := mixedVersionSteps[:idx]
				steps = append(steps, mixedVersionStepsDuringSystemMigrations...)

				mixedVersionStepsDuringTenantMigrations = mixedVersionSteps[idx:]
			}
		}
	}

	steps = append(steps, p.newSingleStep(
		waitForStableClusterVersionStep{
			nodes:              p.currentContext.System.Descriptor.Nodes,
			timeout:            p.options.upgradeTimeout,
			desiredVersion:     versionToClusterVersion(toVersion),
			virtualClusterName: install.SystemInterfaceName,
		},
	))

	// At this point, we just upgraded the storage cluster; we might
	// need to patch up the `system.tenant_settings` table in order for
	// the tenant upgrade to work.
	steps = append(steps, p.maybeDeleteAllTenantsVersionOverride(toVersion)...)

	switch p.deploymentMode {
	case SharedProcessDeployment:
		if virtualClusterSetup {
			steps = append(
				steps,
				p.newSingleStep(allowUpgradeStep{
					virtualClusterName: p.tenantName(),
				}),
			)

			// If we are upgrading to a version that does not support
			// auto-upgrading after resetting a cluster setting, we need to
			// "manually" run the migrations at this point.
			if !toVersion.AtLeast(tenantSupportsAutoUpgradeVersion) {
				steps = append(steps,
					p.newSingleStep(
						setTenantClusterVersionStep{
							virtualClusterName: p.tenantName(),
							nodes:              p.currentContext.Tenant.Descriptor.Nodes,
							targetVersion:      versionToClusterVersion(toVersion),
						},
					))
			}

			steps = append(steps, mixedVersionStepsDuringTenantMigrations...)
			steps = append(steps, p.newSingleStep(waitForStableClusterVersionStep{
				nodes:              p.currentContext.Tenant.Descriptor.Nodes,
				timeout:            p.options.upgradeTimeout,
				desiredVersion:     versionToClusterVersion(toVersion),
				virtualClusterName: p.currentContext.Tenant.Descriptor.Name,
			}))
		}
	}

	return steps
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

// maybeDeleteAllTenantsVersionOverride will delete the bad 'version'
// key from the system.tenant_settings table when necessary.
// Specifically, doing this is necessary when we are in virtual
// cluster deployment mode (shared or external process) and we just
// upgraded to a version in the 23.1 release series older than
// v23.1.9.
func (p *testPlanner) maybeDeleteAllTenantsVersionOverride(
	toVersion *clusterupgrade.Version,
) []testStep {
	if p.deploymentMode == SystemOnlyDeployment {
		return nil
	}

	if isAffected := toVersion.Series() == "23.1" && toVersion.Patch() <= 8; !isAffected {
		return nil
	}

	return []testStep{p.newSingleStep(deleteAllTenantsVersionOverrideStep{})}
}

func (p *testPlanner) newSingleStep(impl singleStepProtocol) *singleStep {
	return p.newSingleStepWithContext(p.currentContext, impl)
}

func (p *testPlanner) newSingleStepWithContext(
	testContext *Context, impl singleStepProtocol,
) *singleStep {
	return newSingleStep(testContext, impl, p.newRNG())
}

func (p *testPlanner) services() []*ServiceContext {
	services := []*ServiceContext{p.currentContext.System}
	if p.currentContext.Tenant != nil {
		services = append(services, p.currentContext.Tenant)
	}

	return services
}

func (p *testPlanner) serviceDescriptors() []*ServiceDescriptor {
	var descriptors []*ServiceDescriptor
	for _, sc := range p.services() {
		descriptors = append(descriptors, sc.Descriptor)
	}

	return descriptors
}

func (p *testPlanner) isMultitenant() bool {
	return p.deploymentMode != SystemOnlyDeployment
}

func (p *testPlanner) tenantName() string {
	return p.currentContext.Tenant.Descriptor.Name
}

func (p *testPlanner) clusterSettingsForSystem() []install.ClusterSettingOption {
	cs := []install.ClusterSettingOption{}
	cs = append(cs, defaultClusterSettings...)
	cs = append(cs, p.options.settings...)
	return cs
}

// clusterSettingsForTenant returns the cluster settings options to be
// passed when starting tenants during a test. The default and custom
// options are passed, just like we do for system tenants. However,
// none of the `ClusterSettingOption` options are passed to the
// tenants. There's no easy way to tell which settings are SystemOnly
// (and would fail if applied to a tenant), especially when you take
// multiple versions into account. If really needed, we could provide
// some option for the caller to indicate which options apply to
// system vs tenant, but that's not necessary at the moment.
func (p *testPlanner) clusterSettingsForTenant() []install.ClusterSettingOption {
	settings := p.clusterSettingsForSystem()

	var tenantSettings []install.ClusterSettingOption
	for _, s := range settings {
		if _, ok := s.(install.ClusterSettingsOption); !ok {
			tenantSettings = append(tenantSettings, s)
		}
	}

	return tenantSettings
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

	mapServiceSetup := func(s *serviceSetup) *serviceSetup {
		if s == nil {
			return nil
		}

		return &serviceSetup{
			steps:    mapSteps(s.steps),
			upgrades: mapUpgrades(s.upgrades),
		}
	}

	plan.setup.systemSetup = mapServiceSetup(plan.setup.systemSetup)
	plan.setup.tenantSetup = mapServiceSetup(plan.setup.tenantSetup)
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
		_, isStartSystem := ss.impl.(startStep)
		_, isStartTenant := ss.impl.(startSharedProcessVirtualClusterStep)

		if plan.startSystemID == 0 && isStartSystem {
			plan.startSystemID = stepID
		}

		if plan.startTenantID == 0 && isStartTenant {
			plan.startTenantID = stepID
		}

		ss.ID = stepID
		return []testStep{ss}
	})
}

// allUpgrades returns a list of all upgrades encoded in this test
// plan, including the ones that are run as part of test setup (i.e.,
// before any user-provided test logic is scheduled).
func (plan *TestPlan) allUpgrades() []*upgradePlan {
	allUpgrades := append([]*upgradePlan{}, plan.setup.systemSetup.upgrades...)
	if plan.setup.tenantSetup != nil {
		allUpgrades = append(allUpgrades, plan.setup.tenantSetup.upgrades...)
	}
	allUpgrades = append(allUpgrades, plan.upgrades...)

	return allUpgrades
}

// Steps returns a list of all steps involved in carrying out the
// entire test plan (comprised of one or multiple upgrades).
func (plan *TestPlan) Steps() []testStep {
	// 1. Set up the cluster (install fixtures, start binaries, start
	// tenant if any, run setup upgrades, etc)
	steps := append([]testStep{}, plan.setup.Steps()...)

	// 2. Run user provided initial steps.
	steps = append(steps, plan.initSteps...)

	// 3. Run upgrades with user-provided hooks.
	for _, upgrade := range plan.upgrades {
		steps = append(steps, upgrade.sequentialStep)
	}

	return steps
}

// Steps returns the list of steps to be performed to setup the
// cluster for the test (which may include creating tenants where the
// tests will run).
func (ts testSetup) Steps() []testStep {
	steps := append([]testStep{}, ts.systemSetup.steps...)
	for _, upgrade := range ts.systemSetup.upgrades {
		steps = append(steps, upgrade.sequentialStep)
	}

	if ts.tenantSetup != nil {
		steps = append(steps, ts.tenantSetup.steps...)
		for _, upgrade := range ts.tenantSetup.upgrades {
			steps = append(steps, upgrade.sequentialStep)
		}
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
		formattedVersions = append(formattedVersions, v.String())
	}

	var lines []string
	addLine := func(title string, val any) {
		titleWithColon := fmt.Sprintf("%s:", title)
		lines = append(lines, fmt.Sprintf("%-20s%v", titleWithColon, val))
	}

	addLine("Seed", plan.seed)
	addLine("Upgrades", strings.Join(formattedVersions, " → "))
	addLine("Deployment mode", plan.deploymentMode)

	if len(plan.enabledMutators) > 0 {
		mutatorNames := make([]string, 0, len(plan.enabledMutators))
		for _, mut := range plan.enabledMutators {
			mutatorNames = append(mutatorNames, mut.Name())
		}

		addLine("Mutators", strings.Join(mutatorNames, ", "))
	}

	return fmt.Sprintf(
		"%s\nPlan:\n%s",
		strings.Join(lines, "\n"), out.String(),
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
			var finalizingStr string
			if ss.context.Finalizing() {
				finalizingStr = ",finalizing"
			}
			debugInfo = fmt.Sprintf(" [stage=%s%s]", ss.context.System.Stage, finalizingStr)
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

// defaultTenantClusterSetting returns the name of the cluster setting
// that changes the default tenant on a cluster. The setting changed
// its name from 23.1 to 23.2.
func defaultTenantClusterSetting(v *clusterupgrade.Version) string {
	if !v.AtLeast(OldestSupportedVersionVC) {
		handleInternalError(fmt.Errorf("defaultTenantClusterSetting called on version %s", v))
	}

	if v.Series() == "23.1" {
		return "server.controller.default_tenant"
	}

	return "server.controller.default_target_cluster"
}

func (u UpgradeStage) String() string {
	switch u {
	case SystemSetupStage:
		return "system-setup"
	case TenantSetupStage:
		return "tenant-setup"
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
	return v.Series()
}
