// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mixedversion

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"reflect"
	"strconv"
	"testing"
	"testing/quick"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/roachprod/failureinjection/failures"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/release"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/version"
	"github.com/stretchr/testify/require"
)

var (
	nilLogger = func() *logger.Logger {
		cfg := logger.Config{
			Stdout: io.Discard,
			Stderr: io.Discard,
		}
		l, err := cfg.NewLogger("" /* path */)
		if err != nil {
			panic(err)
		}

		return l
	}()

	// Like nilLogger but keeps stderr.
	// Useful for debugging a failed test like Test_UpgradePaths since otherwise the test plan isn't logged.
	stderrLogger = func() *logger.Logger {
		cfg := logger.Config{
			Stdout: io.Discard,
		}
		l, err := cfg.NewLogger("" /* path */)
		if err != nil {
			panic(err)
		}

		return l
	}()

	ctx   = context.Background()
	nodes = option.NodeListOption{1, 2, 3, 4}

	// Hardcode build, previous, and minimum supported versions so that
	// the test won't fail when new versions are released.
	buildVersion       = version.MustParse("v24.2.0")
	predecessorVersion = "v24.1.8"
	minimumSupported   = clusterupgrade.MustParseVersion("v22.2.0")
)

const seed = 12345 // expectations are based on this seed

func TestTestPlanner(t *testing.T) {
	// N.B. we must restore default versions since other tests may depend on it.
	defer setDefaultVersions()
	// Make some test-only mutators available to the test.
	mutatorsAvailable := append([]mutator{
		concurrentUserHooksMutator{},
		removeUserHooksMutator{},
		newClusterSettingMutator(
			"test_cluster_setting", []int{1, 2, 3},
			clusterSettingMinimumVersion("v23.2.0"),
			clusterSettingMaxChanges(10),
		),
	}, planMutators...)

	// Tests run from an empty list of mutators; the only way to add
	// mutators is by using the `add-mutators` directive in the
	// test. This allows the output to remain stable when new mutators
	// are added, and also allows us to test mutators explicitly and in
	// isolation.
	originalMutators := planMutators
	defer func() {
		planMutators = originalMutators
	}()

	datadriven.Walk(t, datapathutils.TestDataPath(t, "planner"), func(t *testing.T, path string) {
		defer withTestBuildVersion("v24.3.0")()
		resetMutators()
		mvt := newTest()

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			if d.Cmd == "plan" {
				assertValidTest(mvt, t.Fatal)
				plan, err := mvt.plan()
				require.NoError(t, err)

				var debug bool
				if len(d.CmdArgs) > 0 && d.CmdArgs[0].Vals[0] == "true" {
					debug = true
				}

				return plan.prettyPrintInternal(debug)
			}

			switch d.Cmd {
			case "add-mutators":
				for _, arg := range d.CmdArgs {
					mutatorName := arg.Key
					var m mutator
					for _, mut := range mutatorsAvailable {
						if mutatorName == mut.Name() {
							m = mut
							break
						}
					}

					if m == nil {
						t.Errorf("unknown mutator: %s", mutatorName)
					}

					planMutators = append(planMutators, m)
				}
			case "mixed-version-test":
				mvt = createDataDrivenMixedVersionTest(t, d.CmdArgs)
			case "before-cluster-start":
				mvt.BeforeClusterStart(d.CmdArgs[0].Vals[0], dummyHook)
			case "on-startup":
				mvt.OnStartup(d.CmdArgs[0].Vals[0], dummyHook)
			case "in-mixed-version":
				mvt.InMixedVersion(d.CmdArgs[0].Vals[0], dummyHook)
			case "after-upgrade-finalized":
				mvt.AfterUpgradeFinalized(d.CmdArgs[0].Vals[0], dummyHook)
			case "workload":
				initCmd := roachtestutil.NewCommand("./cockroach workload init some-workload")
				runCmd := roachtestutil.NewCommand("./cockroach workload run some-workload")
				mvt.Workload(d.CmdArgs[0].Vals[0], nodes, initCmd, runCmd)
			case "background-command":
				cmd := roachtestutil.NewCommand("./cockroach some-command")
				mvt.BackgroundCommand(d.CmdArgs[0].Vals[0], nodes, cmd)
			case "require-concurrent-hooks":
				assertValidTest(mvt, t.Fatal)
				plan, err := mvt.plan()
				require.NoError(t, err)
				require.NoError(t, requireConcurrentHooks(t, plan.Steps(), d.CmdArgs[0].Vals...))
			default:
				t.Fatalf("unknown directive: %s", d.Cmd)
			}

			return "ok"
		})
	})
}

// TestDeterministicTestPlan tests that generating a test plan with
// the same seed multiple times yields the same plan every time.
func TestDeterministicTestPlan(t *testing.T) {
	makePlan := func() *TestPlan {
		mvt := newTest()
		mvt.InMixedVersion("mixed-version 1", dummyHook)
		mvt.InMixedVersion("mixed-version 2", dummyHook)

		assertValidTest(mvt, t.Fatal)
		plan, err := mvt.plan()
		require.NoError(t, err)
		return plan
	}

	expectedPlan := makePlan()
	const numRuns = 50
	for j := 0; j < numRuns; j++ {
		require.Equal(t, expectedPlan.PrettyPrint(), makePlan().PrettyPrint(), "j = %d", j)
	}
}

var unused float64

// TestDeterministicHookSeeds ensures that user functions passed to
// `InMixedVersion` always see the same sequence of values even if the
// PRNG passed to the `Test` struct is perturbed during runs. In other
// words, this ensures that user functions have at their disposal a
// random number generator that is unique to them and concurrency with
// other functions should not change the sequence of values they see
// as long as the RNG is used deterministically in the user function
// itself.
func TestDeterministicHookSeeds(t *testing.T) {
	defer resetMutators()()

	generateData := func(generateMoreRandomNumbers bool) [][]int {
		var generatedData [][]int
		mvt := newTest(NumUpgrades(1), EnabledDeploymentModes(SystemOnlyDeployment))
		mvt.InMixedVersion("do something", func(_ context.Context, _ *logger.Logger, rng *rand.Rand, _ *Helper) error {
			var data []int
			for j := 0; j < 5; j++ {
				data = append(data, rng.Intn(100))
			}

			generatedData = append(generatedData, data)

			// Ensure that changing the top-level random number generator
			// has no impact on the rng passed to the user function.
			if generateMoreRandomNumbers {
				for j := 0; j < 10; j++ {
					unused = mvt.prng.Float64()
				}
			}
			return nil
		})

		var (
			// these variables are not used by the hook so they can be nil
			ctx         = context.Background()
			emptyHelper = &Helper{}
		)

		assertValidTest(mvt, t.Fatal)
		plan, err := mvt.plan()
		require.NoError(t, err)

		upgradeStep := plan.Steps()[3].(sequentialRunStep)
		// We can hardcode these paths since we are using a fixed seed in
		// these tests.
		firstRunStep := upgradeStep.steps[1].(sequentialRunStep).steps[1].(*singleStep)
		firstRun := firstRunStep.impl.(runHookStep)
		require.Equal(t, "do something", firstRun.hook.name)
		require.NoError(t, firstRun.Run(ctx, nilLogger, firstRunStep.rng, emptyHelper))

		secondRunStep := upgradeStep.steps[2].(sequentialRunStep).steps[3].(*singleStep)
		secondRun := secondRunStep.impl.(runHookStep)
		require.Equal(t, "do something", secondRun.hook.name)
		require.NoError(t, secondRun.Run(ctx, nilLogger, secondRunStep.rng, emptyHelper))

		thirdRunStep := upgradeStep.steps[3].(sequentialRunStep).steps[2].(*singleStep)
		thirdRun := thirdRunStep.impl.(runHookStep)
		require.Equal(t, "do something", thirdRun.hook.name)
		require.NoError(t, thirdRun.Run(ctx, nilLogger, thirdRunStep.rng, emptyHelper))

		require.Len(t, generatedData, 3)
		return generatedData
	}

	expectedData := [][]int{
		{58, 39, 39, 59, 92},
		{25, 95, 87, 77, 75},
		{40, 27, 42, 16, 3},
	}
	const numRums = 50
	for j := 0; j < numRums; j++ {
		for _, b := range []bool{true, false} {
			require.Equal(t, expectedData, generateData(b), "j = %d | b = %t", j, b)
		}
	}
}

// TestIterateSingleStepsIsPure verifies two properties of iterateSingleSteps:
// 1. Delays don't change - the delay values in the plan remain unchanged.
// 2. Idempotent - calling it twice produces the same step count.
func TestIterateSingleStepsIsPure(t *testing.T) {
	defer resetMutators()()

	// Create a test with concurrent hooks to ensure we have concurrentRunStep
	// instances with delays.
	mvt := newTest(NumUpgrades(1), EnabledDeploymentModes(SystemOnlyDeployment))
	mvt.InMixedVersion("hook 1", dummyHook)
	mvt.InMixedVersion("hook 2", dummyHook)

	assertValidTest(mvt, t.Fatal)
	plan, err := mvt.plan()
	require.NoError(t, err)

	// Capture plan representation before iteration.
	planBefore := plan.PrettyPrint()

	// First call to iterateSingleSteps.
	var expectedNumSteps int
	plan.iterateSingleSteps(func(ss *singleStep, isConcurrent bool) {
		expectedNumSteps++
	})
	require.Greater(t, expectedNumSteps, 0, "should have iterated over at least one step")

	// Capture plan representation after first iteration.
	planAfterFirst := plan.PrettyPrint()

	// Second call to iterateSingleSteps.
	var actualNumSteps int
	plan.iterateSingleSteps(func(ss *singleStep, isConcurrent bool) {
		actualNumSteps++
	})

	// Property 1: Idempotent - same step count each time (cheap check first).
	require.Equal(t, expectedNumSteps, actualNumSteps,
		"iterateSingleSteps should return same step count on each call")

	// Property 2: Delays don't change - plan representation should be identical.
	require.Equal(t, planBefore, planAfterFirst,
		"plan should not change after first iterateSingleSteps call")
	planAfterSecond := plan.PrettyPrint()
	require.Equal(t, planBefore, planAfterSecond,
		"plan should not change after second iterateSingleSteps call")
}

// TestMapSingleStepsGeneratesDelays verifies that mapSingleSteps generates
// new delays when inserting steps into concurrent groups.
func TestMapSingleStepsGeneratesDelays(t *testing.T) {
	defer resetMutators()()

	// Override possibleDelays with a continuous range of 0-10 minutes.
	originalPossibleDelays := possibleDelays
	continuousDelays := make([]time.Duration, 600000)
	for i := range continuousDelays {
		continuousDelays[i] = time.Duration(i) * time.Millisecond
	}
	possibleDelays = continuousDelays
	defer func() { possibleDelays = originalPossibleDelays }()

	// Create a test with concurrent hooks.
	mvt := newTest(NumUpgrades(1), EnabledDeploymentModes(SystemOnlyDeployment))
	mvt.InMixedVersion("hook 1", dummyHook)
	mvt.InMixedVersion("hook 2", dummyHook)

	assertValidTest(mvt, t.Fatal)
	plan, err := mvt.plan()
	require.NoError(t, err)

	// Collect the delays of steps in concurrent groups before mutation.
	collectDelays := func(steps []testStep) map[*singleStep]time.Duration {
		delays := make(map[*singleStep]time.Duration)
		var visit func(testStep)
		visit = func(step testStep) {
			switch s := step.(type) {
			case sequentialRunStep:
				for _, seqStep := range s.steps {
					visit(seqStep)
				}
			case concurrentRunStep:
				for _, concurrentStep := range s.delayedSteps {
					ds := concurrentStep.(delayedStep)
					if ss, ok := ds.step.(*singleStep); ok {
						delays[ss] = ds.delay
					}
				}
			}
		}
		for _, step := range steps {
			visit(step)
		}
		return delays
	}

	delaysBefore := collectDelays(plan.Steps())
	require.Greater(t, len(delaysBefore), 0, "should have concurrent steps before mutation")

	// Track which steps we insert.
	var insertedSteps []*singleStep

	// Use mapSingleSteps to insert a new step into every concurrent group.
	plan.mapSingleSteps(func(ss *singleStep, isConcurrent bool) []testStep {
		if isConcurrent {
			newStep := newTestStep(func() error { return nil })
			insertedSteps = append(insertedSteps, newStep)
			return []testStep{ss, newStep}
		}
		return []testStep{ss}
	})

	// Verify we actually inserted some steps.
	require.Greater(t, len(insertedSteps), 0, "should have inserted at least one step")

	// Collect delays after mutation.
	delaysAfter := collectDelays(plan.Steps())

	// Verify that each newly inserted step has a delay assigned.
	for _, insertedStep := range insertedSteps {
		_, exists := delaysAfter[insertedStep]
		require.True(t, exists, "inserted step should have a delay assigned")
	}

	// Verify that original steps still have their original delays.
	for originalStep, originalDelay := range delaysBefore {
		newDelay, exists := delaysAfter[originalStep]
		require.True(t, exists, "original step should still exist after mutation")
		require.Equal(t, originalDelay, newDelay,
			"original step's delay should not change after mutation")
	}
}

// Test_startSystemID tests that the plan generated by the test
// planner keeps track of the correct ID for the test's start step.
func Test_startClusterID(t *testing.T) {
	// When fixtures are disabled, the startStep should always be the
	// first step of the test (ID = 1) in system-only deployments.
	mvt := newTest(NeverUseFixtures, EnabledDeploymentModes(SystemOnlyDeployment))
	assertValidTest(mvt, t.Fatal)
	plan, err := mvt.plan()
	require.NoError(t, err)

	ss := plan.Steps()[0].(*singleStep)
	require.IsType(t, startStep{}, ss.impl)
	require.Equal(t, 1, ss.ID)
	require.Equal(t, 1, plan.startSystemID)

	// When fixtures are used, the startStep should always be the second
	// step of the test (ID = 2) in system-only deployments i.e., after
	// fixtures are installed.
	mvt = newTest(AlwaysUseFixtures, EnabledDeploymentModes(SystemOnlyDeployment))
	assertValidTest(mvt, t.Fatal)
	plan, err = mvt.plan()
	require.NoError(t, err)
	ss = plan.Steps()[1].(*singleStep)
	require.IsType(t, startStep{}, ss.impl)
	require.Equal(t, 2, ss.ID)
	require.Equal(t, 2, plan.startSystemID)
}

// Test_upgradeTimeout tests the behaviour of upgrade timeouts in
// mixedversion tests. If no custom value is passed, the default
// timeout in the clusterupgrade package is used; otherwise, the
// custom value is enforced.
func Test_upgradeTimeout(t *testing.T) {
	findUpgradeWaitSteps := func(plan *TestPlan) []waitForStableClusterVersionStep {
		var steps []waitForStableClusterVersionStep
		for _, s := range plan.Steps() {
			if ss, isSingle := s.(*singleStep); isSingle {
				if step, isUpgrade := ss.impl.(waitForStableClusterVersionStep); isUpgrade {
					steps = append(steps, step)
				}
			}
		}
		if len(steps) == 0 {
			require.Fail(t, "could not find any waitForStableClusterVersionStep in the plan")
		}
		return steps
	}

	assertTimeout := func(expectedTimeout time.Duration, opts ...CustomOption) {
		mvt := newTest(opts...)
		assertValidTest(mvt, t.Fatal)
		plan, err := mvt.plan()
		require.NoError(t, err)
		waitUpgrades := findUpgradeWaitSteps(plan)

		for _, s := range waitUpgrades {
			require.Equal(t, expectedTimeout, s.timeout)
		}
	}

	assertTimeout(10 * time.Minute)                               // using default settings, the default timeout applies
	assertTimeout(30*time.Minute, UpgradeTimeout(30*time.Minute)) // custom timeout applies.
}

// Test_maxNumPlanSteps tests the behaviour of plan generation in
// mixedversion tests. If no custom value is passed, the default
// should yield a (valid) test plan. Otherwise, the length of a test plan
// should be <= `maxNumPlanSteps`. If maxNumPlanSteps is too low, an error
// should be returned.
func Test_maxNumPlanSteps(t *testing.T) {
	mvt := newBasicUpgradeTest()
	assertValidTest(mvt, t.Fatal)
	plan, err := mvt.plan()
	require.NoError(t, err)
	// N.B. the upper bound is very conservative; largest "basic upgrade" plan is well below it.
	require.LessOrEqual(t, plan.length, 100)

	mvt = newBasicUpgradeTest(MaxNumPlanSteps(15))
	r := retry.StartWithCtx(ctx, retry.Options{MaxRetries: 5})
	for r.Next() {
		plan, err = mvt.plan()
		if err != nil {
			continue
		}
		require.LessOrEqual(t, plan.length, 15)
		break
	}

	// There is in fact no "basic upgrade" test plan with fewer than 13 steps.
	// The smallest plan is,
	// planner_test.go:314: Seed:               12345
	// Upgrades:           v24.1.1 → <current>
	//		Deployment mode:    system-only
	// Plan:
	//	├── start cluster at version "v24.1.1" (1)
	//	├── wait for all nodes (:1-4) to acknowledge cluster version '24.1' on system tenant (2)
	//	└── upgrade cluster from "v24.1.1" to "<current>"
	//	├── prevent auto-upgrades on system tenant by setting `preserve_downgrade_option` (3)
	//	├── upgrade nodes :1-4 from "v24.1.1" to "<current>"
	//	│   ├── restart node 2 with binary version <current> (4)
	//	│   ├── run mixed-version hooks concurrently
	//	│   │   ├── run "on startup 1", after 5s delay (5)
	//	│   │   └── run "mixed-version 1", after 5s delay (6)
	//	│   ├── restart node 4 with binary version <current> (7)
	//	│   ├── restart node 1 with binary version <current> (8)
	//	│   ├── restart node 3 with binary version <current> (9)
	//	│   └── run "mixed-version 2" (10)
	//	├── allow upgrade to happen on system tenant by resetting `preserve_downgrade_option` (11)
	//	├── wait for all nodes (:1-4) to acknowledge cluster version <current> on system tenant (12)
	//	└── run "after finalization" (13)
	mvt = newBasicUpgradeTest(MaxNumPlanSteps(5))
	assertValidTest(mvt, t.Fatal)
	plan, err = mvt.plan()
	require.ErrorContains(t, err, "unable to generate a test plan")
	require.Nil(t, plan)
}

// TestNoConcurrentFailureInjections tests that failure injection
// steps properly manage node availability. Specifically:
// - Failure injection steps should only run if no other failure is currently injected.
// - Failure recovery steps can only occur if there is an active failure injected.
// - We can only bump the cluster version if no failures are currently injected.
func TestNoConcurrentFailureInjections(t *testing.T) {
	const numIterations = 500
	rngSource := rand.NewSource(randutil.NewPseudoSeed())
	// Set all failure injection mutator probabilities to 1.
	var opts []CustomOption
	for _, mutator := range failureInjectionMutators {
		opts = append(opts, WithMutatorProbability(mutator.Name(), 1.0))
	}
	opts = append(opts, NumUpgrades(3))
	getFailer := func(name string) (*failures.Failer, error) {
		return nil, nil
	}

	for range numIterations {
		mvt := newTest(opts...)
		mvt._getFailer = getFailer
		mvt.InMixedVersion("test hook", dummyHook)
		// Use different seed for each iteration
		mvt.prng = rand.New(rngSource)

		assertValidTest(mvt, t.Fatal)
		plan, err := mvt.plan()
		require.NoError(t, err)

		isFailureInjected := false

		var checkSteps func(steps []testStep)
		checkSteps = func(steps []testStep) {
			for _, step := range steps {
				switch s := step.(type) {
				case *singleStep:
					switch s.impl.(type) {
					case panicNodeStep:
						require.False(t, isFailureInjected, "there should be no active failure when panicNodeStep runs")
						isFailureInjected = true
					case networkPartitionInjectStep:
						require.False(t, isFailureInjected, "there should be no active failure when networkPartitionInjectStep runs")
						isFailureInjected = true
					case restartNodeStep:
						require.True(t, isFailureInjected, "there is no active failure to recover from")
						isFailureInjected = false
					case networkPartitionRecoveryStep:
						require.True(t, isFailureInjected, "there is no active failure to recover from")
						isFailureInjected = false
					case waitForStableClusterVersionStep:
						require.False(t, isFailureInjected, "waitForStableClusterVersionStep cannot run under failure injection")
					}
				case sequentialRunStep:
					checkSteps(s.steps)
				case concurrentRunStep:
					// Failure injection steps should never run concurrently with other steps, so treat concurrent
					// steps as sequential for simplicity.
					for _, delayedStepInterface := range s.delayedSteps {
						ds := delayedStepInterface.(delayedStep)
						checkSteps([]testStep{ds.step})
					}
				}
			}
		}

		checkSteps(plan.Steps())

		require.False(t, isFailureInjected, "all failure injections should be cleaned up at the end of the test")
	}
}

// setDefaultVersions overrides the test's view of the current build
// as well as the oldest supported version. This allows the test
// output to remain stable as new versions are released and/or we bump
// the oldest supported version. Called by TestMain.
func setDefaultVersions() func() {
	previousBuildV := clusterupgrade.TestBuildVersion
	clusterupgrade.TestBuildVersion = &buildVersion

	previousOldestV := OldestSupportedVersion
	OldestSupportedVersion = minimumSupported

	return func() {
		clusterupgrade.TestBuildVersion = previousBuildV
		OldestSupportedVersion = previousOldestV
	}
}

func resetMutators() func() {
	originalMutators := planMutators
	planMutators = nil

	return func() { planMutators = originalMutators }
}

func newRand() *rand.Rand {
	return rand.New(rand.NewSource(seed))
}

func newTest(options ...CustomOption) *Test {
	testOptions := defaultTestOptions()
	// Enforce some default options by default in tests; those that test
	// multitenant deployments or skip-version upgrades specifically
	// should pass the corresponding option explicitly.
	defaultTestOverrides := []CustomOption{
		EnabledDeploymentModes(SystemOnlyDeployment),
		DisableSkipVersionUpgrades,
		DisableAllFailureInjectionMutators(),
	}

	for _, fn := range defaultTestOverrides {
		fn(&testOptions)
	}

	for _, fn := range options {
		fn(&testOptions)
	}

	// N.B. by setting this, we override the framework defaults that force
	// separate process to use latestPredecessor. This is intentional as it
	// prevents flaking whenever new versions are added.
	testOptions.predecessorFunc = testPredecessorFunc

	return &Test{
		ctx:       ctx,
		logger:    stderrLogger,
		crdbNodes: nodes,
		options:   testOptions,
		_arch:     archP(vm.ArchAMD64),
		_isLocal:  boolP(false),
		prng:      newRand(),
		hooks:     &testHooks{crdbNodes: nodes},
		seed:      seed,
	}
}

func newBasicUpgradeTest(options ...CustomOption) *Test {
	mvt := newTest(options...)
	mvt.InMixedVersion("on startup 1", dummyHook)
	mvt.InMixedVersion("mixed-version 1", dummyHook)
	mvt.InMixedVersion("mixed-version 2", dummyHook)
	mvt.AfterUpgradeFinalized("after finalization", dummyHook)

	return mvt
}

func archP(a vm.CPUArch) *vm.CPUArch {
	return &a
}

func boolP(b bool) *bool {
	return &b
}

// Always use the same predecessor version to make this test
// deterministic even as changes continue to happen in the
// cockroach_releases.yaml file.
func testPredecessorFunc(_ *rand.Rand, v *clusterupgrade.Version) (*clusterupgrade.Version, error) {
	pred, ok := testPredecessorMapping[v.Series()]
	if !ok {
		return nil, fmt.Errorf("no known predecessor for %q (%q series)", v, v.Series())
	}

	return pred, nil
}

// createDataDrivenMixedVersionTest creates a `*Test` instance based
// on the parameters passed to the `mixed-version-test` datadriven
// directive.
func createDataDrivenMixedVersionTest(t *testing.T, args []datadriven.CmdArg) *Test {
	var opts []CustomOption
	var predecessors []*clusterupgrade.Version
	var isLocal *bool

	for _, arg := range args {
		switch arg.Key {
		case "predecessors":
			for _, v := range arg.Vals {
				predecessors = append(predecessors, clusterupgrade.MustParseVersion(v))
			}

		case "num_upgrades":
			n, err := strconv.Atoi(arg.Vals[0])
			require.NoError(t, err)
			opts = append(opts, NumUpgrades(n))

		case "min_upgrades":
			n, err := strconv.Atoi(arg.Vals[0])
			require.NoError(t, err)
			opts = append(opts, MinUpgrades(n))

		case "max_upgrades":
			n, err := strconv.Atoi(arg.Vals[0])
			require.NoError(t, err)
			opts = append(opts, MaxUpgrades(n))

		case "minimum_supported_version":
			v := arg.Vals[0]
			opts = append(opts, MinimumSupportedVersion(v))

		case "is_local":
			b, err := strconv.ParseBool(arg.Vals[0])
			require.NoError(t, err)
			isLocal = boolP(b)

		case "mutator_probabilities":
			if len(arg.Vals)%2 != 0 {
				t.Fatalf("even number of values required for %s directive", arg.Key)
			}

			var j int
			for j < len(arg.Vals) {
				name, probStr := arg.Vals[j], arg.Vals[j+1]
				prob, err := strconv.ParseFloat(probStr, 64)
				require.NoError(t, err)
				opts = append(opts, WithMutatorProbability(name, prob))

				j += 2
			}

		case "disable_mutator":
			opts = append(opts, DisableMutators(arg.Vals[0]))

		case "enable_skip_version":
			opts = append(opts, WithSkipVersionProbability(1))

		case "same_series_upgrade_probability":
			prob, err := strconv.ParseFloat(arg.Vals[0], 64)
			require.NoError(t, err)
			opts = append(opts, WithSameSeriesUpgradeProbability(prob))

		case "deployment_mode":
			opts = append(opts, EnabledDeploymentModes(DeploymentMode(arg.Vals[0])))

		case "workload_node":
			workloadNodes := option.NodeListOption{}
			for _, nodeStr := range arg.Vals {
				n, err := strconv.Atoi(nodeStr)
				require.NoError(t, err)
				workloadNodes = append(workloadNodes, n)
			}
			opts = append(opts, WithWorkloadNodes(workloadNodes))

		default:
			t.Errorf("unknown mixed-version-test option: %s", arg.Key)
		}
	}

	mvt := newTest(opts...)

	if isLocal != nil {
		mvt._isLocal = isLocal
	}

	if predecessors != nil {
		mvt.options.predecessorFunc = func(_ *rand.Rand, v *clusterupgrade.Version) (*clusterupgrade.Version, error) {
			if v.IsCurrent() {
				return predecessors[len(predecessors)-1], nil
			}

			previous := predecessors[0]
			for j := 1; j < len(predecessors); j++ {
				if predecessors[j].Equal(v) {
					return previous, nil
				}

				previous = predecessors[j]
			}

			return nil, fmt.Errorf("no predecessor for %s", v.String())
		}
	}

	return mvt
}

func Test_stepSelectorFilter(t *testing.T) {
	testCases := []struct {
		name      string
		predicate func(*singleStep) bool
		// expectedAllSteps returns whether the step selector should match
		// all original steps even after applying the `predicate`.
		expectedAllSteps bool
		// assertStepsFunc is called to assert on the current state of the
		// selector, if `expectedAllSteps` is false.
		assertStepsFunc func(*testing.T, stepSelector)
		// expectedRandomStepType is the type of the step to be returned
		// by RandomStep() after applying the predicate. Leave unset if
		// the selector should be empty.
		expectedRandomStepType interface{}
	}{
		{
			name:                   "no filter",
			predicate:              func(*singleStep) bool { return true },
			expectedAllSteps:       true,
			expectedRandomStepType: restartWithNewBinaryStep{},
		},
		{
			name: "filter eliminates all steps",
			predicate: func(s *singleStep) bool {
				return s.context.System.Stage == BackgroundStage // no background steps in the plan used in the test
			},
			assertStepsFunc: func(t *testing.T, sel stepSelector) {
				require.Empty(t, sel)
			},
			expectedRandomStepType: nil, // no steps selected
		},
		{
			name: "filtering by a specific stage",
			predicate: func(s *singleStep) bool {
				return s.context.System.Stage == AfterUpgradeFinalizedStage
			},
			assertStepsFunc: func(t *testing.T, sel stepSelector) {
				require.Len(t, sel, 3) // number of upgrades we are performing
				for _, s := range sel {
					require.IsType(t, runHookStep{}, s.impl)
					rhs := s.impl.(runHookStep)
					require.Equal(t, "after finalization", rhs.hook.name)
				}
			},
			expectedRandomStepType: runHookStep{},
		},
		{
			name: "filtering by a specific stage and upgrade cycle",
			predicate: func(s *singleStep) bool {
				return s.context.System.ToVersion.IsCurrent() && s.context.System.Stage == AfterUpgradeFinalizedStage
			},
			assertStepsFunc: func(t *testing.T, sel stepSelector) {
				require.Len(t, sel, 1)
				require.IsType(t, runHookStep{}, sel[0].impl)
				rhs := sel[0].impl.(runHookStep)
				require.Equal(t, "after finalization", rhs.hook.name)
			},
			expectedRandomStepType: runHookStep{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mvt := newBasicUpgradeTest(
				NumUpgrades(3),
				DisableSkipVersionUpgrades,
				EnabledDeploymentModes(SystemOnlyDeployment),
			)
			assertValidTest(mvt, t.Fatal)
			plan, err := mvt.plan()
			require.NoError(t, err)

			sel := plan.newStepSelector()
			allSteps := sel

			newSelector := sel.Filter(tc.predicate)
			if tc.expectedAllSteps {
				require.Equal(t, allSteps, newSelector)
			} else {
				tc.assertStepsFunc(t, newSelector)
			}

			randomSelector := newSelector.RandomStep(newRand())

			if tc.expectedRandomStepType == nil {
				require.Empty(t, randomSelector)
			} else {
				require.Len(t, randomSelector, 1)
				require.IsType(t, tc.expectedRandomStepType, randomSelector[0].impl)
			}
		})
	}
}

func Test_stepSelectorMutations(t *testing.T) {
	defer withTestBuildVersion("v24.1.0")()

	validateMutations := func(
		t *testing.T,
		mutations []mutation,
		expectedMutations int,
		expectedName string,
		expectedImpl singleStepProtocol,
		expectedOps []mutationOp,
	) {
		require.Len(t, mutations, expectedMutations)

		var ops []mutationOp
		for _, mut := range mutations {
			require.IsType(t, runHookStep{}, mut.reference.impl)
			rhs := mut.reference.impl.(runHookStep)
			require.Equal(t, expectedName, rhs.hook.name)

			require.Equal(t, expectedImpl, mut.impl)
			ops = append(ops, mut.op)
		}

		require.Equal(t, expectedOps, ops)
	}

	testCases := []struct {
		name string
		// numUpgrades limits the number of upgrades performed by the test
		// planner.
		numUpgrades int
		// predicate is applied to the step selector for the basic test
		// plan generated in each test case.
		predicate func(*singleStep) bool
		// op is the operation to be performed on the selector after
		// applying the `predicate`.
		op string
		// assertMutationsFunc is called to validate that the mutations
		// generated by this test case match the expectations.
		assertMutationsFunc func(*testing.T, *singleStep, []mutation)
	}{
		{
			name:        "insert step when filter has no matches",
			numUpgrades: 1,
			predicate: func(s *singleStep) bool {
				return s.context.System.Stage == BackgroundStage // no background steps
			},
			op: "insert",
			assertMutationsFunc: func(t *testing.T, step *singleStep, mutations []mutation) {
				validateMutations(t, mutations, 0, "", nil, nil)
			},
		},
		{
			name:        "insert step in single upgrade, single step filtered",
			numUpgrades: 1,
			predicate: func(s *singleStep) bool {
				return s.context.System.Stage == AfterUpgradeFinalizedStage
			},
			op: "insert",
			assertMutationsFunc: func(t *testing.T, step *singleStep, mutations []mutation) {
				validateMutations(
					t, mutations, 1, "after finalization", step.impl,
					[]mutationOp{mutationInsertConcurrent},
				)
			},
		},
		{
			name:        "insert step in multiple upgrade plan",
			numUpgrades: 3,
			predicate: func(s *singleStep) bool {
				return s.context.System.Stage == AfterUpgradeFinalizedStage
			},
			op: "insert",
			assertMutationsFunc: func(t *testing.T, step *singleStep, mutations []mutation) {
				validateMutations(
					t, mutations, 3, "after finalization", step.impl,
					[]mutationOp{mutationInsertConcurrent, mutationInsertAfter, mutationInsertAfter},
				)
			},
		},
		{
			name:        "remove step when filter has no matches",
			numUpgrades: 1,
			predicate: func(s *singleStep) bool {
				return s.context.System.Stage == BackgroundStage // no background steps
			},
			op: "remove",
			assertMutationsFunc: func(t *testing.T, _ *singleStep, mutations []mutation) {
				validateMutations(t, mutations, 0, "", nil, nil)
			},
		},
		{
			name:        "remove step in single upgrade, single step filtered",
			numUpgrades: 1,
			predicate: func(s *singleStep) bool {
				return s.context.System.Stage == AfterUpgradeFinalizedStage
			},
			op: "remove",
			assertMutationsFunc: func(t *testing.T, _ *singleStep, mutations []mutation) {
				validateMutations(
					t, mutations, 1, "after finalization", nil,
					[]mutationOp{mutationRemove},
				)
			},
		},
		{
			name:        "remove step in multiple upgrade plan",
			numUpgrades: 3,
			predicate: func(s *singleStep) bool {
				return s.context.System.Stage == AfterUpgradeFinalizedStage
			},
			op: "remove",
			assertMutationsFunc: func(t *testing.T, _ *singleStep, mutations []mutation) {
				validateMutations(
					t, mutations, 3, "after finalization", nil,
					[]mutationOp{mutationRemove, mutationRemove, mutationRemove},
				)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Disable mutators to make the assertions in this test more
			// stable as we add / remove / change mutators.
			defer resetMutators()()

			mvt := newBasicUpgradeTest(
				NumUpgrades(tc.numUpgrades),
				DisableSkipVersionUpgrades,
				EnabledDeploymentModes(SystemOnlyDeployment),
			)
			assertValidTest(mvt, t.Fatal)
			plan, err := mvt.plan()
			require.NoError(t, err)

			sel := plan.newStepSelector().Filter(tc.predicate)
			ss := newTestStep(func() error { return nil })

			var mutations []mutation
			if tc.op == "insert" {
				mutations = sel.Insert(newRand(), ss.impl)
			} else if tc.op == "remove" {
				mutations = sel.Remove()
			} else {
				require.FailNowf(t, "unknown op: %s", tc.op)
			}

			tc.assertMutationsFunc(t, ss, mutations)
		})
	}
}

func Test_mutationApplicationOrder(t *testing.T) {
	mutations := []mutation{
		{op: mutationInsertBefore},
		{op: mutationRemove},
		{op: mutationInsertAfter},
	}

	require.Equal(t, []mutation{
		{op: mutationInsertBefore},
		{op: mutationInsertAfter},
		{op: mutationRemove},
	}, mutationApplicationOrder(mutations))
}

// requireConcurrentHooks asserts that there is a concurrent step with
// user-provided hooks of the given names.
func requireConcurrentHooks(t *testing.T, steps []testStep, names ...string) error {
	// We first flatten all sequential steps since the concurrent step
	// might be within a series of sequential steps.
	var flattenSequentialSteps func(s testStep) []testStep
	flattenSequentialSteps = func(s testStep) []testStep {
		if seqStep, ok := s.(sequentialRunStep); ok {
			var result []testStep
			for _, s := range seqStep.steps {
				result = append(result, flattenSequentialSteps(s)...)
			}

			return result
		}

		return []testStep{s}
	}

	var allSteps []testStep
	for _, step := range steps {
		allSteps = append(allSteps, flattenSequentialSteps(step)...)
	}

NEXT_STEP:
	for _, step := range allSteps {
		if crs, ok := step.(concurrentRunStep); ok {
			if len(crs.delayedSteps) != len(names) {
				continue NEXT_STEP
			}

			stepNames := map[string]struct{}{}
			for _, concurrentStep := range crs.delayedSteps {
				ds := concurrentStep.(delayedStep)
				ss, ok := ds.step.(*singleStep)
				if !ok {
					continue NEXT_STEP
				}
				rhs, ok := ss.impl.(runHookStep)
				if !ok {
					continue NEXT_STEP
				}

				stepNames[rhs.hook.name] = struct{}{}
			}

			// Check if this concurrent step has all the steps passed as
			// parameter, if not, we move on to the next concurrent step, if
			// any.
			for _, requiredName := range names {
				if _, exists := stepNames[requiredName]; !exists {
					continue NEXT_STEP
				}
			}

			return nil
		}
	}

	return fmt.Errorf("no concurrent step that includes: %#v", names)
}

// concurrentUserHooksMutator is a test mutator that inserts a step
// concurrently with every user-provided hook.
type concurrentUserHooksMutator struct{}

func (concurrentUserHooksMutator) Name() string         { return "concurrent_user_hooks_mutator" }
func (concurrentUserHooksMutator) Probability() float64 { return 0.5 }

func (concurrentUserHooksMutator) Generate(
	rng *rand.Rand, plan *TestPlan, planner *testPlanner,
) ([]mutation, error) {
	// Insert our `testSingleStep` implementation concurrently with every
	// user-provided function.
	return plan.
		newStepSelector().
		Filter(func(s *singleStep) bool {
			_, ok := s.impl.(runHookStep)
			return ok
		}).
		InsertConcurrent(&testSingleStep{}), nil
}

// removeUserHooksMutator is a test mutator that removes every
// user-provided hook from the plan.
type removeUserHooksMutator struct{}

func (removeUserHooksMutator) Name() string         { return "remove_user_hooks_mutator" }
func (removeUserHooksMutator) Probability() float64 { return 0.5 }

func (removeUserHooksMutator) Generate(
	rng *rand.Rand, plan *TestPlan, planner *testPlanner,
) ([]mutation, error) {
	return plan.
		newStepSelector().
		Filter(func(s *singleStep) bool {
			_, ok := s.impl.(runHookStep)
			return ok
		}).
		Remove(), nil
}

func dummyHook(context.Context, *logger.Logger, *rand.Rand, *Helper) error {
	return nil
}

func Test_DisableAllMutators(t *testing.T) {
	mvt := newTest(DisableAllMutators())

	rng, seed := randutil.NewTestRand()
	mvt.seed = seed
	mvt.prng = rng

	assertValidTest(mvt, t.Fatal)
	plan, err := mvt.plan()
	require.NoError(t, err)
	require.Nil(t, plan.enabledMutators)

}

func Test_DisableAllClusterSettingMutators(t *testing.T) {
	mvt := newTest(DisableAllClusterSettingMutators())

	rng, seed := randutil.NewTestRand()
	mvt.seed = seed
	mvt.prng = rng

	assertValidTest(mvt, t.Fatal)
	plan, err := mvt.plan()
	require.NoError(t, err)

	for _, enabled := range plan.enabledMutators {
		if _, ok := enabled.(clusterSettingMutator); ok {
			t.Errorf("cluster setting mutator %q was not disabled", enabled.Name())
		}
	}
}

// This is a regression test to ensure that separate process deployments
// correctly default to using latest predecessors.
func Test_SeparateProcessUsesLatestPred(t *testing.T) {
	testOptions := defaultTestOptions()
	testOverrides := []CustomOption{
		EnabledDeploymentModes(SeparateProcessDeployment),
		DisableSkipVersionUpgrades,
		DisableAllFailureInjectionMutators(),
		MinUpgrades(5),
		MaxUpgrades(5),
		// Lower the minimum bootstrap version to allow for 5 upgrades.
		MinimumBootstrapVersion("v22.1.1"),
	}

	for _, fn := range testOverrides {
		fn(&testOptions)
	}
	mvt := &Test{
		ctx:       ctx,
		logger:    nilLogger,
		crdbNodes: nodes,
		options:   testOptions,
		_arch:     archP(vm.ArchAMD64),
		_isLocal:  boolP(false),
		prng:      newRand(),
		hooks:     &testHooks{crdbNodes: nodes},
		seed:      seed,
	}

	assertValidTest(mvt, t.Fatal)
	plan, err := mvt.plan()
	require.NoError(t, err)
	upgradePath := plan.Versions()
	// Remove the last element as it's the current version which is a special case.
	// The unit test framework hardcodes the current version which should have no
	// patch releases but LatestPatchRelease will pull the actual latest patch.
	upgradePath = upgradePath[:len(upgradePath)-1]
	for _, version := range upgradePath {
		series := version.Series()
		latestVersion, err := clusterupgrade.LatestPatchRelease(series)
		require.NoError(t, err)
		if !version.Equal(latestVersion) && !version.Equal(mvt.options.minimumSupportedVersion) {
			t.Errorf("expected version %s to be latest patch release %s or mininumSupportVersion %s", version, latestVersion, mvt.options.minimumSupportedVersion)
		}
		require.Equal(t, latestVersion, version)
	}
}

// This is a regression test for the problem described in [1] and addressed in [2].
// Namely, when 24.3 is the minimum supported version,
// an upgrade plan terminating in 25.2 could end up skipping 25.1, at which point the user hooks
// would never execute.
//
// N.B. Test_UpgradePaths below is a more exhaustive test for the soundness of `chooseUpgradePath`.
//
// [1] https://github.com/cockroachdb/cockroach/pull/151404
// [2] https://github.com/cockroachdb/cockroach/pull/146857
func Test_minimum_supported_version_regression(t *testing.T) {
	defer withTestBuildVersion("v25.2.1")()

	mvt := newBasicUpgradeTest(
		NumUpgrades(4),
		MinimumSupportedVersion("v24.3.1"),
		// N.B. prior to the fix in [2], this would yield a runtime panic of the form,
		// panic: runtime error: slice bounds out of range [:-1]
		// owing to slices.IndexFunc in setupTest, failing to find a _from_ version <= MinimumSupportedVersion("v24.3.1").
		WithSkipVersionProbability(1))

	assertValidTest(mvt, t.Fatal)
	plan, err := mvt.plan()
	require.NoError(t, err)
	require.NotNil(t, plan)
	// We expect the upgrade path to be ...-> v24.2.x -> v24.3.1 -> v24.3.x -> v25.2.1
	require.GreaterOrEqual(t, len(plan.Versions()), mvt.options.minUpgrades,
		"expected at least %d upgrades in the plan", mvt.options.minUpgrades)
}

// A property-based test that generates random upgrade plans and verifies their validity.
//
// The generator uses `minimumSupported` as the oldest possible version, v25.3.0 as the latest possible version.
// It generates the following random inputs, constrained by the above oldest and latest versions,
//
//	finalVersion, minBootstrapVersion, minSupportedVersion, numUpgrades, skipVersions
//
// The asserted properties are described below in `assertNumUpgrades`, `assertMsbAndMsv`, `assertSkipUpgrade`.
// In summary, we check that for all possible number of upgrades between the initial and final versions, a test plan
// is feasible; i.e., the resulting test plan has the expected number of upgrades. We check that the minimum supported
// and bootstrap versions, when specified, yield a valid test plan. This also exercises `assertExpectedUserHooks`, which
// ensures that the resulting plan contains all expected user hooks. Finally, we check that skip upgrades are exercised,
// when feasible, and that the skip to the final version is prioritized.
func Test_UpgradePaths(t *testing.T) {
	rng, _ := randutil.NewTestRand()
	defer withTestBuildVersion("v25.3.0")()

	randomNthPredecessor := func(rng *rand.Rand, v *clusterupgrade.Version, n int) *clusterupgrade.Version {
		curVersion := v.Version
		for range n {
			versionStr, err := release.RandomPredecessor(rng, &curVersion)
			require.NoError(t, err)
			clusterupgradeVersion, err := clusterupgrade.ParseVersion(versionStr)
			require.NoError(t, err)
			curVersion = clusterupgradeVersion.Version
		}
		return &clusterupgrade.Version{Version: curVersion}
	}
	// N.B. technically, v22.1.0 is the oldest version with release data, but we want to streamline input generation.
	// By designating `minimumSupported` (v22.2.0) as oldest, we can avoid a special case wherein generated finalVersion
	// has the same release series as `minimumSupported`.
	oldestVersionSupported := minimumSupported
	newestPossibleVersion := clusterupgrade.MustParseVersion("v25.3.0")
	maxPossibleUpgrades, err := release.MajorReleasesBetween(&newestPossibleVersion.Version, &oldestVersionSupported.Version)
	require.NoError(t, err)

	generator := func(values []reflect.Value, rng *rand.Rand) {
		// We don't always want to upgrade into the latest release (v25.3) so pick a release
		// n predecessors older, excluding oldestVersionSupported.
		n := rng.Intn(maxPossibleUpgrades)
		finalVersion := randomNthPredecessor(rng, newestPossibleVersion, n)
		values[0] = reflect.ValueOf(finalVersion.Version.String())

		upgradesRemaining := maxPossibleUpgrades - n
		require.Greater(t, upgradesRemaining, 0)
		// Randomly choose minimumBootstrapVersion <= minimumSupportedVersion.
		// N.B. we use rejection sampling to ensure all ordered pairs are chosen with uniform distribution.
		chooseMbvAndMsv := func() (*clusterupgrade.Version, *clusterupgrade.Version) {
			for {
				// N.B. zero predecessor is the same as "no minimum bootstrap/supported version".
				versionA := randomNthPredecessor(rng, finalVersion, rng.Intn(upgradesRemaining+1))
				versionB := randomNthPredecessor(rng, finalVersion, rng.Intn(upgradesRemaining+1))

				if versionB.Version.AtLeast(versionA.Version) {
					return versionA, versionB
				}
				// Otherwise, reject the sample and try again.
				// N.B. it may seem tempting to swap the versions and return, but that would bias the distribution since
				// there are two ways to pick a valid pair when the versions are distinct, and one way when they are equal.
			}
		}
		mbv, msv := chooseMbvAndMsv()
		initialVersion := oldestVersionSupported // default, in case mbv is "zero".
		if mbv.Version.Equals(finalVersion.Version) {
			// Chosen minimumBootstrapVersion equals finalVersion, assume it's unspecified.
			values[1] = reflect.ValueOf("")
		} else {
			values[1] = reflect.ValueOf(mbv.Version.String())
			initialVersion = mbv
		}
		if msv.Version.Equals(finalVersion.Version) {
			// Chosen minimumSupportedVersion equals finalVersion, assume it's unspecified.
			values[2] = reflect.ValueOf("")
		} else {
			values[2] = reflect.ValueOf(msv.Version.String())
		}

		// Find the maximum amount of possible upgrades from the minBootstrapVersion
		// and pick a random value [1, maxUpgradesFromMBV] to set maxUpgrades to.
		maxUpgradesFromMBV, err := release.MajorReleasesBetween(&finalVersion.Version, &initialVersion.Version)
		require.NoError(t, err)

		maxUpgrades := rng.Intn(maxUpgradesFromMBV) + 1
		values[3] = reflect.ValueOf(maxUpgrades)
		values[4] = reflect.ValueOf(rng.Float64() > 0.5) // skipVersions
		values[5] = reflect.ValueOf(rng.Float64() > 0.5) // sameSeriesUpgrades
	}

	verifyPlan := func(
		finalVersion string, minBootstrapVersion string, minSupportedVersion string, maxUpgrades int, skipVersions bool, sameSeriesUpgrades bool,
	) bool {
		// The top level withTestBuildVersion will take care of resetting this for us.
		_ = withTestBuildVersion(finalVersion)

		genNewTest := func() *Test {
			// Set up our test plan using the generated values.
			// Use MaxUpgrades to set the upper bound; minUpgrades defaults to 1.
			// This leaves room for same-series upgrades when enabled, since
			// numUpgrades() picks majorUpgrades in [1, maxUpgrades] and
			// sameSeriesUpgrades in [0, maxUpgrades - majorUpgrades].
			var opts []CustomOption
			opts = append(opts, MaxUpgrades(maxUpgrades))
			if minBootstrapVersion != "" {
				opts = append(opts, MinimumBootstrapVersion(minBootstrapVersion))
			}
			if minSupportedVersion != "" {
				opts = append(opts, MinimumSupportedVersion(minSupportedVersion))
			}
			mvt := newTest(
				opts...,
			)
			// N.B. randomPredecessor is the default, but it can be overridden; e.g., SeparateProcessDeployment uses latestPredecessor.
			// Here, we want to ensure that we are testing all possible upgrade paths, so we explicitly set randomPredecessor.
			mvt.options.predecessorFunc = randomPredecessor

			if skipVersions {
				mvt.options.skipVersionProbability = 1
			} else {
				mvt.options.skipVersionProbability = 0
			}
			if sameSeriesUpgrades {
				mvt.options.sameSeriesUpgradeProbability = 1
			} else {
				mvt.options.sameSeriesUpgradeProbability = 0
			}
			// Setup user hooks so that we can exercise `mvt.assertExpectedUserHooks`.
			mvt.BeforeClusterStart("BeforeClusterStart", dummyHook)
			mvt.BackgroundFunc("Background", dummyHook)
			mvt.OnStartup("OnStartup", dummyHook)
			mvt.InMixedVersion("InMixedVersion", dummyHook)
			mvt.AfterUpgradeFinalized("AfterUpgradeFinalized", dummyHook)

			// As in mixedversion.NewTest, we must ensure that we have a valid test, before returning it.
			assertValidTest(mvt, t.Fatal)
			return mvt
		}
		// We have a valid test, let's get a plan.
		mvt := genNewTest()
		assertValidTest(mvt, t.Fatal)
		plan, err := mvt.plan()
		if err != nil {
			// Log the error and assert property failure.
			// N.B. To retain the stacktrace, use "%+v"; t.Error uses "%v".
			t.Errorf("%+v", err)
			return false
		}
		if plan == nil {
			t.Error("expected a valid plan; got nil")
			return false
		}
		// We have a valid plan owing to `assertValidPlan` in `mvt.plan()`.
		return true
	}
	// 100 iterations take ~1.5 seconds on mac m1pro
	numIterations := 100

	if skip.NightlyStress() {
		// Nightly stress tests take longer to run, so we increase the number of iterations.
		numIterations = 1000
	}

	cfg := quick.Config{
		MaxCount: numIterations,
		Rand:     rng,
		Values:   generator,
	}

	require.NoError(t, quick.Check(verifyPlan, &cfg))
}
