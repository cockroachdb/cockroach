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
	"context"
	"fmt"
	"io"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

var (
	nilLogger = func() *logger.Logger {
		cfg := logger.Config{
			Stdout: io.Discard,
			Stderr: io.Discard,
		}
		l, err := cfg.NewLogger("/dev/null" /* path */)
		if err != nil {
			panic(err)
		}

		return l
	}()

	ctx   = context.Background()
	nodes = option.NodeListOption{1, 2, 3, 4}

	// Hardcode build and previous versions so that the test won't fail
	// when new versions are released.
	buildVersion       = version.MustParse("v23.1.0")
	predecessorVersion = "v22.2.8"
)

const seed = 12345 // expectations are based on this seed

func TestTestPlanner(t *testing.T) {
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
	resetMutators := func() { planMutators = nil }
	resetBuildVersion := setBuildVersion()
	defer func() {
		resetBuildVersion()
		resetMutators()
	}()

	datadriven.Walk(t, datapathutils.TestDataPath(t, "planner"), func(t *testing.T, path string) {
		resetMutators()
		mvt := newTest()

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			if d.Cmd == "plan" {
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
	generateData := func(generateMoreRandomNumbers bool) [][]int {
		var generatedData [][]int
		mvt := newTest()
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

		plan, err := mvt.plan()
		require.NoError(t, err)

		upgradeStep := plan.Steps()[3].(sequentialRunStep)

		// We can hardcode these paths since we are using a fixed seed in
		// these tests.
		firstRunStep := upgradeStep.steps[1].(sequentialRunStep).steps[2].(*singleStep)
		firstRun := firstRunStep.impl.(runHookStep)
		require.Equal(t, "do something", firstRun.hook.name)
		require.NoError(t, firstRun.Run(ctx, nilLogger, firstRunStep.rng, emptyHelper))

		secondRunStep := upgradeStep.steps[2].(sequentialRunStep).steps[3].(*singleStep)
		secondRun := secondRunStep.impl.(runHookStep)
		require.Equal(t, "do something", secondRun.hook.name)
		require.NoError(t, secondRun.Run(ctx, nilLogger, secondRunStep.rng, emptyHelper))

		thirdRunStep := upgradeStep.steps[3].(sequentialRunStep).steps[3].(*singleStep)
		thirdRun := thirdRunStep.impl.(runHookStep)
		require.Equal(t, "do something", thirdRun.hook.name)
		require.NoError(t, thirdRun.Run(ctx, nilLogger, thirdRunStep.rng, emptyHelper))

		require.Len(t, generatedData, 3)
		return generatedData
	}

	expectedData := [][]int{
		{30, 17, 84, 24, 33},
		{32, 20, 74, 99, 55},
		{82, 60, 36, 78, 51},
	}
	const numRums = 50
	for j := 0; j < numRums; j++ {
		for _, b := range []bool{true, false} {
			require.Equal(t, expectedData, generateData(b), "j = %d | b = %t", j, b)
		}
	}
}

// Test_startClusterID tests that the plan generated by the test
// planner keeps track of the correct ID for the test's start step.
func Test_startClusterID(t *testing.T) {
	// When fixtures are disabled, the startStep should always be the
	// first step of the test (ID = 1).
	mvt := newTest(NeverUseFixtures)
	plan, err := mvt.plan()
	require.NoError(t, err)

	ss := plan.Steps()[0].(*singleStep)
	require.IsType(t, startStep{}, ss.impl)
	require.Equal(t, 1, ss.ID)
	require.Equal(t, 1, plan.startClusterID)

	// Overwrite probability to 1 so that our test plan will always
	// start the cluster from fixtures.
	origProbability := defaultTestOptions.useFixturesProbability
	defaultTestOptions.useFixturesProbability = 1
	defer func() { defaultTestOptions.useFixturesProbability = origProbability }()

	// When fixtures are used, the startStep should always be the second
	// step of the test (ID = 2), after fixtures are installed.
	mvt = newTest()
	plan, err = mvt.plan()
	require.NoError(t, err)
	ss = plan.Steps()[1].(*singleStep)
	require.IsType(t, startStep{}, ss.impl)
	require.Equal(t, 2, ss.ID)
	require.Equal(t, 2, plan.startClusterID)
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

func setBuildVersion() func() {
	previousV := clusterupgrade.TestBuildVersion
	clusterupgrade.TestBuildVersion = buildVersion

	return func() { clusterupgrade.TestBuildVersion = previousV }
}

func newRand() *rand.Rand {
	return rand.New(rand.NewSource(seed))
}

func newTest(options ...CustomOption) *Test {
	testOptions := defaultTestOptions
	for _, fn := range options {
		fn(&testOptions)
	}

	return &Test{
		ctx:             ctx,
		logger:          nilLogger,
		crdbNodes:       nodes,
		options:         testOptions,
		_arch:           archP(vm.ArchAMD64),
		_isLocal:        boolP(false),
		prng:            newRand(),
		hooks:           &testHooks{crdbNodes: nodes},
		predecessorFunc: testPredecessorFunc,
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
func testPredecessorFunc(
	rng *rand.Rand, v *clusterupgrade.Version, n int,
) ([]*clusterupgrade.Version, error) {
	return parseVersions([]string{predecessorVersion}), nil
}

// createDataDrivenMixedVersionTest creates a `*Test` instance based
// on the parameters passed to the `mixed-version-test` datadriven
// directive.
func createDataDrivenMixedVersionTest(t *testing.T, args []datadriven.CmdArg) *Test {
	var opts []CustomOption
	var predecessors predecessorFunc
	var isLocal *bool

	for _, arg := range args {
		switch arg.Key {
		case "predecessors":
			arg := arg // copy range variable
			predecessors = func(rng *rand.Rand, v *clusterupgrade.Version, n int) ([]*clusterupgrade.Version, error) {
				return parseVersions(arg.Vals), nil
			}

		case "num_upgrades":
			n, err := strconv.Atoi(arg.Vals[0])
			require.NoError(t, err)
			opts = append(opts, NumUpgrades(n))

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

		default:
			t.Errorf("unknown mixed-version-test option: %s", arg.Key)
		}
	}

	mvt := newTest(opts...)

	if isLocal != nil {
		mvt._isLocal = isLocal
	}
	if predecessors != nil {
		mvt.predecessorFunc = predecessors
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
				return s.context.Stage == BackgroundStage // no background steps in the plan used in the test
			},
			assertStepsFunc: func(t *testing.T, sel stepSelector) {
				require.Empty(t, sel)
			},
			expectedRandomStepType: nil, // no steps selected
		},
		{
			name: "filtering by a specific stage",
			predicate: func(s *singleStep) bool {
				return s.context.Stage == AfterUpgradeFinalizedStage
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
				return s.context.ToVersion.IsCurrent() && s.context.Stage == AfterUpgradeFinalizedStage
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
			mvt := newBasicUpgradeTest(NumUpgrades(3))
			mvt.predecessorFunc = func(rng *rand.Rand, v *clusterupgrade.Version, n int) ([]*clusterupgrade.Version, error) {
				return parseVersions([]string{"v22.2.2", "v23.1.9", "v23.2.0"}), nil
			}

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
				return s.context.Stage == BackgroundStage // no background steps
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
				return s.context.Stage == AfterUpgradeFinalizedStage
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
				return s.context.Stage == AfterUpgradeFinalizedStage
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
				return s.context.Stage == BackgroundStage // no background steps
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
				return s.context.Stage == AfterUpgradeFinalizedStage
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
				return s.context.Stage == AfterUpgradeFinalizedStage
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
			mvt := newBasicUpgradeTest(NumUpgrades(tc.numUpgrades))
			mvt.predecessorFunc = func(rng *rand.Rand, v *clusterupgrade.Version, n int) ([]*clusterupgrade.Version, error) {
				return parseVersions([]string{"v22.2.2", "v23.1.9", "v23.2.0"})[:tc.numUpgrades], nil
			}

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

func (concurrentUserHooksMutator) Generate(rng *rand.Rand, plan *TestPlan) []mutation {
	// Insert our `testSingleStep` implementation concurrently with every
	// user-provided function.
	return plan.
		newStepSelector().
		Filter(func(s *singleStep) bool {
			_, ok := s.impl.(runHookStep)
			return ok
		}).
		InsertConcurrent(&testSingleStep{})
}

// removeUserHooksMutator is a test mutator that removes every
// user-provided hook from the plan.
type removeUserHooksMutator struct{}

func (removeUserHooksMutator) Name() string         { return "remove_user_hooks_mutator" }
func (removeUserHooksMutator) Probability() float64 { return 0.5 }

func (removeUserHooksMutator) Generate(rng *rand.Rand, plan *TestPlan) []mutation {
	return plan.
		newStepSelector().
		Filter(func(s *singleStep) bool {
			_, ok := s.impl.(runHookStep)
			return ok
		}).
		Remove()
}

func dummyHook(context.Context, *logger.Logger, *rand.Rand, *Helper) error {
	return nil
}
