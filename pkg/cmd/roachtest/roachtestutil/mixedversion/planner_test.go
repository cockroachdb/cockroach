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

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
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
	reset := setBuildVersion()
	defer reset()

	datadriven.Walk(t, datapathutils.TestDataPath(t, "planner"), func(t *testing.T, path string) {
		mvt := newTest()
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			if d.Cmd == "plan" {
				plan, err := mvt.plan()
				require.NoError(t, err)

				return plan.PrettyPrint()
			}

			switch d.Cmd {
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
				require.NoError(t, requireConcurrentHooks(t, plan.steps, d.CmdArgs[0].Vals...))
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
			nilCluster  cluster.Cluster
			emptyHelper = &Helper{}
		)

		plan, err := mvt.plan()
		require.NoError(t, err)

		upgradeStep := plan.steps[3].(sequentialRunStep)

		// We can hardcode these paths since we are using a fixed seed in
		// these tests.
		firstRun := upgradeStep.steps[1].(sequentialRunStep).steps[3].(singleStep).impl.(runHookStep)
		require.Equal(t, "do something", firstRun.hook.name)
		require.NoError(t, firstRun.Run(ctx, nilLogger, nilCluster, emptyHelper))

		secondRun := upgradeStep.steps[2].(sequentialRunStep).steps[1].(singleStep).impl.(runHookStep)
		require.Equal(t, "do something", secondRun.hook.name)
		require.NoError(t, secondRun.Run(ctx, nilLogger, nilCluster, emptyHelper))

		thirdRun := upgradeStep.steps[3].(sequentialRunStep).steps[3].(singleStep).impl.(runHookStep)
		require.Equal(t, "do something", thirdRun.hook.name)
		require.NoError(t, thirdRun.Run(ctx, nilLogger, nilCluster, emptyHelper))

		require.Len(t, generatedData, 3)
		return generatedData
	}

	expectedData := [][]int{
		{37, 94, 58, 5, 22},
		{56, 88, 23, 85, 45},
		{96, 91, 48, 85, 76},
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

	step, isStartStep := plan.steps[0].(singleStep).impl.(startStep)
	require.True(t, isStartStep)
	require.Equal(t, 1, step.ID())
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
	step, isStartStep = plan.steps[1].(singleStep).impl.(startStep)
	require.True(t, isStartStep)
	require.Equal(t, 2, step.ID())
	require.Equal(t, 2, plan.startClusterID)
}

// Test_upgradeTimeout tests the behaviour of upgrade timeouts in
// mixedversion tests. If no custom value is passed, the default
// timeout in the clusterupgrade package is used; otherwise, the
// custom value is enforced.
func Test_upgradeTimeout(t *testing.T) {
	findUpgradeWaitSteps := func(plan *TestPlan) []waitForStableClusterVersionStep {
		var steps []waitForStableClusterVersionStep
		for _, s := range plan.steps {
			if ss, isSingle := s.(singleStep); isSingle {
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

func newTest(options ...CustomOption) *Test {
	testOptions := defaultTestOptions
	for _, fn := range options {
		fn(&testOptions)
	}

	prng := rand.New(rand.NewSource(seed))
	return &Test{
		ctx:             ctx,
		logger:          nilLogger,
		crdbNodes:       nodes,
		options:         testOptions,
		_arch:           archP(vm.ArchAMD64),
		prng:            prng,
		hooks:           &testHooks{prng: prng, crdbNodes: nodes},
		predecessorFunc: testPredecessorFunc,
	}
}

func archP(a vm.CPUArch) *vm.CPUArch {
	return &a
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
		}
	}

	mvt := newTest(opts...)
	if predecessors != nil {
		mvt.predecessorFunc = predecessors
	}

	return mvt
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
				ss, ok := ds.step.(singleStep)
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

func dummyHook(context.Context, *logger.Logger, *rand.Rand, *Helper) error {
	return nil
}
