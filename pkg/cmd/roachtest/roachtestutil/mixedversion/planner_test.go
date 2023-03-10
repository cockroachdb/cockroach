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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/version"
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

	ctx   = context.Background()
	nodes = option.NodeListOption{1, 2, 3, 4}

	// Hardcode build and previous versions so that the test won't fail
	// when new versions are released.
	buildVersion = func() version.Version {
		bv, err := version.Parse("v23.1.0")
		if err != nil {
			panic(err)
		}
		return *bv
	}()
	previousVersion = func() string {
		pv, err := version.PredecessorVersion(buildVersion)
		if err != nil {
			panic(err)
		}
		return pv
	}()
)

const (
	seed        = 12345 // expectations are based on this seed
	mainVersion = clusterupgrade.MainVersion
)

func TestTestPlanner(t *testing.T) {
	mvt := newTest(t)
	mvt.InMixedVersion("mixed-version 1", dummyHook)
	mvt.InMixedVersion("mixed-version 2", dummyHook)
	initBank := roachtestutil.NewCommand("./cockroach workload bank init")
	runBank := roachtestutil.NewCommand("./cockroach workload run bank").Flag("max-ops", 100)
	mvt.Workload("bank", nodes, initBank, runBank)
	runRand := roachtestutil.NewCommand("./cockroach run rand").Flag("seed", 321)
	mvt.Workload("rand", nodes, nil /* initCmd */, runRand)
	csvServer := roachtestutil.NewCommand("./cockroach workload csv-server").Flag("port", 9999)
	mvt.BackgroundCommand("csv server", nodes, csvServer)

	plan, err := mvt.plan()
	require.NoError(t, err)
	require.Len(t, plan.steps, 12)

	// Assert on the pretty-printed version of the test plan as that
	// asserts the ordering of the steps we want to take, and as a bonus
	// tests the printing function itself.
	expectedPrettyPlan := fmt.Sprintf(`
mixed-version test plan for upgrading from %[1]s to <current>:
├── starting cluster from fixtures for version "%[1]s" (1)
├── upload current binary to all cockroach nodes (:1-4) (2)
├── wait for nodes :1-4 to all have the same cluster version (same as binary version of node 1) (3)
├── preventing auto-upgrades by setting `+"`preserve_downgrade_option`"+` (4)
├── run "initialize bank workload" (5)
├── start background hooks concurrently
│   ├── run "bank workload", after 50ms delay (6)
│   ├── run "rand workload", after 50ms delay (7)
│   └── run "csv server", after 200ms delay (8)
├── upgrade nodes :1-4 from "%[1]s" to "<current>"
│   ├── restart node 4 with binary version <current> (9)
│   ├── run mixed-version hooks concurrently
│   │   ├── run "mixed-version 1", after 100ms delay (10)
│   │   └── run "mixed-version 2", after 100ms delay (11)
│   ├── restart node 3 with binary version <current> (12)
│   ├── restart node 2 with binary version <current> (13)
│   └── restart node 1 with binary version <current> (14)
├── downgrade nodes :1-4 from "<current>" to "%[1]s"
│   ├── restart node 2 with binary version %[1]s (15)
│   ├── run "mixed-version 1" (16)
│   ├── restart node 1 with binary version %[1]s (17)
│   ├── run "mixed-version 2" (18)
│   ├── restart node 3 with binary version %[1]s (19)
│   └── restart node 4 with binary version %[1]s (20)
├── upgrade nodes :1-4 from "%[1]s" to "<current>"
│   ├── restart node 4 with binary version <current> (21)
│   ├── restart node 3 with binary version <current> (22)
│   ├── restart node 1 with binary version <current> (23)
│   ├── run mixed-version hooks concurrently
│   │   ├── run "mixed-version 1", after 0s delay (24)
│   │   └── run "mixed-version 2", after 0s delay (25)
│   └── restart node 2 with binary version <current> (26)
├── finalize upgrade by resetting `+"`preserve_downgrade_option`"+` (27)
├── run mixed-version hooks concurrently
│   ├── run "mixed-version 1", after 100ms delay (28)
│   └── run "mixed-version 2", after 0s delay (29)
└── wait for nodes :1-4 to all have the same cluster version (same as binary version of node 1) (30)
`, previousVersion,
	)

	expectedPrettyPlan = expectedPrettyPlan[1:] // remove leading newline
	require.Equal(t, expectedPrettyPlan, plan.PrettyPrint())

	// Assert that startup hooks are scheduled to run before any
	// upgrades, i.e., after cluster is initialized (step 1), and after
	// we wait for the cluster version to match on all nodes (step 2).
	mvt = newTest(t)
	mvt.OnStartup("startup 1", dummyHook)
	mvt.OnStartup("startup 2", dummyHook)
	plan, err = mvt.plan()
	require.NoError(t, err)
	requireConcurrentHooks(t, plan.steps[4], "startup 1", "startup 2")

	// Assert that AfterUpgradeFinalized hooks are scheduled to run in
	// the last step of the test.
	mvt = newTest(t)
	mvt.AfterUpgradeFinalized("finalizer 1", dummyHook)
	mvt.AfterUpgradeFinalized("finalizer 2", dummyHook)
	mvt.AfterUpgradeFinalized("finalizer 3", dummyHook)
	plan, err = mvt.plan()
	require.NoError(t, err)
	require.Len(t, plan.steps, 10)
	requireConcurrentHooks(t, plan.steps[9], "finalizer 1", "finalizer 2", "finalizer 3")
}

// TestDeterministicTestPlan tests that generating a test plan with
// the same seed multiple times yields the same plan every time.
func TestDeterministicTestPlan(t *testing.T) {
	makePlan := func() *TestPlan {
		mvt := newTest(t)
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
		mvt := newTest(t)
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

		// We can hardcode these paths since we are using a fixed seed in
		// these tests.
		firstRun := plan.steps[4].(sequentialRunStep).steps[2].(runHookStep)
		require.Equal(t, "do something", firstRun.hook.name)
		require.NoError(t, firstRun.Run(ctx, nilLogger, nilCluster, emptyHelper))

		secondRun := plan.steps[5].(sequentialRunStep).steps[3].(runHookStep)
		require.Equal(t, "do something", secondRun.hook.name)
		require.NoError(t, secondRun.Run(ctx, nilLogger, nilCluster, emptyHelper))

		thirdRun := plan.steps[6].(sequentialRunStep).steps[1].(runHookStep)
		require.Equal(t, "do something", thirdRun.hook.name)
		require.NoError(t, thirdRun.Run(ctx, nilLogger, nilCluster, emptyHelper))

		fourthRun := plan.steps[8].(runHookStep)
		require.Equal(t, "do something", fourthRun.hook.name)
		require.NoError(t, fourthRun.Run(ctx, nilLogger, nilCluster, emptyHelper))

		require.Len(t, generatedData, 4)
		return generatedData
	}

	expectedData := [][]int{
		{82, 1, 17, 3, 87},
		{73, 17, 6, 37, 43},
		{82, 35, 57, 54, 8},
		{7, 95, 26, 31, 65},
	}
	const numRums = 50
	for j := 0; j < numRums; j++ {
		for _, b := range []bool{true, false} {
			require.Equal(t, expectedData, generateData(b), "j = %d | b = %t", j, b)
		}
	}
}

func newTest(t *testing.T) *Test {
	prng := rand.New(rand.NewSource(seed))
	return &Test{
		ctx:           ctx,
		logger:        nilLogger,
		crdbNodes:     nodes,
		_buildVersion: buildVersion,
		prng:          prng,
		hooks:         &testHooks{prng: prng, crdbNodes: nodes},
	}
}

// requireConcurrentHooks asserts that the given step is a concurrent
// run of multiple user-provided hooks with the names passed as
// parameter.
func requireConcurrentHooks(t *testing.T, step testStep, names ...string) {
	require.IsType(t, concurrentRunStep{}, step)
	crs := step.(concurrentRunStep)
	require.Len(t, crs.delayedSteps, len(names))

	for j, concurrentStep := range crs.delayedSteps {
		require.IsType(t, delayedStep{}, concurrentStep)
		ds := concurrentStep.(delayedStep)
		require.IsType(t, runHookStep{}, ds.step)
		rhs := ds.step.(runHookStep)
		require.Equal(t, names[j], rhs.hook.name, "j = %d", j)
	}
}

func dummyHook(context.Context, *logger.Logger, *rand.Rand, *Helper) error {
	return nil
}
