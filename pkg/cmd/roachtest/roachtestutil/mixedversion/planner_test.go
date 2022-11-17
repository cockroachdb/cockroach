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
	gosql "database/sql"
	"fmt"
	"io"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
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
		pv, err := clusterupgrade.PredecessorVersion(buildVersion)
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

	plan, err := mvt.plan()
	require.NoError(t, err)
	require.Len(t, plan.steps, 9)

	// Assert on the pretty-printed version of the test plan as that
	// asserts the ordering of the steps we want to take, and as a bonus
	// tests the printing function itself.
	expectedPrettyPlan := fmt.Sprintf(`
mixed-version test plan for upgrading from %[1]s to <current>:
├── starting cluster from fixtures for version "%[1]s" (1)
├── wait for nodes :1-4 to all have the same cluster version (same as binary version of node 1) (2)
├── preventing auto-upgrades by setting `+"`preserve_downgrade_option`"+`, with connection to node 4 (3)
├── upgrade nodes :1-4 from "%[1]s" to "<current>"
│   ├── restart node 2 with binary version <current> (4)
│   ├── restart node 1 with binary version <current> (5)
│   ├── run "mixed-version 1", with connection to node 3 (6)
│   ├── restart node 4 with binary version <current> (7)
│   ├── restart node 3 with binary version <current> (8)
│   └── run "mixed-version 2", with connection to node 3 (9)
├── downgrade nodes :1-4 from "<current>" to "%[1]s"
│   ├── restart node 3 with binary version %[1]s (10)
│   ├── restart node 4 with binary version %[1]s (11)
│   ├── run mixed-version hooks concurrently
│   │   ├── run "mixed-version 1", with connection to node 1, after 200ms delay (12)
│   │   └── run "mixed-version 2", with connection to node 1, after 200ms delay (13)
│   ├── restart node 2 with binary version %[1]s (14)
│   └── restart node 1 with binary version %[1]s (15)
├── upgrade nodes :1-4 from "%[1]s" to "<current>"
│   ├── restart node 3 with binary version <current> (16)
│   ├── run "mixed-version 1", with connection to node 1 (17)
│   ├── restart node 4 with binary version <current> (18)
│   ├── restart node 1 with binary version <current> (19)
│   ├── restart node 2 with binary version <current> (20)
│   └── run "mixed-version 2", with connection to node 2 (21)
├── finalize upgrade by resetting `+"`preserve_downgrade_option`"+`, with connection to node 3 (22)
├── run "mixed-version 2", with connection to node 1 (23)
└── wait for nodes :1-4 to all have the same cluster version (same as binary version of node 1) (24)
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
	requireConcurrentHooks(t, plan.steps[3], "startup 1", "startup 2")

	// Assert that AfterUpgradeFinalized hooks are scheduled to run in
	// the last step of the test.
	mvt = newTest(t)
	mvt.AfterUpgradeFinalized("finalizer 1", dummyHook)
	mvt.AfterUpgradeFinalized("finalizer 2", dummyHook)
	mvt.AfterUpgradeFinalized("finalizer 3", dummyHook)
	plan, err = mvt.plan()
	require.NoError(t, err)
	require.Len(t, plan.steps, 9)
	requireConcurrentHooks(t, plan.steps[8], "finalizer 1", "finalizer 2", "finalizer 3")
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

func dummyHook(*logger.Logger, *gosql.DB) error {
	return nil
}
