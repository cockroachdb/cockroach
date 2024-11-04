// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mixedversion

import (
	"context"
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/task"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func successStep() *singleStep {
	return newTestStep(func() error { return nil })
}

func errorStep() *singleStep {
	return newTestStep(func() error { return fmt.Errorf("oops") })
}

func panicStep() *singleStep {
	return newTestStep(func() error {
		var ids []int
		if ids[0] > 42 {
			return nil
		}
		return fmt.Errorf("unreachable")
	})
}

func Test_runSingleStep(t *testing.T) {
	tr := testTestRunner()

	// steps that run without errors do not return errors
	err := tr.runSingleStep(ctx, successStep(), nilLogger)
	require.NoError(t, err)

	// steps that return an error have that error surfaced
	err = tr.runSingleStep(ctx, errorStep(), nilLogger)
	require.Error(t, err)
	require.Contains(t, err.Error(), "oops")

	// steps that panic cause an error to be returned
	err = nil
	require.NotPanics(t, func() {
		err = tr.runSingleStep(ctx, panicStep(), nilLogger)
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "panic (stack trace above): runtime error: index out of range [0] with length 0")
}

// Test_run verifies that the test runner's `run` function is able to
// appropriately change ownership to Test Eng when no user provided
// functions have run at the time the failure happened.
func Test_run(t *testing.T) {
	hookStep := func(name string, retErr error) *singleStep {
		step := runHookStep{
			hook: versionUpgradeHook{
				name: fmt.Sprintf("hook %s", name),
				fn: func(_ context.Context, _ *logger.Logger, _ *rand.Rand, _ *Helper) error {
					return retErr
				},
			},
		}

		initialVersion := clusterupgrade.MustParseVersion(predecessorVersion)
		return newSingleStep(
			newInitialContext(initialVersion, nodes, nil),
			step,
			newRand(),
		)
	}

	successfulHook := func() *singleStep { return hookStep("success", nil) }
	buggyHook := func() *singleStep { return hookStep("buggy", errors.New("oops")) }

	testCases := []struct {
		name                  string
		steps                 []testStep
		expectOwnershipChange bool
	}{
		{
			name:                  "error in user-provided step",
			steps:                 []testStep{successStep(), buggyHook(), errorStep()},
			expectOwnershipChange: false,
		},
		{
			name:                  "error in test step after user-hook ran",
			steps:                 []testStep{successStep(), successfulHook(), errorStep()},
			expectOwnershipChange: false,
		},
		{
			name:                  "error in test step before user-hook ran",
			steps:                 []testStep{successStep(), errorStep(), buggyHook()},
			expectOwnershipChange: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			runner := testTestRunner()
			runner.plan = &TestPlan{
				setup:     testSetup{systemSetup: &serviceSetup{}},
				initSteps: tc.steps,
				// Set an artificially large `startSystemID` to stop the test
				// runner from attempting to perform post-initialization tasks
				// that wouldn't work in this limited test environment.
				startSystemID: 9999,
			}

			runnerCh := make(chan error)
			defer close(runnerCh)
			runner.monitor = &crdbMonitor{errCh: runnerCh}

			runErr := runner.run()
			require.Error(t, runErr)

			var ref registry.ErrorWithOwnership
			ownershipChanged := errors.As(runErr, &ref)
			if tc.expectOwnershipChange {
				require.True(
					t, ownershipChanged,
					"failures before user functions ran SHOULD overwrite ownership: %v",
					runErr,
				)
			} else {
				require.False(
					t, ownershipChanged,
					"failures in user functions should NOT overwrite ownership: %v",
					runErr,
				)
			}
		})
	}
}

func testAddAnnotation() error {
	return nil
}

func testTestRunner() *testRunner {
	runnerCtx, cancel := context.WithCancel(ctx)
	var ranUserHooks atomic.Bool
	systemDescriptor := &ServiceDescriptor{
		Name:  install.SystemInterfaceName,
		Nodes: nodes,
	}
	return &testRunner{
		ctx:            runnerCtx,
		cancel:         cancel,
		logger:         nilLogger,
		systemService:  newServiceRuntime(systemDescriptor),
		background:     task.NewManager(runnerCtx, nilLogger),
		ranUserHooks:   &ranUserHooks,
		plan:           &TestPlan{seed: seed},
		_addAnnotation: testAddAnnotation,
	}
}

type testSingleStep struct {
	runFunc func() error
}

func (s *testSingleStep) Description() string  { return "testSingleStep" }
func (*testSingleStep) Background() shouldStop { return nil }

func (tss *testSingleStep) Run(_ context.Context, _ *logger.Logger, _ *rand.Rand, _ *Helper) error {
	return tss.runFunc()
}

func newTestStep(f func() error) *singleStep {
	initialVersion := clusterupgrade.MustParseVersion(predecessorVersion)
	return newSingleStep(
		newInitialContext(initialVersion, nodes, nil),
		&testSingleStep{runFunc: f},
		newRand(),
	)
}
