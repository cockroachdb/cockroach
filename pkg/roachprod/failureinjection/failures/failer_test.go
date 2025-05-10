// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package failures

import (
	"context"
	"io"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func nilLogger() *logger.Logger {
	lcfg := logger.Config{
		Stdout: io.Discard,
		Stderr: io.Discard,
	}
	l, err := lcfg.NewLogger("" /* path */)
	if err != nil {
		panic(err)
	}
	return l
}

// failureModeStateTransition represents FailureMode method that needs to
// be validated before running to ensure we maintain a consistent state.
type failureModeStateTransition int

const (
	Setup failureModeStateTransition = iota
	Inject
	WaitForFailureToPropagate
	Recover
	WaitForFailureToRecover
	Cleanup
)

func (f failureModeStateTransition) String() string {
	switch f {
	case Setup:
		return "Setup()"
	case Inject:
		return "Inject()"
	case WaitForFailureToPropagate:
		return "WaitForFailureToPropagate()"
	case Recover:
		return "Recover()"
	case WaitForFailureToRecover:
		return "WaitForFailureToRecover()"
	case Cleanup:
		return "Cleanup()"
	default:
		panic("unknown transition")
	}
}

func generateFailerPlan(rng *rand.Rand) []failureModeStateTransition {
	numSteps := 10000
	plan := make([]failureModeStateTransition, 0, numSteps)
	for range numSteps {
		plan = append(plan, failureModeStateTransition(rng.Intn(6)))
	}
	return plan
}

// isTransitionLegal checks if the given transition is legal
// based on the current state of the Failer. If false, then the
// Failer should error out and not attempt to run the transition.
func isTransitionLegal(f *Failer, transition failureModeStateTransition) bool {
	switch transition {
	case Setup:
		// We can only call Setup() if we haven't called it yet or if
		// we reversed it with a Cleanup().
		if f.state == uninitialized {
			return true
		}
	case Inject:
		// We can only call Inject() if there is no active failure, and
		// we have initialized the failure mode.
		if f.state == readyForFailure {
			return true
		}
	case WaitForFailureToPropagate:
		// WaitForFailureToPropagate() can only be called if there is an active failure.
		if f.state == activeFailure {
			return true
		}
	case Recover:
		// We can only call Recover() if there is an active failure injected.
		if f.state == activeFailure {
			return true
		}
	case WaitForFailureToRecover:
		// WaitForFailureToRecover() can only be called if there is no active failure,
		// i.e. it was recovered.
		if f.state == readyForFailure {
			return true
		}
	case Cleanup:
		// We can call Cleanup() from any state except for when we haven't yet called Setup(),
		// i.e. there is nothing to clean up.
		if f.state != uninitialized {
			return true
		}
	}
	return false
}

func runTransitionStep(
	f *Failer, transition failureModeStateTransition, injectedTransitionError error,
) error {
	args := NoopFailureArgs{
		InjectedError: injectedTransitionError,
	}
	// This is normally unsafe to do as it defeats the purpose of Failer
	// keeping track of args, but should be okay as this injected error
	// used for testing is the only state kept by noopFailure.
	f.setupArgs = args
	f.injectArgs = args

	switch transition {
	case Setup:
		return f.Setup(context.Background(), nilLogger(), f.setupArgs)
	case Inject:
		return f.Inject(context.Background(), nilLogger(), f.injectArgs)
	case WaitForFailureToPropagate:
		return f.WaitForFailureToPropagate(context.Background(), nilLogger())
	case Recover:
		return f.Recover(context.Background(), nilLogger())
	case WaitForFailureToRecover:
		return f.WaitForFailureToRecover(context.Background(), nilLogger())
	case Cleanup:
		return f.Cleanup(context.Background(), nilLogger())
	default:
		panic("unknown transition")
	}
}

func Test_FailerLifecycle(t *testing.T) {
	fr := NewFailureRegistry()
	fr.Register()
	f, err := fr.GetFailer("", NoopFailureName, nilLogger(), false /* secure */)
	require.NoError(t, err)

	rng, _ := randutil.NewTestRand()
	plan := generateFailerPlan(rng)

	injectedTransitionErr := errors.New("forced error")

	var step int
	var transition failureModeStateTransition
	var testPassed bool
	defer func() {
		if !testPassed {
			t.Log("Test failed, printing state")
			t.Logf("Failed at step %d", step+1)
			t.Logf("Current state: %s", f.state)
			t.Logf("Failed transition: %s", transition)
		}
	}()

	for step, transition = range plan {
		// Simulate a 10% chance of our given transitions failing
		// and leaving the Failer in the invalid state.
		forceTransitionError := rng.Float64() < 0.1
		var injectedErr error
		if forceTransitionError {
			injectedErr = injectedTransitionErr
		}

		if isTransitionLegal(f, transition) {
			if err = runTransitionStep(f, transition, injectedErr); err != nil {
				// If the transition is expected to succeed, and we see it fail,
				// it must be because we injected an error, i.e. something went wrong
				// with the Inject/Recover/etc. itself.
				require.ErrorIs(t, err, injectedTransitionErr)
				// Because we injected an error, it should have left the Failer in
				// the invalid state if the transition was not WaitForFailureToPropagate
				// or WaitForFailureToRecover.
				if transition != WaitForFailureToPropagate && transition != WaitForFailureToRecover {
					require.Equal(t, f.state, invalid)
				}
			}
		} else {
			// If the transition is expected to fail, we should see an error.
			err = runTransitionStep(f, transition, injectedTransitionErr)
			require.Error(t, err)
			// Additionally, it should never be because we injected an error.
			// It should error out when validating if the transition is legal,
			// and before we even call the transition.
			require.NotErrorIs(t, err, injectedTransitionErr)
		}
	}
	testPassed = true
}
