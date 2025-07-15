// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package failures

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// failureModeStage represents the state of the failure mode
// and determines what methods are allowed to be called on it.
type failureModeStage int

const (
	// uninitialized means the FailureMode has not been setup yet, or
	// we have called Cleanup() on it and setup needs to be redone.
	uninitialized failureModeStage = iota
	// readyForFailure means there is no active FailureMode injected by this Failer,
	// i.e. we have recovered from a previous failure mode, or we never injected one.
	readyForFailure
	// activeFailure means there is an active FailureMode injected by this Failer.
	// The Failer will not allow another FailureMode to be injected until the current
	// one has been recovered. However, multiple different Failers can inject
	// FailureModes concurrently.
	activeFailure
	// If something went wrong at any point during the FailureMode, we set the state to this.
	// It is unclear what kind of state the cluster is in and steps may not necessarily be idempotent.
	// Instead, this state indicates that the failure mode should be cleaned up for safety. Note this
	// doesn't apply to WaitForFailureToPropagate or WaitForFailureToRecover, which shouldn't modify
	// anything in the cluster, only monitor it.
	invalid
)

func (s failureModeStage) String() string {
	switch s {
	case uninitialized:
		return "uninitialized"
	case readyForFailure:
		return "readyForFailure"
	case activeFailure:
		return "activeFailure"
	case invalid:
		return "invalid"
	default:
		panic("unknown failure mode stage")
	}
}

// Failer is a wrapper over a FailureMode that enforces that we maintain
// a consistent state in regards to a single failure mode usage. A single
// Failer only allows one failure to be injected at a time, but multiple
// failure modes can be injected by using multiple Failer instances created
// by newFailer. These Failer maintain their own state regarding the lifecyle
// of the FailureMode, but do not duplicate Setup or Cleanup work.
//
// The following diagram illustrates the valid states and transitions:
//
//          ┌───────────────┐◄────────────────────────────┐
//     ┌───►│ uninitialized │                             │
//     │    └───────┬───────┴────────────────┐            │
//     │            │                        │            │
// Cleanup()     Setup()                     │        Cleanup()
//     │            │                  Setup() Error      │
//     │            │                        │            │
//     │            │                        │            │
//     │            │                        │            │
//     │            ▼                        │            │
//     │    ┌───────────────┐ Inject() Error │     ┌──────┴──────┐
//     ├────┤readyForFailure├───────OR───────┼────►│   invalid   │◄───────┐
//     │    └─┬─────────────┘Cleanup() Error │     └──────────┬──┘        │
//     │      │          ▲                   │                │           │
//     │      │          │                   │                │           │
//     │      │          │            Recover() Error         │           │
//     │   Inject()  Recover()               OR         Cleanup() Error───┘
//     │      │          │            Cleanup() Error
//     │      │          │                   │
//     │      ▼          │                   │
//     │    ┌───────────────┐                │
//     └────┤ activeFailure ├────────────────┘
//          └───────────────┘

type Failer struct {
	sharedFailer
	state        failureModeStage
	injectArgs   FailureArgs
	TestingKnobs struct {
		// InjectedTransitionError if set, will be the error that returned
		// after any state transition method is called, if one is not already
		// returned by the underlying FailureMode method itself.
		InjectedTransitionError error
	}
}

// sharedFailer represents a way to access state that Failers
// of the same FailureMode acting on the same cluster share.
// Setup and Cleanup need to be called for each Failer to maintain
// a consistent state, however the underlying FailureMode's Setup
// and Cleanup may not necessarily be idempotent. sharedFailer provides
// a way to synchronize these calls, such that only the first Setup required
// actually sets up, while only the last Cleanup actually cleans up.
type sharedFailer interface {
	newFailer() (*Failer, error)
	failureMode() FailureMode
	maybeSetup(ctx context.Context, l *logger.Logger, args FailureArgs) error
	maybeCleanup(ctx context.Context, l *logger.Logger) error
}

// sharedFailerImpl manages the shared setup state for a FailureMode and creates
// Failer instances that can inject the same failure mode concurrently.
type sharedFailerImpl struct {
	syncutil.Mutex
	failure   FailureMode
	setupArgs FailureArgs
	// initializedFailers tracks the number of Failer instances that are initialized.
	// We can only safely cleanup the failure mode when we have no initialized Failers left.
	initializedFailers int
}

func (f *sharedFailerImpl) newFailer() (*Failer, error) {
	f.Lock()
	defer f.Unlock()
	f.initializedFailers++
	return &Failer{
		sharedFailer: f,
	}, nil
}

// FailureMode returns the shared FailureMode.
func (f *sharedFailerImpl) failureMode() FailureMode {
	return f.failure
}

// MaybeSetup sets up the FailureMode if it has not yet been initialized.
func (f *sharedFailerImpl) maybeSetup(
	ctx context.Context, l *logger.Logger, args FailureArgs,
) error {
	f.Lock()
	defer f.Unlock()
	f.initializedFailers++
	if f.initializedFailers > 1 {
		l.Printf("not setting up failure mode, already initialized")
		return nil
	}

	f.setupArgs = args
	if err := f.failure.Setup(ctx, l, args); err != nil {
		return err
	}
	return nil
}

// MaybeCleanup cleans up the FailureMode if all Failers have indicated
// that they want cleanup to happen.
func (f *sharedFailerImpl) maybeCleanup(ctx context.Context, l *logger.Logger) error {
	f.Lock()
	defer f.Unlock()

	if f.initializedFailers > 1 {
		f.initializedFailers--
		l.Printf("not cleaning up failure mode, %d failers still initialized", f.initializedFailers)
		return nil
	}

	if err := f.failure.Cleanup(ctx, l, f.setupArgs); err != nil {
		return err
	}
	// Only decrement if cleanup was successful. If it wasn't we can't be
	// sure what state the cluster is in. Retrying the cleanup may be required.
	f.initializedFailers--
	return nil
}

// checkValidTransition is a helper that returns a generic error message if
// the current state of the Failer is not in one of the valid states passed.
func (f *Failer) checkValidTransition(validStates ...failureModeStage) error {
	validStateMap := make(map[failureModeStage]struct{}, len(validStates))
	for _, state := range validStates {
		validStateMap[state] = struct{}{}
	}

	switch f.state {
	case invalid:
		if _, ok := validStateMap[invalid]; ok {
			return nil
		}
		return errors.New("failure mode is in invalid state, Cleanup() the failure mode and retry")
	case uninitialized:
		if _, ok := validStateMap[uninitialized]; ok {
			return nil
		}
		return errors.New("failure mode is not setup")
	case activeFailure:
		if _, ok := validStateMap[activeFailure]; ok {
			return nil
		}
		return errors.New("there is an active failure mode injected")
	case readyForFailure:
		if _, ok := validStateMap[readyForFailure]; ok {
			return nil
		}
		return errors.New("there is no failure mode injected")
	}
	return nil
}

// NewFailer returns a new Failer of the same FailureMode that is initialized,
// i.e. Setup() is not required, but can be used to inject different failures
// than the original Failer.
//
// Consider the following usage where we wish to inject two different dmsetup
// failures on the same cluster:
//
//	f1 := registry.GetFailer()
//	f1.Setup()
//	f2.Inject(stallReadsOnN1Args)
//	f2 := f1.NewFailer()
//	f2.Inject(stallWritesOnN2Args)
//	f1.Cleanup()
//	f2.Cleanup()
//
// f1 and f2 are able to inject different failures onto the same cluster,
// but don't require separate Setup() calls and do not actually Cleanup()
// until both are ready to do so.
func (f *Failer) NewFailer() (*Failer, error) {
	if err := f.checkValidTransition(readyForFailure, activeFailure); err != nil {
		return nil, err
	}
	newFailer, err := f.sharedFailer.newFailer()
	if err != nil {
		return nil, err
	}
	// We can skip setup since we gate newFailer calls to failers that
	// have already initialized.
	newFailer.state = readyForFailure
	return newFailer, nil
}

func (f *Failer) Setup(ctx context.Context, l *logger.Logger, args FailureArgs) error {
	if f.state != uninitialized {
		return errors.New("failure mode is already setup")
	}

	f.state = readyForFailure
	if err := f.maybeSetup(ctx, l, args); err != nil {
		f.state = invalid
		return err
	}

	if f.TestingKnobs.InjectedTransitionError != nil {
		f.state = invalid
		return f.TestingKnobs.InjectedTransitionError
	}
	return nil
}

func (f *Failer) Inject(ctx context.Context, l *logger.Logger, args FailureArgs) error {
	if err := f.checkValidTransition(readyForFailure); err != nil {
		return err
	}

	f.state = activeFailure
	f.injectArgs = args
	if err := f.failureMode().Inject(ctx, l, f.injectArgs); err != nil {
		f.state = invalid
		return err
	}

	if f.TestingKnobs.InjectedTransitionError != nil {
		f.state = invalid
		return f.TestingKnobs.InjectedTransitionError
	}
	return nil
}

func (f *Failer) WaitForFailureToPropagate(ctx context.Context, l *logger.Logger) error {
	if err := f.checkValidTransition(activeFailure); err != nil {
		return err
	}

	return f.failureMode().WaitForFailureToPropagate(ctx, l, f.injectArgs)
}

func (f *Failer) Recover(ctx context.Context, l *logger.Logger) error {
	if err := f.checkValidTransition(activeFailure); err != nil {
		return err
	}
	f.state = readyForFailure

	if err := f.failureMode().Recover(ctx, l, f.injectArgs); err != nil {
		f.state = invalid
		return err
	}

	if f.TestingKnobs.InjectedTransitionError != nil {
		f.state = invalid
		return f.TestingKnobs.InjectedTransitionError
	}
	return nil
}

func (f *Failer) WaitForFailureToRecover(ctx context.Context, l *logger.Logger) error {
	if err := f.checkValidTransition(readyForFailure); err != nil {
		return err
	}

	return f.failureMode().WaitForFailureToRecover(ctx, l, f.injectArgs)
}

func (f *Failer) Cleanup(ctx context.Context, l *logger.Logger) error {
	if err := f.checkValidTransition(readyForFailure, activeFailure, invalid); err != nil {
		return err
	}

	if f.state == activeFailure {
		if err := f.Recover(ctx, l); err != nil {
			l.Printf("failed to recover failure mode: %s\nproceeding with Cleanup()", err)
		}
	}

	f.state = uninitialized
	if err := f.maybeCleanup(ctx, l); err != nil {
		f.state = invalid
		return err
	}

	if f.TestingKnobs.InjectedTransitionError != nil {
		f.state = invalid
		return f.TestingKnobs.InjectedTransitionError
	}
	return nil
}
