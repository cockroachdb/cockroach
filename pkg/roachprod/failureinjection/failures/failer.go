// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package failures

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
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
// a consistent state in regards to a single failure mode usage. A Failer is
// not thread-safe, and multiple Failers should be used for concurrent
// failure injection instead.
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
	FailureMode
	state failureModeStage
	// setupArgs stores the FailureArgs passed into Setup() so that we can
	// make sure to pass the same args into Cleanup().
	setupArgs FailureArgs
	// injectArgs stores the FailureArgs passed into Inject() so that we can
	// make sure to pass the same args into Recover(), WaitForFailureToPropagate(),
	// and WaitForFailureToRecover(). We make the distinction between setupArgs
	// and injectArgs because a common pattern may be to setup the failure mode
	// on all nodes in the cluster, but then randomly inject the failure on a
	// subset of nodes at a time.
	injectArgs FailureArgs
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

func (f *Failer) Setup(ctx context.Context, l *logger.Logger, args FailureArgs) error {
	// We can only set up the failure mode once, as Setup() is not guaranteed to be
	// idempotent.
	if f.state != uninitialized {
		return errors.New("failure mode is already setup")
	}
	f.state = readyForFailure
	f.setupArgs = args
	if err := f.FailureMode.Setup(ctx, l, f.setupArgs); err != nil {
		f.state = invalid
		return err
	}
	return nil
}

func (f *Failer) Inject(ctx context.Context, l *logger.Logger, args FailureArgs) error {
	if err := f.checkValidTransition(readyForFailure); err != nil {
		return err
	}

	f.state = activeFailure
	f.injectArgs = args
	if err := f.FailureMode.Inject(ctx, l, f.injectArgs); err != nil {
		f.state = invalid
		return err
	}
	return nil
}

func (f *Failer) WaitForFailureToPropagate(ctx context.Context, l *logger.Logger) error {
	if err := f.checkValidTransition(activeFailure); err != nil {
		return err
	}

	return f.FailureMode.WaitForFailureToPropagate(ctx, l, f.injectArgs)
}

func (f *Failer) Recover(ctx context.Context, l *logger.Logger) error {
	if err := f.checkValidTransition(activeFailure); err != nil {
		return err
	}
	f.state = readyForFailure

	if err := f.FailureMode.Recover(ctx, l, f.injectArgs); err != nil {
		f.state = invalid
		return err
	}
	return nil
}

func (f *Failer) WaitForFailureToRecover(ctx context.Context, l *logger.Logger) error {
	if err := f.checkValidTransition(readyForFailure); err != nil {
		return err
	}

	return f.FailureMode.WaitForFailureToRecover(ctx, l, f.injectArgs)
}

func (f *Failer) Cleanup(ctx context.Context, l *logger.Logger) error {
	if err := f.checkValidTransition(readyForFailure, activeFailure, invalid); err != nil {
		return err
	}

	if f.state == activeFailure {
		// If there is an active failure injected, best attempt effort to recover it first.
		if err := f.Recover(ctx, l); err != nil {
			l.Printf("failed to recover failure mode: %s\nproceeding with Cleanup()", err)
		}
	}

	f.state = uninitialized
	if err := f.FailureMode.Cleanup(ctx, l, f.setupArgs); err != nil {
		f.state = invalid
		return err
	}
	return nil
}
