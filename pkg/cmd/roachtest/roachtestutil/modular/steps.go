// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package modular

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

var (
	// possibleDelays lists the possible delays to be added to
	// concurrent steps.
	possibleDelays = []time.Duration{
		0,
		100 * time.Millisecond,
		500 * time.Millisecond,
		5 * time.Second,
		30 * time.Second,
		3 * time.Minute,
	}
)

// StepContext defines the interface that any context implementation must satisfy.
// This allows different packages to provide their own context implementations
// while remaining compatible with the modular step framework.
type StepContext interface {
	// Clone creates a deep copy of the context to ensure step isolation
	Clone() StepContext
}

// StepHelper defines the interface that any helper implementation must satisfy.
// This allows different packages to provide their own helper implementations
// with package-specific functionality while working with the modular step framework.
type StepHelper interface {
	// Add methods that are common across different helper implementations
	// This can be extended as needed by different packages
}

type (
	ShouldStop chan struct{}

	// TestStep is an opaque reference to one step of a mixed-version
	// test. It can be a singleStep (see below), or a "meta-step",
	// meaning that it combines other steps in some way (for instance, a
	// series of steps to be run sequentially or concurrently).
	TestStep interface{}

	// SingleStepProtocol is the set of functions that single step
	// implementations need to provide. It uses generics to allow
	// different packages to provide their own Context and Helper implementations.
	SingleStepProtocol[C StepContext, H StepHelper] interface {
		// Description is a string representation of the step, intended
		// for human-consumption. Displayed when pretty-printing the test
		// plan.
		Description() string
		// Background returns a channel that controls the execution of a
		// background step: when that channel is closed, the context
		// associated with the step will be canceled. Returning `nil`
		// indicates that the step should not be run in the background.
		// When a step is *not* run in the background, the test will wait
		// for it to finish before moving on. When a background step
		// fails, the entire test fails.
		Background() ShouldStop
		// Run implements the actual functionality of the step.
		// Uses generic types C and H for Context and Helper respectively.
		Run(context.Context, *logger.Logger, *rand.Rand, H) error
		// ConcurrencyDisabled returns true if the step should not be run
		// concurrently with other steps. This is the case for any steps
		// that involve restarting a node, as they may attempt to connect
		// to an unavailable node.
		ConcurrencyDisabled() bool
	}

	// SingleStep represents steps that implement the pieces on top of
	// which a mixed-version test is built. In other words, they are not
	// composed by other steps and hence can be directly executed.
	// Uses generics to allow different Context and Helper implementations.
	SingleStep[C StepContext, H StepHelper] struct {
		Context C                        // the context the step runs in
		RNG     *rand.Rand               // the RNG to be used when running this step
		ID      int                      // unique ID associated with the step
		Impl    SingleStepProtocol[C, H] // the concrete implementation of the step
	}
)

// NewSingleStep creates a `singleStep` struct for the implementation
// passed, making sure to copy the context so that any modifications
// made to it do not affect this step's view of the context.
// Uses generics to support different Context and Helper implementations.
func NewSingleStep[C StepContext, H StepHelper](
	context C, impl SingleStepProtocol[C, H], rng *rand.Rand,
) *SingleStep[C, H] {
	return &SingleStep[C, H]{Context: context.Clone().(C), Impl: impl, RNG: rng}
}

// SequentialStep represents a sequence of steps that should be executed one after another.
type SequentialStep struct {
	Label string
	Steps []TestStep
}

func (s SequentialStep) Description() string {
	return s.Label
}

// DelayedStep represents a step that should be executed after a certain delay.
type DelayedStep struct {
	Step  TestStep
	Delay time.Duration
}

// ConcurrentRunStep represents a step where multiple operations should run concurrently.
type ConcurrentRunStep struct {
	Label        string
	RNG          *rand.Rand
	DelayedSteps []TestStep
}

func (s ConcurrentRunStep) Description() string {
	return fmt.Sprintf("%s concurrently", s.Label)
}

func NewConcurrentRunStep(
	label string, steps []TestStep, rng *rand.Rand, isLocal bool,
) ConcurrentRunStep {
	delayedSteps := make([]TestStep, 0, len(steps))
	for _, step := range steps {
		delayedSteps = append(delayedSteps, DelayedStep{
			Delay: RandomConcurrencyDelay(rng, isLocal), Step: step,
		})
	}

	return ConcurrentRunStep{Label: label, DelayedSteps: delayedSteps, RNG: rng}
}

func RandomConcurrencyDelay(rng *rand.Rand, isLocal bool) time.Duration {
	return PickRandomDelay(rng, isLocal, possibleDelays)
}

// PickRandomDelay chooses a random duration from the list passed,
// reducing it in local runs, as some tests run as part of CI and we
// don't want to spend too much time waiting in that context.
func PickRandomDelay(rng *rand.Rand, isLocal bool, durations []time.Duration) time.Duration {
	dur := durations[rng.Intn(len(durations))]
	if isLocal {
		dur = dur / 10
	}

	return dur
}

type Context struct{}

func (c Context) Clone() StepContext {
	return Context{}
}

type Helper struct{}
