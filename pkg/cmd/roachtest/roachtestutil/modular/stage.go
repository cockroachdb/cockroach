package modular

import (
	"context"
	"fmt"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

type stepFunc func(context.Context, *logger.Logger, *Helper) error
type singleStep struct {
	id                  int
	description         string
	fn                  stepFunc
	background          ShouldStop
	concurrencyDisabled bool
}

func newSingleStep(description string, fn stepFunc, background ShouldStop, concurrencyDisabled bool) *singleStep {
	return &singleStep{
		description:         description,
		fn:                  fn,
		background:          background,
		concurrencyDisabled: concurrencyDisabled,
	}
}

func (s *singleStep) Description() string {
	return s.description
}

func (s *singleStep) Background() ShouldStop {
	return s.background
}

func (s *singleStep) Run(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *Helper) error {
	return s.fn(ctx, l, h)
}

func (s *singleStep) ConcurrencyDisabled() bool {
	return false
}

type Stage struct {
	RNG   *rand.Rand
	Name  string
	Idx   int
	Steps []TestStep
}

func newStage(name string, rng *rand.Rand) *Stage {
	return &Stage{
		RNG:   rng,
		Name:  name,
		Idx:   0,
		Steps: make([]TestStep, 0),
	}
}

func (s *Stage) AsSteps() []TestStep {
	return s.Steps
}

func (s *Stage) Execute(stepName string, fn stepFunc) *Stage {
	ts := newSingleStep(stepName, fn /*background*/, nil /*concurrencyDisabled*/, false)

	sequentialStep := s.newSequentialStep(ts)
	s.Steps = append(s.Steps, sequentialStep)
	return s
}

func (s *Stage) Concurrent(stepName string, fn stepFunc) *Stage {
	ts := newSingleStep(stepName, fn, nil, false)
	concurrentStep := s.newConcurrentStep(ts)
	s.Steps = append(s.Steps, concurrentStep)
	return s
}

// Then adds another step that runs after this one in sequence.
func (s *Stage) Then(stepName string, fn stepFunc) *Stage {
	ts := newSingleStep(stepName, fn /*background*/, nil /*concurrencyDisabled*/, false)

	if len(s.Steps) == 0 {
		s.Steps = append(s.Steps, s.newSequentialStep(ts))
	} else {
		lastIdx := len(s.Steps) - 1
		sequentialStep, ok := s.Steps[lastIdx].(SequentialStep)
		if !ok {
			panic("could not reach here")
		}
		sequentialStep.Steps = append(sequentialStep.Steps, ts)
		s.Steps[lastIdx] = sequentialStep
	}
	return s
}

// And adds another step that runs concurrently with the previous steps in the current group.
func (s *Stage) And(stepName string, fn stepFunc) *Stage {
	ts := newSingleStep(stepName, fn /*background*/, nil /*concurrencyDisabled*/, false)

	if len(s.Steps) == 0 {
		// If no steps exist, create a new concurrent group with this step
		s.Steps = append(s.Steps, s.newConcurrentStep(ts))
	} else {
		lastIdx := len(s.Steps) - 1
		lastStep, ok := s.Steps[lastIdx].(ConcurrentRunStep)
		if !ok {
			panic("could not add a concurrent step")
		}
		lastStep.DelayedSteps = append(lastStep.DelayedSteps, DelayedStep{
			Delay: RandomConcurrencyDelay(lastStep.RNG, true),
			Step:  ts,
		})
		s.Steps[lastIdx] = lastStep
	}
	return s
}

func (s *Stage) newSequentialStep(step *singleStep) SequentialStep {
	s.Idx++
	ss := SequentialStep{
		Label: fmt.Sprintf("Sequential Step: %d", s.Idx),
		Steps: make([]TestStep, 0),
	}
	ss.Steps = append(ss.Steps, step)
	return ss
}

func (s *Stage) newConcurrentStep(step *singleStep) ConcurrentRunStep {
	s.Idx++
	return NewConcurrentRunStep(
		fmt.Sprintf("Concurrent Step: %d", s.Idx),
		[]TestStep{step},
		s.RNG,
		true, // assuming local for now
	)
}
