package modular

import (
	"context"
	"fmt"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

// Test -> mixedversion.TestPlan
type Test struct {
	seed           int64
	rng            *rand.Rand
	maxConcurrency int
	setupStage     *Stage
	bgStage        *Stage
	stages         []*Stage
	afterTestStage *Stage
}

// NewTest creates a new modular test with a random seed.
func NewTest(
	ctx context.Context,
	l *logger.Logger,
	c cluster.Cluster,
	crdbNodes option.NodeListOption,
) *Test {
	prng, seed := randutil.NewLockedPseudoRand()
	return &Test{
		seed:           seed,
		rng:            prng,
		maxConcurrency: 2, // default max concurrency
		setupStage:     nil,
		stages:         make([]*Stage, 0),
		afterTestStage: nil,
	}
}

func (t *Test) Setup(stepName string, fn stepFunc) {
	if t.setupStage == nil {
		t.setupStage = newStage("setup", t.rng)
		t.setupStage.Execute(stepName, fn)
	} else {
		t.setupStage.Then(stepName, fn)
	}
}

func (t *Test) Background(description string, fn stepFunc) {
	if t.bgStage == nil {
		t.bgStage = newStage("background task", t.rng)
		t.bgStage.Execute(description, fn)
	}
}

func (t *Test) AfterTest(stepName string, fn stepFunc) {
	if t.afterTestStage == nil {
		t.afterTestStage = newStage("after-test", t.rng)
		t.afterTestStage.Execute(stepName, fn)
	} else {
		t.afterTestStage.Then(stepName, fn)
	}
}

func (t *Test) NewStage(name string) *Stage {
	stage := &Stage{
		RNG:   t.rng,
		Name:  name,
		Idx:   0,
		Steps: make([]TestStep, 0),
	}
	t.stages = append(t.stages, stage)
	return stage
}

// SetMaxConcurrency sets the maximum number of steps that can run concurrently
// during the randomized mixing of sequences.
func (t *Test) SetMaxConcurrency(maxConcurrency int) {
	if maxConcurrency < 1 {
		maxConcurrency = 1
	}
	t.maxConcurrency = maxConcurrency
}

// Plan creates a randomized test execution plan that follows the new execution model:
// 1. Stages execute sequentially (one after another)
// 2. Within each stage, sequences can run concurrently with each other
// 3. Within each sequence, steps maintain their sequential order
//
// The plan generation works as follows:
// 1. Setup stages are executed first (unchanged)
// 2. Each main stage is processed individually with sequence mixing within that stage
// 3. After-test stages are executed last (unchanged)
//
// Example with TestConcurrentStepsWithAnd test:
//
// Test structure:
//
//	stage1:
//	  seq1: [stage1,seq1,step1, stage1,seq1,2, stage1,seq1,3, stage1,seq1,4]
//	  seq2: [stage1,seq2,step1, stage1,seq2,2]
//	stage2:
//	  seq1: [stage2 step 1, stage2 2]
//
// Execution plan:
//
//	setup: [setup1, setup2] (sequential)
//	stage1: sequences mixed within this stage
//	stage2: sequences mixed within this stage
//	after-test: [afterTest] (sequential)
func (t *Test) Plan() *TestPlan {
	plan := &TestPlan{
		seed: t.seed,
		rng:  t.rng,
	}

	// Add setup stage as-is
	if t.setupStage != nil {
		plan.stages = append(plan.stages, t.setupStage)
	}

	if t.bgStage != nil {
		plan.stages = append(plan.stages, t.bgStage)
	}

	// Process each stage individually with sequence mixing
	for _, stage := range t.stages {
		mixedStage := t.createMixedStageFromSequences(stage)
		plan.stages = append(plan.stages, mixedStage)
	}

	// Add after-test stage as-is
	if t.afterTestStage != nil {
		plan.stages = append(plan.stages, t.afterTestStage)
	}

	plan.assignStepIDs()

	return plan
}

// SequenceTracker tracks the progress of each sequence during mixing.
// Each sequence maintains its own tracker to ensure steps are executed in order.
//
// Example: For sequence [A, B, C], the tracker starts at stepIndex=0 (step A)
// and advances to stepIndex=1 (step B) only after A completes.
type SequenceTracker struct {
	stageIndex int           // Index of the original stage this sequence came from
	stepIndex  int           // Current position in the sequence (0-based)
	steps      []*singleStep // All steps in this sequence
	completed  bool          // Whether all steps in this sequence have been scheduled
}

// createMixedStageFromSequences creates a mixed stage that interleaves steps from different sequences
// within a single stage while maintaining the sequential order within each sequence.
//
// The new execution model:
// 1. Stages execute sequentially (one after another)
// 2. Within each stage, sequences can run concurrently with each other
// 3. Within each sequence, steps maintain their sequential order
//
// The mixing algorithm works as follows:
// 1. Extract all sequences from the given stage
// 2. Create trackers to monitor progress through each sequence
// 3. In each iteration:
//   - Collect the next available step from each non-completed sequence
//   - Randomly decide between sequential (1 step) or concurrent execution
//   - If concurrent, randomly select concurrency level between 2 and maxConcurrency
//   - Randomly select which specific steps to include in this batch
//   - Create either a single step or a concurrent group
//   - Advance the trackers for selected sequences
//
// 4. Repeat until all sequences in the stage are completed
//
// Example execution with stage containing sequences A:[A1,A2], B:[B1,B2,B3], C:[C1] and maxConcurrency=3:
//
// Iteration 1: Available=[A1,B1,C1] → Random choice: Concurrent(2) → Select [A1,B1] → Concurrent(A1,B1)
// Iteration 2: Available=[A2,B2,C1] → Random choice: Sequential(1) → Select [B2] → Single(B2)
// Iteration 3: Available=[A2,B3,C1] → Random choice: Concurrent(3) → Select [A2,B3,C1] → Concurrent(A2,B3,C1)
// Iteration 4: Available=[] → Done
func (t *Test) createMixedStageFromSequences(stage *Stage) *Stage {
	mixedStage := newStage(stage.Name, t.rng)

	// Extract all sequences from the given stage
	sequences := t.extractSequencesFromStage(stage)
	if len(sequences) == 0 {
		return mixedStage
	}

	// Create trackers for each sequence
	trackers := make([]*SequenceTracker, len(sequences))
	for i, seq := range sequences {
		trackers[i] = &SequenceTracker{
			stageIndex: i,
			stepIndex:  0,
			steps:      seq,
			completed:  false,
		}
	}

	// Generate mixed steps
	var result []TestStep
	for {
		// Get available next steps from all sequences
		availableSteps := t.getAvailableSteps(trackers)
		if len(availableSteps) == 0 {
			break
		}

		// Randomly decide execution strategy and batch size
		batchSize := t.determineRandomBatchSize(len(availableSteps))

		// Randomly select which steps to include in this batch
		selectedIndices := t.selectRandomSteps(availableSteps, batchSize)

		if batchSize == 1 {
			// Single step execution
			stepInfo := availableSteps[selectedIndices[0]]
			result = append(result, stepInfo.step)
			trackers[stepInfo.trackerIndex].stepIndex++
		} else {
			// Concurrent execution
			var concurrentSteps []TestStep
			for _, idx := range selectedIndices {
				stepInfo := availableSteps[idx]
				concurrentSteps = append(concurrentSteps, stepInfo.step)
				trackers[stepInfo.trackerIndex].stepIndex++
			}

			concurrentStep := NewConcurrentRunStep(
				fmt.Sprintf("running %d steps concurrently", batchSize),
				concurrentSteps,
				t.rng,
				true, // assuming local for now
			)
			result = append(result, concurrentStep)
		}

		// Update completion status
		for _, tracker := range trackers {
			if tracker.stepIndex >= len(tracker.steps) {
				tracker.completed = true
			}
		}
	}

	mixedStage.Steps = result
	return mixedStage
}

// StepInfo holds information about an available step that can be executed next.
// This links a step to its sequence tracker for coordination during mixing.
type StepInfo struct {
	step         *singleStep // The actual step to execute
	trackerIndex int         // Index of the sequence tracker this step belongs to
}

// extractSequencesFromStage extracts all sequential step groups from a single stage and converts them
// into sequences that can be mixed while preserving their internal order.
//
// Each SequentialStep becomes one sequence, and each individual ConcurrentRunStep
// becomes its own single-step sequence.
//
// Example transformation for a single stage:
//
//	Input stage:
//	  Stage1: SequentialStep[A,B,C], ConcurrentRunStep[D,E]
//
//	Output sequences:
//	  Sequence1: [A,B,C]  (from SequentialStep)
//	  Sequence2: [D]      (from ConcurrentRunStep)
//	  Sequence3: [E]      (from ConcurrentRunStep)
func (t *Test) extractSequencesFromStage(stage *Stage) [][]*singleStep {
	var sequences [][]*singleStep

	for _, step := range stage.AsSteps() {
		switch s := step.(type) {
		case SequentialStep:
			// Extract all single steps from this sequential group
			singleSteps := t.extractSingleStepsFromSequential(s.Steps)
			if len(singleSteps) > 0 {
				sequences = append(sequences, singleSteps)
			}
		case ConcurrentRunStep:
			// For concurrent steps, each one becomes its own sequence
			for _, delayedStep := range s.DelayedSteps {
				if ds, ok := delayedStep.(DelayedStep); ok {
					if ss, ok := ds.Step.(*singleStep); ok {
						sequences = append(sequences, []*singleStep{ss})
					}
				}
			}
		}
	}

	return sequences
}

// extractSingleStepsFromSequential extracts single steps from sequential step groups.
// This recursively unwraps nested step structures to get to the actual executable steps.
func (t *Test) extractSingleStepsFromSequential(steps []TestStep) []*singleStep {
	var result []*singleStep

	for _, step := range steps {
		if ss, ok := step.(*singleStep); ok {
			result = append(result, ss)
		}
	}

	return result
}

// getAvailableSteps returns the next available step from each non-completed sequence.
// This is called in each iteration of the mixing loop to determine which steps
// can be scheduled for execution.
//
// Example: If we have sequences A:[A1,A2], B:[B1,B2] with trackers at stepIndex=0,
// this returns [StepInfo{A1, 0}, StepInfo{B1, 1}].
func (t *Test) getAvailableSteps(trackers []*SequenceTracker) []StepInfo {
	var available []StepInfo

	for i, tracker := range trackers {
		if !tracker.completed && tracker.stepIndex < len(tracker.steps) {
			available = append(available, StepInfo{
				step:         tracker.steps[tracker.stepIndex],
				trackerIndex: i,
			})
		}
	}

	return available
}

// selectRandomSteps randomly selects up to batchSize steps from available steps.
// This introduces randomness into the mixing by choosing which available steps
// to execute together in each iteration.
//
// Example: Given available=[A1,B1,C1] and batchSize=2, this might return [0,2]
// to select A1 and C1 for concurrent execution.
func (t *Test) selectRandomSteps(available []StepInfo, batchSize int) []int {
	if batchSize >= len(available) {
		// Return all indices if we want to select everything
		indices := make([]int, len(available))
		for i := range indices {
			indices[i] = i
		}
		return indices
	}

	// Create a list of all indices and shuffle them
	indices := make([]int, len(available))
	for i := range indices {
		indices[i] = i
	}

	// Shuffle and take first batchSize elements
	t.rng.Shuffle(len(indices), func(i, j int) {
		indices[i], indices[j] = indices[j], indices[i]
	})

	return indices[:batchSize]
}

// determineRandomBatchSize randomly decides the execution strategy and batch size.
// This function introduces randomness in choosing between sequential and concurrent execution:
//
// 1. If only 1 step is available, return 1 (sequential execution)
// 2. If multiple steps are available, randomly choose between:
//   - Sequential execution (batchSize = 1)
//   - Concurrent execution (batchSize randomly selected between 2 and min(availableCount, maxConcurrency))
//
// This creates varied execution patterns where some iterations run single steps
// and others run multiple steps concurrently with random concurrency levels.
//
// Example with availableCount=3, maxConcurrency=3:
//   - 50% chance: return 1 (sequential)
//   - 50% chance: return random(2,3) (concurrent with 2 or 3 steps)
func (t *Test) determineRandomBatchSize(availableCount int) int {
	if availableCount <= 1 {
		return availableCount
	}

	// Randomly decide between sequential (batchSize=1) and concurrent execution
	// 50% chance for sequential, 50% chance for concurrent
	if t.rng.Float64() < 0.5 {
		// Sequential execution: pick only 1 step
		return 1
	}

	// Concurrent execution: randomly select concurrency level between 2 and maxConcurrency
	maxPossible := min(availableCount, t.maxConcurrency)
	if maxPossible <= 1 {
		return 1
	}

	// Random concurrency level between 2 and maxPossible (inclusive)
	if maxPossible == 2 {
		return 2
	}
	return t.rng.Intn(maxPossible-1) + 2 // +2 because we want range [2, maxPossible]
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
