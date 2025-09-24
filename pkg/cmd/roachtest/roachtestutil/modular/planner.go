package modular

import (
	"fmt"
	"math/rand"
	"strings"
)

var stepID = 1

const (
	branchString       = "├──"
	nestedBranchString = "│   "
	lastBranchString   = "└──"
	lastBranchPadding  = "   "
)

type TestPlan struct {
	seed   int64
	rng    *rand.Rand
	stages []*Stage
}

func (plan *TestPlan) prettyPrintStep(
	out *strings.Builder, step TestStep, prefix string,
) {
	writeNested := func(label string, steps []TestStep) {
		out.WriteString(fmt.Sprintf("%s %s\n", prefix, label))
		for i, subStep := range steps {
			nestedPrefix := strings.ReplaceAll(prefix, branchString, nestedBranchString)
			nestedPrefix = strings.ReplaceAll(nestedPrefix, lastBranchString, lastBranchPadding)
			subPrefix := fmt.Sprintf("%s%s", nestedPrefix, treeBranchString(i, len(steps)))
			plan.prettyPrintStep(out, subStep, subPrefix)
		}
	}

	// writeSingle is the function that generates the description for
	// a singleStep. It can include extra information, such as whether
	// there's a delay associated with the step (in the case of
	// concurrent execution), and what database node the step is
	// connecting to.
	writeSingle := func(ss *singleStep, extraContext ...string) {
		var extras string
		if contextStr := strings.Join(extraContext, ", "); contextStr != "" {
			extras = ", " + contextStr
		}
		out.WriteString(fmt.Sprintf("%s %s%s (%d)\n", prefix, ss.Description(), extras, ss.id))
	}

	switch s := step.(type) {
	case SequentialStep:
		writeNested(s.Description(), s.Steps)
	case ConcurrentRunStep:
		writeNested(s.Description(), s.DelayedSteps)
	case DelayedStep:
		delayStr := fmt.Sprintf("after %s delay", s.Delay)
		writeSingle(s.Step.(*singleStep), delayStr)
	default:
		writeSingle(s.(*singleStep))
	}
}

func (plan *TestPlan) prettyPrintStage(
	out *strings.Builder, stage *Stage, prefix string,
) {
	out.WriteString(fmt.Sprintf("%s %s\n", prefix, stage.Name))

	allSteps := stage.AsSteps()
	for i, step := range allSteps {
		nestedPrefix := strings.ReplaceAll(prefix, branchString, nestedBranchString)
		nestedPrefix = strings.ReplaceAll(nestedPrefix, lastBranchString, lastBranchPadding)
		stepPrefix := fmt.Sprintf("%s%s", nestedPrefix, treeBranchString(i, len(allSteps)))
		plan.prettyPrintStep(out, step, stepPrefix)
	}

}

func (plan *TestPlan) String() string {
	var out strings.Builder
	for idx, stage := range plan.stages {
		stagePrefix := treeBranchString(idx, len(plan.stages))
		plan.prettyPrintStage(&out, stage, stagePrefix)
	}

	var lines []string
	addLine := func(title string, val any) {
		titleWithColon := fmt.Sprintf("%s:", title)
		lines = append(lines, fmt.Sprintf("%-20s%v", titleWithColon, val))
	}

	addLine("Seed", plan.seed)

	return fmt.Sprintf(
		"%s\nPlan:\n%s",
		strings.Join(lines, "\n"), out.String(),
	)
}

func treeBranchString(idx, sliceLen int) string {
	if idx == sliceLen-1 {
		return lastBranchString
	}
	return branchString
}

func (plan *TestPlan) assignStepIDs() {
	for _, stage := range plan.stages {
		for _, grpSteps := range stage.AsSteps() {
			assignStepID(grpSteps)
		}
	}
}

func assignStepID(step TestStep) {
	switch s := step.(type) {
	case SequentialStep:
		for _, ss := range s.Steps {
			assignStepID(ss)
		}
	case ConcurrentRunStep:
		for _, ss := range s.DelayedSteps {
			assignStepID(ss)
		}
	case DelayedStep:
		assignStepID(s.Step)
	case *singleStep:
		s.id = stepID
		stepID++
	}
}
