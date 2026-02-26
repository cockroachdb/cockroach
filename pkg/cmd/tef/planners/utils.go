// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package planners provide utility functions for the Task Execution Framework (TEF).
// This file contains helper functions for plan ID generation, validation, function name
// extraction, and child workflow ID construction. These utilities support plan execution
// and identification across the TEF system.
package planners

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"

	"github.com/cockroachdb/errors"
)

const (
	// PlanIDPrefix is prepended to plan names.
	PlanIDPrefix = "tef."

	// Color generation constants for subgraph visualization.
	maxNestingLevel = 14   // Maximum nesting level before color saturates (0xE0 / 0x10).
	baseGray        = 0xE0 // Base grayscale value for level 0.
	grayStep        = 0x10 // Grayscale decrement per nesting level.

	// outputDirPerms defines the file permissions for created output directories.
	// 0755 = rwxr-xr-x (owner can read/write/execute, group and others can read/execute).
	outputDirPerms = 0755
)

// funcForPC is a variable that holds the function for getting function info from a PC.
// This allows for testing edge cases where runtime.FuncForPC might return nil.
var funcForPC = runtime.FuncForPC

// visitedKey uniquely identifies an edge in the task graph for deduplication.
// It tracks transitions from one task to another with a specific status (next/fail).
// Task names are used instead of Task pointers to ensure stable map keys.
// NOTE: This assumes task names are unique within a plan, which is enforced during
// plan validation.
type visitedKey struct {
	fromName string
	toName   string
	status   string
}

// escapeDOTLabel escapes special characters in task names for safe DOT format output.
// This prevents DOT syntax errors and potential injection issues when task names
// contain quotes, newlines, or other special characters.
func escapeDOTLabel(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	s = strings.ReplaceAll(s, "\n", `\n`)
	s = strings.ReplaceAll(s, "\t", `\t`)
	return s
}

// GeneratePlan generates a visual representation of the plan as a DOT graph and PNG image.
// The DOT file and PNG are created in the specified output directory using the plan name.
// If outputDir is empty, the current working directory is used.
// If withFailurePath is false, failure edges are excluded from the visualization.
// Returns an error if file creation, graph traversal, or PNG generation fails.
func GeneratePlan(
	ctx context.Context, seq *BasePlanner, outputDir string, withFailurePath bool,
) error {
	logger := LoggerFromContext(ctx)
	outputDir = strings.TrimSpace(outputDir)
	if outputDir == "" {
		outputDir = "."
	}
	dotFile := filepath.Join(outputDir, seq.Name+".dot")
	pngFile := filepath.Join(outputDir, seq.Name+".png")

	// Check if the Graphviz 'dot' command is available.
	if _, err := exec.LookPath("dot"); err != nil {
		return errors.Wrap(err, "graphviz 'dot' command not found in PATH; install graphviz to generate visualizations")
	}

	// Create the output directory if it doesn't exist.
	if err := os.MkdirAll(outputDir, outputDirPerms); err != nil {
		return errors.Wrap(err, "create output directory")
	}

	// Create a DOT file to contain the graph definition.
	f, err := os.Create(dotFile)
	if err != nil {
		return errors.Wrap(err, "create dot file")
	}
	defer func() {
		_ = f.Close()
	}()

	// Traverse the task graph to build the DOT representation.
	visited := make(map[visitedKey]struct{})
	body, endStep := visitTask(seq.First, visited, 0, withFailurePath)

	// Verify that an end step was found.
	if endStep == nil {
		return errors.Newf("failed to determine end step for plan %s", seq.Name)
	}

	// Assemble the DOT graph structure with start and end nodes.
	// The first task node is not rendered here as it's rendered during traversal.
	endStepName := escapeDOTLabel(endStep.Name())
	endStepLabel := endStepName + " [" + string(endStep.Type()) + "]"
	firstTaskName := escapeDOTLabel(seq.First.Name())
	body = fmt.Sprintf(
		`digraph sequence {
"Start" [shape=circle style=filled fillcolor=lightgreen];
"%s" [label="%s" shape=%s];
%s
"Start" -> "%s" [label="next"];
}`,
		endStepName,
		endStepLabel,
		getTaskShape(endStep.Type()),
		body,
		firstTaskName,
	)

	if _, err := fmt.Fprintf(f, "%s", addIndent(body)); err != nil {
		return errors.Wrap(err, "write dot file")
	}

	// Generate a PNG image from the DOT file using the Graphviz dot command.
	cmd := exec.Command("dot", "-Tpng", dotFile, "-o", pngFile)
	if err := cmd.Run(); err != nil {
		return errors.Wrap(err, "generate png")
	}

	logger.Info("DOT file written and PNG generated", "dot_file", dotFile, "png_file", pngFile)
	return nil
}

// visitTask recursively traverses the task graph and generates DOT notation.
// It handles special rendering for ForkTasks and tracks visited edges to avoid duplication.
// If withFailurePath is false, failure edges are excluded from the visualization.
func visitTask(
	t Task, visited map[visitedKey]struct{}, level int, withFailurePath bool,
) (string, Task) {
	builder := &strings.Builder{}
	var body string
	var endStep Task
	// EndTasks are terminal nodes and stop the traversal.
	if t.Type() == TaskTypeEndTask {
		return builder.String(), t
	}
	// ForkJoinTasks are synchronization points where parallel branches converge.
	// They stop the traversal of individual branches but don't terminate the workflow.
	if t.Type() == TaskTypeForkJoinTask {
		return builder.String(), t
	}
	// ConditionTasks are rendered with Then and Else branches.
	if t.Type() == TaskTypeConditionTask {
		conditionTask := t.(*ConditionTask)
		// Render the if task node with diamond shape.
		if _, ok := visited[visitedKey{fromName: t.Name()}]; !ok {
			_, _ = fmt.Fprintf(builder, `"%s" [label="%s" shape=%s];`+"\n", escapeDOTLabel(t.Name()), formatTaskLabel(t), getTaskShape(t.Type()))
			visited[visitedKey{fromName: t.Name()}] = struct{}{}
		}

		var thenEndStep, elseEndStep Task

		// Traverse the Then branch
		if conditionTask.Then != nil {
			if _, ok := visited[visitedKey{fromName: t.Name(), toName: conditionTask.Then.Name(), status: "then"}]; !ok {
				_, _ = fmt.Fprintf(builder, `"%s" -> "%s" [label="then" color=green];`+"\n", escapeDOTLabel(t.Name()), escapeDOTLabel(conditionTask.Then.Name()))
				visited[visitedKey{fromName: t.Name(), toName: conditionTask.Then.Name(), status: "then"}] = struct{}{}
			}
			body, thenEndStep = visitTask(conditionTask.Then, visited, level, withFailurePath)
			if body != "" {
				_, _ = fmt.Fprintf(builder, "%s", body)
			}
		}

		// Traverse the Else branch
		if conditionTask.Else != nil {
			if _, ok := visited[visitedKey{fromName: t.Name(), toName: conditionTask.Else.Name(), status: "else"}]; !ok {
				_, _ = fmt.Fprintf(builder, `"%s" -> "%s" [label="else" color=orange];`+"\n", escapeDOTLabel(t.Name()), escapeDOTLabel(conditionTask.Else.Name()))
				visited[visitedKey{fromName: t.Name(), toName: conditionTask.Else.Name(), status: "else"}] = struct{}{}
			}
			body, elseEndStep = visitTask(conditionTask.Else, visited, level, withFailurePath)
			if body != "" {
				_, _ = fmt.Fprintf(builder, "%s", body)
			}
		}

		// Return the end step from one of the branches (prefer Then).
		// May return nil if both branches are nil or have no end step.
		if thenEndStep != nil {
			return builder.String(), thenEndStep
		}
		return builder.String(), elseEndStep
	}
	// ForkTasks are rendered as DOT subgraphs to visualize parallel execution.
	if t.Type() == TaskTypeFork {
		forkTask := t.(*ForkTask)
		// Create a single colored box containing all fork branches.
		_, _ = fmt.Fprintf(builder, "subgraph cluster_fork_%s {\n", strings.ReplaceAll(escapeDOTLabel(t.Name()), " ", "_"))
		_, _ = fmt.Fprintf(builder, `label="%s [%s]";`+"\n", escapeDOTLabel(t.Name()), t.Type())
		_, _ = fmt.Fprintf(builder, "style=filled;\n")
		_, _ = fmt.Fprintf(builder, `color="%s";`+"\n", getColor(level+1))

		// Render the fork task node inside the colored box with parallelogram shape.
		_, _ = fmt.Fprintf(builder, `"%s" [label="%s" shape=%s];`+"\n", escapeDOTLabel(t.Name()), formatTaskLabel(t), getTaskShape(t.Type()))
		visited[visitedKey{fromName: t.Name()}] = struct{}{}

		for i, branchTask := range forkTask.Tasks {
			body, _ = visitTask(branchTask, visited, level+1, withFailurePath)

			// Add the fork branch content inside the colored box
			_, _ = fmt.Fprintf(builder, "%s", body)
			_, _ = fmt.Fprintf(builder, `"%s" -> "%s" [label="fork %d" color=blue];`+"\n", escapeDOTLabel(t.Name()), escapeDOTLabel(branchTask.Name()), i+1)
		}

		// Render the join point if it exists
		if forkTask.Join != nil {
			join := forkTask.Join
			// Render the join node
			if _, ok := visited[visitedKey{fromName: join.Name()}]; !ok {
				_, _ = fmt.Fprintf(builder, `"%s" [label="%s" shape=%s];`+"\n", escapeDOTLabel(join.Name()), formatTaskLabel(join), getTaskShape(join.Type()))
				visited[visitedKey{fromName: join.Name()}] = struct{}{}
			}
			endStep = join
		}

		// Close the colored box after all branches and the join point
		_, _ = fmt.Fprintf(builder, "}\n")

		// After fork branches converge at the join point, connect to Next and Fail tasks
		if t.isStepTask() {
			nextTask := t.getNextTask()
			failTask := t.getFailTask()
			if nextTask != nil && endStep != nil {
				if _, ok := visited[visitedKey{fromName: endStep.Name(), toName: nextTask.Name(), status: "next"}]; !ok {
					_, _ = fmt.Fprintf(builder, `"%s" -> "%s" [label="next"];`+"\n", escapeDOTLabel(endStep.Name()), escapeDOTLabel(nextTask.Name()))
					visited[visitedKey{fromName: endStep.Name(), toName: nextTask.Name(), status: "next"}] = struct{}{}
				}
				body, endStep = visitTask(nextTask, visited, level, withFailurePath)
				if body != "" {
					_, _ = fmt.Fprintf(builder, "%s\n", body)
				}
			}
			// Only add fail edges if withFailurePath is true
			if withFailurePath && failTask != nil && endStep != nil {
				if _, ok := visited[visitedKey{fromName: endStep.Name(), toName: failTask.Name(), status: "fail"}]; !ok {
					_, _ = fmt.Fprintf(builder, `"%s" -> "%s" [label="fail" color=red];`+"\n", escapeDOTLabel(endStep.Name()), escapeDOTLabel(failTask.Name()))
					visited[visitedKey{fromName: endStep.Name(), toName: failTask.Name(), status: "fail"}] = struct{}{}
				}
				body, endStep = visitTask(failTask, visited, level, withFailurePath)
				if body != "" {
					_, _ = fmt.Fprintf(builder, "%s\n", body)
				}
			}
		}
		return builder.String(), endStep
	}
	// Regular tasks (ExecutionTask, CallbackTask, ChildPlanTask) are rendered as
	// simple nodes with edges to Next and Fail tasks.
	if _, ok := visited[visitedKey{fromName: t.Name()}]; !ok {
		_, _ = fmt.Fprintf(builder, `"%s" [label="%s" shape=%s];`+"\n", escapeDOTLabel(t.Name()), formatTaskLabel(t), getTaskShape(t.Type()))
		visited[visitedKey{fromName: t.Name()}] = struct{}{}
	}
	if t.isStepTask() {
		nextTask := t.getNextTask()
		failTask := t.getFailTask()
		// Add the Next edge if it hasn't been visited yet.
		if nextTask != nil {
			if _, ok := visited[visitedKey{fromName: t.Name(), toName: nextTask.Name(), status: "next"}]; !ok {
				_, _ = fmt.Fprintf(builder, `"%s" -> "%s" [label="next"];`+"\n", escapeDOTLabel(t.Name()), escapeDOTLabel(nextTask.Name()))
				visited[visitedKey{fromName: t.Name(), toName: nextTask.Name(), status: "next"}] = struct{}{}
			}
			body, endStep = visitTask(nextTask, visited, level, withFailurePath)
			if body != "" {
				_, _ = fmt.Fprintf(builder, "%s", body)
			}
		}
		// Add the Fail edge if withFailurePath is true and it exists and hasn't been visited yet.
		if withFailurePath && failTask != nil {
			if _, ok := visited[visitedKey{fromName: t.Name(), toName: failTask.Name(), status: "fail"}]; !ok {
				_, _ = fmt.Fprintf(builder, `"%s" -> "%s" [label="fail" color=red];`+"\n", escapeDOTLabel(t.Name()), escapeDOTLabel(failTask.Name()))
				visited[visitedKey{fromName: t.Name(), toName: failTask.Name(), status: "fail"}] = struct{}{}
			}
			body, endStep = visitTask(failTask, visited, level, withFailurePath)
			if body != "" {
				_, _ = fmt.Fprintf(builder, "%s", body)
			}
		}
	}

	return builder.String(), endStep
}

// getColor generates a grayscale color based on nesting level for subgraph visualization.
// Deeper nesting levels produce darker colors for visual distinction.
// Color saturates at maxNestingLevel to avoid negative values.
func getColor(level int) string {
	if level > maxNestingLevel {
		level = maxNestingLevel
	}
	gray := baseGray - level*grayStep
	return strings.ToUpper(fmt.Sprintf("#%02x%02x%02x", gray, gray, gray))
}

// getTaskShape returns the DOT shape for a given task type.
// Different shapes help visually distinguish task types in the graph.
func getTaskShape(taskType TaskType) string {
	switch taskType {
	case TaskTypeExecution:
		return "box"
	case TaskTypeConditionTask:
		return "diamond"
	case TaskTypeCallbackTask:
		return "octagon"
	case TaskTypeFork:
		return "parallelogram"
	case TaskTypeForkJoinTask:
		return "circle"
	case TaskTypeEndTask:
		return "doublecircle"
	default:
		return "ellipse"
	}
}

// formatTaskLabel formats a task label with its type in brackets.
// Task names are escaped for safe DOT format output.
func formatTaskLabel(task Task) string {
	return fmt.Sprintf("%s [%s]", escapeDOTLabel(task.Name()), task.Type())
}

// addIndent formats a DOT graph string with proper indentation based on brace nesting.
// Opening braces increase indentation, and closing braces decrease it.
// This implementation assumes braces only appear as structural elements, not in quoted strings,
// which is guaranteed by our DOT generation (quoted content is properly escaped).
func addIndent(s string) string {
	lines := strings.Split(s, "\n")
	var result strings.Builder
	indent := 0

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		// Decrease indentation before printing lines that end with closing braces.
		if strings.HasSuffix(trimmed, "}") || strings.HasSuffix(trimmed, "};") {
			indent -= 2
			if indent < 0 {
				indent = 0
			}
		}

		// Apply the current indentation to non-empty lines.
		if trimmed != "" {
			result.WriteString(strings.Repeat(" ", indent))
			result.WriteString(trimmed)
		}
		result.WriteString("\n")

		// Increase indentation after printing lines that end with opening braces.
		if strings.HasSuffix(trimmed, "{") {
			indent += 2
		}
	}

	return result.String()
}

// GetPlanID generates a plan ID by combining a prefix, the planner's name, and a given variant for unique identification.
func GetPlanID(planName, planVariant string) string {
	return fmt.Sprintf("%s%s.%s", PlanIDPrefix, planName, planVariant)
}

// IsValidPlanID returns true if the plan ID starts with the expected prefix.
func IsValidPlanID(planID string) bool {
	return strings.HasPrefix(planID, PlanIDPrefix)
}

// ExtractPlanNameAndVariant extracts the plan name and variant from a plan ID.
// Plan IDs are in the format "tef_plan_{planName}.{variant}".
// This function extracts the plan name and variant portions.
func ExtractPlanNameAndVariant(planID string) (string, string, error) {
	if !IsValidPlanID(planID) {
		return "", "", fmt.Errorf("invalid plan ID format: %s", planID)
	}

	// Remove the prefix
	remainder := strings.TrimPrefix(planID, PlanIDPrefix)

	// Find the first "." after the plan name
	// The plan name ends at the first "." (if there's a variant)
	// or at the end of the string (if there's no variant)
	parts := strings.SplitN(remainder, ".", 2)
	if len(parts) <= 1 {
		return "", "", fmt.Errorf("invalid plan ID format: %s", planID)
	}

	return parts[0], parts[1], nil
}

// GetFunctionName returns the actual function name using reflection.
// This is what Temporal uses as the activity name.
func GetFunctionName(fn interface{}) string {
	if fn == nil {
		return ""
	}
	// Validate that fn is a function before calling Pointer().
	fnVal := reflect.ValueOf(fn)
	if fnVal.Kind() != reflect.Func {
		return "<non-function>"
	}
	fnPtr := fnVal.Pointer()
	// funcForPC can return nil for certain edge cases (e.g., invalid PC values).
	runtimeFunc := funcForPC(fnPtr)
	if runtimeFunc == nil {
		return "<unknown-function>"
	}
	fullName := runtimeFunc.Name()
	// Extract just the function name without the package path.
	// e.g., "github.com/cockroachdb/cockroach/pkg/cmd/tef/planners.setupCluster" -> "setupCluster"
	// Note: strings.Split always returns at least one element, so parts[len(parts)-1] is always safe.
	parts := strings.Split(fullName, ".")
	return parts[len(parts)-1]
}

// GetChildWFID constructs a dot-separated string from workflowID, taskName, planeName, and planVariant.
func GetChildWFID(childWorkflowID, taskName, planName, planVariant string) string {
	return strings.Join([]string{childWorkflowID, taskName, planName, planVariant}, ".")
}
