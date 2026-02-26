// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// This file provides tests for the plan visualization helper functions.
// Tests verify DOT formatting, color generation, and task graph traversal.
// Note: Tests that require Graphviz (GeneratePlan) are excluded to support CI environments.
package planners

import (
	"context"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

// TestGetTaskShape verifies that each task type maps to the correct DOT shape.
func TestGetTaskShape(t *testing.T) {
	tests := []struct {
		taskType TaskType
		expected string
	}{
		{TaskTypeExecution, "box"},
		{TaskTypeConditionTask, "diamond"},
		{TaskTypeCallbackTask, "octagon"},
		{TaskTypeFork, "parallelogram"},
		{TaskTypeForkJoinTask, "circle"},
		{TaskTypeEndTask, "doublecircle"},
		{TaskType("CustomTask"), "ellipse"}, // default case
	}

	for _, tt := range tests {
		t.Run(string(tt.taskType), func(t *testing.T) {
			shape := getTaskShape(tt.taskType)
			require.Equal(t, tt.expected, shape)
		})
	}
}

// TestGetColor verifies color generation for different nesting levels.
func TestGetColor(t *testing.T) {
	tests := []struct {
		level    int
		expected string
	}{
		{0, "#E0E0E0"},
		{1, "#D0D0D0"},
		{2, "#C0C0C0"},
		{3, "#B0B0B0"},
		{maxNestingLevel, "#000000"},     // At max level.
		{maxNestingLevel + 5, "#000000"}, // Beyond max level (clamped).
	}

	for _, tt := range tests {
		t.Run(string(rune(tt.level)), func(t *testing.T) {
			color := getColor(tt.level)
			require.Equal(t, tt.expected, color)
		})
	}
}

// TestAddIndent verifies DOT graph indentation.
func TestAddIndent(t *testing.T) {
	t.Run("indents simple graph", func(t *testing.T) {
		input := `digraph test {
node1;
node2;
}`
		expected := `digraph test {
  node1;
  node2;
}
`
		result := addIndent(input)
		require.Equal(t, expected, result)
	})

	t.Run("indents nested subgraph", func(t *testing.T) {
		input := `digraph test {
subgraph cluster {
node1;
}
}`
		expected := `digraph test {
  subgraph cluster {
    node1;
  }
}
`
		result := addIndent(input)
		require.Equal(t, expected, result)
	})

	t.Run("handles multiple nesting levels", func(t *testing.T) {
		input := `digraph test {
subgraph cluster1 {
subgraph cluster2 {
node1;
}
}
}`
		expected := `digraph test {
  subgraph cluster1 {
    subgraph cluster2 {
      node1;
    }
  }
}
`
		result := addIndent(input)
		require.Equal(t, expected, result)
	})

	t.Run("handles empty lines", func(t *testing.T) {
		input := `digraph test {

node1;

}`
		expected := `digraph test {

  node1;

}
`
		result := addIndent(input)
		require.Equal(t, expected, result)
	})
}

// TestEscapeDOTLabel verifies that special characters are properly escaped.
func TestEscapeDOTLabel(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "escapes backslashes",
			input:    `path\to\file`,
			expected: `path\\to\\file`,
		},
		{
			name:     "escapes quotes",
			input:    `task"with"quotes`,
			expected: `task\"with\"quotes`,
		},
		{
			name:     "escapes newlines",
			input:    "task\nwith\nnewlines",
			expected: `task\nwith\nnewlines`,
		},
		{
			name:     "escapes tabs",
			input:    "task\twith\ttabs",
			expected: `task\twith\ttabs`,
		},
		{
			name:     "escapes multiple special characters",
			input:    "task\\\"with\nmany\tspecial",
			expected: `task\\\"with\nmany\tspecial`,
		},
		{
			name:     "handles empty string",
			input:    "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := escapeDOTLabel(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}

// TestVisitTask_EdgeDeduplication tests that visitTask doesn't duplicate edges.
func TestVisitTask_EdgeDeduplication(t *testing.T) {
	ctx := ContextWithLogger(context.Background(), NewLogger("info"))

	// Create a plan with a diamond pattern (condition with two branches converging)
	registry := &testRegistry{
		planName: "test-dedup",
		generateFunc: func(ctx context.Context, p Planner) {
			end := p.NewEndTask(ctx, "end")

			task2 := p.NewExecutionTask(ctx, "task2")
			task2.Next = end
			task2.ExecutorFn = testExecutor1

			task1 := p.NewExecutionTask(ctx, "task1")
			task1.Next = end
			task1.ExecutorFn = testExecutor1

			condition := p.NewConditionTask(ctx, "start")
			condition.Then = task1
			condition.Else = task2
			condition.ExecutorFn = testConditionExecutor

			p.RegisterExecutor(ctx, &Executor{Name: "testExecutor1", Func: testExecutor1})
			p.RegisterExecutor(ctx, &Executor{Name: "testConditionExecutor", Func: testConditionExecutor})
			p.RegisterPlan(ctx, condition, nil)
		},
	}

	planner, err := NewBasePlanner(ctx, registry)
	require.NoError(t, err)

	visited := make(map[visitedKey]struct{})
	dot, _ := visitTask(planner.First, visited, 0, false)

	// Verify that intermediate nodes (task1, task2) are only defined once
	// even though the diamond pattern could cause them to be visited multiple times
	task1DefCount := strings.Count(dot, `"task1" [label="task1 [execution task]" shape=box]`)
	task2DefCount := strings.Count(dot, `"task2" [label="task2 [execution task]" shape=box]`)

	require.Equal(t, 1, task1DefCount, "task1 should only be defined once")
	require.Equal(t, 1, task2DefCount, "task2 should only be defined once")

	// Verify edges to end are present
	require.Contains(t, dot, `"task1" -> "end" [label="next"]`)
	require.Contains(t, dot, `"task2" -> "end" [label="next"]`)
}

// testRegistry is a simple implementation of Registry for testing.
type testRegistry struct {
	planName     string
	generateFunc func(context.Context, Planner)
}

func (r *testRegistry) GetPlanName() string        { return r.planName }
func (r *testRegistry) GetPlanDescription() string { return "Test plan" }
func (r *testRegistry) GetPlanVersion() int        { return 1 }
func (r *testRegistry) GeneratePlan(ctx context.Context, planner Planner) {
	r.generateFunc(ctx, planner)
}
func (r *testRegistry) ParsePlanInput(input string) (interface{}, error) { return nil, nil }
func (r *testRegistry) PrepareExecution(ctx context.Context) error       { return nil }

// AddStartWorkerCmdFlags is a no-op for testing.
func (r *testRegistry) AddStartWorkerCmdFlags(_ *cobra.Command) {}

// testConditionExecutor is a test executor for condition tasks that returns a boolean.
func testConditionExecutor(context.Context, *PlanExecutionInfo, any) (bool, error) {
	return true, nil
}
