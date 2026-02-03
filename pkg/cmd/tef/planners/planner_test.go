// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package planners provide tests for the BasePlanner implementation.
// These tests verify task registration, validation, cycle detection,
// and convergence checks for the TEF planner.
package planners

import (
	"context"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

// mockRegistry is a test helper for creating registries
type mockRegistry struct {
	name        string
	description string
	generateFn  func(context.Context, Planner)
}

func (m *mockRegistry) PrepareExecution(_ context.Context) error {
	return nil
}

func (m *mockRegistry) AddStartWorkerCmdFlags(_ *cobra.Command) {
}

func (m *mockRegistry) GetPlanName() string {
	return m.name
}

func (m *mockRegistry) GetPlanDescription() string {
	return m.description
}

func (m *mockRegistry) GetWorkflowVersion() int {
	return 1
}

func (m *mockRegistry) GeneratePlan(ctx context.Context, p Planner) {
	if m.generateFn != nil {
		m.generateFn(ctx, p)
	}
}

func (m *mockRegistry) ParsePlanInput(input string) (interface{}, error) {
	return input, nil
}

// Test executor functions
func testExecutor1(_ context.Context, _ *PlanExecutionInfo, _ string) error {
	return nil
}

func testExecutor2(_ context.Context, _ *PlanExecutionInfo, _ string) error {
	return nil
}

func testBoolExecutor(_ context.Context, _ *PlanExecutionInfo, _ string) (bool, error) {
	return true, nil
}

func TestNewBasePlanner(t *testing.T) {
	ctx := context.Background()

	t.Run("valid planner creation", func(t *testing.T) {
		registry := &mockRegistry{
			name:        "test_plan",
			description: "Test plan description",
			generateFn: func(ctx context.Context, p Planner) {
				end := p.NewEndTask(ctx, "end")
				exec := p.NewExecutionTask(ctx, "task1")
				exec.Next = end
				exec.ExecutorFn = testExecutor1

				executor := &Executor{
					Name: "testExecutor1",
					Func: testExecutor1,
				}
				p.RegisterExecutor(ctx, executor)
				p.RegisterPlan(ctx, exec, end)
			},
		}

		planner, err := NewBasePlanner(ctx, registry)
		require.NoError(t, err)
		require.NotNil(t, planner)
		require.Equal(t, "test_plan", planner.Name)
		require.Equal(t, "Test plan description", planner.Description)
		require.True(t, planner.Registered)
		require.NotNil(t, planner.First)
		require.NotNil(t, planner.Output)
	})

	t.Run("plan name with whitespace", func(t *testing.T) {
		registry := &mockRegistry{
			name:        "test plan",
			description: "Test plan",
			generateFn: func(ctx context.Context, p Planner) {
				end := p.NewEndTask(ctx, "end")
				p.RegisterPlan(ctx, end, end)
			},
		}

		_, err := NewBasePlanner(ctx, registry)
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot contain whitespace")
	})

	t.Run("unregistered plan", func(t *testing.T) {
		registry := &mockRegistry{
			name:        "test_plan",
			description: "Test plan",
			generateFn:  func(ctx context.Context, p Planner) {},
		}

		_, err := NewBasePlanner(ctx, registry)
		require.Error(t, err)
		require.Contains(t, err.Error(), "is not registered")
	})

	t.Run("plan with no first task", func(t *testing.T) {
		registry := &mockRegistry{
			name:        "test_plan",
			description: "Test plan",
			generateFn: func(ctx context.Context, p Planner) {
				p.(*BasePlanner).Registered = true
			},
		}

		_, err := NewBasePlanner(ctx, registry)
		require.Error(t, err)
		require.Contains(t, err.Error(), "has no first task")
	})
}

func TestRegisterPlan(t *testing.T) {
	ctx := context.Background()
	planner := &BasePlanner{
		Name:             "test",
		TasksRegistry:    make(map[string]Task),
		ExecutorRegistry: make(map[string]*Executor),
	}

	end := planner.NewEndTask(ctx, "end")
	exec := planner.NewExecutionTask(ctx, "start")

	planner.RegisterPlan(ctx, exec, end)

	require.True(t, planner.Registered)
	require.Equal(t, exec, planner.First)
	require.Equal(t, end, planner.Output)
}

func TestNewEndTask(t *testing.T) {
	ctx := context.Background()
	planner := &BasePlanner{
		TasksRegistry:    make(map[string]Task),
		ExecutorRegistry: make(map[string]*Executor),
	}

	task := planner.NewEndTask(ctx, "end_task")
	require.NotNil(t, task)
	require.Equal(t, "end_task", task.Name())
	require.Equal(t, TaskTypeEndTask, task.Type())
	require.Contains(t, planner.TasksRegistry, "end_task")
}

func TestNewExecutionTask(t *testing.T) {
	ctx := context.Background()
	planner := &BasePlanner{
		TasksRegistry:    make(map[string]Task),
		ExecutorRegistry: make(map[string]*Executor),
	}

	task := planner.NewExecutionTask(ctx, "exec_task")
	require.NotNil(t, task)
	require.Equal(t, "exec_task", task.Name())
	require.Equal(t, TaskTypeExecution, task.Type())
	require.Contains(t, planner.TasksRegistry, "exec_task")
}

func TestNewForkTask(t *testing.T) {
	ctx := context.Background()
	planner := &BasePlanner{
		TasksRegistry:    make(map[string]Task),
		ExecutorRegistry: make(map[string]*Executor),
	}

	task := planner.NewForkTask(ctx, "fork_task")
	require.NotNil(t, task)
	require.Equal(t, "fork_task", task.Name())
	require.Equal(t, TaskTypeFork, task.Type())
	require.Contains(t, planner.TasksRegistry, "fork_task")
}

func TestNewIfTask(t *testing.T) {
	ctx := context.Background()
	planner := &BasePlanner{
		TasksRegistry:    make(map[string]Task),
		ExecutorRegistry: make(map[string]*Executor),
	}

	task := planner.NewIfTask(ctx, "if_task")
	require.NotNil(t, task)
	require.Equal(t, "if_task", task.Name())
	require.Equal(t, TaskTypeIfTask, task.Type())
	require.Contains(t, planner.TasksRegistry, "if_task")
}

func TestNewSleepTask(t *testing.T) {
	ctx := context.Background()
	planner := &BasePlanner{
		TasksRegistry:    make(map[string]Task),
		ExecutorRegistry: make(map[string]*Executor),
	}

	task := planner.NewSleepTask(ctx, "sleep_task")
	require.NotNil(t, task)
	require.Equal(t, "sleep_task", task.Name())
	require.Equal(t, TaskTypeSleepTask, task.Type())
	require.Contains(t, planner.TasksRegistry, "sleep_task")
}

func TestRegisterExecutor(t *testing.T) {
	ctx := context.Background()

	t.Run("register executor successfully", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			ExecutorRegistry: make(map[string]*Executor),
		}

		executor := &Executor{
			Name: "test_executor",
			Func: testExecutor1,
		}

		planner.RegisterExecutor(ctx, executor)
		require.Contains(t, planner.ExecutorRegistry, "test_executor")
		require.Equal(t, executor, planner.ExecutorRegistry["test_executor"])
	})

	t.Run("panic on duplicate executor", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			ExecutorRegistry: make(map[string]*Executor),
		}

		executor := &Executor{
			Name: "test_executor",
			Func: testExecutor1,
		}

		planner.RegisterExecutor(ctx, executor)

		require.Panics(t, func() {
			planner.RegisterExecutor(ctx, executor)
		})
	})
}

func TestRegisterTask(t *testing.T) {
	ctx := context.Background()

	t.Run("register task successfully", func(t *testing.T) {
		planner := &BasePlanner{
			Name:          "test",
			TasksRegistry: make(map[string]Task),
		}

		task := &EndTask{}
		task.taskName = "test_task"

		planner.registerTask(ctx, task)
		require.Contains(t, planner.TasksRegistry, "test_task")
	})

	t.Run("panic on duplicate task", func(t *testing.T) {
		planner := &BasePlanner{
			Name:          "test",
			TasksRegistry: make(map[string]Task),
		}

		task := &EndTask{}
		task.taskName = "test_task"

		planner.registerTask(ctx, task)

		require.Panics(t, func() {
			planner.registerTask(ctx, task)
		})
	})
}

func TestValidateTaskChain(t *testing.T) {
	ctx := context.Background()

	t.Run("nil task", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		visited := make(map[string]*EndTask)
		currentPath := make(map[string]bool)

		_, err := planner.validateTaskChain(ctx, nil, visited, currentPath, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "nil task")
	})

	t.Run("cyclic dependency detection", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		executor := &Executor{
			Name: "testExecutor1",
			Func: testExecutor1,
		}
		planner.RegisterExecutor(ctx, executor)

		// Create a cycle: task1 -> task2 -> task1
		task1 := planner.NewExecutionTask(ctx, "task1")
		task2 := planner.NewExecutionTask(ctx, "task2")
		task1.ExecutorFn = testExecutor1
		task2.ExecutorFn = testExecutor1
		task1.Next = task2
		task2.Next = task1

		visited := make(map[string]*EndTask)
		currentPath := make(map[string]bool)

		_, err := planner.validateTaskChain(ctx, task1, visited, currentPath, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "cyclic dependency")
	})

	t.Run("valid convergence", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		executor := &Executor{
			Name: "testExecutor1",
			Func: testExecutor1,
		}
		planner.RegisterExecutor(ctx, executor)

		// Create convergence: task1 -> task3, task2 -> task3
		end := planner.NewEndTask(ctx, "end")
		task3 := planner.NewExecutionTask(ctx, "task3")
		task3.ExecutorFn = testExecutor1
		task3.Next = end

		task1 := planner.NewExecutionTask(ctx, "task1")
		task1.ExecutorFn = testExecutor1
		task1.Next = task3

		task2 := planner.NewExecutionTask(ctx, "task2")
		task2.ExecutorFn = testExecutor1
		task2.Next = task3

		visited := make(map[string]*EndTask)
		currentPath := make(map[string]bool)

		endTask1, err := planner.validateTaskChain(ctx, task1, visited, currentPath, false)
		require.NoError(t, err)

		endTask2, err := planner.validateTaskChain(ctx, task2, visited, currentPath, false)
		require.NoError(t, err)
		require.Equal(t, endTask1, endTask2)
	})

	t.Run("fork with different end tasks", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		executor := &Executor{
			Name: "testExecutor1",
			Func: testExecutor1,
		}
		planner.RegisterExecutor(ctx, executor)

		end1 := planner.NewEndTask(ctx, "end1")
		end2 := planner.NewEndTask(ctx, "end2")

		branch1 := planner.NewExecutionTask(ctx, "branch1")
		branch1.ExecutorFn = testExecutor1
		branch1.Next = end1

		branch2 := planner.NewExecutionTask(ctx, "branch2")
		branch2.ExecutorFn = testExecutor1
		branch2.Next = end2

		fork := planner.NewForkTask(ctx, "fork")
		fork.Tasks = []Task{branch1, branch2}
		fork.Next = end1

		visited := make(map[string]*EndTask)
		currentPath := make(map[string]bool)

		_, err := planner.validateTaskChain(ctx, fork, visited, currentPath, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "converge to different EndTasks")
	})

	t.Run("if task with different end tasks", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		executor := &Executor{
			Name: "testBoolExecutor",
			Func: testBoolExecutor,
		}
		planner.RegisterExecutor(ctx, executor)

		end1 := planner.NewEndTask(ctx, "end1")
		end2 := planner.NewEndTask(ctx, "end2")

		ifTask := planner.NewIfTask(ctx, "if")
		ifTask.ExecutorFn = testBoolExecutor
		ifTask.Then = end1
		ifTask.Else = end2

		visited := make(map[string]*EndTask)
		currentPath := make(map[string]bool)

		_, err := planner.validateTaskChain(ctx, ifTask, visited, currentPath, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "converge to different EndTasks")
	})

	t.Run("fork in failure path", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		end := planner.NewEndTask(ctx, "end")
		fork := planner.NewForkTask(ctx, "fork")
		fork.Next = end
		fork.Tasks = []Task{end}

		visited := make(map[string]*EndTask)
		currentPath := make(map[string]bool)

		_, err := planner.validateTaskChain(ctx, fork, visited, currentPath, true)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not allowed for the failure flow")
	})
}

func TestValidateExecutorRegistration(t *testing.T) {
	ctx := context.Background()

	t.Run("execution task with registered executor", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		executor := &Executor{
			Name: "testExecutor1",
			Func: testExecutor1,
		}
		planner.RegisterExecutor(ctx, executor)

		task := planner.NewExecutionTask(ctx, "task")
		task.ExecutorFn = testExecutor1

		err := planner.validateExecutorRegistration(task)
		require.NoError(t, err)
	})

	t.Run("execution task with unregistered executor", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		task := planner.NewExecutionTask(ctx, "task")
		task.ExecutorFn = testExecutor1

		err := planner.validateExecutorRegistration(task)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not registered in ExecutorRegistry")
	})

	t.Run("if task with registered executor", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		executor := &Executor{
			Name: "testBoolExecutor",
			Func: testBoolExecutor,
		}
		planner.RegisterExecutor(ctx, executor)

		task := planner.NewIfTask(ctx, "if")
		task.ExecutorFn = testBoolExecutor

		err := planner.validateExecutorRegistration(task)
		require.NoError(t, err)
	})

	t.Run("end task requires no executor", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		task := planner.NewEndTask(ctx, "end")

		err := planner.validateExecutorRegistration(task)
		require.NoError(t, err)
	})

	t.Run("execution task with deprecated executor", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		executor := &Executor{
			Name:       "testExecutor1",
			Func:       testExecutor1,
			Deprecated: true,
		}
		planner.RegisterExecutor(ctx, executor)

		task := planner.NewExecutionTask(ctx, "task")
		task.ExecutorFn = testExecutor1

		err := planner.validateExecutorRegistration(task)
		require.Error(t, err)
		require.Contains(t, err.Error(), "uses deprecated executor")
	})

	t.Run("if task with deprecated executor", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		executor := &Executor{
			Name:       "testBoolExecutor",
			Func:       testBoolExecutor,
			Deprecated: true,
		}
		planner.RegisterExecutor(ctx, executor)

		task := planner.NewIfTask(ctx, "if")
		task.ExecutorFn = testBoolExecutor

		err := planner.validateExecutorRegistration(task)
		require.Error(t, err)
		require.Contains(t, err.Error(), "uses deprecated executor")
	})

	t.Run("sleep task with deprecated executor", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		sleepExecutor := func(_ context.Context, _ *PlanExecutionInfo) (time.Duration, error) {
			return time.Second, nil
		}

		executor := &Executor{
			Name:       "sleepExecutor",
			Func:       sleepExecutor,
			Deprecated: true,
		}
		planner.RegisterExecutor(ctx, executor)

		task := planner.NewSleepTask(ctx, "sleep")
		task.ExecutorFn = sleepExecutor

		err := planner.validateExecutorRegistration(task)
		require.Error(t, err)
		require.Contains(t, err.Error(), "uses deprecated executor")
	})
}

func TestValidateStepTaskPaths(t *testing.T) {
	ctx := context.Background()

	t.Run("normal path with next task", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		end := planner.NewEndTask(ctx, "end")
		st := &stepTask{
			Next: end,
		}
		st.taskName = "step"

		visited := make(map[string]*EndTask)
		currentPath := make(map[string]bool)

		endTask, err := planner.validateStepTaskPaths(ctx, "step", st, visited, currentPath, false)
		require.NoError(t, err)
		require.Equal(t, end, endTask)
	})

	t.Run("normal path without next task", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		st := &stepTask{}
		st.taskName = "step"

		visited := make(map[string]*EndTask)
		currentPath := make(map[string]bool)

		_, err := planner.validateStepTaskPaths(ctx, "step", st, visited, currentPath, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "has no Next task")
	})

	t.Run("failure path with fail task", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		end := planner.NewEndTask(ctx, "end")
		st := &stepTask{
			Fail: end,
		}
		st.taskName = "step"

		visited := make(map[string]*EndTask)
		currentPath := make(map[string]bool)

		endTask, err := planner.validateStepTaskPaths(ctx, "step", st, visited, currentPath, true)
		require.NoError(t, err)
		require.Equal(t, end, endTask)
	})

	t.Run("failure path without fail task", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		st := &stepTask{}
		st.taskName = "step"

		visited := make(map[string]*EndTask)
		currentPath := make(map[string]bool)

		_, err := planner.validateStepTaskPaths(ctx, "step", st, visited, currentPath, true)
		require.Error(t, err)
		require.Contains(t, err.Error(), "has no Fail task")
	})

	t.Run("next and fail paths converge to same end", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		end := planner.NewEndTask(ctx, "end")
		st := &stepTask{
			Next: end,
			Fail: end,
		}
		st.taskName = "step"

		visited := make(map[string]*EndTask)
		currentPath := make(map[string]bool)

		endTask, err := planner.validateStepTaskPaths(ctx, "step", st, visited, currentPath, false)
		require.NoError(t, err)
		require.Equal(t, end, endTask)
	})

	t.Run("next and fail paths converge to different ends", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		end1 := planner.NewEndTask(ctx, "end1")
		end2 := planner.NewEndTask(ctx, "end2")
		st := &stepTask{
			Next: end1,
			Fail: end2,
		}
		st.taskName = "step"

		visited := make(map[string]*EndTask)
		currentPath := make(map[string]bool)

		_, err := planner.validateStepTaskPaths(ctx, "step", st, visited, currentPath, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "converge to different EndTasks")
	})
}

func TestComplexPlanValidation(t *testing.T) {
	ctx := context.Background()

	t.Run("complex valid plan", func(t *testing.T) {
		registry := &mockRegistry{
			name:        "complex_plan",
			description: "Complex test plan",
			generateFn: func(ctx context.Context, p Planner) {
				executor1 := &Executor{Name: "exec1", Func: testExecutor1}
				executor2 := &Executor{Name: "exec2", Func: testExecutor2}
				executorBool := &Executor{Name: "execBool", Func: testBoolExecutor}
				p.RegisterExecutor(ctx, executor1)
				p.RegisterExecutor(ctx, executor2)
				p.RegisterExecutor(ctx, executorBool)

				end := p.NewEndTask(ctx, "end")

				// Create if task
				ifTask := p.NewIfTask(ctx, "check")
				ifTask.ExecutorFn = testBoolExecutor
				ifTask.Then = end
				ifTask.Else = end

				// Create fork task
				fork1 := p.NewExecutionTask(ctx, "fork1")
				fork1.ExecutorFn = testExecutor1
				fork1.Next = end

				fork2 := p.NewExecutionTask(ctx, "fork2")
				fork2.ExecutorFn = testExecutor2
				fork2.Next = end

				forkTask := p.NewForkTask(ctx, "parallel")
				forkTask.Tasks = []Task{fork1, fork2}
				forkTask.Next = ifTask

				// Create a start task
				start := p.NewExecutionTask(ctx, "start")
				start.ExecutorFn = testExecutor1
				start.Next = forkTask

				p.RegisterPlan(ctx, start, end)
			},
		}

		planner, err := NewBasePlanner(ctx, registry)
		require.NoError(t, err)
		require.NotNil(t, planner)
	})
}

func TestAdditionalValidationCoverage(t *testing.T) {
	ctx := context.Background()

	t.Run("task validation error in chain", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		// Create a task without a name to trigger a validation error
		task := &ExecutionTask{}
		task.ExecutorFn = testExecutor1

		visited := make(map[string]*EndTask)
		currentPath := make(map[string]bool)

		_, err := planner.validateTaskChain(ctx, task, visited, currentPath, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "task name is missing")
	})

	t.Run("executor registration validation error", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		end := planner.NewEndTask(ctx, "end")
		task := planner.NewExecutionTask(ctx, "task")
		task.ExecutorFn = testExecutor1 // Not registered
		task.Next = end

		visited := make(map[string]*EndTask)
		currentPath := make(map[string]bool)

		_, err := planner.validateTaskChain(ctx, task, visited, currentPath, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not registered in ExecutorRegistry")
	})

	t.Run("fork task with nil branch", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		end := planner.NewEndTask(ctx, "end")
		fork := planner.NewForkTask(ctx, "fork")
		fork.Next = end
		fork.Tasks = []Task{nil} // nil task in fork branches

		visited := make(map[string]*EndTask)
		currentPath := make(map[string]bool)

		_, err := planner.validateTaskChain(ctx, fork, visited, currentPath, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "has nil task at index")
	})

	t.Run("fork task branch validation error", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		executor := &Executor{
			Name: "testExecutor1",
			Func: testExecutor1,
		}
		planner.RegisterExecutor(ctx, executor)

		end := planner.NewEndTask(ctx, "end")

		// Create a branch with an invalid task (missing executor registration)
		invalidBranch := planner.NewExecutionTask(ctx, "invalid")
		invalidBranch.ExecutorFn = testExecutor2 // Not registered
		invalidBranch.Next = end

		fork := planner.NewForkTask(ctx, "fork")
		fork.Next = end
		fork.Tasks = []Task{invalidBranch}

		visited := make(map[string]*EndTask)
		currentPath := make(map[string]bool)

		_, err := planner.validateTaskChain(ctx, fork, visited, currentPath, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not registered in ExecutorRegistry")
	})

	t.Run("fork stepTask validation error", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		end := planner.NewEndTask(ctx, "end")
		branch := planner.NewEndTask(ctx, "branch")

		fork := planner.NewForkTask(ctx, "fork")
		fork.Tasks = []Task{branch}
		fork.Next = end
		fork.Fail = planner.NewEndTask(ctx, "fail_different") // Different end task

		visited := make(map[string]*EndTask)
		currentPath := make(map[string]bool)

		_, err := planner.validateTaskChain(ctx, fork, visited, currentPath, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "converge to different EndTasks")
	})

	t.Run("if task missing then branch", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		executor := &Executor{
			Name: "testBoolExecutor",
			Func: testBoolExecutor,
		}
		planner.RegisterExecutor(ctx, executor)

		ifTask := &IfTask{}
		ifTask.taskName = "if"
		ifTask.ExecutorFn = testBoolExecutor
		ifTask.Else = &EndTask{baseTask: baseTask{taskName: "else_end"}}
		// Then is nil

		visited := make(map[string]*EndTask)
		currentPath := make(map[string]bool)

		_, err := planner.validateTaskChain(ctx, ifTask, visited, currentPath, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "then is missing")
	})

	t.Run("if task missing else branch", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		executor := &Executor{
			Name: "testBoolExecutor",
			Func: testBoolExecutor,
		}
		planner.RegisterExecutor(ctx, executor)

		ifTask := &IfTask{}
		ifTask.taskName = "if"
		ifTask.ExecutorFn = testBoolExecutor
		ifTask.Then = &EndTask{baseTask: baseTask{taskName: "then_end"}}
		// Else is nil

		visited := make(map[string]*EndTask)
		currentPath := make(map[string]bool)

		_, err := planner.validateTaskChain(ctx, ifTask, visited, currentPath, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "else is missing")
	})

	t.Run("if task then branch validation error", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		executor := &Executor{
			Name: "testBoolExecutor",
			Func: testBoolExecutor,
		}
		planner.RegisterExecutor(ctx, executor)

		end := planner.NewEndTask(ctx, "end")

		// Create a branch with an invalid task then
		invalidThen := planner.NewExecutionTask(ctx, "invalid_then")
		invalidThen.ExecutorFn = testExecutor1 // Not registered
		invalidThen.Next = end

		ifTask := planner.NewIfTask(ctx, "if")
		ifTask.ExecutorFn = testBoolExecutor
		ifTask.Then = invalidThen
		ifTask.Else = end

		visited := make(map[string]*EndTask)
		currentPath := make(map[string]bool)

		_, err := planner.validateTaskChain(ctx, ifTask, visited, currentPath, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not registered in ExecutorRegistry")
	})

	t.Run("if task else branch validation error", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		executor := &Executor{
			Name: "testBoolExecutor",
			Func: testBoolExecutor,
		}
		planner.RegisterExecutor(ctx, executor)

		end := planner.NewEndTask(ctx, "end")

		// Create else branch with an invalid task
		invalidElse := planner.NewExecutionTask(ctx, "invalid_else")
		invalidElse.ExecutorFn = testExecutor1 // Not registered
		invalidElse.Next = end

		ifTask := planner.NewIfTask(ctx, "if")
		ifTask.ExecutorFn = testBoolExecutor
		ifTask.Then = end
		ifTask.Else = invalidElse

		visited := make(map[string]*EndTask)
		currentPath := make(map[string]bool)

		_, err := planner.validateTaskChain(ctx, ifTask, visited, currentPath, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not registered in ExecutorRegistry")
	})

	t.Run("sleep task validation", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		sleepExecutor := func(_ context.Context, _ *PlanExecutionInfo, _ string) (time.Duration, error) {
			return time.Second, nil
		}

		executor := &Executor{
			Name: "sleepExecutor",
			Func: sleepExecutor,
		}
		planner.RegisterExecutor(ctx, executor)

		end := planner.NewEndTask(ctx, "end")
		sleepTask := planner.NewSleepTask(ctx, "sleep")
		sleepTask.ExecutorFn = sleepExecutor
		sleepTask.Next = end

		visited := make(map[string]*EndTask)
		currentPath := make(map[string]bool)

		endTask, err := planner.validateTaskChain(ctx, sleepTask, visited, currentPath, false)
		require.NoError(t, err)
		require.Equal(t, end, endTask)
	})

	t.Run("sleep task with fail path different end", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		sleepExecutor := func(_ context.Context, _ *PlanExecutionInfo, _ string) (time.Duration, error) {
			return time.Second, nil
		}

		executor := &Executor{
			Name: "sleepExecutor",
			Func: sleepExecutor,
		}
		planner.RegisterExecutor(ctx, executor)

		end1 := planner.NewEndTask(ctx, "end1")
		end2 := planner.NewEndTask(ctx, "end2")

		sleepTask := planner.NewSleepTask(ctx, "sleep")
		sleepTask.ExecutorFn = sleepExecutor
		sleepTask.Next = end1
		sleepTask.Fail = end2

		visited := make(map[string]*EndTask)
		currentPath := make(map[string]bool)

		_, err := planner.validateTaskChain(ctx, sleepTask, visited, currentPath, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "converge to different EndTasks")
	})

	t.Run("unknown task type", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		// Create a custom task with an unknown type
		unknownTask := &customUnknownTask{
			baseTask: baseTask{taskName: "unknown"},
		}

		visited := make(map[string]*EndTask)
		currentPath := make(map[string]bool)

		_, err := planner.validateTaskChain(ctx, unknownTask, visited, currentPath, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unknown task type")
	})

	t.Run("failure path validation error in stepTask", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		executor := &Executor{
			Name: "testExecutor1",
			Func: testExecutor1,
		}
		planner.RegisterExecutor(ctx, executor)

		// Create a fail path with an invalid task
		invalidFail := planner.NewExecutionTask(ctx, "invalid_fail")
		invalidFail.ExecutorFn = testExecutor2 // Not registered
		invalidFail.Next = planner.NewEndTask(ctx, "fail_end")

		task := planner.NewExecutionTask(ctx, "task")
		task.ExecutorFn = testExecutor1
		task.Next = planner.NewEndTask(ctx, "next_end")
		task.Fail = invalidFail

		visited := make(map[string]*EndTask)
		currentPath := make(map[string]bool)

		_, err := planner.validateTaskChain(ctx, task, visited, currentPath, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not registered in ExecutorRegistry")
	})

	t.Run("sleep task executor validation", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		sleepExecutor := func(_ context.Context, _ *PlanExecutionInfo) (time.Duration, error) {
			return time.Second, nil
		}

		// Don't register the executor
		sleepTask := &SleepTask{}
		sleepTask.taskName = "sleep"
		sleepTask.ExecutorFn = sleepExecutor
		sleepTask.Next = &EndTask{baseTask: baseTask{taskName: "end"}}

		err := planner.validateExecutorRegistration(sleepTask)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not registered in ExecutorRegistry")
	})

	t.Run("fail path validation error in validateStepTaskPaths", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		// Create a fail task that will cause a validation error (unregistered executor)
		failTask := &ExecutionTask{}
		failTask.taskName = "fail_task"
		failTask.ExecutorFn = testExecutor1 // Not registered

		st := &stepTask{
			Fail: failTask,
		}
		st.taskName = "step"

		visited := make(map[string]*EndTask)
		currentPath := make(map[string]bool)

		// This tests lines 302-305: validateTaskChain is called on Fail and returns an error
		_, err := planner.validateStepTaskPaths(ctx, "step", st, visited, currentPath, true)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not registered in ExecutorRegistry")
	})
}

func TestNewAsyncTask(t *testing.T) {
	ctx := context.Background()
	planner := &BasePlanner{
		TasksRegistry:    make(map[string]Task),
		ExecutorRegistry: make(map[string]*Executor),
	}

	task := planner.NewAsyncTask(ctx, "async_task")
	require.NotNil(t, task)
	require.Equal(t, "async_task", task.Name())
	require.Equal(t, TaskTypeAsyncTask, task.Type())
	require.Contains(t, planner.TasksRegistry, "async_task")
}

func TestAsyncTaskValidation(t *testing.T) {
	ctx := context.Background()

	// Test executor functions for async tasks
	// ExecutionFn must return (string, error)
	asyncExecutor := func(_ context.Context, _ *PlanExecutionInfo, _ string) (string, error) {
		return "step-id", nil
	}

	// ResultProcessorFn must take (context.Context, *PlanExecutionInfo, interface{}, string, string) and return (interface{}, error)
	asyncResultProcessor := func(_ context.Context, _ *PlanExecutionInfo, _ string, _ string, _ string) (interface{}, error) {
		return "processed", nil
	}

	t.Run("async task with valid executors", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		executor1 := &Executor{
			Name: "asyncExecutor",
			Func: asyncExecutor,
		}
		executor2 := &Executor{
			Name: "asyncResultProcessor",
			Func: asyncResultProcessor,
		}
		planner.RegisterExecutor(ctx, executor1)
		planner.RegisterExecutor(ctx, executor2)

		end := planner.NewEndTask(ctx, "end")
		asyncTask := planner.NewAsyncTask(ctx, "async")
		asyncTask.ExecutionFn = asyncExecutor
		asyncTask.ResultProcessorFn = asyncResultProcessor
		asyncTask.Next = end

		visited := make(map[string]*EndTask)
		currentPath := make(map[string]bool)

		endTask, err := planner.validateTaskChain(ctx, asyncTask, visited, currentPath, false)
		require.NoError(t, err)
		require.Equal(t, end, endTask)
	})

	t.Run("async task with unregistered execution function", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		executor2 := &Executor{
			Name: "asyncResultProcessor",
			Func: asyncResultProcessor,
		}
		planner.RegisterExecutor(ctx, executor2)

		asyncTask := &AsyncTask{}
		asyncTask.taskName = "async"
		asyncTask.ExecutionFn = asyncExecutor // Not registered
		asyncTask.ResultProcessorFn = asyncResultProcessor

		err := planner.validateExecutorRegistration(asyncTask)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not registered in ExecutorRegistry")
	})

	t.Run("async task with unregistered result processor", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		executor1 := &Executor{
			Name: "asyncExecutor",
			Func: asyncExecutor,
		}
		planner.RegisterExecutor(ctx, executor1)

		asyncTask := &AsyncTask{}
		asyncTask.taskName = "async"
		asyncTask.ExecutionFn = asyncExecutor
		asyncTask.ResultProcessorFn = asyncResultProcessor // Not registered

		err := planner.validateExecutorRegistration(asyncTask)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not registered in ExecutorRegistry")
	})

	t.Run("async task with deprecated execution function", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		executor1 := &Executor{
			Name:       "asyncExecutor",
			Func:       asyncExecutor,
			Deprecated: true,
		}
		executor2 := &Executor{
			Name: "asyncResultProcessor",
			Func: asyncResultProcessor,
		}
		planner.RegisterExecutor(ctx, executor1)
		planner.RegisterExecutor(ctx, executor2)

		asyncTask := &AsyncTask{}
		asyncTask.taskName = "async"
		asyncTask.ExecutionFn = asyncExecutor
		asyncTask.ResultProcessorFn = asyncResultProcessor

		err := planner.validateExecutorRegistration(asyncTask)
		require.Error(t, err)
		require.Contains(t, err.Error(), "uses deprecated executor")
	})

	t.Run("async task with deprecated result processor", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		executor1 := &Executor{
			Name: "asyncExecutor",
			Func: asyncExecutor,
		}
		executor2 := &Executor{
			Name:       "asyncResultProcessor",
			Func:       asyncResultProcessor,
			Deprecated: true,
		}
		planner.RegisterExecutor(ctx, executor1)
		planner.RegisterExecutor(ctx, executor2)

		asyncTask := &AsyncTask{}
		asyncTask.taskName = "async"
		asyncTask.ExecutionFn = asyncExecutor
		asyncTask.ResultProcessorFn = asyncResultProcessor

		err := planner.validateExecutorRegistration(asyncTask)
		require.Error(t, err)
		require.Contains(t, err.Error(), "uses deprecated executor")
	})

	t.Run("async task with different end tasks for next and fail", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		executor1 := &Executor{
			Name: "asyncExecutor",
			Func: asyncExecutor,
		}
		executor2 := &Executor{
			Name: "asyncResultProcessor",
			Func: asyncResultProcessor,
		}
		planner.RegisterExecutor(ctx, executor1)
		planner.RegisterExecutor(ctx, executor2)

		end1 := planner.NewEndTask(ctx, "end1")
		end2 := planner.NewEndTask(ctx, "end2")

		asyncTask := planner.NewAsyncTask(ctx, "async")
		asyncTask.ExecutionFn = asyncExecutor
		asyncTask.ResultProcessorFn = asyncResultProcessor
		asyncTask.Next = end1
		asyncTask.Fail = end2

		visited := make(map[string]*EndTask)
		currentPath := make(map[string]bool)

		_, err := planner.validateTaskChain(ctx, asyncTask, visited, currentPath, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "converge to different EndTasks")
	})

	t.Run("async task in failure path", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		executor1 := &Executor{
			Name: "asyncExecutor",
			Func: asyncExecutor,
		}
		executor2 := &Executor{
			Name: "asyncResultProcessor",
			Func: asyncResultProcessor,
		}
		planner.RegisterExecutor(ctx, executor1)
		planner.RegisterExecutor(ctx, executor2)

		end := planner.NewEndTask(ctx, "end")
		asyncTask := planner.NewAsyncTask(ctx, "async")
		asyncTask.ExecutionFn = asyncExecutor
		asyncTask.ResultProcessorFn = asyncResultProcessor
		asyncTask.Next = end
		asyncTask.Fail = end

		visited := make(map[string]*EndTask)
		currentPath := make(map[string]bool)

		endTask, err := planner.validateTaskChain(ctx, asyncTask, visited, currentPath, true)
		require.NoError(t, err)
		require.Equal(t, end, endTask)
	})
}

// customUnknownTask is a helper for testing unknown task types
type customUnknownTask struct {
	baseTask
}

func (c *customUnknownTask) Type() TaskType {
	return "unknown_type"
}

func (c *customUnknownTask) validate() error {
	return nil
}
