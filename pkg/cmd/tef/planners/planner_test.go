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

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestNewBasePlanner(t *testing.T) {
	ctx := context.Background()

	t.Run("valid planner creation", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		registry := setupMockRegistry(ctrl, "test_plan", "Test plan description", 1,
			func(ctx context.Context, p Planner) {
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
			})

		planner, err := NewBasePlanner(ctx, registry)
		require.NoError(t, err)
		require.NotNil(t, planner)
		require.Equal(t, "test_plan", planner.Name)
		require.Equal(t, "Test plan description", planner.Description)
		require.True(t, planner.Registered)
		require.NotNil(t, planner.First)
		require.NotNil(t, planner.Output)
	})
	t.Run("valid planner creation with no output task", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		registry := setupMockRegistry(ctrl, "test_plan", "Test plan description", 1,
			func(ctx context.Context, p Planner) {
				end := p.NewEndTask(ctx, "end")
				exec := p.NewExecutionTask(ctx, "task1")
				exec.Next = end
				exec.ExecutorFn = testExecutor1

				executor := &Executor{
					Name: "testExecutor1",
					Func: testExecutor1,
				}
				p.RegisterExecutor(ctx, executor)
				p.RegisterPlan(ctx, exec, nil)
			})

		planner, err := NewBasePlanner(ctx, registry)
		require.NoError(t, err)
		require.NotNil(t, planner)
		require.Equal(t, "test_plan", planner.Name)
		require.Equal(t, "Test plan description", planner.Description)
		require.True(t, planner.Registered)
		require.NotNil(t, planner.First)
		require.Nil(t, planner.Output)
	})
	t.Run("planner creation failure for unregistered executor", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		registry := setupMockRegistry(ctrl, "test_plan", "Test plan description", 1,
			func(ctx context.Context, p Planner) {
				end := p.NewEndTask(ctx, "end")
				exec := p.NewExecutionTask(ctx, "task1")
				exec.Next = end
				exec.ExecutorFn = testExecutor1
				// Note: NOT registering the executor, which should cause validation to fail
				p.RegisterPlan(ctx, exec, nil)
			})

		_, err := NewBasePlanner(ctx, registry)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not registered in ExecutorRegistry")
	})
	t.Run("plan name with whitespace", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		registry := setupMockRegistry(ctrl, "test plan", "Test plan", 1, func(ctx context.Context, p Planner) {
			end := p.NewEndTask(ctx, "end")
			p.RegisterPlan(ctx, end, end)
		})

		_, err := NewBasePlanner(ctx, registry)
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot contain whitespace")
	})
	t.Run("plan name with .", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		registry := setupMockRegistry(ctrl, "test.plan", "Test plan", 1, func(ctx context.Context, p Planner) {
			end := p.NewEndTask(ctx, "end")
			p.RegisterPlan(ctx, end, end)
		})

		_, err := NewBasePlanner(ctx, registry)
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot contain whitespace, new line, or dots")
	})
	t.Run("unregistered plan", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		registry := setupMockRegistry(ctrl, "test_plan", "Test plan", 1, func(ctx context.Context, p Planner) {})

		_, err := NewBasePlanner(ctx, registry)
		require.Error(t, err)
		require.Contains(t, err.Error(), "is not registered")
	})
	t.Run("plan with no first task", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		registry := setupMockRegistry(ctrl, "test_plan", "Test plan", 1, func(ctx context.Context, p Planner) {
			p.(*BasePlanner).Registered = true
		})

		_, err := NewBasePlanner(ctx, registry)
		require.Error(t, err)
		require.Contains(t, err.Error(), "has no first task")
	})

	t.Run("plan with no output/end task", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		registry := setupMockRegistry(ctrl, "test_plan", "Test plan", 1, func(ctx context.Context, p Planner) {
			exec := p.NewExecutionTask(ctx, "task1")
			exec.ExecutorFn = testExecutor1
			executor := &Executor{
				Name: "testExecutor1",
				Func: testExecutor1,
			}
			p.RegisterExecutor(ctx, executor)
			p.RegisterPlan(ctx, exec, nil)
		})

		_, err := NewBasePlanner(ctx, registry)
		require.Error(t, err)
		require.Contains(t, err.Error(), "has no Next task and doesn't end with a termination point")
	})
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

func TestNewConditionTask(t *testing.T) {
	ctx := context.Background()
	planner := &BasePlanner{
		TasksRegistry:    make(map[string]Task),
		ExecutorRegistry: make(map[string]*Executor),
	}

	task := planner.NewConditionTask(ctx, "if_task")
	require.NotNil(t, task)
	require.Equal(t, "if_task", task.Name())
	require.Equal(t, TaskTypeConditionTask, task.Type())
	require.Contains(t, planner.TasksRegistry, "if_task")
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

func TestRegisterPlan(t *testing.T) {
	ctx := context.Background()

	t.Run("register plan successfully", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		first := planner.NewEndTask(ctx, "first")
		output := planner.NewEndTask(ctx, "output")

		planner.RegisterPlan(ctx, first, output)
		require.True(t, planner.Registered)
		require.Equal(t, first, planner.First)
		require.Equal(t, output, planner.Output)
	})

	t.Run("calling RegisterPlan twice overwrites previous values", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		// Register a plan with initial tasks
		first1 := planner.NewEndTask(ctx, "first1")
		output1 := planner.NewEndTask(ctx, "output1")
		planner.RegisterPlan(ctx, first1, output1)

		// Verify initial registration
		require.True(t, planner.Registered)
		require.Equal(t, first1, planner.First)
		require.Equal(t, output1, planner.Output)

		// Register a plan again with different tasks
		first2 := planner.NewEndTask(ctx, "first2")
		output2 := planner.NewEndTask(ctx, "output2")
		planner.RegisterPlan(ctx, first2, output2)

		// Verify that new values have overwritten the old ones
		require.True(t, planner.Registered)
		require.Equal(t, first2, planner.First)
		require.Equal(t, output2, planner.Output)
		require.NotEqual(t, first1, planner.First)
		require.NotEqual(t, output1, planner.Output)
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

		visited := make(map[string]Task)
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

		visited := make(map[string]Task)
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

		visited := make(map[string]Task)
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

		join := planner.NewForkJoinTask(ctx, "join")

		branch1 := planner.NewExecutionTask(ctx, "branch1")
		branch1.ExecutorFn = testExecutor1
		branch1.Next = end1 // Converges to end1

		branch2 := planner.NewExecutionTask(ctx, "branch2")
		branch2.ExecutorFn = testExecutor1
		branch2.Next = end2 // Converges to end2 (different!)

		fork := planner.NewForkTask(ctx, "fork")
		fork.Tasks = []Task{branch1, branch2}
		fork.Join = join // Both branches should converge to join
		fork.Next = end1

		visited := make(map[string]Task)
		currentPath := make(map[string]bool)

		_, err := planner.validateTaskChain(ctx, fork, visited, currentPath, false)
		require.Error(t, err)
		// Error should be about branches not converging to the Join point
		require.Contains(t, err.Error(), "converges to")
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

		conditionTask := planner.NewConditionTask(ctx, "if")
		conditionTask.ExecutorFn = testBoolExecutor
		conditionTask.Then = end1
		conditionTask.Else = end2

		visited := make(map[string]Task)
		currentPath := make(map[string]bool)

		_, err := planner.validateTaskChain(ctx, conditionTask, visited, currentPath, false)
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
		join := planner.NewForkJoinTask(ctx, "join")
		fork := planner.NewForkTask(ctx, "fork")
		fork.Next = end
		fork.Join = join
		fork.Tasks = []Task{join}

		visited := make(map[string]Task)
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

		task := planner.NewConditionTask(ctx, "if")
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

		task := planner.NewConditionTask(ctx, "if")
		task.ExecutorFn = testBoolExecutor

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

		visited := make(map[string]Task)
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

		visited := make(map[string]Task)
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

		visited := make(map[string]Task)
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

		visited := make(map[string]Task)
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

		visited := make(map[string]Task)
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

		visited := make(map[string]Task)
		currentPath := make(map[string]bool)

		_, err := planner.validateStepTaskPaths(ctx, "step", st, visited, currentPath, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "converge to different termination points")
	})
}

func TestComplexPlanValidation(t *testing.T) {
	ctx := context.Background()

	t.Run("complex valid plan", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		registry := setupMockRegistry(ctrl, "complex_plan", "Complex test plan", 1, func(ctx context.Context, p Planner) {
			executor1 := &Executor{Name: "exec1", Func: testExecutor1}
			executor2 := &Executor{Name: "exec2", Func: testExecutor2}
			executorBool := &Executor{Name: "execBool", Func: testBoolExecutor}
			p.RegisterExecutor(ctx, executor1)
			p.RegisterExecutor(ctx, executor2)
			p.RegisterExecutor(ctx, executorBool)

			end := p.NewEndTask(ctx, "end")

			// Create if task
			conditionTask := p.NewConditionTask(ctx, "check")
			conditionTask.ExecutorFn = testBoolExecutor
			conditionTask.Then = end
			conditionTask.Else = end

			// Create a fork task with a join point
			join := p.NewForkJoinTask(ctx, "join")

			fork1 := p.NewExecutionTask(ctx, "fork1")
			fork1.ExecutorFn = testExecutor1
			fork1.Next = join

			fork2 := p.NewExecutionTask(ctx, "fork2")
			fork2.ExecutorFn = testExecutor2
			fork2.Next = join

			forkTask := p.NewForkTask(ctx, "parallel")
			forkTask.Join = join
			forkTask.Tasks = []Task{fork1, fork2}
			forkTask.Next = conditionTask

			// Create a start task
			start := p.NewExecutionTask(ctx, "start")
			start.ExecutorFn = testExecutor1
			start.Next = forkTask

			p.RegisterPlan(ctx, start, end)
		})

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

		visited := make(map[string]Task)
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

		visited := make(map[string]Task)
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
		join := planner.NewForkJoinTask(ctx, "join")
		fork := planner.NewForkTask(ctx, "fork")
		fork.Next = end
		fork.Join = join
		fork.Tasks = []Task{nil} // nil task in fork branches

		visited := make(map[string]Task)
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
		join := planner.NewForkJoinTask(ctx, "join")

		// Create a branch with an invalid task (missing executor registration)
		invalidBranch := planner.NewExecutionTask(ctx, "invalid")
		invalidBranch.ExecutorFn = testExecutor2 // Not registered
		invalidBranch.Next = join

		fork := planner.NewForkTask(ctx, "fork")
		fork.Join = join
		fork.Next = end
		fork.Tasks = []Task{invalidBranch}

		visited := make(map[string]Task)
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
		join := planner.NewForkJoinTask(ctx, "join")

		fork := planner.NewForkTask(ctx, "fork")
		fork.Tasks = []Task{join}
		fork.Join = join
		fork.Next = end
		fork.Fail = planner.NewEndTask(ctx, "fail_different") // Different end task

		visited := make(map[string]Task)
		currentPath := make(map[string]bool)

		_, err := planner.validateTaskChain(ctx, fork, visited, currentPath, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "converge to different termination points")
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

		conditionTask := &ConditionTask{}
		conditionTask.taskName = "if"
		conditionTask.ExecutorFn = testBoolExecutor
		conditionTask.Else = &EndTask{baseTask: baseTask{taskName: "else_end"}}
		// Then is nil

		visited := make(map[string]Task)
		currentPath := make(map[string]bool)

		_, err := planner.validateTaskChain(ctx, conditionTask, visited, currentPath, false)
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

		conditionTask := &ConditionTask{}
		conditionTask.taskName = "if"
		conditionTask.ExecutorFn = testBoolExecutor
		conditionTask.Then = &EndTask{baseTask: baseTask{taskName: "then_end"}}
		// Else is nil

		visited := make(map[string]Task)
		currentPath := make(map[string]bool)

		_, err := planner.validateTaskChain(ctx, conditionTask, visited, currentPath, false)
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

		conditionTask := planner.NewConditionTask(ctx, "if")
		conditionTask.ExecutorFn = testBoolExecutor
		conditionTask.Then = invalidThen
		conditionTask.Else = end

		visited := make(map[string]Task)
		currentPath := make(map[string]bool)

		_, err := planner.validateTaskChain(ctx, conditionTask, visited, currentPath, false)
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

		conditionTask := planner.NewConditionTask(ctx, "if")
		conditionTask.ExecutorFn = testBoolExecutor
		conditionTask.Then = end
		conditionTask.Else = invalidElse

		visited := make(map[string]Task)
		currentPath := make(map[string]bool)

		_, err := planner.validateTaskChain(ctx, conditionTask, visited, currentPath, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not registered in ExecutorRegistry")
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

		visited := make(map[string]Task)
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
		invalidFail.ExecutorFn = testFailureExecutor // Not registered, but has the correct signature for a failure path
		invalidFail.Next = planner.NewEndTask(ctx, "fail_end")

		task := planner.NewExecutionTask(ctx, "task")
		task.ExecutorFn = testExecutor1
		task.Next = planner.NewEndTask(ctx, "next_end")
		task.Fail = invalidFail

		visited := make(map[string]Task)
		currentPath := make(map[string]bool)

		_, err := planner.validateTaskChain(ctx, task, visited, currentPath, false)
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
		failTask.ExecutorFn = testFailureExecutor // Not registered, but has the correct signature for a failure path
		failTask.Next = planner.NewEndTask(ctx, "fail_end")

		st := &stepTask{
			Fail: failTask,
		}
		st.taskName = "step"

		visited := make(map[string]Task)
		currentPath := make(map[string]bool)

		// This tests lines 302-305: validateTaskChain is called on Fail and returns an error
		_, err := planner.validateStepTaskPaths(ctx, "step", st, visited, currentPath, true)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not registered in ExecutorRegistry")
	})
}

func TestNewCallbackTask(t *testing.T) {
	ctx := context.Background()
	planner := &BasePlanner{
		TasksRegistry:    make(map[string]Task),
		ExecutorRegistry: make(map[string]*Executor),
	}

	task := planner.NewCallbackTask(ctx, "callback_task")
	require.NotNil(t, task)
	require.Equal(t, "callback_task", task.Name())
	require.Equal(t, TaskTypeCallbackTask, task.Type())
	require.Contains(t, planner.TasksRegistry, "callback_task")
}

func TestCallbackTaskValidation(t *testing.T) {
	ctx := context.Background()

	// Test executor functions for callback tasks
	// ExecutionFn must return (string, error)
	callbackExecutor := func(_ context.Context, _ *PlanExecutionInfo, _ string) (string, error) {
		return "step-id", nil
	}

	// ResultProcessorFn must take (context.Context, *PlanExecutionInfo, interface{}, string, string) and return (interface{}, error)
	callbackResultProcessor := func(_ context.Context, _ *PlanExecutionInfo, _ string, _ string, _ string) (interface{}, error) {
		return "processed", nil
	}

	t.Run("callback task with valid executors", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		executor1 := &Executor{
			Name: "callbackExecutor",
			Func: callbackExecutor,
		}
		executor2 := &Executor{
			Name: "callbackResultProcessor",
			Func: callbackResultProcessor,
		}
		planner.RegisterExecutor(ctx, executor1)
		planner.RegisterExecutor(ctx, executor2)

		end := planner.NewEndTask(ctx, "end")
		callbackTask := planner.NewCallbackTask(ctx, "callback")
		callbackTask.ExecutionFn = callbackExecutor
		callbackTask.ResultProcessorFn = callbackResultProcessor
		callbackTask.Next = end

		visited := make(map[string]Task)
		currentPath := make(map[string]bool)

		endTask, err := planner.validateTaskChain(ctx, callbackTask, visited, currentPath, false)
		require.NoError(t, err)
		require.Equal(t, end, endTask)
	})

	t.Run("callback task with unregistered execution function", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		executor2 := &Executor{
			Name: "callbackResultProcessor",
			Func: callbackResultProcessor,
		}
		planner.RegisterExecutor(ctx, executor2)

		callbackTask := &CallbackTask{}
		callbackTask.taskName = "callback"
		callbackTask.ExecutionFn = callbackExecutor // Not registered
		callbackTask.ResultProcessorFn = callbackResultProcessor

		err := planner.validateExecutorRegistration(callbackTask)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not registered in ExecutorRegistry")
	})

	t.Run("callback task with unregistered result processor", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		executor1 := &Executor{
			Name: "callbackExecutor",
			Func: callbackExecutor,
		}
		planner.RegisterExecutor(ctx, executor1)

		callbackTask := &CallbackTask{}
		callbackTask.taskName = "callback"
		callbackTask.ExecutionFn = callbackExecutor
		callbackTask.ResultProcessorFn = callbackResultProcessor // Not registered

		err := planner.validateExecutorRegistration(callbackTask)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not registered in ExecutorRegistry")
	})

	t.Run("callback task with deprecated execution function", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		executor1 := &Executor{
			Name:       "callbackExecutor",
			Func:       callbackExecutor,
			Deprecated: true,
		}
		executor2 := &Executor{
			Name: "callbackResultProcessor",
			Func: callbackResultProcessor,
		}
		planner.RegisterExecutor(ctx, executor1)
		planner.RegisterExecutor(ctx, executor2)

		callbackTask := &CallbackTask{}
		callbackTask.taskName = "callback"
		callbackTask.ExecutionFn = callbackExecutor
		callbackTask.ResultProcessorFn = callbackResultProcessor

		err := planner.validateExecutorRegistration(callbackTask)
		require.Error(t, err)
		require.Contains(t, err.Error(), "uses deprecated executor")
	})

	t.Run("callback task with deprecated result processor", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		executor1 := &Executor{
			Name: "callbackExecutor",
			Func: callbackExecutor,
		}
		executor2 := &Executor{
			Name:       "callbackResultProcessor",
			Func:       callbackResultProcessor,
			Deprecated: true,
		}
		planner.RegisterExecutor(ctx, executor1)
		planner.RegisterExecutor(ctx, executor2)

		callbackTask := &CallbackTask{}
		callbackTask.taskName = "callback"
		callbackTask.ExecutionFn = callbackExecutor
		callbackTask.ResultProcessorFn = callbackResultProcessor

		err := planner.validateExecutorRegistration(callbackTask)
		require.Error(t, err)
		require.Contains(t, err.Error(), "uses deprecated executor")
	})

	t.Run("callback task with different end tasks for next and fail", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		executor1 := &Executor{
			Name: "callbackExecutor",
			Func: callbackExecutor,
		}
		executor2 := &Executor{
			Name: "callbackResultProcessor",
			Func: callbackResultProcessor,
		}
		planner.RegisterExecutor(ctx, executor1)
		planner.RegisterExecutor(ctx, executor2)

		end1 := planner.NewEndTask(ctx, "end1")
		end2 := planner.NewEndTask(ctx, "end2")

		callbackTask := planner.NewCallbackTask(ctx, "callback")
		callbackTask.ExecutionFn = callbackExecutor
		callbackTask.ResultProcessorFn = callbackResultProcessor
		callbackTask.Next = end1
		callbackTask.Fail = end2

		visited := make(map[string]Task)
		currentPath := make(map[string]bool)

		_, err := planner.validateTaskChain(ctx, callbackTask, visited, currentPath, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "converge to different termination points")
	})

	t.Run("callback task in failure path", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		// Failure path executors need an extra error string parameter
		failureCallbackExecutor := func(_ context.Context, _ *PlanExecutionInfo, _ string, _ string) (string, error) {
			return "step-id", nil
		}
		failureCallbackResultProcessor := func(_ context.Context, _ *PlanExecutionInfo, _ string, _ string, _ string, _ string) (interface{}, error) {
			return nil, nil
		}

		executor1 := &Executor{
			Name: "failureCallbackExecutor",
			Func: failureCallbackExecutor,
		}
		executor2 := &Executor{
			Name: "failureCallbackResultProcessor",
			Func: failureCallbackResultProcessor,
		}
		planner.RegisterExecutor(ctx, executor1)
		planner.RegisterExecutor(ctx, executor2)

		end := planner.NewEndTask(ctx, "end")
		callbackTask := planner.NewCallbackTask(ctx, "callback")
		callbackTask.ExecutionFn = failureCallbackExecutor
		callbackTask.ResultProcessorFn = failureCallbackResultProcessor
		callbackTask.Next = end
		callbackTask.Fail = end

		visited := make(map[string]Task)
		currentPath := make(map[string]bool)

		endTask, err := planner.validateTaskChain(ctx, callbackTask, visited, currentPath, true)
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

func (c *customUnknownTask) validate(_ bool) error {
	return nil
}

func TestNewChildPlanTask(t *testing.T) {
	ctx := context.Background()
	planner := &BasePlanner{
		TasksRegistry:    make(map[string]Task),
		ExecutorRegistry: make(map[string]*Executor),
	}

	task := planner.NewChildPlanTask(ctx, "child_task")
	require.NotNil(t, task)
	require.Equal(t, "child_task", task.Name())
	require.Equal(t, TaskTypeChildPlanTask, task.Type())
	require.Contains(t, planner.TasksRegistry, "child_task")
}

func TestNewForkJoinTask(t *testing.T) {
	ctx := context.Background()
	planner := &BasePlanner{
		TasksRegistry:    make(map[string]Task),
		ExecutorRegistry: make(map[string]*Executor),
	}

	task := planner.NewForkJoinTask(ctx, "join_task")
	require.NotNil(t, task)
	require.Equal(t, "join_task", task.Name())
	require.Equal(t, TaskTypeForkJoinTask, task.Type())
	require.Contains(t, planner.TasksRegistry, "join_task")
}

func TestChildPlanTaskValidation(t *testing.T) {
	ctx := context.Background()

	childTaskInfoFn := func(_ context.Context, _ *PlanExecutionInfo, _ string) (ChildTaskInfo, error) {
		return ChildTaskInfo{PlanVariant: "variant1", Input: "input"}, nil
	}

	childTaskInfoFnWithParam := func(_ context.Context, _ *PlanExecutionInfo, _ string, _ string) (ChildTaskInfo, error) {
		return ChildTaskInfo{PlanVariant: "variant1", Input: "input"}, nil
	}

	t.Run("child task with valid executor", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		executor := &Executor{
			Name: "childTaskInfoFn",
			Func: childTaskInfoFn,
		}
		planner.RegisterExecutor(ctx, executor)

		end := planner.NewEndTask(ctx, "end")
		childTask := planner.NewChildPlanTask(ctx, "child")
		childTask.PlanName = "child_plan"
		childTask.ChildTaskInfoFn = childTaskInfoFn
		childTask.Next = end

		visited := make(map[string]Task)
		currentPath := make(map[string]bool)

		endTask, err := planner.validateTaskChain(ctx, childTask, visited, currentPath, false)
		require.NoError(t, err)
		require.Equal(t, end, endTask)
	})

	t.Run("child task with unregistered executor", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		childTask := &ChildPlanTask{}
		childTask.taskName = "child"
		childTask.PlanName = "child_plan"
		childTask.ChildTaskInfoFn = childTaskInfoFn

		err := planner.validateExecutorRegistration(childTask)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not registered in ExecutorRegistry")
	})

	t.Run("child task with deprecated executor", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		executor := &Executor{
			Name:       "childTaskInfoFn",
			Func:       childTaskInfoFn,
			Deprecated: true,
		}
		planner.RegisterExecutor(ctx, executor)

		childTask := &ChildPlanTask{}
		childTask.taskName = "child"
		childTask.PlanName = "child_plan"
		childTask.ChildTaskInfoFn = childTaskInfoFn

		err := planner.validateExecutorRegistration(childTask)
		require.Error(t, err)
		require.Contains(t, err.Error(), "uses deprecated executor")
	})

	t.Run("child task with different end tasks for next and fail", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		executor := &Executor{
			Name: "childTaskInfoFn",
			Func: childTaskInfoFn,
		}
		planner.RegisterExecutor(ctx, executor)

		end1 := planner.NewEndTask(ctx, "end1")
		end2 := planner.NewEndTask(ctx, "end2")

		childTask := planner.NewChildPlanTask(ctx, "child")
		childTask.PlanName = "child_plan"
		childTask.ChildTaskInfoFn = childTaskInfoFn
		childTask.Next = end1
		childTask.Fail = end2

		visited := make(map[string]Task)
		currentPath := make(map[string]bool)

		_, err := planner.validateTaskChain(ctx, childTask, visited, currentPath, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "converge to different termination points")
	})

	t.Run("child task with params", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		executor := &Executor{
			Name: "childTaskInfoFnWithParam",
			Func: childTaskInfoFnWithParam,
		}
		planner.RegisterExecutor(ctx, executor)

		end := planner.NewEndTask(ctx, "end")
		param := planner.NewEndTask(ctx, "param")

		childTask := planner.NewChildPlanTask(ctx, "child")
		childTask.PlanName = "child_plan"
		childTask.ChildTaskInfoFn = childTaskInfoFnWithParam
		childTask.Params = []Task{param}
		childTask.Next = end

		visited := make(map[string]Task)
		currentPath := make(map[string]bool)

		endTask, err := planner.validateTaskChain(ctx, childTask, visited, currentPath, false)
		require.NoError(t, err)
		require.Equal(t, end, endTask)
	})

	t.Run("child task in failure path", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		// In the failure path, the function receives an extra error string parameter
		childTaskInfoFnFailurePath := func(_ context.Context, _ *PlanExecutionInfo, _ string, _ string) (ChildTaskInfo, error) {
			return ChildTaskInfo{PlanVariant: "variant1", Input: "input"}, nil
		}

		executor := &Executor{
			Name: "childTaskInfoFnFailurePath",
			Func: childTaskInfoFnFailurePath,
		}
		planner.RegisterExecutor(ctx, executor)

		end := planner.NewEndTask(ctx, "end")

		childTask := planner.NewChildPlanTask(ctx, "child")
		childTask.PlanName = "child_plan"
		childTask.ChildTaskInfoFn = childTaskInfoFnFailurePath
		childTask.Next = end
		childTask.Fail = end

		visited := make(map[string]Task)
		currentPath := make(map[string]bool)

		endTask, err := planner.validateTaskChain(ctx, childTask, visited, currentPath, true)
		require.NoError(t, err)
		require.Equal(t, end, endTask)
	})
}

func TestParameterValidation(t *testing.T) {
	ctx := context.Background()

	// Executors with different parameter counts
	executorNoParams := func(_ context.Context, _ *PlanExecutionInfo, _ string) (interface{}, error) {
		return nil, nil
	}

	executorOneParam := func(_ context.Context, _ *PlanExecutionInfo, _ string, _ string) (interface{}, error) {
		return nil, nil
	}

	// Executors for a failure path (extra error string parameter)
	executorNoParamsFailure := func(_ context.Context, _ *PlanExecutionInfo, _ string, _ string) (interface{}, error) {
		return nil, nil
	}

	executorOneParamFailure := func(_ context.Context, _ *PlanExecutionInfo, _ string, _ string, _ string) (interface{}, error) {
		return nil, nil
	}

	t.Run("execution task with correct param count", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		executor := &Executor{
			Name: "executorOneParam",
			Func: executorOneParam,
		}
		planner.RegisterExecutor(ctx, executor)

		end := planner.NewEndTask(ctx, "end")
		param := planner.NewEndTask(ctx, "param")

		task := planner.NewExecutionTask(ctx, "task")
		task.ExecutorFn = executorOneParam
		task.Params = []Task{param}
		task.Next = end

		visited := make(map[string]Task)
		currentPath := make(map[string]bool)

		_, err := planner.validateTaskChain(ctx, task, visited, currentPath, false)
		require.NoError(t, err)
	})

	t.Run("execution task with incorrect param count", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		executor := &Executor{
			Name: "executorOneParam",
			Func: executorOneParam,
		}
		planner.RegisterExecutor(ctx, executor)

		end := planner.NewEndTask(ctx, "end")
		param1 := planner.NewEndTask(ctx, "param1")
		param2 := planner.NewEndTask(ctx, "param2")

		task := planner.NewExecutionTask(ctx, "task")
		task.ExecutorFn = executorOneParam
		task.Params = []Task{param1, param2} // Expects 1, got 2
		task.Next = end

		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "expects 1 Param(s), got 2")
	})

	t.Run("execution task with zero params", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		executor := &Executor{
			Name: "executorNoParams",
			Func: executorNoParams,
		}
		planner.RegisterExecutor(ctx, executor)

		end := planner.NewEndTask(ctx, "end")

		task := planner.NewExecutionTask(ctx, "task")
		task.ExecutorFn = executorNoParams
		task.Next = end

		visited := make(map[string]Task)
		currentPath := make(map[string]bool)

		_, err := planner.validateTaskChain(ctx, task, visited, currentPath, false)
		require.NoError(t, err)
	})

	t.Run("execution task in failure path with correct param count", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		executor := &Executor{
			Name: "executorOneParamFailure",
			Func: executorOneParamFailure,
		}
		planner.RegisterExecutor(ctx, executor)

		end := planner.NewEndTask(ctx, "end")
		param := planner.NewEndTask(ctx, "param")

		task := planner.NewExecutionTask(ctx, "task")
		task.ExecutorFn = executorOneParamFailure
		task.Params = []Task{param}
		task.Next = end

		err := task.validate(true) // failure path
		require.NoError(t, err)
	})

	t.Run("execution task in failure path with incorrect param count", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		executor := &Executor{
			Name: "executorNoParamsFailure",
			Func: executorNoParamsFailure,
		}
		planner.RegisterExecutor(ctx, executor)

		end := planner.NewEndTask(ctx, "end")
		param := planner.NewEndTask(ctx, "param")

		task := planner.NewExecutionTask(ctx, "task")
		task.ExecutorFn = executorNoParamsFailure
		task.Params = []Task{param} // Expects 0 params, got 1
		task.Next = end

		err := task.validate(true) // failure path
		require.Error(t, err)
		require.Contains(t, err.Error(), "in failure path expects 0 Param(s), got 1")
	})

	t.Run("condition task with params", func(t *testing.T) {
		condExecutorWithParam := func(_ context.Context, _ *PlanExecutionInfo, _ string, _ int) (bool, error) {
			return true, nil
		}

		planner := &BasePlanner{
			Name:             "test",
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		executor := &Executor{
			Name: "condExecutorWithParam",
			Func: condExecutorWithParam,
		}
		planner.RegisterExecutor(ctx, executor)

		end := planner.NewEndTask(ctx, "end")
		param := planner.NewEndTask(ctx, "param")

		task := planner.NewConditionTask(ctx, "cond")
		task.ExecutorFn = condExecutorWithParam
		task.Params = []Task{param}
		task.Then = end
		task.Else = end

		visited := make(map[string]Task)
		currentPath := make(map[string]bool)

		_, err := planner.validateTaskChain(ctx, task, visited, currentPath, false)
		require.NoError(t, err)
	})

	t.Run("condition task with incorrect param count", func(t *testing.T) {
		condExecutorWithParam := func(_ context.Context, _ *PlanExecutionInfo, _ string, _ int) (bool, error) {
			return true, nil
		}

		task := &ConditionTask{
			ExecutorFn: condExecutorWithParam,
			Then:       &EndTask{baseTask: baseTask{taskName: "then"}},
			Else:       &EndTask{baseTask: baseTask{taskName: "else"}},
			Params:     []Task{}, // Expects 1, got 0
		}
		task.taskName = "cond"

		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "expects 1 additional parameter(s)")
	})
}

func TestWorkflowVersionValidation(t *testing.T) {
	ctx := context.Background()

	t.Run("workflow version less than 1", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		registry := setupMockRegistry(ctrl, "test_plan", "Test plan", 0, func(ctx context.Context, p Planner) {
			end := p.NewEndTask(ctx, "end")
			p.RegisterPlan(ctx, end, end)
		})

		_, err := NewBasePlanner(ctx, registry)
		require.Error(t, err)
		require.Contains(t, err.Error(), "workflow version 0 is less than 1")
	})

	t.Run("workflow version equal to 1", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		registry := setupMockRegistry(ctrl, "test_plan", "Test plan", 1, func(ctx context.Context, p Planner) {
			end := p.NewEndTask(ctx, "end")
			p.RegisterPlan(ctx, end, end)
		})

		planner, err := NewBasePlanner(ctx, registry)
		require.NoError(t, err)
		require.Equal(t, 1, planner.WorkflowVersion)
	})

	t.Run("workflow version greater than 1", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		registry := setupMockRegistry(ctrl, "test_plan", "Test plan", 5, func(ctx context.Context, p Planner) {
			end := p.NewEndTask(ctx, "end")
			p.RegisterPlan(ctx, end, end)
		})

		planner, err := NewBasePlanner(ctx, registry)
		require.NoError(t, err)
		require.Equal(t, 5, planner.WorkflowVersion)
	})
}

func TestPlanNameValidation(t *testing.T) {
	ctx := context.Background()

	t.Run("plan name with dots", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		registry := setupMockRegistry(ctrl, "test.plan", "Test plan", 1, func(ctx context.Context, p Planner) {
			end := p.NewEndTask(ctx, "end")
			p.RegisterPlan(ctx, end, end)
		})

		_, err := NewBasePlanner(ctx, registry)
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot contain whitespace, new line, or dots")
	})

	t.Run("plan name with tab character", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		registry := setupMockRegistry(ctrl, "test\tplan", "Test plan", 1, func(ctx context.Context, p Planner) {
			end := p.NewEndTask(ctx, "end")
			p.RegisterPlan(ctx, end, end)
		})

		_, err := NewBasePlanner(ctx, registry)
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot contain whitespace")
	})

	t.Run("plan name with newline", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		registry := setupMockRegistry(ctrl, "test\nplan", "Test plan", 1, func(ctx context.Context, p Planner) {
			end := p.NewEndTask(ctx, "end")
			p.RegisterPlan(ctx, end, end)
		})

		_, err := NewBasePlanner(ctx, registry)
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot contain whitespace")
	})

	t.Run("plan name with carriage return", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		registry := setupMockRegistry(ctrl, "test\rplan", "Test plan", 1, func(ctx context.Context, p Planner) {
			end := p.NewEndTask(ctx, "end")
			p.RegisterPlan(ctx, end, end)
		})

		_, err := NewBasePlanner(ctx, registry)
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot contain whitespace")
	})
}

func TestMakeValidationKey(t *testing.T) {
	t.Run("normal path", func(t *testing.T) {
		key := makeValidationKey("task1", false)
		require.Equal(t, "task1:normal", key)
	})

	t.Run("failed path", func(t *testing.T) {
		key := makeValidationKey("task1", true)
		require.Equal(t, "task1:failed", key)
	})

	t.Run("different keys for same task in different contexts", func(t *testing.T) {
		normalKey := makeValidationKey("task1", false)
		failedKey := makeValidationKey("task1", true)
		require.NotEqual(t, normalKey, failedKey)
	})
}

/********************* Mocks beyond this point *********************/

// setupMockRegistry creates a mock Registry with common expectations
func setupMockRegistry(
	ctrl *gomock.Controller,
	name, description string,
	version int,
	generateFn func(context.Context, Planner),
) *MockRegistry {
	mockReg := NewMockRegistry(ctrl)
	mockReg.EXPECT().GetPlanName().Return(name).AnyTimes()
	mockReg.EXPECT().GetPlanDescription().Return(description).AnyTimes()
	mockReg.EXPECT().GetPlanVersion().Return(version).AnyTimes()
	mockReg.EXPECT().PrepareExecution(gomock.Any()).Return(nil).AnyTimes()
	mockReg.EXPECT().ParsePlanInput(gomock.Any()).DoAndReturn(func(input string) (interface{}, error) {
		return input, nil
	}).AnyTimes()
	if generateFn != nil {
		mockReg.EXPECT().GeneratePlan(gomock.Any(), gomock.Any()).Do(generateFn).AnyTimes()
	}
	return mockReg
}

// Test executor functions
func testExecutor1(_ context.Context, _ *PlanExecutionInfo, _ string) (interface{}, error) {
	return nil, nil
}

func testExecutor2(_ context.Context, _ *PlanExecutionInfo, _ string) (interface{}, error) {
	return nil, nil
}

func testBoolExecutor(_ context.Context, _ *PlanExecutionInfo, _ string) (bool, error) {
	return true, nil
}

// Test executor for failure path tasks (includes error string parameter)
func testFailureExecutor(
	_ context.Context, _ *PlanExecutionInfo, _ string, _ string,
) (interface{}, error) {
	return nil, nil
}
