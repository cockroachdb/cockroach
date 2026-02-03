// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package planners provides tests for task type definitions and validation.
// These tests verify that all task types (ExecutionTask, ForkTask, IfTask,
// SleepTask, AsyncTask, ChildWorkflowTask) validate correctly.
package planners

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Test executor functions for tasks_test.go
func validExecutor(_ context.Context, _ *PlanExecutionInfo, _ string) error {
	return nil
}

func validBoolExecutor(_ context.Context, _ *PlanExecutionInfo, _ string) (bool, error) {
	return true, nil
}

func validDurationExecutor(
	_ context.Context, _ *PlanExecutionInfo, _ string,
) (time.Duration, error) {
	return time.Second, nil
}

func validAsyncExecutor(_ context.Context, _ *PlanExecutionInfo, _ string) (string, error) {
	return "step-id", nil
}

func validAsyncResultProcessor(
	_ context.Context, _ *PlanExecutionInfo, _ interface{}, _ string, _ string,
) (interface{}, error) {
	return "processed", nil
}

func invalidIfExecutor(_ context.Context, _ *PlanExecutionInfo, _ string) (string, error) {
	return "not bool", nil
}

func invalidSleepExecutor(_ context.Context, _ *PlanExecutionInfo, _ string) (string, error) {
	return "not duration", nil
}

func invalidAsyncExecutor(_ context.Context, _ *PlanExecutionInfo, _ string) error {
	return nil
}

func invalidAsyncResultProcessor(_ context.Context) error {
	return nil
}

func TestBaseTask(t *testing.T) {
	t.Run("Name returns task name", func(t *testing.T) {
		task := &baseTask{taskName: "test_task"}
		require.Equal(t, "test_task", task.Name())
	})

	t.Run("isStepTask returns false", func(t *testing.T) {
		task := &baseTask{}
		require.False(t, task.isStepTask())
	})

	t.Run("getNextTask returns nil", func(t *testing.T) {
		task := &baseTask{}
		require.Nil(t, task.getNextTask())
	})

	t.Run("getFailTask returns nil", func(t *testing.T) {
		task := &baseTask{}
		require.Nil(t, task.getFailTask())
	})
}

func TestStepTask(t *testing.T) {
	end := &EndTask{}
	end.taskName = "end"

	fail := &EndTask{}
	fail.taskName = "fail"

	t.Run("Type returns empty string", func(t *testing.T) {
		task := &stepTask{}
		require.Equal(t, TaskType(""), task.Type())
	})

	t.Run("validate requires task name", func(t *testing.T) {
		task := &stepTask{}
		err := task.validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "task name is missing")
	})

	t.Run("validate requires next task", func(t *testing.T) {
		task := &stepTask{}
		task.taskName = "test"
		err := task.validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "next task is missing")
	})

	t.Run("validate succeeds with name and next", func(t *testing.T) {
		task := &stepTask{
			Next: end,
		}
		task.taskName = "test"
		err := task.validate()
		require.NoError(t, err)
	})

	t.Run("isStepTask returns true", func(t *testing.T) {
		task := &stepTask{}
		require.True(t, task.isStepTask())
	})

	t.Run("getNextTask returns Next", func(t *testing.T) {
		task := &stepTask{Next: end}
		require.Equal(t, end, task.getNextTask())
	})

	t.Run("getFailTask returns Fail", func(t *testing.T) {
		task := &stepTask{Fail: fail}
		require.Equal(t, fail, task.getFailTask())
	})

	t.Run("NextTask returns Next", func(t *testing.T) {
		task := &stepTask{Next: end}
		require.Equal(t, end, task.NextTask())
	})

	t.Run("FailTask returns Fail", func(t *testing.T) {
		task := &stepTask{Fail: fail}
		require.Equal(t, fail, task.FailTask())
	})
}

func TestExecutionTask(t *testing.T) {
	end := &EndTask{}
	end.taskName = "end"

	t.Run("Type returns TaskTypeExecution", func(t *testing.T) {
		task := &ExecutionTask{}
		require.Equal(t, TaskTypeExecution, task.Type())
	})

	t.Run("validate requires task name", func(t *testing.T) {
		task := &ExecutionTask{}
		err := task.validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "task name is missing")
	})

	t.Run("validate requires executor", func(t *testing.T) {
		task := &ExecutionTask{}
		task.taskName = "test"
		err := task.validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "executor is missing")
	})

	t.Run("validate requires executor to be a function", func(t *testing.T) {
		task := &ExecutionTask{
			ExecutorFn: "not a function",
		}
		task.taskName = "test"
		err := task.validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "must be a function")
	})

	t.Run("validate requires executor to have at least 3 parameters", func(t *testing.T) {
		invalidFunc := func(ctx context.Context) error {
			return nil
		}
		task := &ExecutionTask{
			ExecutorFn: invalidFunc,
		}
		task.taskName = "test"
		err := task.validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "must take at least 3 parameters")
	})

	t.Run("validate requires first parameter to be context.Context", func(t *testing.T) {
		invalidFunc := func(notCtx string, info *PlanExecutionInfo, input string) error {
			return nil
		}
		task := &ExecutionTask{
			ExecutorFn: invalidFunc,
		}
		task.taskName = "test"
		err := task.validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "must have context.Context as first parameter")
	})

	t.Run("validate requires second parameter to be *PlanExecutionInfo", func(t *testing.T) {
		invalidFunc := func(ctx context.Context, notInfo string, input string) error {
			return nil
		}
		task := &ExecutionTask{
			ExecutorFn: invalidFunc,
		}
		task.taskName = "test"
		err := task.validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "must have *PlanExecutionInfo as second parameter")
	})

	t.Run("validate succeeds with name and executor", func(t *testing.T) {
		task := &ExecutionTask{
			ExecutorFn: validExecutor,
		}
		task.taskName = "test"
		err := task.validate()
		require.NoError(t, err)
	})

	t.Run("GetExecutorName returns executor name", func(t *testing.T) {
		registry := map[string]*Executor{
			"validExecutor": {
				Name: "validExecutor",
				Func: validExecutor,
			},
		}
		task := &ExecutionTask{
			ExecutorFn: validExecutor,
		}
		name := task.GetExecutorName(registry)
		require.Equal(t, "validExecutor", name)
	})

	t.Run("GetExecutorName returns unknown for unregistered executor", func(t *testing.T) {
		registry := map[string]*Executor{}
		task := &ExecutionTask{
			ExecutorFn: validExecutor,
		}
		name := task.GetExecutorName(registry)
		require.Equal(t, "unknown", name)
	})
}

func TestForkTask(t *testing.T) {
	end := &EndTask{}
	end.taskName = "end"

	branch1 := &EndTask{}
	branch1.taskName = "branch1"

	branch2 := &EndTask{}
	branch2.taskName = "branch2"

	t.Run("Type returns TaskTypeFork", func(t *testing.T) {
		task := &ForkTask{}
		require.Equal(t, TaskTypeFork, task.Type())
	})

	t.Run("validate requires task name", func(t *testing.T) {
		task := &ForkTask{}
		err := task.validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "task name is missing")
	})

	t.Run("validate requires next task", func(t *testing.T) {
		task := &ForkTask{}
		task.taskName = "test"
		err := task.validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "next task is missing")
	})

	t.Run("validate requires tasks", func(t *testing.T) {
		task := &ForkTask{
			stepTask: stepTask{
				Next: end,
			},
		}
		task.taskName = "test"
		err := task.validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "tasks is missing")
	})

	t.Run("validate succeeds with all required fields", func(t *testing.T) {
		task := &ForkTask{
			stepTask: stepTask{
				Next: end,
			},
			Tasks: []Task{branch1, branch2},
		}
		task.taskName = "test"
		err := task.validate()
		require.NoError(t, err)
	})
}

func TestEndTask(t *testing.T) {
	t.Run("Type returns TaskTypeEndTask", func(t *testing.T) {
		task := &EndTask{}
		require.Equal(t, TaskTypeEndTask, task.Type())
	})

	t.Run("validate always succeeds", func(t *testing.T) {
		task := &EndTask{}
		err := task.validate()
		require.NoError(t, err)
	})
}

func TestIfTask(t *testing.T) {
	thenTask := &EndTask{}
	thenTask.taskName = "then"

	elseTask := &EndTask{}
	elseTask.taskName = "else"

	t.Run("Type returns TaskTypeIfTask", func(t *testing.T) {
		task := &IfTask{}
		require.Equal(t, TaskTypeIfTask, task.Type())
	})

	t.Run("validate requires executor", func(t *testing.T) {
		task := &IfTask{}
		task.taskName = "test"
		err := task.validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "executor is missing")
	})

	t.Run("validate requires executor to be a function", func(t *testing.T) {
		task := &IfTask{
			ExecutorFn: "not a function",
		}
		task.taskName = "test"
		err := task.validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "must be a function")
	})

	t.Run("validate requires executor to have at least 3 parameters", func(t *testing.T) {
		invalidFunc := func(ctx context.Context) (bool, error) {
			return true, nil
		}
		task := &IfTask{
			ExecutorFn: invalidFunc,
		}
		task.taskName = "test"
		err := task.validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "must take at least 3 parameters")
	})

	t.Run("validate requires first parameter to be context.Context", func(t *testing.T) {
		invalidFunc := func(notCtx string, info *PlanExecutionInfo, input string) (bool, error) {
			return true, nil
		}
		task := &IfTask{
			ExecutorFn: invalidFunc,
		}
		task.taskName = "test"
		err := task.validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "must have context.Context as first parameter")
	})

	t.Run("validate requires second parameter to be *PlanExecutionInfo", func(t *testing.T) {
		invalidFunc := func(ctx context.Context, notInfo string, input string) (bool, error) {
			return true, nil
		}
		task := &IfTask{
			ExecutorFn: invalidFunc,
		}
		task.taskName = "test"
		err := task.validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "must have *PlanExecutionInfo as second parameter")
	})

	t.Run("validate requires executor to return 2 values", func(t *testing.T) {
		invalidFunc := func(ctx context.Context, info *PlanExecutionInfo, input string) bool {
			return true
		}
		task := &IfTask{
			ExecutorFn: invalidFunc,
		}
		task.taskName = "test"
		err := task.validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "must return exactly 2 values")
	})

	t.Run("validate requires first return value to be bool", func(t *testing.T) {
		task := &IfTask{
			ExecutorFn: invalidIfExecutor,
		}
		task.taskName = "test"
		err := task.validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "must return bool as first return value")
	})

	t.Run("validate requires second return value to be error", func(t *testing.T) {
		invalidFunc := func(ctx context.Context, info *PlanExecutionInfo, input string) (bool, string) {
			return true, "not error"
		}
		task := &IfTask{
			ExecutorFn: invalidFunc,
		}
		task.taskName = "test"
		err := task.validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "must return error as second return value")
	})

	t.Run("validate requires then task", func(t *testing.T) {
		task := &IfTask{
			ExecutorFn: validBoolExecutor,
		}
		task.taskName = "test"
		err := task.validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "then is missing")
	})

	t.Run("validate requires else task", func(t *testing.T) {
		task := &IfTask{
			ExecutorFn: validBoolExecutor,
			Then:       thenTask,
		}
		task.taskName = "test"
		err := task.validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "else is missing")
	})

	t.Run("validate succeeds with all required fields", func(t *testing.T) {
		task := &IfTask{
			ExecutorFn: validBoolExecutor,
			Then:       thenTask,
			Else:       elseTask,
		}
		task.taskName = "test"
		err := task.validate()
		require.NoError(t, err)
	})

	t.Run("GetExecutorName returns executor name", func(t *testing.T) {
		registry := map[string]*Executor{
			"validBoolExecutor": {
				Name: "validBoolExecutor",
				Func: validBoolExecutor,
			},
		}
		task := &IfTask{
			ExecutorFn: validBoolExecutor,
		}
		name := task.GetExecutorName(registry)
		require.Equal(t, "validBoolExecutor", name)
	})
}

func TestSleepTask(t *testing.T) {
	end := &EndTask{}
	end.taskName = "end"

	t.Run("Type returns TaskTypeSleepTask", func(t *testing.T) {
		task := &SleepTask{}
		require.Equal(t, TaskTypeSleepTask, task.Type())
	})

	t.Run("validate requires task name", func(t *testing.T) {
		task := &SleepTask{}
		err := task.validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "task name is missing")
	})

	t.Run("validate requires executor", func(t *testing.T) {
		task := &SleepTask{}
		task.taskName = "test"
		err := task.validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "executor is missing")
	})

	t.Run("validate requires executor to be a function", func(t *testing.T) {
		task := &SleepTask{
			ExecutorFn: "not a function",
		}
		task.taskName = "test"
		err := task.validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "must be a function")
	})

	t.Run("validate requires executor to have at least 3 parameters", func(t *testing.T) {
		invalidFunc := func(ctx context.Context) (time.Duration, error) {
			return time.Second, nil
		}
		task := &SleepTask{
			ExecutorFn: invalidFunc,
		}
		task.taskName = "test"
		err := task.validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "must take at least 3 parameters")
	})

	t.Run("validate requires first parameter to be context.Context", func(t *testing.T) {
		invalidFunc := func(notCtx string, info *PlanExecutionInfo, input string) (time.Duration, error) {
			return time.Second, nil
		}
		task := &SleepTask{
			ExecutorFn: invalidFunc,
		}
		task.taskName = "test"
		err := task.validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "must have context.Context as first parameter")
	})

	t.Run("validate requires second parameter to be *PlanExecutionInfo", func(t *testing.T) {
		invalidFunc := func(ctx context.Context, notInfo string, input string) (time.Duration, error) {
			return time.Second, nil
		}
		task := &SleepTask{
			ExecutorFn: invalidFunc,
		}
		task.taskName = "test"
		err := task.validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "must have *PlanExecutionInfo as second parameter")
	})

	t.Run("validate requires executor to return 2 values", func(t *testing.T) {
		invalidFunc := func(ctx context.Context, info *PlanExecutionInfo, input string) time.Duration {
			return time.Second
		}
		task := &SleepTask{
			ExecutorFn: invalidFunc,
		}
		task.taskName = "test"
		err := task.validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "must return exactly 2 values")
	})

	t.Run("validate requires first return value to be time.Duration", func(t *testing.T) {
		task := &SleepTask{
			ExecutorFn: invalidSleepExecutor,
		}
		task.taskName = "test"
		err := task.validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "must return time.Duration as first return value")
	})

	t.Run("validate requires second return value to be error", func(t *testing.T) {
		invalidFunc := func(ctx context.Context, info *PlanExecutionInfo, input string) (time.Duration, string) {
			return time.Second, "not error"
		}
		task := &SleepTask{
			ExecutorFn: invalidFunc,
		}
		task.taskName = "test"
		err := task.validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "must return error as second return value")
	})

	t.Run("validate requires next task", func(t *testing.T) {
		task := &SleepTask{
			ExecutorFn: validDurationExecutor,
		}
		task.taskName = "test"
		err := task.validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "next task is missing")
	})

	t.Run("validate succeeds with all required fields", func(t *testing.T) {
		task := &SleepTask{
			stepTask: stepTask{
				Next: end,
			},
			ExecutorFn: validDurationExecutor,
		}
		task.taskName = "test"
		err := task.validate()
		require.NoError(t, err)
	})

	t.Run("GetExecutorName returns executor name", func(t *testing.T) {
		registry := map[string]*Executor{
			"validDurationExecutor": {
				Name: "validDurationExecutor",
				Func: validDurationExecutor,
			},
		}
		task := &SleepTask{
			ExecutorFn: validDurationExecutor,
		}
		name := task.GetExecutorName(registry)
		require.Equal(t, "validDurationExecutor", name)
	})
}

func TestAsyncTask(t *testing.T) {
	end := &EndTask{}
	end.taskName = "end"

	t.Run("Type returns TaskTypeAsyncTask", func(t *testing.T) {
		task := &AsyncTask{}
		require.Equal(t, TaskTypeAsyncTask, task.Type())
	})

	t.Run("validate requires task name", func(t *testing.T) {
		task := &AsyncTask{}
		err := task.validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "task name is missing")
	})

	t.Run("validate requires execution function", func(t *testing.T) {
		task := &AsyncTask{}
		task.taskName = "test"
		err := task.validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "execution function is missing")
	})

	t.Run("validate requires result processor function", func(t *testing.T) {
		task := &AsyncTask{
			ExecutionFn: validAsyncExecutor,
		}
		task.taskName = "test"
		err := task.validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "result processor function is missing")
	})

	t.Run("validate requires execution function to be a function", func(t *testing.T) {
		task := &AsyncTask{
			ExecutionFn:       "not a function",
			ResultProcessorFn: validAsyncResultProcessor,
		}
		task.taskName = "test"
		err := task.validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "must be a function")
	})

	t.Run("validate requires execution function to have at least 3 parameters", func(t *testing.T) {
		invalidFunc := func(ctx context.Context) (string, error) {
			return "step-id", nil
		}
		task := &AsyncTask{
			ExecutionFn:       invalidFunc,
			ResultProcessorFn: validAsyncResultProcessor,
		}
		task.taskName = "test"
		err := task.validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "must take at least 3 parameters")
	})

	t.Run("validate requires execution function first parameter to be context.Context", func(t *testing.T) {
		invalidFunc := func(notCtx string, info *PlanExecutionInfo, input string) (string, error) {
			return "step-id", nil
		}
		task := &AsyncTask{
			ExecutionFn:       invalidFunc,
			ResultProcessorFn: validAsyncResultProcessor,
		}
		task.taskName = "test"
		err := task.validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "must have context.Context as first parameter")
	})

	t.Run("validate requires execution function second parameter to be *PlanExecutionInfo", func(t *testing.T) {
		invalidFunc := func(ctx context.Context, notInfo string, input string) (string, error) {
			return "step-id", nil
		}
		task := &AsyncTask{
			ExecutionFn:       invalidFunc,
			ResultProcessorFn: validAsyncResultProcessor,
		}
		task.taskName = "test"
		err := task.validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "must have *PlanExecutionInfo as second parameter")
	})

	t.Run("validate requires execution function to return 2 values", func(t *testing.T) {
		task := &AsyncTask{
			ExecutionFn:       invalidAsyncExecutor,
			ResultProcessorFn: validAsyncResultProcessor,
		}
		task.taskName = "test"
		err := task.validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "must return exactly 2 values")
	})

	t.Run("validate requires first return value to be string", func(t *testing.T) {
		invalidFunc := func(ctx context.Context, info *PlanExecutionInfo, input string) (int, error) {
			return 123, nil
		}
		task := &AsyncTask{
			ExecutionFn:       invalidFunc,
			ResultProcessorFn: validAsyncResultProcessor,
		}
		task.taskName = "test"
		err := task.validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "must return string as first return value")
	})

	t.Run("validate requires second return value to be error", func(t *testing.T) {
		invalidFunc := func(ctx context.Context, info *PlanExecutionInfo, input string) (string, string) {
			return "step-id", "not error"
		}
		task := &AsyncTask{
			ExecutionFn:       invalidFunc,
			ResultProcessorFn: validAsyncResultProcessor,
		}
		task.taskName = "test"
		err := task.validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "must return error as second return value")
	})

	t.Run("validate requires result processor to be a function", func(t *testing.T) {
		task := &AsyncTask{
			ExecutionFn:       validAsyncExecutor,
			ResultProcessorFn: "not a function",
		}
		task.taskName = "test"
		err := task.validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "must be a function")
	})

	t.Run("validate requires result processor to take 4 parameters", func(t *testing.T) {
		task := &AsyncTask{
			ExecutionFn:       validAsyncExecutor,
			ResultProcessorFn: invalidAsyncResultProcessor,
		}
		task.taskName = "test"
		err := task.validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "must take at least 4 parameters")
	})

	t.Run("validate requires result processor first parameter to be context.Context", func(t *testing.T) {
		invalidFunc := func(notCtx string, info *PlanExecutionInfo, input interface{}, stepID string, result string) (interface{}, error) {
			return nil, nil
		}
		task := &AsyncTask{
			ExecutionFn:       validAsyncExecutor,
			ResultProcessorFn: invalidFunc,
		}
		task.taskName = "test"
		err := task.validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "must have context.Context as first parameter")
	})

	t.Run("validate requires result processor second parameter to be *PlanExecutionInfo", func(t *testing.T) {
		invalidFunc := func(ctx context.Context, notInfo string, input interface{}, stepID string, result string) (interface{}, error) {
			return nil, nil
		}
		task := &AsyncTask{
			ExecutionFn:       validAsyncExecutor,
			ResultProcessorFn: invalidFunc,
		}
		task.taskName = "test"
		err := task.validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "must have *PlanExecutionInfo as second parameter")
	})

	t.Run("validate requires result processor to return 2 values", func(t *testing.T) {
		invalidFunc := func(ctx context.Context, info *PlanExecutionInfo, input interface{}, stepID string, result string) error {
			return nil
		}
		task := &AsyncTask{
			ExecutionFn:       validAsyncExecutor,
			ResultProcessorFn: invalidFunc,
		}
		task.taskName = "test"
		err := task.validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "must return exactly 2 values")
	})

	t.Run("validate requires result processor second return value to be error", func(t *testing.T) {
		invalidFunc := func(ctx context.Context, info *PlanExecutionInfo, input interface{}, stepID string, result string) (interface{}, string) {
			return nil, "not error"
		}
		task := &AsyncTask{
			ExecutionFn:       validAsyncExecutor,
			ResultProcessorFn: invalidFunc,
		}
		task.taskName = "test"
		err := task.validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "must return error as second return value")
	})

	t.Run("validate requires next task", func(t *testing.T) {
		task := &AsyncTask{
			ExecutionFn:       validAsyncExecutor,
			ResultProcessorFn: validAsyncResultProcessor,
		}
		task.taskName = "test"
		err := task.validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "next task is missing")
	})

	t.Run("validate succeeds with all required fields", func(t *testing.T) {
		task := &AsyncTask{
			stepTask: stepTask{
				Next: end,
			},
			ExecutionFn:       validAsyncExecutor,
			ResultProcessorFn: validAsyncResultProcessor,
		}
		task.taskName = "test"
		err := task.validate()
		require.NoError(t, err)
	})

	t.Run("GetExecutionFnName returns executor name", func(t *testing.T) {
		registry := map[string]*Executor{
			"validAsyncExecutor": {
				Name: "validAsyncExecutor",
				Func: validAsyncExecutor,
			},
		}
		task := &AsyncTask{
			ExecutionFn: validAsyncExecutor,
		}
		name := task.GetExecutionFnName(registry)
		require.Equal(t, "validAsyncExecutor", name)
	})

	t.Run("GetExecutionFnName returns unknown for unregistered executor", func(t *testing.T) {
		registry := map[string]*Executor{}
		task := &AsyncTask{
			ExecutionFn: validAsyncExecutor,
		}
		name := task.GetExecutionFnName(registry)
		require.Equal(t, "unknown", name)
	})

	t.Run("GetResultProcessorFnName returns executor name", func(t *testing.T) {
		registry := map[string]*Executor{
			"validAsyncResultProcessor": {
				Name: "validAsyncResultProcessor",
				Func: validAsyncResultProcessor,
			},
		}
		task := &AsyncTask{
			ResultProcessorFn: validAsyncResultProcessor,
		}
		name := task.GetResultProcessorFnName(registry)
		require.Equal(t, "validAsyncResultProcessor", name)
	})

	t.Run("GetResultProcessorFnName returns unknown for unregistered executor", func(t *testing.T) {
		registry := map[string]*Executor{}
		task := &AsyncTask{
			ResultProcessorFn: validAsyncResultProcessor,
		}
		name := task.GetResultProcessorFnName(registry)
		require.Equal(t, "unknown", name)
	})
}

func TestGetExecutorName(t *testing.T) {
	t.Run("returns function name when registered", func(t *testing.T) {
		registry := map[string]*Executor{
			"validExecutor": {
				Name: "validExecutor",
				Func: validExecutor,
			},
		}
		name := getExecutorName(validExecutor, registry)
		require.Equal(t, "validExecutor", name)
	})

	t.Run("returns unknown when not registered", func(t *testing.T) {
		registry := map[string]*Executor{}
		name := getExecutorName(validExecutor, registry)
		require.Equal(t, "unknown", name)
	})

	t.Run("handles multiple executors in registry", func(t *testing.T) {
		registry := map[string]*Executor{
			"exec1": {
				Name: "exec1",
				Func: validExecutor,
			},
			"exec2": {
				Name: "exec2",
				Func: validBoolExecutor,
			},
		}
		name := getExecutorName(validBoolExecutor, registry)
		require.Equal(t, "validBoolExecutor", name)
	})
}
