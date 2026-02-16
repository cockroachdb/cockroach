// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package planners provide tests for task type definitions and validation.
// These tests verify that all task types (ExecutionTask, ForkTask, ConditionTask,
// SleepTask, CallbackTask, ChildPlanTask) validate correctly.
package planners

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

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
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "task name is missing")
	})

	t.Run("validate requires next task", func(t *testing.T) {
		task := &stepTask{}
		task.taskName = "test"
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "next task is missing")
	})

	t.Run("validate succeeds with name and next", func(t *testing.T) {
		task := &stepTask{
			Next: end,
		}
		task.taskName = "test"
		err := task.validate(false)
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
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "task name is missing")
	})

	t.Run("validate requires executor", func(t *testing.T) {
		task := &ExecutionTask{}
		task.taskName = "test"
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "executor is missing")
	})

	t.Run("validate requires executor to be a function", func(t *testing.T) {
		task := &ExecutionTask{
			ExecutorFn: "not a function",
		}
		task.taskName = "test"
		err := task.validate(false)
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
		err := task.validate(false)
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
		err := task.validate(false)
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
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "must have *PlanExecutionInfo as second parameter")
	})

	t.Run("validate succeeds with name and executor", func(t *testing.T) {
		task := &ExecutionTask{
			ExecutorFn: validExecutor,
		}
		task.taskName = "test"
		err := task.validate(false)
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

	join := &ForkJoinTask{}
	join.taskName = "join"

	t.Run("Type returns TaskTypeFork", func(t *testing.T) {
		task := &ForkTask{}
		require.Equal(t, TaskTypeFork, task.Type())
	})

	t.Run("validate requires task name", func(t *testing.T) {
		task := &ForkTask{}
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "task name is missing")
	})

	t.Run("validate requires next task", func(t *testing.T) {
		task := &ForkTask{}
		task.taskName = "test"
		err := task.validate(false)
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
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "tasks is missing")
	})

	t.Run("validate succeeds with all required fields", func(t *testing.T) {
		task := &ForkTask{
			stepTask: stepTask{
				Next: end,
			},
			Tasks: []Task{branch1, branch2},
			Join:  join,
		}
		task.taskName = "test"
		err := task.validate(false)
		require.NoError(t, err)
	})
}

func TestEndTask(t *testing.T) {
	t.Run("Type returns TaskTypeEndTask", func(t *testing.T) {
		task := &EndTask{}
		require.Equal(t, TaskTypeEndTask, task.Type())
	})

	t.Run("validate requires task name", func(t *testing.T) {
		task := &EndTask{}
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "task name is missing")
	})

	t.Run("validate succeeds with task name", func(t *testing.T) {
		task := &EndTask{}
		task.taskName = "end"
		err := task.validate(false)
		require.NoError(t, err)
	})
}

func TestConditionTask(t *testing.T) {
	thenTask := &EndTask{}
	thenTask.taskName = "then"

	elseTask := &EndTask{}
	elseTask.taskName = "else"

	t.Run("Type returns TaskTypeConditionTask", func(t *testing.T) {
		task := &ConditionTask{}
		require.Equal(t, TaskTypeConditionTask, task.Type())
	})

	t.Run("validate requires executor", func(t *testing.T) {
		task := &ConditionTask{}
		task.taskName = "test"
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "executor is missing")
	})

	t.Run("validate requires executor to be a function", func(t *testing.T) {
		task := &ConditionTask{
			ExecutorFn: "not a function",
		}
		task.taskName = "test"
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "must be a function")
	})

	t.Run("validate requires executor to have at least 3 parameters", func(t *testing.T) {
		invalidFunc := func(ctx context.Context) (bool, error) {
			return true, nil
		}
		task := &ConditionTask{
			ExecutorFn: invalidFunc,
		}
		task.taskName = "test"
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "must take at least 3 parameters")
	})

	t.Run("validate requires first parameter to be context.Context", func(t *testing.T) {
		invalidFunc := func(notCtx string, info *PlanExecutionInfo, input string) (bool, error) {
			return true, nil
		}
		task := &ConditionTask{
			ExecutorFn: invalidFunc,
		}
		task.taskName = "test"
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "must have context.Context as first parameter")
	})

	t.Run("validate requires second parameter to be *PlanExecutionInfo", func(t *testing.T) {
		invalidFunc := func(ctx context.Context, notInfo string, input string) (bool, error) {
			return true, nil
		}
		task := &ConditionTask{
			ExecutorFn: invalidFunc,
		}
		task.taskName = "test"
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "must have *PlanExecutionInfo as second parameter")
	})

	t.Run("validate requires executor to return 2 values", func(t *testing.T) {
		invalidFunc := func(ctx context.Context, info *PlanExecutionInfo, input string) bool {
			return true
		}
		task := &ConditionTask{
			ExecutorFn: invalidFunc,
		}
		task.taskName = "test"
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "must return exactly 2 values")
	})

	t.Run("validate requires first return value to be bool", func(t *testing.T) {
		task := &ConditionTask{
			ExecutorFn: invalidIfExecutor,
		}
		task.taskName = "test"
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "must return bool as first return value")
	})

	t.Run("validate requires second return value to be error", func(t *testing.T) {
		invalidFunc := func(ctx context.Context, info *PlanExecutionInfo, input string) (bool, string) {
			return true, "not error"
		}
		task := &ConditionTask{
			ExecutorFn: invalidFunc,
		}
		task.taskName = "test"
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "must return error as second return value")
	})

	t.Run("validate requires then task", func(t *testing.T) {
		task := &ConditionTask{
			ExecutorFn: validBoolExecutor,
		}
		task.taskName = "test"
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "then is missing")
	})

	t.Run("validate requires else task", func(t *testing.T) {
		task := &ConditionTask{
			ExecutorFn: validBoolExecutor,
			Then:       thenTask,
		}
		task.taskName = "test"
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "else is missing")
	})

	t.Run("validate succeeds with all required fields", func(t *testing.T) {
		task := &ConditionTask{
			ExecutorFn: validBoolExecutor,
			Then:       thenTask,
			Else:       elseTask,
		}
		task.taskName = "test"
		err := task.validate(false)
		require.NoError(t, err)
	})

	t.Run("GetExecutorName returns executor name", func(t *testing.T) {
		registry := map[string]*Executor{
			"validBoolExecutor": {
				Name: "validBoolExecutor",
				Func: validBoolExecutor,
			},
		}
		task := &ConditionTask{
			ExecutorFn: validBoolExecutor,
		}
		name := task.GetExecutorName(registry)
		require.Equal(t, "validBoolExecutor", name)
	})
}

func TestCallbackTask(t *testing.T) {
	end := &EndTask{}
	end.taskName = "end"

	t.Run("Type returns TaskTypeCallbackTask", func(t *testing.T) {
		task := &CallbackTask{}
		require.Equal(t, TaskTypeCallbackTask, task.Type())
	})

	t.Run("validate requires task name", func(t *testing.T) {
		task := &CallbackTask{}
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "task name is missing")
	})

	t.Run("validate requires execution function", func(t *testing.T) {
		task := &CallbackTask{}
		task.taskName = "test"
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "execution function is missing")
	})

	t.Run("validate requires result processor function", func(t *testing.T) {
		task := &CallbackTask{
			ExecutionFn: validAsyncExecutor,
		}
		task.taskName = "test"
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "result processor function is missing")
	})

	t.Run("validate requires execution function to be a function", func(t *testing.T) {
		task := &CallbackTask{
			ExecutionFn:       "not a function",
			ResultProcessorFn: validAsyncResultProcessor,
		}
		task.taskName = "test"
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "must be a function")
	})

	t.Run("validate requires execution function to have at least 3 parameters", func(t *testing.T) {
		invalidFunc := func(ctx context.Context) (string, error) {
			return "step-id", nil
		}
		task := &CallbackTask{
			ExecutionFn:       invalidFunc,
			ResultProcessorFn: validAsyncResultProcessor,
		}
		task.taskName = "test"
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "must take at least 3 parameters")
	})

	t.Run("validate requires execution function first parameter to be context.Context", func(t *testing.T) {
		invalidFunc := func(notCtx string, info *PlanExecutionInfo, input string) (string, error) {
			return "step-id", nil
		}
		task := &CallbackTask{
			ExecutionFn:       invalidFunc,
			ResultProcessorFn: validAsyncResultProcessor,
		}
		task.taskName = "test"
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "must have context.Context as first parameter")
	})

	t.Run("validate requires execution function second parameter to be *PlanExecutionInfo", func(t *testing.T) {
		invalidFunc := func(ctx context.Context, notInfo string, input string) (string, error) {
			return "step-id", nil
		}
		task := &CallbackTask{
			ExecutionFn:       invalidFunc,
			ResultProcessorFn: validAsyncResultProcessor,
		}
		task.taskName = "test"
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "must have *PlanExecutionInfo as second parameter")
	})

	t.Run("validate requires execution function to return 2 values", func(t *testing.T) {
		task := &CallbackTask{
			ExecutionFn:       invalidAsyncExecutor,
			ResultProcessorFn: validAsyncResultProcessor,
		}
		task.taskName = "test"
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "must return exactly 2 values")
	})

	t.Run("validate requires first return value to be string", func(t *testing.T) {
		invalidFunc := func(ctx context.Context, info *PlanExecutionInfo, input string) (int, error) {
			return 123, nil
		}
		task := &CallbackTask{
			ExecutionFn:       invalidFunc,
			ResultProcessorFn: validAsyncResultProcessor,
		}
		task.taskName = "test"
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "must return string as first return value")
	})

	t.Run("validate requires second return value to be error", func(t *testing.T) {
		invalidFunc := func(ctx context.Context, info *PlanExecutionInfo, input string) (string, string) {
			return "step-id", "not error"
		}
		task := &CallbackTask{
			ExecutionFn:       invalidFunc,
			ResultProcessorFn: validAsyncResultProcessor,
		}
		task.taskName = "test"
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "must return error as second return value")
	})

	t.Run("validate requires result processor to be a function", func(t *testing.T) {
		task := &CallbackTask{
			ExecutionFn:       validAsyncExecutor,
			ResultProcessorFn: "not a function",
		}
		task.taskName = "test"
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "must be a function")
	})

	t.Run("validate requires result processor to take 4 parameters", func(t *testing.T) {
		task := &CallbackTask{
			ExecutionFn:       validAsyncExecutor,
			ResultProcessorFn: invalidAsyncResultProcessor,
		}
		task.taskName = "test"
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "must take at least 4 parameters")
	})

	t.Run("validate requires result processor first parameter to be context.Context", func(t *testing.T) {
		invalidFunc := func(notCtx string, info *PlanExecutionInfo, input interface{}, stepID string, result string) (interface{}, error) {
			return nil, nil
		}
		task := &CallbackTask{
			ExecutionFn:       validAsyncExecutor,
			ResultProcessorFn: invalidFunc,
		}
		task.taskName = "test"
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "must have context.Context as first parameter")
	})

	t.Run("validate requires result processor second parameter to be *PlanExecutionInfo", func(t *testing.T) {
		invalidFunc := func(ctx context.Context, notInfo string, input interface{}, stepID string, result string) (interface{}, error) {
			return nil, nil
		}
		task := &CallbackTask{
			ExecutionFn:       validAsyncExecutor,
			ResultProcessorFn: invalidFunc,
		}
		task.taskName = "test"
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "must have *PlanExecutionInfo as second parameter")
	})

	t.Run("validate requires result processor to return 2 values", func(t *testing.T) {
		invalidFunc := func(ctx context.Context, info *PlanExecutionInfo, input interface{}, stepID string, result string) error {
			return nil
		}
		task := &CallbackTask{
			ExecutionFn:       validAsyncExecutor,
			ResultProcessorFn: invalidFunc,
		}
		task.taskName = "test"
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "must return exactly 2 values")
	})

	t.Run("validate requires result processor second return value to be error", func(t *testing.T) {
		invalidFunc := func(ctx context.Context, info *PlanExecutionInfo, input interface{}, stepID string, result string) (interface{}, string) {
			return nil, "not error"
		}
		task := &CallbackTask{
			ExecutionFn:       validAsyncExecutor,
			ResultProcessorFn: invalidFunc,
		}
		task.taskName = "test"
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "must return error as second return value")
	})

	t.Run("validate succeeds without next task (validated later in validateStepTaskPaths)", func(t *testing.T) {
		task := &CallbackTask{
			ExecutionFn:       validAsyncExecutor,
			ResultProcessorFn: validAsyncResultProcessor,
		}
		task.taskName = "test"
		err := task.validate(false)
		require.NoError(t, err)
	})

	t.Run("validate succeeds with all required fields", func(t *testing.T) {
		task := &CallbackTask{
			stepTask: stepTask{
				Next: end,
			},
			ExecutionFn:       validAsyncExecutor,
			ResultProcessorFn: validAsyncResultProcessor,
		}
		task.taskName = "test"
		err := task.validate(false)
		require.NoError(t, err)
	})

	t.Run("GetExecutionFnName returns executor name", func(t *testing.T) {
		registry := map[string]*Executor{
			"validAsyncExecutor": {
				Name: "validAsyncExecutor",
				Func: validAsyncExecutor,
			},
		}
		task := &CallbackTask{
			ExecutionFn: validAsyncExecutor,
		}
		name := task.GetExecutionFnName(registry)
		require.Equal(t, "validAsyncExecutor", name)
	})

	t.Run("GetExecutionFnName returns unknown for unregistered executor", func(t *testing.T) {
		registry := map[string]*Executor{}
		task := &CallbackTask{
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
		task := &CallbackTask{
			ResultProcessorFn: validAsyncResultProcessor,
		}
		name := task.GetResultProcessorFnName(registry)
		require.Equal(t, "validAsyncResultProcessor", name)
	})

	t.Run("GetResultProcessorFnName returns unknown for unregistered executor", func(t *testing.T) {
		registry := map[string]*Executor{}
		task := &CallbackTask{
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

func TestForkJoinTask(t *testing.T) {
	t.Run("Type returns TaskTypeForkJoinTask", func(t *testing.T) {
		task := &ForkJoinTask{}
		require.Equal(t, TaskTypeForkJoinTask, task.Type())
	})

	t.Run("validate requires task name", func(t *testing.T) {
		task := &ForkJoinTask{}
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "task name is missing")
	})

	t.Run("validate succeeds with task name", func(t *testing.T) {
		task := &ForkJoinTask{}
		task.taskName = "join"
		err := task.validate(false)
		require.NoError(t, err)
	})

	t.Run("isStepTask returns false", func(t *testing.T) {
		task := &ForkJoinTask{}
		require.False(t, task.isStepTask())
	})

	t.Run("getNextTask returns nil", func(t *testing.T) {
		task := &ForkJoinTask{}
		require.Nil(t, task.getNextTask())
	})

	t.Run("getFailTask returns nil", func(t *testing.T) {
		task := &ForkJoinTask{}
		require.Nil(t, task.getFailTask())
	})
}

func TestChildPlanTask(t *testing.T) {
	end := &EndTask{}
	end.taskName = "end"

	childTaskInfoFn := func(_ context.Context, _ *PlanExecutionInfo, _ string) (ChildTaskInfo, error) {
		return ChildTaskInfo{PlanVariant: "variant1", Input: "input"}, nil
	}

	t.Run("Type returns TaskTypeChildPlanTask", func(t *testing.T) {
		task := &ChildPlanTask{}
		require.Equal(t, TaskTypeChildPlanTask, task.Type())
	})

	t.Run("validate requires task name", func(t *testing.T) {
		task := &ChildPlanTask{}
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "task name is missing")
	})

	t.Run("validate requires plan name", func(t *testing.T) {
		task := &ChildPlanTask{}
		task.taskName = "test"
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "plan name is missing")
	})

	t.Run("validate requires executor function", func(t *testing.T) {
		task := &ChildPlanTask{
			PlanName: "child_plan",
		}
		task.taskName = "test"
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "executor function is missing")
	})

	t.Run("validate requires executor to be a function", func(t *testing.T) {
		task := &ChildPlanTask{
			PlanName:        "child_plan",
			ChildTaskInfoFn: "not a function",
		}
		task.taskName = "test"
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "must be a function")
	})

	t.Run("validate requires executor to have at least 3 parameters", func(t *testing.T) {
		invalidFunc := func(ctx context.Context) (ChildTaskInfo, error) {
			return ChildTaskInfo{}, nil
		}
		task := &ChildPlanTask{
			PlanName:        "child_plan",
			ChildTaskInfoFn: invalidFunc,
		}
		task.taskName = "test"
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "must take at least 3 parameters")
	})

	t.Run("validate requires first parameter to be context.Context", func(t *testing.T) {
		invalidFunc := func(notCtx string, info *PlanExecutionInfo, input string) (ChildTaskInfo, error) {
			return ChildTaskInfo{}, nil
		}
		task := &ChildPlanTask{
			PlanName:        "child_plan",
			ChildTaskInfoFn: invalidFunc,
		}
		task.taskName = "test"
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "must have context.Context as first parameter")
	})

	t.Run("validate requires second parameter to be *PlanExecutionInfo", func(t *testing.T) {
		invalidFunc := func(ctx context.Context, notInfo string, input string) (ChildTaskInfo, error) {
			return ChildTaskInfo{}, nil
		}
		task := &ChildPlanTask{
			PlanName:        "child_plan",
			ChildTaskInfoFn: invalidFunc,
		}
		task.taskName = "test"
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "must have *PlanExecutionInfo as second parameter")
	})

	t.Run("validate requires executor to return 2 values", func(t *testing.T) {
		invalidFunc := func(ctx context.Context, info *PlanExecutionInfo, input string) ChildTaskInfo {
			return ChildTaskInfo{}
		}
		task := &ChildPlanTask{
			PlanName:        "child_plan",
			ChildTaskInfoFn: invalidFunc,
		}
		task.taskName = "test"
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "must return exactly 2 values")
	})

	t.Run("validate requires first return value to be ChildTaskInfo", func(t *testing.T) {
		invalidFunc := func(ctx context.Context, info *PlanExecutionInfo, input string) (string, error) {
			return "not ChildTaskInfo", nil
		}
		task := &ChildPlanTask{
			PlanName:        "child_plan",
			ChildTaskInfoFn: invalidFunc,
		}
		task.taskName = "test"
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "must return ChildTaskInfo as first return value")
	})

	t.Run("validate requires second return value to be error", func(t *testing.T) {
		invalidFunc := func(ctx context.Context, info *PlanExecutionInfo, input string) (ChildTaskInfo, string) {
			return ChildTaskInfo{}, "not error"
		}
		task := &ChildPlanTask{
			PlanName:        "child_plan",
			ChildTaskInfoFn: invalidFunc,
		}
		task.taskName = "test"
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "must return error as second return value")
	})

	t.Run("validate succeeds with all required fields", func(t *testing.T) {
		task := &ChildPlanTask{
			stepTask: stepTask{
				Next: end,
			},
			PlanName:        "child_plan",
			ChildTaskInfoFn: childTaskInfoFn,
		}
		task.taskName = "test"
		err := task.validate(false)
		require.NoError(t, err)
	})

	t.Run("validate succeeds without next task (validated later)", func(t *testing.T) {
		task := &ChildPlanTask{
			PlanName:        "child_plan",
			ChildTaskInfoFn: childTaskInfoFn,
		}
		task.taskName = "test"
		err := task.validate(false)
		require.NoError(t, err)
	})

	t.Run("GetExecutorName returns executor name", func(t *testing.T) {
		// Use a named function for this test
		namedChildTaskInfoFn := func(_ context.Context, _ *PlanExecutionInfo, _ string) (ChildTaskInfo, error) {
			return ChildTaskInfo{}, nil
		}

		registry := map[string]*Executor{
			"childTaskInfoFn": {
				Name: "childTaskInfoFn",
				Func: namedChildTaskInfoFn,
			},
		}
		task := &ChildPlanTask{
			ChildTaskInfoFn: namedChildTaskInfoFn,
		}
		name := task.GetExecutorName(registry)
		// GetFunctionName returns the reflection-based name which will be the function name
		// For anonymous functions, this will be something like "func1"
		require.NotEqual(t, "unknown", name)
		require.NotEmpty(t, name)
	})

	t.Run("GetExecutorName returns unknown for unregistered executor", func(t *testing.T) {
		registry := map[string]*Executor{}
		task := &ChildPlanTask{
			ChildTaskInfoFn: childTaskInfoFn,
		}
		name := task.GetExecutorName(registry)
		require.Equal(t, "unknown", name)
	})

	t.Run("validate with params", func(t *testing.T) {
		childTaskInfoFnWithParam := func(_ context.Context, _ *PlanExecutionInfo, _ string, _ int) (ChildTaskInfo, error) {
			return ChildTaskInfo{}, nil
		}

		param := &EndTask{}
		param.taskName = "param"

		task := &ChildPlanTask{
			PlanName:        "child_plan",
			ChildTaskInfoFn: childTaskInfoFnWithParam,
			Params:          []Task{param},
		}
		task.taskName = "test"
		err := task.validate(false)
		require.NoError(t, err)
	})

	t.Run("validate with incorrect param count", func(t *testing.T) {
		childTaskInfoFnWithParam := func(_ context.Context, _ *PlanExecutionInfo, _ string, _ int) (ChildTaskInfo, error) {
			return ChildTaskInfo{}, nil
		}

		task := &ChildPlanTask{
			PlanName:        "child_plan",
			ChildTaskInfoFn: childTaskInfoFnWithParam,
			Params:          []Task{}, // Expects 1, got 0
		}
		task.taskName = "test"
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "expects 1 Param(s), got 0")
	})

	t.Run("validate in failure path with correct param count", func(t *testing.T) {
		childTaskInfoFnFailure := func(_ context.Context, _ *PlanExecutionInfo, _ string, _ int, _ string) (ChildTaskInfo, error) {
			return ChildTaskInfo{}, nil
		}

		param := &EndTask{}
		param.taskName = "param"

		task := &ChildPlanTask{
			PlanName:        "child_plan",
			ChildTaskInfoFn: childTaskInfoFnFailure,
			Params:          []Task{param},
		}
		task.taskName = "test"
		err := task.validate(true) // failure path
		require.NoError(t, err)
	})

	t.Run("validate in failure path with incorrect param count", func(t *testing.T) {
		childTaskInfoFnFailure := func(_ context.Context, _ *PlanExecutionInfo, _ string, _ string) (ChildTaskInfo, error) {
			return ChildTaskInfo{}, nil
		}

		param := &EndTask{}
		param.taskName = "param"

		task := &ChildPlanTask{
			PlanName:        "child_plan",
			ChildTaskInfoFn: childTaskInfoFnFailure,
			Params:          []Task{param}, // Expects 0 params (3 base + 1 error), got 1
		}
		task.taskName = "test"
		err := task.validate(true) // failure path
		require.Error(t, err)
		require.Contains(t, err.Error(), "in failure path expects 0 Param(s), got 1")
	})
}

func TestExecutionTaskAdditionalValidation(t *testing.T) {
	t.Run("validate with multiple params", func(t *testing.T) {
		executorWithTwoParams := func(_ context.Context, _ *PlanExecutionInfo, _ string, _ int, _ bool) (interface{}, error) {
			return nil, nil
		}

		param1 := &EndTask{}
		param1.taskName = "param1"

		param2 := &EndTask{}
		param2.taskName = "param2"

		task := &ExecutionTask{
			ExecutorFn: executorWithTwoParams,
			Params:     []Task{param1, param2},
		}
		task.taskName = "test"
		err := task.validate(false)
		require.NoError(t, err)
	})

	t.Run("validate returns error for wrong number of outputs", func(t *testing.T) {
		executorOneOutput := func(_ context.Context, _ *PlanExecutionInfo, _ string) interface{} {
			return nil
		}

		task := &ExecutionTask{
			ExecutorFn: executorOneOutput,
		}
		task.taskName = "test"
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "must return exactly 2 values")
	})

	t.Run("validate returns error for wrong second output type", func(t *testing.T) {
		executorWrongOutput := func(_ context.Context, _ *PlanExecutionInfo, _ string) (interface{}, string) {
			return nil, "not error"
		}

		task := &ExecutionTask{
			ExecutorFn: executorWrongOutput,
		}
		task.taskName = "test"
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "must return error as second return value")
	})
}

func TestForkTaskAdditionalValidation(t *testing.T) {
	t.Run("validate requires join point", func(t *testing.T) {
		end := &EndTask{}
		end.taskName = "end"

		branch := &EndTask{}
		branch.taskName = "branch"

		task := &ForkTask{
			stepTask: stepTask{
				Next: end,
			},
			Tasks: []Task{branch},
			// Join is nil
		}
		task.taskName = "fork"
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "join point is missing")
	})
}

func TestConditionTaskAdditionalValidation(t *testing.T) {
	t.Run("validate with multiple params", func(t *testing.T) {
		condExecutorWithTwoParams := func(_ context.Context, _ *PlanExecutionInfo, _ string, _ int, _ bool) (bool, error) {
			return true, nil
		}

		param1 := &EndTask{}
		param1.taskName = "param1"

		param2 := &EndTask{}
		param2.taskName = "param2"

		thenTask := &EndTask{}
		thenTask.taskName = "then"

		elseTask := &EndTask{}
		elseTask.taskName = "else"

		task := &ConditionTask{
			ExecutorFn: condExecutorWithTwoParams,
			Params:     []Task{param1, param2},
			Then:       thenTask,
			Else:       elseTask,
		}
		task.taskName = "test"
		err := task.validate(false)
		require.NoError(t, err)
	})
}

func TestCallbackTaskAdditionalValidation(t *testing.T) {
	t.Run("validate with multiple params", func(t *testing.T) {
		callbackExecutorWithTwoParams := func(_ context.Context, _ *PlanExecutionInfo, _ string, _ int, _ bool) (string, error) {
			return "step-id", nil
		}

		callbackResultProcessor := func(_ context.Context, _ *PlanExecutionInfo, _ interface{}, _ string, _ string) (interface{}, error) {
			return nil, nil
		}

		param1 := &EndTask{}
		param1.taskName = "param1"

		param2 := &EndTask{}
		param2.taskName = "param2"

		task := &CallbackTask{
			ExecutionFn:       callbackExecutorWithTwoParams,
			ResultProcessorFn: callbackResultProcessor,
			Params:            []Task{param1, param2},
		}
		task.taskName = "test"
		err := task.validate(false)
		require.NoError(t, err)
	})

	t.Run("validate with incorrect param count in normal path", func(t *testing.T) {
		callbackExecutorWithTwoParams := func(_ context.Context, _ *PlanExecutionInfo, _ string, _ int, _ bool) (string, error) {
			return "step-id", nil
		}

		callbackResultProcessor := func(_ context.Context, _ *PlanExecutionInfo, _ interface{}, _ string, _ string) (interface{}, error) {
			return nil, nil
		}

		param1 := &EndTask{}
		param1.taskName = "param1"

		task := &CallbackTask{
			ExecutionFn:       callbackExecutorWithTwoParams,
			ResultProcessorFn: callbackResultProcessor,
			Params:            []Task{param1}, // Expects 2 params, got 1
		}
		task.taskName = "test"
		err := task.validate(false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "expects 2 Param(s), got 1")
	})

	t.Run("validate in failure path with correct param count", func(t *testing.T) {
		callbackExecutorFailure := func(_ context.Context, _ *PlanExecutionInfo, _ string, _ int, _ string) (string, error) {
			return "step-id", nil
		}

		callbackResultProcessor := func(_ context.Context, _ *PlanExecutionInfo, _ interface{}, _ string, _ string) (interface{}, error) {
			return nil, nil
		}

		param := &EndTask{}
		param.taskName = "param"

		task := &CallbackTask{
			ExecutionFn:       callbackExecutorFailure,
			ResultProcessorFn: callbackResultProcessor,
			Params:            []Task{param},
		}
		task.taskName = "test"
		err := task.validate(true) // failure path
		require.NoError(t, err)
	})

	t.Run("validate in failure path with incorrect param count", func(t *testing.T) {
		callbackExecutorFailure := func(_ context.Context, _ *PlanExecutionInfo, _ string, _ string) (string, error) {
			return "step-id", nil
		}

		callbackResultProcessor := func(_ context.Context, _ *PlanExecutionInfo, _ interface{}, _ string, _ string) (interface{}, error) {
			return nil, nil
		}

		param := &EndTask{}
		param.taskName = "param"

		task := &CallbackTask{
			ExecutionFn:       callbackExecutorFailure,
			ResultProcessorFn: callbackResultProcessor,
			Params:            []Task{param}, // Expects 0 params (3 base + 1 error), got 1
		}
		task.taskName = "test"
		err := task.validate(true) // failure path
		require.Error(t, err)
		require.Contains(t, err.Error(), "in failure path expects 0 Param(s), got 1")
	})
}

/********************* Mocks beyond this point *********************/

// Test executor functions for tasks_test.go
func validExecutor(_ context.Context, _ *PlanExecutionInfo, _ string) (interface{}, error) {
	return nil, nil
}

func validBoolExecutor(_ context.Context, _ *PlanExecutionInfo, _ string) (bool, error) {
	return true, nil
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

func invalidAsyncExecutor(_ context.Context, _ *PlanExecutionInfo, _ string) error {
	return nil
}

func invalidAsyncResultProcessor(_ context.Context) error {
	return nil
}
