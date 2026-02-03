// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package planners provide tests for status and execution tracking functionality.
// These tests verify task serialization, plan structure serialization, and
// workflow information representation.
package planners

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Test executor for status tests
func statusTestExecutor(_ context.Context, _ *PlanExecutionInfo) error {
	return nil
}

func statusTestBoolExecutor(_ context.Context, _ *PlanExecutionInfo) (bool, error) {
	return true, nil
}

func statusTestDurationExecutor(_ context.Context, _ *PlanExecutionInfo) (time.Duration, error) {
	return time.Second, nil
}

func statusTestAsyncExecutor(_ context.Context, _ *PlanExecutionInfo) (string, error) {
	return "step-id", nil
}

func statusTestAsyncResultProcessor(
	_ context.Context, _ *PlanExecutionInfo, _ interface{}, _ string, _ string,
) (interface{}, error) {
	return "processed", nil
}

func TestSerializeTask(t *testing.T) {
	t.Run("serialize EndTask", func(t *testing.T) {
		task := &EndTask{}
		task.taskName = "end_task"

		registry := map[string]*Executor{}
		taskInfo := SerializeTask(task, registry)

		require.Equal(t, "end_task", taskInfo.Name)
		require.Equal(t, string(TaskTypeEndTask), taskInfo.Type)
		require.Empty(t, taskInfo.Next)
		require.Empty(t, taskInfo.Fail)
		require.Empty(t, taskInfo.Executor)
		require.Nil(t, taskInfo.Params)
		require.Nil(t, taskInfo.ForkTasks)
	})

	t.Run("serialize ExecutionTask", func(t *testing.T) {
		end := &EndTask{}
		end.taskName = "end"

		fail := &EndTask{}
		fail.taskName = "fail"

		param1 := &EndTask{}
		param1.taskName = "param1"

		param2 := &EndTask{}
		param2.taskName = "param2"

		task := &ExecutionTask{
			stepTask: stepTask{
				Next: end,
				Fail: fail,
			},
			ExecutorFn: statusTestExecutor,
			Params:     []Task{param1, param2},
		}
		task.taskName = "exec_task"

		registry := map[string]*Executor{
			"statusTestExecutor": {
				Name: "statusTestExecutor",
				Func: statusTestExecutor,
			},
		}
		taskInfo := SerializeTask(task, registry)

		require.Equal(t, "exec_task", taskInfo.Name)
		require.Equal(t, string(TaskTypeExecution), taskInfo.Type)
		require.Equal(t, "end", taskInfo.Next)
		require.Equal(t, "fail", taskInfo.Fail)
		require.Equal(t, "statusTestExecutor", taskInfo.Executor)
		require.Equal(t, []string{"param1", "param2"}, taskInfo.Params)
	})

	t.Run("serialize ExecutionTask without fail", func(t *testing.T) {
		end := &EndTask{}
		end.taskName = "end"

		task := &ExecutionTask{
			stepTask: stepTask{
				Next: end,
			},
			ExecutorFn: statusTestExecutor,
		}
		task.taskName = "exec_task"

		registry := map[string]*Executor{
			"statusTestExecutor": {
				Name: "statusTestExecutor",
				Func: statusTestExecutor,
			},
		}
		taskInfo := SerializeTask(task, registry)

		require.Equal(t, "exec_task", taskInfo.Name)
		require.Equal(t, "end", taskInfo.Next)
		require.Empty(t, taskInfo.Fail)
	})

	t.Run("serialize ForkTask", func(t *testing.T) {
		end := &EndTask{}
		end.taskName = "end"

		branch1 := &EndTask{}
		branch1.taskName = "branch1"

		branch2 := &EndTask{}
		branch2.taskName = "branch2"

		task := &ForkTask{
			stepTask: stepTask{
				Next: end,
			},
			Tasks: []Task{branch1, branch2},
		}
		task.taskName = "fork_task"

		registry := map[string]*Executor{}
		taskInfo := SerializeTask(task, registry)

		require.Equal(t, "fork_task", taskInfo.Name)
		require.Equal(t, string(TaskTypeFork), taskInfo.Type)
		require.Equal(t, "end", taskInfo.Next)
		require.Equal(t, []string{"branch1", "branch2"}, taskInfo.ForkTasks)
	})

	t.Run("serialize IfTask", func(t *testing.T) {
		thenTask := &EndTask{}
		thenTask.taskName = "then"

		elseTask := &EndTask{}
		elseTask.taskName = "else"

		param := &EndTask{}
		param.taskName = "param"

		task := &IfTask{
			ExecutorFn: statusTestBoolExecutor,
			Then:       thenTask,
			Else:       elseTask,
			Params:     []Task{param},
		}
		task.taskName = "if_task"

		registry := map[string]*Executor{
			"statusTestBoolExecutor": {
				Name: "statusTestBoolExecutor",
				Func: statusTestBoolExecutor,
			},
		}
		taskInfo := SerializeTask(task, registry)

		require.Equal(t, "if_task", taskInfo.Name)
		require.Equal(t, string(TaskTypeIfTask), taskInfo.Type)
		require.Equal(t, "statusTestBoolExecutor", taskInfo.Executor)
		require.Equal(t, "then", taskInfo.Then)
		require.Equal(t, "else", taskInfo.Else)
		require.Equal(t, []string{"param"}, taskInfo.Params)
	})

	t.Run("serialize SleepTask", func(t *testing.T) {
		end := &EndTask{}
		end.taskName = "end"

		param := &EndTask{}
		param.taskName = "param"

		task := &SleepTask{
			stepTask: stepTask{
				Next: end,
			},
			ExecutorFn: statusTestDurationExecutor,
			Params:     []Task{param},
		}
		task.taskName = "sleep_task"

		registry := map[string]*Executor{
			"statusTestDurationExecutor": {
				Name: "statusTestDurationExecutor",
				Func: statusTestDurationExecutor,
			},
		}
		taskInfo := SerializeTask(task, registry)

		require.Equal(t, "sleep_task", taskInfo.Name)
		require.Equal(t, string(TaskTypeSleepTask), taskInfo.Type)
		require.Equal(t, "statusTestDurationExecutor", taskInfo.Executor)
		require.Equal(t, "end", taskInfo.Next)
		require.Equal(t, []string{"param"}, taskInfo.Params)
	})

	t.Run("serialize AsyncTask", func(t *testing.T) {
		end := &EndTask{}
		end.taskName = "end"

		fail := &EndTask{}
		fail.taskName = "fail"

		param := &EndTask{}
		param.taskName = "param"

		task := &AsyncTask{
			stepTask: stepTask{
				Next: end,
				Fail: fail,
			},
			ExecutionFn:       statusTestAsyncExecutor,
			ResultProcessorFn: statusTestAsyncResultProcessor,
			Params:            []Task{param},
		}
		task.taskName = "async_task"

		registry := map[string]*Executor{
			"statusTestAsyncExecutor": {
				Name: "statusTestAsyncExecutor",
				Func: statusTestAsyncExecutor,
			},
			"statusTestAsyncResultProcessor": {
				Name: "statusTestAsyncResultProcessor",
				Func: statusTestAsyncResultProcessor,
			},
		}
		taskInfo := SerializeTask(task, registry)

		require.Equal(t, "async_task", taskInfo.Name)
		require.Equal(t, string(TaskTypeAsyncTask), taskInfo.Type)
		require.Equal(t, "statusTestAsyncExecutor", taskInfo.Executor)
		require.Equal(t, "statusTestAsyncResultProcessor", taskInfo.ResultProcessor)
		require.Equal(t, "end", taskInfo.Next)
		require.Equal(t, "fail", taskInfo.Fail)
		require.Equal(t, []string{"param"}, taskInfo.Params)
	})

	t.Run("serialize AsyncTask without fail", func(t *testing.T) {
		end := &EndTask{}
		end.taskName = "end"

		task := &AsyncTask{
			stepTask: stepTask{
				Next: end,
			},
			ExecutionFn:       statusTestAsyncExecutor,
			ResultProcessorFn: statusTestAsyncResultProcessor,
		}
		task.taskName = "async_task"

		registry := map[string]*Executor{
			"statusTestAsyncExecutor": {
				Name: "statusTestAsyncExecutor",
				Func: statusTestAsyncExecutor,
			},
			"statusTestAsyncResultProcessor": {
				Name: "statusTestAsyncResultProcessor",
				Func: statusTestAsyncResultProcessor,
			},
		}
		taskInfo := SerializeTask(task, registry)

		require.Equal(t, "async_task", taskInfo.Name)
		require.Equal(t, "end", taskInfo.Next)
		require.Empty(t, taskInfo.Fail)
		require.Equal(t, "statusTestAsyncExecutor", taskInfo.Executor)
		require.Equal(t, "statusTestAsyncResultProcessor", taskInfo.ResultProcessor)
	})

	t.Run("serialize AsyncTask with unknown executors", func(t *testing.T) {
		end := &EndTask{}
		end.taskName = "end"

		task := &AsyncTask{
			stepTask: stepTask{
				Next: end,
			},
			ExecutionFn:       statusTestAsyncExecutor,
			ResultProcessorFn: statusTestAsyncResultProcessor,
		}
		task.taskName = "async_task"

		registry := map[string]*Executor{}
		taskInfo := SerializeTask(task, registry)

		require.Equal(t, "unknown", taskInfo.Executor)
		require.Equal(t, "unknown", taskInfo.ResultProcessor)
	})

	t.Run("serialize task with unknown executor", func(t *testing.T) {
		task := &ExecutionTask{
			ExecutorFn: statusTestExecutor,
		}
		task.taskName = "exec_task"

		registry := map[string]*Executor{}
		taskInfo := SerializeTask(task, registry)

		require.Equal(t, "unknown", taskInfo.Executor)
	})
}

func TestGetTaskNames(t *testing.T) {
	t.Run("nil tasks returns nil", func(t *testing.T) {
		names := getTaskNames(nil)
		require.Nil(t, names)
	})

	t.Run("empty tasks returns nil", func(t *testing.T) {
		names := getTaskNames([]Task{})
		require.Nil(t, names)
	})

	t.Run("single task", func(t *testing.T) {
		task1 := &EndTask{}
		task1.taskName = "task1"

		names := getTaskNames([]Task{task1})
		require.Equal(t, []string{"task1"}, names)
	})

	t.Run("multiple tasks", func(t *testing.T) {
		task1 := &EndTask{}
		task1.taskName = "task1"

		task2 := &EndTask{}
		task2.taskName = "task2"

		task3 := &EndTask{}
		task3.taskName = "task3"

		names := getTaskNames([]Task{task1, task2, task3})
		require.Equal(t, []string{"task1", "task2", "task3"}, names)
	})

	t.Run("handles nil tasks in slice", func(t *testing.T) {
		task1 := &EndTask{}
		task1.taskName = "task1"

		task2 := &EndTask{}
		task2.taskName = "task2"

		names := getTaskNames([]Task{task1, nil, task2})
		require.Equal(t, []string{"task1", "", "task2"}, names)
	})
}
