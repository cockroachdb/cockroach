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

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

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
		require.Empty(t, taskInfo.ExecutorFunc)
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
		require.Equal(t, "statusTestExecutor", taskInfo.ExecutorFunc)
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

	t.Run("serialize ConditionTask", func(t *testing.T) {
		thenTask := &EndTask{}
		thenTask.taskName = "then"

		elseTask := &EndTask{}
		elseTask.taskName = "else"

		param := &EndTask{}
		param.taskName = "param"

		task := &ConditionTask{
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
		require.Equal(t, string(TaskTypeConditionTask), taskInfo.Type)
		require.Equal(t, "statusTestBoolExecutor", taskInfo.ExecutorFunc)
		require.Equal(t, "then", taskInfo.Then)
		require.Equal(t, "else", taskInfo.Else)
		require.Equal(t, []string{"param"}, taskInfo.Params)
	})

	t.Run("serialize CallbackTask", func(t *testing.T) {
		end := &EndTask{}
		end.taskName = "end"

		fail := &EndTask{}
		fail.taskName = "fail"

		param := &EndTask{}
		param.taskName = "param"

		task := &CallbackTask{
			stepTask: stepTask{
				Next: end,
				Fail: fail,
			},
			ExecutionFn:       statusTestAsyncExecutor,
			ResultProcessorFn: statusTestAsyncResultProcessor,
			Params:            []Task{param},
		}
		task.taskName = "callback_task"

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

		require.Equal(t, "callback_task", taskInfo.Name)
		require.Equal(t, string(TaskTypeCallbackTask), taskInfo.Type)
		require.Equal(t, "statusTestAsyncExecutor", taskInfo.ExecutorFunc)
		require.Equal(t, "statusTestAsyncResultProcessor", taskInfo.ResultProcessor)
		require.Equal(t, "end", taskInfo.Next)
		require.Equal(t, "fail", taskInfo.Fail)
		require.Equal(t, []string{"param"}, taskInfo.Params)
	})

	t.Run("serialize CallbackTask without fail", func(t *testing.T) {
		end := &EndTask{}
		end.taskName = "end"

		task := &CallbackTask{
			stepTask: stepTask{
				Next: end,
			},
			ExecutionFn:       statusTestAsyncExecutor,
			ResultProcessorFn: statusTestAsyncResultProcessor,
		}
		task.taskName = "callback_task"

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

		require.Equal(t, "callback_task", taskInfo.Name)
		require.Equal(t, "end", taskInfo.Next)
		require.Empty(t, taskInfo.Fail)
		require.Equal(t, "statusTestAsyncExecutor", taskInfo.ExecutorFunc)
		require.Equal(t, "statusTestAsyncResultProcessor", taskInfo.ResultProcessor)
	})

	t.Run("serialize CallbackTask with unknown executors", func(t *testing.T) {
		end := &EndTask{}
		end.taskName = "end"

		task := &CallbackTask{
			stepTask: stepTask{
				Next: end,
			},
			ExecutionFn:       statusTestAsyncExecutor,
			ResultProcessorFn: statusTestAsyncResultProcessor,
		}
		task.taskName = "callback_task"

		registry := map[string]*Executor{}
		taskInfo := SerializeTask(task, registry)

		require.Equal(t, "unknown", taskInfo.ExecutorFunc)
		require.Equal(t, "unknown", taskInfo.ResultProcessor)
	})

	t.Run("serialize task with unknown executor", func(t *testing.T) {
		task := &ExecutionTask{
			ExecutorFn: statusTestExecutor,
		}
		task.taskName = "exec_task"

		registry := map[string]*Executor{}
		taskInfo := SerializeTask(task, registry)

		require.Equal(t, "unknown", taskInfo.ExecutorFunc)
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

func TestSerializePlanStructure(t *testing.T) {
	ctx := context.Background()

	t.Run("returns nil for nil planner", func(t *testing.T) {
		workflowInfo := SerializePlanStructure(nil)
		require.Nil(t, workflowInfo)
	})

	t.Run("serializes basic planner structure", func(t *testing.T) {
		planner := &BasePlanner{
			Name:            "test_plan",
			Description:     "Test plan description",
			WorkflowVersion: 1,
			TasksRegistry:   make(map[string]Task),
			ExecutorRegistry: map[string]*Executor{
				"testExecutor": {
					Name: "testExecutor",
					Func: statusTestExecutor,
				},
			},
		}

		end := &EndTask{}
		end.taskName = "end"
		planner.TasksRegistry["end"] = end

		exec := &ExecutionTask{
			stepTask: stepTask{
				Next: end,
			},
			ExecutorFn: statusTestExecutor,
		}
		exec.taskName = "start"
		planner.TasksRegistry["start"] = exec

		planner.First = exec
		planner.Output = end

		workflowInfo := SerializePlanStructure(planner)
		require.NotNil(t, workflowInfo)
		require.Equal(t, "test_plan", workflowInfo.Name)
		require.Equal(t, "Test plan description", workflowInfo.Description)
		require.Equal(t, 1, workflowInfo.WorkflowVersion)
		require.Equal(t, "start", workflowInfo.FirstTask)
		require.Equal(t, "end", workflowInfo.OutputTask)
		require.Len(t, workflowInfo.Tasks, 2)
		require.Contains(t, workflowInfo.Tasks, "start")
		require.Contains(t, workflowInfo.Tasks, "end")
	})

	t.Run("serializes planner without output task", func(t *testing.T) {
		planner := &BasePlanner{
			Name:             "test_plan",
			Description:      "Test plan",
			WorkflowVersion:  1,
			TasksRegistry:    make(map[string]Task),
			ExecutorRegistry: make(map[string]*Executor),
		}

		end := &EndTask{}
		end.taskName = "end"
		planner.TasksRegistry["end"] = end
		planner.First = end

		workflowInfo := SerializePlanStructure(planner)
		require.NotNil(t, workflowInfo)
		require.Equal(t, "end", workflowInfo.FirstTask)
		require.Empty(t, workflowInfo.OutputTask)
	})

	t.Run("serializes planner with complex task graph", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockReg := NewMockRegistry(ctrl)
		mockReg.EXPECT().GetPlanName().Return("complex_plan").AnyTimes()
		mockReg.EXPECT().GetPlanDescription().Return("Complex test plan").AnyTimes()
		mockReg.EXPECT().GetPlanVersion().Return(1).AnyTimes()
		mockReg.EXPECT().PrepareExecution(gomock.Any()).Return(nil).AnyTimes()
		mockReg.EXPECT().ParsePlanInput(gomock.Any()).DoAndReturn(func(input string) (interface{}, error) {
			return input, nil
		}).AnyTimes()
		mockReg.EXPECT().GeneratePlan(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, p Planner) {
			executor1 := &Executor{Name: "exec1", Func: statusTestExecutor}
			p.RegisterExecutor(ctx, executor1)

			end := p.NewEndTask(ctx, "end")

			exec1 := p.NewExecutionTask(ctx, "exec1")
			exec1.ExecutorFn = statusTestExecutor
			exec1.Next = end

			exec2 := p.NewExecutionTask(ctx, "exec2")
			exec2.ExecutorFn = statusTestExecutor
			exec2.Next = end

			p.RegisterPlan(ctx, exec1, end)
		}).AnyTimes()

		planner, err := NewBasePlanner(ctx, mockReg)
		require.NoError(t, err)

		workflowInfo := SerializePlanStructure(planner)
		require.NotNil(t, workflowInfo)
		require.Equal(t, "complex_plan", workflowInfo.Name)
		require.Equal(t, "Complex test plan", workflowInfo.Description)
		require.Len(t, workflowInfo.Tasks, 3)
	})
}

func TestSerializeTaskAdditional(t *testing.T) {
	t.Run("serialize ForkJoinTask", func(t *testing.T) {
		task := &ForkJoinTask{}
		task.taskName = "join_task"

		registry := map[string]*Executor{}
		taskInfo := SerializeTask(task, registry)

		require.Equal(t, "join_task", taskInfo.Name)
		require.Equal(t, string(TaskTypeForkJoinTask), taskInfo.Type)
		require.Empty(t, taskInfo.Next)
		require.Empty(t, taskInfo.Fail)
	})

	t.Run("serialize ForkTask with join", func(t *testing.T) {
		end := &EndTask{}
		end.taskName = "end"

		join := &ForkJoinTask{}
		join.taskName = "join"

		branch1 := &EndTask{}
		branch1.taskName = "branch1"

		task := &ForkTask{
			stepTask: stepTask{
				Next: end,
			},
			Tasks: []Task{branch1},
			Join:  join,
		}
		task.taskName = "fork_task"

		registry := map[string]*Executor{}
		taskInfo := SerializeTask(task, registry)

		require.Equal(t, "fork_task", taskInfo.Name)
		require.Equal(t, string(TaskTypeFork), taskInfo.Type)
		require.Equal(t, "join", taskInfo.Join)
		require.Equal(t, []string{"branch1"}, taskInfo.ForkTasks)
	})

	t.Run("serialize ExecutionTask with params", func(t *testing.T) {
		end := &EndTask{}
		end.taskName = "end"

		param1 := &EndTask{}
		param1.taskName = "param1"

		task := &ExecutionTask{
			stepTask: stepTask{
				Next: end,
			},
			ExecutorFn: statusTestExecutor,
			Params:     []Task{param1},
		}
		task.taskName = "exec_task"

		registry := map[string]*Executor{
			"statusTestExecutor": {
				Name: "statusTestExecutor",
				Func: statusTestExecutor,
			},
		}
		taskInfo := SerializeTask(task, registry)

		require.Equal(t, []string{"param1"}, taskInfo.Params)
	})

	t.Run("serialize ExecutionTask without params", func(t *testing.T) {
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

		require.Nil(t, taskInfo.Params)
	})

	t.Run("serialize ConditionTask without params", func(t *testing.T) {
		thenTask := &EndTask{}
		thenTask.taskName = "then"

		elseTask := &EndTask{}
		elseTask.taskName = "else"

		task := &ConditionTask{
			ExecutorFn: statusTestBoolExecutor,
			Then:       thenTask,
			Else:       elseTask,
		}
		task.taskName = "if_task"

		registry := map[string]*Executor{
			"statusTestBoolExecutor": {
				Name: "statusTestBoolExecutor",
				Func: statusTestBoolExecutor,
			},
		}
		taskInfo := SerializeTask(task, registry)

		require.Nil(t, taskInfo.Params)
	})

	t.Run("serialize ChildPlanTask", func(t *testing.T) {
		end := &EndTask{}
		end.taskName = "end"

		childTaskInfoFn := func(_ context.Context, _ *PlanExecutionInfo, _ string) (ChildTaskInfo, error) {
			return ChildTaskInfo{PlanVariant: "variant1", Input: "input"}, nil
		}

		task := &ChildPlanTask{
			stepTask: stepTask{
				Next: end,
			},
			PlanName:        "child_plan",
			ChildTaskInfoFn: childTaskInfoFn,
		}
		task.taskName = "child_task"

		registry := map[string]*Executor{
			"childTaskInfoFn": {
				Name: "childTaskInfoFn",
				Func: childTaskInfoFn,
			},
		}
		taskInfo := SerializeTask(task, registry)

		require.Equal(t, "child_task", taskInfo.Name)
		require.Equal(t, string(TaskTypeChildPlanTask), taskInfo.Type)
		require.NotNil(t, taskInfo.ChildPlanInfo)
		require.Equal(t, "child_plan", taskInfo.ChildPlanInfo.PlanName)
	})
}

/********************* Mocks beyond this point *********************/

// Test executor for status tests
func statusTestExecutor(_ context.Context, _ *PlanExecutionInfo, _ string) (interface{}, error) {
	return nil, nil
}

func statusTestBoolExecutor(_ context.Context, _ *PlanExecutionInfo) (bool, error) {
	return true, nil
}

func statusTestAsyncExecutor(_ context.Context, _ *PlanExecutionInfo) (string, error) {
	return "step-id", nil
}

func statusTestAsyncResultProcessor(
	_ context.Context, _ *PlanExecutionInfo, _ interface{}, _ string, _ string,
) (interface{}, error) {
	return "processed", nil
}
