// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package planners provide status and execution tracking types for the Task
// Execution Framework (TEF). This file defines structures for representing workflow
// execution status, task information, and plan structure serialization. These types
// are used for monitoring and querying workflow state.
package planners

import "time"

// ChildPlanInfo represents details about a specific child plan within a workflow system.
// It includes the plan's name and variant uniquely identifying the child plan.
type ChildPlanInfo struct {
	PlanName           string `json:"child_plan_name,omitempty"`
	PlanVariant        string `json:"child_plan_variant,omitempty"`
	ChildWorkflowURL   string `json:"child_workflow_url,omitempty"`    // API endpoint
	ChildWorkflowUIURL string `json:"child_workflow_ui_url,omitempty"` // UI page URL
}

// TaskInfo represents information about a task in the workflow.
type TaskInfo struct {
	Name            string                 `json:"name"`
	Type            string                 `json:"type"`
	Next            string                 `json:"next,omitempty"`
	Fail            string                 `json:"fail,omitempty"`
	Then            string                 `json:"then,omitempty"`
	Else            string                 `json:"else,omitempty"`
	Executor        string                 `json:"executor,omitempty"`
	ResultProcessor string                 `json:"result_processor,omitempty"`
	Params          []string               `json:"params,omitempty"`
	ForkTasks       []string               `json:"fork_tasks,omitempty"`
	ChildPlanInfo   *ChildPlanInfo         `json:"child_plan_info,omitempty"`
	Input           []interface{}          `json:"input,omitempty"`
	Output          interface{}            `json:"output,omitempty"`
	Error           string                 `json:"error,omitempty"`
	Status          string                 `json:"status,omitempty"` // Pending, InProgress, Completed, Failed
	StartTime       *time.Time             `json:"start_time,omitempty"`
	EndTime         *time.Time             `json:"end_time,omitempty"`
	Properties      map[string]interface{} `json:"properties,omitempty"`
}

// WorkflowInfo represents the complete workflow definition and state.
type WorkflowInfo struct {
	Name            string              `json:"name"`
	Description     string              `json:"description"`
	WorkflowVersion int                 `json:"workflow_version"`
	FirstTask       string              `json:"first_task"`
	OutputTask      string              `json:"output_task"`
	Tasks           map[string]TaskInfo `json:"tasks"`
	Input           []interface{}       `json:"input,omitempty"`
	Output          interface{}         `json:"output,omitempty"`
}

// ExecutionStatus represents the status of a workflow execution.
type ExecutionStatus struct {
	WorkflowID          string        `json:"workflow_id"`
	Status              string        `json:"status"` // Running, Completed, Failed, etc.
	CurrentTasks        []string      `json:"current_tasks,omitempty"`
	Workflow            *WorkflowInfo `json:"workflow,omitempty"`
	ParentWorkflowURL   string        `json:"parent_workflow_url,omitempty"`    // API endpoint
	ParentWorkflowUIURL string        `json:"parent_workflow_ui_url,omitempty"` // UI page URL
}

// WorkflowExecutionInfo represents summary information about a workflow execution.
type WorkflowExecutionInfo struct {
	WorkflowID string     `json:"workflow_id"`
	RunID      string     `json:"run_id"`
	Status     string     `json:"status"`
	StartTime  *time.Time `json:"start_time,omitempty"`
	EndTime    *time.Time `json:"end_time,omitempty"`
}

// ActivityResult holds the execution result of a task/activity.
type ActivityResult struct {
	Status    string // Pending, InProgress, Completed, Failed
	Input     []interface{}
	Output    interface{}
	Error     error
	StartTime *time.Time
	EndTime   *time.Time
}

// SerializeTask converts a Task into a TaskInfo structure for JSON serialization.
// This function extracts task metadata (name, type, executor, parameters) and
// task-specific fields based on the task type. The resulting TaskInfo can be
// included in workflow status responses or stored in workflow metadata.
func SerializeTask(task Task, executorRegistry map[string]*Executor) TaskInfo {
	taskInfo := TaskInfo{
		Name: task.Name(),
		Type: string(task.Type()),
	}

	// Common fields for step tasks (ExecutionTask, ForkTask, ChildPlanTask)
	if task.isStepTask() {
		if next := task.getNextTask(); next != nil {
			taskInfo.Next = next.Name()
		}
		if fail := task.getFailTask(); fail != nil {
			taskInfo.Fail = fail.Name()
		}
	}

	// Type-specific fields
	switch task.Type() {
	case TaskTypeExecution:
		execTask := task.(*ExecutionTask)
		taskInfo.Executor = execTask.GetExecutorName(executorRegistry)
		taskInfo.Params = getTaskNames(execTask.Params)

	case TaskTypeFork:
		forkTask := task.(*ForkTask)
		taskInfo.ForkTasks = getTaskNames(forkTask.Tasks)

	case TaskTypeConditionTask:
		conditionTask := task.(*ConditionTask)
		taskInfo.Executor = conditionTask.GetExecutorName(executorRegistry)
		taskInfo.Params = getTaskNames(conditionTask.Params)
		if conditionTask.Then != nil {
			taskInfo.Then = conditionTask.Then.Name()
		}
		if conditionTask.Else != nil {
			taskInfo.Else = conditionTask.Else.Name()
		}

	case TaskTypeAsyncTask:
		asyncTask := task.(*AsyncTask)
		taskInfo.Executor = asyncTask.GetExecutionFnName(executorRegistry)
		taskInfo.ResultProcessor = asyncTask.GetResultProcessorFnName(executorRegistry)
		taskInfo.Params = getTaskNames(asyncTask.Params)

	case TaskTypeChildPlanTask:
		childTask := task.(*ChildPlanTask)
		taskInfo.ChildPlanInfo = &ChildPlanInfo{
			PlanName: childTask.PlanName,
		}
		taskInfo.Executor = childTask.GetExecutorName(executorRegistry)
		taskInfo.Params = getTaskNames(childTask.Params)
	}

	return taskInfo
}

// getTaskNames extracts task names from a slice of tasks.
// Returns nil if the input slice is empty. This is used to serialize
// task parameter lists and fork task branches.
func getTaskNames(tasks []Task) []string {
	if len(tasks) == 0 {
		return nil
	}

	names := make([]string, len(tasks))
	for i, task := range tasks {
		if task != nil {
			names[i] = task.Name()
		}
	}
	return names
}

// SerializePlanStructure converts a BasePlanner into a WorkflowInfo structure
// containing only the static plan definition (no runtime data).
// This structure can be stored in the workflow store and used to reconstruct
// the plan definition when querying workflow status.
func SerializePlanStructure(basePlanner *BasePlanner) *WorkflowInfo {
	if basePlanner == nil {
		return nil
	}

	workflowInfo := &WorkflowInfo{
		Name:            basePlanner.Name,
		Description:     basePlanner.Description,
		WorkflowVersion: basePlanner.WorkflowVersion,
		Tasks:           make(map[string]TaskInfo),
	}

	// Set first and output task names
	if basePlanner.First != nil {
		workflowInfo.FirstTask = basePlanner.First.Name()
	}
	if basePlanner.Output != nil {
		workflowInfo.OutputTask = basePlanner.Output.Name()
	}

	// Serialize all tasks in the registry (without runtime data)
	for taskName, task := range basePlanner.TasksRegistry {
		workflowInfo.Tasks[taskName] = SerializeTask(task, basePlanner.ExecutorRegistry)
	}

	return workflowInfo
}
