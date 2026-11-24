// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package planners define the core interfaces and types for the Task Execution
// Framework (TEF). This file contains the foundational interface definitions for
// planners, executors, registries, and plan execution services. These interfaces
// are framework-agnostic and can be implemented by different orchestration engines.
package planners

import (
	"context"
	"time"

	"github.com/spf13/cobra"
)

// Planner provides the interface for building and registering task execution plans.
// Plan authors use this interface to create tasks, register executors, and define
// the execution flow during plan generation. All task creation methods return
// task-specific builders that allow chaining configuration before adding to the graph.
type Planner interface {
	// RegisterExecutor adds an executor to the plan's registry for use by tasks.
	RegisterExecutor(ctx context.Context, executor *Executor)
	// RegisterPlan sets the first task and output task that define the plan's execution boundaries.
	// The first task is where execution begins, and the output task is where results are collected.
	RegisterPlan(ctx context.Context, first, output Task)

	// NewExecutionTask creates a new task that executes a registered executor.
	NewExecutionTask(ctx context.Context, name string) *ExecutionTask
	// NewForkTask creates a new task that executes multiple branches in parallel.
	NewForkTask(ctx context.Context, name string) *ForkTask
	// NewForkJoinTask creates a synchronization point where parallel fork branches converge.
	NewForkJoinTask(ctx context.Context, name string) *ForkJoinTask
	// NewConditionTask creates a new task that branches conditionally based on executor results.
	NewConditionTask(ctx context.Context, name string) *ConditionTask
	// NewCallbackTask creates a new task that executes and waits for callback-driven completion.
	NewCallbackTask(ctx context.Context, name string) *CallbackTask
	// NewChildPlanTask creates a new task that executes a child plan synchronously.
	NewChildPlanTask(ctx context.Context, name string) *ChildPlanTask
	// NewEndTask creates a new task that marks the termination of an execution path.
	NewEndTask(ctx context.Context, name string) *EndTask
}

// Registry defines the interface that plan implementations must satisfy.
// Each plan registers itself by implementing this interface, which provides
// plan metadata, generates the task graph, and handles input parsing.
type Registry interface {
	// PrepareExecution sets up the necessary resources or state that a plan requires for execution.
	// It is invoked during the plan's registration lifecycle.
	PrepareExecution(ctx context.Context) error
	// GetPlanName returns the unique identifier for this plan.
	GetPlanName() string
	// GetPlanDescription returns a human-readable description of what this plan does.
	GetPlanDescription() string
	// GetPlanVersion returns the current version of the workflow definition.
	// Increment this when making backward incompatible changes to the workflow.
	// The default version is 1. Use workflow versioning to ensure old running workflows
	// continue with their original logic while new workflows use updated logic.
	GetPlanVersion() int
	// GeneratePlan constructs the task execution graph using the provided planner.
	// This method is called during plan registration to build the complete workflow.
	GeneratePlan(ctx context.Context, p Planner)
	// ParsePlanInput parses and validates the user-provided input string into the
	// plan's expected input type. The returned interface{} should match the input
	// type expected by the plan's executors.
	ParsePlanInput(input string) (interface{}, error)
	// AddStartWorkerCmdFlags allows plans to add custom flags to worker commands.
	// This method is called during worker command setup to allow plan-specific configuration.
	AddStartWorkerCmdFlags(cmd *cobra.Command)
}

// PlanExecutor defines the interface for plan-specific execution operations.
// These operations require plan-specific knowledge (task graph, executors, etc.)
// and must be implemented by plan-specific manager instances.
type PlanExecutor interface {
	// StartWorker initializes and starts the worker that processes plan executions.
	// The plan variant is part of the planID and is used for worker disambiguation.
	StartWorker(ctx context.Context, planID string) error
	// ExecutePlan executes the plan identified by planID with the given input and returns a workflow identifier.
	ExecutePlan(ctx context.Context, input interface{}, planID string) (string, error)
}

// PlanMetadata contains metadata about a plan discovered from the execution framework.
type PlanMetadata struct {
	// PlanID is the unique identifier for the plan instance (e.g., "tef_plan_my-plan_variant").
	PlanID string
	// Description is the human-readable description of what this plan does.
	// This may be empty if the plan was started without description metadata.
	Description string
}

// SharedPlanService defines the interface for plan-agnostic operations that work across all plans.
// These operations don't require plan-specific knowledge and can be implemented by a shared
// service instance. This includes both query operations (status, list) and control operations
// (resume) that operate at the framework level rather than the plan level.
type SharedPlanService interface {
	// GetExecutionStatus queries the execution status of a specific workflow execution.
	GetExecutionStatus(ctx context.Context, planID, workflowID string) (*ExecutionStatus, error)
	// ListExecutions retrieves a list of workflow execution summaries for a specified planID.
	ListExecutions(ctx context.Context, planID string) ([]*WorkflowExecutionInfo, error)
	// ListAllPlanIDs queries the execution framework for all active plan instances and their metadata.
	ListAllPlanIDs(ctx context.Context) ([]PlanMetadata, error)
	// ResumeTask resumes a callback task with the provided result.
	// It signals the workflow identified by planID and workflowID with the result for the specified stepID.
	// This is a framework-level operation that doesn't require plan-specific knowledge.
	ResumeTask(ctx context.Context, planID, workflowID, stepID, result string) error
	// AddPlannerFlags allows planner implementations to add framework-specific flags to commands.
	// This method is called by the API server CLI and Start Worker CLI that needs to connect to the Planner
	AddPlannerFlags(cmd *cobra.Command)
	// ClonePropertiesFrom copies planner-specific configuration properties from another manager.
	// This is used to ensure child plan managers inherit the same connection configuration as the parent plan manager.
	ClonePropertiesFrom(source PlannerManager)
}

// PlannerManager defines the combined interface for managing plan execution and shared operations.
// Implementations should satisfy both PlanExecutor and SharedPlanService interfaces.
// This interface is maintained for backward compatibility, but new code should use
// PlanExecutor and SharedPlanService directly.
type PlannerManager interface {
	PlanExecutor
	SharedPlanService
}

// PlanExecutionInfo contains metadata about the execution of a specific plan within a workflow.
// It creates a chain of execution context, allowing correlation of parent-child plan relationships.
type PlanExecutionInfo struct {
	// WorkflowID is the unique identifier of the associated workflow.
	WorkflowID string
	// PlanID is the unique identifier for the executed plan.
	PlanID string
	// PlanVariant is the specific variant of the plan being executed.
	PlanVariant string
	// WorkflowVersion is the version of the workflow definition being executed.
	WorkflowVersion int
	// ParentTaskName is the name of the task in the parent plan that invoked this child plan.
	ParentTaskName string
	// ParentInfo is the execution info of the parent plan that triggered this plan (for child workflows).
	ParentInfo *PlanExecutionInfo
}

// RetryConfig defines the retry policy configuration for an executor.
// This is a framework-agnostic representation that can be converted to
// framework-specific retry policies (e.g., Temporal's RetryPolicy).
type RetryConfig struct {
	// MaximumAttempts is the maximum number of execution attempts.
	// If set to 0, the executor will use the default retry policy.
	// If set to 1, the executor will not retry on failure.
	MaximumAttempts int32
	// InitialInterval is the backoff interval for the first retry.
	// If not specified, a default interval will be used.
	InitialInterval time.Duration
	// BackoffCoefficient is the rate of increase for the backoff interval.
	// The backoff interval for each retry is: InitialInterval * BackoffCoefficient^(attempt-1)
	BackoffCoefficient float64
	// MaximumInterval is the maximum backoff interval.
	MaximumInterval time.Duration
	// NonRetryableErrorTypes is a list of error type names that should not be retried.
	// The framework will stop retrying if the error type matches this list.
	// Note:
	//  - cancellation is not a failure, so it won't be retried,
	//  - only StartToClose or Heartbeat timeouts are retryable.
	NonRetryableErrorTypes []string
}

// Executor defines the metadata and implementation for a callable function that performs
// a specific task within a plan. Executors are registered with a planner and can be invoked
// by ExecutionTasks or ConditionTasks.
type Executor struct {
	// Name uniquely identifies the executor within a plan.
	Name string
	// Description provides human-readable documentation for the executor's purpose.
	Description string
	// Func is the actual function to execute. All executors must have at least 3 parameters:
	// (context.Context, *PlanExecutionInfo, input interface{}). Additional parameters can be
	// provided via the task's Params field. Return type depends on the task type:
	// - ExecutionTask: func(ctx context.Context, info *PlanExecutionInfo, input interface{}, ...params) (interface{}, error)
	// - ConditionTask: func(ctx context.Context, info *PlanExecutionInfo, input interface{}, ...params) (bool, error)
	// - CallbackTask.ExecutionFn: func(ctx context.Context, info *PlanExecutionInfo, input interface{}, ...params) (string, error)
	// - ChildPlanTask.ChildTaskInfoFn: func(ctx context.Context, info *PlanExecutionInfo, input interface{}, ...params) (ChildTaskInfo, error)
	Func interface{}
	// Idempotent indicates whether the executor can be safely retried without side effects.
	Idempotent bool
	// Deprecated marks the executor as obsolete and discourages its use.
	Deprecated bool
	// RetryConfig specifies the retry policy for this executor.
	// If nil, a default retry policy will be used (MaximumAttempts: 1, no retries).
	RetryConfig *RetryConfig
}
