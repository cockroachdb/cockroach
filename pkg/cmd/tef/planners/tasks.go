// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package planners provides task type definitions for the Task Execution Framework (TEF).
// This file defines all task types (ExecutionTask, ForkTask, ConditionTask, CallbackTask,
// ChildPlanTask, EndTask) and their validation logic. Tasks form the building blocks
// of execution plans and represent individual steps in a plan execution.
package planners

import (
	"context"
	"reflect"

	"github.com/cockroachdb/errors"
)

// TaskType identifies the kind of task in an execution plan.
type TaskType string

const (
	// TaskTypeExecution represents a task that executes a specific function.
	TaskTypeExecution TaskType = "execution task"
	// TaskTypeFork represents a task that executes multiple branches in parallel.
	TaskTypeFork TaskType = "fork task"
	// TaskTypeForkJoinTask represents a synchronization point where parallel fork branches converge.
	TaskTypeForkJoinTask TaskType = "fork join task"
	// TaskTypeEndTask represents a task that marks the termination of an execution path.
	TaskTypeEndTask TaskType = "end task"
	// TaskTypeConditionTask represents a task that branches conditionally based on a boolean result.
	TaskTypeConditionTask TaskType = "condition task"
	// TaskTypeCallbackTask represents a task that initiates an operation and waits for callback-driven completion.
	TaskTypeCallbackTask TaskType = "callback task"
	// TaskTypeChildPlanTask represents a task that executes a child plan synchronously.
	TaskTypeChildPlanTask TaskType = "child task"
)

// Task defines the interface that all task types must implement.
// Tasks represent individual steps in an execution plan and form a directed graph.
type Task interface {
	// Name returns the unique identifier for this task within its plan.
	Name() string
	// Type returns the task type, indicating its execution behavior.
	Type() TaskType

	// validate checks that the task is properly configured.
	// The failedPath parameter indicates whether this task is reachable via a Fail path,
	// which affects parameter validation (tasks in Fail paths receive an extra error string).
	validate(failedPath bool) error
	// isStepTask returns true if the task has Next and Fail paths.
	isStepTask() bool
	// getNextTask returns the next task to execute on success (nil if not a step task).
	getNextTask() Task
	// getFailTask returns the task to execute on failure (nil if not a step task or no fail path).
	getFailTask() Task
}

// baseTask provides the foundational fields and methods shared by all task types.
// It implements the Name() and isStepTask() methods of the Task interface.
type baseTask struct {
	taskName string
}

// Name returns the task's unique identifier.
func (b *baseTask) Name() string {
	return b.taskName
}

// isStepTask returns false for baseTask, as it does not have Next/Fail paths.
// This method is overridden by stepTask to return true.
func (b *baseTask) isStepTask() bool {
	return false
}

// getNextTask returns nil for baseTask, as it has no next task.
func (b *baseTask) getNextTask() Task {
	return nil
}

// getFailTask returns nil for baseTask, as it has no fail task.
func (b *baseTask) getFailTask() Task {
	return nil
}

// stepTask extends baseTask with Next and Fail paths for sequential execution.
// Most task types embed stepTask to support success and failure paths.
type stepTask struct {
	baseTask
	// Next is the task to execute upon successful completion.
	Next Task
	// Fail is the task to execute if this task fails (optional).
	Fail Task
}

// The stepTask type partially implements the Task interface.
var _ Task = &stepTask{}

// Type returns an empty string as stepTask is abstract and should not be instantiated directly.
func (s *stepTask) Type() TaskType {
	return ""
}

// validate checks that the task has a name and a Next task defined.
func (s *stepTask) validate(_ bool) error {
	if s.taskName == "" {
		return errors.Newf("task name is missing")
	}
	if s.Next == nil {
		return errors.Newf("next task is missing for task <%s>", s.taskName)
	}
	return nil
}

// isStepTask returns true, indicating this task has Next and Fail paths.
func (s *stepTask) isStepTask() bool {
	return true
}

// getNextTask returns the next task to execute on success.
func (s *stepTask) getNextTask() Task {
	return s.Next
}

// getFailTask returns the task to execute on failure, which may be nil.
func (s *stepTask) getFailTask() Task {
	return s.Fail
}

// NextTask returns the task to execute on successful completion.
// This is a public accessor for the Next field.
func (s *stepTask) NextTask() Task {
	return s.Next
}

// FailTask returns the task to execute on failure.
// Returns nil if no failure handler is defined.
// This is a public accessor for the Fail field.
func (s *stepTask) FailTask() Task {
	return s.Fail
}

// ExecutionTask executes a registered executor function with optional parameters.
// It embeds stepTask to support Next and Fail paths based on execution results.
type ExecutionTask struct {
	stepTask
	// ExecutorFn is the function to execute, which must be registered with the planner.
	ExecutorFn interface{}
	// Params contains tasks whose results are passed as additional inputs to the executor.
	Params []Task
}

// GetExecutorName returns the Go function name (e.g., "validateCluster") for this execution task's executor.
// Note: This returns the reflection-based function name, not the registered executor name (e.g., "validate-cluster").
// The executor is looked up in the provided registry by comparing function pointers.
func (e *ExecutionTask) GetExecutorName(executorRegistry map[string]*Executor) string {
	return getExecutorName(e.ExecutorFn, executorRegistry)
}

// Type returns TaskTypeExecution, identifying this as an execution task.
func (e *ExecutionTask) Type() TaskType {
	return TaskTypeExecution
}

// validate ensures the task has a name and an executor function.
// When failedPath is true, the executor must accept one additional string parameter (error message).
func (e *ExecutionTask) validate(failedPath bool) error {
	if e.taskName == "" {
		return errors.Newf("task name is missing")
	}
	if e.ExecutorFn == nil {
		return errors.Newf("executor is missing for execution task <%s>", e.taskName)
	}

	// Validate that ExecutorFn has the correct parameter signature
	fnType := reflect.TypeOf(e.ExecutorFn)
	if fnType.Kind() != reflect.Func {
		return errors.Newf("executor for execution task <%s> must be a function", e.taskName)
	}
	if fnType.NumIn() < 3 {
		return errors.Newf("executor for execution task <%s> must take at least 3 parameters (context.Context, *PlanExecutionInfo, input interface{}), got %d parameters", e.taskName, fnType.NumIn())
	}

	// Validate first parameter is context.Context
	contextType := reflect.TypeOf((*context.Context)(nil)).Elem()
	if !fnType.In(0).Implements(contextType) {
		return errors.Newf("executor for execution task <%s> must have context.Context as first parameter, got %s", e.taskName, fnType.In(0))
	}

	// Validate second parameter is *PlanExecutionInfo
	planExecutionInfoType := reflect.TypeOf(&PlanExecutionInfo{})
	if fnType.In(1) != planExecutionInfoType {
		return errors.Newf("executor for execution task <%s> must have *PlanExecutionInfo as second parameter, got %s", e.taskName, fnType.In(1))
	}

	// Validate that the number of Params matches the number of additional parameters beyond the 3 required ones.
	// If in a failure path, the executor receives one extra parameter (error string) after the params.
	// Normal path: func(ctx, planInfo, input, ...params) -> expectedParams = NumIn() - 3
	// Failure path: func(ctx, planInfo, input, ...params, errorString) -> expectedParams = NumIn() - 4
	var expectedParams int
	if failedPath {
		// In failure path: func(ctx, planInfo, input, ...params, errorString)
		expectedParams = fnType.NumIn() - 4 // 3 base params + 1 error string
	} else {
		// In normal path: func(ctx, planInfo, input, ...params)
		expectedParams = fnType.NumIn() - 3 // 3 base params
	}
	if len(e.Params) != expectedParams {
		if failedPath {
			return errors.Newf("executor for execution task <%s> in failure path expects %d Param(s), got %d (signature should be: func(ctx, planInfo, input, ...%d params, errorString))", e.taskName, expectedParams, len(e.Params), expectedParams)
		}
		return errors.Newf("executor for execution task <%s> expects %d Param(s), got %d (signature should be: func(ctx, planInfo, input, ...%d params))", e.taskName, expectedParams, len(e.Params), expectedParams)
	}

	// Validate return signature: (interface{}, error)
	if fnType.NumOut() != 2 {
		return errors.Newf("executor for execution task <%s> must return exactly 2 values (interface{}, error), got %d return values", e.taskName, fnType.NumOut())
	}
	errorInterface := reflect.TypeOf((*error)(nil)).Elem()
	if !fnType.Out(1).Implements(errorInterface) {
		return errors.Newf("executor for execution task <%s> must return error as second return value, got %s", e.taskName, fnType.Out(1))
	}

	return nil
}

// ForkTask executes multiple task branches in parallel.
// It embeds stepTask, and the Next task is executed after all parallel branches complete.
// All parallel branches must converge to the same Join point (a ForkJoinTask) before execution
// continues to Next. The Join ForkJoinTask acts as a synchronization barrier, not as termination
// of the entire execution path.
type ForkTask struct {
	stepTask
	// Tasks contain the list of task branches to execute in parallel.
	Tasks []Task
	// Join specifies the ForkJoinTask where all parallel branches must converge.
	// This ForkJoinTask acts as a synchronization barrier - all branches must reach it
	// before the fork's Next task executes. The Join point does not terminate execution;
	// it merely synchronizes the parallel branches.
	Join *ForkJoinTask
}

// Type returns TaskTypeFork, identifying this as a fork task.
func (f *ForkTask) Type() TaskType {
	return TaskTypeFork
}

// validate ensures the task has a name, a Next task, at least one parallel branch, and a Join point.
func (f *ForkTask) validate(_ bool) error {
	if f.taskName == "" {
		return errors.Newf("task name is missing")
	}
	if f.Next == nil {
		return errors.Newf("next task is missing for task <%s>", f.taskName)
	}
	if len(f.Tasks) == 0 {
		return errors.Newf("tasks is missing for fork task <%s>", f.taskName)
	}
	if f.Join == nil {
		return errors.Newf("join point is missing for fork task <%s>", f.taskName)
	}
	return nil
}

// getExecutorName returns the Go function name for the executor (e.g., "validateCluster"),
// not the registered executor name (e.g., "validate-cluster"). This is used for serialization
// and debugging. The function verifies the executor is registered by comparing function pointers,
// then returns the reflection-based function name via GetFunctionName.
func getExecutorName(executorFn interface{}, executorRegistry map[string]*Executor) string {
	fnPtr := reflect.ValueOf(executorFn).Pointer()
	for _, executor := range executorRegistry {
		if reflect.ValueOf(executor.Func).Pointer() == fnPtr {
			return GetFunctionName(executorFn)
		}
	}
	return "unknown"
}

// ForkJoinTask marks a synchronization point where parallel fork branches converge.
// It acts as a barrier that all parallel branches must reach before the fork's Next task executes.
// Unlike EndTask, ForkJoinTask does not terminate execution - it only synchronizes parallel branches.
type ForkJoinTask struct {
	baseTask
}

// Type returns TaskTypeForkJoinTask, identifying this as a fork join task.
func (f *ForkJoinTask) Type() TaskType {
	return TaskTypeForkJoinTask
}

// validate ensures the task has a name.
func (f *ForkJoinTask) validate(_ bool) error {
	if f.taskName == "" {
		return errors.Newf("task name is missing")
	}
	return nil
}

// EndTask marks the termination of an execution path.
// It embeds baseTask and has no Next or Fail paths.
type EndTask struct {
	baseTask
}

// Type returns TaskTypeEndTask, identifying this as an end task.
func (e *EndTask) Type() TaskType {
	return TaskTypeEndTask
}

// validate ensures the task has a name.
func (e *EndTask) validate(_ bool) error {
	if e.taskName == "" {
		return errors.Newf("task name is missing")
	}
	return nil
}

// ConditionTask provides conditional branching based on a boolean executor result.
// It embeds baseTask and branches to Then or Else based on the executor's return value.
type ConditionTask struct {
	baseTask
	// ExecutorFn is the function to execute, which must return a boolean value.
	ExecutorFn interface{}
	// Params contains tasks whose results are passed as additional inputs to the executor.
	Params []Task
	// Then is the task to execute if the executor returns true.
	Then Task
	// Else is the task to execute if the executor returns false.
	Else Task
}

// Type returns TaskTypeConditionTask, identifying this as a condition task.
func (i *ConditionTask) Type() TaskType {
	return TaskTypeConditionTask
}

// validate ensures the task has an executor and both Then and Else branches.
// ConditionTask validation does not differ between normal and failure paths.
func (i *ConditionTask) validate(_ bool) error {
	if i.ExecutorFn == nil {
		return errors.Newf("executor is missing for execution task <%s>", i.taskName)
	}
	// Validate that ExecutorFn has the correct signature: (params...) (bool, error)
	fnType := reflect.TypeOf(i.ExecutorFn)
	if fnType.Kind() != reflect.Func {
		return errors.Newf("executor for condition task <%s> must be a function", i.taskName)
	}
	if fnType.NumIn() < 3 {
		return errors.Newf("executor for condition task <%s> must take at least 3 parameters (context.Context, *PlanExecutionInfo, input interface{}), got %d parameters", i.taskName, fnType.NumIn())
	}

	// Validate first parameter is context.Context
	contextType := reflect.TypeOf((*context.Context)(nil)).Elem()
	if !fnType.In(0).Implements(contextType) {
		return errors.Newf("executor for condition task <%s> must have context.Context as first parameter, got %s", i.taskName, fnType.In(0))
	}

	// Validate second parameter is *PlanExecutionInfo
	planExecutionInfoType := reflect.TypeOf(&PlanExecutionInfo{})
	if fnType.In(1) != planExecutionInfoType {
		return errors.Newf("executor for condition task <%s> must have *PlanExecutionInfo as second parameter, got %s", i.taskName, fnType.In(1))
	}

	// Validate that the number of Params matches the number of additional parameters beyond the 3 required ones.
	// Note: ConditionTask does not receive an error parameter even in failure paths, as it doesn't use hasFailed logic.
	expectedParams := fnType.NumIn() - 3
	if len(i.Params) != expectedParams {
		return errors.Newf("executor for condition task <%s> expects %d additional parameter(s) beyond (context.Context, *PlanExecutionInfo, input), got %d Params", i.taskName, expectedParams, len(i.Params))
	}

	if fnType.NumOut() != 2 {
		return errors.Newf("executor for condition task <%s> must return exactly 2 values (bool, error), got %d return values", i.taskName, fnType.NumOut())
	}
	if fnType.Out(0).Kind() != reflect.Bool {
		return errors.Newf("executor for condition task <%s> must return bool as first return value, got %s", i.taskName, fnType.Out(0).Kind())
	}
	errorInterface := reflect.TypeOf((*error)(nil)).Elem()
	if !fnType.Out(1).Implements(errorInterface) {
		return errors.Newf("executor for condition task <%s> must return error as second return value, got %s", i.taskName, fnType.Out(1))
	}
	if i.Then == nil {
		return errors.Newf("then is missing for execution task <%s>", i.taskName)
	}
	if i.Else == nil {
		return errors.Newf("else is missing for execution task <%s>", i.taskName)
	}
	return nil
}

// GetExecutorName returns the Go function name (e.g., "checkCondition") for this condition task's executor.
// Note: This returns the reflection-based function name, not the registered executor name (e.g., "check-condition").
// The executor is looked up in the provided registry by comparing function pointers.
func (i *ConditionTask) GetExecutorName(executorRegistry map[string]*Executor) string {
	return getExecutorName(i.ExecutorFn, executorRegistry)
}

// CallbackTask initiates an operation and waits for callback-driven completion.
// It embeds stepTask and starts an operation that returns a step ID,
// then waits for an external signal to resume with the result.
type CallbackTask struct {
	stepTask
	// ExecutionFn is the function to execute, which must return a step ID as string.
	// Expected signature: func(ctx context.Context, input...) (string, error)
	ExecutionFn interface{}
	// ResultProcessorFn processes the result when the async operation completes.
	// Expected signature: func(ctx context.Context, input interface{}, stepID string, result string) (interface{}, error)
	ResultProcessorFn interface{}
	// Params contains tasks whose results are passed as additional inputs to the ExecutionFn.
	Params []Task
}

// Type returns TaskTypeCallbackTask, identifying this as a callback task.
func (a *CallbackTask) Type() TaskType {
	return TaskTypeCallbackTask
}

// validate ensures the task has a name, both executor functions, and a Next task.
// When failedPath is true, both functions must accept one additional string parameter (error message).
func (a *CallbackTask) validate(failedPath bool) error {
	if a.taskName == "" {
		return errors.Newf("task name is missing")
	}
	if a.ExecutionFn == nil {
		return errors.Newf("execution function is missing for callback task <%s>", a.taskName)
	}
	if a.ResultProcessorFn == nil {
		return errors.Newf("result processor function is missing for callback task <%s>", a.taskName)
	}

	// Validate that ExecutionFn has the correct signature: (params...) (string, error)
	fnType := reflect.TypeOf(a.ExecutionFn)
	if fnType.Kind() != reflect.Func {
		return errors.Newf("execution function for callback task <%s> must be a function", a.taskName)
	}
	if fnType.NumIn() < 3 {
		return errors.Newf("execution function for callback task <%s> must take at least 3 parameters (context.Context, *PlanExecutionInfo, input interface{}), got %d parameters", a.taskName, fnType.NumIn())
	}

	// Validate first parameter is context.Context
	contextType := reflect.TypeOf((*context.Context)(nil)).Elem()
	if !fnType.In(0).Implements(contextType) {
		return errors.Newf("execution function for callback task <%s> must have context.Context as first parameter, got %s", a.taskName, fnType.In(0))
	}

	// Validate second parameter is *PlanExecutionInfo
	planExecutionInfoType := reflect.TypeOf(&PlanExecutionInfo{})
	if fnType.In(1) != planExecutionInfoType {
		return errors.Newf("execution function for callback task <%s> must have *PlanExecutionInfo as second parameter, got %s", a.taskName, fnType.In(1))
	}

	// Validate that the number of Params matches the number of additional parameters beyond the 3 required ones.
	// If in a failure path, the executor receives one extra parameter (error string) after the params.
	// Normal path: func(ctx, planInfo, input, ...params) -> expectedParams = NumIn() - 3
	// Failure path: func(ctx, planInfo, input, ...params, errorString) -> expectedParams = NumIn() - 4
	var expectedParams int
	if failedPath {
		// In failure path: func(ctx, planInfo, input, ...params, errorString)
		expectedParams = fnType.NumIn() - 4 // 3 base params + 1 error string
	} else {
		// In the normal path: func(ctx, planInfo, input, ...params)
		expectedParams = fnType.NumIn() - 3 // 3 base params
	}
	if len(a.Params) != expectedParams {
		if failedPath {
			return errors.Newf("execution function for callback task <%s> in failure path expects %d Param(s), got %d (signature should be: func(ctx, planInfo, input, ...%d params, errorString))", a.taskName, expectedParams, len(a.Params), expectedParams)
		}
		return errors.Newf("execution function for callback task <%s> expects %d Param(s), got %d (signature should be: func(ctx, planInfo, input, ...%d params))", a.taskName, expectedParams, len(a.Params), expectedParams)
	}

	if fnType.NumOut() != 2 {
		return errors.Newf("execution function for callback task <%s> must return exactly 2 values (string, error), got %d return values", a.taskName, fnType.NumOut())
	}
	if fnType.Out(0).Kind() != reflect.String {
		return errors.Newf("execution function for callback task <%s> must return string as first return value, got %s", a.taskName, fnType.Out(0).Kind())
	}
	errorInterface := reflect.TypeOf((*error)(nil)).Elem()
	if !fnType.Out(1).Implements(errorInterface) {
		return errors.Newf("execution function for callback task <%s> must return error as second return value, got %s", a.taskName, fnType.Out(1))
	}

	// Validate that ResultProcessorFn has the correct signature: (ctx, input, stepID, result) (interface{}, error)
	processorType := reflect.TypeOf(a.ResultProcessorFn)
	if processorType.Kind() != reflect.Func {
		return errors.Newf("result processor function for callback task <%s> must be a function", a.taskName)
	}
	if processorType.NumIn() < 5 {
		return errors.Newf("result processor function for callback task <%s> must take at least 4 parameters (context.Context, *PlanExecutionInfo, input interface{}, stepID string, asyncResult string), got %d parameters", a.taskName, processorType.NumIn())
	}

	// Validate first parameter is context.Context
	if !processorType.In(0).Implements(contextType) {
		return errors.Newf("result processor function for callback task <%s> must have context.Context as first parameter, got %s", a.taskName, processorType.In(0))
	}

	// Validate second parameter is *PlanExecutionInfo
	if processorType.In(1) != planExecutionInfoType {
		return errors.Newf("result processor function for callback task <%s> must have *PlanExecutionInfo as second parameter, got %s", a.taskName, processorType.In(1))
	}

	if processorType.NumOut() != 2 {
		return errors.Newf("result processor function for callback task <%s> must return exactly 2 values (interface{}, error), got %d return values", a.taskName, processorType.NumOut())
	}
	if !processorType.Out(1).Implements(errorInterface) {
		return errors.Newf("result processor function for callback task <%s> must return error as second return value, got %s", a.taskName, processorType.Out(1))
	}

	// Note: Next is validated in validateStepTaskPaths based on execution context (normal vs failure path).
	// When failedPath=false, Next is required. When failedPath=true, Fail is required instead.
	return nil
}

// GetExecutionFnName returns the Go function name for this callback task's execution function.
// Note: This returns the reflection-based function name, not the registered executor name.
// The execution function starts the operation and returns a step ID.
func (a *CallbackTask) GetExecutionFnName(executorRegistry map[string]*Executor) string {
	return getExecutorName(a.ExecutionFn, executorRegistry)
}

// GetResultProcessorFnName returns the Go function name for this callback task's result processor function.
// Note: This returns the reflection-based function name, not the registered executor name.
// The result processor function processes the result when the operation completes.
func (a *CallbackTask) GetResultProcessorFnName(executorRegistry map[string]*Executor) string {
	return getExecutorName(a.ResultProcessorFn, executorRegistry)
}

// ChildTaskInfo contains the information needed to execute a child plan task.
type ChildTaskInfo struct {
	// PlanVariant is the variant of the child plan to execute.
	PlanVariant string `json:"PlanVariant"`
	// Input is the input data to be passed to the child plan.
	Input interface{} `json:"Input"`
}

// ChildPlanTask executes a child plan synchronously by invoking ExecutePlanSync on the appropriate manager.
// It embeds a stepTask and waits for the child plan to complete before continuing to Next.
// The assumption is that the worker for the child plan is already running; otherwise the task fails.
type ChildPlanTask struct {
	stepTask
	// PlanName is the name of the child plan to execute.
	PlanName string
	// ChildTaskInfoFn is the function to execute, which must return a ChildTaskInfo containing the plan variant and input.
	// Expected signature: func(ctx context.Context, planExecutionInfo *PlanExecutionInfo, input interface{}, params...) (ChildTaskInfo, error)
	ChildTaskInfoFn interface{}
	// Params contains tasks whose results are passed as additional inputs to the executor.
	Params []Task
}

// Type returns TaskTypeChildPlanTask, identifying this as a child task.
func (c *ChildPlanTask) Type() TaskType {
	return TaskTypeChildPlanTask
}

// validate ensures the task has a name, plan name, an executor function, and a Next task.
// validate ensures the task has a name, plan name, an executor function, and a Next task.
// When failedPath is true, the executor must accept one additional string parameter (error message).
func (c *ChildPlanTask) validate(failedPath bool) error {
	if c.taskName == "" {
		return errors.Newf("task name is missing")
	}
	if c.PlanName == "" {
		return errors.Newf("plan name is missing for child task <%s>", c.taskName)
	}
	if c.ChildTaskInfoFn == nil {
		return errors.Newf("executor function is missing for child task <%s>", c.taskName)
	}

	// Validate that ChildTaskInfoFn has the correct signature: (params...) (ChildTaskInfo, error)
	fnType := reflect.TypeOf(c.ChildTaskInfoFn)
	if fnType.Kind() != reflect.Func {
		return errors.Newf("executor for child task <%s> must be a function", c.taskName)
	}
	if fnType.NumIn() < 3 {
		return errors.Newf("executor for child task <%s> must take at least 3 parameters (context.Context, *PlanExecutionInfo, input interface{}), got %d parameters", c.taskName, fnType.NumIn())
	}

	// Validate first parameter is context.Context
	contextType := reflect.TypeOf((*context.Context)(nil)).Elem()
	if !fnType.In(0).Implements(contextType) {
		return errors.Newf("executor for child task <%s> must have context.Context as first parameter, got %s", c.taskName, fnType.In(0))
	}

	// Validate second parameter is *PlanExecutionInfo
	planExecutionInfoType := reflect.TypeOf(&PlanExecutionInfo{})
	if fnType.In(1) != planExecutionInfoType {
		return errors.Newf("executor for child task <%s> must have *PlanExecutionInfo as second parameter, got %s", c.taskName, fnType.In(1))
	}

	// Validate that the number of Params matches the number of additional parameters beyond the 3 required ones.
	// If in a failure path, the executor receives one extra parameter (error string) after the params.
	// Normal path: func(ctx, planInfo, input, ...params) -> expectedParams = NumIn() - 3
	// Failure path: func(ctx, planInfo, input, ...params, errorString) -> expectedParams = NumIn() - 4
	var expectedParams int
	if failedPath {
		// In failure path: func(ctx, planInfo, input, ...params, errorString)
		expectedParams = fnType.NumIn() - 4 // 3 base params + 1 error string
	} else {
		// In the normal path: func(ctx, planInfo, input, ...params)
		expectedParams = fnType.NumIn() - 3 // 3 base params
	}
	if len(c.Params) != expectedParams {
		if failedPath {
			return errors.Newf("executor for child task <%s> in failure path expects %d Param(s), got %d (signature should be: func(ctx, planInfo, input, ...%d params, errorString))", c.taskName, expectedParams, len(c.Params), expectedParams)
		}
		return errors.Newf("executor for child task <%s> expects %d Param(s), got %d (signature should be: func(ctx, planInfo, input, ...%d params))", c.taskName, expectedParams, len(c.Params), expectedParams)
	}

	if fnType.NumOut() != 2 {
		return errors.Newf("executor for child task <%s> must return exactly 2 values (ChildTaskInfo, error), got %d return values", c.taskName, fnType.NumOut())
	}
	childTaskInfoType := reflect.TypeOf(ChildTaskInfo{})
	if fnType.Out(0) != childTaskInfoType {
		return errors.Newf("executor for child task <%s> must return ChildTaskInfo as first return value, got %s", c.taskName, fnType.Out(0))
	}
	errorInterface := reflect.TypeOf((*error)(nil)).Elem()
	if !fnType.Out(1).Implements(errorInterface) {
		return errors.Newf("executor for child task <%s> must return error as second return value, got %s", c.taskName, fnType.Out(1))
	}

	// Note: Next is validated in validateStepTaskPaths based on execution context (normal vs failure path).
	// When failedPath=false, Next is required. When failedPath=true, Fail is required instead.
	return nil
}

// GetExecutorName returns the Go function name for this child plan task's child task info function.
// Note: This returns the reflection-based function name, not the registered executor name.
// The child task info function determines the plan variant and input at runtime.
func (c *ChildPlanTask) GetExecutorName(executorRegistry map[string]*Executor) string {
	return getExecutorName(c.ChildTaskInfoFn, executorRegistry)
}
