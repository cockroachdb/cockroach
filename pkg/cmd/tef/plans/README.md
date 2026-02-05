# Task Execution Framework (TEF) - Plan Development Guide

This guide explains how to create custom execution plans for the Task Execution Framework (TEF). Plans define complex, multi-step workflows with support for sequential execution, parallel execution, conditional branching, failure handling, and workflow visualization.

## Table of Contents

- [Overview](#overview)
- [Getting Started](#getting-started)
- [Plan Structure](#plan-structure)
- [Task Types](#task-types)
- [Executor Functions](#executor-functions)
  - [Logger Usage](./LOGGER_USAGE.md)
- [Plan Registration](#plan-registration)
- [CLI Commands](#cli-commands)
- [Validation and Error Handling](#validation-and-error-handling)
- [Examples](#examples)

## Overview

The TEF allows you to define structured, validated workflows for automating complex operations such as:
- Cluster provisioning and configuration
- Performance testing under adversity
- Multi-phase deployment workflows
- Data import and migration operations

Each plan consists of:
1. **Plan metadata** (name, description)
2. **Input data structure** (JSON-parseable)
3. **Executor functions** (the actual work)
4. **Task graph** (the workflow structure)

## Getting Started

To create a new plan:

1. Create a new package under `pkg/cmd/tef/plans/`
2. Implement the `Registry` interface
3. Define your input data structure
4. Create executor functions
5. Build the task graph in `GeneratePlan()`
6. Register your plan in `pkg/cmd/tef/plans/registry.go`

### Minimal Example Structure

```
pkg/cmd/tef/plans/
└── myplan/
    └── plan.go
```

## Plan Structure

Every plan must implement the `planners.Registry` interface:

```go
package myplan

import (
    "context"
    "encoding/json"
    "github.com/cockroachdb/cockroach/pkg/cmd/tef/planners"
)

const myPlanName = "myplan"

type MyPlan struct{}

func NewMyPlan() *MyPlan {
    return &MyPlan{}
}

var _ planners.Registry = &MyPlan{}

// GetPlanName returns the unique identifier for this plan
func (m *MyPlan) GetPlanName() string {
    return myPlanName
}

// GetPlanDescription returns a human-readable description
func (m *MyPlan) GetPlanDescription() string {
    return "Description of what your plan does"
}

// Input data structure
type myPlanData struct {
    ClusterName string `json:"cluster_name"`
    NodeCount   int    `json:"node_count"`
}

// ParsePlanInput parses and validates JSON input
func (m *MyPlan) ParsePlanInput(input string) (interface{}, error) {
    data := &myPlanData{}
    if err := json.Unmarshal([]byte(input), data); err != nil {
        return nil, err
    }

    // Set defaults
    if data.NodeCount == 0 {
        data.NodeCount = 3
    }

    return data, nil
}

// GeneratePlan builds the task execution graph
func (m *MyPlan) GeneratePlan(ctx context.Context, p planners.Planner) {
    // Register executors
    registerExecutors(ctx, p)

    // Create tasks and wire them together
    // (See examples below)
}
```

## Task Types

TEF supports several task types for building workflows:

### 1. ExecutionTask

Executes a registered executor function.

```
     ┌───────────────┐
  ───┤ ExecutionTask ├─── success ──> Next
     └───────────────┘
            │
          failure
            │
            v
          Fail (optional)
```

```go
task := p.NewExecutionTask(ctx, "setup cluster")
task.ExecutorFn = setupCluster  // Must be registered
task.Next = nextTask            // Task to run on success
task.Fail = failureTask         // Task to run on failure (optional)

// Can pass outputs from previous tasks as parameters
task.Params = []planners.Task{previousTask}
```

**Requirements:**
- Must have `ExecutorFn` set to a registered executor
- Must have `Next` task defined
- `Fail` task is optional

### 2. ForkTask

Executes multiple task branches in parallel. All branches must converge to the same `EndTask`.

```
                ┌─────────┐
             ┌──┤ Branch1 ├──┐
             │  └─────────┘  │
             │               │
  ─── ForkTask               ├──> ForkJoinTask ─── success ──> Next
             │               │
             │  ┌─────────┐  │
             └──┤ Branch2 ├──┘
                └─────────┘
                     │
                   failure
                     │
                     v
                   Fail (optional)
```

```go
forkTask := p.NewForkTask(ctx, "parallel setup")
forkEndTask := p.NewForkJoinTask(ctx, "fork end")

// Branch 1
branch1Task := p.NewExecutionTask(ctx, "setup network")
branch1Task.ExecutorFn = setupNetwork
branch1Task.Next = forkEndTask

// Branch 2
branch2Task := p.NewExecutionTask(ctx, "setup storage")
branch2Task.ExecutorFn = setupStorage
branch2Task.Next = forkEndTask

// Configure fork
forkTask.Tasks = []planners.Task{branch1Task, branch2Task}
forkTask.Join = forkEndTask    // Synchronization point where all branches converge
forkTask.Next = afterForkTask  // Runs after all branches complete
forkTask.Fail = cleanupTask    // Runs if any branch fails
```

**Requirements:**
- Must have at least one task in `Tasks` slice
- Must set `Join` to a `ForkJoinTask` where all branches converge
- All forked branches must have their `Next` set to the same `Join` ForkJoinTask
- Must have `Next` task defined (runs after join point)
- Cannot be used in failure paths

### 3. ForkJoinTask

Marks a synchronization point where parallel fork branches converge. This task has no execution logic - it only serves as a barrier ensuring all fork branches complete before continuing.

```
  ───> ForkJoinTask
         │
         ● (synchronization barrier for fork branches)
```

```go
forkJoin := p.NewForkJoinTask(ctx, "join point")

// Use as the Join point for a ForkTask
forkTask.Join = forkJoin
```

**Requirements:**
- No `Next` or `Fail` tasks (controlled by the parent ForkTask)
- Must be referenced as the `Join` point of a `ForkTask`
- All branches of the fork must have their `Next` set to this task

**Important Notes:**
- ForkJoinTask does not terminate execution - it only synchronizes parallel branches
- After all fork branches reach the join point, execution continues to the ForkTask's `Next` task
- This is different from EndTask, which actually terminates an execution path

### 4. ConditionTask

Conditional branching based on a boolean executor result.

```
                  ┌─────────────┐
                ┌─┤ Then (true) ├─┐
                │ └─────────────┘ │
                │                 │
  ─── ConditionTask               ├──> EndTask (join)
                │                 │
                │ ┌──────────────┐│
                └─┤ Else (false) ├┘
                  └──────────────┘
```

```go
conditionTask := p.NewConditionTask(ctx, "check cluster ready")
conditionTask.ExecutorFn = checkClusterReady  // Must return (bool, error)
conditionTask.Then = successTask              // If true
conditionTask.Else = alternativeTask          // If false

// Both Then and Else branches must converge to the same EndTask
```

**Requirements:**
- Executor must return `(bool, error)`
- Must have both `Then` and `Else` tasks defined
- Both branches must converge to the same `EndTask`

### 5. CallbackTask

Executes an asynchronous operation that starts work and later processes the result when signaled externally. This is useful for operations that:
- Submit work to external systems (e.g., Temporal workflows, batch jobs)
- Need to wait for external completion signals
- Require separate initiation and result processing logic

```
  ─── CallbackTask
         │
         ├─ Phase 1: ExecutionFn() ──> returns stepID
         │                                   │
         │                              [wait for external signal]
         │                                   │
         └─ Phase 2: ResultProcessorFn(stepID, result)
                          │
                       success ──> Next
                          │
                        failure
                          │
                          v
                        Fail (optional)
```

```go
callbackTask := p.NewCallbackTask(ctx, "submit analysis job")
callbackTask.ExecutionFn = startAnalysisJob     // Must return (string, error) - returns step ID
callbackTask.ResultProcessorFn = processResults  // Processes results when job completes
callbackTask.Next = nextTask
callbackTask.Fail = failureTask  // Optional failure handler
callbackTask.Params = []planners.Task{previousTask}  // Optional parameters
```

**Executor Signatures:**

The ExecutionFn starts the async operation and returns a step ID:
```go
func startAnalysisJob(ctx context.Context, input *myPlanData) (string, error) {
    // Start the async operation (e.g., submit to external system)
    stepID := // ... get step ID from external system
    return stepID, nil  // Return step ID for tracking
}
```

The ResultProcessorFn processes the result when the operation completes:
```go
func processResults(
    ctx context.Context,
    input interface{},      // Original input data
    stepID string,          // Step ID from ExecutionFn
    result string,          // Result from external system
) (interface{}, error) {
    // Process the result
    processedData := // ... parse and process result
    return processedData, nil
}
```

**Requirements:**
- `ExecutionFn` must return `(string, error)` - the string is the step ID for tracking
- `ResultProcessorFn` must take `(context.Context, interface{}, string, string)` and return `(interface{}, error)`
- Must have `Next` task defined
- `Fail` task is optional
- Both executor functions must be registered
- Use the `resume` command to signal completion (see [Resume Callback Tasks](#4-resume-callback-tasks))

### 6. ChildWorkflowTask

Executes a child plan synchronously and waits for it to complete before continuing. This task type enables plan composition, allowing you to build complex workflows by orchestrating multiple plans together.

The ChildWorkflowTask requires a static PlanName (set on the task) and uses an ChildTaskInfoFn that determines the plan variant and input at runtime, providing flexibility to select different plan variants based on workflow state.

```
  ─── ChildWorkflowTask
         │
         ├─ ChildTaskInfoFn() ──> determines variant + input
         │
         └─ Execute Child Plan (wait for completion)
                     │
                  ┌──┴──┐
              success  failure
                  │      │
                  v      v
                Next   Fail (optional)
```

```go
childTask := p.NewChildWorkflowTask(ctx, "provision cluster")
childTask.PlanName = "roachprod"         // Static plan name
childTask.ChildTaskInfoFn = getProvisionInfo  // Must return ChildTaskInfo with variant
childTask.Params = []planners.Task{previousTask}  // Optional parameters
childTask.Next = nextTask
childTask.Fail = failureTask  // Optional failure handler
```

**Executor Signature:**

The ChildTaskInfoFn must return a `ChildTaskInfo` struct containing the plan variant and input data. The plan name is statically defined on the ChildWorkflowTask, and the plan ID is generated at execution time by combining the plan name and variant.

```go
func getProvisionInfo(
    ctx context.Context,
    planInfo *planners.PlanExecutionInfo,  // Current plan execution info
    input *myPlanData,                      // Original input data
    param1 string,                          // Optional: output from Params tasks
) (planners.ChildTaskInfo, error) {
    // Determine plan variant and prepare input
    // The plan name "roachprod" is set on the ChildWorkflowTask
    // Plan ID will be generated as: planners.GetPlanID("roachprod", "dev")
    childInput := &roachprod.ProvisionData{
        ClusterName: input.ClusterName,
        NodeCount:   input.NodeCount,
    }

    return planners.ChildTaskInfo{
        PlanVariant: "dev",        // Variant of the child plan
        Input:       childInput,   // Input to pass to child plan
    }, nil
}
```

**ChildTaskInfo Structure:**

```go
type ChildTaskInfo struct {
    PlanVariant string       // Variant of the child plan to execute (e.g., "dev", "prod")
    Input       interface{}  // Input data to be passed to the child plan
}
```

**Requirements:**
- Must set `PlanName` on the ChildWorkflowTask (static plan name)
- ChildTaskInfoFn must return `(planners.ChildTaskInfo, error)` with the plan variant
- ChildTaskInfoFn signature must be: `func(context.Context, *PlanExecutionInfo, input, params...) (ChildTaskInfo, error)`
- Must have `Next` task defined
- `Fail` task is optional
- The child plan's worker must be running before execution
- The child plan executes synchronously - the parent workflow waits for completion
- Use `Params` to pass outputs from previous tasks to the executor
- The plan ID is generated during execution as: `GetPlanID(task.PlanName, childTaskInfo.PlanVariant)`

**Important Notes:**
- The child plan executes to completion before the parent workflow continues
- If the child plan fails, the ChildWorkflowTask fails and triggers the `Fail` path (if defined)
- The child plan's worker must already be running - ChildWorkflowTask does not start workers
- Use this for plan composition, not parallel execution (use ForkTask for parallelism)

### 7. EndTask

Marks the termination of an execution path. All execution paths must end with an `EndTask`.

```
  ───> EndTask
         │
         ● (terminates execution path)
```

```go
endTask := p.NewEndTask(ctx, "end")
```

**Requirements:**
- No `Next` or `Fail` tasks
- All execution paths must converge to a common `EndTask`

## Executor Functions

Executors are functions that perform the actual work. They must be registered with the planner.

### Executor Function Signatures

Executors can have various signatures depending on their purpose:

```go
// Basic executor: takes input data
func setupCluster(ctx context.Context, input *myPlanData) (string, error) {
    // Perform work
    return "cluster-id", nil
}

// Executor with parameters: takes input data + outputs from previous tasks
func provisionCluster(
    ctx context.Context,
    input *myPlanData,
    setupOutput string,  // Output from previous task
) (string, error) {
    // Use setupOutput from the task in Params
    return "provision-id", nil
}

// Conditional executor: returns bool for ConditionTask
func checkClusterReady(ctx context.Context, input *myPlanData) (bool, error) {
    ready := // ... check logic
    return ready, nil
}

// Async execution function: starts async work and returns step ID for CallbackTask
func startBatchImport(ctx context.Context, input *myPlanData) (string, error) {
    // Submit to external system and get tracking ID
    stepID := submitToExternalSystem(input)
    return stepID, nil
}

// Async result processor: processes results when async work completes
func processBatchResults(
    ctx context.Context,
    input interface{},  // Original input data
    stepID string,      // Step ID from execution function
    result string,      // Result from external system
) (interface{}, error) {
    // Parse and process the result
    data := parseResults(result)
    return data, nil
}
```

### Registering Executors

All executors must be registered before use:

```go
func registerExecutors(ctx context.Context, p planners.Planner) {
    for _, exe := range []*planners.Executor{
        {
            Name:        "setup cluster",
            Description: "Sets up a cluster with the given configuration",
            Func:        setupCluster,
            Idempotent:  true,  // Can be safely retried
        },
        {
            Name:        "provision cluster",
            Description: "Provisions the cluster resources",
            Func:        provisionCluster,
        },
        // ... more executors
    } {
        p.RegisterExecutor(ctx, exe)
    }
}
```

**Important:** The framework validates that all `ExecutorFn` references are registered. If an executor is not registered, the plan will panic during validation.

### Logging in Executors

For detailed information on using the integrated logger in your executor functions, see [LOGGER_USAGE.md](./LOGGER_USAGE.md).

## Plan Registration

After creating your plan, register it in `pkg/cmd/tef/plans/registry.go`:

```go
func GetRegistrations() []planners.Registry {
    return []planners.Registry{
        roachprod.NewRoachprod(),
        demo.NewDemo(),
        pua.NewPUA(),
        myplan.NewMyPlan(),  // Add your plan here
    }
}
```

Once registered, TEF automatically generates CLI commands for your plan.

## CLI Commands

TEF automatically generates CLI commands for each registered plan. For complete CLI documentation, including all commands, flags, and usage examples, see [../CLI.md](../CLI.md).

Quick reference for the most common commands:

```bash
# Start a worker
./bin/tef start-worker <planname> --plan-variant <variant>

# Execute a plan
./bin/tef execute <planname> '<json-input>' <variant>

# Visualize workflow
./bin/tef gen-view <planname>
```

## Validation and Error Handling

The TEF performs comprehensive validation when a plan is registered. The framework **panics** if it detects any of the following errors:

### Plan-Level Validation Errors

1. **Plan name contains whitespace**
   ```
   panic: plan name <my plan> cannot contain whitespace or new line
   ```

2. **Plan not registered**
   ```
   panic: the plan <myplan> is not registered
   ```
   *Solution:* Call `p.RegisterPlan(ctx, firstTask, outputTask)` in `GeneratePlan()`

3. **No first task defined**
   ```
   panic: planner <myplan> has no first task
   ```

### Task Validation Errors

4. **Duplicate task names**
   ```
   panic: task with name <setup cluster> is already registered for the plan <myplan>
   ```

5. **Duplicate executor names**
   ```
   panic: executor <setup cluster> is already registered for plan <myplan>
   ```

6. **Missing executor function**
   ```
   panic: executor is missing for execution task <setup cluster>
   ```

7. **Unregistered executor**
   ```
   panic: executor function for task <setup cluster> is not registered in ExecutorRegistry
   ```
   *Solution:* Ensure you call `p.RegisterExecutor(ctx, executor)` for all executors

8. **Cyclic dependency detected**
   ```
   panic: cyclic dependency detected: task <setup cluster> appears in its own execution path
   ```

### Workflow Structure Errors

9. **No common end task**
   ```
   panic: fork task <parallel setup> has forked tasks that converge to different EndTasks (<end1> vs <end2>)
   panic: if task <check cluster> has Then and Else branches that converge to different EndTasks (<end1> vs <end2>)
   panic: task <setup cluster> has Next and Fail paths that converge to different EndTasks (<end1> vs <end2>)
   ```
   *Solution:* Ensure all branches converge to the same `EndTask`

10. **Nil task in chain**
    ```
    panic: task chain has nil task
    ```

11. **Missing Next task**
    ```
    panic: task <setup cluster> has no Next task and doesn't end with EndTask
    ```

12. **Missing Then/Else for ConditionTask**
    ```
    panic: if task <check cluster> has no Then task
    panic: if task <check cluster> has no Else task
    ```

13. **Invalid executor signature for ConditionTask**
    ```
    panic: executor for if task <check cluster> must return exactly 2 values (bool, error), got 1 return values
    panic: executor for if task <check cluster> must return bool as first return value, got string
    ```

14. **Fork task in failure path**
    ```
    panic: fork task <parallel setup> is not allowed for the failure flow
    ```

15. **Invalid CallbackTask configuration**
    ```
    panic: execution function is missing for callback task <batch import>
    panic: result processor function is missing for callback task <batch import>
    panic: executor for callback task <batch import> must return exactly 2 values (string, error), got 1 return values
    panic: executor for callback task <batch import> must return string as first return value, got int
    panic: result processor for callback task <batch import> must take exactly 4 parameters, got 3 parameters
    panic: result processor for callback task <batch import> must return exactly 2 values, got 1 return values
    panic: result processor for callback task <batch import> must return error as second return value, got string
    ```
    *Solution:* Ensure `ExecutionFn` returns `(string, error)` and `ResultProcessorFn` has signature `func(context.Context, interface{}, string, string) (interface{}, error)`. Both functions must be registered.

16. **Invalid ChildWorkflowTask configuration**
    ```
    panic: executor function is missing for child task <provision cluster>
    panic: executor for child task <provision cluster> must be a function
    panic: executor for child task <provision cluster> must take at least 3 parameters (context.Context, *PlanExecutionInfo, input interface{}), got 2 parameters
    panic: executor for child task <provision cluster> must have context.Context as first parameter, got string
    panic: executor for child task <provision cluster> must have *PlanExecutionInfo as second parameter, got string
    panic: executor for child task <provision cluster> must return exactly 2 values (ChildTaskInfo, error), got 1 return values
    panic: executor for child task <provision cluster> must return ChildTaskInfo as first return value, got string
    panic: executor for child task <provision cluster> must return error as second return value, got string
    panic: next task is missing for task <provision cluster>
    ```
    *Solution:* Ensure ChildTaskInfoFn has signature `func(context.Context, *PlanExecutionInfo, input, params...) (ChildTaskInfo, error)` and must be registered. Must have Next task defined.

### Best Practices to Avoid Validation Errors

1. **Always create a single EndTask** and ensure all paths lead to it
2. **Register all executors** before referencing them in tasks
3. **Use unique names** for all tasks and executors
4. **Test your plan** using `gen-view-<planname>` to visualize the workflow
5. **Verify executor signatures** match task requirements:
   - `bool` for ConditionTask
   - `(string, error)` for CallbackTask ExecutionFn
   - `(context.Context, interface{}, string, string) (interface{}, error)` for CallbackTask ResultProcessorFn
   - `(ChildTaskInfo, error)` for ChildWorkflowTask ChildTaskInfoFn
6. **Avoid cycles** by carefully planning your task graph
7. **Always set Next tasks** for ExecutionTask, ForkTask, CallbackTask, and ChildWorkflowTask
8. **Always set Then and Else** for ConditionTask
9. **For CallbackTask**: Register both ExecutionFn and ResultProcessorFn as separate executors
10. **For ChildWorkflowTask**: Ensure the child plan's worker is running before execution

## Examples

### Example 1: Simple Sequential Workflow

```go
func (m *MyPlan) GeneratePlan(ctx context.Context, p planners.Planner) {
    registerExecutors(ctx, p)

    // Create tasks
    task1 := p.NewExecutionTask(ctx, "create cluster")
    task1.ExecutorFn = createCluster

    task2 := p.NewExecutionTask(ctx, "configure cluster")
    task2.ExecutorFn = configureCluster
    task2.Params = []planners.Task{task1}  // Pass task1 output to task2

    task3 := p.NewExecutionTask(ctx, "start cluster")
    task3.ExecutorFn = startCluster

    endTask := p.NewEndTask(ctx, "end")

    // Wire tasks together
    task1.Next = task2
    task1.Fail = endTask

    task2.Next = task3
    task2.Fail = endTask

    task3.Next = endTask
    task3.Fail = endTask

    // Register the plan
    p.RegisterPlan(ctx, task1, task1)
}
```

### Example 2: Parallel Execution with Fork

```go
func (m *MyPlan) GeneratePlan(ctx context.Context, p planners.Planner) {
    registerExecutors(ctx, p)

    // Fork setup
    forkTask := p.NewForkTask(ctx, "parallel setup")
    forkEndTask := p.NewForkJoinTask(ctx, "fork end")

    // Branch 1: Network setup
    setupNetwork := p.NewExecutionTask(ctx, "setup network")
    setupNetwork.ExecutorFn = setupNetworkFn
    setupNetwork.Next = forkEndTask
    setupNetwork.Fail = forkEndTask

    // Branch 2: Storage setup
    setupStorage := p.NewExecutionTask(ctx, "setup storage")
    setupStorage.ExecutorFn = setupStorageFn
    setupStorage.Next = forkEndTask
    setupStorage.Fail = forkEndTask

    // Configure fork
    forkTask.Tasks = []planners.Task{setupNetwork, setupStorage}
    forkTask.Join = forkEndTask  // Synchronization point where all branches converge

    // Continue after fork
    deployTask := p.NewExecutionTask(ctx, "deploy application")
    deployTask.ExecutorFn = deployApp

    endTask := p.NewEndTask(ctx, "end")

    // Wire together
    forkTask.Next = deployTask
    forkTask.Fail = endTask

    deployTask.Next = endTask
    deployTask.Fail = endTask

    p.RegisterPlan(ctx, forkTask, forkTask)
}
```

### Example 3: Conditional Branching

```go
func (m *MyPlan) GeneratePlan(ctx context.Context, p planners.Planner) {
    registerExecutors(ctx, p)

    setupTask := p.NewExecutionTask(ctx, "setup cluster")
    setupTask.ExecutorFn = setupCluster

    // Conditional check
    checkTask := p.NewConditionTask(ctx, "check cluster ready")
    checkTask.ExecutorFn = checkClusterReady  // Returns (bool, error)

    // If ready: proceed with normal flow
    deployTask := p.NewExecutionTask(ctx, "deploy workload")
    deployTask.ExecutorFn = deployWorkload

    // If not ready: retry
    retryTask := p.NewExecutionTask(ctx, "retry setup")
    retryTask.ExecutorFn = retrySetup

    endTask := p.NewEndTask(ctx, "end")

    // Wire together
    setupTask.Next = checkTask
    setupTask.Fail = endTask

    checkTask.Then = deployTask
    checkTask.Else = retryTask

    deployTask.Next = endTask
    deployTask.Fail = endTask

    retryTask.Next = endTask
    retryTask.Fail = endTask

    p.RegisterPlan(ctx, setupTask, setupTask)
}
```

### Example 4: Complex Workflow with Sleep and Failure Handling

```go
func (m *MyPlan) GeneratePlan(ctx context.Context, p planners.Planner) {
    registerExecutors(ctx, p)

    initTask := p.NewExecutionTask(ctx, "initialize")
    initTask.ExecutorFn = initialize

    // Sleep before checking
    sleepTask := p.NewExecutionTask(ctx, "wait for startup")
    sleepTask.ExecutorFn = sleepFor30Sec  // Sleeps for 30 seconds using time.Sleep

    validateTask := p.NewExecutionTask(ctx, "validate")
    validateTask.ExecutorFn = validate

    cleanupTask := p.NewExecutionTask(ctx, "cleanup")
    cleanupTask.ExecutorFn = cleanup

    endTask := p.NewEndTask(ctx, "end")

    // Wire with failure handling
    initTask.Next = sleepTask
    initTask.Fail = cleanupTask  // On failure, cleanup

    sleepTask.Next = validateTask
    sleepTask.Fail = cleanupTask

    validateTask.Next = endTask
    validateTask.Fail = cleanupTask  // Validation failure triggers cleanup

    cleanupTask.Next = endTask
    cleanupTask.Fail = endTask  // Cleanup always ends

    p.RegisterPlan(ctx, initTask, initTask)
}
```

### Example 5: Callback Task Execution

```go
func (m *MyPlan) GeneratePlan(ctx context.Context, p planners.Planner) {
    registerExecutors(ctx, p)

    // Prepare data
    prepareTask := p.NewExecutionTask(ctx, "prepare data")
    prepareTask.ExecutorFn = prepareData

    // Submit async batch import job
    callbackTask := p.NewCallbackTask(ctx, "batch import")
    callbackTask.ExecutorFn = startBatchImport        // Returns (string, error) - step ID
    callbackTask.ResultProcessorFn = processBatchResults  // Processes result when complete
    callbackTask.Params = []planners.Task{prepareTask}    // Pass prepared data

    // Verify results after callback task completes
    verifyTask := p.NewExecutionTask(ctx, "verify import")
    verifyTask.ExecutorFn = verifyImport
    verifyTask.Params = []planners.Task{callbackTask}  // Uses output from async result processor

    // Cleanup on failure
    cleanupTask := p.NewExecutionTask(ctx, "cleanup")
    cleanupTask.ExecutorFn = cleanup

    endTask := p.NewEndTask(ctx, "end")

    // Wire together
    prepareTask.Next = callbackTask
    prepareTask.Fail = cleanupTask

    callbackTask.Next = verifyTask
    callbackTask.Fail = cleanupTask  // Cleanup if async job fails

    verifyTask.Next = endTask
    verifyTask.Fail = cleanupTask  // Cleanup if verification fails

    cleanupTask.Next = endTask
    cleanupTask.Fail = endTask

    p.RegisterPlan(ctx, prepareTask, prepareTask)
}
```

**Key Points:**
- `startBatchImport` submits work to an external system and returns a step ID
- The framework waits for external signal indicating completion
- Use the `resume` command to signal completion and provide results (see [Resume Callback Tasks](#4-resume-callback-tasks))
- `processBatchResults` processes the result when the async operation completes
- Output from `processBatchResults` can be passed to subsequent tasks via `Params`

**Resuming the Callback Task:**
```bash
# When the batch import completes externally, resume with results
./bin/tef resume myplan \
  tef_plan_myplan.dev \
  <workflow-id> \
  <step-id-from-startBatchImport> \
  '{"rows_imported": 10000, "status": "success"}'
```

### Example 6: Child Workflow Execution

```go
func (m *MyPlan) GeneratePlan(ctx context.Context, p planners.Planner) {
    registerExecutors(ctx, p)

    // Prepare input for child plan
    prepareTask := p.NewExecutionTask(ctx, "prepare cluster config")
    prepareTask.ExecutorFn = prepareClusterConfig

    // Execute child plan that provisions infrastructure
    childTask := p.NewChildWorkflowTask(ctx, "provision infrastructure")
    childTask.PlanName = "roachprod"             // Set the child plan name
    childTask.ChildTaskInfoFn = getProvisionPlanInfo  // Returns ChildTaskInfo with variant
    childTask.Params = []planners.Task{prepareTask}  // Pass prepared config

    // Configure cluster after provisioning
    configureTask := p.NewExecutionTask(ctx, "configure cluster")
    configureTask.ExecutorFn = configureCluster
    configureTask.Params = []planners.Task{childTask}  // Use child plan output

    // Cleanup on failure
    cleanupTask := p.NewExecutionTask(ctx, "cleanup")
    cleanupTask.ExecutorFn = cleanup

    endTask := p.NewEndTask(ctx, "end")

    // Wire together
    prepareTask.Next = childTask
    prepareTask.Fail = cleanupTask

    childTask.Next = configureTask
    childTask.Fail = cleanupTask  // Cleanup if child plan fails

    configureTask.Next = endTask
    configureTask.Fail = cleanupTask

    cleanupTask.Next = endTask
    cleanupTask.Fail = endTask

    p.RegisterPlan(ctx, prepareTask, prepareTask)
}

// Executor that returns child plan info
func getProvisionPlanInfo(
    ctx context.Context,
    planInfo *planners.PlanExecutionInfo,
    input *myPlanData,
    config *ClusterConfig,  // Output from prepareTask
) (planners.ChildTaskInfo, error) {
    // Prepare input for the child plan
    // The plan name "roachprod" is set on the ChildWorkflowTask
    // The plan ID will be generated during execution as: GetPlanID("roachprod", "prod")
    childInput := &roachprod.ProvisionData{
        ClusterName: input.ClusterName,
        NodeCount:   config.NodeCount,
        Region:      config.Region,
    }

    return planners.ChildTaskInfo{
        PlanVariant: "prod",
        Input:       childInput,
    }, nil
}
```

**Key Points:**
- `getProvisionPlanInfo` determines the plan variant and prepares input at runtime
- The plan name "roachprod" must be set on the ChildWorkflowTask itself
- The plan ID is generated during execution as: `GetPlanID("roachprod", "prod")`
- The child plan (roachprod) executes synchronously and must complete before configureTask runs
- The child plan's worker must be running before execution (e.g., `./bin/tef start-worker roachprod --plan-variant prod`)
- If the child plan fails, the ChildWorkflowTask triggers the Fail path (cleanupTask)
- Output from the child plan can be passed to subsequent tasks via Params

**Running this workflow:**
```bash
# Terminal 1: Start child plan worker
./bin/tef start-worker roachprod --plan-variant prod

# Terminal 2: Start parent plan worker
./bin/tef start-worker myplan --plan-variant prod

# Terminal 3: Execute parent plan
./bin/tef execute myplan '{"cluster_name": "prod-cluster"}' prod
# This will automatically execute the roachprod child plan when childTask runs
```

### Example 7: Real-World Plan Structure

See the complete examples in:
- `pkg/cmd/tef/plans/demo/plan.go` - Comprehensive demo with all task types
- `pkg/cmd/tef/plans/pua/plan.go` - Performance Under Adversity testing
- `pkg/cmd/tef/plans/roachprod/plan.go` - Simple cluster provisioning

## Additional Resources

- **Framework Code**: `pkg/cmd/tef/planners/` - Core framework implementation
- **Task Definitions**: `pkg/cmd/tef/planners/tasks.go` - Task type definitions
- **Validation Logic**: `pkg/cmd/tef/planners/planner.go` - Plan validation

## Tips for Development

1. **Start simple**: Begin with a sequential workflow before adding complexity
2. **Use visualization**: Run `gen-view <planname>` frequently to verify structure
3. **Test incrementally**: Add one task at a time and validate
4. **Handle failures**: Always define `Fail` paths for critical tasks
5. **Document executors**: Provide clear descriptions for all executors
6. **Set defaults**: Parse input with sensible defaults for optional parameters
7. **Validate early**: The framework will panic on invalid plans - fix issues before runtime
