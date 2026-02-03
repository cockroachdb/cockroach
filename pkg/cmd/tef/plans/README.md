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

**Prerequisites**:
- For TEF architecture and system design, see [../architecture.md](../architecture.md)
- For current implementation status and getting started, see [../README.md](../README.md)

## Getting Started

To create a new plan:

1. Create a new package under `pkg/cmd/tef/plans/`
2. Implement the `Registry` interface (7 required methods)
3. Define your input data structure
4. Create executor functions
5. Build the task graph in `GeneratePlan()`
6. Create a `RegisterXxxPlans(pr *planners.PlanRegistry)` function
7. Register your plan in `pkg/cmd/tef/plans/registry.go`

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
    "github.com/spf13/cobra"
)

const myPlanName = "myplan"

type MyPlan struct{}

var _ planners.Registry = &MyPlan{}

// PrepareExecution sets up necessary resources or state for execution
func (m *MyPlan) PrepareExecution(ctx context.Context) error {
    // Initialize any required resources (e.g., API clients, connections)
    return nil
}

// GetPlanName returns the unique identifier for this plan
func (m *MyPlan) GetPlanName() string {
    return myPlanName
}

// GetPlanDescription returns a human-readable description
func (m *MyPlan) GetPlanDescription() string {
    return "Description of what your plan does"
}

// GetWorkflowVersion returns the workflow version (increment for incompatible changes)
func (m *MyPlan) GetWorkflowVersion() int {
    return 1
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

// AddStartWorkerCmdFlags adds custom flags to worker commands
func (m *MyPlan) AddStartWorkerCmdFlags(cmd *cobra.Command) {
    // Add plan-specific flags if needed
    // cmd.Flags().StringVar(&m.customFlag, "custom-flag", "default", "Description")
}

// GeneratePlan builds the task execution graph
func (m *MyPlan) GeneratePlan(ctx context.Context, p planners.Planner) {
    // Register executors
    registerExecutors(ctx, p)

    // Create tasks and wire them together
    // (See examples below)
}

// RegisterMyPlanPlans registers this plan and any child plans with the registry
func RegisterMyPlanPlans(pr *planners.PlanRegistry) {
    pr.Register(&MyPlan{})
    // Register child plans if needed
    // childplan.RegisterChildPlans(pr)
}
```

## Task Types

TEF supports several task types for building workflows:

### 1. ExecutionTask

Executes a registered executor function.

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

```go
forkTask := p.NewForkTask(ctx, "parallel setup")
forkEndTask := p.NewEndTask(ctx, "fork end")

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
forkTask.Next = afterForkTask  // Runs after all branches complete
forkTask.Fail = cleanupTask    // Runs if any branch fails
```

**Requirements:**
- Must have at least one task in `Tasks` slice
- All forked branches must converge to the same `EndTask`
- Cannot be used in failure paths

### 3. IfTask

Conditional branching based on a boolean executor result.

```go
ifTask := p.NewIfTask(ctx, "check cluster ready")
ifTask.ExecutorFn = checkClusterReady  // Must return (bool, error)
ifTask.Then = successTask              // If true
ifTask.Else = alternativeTask          // If false

// Both Then and Else branches must converge to the same EndTask
```

**Requirements:**
- Executor must return `(bool, error)`
- Must have both `Then` and `Else` tasks defined
- Both branches must converge to the same `EndTask`

### 4. SleepTask

Delays execution for a duration returned by an executor.

```go
sleepTask := p.NewSleepTask(ctx, "wait for cluster")
sleepTask.ExecutorFn = sleepFor1Min  // Must return (time.Duration, error)
sleepTask.Next = nextTask
```

**Requirements:**
- Executor must return `(time.Duration, error)`
- Must have `Next` task defined

### 5. AsyncTask

Executes an asynchronous operation that starts work and later processes the result when signaled externally. This is useful for operations that:
- Submit work to external systems (e.g., Temporal workflows, batch jobs)
- Need to wait for external completion signals
- Require separate initiation and result processing logic

```go
asyncTask := p.NewAsyncTask(ctx, "submit analysis job")
asyncTask.ExecutionFn = startAnalysisJob     // Must return (string, error) - returns step ID
asyncTask.ResultProcessorFn = processResults  // Processes results when job completes
asyncTask.Next = nextTask
asyncTask.Fail = failureTask  // Optional failure handler
asyncTask.Params = []planners.Task{previousTask}  // Optional parameters
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
- Use the `resume` command to signal completion (see [Resume Async Tasks](#4-resume-async-tasks))

### 6. ChildWorkflowTask

Executes a child plan synchronously and waits for it to complete before continuing. This task type enables plan composition, allowing you to build complex workflows by orchestrating multiple plans together.

The ChildWorkflowTask requires a static PlanName (set on the task) and uses an ChildTaskInfoFn that determines the plan variant and input at runtime, providing flexibility to select different plan variants based on workflow state.

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

// Conditional executor: returns bool for IfTask
func checkClusterReady(ctx context.Context, input *myPlanData) (bool, error) {
    ready := // ... check logic
    return ready, nil
}

// Sleep executor: returns duration for SleepTask
func sleepFor1Min(ctx context.Context, input *myPlanData) (time.Duration, error) {
    return 1 * time.Minute, nil
}

// Async execution function: starts async work and returns step ID for AsyncTask
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
func RegisterPlans(pr *planners.PlanRegistry) {
    demo.RegisterDemoPlans(pr)
    pua.RegisterPUAPlans(pr)
    gc.RegisterGCPlans(pr)
    myplan.RegisterMyPlanPlans(pr)  // Add your plan here
}
```

**Important Notes:**
- Each plan package must provide a `RegisterXxxPlans(pr *planners.PlanRegistry)` function
- The registration function should call `pr.Register(&YourPlan{})` for the main plan
- If your plan has child plans, register them in the same function by calling their registration functions
- The `PlanRegistry` manages all registered plans and makes them available to the TEF framework

**Example with Child Plans:**
```go
func RegisterMyPlanPlans(pr *planners.PlanRegistry) {
    pr.Register(&MyPlan{})

    // Register child plans
    childplan1.RegisterChildPlan1Plans(pr)
    childplan2.RegisterChildPlan2Plans(pr)
}
```

Once registered, TEF automatically generates CLI commands for your plan.

## CLI Commands

When you register a plan named `myplan`, TEF automatically creates these commands:

### Understanding Plan IDs

Each worker and execution is associated with a **plan ID**, which is composed of:
```
plan_id = <plan_name>-<plan_suffix>
```

The plan ID serves several important purposes:

1. **Run different versions**: Different plan IDs allow you to run multiple versions of the same plan simultaneously
2. **Separate environments**: Use different suffixes for `dev`, `staging`, `prod`, etc.
3. **Isolate experiments**: Test workflow changes without affecting production executions
4. **Multiple instances**: Run the same plan with different configurations in parallel

**Plan Suffix Behavior:**
- For **workers**: If no suffix is provided via `--plan-variant`, a UUID is auto-generated
- For **executions**: The plan id is required as the second argument to `execute <planname>`

### 1. Start Worker

Start a worker to process plan executions:

```bash
# Auto-generated plan ID (e.g., myplan-a1b2c3d4-e5f6-7890-abcd-ef1234567890)
./dev build tef
./bin/tef start-worker myplan

# Explicit plan ID for development environment (myplan-dev)
./bin/tef start-worker myplan --plan-variant dev

# Explicit plan ID for production environment (myplan-prod)
./bin/tef start-worker myplan --plan-variant prod

# Explicit plan ID for a specific version (myplan-v2)
./bin/tef start-worker myplan --plan-variant v2
```

The worker listens for execution requests on the specified plan ID and processes workflows.

**Additional Worker Options:**
```bash
./bin/tef start-worker myplan \
  --plan-variant dev \
  --temporal-address localhost:7233 \
  --temporal-namespace my-namespace
```

### 2. Execute Plan

Execute your plan with JSON input and a plan ID:

```bash
# Execute on the 'dev' plan instance
./bin/tef execute myplan \
  '{"cluster_name": "test-cluster", "node_count": 5}' \
  dev

# Execute on the 'prod' plan instance
./bin/tef execute myplan \
  '{"cluster_name": "prod-cluster", "node_count": 9}' \
  prod

# Execute on a version-specific plan instance
./bin/tef execute myplan \
  '{"cluster_name": "test-cluster", "node_count": 5}' \
  v2-beta
```

**Arguments:**
1. **JSON input**: The plan input data as a JSON string
2. **Plan variant**: The suffix that forms the plan ID (must match a running worker)

This returns a workflow ID that you can use to track execution.

**Common Use Cases:**

- **Development workflow**:
  ```bash
  # Terminal 1: Start dev worker
  ./bin/tef start-worker myplan --plan-variant dev

  # Terminal 2: Execute on dev
  ./bin/tef execute myplan '{"cluster_name": "dev-cluster"}' dev
  ```

- **Multiple environments**:
  ```bash
  # Start workers for different environments
  ./bin/tef start-worker myplan --plan-variant dev
  ./bin/tef start-worker myplan --plan-variant staging
  ./bin/tef start-worker myplan --plan-variant prod

  # Execute on specific environment
  ./bin/tef execute myplan '{"cluster_name": "staging-test"}' tef_plan_myplan.staging
  ```

- **A/B testing workflows**:
  ```bash
  # Start workers for different plan versions
  ./bin/tef start-worker myplan --plan-variant v1
  ./bin/tef start-worker myplan --plan-variant v2

  # Test both versions
  ./bin/tef execute myplan '{"cluster_name": "test"}' tef_plan_myplan.v1
  ./bin/tef execute myplan '{"cluster_name": "test"}' tef_plan_myplan.v2
  ```

### 3. Visualize Plan

Generate a visual representation of your workflow:

```bash
./bin/tef gen-view myplan
```

This generates a DOT file and renders it as an image (requires Graphviz), showing:
- All tasks and their relationships
- Execution paths (Next, Fail)
- Fork branches
- Conditional branches (Then/Else)
- End tasks

Example visualization output:
```
Generating plan visualization for: myplan
DOT file generated: myplan.dot
Rendering diagram to: myplan.jpg
Visualization complete!
```

The generated diagram helps you:
- Verify your workflow structure
- Identify issues with task connections
- Document your workflow
- Share workflow designs with your team

### 4. Resume Async Tasks

Resume an async task that is waiting for external input. This is used when your plan includes AsyncTask types that submit work to external systems and wait for completion signals.

```bash
# Resume an async task via REST API (default)
./bin/tef resume myplan \
  tef_plan_myplan.dev \
  workflow-abc123 \
  step-xyz789 \
  '{"status": "completed", "result": "job finished successfully"}'

# Resume via direct Temporal connection
./bin/tef resume myplan \
  tef_plan_myplan.dev \
  workflow-abc123 \
  step-xyz789 \
  '{"status": "completed", "result": "job finished successfully"}' \
  --use-api=false

# Resume with custom API server
./bin/tef resume myplan \
  tef_plan_myplan.dev \
  workflow-abc123 \
  step-xyz789 \
  '{"status": "completed"}' \
  --api-host api.production.example.com \
  --api-port 8080
```

**Arguments:**
1. **Plan ID**: Full plan ID including suffix (e.g., `tef_plan_myplan.dev`)
2. **Workflow ID**: The workflow execution ID returned by the execute command
3. **Step ID**: The step ID returned by AsyncTask's ExecutionFn
4. **Input**: JSON string with result data to pass to AsyncTask's ResultProcessorFn

**Resume Options:**
- `--use-api`: Resume via REST API (default: `true`)
- `--api-host`: API server host (default: `localhost`)
- `--api-port`: API server port (default: `25780`)

**Workflow:**
1. Execute a plan containing an AsyncTask
2. The AsyncTask's ExecutionFn submits work to an external system and returns a step ID
3. The workflow waits for external completion
4. When the external work completes, use `resume` to signal completion and provide results
5. The AsyncTask's ResultProcessorFn processes the results and continues the workflow

**Example:**
```bash
# Terminal 1: Start worker
./bin/tef start-worker batch-import --plan-variant prod

# Terminal 2: Execute plan (starts async batch import)
./bin/tef execute batch-import '{"file": "data.csv"}' prod
# Output: Workflow started with ID: batch-import-abc123
#         Step ID from AsyncTask: import-job-xyz789

# Terminal 3: Later, when external batch job completes, resume the workflow
./bin/tef resume batch-import \
  tef_plan_batch-import.prod \
  batch-import-abc123 \
  import-job-xyz789 \
  '{"rows_imported": 10000, "status": "success"}'
```

### 5. Check Status

Check the status of a workflow execution:

```bash
./bin/tef status myplan <workflow-id>
```

### 6. List Executions

List all executions for your plan:

```bash
./bin/tef list-executions myplan
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

12. **Missing Then/Else for IfTask**
    ```
    panic: if task <check cluster> has no Then task
    panic: if task <check cluster> has no Else task
    ```

13. **Invalid executor signature for IfTask**
    ```
    panic: executor for if task <check cluster> must return exactly 2 values (bool, error), got 1 return values
    panic: executor for if task <check cluster> must return bool as first return value, got string
    ```

14. **Invalid executor signature for SleepTask**
    ```
    panic: executor for sleep task <wait> must return time.Duration as first return value, got int
    ```

15. **Fork task in failure path**
    ```
    panic: fork task <parallel setup> is not allowed for the failure flow
    ```

16. **Invalid AsyncTask configuration**
    ```
    panic: execution function is missing for async task <batch import>
    panic: result processor function is missing for async task <batch import>
    panic: executor for async task <batch import> must return exactly 2 values (string, error), got 1 return values
    panic: executor for async task <batch import> must return string as first return value, got int
    panic: result processor for async task <batch import> must take exactly 4 parameters, got 3 parameters
    panic: result processor for async task <batch import> must return exactly 2 values, got 1 return values
    panic: result processor for async task <batch import> must return error as second return value, got string
    ```
    *Solution:* Ensure `ExecutionFn` returns `(string, error)` and `ResultProcessorFn` has signature `func(context.Context, interface{}, string, string) (interface{}, error)`. Both functions must be registered.

17. **Invalid ChildWorkflowTask configuration**
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
   - `bool` for IfTask
   - `time.Duration` for SleepTask
   - `(string, error)` for AsyncTask ExecutionFn
   - `(context.Context, interface{}, string, string) (interface{}, error)` for AsyncTask ResultProcessorFn
   - `(ChildTaskInfo, error)` for ChildWorkflowTask ChildTaskInfoFn
6. **Avoid cycles** by carefully planning your task graph
7. **Always set Next tasks** for ExecutionTask, ForkTask, SleepTask, AsyncTask, and ChildWorkflowTask
8. **Always set Then and Else** for IfTask
9. **For AsyncTask**: Register both ExecutionFn and ResultProcessorFn as separate executors
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
    forkEndTask := p.NewEndTask(ctx, "fork end")

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
    checkTask := p.NewIfTask(ctx, "check cluster ready")
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
    sleepTask := p.NewSleepTask(ctx, "wait for startup")
    sleepTask.ExecutorFn = sleepFor30Sec  // Returns (time.Duration, error)

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

### Example 5: Asynchronous Task Execution

```go
func (m *MyPlan) GeneratePlan(ctx context.Context, p planners.Planner) {
    registerExecutors(ctx, p)

    // Prepare data
    prepareTask := p.NewExecutionTask(ctx, "prepare data")
    prepareTask.ExecutorFn = prepareData

    // Submit async batch import job
    asyncTask := p.NewAsyncTask(ctx, "batch import")
    asyncTask.ExecutorFn = startBatchImport        // Returns (string, error) - step ID
    asyncTask.ResultProcessorFn = processBatchResults  // Processes result when complete
    asyncTask.Params = []planners.Task{prepareTask}    // Pass prepared data

    // Verify results after async task completes
    verifyTask := p.NewExecutionTask(ctx, "verify import")
    verifyTask.ExecutorFn = verifyImport
    verifyTask.Params = []planners.Task{asyncTask}  // Uses output from async result processor

    // Cleanup on failure
    cleanupTask := p.NewExecutionTask(ctx, "cleanup")
    cleanupTask.ExecutorFn = cleanup

    endTask := p.NewEndTask(ctx, "end")

    // Wire together
    prepareTask.Next = asyncTask
    prepareTask.Fail = cleanupTask

    asyncTask.Next = verifyTask
    asyncTask.Fail = cleanupTask  // Cleanup if async job fails

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
- Use the `resume` command to signal completion and provide results (see [Resume Async Tasks](#4-resume-async-tasks))
- `processBatchResults` processes the result when the async operation completes
- Output from `processBatchResults` can be passed to subsequent tasks via `Params`

**Resuming the Async Task:**
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

### Documentation
- **[TEF Main README](../README.md)** - Overview, current implementation status, getting started
- **[TEF Architecture](../architecture.md)** - System architecture, code flow, extending TEF
- **[Logger Usage](./LOGGER_USAGE.md)** - Detailed logging guide for executor functions

### Code References
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
