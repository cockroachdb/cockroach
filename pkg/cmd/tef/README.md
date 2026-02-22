# Task Execution Framework (TEF)

TEF is a workflow orchestration framework for building multi-step task execution systems. It provides core abstractions for defining workflows with sequential execution, parallel execution, conditional branching, failure handling, and automatic validation.

## High Level Overview

Every TEF workflow follows this structure:

* A **Plan** defines the workflow structure - what tasks to execute and in what order
* Tasks can execute sequentially, in parallel (via ForkTask), or conditionally (via ConditionTask)
* Each task runs an **Executor** function that performs the actual work
* The framework validates the workflow at plan creation time (cycle detection, convergence validation)
* Execution is delegated to an orchestration engine (e.g., Temporal, Airflow)

TEF is framework-agnostic: the core Plan definitions have no dependency on any specific orchestration engine. Swapping execution engines only requires implementing the `PlannerManager` interface.

**Current Status**:
- ✅ Core framework complete (interfaces, validation, task types)
- ✅ Manager registry for cross-plan task execution
- 🚧 Temporal planner skeleton (interface layer, client integration, status queries)
- ❌ Workflow execution, CLI commands, and plan implementations pending

## Terminology

Before diving into examples, here are the key terms:

* **Plan**: A workflow definition that implements the `Registry` interface. Plans describe what tasks to run and how they connect.
* **Workflow**: An execution instance of a Plan. When you execute a plan with specific input, you create a workflow.
* **Task**: An individual step in a workflow. TEF provides seven task types (ExecutionTask, ForkTask, ForkJoinTask, ConditionTask, CallbackTask, ChildWorkflowTask, EndTask).
* **Executor**: A function that performs actual work. Executors are registered with the plan and invoked by tasks.
* **Planner**: The interface for building task graphs. Plans use the Planner to create and wire tasks together.
* **Worker**: A process that executes workflows. Workers run the orchestration engine and process plan executions.

## Simple Example

Here's a simple plan that demonstrates TEF's core features:

```go
package demo

import (
    "context"
    "encoding/json"
    "fmt"
    "time"

    "github.com/cockroachdb/cockroach/pkg/cmd/tef/planners"
    "github.com/spf13/cobra"
)

type DemoPlan struct{}

// Input data for the plan
type DemoInput struct {
    Message string `json:"message"`
    Count   int    `json:"count"`
}

// PrepareExecution sets up necessary resources for plan execution
func (d *DemoPlan) PrepareExecution(ctx context.Context) error {
    return nil
}

// GetPlanName returns the plan identifier
func (d *DemoPlan) GetPlanName() string {
    return "demo"
}

// GetPlanDescription returns a human-readable description
func (d *DemoPlan) GetPlanDescription() string {
    return "Demonstrates TEF's core features: sequential execution, conditional branching, parallel execution, and sleep tasks"
}

// GetPlanVersion returns the workflow version
func (d *DemoPlan) GetPlanVersion() int {
    return 1
}

// ParsePlanInput parses JSON input
func (d *DemoPlan) ParsePlanInput(input string) (interface{}, error) {
    var data DemoInput
    if err := json.Unmarshal([]byte(input), &data); err != nil {
        return nil, err
    }
    return &data, nil
}

// AddStartWorkerCmdFlags adds plan-specific flags to worker commands
func (d *DemoPlan) AddStartWorkerCmdFlags(cmd *cobra.Command) {
    // No custom flags for this demo plan
}

// GeneratePlan builds the workflow structure
func (d *DemoPlan) GeneratePlan(ctx context.Context, p planners.Planner) {
    // Register executor functions
    p.RegisterExecutor(ctx, &planners.Executor{
        Name:        "print message",
        Description: "Prints a message",
        Func:        printMessage,
    })
    p.RegisterExecutor(ctx, &planners.Executor{
        Name:        "check count",
        Description: "Checks if count is positive",
        Func:        checkCount,
    })
    p.RegisterExecutor(ctx, &planners.Executor{
        Name:        "process parallel",
        Description: "Processes data in parallel branch",
        Func:        processParallel,
    })
    p.RegisterExecutor(ctx, &planners.Executor{
        Name:        "wait time",
        Description: "Returns wait duration",
        Func:        waitTime,
    })

    // 1. SEQUENTIAL EXECUTION: Print message
    task1 := p.NewExecutionTask(ctx, "print message")
    task1.ExecutorFn = printMessage

    // 2. CONDITIONAL BRANCHING: Check if count is positive
    conditionTask := p.NewConditionTask(ctx, "check count")
    conditionTask.ExecutorFn = checkCount  // Returns (bool, error)

    // 3. PARALLEL EXECUTION: Process two branches concurrently
    forkTask := p.NewForkTask(ctx, "parallel processing")
    forkJoin := p.NewForkJoinTask(ctx, "fork join")

    branch1 := p.NewExecutionTask(ctx, "process branch 1")
    branch1.ExecutorFn = processParallel
    branch1.Next = forkJoin

    branch2 := p.NewExecutionTask(ctx, "process branch 2")
    branch2.ExecutorFn = processParallel
    branch2.Next = forkJoin

    forkTask.Tasks = []planners.Task{branch1, branch2}
    forkTask.Join = forkJoin  // All branches must converge to this join point

    // 4. END: Workflow termination
    endTask := p.NewEndTask(ctx, "end")

    // Wire tasks together with FAILURE HANDLING
    task1.Next = conditionTask
    task1.Fail = endTask  // On failure, end workflow

    // Conditional paths
    conditionTask.Then = forkTask  // If count > 0, run parallel tasks
    conditionTask.Else = endTask  // If count <= 0, end

    // After parallel work
    forkTask.Next = endTask
    forkTask.Fail = endTask

    // Register the plan (first task, output task)
    p.RegisterPlan(ctx, task1, task1)
}

// Executor functions
func printMessage(ctx context.Context, input *DemoInput) (string, error) {
    fmt.Printf("Message: %s\n", input.Message)
    return "printed", nil
}

func checkCount(ctx context.Context, input *DemoInput) (bool, error) {
    return input.Count > 0, nil
}

func processParallel(ctx context.Context, input *DemoInput) (string, error) {
    // Parallel processing work
    return "processed", nil
}

func waitTime(ctx context.Context, input *DemoInput) (time.Duration, error) {
    return 5 * time.Second, nil
}
```

## Breakdown of the Example

Let's walk through each part of the example:

### Defining the Plan Structure

```go
type DemoPlan struct{}

func (d *DemoPlan) GetPlanName() string {
    return "demo"
}
```

Every plan implements the `Registry` interface. At minimum, it needs a name and a way to parse input.

### Plan Description and Versioning

```go
func (d *DemoPlan) GetPlanDescription() string {
    return "Demonstrates TEF's core features: sequential execution, conditional branching, parallel execution, and sleep tasks"
}

func (d *DemoPlan) GetPlanVersion() int {
    return 1
}
```

**GetPlanDescription()** returns a human-readable description that helps users and operators understand the plan's purpose. This description is exposed through the TEF API and CLI for documentation and discovery.

**GetPlanVersion()** returns the workflow version number, which is critical for managing backward compatibility:
- Start with version 1 for new plans
- Increment the version when making backward-incompatible changes to the workflow structure (adding/removing tasks, changing task order, modifying input/output schemas)
- The orchestration engine uses this version to ensure running workflows continue executing with their original logic while new executions use the updated version
- Non-breaking changes (bug fixes in executor logic, performance improvements) don't require version increments

### Parsing Input

```go
func (d *DemoPlan) ParsePlanInput(input string) (interface{}, error) {
    var data DemoInput
    if err := json.Unmarshal([]byte(input), &data); err != nil {
        return nil, err
    }
    return &data, nil
}
```

Plans receive input as JSON strings. The framework validates and parses this input before execution.

### Registering Executors

```go
p.RegisterExecutor(ctx, &planners.Executor{
    Name:        "print message",
    Description: "Prints a message",
    Func:        printMessage,
})
```

All executor functions must be registered before they can be referenced in tasks. The framework validates that executors exist and have correct signatures.

### Sequential Execution

```go
task1 := p.NewExecutionTask(ctx, "print message")
task1.ExecutorFn = printMessage
task1.Next = conditionTask  // Next task on success
task1.Fail = endTask  // Failure handler
```

ExecutionTask runs an executor and proceeds to the Next task on success or Fail task on error.

### Conditional Branching

```go
conditionTask := p.NewConditionTask(ctx, "check count")
conditionTask.ExecutorFn = checkCount  // Must return (bool, error)
conditionTask.Then = forkTask  // If true
conditionTask.Else = sleepTask  // If false
```

ConditionTask evaluates a boolean executor and follows the Then or Else path.

### Parallel Execution

```go
forkTask := p.NewForkTask(ctx, "parallel processing")
forkJoin := p.NewForkJoinTask(ctx, "fork join")
branch1 := p.NewExecutionTask(ctx, "process branch 1")
branch1.Next = forkJoin
branch2 := p.NewExecutionTask(ctx, "process branch 2")
branch2.Next = forkJoin
forkTask.Tasks = []planners.Task{branch1, branch2}
forkTask.Join = forkJoin  // Synchronization point for all branches
```

ForkTask executes multiple branches concurrently. All branches must converge to the specified Join point (a ForkJoinTask) before execution continues to the fork's Next task. The Join ForkJoinTask acts as a synchronization barrier, not as termination of the entire execution path.

### Ending the Workflow

```go
endTask := p.NewEndTask(ctx, "end")
```

All execution paths must end with an EndTask. EndTask serves two purposes:
1. **Termination**: When used as the final task, it marks the end of the entire execution path
2. **Synchronization**: When used as a ForkTask.Join, it acts as a synchronization barrier where parallel branches converge before execution continues to the fork's Next task

The framework validates that all branches converge to appropriate EndTasks.

## Additional Task Types

The example above shows the most common task types. TEF also supports:

* **CallbackTask**: For operations that start work and wait for external completion signals
* **ChildWorkflowTask**: For executing other plans as sub-workflows

See the [Plan Development Guide](plans/README.md) for complete documentation of all task types.

## Getting Started

### Prerequisites

TEF requires an orchestration engine (e.g., Temporal) to execute workflows. The core framework provides the abstractions; you need to implement the `PlannerManager` interface for your chosen engine.

**What's Implemented:**
- Core interfaces and abstractions
- BasePlanner with full validation (cycle detection, convergence checks)
- All seven task types
- Plan registry infrastructure
- Manager registry for child task execution
- Temporal planner skeleton (interface layer, client integration, status queries)

**What's TODO:**
- Temporal workflow execution implementation
- CLI command generation
- Plan implementations
- Worker runtime

See [TODO.md](TODO.md) for the complete list of pending work.

### Building TEF

```bash
# From the cockroach repository root
./dev build tef

# The binary will be available at: ./bin/tef
```

Note: The current binary has minimal functionality until the orchestration engine and CLI commands are implemented.

### Creating Your First Plan

1. Create a package under `pkg/cmd/tef/plans/myplan/`
2. Implement the `planners.Registry` interface (see example above)
3. Register your plan in `pkg/cmd/tef/plans/registry.go`
4. Run validation: the framework will panic if your plan has issues

See the [Plan Development Guide](plans/README.md) for detailed instructions.

### Running Workflows (Pseudo Code)

Once implemented, the workflow would be:

```bash
# Terminal 1: Start a worker
./bin/tef start-worker demo --plan-variant dev

# Terminal 2: Execute the plan
./bin/tef execute demo '{"message": "Hello", "count": 5}' dev
# Returns: Workflow ID for tracking

# Check status
./bin/tef status demo <workflow-id>
```

## Architecture and Design

TEF follows key design principles:

**Framework Independence**: Core abstractions have zero dependency on any orchestration engine. The entire execution layer can be swapped by implementing the `PlannerManager` interface.

**Comprehensive Validation**: Plans are validated at creation time:
- Cycle detection using depth-first traversal
- Convergence validation (all branches must end at the same EndTask)
- Executor registration verification
- Function signature validation

**Type Safety**: Executor function signatures are validated using reflection. ConditionTask executors must return `(bool, error)`, etc.

**Workflow Versioning**: Plans support versioning via `GetWorkflowVersion()`. Increment the version for backward-incompatible changes.

For detailed architecture documentation, see [architecture.md](architecture.md).

## Additional Resources

* **[Plan Development Guide](plans/README.md)**: Comprehensive guide for creating plans with all task types and examples
* **[Architecture Documentation](architecture.md)**: Detailed architecture, code flow, and extension guide
* **[CLI Commands](CLI.md)**: Command-line interface reference
* **[REST API](API.md)**: HTTP API documentation
* **[TODO List](TODO.md)**: Pending implementation work

## Key Files

* `pkg/cmd/tef/planners/definitions.go` - Core interface definitions
* `pkg/cmd/tef/planners/planner.go` - BasePlanner implementation and validation
* `pkg/cmd/tef/planners/tasks.go` - All task type definitions
* `pkg/cmd/tef/plans/registry.go` - Plan registration

## Quick Reference

### Task Types

| Task Type | Purpose | Key Signature |
|-----------|---------|---------------|
| ExecutionTask | Run executor function | `func(ctx, input, ...params) (output, error)` |
| ForkTask | Parallel execution | Multiple branches, all converge to Join (ForkJoinTask) |
| ConditionTask | Conditional branching | `func(ctx, input, ...params) (bool, error)` |
| CallbackTask | External async work | ExecutionFn + ResultProcessorFn |
| ChildWorkflowTask | Execute child plan | `func(ctx, planInfo, input, ...params) (ChildTaskInfo, error)` |
| ForkJoinTask | Fork synchronization | Synchronization point where fork branches converge |
| EndTask | Termination | Marks end of execution path |

### Validation Rules

- All tasks must have unique names
- All executors must be registered before use
- No cyclic dependencies allowed
- All execution paths must converge to a common EndTask
- ForkTask must specify a Join (ForkJoinTask) and all branches must converge to it
- ConditionTask Then/Else paths must converge to the same EndTask
- Tasks with Next/Fail paths must converge to the same EndTask

For complete validation rules and error messages, see the [Plan Development Guide](plans/README.md#validation-and-error-handling).
