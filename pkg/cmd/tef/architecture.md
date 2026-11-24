# TEF Architecture

This document explains the technical architecture and code organization of the Task Execution Framework (TEF).

For a high-level overview and getting started guide, see [README.md](README.md).

## Architectural Overview

TEF follows a layered, interface-based architecture that separates workflow definition from execution:

```
┌────────────────────────────────────────────────────────────┐
│                Core Abstractions                           │
│  - Planner interface                                       │
│  - Registry interface                                      │
│  - PlanExecutor & SharedPlanService interfaces             │
│  - Task type definitions                                   │
└────────────────────┬───────────────────────────────────────┘
                     │
┌────────────────────┴───────────────────────────────────────┐
│              BasePlanner Implementation                    │
│  - Task registration and validation                        │
│  - Cycle detection                                         │
│  - Convergence validation                                  │
│  - Executor registration                                   │
└────────────────────┬───────────────────────────────────────┘
                     │
          ┌──────────┴──────────┐
          │                     │
┌─────────┴─────────┐  ┌────────┴──────────────────────┐
│  Plans            │  │  Execution Engine             │
│  - Plan defs      │  │  - PlannerManager impl        │
│  - Executors      │  │  - Worker runtime             │
│  - Registry       │  │  - Plan execution             │
└───────────────────┘  └───────────────────────────────┘
```

**Status**:
- ✅ Core Abstractions: Fully implemented
- ✅ BasePlanner: Fully implemented with validation
- ❌ Plans: Registry exists, no implementations
- ❌ Execution Engine: Not implemented

## Directory Structure

```
pkg/cmd/tef/
├── main.go                      # Entry point
├── README.md                    # User documentation
├── architecture.md              # This file
├── CLI.md                       # CLI reference
├── API.md                       # REST API reference
├── TODO.md                      # Pending work
│
├── cli/                         # CLI command generation
│   ├── commands.go              # TODO: Implement initializeWorkerCLI()
│   └── initializer.go           # CLI initialization structure
│
├── planners/                    # Core abstractions (✅ fully implemented)
│   ├── definitions.go           # Executor, PlannerManager, Registry interfaces
│   ├── planner.go               # BasePlanner implementation
│   ├── tasks.go                 # Task type definitions (7 types)
│   ├── status.go                # Execution status types
│   ├── utils.go                 # Helper functions
│   ├── logger.go                # Logger interface and implementation
│   ├── plan_registry.go         # Plan registry management
│   ├── *_test.go                # Comprehensive tests
│   │
│   └── mock/                    # Generated mocks for testing
│       └── *.go
│
└── plans/                       # Plan definitions (❌ TODO)
    └── registry.go              # Registry stub

NOT YET CREATED:
├── planners/temporal/           # Temporal-specific implementation
│   ├── manager.go               # Implement PlannerManager for Temporal
│   ├── workflow.go              # Temporal workflow definitions
│   └── status.go                # Temporal status queries
│
├── plans/demo/                  # Example plan implementations
│   └── plan.go
│
└── api/                         # REST API (optional)
    ├── server.go
    └── handlers/
```

## Core Components

### 1. Interfaces and Abstractions

Defined in `pkg/cmd/tef/planners/definitions.go`:

**Planner Interface:**
```go
type Planner interface {
    RegisterExecutor(ctx context.Context, executor *Executor)
    RegisterPlan(ctx context.Context, first, output Task)
    NewExecutionTask(ctx context.Context, name string) *ExecutionTask
    NewForkTask(ctx context.Context, name string) *ForkTask
    NewConditionTask(ctx context.Context, name string) *ConditionTask
    NewCallbackTask(ctx context.Context, name string) *CallbackTask
    NewChildWorkflowTask(ctx context.Context, name string) *ChildWorkflowTask
    NewEndTask(ctx context.Context, name string) *EndTask
}
```

**Registry Interface:**
```go
type Registry interface {
    PrepareExecution(ctx context.Context) error
    GetPlanName() string
    GetPlanDescription() string
    GetWorkflowVersion() int
    ParsePlanInput(input string) (interface{}, error)
    AddStartWorkerCmdFlags(cmd *cobra.Command)
    GeneratePlan(ctx context.Context, p Planner)
}
```

**PlannerManager Interface:**
```go
type PlannerManager interface {
    StartWorker(ctx context.Context, planVariant string) error
    ExecutePlan(ctx context.Context, input interface{}, planID string) (string, error)
    GetBasePlanner() *BasePlanner
    GetExecutionStatus(ctx context.Context, planID, workflowID string) (*ExecutionStatus, error)
    ListExecutions(ctx context.Context, planID string) ([]*WorkflowExecutionInfo, error)
    ListAllPlans(ctx context.Context) ([]PlanInfo, error)
}
```

### 2. BasePlanner

The `BasePlanner` (`pkg/cmd/tef/planners/planner.go`) provides:

- **Task Creation**: Factory methods for all seven task types
- **Task Registration**: Maintains `TasksRegistry` map of all tasks
- **Executor Registration**: Maintains `ExecutorRegistry` map of all executors
- **Comprehensive Validation**:
  - Cycle detection using depth-first traversal
  - Convergence validation ensuring all branches lead to common EndTasks
  - Executor registration verification
  - Task configuration validation
  - Function signature validation using reflection

**Key Data Structures:**
```go
type BasePlanner struct {
    Registry         Registry
    First            Task
    Output           Task
    TasksRegistry    map[string]Task
    ExecutorRegistry map[string]*Executor
}
```

### 3. Task Types

Seven task types defined in `pkg/cmd/tef/planners/tasks.go`:

**Type Hierarchy:**

```
baseTask (name, taskName)
    ├── ExecutionTask (extends stepTask)
    ├── ForkTask (extends stepTask)
    ├── ConditionTask
    ├── CallbackTask (extends stepTask)
    ├── ChildWorkflowTask (extends stepTask)
    └── EndTask

stepTask (extends baseTask, adds Next/Fail)
```

**Validation per Task Type:**
- ExecutionTask: Must have ExecutorFn, Next
- ForkTask: Must have Tasks (>=1), Join (ForkJoinTask), Next; all branches converge to Join EndTask
- ConditionTask: Must have ExecutorFn returning (bool, error), Then, Else; branches converge to same EndTask
- CallbackTask: Must have ExecutionFn and ResultProcessorFn with correct signatures, Next
- ChildWorkflowTask: Must have ChildTaskInfoFn with correct signature, Next
- EndTask: No Next or Fail

## Code Flow

### 1. Plan Creation and Validation

```
Plan Author
    │
    ├─► Implements Registry interface
    │   ├─ GetPlanName()
    │   ├─ GetPlanDescription()
    │   ├─ GetWorkflowVersion()
    │   ├─ ParsePlanInput()
    │   ├─ GeneratePlan(ctx, Planner)
    │   ├─ PrepareExecution(ctx)
    │   └─ AddStartWorkerCmdFlags(cmd)
    │
    └─► Registers in RegisterPlans() (plans/registry.go)
```

### 2. Worker Startup Flow

```
./bin/tef start-worker myplan --plan-variant dev
    │
    ├─► CLI Command (cli/commands.go - initializeWorkerCLI)
    │
    ├─► NewPlannerManager(registry, orchestrationConfig)
    │   │
    │   └─► NewBasePlanner(ctx, registry)
    │       │
    │       ├─► registry.GeneratePlan(ctx, planner)
    │       │   ├─ Create tasks using Planner interface
    │       │   ├─ Register executors
    │       │   └─ Define task graph
    │       │
    │       └─► Validate plan
    │           ├─ Check for cycles using DFS
    │           ├─ Ensure convergence to common EndTasks
    │           ├─ Validate executor registration
    │           ├─ Validate executor function signatures
    │           └─ Validate task configuration
    │
    └─► manager.StartWorker(ctx, planID)
        ├─► Connect to orchestration engine
        ├─► Create worker for task queue
        ├─► Register workflows and activities
        └─► Start listening for executions
```

### 3. Plan Execution Flow

```
./bin/tef execute myplan '{"param": "value"}' tef.myplan.dev
    │
    ├─► CLI Command (cli/commands.go)
    │
    ├─► registry.ParsePlanInput(inputJSON)
    │
    ├─► NewPlannerManager(registry, orchestrationConfig)
    │
    └─► manager.ExecutePlan(ctx, input, planID)
        │
        ├─► Connect to orchestration engine
        ├─► Check for active workers on task queue
        ├─► Generate unique workflow ID
        └─► Start workflow execution
            │
            └─► Worker picks up workflow
                │
                └─► Execute task graph using BasePlanner structure
                    ├─ Run executors based on task types
                    ├─ Handle forks (parallel execution)
                    ├─ Handle conditionals (if/then/else)
                    ├─ Handle sleeps (duration-based delays)
                    ├─ Handle callback tasks (wait for signals)
                    ├─ Handle child workflows (synchronous sub-plans)
                    └─ Follow Next/Fail paths
```

## Validation Algorithm

The BasePlanner performs comprehensive validation when a plan is created.

### Cycle Detection

Uses depth-first search to detect cycles:

```go
func (sr *BasePlanner) validateTaskChain(task Task, visited map[string]bool) error {
    if visited[task.Name()] {
        return errors.Errorf("cyclic dependency detected: task %s appears in its own execution path", task.Name())
    }
    visited[task.Name()] = true
    // Recursively validate next tasks
    // ...
}
```

**Algorithm:**
1. Start from the first task
2. Track visited tasks in a map
3. For each task, recursively visit Next, Fail, Then, Else, or fork branches
4. If we encounter a task already in the visited map, report cycle error

### Convergence Validation

Ensures all branches converge to the same EndTask:

```go
func findEndTask(task Task) *EndTask {
    // Traverse Next path until we hit an EndTask
    // ...
}
```

**Algorithm:**
1. For tasks with multiple paths (ForkTask, ConditionTask, tasks with Next/Fail):
   - Find the EndTask for each branch
   - Verify all branches converge to the same EndTask
2. Report error if branches converge to different EndTasks

### Executor Validation

Validates executor registration and function signatures:

```go
func (sr *BasePlanner) validateExecutorSignature(executor *Executor, taskType TaskType) error {
    // Use reflection to inspect function signature
    // Validate parameter types and return types
    // ...
}
```

**Checks:**
- Executor is registered in ExecutorRegistry
- Function signature matches task type requirements:
  - ConditionTask: Returns (bool, error)
  - CallbackTask ExecutionFn: Returns (string, error)
  - CallbackTask ResultProcessorFn: Takes (context.Context, interface{}, string, string) and returns (interface{}, error)
  - ChildWorkflowTask: Returns (ChildTaskInfo, error)

## Interface-Based Design

### Framework Independence

TEF's core abstractions have zero dependencies on any orchestration engine:

- `Planner` interface: Framework-agnostic task creation
- `Registry` interface: Framework-agnostic plan definition
- `Task` types: Pure data structures with no framework dependencies
- `BasePlanner`: Validation logic independent of execution engine

The only place framework-specific code lives is in the `PlannerManager` implementation (e.g., `planners/temporal/`).

### Implementing a New Orchestration Engine

To integrate TEF with an orchestration engine:

1. **Create implementation directory**: `pkg/cmd/tef/planners/myengine/`

2. **Implement PlannerManager**:
   ```go
   type manager struct {
       basePlanner *planners.BasePlanner
       // Your engine-specific client/config
   }

   func (m *manager) StartWorker(ctx context.Context, planVariant string) error {
       // Start your engine's worker
   }

   func (m *manager) ExecutePlan(ctx context.Context, input interface{}, planID string) (string, error) {
       // Execute using your engine
   }

   // ... implement other methods
   ```

3. **Translate task graph to engine's format**:
   - Read from `basePlanner.TasksRegistry`
   - Traverse the task graph starting from `basePlanner.First`
   - Map TEF task types to your engine's constructs

4. **Implement CLI initialization**:
   ```go
   func initializeWorkerCLI(ctx context.Context, rootCmd *cobra.Command, registries []planners.Registry) {
       for _, r := range registries {
           manager, err := myengine.NewPlannerManager(ctx, r, myEngineConfig)
           // Generate commands for this plan
       }
   }
   ```

**Key Insight**: Plan definitions, task types, validation logic, and BasePlanner remain unchanged when switching engines. Only the PlannerManager implementation needs to be engine-specific.

## Extending TEF

### Adding a New Task Type

1. Define the task struct in `pkg/cmd/tef/planners/tasks.go`:
   ```go
   type MyCustomTask struct {
       stepTask  // or baseTask
       // Custom fields
   }

   func (t *MyCustomTask) Type() TaskType { return TaskTypeMyCustom }
   func (t *MyCustomTask) validate() error { /* validation logic */ }
   ```

2. Add factory method to `Planner` interface:
   ```go
   type Planner interface {
       // ... existing methods
       NewMyCustomTask(ctx context.Context, name string) *MyCustomTask
   }
   ```

3. Implement in `BasePlanner`:
   ```go
   func (sr *BasePlanner) NewMyCustomTask(ctx context.Context, name string) *MyCustomTask {
       task := &MyCustomTask{}
       task.taskName = name
       sr.registerTask(ctx, task)
       return task
   }
   ```

4. Update validation logic in `BasePlanner.validateTaskChain()`

5. Implement execution logic in your orchestration engine implementation

### Adding a New Plan

See [README.md](README.md#creating-your-first-plan) and [plans/README.md](plans/README.md) for complete instructions.

## Design Principles

1. **Framework Independence**: Core abstractions have no dependencies on any orchestration engine
2. **Comprehensive Validation**: Plan correctness enforced at creation time (cycle detection, convergence validation)
3. **Extensibility**: New plans, tasks, and implementations can be added without modifying core code
4. **Type Safety**: Executor function signatures validated using reflection
5. **Composability**: Plans can embed other plans using `ChildWorkflowTask`
6. **Asynchronous Support**: CallbackTask enables integration with external systems
7. **Workflow Versioning**: Support for evolving workflows over time

## Additional Resources

* **[README.md](README.md)**: Overview and getting started
* **[Plan Development Guide](plans/README.md)**: Creating custom plans
* **[CLI Reference](CLI.md)**: Command-line interface
* **[API Reference](API.md)**: REST API documentation
* **[TODO List](TODO.md)**: Pending implementation work

## Key Files for Code Navigation

* `planners/definitions.go`: All interface definitions
* `planners/planner.go`: BasePlanner implementation and validation
* `planners/tasks.go`: Task type definitions
* `planners/status.go`: Execution status types
* `plans/registry.go`: Plan registration
* `cli/commands.go`: CLI command generation (TODO)
