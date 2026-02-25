# Task Execution Framework (TEF)

This package contains the framework-agnostic core of the Task Execution Framework (TEF). The TEF CLI and orchestration-specific implementations are maintained in a separate private repository to avoid bringing Temporal dependencies into the CockroachDB codebase.

## Architecture

TEF is split across two repositories:

### CockroachDB Repository (`pkg/cmd/tef`)

Contains framework-agnostic code and plan implementations:

- **`planners/`**: Core interfaces and framework-agnostic implementations
  - `Registry` interface - plan metadata and task graph generation
  - `Planner` interface - task creation API
  - `PlanExecutor` interface - plan execution operations
  - `SharedPlanService` interface - framework-level operations
  - `PlannerManager` interface - combined executor and service operations
  - `BasePlanner` - validates and manages task graphs
  - Task types - `ExecutionTask`, `ForkTask`, `ConditionTask`, `CallbackTask`, `ChildPlanTask`, `EndTask`
  - `PlanRegistry` - manages Registry instances

- **`plans/`**: Plan implementations (OPTIONAL - can also be in private repo)
  - `RegisterPlans()` - central registration function where plans register themselves
  - Plan implementations here can depend on CockroachDB code (e.g., SQL, KV, cluster)
  - Plans that don't need CockroachDB dependencies can be written in the private repo instead

### Private Repository ([`task-exec-framework`](https://github.com/cockroachlabs/task-exec-framework))

Contains orchestration-specific code and optionally plan implementations:

- **Temporal Implementation**: `PlannerManager` implementation using Temporal workflows
- **CLI**: Commands for `start-worker`, `execute`, `gen-view`, `resume`, etc.
- **Binary**: `tef` executable built from `main.go`
- **Plans (OPTIONAL)**: Plan implementations that don't need CockroachDB dependencies

## Dependency Flow

```
┌─────────────────────────────────────┐
│  cockroach/pkg/cmd/tef              │
│  ┌─────────────┐  ┌──────────────┐  │
│  │  planners/  │  │   plans/     │  │
│  │ (interfaces)│  │(impl Registry)│ │
│  └─────────────┘  └──────────────┘  │
└─────────────────────────────────────┘
                ▲
                │ (imports)
                │
┌───────────────┴─────────────────────┐
│  task-exec-framework (private)      │
│  ┌──────────┐  ┌────────┐  ┌─────┐  │
│  │temporal/ │  │  cli/  │  │main │  │
│  │(manager) │  │(cobra) │  │     │  │
│  └──────────┘  └────────┘  └─────┘  │
│  ┌──────────┐                        │
│  │  plans/  │ (optional)             │
│  └──────────┘                        │
│         +                            │
│  Temporal SDK dependencies           │
└─────────────────────────────────────┘
```

## Where to Write Plans

You have two options for where to implement plans:

### Option 1: Plans in CockroachDB Repo (when you need CockroachDB dependencies)

Write plans in `pkg/cmd/tef/plans/` when they need to:
- Access CockroachDB internals (SQL, KV, cluster APIs)
- Use CockroachDB utilities or packages
- Share code with other CockroachDB components

**Example structure**:
```
pkg/cmd/tef/plans/
├── registry.go          # Calls RegisterPlans for all plans in this repo
├── myplan/
│   ├── BUILD.bazel
│   ├── registry.go      # Implements planners.Registry
│   └── executors.go     # Executor implementations
```

### Option 2: Plans in Private Repo (when you don't need CockroachDB dependencies)

Write plans in the private `task-exec-framework` repo when they:
- Don't need CockroachDB-specific code
- Are simple orchestration workflows
- Need Temporal-specific features
- Should remain private

**Example structure** (in private repo):
```
task-exec-framework/
├── plans/
│   ├── registry.go      # Calls RegisterPlans for all plans in private repo
│   ├── demo/
│   │   ├── registry.go  # Implements planners.Registry
│   │   └── executors.go
```

The private repo's `main.go` would combine both:
```go
import (
    "github.com/cockroachdb/cockroach/pkg/cmd/tef/planners"
    cockroachplans "github.com/cockroachdb/cockroach/pkg/cmd/tef/plans"
    privateplans "github.com/cockroachlabs/task-exec-framework/plans"
)

func main() {
    pr := planners.NewPlanRegistry()
    
    // Register plans from cockroach repo
    cockroachplans.RegisterPlans(pr)
    
    // Register plans from private repo
    privateplans.RegisterPlans(pr)
    
    // Initialize CLI with all plans
    cli.Initialize(pr)
}
```

## How to Write a New Plan

Regardless of which repository you choose, the implementation is the same:

1. **Create a plan package** (e.g., `plans/myplan/`)

2. **Implement the `Registry` interface**:
   ```go
   package myplan

   import (
       "context"
       "github.com/cockroachdb/cockroach/pkg/cmd/tef/planners"
   )

   type MyPlanRegistry struct{}

   func (r *MyPlanRegistry) GetPlanName() string {
       return "my-plan"
   }

   func (r *MyPlanRegistry) GetPlanDescription() string {
       return "Description of what this plan does"
   }

   func (r *MyPlanRegistry) GetPlanVersion() int {
       return 1
   }

   func (r *MyPlanRegistry) PrepareExecution(ctx context.Context) error {
       // Initialize any resources needed
       return nil
   }

   func (r *MyPlanRegistry) GeneratePlan(ctx context.Context, p planners.Planner) {
       // Register executors
       p.RegisterExecutor(ctx, &planners.Executor{
           Name: "my-executor",
           Description: "Does something useful",
           Func: myExecutorFunc,
       })

       // Build task graph
       task1 := p.NewExecutionTask(ctx, "step1").
           WithExecutor(myExecutorFunc).
           WithDescription("First step")

       endTask := p.NewEndTask(ctx, "end")
       task1.Then(endTask)

       // Register the plan with first task and output task
       p.RegisterPlan(ctx, task1, task1)
   }

   func (r *MyPlanRegistry) ParsePlanInput(input string) (interface{}, error) {
       // Parse JSON input into your plan's input type
       return nil, nil
   }

   func (r *MyPlanRegistry) AddStartWorkerCmdFlags(cmd *cobra.Command) {
       // Add plan-specific flags if needed
   }

   func myExecutorFunc(ctx context.Context, info *planners.PlanExecutionInfo, input interface{}) (interface{}, error) {
       // Your execution logic here
       return nil, nil
   }
   ```

3. **Register the plan** in the appropriate `registry.go`:
   ```go
   func RegisterPlans(pr *planners.PlanRegistry) {
       pr.Register(&myplan.MyPlanRegistry{})
   }
   ```

4. **Build and test in the private repository**:
   - The private repo imports both `github.com/cockroachdb/cockroach/pkg/cmd/tef/planners` and `.../plans`
   - Run `go build` in the private repo to build the `tef` binary
   - Use `tef start-worker my-plan`, `tef execute my-plan`, etc.

## Key Interfaces

### Registry Interface

Plans must implement this interface to be registered with TEF:

```go
type Registry interface {
    PrepareExecution(ctx context.Context) error
    GetPlanName() string
    GetPlanDescription() string
    GetPlanVersion() int
    GeneratePlan(ctx context.Context, p Planner)
    ParsePlanInput(input string) (interface{}, error)
    AddStartWorkerCmdFlags(cmd *cobra.Command)
}
```

### Planner Interface

Used during `GeneratePlan()` to build the task graph:

```go
type Planner interface {
    RegisterExecutor(ctx context.Context, executor *Executor)
    RegisterPlan(ctx context.Context, first, output Task)
    NewExecutionTask(ctx context.Context, name string) *ExecutionTask
    NewForkTask(ctx context.Context, name string) *ForkTask
    NewForkJoinTask(ctx context.Context, name string) *ForkJoinTask
    NewConditionTask(ctx context.Context, name string) *ConditionTask
    NewCallbackTask(ctx context.Context, name string) *CallbackTask
    NewChildPlanTask(ctx context.Context, name string) *ChildPlanTask
    NewEndTask(ctx context.Context, name string) *EndTask
}
```

### PlannerManager Interface

Implemented in the **private repository** using Temporal:

```go
type PlannerManager interface {
    // Execution operations
    StartWorker(ctx context.Context, planID string) error
    ExecutePlan(ctx context.Context, input interface{}, planID string) (string, error)

    // Framework-level operations
    GetExecutionStatus(ctx context.Context, planID, workflowID string) (*ExecutionStatus, error)
    ListExecutions(ctx context.Context, planID string) ([]*WorkflowExecutionInfo, error)
    ListAllPlanIDs(ctx context.Context) ([]PlanMetadata, error)
    ResumeTask(ctx context.Context, planID, workflowID, stepID, result string) error
    AddPlannerFlags(cmd *cobra.Command)
    ClonePropertiesFrom(source PlannerManager)
}
```

## Benefits of This Architecture

1. **No Temporal dependency in CockroachDB**: The cockroach repo remains free of Temporal SDK dependencies
2. **Flexible plan location**: Plans can be in either repo based on their dependencies
3. **Plan code can access CockroachDB code**: Plans in the cockroach repo can import CockroachDB packages
4. **Framework-agnostic design**: Could support multiple orchestration backends (Temporal, Cadence, etc.)
5. **Clean separation of concerns**: Core abstractions vs orchestration implementation
6. **Easy testing**: `BasePlanner` validates task graphs without requiring Temporal

## Testing

Tests for framework-agnostic code are in this repository:
- `planners/planner_test.go` - BasePlanner validation
- `planners/tasks_test.go` - Task type validation
- `planners/plan_registry_test.go` - Registry management

Integration tests that require Temporal are in the private repository.

## Repository Links

- **CockroachDB**: https://github.com/cockroachdb/cockroach
- **TEF Private Repo**: https://github.com/cockroachlabs/task-exec-framework
