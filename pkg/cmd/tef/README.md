# Task Execution Framework (TEF)

TEF is a workflow orchestration framework that provides core interfaces and abstractions for defining, validating, and executing complex multi-step workflows. It provides a framework-agnostic foundation for building task execution systems with support for sequential execution, parallel execution, conditional branching, failure handling, and workflow validation.

**Current Status**: This is the core framework implementation containing interfaces and base planner logic. Specific orchestration engine implementations (e.g., Temporal), REST API, and CLI commands are not yet implemented.

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Core Components](#core-components)
- [Current Implementation Status](#current-implementation-status)
- [For Plan Developers](#for-plan-developers)
- [Next Steps](#next-steps)

## Architecture Overview

TEF is designed as a layered, framework-agnostic architecture that separates workflow definition from execution:

1. **Core Abstractions** (`pkg/cmd/tef/planners/`): Framework-agnostic interfaces for planners, tasks, and executors
2. **BasePlanner** (`pkg/cmd/tef/planners/planner.go`): Task graph validation with cycle detection and convergence checks
3. **Task Types** (`pkg/cmd/tef/planners/tasks.go`): Seven task types for building workflows
4. **Plan Registry** (`pkg/cmd/tef/plans/`): Central registration for plan implementations
5. **CLI** (`pkg/cmd/tef/cli/`): Command-line interface skeleton (implementation pending)

```
┌────────────────────────────────────────────────────────┐
│                Core Abstractions                       │
│  - Planner interface                                   │
│  - Registry interface                                  │
│  - PlanExecutor & SharedPlanService interfaces         │
│  - Task type definitions                               │
└────────────────────┬───────────────────────────────────┘
                     │
┌────────────────────┴───────────────────────────────────┐
│              BasePlanner Implementation                │
│  - Task registration and validation                    │
│  - Cycle detection                                     │
│  - Convergence validation                              │
│  - Executor registration                               │
└────────────────────┬───────────────────────────────────┘
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

For detailed architecture documentation, code flow diagrams, and extension guides, see [architecture.md](architecture.md).

## Core Components

### Interfaces and Abstractions

The core of TEF is a set of framework-agnostic interfaces defined in `pkg/cmd/tef/planners/definitions.go`:

- **`Planner`**: Interface for building task execution plans with methods to create tasks and register executors
- **`Registry`**: Interface that plan implementations must satisfy to provide plan metadata and generate task graphs
- **`PlanExecutor`**: Interface for plan-specific execution operations (starting workers, executing plans)
- **`SharedPlanService`**: Interface for plan-agnostic operations (status queries, execution lists)
- **`PlannerManager`**: Combined interface extending both PlanExecutor and SharedPlanService

### BasePlanner

The `BasePlanner` (`pkg/cmd/tef/planners/planner.go`) provides:

- Task creation and registration
- Executor registration
- Comprehensive plan validation:
  - Cycle detection using depth-first traversal
  - Convergence validation ensuring all branches lead to common EndTasks
  - Executor registration verification
  - Task configuration validation

### Task Types

Seven task types are defined in `pkg/cmd/tef/planners/tasks.go`:

1. **ExecutionTask**: Runs an executor function
2. **ForkTask**: Executes multiple branches in parallel
3. **IfTask**: Conditional branching based on boolean results
4. **SleepTask**: Delays execution for a computed duration
5. **AsyncTask**: Starts async work and waits for external completion
6. **ChildWorkflowTask**: Executes a child plan synchronously
7. **EndTask**: Marks termination of an execution path

For detailed descriptions, requirements, and examples of each task type, see the [Plan Development Guide](plans/README.md#task-types).

## Current Implementation Status

### Implemented

✅ Core interfaces and type definitions
✅ BasePlanner with full validation logic
✅ All seven task types with validation
✅ Plan registry infrastructure
✅ Utility functions for plan ID management
✅ Status and execution tracking types
✅ Logger interface and implementation

### Not Yet Implemented

❌ **PlannerManager Implementation**: No concrete implementation of the execution runtime
❌ **CLI Commands**: Command generation skeleton exists but not implemented (`cli/commands.go`)
❌ **Plan Implementations**: Plan registry exists but no plans registered (`plans/registry.go`)
❌ **Orchestration Engine Integration**: No Temporal, Airflow, or other engine bindings
❌ **REST API**: No HTTP server or API endpoints
❌ **Worker Runtime**: No worker implementation

### Prerequisites for a Working System

To make TEF functional, the following components need to be implemented:

### 1. Orchestration Engine Integration

TEF requires a PlannerManager implementation that integrates with an orchestration engine:

**Example: Temporal Integration**

```go
// pkg/cmd/tef/planners/temporal/manager.go
type TemporalManager struct {
    basePlanner *BasePlanner
    client      temporal.Client
    // ...
}

func (m *TemporalManager) StartWorker(ctx context.Context, planID string) error {
    // Create temporal worker
    // Register workflow and activities
    // Start listening for executions
}

func (m *TemporalManager) ExecutePlan(ctx context.Context, input interface{}, planID string) (string, error) {
    // Start temporal workflow execution
}
```

**Other Options**: Airflow, Cadence, custom scheduler, or any orchestration engine

### 2. CLI Command Implementation

The CLI skeleton exists in `pkg/cmd/tef/cli/commands.go` but `initializeWorkerCLI` needs implementation:

```go
// TODO in cli/commands.go:
func initializeWorkerCLI(ctx context.Context, rootCmd *cobra.Command, registries []planners.Registry) {
    // For each plan registry:
    //   - Create start-worker <planname> command
    //   - Create execute <planname> command
    //   - Create gen-view <planname> command
    //   - Add commands to rootCmd
}
```

### 3. Plan Implementations

Plans need to be created and registered in `pkg/cmd/tef/plans/registry.go`:

```go
// TODO in plans/registry.go:
func RegisterPlans(pr *planners.PlanRegistry) {
    demo.RegisterDemoPlans(pr)
    pua.RegisterPUAPlans(pr)
    myplan.RegisterMyPlanPlans(pr)
    // ... more plans
}
```

Each plan must implement the `planners.Registry` interface and provide a `RegisterXxxPlans(pr *planners.PlanRegistry)` function. See the [Plan Development Guide](plans/README.md#plan-registration) for details.

### 4. Build TEF

```bash
# From the cockroach repository root
./dev build tef

# The binary will be available at: ./bin/tef
```

**Note**: Currently, building TEF will produce a binary with minimal functionality since the orchestration engine, CLI commands, and plans are not implemented.

## Next Steps

To make TEF fully functional, implement the following components in order:

### Step 1: Implement PlannerManager

Create a concrete implementation of the `PlannerManager` interface that integrates with your chosen orchestration engine (e.g., Temporal, Airflow):

1. Create directory `pkg/cmd/tef/planners/temporal/` (or your chosen engine)
2. Implement `StartWorker()` to start the orchestration engine's worker
3. Implement `ExecutePlan()` to submit workflows for execution
4. Implement `GetExecutionStatus()` to query workflow status
5. Implement `ListExecutions()` to list workflow executions
6. Implement `ListAllPlanIDs()` to discover active plans
7. Implement `ResumeTask()` to signal async tasks

### Step 2: Implement CLI Commands

Complete the CLI command generation in `pkg/cmd/tef/cli/commands.go`:

1. Implement `initializeWorkerCLI()` function
2. For each registered plan, generate commands:
   - `start-worker <planname>`: Start a worker for the plan
   - `execute <planname>`: Execute the plan with JSON input
   - `gen-view <planname>`: Generate workflow visualization
   - `resume <planname>`: Resume async tasks (if applicable)

### Step 3: Create Plan Implementations

1. Create plan packages under `pkg/cmd/tef/plans/`
2. Implement the `Registry` interface for each plan:
   - `GetPlanName()`: Return unique plan identifier
   - `GetPlanDescription()`: Return plan description
   - `GetWorkflowVersion()`: Return workflow version
   - `GeneratePlan()`: Build task execution graph
   - `ParsePlanInput()`: Parse and validate JSON input
   - `PrepareExecution()`: Set up plan resources
   - `AddStartWorkerCmdFlags()`: Add plan-specific CLI flags
3. Register plans in `pkg/cmd/tef/plans/registry.go`

### Step 4: Test the Implementation

Once all components are implemented:

1. Build TEF: `./dev build tef`
2. Start the orchestration engine (e.g., `temporal server start-dev`)
3. Start a worker: `./bin/tef start-worker <planname> --plan-variant dev`
4. Execute a plan: `./bin/tef execute <planname> '{"param": "value"}' dev`
5. Monitor execution status

## For Plan Developers

If you want to **create your own plans**, refer to the detailed plan development guide:

📖 **[Plan Development Guide](plans/README.md)**

The plan development guide covers:
- Plan structure and Registry interface
- Task types (Execution, Fork, If, Sleep, Async, Child Workflow, End)
- Executor functions and registration
- Workflow validation rules
- Examples and best practices

### Plan Creation Checklist

When implementing a new plan:

1. ✅ Create a new package under `pkg/cmd/tef/plans/myplan/`
2. ✅ Implement the `planners.Registry` interface
3. ✅ Define your input data structure (must be JSON-parseable)
4. ✅ Create executor functions that perform the actual work
5. ✅ Build the task graph in `GeneratePlan()` using the provided `Planner`
6. ✅ Register executors before referencing them in tasks
7. ✅ Ensure all execution paths converge to a common `EndTask`
8. ✅ Register your plan in `pkg/cmd/tef/plans/registry.go`
9. ✅ Test plan validation by creating a BasePlanner instance

### Example Plan Structure

Plans implement the `planners.Registry` interface with 7 required methods:

```go
type MyPlan struct{}

var _ planners.Registry = &MyPlan{}

func (m *MyPlan) PrepareExecution(ctx context.Context) error { ... }
func (m *MyPlan) GetPlanName() string { return "myplan" }
func (m *MyPlan) GetPlanDescription() string { ... }
func (m *MyPlan) GetWorkflowVersion() int { return 1 }
func (m *MyPlan) ParsePlanInput(input string) (interface{}, error) { ... }
func (m *MyPlan) AddStartWorkerCmdFlags(cmd *cobra.Command) { ... }
func (m *MyPlan) GeneratePlan(ctx context.Context, p planners.Planner) {
    // Register executors, create tasks, wire them together
}
```

For complete plan structure examples with detailed implementations, see the [Plan Development Guide](plans/README.md#plan-structure).

## Available Commands

Once CLI command generation is implemented, TEF will automatically generate commands for each registered plan. Key commands include:

### Core Commands

```bash
# Start a worker
./bin/tef start-worker <planname> --plan-variant <variant>

# Execute a plan
./bin/tef execute <planname> '<json-input>' <plan-variant>

# Visualize workflow
./bin/tef gen-view <planname>

# Resume async tasks
./bin/tef resume <planname> <plan-id> <workflow-id> <step-id> '<result-json>'

# Start REST API server
./bin/tef serve --port 25780
```

For complete CLI command reference with all options, flags, and examples, see the [Plan Development Guide](plans/README.md#cli-commands).

## Key Design Principles

### Framework Independence

TEF's core abstractions (`Planner`, `Registry`, `Task`) have no dependencies on any specific orchestration engine. The entire execution engine can be swapped by implementing the `PlannerManager` interface for a different framework.

### Comprehensive Validation

Plan validation happens at creation time through the `BasePlanner`:

- **Cycle Detection**: Depth-first traversal detects circular dependencies in task graphs
- **Convergence Validation**: All branches (Fork, If, Next/Fail paths) must converge to common EndTasks
- **Executor Verification**: All referenced executors must be registered before use
- **Type Safety**: Executor function signatures are validated at plan creation time

### Workflow Versioning

Plans support workflow versioning via `GetWorkflowVersion()`:

- Increment the version when making backward-incompatible changes
- Old running workflows continue with their original logic
- New workflows use the updated logic
- Default version is 1

##Troubleshooting

### Plan Validation Errors

When creating a `BasePlanner` instance, validation errors will cause panics. Common validation errors:

**Cyclic Dependencies:**
```
panic: cyclic dependency detected: task <task-name> appears in its own execution path
```
*Solution:* Review your task graph to ensure no task directly or indirectly references itself.

**Convergence Errors:**
```
panic: fork task <fork-name> has forked tasks that converge to different EndTasks
panic: if task <if-name> has Then and Else branches that converge to different EndTasks
panic: task <task-name> has Next and Fail paths that converge to different EndTasks
```
*Solution:* Ensure all branches lead to the same `EndTask`. All execution paths in a plan must eventually reach a common termination point.

**Missing Executors:**
```
panic: executor function for task <task-name> is not registered in ExecutorRegistry
```
*Solution:* Call `p.RegisterExecutor(ctx, executor)` for all executors before referencing them in tasks.

**Invalid Executor Signatures:**
```
panic: executor for if task <task-name> must return (bool, error), got <actual-signature>
panic: executor for sleep task <task-name> must return (time.Duration, error), got <actual-signature>
panic: execution function for async task <task-name> must return (string, error), got <actual-signature>
```
*Solution:* Ensure executor function signatures match task type requirements. See the [Plan Development Guide](plans/README.md) for details.

### Testing Your Plan

Test plan validation without a full orchestration engine:

```go
func TestMyPlan(t *testing.T) {
    ctx := context.Background()
    plan := myplan.NewMyPlan()

    // This will panic if validation fails
    basePlanner, err := planners.NewBasePlanner(ctx, plan)
    require.NoError(t, err)
    require.NotNil(t, basePlanner)

    // Verify task structure
    require.NotNil(t, basePlanner.First)
    require.NotNil(t, basePlanner.Output)
    require.Greater(t, len(basePlanner.TasksRegistry), 0)
    require.Greater(t, len(basePlanner.ExecutorRegistry), 0)
}
```

## Additional Resources

- **Architecture Documentation**: [architecture.md](architecture.md) - Detailed architecture and code flow documentation
- **Plan Development Guide**: [plans/README.md](plans/README.md) - Comprehensive guide for creating plans
- **CockroachDB Development**: [/CLAUDE.md](/CLAUDE.md) - CockroachDB development guidelines and best practices
- **Core Interfaces**: `pkg/cmd/tef/planners/definitions.go` - Framework interface definitions
- **Task Types**: `pkg/cmd/tef/planners/tasks.go` - All task type definitions and validation
- **BasePlanner**: `pkg/cmd/tef/planners/planner.go` - Core planner implementation and validation logic
