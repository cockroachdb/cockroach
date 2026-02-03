# TEF Architecture

This document explains the architecture and code organization of the Task Execution Framework (TEF), making it easier to understand the code flow and extend the system.

**Current Status**: This document describes the framework-agnostic core of TEF. Orchestration engine implementations (e.g., Temporal), REST API, and CLI command generation are not yet implemented. For current implementation status and getting started guide, see [README.md](README.md).

## Table of Contents

- [Architectural Overview](#architectural-overview)
- [Core Concepts](#core-concepts)
- [Directory Structure](#directory-structure)
- [Code Flow](#code-flow)
- [Interface-Based Design](#interface-based-design)
- [Extending TEF](#extending-tef)

## Architectural Overview

TEF follows a **layered, interface-based architecture** that separates workflow definition from execution. The current implementation includes the core framework-agnostic layer:

```
┌─────────────────────────────────────────────────────────────┐
│                   CLI Layer                                 │
│  (pkg/cmd/tef/cli/)                                         │
│  - Command generation skeleton exists                       │
│  - Implementation pending in initializeWorkerCLI()          │
└────────────────────┬────────────────────────────────────────┘
                     │
┌────────────────────┴────────────────────────────────────────┐
│            Core Abstraction Layer                           │
│  (pkg/cmd/tef/planners/)                                    │
│  ✅ Planner interface                                       │
│  ✅ Registry interface                                      │
│  ✅ PlanExecutor & SharedPlanService interfaces             │
│  ✅ BasePlanner (task graph validation)                     │
│  ✅ Task type definitions (7 types)                         │
│  ✅ Status and execution tracking types                     │
│  ✅ Utility functions                                       │
└────────────────────┬────────────────────────────────────────┘
                     │
          ┌──────────┴──────────┐
          │                     │
┌─────────┴───────────┐  ┌──────┴──────────────────────────────┐
│  Plans              │  │  Execution Engine                   │
│  (pkg/cmd/tef/plans)│  │  (e.g., planners/temporal/)         │
│  - Plan defs        │  │  - PlannerManager implementation    │
│  - Executors        │  │  - Worker runtime                   │
│  - Registry stub    │  │  - Plan execution                   │
└─────────────────────┘  └─────────────────────────────────────┘
```

**Legend**:
- ✅ **Implemented**: Core framework with full validation
- ❌ **TODO**: Orchestration engine, plans, CLI implementation

## Core Concepts

### 1. Plans

A **Plan** is a workflow definition that describes a sequence of tasks to execute. Plans are defined by implementing the `Registry` interface:

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

**Location**: `pkg/cmd/tef/plans/`

**Status**: TODO - Registry stub exists but no plan implementations

For complete plan development guide with examples, see [plans/README.md](plans/README.md).

### 2. Tasks

**Tasks** are the building blocks of plans. They represent individual steps in a workflow. Seven task types are available:

| Task Type | Purpose | Key Characteristics |
|-----------|---------|---------------------|
| **ExecutionTask** | Run executor function | Accepts params, has Next/Fail paths |
| **ForkTask** | Parallel execution | Multiple branches, must converge |
| **IfTask** | Conditional branching | Boolean executor, Then/Else paths |
| **SleepTask** | Delayed execution | Duration-based pause |
| **AsyncTask** | External async work | ExecutionFn + ResultProcessorFn |
| **ChildWorkflowTask** | Execute child plan | Synchronous sub-workflow |
| **EndTask** | Path termination | All paths must end here |

**Location**: `pkg/cmd/tef/planners/tasks.go`

For detailed task type descriptions, requirements, validation rules, and code examples, see [plans/README.md - Task Types](plans/README.md#task-types).

### 3. Executors

**Executors** are functions that perform actual work. They are registered with a plan and invoked by tasks:

```go
type Executor struct {
    Name        string
    Description string
    Func        interface{}
    Idempotent  bool
    Deprecated  bool
}
```

**Location**: Defined in `pkg/cmd/tef/planners/definitions.go`, registered in plan implementations

For executor function patterns, signatures, and registration examples, see [plans/README.md - Executor Functions](plans/README.md#executor-functions).

### 4. Planner

The **Planner** interface provides methods for building task execution graphs:

```go
type Planner interface {
    RegisterExecutor(ctx context.Context, executor *Executor)
    RegisterPlan(ctx context.Context, first, output Task)
    NewExecutionTask(ctx context.Context, name string) *ExecutionTask
    NewForkTask(ctx context.Context, name string) *ForkTask
    NewIfTask(ctx context.Context, name string) *IfTask
    NewSleepTask(ctx context.Context, name string) *SleepTask
    NewAsyncTask(ctx context.Context, name string) *AsyncTask
    NewChildWorkflowTask(ctx context.Context, name string) *ChildWorkflowTask
    NewEndTask(ctx context.Context, name string) *EndTask
}
```

**Implementation**: `BasePlanner` in `pkg/cmd/tef/planners/planner.go`

**Status**: ✅ Fully implemented

**Responsibilities**:
- Task creation and registration
- Executor registration
- Plan validation (cycle detection, convergence validation)
- Framework-agnostic workflow representation

### 5. PlannerManager

The **PlannerManager** interface handles the execution runtime:

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

**Location**: `pkg/cmd/tef/planners/definitions.go`

**Status**: ✅ Interface definitions complete, ❌ No concrete implementations

## Directory Structure

```
pkg/cmd/tef/
├── main.go                      # ✅ Entry point
├── README.md                    # ✅ User documentation (updated)
├── architecture.md              # ✅ This file (updated)
│
├── cli/                         # ⚠️  CLI command generation (skeleton only)
│   ├── commands.go              # ❌ TODO: Implement initializeWorkerCLI()
│   └── initialiser.go           # ✅ CLI initialization structure
│
├── planners/                    # ✅ Core abstractions (fully implemented)
│   ├── definitions.go           # ✅ Executor, PlannerManager, Registry interfaces
│   ├── planner.go               # ✅ BasePlanner implementation
│   ├── tasks.go                 # ✅ Task type definitions (7 types)
│   ├── status.go                # ✅ Execution status types
│   ├── utils.go                 # ✅ Helper functions
│   ├── logger.go                # ✅ Logger interface and implementation
│   ├── plan_registry.go         # ✅ Plan registry management
│   ├── *_test.go                # ✅ Comprehensive tests
│   │
│   └── mock/                    # ✅ Generated mocks for testing
│       └── *.go                 # Mock implementations
│
└── plans/                       # ❌ Plan definitions (TODO)
    └── registry.go              # ⚠️  Registry stub (RegisterPlans not implemented)

NOT YET CREATED:
├── planners/temporal/           # ❌ TODO: Temporal-specific implementation
│   ├── manager.go               # ❌ Implement PlannerManager for Temporal
│   ├── workflow.go              # ❌ Temporal workflow definitions
│   └── status.go                # ❌ Temporal status queries
│
├── plans/myplan/                # ❌ TODO: Example plan implementations
│   └── plan.go                  # ❌ Implement Registry interface
│
└── api/                         # ❌ TODO: REST API (optional)
    ├── server.go                # ❌ HTTP server
    └── handlers/                # ❌ API handlers
```

**Legend**:
- ✅ **Implemented**: Complete and functional
- ⚠️  **Partial**: Structure exists but needs implementation
- ❌ **TODO**: Not yet created or implemented

## Code Flow

### 1. Plan Definition Flow (TODO)

This flow shows how plan authors will create and register plans once the infrastructure is complete:

```
Plan Author
    │
    ├─► ❌ TODO: Implements Registry interface (plans/myplan/plan.go)
    │   ├─ GetPlanName()
    │   ├─ GetPlanDescription()
    │   ├─ GetWorkflowVersion()
    │   ├─ ParsePlanInput()
    │   ├─ GeneratePlan(ctx, Planner)
    │   ├─ PrepareExecution(ctx)
    │   └─ AddStartWorkerCmdFlags(cmd)
    │
    └─► ❌ TODO: Registers in RegisterPlans() (plans/registry.go)
```

**Current Status**: Registry stub exists but RegisterPlans() is not implemented.

### 2. Worker Startup Flow (TODO)

This flow shows how workers will start once the PlannerManager is implemented:

```
❌ TODO: ./bin/tef start-worker myplan --plan-variant dev
    │
    ├─► ❌ TODO: CLI Command (cli/commands.go - initializeWorkerCLI)
    │
    ├─► ❌ TODO: NewPlannerManager(registry, orchestrationConfig)
    │   │   (e.g., planners/temporal/manager.go)
    │   │
    │   └─► ✅ IMPLEMENTED: NewBasePlanner(ctx, registry)
    │       │   (planners/planner.go)
    │       │
    │       ├─► ✅ registry.GeneratePlan(ctx, planner)
    │       │   ├─ Create tasks using Planner interface
    │       │   ├─ Register executors
    │       │   └─ Define task graph
    │       │
    │       └─► ✅ Validate plan (COMPREHENSIVE VALIDATION)
    │           ├─ Check for cycles using DFS
    │           ├─ Ensure convergence to common EndTasks
    │           ├─ Validate executor registration
    │           ├─ Validate executor function signatures
    │           └─ Validate task configuration
    │
    └─► ❌ TODO: manager.StartWorker(ctx, planID)
        │   (PlannerManager implementation)
        │
        ├─► Connect to orchestration engine
        ├─► Create worker for task queue
        ├─► Register workflows and activities
        └─► Start listening for executions
```

**Implemented**: BasePlanner creation and full validation
**TODO**: PlannerManager implementation, CLI commands

### 3. Plan Execution Flow (TODO)

This flow shows how plans will be executed once the implementation is complete:

```
❌ TODO: ./bin/tef execute myplan '{"param": "value"}' dev
    │
    ├─► ❌ TODO: CLI Command (cli/commands.go - initializeWorkerCLI)
    │
    ├─► ✅ IMPLEMENTED: registry.ParsePlanInput(inputJSON)
    │
    ├─► ❌ TODO: NewPlannerManager(registry, orchestrationConfig)
    │
    └─► ❌ TODO: manager.ExecutePlan(ctx, input, planID)
        │   (PlannerManager implementation)
        │
        ├─► Connect to orchestration engine
        ├─► Check for active workers on task queue
        ├─► Generate unique workflow ID
        └─► Start workflow execution
            │
            └─► Worker picks up workflow
                │   (Engine-specific workflow implementation)
                │
                └─► Execute task graph using BasePlanner structure
                    ├─ Run executors based on task types
                    ├─ Handle forks (parallel execution)
                    ├─ Handle conditionals (if/then/else)
                    ├─ Handle sleeps (duration-based delays)
                    ├─ Handle async tasks (wait for signals)
                    ├─ Handle child workflows (synchronous sub-plans)
                    └─ Follow Next/Fail paths
```

**Implemented**: Plan input parsing, BasePlanner with task graph
**TODO**: PlannerManager implementation, workflow execution runtime

### 4. REST API Flow (TODO - Optional)

This flow shows how the REST API will work once implemented:

```
❌ TODO: ./bin/tef serve --port 25780
    │
    ├─► ❌ TODO: Create managers for all plans
    │
    └─► ❌ TODO: Start HTTP server (api/server.go)
        │
        ├─► Register routes
        │   ├─ POST /v1/plans/{planID}/executions
        │   ├─ GET  /v1/plans/{planID}/executions
        │   ├─ GET  /v1/plans/{planID}/executions/{workflowID}
        │   ├─ POST /v1/plans/{planID}/executions/{executionID}/steps/{stepID}/resume
        │   ├─ GET  /v1/plans
        │   └─ GET  / (UI)
        │
        └─► Listen for requests
            │
            └─► Forward to PlannerManager implementation
```

**Status**: REST API is not yet implemented

## REST API Reference

The REST API provides HTTP endpoints for executing and monitoring plans. All endpoints return JSON responses.

### Endpoints Overview

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/v1/plans` | List all available plans |
| GET | `/v1/plans/{plan_id}/executions` | List executions for a plan |
| POST | `/v1/plans/{plan_id}/executions` | Execute a plan |
| GET | `/v1/plans/{plan_id}/executions/{execution_id}` | Get execution status |

### 1. List Plans

**Endpoint**: `GET /v1/plans`

**Description**: Returns all available plan instances registered with the server.

**Request**: No body required

**Response** (200 OK):
```json
{
  "plans": [
    {
      "plan_id": "demo-dev",
      "plan_name": "demo",
      "plan_description": "A demo plan showcasing TEF features"
    }
  ]
}
```

**Response Fields**:
- `plans`: Array of plan information objects
  - `plan_id`: Unique identifier for the plan instance (format: `{plan_name}-{variant}`)
  - `plan_name`: The plan's base name
  - `plan_description`: Human-readable description from the plan's Registry

**Error Response** (500):
```json
{
  "error": "failed to list plans: <error details>"
}
```

### 2. Execute Plan

**Endpoint**: `POST /v1/plans/{plan_id}/executions`

**Description**: Starts a new execution of the specified plan.

**Path Parameters**:
- `plan_id`: Plan identifier (e.g., "demo-dev")

**Request Body**:
```json
{
  "request": {
    "param1": "value1",
    "param2": 123,
    "nested": {
      "field": "value"
    }
  },
  "flags": {
    "flag1": "value1"
  }
}
```

**Request Fields**:
- `request`: Plan-specific input parameters (structure varies per plan)
- `flags`: Optional execution flags (currently unused, reserved for future use)

**Response** (200 OK):
```json
{
  "workflow_id": "demo-20250126-123456-abc123",
  "plan_id": "demo-dev"
}
```

**Response Fields**:
- `workflow_id`: Unique identifier for this execution instance
- `plan_id`: The plan that was executed

**Error Responses**:

400 Bad Request - Invalid plan ID:
```json
{
  "error": "invalid plan ID: <error details>"
}
```

400 Bad Request - Invalid request body:
```json
{
  "error": "invalid request body: <error details>"
}
```

400 Bad Request - Failed to parse plan input:
```json
{
  "error": "failed to parse plan input: <error details>"
}
```

404 Not Found - Plan doesn't exist:
```json
{
  "error": "plan demo not found"
}
```

500 Internal Server Error - Execution failed:
```json
{
  "error": "failed to execute plan: <error details>"
}
```

### 3. List Executions

**Endpoint**: `GET /v1/plans/{plan_id}/executions`

**Description**: Returns all executions for a specific plan instance.

**Path Parameters**:
- `plan_id`: Plan identifier (e.g., "demo-dev")

**Request**: No body required

**Response** (200 OK):
```json
{
  "executions": [
    {
      "workflow_id": "demo-20250126-123456-abc123",
      "run_id": "temporal-run-id-xyz",
      "status": "Running",
      "start_time": "2025-01-26T12:34:56Z"
    },
    {
      "workflow_id": "demo-20250126-120000-def456",
      "run_id": "temporal-run-id-uvw",
      "status": "Completed",
      "start_time": "2025-01-26T12:00:00Z"
    }
  ],
  "plan_id": "demo-dev",
  "plan_name": "demo"
}
```

**Response Fields**:
- `executions`: Array of execution summary objects
  - `workflow_id`: Unique identifier for the execution
  - `run_id`: Framework-specific execution ID (e.g., Temporal run ID)
  - `status`: Current status (Running, Completed, Failed, Terminated, etc.)
  - `start_time`: ISO 8601 formatted start time (optional)
- `plan_id`: The plan instance ID
- `plan_name`: The plan's base name

**Error Responses**:

400 Bad Request:
```json
{
  "error": "invalid plan ID: <error details>"
}
```

404 Not Found:
```json
{
  "error": "plan demo not found"
}
```

500 Internal Server Error:
```json
{
  "error": "failed to list executions: <error details>"
}
```

### 4. Get Execution Status

**Endpoint**: `GET /v1/plans/{plan_id}/executions/{execution_id}`

**Description**: Returns detailed status information for a specific execution.

**Path Parameters**:
- `plan_id`: Plan identifier (e.g., "demo-dev")
- `execution_id`: Workflow ID from execution response

**Request**: No body required

**Response** (200 OK):
```json
{
  "workflow_id": "demo-20250126-123456-abc123",
  "status": "Running",
  "current_tasks": ["task2", "task3"],
  "workflow": {
    "name": "demo-dev",
    "description": "A demo plan showcasing TEF features",
    "first_task": "task1",
    "output_task": "output",
    "input": {
      "param1": "value1"
    },
    "tasks": {
      "task1": {
        "name": "task1",
        "type": "ExecutionTask",
        "executor": "printMessage",
        "params": [],
        "next": "task2",
        "fail": "error_handler",
        "status": "Completed",
        "start_time": "2025-01-26T12:34:56Z",
        "end_time": "2025-01-26T12:34:57Z",
        "input": {"message": "Hello"},
        "output": {"result": "Success"}
      },
      "task2": {
        "name": "task2",
        "type": "ForkTask",
        "fork_tasks": ["branch1", "branch2"],
        "next": "task3",
        "status": "InProgress",
        "start_time": "2025-01-26T12:34:57Z"
      },
      "task3": {
        "name": "task3",
        "type": "IfTask",
        "executor": "checkCondition",
        "params": ["task1"],
        "then": "success_task",
        "else": "failure_task",
        "status": "Pending"
      },
      "sleep1": {
        "name": "sleep1",
        "type": "SleepTask",
        "executor": "sleepDuration",
        "params": [],
        "next": "task4",
        "properties": {
          "duration_seconds": 30
        }
      }
    }
  }
}
```

**Response Fields**:
- `workflow_id`: Unique identifier for this execution
- `status`: Overall workflow status (Running, Completed, Failed, etc.)
- `current_tasks`: Array of task names currently executing (for Running workflows)
- `workflow`: Detailed workflow information
  - `name`: Workflow/plan name
  - `description`: Plan description
  - `first_task`: Name of the starting task
  - `output_task`: Name of the task that produces the final output
  - `input`: The input data provided to the workflow
  - `output`: The final output (only present when workflow is Completed)
  - `tasks`: Map of task name to TaskInfo

**TaskInfo Structure**:
- Common fields for all task types:
  - `name`: Task identifier
  - `type`: Task type (ExecutionTask, ForkTask, IfTask, SleepTask, AsyncTask, ChildWorkflowTask, EndTask)
  - `status`: Task execution status (Pending, InProgress, Completed, Failed)
  - `start_time`: ISO 8601 formatted start time (optional)
  - `end_time`: ISO 8601 formatted end time (optional)
  - `input`: Input data passed to the task (optional)
  - `output`: Output data produced by the task (optional)
  - `error`: Error message if task failed (optional)
  - `properties`: Additional task-specific properties (optional)

- ExecutionTask-specific fields:
  - `executor`: Name of the executor function
  - `params`: Array of task names whose outputs are passed as parameters
  - `next`: Name of the next task on success
  - `fail`: Name of the task to execute on failure (optional)

- ForkTask-specific fields:
  - `fork_tasks`: Array of task names to execute in parallel
  - `next`: Name of the task to execute after all branches complete
  - `fail`: Name of the task to execute if any branch fails (optional)

- IfTask-specific fields:
  - `executor`: Name of the boolean executor function
  - `params`: Array of task names whose outputs are passed as parameters
  - `then`: Name of the task to execute if condition is true
  - `else`: Name of the task to execute if condition is false (optional)

- SleepTask-specific fields:
  - `executor`: Name of the duration executor function
  - `params`: Array of task names whose outputs are passed as parameters
  - `next`: Name of the next task after sleep completes
  - `fail`: Name of the task to execute on failure (optional)

- AsyncTask-specific fields:
  - `execution_fn`: Name of the executor that starts async work (returns step ID)
  - `result_processor_fn`: Name of the executor that processes results
  - `params`: Array of task names whose outputs are passed as parameters
  - `next`: Name of the next task after async work completes
  - `fail`: Name of the task to execute on failure (optional)
  - `step_id`: The step ID returned by execution_fn (optional)

- ChildWorkflowTask-specific fields:
  - `executor`: Name of the executor function that returns ChildTaskInfo
  - `params`: Array of task names whose outputs are passed as parameters
  - `child_plan_id`: ID of the child plan to execute (determined at runtime)
  - `child_input`: Input data for the child plan (determined at runtime)
  - `next`: Name of the next task after child plan completes
  - `fail`: Name of the task to execute if child plan fails (optional)

**Error Responses**:

400 Bad Request:
```json
{
  "error": "invalid plan ID: <error details>"
}
```

404 Not Found:
```json
{
  "error": "plan demo not found"
}
```

500 Internal Server Error:
```json
{
  "error": "failed to get execution status: <error details>"
}
```

### Example Usage

**Starting a new execution**:
```bash
curl -X POST http://localhost:25780/v1/plans/demo-dev/executions \
  -H "Content-Type: application/json" \
  -d '{
    "request": {
      "message": "Hello TEF",
      "count": 5
    }
  }'
```

**Checking execution status**:
```bash
curl http://localhost:25780/v1/plans/demo-dev/executions/demo-20250126-123456-abc123
```

**Listing all executions**:
```bash
curl http://localhost:25780/v1/plans/demo-dev/executions
```

**Listing all plans**:
```bash
curl http://localhost:25780/v1/plans
```

## Interface-Based Design

### Why Interfaces Matter

TEF is designed around **clean interfaces** to ensure **framework independence**. The core abstractions (`Planner`, `PlannerManager`, `Registry`, `Task`) have **no dependency on Temporal** or any specific orchestration engine.

### Current Implementation: Temporal

The **only** place where Temporal is referenced is in `pkg/cmd/tef/planners/temporal_planner/`. This directory contains:

- `manager.go`: Implements `PlannerManager` using Temporal client
- `workflow.go`: Translates task graph into Temporal workflow
- `status.go`: Queries Temporal for execution status

### Key Interfaces

#### 1. Registry (Plan Definition)

```go
// Framework-agnostic interface for defining plans
type Registry interface {
    GetPlanName() string
    GetPlanDescription() string
    GeneratePlan(ctx context.Context, p Planner)
    ParsePlanInput(input string) (interface{}, error)
}
```

**No framework dependencies** - Plans don't know about Temporal, Airflow, or any execution engine.

#### 2. PlannerManager (Execution Runtime)

```go
// Framework-agnostic interface for managing execution
type PlannerManager interface {
    StartWorker(ctx context.Context, planVariant string) error
    ExecutePlan(ctx context.Context, input interface{}, planID string) (string, error)
    GetExecutionStatus(ctx context.Context, planID, workflowID string) (*ExecutionStatus, error)
    ListExecutions(ctx context.Context, planID string) ([]*WorkflowExecutionInfo, error)
    ListAllPlans(ctx context.Context) ([]PlanInfo, error)
}
```

**Implementations**:
- Current: `temporal_planner.manager` (Temporal-based)
- Future: Could add `airflow_planner.manager`, `cadence_planner.manager`, etc.

#### 3. BasePlanner (Plan Validation)

The `BasePlanner` is the **framework-agnostic implementation** of the `Planner` interface. It:

- Creates and registers tasks
- Validates the task graph (cycle detection, convergence checks)
- Stores executor registry

**Location**: `pkg/cmd/tef/planners/planner.go`

**Key point**: Validation happens at plan creation time, **before** any framework-specific code runs.

### Implementing an Orchestration Engine

To integrate TEF with an orchestration engine (e.g., Temporal, Airflow, Cadence, custom scheduler):

1. **Create a new implementation directory**: `pkg/cmd/tef/planners/myengine_planner/`

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

3. **Translate task graph to your engine's format**:
   - Read from `basePlanner.TasksRegistry`
   - Traverse the task graph starting from `basePlanner.First`
   - Map TEF task types to your engine's constructs

4. **Implement CLI initialization** (`cli/commands.go`):
   ```go
   func initializeWorkerCLI(ctx context.Context, rootCmd *cobra.Command, registries []planners.Registry) {
       for _, r := range registries {
           // Create PlannerManager instance
           manager, err := myengine_planner.NewPlannerManager(ctx, r, myEngineConfig)

           // Generate commands for this plan
           // - start-worker <planname>
           // - execute <planname>
           // - gen-view <planname>
           // - resume <planname> (if applicable)
       }
   }
   ```

**Key Insight**: Plan definitions, task types, validation logic, and BasePlanner all remain unchanged when switching orchestration engines. Only the PlannerManager implementation and CLI commands need to be engine-specific.

## Extending TEF

### Adding a New Plan

1. Create a new directory: `pkg/cmd/tef/plans/myplan/`

2. Implement the `Registry` interface (7 required methods):
   ```go
   type MyPlan struct{}

   var _ planners.Registry = &MyPlan{}

   func (m *MyPlan) PrepareExecution(ctx context.Context) error { ... }
   func (m *MyPlan) GetPlanName() string { return "myplan" }
   func (m *MyPlan) GetPlanDescription() string { return "Description" }
   func (m *MyPlan) GetWorkflowVersion() int { return 1 }
   func (m *MyPlan) ParsePlanInput(input string) (interface{}, error) { ... }
   func (m *MyPlan) AddStartWorkerCmdFlags(cmd *cobra.Command) { ... }
   func (m *MyPlan) GeneratePlan(ctx context.Context, p planners.Planner) {
       // Create tasks, register executors, define flow
   }
   ```

3. Create a registration function:
   ```go
   func RegisterMyPlanPlans(pr *planners.PlanRegistry) {
       pr.Register(&MyPlan{})
       // Register child plans if needed
   }
   ```

4. Register in `pkg/cmd/tef/plans/registry.go`:
   ```go
   func RegisterPlans(pr *planners.PlanRegistry) {
       myplan.RegisterMyPlanPlans(pr)
       // ... other plans
   }
   ```

5. Rebuild: `./dev build tef`

6. CLI commands are **auto-generated**:
   - `./bin/tef start-worker myplan`
   - `./bin/tef execute myplan`
   - `./bin/tef gen-view myplan`

For complete plan development guide with examples, see [plans/README.md](plans/README.md).

### Adding a New Task Type

1. Define the task struct in `pkg/cmd/tef/planners/tasks.go`:
   ```go
   type MyCustomTask struct {
       stepTask  // or baseTask, depending on whether it has Next/Fail
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

3. Implement in `BasePlanner` (`pkg/cmd/tef/planners/planner.go`):
   ```go
   func (sr *BasePlanner) NewMyCustomTask(ctx context.Context, name string) *MyCustomTask {
       task := &MyCustomTask{}
       task.taskName = name
       sr.registerTask(ctx, task)
       return task
   }
   ```

4. Update validation logic in `BasePlanner.validateTaskChain()` to handle the new type

5. Implement execution logic in `temporal_planner/workflow.go` (or your engine's implementation)

### Adding REST API Endpoints

1. Create a new handler in `pkg/cmd/tef/api/handlers/v1/`:
   ```go
   type MyHandler struct { ... }

   func (h *MyHandler) GetRoutes() []Route {
       return []Route{
           {Path: "/api/v1/my-endpoint", Handler: h.HandleMyRequest, Methods: []string{"GET"}},
       }
   }
   ```

2. Register in `api/server.go`:
   ```go
   func (s *Server) createHandlerRegistries() []HandlerRegistry {
       return []HandlerRegistry{
           v1.NewMyHandler(...),
           // ... existing handlers
       }
   }
   ```

## Summary

TEF's architecture prioritizes:

1. **Framework Independence**: ✅ Core abstractions have no dependencies on any orchestration engine
2. **Comprehensive Validation**: ✅ Plan correctness enforced at creation time (cycle detection, convergence validation)
3. **Extensibility**: ✅ New plans, tasks, and implementations can be added without modifying core code
4. **Type Safety**: ✅ Executor function signatures validated using reflection
5. **Composability**: ✅ Plans can embed other plans using `ChildWorkflowTask`
6. **Asynchronous Support**: ✅ AsyncTask enables integration with external systems
7. **Comprehensive Task Types**: ✅ Seven task types cover most workflow patterns
8. **Workflow Versioning**: ✅ Support for evolving workflows over time

**Current Implementation Status**:
- ✅ **Fully Implemented**: Core interfaces, BasePlanner, all task types, validation, utilities
- ❌ **Not Implemented**: PlannerManager, CLI commands, plan implementations, orchestration engine integration

**The key insight**: TEF provides a complete framework-agnostic workflow definition and validation system. Any orchestration engine (Temporal, Airflow, Cadence, etc.) can be integrated by implementing the `PlannerManager` interface, without changing plan definitions or validation logic.

**Next Steps for Developers**:
1. Implement `PlannerManager` for your chosen orchestration engine
2. Implement CLI command generation in `initializeWorkerCLI()`
3. Create plan implementations following the `Registry` interface
4. Register plans in `plans/registry.go`
