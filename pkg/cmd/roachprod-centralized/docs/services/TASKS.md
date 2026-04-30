# Tasks Service

The Tasks Service provides a distributed, fault-tolerant background task processing system for roachprod-centralized. It handles asynchronous operations like cluster synchronization, DNS updates, and health checks.

## Architecture Overview

The tasks service has been refactored into a modular architecture with clean separation of concerns:

```
pkg/cmd/roachprod-centralized/services/tasks/
├── service.go                    # Service orchestration and lifecycle
├── api.go                        # Public CRUD operations
├── coordination.go               # Inter-service coordination helpers
├── registry.go                   # Task type registration system
├── operations.go                 # Business operations (called by tasks)
├── internal/                     # Implementation details (encapsulated)
│   ├── processor/
│   │   ├── processor.go          # Worker pool and queue management
│   │   └── executor.go           # Task execution with timeouts
│   ├── scheduler/
│   │   ├── scheduler.go          # Base scheduler interface
│   │   └── purge_scheduler.go    # Periodic purge scheduling
│   └── metrics/
│       └── collector.go          # Metrics collection
├── tasks/                        # Concrete task implementations
│   └── purge.go                  # Task purge implementation
├── types/                        # Public interfaces and DTOs
│   └── types.go                  # IService, ITask, ITasksService interfaces
└── mocks/                        # Auto-generated test mocks
    └── tasks.go
```

## Key Components

### 1. Service Orchestration (`service.go`)

**Purpose**: Main service struct, lifecycle management, and coordination of all components.

**Key Methods**:
- `NewService()` - Creates service instance with options
- `RegisterTasks()` - Called during app initialization
- `StartService()` - Initializes service (performs initial sync if needed)
- `StartBackgroundWork()` - Starts processors, schedulers, and metrics
- `Shutdown()` - Graceful shutdown with WaitGroup synchronization

**Background Work**:
```go
func (s *Service) StartBackgroundWork(ctx context.Context, l *logger.Logger) error {
    // 1. Start task processor (workers poll and execute tasks)
    processor.StartProcessing(ctx, l, errChan, workers, instanceID, repo, s)

    // 2. Start purge scheduler (if workers enabled)
    scheduler.StartPurgeScheduling(ctx, l, errChan, interval, s, onComplete)

    // 3. Start metrics collection (if enabled)
    metrics.StartMetricsCollection(ctx, l, errChan, interval, s, onComplete)
}
```

### 2. Public API (`api.go`)

**Purpose**: CRUD operations exposed via `types.IService` interface.

**Methods**:
- `GetTasks(ctx, l, input)` - Query tasks with filters
- `GetTask(ctx, l, input)` - Get single task by ID
- `CreateTask(ctx, l, task)` - Create new task

**Used by**: Controllers (HTTP endpoints)

### 3. Coordination Helpers (`coordination.go`)

**Purpose**: Helper methods used by other services to coordinate task scheduling.

**Methods**:
- `CreateTaskIfNotAlreadyPlanned(ctx, l, task)` - Prevents duplicate ad-hoc tasks
- `CreateTaskIfNotRecentlyScheduled(ctx, l, task, window)` - Prevents duplicate periodic tasks
- `WaitForTaskCompletion(ctx, l, taskID, timeout)` - Blocks until task completes
- `GetMostRecentCompletedTaskOfType(ctx, l, taskType)` - Query helper

**Used by**: Clusters service, Health service, Public DNS service

### 4. Task Registration (`registry.go`)

**Purpose**: Allows services to register their task types and enables task hydration.

**Key Concepts**:
- **Registration**: Services register their task types during app initialization
- **Hydration**: Converting repository tasks (base `tasks.Task`) to concrete types with service references

**Example**:
```go
// Service registers its tasks
func (s *ClustersService) RegisterTasks(tasksService types.ITasksService) {
    tasksService.RegisterTasksService(s)
}

// Service implements ITasksService interface
func (s *ClustersService) GetTaskServiceName() string {
    return "clusters"
}

func (s *ClustersService) GetHandledTasks() map[string]types.ITask {
    return map[string]types.ITask{
        "cluster_sync": &TaskSync{},
    }
}

func (s *ClustersService) CreateTaskInstance(taskType string) (tasks.ITask, error) {
    if taskType == "cluster_sync" {
        return NewTaskSync(s.clustersService), nil
    }
    return nil, types.ErrUnknownTaskType
}
```

**Hydration Flow**:
1. Worker polls task from database (gets base `tasks.Task`)
2. Worker calls `HydrateTask(baseTask)` on tasks service
3. Tasks service looks up registered service for task type
4. Calls service's `CreateTaskInstance(taskType)` to get concrete type with dependencies
5. Returns hydrated task ready for processing

### 5. Operations (`operations.go`)

**Purpose**: Business operations that task implementations can call.

**Methods**:
- `PurgeTasks(ctx, l)` - Purge old done/failed tasks
- `purgeTasksInState(ctx, l, duration, state)` - Helper for state-specific purging

**Used by**: TaskPurge implementation

### 6. Internal Packages

#### Processor (`internal/processor/`)

**Purpose**: Task queue management and execution.

**Components**:
- `processor.go` - Worker pool, polling, queue coordination
- `executor.go` - Individual task execution with timeout handling

**Key Features**:
- Adaptive polling (100ms when busy, 5s when idle)
- Configurable worker count
- Timeout handling per task
- State transitions (pending → running → done/failed)

**Interface**:
```go
type TaskExecutor interface {
    HydrateTask(base mtasks.ITask) (types.ITask, error)
    MarkTaskAs(ctx, l, id, status) error
    UpdateError(ctx, l, id, errMsg) error
    GetManagedTask(taskType string) types.ITask
    GetMetricsEnabled() bool
    IncrementProcessedTasks()
    GetDefaultTimeout() types.TimeoutGetter
}
```

#### Scheduler (`internal/scheduler/`)

**Purpose**: Periodic task scheduling.

**Components**:
- `scheduler.go` - Base scheduler interface
- `purge_scheduler.go` - Schedules periodic purge tasks

**Key Features**:
- Prevents duplicate scheduled tasks using `CreateTaskIfNotRecentlyScheduled`
- Configurable intervals
- Graceful shutdown with callback

#### Metrics (`internal/metrics/`)

**Purpose**: Prometheus metrics collection.

**Components**:
- `collector.go` - Periodic statistics updates

**Metrics Exported**:
See [Metrics Documentation](../METRICS.md) for complete reference of all exposed metrics.

## Task Lifecycle

Tasks flow through a simple state machine:

```
┌─────────────────────────┐
│  Task Created           │
│  (via API or scheduler) │
└───────────┬─────────────┘
            │
            ▼
     ┌────────────┐
     │  pending   │  ◄─── Tasks wait here until claimed by worker
     └──────┬─────┘
            │
            │ Worker claims via GetTasksForProcessing()
            ▼
     ┌────────────┐
     │  running   │  ◄─── Worker actively processing
     └──────┬─────┘
            │
┌───────────┴────────────┐
│                        │
│ Success                │ Error/Timeout
▼                        ▼
┌────────────┐    ┌─────────────┐
│    done    │    │   failed    │  ◄─── Terminal states
└────────────┘    └─────────────┘       (no auto-retry)
```

**State Transitions**:
- `pending` → `running`: Worker claims task
- `running` → `done`: Task execution succeeds
- `running` → `failed`: Task execution errors or times out

**Note**: Failed tasks are not automatically retried. Retry logic must be implemented at the application level.

## Linkage vs Locking

The tasks model separates two different concerns:

- `reference`: soft-link to a domain entity (for example, `provisionings#<uuid>`)
- `concurrency_key`: worker lock scope used for execution mutual exclusion

This keeps domain coordination logic independent from worker-level locking.

### Data Model and Indexes

Relevant `tasks` table columns:

- `reference VARCHAR(512) NULL`
- `concurrency_key VARCHAR(512) NULL`

Relevant indexes:

- `idx_tasks_reference` (partial index on `reference IS NOT NULL`) for service lookups
- `idx_tasks_concurrency_key_running_unique` (unique partial index on `concurrency_key` where `state='running'`)

Effect:

- multiple tasks can be `pending` for the same key
- at most one task can be `running` for the same `concurrency_key`

### Runtime Behavior

When a task is created through `CreateTask()`:

1. If `concurrency_key` is already set, it is preserved.
2. If empty, the service calls `ResolveConcurrencyKey()`.
3. If the resolved key is non-empty, it is persisted to `concurrency_key`.

Default behavior in base `tasks.Task`:

- `ResolveConcurrencyKey()` returns `Type`
- this gives one-running-task-per-type locking by default

Task types can override this:

- return a per-entity key for finer lock scope
- return empty string to opt out of lock-key auto-population

At claim time, workers filter candidates to avoid keys that already have a
running task. A unique index also protects race windows across concurrent
claimers.

### Current Usage Patterns

- Provisioning tasks (`provision`/`destroy`):
  - set `reference = "provisionings#<uuid>"`
  - override `ResolveConcurrencyKey()` to return the same per-provisioning key
  - result: one running provisioning task per provisioning, while different provisionings run in parallel

- `public_dns_manage_records`:
  - overrides `ResolveConcurrencyKey()` to return empty string
  - result: multiple record-batch tasks can run concurrently

- Other task types:
  - use default per-type locking unless they override `ResolveConcurrencyKey()`

### Guidance for New Task Types

- Use `reference` for domain linkage, filtering, and audit.
- Use `concurrency_key` (via `ResolveConcurrencyKey()`) for execution lock policy.
- Do not use `reference` to encode lock behavior.

## Creating a New Task Type

### Step 1: Define the Task Struct

```go
package mytasks

import (
    "context"
    "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
    "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
)

const (
    MyTaskType = "my_task"
)

type MyTask struct {
    tasks.Task  // Embed base task
    myService   IMyService  // Service dependency
}

func NewMyTask(service IMyService) *MyTask {
    return &MyTask{
        Task: tasks.Task{
            Type: MyTaskType,
        },
        myService: service,
    }
}
```

### Step 2: Implement the Process Method

```go
func (t *MyTask) Process(ctx context.Context, l *logger.Logger) error {
    l.Info("Starting my task", "task_id", t.ID)

    if err := t.myService.DoSomething(ctx, l); err != nil {
        l.Error("Task failed", "error", err)
        return err
    }

    l.Info("Task completed successfully")
    return nil
}
```

### Step 3: Optional - Add Timeout Support

```go
func (t *MyTask) GetTimeout() time.Duration {
    return 5 * time.Minute  // Custom timeout for this task type
}
```

### Step 4: Register the Task Type

In your service:

```go
func (s *MyService) RegisterTasks(tasksService types.ITasksService) {
    tasksService.RegisterTasksService(s)
}

func (s *MyService) GetTaskServiceName() string {
    return "myservice"
}

func (s *MyService) GetHandledTasks() map[string]types.ITask {
    return map[string]types.ITask{
        MyTaskType: &MyTask{},  // Template instance
    }
}

func (s *MyService) CreateTaskInstance(taskType string) (tasks.ITask, error) {
    switch taskType {
    case MyTaskType:
        return NewMyTask(s), nil  // Create with service dependency
    default:
        return nil, types.ErrUnknownTaskType
    }
}
```

### Step 5: Use the Task

```go
// Create and enqueue the task
task := NewMyTask(myService)
createdTask, err := tasksService.CreateTask(ctx, l, task)
if err != nil {
    return err
}

// Optionally wait for completion
err = tasksService.WaitForTaskCompletion(ctx, l, createdTask.GetID(), 10*time.Minute)
```

## Task Options Pattern

Tasks can store typed options using the generic `TaskWithOptions[T]` type. This provides type-safe access to task parameters with automatic JSON marshaling.

### Basic Example

```go
// 1. Define your options struct
type MyTaskOptions struct {
    Param1 string        `json:"param1"`
    Param2 int           `json:"param2"`
    Timeout time.Duration `json:"timeout"`
}

// 2. Use TaskWithOptions[T] generic type
type MyTask struct {
    mtasks.TaskWithOptions[MyTaskOptions]  // Embed generic task with options
    Service IMyService                      // Service dependency
}

// 3. Create task with SetOptions
func NewMyTask(service IMyService, param1 string, param2 int) (*MyTask, error) {
    task := &MyTask{Service: service}
    task.Type = MyTaskType

    // SetOptions automatically marshals to JSON
    if err := task.SetOptions(MyTaskOptions{
        Param1:  param1,
        Param2:  param2,
        Timeout: 5 * time.Minute,
    }); err != nil {
        return nil, err
    }

    return task, nil
}

// 4. Access options with GetOptions
func (t *MyTask) Process(ctx context.Context, l *logger.Logger) error {
    // GetOptions returns typed options (no unmarshaling needed!)
    opts := t.GetOptions()

    l.Info("Processing task",
        "param1", opts.Param1,
        "param2", opts.Param2,
        "timeout", opts.Timeout,
    )

    // Use typed options directly
    return t.Service.DoSomethingWith(ctx, l, opts.Param1, opts.Param2)
}
```

### Real-World Example (Health Cleanup Task)

```go
// From services/health/tasks/tasks.go

type TaskCleanupOptions struct {
    InstanceTimeout  time.Duration `json:"instance_timeout"`
    CleanupRetention time.Duration `json:"cleanup_retention"`
}

type TaskCleanup struct {
    mtasks.TaskWithOptions[TaskCleanupOptions]
    Service types.IHealthService
}

func NewTaskCleanup(instanceTimeout, cleanupRetention time.Duration) (*TaskCleanup, error) {
    task := &TaskCleanup{}
    task.Type = string(HealthTaskCleanup)

    if err := task.SetOptions(TaskCleanupOptions{
        InstanceTimeout:  instanceTimeout,
        CleanupRetention: cleanupRetention,
    }); err != nil {
        return nil, err
    }

    return task, nil
}

func (t *TaskCleanup) Process(ctx context.Context, l *logger.Logger) error {
    opts := t.GetOptions()  // Type-safe access!

    deletedCount, err := t.Service.CleanupDeadInstances(
        ctx, l,
        opts.InstanceTimeout,
        opts.CleanupRetention,
    )

    if err != nil {
        return err
    }

    _ = deletedCount  // Use result as needed
    return nil
}
```

### How It Works

The `TaskWithOptions[T]` generic type:

```go
type TaskWithOptions[T any] struct {
    Task                    // Embed base task
    Options T `json:"-"`   // Typed options (not directly serialized)
}

// GetOptions returns typed options (reconstructed from Payload)
func (t *TaskWithOptions[T]) GetOptions() *T {
    return &t.Options
}

// SetOptions marshals to JSON and stores in Payload field
func (t *TaskWithOptions[T]) SetOptions(opts T) error {
    t.Options = opts
    // Auto-marshal to JSON and store in task.Payload
    data, err := json.Marshal(opts)
    if err != nil {
        return err
    }
    t.Payload = data
    return nil
}
```

## Service Hydration

**Why Hydration?**

When tasks are stored in the database, they lose their service references. Hydration reconstructs the task with its dependencies:

```
Database Task (serialized)
    │
    │ GetTasksForProcessing()
    ▼
tasks.Task (base type)
    │
    │ HydrateTask()
    ▼
Concrete Task with Services
    │
    │ Process()
    ▼
Execution
```

**Example**:

```go
// 1. Task stored in DB as tasks.Task
dbTask := &tasks.Task{
    ID:   uuid.MakeV4(),
    Type: "cluster_sync",
}

// 2. Worker hydrates it
concreteTask, err := tasksService.HydrateTask(dbTask)
// Returns: &TaskSync{Task: *dbTask, clustersService: ...}

// 3. Now has service dependency and can execute
concreteTask.Process(ctx, l, errChan)
```

## Configuration

### Service Options

```go
type Options struct {
    Workers                  int           // Number of concurrent workers (default: 1)
    WorkersEnabled           bool          // Enable task processing (default: true)
    DefaultTasksTimeout      time.Duration // Default task timeout (default: 30s)
    PurgeDoneTaskOlderThan   time.Duration // Purge done tasks after (default: 2h)
    PurgeFailedTaskOlderThan time.Duration // Purge failed tasks after (default: 24h)
    PurgeTasksInterval       time.Duration // How often to purge (default: 10m)
    CollectMetrics           bool          // Enable metrics collection (default: true)
    StatisticsUpdateInterval time.Duration // Metrics update frequency (default: 30s)
}
```

### Environment Variables

```bash
# Task processing
export ROACHPROD_TASKS_WORKERS=3

# Disable workers (API-only mode)
roachprod-centralized api --no-workers
```

## Testing

The tasks service includes comprehensive test coverage:

- **service_test.go**: Shared test helpers and mocks
- **api_test.go**: CRUD operation tests (8 tests)
- **coordination_test.go**: Coordination helper tests (4 tests)
- **registry_test.go**: Registration tests (1 test)
- **operations_test.go**: Business operation tests (5 tests)
- **scheduler_test.go**: Scheduler tests (2 tests)
- **metrics_test.go**: Metrics collection tests (6 tests)
- **processor_test.go**: Task processing tests (13 tests)

**Total**: 36 tests, 100% pass rate

### Example Test

```go
func TestCreateTaskIfNotAlreadyPlanned(t *testing.T) {
    mockRepo := &tasksrepomock.ITasksRepository{}
    taskService := NewService(mockRepo, "test-instance", Options{})

    // Setup: no existing pending tasks
    mockRepo.On("GetTasks", ctx, mock.Anything, mock.Anything).Return([]tasks.ITask{}, nil)
    mockRepo.On("CreateTask", mock.Anything, mock.Anything, mock.Anything).Return(nil)

    // Execute
    task := &MockTask{Task: tasks.Task{Type: "test"}}
    createdTask, err := taskService.CreateTaskIfNotAlreadyPlanned(ctx, logger.DefaultLogger, task)

    // Assert
    assert.NotNil(t, createdTask)
    assert.Nil(t, err)
    mockRepo.AssertExpectations(t)
}
```

## Performance Considerations

### Worker Scaling

- **Default**: 1 worker per instance
- **Recommended**: 2-5 workers for production
- **Considerations**: Balance between throughput and resource usage

### Polling Strategy

- **Busy**: 100ms interval when tasks are actively being processed
- **Idle**: 5s interval when queue is empty
- **Benefit**: Reduces database load during idle periods

### Database Load

- **GetTasksForProcessing()**: Single query with SKIP LOCKED
- **State Updates**: Individual updates per task
- **Cleanup**: Periodic batch deletion of old tasks

## Troubleshooting

### Tasks Stuck in Pending

**Symptoms**: Tasks created but never processed

**Causes**:
1. No workers running (`--no-workers` mode)
2. All workers crashed
3. Task type not registered

**Solution**:
```bash
# Check worker instances
roachprod-centralized workers

# Check task registration
# Verify service implements ITasksService and calls RegisterTasksService
```

### Tasks Timing Out

**Symptoms**: Tasks fail with timeout error

**Causes**:
1. Task execution exceeds timeout
2. Blocking operations in Process()

**Solution**:
```go
// Increase timeout for specific task type
func (t *MyTask) GetTimeout() time.Duration {
    return 10 * time.Minute  // Longer timeout
}

// Or increase default timeout
tasksService := NewService(repo, instanceID, Options{
    DefaultTasksTimeout: 5 * time.Minute,
})
```

### Deadlock on Shutdown

**Symptoms**: Service hangs during shutdown

**Cause**: Background goroutines not signaling completion

**Solution**: Verify all background routines call `WaitGroup.Done()` or use onComplete callback

## Related Documentation

- [Main Architecture](../ARCHITECTURE.md) - System-wide architecture
- [API Reference](../API.md) - REST API endpoints
- [Development Guide](../DEVELOPMENT.md) - Local development setup

---

*Last Updated: February 17, 2026*
