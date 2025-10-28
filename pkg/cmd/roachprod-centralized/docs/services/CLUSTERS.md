# Clusters Service

The Clusters Service manages cloud cluster lifecycle operations for roachprod-centralized, providing synchronized cluster state management across multiple cloud providers with distributed locking and health-aware coordination.

## Architecture Overview

The clusters service has been refactored into a modular architecture with clean separation of concerns:

```
pkg/cmd/roachprod-centralized/services/clusters/
├── service.go                    # Service orchestration and lifecycle
├── api.go                        # Public CRUD operations
├── sync.go                       # Cloud sync with distributed locking
├── coordination.go               # Inter-service coordination helpers
├── registry.go                   # Task type registration system
├── internal/                     # Implementation details (encapsulated)
│   ├── operations/
│   │   └── operations.go         # CRUD operation implementations
│   └── scheduler/
│       └── scheduler.go          # Periodic refresh scheduling
├── tasks/                        # Concrete task implementations
│   └── sync.go                   # Cloud sync task
├── types/                        # Public interfaces and DTOs
│   └── types.go                  # IService interface and DTOs
└── mocks/                        # Auto-generated test mocks
    └── clusters.go
```

## Key Components

### 1. Service Orchestration (`service.go`)

**Purpose**: Main service struct, lifecycle management, and background work coordination.

**Key Methods**:
- `NewService()` - Creates service instance with cloud providers and options
- `RegisterTasks()` - Called during app initialization
- `StartService()` - Initializes service (performs initial sync if needed)
- `StartBackgroundWork()` - Starts periodic refresh scheduler
- `Shutdown()` - Graceful shutdown with WaitGroup synchronization

**Configuration**:
```go
type Options struct {
    WorkersEnabled          bool          // Enable background workers
    NoInitialSync          bool          // Skip cloud sync on startup
    PeriodicRefreshEnabled bool          // Enable periodic cloud refresh
    PeriodicRefreshInterval time.Duration // Refresh interval (default: 5m)
}
```

**Background Work**:
```go
func (s *Service) StartBackgroundWork(ctx context.Context, l *logger.Logger, errChan chan<- error) error {
    // Start periodic cloud sync scheduler (if enabled)
    scheduler.StartPeriodicRefresh(ctx, l, errChan, interval, s, onComplete)
}
```

### 2. Public API (`api.go`)

**Purpose**: CRUD operations exposed via `types.IService` interface.

**Methods**:
- `GetAllClusters(ctx, l, input)` - Query clusters with filters
- `GetCluster(ctx, l, input)` - Get single cluster by name
- `RegisterCluster(ctx, l, input)` - Register an external creation of a cluster
- `RegisterClusterUpdate(ctx, l, input)` - Register an external update to an existing cluster
- `RegisterClusterDelete(ctx, l, input)` - Register an external deletion of a cluster
- `GetAllDNSZoneVMs(ctx, l, input)` - Get DNS zone VMs
- `SyncClouds(ctx, l)` - Trigger manual cloud sync (creates task)

**Used by**: Controllers (HTTP endpoints), CLI commands

**CRUD Flow**:
```
API Call → Check Sync Status
             │
    ┌────────┴───────────┐
    │                    │
No Sync          Sync In Progress
    │                    │
    ▼                    ▼
Apply Now       Queue Operation
    │                    │
    └────────┬───────────┘
             ▼
  Trigger DNS Sync (if needed)
```

### 3. Cloud Synchronization (`sync.go`)

**Purpose**: Core cloud synchronization logic with distributed locking and operation replay.

**Key Methods**:
- `Sync(ctx, l)` - Main sync operation with lock management
- `ScheduleSyncTaskIfNeeded(ctx, l)` - Create sync task (prevents duplicates)
- `acquireSyncLockWithHealthCheck(ctx, l, instanceID)` - Atomic lock acquisition

**Sync Algorithm**:
```
1. Get instance ID from health service
2. Try to acquire distributed lock (atomic + health check)
   - If lock held by healthy instance → Skip, return cached data
   - If lock held by unhealthy instance → Acquire lock
   - If lock free → Acquire lock
3. Clear stale operations from previous crashed syncs
4. List clusters from all cloud providers (GCE, AWS, Azure)
5. LOOP until no operations remain:
   a. Fetch pending operations WITH timestamp
   b. Apply operations to staging clusters (in-memory)
   c. Clear applied operations by timestamp
6. Atomically store clusters + release lock (single transaction)
7. Clear any remaining operations (arrived during final window)
8. Trigger DNS sync task
```

**Health-Aware Locking**:
The lock acquisition is atomic and includes health verification to prevent stale locks:

```go
// Repository method checks lock state and owner health in single transaction
acquired, err := s._store.AcquireSyncLockWithHealthCheck(
    ctx, l, instanceID, healthTimeout,
)
```

**Benefits**:
- ✅ Prevents race conditions where unhealthy instance holds lock
- ✅ Ensures only one healthy instance performs sync at a time
- ✅ Automatic lock recovery when syncing instance dies

### 4. Coordination Helpers (`coordination.go`)

**Purpose**: Inter-service coordination and data conversion.

**Methods**:
- `operationToOperationData(op)` - Convert IOperation → OperationData for storage
- `operationDataToOperation(opData)` - Convert OperationData → IOperation for replay
- `maybeEnqueuePublicDNSSyncTaskService(ctx, l)` - Trigger DNS sync task
- `conditionalEnqueueOperationWithHealthCheck(ctx, l, operation)` - Queue operation if sync active

**Operation Conversion**:
```go
// In-memory operation
op := operations.OperationCreate{Cluster: newCluster}

// Convert to persistable format
opData, err := s.operationToOperationData(op)
// opData contains: Type, ClusterName, ClusterData (JSON), Timestamp

// Later: Replay from queue
op, err := s.operationDataToOperation(opData)
err = op.ApplyOnStagingClusters(ctx, l, stagingClusters)
```

**Conditional Enqueueing**:
```go
// Only queue if sync is active AND syncing instance is healthy
enqueued, err := s.conditionalEnqueueOperationWithHealthCheck(ctx, l, operation)
if !enqueued {
    // Apply directly to repository
    err = op.ApplyOnRepository(ctx, l, s._store)
}
```

### 5. Task Registration (`registry.go`)

**Purpose**: Integration with task system for async cloud syncs.

**Task Types**:
- `ClustersTaskSync` - Async cloud sync task

**Key Methods**:
- `GetTaskServiceName()` → `"clusters"`
- `GetHandledTasks()` → Map of task type to task implementation
- `CreateTaskInstance(taskType)` - Hydrate task from type string

**Example**:
```go
// Service registers its tasks
func (s *Service) RegisterTasks(taskRegistry IRegistry) {
    taskRegistry.RegisterTasksService(s)
}

// Service implements ITasksService interface
func (s *Service) GetTaskServiceName() string {
    return "clusters"
}

func (s *Service) GetHandledTasks() map[string]types.ITask {
    return map[string]types.ITask{
        string(ClustersTaskSync): &TaskSync{Service: s},
    }
}

func (s *Service) CreateTaskInstance(taskType string) (tasks.ITask, error) {
    if taskType == string(ClustersTaskSync) {
        return &TaskSync{
            Task:    tasks.Task{Type: taskType},
            Service: s,
        }, nil
    }
    return nil, types.ErrUnknownTaskType
}
```

### 6. Internal Packages

#### Operations (`internal/operations/`)

**Purpose**: Encapsulated CRUD operation implementations.

**Interface**:
```go
type IOperation interface {
    ApplyOnRepository(ctx, l, store) error        // Apply to database
    ApplyOnStagingClusters(ctx, l, clusters) error // Apply to in-memory cloud data
}
```

**Operation Types**:
```go
type OperationCreate struct { Cluster cloudcluster.Cluster }
type OperationUpdate struct { Cluster cloudcluster.Cluster }
type OperationDelete struct { Cluster cloudcluster.Cluster }
```

**Usage**:
```go
// Create operation
op := operations.OperationCreate{Cluster: newCluster}

// Apply to repository
err := op.ApplyOnRepository(ctx, l, store)

// During sync: replay on staging data
err := op.ApplyOnStagingClusters(ctx, l, stagingClusters)
```

#### Scheduler (`internal/scheduler/`)

**Purpose**: Periodic cloud refresh scheduling.

**Interface**:
```go
type PeriodicRefreshScheduler interface {
    ScheduleSyncTaskIfNeeded(ctx, l) (tasks.ITask, error)
}

func StartPeriodicRefresh(
    ctx context.Context,
    l *logger.Logger,
    errChan chan<- error,
    refreshInterval time.Duration,
    scheduler PeriodicRefreshScheduler,
    onComplete func(),
)
```

**Behavior**:
- Starts background goroutine with ticker
- Calls `scheduler.ScheduleSyncTaskIfNeeded()` on each interval
- Uses `CreateTaskIfNotRecentlyScheduled` to prevent duplicate tasks
- Propagates errors to `errChan`
- Calls `onComplete()` callback on exit (for WaitGroup.Done)
- Respects context cancellation

## Cloud Sync Lifecycle

### Periodic Sync Flow

```
Scheduler Ticker Fires (every 10m)
    │
    ▼
ScheduleSyncTaskIfNeeded()
    │
    │ Check: Is there a pending/running sync task recently scheduled?
    │
    ├─ Yes → Skip (prevent duplicate)
    │
    └─ No → CreateTaskIfNotRecentlyScheduled()
            │
            ▼
       Task Enqueued
            │
            ▼
       Worker Picks Up Task
            │
            ▼
       TaskSync.Process()
            │
            ▼
       Service.Sync()
            │
            ▼
    [Acquire Lock] → [List Cloud] → [Replay Ops] → [Store] → [Release Lock]
```

### Sync with Operation Queueing

**Scenario**: User creates cluster while sync is in progress

```
Time 0: Sync starts, acquires lock
        sync_status.InProgress = true
        sync_status.InstanceID = "instance-1"
        Clears stale operations from previous crashes

Time 1: Cloud listing begins

Time 2: User calls CreateCluster()
        ↓
        Check: Is sync in progress?
        ↓
        Yes → Check: Is syncing instance healthy?
        ↓
        Yes → Queue operation
              store.ConditionalEnqueueOperation(opData)
        ↓
        Return success to user

Time 3: Cloud listing completes
        ↓
        LOOP: Fetch operations WITH timestamp
        ↓
        Found operations (including user's create)
        ↓
        Replay operations on staging clusters
        ↓
        Clear applied operations by timestamp
        ↓
        Fetch again → No more operations
        ↓
        Exit loop

Time 4: Atomically store clusters + release lock
        ↓
        Clear final operations (if any arrived during step 3-4)
        ↓
        Trigger DNS sync
```

**Race Condition Handling**:
- Operations arriving **before** cloud listing completes: Applied during replay loop
- Operations arriving **during** replay loop: Caught in next loop iteration
- Operations arriving **during final window** (between last fetch and lock release): Cleared but ignored (next sync will fetch fresh state)

### Lock Recovery Flow

**Scenario**: Syncing instance crashes mid-sync

```
Time 0: Instance A starts sync, acquires lock
        sync_status.InProgress = true
        sync_status.InstanceID = "instance-a"
        health_service.instance-a.LastHeartbeat = now()

Time 1: Instance A crashes (no more heartbeats)

Time 2: Instance B attempts sync
        ↓
        acquireSyncLockWithHealthCheck("instance-b")
        ↓
        Repository checks:
        - Lock held by "instance-a"
        - Last heartbeat > healthTimeout (stale!)
        ↓
        Atomic operation:
        - Set sync_status.InstanceID = "instance-b"
        - Set sync_status.Timestamp = now()
        ↓
        Instance B acquires lock, proceeds with sync
```

## Operation Replay Details

### Why Replay Operations?

During cloud sync, we fetch fresh cluster state from cloud providers (source of truth). Any CRUD operations performed *during* the sync would be overwritten if we simply replaced the repository data. Operation replay solves this:

```
Without Replay:
  User creates cluster X → Sync lists clouds (no X) → Store overwrites → Cluster X lost!

With Replay:
  User creates cluster X → Queued as OperationCreate
  Sync lists clouds → Replay OperationCreate(X) on staging data → Store includes X → Success!
```

### Replay Implementation

```go
// 1. Clear stale operations from previous crashed syncs
staleCount, err := s._store.ClearPendingOperations(ctx, l)
if staleCount > 0 {
    l.Warn("cleared stale operations", slog.Int64("count", staleCount))
}

// 2. List fresh cloud data
err = s.roachprodCloud.ListCloud(crlLogger, vm.ListOptions{...})

// 3. Loop to apply all operations on staging clusters
totalOpsApplied := 0
for {
    // Fetch operations with timestamp for safe clearing
    pendingOps, fetchTimestamp, err := s._store.GetPendingOperationsWithTimestamp(ctx, l)
    if len(pendingOps) == 0 {
        break
    }

    // Replay each operation on staging data
    for _, opData := range pendingOps {
        op, err := s.operationDataToOperation(opData)
        err = op.ApplyOnStagingClusters(ctx, l, s.roachprodCloud.Clusters)
    }

    totalOpsApplied += len(pendingOps)

    // Clear only the operations we just applied
    clearedCount, err := s._store.ClearPendingOperationsBefore(ctx, l, fetchTimestamp)
    l.Debug("cleared applied operations", slog.Int64("count", clearedCount))
}

// 4. Atomically store merged result + release lock (single transaction)
err = s._store.StoreClustersAndReleaseSyncLock(ctx, l, s.roachprodCloud.Clusters, instanceID)

// 5. Clear any remaining operations (arrived during final window)
remainingCount, err := s._store.ClearPendingOperations(ctx, l)
if remainingCount > 0 {
    l.Warn("cleared operations from final window", slog.Int64("count", remainingCount))
}
```

### Operation Types and Replay Behavior

**OperationCreate**:
```go
func (o OperationCreate) ApplyOnStagingClusters(ctx, l, clusters) error {
    clusters[o.Cluster.Name] = &o.Cluster  // Add to staging
    return nil
}
```

**OperationUpdate**:
```go
func (o OperationUpdate) ApplyOnStagingClusters(ctx, l, clusters) error {
    clusters[o.Cluster.Name] = &o.Cluster  // Replace in staging
    return nil
}
```

**OperationDelete**:
```go
func (o OperationDelete) ApplyOnStagingClusters(ctx, l, clusters) error {
    delete(clusters, o.Cluster.Name)  // Remove from staging
    return nil
}
```

## Configuration

### Service Options

```go
type Options struct {
    WorkersEnabled          bool          // Enable background workers (default: true)
    NoInitialSync          bool          // Skip sync on startup (default: false)
    PeriodicRefreshEnabled bool          // Enable periodic refresh (default: true)
    PeriodicRefreshInterval time.Duration // Refresh interval (default: 5m)
}
```

### Environment Variables

```bash
# Periodic refresh interval
export ROACHPROD_CLUSTERS_REFRESH_INTERVAL=10m

# Disable periodic refresh
roachprod-centralized api --no-periodic-refresh

# Disable all background workers
roachprod-centralized api --no-workers
```

### Cloud Provider Configuration

Clusters service integrates with roachprod's cloud provider system. Configure providers in `cloud.yaml`:

```yaml
gce:
  enabled: true
  project: my-gce-project
  credentials: /path/to/gce-creds.json

aws:
  enabled: true
  regions: [us-east-1, us-west-2]
  credentials: /path/to/aws-creds

azure:
  enabled: true
  subscription: my-subscription-id
  credentials: /path/to/azure-creds
```

## Testing

The clusters service includes comprehensive test coverage:

- **service_test.go**: Shared test helpers + lifecycle tests (2 tests)
- **api_test.go**: CRUD operation tests (4 tests, 6 cases)
- **coordination_test.go**: Coordination helper tests (6 tests, 16 cases)
- **registry_test.go**: Registration tests (4 tests, 5 cases)
- **sync_test.go**: Sync logic tests (placeholder)
- **internal/operations/operations_test.go**: Operation tests (8 tests, 13 cases)
- **internal/scheduler/scheduler_test.go**: Scheduler tests (placeholder)

**Total**: 24 test functions, 44+ test cases, 100% pass rate

### Example Test

```go
func TestService_CreateCluster(t *testing.T) {
    tests := []struct {
        name     string
        input    types.InputCreateClusterDTO
        mockFunc func(*clustersrepomock.IClustersRepository)
        want     *cloudcluster.Cluster
        wantErr  error
    }{
        {
            name: "success",
            input: types.InputCreateClusterDTO{
                Cluster: cloudcluster.Cluster{Name: "new-cluster"},
            },
            mockFunc: func(repo *clustersrepomock.IClustersRepository) {
                repo.On("GetCluster", mock.Anything, mock.Anything, "new-cluster").
                    Return(cloudcluster.Cluster{}, types.ErrClusterNotFound)
                repo.On("StoreCluster", mock.Anything, mock.Anything, mock.Anything).
                    Return(nil)
                repo.On("GetSyncStatus", mock.Anything, mock.Anything).
                    Return(&clusters.SyncStatus{InProgress: false}, nil)
            },
            want: &cloudcluster.Cluster{Name: "new-cluster"},
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            // ... test implementation
        })
    }
}
```

## Performance Considerations

### Sync Frequency

- **Default**: 10 minutes
- **Recommended**: 5-15 minutes for production
- **Trade-off**: More frequent = fresher data, but higher cloud API usage

### Cloud API Rate Limits

Each sync calls cloud provider APIs:
- GCE: List instances across all zones
- AWS: Describe instances across all regions
- Azure: List resources across subscriptions

**Rate Limit Mitigation**:
- Single sync per cluster (distributed lock)
- Configurable refresh interval
- Operations queued during sync (not re-synced)

### Database Load

**Read Operations**:
- GetClusters: Frequent (every API call)
- GetSyncStatus: On every CRUD operation
- GetPendingOperations: Once per sync

**Write Operations**:
- StoreClusters: Once per sync (batch write)
- ConditionalEnqueueOperation: On CRUD during sync
- AcquireSyncLock/ReleaseSyncLock: Per sync

## Troubleshooting

### Clusters Not Syncing

**Symptoms**: Cloud clusters not appearing in database

**Causes**:
1. No workers running (`--no-workers` mode)
2. Periodic refresh disabled
3. Lock held by crashed instance

**Solution**:
```bash
# Check worker instances
roachprod-centralized workers

# Check sync status
roachprod-centralized clusters sync-status

# Force manual sync
roachprod-centralized clusters sync
```

### Stale Lock

**Symptoms**: Sync never runs, lock always held

**Cause**: Syncing instance crashed without releasing lock, health check timeout too long

**Solution**:
```bash
# Check lock status
SELECT * FROM sync_status WHERE service = 'clusters';

# Health service will automatically release lock after timeout
# Or manually release (emergency only):
UPDATE sync_status SET in_progress = false WHERE service = 'clusters';
```

### Operation Queue Growing

**Symptoms**: cluster_operations table growing, never cleared

**Cause**: Sync consistently failing or not running

**Automatic Cleanup**: Each sync clears stale operations at startup, so this should be rare

**Solution**:
```bash
# Check pending operations
SELECT * FROM cluster_operations ORDER BY created_at DESC;

# Check for stale operations in logs
tail -f /var/log/roachprod-centralized/app.log | grep "cleared stale operations"

# Check sync errors in logs
tail -f /var/log/roachprod-centralized/app.log | grep "sync"

# Manual cleanup (emergency only, system handles this automatically):
DELETE FROM cluster_operations WHERE created_at < now() - INTERVAL '1 hour';
```

### CRUD Operations Slow During Sync

**Symptoms**: CreateCluster/UpdateCluster taking longer than usual

**Cause**: Operations being queued, waiting for sync to complete and replay

**Expected**: Operations queue instantly, but final state not visible until sync completes

**Solution**: This is normal behavior. If problematic, reduce sync frequency or optimize cloud API calls.

## API Reference

### DTOs

```go
type InputGetAllClustersDTO struct {
    Filters filters.FilterSet
}

type InputGetClusterDTO struct {
    Name string
}

type InputRegisterClusterDTO struct {
    Cluster cloudcluster.Cluster
}

type InputRegisterClusterUpdateDTO struct {
    Cluster cloudcluster.Cluster
}

type InputRegisterClusterDeleteDTO struct {
    Cluster cloudcluster.Cluster
}

type InputGetAllDNSZoneVMsDTO struct {
    Filters filters.FilterSet
}
```

### Errors

```go
var (
    ErrClusterNotFound      = errors.New("cluster not found")
    ErrClusterAlreadyExists = errors.New("cluster already exists")
    ErrShutdownTimeout      = errors.New("shutdown timeout")
)
```

### IService Interface

```go
type IService interface {
    // Lifecycle
    StartService(ctx context.Context, l *logger.Logger) error
    StartBackgroundWork(ctx context.Context, l *logger.Logger, errChan chan<- error) error
    Shutdown(ctx context.Context) error
    RegisterTasks(ctx context.Context, l *logger.Logger, taskRegistry IRegistry) error

    // Task Registry
    GetTaskServiceName() string
    GetHandledTasks() map[string]ITask
    CreateTaskInstance(taskType string) (ITask, error)

    // CRUD Operations
    GetAllClusters(ctx, l, input) (Clusters, error)
    GetCluster(ctx, l, input) (*Cluster, error)
    RegisterCluster(ctx, l, input) (*Cluster, error)
    RegisterClusterUpdate(ctx, l, input) (*Cluster, error)
    RegisterClusterDelete(ctx, l, input) error
    GetAllDNSZoneVMs(ctx, l, input) (DNSZoneVMs, error)

    // Sync
    SyncClouds(ctx, l) (ITask, error)  // Async via task system
    Sync(ctx, l) (Clusters, error)     // Synchronous direct sync
}
```

## Repository Methods

The clusters service relies on several specialized repository methods for operation management:

### Operation Queue Methods

```go
// Fetch operations with timestamp for safe clearing
GetPendingOperationsWithTimestamp(ctx, l) ([]OperationData, time.Time, error)

// Clear operations created before a specific timestamp
// Returns the number of operations deleted
ClearPendingOperationsBefore(ctx, l, timestamp) (int64, error)

// Clear all pending operations
// Returns the number of operations deleted
ClearPendingOperations(ctx, l) (int64, error)
```

**Usage Pattern**:
```go
// Fetch with timestamp
ops, fetchTime, err := store.GetPendingOperationsWithTimestamp(ctx, l)

// Apply operations...

// Clear only what was fetched
count, err := store.ClearPendingOperationsBefore(ctx, l, fetchTime)
l.Debug("cleared operations", slog.Int64("count", count))
```

### Atomic Operations

```go
// Atomically store clusters and release sync lock in a single transaction
StoreClustersAndReleaseSyncLock(ctx, l, clusters, instanceID) error
```

**Why Atomic?**
- Prevents operations from being enqueued between storing clusters and releasing lock
- Ensures consistency: clusters stored ⟺ lock released
- Eliminates race condition window

**Database Implementation**:
```sql
BEGIN;
  DELETE FROM clusters;
  INSERT INTO clusters (name, data) VALUES (...);
  UPDATE cluster_sync_state
    SET in_progress = false, instance_id = NULL
    WHERE id = 1 AND instance_id = $1;
COMMIT;
```

## Related Documentation

- [Main Architecture](ARCHITECTURE.md) - System-wide architecture
- [Tasks Service](TASKS.md) - Task system integration
- [API Reference](../API.md) - REST API endpoints
- [Development Guide](../DEVELOPMENT.md) - Local development setup

---

*Last Updated: October 3, 2025*
