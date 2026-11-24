# CockroachDB Tasks Repository

This package implements a CockroachDB-backed tasks repository for the roachprod-centralized service.

## Features

- **Distributed task processing**: Multiple workers can safely claim tasks using atomic updates
- **Intelligent polling**: Adaptive polling intervals that speed up when tasks are available and slow down when idle
- **Automatic cleanup**: Distributed cleanup of stale tasks (tasks stuck in "running" state from dead workers)
- **Full PostgreSQL/CockroachDB compatibility**: Uses standard SQL with optimizations for CockroachDB

## Configuration

Add database configuration to your config file or environment variables:

```yaml
database:
  type: cockroachdb
  url: "postgresql://user:password@localhost:26257/roachprod?sslmode=require"
  max_conns: 10
  max_idle_time: 300
```

Or using environment variables:
```bash
export ROACHPROD_DATABASE_TYPE=cockroachdb
export ROACHPROD_DATABASE_URL="postgresql://user:password@localhost:26257/roachprod?sslmode=require"
export ROACHPROD_DATABASE_MAX_CONNS=10
export ROACHPROD_DATABASE_MAX_IDLE_TIME=300
```

## Database Setup

1. Create a CockroachDB database for the service
2. Run the schema creation script:

```sql
-- Apply the schema
\i pkg/cmd/roachprod-centralized/repositories/tasks/cockroachdb/schema.sql
```

## How It Works

### Task Processing
- Workers poll for pending tasks using adaptive intervals (100ms-5s)
- Tasks are atomically claimed using `UPDATE ... WHERE state = 'pending'` 
- Each worker gets a unique `consumer_id` to track ownership
- Polling speeds up when tasks are found, slows down when idle

### Distributed Cleanup
- Each worker cleans up 1-2 stale tasks before polling for new ones
- Tasks stuck in "running" state for >10 minutes are reset to "pending"
- No coordination required between workers - cleanup is distributed

### Concurrency Safety
- Uses CockroachDB's strong consistency for safe concurrent access
- No version columns or complex locking needed
- Multiple workers can safely process tasks without conflicts

## Performance Characteristics

- **Low latency**: Sub-second task pickup when busy (100ms polling)
- **Low overhead**: 5-second intervals when idle, distributed cleanup
- **Scalable**: Works with any number of horizontal workers
- **Resilient**: Automatic recovery from worker failures

## Monitoring

The repository provides statistics via `GetStatistics()` showing task counts by state:
- `pending`: Tasks waiting to be processed
- `running`: Tasks currently being processed  
- `done`: Successfully completed tasks
- `failed`: Tasks that failed processing

## Configuration Options

When creating the repository, you can configure:

```go
repo := cockroachdb.NewTasksRepository(db, cockroachdb.Options{
    BasePollingInterval: 100 * time.Millisecond, // Fast polling when busy
    MaxPollingInterval:  5 * time.Second,        // Slow polling when idle  
    TaskTimeout:         10 * time.Minute,       // When to consider tasks stale
})
```