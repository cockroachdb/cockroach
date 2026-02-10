# roachprod-centralized Architecture

This document describes the system architecture, design patterns, and key components of the roachprod-centralized service.

**📚 Related Documentation:**
- [← Back to Main README](../README.md)
- [🔌 API Reference](API.md) - Complete REST API documentation
- [💻 Development Guide](DEVELOPMENT.md) - Local development setup
- [📋 Examples & Workflows](EXAMPLES.md) - Practical usage examples
- [⚙️ Configuration Examples](../examples/) - Ready-to-use configurations

## Table of Contents

- [Overview](#overview)
- [System Architecture](#system-architecture)
- [Core Components](#core-components)
- [Data Flow](#data-flow)
- [Background Processing](#background-processing)
- [Cloud Provider Integration](#cloud-provider-integration)
- [Database Layer](#database-layer)
- [Security](#security)
- [Configuration Management](#configuration-management)
- [Design Patterns](#design-patterns)
- [Scalability Considerations](#scalability-considerations)

## Overview

The roachprod-centralized service is designed as a modern, cloud-native REST API that centralizes management of CockroachDB roachprod clusters across multiple cloud providers. The architecture follows clean architecture principles with clear separation of concerns and dependency inversion.

### Design Goals

- **Scalability**: Support horizontal scaling with separate API and worker tiers
- **Reliability**: Graceful handling of failures and recovery
- **Maintainability**: Clean code structure with clear responsibilities
- **Extensibility**: Easy addition of new cloud providers and features
- **Security**: Secure authentication and authorization
- **Observability**: Comprehensive logging, metrics, and monitoring
- **Flexibility**: Multiple deployment modes for different scale requirements

## System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                          Load Balancer                          │
└─────────────────────┬───────────────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────────┐
│                    API Gateway / Router                         │
│                   (Gin HTTP Framework)                          │
└─────────────────────┬───────────────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────────┐
│                     Controllers Layer                           │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐│
│  │   Health    │ │  Clusters   │ │    Tasks    │ │ Public DNS  ││
│  │ Controller  │ │ Controller  │ │ Controller  │ │ Controller  ││
│  └─────────────┘ └─────────────┘ └─────────────┘ └─────────────┘│
└─────────────────────┬───────────────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────────┐
│                     Services Layer                              │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐│
│  │   Health    │ │  Clusters   │ │    Tasks    │ │ Public DNS  ││
│  │   Service   │ │   Service   │ │   Service   │ │   Service   ││
│  └─────────────┘ └─────────────┘ └─────────────┘ └─────────────┘│
└─────────────────────┬───────────────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────────┐
│                  Repositories Layer                             │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐│
│  │   Health    │ │  Clusters   │ │    Tasks    │ │   Config    ││
│  │ Repository  │ │ Repository  │ │ Repository  │ │ Repository  ││
│  └─────────────┘ └─────────────┘ └─────────────┘ └─────────────┘│
└─────────────────────┬───────────────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────────┐
│                    Storage Layer                                │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐│
│  │   Memory    │ │ CockroachDB │ │    Files    │ │ Cloud APIs  ││
│  │   Storage   │ │  Database   │ │   System    │ │Integration  ││
│  └─────────────┘ └─────────────┘ └─────────────┘ └─────────────┘│
└─────────────────────────────────────────────────────────────────┘

         ┌─────────────────────────────────────────────┐
         │           Background Systems                │
         │  ┌─────────────┐ ┌─────────────┐ ┌─────────┐│
         │  │Task Workers │ │  Cluster    │ │   DNS   ││
         │  │   Pool      │ │   Sync      │ │  Sync   ││
         │  └─────────────┘ └─────────────┘ └─────────┘│
         └─────────────────────────────────────────────┘
```

## Core Components

### 1. Controllers (`controllers/`)

**Responsibility**: HTTP request/response handling and routing

- **Health Controller**: Basic health checks and status reporting
- **Clusters Controller**: CRUD operations for cluster management
- **Tasks Controller**: Task querying and monitoring
- **Public DNS Controller**: DNS synchronization triggers

**Key Features**:
- Request validation and binding
- Response formatting with `request_id` and `result_type`
- Error handling and HTTP status code mapping
- Authentication middleware integration
- Coarse authorization gate via `AuthorizationRequirement` on handlers (`RequiredPermissions` / `AnyOf`)

### 2. Services (`services/`)

**Responsibility**: Business logic and orchestration

Each service package contains:
- **Clusters Service** (`clusters/`):
  - Core cluster management logic
  - `tasks/`: Cluster-related background tasks
  - `models/`: Cluster-specific data models
  - `mocks/`: Test mocks
- **Tasks Service** (`tasks/`):
  - Background task coordination and processing
  - Modular architecture with clean separation of concerns
  - `service.go`: Orchestration and lifecycle
  - `api.go`: Public CRUD operations
  - `coordination.go`: Inter-service helpers
  - `registry.go`: Task type registration and hydration
  - `operations.go`: Business operations
  - `internal/processor/`: Worker pool and task execution
  - `internal/scheduler/`: Periodic task scheduling
  - `internal/metrics/`: Metrics collection
  - `tasks/`: Concrete task implementations (e.g., purge)
  - `types/`: Task interfaces and DTOs
  - `mocks/`: Test mocks
  - **[📖 Detailed Documentation](services/TASKS.md)**
- **Public DNS Service** (`public-dns/`):
  - DNS record management and synchronization
  - `tasks/`: DNS-related background tasks
  - `models/`: DNS-specific data models
  - `mocks/`: Test mocks
- **Health Service** (`health/`):
  - System health monitoring
  - `tasks/`: Health check background tasks
  - `mocks/`: Test mocks

**Key Features**:
- Business rule enforcement
- Cross-service coordination
- Background job scheduling
- Cloud provider orchestration
- Fine-grained authorization (scope/environment checks, ownership checks, resource-level checks)

### Authorization Boundary Contract

To keep authorization understandable and maintainable:

1. Controllers handle coarse access control:
   - Authenticate request.
   - Check required permission family for the endpoint.
   - Do not implement scope-specific or resource ownership authorization logic.
2. Services handle fine-grained authorization:
   - Resolve scope from trusted server-side data.
   - Enforce ownership/resource checks.
   - Make final allow/deny decision before performing mutations.
3. Authorization decisions must be based on trusted state:
   - Use persisted resources and server config.
   - Do not rely only on request payload fields for authorization-critical checks.

This split keeps route declarations simple while preventing authorization bypasses in complex business flows.

### 3. Repositories (`repositories/`)

**Responsibility**: Data access abstraction

Each repository has multiple implementations:
- **Abstract Interfaces**: Define data access contracts
- **Memory Implementation** (`memory/`): In-memory storage for development
- **CockroachDB Implementation** (`cockroachdb/`): Production database storage with migrations
- **Mock Implementation** (`mocks/`): Testing support

**Repositories**:
- **Clusters Repository**: Cluster data persistence
- **Tasks Repository**: Background task storage and state management
- **Health Repository**: Health check state storage

**Key Features**:
- Storage backend abstraction
- Transaction management
- Data consistency guarantees
- Built-in migration support (migrations defined in code)

### 4. Configuration System (`config/`)

**Responsibility**: Multi-source configuration management

The configuration system supports hierarchical configuration from multiple sources:

**Packages**:
- **`types/`**: Configuration structure definitions
- **`flags/`**: CLI flag handling (Cobra integration)
- **`env/`**: Environment variable processing
- **`processing/`**: Configuration merging and validation
- **`recursive/`**: Recursive configuration merging

**Configuration Sources** (in order of precedence):
1. Environment variables (`ROACHPROD_*`)
2. CLI flags
3. YAML configuration file
4. Default values

**Key Features**:
- Type-safe configuration structs
- Automatic environment variable mapping
- Cloud provider configuration with multiple formats
- Validation on startup

### 5. Utilities (`utils/`)

**Responsibility**: Shared utilities across the application

**Packages**:
- **`api/`**: HTTP API utilities
  - `bindings/`: Request binding helpers for Gin framework
- **`database/`**: Database connection and helper utilities
- **`filters/`**: Query filtering system (Stripe-style)
  - `types/`: Filter type definitions
  - `memory/`: In-memory filter implementation
  - `sql/`: SQL query filter generation
- **`logger/`**: Structured logging wrapper

### 6. Background Task System

**Responsibility**: Asynchronous task processing

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Task Queue    │────│ Task Processor  │────│   Task Workers  │
│   (Database)    │    │   Coordinator   │    │      Pool       │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                      │
         │              ┌─────────────────┐             │
         └──────────────│   Task Types    │─────────────┘
                        │                 │
                        │ • Cluster Sync  │
                        │ • DNS Sync      │
                        │ • Health Check  │
                        └─────────────────┘
```

## Data Flow

### 1. HTTP Request Flow

```
HTTP Request
    │
    ▼
┌─────────────────┐
│   Middleware    │ ── Authentication, Logging, CORS
│    Pipeline     │
└─────────┬───────┘
          │
          ▼
┌─────────────────┐
│   Controller    │ ── Request validation, parameter binding
│                 │
└─────────┬───────┘
          │
          ▼
┌─────────────────┐
│     Service     │ ── Business logic, orchestration
│                 │
└─────────┬───────┘
          │
          ▼
┌─────────────────┐
│   Repository    │ ── Data access, persistence
│                 │
└─────────┬───────┘
          │
          ▼
┌─────────────────┐
│    Response     │ ── Formatted JSON with request_id/result_type
│                 │
└─────────────────┘
```

### 2. Background Task Flow

```
API Request (POST /clusters/sync)
    │
    ▼
┌─────────────────┐
│   Controller    │ ── Validate request
│                 │
└─────────┬───────┘
          │
          ▼
┌─────────────────┐
│     Service     │ ── Create task record
│                 │
└─────────┬───────┘
          │
          ▼
┌─────────────────┐
│   Task Queue    │ ── Store task in database
│                 │
└─────────┬───────┘
          │
          ▼
┌─────────────────┐
│   Background    │ ── Process task asynchronously
│     Worker      │
└─────────┬───────┘
          │
          ▼
┌─────────────────┐
│ Cloud Provider  │ ── Execute actual operations
│      APIs       │
└─────────────────┘
```

## Background Processing

### Task Processing Architecture

The background task system uses a distributed, fault-tolerant design that supports multiple deployment modes:

#### Components

1. **Task Repository**: Persistent task storage with atomic operations
2. **Task Processor**: Coordinates task execution across workers
3. **Worker Pool**: Configurable number of concurrent workers
4. **Cleanup System**: Handles stale tasks and failure recovery

#### Deployment Mode Behavior

**All-in-One Mode**:
- Background work runs in the same process as the API
- Task workers process tasks from the queue
- Periodic background jobs (cluster sync, health checks) are active
- Suitable for single-instance deployments

**API-Only Mode** (`--no-workers`):
- Background work is **disabled**
- No task workers running (tasks remain pending until workers process them)
- No periodic jobs (no automatic cluster sync, no health heartbeats)
- API can still **enqueue** tasks (e.g., POST /clusters/sync creates a task)
- Requires separate worker instances to process tasks

**Workers-Only Mode** (`workers` command):
- Only background work runs (no API routes except metrics)
- Task workers process tasks from the shared queue
- Periodic background jobs are active (cluster sync, health cleanup)
- Multiple worker instances coordinate via CockroachDB

#### Task Lifecycle

Tasks flow through a simple state machine:

```
       ┌─────────────────────────┐
       │  Task Created           │
       │  (via API or background │
       │   periodic job)         │
       └───────────┬─────────────┘
                   │
                   ▼
            ┌────────────┐
            │  pending   │  ◄─── Tasks wait here until claimed by worker
            └──────┬─────┘
                   │
                   │ Worker claims task
                   ▼
            ┌────────────┐
            │  running   │  ◄─── Worker actively processing task
            └──────┬─────┘
                   │
       ┌───────────┴────────────┐
       │                        │
       │ Success                │ Error/Timeout
       ▼                        ▼
┌────────────┐          ┌─────────────┐
│    done    │          │   failed    │  ◄─── Terminal states
└────────────┘          └─────────────┘       (no auto-retry)
```

**State Transitions**:
- `pending` → `running`: Worker claims task via `GetTasksForProcessing()`
- `running` → `done`: Task execution succeeds
- `running` → `failed`: Task execution errors or times out

**Note**: There is no automatic retry mechanism. Failed tasks remain in `failed` state. Retry logic must be implemented at the application level (e.g., manually re-creating the task).

#### Features

- **Adaptive Polling**: Fast polling when busy (100ms), slow when idle (5s)
- **Distributed Cleanup**: Each worker cleans stale tasks before polling
- **Concurrency Safety**: Uses CockroachDB's strong consistency
- **Failure Recovery**: Automatic retry of failed tasks

### Task Types

| Task Type | Description | Trigger |
|-----------|-------------|---------|
| `cluster_sync` | Synchronize cluster data from cloud providers | POST /clusters/sync, Initial sync, Periodic sync |
| `dns_sync` | Update DNS records for clusters | POST /public-dns/sync |
| `health_check` | Periodic system health validation | Scheduled |

### Smart Initial Sync

When instances start, they use an intelligent decision algorithm to determine whether to perform an initial cluster synchronization. This prevents redundant syncs in distributed deployments while ensuring data freshness.

#### Decision Logic

The system performs an initial sync if any of these conditions are true:
1. **No recent sync exists** - No completed `cluster_sync` task found in the database
2. **Sync is stale** - Last completed sync is older than `PeriodicRefreshInterval` (default: 10 minutes)
3. **No worker coverage** - No healthy worker instances exist to handle periodic refresh
4. **No API coverage** - No healthy API instances exist to receive cluster operations

The system **skips** the initial sync only when ALL conditions are met:
- Recent sync exists (within last 10 minutes)
- At least one healthy worker instance exists (for periodic refresh)
- At least one healthy API instance exists (to receive cluster operations)
- The current instance is excluded from coverage checks (since its API isn't running yet)

#### Why API Coverage Matters

The system tracks API instance coverage because:
- Without API instances, cluster create/update/delete operations cannot be received
- If all API instances were down between syncs, the database might be stale
- When only worker instances exist, a fresh sync from cloud providers is the only source of truth

#### Startup Flow

```
Instance Starts
    │
    ▼
┌─────────────────────────────────┐
│  Health Service Registers       │ ── Sends heartbeat to database
│  Instance (heartbeat starts)    │
└────────────┬────────────────────┘
             │
             ▼
┌─────────────────────────────────┐
│  Clusters Service Checks        │ ── Query for recent completed sync
│  If Initial Sync Needed         │    Check for other healthy instances
└────────────┬────────────────────┘
             │
      ┌──────┴──────┐
      │             │
  YES │             │ NO
      ▼             ▼
┌──────────────┐  ┌──────────────┐
│ Schedule     │  │ Skip Initial │
│ Sync Task    │  │ Sync         │
└──────┬───────┘  └──────┬───────┘
       │                 │
       ▼                 │
┌──────────────┐         │
│ Wait for     │         │
│ Task Done    │         │
└──────┬───────┘         │
       │                 │
       └────────┬────────┘
                │
                ▼
       ┌──────────────┐
       │ Start API    │ ── Fresh data guaranteed
       │ Server       │
       └──────────────┘
```

#### Benefits

- **Prevents redundant syncs**: Multiple instances starting simultaneously won't all sync
- **Ensures data freshness**: API only starts after fresh cluster data is loaded
- **Handles API gaps**: Detects when no API instances existed to receive operations
- **Distributed coordination**: Uses task system for consistent tracking across instances
- **Blocks until ready**: Initial sync waits for completion before serving requests

#### Example Scenarios

**Scenario 1: First Worker Instance**
- No recent sync found
- **Result**: Performs initial sync ✅

**Scenario 2: Second Worker Instance (10 minutes later)**
- Recent sync exists (from first worker)
- Other worker exists BUT no API instances
- **Result**: Performs initial sync ✅ (no way to receive cluster operations)

**Scenario 3: First API Instance (5 minutes after workers)**
- Recent sync exists
- Worker instances exist BUT current instance excluded
- No OTHER API instances exist yet
- **Result**: Performs initial sync ✅ (this API isn't ready yet)

**Scenario 4: Second API Instance (2 minutes later)**
- Recent sync exists
- Worker instances exist
- OTHER API instance exists (the first one)
- **Result**: Skips initial sync ✅ (full coverage exists)

## Cloud Provider Integration

### Provider Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Cloud Provider Abstraction                   │
└────────────────────────────────┬────────────────────────────────┘
                                 │
┌────────────────────────────────▼────────────────────────────────┐
│.                   Provider Implementations                     │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐│
│  │     GCE     │ │     AWS     │ │    Azure    │ │     IBM     ││
│  │   Provider  │ │   Provider  │ │   Provider  │ │   Provider  ││
│  └─────────────┘ └─────────────┘ └─────────────┘ └─────────────┘│
└────────────────────────────────┬────────────────────────────────┘
                                 │
┌────────────────────────────────▼────────────────────────────────┐
│                           Cloud APIs                            │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐│
│  │ GCP Compute │ │   AWS EC2   │ │  Azure VMs  │ │  IBM Cloud  ││
│  │    Engine   │ │             │ │             │ │     VMs     ││
│  └─────────────┘ └─────────────┘ └─────────────┘ └─────────────┘│
└─────────────────────────────────────────────────────────────────┘
```

### Provider Configuration

Each provider supports:
- **Credentials Management**: Secure credential storage and rotation
- **Multi-Region Support**: Operations across different regions
- **DNS Integration**: Public DNS record management
- **Resource Tagging**: Consistent tagging for resource organization

## Database Layer

### Schema Design

```sql
-- Tasks table (primary workload)
CREATE TABLE tasks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type STRING NOT NULL,
    status STRING NOT NULL DEFAULT 'pending',
    payload JSONB,
    result JSONB,
    consumer_id STRING,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    INDEX idx_tasks_status_type (status, type),
    INDEX idx_tasks_consumer (consumer_id),
    INDEX idx_tasks_created (created_at)
);

-- Clusters table (cached cloud data)
CREATE TABLE clusters (
    name STRING PRIMARY KEY,
    provider STRING NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    INDEX idx_clusters_provider (provider)
);
```

### Storage Backends

#### Memory Backend
- **Use Case**: Development and testing
- **Features**: Fast, ephemeral, no persistence
- **Limitations**: Single instance, data loss on restart

#### CockroachDB Backend
- **Use Case**: Production deployments
- **Features**: Distributed, consistent, scalable
- **Benefits**:
  - Strong consistency for task coordination
  - Horizontal scalability
  - Built-in replication and fault tolerance
  - ACID transactions

## Security

### Authentication

HTTP request authentication flow:

```
┌──────────┐
│  Client  │
│          │
└─────┬────┘
      │ HTTP Request with JWT
      │ (X-Goog-IAP-JWT-Assertion header)
      ▼
┌─────────────────┐
│ Load Balancer   │
│  (Optional      │
│   Google IAP)   │
└────────┬────────┘
         │ Forwards request with JWT
         ▼
┌─────────────────────────────────────────┐
│          roachprod-centralized          │
│                                         │
│  ┌───────────────────────────────────┐  │
│  │  Gin Middleware Pipeline          │  │
│  │                                   │  │
│  │  1. Request ID                    │  │
│  │  2. Logging                       │  │
│  │  3. JWT Authentication (optional) │◄─┼─── Configured via
│  │     - Extract JWT from header     │  │    --api-authentication-disabled
│  │     - Validate signature          │  │    --api-authentication-jwt-audience
│  │     - Check audience              │  │
│  │                                   │  │
│  └────────────┬──────────────────────┘  │
│               │                         │
│               ▼                         │
│  ┌───────────────────────────────────┐  │
│  │      Controller Handler           │  │
│  │  (clusters, tasks, dns, health)   │  │
│  └───────────────────────────────────┘  │
└─────────────────────────────────────────┘
```

**Authentication Modes**:
- **Development**: `--api-authentication-disabled=true` (no JWT validation)
- **Production**: JWT validation enabled with audience check

### Security Features

- **JWT Authentication**: Google IAP integration for production
- **Input Validation**: Comprehensive request validation
- **SQL Injection Prevention**: Parameterized queries only
- **Credential Management**: Secure cloud provider credential handling
- **Audit Logging**: Request tracking with unique IDs

## Configuration Management

### Configuration Hierarchy

```
┌─────────────────────────────────┐
│      Configuration Sources      │
│   (Highest to Lowest Priority)  │
└───────────────┬─────────────────┘
                ▼
┌─────────────────────────────────┐
│      Environment Variables      │
│      (ROACHPROD_* prefix)       │
└───────────────┬─────────────────┘
                ▼
┌─────────────────────────────────┐
│       Command Line Flags        │
│ (--api-port, --log-level, etc.) │
└───────────────┬─────────────────┘
                ▼
┌─────────────────────────────────┐
│     YAML Configuration File     │
│   (config.yaml, --config flag)  │
└───────────────┬─────────────────┘
                ▼
┌─────────────────────────────────┐
│         Default Values          │
│       (Built into code)         │
└─────────────────────────────────┘
```

### Configuration Features

- **Hot Reloading**: Not currently supported (restart required)
- **Validation**: Comprehensive configuration validation on startup
- **Environment Support**: Development, staging, production profiles
- **Secrets Management**: Secure handling of sensitive configuration

## Design Patterns

### 1. Dependency Injection

```go
// Service interfaces define contracts
type IClusterService interface {
    GetAllClusters(ctx context.Context, ...) ([]Cluster, error)
}

// Implementation injected at runtime
type ClusterService struct {
    repo clusters.IRepository
    logger *logger.Logger
}

// Factory pattern for service creation
func NewServicesFromConfig(cfg *config.Config) (*Services, error) {
    // Create repositories based on configuration
    // Inject dependencies into services
    // Return configured service collection
}
```

### 2. Repository Pattern

```go
// Abstract interface for data access
type IRepository interface {
    Create(ctx context.Context, cluster *Cluster) error
    GetByName(ctx context.Context, name string) (*Cluster, error)
    Update(ctx context.Context, cluster *Cluster) error
    Delete(ctx context.Context, name string) error
}

// Multiple implementations
type MemoryRepository struct { /* ... */ }
type CockroachDBRepository struct { /* ... */ }
```

### 3. Factory Pattern

- **Service Factory**: Creates service instances based on configuration
- **Repository Factory**: Selects storage backend based on settings
- **Provider Factory**: Instantiates cloud providers dynamically

### 4. Observer Pattern

- **Task Events**: Background workers observe task state changes
- **Health Events**: Health service monitors component status
- **Metrics Events**: Prometheus metrics collection

## Scalability Considerations

### Deployment Modes

The service supports three deployment modes for different scalability and reliability requirements:

#### 1. All-in-One Mode (Default)

Single process handles both API and background work:

```
┌───────────────────────────┐
│         Instance          │
│  ┌─────────────────────┐  │
│  │   API Server        │  │
│  │   + Controllers     │  │
│  └─────────────────────┘  │
│  ┌─────────────────────┐  │
│  │ Background Work     │  │
│  │ - Task Workers      │  │
│  │ - Periodic Sync     │  │
│  │ - Health Heartbeat  │  │
│  └─────────────────────┘  │
└─────────────┬─────────────┘
              │
    ┌─────────▼─────────┐
    │   Database        │
    │ (memory or CRDB)  │
    └───────────────────┘
```

**Use Case**: Development, small production deployments
**Database**: Memory or CockroachDB
**Command**: `roachprod-centralized api`

#### 2. Horizontally Scaled Mode (API + Workers Separation)

Separate API tier and worker tier for independent scaling:

```
               ┌─────────────┐
               │Load Balancer│
               │ (HTTP :80)  │
               └──────┬──────┘
                      │
           ┌──────────┴───────────┐
           │                      │
           ▼                      ▼
┌─────────────────┐      ┌─────────────────┐
│ API Instance 1  │      │ API Instance 2  │
│                 │      │                 │
│ ┌─────────────┐ │      │ ┌─────────────┐ │
│ │ Controllers │ │      │ │ Controllers │ │
│ │ HTTP Routes │ │      │ │ HTTP Routes │ │
│ │ (No Workers)│ │      │ │ (No Workers)│ │
│ └─────────────┘ │      │ └─────────────┘ │
└────────┬────────┘      └────────┬────────┘
         │                        │
         └────────────┬───────────┘
                      │ (Read/write cluster data, enqueue tasks)
                      ▼
             ┌──────────────────┐
             │  CockroachDB     │
             │  (Shared state,  │
             │   task queue)    │
             └──────────────────┘
                      ▲
                      │ (Poll & process tasks, coordinate via DB)
         ┌────────────┴───────────┐
         │                        │
┌────────┴──────────┐   ┌─────────┴─────────┐
│ Worker Instance1  │   │ Worker Instance2  │
│                   │   │                   │
│ ┌───────────────┐ │   │ ┌───────────────┐ │
│ │ Task Workers  │ │   │ │ Task Workers  │ │
│ │ Periodic Sync │ │   │ │ Periodic Sync │ │
│ │ (Metrics Only)│ │   │ │ (Metrics Only)│ │
│ └───────────────┘ │   │ └───────────────┘ │
└───────────────────┘   └───────────────────┘
```

**Use Case**: High-availability, load distribution, independent scaling
**Database**: CockroachDB (required)
**Commands**:
- API instances: `roachprod-centralized api --no-workers`
- Worker instances: `roachprod-centralized workers`

**Benefits**:
- **Independent Scaling**: Scale API and workers separately based on load
- **Fault Isolation**: API failures don't affect background processing
- **Resource Optimization**: Different resource profiles for API vs. workers
- **Zero-Downtime Updates**: Update API and workers independently

**Key Differences from All-in-One**:
- **API instances** (`--no-workers`):
  - Handle HTTP requests only
  - No background work (no periodic sync, no health heartbeats)
  - Multiple instances can run behind load balancer
- **Worker instances** (`workers` command):
  - Process background tasks only
  - Expose metrics endpoint only (no API routes)
  - Coordinate through CockroachDB for distributed task processing

### Horizontal Scaling Characteristics

**API Tier Scaling**:
- **Stateless**: No local state, safe to scale horizontally
- **Load Balancing**: Standard HTTP load balancing across instances
- **Session Affinity**: Not required (stateless design)
- **Health Checks**: `/health` endpoint for LB health checks

**Worker Tier Scaling**:
- **Distributed Coordination**: Workers coordinate via CockroachDB task queue
- **Automatic Load Distribution**: Tasks claimed by available workers
- **No Coordination Required**: Workers operate independently
- **Metrics**: Each worker exposes metrics on separate port

### Performance Characteristics

- **API Throughput**: ~1000 requests/second per instance
- **Task Processing**: Configurable worker count (default: 1-5)
- **Database Connections**: Pooled connections with configurable limits
- **Memory Usage**: ~100MB baseline + working set
- **Startup Time**: < 5 seconds

### Bottlenecks and Mitigation

1. **Database Connections**: Connection pooling and limits
2. **Cloud API Rate Limits**: Exponential backoff and retry logic
3. **Task Queue Contention**: Optimistic concurrency control
4. **Memory Usage**: Streaming responses for large datasets

### Monitoring and Observability

- **Metrics**: Prometheus integration with custom metrics ([see Metrics Reference](METRICS.md))
- **Logging**: Structured logging with correlation IDs
- **Tracing**: Request tracing for debugging
- **Health Checks**: Multi-level health status reporting
