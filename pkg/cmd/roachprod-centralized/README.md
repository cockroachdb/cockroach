# roachprod-centralized

A centralized REST API service for managing CockroachDB roachprod clusters, tasks, and cloud provider operations.

## Table of Contents

- [Overview](#overview)
- [Quick Start](#quick-start)
- [CLI Usage](#cli-usage)
- [Configuration](#configuration)
- [Architecture](#architecture)
- [Deployment](#deployment)
- [Development](#development)
- [Troubleshooting](#troubleshooting)
- [Documentation](#documentation)

## Overview

The `roachprod-centralized` service provides a unified HTTP API for:

- **Cluster Management**: Create, monitor, and manage roachprod clusters across multiple cloud providers
- **Task Processing**: Distributed background task system for cluster operations
- **DNS Management**: Public DNS record management for clusters
- **Multi-Cloud Support**: AWS, GCE, Azure, and IBM cloud provider integration
- **Health Monitoring**: Comprehensive health checks and metrics

## Quick Start

### Prerequisites

- CockroachDB development environment (run `./dev doctor` to verify)
- Access to cloud provider credentials (AWS, GCE, Azure, or IBM)

### 1. Build the Application

```bash
# From the CockroachDB repository root
./dev build roachprod-centralized
```

### 2. Basic Configuration

```bash
# Set minimum required configuration
export ROACHPROD_API_AUTHENTICATION_TYPE=disabled
export ROACHPROD_DATABASE_TYPE=memory
```

### 3. Start the API Server

```bash
./dev run roachprod-centralized api
```

The API will be available at `http://localhost:8080` with metrics at `http://localhost:8081/metrics`.

## CLI Usage

The `roachprod-centralized` command provides the following subcommands:

### API Server

```bash
# Start the API server with default configuration (all-in-one mode)
roachprod-centralized api

# Start with custom configuration file
roachprod-centralized api --config /path/to/config.yaml

# Start with specific port
roachprod-centralized api --api-port 9090

# Start API-only mode without task workers (requires CockroachDB)
roachprod-centralized api --no-workers --database-type cockroachdb

# View all available options
roachprod-centralized api --help
```

### Workers (Task Processors)

```bash
# Start dedicated task workers with metrics endpoint (requires CockroachDB)
roachprod-centralized workers --database-type cockroachdb --database-url "postgresql://..."

# Start workers with custom configuration
roachprod-centralized workers --config /path/to/config.yaml

# Configure number of concurrent workers
roachprod-centralized workers --tasks-workers 5

# View all available options
roachprod-centralized workers --help
```

### Deployment Modes

The service supports three deployment modes for flexibility and scalability:

| Mode | Command | Use Case | Database | Workers | API | Background Tasks |
|------|---------|----------|----------|---------|-----|------------------|
| **All-in-one** | `api` | Development, small deployments | memory or cockroachdb | ✓ | ✓ | ✓ |
| **API-only** | `api --no-workers` | Horizontally scaled API tier | cockroachdb (required) | ✗ | ✓ | ✗ |
| **Workers-only** | `workers` | Horizontally scaled task processing | cockroachdb (required) | ✓ | ✗ (metrics only) | ✓ |

**All-in-one mode** (default):
- Single process handles both API requests and background tasks
- Suitable for development and small production deployments
- Can use either memory or CockroachDB backend

**API-only mode** (`--no-workers`):
- Multiple API instances can handle HTTP requests concurrently
- Background task scheduling is disabled (no periodic refreshes, no health heartbeats)
- Requires CockroachDB for distributed coordination
- Use with separate workers instances for task processing

**Workers-only mode** (`workers` command):
- Dedicated task processing instances
- Only exposes metrics endpoint (no API routes)
- Multiple workers can run concurrently, coordinating through CockroachDB
- Requires CockroachDB for distributed task queue

**Smart Initial Sync**:
When instances start, they intelligently decide whether to sync cluster data from cloud providers:
- **Skip sync** if another instance synced recently (within 10 min) AND both workers and API coverage exists
- **Perform sync** if no recent sync, stale sync, missing workers, or no API instances to receive cluster operations
- This prevents redundant syncs in distributed setups while ensuring fresh data when needed
- Initial sync uses the task system and blocks API startup until complete, ensuring no stale data is served

**Example: Scaled Production Setup**
```bash
# Terminal 1: API instance 1 (no workers)
roachprod-centralized api --no-workers --api-port 8080 --database-type cockroachdb

# Terminal 2: API instance 2 (no workers)
roachprod-centralized api --no-workers --api-port 8090 --database-type cockroachdb

# Terminal 3: Workers instance 1
roachprod-centralized workers --tasks-workers 5 --api-metrics-port 9081

# Terminal 4: Workers instance 2
roachprod-centralized workers --tasks-workers 5 --api-metrics-port 9082
```

### Available Flags

Key configuration flags (all can be set via environment variables):

```bash
--api-port                     HTTP API port (default: 8080)
--api-base-path               Base URL path for API endpoints
--api-metrics-enabled         Enable metrics collection (default: true)
--api-metrics-port            Metrics HTTP port (default: 8081)
--api-authentication-disabled Disable API authentication (default: false)
--database-type               Database type: memory|cockroachdb (default: memory)
--database-url                Database connection URL
--log-level                   Logging level: debug|info|warn|error (default: info)
--tasks-workers               Number of background task workers (default: 1)
--no-workers                  Run API without task workers (api command only, requires CockroachDB)
```

## Configuration

### Environment Variables

All configuration can be set via environment variables with the `ROACHPROD_` prefix:

```bash
# Core API settings
export ROACHPROD_API_PORT=8080
export ROACHPROD_API_METRICS_ENABLED=true
export ROACHPROD_LOG_LEVEL=info

# Authentication disabled (for development)
export ROACHPROD_API_AUTHENTICATION_METHOD=disabled

# Authentication via GCP Identity-Aware Proxy
export ROACHPROD_API_AUTHENTICATION_METHOD=jwt
export ROACHPROD_API_AUTHENTICATION_JWT_HEADER="X-Goog-IAP-JWT-Assertion"
export ROACHPROD_API_AUTHENTICATION_JWT_AUDIENCE="your-audience"

# Database configuration
export ROACHPROD_DATABASE_TYPE=cockroachdb
export ROACHPROD_DATABASE_URL="postgresql://user:password@localhost:26257/roachprod?sslmode=require"
export ROACHPROD_DATABASE_MAX_CONNS=10

# Task processing
export ROACHPROD_TASKS_WORKERS=3
```

### Configuration File

Create a YAML configuration file for more complex setups:

```yaml
Log:
  Level: info
API:
  Port: 8080
  BasePath: "/api"
  Metrics:
    Enabled: true
    Port: 8081
  Authentication:
    Disabled: false
    JWT:
      Header: "X-Goog-IAP-JWT-Assertion"
      Audience: "your-audience"
Database:
  Type: cockroachdb
  URL: "postgresql://user:password@localhost:26257/roachprod?sslmode=require"
  MaxConns: 10
  MaxIdleTime: 300
Tasks:
  Workers: 3
```

### Cloud Provider Configuration

See [docs/CLOUD_PROVIDER_CONFIG.md](docs/CLOUD_PROVIDER_CONFIG.md) for detailed cloud provider setup.

**Quick Examples:**
- [Configuration file](examples/development-config.yaml) - Development and production examples
- [Cloud config example](examples/cloud_config.yaml.example) - Multi-cloud provider setup
- [Docker Compose](examples/docker-compose.yml) - Local testing environment

## Architecture

The service follows a clean architecture pattern:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Controllers   │────│    Services     │────│  Repositories   │
│  (HTTP Layer)   │    │ (Business Logic)│    │  (Data Layer)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         │              ┌─────────────────┐             │
         └──────────────│   Background    │─────────────┘
                        │   Task System   │
                        └─────────────────┘
```

**Key Components:**
- **Controllers**: HTTP request handlers (`controllers/`)
- **Services**: Business logic and orchestration (`services/`)
- **Repositories**: Data persistence abstraction (`repositories/`)
- **Models**: Data structures and entities (`models/`)
- **Utils**: Shared utilities and helpers (`utils/`)

**Authorization Boundary (Important):**
- Controllers enforce coarse endpoint access (authentication + required permission family).
- Services enforce fine-grained authorization (scope/environment, ownership, and resource-level checks).
- Service-layer authorization must use trusted data (stored resource state or server config), not only request payload.

For detailed architecture information, see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Deployment

### Single-Instance Deployment (All-in-one)

For small production deployments or development:

```bash
# Enable authentication
export ROACHPROD_API_AUTHENTICATION_TYPE=bearer
export ROACHPROD_API_AUTHENTICATION_BEARER_OKTA_ISSUER="https://your-org.okta.com"
export ROACHPROD_API_AUTHENTICATION_BEARER_OKTA_AUDIENCE="your-audience"
export ROACHPROD_API_AUTHENTICATION_BEARER_OKTA_CLIENT_ID="your-client-id"
export ROACHPROD_API_AUTHENTICATION_BEARER_OKTA_CLIENT_SECRET="your-client-secret"

# Use CockroachDB backend
export ROACHPROD_DATABASE_TYPE=cockroachdb
export ROACHPROD_DATABASE_URL="postgresql://user:password@prod-cluster:26257/roachprod?sslmode=require"

# Configure workers
export ROACHPROD_TASKS_WORKERS=5

# Start the service
roachprod-centralized api
```

### Horizontally Scaled Deployment

For high-availability and load distribution, run separate API and worker instances:

**1. API Instances** (scale horizontally for HTTP load):
```bash
# API Instance 1
export ROACHPROD_DATABASE_TYPE=cockroachdb
export ROACHPROD_DATABASE_URL="postgresql://user:password@prod-cluster:26257/roachprod?sslmode=require"
export ROACHPROD_API_PORT=8080
roachprod-centralized api --no-workers

# API Instance 2 (different server/container)
export ROACHPROD_DATABASE_TYPE=cockroachdb
export ROACHPROD_DATABASE_URL="postgresql://user:password@prod-cluster:26257/roachprod?sslmode=require"
export ROACHPROD_API_PORT=8080
roachprod-centralized api --no-workers
```

**2. Worker Instances** (scale horizontally for task processing):
```bash
# Worker Instance 1
export ROACHPROD_DATABASE_TYPE=cockroachdb
export ROACHPROD_DATABASE_URL="postgresql://user:password@prod-cluster:26257/roachprod?sslmode=require"
export ROACHPROD_TASKS_WORKERS=5
export ROACHPROD_API_METRICS_PORT=9081
roachprod-centralized workers

# Worker Instance 2 (different server/container)
export ROACHPROD_DATABASE_TYPE=cockroachdb
export ROACHPROD_DATABASE_URL="postgresql://user:password@prod-cluster:26257/roachprod?sslmode=require"
export ROACHPROD_TASKS_WORKERS=5
export ROACHPROD_API_METRICS_PORT=9082
roachprod-centralized workers
```

**3. Load Balancer Configuration** (for API instances):
- Point load balancer to multiple API instances on port 8080
- Health check endpoint: `GET /health`
- Workers don't need load balancing (they coordinate via CockroachDB)

### Production Configuration Checklist

1. **Enable Bearer Authentication**:
   ```bash
   export ROACHPROD_API_AUTHENTICATION_TYPE=bearer
   export ROACHPROD_API_AUTHENTICATION_BEARER_OKTA_ISSUER="https://your-org.okta.com"
   export ROACHPROD_API_AUTHENTICATION_BEARER_OKTA_AUDIENCE="your-audience"
   export ROACHPROD_API_AUTHENTICATION_BEARER_OKTA_CLIENT_ID="your-client-id"
   export ROACHPROD_API_AUTHENTICATION_BEARER_OKTA_CLIENT_SECRET="your-client-secret"
   ```

2. **Use CockroachDB Backend** (required for scaled deployments):
   ```bash
   export ROACHPROD_DATABASE_TYPE=cockroachdb
   export ROACHPROD_DATABASE_URL="postgresql://user:password@prod-cluster:26257/roachprod?sslmode=require"
   ```

3. **Bootstrap SCIM Provisioning**:
   ```bash
   # Generate a bootstrap token for initial SCIM setup (first startup only)
   export ROACHPROD_BOOTSTRAP_SCIM_TOKEN="rp\$sa\$1\$$(openssl rand -base64 32 | tr -dc 'a-zA-Z0-9' | head -c 43)"
   ```
   On first startup, this creates a short-lived (6 hour) service account for configuring Okta SCIM.
   See [docs/services/AUTH.md](docs/services/AUTH.md#bootstrap-configuration) for details.

4. **Configure Cloud Providers**: Set up cloud provider credentials as detailed in [docs/CLOUD_PROVIDER_CONFIG.md](docs/CLOUD_PROVIDER_CONFIG.md)

5. **Resource Limits**:
   ```bash
   export ROACHPROD_DATABASE_MAX_CONNS=20
   export ROACHPROD_TASKS_WORKERS=5  # Per worker instance
   ```

6. **Monitoring**:
   - Collect Prometheus metrics from `:8081/metrics` (API instances)
   - Collect Prometheus metrics from workers' metrics ports
   - Monitor health endpoints: `/health` and `/health/detailed`

### Docker Deployment

See [docker/README.md](docker/README.md) for containerized deployment options.

### Health Checks

The service provides comprehensive health checks:

- **API Health**: `GET /health` - Basic API availability
- **Detailed Health**: `GET /health/detailed` - Component-level status
- **Metrics**: `GET :8081/metrics` - Prometheus metrics

## Development

For local development setup and contribution guidelines, see [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md).

### Running Tests

```bash
# Run all tests
./dev test pkg/cmd/roachprod-centralized/...

# Run specific package tests
./dev test pkg/cmd/roachprod-centralized/services/clusters

# Run with race detection
./dev test pkg/cmd/roachprod-centralized/... --race
```

### Code Generation

After modifying protocol buffers or other generated code:

```bash
./dev generate
```

## Troubleshooting

### Common Issues

**1. Authentication Errors**
```
Error: authentication failed
```
**Solution**: For development, disable authentication:
```bash
export ROACHPROD_API_AUTHENTICATION_METHOD=disabled
```

**2. Database Connection Issues**
```
Error: failed to connect to database
```
**Solution**: Check database configuration and connectivity:
```bash
# For development, use in-memory storage
export ROACHPROD_DATABASE_TYPE=memory

# Or verify CockroachDB connection
psql "postgresql://user:password@localhost:26257/roachprod?sslmode=require"
```

**3. Cloud Provider Configuration**
```
Error: failed to initialize cloud provider
```
**Solution**: Verify cloud provider credentials are properly configured. See [docs/CLOUD_PROVIDER_CONFIG.md](docs/CLOUD_PROVIDER_CONFIG.md).

**4. Port Already in Use**
```
Error: bind: address already in use
```
**Solution**: Change the API port:
```bash
export ROACHPROD_API_PORT=9090
```

**5. --no-workers with Memory Database**
```
Error: --no-workers cannot be used with memory database backend
```
**Solution**: The `--no-workers` flag requires CockroachDB for distributed coordination:
```bash
export ROACHPROD_DATABASE_TYPE=cockroachdb
export ROACHPROD_DATABASE_URL="postgresql://..."
roachprod-centralized api --no-workers
```

**6. Workers Command with Memory Database**
```
Error: workers command requires database.type=cockroachdb
```
**Solution**: The `workers` command requires CockroachDB for distributed task coordination:
```bash
export ROACHPROD_DATABASE_TYPE=cockroachdb
export ROACHPROD_DATABASE_URL="postgresql://..."
roachprod-centralized workers
```

**7. Tasks Not Being Processed**
```
Tasks remain in pending state indefinitely
```
**Solution**: Verify workers are running:
- In all-in-one mode, check `ROACHPROD_TASKS_WORKERS` is > 0
- In scaled mode, ensure at least one `workers` instance is running
- Check worker logs for errors: `export ROACHPROD_LOG_LEVEL=debug`

**8. Background Work Not Running in API-only Mode**
```
Clusters not syncing, health checks not working
```
**Expected behavior**: When using `--no-workers`, background work is intentionally disabled:
- Periodic cluster refresh: disabled
- Health heartbeats: disabled
- Automatic task scheduling: disabled

This is correct - background work that schedules tasks shouldn't run without workers. To enable background work, run dedicated `workers` instances.

### Debugging

Enable debug logging for detailed troubleshooting:

```bash
export ROACHPROD_LOG_LEVEL=debug
```

**Checking Service Logs:**
```bash
# Look for these log messages to verify correct mode:
# API-only mode:
#   "health service: skipping instance registration (workers disabled)"
#   "clusters service: skipping background work (workers disabled)"
#   "Task workers disabled (Workers=0), skipping task processing routine"

# Workers mode:
#   "Starting in metrics-only mode (workers)"
#   "Starting tasks processing routine"
```

## Documentation

### General Documentation
- **[API Reference](docs/API.md)** - Complete REST API documentation
- **[Architecture Guide](docs/ARCHITECTURE.md)** - System design and components
- **[Development Guide](docs/DEVELOPMENT.md)** - Local setup and contribution guidelines
- **[Cloud Provider Configuration](docs/CLOUD_PROVIDER_CONFIG.md)** - Multi-cloud setup
- **[Metrics Reference](docs/METRICS.md)** - Prometheus metrics documentation
- **[Examples](docs/EXAMPLES.md)** - Common workflows and use cases
- **[Docker Deployment](docker/README.md)** - Container deployment guide

### Component Documentation
- **[Clusters Service Guide](docs/services/CLUSTERS.md)** - Clusters service
- **[Tasks Service Guide](docs/services/TASKS.md)** - Background task system, creating tasks, task hydration

### Related CockroachDB Documentation

- **[Main Documentation](https://cockroachlabs.com/docs/stable/)**
- **[Roachprod Documentation](https://cockroachlabs.com/docs/stable/roachprod.html)**
- **[CockroachDB Contributing Guide](../../CONTRIBUTING.md)**
