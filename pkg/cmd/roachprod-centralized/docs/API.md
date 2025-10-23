# roachprod-centralized API Reference

This document provides comprehensive documentation for the roachprod-centralized REST API.

**📚 Related Documentation:**
- [← Back to Main README](../README.md)
- [🏗️ Architecture Guide](ARCHITECTURE.md) - System design and components
- [💻 Development Guide](DEVELOPMENT.md) - Local development setup
- [📋 Examples & Workflows](EXAMPLES.md) - Practical usage examples
- [🐳 Docker Deployment](../docker/README.md) - Container deployment

## Table of Contents

- [Overview](#overview)
- [Authentication](#authentication)
- [Base URL](#base-url)
- [Response Format](#response-format)
- [Error Handling](#error-handling)
- [Endpoints](#endpoints)
  - [Health Endpoints](#health-endpoints)
  - [Cluster Management](#cluster-management)
  - [Task Management](#task-management)
  - [DNS Management](#dns-management)
- [Query Parameters](#query-parameters)
- [Examples](#examples)

## Overview

The roachprod-centralized API is a RESTful service that provides programmatic access to:

- **Cluster Management**: CRUD operations for roachprod clusters
- **Task Processing**: Background task creation and monitoring
- **DNS Management**: Public DNS record synchronization
- **Health Monitoring**: Service health and status checks

### API Version

Current API version: `v1` (defined by each controller)

### Content Type

All requests and responses use `application/json` content type unless otherwise specified.

## Authentication

### Development Mode

For development environments, authentication can be disabled:

```bash
export ROACHPROD_API_AUTHENTICATION_DISABLED=true
```

### Production Mode

In production, the API uses JWT authentication with Google IAP:

- **Header**: `X-Goog-IAP-JWT-Assertion` (configurable)
- **Type**: JWT Bearer token
- **Validation**: JWT signature and audience verification

**Authentication Header Example**:
```http
X-Goog-IAP-JWT-Assertion: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```

## Base URL

Default base URL: `http://localhost:8080`

The base path can be configured via `ROACHPROD_API_BASE_PATH` environment variable.

## Response Format

### Success Response

All API responses include these top-level fields:

```json
{
  "request_id": "req_1a2b3c4d5e6f7g8h",
  "result_type": "cloud.Clusters",
  "data": {
    // Response data matching the result_type
  }
}
```

### Error Response

```json
{
  "request_id": "req_1a2b3c4d5e6f7g8h",
  "result_type": "error",
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Invalid cluster name",
    "details": {
      "field": "name",
      "reason": "name must be between 3 and 50 characters"
    }
  }
}
```

### Response Fields

| Field | Type | Description |
|-------|------|-------------|
| `request_id` | string | Unique identifier for the request |
| `result_type` | string | Go struct type contained in the data field |
| `data` | object | Response data (success only) |
| `error` | object | Error information (error only) |

### Common Result Types

| Result Type | Description | Used By |
|-------------|-------------|---------|
| `health.HealthDTO` | Health check response | `/health` |
| `cloud.Clusters` | List of clusters | `/clusters` (GET) |
| `cloud.Cluster` | Single cluster | `/clusters/{name}` (GET) |
| `tasks.Tasks` | List of tasks | `/tasks` (GET) |
| `tasks.Task` | Single task | `/tasks/{id}` (GET) |
| `string` | Simple string response | Various endpoints |

## Error Handling

### HTTP Status Codes

| Status | Description |
|--------|-------------|
| `200`  | Success |
| `201`  | Created |
| `204`  | No Content |
| `400`  | Bad Request - Invalid parameters |
| `401`  | Unauthorized - Authentication required |
| `403`  | Forbidden - Insufficient permissions |
| `404`  | Not Found - Resource not found |
| `409`  | Conflict - Resource already exists |
| `422`  | Unprocessable Entity - Validation failed |
| `500`  | Internal Server Error |

### Common Error Codes

| Code | Description |
|------|-------------|
| `VALIDATION_ERROR` | Request validation failed |
| `CLUSTER_NOT_FOUND` | Cluster does not exist |
| `CLUSTER_ALREADY_EXISTS` | Cluster name already in use |
| `TASK_NOT_FOUND` | Task does not exist |
| `AUTHENTICATION_FAILED` | Invalid or missing authentication |
| `INTERNAL_ERROR` | Server-side error |

## Endpoints

### Health Endpoints

#### GET /v1/health

Basic health check endpoint (no authentication required).

**Response**:
```json
{
  "request_id": "req_1a2b3c4d5e6f7g8h",
  "result_type": "health.HealthDTO",
  "data": {
    "status": "ok",
    "timestamp": "2025-01-15T10:30:00Z"
  }
}
```

**Example**:
```bash
curl http://localhost:8080/v1/health
```

### Cluster Management

#### GET /v1/clusters

Retrieve all clusters with optional filtering.

**Query Parameters**:
- `name` (string, optional): Filter clusters by name (supports partial matching)

**Response**:
```json
{
  "request_id": "req_1a2b3c4d5e6f7g8h",
  "result_type": "cloud.Clusters",
  "data": [
    {
      "name": "test-cluster-1",
      "provider": "gce",
      "region": "us-central1",
      "status": "running",
      "created_at": "2025-01-15T10:00:00Z",
      "updated_at": "2025-01-15T10:30:00Z",
      "nodes": [
        {
          "name": "test-cluster-1-1",
          "external_ip": "34.123.45.67",
          "internal_ip": "10.0.1.10",
          "zone": "us-central1-a"
        }
      ]
    }
  ]
}
```

**Examples**:
```bash
# Get all clusters
curl http://localhost:8080/v1/clusters

# Filter by name
curl "http://localhost:8080/v1/clusters?name=test-cluster"
```

#### GET /v1/clusters/{name}

Retrieve a specific cluster by name.

**Path Parameters**:
- `name` (string, required): Cluster name

**Response**:
```json
{
  "request_id": "req_1a2b3c4d5e6f7g8h",
  "result_type": "cloud.Cluster",
  "data": {
    "name": "test-cluster-1",
    "provider": "gce",
    "region": "us-central1",
    "status": "running",
    "created_at": "2025-01-15T10:00:00Z",
    "updated_at": "2025-01-15T10:30:00Z",
    "nodes": [
      {
        "name": "test-cluster-1-1",
        "external_ip": "34.123.45.67",
        "internal_ip": "10.0.1.10",
        "zone": "us-central1-a"
      }
    ]
  }
}
```

**Example**:
```bash
curl http://localhost:8080/v1/clusters/test-cluster-1
```

#### POST /v1/clusters/register

Registers a new cluster with the state.

**Request Body**:
```json
{
  "name": "new-cluster",
  "provider": "gce",
  "region": "us-central1",
  "nodes": ["new-cluster-1", "new-cluster-2", "new-cluster-3"],
  "vm_type": "n1-standard-4",
  "disk_size": 100
}
```

**Response**: `201 Created`
```json
{
  "request_id": "req_1a2b3c4d5e6f7g8h",
  "result_type": "cloud.Cluster",
  "data": {
    "name": "new-cluster",
    "provider": "gce",
    "region": "us-central1",
    "status": "creating",
    "created_at": "2025-01-15T10:35:00Z",
    "updated_at": "2025-01-15T10:35:00Z"
  }
}
```

**Example**:
```bash
curl -X POST http://localhost:8080/v1/clusters/register \
  -H "Content-Type: application/json" \
  -d '{
    "name": "new-cluster",
    "provider": "gce",
    "region": "us-central1",
    "nodes": ["new-cluster-1", "new-cluster-2", "new-cluster-3"]
  }'
```

#### PUT /v1/clusters/register/{name}

Register an update to an existing cluster with the state.

**Path Parameters**:
- `name` (string, required): Cluster name

**Request Body**: Same as POST /clusters/register

**Response**: `200 OK`
```json
{
  "request_id": "req_1a2b3c4d5e6f7g8h",
  "result_type": "cloud.Cluster",
  "data": {
    // Updated cluster data
  }
}
```

**Example**:
```bash
curl -X PUT http://localhost:8080/v1/clusters/register/test-cluster-1 \
  -H "Content-Type: application/json" \
  -d '{
    "name": "test-cluster-1",
    "provider": "gce",
    "region": "us-central1",
    "nodes": ["test-cluster-1-1", "test-cluster-1-2", "test-cluster-1-3", "test-cluster-1-4"]
  }'
```

#### DELETE /v1/clusters/register/{name}

Register a cluster deletion with the state.

**Path Parameters**:
- `name` (string, required): Cluster name

**Response**: `204 No Content`

**Example**:
```bash
curl -X DELETE http://localhost:8080/v1/clusters/register/test-cluster-1
```

#### POST /v1/clusters/sync

Trigger synchronization of cluster data from cloud providers.

**Response**: `200 OK`
```json
{
  "request_id": "req_1a2b3c4d5e6f7g8h",
  "result_type": "tasks.Task",
  "data": {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "type": "cluster_sync",
    "status": "pending",
    "created_at": "2025-01-15T10:30:00Z"
  }
}
```

**Example**:
```bash
curl -X POST http://localhost:8080/v1/clusters/sync
```

### Task Management

#### GET /v1/tasks

Retrieve all tasks with optional filtering.

**Query Parameters**:
- `status` (string, optional): Filter by task status (`pending`, `running`, `done`, `failed`)
- `type` (string, optional): Filter by task type

**Response**:
```json
{
  "request_id": "req_1a2b3c4d5e6f7g8h",
  "result_type": "tasks.Tasks",
  "data": [
    {
      "id": "550e8400-e29b-41d4-a716-446655440000",
      "type": "cluster_sync",
      "status": "done",
      "payload": {
        "cluster_name": "test-cluster-1"
      },
      "result": {
        "clusters_synced": 5,
        "duration_ms": 2500
      },
      "created_at": "2025-01-15T10:00:00Z",
      "updated_at": "2025-01-15T10:02:30Z"
    }
  ]
}
```

**Examples**:
```bash
# Get all tasks
curl http://localhost:8080/v1/tasks

# Filter by status
curl "http://localhost:8080/v1/tasks?status=running"
```

#### GET /v1/tasks/{id}

Retrieve a specific task by ID.

**Path Parameters**:
- `id` (UUID, required): Task ID

**Response**:
```json
{
  "request_id": "req_1a2b3c4d5e6f7g8h",
  "result_type": "tasks.Task",
  "data": {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "type": "cluster_sync",
    "status": "done",
    "payload": {
      "cluster_name": "test-cluster-1"
    },
    "result": {
      "clusters_synced": 5,
      "duration_ms": 2500
    },
    "created_at": "2025-01-15T10:00:00Z",
    "updated_at": "2025-01-15T10:02:30Z"
  }
}
```

**Example**:
```bash
curl http://localhost:8080/v1/tasks/550e8400-e29b-41d4-a716-446655440000
```

### DNS Management

#### POST /v1/public-dns/sync

Trigger synchronization of public DNS records.

**Response**: `200 OK`
```json
{
  "request_id": "req_1a2b3c4d5e6f7g8h",
  "result_type": "tasks.Task",
  "data": {
    "id": "660e8400-e29b-41d4-a716-446655440001",
    "type": "dns_sync",
    "status": "pending",
    "created_at": "2025-01-15T10:30:00Z"
  }
}
```

**Example**:
```bash
curl -X POST http://localhost:8080/v1/public-dns/sync
```

## Query Parameters

### Filtering

The API supports Stripe-style filtering for GET endpoints:

| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| `name` | string | Exact or partial match | `?name=test` |
| `name[eq]` | string | Exact match | `?name[eq]=test-cluster` |
| `name[ne]` | string | Not equal | `?name[ne]=prod-cluster` |
| `name[in]` | array | In list | `?name[in]=cluster1,cluster2` |
| `name[nin]` | array | Not in list | `?name[nin]=temp,test` |
| `name[regex]` | string | Regex match | `?name[regex]=^prod-.*` |

**Note**: Pagination is not currently implemented but may be added in future versions.

## Examples

### Complete Cluster Workflow

```bash
# 1. Check API health
curl http://localhost:8080/v1/health

# 2. List existing clusters
curl http://localhost:8080/v1/clusters

# 3. Register a new cluster
curl -X POST http://localhost:8080/v1/clusters/register \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my-test-cluster",
    "provider": "gce",
    "region": "us-central1",
    "nodes": ["my-test-cluster-1", "my-test-cluster-2", "my-test-cluster-3"]
  }'

# 4. Check cluster status
curl http://localhost:8080/v1/clusters/my-test-cluster

# 5. Sync cluster data
curl -X POST http://localhost:8080/v1/clusters/sync

# 6. Monitor sync task
RESPONSE=$(curl -s -X POST http://localhost:8080/v1/clusters/sync)
TASK_ID=$(echo $RESPONSE | jq -r '.data.id')
curl http://localhost:8080/v1/tasks/$TASK_ID

# 7. Register the external cluster deletion when done
curl -X DELETE http://localhost:8080/v1/clusters/register/my-test-cluster
```

### Task Monitoring

```bash
# Start a sync operation
RESPONSE=$(curl -s -X POST http://localhost:8080/v1/clusters/sync)
TASK_ID=$(echo $RESPONSE | jq -r '.data.id')

# Monitor task progress
curl http://localhost:8080/v1/tasks/$TASK_ID

# Check all running tasks
curl "http://localhost:8080/v1/tasks?status=running"
```

### Error Handling Example

```bash
# Try to get a non-existent cluster
curl http://localhost:8080/v1/clusters/non-existent

# Response:
{
  "request_id": "req_1a2b3c4d5e6f7g8h",
  "result_type": "error",
  "error": {
    "code": "CLUSTER_NOT_FOUND",
    "message": "cluster not found"
  }
}
```

### Filtering Examples

```bash
# Find clusters with names containing "prod"
curl "http://localhost:8080/v1/clusters?name=prod"

# Find clusters NOT in test or temp categories
curl "http://localhost:8080/v1/clusters?name[nin]=test,temp"

# Get failed tasks
curl "http://localhost:8080/v1/tasks?status=failed"

# Get tasks of specific type
curl "http://localhost:8080/v1/tasks?type=cluster_sync"
```

## Rate Limiting

Currently, no rate limiting is implemented. This may be added in future versions.

## Metrics

Prometheus metrics are exposed on a separate port (default: 8081):

```bash
curl http://localhost:8081/metrics
```

## Changelog

### v1.0.0 (Current)
- Initial API implementation
- Cluster CRUD operations (registration)
- Task management
- DNS synchronization
- Health checks
- Stripe-style query filtering
- Request ID tracking
- Structured response format with result types