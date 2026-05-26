# roachprod-centralized Examples

This document provides practical examples and common workflows for using the roachprod-centralized service.

**📚 Related Documentation:**
- [← Back to Main README](../README.md)
- [🔌 API Reference](API.md) - Complete REST API documentation
- [🏗️ Architecture Guide](ARCHITECTURE.md) - System design and components
- [💻 Development Guide](DEVELOPMENT.md) - Local development setup
- [⚙️ Configuration Examples](../examples/) - Ready-to-use configurations

## Table of Contents

- [Quick Start Examples](#quick-start-examples)
- [Cluster Management Workflows](#cluster-management-workflows)
- [Task Monitoring and Management](#task-monitoring-and-management)
- [DNS Management](#dns-management)
- [Troubleshooting](#troubleshooting)

## Quick Start Examples

### Basic Setup and Health Check

```bash
# 1. Start the service in development mode
export ROACHPROD_API_AUTHENTICATION_METHOD=disabled
export ROACHPROD_DATABASE_TYPE=memory
export ROACHPROD_LOG_LEVEL=info

./dev run roachprod-centralized api

# 2. Verify the service is running
curl http://localhost:8080/v1/health

# Expected response:
{
  "request_id": "req_1a2b3c4d5e6f7g8h",
  "result_type": "health.HealthDTO",
  "data": {
    "status": "ok",
    "timestamp": "2025-01-15T10:30:00Z"
  }
}
```

### First API Call

```bash
# List all clusters (should be empty initially)
curl http://localhost:8080/v1/clusters

# Expected response:
{
  "request_id": "req_2b3c4d5e6f7g8h9i",
  "result_type": "cloud.Clusters",
  "data": []
}
```

## Cluster Management Workflows

### Complete Cluster Lifecycle

```bash
API_BASE="http://localhost:8080/v1"

# 1. Register a new test cluster
echo "Creating test cluster..."
CLUSTER_RESPONSE=$(curl -s -X POST $API_BASE/clusters/register \
  -H "Content-Type: application/json" \
  -d '{
    "name": "example-test-cluster",
    "provider": "gce",
    "region": "us-central1",
    "nodes": ["example-test-cluster-1", "example-test-cluster-2", "example-test-cluster-3"]
  }')

echo "Cluster creation response:"
echo $CLUSTER_RESPONSE | jq .

# 2. Verify cluster was created
echo -e "\nFetching cluster details..."
curl -s $API_BASE/clusters/example-test-cluster | jq .

# 3. Register a cluster update (add a node)
echo -e "\nUpdating cluster to add a node..."
curl -s -X PUT $API_BASE/clusters/register/example-test-cluster \
  -H "Content-Type: application/json" \
  -d '{
    "name": "example-test-cluster",
    "provider": "gce",
    "region": "us-central1",
    "nodes": ["example-test-cluster-1", "example-test-cluster-2", "example-test-cluster-3", "example-test-cluster-4"]
  }' | jq .

# 4. Trigger cluster sync
echo -e "\nTriggering cluster sync..."
SYNC_RESPONSE=$(curl -s -X POST $API_BASE/clusters/sync)
TASK_ID=$(echo $SYNC_RESPONSE | jq -r '.data.id')
echo "Sync task ID: $TASK_ID"

# 5. Monitor sync task
for i in {1..10}; do
  TASK_STATUS=$(curl -s $API_BASE/tasks/$TASK_ID | jq -r '.data.status')
  echo "Task status: $TASK_STATUS"

  if [[ "$TASK_STATUS" == "done" || "$TASK_STATUS" == "failed" ]]; then
    break
  fi

  sleep 2
done

# 6. Get final task details
curl -s $API_BASE/tasks/$TASK_ID | jq .

# 7. Register the deletion of the cluster
curl -s -X DELETE $API_BASE/clusters/register/example-test-cluster
```

### Filtering Clusters

```bash
API_BASE="http://localhost:8080/v1"

# Find clusters with names containing "prod"
curl "$API_BASE/clusters?name=prod"

# Find clusters NOT in test or temp categories
curl "$API_BASE/clusters?name[nin]=test,temp"

# List all clusters
curl "$API_BASE/clusters" | jq '.data[].name'
```

## Task Monitoring and Management

### Monitoring Tasks

```bash
API_BASE="http://localhost:8080/v1"

# Start a sync operation
SYNC_RESPONSE=$(curl -s -X POST $API_BASE/clusters/sync)
TASK_ID=$(echo $SYNC_RESPONSE | jq -r '.data.id')

# Monitor task progress
while true; do
  TASK_STATUS=$(curl -s $API_BASE/tasks/$TASK_ID | jq -r '.data.status')
  echo "Task status: $TASK_STATUS"

  if [[ "$TASK_STATUS" == "done" || "$TASK_STATUS" == "failed" ]]; then
    break
  fi

  sleep 2
done

# Get final result
curl $API_BASE/tasks/$TASK_ID | jq .
```

### Filtering Tasks

```bash
API_BASE="http://localhost:8080/v1"

# Get failed tasks
curl "$API_BASE/tasks?status=failed"

# Get tasks of specific type
curl "$API_BASE/tasks?type=cluster_sync"

# Get running tasks
curl "$API_BASE/tasks?status=running"

# Task statistics
ALL_TASKS=$(curl -s $API_BASE/tasks | jq '.data[]')

TOTAL=$(echo $ALL_TASKS | jq -s 'length')
PENDING=$(echo $ALL_TASKS | jq -s 'map(select(.status == "pending")) | length')
RUNNING=$(echo $ALL_TASKS | jq -s 'map(select(.status == "running")) | length')
DONE=$(echo $ALL_TASKS | jq -s 'map(select(.status == "done")) | length')
FAILED=$(echo $ALL_TASKS | jq -s 'map(select(.status == "failed")) | length')

echo "Total: $TOTAL, Pending: $PENDING, Running: $RUNNING, Done: $DONE, Failed: $FAILED"
```

## DNS Management

### DNS Synchronization

```bash
API_BASE="http://localhost:8080/v1"

# Trigger DNS sync
DNS_RESPONSE=$(curl -s -X POST $API_BASE/public-dns/sync)
TASK_ID=$(echo $DNS_RESPONSE | jq -r '.data.id')

# Monitor DNS sync
while true; do
  TASK_STATUS=$(curl -s $API_BASE/tasks/$TASK_ID | jq -r '.data.status')
  echo "DNS sync status: $TASK_STATUS"

  if [[ "$TASK_STATUS" == "done" || "$TASK_STATUS" == "failed" ]]; then
    echo -e "\nFinal result:"
    curl -s $API_BASE/tasks/$TASK_ID | jq '.data'
    break
  fi

  sleep 2
done

# List all DNS sync tasks
curl "$API_BASE/tasks?type=dns_sync" | jq '.data[] | {id: .id, status: .status, created_at: .created_at}'
```

## Troubleshooting

### Service Won't Start

```bash
# Check if port is already in use
lsof -i :8080

# Check for existing processes
pgrep -f roachprod-centralized

# Start with minimal configuration
export ROACHPROD_API_AUTHENTICATION_METHOD=disabled
export ROACHPROD_DATABASE_TYPE=memory
export ROACHPROD_LOG_LEVEL=debug
./dev run roachprod-centralized api

# Test health endpoint
curl http://localhost:8080/v1/health
```

### Database Connection Issues

```bash
# Test with memory database
export ROACHPROD_DATABASE_TYPE=memory
./dev run roachprod-centralized api

# Test CockroachDB connection
DB_URL="postgresql://root@localhost:26257/roachprod?sslmode=disable"
cockroach sql --url="$DB_URL" --execute="SELECT 1;"

# Use CockroachDB
export ROACHPROD_DATABASE_TYPE=cockroachdb
export ROACHPROD_DATABASE_URL="$DB_URL"
./dev run roachprod-centralized api
```

### Authentication Errors

```bash
# Disable authentication for development
export ROACHPROD_API_AUTHENTICATION_METHOD=disabled
./dev run roachprod-centralized api
```

### Checking Logs

```bash
# Enable debug logging
export ROACHPROD_LOG_LEVEL=debug
./dev run roachprod-centralized api

# Check specific operations
curl -X POST http://localhost:8080/v1/clusters/sync
# Watch logs for detailed traces
```

### Health Monitoring

```bash
# Check API health
curl http://localhost:8080/v1/health

# Check metrics
curl http://localhost:8081/metrics | grep roachprod
```
