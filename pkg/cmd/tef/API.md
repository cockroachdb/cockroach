# TEF REST API Reference

This document provides a comprehensive reference for the TEF REST API.

**Current Status**: REST API is not yet implemented. This document describes the planned API interface.

## Overview

The REST API provides HTTP endpoints for executing and monitoring plans. All endpoints return JSON responses.

Base URL: `http://localhost:25780` (configurable via `--port` flag)

## Quick Start

```bash
# Start the API server
./bin/tef serve --port 25780

# List available plans
curl http://localhost:25780/v1/plans

# Execute a plan
curl -X POST http://localhost:25780/v1/plans/demo-dev/executions \
  -H "Content-Type: application/json" \
  -d '{"request": {"message": "Hello", "count": 5}}'

# Check execution status
curl http://localhost:25780/v1/plans/demo-dev/executions/<workflow-id>
```

## Endpoints Overview

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/v1/plans` | List all available plans |
| GET | `/v1/plans/{plan_id}/executions` | List executions for a plan |
| POST | `/v1/plans/{plan_id}/executions` | Execute a plan |
| GET | `/v1/plans/{plan_id}/executions/{execution_id}` | Get execution status |
| POST | `/v1/plans/{plan_id}/executions/{execution_id}/steps/{step_id}/resume` | Resume async task |

## Authentication

**Current Implementation**: No authentication required.

**Future**: Will support API key authentication and/or OAuth2.

## Error Responses

All error responses follow this format:

```json
{
  "error": "Error message describing what went wrong"
}
```

Common HTTP status codes:
* `200 OK`: Success
* `400 Bad Request`: Invalid input
* `404 Not Found`: Resource not found
* `500 Internal Server Error`: Server error

## API Endpoints

### 1. List Plans

Get all available plan instances registered with the server.

**Endpoint:** `GET /v1/plans`

**Request:**
```bash
curl http://localhost:25780/v1/plans
```

**Response (200 OK):**
```json
{
  "plans": [
    {
      "plan_id": "demo-dev",
      "plan_name": "demo",
      "plan_description": "Demo plan showcasing TEF features"
    },
    {
      "plan_id": "pua-prod",
      "plan_name": "pua",
      "plan_description": "Performance Under Adversity testing"
    }
  ]
}
```

**Response Fields:**
* `plans`: Array of plan information objects
  * `plan_id`: Unique identifier for the plan instance (format: `{plan_name}-{variant}`)
  * `plan_name`: The plan's base name
  * `plan_description`: Human-readable description from the plan's Registry

**Error Response (500):**
```json
{
  "error": "failed to list plans: <error details>"
}
```

### 2. Execute Plan

Start a new execution of the specified plan.

**Endpoint:** `POST /v1/plans/{plan_id}/executions`

**Path Parameters:**
* `plan_id`: Plan identifier (e.g., "demo-dev")

**Request:**
```bash
curl -X POST http://localhost:25780/v1/plans/demo-dev/executions \
  -H "Content-Type: application/json" \
  -d '{
    "request": {
      "message": "Hello TEF",
      "count": 5
    },
    "flags": {
      "flag1": "value1"
    }
  }'
```

**Request Body:**
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

**Request Fields:**
* `request`: Plan-specific input parameters (structure varies per plan)
* `flags`: Optional execution flags (currently unused, reserved for future use)

**Response (200 OK):**
```json
{
  "workflow_id": "demo-20250126-123456-abc123",
  "plan_id": "demo-dev"
}
```

**Response Fields:**
* `workflow_id`: Unique identifier for this execution instance
* `plan_id`: The plan that was executed

**Error Responses:**

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

Get all executions for a specific plan instance.

**Endpoint:** `GET /v1/plans/{plan_id}/executions`

**Path Parameters:**
* `plan_id`: Plan identifier (e.g., "demo-dev")

**Request:**
```bash
curl http://localhost:25780/v1/plans/demo-dev/executions
```

**Response (200 OK):**
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

**Response Fields:**
* `executions`: Array of execution summary objects
  * `workflow_id`: Unique identifier for the execution
  * `run_id`: Framework-specific execution ID (e.g., Temporal run ID)
  * `status`: Current status (Running, Completed, Failed, Terminated, etc.)
  * `start_time`: ISO 8601 formatted start time (optional)
* `plan_id`: The plan instance ID
* `plan_name`: The plan's base name

**Error Responses:**

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

Get detailed status information for a specific execution.

**Endpoint:** `GET /v1/plans/{plan_id}/executions/{execution_id}`

**Path Parameters:**
* `plan_id`: Plan identifier (e.g., "demo-dev")
* `execution_id`: Workflow ID from execution response

**Request:**
```bash
curl http://localhost:25780/v1/plans/demo-dev/executions/demo-20250126-123456-abc123
```

**Response (200 OK):**
```json
{
  "workflow_id": "demo-20250126-123456-abc123",
  "status": "Running",
  "current_tasks": ["process branch 1", "process branch 2"],
  "workflow": {
    "name": "demo-dev",
    "description": "Demo plan showcasing TEF features",
    "first_task": "print message",
    "output_task": "output",
    "input": {
      "message": "Hello",
      "count": 5
    },
    "tasks": {
      "print message": {
        "name": "print message",
        "type": "ExecutionTask",
        "executor": "print message",
        "params": [],
        "next": "check count",
        "fail": "end",
        "status": "Completed",
        "start_time": "2025-01-26T12:34:56Z",
        "end_time": "2025-01-26T12:34:57Z",
        "input": {"message": "Hello"},
        "output": {"result": "printed"}
      },
      "check count": {
        "name": "check count",
        "type": "ConditionTask",
        "executor": "check count",
        "params": [],
        "then": "parallel processing",
        "else": "wait",
        "status": "Completed",
        "start_time": "2025-01-26T12:34:57Z",
        "end_time": "2025-01-26T12:34:57Z",
        "output": true
      },
      "parallel processing": {
        "name": "parallel processing",
        "type": "ForkTask",
        "fork_tasks": ["process branch 1", "process branch 2"],
        "next": "wait",
        "status": "InProgress",
        "start_time": "2025-01-26T12:34:57Z"
      }
    }
  }
}
```

**Response Fields:**
* `workflow_id`: Unique identifier for this execution
* `status`: Overall workflow status (Running, Completed, Failed, etc.)
* `current_tasks`: Array of task names currently executing (for Running workflows)
* `workflow`: Detailed workflow information
  * `name`: Workflow/plan name
  * `description`: Plan description
  * `first_task`: Name of the starting task
  * `output_task`: Name of the task that produces the final output
  * `input`: The input data provided to the workflow
  * `output`: The final output (only present when workflow is Completed)
  * `tasks`: Map of task name to TaskInfo

**TaskInfo Structure:**

Common fields for all task types:
* `name`: Task identifier
* `type`: Task type (ExecutionTask, ForkTask, ConditionTask, AsyncTask, ChildWorkflowTask, EndTask)
* `status`: Task execution status (Pending, InProgress, Completed, Failed)
* `start_time`: ISO 8601 formatted start time (optional)
* `end_time`: ISO 8601 formatted end time (optional)
* `input`: Input data passed to the task (optional)
* `output`: Output data produced by the task (optional)
* `error`: Error message if task failed (optional)
* `properties`: Additional task-specific properties (optional)

ExecutionTask-specific fields:
* `executor`: Name of the executor function
* `params`: Array of task names whose outputs are passed as parameters
* `next`: Name of the next task on success
* `fail`: Name of the task to execute on failure (optional)

ForkTask-specific fields:
* `fork_tasks`: Array of task names to execute in parallel
* `next`: Name of the task to execute after all branches complete
* `fail`: Name of the task to execute if any branch fails (optional)

ConditionTask-specific fields:
* `executor`: Name of the boolean executor function
* `params`: Array of task names whose outputs are passed as parameters
* `then`: Name of the task to execute if condition is true
* `else`: Name of the task to execute if condition is false (optional)

AsyncTask-specific fields:
* `execution_fn`: Name of the executor that starts async work (returns step ID)
* `result_processor_fn`: Name of the executor that processes results
* `params`: Array of task names whose outputs are passed as parameters
* `next`: Name of the next task after async work completes
* `fail`: Name of the task to execute on failure (optional)
* `step_id`: The step ID returned by execution_fn (optional)

ChildWorkflowTask-specific fields:
* `executor`: Name of the executor function that returns ChildTaskInfo
* `params`: Array of task names whose outputs are passed as parameters
* `child_plan_id`: ID of the child plan to execute (determined at runtime)
* `child_input`: Input data for the child plan (determined at runtime)
* `next`: Name of the next task after child plan completes
* `fail`: Name of the task to execute if child plan fails (optional)

**Error Responses:**

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

### 5. Resume Async Task

Resume an async task that is waiting for external completion.

**Endpoint:** `POST /v1/plans/{plan_id}/executions/{execution_id}/steps/{step_id}/resume`

**Path Parameters:**
* `plan_id`: Plan identifier (e.g., "batch-import-prod")
* `execution_id`: Workflow ID
* `step_id`: Step ID returned by AsyncTask's ExecutionFn

**Request:**
```bash
curl -X POST http://localhost:25780/v1/plans/batch-import-prod/executions/batch-import-abc123/steps/import-job-xyz789/resume \
  -H "Content-Type: application/json" \
  -d '{"status": "completed", "rows_imported": 10000}'
```

**Request Body:**
```json
{
  "status": "completed",
  "rows_imported": 10000,
  "result": "job finished successfully"
}
```

The request body is passed to the AsyncTask's ResultProcessorFn as the `result` parameter.

**Response (200 OK):**
```json
{
  "success": true,
  "message": "Async task resumed successfully"
}
```

**Error Responses:**

400 Bad Request:
```json
{
  "error": "invalid plan ID: <error details>"
}
```

404 Not Found - Plan:
```json
{
  "error": "plan batch-import not found"
}
```

404 Not Found - Workflow:
```json
{
  "error": "workflow batch-import-abc123 not found"
}
```

404 Not Found - Step:
```json
{
  "error": "step import-job-xyz789 not found"
}
```

500 Internal Server Error:
```json
{
  "error": "failed to resume async task: <error details>"
}
```

## Example Workflows

### Complete Execution Flow

```bash
# 1. List available plans
curl http://localhost:25780/v1/plans

# 2. Execute a plan
WORKFLOW_ID=$(curl -X POST http://localhost:25780/v1/plans/demo-dev/executions \
  -H "Content-Type: application/json" \
  -d '{"request": {"message": "Hello", "count": 5}}' | jq -r '.workflow_id')

# 3. Check status
curl http://localhost:25780/v1/plans/demo-dev/executions/$WORKFLOW_ID

# 4. List all executions
curl http://localhost:25780/v1/plans/demo-dev/executions
```

### Async Task Workflow

```bash
# 1. Execute plan with async task
RESPONSE=$(curl -X POST http://localhost:25780/v1/plans/batch-import-prod/executions \
  -H "Content-Type: application/json" \
  -d '{"request": {"file": "data.csv"}}')

WORKFLOW_ID=$(echo $RESPONSE | jq -r '.workflow_id')

# 2. Check status to get step ID
STATUS=$(curl http://localhost:25780/v1/plans/batch-import-prod/executions/$WORKFLOW_ID)
STEP_ID=$(echo $STATUS | jq -r '.workflow.tasks["batch import"].step_id')

# 3. When external work completes, resume
curl -X POST http://localhost:25780/v1/plans/batch-import-prod/executions/$WORKFLOW_ID/steps/$STEP_ID/resume \
  -H "Content-Type: application/json" \
  -d '{"status": "completed", "rows_imported": 10000}'

# 4. Verify completion
curl http://localhost:25780/v1/plans/batch-import-prod/executions/$WORKFLOW_ID
```

## Rate Limiting

**Current Implementation**: No rate limiting.

**Future**: Rate limiting will be implemented per API key or IP address.

## CORS Support

Enable CORS when starting the server:

```bash
./bin/tef serve --cors
```

This allows web applications to make requests to the API from different origins.

## Content Negotiation

All endpoints accept and return `application/json`.

**Request Headers:**
```
Content-Type: application/json
```

**Response Headers:**
```
Content-Type: application/json
```

## Pagination

**Current Implementation**: No pagination for list endpoints.

**Future**: List endpoints will support pagination with `page` and `page_size` query parameters.

## WebSocket Support

**Future Feature**: Real-time workflow status updates via WebSockets.

Planned endpoint:
```
ws://localhost:25780/v1/plans/{plan_id}/executions/{execution_id}/ws
```

## Health Check

**Future Endpoint:** `GET /health`

Will return server health status:
```json
{
  "status": "healthy",
  "version": "1.0.0",
  "timestamp": "2025-01-26T12:34:56Z"
}
```

## API Versioning

The API is versioned via the URL path (`/v1/`). Future versions will be introduced as needed (`/v2/`, etc.) while maintaining backward compatibility for existing versions.

## Additional Resources

* [CLI.md](CLI.md) - Command-line interface reference
* [README.md](README.md) - Overview and getting started
* [plans/README.md](plans/README.md) - Plan development guide
* [TODO.md](TODO.md) - Implementation status
