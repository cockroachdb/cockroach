# TEF Implementation TODO

This document tracks pending implementation work for the Task Execution Framework.

## Status Overview

### ✅ Implemented

- Core interfaces and type definitions
- BasePlanner with full validation logic
- All seven task types with validation
- Plan registry infrastructure
- Utility functions for plan ID management
- Status and execution tracking types
- Logger interface and implementation
- **Manager registry for cross-plan task execution**
- **Temporal planner skeleton (interface layer and client integration)**

### 🚧 Partially Implemented

- Temporal integration (skeleton exists, workflow execution pending)

### ❌ Not Yet Implemented

The following components need implementation to make TEF fully functional:

## 1. Orchestration Engine Integration

**Priority: HIGH** - Required for any workflow execution

### Temporal Integration (In Progress)

The Temporal planner skeleton is implemented in `pkg/cmd/tef/planners/temporal_planner/`:

**✅ Completed:**
- `manager.go`: PlannerManager interface implementation with:
  - Temporal client connection with DNS resolver configuration
  - Prometheus metrics integration and HTTP server
  - Status querying via `GetExecutionStatus()`
  - Execution listing via `ListExecutions()`
  - Plan discovery via `ListAllPlanIDs()`
  - Task resumption via `ResumeTask()`
  - Configuration flags for Temporal server and metrics
- `status.go`: Execution status queries and workflow info parsing

**❌ TODO (deferred to follow-up PR):**
- `StartWorker()`: Temporal worker initialization and workflow registration
- `ExecutePlan()`: Workflow execution initiation
- Workflow definition that translates task graph to Temporal workflows
- Activity implementations for task execution

**Alternative Options**: Airflow, Cadence, custom scheduler, or any orchestration engine

## 2. CLI Command Implementation

Complete the CLI command generation in `pkg/cmd/tef/cli/commands.go`.

**Priority: HIGH** - Required for user interaction

### Implementation

Implement `initializeWorkerCLI()` function:

```go
func initializeWorkerCLI(ctx context.Context, rootCmd *cobra.Command, registries []planners.Registry) {
    // For each plan registry:
    //   - Create start-worker <planname> command
    //   - Create execute <planname> command
    //   - Create gen-view <planname> command
    //   - Add commands to rootCmd
}
```

### Commands to Generate

For each registered plan, auto-generate:

1. `start-worker <planname>`: Start a worker for the plan
2. `execute <planname>`: Execute the plan with JSON input
3. `gen-view <planname>`: Generate workflow visualization
4. `resume <planname>`: Resume callback tasks (if applicable)

## 3. Plan Implementations

Create plan packages under `pkg/cmd/tef/plans/`.

**Priority: MEDIUM** - Required for testing and validation

### Steps

1. Create plan packages (e.g., `demo/`, `pua/`, etc.)
2. Implement the `Registry` interface for each plan:
   - `GetPlanName()`: Return unique plan identifier
   - `GetPlanDescription()`: Return plan description
   - `GetWorkflowVersion()`: Return workflow version
   - `GeneratePlan()`: Build task execution graph
   - `ParsePlanInput()`: Parse and validate JSON input
   - `PrepareExecution()`: Set up plan resources
   - `AddStartWorkerCmdFlags()`: Add plan-specific CLI flags

3. Register plans in `pkg/cmd/tef/plans/registry.go`:
   ```go
   func RegisterPlans(pr *planners.PlanRegistry) {
       demo.RegisterDemoPlans(pr)
       pua.RegisterPUAPlans(pr)
       myplan.RegisterMyPlanPlans(pr)
       // ... more plans
   }
   ```

### Suggested Plans

- **demo**: Simple plan showcasing all task types
- **pua**: Performance Under Adversity testing
- **roachprod**: Cluster provisioning
- **gc**: Garbage collection workflows

## 4. REST API Implementation

Create REST API server and handlers.

**Priority: LOW** - Optional, CLI is primary interface

### Structure

Create `pkg/cmd/tef/api/`:

- `server.go`: HTTP server implementation
- `handlers/v1/`: API handlers for plans, executions, status

### Endpoints to Implement

```
POST /v1/plans/{plan_id}/executions       - Execute a plan
GET  /v1/plans/{plan_id}/executions       - List executions
GET  /v1/plans/{plan_id}/executions/{id}  - Get execution status
POST /v1/plans/{plan_id}/executions/{id}/steps/{step_id}/resume - Resume callback task
GET  /v1/plans                             - List all plans
GET  /                                     - UI (optional)
```

### Implementation Notes

- Use the PlannerManager interface for all operations
- Support both plan execution and status queries
- Enable CORS for web UI access

### Request Tracing and Observability

Implement comprehensive request tracing for debugging and observability:

**X-Request-ID Header Support:**
- Honor client-provided `X-Request-ID` headers and propagate through entire request lifecycle
- Generate unique request ID (UUID v4) when not provided by client
- Include request ID in all log messages for correlation
- Return `X-Request-ID` in all response headers (both success and error responses)

**Distributed Tracing:**
- Support standard tracing formats:
  - W3C Trace Context (`traceparent`, `tracestate` headers)
  - OpenTelemetry context propagation
- Honor trace context from upstream components (load balancers, API gateways, reverse proxies)
- Propagate trace context to downstream services:
  - Temporal workflow executions
  - Database queries
  - Any other external service calls
- Consider using OpenTelemetry SDK for automatic instrumentation

**Logging and Correlation:**
- Include request ID in all structured log messages
- Log request start/end with timing information
- Log request method, path, status code, response time
- Use structured logging format (JSON) for easy parsing

**Error Responses:**
- Always include `X-Request-ID` in error responses
- Consider including trace information in error details for debugging

This enables end-to-end request tracking across distributed components and simplifies debugging of production issues.

## 5. Worker Runtime

Implement worker process that runs orchestration engine and executes workflows.

**Priority: HIGH** - Required for execution (part of orchestration engine integration)

### Components

1. Worker initialization and startup
2. Plan loading and validation
3. Executor registration with orchestration engine
4. Graceful shutdown handling
5. Health check endpoints

## 6. Testing Infrastructure

Add comprehensive testing for all components.

**Priority: MEDIUM** - Required for reliability

### Test Coverage Needed

- End-to-end workflow execution tests
- Plan validation tests
- CLI command tests
- API endpoint tests
- Error handling and failure scenarios
- Concurrent execution tests
- Upgrade compatibility tests

## 7. Documentation

Complete and refine documentation.

**Priority: MEDIUM** - In progress

### Tasks

- [x] Restructure README.md with linear flow
- [x] Create TODO.md (this file)
- [ ] Create CLI.md with full CLI reference
- [ ] Create API.md with REST API documentation
- [ ] Update architecture.md to remove duplication
- [ ] Update plans/README.md to remove duplication
- [ ] Add deployment and operations guide
- [ ] Add troubleshooting guide
- [ ] Add migration guide for adding new task types

## 8. Build and Deployment

Set up build and deployment infrastructure.

**Priority: LOW** - Can be done after implementation

### Tasks

- Bazel build configuration
- Docker container support
- Deployment scripts
- Configuration management
- Monitoring and observability setup

## Implementation Order (Recommended)

1. **Temporal Integration** (Step 1) - Enables execution
2. **CLI Commands** (Step 2) - Enables user interaction
3. **Demo Plan** (Step 3) - Provides working example
4. **Testing** (Step 6) - Validates implementation
5. **Additional Plans** (Step 3 cont.) - Expands functionality
6. **Documentation** (Step 7) - Completes user experience
7. **REST API** (Step 4) - Optional enhancement
8. **Build/Deployment** (Step 8) - Productionization

## Quick Start After Implementation

Once components are implemented:

```bash
# Build TEF
./dev build tef

# Start orchestration engine (e.g., Temporal)
temporal server start-dev

# Start a worker
./bin/tef start-worker demo --plan-variant dev

# Execute a plan
./bin/tef execute demo '{"message": "Hello", "count": 5}' dev

# Check status
./bin/tef status demo <workflow-id>
```

## Contributing

When implementing any of these components:

1. Follow CockroachDB coding guidelines (see `/CLAUDE.md`)
2. Add comprehensive tests
3. Update relevant documentation
4. Ensure backward compatibility
5. Use proper error handling with `cockroachdb/errors`
6. Follow redactability guidelines for logging

## Questions or Issues

For questions about implementation:
- Review architecture.md for design details
- Check plans/README.md for plan development patterns
- Refer to existing code in `pkg/cmd/tef/planners/`
- Consult with the TEF maintainers
