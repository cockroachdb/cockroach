## At a glance
- **Component index**
  - pgwire (wire protocol) → `pkg/sql/pgwire/` → parses protocol, fills `StmtBuf`, streams results.
  - connection executor → `pkg/sql/conn_executor*.go` → session state machine; dispatches to planning/execution.
  - optimizer & planning surface → `pkg/sql/opt/` (+ `pkg/sql/plan_opt.go`) → builds memo, shapes physical plan, exec factory.
  - DistSQL planner & flows → `pkg/sql/distsql_*.go`, `pkg/sql/execinfra/`, `pkg/sql/flowinfra/` → flow specs, local/remote setup.
  - execution engines → row (processors in `pkg/sql/rowexec/`) and vectorized (operators in `pkg/sql/colflow/`, `pkg/sql/colexec/`).
  - session state → `pkg/sql/sessiondata/`, `pkg/sql/sessiondatapb/` → settings propagated to flows.
  - privileges → checks via `pkg/sql/authorization.go` + catalog bridge (`opt_catalog.go`).
  - jobs integration → `pkg/jobs/` registry + resumers; created from SQL transactions.
  - SQL→KV boundary → kv fetchers (`pkg/sql/row/`, `pkg/sql/colfetcher/`) using `kv.Txn`/`kvcoord.DistSender`.

- **Owners**: SQL Foundation

- **Ownership & boundaries (allowed/forbidden)**
  - Allowed: `pgwire` → `StmtBuf` only; never touches KV directly.
  - Allowed: `connExecutor` owns session lifecycle, statement state machine, and calls into planning/execution.
  - Allowed: planning uses catalog/authorization via `planner`/`optCatalog`; KV access only through execution.
  - Allowed: execution uses `execinfra.Flow` APIs; KV reads/writes happen only inside processors/operators via fetchers under a `kv.Txn` from the executor.
  - Forbidden: bypassing privilege checks; issuing KV ops from `pgwire`/optimizer layers; leaking flows/monitors.
- **See also**: `pkg/kv/kvclient/kvcoord/AGENTS.md`, `pkg/kv/kvserver/AGENTS.md`, `pkg/sql/schemachanger/AGENTS.md`

- **Key entry points & types (anchors)**
  - `pkg/sql/pgwire/conn.go` → `conn.serve` feeds a `sql.StmtBuf` to executor; implements `sql.ClientComm`.
  - `pkg/sql/conn_executor.go` → `connExecutor` lifecycle and `run` loop.
  - `pkg/sql/conn_executor_exec.go` → `dispatchToExecutionEngine`, `PlanAndRunAll` wiring.
  - `pkg/sql/plan_opt.go` → `optPlanningCtx.runExecBuilder` bridges optimizer → executable plan components.
  - `pkg/sql/distsql_running.go` → `DistSQLPlanner.Run` and `setupFlows` to create local/remote flows.
  - `pkg/sql/distsql/server.go` → `ServerImpl.SetupFlow` RPC entry for remote flows.
  - `pkg/sql/flowinfra/flow.go` → `Flow` lifecycle (start, drain, cleanup) and memory.
  - `pkg/sql/rowexec/tablereader.go` / `pkg/sql/rowexec/joinreader.go` → row processors.
  - `pkg/sql/colflow/vectorized_flow.go` / `pkg/sql/colexec/` → vectorized operators & orchestration.
  - `pkg/sql/colfetcher/cfetcher.go` / `pkg/sql/row/kv_fetcher.go` → SQL→KV fetchers, `kv.Txn` boundary.
  - `pkg/sql/authorization.go` / `pkg/sql/opt_catalog.go` → privilege checks through planner/catalog.

- **Critical invariants**
  - Every statement executes under a well-defined `kv.Txn` (root or leaf); no KV ops outside a transaction.
  - Privileges are checked before planning/execution of descriptor-backed objects; errors are not downgraded.
  - Session settings (`sessiondata.SessionData`) are snapshotted and propagated to all flows; remote nodes must see consistent settings.
  - DistSQL flows always cleaned up (local and remote); memory monitors stop; tracing spans finished.
  - Row and vectorized engines are semantically equivalent; choice controlled by vectorize mode and operator support.
  - Flow version compatibility is enforced (`execversion`); mixed-version clusters negotiate common range.
  - Memory is hierarchically accounted (session → txn → flow → processor/operator); no unbounded allocations.
  - Plan evaluation never mutates descriptors outside declared jobs/DDL paths; side-effects use jobs or transactional writes.

- **Primary flow (read path)**
```
client → pgwire/conn → StmtBuf → connExecutor.run/execCmd
    → parse + opt + exec builder (opt/cat, plan_opt)
    → DistSQLPlanner.PlanAndRunAll → PhysicalPlan → FlowSpec(s)
    → setup local flow + SetupFlow RPCs
    → execinfra.Flow (rowexec/colflow)
    → {TableReader/Join/…} → KVFetcher/cFetcher → kv.Txn → kvcoord.DistSender
    ← rows/batches → DistSQLReceiver → pgwire write → client
```

## Deep dive
1) Architecture & control flow
- Wire protocol: `pgwire.Server.newConn` and `conn.serve` parse pg messages into `StmtBuf`; results streamed via `ClientComm`.
- Executor loop: `connExecutor.run` reads, drives txn state machine, and calls `execCmd`/`dispatchToExecutionEngine`.
- Optimization/planning: `optPlanningCtx.runExecBuilder` builds executable components from the optimizer memo via `execbuilder`.
- DistSQL planning: `DistSQLPlanner.PlanAndRunAll` generates `PhysicalPlan` and per-node `execinfrapb.FlowSpec`.
- Flow setup: gateway sets up local flow and issues `SetupFlow` RPCs (remote). Version negotiated via `execversion`.
- Execution: `execinfra.Flow` runs processors/operators. Row engine lives in `rowexec/*`; vectorized in `colflow`/`colexec/*`.
- Results: processors push to `DistSQLReceiver`, which writes to `ClientComm` (pgwire); stats/traces collected per settings.

2) Data model & protos
- `execinfrapb.FlowSpec`, `ProcessorSpec`, `PostProcessSpec` describe physical flows and processors.
- `sessiondatapb.SessionData` and `LocalOnlySessionData` carry settings to remote nodes.
- Catalog descriptors resolved via planner/`optCatalog`; privilege objects from `catalog` types.

3) Concurrency & resource controls
- Memory monitors: `mon.BytesMonitor` at session/txn/flow levels; operator/processor accounts use bounded accounts.
- Backpressure: receivers can block senders; `DistSQLReceiver` can switch to draining on error.
- Admission: Setup may reserve memory; fetchers obey batch sizing; vectorized batch sizes bounded.
- Contexts: per-connection, per-flow, per-processor contexts cancel promptly on errors/drain.

4) Failure modes & recovery
- Txn retries: executor restarts statements on retriable errors with correct timestamps and statements re-planned.
- Flow setup failures: ensure flow cleanup on error (local and remote) before returning.
- Partial results: on error, receiver drains remaining inputs; client gets a clear SQL error.
- Vectorized panics: translated into errors; optional test knobs inject panics for robustness.

5) Observability & triage
- Explain: `EXPLAIN`, `EXPLAIN ANALYZE (DEBUG)` to inspect optimizer plan, flow, RU/bytes.
- Tracing: session tracing; DistSQL spans across processors/flows; statement diagnostics recorder.
- Metrics: per-processor/flow stats, SQL memory usage, contention events; check node and SQL dashboards.
- Debug recipe:
  1. `SHOW TRACE FOR SESSION` or statement diagnostics for the query.
  2. `EXPLAIN ANALYZE (DEBUG)`; verify vectorized vs row; check distribution and scan counts.
  3. Look for flow setup errors in logs/traces; confirm `SetupFlow` RPCs return.
  4. Validate memory accounts didn’t OOM; check session/flow monitors.
  5. For KV stalls, inspect contention and `kv` traces (look for `DistSender`/`TxnCoordSender`).

6) Interoperability
- Optimizer → execution: optimizer feeds executable plan components to the exec builder surface.
- Flow/execution: distributed flow and processor lifecycles coordinate setup, execution, and cleanup across nodes.
- Vectorized vs row engines: interchangeable semantics; selection controlled by settings and operator support.
- pgwire: network protocol and buffering speak only to the executor via `StmtBuf`/`ClientComm` abstractions.
- Jobs: job registry runs resumers for long-running or side-effecting work created within SQL transactions.
- KV boundary: all KV interaction happens inside processors/operators via fetchers under a `kv.Txn`.

7) Mixed-version / upgrades
- Flow versioning: `distsql/server.go` rejects unsupported `execversion`; flows run at negotiated version.
- Optimizer/version gates: settings and `clusterversion` guard rules and features; avoid emitting unsupported processors.

8) Configuration & feature gates
- Session settings affect distribution and vectorization (e.g., vectorize mode), lock timeouts, tracing.
- Cluster/settings toggle DistSQL defaults, vectorized defaults, telemetry, plan gists.

9) Background jobs & schedules
- Long-running or side-effecting work (schema changes, IMPORT/EXPORT, stats) use `jobs.Registry` APIs; jobs created inside SQL txns and may be started post-commit via `CreateStartableJobWithTxn`.

10) Edge cases & gotchas
- Always close flows on all paths; forgetting leaks memory/monitors and spans.
- SessionData must be consistent across gateway and leaves; propagate before setup.
- Plan-node fallbacks exist; ensure semantics parity with vectorized execution.
- Memory aliasing across flow specs is guarded (see `setupFlows` comment/assertions); don’t reuse mutable slices across local/remote specs.

11) Examples (tiny)
- Simple read: `SELECT a FROM t` → optimizer emits scan → DistSQLPlanner builds single-node FlowSpec → TableReader → KVFetcher → rows.
- Distributed join: `SELECT * FROM t JOIN u ON ...` → multiple TableReaders → Join (hash/merge) in rowexec or vectorized; remote flows per node; receiver merges.

12) References
- `docs/tech-notes/life_of_a_query.md` (high-level narrative)

13) Glossary
- connExecutor: per-connection state machine executing statements in a session.
- Flow/Processor: distributed execution units described by FlowSpec/ProcessorSpec.
- DistSQL: distributed SQL execution framework.
- Vectorized engine: columnar operators (`colflow`/`colexec`).
- Row engine: row-based processors (`rowexec`).
- Receiver: `DistSQLReceiver`, consumes processor outputs into pgwire.
- FlowCtx: execution context carrying txn, monitors, settings.
- Leaf txn: child txn state for remote processors derived from root.
- FlowSpec: protobuf describing processors/streams on a node.

14) Search keys & synonyms
- connExecutor.run, execCmd, StmtBuf, ClientComm, pgwire
- DistSQLPlanner.PlanAndRunAll, setupFlows, Flow.Cleanup, DistSQLReceiver
- execinfrapb.FlowSpec, ProcessorSpec, PostProcessSpec
- flowinfra.Flow, execinfra, ServerImpl.SetupFlow
- planNode, rowexec, tablereader, joinreader
- colflow, colexec, cfetcher, KVFetcher, colbatch
- optbuilder, execbuilder, memo, plan_top, planComponents, plan gist
- sessiondata.SessionData, sessiondatapb, settings, vectorize
- kv.Txn, kvcoord.DistSender, TxnCoordSender, BatchRequest
