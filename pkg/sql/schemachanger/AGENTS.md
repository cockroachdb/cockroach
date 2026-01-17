## At a glance

- **Component index**
  - Declarative schema changer core: `pkg/sql/schemachanger/`
  - Planner/rules: `pkg/sql/schemachanger/scplan/`
  - Builder (DDL → targets/elements): `pkg/sql/schemachanger/scbuild/`
  - Executor (ops): `pkg/sql/schemachanger/scexec/`
  - Ops/types/phases: `pkg/sql/schemachanger/scop/`, `pkg/sql/schemachanger/scpb/`
  - Job resumer/integration: `pkg/sql/schemachanger/scjob/` (uses `pkg/jobs/`)
  - Runtime driver: `pkg/sql/schemachanger/scrun/`

- **Ownership & boundaries**
  - Owners: SQL (schema changer)
  - Owns: generation of targets, planning into phases/stages, execution of metadata changes, backfills, and validations.
  - Uses: `pkg/jobs/` for background execution, `pkg/kv/` for transactions, `pkg/kv/kvserver/protectedts/` for GC protection.
  - Avoids: direct storage/raft coupling; interacts via descriptors, jobs, and KV transactions.
- **See also**: `pkg/sql/AGENTS.md`, `pkg/kv/kvserver/AGENTS.md`, `pkg/upgrade/AGENTS.md`

- **Key entry points & types**
  - `scbuild/build.go`: Build targets/elements from DDL.
  - `scplan/plan.go`: Make plan, compute stages by phase.
  - `scop/phase.go`: Phases: Statement, PreCommit, PostCommit, PostCommitNonRevertible.
  - `scexec/executor.go`: Execute a stage (Mutation/Backfill/Validation ops).
  - `scexec/backfiller/*.go`: Backfill progress and periodic flushing.
  - `scrun/scrun.go`: Run in-txn phases; run post-commit stages inside the job.
  - `scjob/job.go`: Job resumer for `NEW_SCHEMA_CHANGE` and rollback handling.
  - `scpb/elements.proto(.go)`: Element model, statuses.
  - `scdeps/*.go`: Wiring to jobs, descriptors, stats, protected timestamps.

- **Critical invariants**
  - 2-version rule for online schema changes is preserved (see `docs/RFCS/20151014_online_schema_change.md`).
  - Planning is deterministic from descriptor state + cluster version; execution is idempotent and resumable.
  - Statement/PreCommit phases are transactional with the user txn; PostCommit runs via a job in separate txns.
  - Rollbacks always converge to a clean state; non-revertible post-commit stages ensure monotonic cleanup.

- **Primary flow (phases)**

```
DDL stmt -> scbuild (targets) -> scplan (StatementPhase stages)
            │                                    │
            └─ run in user txn ───────────────────┘
                        ↓
                scplan (PreCommitPhase)  ── create job, persist state
                        ↓
                    COMMIT
                        ↓
                scjob resumer (NEW_SCHEMA_CHANGE)
                        ↓
         scplan (PostCommit[/NonRevertible]) → backfills/validations/cleanup
                        ↓
                remove schema-changer state; GC jobs as needed
```

## Deep dive

1) Architecture & control flow
- Declarative engine
  - DDL is converted to a set of element targets (`scpb`) via `scbuild`.
  - `scplan` uses rule sets to produce an ordered sequence of stages, grouped by phase (`scop.Phase`). Each stage contains homogenous ops: metadata mutation, backfill, or validation.
  - In-txn phases
    - StatementPhase: applies metadata changes in-memory during statement execution; no durable writes yet.
    - PreCommitPhase: persists descriptor/job metadata changes within the user transaction, including creating/updating the schema change job.
  - Post-commit phases (via job)
    - PostCommit: metadata changes that don't need to be in the user txn, plus long-running work (backfills, validations).
    - PostCommitNonRevertible: used at the start of rollbacks and late cleanup where status transitions are not reverted.
- Driver
  - `scrun.RunStatementPhase`/`RunPreCommitPhase` execute in the user transaction; `RunSchemaChangesInJob` reconstructs state from descriptors and executes each post-commit stage in its own KV transaction.

2) Data model & protos
- Element model (`scpb`) encodes element kinds (columns, indexes, constraints, names, locks, etc.) and their statuses (e.g., ABSENT, PUBLIC, TRANSIENT_ABSENT, VALIDATED).
- Declarative state is persisted inside descriptors (`declarative_schema_changer_state`) and includes the job id, auth, and target state; jobs track only progress for backfills/merges.

3) Concurrency & resource controls
- KV transactions per stage ensure atomicity at stage boundaries; retries are confined to a single stage.
- Backfills/merges use range-aware progress tracking and periodic fraction/ checkpoint flushes to the job (`scexec.BackfillerTracker`).
- Large backfills rely on span splitting and stats refresh hooks to reduce contention.

4) Failure modes & recovery
- Any error during post-commit causes the job to transition to Reverting; `scrun` plans a rollback with `PostCommitNonRevertible` as the initial phase and executes cleanup deterministically.
- User errors (constraint violations, etc.) are surfaced; space errors pause the job (see `scexec/exec_backfill.go`).
- Missing descriptors or assertion failures are marked as permanent job errors and will not be retried.
- Protected timestamps are unprotected as a last resort in `scjob.OnFailOrCancel` to prevent leaked protection.

5) Observability & triage
- Jobs
  - Inspect with `SHOW JOB <id>` / UI. Look for `status`, `error`, `running_status`, and `reverting`.
  - The resumer writes compact explain output to job info (`explain-phase.*.txt`); retrieve via `system.job_info` or the UI.
- Backfills
  - Progress and checkpoints are stored on the job; fraction completed is range-based.
  - Paused due to ENOSPC: free space or adjust zone/configuration, then `RESUME JOB <id>`.
- Protected timestamps
  - List protections: `SELECT * FROM crdb_internal.kv_protected_ts_records WHERE internal_meta->>'job_id' = '<job id>';` (targets decoded in `decoded_target`).
  - Ensure protections advance or are cleared on success/failure; stale protections block GC and delay drop GC jobs.
- Quick triage playbook
  1. Identify the schema change job and whether it's Reverting.
  2. Pull the latest `explain-phase.*.txt` from job info to see the current/next stage.
  3. Check protected timestamps for the job; confirm they match the backfill/validation timestamp.
  4. If paused for space or throttling, resolve the resource issue and resume.
  5. If retrying frequently, look for descriptor lease contention or mixed-version gates; verify cluster version.

6) Interoperability
- Jobs: integrates with the job registry for post-commit execution and progress/checkpoint reporting.
- KV: executes each stage within KV transactions with standard retry/contend semantics.
- Protected timestamps: coordinates protections to prevent GC of data required during long-running changes.

7) Mixed-version / upgrades
- Planning/rollback respect the active cluster version; `scrun.makeState` migrates persisted state (`scpb.MigrateDescriptorState`) to the active version before planning.
- Rules can be version-gated; nodes on older versions won’t schedule newer transitions.
- During rolling upgrades, state reconstruction + deterministic planning ensures resumability across versions.

8) Configuration & feature gates
- Settings
  - `sql.schemachanger.strict_planning_sanity_check.enabled` (testing/dev) enforces planner sanity checks.
  - Job pausepoints via `jobs.debug.pausepoints` can pause at post-commit stage boundaries for debugging.

9) Background jobs & schedules
- Primary job: `NEW_SCHEMA_CHANGE` executes post-commit stages. Creates GC jobs for dropped data when needed.
- Stats refresh and TTL schedule updates are triggered via execution hooks when appropriate.

10) Edge cases & gotchas
- Multiple DDLs in one txn: atomic planning across statements; rollback restores pre-txn visibility.
- Non-revertible phases appear early in rollback to prevent oscillation and ensure eventual cleanup.
- Concurrent DDL: builders and gates reject conflicting declarative changes; legacy and declarative cannot mutate the same descriptor concurrently.

11) Examples
- Add index (simplified)
  - Statement/PreCommit: add metadata (write-only), create job.
  - PostCommit: backfill index, validate, swap visibility, drop temporary artifacts, schedule GC for old index if necessary.
- Drop table
  - PostCommit: mark dropped, create GC job; protected timestamps ensure retention until TTL; GC job clears data after protections expire.

12) References
- New schema changer RFC: `docs/RFCS/20210115_new_schema_changer.md`
- Online schema change invariant: `docs/RFCS/20151014_online_schema_change.md`
- See also: `pkg/sql/AGENTS.md`

13) Glossary
- **Element/Target**: A schema object fragment (e.g., index, column name) with a desired end status.
- **Phase**: Execution context: Statement, PreCommit, PostCommit, PostCommitNonRevertible.
- **Stage**: Ordered group of same-typed ops executed atomically in a txn.
- **Op types**: Mutation (descriptor/namespace/zone changes), Backfill, Validation.
- **Backfill**: Historical scan + writes to build new structures; range-based progress, resumable.
- **Protected timestamp (PTS)**: GC-protection for required historical reads or post-drop data.
- **Rollback**: Planned reversal to stable state; starts in non-revertible phase.
- **Resumer**: Job process executing post-commit stages with per-stage txns.

14) Search keys & synonyms
- "declarative schema changer", "new schema changer", phases, post-commit, backfill checkpoint, non-revertible, protected timestamps, NEW_SCHEMA_CHANGE job, scpb elements, scplan stages.
