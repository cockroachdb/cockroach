## Upgrade subsystem — version gates, migrations, mixed-version semantics

## At a glance

- **Component index**:
  - Responsibilities: orchestrate cluster upgrades; execute idempotent migrations; enforce version-gate invariants; coordinate KV vs tenant upgrade sequencing.
  - Owners: KV/Server + SQL teams (shared surface).
  - Repo anchors: `pkg/upgrade/` (manager, jobs, registry), `pkg/clusterversion/` (version gates & settings).
  - Primary entrypoints: `upgrademanager.Manager.Migrate` (`pkg/upgrade/upgrademanager/manager.go`), `server.auto_upgrade` (`pkg/server/auto_upgrade.go`), migration registry in `pkg/upgrade/upgrades/`.

- **Ownership & boundaries**:
  - Allowed: upgrade manager → server migration RPCs; jobs; SQL internal executor; tenant instance discovery.
  - Forbidden: feature logic must not assume peers have already flipped gates; inbound request handling must remain backward compatible until the gate activates cluster-wide.
  - See also: `pkg/settings/AGENTS.md`, `pkg/multitenant/AGENTS.md`

- **Key entry points & types**:
  - `pkg/upgrade/upgrademanager/manager.go`: `Manager.Migrate`, `runMigration`, interlock/fencing.
  - `pkg/server/migration.go`: `ValidateTargetClusterVersion`, `BumpClusterVersion`, engine persistence.
  - `pkg/upgrade/upgradejob/upgrade_job.go`: executes upgrades inside jobs, marks completion in `system.migrations`.
  - `pkg/upgrade/upgrades/upgrades.go`: migration registry keyed by `roachpb.Version`.
  - `pkg/upgrade/upgrade.go`, `tenant_upgrade.go`: `SystemUpgrade` vs `TenantUpgrade`, `TenantDeps`.
  - `pkg/clusterversion/clusterversion.go`, `cockroach_versions.go`: version keys, `Handle`, `IsActive` semantics.
  - `pkg/clusterversion/setting.go`: `version` setting; `cluster.auto_upgrade.enabled`; `cluster.preserve_downgrade_option`.

- **Critical invariants**:
  - Cluster version increases monotonically; never decreases.
  - At any time nodes see either version X or X+1; no gaps >1.
  - Migrations are idempotent; a single job exists per migration cluster-wide; completion recorded in `system.migrations`.
  - Outbound behavior may switch at `IsActive(v)`; inbound paths must remain compatible until all nodes adopt v.
  - Permanent upgrades won’t re-run across binaries with different `LatestVersion` once succeeded.

- **Flow (single step X→X+1)**:
```
Operator/auto → Manager.Migrate → ValidateTargetClusterVersion(all nodes)
                  │                      │
                  │                      └─ reject if target < MinSupported or > binary
                  │
                  ├─ create/ensure job for X+1
                  │    └─ run migration (idempotent) → mark system.migrations → optional schema-version bump
                  │
                  └─ BumpClusterVersion(all nodes) → persist to stores → set active setting → update settings table
```

- Operator finalize quickstart
  - Ensure all nodes run the same binary version (`ServerVersion`).
  - Clear `cluster.preserve_downgrade_option` if set to current cluster version.
  - Finalize: `SET CLUSTER SETTING version = crdb_internal.node_executable_version();`.

Deep dive

1) Architecture & control flow

- The upgrade path is driven by `Manager.Migrate` which steps through the list of internal versions between `from` and `to`, running at most one migration per step before bumping the gate.
- For each step, the manager interlocks with KV via `server.migration` RPCs: pre-check (`ValidateTargetClusterVersion`), run migration (job or inline under testing knobs), post-checks, and finally `BumpClusterVersion` to durably persist and activate the new gate on every node.
- Migrations run under the jobs framework (`upgrade_job.go`). On success, `system.migrations` is updated and, for non-permanent migrations, the system database schema version is bumped.

2) Data model & types

- Versioning:
  - Binary carries `LatestVersion` and `MinSupportedVersion` (previous major).
  - Cluster maintains an active `version` (a special cluster setting backed by per-store persisted state) that gates behavior.
  - Version keys live in `pkg/clusterversion/` and are used as feature gates in code: `if IsActive(FeatureX) { new path } else { old path }`.
- Upgrades:
  - `SystemUpgrade`: runs in the system tenant; for below-Raft or KV-layer changes.
  - `TenantUpgrade`: runs for each tenant (including system tenant) for SQL-layer/state changes.
  - Registry (`pkg/upgrade/upgrades/upgrades.go`) maps `roachpb.Version` → upgrade implementation; first entry per release is a sentinel `...Start` version.

3) Concurrency & resource controls

- Only one job per migration globally; nodes coordinate to avoid duplicates.
- Manager exposes pause points (testing knobs) around fence writes, migrations, and version bumps to validate interlocks.
- Jobs benefit from standard backoff/retry; migrations must be idempotent by contract.

4) Failure modes & recovery

- Node crashes/restarts: safe due to idempotent migrations and persisted `system.migrations` checkpoints.
- Mixed binaries: `ValidateTargetClusterVersion` prevents advancing if any node’s `LatestVersion` is below the target or if the target is below any node’s `MinSupportedVersion`.
- Settings races: upgrade uses a fence and validates again before the final `BumpClusterVersion` to ensure new SQL servers didn’t appear mid-flight with incompatible binaries.

5) Observability & triage

- Inspect progress: `SHOW CLUSTER SETTING version;`, `SHOW CLUSTER SETTING cluster.preserve_downgrade_option;`.
- Migration history: `SELECT * FROM system.migrations ORDER BY completed_at DESC;`.
- Job state: `SHOW JOBS` for upgrade jobs; node logs around `migration-mgr`, `validate-cluster-version`, `bump-cluster-version`.
- Metrics: `cluster.preserve-downgrade-option.last-updated` gauge.

6) Interoperability seams

- Version gates: components coordinate activation via version-gate checks and cluster-wide persistence.
- Server RPCs: nodes validate targets and durably bump the cluster version in a coordinated sequence.
- Jobs & SQL: upgrades run under the jobs framework using the internal executor; completion is recorded cluster-wide.
- Tenants: per-tenant bumps are coordinated for virtual clusters while respecting storage cluster constraints.

7) Mixed-version / upgrades — pitfalls

- Never gate correctness of inbound handling on `IsActive(v)`; nodes might still receive “old” or “new” requests during propagation. Only outbound behavior may change immediately upon activation.
- Two-step features: often introduce a preparatory migration at version N (schema/index/bootstrap change) and enable the SQL feature at N+1. Ensure that command handlers/plan hooks enforce “bump required” until N+1 is active.
- Below-Raft storage changes require KV migration loops (range iteration) and careful fencing; add version switches at the KV layer and corresponding `SystemUpgrade`.
- Permanent vs non-permanent: permanent upgrades won’t rerun across binaries; rely on this when bootstrap schemas evolve.

8) Configuration & feature gates

- Manual finalize: `SET CLUSTER SETTING version = crdb_internal.node_executable_version();` (issued once cluster binaries are homogenous).
- Auto-upgrade: controlled by `cluster.auto_upgrade.enabled` (default true) and `cluster.preserve_downgrade_option` (blocks both auto and manual finalize when set to the current cluster version).
- Downgrade/rollback window: keep `cluster.preserve_downgrade_option` set to freeze the gate while rolling binaries. Once the gate is bumped, downgrade is prohibited by design.

9) Tenants and upgrade sequencing

- The system tenant’s cluster version is the KV cluster version. Secondary tenants maintain their own active cluster versions (stored under their keyspace) and upgrade at their own pace, within constraints.
- Connection rules loosened for tenants: SQL pods may connect to KV as long as the tenant’s active cluster version is ≥ KV `MinSupportedVersion`. KV never dials tenants, avoiding reverse constraints.
- Operator requirement: before KV advances again (next release), all tenants must have advanced to at least the prior release’s gate. `pkg/upgrade/upgradecluster/tenant_cluster.go` documents the model.

10) Operator playbook

- Safe rolling upgrade with rollback window:
  1) Set `cluster.preserve_downgrade_option = <current_version>` to block finalize.
  2) Roll binaries to the target release (ensure all nodes report the same `ServerVersion`).
  3) Verify health; if issues arise, roll back binaries (gate unchanged). Otherwise clear the setting.
  4) Finalize: either let auto-upgrade advance or run the manual `SET CLUSTER SETTING version = crdb_internal.node_executable_version();`.
- Mixed-version triage:
  - If finalize is blocked: check for mixed binaries, liveness, and storage version constraints; inspect logs from `upgradeStatus` and validation RPCs.
  - If migration repeatedly restarts: audit idempotence and ensure proper checkpointing in `system.migrations` and any side effects.

11) Examples

- Adding a new gated feature:
  - Define `clusterversion.VX_Y_Feature` and guard new code paths with `IsActive`.
  - Add a `TenantUpgrade` or `SystemUpgrade` in `pkg/upgrade/upgrades/` keyed to that version for any schema/storage prep.
  - Write mixed-version tests (roachtests under `pkg/cmd/roachtest/roachtestutil/mixedversion/`).

12) References

- Background: `docs/tech-notes/version_upgrades.md`, `docs/RFCS/20170815_version_migration.md`.

13) Glossary

- Cluster version: active version gate controlling feature exposure and compatibility.
- Binary version: `LatestVersion` embedded in a node’s binary; upper bound for cluster version.
- MinSupportedVersion: lowest cluster version a binary will run with.
- Upgrade/migration: idempotent program executed once per version step, recorded in `system.migrations`.
- SystemUpgrade vs TenantUpgrade: KV-layer vs SQL-layer scope; the latter runs per tenant.
- Fence/interlock: validation and sequencing ensuring no incompatible nodes appear mid-upgrade.
- Permanent upgrade: migration that won’t re-run across binaries once succeeded.

14) Search keys & synonyms

- “upgrade manager”, “migrations”, “version gates”, “finalize upgrade”, “preserve_downgrade_option”, “auto upgrade”, “system.migrations”, “IsActive”, “MinSupportedVersion”, “LatestVersion”, “Tenant upgrade”.
