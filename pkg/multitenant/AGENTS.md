## At a glance
- **Component index**
  - `pkg/multitenant/` — Tenant metadata, capabilities, cost-control interfaces; helper packages `mtinfo/`, `mtinfopb/`, `tenantcapabilities*`, `tenantcostmodel/`.
  - `pkg/kv/kvclient/kvtenant/` — Tenant-side KV connector and restricted APIs to the KV (storage) cluster.
  - `pkg/server/tenant*.go` — Bootstrap of a tenant SQL server, cost control wiring, migration/upgrade RPCs.
  - Cross-link: `pkg/security/` — RPC authz, TLS, and capability authorizers at the RPC boundary.
- **Owners**: Server (multitenant) and SQL
- **Key entry points & types**
  - `pkg/kv/kvclient/kvtenant/connector.go` — `Connector`, `TokenBucketProvider`, `NodeDescStore`, `RangeDescriptorDB`, span config accessor/reporting, settings overrides.
  - `pkg/multitenant/tenantcapabilitiespb/capabilities.proto` — Capability IDs and proto; `pkg/multitenant/tenantcapabilities/*.go` typed helpers and authorizer.
  - `pkg/multitenant/cost_controller.go` — Tenant-side KV interceptor and external IO recorder interfaces.
  - `pkg/server/tenant.go` — `NewSeparateProcessTenantServer`, `newSharedProcessTenantServer`, wiring of DistSender/connector/cost controller.
  - `pkg/server/tenant_migration.go` — Tenant `MigrationServer` (validate target, bump version) and invariants.
  - `pkg/server/tenant_auto_upgrade.go` — Auto-upgrade loop and rules.
  - `pkg/server/settingswatcher/settings_watcher.go` — Settings/cluster-version propagation to tenants, storage cluster version cache.
  - `pkg/rpc/auth_tenant.go` — Tenant RPC authn/z, keyspace bounds checks, capability checks.
- **See also**: `pkg/kv/kvserver/AGENTS.md`, `pkg/kv/kvclient/kvcoord/AGENTS.md`, `pkg/settings/AGENTS.md`, `pkg/sql/AGENTS.md`, `pkg/upgrade/AGENTS.md`
- **Critical invariants**
  - Keyspace isolation: all tenant requests must be contained within that tenant’s keyspace; enforced at RPC authz (`Range`, `RangeLookup`, `Batch`, span stats) and by connector-mediated access paths.
  - Privileged ops gated by capabilities; absence denies by default.
  - SQL-only tenant cannot join gossip nor read range metadata directly; all such data flows via restricted Internal APIs and are filtered.
  - Cost control enforced via token-bucket RU model; no billing enforcement for system tenant.
  - Mixed-version: tenant cluster version never exceeds storage cluster active version; upgrade requires all tenant SQL instances’ binary versions to support target.
- **Request/data flow (read path)**
```
SQL client → tenant SQL → DistSender (KV client)
  ↳ Node/Range metadata via kvtenant.Connector
  ↳ KV RPC over (D)gRPC → KV Internal service
      ↳ rpc/auth_tenant: capability + key-bounds checks
      ↳ KV executes, returns → DistSender → SQL
```
- **Upgrade control flow**
```
Storage cluster bumps version → propagated via settings override
Tenant settings watcher caches storage version
Tenant auto-upgrade loop:
  - if auto-upgrade enabled and preserve_downgrade unset
  - compute min tenant binary across instances
  - target = min(minBinary, storageVersion)
  - SET CLUSTER SETTING version = target (via MigrationServer)
```

## Deep dive

### 1) Architecture & control flow
- **Process models**
  - Separate-process tenants: tenant SQL servers talk to KV over network; `NewSeparateProcessTenantServer` wires `kvtenant.Connector` → `DistSender` with `GRPCTransportFactory` and `NodeDescStore`/`RangeDescriptorDB` from the connector. Cost controller is enabled.
  - Shared-process tenants: co-resident with KV on same process; still use restricted interfaces but with local fast-paths. Cost controller is a noop by design (see comments in `tenant.go`).
- **Connector (`kvtenant.Connector`) responsibilities** (`pkg/kv/kvclient/kvtenant/connector.go`)
  - Bootstraps by dialing one of the provided KV addresses, discovers cluster topology via `GossipSubscription` (cluster ID, `NodeDescriptor`/`StoreDescriptor`).
  - Proxies privileged reads via KV’s Internal service:
    - Range addressing: `RangeLookup`, `GetRangeDescriptors` (iterator factory); responses are constrained to tenant keyspace.
    - Span configs: `spanconfig.KVAccessor` and `spanconfig.Reporter` for tenant-applied configs and conformance.
    - Settings overrides stream for per-tenant and all-tenant overrides.
    - Status/admin/time-series subset surfaces exposed to tenants (subject to capabilities).
  - Provides `TokenBucketProvider` endpoint for RU token-bucket (cost control).
  - Retries and forgets broken connections on “soft” errors; propagates “hard” auth/logic errors.
- **Tenant SQL server wiring** (`pkg/server/tenant.go`)
  - Specializes settings to “virtual cluster” mode: system-only settings guarded; app-level settings mutable by tenant.
  - DistSender built with:
    - `NodeDescs` and `RangeDescriptorDB` = connector
    - Transport = `nodedialer` using connector address resolver
    - KV interceptor = tenant cost controller (separate-process) or noop (shared-process)
  - HTTP/gRPC servers only expose tenant-safe endpoints; `TenantRPCAuthorizer` forbids serving KV RPCs from tenant.
  - Starts settings watcher, metrics recorder, time-series, external storage builder, and cost controller background loops.

### 2) Data model & protos
- `mtinfopb.ProtoInfo` in `system.tenants.info` encodes tenant state, prior replication info, and `tenantcapabilitiespb.TenantCapabilities`.
- `mtinfopb.SQLInfo` mirrors name/data_state/service_mode columns.
- `tenantcapabilitiespb.ID` enumerates capabilities (e.g., `CanAdminSplit`, `CanViewNodeInfo`, `ExemptFromRateLimiting`, `TenantSpanConfigBounds`). Helpers in `tenantcapabilities/*.go` provide typed accessors.
- `mtinfopb.UsageInfo` and `system.tenant_usage` hold token-bucket params and all-time consumption.

### 3) Security, resource controls, and boundaries
- **Keyspace isolation** (`pkg/rpc/auth_tenant.go`)
  - Batch/RangeLookup/RangeFeed/SpanStats are denied if the requested key/span lies outside the tenant’s prefix unless the tenant has explicit cross-tenant read capability.
  - Internal endpoints like `GetRangeDescriptors` validate spans similarly.
- **Capability checks**
  - RPCs for node info, time series, span config updates, nodelocal blobs, etc., gated on specific capabilities; defaults are least-privilege for secondary tenants; system tenant gets all.
- **Span config bounds** (`pkg/spanconfig/...`)
  - Per-tenant span config bounds capability clamps effective configs applied by the tenant. Bounds do not apply to system tenant.
- **Cost control/admission**
  - Tenant-side KV interceptor and external IO recorder account RUs and throttle via a distributed token bucket (RFC “Distributed token bucket for tenant cost control”).
  - Connector forwards `TokenBucket` requests to host KV; KV-side server serializes updates per tenant and returns grants and fallback rates (`pkg/ccl/multitenantccl/tenantcostserver/...`).
  - Exemptions for critical internal traffic use `multitenant.WithTenantCostControlExemption(ctx)`.

### 4) Failure modes & recovery
- **Connector**: soft transport errors trigger client forget and redial; auth errors are surfaced to caller. Startup blocks until initial gossip + setting overrides arrive.
- **Cost control**: if token-bucket RPCs fail, the client uses a fallback grant rate bounded by refill + remaining burst over a time window; this avoids immediate stalls while isolating misconfiguration.
- **Isolation**: a tenant crash or overload only affects its own SQL instances and RU budget; storage continues to enforce bounds and authz independently.

### 5) Observability & triage
- Metrics
  - Cost controller client/server metrics (`pkg/multitenant/cost_controller.go`, `pkg/ccl/multitenantccl/tenantcost*`).
  - Connector node/store descriptor counts; settings watcher logs when initial scan completes and on overrides.
- Debug/status
  - Tenant-safe status/admin/time-series RPCs via connector (capability-gated). Hot ranges v2 requires tenant ID and permission checks.
- Playbooks
  - 429-like throttling or high latencies: check RU grant/consumption, token-bucket responses, and capability `ExemptFromRateLimiting`.
  - “key span not fully contained” errors: verify SQL attempted to read outside keyspace; inspect `rpc/auth_tenant.go` logs and the request spans.
  - Upgrade stuck: inspect `tenant_auto_upgrade` logs, settings watcher’s cached storage version, `cluster.preserve_downgrade_option`, and binary versions of all tenant instances.

### 6) Interoperability
- KV client/coordinator: uses DistSender/txn coordination via restricted connector interfaces.
- KV server & auth: relies on RPC authorization and key-bounds enforcement at the server boundary.
- Security: authenticates via tenant TLS; capability authorizer gates privileged operations.
- Settings propagation and defaults: observes system→tenant overrides and alternate defaults for compatibility.

### 7) Mixed-version and upgrade flows
- Storage cluster version is propagated to tenants via settings overrides (`settingswatcher.SettingsWatcher` caches as “storage cluster active version”).
- Tenant upgrade rules (`pkg/server/tenant_auto_upgrade.go`):
  - Auto-upgrade must be enabled and `cluster.preserve_downgrade_option` unset for current version.
  - Compute `minInstanceBinaryVersion` across live tenant SQL instances.
  - If tenant’s cluster version equals `minInstanceBinaryVersion`: done.
  - Else target = min(`minInstanceBinaryVersion`, storage cluster active version). If storage version < target: blocked.
  - Perform upgrade by `SET CLUSTER SETTING version = target` via `MigrationServer` after validating `minSupported ≤ target ≤ tenant binary` (`tenant_migration.go`).
- No engine version for tenants; only active cluster version is bumped.

### 8) Configuration & feature gates
- Settings:
  - `server.controller.default_tenant` / `server.controller.default_target_cluster` — default VC for unaffinitized SQL/HTTP.
  - `server.controller.mux_virtual_cluster_wait.timeout` — time the controller waits for default VC to become available.
  - `cluster.auto_upgrade.enabled`, `cluster.preserve_downgrade_option` — upgrade controls.
- Capabilities are stored in `system.tenants` and mutable by operators via SQL (see `pkg/sql/tenant_capability.go`).

### 9) Edge cases & gotchas
- Shared-process tenants do not run tenant-side cost controller; do not assume RU throttling there.
- Range metadata access from tenants must be proxied; direct scans over meta keys will fail.
- Cross-tenant reads require explicit capability; errors mention spans not contained within tenant keyspace.
- During tenant bootstrap or missing tenant record, connector may early-shutdown if configured for tests.

### 10) Examples (tiny)
- Route a scan: DistSender asks connector for range desc via `RangeLookup` → KV authz validates key in tenant span → returns descriptors.
- Check capability: servers use `tenantcapabilities.Authorizer` to gate RPCs (e.g., TSDB query requires `CanViewTSDBMetrics`).

### 11) References
- `docs/RFCS/20210604_distributed_token_bucket.md` (RU token bucket)
- `docs/RFCS/20220810_virtual_cluster_capabilities.md` (capabilities)
- See also: `pkg/sql/AGENTS.md` (SQL→KV boundary), `pkg/kv/kvclient/kvcoord/AGENTS.md`, `pkg/kv/kvserver/AGENTS.md`.

### 12) Glossary
- Virtual cluster (tenant): A logically isolated SQL cluster sharing the KV storage cluster.
- System tenant: Special tenant with full capabilities; hosts system tables and management services.
- Connector: Tenant-side component brokering restricted access to KV internals.
- Capability: Feature/permission bit controlling privileged ops available to a tenant.
- Request Unit (RU): Abstract resource unit for cost control across CPU, IO, network.
- Token bucket: RU grant mechanism providing burst + refill, enforced at tenant SQL.
- Span config bounds: Per-tenant clamp on allowed span config settings.
- Storage cluster active version: Version of the KV/storage cluster; upper bound for tenant versions.
- Service mode: Tenant service state: external/shared/none.

### 13) Search keys & synonyms
- kvtenant connector; tenant capabilities; token bucket; RU; span config bounds; tenant auto-upgrade; MigrationServer; settings watcher; rpc/auth_tenant; cross-tenant reads; virtual cluster.

Omitted sections: Background jobs & schedules
