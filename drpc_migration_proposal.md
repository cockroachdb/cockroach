# CockroachDB DRPC Migration and Rollout Proposal

## Executive Summary

This document provides a detailed technical proposal for migrating CockroachDB's networking layer from gRPC to DRPC. The analysis is grounded in the current implementation found in `pkg/rpc/`, `pkg/server/`, and related packages. DRPC is currently controlled by the cluster setting `rpc.experimental_drpc.enabled` (defined in `pkg/rpc/rpcbase/nodedialer.go:24-28`).

**Key Finding:** While DRPC infrastructure is largely in place, the current implementation has a **critical startup timing issue** that prevents safe migration without code changes.

---

## 1. Cluster Settings Architecture

### 1.1 Overview

Before diving into startup behavior, it's critical to understand how cluster settings work in CockroachDB, as this directly impacts the DRPC migration strategy.

### 1.2 Settings Registration and Initialization

**Registration** (`pkg/settings/registry.go`):
```go
// Settings are registered globally during package initialization
var ExperimentalDRPCEnabled = settings.RegisterBoolSetting(
    settings.ApplicationLevel,
    "rpc.experimental_drpc.enabled",
    "if true, use drpc to execute Batch RPCs (instead of gRPC)",
    envExperimentalDRPCEnabled)
```

**Default Value Initialization** (`pkg/settings/values.go:142-147`):
```go
func (sv *Values) Init(ctx context.Context, opaque interface{}) {
    sv.opaque = opaque
    for _, s := range registry {
        s.setToDefault(ctx, sv)  // Sets to registered default or env var
    }
}
```

Each setting gets:
- A unique slot index (1-based, up to MaxSettings = 1151)
- A default value (from code or environment variable)
- A setting class (SystemOnly, SystemVisible, or ApplicationLevel)

### 1.3 Settings Storage Architecture

**In-Memory Storage** (`pkg/settings/values.go:75-86`):
```go
type valuesContainer struct {
    intVals     [numSlots]int64        // For int/duration settings
    genericVals [numSlots]atomic.Value // For string/bool/other settings
    forbidden   [numSlots]bool         // Access control
    hasValue    [numSlots]uint32       // Value origin tracking
}
```

- Thread-safe via atomic operations
- Shared across all components that receive the same `cluster.Settings` instance
- Settings are **NOT** copied - all components reference the same `settings.Values`

### 1.4 Persisted Settings Location

Settings are persisted in the `system.settings` table (`pkg/sql/catalog/systemschema/system.go:88`):
```sql
CREATE TABLE system.settings (
    name STRING PRIMARY KEY,
    value STRING,
    lastUpdated TIMESTAMP,
    valueType STRING
);
```

### 1.5 Settings Loading from Disk

**When:** After engines are opened, via `SettingsWatcher` rangefeed

**Mechanism** (`pkg/server/settingswatcher/settings_watcher.go`):
```go
// SettingsWatcher watches system.settings table via rangefeed
type SettingsWatcher struct {
    settings *cluster.Settings
    mu struct {
        updater settings.Updater
        values  map[settings.InternalKey]settingsValue
    }
}

// Updates are applied via:
updater.SetFromStorage(ctx, key, value, origin)
```

**Important:** This happens **asynchronously** after startup via rangefeed, not synchronously during `NewServer()`.

### 1.6 Settings Propagation

**When a user runs `SET CLUSTER SETTING`:**

1. **SQL Layer** (`pkg/sql/set_cluster_setting.go:536`):
   ```sql
   UPSERT INTO system.settings (name, value, "lastUpdated", "valueType")
   VALUES ($1, $2, now(), $3)
   ```

2. **Rangefeed Notification**:
   - All nodes watch `system.settings` via rangefeed
   - Changes are broadcast cluster-wide within milliseconds

3. **Local Update** (`pkg/server/settingswatcher/`):
   ```go
   updater.SetFromStorage(ctx, key, encodedValue, ValueOriginStorage)
   ```

4. **Change Callbacks**:
   ```go
   // Registered callbacks are invoked
   setting.SetOnChange(&sv.SV, func(ctx context.Context) {
       // React to setting change
   })
   ```

### 1.7 Setting Classes

From `pkg/settings/setting.go`:

| Class | Visibility | Use Case | Example |
|-------|-----------|----------|---------|
| **SystemOnly** | KV/storage layer only | Storage configuration | `kv.range_split.by_load_enabled` |
| **SystemVisible** | All virtual clusters (read-only) | Cross-tenant coordination | `cluster.version` |
| **ApplicationLevel** | Per virtual cluster | SQL/app configuration | `rpc.experimental_drpc.enabled` |

**`rpc.experimental_drpc.enabled` is ApplicationLevel** - can be set independently per virtual cluster.

### 1.8 Reading Settings

**At runtime** (`pkg/settings/values.go:102-110`):
```go
func (c *valuesContainer) getInt64(slot slotIdx) int64 {
    c.checkForbidden(slot)  // Access control check
    return atomic.LoadInt64(&c.intVals[slot])  // Atomic read
}

// Usage:
enabled := rpcbase.ExperimentalDRPCEnabled.Get(&settings.SV)
```

- **No disk I/O** - reads from in-memory atomic values
- **Thread-safe** - uses atomic operations
- **Fast** - just a memory load

---

## 2. Startup Behavior Analysis

### 2.1 RPC Initialization Timeline

Based on analysis of `pkg/server/server.go`, the startup sequence is:

```
NewServer() (line 247)
  ├─> Get Settings from Config (line 254)
  │   └─> st := cfg.Settings
  │       - Settings object exists
  │       - Contains ONLY default values (set via Init())
  │       - Persisted values NOT loaded yet
  │
  ├─> Create RPC Context (line 361)
  │   └─> rpcContext := rpc.NewContext(ctx, rpcCtxOpts)
  │       - rpcCtxOpts.Settings = cfg.Settings (line 346)
  │       - RPC context captures settings object
  │       - DRPC setting read here has default/env var value only
  │
PreStart() (line 1535)
  ├─> Load cluster version from disk (line 1608)
  ├─> Initialize cluster version setting (line 1636)
  │   └─> clusterversion.Initialize(ctx, initialDiskClusterVersion.Version, &s.cfg.Settings.SV)
  │       - NOW cluster version is set from disk
  │       - Other settings still have defaults
  │
  ├─> Start RPC listeners (line 1675)
  ├─> Start Gossip (line 1938)
  │   └─> FIRST RPC CALLS OCCUR HERE
  │
  ├─> Start Node Liveness (line 2056)
  ├─> Start Raft Transport (line 2260)
  └─> Start SettingsWatcher (after PreStart completes)
      └─> Rangefeed begins watching system.settings
          - Persisted settings loaded asynchronously
          - Updates applied to same settings.Values object
```

### 2.2 Cluster Settings Availability

**Critical Finding:** At RPC context creation time, cluster settings **ONLY have default values**.

**Evidence:**

1. **Settings initialized with defaults** (`pkg/settings/values.go:142-147`):
   ```go
   func (sv *Values) Init(ctx context.Context, opaque interface{}) {
       for _, s := range registry {
           s.setToDefault(ctx, sv)  // Default or env var only
       }
   }
   ```

2. **RPC context created in `NewServer()`** (line 361):
   ```go
   st := cfg.Settings  // Line 254 - has defaults only
   rpcCtxOpts.Settings = cfg.Settings  // Line 346
   rpcContext := rpc.NewContext(ctx, rpcCtxOpts)  // Line 361
   ```

3. **Persisted settings loaded later** in `PreStart()` (line 1636+):
   ```go
   // Cluster version initialized from disk
   clusterversion.Initialize(ctx, initialDiskClusterVersion.Version, &s.cfg.Settings.SV)

   // Other settings loaded asynchronously via SettingsWatcher
   // (starts after PreStart completes)
   ```

### 2.3 The Startup Problem

**The Issue:** The RPC context is created before persisted cluster settings are available.

**Timeline breakdown:**
```
T0: NewServer() starts
    └─> Settings have defaults only

T1: rpc.NewContext() created (line 361)
    └─> Reads rpc.experimental_drpc.enabled
    └─> Value = envExperimentalDRPCEnabled (default: false)

T2: PreStart() starts (line 1535)
    └─> Cluster version loaded from disk
    └─> Other settings still have defaults

T3: Gossip.Start() (line 1938)
    └─> FIRST RPC CALLS - using RPC context from T1

T4: SettingsWatcher starts (after PreStart)
    └─> Rangefeed watches system.settings
    └─> Persisted value of rpc.experimental_drpc.enabled loaded
    └─> BUT: RPC context already created at T1
```

**Implications:**

1. **Cannot use persisted DRPC setting at startup** - Only environment variable matters
2. **Both gRPC and DRPC servers created unconditionally** - See `pkg/server/server.go:412-420`
3. **Node restarts with wrong protocol** - If cluster has `rpc.experimental_drpc.enabled=true` persisted, but env var is false (default), node will start using gRPC

**What's available when:**

| Timing | Setting Value Source | Available to RPC Context? |
|--------|---------------------|--------------------------|
| `NewServer()` | Code default or env var | ✅ Yes (but wrong value) |
| `PreStart()` early | Code default or env var | ❌ Already created |
| `PreStart()` late | Cluster version from disk | ❌ Already created |
| After `SettingsWatcher` | Persisted from `system.settings` | ❌ Already created |
| Runtime (after startup) | Persisted from `system.settings` | ✅ Yes (but no restart) |

### 2.4 Settings After Startup

**Important:** Even though the RPC context is created with default values, the underlying `settings.Values` object is **shared**.

```go
// RPC context holds a reference
rpcContext.Settings -> cfg.Settings

// SettingsWatcher updates the same object
settingsWatcher.settings -> cfg.Settings  // Same instance!

// When setting is updated:
updater.SetFromStorage(ctx, "rpc.experimental_drpc.enabled", "true", ...)
// This updates cfg.Settings.SV atomically

// Future reads see new value:
enabled := rpcbase.ExperimentalDRPCEnabled.Get(&rpcContext.Settings.SV)
// Returns true (the updated value)
```

This is why dynamic switching theoretically works - the setting is checked at **dial time**, not at RPC context creation time. However, as covered in Section 4, dynamic switching has other critical problems.

### 2.5 Earliest RPC Call Point

The earliest RPC calls occur in `pkg/server/server.go:1938`:
```go
s.gossip.Start(advAddrU, filtered, s.rpcContext)
```

At this point:
- RPC context fully initialized (with default settings)
- Both gRPC and DRPC servers are running
- Cluster version loaded from disk
- Other settings still have defaults (SettingsWatcher not started yet)
- Node is ready to make outbound connections

---

## 3. Upgrade & Versioning Constraints

### 3.1 CockroachDB Upgrade Process

CockroachDB uses a two-phase upgrade process:

**Phase 1: Binary Upgrade (Rolling Restart)**
- Nodes are restarted one-by-one with new binaries
- Mixed binary versions are supported (within one major release)
- No cluster-wide coordination required
- Cluster remains fully available

**Phase 2: Cluster Version Upgrade**
- Triggered via `SET CLUSTER SETTING version = 'X.Y'` or auto-upgrade
- Uses `BumpClusterVersion` RPC (`pkg/server/migration.go`)
- Version is persisted to all storage engines before activation
- Features are gated on cluster version via `clusterversion.IsActive()`

### 3.2 Version Validation

From `pkg/server/migration.go`, the `ValidateTargetClusterVersion` RPC ensures:
```
node's MinSupportedVersion ≤ targetVersion ≤ node's BinaryVersion
```

### 3.3 Implications for DRPC Rollout

**Constraint 1:** DRPC must support mixed-mode operation during binary upgrades
- Old nodes on gRPC must be able to communicate with new nodes that support DRPC
- The RPC protocol choice must be backward compatible

**Constraint 2:** DRPC enablement should be tied to cluster version
- Cannot enable DRPC until all nodes support it
- Should use a cluster version gate similar to other features
- Requires all nodes to be on binaries that support DRPC

**Constraint 3:** Rolling restart is mandatory
- Changing RPC implementation requires node restart (as RPC context is created at startup)
- Cannot hot-swap RPC protocols without restart

---

## 4. Dynamic Enablement Evaluation

### 4.1 Current Implementation

The cluster setting `rpc.experimental_drpc.enabled` is checked at **connection establishment time** in `pkg/rpc/nodedialer/nodedialer.go:174`:

```go
func (n *Dialer) DialInternalClient(
    ctx context.Context, nodeID roachpb.NodeID, class rpcbase.ConnectionClass,
) (rpc.RestrictedInternalClient, error) {
    // ...
    if !rpcbase.DRPCEnabled(ctx, n.rpcContext.Settings) {
        // Use gRPC
        gc, conn, err := dial(ctx, n.resolver, n.rpcContext.GRPCDialNode, nodeID, class, true)
        // ...
    } else {
        // Use DRPC
        dc, conn, err := dial(ctx, n.resolver, n.rpcContext.DRPCDialNode, nodeID, class, true)
        // ...
    }
}
```

### 4.2 Connection Lifecycle

**Connection Establishment:** Lazy (on-demand)
- Connections are created when first needed
- Managed through `pkg/rpc/nodedialer/`
- Subject to circuit breakers (`pkg/util/circuit/`)

**Connection Pooling:**
- DRPC idle timeout: 5 minutes (`pkg/rpc/drpc.go:32`)
- Connection reuse via connection pools
- Stream pooling configurable via `rpc.batch_stream_pool.enabled`

**Connection State:**
- Tracked via `Connection[Conn]` struct in `pkg/rpc/connection.go`
- Circuit breaker per (NodeID, Class, Addr) tuple
- Close notification via channels

### 4.3 Dynamic Switching Feasibility Analysis

**Technically Possible But Unsafe:**

The current code theoretically allows changing `rpc.experimental_drpc.enabled` at runtime because:
1. ✅ The setting is checked per-connection attempt
2. ✅ New connections will use the new value
3. ✅ Both gRPC and DRPC servers run concurrently

**However, Critical Problems Exist:**

**Problem 1: Existing Connections Not Recreated**
- Existing gRPC connections remain gRPC connections
- No automatic draining or recreation mechanism
- Connection pools may hold connections for up to 5 minutes (idle timeout)
- Long-running streams (DistSQL, Raft) may persist indefinitely

**Problem 2: Asymmetric Configuration Risk**
```
Scenario:
- Node A enables DRPC (sets rpc.experimental_drpc.enabled = true)
- Node B has not yet received the setting update
- Node A tries to dial Node B using DRPC
- Node B's DRPC server is running but may not be properly configured
- Potential connection failures or silent errors
```

**Problem 3: No Protocol Negotiation**
- No handshake to verify both sides support the same protocol
- No fallback mechanism if DRPC fails
- Client chooses protocol unilaterally based on local setting

**Problem 4: Startup Setting Mismatch**
- As identified in Section 1.3, nodes restart with environment variable defaults
- Persisted cluster setting not available at RPC context creation time
- Nodes may start with wrong RPC protocol

### 4.4 Code Paths That Prevent Safe Dynamic Switching

**Path 1: RPC Context Initialization** (`pkg/rpc/context.go`)
```go
// RPC context created once at startup
rpcContext := rpc.NewContext(ctx, rpcCtxOpts)

// Dial functions bound at creation time
rpcContext.GRPCDialNode  // Set once, never changes
rpcContext.DRPCDialNode  // Set once, never changes
```

**Path 2: Server Registration** (`pkg/server/server.go:417-425`)
```go
drpcServer, err := newDRPCServer(ctx, rpcContext, requestMetrics)

// All services registered at startup
gossip.DRPCRegisterGossip(drpcServer, g.AsDRPCServer())
serverpb.DRPCRegisterInit(s.drpc.DRPCServer, initServer)
// ... etc
```

These servers cannot be dynamically recreated or reconfigured.

**Path 3: Circuit Breaker Coupling** (`pkg/rpc/context.go:1533-1545`)
```go
func (rpcCtx *Context) GetBreakerForAddr(...) (*circuitbreaker.Breaker, bool) {
    if !rpcbase.DRPCEnabled(...) {
        return rpcCtx.grpcGetBreakerForAddr(k)
    }
    return rpcCtx.drpcGetBreakerForAddr(k)
}
```

Circuit breakers are separate for gRPC and DRPC - changing protocols resets breaker state.

### 4.5 Conclusion: Dynamic Enablement Not Feasible

**Verdict: Dynamic switching without node restart is NOT SAFE** due to:
1. No mechanism to drain and recreate existing connections
2. Asymmetric configuration risk between nodes
3. Circuit breaker state loss
4. Startup configuration timing issue

**Required: Node restart to change RPC protocol**

---

## 5. Mixed-Mode Cluster Behavior

### 5.1 Definition of Mixed-Mode

In the context of DRPC migration, "mixed-mode" can mean:

**Mode 1:** Mixed binary versions (some nodes support DRPC, some don't)
- Occurs during rolling binary upgrade
- Older nodes: gRPC only
- Newer nodes: gRPC + DRPC capable

**Mode 2:** Mixed configuration (all nodes support DRPC, some have it enabled)
- All nodes on DRPC-capable binary
- Some nodes: `rpc.experimental_drpc.enabled = false` (using gRPC)
- Some nodes: `rpc.experimental_drpc.enabled = true` (using DRPC)

### 5.2 Current Support for Mixed-Mode

**The current implementation DOES support mixed-mode operation:**

Both gRPC and DRPC servers run concurrently on every node:
- gRPC server: `pkg/server/server.go:412`
- DRPC server: `pkg/server/server.go:417`
- Both listen on the same port with protocol multiplexing

From `pkg/server/start_listen.go`, the listener setup uses:
```go
// Single listener multiplexes both gRPC and DRPC
grpcL, drpcL, err := startRPCAndSQLListener(...)
```

This leverages `drpcmigrate.DialWithHeader()` which sends a protocol header to distinguish DRPC from gRPC.

### 5.3 Component-by-Component Impact

Based on exploration, all major components have DRPC support:

| Component | Location | DRPC Support | Impact |
|-----------|----------|--------------|--------|
| **Gossip** | `pkg/gossip/client.go` | ✅ Full | Connections per-node based on setting |
| **Node Liveness** | `pkg/kv/kvserver/liveness/` | ✅ Full | Uses nodedialer, auto-selects protocol |
| **Raft Transport** | `pkg/kv/kvserver/raft_transport.go` | ✅ Full | Supports both protocols |
| **DistSQL** | `pkg/sql/distsql/server.go` | ✅ Full | `execinfrapb.DistSQL` registered for both |
| **KV Batch** | `pkg/kv/kvpb/` | ✅ Full | `BatchStream` works with both |
| **Closed Timestamp** | `pkg/kv/kvserver/closedts/` | ✅ Full | Side transport supports both |
| **Migration RPC** | `pkg/server/migration.go` | ✅ Full | Both protocols registered |
| **Internal Admin** | `pkg/server/` | ✅ Full | All admin RPCs registered for both |

**Implementation Pattern:**
```go
// Service registration for both protocols
serverpb.RegisterInitServer(s.grpc.Server, initServer)
serverpb.DRPCRegisterInit(s.drpc.DRPCServer, initServer)
```

### 5.4 Per-Connection Protocol Selection

The protocol is selected **per outbound connection** in `pkg/rpc/rpcbase/nodedialer.go:52-75`:

```go
func DialRPCClient[C any](
    nd NodeDialer,
    ctx context.Context,
    nodeID roachpb.NodeID,
    class ConnectionClass,
    grpcClientFn func(*grpc.ClientConn) C,
    drpcClientFn func(drpc.Conn) C,
    st *cluster.Settings,
) (C, error) {
    if !DRPCEnabled(ctx, st) {
        conn, err := nd.Dial(ctx, nodeID, class)
        return grpcClientFn(conn), nil
    }

    conn, err := nd.DRPCDial(ctx, nodeID, class)
    return drpcClientFn(conn), nil
}
```

**Key Insight:** The calling node's setting determines the protocol, not the receiving node's setting.

### 5.5 Mixed-Mode Scenarios

**Scenario A: Node A (DRPC enabled) → Node B (DRPC disabled)**
```
✅ Works: Node A uses DRPC client, Node B's DRPC server accepts
✅ Both servers running on all nodes
✅ No compatibility issue
```

**Scenario B: Node A (DRPC disabled) → Node B (DRPC enabled)**
```
✅ Works: Node A uses gRPC client, Node B's gRPC server accepts
✅ Both servers running on all nodes
✅ No compatibility issue
```

**Scenario C: Node A (old binary, no DRPC) → Node B (new binary, DRPC enabled)**
```
✅ Works: Node A uses gRPC only, Node B's gRPC server accepts
⚠️  B→A connections use gRPC (B checks setting, sees enabled, but A doesn't support DRPC)
❌ PROBLEM: B will try DRPC to A, but A doesn't have DRPC server
```

### 5.6 The Critical Gap: Binary Version Detection

**Current Code Has No Binary Version Check Before Using DRPC**

In `pkg/rpc/rpcbase/nodedialer.go:104-106`:
```go
func DRPCEnabled(ctx context.Context, st *cluster.Settings) bool {
    return st != nil && ExperimentalDRPCEnabled.Get(&st.SV)
}
```

This only checks the local setting - it doesn't check if the remote node supports DRPC.

**Missing:** A mechanism like:
```go
// Pseudocode - doesn't exist in current implementation
if DRPCEnabled(ctx, st) && remotePeerSupportsDRPC(nodeID) {
    // Use DRPC
} else {
    // Use gRPC
}
```

### 5.7 Conclusion: Mixed-Mode Support Assessment

**Current state:**
- ✅ Infrastructure supports mixed-mode (both servers run)
- ✅ Works if all nodes have same binary version
- ❌ **UNSAFE** if mixing DRPC-enabled and DRPC-incapable binaries
- ❌ No fallback mechanism if DRPC connection fails

**Required for safe mixed-mode:**
1. Cluster version gate to enable DRPC
2. Minimum version check before attempting DRPC connections
3. Fallback to gRPC if DRPC unavailable

---

## 6. Inflight Request Handling

### 6.1 Request Lifecycle and Tracking

**RPC Request Flow:**
```
Client → NodeDialer → Connection → Circuit Breaker → Network → Server
```

**Inflight Tracking Mechanisms:**

1. **Stopper Tasks** (`pkg/util/stop/stopper.go`)
   - Every RPC handler runs in a stopper task
   - Stopper tracks count of running tasks
   - Prevents new tasks during shutdown

2. **Connection Futures** (`pkg/rpc/connection.go:69-90`)
   ```go
   type connFuture struct {
       // Channel signals connection readiness
       ch chan struct{}
       // Stores connection or error
       result struct {
           conn Conn
           err error
       }
   }
   ```

3. **Stream Pools** (`pkg/rpc/connection.go`)
   - Manages pooled batch stream connections
   - Tracks active streams
   - Handles stream lifecycle

### 6.2 Connection Closure Handling

**When a connection is closed:**

1. **Close notification** (`pkg/rpc/drpc.go:38-40`)
   ```go
   func (d *drpcCloseNotifier) CloseNotify(ctx context.Context) <-chan struct{} {
       return d.conn.Closed()
   }
   ```

2. **Circuit breaker trips** (`pkg/util/circuit/circuitbreaker.go`)
   ```go
   func (b *Breaker) Report(err error) {
       close(b.mu.errAndCh.ch)  // Trip the breaker
       b.maybeTriggerProbe(false)  // Start healing
   }
   ```

3. **Inflight requests fail** with connection error
   - Returned to caller
   - Subject to retry logic

### 6.3 Graceful Shutdown Process

**Stopper Quiesce** (`pkg/util/stop/stopper.go`):

```
1. Close quiescer channel
   └─> stop.ShouldQuiesce() returns closed channel
2. Stop accepting new tasks
   └─> RunTask() returns errors
3. Wait for existing tasks
   └─> Spin until NumTasks() == 0
4. Run registered closers
   └─> Close all connections
```

**RPC Server Draining** (`pkg/server/server.go:2053-2054`):
```go
s.grpc.setMode(modeOperational)
s.drpc.setMode(modeOperational)
```

Server modes control RPC acceptance:
- `modeInitializing`: Only certain RPCs allowed
- `modeOperational`: All RPCs allowed
- `modeDraining`: Stop accepting new RPCs (during shutdown)

### 6.4 Retry Mechanisms

**KV Transaction Retry** (`pkg/kv/txn.go`):

```go
for attempt := 1; ; attempt++ {
    err = fn(ctx, txn)

    if !errors.HasType(err, (*kvpb.TransactionRetryWithProtoRefreshError)(nil)) {
        break
    }

    if attempt >= maxRetries {
        txn.Rollback(ctx)
        break
    }

    txn.PrepareForRetry(ctx)
}
```

**Retry Conditions:**
- Network errors
- Timestamp conflicts
- Transaction push errors
- Circuit breaker errors

**No Retry:**
- Context cancellation
- Explicit transaction abort
- Stopper quiescing

### 6.5 Scenario: Node Switches from gRPC to DRPC

**This scenario is hypothetical** (dynamic switching not supported), but analyzing what would happen:

**Outbound connections (this node calling others):**
```
1. Node A changes rpc.experimental_drpc.enabled = true
2. Existing gRPC connections remain active
3. New connections use DRPC
4. Eventually old connections expire (5min timeout) or close
5. All new connections use DRPC
```

**Inbound connections (others calling this node):**
```
1. Node A has both gRPC and DRPC servers running
2. Remote nodes still using gRPC: ✅ Continue working (connect to gRPC server)
3. Remote nodes using DRPC: ✅ Work (connect to DRPC server)
4. No interruption to inbound traffic
```

**Inflight requests at moment of switch:**
```
1. Requests on existing connections: ✅ Complete normally
2. No connection disruption (setting only affects NEW connections)
3. Circuit breakers: ✅ Remain valid (per-protocol breakers)
```

**The problem:**
```
❌ Connections may remain on old protocol indefinitely
❌ Long-running streams (DistSQL) won't switch
❌ No way to force connection refresh
```

### 6.6 Scenario: Connection Severed Mid-Request

**Network partition or node failure:**

```
1. TCP connection breaks
2. Pending requests fail with connection error
3. Circuit breaker trips immediately
4. Client receives error (e.g., "connection reset")
5. Retry logic activates:
   └─> For transactional KV: PrepareForRetry()
   └─> For DistSQL: Query fails (user can retry)
   └─> For Raft: Queued and retried automatically
6. Circuit breaker probe attempts reconnection
7. If successful, breaker resets
8. New requests succeed
```

**Code path** (`pkg/rpc/connection.go:122-125`):
```go
// Check the circuit breaker first
if sig := breakerSignalFn(); sig != nil {
    return nilConn, errors.Wrapf(circuit.ErrBreakerOpen, ...)
}
```

### 6.7 Retry Behavior for Different RPC Types

| RPC Type | Retry Strategy | Location |
|----------|---------------|----------|
| **KV Batch** | Automatic with txn restart | `pkg/kv/txn.go` |
| **Gossip** | Automatic reconnect | `pkg/gossip/client.go` |
| **Raft Messages** | Queued, retry async | `pkg/kv/kvserver/raft_transport.go` |
| **DistSQL** | Query-level, not auto | `pkg/sql/distsql/` |
| **Node Liveness** | Heartbeat retry with backoff | `pkg/kv/kvserver/liveness/` |
| **Admin RPCs** | Client responsibility | Various |

### 6.8 Transition Impact Assessment

**If attempting to switch RPC protocols (gRPC ↔ DRPC):**

**Low Risk:**
- ✅ Short-lived requests (KV point reads/writes) - Will retry automatically
- ✅ Gossip traffic - Reconnects automatically
- ✅ Heartbeats - Retry with backoff

**Medium Risk:**
- ⚠️ Raft snapshots - Large transfers may fail and retry (bandwidth waste)
- ⚠️ Bulk operations - May cause temporary slowness

**High Risk:**
- ❌ Long-running DistSQL queries - May fail with user-visible errors
- ❌ Backups/restores in progress - May fail completely
- ❌ Schema changes - May require cleanup and restart

**Recommended:** Coordinate transition during maintenance window with no long-running operations.

---

## 7. Migration Challenges and Solutions

### Challenge 1: Startup Setting Availability

**Problem:**
- RPC context created in `NewServer()` before cluster settings loaded
- Persisted value of `rpc.experimental_drpc.enabled` not available
- Nodes restart with environment variable default

**Evidence:**
```
pkg/server/server.go:254  - st := cfg.Settings (has defaults only)
pkg/server/server.go:346  - rpcCtxOpts.Settings = cfg.Settings
pkg/server/server.go:361  - rpc.NewContext() called (uses defaults)
pkg/server/server.go:1636 - clusterversion.Initialize() called (only cluster version)
(After PreStart)          - SettingsWatcher loads other settings asynchronously
```

**Solution:**

**Option A: Defer RPC Context Creation** (Recommended)
```go
// Modify server.go startup sequence
func NewServer(...) {
    // ... early initialization ...

    // Load cluster version FIRST
    inspectedDiskState, _ := inspectEngines(ctx, engines, ...)
    initialVersion := inspectedDiskState.clusterVersion
    clusterversion.Initialize(ctx, initialVersion, &cfg.Settings.SV)

    // NOW create RPC context with correct settings
    rpcContext := rpc.NewContext(ctx, rpcCtxOpts)

    // ... continue ...
}
```

**Impact:** Moderate refactoring of startup sequence

**Option B: Environment Variable Control** (Interim)
```bash
# Set on all nodes via systemd or init script
export COCKROACH_EXPERIMENTAL_DRPC_ENABLED=true
```

**Impact:** Requires external configuration management, error-prone

**Recommended: Option A** - Fix the code to load settings before creating RPC context

---

### Challenge 2: Lack of Binary Version Check

**Problem:**
- No check if remote node supports DRPC before attempting connection
- Can try to DRPC-dial a node that doesn't support it
- Results in connection failures

**Evidence:**
```
pkg/rpc/rpcbase/nodedialer.go:104-106
// Only checks local setting, not remote capability
func DRPCEnabled(...) bool {
    return ExperimentalDRPCEnabled.Get(&st.SV)
}
```

**Solution:**

**Implement Capability Detection:**

```go
// New function in pkg/rpc/rpcbase/nodedialer.go
func shouldUseDRPC(
    ctx context.Context,
    st *cluster.Settings,
    nodeID roachpb.NodeID,
    clusterVersion clusterversion.Handle,
) bool {
    // Check local setting
    if !ExperimentalDRPCEnabled.Get(&st.SV) {
        return false
    }

    // Check cluster version supports DRPC
    if !clusterVersion.IsActive(clusterversion.V25_1_DRPCSupport) {
        return false
    }

    // All nodes at this version support DRPC
    return true
}
```

**Modify connection logic:**
```go
// In pkg/rpc/nodedialer/nodedialer.go:174
func (n *Dialer) DialInternalClient(...) {
    if !shouldUseDRPC(ctx, n.rpcContext.Settings, nodeID, n.clusterVersion) {
        // Use gRPC
        gc, conn, err := dial(ctx, n.resolver, n.rpcContext.GRPCDialNode, ...)
    } else {
        // Use DRPC
        dc, conn, err := dial(ctx, n.resolver, n.rpcContext.DRPCDialNode, ...)
    }
}
```

**Impact:** Small code change, requires cluster version gate

---

### Challenge 3: No Fallback Mechanism

**Problem:**
- If DRPC connection fails, no automatic fallback to gRPC
- Can cause availability issues during transition

**Solution:**

**Implement Fallback Logic:**

```go
func (n *Dialer) DialInternalClient(...) {
    // Try DRPC first if enabled
    if shouldUseDRPC(...) {
        dc, conn, err := dial(ctx, n.resolver, n.rpcContext.DRPCDialNode, ...)
        if err == nil {
            return newDRPCClient(dc), nil
        }

        // Log fallback
        log.Warningf(ctx, "DRPC connection to n%d failed (%v), falling back to gRPC", nodeID, err)
    }

    // Use gRPC (fallback or primary)
    gc, conn, err := dial(ctx, n.resolver, n.rpcContext.GRPCDialNode, ...)
    return newGRPCClient(gc), err
}
```

**Impact:** Small code change, improves reliability

---

### Challenge 4: Connection Draining

**Problem:**
- No mechanism to force existing connections to close and recreate
- Old gRPC connections may persist after enabling DRPC
- Inconsistent cluster state

**Solution:**

**Option A: Connection TTL** (Recommended)
```go
// Add to pkg/rpc/context.go
var connectionTTL = settings.RegisterDurationSetting(
    settings.ApplicationLevel,
    "rpc.connection.max_age",
    "maximum age of an RPC connection before forced recreation",
    10 * time.Minute,
)

// Implement in connection management
type Connection[Conn] struct {
    createdAt time.Time
    // ...
}

func (c *Connection[Conn]) Connect(...) (Conn, error) {
    age := timeutil.Since(c.createdAt)
    if age > connectionTTL.Get(&settings.SV) {
        // Force recreation
        c.close()
        c.createdAt = timeutil.Now()
    }
    // ... normal connection logic ...
}
```

**Option B: Administrative Command**
```sql
-- New SQL command
ALTER CLUSTER RESET RPC CONNECTIONS;
```

Triggers connection drain across cluster.

**Recommended: Option A** - Automatic, no manual intervention needed

---

### Challenge 5: Long-Running Operations

**Problem:**
- DistSQL queries, backups, schema changes may fail during transition
- User-facing errors during migration

**Solution:**

**Graceful Migration Window:**

1. **Prepare Phase:**
   ```sql
   -- Notify users
   SET CLUSTER SETTING server.shutdown.drain_wait = '120s';
   SET CLUSTER SETTING server.shutdown.query_wait = '120s';
   ```

2. **Wait for Operations:**
   ```sql
   -- Check for long-running operations
   SELECT count(*) FROM [SHOW QUERIES] WHERE start < now() - INTERVAL '5 minutes';
   SELECT count(*) FROM [SHOW JOBS] WHERE status = 'running';
   ```

3. **Enable DRPC:**
   ```sql
   SET CLUSTER SETTING rpc.experimental_drpc.enabled = true;
   ```

4. **Rolling Restart:**
   - Restart nodes one at a time
   - Wait for node to rejoin cluster
   - Verify health before proceeding

**Impact:** Requires maintenance window, but avoids failures

---

### Challenge 6: Testing and Validation

**Problem:**
- Complex state space (gRPC, DRPC, mixed-mode)
- Need comprehensive testing before production rollout

**Solution:**

**Testing Strategy:**

1. **Unit Tests:**
   - Test `DRPCEnabled()` logic with various settings
   - Test connection selection logic
   - Test fallback mechanisms

2. **Roachtests:**
   - Mixed-mode cluster test
   - Rolling upgrade with DRPC enablement
   - Failure injection during transition

3. **Chaos Engineering:**
   - Randomly kill connections during DRPC migration
   - Verify retry behavior
   - Ensure no data corruption

4. **Performance Testing:**
   - Benchmark gRPC vs DRPC latency
   - Throughput comparison
   - Overhead measurement

**Test Locations:**
- `pkg/rpc/*_test.go` - Unit tests
- `pkg/cmd/roachtest/tests/` - Integration tests

---

## 8. Recommended Rollout Plan

### Phase 0: Preparation (Pre-requisites)

**Code Changes Required:**

1. **Fix startup timing** (Challenge 1)
   - Move cluster settings load before RPC context creation
   - PR estimated: ~500 LOC changes in `pkg/server/server.go`

2. **Add cluster version gate** (Challenge 2)
   - Define `V25_1_DRPCSupport` version key
   - Implement capability check
   - PR estimated: ~200 LOC changes

3. **Implement fallback** (Challenge 3)
   - Add gRPC fallback if DRPC fails
   - PR estimated: ~100 LOC changes

4. **Add connection TTL** (Challenge 4)
   - Implement max connection age
   - PR estimated: ~300 LOC changes

5. **Comprehensive testing**
   - Roachtests for mixed-mode operation
   - Stress tests for connection transitions
   - Estimated: 2-3 weeks testing cycles

**Timeline:** 1-2 releases (6-12 weeks)

---

### Phase 1: Binary Rollout (DRPC Capability)

**Objective:** Deploy binaries that support DRPC, but keep it disabled

**Steps:**

1. **Release V25.1 with DRPC support**
   - All code changes from Phase 0 included
   - `rpc.experimental_drpc.enabled` defaults to `false`
   - Cluster version `V25_1_DRPCSupport` defined but not active

2. **Rolling binary upgrade:**
   ```bash
   # For each node:
   systemctl stop cockroach
   # Replace binary
   systemctl start cockroach
   # Wait for node to rejoin
   # Verify: SELECT * FROM crdb_internal.gossip_liveness
   ```

3. **Validation:**
   ```sql
   -- Verify all nodes on V25.1
   SELECT node_id, build_tag FROM crdb_internal.cluster_versions;

   -- DRPC should still be disabled
   SHOW CLUSTER SETTING rpc.experimental_drpc.enabled;
   -- Returns: false
   ```

**Rollback Plan:** Standard binary downgrade (within version skew policy)

**Duration:** 1-2 hours per cluster (depending on size)

---

### Phase 2: Cluster Version Upgrade

**Objective:** Activate V25.1 cluster version to enable DRPC capability

**Steps:**

1. **Trigger version upgrade:**
   ```sql
   SET CLUSTER SETTING version = '25.1';
   ```

2. **Monitor version upgrade:**
   ```sql
   SELECT * FROM system.migrations WHERE major = 25 AND minor = 1;
   ```

3. **Validation:**
   ```sql
   -- All nodes should be at V25.1
   SELECT min(version) FROM crdb_internal.cluster_versions;
   ```

**Rollback Plan:** Cannot downgrade cluster version (one-way operation)

**Duration:** Minutes (version bump is fast)

---

### Phase 3: DRPC Enablement (Per-Cluster)

**Objective:** Enable DRPC on a single cluster (canary)

**Prerequisites:**
- No long-running operations:
  ```sql
  SELECT count(*) FROM [SHOW JOBS] WHERE status IN ('running', 'pending');
  -- Should be 0 or only background jobs
  ```

**Steps:**

1. **Enable DRPC cluster setting:**
   ```sql
   SET CLUSTER SETTING rpc.experimental_drpc.enabled = true;
   ```

2. **Rolling restart to pick up setting:**
   ```bash
   # For each node:
   # Option 1: Graceful drain
   cockroach node drain --self
   systemctl restart cockroach

   # Option 2: Direct restart (faster, brief unavailability)
   systemctl restart cockroach

   # Wait for node to rejoin
   # Verify using:
   cockroach node status
   ```

3. **Monitor connection transitions:**
   ```sql
   -- Check RPC metrics
   SELECT * FROM crdb_internal.node_metrics
   WHERE name LIKE '%rpc%connection%';

   -- Monitor circuit breaker trips
   SELECT * FROM crdb_internal.node_metrics
   WHERE name LIKE '%circuit_breaker%';
   ```

4. **Validation:**
   ```bash
   # Check DRPC server is serving traffic
   # (Requires observability tooling or metrics)
   curl http://node:8080/_status/vars | grep drpc_connections
   ```

**Expected Behavior:**
- ✅ New connections use DRPC
- ✅ Old gRPC connections drain over time (TTL: 10 minutes)
- ✅ No user-visible errors
- ✅ Metrics show DRPC connection count increasing

**Rollback Plan:**
```sql
-- Disable DRPC
SET CLUSTER SETTING rpc.experimental_drpc.enabled = false;

-- Rolling restart
# Nodes will revert to gRPC
```

**Duration:** 30-60 minutes per cluster (depends on drain settings)

---

### Phase 4: Progressive Rollout

**Objective:** Enable DRPC across all clusters

**Strategy:**

1. **Week 1: Dev/Test Clusters**
   - Enable DRPC on internal development clusters
   - Run full test suite
   - Monitor for 1 week

2. **Week 2: Canary Production Cluster**
   - Choose low-traffic production cluster
   - Enable DRPC with Phase 3 procedure
   - Monitor intensively (24/7 on-call)

3. **Week 3-4: 10% Production Rollout**
   - Enable on 10% of production clusters
   - Monitor metrics:
     - Latency (p50, p99, p999)
     - Error rates
     - Circuit breaker trips
     - CPU/memory usage

4. **Week 5-8: 50% Production Rollout**
   - If metrics look good, expand to 50%
   - Continue monitoring

5. **Week 9-12: 100% Production Rollout**
   - Complete rollout to all clusters
   - Declare DRPC as default

**Go/No-Go Criteria:**

✅ **Proceed if:**
- RPC error rate < 0.01%
- P99 latency within 5% of gRPC baseline
- No circuit breaker thrashing
- No user-facing issues

❌ **Halt if:**
- Error rate spike > 0.1%
- Latency regression > 10%
- Customer-visible outages
- Unexpected failure modes

---

### Phase 5: Deprecate gRPC (Future)

**Objective:** Remove gRPC code paths (18-24 months post-DRPC rollout)

**Prerequisites:**
- All supported cluster versions have DRPC
- At least 2 major releases with DRPC-only code

**Steps:**

1. **Announce deprecation**
   - Release notes: "gRPC support deprecated, will be removed in V27.1"

2. **Make DRPC default in V26.1**
   ```go
   var ExperimentalDRPCEnabled = settings.RegisterBoolSetting(
       settings.ApplicationLevel,
       "rpc.experimental_drpc.enabled",
       "...",
       true,  // Default changes to true
   )
   ```

3. **Remove gRPC in V27.1**
   - Delete gRPC server code
   - Remove gRPC client code
   - Simplify connection logic

**Timeline:** 18-24 months from Phase 4 completion

---

## 9. Compatibility Matrix

| Cluster State | Node A | Node B | A→B Connection | B→A Connection | Status |
|---------------|--------|--------|----------------|----------------|--------|
| Both old | gRPC only | gRPC only | gRPC | gRPC | ✅ Works |
| Mixed binary | gRPC only | gRPC+DRPC (disabled) | gRPC | gRPC | ✅ Works |
| Mixed binary | gRPC only | gRPC+DRPC (enabled) | gRPC | ❌ DRPC attempt fails | ❌ **BROKEN** |
| Same binary | gRPC+DRPC (disabled) | gRPC+DRPC (disabled) | gRPC | gRPC | ✅ Works |
| Same binary | gRPC+DRPC (disabled) | gRPC+DRPC (enabled) | gRPC | DRPC | ✅ Works (with version gate) |
| Same binary | gRPC+DRPC (enabled) | gRPC+DRPC (enabled) | DRPC | DRPC | ✅ Works |

**Key Insight:** Cluster version gate (Challenge 2 solution) prevents the broken scenario.

---

## 10. Monitoring and Observability

### Metrics to Track

**Connection Metrics:**
```
rpc.connection.grpc.active
rpc.connection.drpc.active
rpc.connection.grpc.created
rpc.connection.drpc.created
rpc.connection.grpc.failed
rpc.connection.drpc.failed
```

**Circuit Breaker Metrics:**
```
rpc.circuit_breaker.trips.grpc
rpc.circuit_breaker.trips.drpc
rpc.circuit_breaker.recoveries
```

**Latency Metrics:**
```
rpc.method.latency{protocol=grpc}
rpc.method.latency{protocol=drpc}
```

**Error Metrics:**
```
rpc.errors.total{protocol=grpc}
rpc.errors.total{protocol=drpc}
rpc.errors.by_code{code=unavailable,protocol=drpc}
```

### Alerts

**Critical:**
```
alert: RPCErrorRateHigh
expr: rate(rpc.errors.total[5m]) > 0.01
severity: page
```

**Warning:**
```
alert: CircuitBreakerThrashing
expr: rate(rpc.circuit_breaker.trips[5m]) > 10
severity: ticket
```

### Dashboards

**DRPC Migration Dashboard:**
1. Connection count by protocol (stacked area chart)
2. Error rate by protocol (line chart)
3. Latency comparison (side-by-side histograms)
4. Circuit breaker state (gauge)

---

## 11. Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|-------------|---------|------------|
| **Connection failures during transition** | Medium | High | Implement fallback (Challenge 3), connection TTL |
| **Long-running query failures** | High | Medium | Maintenance window, user notification |
| **Startup setting mismatch** | High | Critical | Fix startup timing (Challenge 1) |
| **Mixed binary DRPC attempt** | High | Critical | Cluster version gate (Challenge 2) |
| **Performance regression** | Low | High | Benchmark before rollout, canary testing |
| **Unknown DRPC bugs** | Medium | High | Progressive rollout, comprehensive testing |
| **Rollback complications** | Low | High | Clear rollback procedures, test in dev |

**Overall Risk Level:** Medium-High (before mitigations), Low-Medium (after mitigations)

---

## 12. Success Criteria

**Technical:**
- [ ] All code changes implemented and tested
- [ ] Roachtests passing in mixed-mode scenarios
- [ ] Performance parity or improvement vs gRPC
- [ ] Zero data corruption incidents
- [ ] Connection success rate > 99.99%

**Operational:**
- [ ] Zero customer-visible outages during migration
- [ ] RPC error rate < 0.01%
- [ ] P99 latency within 5% of baseline
- [ ] Successful rollout to 100% of clusters
- [ ] Runbook validated and documented

**Business:**
- [ ] Migration completed within planned timeline
- [ ] No escalations to engineering leadership
- [ ] Positive or neutral customer feedback
- [ ] Knowledge transfer to support teams complete

---

## 13. Conclusion

**DRPC migration is technically feasible but requires significant preparation work.**

### Key Findings:

1. ✅ **Infrastructure largely ready:** Both gRPC and DRPC servers run concurrently, most components have DRPC support

2. ❌ **Critical gaps exist:**
   - Startup timing issue prevents using persisted cluster setting
   - No binary version check before DRPC attempts
   - No fallback mechanism if DRPC fails
   - No connection draining mechanism

3. ⚠️ **Dynamic switching is unsafe:** Node restart required to change RPC protocol

4. ✅ **Safe migration path exists:** With code changes and proper rollout plan

### Recommended Path Forward:

**Immediate (Next 1-2 Releases):**
1. Implement code fixes for Challenges 1-4
2. Add comprehensive testing (unit, integration, roachtest)
3. Create detailed runbooks for rollout and rollback

**Near-term (Release V25.1):**
1. Ship DRPC-capable binaries (disabled by default)
2. Validate in production with real traffic
3. Gather performance data

**Medium-term (Release V25.2+):**
1. Progressive rollout using recommended plan
2. Monitor closely, iterate based on feedback
3. Build confidence over multiple releases

**Long-term (V26-27):**
1. Make DRPC default
2. Deprecate gRPC
3. Remove legacy code

### Estimated Timeline:
- **Code changes:** 6-12 weeks
- **Testing and validation:** 4-6 weeks
- **Production rollout:** 12-16 weeks (progressive)
- **Total:** 6-9 months to 100% DRPC

**This migration is a major infrastructure change requiring careful execution, but the benefits of a simpler, more performant RPC layer justify the investment.**

---

## Appendix A: Code References

All file paths relative to `/Users/chandrat/go/src/github.com/cockroachdb/cockroach/`

### Startup Sequence
- `pkg/server/server.go:247` - NewServer()
- `pkg/server/server.go:361` - rpc.NewContext()
- `pkg/server/server.go:1535` - PreStart()
- `pkg/server/server.go:1636` - clusterversion.Initialize()
- `pkg/server/server.go:1938` - gossip.Start() (first RPC)

### DRPC Configuration
- `pkg/rpc/rpcbase/nodedialer.go:19` - Environment variable
- `pkg/rpc/rpcbase/nodedialer.go:24-28` - Cluster setting definition
- `pkg/rpc/rpcbase/nodedialer.go:104-106` - DRPCEnabled() check

### Connection Management
- `pkg/rpc/nodedialer/nodedialer.go:88-101` - Dial()
- `pkg/rpc/nodedialer/nodedialer.go:103-117` - DRPCDial()
- `pkg/rpc/nodedialer/nodedialer.go:157-200` - DialInternalClient()
- `pkg/rpc/connection.go:69-90` - Connection struct
- `pkg/rpc/drpc.go:43-78` - DialDRPC()

### Server Setup
- `pkg/server/server.go:412-420` - Server creation
- `pkg/server/drpc_server.go:39` - NewDRPCServer()
- `pkg/rpc/drpc.go:269-351` - NewDRPCServer() implementation

### Circuit Breaker
- `pkg/util/circuit/circuitbreaker.go` - Breaker implementation
- `pkg/rpc/context.go:1533-1545` - GetBreakerForAddr()

### Retry Logic
- `pkg/kv/txn.go` - Transaction retry
- `pkg/util/stop/stopper.go` - Graceful shutdown

---

## Appendix B: Open Questions

1. **DRPC performance characteristics:** Need production benchmarks
2. **Memory usage comparison:** gRPC vs DRPC under load
3. **Compression support:** DRPC TODOs mention missing compression (pkg/rpc/drpc.go:175)
4. **Dial timeout support:** DRPC TODOs mention missing dial timeout (pkg/rpc/drpc.go:176)
5. **HTTP/2 compatibility:** Any proxies or load balancers that need updates?

---

**Document Version:** 1.0
**Date:** 2025-12-12
**Author:** Technical Analysis based on CockroachDB master branch (commit 433195fd486)
