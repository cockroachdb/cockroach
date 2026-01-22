# DRPC Connection Error Handling - Root Cause Analysis

## Overview

This document explains the connection error handling flow in CockroachDB's RPC layer and why DRPC required a manual fix to match gRPC's behavior for detecting unavailable connections.

## The Problem

When attempting to connect to a decommissioned/stopped node:
- **gRPC**: Test passes in 4.8s - correctly detects the node is unavailable and stops retrying
- **DRPC (before fix)**: Test times out after 54.4s - keeps retrying indefinitely

## Root Cause

The gRPC framework automatically converts connection errors (EOF) to `codes.Unavailable`, while DRPC returns plain errors that default to `codes.Unknown`. The `grpcutil.IsConnectionUnavailable()` function only recognizes `codes.Unavailable` and `codes.FailedPrecondition`, so DRPC errors were not detected as connection failures.

## Connection Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│ Call Flow: Connection Heartbeat Error Handling                     │
└─────────────────────────────────────────────────────────────────────┘

1. loqrecovery/server.go:visitNodeWithRetry()
   │
   ├─→ serverpb.DialAdminClientNoBreaker(nd, ctx, node.NodeID)
       │
       ├─→ nodedialer/nodedialer.go:DRPCDialNoBreaker()  [or Dial() for gRPC]
           │
           ├─→ dial() → connection.Connect()
               │
               ├─→ peer.go:runSingleHeartbeat()
                   │
                   ├─→ heartbeatClient.Ping(ctx, request)  ← peer.go:479
                       │
                       ├─────────────────┬──────────────────
                       │                 │
                       ▼ gRPC            ▼ DRPC (before fix)
                       │                 │
                   EOF from network  EOF from network
                       │                 │
                   gRPC transport    DRPC returns
                   auto-converts:    plain error
                   ↓                     ↓
              codes.Unavailable     (no status code)
              [code 14]                 │
                       │                 │
                       ├─────────────────┘
                       │
                       ▼
               peer.go:onHeartbeatFailed() wraps in:
               InitialHeartbeatFailedError{WrappedErr: err}
                       │
                       ▼
               Returns to DRPCDialNoBreaker()
                       │
                       ├────────────────┬─────────────────
                       │                │
                   gRPC Path        DRPC (before fix)
                       │                │
               errors.Wrapf()      errors.Wrapf()
               preserves           loses status:
               codes.Unavailable   → codes.Unknown
                       │            [code 2]
                       │                │
                       ├────────────────┘
                       ▼
           grpcutil.IsConnectionUnavailable(err) checks:
           status.FromError(err).Code() == codes.Unavailable?
                       │
                       ├─────────────────┬──────────────────
                       │                 │
                   gRPC: TRUE        DRPC (before): FALSE
                   → Skip retry      → Keep retrying (BUG!)
                       │                 │
                   Test passes       Test hangs/times out
                   (4.8s)            (54.4s)
```

## Detailed Code Path

### 1. Initial Call (loqrecovery/server.go:759-796)

```go
func visitNodeWithRetry(...) error {
    for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
        ac, err = serverpb.DialAdminClientNoBreaker(nd, ctx, node.NodeID)
        if err != nil {
            if grpcutil.IsConnectionUnavailable(err) {  // ← Key check!
                log.KvExec.Infof(ctx, "rejecting node n%d because of suspected un-retryable error: %s",
                    node.NodeID, err)
                return nil  // Skip this node
            }
            continue  // Retry
        }
        // ... use the client
    }
}
```

### 2. Dialing (nodedialer/nodedialer.go:141-158)

**DRPC Path (before fix):**
```go
func (n *Dialer) DRPCDialNoBreaker(...) (drpc.Conn, error) {
    dc, _, err := dial(ctx, n.resolver, n.rpcContext.DRPCDialNode, nodeID, class, false)
    if err != nil {
        // Error has InitialHeartbeatFailedError{WrappedErr: EOF}
        // But no gRPC status code!
        return nil, errors.Wrapf(err, "DRPC")
    }
    return dc, err
}
```

**gRPC Path:**
```go
func (n *Dialer) DialNoBreaker(...) (*grpc.ClientConn, err error) {
    gc, _, err := dial(ctx, n.resolver, n.rpcContext.GRPCDialNode, nodeID, class, false)
    if err != nil {
        // Error already has codes.Unavailable from gRPC transport!
        return nil, errors.Wrapf(err, "gRPC")
    }
    return gc, err
}
```

### 3. Heartbeat Execution (peer.go:430-492)

```go
func runSingleHeartbeat(...) error {
    // ...
    response, err = heartbeatClient.Ping(ctx, request)  // ← Line 479
    // For gRPC: err is already status.Error(codes.Unavailable, "...")
    // For DRPC: err is plain error (EOF)

    if err != nil {
        log.VEventf(ctx, 2, "received error on ping response from n%d, %v", k.NodeID, err)
        return err
    }
    // ...
}
```

### 4. Heartbeat Failure Handling (peer.go:702-739)

```go
func (p *peer[Conn]) onHeartbeatFailed(...) {
    // ...
    if !ls.c.connFuture.Resolved() {
        // Wrap the error in InitialHeartbeatFailedError
        err = &netutil.InitialHeartbeatFailedError{WrappedErr: err}
        var nilConn Conn
        ls.c.connFuture.Resolve(nilConn, err)
    }
    // ...
}
```

### 5. Connection Unavailability Check (util/grpcutil/grpc_util.go:64-69)

```go
func IsConnectionUnavailable(err error) bool {
    if s, ok := status.FromError(errors.UnwrapAll(err)); ok {
        return s.Code() == codes.Unavailable || s.Code() == codes.FailedPrecondition
    }
    return false
}
```

## Error Log Comparison

### gRPC (test.log, line 471)
```
initial connection heartbeat failed: grpc: connection error: desc = "transport:
authentication handshake failed: EOF" [code 14/Unavailable]
```
**Result**: `IsConnectionUnavailable()` returns `true` → retry loop exits

### DRPC Before Fix (test_1.log, line 583)
```
initial connection heartbeat failed: EOF; EOF [code 2/Unknown]
```
**Result**: `IsConnectionUnavailable()` returns `false` → retry loop continues

### DRPC After Fix (test_fix.log)
```
--- PASS: TestNodeDecommissioned (4.23s)
```
**Result**: Behaves like gRPC - correctly detects unavailability

## The Fix

### Location: pkg/rpc/nodedialer/nodedialer.go

Applied to both `DRPCDial()` and `DRPCDialNoBreaker()`:

```go
func (n *Dialer) DRPCDialNoBreaker(
    ctx context.Context, nodeID roachpb.NodeID, class rpcbase.ConnectionClass,
) (drpc.Conn, error) {
    if n == nil || n.resolver == nil {
        return nil, errors.New("no node dialer configured")
    }
    dc, _, err := dial(ctx, n.resolver, n.rpcContext.DRPCDialNode, nodeID, class, false)
    if err != nil {
        // *** THE FIX ***
        // If this is an InitialHeartbeatFailedError, convert it to a gRPC status
        // error with codes.Unavailable so that grpcutil.IsConnectionUnavailable
        // can properly detect that the connection is unavailable.
        if errors.HasType(err, (*netutil.InitialHeartbeatFailedError)(nil)) {
            err = status.Error(codes.Unavailable, err.Error())
        }
        return nil, errors.Wrapf(err, "DRPC")
    }
    return dc, err
}
```

### Required Dependencies (BUILD.bazel)

```bazel
deps = [
    # ... existing deps ...
    "//pkg/util/netutil",           # ← Added for InitialHeartbeatFailedError
    "@org_golang_google_grpc//codes",   # ← Added for codes.Unavailable
    "@org_golang_google_grpc//status",  # ← Added for status.Error
]
```

## Why gRPC Doesn't Need This Fix

The **gRPC framework's transport layer** (in `google.golang.org/grpc/internal/transport`) automatically converts connection errors to status errors **before** returning from RPC calls:

1. When `heartbeatClient.Ping()` is called (peer.go:479)
2. The gRPC client stub routes the call through the HTTP/2 transport layer
3. If the transport encounters EOF, connection reset, etc., it automatically calls:
   ```go
   status.Error(codes.Unavailable, err.Error())
   ```
4. This happens **before** the error is returned to application code
5. By the time `onHeartbeatFailed()` wraps it in `InitialHeartbeatFailedError`, it already has the status code

DRPC doesn't have this automatic conversion, so we must do it manually at the boundary.

## Key Takeaway

**gRPC's automatic status code conversion** is a framework feature that happens in the transport layer. When migrating to DRPC, we need to **explicitly preserve status codes** at appropriate boundaries to maintain compatibility with code that depends on gRPC status codes for error handling logic.

## Related Files

- `pkg/rpc/nodedialer/nodedialer.go` - The fix location
- `pkg/rpc/peer.go:702-739` - Where InitialHeartbeatFailedError is created
- `pkg/rpc/peer.go:430-492` - Heartbeat execution
- `pkg/util/grpcutil/grpc_util.go:64-69` - Connection unavailability check
- `pkg/util/netutil/net.go:223-254` - InitialHeartbeatFailedError definition
- `pkg/kv/kvserver/loqrecovery/server.go:759-796` - Retry logic that depends on the check
