# Failure Injection in Roachtests

This document describes how to use the failure injection framework within roachtests
to simulate various failure scenarios like network partitions, disk stalls, and
process crashes.

## Overview

The failure injection framework provides a structured way to inject, validate, and
recover from failures during roachtests. It is built on two main components:

1. **FailureMode Interface**: Defines the contract for individual failure types
   (e.g., network partition, disk stall). Each failure mode implements methods for
   setup, injection, recovery, and cleanup.

2. **Failer Wrapper**: A state-machine wrapper around FailureMode that ensures
   methods are called in the correct order and handles error states gracefully.

### When to Use Failure Injection

Use failure injection when you want to test:
- Cluster behavior during network partitions between nodes
- Database resilience to disk I/O stalls or failures
- Recovery behavior after node crashes or restarts
- Replication and rebalancing under failure conditions

## Failure Lifecycle

Every failure injection follows this lifecycle:

```
Setup() -> Inject() -> [WaitForFailureToPropagate()] -> Recover() -> [WaitForFailureToRecover()] -> Cleanup()
```

1. **Setup()**: Prepare any dependencies required for the failure (e.g., install tools,
   configure cgroups). Called once before any injections.

2. **Inject()**: Actually inject the failure into the system.

3. **WaitForFailureToPropagate()** *(optional)*: Block until the failure has taken full
   effect (e.g., node is marked as dead, replicas have moved). Use this when you need
   to ensure the cluster has reacted to the failure before proceeding.

4. **Recover()**: Reverse the effects of Inject() (e.g., remove iptables rules,
   restart nodes).

5. **WaitForFailureToRecover()** *(optional)*: Block until the cluster has fully
   recovered (e.g., replicas are balanced, nodes are healthy). Use this when you need
   to verify the cluster is stable before continuing with other operations.

6. **Cleanup()**: Uninstall any dependencies installed during Setup().

## Available Failure Modes

| Failure Name | Constant | Description |
|-------------|----------|-------------|
| Network Partition | `failures.IPTablesNetworkPartitionName` | Block traffic between nodes using iptables |
| Network Latency | `failures.NetworkLatencyName` | Add artificial latency between nodes using tc |
| CGroup Disk Stall | `failures.CgroupsDiskStallName` | Stall disk I/O using cgroups v2 throttling |
| Dmsetup Disk Stall | `failures.DmsetupDiskStallName` | Stall disk I/O using dmsetup suspend |
| Process Kill | `failures.ProcessKillFailureName` | Kill CockroachDB processes (graceful or forced) |
| VM Reset | `failures.ResetVMFailureName` | Simulate VM reboot |

## API Reference

### Helper Methods (Recommended)

The `roachtestutil` package provides convenient helper methods that create pre-configured
Failers with the appropriate args. These reduce boilerplate and make tests easier to read.

#### Network Partition Helpers

```go
// Bidirectional partition (drops traffic both ways)
failer, args, err := roachtestutil.MakeBidirectionalPartitionFailer(
    t.L(), c, c.Node(1), c.Node(2))

// Incoming partition (drops incoming traffic on source from destination)
failer, args, err := roachtestutil.MakeIncomingPartitionFailer(
    t.L(), c, c.Node(1), c.Node(2))

// Outgoing partition (drops outgoing traffic from source to destination)
failer, args, err := roachtestutil.MakeOutgoingPartitionFailer(
    t.L(), c, c.Node(1), c.Node(2))

// Custom partition type
failer, args, err := roachtestutil.MakeNetworkPartitionFailer(
    t.L(), c, c.Node(1), c.Node(2), failures.Bidirectional)
```

#### Network Latency Helpers

```go
failer, args, err := roachtestutil.MakeNetworkLatencyFailer(
    t.L(), c, c.Node(1), c.Node(2), 100*time.Millisecond)
```

#### Process Kill Helpers

```go
// Hard kill (SIGKILL, immediate)
failer, args, err := roachtestutil.MakeProcessKillFailer(
    t.L(), c, c.Node(1), false /* graceful */, 0)

// Graceful kill (SIGTERM + drain, then SIGKILL after grace period)
failer, args, err := roachtestutil.MakeProcessKillFailer(
    t.L(), c, c.Node(1), true /* graceful */, 5*time.Minute)
```

#### VM Reset Helpers

```go
failer, args, err := roachtestutil.MakeVMResetFailer(
    t.L(), c, c.Node(1), true /* stopProcesses */)
```

#### Disk Stall Helpers

```go
// Cgroup disk stall (uses cgroups v2 I/O throttling)
failer, args, err := roachtestutil.MakeCgroupDiskStallFailer(
    t.L(), c, c.Node(1),
    true,  // stallWrites
    false, // stallReads
    false, // stallLogs
)

// Dmsetup disk stall (uses dmsetup suspend)
failer, args, err := roachtestutil.MakeDmsetupDiskStallFailer(t.L(), c, c.Node(1), failer /*disableStateValidation*/)
```

## Usage Pattern

The basic pattern for using failure injection in a roachtest:

```go
// 1. Start the cluster first (required for certificate handling in secure mode)
c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.CRDBNodes())

// 2. Create the failer using a helper method (AFTER c.Start)
failer, args, err := roachtestutil.MakeBidirectionalPartitionFailer(
    t.L(), c, c.Node(1), c.Node(2))
if err != nil {
    t.Fatal(err)
}

// 3. Always defer cleanup
defer func() {
    if err := failer.Cleanup(context.Background(), t.L()); err != nil {
        t.L().Printf("cleanup failed: %v", err)
    }
}()

// 4. Setup the failure mode
if err := failer.Setup(ctx, t.L(), args); err != nil {
    t.Fatal(err)
}

// 5. Run your workload or test setup here...

// 6. Inject the failure
if err := failer.Inject(ctx, t.L(), args); err != nil {
    t.Fatal(err)
}

// 7. (Optional) Wait for failure to take effect.
// Use this when you need to ensure the cluster has reacted to the failure
// (e.g., node marked dead, replicas moved) before proceeding.
if err := failer.WaitForFailureToPropagate(ctx, t.L()); err != nil {
    t.Fatal(err)
}

// 8. Test behavior under failure...

// 9. Recover from the failure
if err := failer.Recover(ctx, t.L()); err != nil {
    t.Fatal(err)
}

// 10. (Optional) Wait for cluster to stabilize.
// Use this when you need to verify the cluster is stable (e.g., replicas
// rebalanced, nodes healthy) before continuing with other operations.
if err := failer.WaitForFailureToRecover(ctx, t.L()); err != nil {
    t.Fatal(err)
}
```

**Important:** The failer must be created AFTER `c.Start()` completes. This is because
`c.Start()` downloads SSL certificates for secure clusters, and `c.GetFailer()` needs
access to those certificates.

For complete working examples, see:
- `runNetworkPartitionExample` in `pkg/cmd/roachtest/tests/failure_injection.go`
- `runDiskStallExample` in `pkg/cmd/roachtest/tests/failure_injection.go`


## See Also

- `pkg/roachprod/failureinjection/failures/` - Failure mode implementations
- `pkg/cmd/roachtest/roachtestutil/failure_injection.go` - Helper methods for creating Failers
- `pkg/cmd/roachtest/tests/failure_injection.go` - Smoke tests for all failure modes
