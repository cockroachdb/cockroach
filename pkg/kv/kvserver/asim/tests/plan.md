# Plan: Production fix for MMA finalize-atomic-replication changes

## Context

The `mmra` branch adds a new `LBRebalancingMultiMetricRepairAndRebalance` mode
under which MMA handles both rebalancing and repair exclusively (replicate and
lease queues fully disabled). The recent commits on this branch (see `git log`
for details) added the mode enum, repair metrics, the `IncludeRepair` option
that wires MMA's `repair()` into `ComputeChanges`, queue disabling, ASIM
integration, and 15 ASIM repair tests covering every repair scenario.

All 15 ASIM repair tests pass, including `repair_finalize_atomic` (which
required a fix to ASIM's `change.go` — commit `d9dd6810eb4` — to handle
same-store voter type transitions). Now we need to fix the **production** code
path so that MMA-generated finalize-atomic-replication changes work when applied
through the real kvserver.

### The problem

MMA's `repairFinalizeAtomicReplicationChange` (`cluster_state_repair.go:1250`)
generates changes to finalize joint configs:
- VOTER_INCOMING → VOTER_FULL (`ChangeReplica`)
- VOTER_DEMOTING_LEARNER → removed (`RemoveReplica`)
- VOTER_DEMOTING_NON_VOTER → NON_VOTER (`ChangeReplica`)

When `ReplicationChanges()` (`range_change.go:170`) converts these to kvpb
format, a VOTER_INCOMING→VOTER_FULL transition becomes `{ADD_VOTER, REMOVE_VOTER}`
on the **same store** (because `mapReplicaTypeToVoterOrNonVoter` maps
VOTER_INCOMING → VOTER_FULL for the removal side).

In production, `changeReplicasImpl` (`replica_command.go:1220`):
1. Calls `maybeLeaveAtomicChangeReplicas(ctx, desc)` — this **silently finalizes
   the joint config** via `execChangeReplicasTxn` with nil iChgs.
2. Calls `validateReplicationChanges(desc, chgs)` — this **fails** because
   `validatePromotionsAndDemotions` (line 1730-1738) only allows
   `{ADD_VOTER, REMOVE_NON_VOTER}` (promotion) or `{ADD_NON_VOTER, REMOVE_VOTER}`
   (demotion), and rejects `{ADD_VOTER, REMOVE_VOTER}`.

Result: the range actually gets fixed (step 1), but MMA sees a failure (step 2)
and records a failed repair. MMA will retry the repair on the next tick, see the
range is no longer in a joint config, and stop. Functionally correct but noisy
and wrong in accounting.

### The fix

Route leave-joint changes through the production leave-joint path
(`maybeLeaveAtomicChangeReplicas`) directly, bypassing `ReplicationChanges()`
and `changeReplicasImpl`.

---

## Commit: Handle leave-joint changes in MMA store rebalancer

**Files:**
- `pkg/kv/kvserver/allocator/mmaprototype/range_change.go` — add `IsLeaveJoint()`
- `pkg/kv/kvserver/mma_store_rebalancer.go` — add interface method + dispatch case

### 1. Add `IsLeaveJoint()` to `ExternalRangeChange` (`range_change.go`)

Add after `IsChangeReplicas()` (~line 127):

```go
// IsLeaveJoint returns true if this change exclusively finalizes a joint
// configuration. All changes must transition replicas from joint-config types
// (VOTER_INCOMING, VOTER_DEMOTING_LEARNER, VOTER_DEMOTING_NON_VOTER) to
// their finalized forms.
//
// These changes cannot be expressed as kvpb.ReplicationChanges because the
// production code uses a fundamentally different mechanism to leave joint
// configs (execChangeReplicasTxn with nil internal changes). Callers must
// detect this case and use the leave-joint path directly.
func (rc *ExternalRangeChange) IsLeaveJoint() bool {
	if len(rc.Changes) == 0 {
		return false
	}
	for _, c := range rc.Changes {
		switch c.Prev.ReplicaType.ReplicaType {
		case roachpb.VOTER_INCOMING, roachpb.VOTER_DEMOTING_LEARNER,
			roachpb.VOTER_DEMOTING_NON_VOTER:
			// OK — this is a joint-config type being finalized.
		default:
			return false
		}
	}
	return true
}
```

### 2. Add `maybeLeaveAtomicChangeReplicas` to interface (`mma_store_rebalancer.go`)

Add to `replicaToApplyChanges` (line 26):

```go
maybeLeaveAtomicChangeReplicas(
	ctx context.Context, desc *roachpb.RangeDescriptor,
) (*roachpb.RangeDescriptor, error)
```

`*Replica` already implements this method (`replica_command.go:1473`), so no
additional implementation is needed.

### 3. Add dispatch case in `applyChange` (`mma_store_rebalancer.go`)

In `applyChange` (~line 170), add a new case **before** the existing
`IsChangeReplicas()` case (since `IsLeaveJoint()` changes also satisfy
`IsChangeReplicas()`):

```go
case change.IsLeaveJoint():
	_, err = repl.maybeLeaveAtomicChangeReplicas(ctx, repl.Desc())
```

### ASIM impact

No ASIM changes needed. The ASIM path already works because commit `d9dd6810eb4`
added same-store voter type transition handling to `change.go`. The ASIM
integration dispatches leave-joint changes as `NewChangeReplicasOp` via
`ReplicationChanges()`, and `change.go` handles the resulting `{ADD_VOTER,
REMOVE_VOTER}` pairs correctly by detecting same-store entries and applying them
as type transitions via `promoDemo`.

---

## Verification

```bash
# Build check (confirms interface satisfaction)
./dev build pkg/kv/kvserver:kvserver_test

# MMA prototype tests
./dev test pkg/kv/kvserver/allocator/mmaprototype -v

# ASIM tests (should still pass — no ASIM changes)
./dev test pkg/kv/kvserver/asim/tests -v -f TestDataDriven -- \
  --test_env COCKROACH_RUN_ASIM_TESTS=true
```
