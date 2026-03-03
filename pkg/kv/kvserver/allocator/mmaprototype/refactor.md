# Refactoring Opportunities in `cluster_state_repair.go`

The file contains ~2275 lines split across 13 repair functions. Most of the
bulk comes from a handful of structural patterns repeated verbatim across
nearly every function. The sections below identify the patterns, quantify
their prevalence, and suggest targeted extractions that reduce noise without
losing clarity.

---

## Pattern 1: Find ReplicaState for a known StoreID (~9 sites)

Every repair function that removes or type-changes a replica needs to look up
its `ReplicaState` from `rs.replicas`. The code is always:

```go
var prevState ReplicaState
found := false
for _, repl := range rs.replicas {
    if repl.StoreID == storeID {
        prevState = repl.ReplicaState
        found = true
        break
    }
}
if !found {
    log.KvDistribution.Warningf(ctx, "skipping X repair for r%d: ... s%d not found in replicas",
        rangeID, storeID)
    return
}
```

**Extract:**

```go
// replicaStateForStore returns the ReplicaState of the replica on the given
// store, and whether it was found.
func replicaStateForStore(rs *rangeState, storeID roachpb.StoreID) (ReplicaState, bool) {
    for _, repl := range rs.replicas {
        if repl.StoreID == storeID {
            return repl.ReplicaState, true
        }
    }
    return ReplicaState{}, false
}
```

Each call site collapses from ~8 lines to 2.

---

## Pattern 2: Build valid add-candidate stores (8 sites)

Every repair function that adds a replica (AddVoter, AddNonVoter,
ReplaceDeadVoter, ReplaceDeadNonVoter, ReplaceDecommissioningVoter,
ReplaceDecommissioningNonVoter, SwapVoterForConstraints,
SwapNonVoterForConstraints) repeats the same ~20-line block:

```go
var existingReplicas storeSet
existingNodes := make(map[roachpb.NodeID]struct{})
for _, repl := range rs.replicas {
    if repl.StoreID == excludeStoreID { // sometimes absent
        continue
    }
    existingReplicas.insert(repl.StoreID)
    ss := re.stores[repl.StoreID]
    if ss != nil {
        existingNodes[ss.NodeID] = struct{}{}
    }
}

candidateStores = retainReadyReplicaTargetStoresOnly(
    ctx, candidateStores, re.stores, existingReplicas)

var validCandidates storeSet
for _, storeID := range candidateStores {
    if existingReplicas.contains(storeID) { continue }
    ss := re.stores[storeID]
    if ss == nil { continue }
    if _, ok := existingNodes[ss.NodeID]; ok { continue }
    validCandidates = append(validCandidates, storeID)
}
```

**Extract:**

```go
// filterAddCandidates filters candidateStores down to stores that are ready
// (not dead/draining/IO-overloaded) and not already hosting a replica for the
// range at the node level. excludeStoreID, if non-zero, is excluded from the
// existing-replica set (used when a replica on that store is being
// concurrently removed as part of the same change).
func (re *rebalanceEnv) filterAddCandidates(
    ctx context.Context,
    rs *rangeState,
    candidateStores storeSet,
    excludeStoreID roachpb.StoreID,
) storeSet {
    var existingReplicas storeSet
    existingNodes := make(map[roachpb.NodeID]struct{})
    for _, repl := range rs.replicas {
        if repl.StoreID == excludeStoreID {
            continue
        }
        existingReplicas.insert(repl.StoreID)
        ss := re.stores[repl.StoreID]
        if ss != nil {
            existingNodes[ss.NodeID] = struct{}{}
        }
    }
    candidateStores = retainReadyReplicaTargetStoresOnly(
        ctx, candidateStores, re.stores, existingReplicas)
    var valid storeSet
    for _, storeID := range candidateStores {
        if existingReplicas.contains(storeID) {
            continue
        }
        ss := re.stores[storeID]
        if ss == nil {
            continue
        }
        if _, ok := existingNodes[ss.NodeID]; ok {
            continue
        }
        valid = append(valid, storeID)
    }
    return valid
}
```

Call sites that don't exclude any store pass `0` for `excludeStoreID`. This
removes ~160 lines of boilerplate.

---

## Pattern 3: Priority bucket + diversity pick for removal (3 sites)

`repairRemoveVoter`, `repairRemoveNonVoter`, and `repairRemoveLearner` all
implement the same two-pass removal selection: bucket candidates by
`removalPriority`, isolate the worst-health bucket, then pick within the
bucket by diversity score. This is ~15 lines of identical logic each time.

**Extract:**

```go
// pickBestRemovalCandidate buckets candidates by removalPriority (lowest
// value = worst health = remove first), then within the worst bucket picks
// the store whose removal hurts diversity the least. localityTiers controls
// which replicas' localities are used for scoring.
func (re *rebalanceEnv) pickBestRemovalCandidate(
    candidates []roachpb.StoreID,
    localityTiers replicasLocalityTiers,
) roachpb.StoreID {
    bestPriority := math.MaxInt
    for _, storeID := range candidates {
        if p := removalPriority(re.stores[storeID]); p < bestPriority {
            bestPriority = p
        }
    }
    var bucket []roachpb.StoreID
    for _, storeID := range candidates {
        if removalPriority(re.stores[storeID]) == bestPriority {
            bucket = append(bucket, storeID)
        }
    }
    return re.pickStoreByDiversity(
        bucket, localityTiers,
        (*existingReplicaLocalities).getScoreChangeForReplicaRemoval)
}
```

Each call site collapses from ~15 lines to 3.

---

## Pattern 4: Leaseholder exclusion via `goto` (3 sites)

Excluding the leaseholder from a candidate list is done three times using a
`goto` idiom that is slightly unusual Go style:

```go
for _, storeID := range candidates {
    for _, repl := range rs.replicas {
        if repl.StoreID == storeID && repl.IsLeaseholder {
            goto skipLabel
        }
    }
    filtered = append(filtered, storeID)
skipLabel:
}
```

**Extract:**

```go
// excludeLeaseholder returns candidates with the leaseholder store removed.
func excludeLeaseholder(rs *rangeState, candidates []roachpb.StoreID) []roachpb.StoreID {
    var leaseholderStore roachpb.StoreID
    for _, repl := range rs.replicas {
        if repl.IsLeaseholder {
            leaseholderStore = repl.StoreID
            break
        }
    }
    if leaseholderStore == 0 {
        return candidates
    }
    var filtered []roachpb.StoreID
    for _, storeID := range candidates {
        if storeID != leaseholderStore {
            filtered = append(filtered, storeID)
        }
    }
    return filtered
}
```

Cleaner than `goto`, and easier to follow in context.

---

## Pattern 5: Commit boilerplate (13 sites)

Every repair function ends with the same two statements before its log line:

```go
re.addPendingRangeChange(ctx, rangeChange)
re.changes = append(re.changes,
    MakeExternalRangeChange(originMMARepair, localStoreID, rangeChange))
```

**Extract:**

```go
// enactRepair records a repair change as pending and appends it to the
// changes list for external delivery. The caller is responsible for logging
// the success message.
func (re *rebalanceEnv) enactRepair(
    ctx context.Context, localStoreID roachpb.StoreID, rangeChange PendingRangeChange,
) {
    re.addPendingRangeChange(ctx, rangeChange)
    re.changes = append(re.changes,
        MakeExternalRangeChange(originMMARepair, localStoreID, rangeChange))
}
```

The log line stays at the call site since the message content differs per
action. This removes 13 duplicated `append` lines (minor but consistent).

---

## Pattern 6: The four Replace functions (voter/non-voter × dead/decommissioning)

`repairReplaceDeadVoter`, `repairReplaceDecommissioningVoter`,
`repairReplaceDeadNonVoter`, and `repairReplaceDecommissioningNonVoter`
are structurally identical. The comments even say so: "Identical to X but
targets stores with Y instead of Z." The only differences are:

| Dimension            | Voter variant             | Non-voter variant           |
|----------------------|---------------------------|-----------------------------|
| Replica kind filter  | `isVoter`                 | `isNonVoter`                |
| Removal condition    | `HealthDead` or `ReplicaDispositionShedding` | same, applied to non-voters |
| Leaseholder exclude  | yes                       | no                          |
| Locality tiers       | `voterLocalityTiers`      | `replicaLocalityTiers`      |
| Add type             | `VOTER_FULL`              | `NON_VOTER`                 |

With patterns 1–5 extracted first, each function is ~50 lines. A merged helper
`repairReplaceReplica` with a small parameter struct collapses all four into
one ~80-line function:

```go
type replaceReplicaSpec struct {
    actionName      string
    isTargetKind    func(roachpb.ReplicaType) bool
    isTargetStore   func(*storeState) bool // dead or decommissioning?
    excludeLeasehdr bool
    localityTiers   func(*constraintAnalysis) replicasLocalityTiers
    addReplicaType  roachpb.ReplicaType
}
```

The four public entry points become trivial one-line wrappers that pass the
appropriate spec. This is the highest-value individual refactor: ~300 lines
become ~100.

---

## Pattern 7: Add voter vs Add non-voter / Remove voter vs Remove non-voter

`repairAddVoter` and `repairAddNonVoter` are parallel but not identical:
- AddVoter tries `candidatesToConvertFromNonVoterToVoter` first (promote).
- AddNonVoter tries `candidatesToConvertFromVoterToNonVoter` first (demote).

The "try to convert an existing replica first, otherwise find a new store"
skeleton is the same, but the sub-helpers (`promoteNonVoterToVoter` vs
`demoteVoterToNonVoter`) have their own non-trivial logic. Similarly for the
Remove pair. Merging these is feasible but yields less obvious savings and
requires parametrizing the conversion sub-path. **Recommended as a follow-up**
after the simpler patterns above are extracted.

---

## Summary of Expected LOC Reduction

| Extraction                              | Approx. lines saved |
|-----------------------------------------|---------------------|
| `replicaStateForStore`                  | ~65                 |
| `filterAddCandidates`                   | ~140                |
| `pickBestRemovalCandidate`              | ~35                 |
| `excludeLeaseholder`                    | ~20                 |
| `enactRepair`                           | ~15                 |
| Merge 4 replace functions               | ~300                |
| **Total (conservative)**                | **~575 lines**      |

The file currently has ~2275 lines. The extractions above would reduce it to
roughly ~1700 lines — a 25% reduction — with no loss of readability and a gain
in clarity: each repair function's distinctive logic stands out rather than
being buried in structural boilerplate.

The more aggressive mergers (Add, Remove) could bring it to ~1400 lines.