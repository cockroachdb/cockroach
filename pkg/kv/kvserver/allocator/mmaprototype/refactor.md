# Refactoring Opportunities in `cluster_state_repair.go`

The file originally contained ~2275 lines split across 13 repair functions.
Most of the bulk came from a handful of structural patterns repeated verbatim
across nearly every function. Patterns 1–5 have been extracted; Patterns 6–7
remain as future opportunities.

---

## Completed Extractions

- **Pattern 1** (`replicaStateForStore`): Find ReplicaState for a known
  StoreID. Extracted to standalone function, replacing 12 lookup sites across
  11 functions.
- **Pattern 2** (`filterAddCandidates`): Build valid add-candidate stores.
  Extracted to `rebalanceEnv` method, replacing 8 call sites.
- **Pattern 3** (`pickBestRemovalCandidate`): Priority bucket + diversity pick
  for removal. Extracted to `rebalanceEnv` method, replacing 3 call sites.
- **Pattern 4** (`excludeLeaseholder`): Leaseholder exclusion. Replaced 3
  `goto`-based loops with a standalone function.
- **Pattern 5** (`enactRepair`): Commit boilerplate. Extracted to
  `rebalanceEnv` method, replacing 15 call sites.

---

## Remaining Opportunities

### Pattern 6: The four Replace functions (voter/non-voter × dead/decommissioning)

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

With patterns 1–5 extracted, each function is ~50 lines. A merged helper
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
appropriate spec. This is the highest-value remaining refactor: ~200 lines
become ~100.

---

### Pattern 7: Add voter vs Add non-voter / Remove voter vs Remove non-voter

`repairAddVoter` and `repairAddNonVoter` are parallel but not identical:
- AddVoter tries `candidatesToConvertFromNonVoterToVoter` first (promote).
- AddNonVoter tries `candidatesToConvertFromVoterToNonVoter` first (demote).

The "try to convert an existing replica first, otherwise find a new store"
skeleton is the same, but the sub-helpers (`promoteNonVoterToVoter` vs
`demoteVoterToNonVoter`) have their own non-trivial logic. Similarly for the
Remove pair. Merging these is feasible but yields less obvious savings and
requires parametrizing the conversion sub-path. **Recommended as a follow-up**
after the Replace functions (Pattern 6) are merged.
