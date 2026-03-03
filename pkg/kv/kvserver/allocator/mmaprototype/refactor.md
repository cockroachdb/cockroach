# Refactoring Opportunities in `cluster_state_repair.go`

The file originally contained ~2275 lines split across 13 repair functions.
Most of the bulk came from a handful of structural patterns repeated verbatim
across nearly every function. Patterns 1–6 have been extracted; Pattern 7
remains as a future opportunity.

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
- **Pattern 6** (`repairReplaceReplica`): Merged the four Replace functions
  (dead/decommissioning × voter/non-voter) into a single parametrized
  `repairReplaceReplica` method driven by a `replaceReplicaSpec` struct.
  The four public entry points are now thin wrappers.

---

## Remaining Opportunities

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
