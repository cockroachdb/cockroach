# Refactoring Opportunities in `cluster_state_repair.go`

The file originally contained ~2275 lines split across 13 repair functions.
Most of the bulk came from a handful of structural patterns repeated verbatim
across nearly every function. Patterns 1–6 have been extracted.

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

## Considered and Rejected

### Pattern 7: Add voter vs Add non-voter / Remove voter vs Remove non-voter

The Add pair shares a "try conversion first, then find a new store" skeleton,
but the conversion sub-paths differ meaningfully: `promoteNonVoterToVoter` vs
`demoteVoterToNonVoter` (different leaseholder handling, different diversity
tiers, opposite type changes). Merging would require a spec struct with ~8
fields including function-typed fields for conversion candidate selection and
the conversion helper itself. The indirection costs more than the duplication
saves — readers would have to mentally inline the spec to follow the logic.

The Remove pair (RemoveVoter / RemoveNonVoter) would merge cleanly, but at
~70 lines each the savings (~40 lines) don't justify the abstraction given
that both functions are individually straightforward to read.
