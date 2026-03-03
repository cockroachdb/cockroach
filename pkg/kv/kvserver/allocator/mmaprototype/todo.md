# Repair Actions TODO

## Easiest
- [x] `RemoveLearner` — find the learner replica and remove it. No constraint analysis, no diversity scoring. Simplified `RemoveNonVoter`.
- [x] `FinalizeAtomicReplicationChange` — send a finalization command, no store selection needed.

## Easy (compose existing Add + Remove)
- [x] `ReplaceDeadVoter` — combine remove (targeting dead store) + add (find new store) in a single atomic change.
- [x] `ReplaceDeadNonVoter` — same pattern for non-voters.
- [x] `ReplaceDecommissioningVoter` — identical to dead voter but targeting decommissioning stores.
- [ ] `ReplaceDecommissioningNonVoter` — same for non-voters.

## Harder (need constraint analysis for swaps)
- [ ] `SwapVoterForConstraints` — identify which voter violates constraints and find a replacement. Requires helpers from `constraint_unused_test.go`.
- [ ] `SwapNonVoterForConstraints` — same for non-voters.
