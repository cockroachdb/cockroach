# ASIM MMA Repair Integration Tests

Status: Each test mirrors a TestClusterState repair scenario from
`pkg/kv/kvserver/allocator/mmaprototype/testdata/cluster_state/`.

- [x] repair_add_voter — Under-replicated voters (implemented)
- [ ] repair_add_nonvoter — Under-replicated non-voters
- [ ] repair_promote_nonvoter — Promote non-voter to voter
- [ ] repair_remove_voter — Over-replicated voters (dead store)
- [ ] repair_remove_voter_healthy — Over-replicated voters (all healthy)
- [ ] repair_remove_nonvoter — Excess non-voters
- [ ] repair_remove_learner — Stuck LEARNER removal
- [ ] repair_finalize_atomic — Joint config finalization
- [ ] repair_replace_dead_voter — Dead voter replacement
- [ ] repair_replace_dead_nonvoter — Dead non-voter replacement
- [ ] repair_replace_decom_voter — Decommissioning voter replacement
- [ ] repair_replace_decom_nonvoter — Decommissioning non-voter replacement
- [ ] repair_swap_voter — Voter constraint swap
- [ ] repair_swap_nonvoter — Non-voter constraint swap
- [ ] repair_range_unavailable — Quorum loss (repair skipped)
