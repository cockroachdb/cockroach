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

---

## Test Specs

### repair_add_nonvoter

Mirrors `repair_add_nonvoter.txt`: ranges have 3 voters, config says 5 replicas
with 3 voters. MMA adds 2 non-voters per range.

```
# All 10 ranges start with 3 voters but need 5 replicas (3V+2NV).
# MMA must add 2 non-voters per range to satisfy the span config.

gen_cluster nodes=6 region=(a,b,c) nodes_per_region=(2,2,2)
----

gen_ranges ranges=10 repl_factor=3
----

set_span_config
[0,9999999999): num_replicas=5 num_voters=3
----

assertion type=conformance under=0
----

eval duration=10m seed=42 cfgs=(mma-repair) metrics=(replicas,leases)
----
```

### repair_promote_nonvoter

Mirrors `repair_promote_nonvoter.txt`: ranges have 2 voters + 1 non-voter,
config says 3 voters. MMA promotes the non-voter instead of adding a new replica.

```
# 10 ranges with 2 voters + 1 non-voter each. Config says all 3 should
# be voters. MMA promotes non-voters rather than adding 4th replicas.

gen_cluster nodes=6 region=(a,b,c) nodes_per_region=(2,2,2)
----

gen_ranges ranges=10 repl_factor=3 placement_type=replica_placement
{s1:*,s2,s3:NON_VOTER}:1
----

set_span_config
[0,9999999999): num_replicas=3 num_voters=3
----

assertion type=conformance under=0
----

eval duration=10m seed=42 cfgs=(mma-repair) metrics=(replicas,leases)
----
```

### repair_remove_voter

Mirrors `repair_remove_voter.txt`: ranges have 4 voters, config says 3, one
node is dead. MMA removes the dead voter (no replacement needed since
4 > 3).

```
# 10 ranges with 4 voters each, config says 3. Node 4 is dead.
# MMA removes the dead voter, leaving 3 healthy voters per range.

gen_cluster nodes=6 region=(a,b,c) nodes_per_region=(2,2,2)
----

gen_ranges ranges=10 repl_factor=4 placement_type=replica_placement
{s1:*,s2,s3,s4}:1
----

set_span_config
[0,9999999999): num_replicas=3
----

setting split_queue_enabled=false
----

set_status node=4 liveness=dead
----

assertion type=conformance over=0
----

eval duration=10m seed=42 cfgs=(mma-repair) metrics=(replicas,leases)
----
```

### repair_remove_voter_healthy

Mirrors `repair_remove_voter_healthy.txt`: ranges have 4 healthy voters, config
says 3. MMA removes the least-diverse voter.

```
# 10 ranges with 4 voters each, config says 3. All nodes healthy.
# MMA removes 1 excess voter per range, preferring to drop the
# least-diverse one.

gen_cluster nodes=6 region=(a,b,c) nodes_per_region=(2,2,2)
----

gen_ranges ranges=10 repl_factor=4 placement_type=replica_placement
{s1:*,s2,s3,s4}:1
----

set_span_config
[0,9999999999): num_replicas=3
----

setting split_queue_enabled=false
----

assertion type=conformance over=0
----

eval duration=10m seed=42 cfgs=(mma-repair) metrics=(replicas,leases)
----
```

### repair_remove_nonvoter

Mirrors `repair_remove_nonvoter.txt`: ranges have 3V+3NV (6 total), config says
5 replicas with 3 voters. MMA removes 1 excess non-voter.

```
# 10 ranges with 3 voters + 3 non-voters = 6 total. Config says 5
# replicas (3V+2NV). MMA removes 1 non-voter per range.

gen_cluster nodes=6 region=(a,b,c) nodes_per_region=(2,2,2)
----

gen_ranges ranges=10 repl_factor=6 placement_type=replica_placement
{s1:*,s2,s3,s4:NON_VOTER,s5:NON_VOTER,s6:NON_VOTER}:1
----

set_span_config
[0,9999999999): num_replicas=5 num_voters=3
----

assertion type=conformance over=0
----

eval duration=10m seed=42 cfgs=(mma-repair) metrics=(replicas,leases)
----
```

### repair_remove_learner

Mirrors `repair_remove_learner.txt`: ranges have 3 voters + 1 LEARNER, config
says 3. MMA removes the stuck learner.

**Risk**: LEARNER replica type is supported by the placement parser but ASIM's
simulation engine may not fully handle learner semantics. Verify this works
before implementing.

```
# 10 ranges with 3 voters + 1 learner. Config says 3 replicas.
# MMA removes the stuck learner from each range.

gen_cluster nodes=6 region=(a,b,c) nodes_per_region=(2,2,2)
----

gen_ranges ranges=10 repl_factor=4 placement_type=replica_placement
{s1:*,s2,s3,s4:LEARNER}:1
----

set_span_config
[0,9999999999): num_replicas=3
----

assertion type=conformance over=0
----

eval duration=10m seed=42 cfgs=(mma-repair) metrics=(replicas,leases)
----
```

### repair_finalize_atomic

Mirrors `repair_finalize_atomic.txt`: ranges are stuck in joint config with
VOTER_INCOMING. MMA finalizes by promoting to VOTER_FULL.

**Risk**: VOTER_INCOMING is supported by the placement parser but ASIM may not
model joint config semantics. Verify this works before implementing.

```
# 10 ranges with 2 VOTER_FULL + 1 VOTER_INCOMING. Config says 3
# replicas. MMA finalizes the joint config (promotes VOTER_INCOMING
# to VOTER_FULL).

gen_cluster nodes=6 region=(a,b,c) nodes_per_region=(2,2,2)
----

gen_ranges ranges=10 repl_factor=3 placement_type=replica_placement
{s1:*,s2,s3:VOTER_INCOMING}:1
----

set_span_config
[0,9999999999): num_replicas=3
----

assertion type=conformance under=0
----

eval duration=10m seed=42 cfgs=(mma-repair) metrics=(replicas,leases)
----
```

### repair_replace_dead_voter

Mirrors `repair_replace_dead_voter.txt`: ranges have 3 voters at RF=3, one
node dies. MMA adds voter on healthy node + removes dead voter (atomic
replace).

```
# 10 ranges with 3 voters, all placed on s1-s3. Node 3 dies.
# MMA replaces dead voter: adds voter on a healthy node, removes
# dead voter on s3.

gen_cluster nodes=6 region=(a,b,c) nodes_per_region=(2,2,2)
----

gen_ranges ranges=10 repl_factor=3 placement_type=replica_placement
{s1:*,s2,s3}:1
----

set_span_config
[0,9999999999): num_replicas=3 num_voters=3
----

setting split_queue_enabled=false
----

set_status node=3 liveness=dead
----

assertion type=conformance under=0
----

eval duration=10m seed=42 cfgs=(mma-repair) metrics=(replicas,leases)
----
```

### repair_replace_dead_nonvoter

Mirrors `repair_replace_dead_nonvoter.txt`: ranges have 3V+2NV, one NV's node
dies. MMA adds non-voter on healthy node + removes dead non-voter.

```
# 10 ranges with 3 voters + 2 non-voters. Node 5 (hosting one NV)
# dies. MMA replaces the dead non-voter.

gen_cluster nodes=6 region=(a,b,c) nodes_per_region=(2,2,2)
----

gen_ranges ranges=10 repl_factor=5 placement_type=replica_placement
{s1:*,s2,s3,s4:NON_VOTER,s5:NON_VOTER}:1
----

set_span_config
[0,9999999999): num_replicas=5 num_voters=3
----

setting split_queue_enabled=false
----

set_status node=5 liveness=dead
----

assertion type=conformance under=0
----

eval duration=10m seed=42 cfgs=(mma-repair) metrics=(replicas,leases)
----
```

### repair_replace_decom_voter

Mirrors `repair_replace_decom_voter.txt`: ranges have 3 voters at RF=3, one
node is decommissioning. MMA replaces the decommissioning voter.

```
# 10 ranges with 3 voters, all placed on s1-s3. Node 3 is
# decommissioning. MMA replaces its voter on each affected range.

gen_cluster nodes=6 region=(a,b,c) nodes_per_region=(2,2,2)
----

gen_ranges ranges=10 repl_factor=3 placement_type=replica_placement
{s1:*,s2,s3}:1
----

set_span_config
[0,9999999999): num_replicas=3 num_voters=3
----

setting split_queue_enabled=false
----

set_status node=3 membership=decommissioning
----

assertion type=conformance under=0
----

eval duration=10m seed=42 cfgs=(mma-repair) metrics=(replicas,leases)
----
```

### repair_replace_decom_nonvoter

Mirrors `repair_replace_decom_nonvoter.txt`: ranges have 3V+2NV, one NV's node
is decommissioning. MMA replaces the decommissioning non-voter.

```
# 10 ranges with 3 voters + 2 non-voters. Node 5 (hosting one NV)
# is decommissioning. MMA replaces its non-voter.

gen_cluster nodes=6 region=(a,b,c) nodes_per_region=(2,2,2)
----

gen_ranges ranges=10 repl_factor=5 placement_type=replica_placement
{s1:*,s2,s3,s4:NON_VOTER,s5:NON_VOTER}:1
----

set_span_config
[0,9999999999): num_replicas=5 num_voters=3
----

setting split_queue_enabled=false
----

set_status node=5 membership=decommissioning
----

assertion type=conformance under=0
----

eval duration=10m seed=42 cfgs=(mma-repair) metrics=(replicas,leases)
----
```

### repair_swap_voter

Mirrors `repair_swap_voter.txt`: voter_constraints require 2 voters in region b,
but all voters are in region a. MMA swaps 2 voters from region a to region b.

```
# 10 ranges with 3 voters all in region a. voter_constraints require
# 2 voters in region b. MMA swaps voters: adds 2 in b, removes 2
# from a, keeping s1 (leaseholder) in a.

gen_cluster nodes=6 region=(a,b) nodes_per_region=(3,3)
----

gen_ranges ranges=10 repl_factor=3 placement_type=replica_placement
{s1:*,s2,s3}:1
----

set_span_config
[0,9999999999): num_replicas=3 voter_constraints={'+region=b':2}
----

setting split_queue_enabled=false
----

assertion type=conformance violating=0
----

eval duration=10m seed=42 cfgs=(mma-repair) metrics=(replicas,leases)
----
```

### repair_swap_nonvoter

Mirrors `repair_swap_nonvoter.txt`: constraints require 2 replicas in region b,
but one non-voter is misplaced in region a. MMA swaps it to region b.

```
# 10 ranges: 3 voters in region a, 2 non-voters (1 in region a
# misplaced, 1 in region b). Constraints require 2 total replicas
# in region b. MMA swaps the misplaced NV from a to b.
#
# s1-s3 are in region a, s4-s6 are in region b.
# s4 initially gets a non-voter (misplaced in region a? No — s4 is
# in region b already). We need the misplaced NV in region a.
# With nodes_per_region=(3,3): s1,s2,s3=region a; s4,s5,s6=region b.
# Place NV on s3 (region a, misplaced) and s4 (region b, correct).
# Wait — s3 already has a voter. Need a different approach.
#
# Use 8 nodes: 4 in region a (s1-s4), 4 in region b (s5-s8).
# Place 3 voters on s1-s3, 1 NV on s4 (region a, misplaced),
# 1 NV on s5 (region b, correct). Constraint: 2 in region b.

gen_cluster nodes=8 region=(a,b) nodes_per_region=(4,4)
----

gen_ranges ranges=10 repl_factor=5 placement_type=replica_placement
{s1:*,s2,s3,s4:NON_VOTER,s5:NON_VOTER}:1
----

set_span_config
[0,9999999999): num_replicas=5 num_voters=3 constraints={'+region=b':2}
----

setting split_queue_enabled=false
----

assertion type=conformance violating=0
----

eval duration=10m seed=42 cfgs=(mma-repair) metrics=(replicas,leases)
----
```

### repair_range_unavailable

Mirrors `repair_range_unavailable.txt`: 3 voters, 2 nodes die, quorum lost.
MMA detects unavailability and skips repair.

**Note**: The unit test's repair action is `RepairSkipped` — MMA doesn't attempt
repair on unavailable ranges. The ASIM test verifies the simulator doesn't
crash/hang.

```
# 10 ranges with 3 voters on s1-s3. Nodes 2 and 3 die, leaving
# only 1 of 3 voters alive (no quorum). MMA should skip repair.

gen_cluster nodes=5 region=(a,b,c) nodes_per_region=(2,2,1)
----

gen_ranges ranges=10 repl_factor=3 placement_type=replica_placement
{s1:*,s2,s3}:1
----

set_span_config
[0,9999999999): num_replicas=3 num_voters=3
----

setting split_queue_enabled=false
----

set_status node=2 liveness=dead
----

set_status node=3 liveness=dead
----

assertion type=conformance unavailable=10
----

eval duration=10m seed=42 cfgs=(mma-repair) metrics=(replicas,leases)
----
```
