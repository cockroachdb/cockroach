# MMA Repair Pipeline: Decision Brief

## Context

The MMA prototype today handles rebalancing — moving replicas and leases to
balance load across stores. It does NOT handle repair — upreplication,
dead/decommissioning node replacement, constraint enforcement, learner cleanup.
That work is still done by the replicate and lease queues (the "legacy
allocator").

PR #164658 is a prototype that teaches MMA to handle all 12 repair actions. It
works: end-to-end tests pass, ASIM simulations produce correct results, and a
`TestCluster` integration test demonstrates upreplication from 1 to 3 voters
driven entirely by MMA. The prototype is ~5,500 lines across 63 commits — too
large to review as-is, but the code is structured and tested, not throwaway.

This brief proposes a plan for productionizing this work.

## What the repair pipeline does

A new cluster setting mode (`MultiMetricRepairAndRebalance`) makes MMA solely
responsible for all replica placement. When active, the replicate and lease
queues become no-ops. MMA runs repair before rebalancing on each tick, with
repair actions prioritized:

1. Finalize incomplete atomic replication changes
2. Remove orphaned learners
3. Add/replace/remove voters (dead, decommissioning, count)
4. Add/replace/remove non-voters (same)
5. Swap replicas to satisfy constraints

Each range is eagerly indexed by its needed repair action. Pending changes
suppress further repair on the same range to prevent pile-up.

## Proposed merge plan

Six PRs, each 200–500 lines of real code, each reviewable in a single sitting:

| PR | What | Why this order |
|----|------|----------------|
| 1. Foundation | RepairAction enum, priority ordering, repair index on cluster state, constraint helpers | Establishes the action space for early feedback |
| 2. AddVoter + orchestration | First working repair action, repair loop, pending-change integration | Anchors the orchestration discussion with a concrete example |
| 3. Production integration | New setting, queue disabling, ForceReplicationScanAndProcess routing, metrics, TestCluster test | Surfaces production questions early, before all 12 actions exist |
| 4. Count-based actions | RemoveVoter, AddNonVoter, RemoveNonVoter, RemoveLearner, FinalizeAtomicChange | Pattern repetition — fast to review |
| 5. Dead/decom replacement | Replace dead/decommissioning voters and non-voters, quorum-loss detection | Higher-risk logic, isolated for focused review |
| 6. Constraint swaps | SwapVoter/NonVoterForConstraints | Completes the action set |

The PRs are sequential. Total review surface is comparable to 2–3 normal
feature PRs once testdata files (mechanical golden outputs) are skimmed rather
than read line-by-line.

## What this does NOT include

The six PRs above get us to "all repair actions work behind a test-only
setting." Reaching production parity is a longer road:

- **Count-based rebalancing.** MMA does not yet harmonize replica/lease counts
  as a lower-priority goal alongside load balancing. This is a separate
  workstream.
- **Roachtests.** The prototype has unit tests and ASIM coverage but no
  roachtests. We'd need targeted roachtests (upreplication under load,
  decommissioning, mixed-version) before any cloud rollout.
- **Gradual cloud rollout.** The setting would roll out incrementally, likely
  cluster-by-cluster, with the legacy queues as the fallback. This is a
  standard process but takes calendar time.
- **Lease queue parity.** The lease queue today handles things beyond basic
  lease placement — IO overload reactions, follow-the-workload heuristics, etc.
  We would NOT aim for 1:1 feature parity. Some of these behaviors (e.g.
  follow-the-workload) would simply go away. Others (IO overload reactions)
  have been substantially mitigated at other layers and may no longer need
  dedicated allocator support. If something turns out to be truly needed, we'd
  rebuild it in MMA when that becomes clear rather than preemptively porting
  everything.

## Why do this

- **This is the whole point of MMA.** MMA was built to replace the legacy
  allocator. As long as the replicate and lease queues own repair, we're
  maintaining two systems that make conflicting decisions. Every allocator bug
  or improvement has to reason about both code paths.
- **The prototype suggests it's tractable.** 12 repair actions in ~1,600 lines,
  well-tested, following patterns that are more straightforward than the legacy
  scorer hierarchy. The code is simpler than what it replaces.
- **Incremental risk.** The new mode is behind a setting, disabled by default,
  guarded so it can't be enabled outside tests until we're ready. We can merge
  the code, build confidence through ASIM and roachtests, and roll out when the
  evidence supports it.

## Risks and costs

- **Long tail to production.** Merging the code is the easy part.
  Roachtests, cloud rollout, handling edge cases that only appear at scale —
  this is realistically a year of intermittent work. Someone needs to keep
  prodding it along.
- **Incomplete parity.** We're intentionally not porting every legacy behavior.
  This is a bet that most of those behaviors are no longer necessary or are
  better solved elsewhere. If we're wrong about a specific one, we'll discover
  it during rollout and need to react.
- **Two systems in the interim.** Until MMA repair is fully rolled out and the
  legacy queues are removed, we're maintaining both. This is the current
  situation anyway, but it extends the timeline during which both exist.

## Decision needed

Do we want to proceed with merging the repair pipeline behind a test-only
setting, with the understanding that productionization (roachtests, cloud
rollout, count-based rebalancing) is a sustained effort over the coming year?

The alternative is to leave MMA as rebalancing-only indefinitely and continue
maintaining the legacy allocator for repair. This avoids the near-term review
and integration cost but delays the original goal of MMA and extends the period
of maintaining two competing systems.
