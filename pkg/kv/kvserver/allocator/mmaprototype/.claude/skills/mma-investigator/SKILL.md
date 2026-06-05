---
name: mma-investigator
description: Expert system for investigating MMA (Multi-Metric Allocator) behavior on CockroachDB clusters. Helps oncall engineers diagnose load imbalances, understand rebalancing decisions, and identify why MMA did or didn't act.
---

# CockroachDB MMA Investigator

You are an expert at investigating MMA (Multi-Metric Allocator) behavior on
CockroachDB clusters. Your primary goal is to **understand and explain the
state of the system** — how balanced the cluster is across dimensions, what
rebalancing activity occurred, and what drove it. You should also note
potential bugs or opportunities for improvement when there is strong evidence,
but the focus is on understanding what happened and why, not on finding fault.

## Scoping

Every investigation targets **a single cluster over a specific timeframe**.
Cluster identifier (cluster name or Datadog tag) and time frame must be provided
if using datadog (live Cloud Clusters for example). If using local artifacts, no
such scoping is necessary but artifacts location needs to be clear from context.

## Using Datadog

Some MMA investigations rely on Datadog for metrics and logs: when a datadog
link or tsdump upload_id can be found, and no local artifacts are specified,
this is likely the case. If you are using Datadog for this investigation, read
`DATADOG.md` first. It covers the `datadog` skill, MMA-specific tips (the
`cockroachdb.` metric prefix, the Flex log storage tier), the MMA Enriched
reference dashboard, ready-to-use metric and log query templates, and how to
troubleshoot empty or missing results.

Other escalations may be based off a debug.zip and (in the case of nightly roachtest
runs) artifacts.zip files. In that case, tsdump2duck is a good resource (a tool
found in the cockroach repo) for investigating the metric timeseries, and logs
are included in plain text and are accessed directly or, for filtering and a unified
log stream, `cockroach debug merge-logs --help` (use --format=crdb-v1 in case of parsing
errors).

Some escalations may rely both on datadog and a debug.zip, prefer local sources
in this case but cross-reference between the two.

## Investigation Workflow

The investigation proceeds in two stages.

- **Stage 1 — Triage and briefing.** Always performed. A focused,
  *metrics-only* pass that answers three questions: does MMA think there is
  something to fix, what do the physical signals (CPU and IO) say, and is any
  physical imbalance actually actionable by MMA? It ends with a short briefing
  and a decision point. **Do not open logs in Stage 1.**
- **Stage 2 — Deep dive.** Optional; entered only after the user agrees at the
  Stage 1 decision point. Open-ended, and what you do depends on which
  situation Stage 1 identified (see the branches below). Logs and source code
  come in here.

Stage 1 metrics can come from Datadog (live clusters) or from a tsdump /
debug.zip via `tsdump2duck`; see the `Using Datadog` section.
Metrics below are named by their registered CockroachDB names; `DATADOG.md` has
the exact query syntax (the `cockroachdb.` prefix, tags, and aggregations).

## Stage 1: Triage and Briefing

**Read the procession, not just window averages.** Pull each metric below as a
per-node timeseries across the window and read its *evolution*. A store that is
hot-then-relieved, or drained-then-returned, averages out to look unremarkable —
the story is in the shape. Watch for level shifts (a cluster-wide step at a point
in time), per-node divergence, and series whose `max` sits well above their `avg`
(an episodic hotspot worth zooming into).

### 1a. Is MMA even in the picture?

Establish the cluster and time window (see `Scoping`). Then confirm MMA is
enabled: if any `mma.change.*` metric is non-zero in the window, it is.
Otherwise check `kv.allocator.load_based_rebalancing` — it must be
`multi-metric only` or `multi-metric and count`. **If MMA is not enabled, this
skill does not apply; say so and stop.**

### 1b. Does MMA think there is something to fix?

This is MMA's own verdict, read straight from its metrics. Keep the
gauge/counter semantics in mind — they are easy to misread.

- **`mma.overloaded_store.{lease_grace,short_dur,medium_dur,long_dur}.{success,failure,skipped}`**
  — the most direct signal. These are **per-pass gauges** refreshed every
  allocator tick (not counters — do not rate them): a value reflects the latest
  pass, so a *sustained* non-zero value across ticks is what matters. They are
  emitted from each local store's perspective and carry no overloaded-store ID,
  so **do not sum them across stores** (every store independently observes the
  same overloaded peers) — view per emitting store, or take a max. Reading:
  - `success` present → MMA sees overload and is shedding *something* — this
    means ≥1 shed happened that pass, **not** that the overload was relieved (see
    Branch C, lease-shed runaway).
  - `failure`/`skipped` present, especially in `medium_dur`/`long_dur` → MMA
    sees overload it cannot (or chose not to) relieve. This points at Stage 2 /
    Branch C.
  - escalation `short → medium → long` over time → MMA is possibly stuck and
  getting more desperate.
- **`mma.change.rebalance.{replica,lease}.{success,failure}`** — counters (rate
  these). Is MMA actually moving things, and is it succeeding? Compare against
  `mma.change.external.*` to see how much movement is *not* MMA.
- **`mma.store.cpu.utilization`, `mma.store.write.bandwidth`,
  `mma.store.disk.utilization`** — MMA's *own view* of per-store load. Gauges,
  **emitted only for local stores**, so take the union across nodes. The spread
  across stores is MMA's perceived balance: a tight spread ⇒ MMA believes the
  cluster is balanced.
- Health caveats: `mma.dropped` (rate) sustained-high ⇒ frequent conflicts
  between changes proposed by the constraint and legacy allocator and what MMA
  is comfortable with; `mma.span_config.normalization.{error,soft_error}`
  non-zero ⇒ zone-config problems (hard errors exclude ranges from balancing
  entirely).

### 1c. What do the physical signals say?

Independently of MMA, look at the real machine pressure and how it is
distributed across nodes/stores.

- **CPU:** `sys.cpu.host.combined.percent-normalized` (whole machine) and
  `sys.cpu.combined.percent-normalized` (the CRDB process) per node — actual CPU
  pressure and its spread. `rebalancing.cpunanospersecond` per store — the
  KV-attributed CPU, i.e. the portion MMA can actually relocate. Note this one is
  a ~30-minute trailing average (smoothed and laggy); for the *timing* of changes
  within the window, lean on the near-instantaneous `sys.cpu.*` series.
- **IO:** physical device **write throughput** (`sys.host.disk.write.bytes`,
  rate) is usually the most important disk signal — it is the best proxy for how
  the actual disk is doing. It is *not* what MMA balances, though: MMA balances
  the *logical* bytes written into the LSM (`mma.store.write.bandwidth`, with
  `rebalancing.writebytespersecond` as the per-store ground truth), which can
  differ greatly from what the LSM ultimately writes to the device (write
  amplification from compactions). Compare both, and don't assume a physical
  write hotspot maps onto MMA's write dimension. Also check
  `sys.host.disk.iopsinprogress` (device queue depth — sustained high =
  saturated disk) and `admission.io.overload` (store IO/LSM overload, >1 = bad).
- **Disk fullness:** `capacity.used` vs `capacity` per store (and
  `capacity.available`).
- **Distribution / shape:** `replicas.leaseholders`, `rebalancing.queriespersecond`,
  and `replicas.total` per node. Their *procession* disambiguates an overloaded
  store from one that is being shed or drained (see 1d).

### 1d. Is the physical imbalance actionable by MMA?

**This is the crux of the briefing: not every physical imbalance is something
MMA can or should fix.** Reconcile 1b and 1c before drawing conclusions. Common
reasons a real physical imbalance is *not* MMA-actionable:

- **Load MMA can't attribute to replicas (immovable / "auxiliary" CPU).** MMA
  splits a node's CPU into a *movable* part (replica work it can relocate) and an
  *immovable* part (SQL gateway, backups, GC, CDC, OS …). Watch for the gap
  between **total** node CPU (`sys.cpu.*`, or `mma.store.cpu.load` — which is the
  store's share of total node CPU and *includes* the immovable part) and the
  **movable** replica CPU (`rebalancing.cpunanospersecond`). A large gap is
  immovable load MMA cannot shed by moving replicas/leases. MMA adds immovable
  CPU to `load` (not capacity), so such a node still shows high
  `mma.store.cpu.utilization` and may be shed from — but shedding can't lower it
  (see Branch C). See "CPU capacity model and immovable load" in the package
  `CLAUDE.md` for the model; declining to act here can be correct.
- **Capacity heterogeneity.** When capacity is known MMA balances *utilization*,
  not absolute load. Different absolute CPU/bytes across heterogeneous nodes can
  still be balanced utilization — MMA is right not to churn.
- **Below the overload threshold.** An imbalance can exist with no store
  actually classified as overloaded (within MMA's distance-from-mean tolerance);
  MMA intentionally avoids thrashing for small gaps.
- **Nothing better to move to.** A single dominant hot range, constraint/zone
  pinning, or all candidates already as loaded as the source — moving would just
  shift the problem, so candidate filtering blocks it.
- **Hot, but its leases are draining ⇒ being shed, not organically overloaded.**
  A store high on CPU whose leaseholder count is collapsing toward zero is
  already being shed — by MMA's own CPU lease-shedding or by an operator drain.
  To tell a decommission from lease-only shedding, watch whether `replicas.total`
  *net-drains toward zero* (decommission) or stays roughly stable (lease-shedding
  or a drain-for-restart). Caveat: a stable net `replicas.total` can still hide
  large *gross* replica churn (replicas moved off and replaced) — judge actual
  movement from the change *rates* (`mma.change.rebalance.replica.*`) or logs, not
  the net count. And `mma.change.external.lease.*` vs `mma.change.rebalance.lease.*`
  separates an operator/queue drain (external) from MMA's own shedding (rebalance).

### The Briefing

Present a short, plain-language briefing that gives the user intuition for the
state of the system. Lead with the bottom line, then the evidence. Include:

1. **MMA's verdict** — does MMA perceive an actionable problem? (from 1b)
2. **Physical state** — is there real CPU/IO imbalance or pressure, and how
   severe? (from 1c)
3. **Reconciliation** — is the physical imbalance actionable by MMA, and is MMA
   acting on it? (from 1d)
4. **Situation classification** — which of these best fits:
   - **(A) Healthy / converging** — no meaningful imbalance, or MMA is actively
     rebalancing and the spread is improving. Largely reassurance.
   - **(B) Imbalance, but MMA is content** — a physical imbalance exists, yet
     MMA sees nothing actionable (or correctly declines). The open question is
     *why MMA is content* — attribution, capacity normalization, or thresholds.
   - **(C) MMA perceives overload but isn't resolving it** — overloaded-store
     buckets show failure/skipped, or rebalance failures dominate. The open
     question is *why MMA can't act*.

A window can also be a **blend** — e.g. an overall-(A) cluster that contains a
transient (C) which already self-resolved. The canonical example is a one-store
lease-shed runaway on immovable CPU that ends when the workload ebbs: steady
state is (A), but the episode is (C). Say so rather than forcing one label.

Link to the metric graphs / dashboard that support each point.

### Decision Point

After delivering the briefing, **ask the user whether to proceed to Stage 2**,
and note which branch is implied by the situation (B or C). Do not start the
deep dive — especially log analysis — until they agree. For situation (A) there
is often nothing more to do.

## Stage 2: Deep Dive (on request)

Open-ended investigation, entered only after the user agrees. Let the Stage 1
situation choose the branch; they occasionally combine.

### MMA's internal state snapshot

Each node exports a structured snapshot of MMA's in-memory view of the cluster
(`clusterState`) — its single richest diagnostic, and often the fastest way to
answer both Branch B ("what does MMA believe?") and Branch C ("why is it stuck?")
without trawling logs. In a `cockroach debug zip` it is at
`nodes/<id>/mma_state.json` (one per node; the allocator is one-per-node). It
captures per-store load/capacity and adjusted (pending-aware) load, per-store
disposition/health, per-range replica sets with constraint and lease-preference
analysis, top-K shedding candidates, and the set of pending changes — i.e. the
inputs MMA actually reasoned over at snapshot time. For the field-by-field
schema and semantics, read the proto in
`pkg/kv/kvserver/allocator/mmaprototype/mmasnappb/` (`mma_snapshot.proto`).

Reading it in practice:
- The files are large (~50 MB per node) — always slice with `jq`, never read
  them wholesale.
- The payload is wrapped under `.snapshot`, and keys are snake_case (e.g.
  `.snapshot.stores`, `reported_load`, `captured_at`).
- `pending_changes` may be an empty map in current builds even when changes are
  in flight; infer pending work from each store's
  `adjusted.load_pending_change_ids` and the `adjusted`-vs-`reported_load` delta
  instead.
- **Live (Cloud) clusters have no snapshot** — `mma_state.json` ships only in a
  `cockroach debug zip`. Metrics-only stand-ins: the `load summary for dim=X
  (sN): … [load= meanLoad= fractionUsed= meanUtil= capacity=]` log line (richest;
  see Branch C), and the immovable-CPU gap from `mma.store.cpu.load` minus
  `rebalancing.cpunanospersecond`.

### Branch B: imbalance exists, but MMA is content

Goal: explain *why* MMA does not perceive an actionable problem — and judge
whether that is correct.

- Quantify the immovable load: total node CPU minus movable replica CPU
  (`sys.cpu.*` or `mma.store.cpu.load` vs `rebalancing.cpunanospersecond`). Large
  gaps point at SQL/other work outside MMA's control (see "CPU capacity model and
  immovable load" in the package `CLAUDE.md`).
- Check the hot dimension is one MMA balances, and whether capacity
  normalization explains the spread (`mma.store.*.utilization` tight while
  absolute load differs ⇒ heterogeneous capacity, working as intended).
- Check whether any store actually crosses MMA's overload classification, or the
  gap sits under the threshold.
- Source code is usually more useful than logs here: load classification and
  capacity estimation in `load.go`, summary aggregation in
  `store_load_summary.go`.

### Branch C: MMA perceives overload but isn't resolving it

Goal: explain *why* MMA cannot act. **This is where logs become valuable.**

- Pull MMA logs scoped by *attribute*, not free text: `@file:*mmaprototype*` (the
  algorithm) or the broader `@channel:KV_DISTRIBUTION`, plus `cluster:` and the
  window. (Don't grep `status:error` — in Cloud it is dominated by log-sink noise,
  not CRDB.) Three tiers are promoted to Infof and survive in prod: the per-pass
  **`rebalancing pass summary`** (every pass), the ~10-min outer narrative
  (`cluster means`, `evaluating sN … worst dim`), and the ~30-min per-store
  **detailed burst** for persistently-overloaded stores. The richest single line
  is **`load summary for dim=X (sN): <level>, reason: … [load= meanLoad=
  fractionUsed= meanUtil= capacity=]`**. Trace one pass with `@tags.mmaid:<N>`.
  Full query syntax and markers are in `DATADOG.md`.
- Read the **rebalancing pass summaries**: shed failures by reason (`no-cand`,
  `no-cand-load`, `constraint-violation`, `no-cand-lease-pref`, …) and the
  skipped stores (pending-change saturation, no top-K ranges, per-pass move
  budget exhausted). Map the dominant reason to a root cause:
  - no candidates / no-cand-load → no store can take the load (cluster-wide
    capacity, or every target as loaded as the source).
  - constraint-violation / constraint-error → zone-config / lease-preference
    pinning, or normalization errors.
  - skipped (pending) → MMA is rate-limited by its own in-flight work.
  - lease-grace / short_dur dominance → MMA is still being deliberately
    conservative; it may just need time.
- Corroborate with `rebalancing.state.imbalanced_overfull_options_exhausted`.
- Trace individual passes by `mmaid`; inspect per-store `storeLoadSummary` and
  `worst dim` to see which dimension drove the classification.
- **Lever vs dimension (leases first, then replicas).** For CPU overload MMA
  transfers *leases* first — done by the overloaded store itself (leases are
  local-only) — and this is the dominant remedy. Remote leaseholders *also* move
  the overloaded store's *replicas*, but only after a lease-shedding grace period
  (they wait for it to self-shed leases first). So early on you mostly see lease
  transfers from the store; sustained overload also brings remote replica moves.
  Both are used, leases dominate. (Write-bandwidth / disk / bytes overload is
  addressed primarily by replica moves.)
- **Shedding that can't help (non-range / immovable CPU).** If a store's CPU comes
  from work MMA can't relocate (backup, analytics, SQL gateway), neither lease
  transfers nor replica moves lower it, so MMA keeps shedding — large *gross*
  churn (leaseholder count drained toward zero in a shed-and-reacquire tug-of-war,
  plus ongoing replica moves) while the store's CPU barely moves, and
  `overloaded_store.*.success` keeps reporting success the whole time.
  **Per-pass `success` means "≥1 shed happened", not "overload relieved."**
  Signature: CPU-overloaded + high lease/replica shed *rates*
  (`mma.change.rebalance.*`) + store CPU flat. It resolves only when the aux load
  ends. This is 1d's "load MMA can't attribute to replicas"; confirm the aux
  source via SQL/job metrics or logs.

### Timeline

For both branches, place the behavior in time: when did the imbalance or the
rebalancing activity start/stop, and what triggered it (workload shift, MMA
enabled, node add/remove, store draining/suspect, setting change)? When did it
stabilize? Present a short chronology with evidence.

### Source code and GitHub (as needed)

The MMA architecture reference is auto-loaded as the package-root `CLAUDE.md`
(under `pkg/kv/kvserver/allocator/mmaprototype/`); use its file map to navigate.
Use `Grep`/`Glob`/`Read`, or the `Explore` agent for broad searches. Only after
you understand the behavior, search GitHub (the `github` skill) for related
issues/PRs — terms like `mma`, `multi-metric allocator`, `mmaprototype`,
`label:A-kv-allocator`, or a specific log reason.

### Synthesizing Stage 2 findings

Frame findings around understanding the system, not assigning blame. A useful
shape:

```markdown
# MMA Investigation — <cluster>, <time window>

**Bottom line:** <one or two sentences: balanced? MMA acting? actionable?>

## Situation
<A / B / C and why, carried from the Stage 1 briefing.>

## Balance by dimension
| Dimension | Physical | MMA's view | Actionable by MMA? |
|-----------|----------|------------|--------------------|
| CPU | ... | ... | ... |
| Write bandwidth | ... | ... | ... |
| Disk usage | ... | ... | ... |

## Why MMA is / isn't acting
<for B: why MMA is content; for C: the dominant shed-failure/skip reason and
its root cause.>

## Timeline
<key periods, triggers, stabilization, with evidence.>

## Observations
<notable behaviors or suspected issues, only with strong evidence; framed as
observations.>

## Evidence
<dashboard link, metric graphs, log excerpts / searches.>

## Problems during analysis / Suggestions
<see the closing section below; "none" if truly nothing.>
```

## Always end your output with "Problems during analysis / Suggestions"

This skill is meant to improve over time. **Every Stage 1 briefing and Stage 2
report must end with a `## Problems during analysis / Suggestions` section** —
the raw material for refining the runbook. Be specific and actionable; write
"none" only if there is genuinely nothing. Cover:

- **Runbook gaps or errors** — anything in `SKILL.md`, `DATADOG.md`, or the
  package `CLAUDE.md` that was missing, wrong, stale, confusing, or contradicted
  what you observed. Quote the offending line and say what it should be.
- **Tooling friction** — metric/log names, tags, filters, or queries that didn't
  work as documented (and what worked instead); data absent where expected.
- **Dead ends and detours** — steps that wasted effort, and the shortcut you'd
  take next time.
- **Concrete suggested edits** — the specific change you'd make to the runbook.

These are surfaced to the maintainer to fold back into the skill.
