---
name: runbook
description: Generate a metric-based diagnostic runbook for a CockroachDB production symptom. Reads the source code to find exact metric names, then produces a structured framework with pipeline explanation, grouped metrics, noise elimination, decision procedure, and optionally a Datadog dashboard.
---

# Diagnostic Runbook Generator

Generate a comprehensive diagnostic runbook given a production symptom. The output is a structured document that allows an on-call engineer to systematically determine root cause by triangulating metrics across independent measurement points and eliminating noise.

## When to Use

Activate when the user says:
- "I'm investigating an escalation with these symptoms: ..."
- "Can you build a diagnostic framework for ..."
- "What metrics should I look at to diagnose ..."
- "Customer is seeing X, help me figure out why"
- Any description of a production issue requiring metric-based root cause analysis

## Input

The user provides free-form symptoms as the argument. Examples:
- `throughput tanks periodically`
- `replicate queue failures from snapshot reservation timeouts`
- `high p99 latency on SELECT queries`
- `OOM kills on node 3 every 6 hours`
- `cross-region replication lag increasing`

## Workflow

### Phase 1: Research (read the code, don't guess)

Before writing any framework, you MUST read the CockroachDB source code to find the exact metric names relevant to the symptoms. **Never hardcode or guess metric names — always verify by reading the source.**

#### Step 1a: Identify research areas and launch parallel Explore agents

From the symptom, identify 3-6 independent research areas that together cover:
- The metrics that directly measure the reported symptom
- What could cause this symptom (resource bottlenecks, contention, configuration, upstream systems)
- What this symptom causes downstream (secondary failures, latency, under-replication)
- Resource baselines (CPU, memory, disk, network) that could explain the symptom

Launch one Explore agent per area **in parallel** (in a single message). Each agent should:
1. Read the relevant source files to find **exact metric name strings** and their types (gauge/counter/histogram)
2. Find **cluster settings** with their default values
3. Understand the **code path** — how does the system work, what triggers errors, what are the retry/timeout semantics
4. Identify the **measurement point** for each metric (pgwire vs gateway vs leaseholder vs store)
5. **For every metric, explain what it means in the codebase context**: Look at the metric's `Help` description in its `metric.Metadata` definition, then follow how and where the metric is used in the code. The goal is to produce a description that explains the system behavior — when and why this number changes, what a non-zero or elevated value implies for the operator, and what it does NOT capture. For reference, see the metric descriptions in `pkg/kv/kvserver/allocator/mmaprototype/mma_metrics.go` — they explain the operational meaning, not just paraphrase the metric name. For example:
   - BAD: "`range.snapshots.recv-failed` — snapshot receive failures"
   - GOOD: "`range.snapshots.recv-failed` — Number of incoming snapshot init messages that errored on the recipient store before data transfer begins. This includes rejections from `canAcceptSnapshotLocked()` (e.g., overlapping range descriptor, store draining, replica too old) and header validation failures. Does NOT count reservation queue timeouts or errors during data transmission/application — those are tracked by different error paths. A sustained non-zero rate indicates the receiver is consistently rejecting snapshots, which may point to descriptor conflicts or a draining node."

Agents should search beyond the index below — grep for metric name patterns, follow code paths, find settings files. The index is a starting point, not a boundary.

**Key source file index:**

| Area | File |
|------|------|
| Go runtime, RSS, CPU, disk, network | `pkg/server/status/runtime.go` |
| KV store metrics (Pebble, replication, snapshots, queues) | `pkg/kv/kvserver/metrics.go` |
| KV method enumeration (50 RPC methods) | `pkg/kv/kvpb/method.go` |
| DistSender per-method counters | `pkg/kv/kvclient/kvcoord/dist_sender.go` |
| Node per-method recv counters | `pkg/server/node.go` |
| RPC connection metrics (TCP RTT, heartbeats) | `pkg/rpc/metrics.go` |
| Clock offset, round-trip-latency | `pkg/rpc/clock_offset.go` |
| SQL memory accounting | `pkg/sql/mem_metrics.go` |
| Admission control | `pkg/util/admission/granter.go` |
| Snapshot settings & reservation logic | `pkg/kv/kvserver/snapshot_settings.go`, `store_snapshot.go` |
| Replicate queue metrics | `pkg/kv/kvserver/replicate_queue.go` |
| Raft entry cache | `pkg/kv/kvserver/raftentry/metrics.go` |
| Rangefeed memory | `pkg/kv/kvserver/rangefeed/metrics.go` |
| Lock table concurrency | `pkg/kv/kvserver/concurrency/metrics.go` |

#### Step 1b: Compile results and identify noise

From the agent results, collect ALL metrics and settings found. Then identify **noise** — metrics or signals in the cluster that could look like the symptom but have a different root cause. For each noise source, note which metric disambiguates it from the real symptom. Include these noise metrics in the framework so the engineer can rule them out.

**Rules:**
- Always verify metric name strings by reading the code. Never guess.
- Check both gateway-side (`distsender.rpc.{method}.sent`) and leaseholder-side (`rpc.method.{method}.recv`) variants when relevant.
- Record the type (gauge/counter/histogram) for every metric — this determines the Datadog query pattern.
- For every metric, read its `metric.Metadata` `Help` field and trace how it is used in the surrounding code. Even if you cannot find the exact `.Inc()` call site, always include the metric — use the `Help` description and the code context where the metric struct is referenced to explain what it means operationally.

### Phase 2: Build the Framework

#### Completeness check (MANDATORY before writing)

Before writing the framework, compile a master list of every metric and setting found by ALL research agents. This prevents silently dropping metrics during the write phase.

1. **Collect**: Go through each agent's results and extract every metric name, type, and Help text into a flat list.
2. **Deduplicate**: Remove exact duplicates (same metric reported by multiple agents).
3. **Triage into tiers**:
   - **Primary**: Metrics that directly measure the symptom, prove/eliminate a hypothesis, or are needed for the decision procedure. These go in the grouped metric tables and Quick Reference with full operational descriptions.
   - **Supporting**: Metrics that provide useful context, per-type breakdowns of primary metrics (e.g., voter/non-voter variants), or secondary signals. These go in the Quick Reference under a dedicated "Supporting Metrics" subsection — one line each with name, type, and a brief description.
4. **No silent drops**: Every metric on the master list must appear in either the primary tables or the supporting section. If you choose not to include a metric at all, add it to a "Excluded (not relevant)" list at the bottom with a one-line reason.

Write the framework to `/tmp/<topic-slug>-framework.md` following this exact structure.

**CRITICAL FORMATTING RULE**: Do NOT use markdown tables anywhere in the
framework. Tables render poorly in Google Docs via `md2json` — columns
collapse into flat text with no structure. Use bullet lists instead:
- For metrics: `metric.name` (type) — description
- For settings: `setting.name` (default: value) — description
- For decision checks: **Check?**: `metric` — signal → inference

```markdown
# <Topic>: <Subtitle>

## The Question
Given <symptom description>:
1. **<Primary question>** (what is the root cause?)
2. **<Secondary question>** (what is noise vs signal?)

---

## How <Relevant System> Works: The Pipeline

This section is the backbone of the framework. It must give the reader a
comprehensive mental model of the system — not just a linear list of steps,
but enough architectural context to reason about failures independently.

### Pipeline overview

Start with a high-level ASCII diagram showing all the stages and how they
connect. Then describe each stage in its own subsection.

### Stage N: <Stage Name>

For EACH stage in the pipeline, write a dedicated subsection that covers:

1. **What this component does and why it exists** — a 2-4 sentence overview
   that explains the component's role in the system. Write this for an
   engineer who is familiar with CockroachDB at a high level but has never
   looked at this subsystem. Explain the design motivation (e.g., "The
   receiver apply queue exists because applying a snapshot is an expensive
   disk-bound operation — it rewrites the entire range's state. The
   concurrency limit prevents multiple concurrent applies from saturating
   disk I/O and causing write stalls across all ranges on the store.").

2. **How it works internally** — describe the key data structures, queues,
   state machines, or algorithms that govern this stage. For queues: what
   is the ordering, what are the capacity limits, what happens when the
   queue is full. For state machines: what are the states and transitions.
   For algorithms: what is the decision logic. Include the key types and
   functions by name so the reader can find them in the code.

3. **Metrics at this stage** — the exact metric names that observe this
   stage, annotated with what each one means operationally at this point
   in the pipeline.

4. **Concurrency limits, timeouts, and settings** — at each stage where
   there is a queue, rate limit, or timeout, note the cluster setting
   name, its default value, and what happens when it's exceeded.

5. **Error behavior** — what errors can this stage produce, how are they
   classified (transient vs permanent), and how do they propagate to the
   next stage. Include retry counts, backoff parameters, and error markers
   (e.g. `errMarkSnapshotError`) that control retry behavior.

6. **Competing consumers** — if multiple subsystems share the same resource
   at this stage (e.g., snapshots and regular writes both consume disk
   bandwidth), note the contention point and how to distinguish them in
   metrics.

Write each stage so that it stands alone — the reader should be able to
jump to "Stage 4: Receiver Reservation" and understand what it does
without reading the previous stages. Cross-reference other stages by name
when needed.

---

## The Metrics, Grouped by What They Prove

IMPORTANT: Do NOT use markdown tables for metric groups. Tables render
poorly in Google Docs (columns collapse into flat text). Instead, use a
bullet-list format for each metric:

### Group 1: <What this group answers>

- `metric.name` (<type>) — <What it measures>. <Codebase context: what system event it represents, what a non-zero or elevated value implies for the operator, what it does NOT capture (if non-obvious), and measurement point.>

The codebase context MUST explain each metric the way MMA metrics
(`mma_metrics.go`) explain theirs — in terms of the system behavior, not as a
code-level trace. Specifically:
- **What system event it represents**: The operational situation that causes the number to change (e.g., "an external replica change completed successfully", not "incremented in function X")
- **What a non-zero or elevated value implies for the operator**: What should the engineer conclude?
- **What it does NOT capture**: Important exclusions that prevent misinterpretation
- **Measurement point**: Whether this is measured at the sender, receiver, gateway, leaseholder, or store level

Example:
- `range.snapshots.recv-failed` (counter) — Snapshot init errors on receiver. Number of incoming snapshot init messages that errored on the recipient store before data transfer begins. This includes rejections due to overlapping range descriptors, store draining, or stale replicas. Does NOT count reservation queue timeouts or errors during data transmission/application — those are tracked separately. A sustained non-zero rate indicates the receiver is consistently rejecting snapshots, pointing to descriptor conflicts or a draining node. Measured at the receiver store.

**Key ratios:**
<Derived metrics and what they infer>

### Group 2: ...
(continue for all relevant groups)

### Group N: Noise / Context (NOT the symptom, but needed to rule out)
<Metrics that look like the symptom but have different root causes>

---

## The Decision Procedure: Diagnosing <Symptom>

IMPORTANT: Do NOT use markdown tables for the decision procedure steps.
Tables render poorly in Google Docs (columns collapse into flat text).
Instead, use a bullet-list format for each check:

### Step 1: <First elimination>

- **<Check question>**: `metric.name` — <signal to look for> → <inference if signal matches>
- **<Check question>**: `metric.name` — <signal to look for> → <inference if signal matches>

**Conclusion:** <what to do based on the results above>

### Step 2: ...
(continue for all steps)

---

## Key Equations
<Named equations with metric formulas>

---

## Quick Reference: Complete Metric List

### <Category>
- `metric.name` (type, unit) — Operational description: what system event this tracks, what a non-zero/elevated value implies for the operator, and what it does NOT capture (if non-obvious). Include measurement point (sender/receiver/gateway/store).
(for every primary metric mentioned in the framework)

### Supporting Metrics
Metrics that provide additional context, per-type breakdowns, or secondary
signals. Not needed for the core decision procedure but useful for deeper
investigation.

#### <Category>
- `metric.name` (type) — Brief description. Measurement point.
(for every supporting metric found by research agents)
```

### Phase 3: Output

1. Display the framework to the user
2. Ask if they want it pushed to a Google Doc
3. If yes, use the `roachdev gdoc` toolchain:
   - Create new doc: `roachdev gdoc api -X POST '/documents' --body '{"title": "..."}'`
   - Convert markdown: `roachdev gdoc md2json --file /tmp/<file>.md`
   - Push: pipe md2json output to `roachdev gdoc api '/documents/<ID>:batchUpdate'`
   - Or add as a tab to existing doc if specified
4. After Google Doc handling (whether created or skipped), ask:
   **"Would you like me to generate a Datadog dashboard with these metrics?"**
5. If yes, use the `/datadog` skill's dashboard generation workflow:
   - Extract all metrics from the **Quick Reference: Complete Metric List** section
   - Write the YAML spec to `./<topic-slug>-dashboard-spec.yaml` and output the dashboard JSON to `./<topic-slug>-dashboard.json` (current working directory, not `/tmp`)
   - Use `roachdev datadog dashboards generate` to produce the dashboard JSON
   - See the `/datadog` skill's "Dashboard Generation" section for the YAML spec format and query construction rules

---

## The Proof Structure: Core Principles

Every framework MUST follow these principles:

### 1. Metrics are grouped by what they prove, not alphabetically
Each group answers a specific diagnostic question. The first group always addresses the most direct signal. Later groups address noise and competing explanations.

### 2. Noise elimination is explicit
For every symptom, there are metrics that LOOK like the symptom but have a different root cause. These must be listed and the framework must explain how to distinguish them. Examples:
- `sql.query.count` drops → could be workload drop (app stopped sending) OR closed-loop feedback (pool exhaustion). Disambiguate with `sql.bytesin`.
- `sys.gc.pause.percent` high → could be memory pressure OR CPU overload starving GC workers. Disambiguate with `sys.runnable.goroutines.per.cpu`.
- `queue.replicate.process.failure` high → could be snapshot timeout OR allocation failure. Disambiguate with `addreplica.error` vs `purgatory`.

### 3. Cross-layer triangulation is the proof
No single metric proves anything because each is contaminated by at least one system effect. Proof = agreement across independent measurement points:
- **Layer 1 (pgwire)**: `sql.conns`, `sql.bytesin` — app-controlled, purest signal
- **Layer 2 (SQL + DistSender)**: `sql.*.count`, `distsender.*` — gateway-side, mixed signal (retries inflate)
- **Layer 3 (Store)**: `rpc.method.*.recv`, `rebalancing.*` — leaseholder-side, execution signal
- **Corroboration only**: Latency metrics (`sql.exec.latency`, `sql.service.latency`) — never proof alone

### 4. Decision procedures are elimination-based
Each step either proves or eliminates a hypothesis. The procedure must be ordered from purest signal to most contaminated. Typical ordering:
1. Is the app even sending? (pgwire layer)
2. Did the workload change? (SQL ratios)
3. Did CRDB internals change? (KV layer ratios, AC, contention)
4. Is it a resource bottleneck? (CPU, memory, disk, network)
5. Is it a background job? (export, addsstable, gc)

### 5. Every metric has an operational explanation grounded in the codebase
For every metric in the framework, the reader must be able to answer: "What system event does this track, and what should I conclude from it?" Each metric entry must include:
- **What system event it represents** — the operational situation, not just a restatement of the metric name. Read the metric's `Help` field in its `metric.Metadata` definition and the surrounding code context.
- **What a non-zero or elevated value implies** — what should the engineer conclude or investigate next?
- **What it does NOT capture** — important exclusions that prevent misinterpretation (if non-obvious)
- **Measurement point** — sender vs receiver, gateway vs leaseholder, etc.

Follow the style of `mma_metrics.go` — descriptions that explain the operational meaning, when the event occurs, and what it implies for the operator. A metric without operational context is a label — it tells you what to search in Datadog but not what to conclude from it.

### 6. Every metric has a type annotation
Always indicate whether each metric is a gauge, counter, or histogram.

---

## Reference Frameworks

Existing frameworks built with this methodology (for reference and pattern matching):

| Framework | Topic | Key decision procedure |
|-----------|-------|----------------------|
| Workload Change Detection | Did the workload change? | 6-step: pgwire → query mix → query weight → key distribution → noise filter → AC vs workload |
| Network Latency Diagnosis | Is the network the problem? | 10-step: TCP RTT → gRPC RTT → client-server decomposition → noise elimination |
| RPC Deep Dive | What operations dominate? | 4-step: workload profile → before/during/after comparison → system-generated filter → SQL cross-validation |
| Memory Deep Dive | Where is memory going? | 6-step: is there pressure? → Go vs CGo → Go heap breakdown → Pebble breakdown → GOGC tuning → noise |
| Snapshot & Replication | Why is replicate queue failing? | 7-step: confirm snapshot cause → sender vs receiver → slow receiver → slow sender → saturation → network → periodicity |

Local framework files: `/tmp/*-framework.md`
