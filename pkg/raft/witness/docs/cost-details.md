<div style="background: #fff8e1; border-left: 4px solid #f5a623; padding: 1rem 1.25rem; margin: 1.5rem 0; border-radius: 0 4px 4px 0; color: #5d4037;">
<strong>Heads up:</strong> This page is mostly a data and methodology dump
kept around for reference. It is not intended as readable prose. For the
high-level cost story, start at the <a href="/introduction">introduction</a>.
</div>

## Headline

For a small uniform-write KV workload (256-byte values, ~5.7k ops/s,
~1.46 MiB/s of committed write goodput) on an 8-vCPU n2-standard-8 node:

- Total cost of replicating to **2 followers** on the leader: **~16 pp** of
  leader CPU (57% with replication vs 41% without — a ~39% relative bump).
- Per-follower outbound-replication cost on the leader (linear): **~8 pp**,
  i.e. ~14% of total leader CPU at this workload.
- **Witness prediction**: replacing one of the two followers with a true
  witness (no log replication, no MsgApp) would drop the leader from ~57% →
  **~49%** at the same QPS.

The "stop one follower" experiment under-shoots that prediction (it shows
only a ~5 pp drop instead of the predicted ~8 pp). Several plausible
contributors to the gap; they aren't disentangled here. See
[Caveats](#caveats--known-confounds).

| config | n1 (leader) CPU | n2 | n3 | actual QPS | n1 p50 / p99 |
|---|---|---|---|---|---|
| RF=3, both followers up | ~57% | ~19% | ~19% | ~5685 | 1.0 / 1.7 ms |
| RF=3, n3 stopped (1 active follower + dead peer) | ~52% | ~18% | 0 | ~5380 | 1.0 / 2.6 ms |
| RF=1 (no replication) | ~41% | ~1% | ~1% | ~5862 | 0.7 / 2.5 ms |

All three rows: `--concurrency 10 --max-rate 6000`, identical workload binary.

### Direct measurement via 1 voter + 1 non-voter

The earlier "per-follower ≈ 8 pp" headline came from a *differential* (RF=3
vs RF=1, divided by 2). A direct measurement is possible by stepping
through 2 → 1 → 0 followers using the carve-out for `num_replicas=2,
num_voters=1` (1 voting replica on the leader + 1 non-voting follower).
Each flavor was run on a fresh empty database (`db3v256`, `db1v1n256`,
`db1v256`) with identical 10 splits, after 2 minutes of warmup:

| flavor | n1 (leader) CPU | n2 CPU | n3 CPU | n1 send | n2 recv | actual QPS | p50 / p99 |
|---|---|---|---|---|---|---|---|
| db3v256 (RF=3, 3 voters)        | ~52% | ~19% | ~20% | 8.20 MB/s | 3.82 MB/s | ~5757 | 1.0 / 2.5 ms |
| db1v1n256 (1 voter + 1 non-voter) | ~45% | ~17% | ~1%  | 4.32 MB/s | 3.78 MB/s | ~5611 | 0.8 / 2.6 ms |
| db1v256 (RF=1, no replication)  | ~38% | ~1%  | ~1%  | 0.63 MB/s | n/a       | ~5619 | 0.7 / 2.8 ms |

Zone config for the middle row:

```sql
ALTER TABLE db1v1n256.kv CONFIGURE ZONE USING
  num_replicas = 2, num_voters = 1,
  constraints       = '{"+rack=0": 1, "+rack=1": 1}',
  voter_constraints = '{"+rack=0": 1}',
  lease_preferences = '[[+rack=0]]';
```

**Leader-side win: ~7 pp per follower removed.** The drop is exactly
linear across the three steps:

- 3 voters → 1 voter + 1 non-voter: **−7 pp** (52 → 45)
- 1 voter + 1 non-voter → 1 voter only: **−7 pp** (45 → 38)

So replicating to one additional follower costs the leader ~7 pp of CPU
at this workload — equivalently ~14% relative to the 2-follower baseline
or ~18% relative to the 0-follower baseline. This is a **direct**
measurement of the per-follower replication cost on the leader (not the
linear extrapolation), and it lines up with the ~8 pp differential
estimate from the original RF=3 vs RF=1 comparison.

Two corroborating observations from the same run:

- **Non-voters look identical to voters from the leader's perspective.**
  In `db1v1n256`, n2 (a non-voter) sits at ~17% CPU and ~3.8 MB/s
  inbound — basically indistinguishable from the same node when it was
  a voting follower in `db3v256`. Non-voters receive the full raft log;
  they just don't gate quorum.
- **Leader send halves cleanly with each follower removed**: 8.20 →
  4.32 → 0.63 MB/s. The residual ~0.6 MB/s at RF=1 is pgwire response
  traffic to the workload generator on n4.
- **Latency p50 sits between RF=3 and RF=1** at 0.8 ms (vs 1.0 ms /
  0.7 ms). The leader still does the work of building/sending the
  raft message to the non-voter, but doesn't *wait* on the ack, so it
  saves the quorum round-trip but pays the per-op processing.

A real witness — which receives no log appends — would presumably save
this final ~7 pp as well, putting witness-replacement savings closer to
~7 pp per follower.

### Same experiment at 1024 B value size

Repeated with `--min-block-bytes 1024 --max-block-bytes 1024` (same
`--concurrency 10 --max-rate 6000`), to see how block size shifts the cost
shape:

| config | n1 (leader) CPU | n2 | n3 | actual QPS | n1 p50 / p99 |
|---|---|---|---|---|---|
| RF=3, both followers up | ~54% | ~22% | ~21% | ~5466 | 1.0 / 2.6 ms |
| RF=3, n3 stopped         | ~43% | ~19% | 0 | ~4377 | 1.2 / 5.0 ms |
| RF=1 (no replication)    | ~42% | ~1% | ~1% | ~5566 | 0.8 / 2.6 ms |

**Per-follower CPU savings on the leader: ~6 pp** (54 − 42 = 12 pp total /
2 followers). Slightly *less* than the ~8 pp at 256 B, because per-op CPU
on the leader barely budges with block size at this range — the bigger
write moves more bytes but doesn't add proportional CPU. Witness
prediction: ~54% → ~48% if one follower were a witness.

The n3-stopped row at 1024 B drops by 11 pp absolute (54 → 43) but its QPS
also drops 20% (5466 → 4377), much more than at 256 B. Per-op CPU is
basically unchanged from the RF=3 baseline (~0.0099 vs ~0.0098 %·s/op),
so the absolute drop here is almost entirely "less work because fewer
ops/s", not real savings — Phase A is not informative at 1024 B.

Network at the follower (RF=3 baseline, 1024 B):

| node | recv MB/s | send MB/s |
|---|---|---|
| n1 (leader)   | 9.0  | 16.1 |
| n2 (follower) | 7.85 | 1.51 |
| n3 (follower) | 7.83 | 1.44 |

Goodput at this baseline: 5466 ops/s × 1024 B ≈ **5.6 MiB/s**. Per byte
of goodput, a follower sees:

- Inbound: ~1.40× goodput (down from 2.66× at 256 B)
- Outbound: ~0.27× goodput (down from 1.09× at 256 B)
- Total follower NIC: ~1.67× goodput (down from 3.75× at 256 B)

Larger blocks amortize per-write framing/header overhead better, so the
on-the-wire amplification drops sharply. CPU savings shrink slightly,
but network savings from a witness scale roughly linearly with block size
(eliminating the ~8 MB/s outbound the leader otherwise spends on that
follower).

### Same experiment at 4096 B value size

OLTP-relevant point: a 4 KiB raft entry roughly corresponds to a typical
multi-row transaction (e.g. a TPC-C NewOrder commit).

| config | n1 (leader) CPU | n2 | n3 | actual QPS | n1 p50 / p99 |
|---|---|---|---|---|---|
| RF=3, both followers up | ~57% | ~20% | ~21% | ~4636 | 1.2 / 6.0 ms |
| RF=3, n3 stopped         | ~49% | ~22% | 0 | ~2200–3500 (unstable) | 2.2 / 16.3 ms |
| RF=1 (no replication)    | ~47% | ~1% | ~1% | ~4702 | 1.0 / 7.1 ms |

**Per-follower CPU savings on the leader: ~5 pp** (57 − 47 = 10 pp / 2).
Witness prediction: ~57% → ~52%.

Phase A (n3-stopped) at 4 KiB was very unstable: throughput collapsed to
~2.2 k ops/s with p99 latency tripling (6 ms → 16 ms). The dead peer's
overhead on the leader is large enough at this block size that the system
can't sustain its previous offered rate. Phase B (RF=1) is the only
trustworthy data point here.

Network at the follower (RF=3 baseline, 4 KiB):

| node | recv MB/s | send MB/s |
|---|---|---|
| n1 (leader)   | 23.6 | 46.1 |
| n2 (follower) | 22.6 | 1.50 |
| n3 (follower) | 22.5 | 1.44 |

Goodput: 4636 ops/s × 4096 B ≈ **19.0 MiB/s**. Per byte of goodput, a
follower sees:

- Inbound: ~1.19× goodput (256 B: 2.66×; 1024 B: 1.40×)
- Outbound: ~0.08× goodput (256 B: 1.09×; 1024 B: 0.27×)
- Total follower NIC: ~1.27× goodput (256 B: 3.75×; 1024 B: 1.67×)

Amplification is converging toward 1× as block size grows.

### Block-size sweep summary

| value size | RF=3 n1 CPU | RF=1 n1 CPU | per-follower savings (pp) | per-op CPU drop (RF=3 → RF=1) | follower NIC / goodput |
|---|---|---|---|---|---|
| 256 B    | 57% | 41% | **8 pp** | 30% | 3.75× |
| 1024 B   | 54% | 42% | **6 pp** | 24% | 1.67× |
| 4096 B   | 57% | 47% | **5 pp** | 19% | 1.27× |

Key trends as values grow:

- **Per-follower CPU savings shrink** — 8 → 6 → 5 pp. The leader's per-op
  cost is dominated by per-op overhead (proto encoding, raft bookkeeping,
  pgwire); larger blocks add CPU on the leader's own write path that a
  witness wouldn't avoid, so the *absolute* savings from skipping
  outbound replication shrink in proportion.
- **Network savings at the leader's NIC scale roughly linearly with block
  size** — eliminating one follower's MsgApp stream saves ~4 MB/s at
  256 B, ~8 MB/s at 1024 B, ~23 MB/s at 4096 B.
- **The "stop one follower" experiment becomes less useful** as block
  size grows — at 4 KiB it destabilizes the workload entirely.

---

## Model

The full CPU cost of a write includes the work handling the SQL connection,
forming KV batches, sending to the leaseholder, evaluating the command,
proposing the command, replicating it to the leaseholder's and followers'
logs, and applying it to each replica's state machine (including compaction
work). We can model the per-write CPU cost across the cluster as a function
of the replication factor `n` in the presence of `v` voters and `w`
witnesses (i.e. `n=v+w`), with three parameters:

```
total_cluster_cpu(v, w) = L              # unreplicated work (SQL, eval, ...)
                        + v * A          # log append + state-machine apply
                                         # on leader and each follower
                        + (v-1) * S      # leader sending log to each follower
```

- `S` = leader's per-follower **S**end cost (MsgApp send, ack tracking)
- `A` = per-replica log-**A**ppend + state-machine-apply cost (the leader pays
  this for its own write too - we neglect that the leader does not need to
  send to itself as this is hopefully not dominant)
- `L` = **L**eader-only constant: SQL serving, KV batch handling, command
  evaluation, proposal building, raft leadership — independent of `n`

Fitting the model to the experimental data above (uniform-write KV at 256 B,
1 KiB, and 4 KiB; ~4600–5700 ops/s on 8-vCPU nodes):

| parameter               | value (pp of host-normalized CPU)         |
|-------------------------|-------------------------------------------|
| `S` (per-follower send) | ~8 (256 B) → ~5 (4 KiB), decreasing       |
| `A` (per-replica apply) | ~18 (256 B) → ~20 (4 KiB), slowly growing |
| `L` (leader-only)       | ~22–28, growing at large block sizes      |

A few consequences fall out:

- **A leader is roughly 2× as expensive as a follower in CPU terms**, even
  after subtracting the work the leader does to send the log to followers.
  The extra cost is the leader-only constant `L`.
- **A logless witness recovers the full per-follower replication cost
  `S + A`** — `S` on the leader (no MsgApp to build) and `A` on the
  replaced node (no log, no apply). At RF=`v`, replacing one full follower
  with a logless witness eliminates `1/(v-1)` of the cluster's replication
  CPU — that's **50% at RF=3, 25% at RF=5**.
- **A full-log witness saves only the apply portion of `A`** — the leader
  still sends the log (`S`) and the witness still appends it; only the
  state-machine apply is skipped. Roughly half of `A` in our model.
- **Half of total cluster CPU at RF=3 is the leader-only constant plus
  leader's own apply** (`L + A ≈ 40 pp` out of ~91 pp); the other half is
  the work caused by the two followers (`2(S+A) ≈ 48 pp`). Witnesses can
  only attack the latter.

---

## Cluster

4× `n2-standard-8` (8 vCPU) on GCE, us-east1-d. n1–n3 run cockroach
(master, edge build), n4 runs the workload.

```
roachprod create tobias-wtns -n 4 --gce-machine-type n2-standard-8
roachprod stage tobias-wtns cockroach
roachprod stage tobias-wtns workload
roachprod opentelemetry-start tobias-wtns:1-3 --datadog-api-key=$DD_API_KEY
roachprod start tobias-wtns:1-3 --racks 3
```

`--racks 3` puts each node in its own rack (n1→rack=0, n2→rack=1,
n3→rack=2). Used downstream to pin the lease.

## Schema and zone config (RF=3 baseline)

```
roachprod run tobias-wtns:4 -- ./cockroach workload init kv --splits 10 {pgurl:1}
```

```sql
ALTER TABLE kv.kv CONFIGURE ZONE USING
  num_replicas = 3,
  constraints = '{"+rack=0": 1, "+rack=1": 1, "+rack=2": 1}',
  lease_preferences = '[[+rack=0]]';
```

This forces every range to have replicas {n1, n2, n3} with the leaseholder
on n1. Verified via:

```sql
SELECT lease_holder, count(*)
FROM [SHOW RANGES FROM TABLE kv.kv WITH DETAILS] GROUP BY 1;
-- expect: 1 | 22
```

## Workload

256-byte uniform writes, run from n4, in tmux:

```
roachprod run tobias-wtns:4 -- "tmux new-session -d -s kv \
  './cockroach workload run kv \
     --read-percent 0 \
     --min-block-bytes 256 --max-block-bytes 256 \
     --concurrency 10 \
     --max-rate 6000 \
     --duration 30m \
     {pgurl:1}'"
```

Two important workload parameters:

- `--concurrency 10`: small enough that n1 lands in the 50–70% CPU band
  with RF=3 (concurrency 64 saturated n1 at ~91%, 24 at ~88%, 12 at ~74%).
- `--max-rate 6000`: rate-cap that all three configurations can sustain.
  Picked after confirming the n3-stopped configuration naturally tops out
  near 8000 ops/s — 6000 leaves margin so QPS is not the bottleneck.
  Actual achieved ranges 5380–5862 ops/s across configs (latency-bound
  at concurrency 10 even under the cap).

## Measurement

Per-host CPU via Datadog (OpenTelemetry collector exporting to us5):

```
roachdev datadog metrics query \
  "avg:cockroachdb.sys.cpu.host.combined.percent_normalized{cluster:tobias-wtns} by {host}" \
  --from now-1m
```

Wait ~90 s after each configuration change for steady state before reading.

## Configurations

### RF=3 baseline

Zone config above. Restart workload, wait 90 s, read CPU.

### Stop one follower (Phase A)

```
roachprod stop tobias-wtns:3
```

(Restart workload — connections established while n3 was down get into a
degraded state. Restarting the workload after each topology change is
mandatory for clean numbers. Restore with `roachprod start tobias-wtns:3
--racks 3`.)

### RF=1 differential (Phase B)

```sql
ALTER TABLE kv.kv CONFIGURE ZONE USING
  num_replicas = 1, num_voters = 1,
  constraints       = '{"+rack=0": 1}',
  voter_constraints = '{"+rack=0": 1}',
  lease_preferences = '[[+rack=0]]';
```

Wait ~90 s for downreplication; verify with:

```sql
SELECT replicas, lease_holder, count(*)
FROM [SHOW RANGES FROM TABLE kv.kv WITH DETAILS]
GROUP BY 1, 2;
-- expect: {1} | 1 | 22
```

Restore with the RF=3 zone config above.

## Things that did *not* work

- **`num_replicas = 2`**: rejected by zone validation
  ("at least 3 replicas are required for multi-replica configurations",
  `pkg/config/zonepb/zone.go:404`). Voter/non-voter splits don't help
  because non-voters still receive raft log replication.
- **Stop n1, hope the allocator drops to RF=2 on {n2,n3}**: the allocator
  prefers `ReplaceDeadVoter` over `RemoveDeadVoter` and gives up when no
  live target exists; ranges stay `{1,2,3}` indefinitely.
  `cockroach node decommission 1` also fails for the same reason
  ("0 of 2 live stores are able to take a new replica"). Lowering
  `server.time_until_store_dead = '1m15s'` did not help.
- **`roachprod pprof`** silently produced no output file when
  the cluster was started with the default `--secure` (likely a
  scheme/cert mismatch in the http client path). Worked around with:

  ```
  curl -o cpu.pprof "http://tobias-wtns-0003.roachprod.crdb.io:26258/debug/pprof/profile?seconds=30"
  ```

## Caveats / known confounds

- **Per-op vs per-second comparison**: the n3-stopped config can't sustain
  the same QPS as RF=3 at the same concurrency (latency goes up because
  raft batches wait on a dead peer before declaring it
  unreachable). Pin `--max-rate` low enough that none of the configs are
  rate-bound.
- **Phase A under-shoots Phase B's prediction**: Phase A (stop one
  follower) shows ~5 pp drop on the leader; Phase B (RF=1 differential)
  predicts ~8 pp savings per follower. ~3 pp gap, source not isolated.
  Plausible contributors, in no particular order:
  - Raft tracker / probe traffic toward the dead replica still costs
    something on the leader.
  - Raft log truncation is gated on the slowest follower's match index,
    so a stopped peer can pin the on-disk log longer; that has knock-on
    effects on Pebble (compaction work, larger memtable / sstable sets,
    block cache pressure) that show up as leader CPU.
  - Range-level circuit breakers / unreachable bookkeeping run more
    work when a peer is unhealthy.
  - Workload latency profile changes (p99 went from 1.7 → 2.6 ms),
    which shifts where in the per-op pipeline CPU lands.

  A real witness scenario would avoid most of these — but the magnitude
  of each contribution above is not measured here.
- **Linear extrapolation**: "per-follower cost" is computed as
  `(RF=3 leader CPU − RF=1 leader CPU) / 2`. Reasonable for two identical
  followers on identical hardware; would not generalize across
  heterogeneous setups.
- **Small workload**: 256-byte uniform writes at ~1.5 MiB/s. Replication
  cost shape will differ for larger blocks, batched txns, or read-heavy
  mixes.

## Network traffic at the follower

Measured at the same RF=3, `--max-rate 6000` baseline (~5685 ops/s, payload
goodput ≈ **1.46 MiB/s**). Host-level NIC counters (whole machine, but the
non-CRDB traffic on a fresh roachprod node is negligible):

| node | recv MB/s | send MB/s |
|---|---|---|
| n1 (leader)   | 5.13 | 8.21 |
| n2 (follower) | 3.88 | 1.59 |
| n3 (follower) | 3.83 | 1.50 |

Per byte of payload goodput, a follower sees:

- **Inbound (raft log appends from the leader): ~2.66× goodput**
- **Outbound (raft acks back to the leader): ~1.09× goodput**
- **Total follower NIC: ~3.75× goodput**

Translation: **1 MiB/s of committed-write goodput at the leader produces
~2.66 MiB/s inbound and ~1.09 MiB/s outbound at each follower.**

The 2.66× inbound multiplier is consistent with each 256 B value, once
wrapped in a CRDB write batch, raft entry header, command metadata, and
gRPC framing, landing at ~680–700 B on the wire.

Sanity check on the leader: send ≈ 2 × follower recv + pgwire response
(8.21 ≈ 2 × 3.86 + 0.49 → checks out).

### Reproducing the network measurement

```
roachdev datadog metrics query \
  "sum:cockroachdb.sys.host.net.recv.bytes{cluster:tobias-wtns} by {host}.as_rate()" \
  --from now-2m

roachdev datadog metrics query \
  "sum:cockroachdb.sys.host.net.send.bytes{cluster:tobias-wtns} by {host}.as_rate()" \
  --from now-2m
```

These metrics report whole-host NIC throughput in bytes per second.

## Network savings from witnesses

While intra-AZ traffic is usually free, traffic between availability zones
and regions is typically not. If we assume that the SQL gateway is
colocated with the leaseholder (a common case), the network usage (and
thus cost) of the write workload is dominated by replication traffic from
the leaseholder to the followers. Replacing one full voter with a logless
witness reduces this traffic by `1/(n-1)`, i.e. **50%** at n=3 and **25%**
at n=5. A full-log witness still receives the full log and so saves no
network.

## Disk throughput and usage

The combined disk throughput of a follower is the sum of its log and state
machine throughput (including compactions and truncation work). There is a
spread between goodput (payload supplied to the KV layer) and throughput
(bytes written to disk) that is highly dependent on the workload but
empirically is at least 2-3x for small values, and can increase 10-20x
from that baseline for uniform writes on a large LSM.

Either way, replacing a voter with a logless witness reduces both disk
throughput and disk usage by `1/n`, i.e. **33%** at n=3 and **20%** at
n=5. A full-log witness still appends the log but skips the state-machine
apply; since state-machine work (apply, compaction, stored bytes) dominates
log work for typical CRDB workloads, full-log savings on these dimensions
land close to the logless figures.

## Cleanup

```
roachprod destroy tobias-wtns
```
