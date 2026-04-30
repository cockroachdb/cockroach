# Leader Lease Interval & Failover: Mental Model

This document explains how leader lease intervals and failover times work in
CockroachDB: what makes a leader lease valid, what happens when a leaseholder
crashes, how the stasis period interacts with shorter lease intervals, and what
knobs are available for tuning.

---

## Lease Validity

A leader lease is valid when:

1. The lease's Raft term matches the current Raft term
2. The Raft leader has fortified support from a quorum of stores
3. The current time is before the lease's effective expiration minus
   `MaxClockOffset` (the stasis period; default `--max-offset` is 500ms)

The effective expiration is:

```
expiration = max(lease.MinExpiration, LeadSupportUntil)
```

- `MinExpiration = now + FortificationGracePeriod` (set at lease proposal time)
- `LeadSupportUntil` = min support expiration across all fortified quorum
  members (the "Quorum Support Expiration")

During steady state, the leader sends store liveness heartbeats (default: every
1s). Each heartbeat grants a fixed duration of support (default: 3s). The
leader tracks support from each follower and computes `LeadSupportUntil` as
the minimum across a quorum. As long as heartbeats keep flowing, support is
continuously extended and the lease remains valid.

Followers **cannot** directly evaluate leader lease validity — they don't
know `LeadSupportUntil`. Instead:
- If `now < lease.MinExpiration` → lease is valid
- If `now >= lease.MinExpiration` and a leader at a **higher** term exists →
  lease is expired
- Otherwise → `ERROR`; must wait for a new election

---

## Failover

### The sequential model

Store liveness and Raft elections are **sequential, not parallel**. While a
follower is fortifying a leader, its election timeout counter
(`electionElapsed`) is **frozen at zero**. The follower will not campaign.

When support expires, the follower calls `deFortify()` which:
1. Resets fortification state
2. Jumps `electionElapsed` to `electionTimeout`, skipping the base timeout
3. Only the jitter (0 to `ELECTION_TIMEOUT_JITTER_TICKS`) remains

Two additional gates:
- A candidate must have **quorum support** in store liveness to campaign
- PreVote requests are **rejected** by followers still fortifying a leader

### Default failover timing (from `pkg/base/config.go`)

Each step is sequential. RTTs assumed 10ms-400ms (see `COCKROACH_NETWORK_TIMEOUT`).

```
                                                            Min       Max
1. Store Liveness heartbeat offset (0-1 heartbeat interval)[-1.00s - 0.00s]
2. Store Liveness expiration (constant)                    [ 3.00s - 3.00s]
3. Store Liveness withdrawal (0-1 withdrawal interval)     [ 0.00s - 0.10s]
4. Raft election timeout jitter (only jitter, not base)    [ 0.00s - 2.00s]
5. Election (3x RTT: prevote, vote, append)                [ 0.03s - 1.20s]
6. Lease acquisition (1x RTT: append)                      [ 0.01s - 0.40s]
Total                                                      [ 2.04s - 6.70s]
```

Step 4 says "jitter" not "election timeout" — the base 2s election timeout is
skipped because `deFortify()` fast-forwards past it.

---

## The Stasis Period

A lease has four possible states: VALID, UNUSABLE, EXPIRED, and PROSCRIBED.
The stasis period is the window at the tail end of a lease's lifetime where
the lease hasn't technically expired, but can no longer serve requests.

### Why stasis exists

Without a stasis period, this linearizability violation is possible during a
lease change:

1. New leaseholder commits a write at timestamp just past the old lease's
   expiration
2. A client reads that write, then issues another read that hits a slow
   coordinator assigning a timestamp covered by the old lease
3. The old leaseholder (which hasn't yet processed the lease change) serves
   the read — failing to see the write

The stasis period prevents this by making the old lease UNUSABLE for the last
`MaxClockOffset` before expiration. No requests can be served in this window,
so there's no risk of the old leaseholder serving stale reads.

### How it's computed

```
stasis_start = expiration - MaxClockOffset
```

- **`--max-offset`** (default 500ms): The maximum tolerable clock skew across
  the cluster. Set at node startup. If a node detects its clock offset exceeds
  `0.8 × max-offset`, it self-terminates.
- **`expiration`**: For leader leases, this is
  `max(MinExpiration, LeadSupportUntil)`.

The lease is:
- **VALID** when `request_timestamp < stasis_start`
- **UNUSABLE** when `stasis_start <= request_timestamp < expiration`
- **EXPIRED** when `expiration <= now`

### Steady-state impact

During normal operation, `LeadSupportUntil` advances with each heartbeat.
The usable window after a heartbeat extends support is:

```
usable_window = support_duration - MaxClockOffset
```

As long as the next heartbeat arrives before the usable window closes, the
lease remains VALID. The **slack** — how late a heartbeat can be before stasis
is reached — is:

```
slack = usable_window - heartbeat_interval
      = support_duration - MaxClockOffset - heartbeat_interval
```

| Config | Support | MaxOffset | HB Interval | Usable | Slack | Stasis % |
|--------|---------|-----------|-------------|--------|-------|----------|
| Default | 3000ms | 500ms | 1000ms | 2500ms | 1500ms | 17% |
| Aggressive | 1000ms | 500ms | 200ms | 500ms | 300ms | 50% |
| Aggressive + low offset | 1000ms | 100ms | 200ms | 900ms | 700ms | 10% |

When tuning support duration down, `MaxClockOffset` becomes a proportionally
larger tax. Reducing `--max-offset` is essential to keep the stasis fraction
reasonable.

---

## Knobs Reference

### Store Liveness

| Env Var | Default | Controls |
|---------|---------|----------|
| `COCKROACH_STORE_LIVENESS_HEARTBEAT_INTERVAL` | 1s | How often stores request/extend support. |
| `COCKROACH_STORE_LIVENESS_SUPPORT_DURATION` | 3s | Duration of support granted per heartbeat. |
| `COCKROACH_STORE_LIVENESS_SUPPORT_EXPIRY_INTERVAL` | 100ms | How often the support manager checks for withdrawal. |
| `COCKROACH_STORE_LIVENESS_IDLE_SUPPORT_FROM_INTERVAL` | 1m | Cleanup interval for idle support-from entries. |
| `COCKROACH_RAFT_FORTIFICATION_GRACE_PERIOD` | 3s | Minimum validity of a new leader lease (time to fortify). |

### Raft

| Env Var | Default | Real Time | Controls |
|---------|---------|-----------|----------|
| `COCKROACH_RAFT_TICK_INTERVAL` | 500ms | — | Base tick resolution. All Raft timing = ticks × this. |
| `COCKROACH_RAFT_HEARTBEAT_INTERVAL_TICKS` | 2 | 1s | Ticks between leader heartbeats. |
| `COCKROACH_RAFT_ELECTION_TIMEOUT_TICKS` | 4 | 2s | Base election timeout (skipped during fortified failover). |
| `COCKROACH_RAFT_ELECTION_TIMEOUT_JITTER_TICKS` | 4 | 0-2s | Random jitter added to election timeout. |
| `COCKROACH_RAFT_REPROPOSAL_TIMEOUT_TICKS` | 6 | 3s | Ticks before a command is reproposed. |
| `COCKROACH_RAFT_ENABLE_CHECKQUORUM` | true | — | Leader steps down without quorum contact. |
| `COCKROACH_RAFT_TICK_SMEAR_INTERVAL` | 1ms | — | Smears tick processing to avoid thundering herd. |

### Lease

| Env Var | Default | Controls |
|---------|---------|----------|
| `COCKROACH_RANGE_LEASE_DURATION` | 6s | Range lease duration. |
| `COCKROACH_RANGE_LEASE_RENEWAL_FRACTION` | 0.5 | Fraction of duration at which proactive renewal starts (= 3s). |

### Network

| Env Var | Default | Controls |
|---------|---------|----------|
| `COCKROACH_NETWORK_TIMEOUT` | 2s | Single roundtrip timeout (RTT + RTO + margin). |
| `COCKROACH_RPC_DIAL_TIMEOUT` | 4s | Dial timeout (2× NetworkTimeout). |
| `COCKROACH_RPC_HEARTBEAT_TIMEOUT` | 6s | RPC heartbeat timeout (3× NetworkTimeout). |

### Startup flags

| Flag | Default | Controls |
|------|---------|----------|
| `--max-offset` | 500ms | Maximum tolerable clock skew. Determines the stasis period. |

### Constraints between knobs

- **Support duration > heartbeat interval**: Otherwise support lapses during
  normal operation. Default: 3s > 1s (3× ratio).
- **Fortification grace period ≈ support duration**: Grace period gives a new
  leader time to establish fortification. Default: both 3s.
- **Election timeout > heartbeat interval**: Prevents spurious elections.
  Default: 2s > 1s. (Base timeout is skipped during failover, but matters for
  the non-fortified path.)
- **Usable window > heartbeat interval**: Otherwise the lease enters stasis
  before the next heartbeat can extend it. See the stasis period section.

---

## Example: Tuning for ≤2s Failover (US-Only Cluster)

### Network assumptions

A cluster spanning us-east, us-central, and us-west. Typical RTTs:

| Route | RTT |
|-------|-----|
| us-east ↔ us-central | ~20-30ms |
| us-central ↔ us-west | ~40-50ms |
| us-east ↔ us-west | ~60-80ms |

Worst-case RTT: **80ms**. This is far below the global worst case (~600ms p99)
that the default `COCKROACH_NETWORK_TIMEOUT` is designed for.

### Network timeouts

The defaults are sized for global clusters (600ms p99 RTT + 900ms RTO + 500ms
margin = 2s). For a US-only cluster we can tighten these significantly.

`NetworkTimeout` must account for a round trip plus one TCP retransmit. The
TCP Retransmission Timeout (RTO) is how long the OS waits before resending a
lost packet. On Linux, RTO ≈ 1.5 × smoothed RTT, with a floor of 200ms.

For our cluster: RTT = 80ms, so RTO = max(200ms, 1.5 × 80ms) = 200ms. That
gives RTT + RTO = 280ms. We round up to 500ms for safety margin.

| Env Var | Default | Tuned | Reasoning |
|---------|---------|-------|-----------|
| `COCKROACH_NETWORK_TIMEOUT` | 2s | 500ms | 80ms RTT + 200ms RTO + 220ms margin. |
| `COCKROACH_RPC_DIAL_TIMEOUT` | 4s | 1s | 2× NetworkTimeout (TCP + TLS handshake). |
| `COCKROACH_RPC_HEARTBEAT_TIMEOUT` | 6s | 1.5s | 3× NetworkTimeout. |

Note: these also affect the `SupportWithdrawalGracePeriod` (used after node
restart to prevent premature support withdrawal), which is derived as
`PingInterval + RPCHeartbeatTimeout + DialTimeout + NetworkTimeout`. With
defaults this is 13s; with tuned values it drops to ~4s.

### Failover knobs

Working backwards from the 2s target, using the sequential failover formula
with 80ms worst-case RTT:

```
Max = SL_heartbeat_offset + SL_support + SL_withdrawal + election_jitter + 3×RTT + RTT
    = 0 + SL_SUPPORT + SL_EXPIRY + JITTER_TICKS × TICK_INTERVAL + 4 × 80ms
```

We need `SL_SUPPORT + SL_EXPIRY + JITTER ≤ 2.0 - 0.32 = 1.68s`.

| Env Var | Default | Tuned | Reasoning |
|---------|---------|-------|-----------|
| `COCKROACH_RAFT_TICK_INTERVAL` | 500ms | 200ms | Finer resolution for tighter timeouts. |
| `COCKROACH_RAFT_HEARTBEAT_INTERVAL_TICKS` | 2 | 2 (= 400ms) | Unchanged in ticks. 400ms > 80ms RTT, plenty of headroom. |
| `COCKROACH_RAFT_ELECTION_TIMEOUT_TICKS` | 4 | 4 (= 800ms) | Unchanged in ticks. 2× heartbeat ratio preserved. Skipped during fortified failover. |
| `COCKROACH_RAFT_ELECTION_TIMEOUT_JITTER_TICKS` | 4 | 2 (= 400ms) | Reduced — this directly contributes to failover time. |
| `COCKROACH_STORE_LIVENESS_HEARTBEAT_INTERVAL` | 1s | 200ms | Tighter heartbeat. 200ms > 80ms RTT for round-trip headroom. |
| `COCKROACH_STORE_LIVENESS_SUPPORT_DURATION` | 3s | 1s | 5× heartbeat interval. Longer support dilutes the stasis tax. |
| `COCKROACH_STORE_LIVENESS_SUPPORT_EXPIRY_INTERVAL` | 100ms | 50ms | Faster withdrawal detection. |
| `COCKROACH_RAFT_FORTIFICATION_GRACE_PERIOD` | 3s | 1s | Matches support duration. |

### Failover timing with tuned values

```
                                                              Min       Max
1. Store Liveness heartbeat offset (0-1 heartbeat interval) [-0.20s - 0.00s]
2. Store Liveness expiration (constant)                     [ 1.00s - 1.00s]
3. Store Liveness withdrawal (0-1 withdrawal interval)      [ 0.00s - 0.05s]
4. Raft election timeout jitter (only jitter, not base)     [ 0.00s - 0.40s]
5. Election (3x RTT: prevote, vote, append)                 [ 0.06s - 0.24s]
6. Lease acquisition (1x RTT: append)                       [ 0.02s - 0.08s]
Total                                                       [ 0.88s - 1.77s]
```

Worst case **1.77s**, well within the 2s target.

### Stasis with tuned values

With 1s support duration, the stasis period must be tuned down alongside it.
On AWS/GCP in a US-only deployment, clock offsets typically stay well under
100ms — NTP synchronisation across US regions is tight. We set
`--max-offset=100ms`.

| Flag | Default | Tuned | Reasoning |
|------|---------|-------|-----------|
| `--max-offset` | 500ms | 100ms | AWS/GCP US-only offsets are well under 100ms. |

With `--max-offset=100ms` and 1s support duration:

```
usable_window = support_duration - MaxClockOffset = 1000ms - 100ms = 900ms
slack          = usable_window - heartbeat_interval = 900ms - 200ms = 700ms
stasis %       = 100ms / 1000ms = 10%
```

This is actually better than the default configuration (17% stasis). With
700ms of slack, **three consecutive heartbeats** can be missed entirely before
the lease enters stasis. The 200ms heartbeat interval combined with the low
max-offset means the stasis period is a negligible fraction of the support
window, and steady-state operation has generous headroom against heartbeat
jitter.

### Constraints check

| Constraint | Required | Tuned | Status |
|------------|----------|-------|--------|
| SL support > SL heartbeat | > 1× | 1000ms / 200ms = 5× | Better than default (3×) |
| Grace period ≈ support duration | ≈ equal | 1s = 1s | Matched |
| Election timeout > Raft heartbeat | 2× ratio | 800ms / 400ms = 2× | Same as default |
| SL heartbeat > worst RTT | headroom | 200ms > 80ms | 2.5× headroom |
| Raft heartbeat > worst RTT | headroom | 400ms > 80ms | 5× headroom |
| Usable window > SL heartbeat | headroom | 900ms > 200ms (max-offset=100ms) | 700ms slack |
