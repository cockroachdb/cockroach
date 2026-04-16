# No-Linger Batching Sink: Prototype Gaps

This document tracks what the prototype does NOT yet implement relative
to the design docs. These are known gaps, not bugs.

## 1. No Admission Pacer

The design calls for an `admission.Pacer` in `addRow` to pace the
producer before adding to the buffer. The prototype skips this entirely.
The current `batchingSink` swallows the pacer error (acknowledged as
wrong in a TODO), so parity is arguably met, but the design intended to
fix this by returning the pacer error from `addRow` to `EmitRow`.

**To do**: Add `pacerFactory` parameter to `makeNoLingerSink`, call
`pacer.Pace(ctx)` at the top of `addRow`, return error to `EmitRow`.

## 2. No Buffer Bound / Backpressure

The design specifies a bounded pending buffer (event count or byte
limit) where `addRow` blocks via `cond.Wait` when the buffer is full.
The prototype's buffer is unbounded — `addRow` only blocks during flush.

Without a bound, a slow sink with fast ingestion could accumulate
unbounded memory in the pending buffer before backpressure kicks in.
In practice, the changefeed's KV feed and memory quota provide an
upstream bound, but the sink-level bound is still important for
predictable memory usage.

**To do**: Add `maxBufferedEvents` or `maxBufferedBytes` to
`pendingBuffer`. In `addRow`, block while the limit is hit. In
`getBatch`, broadcast after draining to wake blocked producers.

## 3. No New Metrics

The design proposes three new metrics:
- **`getBatch` wait time**: How long workers block waiting for work.
- **Batch fill ratio**: `actualMessages / maxMessages`.
- **Pending buffer depth**: Gauge of total events in the buffer.

The prototype uses the existing `metricsRecorder` interface but does not
record any of these new metrics.

**To do**: Add these metrics. `getBatch` wait time requires timing
the `cond.Wait` loop. Batch fill ratio is computed after `getBatch`
returns. Buffer depth is `totalEvents`.

## 4. Cluster Setting Defaults to False

The design says the setting should be "on by default" for rollout. The
prototype defaults to `false` since it hasn't been validated in
production yet. Flip to `true` once confidence is established.

## 5. No Benchmarks

The design's M2 milestone calls for throughput/latency benchmarks
(unit-level with mock client + roachtest against real endpoints) to
establish a baseline and compare against. These were not built.
Decided out of scope for the prototype — manual testing with webhook
sink is the current validation approach.