# No-Linger Batching Sink Design

## Problem

Changefeed sinks currently require users to tune
[`min_flush_frequency`](https://www.cockroachlabs.com/docs/stable/changefeed-sinks#flush)
to balance throughput against latency: larger batches improve throughput but
every event pays up to the flush interval in added delay, even when the sink is
idle. There is no default that works well for all workloads
([#166075](https://github.com/cockroachdb/cockroach/issues/166075)). We want to
remove this dependency so that batching works well out of the box.

## Design Overview

Replace the current push-based `batchingSink` + `parallelIO` with a single
pull-based `noLingerSink` where IO workers pull batches from a shared buffer
when they are ready for more work.

### Key Idea

Under high throughput, events naturally accumulate in the buffer while workers
are busy with downstream IO. When a worker finishes and pulls a new batch, it
gets a full batch with zero artificial delay. Under low throughput, workers are
idle and waiting, so when an event arrives, a worker immediately pulls it as a
batch of 1. **Batching emerges from natural backpressure rather than a timer.**

## Architecture

```
                    addRow()
  EmitRow ────────────────────► PendingBuffer
                                  │
                    getBatch()    │    completeBatch()
              ┌───────────────────┤◄──────────────┐
              │                   │                │
              ▼                   ▼                │
         ┌─────────┐        ┌─────────┐     ┌─────────┐
         │ worker 1│        │ worker 2│     │ worker n│
         └────┬────┘        └────┬────┘     └────┬────┘
              │                  │                │
              │  client.Flush()  │                │
              ▼                  ▼                ▼
         ┌──────────────────────────────────────────┐
         │           Downstream Sink                 │
         │     (Kafka / Webhook / PubSub)            │
         └──────────────────────────────────────────┘
```

### Worker Loop

Each IO worker runs a simple loop:

```
for {
    batch := buffer.getBatch(maxMessages, maxBytes)  // blocks if empty
    err := client.Flush(ctx, batch.payload)
    buffer.completeBatch(batch)                      // removes keys from inflight
    reportResult(batch, err)
}
```

No separate batching goroutine. No channel-based IO dispatch. Workers pull
directly from the shared buffer.

## Pending Buffer Data Structure

### Global Pending Buffer, Per-Topic Batches

The pending buffer is global across all topics in the changefeed, but each
batch produced by `getBatch` contains events for exactly one topic. The global
structure exists to schedule fairly across topics and to localize all per-key
state in one place; the per-topic batching boundary exists because pubsub and
webhook can only flush one topic at a time:

- **Pubsub**: `Publish` is called on a `*pubsub.Topic`. There is no API call
  that publishes to multiple topics in one request.
- **Webhook**: each topic is a separate HTTP endpoint (or at minimum a
  separate body framing). One POST cannot carry mixed-topic events.
- **Kafka**: a single Produce request *can* carry records for multiple
  topics, but constraining to one topic per batch costs little and keeps a
  uniform contract across all sink clients.

To avoid false conflicts where the same raw key exists in multiple topics
that happen to be inflight via different workers, keys are tracked
per-(topic, key) throughout.

Structure (two-level heap):
- **`topicHeap`**: A min-heap of topics, ordered by the oldest pending
  message across that topic. Contains only topics with actionable
  (non-inflight) work. Small — bounded by the number of topics in the
  changefeed (typically one per watched table).
- **`keyHeaps`**: `map[topic] → min-heap of keys`, where each per-topic
  heap is ordered by the oldest pending message for that key. Contains only
  non-inflight keys.
- **`keyMessages`**: `map[(topic, key)] → []event` — per-key FIFO queues of
  pending events.
- **`inflight`**: set of `(topic, key)` pairs currently being processed by
  workers.

```
PendingBuffer
├── topicHeap:    min-heap of topics by oldest pending message (actionable only)
├── keyHeaps:     map[topic] → min-heap of keys by oldest pending message
├── keyMessages:  map[(topic, key)] → []event
├── inflight:     set of (topic, key)
└── (synchronization primitives)
```

Within a topic, the inner heap (rather than a simple FIFO of keys) is needed
because when an inflight key returns via `completeBatch` with leftover
events, its oldest-pending-message time may predate keys that arrived in the
meantime — insertion order would violate the "oldest waiting message wins"
invariant.

### Operations

**`addRow(topic, key, event)`** — called from EmitRow:
1. Append event to `keyMessages[(topic, key)]`.
2. If `(topic, key)` is not inflight and not already in `keyHeaps[topic]`,
   push it onto `keyHeaps[topic]` with the event's timestamp.
3. If `topic` is not already in `topicHeap` and now has actionable work,
   push it onto `topicHeap` with the same timestamp.

**`getBatch(maxMessages, maxBytes)`** — called by idle worker, blocks if empty:
1. Pop the head topic `T` from `topicHeap` (the topic with the oldest
   waiting message).
2. Pop keys from `keyHeaps[T]`, draining each key's FIFO into the batch,
   until `maxMessages`/`maxBytes` is hit or `keyHeaps[T]` is empty.
3. If the batch fills mid-key, stop (remaining events for that key stay in
   `keyMessages` but the key is not re-added to `keyHeaps[T]` since it
   becomes inflight).
4. Mark all `(T, key)` pairs in the batch as inflight.
5. If `keyHeaps[T]` still has non-inflight keys, push `T` back onto
   `topicHeap` with its new oldest age.
6. If no events are available, block until `addRow` signals new work.

**`completeBatch(batch)`** — called when `client.Flush` returns:
1. For each `(T, key)` in the batch, remove from `inflight`.
2. If `keyMessages[(T, key)]` still has pending events, re-insert the key
   into `keyHeaps[T]` with its oldest remaining message's timestamp.
3. If `keyMessages[(T, key)]` is empty, delete the entry.
4. If `T` now has actionable work and is not already in `topicHeap`, push
   it on with its new oldest age.
5. Signal any blocked `getBatch` callers (newly actionable work may be
   available).

Cost per batch is `O(log T)` for the topic pick plus `O(B log K_T)` for
filling, where `T` is the number of topics in the changefeed and `K_T` is
the number of distinct pending keys for the chosen topic. `T` is small in
practice, so the outer heap is cheap.

### SinkClient Interface

The existing `BatchBuffer` interface (`MakeBatchBuffer(topic)` and
`Append(ctx, key, value, attributes)`) is retained unchanged. Because each
batch is single-topic, a buffer is still created per-topic per-batch and
the topic does not need to move onto per-record state. This keeps Kafka v2,
webhook, and pubsub `SinkClient` implementations untouched.

### Conflict Avoidance

Unlike the current `parallelIO` which detects conflicts at dispatch time and
queues conflicting requests, the `noLingerSink` avoids conflicts by
construction:

- Inflight `(topic, key)` pairs are removed from the per-topic heap entirely,
  so `getBatch` never produces a batch that conflicts with in-progress work.
- Events for the same key are grouped together in batches, reducing cross-batch
  conflicts compared to the current arrival-order batching.
- Tracking conflicts per-`(topic, key)` ensures that the same raw key in
  different topics does not falsely conflict.
- No pending queue, no conflict-checking intset scan, no wasted batch
  construction.

### Batching Properties

- **High throughput:** Workers are busy with Flush calls. Events accumulate in
  the buffer. When a worker returns and calls `getBatch`, it gets a full batch
  immediately. Good batching, zero artificial delay.
- **Low throughput:** Workers are blocked in `getBatch` waiting for events. When
  an event arrives, a worker wakes up and takes it immediately (batch of 1).
  Minimal latency, no timer-induced waiting.
- **Mixed volume topics:** `topicHeap` orders topics by oldest pending event,
  so a low-volume topic's event is picked up promptly the moment it becomes
  the oldest waiting message. High-volume topics still batch efficiently
  because their events accumulate while workers handle other work. Each batch
  is single-topic, but cross-topic *fairness* is preserved by the outer heap.

## Sink Interface Compatibility

The `noLingerSink` implements the existing `Sink` interface:

- **`EmitRow`**: Calls `addRow` on the pending buffer.
- **`Flush`**: Stops workers from pulling new batches, waits for all inflight
  batches to complete, then returns. Same semantics as today (required before
  emitting resolved timestamps).
- **`EmitResolvedTimestamp`**: Calls `Flush`, then emits the resolved payload
  via `SinkClient.FlushResolvedPayload`. Same as today.
- **`Close`**: Signals workers to stop, waits for shutdown.

The `SinkClient` and `BatchBuffer` interfaces are unchanged. Kafka, webhook,
and pubsub `SinkClient` implementations are untouched.

## Rollout Strategy

The `noLingerSink` will be gated behind a cluster setting (on by default) so it
can be toggled off if issues arise in production. Both `batchingSink` and
`noLingerSink` use the same `SinkClient` and `BatchBuffer` interfaces, so the
cluster setting simply controls which batching/dispatch layer wraps the
`SinkClient`. Once `noLingerSink` is validated, `batchingSink` and `parallelIO`
can be removed.

## Synchronization

The pending buffer is shared between the EmitRow caller goroutine and multiple
worker goroutines. Synchronization uses a `sync.Mutex` paired with a single
`sync.Cond`:

- A mutex protects the buffer's internal state (heaps, keyMessages maps,
  inflight sets).
- A condition variable coordinates blocking and waking across producers,
  workers, and flush requests. All waiters recheck their own condition on
  wakeup, so one cond serves multiple wait reasons.
- `addRow` signals one worker (new event). `completeBatch` broadcasts to all
  (may unblock multiple workers and Flush). During `Flush`, workers are
  prevented from pulling new batches; inflight workers continue to completion.
- This replaces the current single-goroutine batching worker with
  channel-based interface, avoiding the producer-consumer deadlock issues in
  the current design.

## Configuration

- **`maxMessages`**: Maximum number of messages per batch. Corresponds to
  current `sinkBatchConfig` message limit.
- **`maxBytes`**: Maximum byte size per batch. Corresponds to current
  `sinkBatchConfig` byte limit. Currently set manually; in the future, sinks
  like Kafka could report their actual maximum (e.g. from broker config) so the
  limit is derived automatically rather than configured. Out of scope for this
  work.
- **`numWorkers`**: Number of concurrent IO workers (same as current
  `ioWorkers` / parallelism setting).
- **`minFlushFrequency` is removed.** No linger timer.

## Admission Control / Pacing

The `admission.Pacer` call lives in `addRow`, pacing the producer before adding
to the buffer. Unlike the current batching sink (which swallows the pacer
error), `addRow` returns the error to `EmitRow`, allowing clean shutdown on
context cancellation. `addRow` can already block due to the buffer bound, so
pacing is just one more reason it might wait.

## Quota / Backpressure

Inflight concurrency is naturally bounded by `numWorkers`. The pending buffer is
bounded by a configurable max size (event count or byte size). When the limit is
hit, `addRow` blocks via `cond.Wait` until `getBatch` drains below the
threshold. The current `eventCh` buffered channel uses a hardcoded depth of 256
events; the pending buffer default should be comparable but may benefit from
scaling with `numWorkers`. The backpressure chain is: slow sink → workers
blocked in Flush → buffer fills → `addRow` blocks → changefeed processor
stalls → KV feed slows down.

## Retry Semantics

Retries live in the worker loop. Workers retry `client.Flush` using the same
`retry.Options` as the current system. If all retries are exhausted, the error
is terminal — the changefeed restarts. Events from a failed batch are not
re-queued into the buffer.

## Metrics

Most current sink metrics (message size, emitted batches, inflight changes,
backpressure, worker counts, inflight keys) map directly to the new design.
Metrics tied to the linger timer (`recordSizeBasedFlush`) or the conflict
pending queue (`recordPendingQueuePush/Pop`, `recordResultQueueLatency`) are
removed.

New metrics:

- **`getBatch` wait time** — how long a worker blocked waiting for work (high =
  sink faster than changefeed, low = good utilization)
- **Batch fill ratio** — `actualMessages / maxMessages` (shows how well natural
  backpressure batching is working)
- **Pending buffer depth** — gauge of total events in the buffer (spots
  sustained backlog)

## Milestones

### M0: Design Doc

Finalize this document.

### M1: Throughput/Latency Benchmark

Build a test that measures throughput and latency for both Kafka and webhook
sinks under varying load. Two levels:
- A unit-level benchmark with a mock sink client for fast development iteration.
- A roachtest against real Kafka/webhook endpoints for real-world validation.

Establishes a baseline with the current batching sink to compare against.

### M2: Basic Batching Algorithm

Implement the `PendingBuffer` with `addRow`, `getBatch`, and `completeBatch`
using a simple FIFO queue. One event per batch. No key-grouping heap, no
conflict optimization. Includes `sync.Cond` wiring. Unit tested in isolation
for correctness: ordering, blocking behavior, basic conflict avoidance (skip
inflight keys).

### Prototype limitation: single-topic only

While M3's worker pool is being built and before M4's per-topic batching
lands, the noLingerSink (gated by `changefeed.no_linger_sink.enabled`,
off by default) only supports single-topic changefeeds. EmitRow records
the first topic it sees and returns an error on any subsequent row
whose topic differs, so multi-target changefeeds opting into the
setting fail fast instead of silently producing wrong output for
pubsub/webhook. The guard is removed when M4 lands.

### M3: IO Loop + Sink Integration

Wire the worker loop to the `PendingBuffer` and connect to the `SinkClient`
interface. Implement `Flush` and `EmitResolvedTimestamp` to complete the `Sink`
interface. Gate behind a cluster setting. Can re-run M1 benchmark at this
point — expected to work but not perform optimally due to naive batching.

### M4: Full Batching Algorithm

Replace the FIFO queue with the two-level heap (`topicHeap` + per-topic
`keyHeaps`) tracking conflicts per `(topic, key)`. `getBatch` produces
single-topic batches, grouping events by key to minimize cross-batch
conflicts, filling up to `maxMessages` / `maxBytes`. Re-run M1 benchmark and
expect performance improvement.

