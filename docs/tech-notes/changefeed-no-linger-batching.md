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

### Global Buffer (Not Per-Topic)

The buffer is global across all topics in the changefeed. Ordering guarantees
are per (topic, key) pair, so events for different topics with the same key hash
are independent. To avoid false conflicts, keys are hashed as a composite of
topic and key: `hash(topic + key)`.

This simplifies the design (no "which topic to serve" problem) and matches how
kgo works internally — `ProduceSync` accepts records for any topic and routes
them to the appropriate broker.

Structure:
- **`keyHeap`**: A min-heap of composite key hashes, ordered by the oldest
  pending message for that key. Contains only non-inflight keys (actionable
  work).
- **`keyMessages`**: `map[keyHash][]event` — per-key FIFO queues of pending
  events.
- **`inflight`**: `intsets.Fast` — the set of composite key hashes currently
  being processed by workers.

```
PendingBuffer
├── keyHeap:      min-heap by oldest message time (non-inflight keys only)
├── keyMessages:  map[int][]event
├── inflight:     intsets.Fast
└── (synchronization primitives)
```

### Operations

**`addRow(topic, key, event)`** — called from EmitRow:
1. Compute composite key hash: `hash(topic + key)`.
2. Append event to `keyMessages[compositeKeyHash]`.
3. If compositeKeyHash is not already in the heap and not inflight, push it
   onto the heap with the event's timestamp.

**`getBatch(maxMessages, maxBytes)`** — called by idle worker, blocks if empty:
1. Pop keys from the heap, taking all pending events for each key (or as many
   as fit within `maxMessages`/`maxBytes`).
2. If the batch fills mid-key, stop (remaining events for that key stay in
   `keyMessages` but the key is not re-added to the heap since it becomes
   inflight).
3. Add all keys included in the batch to `inflight`.
4. If no events are available, block until `addRow` signals new work.

**`completeBatch(batch)`** — called when `client.Flush` returns:
1. Remove the batch's keys from `inflight`.
2. For each key, if `keyMessages[key]` still has pending events, re-insert the
   key into the heap with its oldest remaining message's timestamp.
3. If `keyMessages[key]` is empty, delete the entry.
4. Signal any blocked `getBatch` callers (newly actionable work may be
   available).

### SinkClient Interface Changes

Since batches can now span topics, the `BatchBuffer` interface changes:

- `MakeBatchBuffer(topic string)` → `MakeBatchBuffer()` (no topic parameter)
- `Append(ctx, key, value, attributes)` → `Append(ctx, topic, key, value, attributes)`
  (topic moves to per-record)

The topic ultimately lives per-record at the point of consumption (e.g. each
`kgo.Record` carries its own `Topic` field), so moving it from buffer-level
state to a per-`Append` argument aligns the interface with where the data is
actually used. For Kafka v2, this means replacing `b.topic` (set at buffer
creation) with the `topic` argument in `Append` — the `b.topic` field and the
topic parameter on `MakeBatchBuffer` both go away, making the implementation
net simpler. Webhook and pubsub need analogous adjustments.

This interface change can land as a standalone refactor before `noLingerSink`
exists. The existing `batchingSink` can use the new signature with a minor
mechanical change: instead of passing the topic at `MakeBatchBuffer` time, it
passes it to each `Append` call. This keeps the interface clean (one version,
not two) and enables both sinks to coexist behind a cluster setting during
rollout.

### Conflict Avoidance

Unlike the current `parallelIO` which detects conflicts at dispatch time and
queues conflicting requests, the `noLingerSink` avoids conflicts by
construction:

- Inflight keys are removed from the heap entirely, so `getBatch` never
  produces a batch that conflicts with in-progress work.
- Events for the same key are grouped together in batches, reducing cross-batch
  conflicts compared to the current arrival-order batching.
- Composite key hashes (topic + key) ensure that events for different topics
  with the same raw key do not falsely conflict.
- No pending queue, no conflict-checking intset scan, no wasted batch
  construction.

### Batching Properties

- **High throughput:** Workers are busy with Flush calls. Events accumulate in
  the buffer. When a worker returns and calls `getBatch`, it gets a full batch
  immediately. Good batching, zero artificial delay.
- **Low throughput:** Workers are blocked in `getBatch` waiting for events. When
  an event arrives, a worker wakes up and takes it immediately (batch of 1).
  Minimal latency, no timer-induced waiting.
- **Mixed volume topics:** The global heap orders by oldest event across all
  topics. A low-volume topic's event gets picked up promptly because its age
  makes it the oldest. High-volume topics still batch efficiently because events
  accumulate while workers handle other work. Workers naturally fill batches
  with events from whichever keys are oldest and non-inflight, regardless of
  topic.

## Sink Interface Compatibility

The `noLingerSink` implements the existing `Sink` interface:

- **`EmitRow`**: Calls `addRow` on the pending buffer.
- **`Flush`**: Stops workers from pulling new batches, waits for all inflight
  batches to complete, then returns. Same semantics as today (required before
  emitting resolved timestamps).
- **`EmitResolvedTimestamp`**: Calls `Flush`, then emits the resolved payload
  via `SinkClient.FlushResolvedPayload`. Same as today.
- **`Close`**: Signals workers to stop, waits for shutdown.

The `SinkClient` interface methods `Flush`, `FlushResolvedPayload`,
`CheckConnection`, and `Close` are unchanged. The `BatchBuffer` interface
changes as described above (topic moves from `MakeBatchBuffer` to `Append`),
but this is a refactor that both the old `batchingSink` and new `noLingerSink`
can use. Kafka, webhook, and pubsub `SinkClient` implementations continue to
work with the updated `BatchBuffer` signature.

## Rollout Strategy

The `noLingerSink` will be gated behind a cluster setting (on by default) so it
can be toggled off if issues arise in production. Both `batchingSink` and
`noLingerSink` share the same `SinkClient` and `BatchBuffer` interfaces, so the
cluster setting simply controls which batching/dispatch layer wraps the
`SinkClient`. The `BatchBuffer` interface refactor (moving topic from
`MakeBatchBuffer` to `Append`) lands first as a standalone change, before
`noLingerSink` is introduced, so both sinks use a single interface version with
no compatibility shims. Once `noLingerSink` is validated, `batchingSink` and
`parallelIO` can be removed.

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

### M1: BatchBuffer Interface Refactor

Move topic from `MakeBatchBuffer(topic)` to `Append(ctx, topic, ...)` across
all `SinkClient` implementations (Kafka v2, webhook, pubsub) and update
`batchingSink` to use the new signature. This is a standalone mechanical
refactor with no behavioral change, and unblocks both sinks to share a single
interface.

### M2: Throughput/Latency Benchmark

Build a test that measures throughput and latency for both Kafka and webhook
sinks under varying load. Two levels:
- A unit-level benchmark with a mock sink client for fast development iteration.
- A roachtest against real Kafka/webhook endpoints for real-world validation.

Establishes a baseline with the current batching sink to compare against.

### M3: Basic Batching Algorithm

Implement the `PendingBuffer` with `addRow`, `getBatch`, and `completeBatch`
using a simple FIFO queue. One event per batch. No key-grouping heap, no
conflict optimization. Includes `sync.Cond` wiring. Unit tested in isolation
for correctness: ordering, blocking behavior, basic conflict avoidance (skip
inflight keys).

### M4: IO Loop + Sink Integration

Wire the worker loop to the `PendingBuffer` and connect to the `SinkClient`
interface. Implement `Flush` and `EmitResolvedTimestamp` to complete the `Sink`
interface. Gate behind a cluster setting. Can re-run M2 benchmark at this
point — expected to work but not perform optimally due to naive batching.

### M5: Full Batching Algorithm

Replace the FIFO queue with the global key heap using composite (topic + key)
hashes. `getBatch` groups events by key to minimize cross-batch conflicts, fills
batches up to `maxMessages` / `maxBytes`. Re-run M2 benchmark and expect
performance improvement.

