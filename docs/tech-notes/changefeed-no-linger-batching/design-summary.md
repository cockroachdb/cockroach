# No-Linger Batching Sink

## Problem

Changefeed sinks require users to tune [`min_flush_frequency`](https://www.cockroachlabs.com/docs/stable/changefeed-sinks#flush) to balance throughput against latency: larger batches improve throughput but every event pays up to the flush interval in added delay, even when the sink is idle. There is no default that works well for all workloads ([#166075](https://github.com/cockroachdb/cockroach/issues/166075)). We want to remove this dependency so that batching works well out of the box.

## Non-Goals

- Changing `SinkClient` or `BatchBuffer` interfaces. Kafka, webhook, and pubsub `SinkClient` implementations are untouched.
- Any sinks other than Kafka, webhook, pubsub.
- Automatically deriving `maxBytes` from sink configuration (e.g. Kafka broker limits). This work will position us well to make that change, but is not part of this effort. 

## Solution

Replace the current push-based `batchingSink` + `parallelIO` with a pull-based `noLingerSink`. Instead of building batches on a timer and pushing them to workers, workers pull batches from a shared buffer when they finish their current work.

**Under high throughput**, events accumulate in the buffer while workers are busy with downstream IO. When a worker finishes, it pulls a full batch immediately — zero artificial delay.
**Under low throughput**, workers are idle and waiting, so an arriving event is pulled immediately as a batch of 1 achieving minimal latency.
**Batching emerges from natural backpressure rather than a timer.**

### How It Works

- **Worker loop**: each worker calls `getBatch` (blocks if empty), flushes to the sink, then calls `completeBatch`. No separate batching goroutine. Batches will be full when enough work is waiting.
- **Pending buffer/batch construction**: A two-level heap — an outer `topicHeap` of topics ordered by oldest pending message, and per-topic inner heaps of keys ordered the same way, with a FIFO of messages per `(topic, key)`. `getBatch` picks the head topic (oldest waiting message), then repeatedly pulls keys from that topic's inner heap until the batch fills. Each batch is single-topic (pubsub and webhook can only flush one topic per request).
- **Conflict avoidance**: inflight `(topic, key)` pairs are excluded from the per-topic heap, so batches are conflict free without runtime checks. Tracking per `(topic, key)` ensures the same raw key in different topics does not falsely conflict.
- **Sink interface**: this noLinger sink wraps Kafka, webhook, pubsub sinks like the current batching sink and implements the sink interface. The `SinkClient` and `BatchBuffer` interfaces are unchanged.
- **Synchronization**: we use a `sync.Mutex` with a single `sync.Cond` for worker wake (after buffer is empty), flush and drain as well as blocking when the buffer is full.
- **Backpressure**: when the bounded buffer is full, `addRow` blocks until workers drain. Admission control pacing also lives in `addRow`.
- **Configuration**: `minFlushFrequency` (the linger) is removed but other configurations (`maxMessages`, `maxBytes`, `numWorkers`) remain. 
- **Rollout**: we will gate this behind a cluster setting (which will be on by default).
- **Metrics**: We will measure the `getBatch` wait time, batch fill ratio, and the size of the pending buffer. We remove changefeed.size_based_flushes, changefeed.parallel_io_queue_nanos, changefeed.parallel_io_pending_rows, changefeed.parallel_io_result_queue_nanos since they no longer make sense. 

## Milestones

1. **Throughput/latency benchmark** - Use roachtests and roach perf to establish a baseline of our batching sink's latency and throughput. Will be used to evaluate the eventual batching algorithm against. 
2. **Basic batching algorithm** — Implement the `PendingBuffer` with FIFO queue, inflight tracking and `sync.Cond` wiring for the batching portion of this algorithm and unit test basic properties (we can append events which then show up in batches).
3. **IO loop + sink integration** — Add the worker loop, `Flush` and `EmitResolvedTimestamp` support. At this point the sink should work, but not be fully performant. Add the gate behind cluster setting, but keep off by default until M4. Our M1 test should run, though the performance may be worse than baseline.
4. **Full batching algorithm** — Add the two-level `topicHeap` + per-topic `keyHeaps`, key grouping, batch size limits. Each batch is single-topic. At this point our M1 tests should run and show improved performance and we can turn the cluster setting on by default. 