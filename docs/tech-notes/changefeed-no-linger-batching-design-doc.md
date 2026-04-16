# No-Linger Batching Sink

## Problem

Changefeed sinks require users to tune [`min_flush_frequency`](https://www.cockroachlabs.com/docs/stable/changefeed-sinks#flush) to balance throughput against latency: larger batches improve throughput but every event pays up to the flush interval in added delay, even when the sink is idle. There is no default that works well for all workloads ([#166075](https://github.com/cockroachdb/cockroach/issues/166075)). We want to remove this dependency so that batching works well out of the box.

## Non-Goals

- Changing `SinkClient` implementations (Kafka, webhook, pubsub). They continue to work as-is with a minor `BatchBuffer` interface adjustment.
- Any sinks other than Kafka, webhook, pubsub.
- Automatically deriving `maxBytes` from sink configuration (e.g. Kafka broker limits). This work will position us well to make that change, but is not part of this effort. 

## Solution

Replace the current push-based `batchingSink` + `parallelIO` with a pull-based `noLingerSink`. Instead of building batches on a timer and pushing them to workers, workers pull batches from a shared buffer when they finish their current work.

**Under high throughput**, events accumulate in the buffer while workers are busy with downstream IO. When a worker finishes, it pulls a full batch immediately — zero artificial delay.
**Under low throughput**, workers are idle and waiting, so an arriving event is pulled immediately as a batch of 1 achieving minimal latency.
**Batching emerges from natural backpressure rather than a timer.**

### How It Works

- **Worker loop**: each worker calls `getBatch` (blocks if empty), flushes to the sink, then calls `completeBatch`. No separate batching goroutine. Batches will be full when enough work is waiting.
- **Pending buffer/batch construction**: We store a min-heap of keys by age of the oldest message for that key and a FIFO queue of messages per key. Batches are formed by repeatedly finding the key that's not in flight with the oldest unsent messages and taking as many messages as possible, repeating for more keys until the batch is full. 
- **Conflict avoidance**: since inflight keys are excluded from the heap, batches are conflict free and does not need to be handled by workers. 
- **Sink interface**: this noLinger sink wraps Kafka, webhook, pubsub sinks like the current batching sink and implements the sink interface. 
- **Synchronization**: we use a `sync.Mutex` with a single `sync.Cond` for worker wake (after buffer is empty), flush and drain as well as blocking when the buffer is full. During Flush, `addRow` is blocked (not `getBatch`) so workers can keep draining the buffer — blocking workers instead causes a deadlock since pending events can never become inflight.
- **Backpressure**: when the bounded buffer is full, `addRow` blocks until workers drain. Admission control pacing also lives in `addRow`.
- **Configuration**: `minFlushFrequency` (the linger) is removed but other configurations (`maxMessages`, `maxBytes`, `numWorkers`) remain. 
- **Rollout**: gated behind a cluster setting (off by default during validation, on by default once validated). The `BatchBuffer` interface refactor lands first so both sinks share one interface.
- **Metrics**: We will measure the `getBatch` wait time, batch fill ratio, and the size of the pending buffer. We remove changefeed.size_based_flushes, changefeed.parallel_io_queue_nanos, changefeed.parallel_io_pending_rows, changefeed.parallel_io_result_queue_nanos since they no longer make sense. 

## Milestones

1. **Throughput/latency benchmark** — Use roachtests and roach perf to establish a baseline of our batching sink's latency and throughput. Will be used to evaluate the eventual batching algorithm against.
2. **BatchBuffer interface refactor** — Since the new batches will not be made per topic, we make slight adjustments to the BatchBuffer interface so that we can support both the old and new batching sinks with the same interface.
3. **PendingBuffer + noLingerSink** — Implement the full mvcc-ordered key heap, key grouping, batch size limits, worker loop, `Flush`, and `EmitResolvedTimestamp`. Gate behind cluster setting.
4. **Remaining** — Buffer bound/backpressure, admission pacer, new metrics, flip default to true.