# No-Linger Batching: Implementation Notes

Personal notes and implementation details that supplement the main design doc.

## Current Architecture Issues (Secondary Motivation)

Beyond the throughput/latency tradeoff, the current architecture has structural
issues that the new design also addresses:

- The batching sink uses a single goroutine to both produce IO requests and
  consume results, requiring a `TryAcquire`/`ErrNotEnoughQuota` workaround to
  avoid deadlock (see `parallel_io.go` TODO at line 183).
- Batches are built by arrival order, so consecutive batches for the same topic
  frequently share keys, causing unnecessary conflicts and serialization in
  `parallelIO`.
- The push-based design means batch construction is decoupled from downstream
  readiness, leading to either idle waiting or suboptimal batch sizes.

## Pacer Error Handling

The current `batchingSink` swallows the pacer error (`_, _ = s.pacer.Pace(ctx)`
at `batching_sink.go:479`). The TODO above it acknowledges this is wrong.

`Pace` only errors when `wq.Admit(ctx, ...)` fails, which only happens on
context cancellation (changefeed shutting down, node draining, etc.). So the
row doesn't need to be retried — on context cancellation the changefeed
restarts from its last checkpoint and replays from the last resolved timestamp.

The current code "gets away with" swallowing because the next `select` on
`ctx.Done()` catches the cancellation. But returning the error from `addRow`
is cleaner: error → `EmitRow` returns error → changefeed restarts from
checkpoint. No ambiguity about control flow.

There's also a second pacer call upstream in `event_processing.go:356`
(`kvEventToRowConsumer.ConsumeEvent`) which properly checks the error. That
one paces CPU for event encoding and is unrelated to the sink.

## Buffer Bound

The current `eventCh` uses `flushQueueDepth = 256` (hardcoded constant defined
in `sink_cloudstorage.go:398`). The cloud storage comment explains the reasoning:
256 slots × 16MB file size = ~2.5GB max queued data. For the batching sink,
each event is much smaller than a file, so actual memory at 256 events is
modest.

For the pending buffer, options:
- Simple event count limit (e.g. 256, matching current behavior)
- Scale with numWorkers (e.g. 256 * numWorkers) to avoid starving workers
- Byte-based limit for more predictable memory usage
- Both event count and byte limit (whichever hits first)

Start simple (event count) and add byte-based limits if needed.

## Synchronization Pseudocode

Detailed `sync.Cond` usage for each operation:

**`addRow`** — blocks during flush, then wakes one worker:
```go
mu.Lock()
defer mu.Unlock()
for flushing && !closed {
    cond.Wait()  // block new events while Flush is draining
}
// ... append event, update heap ...
cond.Signal()  // one new event can only be taken by one worker
```

**`getBatch`** (workers) — blocks until actionable work is available:
```go
mu.Lock()
defer mu.Unlock()
for !hasActionableWork() && !closed {  // no flushing check here
    cond.Wait()
}
return buildBatchFromHeap()
```

**`completeBatch`** — wakes all blocked waiters (workers and Flush):
```go
mu.Lock()
defer mu.Unlock()
// ... remove keys from inflight, re-add to heap with oldest remaining mvcc ...
cond.Broadcast()  // may unblock work for multiple workers
```

**`Flush`** — blocks addRow and waits for full drain:
```go
mu.Lock()
defer mu.Unlock()
flushing = true
defer func() { flushing = false; cond.Broadcast() }()
for (totalInflight > 0 || totalEvents > 0) && termErr == nil && !closed {
    cond.Wait()  // woken by completeBatch's Broadcast
}
```

**Why Flush blocks `addRow`, not `getBatch`:** The original design blocked
workers from pulling new batches during Flush. This causes a deadlock when
Flush must wait for both inflight and pending events: pending events can
never become inflight (workers are blocked), so Flush waits forever. The
fix: block new arrivals instead. Workers keep pulling and draining the
buffer, no new events arrive, and eventually everything drains.

## Metrics Mapping (Old → New)

Detailed mapping of current metrics to their equivalents in the new design:

- `recordMessageSize` → in `addRow`
- `recordEmittedBatch` → after `completeBatch`
- `recordSinkIOInflightChange` → in worker loop around `client.Flush`
- `recordSinkBackpressure` → `addRow` blocking time when buffer is full
- `recordParallelIOWorkers`, `recordFlushRequestCallback`, `setInFlightKeys`
  → same semantics
- `recordSizeBasedFlush` → removed (no timer vs size distinction)
- `recordPendingQueuePush/Pop`, `recordResultQueueLatency` → removed (no
  conflict pending queue or result channel)

## Shutdown / Close Semantics

The current `batchingSink.Close()` closes `doneCh` and waits on the worker
goroutine's `wg`. The `doneCh` signal causes the batching worker to exit its
select loops — it does not drain remaining events. Buffered events are dropped.
This is fine because on shutdown the changefeed restarts from its last
checkpoint and replays.

`noLingerSink.Close` should match: signal workers to stop, wait for them to
exit, drop anything left in the buffer.

## Error Propagation from Workers

Workers that hit a terminal error (all retries exhausted) need to surface
that error to `EmitRow` and `Flush` callers. The current `batchingSink` uses
`termErr` — once set, subsequent `EmitRow` calls check it and return early.
`noLingerSink` should do the same: set a `termErr` under the mutex, and have
`addRow` / `Flush` check it before doing work. `completeBatch` should
broadcast after setting `termErr` to unblock any waiters.

## Hash Collisions

`hash(topic + key)` can produce false conflicts for different (topic, key)
pairs. This only causes unnecessary serialization (correctness is not
affected). Given the hash space is large and the number of concurrent inflight
keys is bounded by `numWorkers × maxMessages`, collisions should be rare
enough not to matter in practice.

## Threading Model: EmitRow vs Flush

`EmitRow` is called from `kvEventToRowConsumer.ConsumeEvent` in the changefeed
event consumer processor. `EmitResolvedTimestamp` (which calls `Flush`) is
called from `changeFrontier.maybeEmitResolved` — a different processor in the
DistSQL flow, running in a different goroutine. So there's no risk of `addRow`
blocking and preventing `Flush` from being called — they're never in the same
goroutine.

## Memory Accounting (kvevent.Alloc)

The current `EmitRow` takes an `alloc` parameter for memory accounting.
`addRow` should accept and carry it along with the event. The alloc should be
released after `completeBatch` when the event is fully processed, matching
the current lifecycle.
