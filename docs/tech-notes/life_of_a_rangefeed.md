# Life of a Rangefeed (Server-Side)

Generated: 2026-02-12
Repository: https://github.com/cockroachdb/cockroach
Commit: 1b12bbdd504af874ba0fd6e99958681bea968a79

## Introduction

This document traces the execution of a rangefeed request through the CockroachDB
codebase on the server side, focusing on how data flows from Raft log application
through the rangefeed processor and ultimately to the gRPC stream back to the client.

A rangefeed is a streaming mechanism that allows clients to subscribe to real-time
updates for a range of keys. It's the foundation for CDC (Change Data Capture),
schema change monitoring, and other reactive features in CockroachDB.

## High-Level Architecture

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   Client     │────▶│    Node      │────▶│    Store     │────▶│   Replica    │
│ (DistSender) │     │ MuxRangeFeed │     │  RangeFeed   │     │  RangeFeed   │
└──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
                            │                                         │
                            ▼                                         ▼
                     ┌──────────────┐                          ┌──────────────┐
                     │    Stream    │◀─────────────────────────│  Processor   │
                     │   Manager    │                          │  (Scheduler) │
                     └──────────────┘                          └──────────────┘
                            │                                         ▲
                            ▼                                         │
                     ┌──────────────┐                          ┌──────────────┐
                     │   Buffered   │                          │     Raft     │
                     │    Sender    │                          │   Apply      │
                     └──────────────┘                          └──────────────┘
                            │
                            ▼
                     ┌──────────────┐
                     │  gRPC Stream │
                     │   (Client)   │
                     └──────────────┘
```

## Phase 1: Entry Point - Node.MuxRangeFeed

The rangefeed request enters the server via the gRPC `MuxRangeFeed` endpoint.
Modern rangefeeds use the multiplexed (Mux) protocol where a single gRPC stream
handles multiple range subscriptions.

### 1.1 gRPC Service Entry

The flow begins at [`MuxRangeFeed`](https://github.com/cockroachdb/cockroach/blob/1b12bbdd504af874ba0fd6e99958681bea968a79/pkg/server/node.go#L1946-L1948)
in `pkg/server/node.go`:

```go
func (n *kvRangeFeedServer) MuxRangeFeed(stream kvpb.DRPCRangeFeed_MuxRangeFeedStream) error {
    return (*Node)(n).muxRangeFeed(stream)
}
```

This delegates to [`muxRangeFeed`](https://github.com/cockroachdb/cockroach/blob/1b12bbdd504af874ba0fd6e99958681bea968a79/pkg/server/node.go#L2222-L2326)
which is the main implementation.

### 1.2 Stream Manager Setup

The `muxRangeFeed` function creates the core streaming infrastructure:

```go
func (n *Node) muxRangeFeed(muxStream kvpb.RPCInternal_MuxRangeFeedStream) error {
    lockedMuxStream := &lockedMuxStream{
        wrapped: muxStream,
        metrics: n.metrics.LockedMuxStreamMetrics,
    }

    sm := &rangefeed.StreamManager{}
    if kvserver.RangefeedUseBufferedSender.Get(&n.storeCfg.Settings.SV) {
        sm = rangefeed.NewStreamManager(
            rangefeed.NewBufferedSender(lockedMuxStream, n.storeCfg.Settings, n.metrics.BufferedSenderMetrics),
            n.metrics.StreamManagerMetrics)
    } else {
        sm = rangefeed.NewStreamManager(
            rangefeed.NewUnbufferedSender(lockedMuxStream),
            n.metrics.StreamManagerMetrics)
    }
```

Key components created:
- [`lockedMuxStream`](https://github.com/cockroachdb/cockroach/blob/1b12bbdd504af874ba0fd6e99958681bea968a79/pkg/server/node.go#L2123-L2127):
  Thread-safe wrapper around the gRPC stream
- [`StreamManager`](https://github.com/cockroachdb/cockroach/blob/1b12bbdd504af874ba0fd6e99958681bea968a79/pkg/kv/kvserver/rangefeed/stream_manager.go#L36-L62):
  Manages streams and coordinates between registrations and the underlying sender
- [`BufferedSender`](https://github.com/cockroachdb/cockroach/blob/1b12bbdd504af874ba0fd6e99958681bea968a79/pkg/kv/kvserver/rangefeed/buffered_sender.go#L73-L101):
  Buffers events at the node level before sending to gRPC

### 1.3 Request Processing Loop

The main loop in [`muxRangeFeed`](https://github.com/cockroachdb/cockroach/blob/1b12bbdd504af874ba0fd6e99958681bea968a79/pkg/server/node.go#L2255-L2325)
receives requests from the client:

```go
for {
    select {
    case err := <-sm.Error():
        return err
    case <-ctx.Done():
        return ctx.Err()
    default:
        req, err := muxStream.Recv()
        // ... handle request

        streamSink := sm.NewStream(req.StreamID, req.RangeID)

        sm.RegisteringStream(req.StreamID)
        if disconnector, err := n.stores.RangeFeed(streamCtx, req, streamSink, limiter); err != nil {
            streamSink.SendError(kvpb.NewError(err))
        } else {
            sm.AddStream(req.StreamID, disconnector)
        }
    }
}
```

For each incoming request:
1. A new [`PerRangeEventSink`](https://github.com/cockroachdb/cockroach/blob/1b12bbdd504af874ba0fd6e99958681bea968a79/pkg/kv/kvserver/rangefeed/stream.go#L41-L55)
   is created via `sm.NewStream()`, tagged with `rangeID` and `streamID`
2. The request is forwarded to the stores via `n.stores.RangeFeed()`
3. The returned disconnector is stored in the StreamManager for lifecycle management

## Phase 2: Store and Replica Registration

### 2.1 Stores.RangeFeed

[`Stores.RangeFeed`](https://github.com/cockroachdb/cockroach/blob/1b12bbdd504af874ba0fd6e99958681bea968a79/pkg/kv/kvserver/stores.go#L202-L220)
routes to the appropriate store:

```go
func (ls *Stores) RangeFeed(
    streamCtx context.Context,
    args *kvpb.RangeFeedRequest,
    stream rangefeed.Stream,
    perConsumerCatchupLimiter *limit.ConcurrentRequestLimiter,
) (rangefeed.Disconnector, error) {
    store, err := ls.GetStore(args.Replica.StoreID)
    if err != nil {
        return nil, err
    }
    return store.RangeFeed(streamCtx, args, stream, perConsumerCatchupLimiter)
}
```

### 2.2 Store.RangeFeed

[`Store.RangeFeed`](https://github.com/cockroachdb/cockroach/blob/1b12bbdd504af874ba0fd6e99958681bea968a79/pkg/kv/kvserver/store.go#L3283-L3316)
finds the replica and delegates:

```go
func (s *Store) RangeFeed(...) (rangefeed.Disconnector, error) {
    repl, err := s.GetReplica(args.RangeID)
    if err != nil {
        return nil, err
    }

    tenID, _ := repl.TenantID()
    pacer := s.cfg.KVAdmissionController.AdmitRangefeedRequest(tenID, args)
    return repl.RangeFeed(streamCtx, args, stream, pacer, perConsumerCatchupLimiter)
}
```

### 2.3 Replica.RangeFeed - The Core Registration

[`Replica.RangeFeed`](https://github.com/cockroachdb/cockroach/blob/1b12bbdd504af874ba0fd6e99958681bea968a79/pkg/kv/kvserver/replica_rangefeed.go#L223-L339)
is the heart of rangefeed registration:

```go
func (r *Replica) RangeFeed(...) (rangefeed.Disconnector, error) {
    // Acquire catch-up iterator semaphore if needed
    if !args.Timestamp.IsEmpty() {
        alloc, err := r.store.limiters.ConcurrentRangefeedIters.Begin(streamCtx)
        // ...
    }

    // Lock raftMu to ensure registration captures all events
    r.raftMu.Lock()
    if err := r.checkExecutionCanProceedForRangeFeed(streamCtx, rSpan, checkTS); err != nil {
        r.raftMu.Unlock()
        return nil, err
    }

    // Create catch-up snapshot for historical data
    var catchUpSnap *rangefeed.CatchUpSnapshot
    if usingCatchUpSnap {
        catchUpSnap = rangefeed.NewCatchUpSnapshot(
            r.store.StateEngine(), rSpan.AsRawSpanWithNoLocals(),
            args.Timestamp, iterSemRelease, pacer, ...)
    }

    // Register with processor
    p, disconnector, err := r.registerWithRangefeedRaftMuLocked(...)
    r.raftMu.Unlock()

    return disconnector, err
}
```

Key steps:
1. **Semaphore acquisition**: Limits concurrent catch-up scans to prevent resource exhaustion
2. **raftMu lock**: Critical for ensuring no events are missed between catch-up and live updates
3. **Catch-up snapshot**: For replaying historical changes since `args.Timestamp`
4. **Processor registration**: Creates or reuses the per-replica processor

### 2.4 Processor Registration

[`registerWithRangefeedRaftMuLocked`](https://github.com/cockroachdb/cockroach/blob/1b12bbdd504af874ba0fd6e99958681bea968a79/pkg/kv/kvserver/replica_rangefeed.go#L430-L556)
either registers with an existing processor or creates a new one:

```go
func (r *Replica) registerWithRangefeedRaftMuLocked(...) (rangefeed.Processor, rangefeed.Disconnector, error) {
    r.rangefeedMu.Lock()
    p := r.rangefeedMu.proc

    if p != nil {
        // Try to register with existing processor
        reg, disconnector, filter := p.Register(streamCtx, span, startTS, catchUpSnap, ...)
        if reg {
            r.setRangefeedFilterLocked(filter)
            r.rangefeedMu.Unlock()
            return p, disconnector, nil
        }
        // Processor shutting down, unset and create new one
        r.unsetRangefeedProcessorLocked(p)
    }
    r.rangefeedMu.Unlock()

    // Create new processor
    cfg := rangefeed.Config{
        AmbientContext:   r.AmbientContext,
        Clock:            r.Clock(),
        Stopper:          r.store.stopper,
        RangeID:          r.RangeID,
        Span:             desc.RSpan(),
        TxnPusher:        &tp,
        Scheduler:        r.store.getRangefeedScheduler(),
        // ...
    }
    p = rangefeed.NewProcessor(cfg)

    // Start with resolved timestamp iterator
    p.Start(r.store.Stopper(), rtsIter)

    // Register this stream
    reg, disconnector, filter := p.Register(streamCtx, span, startTS, catchUpSnap, ...)

    r.setRangefeedProcessor(p)
    r.handleClosedTimestampUpdateRaftMuLocked(ctx, r.GetCurrentClosedTimestamp(ctx))

    return p, disconnector, nil
}
```

## Phase 3: The Rangefeed Processor

The [`ScheduledProcessor`](https://github.com/cockroachdb/cockroach/blob/1b12bbdd504af874ba0fd6e99958681bea968a79/pkg/kv/kvserver/rangefeed/scheduled_processor.go#L35-L84)
is the central coordinator for a range's rangefeed:

```go
type ScheduledProcessor struct {
    Config
    scheduler ClientScheduler

    reg registry          // Manages registrations
    rts resolvedTimestamp // Tracks resolved timestamp

    requestQueue chan request
    eventC       chan *event

    stopping atomic.Bool
    stoppedC chan struct{}
}
```

### 3.1 Processor Callback

The processor registers a callback with the scheduler that processes events:

[`process`](https://github.com/cockroachdb/cockroach/blob/1b12bbdd504af874ba0fd6e99958681bea968a79/pkg/kv/kvserver/rangefeed/scheduled_processor.go#L158-L173):

```go
func (p *ScheduledProcessor) process(e processorEventType) processorEventType {
    ctx := p.processCtx
    if e&RequestQueued != 0 {
        p.processRequests(ctx)
    }
    if e&EventQueued != 0 {
        p.processEvents(ctx)
    }
    if e&PushTxnQueued != 0 {
        p.processPushTxn(ctx)
    }
    if e&Stopped != 0 {
        p.processStop()
    }
    return 0
}
```

### 3.2 Event Processing

[`processEvents`](https://github.com/cockroachdb/cockroach/blob/1b12bbdd504af874ba0fd6e99958681bea968a79/pkg/kv/kvserver/rangefeed/scheduled_processor.go#L200-L217)
drains events from the channel:

```go
func (p *ScheduledProcessor) processEvents(ctx context.Context) {
    for max := len(p.eventC); max > 0; max-- {
        select {
        case e := <-p.eventC:
            if !p.stopping.Load() {
                p.consumeEvent(ctx, e)
            }
            e.alloc.Release(ctx)
            putPooledEvent(e)
        default:
            return
        }
    }
}
```

## Phase 4: Raft to Processor - The Data Path

This is the critical path where Raft-applied mutations become rangefeed events.

### 4.1 Raft Apply Triggers Rangefeed

When Raft applies a command batch, [`replicaAppBatch.ApplySideEffects`](https://github.com/cockroachdb/cockroach/blob/1b12bbdd504af874ba0fd6e99958681bea968a79/pkg/kv/kvserver/replica_app_batch.go#L490-L494)
feeds logical operations to the rangefeed:

```go
// Provide the command's corresponding logical operations to the Replica's
// rangefeed.
if ops := cmd.Cmd.LogicalOpLog; cmd.Cmd.WriteBatch != nil {
    b.r.handleLogicalOpLogRaftMuLocked(ctx, ops, b.batch)
}
```

### 4.2 Processing Logical Operations

[`handleLogicalOpLogRaftMuLocked`](https://github.com/cockroachdb/cockroach/blob/1b12bbdd504af874ba0fd6e99958681bea968a79/pkg/kv/kvserver/replica_rangefeed.go#L678-L802)
transforms raw Raft operations into rangefeed events:

```go
func (r *Replica) handleLogicalOpLogRaftMuLocked(
    ctx context.Context, ops *kvserverpb.LogicalOpLog, batch storage.Batch,
) {
    p, filter := r.getRangefeedProcessorAndFilter()
    if p == nil {
        return
    }

    // Read values from batch to populate logical ops
    for _, op := range ops.Ops {
        switch t := op.GetValue().(type) {
        case *enginepb.MVCCWriteValueOp:
            key, ts, valPtr = t.Key, t.Timestamp, &t.Value
        case *enginepb.MVCCCommitIntentOp:
            key, ts, valPtr = t.Key, t.Timestamp, &t.Value
        // ... other op types
        }

        // Skip if no registration needs this key
        if !filter.NeedVal(roachpb.Span{Key: key}) {
            continue
        }

        // Read value from batch
        val, vh, err := storage.MVCCGetForKnownTimestampWithNoIntent(ctx, batch, key, ts, valueInBatch)
        *valPtr = val.RawBytes
    }

    // Pass ops to processor
    if !p.ConsumeLogicalOps(ctx, ops.Ops...) {
        r.unsetRangefeedProcessor(p)
    }
}
```

The filter is critical for performance - it allows the system to skip reading values
for keys that no registration is interested in.

### 4.3 ConsumeLogicalOps Enqueues Events

[`ConsumeLogicalOps`](https://github.com/cockroachdb/cockroach/blob/1b12bbdd504af874ba0fd6e99958681bea968a79/pkg/kv/kvserver/rangefeed/scheduled_processor.go#L596-L620)
on the processor enqueues the operations:

```go
func (p *ScheduledProcessor) ConsumeLogicalOps(
    ctx context.Context, ops ...enginepb.MVCCLogicalOp,
) bool {
    return p.sendEvent(ctx, event{ops: ops}, p.EventChanTimeout)
}
```

### 4.4 Event Consumption and Publishing

When the scheduler invokes the processor callback,
[`consumeLogicalOps`](https://github.com/cockroachdb/cockroach/blob/1b12bbdd504af874ba0fd6e99958681bea968a79/pkg/kv/kvserver/rangefeed/scheduled_processor.go#L703-L746)
transforms operations into events:

```go
func (p *ScheduledProcessor) consumeLogicalOps(
    ctx context.Context, ops []enginepb.MVCCLogicalOp, alloc *SharedBudgetAllocation,
) {
    for _, op := range ops {
        switch t := op.GetValue().(type) {
        case *enginepb.MVCCWriteValueOp:
            p.publishValue(ctx, t.Key, t.Timestamp, t.Value, t.PrevValue, ...)
        case *enginepb.MVCCDeleteRangeOp:
            p.publishDeleteRange(ctx, t.StartKey, t.EndKey, t.Timestamp, alloc)
        case *enginepb.MVCCCommitIntentOp:
            p.publishValue(ctx, t.Key, t.Timestamp, t.Value, t.PrevValue, ...)
        // ... other cases
        }

        // Update resolved timestamp
        if p.rts.ConsumeLogicalOp(ctx, op) {
            p.publishCheckpoint(ctx, nil)
        }
    }
}
```

### 4.5 Publishing to Registrations

[`publishValue`](https://github.com/cockroachdb/cockroach/blob/1b12bbdd504af874ba0fd6e99958681bea968a79/pkg/kv/kvserver/rangefeed/scheduled_processor.go#L772-L798)
creates the `RangeFeedEvent` and routes it to matching registrations:

```go
func (p *ScheduledProcessor) publishValue(...) {
    var event kvpb.RangeFeedEvent
    event.MustSetValue(&kvpb.RangeFeedValue{
        Key: key,
        Value: roachpb.Value{
            RawBytes:  value,
            Timestamp: timestamp,
        },
        PrevValue: prevVal,
    })
    p.reg.PublishToOverlapping(ctx, roachpb.Span{Key: key}, &event, valueMetadata, alloc)
}
```

### 4.6 Registry Distribution

The registry's [`PublishToOverlapping`](https://github.com/cockroachdb/cockroach/blob/1b12bbdd504af874ba0fd6e99958681bea968a79/pkg/kv/kvserver/rangefeed/registry.go#L381-L415)
finds matching registrations using an interval tree:

```go
func (reg *registry) PublishToOverlapping(
    ctx context.Context,
    span roachpb.Span,
    event *kvpb.RangeFeedEvent,
    valueMetadata logicalOpMetadata,
    alloc *SharedBudgetAllocation,
) {
    // Determine minimum timestamp for this event
    var minTS hlc.Timestamp
    switch t := event.GetValue().(type) {
    case *kvpb.RangeFeedValue:
        minTS = t.Value.Timestamp
    // ...
    }

    reg.forOverlappingRegs(ctx, span, func(r registration) (bool, *kvpb.Error) {
        if r.shouldPublishLogicalOp(minTS, valueMetadata) {
            r.publish(ctx, event, alloc)
        }
        return false, nil
    })
}
```

## Phase 5: Registration to Stream

### 5.1 Registration Types

There are two registration implementations:

1. [`bufferedRegistration`](https://github.com/cockroachdb/cockroach/blob/1b12bbdd504af874ba0fd6e99958681bea968a79/pkg/kv/kvserver/rangefeed/buffered_registration.go#L38-L69):
   Used with `UnbufferedSender`, buffers events per-registration
2. [`unbufferedRegistration`](https://github.com/cockroachdb/cockroach/blob/1b12bbdd504af874ba0fd6e99958681bea968a79/pkg/kv/kvserver/rangefeed/unbuffered_registration.go):
   Used with `BufferedSender`, sends directly to the sender's buffer

### 5.2 Buffered Registration Flow

For `bufferedRegistration`, [`publish`](https://github.com/cockroachdb/cockroach/blob/1b12bbdd504af874ba0fd6e99958681bea968a79/pkg/kv/kvserver/rangefeed/buffered_registration.go#L114-L152)
adds events to an internal channel:

```go
func (br *bufferedRegistration) publish(
    ctx context.Context, event *kvpb.RangeFeedEvent, alloc *SharedBudgetAllocation,
) {
    br.mu.Lock()
    defer br.mu.Unlock()
    if br.mu.overflowed || br.mu.disconnected {
        return
    }

    select {
    case br.buf <- e:
        br.testPendingEventToSend.Add(1)
    default:
        // Buffer overflow - registration needs reconnection
        br.mu.overflowed = true
    }
}
```

The registration's [`outputLoop`](https://github.com/cockroachdb/cockroach/blob/1b12bbdd504af874ba0fd6e99958681bea968a79/pkg/kv/kvserver/rangefeed/buffered_registration.go#L202-L266)
runs in a separate goroutine and sends events to the stream:

```go
func (br *bufferedRegistration) outputLoop(ctx context.Context) error {
    // First, run catch-up scan
    if err := br.maybeRunCatchUpScan(ctx); err != nil {
        return errors.Wrap(err, "catch-up scan failed")
    }

    // Then process buffered live events
    for {
        select {
        case nextEvent := <-br.buf:
            err := br.stream.SendUnbuffered(nextEvent.event)
            nextEvent.alloc.Release(ctx)
            if err != nil {
                return err
            }
        case <-ctx.Done():
            return ctx.Err()
        }
    }
}
```

### 5.3 Stream Implementation

The [`PerRangeEventSink`](https://github.com/cockroachdb/cockroach/blob/1b12bbdd504af874ba0fd6e99958681bea968a79/pkg/kv/kvserver/rangefeed/stream.go#L41-L55)
wraps events with routing information:

```go
type PerRangeEventSink struct {
    rangeID  roachpb.RangeID
    streamID int64
    wrapped  sender
}

func (s *PerRangeEventSink) SendUnbuffered(event *kvpb.RangeFeedEvent) error {
    response := &kvpb.MuxRangeFeedEvent{
        RangeFeedEvent: *event,
        RangeID:        s.rangeID,
        StreamID:       s.streamID,
    }
    return s.wrapped.sendUnbuffered(response)
}
```

## Phase 6: Sender to gRPC

### 6.1 BufferedSender

The [`BufferedSender`](https://github.com/cockroachdb/cockroach/blob/1b12bbdd504af874ba0fd6e99958681bea968a79/pkg/kv/kvserver/rangefeed/buffered_sender.go#L73-L101)
provides node-level buffering with per-stream capacity limits:

```go
type BufferedSender struct {
    sender      ServerStreamSender  // The underlying gRPC stream
    sendBufSize int                 // Batch size for sending

    queueMu struct {
        syncutil.Mutex
        stopped           bool
        buffer            *eventQueue
        perStreamCapacity int64
        byStream          map[int64]streamStatus
    }

    notifyDataC chan struct{}
    metrics     *BufferedSenderMetrics
}
```

[`sendBuffered`](https://github.com/cockroachdb/cockroach/blob/1b12bbdd504af874ba0fd6e99958681bea968a79/pkg/kv/kvserver/rangefeed/buffered_sender.go#L174-L216)
enqueues events:

```go
func (bs *BufferedSender) sendBuffered(
    ev *kvpb.MuxRangeFeedEvent, alloc *SharedBudgetAllocation,
) error {
    bs.queueMu.Lock()
    defer bs.queueMu.Unlock()

    // Check per-stream capacity
    status, ok := bs.queueMu.byStream[ev.StreamID]
    if !ok {
        return errNoSuchStream
    }

    // Enqueue event
    bs.queueMu.buffer.pushBack(sharedMuxEvent{ev, alloc})

    // Notify sender goroutine
    select {
    case bs.notifyDataC <- struct{}{}:
    default:
    }
    return nil
}
```

### 6.2 Sender Run Loop

The [`run`](https://github.com/cockroachdb/cockroach/blob/1b12bbdd504af874ba0fd6e99958681bea968a79/pkg/kv/kvserver/rangefeed/buffered_sender.go#L270-L320)
loop batches and sends events:

```go
func (bs *BufferedSender) run(
    ctx context.Context, stopper *stop.Stopper, onError func(streamID int64),
) error {
    eventsBuf := make([]sharedMuxEvent, 0, bs.sendBufSize)

    for {
        select {
        case <-ctx.Done():
            return nil
        case <-bs.notifyDataC:
            for {
                eventsBuf = bs.popEvents(eventsBuf[:0], bs.sendBufSize)
                if len(eventsBuf) == 0 {
                    break
                }

                for _, evt := range eventsBuf {
                    err := bs.sender.Send(evt.ev)
                    evt.alloc.Release(ctx)
                    if err != nil {
                        // Handle error, notify stream manager
                        onError(evt.ev.StreamID)
                    }
                }
            }
        }
    }
}
```

### 6.3 lockedMuxStream to gRPC

Finally, [`lockedMuxStream.Send`](https://github.com/cockroachdb/cockroach/blob/1b12bbdd504af874ba0fd6e99958681bea968a79/pkg/server/node.go#L2131-L2153)
provides thread-safe access to the gRPC stream:

```go
func (s *lockedMuxStream) Send(e *kvpb.MuxRangeFeedEvent) error {
    s.sendMu.Lock()
    defer s.sendMu.Unlock()

    start := crtime.NowMono()
    defer func() {
        dur := start.Elapsed()
        if dur > slowMuxStreamSendThreshold {
            s.metrics.SlowSends.Inc(1)
        }
    }()

    return s.wrapped.Send(e)
}
```

## Phase 7: Closed Timestamp and Checkpoints

Rangefeeds emit checkpoint events to indicate progress. These are driven by
closed timestamp updates.

### 7.1 Closed Timestamp Updates

[`handleClosedTimestampUpdateRaftMuLocked`](https://github.com/cockroachdb/cockroach/blob/1b12bbdd504af874ba0fd6e99958681bea968a79/pkg/kv/kvserver/replica_rangefeed.go#L845-L891)
forwards closed timestamps to the processor:

```go
func (r *Replica) handleClosedTimestampUpdateRaftMuLocked(
    ctx context.Context, closedTS hlc.Timestamp,
) (exceedsSlowLagThresh bool) {
    p := r.getRangefeedProcessor()
    if p == nil {
        return false
    }

    // Check for slow closed timestamp
    signal := r.raftMu.rangefeedCTLagObserver.observeClosedTimestampUpdate(...)

    // Forward to processor
    p.ForwardClosedTS(ctx, closedTS)
}
```

### 7.2 Checkpoint Publishing

When the resolved timestamp advances, [`publishCheckpoint`](https://github.com/cockroachdb/cockroach/blob/1b12bbdd504af874ba0fd6e99958681bea968a79/pkg/kv/kvserver/rangefeed/scheduled_processor.go#L841-L847)
sends checkpoint events:

```go
func (p *ScheduledProcessor) publishCheckpoint(ctx context.Context, alloc *SharedBudgetAllocation) {
    event := p.newCheckpointEvent()
    p.reg.PublishToOverlapping(ctx, all, event, logicalOpMetadata{}, alloc)
}
```

## The Scheduler

The [`Scheduler`](https://github.com/cockroachdb/cockroach/blob/1b12bbdd504af874ba0fd6e99958681bea968a79/pkg/kv/kvserver/rangefeed/scheduler.go#L146-L157)
is a store-wide worker pool that processes rangefeed events efficiently:

```go
type Scheduler struct {
    nextID atomic.Int64
    shards []*schedulerShard  // Multiple shards to reduce contention
    wg     sync.WaitGroup
    // ...
}
```

Key features:
- **Sharding**: Processors are distributed across shards to reduce lock contention
- **Priority scheduling**: System ranges get dedicated workers (shard 0)
- **Event coalescing**: Multiple events of the same type are coalesced into a single callback

## Data Flow Summary

```
1. Raft Apply (replica_app_batch.go)
   └─▶ handleLogicalOpLogRaftMuLocked (replica_rangefeed.go)
       └─▶ ConsumeLogicalOps (scheduled_processor.go)
           └─▶ eventC channel

2. Scheduler invokes process callback
   └─▶ processEvents (scheduled_processor.go)
       └─▶ consumeEvent / consumeLogicalOps
           └─▶ publishValue / publishCheckpoint
               └─▶ reg.PublishToOverlapping (registry.go)
                   └─▶ registration.publish

3. Registration to Stream
   └─▶ bufferedRegistration.outputLoop
       └─▶ stream.SendUnbuffered
           └─▶ PerRangeEventSink.SendUnbuffered
               └─▶ sender.sendUnbuffered / sendBuffered

4. Sender to gRPC
   └─▶ BufferedSender.run
       └─▶ sender.Send (lockedMuxStream)
           └─▶ grpcStream.Send (to client)
```

## Key Design Decisions

1. **raftMu synchronization**: Registration happens under raftMu to ensure no events
   are missed between catch-up scan and live updates.

2. **Memory budgets**: Each rangefeed has a memory budget to prevent unbounded
   memory growth from slow consumers.

3. **Per-stream capacity limits**: The BufferedSender enforces per-stream limits
   to prevent a single slow consumer from affecting others.

4. **Event filtering**: The `Filter` allows skipping value reads for keys no
   registration cares about, crucial for performance.

5. **Scheduler-based processing**: A shared worker pool (scheduler) processes
   events for all rangefeeds, avoiding per-range goroutine overhead.

6. **Multiplexed streams**: MuxRangeFeed allows multiple range subscriptions
   over a single gRPC stream, reducing connection overhead.
