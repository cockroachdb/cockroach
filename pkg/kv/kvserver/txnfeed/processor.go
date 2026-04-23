// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnfeed

import (
	"context"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// Processor manages TxnFeed registrations for a single range. It receives
// transaction lifecycle events from Raft apply and forwards committed
// transaction events to all registered streams.
//
// The processor tracks unresolved (PENDING/STAGING) transaction records via
// the resolvedTimestamp subsystem and computes the resolved timestamp as:
//
//	min(closedTS, oldestUnresolved.WriteTimestamp.Prev())
//
// Checkpoints are emitted to registrations only when the resolved timestamp
// advances, ensuring the guarantee that no future TxnFeedCommitted events
// will be emitted at or below the resolved timestamp.
//
// The processor uses the rangefeed scheduler pattern: event production under
// raftMu is O(1) (channel send + scheduler enqueue), and event consumption
// (iterating registrations, publishing events) runs on scheduler worker
// goroutines. This avoids blocking raftMu during event delivery.
//
// Unlike rangefeed's bufferedRegistration, registrations do not have
// long-lived goroutines. Instead, each registration runs a short-lived
// goroutine only during the catch-up scan. After the catch-up scan, the
// publish method sends events directly via Stream.SendBuffered (non-blocking),
// matching the rangefeed's unbufferedRegistration pattern.
type Processor struct {
	ambientCtx log.AmbientContext
	span       roachpb.RSpan
	stopper    *stop.Stopper

	// scheduler is the per-processor handle to the shared rangefeed scheduler.
	scheduler rangefeed.ClientScheduler
	// processCtx is an annotated background context used by the scheduler
	// callback. It outlives any single RPC context.
	processCtx context.Context
	// eventC buffers events (txn lifecycle ops and closed timestamp updates)
	// for async processing by the scheduler callback.
	eventC chan *txnFeedEvent
	// requestQueue buffers request closures (register, stop, disconnect) for
	// serial execution by the scheduler callback.
	requestQueue chan request
	// stoppedC is closed when the processor has fully stopped and unregistered
	// from the scheduler.
	stoppedC chan struct{}
	// stopping is set when a stop has been initiated. Once set, new events are
	// discarded in the scheduler callback.
	stopping atomic.Bool

	// rts tracks unresolved transaction records and computes the resolved
	// timestamp. Only accessed from the scheduler callback thread (no
	// synchronization needed).
	rts resolvedTimestamp
	// lastEmittedRTS is the last resolved timestamp emitted as a checkpoint
	// event. Used to avoid emitting duplicate checkpoints. Only accessed
	// from the scheduler callback thread.
	lastEmittedRTS hlc.Timestamp

	mu struct {
		syncutil.RWMutex
		stopped bool
		regs    []*registration
	}
}

// txnFeedEvent is an event enqueued for async processing by the scheduler.
// Only one field is set per event.
type txnFeedEvent struct {
	ops      *kvserverpb.TxnFeedOps // txn lifecycle ops from Raft apply
	ct       hlc.Timestamp          // closed timestamp forward
	initScan *initScanResult        // buffered init scan results
	sync     *syncEvent             // synchronization barrier
}

// initScanResult holds the buffered results of the initial unresolved txn
// record scan. Sent as a single event through eventC after the async scan
// completes.
type initScanResult struct {
	records []initScanRecord
}

// initScanRecord is a single unresolved transaction record discovered
// during the initial scan.
type initScanRecord struct {
	txnID   uuid.UUID
	writeTS hlc.Timestamp
}

// syncEvent is used to synchronize with the scheduler callback. The caller
// sends a syncEvent through eventC and waits on c; the scheduler callback
// closes c after processing all prior events.
type syncEvent struct {
	c chan struct{}
}

// request is a closure executed serially on the scheduler callback thread.
type request func(context.Context)

// Config holds the configuration for a TxnFeed Processor.
type Config struct {
	log.AmbientContext
	Span      roachpb.RSpan
	Stopper   *stop.Stopper
	Scheduler *rangefeed.Scheduler
}

// NewProcessor creates a new TxnFeed Processor. The caller must call Start
// to register with the scheduler before delivering events.
func NewProcessor(cfg Config) *Processor {
	cfg.AmbientContext.AddLogTag("txnfeed", nil)
	p := &Processor{
		ambientCtx:   cfg.AmbientContext,
		span:         cfg.Span,
		stopper:      cfg.Stopper,
		scheduler:    cfg.Scheduler.NewClientScheduler(),
		processCtx:   cfg.AmbientContext.AnnotateCtx(context.Background()),
		eventC:       make(chan *txnFeedEvent, 16),
		requestQueue: make(chan request, 20),
		stoppedC:     make(chan struct{}),
	}
	return p
}

// Start registers the processor with the scheduler. Must be called after
// NewProcessor and before any events are delivered.
func (p *Processor) Start() error {
	return p.scheduler.Register(p.process, false /* priority */)
}

// InitAsync launches an asynchronous scan of unresolved transaction records
// to initialize the resolved timestamp. The snapshot must be taken under
// raftMu to ensure atomicity with the start of live event delivery; the
// actual scan runs on a background goroutine via the stopper.
//
// On completion, the scan results are sent through eventC and processed on
// the scheduler callback thread (where rts is safe to access). Events
// arriving before initialization are tracked but do not produce checkpoints
// until Init completes.
//
// The snapshot is always closed by this method or the background goroutine.
func (p *Processor) InitAsync(ctx context.Context, snap storage.Reader) error {
	err := p.stopper.RunAsyncTask(
		ctx, "txnfeed: init resolved ts", func(ctx context.Context) {
			defer snap.Close()
			var result initScanResult
			scanErr := ScanUnresolvedTxnRecords(
				ctx, snap, p.span.Key, p.span.EndKey,
				func(txnID uuid.UUID, writeTS hlc.Timestamp) {
					result.records = append(
						result.records,
						initScanRecord{txnID: txnID, writeTS: writeTS},
					)
				},
			)
			if scanErr != nil {
				scanErr = errors.Wrap(
					scanErr, "initial resolved timestamp scan failed")
				if ctx.Err() == nil {
					log.KvExec.Errorf(ctx, "%v", scanErr)
				}
				p.StopWithErr(kvpb.NewError(scanErr))
				return
			}
			p.sendEvent(ctx, &txnFeedEvent{initScan: &result})
		},
	)
	if err != nil {
		snap.Close()
		return err
	}
	return nil
}

// Register registers a new TxnFeed stream for the specified anchor span.
// If a catch-up scan is needed (startTS is non-empty), a short-lived goroutine
// is spawned to run the scan and drain the catch-up buffer. After the goroutine
// exits, live events are delivered directly via Stream.SendBuffered.
//
// The caller must hold raftMu when calling Register to ensure that no
// TxnFeedOps are missed between the catch-up scan snapshot and the
// start of live event delivery.
//
// NB: startTS is exclusive; the first possible event will have an MVCC
// timestamp strictly after startTS.
func (p *Processor) Register(
	ctx context.Context,
	span roachpb.RSpan,
	startTS hlc.Timestamp,
	snap storage.Reader,
	stream Stream,
) (Disconnector, error) {
	// Synchronize the event channel so that this registration doesn't see any
	// events that were consumed before this registration was called. Instead,
	// it should see these events during its catch-up scan.
	p.syncEventC()

	type result struct {
		disconnector Disconnector
		err          error
	}
	ch := make(chan result, 1)

	p.enqueueRequest(func(_ context.Context) {
		if p.stopping.Load() {
			if snap != nil {
				snap.Close()
			}
			ch <- result{err: errors.New("txnfeed processor is stopped")}
			return
		}

		if startTS.IsEmpty() && snap != nil {
			snap.Close()
			snap = nil
		}

		reg, err := p.addRegistration(span, startTS, stream, snap)
		if err != nil {
			if snap != nil {
				snap.Close()
			}
			ch <- result{err: err}
			return
		}

		if reg.needsCatchUp() {
			if err := p.stopper.RunAsyncTask(
				ctx, "txnfeed: catch-up scan", reg.runOutputLoop,
			); err != nil {
				p.unregisterInternal(reg)
				reg.Disconnect(kvpb.NewError(err))
				ch <- result{err: err}
				return
			}
		}

		ch <- result{disconnector: reg}
	})

	select {
	case r := <-ch:
		return r.disconnector, r.err
	case <-p.stoppedC:
		select {
		case r := <-ch:
			return r.disconnector, r.err
		default:
			return nil, errors.New("txnfeed processor stopped")
		}
	}
}

func (p *Processor) addRegistration(
	span roachpb.RSpan, startTS hlc.Timestamp, stream Stream, snap storage.Reader,
) (*registration, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.mu.stopped {
		return nil, errors.New("txnfeed processor is stopped")
	}
	reg := &registration{
		span:    span,
		startTS: startTS,
		stream:  stream,
	}
	reg.mu.snap = snap
	if !startTS.IsEmpty() {
		reg.mu.catchUpBuf = make(
			chan *kvpb.TxnFeedEvent, kvserverbase.DefaultRangefeedEventCap)
	}
	reg.unregisterFromProcessor = func() { p.unregister(reg) }
	p.mu.regs = append(p.mu.regs, reg)
	return reg, nil
}

// ConsumeTxnFeedOps enqueues transaction lifecycle ops for async processing
// by the scheduler. Called under raftMu — O(1).
func (p *Processor) ConsumeTxnFeedOps(ctx context.Context, ops *kvserverpb.TxnFeedOps) {
	if ops == nil {
		return
	}
	p.sendEvent(ctx, &txnFeedEvent{ops: ops})
}

// ForwardClosedTS enqueues a closed timestamp update for async processing by
// the scheduler. Called under raftMu — O(1).
func (p *Processor) ForwardClosedTS(ctx context.Context, closedTS hlc.Timestamp) {
	if closedTS.IsEmpty() {
		return
	}
	p.sendEvent(ctx, &txnFeedEvent{ct: closedTS})
}

// Stop stops the processor and disconnects all registrations.
func (p *Processor) Stop() {
	p.StopWithErr(nil)
}

// StopWithErr flushes pending events and then enqueues a stop request that
// terminates all registrations with the given error.
func (p *Processor) StopWithErr(pErr *kvpb.Error) {
	// Flush any remaining events before stopping.
	p.syncEventC()
	// Send the processor a stop signal.
	p.sendStop(pErr)
}

func (p *Processor) sendStop(pErr *kvpb.Error) {
	p.enqueueRequest(func(ctx context.Context) {
		p.stopInternal(ctx, pErr)
	})
}

// nolint:deferunlockcheck
func (p *Processor) stopInternal(ctx context.Context, pErr *kvpb.Error) {
	p.mu.Lock()
	if p.mu.stopped {
		p.mu.Unlock()
		return
	}
	p.mu.stopped = true
	regs := p.mu.regs
	p.mu.regs = nil
	p.mu.Unlock()

	// Disconnect registrations outside the lock to avoid holding mu while
	// calling into stream.SendError which may block.
	for _, reg := range regs {
		reg.Disconnect(pErr)
	}
	p.stopping.Store(true)
	p.scheduler.StopProcessor()
}

// Len returns the number of active registrations.
func (p *Processor) Len() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.mu.regs)
}

// DisconnectSpanWithErr disconnects all registrations that overlap the given
// span with the given error.
func (p *Processor) DisconnectSpanWithErr(span roachpb.Span, pErr *kvpb.Error) {
	p.enqueueRequest(func(ctx context.Context) {
		p.mu.Lock()
		defer p.mu.Unlock()

		remaining := p.mu.regs[:0]
		for _, reg := range p.mu.regs {
			if reg.span.AsRawSpanWithNoLocals().Overlaps(span) {
				reg.Disconnect(pErr)
			} else {
				remaining = append(remaining, reg)
			}
		}
		p.mu.regs = remaining
	})
}

// unregister enqueues a request to remove the registration from the
// processor's registration list. Used as the unregisterFromProcessor callback
// on each registration.
func (p *Processor) unregister(target *registration) {
	p.enqueueRequest(func(ctx context.Context) {
		p.unregisterInternal(target)
	})
}

// unregisterInternal removes the registration from the list. Must be called on
// the scheduler callback thread.
func (p *Processor) unregisterInternal(target *registration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, reg := range p.mu.regs {
		if reg == target {
			p.mu.regs = append(p.mu.regs[:i], p.mu.regs[i+1:]...)
			return
		}
	}
}

// process is the scheduler callback. It processes pending requests and events.
func (p *Processor) process(e rangefeed.ProcessorEventType) rangefeed.ProcessorEventType {
	ctx := p.processCtx
	if e&rangefeed.RequestQueued != 0 {
		p.processRequests(ctx)
	}
	if e&rangefeed.EventQueued != 0 {
		p.processEvents(ctx)
	}
	if e&rangefeed.Stopped != 0 {
		p.processStop()
	}
	return 0
}

func (p *Processor) processRequests(ctx context.Context) {
	for {
		select {
		case req := <-p.requestQueue:
			req(ctx)
		default:
			return
		}
	}
}

func (p *Processor) processEvents(ctx context.Context) {
	// Only process as many events as are currently buffered to avoid starving
	// other processors sharing the same scheduler.
	for max := len(p.eventC); max > 0; max-- {
		select {
		case e := <-p.eventC:
			if !p.stopping.Load() {
				p.consumeEvent(ctx, e)
			}
		default:
			return
		}
	}
}

func (p *Processor) consumeEvent(ctx context.Context, e *txnFeedEvent) {
	switch {
	case e.ops != nil:
		p.consumeTxnFeedOps(ctx, e.ops)
	case !e.ct.IsEmpty():
		p.forwardClosedTS(ctx, e.ct)
	case e.initScan != nil:
		p.consumeInitScan(ctx, e.initScan)
	case e.sync != nil:
		close(e.sync.c)
	}
}

// consumeTxnFeedOps processes transaction lifecycle ops. It updates the
// resolved timestamp tracker and delivers COMMITTED ops to matching
// registrations. Called on the scheduler callback thread.
func (p *Processor) consumeTxnFeedOps(ctx context.Context, ops *kvserverpb.TxnFeedOps) {
	for i := range ops.Ops {
		op := &ops.Ops[i]
		switch op.Type {
		case kvserverpb.TxnFeedOp_RECORD_WRITTEN:
			p.rts.ConsumeRecordWritten(ctx, op.TxnID, op.WriteTimestamp)
		case kvserverpb.TxnFeedOp_COMMITTED:
			p.rts.ConsumeCommitted(ctx, op.TxnID)
			p.publishCommitted(ctx, op)
		case kvserverpb.TxnFeedOp_ABORTED:
			p.rts.ConsumeAborted(ctx, op.TxnID)
		default:
			log.KvExec.Fatalf(ctx, "unknown TxnFeedOp type: %v", op.Type)
		}
	}
	p.maybeEmitCheckpoints(ctx)
}

// consumeInitScan processes the buffered results from the initial unresolved
// txn record scan and initializes the resolved timestamp. Records that were
// already committed or aborted by live events (tracked in preInitRemovals)
// are skipped. Called on the scheduler callback thread.
func (p *Processor) consumeInitScan(ctx context.Context, result *initScanResult) {
	for i := range result.records {
		rec := &result.records[i]
		if p.rts.wasRemovedBeforeInit(rec.txnID) {
			continue
		}
		p.rts.txnQ.Add(rec.txnID, rec.writeTS)
	}
	p.rts.Init(ctx)
	p.maybeEmitCheckpoints(ctx)
}

// publishCommitted delivers a COMMITTED event to matching registrations.
func (p *Processor) publishCommitted(ctx context.Context, op *kvserverpb.TxnFeedOp) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.mu.stopped || len(p.mu.regs) == 0 {
		return
	}

	event := &kvpb.TxnFeedEvent{
		Committed: &kvpb.TxnFeedCommitted{
			TxnID:           op.TxnID,
			AnchorKey:       op.AnchorKey,
			CommitTimestamp: op.WriteTimestamp,
			WriteSpans:      op.WriteSpans,
			ReadSpans:       op.ReadSpans,
		},
	}
	for _, reg := range p.mu.regs {
		if !reg.containsKey(op.AnchorKey) {
			continue
		}
		reg.publish(event)
	}
}

// forwardClosedTS advances the closed timestamp and emits checkpoint events
// if the resolved timestamp advanced. Called on the scheduler callback thread.
func (p *Processor) forwardClosedTS(ctx context.Context, closedTS hlc.Timestamp) {
	p.rts.ForwardClosedTS(ctx, closedTS)
	p.maybeEmitCheckpoints(ctx)
}

// maybeEmitCheckpoints emits TxnFeedCheckpoint events to all registrations
// if the resolved timestamp has advanced past the last emitted checkpoint.
func (p *Processor) maybeEmitCheckpoints(ctx context.Context) {
	resolvedTS := p.rts.Get()
	if resolvedTS.IsEmpty() || !p.lastEmittedRTS.Less(resolvedTS) {
		return
	}
	p.lastEmittedRTS = resolvedTS

	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.mu.stopped || len(p.mu.regs) == 0 {
		return
	}

	for _, reg := range p.mu.regs {
		event := &kvpb.TxnFeedEvent{
			Checkpoint: &kvpb.TxnFeedCheckpoint{
				AnchorSpan: reg.span.AsRawSpanWithNoLocals(),
				ResolvedTS: resolvedTS,
			},
		}
		reg.publish(event)
	}
}

func (p *Processor) processStop() {
	p.scheduler.Unregister()
	close(p.stoppedC)
}

// sendEvent puts an event on the event channel and notifies the scheduler.
func (p *Processor) sendEvent(ctx context.Context, e *txnFeedEvent) {
	select {
	case p.eventC <- e:
		p.scheduler.Enqueue(rangefeed.EventQueued)
	case <-p.stoppedC:
	}
}

// enqueueRequest puts a request closure on the request queue and notifies the
// scheduler.
func (p *Processor) enqueueRequest(req request) {
	select {
	case p.requestQueue <- req:
		p.scheduler.Enqueue(rangefeed.RequestQueued)
	case <-p.stoppedC:
	}
}

// syncEventC synchronizes with the scheduler callback by flushing the event
// pipeline. It blocks until all events enqueued before this call have been
// processed.
func (p *Processor) syncEventC() {
	se := &syncEvent{c: make(chan struct{})}
	select {
	case p.eventC <- &txnFeedEvent{sync: se}:
		p.scheduler.Enqueue(rangefeed.EventQueued)
		select {
		case <-se.c:
		case <-p.stoppedC:
		}
	case <-p.stoppedC:
	}
}

// registration tracks a single TxnFeed client subscription. It follows the
// rangefeed unbufferedRegistration pattern: a short-lived goroutine runs the
// catch-up scan and drains the catchUpBuf, then exits. After that, live events
// are delivered directly via Stream.SendBuffered (non-blocking) from the
// publish method, with no long-lived goroutine required.
type registration struct {
	span    roachpb.RSpan
	startTS hlc.Timestamp
	stream  Stream

	// unregisterFromProcessor removes this registration from the processor's
	// registration list. Set at construction time. Must be non-blocking.
	unregisterFromProcessor func()

	mu struct {
		syncutil.Mutex
		disconnected bool
		// catchUpOverflowed is set when catchUpBuf is full and a live event is
		// dropped. The catch-up goroutine detects this and disconnects the
		// registration after draining remaining buffered events.
		catchUpOverflowed bool
		// catchUpScanCancelFn cancels the context of the catch-up scan
		// goroutine. Set when the goroutine starts; called by Disconnect.
		catchUpScanCancelFn func()

		// catchUpBuf holds live events published via publish() while the catch-up
		// scan is running. Set to nil once the catch-up scan completes and the
		// buffer has been drained. When nil, publish() sends events directly to
		// stream.SendBuffered().
		//
		// NB: Readers read from this channel without holding mu, as there are
		// not (and should never be) concurrent readers. See publishCatchUpBuffer
		// and drainCatchUpBuf.
		catchUpBuf chan *kvpb.TxnFeedEvent

		// snap is the MVCC snapshot for the catch-up scan. Taken under raftMu in
		// Register(), consumed and closed by runOutputLoop(). nil if no catch-up
		// scan is needed.
		snap storage.Reader
	}
}

// needsCatchUp returns true if the registration needs a catch-up scan.
func (r *registration) needsCatchUp() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.catchUpBuf != nil
}

// publish sends a single event to this registration. It is called by the
// processor if the event overlaps the span this registration is interested in.
//
// After the catch-up scan completes (catchUpBuf == nil), events are sent
// directly to Stream.SendBuffered (non-blocking). During the catch-up scan,
// events are buffered in catchUpBuf.
func (r *registration) publish(event *kvpb.TxnFeedEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.mu.disconnected || r.mu.catchUpOverflowed {
		return
	}

	if r.mu.catchUpBuf == nil {
		// Catch-up scan is done (or was not needed). Send directly to the
		// buffered stream (non-blocking).
		if err := r.stream.SendBuffered(event); err != nil {
			r.disconnectLocked(kvpb.NewError(err))
		}
		return
	}

	// Catch-up scan is still running. Buffer the event.
	select {
	case r.mu.catchUpBuf <- event:
	default:
		r.mu.catchUpOverflowed = true
	}
}

// runOutputLoop is run in a short-lived goroutine. It runs the catch-up scan,
// then drains the catchUpBuf into Stream.SendBuffered. Once complete, the
// goroutine exits and future events flow through publish() → SendBuffered().
//
// nolint:deferunlockcheck
func (r *registration) runOutputLoop(ctx context.Context) {
	defer r.drainCatchUpBuf(ctx)

	r.mu.Lock()
	if r.mu.disconnected {
		r.mu.Unlock()
		return
	}
	ctx, r.mu.catchUpScanCancelFn = context.WithCancel(ctx)
	r.mu.Unlock()

	if err := r.maybeRunCatchUpScan(ctx); err != nil {
		r.Disconnect(kvpb.NewError(err))
		return
	}

	if err := r.publishCatchUpBuffer(ctx); err != nil {
		r.Disconnect(kvpb.NewError(err))
		return
	}
}

// maybeRunCatchUpScan runs the catch-up scan if a snapshot was provided. The
// snapshot is consumed and closed by this method. Catch-up events are sent
// directly to Stream.SendUnbuffered (blocking, direct to gRPC) because the
// BufferedSender may not be fully operational yet and we want catch-up events
// to reach the client promptly.
func (r *registration) maybeRunCatchUpScan(ctx context.Context) error {
	snap := r.detachSnap()
	if snap == nil {
		return nil
	}
	defer snap.Close()

	return CatchUpScan(
		ctx, snap, r.span.Key, r.span.EndKey, r.startTS,
		func(event *kvpb.TxnFeedEvent) error {
			return r.stream.SendUnbuffered(event)
		},
	)
}

// detachSnap detaches the snapshot from the registration so it can be used
// by the catch-up scan. Returns nil if no snapshot was attached.
func (r *registration) detachSnap() storage.Reader {
	r.mu.Lock()
	defer r.mu.Unlock()
	snap := r.mu.snap
	r.mu.snap = nil
	return snap
}

// publishCatchUpBuffer drains the catchUpBuf and sends events to
// Stream.SendBuffered. It follows a two-phase drain pattern: first drain
// without the lock (to avoid blocking publish()), then drain again with the
// lock to catch events added during the first drain.
//
// nolint:deferunlockcheck
func (r *registration) publishCatchUpBuffer(ctx context.Context) error {
	drain := func() error {
		for {
			select {
			case event := <-r.mu.catchUpBuf:
				if err := r.stream.SendBuffered(event); err != nil {
					return err
				}
			case <-ctx.Done():
				return ctx.Err()
			default:
				// Buffer is empty.
				return nil
			}
		}
	}

	// Drain without holding the lock first.
	if err := drain(); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Drain again with the lock held to catch events added while we were
	// draining.
	if err := drain(); err != nil {
		return err
	}

	if r.mu.catchUpOverflowed {
		return errors.New("txnfeed registration catch-up buffer overflow")
	}

	// Signal to publish() that it can now send directly to SendBuffered.
	r.mu.catchUpBuf = nil
	return nil
}

// drainCatchUpBuf discards any remaining events in the catch-up buffer. This
// is called on error paths to ensure the buffer is cleaned up. It is safe to
// call even if the catch-up scan completed successfully (catchUpBuf will be
// nil).
func (r *registration) drainCatchUpBuf(_ context.Context) {
	r.mu.Lock()
	buf := r.mu.catchUpBuf
	r.mu.catchUpBuf = nil
	r.mu.Unlock()

	if buf == nil {
		return
	}

	for {
		select {
		case <-buf:
		default:
			return
		}
	}
}

func (r *registration) containsKey(key roachpb.Key) bool {
	rKey, err := keys.Addr(key)
	if err != nil {
		return false
	}
	return r.span.ContainsKey(rKey)
}

// Disconnect implements Disconnector.
func (r *registration) Disconnect(pErr *kvpb.Error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.disconnectLocked(pErr)
}

func (r *registration) disconnectLocked(pErr *kvpb.Error) {
	if r.mu.disconnected {
		return
	}
	r.mu.disconnected = true
	if r.mu.snap != nil {
		r.mu.snap.Close()
		r.mu.snap = nil
	}
	if r.mu.catchUpScanCancelFn != nil {
		r.mu.catchUpScanCancelFn()
	}
	r.stream.SendError(pErr)
}

// IsDisconnected implements Disconnector.
func (r *registration) IsDisconnected() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.disconnected
}

// Unregister implements Disconnector.
func (r *registration) Unregister() {
	r.unregisterFromProcessor()
}
