// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnfeed

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// Processor manages TxnFeed registrations for a single range. It receives
// committed transaction events from Raft apply and forwards them to all
// registered streams. It is simpler than the rangefeed Processor because it
// does not track intents or compute resolved timestamps from lock state — the
// resolved timestamp is simply the range's closed timestamp.
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

	mu struct {
		syncutil.RWMutex
		stopped bool
		regs    []*registration
	}
}

// Config holds the configuration for a TxnFeed Processor.
type Config struct {
	log.AmbientContext
	Span    roachpb.RSpan
	Stopper *stop.Stopper
}

// NewProcessor creates a new TxnFeed Processor.
func NewProcessor(cfg Config) *Processor {
	p := &Processor{
		ambientCtx: cfg.AmbientContext,
		span:       cfg.Span,
		stopper:    cfg.Stopper,
	}
	return p
}

// Register registers a new TxnFeed stream for the specified anchor span.
// If a catch-up scan is needed (startTS is non-empty), a short-lived goroutine
// is spawned to run the scan and drain the catch-up buffer. After the goroutine
// exits, live events are delivered directly via Stream.SendBuffered.
//
// The caller must hold raftMu when calling Register to ensure that no
// CommitTxnOps are missed between the catch-up scan snapshot and the
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
	if startTS.IsEmpty() {
		// No catch-up scan needed — close the snapshot immediately.
		snap.Close()
		snap = nil
	}

	reg, err := p.addRegistration(span, startTS, stream, snap)
	if err != nil {
		if snap != nil {
			snap.Close()
		}
		return nil, err
	}

	if reg.needsCatchUp() {
		// Start a short-lived goroutine to run the catch-up scan and drain the
		// catch-up buffer. Once complete, the goroutine exits and future events
		// flow through publish() → SendBuffered().
		if err := p.stopper.RunAsyncTask(
			ctx, "txnfeed: catch-up scan", reg.runOutputLoop,
		); err != nil {
			p.unregister(reg)
			reg.Disconnect(kvpb.NewError(err))
			return nil, err
		}
	}

	return reg, nil
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

// ConsumeCommitTxnOps delivers committed transaction ops from Raft apply to
// all matching registrations. Called under raftMu.
//
// If a registration's catch-up buffer is full, it is marked as overflowed and
// will be disconnected once the catch-up scan goroutine finishes. After the
// catch-up scan, events are sent via Stream.SendBuffered (non-blocking).
func (p *Processor) ConsumeCommitTxnOps(ctx context.Context, ops *kvserverpb.CommitTxnOps) {
	if ops == nil {
		return
	}
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.mu.stopped || len(p.mu.regs) == 0 {
		return
	}

	for i := range ops.Ops {
		op := &ops.Ops[i]
		event := &kvpb.TxnFeedEvent{
			Committed: &kvpb.TxnFeedCommitted{
				TxnID:           op.TxnID,
				AnchorKey:       op.AnchorKey,
				CommitTimestamp: op.CommitTimestamp,
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
}

// ForwardClosedTS forwards the closed timestamp to all registrations as a
// TxnFeedCheckpoint event. Called under raftMu.
func (p *Processor) ForwardClosedTS(ctx context.Context, closedTS hlc.Timestamp) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.mu.stopped || len(p.mu.regs) == 0 {
		return
	}

	for _, reg := range p.mu.regs {
		event := &kvpb.TxnFeedEvent{
			Checkpoint: &kvpb.TxnFeedCheckpoint{
				AnchorSpan: reg.span.AsRawSpanWithNoLocals(),
				ResolvedTS: closedTS,
			},
		}
		reg.publish(event)
	}
}

// Stop stops the processor and disconnects all registrations.
func (p *Processor) Stop() {
	p.StopWithErr(nil)
}

// StopWithErr terminates all registrations with the given error and stops the
// processor.
func (p *Processor) StopWithErr(pErr *kvpb.Error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.mu.stopped {
		return
	}
	p.mu.stopped = true
	for _, reg := range p.mu.regs {
		reg.Disconnect(pErr)
	}
	p.mu.regs = nil
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
}

func (p *Processor) unregister(target *registration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, reg := range p.mu.regs {
		if reg == target {
			p.mu.regs = append(p.mu.regs[:i], p.mu.regs[i+1:]...)
			return
		}
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
