// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvcoord

import (
	"context"
	"io"
	"net"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangecache"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/future"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/limit"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/pprofutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

// rangefeedMuxer is responsible for coordination and management of mux
// rangefeeds. rangefeedMuxer caches MuxRangeFeed stream per node, and executes
// each range feed request on an appropriate node.
type rangefeedMuxer struct {
	// Context group controlling execution of MuxRangeFeed calls. When this group
	// cancels, the entire muxer shuts down.
	g ctxgroup.Group

	ds         *DistSender
	cfg        rangeFeedConfig
	registry   *rangeFeedRegistry
	catchupSem *limit.ConcurrentRequestLimiter
	eventCh    chan<- RangeFeedMessage

	// Each call to start new range feed gets a unique ID which is echoed back
	// by MuxRangeFeed rpc.  This is done as a safety mechanism to make sure
	// that we always send the event to the correct consumer -- even if the
	// range feed is terminated and re-established rapidly.
	// Accessed atomically.
	seqID int64

	// muxClient is a nodeID -> *muxStreamOrError
	muxClients syncutil.IntMap
}

// muxRangeFeed is an entry point to establish MuxRangeFeed
// RPC for the specified spans. Waits for rangefeed to complete.
func muxRangeFeed(
	ctx context.Context,
	cfg rangeFeedConfig,
	spans []SpanTimePair,
	ds *DistSender,
	rr *rangeFeedRegistry,
	catchupSem *limit.ConcurrentRequestLimiter,
	eventCh chan<- RangeFeedMessage,
) (retErr error) {
	// TODO(yevgeniy): Undo log.V(0) and replace with log.V(1) once stuck rangefeed issue resolved.
	if log.V(0) {
		log.Infof(ctx, "Establishing MuxRangeFeed (%s...; %d spans)", spans[0], len(spans))
		start := timeutil.Now()
		defer func() {
			log.Infof(ctx, "MuxRangeFeed  terminating after %s with err=%v", timeutil.Since(start), retErr)
		}()
	}

	m := &rangefeedMuxer{
		g:          ctxgroup.WithContext(ctx),
		registry:   rr,
		ds:         ds,
		cfg:        cfg,
		catchupSem: catchupSem,
		eventCh:    eventCh,
	}
	divideAllSpansOnRangeBoundaries(spans, m.startSingleRangeFeed, ds, &m.g)
	return errors.CombineErrors(m.g.Wait(), ctx.Err())
}

// muxStream represents MuxRangeFeed RPC established with a node.
//
// MuxRangeFeed is a bidirectional RPC: the muxStream.sender is the client ->
// server portion of the stream, and muxStream.receiver is the server -> client
// portion. Any number of RangeFeedRequests may be initiated with the server
// (sender.Send). The server will send MuxRangeFeed for all the range feeds, and
// those events are received via receiver.Recv. If an error occurs with one of
// the logical range feeds, a MuxRangeFeedEvent describing the error will be
// emitted.  This error can be handled appropriately, and rangefeed may be
// restarted.  The sender and receiver may continue to be used to handle other
// requests and events.  However, if either sender or receiver return an error,
// the entire stream must be torn down, and all active range feeds should be
// restarted.
type muxStream struct {
	nodeID roachpb.NodeID

	// mu must be held when starting rangefeed.
	mu struct {
		syncutil.Mutex
		sender  rangeFeedRequestSender
		streams map[int64]*activeMuxRangeFeed
		closed  bool
	}
}

// muxStreamOrError is a tuple of mux stream connection or an error that
// occurred while connecting to the node.
type muxStreamOrError struct {
	stream *muxStream
	err    error
}

// activeMuxRangeFeed augments activeRangeFeed with additional state.
type activeMuxRangeFeed struct {
	*activeRangeFeed
	token      rangecache.EvictionToken
	startAfter hlc.Timestamp
	catchupRes catchupAlloc
}

func (a *activeMuxRangeFeed) release() {
	a.activeRangeFeed.release()
	if a.catchupRes != nil {
		a.catchupRes.Release()
	}
}

// the "Send" portion of the kvpb.Internal_MuxRangeFeedClient
type rangeFeedRequestSender interface {
	Send(req *kvpb.RangeFeedRequest) error
}

// the "Recv" portion of the kvpb.Internal_MuxRangeFeedClient.
type muxRangeFeedEventReceiver interface {
	Recv() (*kvpb.MuxRangeFeedEvent, error)
}

// startSingleRangeFeed looks up routing information for the
// span, and begins execution of rangefeed.
func (m *rangefeedMuxer) startSingleRangeFeed(
	ctx context.Context, rs roachpb.RSpan, startAfter hlc.Timestamp, token rangecache.EvictionToken,
) error {
	// Bound the partial rangefeed to the partial span.
	span := rs.AsRawSpanWithNoLocals()

	var releaseTransport func()
	maybeReleaseTransport := func() {
		if releaseTransport != nil {
			releaseTransport()
			releaseTransport = nil
		}
	}
	defer maybeReleaseTransport()

	// Before starting single rangefeed, acquire catchup scan quota.
	catchupRes, err := acquireCatchupScanQuota(ctx, m.ds, m.catchupSem)
	if err != nil {
		return err
	}

	// Register active mux range feed.
	stream := &activeMuxRangeFeed{
		activeRangeFeed: newActiveRangeFeed(span, startAfter, m.registry, m.ds.metrics.RangefeedRanges),
		startAfter:      startAfter,
		catchupRes:      catchupRes,
		token:           token,
	}
	streamID := atomic.AddInt64(&m.seqID, 1)

	// stream ownership gets transferred (indicated by setting stream to nil) when
	// we successfully send request. If we fail to do so, cleanup.
	defer func() {
		if stream != nil {
			stream.release()
		}
	}()

	// Start a retry loop for sending the batch to the range.
	for r := retry.StartWithCtx(ctx, m.ds.rpcRetryOptions); r.Next(); {
		maybeReleaseTransport()

		// If we've cleared the descriptor on failure, re-lookup.
		if !token.Valid() {
			var err error
			ri, err := m.ds.getRoutingInfo(ctx, rs.Key, rangecache.EvictionToken{}, false)
			if err != nil {
				log.VErrEventf(ctx, 0, "range descriptor re-lookup failed: %s", err)
				if !rangecache.IsRangeLookupErrorRetryable(err) {
					return err
				}
				continue
			}
			token = ri
		}

		// Establish a RangeFeed for a single Range.
		log.VEventf(ctx, 0, "MuxRangeFeed starting for range %s@%s (rangeID %d, attempt %d)",
			span, startAfter, token.Desc().RangeID, r.CurrentAttempt())
		transport, err := newTransportForRange(ctx, token.Desc(), m.ds)
		if err != nil {
			log.VErrEventf(ctx, 0, "Failed to create transport for %s (err=%s) ", token.String(), err)
			continue
		}
		releaseTransport = transport.Release

		for !transport.IsExhausted() {
			args := makeRangeFeedRequest(span, token.Desc().RangeID, m.cfg.overSystemTable, startAfter, m.cfg.withDiff)
			args.Replica = transport.NextReplica()
			args.StreamID = streamID

			rpcClient, err := transport.NextInternalClient(ctx)
			if err != nil {
				log.VErrEventf(ctx, 0, "RPC error connecting to replica %s: %s", args.Replica, err)
				continue
			}

			conn, err := m.establishMuxConnection(ctx, rpcClient, args.Replica.NodeID)
			if err != nil {
				return err
			}

			if err := conn.startRangeFeed(streamID, stream, &args); err != nil {
				log.VErrEventf(ctx, 0,
					"RPC error establishing mux rangefeed to replica %s: %s", args.Replica, err)
				continue
			}
			// We successfully established rangefeed, so the responsibility
			// for releasing the stream is transferred to the mux go routine.
			stream = nil
			return nil
		}

		// If the transport is exhausted, we evict routing token and retry range
		// resolution.
		token.Evict(ctx)
		token = rangecache.EvictionToken{}
	}

	return ctx.Err()
}

// establishMuxConnection establishes MuxRangeFeed RPC with the node.
func (m *rangefeedMuxer) establishMuxConnection(
	ctx context.Context, client rpc.RestrictedInternalClient, nodeID roachpb.NodeID,
) (*muxStream, error) {
	ptr, exists := m.muxClients.LoadOrStore(int64(nodeID), unsafe.Pointer(future.Make[muxStreamOrError]()))
	muxClient := (*future.Future[muxStreamOrError])(ptr)
	if !exists {
		// Start mux rangefeed go routine responsible for receiving MuxRangeFeedEvents.
		m.g.GoCtx(func(ctx context.Context) error {
			return m.startNodeMuxRangeFeed(ctx, client, nodeID, muxClient)
		})
	}

	// Ensure mux client is ready.
	init := future.MakeAwaitableFuture(muxClient)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-init.Done():
		c := init.Get()
		return c.stream, c.err
	}
}

// startNodeMuxRangeFeedLocked establishes MuxRangeFeed RPC with the node.
func (m *rangefeedMuxer) startNodeMuxRangeFeed(
	ctx context.Context,
	client rpc.RestrictedInternalClient,
	nodeID roachpb.NodeID,
	stream *future.Future[muxStreamOrError],
) (retErr error) {
	ctx = logtags.AddTag(ctx, "mux_n", nodeID)
	// Add "generation" number to the context so that log messages and stacks can
	// differentiate between multiple instances of mux rangefeed Go routine
	// (this can happen when one was shutdown, then re-established).
	ctx = logtags.AddTag(ctx, "gen", atomic.AddInt64(&m.seqID, 1))
	ctx, restore := pprofutil.SetProfilerLabelsFromCtxTags(ctx)
	defer restore()

	if log.V(0) {
		log.Infof(ctx, "Establishing MuxRangeFeed to node %d", nodeID)
		start := timeutil.Now()
		defer func() {
			log.Infof(ctx, "MuxRangeFeed to node %d terminating after %s with err=%v",
				nodeID, timeutil.Since(start), retErr)
		}()
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	mux, err := client.MuxRangeFeed(ctx)
	if err != nil {
		return future.MustSet(stream, muxStreamOrError{err: err})
	}

	ms := muxStream{nodeID: nodeID}
	ms.mu.sender = mux
	ms.mu.streams = make(map[int64]*activeMuxRangeFeed)
	if err := future.MustSet(stream, muxStreamOrError{stream: &ms}); err != nil {
		return err
	}

	stuckWatcher := newStuckRangeFeedCanceler(cancel, defaultStuckRangeThreshold(m.ds.st))
	defer stuckWatcher.stop()

	if recvErr := m.receiveEventsFromNode(ctx, mux, stuckWatcher, &ms); recvErr != nil {
		// Clear out this client, and restart all streams on this node.
		// Note: there is a race here where we may delete this muxClient, while
		// another go routine loaded it.  That's fine, since we would not
		// be able to send new request on this stream anymore, and we'll retry
		// against another node.
		m.muxClients.Delete(int64(nodeID))

		if recvErr == io.EOF {
			recvErr = nil
		}

		// make sure that the underlying error is not fatal. If it is, there is no
		// reason to restart each rangefeed, so just bail out.
		if _, err := handleRangefeedError(ctx, recvErr); err != nil {
			return err
		}

		toRestart := ms.close()
		if log.V(0) {
			log.Infof(ctx, "mux to node %d restarted %d streams", ms.nodeID, len(toRestart))
		}
		return m.restartActiveRangeFeeds(ctx, recvErr, toRestart)
	}

	return nil
}

// receiveEventsFromNode receives mux rangefeed events from a node.
func (m *rangefeedMuxer) receiveEventsFromNode(
	ctx context.Context,
	receiver muxRangeFeedEventReceiver,
	stuckWatcher *stuckRangeFeedCanceler,
	ms *muxStream,
) error {
	stuckThreshold := defaultStuckRangeThreshold(m.ds.st)
	stuckCheckFreq := func() time.Duration {
		if threshold := stuckThreshold(); threshold > 0 {
			return threshold
		}
		return time.Minute
	}
	nextStuckCheck := timeutil.Now().Add(stuckCheckFreq())

	var event *kvpb.MuxRangeFeedEvent
	for {
		if err := stuckWatcher.do(func() (err error) {
			event, err = receiver.Recv()
			return err
		}); err != nil {
			return err
		}

		active := ms.lookupStream(event.StreamID)

		// The stream may already have terminated. That's fine -- we may have
		// encountered range split or similar rangefeed error, causing the caller to
		// exit (and terminate this stream), but the server side stream termination
		// is async and probabilistic (rangefeed registration output loop may have a
		// checkpoint event available, *and* it may have context cancellation, but
		// which one executes is a coin flip) and so it is possible that we may see
		// additional event(s) arriving for a stream that is no longer active.
		if active == nil {
			if log.V(0) {
				log.Infof(ctx, "received stray event stream %d: %v", event.StreamID, event)
			}
			continue
		}

		switch t := event.GetValue().(type) {
		case *kvpb.RangeFeedCheckpoint:
			if t.Span.Contains(active.Span) {
				// If we see the first non-empty checkpoint, we know we're done with the catchup scan.
				if !t.ResolvedTS.IsEmpty() && active.catchupRes != nil {
					active.catchupRes.Release()
					active.catchupRes = nil
				}
				// Note that this timestamp means that all rows in the span with
				// writes at or before the timestamp have now been seen. The
				// Timestamp field in the request is exclusive, meaning if we send
				// the request with exactly the ResolveTS, we'll see only rows after
				// that timestamp.
				active.startAfter.Forward(t.ResolvedTS)
			}
		case *kvpb.RangeFeedError:
			log.VErrEventf(ctx, 2, "RangeFeedError: %s", t.Error.GoError())
			if active.catchupRes != nil {
				m.ds.metrics.RangefeedErrorCatchup.Inc(1)
			}
			ms.deleteStream(event.StreamID)
			// Restart rangefeed on another goroutine. Restart might be a bit
			// expensive, particularly if we have to resolve span.  We do not want
			// to block receiveEventsFromNode for too long.
			m.g.GoCtx(func(ctx context.Context) error {
				return m.restartActiveRangeFeed(ctx, active, t.Error.GoError())
			})
			continue
		}

		active.onRangeEvent(ms.nodeID, event.RangeID, &event.RangeFeedEvent)
		msg := RangeFeedMessage{RangeFeedEvent: &event.RangeFeedEvent, RegisteredSpan: active.Span}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case m.eventCh <- msg:
		}

		// Piggyback on this loop to check if any of the active ranges
		// on this node appear to be stuck.
		// NB: this does not notify the server in any way.  We may have to add
		// a more complex protocol -- or better yet, figure out why ranges
		// get stuck in the first place.
		if timeutil.Now().Before(nextStuckCheck) {
			if threshold := stuckThreshold(); threshold > 0 {
				// Restart rangefeed on another goroutine. Restart might be a bit
				// expensive, particularly if we have to resolve span.  We do not want
				// to block receiveEventsFromNode for too long.
				toRestart := ms.purgeStuckStreams(threshold)
				m.g.GoCtx(func(ctx context.Context) error {
					return m.restartActiveRangeFeeds(ctx, errRestartStuckRange, toRestart)
				})
			}
			nextStuckCheck = timeutil.Now().Add(stuckCheckFreq())
		}
	}
}

// restarActiveRangeFeeds restarts one or more rangefeeds.
func (m *rangefeedMuxer) restartActiveRangeFeeds(
	ctx context.Context, reason error, toRestart []*activeMuxRangeFeed,
) error {
	for _, active := range toRestart {
		if err := m.restartActiveRangeFeed(ctx, active, reason); err != nil {
			return err
		}
	}
	return nil
}

// restartActiveRangeFeed restarts rangefeed after it encountered "reason" error.
func (m *rangefeedMuxer) restartActiveRangeFeed(
	ctx context.Context, active *activeMuxRangeFeed, reason error,
) error {
	m.ds.metrics.RangefeedRestartRanges.Inc(1)

	if log.V(0) {
		log.Infof(ctx, "RangeFeed %s@%s disconnected with last checkpoint %s ago: %v",
			active.Span, active.StartAfter, timeutil.Since(active.Resolved.GoTime()), reason)
	}
	active.setLastError(reason)
	active.release()

	errInfo, err := handleRangefeedError(ctx, reason)
	if err != nil {
		// If this is an error we cannot recover from, terminate the rangefeed.
		return err
	}

	if errInfo.evict && active.token.Valid() {
		active.token.Evict(ctx)
		active.token = rangecache.EvictionToken{}
	}

	rs, err := keys.SpanAddr(active.Span)
	if err != nil {
		return err
	}

	if errInfo.resolveSpan {
		return divideSpanOnRangeBoundaries(ctx, m.ds, rs, active.startAfter, m.startSingleRangeFeed)
	}
	return m.startSingleRangeFeed(ctx, rs, active.startAfter, active.token)
}

// startRangeFeed initiates rangefeed for the specified request running
// on this node connection.  If no error returned, registers stream
// with this connection.  Otherwise, stream is not registered.
func (c *muxStream) startRangeFeed(
	streamID int64, stream *activeMuxRangeFeed, req *kvpb.RangeFeedRequest,
) error {
	// NB: lock must be held for the duration of this method.
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.mu.closed {
		return net.ErrClosed
	}

	// Concurrent Send calls are not thread safe; thus Send calls must be
	// synchronized.
	if err := c.mu.sender.Send(req); err != nil {
		return err
	}

	// As soon as we issue Send above, the stream may return an error that
	// may be seen by the event consumer (receiveEventsFromNode).
	// Therefore, we update streams map under the lock to ensure that the
	// receiver will be able to observe this stream.
	c.mu.streams[streamID] = stream
	return nil
}

func (c *muxStream) lookupStream(streamID int64) (a *activeMuxRangeFeed) {
	c.mu.Lock()
	a = c.mu.streams[streamID]
	c.mu.Unlock()
	return a
}

func (c *muxStream) purgeStuckStreams(threshold time.Duration) (stuck []*activeMuxRangeFeed) {
	c.mu.Lock()
	for streamID, a := range c.mu.streams {
		if !a.startAfter.IsEmpty() && timeutil.Since(a.startAfter.GoTime()) > threshold {
			stuck = append(stuck, a)
			delete(c.mu.streams, streamID)
		}
	}
	c.mu.Unlock()
	return stuck
}

func (c *muxStream) deleteStream(streamID int64) {
	c.mu.Lock()
	delete(c.mu.streams, streamID)
	c.mu.Unlock()
}

// close closes mux stream returning the list of active range feeds.
func (c *muxStream) close() []*activeMuxRangeFeed {
	c.mu.Lock()
	c.mu.closed = true
	toRestart := make([]*activeMuxRangeFeed, 0, len(c.mu.streams))
	for _, a := range c.mu.streams {
		toRestart = append(toRestart, a)
	}
	c.mu.streams = nil
	c.mu.Unlock()

	return toRestart
}
