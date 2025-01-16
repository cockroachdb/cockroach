// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord

import (
	"context"
	"io"
	"net"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangecache"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/future"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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
	metrics    *DistSenderRangeFeedMetrics
	cfg        rangeFeedConfig
	registry   *rangeFeedRegistry
	catchupSem *catchupScanRateLimiter
	eventCh    chan<- RangeFeedMessage

	// Each call to start new range feed gets a unique ID which is echoed back
	// by MuxRangeFeed rpc.  This is done as a safety mechanism to make sure
	// that we always send the event to the correct consumer -- even if the
	// range feed is terminated and re-established rapidly.
	// Accessed atomically.
	seqID int64

	muxClients syncutil.Map[roachpb.NodeID, future.Future[muxStreamOrError]]
}

// muxRangeFeed is an entry point to establish MuxRangeFeed
// RPC for the specified spans. Waits for rangefeed to complete.
func muxRangeFeed(
	ctx context.Context,
	cfg rangeFeedConfig,
	spans []SpanTimePair,
	ds *DistSender,
	rr *rangeFeedRegistry,
	catchupRateLimiter *catchupScanRateLimiter,
	eventCh chan<- RangeFeedMessage,
) (retErr error) {
	if log.V(1) {
		log.Infof(ctx, "Establishing MuxRangeFeed (%s...; %d spans)", spans[0], len(spans))
		start := timeutil.Now()
		defer func() {
			log.Infof(ctx, "MuxRangeFeed terminating after %s with err=%v", timeutil.Since(start), retErr)
		}()
	}

	m := &rangefeedMuxer{
		g:          ctxgroup.WithContext(ctx),
		registry:   rr,
		ds:         ds,
		cfg:        cfg,
		metrics:    &ds.metrics.DistSenderRangeFeedMetrics,
		catchupSem: catchupRateLimiter,
		eventCh:    eventCh,
	}
	if cfg.knobs.metrics != nil {
		m.metrics = cfg.knobs.metrics
	}

	m.g.GoCtx(func(ctx context.Context) error {
		return divideAllSpansOnRangeBoundaries(ctx, spans, m.startSingleRangeFeed, ds)
	})

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

	streams syncutil.Map[int64, activeMuxRangeFeed]

	// mu must be held when starting rangefeed.
	mu struct {
		syncutil.Mutex
		sender rangeFeedRequestSender
		closed bool
	}
}

// muxStreamOrError is a tuple of mux stream connection or an error that
// occurred while connecting to the node.
type muxStreamOrError struct {
	stream *muxStream
	err    error
}

// activeMuxRangeFeed augments activeRangeFeed with additional state.
// This is a long-lived data structure representing the lifetime of a single
// range feed.
// The lifetime of a single range feed is as follows:
//
// ┌─* muxRangeFeed
// ┌───►├─► divideSpanOnRangeBoundaries
// │    │   Divide target span(s) on range boundaries
// │    ├─► startSingleRangeFeed
// │    │   Allocate activeMuxRangeFeed, acquire catchup scan quota
// │┌──►├─► activeMuxRangeFeed.start
// ││   │   Determine the first replica that's usable for running range feed
// ││   │   Establish MuxRangeFeed connection with the node
// ││   │   Start single rangefeed on that node
// ││   │   Assign unique "stream ID" which will be echoed back by range feed server
// ││   │
// ││┌─►├─► receiveEventsFromNode
// │││  │   Lookup activeMuxRangeFeed corresponding to the received event
// │││  ◊─► Handle event
// ││└──│── Maybe release catchup scan quota
// ││   │
// ││   ├─► OR Handle error
// ││   └─► restartActiveRangeFeeds
// ││       Determine if the error is fatal, if so terminate
// ││       Transient error can be retried, using retry/transport state
// │└────── stored in this structure (for example: try next replica)
// │        Some errors (range split) need to perform full lookup
// └─────── activeMuxRangeFeed is released, replaced by one or more new instances
type activeMuxRangeFeed struct {
	*activeRangeFeed
	rSpan roachpb.RSpan
	roachpb.ReplicaDescriptor
	startAfter              hlc.Timestamp
	parentRangeFeedMetadata parentRangeFeedMetadata

	// State pertaining to execution of rangefeed call.
	token     rangecache.EvictionToken
	transport Transport
}

func (s *activeMuxRangeFeed) release() {
	s.activeRangeFeed.release()
	if s.catchupRes != nil {
		s.catchupRes.Release()
	}
	if s.transport != nil {
		s.transport.Release()
		s.transport = nil
	}
}

func (s *activeMuxRangeFeed) resetRouting(ctx context.Context, newToken rangecache.EvictionToken) {
	if s.token.Valid() {
		s.token.Evict(ctx)
	}
	if s.transport != nil {
		s.transport.Release()
		s.transport = nil
	}
	s.token = newToken
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
	ctx context.Context,
	rs roachpb.RSpan,
	startAfter hlc.Timestamp,
	token rangecache.EvictionToken,
	parentRangefeedMetadata parentRangeFeedMetadata,
) error {
	// Bound the partial rangefeed to the partial span.
	span := rs.AsRawSpanWithNoLocals()

	// Register active mux range feed.
	stream := &activeMuxRangeFeed{
		// TODO(msbutler): It's sad that there's a bunch of repeat metadata.
		// Deduplicate once old style rangefeed code is banished from the codebase.
		activeRangeFeed:         newActiveRangeFeed(span, startAfter, m.registry, m.metrics, parentRangefeedMetadata, token.Desc().RangeID),
		rSpan:                   rs,
		startAfter:              startAfter,
		token:                   token,
		parentRangeFeedMetadata: parentRangefeedMetadata,
	}

	if err := stream.start(ctx, m); err != nil {
		stream.release()
		return err
	}

	if m.cfg.withMetadata {
		// Send metadata after the stream successfully registers to avoid sending
		// metadata about a rangefeed that never starts.
		if err := sendMetadata(ctx, m.eventCh, span, parentRangefeedMetadata); err != nil {
			return err
		}
	}

	return nil
}

// start begins execution of activeMuxRangeFeed.
// This method uses the routing and transport information associated with this active stream,
// to find the node that hosts range replica, establish MuxRangeFeed RPC stream
// with the node, and then establish rangefeed for the span with that node.
// If the routing/transport information are not valid, performs lookup to refresh this
// information.
// Transient errors while establishing RPCs are retried with backoff.
// Certain non-recoverable errors (such as grpcutil.IsAuthError) are propagated to the
// caller and will cause the whole rangefeed to terminate.
// Upon successfully establishing RPC stream, the ownership of the activeMuxRangeFeed
// gets transferred to the node event loop goroutine (receiveEventsFromNode).
func (s *activeMuxRangeFeed) start(ctx context.Context, m *rangefeedMuxer) error {
	streamID := atomic.AddInt64(&m.seqID, 1)

	// Before starting single rangefeed, acquire catchup scan quota.
	if err := s.acquireCatchupScanQuota(ctx, m.catchupSem, m.metrics); err != nil {
		return err
	}

	// Start a retry loop for sending the batch to the range.
	for r := retry.StartWithCtx(ctx, m.ds.rpcRetryOptions); r.Next(); {
		// If we've cleared the descriptor on failure, re-lookup.
		if !s.token.Valid() {
			ri, err := m.ds.getRoutingInfo(ctx, s.rSpan.Key, rangecache.EvictionToken{}, false)
			if err != nil {
				log.VErrEventf(ctx, 0, "range descriptor re-lookup failed: %s", err)
				if !rangecache.IsRangeLookupErrorRetryable(err) {
					return err
				}
				continue
			}
			s.resetRouting(ctx, ri)
		}

		// Establish a RangeFeed for a single Range.
		if s.transport == nil {
			transport, err := newTransportForRange(ctx, s.token.Desc(), m.ds)
			if err != nil {
				log.VErrEventf(ctx, 1, "Failed to create transport for %s (err=%s) ", s.token.String(), err)
				continue
			}
			s.transport = transport
		}

		for !s.transport.IsExhausted() {
			args := makeRangeFeedRequest(
				s.Span, s.token.Desc().RangeID, m.cfg.overSystemTable, s.startAfter, m.cfg.withDiff, m.cfg.withFiltering, m.cfg.withMatchingOriginIDs, m.cfg.consumerID)
			args.Replica = s.transport.NextReplica()
			args.StreamID = streamID
			s.ReplicaDescriptor = args.Replica

			s.activeRangeFeed.Lock()
			s.activeRangeFeed.NodeID = args.Replica.NodeID
			s.activeRangeFeed.Unlock()

			rpcClient, err := s.transport.NextInternalClient(ctx)
			if err != nil {
				log.VErrEventf(ctx, 1, "RPC error connecting to replica %s: %s", args.Replica, err)
				continue
			}

			log.VEventf(ctx, 1,
				"MuxRangeFeed starting for span %s@%s (rangeID %d, replica %s, attempt %d)",
				s.Span, s.startAfter, s.token.Desc().RangeID, args.Replica, r.CurrentAttempt())

			conn, err := m.establishMuxConnection(ctx, rpcClient, args.Replica.NodeID)
			s.onConnect(rpcClient, m.metrics)

			if err == nil {
				err = conn.startRangeFeed(streamID, s, &args, m.cfg.knobs.beforeSendRequest)
			}

			if err != nil {
				log.VErrEventf(ctx, 1,
					"RPC error establishing mux rangefeed to r%d, replica %s: %s", args.RangeID, args.Replica, err)
				if grpcutil.IsAuthError(err) {
					// Authentication or authorization error. Propagate.
					return err
				}
				continue
			}

			if m.cfg.knobs.captureMuxRangeFeedRequestSender != nil {
				m.cfg.knobs.captureMuxRangeFeedRequestSender(
					args.Replica.NodeID,
					func(req *kvpb.RangeFeedRequest) error {
						conn.mu.Lock()
						defer conn.mu.Unlock()
						return conn.mu.sender.Send(req)
					})
			}

			return nil
		}

		s.resetRouting(ctx, rangecache.EvictionToken{}) // Transport exhausted; reset and retry.
	}

	return ctx.Err()
}

// establishMuxConnection establishes MuxRangeFeed RPC with the node.
func (m *rangefeedMuxer) establishMuxConnection(
	ctx context.Context, client rpc.RestrictedInternalClient, nodeID roachpb.NodeID,
) (*muxStream, error) {
	muxClient, exists := m.muxClients.LoadOrStore(nodeID, future.Make[muxStreamOrError]())
	if !exists {
		// Start mux rangefeed goroutine responsible for receiving MuxRangeFeedEvents.
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

	tags := &logtags.Buffer{}
	tags = tags.Add("mux_n", nodeID)
	// Add "generation" number to the context so that log messages and stacks can
	// differentiate between multiple instances of mux rangefeed goroutine
	// (this can happen when one was shutdown, then re-established).
	tags = tags.Add("gen", atomic.AddInt64(&m.seqID, 1))

	ctx = logtags.AddTags(ctx, tags)
	ctx, restore := pprofutil.SetProfilerLabelsFromCtxTags(ctx)
	defer restore()

	if log.V(1) {
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
		// Remove the mux client from the cache if it hit an
		// error.
		m.muxClients.Delete(nodeID)
		return future.MustSet(stream, muxStreamOrError{err: err})
	}

	maybeCloseClient := func() {
		if closer, ok := mux.(io.Closer); ok {
			if err := closer.Close(); err != nil {
				log.Warningf(ctx, "error closing mux rangefeed client: %v", err)
			}
		}
	}

	ms := muxStream{nodeID: nodeID}
	ms.mu.sender = mux
	if err := future.MustSet(stream, muxStreamOrError{stream: &ms}); err != nil {
		maybeCloseClient()
		return err
	}

	if recvErr := m.receiveEventsFromNode(ctx, mux, &ms); recvErr != nil {
		// Clear out this client, and restart all streams on this node.
		// Note: there is a race here where we may delete this muxClient, while
		// another goroutine loaded it.  That's fine, since we would not
		// be able to send new request on this stream anymore, and we'll retry
		// against another node.
		maybeCloseClient()
		m.muxClients.Delete(nodeID)

		if recvErr == io.EOF {
			recvErr = nil
		}

		toRestart := ms.close()

		// make sure that the underlying error is not fatal. If it is, there is no
		// reason to restart each rangefeed, so just bail out.
		if _, err := handleRangefeedError(ctx, m.metrics, recvErr, false); err != nil {
			// Regardless of an error, release any resources (i.e. metrics) still
			// being held by active stream.
			for _, s := range toRestart {
				s.release()
			}
			return err
		}

		if log.V(1) {
			log.Infof(ctx, "mux to node %d restarted %d streams", ms.nodeID, len(toRestart))
		}
		return m.restartActiveRangeFeeds(ctx, recvErr, toRestart)
	}

	return nil
}

// receiveEventsFromNode receives mux rangefeed events from a node.
func (m *rangefeedMuxer) receiveEventsFromNode(
	ctx context.Context, receiver muxRangeFeedEventReceiver, ms *muxStream,
) error {
	for {
		event, err := receiver.Recv()
		if err != nil {
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
			if log.V(1) {
				log.Infof(ctx, "received stray event stream %d: %v", event.StreamID, event)
			}
			continue
		}

		if m.cfg.knobs.onRangefeedEvent != nil {
			skip, err := m.cfg.knobs.onRangefeedEvent(ctx, active.Span, event.StreamID, &event.RangeFeedEvent)
			if err != nil {
				return err
			}
			if skip {
				continue
			}
		}

		switch t := event.GetValue().(type) {
		case *kvpb.RangeFeedCheckpoint:
			if t.Span.Contains(active.Span) {
				// If we see the first non-empty checkpoint, we know we're done with the catchup scan.
				if active.catchupRes != nil {
					active.releaseCatchupScan()
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
				m.metrics.Errors.RangefeedErrorCatchup.Inc(1)
			}
			ms.streams.Delete(event.StreamID)
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
	}
}

// restartActiveRangeFeeds restarts one or more rangefeeds.
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
	m.metrics.Errors.RangefeedRestartRanges.Inc(1)
	active.setLastError(reason)

	// Release catchup scan reservation if any -- we will acquire another
	// one when we restart.
	active.releaseCatchupScan()

	doRelease := true
	defer func() {
		if doRelease {
			active.release()
		}
	}()

	errInfo, err := handleRangefeedError(ctx, m.metrics, reason, active.ParentRangefeedMetadata.fromManualSplit)
	if err != nil {
		// If this is an error we cannot recover from, terminate the rangefeed.
		return err
	}

	if log.V(1) {
		log.Infof(ctx, "RangeFeed %s@%s (r%d, replica %s) disconnected with last checkpoint %s ago: %v (errInfo %v)",
			active.Span, active.StartAfter, active.RangeID, active.ReplicaDescriptor,
			timeutil.Since(active.Resolved.GoTime()), reason, errInfo)
	}

	if errInfo.evict {
		active.resetRouting(ctx, rangecache.EvictionToken{})
	}

	if errInfo.resolveSpan {
		parentMetadata := parentRangeFeedMetadata{
			fromManualSplit: errInfo.manualSplit,
			startKey:        active.rSpan.Key.AsRawKey(),
		}
		return divideSpanOnRangeBoundaries(ctx, m.ds, active.rSpan, active.startAfter, m.startSingleRangeFeed, parentMetadata)
	}

	if err := active.start(ctx, m); err != nil {
		return err
	}
	doRelease = false // active stream ownership transferred to start above.
	return nil
}

// startRangeFeed initiates rangefeed for the specified request running
// on this node connection.  If no error returned, registers stream
// with this connection.  Otherwise, stream is not registered.
func (c *muxStream) startRangeFeed(
	streamID int64, stream *activeMuxRangeFeed, req *kvpb.RangeFeedRequest, beforeSend func(),
) (retErr error) {
	// NB: lock must be held for the duration of this method.
	// The reasons for this are twofold:
	//  1. Send calls must be protected against concurrent calls.
	//  2. The muxStream may be in the process of restart -- that is receiveEventsFromNode just
	//     returned an error.  When that happens, muxStream is closed, and all rangefeeds
	//     belonging to this muxStream are restarted.  The lock here synchronizes with the close()
	//     call so that we either observe the fact that muxStream is closed when this method runs,
	//     or that the close waits until this call completes.
	//     Note also, the Send method may block.  That's alright.  If the call is blocked because
	//     the server side just returned an error, then, the send call should abort and cause an
	//     error to be returned, releasing the lock, and letting close proceed.
	c.mu.Lock()
	defer c.mu.Unlock()

	// As soon as we issue Send below, the stream may return an event or an error that
	// may be seen by the event consumer (receiveEventsFromNode).
	// Therefore, we update streams map immediately, but undo this insert in case of an error,
	// which is returned to the caller for retry.
	c.streams.Store(streamID, stream)

	defer func() {
		if retErr != nil {
			// undo stream registration.
			c.streams.Delete(streamID)
		}
	}()

	if c.mu.closed {
		return net.ErrClosed
	}

	if beforeSend != nil {
		beforeSend()
	}

	return c.mu.sender.Send(req)
}

func (c *muxStream) lookupStream(streamID int64) *activeMuxRangeFeed {
	v, _ := c.streams.Load(streamID)
	return v
}

// close closes mux stream returning the list of active range feeds.
func (c *muxStream) close() (toRestart []*activeMuxRangeFeed) {
	// NB: lock must be held for the duration of this method to synchronize with startRangeFeed.
	c.mu.Lock()
	defer c.mu.Unlock()

	c.mu.closed = true

	c.streams.Range(func(_ int64, v *activeMuxRangeFeed) bool {
		toRestart = append(toRestart, v)
		return true
	})

	return toRestart
}

// NewCloseStreamRequest returns a mux rangefeed request to close specified stream.
func NewCloseStreamRequest(
	ctx context.Context, st *cluster.Settings, streamID int64,
) (*kvpb.RangeFeedRequest, error) {
	return &kvpb.RangeFeedRequest{
		StreamID:    streamID,
		CloseStream: true,
	}, nil
}
