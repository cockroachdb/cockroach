// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord

import (
	"context"
	"io"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangecache"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/future"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// txnfeedMuxer coordinates multiplexed TxnFeed streams across
// multiple nodes.
type txnfeedMuxer struct {
	g       ctxgroup.Group
	ds      *DistSender
	eventCh chan<- TxnFeedMessage
	seqID   int64 // atomically incremented stream ID

	muxClients syncutil.Map[roachpb.NodeID, future.Future[muxTxnFeedStreamOrError]]
}

type muxTxnFeedStreamOrError struct {
	stream *muxTxnFeedStream
	err    error
}

// muxTxnFeedStream represents a MuxTxnFeed RPC to a single node.
type muxTxnFeedStream struct {
	nodeID  roachpb.NodeID
	streams syncutil.Map[int64, activeMuxTxnFeed]
	mu      struct {
		syncutil.Mutex
		sender txnFeedRequestSender
		closed bool
	}
}

type txnFeedRequestSender interface {
	Send(req *kvpb.TxnFeedRequest) error
}

type muxTxnFeedEventReceiver interface {
	Recv() (*kvpb.MuxTxnFeedEvent, error)
}

// activeMuxTxnFeed tracks a single per-range TxnFeed stream.
type activeMuxTxnFeed struct {
	span       roachpb.Span
	rSpan      roachpb.RSpan
	startAfter hlc.Timestamp
	token      rangecache.EvictionToken
	transport  Transport
}

func (s *activeMuxTxnFeed) release() {
	if s.transport != nil {
		s.transport.Release()
		s.transport = nil
	}
}

func (s *activeMuxTxnFeed) resetRouting(ctx context.Context, newToken rangecache.EvictionToken) {
	if s.token.Valid() {
		s.token.Evict(ctx)
	}
	s.token = newToken
	if s.transport != nil {
		s.transport.Release()
		s.transport = nil
	}
}

// muxTxnFeed is the entry point. Creates the muxer, divides spans,
// and waits for completion.
func muxTxnFeed(
	ctx context.Context, spans []SpanTimePair, ds *DistSender, eventCh chan<- TxnFeedMessage,
) error {
	m := &txnfeedMuxer{
		g:       ctxgroup.WithContext(ctx),
		ds:      ds,
		eventCh: eventCh,
	}

	m.g.GoCtx(func(ctx context.Context) error {
		return divideAllSpansOnRangeBoundaries(
			ctx, spans, m.startSingleRangeTxnFeed, ds,
		)
	})

	return errors.CombineErrors(m.g.Wait(), ctx.Err())
}

// startSingleRangeTxnFeed implements onRangeFn.
func (m *txnfeedMuxer) startSingleRangeTxnFeed(
	ctx context.Context,
	rs roachpb.RSpan,
	startAfter hlc.Timestamp,
	token rangecache.EvictionToken,
	_ parentRangeFeedMetadata,
) error {
	s := &activeMuxTxnFeed{
		span:       rs.AsRawSpanWithNoLocals(),
		rSpan:      rs,
		startAfter: startAfter,
		token:      token,
	}
	return s.start(ctx, m)
}

// start establishes the MuxTxnFeed connection for this stream.
// Retries with backoff when transport is exhausted.
func (s *activeMuxTxnFeed) start(ctx context.Context, m *txnfeedMuxer) error {
	for r := retry.StartWithCtx(ctx, m.ds.rpcRetryOptions); r.Next(); {
		// Look up routing info if needed.
		if !s.token.Valid() {
			ri, err := m.ds.getRoutingInfo(
				ctx, s.rSpan.Key, rangecache.EvictionToken{}, false,
			)
			if err != nil {
				if rangecache.IsRangeLookupErrorRetryable(err) {
					continue
				}
				return err
			}
			s.token = ri
		}

		// Create transport if needed.
		if s.transport == nil {
			var err error
			s.transport, err = newTransportForRange(ctx, s.token.Desc(), m.ds)
			if err != nil {
				return err
			}
		}

		// Try replicas until we connect.
		for !s.transport.IsExhausted() {
			streamID := atomic.AddInt64(&m.seqID, 1)

			args := &kvpb.TxnFeedRequest{
				AnchorSpan: s.span,
				StreamID:   streamID,
			}
			args.Header.Timestamp = s.startAfter
			args.Header.RangeID = s.token.Desc().RangeID

			replica := s.transport.NextReplica()
			args.Header.Replica = replica
			client, err := s.transport.NextInternalClient(ctx)
			if err != nil {
				log.VErrEventf(ctx, 1, "TxnFeed transport error: %s", err)
				continue
			}

			conn, err := m.establishMuxConnection(
				ctx, client, replica.NodeID,
			)
			if err != nil {
				log.VErrEventf(ctx, 1, "TxnFeed mux connection error: %s", err)
				continue
			}

			retErr, takenOver := conn.startTxnFeed(streamID, s, args)
			if retErr != nil {
				if takenOver {
					// Another goroutine took over via restartActiveTxnFeed.
					return nil
				}
				log.VErrEventf(ctx, 1, "TxnFeed start error: %s", retErr)
				continue
			}

			// Successfully started — event receiver now owns this feed.
			return nil
		}

		// Transport exhausted; reset and retry with fresh descriptor.
		s.resetRouting(ctx, rangecache.EvictionToken{})
	}

	return ctx.Err()
}

// establishMuxConnection lazily creates one MuxTxnFeed stream per
// node using future.Future for coordination.
func (m *txnfeedMuxer) establishMuxConnection(
	ctx context.Context, client rpc.RestrictedInternalClient, nodeID roachpb.NodeID,
) (*muxTxnFeedStream, error) {
	f, exists := m.muxClients.LoadOrStore(
		nodeID, future.Make[muxTxnFeedStreamOrError](),
	)
	if !exists {
		m.g.GoCtx(func(ctx context.Context) error {
			return m.startNodeMuxTxnFeed(ctx, client, nodeID, f)
		})
	}

	result, err := future.Wait(ctx, f)
	if err != nil {
		return nil, err
	}
	if result.err != nil {
		return nil, result.err
	}
	return result.stream, nil
}

// startNodeMuxTxnFeed opens the MuxTxnFeed RPC and enters the
// receive loop. When the receive loop exits, collects all active
// streams and restarts them.
func (m *txnfeedMuxer) startNodeMuxTxnFeed(
	ctx context.Context,
	client rpc.RestrictedInternalClient,
	nodeID roachpb.NodeID,
	stream *future.Future[muxTxnFeedStreamOrError],
) error {
	mux, err := client.MuxTxnFeed(ctx)
	if err != nil {
		m.muxClients.Delete(nodeID)
		return future.MustSet(stream, muxTxnFeedStreamOrError{err: err})
	}

	ms := &muxTxnFeedStream{nodeID: nodeID}
	ms.mu.sender = mux
	if err := future.MustSet(stream, muxTxnFeedStreamOrError{stream: ms}); err != nil {
		return err
	}

	recvErr := m.receiveEventsFromNode(ctx, mux, ms)

	// Connection died. Clean up and restart affected streams.
	m.muxClients.Delete(nodeID)

	if recvErr == io.EOF {
		recvErr = nil
	}

	toRestart := ms.close()

	// Check if the error is fatal before restarting.
	if _, err := handleTxnFeedError(ctx, recvErr); err != nil {
		for _, s := range toRestart {
			s.release()
		}
		return err
	}

	return m.restartActiveTxnFeeds(ctx, recvErr, toRestart)
}

// receiveEventsFromNode loops on Recv(), routing events by StreamID.
func (m *txnfeedMuxer) receiveEventsFromNode(
	ctx context.Context, receiver muxTxnFeedEventReceiver, ms *muxTxnFeedStream,
) error {
	for {
		event, err := receiver.Recv()
		if err != nil {
			return errors.Wrapf(err, "receiving from node %d", ms.nodeID)
		}

		active, ok := ms.streams.Load(event.StreamID)
		if !ok {
			continue
		}

		switch {
		case event.Checkpoint != nil:
			active.startAfter.Forward(event.Checkpoint.ResolvedTS)
		case event.Error != nil:
			log.VErrEventf(ctx, 2, "TxnFeedError on stream %d: %s",
				event.StreamID, event.Error.Error.GoError())
			// Use LoadAndDelete to coordinate with startTxnFeed.
			// Only spawn restart if we successfully deleted.
			if _, deleted := ms.streams.LoadAndDelete(event.StreamID); deleted {
				m.g.GoCtx(func(ctx context.Context) error {
					return m.restartActiveTxnFeed(ctx, active, event.Error.Error.GoError())
				})
			}
			continue
		}

		msg := TxnFeedMessage{
			TxnFeedEvent:   &event.TxnFeedEvent,
			RegisteredSpan: active.span,
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case m.eventCh <- msg:
		}
	}
}

// restartActiveTxnFeeds restarts multiple active feeds (e.g. after
// node connection death).
func (m *txnfeedMuxer) restartActiveTxnFeeds(
	ctx context.Context, reason error, toRestart []*activeMuxTxnFeed,
) error {
	for i, active := range toRestart {
		if err := m.restartActiveTxnFeed(ctx, active, reason); err != nil {
			for _, remaining := range toRestart[i+1:] {
				remaining.release()
			}
			return err
		}
	}
	return nil
}

// restartActiveTxnFeed classifies the error and either re-divides
// the span or retries start().
func (m *txnfeedMuxer) restartActiveTxnFeed(
	ctx context.Context, active *activeMuxTxnFeed, reason error,
) error {
	doRelease := true
	defer func() {
		if doRelease {
			active.release()
		}
	}()

	errInfo, err := handleTxnFeedError(ctx, reason)
	if err != nil {
		return err
	}

	if errInfo.evict {
		active.resetRouting(ctx, rangecache.EvictionToken{})
	}

	if errInfo.resolveSpan {
		return divideSpanOnRangeBoundaries(
			ctx, m.ds, active.rSpan, active.startAfter,
			m.startSingleRangeTxnFeed, parentRangeFeedMetadata{},
		)
	}

	if err := active.start(ctx, m); err != nil {
		return err
	}
	doRelease = false
	return nil
}

// startTxnFeed registers the active feed and sends the request.
// Returns (error, takenOver) where takenOver=true means another
// goroutine (via receiveEventsFromNode) has taken responsibility
// for restarting this feed.
func (c *muxTxnFeedStream) startTxnFeed(
	streamID int64, s *activeMuxTxnFeed, req *kvpb.TxnFeedRequest,
) (retErr error, takenOver bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.streams.Store(streamID, s)

	defer func() {
		if retErr != nil {
			if _, found := c.streams.LoadAndDelete(streamID); !found {
				takenOver = true
			}
		}
	}()

	if c.mu.closed {
		return errors.New("txnfeed: mux stream closed"), false
	}

	return c.mu.sender.Send(req), false
}

// close closes the mux stream and returns the active feeds that
// need to be restarted.
func (c *muxTxnFeedStream) close() (toRestart []*activeMuxTxnFeed) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.mu.closed = true

	c.streams.Range(func(streamID int64, v *activeMuxTxnFeed) bool {
		toRestart = append(toRestart, v)
		c.streams.Delete(streamID)
		return true
	})

	return toRestart
}
