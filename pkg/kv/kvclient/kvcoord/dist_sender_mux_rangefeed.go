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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// rangefeedMuxer is responsible for coordination and management of mux
// rangefeeds. rangefeedMuxer caches MuxRangeFeed stream per node, and executes
// each range feed request on an appropriate node.
type rangefeedMuxer struct {
	// eventCh receives events from all active muxStreams.
	eventCh chan *roachpb.MuxRangeFeedEvent

	// Context group controlling execution of MuxRangeFeed calls.
	g ctxgroup.Group

	// State pertaining to actively executing MuxRangeFeeds.
	mu struct {
		syncutil.Mutex

		// terminalErr set when a terminal error occurs.
		// Subsequent calls to startMuxRangeFeed will return this error.
		terminalErr error

		// Each call to start new range feed gets a unique ID which is echoed back
		// by MuxRangeFeed rpc.  This is done as a safety mechanism to make sure
		// that we always send the event to the correct consumer -- even if the
		// range feed is terminated and re-established rapidly.
		nextStreamID int64

		// muxClient contains a nodeID->MuxRangeFeedClient.
		muxClients map[roachpb.NodeID]*muxClientState
		// producers maps streamID to the event producer -- data sent back to the
		// consumer of range feed events.
		producers map[int64]*channelRangeFeedEventProducer
	}
}

type muxClientState struct {
	client  roachpb.Internal_MuxRangeFeedClient
	streams intsets.Fast
	cancel  context.CancelFunc
}

func newRangefeedMuxer(g ctxgroup.Group) *rangefeedMuxer {
	m := &rangefeedMuxer{
		eventCh: make(chan *roachpb.MuxRangeFeedEvent),
		g:       g,
	}
	m.mu.muxClients = make(map[roachpb.NodeID]*muxClientState)
	m.mu.producers = make(map[int64]*channelRangeFeedEventProducer)

	m.g.GoCtx(m.demuxLoop)

	return m
}

// channelRangeFeedEventProducer is a rangeFeedEventProducer which receives
// events on input channel, and returns events when Recv is called.
type channelRangeFeedEventProducer struct {
	ctx       context.Context
	termErrCh chan struct{} // Signalled to propagate terminal error the consumer.
	termErr   error         // Set when terminal error occurs.
	eventCh   chan *roachpb.RangeFeedEvent
}

// Recv implements rangeFeedEventProducer interface.
func (c *channelRangeFeedEventProducer) Recv() (*roachpb.RangeFeedEvent, error) {
	select {
	case <-c.ctx.Done():
		return nil, c.ctx.Err()
	case <-c.termErrCh:
		return nil, c.termErr
	case e := <-c.eventCh:
		log.Infof(c.ctx, "ZXCV %s", e.GetValue())
		return e, nil
	}
}

var _ roachpb.RangeFeedEventProducer = (*channelRangeFeedEventProducer)(nil)

// startMuxRangeFeed begins the execution of rangefeed for the specified
// RangeFeedRequest.
// The passed in client is only needed to establish MuxRangeFeed RPC.
func (m *rangefeedMuxer) startMuxRangeFeed(
	ctx context.Context, client rpc.RestrictedInternalClient, req *roachpb.RangeFeedRequest,
) (_ roachpb.RangeFeedEventProducer, cleanup func(), _ error) {
	producer, rpcClient, streamID, cleanup, err := m.connect(ctx, client, req.Replica.NodeID)
	if err != nil {
		return nil, cleanup, err
	}
	req.StreamID = streamID
	if err := rpcClient.Send(req); err != nil {
		return nil, cleanup, err
	}
	return producer, cleanup, nil
}

// connect establishes MuxRangeFeed connection for the specified node, re-using
// the existing one if one exists. Returns event producer, RPC client to send
// requests on, the streamID which should be used when sending request, and a
// cleanup function. Cleanup function never nil, and must always be invoked,
// even if error is returned.
func (m *rangefeedMuxer) connect(
	ctx context.Context, client rpc.RestrictedInternalClient, nodeID roachpb.NodeID,
) (
	_ roachpb.RangeFeedEventProducer,
	_ roachpb.Internal_MuxRangeFeedClient,
	streamID int64,
	cleanup func(),
	_ error,
) {
	m.mu.Lock()
	defer m.mu.Unlock()

	streamID = m.mu.nextStreamID
	m.mu.nextStreamID++

	cleanup = func() {
		m.mu.Lock()
		defer m.mu.Unlock()

		delete(m.mu.producers, streamID)
		// Cleanup mux state if it exists; it may be nil if this function exits
		// early (for example if MuxRangeFeed call fails, before muxState
		// initialized).
		muxState := m.mu.muxClients[nodeID]
		if muxState != nil {
			muxState.streams.Remove(int(streamID))
			if muxState.streams.Len() == 0 {
				// This node no longer has any active streams.
				// Delete node from the muxClient list, and gracefully
				// shutdown consumer go routine.
				delete(m.mu.muxClients, nodeID)
				muxState.cancel()
			}
		}
	}

	if m.mu.terminalErr != nil {
		return nil, nil, streamID, cleanup, m.mu.terminalErr
	}

	var found bool
	ms, found := m.mu.muxClients[nodeID]

	if !found {
		ctx, cancel := context.WithCancel(ctx)
		stream, err := client.MuxRangeFeed(ctx)
		if err != nil {
			cancel()
			return nil, nil, streamID, cleanup, err
		}

		ms = &muxClientState{client: stream, cancel: cancel}
		m.mu.muxClients[nodeID] = ms
		m.g.GoCtx(func(ctx context.Context) error {
			defer cancel()
			return m.receiveEventsFromNode(ctx, nodeID, stream)
		})
	}

	// Start RangeFeed for this request.
	ms.streams.Add(int(streamID))

	producer := &channelRangeFeedEventProducer{
		ctx:       ctx,
		termErrCh: make(chan struct{}),
		eventCh:   make(chan *roachpb.RangeFeedEvent),
	}
	m.mu.producers[streamID] = producer
	return producer, ms.client, streamID, cleanup, nil
}

// demuxLoop de-multiplexes events and sends them to appropriate rangefeed event
// consumer.
func (m *rangefeedMuxer) demuxLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case e := <-m.eventCh:
			m.mu.Lock()
			producer, found := m.mu.producers[e.StreamID]
			m.mu.Unlock()

			log.Infof(ctx, "QWER %d %s ", e.StreamID, e.GetValue())

			if !found {
				return m.shutdownWithError(errors.AssertionFailedf(
					"expected to find consumer for streamID=%d", e.StreamID),
				)
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case producer.eventCh <- &e.RangeFeedEvent:
			}
		}
	}
}

// receiveEventsFromNode receives mux rangefeed events, and forwards them to the
// consumer channel.
func (m *rangefeedMuxer) receiveEventsFromNode(
	ctx context.Context, nodeID roachpb.NodeID, stream roachpb.Internal_MuxRangeFeedClient,
) error {
	for {
		event, streamErr := stream.Recv()
		if streamErr != nil {
			m.propagateStreamTerminationErrorToConsumers(nodeID, streamErr)
			log.Infof(ctx, "PROPAGATING err %s", streamErr)
			// Since the stream error is handled above, we return nil to gracefully shut down
			// this go routine.
			return nil //nolint:returnerrcheck
		}
		log.Infof(ctx, "ASDF %d %s", event.StreamID, event.GetValue())

		select {
		case <-ctx.Done():
			return ctx.Err()
		case m.eventCh <- event:
		}
	}
}

// propagateStreamTerminationErrorToConsumers called when mux stream running on
// a node encountered an error.  All consumers will receive the stream
// termination error and will handle it appropriately.
func (m *rangefeedMuxer) propagateStreamTerminationErrorToConsumers(
	nodeID roachpb.NodeID, streamErr error,
) {
	// Grab muxStream associated with the node, and clear it out.
	m.mu.Lock()
	defer m.mu.Unlock()

	ms, streamFound := m.mu.muxClients[nodeID]
	delete(m.mu.muxClients, nodeID)
	// Note: it's okay if the stream is not found; this can happen if the
	// nodeID was already removed from muxClients because we're trying to gracefully
	// shutdown receiveEventsFromNode go routine.
	if streamFound {
		ms.streams.ForEach(func(streamID int) {
			p := m.mu.producers[int64(streamID)]
			delete(m.mu.producers, int64(streamID))
			if p != nil {
				p.termErr = streamErr
				close(p.termErrCh)
			}
		})
	}

}

// shutdownWithError terminates this rangefeedMuxer with a terminal error
// (usually an assertion failure).  It's a bit unwieldy way to propagate this
// error to the caller, but as soon as  one of consumers notices this terminal
// error, the context should be cancelled.
// Returns the terminal error passed in.
func (m *rangefeedMuxer) shutdownWithError(terminalErr error) error {
	// Okay to run this under the lock since as soon as a consumer sees this terminal
	// error, the whole rangefeed should terminate.
	m.mu.Lock()
	defer m.mu.Unlock()

	m.mu.terminalErr = terminalErr
	for id, p := range m.mu.producers {
		p.termErr = terminalErr
		close(p.termErrCh)
		delete(m.mu.producers, id)
	}

	return terminalErr
}
