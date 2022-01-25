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
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
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
	streams util.FastIntSet
	cancel  context.CancelFunc
}

func newRangefeedMuxer(g ctxgroup.Group) *rangefeedMuxer {
	m := &rangefeedMuxer{
		eventCh: make(chan *roachpb.MuxRangeFeedEvent),
		g:       g,
	}
	m.mu.muxClients = make(map[roachpb.NodeID]*muxClientState)
	m.mu.producers = make(map[int64]*channelRangeFeedEventProducer)

	m.g.GoCtx(m.demux)

	return m
}

type eventOrError struct {
	event *roachpb.RangeFeedEvent
	err   error
}

// channelRangeFeedEventProducer is a rangeFeedEventProducer which receives
// events on input channel, and returns events when Recv is called.
type channelRangeFeedEventProducer struct {
	ctx   context.Context
	errCh chan error // Signalled to propagate terminal error the consumer.
	input chan eventOrError
}

// Recv implements rangeFeedEventProducer interface.
func (c *channelRangeFeedEventProducer) Recv() (*roachpb.RangeFeedEvent, error) {
	select {
	case <-c.ctx.Done():
		return nil, c.ctx.Err()
	case err := <-c.errCh:
		return nil, err
	case e := <-c.input:
		return e.event, e.err
	}
}

var _ roachpb.RangeFeedEventProducer = (*channelRangeFeedEventProducer)(nil)

// startMuxRangeFeed begins the execution of rangefeed for the specified
// RangeFeedRequest.
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

// connect establishes MuxRangeFeed connection for the specified node. returns
// event producer, RPC client to send requests on, the streamID which should be
// used when sending request, and a cleanup function. Cleanup function never
// nil, and must always be invoked, even if error is returned.
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

	// Lookup or establish MuxRangeFeed for this node.
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
		ctx:   ctx,
		errCh: make(chan error, 1),
		input: make(chan eventOrError),
	}
	m.mu.producers[streamID] = producer
	return producer, ms.client, streamID, cleanup, nil
}

// demux de-multiplexes events and sends them to appropriate rangefeed event
// consumer.
func (m *rangefeedMuxer) demux(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case e := <-m.eventCh:
			m.mu.Lock()
			producer, found := m.mu.producers[e.StreamID]
			m.mu.Unlock()

			if !found {
				return m.shutdownWithError(errors.AssertionFailedf(
					"expected to find consumer for streamID=%d", e.StreamID),
				)
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case producer.input <- eventOrError{event: &e.RangeFeedEvent}:
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
			if err := m.propagateStreamTerminationErrorToConsumers(ctx, nodeID, streamErr); err != nil {
				return err
			}
			// Since the stream error is handled above, we return nil to gracefully shut down
			// this go routine.
			return nil //nolint:returnerrcheck
		}

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
	ctx context.Context, nodeID roachpb.NodeID, streamErr error,
) error {
	// Grab muxStream associated with the node, and clear it out.
	// We don't run entirety of this function under lock since
	// as soon as we send an error to a consumer, a re-entrant call
	// to re-establish range feed may be made.
	m.mu.Lock()
	ms, streamFound := m.mu.muxClients[nodeID]
	// Note: it's okay if the stream is not found; this can happen if the
	// nodeID was already removed from muxClients because we're trying to gracefully
	// shutdown receiveEventsFromNode go routine.

	var producers []*channelRangeFeedEventProducer
	if streamFound {
		ms.streams.ForEach(func(streamID int) {
			producers = append(producers, m.mu.producers[int64(streamID)])
			delete(m.mu.producers, int64(streamID))
		})
	}

	delete(m.mu.muxClients, nodeID)
	m.mu.Unlock()

	for _, producer := range producers {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case producer.input <- eventOrError{err: streamErr}:
		}
	}

	return nil
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
	for _, p := range m.mu.producers {
		p.errCh <- terminalErr // buffered channel
	}

	return terminalErr
}
