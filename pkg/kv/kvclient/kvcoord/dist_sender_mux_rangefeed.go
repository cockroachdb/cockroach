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
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// rangefeedMuxer is responsible for coordination and management of mux rangefeeds.
// rangefeedMuxer caches MuxRangeFeed stream per node, and executes each range feed request
// on an appropriate node.
type rangefeedMuxer struct {
	// eventCh receives events from all of the active muxStreams.
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
		muxClients map[roachpb.NodeID]roachpb.Internal_MuxRangeFeedClient
		// activeFeeds maps nodeID to the list of streamIDs currently running on that node.
		activeFeeds map[roachpb.NodeID][]int64
		// producers maps streamID to the event producer -- data sent back to the
		// consumer of range feed events.
		producers map[int64]*channelRangeFeedEventProducer
	}
}

func newRangefeedMuxer(g ctxgroup.Group) *rangefeedMuxer {
	m := &rangefeedMuxer{
		eventCh: make(chan *roachpb.MuxRangeFeedEvent),
		g:       g,
	}

	m.mu.muxClients = make(map[roachpb.NodeID]roachpb.Internal_MuxRangeFeedClient)
	m.mu.activeFeeds = make(map[roachpb.NodeID][]int64)
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

// startMuxRangeFeed begins the execution of rangefeed for the specified RangeFeedRequest.
func (m *rangefeedMuxer) startMuxRangeFeed(
	ctx context.Context, client roachpb.InternalClient, req *roachpb.RangeFeedRequest,
) (roachpb.RangeFeedEventProducer, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.mu.terminalErr != nil {
		return nil, m.mu.terminalErr
	}

	// Lookup or establish MuxRangeFeed for this node.
	nodeID := req.Replica.NodeID
	stream, found := m.mu.muxClients[nodeID]
	if !found {
		var err error
		stream, err = client.MuxRangeFeed(ctx)
		if err != nil {
			return nil, err
		}

		m.mu.muxClients[req.Replica.NodeID] = stream
		m.g.GoCtx(func(ctx context.Context) error {
			return m.receiveEventsFromNode(ctx, nodeID, stream)
		})
	}

	streamID := m.mu.nextStreamID
	m.mu.nextStreamID++

	// Start RangeFeed for this request.
	req.StreamID = streamID
	if err := stream.Send(req); err != nil {
		return nil, err
	}
	m.mu.activeFeeds[nodeID] = append(m.mu.activeFeeds[nodeID], streamID)
	producer := &channelRangeFeedEventProducer{
		ctx:   ctx,
		errCh: make(chan error, 1),
		input: make(chan eventOrError),
	}
	m.mu.producers[streamID] = producer
	return producer, nil
}

// demux de-multiplexes events and sends them to appropriate rangefeed event consumer.
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

// receiveEventsFromNode receives mux rangefeed events, and forwards them to the consumer channel.
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

// propagateStreamTerminationErrorToConsumers called when mux stream running on a node
// encountered an error.  All consumers will receive the stream termination error and
// will handle it appropriately.
func (m *rangefeedMuxer) propagateStreamTerminationErrorToConsumers(
	ctx context.Context, nodeID roachpb.NodeID, streamErr error,
) error {
	// Grab muxStream associated with the node, and clear it out.
	// We don't run entirety of this function under lock since
	// as soon as we send an error to a consumer, a re-entrant call
	// to re-establish range feed may be made.
	m.mu.Lock()
	_, streamFound := m.mu.muxClients[nodeID]
	var producers []*channelRangeFeedEventProducer
	for _, streamID := range m.mu.activeFeeds[nodeID] {
		producers = append(producers, m.mu.producers[streamID])
		delete(m.mu.producers, streamID)
	}
	delete(m.mu.muxClients, nodeID)
	delete(m.mu.activeFeeds, nodeID)
	m.mu.Unlock()

	if !streamFound {
		return errors.Wrapf(streamErr, "expected to find mux stream for node %d", nodeID)
	}

	for _, producer := range producers {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case producer.input <- eventOrError{err: streamErr}:
		}
	}

	return nil
}

// shutdownWithError terminates this rangefeedMuxer with a terminal error (usually an
// assertion failure).  It's a bit unwieldy way to propagate this error to the caller,
// but as soon as  one of consumers notices this terminal error, the context should be cancelled.
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
