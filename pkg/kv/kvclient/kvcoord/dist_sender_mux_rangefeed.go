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
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/pprofutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/logtags"
)

// rangefeedMuxer is responsible for coordination and management of mux
// rangefeeds. rangefeedMuxer caches MuxRangeFeed stream per node, and executes
// each range feed request on an appropriate node.
type rangefeedMuxer struct {
	// eventCh receives events from all active muxStreams.
	eventCh chan *roachpb.MuxRangeFeedEvent

	// Context group controlling execution of MuxRangeFeed calls. When this group
	// cancels, the entire muxer shuts down. The goroutines started in `g` will
	// always return `nil` errors except when they detect that the mux is shutting
	// down.
	g ctxgroup.Group

	// When g cancels, muxDone gets closed.
	muxDone chan struct{}

	mu struct {
		// lock used to coordinate establishment and tear down of client connections.
		syncutil.Mutex

		// Each call to start new range feed gets a unique ID which is echoed back
		// by MuxRangeFeed rpc.  This is done as a safety mechanism to make sure
		// that we always send the event to the correct consumer -- even if the
		// range feed is terminated and re-established rapidly.
		nextStreamID int64
		connectDone  chan error // Channel to communicate connect attempt completion.

		// map of active MuxRangeFeed clients.
		clients map[roachpb.NodeID]*muxClientState
	}

	// producers is a map of all rangefeeds running across all nodes.
	// streamID -> *channelRangeFeedEventProducer.
	producers syncutil.IntMap
}

// muxClientState is the state maintained for each MuxRangeFeed rpc.
type muxClientState struct {
	// RPC state.
	client roachpb.Internal_MuxRangeFeedClient
	cancel context.CancelFunc

	done    chan struct{} // Signaled to indicate MuxRangeFeed shutdown.
	recvErr error         // Set when shutdown.

	// Number of consumers (ranges) running on this node; accessed under rangefeedMuxer lock.
	numStreams int
}

func newRangefeedMuxer(g ctxgroup.Group) *rangefeedMuxer {
	m := &rangefeedMuxer{
		eventCh: make(chan *roachpb.MuxRangeFeedEvent),
		muxDone: make(chan struct{}),
		g:       g,
	}

	m.mu.clients = make(map[roachpb.NodeID]*muxClientState)
	m.g.GoCtx(m.demuxLoop)

	return m
}

// channelRangeFeedEventProducer is a rangeFeedEventProducer which receives
// events on input channel, and returns events when Recv is called.
type channelRangeFeedEventProducer struct {
	// Event producer utilizes two contexts: streamCtx connected to the caller of
	// "Recv" method (i.e. singleRangeFeed method), and muxCtx connected to the
	// node level MuxRangeFeed RPC (receiveEventsFromNode). When the
	// caller blocks in Recv(), we have to respect cancellations in both contexts.
	// streamCtx may be canceled by the caller if, for example, range becomes
	// stuck (or parent context cancels). The muxCtx gets notified when
	// receveEvents exits -- this may happen if connection was broken, or perhaps
	// node is being shut down, etc.
	streamCtx context.Context
	muxCtx    terminationContext

	streamID int64                        // stream ID for this producer.
	eventCh  chan *roachpb.RangeFeedEvent // consumer event channel.
}

var _ roachpb.RangeFeedEventProducer = (*channelRangeFeedEventProducer)(nil)

// Recv implements rangeFeedEventProducer interface.
func (c *channelRangeFeedEventProducer) Recv() (*roachpb.RangeFeedEvent, error) {
	select {
	case <-c.streamCtx.Done():
		return nil, c.streamCtx.Err()
	case <-c.muxCtx.Done():
		return nil, c.muxCtx.Err()
	case e := <-c.eventCh:
		return e, nil
	}
}

// startMuxRangeFeed begins the execution of rangefeed for the specified
// RangeFeedRequest.
// The passed in client is only needed to establish MuxRangeFeed RPC.
func (m *rangefeedMuxer) startMuxRangeFeed(
	ctx context.Context, client rpc.RestrictedInternalClient, req *roachpb.RangeFeedRequest,
) (roachpb.RangeFeedEventProducer, func(), error) {
	streamID, ms, err := m.establishMuxConnection(client, req.Replica.NodeID)
	if err != nil {
		return nil, nil, err
	}

	req.StreamID = streamID
	streamCtx := logtags.AddTag(ctx, "stream", req.StreamID)
	producer := &channelRangeFeedEventProducer{
		streamCtx: streamCtx,
		muxCtx:    ms,
		streamID:  req.StreamID,
		eventCh:   make(chan *roachpb.RangeFeedEvent),
	}
	m.producers.Store(req.StreamID, unsafe.Pointer(producer))

	if log.V(1) {
		log.Info(streamCtx, "starting rangefeed")
	}

	cleanup := func() {
		m.producers.Delete(req.StreamID)

		m.mu.Lock()
		defer m.mu.Unlock()

		ms.numStreams--
		if ms.numStreams == 0 {
			delete(m.mu.clients, req.Replica.NodeID)
			if log.V(1) {
				log.InfofDepth(streamCtx, 1, "shut down inactive mux for node %d", req.Replica.NodeID)
			}
			ms.cancel()
		}
	}

	if err := ms.client.Send(req); err != nil {
		cleanup()
		return nil, nil, err
	}
	return producer, cleanup, nil
}

// establishMuxConnection establishes MuxRangeFeed RPC with the node, and
// returns muxClientState, along with a streamID that can be used to send
// range feed request on the RPC.
func (m *rangefeedMuxer) establishMuxConnection(
	client rpc.RestrictedInternalClient, nodeID roachpb.NodeID,
) (streamID int64, _ *muxClientState, err error) {
	// Grab a lock for the duration of connection setup.
	m.mu.Lock()
	defer m.mu.Unlock()

	ms, found := m.mu.clients[nodeID]
	if !found {
		// Establish new MuxRangefeed for this node.
		ms, err = m.startNodeMuxRangeFeed(client, nodeID)
		if err != nil {
			return 0, nil, err
		}
		m.mu.clients[nodeID] = ms
	}

	ms.numStreams++
	m.mu.nextStreamID++
	return m.mu.nextStreamID, ms, nil
}

// startNodeMuxRangeFeed establishes MuxRangeFeed RPC with the node.
// Runs under mux lock.
func (m *rangefeedMuxer) startNodeMuxRangeFeed(
	client rpc.RestrictedInternalClient, nodeID roachpb.NodeID,
) (*muxClientState, error) {
	if m.mu.connectDone == nil {
		m.mu.connectDone = make(chan error, 1)
	}

	ms := &muxClientState{done: make(chan struct{})}

	// It is important that we start MuxRangeFeed RPC using long-lived
	// context available in the main context group used for this muxer.
	m.g.GoCtx(func(ctx context.Context) (err error) {
		ctx = logtags.AddTag(ctx, "mux_n", nodeID)
		// Add "generation" number to the context so that log messages and stacks can
		// differentiate between multiple instances of mux rangefeed Go routine
		// (this can happen when one was shutdown, then re-established).
		ctx = logtags.AddTag(ctx, "gen", uintptr(unsafe.Pointer(ms)))
		ctx, restore := pprofutil.SetProfilerLabelsFromCtxTags(ctx)
		defer restore()

		if log.V(1) {
			log.Info(ctx, "Establishing MuxRangeFeed")
			start := timeutil.Now()
			defer func() {
				log.Infof(ctx, "MuxRangeFeed terminating with recvErr=%v after %s", err, timeutil.Since(start))
			}()
		}

		ctx, ms.cancel = context.WithCancel(ctx)
		defer ms.cancel()

		ms.client, err = client.MuxRangeFeed(ctx)
		m.mu.connectDone <- err
		if err != nil {
			return err
		}
		return m.receiveEventsFromNode(ms)
	})

	return ms, <-m.mu.connectDone
}

// demuxLoop de-multiplexes events and sends them to appropriate rangefeed event
// consumer.
func (m *rangefeedMuxer) demuxLoop(ctx context.Context) (retErr error) {
	defer close(m.muxDone)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case e := <-m.eventCh:
			var producer *channelRangeFeedEventProducer
			if v, found := m.producers.Load(e.StreamID); found {
				producer = (*channelRangeFeedEventProducer)(v)
			}

			// The stream may already have terminated (either producer is nil, or
			// producer.muxCtx.Done()). That's fine -- we may have encountered range
			// split or similar rangefeed error, causing the caller to exit (and
			// terminate this stream), but the server side stream termination is async
			// and probabilistic (rangefeed registration output loop may have a
			// checkpoint event available, *and* it may have context cancellation, but
			// which one executes is a coin flip) and so it is possible that we may
			// see additional event(s) arriving for a stream that is no longer active.
			if producer == nil {
				if log.V(1) {
					log.Infof(ctx, "received stray event stream %d: %v", e.StreamID, e)
				}
				continue
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case producer.eventCh <- &e.RangeFeedEvent:
			case <-producer.muxCtx.Done():
				if log.V(1) {
					log.Infof(ctx, "received stray event stream %d: %v", e.StreamID, e)
				}
			}
		}
	}
}

// terminationContext (inspired by context.Context) describes
// termination information.
type terminationContext interface {
	Done() <-chan struct{}
	Err() error
}

// receiveEventsFromNode receives mux rangefeed events, and forwards them to the
// demuxLoop.
func (m *rangefeedMuxer) receiveEventsFromNode(ms *muxClientState) error {
	for {
		event, streamErr := ms.client.Recv()

		if streamErr != nil {
			ms.recvErr = streamErr
			close(ms.done)
			// Since the stream error is handled above, we return nil to gracefully
			// shut down this go routine.
			return nil //nolint:returnerrcheck
		}

		select {
		case <-m.muxDone:
			// demuxLoop exited, and so should we (happens when main context group completes)
			return nil
		case m.eventCh <- event:
		}
	}
}

func (s *muxClientState) Done() <-chan struct{} {
	return s.done
}

func (s *muxClientState) Err() error {
	return s.recvErr
}
