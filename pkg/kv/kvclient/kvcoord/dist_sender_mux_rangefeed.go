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
	"sync"
	"sync/atomic"
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

	// When g cancels, demuxLoopDone gets closed.
	demuxLoopDone chan struct{}

	mu struct {
		syncutil.Mutex

		// map of active MuxRangeFeed clients.
		clients map[roachpb.NodeID]*muxClientState
	}

	// Each call to start new range feed gets a unique ID which is echoed back
	// by MuxRangeFeed rpc.  This is done as a safety mechanism to make sure
	// that we always send the event to the correct consumer -- even if the
	// range feed is terminated and re-established rapidly.
	// Accessed atomically.
	seqID int64

	// producers is a map of all rangefeeds running across all nodes.
	// streamID -> *channelRangeFeedEventProducer.
	producers syncutil.IntMap
}

// muxClientState is the state maintained for each MuxRangeFeed rpc.
type muxClientState struct {
	initCtx termCtx            // signaled when client ready to be used.
	doneCtx terminationContext // signaled when client shuts down.

	// RPC state. Valid only after initCtx.Done().
	client roachpb.Internal_MuxRangeFeedClient
	cancel context.CancelFunc

	// Number of consumers (ranges) running on this node; accessed under rangefeedMuxer lock.
	numStreams int
}

func newRangefeedMuxer(g ctxgroup.Group) *rangefeedMuxer {
	m := &rangefeedMuxer{
		eventCh:       make(chan *roachpb.MuxRangeFeedEvent),
		demuxLoopDone: make(chan struct{}),
		g:             g,
	}

	m.mu.clients = make(map[roachpb.NodeID]*muxClientState)
	m.g.GoCtx(m.demuxLoop)

	return m
}

// channelRangeFeedEventProducer is a rangeFeedEventProducer which receives
// events on input channel, and returns events when Recv is called.
type channelRangeFeedEventProducer struct {
	// Event producer utilizes two contexts:
	//
	// - callerCtx connected to singleRangeFeed, i.e. a context that will cancel
	//   if a single-range rangefeed fails (range stuck, parent ctx cancels).
	// - muxClientCtx connected to receiveEventsFromNode, i.e. a streaming RPC to
	//   a node serving multiple rangefeeds. This cancels if, for example, the
	//   remote node goes down or there are networking issues.
	//
	// When singleRangeFeed blocks in Recv(), we have to respect cancellations in
	// both contexts. The implementation of Recv() on this type does this.
	callerCtx    context.Context
	muxClientCtx terminationContext

	streamID int64                        // stream ID for this producer.
	eventCh  chan *roachpb.RangeFeedEvent // consumer event channel.
}

var _ roachpb.RangeFeedEventProducer = (*channelRangeFeedEventProducer)(nil)

// Recv implements rangeFeedEventProducer interface.
func (c *channelRangeFeedEventProducer) Recv() (*roachpb.RangeFeedEvent, error) {
	select {
	case <-c.callerCtx.Done():
		return nil, c.callerCtx.Err()
	case <-c.muxClientCtx.Done():
		return nil, c.muxClientCtx.Err()
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
	ms, err := m.establishMuxConnection(ctx, client, req.Replica.NodeID)
	if err != nil {
		return nil, nil, err
	}

	req.StreamID = atomic.AddInt64(&m.seqID, 1)
	streamCtx := logtags.AddTag(ctx, "stream", req.StreamID)
	producer := &channelRangeFeedEventProducer{
		callerCtx:    streamCtx,
		muxClientCtx: ms.doneCtx,
		streamID:     req.StreamID,
		eventCh:      make(chan *roachpb.RangeFeedEvent),
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

// establishMuxConnection establishes MuxRangeFeed RPC with the node.
func (m *rangefeedMuxer) establishMuxConnection(
	ctx context.Context, client rpc.RestrictedInternalClient, nodeID roachpb.NodeID,
) (*muxClientState, error) {
	// NB: the `ctx` in scope here belongs to a client for a single range feed, and must
	// not influence the lifetime of the mux connection. At the time of writing, the caller
	// is `singleRangeFeed` which calls into this method through its streamProducerFactory
	// argument.
	m.mu.Lock()
	ms, found := m.mu.clients[nodeID]
	if !found {
		// Initialize muxClientState.
		// Only initCtx is initialized here since we need to block on it.
		// The rest of the initialization happens in startNodeMuxRangeFeed.
		ms = &muxClientState{initCtx: makeTerminationContext()}
		// Kick off client initialization on another Go routine.
		// It is important that we start MuxRangeFeed RPC using long-lived
		// context available in the main context group used for this muxer.
		m.g.GoCtx(func(ctx context.Context) error {
			return m.startNodeMuxRangeFeed(ctx, ms, client, nodeID)
		})
		m.mu.clients[nodeID] = ms
	}
	ms.numStreams++
	m.mu.Unlock()

	// Ensure mux client is ready.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-ms.initCtx.Done():
		return ms, ms.initCtx.Err()
	}
}

// startNodeMuxRangeFeedLocked establishes MuxRangeFeed RPC with the node.
func (m *rangefeedMuxer) startNodeMuxRangeFeed(
	ctx context.Context,
	ms *muxClientState,
	client rpc.RestrictedInternalClient,
	nodeID roachpb.NodeID,
) error {
	ctx = logtags.AddTag(ctx, "mux_n", nodeID)
	// Add "generation" number to the context so that log messages and stacks can
	// differentiate between multiple instances of mux rangefeed Go routine
	// (this can happen when one was shutdown, then re-established).
	ctx = logtags.AddTag(ctx, "gen", atomic.AddInt64(&m.seqID, 1))
	ctx, restore := pprofutil.SetProfilerLabelsFromCtxTags(ctx)
	defer restore()

	if log.V(1) {
		log.Info(ctx, "Establishing MuxRangeFeed")
		start := timeutil.Now()
		defer func() {
			log.Infof(ctx, "MuxRangeFeed terminating after %s", timeutil.Since(start))
		}()
	}

	doneCtx := makeTerminationContext()
	ms.doneCtx = &doneCtx
	ctx, cancel := context.WithCancel(ctx)

	ms.cancel = func() {
		cancel()
		doneCtx.close(context.Canceled)
	}
	defer ms.cancel()

	// NB: it is important that this Go routine never returns an error. Errors
	// should be propagated to the caller either via initCtx.err, or doneCtx.err.
	// We do this to make sure that this error does not kill entire context group.
	// We want the caller (singleRangeFeed) to decide if this error is retry-able.
	var err error
	ms.client, err = client.MuxRangeFeed(ctx)
	ms.initCtx.close(err)

	if err == nil {
		doneCtx.close(m.receiveEventsFromNode(ctx, ms))
	}

	// We propagated error to the caller via init/done context.
	return nil //nolint:returnerrcheck
}

// demuxLoop de-multiplexes events and sends them to appropriate rangefeed event
// consumer.
func (m *rangefeedMuxer) demuxLoop(ctx context.Context) (retErr error) {
	defer close(m.demuxLoopDone)

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
			// producer.muxClientCtx.Done()). That's fine -- we may have encountered range
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
			case <-producer.muxClientCtx.Done():
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

// termCtx implements terminationContext, and allows error to be set.
type termCtx struct {
	sync.Once
	done chan struct{}
	err  error
}

func makeTerminationContext() termCtx {
	return termCtx{done: make(chan struct{})}
}

func (tc *termCtx) Done() <-chan struct{} {
	return tc.done
}
func (tc *termCtx) Err() error {
	return tc.err
}

// close closes this context with specified error.
func (tc *termCtx) close(err error) {
	tc.Do(func() {
		tc.err = err
		close(tc.done)
	})
}

// receiveEventsFromNode receives mux rangefeed events, and forwards them to the
// demuxLoop.
// Passed in context must be the context used to create ms.client.
func (m *rangefeedMuxer) receiveEventsFromNode(ctx context.Context, ms *muxClientState) error {
	for {
		event, streamErr := ms.client.Recv()

		if streamErr != nil {
			return streamErr
		}

		select {
		case <-ctx.Done():
			// Normally, when ctx is done, we would receive streamErr above.
			// But it's possible that the context was canceled right after the last Recv(),
			// and in that case we must exit.
			return ctx.Err()
		case <-m.demuxLoopDone:
			// demuxLoop exited, and so should we (happens when main context group completes)
			return nil
		case m.eventCh <- event:
		}
	}
}
