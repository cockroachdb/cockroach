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
	"fmt"
	"io"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangecache"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

type muxRangeFeed struct {
	ds       *DistSender
	rr       *rangeFeedRegistry
	withDiff bool

	g           ctxgroup.Group
	muxRegistry muxStreamRegistry
	eventsCh    chan<- *roachpb.RangeFeedEvent
}

// startMuxRangeFeed starts rangefeed using MuxRangeFeedStream RPC.
func (ds *DistSender) startMuxRangeFeed(
	ctx context.Context,
	spans []roachpb.Span,
	startFrom hlc.Timestamp,
	withDiff bool,
	rr *rangeFeedRegistry,
	eventCh chan<- *roachpb.RangeFeedEvent,
) error {
	rSpans, err := spansToResolvedSpans(spans)
	if err != nil {
		return err
	}

	mrf := muxRangeFeed{
		g:        ctxgroup.WithContext(ctx),
		ds:       ds,
		rr:       rr,
		eventsCh: eventCh,
		withDiff: withDiff,
	}

	// Kick off the initial set of ranges.
	for i := range rSpans {
		rSpan := rSpans[i]
		mrf.g.GoCtx(func(ctx context.Context) error {
			return divideAndIterateRSpan(ctx, ds, rSpan,
				func(rs roachpb.RSpan, token rangecache.EvictionToken) error {
					return mrf.startMuxRangeFeed(ctx, rs, startFrom, token)
				},
			)
		})
	}

	return mrf.g.Wait()
}

type rangeState struct {
	*activeRangeFeed
	startFrom hlc.Timestamp
	token     rangecache.EvictionToken
	rSpan     roachpb.RSpan
}

func (rs *rangeState) restartFrom() hlc.Timestamp {
	restartFrom := rs.activeRangeFeed.snapshot().Resolved
	if restartFrom.Less(rs.startFrom) {
		// RangeFeed happily forwards any closed timestamps it receives as
		// soon as there are no outstanding intents under them.
		// There it's possible that the resolved timestamp observed by this stream
		// might be lower than the initial startFrom.
		restartFrom = rs.startFrom
	}
	return restartFrom
}

type rangeStateKey struct {
	streamID int64
	rangeID  roachpb.RangeID
}

func makeRangeStateKey(rangeID roachpb.RangeID, streamID int64) rangeStateKey {
	return rangeStateKey{
		streamID: streamID,
		rangeID:  rangeID,
	}
}

// String implements stringer interface.
func (k rangeStateKey) String() string {
	return fmt.Sprintf("{rangeID: %d, streamID: %d}", k.rangeID, k.streamID)
}

// startMuxRangeFeed opens MuxRangeFeed connection to a node that can serve
// rangefeed for the one of the replicas in token range descriptor.
func (mrf *muxRangeFeed) startMuxRangeFeed(
	ctx context.Context, rs roachpb.RSpan, startFrom hlc.Timestamp, token rangecache.EvictionToken,
) error {
	if ctx.Err() != nil {
		// Don't bother starting stream if we are already cancelled.
		return ctx.Err()
	}

	for r := retry.StartWithCtx(ctx, mrf.ds.rpcRetryOptions); r.Next(); {
		if !token.Valid() {
			var err error
			ri, err := mrf.ds.getRoutingInfo(ctx, rs.Key, rangecache.EvictionToken{}, false)
			if err != nil {
				log.VErrEventf(ctx, 1, "range descriptor re-lookup failed: %s", err)
				if !rangecache.IsRangeLookupErrorRetryable(err) {
					return err
				}
				continue
			}
			token = ri
		}

		muxer, err := mrf.lookupOrCreateStream(ctx, token)

		if err != nil {
			if IsSendError(err) {
				token.Evict(ctx)
				continue
			}
			return err
		}

		err = muxer.rangeFeed(token, rs, startFrom)
		if err == nil {
			return nil
		}

		log.VErrEventf(ctx, 2, "RPC error: %s", err)
		if grpcutil.IsAuthError(err) {
			// Authentication or authorization error. Propagate.
			return err
		}
		// Otherwise, fallback to retry.
	}

	return ctx.Err()
}

func (mrf *muxRangeFeed) lookupOrCreateStream(
	ctx context.Context, token rangecache.EvictionToken,
) (*nodeMuxer, error) {
	transport, replicas, err := mrf.ds.prepareTransportForDescriptor(ctx, token.Desc())
	if err != nil {
		return nil, err
	}
	defer transport.Release()

	for !transport.IsExhausted() {
		muxer, err := mrf.muxRegistry.lookupOrCreateMuxRangeFeedForNextReplica(ctx, mrf, transport)
		if err == nil {
			return muxer, err
		}
	}

	return nil, newSendError(
		fmt.Sprintf("sending to all %d replicas failed", len(replicas)))
}

func (mrf *muxRangeFeed) restartRange(
	ctx context.Context, rs roachpb.RSpan, restartFrom hlc.Timestamp,
) error {
	return divideAndIterateRSpan(ctx, mrf.ds, rs,
		func(rs roachpb.RSpan, token rangecache.EvictionToken) error {
			return mrf.startMuxRangeFeed(ctx, rs, restartFrom, token)
		},
	)
}

// muxStreamRegistry maintains the state necessary to execute mux rangefeed.
type muxStreamRegistry struct {
	sync.Map // nodeID -> *nodeMuxer

	mu struct {
		syncutil.Mutex
		// Counter identifying particular RangeFeed stream.
		rangeFeedID int64
	}
}

// lookupOrCreateMuxRangeFeedForNextReplica attempts to see if the MuxRangeFeed is
// running on the next transport replica.  If so, returns the nodeMuxer structure
// which coordinates rangefeed streams on that node.
// If not found, connects to the next replica, establishes MuxRangeFeed stream, and
// registers and returns nodeMuxer.
func (r *muxStreamRegistry) lookupOrCreateMuxRangeFeedForNextReplica(
	ctx context.Context, mrf *muxRangeFeed, transport Transport,
) (*nodeMuxer, error) {
	replica := transport.NextReplica()
	nodeID := replica.NodeID
	if val, ok := r.Map.Load(nodeID); ok {
		return val.(*nodeMuxer), nil
	}

	clientCtx, client, err := transport.NextInternalClient(ctx)
	if err != nil {
		return nil, err
	}

	stream, err := client.MuxRangeFeed(clientCtx)
	if err != nil {
		return nil, err
	}

	m := &nodeMuxer{
		replica: replica,
		stream:  stream,
		close:   func() { r.Delete(nodeID) },
		nextID: func() int64 {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.mu.rangeFeedID++
			return r.mu.rangeFeedID
		},
		mrf: mrf,
	}

	mrf.g.GoCtx(m.receive)
	r.Store(nodeID, m)

	return m, nil
}

// nodeMuxer is responsible for running multiplexing rangefeed on a single node.
type nodeMuxer struct {
	replica roachpb.ReplicaDescriptor
	stream  roachpb.Internal_MuxRangeFeedClient
	close   func()
	nextID  func() int64
	mrf     *muxRangeFeed

	mu struct {
		syncutil.Mutex
		activeFeeds map[rangeStateKey]rangeState
	}
}

// rangeFeed issues rangefeed for this nodeMuxer.
// Note: it is possible that this call fails.  In such cases any errors returned by this
// function ought to be treated as a retryable error, and another replica should be attempted.
// This is mostly due to the fact that there is a race between previously started MuxRangeFeed
// exiting (e.g. because of node shutdown), and an attempt to start rangefeed on that node.
func (m *nodeMuxer) rangeFeed(
	token rangecache.EvictionToken, rs roachpb.RSpan, startFrom hlc.Timestamp,
) error {
	streamID := m.nextID()
	rangeID := token.Desc().RangeID
	sp := rs.AsRawSpanWithNoLocals()
	req := roachpb.RangeFeedRequest{
		Span: sp,
		Header: roachpb.Header{
			Timestamp: startFrom,
			RangeID:   rangeID,
		},
		WithDiff: m.mrf.withDiff,
		StreamID: streamID,
	}
	req.Replica = m.replica

	// NB: mu must be locked before we Send on the stream.
	// This is so that a race with stream terminating (due to an EOF) doesn't miss
	// this new stream when it restarts.
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.mu.activeFeeds == nil {
		m.mu.activeFeeds = make(map[rangeStateKey]rangeState)
	}

	rfState := rangeState{
		activeRangeFeed: newActiveRangeFeed(m.mrf.rr, rangeID, sp, startFrom),
		startFrom:       startFrom,
		token:           token,
		rSpan:           rs,
	}
	rfKey := makeRangeStateKey(rangeID, streamID)
	m.mu.activeFeeds[rfKey] = rfState

	if err := m.stream.Send(&req); err != nil {
		// Cleanup, and return an error to the caller, which will retry this range feed
		// with another replica.
		delete(m.mu.activeFeeds, rfKey)
		rfState.activeRangeFeed.release()
		log.Infof(context.Background(), "Clearing out range state for %s: send error: %v", rfKey, err)
		return err
	}

	return nil
}

func (m *nodeMuxer) restartAllActive(ctx context.Context) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, active := range m.mu.activeFeeds {
		if err := m.mrf.restartRange(ctx, active.rSpan, active.restartFrom()); err != nil {
			return 0, err
		}
	}
	return len(m.mu.activeFeeds), nil
}

func (m *nodeMuxer) clearActive(key rangeStateKey) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if rs, found := m.mu.activeFeeds[key]; found {
		rs.activeRangeFeed.release()
		delete(m.mu.activeFeeds, key)
		return nil
	}
	return errors.AssertionFailedf(
		"expected to find active range feed for %s, found none", key)
}

// receive is responsible for processing rangefeed events from a node.
func (m *nodeMuxer) receive(ctx context.Context) error {
	for {
		event, eventErr := m.stream.Recv()
		if eventErr == io.EOF {
			m.close()
			numRestarted, err := m.restartAllActive(ctx)
			if err != nil {
				return err
			}
			if log.V(1) {
				log.Infof(ctx, "RangeFeed for node %d disconnected.  Restarted %d ranges",
					m.replica.NodeID, numRestarted)
			}
			return nil
		}

		// Get range state associated with this rangefeed.
		var rs rangeState
		if eventErr == nil {
			if event == nil {
				return errors.AssertionFailedf("unexpected state: expected non nil event")
			}
			rsKey := rangeStateKey{streamID: event.StreamID, rangeID: event.RangeID}
			m.mu.Lock()
			rs = m.mu.activeFeeds[rsKey]
			m.mu.Unlock()
			if rs.activeRangeFeed == nil {
				return errors.AssertionFailedf(
					"unexpected state: expected to find active range feed for %s, found none",
					rsKey)
			}
		}

		if eventErr == nil && event.Error != nil {
			eventErr = event.Error.Error.GoError()
		}

		if eventErr == nil {
			// Dispatch event to the caller.
			rs.onRangeEvent(&event.RangeFeedEvent)

			select {
			case m.mrf.eventsCh <- &event.RangeFeedEvent:
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}

		errDisposition, err := handleRangeError(eventErr)
		if err != nil {
			return err
		}

		// Clear active range feed -- it will be re-added when we retry/restart.
		if err := m.clearActive(makeRangeStateKey(event.RangeID, event.StreamID)); err != nil {
			return err
		}

		restartFrom := rs.restartFrom()
		if log.V(1) {
			log.Infof(ctx, "Transient error (%v) for rangefeed from node %d for range %d: %s %s@%s",
				eventErr, m.replica.NodeID, event.RangeID, errDisposition, rs.rSpan, restartFrom)
		}

		switch errDisposition {
		case retryRange:
			if err := m.mrf.startMuxRangeFeed(ctx, rs.rSpan, restartFrom, rs.token); err != nil {
				return err
			}
		case restartRange:
			rs.token.Evict(ctx)
			if err := m.mrf.restartRange(ctx, rs.rSpan, restartFrom); err != nil {
				return err
			}
		default:
			panic("unexpected error disposition")
		}
	}
}
