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

type rfStream struct {
	g        ctxgroup.Group
	ds       *DistSender
	rr       *rangeFeedRegistry
	eventsCh chan<- *roachpb.RangeFeedEvent
	withDiff bool

	mu struct {
		syncutil.Mutex

		// Currently running MuxRangeFeed streams.
		streams map[roachpb.NodeID]*streamState

		// Counter identifying particular RangeFeed stream.
		rangeFeedID int64
	}
}

// startMuxRangeFeed starts rangefeed using streaming RangeFeedStream RPC.
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

	rfs := rfStream{
		g:        ctxgroup.WithContext(ctx),
		ds:       ds,
		rr:       rr,
		eventsCh: eventCh,
		withDiff: withDiff,
	}

	// Kick off the initial set of ranges.
	for i := range rSpans {
		rSpan := rSpans[i]
		rfs.g.GoCtx(func(ctx context.Context) error {
			return divideAndIterateRSpan(ctx, ds, rSpan,
				func(rs roachpb.RSpan, token rangecache.EvictionToken) error {
					return rfs.startRangeFeedStream(ctx, rs, startFrom, token)
				},
			)
		})
	}

	return rfs.g.Wait()
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
	requestID int64
	rangeID   roachpb.RangeID
}

type streamState struct {
	nodeID roachpb.NodeID
	stream roachpb.Internal_MuxRangeFeedClient

	mu struct {
		syncutil.Mutex
		activeFeeds map[rangeStateKey]rangeState
	}
}

func (ss *streamState) getRangeState(key rangeStateKey) rangeState {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return ss.mu.activeFeeds[key]
}

func (ss *streamState) clearActive(key rangeStateKey) error {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if rs, found := ss.mu.activeFeeds[key]; found {
		rs.activeRangeFeed.release()
		delete(ss.mu.activeFeeds, key)
		return nil
	}
	return errors.AssertionFailedf("expected to find active range feed %s, found none", key)
}

func (ss *streamState) numActiveFeeds() int {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return len(ss.mu.activeFeeds)
}

func (ss *streamState) markActive(
	rr *rangeFeedRegistry,
	nodeID roachpb.NodeID,
	requestID int64,
	token rangecache.EvictionToken,
	rSpan roachpb.RSpan,
	startFrom hlc.Timestamp,
) rangeStateKey {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	if ss.mu.activeFeeds == nil {
		ss.mu.activeFeeds = make(map[rangeStateKey]rangeState)
	}

	rangeID := token.Desc().RangeID
	span := rSpan.AsRawSpanWithNoLocals()

	active := newActiveRangeFeed(rr, span, startFrom)
	active.setNodeID(nodeID)
	active.setRangeID(rangeID)

	key := rangeStateKey{
		requestID: requestID,
		rangeID:   rangeID,
	}

	ss.mu.activeFeeds[key] = rangeState{
		activeRangeFeed: active,
		token:           token,
		rSpan:           rSpan,
		startFrom:       startFrom,
	}
	return key
}

// nextRequestID returns the next id to use when issuing RangeFeed calls on MuxStream.
func (rfs *rfStream) nextRequestID() int64 {
	rfs.mu.Lock()
	defer rfs.mu.Unlock()
	rfs.mu.rangeFeedID++
	return rfs.mu.rangeFeedID
}

// startRangeFeedStreem opens RangeFeedStream connection to a node that can serve
// rangefeed for the one of the replicas in token range descriptor.
func (rfs *rfStream) startRangeFeedStream(
	ctx context.Context, rs roachpb.RSpan, startFrom hlc.Timestamp, token rangecache.EvictionToken,
) error {
	if ctx.Err() != nil {
		// Don't bother starting stream if we are already cancelled.
		return ctx.Err()
	}

	for r := retry.StartWithCtx(ctx, rfs.ds.rpcRetryOptions); r.Next(); {
		if !token.Valid() {
			var err error
			ri, err := rfs.ds.getRoutingInfo(ctx, rs.Key, rangecache.EvictionToken{}, false)
			if err != nil {
				log.VErrEventf(ctx, 1, "range descriptor re-lookup failed: %s", err)
				if !rangecache.IsRangeLookupErrorRetryable(err) {
					return err
				}
				continue
			}
			token = ri
		}

		ss, replica, mustStartConsumer, err := rfs.lookupOrCreateStream(ctx, token)

		if err != nil {
			if IsSendError(err) {
				token.Evict(ctx)
				continue
			}
			return err
		}

		if mustStartConsumer {
			rfs.g.GoCtx(func(ctx context.Context) error {
				if ss == nil {
					panic("ss is nil")
				}
				return rfs.rfStreamEventProcessor(ctx, ss)
			})
		}

		rangeID := token.Desc().RangeID
		activeKey := ss.markActive(rfs.rr, replica.NodeID, rfs.nextRequestID(), token, rs, startFrom)

		req := roachpb.RangeFeedRequest{
			Span: rs.AsRawSpanWithNoLocals(),
			Header: roachpb.Header{
				Timestamp: startFrom,
				RangeID:   rangeID,
			},
			WithDiff:  rfs.withDiff,
			RequestID: activeKey.requestID,
		}
		req.Replica = replica
		err = ss.stream.Send(&req)
		if err == nil {
			return nil
		}

		log.VErrEventf(ctx, 2, "RPC error: %s", err)
		if grpcutil.IsAuthError(err) {
			// Authentication or authorization error. Propagate.
			return err
		}
		// Otherwise, fallback to retry.
		if err := ss.clearActive(activeKey); err != nil {
			return err
		}
	}

	return ctx.Err()
}

func (rfs *rfStream) lookupOrCreateStream(
	ctx context.Context, token rangecache.EvictionToken,
) (*streamState, roachpb.ReplicaDescriptor, bool, error) {
	transport, replicas, err := rfs.ds.prepareTransportForDescriptor(ctx, token.Desc())
	if err != nil {
		return nil, roachpb.ReplicaDescriptor{}, false, err
	}
	defer transport.Release()

	for !transport.IsExhausted() {
		ss, replica, mustStartConsumer, err := rfs.lookupOrCreateStreamForNextReplica(ctx, transport)
		if err == nil {
			return ss, replica, mustStartConsumer, err
		}
	}

	return nil, roachpb.ReplicaDescriptor{}, false, newSendError(
		fmt.Sprintf("sending to all %d replicas failed", len(replicas)))
}

func (rfs *rfStream) lookupOrCreateStreamForNextReplica(
	ctx context.Context, transport Transport,
) (*streamState, roachpb.ReplicaDescriptor, bool, error) {
	replica := transport.NextReplica()
	nodeID := replica.NodeID

	rfs.mu.Lock()
	defer rfs.mu.Unlock()

	if rfs.mu.streams == nil {
		rfs.mu.streams = make(map[roachpb.NodeID]*streamState)
	}
	if ss, ok := rfs.mu.streams[nodeID]; ok {
		return ss, replica, false, nil
	}
	clientCtx, client, err := transport.NextInternalClient(ctx)
	if err != nil {
		return nil, roachpb.ReplicaDescriptor{}, false, err
	}
	stream, err := client.MuxRangeFeed(clientCtx)
	if err != nil {
		return nil, roachpb.ReplicaDescriptor{}, false, err
	}

	ss := &streamState{
		nodeID: nodeID,
		stream: stream,
	}

	rfs.mu.streams[nodeID] = ss
	return ss, replica, true, nil
}

func (rfs *rfStream) restartAllActive(ctx context.Context, ss *streamState) error {
	rfs.mu.Lock()
	defer rfs.mu.Unlock()

	if rfs.mu.streams[ss.nodeID] != ss {
		panic("corrupt rangefeed state")
	}

	delete(rfs.mu.streams, ss.nodeID)

	ss.mu.Lock()
	defer ss.mu.Unlock()

	for _, active := range ss.mu.activeFeeds {
		if err := rfs.restartRange(ctx, active.rSpan, active.restartFrom()); err != nil {
			return err
		}
	}

	return nil
}

func (rfs *rfStream) restartRange(
	ctx context.Context, rs roachpb.RSpan, restartFrom hlc.Timestamp,
) error {
	return divideAndIterateRSpan(ctx, rfs.ds, rs,
		func(rs roachpb.RSpan, token rangecache.EvictionToken) error {
			return rfs.startRangeFeedStream(ctx, rs, restartFrom, token)
		},
	)
}

// rfStreamEventProcessor is responsible for processing rangefeed events from a node.
func (rfs *rfStream) rfStreamEventProcessor(ctx context.Context, ss *streamState) error {
	for {
		event, eventErr := ss.stream.Recv()
		if eventErr == io.EOF {
			if log.V(1) {
				log.Infof(ctx, "RangeFeed for node %d disconnected.  Restarting %d ranges",
					ss.nodeID, ss.numActiveFeeds())
			}
			return rfs.restartAllActive(ctx, ss)
		}

		// Get range state associated with this rangefeed.
		var rs rangeState
		if eventErr == nil {
			if event == nil {
				return errors.AssertionFailedf("unexpected state: expected non nil event")
			}
			rsKey := rangeStateKey{requestID: event.RequestID, rangeID: event.RangeID}
			rs = ss.getRangeState(rsKey)
			if rs.activeRangeFeed == nil {
				return errors.AssertionFailedf(
					"unexpected state: expected to find active range feed for %v, found none",
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
			case rfs.eventsCh <- &event.RangeFeedEvent:
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
		if err := ss.clearActive(
			rangeStateKey{requestID: event.RequestID, rangeID: event.RangeID},
		); err != nil {
			return err
		}

		restartFrom := rs.restartFrom()
		if log.V(1) {
			log.Infof(ctx, "Transient error (%v) for rangefeed from node %d for range %d: %s %s@%s",
				eventErr, ss.nodeID, event.RangeID, errDisposition, rs.rSpan, restartFrom)
		}

		switch errDisposition {
		case retryRange:
			if err := rfs.startRangeFeedStream(ctx, rs.rSpan, restartFrom, rs.token); err != nil {
				return err
			}
		case restartRange:
			if err := rfs.restartRange(ctx, rs.rSpan, restartFrom); err != nil {
				return err
			}
		default:
			panic("unexpected error disposition")
		}
	}
}
