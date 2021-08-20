// Copyright 2018 The Cockroach Authors.
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
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangecache"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

type singleRangeInfo struct {
	rs    roachpb.RSpan
	ts    hlc.Timestamp
	token rangecache.EvictionToken
}

// RangeFeed divides a RangeFeed request on range boundaries and establishes a
// RangeFeed to each of the individual ranges. It streams back results on the
// provided channel.
//
// Note that the timestamps in RangeFeedCheckpoint events that are streamed back
// may be lower than the timestamp given here.
func (ds *DistSender) RangeFeed(
	ctx context.Context,
	span roachpb.Span,
	ts hlc.Timestamp,
	withDiff bool,
	eventCh chan<- *roachpb.RangeFeedEvent,
) error {
	ctx = ds.AnnotateCtx(ctx)
	ctx, sp := tracing.EnsureChildSpan(ctx, ds.AmbientContext.Tracer, "dist sender")
	defer sp.Finish()

	rs, err := keys.SpanAddr(span)
	if err != nil {
		return err
	}

	rr := newRangeFeedRegistry(ctx, span, ts, withDiff)
	ds.activeRangeFeeds.Store(rr, nil)
	defer ds.activeRangeFeeds.Delete(rr)

	g := ctxgroup.WithContext(ctx)
	// Goroutine that processes subdivided ranges and creates a rangefeed for
	// each.
	rangeCh := make(chan singleRangeInfo, 16)
	g.GoCtx(func(ctx context.Context) error {
		for {
			select {
			case sri := <-rangeCh:
				// Spawn a child goroutine to process this feed.
				g.GoCtx(func(ctx context.Context) error {
					return ds.partialRangeFeed(ctx, rr, sri.rs, sri.ts, sri.token, withDiff, rangeCh, eventCh)
				})
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})

	// Kick off the initial set of ranges.
	g.GoCtx(func(ctx context.Context) error {
		return ds.divideAndSendRangeFeedToRanges(ctx, rs, ts, rangeCh)
	})

	return g.Wait()
}

// RangeFeedContext is the structure containing arguments passed to
// RangeFeed call.  It functions as a kind of key for an active range feed.
type RangeFeedContext struct {
	ID      int64  // unique ID identifying range feed.
	CtxTags string // context tags

	// Span, timestamp and withDiff options passed to RangeFeed call.
	Span     roachpb.Span
	TS       hlc.Timestamp
	WithDiff bool
}

// PartialRangeFeed structure describes the state of currently executing partial range feed.
type PartialRangeFeed struct {
	Span              roachpb.Span
	StartTS           hlc.Timestamp
	NodeID            roachpb.NodeID
	RangeID           roachpb.RangeID
	LastValueReceived time.Time
	Resolved          hlc.Timestamp
}

// ActiveRangeFeedIterFn is an iterator function which is passed PartialRangeFeed structure.
// Iterator function may return an iterutil.StopIteration sentinel error to stop iteration
// early; any other error is propagated.
type ActiveRangeFeedIterFn func(rfCtx RangeFeedContext, feed PartialRangeFeed) error

// ForEachActiveRangeFeed invokes provided function for each active range feed.
func (ds *DistSender) ForEachActiveRangeFeed(fn ActiveRangeFeedIterFn) (iterErr error) {
	const continueIter = true
	const stopIter = false

	partialRangeFeed := func(active *activeRangeFeed) PartialRangeFeed {
		active.Lock()
		defer active.Unlock()
		return active.PartialRangeFeed
	}

	ds.activeRangeFeeds.Range(func(k, v interface{}) bool {
		r := k.(*rangeFeedRegistry)
		r.ranges.Range(func(k, v interface{}) bool {
			active := k.(*activeRangeFeed)
			if err := fn(r.RangeFeedContext, partialRangeFeed(active)); err != nil {
				iterErr = err
				return stopIter
			}
			return continueIter
		})
		return iterErr == nil
	})

	if iterutil.Done(iterErr) {
		iterErr = nil // Early termination is fine.
	}

	return
}

// activeRangeFeed is a thread safe PartialRangeFeed.
type activeRangeFeed struct {
	syncutil.Mutex
	PartialRangeFeed
}

func (a *activeRangeFeed) onRangeEvent(
	nodeID roachpb.NodeID, rangeID roachpb.RangeID, event *roachpb.RangeFeedEvent,
) {
	a.Lock()
	defer a.Unlock()
	if event.Val != nil {
		a.LastValueReceived = timeutil.Now()
	} else if event.Checkpoint != nil {
		a.Resolved = event.Checkpoint.ResolvedTS
	}

	a.NodeID = nodeID
	a.RangeID = rangeID
}

// rangeFeedRegistry is responsible for keeping track of currently executing
// range feeds.
type rangeFeedRegistry struct {
	RangeFeedContext
	ranges sync.Map // map[*activeRangeFeed]nil
}

func newRangeFeedRegistry(
	ctx context.Context, span roachpb.Span, ts hlc.Timestamp, withDiff bool,
) *rangeFeedRegistry {
	rr := &rangeFeedRegistry{
		RangeFeedContext: RangeFeedContext{
			Span:     span,
			TS:       ts,
			WithDiff: withDiff,
		},
	}
	rr.ID = *(*int64)(unsafe.Pointer(&rr))

	if b := logtags.FromContext(ctx); b != nil {
		rr.CtxTags = b.String()
	}
	return rr
}

func (ds *DistSender) divideAndSendRangeFeedToRanges(
	ctx context.Context, rs roachpb.RSpan, ts hlc.Timestamp, rangeCh chan<- singleRangeInfo,
) error {
	// As RangeIterator iterates, it can return overlapping descriptors (and
	// during splits, this happens frequently), but divideAndSendRangeFeedToRanges
	// intends to split up the input into non-overlapping spans aligned to range
	// boundaries. So, as we go, keep track of the remaining uncovered part of
	// `rs` in `nextRS`.
	nextRS := rs
	ri := NewRangeIterator(ds)
	for ri.Seek(ctx, nextRS.Key, Ascending); ri.Valid(); ri.Next(ctx) {
		desc := ri.Desc()
		partialRS, err := nextRS.Intersect(desc)
		if err != nil {
			return err
		}
		nextRS.Key = partialRS.EndKey
		select {
		case rangeCh <- singleRangeInfo{
			rs:    partialRS,
			ts:    ts,
			token: ri.Token(),
		}:
		case <-ctx.Done():
			return ctx.Err()
		}
		if !ri.NeedAnother(nextRS) {
			break
		}
	}
	return ri.Error()
}

// partialRangeFeed establishes a RangeFeed to the range specified by desc. It
// manages lifecycle events of the range in order to maintain the RangeFeed
// connection; this may involve instructing higher-level functions to retry
// this rangefeed, or subdividing the range further in the event of a split.
func (ds *DistSender) partialRangeFeed(
	ctx context.Context,
	rr *rangeFeedRegistry,
	rs roachpb.RSpan,
	ts hlc.Timestamp,
	token rangecache.EvictionToken,
	withDiff bool,
	rangeCh chan<- singleRangeInfo,
	eventCh chan<- *roachpb.RangeFeedEvent,
) error {
	// Bound the partial rangefeed to the partial span.
	span := rs.AsRawSpanWithNoLocals()

	// Register partial range feed with registry.
	active := &activeRangeFeed{
		PartialRangeFeed: PartialRangeFeed{
			Span:    span,
			StartTS: ts,
		},
	}
	rr.ranges.Store(active, nil)
	defer rr.ranges.Delete(active)

	// Start a retry loop for sending the batch to the range.
	for r := retry.StartWithCtx(ctx, ds.rpcRetryOptions); r.Next(); {
		// If we've cleared the descriptor on a send failure, re-lookup.
		if !token.Valid() {
			var err error
			ri, err := ds.getRoutingInfo(ctx, rs.Key, rangecache.EvictionToken{}, false)
			if err != nil {
				log.VErrEventf(ctx, 1, "range descriptor re-lookup failed: %s", err)
				if !rangecache.IsRangeLookupErrorRetryable(err) {
					return err
				}
				continue
			}
			token = ri
		}

		// Establish a RangeFeed for a single Range.
		maxTS, err := ds.singleRangeFeed(ctx, span, ts, withDiff, token.Desc(), eventCh, active.onRangeEvent)

		// Forward the timestamp in case we end up sending it again.
		ts.Forward(maxTS)

		if err != nil {
			if log.V(1) {
				log.Infof(ctx, "RangeFeed %s disconnected with last checkpoint %s ago: %v",
					span, timeutil.Since(ts.GoTime()), err)
			}
			switch {
			case errors.HasType(err, (*roachpb.StoreNotFoundError)(nil)) ||
				errors.HasType(err, (*roachpb.NodeUnavailableError)(nil)):
				// These errors are likely to be unique to the replica that
				// reported them, so no action is required before the next
				// retry.
			case IsSendError(err), errors.HasType(err, (*roachpb.RangeNotFoundError)(nil)):
				// Evict the descriptor from the cache and reload on next attempt.
				token.Evict(ctx)
				token = rangecache.EvictionToken{}
				continue
			case errors.HasType(err, (*roachpb.RangeKeyMismatchError)(nil)):
				// Evict the descriptor from the cache.
				token.Evict(ctx)
				return ds.divideAndSendRangeFeedToRanges(ctx, rs, ts, rangeCh)
			case errors.HasType(err, (*roachpb.RangeFeedRetryError)(nil)):
				var t *roachpb.RangeFeedRetryError
				if ok := errors.As(err, &t); !ok {
					return errors.AssertionFailedf("wrong error type: %T", err)
				}
				switch t.Reason {
				case roachpb.RangeFeedRetryError_REASON_REPLICA_REMOVED,
					roachpb.RangeFeedRetryError_REASON_RAFT_SNAPSHOT,
					roachpb.RangeFeedRetryError_REASON_LOGICAL_OPS_MISSING,
					roachpb.RangeFeedRetryError_REASON_SLOW_CONSUMER:
					// Try again with same descriptor. These are transient
					// errors that should not show up again.
					continue
				case roachpb.RangeFeedRetryError_REASON_RANGE_SPLIT,
					roachpb.RangeFeedRetryError_REASON_RANGE_MERGED,
					roachpb.RangeFeedRetryError_REASON_NO_LEASEHOLDER:
					// Evict the descriptor from the cache.
					token.Evict(ctx)
					return ds.divideAndSendRangeFeedToRanges(ctx, rs, ts, rangeCh)
				default:
					return errors.AssertionFailedf("unrecognized retriable error type: %T", err)
				}
			default:
				return err
			}
		}
	}
	return ctx.Err()
}

// onRangeEventCb is invoked for each non-error range event.
// nodeID identifies the node ID which generated the event.
type onRangeEventCb func(nodeID roachpb.NodeID, rangeID roachpb.RangeID, event *roachpb.RangeFeedEvent)

// singleRangeFeed gathers and rearranges the replicas, and makes a RangeFeed
// RPC call. Results will be sent on the provided channel. Returns the timestamp
// of the maximum rangefeed checkpoint seen, which can be used to re-establish
// the rangefeed with a larger starting timestamp, reflecting the fact that all
// values up to the last checkpoint have already been observed. Returns the
// request's timestamp if not checkpoints are seen.
func (ds *DistSender) singleRangeFeed(
	ctx context.Context,
	span roachpb.Span,
	ts hlc.Timestamp,
	withDiff bool,
	desc *roachpb.RangeDescriptor,
	eventCh chan<- *roachpb.RangeFeedEvent,
	onRangeEvent onRangeEventCb,
) (hlc.Timestamp, error) {
	args := roachpb.RangeFeedRequest{
		Span: span,
		Header: roachpb.Header{
			Timestamp: ts,
			RangeID:   desc.RangeID,
		},
		WithDiff: withDiff,
	}

	var latencyFn LatencyFunc
	if ds.rpcContext != nil {
		latencyFn = ds.rpcContext.RemoteClocks.Latency
	}
	replicas, err := NewReplicaSlice(ctx, ds.nodeDescs, desc, nil, AllExtantReplicas)
	if err != nil {
		return args.Timestamp, err
	}
	replicas.OptimizeReplicaOrder(ds.getNodeDescriptor(), latencyFn)
	// The RangeFeed is not used for system critical traffic so use a DefaultClass
	// connection regardless of the range.
	opts := SendOptions{class: rpc.DefaultClass}
	transport, err := ds.transportFactory(opts, ds.nodeDialer, replicas)
	if err != nil {
		return args.Timestamp, err
	}
	defer transport.Release()

	for {
		if transport.IsExhausted() {
			return args.Timestamp, newSendError(
				fmt.Sprintf("sending to all %d replicas failed", len(replicas)))
		}

		args.Replica = transport.NextReplica()
		clientCtx, client, err := transport.NextInternalClient(ctx)
		if err != nil {
			log.VErrEventf(ctx, 2, "RPC error: %s", err)
			continue
		}
		log.VEventf(ctx, 3, "attempting to create a RangeFeed over replica %s", args.Replica)
		stream, err := client.RangeFeed(clientCtx, &args)
		if err != nil {
			log.VErrEventf(ctx, 2, "RPC error: %s", err)
			if grpcutil.IsAuthError(err) {
				// Authentication or authorization error. Propagate.
				return args.Timestamp, err
			}
			continue
		}
		for {
			event, err := stream.Recv()
			if err == io.EOF {
				return args.Timestamp, nil
			}
			if err != nil {
				return args.Timestamp, err
			}
			switch t := event.GetValue().(type) {
			case *roachpb.RangeFeedCheckpoint:
				if t.Span.Contains(args.Span) {
					args.Timestamp.Forward(t.ResolvedTS)
				}
			case *roachpb.RangeFeedError:
				log.VErrEventf(ctx, 2, "RangeFeedError: %s", t.Error.GoError())
				return args.Timestamp, t.Error.GoError()
			}
			onRangeEvent(args.Replica.NodeID, desc.RangeID, event)

			select {
			case eventCh <- event:
			case <-ctx.Done():
				return args.Timestamp, ctx.Err()
			}
		}
	}
}
