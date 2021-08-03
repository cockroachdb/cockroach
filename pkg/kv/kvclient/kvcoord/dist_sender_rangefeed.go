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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangecache"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
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

	rf := &rangeFeed{}
	ds.rangeFeeds.Store(rf, nil)
	g := ctxgroup.WithContext(ctx)
	// Goroutine that processes subdivided ranges and creates a rangefeed for
	// each.
	rangeCh := make(chan singleRangeInfo, 16)
	g.GoCtx(func(ctx context.Context) error {
		timer := timeutil.NewTimer()
		timer.Reset(10 * time.Minute)
		for {
			select {
			case <-timer.C:
				timer.Read = true
				maybeCancel := func(prf *partialRangeFeed) {
					prf.mu.Lock()
					defer prf.mu.Unlock()
					if prf.mu.canceled {
						return
					}
					// TODO(ajwerner): Consult some settings and what not.
					const tooLong = 5 * time.Minute
					if timeutil.Since(prf.mu.ts.GoTime()) > tooLong {
						prf.mu.canceled = true
						prf.mu.cancel()
					}
				}
				rf.m.Range(func(key, value interface{}) (wantMore bool) {
					maybeCancel(key.(*partialRangeFeed))
					return true
				})
			case sri := <-rangeCh:
				// Spawn a child goroutine to process this feed.
				// TODO(ajwerner): Consider pooling these things.
				prf := &partialRangeFeed{
					rs: sri.rs, token: sri.token,
				}
				prf.mu.streamCtx, prf.mu.cancel = context.WithCancel(ctx)
				prf.mu.ts = sri.ts
				rf.m.Store(prf, nil)
				g.GoCtx(func(ctx context.Context) error {
					defer rf.m.Delete(prf)
					return ds.partialRangeFeed(ctx, prf, withDiff, rangeCh, eventCh)
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

// TODO(ajwerner): Expose iterating the partialRangeFeed structures
// through here and then hook up iterating these things off of the
// DistSender in a crdb_internal virtual table.
type rangeFeed struct {
	m sync.Map
}

type partialRangeFeed struct {
	rs    roachpb.RSpan
	token rangecache.EvictionToken

	mu struct {
		syncutil.Mutex
		ts hlc.Timestamp

		streamCtx context.Context
		cancel    context.CancelFunc
		canceled  bool

		// TODO(ajwerner): Add some more bookkeeping about number of
		// retries, number of messages, etc.
	}
}

func (f *partialRangeFeed) getTS() hlc.Timestamp {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.mu.ts
}

func (f *partialRangeFeed) getCtx() context.Context {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.mu.streamCtx
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
	rangeInfo *partialRangeFeed,
	withDiff bool,
	rangeCh chan<- singleRangeInfo,
	eventCh chan<- *roachpb.RangeFeedEvent,
) error {
	// Bound the partial rangefeed to the partial span.
	span := rangeInfo.rs.AsRawSpanWithNoLocals()

	// Start a retry loop for sending the batch to the range.
	for r := retry.StartWithCtx(ctx, ds.rpcRetryOptions); r.Next(); {
		// If we've cleared the descriptor on a send failure, re-lookup.
		if !rangeInfo.token.Valid() {
			var err error
			ri, err := ds.getRoutingInfo(ctx, rangeInfo.rs.Key, rangecache.EvictionToken{}, false)
			if err != nil {
				log.VErrEventf(ctx, 1, "range descriptor re-lookup failed: %s", err)
				if !rangecache.IsRangeLookupErrorRetryable(err) {
					return err
				}
				continue
			}
			rangeInfo.token = ri
		}

		// Establish a RangeFeed for a single Range.
		err := ds.singleRangeFeed(rangeInfo.getCtx(), rangeInfo, withDiff, eventCh)

		if err != nil {
			if log.V(1) {
				log.Infof(ctx, "RangeFeed %s disconnected with last checkpoint %s ago: %v",
					span, timeutil.Since(rangeInfo.getTS().GoTime()), err)
			}
			if rangeInfo.getCtx().Err() != nil && ctx.Err() == nil {
				func() {
					rangeInfo.mu.Lock()
					defer rangeInfo.mu.Unlock()
					rangeInfo.mu.cancel() // just in case
					rangeInfo.mu.streamCtx, rangeInfo.mu.cancel = context.WithCancel(ctx)
					rangeInfo.mu.canceled = false
				}()
				continue
			}
			switch {
			case errors.HasType(err, (*roachpb.StoreNotFoundError)(nil)) ||
				errors.HasType(err, (*roachpb.NodeUnavailableError)(nil)):
				// These errors are likely to be unique to the replica that
				// reported them, so no action is required before the next
				// retry.
			case IsSendError(err), errors.HasType(err, (*roachpb.RangeNotFoundError)(nil)):
				// Evict the descriptor from the cache and reload on next attempt.
				rangeInfo.token.Evict(ctx)
				rangeInfo.token = rangecache.EvictionToken{}
				continue
			case errors.HasType(err, (*roachpb.RangeKeyMismatchError)(nil)):
				// Evict the descriptor from the cache.
				rangeInfo.token.Evict(ctx)
				return ds.divideAndSendRangeFeedToRanges(ctx, rangeInfo.rs, rangeInfo.getTS(), rangeCh)
			case errors.HasType(err, (*roachpb.RangeFeedRetryError)(nil)):
				var t *roachpb.RangeFeedRetryError
				if ok := errors.As(err, &t); !ok {
					panic(errors.AssertionFailedf("wrong error type: %T", err))
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
					roachpb.RangeFeedRetryError_REASON_RANGE_MERGED:
					// Evict the decriptor from the cache.
					rangeInfo.token.Evict(ctx)
					return ds.divideAndSendRangeFeedToRanges(ctx, rangeInfo.rs, rangeInfo.getTS(), rangeCh)
				default:
					log.Fatalf(ctx, "unexpected RangeFeedRetryError reason %v", t.Reason)
				}
			default:
				return err
			}
		}
	}
	return ctx.Err()
}

// singleRangeFeed gathers and rearranges the replicas, and makes a RangeFeed
// RPC call. Results will be send on the provided channel. Returns the timestamp
// of the maximum rangefeed checkpoint seen, which can be used to re-establish
// the rangefeed with a larger starting timestamp, reflecting the fact that all
// values up to the last checkpoint have already been observed. Returns the
// request's timestamp if not checkpoints are seen.
func (ds *DistSender) singleRangeFeed(
	ctx context.Context, prf *partialRangeFeed, withDiff bool, eventCh chan<- *roachpb.RangeFeedEvent,
) error {
	args := roachpb.RangeFeedRequest{
		Span: prf.rs.AsRawSpanWithNoLocals(),
		Header: roachpb.Header{
			Timestamp: prf.getTS(),
			RangeID:   prf.token.Desc().RangeID,
		},
		WithDiff: withDiff,
	}
	forward := func(ts hlc.Timestamp) {
		args.Timestamp.Forward(ts)
		prf.mu.Lock()
		defer prf.mu.Unlock()
		prf.mu.ts = args.Timestamp
	}

	var latencyFn LatencyFunc
	if ds.rpcContext != nil {
		latencyFn = ds.rpcContext.RemoteClocks.Latency
	}
	replicas, err := NewReplicaSlice(ctx, ds.nodeDescs, prf.token.Desc(), nil, AllExtantReplicas)
	if err != nil {
		return err
	}
	replicas.OptimizeReplicaOrder(ds.getNodeDescriptor(), latencyFn)
	// The RangeFeed is not used for system critical traffic so use a DefaultClass
	// connection regardless of the range.
	opts := SendOptions{class: rpc.DefaultClass}
	transport, err := ds.transportFactory(opts, ds.nodeDialer, replicas)
	if err != nil {
		return err
	}
	defer transport.Release()

	for {
		if transport.IsExhausted() {
			return newSendError(
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
				return err
			}
			continue
		}
		for {
			event, err := stream.Recv()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			switch t := event.GetValue().(type) {
			case *roachpb.RangeFeedCheckpoint:
				if t.Span.Contains(args.Span) {
					forward(t.ResolvedTS)
				}
			case *roachpb.RangeFeedError:
				log.VErrEventf(ctx, 2, "RangeFeedError: %s", t.Error.GoError())
				return t.Error.GoError()
			}
			select {
			case eventCh <- event:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}
