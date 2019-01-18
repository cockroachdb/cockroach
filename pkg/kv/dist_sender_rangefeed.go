// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package kv

import (
	"context"
	"fmt"
	"io"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

const maxConcurrentIterators = 64

type singleRangeInfo struct {
	desc  *roachpb.RangeDescriptor
	rs    roachpb.RSpan
	token *EvictionToken
}

// RangeFeed divides a RangeFeed request on range boundaries and establishes a
// RangeFeed to each of the individual ranges. It streams back results on the
// provided channel.
func (ds *DistSender) RangeFeed(
	ctx context.Context, args *roachpb.RangeFeedRequest, eventCh chan<- *roachpb.RangeFeedEvent,
) *roachpb.Error {
	ctx = ds.AnnotateCtx(ctx)
	ctx, sp := tracing.EnsureChildSpan(ctx, ds.AmbientContext.Tracer, "dist sender")
	defer sp.Finish()

	startRKey, err := keys.Addr(args.Span.Key)
	if err != nil {
		return roachpb.NewError(err)
	}
	endRKey, err := keys.Addr(args.Span.EndKey)
	if err != nil {
		return roachpb.NewError(err)
	}
	rs := roachpb.RSpan{Key: startRKey, EndKey: endRKey}

	// Iterator limiter - when starting a single rangefeed, it initially undergoes
	// a "catchup scan" which requires an engine iterator, which is significantly
	// more expensive than a feed sending events directly from raft. Therefore, we
	// use a sempahore to limit the number of singleRangeFeed streams that can be
	// using an iterator at the same time.
	rangeCh := make(chan singleRangeInfo, 16)
	var limitIteratorSem chan struct{}
	if !args.Timestamp.IsEmpty() {
		// Only rangefeeds which specify a start timestamp use a catchup iterator.
		limitIteratorSem = make(chan struct{}, maxConcurrentIterators)
	}

	g := ctxgroup.WithContext(ctx)
	// Goroutine that processes subdivided ranges and creates a changefeed for
	// each, while using the semaphore to limiting the number of rangefeeds that
	// are simultaneously using a catchup iterator.
	g.GoCtx(func(ctx context.Context) error {
		for {
			select {
			case sri := <-rangeCh:
				// Semaphore management. If a semaphore is being used to limit
				// iterators, require each range received to wait for a sempahore slot.
				// To make downlevel functions easier, a "releaseSem" function is always
				// provided, although it is a no-op if no semaphore is being used.
				semTaken := false
				releaseSem := func() {
					if semTaken {
						<-limitIteratorSem
						semTaken = false
					}
				}
				if limitIteratorSem != nil {
					select {
					case limitIteratorSem <- struct{}{}:
					case <-ctx.Done():
						return ctx.Err()
					}
					semTaken = true
				}
				// Spawn a child goroutine to process this feed. If the feed returns as
				// retryable, place the range back in the range channel so that it will
				// be picked up again; this is required because it may need to use
				// another catchup iterator when reinstantiated.
				g.GoCtx(func(ctx context.Context) error {
					err, retry := ds.partialRangeFeed(ctx, *args, &sri, rangeCh, eventCh, releaseSem)
					if retry {
						select {
						case rangeCh <- sri:
							return nil
						case <-ctx.Done():
							return ctx.Err()
						}
					}
					return err
				})
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})

	// Kick off the initial round of spans.
	g.GoCtx(func(ctx context.Context) error {
		return ds.divideAndSendRangeFeedToRanges(ctx, args, rs, rangeCh)
	})
	return roachpb.NewError(g.Wait())
}

func (ds *DistSender) divideAndSendRangeFeedToRanges(
	ctx context.Context,
	args *roachpb.RangeFeedRequest,
	rs roachpb.RSpan,
	rangeCh chan<- singleRangeInfo,
) error {
	ri := NewRangeIterator(ds)
	for ri.Seek(ctx, rs.Key, Ascending); ri.Valid(); ri.Next(ctx) {
		desc := ri.Desc()
		partialRS, err := rs.Intersect(desc)
		if err != nil {
			return err
		}
		rangeCh <- singleRangeInfo{
			desc:  desc,
			rs:    partialRS,
			token: ri.Token(),
		}
		if !ri.NeedAnother(rs) {
			break
		}
	}
	return ri.Error().GoError()
}

// partialRangeFeed establishes a RangeFeed to the range specified by desc. It
// manages lifecycle events of the range in order to maintain the RangeFeed
// connection; this may involve instructing higher-level functions to retry
// this rangefeed, or subdividing the range further in the event of a split.
func (ds *DistSender) partialRangeFeed(
	ctx context.Context,
	argsCopy roachpb.RangeFeedRequest,
	rangeInfo *singleRangeInfo,
	rangeCh chan<- singleRangeInfo,
	eventCh chan<- *roachpb.RangeFeedEvent,
	releaseSem func(),
) (error, bool) {
	defer releaseSem()

	// Bound the partial rangefeed to the partial span.
	argsCopy.Span = rangeInfo.rs.AsRawSpanWithNoLocals()

	// If we've cleared the descriptor on a send failure, re-lookup.
	if rangeInfo.desc == nil {
		var err error
		rangeInfo.desc, rangeInfo.token, err = ds.getDescriptor(ctx, rangeInfo.rs.Key, nil, false)
		if err != nil {
			log.VErrEventf(ctx, 1, "range descriptor re-lookup failed: %s", err)
			return nil, true
		}
	}

	// Establish a RangeFeed for a single Range.
	maxTS, pErr := ds.singleRangeFeed(ctx, argsCopy, rangeInfo.desc, eventCh, releaseSem)

	// Forward the timestamp of the request in case we end up sending it
	// again.
	argsCopy.Timestamp.Forward(maxTS)

	if pErr != nil {
		switch t := pErr.GetDetail().(type) {
		case *roachpb.SendError, *roachpb.RangeNotFoundError:
			// Evict the decriptor from the cache and reload on next attempt.
			if err := rangeInfo.token.Evict(ctx); err != nil {
				return err, false
			}
			rangeInfo.desc = nil
			return nil, true
		case *roachpb.RangeKeyMismatchError:
			// Evict the decriptor from the cache.
			if err := rangeInfo.token.Evict(ctx); err != nil {
				return err, false
			}
			releaseSem()
			return ds.divideAndSendRangeFeedToRanges(ctx, &argsCopy, rangeInfo.rs, rangeCh), false
		case *roachpb.RangeFeedRetryError:
			switch t.Reason {
			case roachpb.RangeFeedRetryError_REASON_REPLICA_REMOVED,
				roachpb.RangeFeedRetryError_REASON_RAFT_SNAPSHOT,
				roachpb.RangeFeedRetryError_REASON_LOGICAL_OPS_MISSING:
				// Try again with same descriptor. These are transient
				// errors that should not show up again.
				return nil, true
			case roachpb.RangeFeedRetryError_REASON_RANGE_SPLIT,
				roachpb.RangeFeedRetryError_REASON_RANGE_MERGED:
				// TODO: Looks like this might duplicate a rangefeed in the event of a
				// merge, which seems like a rare event but may be worth
				// investigating.

				// Evict the decriptor from the cache.
				if err := rangeInfo.token.Evict(ctx); err != nil {
					return err, false
				}
				releaseSem()
				return ds.divideAndSendRangeFeedToRanges(ctx, &argsCopy, rangeInfo.rs, rangeCh), false
			case roachpb.RangeFeedRetryError_REASON_SLOW_CONSUMER:
				return nil, true
			default:
				log.Fatalf(ctx, "unexpected RangeFeedRetryError reason %v", t.Reason)
			}
		default:
			return t, false
		}
	}
	return nil, true
}

// singleRangeFeed gathers and rearranges the replicas, and makes a RangeFeed
// RPC call. Results will be send on the provided channel. Returns the timestamp
// of the maximum rangefeed checkpoint seen, which can be used to re-establish
// the rangefeed with a larger starting timestamp, reflecting the fact that all
// values up to the last checkpoint have already been observed. Returns the
// request's timestamp if not checkpoints are seen.
func (ds *DistSender) singleRangeFeed(
	ctx context.Context,
	argsCopy roachpb.RangeFeedRequest,
	desc *roachpb.RangeDescriptor,
	eventCh chan<- *roachpb.RangeFeedEvent,
	releaseSem func(),
) (hlc.Timestamp, *roachpb.Error) {
	// Direct the rangefeed to the specified Range.
	argsCopy.RangeID = desc.RangeID

	var latencyFn LatencyFunc
	if ds.rpcContext != nil {
		latencyFn = ds.rpcContext.RemoteClocks.Latency
	}
	replicas := NewReplicaSlice(ds.gossip, desc)
	replicas.OptimizeReplicaOrder(ds.getNodeDescriptor(), latencyFn)

	transport, err := ds.transportFactory(SendOptions{}, ds.nodeDialer, replicas)
	if err != nil {
		return argsCopy.Timestamp, roachpb.NewError(err)
	}

	for {
		if transport.IsExhausted() {
			return argsCopy.Timestamp, roachpb.NewError(roachpb.NewSendError(
				fmt.Sprintf("sending to all %d replicas failed", len(replicas)),
			))
		}

		argsCopy.Replica = transport.NextReplica()
		clientCtx, client, err := transport.NextInternalClient(ctx)
		if err != nil {
			log.VErrEventf(ctx, 2, "RPC error: %s", err)
			continue
		}

		stream, err := client.RangeFeed(clientCtx, &argsCopy)
		if err != nil {
			log.VErrEventf(ctx, 2, "RPC error: %s", err)
			continue
		}
		for {
			event, err := stream.Recv()
			if err == io.EOF {
				return argsCopy.Timestamp, nil
			}
			if err != nil {
				return argsCopy.Timestamp, roachpb.NewError(err)
			}
			switch t := event.GetValue().(type) {
			case *roachpb.RangeFeedCheckpoint:
				if t.Span.Contains(argsCopy.Span) {
					argsCopy.Timestamp.Forward(t.ResolvedTS)
				}
				// Checkpoint reached means that we can release our iterator semaphore
				// if we are holding onto it.
				releaseSem()
			case *roachpb.RangeFeedError:
				log.VErrEventf(ctx, 2, "RangeFeedError: %s", t.Error.GoError())
				return argsCopy.Timestamp, &t.Error
			}
			select {
			case eventCh <- event:
			case <-ctx.Done():
				return argsCopy.Timestamp, roachpb.NewError(ctx.Err())
			}
		}
	}
}
