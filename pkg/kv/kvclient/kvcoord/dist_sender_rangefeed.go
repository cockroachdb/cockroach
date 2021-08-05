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
	"github.com/cockroachdb/cockroach/pkg/settings"
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
	ts    syncTimestamp
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

	wd := newRangeFeedWatchdog(ds)
	defer wd.stop()

	g := ctxgroup.WithContext(ctx)
	// Goroutine that processes subdivided ranges and creates a rangefeed for
	// each.
	rangeCh := make(chan *singleRangeInfo, 16)
	g.GoCtx(func(ctx context.Context) error {
		for {
			select {
			case sri := <-rangeCh:
				// Spawn a child goroutine to process this feed.
				g.GoCtx(func(ctx context.Context) error {
					return wd.partialRangeFeedWithLivenessRestart(ctx, sri, withDiff, rangeCh, eventCh)
				})
			case <-wd.C:
				wd.restartSlowRanges()
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

var enableRangefeedWatchdog = settings.RegisterBoolSetting(
	"kv.dist_sender.enable_rangefeed_watchdog",
	"when set, starts a heart beat watcher for rangefeed, and restarts rangefeed if necessary",
	false,
)
var restartRangefeedThreshold = settings.RegisterDurationSetting(
	"kv.dist_sender.rangefeed_watchdog_timeout",
	"restart rangefeed if the range does not publish closed timestamp for "+
		"longer than this threshold; 0 selects a reasonable default",
	0,
	settings.NonNegativeDuration,
)
var watchdogPace = settings.RegisterDurationSetting(
	"kv.dist_sender.rangefeed_watchdog_pace",
	"how often the set of all rangefeed is checked for liveness",
	10*time.Minute,
	func(d time.Duration) error {
		if d < 5*time.Second {
			return errors.Newf("must be at least 5 seconds")
		}
		return nil
	},
)

// rangeFeeedWatchdog is responsible for watching over the health of the
// rangefeed ranges.
type rangeFeedWatchdog struct {
	ds *DistSender

	// C is the time channel pacing the frequency of
	// watchdog operation.
	// It is left nil if the watchdog is disabled.
	C <-chan time.Time

	// All members below are initialized iff C != nil.

	ticker     *timeutil.Timer
	tickerPace time.Duration
	// Map of ranges (singleRangeInfo* -> context.CancelFunc) watched by this watchdog.
	// TODO(yevgeniy): Expose iterating the partialRangeFeed structures
	//  through here and then hook up iterating these things off of the
	//  DistSender in a crdb_internal virtual table.
	ranges sync.Map
}

func newRangeFeedWatchdog(ds *DistSender) *rangeFeedWatchdog {
	wd := &rangeFeedWatchdog{ds: ds}
	if enableRangefeedWatchdog.Get(&ds.st.SV) {
		wd.init()
	}
	return wd
}

func (wd *rangeFeedWatchdog) init() {
	wd.ticker = timeutil.NewTimer()
	wd.tickerPace = watchdogPace.Get(&wd.ds.st.SV)
	wd.ticker.Reset(wd.tickerPace)
	wd.C = wd.ticker.C
}

func (wd *rangeFeedWatchdog) stop() {
	if wd.ticker != nil {
		wd.ticker.Stop()
	}
}

func (wd *rangeFeedWatchdog) restartThreshold() time.Duration {
	threshold := restartRangefeedThreshold.Get(&wd.ds.st.SV)
	if threshold == 0 {
		// TODO(yevgeniy): Perhaps we can default to a function of closedts.SideTransportCloseInterval
		return 10 * time.Minute
	}
	return threshold
}

// restartSlowRanges is responsible for restarting rangescans for ranges from
// which we have not heard any updates in a long time.
// Note: Restarts as performed by this method paper over the underlying problem, and
// a potential bug in a system.  However, this seems to be more of a defensive mechanism
// to guard against potential issues introduced downstream (e.g. KV, or closed time stamp, etc).
// TODO(yevgeniy): Perhaps we can remove this hack at some point.
func (wd *rangeFeedWatchdog) restartSlowRanges() {
	wd.ticker.Read = true
	if newPace := watchdogPace.Get(&wd.ds.st.SV); newPace != wd.tickerPace {
		wd.ticker.Reset(newPace)
	}

	tooLong := wd.restartThreshold()
	wd.ranges.Range(func(k, v interface{}) (continueMatch bool) {
		if timeutil.Since(k.(*singleRangeInfo).ts.Timestamp().GoTime()) > tooLong {
			v.(context.CancelFunc)()
			wd.ds.metrics.SlowRangeFeedRanges.Inc(1)
		}
		return true
	})
}

// partialRangeFeedWithLivenessRestart executes partial rangefeed.
// If the watchdog enabled, arranges for cancellation context to be set so that
// restartSlowRanges can cancel range feeds that appear to be stuck.
func (wd *rangeFeedWatchdog) partialRangeFeedWithLivenessRestart(
	ctx context.Context,
	rangeInfo *singleRangeInfo,
	withDiff bool,
	rangeCh chan<- *singleRangeInfo,
	eventCh chan<- *roachpb.RangeFeedEvent,
) error {
	if wd.C == nil {
		// Watchdog disabled
		return wd.ds.partialRangeFeed(ctx, rangeInfo, withDiff, rangeCh, eventCh)
	}

	streamCtx, cancelStream := context.WithCancel(ctx)
	defer func() {
		cancelStream()
		wd.ranges.Delete(rangeInfo)
	}()

	for {
		wd.ranges.Store(rangeInfo, cancelStream)

		err := wd.ds.partialRangeFeed(streamCtx, rangeInfo, withDiff, rangeCh, eventCh)
		if err == nil {
			return nil
		}

		wasStuck := streamCtx.Err() != nil && ctx.Err() == nil
		if !wasStuck {
			return err
		}
		// Cancel previous stream context, and reset stream context for the next attempt.
		cancelStream()
		streamCtx, cancelStream = context.WithCancel(ctx)
	}
}

func newSingleRangeInfo(
	rs roachpb.RSpan, ts hlc.Timestamp, token rangecache.EvictionToken,
) *singleRangeInfo {
	sri := &singleRangeInfo{
		rs:    rs,
		token: token,
		ts:    syncTimestamp{ts: ts},
	}
	return sri
}

func (ds *DistSender) divideAndSendRangeFeedToRanges(
	ctx context.Context, rs roachpb.RSpan, ts hlc.Timestamp, rangeCh chan<- *singleRangeInfo,
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
		case rangeCh <- newSingleRangeInfo(partialRS, ts, ri.Token()):
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
	rangeInfo *singleRangeInfo,
	withDiff bool,
	rangeCh chan<- *singleRangeInfo,
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
		maxTS, err := ds.singleRangeFeed(ctx, span, &rangeInfo.ts, withDiff, rangeInfo.token.Desc(), eventCh)

		// Forward the timestamp in case we end up sending it again.
		rangeInfo.ts.Forward(maxTS)

		if err != nil {
			if log.V(1) {
				log.Infof(ctx, "RangeFeed %s disconnected with last checkpoint %s ago: %v",
					span, timeutil.Since(rangeInfo.ts.Timestamp().GoTime()), err)
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
				return ds.divideAndSendRangeFeedToRanges(ctx, rangeInfo.rs, rangeInfo.ts.Timestamp(), rangeCh)
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
					rangeInfo.token.Evict(ctx)
					return ds.divideAndSendRangeFeedToRanges(ctx, rangeInfo.rs, rangeInfo.ts.Timestamp(), rangeCh)
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

// singleRangeFeed gathers and rearranges the replicas, and makes a RangeFeed
// RPC call. Results will be sent on the provided channel. Returns the timestamp
// of the maximum rangefeed checkpoint seen, which can be used to re-establish
// the rangefeed with a larger starting timestamp, reflecting the fact that all
// values up to the last checkpoint have already been observed. Returns the
// request's timestamp if not checkpoints are seen.
func (ds *DistSender) singleRangeFeed(
	ctx context.Context,
	span roachpb.Span,
	ts *syncTimestamp,
	withDiff bool,
	desc *roachpb.RangeDescriptor,
	eventCh chan<- *roachpb.RangeFeedEvent,
) (hlc.Timestamp, error) {
	args := roachpb.RangeFeedRequest{
		Span: span,
		Header: roachpb.Header{
			Timestamp: ts.Timestamp(),
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
					ts.Forward(t.ResolvedTS)
				}
			case *roachpb.RangeFeedError:
				log.VErrEventf(ctx, 2, "RangeFeedError: %s", t.Error.GoError())
				return args.Timestamp, t.Error.GoError()
			}
			select {
			case eventCh <- event:
			case <-ctx.Done():
				return args.Timestamp, ctx.Err()
			}
		}
	}
}

type syncTimestamp struct {
	syncutil.Mutex
	ts hlc.Timestamp
}

func (t *syncTimestamp) Timestamp() hlc.Timestamp {
	t.Lock()
	defer t.Unlock()
	return t.ts
}

func (t *syncTimestamp) Forward(ts hlc.Timestamp) {
	t.Lock()
	defer t.Unlock()
	t.ts.Forward(ts)
}
