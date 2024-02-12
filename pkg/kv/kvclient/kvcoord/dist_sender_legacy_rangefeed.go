package kvcoord

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangecache"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/pprofutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"math"
	"time"
)

// TODO(erikgrinaker): this entire file should be removed when support for the
// legacy RangeFeed protocol is no longer needed in mixed-version clusters, and
// we don't need test coverage for it.

type singleRangeInfo struct {
	rs         roachpb.RSpan
	startAfter hlc.Timestamp
	token      rangecache.EvictionToken
}

// WithoutMuxRangeFeed configures range feed to use legacy RangeFeed RPC.
func WithoutMuxRangeFeed() RangeFeedOption {
	return optionFunc(func(c *rangeFeedConfig) {
		c.disableMuxRangeFeed = true
	})
}

func legacyRangeFeed(
	ctx context.Context,
	cfg rangeFeedConfig,
	spans []SpanTimePair,
	ds *DistSender,
	rr *rangeFeedRegistry,
	eventCh chan<- RangeFeedMessage,
) error {
	rl := newCatchupScanRateLimiter(&ds.st.SV)

	metrics := &ds.metrics.DistSenderRangeFeedMetrics
	if cfg.knobs.metrics != nil {
		metrics = cfg.knobs.metrics
	}

	// Goroutine that processes subdivided ranges and creates a rangefeed for
	// each.
	g := ctxgroup.WithContext(ctx)
	rangeCh := make(chan singleRangeInfo, 16)
	g.GoCtx(func(ctx context.Context) error {
		for {
			select {
			case sri := <-rangeCh:
				// Bound the partial rangefeed to the partial span.
				span := sri.rs.AsRawSpanWithNoLocals()

				// Register partial range feed with registry.  We do this prior to acquiring
				// catchup scan quota so that we have some observability into the ranges
				// that are blocked, waiting for quota.
				active := rr.newActiveRangeFeed(span, sri.startAfter, metrics)

				acquireStart := timeutil.Now()
				if log.V(1) {
					log.Infof(ctx, "RangeFeed starting for span %s@%s (quota acquisition)", span, sri.startAfter)
				}
				// Prior to spawning goroutine to process this feed, acquire catchup scan quota.
				// This quota acquisition paces the rate of new goroutine creation.
				if err := active.acquireCatchupScanQuota(ctx, rl, metrics); err != nil {
					return err
				}
				if log.V(1) {
					log.Infof(ctx, "RangeFeed starting for span %s@%s (quota acquired in %s)",
						span, sri.startAfter, timeutil.Since(acquireStart))
				}

				// Spawn a child goroutine to process this feed.
				g.GoCtx(func(ctx context.Context) error {
					defer active.release()

					return ds.partialRangeFeed(ctx, active, sri.rs, sri.startAfter,
						sri.token, rangeCh, eventCh, cfg, metrics)
				})
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})

	// Kick off the initial set of ranges.
	g.GoCtx(func(ctx context.Context) error {
		return divideAllSpansOnRangeBoundaries(ctx, spans, sendSingleRangeInfo(rangeCh), ds)
	})

	return g.Wait()
}

// partialRangeFeed establishes a RangeFeed to the range specified the routing token.
// This method manages lifecycle events of the range in order to maintain the RangeFeed
// connection; this may involve instructing higher-level functions to retry
// this rangefeed, or subdividing the range further in the event of a split.
func (ds *DistSender) partialRangeFeed(
	ctx context.Context,
	active *activeRangeFeed,
	rs roachpb.RSpan,
	startAfter hlc.Timestamp,
	token rangecache.EvictionToken,
	rangeCh chan<- singleRangeInfo,
	eventCh chan<- RangeFeedMessage,
	cfg rangeFeedConfig,
	metrics *DistSenderRangeFeedMetrics,
) error {
	// Bound the partial rangefeed to the partial span.
	span := rs.AsRawSpanWithNoLocals()

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
		if log.V(1) {
			log.Infof(ctx, "RangeFeed starting for range %d@%s (%s)", token.Desc().RangeID, startAfter, span)
		}

		maxTS, err := ds.singleRangeFeed(ctx, active, span, startAfter, token.Desc(), eventCh, cfg, metrics)

		// Forward the timestamp in case we end up sending it again.
		startAfter.Forward(maxTS)

		if log.V(1) {
			log.Infof(ctx, "RangeFeed %d@%s (%s) disconnected with last checkpoint %s ago: %v",
				token.Desc().RangeID, active.StartAfter, active.Span, timeutil.Since(active.Resolved.GoTime()), err)
		}
		active.setLastError(err)

		errInfo, err := handleRangefeedError(ctx, metrics, err)
		if err != nil {
			return err
		}
		if errInfo.evict {
			token.Evict(ctx)
			token = rangecache.EvictionToken{}
		}

		if errInfo.resolveSpan {
			// We must release catchup scan reservation prior to attempt to
			// re-resolve since this will attempt to acquire 1 or more catchup
			// scan reservations.
			active.releaseCatchupScan()
			return divideSpanOnRangeBoundaries(ctx, ds, rs, startAfter, sendSingleRangeInfo(rangeCh))
		}
	}
	return ctx.Err()
}

func sendSingleRangeInfo(rangeCh chan<- singleRangeInfo) onRangeFn {
	return func(ctx context.Context, rs roachpb.RSpan, startAfter hlc.Timestamp, token rangecache.EvictionToken) error {
		select {
		case rangeCh <- singleRangeInfo{
			rs:         rs,
			startAfter: startAfter,
			token:      token,
		}:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// singleRangeFeed gathers and rearranges the replicas, and makes a RangeFeed
// RPC call. Results will be sent on the provided channel. Returns the timestamp
// of the maximum rangefeed checkpoint seen, which can be used to re-establish
// the rangefeed with a larger starting timestamp, reflecting the fact that all
// values up to the last checkpoint have already been observed. Returns the
// request's timestamp if not checkpoints are seen.
func (ds *DistSender) singleRangeFeed(
	ctx context.Context,
	active *activeRangeFeed,
	span roachpb.Span,
	startAfter hlc.Timestamp,
	desc *roachpb.RangeDescriptor,
	eventCh chan<- RangeFeedMessage,
	cfg rangeFeedConfig,
	metrics *DistSenderRangeFeedMetrics,
) (_ hlc.Timestamp, retErr error) {
	// Ensure context is cancelled on all errors, to prevent gRPC stream leaks.
	ctx, cancelFeed := context.WithCancel(ctx)
	defer func() {
		if log.V(1) {
			log.Infof(ctx, "singleRangeFeed terminating with err=%v", retErr)
		}
		cancelFeed()
	}()

	args := makeRangeFeedRequest(span, desc.RangeID, cfg.overSystemTable, startAfter, cfg.withDiff, cfg.withFiltering)
	transport, err := newTransportForRange(ctx, desc, ds)
	if err != nil {
		return args.Timestamp, err
	}
	defer transport.Release()

	stuckWatcher := newStuckRangeFeedCanceler(cancelFeed, defaultStuckRangeThreshold(ds.st))
	defer stuckWatcher.stop()

	var streamCleanup func()
	maybeCleanupStream := func() {
		if streamCleanup != nil {
			streamCleanup()
			streamCleanup = nil
		}
	}
	defer maybeCleanupStream()

	for {
		stuckWatcher.stop() // if timer is running from previous iteration, stop it now
		if transport.IsExhausted() {
			return args.Timestamp, newSendError(errors.New("sending to all replicas failed"))
		}
		maybeCleanupStream()

		args.Replica = transport.NextReplica()
		client, err := transport.NextInternalClient(ctx)
		if err != nil {
			log.VErrEventf(ctx, 2, "RPC error: %s", err)
			continue
		}

		log.VEventf(ctx, 3, "attempting to create a RangeFeed over replica %s", args.Replica)

		ctx := ds.AnnotateCtx(ctx)
		ctx = logtags.AddTag(ctx, "dest_n", args.Replica.NodeID)
		ctx = logtags.AddTag(ctx, "dest_s", args.Replica.StoreID)
		ctx = logtags.AddTag(ctx, "dest_r", args.RangeID)
		ctx, restore := pprofutil.SetProfilerLabelsFromCtxTags(ctx)
		streamCleanup = restore

		active.onConnect(client, metrics)
		stream, err := client.RangeFeed(ctx, &args)
		if err != nil {
			restore()
			log.VErrEventf(ctx, 2, "RPC error: %s", err)
			if grpcutil.IsAuthError(err) {
				// Authentication or authorization error. Propagate.
				return args.Timestamp, err
			}
			continue
		}

		var event *kvpb.RangeFeedEvent
		for {
			if err := stuckWatcher.do(func() (err error) {
				event, err = stream.Recv()
				return err
			}); err != nil {
				log.VErrEventf(ctx, 2, "RPC error: %s", err)
				if stuckWatcher.stuck() {
					afterCatchUpScan := active.catchupRes == nil
					return args.Timestamp, handleStuckEvent(&args, afterCatchUpScan, stuckWatcher.threshold(), metrics)
				}
				return args.Timestamp, err
			}

			if cfg.knobs.onRangefeedEvent != nil {
				skip, err := cfg.knobs.onRangefeedEvent(ctx, span, 0 /*streamID */, event)
				if err != nil {
					return args.Timestamp, err
				}
				if skip {
					continue
				}
			}

			msg := RangeFeedMessage{RangeFeedEvent: event, RegisteredSpan: span}
			switch t := event.GetValue().(type) {
			case *kvpb.RangeFeedCheckpoint:
				if t.Span.Contains(args.Span) {
					// If we see the first checkpoint, we know we're done with the catchup scan.
					if active.catchupRes != nil {
						active.releaseCatchupScan()
					}
					// Note that this timestamp means that all rows in the span with
					// writes at or before the timestamp have now been seen. The
					// Timestamp field in the request is exclusive, meaning if we send
					// the request with exactly the ResolveTS, we'll see only rows after
					// that timestamp.
					args.Timestamp.Forward(t.ResolvedTS)
				}
			case *kvpb.RangeFeedError:
				log.VErrEventf(ctx, 2, "RangeFeedError: %s", t.Error.GoError())
				if active.catchupRes != nil {
					metrics.Errors.RangefeedErrorCatchup.Inc(1)
				}
				if stuckWatcher.stuck() {
					// When the stuck watcher fired, and the rangefeed call is local,
					// the remote might notice the cancellation first and return from
					// Recv with an error that we need to special-case here.
					afterCatchUpScan := active.catchupRes == nil
					return args.Timestamp, handleStuckEvent(&args, afterCatchUpScan, stuckWatcher.threshold(), metrics)
				}
				return args.Timestamp, t.Error.GoError()
			}
			active.onRangeEvent(args.Replica.NodeID, desc.RangeID, event)

			select {
			case eventCh <- msg:
			case <-ctx.Done():
				return args.Timestamp, ctx.Err()
			}
		}
	}
}

func handleStuckEvent(
	args *kvpb.RangeFeedRequest,
	afterCatchupScan bool,
	threshold time.Duration,
	m *DistSenderRangeFeedMetrics,
) error {
	if afterCatchupScan {
		telemetry.Count("rangefeed.stuck.after-catchup-scan")
	} else {
		telemetry.Count("rangefeed.stuck.during-catchup-scan")
	}
	return errors.Wrapf(errRestartStuckRange, "waiting for r%d %s [threshold %s]", args.RangeID, args.Replica, threshold)
}

func defaultStuckRangeThreshold(st *cluster.Settings) func() time.Duration {
	return func() time.Duration {
		// Before the introduction of kv.rangefeed.range_stuck_threshold = 1m,
		// clusters may already have kv.closed_timestamp.side_transport_interval or
		// kv.rangefeed.closed_timestamp_refresh_interval set to >1m. This would
		// cause rangefeeds to continually restart. We therefore conservatively use
		// the highest value, with a 1.2 safety factor.
		threshold := rangefeedRangeStuckThreshold.Get(&st.SV)
		if threshold > 0 {
			interval := kvserverbase.RangeFeedRefreshInterval.Get(&st.SV)
			if i := closedts.SideTransportCloseInterval.Get(&st.SV); i > interval {
				interval = i
			}
			if t := time.Duration(math.Round(1.2 * float64(interval))); t > threshold {
				threshold = t
			}
		}
		return threshold
	}
}
