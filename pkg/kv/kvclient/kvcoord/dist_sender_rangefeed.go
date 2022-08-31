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
	"math"
	"sync"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangecache"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/limit"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/pprofutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
)

type singleRangeInfo struct {
	rs         roachpb.RSpan
	startAfter hlc.Timestamp
	token      rangecache.EvictionToken
}

var useDedicatedRangefeedConnectionClass = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.rangefeed.use_dedicated_connection_class.enabled",
	"uses dedicated connection when running rangefeeds",
	util.ConstantWithMetamorphicTestBool(
		"kv.rangefeed.use_dedicated_connection_class.enabled", false),
)

var catchupScanConcurrency = settings.RegisterIntSetting(
	settings.TenantWritable,
	"kv.rangefeed.catchup_scan_concurrency",
	"number of catchup scans that a single rangefeed can execute concurrently; 0 implies unlimited",
	8,
	settings.NonNegativeInt,
)

var rangefeedRangeStuckThreshold = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"kv.rangefeed.range_stuck_threshold",
	"restart rangefeeds if they appear to be stuck for the specified threshold; 0 disables",
	time.Minute,
	settings.NonNegativeDuration,
)

func maxConcurrentCatchupScans(sv *settings.Values) int {
	l := catchupScanConcurrency.Get(sv)
	if l == 0 {
		return math.MaxInt
	}
	return int(l)
}

type rangeFeedConfig struct {
	useMuxRangeFeed bool
}

// RangeFeedOption configures a RangeFeed.
type RangeFeedOption interface {
	set(*rangeFeedConfig)
}

type optionFunc func(*rangeFeedConfig)

func (o optionFunc) set(c *rangeFeedConfig) { o(c) }

// WithMuxRangeFeed configures range feed to use MuxRangeFeed RPC.
func WithMuxRangeFeed() RangeFeedOption {
	return optionFunc(func(c *rangeFeedConfig) {
		c.useMuxRangeFeed = true
	})
}

// A "kill switch" to disable multiplexing rangefeed if severe issues discovered with new implementation.
var enableMuxRangeFeed = envutil.EnvOrDefaultBool("COCKROACH_ENABLE_MULTIPLEXING_RANGEFEED", true)

// RangeFeed divides a RangeFeed request on range boundaries and establishes a
// RangeFeed to each of the individual ranges. It streams back results on the
// provided channel.
//
// Note that the timestamps in RangeFeedCheckpoint events that are streamed back
// may be lower than the timestamp given here.
//
// Rangefeeds do not support inline (unversioned) values, and may omit them or
// error on them. Similarly, rangefeeds will error if MVCC history is mutated
// via e.g. ClearRange. Do not use rangefeeds across such key spans.
//
// NB: the given startAfter timestamp is exclusive, i.e. the first possible
// emitted event (including catchup scans) will be at startAfter.Next().
func (ds *DistSender) RangeFeed(
	ctx context.Context,
	spans []roachpb.Span,
	startAfter hlc.Timestamp, // exclusive
	withDiff bool,
	eventCh chan<- RangeFeedMessage,
	opts ...RangeFeedOption,
) error {
	timedSpans := make([]SpanTimePair, 0, len(spans))
	for _, sp := range spans {
		timedSpans = append(timedSpans, SpanTimePair{
			Span:       sp,
			StartAfter: startAfter,
		})
	}
	return ds.RangeFeedSpans(ctx, timedSpans, withDiff, eventCh, opts...)
}

// SpanTimePair is a pair of span along with its starting time. The starting
// time is exclusive, i.e. the first possible emitted event (including catchup
// scans) will be at startAfter.Next().
type SpanTimePair struct {
	Span       roachpb.Span
	StartAfter hlc.Timestamp // exclusive
}

// RangeFeedSpans is similar to RangeFeed but allows specification of different
// starting time for each span.
func (ds *DistSender) RangeFeedSpans(
	ctx context.Context,
	spans []SpanTimePair,
	withDiff bool,
	eventCh chan<- RangeFeedMessage,
	opts ...RangeFeedOption,
) error {
	if len(spans) == 0 {
		return errors.AssertionFailedf("expected at least 1 span, got none")
	}

	var cfg rangeFeedConfig
	for _, opt := range opts {
		opt.set(&cfg)
	}

	ctx = ds.AnnotateCtx(ctx)
	ctx, sp := tracing.EnsureChildSpan(ctx, ds.AmbientContext.Tracer, "dist sender")
	defer sp.Finish()

	rr := newRangeFeedRegistry(ctx, withDiff)
	ds.activeRangeFeeds.Store(rr, nil)
	defer ds.activeRangeFeeds.Delete(rr)

	catchupSem := limit.MakeConcurrentRequestLimiter(
		"distSenderCatchupLimit", maxConcurrentCatchupScans(&ds.st.SV))

	g := ctxgroup.WithContext(ctx)

	var eventProducer rangeFeedEventProducerFactory
	if ds.st.Version.IsActive(ctx, clusterversion.RangefeedUseOneStreamPerNode) &&
		enableMuxRangeFeed && cfg.useMuxRangeFeed {
		m := newRangefeedMuxer(g)
		eventProducer = m.startMuxRangeFeed
	} else {
		eventProducer = legacyRangeFeedEventProducer
	}

	// Goroutine that processes subdivided ranges and creates a rangefeed for
	// each.
	rangeCh := make(chan singleRangeInfo, 16)
	g.GoCtx(func(ctx context.Context) error {
		for {
			select {
			case sri := <-rangeCh:
				// Spawn a child goroutine to process this feed.
				g.GoCtx(func(ctx context.Context) error {
					return ds.partialRangeFeed(ctx, rr, eventProducer, sri.rs, sri.startAfter,
						sri.token, withDiff, &catchupSem, rangeCh, eventCh)
				})
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})

	// Kick off the initial set of ranges.
	for _, s := range spans {
		func(stp SpanTimePair) {
			g.GoCtx(func(ctx context.Context) error {
				rs, err := keys.SpanAddr(stp.Span)
				if err != nil {
					return err
				}
				return ds.divideAndSendRangeFeedToRanges(ctx, rs, stp.StartAfter, rangeCh)
			})
		}(s)
	}

	return g.Wait()
}

// RangeFeedContext is the structure containing arguments passed to
// RangeFeed call.  It functions as a kind of key for an active range feed.
type RangeFeedContext struct {
	ID      int64  // unique ID identifying range feed.
	CtxTags string // context tags

	// WithDiff options passed to RangeFeed call. StartFrom hlc.Timestamp
	WithDiff bool
}

// PartialRangeFeed structure describes the state of currently executing partial range feed.
type PartialRangeFeed struct {
	Span              roachpb.Span
	StartAfter        hlc.Timestamp // exclusive
	NodeID            roachpb.NodeID
	RangeID           roachpb.RangeID
	CreatedTime       time.Time
	LastValueReceived time.Time
	Resolved          hlc.Timestamp
	NumErrs           int
	LastErr           error
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

	return iterutil.Map(iterErr)
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
	if event.Val != nil || event.SST != nil {
		a.LastValueReceived = timeutil.Now()
	} else if event.Checkpoint != nil {
		a.Resolved = event.Checkpoint.ResolvedTS
	}

	a.NodeID = nodeID
	a.RangeID = rangeID
}

func (a *activeRangeFeed) setLastError(err error) {
	now := timeutil.Now()
	a.Lock()
	defer a.Unlock()
	a.LastErr = errors.Wrapf(err, "disconnect at %s: checkpoint %s/-%s",
		redact.SafeString(now.Format(time.RFC3339)), a.Resolved, now.Sub(a.Resolved.GoTime()))
	a.NumErrs++
}

// rangeFeedRegistry is responsible for keeping track of currently executing
// range feeds.
type rangeFeedRegistry struct {
	RangeFeedContext
	ranges sync.Map // map[*activeRangeFeed]nil
}

func newRangeFeedRegistry(ctx context.Context, withDiff bool) *rangeFeedRegistry {
	rr := &rangeFeedRegistry{
		RangeFeedContext: RangeFeedContext{WithDiff: withDiff},
	}
	rr.ID = *(*int64)(unsafe.Pointer(&rr))

	if b := logtags.FromContext(ctx); b != nil {
		rr.CtxTags = b.String()
	}
	return rr
}

func (ds *DistSender) divideAndSendRangeFeedToRanges(
	ctx context.Context, rs roachpb.RSpan, startAfter hlc.Timestamp, rangeCh chan<- singleRangeInfo,
) error {
	// As RangeIterator iterates, it can return overlapping descriptors (and
	// during splits, this happens frequently), but divideAndSendRangeFeedToRanges
	// intends to split up the input into non-overlapping spans aligned to range
	// boundaries. So, as we go, keep track of the remaining uncovered part of
	// `rs` in `nextRS`.
	nextRS := rs
	ri := MakeRangeIterator(ds)
	for ri.Seek(ctx, nextRS.Key, Ascending); ri.Valid(); ri.Next(ctx) {
		desc := ri.Desc()
		partialRS, err := nextRS.Intersect(desc)
		if err != nil {
			return err
		}
		nextRS.Key = partialRS.EndKey
		select {
		case rangeCh <- singleRangeInfo{
			rs:         partialRS,
			startAfter: startAfter,
			token:      ri.Token(),
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
	streamProducerFactory rangeFeedEventProducerFactory,
	rs roachpb.RSpan,
	startAfter hlc.Timestamp,
	token rangecache.EvictionToken,
	withDiff bool,
	catchupSem *limit.ConcurrentRequestLimiter,
	rangeCh chan<- singleRangeInfo,
	eventCh chan<- RangeFeedMessage,
) error {
	// Bound the partial rangefeed to the partial span.
	span := rs.AsRawSpanWithNoLocals()

	// Register partial range feed with registry.
	active := &activeRangeFeed{
		PartialRangeFeed: PartialRangeFeed{
			Span:        span,
			StartAfter:  startAfter,
			CreatedTime: timeutil.Now(),
		},
	}
	rr.ranges.Store(active, nil)
	ds.metrics.RangefeedRanges.Inc(1)
	defer rr.ranges.Delete(active)
	defer ds.metrics.RangefeedRanges.Dec(1)

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
		maxTS, err := ds.singleRangeFeed(
			ctx, span, startAfter, withDiff, token.Desc(),
			catchupSem, eventCh, streamProducerFactory, active.onRangeEvent)

		// Forward the timestamp in case we end up sending it again.
		startAfter.Forward(maxTS)

		if err != nil {
			active.setLastError(err)

			if log.V(1) {
				log.Infof(ctx, "RangeFeed %s disconnected with last checkpoint %s ago: %v",
					span, timeutil.Since(startAfter.GoTime()), err)
			}
			switch {
			case errors.HasType(err, (*roachpb.StoreNotFoundError)(nil)) ||
				errors.HasType(err, (*roachpb.NodeUnavailableError)(nil)):
			// These errors are likely to be unique to the replica that
			// reported them, so no action is required before the next
			// retry.
			case errors.Is(err, errRestartStuckRange):
				// Stuck ranges indicate a bug somewhere in the system.  We are being
				// defensive and attempt to restart this rangefeed. Usually, any
				// stuck-ness is cleared out if we just attempt to re-resolve range
				// descriptor and retry.
				//
				// The error contains the replica which we were waiting for.
				log.Warningf(ctx, "restarting stuck rangefeed: %s", err)
				token.Evict(ctx)
				token = rangecache.EvictionToken{}
				continue
			case IsSendError(err), errors.HasType(err, (*roachpb.RangeNotFoundError)(nil)):
				// Evict the descriptor from the cache and reload on next attempt.
				token.Evict(ctx)
				token = rangecache.EvictionToken{}
				continue
			case errors.HasType(err, (*roachpb.RangeKeyMismatchError)(nil)):
				// Evict the descriptor from the cache.
				token.Evict(ctx)
				return ds.divideAndSendRangeFeedToRanges(ctx, rs, startAfter, rangeCh)
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
					return ds.divideAndSendRangeFeedToRanges(ctx, rs, startAfter, rangeCh)
				default:
					return errors.AssertionFailedf("unrecognized retryable error type: %T", err)
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
	startAfter hlc.Timestamp,
	withDiff bool,
	desc *roachpb.RangeDescriptor,
	catchupSem *limit.ConcurrentRequestLimiter,
	eventCh chan<- RangeFeedMessage,
	streamProducerFactory rangeFeedEventProducerFactory,
	onRangeEvent onRangeEventCb,
) (hlc.Timestamp, error) {
	// Ensure context is cancelled on all errors, to prevent gRPC stream leaks.
	ctx, cancelFeed := context.WithCancel(ctx)
	defer cancelFeed()

	args := roachpb.RangeFeedRequest{
		Span: span,
		Header: roachpb.Header{
			Timestamp: startAfter,
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
	replicas.OptimizeReplicaOrder(ds.getNodeID(), latencyFn, ds.locality)
	opts := SendOptions{class: connectionClass(&ds.st.SV)}
	transport, err := ds.transportFactory(opts, ds.nodeDialer, replicas)
	if err != nil {
		return args.Timestamp, err
	}
	defer transport.Release()

	// Indicate catchup scan is starting;  Before potentially blocking on a semaphore, take
	// opportunity to update semaphore limit.
	catchupSem.SetLimit(maxConcurrentCatchupScans(&ds.st.SV))
	catchupRes, err := catchupSem.Begin(ctx)
	if err != nil {
		return hlc.Timestamp{}, err
	}
	ds.metrics.RangefeedCatchupRanges.Inc(1)
	finishCatchupScan := func() {
		if catchupRes != nil {
			catchupRes.Release()
			ds.metrics.RangefeedCatchupRanges.Dec(1)
			catchupRes = nil
		}
	}
	// cleanup catchup reservation in case of early termination.
	defer finishCatchupScan()

	stuckWatcher := newStuckRangeFeedCanceler(cancelFeed, func() time.Duration {
		return rangefeedRangeStuckThreshold.Get(&ds.st.SV)
	})
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
		if catchupRes == nil {
			// Already finished catch-up scan (in an earlier iteration of this loop),
			// so start timer early, not on first event received.
			stuckWatcher.ping()
		}
		if transport.IsExhausted() {
			return args.Timestamp, newSendError(
				fmt.Sprintf("sending to all %d replicas failed", len(replicas)))
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

		var stream roachpb.RangeFeedEventProducer
		stream, streamCleanup, err = streamProducerFactory(ctx, client, &args)
		if err != nil {
			restore()
			log.VErrEventf(ctx, 2, "RPC error: %s", err)
			if grpcutil.IsAuthError(err) {
				// Authentication or authorization error. Propagate.
				return args.Timestamp, err
			}
			continue
		}
		{
			origStreamCleanup := streamCleanup
			streamCleanup = func() {
				origStreamCleanup()
				restore()
			}
		}

		for {
			event, err := stream.Recv()
			if err == io.EOF {
				return args.Timestamp, nil
			}
			if err != nil {
				if stuckWatcher.stuck() {
					ds.metrics.RangefeedRestartStuck.Inc(1)
					if catchupRes == nil {
						telemetry.Count("rangefeed.stuck.after-catchup-scan")
					} else {
						telemetry.Count("rangefeed.stuck.during-catchup-scan")
					}
					return args.Timestamp, errors.Wrapf(errRestartStuckRange, "waiting for r%d %s [threshold %s]", args.RangeID, args.Replica, stuckWatcher.threshold())
				}
				return args.Timestamp, err
			}
			stuckWatcher.ping() // starts timer on first event only

			msg := RangeFeedMessage{RangeFeedEvent: event, RegisteredSpan: span}
			switch t := event.GetValue().(type) {
			case *roachpb.RangeFeedCheckpoint:
				if t.Span.Contains(args.Span) {
					// If we see the first non-empty checkpoint, we know we're done with the catchup scan.
					if !t.ResolvedTS.IsEmpty() && catchupRes != nil {
						finishCatchupScan()
					}
					args.Timestamp.Forward(t.ResolvedTS.Next())
				}
			case *roachpb.RangeFeedSSTable:
			case *roachpb.RangeFeedError:
				log.VErrEventf(ctx, 2, "RangeFeedError: %s", t.Error.GoError())
				if catchupRes != nil {
					ds.metrics.RangefeedErrorCatchup.Inc(1)
				}
				return args.Timestamp, t.Error.GoError()
			}
			onRangeEvent(args.Replica.NodeID, desc.RangeID, event)

			select {
			case eventCh <- msg:
			case <-ctx.Done():
				return args.Timestamp, ctx.Err()
			}
		}
	}
}

func connectionClass(sv *settings.Values) rpc.ConnectionClass {
	if useDedicatedRangefeedConnectionClass.Get(sv) {
		return rpc.RangefeedClass
	}
	return rpc.DefaultClass
}

type rangeFeedEventProducerFactory func(
	ctx context.Context,
	client rpc.RestrictedInternalClient,
	req *roachpb.RangeFeedRequest,
) (roachpb.RangeFeedEventProducer, func(), error)

// legacyRangeFeedEventProducer is a rangeFeedEventProducerFactory using
// legacy RangeFeed RPC.
func legacyRangeFeedEventProducer(
	ctx context.Context, client rpc.RestrictedInternalClient, req *roachpb.RangeFeedRequest,
) (producer roachpb.RangeFeedEventProducer, cleanup func(), err error) {
	cleanup = func() {}
	producer, err = client.RangeFeed(ctx, req)
	return producer, cleanup, err
}

// sentinel error returned when cancelling rangefeed when it is stuck.
var errRestartStuckRange = errors.New("rangefeed restarting due to inactivity")
