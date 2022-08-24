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
	"container/heap"
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
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/limit"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
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

var rangefeedRangeLivenessThreshold = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"kv.rangefeed.range_liveness_threshold",
	`rangefeed is expected to receive events at all times.

Restart rangefeed if it appears to be stuck and no events have been received for at least
kv.rangefeed.range_liveness_threshold.
This setting should be conservative; it should be larger than kv.closed_timestamp.side_transport_interval
Set to 0 to disable heartbeat checks`,
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

	// Kick off heartbeat watcher.
	g.GoCtx(func(ctx context.Context) error {
		return rr.heartBeatLoop(ctx, ds.st)
	})

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

func (a *activeRangeFeed) setError(err error) {
	a.Lock()
	defer a.Unlock()
	var sinceCheckpoint string
	if a.Resolved.IsEmpty() {
		sinceCheckpoint = "Inf"
	} else {
		sinceCheckpoint = timeutil.Since(a.Resolved.GoTime()).String()
	}
	a.LastErr = errors.Wrapf(err, "disconnect with last checkpoint at %s (%s ago)",
		a.Resolved, sinceCheckpoint)
	a.NumErrs++
}

// rangeFeedRegistry is responsible for keeping track of currently executing
// range feeds.
type rangeFeedRegistry struct {
	RangeFeedContext
	ranges sync.Map // map[*activeRangeFeed]nil
	mu     struct {
		syncutil.Mutex
		heartbeats heartBeatHeap
	}
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
		var maxTS hlc.Timestamp
		var err error
		rr.withHeartbeat(ctx, active, func(ctx context.Context, cb onRangeEventCb) {
			maxTS, err = ds.singleRangeFeed(
				ctx, span, startAfter, withDiff, token.Desc(),
				catchupSem, eventCh, streamProducerFactory, cb)
		})

		// Forward the timestamp in case we end up sending it again.
		startAfter.Forward(maxTS)

		if err != nil {
			active.setError(err)

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
				if reason := contextutil.GetCancelReason(ctx); reason != nil && errors.Is(reason, restartLivenessErr) {
					// Evict the descriptor from the cache and reload on next attempt.
					token.Evict(ctx)
					token = rangecache.EvictionToken{}
					continue
				}
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
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

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

	var streamCleanup func()
	maybeCleanupStream := func() {
		if streamCleanup != nil {
			streamCleanup()
			streamCleanup = nil
		}
	}
	defer maybeCleanupStream()

	for {
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

		var stream roachpb.RangeFeedEventProducer
		stream, streamCleanup, err = streamProducerFactory(ctx, client, &args)
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

// sentinel error returned when cancelling rangefeed due to liveness.
var restartLivenessErr = errors.New("rangefeed restarting due to liveness")

// withHeartbeat invokes provided function with the context and a onRangeEvent
// callback. The context may be cancelled if the callback is not invoked for too
// long. contextutil.GetCancelReason may be used to check if the reason for
// cancellation was restartLivenessErr.
func (r *rangeFeedRegistry) withHeartbeat(
	ctx context.Context, active *activeRangeFeed, fn func(ctx context.Context, cb onRangeEventCb),
) {
	ctx, cancel := contextutil.WithCancelReason(ctx)
	hbCtx := &heartBeatCtx{
		cancel: cancel,
		idx:    -1,
	}

	heartBeat := func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		hbCtx.last = timeutil.Now()

		if hbCtx.idx == -1 {
			heap.Push(&r.mu.heartbeats, &hbCtx)
		} else {
			heap.Fix(&r.mu.heartbeats, hbCtx.idx)
		}
	}

	// Put heartbeat ctx into the heap.
	heartBeat()

	// Run the function, updating heartbeat whenever we receive an event.
	fn(ctx, func(nodeID roachpb.NodeID, rangeID roachpb.RangeID, event *roachpb.RangeFeedEvent) {
		active.onRangeEvent(nodeID, rangeID, event)
		heartBeat()
	})

	// Cleanup.
	r.mu.Lock()
	heap.Remove(&r.mu.heartbeats, hbCtx.idx)
	r.mu.Unlock()
}

// heartBeatLoop perioodically checks active range feeds, and cancels
// ones that appear to be stuck.
func (r *rangeFeedRegistry) heartBeatLoop(ctx context.Context, st *cluster.Settings) error {
	tick := timeutil.NewTimer()
	defer tick.Stop()

	restartStuckRanges := func(liveness time.Duration) {
		r.mu.Lock()
		defer r.mu.Unlock()
		threshold := timeutil.Now().Add(-liveness)
		for r.mu.heartbeats.Len() > 0 && r.mu.heartbeats[0].last.Before(threshold) {
			hb := heap.Pop(&r.mu.heartbeats).(*heartBeatCtx)
			hb.cancel(restartLivenessErr)
		}
	}

	resetTimer := func() time.Duration {
		const recheckAfter = time.Minute
		l := rangefeedRangeLivenessThreshold.Get(&st.SV)
		if l == 0 {
			tick.Reset(recheckAfter)
		} else {
			tick.Reset(l)
		}
		return l
	}
	resetTimer()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
			tick.Read = true
			if threshold := resetTimer(); threshold > 0 {
				restartStuckRanges(threshold)
			}
		}
	}
}

// heartBeatCtx contains heartbeat context.
type heartBeatCtx struct {
	cancel contextutil.CancelWithReasonFunc
	last   time.Time
	idx    int
}

// a min-heap of heartBeatCtx, ordered from oldest to newest
// implements heap.Interface.
type heartBeatHeap []*heartBeatCtx

var _ heap.Interface = (*heartBeatHeap)(nil)

func (h heartBeatHeap) Len() int {
	return len(h)
}

func (h heartBeatHeap) Less(i, j int) bool {
	return h[i].last.Before(h[j].last)
}

func (h heartBeatHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].idx, h[j].idx = i, j
}

func (h *heartBeatHeap) Push(x any) {
	heartBeat := x.(*heartBeatCtx)
	heartBeat.idx = len(*h)
	*h = append(*h, heartBeat)
}

func (h *heartBeatHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
