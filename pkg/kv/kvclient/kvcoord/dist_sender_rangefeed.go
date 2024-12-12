// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord

import (
	"context"
	"fmt"
	"io"
	"math"
	"sort"
	"sync"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangecache"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/limit"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/pprofutil"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
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
	settings.SystemVisible,
	"kv.rangefeed.use_dedicated_connection_class.enabled",
	"uses dedicated connection when running rangefeeds",
	util.ConstantWithMetamorphicTestBool(
		"kv.rangefeed.use_dedicated_connection_class.enabled", false),
)

var catchupStartupRate = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"kv.rangefeed.client.stream_startup_rate",
	"controls the rate per second the client will initiate new rangefeed stream for a single range; 0 implies unlimited",
	100, // e.g.: 200 seconds for 20k ranges.
	settings.NonNegativeInt,
	settings.WithPublic,
)

var catchupScanConcurrency = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"kv.rangefeed.catchup_scan_concurrency",
	"number of catchup scans that a single rangefeed can execute concurrently; 0 implies unlimited",
	8,
	settings.NonNegativeInt,
)

var rangefeedRangeStuckThreshold = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"kv.rangefeed.range_stuck_threshold",
	"restart rangefeeds if they don't emit anything for the specified threshold; 0 disables (kv.rangefeed.closed_timestamp_refresh_interval takes precedence)",
	time.Minute,
	settings.NonNegativeDuration,
	settings.WithPublic)

// ForEachRangeFn is used to execute `fn` over each range in a rangefeed.
type ForEachRangeFn func(fn ActiveRangeFeedIterFn) error

// A RangeObserver is a function that observes the ranges in a rangefeed
// by polling fn.
type RangeObserver func(fn ForEachRangeFn)

type rangeFeedConfig struct {
	useMuxRangeFeed bool
	overSystemTable bool
	withDiff        bool
	withFiltering   bool
	rangeObserver   RangeObserver

	knobs struct {
		// onRangefeedEvent invoked on each rangefeed event.
		// Returns boolean indicating if event should be skipped or an error
		// indicating if rangefeed should terminate.
		// streamID set only for mux rangefeed.
		onRangefeedEvent func(ctx context.Context, s roachpb.Span, muxStreamID int64, event *kvpb.RangeFeedEvent) (skip bool, _ error)
		// metrics overrides rangefeed metrics to use.
		metrics *DistSenderRangeFeedMetrics
		// captureMuxRangeFeedRequestSender is a callback invoked when mux
		// rangefeed establishes connection to the node.
		captureMuxRangeFeedRequestSender func(nodeID roachpb.NodeID, sender func(req *kvpb.RangeFeedRequest) error)
		// beforeSendRequest is a mux rangefeed callback invoked prior to sending rangefeed request.
		beforeSendRequest func()
	}
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

// WithSystemTablePriority is used for system-internal rangefeeds, it uses a
// higher admission priority during catch up scans.
func WithSystemTablePriority() RangeFeedOption {
	return optionFunc(func(c *rangeFeedConfig) {
		c.overSystemTable = true
	})
}

// WithDiff turns on "diff" option for the rangefeed.
func WithDiff() RangeFeedOption {
	return optionFunc(func(c *rangeFeedConfig) {
		c.withDiff = true
	})
}

// WithFiltering opts into rangefeed filtering. When rangefeed filtering is on,
// any transactional write with OmitInRangefeeds = true will be dropped.
func WithFiltering() RangeFeedOption {
	return optionFunc(func(c *rangeFeedConfig) {
		c.withFiltering = true
	})
}

// WithRangeObserver is called when the rangefeed starts with a function that
// can be used to iterate over all the ranges.
func WithRangeObserver(observer RangeObserver) RangeFeedOption {
	return optionFunc(func(c *rangeFeedConfig) {
		c.rangeObserver = observer
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
	return ds.RangeFeedSpans(ctx, timedSpans, eventCh, opts...)
}

// SpanTimePair is a pair of span along with its starting time. The starting
// time is exclusive, i.e. the first possible emitted event (including catchup
// scans) will be at startAfter.Next().
type SpanTimePair struct {
	Span       roachpb.Span
	StartAfter hlc.Timestamp // exclusive
}

func (p SpanTimePair) String() string {
	return fmt.Sprintf("%s@%s", p.Span, p.StartAfter)
}

// RangeFeedSpans is similar to RangeFeed but allows specification of different
// starting time for each span.
func (ds *DistSender) RangeFeedSpans(
	ctx context.Context,
	spans []SpanTimePair,
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
	metrics := &ds.metrics.DistSenderRangeFeedMetrics
	if cfg.knobs.metrics != nil {
		metrics = cfg.knobs.metrics
	}
	ctx = ds.AnnotateCtx(ctx)
	ctx, sp := tracing.EnsureChildSpan(ctx, ds.AmbientContext.Tracer, "dist sender")
	defer sp.Finish()

	rr := newRangeFeedRegistry(ctx, cfg.withDiff)
	ds.activeRangeFeeds.Store(rr, nil)
	defer ds.activeRangeFeeds.Delete(rr)
	if cfg.rangeObserver != nil {
		cfg.rangeObserver(rr.ForEachPartialRangefeed)
	}

	rl := newCatchupScanRateLimiter(&ds.st.SV, cfg.useMuxRangeFeed)

	if enableMuxRangeFeed && cfg.useMuxRangeFeed {
		return muxRangeFeed(ctx, cfg, spans, ds, rr, rl, eventCh)
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
				active := newActiveRangeFeed(span, sri.startAfter, rr, metrics)

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
		return divideAllSpansOnRangeBoundaries(ctx, spans, sendSingleRangeInfo(rangeCh), ds, &g)
	})

	return g.Wait()
}

// divideAllSpansOnRangeBoundaries divides all spans on range boundaries and invokes
// provided onRange function for each range.  Resolution happens concurrently using provided
// context group.
func divideAllSpansOnRangeBoundaries(
	ctx context.Context, spans []SpanTimePair, onRange onRangeFn, ds *DistSender, g *ctxgroup.Group,
) error {
	// Sort input spans based on their start time -- older spans first.
	// Starting rangefeed over large number of spans is an expensive proposition,
	// since this involves initiating catch-up scan operation for each span. These
	// operations are throttled (both on the client and on the server). Thus, it
	// is possible that only some portion of the spans  will make it past catch-up
	// phase.  If the caller maintains checkpoint, and then restarts rangefeed
	// (for any reason), then we will restart against the same list of spans --
	// but this time, we'll begin with the spans that might be substantially ahead
	// of the rest of the spans. We simply sort input spans so that the oldest
	// spans get a chance to complete their catch-up scan.
	sort.Slice(spans, func(i, j int) bool {
		return spans[i].StartAfter.Less(spans[j].StartAfter)
	})

	for _, stp := range spans {
		rs, err := keys.SpanAddr(stp.Span)
		if err != nil {
			return err
		}
		if err := divideSpanOnRangeBoundaries(ctx, ds, rs, stp.StartAfter, onRange); err != nil {
			return err
		}
	}
	return nil
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
	// The following fields are immutable and are initialized
	// once the rangefeed for a range starts.
	Span        roachpb.Span
	StartAfter  hlc.Timestamp // exclusive
	CreatedTime time.Time

	// Fields below are mutable.
	NodeID            roachpb.NodeID
	RangeID           roachpb.RangeID
	LastValueReceived time.Time
	Resolved          hlc.Timestamp
	InCatchup         bool
	NumErrs           int
	LastErr           error
}

// ActiveRangeFeedIterFn is an iterator function which is passed PartialRangeFeed structure.
// Iterator function may return an iterutil.StopIteration sentinel error to stop iteration
// early.
type ActiveRangeFeedIterFn func(rfCtx RangeFeedContext, feed PartialRangeFeed) error

const continueIter = true
const stopIter = false

// ForEachActiveRangeFeed invokes provided function for each active rangefeed.
// iterutil.StopIteration can be returned by `fn` to stop iteration, and doing
// so will not return this error.
func (ds *DistSender) ForEachActiveRangeFeed(fn ActiveRangeFeedIterFn) (iterErr error) {
	ds.activeRangeFeeds.Range(func(k, v interface{}) bool {
		r := k.(*rangeFeedRegistry)
		iterErr = r.ForEachPartialRangefeed(fn)
		return iterErr == nil
	})

	return iterutil.Map(iterErr)
}

// ForEachPartialRangefeed invokes provided function for each partial rangefeed. Use manageIterationErrs
// if the fn uses iterutil.StopIteration to stop iteration.
func (r *rangeFeedRegistry) ForEachPartialRangefeed(fn ActiveRangeFeedIterFn) (iterErr error) {
	partialRangeFeed := func(active *activeRangeFeed) PartialRangeFeed {
		active.Lock()
		defer active.Unlock()
		return active.PartialRangeFeed
	}
	r.ranges.Range(func(k, v interface{}) bool {
		active := k.(*activeRangeFeed)
		if err := fn(r.RangeFeedContext, partialRangeFeed(active)); err != nil {
			iterErr = err
			return stopIter
		}
		return continueIter
	})
	return iterErr
}

// activeRangeFeed is a thread safe PartialRangeFeed.
type activeRangeFeed struct {
	// release releases resources and updates metrics when
	// active rangefeed completes.
	release func()

	// catchupRes is the catchup scan quota acquired upon the
	// start of rangefeed.
	// It is released when this stream receives first checkpoint
	// (meaning: catchup scan completes).
	// Safe to release multiple times.
	catchupRes catchupAlloc

	// PartialRangeFeed contains information about this range
	// mostly for the purpose of exposing it to the external
	// observability tools (crdb_internal.active_range_feeds).
	// This state is protected by the mutex to avoid data races.
	// The mutex overhead in a common case -- a single goroutine
	// (singleRangeFeed) mutating this data is low.
	syncutil.Mutex
	PartialRangeFeed
}

func (a *activeRangeFeed) onRangeEvent(
	nodeID roachpb.NodeID, rangeID roachpb.RangeID, event *kvpb.RangeFeedEvent,
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

type onRangeFn func(
	ctx context.Context, rs roachpb.RSpan, startAfter hlc.Timestamp, token rangecache.EvictionToken,
) error

func divideSpanOnRangeBoundaries(
	ctx context.Context,
	ds *DistSender,
	rs roachpb.RSpan,
	startAfter hlc.Timestamp,
	onRange onRangeFn,
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
		partialRS, err := nextRS.Intersect(desc.RSpan())
		if err != nil {
			return err
		}
		nextRS.Key = partialRS.EndKey
		if err := onRange(ctx, partialRS, startAfter, ri.Token()); err != nil {
			return err
		}
		if !ri.NeedAnother(nextRS) {
			break
		}
	}
	return ri.Error()
}

// newActiveRangeFeed registers active rangefeed with rangefeedRegistry.
// The caller must call active.release() in order to cleanup.
func newActiveRangeFeed(
	span roachpb.Span,
	startAfter hlc.Timestamp,
	rr *rangeFeedRegistry,
	metrics *DistSenderRangeFeedMetrics,
) *activeRangeFeed {
	// Register partial range feed with registry.
	active := &activeRangeFeed{
		PartialRangeFeed: PartialRangeFeed{
			Span:        span,
			StartAfter:  startAfter,
			CreatedTime: timeutil.Now(),
		},
	}

	active.release = func() {
		active.releaseCatchupScan()
		rr.ranges.Delete(active)
		metrics.RangefeedRanges.Dec(1)
	}

	rr.ranges.Store(active, nil)
	metrics.RangefeedRanges.Inc(1)

	return active
}

// releaseCatchupScan releases catchup scan allocation, if any.
// safe to call multiple times.
func (a *activeRangeFeed) releaseCatchupScan() {
	if a.catchupRes != nil {
		a.catchupRes.Release()
		a.catchupRes = nil
		a.Lock()
		a.InCatchup = false
		a.Unlock()
	}
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

type rangefeedErrorInfo struct {
	resolveSpan bool // true if the span resolution needs to be performed, and rangefeed restarted.
	evict       bool // true if routing info needs to be updated prior to retry.
}

// handleRangefeedError handles an error that occurred while running rangefeed.
// Returns rangefeedErrorInfo describing how the error should be handled for the
// range. Returns an error if the entire rangefeed should terminate.
func handleRangefeedError(
	ctx context.Context, metrics *DistSenderRangeFeedMetrics, err error,
) (rangefeedErrorInfo, error) {
	metrics.Errors.RangefeedRestartRanges.Inc(1)

	if err == nil {
		return rangefeedErrorInfo{}, nil
	}

	switch {
	case errors.Is(err, io.EOF):
		// If we got an EOF, treat it as a signal to restart single range feed.
		return rangefeedErrorInfo{}, nil
	case errors.HasType(err, (*kvpb.StoreNotFoundError)(nil)):
		// We shouldn't be seeing these errors if descriptors are correct, but if
		// we do, we'd rather evict descriptor before retrying.
		metrics.Errors.StoreNotFound.Inc(1)
		return rangefeedErrorInfo{evict: true}, nil
	case errors.HasType(err, (*kvpb.NodeUnavailableError)(nil)):
		// These errors are likely to be unique to the replica that
		// reported them, so no action is required before the next
		// retry.
		metrics.Errors.NodeNotFound.Inc(1)
		return rangefeedErrorInfo{}, nil
	case errors.Is(err, errRestartStuckRange):
		// Stuck ranges indicate a bug somewhere in the system.  We are being
		// defensive and attempt to restart this rangefeed. Usually, any
		// stuck-ness is cleared out if we just attempt to re-resolve range
		// descriptor and retry.
		//
		// The error contains the replica which we were waiting for.
		log.Warningf(ctx, "restarting stuck rangefeed: %s", err)
		metrics.Errors.Stuck.Inc(1)
		return rangefeedErrorInfo{evict: true}, nil
	case IsSendError(err):
		metrics.Errors.SendErrors.Inc(1)
		return rangefeedErrorInfo{evict: true}, nil
	case errors.HasType(err, (*kvpb.RangeNotFoundError)(nil)):
		metrics.Errors.RangeNotFound.Inc(1)
		return rangefeedErrorInfo{evict: true}, nil
	case errors.HasType(err, (*kvpb.RangeKeyMismatchError)(nil)):
		metrics.Errors.RangeKeyMismatch.Inc(1)
		return rangefeedErrorInfo{evict: true, resolveSpan: true}, nil
	case errors.HasType(err, (*kvpb.RangeFeedRetryError)(nil)):
		var t *kvpb.RangeFeedRetryError
		if ok := errors.As(err, &t); !ok {
			return rangefeedErrorInfo{}, errors.AssertionFailedf("wrong error type: %T", err)
		}
		metrics.Errors.GetRangeFeedRetryCounter(t.Reason).Inc(1)
		switch t.Reason {
		case kvpb.RangeFeedRetryError_REASON_REPLICA_REMOVED,
			kvpb.RangeFeedRetryError_REASON_RAFT_SNAPSHOT,
			kvpb.RangeFeedRetryError_REASON_LOGICAL_OPS_MISSING,
			kvpb.RangeFeedRetryError_REASON_SLOW_CONSUMER,
			kvpb.RangeFeedRetryError_REASON_RANGEFEED_CLOSED:
			// Try again with same descriptor. These are transient
			// errors that should not show up again.
			return rangefeedErrorInfo{}, nil
		case kvpb.RangeFeedRetryError_REASON_RANGE_SPLIT,
			kvpb.RangeFeedRetryError_REASON_RANGE_MERGED,
			kvpb.RangeFeedRetryError_REASON_NO_LEASEHOLDER:
			return rangefeedErrorInfo{evict: true, resolveSpan: true}, nil
		default:
			return rangefeedErrorInfo{}, errors.AssertionFailedf("unrecognized retryable error type: %T", err)
		}
	default:
		return rangefeedErrorInfo{}, err
	}
}

// catchup alloc is a catchup scan allocation.
type catchupAlloc func()

// Release implements limit.Reservation
func (a catchupAlloc) Release() {
	a()
}

func (a *activeRangeFeed) acquireCatchupScanQuota(
	ctx context.Context, rl *catchupScanRateLimiter, metrics *DistSenderRangeFeedMetrics,
) error {
	metrics.RangefeedCatchupRangesWaitingClientSide.Inc(1)
	defer metrics.RangefeedCatchupRangesWaitingClientSide.Dec(1)

	// Indicate catchup scan is starting.
	alloc, err := rl.Pace(ctx)
	if err != nil {
		return err
	}
	metrics.RangefeedCatchupRanges.Inc(1)
	a.catchupRes = func() {
		alloc.Release()
		metrics.RangefeedCatchupRanges.Dec(1)
	}

	a.Lock()
	defer a.Unlock()
	a.InCatchup = true
	return nil
}

// nweTransportForRange returns Transport for the specified range descriptor.
func newTransportForRange(
	ctx context.Context, desc *roachpb.RangeDescriptor, ds *DistSender,
) (Transport, error) {
	var latencyFn LatencyFunc
	if ds.rpcContext != nil {
		latencyFn = ds.rpcContext.RemoteClocks.Latency
	}
	replicas, err := NewReplicaSlice(ctx, ds.nodeDescs, desc, nil, AllExtantReplicas)
	if err != nil {
		return nil, err
	}
	replicas.OptimizeReplicaOrder(ds.st, ds.nodeIDGetter(), ds.HealthFunc(), latencyFn, ds.locality)
	opts := SendOptions{class: connectionClass(&ds.st.SV)}
	return ds.transportFactory(opts, ds.nodeDialer, replicas)
}

// makeRangeFeedRequest constructs kvpb.RangeFeedRequest for specified span and
// rangeID. Request is constructed to watch event after specified timestamp, and
// with optional diff.  If the request corresponds to a system range, request
// receives higher admission priority.
func makeRangeFeedRequest(
	span roachpb.Span,
	rangeID roachpb.RangeID,
	isSystemRange bool,
	startAfter hlc.Timestamp,
	withDiff bool,
	withFiltering bool,
) kvpb.RangeFeedRequest {
	admissionPri := admissionpb.BulkNormalPri
	if isSystemRange {
		admissionPri = admissionpb.NormalPri
	}
	return kvpb.RangeFeedRequest{
		Span: span,
		Header: kvpb.Header{
			Timestamp: startAfter,
			RangeID:   rangeID,
		},
		WithDiff:      withDiff,
		WithFiltering: withFiltering,
		AdmissionHeader: kvpb.AdmissionHeader{
			// NB: AdmissionHeader is used only at the start of the range feed
			// stream since the initial catch-up scan is expensive.
			Priority:                 int32(admissionPri),
			CreateTime:               timeutil.Now().UnixNano(),
			Source:                   kvpb.AdmissionHeader_FROM_SQL,
			NoMemoryReservedAtSource: true,
		},
	}
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

func connectionClass(sv *settings.Values) rpc.ConnectionClass {
	if useDedicatedRangefeedConnectionClass.Get(sv) {
		return rpc.RangefeedClass
	}
	return rpc.DefaultClass
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

// sentinel error returned when cancelling rangefeed when it is stuck.
var errRestartStuckRange = errors.New("rangefeed restarting due to inactivity")

// TestingWithOnRangefeedEvent returns a test only option to modify rangefeed event.
func TestingWithOnRangefeedEvent(
	fn func(ctx context.Context, s roachpb.Span, streamID int64, event *kvpb.RangeFeedEvent) (skip bool, _ error),
) RangeFeedOption {
	return optionFunc(func(c *rangeFeedConfig) {
		c.knobs.onRangefeedEvent = fn
	})
}

// TestingWithRangeFeedMetrics returns a test only option to specify metrics to
// use while executing this rangefeed.
func TestingWithRangeFeedMetrics(m *DistSenderRangeFeedMetrics) RangeFeedOption {
	return optionFunc(func(c *rangeFeedConfig) {
		c.knobs.metrics = m
	})
}

// TestingWithMuxRangeFeedRequestSenderCapture returns a test only option to specify a callback
// that will be invoked when mux establishes connection to a node.
func TestingWithMuxRangeFeedRequestSenderCapture(
	fn func(nodeID roachpb.NodeID, capture func(request *kvpb.RangeFeedRequest) error),
) RangeFeedOption {
	return optionFunc(func(c *rangeFeedConfig) {
		c.knobs.captureMuxRangeFeedRequestSender = fn
	})
}

// TestingWithBeforeSendRequest returns a test only option that invokes
// function before sending rangefeed request.
func TestingWithBeforeSendRequest(fn func()) RangeFeedOption {
	return optionFunc(func(c *rangeFeedConfig) {
		c.knobs.beforeSendRequest = fn
	})
}

// TestingMakeRangeFeedMetrics exposes makeDistSenderRangeFeedMetrics for test use.
var TestingMakeRangeFeedMetrics = makeDistSenderRangeFeedMetrics

type catchupScanRateLimiter struct {
	pacer *quotapool.RateLimiter
	sv    *settings.Values
	limit quotapool.Limit

	// In addition to rate limiting catchup scans, a semaphore is used to restrict
	// catchup scan concurrency for regular range feeds (catchupSem is nil for mux
	// rangefeed).
	// This additional limit is necessary due to the fact that regular
	// rangefeed may buffer up to 2MB of data (or 128KB if
	// useDedicatedRangefeedConnectionClass set to true) per rangefeed stream in the
	// http2/gRPC buffers -- making OOMs likely if the consumer does not consume
	// events quickly enough. See
	// https://github.com/cockroachdb/cockroach/issues/74219 for details.
	// TODO(yevgeniy): Drop this once regular rangefeed gets deprecated.
	catchupSemLimit int
	catchupSem      *limit.ConcurrentRequestLimiter
}

func newCatchupScanRateLimiter(sv *settings.Values, useMuxRangeFeed bool) *catchupScanRateLimiter {
	const slowAcquisitionThreshold = 5 * time.Second
	lim := getCatchupRateLimit(sv)

	rl := &catchupScanRateLimiter{
		sv:    sv,
		limit: lim,
		pacer: quotapool.NewRateLimiter(
			"distSenderCatchupLimit", lim, 0, /* smooth rate limit without burst */
			quotapool.OnSlowAcquisition(slowAcquisitionThreshold, logSlowCatchupScanAcquisition(slowAcquisitionThreshold))),
	}

	if !useMuxRangeFeed {
		rl.catchupSemLimit = maxConcurrentCatchupScans(sv)
		l := limit.MakeConcurrentRequestLimiter("distSenderCatchupLimit", rl.catchupSemLimit)
		rl.catchupSem = &l
	}

	return rl
}

func maxConcurrentCatchupScans(sv *settings.Values) int {
	l := catchupScanConcurrency.Get(sv)
	if l == 0 {
		return math.MaxInt
	}
	return int(l)
}

func getCatchupRateLimit(sv *settings.Values) quotapool.Limit {
	if r := catchupStartupRate.Get(sv); r > 0 {
		return quotapool.Limit(r)
	}
	return quotapool.Inf()
}

// Pace paces the catchup scan startup.
func (rl *catchupScanRateLimiter) Pace(ctx context.Context) (limit.Reservation, error) {
	// Take opportunity to update limits if they have changed.
	if lim := getCatchupRateLimit(rl.sv); lim != rl.limit {
		rl.limit = lim
		rl.pacer.UpdateLimit(lim, 0 /* smooth rate limit without burst */)
	}

	if err := rl.pacer.WaitN(ctx, 1); err != nil {
		return nil, err
	}

	// Regular rangefeed, in addition to pacing also acquires catchup scan quota.
	if rl.catchupSem != nil {
		// Take opportunity to update limits if they have changed.
		if lim := maxConcurrentCatchupScans(rl.sv); lim != rl.catchupSemLimit {
			rl.catchupSem.SetLimit(lim)
		}
		return rl.catchupSem.Begin(ctx)
	}

	return catchupAlloc(releaseNothing), nil
}

func releaseNothing() {}

// logSlowCatchupScanAcquisition is a function returning a quotapool.SlowAcquisitionFunction.
// It differs from the quotapool.LogSlowAcquisition in that only some of slow acquisition
// events are logged to reduce log spam.
func logSlowCatchupScanAcquisition(loggingMinInterval time.Duration) quotapool.SlowAcquisitionFunc {
	logSlowAcquire := log.Every(loggingMinInterval)

	return func(ctx context.Context, poolName string, r quotapool.Request, start time.Time) func() {
		shouldLog := logSlowAcquire.ShouldLog()
		if shouldLog {
			log.Warningf(ctx, "have been waiting %s attempting to acquire catchup scan quota",
				timeutil.Since(start))
		}

		return func() {
			if shouldLog {
				log.Infof(ctx, "acquired catchup quota after %s", timeutil.Since(start))
			}
		}
	}
}
