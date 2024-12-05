// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord

import (
	"context"
	"fmt"
	"io"
	"slices"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangecache"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/limit"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
)

// defRangefeedConnClass is the default rpc.ConnectionClass used for rangefeed
// traffic. Normally it is RangefeedClass, but can be flipped to DefaultClass if
// the corresponding env variable is true.
var defRangefeedConnClass = func() rpc.ConnectionClass {
	if envutil.EnvOrDefaultBool("COCKROACH_RANGEFEED_USE_DEFAULT_CONNECTION_CLASS", false) {
		return rpc.DefaultClass
	}
	return rpc.RangefeedClass
}()

var catchupStartupRate = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"kv.rangefeed.client.stream_startup_rate",
	"controls the rate per second the client will initiate new rangefeed stream for a single range; 0 implies unlimited",
	100, // e.g.: 200 seconds for 20k ranges.
	settings.NonNegativeInt,
	settings.WithPublic,
)

// ForEachRangeFn is used to execute `fn` over each range in a rangefeed.
type ForEachRangeFn func(fn ActiveRangeFeedIterFn) error

// A RangeObserver is a function that observes the ranges in a rangefeed
// by polling fn.
type RangeObserver func(fn ForEachRangeFn)

type rangeFeedConfig struct {
	overSystemTable       bool
	withDiff              bool
	withFiltering         bool
	withMetadata          bool
	withMatchingOriginIDs []uint32
	rangeObserver         RangeObserver
	consumerID            int64

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

// WithMatchingOriginIDs opts the rangefeed into emitting events originally written by
// clusters with the assoicated origin IDs during logical data replication.
func WithMatchingOriginIDs(originIDs ...uint32) RangeFeedOption {
	return optionFunc(func(c *rangeFeedConfig) {
		c.withMatchingOriginIDs = originIDs
	})
}

// WithRangeObserver is called when the rangefeed starts with a function that
// can be used to iterate over all the ranges.
func WithRangeObserver(observer RangeObserver) RangeFeedOption {
	return optionFunc(func(c *rangeFeedConfig) {
		c.rangeObserver = observer
	})
}

func WithMetadata() RangeFeedOption {
	return optionFunc(func(c *rangeFeedConfig) {
		c.withMetadata = true
	})
}

func WithConsumerID(cid int64) RangeFeedOption {
	return optionFunc(func(c *rangeFeedConfig) {
		c.consumerID = cid
	})
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
	ctx = ds.AnnotateCtx(ctx)
	ctx, sp := tracing.EnsureChildSpan(ctx, ds.AmbientContext.Tracer, "dist sender")
	defer sp.Finish()

	rr := newRangeFeedRegistry(ctx, cfg.withDiff)
	ds.activeRangeFeeds.Add(rr)
	defer ds.activeRangeFeeds.Remove(rr)
	if cfg.rangeObserver != nil {
		cfg.rangeObserver(rr.ForEachPartialRangefeed)
	}

	rl := newCatchupScanRateLimiter(&ds.st.SV)
	return muxRangeFeed(ctx, cfg, spans, ds, rr, rl, eventCh)
}

// divideAllSpansOnRangeBoundaries divides all spans on range boundaries and invokes
// provided onRange function for each range.
func divideAllSpansOnRangeBoundaries(
	ctx context.Context, spans []SpanTimePair, onRange onRangeFn, ds *DistSender,
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
	slices.SortFunc(spans, func(a, b SpanTimePair) int {
		return a.StartAfter.Compare(b.StartAfter)
	})

	for _, stp := range spans {
		rs, err := keys.SpanAddr(stp.Span)
		if err != nil {
			return err
		}
		if err := divideSpanOnRangeBoundaries(ctx, ds, rs, stp.StartAfter, onRange, parentRangeFeedMetadata{}); err != nil {
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
	Span                    roachpb.Span
	StartAfter              hlc.Timestamp // exclusive
	CreatedTime             time.Time
	ParentRangefeedMetadata parentRangeFeedMetadata

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
	ds.activeRangeFeeds.Range(func(r *rangeFeedRegistry) bool {
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
	r.ranges.Range(func(active *activeRangeFeed) bool {
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

	// localConnection indicates if this rangefeed connected
	// to a local node.
	localConnection bool

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

// onConnect is a callback invoked when attempt to connect to the specified
// destination is made.
func (a *activeRangeFeed) onConnect(
	dest rpc.RestrictedInternalClient, metrics *DistSenderRangeFeedMetrics,
) {
	if rpc.IsLocal(dest) {
		if !a.localConnection {
			metrics.RangefeedLocalRanges.Inc(1)
		}
		a.localConnection = true
	} else if a.localConnection {
		// We used to connect to local node, but no more.
		a.localConnection = false
		metrics.RangefeedLocalRanges.Dec(1)
	}
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
	ranges syncutil.Set[*activeRangeFeed]
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

type onRangeFn func(
	ctx context.Context, rs roachpb.RSpan, startAfter hlc.Timestamp, token rangecache.EvictionToken, metadata parentRangeFeedMetadata,
) error

// parentRangeFeedMetadata contains metadata around a parent rangefeed that is
// being retried.
type parentRangeFeedMetadata struct {
	// FromManualSplit is true when the previous partial rangefeed was interrupted
	// by a manual split.
	fromManualSplit bool
	// StartKey is the start key of the previous partial rangefeed.
	startKey roachpb.Key
}

func sendMetadata(
	ctx context.Context,
	eventCh chan<- RangeFeedMessage,
	span roachpb.Span,
	parentMetadata parentRangeFeedMetadata,
) error {
	select {
	case eventCh <- RangeFeedMessage{
		RangeFeedEvent: &kvpb.RangeFeedEvent{
			Metadata: &kvpb.RangeFeedMetadata{
				Span:            span,
				FromManualSplit: parentMetadata.fromManualSplit,
				ParentStartKey:  parentMetadata.startKey,
			},
		},
		RegisteredSpan: span,
	}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func divideSpanOnRangeBoundaries(
	ctx context.Context,
	ds *DistSender,
	rs roachpb.RSpan,
	startAfter hlc.Timestamp,
	onRange onRangeFn,
	parentMetadata parentRangeFeedMetadata,
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
		if err := onRange(ctx, partialRS, startAfter, ri.Token(), parentMetadata); err != nil {
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
	parentMetadata parentRangeFeedMetadata,
	initialRangeID roachpb.RangeID,
) *activeRangeFeed {
	// Register partial range feed with registry.
	active := &activeRangeFeed{
		PartialRangeFeed: PartialRangeFeed{
			Span:                    span,
			StartAfter:              startAfter,
			ParentRangefeedMetadata: parentMetadata,
			CreatedTime:             timeutil.Now(),
			RangeID:                 initialRangeID,
		},
	}

	active.release = func() {
		active.releaseCatchupScan()
		rr.ranges.Remove(active)
		metrics.RangefeedRanges.Dec(1)
		if active.localConnection {
			metrics.RangefeedLocalRanges.Dec(1)
		}
	}

	rr.ranges.Add(active)
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

type rangefeedErrorInfo struct {
	resolveSpan bool // true if the span resolution needs to be performed, and rangefeed restarted.
	evict       bool // true if routing info needs to be updated prior to retry.
	manualSplit bool // true if the rangefeed restarted from a manual split.
}

// handleRangefeedError handles an error that occurred while running rangefeed.
// Returns rangefeedErrorInfo describing how the error should be handled for the
// range. Returns an error if the entire rangefeed should terminate.
func handleRangefeedError(
	ctx context.Context, metrics *DistSenderRangeFeedMetrics, err error, spawnedFromManualSplit bool,
) (rangefeedErrorInfo, error) {
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
	case IsSendError(err):
		metrics.Errors.SendErrors.Inc(1)
		return rangefeedErrorInfo{evict: true}, nil
	case errors.HasType(err, (*kvpb.RangeNotFoundError)(nil)):
		metrics.Errors.RangeNotFound.Inc(1)
		return rangefeedErrorInfo{evict: true}, nil
	case errors.HasType(err, (*kvpb.RangeKeyMismatchError)(nil)):
		metrics.Errors.RangeKeyMismatch.Inc(1)
		// If the retrying rangefeed was created after a manual split, but retried
		// due to a rangekey mismatch error, the range descriptor cache that spawned
		// the retrying rangefeed was stale, so carry over manualSplit flag to the
		// next rangefeed.
		return rangefeedErrorInfo{evict: true, resolveSpan: true, manualSplit: spawnedFromManualSplit}, nil
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
			// Try again with same descriptor. These are transient errors that should
			// not show up again, or should be retried on another replica which will
			// likely not have the same issue.
			return rangefeedErrorInfo{}, nil
		case kvpb.RangeFeedRetryError_REASON_RANGE_SPLIT,
			kvpb.RangeFeedRetryError_REASON_RANGE_MERGED,
			kvpb.RangeFeedRetryError_REASON_NO_LEASEHOLDER:
			return rangefeedErrorInfo{evict: true, resolveSpan: true}, nil
		case kvpb.RangeFeedRetryError_REASON_MANUAL_RANGE_SPLIT:
			return rangefeedErrorInfo{evict: true, resolveSpan: true, manualSplit: true}, nil
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
	replicas, err := NewReplicaSlice(ctx, ds.nodeDescs, desc, nil, AllExtantReplicas)
	if err != nil {
		return nil, err
	}
	replicas.OptimizeReplicaOrder(ctx, ds.st, ds.nodeIDGetter(), ds.healthFunc, ds.latencyFunc, ds.locality)
	opts := SendOptions{class: defRangefeedConnClass}
	return ds.transportFactory(opts, replicas), nil
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
	withMatchingOriginIDs []uint32,
	consumerID int64,
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
		ConsumerID:            consumerID,
		WithDiff:              withDiff,
		WithFiltering:         withFiltering,
		WithMatchingOriginIDs: withMatchingOriginIDs,
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
}

func newCatchupScanRateLimiter(sv *settings.Values) *catchupScanRateLimiter {
	const slowAcquisitionThreshold = 5 * time.Second
	lim := getCatchupRateLimit(sv)

	return &catchupScanRateLimiter{
		sv:    sv,
		limit: lim,
		pacer: quotapool.NewRateLimiter(
			"distSenderCatchupLimit", lim, 0, /* smooth rate limit without burst */
			quotapool.OnSlowAcquisition(slowAcquisitionThreshold, logSlowCatchupScanAcquisition(slowAcquisitionThreshold))),
	}
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
