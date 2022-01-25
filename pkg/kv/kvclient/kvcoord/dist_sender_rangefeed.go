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

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangecache"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
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
	rs        roachpb.RSpan
	startFrom hlc.Timestamp
	token     rangecache.EvictionToken
}

var useDedicatedRangefeedConnectionClass = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.rangefeed.use_dedicated_connection_class.enabled",
	"uses dedicated connection when running rangefeeds",
	util.ConstantWithMetamorphicTestBool(
		"kv.rangefeed.use_dedicated_connection_class.enabled", false),
)

// A "kill switch" to disable multiplexing rangefeed if severe issues discovered with new implementation.
var enableMuxRangeFeed = envutil.EnvOrDefaultBool("COCKROACH_ENABLE_MULTIPLEXING_RANGEFEED", true)

// RangeFeed divides a RangeFeed request on range boundaries and establishes a
// RangeFeed to each of the individual ranges. It streams back results on the
// provided channel.
//
// Note that the timestamps in RangeFeedCheckpoint events that are streamed back
// may be lower than the timestamp given here.
func (ds *DistSender) RangeFeed(
	ctx context.Context,
	spans []roachpb.Span,
	startFrom hlc.Timestamp,
	withDiff bool,
	eventCh chan<- *roachpb.RangeFeedEvent,
) error {
	if len(spans) == 0 {
		return errors.AssertionFailedf("expected at least 1 span, got none")
	}

	ctx = ds.AnnotateCtx(ctx)
	ctx, sp := tracing.EnsureChildSpan(ctx, ds.AmbientContext.Tracer, "dist sender")
	defer sp.Finish()

	rr := newRangeFeedRegistry(ctx, startFrom, withDiff)
	ds.activeRangeFeeds.Store(rr, nil)
	defer ds.activeRangeFeeds.Delete(rr)

	if ds.st.Version.IsActive(ctx, clusterversion.RangefeedUseOneStreamPerNode) &&
		enableMuxRangeFeed {
		return ds.startMuxRangeFeed(ctx, spans, startFrom, withDiff, rr, eventCh)
	}

	return ds.startRangeFeed(ctx, spans, startFrom, withDiff, rr, eventCh)
}

// TODO(yevgeniy): Deprecate and remove non-streaming implementation in 22.2
func (ds *DistSender) startRangeFeed(
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
					return ds.partialRangeFeed(ctx, rr, sri.rs, sri.startFrom, sri.token, withDiff, rangeCh, eventCh)
				})
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})

	// Kick off the initial set of ranges.
	for i := range rSpans {
		rs := rSpans[i]
		g.GoCtx(func(ctx context.Context) error {
			return divideAndSendRangeFeedToRanges(ctx, ds, rs, startFrom, rangeCh)
		})
	}

	return g.Wait()
}

func spansToResolvedSpans(spans []roachpb.Span) (rSpans []roachpb.RSpan, _ error) {
	var sg roachpb.SpanGroup
	sg.Add(spans...)
	for _, sp := range sg.Slice() {
		rs, err := keys.SpanAddr(sp)
		if err != nil {
			return nil, err
		}
		rSpans = append(rSpans, rs)
	}
	return rSpans, nil
}

// RangeFeedContext is the structure containing arguments passed to
// RangeFeed call.  It functions as a kind of key for an active range feed.
type RangeFeedContext struct {
	ID      int64  // unique ID identifying range feed.
	CtxTags string // context tags

	// StartFrom and withDiff options passed to RangeFeed call.
	StartFrom hlc.Timestamp
	WithDiff  bool
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

	ds.activeRangeFeeds.Range(func(k, v interface{}) bool {
		r := k.(*rangeFeedRegistry)
		r.ranges.Range(func(k, v interface{}) bool {
			active := k.(*activeRangeFeed)
			if err := fn(r.RangeFeedContext, active.snapshot()); err != nil {
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
	release func()
	mu      struct {
		syncutil.Mutex
		PartialRangeFeed
	}
}

func newActiveRangeFeed(
	rr *rangeFeedRegistry, rangeID roachpb.RangeID, span roachpb.Span, startTS hlc.Timestamp,
) *activeRangeFeed {
	a := &activeRangeFeed{}
	a.mu.PartialRangeFeed = PartialRangeFeed{
		Span:    span,
		StartTS: startTS,
		RangeID: rangeID,
	}
	a.release = func() { rr.ranges.Delete(a) }
	rr.ranges.Store(a, nil)
	return a
}

func (a *activeRangeFeed) snapshot() PartialRangeFeed {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.mu.PartialRangeFeed
}

// setNodeID updates nodeID since the range may migrate to another node.
func (a *activeRangeFeed) setNodeID(nodeID roachpb.NodeID) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.mu.NodeID = nodeID
}

func (a *activeRangeFeed) onRangeEvent(event *roachpb.RangeFeedEvent) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if event.Val != nil || event.SST != nil {
		a.mu.LastValueReceived = timeutil.Now()
	} else if event.Checkpoint != nil {
		a.mu.Resolved = event.Checkpoint.ResolvedTS
	}
}

// rangeFeedRegistry is responsible for keeping track of currently executing
// range feeds.
type rangeFeedRegistry struct {
	RangeFeedContext
	ranges sync.Map // map[*activeRangeFeed]nil
}

func newRangeFeedRegistry(
	ctx context.Context, startFrom hlc.Timestamp, withDiff bool,
) *rangeFeedRegistry {
	rr := &rangeFeedRegistry{
		RangeFeedContext: RangeFeedContext{
			StartFrom: startFrom,
			WithDiff:  withDiff,
		},
	}
	rr.ID = *(*int64)(unsafe.Pointer(&rr))

	if b := logtags.FromContext(ctx); b != nil {
		rr.CtxTags = b.String()
	}
	return rr
}

func divideAndIterateRSpan(
	ctx context.Context,
	ds *DistSender,
	rs roachpb.RSpan,
	fn func(rs roachpb.RSpan, token rangecache.EvictionToken) error,
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
		if err := fn(partialRS, ri.Token()); err != nil {
			return err
		}
		if !ri.NeedAnother(nextRS) {
			break
		}
	}
	return ri.Error()
}

func divideAndSendRangeFeedToRanges(
	ctx context.Context,
	ds *DistSender,
	rs roachpb.RSpan,
	startFrom hlc.Timestamp,
	rangeCh chan<- singleRangeInfo,
) error {
	return divideAndIterateRSpan(
		ctx, ds, rs,
		func(rs roachpb.RSpan, token rangecache.EvictionToken) error {
			select {
			case rangeCh <- singleRangeInfo{
				rs:        rs,
				startFrom: startFrom,
				token:     token,
			}:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
}

// partialRangeFeed establishes a RangeFeed to the range specified by desc. It
// manages lifecycle events of the range in order to maintain the RangeFeed
// connection; this may involve instructing higher-level functions to retry
// this rangefeed, or subdividing the range further in the event of a split.
func (ds *DistSender) partialRangeFeed(
	ctx context.Context,
	rr *rangeFeedRegistry,
	rs roachpb.RSpan,
	startFrom hlc.Timestamp,
	token rangecache.EvictionToken,
	withDiff bool,
	rangeCh chan<- singleRangeInfo,
	eventCh chan<- *roachpb.RangeFeedEvent,
) error {
	// Bound the partial rangefeed to the partial span.
	span := rs.AsRawSpanWithNoLocals()

	// Register partial range feed with registry.
	var active *activeRangeFeed
	defer func() {
		if active != nil {
			active.release()
		}
	}()

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

		if active == nil {
			active = newActiveRangeFeed(rr, token.Desc().RangeID, span, startFrom)
		}

		// Establish a RangeFeed for a single Range.
		maxTS, err := ds.singleRangeFeed(ctx, span, startFrom, withDiff, token.Desc(), active, eventCh)

		// Forward the timestamp in case we end up sending it again.
		startFrom.Forward(maxTS)

		if err != nil {
			if log.V(1) {
				log.Infof(ctx, "RangeFeed %s disconnected with last checkpoint %s ago: %v",
					span, timeutil.Since(startFrom.GoTime()), err)
			}
			errDisposition, err := handleRangeError(err)
			if err != nil {
				return err
			}

			switch errDisposition {
			case restartRange:
				// Evict the descriptor from the cache.
				token.Evict(ctx)
				return divideAndSendRangeFeedToRanges(ctx, ds, rs, startFrom, rangeCh)
			case retryRange:
				// Nothing -- fallback to retry loop.
			default:
				panic("unexpected error disposition")
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
	startFrom hlc.Timestamp,
	withDiff bool,
	desc *roachpb.RangeDescriptor,
	active *activeRangeFeed,
	eventCh chan<- *roachpb.RangeFeedEvent,
) (hlc.Timestamp, error) {
	args := roachpb.RangeFeedRequest{
		Span: span,
		Header: roachpb.Header{
			Timestamp: startFrom,
			RangeID:   desc.RangeID,
		},
		WithDiff: withDiff,
	}

	transport, replicas, err := ds.prepareTransportForDescriptor(ctx, desc)
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
		active.setNodeID(args.Replica.NodeID)

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
			active.onRangeEvent(event)

			select {
			case eventCh <- event:
			case <-ctx.Done():
				return args.Timestamp, ctx.Err()
			}
		}
	}
}

type errorDisposition int

const (
	// abortRange is a sentinel indicating rangefeed should be aborted because
	// of an error.
	abortRange errorDisposition = iota

	// restartRange indicates that the rangefeed for the range should be restarted.
	// this includes updating routing information, splitting range on range boundaries
	// and re-establishing rangefeeds for 1 or more ranges.
	restartRange

	// retryRange indicates that the rangefeed should be simply retried.
	retryRange
)

func (d errorDisposition) String() string {
	switch d {
	case restartRange:
		return "restart"
	case retryRange:
		return "retry"
	default:
		return "abort"
	}
}

// handleRangeError classifies rangefeed error and returns error disposition to the caller
// indicating how such error should be handled.
func handleRangeError(err error) (errorDisposition, error) {
	switch {
	case errors.HasType(err, (*roachpb.StoreNotFoundError)(nil)) ||
		errors.HasType(err, (*roachpb.NodeUnavailableError)(nil)):
		// These errors are likely to be unique to the replica that
		// reported them, so no action is required before the next
		// retry.
		return retryRange, nil
	case IsSendError(err), errors.HasType(err, (*roachpb.RangeNotFoundError)(nil)):
		return restartRange, nil
	case errors.HasType(err, (*roachpb.RangeKeyMismatchError)(nil)):
		return restartRange, nil
	case errors.HasType(err, (*roachpb.RangeFeedRetryError)(nil)):
		var t *roachpb.RangeFeedRetryError
		if ok := errors.As(err, &t); !ok {
			return abortRange, errors.AssertionFailedf("wrong error type: %T", err)
		}
		switch t.Reason {
		case roachpb.RangeFeedRetryError_REASON_REPLICA_REMOVED,
			roachpb.RangeFeedRetryError_REASON_RAFT_SNAPSHOT,
			roachpb.RangeFeedRetryError_REASON_LOGICAL_OPS_MISSING,
			roachpb.RangeFeedRetryError_REASON_SLOW_CONSUMER:
			// Try again with same descriptor. These are transient
			// errors that should not show up again.
			return retryRange, nil
		case roachpb.RangeFeedRetryError_REASON_RANGE_SPLIT,
			roachpb.RangeFeedRetryError_REASON_RANGE_MERGED,
			roachpb.RangeFeedRetryError_REASON_NO_LEASEHOLDER:
			return restartRange, nil
		default:
			return abortRange, errors.AssertionFailedf("unrecognized retryable error type: %T", err)
		}
	default:
		return abortRange, err
	}
}

// prepareTransportForDescriptor creates and configures RPC transport for the specified
// descriptor.  Returns the transport, which the caller is expected to release along with the
// replicas slice used to configure the transport.
func (ds *DistSender) prepareTransportForDescriptor(
	ctx context.Context, desc *roachpb.RangeDescriptor,
) (Transport, ReplicaSlice, error) {
	var latencyFn LatencyFunc
	if ds.rpcContext != nil {
		latencyFn = ds.rpcContext.RemoteClocks.Latency
	}
	replicas, err := NewReplicaSlice(ctx, ds.nodeDescs, desc, nil, AllExtantReplicas)
	if err != nil {
		return nil, nil, err
	}
	replicas.OptimizeReplicaOrder(ds.getNodeDescriptor(), latencyFn)
	// The RangeFeed is not used for system critical traffic so use a DefaultClass
	// connection regardless of the range.
	opts := SendOptions{class: connectionClass(&ds.st.SV)}
	transport, err := ds.transportFactory(opts, ds.nodeDialer, replicas)
	if err != nil {
		return nil, nil, err
	}
	return transport, replicas, nil
}

func connectionClass(sv *settings.Values) rpc.ConnectionClass {
	if useDedicatedRangefeedConnectionClass.Get(sv) {
		return rpc.RangefeedClass
	}
	return rpc.DefaultClass
}
