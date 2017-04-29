// Copyright 2014 The Cockroach Authors.
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
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package kv

import (
	"fmt"
	"sync/atomic"
	"time"
	"unsafe"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/shuffle"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// Default constants for timeouts.
const (
	defaultClientTimeout     = 10 * time.Second
	defaultPendingRPCTimeout = 500 * time.Millisecond

	// The default maximum number of ranges to return from a range
	// lookup.
	defaultRangeLookupMaxRanges = 8
	// The default size of the range lease holder cache.
	defaultLeaseHolderCacheSize = 1 << 16
	// The default size of the range descriptor cache.
	defaultRangeDescriptorCacheSize = 1 << 20
	// The default limit for asynchronous senders.
	defaultSenderConcurrency = 500
)

var (
	metaDistSenderBatchCount = metric.Metadata{
		Name: "distsender.batches",
		Help: "Number of batches processed"}
	metaDistSenderPartialBatchCount = metric.Metadata{
		Name: "distsender.batches.partial",
		Help: "Number of partial batches processed"}
	metaTransportSentCount = metric.Metadata{
		Name: "distsender.rpc.sent",
		Help: "Number of RPCs sent"}
	metaTransportLocalSentCount = metric.Metadata{
		Name: "distsender.rpc.sent.local",
		Help: "Number of local RPCs sent"}
	metaDistSenderSendNextTimeoutCount = metric.Metadata{
		Name: "distsender.rpc.sent.sendnexttimeout",
		Help: "Number of RPCs sent due to outstanding RPCs not returning promptly"}
	metaDistSenderNextReplicaErrCount = metric.Metadata{
		Name: "distsender.rpc.sent.nextreplicaerror",
		Help: "Number of RPCs sent due to per-replica errors"}
	metaDistSenderNotLeaseHolderErrCount = metric.Metadata{
		Name: "distsender.errors.notleaseholder",
		Help: "Number of NotLeaseHolderErrors encountered"}
	metaSlowDistSenderRequests = metric.Metadata{
		Name: "requests.slow.distsender",
		Help: "Number of requests that have been stuck for a long time in the dist sender"}
)

// DistSenderMetrics is the set of metrics for a given distributed sender.
type DistSenderMetrics struct {
	BatchCount             *metric.Counter
	PartialBatchCount      *metric.Counter
	SentCount              *metric.Counter
	LocalSentCount         *metric.Counter
	SendNextTimeoutCount   *metric.Counter
	NextReplicaErrCount    *metric.Counter
	NotLeaseHolderErrCount *metric.Counter
	SlowRequestsCount      *metric.Gauge
}

func makeDistSenderMetrics() DistSenderMetrics {
	return DistSenderMetrics{
		BatchCount:             metric.NewCounter(metaDistSenderBatchCount),
		PartialBatchCount:      metric.NewCounter(metaDistSenderPartialBatchCount),
		SentCount:              metric.NewCounter(metaTransportSentCount),
		LocalSentCount:         metric.NewCounter(metaTransportLocalSentCount),
		SendNextTimeoutCount:   metric.NewCounter(metaDistSenderSendNextTimeoutCount),
		NextReplicaErrCount:    metric.NewCounter(metaDistSenderNextReplicaErrCount),
		NotLeaseHolderErrCount: metric.NewCounter(metaDistSenderNotLeaseHolderErrCount),
		SlowRequestsCount:      metric.NewGauge(metaSlowDistSenderRequests),
	}
}

// A firstRangeMissingError indicates that the first range has not yet
// been gossiped. This will be the case for a node which hasn't yet
// joined the gossip network.
type firstRangeMissingError struct{}

// Error implements the error interface.
func (f firstRangeMissingError) Error() string {
	return "the descriptor for the first range is not available via gossip"
}

// A DistSender provides methods to access Cockroach's monolithic,
// distributed key value store. Each method invocation triggers a
// lookup or lookups to find replica metadata for implicated key
// ranges. RPCs are sent to one or more of the replicas to satisfy
// the method invocation.
type DistSender struct {
	log.AmbientContext

	// nodeDescriptor, if set, holds the descriptor of the node the
	// DistSender lives on. It should be accessed via getNodeDescriptor(),
	// which tries to obtain the value from the Gossip network if the
	// descriptor is unknown.
	nodeDescriptor unsafe.Pointer
	// clock is used to set time for some calls. E.g. read-only ops
	// which span ranges and don't require read consistency.
	clock *hlc.Clock
	// gossip provides up-to-date information about the start of the
	// key range, used to find the replica metadata for arbitrary key
	// ranges.
	gossip  *gossip.Gossip
	metrics DistSenderMetrics
	// rangeCache caches replica metadata for key ranges.
	rangeCache           *RangeDescriptorCache
	rangeLookupMaxRanges int32
	// leaseHolderCache caches range lease holders by range ID.
	leaseHolderCache *LeaseHolderCache
	transportFactory TransportFactory
	rpcContext       *rpc.Context
	rpcRetryOptions  retry.Options
	sendNextTimeout  time.Duration
	asyncSenderSem   chan struct{}
	asyncSenderCount int32
}

var _ client.Sender = &DistSender{}

// DistSenderConfig holds configuration and auxiliary objects that can be passed
// to NewDistSender.
type DistSenderConfig struct {
	AmbientCtx log.AmbientContext

	Clock                    *hlc.Clock
	RangeDescriptorCacheSize int32
	// RangeLookupMaxRanges sets how many ranges will be prefetched into the
	// range descriptor cache when dispatching a range lookup request.
	RangeLookupMaxRanges int32
	LeaseHolderCacheSize int32
	RPCRetryOptions      *retry.Options
	// nodeDescriptor, if provided, is used to describe which node the DistSender
	// lives on, for instance when deciding where to send RPCs.
	// Usually it is filled in from the Gossip network on demand.
	nodeDescriptor *roachpb.NodeDescriptor
	// The RPC dispatcher. Defaults to grpc but can be changed here for testing
	// purposes.
	TransportFactory  TransportFactory
	RPCContext        *rpc.Context
	RangeDescriptorDB RangeDescriptorDB
	SendNextTimeout   time.Duration
	// SenderConcurrency specifies the parallelization available when
	// splitting batches into multiple requests when they span ranges.
	// TODO(spencer): This is per-process. We should add a per-batch limit.
	SenderConcurrency int32
}

// NewDistSender returns a batch.Sender instance which connects to the
// Cockroach cluster via the supplied gossip instance. Supplying a
// DistSenderContext or the fields within is optional. For omitted values, sane
// defaults will be used.
func NewDistSender(cfg DistSenderConfig, g *gossip.Gossip) *DistSender {
	ds := &DistSender{
		clock:   cfg.Clock,
		gossip:  g,
		metrics: makeDistSenderMetrics(),
	}

	ds.AmbientContext = cfg.AmbientCtx
	if ds.AmbientContext.Tracer == nil {
		ds.AmbientContext.Tracer = tracing.NewTracer()
	}

	if cfg.nodeDescriptor != nil {
		atomic.StorePointer(&ds.nodeDescriptor, unsafe.Pointer(cfg.nodeDescriptor))
	}
	rcSize := cfg.RangeDescriptorCacheSize
	if rcSize <= 0 {
		rcSize = defaultRangeDescriptorCacheSize
	}
	rdb := cfg.RangeDescriptorDB
	if rdb == nil {
		rdb = ds
	}
	ds.rangeCache = NewRangeDescriptorCache(rdb, int(rcSize))
	lcSize := cfg.LeaseHolderCacheSize
	if lcSize <= 0 {
		lcSize = defaultLeaseHolderCacheSize
	}
	ds.leaseHolderCache = NewLeaseHolderCache(int(lcSize))
	if cfg.RangeLookupMaxRanges <= 0 {
		ds.rangeLookupMaxRanges = defaultRangeLookupMaxRanges
	}
	if cfg.TransportFactory != nil {
		ds.transportFactory = cfg.TransportFactory
	}
	ds.rpcRetryOptions = base.DefaultRetryOptions()
	if cfg.RPCRetryOptions != nil {
		ds.rpcRetryOptions = *cfg.RPCRetryOptions
	}
	if cfg.RPCContext != nil {
		ds.rpcContext = cfg.RPCContext
		if ds.rpcRetryOptions.Closer == nil {
			ds.rpcRetryOptions.Closer = ds.rpcContext.Stopper.ShouldQuiesce()
		}
	}
	if cfg.SendNextTimeout != 0 {
		ds.sendNextTimeout = cfg.SendNextTimeout
	} else {
		ds.sendNextTimeout = base.DefaultSendNextTimeout
	}
	if cfg.SenderConcurrency != 0 {
		ds.asyncSenderSem = make(chan struct{}, cfg.SenderConcurrency)
	} else {
		ds.asyncSenderSem = make(chan struct{}, defaultSenderConcurrency)
	}

	if g != nil {
		ctx := ds.AnnotateCtx(context.Background())
		g.RegisterCallback(gossip.KeyFirstRangeDescriptor,
			func(_ string, value roachpb.Value) {
				if log.V(1) {
					var desc roachpb.RangeDescriptor
					if err := value.GetProto(&desc); err != nil {
						log.Errorf(ctx, "unable to parse gossiped first range descriptor: %s", err)
					} else {
						log.Infof(ctx, "gossiped first range descriptor: %+v", desc.Replicas)
					}
				}
				err := ds.rangeCache.EvictCachedRangeDescriptor(ctx, roachpb.RKeyMin, nil, false)
				if err != nil {
					log.Warningf(ctx, "failed to evict first range descriptor: %s", err)
				}
			})
	}
	return ds
}

// Metrics returns a struct which contains metrics related to the distributed
// sender's activity.
func (ds *DistSender) Metrics() DistSenderMetrics {
	return ds.metrics
}

// GetParallelSendCount returns the number of parallel batch requests
// the dist sender has dispatched in its lifetime.
func (ds *DistSender) GetParallelSendCount() int32 {
	return atomic.LoadInt32(&ds.asyncSenderCount)
}

// RangeDescriptorCache gives access to the DistSender's range cache.
func (ds *DistSender) RangeDescriptorCache() *RangeDescriptorCache {
	return ds.rangeCache
}

// LeaseHolderCache gives access to the DistSender's lease cache.
func (ds *DistSender) LeaseHolderCache() *LeaseHolderCache {
	return ds.leaseHolderCache
}

// RangeLookup implements the RangeDescriptorDB interface.
// RangeLookup dispatches a RangeLookup request for the given metadata
// key to the replicas of the given range. Note that we allow
// inconsistent reads when doing range lookups for efficiency. Getting
// stale data is not a correctness problem but instead may
// infrequently result in additional latency as additional range
// lookups may be required. Note also that rangeLookup bypasses the
// DistSender's Send() method, so there is no error inspection and
// retry logic here; this is not an issue since the lookup performs a
// single inconsistent read only.
func (ds *DistSender) RangeLookup(
	ctx context.Context, key roachpb.RKey, desc *roachpb.RangeDescriptor, useReverseScan bool,
) ([]roachpb.RangeDescriptor, []roachpb.RangeDescriptor, *roachpb.Error) {
	ba := roachpb.BatchRequest{}
	ba.ReadConsistency = roachpb.INCONSISTENT
	ba.Add(&roachpb.RangeLookupRequest{
		Span: roachpb.Span{
			// We can interpret the RKey as a Key here since it's a metadata
			// lookup; those are never local.
			Key: key.AsRawKey(),
		},
		MaxRanges: ds.rangeLookupMaxRanges,
		Reverse:   useReverseScan,
	})
	replicas := NewReplicaSlice(ds.gossip, desc)
	shuffle.Shuffle(replicas)
	br, err := ds.sendRPC(ctx, desc.RangeID, replicas, ba)
	if err != nil {
		return nil, nil, roachpb.NewError(err)
	}
	if br.Error != nil {
		return nil, nil, br.Error
	}
	resp := br.Responses[0].GetInner().(*roachpb.RangeLookupResponse)
	return resp.Ranges, resp.PrefetchedRanges, nil
}

// FirstRange implements the RangeDescriptorDB interface.
// FirstRange returns the RangeDescriptor for the first range on the cluster,
// which is retrieved from the gossip protocol instead of the datastore.
func (ds *DistSender) FirstRange() (*roachpb.RangeDescriptor, error) {
	if ds.gossip == nil {
		panic("with `nil` Gossip, DistSender must not use itself as rangeDescriptorDB")
	}
	rangeDesc := &roachpb.RangeDescriptor{}
	if err := ds.gossip.GetInfoProto(gossip.KeyFirstRangeDescriptor, rangeDesc); err != nil {
		return nil, firstRangeMissingError{}
	}
	return rangeDesc, nil
}

// getNodeDescriptor returns ds.nodeDescriptor, but makes an attempt to load
// it from the Gossip network if a nil value is found.
// We must jump through hoops here to get the node descriptor because it's not available
// until after the node has joined the gossip network and been allowed to initialize
// its stores.
func (ds *DistSender) getNodeDescriptor() *roachpb.NodeDescriptor {
	if desc := atomic.LoadPointer(&ds.nodeDescriptor); desc != nil {
		return (*roachpb.NodeDescriptor)(desc)
	}
	if ds.gossip == nil {
		return nil
	}

	ownNodeID := ds.gossip.NodeID.Get()
	if ownNodeID > 0 {
		// TODO(tschottdorf): Consider instead adding the NodeID of the
		// coordinator to the header, so we can get this from incoming
		// requests. Just in case we want to mostly eliminate gossip here.
		nodeDesc := &roachpb.NodeDescriptor{}
		if err := ds.gossip.GetInfoProto(gossip.MakeNodeIDKey(ownNodeID), nodeDesc); err == nil {
			atomic.StorePointer(&ds.nodeDescriptor, unsafe.Pointer(nodeDesc))
			return nodeDesc
		}
	}
	ctx := ds.AnnotateCtx(context.TODO())
	log.Infof(ctx, "unable to determine this node's attributes for replica "+
		"selection; node is most likely bootstrapping")
	return nil
}

// sendRPC sends one or more RPCs to replicas from the supplied
// roachpb.Replica slice. Returns an RPC error if the request could
// not be sent. Note that the reply may contain a higher level error
// and must be checked in addition to the RPC error.
//
// The replicas are assumed to be ordered by preference, with closer
// ones (i.e. expected lowest latency) first.
func (ds *DistSender) sendRPC(
	ctx context.Context, rangeID roachpb.RangeID, replicas ReplicaSlice, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, error) {
	if len(replicas) == 0 {
		return nil, roachpb.NewSendError(
			fmt.Sprintf("no replica node addresses available via gossip for r%d", rangeID))
	}

	// TODO(pmattis): This needs to be tested. If it isn't set we'll
	// still route the request appropriately by key, but won't receive
	// RangeNotFoundErrors.
	ba.RangeID = rangeID

	// A given RPC may generate retries to multiple replicas, but as soon as we
	// get a response from one we want to cancel those other RPCs.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Set RPC opts with stipulation that one of N RPCs must succeed.
	rpcOpts := SendOptions{
		SendNextTimeout:  ds.sendNextTimeout,
		transportFactory: ds.transportFactory,
		metrics:          &ds.metrics,
	}
	tracing.AnnotateTrace()
	defer tracing.AnnotateTrace()

	reply, err := ds.sendToReplicas(ctx, rpcOpts, rangeID, replicas, ba, ds.rpcContext)
	if err != nil {
		return nil, err
	}
	return reply, nil
}

// CountRanges returns the number of ranges that encompass the given key span.
func (ds *DistSender) CountRanges(ctx context.Context, rs roachpb.RSpan) (int64, error) {
	var count int64
	ri := NewRangeIterator(ds)
	for ri.Seek(ctx, rs.Key, Ascending); ri.Valid(); ri.Next(ctx) {
		count++
		if !ri.NeedAnother(rs) {
			break
		}
	}
	return count, ri.Error().GoError()
}

// getDescriptor looks up the range descriptor to use for a query of
// the key descKey with the given options. The lookup takes into
// consideration the last range descriptor that the caller had used
// for this key span, if any, and if the last range descriptor has
// been evicted because it was found to be stale, which is all managed
// through the EvictionToken. The function should be provided with an
// EvictionToken if one was acquired from this function on a previous
// call. If not, an empty EvictionToken can be provided.
//
// The range descriptor which contains the range in which the request should
// start its query is returned first. Next returned is an EvictionToken. In
// case the descriptor is discovered stale, the returned EvictionToken's evict
// method should be called; it evicts the cache appropriately.
func (ds *DistSender) getDescriptor(
	ctx context.Context, descKey roachpb.RKey, evictToken *EvictionToken, useReverseScan bool,
) (*roachpb.RangeDescriptor, *EvictionToken, error) {
	desc, returnToken, err := ds.rangeCache.LookupRangeDescriptor(
		ctx, descKey, evictToken, useReverseScan,
	)
	if err != nil {
		return nil, returnToken, err
	}

	return desc, returnToken, nil
}

// sendSingleRange gathers and rearranges the replicas, and makes an RPC call.
func (ds *DistSender) sendSingleRange(
	ctx context.Context, ba roachpb.BatchRequest, desc *roachpb.RangeDescriptor,
) (*roachpb.BatchResponse, *roachpb.Error) {
	// Try to send the call.
	replicas := NewReplicaSlice(ds.gossip, desc)

	// Rearrange the replicas so that those replicas with long common
	// prefix of attributes end up first. If there's no prefix, this is a
	// no-op.
	replicas.OptimizeReplicaOrder(ds.getNodeDescriptor())

	// If this request needs to go to a lease holder and we know who that is, move
	// it to the front.
	if !(ba.IsReadOnly() && ba.ReadConsistency == roachpb.INCONSISTENT) {
		if leaseHolder, ok := ds.leaseHolderCache.Lookup(ctx, desc.RangeID); ok {
			if i := replicas.FindReplica(leaseHolder.StoreID); i >= 0 {
				replicas.MoveToFront(i)
			}
		}
	}

	// TODO(tschottdorf): should serialize the trace here, not higher up.
	br, err := ds.sendRPC(ctx, desc.RangeID, replicas, ba)
	if err != nil {
		return nil, roachpb.NewError(err)
	}

	// If the reply contains a timestamp, update the local HLC with it.
	if br.Error != nil && br.Error.Now != (hlc.Timestamp{}) {
		ds.clock.Update(br.Error.Now)
	} else if br.Now != (hlc.Timestamp{}) {
		ds.clock.Update(br.Now)
	}

	// Untangle the error from the received response.
	pErr := br.Error
	br.Error = nil // scrub the response error
	return br, pErr
}

// initAndVerifyBatch initializes timestamp-related information and
// verifies batch constraints before splitting.
func (ds *DistSender) initAndVerifyBatch(
	ctx context.Context, ba *roachpb.BatchRequest,
) *roachpb.Error {
	// Attach the local node ID to each request.
	if ba.Header.GatewayNodeID == 0 && ds.gossip != nil {
		ba.Header.GatewayNodeID = ds.gossip.NodeID.Get()
	}

	// In the event that timestamp isn't set and read consistency isn't
	// required, set the timestamp using the local clock.
	if ba.ReadConsistency == roachpb.INCONSISTENT && ba.Timestamp == (hlc.Timestamp{}) {
		ba.Timestamp = ds.clock.Now()
	}

	if ba.Txn != nil {
		// Make a copy here since the code below modifies it in different places.
		// TODO(tschottdorf): be smarter about this - no need to do it for
		// requests that don't get split.
		txnClone := ba.Txn.Clone()
		ba.Txn = &txnClone

		if len(ba.Txn.ObservedTimestamps) == 0 {
			// Ensure the local NodeID is marked as free from clock offset;
			// the transaction's timestamp was taken off the local clock.
			// TODO(andrei): This is broken when Txn.OrigTimestamp has not been, in
			// fact, taken off this node's clock. This happens when the transaction
			// was created remotely and is being run through the ExternalClient. I
			// think we shold move this initialization to client.Txn.
			if nDesc := ds.getNodeDescriptor(); nDesc != nil {
				// TODO(tschottdorf): future refactoring should move this to txn
				// creation in TxnCoordSender, which is currently unaware of the
				// NodeID (and wraps *DistSender through client.Sender since it
				// also needs test compatibility with *LocalSender).
				//
				// Taking care below to not modify any memory referenced from
				// our BatchRequest which may be shared with others.
				//
				// We already have a clone of our txn (see above), so we can
				// modify it freely.
				//
				// Zero the existing data. That makes sure that if we had
				// something of size zero but with capacity, we don't re-use the
				// existing space (which others may also use). This is just to
				// satisfy paranoia/OCD and not expected to matter in practice.
				ba.Txn.ResetObservedTimestamps()
				// OrigTimestamp is the HLC timestamp at which the Txn started, so
				// this effectively means no more uncertainty on this node.
				ba.Txn.UpdateObservedTimestamp(nDesc.NodeID, ba.Txn.OrigTimestamp)
			}
		}
	}

	if len(ba.Requests) < 1 {
		return roachpb.NewErrorf("empty batch")
	}

	if ba.MaxSpanRequestKeys != 0 {
		// Verify that the batch contains only specific range requests or the
		// Begin/EndTransactionRequest. Verify that a batch with a ReverseScan
		// only contains ReverseScan range requests.
		isReverse := ba.IsReverse()
		for _, req := range ba.Requests {
			inner := req.GetInner()
			switch inner.(type) {
			case *roachpb.ScanRequest, *roachpb.DeleteRangeRequest:
				// Accepted range requests. All other range requests are still
				// not supported.
				// TODO(vivek): don't enumerate all range requests.
				if isReverse {
					return roachpb.NewErrorf("batch with limit contains both forward and reverse scans")
				}

			case *roachpb.BeginTransactionRequest, *roachpb.EndTransactionRequest, *roachpb.ReverseScanRequest:
				continue

			default:
				return roachpb.NewErrorf("batch with limit contains %T request", inner)
			}
		}
	}

	return nil
}

// errNo1PCTxn indicates that a batch cannot be sent as a 1 phase
// commit because it spans multiple ranges and must be split into at
// least two parts, with the final part containing the EndTransaction
// request.
var errNo1PCTxn = roachpb.NewErrorf("cannot send 1PC txn to multiple ranges")

// Send implements the batch.Sender interface. It subdivides the Batch
// into batches admissible for sending (preventing certain illegal
// mixtures of requests), executes each individual part (which may
// span multiple ranges), and recombines the response.
//
// When the request spans ranges, it is split by range and a partial
// subset of the batch request is sent to affected ranges in parallel.
//
// The first write in a transaction may not arrive before writes to
// other ranges. This is relevant in the case of a BeginTransaction
// request. Intents written to other ranges before the transaction
// record is created will cause the transaction to abort early.
func (ds *DistSender) Send(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	ds.metrics.BatchCount.Inc(1)

	tracing.AnnotateTrace()

	if pErr := ds.initAndVerifyBatch(ctx, &ba); pErr != nil {
		return nil, pErr
	}

	ctx = ds.AnnotateCtx(ctx)
	ctx, cleanup := tracing.EnsureContext(ctx, ds.AmbientContext.Tracer, "dist sender")
	defer cleanup()

	var rplChunks []*roachpb.BatchResponse
	parts := ba.Split(false /* don't split ET */)
	if len(parts) > 1 && ba.MaxSpanRequestKeys != 0 {
		// We already verified above that the batch contains only scan requests of the same type.
		// Such a batch should never need splitting.
		panic("batch with MaxSpanRequestKeys needs splitting")
	}
	for len(parts) > 0 {
		part := parts[0]
		ba.Requests = part
		// The minimal key range encompassing all requests contained within.
		// Local addressing has already been resolved.
		// TODO(tschottdorf): consider rudimentary validation of the batch here
		// (for example, non-range requests with EndKey, or empty key ranges).
		rs, err := keys.Range(ba)
		if err != nil {
			return nil, roachpb.NewError(err)
		}
		rpl, pErr := ds.divideAndSendBatchToRanges(ctx, ba, rs, 0 /* batchIdx */)

		if pErr == errNo1PCTxn {
			// If we tried to send a single round-trip EndTransaction but
			// it looks like it's going to hit multiple ranges, split it
			// here and try again.
			if len(parts) != 1 {
				panic("EndTransaction not in last chunk of batch")
			}
			parts = ba.Split(true /* split ET */)
			if len(parts) != 2 {
				panic("split of final EndTransaction chunk resulted in != 2 parts")
			}
			continue
		}
		if pErr != nil {
			return nil, pErr
		}
		// Propagate transaction from last reply to next request. The final
		// update is taken and put into the response's main header.
		ba.UpdateTxn(rpl.Txn)
		rplChunks = append(rplChunks, rpl)
		parts = parts[1:]
	}

	reply := rplChunks[0]
	for _, rpl := range rplChunks[1:] {
		reply.Responses = append(reply.Responses, rpl.Responses...)
		reply.CollectedSpans = append(reply.CollectedSpans, rpl.CollectedSpans...)
	}
	reply.BatchResponse_Header = rplChunks[len(rplChunks)-1].BatchResponse_Header
	return reply, nil
}

type response struct {
	reply *roachpb.BatchResponse
	pErr  *roachpb.Error
}

// divideAndSendBatchToRanges sends the supplied batch to all of the
// ranges which comprise the span specified by rs. The batch request
// is trimmed against each range which is part of the span and sent
// either serially or in parallel, if possible. batchIdx indicates
// which partial fragment of the larger batch is being processed by
// this method. It's specified as non-zero when this method is invoked
// recursively.
func (ds *DistSender) divideAndSendBatchToRanges(
	ctx context.Context, ba roachpb.BatchRequest, rs roachpb.RSpan, batchIdx int,
) (br *roachpb.BatchResponse, pErr *roachpb.Error) {
	// This function builds a channel of responses for each range
	// implicated in the span (rs) and combines them into a single
	// BatchResponse when finished.
	var responseChs []chan response
	defer func() {
		if r := recover(); r != nil {
			// If we're in the middle of a panic, don't wait on responseChs.
			panic(r)
		}
		for _, responseCh := range responseChs {
			resp := <-responseCh
			if resp.pErr != nil {
				if pErr == nil {
					pErr = resp.pErr
				}
				continue
			}
			if br == nil {
				// First response from a Range.
				br = resp.reply
			} else {
				// This was the second or later call in a cross-Range request.
				// Combine the new response with the existing one.
				if err := br.Combine(resp.reply); err != nil {
					pErr = roachpb.NewError(err)
					return
				}
				br.Txn.Update(resp.reply.Txn)
			}
		}

		// If we experienced an error, don't neglect to update the error's
		// attached transaction with any responses which were received.
		if pErr != nil {
			if br != nil {
				pErr.UpdateTxn(br.Txn)
			}
		}
	}()

	// Get initial seek key depending on direction of iteration.
	var seekKey roachpb.RKey
	var scanDir ScanDirection
	if !ba.IsReverse() {
		scanDir = Ascending
		seekKey = rs.Key
	} else {
		scanDir = Descending
		seekKey = rs.EndKey
	}
	// Send the request to one range per iteration.
	ri := NewRangeIterator(ds)
	for ri.Seek(ctx, seekKey, scanDir); ri.Valid(); ri.Seek(ctx, seekKey, scanDir) {
		// Increase the sequence counter only once before sending RPCs to
		// the ranges involved in this chunk of the batch (as opposed to
		// for each RPC individually). On RPC errors, there's no guarantee
		// that the request hasn't made its way to the target regardless
		// of the error; we'd like the second execution to be caught by
		// the sequence cache if that happens. There is a small chance
		// that we address a range twice in this chunk (stale/suboptimal
		// descriptors due to splits/merges) which leads to a transaction
		// retry.
		//
		// TODO(tschottdorf): it's possible that if we don't evict from
		// the cache we could be in for a busy loop.
		ba.SetNewRequest()

		responseCh := make(chan response, 1)
		responseChs = append(responseChs, responseCh)

		if batchIdx == 0 && ri.NeedAnother(rs) {
			// TODO(tschottdorf): we should have a mechanism for discovering
			// range merges (descriptor staleness will mostly go unnoticed),
			// or we'll be turning single-range queries into multi-range
			// queries for no good reason.
			//
			// If there's no transaction and op spans ranges, possibly
			// re-run as part of a transaction for consistency. The
			// case where we don't need to re-run is if the read
			// consistency is not required.
			if ba.Txn == nil && ba.IsPossibleTransaction() && ba.ReadConsistency != roachpb.INCONSISTENT {
				responseCh <- response{pErr: roachpb.NewError(&roachpb.OpRequiresTxnError{})}
				return
			}
			// If the request is more than but ends with EndTransaction, we
			// want the caller to come again with the EndTransaction in an
			// extra call.
			if l := len(ba.Requests) - 1; l > 0 && ba.Requests[l].GetInner().Method() == roachpb.EndTransaction {
				responseCh <- response{pErr: errNo1PCTxn}
				return
			}
		}

		// Determine next seek key, taking a potentially sparse batch into
		// consideration.
		var err error
		nextRS := rs
		if scanDir == Descending {
			// In next iteration, query previous range.
			// We use the StartKey of the current descriptor as opposed to the
			// EndKey of the previous one since that doesn't have bugs when
			// stale descriptors come into play.
			seekKey, err = prev(ba, ri.Desc().StartKey)
			nextRS.EndKey = seekKey
		} else {
			// In next iteration, query next range.
			// It's important that we use the EndKey of the current descriptor
			// as opposed to the StartKey of the next one: if the former is stale,
			// it's possible that the next range has since merged the subsequent
			// one, and unless both descriptors are stale, the next descriptor's
			// StartKey would move us to the beginning of the current range,
			// resulting in a duplicate scan.
			seekKey, err = next(ba, ri.Desc().EndKey)
			nextRS.Key = seekKey
		}
		if err != nil {
			responseCh <- response{pErr: roachpb.NewError(err)}
			return
		}

		// Send the next partial batch to the first range in the "rs" span.
		// If we're not handling a request which limits responses and we
		// can reserve one of the limited goroutines available for parallel
		// batch RPCs, send asynchronously.
		if ba.MaxSpanRequestKeys == 0 && ri.NeedAnother(rs) && ds.rpcContext != nil &&
			ds.sendPartialBatchAsync(ctx, ba, rs, ri.Desc(), ri.Token(), batchIdx, responseCh) {
			// Note that we pass the batch request by value to the parallel
			// goroutine to avoid using the cloned txn.

			// Clone the txn to preserve the current txn sequence for the async call.
			if ba.Txn != nil {
				txnClone := ba.Txn.Clone()
				ba.Txn = &txnClone
			}
		} else {
			// Send synchronously if there is no parallel capacity left, there's a
			// max results limit, or this is the final request in the span.
			resp := ds.sendPartialBatch(ctx, ba, rs, ri.Desc(), ri.Token(), batchIdx)
			responseCh <- resp
			if resp.pErr != nil {
				return
			}
			ba.UpdateTxn(resp.reply.Txn)

			// Check whether we've received enough responses to exit query loop.
			if ba.MaxSpanRequestKeys > 0 {
				var numResults int64
				for _, r := range resp.reply.Responses {
					numResults += r.GetInner().Header().NumKeys
				}
				if numResults > ba.MaxSpanRequestKeys {
					panic(fmt.Sprintf("received %d results, limit was %d", numResults, ba.MaxSpanRequestKeys))
				}
				ba.MaxSpanRequestKeys -= numResults
				// Exiting; fill in missing responses.
				if ba.MaxSpanRequestKeys == 0 {
					fillSkippedResponses(ba, resp.reply, seekKey)
					return
				}
			}
		}

		// The iteration is complete if the iterator's current range
		// encompasses the remaining span, OR if the next span has
		// inverted. This can happen if this method is invoked
		// re-entrantly due to ranges being split or merged. In that case
		// the batch request has all the original requests but the span is
		// a sub-span of the original, causing next() and prev() methods
		// to potentially return values which invert the span.
		if !ri.NeedAnother(rs) || !nextRS.Key.Less(nextRS.EndKey) {
			return
		}
		batchIdx++
		rs = nextRS
	}

	// We've exited early. Return the range iterator error.
	responseCh := make(chan response, 1)
	responseCh <- response{pErr: ri.Error()}
	responseChs = append(responseChs, responseCh)
	return
}

// sendPartialBatchAsync sends the partial batch asynchronously if
// there aren't currently more than the allowed number of concurrent
// async requests outstanding. Returns whether the partial batch was
// sent.
func (ds *DistSender) sendPartialBatchAsync(
	ctx context.Context,
	ba roachpb.BatchRequest,
	rs roachpb.RSpan,
	desc *roachpb.RangeDescriptor,
	evictToken *EvictionToken,
	batchIdx int,
	responseCh chan response,
) bool {
	if err := ds.rpcContext.Stopper.RunLimitedAsyncTask(
		ctx, ds.asyncSenderSem, false /* !wait */, func(ctx context.Context) {
			atomic.AddInt32(&ds.asyncSenderCount, 1)
			responseCh <- ds.sendPartialBatch(ctx, ba, rs, desc, evictToken, batchIdx)
		},
	); err != nil {
		return false
	}
	return true
}

// sendPartialBatch sends the supplied batch to the range specified by
// desc. The batch request is first truncated so that it contains only
// requests which intersect the range descriptor and keys for each
// request are limited to the range's key span. The send occurs in a
// retry loop to handle send failures. On failure to send to any
// replicas, we backoff and retry by refetching the range
// descriptor. If the underlying range seems to have split, we
// recursively invoke divideAndSendBatchToRanges to re-enumerate the
// ranges in the span and resend to each.
func (ds *DistSender) sendPartialBatch(
	ctx context.Context,
	ba roachpb.BatchRequest,
	rs roachpb.RSpan,
	desc *roachpb.RangeDescriptor,
	evictToken *EvictionToken,
	batchIdx int,
) response {
	if batchIdx == 1 {
		ds.metrics.PartialBatchCount.Inc(2) // account for first batch
	} else if batchIdx > 1 {
		ds.metrics.PartialBatchCount.Inc(1)
	}
	var reply *roachpb.BatchResponse
	var pErr *roachpb.Error

	isReverse := ba.IsReverse()

	// Truncate the request to range descriptor.
	intersected, err := rs.Intersect(desc)
	if err != nil {
		return response{pErr: roachpb.NewError(err)}
	}
	truncBA, numActive, err := truncate(ba, intersected)
	if numActive == 0 && err == nil {
		// This shouldn't happen in the wild, but some tests exercise it.
		return response{
			pErr: roachpb.NewErrorf("truncation resulted in empty batch on %s: %s", intersected, ba),
		}
	}
	if err != nil {
		return response{pErr: roachpb.NewError(err)}
	}

	// Start a retry loop for sending the batch to the range.
	for r := retry.StartWithCtx(ctx, ds.rpcRetryOptions); r.Next(); {
		// If we've cleared the descriptor on a send failure, re-lookup.
		if desc == nil {
			var descKey roachpb.RKey
			if isReverse {
				descKey = intersected.EndKey
			} else {
				descKey = intersected.Key
			}
			desc, evictToken, err = ds.getDescriptor(ctx, descKey, nil, isReverse)
			if err != nil {
				log.ErrEventf(ctx, "range descriptor re-lookup failed: %s", err)
				continue
			}
		}

		reply, pErr = ds.sendSingleRange(ctx, truncBA, desc)

		// If sending succeeded, return immediately.
		if pErr == nil {
			return response{reply: reply}
		}

		log.ErrEventf(ctx, "reply error %s: %s", ba, pErr)

		// Error handling: If the error indicates that our range
		// descriptor is out of date, evict it from the cache and try
		// again. Errors that apply only to a single replica were
		// handled in send().
		//
		// TODO(bdarnell): Don't retry endlessly. If we fail twice in a
		// row and the range descriptor hasn't changed, return the error
		// to our caller.
		switch tErr := pErr.GetDetail().(type) {
		case *roachpb.SendError:
			// We've tried all the replicas without success. Either
			// they're all down, or we're using an out-of-date range
			// descriptor. Invalidate the cache and try again with the new
			// metadata.
			log.Event(ctx, "evicting range descriptor on send error and backoff for re-lookup")
			if err := evictToken.Evict(ctx); err != nil {
				return response{pErr: roachpb.NewError(err)}
			}
			// Clear the descriptor to reload on the next attempt.
			desc = nil
			continue
		case *roachpb.RangeKeyMismatchError:
			// Range descriptor might be out of date - evict it. This is
			// likely the result of a range split. If we have new range
			// descriptors, insert them instead as long as they are different
			// from the last descriptor to avoid endless loops.
			var replacements []roachpb.RangeDescriptor
			different := func(rd *roachpb.RangeDescriptor) bool {
				return !desc.RSpan().Equal(rd.RSpan())
			}
			if tErr.MismatchedRange != nil && different(tErr.MismatchedRange) {
				replacements = append(replacements, *tErr.MismatchedRange)
			}
			if tErr.SuggestedRange != nil && different(tErr.SuggestedRange) {
				if includesFrontOfCurSpan(isReverse, tErr.SuggestedRange, rs) {
					replacements = append(replacements, *tErr.SuggestedRange)
				}
			}
			// Same as Evict() if replacements is empty.
			if err := evictToken.EvictAndReplace(ctx, replacements...); err != nil {
				return response{pErr: roachpb.NewError(err)}
			}
			// On addressing errors (likely a split), we need to re-invoke
			// the range descriptor lookup machinery, so we recurse by
			// sending batch to just the partial span this descriptor was
			// supposed to cover.
			log.VEventf(ctx, 1, "likely split; resending batch to span: %s", tErr)
			reply, pErr = ds.divideAndSendBatchToRanges(ctx, ba, intersected, batchIdx)
			return response{reply: reply, pErr: pErr}
		}
		break
	}

	// Propagate error if either the retry closer or context done
	// channels were closed.
	if pErr == nil {
		if pErr = ds.deduceRetryEarlyExitError(ctx); pErr == nil {
			log.Fatal(ctx, "exited retry loop without an error")
		}
	}

	return response{pErr: pErr}
}

func (ds *DistSender) deduceRetryEarlyExitError(ctx context.Context) *roachpb.Error {
	select {
	case <-ds.rpcRetryOptions.Closer:
		// Typically happens during shutdown.
		return roachpb.NewError(&roachpb.NodeUnavailableError{})
	case <-ctx.Done():
		// Happens when the client request is cancelled.
		return roachpb.NewError(ctx.Err())
	default:
	}
	return nil
}

func includesFrontOfCurSpan(isReverse bool, rd *roachpb.RangeDescriptor, rs roachpb.RSpan) bool {
	if isReverse {
		return rd.ContainsExclusiveEndKey(rs.EndKey)
	}
	return rd.ContainsKey(rs.Key)
}

// fillSkippedResponses after meeting the batch key max limit for range
// requests.
func fillSkippedResponses(
	ba roachpb.BatchRequest, br *roachpb.BatchResponse, nextKey roachpb.RKey,
) {
	// Some requests might have NoopResponses; we must replace them with empty
	// responses of the proper type.
	for i, req := range ba.Requests {
		if _, ok := br.Responses[i].GetInner().(*roachpb.NoopResponse); !ok {
			continue
		}
		var reply roachpb.Response
		switch t := req.GetInner().(type) {
		case *roachpb.ScanRequest:
			reply = &roachpb.ScanResponse{}

		case *roachpb.ReverseScanRequest:
			reply = &roachpb.ReverseScanResponse{}

		case *roachpb.DeleteRangeRequest:
			reply = &roachpb.DeleteRangeResponse{}

		case *roachpb.BeginTransactionRequest, *roachpb.EndTransactionRequest:
			continue

		default:
			panic(fmt.Sprintf("bad type %T", t))
		}
		union := roachpb.ResponseUnion{}
		union.MustSetInner(reply)
		br.Responses[i] = union
	}
	// Set the ResumeSpan for future batch requests.
	isReverse := ba.IsReverse()
	for i, resp := range br.Responses {
		req := ba.Requests[i].GetInner()
		if !roachpb.IsRange(req) {
			continue
		}
		hdr := resp.GetInner().Header()
		origSpan := req.Header()
		if isReverse {
			if hdr.ResumeSpan != nil {
				// The ResumeSpan.Key might be set to the StartKey of a range;
				// correctly set it to the Key of the original request span.
				hdr.ResumeSpan.Key = origSpan.Key
			} else if roachpb.RKey(origSpan.Key).Less(nextKey) {
				// Some keys have yet to be processed.
				hdr.ResumeSpan = &origSpan
				if nextKey.Less(roachpb.RKey(origSpan.EndKey)) {
					// The original span has been partially processed.
					hdr.ResumeSpan.EndKey = nextKey.AsRawKey()
				}
			}
		} else {
			if hdr.ResumeSpan != nil {
				// The ResumeSpan.EndKey might be set to the EndKey of a
				// range; correctly set it to the EndKey of the original
				// request span.
				hdr.ResumeSpan.EndKey = origSpan.EndKey
			} else if nextKey.Less(roachpb.RKey(origSpan.EndKey)) {
				// Some keys have yet to be processed.
				hdr.ResumeSpan = &origSpan
				if roachpb.RKey(origSpan.Key).Less(nextKey) {
					// The original span has been partially processed.
					hdr.ResumeSpan.Key = nextKey.AsRawKey()
				}
			}
		}
		br.Responses[i].GetInner().SetHeader(hdr)
	}
}

// sendToReplicas sends one or more RPCs to clients specified by the
// slice of replicas. On success, Send returns the first successful
// reply. If an error occurs which is not specific to a single
// replica, it's returned immediately. Otherwise, when all replicas
// have been tried and failed, returns a send error.
func (ds *DistSender) sendToReplicas(
	ctx context.Context,
	opts SendOptions,
	rangeID roachpb.RangeID,
	replicas ReplicaSlice,
	args roachpb.BatchRequest,
	rpcContext *rpc.Context,
) (*roachpb.BatchResponse, error) {
	if len(replicas) < 1 {
		return nil, roachpb.NewSendError(
			fmt.Sprintf("insufficient replicas (%d) to satisfy send request of %d",
				len(replicas), 1))
	}

	var ambiguousResult bool
	var haveCommit bool
	// We only check for committed txns, not aborts because aborts may
	// be retried without any risk of inconsistencies.
	if etArg, ok := args.GetArg(roachpb.EndTransaction); ok &&
		etArg.(*roachpb.EndTransactionRequest).Commit {
		haveCommit = true
	}
	done := make(chan BatchCall, len(replicas))

	transportFactory := opts.transportFactory
	if transportFactory == nil {
		transportFactory = grpcTransportFactory
	}
	transport, err := transportFactory(opts, rpcContext, replicas, args)
	if err != nil {
		return nil, err
	}
	defer transport.Close()
	if transport.IsExhausted() {
		return nil, roachpb.NewSendError(
			fmt.Sprintf("sending to all %d replicas failed", len(replicas)))
	}

	// Send the first request.
	pending := 1
	if log.V(2) || log.HasSpanOrEvent(ctx) {
		log.VEventf(ctx, 2, "sending batch %s to r%d", args.Summary(), rangeID)
	}
	transport.SendNext(ctx, done)

	// Wait for completions. This loop will retry operations that fail
	// with errors that reflect per-replica state and may succeed on
	// other replicas.
	sendNextTimer := timeutil.NewTimer()
	slowTimer := timeutil.NewTimer()
	defer sendNextTimer.Stop()
	defer slowTimer.Stop()
	slowTimer.Reset(base.SlowRequestThreshold)
	for {
		if timeout, ok := transport.SendNextTimeout(opts.SendNextTimeout); ok {
			// Only start the send-next timer if we haven't exhausted the transport
			// (i.e. there is another replica to send to).
			sendNextTimer.Reset(timeout)
		}

		select {
		case <-sendNextTimer.C:
			sendNextTimer.Read = true
			// On successive RPC timeouts, send to additional replicas if available.
			if !transport.IsExhausted() {
				ds.metrics.SendNextTimeoutCount.Inc(1)
				log.VEventf(ctx, 2, "timeout, trying next peer")
				pending++
				transport.SendNext(ctx, done)
			}

		case <-slowTimer.C:
			log.Warningf(ctx, "have been waiting %s sending RPC to r%d for batch: %s",
				base.SlowRequestThreshold, rangeID, args)
			ds.metrics.SlowRequestsCount.Inc(1)
			defer ds.metrics.SlowRequestsCount.Dec(1)

		case call := <-done:
			pending--
			err := call.Err
			if err == nil {
				switch tErr := call.Reply.Error.GetDetail().(type) {
				case nil:
					return call.Reply, nil
				case *roachpb.RangeNotFoundError, *roachpb.StoreNotFoundError, *roachpb.NodeUnavailableError:
					// These errors are likely to be unique to the replica that reported
					// them, so no action is required before the next retry.
				case *roachpb.NotLeaseHolderError:
					ds.metrics.NotLeaseHolderErrCount.Inc(1)
					if lh := tErr.LeaseHolder; lh != nil {
						// If the replica we contacted knows the new lease holder, update the cache.
						ds.updateLeaseHolderCache(ctx, rangeID, *lh)

						// Move the new lease holder to the head of the queue for the next retry.
						transport.MoveToFront(*lh)
					}
				default:
					// The error received is not specific to this replica, so we
					// should return it instead of trying other replicas. However,
					// if we're trying to commit a transaction and there are
					// still other RPCs outstanding or an ambiguous RPC error
					// was already received, we must return an ambiguous commit
					// error instead of returned error.
					log.ErrEventf(ctx, "application error: %s", call.Reply.Error)
					if haveCommit {
						timer := time.NewTimer(defaultPendingRPCTimeout)
						defer timer.Stop()
						// If there are still pending RPC(s), try to wait them out.
						for timedOut := false; pending > 0 && !timedOut; {
							select {
							case pendingCall := <-done:
								pending--
								if err := pendingCall.Err; err != nil {
									if grpc.Code(err) != codes.Unavailable {
										ambiguousResult = true
									}
								} else if pendingCall.Reply.Error == nil {
									return pendingCall.Reply, nil
								}
							case <-timer.C:
								timedOut = true
							}
						}
						if pending > 0 || ambiguousResult {
							log.ErrEventf(ctx, "returning ambiguous result (pending=%d)", pending)
							return nil, roachpb.NewAmbiguousResultError(
								fmt.Sprintf("error=%s, pending RPCs=%d", call.Reply.Error, pending))
						}
					}
					return call.Reply, nil
				}

				// Extract the detail so it can be included in the error
				// message if this is our last replica.
				//
				// TODO(bdarnell): The last error is not necessarily the best
				// one to return; we may want to remember the "best" error
				// we've seen (for example, a NotLeaseHolderError conveys more
				// information than a RangeNotFound).
				log.ErrEventf(ctx, "application error: %s", call.Reply.Error)
				err = call.Reply.Error.GoError()
			} else {
				// All connection errors except for an unavailable node (this
				// is GRPC's fail-fast error), may mean that the request
				// succeeded on the remote server, but we were unable to
				// receive the reply. Set the ambiguous commit flag.
				//
				// We retry ambiguous commit batches to avoid returning the
				// unrecoverable AmbiguousResultError. This is safe because
				// repeating an already-successfully applied batch is
				// guaranteed to return either a TransactionReplayError (in
				// case the replay happens at the original leader), or a
				// TransactionRetryError (in case the replay happens at a new
				// leader). If the original attempt merely timed out or was
				// lost, then the batch will succeed and we can be assured the
				// commit was applied just once.
				//
				// The Unavailable code is used by GRPC to indicate that a
				// request fails fast and is not sent, so we can be sure there
				// is no ambiguity on these errors. Note that these are common
				// if a node is down.
				// See https://github.com/grpc/grpc-go/blob/52f6504dc290bd928a8139ba94e3ab32ed9a6273/call.go#L182
				// See https://github.com/grpc/grpc-go/blob/52f6504dc290bd928a8139ba94e3ab32ed9a6273/stream.go#L158
				if haveCommit && grpc.Code(err) != codes.Unavailable {
					log.ErrEventf(ctx, "txn may have committed despite RPC error: %s", err)
					ambiguousResult = true
				} else {
					log.ErrEventf(ctx, "RPC error: %s", err)
				}
			}

			// Send to additional replicas if available.
			if !transport.IsExhausted() {
				ds.metrics.NextReplicaErrCount.Inc(1)
				log.VEventf(ctx, 2, "error, trying next peer")
				pending++
				transport.SendNext(ctx, done)
			}
			if pending == 0 {
				if ambiguousResult {
					err = roachpb.NewAmbiguousResultError(
						fmt.Sprintf("sending to all %d replicas failed; last error: %v, "+
							"but RPC failure may have masked txn commit", len(replicas), err))
				} else {
					err = roachpb.NewSendError(
						fmt.Sprintf("sending to all %d replicas failed; last error: %v", len(replicas), err),
					)
				}
				log.ErrEvent(ctx, err.Error())
				return nil, err
			}
		}
	}
}

// updateLeaseHolderCache updates the cached lease holder for the given range.
func (ds *DistSender) updateLeaseHolderCache(
	ctx context.Context, rangeID roachpb.RangeID, newLeaseHolder roachpb.ReplicaDescriptor,
) {
	if log.V(1) {
		if oldLeaseHolder, ok := ds.leaseHolderCache.Lookup(ctx, rangeID); ok {
			if (newLeaseHolder == roachpb.ReplicaDescriptor{}) {
				log.Infof(ctx, "r%d: evicting cached lease holder %+v", rangeID, oldLeaseHolder)
			} else if newLeaseHolder != oldLeaseHolder {
				log.Infof(
					ctx, "r%d: replacing cached lease holder %+v with %+v",
					rangeID, oldLeaseHolder, newLeaseHolder,
				)
			}
		} else {
			log.Infof(ctx, "r%d: caching new lease holder %+v", rangeID, newLeaseHolder)
		}
	}
	ds.leaseHolderCache.Update(ctx, rangeID, newLeaseHolder)
}
