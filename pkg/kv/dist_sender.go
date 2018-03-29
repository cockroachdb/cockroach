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

package kv

import (
	"context"
	"fmt"
	"sync/atomic"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/shuffle"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

const (
	// The default limit for asynchronous senders.
	defaultSenderConcurrency = 500
	// The maximum number of range descriptors to prefetch during range lookups.
	rangeLookupPrefetchCount = 8
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

var rangeDescriptorCacheSize = settings.RegisterIntSetting(
	"kv.range_descriptor_cache.size",
	"maximum number of entries in the range descriptor and leaseholder caches",
	1e6,
)

// DistSenderMetrics is the set of metrics for a given distributed sender.
type DistSenderMetrics struct {
	BatchCount             *metric.Counter
	PartialBatchCount      *metric.Counter
	SentCount              *metric.Counter
	LocalSentCount         *metric.Counter
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
		NextReplicaErrCount:    metric.NewCounter(metaDistSenderNextReplicaErrCount),
		NotLeaseHolderErrCount: metric.NewCounter(metaDistSenderNotLeaseHolderErrCount),
		SlowRequestsCount:      metric.NewGauge(metaSlowDistSenderRequests),
	}
}

// A firstRangeMissingError indicates that the first range has not yet
// been gossiped. This will be the case for a node which hasn't yet
// joined the gossip network.
type firstRangeMissingError struct{}

// Error is part of the error interface.
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

	st *cluster.Settings
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
	rangeCache *RangeDescriptorCache
	// leaseHolderCache caches range lease holders by range ID.
	leaseHolderCache *LeaseHolderCache
	transportFactory TransportFactory
	rpcContext       *rpc.Context
	rpcRetryOptions  retry.Options
	asyncSenderSem   chan struct{}
	asyncSenderCount int32
}

var _ client.Sender = &DistSender{}

// DistSenderConfig holds configuration and auxiliary objects that can be passed
// to NewDistSender.
type DistSenderConfig struct {
	AmbientCtx log.AmbientContext

	Settings        *cluster.Settings
	Clock           *hlc.Clock
	RPCRetryOptions *retry.Options
	// nodeDescriptor, if provided, is used to describe which node the DistSender
	// lives on, for instance when deciding where to send RPCs.
	// Usually it is filled in from the Gossip network on demand.
	nodeDescriptor    *roachpb.NodeDescriptor
	RPCContext        *rpc.Context
	RangeDescriptorDB RangeDescriptorDB

	TestingKnobs DistSenderTestingKnobs
}

// DistSenderTestingKnobs is a part of the context used to control parts of
// the system.
type DistSenderTestingKnobs struct {
	// The RPC dispatcher. Defaults to grpc but can be changed here for
	// testing purposes.
	TransportFactory TransportFactory
}

var _ base.ModuleTestingKnobs = &DistSenderTestingKnobs{}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*DistSenderTestingKnobs) ModuleTestingKnobs() {}

// NewDistSender returns a batch.Sender instance which connects to the
// Cockroach cluster via the supplied gossip instance. Supplying a
// DistSenderContext or the fields within is optional. For omitted values, sane
// defaults will be used.
func NewDistSender(cfg DistSenderConfig, g *gossip.Gossip) *DistSender {
	ds := &DistSender{
		st:      cfg.Settings,
		clock:   cfg.Clock,
		gossip:  g,
		metrics: makeDistSenderMetrics(),
	}
	if ds.st == nil {
		ds.st = cluster.MakeTestingClusterSettings()
	}

	ds.AmbientContext = cfg.AmbientCtx
	if ds.AmbientContext.Tracer == nil {
		panic("no tracer set in AmbientCtx")
	}

	if cfg.nodeDescriptor != nil {
		atomic.StorePointer(&ds.nodeDescriptor, unsafe.Pointer(cfg.nodeDescriptor))
	}
	rdb := cfg.RangeDescriptorDB
	if rdb == nil {
		rdb = ds
	}
	getRangeDescCacheSize := func() int64 {
		return rangeDescriptorCacheSize.Get(&ds.st.SV)
	}
	ds.rangeCache = NewRangeDescriptorCache(ds.st, rdb, getRangeDescCacheSize)
	ds.leaseHolderCache = NewLeaseHolderCache(getRangeDescCacheSize)
	if tf := cfg.TestingKnobs.TransportFactory; tf != nil {
		ds.transportFactory = tf
	} else {
		ds.transportFactory = GRPCTransportFactory
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
	ds.asyncSenderSem = make(chan struct{}, defaultSenderConcurrency)

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

// RangeLookup implements the RangeDescriptorDB interface. It uses LookupRange
// to perform a lookup scan for the provided key, using DistSender itself as the
// client.Sender. This means that the scan will recurse into DistSender, which
// will in turn use the RangeDescriptorCache again to lookup the RangeDescriptor
// necessary to perform the scan.
func (ds *DistSender) RangeLookup(
	ctx context.Context, key roachpb.RKey, useReverseScan bool,
) ([]roachpb.RangeDescriptor, []roachpb.RangeDescriptor, error) {
	// We perform the range lookup scan with a READ_UNCOMMITTED consistency
	// level because we want the scan to return intents as well as committed
	// values. The reason for this is because it's not clear whether the intent
	// or the previous value points to the correct location of the Range. It
	// gets even more complicated when there are split-related intents or a txn
	// record co-located with a replica involved in the split. Since we cannot
	// know the correct answer, we lookup both the pre- and post- transaction
	// values.
	rc := roachpb.READ_UNCOMMITTED
	if !ds.st.Version.IsActive(cluster.VersionReadUncommittedRangeLookups) {
		// READ_UNCOMMITTED was unsupported before this version. RangeLookup
		// will set the DeprecatedReturnIntents when scanning inconsistently,
		// which will have the same effect of returning both committed and
		// uncommitted values.
		//
		// TODO(nvanbenschoten): remove in version 2.1.
		rc = roachpb.INCONSISTENT
	}
	// By using DistSender as the sender, we guarantee that even if the desired
	// RangeDescriptor is not on the first range we send the lookup too, we'll
	// still find it when we scan to the next range. This addresses the issue
	// described in #18032 and #16266, allowing us to support meta2 splits.
	return client.RangeLookup(ctx, ds, key.AsRawKey(), rc, rangeLookupPrefetchCount, useReverseScan)
}

// legacyRangeLookup implements the legacyRangeDescriptorDB interface. The
// method dispatches a DeprecatedRangeLookupRequest with the range metadata key
// for the given key to the replicas of the given range. Note that we allow
// inconsistent reads when doing range lookups for efficiency. Getting stale
// data is not a correctness problem but instead may infrequently result in
// additional latency as additional range lookups may be required. Note also
// that rangeLookup bypasses the DistSender's Send() method, so there is no
// error inspection and retry logic here; this is not an issue since the lookup
// performs a single inconsistent read only.
//
// TODO(nvanbenschoten): remove in version 2.1.
func (ds *DistSender) legacyRangeLookup(
	ctx context.Context, key roachpb.RKey, desc *roachpb.RangeDescriptor, useReverseScan bool,
) ([]roachpb.RangeDescriptor, []roachpb.RangeDescriptor, error) {
	sender := client.SenderFunc(func(ctx context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		replicas := NewReplicaSlice(ds.gossip, desc)
		shuffle.Shuffle(replicas)
		br, err := ds.sendRPC(ctx, desc.RangeID, replicas, ba)
		if err != nil {
			return nil, roachpb.NewError(err)
		}
		return br, nil
	})
	return client.LegacyRangeLookup(ctx, sender, key.AsRawKey(),
		roachpb.INCONSISTENT, rangeLookupPrefetchCount, useReverseScan)
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

	tracing.AnnotateTrace()
	defer tracing.AnnotateTrace()

	return ds.sendToReplicas(ctx, SendOptions{metrics: &ds.metrics}, rangeID, replicas, ba, ds.rpcContext)
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

	// Rearrange the replicas so that they're ordered in expectation of
	// request latency.
	var latencyFn LatencyFunc
	if ds.rpcContext != nil {
		latencyFn = ds.rpcContext.RemoteClocks.Latency
	}
	replicas.OptimizeReplicaOrder(ds.getNodeDescriptor(), latencyFn)

	// If this request needs to go to a lease holder and we know who that is, move
	// it to the front.
	if !ba.IsReadOnly() || ba.ReadConsistency.RequiresReadLease() {
		if storeID, ok := ds.leaseHolderCache.Lookup(ctx, desc.RangeID); ok {
			if i := replicas.FindReplica(storeID); i >= 0 {
				replicas.MoveToFront(i)
			}
		}
	}

	br, err := ds.sendRPC(ctx, desc.RangeID, replicas, ba)
	if err != nil {
		log.VErrEvent(ctx, 2, err.Error())
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
	if ba.ReadConsistency != roachpb.CONSISTENT && ba.Timestamp == (hlc.Timestamp{}) {
		ba.Timestamp = ds.clock.Now()
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
				// not supported. Note that ReverseScanRequest is _not_ handled here.
				// TODO(vivek): don't enumerate all range requests.
				if isReverse {
					return roachpb.NewErrorf("batch with limit contains both forward and reverse scans")
				}

			case *roachpb.ResolveIntentRangeRequest:
				continue

			case *roachpb.BeginTransactionRequest, *roachpb.EndTransactionRequest, *roachpb.ReverseScanRequest:
				continue

			default:
				return roachpb.NewErrorf("batch with limit contains %T request", inner)
			}
		}
	}

	// If ScanOptions is set the batch is only allowed to contain scans.
	if ba.ScanOptions != nil {
		for _, req := range ba.Requests {
			switch req.GetInner().(type) {
			case *roachpb.ScanRequest, *roachpb.ReverseScanRequest:
				// Scans are supported.
			case *roachpb.BeginTransactionRequest, *roachpb.EndTransactionRequest:
				// These requests are ignored.
			default:
				return roachpb.NewErrorf("batch with scan option has non-scans: %s", ba)
			}
		}
		// If both MaxSpanRequestKeys and MinResults are set, then they can't be
		// contradictory.
		if ba.Header.MaxSpanRequestKeys != 0 &&
			ba.Header.MaxSpanRequestKeys < ba.Header.ScanOptions.MinResults {
			return roachpb.NewErrorf("MaxSpanRequestKeys (%d) < MinResults (%d): %s",
				ba.Header.MaxSpanRequestKeys, ba.Header.ScanOptions.MinResults, ba)
		}
	}

	return nil
}

// errNo1PCTxn indicates that a batch cannot be sent as a 1 phase
// commit because it spans multiple ranges and must be split into at
// least two parts, with the final part containing the EndTransaction
// request.
var errNo1PCTxn = roachpb.NewErrorf("cannot send 1PC txn to multiple ranges")

// splitBatchAndCheckForRefreshSpans splits the batch according to the
// canSplitET parmeter and checks whether the final request is an
// EndTransaction. If so, the EndTransactionRequest.NoRefreshSpans
// flag is reset to indicate whether earlier parts of the split may
// result in refresh spans.
func splitBatchAndCheckForRefreshSpans(
	ba roachpb.BatchRequest, canSplitET bool,
) [][]roachpb.RequestUnion {
	parts := ba.Split(canSplitET)
	// If the final part contains an EndTransaction, we need to check
	// whether earlier split parts contain any refresh spans and properly
	// set the NoRefreshSpans flag on the end transaction.
	lastPart := parts[len(parts)-1]
	lastReq := lastPart[len(lastPart)-1].GetInner()
	if et, ok := lastReq.(*roachpb.EndTransactionRequest); ok && et.NoRefreshSpans {
		hasRefreshSpans := false
		for _, part := range parts[:len(parts)-1] {
			for _, req := range part {
				if roachpb.NeedsRefresh(req.GetInner()) {
					hasRefreshSpans = true
				}
			}
		}
		if hasRefreshSpans {
			etCopy := *et
			etCopy.NoRefreshSpans = false
			lastPart = append([]roachpb.RequestUnion(nil), lastPart...)
			lastPart[len(lastPart)-1].MustSetInner(&etCopy)
			parts[len(parts)-1] = lastPart
		}
	}
	return parts
}

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
//
// Note that on error, this method will return any batch responses for
// successfully processed batch requests. This allows the caller to
// deal with potential retry situations where a batch is split so that
// EndTransaction is processed alone, after earlier requests in the
// batch succeeded. Where possible, the caller may be able to update
// spans encountered in the transaction and retry just the
// EndTransaction request to avoid client-side serializable txn retries.
func (ds *DistSender) Send(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	ds.metrics.BatchCount.Inc(1)

	tracing.AnnotateTrace()

	if pErr := ds.initAndVerifyBatch(ctx, &ba); pErr != nil {
		return nil, pErr
	}

	ctx = ds.AnnotateCtx(ctx)
	ctx, cleanup := tracing.EnsureChildSpan(ctx, ds.AmbientContext.Tracer, "dist sender")
	defer cleanup()

	var rplChunks []*roachpb.BatchResponse
	splitET := false
	// To ensure that we lay down intents to prevent starvation, always
	// split the end transaction request into its own batch on retries.
	if ba.Txn != nil && ba.Txn.Epoch > 0 {
		splitET = true
	}
	parts := splitBatchAndCheckForRefreshSpans(ba, splitET)
	if len(parts) > 1 && ba.MaxSpanRequestKeys != 0 {
		// We already verified above that the batch contains only scan requests of the same type.
		// Such a batch should never need splitting.
		panic("batch with MaxSpanRequestKeys needs splitting")
	}

	var pErr *roachpb.Error
	errIdxOffset := 0
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

		var rpl *roachpb.BatchResponse
		rpl, pErr = ds.divideAndSendBatchToRanges(ctx, ba, rs, 0 /* batchIdx */)

		if pErr == errNo1PCTxn {
			// If we tried to send a single round-trip EndTransaction but
			// it looks like it's going to hit multiple ranges, split it
			// here and try again.
			if len(parts) != 1 {
				panic("EndTransaction not in last chunk of batch")
			}
			parts = splitBatchAndCheckForRefreshSpans(ba, true /* split ET */)
			if len(parts) != 2 {
				panic("split of final EndTransaction chunk resulted in != 2 parts")
			}
			// Restart transaction of the last chunk as two parts
			// with EndTransaction in the second part.
			continue
		}
		if pErr != nil {
			if pErr.Index != nil && pErr.Index.Index != -1 {
				pErr.Index.Index += int32(errIdxOffset)
			}
			// Break out of loop to collate batch responses received so far to
			// return with error.
			break
		}

		errIdxOffset += len(ba.Requests)

		// Propagate transaction from last reply to next request. The final
		// update is taken and put into the response's main header.
		ba.UpdateTxn(rpl.Txn)
		rplChunks = append(rplChunks, rpl)
		parts = parts[1:]
	}

	var reply *roachpb.BatchResponse
	if len(rplChunks) > 0 {
		reply = rplChunks[0]
		for _, rpl := range rplChunks[1:] {
			reply.Responses = append(reply.Responses, rpl.Responses...)
			reply.CollectedSpans = append(reply.CollectedSpans, rpl.CollectedSpans...)
		}
		lastHeader := rplChunks[len(rplChunks)-1].BatchResponse_Header
		lastHeader.CollectedSpans = reply.CollectedSpans
		reply.BatchResponse_Header = lastHeader
	}

	return reply, pErr
}

type response struct {
	reply     *roachpb.BatchResponse
	positions []int
	pErr      *roachpb.Error
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
	// Get initial seek key depending on direction of iteration.
	var scanDir ScanDirection
	var seekKey roachpb.RKey
	if !ba.IsReverse() {
		scanDir = Ascending
		seekKey = rs.Key
	} else {
		scanDir = Descending
		seekKey = rs.EndKey
	}
	ri := NewRangeIterator(ds)
	ri.Seek(ctx, seekKey, scanDir)
	if !ri.Valid() {
		return nil, ri.Error()
	}
	// Take the fast path if this batch fits within a single range.
	if !ri.NeedAnother(rs) {
		ba.SetNewRequest()
		resp := ds.sendPartialBatch(ctx, ba, rs, ri.Desc(), ri.Token(), batchIdx, false /* needsTruncate */)
		return resp.reply, resp.pErr
	}

	// Make an empty slice of responses which will be populated with responses
	// as they come in via Combine().
	br = &roachpb.BatchResponse{
		Responses: make([]roachpb.ResponseUnion, len(ba.Requests)),
	}
	// This function builds a channel of responses for each range
	// implicated in the span (rs) and combines them into a single
	// BatchResponse when finished.
	var responseChs []chan response
	// couldHaveSkippedResponses is set if a ResumeSpan needs to be sent back.
	var couldHaveSkippedResponses bool
	// If couldHaveSkippedResponses is set, resumeReason indicates the reason why
	// the ResumeSpan is necessary. This reason is common to all individual
	// responses that carry a ResumeSpan.
	var resumeReason roachpb.ResponseHeader_ResumeReason
	defer func() {
		if r := recover(); r != nil {
			// If we're in the middle of a panic, don't wait on responseChs.
			panic(r)
		}
		var hadSuccess bool
		for _, responseCh := range responseChs {
			resp := <-responseCh
			if resp.pErr != nil {
				if pErr == nil {
					pErr = resp.pErr
				}
				continue
			}
			hadSuccess = true

			// Combine the new response with the existing one (including updating
			// the headers).
			if err := br.Combine(resp.reply, resp.positions); err != nil {
				pErr = roachpb.NewError(err)
				return
			}
		}

		// If we experienced an error, don't neglect to update the error's
		// attached transaction with any responses which were received.
		if pErr != nil {
			// The br.Txn != nil check looks unnecessary, but note that we might
			// be returning errNo1PCTxn which is a singleton, so we could end up
			// with data races.
			//
			// TODO(tschottdorf): get rid of the errNo1PCTxn singleton. It's
			// ugly.
			if br.Txn != nil {
				pErr.UpdateTxn(br.Txn)
			}
			// If this is a write batch with any successful responses, but
			// we're ultimately returning an error, wrap the error with a
			// MixedSuccessError.
			if hadSuccess && ba.IsWrite() {
				pErr = roachpb.NewError(&roachpb.MixedSuccessError{Wrapped: pErr})
			}
		} else if couldHaveSkippedResponses {
			fillSkippedResponses(ba, br, seekKey, resumeReason)
		}
	}()

	stopAtRangeBoundary := ba.Header.ScanOptions != nil && ba.Header.ScanOptions.StopAtRangeBoundary
	// If min_results is set, num_results will count how many results scans have
	// accumulated so far.
	var numResults int64
	canParallelize := (ba.Header.MaxSpanRequestKeys == 0) && !stopAtRangeBoundary

	for ; ri.Valid(); ri.Seek(ctx, seekKey, scanDir) {
		// Increase the sequence counter only once before sending RPCs to
		// the ranges involved in this chunk of the batch (as opposed to
		// for each RPC individually). On RPC errors, there's no guarantee
		// that the request hasn't made its way to the target regardless
		// of the error; we'd like the second execution to be caught. There
		// is a small chance that we address a range twice in this chunk
		// (stale/suboptimal descriptors due to splits/merges) which leads
		//to a transaction retry.
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
			if ba.Txn == nil && ba.IsPossibleTransaction() && ba.ReadConsistency == roachpb.CONSISTENT {
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

		lastRange := !ri.NeedAnother(rs)
		// Send the next partial batch to the first range in the "rs" span.
		// If we can reserve one of the limited goroutines available for parallel
		// batch RPCs, send asynchronously.
		if canParallelize && !lastRange && ds.rpcContext != nil &&
			ds.sendPartialBatchAsync(ctx, ba, rs, ri.Desc(), ri.Token(), batchIdx, responseCh) {
			// Note that we pass the batch request by value to the parallel
			// goroutine to avoid using the cloned txn.

			// Clone the txn to preserve the current txn sequence for the async call.
			if ba.Txn != nil {
				txnClone := ba.Txn.Clone()
				ba.Txn = &txnClone
			}
		} else {
			resp := ds.sendPartialBatch(ctx, ba, rs, ri.Desc(), ri.Token(), batchIdx, true /* needsTruncate */)
			responseCh <- resp
			if resp.pErr != nil {
				return
			}
			// Update the transaction from the response. Note that this wouldn't happen
			// on the asynchronous path, but if we have newer information it's good to
			// use it.
			ba.UpdateTxn(resp.reply.Txn)

			mightStopEarly := ba.MaxSpanRequestKeys > 0 || stopAtRangeBoundary
			// Check whether we've received enough responses to exit query loop.
			if mightStopEarly {
				var replyResults int64
				for _, r := range resp.reply.Responses {
					replyResults += r.GetInner().Header().NumKeys
				}
				// Do accounting for results. It's important that we update
				// MaxSpanRequestKeys and ScanOptions.MinResults, as ba might be
				// passed recursively to further divideAndSendBatchToRanges() calls.
				numResults += replyResults
				if ba.MaxSpanRequestKeys > 0 {
					if replyResults > ba.MaxSpanRequestKeys {
						log.Fatalf(ctx, "received %d results, limit was %d",
							replyResults, ba.MaxSpanRequestKeys)
					}
					ba.MaxSpanRequestKeys -= replyResults
					// Exiting; any missing responses will be filled in via defer().
					if ba.MaxSpanRequestKeys == 0 {
						couldHaveSkippedResponses = true
						resumeReason = roachpb.RESUME_KEY_LIMIT
						return
					}
				}
				var minResultsSatisfied bool
				if !stopAtRangeBoundary {
					minResultsSatisfied = true
				} else {
					if ba.Header.ScanOptions.MinResults == 0 {
						minResultsSatisfied = true
					} else {
						// We need to change ba.Header.ScanOptions, so we have to make a
						// copy so as to not mutate the one that we have already passed to
						// gRPC.
						scanOptsCopy := *ba.Header.ScanOptions
						scanOptsCopy.MinResults -= numResults
						minResultsSatisfied = scanOptsCopy.MinResults <= 0
						ba.Header.ScanOptions = &scanOptsCopy
					}
				}
				// If stopAtRangeBoundary is set, we stop unless MinResults is not
				// satisfied.
				if stopAtRangeBoundary && minResultsSatisfied {
					couldHaveSkippedResponses = true
					resumeReason = roachpb.RESUME_RANGE_BOUNDARY
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
		if lastRange || !nextRS.Key.Less(nextRS.EndKey) {
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
		ctx, "kv.DistSender: sending partial batch",
		ds.asyncSenderSem, false, /* wait */
		func(ctx context.Context) {
			atomic.AddInt32(&ds.asyncSenderCount, 1)
			responseCh <- ds.sendPartialBatch(ctx, ba, rs, desc, evictToken, batchIdx, true /* needsTruncate */)
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
// ranges in the span and resend to each. If needsTruncate is true,
// the supplied batch and span must be truncated to the supplied range
// descriptor.
func (ds *DistSender) sendPartialBatch(
	ctx context.Context,
	ba roachpb.BatchRequest,
	rs roachpb.RSpan,
	desc *roachpb.RangeDescriptor,
	evictToken *EvictionToken,
	batchIdx int,
	needsTruncate bool,
) response {
	if batchIdx == 1 {
		ds.metrics.PartialBatchCount.Inc(2) // account for first batch
	} else if batchIdx > 1 {
		ds.metrics.PartialBatchCount.Inc(1)
	}
	var reply *roachpb.BatchResponse
	var pErr *roachpb.Error
	var err error
	var positions []int

	isReverse := ba.IsReverse()

	if needsTruncate {
		// Truncate the request to range descriptor.
		rs, err = rs.Intersect(desc)
		if err != nil {
			return response{pErr: roachpb.NewError(err)}
		}
		ba, positions, err = truncate(ba, rs)
		if len(positions) == 0 && err == nil {
			// This shouldn't happen in the wild, but some tests exercise it.
			return response{
				pErr: roachpb.NewErrorf("truncation resulted in empty batch on %s: %s", rs, ba),
			}
		}
		if err != nil {
			return response{pErr: roachpb.NewError(err)}
		}
	}

	// Start a retry loop for sending the batch to the range.
	for r := retry.StartWithCtx(ctx, ds.rpcRetryOptions); r.Next(); {
		// If we've cleared the descriptor on a send failure, re-lookup.
		if desc == nil {
			var descKey roachpb.RKey
			if isReverse {
				descKey = rs.EndKey
			} else {
				descKey = rs.Key
			}
			desc, evictToken, err = ds.getDescriptor(ctx, descKey, nil, isReverse)
			if err != nil {
				log.VErrEventf(ctx, 1, "range descriptor re-lookup failed: %s", err)
				continue
			}
		}

		reply, pErr = ds.sendSingleRange(ctx, ba, desc)

		// If sending succeeded, return immediately.
		if pErr == nil {
			return response{reply: reply, positions: positions}
		}

		// Re-map the error index within this partial batch back
		// to its position in the encompassing batch.
		if pErr.Index != nil && pErr.Index.Index != -1 && positions != nil {
			pErr.Index.Index = int32(positions[pErr.Index.Index])
		}

		log.VErrEventf(ctx, 2, "reply error %s: %s", ba, pErr)

		// Error handling: If the error indicates that our range
		// descriptor is out of date, evict it from the cache and try
		// again. Errors that apply only to a single replica were
		// handled in send().
		//
		// TODO(bdarnell): Don't retry endlessly. If we fail twice in a
		// row and the range descriptor hasn't changed, return the error
		// to our caller.
		switch tErr := pErr.GetDetail().(type) {
		case *roachpb.SendError, *roachpb.RangeNotFoundError:
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
			// supposed to cover. Note that for the resending, we use the
			// already truncated batch, so that we know that the response
			// to it matches the positions into our batch (using the full
			// batch here would give a potentially larger response slice
			// with unknown mapping to our truncated reply).
			log.VEventf(ctx, 1, "likely split; resending batch to span: %s", tErr)
			reply, pErr = ds.divideAndSendBatchToRanges(ctx, ba, rs, batchIdx)
			return response{reply: reply, positions: positions, pErr: pErr}
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
		// Happens when the client request is canceled.
		return roachpb.NewError(ctx.Err())
	default:
	}
	return nil
}

func includesFrontOfCurSpan(isReverse bool, rd *roachpb.RangeDescriptor, rs roachpb.RSpan) bool {
	if isReverse {
		return rd.ContainsKeyInverted(rs.EndKey)
	}
	return rd.ContainsKey(rs.Key)
}

// fillSkippedResponses fills in responses and ResumeSpans for requests
// when a batch finished without fully processing the requested key spans for
// (some of) the requests in the batch. This can happen when processing has met
// the batch key max limit for range requests, or some other stop condition
// based on ScanOptions.
//
// nextKey is the first key that was not processed. This will be used when
// filling up the ResumeSpan's.
func fillSkippedResponses(
	ba roachpb.BatchRequest,
	br *roachpb.BatchResponse,
	nextKey roachpb.RKey,
	resumeReason roachpb.ResponseHeader_ResumeReason,
) {
	// Some requests might have no response at all if we used a batch-wide
	// limit; simply create trivial responses for those. Note that any type
	// of request can crop up here - simply take a batch that exceeds the
	// limit, and add any other requests at higher keys at the end of the
	// batch -- they'll all come back without any response since they never
	// execute.
	var scratchBA roachpb.BatchRequest
	for i := range br.Responses {
		if br.Responses[i] != (roachpb.ResponseUnion{}) {
			continue
		}
		req := ba.Requests[i].GetInner()
		// We need to summon an empty response. The most convenient (but not
		// most efficient) way is to use (*BatchRequest).CreateReply.
		//
		// TODO(tschottdorf): can autogenerate CreateReply for individual
		// requests, see roachpb/gen_batch.go.
		if scratchBA.Requests == nil {
			scratchBA.Requests = make([]roachpb.RequestUnion, 1)
		}
		scratchBA.Requests[0].MustSetInner(req)
		br.Responses[i] = scratchBA.CreateReply().Responses[0]
	}
	// Set the ResumeSpan for future batch requests.
	isReverse := ba.IsReverse()
	for i, resp := range br.Responses {
		req := ba.Requests[i].GetInner()
		if !roachpb.IsRange(req) {
			continue
		}
		hdr := resp.GetInner().Header()
		hdr.ResumeReason = resumeReason
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
				// The ResumeSpan.EndKey might be set to the EndKey of a range because
				// that's what a store will set it to when the limit is reached; it
				// doesn't know any better). In that case, we correct it to the EndKey
				// of the original request span. Note that this doesn't touch
				// ResumeSpan.Key, which is really the important part of the ResumeSpan.
				hdr.ResumeSpan.EndKey = origSpan.EndKey
			} else {
				// The request might have been fully satisfied, in which case it doesn't
				// need a ResumeSpan, or it might not have. Figure out if we're in the
				// latter case.
				if nextKey.Less(roachpb.RKey(origSpan.EndKey)) {
					// Some keys have yet to be processed.
					hdr.ResumeSpan = &origSpan
					if roachpb.RKey(origSpan.Key).Less(nextKey) {
						// The original span has been partially processed.
						hdr.ResumeSpan.Key = nextKey.AsRawKey()
					}
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
	var ambiguousError error
	var haveCommit bool
	// We only check for committed txns, not aborts because aborts may
	// be retried without any risk of inconsistencies.
	if etArg, ok := args.GetArg(roachpb.EndTransaction); ok {
		haveCommit = etArg.(*roachpb.EndTransactionRequest).Commit
	}

	transport, err := ds.transportFactory(opts, rpcContext, replicas, args)
	if err != nil {
		return nil, err
	}
	defer transport.Close()
	if transport.IsExhausted() {
		return nil, roachpb.NewSendError(
			fmt.Sprintf("sending to all %d replicas failed", len(replicas)))
	}
	// Must be buffered because tests have blocking SendNext implementations.
	done := make(chan BatchCall, 1)
	log.VEventf(ctx, 2, "r%d: sending batch %s to %s", rangeID, args.Summary(), transport.NextReplica())
	transport.SendNext(ctx, done)

	// Wait for completions. This loop will retry operations that fail
	// with errors that reflect per-replica state and may succeed on
	// other replicas.
	slowTimer := timeutil.NewTimer()
	defer slowTimer.Stop()
	slowTimer.Reset(base.SlowRequestThreshold)
	for {
		select {
		case <-slowTimer.C:
			slowTimer.Read = true
			log.Warningf(ctx,
				"have been waiting %s sending RPC to r%d (currently pending: %s) for batch: %s",
				base.SlowRequestThreshold, rangeID, transport.GetPending(), args)
			ds.metrics.SlowRequestsCount.Inc(1)
			defer ds.metrics.SlowRequestsCount.Dec(1)

		case <-ctx.Done():
			// Caller has given up.
			errMsg := fmt.Sprintf("context finished during distsender send: %v", ctx.Err())
			log.Eventf(ctx, errMsg)
			return nil, roachpb.NewAmbiguousResultError(errMsg)

		case call := <-done:
			if err := call.Err; err != nil {
				// For most connection errors, we cannot tell whether or not
				// the request may have succeeded on the remote server, so we
				// set the ambiguous commit flag (exceptions are captured in
				// the grpcutil.RequestDidNotStart function).
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
				if haveCommit && !grpcutil.RequestDidNotStart(err) {
					ambiguousError = err
				}
				log.VErrEventf(ctx, 2, "RPC error: %s", err)
			} else {
				propagateError := false
				switch tErr := call.Reply.Error.GetDetail().(type) {
				case nil:
					return call.Reply, nil
				case *roachpb.StoreNotFoundError, *roachpb.NodeUnavailableError:
					// These errors are likely to be unique to the replica that reported
					// them, so no action is required before the next retry.
				case *roachpb.NotLeaseHolderError:
					ds.metrics.NotLeaseHolderErrCount.Inc(1)
					if lh := tErr.LeaseHolder; lh != nil {
						// If the replica we contacted knows the new lease holder, update the cache.
						ds.leaseHolderCache.Update(ctx, rangeID, lh.StoreID)

						// If the implicated leaseholder is not a known replica,
						// return a RangeNotFoundError to signal eviction of the
						// cached RangeDescriptor and re-send.
						if replicas.FindReplica(lh.StoreID) == -1 {
							// Replace NotLeaseHolderError with RangeNotFoundError.
							call.Reply.Error = roachpb.NewError(roachpb.NewRangeNotFoundError(rangeID))
							propagateError = true
						} else {
							// Move the new lease holder to the head of the queue for the next retry.
							transport.MoveToFront(*lh)
						}
					}
				default:
					propagateError = true
				}

				if propagateError {
					if ambiguousError != nil {
						return nil, roachpb.NewAmbiguousResultError(fmt.Sprintf("error=%s [propagate]", ambiguousError))
					}

					// The error received is likely not specific to this
					// replica, so we should return it instead of trying other
					// replicas.
					return call.Reply, nil
				}

				log.VErrEventf(ctx, 1, "application error: %s", call.Reply.Error)
			}

			if transport.IsExhausted() {
				if ambiguousError != nil {
					return nil, roachpb.NewAmbiguousResultError(fmt.Sprintf("error=%s [exhausted]", ambiguousError))
				}

				// TODO(bdarnell): The last error is not necessarily the best
				// one to return; we may want to remember the "best" error
				// we've seen (for example, a NotLeaseHolderError conveys more
				// information than a RangeNotFound).
				return nil, roachpb.NewSendError(
					fmt.Sprintf("sending to all %d replicas failed; last error: %v", len(replicas), call),
				)
			}

			ds.metrics.NextReplicaErrCount.Inc(1)
			log.VEventf(ctx, 2, "error: %v; trying next peer %s", call, transport.NextReplica())
			transport.SendNext(ctx, done)
		}
	}
}
