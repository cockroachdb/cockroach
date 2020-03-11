// Copyright 2014 The Cockroach Authors.
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
	"runtime"
	"sync/atomic"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
)

var (
	metaDistSenderBatchCount = metric.Metadata{
		Name:        "distsender.batches",
		Help:        "Number of batches processed",
		Measurement: "Batches",
		Unit:        metric.Unit_COUNT,
	}
	metaDistSenderPartialBatchCount = metric.Metadata{
		Name:        "distsender.batches.partial",
		Help:        "Number of partial batches processed after being divided on range boundaries",
		Measurement: "Partial Batches",
		Unit:        metric.Unit_COUNT,
	}
	metaDistSenderAsyncSentCount = metric.Metadata{
		Name:        "distsender.batches.async.sent",
		Help:        "Number of partial batches sent asynchronously",
		Measurement: "Partial Batches",
		Unit:        metric.Unit_COUNT,
	}
	metaDistSenderAsyncThrottledCount = metric.Metadata{
		Name:        "distsender.batches.async.throttled",
		Help:        "Number of partial batches not sent asynchronously due to throttling",
		Measurement: "Partial Batches",
		Unit:        metric.Unit_COUNT,
	}
	metaTransportSentCount = metric.Metadata{
		Name:        "distsender.rpc.sent",
		Help:        "Number of RPCs sent",
		Measurement: "RPCs",
		Unit:        metric.Unit_COUNT,
	}
	metaTransportLocalSentCount = metric.Metadata{
		Name:        "distsender.rpc.sent.local",
		Help:        "Number of local RPCs sent",
		Measurement: "RPCs",
		Unit:        metric.Unit_COUNT,
	}
	metaTransportSenderNextReplicaErrCount = metric.Metadata{
		Name:        "distsender.rpc.sent.nextreplicaerror",
		Help:        "Number of RPCs sent due to per-replica errors",
		Measurement: "RPCs",
		Unit:        metric.Unit_COUNT,
	}
	metaDistSenderNotLeaseHolderErrCount = metric.Metadata{
		Name:        "distsender.errors.notleaseholder",
		Help:        "Number of NotLeaseHolderErrors encountered",
		Measurement: "Errors",
		Unit:        metric.Unit_COUNT,
	}
	metaDistSenderInLeaseTransferBackoffsCount = metric.Metadata{
		Name:        "distsender.errors.inleasetransferbackoffs",
		Help:        "Number of times backed off due to NotLeaseHolderErrors during lease transfer.",
		Measurement: "Errors",
		Unit:        metric.Unit_COUNT,
	}
	metaDistSenderRangeLookups = metric.Metadata{
		Name:        "distsender.rangelookups",
		Help:        "Number of range lookups.",
		Measurement: "Range Lookups",
		Unit:        metric.Unit_COUNT,
	}
)

// CanSendToFollower is used by the DistSender to determine if it needs to look
// up the current lease holder for a request. It is used by the
// followerreadsccl code to inject logic to check if follower reads are enabled.
// By default, without CCL code, this function returns false.
var CanSendToFollower = func(
	clusterID uuid.UUID, st *cluster.Settings, ba roachpb.BatchRequest,
) bool {
	return false
}

const (
	// The default limit for asynchronous senders.
	defaultSenderConcurrency = 500
	// The maximum number of range descriptors to prefetch during range lookups.
	rangeLookupPrefetchCount = 8
)

var rangeDescriptorCacheSize = settings.RegisterIntSetting(
	"kv.range_descriptor_cache.size",
	"maximum number of entries in the range descriptor and leaseholder caches",
	1e6,
)

var senderConcurrencyLimit = settings.RegisterNonNegativeIntSetting(
	"kv.dist_sender.concurrency_limit",
	"maximum number of asynchronous send requests",
	max(defaultSenderConcurrency, int64(32*runtime.NumCPU())),
)

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// DistSenderMetrics is the set of metrics for a given distributed sender.
type DistSenderMetrics struct {
	BatchCount              *metric.Counter
	PartialBatchCount       *metric.Counter
	AsyncSentCount          *metric.Counter
	AsyncThrottledCount     *metric.Counter
	SentCount               *metric.Counter
	LocalSentCount          *metric.Counter
	NextReplicaErrCount     *metric.Counter
	NotLeaseHolderErrCount  *metric.Counter
	InLeaseTransferBackoffs *metric.Counter
	RangeLookups            *metric.Counter
}

func makeDistSenderMetrics() DistSenderMetrics {
	return DistSenderMetrics{
		BatchCount:              metric.NewCounter(metaDistSenderBatchCount),
		PartialBatchCount:       metric.NewCounter(metaDistSenderPartialBatchCount),
		AsyncSentCount:          metric.NewCounter(metaDistSenderAsyncSentCount),
		AsyncThrottledCount:     metric.NewCounter(metaDistSenderAsyncThrottledCount),
		SentCount:               metric.NewCounter(metaTransportSentCount),
		LocalSentCount:          metric.NewCounter(metaTransportLocalSentCount),
		NextReplicaErrCount:     metric.NewCounter(metaTransportSenderNextReplicaErrCount),
		NotLeaseHolderErrCount:  metric.NewCounter(metaDistSenderNotLeaseHolderErrCount),
		InLeaseTransferBackoffs: metric.NewCounter(metaDistSenderInLeaseTransferBackoffsCount),
		RangeLookups:            metric.NewCounter(metaDistSenderRangeLookups),
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
	nodeDialer       *nodedialer.Dialer
	rpcRetryOptions  retry.Options
	asyncSenderSem   *quotapool.IntPool
	// clusterID is used to verify access to enterprise features.
	// It is copied out of the rpcContext at construction time and used in
	// testing.
	clusterID *base.ClusterIDContainer
	// rangeIteratorGen returns a range iterator bound to the DistSender.
	// Used to avoid allocations.
	rangeIteratorGen RangeIteratorGen

	// disableFirstRangeUpdates disables updates of the first range via
	// gossip. Used by tests which want finer control of the contents of the
	// range cache.
	disableFirstRangeUpdates int32

	// disableParallelBatches instructs DistSender to never parallelize
	// the transmission of partial batch requests across ranges.
	disableParallelBatches bool
}

var _ kv.Sender = &DistSender{}

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

	NodeDialer *nodedialer.Dialer

	TestingKnobs ClientTestingKnobs
}

// NewDistSender returns a batch.Sender instance which connects to the
// Cockroach cluster via the supplied gossip instance. Supplying a
// DistSenderContext or the fields within is optional. For omitted values, sane
// defaults will be used.
func NewDistSender(cfg DistSenderConfig, g *gossip.Gossip) *DistSender {
	ds := &DistSender{
		st:         cfg.Settings,
		clock:      cfg.Clock,
		gossip:     g,
		metrics:    makeDistSenderMetrics(),
		nodeDialer: cfg.NodeDialer,
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
	if cfg.RPCContext == nil {
		panic("no RPCContext set in DistSenderConfig")
	}
	ds.rpcContext = cfg.RPCContext
	if ds.rpcRetryOptions.Closer == nil {
		ds.rpcRetryOptions.Closer = ds.rpcContext.Stopper.ShouldQuiesce()
	}
	ds.clusterID = &cfg.RPCContext.ClusterID
	ds.nodeDialer = cfg.NodeDialer
	ds.asyncSenderSem = quotapool.NewIntPool("DistSender async concurrency",
		uint64(senderConcurrencyLimit.Get(&cfg.Settings.SV)))
	senderConcurrencyLimit.SetOnChange(&cfg.Settings.SV, func() {
		ds.asyncSenderSem.UpdateCapacity(uint64(senderConcurrencyLimit.Get(&cfg.Settings.SV)))
	})
	ds.rpcContext.Stopper.AddCloser(ds.asyncSenderSem.Closer("stopper"))
	ds.rangeIteratorGen = func() *RangeIterator { return NewRangeIterator(ds) }

	if g != nil {
		ctx := ds.AnnotateCtx(context.Background())
		g.RegisterCallback(gossip.KeyFirstRangeDescriptor,
			func(_ string, value roachpb.Value) {
				if atomic.LoadInt32(&ds.disableFirstRangeUpdates) == 1 {
					return
				}
				if log.V(1) {
					var desc roachpb.RangeDescriptor
					if err := value.GetProto(&desc); err != nil {
						log.Errorf(ctx, "unable to parse gossiped first range descriptor: %s", err)
					} else {
						log.Infof(ctx, "gossiped first range descriptor: %+v", desc.Replicas())
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

// DisableFirstRangeUpdates disables updates of the first range via
// gossip. Used by tests which want finer control of the contents of the range
// cache.
func (ds *DistSender) DisableFirstRangeUpdates() {
	atomic.StoreInt32(&ds.disableFirstRangeUpdates, 1)
}

// DisableParallelBatches instructs DistSender to never parallelize the
// transmission of partial batch requests across ranges.
func (ds *DistSender) DisableParallelBatches() {
	ds.disableParallelBatches = true
}

// Metrics returns a struct which contains metrics related to the distributed
// sender's activity.
func (ds *DistSender) Metrics() DistSenderMetrics {
	return ds.metrics
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
	ds.metrics.RangeLookups.Inc(1)
	// We perform the range lookup scan with a READ_UNCOMMITTED consistency
	// level because we want the scan to return intents as well as committed
	// values. The reason for this is because it's not clear whether the intent
	// or the previous value points to the correct location of the Range. It
	// gets even more complicated when there are split-related intents or a txn
	// record co-located with a replica involved in the split. Since we cannot
	// know the correct answer, we lookup both the pre- and post- transaction
	// values.
	rc := roachpb.READ_UNCOMMITTED
	// By using DistSender as the sender, we guarantee that even if the desired
	// RangeDescriptor is not on the first range we send the lookup too, we'll
	// still find it when we scan to the next range. This addresses the issue
	// described in #18032 and #16266, allowing us to support meta2 splits.
	return kv.RangeLookup(ctx, ds, key.AsRawKey(), rc, rangeLookupPrefetchCount, useReverseScan)
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
	if log.V(1) {
		ctx := ds.AnnotateCtx(context.TODO())
		log.Infof(ctx, "unable to determine this node's attributes for replica "+
			"selection; node is most likely bootstrapping")
	}
	return nil
}

// sendRPC sends one or more RPCs to replicas from the supplied
// roachpb.Replica slice. Returns an RPC error if the request could
// not be sent. Note that the reply may contain a higher level error
// and must be checked in addition to the RPC error.
//
// The replicas are assumed to be ordered by preference, with closer
// ones (i.e. expected lowest latency) first.
//
// See sendToReplicas for a description of the withCommit parameter.
func (ds *DistSender) sendRPC(
	ctx context.Context,
	ba roachpb.BatchRequest,
	class rpc.ConnectionClass,
	rangeID roachpb.RangeID,
	replicas ReplicaSlice,
	cachedLeaseHolder roachpb.ReplicaDescriptor,
	withCommit bool,
) (*roachpb.BatchResponse, error) {
	if len(replicas) == 0 {
		return nil, roachpb.NewSendError(
			fmt.Sprintf("no replica node addresses available via gossip for r%d", rangeID))
	}

	ba.RangeID = rangeID

	tracing.AnnotateTrace()
	defer tracing.AnnotateTrace()

	return ds.sendToReplicas(
		ctx,
		ba,
		SendOptions{
			class:   class,
			metrics: &ds.metrics,
		},
		rangeID,
		replicas,
		ds.nodeDialer,
		cachedLeaseHolder,
		withCommit,
	)
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
	desc, returnToken, err := ds.rangeCache.LookupRangeDescriptorWithEvictionToken(
		ctx, descKey, evictToken, useReverseScan,
	)
	if err != nil {
		return nil, returnToken, err
	}

	return desc, returnToken, nil
}

// sendSingleRange gathers and rearranges the replicas, and makes an RPC call.
func (ds *DistSender) sendSingleRange(
	ctx context.Context, ba roachpb.BatchRequest, desc *roachpb.RangeDescriptor, withCommit bool,
) (*roachpb.BatchResponse, *roachpb.Error) {
	// Try to send the call. Learner replicas won't serve reads/writes, so send
	// only to the `Voters` replicas. This is just an optimization to save a
	// network hop, everything would still work if we had `All` here.
	replicas := NewReplicaSlice(ds.gossip, desc.Replicas().Voters())

	// If this request needs to go to a lease holder and we know who that is, move
	// it to the front.
	var cachedLeaseHolder roachpb.ReplicaDescriptor
	canSendToFollower := ds.clusterID != nil &&
		CanSendToFollower(ds.clusterID.Get(), ds.st, ba)
	if !canSendToFollower && ba.RequiresLeaseHolder() {
		if storeID, ok := ds.leaseHolderCache.Lookup(ctx, desc.RangeID); ok {
			if i := replicas.FindReplica(storeID); i >= 0 {
				replicas.MoveToFront(i)
				cachedLeaseHolder = replicas[0].ReplicaDescriptor
			}
		}
	}
	if (cachedLeaseHolder == roachpb.ReplicaDescriptor{}) {
		// Rearrange the replicas so that they're ordered in expectation of
		// request latency.
		replicas.OptimizeReplicaOrder(ds.getNodeDescriptor(), ds.rpcContext.RemoteClocks.Latency)
	}
	class := rpc.ConnectionClassForKey(desc.RSpan().Key)
	br, err := ds.sendRPC(ctx, ba, class, desc.RangeID, replicas, cachedLeaseHolder, withCommit)
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

	if ba.MaxSpanRequestKeys != 0 || ba.TargetBytes != 0 {
		// Verify that the batch contains only specific range requests or the
		// EndTxnRequest. Verify that a batch with a ReverseScan only contains
		// ReverseScan range requests.
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

			case *roachpb.QueryIntentRequest, *roachpb.ResolveIntentRangeRequest:
				continue

			case *roachpb.EndTxnRequest, *roachpb.ReverseScanRequest:
				continue

			case *roachpb.RevertRangeRequest:
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
// least two parts, with the final part containing the EndTxn
// request.
var errNo1PCTxn = roachpb.NewErrorf("cannot send 1PC txn to multiple ranges")

// splitBatchAndCheckForRefreshSpans splits the batch according to the
// canSplitET parameter and checks whether the batch can forward its
// read timestamp. If the batch has its CanForwardReadTimestamp flag
// set but is being split across multiple sub-batches then the flag in
// the batch header is unset.
func splitBatchAndCheckForRefreshSpans(
	ba *roachpb.BatchRequest, canSplitET bool,
) [][]roachpb.RequestUnion {
	parts := ba.Split(canSplitET)

	// If the batch is split and the header has its CanForwardReadTimestamp flag
	// set then we much check whether any request would need to be refreshed in
	// the event that the one of the partial batches was to forward its read
	// timestamp during a server-side refresh. If any such request exists then
	// we unset the CanForwardReadTimestamp flag.
	if len(parts) > 1 && ba.CanForwardReadTimestamp {
		hasRefreshSpans := func() bool {
			for _, part := range parts {
				for _, req := range part {
					if roachpb.NeedsRefresh(req.GetInner()) {
						return true
					}
				}
			}
			return false
		}()
		if hasRefreshSpans {
			ba.CanForwardReadTimestamp = false

			// If the final part contains an EndTxn request, unset its
			// CanCommitAtHigherTimestamp flag as well.
			lastPart := parts[len(parts)-1]
			if et := lastPart[len(lastPart)-1].GetEndTxn(); et != nil {
				etCopy := *et
				etCopy.CanCommitAtHigherTimestamp = false
				lastPart = append([]roachpb.RequestUnion(nil), lastPart...)
				lastPart[len(lastPart)-1].MustSetInner(&etCopy)
				parts[len(parts)-1] = lastPart
			}
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
func (ds *DistSender) Send(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	ds.metrics.BatchCount.Inc(1)

	tracing.AnnotateTrace()

	// TODO(nvanbenschoten): This causes ba to escape to the heap. Either
	// commit to passing BatchRequests by reference or return an updated
	// value from this method instead.
	if pErr := ds.initAndVerifyBatch(ctx, &ba); pErr != nil {
		return nil, pErr
	}

	ctx = ds.AnnotateCtx(ctx)
	ctx, sp := tracing.EnsureChildSpan(ctx, ds.AmbientContext.Tracer, "dist sender send")
	defer sp.Finish()

	var rplChunks []*roachpb.BatchResponse
	splitET := false
	var require1PC bool
	lastReq := ba.Requests[len(ba.Requests)-1].GetInner()
	if et, ok := lastReq.(*roachpb.EndTxnRequest); ok && et.Require1PC {
		require1PC = true
	}
	// To ensure that we lay down intents to prevent starvation, always
	// split the end transaction request into its own batch on retries.
	// Txns requiring 1PC are an exception and should never be split.
	if ba.Txn != nil && ba.Txn.Epoch > 0 && !require1PC {
		splitET = true
	}
	parts := splitBatchAndCheckForRefreshSpans(&ba, splitET)
	if len(parts) > 1 && (ba.MaxSpanRequestKeys != 0 || ba.TargetBytes != 0) {
		// We already verified above that the batch contains only scan requests of the same type.
		// Such a batch should never need splitting.
		log.Fatalf(ctx, "batch with MaxSpanRequestKeys=%d, TargetBytes=%d needs splitting",
			log.Safe(ba.MaxSpanRequestKeys), log.Safe(ba.TargetBytes))
	}

	errIdxOffset := 0
	for len(parts) > 0 {
		part := parts[0]
		ba.Requests = part
		// The minimal key range encompassing all requests contained within.
		// Local addressing has already been resolved.
		// TODO(tschottdorf): consider rudimentary validation of the batch here
		// (for example, non-range requests with EndKey, or empty key ranges).
		rs, err := keys.Range(ba.Requests)
		if err != nil {
			return nil, roachpb.NewError(err)
		}

		// Determine whether this part of the BatchRequest contains a committing
		// EndTxn request.
		var withCommit, withParallelCommit bool
		if etArg, ok := ba.GetArg(roachpb.EndTxn); ok {
			et := etArg.(*roachpb.EndTxnRequest)
			withCommit = et.Commit
			withParallelCommit = et.IsParallelCommit()
		}

		var rpl *roachpb.BatchResponse
		var pErr *roachpb.Error
		if withParallelCommit {
			rpl, pErr = ds.divideAndSendParallelCommit(ctx, ba, rs, 0 /* batchIdx */)
		} else {
			rpl, pErr = ds.divideAndSendBatchToRanges(ctx, ba, rs, withCommit, 0 /* batchIdx */)
		}

		if pErr == errNo1PCTxn {
			// If we tried to send a single round-trip EndTxn but it looks like
			// it's going to hit multiple ranges, split it here and try again.
			if len(parts) != 1 {
				panic("EndTxn not in last chunk of batch")
			} else if require1PC {
				log.Fatalf(ctx, "required 1PC transaction cannot be split: %s", ba)
			}
			parts = splitBatchAndCheckForRefreshSpans(&ba, true /* split ET */)
			// Restart transaction of the last chunk as multiple parts with
			// EndTxn in the last part.
			continue
		}
		if pErr != nil {
			if pErr.Index != nil && pErr.Index.Index != -1 {
				pErr.Index.Index += int32(errIdxOffset)
			}
			return nil, pErr
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

	return reply, nil
}

type response struct {
	reply     *roachpb.BatchResponse
	positions []int
	pErr      *roachpb.Error
}

// divideAndSendParallelCommit divides a parallel-committing batch into
// sub-batches that can be evaluated in parallel but should not be evaluated
// on a Store together.
//
// The case where this comes up is if the batch is performing a parallel commit
// and the transaction has previously pipelined writes that have yet to be
// proven successful. In this scenario, the EndTxn request will be preceded by a
// series of QueryIntent requests (see txn_pipeliner.go). Before evaluating,
// each of these QueryIntent requests will grab latches and wait for their
// corresponding write to finish. This is how the QueryIntent requests
// synchronize with the write they are trying to verify.
//
// If these QueryIntents remained in the same batch as the EndTxn request then
// they would force the EndTxn request to wait for the previous write before
// evaluating itself. This "pipeline stall" would effectively negate the benefit
// of the parallel commit. To avoid this, we make sure that these "pre-commit"
// QueryIntent requests are split from and issued concurrently with the rest of
// the parallel commit batch.
//
// batchIdx indicates which partial fragment of the larger batch is being
// processed by this method. Currently it is always set to zero because this
// method is never invoked recursively, but it is exposed to maintain symmetry
// with divideAndSendBatchToRanges.
func (ds *DistSender) divideAndSendParallelCommit(
	ctx context.Context, ba roachpb.BatchRequest, rs roachpb.RSpan, batchIdx int,
) (br *roachpb.BatchResponse, pErr *roachpb.Error) {
	// Search backwards, looking for the first pre-commit QueryIntent.
	swapIdx := -1
	lastIdx := len(ba.Requests) - 1
	for i := lastIdx - 1; i >= 0; i-- {
		req := ba.Requests[i].GetInner()
		if req.Method() == roachpb.QueryIntent {
			swapIdx = i
		} else {
			break
		}
	}
	if swapIdx == -1 {
		// No pre-commit QueryIntents. Nothing to split.
		return ds.divideAndSendBatchToRanges(ctx, ba, rs, true /* withCommit */, batchIdx)
	}

	// Swap the EndTxn request and the first pre-commit QueryIntent. This
	// effectively creates a split point between the two groups of requests.
	//
	//  Before:    [put qi(1) put del qi(2) qi(3) qi(4) et]
	//  After:     [put qi(1) put del et qi(3) qi(4) qi(2)]
	//  Separated: [put qi(1) put del et] [qi(3) qi(4) qi(2)]
	//
	// NOTE: the non-pre-commit QueryIntent's must remain where they are in the
	// batch. These ensure that the transaction always reads its writes (see
	// txnPipeliner.chainToInFlightWrites). These will introduce pipeline stalls
	// and undo most of the benefit of this method, but luckily they are rare in
	// practice.
	swappedReqs := append([]roachpb.RequestUnion(nil), ba.Requests...)
	swappedReqs[swapIdx], swappedReqs[lastIdx] = swappedReqs[lastIdx], swappedReqs[swapIdx]

	// Create a new pre-commit QueryIntent-only batch and issue it
	// in a non-limited async task. This batch may need to be split
	// over multiple ranges, so call into divideAndSendBatchToRanges.
	qiBa := ba
	qiBa.Requests = swappedReqs[swapIdx+1:]
	qiRS, err := keys.Range(qiBa.Requests)
	if err != nil {
		return br, roachpb.NewError(err)
	}
	qiBatchIdx := batchIdx + 1
	qiResponseCh := make(chan response, 1)

	runTask := ds.rpcContext.Stopper.RunAsyncTask
	if ds.disableParallelBatches {
		runTask = ds.rpcContext.Stopper.RunTask
	}
	if err := runTask(ctx, "kv.DistSender: sending pre-commit query intents", func(ctx context.Context) {
		// Map response index to the original un-swapped batch index.
		// Remember that we moved the last QueryIntent in this batch
		// from swapIdx to the end.
		//
		// From the example above:
		//  Before:    [put qi(1) put del qi(2) qi(3) qi(4) et]
		//  Separated: [put qi(1) put del et] [qi(3) qi(4) qi(2)]
		//
		//  qiBa.Requests = [qi(3) qi(4) qi(2)]
		//  swapIdx       = 4
		//  positions     = [5 6 4]
		//
		positions := make([]int, len(qiBa.Requests))
		positions[len(positions)-1] = swapIdx
		for i := range positions[:len(positions)-1] {
			positions[i] = swapIdx + 1 + i
		}

		// Send the batch with withCommit=true since it will be inflight
		// concurrently with the EndTxn batch below.
		reply, pErr := ds.divideAndSendBatchToRanges(ctx, qiBa, qiRS, true /* withCommit */, qiBatchIdx)
		qiResponseCh <- response{reply: reply, positions: positions, pErr: pErr}
	}); err != nil {
		return nil, roachpb.NewError(err)
	}

	// Adjust the original batch request to ignore the pre-commit
	// QueryIntent requests. Make sure to determine the request's
	// new key span.
	ba.Requests = swappedReqs[:swapIdx+1]
	rs, err = keys.Range(ba.Requests)
	if err != nil {
		return nil, roachpb.NewError(err)
	}
	br, pErr = ds.divideAndSendBatchToRanges(ctx, ba, rs, true /* withCommit */, batchIdx)

	// Wait for the QueryIntent-only batch to complete and stitch
	// the responses together.
	qiReply := <-qiResponseCh

	// Handle error conditions.
	if pErr != nil {
		// The batch with the EndTxn returned an error. Ignore errors from the
		// pre-commit QueryIntent requests because that request is read-only and
		// will produce the same errors next time, if applicable.
		if qiReply.reply != nil {
			pErr.UpdateTxn(qiReply.reply.Txn)
		}
		maybeSwapErrorIndex(pErr, swapIdx, lastIdx)
		return nil, pErr
	}
	if qiPErr := qiReply.pErr; qiPErr != nil {
		// The batch with the pre-commit QueryIntent requests returned an error.
		ignoreMissing := false
		if _, ok := qiPErr.GetDetail().(*roachpb.IntentMissingError); ok {
			// If the error is an IntentMissingError, detect whether this is due
			// to intent resolution and can be safely ignored.
			ignoreMissing, err = ds.detectIntentMissingDueToIntentResolution(ctx, br.Txn)
			if err != nil {
				return nil, roachpb.NewError(err)
			}
		}
		if !ignoreMissing {
			qiPErr.UpdateTxn(br.Txn)
			maybeSwapErrorIndex(qiPErr, swapIdx, lastIdx)
			return nil, qiPErr
		}
		// Populate the pre-commit QueryIntent batch response. If we made it
		// here then we know we can ignore intent missing errors.
		qiReply.reply = qiBa.CreateReply()
		for _, ru := range qiReply.reply.Responses {
			ru.GetQueryIntent().FoundIntent = true
		}
	}

	// Both halves of the split batch succeeded. Piece them back together.
	resps := make([]roachpb.ResponseUnion, len(swappedReqs))
	copy(resps, br.Responses)
	resps[swapIdx], resps[lastIdx] = resps[lastIdx], resps[swapIdx]
	br.Responses = resps
	if err := br.Combine(qiReply.reply, qiReply.positions); err != nil {
		return nil, roachpb.NewError(err)
	}
	return br, nil
}

// detectIntentMissingDueToIntentResolution attempts to detect whether a missing
// intent error thrown by a pre-commit QueryIntent request was due to intent
// resolution after the transaction was already finalized instead of due to a
// failure of the corresponding pipelined write. It is possible for these two
// situations to be confused because the pre-commit QueryIntent requests are
// issued in parallel with the staging EndTxn request and may evaluate after the
// transaction becomes implicitly committed. If this happens and a concurrent
// transaction observes the implicit commit and makes the commit explicit, it is
// allowed to begin resolving the transactions intents.
//
// MVCC values don't remember their transaction once they have been resolved.
// This loss of information means that QueryIntent returns an intent missing
// error if it finds the resolved value that correspond to its desired intent.
// Because of this, the race discussed above can result in intent missing errors
// during a parallel commit even when the transaction successfully committed.
//
// This method queries the transaction record to determine whether an intent
// missing error was caused by this race or whether the intent missing error
// is real and guarantees that the transaction is not implicitly committed.
//
// See #37866 (issue) and #37900 (corresponding tla+ update).
func (ds *DistSender) detectIntentMissingDueToIntentResolution(
	ctx context.Context, txn *roachpb.Transaction,
) (bool, error) {
	ba := roachpb.BatchRequest{}
	ba.Timestamp = ds.clock.Now()
	ba.Add(&roachpb.QueryTxnRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: txn.TxnMeta.Key,
		},
		Txn: txn.TxnMeta,
	})
	log.VEvent(ctx, 1, "detecting whether missing intent is due to intent resolution")
	br, pErr := ds.Send(ctx, ba)
	if pErr != nil {
		// We weren't able to determine whether the intent missing error is
		// due to intent resolution or not, so it is still ambiguous whether
		// the commit succeeded.
		return false, roachpb.NewAmbiguousResultError(fmt.Sprintf("error=%s [intent missing]", pErr))
	}
	respTxn := &br.Responses[0].GetQueryTxn().QueriedTxn
	switch respTxn.Status {
	case roachpb.COMMITTED:
		// The transaction has already been finalized as committed. The missing
		// intent error must have been a result of a concurrent transaction
		// recovery finding the transaction in the implicit commit state and
		// resolving one of its intents before the pre-commit QueryIntent
		// queried that intent. We know that the transaction was committed
		// successfully, so ignore the error.
		return true, nil
	case roachpb.ABORTED:
		// The transaction has either already been finalized as aborted or has
		// been finalized as committed and already had its transaction record
		// GCed. We can't distinguish between these two conditions with full
		// certainty, so we're forced to return an ambiguous commit error.
		// TODO(nvanbenschoten): QueryTxn will materialize an ABORTED transaction
		// record if one does not already exist. If we are certain that no actor
		// will ever persist an ABORTED transaction record after a COMMIT record is
		// GCed and we returned whether the record was synthesized in the QueryTxn
		// response then we could use the existence of an ABORTED transaction record
		// to further isolates the ambiguity caused by the loss of information
		// during intent resolution. If this error becomes a problem, we can explore
		// this option.
		return false, roachpb.NewAmbiguousResultError("intent missing and record aborted")
	default:
		// The transaction has not been finalized yet, so the missing intent
		// error must have been caused by a real missing intent. Propagate the
		// missing intent error.
		// NB: we don't expect the record to be PENDING at this point, but it's
		// not worth making any hard assertions about what we get back here.
		return false, nil
	}
}

// maybeSwapErrorIndex swaps the error index from a to b or b to a if the
// error's index is set and is equal to one of these to values.
func maybeSwapErrorIndex(pErr *roachpb.Error, a, b int) {
	if pErr.Index == nil {
		return
	}
	if pErr.Index.Index == int32(a) {
		pErr.Index.Index = int32(b)
	} else if pErr.Index.Index == int32(b) {
		pErr.Index.Index = int32(a)
	}
}

// mergeErrors merges the two errors, combining their transaction state and
// returning the error with the highest priority.
func mergeErrors(pErr1, pErr2 *roachpb.Error) *roachpb.Error {
	ret, drop := pErr1, pErr2
	if roachpb.ErrPriority(drop.GoError()) > roachpb.ErrPriority(ret.GoError()) {
		ret, drop = drop, ret
	}
	ret.UpdateTxn(drop.GetTxn())
	return ret
}

// divideAndSendBatchToRanges sends the supplied batch to all of the
// ranges which comprise the span specified by rs. The batch request
// is trimmed against each range which is part of the span and sent
// either serially or in parallel, if possible.
//
// batchIdx indicates which partial fragment of the larger batch is
// being processed by this method. It's specified as non-zero when
// this method is invoked recursively.
//
// withCommit indicates that the batch contains a transaction commit
// or that a transaction commit is being run concurrently with this
// batch. Either way, if this is true then sendToReplicas will need
// to handle errors differently.
func (ds *DistSender) divideAndSendBatchToRanges(
	ctx context.Context, ba roachpb.BatchRequest, rs roachpb.RSpan, withCommit bool, batchIdx int,
) (br *roachpb.BatchResponse, pErr *roachpb.Error) {
	// Clone the BatchRequest's transaction so that future mutations to the
	// proto don't affect the proto in this batch.
	if ba.Txn != nil {
		ba.Txn = ba.Txn.Clone()
	}
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
		resp := ds.sendPartialBatch(
			ctx, ba, rs, ri.Desc(), ri.Token(), withCommit, batchIdx, false, /* needsTruncate */
		)
		return resp.reply, resp.pErr
	}

	// The batch spans ranges (according to our cached range descriptors).
	// Verify that this is ok.
	// TODO(tschottdorf): we should have a mechanism for discovering range
	// merges (descriptor staleness will mostly go unnoticed), or we'll be
	// turning single-range queries into multi-range queries for no good
	// reason.
	if ba.IsUnsplittable() {
		mismatch := roachpb.NewRangeKeyMismatchError(rs.Key.AsRawKey(), rs.EndKey.AsRawKey(), ri.Desc())
		return nil, roachpb.NewError(mismatch)
	}
	// If there's no transaction and ba spans ranges, possibly re-run as part of
	// a transaction for consistency. The case where we don't need to re-run is
	// if the read consistency is not required.
	if ba.Txn == nil && ba.IsTransactional() && ba.ReadConsistency == roachpb.CONSISTENT {
		return nil, roachpb.NewError(&roachpb.OpRequiresTxnError{})
	}
	// If the batch contains a non-parallel commit EndTxn and spans ranges then
	// we want the caller to come again with the EndTxn in a separate
	// (non-concurrent) batch.
	//
	// NB: withCommit allows us to short-circuit the check in the common case,
	// but even when that's true, we still need to search for the EndTxn in the
	// batch.
	if withCommit {
		etArg, ok := ba.GetArg(roachpb.EndTxn)
		if ok && !etArg.(*roachpb.EndTxnRequest).IsParallelCommit() {
			return nil, errNo1PCTxn
		}
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
		// Combine all the responses.
		// It's important that we wait for all of them even if an error is caught
		// because the client.Sender() contract mandates that we don't "hold on" to
		// any part of a request after DistSender.Send() returns.
		for _, responseCh := range responseChs {
			resp := <-responseCh
			if resp.pErr != nil {
				if pErr == nil {
					pErr = resp.pErr
					// Update the error's transaction with any new information from
					// the batch response. This may contain interesting updates if
					// the batch was parallelized and part of it succeeded.
					pErr.UpdateTxn(br.Txn)
				} else {
					// The batch was split and saw (at least) two different errors.
					// Merge their transaction state and determine which to return
					// based on their priorities.
					pErr = mergeErrors(pErr, resp.pErr)
				}
				continue
			}

			// Combine the new response with the existing one (including updating
			// the headers) if we haven't yet seen an error.
			if pErr == nil {
				if err := br.Combine(resp.reply, resp.positions); err != nil {
					pErr = roachpb.NewError(err)
				}
			} else {
				// Update the error's transaction with any new information from
				// the batch response. This may contain interesting updates if
				// the batch was parallelized and part of it succeeded.
				pErr.UpdateTxn(resp.reply.Txn)
			}
		}

		if pErr == nil && couldHaveSkippedResponses {
			fillSkippedResponses(ba, br, seekKey, resumeReason)
		}
	}()

	canParallelize := ba.Header.MaxSpanRequestKeys == 0 && ba.Header.TargetBytes == 0
	if ba.IsSingleCheckConsistencyRequest() {
		// Don't parallelize full checksum requests as they have to touch the
		// entirety of each replica of each range they touch.
		isExpensive := ba.Requests[0].GetCheckConsistency().Mode == roachpb.ChecksumMode_CHECK_FULL
		canParallelize = canParallelize && !isExpensive
	}

	for ; ri.Valid(); ri.Seek(ctx, seekKey, scanDir) {
		responseCh := make(chan response, 1)
		responseChs = append(responseChs, responseCh)

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
		if canParallelize && !lastRange && !ds.disableParallelBatches &&
			ds.sendPartialBatchAsync(ctx, ba, rs, ri.Desc(), ri.Token(), withCommit, batchIdx, responseCh) {
			// Sent the batch asynchronously.
		} else {
			resp := ds.sendPartialBatch(
				ctx, ba, rs, ri.Desc(), ri.Token(), withCommit, batchIdx, true, /* needsTruncate */
			)
			responseCh <- resp
			if resp.pErr != nil {
				return
			}
			// Update the transaction from the response. Note that this wouldn't happen
			// on the asynchronous path, but if we have newer information it's good to
			// use it.
			if !lastRange {
				ba.UpdateTxn(resp.reply.Txn)
			}

			mightStopEarly := ba.MaxSpanRequestKeys > 0 || ba.TargetBytes > 0
			// Check whether we've received enough responses to exit query loop.
			if mightStopEarly {
				var replyResults int64
				var replyBytes int64
				for _, r := range resp.reply.Responses {
					replyResults += r.GetInner().Header().NumKeys
					replyBytes += r.GetInner().Header().NumBytes
				}
				// Update MaxSpanRequestKeys, if applicable. Note that ba might be
				// passed recursively to further divideAndSendBatchToRanges() calls.
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
				if ba.TargetBytes > 0 {
					ba.TargetBytes -= replyBytes
					if ba.TargetBytes <= 0 {
						couldHaveSkippedResponses = true
						resumeReason = roachpb.RESUME_KEY_LIMIT
						return
					}
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
	withCommit bool,
	batchIdx int,
	responseCh chan response,
) bool {
	if err := ds.rpcContext.Stopper.RunLimitedAsyncTask(
		ctx, "kv.DistSender: sending partial batch",
		ds.asyncSenderSem, false, /* wait */
		func(ctx context.Context) {
			ds.metrics.AsyncSentCount.Inc(1)
			responseCh <- ds.sendPartialBatch(
				ctx, ba, rs, desc, evictToken, withCommit, batchIdx, true, /* needsTruncate */
			)
		},
	); err != nil {
		ds.metrics.AsyncThrottledCount.Inc(1)
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
	withCommit bool,
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
			desc, evictToken, err = ds.getDescriptor(ctx, descKey, evictToken, isReverse)
			if err != nil {
				log.VErrEventf(ctx, 1, "range descriptor re-lookup failed: %s", err)
				// We set pErr if we encountered an error getting the descriptor in
				// order to return the most recent error when we are out of retries.
				pErr = roachpb.NewError(err)
				continue
			}
		}

		reply, pErr = ds.sendSingleRange(ctx, ba, desc, withCommit)

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
		case *roachpb.SendError:
			// We've tried all the replicas without success. Either
			// they're all down, or we're using an out-of-date range
			// descriptor. Invalidate the cache and try again with the new
			// metadata.
			log.VEventf(ctx, 1, "evicting range descriptor on %T and backoff for re-lookup: %+v", tErr, desc)
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
			reply, pErr = ds.divideAndSendBatchToRanges(ctx, ba, rs, withCommit, batchIdx)
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
		return roachpb.NewError(errors.Wrap(ctx.Err(), "aborted in distSender"))
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
		origSpan := req.Header().Span()
		if isReverse {
			if hdr.ResumeSpan != nil {
				// The ResumeSpan.Key might be set to the StartKey of a range;
				// correctly set it to the Key of the original request span.
				hdr.ResumeSpan.Key = origSpan.Key
			} else if roachpb.RKey(origSpan.Key).Less(nextKey) {
				// Some keys have yet to be processed.
				hdr.ResumeSpan = new(roachpb.Span)
				*hdr.ResumeSpan = origSpan
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
					hdr.ResumeSpan = new(roachpb.Span)
					*hdr.ResumeSpan = origSpan
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
//
// The method accepts a boolean declaring whether a transaction commit
// is either in this batch or in-flight concurrently with this batch.
// If withCommit is false (i.e. either no EndTxn is in flight,
// or it is attempting to abort), ambiguous results will never be
// returned from this method. This is because both transactional writes
// and aborts can be retried (the former due to seqno idempotency, the
// latter because aborting is idempotent). If withCommit is true, any
// errors that do not definitively rule out the possibility that the
// batch could have succeeded are transformed into AmbiguousResultErrors.
func (ds *DistSender) sendToReplicas(
	ctx context.Context,
	ba roachpb.BatchRequest,
	opts SendOptions,
	rangeID roachpb.RangeID,
	replicas ReplicaSlice,
	nodeDialer *nodedialer.Dialer,
	cachedLeaseHolder roachpb.ReplicaDescriptor,
	withCommit bool,
) (*roachpb.BatchResponse, error) {
	transport, err := ds.transportFactory(opts, nodeDialer, replicas)
	if err != nil {
		return nil, err
	}
	if transport.IsExhausted() {
		return nil, roachpb.NewSendError(
			fmt.Sprintf("sending to all %d replicas failed", len(replicas)))
	}

	curReplica := transport.NextReplica()
	if log.ExpensiveLogEnabled(ctx, 2) {
		log.VEventf(ctx, 2, "r%d: sending batch %s to %s", rangeID, ba.Summary(), curReplica)
	}
	br, err := transport.SendNext(ctx, ba)
	// maxSeenLeaseSequence tracks the maximum LeaseSequence seen in a
	// NotLeaseHolderError. If we encounter a sequence number less than or equal
	// to maxSeenLeaseSequence number in a subsequent NotLeaseHolderError then
	// the range must be experiencing a least transfer and the client should back
	// off using inTransferRetry.
	maxSeenLeaseSequence := roachpb.LeaseSequence(-1)
	inTransferRetry := retry.StartWithCtx(ctx, ds.rpcRetryOptions)
	inTransferRetry.Next() // The first call to Next does not block.

	// This loop will retry operations that fail with errors that reflect
	// per-replica state and may succeed on other replicas.
	var ambiguousError error
	for {
		if err != nil {
			// For most connection errors, we cannot tell whether or not
			// the request may have succeeded on the remote server, so we
			// set the ambiguous commit flag (exceptions are captured in
			// the grpcutil.RequestDidNotStart function).
			//
			// We retry ambiguous commit batches to avoid returning the
			// unrecoverable AmbiguousResultError. This is safe because
			// repeating an already-successfully applied batch is
			// guaranteed to return an error. If the original attempt merely timed out
			// or was lost, then the batch will succeed and we can be assured the
			// commit was applied just once.
			if withCommit && !grpcutil.RequestDidNotStart(err) {
				ambiguousError = err
			}
			log.VErrEventf(ctx, 2, "RPC error: %s", err)

			// If the error wasn't just a context cancellation and the down replica
			// is cached as the lease holder, evict it. The only other eviction
			// happens below on NotLeaseHolderError, but if the next replica is the
			// actual lease holder, we're never going to receive one of those and
			// will thus pay the price of trying the down node first forever.
			//
			// NB: we should consider instead adding a successful reply from the next
			// replica into the cache, but without a leaseholder (and taking into
			// account that the local node can't be down) it won't take long until we
			// talk to a replica that tells us who the leaseholder is.
			if ctx.Err() == nil {
				if storeID, ok := ds.leaseHolderCache.Lookup(ctx, rangeID); ok && curReplica.StoreID == storeID {
					ds.leaseHolderCache.Update(ctx, rangeID, 0 /* evict */)
				}
			}
		} else {
			// NB: This section of code may have unfortunate performance implications. If we
			// exit the below type switch with propagateError remaining at `false`, we'll try
			// more replicas. That may succeed and future requests might do the same thing over
			// and over again, adding needless round-trips to the earlier replicas.
			propagateError := false
			switch tErr := br.Error.GetDetail().(type) {
			case nil:
				// When a request that we know could only succeed on the leaseholder comes
				// back as successful, make sure the leaseholder cache reflects this
				// replica. In steady state, this is almost always the case, and so we
				// gate the update on whether the response comes from a node that we didn't
				// know held the lease.
				if cachedLeaseHolder != curReplica && ba.RequiresLeaseHolder() {
					ds.leaseHolderCache.Update(ctx, rangeID, curReplica.StoreID)
				}
				return br, nil
			case *roachpb.StoreNotFoundError, *roachpb.NodeUnavailableError:
				// These errors are likely to be unique to the replica that reported
				// them, so no action is required before the next retry.
			case *roachpb.RangeNotFoundError:
				// The store we routed to doesn't have this replica. This can happen when
				// our descriptor is outright outdated, but it can also be caused by a
				// replica that has just been added but needs a snapshot to be caught up.
				//
				// We'll try other replicas which typically gives us the leaseholder, either
				// via the NotLeaseHolderError or nil error paths, both of which update the
				// leaseholder cache.
			case *roachpb.NotLeaseHolderError:
				ds.metrics.NotLeaseHolderErrCount.Inc(1)
				if lh := tErr.LeaseHolder; lh != nil {
					// Update the leaseholder cache. Naively this would also happen when the
					// next RPC comes back, but we don't want to wait out the additional RPC
					// latency.
					ds.leaseHolderCache.Update(ctx, rangeID, lh.StoreID)
					// Avoid an extra update to the leaseholder cache if the next RPC succeeds.
					cachedLeaseHolder = *lh

					// If the implicated leaseholder is not a known replica, return a SendError
					// to signal eviction of the cached RangeDescriptor and re-send.
					if replicas.FindReplica(lh.StoreID) == -1 {
						br.Error = roachpb.NewError(roachpb.NewSendError(fmt.Sprintf(
							"leaseholder s%d (via %+v) not in cached replicas %v", lh.StoreID, curReplica, replicas,
						)))
						propagateError = true
					} else {
						// Move the new lease holder to the head of the queue for the next retry.
						transport.MoveToFront(*lh)
					}
				}
				if l := tErr.Lease; !propagateError && l != nil {
					// Check whether we've seen this lease or a prior lease before and
					// backoff if so or update maxSeenLeaseSequence if not.
					if l.Sequence > maxSeenLeaseSequence {
						maxSeenLeaseSequence = l.Sequence
						inTransferRetry.Reset() // The following Next call will not block.
					} else {
						ds.metrics.InLeaseTransferBackoffs.Inc(1)
						log.VErrEventf(ctx, 2, "backing off due to NotLeaseHolderErr at "+
							"LeaseSequence %d <= %d", l.Sequence, maxSeenLeaseSequence)
					}
					inTransferRetry.Next()
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
				return br, nil
			}

			log.VErrEventf(ctx, 1, "application error: %s", br.Error)
		}

		// Has the caller given up?
		if ctx.Err() != nil {
			errMsg := fmt.Sprintf("context done during DistSender.Send: %s", ctx.Err())
			log.Eventf(ctx, errMsg)
			if ambiguousError != nil {
				return nil, roachpb.NewAmbiguousResultError(errMsg)
			}
			// Don't consider this a SendError, because SendErrors indicate that we
			// were unable to reach a replica that could serve the request, and they
			// cause range cache evictions. Context cancellations just mean the
			// sender changed its mind or the request timed out.
			return nil, errors.Wrap(ctx.Err(), "aborted during DistSender.Send")
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
				fmt.Sprintf("sending to all %d replicas failed; last error: %v %v", len(replicas), br, err),
			)
		}

		ds.metrics.NextReplicaErrCount.Inc(1)
		curReplica = transport.NextReplica()
		log.VEventf(ctx, 2, "error: %v %v; trying next peer %s", br, err, curReplica)
		br, err = transport.SendNext(ctx, ba)
	}
}
