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
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangecache"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcostmodel"
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
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
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
		Help:        "Number of replica-addressed RPCs sent",
		Measurement: "RPCs",
		Unit:        metric.Unit_COUNT,
	}
	metaTransportLocalSentCount = metric.Metadata{
		Name:        "distsender.rpc.sent.local",
		Help:        "Number of replica-addressed RPCs sent through the local-server optimization",
		Measurement: "RPCs",
		Unit:        metric.Unit_COUNT,
	}
	metaTransportSenderNextReplicaErrCount = metric.Metadata{
		Name:        "distsender.rpc.sent.nextreplicaerror",
		Help:        "Number of replica-addressed RPCs sent due to per-replica errors",
		Measurement: "RPCs",
		Unit:        metric.Unit_COUNT,
	}
	metaDistSenderNotLeaseHolderErrCount = metric.Metadata{
		Name:        "distsender.errors.notleaseholder",
		Help:        "Number of NotLeaseHolderErrors encountered from replica-addressed RPCs",
		Measurement: "Errors",
		Unit:        metric.Unit_COUNT,
	}
	metaDistSenderInLeaseTransferBackoffsCount = metric.Metadata{
		Name:        "distsender.errors.inleasetransferbackoffs",
		Help:        "Number of times backed off due to NotLeaseHolderErrors during lease transfer",
		Measurement: "Errors",
		Unit:        metric.Unit_COUNT,
	}
	metaDistSenderRangeLookups = metric.Metadata{
		Name:        "distsender.rangelookups",
		Help:        "Number of range lookups",
		Measurement: "Range Lookups",
		Unit:        metric.Unit_COUNT,
	}
	metaDistSenderSlowRPCs = metric.Metadata{
		Name: "requests.slow.distsender",
		Help: `Number of replica-bound RPCs currently stuck or retrying for a long time.

Note that this is not a good signal for KV health. The remote side of the
RPCs tracked here may experience contention, so an end user can easily
cause values for this metric to be emitted by leaving a transaction open
for a long time and contending with it using a second transaction.`,
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
	metaDistSenderMethodCountTmpl = metric.Metadata{
		Name: "distsender.rpc.%s.sent",
		Help: `Number of %s requests processed.

This counts the requests in batches handed to DistSender, not the RPCs
sent to individual Ranges as a result.`,
		Measurement: "RPCs",
		Unit:        metric.Unit_COUNT,
	}
	metaDistSenderErrCountTmpl = metric.Metadata{
		Name: "distsender.rpc.err.%s",
		Help: `Number of %s errors received replica-bound RPCs

This counts how often error of the specified type was received back from replicas
as part of executing possibly range-spanning requests. Failures to reach the target
replica will be accounted for as 'roachpb.CommunicationErrType' and unclassified
errors as 'roachpb.InternalErrType'.
`,
		Measurement: "Errors",
		Unit:        metric.Unit_COUNT,
	}
	metaDistSenderRangefeedTotalRanges = metric.Metadata{
		Name: "distsender.rangefeed.total_ranges",
		Help: `Number of ranges executing rangefeed

This counts the number of ranges with an active rangefeed.
`,
		Measurement: "Ranges",
		Unit:        metric.Unit_COUNT,
	}
	metaDistSenderRangefeedCatchupRanges = metric.Metadata{
		Name: "distsender.rangefeed.catchup_ranges",
		Help: `Number of ranges in catchup mode

This counts the number of ranges with an active rangefeed that are performing catchup scan.
`,
		Measurement: "Ranges",
		Unit:        metric.Unit_COUNT,
	}
	metaDistSenderRangefeedErrorCatchupRanges = metric.Metadata{
		Name:        "distsender.rangefeed.error_catchup_ranges",
		Help:        `Number of ranges in catchup mode which experienced an error`,
		Measurement: "Ranges",
		Unit:        metric.Unit_COUNT,
	}
	metaDistSenderRangefeedRestartStuck = metric.Metadata{
		Name: "distsender.rangefeed.restart_stuck",
		Help: `Number of times a rangefeed was restarted due to not receiving ` +
			`timely updates (kv.rangefeed.range_stuck_threshold cluster setting)`,
		Measurement: "Count",
		Unit:        metric.Unit_COUNT,
	}
)

// CanSendToFollower is used by the DistSender to determine if it needs to look
// up the current lease holder for a request. It is used by the
// followerreadsccl code to inject logic to check if follower reads are enabled.
// By default, without CCL code, this function returns false.
var CanSendToFollower = func(
	_ uuid.UUID,
	_ *cluster.Settings,
	_ *hlc.Clock,
	_ roachpb.RangeClosedTimestampPolicy,
	_ *roachpb.BatchRequest,
) bool {
	return false
}

const (
	// The default limit for asynchronous senders.
	defaultSenderConcurrency = 1024
	// RangeLookupPrefetchCount is the maximum number of range descriptors to prefetch
	// during range lookups.
	RangeLookupPrefetchCount = 8
	// The maximum number of times a replica is retried when it repeatedly returns
	// stale lease info.
	sameReplicaRetryLimit = 10
)

var rangeDescriptorCacheSize = settings.RegisterIntSetting(
	settings.TenantWritable,
	"kv.range_descriptor_cache.size",
	"maximum number of entries in the range descriptor cache",
	1e6,
)

// senderConcurrencyLimit controls the maximum number of asynchronous send
// requests.
var senderConcurrencyLimit = settings.RegisterIntSetting(
	settings.TenantWritable,
	"kv.dist_sender.concurrency_limit",
	"maximum number of asynchronous send requests",
	max(defaultSenderConcurrency, int64(64*runtime.GOMAXPROCS(0))),
	settings.NonNegativeInt,
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
	SlowRPCs                *metric.Gauge
	RangefeedRanges         *metric.Gauge
	RangefeedCatchupRanges  *metric.Gauge
	RangefeedErrorCatchup   *metric.Counter
	RangefeedRestartStuck   *metric.Counter
	MethodCounts            [roachpb.NumMethods]*metric.Counter
	ErrCounts               [roachpb.NumErrors]*metric.Counter
}

func makeDistSenderMetrics() DistSenderMetrics {
	m := DistSenderMetrics{
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
		SlowRPCs:                metric.NewGauge(metaDistSenderSlowRPCs),
		RangefeedRanges:         metric.NewGauge(metaDistSenderRangefeedTotalRanges),
		RangefeedCatchupRanges:  metric.NewGauge(metaDistSenderRangefeedCatchupRanges),
		RangefeedErrorCatchup:   metric.NewCounter(metaDistSenderRangefeedErrorCatchupRanges),
		RangefeedRestartStuck:   metric.NewCounter(metaDistSenderRangefeedRestartStuck),
	}
	for i := range m.MethodCounts {
		method := roachpb.Method(i).String()
		meta := metaDistSenderMethodCountTmpl
		meta.Name = fmt.Sprintf(meta.Name, strings.ToLower(method))
		meta.Help = fmt.Sprintf(meta.Help, method)
		m.MethodCounts[i] = metric.NewCounter(meta)
	}
	for i := range m.ErrCounts {
		errType := roachpb.ErrorDetailType(i).String()
		meta := metaDistSenderErrCountTmpl
		meta.Name = fmt.Sprintf(meta.Name, strings.ToLower(errType))
		meta.Help = fmt.Sprintf(meta.Help, errType)
		m.ErrCounts[i] = metric.NewCounter(meta)
	}
	return m
}

// FirstRangeProvider is capable of providing DistSender with the descriptor of
// the first range in the cluster and notifying the DistSender when the first
// range in the cluster has changed.
type FirstRangeProvider interface {
	// GetFirstRangeDescriptor returns the RangeDescriptor for the first range
	// in the cluster.
	GetFirstRangeDescriptor() (*roachpb.RangeDescriptor, error)

	// OnFirstRangeChanged calls the provided callback when the RangeDescriptor
	// for the first range has changed.
	OnFirstRangeChanged(func(*roachpb.RangeDescriptor))
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
	// nodeDescs provides information on the KV nodes that DistSender may
	// consider routing requests to.
	nodeDescs NodeDescStore
	// metrics stored DistSender-related metrics.
	metrics DistSenderMetrics
	// rangeCache caches replica metadata for key ranges.
	rangeCache *rangecache.RangeCache
	// firstRangeProvider provides the range descriptor for range one.
	// This is not required if a RangeDescriptorDB is supplied.
	firstRangeProvider FirstRangeProvider
	transportFactory   TransportFactory
	rpcContext         *rpc.Context
	// nodeDialer allows RPC calls from the SQL layer to the KV layer.
	nodeDialer      *nodedialer.Dialer
	rpcRetryOptions retry.Options
	asyncSenderSem  *quotapool.IntPool
	// clusterID is the logical cluster ID used to verify access to enterprise features.
	// It is copied out of the rpcContext at construction time and used in
	// testing.
	logicalClusterID *base.ClusterIDContainer

	// batchInterceptor is set for tenants; when set, information about all
	// BatchRequests and BatchResponses are passed through this interceptor, which
	// can potentially throttle requests.
	kvInterceptor multitenant.TenantSideKVInterceptor

	// disableFirstRangeUpdates disables updates of the first range via
	// gossip. Used by tests which want finer control of the contents of the
	// range cache.
	disableFirstRangeUpdates int32

	// disableParallelBatches instructs DistSender to never parallelize
	// the transmission of partial batch requests across ranges.
	disableParallelBatches bool

	// LatencyFunc is used to estimate the latency to other nodes.
	latencyFunc LatencyFunc

	onRangeSpanningNonTxnalBatch func(ba *roachpb.BatchRequest) *roachpb.Error

	// locality is the description of the topography of the server on which the
	// DistSender is running. It is used to estimate the latency to other nodes
	// in the absence of a latency function.
	locality roachpb.Locality

	// If set, the DistSender will try the replicas in the order they appear in
	// the descriptor, instead of trying to reorder them by latency. The knob
	// only applies to requests sent with the LEASEHOLDER routing policy.
	dontReorderReplicas bool
	// dontConsiderConnHealth, if set, makes the GRPCTransport not take into
	// consideration the connection health when deciding the ordering for
	// replicas. When not set, replicas on nodes with unhealthy connections are
	// deprioritized.
	dontConsiderConnHealth bool

	// Currently executing range feeds.
	activeRangeFeeds sync.Map // // map[*rangeFeedRegistry]nil
}

var _ kv.Sender = &DistSender{}

// DistSenderConfig holds configuration and auxiliary objects that can be passed
// to NewDistSender.
type DistSenderConfig struct {
	AmbientCtx log.AmbientContext

	Settings  *cluster.Settings
	Clock     *hlc.Clock
	NodeDescs NodeDescStore
	// nodeDescriptor, if provided, is used to describe which node the
	// DistSender lives on, for instance when deciding where to send RPCs.
	// Usually it is filled in from the Gossip network on demand.
	nodeDescriptor  *roachpb.NodeDescriptor
	RPCRetryOptions *retry.Options
	RPCContext      *rpc.Context
	// NodeDialer is the dialer from the SQL layer to the KV layer.
	NodeDialer *nodedialer.Dialer

	// One of the following two must be provided, but not both.
	//
	// If only FirstRangeProvider is supplied, DistSender will use itself as a
	// RangeDescriptorDB and scan the meta ranges directly to satisfy range
	// lookups, using the FirstRangeProvider to bootstrap the location of the
	// meta1 range. Additionally, it will proactively update its range
	// descriptor cache with any meta1 updates from the provider.
	//
	// If only RangeDescriptorDB is provided, all range lookups will be
	// delegated to it.
	//
	// If both are provided (not required, but allowed for tests) range lookups
	// will be delegated to the RangeDescriptorDB but FirstRangeProvider will
	// still be used to listen for updates to the first range's descriptor.
	FirstRangeProvider FirstRangeProvider
	RangeDescriptorDB  rangecache.RangeDescriptorDB

	// Locality is the description of the topography of the server on which the
	// DistSender is running.
	Locality roachpb.Locality

	// KVInterceptor is set for tenants; when set, information about all
	// BatchRequests and BatchResponses are passed through this interceptor, which
	// can potentially throttle requests.
	KVInterceptor multitenant.TenantSideKVInterceptor

	TestingKnobs ClientTestingKnobs
}

// NewDistSender returns a batch.Sender instance which connects to the
// Cockroach cluster via the supplied gossip instance. Supplying a
// DistSenderContext or the fields within is optional. For omitted values, sane
// defaults will be used.
func NewDistSender(cfg DistSenderConfig) *DistSender {
	ds := &DistSender{
		st:            cfg.Settings,
		clock:         cfg.Clock,
		nodeDescs:     cfg.NodeDescs,
		metrics:       makeDistSenderMetrics(),
		kvInterceptor: cfg.KVInterceptor,
		locality:      cfg.Locality,
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
	var rdb rangecache.RangeDescriptorDB
	if cfg.FirstRangeProvider != nil {
		ds.firstRangeProvider = cfg.FirstRangeProvider
		rdb = ds
	}
	if cfg.RangeDescriptorDB != nil {
		rdb = cfg.RangeDescriptorDB
	}
	if rdb == nil {
		panic("DistSenderConfig must contain either FirstRangeProvider or RangeDescriptorDB")
	}
	getRangeDescCacheSize := func() int64 {
		return rangeDescriptorCacheSize.Get(&ds.st.SV)
	}
	ds.rangeCache = rangecache.NewRangeCache(ds.st, rdb, getRangeDescCacheSize, cfg.RPCContext.Stopper)
	if tf := cfg.TestingKnobs.TransportFactory; tf != nil {
		ds.transportFactory = tf
	} else {
		ds.transportFactory = GRPCTransportFactory
	}
	ds.dontReorderReplicas = cfg.TestingKnobs.DontReorderReplicas
	ds.dontConsiderConnHealth = cfg.TestingKnobs.DontConsiderConnHealth
	ds.rpcRetryOptions = base.DefaultRetryOptions()
	// TODO(arul): The rpcRetryOptions passed in here from server/tenant don't
	// set a max retries limit. Should they?
	if cfg.RPCRetryOptions != nil {
		ds.rpcRetryOptions = *cfg.RPCRetryOptions
	}
	if cfg.RPCContext == nil {
		panic("no RPCContext set in DistSenderConfig")
	}
	ds.rpcContext = cfg.RPCContext
	ds.nodeDialer = cfg.NodeDialer
	if ds.rpcRetryOptions.Closer == nil {
		ds.rpcRetryOptions.Closer = ds.rpcContext.Stopper.ShouldQuiesce()
	}
	ds.logicalClusterID = cfg.RPCContext.LogicalClusterID
	ds.asyncSenderSem = quotapool.NewIntPool("DistSender async concurrency",
		uint64(senderConcurrencyLimit.Get(&cfg.Settings.SV)))
	senderConcurrencyLimit.SetOnChange(&cfg.Settings.SV, func(ctx context.Context) {
		ds.asyncSenderSem.UpdateCapacity(uint64(senderConcurrencyLimit.Get(&cfg.Settings.SV)))
	})
	ds.rpcContext.Stopper.AddCloser(ds.asyncSenderSem.Closer("stopper"))

	if ds.firstRangeProvider != nil {
		ctx := ds.AnnotateCtx(context.Background())
		ds.firstRangeProvider.OnFirstRangeChanged(func(desc *roachpb.RangeDescriptor) {
			if atomic.LoadInt32(&ds.disableFirstRangeUpdates) == 1 {
				return
			}
			log.VEventf(ctx, 1, "gossiped first range descriptor: %+v", desc.Replicas())
			ds.rangeCache.EvictByKey(ctx, roachpb.RKeyMin)
		})
	}

	if cfg.TestingKnobs.LatencyFunc != nil {
		ds.latencyFunc = cfg.TestingKnobs.LatencyFunc
	} else {
		ds.latencyFunc = ds.rpcContext.RemoteClocks.Latency
	}

	if cfg.TestingKnobs.OnRangeSpanningNonTxnalBatch != nil {
		ds.onRangeSpanningNonTxnalBatch = cfg.TestingKnobs.OnRangeSpanningNonTxnalBatch
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
func (ds *DistSender) RangeDescriptorCache() *rangecache.RangeCache {
	return ds.rangeCache
}

// RangeLookup implements the RangeDescriptorDB interface.
//
// It uses LookupRange to perform a lookup scan for the provided key, using
// DistSender itself as the client.Sender. This means that the scan will recurse
// into DistSender, which will in turn use the RangeDescriptorCache again to
// lookup the RangeDescriptor necessary to perform the scan.
//
// The client has some control over the consistency of the lookup. The
// acceptable values for the consistency argument are INCONSISTENT
// or READ_UNCOMMITTED. We use INCONSISTENT for an optimistic lookup
// pass. If we don't fine a new enough descriptor, we do a leaseholder
// read at READ_UNCOMMITTED in order to read intents as well as committed
// values. The reason for this is that it's not clear whether the intent
// or the previous value points to the correct location of the Range. It gets
// even more complicated when there are split-related intents or a txn record
// co-located with a replica involved in the split. Since we cannot know the
// correct answer, we look up both the pre- and post- transaction values.
//
// Note that consistency levels CONSISTENT or INCONSISTENT will result in an
// assertion failed error. See the commentary on kv.RangeLookup for more
// details.
func (ds *DistSender) RangeLookup(
	ctx context.Context, key roachpb.RKey, rc rangecache.RangeLookupConsistency, useReverseScan bool,
) ([]roachpb.RangeDescriptor, []roachpb.RangeDescriptor, error) {
	ds.metrics.RangeLookups.Inc(1)
	switch rc {
	case roachpb.INCONSISTENT, roachpb.READ_UNCOMMITTED:
	default:
		return nil, nil, errors.AssertionFailedf("invalid consistency level %v", rc)
	}

	// By using DistSender as the sender, we guarantee that even if the desired
	// RangeDescriptor is not on the first range we send the lookup too, we'll
	// still find it when we scan to the next range. This addresses the issue
	// described in #18032 and #16266, allowing us to support meta2 splits.
	return kv.RangeLookup(ctx, ds, key.AsRawKey(), rc, RangeLookupPrefetchCount, useReverseScan)
}

// FirstRange implements the RangeDescriptorDB interface.
//
// It returns the RangeDescriptor for the first range in the cluster using the
// FirstRangeProvider, which is typically implemented using the gossip protocol
// instead of the datastore.
func (ds *DistSender) FirstRange() (*roachpb.RangeDescriptor, error) {
	if ds.firstRangeProvider == nil {
		panic("with `nil` firstRangeProvider, DistSender must not use itself as RangeDescriptorDB")
	}
	return ds.firstRangeProvider.GetFirstRangeDescriptor()
}

// getNodeID attempts to return the local node ID. It returns 0 if the DistSender
// does not have access to the Gossip network.
func (ds *DistSender) getNodeID() roachpb.NodeID {
	// Today, secondary tenants don't run in process with KV instances, so they
	// don't have access to the Gossip network. The DistSender uses the node ID to
	// preferentially route requests to a local replica (if one exists). Not
	// knowing the node ID, and thus not being able to take advantage of this
	// optimization is okay, given tenants not running in-process with KV
	// instances have no such optimization to take advantage of to begin with.
	g, ok := ds.nodeDescs.(*gossip.Gossip)
	if !ok {
		return 0
	}
	return g.NodeID.Get()
}

// CountRanges returns the number of ranges that encompass the given key span.
func (ds *DistSender) CountRanges(ctx context.Context, rs roachpb.RSpan) (int64, error) {
	var count int64
	ri := MakeRangeIterator(ds)
	for ri.Seek(ctx, rs.Key, Ascending); ri.Valid(); ri.Next(ctx) {
		count++
		if !ri.NeedAnother(rs) {
			break
		}
	}
	return count, ri.Error()
}

// getRoutingInfo looks up the range information (descriptor, lease) to use for a
// query of the key descKey with the given options. The lookup takes into
// consideration the last range descriptor that the caller had used for this key
// span, if any, and if the last range descriptor has been evicted because it
// was found to be stale, which is all managed through the EvictionToken. The
// function should be provided with an EvictionToken if one was acquired from
// this function on a previous call. If not, an empty EvictionToken can be
// provided.
//
// If useReverseScan is set and descKey is the boundary between the two ranges,
// the left range will be returned (even though descKey is actually contained on
// the right range). This is useful for ReverseScans, which call this method
// with their exclusive EndKey.
//
// The returned EvictionToken reflects the close integration between the
// DistSender and the RangeDescriptorCache; the DistSender concerns itself not
// only with consuming cached information (the descriptor and lease info come
// from the cache), but also with updating the cache.
func (ds *DistSender) getRoutingInfo(
	ctx context.Context,
	descKey roachpb.RKey,
	evictToken rangecache.EvictionToken,
	useReverseScan bool,
) (rangecache.EvictionToken, error) {
	returnToken, err := ds.rangeCache.LookupWithEvictionToken(
		ctx, descKey, evictToken, useReverseScan,
	)
	if err != nil {
		return rangecache.EvictionToken{}, err
	}

	// Sanity check: the descriptor we're about to return must include the key
	// we're interested in.
	{
		containsFn := (*roachpb.RangeDescriptor).ContainsKey
		if useReverseScan {
			containsFn = (*roachpb.RangeDescriptor).ContainsKeyInverted
		}
		if !containsFn(returnToken.Desc(), descKey) {
			log.Fatalf(ctx, "programming error: range resolution returning non-matching descriptor: "+
				"desc: %s, key: %s, reverse: %t", returnToken.Desc(), descKey, redact.Safe(useReverseScan))
		}
	}

	return returnToken, nil
}

// initAndVerifyBatch initializes timestamp-related information and
// verifies batch constraints before splitting.
func (ds *DistSender) initAndVerifyBatch(
	ctx context.Context, ba *roachpb.BatchRequest,
) *roachpb.Error {
	// Attach the local node ID to each request.
	if ba.GatewayNodeID == 0 {
		ba.GatewayNodeID = ds.getNodeID()
	}

	// Attach a clock reading from the local node to help stabilize HLCs across
	// the cluster. This is NOT required for correctness.
	ba.Now = ds.clock.NowAsClockTimestamp()

	// In the event that timestamp isn't set and read consistency isn't
	// required, set the timestamp using the local clock.
	if ba.ReadConsistency != roachpb.CONSISTENT && ba.Timestamp.IsEmpty() {
		ba.Timestamp = ba.Now.ToTimestamp()
	}

	if len(ba.Requests) < 1 {
		return roachpb.NewErrorf("empty batch")
	}

	if ba.MaxSpanRequestKeys != 0 || ba.TargetBytes != 0 {
		// Verify that the batch contains only specific range requests or the
		// EndTxnRequest. Verify that a batch with a ReverseScan only contains
		// ReverseScan range requests.
		var foundForward, foundReverse bool
		for _, req := range ba.Requests {
			inner := req.GetInner()
			switch inner.(type) {
			case *roachpb.ScanRequest, *roachpb.ResolveIntentRangeRequest,
				*roachpb.DeleteRangeRequest, *roachpb.RevertRangeRequest,
				*roachpb.ExportRequest, *roachpb.QueryLocksRequest, *roachpb.IsSpanEmptyRequest:
				// Accepted forward range requests.
				foundForward = true

			case *roachpb.ReverseScanRequest:
				// Accepted reverse range requests.
				foundReverse = true

			case *roachpb.QueryIntentRequest, *roachpb.EndTxnRequest,
				*roachpb.GetRequest, *roachpb.DeleteRequest:
				// Accepted point requests that can be in batches with limit.

			default:
				return roachpb.NewErrorf("batch with limit contains %s request", inner.Method())
			}
		}
		if foundForward && foundReverse {
			return roachpb.NewErrorf("batch with limit contains both forward and reverse scans")
		}
	}

	switch ba.WaitPolicy {
	case lock.WaitPolicy_Block, lock.WaitPolicy_Error:
		// Default. All request types supported.
	case lock.WaitPolicy_SkipLocked:
		for _, req := range ba.Requests {
			inner := req.GetInner()
			if !roachpb.CanSkipLocked(inner) {
				switch inner.(type) {
				case *roachpb.QueryIntentRequest, *roachpb.EndTxnRequest:
					// Not directly supported, but can be part of the same batch.
				default:
					return roachpb.NewErrorf("batch with SkipLocked wait policy contains %s request", inner.Method())
				}
			}
		}
	default:
		return roachpb.NewErrorf("unknown wait policy %s", ba.WaitPolicy)
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
	if len(parts) > 1 {
		unsetCanForwardReadTimestampFlag(ba)
	}

	return parts
}

// unsetCanForwardReadTimestampFlag ensures that if a batch is going to
// be split across ranges and any of its requests would need to refresh
// on read timestamp bumps, it does not have its CanForwardReadTimestamp
// flag set. It would be incorrect to allow part of a batch to perform a
// server-side refresh if another part of the batch that was sent to a
// different range would also need to refresh. Such behavior could cause
// a transaction to observe an inconsistent snapshot and violate
// serializability.
func unsetCanForwardReadTimestampFlag(ba *roachpb.BatchRequest) {
	if !ba.CanForwardReadTimestamp {
		// Already unset.
		return
	}
	for _, req := range ba.Requests {
		if roachpb.NeedsRefresh(req.GetInner()) {
			// Unset the flag.
			ba.CanForwardReadTimestamp = false
			return
		}
	}
}

// Send implements the batch.Sender interface. It subdivides the Batch
// into batches admissible for sending (preventing certain illegal
// mixtures of requests), executes each individual part (which may
// span multiple ranges), and recombines the response.
//
// When the request spans ranges, it is split by range and a partial
// subset of the batch request is sent to affected ranges in parallel.
func (ds *DistSender) Send(
	ctx context.Context, ba *roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	ds.incrementBatchCounters(ba)

	if pErr := ds.initAndVerifyBatch(ctx, ba); pErr != nil {
		return nil, pErr
	}

	ctx = ds.AnnotateCtx(ctx)
	ctx, sp := tracing.EnsureChildSpan(ctx, ds.AmbientContext.Tracer, "dist sender send")
	defer sp.Finish()

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
	parts := splitBatchAndCheckForRefreshSpans(ba, splitET)
	if len(parts) > 1 && (ba.MaxSpanRequestKeys != 0 || ba.TargetBytes != 0) {
		// We already verified above that the batch contains only scan requests of the same type.
		// Such a batch should never need splitting.
		log.Fatalf(ctx, "batch with MaxSpanRequestKeys=%d, TargetBytes=%d needs splitting",
			redact.Safe(ba.MaxSpanRequestKeys), redact.Safe(ba.TargetBytes))
	}
	var singleRplChunk [1]*roachpb.BatchResponse
	rplChunks := singleRplChunk[:0:1]

	onePart := len(parts) == 1
	errIdxOffset := 0
	for len(parts) > 0 {
		if !onePart {
			ba = ba.ShallowCopy()
			ba.Requests = parts[0]
		}
		// The minimal key range encompassing all requests contained within.
		// Local addressing has already been resolved.
		// TODO(tschottdorf): consider rudimentary validation of the batch here
		// (for example, non-range requests with EndKey, or empty key ranges).
		rs, err := keys.Range(ba.Requests)
		if err != nil {
			return nil, roachpb.NewError(err)
		}
		isReverse := ba.IsReverse()

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
			rpl, pErr = ds.divideAndSendParallelCommit(ctx, ba, rs, isReverse, 0 /* batchIdx */)
		} else {
			rpl, pErr = ds.divideAndSendBatchToRanges(ctx, ba, rs, isReverse, withCommit, 0 /* batchIdx */)
		}

		if pErr == errNo1PCTxn {
			// If we tried to send a single round-trip EndTxn but it looks like
			// it's going to hit multiple ranges, split it here and try again.
			if len(parts) != 1 {
				panic("EndTxn not in last chunk of batch")
			} else if require1PC {
				log.Fatalf(ctx, "required 1PC transaction cannot be split: %s", ba)
			}
			parts = splitBatchAndCheckForRefreshSpans(ba, true /* split ET */)
			onePart = false
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
		rplChunks = append(rplChunks, rpl)
		parts = parts[1:]
		if len(parts) > 0 {
			ba.UpdateTxn(rpl.Txn)
		}
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

// incrementBatchCounters increments the appropriate counters to track the
// batch and its composite request methods.
func (ds *DistSender) incrementBatchCounters(ba *roachpb.BatchRequest) {
	ds.metrics.BatchCount.Inc(1)
	for _, ru := range ba.Requests {
		m := ru.GetInner().Method()
		ds.metrics.MethodCounts[m].Inc(1)
	}
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
	ctx context.Context, ba *roachpb.BatchRequest, rs roachpb.RSpan, isReverse bool, batchIdx int,
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
		return ds.divideAndSendBatchToRanges(ctx, ba, rs, isReverse, true /* withCommit */, batchIdx)
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
	qiBa := ba.ShallowCopy()
	qiBa.Requests = swappedReqs[swapIdx+1:]
	qiRS, err := keys.Range(qiBa.Requests)
	if err != nil {
		return br, roachpb.NewError(err)
	}
	qiIsReverse := false // QueryIntentRequests do not carry the isReverse flag
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
		reply, pErr := ds.divideAndSendBatchToRanges(ctx, qiBa, qiRS, qiIsReverse, true /* withCommit */, qiBatchIdx)
		qiResponseCh <- response{reply: reply, positions: positions, pErr: pErr}
	}); err != nil {
		return nil, roachpb.NewError(err)
	}

	// Adjust the original batch request to ignore the pre-commit
	// QueryIntent requests. Make sure to determine the request's
	// new key span.
	ba = ba.ShallowCopy()
	ba.Requests = swappedReqs[:swapIdx+1]
	rs, err = keys.Range(ba.Requests)
	if err != nil {
		return nil, roachpb.NewError(err)
	}
	// Note that we don't need to recompute isReverse for the updated batch
	// since we only separated out QueryIntentRequests which don't carry the
	// isReverse flag.
	br, pErr = ds.divideAndSendBatchToRanges(ctx, ba, rs, isReverse, true /* withCommit */, batchIdx)

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
				return nil, roachpb.NewErrorWithTxn(err, br.Txn)
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
	ba := &roachpb.BatchRequest{}
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
		return false, roachpb.NewAmbiguousResultErrorf("error=%s [intent missing]", pErr)
	}
	resp := br.Responses[0].GetQueryTxn()
	respTxn := &resp.QueriedTxn
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
		// The transaction has either already been finalized as aborted or has been
		// finalized as committed and already had its transaction record GCed. Both
		// these cases return an ABORTED txn; in the GC case the record has been
		// synthesized.
		// If the record has been GC'ed, then we can't distinguish between the
		// two cases, and so we're forced to return an ambiguous error. On the other
		// hand, if the record exists, then we know that the transaction did not
		// commit because a committed record cannot be GC'ed and the recreated as
		// ABORTED.
		if resp.TxnRecordExists {
			return false, roachpb.NewTransactionAbortedError(roachpb.ABORT_REASON_ABORTED_RECORD_FOUND)
		}
		return false, roachpb.NewAmbiguousResultErrorf("intent missing and record aborted")
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
// isReverse indicates the direction that the provided span should be
// iterated over while sending requests. It is passed in by callers
// instead of being recomputed based on the requests in the batch to
// prevent the iteration direction from switching midway through a
// batch, in cases where partial batches recurse into this function.
//
// withCommit indicates that the batch contains a transaction commit
// or that a transaction commit is being run concurrently with this
// batch. Either way, if this is true then sendToReplicas will need
// to handle errors differently.
//
// batchIdx indicates which partial fragment of the larger batch is
// being processed by this method. It's specified as non-zero when
// this method is invoked recursively.
func (ds *DistSender) divideAndSendBatchToRanges(
	ctx context.Context,
	ba *roachpb.BatchRequest,
	rs roachpb.RSpan,
	isReverse bool,
	withCommit bool,
	batchIdx int,
) (br *roachpb.BatchResponse, pErr *roachpb.Error) {
	// Clone the BatchRequest's transaction so that future mutations to the
	// proto don't affect the proto in this batch.
	if ba.Txn != nil {
		ba.Txn = ba.Txn.Clone()
	}
	// Get initial seek key depending on direction of iteration.
	var scanDir ScanDirection
	var seekKey roachpb.RKey
	if !isReverse {
		scanDir = Ascending
		seekKey = rs.Key
	} else {
		scanDir = Descending
		seekKey = rs.EndKey
	}
	ri := MakeRangeIterator(ds)
	ri.Seek(ctx, seekKey, scanDir)
	if !ri.Valid() {
		return nil, roachpb.NewError(ri.Error())
	}
	// Take the fast path if this batch fits within a single range.
	if !ri.NeedAnother(rs) {
		resp := ds.sendPartialBatch(
			ctx, ba, rs, isReverse, withCommit, batchIdx, ri.Token(), nil, /* positions */
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
		mismatch := roachpb.NewRangeKeyMismatchErrorWithCTPolicy(ctx,
			rs.Key.AsRawKey(),
			rs.EndKey.AsRawKey(),
			ri.Desc(),
			nil, /* lease */
			ri.ClosedTimestampPolicy(),
		)
		return nil, roachpb.NewError(mismatch)
	}
	// If there's no transaction and ba spans ranges, possibly re-run as part of
	// a transaction for consistency. The case where we don't need to re-run is
	// if the read consistency is not required.
	if ba.Txn == nil {
		if ba.IsTransactional() && ba.ReadConsistency == roachpb.CONSISTENT {
			// NB: this check isn't quite right. We enter this if there's *any* transactional
			// request here, but there could be a mix (for example a DeleteRangeUsingTombstone
			// and a Put). DeleteRangeUsingTombstone gets split non-transactionally across
			// batches, so that is probably what we would want for the mixed batch as well.
			//
			// Revisit if this ever becomes something we actually want to do, for now such
			// batches will fail (re-wrapped in txn and then fail because some requests
			// don't support txns).
			return nil, roachpb.NewError(&roachpb.OpRequiresTxnError{})
		}
		if fn := ds.onRangeSpanningNonTxnalBatch; fn != nil {
			if pErr := fn(ba); pErr != nil {
				return nil, pErr
			}
		}
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
	// Make sure the CanForwardReadTimestamp flag is set to false, if necessary.
	unsetCanForwardReadTimestampFlag(ba)

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
	var resumeReason roachpb.ResumeReason
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
			fillSkippedResponses(ba, br, seekKey, resumeReason, isReverse)
		}
	}()

	canParallelize := ba.Header.MaxSpanRequestKeys == 0 && ba.Header.TargetBytes == 0 &&
		!ba.Header.ReturnOnRangeBoundary
	if ba.IsSingleCheckConsistencyRequest() {
		// Don't parallelize full checksum requests as they have to touch the
		// entirety of each replica of each range they touch.
		isExpensive := ba.Requests[0].GetCheckConsistency().Mode == roachpb.ChecksumMode_CHECK_FULL
		canParallelize = canParallelize && !isExpensive
	}

	// In several places that handle writes (kvserver.maybeStripInFlightWrites,
	// storage.replayTransactionalWrite, possibly others) we rely on requests
	// being in the original order, so the helper must preserve the order if the
	// batch is not a read-only.
	mustPreserveOrder := !ba.IsReadOnly()
	// The DistSender relies on the order of ba.Requests not being changed when
	// it sets the ResumeSpans on the incomplete requests, so we ask the helper
	// to not modify the ba.Requests slice.
	// TODO(yuzefovich): refactor the DistSender so that the truncation helper
	// could reorder requests as it pleases.
	const canReorderRequestsSlice = false
	truncationHelper, err := NewBatchTruncationHelper(
		scanDir, ba.Requests, mustPreserveOrder, canReorderRequestsSlice,
	)
	if err != nil {
		return nil, roachpb.NewError(err)
	}
	// Iterate over the ranges that the batch touches. The iteration is done in
	// key order - the order of requests in the batch is not relevant for the
	// iteration. Each iteration sends for evaluation one sub-batch to one range.
	// The sub-batch is the union of requests in the batch overlapping the
	// current range. The order of requests in a sub-batch is the same as the
	// relative order of requests in the complete batch. On the server-side,
	// requests in a sub-batch are executed in order. Depending on whether the
	// sub-batches can be executed in parallel or not, each iteration either waits
	// for the sub-batch results (in the no-parallelism case) or defers the
	// results to a channel that will be processed in the defer above.
	//
	// After each sub-batch is executed, if ba has key or memory limits (which
	// imply no parallelism), ba.MaxSpanRequestKeys and ba.TargetBytes are
	// adjusted with the responses for the sub-batch. If a limit is exhausted, the
	// loop breaks.
	for ; ri.Valid(); ri.Seek(ctx, seekKey, scanDir) {
		responseCh := make(chan response, 1)
		responseChs = append(responseChs, responseCh)

		// Truncate the request to range descriptor.
		curRangeRS, err := rs.Intersect(ri.Token().Desc().RSpan())
		if err != nil {
			responseCh <- response{pErr: roachpb.NewError(err)}
			return
		}
		curRangeBatch := ba.ShallowCopy()
		var positions []int
		curRangeBatch.Requests, positions, seekKey, err = truncationHelper.Truncate(curRangeRS)
		if len(positions) == 0 && err == nil {
			// This shouldn't happen in the wild, but some tests exercise it.
			err = errors.Newf("truncation resulted in empty batch on %s: %s", rs, ba)
		}
		if err != nil {
			responseCh <- response{pErr: roachpb.NewError(err)}
			return
		}
		nextRS := rs
		if scanDir == Ascending {
			nextRS.Key = seekKey
		} else {
			nextRS.EndKey = seekKey
		}

		lastRange := !ri.NeedAnother(rs)
		// Send the next partial batch to the first range in the "rs" span.
		// If we can reserve one of the limited goroutines available for parallel
		// batch RPCs, send asynchronously.
		if canParallelize && !lastRange && !ds.disableParallelBatches &&
			ds.sendPartialBatchAsync(ctx, curRangeBatch, curRangeRS, isReverse, withCommit, batchIdx, ri.Token(), responseCh, positions) {
			// Sent the batch asynchronously.
		} else {
			resp := ds.sendPartialBatch(
				ctx, curRangeBatch, curRangeRS, isReverse, withCommit, batchIdx, ri.Token(), positions,
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

			mightStopEarly := ba.MaxSpanRequestKeys > 0 || ba.TargetBytes > 0 || ba.ReturnOnRangeBoundary
			// Check whether we've received enough responses to exit query loop.
			if mightStopEarly {
				var replyKeys int64
				var replyBytes int64
				for _, r := range resp.reply.Responses {
					h := r.GetInner().Header()
					replyKeys += h.NumKeys
					replyBytes += h.NumBytes
					if h.ResumeSpan != nil {
						couldHaveSkippedResponses = true
						resumeReason = h.ResumeReason
						return
					}
				}
				// Update MaxSpanRequestKeys and TargetBytes, if applicable, since ba
				// might be passed recursively to further divideAndSendBatchToRanges()
				// calls.
				if ba.MaxSpanRequestKeys > 0 {
					ba.MaxSpanRequestKeys -= replyKeys
					if ba.MaxSpanRequestKeys <= 0 {
						couldHaveSkippedResponses = true
						resumeReason = roachpb.RESUME_KEY_LIMIT
						return
					}
				}
				if ba.TargetBytes > 0 {
					ba.TargetBytes -= replyBytes
					if ba.TargetBytes <= 0 {
						couldHaveSkippedResponses = true
						resumeReason = roachpb.RESUME_BYTE_LIMIT
						return
					}
				}
				// If we hit a range boundary, return a partial result if requested. We
				// do this after checking the limits, so that they take precedence.
				if ba.Header.ReturnOnRangeBoundary && replyKeys > 0 && !lastRange {
					couldHaveSkippedResponses = true
					resumeReason = roachpb.RESUME_RANGE_BOUNDARY
					return
				}
			}
		}

		// The iteration is complete if the iterator's current range encompasses
		// the remaining span, OR if the next span has inverted. This can happen
		// if this method is invoked re-entrantly due to ranges being split or
		// merged. In that case the batch request has all the original requests
		// but the span is a sub-span of the original, causing Truncate() to
		// potentially return the next seek key which inverts the span.
		if lastRange || !nextRS.Key.Less(nextRS.EndKey) {
			return
		}
		batchIdx++
		rs = nextRS
	}

	// We've exited early. Return the range iterator error.
	responseCh := make(chan response, 1)
	responseCh <- response{pErr: roachpb.NewError(ri.Error())}
	responseChs = append(responseChs, responseCh)
	return
}

// sendPartialBatchAsync sends the partial batch asynchronously if
// there aren't currently more than the allowed number of concurrent
// async requests outstanding. Returns whether the partial batch was
// sent.
func (ds *DistSender) sendPartialBatchAsync(
	ctx context.Context,
	ba *roachpb.BatchRequest,
	rs roachpb.RSpan,
	isReverse bool,
	withCommit bool,
	batchIdx int,
	routing rangecache.EvictionToken,
	responseCh chan response,
	positions []int,
) bool {
	if err := ds.rpcContext.Stopper.RunAsyncTaskEx(
		ctx,
		stop.TaskOpts{
			TaskName:   "kv.DistSender: sending partial batch",
			SpanOpt:    stop.ChildSpan,
			Sem:        ds.asyncSenderSem,
			WaitForSem: false,
		},
		func(ctx context.Context) {
			ds.metrics.AsyncSentCount.Inc(1)
			responseCh <- ds.sendPartialBatch(
				ctx, ba, rs, isReverse, withCommit, batchIdx, routing, positions,
			)
		},
	); err != nil {
		ds.metrics.AsyncThrottledCount.Inc(1)
		return false
	}
	return true
}

func slowRangeRPCWarningStr(
	s *redact.StringBuilder,
	ba *roachpb.BatchRequest,
	dur time.Duration,
	attempts int64,
	desc *roachpb.RangeDescriptor,
	err error,
	br *roachpb.BatchResponse,
) {
	resp := interface{}(err)
	if resp == nil {
		resp = br
	}
	s.Printf("have been waiting %.2fs (%d attempts) for RPC %s to %s; resp: %s",
		dur.Seconds(), attempts, ba, desc, resp)
}

func slowRangeRPCReturnWarningStr(s *redact.StringBuilder, dur time.Duration, attempts int64) {
	s.Printf("slow RPC finished after %.2fs (%d attempts)", dur.Seconds(), attempts)
}

// sendPartialBatch sends the supplied batch to the range specified by the
// routing token.
//
// The batch request is supposed to be truncated already so that it only
// contains requests which intersect the range descriptor and keys for each
// request are limited to the range's key span. The rs argument corresponds to
// the span encompassing the key ranges of all requests in the truncated batch.
// It should be entirely contained within the range descriptor of the supplied
// routing token. The positions argument describes how the given batch request
// corresponds to the original, un-truncated one, and allows us to combine the
// response later via BatchResponse.Combine. (nil positions argument should be
// used when the original batch request is fully contained within a single
// range.)
//
// The send occurs in a retry loop to handle send failures. On failure to send
// to any replicas, we backoff and retry by refetching the range descriptor. If
// the underlying range seems to have split (determined by checking if the
// supplied rs is no longer entirely contained within the refreshed range
// descriptor) we recursively invoke divideAndSendBatchToRanges to re-enumerate
// the ranges in the span and resend to each.
func (ds *DistSender) sendPartialBatch(
	ctx context.Context,
	ba *roachpb.BatchRequest,
	rs roachpb.RSpan,
	isReverse bool,
	withCommit bool,
	batchIdx int,
	routingTok rangecache.EvictionToken,
	positions []int,
) response {
	if batchIdx == 1 {
		ds.metrics.PartialBatchCount.Inc(2) // account for first batch
	} else if batchIdx > 1 {
		ds.metrics.PartialBatchCount.Inc(1)
	}
	var reply *roachpb.BatchResponse
	var pErr *roachpb.Error
	var err error

	// Start a retry loop for sending the batch to the range. Each iteration of
	// this loop uses a new descriptor. Attempts to send to multiple replicas in
	// this descriptor are done at a lower level.
	tBegin, attempts := timeutil.Now(), int64(0) // for slow log message
	// prevTok maintains the EvictionToken used on the previous iteration.
	var prevTok rangecache.EvictionToken
	for r := retry.StartWithCtx(ctx, ds.rpcRetryOptions); r.Next(); {
		attempts++
		pErr = nil
		// If we've invalidated the descriptor on a send failure, re-lookup.
		if !routingTok.Valid() {
			var descKey roachpb.RKey
			if isReverse {
				descKey = rs.EndKey
			} else {
				descKey = rs.Key
			}
			// If prevTok is Valid, then we know that it corresponded to a view of
			// a range descriptor for the range being queried. In all other cases
			// where a valid eviction token is passed to getRoutingInfo, and on
			// to RangeCache.LookupWithEvictionToken will be searching for a key
			// which is not equal to the bound of the token's descriptor. In this
			// way, we can use the eviction token as a lower-bound in terms of
			// generation when doing the lookup. This fact enables the RangeCache
			// to optimistically look up the range addressing from a follower
			// replica, while detecting hazardous cases where the follower does
			// not have the latest information and the current descriptor did
			// not result in a successful send.
			routingTok, err = ds.getRoutingInfo(ctx, descKey, prevTok, isReverse)
			if err != nil {
				log.VErrEventf(ctx, 1, "range descriptor re-lookup failed: %s", err)
				// We set pErr if we encountered an error getting the descriptor in
				// order to return the most recent error when we are out of retries.
				pErr = roachpb.NewError(err)
				if !rangecache.IsRangeLookupErrorRetryable(err) {
					return response{pErr: pErr}
				}
				continue
			}

			// See if the range shrunk. If it has, we need to sub-divide the
			// request. Note that for the resending, we use the already truncated
			// batch, so that we know that the response to it matches the positions
			// into our batch (using the full batch here would give a potentially
			// larger response slice with unknown mapping to our truncated reply).
			intersection, err := rs.Intersect(routingTok.Desc().RSpan())
			if err != nil {
				return response{pErr: roachpb.NewError(err)}
			}
			if !intersection.Equal(rs) {
				log.Eventf(ctx, "range shrunk; sub-dividing the request")
				reply, pErr = ds.divideAndSendBatchToRanges(ctx, ba, rs, isReverse, withCommit, batchIdx)
				return response{reply: reply, positions: positions, pErr: pErr}
			}
		}

		prevTok = routingTok
		reply, err = ds.sendToReplicas(ctx, ba, routingTok, withCommit)

		const slowDistSenderThreshold = time.Minute
		if dur := timeutil.Since(tBegin); dur > slowDistSenderThreshold && !tBegin.IsZero() {
			{
				var s redact.StringBuilder
				slowRangeRPCWarningStr(&s, ba, dur, attempts, routingTok.Desc(), err, reply)
				log.Warningf(ctx, "slow range RPC: %v", &s)
			}
			// If the RPC wasn't successful, defer the logging of a message once the
			// RPC is not retried any more.
			if err != nil || reply.Error != nil {
				ds.metrics.SlowRPCs.Inc(1)
				defer func(tBegin time.Time, attempts int64) {
					ds.metrics.SlowRPCs.Dec(1)
					var s redact.StringBuilder
					slowRangeRPCReturnWarningStr(&s, timeutil.Since(tBegin), attempts)
					log.Warningf(ctx, "slow RPC response: %v", &s)
				}(tBegin, attempts)
			}
			tBegin = time.Time{} // prevent reentering branch for this RPC
		}

		if err != nil {
			// Set pErr so that, if we don't perform any more retries, the
			// deduceRetryEarlyExitError() call below the loop is inhibited.
			pErr = roachpb.NewError(err)
			switch {
			case errors.HasType(err, sendError{}):
				// We've tried all the replicas without success. Either they're all
				// down, or we're using an out-of-date range descriptor. Evict from the
				// cache and try again with an updated descriptor. Re-sending the
				// request is ok even though it might have succeeded the first time
				// around because of idempotency.
				//
				// Note that we're evicting the descriptor that sendToReplicas was
				// called with, not necessarily the current descriptor from the cache.
				// Even if the routing info used by sendToReplicas was updated, we're
				// not aware of that update and that's mostly a good thing: consider
				// calling sendToReplicas with descriptor (r1,r2,r3). Inside, the
				// routing is updated to (r4,r5,r6) and sendToReplicas bails. At that
				// point, we don't want to evict (r4,r5,r6) since we haven't actually
				// used it; we're contempt attempting to evict (r1,r2,r3), failing, and
				// reloading (r4,r5,r6) from the cache on the next iteration.
				log.VEventf(ctx, 1, "evicting range desc %s after %s", routingTok, err)
				routingTok.Evict(ctx)
				continue
			}
			break
		}

		// If sending succeeded, return immediately.
		if reply.Error == nil {
			return response{reply: reply, positions: positions}
		}

		// Untangle the error from the received response.
		pErr = reply.Error
		reply.Error = nil // scrub the response error

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
		case *roachpb.RangeKeyMismatchError:
			// Range descriptor might be out of date - evict it. This is likely the
			// result of a range split. If we have new range descriptors, insert them
			// instead.
			for _, ri := range tErr.Ranges {
				// Sanity check that we got the different descriptors. Getting the same
				// descriptor and putting it in the cache would be bad, as we'd go through
				// an infinite loops of retries.
				if routingTok.Desc().RSpan().Equal(ri.Desc.RSpan()) {
					return response{pErr: roachpb.NewError(errors.AssertionFailedf(
						"mismatched range suggestion not different from original desc. desc: %s. suggested: %s. err: %s",
						routingTok.Desc(), ri.Desc, pErr))}
				}
			}
			routingTok.EvictAndReplace(ctx, tErr.Ranges...)
			// On addressing errors (likely a split), we need to re-invoke
			// the range descriptor lookup machinery, so we recurse by
			// sending batch to just the partial span this descriptor was
			// supposed to cover. Note that for the resending, we use the
			// already truncated batch, so that we know that the response
			// to it matches the positions into our batch (using the full
			// batch here would give a potentially larger response slice
			// with unknown mapping to our truncated reply).
			log.VEventf(ctx, 1, "likely split; will resend. Got new descriptors: %s", tErr.Ranges)
			reply, pErr = ds.divideAndSendBatchToRanges(ctx, ba, rs, isReverse, withCommit, batchIdx)
			return response{reply: reply, positions: positions, pErr: pErr}
		}
		break
	}

	// Propagate error if either the retry closer or context done
	// channels were closed.
	if pErr == nil {
		if err := ds.deduceRetryEarlyExitError(ctx); err == nil {
			log.Fatal(ctx, "exited retry loop without an error")
		} else {
			pErr = roachpb.NewError(err)
		}
	}

	return response{pErr: pErr}
}

func (ds *DistSender) deduceRetryEarlyExitError(ctx context.Context) error {
	select {
	case <-ds.rpcRetryOptions.Closer:
		// Typically happens during shutdown.
		return &roachpb.NodeUnavailableError{}
	case <-ctx.Done():
		// Happens when the client request is canceled.
		return errors.Wrap(ctx.Err(), "aborted in DistSender")
	default:
	}
	return nil
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
	ba *roachpb.BatchRequest,
	br *roachpb.BatchResponse,
	nextKey roachpb.RKey,
	resumeReason roachpb.ResumeReason,
	isReverse bool,
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

	// Set or correct the ResumeSpan as necessary.
	for i, resp := range br.Responses {
		req := ba.Requests[i].GetInner()
		hdr := resp.GetInner().Header()
		maybeSetResumeSpan(req, &hdr, nextKey, isReverse)
		if hdr.ResumeSpan != nil {
			hdr.ResumeReason = resumeReason
		}
		br.Responses[i].GetInner().SetHeader(hdr)
	}
}

// maybeSetResumeSpan sets or corrects the ResumeSpan in the response header, if
// necessary.
//
// nextKey is the first key that was not processed.
func maybeSetResumeSpan(
	req roachpb.Request, hdr *roachpb.ResponseHeader, nextKey roachpb.RKey, isReverse bool,
) {
	if _, ok := req.(*roachpb.GetRequest); ok {
		// This is a Get request. There are three possibilities:
		//
		//  1. The request was completed. In this case we don't want a ResumeSpan.
		//
		//  2. The request was not completed but it was part of a request that made
		//     it to a kvserver (i.e. it was part of the last range we operated on).
		//     In this case the ResumeSpan should be set by the kvserver and we can
		//     leave it alone.
		//
		//  3. The request was not completed and was not sent to a kvserver (it was
		//     beyond the last range we operated on). In this case we need to set
		//     the ResumeSpan here.
		if hdr.ResumeSpan != nil {
			// Case 2.
			return
		}
		key := req.Header().Key
		if isReverse {
			if !nextKey.Less(roachpb.RKey(key)) {
				// key <= nextKey, so this request was not completed (case 3).
				hdr.ResumeSpan = &roachpb.Span{Key: key}
			}
		} else {
			if !roachpb.RKey(key).Less(nextKey) {
				// key >= nextKey, so this request was not completed (case 3).
				hdr.ResumeSpan = &roachpb.Span{Key: key}
			}
		}
		return
	}

	if !roachpb.IsRange(req) {
		return
	}

	origHeader := req.Header()
	if isReverse {
		if hdr.ResumeSpan != nil {
			// The ResumeSpan.Key might be set to the StartKey of a range;
			// correctly set it to the Key of the original request span.
			hdr.ResumeSpan.Key = origHeader.Key
		} else if roachpb.RKey(origHeader.Key).Less(nextKey) {
			// Some keys have yet to be processed.
			hdr.ResumeSpan = new(roachpb.Span)
			*hdr.ResumeSpan = origHeader.Span()
			if nextKey.Less(roachpb.RKey(origHeader.EndKey)) {
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
			hdr.ResumeSpan.EndKey = origHeader.EndKey
		} else {
			// The request might have been fully satisfied, in which case it doesn't
			// need a ResumeSpan, or it might not have. Figure out if we're in the
			// latter case.
			if nextKey.Less(roachpb.RKey(origHeader.EndKey)) {
				// Some keys have yet to be processed.
				hdr.ResumeSpan = new(roachpb.Span)
				*hdr.ResumeSpan = origHeader.Span()
				if roachpb.RKey(origHeader.Key).Less(nextKey) {
					// The original span has been partially processed.
					hdr.ResumeSpan.Key = nextKey.AsRawKey()
				}
			}
		}
	}
}

// noMoreReplicasErr produces the error to be returned from sendToReplicas when
// the transport is exhausted.
//
// ambiguousErr, if not nil, is the error we got from the first attempt when the
// success of the request cannot be ruled out by the error. lastAttemptErr is
// the error that the last attempt to execute the request returned.
func noMoreReplicasErr(ambiguousErr, lastAttemptErr error) error {
	if ambiguousErr != nil {
		return roachpb.NewAmbiguousResultErrorf("error=%s [exhausted]", ambiguousErr)
	}

	// TODO(bdarnell): The error from the last attempt is not necessarily the best
	// one to return; we may want to remember the "best" error we've seen (for
	// example, a NotLeaseHolderError conveys more information than a
	// RangeNotFound).
	return newSendError(fmt.Sprintf("sending to all replicas failed; last error: %s", lastAttemptErr))
}

// defaultSendClosedTimestampPolicy is used when the closed timestamp policy
// is not known by the range cache. This choice prevents sending batch requests
// to only voters when a perfectly good non-voter may exist in the local
// region. It's defined as a constant here to ensure that we use the same
// value when populating the batch header.
const defaultSendClosedTimestampPolicy = roachpb.LEAD_FOR_GLOBAL_READS

// sendToReplicas sends a batch to the replicas of a range. Replicas are tried one
// at a time (generally the leaseholder first). The result of this call is
// either a BatchResponse or an error. In the former case, the BatchResponse
// wraps either a response or a *roachpb.Error; this error will come from a
// replica authorized to evaluate the request (for example ConditionFailedError)
// and can be seen as "data" returned from the request. In the latter case,
// DistSender was unable to get a response from a replica willing to evaluate
// the request, and the second return value is either a sendError or
// AmbiguousResultError. Of those two, the latter has to be passed back to the
// client, while the former should be handled by retrying with an updated range
// descriptor. This method handles other errors returned from replicas
// internally by retrying (NotLeaseholderError, RangeNotFoundError), and falls
// back to a sendError when it runs out of replicas to try.
//
// routing dictates what replicas will be tried (but not necessarily their
// order).
//
// withCommit declares whether a transaction commit is either in this batch or
// in-flight concurrently with this batch. If withCommit is false (i.e. either
// no EndTxn is in flight, or it is attempting to abort), ambiguous results will
// never be generated by method (but can still be piped through br if the
// kvserver returns an AmbiguousResultError). This is because both transactional
// writes and aborts can be retried (the former due to seqno idempotency, the
// latter because aborting is idempotent). If withCommit is true, any errors
// that do not definitively rule out the possibility that the batch could have
// succeeded are transformed into AmbiguousResultErrors.
func (ds *DistSender) sendToReplicas(
	ctx context.Context, ba *roachpb.BatchRequest, routing rangecache.EvictionToken, withCommit bool,
) (*roachpb.BatchResponse, error) {

	// If this request can be sent to a follower to perform a consistent follower
	// read under the closed timestamp, promote its routing policy to NEAREST.
	// If we don't know the closed timestamp policy, we ought to optimistically
	// assume that it's LEAD_FOR_GLOBAL_READS, because if it is, and we assumed
	// otherwise, we may send a request to a remote region unnecessarily.
	if ba.RoutingPolicy == roachpb.RoutingPolicy_LEASEHOLDER &&
		CanSendToFollower(
			ds.logicalClusterID.Get(), ds.st, ds.clock,
			routing.ClosedTimestampPolicy(defaultSendClosedTimestampPolicy), ba,
		) {
		ba = ba.ShallowCopy()
		ba.RoutingPolicy = roachpb.RoutingPolicy_NEAREST
	}
	// Filter the replicas to only those that are relevant to the routing policy.
	// NB: When changing leaseholder policy constraint_status_report should be
	// updated appropriately.
	var replicaFilter ReplicaSliceFilter
	switch ba.RoutingPolicy {
	case roachpb.RoutingPolicy_LEASEHOLDER:
		replicaFilter = OnlyPotentialLeaseholders
	case roachpb.RoutingPolicy_NEAREST:
		replicaFilter = AllExtantReplicas
	default:
		log.Fatalf(ctx, "unknown routing policy: %s", ba.RoutingPolicy)
	}
	desc := routing.Desc()
	leaseholder := routing.Leaseholder()
	replicas, err := NewReplicaSlice(ctx, ds.nodeDescs, desc, leaseholder, replicaFilter)
	if err != nil {
		return nil, err
	}

	// Rearrange the replicas so that they're ordered according to the routing
	// policy.
	var leaseholderFirst bool
	switch ba.RoutingPolicy {
	case roachpb.RoutingPolicy_LEASEHOLDER:
		// First order by latency, then move the leaseholder to the front of the
		// list, if it is known.
		if !ds.dontReorderReplicas {
			replicas.OptimizeReplicaOrder(ds.getNodeID(), ds.latencyFunc, ds.locality)
		}

		idx := -1
		if leaseholder != nil {
			idx = replicas.Find(leaseholder.ReplicaID)
		}
		if idx != -1 {
			replicas.MoveToFront(idx)
			leaseholderFirst = true
		} else {
			// The leaseholder node's info must have been missing from gossip when we
			// created replicas.
			log.VEvent(ctx, 2, "routing to nearest replica; leaseholder not known")
		}

	case roachpb.RoutingPolicy_NEAREST:
		// Order by latency.
		log.VEvent(ctx, 2, "routing to nearest replica; leaseholder not required")
		replicas.OptimizeReplicaOrder(ds.getNodeID(), ds.latencyFunc, ds.locality)

	default:
		log.Fatalf(ctx, "unknown routing policy: %s", ba.RoutingPolicy)
	}

	opts := SendOptions{
		class:                  rpc.ConnectionClassForKey(desc.RSpan().Key),
		metrics:                &ds.metrics,
		dontConsiderConnHealth: ds.dontConsiderConnHealth,
	}
	transport, err := ds.transportFactory(opts, ds.nodeDialer, replicas)
	if err != nil {
		return nil, err
	}
	defer transport.Release()

	// inTransferRetry is used to slow down retries in cases where an ongoing
	// lease transfer is suspected.
	// TODO(andrei): now that requests wait on lease transfers to complete on
	// outgoing leaseholders instead of immediately redirecting, we should
	// rethink this backoff policy.
	inTransferRetry := retry.StartWithCtx(ctx, ds.rpcRetryOptions)
	inTransferRetry.Next() // The first call to Next does not block.
	var sameReplicaRetries int
	var prevReplica roachpb.ReplicaDescriptor

	if ds.kvInterceptor != nil {
		if err := ds.kvInterceptor.OnRequestWait(ctx); err != nil {
			return nil, err
		}
	}

	// This loop will retry operations that fail with errors that reflect
	// per-replica state and may succeed on other replicas.
	var ambiguousError error
	var br *roachpb.BatchResponse
	for first := true; ; first = false {
		if !first {
			ds.metrics.NextReplicaErrCount.Inc(1)
		}

		// Advance through the transport's replicas until we find one that's still
		// part of routing.entry.Desc. The transport starts up initialized with
		// routing's replica info, but routing can be updated as we go through the
		// replicas, whereas transport isn't.
		lastErr := err
		if lastErr == nil && br != nil {
			lastErr = br.Error.GoError()
		}
		err = skipStaleReplicas(transport, routing, ambiguousError, lastErr)
		if err != nil {
			return nil, err
		}
		curReplica := transport.NextReplica()
		if first {
			if log.ExpensiveLogEnabled(ctx, 2) {
				log.VEventf(ctx, 2, "r%d: sending batch %s to %s", desc.RangeID, ba.Summary(), curReplica)
			}
		} else {
			log.VEventf(ctx, 2, "trying next peer %s", curReplica.String())
			if prevReplica == curReplica {
				sameReplicaRetries++
			} else {
				sameReplicaRetries = 0
			}
		}
		prevReplica = curReplica

		ba = ba.ShallowCopy()
		ba.Replica = curReplica
		ba.RangeID = desc.RangeID
		// Communicate to the server the information our cache has about the
		// range. If it's stale, the serve will return an update.
		ba.ClientRangeInfo = roachpb.ClientRangeInfo{
			// Note that DescriptorGeneration will be 0 if the cached descriptor
			// is "speculative" (see DescSpeculative()). Even if the speculation
			// is correct, we want the serve to return an update, at which point
			// the cached entry will no longer be "speculative".
			DescriptorGeneration: routing.Desc().Generation,
			// The LeaseSequence will be 0 if the cache doesn't have lease info,
			// or has a speculative lease. Like above, this asks the server to
			// return an update.
			LeaseSequence: routing.LeaseSeq(),
			// The ClosedTimestampPolicy will be the default if the cache
			// doesn't have info. Like above, this asks the server to return an
			// update.
			ClosedTimestampPolicy: routing.ClosedTimestampPolicy(
				defaultSendClosedTimestampPolicy,
			),

			ExplicitlyRequested: ba.ClientRangeInfo.ExplicitlyRequested,
		}
		br, err = transport.SendNext(ctx, ba)
		ds.maybeIncrementErrCounters(br, err)

		if err != nil {
			if grpcutil.IsAuthError(err) {
				// Authentication or authorization error. Propagate.
				if ambiguousError != nil {
					return nil, roachpb.NewAmbiguousResultErrorf("error=%s [propagate]", ambiguousError)
				}
				return nil, err
			}

			// For most connection errors, we cannot tell whether or not the request
			// may have succeeded on the remote server (exceptions are captured in the
			// grpcutil.RequestDidNotStart function). We'll retry the request in order
			// to attempt to eliminate the ambiguity; see below. If there's a commit
			// in the batch, we track the ambiguity more explicitly by setting
			// ambiguousError. This serves two purposes:
			// 1) the higher-level retries in the DistSender will not forget the
			// ambiguity, like they forget it for non-commit batches. This in turn
			// will ensure that TxnCoordSender-level retries don't happen across
			// commits; that'd be bad since requests are not idempotent across
			// commits.
			// TODO(andrei): This higher-level does things too bluntly, retrying only
			// in case of sendError. It should also retry in case of
			// AmbiguousRetryError as long as it makes sure to not forget about the
			// ambiguity.
			// 2) SQL recognizes AmbiguousResultErrors and gives them a special code
			// (StatementCompletionUnknown).
			// TODO(andrei): The use of this code is inconsistent because a) the
			// DistSender tries to only return the code for commits, but it'll happily
			// forward along AmbiguousResultErrors coming from the replica and b) we
			// probably should be returning that code for non-commit statements too.
			//
			// We retry requests in order to avoid returning errors (in particular,
			// AmbiguousResultError). Retrying the batch will either:
			// a) succeed if the request had not been evaluated the first time.
			// b) succeed if the request also succeeded the first time, but is
			//    idempotent (i.e. it is internal to a txn, without a commit in the
			//    batch).
			// c) fail if it succeeded the first time and the request is not
			//    idempotent. In the case of EndTxn requests, this is ensured by the
			//    tombstone keys in the timestamp cache. The retry failing does not
			//    prove that the request did not succeed the first time around, so we
			//    can't claim success (and even if we could claim success, we still
			//    wouldn't have the complete result of the successful evaluation).
			//
			// Case a) is great - the retry made the request succeed. Case b) is also
			// good; due to idempotency we managed to swallow a communication error.
			// Case c) is not great - we'll end up returning an error even though the
			// request might have succeeded (an AmbiguousResultError if withCommit is
			// set).
			//
			// TODO(andrei): Case c) is broken for non-transactional requests: nothing
			// prevents them from double evaluation. This can result in, for example,
			// an increment applying twice, or more subtle problems like a blind write
			// evaluating twice, overwriting another unrelated write that fell
			// in-between.
			//
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
				if lh := routing.Leaseholder(); lh != nil && lh.IsSame(curReplica) {
					routing.EvictLease(ctx)
				}
			}
		} else {
			// If the reply contains a timestamp, update the local HLC with it.
			if br.Error != nil {
				log.VErrEventf(ctx, 2, "%v", br.Error)
				if !br.Error.Now.IsEmpty() {
					ds.clock.Update(br.Error.Now)
				}
			} else if !br.Now.IsEmpty() {
				ds.clock.Update(br.Now)
			}

			if br.Error == nil {
				// If the server gave us updated range info, lets update our cache with it.
				if len(br.RangeInfos) > 0 {
					log.VEventf(ctx, 2, "received updated range info: %s", br.RangeInfos)
					routing.EvictAndReplace(ctx, br.RangeInfos...)
					if !ba.Header.ClientRangeInfo.ExplicitlyRequested {
						// The field is cleared by the DistSender because it refers
						// routing information not exposed by the KV API.
						br.RangeInfos = nil
					}
				}

				if ds.kvInterceptor != nil {
					numReplicas := len(desc.Replicas().Descriptors())
					reqInfo := tenantcostmodel.MakeRequestInfo(ba, numReplicas)
					respInfo := tenantcostmodel.MakeResponseInfo(br, !reqInfo.IsWrite())
					if err := ds.kvInterceptor.OnResponseWait(ctx, reqInfo, respInfo); err != nil {
						return nil, err
					}
				}

				return br, nil
			}

			// TODO(andrei): There are errors below that cause us to move to a
			// different replica without updating our caches. This means that future
			// requests will attempt the same useless replicas.
			switch tErr := br.Error.GetDetail().(type) {
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
				// leaseholder in the range cache.
			case *roachpb.NotLeaseHolderError:
				ds.metrics.NotLeaseHolderErrCount.Inc(1)
				// If we got some lease information, we use it. If not, we loop around
				// and try the next replica.
				if tErr.Lease != nil || tErr.DeprecatedLeaseHolder != nil {
					// Update the leaseholder in the range cache. Naively this would also
					// happen when the next RPC comes back, but we don't want to wait out
					// the additional RPC latency.

					var updatedLeaseholder bool
					if tErr.Lease != nil {
						updatedLeaseholder = routing.SyncTokenAndMaybeUpdateCache(ctx, tErr.Lease, &tErr.RangeDesc)
					} else if tErr.DeprecatedLeaseHolder != nil {
						updatedLeaseholder = routing.SyncTokenAndMaybeUpdateCacheWithSpeculativeLease(
							ctx, *tErr.DeprecatedLeaseHolder, &tErr.RangeDesc,
						)
					}
					// Move the new leaseholder to the head of the queue for the next
					// retry. Note that the leaseholder might not be the one indicated by
					// the NLHE we just received, in case that error carried stale info.
					if lh := routing.Leaseholder(); lh != nil {
						// If the leaseholder is the replica that we've just tried, and
						// we've tried this replica a bunch of times already, let's move on
						// and not try it again. This prevents us getting stuck on a replica
						// that we think has the lease but keeps returning redirects to us
						// (possibly because it hasn't applied its lease yet). Perhaps that
						// lease expires and someone else gets a new one, so by moving on we
						// get out of possibly infinite loops.
						if !lh.IsSame(curReplica) || sameReplicaRetries < sameReplicaRetryLimit {
							moved := transport.MoveToFront(*lh)
							if !moved {
								// The transport always includes the client's view of the
								// leaseholder when it's constructed. If the leaseholder can't
								// be found on the transport then it must be the case that the
								// routing was updated with lease information that is not
								// compatible with the range descriptor that was used to
								// construct the transport. A client may have an arbitrarily
								// stale view of the leaseholder, but it is never expected to
								// regress. As such, advancing through each replica on the
								// transport until it's exhausted is unlikely to achieve much.
								//
								// We bail early by returning a SendError. The expectation is
								// for the client to retry with a fresher eviction token.
								log.VEventf(
									ctx, 2, "transport incompatible with updated routing; bailing early",
								)
								return nil, newSendError(fmt.Sprintf("leaseholder not found in transport; last error: %s", tErr.Error()))
							}
						}
					}
					// Check whether the request was intentionally sent to a follower
					// replica to perform a follower read. In such cases, the follower
					// may reject the request with a NotLeaseHolderError if it does not
					// have a sufficient closed timestamp. In response, we should
					// immediately redirect to the leaseholder, without a backoff
					// period.
					intentionallySentToFollower := first && !leaseholderFirst
					// See if we want to backoff a little before the next attempt. If
					// the lease info we got is stale and we were intending to send to
					// the leaseholder, we backoff because it might be the case that
					// there's a lease transfer in progress and the would-be leaseholder
					// has not yet applied the new lease.
					//
					// TODO(arul): The idea here is for the client to not keep sending
					// the would-be leaseholder multiple requests and backoff a bit to let
					// it apply the its lease. Instead of deriving this information like
					// we do above, we could instead check if we're retrying the same
					// leaseholder (i.e, if the leaseholder on the routing is the same as
					// the replica we just tried), in which case we should backoff. With
					// this scheme we'd no longer have to track "updatedLeaseholder" state
					// when syncing the NLHE with the range cache.
					shouldBackoff := !updatedLeaseholder && !intentionallySentToFollower
					if shouldBackoff {
						ds.metrics.InLeaseTransferBackoffs.Inc(1)
						log.VErrEventf(ctx, 2, "backing off due to NotLeaseHolderErr with stale info")
					} else {
						inTransferRetry.Reset() // The following Next() call will not block.
					}
					inTransferRetry.Next()
				}
			default:
				if ambiguousError != nil {
					return nil, roachpb.NewAmbiguousResultErrorf("error=%s [propagate]", ambiguousError)
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
			// Don't consider this a sendError, because sendErrors indicate that we
			// were unable to reach a replica that could serve the request, and they
			// cause range cache evictions. Context cancellations just mean the
			// sender changed its mind or the request timed out.

			if ambiguousError != nil {
				err = roachpb.NewAmbiguousResultError(errors.Wrapf(ambiguousError, "context done during DistSender.Send"))
			} else {
				err = errors.Wrap(ctx.Err(), "aborted during DistSender.Send")
			}
			log.Eventf(ctx, "%v", err)
			return nil, err
		}
	}
}

func (ds *DistSender) maybeIncrementErrCounters(br *roachpb.BatchResponse, err error) {
	if err == nil && br.Error == nil {
		return
	}
	if err != nil {
		ds.metrics.ErrCounts[roachpb.CommunicationErrType].Inc(1)
	} else {
		typ := roachpb.InternalErrType
		if detail := br.Error.GetDetail(); detail != nil {
			typ = detail.Type()
		}
		ds.metrics.ErrCounts[typ].Inc(1)
	}
}

// skipStaleReplicas advances the transport until it's positioned on a replica
// that's part of routing. This is called as the DistSender tries replicas one
// by one, as the routing can be updated in the process and so the transport can
// get out of date.
//
// It's valid to pass in an empty routing, in which case the transport will be
// considered to be exhausted.
//
// Returns an error if the transport is exhausted.
func skipStaleReplicas(
	transport Transport, routing rangecache.EvictionToken, ambiguousError error, lastErr error,
) error {
	// Check whether the range cache told us that the routing info we had is
	// very out-of-date. If so, there's not much point in trying the other
	// replicas in the transport; they'll likely all return
	// RangeKeyMismatchError if there's even a replica. We'll bubble up an
	// error and try with a new descriptor.
	if !routing.Valid() {
		return noMoreReplicasErr(
			ambiguousError,
			errors.Wrap(lastErr, "routing information detected to be stale"))
	}

	for {
		if transport.IsExhausted() {
			return noMoreReplicasErr(ambiguousError, lastErr)
		}

		if _, ok := routing.Desc().GetReplicaDescriptorByID(transport.NextReplica().ReplicaID); ok {
			return nil
		}
		transport.SkipReplica()
	}
}

// A sendError indicates that there was a problem communicating with a replica
// that can evaluate the request. It's possible that the request was, in fact,
// evaluated by a replica successfully but then the server connection dropped.
//
// This error is produced by the DistSender. Note that the DistSender generates
// AmbiguousResultError instead of sendError when there's an EndTxn(commit) in
// the BatchRequest. But also note that the server can return
// AmbiguousResultErrors too, in which case the DistSender will pipe it through.
// TODO(andrei): clean up this stuff and tighten the meaning of the different
// errors.
type sendError struct {
	message string
}

// newSendError creates a sendError.
func newSendError(msg string) error {
	return sendError{message: msg}
}

// TestNewSendError creates a new sendError for the purpose of unit tests
func TestNewSendError(msg string) error {
	return newSendError(msg)
}

// SendErrorString is the prefix for all sendErrors, exported in order to
// perform cross-node error-checks.
const SendErrorString = "failed to send RPC"

func (s sendError) Error() string {
	return SendErrorString + ": " + s.message
}

// IsSendError returns true if err is a sendError.
func IsSendError(err error) bool {
	return errors.HasType(err, sendError{})
}
