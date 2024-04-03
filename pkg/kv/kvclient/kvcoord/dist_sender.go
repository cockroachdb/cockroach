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
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangecache"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcostmodel"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/startup"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
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
	metaDistSenderReplicaAddressedBatchRequestBytes = metric.Metadata{
		Name:        "distsender.batch_requests.replica_addressed.bytes",
		Help:        `Total byte count of replica-addressed batch requests processed`,
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaDistSenderReplicaAddressedBatchResponseBytes = metric.Metadata{
		Name:        "distsender.batch_responses.replica_addressed.bytes",
		Help:        `Total byte count of replica-addressed batch responses received`,
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaDistSenderCrossRegionBatchRequestBytes = metric.Metadata{
		Name: "distsender.batch_requests.cross_region.bytes",
		Help: `Total byte count of replica-addressed batch requests processed cross
		region when region tiers are configured`,
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaDistSenderCrossRegionBatchResponseBytes = metric.Metadata{
		Name: "distsender.batch_responses.cross_region.bytes",
		Help: `Total byte count of replica-addressed batch responses received cross
		region when region tiers are configured`,
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaDistSenderCrossZoneBatchRequestBytes = metric.Metadata{
		Name: "distsender.batch_requests.cross_zone.bytes",
		Help: `Total byte count of replica-addressed batch requests processed cross
		zone within the same region when region and zone tiers are configured.
		However, if the region tiers are not configured, this count may also include
		batch data sent between different regions. Ensuring consistent configuration
		of region and zone tiers across nodes helps to accurately monitor the data
		transmitted.`,
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaDistSenderCrossZoneBatchResponseBytes = metric.Metadata{
		Name: "distsender.batch_responses.cross_zone.bytes",
		Help: `Total byte count of replica-addressed batch responses received cross
		zone within the same region when region and zone tiers are configured.
		However, if the region tiers are not configured, this count may also include
		batch data received between different regions. Ensuring consistent
		configuration of region and zone tiers across nodes helps to accurately
		monitor the data transmitted.`,
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
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
		Help: `Number of range-bound RPCs currently stuck or retrying for a long time.

Note that this is not a good signal for KV health. The remote side of the
RPCs tracked here may experience contention, so an end user can easily
cause values for this metric to be emitted by leaving a transaction open
for a long time and contending with it using a second transaction.`,
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
	metaDistSenderSlowReplicaRPCs = metric.Metadata{
		Name: "distsender.slow.replicarpcs",
		Help: `Number of slow replica-bound RPCs.

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
	metaDistSenderProxySentCount = metric.Metadata{
		Name:        "distsender.rpc.proxy.sent",
		Help:        "Number of attempts by a gateway to proxy a request to an unreachable leaseholder via a follower replica.",
		Measurement: "RPCs",
		Unit:        metric.Unit_COUNT,
	}
	metaDistSenderProxyErrCount = metric.Metadata{
		Name:        "distsender.rpc.proxy.err",
		Help:        "Number of attempts by a gateway to proxy a request which resulted in a failure.",
		Measurement: "RPCs",
		Unit:        metric.Unit_COUNT,
	}
	metaDistSenderProxyForwardSentCount = metric.Metadata{
		Name:        "distsender.rpc.proxy.forward.sent",
		Help:        "Number of attempts on a follower replica to proxy a request to an unreachable leaseholder.",
		Measurement: "RPCs",
		Unit:        metric.Unit_COUNT,
	}
	metaDistSenderProxyForwardErrCount = metric.Metadata{
		Name:        "distsender.rpc.proxy.forward.err",
		Help:        "Number of attempts on a follower replica to proxy a request which resulted in a failure.",
		Measurement: "RPCs",
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
	metaDistSenderRangefeedLocalRanges = metric.Metadata{
		Name:        "distsender.rangefeed.local_ranges",
		Help:        `Number of ranges connected to local node.`,
		Measurement: "Ranges",
		Unit:        metric.Unit_COUNT,
	}
	metaDistSenderRangefeedErrorCatchupRanges = metric.Metadata{
		Name:        "distsender.rangefeed.error_catchup_ranges",
		Help:        `Number of ranges in catchup mode which experienced an error`,
		Measurement: "Ranges",
		Unit:        metric.Unit_COUNT,
	}
	metaDistSenderRangefeedRestartRanges = metric.Metadata{
		Name:        "distsender.rangefeed.restart_ranges",
		Help:        `Number of ranges that were restarted due to transient errors`,
		Measurement: "Ranges",
		Unit:        metric.Unit_COUNT,
	}

	metaDistSenderCircuitBreakerReplicasCount = metric.Metadata{
		Name:        "distsender.circuit_breaker.replicas.count",
		Help:        `Number of replicas currently tracked by DistSender circuit breakers`,
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaDistSenderCircuitBreakerReplicasTripped = metric.Metadata{
		Name:        "distsender.circuit_breaker.replicas.tripped",
		Help:        `Number of DistSender replica circuit breakers currently tripped`,
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaDistSenderCircuitBreakerReplicasTrippedEvents = metric.Metadata{
		Name:        "distsender.circuit_breaker.replicas.tripped_events",
		Help:        `Cumulative number of DistSender replica circuit breakers tripped over time`,
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaDistSenderCircuitBreakerReplicasProbesRunning = metric.Metadata{
		Name:        "distsender.circuit_breaker.replicas.probes.running",
		Help:        `Number of currently running DistSender replica circuit breaker probes`,
		Measurement: "Probes",
		Unit:        metric.Unit_COUNT,
	}
	metaDistSenderCircuitBreakerReplicasProbesSuccess = metric.Metadata{
		Name:        "distsender.circuit_breaker.replicas.probes.success",
		Help:        `Cumulative number of successful DistSender replica circuit breaker probes`,
		Measurement: "Probes",
		Unit:        metric.Unit_COUNT,
	}
	metaDistSenderCircuitBreakerReplicasProbesFailure = metric.Metadata{
		Name:        "distsender.circuit_breaker.replicas.probes.failure",
		Help:        `Cumulative number of failed DistSender replica circuit breaker probes`,
		Measurement: "Probes",
		Unit:        metric.Unit_COUNT,
	}
	metaDistSenderCircuitBreakerReplicasRequestsCancelled = metric.Metadata{
		Name:        "distsender.circuit_breaker.replicas.requests.cancelled",
		Help:        `Cumulative number of requests cancelled when DistSender replica circuit breakers trip`,
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
	metaDistSenderCircuitBreakerReplicasRequestsRejected = metric.Metadata{
		Name:        "distsender.circuit_breaker.replicas.requests.rejected",
		Help:        `Cumulative number of requests rejected by tripped DistSender replica circuit breakers`,
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
)

// metamorphicRouteToLeaseholderFirst is used to control the behavior of the
// DistSender when sending a BatchRequest.  The default behavior for a request
// that needs to run on the leaseholder is to send to the leaseholder first, and
// only send to a follower if the leaseholder is unavailable or the client has
// stale leaseholder information.  If this flag is set to false, then it will
// route the request using the default sorting logic without moving the
// leaseholder to the front of the list. This means that most requests will not
// go to the leaseholder first, and instead be sent to the leaseholder through a
// follower using a proxy request. This setting is only intended for testing to
// stress proxy behavior.
var metamorphicRouteToLeaseholderFirst = util.ConstantWithMetamorphicTestBool(
	"distsender-leaseholder-first",
	true,
)

// CanSendToFollower is used by the DistSender to determine if it needs to look
// up the current lease holder for a request. It is used by the
// followerreadsccl code to inject logic to check if follower reads are enabled.
// By default, without CCL code, this function returns false.
var CanSendToFollower = func(
	_ *cluster.Settings,
	_ *hlc.Clock,
	_ roachpb.RangeClosedTimestampPolicy,
	_ *kvpb.BatchRequest,
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
	settings.ApplicationLevel,
	"kv.range_descriptor_cache.size",
	"maximum number of entries in the range descriptor cache",
	1e6,
	// Set a minimum value to avoid a cache that is too small to be useful.
	settings.IntWithMinimum(64),
)

// senderConcurrencyLimit controls the maximum number of asynchronous send
// requests.
var senderConcurrencyLimit = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"kv.dist_sender.concurrency_limit",
	"maximum number of asynchronous send requests",
	max(defaultSenderConcurrency, int64(64*runtime.GOMAXPROCS(0))),
	settings.NonNegativeInt,
)

// FollowerReadsUnhealthy controls whether we will send follower reads to nodes
// that are not considered healthy. By default, we will sort these nodes behind
// healthy nodes.
var FollowerReadsUnhealthy = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"kv.dist_sender.follower_reads_unhealthy.enabled",
	"send follower reads to unhealthy nodes",
	true,
)

// sortByLocalityFirst controls whether we sort by locality before sorting by
// latency. If it is set to false we will only look at the latency values.
// TODO(baptist): Remove this in 25.1 once we have validated that we don't need
// to fall back to the previous behavior of only sorting by latency.
var sortByLocalityFirst = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"kv.dist_sender.sort_locality_first.enabled",
	"sort followers by locality before sorting by latency",
	true,
)

var ProxyBatchRequest = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"kv.dist_sender.proxy.enabled",
	"when true, proxy batch requests that can't be routed directly to the leaseholder",
	true,
)

// DistSenderMetrics is the set of metrics for a given distributed sender.
type DistSenderMetrics struct {
	BatchCount                         *metric.Counter
	PartialBatchCount                  *metric.Counter
	ReplicaAddressedBatchRequestBytes  *metric.Counter
	ReplicaAddressedBatchResponseBytes *metric.Counter
	CrossRegionBatchRequestBytes       *metric.Counter
	CrossRegionBatchResponseBytes      *metric.Counter
	CrossZoneBatchRequestBytes         *metric.Counter
	CrossZoneBatchResponseBytes        *metric.Counter
	AsyncSentCount                     *metric.Counter
	AsyncThrottledCount                *metric.Counter
	SentCount                          *metric.Counter
	LocalSentCount                     *metric.Counter
	NextReplicaErrCount                *metric.Counter
	NotLeaseHolderErrCount             *metric.Counter
	InLeaseTransferBackoffs            *metric.Counter
	RangeLookups                       *metric.Counter
	SlowRPCs                           *metric.Gauge
	SlowReplicaRPCs                    *metric.Counter
	ProxySentCount                     *metric.Counter
	ProxyErrCount                      *metric.Counter
	ProxyForwardSentCount              *metric.Counter
	ProxyForwardErrCount               *metric.Counter
	MethodCounts                       [kvpb.NumMethods]*metric.Counter
	ErrCounts                          [kvpb.NumErrors]*metric.Counter
	CircuitBreaker                     DistSenderCircuitBreakerMetrics
	DistSenderRangeFeedMetrics
}

// DistSenderCircuitBreakerMetrics is the set of circuit breaker metrics.
type DistSenderCircuitBreakerMetrics struct {
	Replicas                  *metric.Gauge
	ReplicasTripped           *metric.Gauge
	ReplicasTrippedEvents     *metric.Counter
	ReplicasProbesRunning     *metric.Gauge
	ReplicasProbesSuccess     *metric.Counter
	ReplicasProbesFailure     *metric.Counter
	ReplicasRequestsCancelled *metric.Counter
	ReplicasRequestsRejected  *metric.Counter
}

func (DistSenderCircuitBreakerMetrics) MetricStruct() {}

// DistSenderRangeFeedMetrics is a set of rangefeed specific metrics.
type DistSenderRangeFeedMetrics struct {
	RangefeedRanges        *metric.Gauge
	RangefeedCatchupRanges *metric.Gauge
	RangefeedLocalRanges   *metric.Gauge
	Errors                 rangeFeedErrorCounters
}

func MakeDistSenderMetrics() DistSenderMetrics {
	m := DistSenderMetrics{
		BatchCount:                         metric.NewCounter(metaDistSenderBatchCount),
		PartialBatchCount:                  metric.NewCounter(metaDistSenderPartialBatchCount),
		AsyncSentCount:                     metric.NewCounter(metaDistSenderAsyncSentCount),
		AsyncThrottledCount:                metric.NewCounter(metaDistSenderAsyncThrottledCount),
		SentCount:                          metric.NewCounter(metaTransportSentCount),
		LocalSentCount:                     metric.NewCounter(metaTransportLocalSentCount),
		ReplicaAddressedBatchRequestBytes:  metric.NewCounter(metaDistSenderReplicaAddressedBatchRequestBytes),
		ReplicaAddressedBatchResponseBytes: metric.NewCounter(metaDistSenderReplicaAddressedBatchResponseBytes),
		CrossRegionBatchRequestBytes:       metric.NewCounter(metaDistSenderCrossRegionBatchRequestBytes),
		CrossRegionBatchResponseBytes:      metric.NewCounter(metaDistSenderCrossRegionBatchResponseBytes),
		CrossZoneBatchRequestBytes:         metric.NewCounter(metaDistSenderCrossZoneBatchRequestBytes),
		CrossZoneBatchResponseBytes:        metric.NewCounter(metaDistSenderCrossZoneBatchResponseBytes),
		NextReplicaErrCount:                metric.NewCounter(metaTransportSenderNextReplicaErrCount),
		NotLeaseHolderErrCount:             metric.NewCounter(metaDistSenderNotLeaseHolderErrCount),
		InLeaseTransferBackoffs:            metric.NewCounter(metaDistSenderInLeaseTransferBackoffsCount),
		RangeLookups:                       metric.NewCounter(metaDistSenderRangeLookups),
		SlowRPCs:                           metric.NewGauge(metaDistSenderSlowRPCs),
		SlowReplicaRPCs:                    metric.NewCounter(metaDistSenderSlowReplicaRPCs),
		CircuitBreaker:                     makeDistSenderCircuitBreakerMetrics(),
		ProxySentCount:                     metric.NewCounter(metaDistSenderProxySentCount),
		ProxyErrCount:                      metric.NewCounter(metaDistSenderProxyErrCount),
		ProxyForwardSentCount:              metric.NewCounter(metaDistSenderProxyForwardSentCount),
		ProxyForwardErrCount:               metric.NewCounter(metaDistSenderProxyForwardErrCount),
		DistSenderRangeFeedMetrics:         makeDistSenderRangeFeedMetrics(),
	}
	for i := range m.MethodCounts {
		method := kvpb.Method(i).String()
		meta := metaDistSenderMethodCountTmpl
		meta.Name = fmt.Sprintf(meta.Name, strings.ToLower(method))
		meta.Help = fmt.Sprintf(meta.Help, method)
		m.MethodCounts[i] = metric.NewCounter(meta)
	}
	for i := range m.ErrCounts {
		errType := kvpb.ErrorDetailType(i).String()
		meta := metaDistSenderErrCountTmpl
		meta.Name = fmt.Sprintf(meta.Name, strings.ToLower(errType))
		meta.Help = fmt.Sprintf(meta.Help, errType)
		m.ErrCounts[i] = metric.NewCounter(meta)
	}
	return m
}

func makeDistSenderCircuitBreakerMetrics() DistSenderCircuitBreakerMetrics {
	return DistSenderCircuitBreakerMetrics{
		Replicas:                  metric.NewGauge(metaDistSenderCircuitBreakerReplicasCount),
		ReplicasTripped:           metric.NewGauge(metaDistSenderCircuitBreakerReplicasTripped),
		ReplicasTrippedEvents:     metric.NewCounter(metaDistSenderCircuitBreakerReplicasTrippedEvents),
		ReplicasProbesRunning:     metric.NewGauge(metaDistSenderCircuitBreakerReplicasProbesRunning),
		ReplicasProbesSuccess:     metric.NewCounter(metaDistSenderCircuitBreakerReplicasProbesSuccess),
		ReplicasProbesFailure:     metric.NewCounter(metaDistSenderCircuitBreakerReplicasProbesFailure),
		ReplicasRequestsCancelled: metric.NewCounter(metaDistSenderCircuitBreakerReplicasRequestsCancelled),
		ReplicasRequestsRejected:  metric.NewCounter(metaDistSenderCircuitBreakerReplicasRequestsRejected),
	}
}

// rangeFeedErrorCounters are various error related counters for rangefeed.
type rangeFeedErrorCounters struct {
	RangefeedRestartRanges *metric.Counter
	RangefeedErrorCatchup  *metric.Counter
	RetryErrors            []*metric.Counter
	Stuck                  *metric.Counter
	SendErrors             *metric.Counter
	StoreNotFound          *metric.Counter
	NodeNotFound           *metric.Counter
	RangeNotFound          *metric.Counter
	RangeKeyMismatch       *metric.Counter
}

func makeRangeFeedErrorCounters() rangeFeedErrorCounters {
	var retryCounters []*metric.Counter
	for name := range kvpb.RangeFeedRetryError_Reason_value {
		name = strings.TrimPrefix(name, "REASON_")
		retryCounters = append(retryCounters, metric.NewCounter(metric.Metadata{
			Name:        fmt.Sprintf("distsender.rangefeed.retry.%s", strings.ToLower(name)),
			Help:        fmt.Sprintf(`Number of ranges that encountered retryable %s error`, name),
			Measurement: "Ranges",
			Unit:        metric.Unit_COUNT,
		}))
	}

	retryMeta := func(name string) metric.Metadata {
		return metric.Metadata{
			Name:        fmt.Sprintf("distsender.rangefeed.retry.%s", strings.ReplaceAll(name, " ", "_")),
			Help:        fmt.Sprintf("Number of ranges that encountered retryable %s error", name),
			Measurement: "Ranges",
			Unit:        metric.Unit_COUNT,
		}
	}

	return rangeFeedErrorCounters{
		RangefeedRestartRanges: metric.NewCounter(metaDistSenderRangefeedRestartRanges),
		RangefeedErrorCatchup:  metric.NewCounter(metaDistSenderRangefeedErrorCatchupRanges),
		RetryErrors:            retryCounters,
		Stuck:                  metric.NewCounter(retryMeta("stuck")),
		SendErrors:             metric.NewCounter(retryMeta("send")),
		StoreNotFound:          metric.NewCounter(retryMeta("store not found")),
		NodeNotFound:           metric.NewCounter(retryMeta("node not found")),
		RangeNotFound:          metric.NewCounter(retryMeta("range not found")),
		RangeKeyMismatch:       metric.NewCounter(retryMeta("range key mismatch")),
	}
}

// GetRangeFeedRetryCounter returns retry reason counter for the specified reason.
// Use this method instead of direct counter access (since this method handles
// potential gaps in retry reason values).
func (c rangeFeedErrorCounters) GetRangeFeedRetryCounter(
	reason kvpb.RangeFeedRetryError_Reason,
) *metric.Counter {
	// Normally, retry reason values are contiguous.  One way gaps could be
	// introduced, is if some retry reasons are retired (deletions are
	// accomplished by reserving enum value to prevent its re-use), and then more
	// reason added after.  Then, we can't use reason value as an index into
	// retryCounters.  Because this scenario is believed to be very unlikely, we
	// forego any fancy re-mapping schemes, and instead opt for explicit handling.
	switch reason {
	case kvpb.RangeFeedRetryError_REASON_REPLICA_REMOVED,
		kvpb.RangeFeedRetryError_REASON_RANGE_SPLIT,
		kvpb.RangeFeedRetryError_REASON_RANGE_MERGED,
		kvpb.RangeFeedRetryError_REASON_RAFT_SNAPSHOT,
		kvpb.RangeFeedRetryError_REASON_LOGICAL_OPS_MISSING,
		kvpb.RangeFeedRetryError_REASON_SLOW_CONSUMER,
		kvpb.RangeFeedRetryError_REASON_NO_LEASEHOLDER,
		kvpb.RangeFeedRetryError_REASON_RANGEFEED_CLOSED:
		return c.RetryErrors[reason]
	default:
		panic(errors.AssertionFailedf("unknown retry reason %d", reason))
	}
}

func (rangeFeedErrorCounters) MetricStruct() {}

func makeDistSenderRangeFeedMetrics() DistSenderRangeFeedMetrics {
	return DistSenderRangeFeedMetrics{
		RangefeedRanges:        metric.NewGauge(metaDistSenderRangefeedTotalRanges),
		RangefeedCatchupRanges: metric.NewGauge(metaDistSenderRangefeedCatchupRanges),
		RangefeedLocalRanges:   metric.NewGauge(metaDistSenderRangefeedLocalRanges),
		Errors:                 makeRangeFeedErrorCounters(),
	}
}

// MetricStruct implements metrics.Struct interface.
func (DistSenderRangeFeedMetrics) MetricStruct() {}

// updateCrossLocalityMetricsOnReplicaAddressedBatchRequest updates
// DistSenderMetrics for batch requests that have been divided and are currently
// forwarding to a specific replica for the corresponding range. The metrics
// being updated include 1. total byte count of replica-addressed batch requests
// processed 2. cross-region metrics, which monitor activities across different
// regions, and 3. cross-zone metrics, which monitor activities across different
// zones within the same region or in cases where region tiers are not
// configured. These metrics may include batches that were not successfully sent
// but were terminated at an early stage.
func (dm *DistSenderMetrics) updateCrossLocalityMetricsOnReplicaAddressedBatchRequest(
	comparisonResult roachpb.LocalityComparisonType, inc int64,
) {
	dm.ReplicaAddressedBatchRequestBytes.Inc(inc)
	switch comparisonResult {
	case roachpb.LocalityComparisonType_CROSS_REGION:
		dm.CrossRegionBatchRequestBytes.Inc(inc)
	case roachpb.LocalityComparisonType_SAME_REGION_CROSS_ZONE:
		dm.CrossZoneBatchRequestBytes.Inc(inc)
	}
}

// updateCrossLocalityMetricsOnReplicaAddressedBatchResponse updates
// DistSenderMetrics for batch responses that are received back from transport
// rpc. It updates based on the comparisonResult parameter determined during the
// initial batch requests check. The underlying assumption is that the response
// should match the cross-region or cross-zone nature of the requests.
func (dm *DistSenderMetrics) updateCrossLocalityMetricsOnReplicaAddressedBatchResponse(
	comparisonResult roachpb.LocalityComparisonType, inc int64,
) {
	dm.ReplicaAddressedBatchResponseBytes.Inc(inc)
	switch comparisonResult {
	case roachpb.LocalityComparisonType_CROSS_REGION:
		dm.CrossRegionBatchResponseBytes.Inc(inc)
	case roachpb.LocalityComparisonType_SAME_REGION_CROSS_ZONE:
		dm.CrossZoneBatchResponseBytes.Inc(inc)
	}
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

	st      *cluster.Settings
	stopper *stop.Stopper
	// clock is used to set time for some calls. E.g. read-only ops
	// which span ranges and don't require read consistency.
	clock *hlc.Clock
	// nodeDescs provides information on the KV nodes that DistSender may
	// consider routing requests to.
	nodeDescs NodeDescStore
	// nodeIDGetter provides access to the local KV node ID if it's available
	// (0 otherwise). The default implementation uses the Gossip network if it's
	// available, but custom implementation can be provided via
	// DistSenderConfig.NodeIDGetter.
	nodeIDGetter func() roachpb.NodeID
	// metrics stored DistSender-related metrics.
	metrics DistSenderMetrics
	// rangeCache caches replica metadata for key ranges.
	rangeCache *rangecache.RangeCache
	// firstRangeProvider provides the range descriptor for range one.
	// This is not required if a RangeDescriptorDB is supplied.
	firstRangeProvider FirstRangeProvider
	transportFactory   TransportFactory
	rpcRetryOptions    retry.Options
	asyncSenderSem     *quotapool.IntPool
	circuitBreakers    *DistSenderCircuitBreakers

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

	// HealthFunc returns true if the node is alive and not draining.
	healthFunc HealthFunc

	onRangeSpanningNonTxnalBatch func(ba *kvpb.BatchRequest) *kvpb.Error

	// locality is the description of the topography of the server on which the
	// DistSender is running. It is used to estimate the latency to other nodes
	// in the absence of a latency function.
	locality roachpb.Locality

	// If set, the DistSender will try the replicas in the order they appear in
	// the descriptor, instead of trying to reorder them by latency. The knob
	// only applies to requests sent with the LEASEHOLDER routing policy.
	dontReorderReplicas bool

	routeToLeaseholderFirst bool

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
	Stopper   *stop.Stopper
	Clock     *hlc.Clock
	NodeDescs NodeDescStore
	// NodeIDGetter, if set, provides non-gossip based implementation for
	// obtaining the local KV node ID. The DistSender uses the node ID to
	// preferentially route requests to a local replica (if one exists).
	NodeIDGetter     func() roachpb.NodeID
	RPCRetryOptions  *retry.Options
	TransportFactory TransportFactory

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

	HealthFunc HealthFunc

	LatencyFunc LatencyFunc
}

// NewDistSender returns a batch.Sender instance which connects to the
// Cockroach cluster via the supplied gossip instance. Supplying a
// DistSenderContext or the fields within is optional. For omitted values, sane
// defaults will be used.
func NewDistSender(cfg DistSenderConfig) *DistSender {
	nodeIDGetter := cfg.NodeIDGetter
	if nodeIDGetter == nil {
		// Fallback to gossip-based implementation if other is not provided.
		nodeIDGetter = func() roachpb.NodeID {
			g, ok := cfg.NodeDescs.(*gossip.Gossip)
			if !ok {
				return 0
			}
			return g.NodeID.Get()
		}
	}
	ds := &DistSender{
		st:            cfg.Settings,
		stopper:       cfg.Stopper,
		clock:         cfg.Clock,
		nodeDescs:     cfg.NodeDescs,
		nodeIDGetter:  nodeIDGetter,
		metrics:       MakeDistSenderMetrics(),
		kvInterceptor: cfg.KVInterceptor,
		locality:      cfg.Locality,
		healthFunc:    cfg.HealthFunc,
		latencyFunc:   cfg.LatencyFunc,
	}
	if ds.st == nil {
		ds.st = cluster.MakeTestingClusterSettings()
	}

	ds.AmbientContext = cfg.AmbientCtx
	if ds.AmbientContext.Tracer == nil {
		panic("no tracer set in AmbientCtx")
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
	ds.rangeCache = rangecache.NewRangeCache(ds.st, rdb, getRangeDescCacheSize, cfg.Stopper)
	if cfg.TransportFactory == nil {
		panic("no TransportFactory set")
	}
	ds.transportFactory = cfg.TransportFactory
	if tf := cfg.TestingKnobs.TransportFactory; tf != nil {
		ds.transportFactory = tf(ds.transportFactory)
	}
	ds.dontReorderReplicas = cfg.TestingKnobs.DontReorderReplicas
	ds.routeToLeaseholderFirst = cfg.TestingKnobs.RouteToLeaseholderFirst || metamorphicRouteToLeaseholderFirst
	ds.dontConsiderConnHealth = cfg.TestingKnobs.DontConsiderConnHealth
	ds.rpcRetryOptions = base.DefaultRetryOptions()
	// TODO(arul): The rpcRetryOptions passed in here from server/tenant don't
	// set a max retries limit. Should they?
	if cfg.RPCRetryOptions != nil {
		ds.rpcRetryOptions = *cfg.RPCRetryOptions
	}
	if ds.rpcRetryOptions.Closer == nil {
		ds.rpcRetryOptions.Closer = cfg.Stopper.ShouldQuiesce()
	}
	ds.asyncSenderSem = quotapool.NewIntPool("DistSender async concurrency",
		uint64(senderConcurrencyLimit.Get(&ds.st.SV)))
	senderConcurrencyLimit.SetOnChange(&ds.st.SV, func(ctx context.Context) {
		ds.asyncSenderSem.UpdateCapacity(uint64(senderConcurrencyLimit.Get(&ds.st.SV)))
	})
	cfg.Stopper.AddCloser(ds.asyncSenderSem.Closer("stopper"))

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

	// Set up circuit breakers and spawn the manager goroutine, which runs until
	// the stopper stops. This can only error if the server is shutting down, so
	// ignore the returned error.
	ds.circuitBreakers = NewDistSenderCircuitBreakers(
		ds.AmbientContext, ds.stopper, ds.st, ds.transportFactory, ds.metrics)
	_ = ds.circuitBreakers.Start()

	if cfg.TestingKnobs.LatencyFunc != nil {
		ds.latencyFunc = cfg.TestingKnobs.LatencyFunc
	}
	// Some tests don't set the latencyFunc.
	if ds.latencyFunc == nil {
		ds.latencyFunc = func(roachpb.NodeID) (time.Duration, bool) {
			return time.Millisecond, true
		}
	}

	if cfg.TestingKnobs.OnRangeSpanningNonTxnalBatch != nil {
		ds.onRangeSpanningNonTxnalBatch = cfg.TestingKnobs.OnRangeSpanningNonTxnalBatch
	}

	// Some tests don't set the healthFunc.
	if ds.healthFunc == nil {
		ds.healthFunc = func(id roachpb.NodeID) bool {
			return true
		}
	}

	return ds
}

// LatencyFunc returns the LatencyFunc of the DistSender.
func (ds *DistSender) LatencyFunc() LatencyFunc {
	return ds.latencyFunc
}

// HealthFunc returns the HealthFunc of the DistSender.
func (ds *DistSender) HealthFunc() HealthFunc {
	return ds.healthFunc
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
// pass. If we don't find a new enough descriptor, we do a leaseholder
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

	// In this case, the requested key is stored in the cluster's first
	// range. Return the first range, which is always gossiped and not
	// queried from the datastore.
	if keys.RangeMetaKey(key).Equal(roachpb.RKeyMin) {
		desc, err := ds.firstRangeProvider.GetFirstRangeDescriptor()
		if err != nil {
			return nil, nil, err
		}
		return []roachpb.RangeDescriptor{*desc}, nil, nil
	}

	ds.metrics.RangeLookups.Inc(1)
	switch rc {
	case kvpb.INCONSISTENT, kvpb.READ_UNCOMMITTED:
	default:
		return nil, nil, errors.AssertionFailedf("invalid consistency level %v", rc)
	}

	// By using DistSender as the sender, we guarantee that even if the desired
	// RangeDescriptor is not on the first range we send the lookup too, we'll
	// still find it when we scan to the next range. This addresses the issue
	// described in #18032 and #16266, allowing us to support meta2 splits.
	return kv.RangeLookup(ctx, ds, key.AsRawKey(), rc, RangeLookupPrefetchCount, useReverseScan)
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
func (ds *DistSender) initAndVerifyBatch(ctx context.Context, ba *kvpb.BatchRequest) *kvpb.Error {
	// Attach the local node ID to each request.
	if ba.GatewayNodeID == 0 {
		ba.GatewayNodeID = ds.nodeIDGetter()
	}

	// Attach a clock reading from the local node to help stabilize HLCs across
	// the cluster. This is NOT required for correctness.
	ba.Now = ds.clock.NowAsClockTimestamp()

	// In the event that timestamp isn't set and read consistency isn't
	// required, set the timestamp using the local clock.
	if ba.ReadConsistency != kvpb.CONSISTENT && ba.Timestamp.IsEmpty() {
		ba.Timestamp = ba.Now.ToTimestamp()
	}

	if len(ba.Requests) < 1 {
		return kvpb.NewErrorf("empty batch")
	}

	if ba.MaxSpanRequestKeys != 0 || ba.TargetBytes != 0 {
		// Verify that the batch contains only specific range requests or the
		// EndTxnRequest. Verify that a batch with a ReverseScan only contains
		// ReverseScan range requests.
		var foundForward, foundReverse bool
		for _, req := range ba.Requests {
			inner := req.GetInner()
			switch inner.(type) {
			case *kvpb.ScanRequest, *kvpb.ResolveIntentRangeRequest,
				*kvpb.DeleteRangeRequest, *kvpb.RevertRangeRequest,
				*kvpb.ExportRequest, *kvpb.QueryLocksRequest, *kvpb.IsSpanEmptyRequest:
				// Accepted forward range requests.
				foundForward = true

			case *kvpb.ReverseScanRequest:
				// Accepted reverse range requests.
				foundReverse = true

			case *kvpb.QueryIntentRequest, *kvpb.EndTxnRequest,
				*kvpb.GetRequest, *kvpb.ResolveIntentRequest, *kvpb.DeleteRequest:
				// Accepted point requests that can be in batches with limit.

			default:
				return kvpb.NewErrorf("batch with limit contains %s request", inner.Method())
			}
		}
		if foundForward && foundReverse {
			return kvpb.NewErrorf("batch with limit contains both forward and reverse scans")
		}
	}

	switch ba.WaitPolicy {
	case lock.WaitPolicy_Block, lock.WaitPolicy_Error:
		// Default. All request types supported.
	case lock.WaitPolicy_SkipLocked:
		for _, req := range ba.Requests {
			inner := req.GetInner()
			if !kvpb.CanSkipLocked(inner) {
				switch inner.(type) {
				case *kvpb.QueryIntentRequest, *kvpb.EndTxnRequest:
					// Not directly supported, but can be part of the same batch.
				default:
					return kvpb.NewErrorf("batch with SkipLocked wait policy contains %s request", inner.Method())
				}
			}
		}
	default:
		return kvpb.NewErrorf("unknown wait policy %s", ba.WaitPolicy)
	}

	//  If the context has any pprof labels, attach them to the BatchRequest.
	//  These labels will be applied to the root context processing the request
	//  server-side, if the node processing the request is collecting a CPU
	//  profile with labels.
	pprof.ForLabels(ctx, func(key, value string) bool {
		ba.ProfileLabels = append(ba.ProfileLabels, key, value)
		return true
	})

	return nil
}

// errNo1PCTxn indicates that a batch cannot be sent as a 1 phase
// commit because it spans multiple ranges and must be split into at
// least two parts, with the final part containing the EndTxn
// request.
var errNo1PCTxn = kvpb.NewErrorf("cannot send 1PC txn to multiple ranges")

// splitBatchAndCheckForRefreshSpans splits the batch according to the
// canSplitET parameter and checks whether the batch can forward its
// read timestamp. If the batch has its CanForwardReadTimestamp flag
// set but is being split across multiple sub-batches then the flag in
// the batch header is unset.
func splitBatchAndCheckForRefreshSpans(
	ba *kvpb.BatchRequest, canSplitET bool,
) [][]kvpb.RequestUnion {
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
func unsetCanForwardReadTimestampFlag(ba *kvpb.BatchRequest) {
	if !ba.CanForwardReadTimestamp {
		// Already unset.
		return
	}
	for _, req := range ba.Requests {
		if kvpb.NeedsRefresh(req.GetInner()) {
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
	ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchResponse, *kvpb.Error) {
	startup.AssertStartupRetry(ctx)

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
	if et, ok := lastReq.(*kvpb.EndTxnRequest); ok && et.Require1PC {
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
	var singleRplChunk [1]*kvpb.BatchResponse
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
			return nil, kvpb.NewError(err)
		}
		isReverse := ba.IsReverse()

		// Determine whether this part of the BatchRequest contains a committing
		// EndTxn request.
		var withCommit, withParallelCommit bool
		if etArg, ok := ba.GetArg(kvpb.EndTxn); ok {
			et := etArg.(*kvpb.EndTxnRequest)
			withCommit = et.Commit
			withParallelCommit = et.IsParallelCommit()
		}

		var rpl *kvpb.BatchResponse
		var pErr *kvpb.Error
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

	var reply *kvpb.BatchResponse
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
func (ds *DistSender) incrementBatchCounters(ba *kvpb.BatchRequest) {
	ds.metrics.BatchCount.Inc(1)
	for _, ru := range ba.Requests {
		m := ru.GetInner().Method()
		ds.metrics.MethodCounts[m].Inc(1)
	}
}

type response struct {
	reply *kvpb.BatchResponse
	// positions argument describes how the given batch request corresponds to
	// the original, un-truncated one, and allows us to combine the response
	// later via BatchResponse.Combine. (nil positions should be used when the
	// original batch request is fully contained within a single range.)
	positions []int
	pErr      *kvpb.Error
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
	ctx context.Context, ba *kvpb.BatchRequest, rs roachpb.RSpan, isReverse bool, batchIdx int,
) (br *kvpb.BatchResponse, pErr *kvpb.Error) {
	// Search backwards, looking for the first pre-commit QueryIntent.
	swapIdx := -1
	lastIdx := len(ba.Requests) - 1
	for i := lastIdx - 1; i >= 0; i-- {
		req := ba.Requests[i].GetInner()
		if req.Method() == kvpb.QueryIntent {
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
	swappedReqs := append([]kvpb.RequestUnion(nil), ba.Requests...)
	swappedReqs[swapIdx], swappedReqs[lastIdx] = swappedReqs[lastIdx], swappedReqs[swapIdx]

	// Create a new pre-commit QueryIntent-only batch and issue it
	// in a non-limited async task. This batch may need to be split
	// over multiple ranges, so call into divideAndSendBatchToRanges.
	qiBa := ba.ShallowCopy()
	qiBa.Requests = swappedReqs[swapIdx+1:]
	qiRS, err := keys.Range(qiBa.Requests)
	if err != nil {
		return br, kvpb.NewError(err)
	}
	qiIsReverse := false // QueryIntentRequests do not carry the isReverse flag
	qiBatchIdx := batchIdx + 1
	qiResponseCh := make(chan response, 1)

	runTask := ds.stopper.RunAsyncTask
	if ds.disableParallelBatches {
		runTask = ds.stopper.RunTask
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
		return nil, kvpb.NewError(err)
	}

	// Adjust the original batch request to ignore the pre-commit
	// QueryIntent requests. Make sure to determine the request's
	// new key span.
	ba = ba.ShallowCopy()
	ba.Requests = swappedReqs[:swapIdx+1]
	rs, err = keys.Range(ba.Requests)
	if err != nil {
		return nil, kvpb.NewError(err)
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
		if _, ok := qiPErr.GetDetail().(*kvpb.IntentMissingError); ok {
			// If the error is an IntentMissingError, detect whether this is due
			// to intent resolution and can be safely ignored.
			ignoreMissing, err = ds.detectIntentMissingDueToIntentResolution(ctx, br.Txn)
			if err != nil {
				return nil, kvpb.NewErrorWithTxn(err, br.Txn)
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
			ru.GetQueryIntent().FoundUnpushedIntent = true
		}
	}

	// Both halves of the split batch succeeded. Piece them back together.
	resps := make([]kvpb.ResponseUnion, len(swappedReqs))
	copy(resps, br.Responses)
	resps[swapIdx], resps[lastIdx] = resps[lastIdx], resps[swapIdx]
	br.Responses = resps
	if err := br.Combine(ctx, qiReply.reply, qiReply.positions, ba); err != nil {
		return nil, kvpb.NewError(err)
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
	ba := &kvpb.BatchRequest{}
	ba.Timestamp = ds.clock.Now()
	ba.Add(&kvpb.QueryTxnRequest{
		RequestHeader: kvpb.RequestHeader{
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
		return false, kvpb.NewAmbiguousResultErrorf("error=%v [intent missing]", pErr)
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
			return false, kvpb.NewTransactionAbortedError(kvpb.ABORT_REASON_ABORTED_RECORD_FOUND)
		}
		return false, kvpb.NewAmbiguousResultErrorf("intent missing and record aborted")
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
func maybeSwapErrorIndex(pErr *kvpb.Error, a, b int) {
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
func mergeErrors(pErr1, pErr2 *kvpb.Error) *kvpb.Error {
	ret, drop := pErr1, pErr2
	if kvpb.ErrPriority(drop.GoError()) > kvpb.ErrPriority(ret.GoError()) {
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
	ba *kvpb.BatchRequest,
	rs roachpb.RSpan,
	isReverse bool,
	withCommit bool,
	batchIdx int,
) (br *kvpb.BatchResponse, pErr *kvpb.Error) {
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
		return nil, kvpb.NewError(ri.Error())
	}
	// Take the fast path if this batch fits within a single range.
	if !ri.NeedAnother(rs) {
		resp := ds.sendPartialBatch(
			ctx, ba, rs, isReverse, withCommit, batchIdx, ri.Token(),
		)
		// resp.positions remains nil since the original batch is fully
		// contained within a single range.
		return resp.reply, resp.pErr
	}

	// The batch spans ranges (according to our cached range descriptors).
	// Verify that this is ok.
	// TODO(tschottdorf): we should have a mechanism for discovering range
	// merges (descriptor staleness will mostly go unnoticed), or we'll be
	// turning single-range queries into multi-range queries for no good
	// reason.
	if ba.IsUnsplittable() {
		mismatch := kvpb.NewRangeKeyMismatchErrorWithCTPolicy(ctx,
			rs.Key.AsRawKey(),
			rs.EndKey.AsRawKey(),
			ri.Desc(),
			nil, /* lease */
			ri.ClosedTimestampPolicy(),
		)
		return nil, kvpb.NewError(mismatch)
	}
	// If there's no transaction and ba spans ranges, possibly re-run as part of
	// a transaction for consistency. The case where we don't need to re-run is
	// if the read consistency is not required.
	if ba.Txn == nil {
		if ba.IsTransactional() && ba.ReadConsistency == kvpb.CONSISTENT {
			// NB: this check isn't quite right. We enter this if there's *any* transactional
			// request here, but there could be a mix (for example a DeleteRangeUsingTombstone
			// and a Put). DeleteRangeUsingTombstone gets split non-transactionally across
			// batches, so that is probably what we would want for the mixed batch as well.
			//
			// Revisit if this ever becomes something we actually want to do, for now such
			// batches will fail (re-wrapped in txn and then fail because some requests
			// don't support txns).
			return nil, kvpb.NewError(&kvpb.OpRequiresTxnError{})
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
		etArg, ok := ba.GetArg(kvpb.EndTxn)
		if ok && !etArg.(*kvpb.EndTxnRequest).IsParallelCommit() {
			return nil, errNo1PCTxn
		}
	}
	// Make sure the CanForwardReadTimestamp flag is set to false, if necessary.
	unsetCanForwardReadTimestampFlag(ba)

	// Make an empty slice of responses which will be populated with responses
	// as they come in via Combine().
	br = &kvpb.BatchResponse{
		Responses: make([]kvpb.ResponseUnion, len(ba.Requests)),
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
	var resumeReason kvpb.ResumeReason
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
				// Re-map the error index within this partial batch back to its
				// position in the encompassing batch.
				if resp.pErr.Index != nil && resp.pErr.Index.Index != -1 && resp.positions != nil {
					resp.pErr.Index.Index = int32(resp.positions[resp.pErr.Index.Index])
				}
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
				if err := br.Combine(ctx, resp.reply, resp.positions, ba); err != nil {
					pErr = kvpb.NewError(err)
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
		!ba.Header.ReturnOnRangeBoundary &&
		!ba.Header.ReturnElasticCPUResumeSpans
	if ba.IsSingleCheckConsistencyRequest() {
		// Don't parallelize full checksum requests as they have to touch the
		// entirety of each replica of each range they touch.
		isExpensive := ba.Requests[0].GetCheckConsistency().Mode == kvpb.ChecksumMode_CHECK_FULL
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
		return nil, kvpb.NewError(err)
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
			responseCh <- response{pErr: kvpb.NewError(err)}
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
			responseCh <- response{pErr: kvpb.NewError(err)}
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
				ctx, curRangeBatch, curRangeRS, isReverse, withCommit, batchIdx, ri.Token(),
			)
			resp.positions = positions
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

			mightStopEarly := ba.MaxSpanRequestKeys > 0 || ba.TargetBytes > 0 || ba.ReturnOnRangeBoundary || ba.ReturnElasticCPUResumeSpans
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
						resumeReason = kvpb.RESUME_KEY_LIMIT
						return
					}
				}
				if ba.TargetBytes > 0 {
					ba.TargetBytes -= replyBytes
					if ba.TargetBytes <= 0 {
						couldHaveSkippedResponses = true
						resumeReason = kvpb.RESUME_BYTE_LIMIT
						return
					}
				}
				// If we hit a range boundary, return a partial result if requested. We
				// do this after checking the limits, so that they take precedence.
				if ba.Header.ReturnOnRangeBoundary && replyKeys > 0 && !lastRange {
					couldHaveSkippedResponses = true
					resumeReason = kvpb.RESUME_RANGE_BOUNDARY
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
	responseCh <- response{pErr: kvpb.NewError(ri.Error())}
	responseChs = append(responseChs, responseCh)
	return
}

// sendPartialBatchAsync sends the partial batch asynchronously if
// there aren't currently more than the allowed number of concurrent
// async requests outstanding. Returns whether the partial batch was
// sent.
func (ds *DistSender) sendPartialBatchAsync(
	ctx context.Context,
	ba *kvpb.BatchRequest,
	rs roachpb.RSpan,
	isReverse bool,
	withCommit bool,
	batchIdx int,
	routing rangecache.EvictionToken,
	responseCh chan response,
	positions []int,
) bool {
	if err := ds.stopper.RunAsyncTaskEx(
		ctx,
		stop.TaskOpts{
			TaskName:   "kv.DistSender: sending partial batch",
			SpanOpt:    stop.ChildSpan,
			Sem:        ds.asyncSenderSem,
			WaitForSem: false,
		},
		func(ctx context.Context) {
			ds.metrics.AsyncSentCount.Inc(1)
			resp := ds.sendPartialBatch(ctx, ba, rs, isReverse, withCommit, batchIdx, routing)
			resp.positions = positions
			responseCh <- resp
		},
	); err != nil {
		ds.metrics.AsyncThrottledCount.Inc(1)
		return false
	}
	return true
}

func slowRangeRPCWarningStr(
	s *redact.StringBuilder,
	ba *kvpb.BatchRequest,
	dur time.Duration,
	attempts int64,
	desc *roachpb.RangeDescriptor,
	err error,
	br *kvpb.BatchResponse,
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

func slowReplicaRPCWarningStr(
	s *redact.StringBuilder,
	ba *kvpb.BatchRequest,
	dur time.Duration,
	attempts int64,
	err error,
	br *kvpb.BatchResponse,
) {
	resp := interface{}(err)
	if resp == nil {
		resp = br
	}
	s.Printf("have been waiting %.2fs (%d attempts) for RPC %s to replica %s; resp: %s",
		dur.Seconds(), attempts, ba, ba.Replica, resp)
}

// ProxyFailedWithSendError is a marker to indicate a proxy request failed with
// a sendError. Node.maybeProxyRequest specifically excludes this error from
// propagation over the wire and instead return the NLHE from local evaluation.
var ProxyFailedWithSendError = kvpb.NewErrorf("proxy request failed with send error")

// sendPartialBatch sends the supplied batch to the range specified by the
// routing token.
//
// The batch request is supposed to be truncated already so that it only
// contains requests which intersect the range descriptor and keys for each
// request are limited to the range's key span. The rs argument corresponds to
// the span encompassing the key ranges of all requests in the truncated batch.
// It should be entirely contained within the range descriptor of the supplied
// routing token.
//
// The send occurs in a retry loop to handle send failures. On failure to send
// to any replicas, we backoff and retry by refetching the range descriptor. If
// the underlying range seems to have split (determined by checking if the
// supplied rs is no longer entirely contained within the refreshed range
// descriptor) we recursively invoke divideAndSendBatchToRanges to re-enumerate
// the ranges in the span and resend to each.
func (ds *DistSender) sendPartialBatch(
	ctx context.Context,
	ba *kvpb.BatchRequest,
	rs roachpb.RSpan,
	isReverse bool,
	withCommit bool,
	batchIdx int,
	routingTok rangecache.EvictionToken,
) response {
	if batchIdx == 1 {
		ds.metrics.PartialBatchCount.Inc(2) // account for first batch
	} else if batchIdx > 1 {
		ds.metrics.PartialBatchCount.Inc(1)
	}
	var reply *kvpb.BatchResponse
	var pErr *kvpb.Error
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

		// On a proxy request, update our routing information with what the
		// client sent us if the client had newer information. We have already
		// validated the client request against our local replica state in
		// node.go and reject requests with stale information. Here we ensure
		// our RangeCache has the same information as both the client request
		// and our local replica before attempting the request. If the sync
		// makes our token invalid, we handle it similarly to a RangeNotFound or
		// NotLeaseHolderError from a remote server.
		if ba.ProxyRangeInfo != nil {
			routingTok.SyncTokenAndMaybeUpdateCache(ctx, &ba.ProxyRangeInfo.Lease, &ba.ProxyRangeInfo.Desc)
		}
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
				pErr = kvpb.NewError(err)
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
				return response{pErr: kvpb.NewError(err)}
			}
			if !intersection.Equal(rs) {
				log.Eventf(ctx, "range shrunk; sub-dividing the request")
				reply, pErr = ds.divideAndSendBatchToRanges(ctx, ba, rs, isReverse, withCommit, batchIdx)
				return response{reply: reply, pErr: pErr}
			}
		}

		prevTok = routingTok
		reply, err = ds.sendToReplicas(ctx, ba, routingTok, withCommit)

		if dur := timeutil.Since(tBegin); dur > slowDistSenderRangeThreshold && !tBegin.IsZero() {
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

		if ba.ProxyRangeInfo != nil {
			// On a proxy request (when ProxyRangeInfo is set) we always return
			// the response immediately without retries. Proxy requests are
			// retried by the remote client, not the proxy node. If the error
			// contains updated range information from the leaseholder and is
			// normally retry, the client will update its cache and routing
			// information and decide how to proceed. If it decides to retry
			// against this proxy node, the proxy node will see the updated
			// range information on the retried request, and apply it to its
			// cache in the SyncTokenAndMaybeUpdateCache call a few lines above.
			//
			// TODO(baptist): Update the cache on a RangeKeyMismatchError before
			// returning the error.
			if err != nil {
				log.VEventf(ctx, 2, "failing proxy request after error %s", err)
				reply = &kvpb.BatchResponse{}
				if IsSendError(err) {
					// sendErrors shouldn't[1] escape from the DistSender. If
					// routing the request resulted in a sendError, we intercept
					// it here and replace it with a ProxyFailedWithSendError.
					//
					// [1] sendErrors are a mechanism for sendToReplicas to
					// communicate back that the routing information used to
					// route the request was stale. Therefore, the cache needs
					// to be flushed and the request should be retried with
					// fresher information. However, this is specific to the
					// proxy node's range cache -- not the remote client's range
					// cache. As such, returning the sendError back to the
					// remote client is nonsensical.
					return response{pErr: ProxyFailedWithSendError}
				} else {
					reply.Error = kvpb.NewError(kvpb.NewProxyFailedError(err))
				}
			}
			return response{reply: reply}
		}

		if err != nil {
			// Set pErr so that, if we don't perform any more retries, the
			// deduceRetryEarlyExitError() call below the loop includes this error.
			pErr = kvpb.NewError(err)
			switch {
			case IsSendError(err):
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
			return response{reply: reply}
		}

		// Untangle the error from the received response.
		pErr = reply.Error
		reply.Error = nil // scrub the response error

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
		case *kvpb.RangeKeyMismatchError:
			// Range descriptor might be out of date - evict it. This is likely the
			// result of a range split. If we have new range descriptors, insert them
			// instead.
			for _, ri := range tErr.Ranges {
				// Sanity check that we got the different descriptors. Getting the same
				// descriptor and putting it in the cache would be bad, as we'd go through
				// an infinite loops of retries.
				if routingTok.Desc().RSpan().Equal(ri.Desc.RSpan()) {
					return response{pErr: kvpb.NewError(errors.AssertionFailedf(
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
			return response{reply: reply, pErr: pErr}
		}
		break
	}

	// Propagate error if either the retry closer or context done channels were
	// closed. This replaces the return error since when the context is closed
	// the underlying error is unpredictable and might have been retried.
	if err := ds.deduceRetryEarlyExitError(ctx, pErr.GoError()); err != nil {
		log.VErrEventf(ctx, 2, "replace error %s with %s", pErr, err)
		pErr = kvpb.NewError(err)
	}

	if pErr == nil {
		log.Fatal(ctx, "exited retry loop without an error or early exit")
	}

	return response{pErr: pErr}
}

func (ds *DistSender) deduceRetryEarlyExitError(ctx context.Context, err error) error {
	// We don't need to rewrap Ambiguous errors.
	if errors.HasType(err, (*kvpb.AmbiguousResultError)(nil)) {
		return nil
	}
	select {
	case <-ds.rpcRetryOptions.Closer:
		return errors.Wrapf(kvpb.NewAmbiguousResultError(errors.CombineErrors(&kvpb.NodeUnavailableError{}, err)), "aborted in DistSender")
	case <-ctx.Done():
		return errors.Wrapf(kvpb.NewAmbiguousResultError(errors.CombineErrors(ctx.Err(), err)), "aborted in DistSender")
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
	ba *kvpb.BatchRequest,
	br *kvpb.BatchResponse,
	nextKey roachpb.RKey,
	resumeReason kvpb.ResumeReason,
	isReverse bool,
) {
	// Some requests might have no response at all if we used a batch-wide
	// limit; simply create trivial responses for those. Note that any type
	// of request can crop up here - simply take a batch that exceeds the
	// limit, and add any other requests at higher keys at the end of the
	// batch -- they'll all come back without any response since they never
	// execute.
	var scratchBA kvpb.BatchRequest
	for i := range br.Responses {
		if br.Responses[i] != (kvpb.ResponseUnion{}) {
			continue
		}
		req := ba.Requests[i].GetInner()
		// We need to summon an empty response. The most convenient (but not
		// most efficient) way is to use (*BatchRequest).CreateReply.
		//
		// TODO(tschottdorf): can autogenerate CreateReply for individual
		// requests, see kv/kvpb/gen/main.go.
		if scratchBA.Requests == nil {
			scratchBA.Requests = make([]kvpb.RequestUnion, 1)
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
	req kvpb.Request, hdr *kvpb.ResponseHeader, nextKey roachpb.RKey, isReverse bool,
) {
	if _, ok := req.(*kvpb.GetRequest); ok {
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

	if !kvpb.IsRange(req) {
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

// selectBestError produces the error to be returned from sendToReplicas when
// the transport is exhausted.
//
// ambiguousErr, if not nil, is the error we got from the first attempt when the
// success of the request cannot be ruled out by the error. lastAttemptErr is
// the error that the last attempt to execute the request returned.
// TODO(baptist): Clean up error handling by tracking errors in a struct and
// choosing the error in a more principled approach.  (for example, a
// NotLeaseHolderError conveys more information than a RangeNotFound).
func selectBestError(ambiguousErr, replicaUnavailableErr, lastAttemptErr error) error {
	if ambiguousErr != nil {
		return kvpb.NewAmbiguousResultErrorf("error=%v [exhausted] (last error: %v)",
			ambiguousErr, lastAttemptErr)
	}
	if replicaUnavailableErr != nil {
		return replicaUnavailableErr
	}

	return lastAttemptErr
}

// slowDistSenderRangeThreshold is a latency threshold for logging slow
// requests to a range, potentially involving RPCs to multiple replicas
// of the range.
const slowDistSenderRangeThreshold = time.Minute

// slowDistSenderReplicaThreshold is a latency threshold for logging a slow RPC
// to a single replica.
const slowDistSenderReplicaThreshold = 10 * time.Second

// defaultSendClosedTimestampPolicy is used when the closed timestamp policy
// is not known by the range cache. This choice prevents sending batch requests
// to only voters when a perfectly good non-voter may exist in the local
// region. It's defined as a constant here to ensure that we use the same
// value when populating the batch header.
const defaultSendClosedTimestampPolicy = roachpb.LEAD_FOR_GLOBAL_READS

// sendToReplicas sends a batch to the replicas of a range. Replicas are tried one
// at a time (generally the leaseholder first). The result of this call is
// either a BatchResponse or an error. In the former case, the BatchResponse
// wraps either a response or a *kvpb.Error; this error will come from a
// replica authorized to evaluate the request (for example ConditionFailedError)
// and can be seen as "data" returned from the request. In the latter case,
// DistSender was unable to get a response from a replica willing to evaluate
// the request, and the second return value is either a sendError or
// AmbiguousResultError. Of those two, the latter has to be passed back to the
// client, while the former should be handled by retrying with an updated range
// descriptor. This method handles other errors returned from replicas
// internally by retrying (NotLeaseHolderError, RangeNotFoundError), and falls
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
	ctx context.Context, ba *kvpb.BatchRequest, routing rangecache.EvictionToken, withCommit bool,
) (*kvpb.BatchResponse, error) {

	// If this request can be sent to a follower to perform a consistent follower
	// read under the closed timestamp, promote its routing policy to NEAREST.
	// If we don't know the closed timestamp policy, we ought to optimistically
	// assume that it's LEAD_FOR_GLOBAL_READS, because if it is, and we assumed
	// otherwise, we may send a request to a remote region unnecessarily.
	if ba.RoutingPolicy == kvpb.RoutingPolicy_LEASEHOLDER &&
		CanSendToFollower(
			ds.st, ds.clock,
			routing.ClosedTimestampPolicy(defaultSendClosedTimestampPolicy), ba,
		) {
		ba = ba.ShallowCopy()
		ba.RoutingPolicy = kvpb.RoutingPolicy_NEAREST
	}
	// Filter the replicas to only those that are relevant to the routing policy.
	// NB: When changing leaseholder policy constraint_status_report should be
	// updated appropriately.
	var replicaFilter ReplicaSliceFilter
	switch ba.RoutingPolicy {
	case kvpb.RoutingPolicy_LEASEHOLDER:
		replicaFilter = OnlyPotentialLeaseholders
	case kvpb.RoutingPolicy_NEAREST:
		replicaFilter = AllExtantReplicas
	default:
		log.Fatalf(ctx, "unknown routing policy: %s", ba.RoutingPolicy)
	}
	desc := routing.Desc()
	replicas, err := NewReplicaSlice(ctx, ds.nodeDescs, desc, routing.Leaseholder(), replicaFilter)
	if err != nil {
		return nil, err
	}

	// This client requested we proxy this request. Only proxy if we can
	// determine the leaseholder and it agrees with the ProxyRangeInfo from
	// the client. We don't support a proxy request to a non-leaseholder
	// replica. If we decide to proxy this request, we will reduce our replica
	// list to only the requested replica. If we fail on that request we fail back
	// to the caller so they can try something else.
	if ba.ProxyRangeInfo != nil {
		log.VEventf(ctx, 3, "processing a proxy request to %v", ba.ProxyRangeInfo)
		ds.metrics.ProxyForwardSentCount.Inc(1)
		// We don't know who the leaseholder is, and it is likely that the
		// client had stale information. Return our information to them through
		// a NLHE and let them retry.
		if routing.Lease().Empty() {
			log.VEventf(ctx, 2, "proxy failed, unknown leaseholder %v", routing)
			br := kvpb.BatchResponse{}
			br.Error = kvpb.NewError(
				kvpb.NewNotLeaseHolderError(roachpb.Lease{},
					0, /* proposerStoreID */
					routing.Desc(),
					"client requested a proxy but we can't figure out the leaseholder"),
			)
			ds.metrics.ProxyForwardErrCount.Inc(1)
			return &br, nil
		}
		if ba.ProxyRangeInfo.Lease.Sequence != routing.Lease().Sequence ||
			ba.ProxyRangeInfo.Desc.Generation != routing.Desc().Generation {
			log.VEventf(ctx, 2, "proxy failed, update client information %v != %v", ba.ProxyRangeInfo, routing)
			br := kvpb.BatchResponse{}
			br.Error = kvpb.NewError(
				kvpb.NewNotLeaseHolderError(
					*routing.Lease(),
					0, /* proposerStoreID */
					routing.Desc(),
					fmt.Sprintf("proxy failed, update client information %v != %v", ba.ProxyRangeInfo, routing)),
			)
			ds.metrics.ProxyForwardErrCount.Inc(1)
			return &br, nil
		}

		// On a proxy request, we only send the request to the leaseholder. If we
		// are here then the client and server agree on the routing information, so
		// use the leaseholder as our only replica to send to.
		idx := replicas.Find(routing.Leaseholder().ReplicaID)
		// This should never happen. We validated the routing above and the token
		// is still valid.
		if idx == -1 {
			return nil, errors.AssertionFailedf("inconsistent routing %v %v", desc, *routing.Leaseholder())
		}
		replicas = replicas[idx : idx+1]
		log.VEventf(ctx, 2, "sender requested proxy to leaseholder %v", replicas)
	}
	// Rearrange the replicas so that they're ordered according to the routing
	// policy.
	var routeToLeaseholder bool
	switch ba.RoutingPolicy {
	case kvpb.RoutingPolicy_LEASEHOLDER:
		// First order by latency, then move the leaseholder to the front of the
		// list, if it is known.
		if !ds.dontReorderReplicas {
			replicas.OptimizeReplicaOrder(ds.st, ds.nodeIDGetter(), ds.healthFunc, ds.latencyFunc, ds.locality)
		}

		idx := -1
		if routing.Leaseholder() != nil {
			idx = replicas.Find(routing.Leaseholder().ReplicaID)
		}
		if idx != -1 {
			if ds.routeToLeaseholderFirst {
				replicas.MoveToFront(idx)
			}
			routeToLeaseholder = true
		} else {
			// The leaseholder node's info must have been missing from gossip when we
			// created replicas.
			log.VEvent(ctx, 2, "routing to nearest replica; leaseholder not known")
		}

	case kvpb.RoutingPolicy_NEAREST:
		// Order by latency.
		log.VEvent(ctx, 2, "routing to nearest replica; leaseholder not required")
		replicas.OptimizeReplicaOrder(ds.st, ds.nodeIDGetter(), ds.healthFunc, ds.latencyFunc, ds.locality)

	default:
		log.Fatalf(ctx, "unknown routing policy: %s", ba.RoutingPolicy)
	}

	// NB: upgrade the connection class to SYSTEM, for critical ranges. Set it to
	// DEFAULT if the class is unknown, to handle mixed-version states gracefully.
	// Other kinds of overrides are possible, see rpc.ConnectionClassForKey().
	opts := SendOptions{
		class:                  rpc.ConnectionClassForKey(desc.RSpan().Key, ba.ConnectionClass),
		metrics:                &ds.metrics,
		dontConsiderConnHealth: ds.dontConsiderConnHealth,
	}
	transport := ds.transportFactory(opts, replicas)
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
	var ambiguousError, replicaUnavailableError error
	var leaseholderUnavailable bool
	var br *kvpb.BatchResponse
	attempts := int64(0)
	for first := true; ; first, attempts = false, attempts+1 {
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
		err = skipStaleReplicas(transport, routing, ambiguousError, replicaUnavailableError, lastErr)
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

		// When a sub-batch from a batch containing a commit experiences an
		// ambiguous error, it is critical to ensure subsequent replay attempts
		// do not permit changing the write timestamp, as the transaction may
		// already have been considered implicitly committed.
		ba.AmbiguousReplayProtection = ambiguousError != nil

		// In the case that the batch has already seen an ambiguous error, in
		// addition to enabling ambiguous replay protection, we also need to
		// disable the ability for the server to forward the read timestamp, as
		// the transaction may have been implicitly committed. If the intents for
		// the implicitly committed transaction were already resolved, on a replay
		// attempt encountering committed values above the read timestamp the
		// server will attempt to handle what seems to be a write-write conflict by
		// throwing a WriteTooOld, which could be refreshed away on the server if
		// the read timestamp can be moved. Disabling this ability protects against
		// refreshing away the error when retrying the ambiguous operation, instead
		// returning to the DistSender so the ambiguous error can be propagated.
		if ambiguousError != nil && ba.CanForwardReadTimestamp {
			ba.CanForwardReadTimestamp = false
		}

		// Communicate to the server the information our cache has about the
		// range. If it's stale, the server will return an update.
		ba.ClientRangeInfo = roachpb.ClientRangeInfo{
			// Note that DescriptorGeneration will be 0 if the cached descriptor
			// is "speculative" (see DescSpeculative()). Even if the speculation
			// is correct, we want the serve to return an update, at which point
			// the cached entry will no longer be "speculative".
			DescriptorGeneration: desc.Generation,
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

			// Range info is only returned when ClientRangeInfo is non-empty.
			// Explicitly request an update for speculative/missing leases and
			// descriptors, or when the client has requested it.
			ExplicitlyRequested: ba.ClientRangeInfo.ExplicitlyRequested ||
				(desc.Generation == 0 && routing.LeaseSeq() == 0),
		}

		comparisonResult := ds.getLocalityComparison(ctx, ds.nodeIDGetter(), ba.Replica.NodeID)
		ds.metrics.updateCrossLocalityMetricsOnReplicaAddressedBatchRequest(comparisonResult, int64(ba.Size()))

		// Determine whether we should proxy this request through a follower to
		// the leaseholder. The primary condition for proxying is that we would
		// like to send the request to the leaseholder, but the transport is
		// about to send the request through a follower.
		requestToSend := ba
		if !ProxyBatchRequest.Get(&ds.st.SV) {
			// The setting is disabled, so we don't proxy this request.
		} else if ba.ProxyRangeInfo != nil {
			// Clear out the proxy information to prevent the recipient from
			// sending this request onwards. This is necessary to prevent proxy
			// chaining. We want the recipient to process the request or fail
			// immediately. This is an extra safety measure to prevent any types
			// of routing loops.
			requestToSend = ba.ShallowCopy()
			requestToSend.ProxyRangeInfo = nil
		} else if !routeToLeaseholder {
			// This request isn't intended for the leaseholder so we don't proxy it.
		} else if routing.Leaseholder() == nil {
			// NB: Normally we don't have both routeToLeaseholder and a nil
			// leaseholder. This could be changed to an assertion.
			log.Errorf(ctx, "attempting %v to route to leaseholder, but the leaseholder is unknown %v", ba, routing)
		} else if ba.Replica.NodeID == routing.Leaseholder().NodeID {
			// We are sending this request to the leaseholder, so it doesn't
			// make sense to attempt to proxy it.
		} else if ds.nodeIDGetter() == ba.Replica.NodeID {
			// This condition prevents proxying a request if we are the same
			// node as the final destination. Without this we would pass through
			// the same DistSender stack again which is pointless.
		} else {
			// We passed all the conditions above and want to attempt to proxy
			// this request. We need to copy it as we are going to modify the
			// Header. For other replicas we may not end up setting the header.
			requestToSend = ba.ShallowCopy()
			rangeInfo := routing.RangeInfo()
			requestToSend.ProxyRangeInfo = &rangeInfo
			log.VEventf(ctx, 1, "attempt proxy request %v using %v", requestToSend, rangeInfo)
			ds.metrics.ProxySentCount.Inc(1)
		}

		tBegin := timeutil.Now() // for slow log message
		sendCtx, cbToken, cbErr := ds.circuitBreakers.ForReplica(desc, &curReplica).
			Track(ctx, ba, withCommit, tBegin.UnixNano())
		if cbErr != nil {
			// Circuit breaker is tripped. err will be handled below.
			err = cbErr
			transport.SkipReplica()
		} else {
			br, err = transport.SendNext(sendCtx, requestToSend)
			tEnd := timeutil.Now()
			if cancelErr := cbToken.Done(br, err, tEnd.UnixNano()); cancelErr != nil {
				// The request was cancelled by the circuit breaker tripping. If this is
				// detected by request evaluation (as opposed to the transport send), it
				// will return the context error in br.Error instead of err, which won't
				// be retried below. Instead, record it as a send error in err and retry
				// when possible. This commonly happens when the replica is local.
				br, err = nil, cancelErr
			}

			if dur := tEnd.Sub(tBegin); dur > slowDistSenderReplicaThreshold {
				var s redact.StringBuilder
				slowReplicaRPCWarningStr(&s, ba, dur, attempts, err, br)
				if admissionpb.WorkPriority(ba.AdmissionHeader.Priority) >= admissionpb.NormalPri {
					// Note that these RPC may or may not have succeeded. Errors are counted separately below.
					ds.metrics.SlowReplicaRPCs.Inc(1)
					log.Warningf(ctx, "slow replica RPC: %v", &s)
				} else {
					log.Eventf(ctx, "slow replica RPC: %v", &s)
				}
			}
		}
		if err == nil {
			if proxyErr, ok := br.Error.GetDetail().(*kvpb.ProxyFailedError); ok {
				// The server proxy attempt resulted in an error, likely a
				// communication error. Unwrap the error from the BatchResponse
				// and set err to the wrapped error. The code below will
				// correctly determine whether this error is ambiguous based on
				// the type of error and the type of request.
				err = proxyErr.Unwrap()
				log.VEventf(ctx, 2, "proxy error: %s", err)
				br = nil
			}
		}

		ds.metrics.updateCrossLocalityMetricsOnReplicaAddressedBatchResponse(comparisonResult, int64(br.Size()))
		ds.maybeIncrementErrCounters(br, err)
		if err != nil {
			if ba.ProxyRangeInfo != nil {
				ds.metrics.ProxyErrCount.Inc(1)
			} else if requestToSend.ProxyRangeInfo != nil {
				ds.metrics.ProxyForwardErrCount.Inc(1)
			}
		}

		if cbErr != nil {
			log.VErrEventf(ctx, 2, "circuit breaker error: %s", cbErr)
			// We know the request did not start, so the error is not ambiguous.

		} else if err != nil {
			log.VErrEventf(ctx, 2, "RPC error: %s", err)

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
			// Note that in case c), a request is not idempotent if the retry finds
			// the request succeeded the first time around, but requires a change to
			// the transaction's write timestamp. This is guarded against by setting
			// the AmbiguousReplayProtection flag, so that the replay is aware the
			// batch has seen an ambiguous error.
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
			// NB: If this partial batch does not contain the EndTxn request but the
			// batch contains a commit, the ambiguous error should be caught on
			// retrying the writes, should it need to be propagated.
			if withCommit && !grpcutil.RequestDidNotStart(err) {
				ambiguousError = err
			}
			// If we get a gRPC error against the leaseholder, we don't want to
			// backoff and keep trying the request against the same leaseholder.
			if lh := routing.Leaseholder(); lh != nil && lh.IsSame(curReplica) {
				leaseholderUnavailable = true
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
					var reqInfo tenantcostmodel.RequestInfo
					var respInfo tenantcostmodel.ResponseInfo
					if ba.IsWrite() {
						networkCost := ds.computeNetworkCost(ctx, desc, &curReplica, true /* isWrite */)
						reqInfo = tenantcostmodel.MakeRequestInfo(ba, len(desc.Replicas().Descriptors()), networkCost)
					}
					if !reqInfo.IsWrite() {
						networkCost := ds.computeNetworkCost(ctx, desc, &curReplica, false /* isWrite */)
						respInfo = tenantcostmodel.MakeResponseInfo(br, true, networkCost)
					}
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
			case *kvpb.StoreNotFoundError, *kvpb.NodeUnavailableError:
				// These errors are likely to be unique to the replica that reported
				// them, so no action is required before the next retry.
			case *kvpb.RangeNotFoundError:
				// The store we routed to doesn't have this replica. This can happen when
				// our descriptor is outright outdated, but it can also be caused by a
				// replica that has just been added but needs a snapshot to be caught up.
				//
				// We'll try other replicas which typically gives us the leaseholder, either
				// via the NotLeaseHolderError or nil error paths, both of which update the
				// leaseholder in the range cache.
			case *kvpb.ReplicaUnavailableError:
				// The replica's circuit breaker is tripped. This only means that this
				// replica is unable to propose writes -- the range may or may not be
				// available with a quorum elsewhere (e.g. in the case of a partial
				// network partition or stale replica). There are several possibilities:
				//
				// 0. This replica knows about a valid leaseholder elsewhere. It will
				//    return a NLHE instead of a RUE even with a tripped circuit
				//    breaker, so we'll take that branch instead and retry the
				//    leaseholder. We list this case explicitly, as a reminder.
				//
				// 1. This replica is the current leaseholder. The range cache can't
				//    tell us with certainty who the leaseholder is, so we try other
				//    replicas in case a lease exists elsewhere, or error out if
				//    unsuccessful. If we get an NLHE pointing back to this one we
				//    ignore it.
				//
				// 2. This replica is the current Raft leader, but it has lost quorum
				//    (prior to stepping down via CheckQuorum). We go on to try other
				//    replicas, which may return a NLHE pointing back to the leader
				//    instead of attempting to acquire a lease, which we'll ignore.
				//
				// 3. This replica does not know about a current quorum or leaseholder,
				//    but one does exist elsewhere. Try other replicas to discover it,
				//    but error out if it's unreachable.
				//
				// 4. There is no quorum nor lease. Error out after trying all replicas.
				//
				// To handle these cases, we track RUEs in *replicaUnavailableError.
				// This contains either:
				//
				// - The last RUE we received from a supposed leaseholder, as given by
				//   the range cache via routing.Leaseholder() at the time of the error.
				//
				// - Otherwise, the first RUE we received from any replica.
				//
				// If, when retrying a later replica, we receive a NLHE pointing to the
				// same replica as the RUE, we ignore the NLHE and move on to the next
				// replica. This also handles the case where a NLHE points to a new
				// leaseholder and that leaseholder returns RUE, in which case the next
				// NLHE will be ignored.
				//
				// If we saw a RUE we'll error out after we've tried all replicas.
				//
				// NB: we can't return tErr directly, because GetDetail() strips error
				// marks from the error (e.g. ErrBreakerOpen).
				if !tErr.Replica.IsSame(curReplica) {
					// The ReplicaUnavailableError may have been proxied via this replica.
					// This can happen e.g. if the replica has to access a txn record on a
					// different range during intent resolution, and that range returns a
					// RUE. In this case we just return it directly, as br.Error.
					return br, nil
				}
				if lh := routing.Leaseholder(); lh != nil && lh.IsSame(curReplica) {
					// This error came from the supposed leaseholder. Record it, such that
					// subsequent NLHEs pointing back to this one can be ignored instead
					// of getting stuck in a retry loop. This ensures we'll eventually
					// error out when the transport is exhausted even if multiple replicas
					// return NLHEs to different replicas all returning RUEs.
					replicaUnavailableError = br.Error.GoError()
					leaseholderUnavailable = true
				} else if replicaUnavailableError == nil {
					// This is the first time we see a RUE. Record it, such that we'll
					// return it if all other replicas fail (regardless of error).
					replicaUnavailableError = br.Error.GoError()
				}
				// The circuit breaker may have tripped while a commit proposal was in
				// flight, so we have to mark it as ambiguous as well.
				if withCommit && ambiguousError == nil {
					ambiguousError = br.Error.GoError()
				}
			case *kvpb.NotLeaseHolderError:
				ds.metrics.NotLeaseHolderErrCount.Inc(1)
				// Update the leaseholder in the range cache. Naively this would also
				// happen when the next RPC comes back, but we don't want to wait out
				// the additional RPC latency.
				var oldLeaseholder = routing.Leaseholder()
				if oldLeaseholder == nil {
					oldLeaseholder = &roachpb.ReplicaDescriptor{}
				}
				errDesc := &tErr.RangeDesc
				errLease := tErr.Lease
				// TODO(baptist): Many tests don't set the RangeDesc on NLHE. The
				// desired behavior is undefined if it is not set. Ideally all tests
				// should be updated to call NewNotLeaseHolderError
				if errDesc.RangeID == 0 {
					errDesc = routing.Desc()
				}
				// If there is no lease in the error, use an empty lease
				if tErr.Lease == nil {
					errLease = &roachpb.Lease{}
				}
				routing.SyncTokenAndMaybeUpdateCache(ctx, errLease, errDesc)
				// Note that the leaseholder might not be the one indicated by
				// the NLHE we just received, in case that error carried stale info.
				lh := routing.Leaseholder()
				// If we got new information, we use it. If not, we loop around and try
				// the next replica.
				if lh != nil && !errLease.Empty() {
					updatedLeaseholder := !lh.IsSame(*oldLeaseholder)
					// If we changed leaseholder, reset whether we think the
					// leaseholder was unavailable. In the worst case this will
					// cause an extra pass through sendToReplicas, but it
					// prevents accidentally returning a replica unavailable
					// error too aggressively.
					if updatedLeaseholder {
						leaseholderUnavailable = false
						routeToLeaseholder = true
						// If we changed the leaseholder, reset the transport to try all the
						// replicas in order again. After a leaseholder change, requests to
						// followers will be marked as potential proxy requests and point to
						// the new leaseholder. We need to try all the replicas again before
						// giving up.
						// NB: We reset and retry here because if we release a sendError to
						// the caller, it will call Evict and evict the leaseholder
						// information we just learned from this error.
						// TODO(baptist): If sendPartialBatch didn't evict valid range
						// information we would not need to reset the transport here.
						transport.Reset()
					}
					// If the leaseholder is the replica that we've just tried, and
					// we've tried this replica a bunch of times already, let's move on
					// and not try it again. This prevents us getting stuck on a replica
					// that we think has the lease but keeps returning redirects to us
					// (possibly because it hasn't applied its lease yet). Perhaps that
					// lease expires and someone else gets a new one, so by moving on we
					// get out of possibly infinite loops.
					if (!lh.IsSame(curReplica) || sameReplicaRetries < sameReplicaRetryLimit) && !leaseholderUnavailable {
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
							// We bail early by returning the best error we have
							// seen so far. The expectation is for the client to
							// retry with a fresher eviction token if possible.
							log.VEventf(
								ctx, 2, "transport incompatible with updated routing; bailing early",
							)
							return nil, selectBestError(
								ambiguousError,
								replicaUnavailableError,
								newSendError(errors.Wrap(tErr, "leaseholder not found in transport; last error")),
							)
						}
					}
					// Check whether the request was intentionally sent to a follower
					// replica to perform a follower read. In such cases, the follower
					// may reject the request with a NotLeaseHolderError if it does not
					// have a sufficient closed timestamp. In response, we should
					// immediately redirect to the leaseholder, without a backoff
					// period.
					intentionallySentToFollower := first && !routeToLeaseholder
					// See if we want to backoff a little before the next attempt. If
					// the lease info we got is stale and we were intending to send to
					// the leaseholder, we backoff because it might be the case that
					// there's a lease transfer in progress and the would-be leaseholder
					// has not yet applied the new lease.
					//
					// If the leaseholder is unavailable, we don't backoff since we aren't
					// trying the leaseholder again, instead we want to cycle through the
					// remaining replicas to see if they have the lease before we return
					// to sendPartialBatch.
					//
					// TODO(arul): The idea here is for the client to not keep sending
					// the would-be leaseholder multiple requests and backoff a bit to let
					// it apply the its lease. Instead of deriving this information like
					// we do above, we could instead check if we're retrying the same
					// leaseholder (i.e, if the leaseholder on the routing is the same as
					// the replica we just tried), in which case we should backoff. With
					// this scheme we'd no longer have to track "updatedLeaseholder" state
					// when syncing the NLHE with the range cache.
					shouldBackoff := !updatedLeaseholder && !intentionallySentToFollower && !leaseholderUnavailable
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
					return nil, kvpb.NewAmbiguousResultErrorf("error=%v [propagate] (last error: %v)",
						ambiguousError, br.Error.GoError())
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
				err = kvpb.NewAmbiguousResultError(errors.Wrapf(ambiguousError, "context done during DistSender.Send"))
			} else {
				err = errors.Wrap(ctx.Err(), "aborted during DistSender.Send")
			}
			log.Eventf(ctx, "%v", err)
			return nil, err
		}
	}
}

// getLocalityComparison takes two nodeIDs as input and returns the locality
// comparison result between their corresponding nodes. This result indicates
// whether the two nodes are located in different regions or zones.
func (ds *DistSender) getLocalityComparison(
	ctx context.Context, fromNodeID roachpb.NodeID, toNodeID roachpb.NodeID,
) roachpb.LocalityComparisonType {
	gatewayNodeDesc, err := ds.nodeDescs.GetNodeDescriptor(fromNodeID)
	if err != nil {
		log.VEventf(ctx, 5, "failed to perform look up for node descriptor %v", err)
		return roachpb.LocalityComparisonType_UNDEFINED
	}
	destinationNodeDesc, err := ds.nodeDescs.GetNodeDescriptor(toNodeID)
	if err != nil {
		log.VEventf(ctx, 5, "failed to perform look up for node descriptor %v", err)
		return roachpb.LocalityComparisonType_UNDEFINED
	}

	comparisonResult, regionValid, zoneValid := gatewayNodeDesc.Locality.CompareWithLocality(destinationNodeDesc.Locality)
	if !regionValid {
		log.VInfof(ctx, 5, "unable to determine if the given nodes are cross region")
	}
	if !zoneValid {
		log.VInfof(ctx, 5, "unable to determine if the given nodes are cross zone")
	}
	return comparisonResult
}

// getCostControllerConfig returns the config for the tenant cost model. This
// returns nil if no KV interceptors are associated with the DistSender, or the
// KV interceptor is not a multitenant.TenantSideCostController.
func (ds *DistSender) getCostControllerConfig(ctx context.Context) *tenantcostmodel.Config {
	if ds.kvInterceptor == nil {
		return nil
	}
	costController, ok := ds.kvInterceptor.(multitenant.TenantSideCostController)
	if !ok {
		log.VErrEvent(ctx, 2, "kvInterceptor is not a TenantSideCostController")
		return nil
	}
	cfg := costController.GetCostConfig()
	if cfg == nil {
		log.VErrEvent(ctx, 2, "cost controller does not have a cost config")
	}
	return cfg
}

// computeNetworkCost calculates the network cost multiplier for a read or
// write operation. The network cost accounts for the logical byte traffic
// between the client region and the replica regions.
func (ds *DistSender) computeNetworkCost(
	ctx context.Context,
	desc *roachpb.RangeDescriptor,
	targetReplica *roachpb.ReplicaDescriptor,
	isWrite bool,
) tenantcostmodel.NetworkCost {
	// It is unfortunate that we hardcode a particular locality tier name here.
	// Ideally, we would have a cluster setting that specifies the name or some
	// other way to configure it.
	clientRegion, _ := ds.locality.Find("region")
	if clientRegion == "" {
		// If we do not have the source, there is no way to find the multiplier.
		log.VErrEventf(ctx, 2, "missing region tier in current node: locality=%s",
			ds.locality.String())
		return tenantcostmodel.NetworkCost(0)
	}

	costCfg := ds.getCostControllerConfig(ctx)
	if costCfg == nil {
		// This case is unlikely to happen since this method will only be
		// called through tenant processes, which has a KV interceptor.
		return tenantcostmodel.NetworkCost(0)
	}

	cost := tenantcostmodel.NetworkCost(0)
	if isWrite {
		for i := range desc.Replicas().Descriptors() {
			if replicaRegion, ok := ds.getReplicaRegion(ctx, &desc.Replicas().Descriptors()[i]); ok {
				cost += costCfg.NetworkCost(tenantcostmodel.NetworkPath{
					ToRegion:   replicaRegion,
					FromRegion: clientRegion,
				})
			}
		}
	} else {
		if replicaRegion, ok := ds.getReplicaRegion(ctx, targetReplica); ok {
			cost = costCfg.NetworkCost(tenantcostmodel.NetworkPath{
				ToRegion:   clientRegion,
				FromRegion: replicaRegion,
			})
		}
	}

	return cost
}

func (ds *DistSender) getReplicaRegion(
	ctx context.Context, replica *roachpb.ReplicaDescriptor,
) (region string, ok bool) {
	nodeDesc, err := ds.nodeDescs.GetNodeDescriptor(replica.NodeID)
	if err != nil {
		log.VErrEventf(ctx, 2, "node %d is not gossiped: %v", replica.NodeID, err)
		// If we don't know where a node is, we can't determine the network cost
		// for the operation.
		return "", false
	}

	region, ok = nodeDesc.Locality.Find("region")
	if !ok {
		log.VErrEventf(ctx, 2, "missing region locality for n %d", nodeDesc.NodeID)
		return "", false
	}

	return region, true
}

func (ds *DistSender) maybeIncrementErrCounters(br *kvpb.BatchResponse, err error) {
	if err == nil && br.Error == nil {
		return
	}
	if err != nil {
		ds.metrics.ErrCounts[kvpb.CommunicationErrType].Inc(1)
	} else {
		typ := kvpb.InternalErrType
		if detail := br.Error.GetDetail(); detail != nil {
			typ = detail.Type()
		}
		ds.metrics.ErrCounts[typ].Inc(1)
	}
}

// AllRangeSpans returns a list of spans equivalent to the input list of spans
// where each returned span represents all the keys in a range.
//
// For example, if the input span is {1-8} and the ranges are
// {0-2}{2-4}{4-6}{6-8}{8-10}, then the output is {1-2}{2-4}{4-6}{6-8}{8-9}.
func (ds *DistSender) AllRangeSpans(
	ctx context.Context, spans []roachpb.Span,
) (_ []roachpb.Span, nodeCountHint int, _ error) {
	ranges := make([]roachpb.Span, 0, len(spans))

	it := MakeRangeIterator(ds)
	var replicas util.FastIntMap

	for i := range spans {
		rSpan, err := keys.SpanAddr(spans[i])
		if err != nil {
			return nil, 0, err
		}
		for it.Seek(ctx, rSpan.Key, Ascending); ; it.Next(ctx) {
			if !it.Valid() {
				return nil, 0, it.Error()
			}

			// If the range boundaries are outside the original span, trim
			// the range.
			startKey := it.Desc().StartKey
			if startKey.Compare(rSpan.Key) == -1 {
				startKey = rSpan.Key
			}
			endKey := it.Desc().EndKey
			if endKey.Compare(rSpan.EndKey) == 1 {
				endKey = rSpan.EndKey
			}
			ranges = append(ranges, roachpb.Span{
				Key: startKey.AsRawKey(), EndKey: endKey.AsRawKey(),
			})
			for _, r := range it.Desc().InternalReplicas {
				replicas.Set(int(r.NodeID), 0)
			}
			if !it.NeedAnother(rSpan) {
				break
			}
		}
	}

	return ranges, replicas.Len(), nil
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
	transport Transport,
	routing rangecache.EvictionToken,
	ambiguousError, replicaUnavailableError, lastErr error,
) error {
	// Check whether the range cache told us that the routing info we had is
	// very out-of-date. If so, there's not much point in trying the other
	// replicas in the transport; they'll likely all return
	// RangeKeyMismatchError if there's even a replica. We'll bubble up an
	// error and try with a new descriptor.
	if !routing.Valid() {
		return selectBestError(
			ambiguousError,
			nil, // ignore the replicaUnavailableError, retry with new routing info
			newSendError(errors.Wrap(lastErr, "routing information detected to be stale")))
	}

	for {
		if transport.IsExhausted() {
			// Authentication and authorization errors should be propagated up rather than
			// wrapped in a sendError and retried as they are likely to be fatal if they
			// are returned from multiple servers.
			if !grpcutil.IsAuthError(lastErr) {
				lastErr = newSendError(errors.Wrap(lastErr, "sending to all replicas failed; last error"))
			}
			return selectBestError(ambiguousError, replicaUnavailableError, lastErr)
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
	cause error
}

// newSendError creates a sendError that wraps the given error.
func newSendError(err error) error {
	return &sendError{cause: err}
}

// TestNewSendError creates a new sendError for the purpose of unit tests
func TestNewSendError(msg string) error {
	return newSendError(errors.NewWithDepthf(1, "%s", msg))
}

// Error implements error.
func (s *sendError) Error() string {
	return fmt.Sprintf("failed to send RPC: %s", s.cause)
}

// Cause implements errors.Causer.
// NB: this is an obsolete method, use Unwrap() instead.
func (s *sendError) Cause() error { return s.cause }

// Unwrap implements errors.Wrapper.
func (s *sendError) Unwrap() error { return s.cause }

// IsSendError returns true if err is a sendError.
func IsSendError(err error) bool {
	return errors.HasType(err, &sendError{})
}
