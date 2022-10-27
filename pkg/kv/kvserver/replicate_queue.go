// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/allocatorimpl"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/replicastats"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
	"go.etcd.io/etcd/raft/v3"
)

const (
	// replicateQueuePurgatoryCheckInterval is the interval at which replicas in
	// the replicate queue purgatory are re-attempted. Note that these replicas
	// may be re-attempted more frequently by the replicateQueue in case there are
	// gossip updates that might affect allocation decisions.
	replicateQueuePurgatoryCheckInterval = 1 * time.Minute

	// replicateQueueTimerDuration is the duration between replication of queued
	// replicas.
	replicateQueueTimerDuration = 0 // zero duration to process replication greedily

	// newReplicaGracePeriod is the amount of time that we allow for a new
	// replica's raft state to catch up to the leader's before we start
	// considering it to be behind for the sake of rebalancing. We choose a
	// large value here because snapshots of large replicas can take a while
	// in high latency clusters, and not allowing enough of a cushion can
	// make rebalance thrashing more likely (#17879).
	newReplicaGracePeriod = 5 * time.Minute
)

// MinLeaseTransferInterval controls how frequently leases can be transferred
// for rebalancing. It does not prevent transferring leases in order to allow
// a replica to be removed from a range.
var MinLeaseTransferInterval = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"kv.allocator.min_lease_transfer_interval",
	"controls how frequently leases can be transferred for rebalancing. "+
		"It does not prevent transferring leases in order to allow a "+
		"replica to be removed from a range.",
	1*time.Second,
	settings.NonNegativeDuration,
)

var (
	metaReplicateQueueAddReplicaCount = metric.Metadata{
		Name:        "queue.replicate.addreplica",
		Help:        "Number of replica additions attempted by the replicate queue",
		Measurement: "Replica Additions",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicateQueueAddVoterReplicaCount = metric.Metadata{
		Name:        "queue.replicate.addvoterreplica",
		Help:        "Number of voter replica additions attempted by the replicate queue",
		Measurement: "Replica Additions",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicateQueueAddNonVoterReplicaCount = metric.Metadata{
		Name:        "queue.replicate.addnonvoterreplica",
		Help:        "Number of non-voter replica additions attempted by the replicate queue",
		Measurement: "Replica Additions",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicateQueueRemoveReplicaCount = metric.Metadata{
		Name:        "queue.replicate.removereplica",
		Help:        "Number of replica removals attempted by the replicate queue (typically in response to a rebalancer-initiated addition)",
		Measurement: "Replica Removals",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicateQueueRemoveVoterReplicaCount = metric.Metadata{
		Name:        "queue.replicate.removevoterreplica",
		Help:        "Number of voter replica removals attempted by the replicate queue (typically in response to a rebalancer-initiated addition)",
		Measurement: "Replica Removals",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicateQueueRemoveNonVoterReplicaCount = metric.Metadata{
		Name:        "queue.replicate.removenonvoterreplica",
		Help:        "Number of non-voter replica removals attempted by the replicate queue (typically in response to a rebalancer-initiated addition)",
		Measurement: "Replica Removals",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicateQueueRemoveDeadReplicaCount = metric.Metadata{
		Name:        "queue.replicate.removedeadreplica",
		Help:        "Number of dead replica removals attempted by the replicate queue (typically in response to a node outage)",
		Measurement: "Replica Removals",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicateQueueRemoveDeadVoterReplicaCount = metric.Metadata{
		Name:        "queue.replicate.removedeadvoterreplica",
		Help:        "Number of dead voter replica removals attempted by the replicate queue (typically in response to a node outage)",
		Measurement: "Replica Removals",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicateQueueRemoveDeadNonVoterReplicaCount = metric.Metadata{
		Name:        "queue.replicate.removedeadnonvoterreplica",
		Help:        "Number of dead non-voter replica removals attempted by the replicate queue (typically in response to a node outage)",
		Measurement: "Replica Removals",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicateQueueRemoveDecommissioningReplicaCount = metric.Metadata{
		Name:        "queue.replicate.removedecommissioningreplica",
		Help:        "Number of decommissioning replica removals attempted by the replicate queue (typically in response to a node outage)",
		Measurement: "Replica Removals",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicateQueueRemoveDecommissioningVoterReplicaCount = metric.Metadata{
		Name:        "queue.replicate.removedecommissioningvoterreplica",
		Help:        "Number of decommissioning voter replica removals attempted by the replicate queue (typically in response to a node outage)",
		Measurement: "Replica Removals",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicateQueueRemoveDecommissioningNonVoterReplicaCount = metric.Metadata{
		Name:        "queue.replicate.removedecommissioningnonvoterreplica",
		Help:        "Number of decommissioning non-voter replica removals attempted by the replicate queue (typically in response to a node outage)",
		Measurement: "Replica Removals",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicateQueueRemoveLearnerReplicaCount = metric.Metadata{
		Name:        "queue.replicate.removelearnerreplica",
		Help:        "Number of learner replica removals attempted by the replicate queue (typically due to internal race conditions)",
		Measurement: "Replica Removals",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicateQueueRebalanceReplicaCount = metric.Metadata{
		Name:        "queue.replicate.rebalancereplica",
		Help:        "Number of replica rebalancer-initiated additions attempted by the replicate queue",
		Measurement: "Replica Additions",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicateQueueRebalanceVoterReplicaCount = metric.Metadata{
		Name:        "queue.replicate.rebalancevoterreplica",
		Help:        "Number of voter replica rebalancer-initiated additions attempted by the replicate queue",
		Measurement: "Replica Additions",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicateQueueRebalanceNonVoterReplicaCount = metric.Metadata{
		Name:        "queue.replicate.rebalancenonvoterreplica",
		Help:        "Number of non-voter replica rebalancer-initiated additions attempted by the replicate queue",
		Measurement: "Replica Additions",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicateQueueTransferLeaseCount = metric.Metadata{
		Name:        "queue.replicate.transferlease",
		Help:        "Number of range lease transfers attempted by the replicate queue",
		Measurement: "Lease Transfers",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicateQueueNonVoterPromotionsCount = metric.Metadata{
		Name:        "queue.replicate.nonvoterpromotions",
		Help:        "Number of non-voters promoted to voters by the replicate queue",
		Measurement: "Promotions of Non Voters to Voters",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicateQueueVoterDemotionsCount = metric.Metadata{
		Name:        "queue.replicate.voterdemotions",
		Help:        "Number of voters demoted to non-voters by the replicate queue",
		Measurement: "Demotions of Voters to Non Voters",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicateQueueAddReplicaSuccessCount = metric.Metadata{
		Name:        "queue.replicate.addreplica.success",
		Help:        "Number of successful replica additions processed by the replicate queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicateQueueAddReplicaErrorCount = metric.Metadata{
		Name:        "queue.replicate.addreplica.error",
		Help:        "Number of failed replica additions processed by the replicate queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicateQueueRemoveReplicaSuccessCount = metric.Metadata{
		Name:        "queue.replicate.removereplica.success",
		Help:        "Number of successful replica removals processed by the replicate queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicateQueueRemoveReplicaErrorCount = metric.Metadata{
		Name:        "queue.replicate.removereplica.error",
		Help:        "Number of failed replica removals processed by the replicate queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicateQueueReplaceDeadReplicaSuccessCount = metric.Metadata{
		Name:        "queue.replicate.replacedeadreplica.success",
		Help:        "Number of successful dead replica replica replacements processed by the replicate queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicateQueueReplaceDeadReplicaErrorCount = metric.Metadata{
		Name:        "queue.replicate.replacedeadreplica.error",
		Help:        "Number of failed dead replica replica replacements processed by the replicate queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicateQueueReplaceDecommissioningReplicaSuccessCount = metric.Metadata{
		Name:        "queue.replicate.replacedecommissioningreplica.success",
		Help:        "Number of successful decommissioning replica replica replacements processed by the replicate queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicateQueueReplaceDecommissioningReplicaErrorCount = metric.Metadata{
		Name:        "queue.replicate.replacedecommissioningreplica.error",
		Help:        "Number of failed decommissioning replica replica replacements processed by the replicate queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicateQueueRemoveDecommissioningReplicaSuccessCount = metric.Metadata{
		Name:        "queue.replicate.removedecommissioningreplica.success",
		Help:        "Number of successful decommissioning replica replica removals processed by the replicate queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicateQueueRemoveDecommissioningReplicaErrorCount = metric.Metadata{
		Name:        "queue.replicate.removedecommissioningreplica.error",
		Help:        "Number of failed decommissioning replica replica removals processed by the replicate queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicateQueueRemoveDeadReplicaSuccessCount = metric.Metadata{
		Name:        "queue.replicate.removedeadreplica.success",
		Help:        "Number of successful dead replica replica removals processed by the replicate queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicateQueueRemoveDeadReplicaErrorCount = metric.Metadata{
		Name:        "queue.replicate.removedeadreplica.error",
		Help:        "Number of failed dead replica replica removals processed by the replicate queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
)

// quorumError indicates a retryable error condition which sends replicas being
// processed through the replicate queue into purgatory so that they can be
// retried quickly as soon as nodes come online.
type quorumError struct {
	msg string
}

func newQuorumError(f string, args ...interface{}) *quorumError {
	return &quorumError{
		msg: fmt.Sprintf(f, args...),
	}
}

func (e *quorumError) Error() string {
	return e.msg
}

func (*quorumError) PurgatoryErrorMarker() {}

// ReplicateQueueMetrics is the set of metrics for the replicate queue.
type ReplicateQueueMetrics struct {
	AddReplicaCount                           *metric.Counter
	AddVoterReplicaCount                      *metric.Counter
	AddNonVoterReplicaCount                   *metric.Counter
	RemoveReplicaCount                        *metric.Counter
	RemoveVoterReplicaCount                   *metric.Counter
	RemoveNonVoterReplicaCount                *metric.Counter
	RemoveDeadReplicaCount                    *metric.Counter
	RemoveDeadVoterReplicaCount               *metric.Counter
	RemoveDeadNonVoterReplicaCount            *metric.Counter
	RemoveDecommissioningReplicaCount         *metric.Counter
	RemoveDecommissioningVoterReplicaCount    *metric.Counter
	RemoveDecommissioningNonVoterReplicaCount *metric.Counter
	RemoveLearnerReplicaCount                 *metric.Counter
	RebalanceReplicaCount                     *metric.Counter
	RebalanceVoterReplicaCount                *metric.Counter
	RebalanceNonVoterReplicaCount             *metric.Counter
	TransferLeaseCount                        *metric.Counter
	NonVoterPromotionsCount                   *metric.Counter
	VoterDemotionsCount                       *metric.Counter

	// Success/error counts by allocator action.
	RemoveReplicaSuccessCount                 *metric.Counter
	RemoveReplicaErrorCount                   *metric.Counter
	AddReplicaSuccessCount                    *metric.Counter
	AddReplicaErrorCount                      *metric.Counter
	ReplaceDeadReplicaSuccessCount            *metric.Counter
	ReplaceDeadReplicaErrorCount              *metric.Counter
	RemoveDeadReplicaSuccessCount             *metric.Counter
	RemoveDeadReplicaErrorCount               *metric.Counter
	ReplaceDecommissioningReplicaSuccessCount *metric.Counter
	ReplaceDecommissioningReplicaErrorCount   *metric.Counter
	RemoveDecommissioningReplicaSuccessCount  *metric.Counter
	RemoveDecommissioningReplicaErrorCount    *metric.Counter
	// TODO(sarkesian): Consider adding metrics for AllocatorRemoveLearner,
	// AllocatorConsiderRebalance, and AllocatorFinalizeAtomicReplicationChange
	// allocator actions.
}

func makeReplicateQueueMetrics() ReplicateQueueMetrics {
	return ReplicateQueueMetrics{
		AddReplicaCount:                           metric.NewCounter(metaReplicateQueueAddReplicaCount),
		AddVoterReplicaCount:                      metric.NewCounter(metaReplicateQueueAddVoterReplicaCount),
		AddNonVoterReplicaCount:                   metric.NewCounter(metaReplicateQueueAddNonVoterReplicaCount),
		RemoveReplicaCount:                        metric.NewCounter(metaReplicateQueueRemoveReplicaCount),
		RemoveVoterReplicaCount:                   metric.NewCounter(metaReplicateQueueRemoveVoterReplicaCount),
		RemoveNonVoterReplicaCount:                metric.NewCounter(metaReplicateQueueRemoveNonVoterReplicaCount),
		RemoveDeadReplicaCount:                    metric.NewCounter(metaReplicateQueueRemoveDeadReplicaCount),
		RemoveDeadVoterReplicaCount:               metric.NewCounter(metaReplicateQueueRemoveDeadVoterReplicaCount),
		RemoveDeadNonVoterReplicaCount:            metric.NewCounter(metaReplicateQueueRemoveDeadNonVoterReplicaCount),
		RemoveLearnerReplicaCount:                 metric.NewCounter(metaReplicateQueueRemoveLearnerReplicaCount),
		RemoveDecommissioningReplicaCount:         metric.NewCounter(metaReplicateQueueRemoveDecommissioningReplicaCount),
		RemoveDecommissioningVoterReplicaCount:    metric.NewCounter(metaReplicateQueueRemoveDecommissioningVoterReplicaCount),
		RemoveDecommissioningNonVoterReplicaCount: metric.NewCounter(metaReplicateQueueRemoveDecommissioningNonVoterReplicaCount),
		RebalanceReplicaCount:                     metric.NewCounter(metaReplicateQueueRebalanceReplicaCount),
		RebalanceVoterReplicaCount:                metric.NewCounter(metaReplicateQueueRebalanceVoterReplicaCount),
		RebalanceNonVoterReplicaCount:             metric.NewCounter(metaReplicateQueueRebalanceNonVoterReplicaCount),
		TransferLeaseCount:                        metric.NewCounter(metaReplicateQueueTransferLeaseCount),
		NonVoterPromotionsCount:                   metric.NewCounter(metaReplicateQueueNonVoterPromotionsCount),
		VoterDemotionsCount:                       metric.NewCounter(metaReplicateQueueVoterDemotionsCount),

		RemoveReplicaSuccessCount:                 metric.NewCounter(metaReplicateQueueRemoveReplicaSuccessCount),
		RemoveReplicaErrorCount:                   metric.NewCounter(metaReplicateQueueRemoveReplicaErrorCount),
		AddReplicaSuccessCount:                    metric.NewCounter(metaReplicateQueueAddReplicaSuccessCount),
		AddReplicaErrorCount:                      metric.NewCounter(metaReplicateQueueAddReplicaErrorCount),
		ReplaceDeadReplicaSuccessCount:            metric.NewCounter(metaReplicateQueueReplaceDeadReplicaSuccessCount),
		ReplaceDeadReplicaErrorCount:              metric.NewCounter(metaReplicateQueueReplaceDeadReplicaErrorCount),
		RemoveDeadReplicaSuccessCount:             metric.NewCounter(metaReplicateQueueRemoveDeadReplicaSuccessCount),
		RemoveDeadReplicaErrorCount:               metric.NewCounter(metaReplicateQueueRemoveDeadReplicaErrorCount),
		ReplaceDecommissioningReplicaSuccessCount: metric.NewCounter(metaReplicateQueueReplaceDecommissioningReplicaSuccessCount),
		ReplaceDecommissioningReplicaErrorCount:   metric.NewCounter(metaReplicateQueueReplaceDecommissioningReplicaErrorCount),
		RemoveDecommissioningReplicaSuccessCount:  metric.NewCounter(metaReplicateQueueRemoveDecommissioningReplicaSuccessCount),
		RemoveDecommissioningReplicaErrorCount:    metric.NewCounter(metaReplicateQueueRemoveDecommissioningReplicaErrorCount),
	}
}

// trackAddReplicaCount increases the AddReplicaCount metric and separately
// tracks voter/non-voter metrics given a replica targetType.
func (metrics *ReplicateQueueMetrics) trackAddReplicaCount(
	targetType allocatorimpl.TargetReplicaType,
) {
	metrics.AddReplicaCount.Inc(1)
	switch targetType {
	case allocatorimpl.VoterTarget:
		metrics.AddVoterReplicaCount.Inc(1)
	case allocatorimpl.NonVoterTarget:
		metrics.AddNonVoterReplicaCount.Inc(1)
	default:
		panic(fmt.Sprintf("unsupported targetReplicaType: %v", targetType))
	}
}

// trackRemoveMetric increases total RemoveReplicaCount metrics and
// increments dead/decommissioning metrics depending on replicaStatus.
func (metrics *ReplicateQueueMetrics) trackRemoveMetric(
	targetType allocatorimpl.TargetReplicaType, replicaStatus allocatorimpl.ReplicaStatus,
) {
	metrics.trackRemoveReplicaCount(targetType)
	switch replicaStatus {
	case allocatorimpl.Dead:
		metrics.trackRemoveDeadReplicaCount(targetType)
	case allocatorimpl.Decommissioning:
		metrics.trackRemoveDecommissioningReplicaCount(targetType)
	case allocatorimpl.Alive:
		return
	default:
		panic(fmt.Sprintf("unknown replicaStatus %v", replicaStatus))
	}
}

// trackRemoveReplicaCount increases the RemoveReplicaCount metric and
// separately tracks voter/non-voter metrics given a replica targetType.
func (metrics *ReplicateQueueMetrics) trackRemoveReplicaCount(
	targetType allocatorimpl.TargetReplicaType,
) {
	metrics.RemoveReplicaCount.Inc(1)
	switch targetType {
	case allocatorimpl.VoterTarget:
		metrics.RemoveVoterReplicaCount.Inc(1)
	case allocatorimpl.NonVoterTarget:
		metrics.RemoveNonVoterReplicaCount.Inc(1)
	default:
		panic(fmt.Sprintf("unsupported targetReplicaType: %v", targetType))
	}
}

// trackRemoveDeadReplicaCount increases the RemoveDeadReplicaCount metric and
// separately tracks voter/non-voter metrics given a replica targetType.
func (metrics *ReplicateQueueMetrics) trackRemoveDeadReplicaCount(
	targetType allocatorimpl.TargetReplicaType,
) {
	metrics.RemoveDeadReplicaCount.Inc(1)
	switch targetType {
	case allocatorimpl.VoterTarget:
		metrics.RemoveDeadVoterReplicaCount.Inc(1)
	case allocatorimpl.NonVoterTarget:
		metrics.RemoveDeadNonVoterReplicaCount.Inc(1)
	default:
		panic(fmt.Sprintf("unsupported targetReplicaType: %v", targetType))
	}
}

// trackRemoveDecommissioningReplicaCount increases the
// RemoveDecommissioningReplicaCount metric and separately tracks
// voter/non-voter metrics given a replica targetType.
func (metrics *ReplicateQueueMetrics) trackRemoveDecommissioningReplicaCount(
	targetType allocatorimpl.TargetReplicaType,
) {
	metrics.RemoveDecommissioningReplicaCount.Inc(1)
	switch targetType {
	case allocatorimpl.VoterTarget:
		metrics.RemoveDecommissioningVoterReplicaCount.Inc(1)
	case allocatorimpl.NonVoterTarget:
		metrics.RemoveDecommissioningNonVoterReplicaCount.Inc(1)
	default:
		panic(fmt.Sprintf("unsupported targetReplicaType: %v", targetType))
	}
}

// trackRebalanceReplicaCount increases the RebalanceReplicaCount metric and
// separately tracks voter/non-voter metrics given a replica targetType.
func (metrics *ReplicateQueueMetrics) trackRebalanceReplicaCount(
	targetType allocatorimpl.TargetReplicaType,
) {
	metrics.RebalanceReplicaCount.Inc(1)
	switch targetType {
	case allocatorimpl.VoterTarget:
		metrics.RebalanceVoterReplicaCount.Inc(1)
	case allocatorimpl.NonVoterTarget:
		metrics.RebalanceNonVoterReplicaCount.Inc(1)
	default:
		panic(fmt.Sprintf("unsupported targetReplicaType: %v", targetType))
	}
}

// trackProcessResult increases the corresponding success/error count metric for
// processing a particular allocator action through the replicate queue.
func (metrics *ReplicateQueueMetrics) trackResultByAllocatorAction(
	ctx context.Context, action allocatorimpl.AllocatorAction, err error, dryRun bool,
) {
	if dryRun {
		return
	}
	switch action {
	case allocatorimpl.AllocatorRemoveVoter, allocatorimpl.AllocatorRemoveNonVoter:
		if err == nil {
			metrics.RemoveReplicaSuccessCount.Inc(1)
		} else {
			metrics.RemoveReplicaErrorCount.Inc(1)
		}
	case allocatorimpl.AllocatorAddVoter, allocatorimpl.AllocatorAddNonVoter:
		if err == nil {
			metrics.AddReplicaSuccessCount.Inc(1)
		} else {
			metrics.AddReplicaErrorCount.Inc(1)
		}
	case allocatorimpl.AllocatorReplaceDeadVoter, allocatorimpl.AllocatorReplaceDeadNonVoter:
		if err == nil {
			metrics.ReplaceDeadReplicaSuccessCount.Inc(1)
		} else {
			metrics.ReplaceDeadReplicaErrorCount.Inc(1)
		}
	case allocatorimpl.AllocatorRemoveDeadVoter, allocatorimpl.AllocatorRemoveDeadNonVoter:
		if err == nil {
			metrics.RemoveDeadReplicaSuccessCount.Inc(1)
		} else {
			metrics.RemoveDeadReplicaErrorCount.Inc(1)
		}
	case allocatorimpl.AllocatorReplaceDecommissioningVoter, allocatorimpl.AllocatorReplaceDecommissioningNonVoter:
		if err == nil {
			metrics.ReplaceDecommissioningReplicaSuccessCount.Inc(1)
		} else {
			metrics.ReplaceDecommissioningReplicaErrorCount.Inc(1)
		}
	case allocatorimpl.AllocatorRemoveDecommissioningVoter, allocatorimpl.AllocatorRemoveDecommissioningNonVoter:
		if err == nil {
			metrics.RemoveDecommissioningReplicaSuccessCount.Inc(1)
		} else {
			metrics.RemoveDecommissioningReplicaErrorCount.Inc(1)
		}
	default:
		log.Errorf(ctx, "AllocatorAction %v unsupported in metrics tracking", action)
	}
}

// replicateQueue manages a queue of replicas which may need to add an
// additional replica to their range.
type replicateQueue struct {
	*baseQueue
	metrics   ReplicateQueueMetrics
	allocator allocatorimpl.Allocator

	// purgCh is signalled every replicateQueuePurgatoryCheckInterval.
	purgCh <-chan time.Time
	// updateCh is signalled every time there is an update to the cluster's store
	// descriptors.
	updateCh          chan time.Time
	lastLeaseTransfer atomic.Value // read and written by scanner & queue goroutines
	// logTracesThresholdFunc returns the threshold for logging traces from
	// processing a replica.
	logTracesThresholdFunc queueProcessTimeoutFunc
}

var _ queueImpl = &replicateQueue{}

// newReplicateQueue returns a new instance of replicateQueue.
func newReplicateQueue(store *Store, allocator allocatorimpl.Allocator) *replicateQueue {
	rq := &replicateQueue{
		metrics:   makeReplicateQueueMetrics(),
		allocator: allocator,
		purgCh:    time.NewTicker(replicateQueuePurgatoryCheckInterval).C,
		updateCh:  make(chan time.Time, 1),
		logTracesThresholdFunc: makeRateLimitedTimeoutFuncByPermittedSlowdown(
			permittedRangeScanSlowdown/2, rebalanceSnapshotRate, recoverySnapshotRate,
		),
	}
	store.metrics.registry.AddMetricStruct(&rq.metrics)
	rq.baseQueue = newBaseQueue(
		"replicate", rq, store,
		queueConfig{
			maxSize:              defaultQueueMaxSize,
			needsLease:           true,
			needsSystemConfig:    true,
			acceptsUnsplitRanges: store.TestingKnobs().ReplicateQueueAcceptsUnsplit,
			// The processing of the replicate queue often needs to send snapshots
			// so we use the raftSnapshotQueueTimeoutFunc. This function sets a
			// timeout based on the range size and the sending rate in addition
			// to consulting the setting which controls the minimum timeout.
			processTimeoutFunc: makeRateLimitedTimeoutFunc(rebalanceSnapshotRate, recoverySnapshotRate),
			successes:          store.metrics.ReplicateQueueSuccesses,
			failures:           store.metrics.ReplicateQueueFailures,
			pending:            store.metrics.ReplicateQueuePending,
			processingNanos:    store.metrics.ReplicateQueueProcessingNanos,
			purgatory:          store.metrics.ReplicateQueuePurgatory,
		},
	)
	updateFn := func() {
		select {
		case rq.updateCh <- timeutil.Now():
		default:
		}
	}

	// Register gossip and node liveness callbacks to signal that
	// replicas in purgatory might be retried.
	if g := store.cfg.Gossip; g != nil { // gossip is nil for some unittests
		g.RegisterCallback(gossip.MakePrefixPattern(gossip.KeyStoreDescPrefix), func(key string, _ roachpb.Value) {
			if !rq.store.IsStarted() {
				return
			}
			// Because updates to our store's own descriptor won't affect
			// replicas in purgatory, skip updating the purgatory channel
			// in this case.
			if storeID, err := gossip.DecodeStoreDescKey(key); err == nil && storeID == rq.store.StoreID() {
				return
			}
			updateFn()
		})
	}
	if nl := store.cfg.NodeLiveness; nl != nil { // node liveness is nil for some unittests
		nl.RegisterCallback(func(_ livenesspb.Liveness) {
			updateFn()
		})
	}

	return rq
}

func (rq *replicateQueue) shouldQueue(
	ctx context.Context, now hlc.ClockTimestamp, repl *Replica, _ spanconfig.StoreReader,
) (shouldQueue bool, priority float64) {
	desc, conf := repl.DescAndSpanConfig()
	action, priority := rq.allocator.ComputeAction(ctx, conf, desc)

	if action == allocatorimpl.AllocatorNoop {
		log.KvDistribution.VEventf(ctx, 2, "no action to take")
		return false, 0
	} else if action != allocatorimpl.AllocatorConsiderRebalance {
		log.KvDistribution.VEventf(ctx, 2, "repair needed (%s), enqueuing", action)
		return true, priority
	}

	voterReplicas := desc.Replicas().VoterDescriptors()
	nonVoterReplicas := desc.Replicas().NonVoterDescriptors()
	if !rq.store.TestingKnobs().DisableReplicaRebalancing {
		rangeUsageInfo := RangeUsageInfoForRepl(repl)
		_, _, _, ok := rq.allocator.RebalanceVoter(
			ctx,
			conf,
			repl.RaftStatus(),
			voterReplicas,
			nonVoterReplicas,
			rangeUsageInfo,
			storepool.StoreFilterThrottled,
			rq.allocator.ScorerOptions(ctx),
		)
		if ok {
			log.KvDistribution.VEventf(ctx, 2, "rebalance target found for voter, enqueuing")
			return true, 0
		}
		_, _, _, ok = rq.allocator.RebalanceNonVoter(
			ctx,
			conf,
			repl.RaftStatus(),
			voterReplicas,
			nonVoterReplicas,
			rangeUsageInfo,
			storepool.StoreFilterThrottled,
			rq.allocator.ScorerOptions(ctx),
		)
		if ok {
			log.KvDistribution.VEventf(ctx, 2, "rebalance target found for non-voter, enqueuing")
			return true, 0
		}
		log.KvDistribution.VEventf(ctx, 2, "no rebalance target found, not enqueuing")
	}

	// If the lease is valid, check to see if we should transfer it.
	if rq.canTransferLeaseFrom(ctx, repl) &&
		rq.allocator.ShouldTransferLease(
			ctx,
			conf,
			voterReplicas,
			repl,
			RangeUsageInfoForRepl(repl),
		) {
		log.KvDistribution.VEventf(ctx, 2, "lease transfer needed, enqueuing")
		return true, 0
	}
	if !repl.LeaseStatusAt(ctx, now).IsValid() {
		// The lease for this range is currently invalid, if this replica is
		// the raft leader then it is necessary that it acquires the lease. We
		// enqueue it regardless of being a leader or follower, where the
		// leader at the time of processing will succeed. There is no
		// requirement that the expired lease belongs to this replica, as
		// regardless of the lease history, the current leader should hold the
		// lease.
		log.KvDistribution.VEventf(ctx, 2, "invalid lease, enqueuing")
		return true, 0
	}

	return false, 0
}

func (rq *replicateQueue) process(
	ctx context.Context, repl *Replica, confReader spanconfig.StoreReader,
) (processed bool, err error) {
	retryOpts := retry.Options{
		InitialBackoff: 50 * time.Millisecond,
		MaxBackoff:     1 * time.Second,
		Multiplier:     2,
		MaxRetries:     5,
	}

	// Use a retry loop in order to backoff in the case of snapshot errors,
	// usually signaling that a rebalancing reservation could not be made with the
	// selected target.
	for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
		requeue, err := rq.processOneChangeWithTracing(ctx, repl)
		if isSnapshotError(err) {
			// If ChangeReplicas failed because the snapshot failed, we attempt to
			// retry the operation. The most likely causes of the snapshot failing
			// are a declined reservation (i.e. snapshot queue too long, or timeout
			// while waiting in queue) or the remote node being unavailable. In
			// either case we don't want to wait another scanner cycle before
			// reconsidering the range.
			// NB: The reason we are retrying snapshot failures immediately is that
			// the recipient node will be "blocked" by a snapshot send failure for a
			// few seconds. By retrying immediately we will choose another equally
			// "good" target store chosen by the allocator.
			// TODO(baptist): This is probably suboptimal behavior. In the case where
			// there is only one option for a recipient, we will block the entire
			// replicate queue until we are able to send this through. Also even if
			// there are multiple options, we may choose a far inferior recipient.
			log.KvDistribution.Infof(ctx, "%v", err)
			continue
		}

		if err != nil {
			return false, err
		}

		if testingAggressiveConsistencyChecks {
			if _, err := rq.store.consistencyQueue.process(ctx, repl, confReader); err != nil {
				log.KvDistribution.Warningf(ctx, "%v", err)
			}
		}

		// After we made a replica change, make sure the lease is still on the
		// correct store.
		if rq.canTransferLeaseFrom(ctx, repl) {
			transferStatus, err := rq.shedLease(
				ctx,
				repl,
				repl.Desc(),
				repl.SpanConfig(),
				allocator.TransferLeaseOptions{
					Goal:                   allocator.FollowTheWorkload,
					ExcludeLeaseRepl:       false,
					CheckCandidateFullness: true,
					DryRun:                 false,
				},
			)
			if err != nil {
				return false, err
			}
			// If we successfully transferred the lease, we can't requeue, let the new
			// leaseholder do it.
			if transferStatus == allocator.TransferOK {
				requeue = false
			}
		}

		if requeue {
			log.KvDistribution.VEventf(ctx, 1, "re-processing")
			rq.maybeAdd(ctx, repl, rq.store.Clock().NowAsClockTimestamp())
		}
		return true, nil
	}

	return false, errors.Errorf("failed to replicate after %d retries", retryOpts.MaxRetries)
}

// decommissionPurgatoryError wraps an error that occurs when attempting to
// rebalance a range that has a replica on a decommissioning node to indicate
// that the error should send the range to purgatory.
type decommissionPurgatoryError struct{ error }

func (decommissionPurgatoryError) PurgatoryErrorMarker() {}

var _ PurgatoryError = decommissionPurgatoryError{}

// filterTracingSpans is a utility for processOneChangeWithTracing in order to
// remove spans with Operation names in opNamesToFilter, as well as all of
// their child spans, to exclude overly verbose spans prior to logging.
func filterTracingSpans(rec tracingpb.Recording, opNamesToFilter ...string) tracingpb.Recording {
	excludedOpNames := make(map[string]struct{})
	excludedSpanIDs := make(map[tracingpb.SpanID]struct{})
	for _, opName := range opNamesToFilter {
		excludedOpNames[opName] = struct{}{}
	}

	filteredRecording := make(tracingpb.Recording, 0, rec.Len())
	for _, span := range rec {
		_, excludedByOpName := excludedOpNames[span.Operation]
		_, excludedByParentSpanID := excludedSpanIDs[span.ParentSpanID]
		if excludedByOpName || excludedByParentSpanID {
			excludedSpanIDs[span.SpanID] = struct{}{}
		} else {
			filteredRecording = append(filteredRecording, span)
		}
	}

	return filteredRecording
}

// processOneChangeWithTracing executes processOneChange within a tracing span,
// logging the resulting traces to the DEV channel in the case of errors or
// when the configured log traces threshold is exceeded.
func (rq *replicateQueue) processOneChangeWithTracing(
	ctx context.Context, repl *Replica,
) (requeue bool, _ error) {
	processStart := timeutil.Now()
	ctx, sp := tracing.EnsureChildSpan(ctx, rq.Tracer, "process replica",
		tracing.WithRecording(tracingpb.RecordingVerbose))
	defer sp.Finish()

	requeue, err := rq.processOneChange(ctx, repl, rq.canTransferLeaseFrom,
		false /* scatter */, false, /* dryRun */
	)

	// Utilize a new background context (properly annotated) to avoid writing
	// traces from a child context into its parent.
	{
		ctx := repl.AnnotateCtx(rq.AnnotateCtx(context.Background()))
		var rec tracingpb.Recording
		processDuration := timeutil.Since(processStart)
		loggingThreshold := rq.logTracesThresholdFunc(rq.store.cfg.Settings, repl)
		exceededDuration := loggingThreshold > time.Duration(0) && processDuration > loggingThreshold

		var traceOutput string
		traceLoggingNeeded := err != nil || exceededDuration
		if traceLoggingNeeded {
			// If we have tracing spans from execChangeReplicasTxn, filter it from
			// the recording so that we can render the traces to the log without it,
			// as the traces from this span (and its children) are highly verbose.
			rec = filterTracingSpans(sp.GetConfiguredRecording(),
				replicaChangeTxnGetDescOpName, replicaChangeTxnUpdateDescOpName,
			)
			traceOutput = fmt.Sprintf("\ntrace:\n%s", rec)
		}

		if err != nil {
			log.KvDistribution.Infof(ctx, "error processing replica: %v%s", err, traceOutput)
		} else if exceededDuration {
			log.KvDistribution.Infof(ctx, "processing replica took %s, exceeding threshold of %s%s",
				processDuration, loggingThreshold, traceOutput)
		}
	}

	return requeue, err
}

func (rq *replicateQueue) processOneChange(
	ctx context.Context,
	repl *Replica,
	canTransferLeaseFrom func(ctx context.Context, repl *Replica) bool,
	scatter, dryRun bool,
) (requeue bool, _ error) {
	// Check lease and destroy status here. The queue does this higher up already, but
	// adminScatter (and potential other future callers) also call this method and don't
	// perform this check, which could lead to infinite loops.
	if _, err := repl.IsDestroyed(); err != nil {
		return false, err
	}
	if _, pErr := repl.redirectOnOrAcquireLease(ctx); pErr != nil {
		return false, pErr.GoError()
	}

	// TODO(aayush): The fact that we're calling `repl.DescAndZone()` here once to
	// pass to `ComputeAction()` to use for deciding which action to take to
	// repair a range, and then calling it again inside methods like
	// `addOrReplace{Non}Voters()` or `remove{Dead,Decommissioning}` to execute
	// upon that decision is a bit unfortunate. It means that we could
	// successfully execute a decision that was based on the state of a stale
	// range descriptor.
	desc, conf := repl.DescAndSpanConfig()

	voterReplicas := desc.Replicas().VoterDescriptors()
	nonVoterReplicas := desc.Replicas().NonVoterDescriptors()
	liveVoterReplicas, deadVoterReplicas := rq.store.cfg.StorePool.LiveAndDeadReplicas(
		voterReplicas, true, /* includeSuspectAndDrainingStores */
	)
	liveNonVoterReplicas, deadNonVoterReplicas := rq.store.cfg.StorePool.LiveAndDeadReplicas(
		nonVoterReplicas, true, /* includeSuspectAndDrainingStores */
	)

	// NB: the replication layer ensures that the below operations don't cause
	// unavailability; see:
	_ = execChangeReplicasTxn

	action, allocatorPrio := rq.allocator.ComputeAction(ctx, conf, desc)
	log.KvDistribution.VEventf(ctx, 1, "next replica action: %s", action)

	switch action {
	case allocatorimpl.AllocatorNoop, allocatorimpl.AllocatorRangeUnavailable:
		// We're either missing liveness information or the range is known to have
		// lost quorum. Either way, it's not a good idea to make changes right now.
		// Let the scanner requeue it again later.
		return false, nil

	// Add replicas.
	case allocatorimpl.AllocatorAddVoter:
		requeue, err := rq.addOrReplaceVoters(
			ctx, repl, liveVoterReplicas, liveNonVoterReplicas, -1 /* removeIdx */, allocatorimpl.Alive, allocatorPrio, dryRun,
		)
		rq.metrics.trackResultByAllocatorAction(ctx, action, err, dryRun)
		return requeue, err
	case allocatorimpl.AllocatorAddNonVoter:
		requeue, err := rq.addOrReplaceNonVoters(
			ctx, repl, liveVoterReplicas, liveNonVoterReplicas, -1 /* removeIdx */, allocatorimpl.Alive, allocatorPrio, dryRun,
		)
		rq.metrics.trackResultByAllocatorAction(ctx, action, err, dryRun)
		return requeue, err

	// Remove replicas.
	case allocatorimpl.AllocatorRemoveVoter:
		requeue, err := rq.removeVoter(ctx, repl, voterReplicas, nonVoterReplicas, dryRun)
		rq.metrics.trackResultByAllocatorAction(ctx, action, err, dryRun)
		return requeue, err
	case allocatorimpl.AllocatorRemoveNonVoter:
		requeue, err := rq.removeNonVoter(ctx, repl, voterReplicas, nonVoterReplicas, dryRun)
		rq.metrics.trackResultByAllocatorAction(ctx, action, err, dryRun)
		return requeue, err

	// Replace dead replicas.
	case allocatorimpl.AllocatorReplaceDeadVoter:
		if len(deadVoterReplicas) == 0 {
			// Nothing to do.
			return false, nil
		}
		removeIdx := getRemoveIdx(voterReplicas, deadVoterReplicas[0])
		if removeIdx < 0 {
			return false, errors.AssertionFailedf(
				"dead voter %v unexpectedly not found in %v",
				deadVoterReplicas[0], voterReplicas)
		}
		requeue, err := rq.addOrReplaceVoters(
			ctx, repl, liveVoterReplicas, liveNonVoterReplicas, removeIdx, allocatorimpl.Dead, allocatorPrio, dryRun)
		rq.metrics.trackResultByAllocatorAction(ctx, action, err, dryRun)
		return requeue, err
	case allocatorimpl.AllocatorReplaceDeadNonVoter:
		if len(deadNonVoterReplicas) == 0 {
			// Nothing to do.
			return false, nil
		}
		removeIdx := getRemoveIdx(nonVoterReplicas, deadNonVoterReplicas[0])
		if removeIdx < 0 {
			return false, errors.AssertionFailedf(
				"dead non-voter %v unexpectedly not found in %v",
				deadNonVoterReplicas[0], nonVoterReplicas)
		}
		requeue, err := rq.addOrReplaceNonVoters(
			ctx, repl, liveVoterReplicas, liveNonVoterReplicas, removeIdx, allocatorimpl.Dead, allocatorPrio, dryRun)
		rq.metrics.trackResultByAllocatorAction(ctx, action, err, dryRun)
		return requeue, err

	// Replace decommissioning replicas.
	case allocatorimpl.AllocatorReplaceDecommissioningVoter:
		decommissioningVoterReplicas := rq.store.cfg.StorePool.DecommissioningReplicas(voterReplicas)
		if len(decommissioningVoterReplicas) == 0 {
			// Nothing to do.
			return false, nil
		}
		removeIdx := getRemoveIdx(voterReplicas, decommissioningVoterReplicas[0])
		if removeIdx < 0 {
			return false, errors.AssertionFailedf(
				"decommissioning voter %v unexpectedly not found in %v",
				decommissioningVoterReplicas[0], voterReplicas)
		}
		requeue, err := rq.addOrReplaceVoters(
			ctx, repl, liveVoterReplicas, liveNonVoterReplicas, removeIdx, allocatorimpl.Decommissioning, allocatorPrio, dryRun)
		rq.metrics.trackResultByAllocatorAction(ctx, action, err, dryRun)
		if err != nil {
			return requeue, decommissionPurgatoryError{err}
		}
		return requeue, nil
	case allocatorimpl.AllocatorReplaceDecommissioningNonVoter:
		decommissioningNonVoterReplicas := rq.store.cfg.StorePool.DecommissioningReplicas(nonVoterReplicas)
		if len(decommissioningNonVoterReplicas) == 0 {
			return false, nil
		}
		removeIdx := getRemoveIdx(nonVoterReplicas, decommissioningNonVoterReplicas[0])
		if removeIdx < 0 {
			return false, errors.AssertionFailedf(
				"decommissioning non-voter %v unexpectedly not found in %v",
				decommissioningNonVoterReplicas[0], nonVoterReplicas)
		}
		requeue, err := rq.addOrReplaceNonVoters(
			ctx, repl, liveVoterReplicas, liveNonVoterReplicas, removeIdx, allocatorimpl.Decommissioning, allocatorPrio, dryRun)
		rq.metrics.trackResultByAllocatorAction(ctx, action, err, dryRun)
		if err != nil {
			return requeue, decommissionPurgatoryError{err}
		}
		return requeue, nil

	// Remove decommissioning replicas.
	//
	// NB: these two paths will only be hit when the range is over-replicated and
	// has decommissioning replicas; in the common case we'll hit
	// AllocatorReplaceDecommissioning{Non}Voter above.
	case allocatorimpl.AllocatorRemoveDecommissioningVoter:
		requeue, err := rq.removeDecommissioning(ctx, repl, allocatorimpl.VoterTarget, dryRun)
		rq.metrics.trackResultByAllocatorAction(ctx, action, err, dryRun)
		if err != nil {
			return requeue, decommissionPurgatoryError{err}
		}
		return requeue, nil
	case allocatorimpl.AllocatorRemoveDecommissioningNonVoter:
		requeue, err := rq.removeDecommissioning(ctx, repl, allocatorimpl.NonVoterTarget, dryRun)
		rq.metrics.trackResultByAllocatorAction(ctx, action, err, dryRun)
		if err != nil {
			return requeue, decommissionPurgatoryError{err}
		}
		return requeue, nil

	// Remove dead replicas.
	//
	// NB: these two paths below will only be hit when the range is
	// over-replicated and has dead replicas; in the common case we'll hit
	// AllocatorReplaceDead{Non}Voter above.
	case allocatorimpl.AllocatorRemoveDeadVoter:
		requeue, err := rq.removeDead(ctx, repl, deadVoterReplicas, allocatorimpl.VoterTarget, dryRun)
		rq.metrics.trackResultByAllocatorAction(ctx, action, err, dryRun)
		return requeue, err
	case allocatorimpl.AllocatorRemoveDeadNonVoter:
		requeue, err := rq.removeDead(ctx, repl, deadNonVoterReplicas, allocatorimpl.NonVoterTarget, dryRun)
		rq.metrics.trackResultByAllocatorAction(ctx, action, err, dryRun)
		return requeue, err

	case allocatorimpl.AllocatorConsiderRebalance:
		return rq.considerRebalance(
			ctx,
			repl,
			voterReplicas,
			nonVoterReplicas,
			allocatorPrio,
			canTransferLeaseFrom,
			scatter,
			dryRun,
		)
	case allocatorimpl.AllocatorFinalizeAtomicReplicationChange, allocatorimpl.AllocatorRemoveLearner:
		_, learnersRemoved, err := repl.maybeLeaveAtomicChangeReplicasAndRemoveLearners(
			ctx, repl.Desc(),
		)
		if err != nil {
			return false, err
		}
		rq.metrics.RemoveLearnerReplicaCount.Inc(learnersRemoved)
		return true, nil
	default:
		return false, errors.Errorf("unknown allocator action %v", action)
	}
}

func getRemoveIdx(
	repls []roachpb.ReplicaDescriptor, deadOrDecommissioningRepl roachpb.ReplicaDescriptor,
) (removeIdx int) {
	for i, rDesc := range repls {
		if rDesc.StoreID == deadOrDecommissioningRepl.StoreID {
			removeIdx = i
			break
		}
	}
	return removeIdx
}

// addOrReplaceVoters adds or replaces a voting replica. If removeIdx is -1, an
// addition is carried out. Otherwise, removeIdx must be a valid index into
// existingVoters and specifies which voter to replace with a new one.
//
// The method preferably issues an atomic replica swap, but may not be able to
// do this in all cases, such as when the range consists of a single replica. As
// a fall back, only the addition is carried out; the removal is then a
// follow-up step for the next scanner cycle.
func (rq *replicateQueue) addOrReplaceVoters(
	ctx context.Context,
	repl *Replica,
	liveVoterReplicas, liveNonVoterReplicas []roachpb.ReplicaDescriptor,
	removeIdx int,
	replicaStatus allocatorimpl.ReplicaStatus,
	allocatorPriority float64,
	dryRun bool,
) (requeue bool, _ error) {
	desc, conf := repl.DescAndSpanConfig()
	existingVoters := desc.Replicas().VoterDescriptors()
	if len(existingVoters) == 1 {
		// If only one replica remains, that replica is the leaseholder and
		// we won't be able to swap it out. Ignore the removal and simply add
		// a replica.
		removeIdx = -1
	}

	remainingLiveVoters := liveVoterReplicas
	remainingLiveNonVoters := liveNonVoterReplicas
	if removeIdx >= 0 {
		replToRemove := existingVoters[removeIdx]
		for i, r := range liveVoterReplicas {
			if r.ReplicaID == replToRemove.ReplicaID {
				remainingLiveVoters = append(liveVoterReplicas[:i:i], liveVoterReplicas[i+1:]...)
				break
			}
		}
	}

	lhBeingRemoved := removeIdx >= 0 && existingVoters[removeIdx].StoreID == repl.store.StoreID()
	// The allocator should not try to re-add this replica since there is a reason
	// we're removing it (i.e. dead or decommissioning). If we left the replica in
	// the slice, the allocator would not be guaranteed to pick a replica that
	// fills the gap removeRepl leaves once it's gone.
	newVoter, details, err := rq.allocator.AllocateVoter(ctx, conf, remainingLiveVoters, remainingLiveNonVoters, replicaStatus)
	if err != nil {
		return false, err
	}
	if removeIdx >= 0 && newVoter.StoreID == existingVoters[removeIdx].StoreID {
		return false, errors.AssertionFailedf("allocator suggested to replace replica on s%d with itself", newVoter.StoreID)
	}

	clusterNodes := rq.store.cfg.StorePool.ClusterNodeCount()
	neededVoters := allocatorimpl.GetNeededVoters(conf.GetNumVoters(), clusterNodes)

	// Only up-replicate if there are suitable allocation targets such that,
	// either the replication goal is met, or it is possible to get to the next
	// odd number of replicas. A consensus group of size 2n has worse failure
	// tolerance properties than a group of size 2n - 1 because it has a larger
	// quorum. For example, up-replicating from 1 to 2 replicas only makes sense
	// if it is possible to be able to go to 3 replicas.
	//
	// NB: If willHave > neededVoters, then always allow up-replicating as that
	// will be the case when up-replicating a range with a decommissioning
	// replica.
	//
	// We skip this check if we're swapping a replica, since that does not
	// change the quorum size.
	if willHave := len(existingVoters) + 1; removeIdx < 0 && willHave < neededVoters && willHave%2 == 0 {
		// This means we are going to up-replicate to an even replica state.
		// Check if it is possible to go to an odd replica state beyond it.
		oldPlusNewReplicas := append([]roachpb.ReplicaDescriptor(nil), existingVoters...)
		oldPlusNewReplicas = append(
			oldPlusNewReplicas,
			roachpb.ReplicaDescriptor{NodeID: newVoter.NodeID, StoreID: newVoter.StoreID},
		)
		_, _, err := rq.allocator.AllocateVoter(ctx, conf, oldPlusNewReplicas, remainingLiveNonVoters, replicaStatus)
		if err != nil {
			// It does not seem possible to go to the next odd replica state. Note
			// that AllocateVoter returns an allocatorError (a PurgatoryError)
			// when purgatory is requested.
			return false, errors.Wrap(err, "avoid up-replicating to fragile quorum")
		}
	}

	// Figure out whether we should be promoting an existing non-voting replica to
	// a voting replica or if we ought to be adding a voter afresh.
	var ops []roachpb.ReplicationChange
	replDesc, found := desc.GetReplicaDescriptor(newVoter.StoreID)
	if found {
		if replDesc.Type != roachpb.NON_VOTER {
			return false, errors.AssertionFailedf("allocation target %s for a voter"+
				" already has an unexpected replica: %s", newVoter, replDesc)
		}
		// If the allocation target has a non-voter already, we will promote it to a
		// voter.
		if !dryRun {
			rq.metrics.NonVoterPromotionsCount.Inc(1)
		}
		ops = roachpb.ReplicationChangesForPromotion(newVoter)
	} else {
		if !dryRun {
			rq.metrics.trackAddReplicaCount(allocatorimpl.VoterTarget)
		}
		ops = roachpb.MakeReplicationChanges(roachpb.ADD_VOTER, newVoter)
	}
	if removeIdx < 0 {
		log.KvDistribution.Infof(ctx, "adding voter %+v: %s",
			newVoter, rangeRaftProgress(repl.RaftStatus(), existingVoters))
	} else {
		if !dryRun {
			rq.metrics.trackRemoveMetric(allocatorimpl.VoterTarget, replicaStatus)
		}
		removeVoter := existingVoters[removeIdx]
		log.KvDistribution.Infof(ctx, "replacing voter %s with %+v: %s",
			removeVoter, newVoter, rangeRaftProgress(repl.RaftStatus(), existingVoters))
		// NB: We may have performed a promotion of a non-voter above, but we will
		// not perform a demotion here and instead just remove the existing replica
		// entirely. This is because we know that the `removeVoter` is either dead
		// or decommissioning (see `Allocator.computeAction`). This means that after
		// this allocation is executed, we could be one non-voter short. This will
		// be handled by the replicateQueue's next attempt at this range.
		ops = append(ops,
			roachpb.MakeReplicationChanges(roachpb.REMOVE_VOTER, roachpb.ReplicationTarget{
				StoreID: removeVoter.StoreID,
				NodeID:  removeVoter.NodeID,
			})...)
	}

	if err := rq.changeReplicas(
		ctx,
		repl,
		ops,
		desc,
		kvserverpb.SnapshotRequest_RECOVERY,
		allocatorPriority,
		kvserverpb.ReasonRangeUnderReplicated,
		details,
		dryRun,
	); err != nil {
		return false, err
	}
	// Unless just removed myself (the leaseholder), always requeue to see
	// if more work needs to be done. If leaseholder is removed, someone
	// else will take over.
	return !lhBeingRemoved, nil
}

// addOrReplaceNonVoters adds a non-voting replica to `repl`s range.
func (rq *replicateQueue) addOrReplaceNonVoters(
	ctx context.Context,
	repl *Replica,
	liveVoterReplicas, liveNonVoterReplicas []roachpb.ReplicaDescriptor,
	removeIdx int,
	replicaStatus allocatorimpl.ReplicaStatus,
	allocatorPrio float64,
	dryRun bool,
) (requeue bool, _ error) {
	desc, conf := repl.DescAndSpanConfig()
	existingNonVoters := desc.Replicas().NonVoterDescriptors()

	newNonVoter, details, err := rq.allocator.AllocateNonVoter(ctx, conf, liveVoterReplicas, liveNonVoterReplicas, replicaStatus)
	if err != nil {
		return false, err
	}
	if !dryRun {
		rq.metrics.trackAddReplicaCount(allocatorimpl.NonVoterTarget)
	}

	ops := roachpb.MakeReplicationChanges(roachpb.ADD_NON_VOTER, newNonVoter)
	if removeIdx < 0 {
		log.KvDistribution.Infof(ctx, "adding non-voter %+v: %s",
			newNonVoter, rangeRaftProgress(repl.RaftStatus(), existingNonVoters))
	} else {
		if !dryRun {
			rq.metrics.trackRemoveMetric(allocatorimpl.NonVoterTarget, replicaStatus)
		}
		removeNonVoter := existingNonVoters[removeIdx]
		log.KvDistribution.Infof(ctx, "replacing non-voter %s with %+v: %s",
			removeNonVoter, newNonVoter, rangeRaftProgress(repl.RaftStatus(), existingNonVoters))
		ops = append(ops,
			roachpb.MakeReplicationChanges(roachpb.REMOVE_NON_VOTER, roachpb.ReplicationTarget{
				StoreID: removeNonVoter.StoreID,
				NodeID:  removeNonVoter.NodeID,
			})...)
	}

	if err := rq.changeReplicas(
		ctx,
		repl,
		ops,
		desc,
		kvserverpb.SnapshotRequest_RECOVERY,
		allocatorPrio,
		kvserverpb.ReasonRangeUnderReplicated,
		details,
		dryRun,
	); err != nil {
		return false, err
	}
	// Always requeue to see if more work needs to be done.
	return true, nil
}

// findRemoveVoter takes a list of voting replicas and picks one to remove,
// making sure to not remove a newly added voter or to violate the zone configs
// in the process.
//
// TODO(aayush): The structure around replica removal is not great. The entire
// logic of this method should probably live inside Allocator.RemoveVoter. Doing
// so also makes the flow of adding new replicas and removing replicas more
// symmetric.
func (rq *replicateQueue) findRemoveVoter(
	ctx context.Context,
	repl interface {
		DescAndSpanConfig() (*roachpb.RangeDescriptor, roachpb.SpanConfig)
		LastReplicaAdded() (roachpb.ReplicaID, time.Time)
		RaftStatus() *raft.Status
	},
	existingVoters, existingNonVoters []roachpb.ReplicaDescriptor,
) (roachpb.ReplicationTarget, string, error) {
	_, zone := repl.DescAndSpanConfig()
	// This retry loop involves quick operations on local state, so a
	// small MaxBackoff is good (but those local variables change on
	// network time scales as raft receives responses).
	//
	// TODO(bdarnell): There's another retry loop at process(). It
	// would be nice to combine these, but I'm keeping them separate
	// for now so we can tune the options separately.
	retryOpts := retry.Options{
		InitialBackoff: time.Millisecond,
		MaxBackoff:     200 * time.Millisecond,
		Multiplier:     2,
	}

	var candidates []roachpb.ReplicaDescriptor
	deadline := timeutil.Now().Add(2 * base.NetworkTimeout)
	for r := retry.StartWithCtx(ctx, retryOpts); r.Next() && timeutil.Now().Before(deadline); {
		lastReplAdded, lastAddedTime := repl.LastReplicaAdded()
		if timeutil.Since(lastAddedTime) > newReplicaGracePeriod {
			lastReplAdded = 0
		}
		raftStatus := repl.RaftStatus()
		if raftStatus == nil || raftStatus.RaftState != raft.StateLeader {
			// If we've lost raft leadership, we're unlikely to regain it so give up immediately.
			return roachpb.ReplicationTarget{}, "", &benignError{errors.Errorf("not raft leader while range needs removal")}
		}
		candidates = allocatorimpl.FilterUnremovableReplicas(ctx, raftStatus, existingVoters, lastReplAdded)
		log.KvDistribution.VEventf(ctx, 3, "filtered unremovable replicas from %v to get %v as candidates for removal: %s",
			existingVoters, candidates, rangeRaftProgress(raftStatus, existingVoters))
		if len(candidates) > 0 {
			break
		}
		if len(raftStatus.Progress) <= 2 {
			// HACK(bdarnell): Downreplicating to a single node from
			// multiple nodes is not really supported. There are edge
			// cases in which the two peers stop communicating with each
			// other too soon and we don't reach a satisfactory
			// resolution. However, some tests (notably
			// TestRepartitioning) get into this state, and if the
			// replication queue spends its entire timeout waiting for the
			// downreplication to finish the test will time out. As a
			// hack, just fail-fast when we're trying to go down to a
			// single replica.
			break
		}
		// After upreplication, the candidates for removal could still
		// be catching up. The allocator determined that the range was
		// over-replicated, and it's important to clear that state as
		// quickly as we can (because over-replicated ranges may be
		// under-diversified). If we return an error here, this range
		// probably won't be processed again until the next scanner
		// cycle, which is too long, so we retry here.
	}
	if len(candidates) == 0 {
		// If we timed out and still don't have any valid candidates, give up.
		return roachpb.ReplicationTarget{}, "", &benignError{
			errors.Errorf(
				"no removable replicas from range that needs a removal: %s",
				rangeRaftProgress(repl.RaftStatus(), existingVoters),
			),
		}
	}

	return rq.allocator.RemoveVoter(
		ctx,
		zone,
		candidates,
		existingVoters,
		existingNonVoters,
		rq.allocator.ScorerOptions(ctx),
	)
}

// maybeTransferLeaseAway is called whenever a replica on a given store is
// slated for removal. If the store corresponds to the store of the caller
// (which is very likely to be the leaseholder), then this removal would fail.
// Instead, this method will attempt to transfer the lease away, and returns
// true to indicate to the caller that it should not pursue the current
// replication change further because it is no longer the leaseholder. When the
// returned bool is false, it should continue. On error, the caller should also
// stop. If canTransferLeaseFrom is non-nil, it is consulted and an error is
// returned if it returns false.
func (rq *replicateQueue) maybeTransferLeaseAway(
	ctx context.Context,
	repl *Replica,
	removeStoreID roachpb.StoreID,
	dryRun bool,
	canTransferLeaseFrom func(ctx context.Context, repl *Replica) bool,
) (done bool, _ error) {
	if removeStoreID != repl.store.StoreID() {
		return false, nil
	}
	if canTransferLeaseFrom != nil && !canTransferLeaseFrom(ctx, repl) {
		return false, errors.Errorf("cannot transfer lease")
	}
	desc, conf := repl.DescAndSpanConfig()
	// The local replica was selected as the removal target, but that replica
	// is the leaseholder, so transfer the lease instead. We don't check that
	// the current store has too many leases in this case under the
	// assumption that replica balance is a greater concern. Also note that
	// AllocatorRemoveVoter action takes preference over AllocatorConsiderRebalance
	// (rebalancing) which is where lease transfer would otherwise occur. We
	// need to be able to transfer leases in AllocatorRemoveVoter in order to get
	// out of situations where this store is overfull and yet holds all the
	// leases. The fullness checks need to be ignored for cases where
	// a replica needs to be removed for constraint violations.
	transferred, err := rq.shedLease(
		ctx,
		repl,
		desc,
		conf,
		allocator.TransferLeaseOptions{
			Goal:   allocator.LeaseCountConvergence,
			DryRun: dryRun,
			// NB: This option means that the allocator is asked to not consider the
			// current replica in its set of potential candidates.
			ExcludeLeaseRepl: true,
		},
	)
	return transferred == allocator.TransferOK, err
}

func (rq *replicateQueue) removeVoter(
	ctx context.Context,
	repl *Replica,
	existingVoters, existingNonVoters []roachpb.ReplicaDescriptor,
	dryRun bool,
) (requeue bool, _ error) {
	removeVoter, details, err := rq.findRemoveVoter(ctx, repl, existingVoters, existingNonVoters)
	if err != nil {
		return false, err
	}
	done, err := rq.maybeTransferLeaseAway(
		ctx, repl, removeVoter.StoreID, dryRun, nil /* canTransferLeaseFrom */)
	if err != nil {
		return false, err
	}
	if done {
		// Lease is now elsewhere, so we're not in charge any more.
		return false, nil
	}

	// Remove a replica.
	if !dryRun {
		rq.metrics.trackRemoveMetric(allocatorimpl.VoterTarget, allocatorimpl.Alive)
	}

	log.KvDistribution.Infof(ctx, "removing voting replica %+v due to over-replication: %s",
		removeVoter, rangeRaftProgress(repl.RaftStatus(), existingVoters))
	desc := repl.Desc()
	// TODO(aayush): Directly removing the voter here is a bit of a missed
	// opportunity since we could potentially be 1 non-voter short and the
	// `target` could be a valid store for a non-voter. In such a scenario, we
	// could save a bunch of work by just performing an atomic demotion of a
	// voter.
	if err := rq.changeReplicas(
		ctx,
		repl,
		roachpb.MakeReplicationChanges(roachpb.REMOVE_VOTER, removeVoter),
		desc,
		kvserverpb.SnapshotRequest_UNKNOWN, // unused
		0.0,                                // unused
		kvserverpb.ReasonRangeOverReplicated,
		details,
		dryRun,
	); err != nil {
		return false, err
	}
	return true, nil
}

func (rq *replicateQueue) removeNonVoter(
	ctx context.Context,
	repl *Replica,
	existingVoters, existingNonVoters []roachpb.ReplicaDescriptor,
	dryRun bool,
) (requeue bool, _ error) {

	desc, conf := repl.DescAndSpanConfig()
	removeNonVoter, details, err := rq.allocator.RemoveNonVoter(
		ctx,
		conf,
		existingNonVoters,
		existingVoters,
		existingNonVoters,
		rq.allocator.ScorerOptions(ctx),
	)
	if err != nil {
		return false, err
	}
	if !dryRun {
		rq.metrics.trackRemoveMetric(allocatorimpl.NonVoterTarget, allocatorimpl.Alive)
	}
	log.KvDistribution.Infof(ctx, "removing non-voting replica %+v due to over-replication: %s",
		removeNonVoter, rangeRaftProgress(repl.RaftStatus(), existingVoters))
	target := roachpb.ReplicationTarget{
		NodeID:  removeNonVoter.NodeID,
		StoreID: removeNonVoter.StoreID,
	}

	if err := rq.changeReplicas(
		ctx,
		repl,
		roachpb.MakeReplicationChanges(roachpb.REMOVE_NON_VOTER, target),
		desc,
		kvserverpb.SnapshotRequest_UNKNOWN, // unused
		0.0,                                // unused
		kvserverpb.ReasonRangeOverReplicated,
		details,
		dryRun,
	); err != nil {
		return false, err
	}
	return true, nil
}

func (rq *replicateQueue) removeDecommissioning(
	ctx context.Context, repl *Replica, targetType allocatorimpl.TargetReplicaType, dryRun bool,
) (requeue bool, _ error) {
	desc := repl.Desc()
	var decommissioningReplicas []roachpb.ReplicaDescriptor
	switch targetType {
	case allocatorimpl.VoterTarget:
		decommissioningReplicas = rq.store.cfg.StorePool.DecommissioningReplicas(
			desc.Replicas().VoterDescriptors(),
		)
	case allocatorimpl.NonVoterTarget:
		decommissioningReplicas = rq.store.cfg.StorePool.DecommissioningReplicas(
			desc.Replicas().NonVoterDescriptors(),
		)
	default:
		panic(fmt.Sprintf("unknown targetReplicaType: %s", targetType))
	}

	if len(decommissioningReplicas) == 0 {
		log.KvDistribution.Infof(ctx, "range of %[1]ss %[2]s was identified as having decommissioning %[1]ss, "+
			"but no decommissioning %[1]ss were found", targetType, repl)
		return true, nil
	}
	decommissioningReplica := decommissioningReplicas[0]

	done, err := rq.maybeTransferLeaseAway(
		ctx, repl, decommissioningReplica.StoreID, dryRun, nil /* canTransferLease */)
	if err != nil {
		return false, err
	}
	if done {
		// Not leaseholder any more.
		return false, nil
	}

	// Remove the decommissioning replica.
	if !dryRun {
		rq.metrics.trackRemoveMetric(targetType, allocatorimpl.Decommissioning)
	}
	log.KvDistribution.Infof(ctx, "removing decommissioning %s %+v from store", targetType, decommissioningReplica)
	target := roachpb.ReplicationTarget{
		NodeID:  decommissioningReplica.NodeID,
		StoreID: decommissioningReplica.StoreID,
	}
	if err := rq.changeReplicas(
		ctx,
		repl,
		roachpb.MakeReplicationChanges(targetType.RemoveChangeType(), target),
		desc,
		kvserverpb.SnapshotRequest_UNKNOWN, // unused
		0.0,                                // unused
		kvserverpb.ReasonStoreDecommissioning, "", dryRun,
	); err != nil {
		return false, err
	}
	// We removed a replica, so check if there's more to do.
	return true, nil
}

func (rq *replicateQueue) removeDead(
	ctx context.Context,
	repl *Replica,
	deadReplicas []roachpb.ReplicaDescriptor,
	targetType allocatorimpl.TargetReplicaType,
	dryRun bool,
) (requeue bool, _ error) {
	desc := repl.Desc()
	if len(deadReplicas) == 0 {
		log.KvDistribution.Infof(
			ctx,
			"range of %[1]s %[2]s was identified as having dead %[1]ss, but no dead %[1]ss were found",
			targetType,
			repl,
		)
		return true, nil
	}
	deadReplica := deadReplicas[0]
	if !dryRun {
		rq.metrics.trackRemoveMetric(targetType, allocatorimpl.Dead)
	}
	log.KvDistribution.Infof(ctx, "removing dead %s %+v from store", targetType, deadReplica)
	target := roachpb.ReplicationTarget{
		NodeID:  deadReplica.NodeID,
		StoreID: deadReplica.StoreID,
	}

	// NB: When removing a dead voter, we don't check whether to transfer the
	// lease away because if the removal target is dead, it's not the voter being
	// removed (and if for some reason that happens, the removal is simply going
	// to fail).
	if err := rq.changeReplicas(
		ctx,
		repl,
		roachpb.MakeReplicationChanges(targetType.RemoveChangeType(), target),
		desc,
		kvserverpb.SnapshotRequest_UNKNOWN, // unused
		0.0,                                // unused
		kvserverpb.ReasonStoreDead,
		"",
		dryRun,
	); err != nil {
		return false, err
	}
	return true, nil
}

func (rq *replicateQueue) considerRebalance(
	ctx context.Context,
	repl *Replica,
	existingVoters, existingNonVoters []roachpb.ReplicaDescriptor,
	allocatorPrio float64,
	canTransferLeaseFrom func(ctx context.Context, repl *Replica) bool,
	scatter, dryRun bool,
) (requeue bool, _ error) {
	desc, conf := repl.DescAndSpanConfig()
	rebalanceTargetType := allocatorimpl.VoterTarget

	scorerOpts := allocatorimpl.ScorerOptions(rq.allocator.ScorerOptions(ctx))
	if scatter {
		scorerOpts = rq.allocator.ScorerOptionsForScatter(ctx)
	}
	if !rq.store.TestingKnobs().DisableReplicaRebalancing {
		rangeUsageInfo := RangeUsageInfoForRepl(repl)
		addTarget, removeTarget, details, ok := rq.allocator.RebalanceVoter(
			ctx,
			conf,
			repl.RaftStatus(),
			existingVoters,
			existingNonVoters,
			rangeUsageInfo,
			storepool.StoreFilterThrottled,
			scorerOpts,
		)
		if !ok {
			// If there was nothing to do for the set of voting replicas on this
			// range, attempt to rebalance non-voters.
			log.KvDistribution.Infof(ctx, "no suitable rebalance target for voters")
			addTarget, removeTarget, details, ok = rq.allocator.RebalanceNonVoter(
				ctx,
				conf,
				repl.RaftStatus(),
				existingVoters,
				existingNonVoters,
				rangeUsageInfo,
				storepool.StoreFilterThrottled,
				scorerOpts,
			)
			rebalanceTargetType = allocatorimpl.NonVoterTarget
		}

		// Determines whether we can remove the leaseholder without first
		// transferring the lease away
		lhRemovalAllowed := addTarget != (roachpb.ReplicationTarget{})
		lhBeingRemoved := removeTarget.StoreID == repl.store.StoreID()

		if !ok {
			log.KvDistribution.Infof(ctx, "no suitable rebalance target for non-voters")
		} else if !lhRemovalAllowed {
			if done, err := rq.maybeTransferLeaseAway(
				ctx, repl, removeTarget.StoreID, dryRun, canTransferLeaseFrom,
			); err != nil {
				log.KvDistribution.Infof(ctx, "want to remove self, but failed to transfer lease away: %s", err)
				ok = false
			} else if done {
				// Lease is now elsewhere, so we're not in charge any more.
				return false, nil
			}
		}
		if ok {
			// If we have a valid rebalance action (ok == true) and we haven't
			// transferred our lease away, execute the rebalance.
			chgs, performingSwap, err := replicationChangesForRebalance(ctx, desc, len(existingVoters), addTarget,
				removeTarget, rebalanceTargetType)
			if err != nil {
				return false, err
			}
			if !dryRun {
				rq.metrics.trackRebalanceReplicaCount(rebalanceTargetType)
				if performingSwap {
					rq.metrics.VoterDemotionsCount.Inc(1)
					rq.metrics.NonVoterPromotionsCount.Inc(1)
				}
			}
			log.KvDistribution.Infof(ctx,
				"rebalancing %s %+v to %+v: %s",
				rebalanceTargetType,
				removeTarget,
				addTarget,
				rangeRaftProgress(repl.RaftStatus(), existingVoters))

			if err := rq.changeReplicas(
				ctx,
				repl,
				chgs,
				desc,
				kvserverpb.SnapshotRequest_REBALANCE,
				allocatorPrio,
				kvserverpb.ReasonRebalance,
				details,
				dryRun,
			); err != nil {
				return false, err
			}
			// Unless just removed myself (the leaseholder), always requeue to see
			// if more work needs to be done. If leaseholder is removed, someone
			// else will take over.
			return !lhBeingRemoved, nil
		}
	}
	return false, nil
}

// replicationChangesForRebalance returns a list of ReplicationChanges to
// execute for a rebalancing decision made by the allocator.
//
// This function assumes that `addTarget` and `removeTarget` are produced by the
// allocator (i.e. they satisfy replica `constraints` and potentially
// `voter_constraints` if we're operating over voter targets).
func replicationChangesForRebalance(
	ctx context.Context,
	desc *roachpb.RangeDescriptor,
	numExistingVoters int,
	addTarget, removeTarget roachpb.ReplicationTarget,
	rebalanceTargetType allocatorimpl.TargetReplicaType,
) (chgs []roachpb.ReplicationChange, performingSwap bool, err error) {
	if rebalanceTargetType == allocatorimpl.VoterTarget && numExistingVoters == 1 {
		// If there's only one replica, the removal target is the
		// leaseholder and this is unsupported and will fail. However,
		// this is also the only way to rebalance in a single-replica
		// range. If we try the atomic swap here, we'll fail doing
		// nothing, and so we stay locked into the current distribution
		// of replicas. (Note that maybeTransferLeaseAway above will not
		// have found a target, and so will have returned (false, nil).
		//
		// Do the best thing we can, which is carry out the addition
		// only, which should succeed, and the next time we touch this
		// range, we will have one more replica and hopefully it will
		// take the lease and remove the current leaseholder.
		//
		// It's possible that "rebalancing deadlock" can occur in other
		// scenarios, it's really impossible to tell from the code given
		// the constraints we support. However, the lease transfer often
		// does not happen spuriously, and we can't enter dangerous
		// configurations sporadically, so this code path is only hit
		// when we know it's necessary, picking the smaller of two evils.
		//
		// See https://github.com/cockroachdb/cockroach/issues/40333.
		chgs = []roachpb.ReplicationChange{
			{ChangeType: roachpb.ADD_VOTER, Target: addTarget},
		}
		log.KvDistribution.Infof(ctx, "can't swap replica due to lease; falling back to add")
		return chgs, false, err
	}

	rdesc, found := desc.GetReplicaDescriptor(addTarget.StoreID)
	switch rebalanceTargetType {
	case allocatorimpl.VoterTarget:
		// Check if the target being added already has a non-voting replica.
		if found && rdesc.Type == roachpb.NON_VOTER {
			// If the receiving store already has a non-voting replica, we *must*
			// execute a swap between that non-voting replica and the voting replica
			// we're trying to move to it. This swap is executed atomically via
			// joint-consensus.
			//
			// NB: Since voting replicas abide by both the overall `constraints` and
			// the `voter_constraints`, it is copacetic to make this swap since:
			//
			// 1. `addTarget` must already be a valid target for a voting replica
			// (i.e. it must already satisfy both *constraints fields) since an
			// allocator method (`allocateTarget..` or `Rebalance{Non}Voter`) just
			// handed it to us.
			// 2. `removeTarget` may or may not be a valid target for a non-voting
			// replica, but `considerRebalance` takes care to `requeue` the current
			// replica into the replicateQueue. So we expect the replicateQueue's next
			// attempt at rebalancing this range to rebalance the non-voter if it ends
			// up being in violation of the range's constraints.
			promo := roachpb.ReplicationChangesForPromotion(addTarget)
			demo := roachpb.ReplicationChangesForDemotion(removeTarget)
			chgs = append(promo, demo...)
			performingSwap = true
		} else if found {
			return nil, false, errors.AssertionFailedf(
				"programming error:"+
					" store being rebalanced to(%s) already has a voting replica", addTarget.StoreID,
			)
		} else {
			// We have a replica to remove and one we can add, so let's swap them out.
			chgs = []roachpb.ReplicationChange{
				{ChangeType: roachpb.ADD_VOTER, Target: addTarget},
				{ChangeType: roachpb.REMOVE_VOTER, Target: removeTarget},
			}
		}
	case allocatorimpl.NonVoterTarget:
		if found {
			// Non-voters should not consider any of the range's existing stores as
			// valid candidates. If we get here, we must have raced with another
			// rebalancing decision.
			return nil, false, errors.AssertionFailedf(
				"invalid rebalancing decision: trying to"+
					" move non-voter to a store that already has a replica %s for the range", rdesc,
			)
		}
		chgs = []roachpb.ReplicationChange{
			{ChangeType: roachpb.ADD_NON_VOTER, Target: addTarget},
			{ChangeType: roachpb.REMOVE_NON_VOTER, Target: removeTarget},
		}
	}
	return chgs, performingSwap, nil
}

// shedLease takes in a leaseholder replica, looks for a target for transferring
// the lease and, if a suitable target is found (e.g. alive, not draining),
// transfers the lease away.
func (rq *replicateQueue) shedLease(
	ctx context.Context,
	repl *Replica,
	desc *roachpb.RangeDescriptor,
	conf roachpb.SpanConfig,
	opts allocator.TransferLeaseOptions,
) (allocator.LeaseTransferOutcome, error) {
	// Learner replicas aren't allowed to become the leaseholder or raft leader,
	// so only consider the `VoterDescriptors` replicas.
	target := rq.allocator.TransferLeaseTarget(
		ctx,
		conf,
		desc.Replicas().VoterDescriptors(),
		repl,
		RangeUsageInfoForRepl(repl),
		false, /* forceDecisionWithoutStats */
		opts,
	)
	if target == (roachpb.ReplicaDescriptor{}) {
		return allocator.NoSuitableTarget, nil
	}

	if opts.DryRun {
		log.KvDistribution.Infof(ctx, "transferring lease to s%d", target.StoreID)
		return allocator.NoTransferDryRun, nil
	}

	if err := rq.TransferLease(ctx, repl, repl.store.StoreID(), target.StoreID, RangeUsageInfoForRepl(repl)); err != nil {
		return allocator.TransferErr, err
	}
	return allocator.TransferOK, nil
}

// ReplicaLeaseMover handles lease transfers for a single range.
type ReplicaLeaseMover interface {
	// AdminTransferLease moves the lease to the requested store.
	AdminTransferLease(ctx context.Context, target roachpb.StoreID, bypassSafetyChecks bool) error

	// String returns info about the replica.
	String() string
}

// RangeRebalancer handles replica moves and lease transfers.
type RangeRebalancer interface {
	// TransferLease uses a LeaseMover interface to move a lease between stores.
	// The QPS is used to update stats for the stores.
	TransferLease(
		ctx context.Context,
		rlm ReplicaLeaseMover,
		source, target roachpb.StoreID,
		rangeUsageInfo allocator.RangeUsageInfo,
	) error

	// RelocateRange relocates replicas to the requested stores, and can transfer
	// the lease for the range to the first target voter.
	RelocateRange(
		ctx context.Context,
		key interface{},
		voterTargets, nonVoterTargets []roachpb.ReplicationTarget,
		transferLeaseToFirstVoter bool,
	) error
}

// TransferLease implements the RangeRebalancer interface.
func (rq *replicateQueue) TransferLease(
	ctx context.Context,
	rlm ReplicaLeaseMover,
	source, target roachpb.StoreID,
	rangeUsageInfo allocator.RangeUsageInfo,
) error {
	rq.metrics.TransferLeaseCount.Inc(1)
	log.KvDistribution.Infof(ctx, "transferring lease to s%d", target)
	if err := rlm.AdminTransferLease(ctx, target, false /* bypassSafetyChecks */); err != nil {
		return errors.Wrapf(err, "%s: unable to transfer lease to s%d", rlm, target)
	}
	rq.lastLeaseTransfer.Store(timeutil.Now())
	rq.store.cfg.StorePool.UpdateLocalStoresAfterLeaseTransfer(
		source, target, rangeUsageInfo)
	return nil
}

// RelocateRange implements the RangeRebalancer interface.
func (rq *replicateQueue) RelocateRange(
	ctx context.Context,
	key interface{},
	voterTargets, nonVoterTargets []roachpb.ReplicationTarget,
	transferLeaseToFirstVoter bool,
) error {
	return rq.store.DB().AdminRelocateRange(
		ctx,
		key,
		voterTargets,
		nonVoterTargets,
		transferLeaseToFirstVoter,
	)
}

func (rq *replicateQueue) changeReplicas(
	ctx context.Context,
	repl *Replica,
	chgs roachpb.ReplicationChanges,
	desc *roachpb.RangeDescriptor,
	priority kvserverpb.SnapshotRequest_Priority,
	allocatorPriority float64,
	reason kvserverpb.RangeLogEventReason,
	details string,
	dryRun bool,
) error {
	if dryRun {
		return nil
	}
	// NB: this calls the impl rather than ChangeReplicas because
	// the latter traps tests that try to call it while the replication
	// queue is active.
	if _, err := repl.changeReplicasImpl(
		ctx, desc, priority, kvserverpb.SnapshotRequest_REPLICATE_QUEUE, allocatorPriority, reason,
		details, chgs,
	); err != nil {
		return err
	}
	rangeUsageInfo := RangeUsageInfoForRepl(repl)
	for _, chg := range chgs {
		rq.store.cfg.StorePool.UpdateLocalStoreAfterRebalance(
			chg.Target.StoreID, rangeUsageInfo, chg.ChangeType)
	}
	return nil
}

// canTransferLeaseFrom checks is a lease can be transferred from the specified
// replica. It considers two factors if the replica is in -conformance with
// lease preferences and the last time a transfer occurred to avoid thrashing.
func (rq *replicateQueue) canTransferLeaseFrom(ctx context.Context, repl *Replica) bool {
	if !repl.OwnsValidLease(ctx, repl.store.cfg.Clock.NowAsClockTimestamp()) {
		// This replica is not the leaseholder, so it can't transfer the lease.
		return false
	}
	// Do a best effort check to see if this replica conforms to the configured
	// lease preferences (if any), if it does not we want to encourage more
	// aggressive lease movement and not delay it.
	if repl.leaseViolatesPreferences(ctx) {
		return true
	}
	if lastLeaseTransfer := rq.lastLeaseTransfer.Load(); lastLeaseTransfer != nil {
		minInterval := MinLeaseTransferInterval.Get(&rq.store.cfg.Settings.SV)
		return timeutil.Since(lastLeaseTransfer.(time.Time)) > minInterval
	}
	return true
}

func (*replicateQueue) postProcessScheduled(
	ctx context.Context, replica replicaInQueue, priority float64,
) {
}

func (*replicateQueue) timer(_ time.Duration) time.Duration {
	return replicateQueueTimerDuration
}

func (rq *replicateQueue) purgatoryChan() <-chan time.Time {
	return rq.purgCh
}

// updateChan returns the replicate queue's store update channel.
func (rq *replicateQueue) updateChan() <-chan time.Time {
	return rq.updateCh
}

// rangeRaftStatus pretty-prints the Raft progress (i.e. Raft log position) of
// the replicas.
func rangeRaftProgress(raftStatus *raft.Status, replicas []roachpb.ReplicaDescriptor) string {
	if raftStatus == nil {
		return "[no raft status]"
	} else if len(raftStatus.Progress) == 0 {
		return "[no raft progress]"
	}
	var buf bytes.Buffer
	buf.WriteString("[")
	for i, r := range replicas {
		if i > 0 {
			buf.WriteString(", ")
		}
		fmt.Fprintf(&buf, "%d", r.ReplicaID)
		if uint64(r.ReplicaID) == raftStatus.Lead {
			buf.WriteString("*")
		}
		if progress, ok := raftStatus.Progress[uint64(r.ReplicaID)]; ok {
			fmt.Fprintf(&buf, ":%d", progress.Match)
		} else {
			buf.WriteString(":?")
		}
	}
	buf.WriteString("]")
	return buf.String()
}

// RangeUsageInfoForRepl returns the usage information for the replica,
// assigning it to the range usage information.
// NB: This is currently just the replicas usage, it assumes that the range's
// usage information matches. Which is dubious in some cases but often
// reasoanble when we only consider the leaseholder.
func RangeUsageInfoForRepl(repl *Replica) allocator.RangeUsageInfo {
	info := allocator.RangeUsageInfo{
		LogicalBytes: repl.GetMVCCStats().Total(),
	}

	localitySummary := repl.loadStats.batchRequests.SnapshotRatedSummary()
	info.RequestLocality = &allocator.RangeRequestLocalityInfo{
		Counts:   localitySummary.LocalityCounts,
		Duration: localitySummary.Duration,
	}

	if localitySummary.Duration >= replicastats.MinStatsDuration {
		info.QueriesPerSecond = localitySummary.QPS
	}

	if writesPerSecond, dur := repl.loadStats.writeKeys.AverageRatePerSecond(); dur >= replicastats.MinStatsDuration {
		info.WritesPerSecond = writesPerSecond
	}

	return info
}
