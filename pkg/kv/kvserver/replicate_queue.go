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

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/allocatorimpl"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
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
	"github.com/cockroachdb/redact"
	raft "go.etcd.io/raft/v3"
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
		Help:        "Number of successful dead replica replacements processed by the replicate queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicateQueueReplaceDeadReplicaErrorCount = metric.Metadata{
		Name:        "queue.replicate.replacedeadreplica.error",
		Help:        "Number of failed dead replica replacements processed by the replicate queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicateQueueReplaceDecommissioningReplicaSuccessCount = metric.Metadata{
		Name:        "queue.replicate.replacedecommissioningreplica.success",
		Help:        "Number of successful decommissioning replica replacements processed by the replicate queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicateQueueReplaceDecommissioningReplicaErrorCount = metric.Metadata{
		Name:        "queue.replicate.replacedecommissioningreplica.error",
		Help:        "Number of failed decommissioning replica replacements processed by the replicate queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicateQueueRemoveDecommissioningReplicaSuccessCount = metric.Metadata{
		Name:        "queue.replicate.removedecommissioningreplica.success",
		Help:        "Number of successful decommissioning replica removals processed by the replicate queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicateQueueRemoveDecommissioningReplicaErrorCount = metric.Metadata{
		Name:        "queue.replicate.removedecommissioningreplica.error",
		Help:        "Number of failed decommissioning replica removals processed by the replicate queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicateQueueRemoveDeadReplicaSuccessCount = metric.Metadata{
		Name:        "queue.replicate.removedeadreplica.success",
		Help:        "Number of successful dead replica removals processed by the replicate queue",
		Measurement: "Replicas",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicateQueueRemoveDeadReplicaErrorCount = metric.Metadata{
		Name:        "queue.replicate.removedeadreplica.error",
		Help:        "Number of failed dead replica removals processed by the replicate queue",
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

// trackSuccessByAllocatorAction increases the corresponding success count
// metric for successfully applying a particular allocator action through the
// replicate queue.
func (metrics *ReplicateQueueMetrics) trackSuccessByAllocatorAction(
	ctx context.Context, action allocatorimpl.AllocatorAction,
) {
	switch action {
	case allocatorimpl.AllocatorRemoveVoter, allocatorimpl.AllocatorRemoveNonVoter:
		metrics.RemoveReplicaSuccessCount.Inc(1)
	case allocatorimpl.AllocatorAddVoter, allocatorimpl.AllocatorAddNonVoter:
		metrics.AddReplicaSuccessCount.Inc(1)
	case allocatorimpl.AllocatorReplaceDeadVoter, allocatorimpl.AllocatorReplaceDeadNonVoter:
		metrics.ReplaceDeadReplicaSuccessCount.Inc(1)
	case allocatorimpl.AllocatorRemoveDeadVoter, allocatorimpl.AllocatorRemoveDeadNonVoter:
		metrics.RemoveDeadReplicaSuccessCount.Inc(1)
	case allocatorimpl.AllocatorReplaceDecommissioningVoter, allocatorimpl.AllocatorReplaceDecommissioningNonVoter:
		metrics.ReplaceDecommissioningReplicaSuccessCount.Inc(1)
	case allocatorimpl.AllocatorRemoveDecommissioningVoter, allocatorimpl.AllocatorRemoveDecommissioningNonVoter:
		metrics.RemoveDecommissioningReplicaSuccessCount.Inc(1)
	case allocatorimpl.AllocatorConsiderRebalance, allocatorimpl.AllocatorNoop,
		allocatorimpl.AllocatorRangeUnavailable, allocatorimpl.AllocatorRemoveLearner,
		allocatorimpl.AllocatorFinalizeAtomicReplicationChange:
		// Nothing to do, not recorded here.
	default:
		log.Errorf(ctx, "AllocatorAction %v unsupported in metrics tracking", action)
	}
}

// trackErrorByAllocatorAction increases the corresponding error count metric
// for failures in applying a particular allocator action through the replicate
// queue.
func (metrics *ReplicateQueueMetrics) trackErrorByAllocatorAction(
	ctx context.Context, action allocatorimpl.AllocatorAction,
) {
	switch action {
	case allocatorimpl.AllocatorRemoveVoter, allocatorimpl.AllocatorRemoveNonVoter:
		metrics.RemoveReplicaErrorCount.Inc(1)
	case allocatorimpl.AllocatorAddVoter, allocatorimpl.AllocatorAddNonVoter:
		metrics.AddReplicaErrorCount.Inc(1)
	case allocatorimpl.AllocatorReplaceDeadVoter, allocatorimpl.AllocatorReplaceDeadNonVoter:
		metrics.ReplaceDeadReplicaErrorCount.Inc(1)
	case allocatorimpl.AllocatorRemoveDeadVoter, allocatorimpl.AllocatorRemoveDeadNonVoter:
		metrics.RemoveDeadReplicaErrorCount.Inc(1)
	case allocatorimpl.AllocatorReplaceDecommissioningVoter, allocatorimpl.AllocatorReplaceDecommissioningNonVoter:
		metrics.ReplaceDecommissioningReplicaErrorCount.Inc(1)
	case allocatorimpl.AllocatorRemoveDecommissioningVoter, allocatorimpl.AllocatorRemoveDecommissioningNonVoter:
		metrics.RemoveDecommissioningReplicaErrorCount.Inc(1)
	case allocatorimpl.AllocatorConsiderRebalance, allocatorimpl.AllocatorNoop,
		allocatorimpl.AllocatorRangeUnavailable, allocatorimpl.AllocatorRemoveLearner,
		allocatorimpl.AllocatorFinalizeAtomicReplicationChange:
		// Nothing to do, not recorded here.
	default:
		log.Errorf(ctx, "AllocatorAction %v unsupported in metrics tracking", action)
	}

}

// trackProcessResult increases the corresponding success/error count metric for
// processing a particular allocator action through the replicate queue.
func (metrics *ReplicateQueueMetrics) trackResultByAllocatorAction(
	ctx context.Context, action allocatorimpl.AllocatorAction, err error,
) {
	if err != nil {
		metrics.trackErrorByAllocatorAction(ctx, action)
	} else {
		metrics.trackSuccessByAllocatorAction(ctx, action)
	}
}

// replicateQueue manages a queue of replicas which may need to add an
// additional replica to their range.
type replicateQueue struct {
	*baseQueue
	metrics   ReplicateQueueMetrics
	allocator allocatorimpl.Allocator
	storePool storepool.AllocatorStorePool

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
	var storePool storepool.AllocatorStorePool
	if store.cfg.StorePool != nil {
		storePool = store.cfg.StorePool
	}
	rq := &replicateQueue{
		metrics:   makeReplicateQueueMetrics(),
		allocator: allocator,
		storePool: storePool,
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
			needsSpanConfigs:     true,
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

func (rq *replicateQueue) enabled() bool {
	st := rq.store.ClusterSettings()
	return kvserverbase.ReplicateQueueEnabled.Get(&st.SV)
}

func (rq *replicateQueue) shouldQueue(
	ctx context.Context, now hlc.ClockTimestamp, repl *Replica, _ spanconfig.StoreReader,
) (shouldQueue bool, priority float64) {
	if !rq.enabled() {
		return false, 0
	}

	desc, conf := repl.DescAndSpanConfig()
	action, priority := rq.allocator.ComputeAction(ctx, rq.storePool, conf, desc)

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
			rq.storePool,
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
			rq.storePool,
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
			rq.storePool,
			conf,
			voterReplicas,
			repl,
			RangeUsageInfoForRepl(repl),
		) {
		log.KvDistribution.VEventf(ctx, 2, "lease transfer needed, enqueuing")
		return true, 0
	}

	leaseStatus := repl.LeaseStatusAt(ctx, now)
	if !leaseStatus.IsValid() {
		// The range has an invalid lease. If this replica is the raft leader then
		// we'd like it to hold a valid lease. We enqueue it regardless of being a
		// leader or follower, where the leader at the time of processing will
		// succeed.
		log.KvDistribution.VEventf(ctx, 2, "invalid lease, enqueuing")
		return true, 0
	}
	if leaseStatus.OwnedBy(repl.StoreID()) && !repl.hasCorrectLeaseType(leaseStatus.Lease) {
		// This replica holds (or held) an incorrect lease type, switch it to the
		// correct type. Typically when changing kv.expiration_leases_only.enabled.
		log.KvDistribution.VEventf(ctx, 2, "incorrect lease type, enqueueing")
		return true, 0
	}

	return false, 0
}

func (rq *replicateQueue) process(
	ctx context.Context, repl *Replica, confReader spanconfig.StoreReader,
) (processed bool, err error) {
	if !rq.enabled() {
		log.VEventf(ctx, 2, "skipping replication: queue has been disabled")
		return false, nil
	}

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

var _ errors.SafeFormatter = decommissionPurgatoryError{}

func (e decommissionPurgatoryError) SafeFormatError(p errors.Printer) (next error) {
	p.Print(e.error)
	return nil
}

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

		var traceOutput redact.RedactableString
		traceLoggingNeeded := (err != nil || exceededDuration) && log.ExpensiveLogEnabled(ctx, 1)
		if traceLoggingNeeded {
			// If we have tracing spans from execChangeReplicasTxn, filter it from
			// the recording so that we can render the traces to the log without it,
			// as the traces from this span (and its children) are highly verbose.
			rec = filterTracingSpans(sp.GetConfiguredRecording(),
				replicaChangeTxnGetDescOpName, replicaChangeTxnUpdateDescOpName,
			)
			traceOutput = redact.Sprintf("\ntrace:\n%s", rec)
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

// ReplicateQueueChange is a planned change, associated with an allocator
// action. It originates from the replicate queue planner and is used to apply
// an allocation step.
type ReplicateQueueChange struct {
	Action allocatorimpl.AllocatorAction
	// replica is the replica of the range associated with the replicate queue
	// change.
	//
	// TODO(kvoli): This will need to change to an interface replica such as
	// CandidateReplica for simulation to work.
	replica *Replica
	// Op is a planned operation associated with a replica.
	Op AllocationOp
}

// applyChange applies a range allocation change. It is responsible only for
// application and returns an error if unsuccessful.
//
// TODO(kvoli): Currently applyChange is only called by the replicate queue. It
// is desirable to funnel all allocation changes via one function. Move this
// application phase onto a separate struct that will be used by both the
// replicate queue and the store rebalancer and specifically for operations
// rather than changes.
func (rq *replicateQueue) applyChange(ctx context.Context, change ReplicateQueueChange) error {
	var err error
	switch op := change.Op.(type) {
	case AllocationNoop:
		// Nothing to do.
	case AllocationFinalizeAtomicReplicationOp:
		err = rq.finalizeAtomicReplication(ctx, change.replica)
	case AllocationTransferLeaseOp:
		err = rq.TransferLease(ctx, change.replica, op.source, op.target, op.usage)
	case AllocationChangeReplicasOp:
		err = rq.changeReplicas(
			ctx,
			change.replica,
			op.chgs,
			change.replica.Desc(),
			op.priority,
			op.allocatorPriority,
			op.reason,
			op.details,
		)
	default:
		panic(fmt.Sprintf("Unknown operation %+v, unable to apply replicate queue change", op))
	}

	return err
}

// ShouldRequeue determines whether a replica should be requeued into the
// replicate queue, using the planned change and error returned from either
// application or planning.
func (rq *replicateQueue) ShouldRequeue(ctx context.Context, change ReplicateQueueChange) bool {
	var requeue bool

	if _, ok := change.Op.(AllocationNoop); ok {
		// Don't requeue on a noop, as the replica had nothing to do the first
		// time around.
		requeue = false

	} else if change.Action == allocatorimpl.AllocatorConsiderRebalance {
		// Don't requeue after a successful rebalance operation.
		requeue = false

	} else if change.Op.lhBeingRemoved() {
		// Don't requeue if the leaseholder was removed as a voter or the range
		// lease was transferred away.
		requeue = false

	} else {
		// Otherwise, requeue to see if there is more work to do. As the
		// operation succeeded and was planned for a repair action i.e. not
		// rebalancing.
		requeue = true
	}

	return requeue
}

func (rq *replicateQueue) processOneChange(
	ctx context.Context,
	repl *Replica,
	canTransferLeaseFrom func(ctx context.Context, repl *Replica) bool,
	scatter, dryRun bool,
) (requeue bool, _ error) {
	change, err := rq.PlanOneChange(ctx, repl, canTransferLeaseFrom, scatter)
	// When there is an error planning a change, return the error immediately
	// and do not requeue. It is unlikely that the range or storepool state
	// will change quickly enough in order to not get the same error and
	// outcome.
	if err != nil {
		// If there was a change during the planning process, possibly due to
		// allocator errors finding a target, we should report this as a failure
		// for the associated allocator action metric if we are not in dry run.
		if !dryRun {
			rq.metrics.trackErrorByAllocatorAction(ctx, change.Action)
		}

		// Annotate the planning error if it is associated with a decommission
		// allocator action so that the replica will be put into purgatory
		// rather than waiting for the next scanner cycle. This is also done
		// for application failures below.
		return false, maybeAnnotateDecommissionErr(err, change.Action)
	}

	// There is nothing further to do during a dry run.
	if dryRun {
		return false, nil
	}

	// Track the metrics generated during planning. These are not updated
	// directly during planning to avoid pushing the dryRun flag into every
	// function.
	change.Op.trackPlanningMetrics()

	// Apply the change generated by PlanOneChange. This call will block until
	// the change has either been applied successfully or failed.
	err = rq.applyChange(ctx, change)

	// TODO(kvoli): The results tracking currently ignore which operation was
	// planned and instead adopts the allocator action to update the metrics.
	// In cases where the action was AllocatorRemoveX, yet a lease transfer
	// operation was returned, it will treat it as a successful or failed
	// AllocatorRemoveX. This is despite no operation to remove a replica
	// having occurred on this store. This should be updated to accurately
	// reflect which operation was applied.
	rq.metrics.trackResultByAllocatorAction(ctx, change.Action, err)

	if err != nil {
		return false, maybeAnnotateDecommissionErr(err, change.Action)
	}

	// Update the local storepool state to reflect the successful application
	// of the change.
	change.Op.applyImpact(rq.storePool)

	// Requeue the replica if it meets the criteria in ShouldRequeue.
	return rq.ShouldRequeue(ctx, change), nil
}

// PlanOneChange calls the allocator to determine an action to be taken upon a
// range. The fn then calls back into the allocator to get the changes
// necessary to act upon the action and returns them as a ReplicateQueueChange.
func (rq *replicateQueue) PlanOneChange(
	ctx context.Context,
	repl *Replica,
	canTransferLeaseFrom func(ctx context.Context, repl *Replica) bool,
	scatter bool,
) (change ReplicateQueueChange, _ error) {
	// Initially set the change to be a no-op, it is then modified below if a
	// step may be taken for this replica.
	change = ReplicateQueueChange{
		Action:  allocatorimpl.AllocatorNoop,
		Op:      AllocationNoop{},
		replica: repl,
	}

	// Check lease and destroy status here. The queue does this higher up already, but
	// adminScatter (and potential other future callers) also call this method and don't
	// perform this check, which could lead to infinite loops.
	if _, err := repl.IsDestroyed(); err != nil {
		return change, err
	}

	// Ensure ranges have a lease (returning NLHE if someone else has it), and
	// switch the lease type if necessary (e.g. due to
	// kv.expiration_leases_only.enabled).
	//
	// TODO(kvoli): This check should fail if not the leaseholder. In the case
	// where we want to use the replicate queue to acquire leases, this should
	// occur before planning or as a result. In order to return this in planning,
	// it is necessary to simulate the prior change having succeeded to then plan
	// this lease transfer.
	//
	// TODO(erikgrinaker): We shouldn't overload the replicate queue to also be
	// responsible for lease maintenance, but it'll do for now. See:
	// https://github.com/cockroachdb/cockroach/issues/98433
	leaseStatus, pErr := repl.redirectOnOrAcquireLease(ctx)
	if pErr != nil {
		return change, pErr.GoError()
	}
	pErr = repl.maybeSwitchLeaseType(ctx, leaseStatus)
	if pErr != nil {
		return change, pErr.GoError()
	}

	// TODO(aayush): The fact that we're calling `repl.DescAndZone()` here once to
	// pass to `ComputeAction()` to use for deciding which action to take to
	// repair a range, and then calling it again inside methods like
	// `addOrReplace{Non}Voters()` or `remove{Dead,Decommissioning}` to execute
	// upon that decision is a bit unfortunate. It means that we could
	// successfully execute a decision that was based on the state of a stale
	// range descriptor.
	desc, conf := repl.DescAndSpanConfig()

	voterReplicas, nonVoterReplicas,
		liveVoterReplicas, deadVoterReplicas,
		liveNonVoterReplicas, deadNonVoterReplicas := allocatorimpl.LiveAndDeadVoterAndNonVoterReplicas(rq.storePool, desc)

	// NB: the replication layer ensures that the below operations don't cause
	// unavailability; see:
	_ = execChangeReplicasTxn

	action, allocatorPrio := rq.allocator.ComputeAction(ctx, rq.storePool, conf, desc)
	log.KvDistribution.VEventf(ctx, 1, "next replica action: %s", action)

	var err error
	var op AllocationOp
	removeIdx := -1
	nothingToDo := false
	switch action {
	case allocatorimpl.AllocatorNoop, allocatorimpl.AllocatorRangeUnavailable:
		// We're either missing liveness information or the range is known to have
		// lost quorum. Either way, it's not a good idea to make changes right now.
		// Let the scanner requeue it again later.

	// Add replicas, replace dead replicas, or replace decommissioning replicas.
	case allocatorimpl.AllocatorAddVoter, allocatorimpl.AllocatorAddNonVoter,
		allocatorimpl.AllocatorReplaceDeadVoter, allocatorimpl.AllocatorReplaceDeadNonVoter,
		allocatorimpl.AllocatorReplaceDecommissioningVoter, allocatorimpl.AllocatorReplaceDecommissioningNonVoter:
		var existing, remainingLiveVoters, remainingLiveNonVoters []roachpb.ReplicaDescriptor

		existing, remainingLiveVoters, remainingLiveNonVoters, removeIdx, nothingToDo, err =
			allocatorimpl.DetermineReplicaToReplaceAndFilter(
				rq.storePool,
				action,
				voterReplicas, nonVoterReplicas,
				liveVoterReplicas, deadVoterReplicas,
				liveNonVoterReplicas, deadNonVoterReplicas,
			)
		if nothingToDo || err != nil {
			// Nothing to do.
			break
		}

		switch action.TargetReplicaType() {
		case allocatorimpl.VoterTarget:
			op, err = rq.addOrReplaceVoters(
				ctx, repl, existing, remainingLiveVoters, remainingLiveNonVoters,
				removeIdx, action.ReplicaStatus(), allocatorPrio,
			)
		case allocatorimpl.NonVoterTarget:
			op, err = rq.addOrReplaceNonVoters(
				ctx, repl, existing, remainingLiveVoters, remainingLiveNonVoters,
				removeIdx, action.ReplicaStatus(), allocatorPrio,
			)
		default:
			panic(fmt.Sprintf("unsupported targetReplicaType: %v", action.TargetReplicaType()))
		}

	// Remove replicas.
	case allocatorimpl.AllocatorRemoveVoter:
		op, err = rq.removeVoter(ctx, repl, voterReplicas, nonVoterReplicas)
	case allocatorimpl.AllocatorRemoveNonVoter:
		op, err = rq.removeNonVoter(ctx, repl, voterReplicas, nonVoterReplicas)

	// Remove decommissioning replicas.
	//
	// NB: these two paths will only be hit when the range is over-replicated and
	// has decommissioning replicas; in the common case we'll hit
	// AllocatorReplaceDecommissioning{Non}Voter above.
	case allocatorimpl.AllocatorRemoveDecommissioningVoter:
		op, err = rq.removeDecommissioning(ctx, repl, allocatorimpl.VoterTarget)
	case allocatorimpl.AllocatorRemoveDecommissioningNonVoter:
		op, err = rq.removeDecommissioning(ctx, repl, allocatorimpl.NonVoterTarget)

	// Remove dead replicas.
	//
	// NB: these two paths below will only be hit when the range is
	// over-replicated and has dead replicas; in the common case we'll hit
	// AllocatorReplaceDead{Non}Voter above.
	case allocatorimpl.AllocatorRemoveDeadVoter:
		op, err = rq.removeDead(ctx, repl, deadVoterReplicas, allocatorimpl.VoterTarget)
	case allocatorimpl.AllocatorRemoveDeadNonVoter:
		op, err = rq.removeDead(ctx, repl, deadNonVoterReplicas, allocatorimpl.NonVoterTarget)

	// Rebalance replicas.
	//
	// NB: Rebalacing attempts to balance replica counts among stores of
	// equivalent localities. This action is returned by default for EVERY
	// replica, when no other action applies. However, it has another important
	// role in satisfying the zone constraints appled to a range, by performing
	// swaps when the voter and total replica counts are correct in aggregate,
	// yet incorrect per locality. See #90110.
	case allocatorimpl.AllocatorConsiderRebalance:
		op, err = rq.considerRebalance(
			ctx,
			repl,
			voterReplicas,
			nonVoterReplicas,
			allocatorPrio,
			canTransferLeaseFrom,
			scatter,
		)
	case allocatorimpl.AllocatorFinalizeAtomicReplicationChange, allocatorimpl.AllocatorRemoveLearner:
		op = AllocationFinalizeAtomicReplicationOp{}
	default:
		err = errors.Errorf("unknown allocator action %v", action)
	}

	// If an operation was found, then wrap it in the change being returned. If
	// no operation was found for the allocator action then return a noop.
	if op == nil {
		op = AllocationNoop{}
	}
	change = ReplicateQueueChange{
		Action:  action,
		replica: repl,
		Op:      op,
	}
	return change, err
}

func maybeAnnotateDecommissionErr(err error, action allocatorimpl.AllocatorAction) error {
	if err != nil && isDecommissionAction(action) {
		err = decommissionPurgatoryError{err}
	}
	return err
}

func isDecommissionAction(action allocatorimpl.AllocatorAction) bool {
	return action == allocatorimpl.AllocatorRemoveDecommissioningVoter ||
		action == allocatorimpl.AllocatorRemoveDecommissioningNonVoter ||
		action == allocatorimpl.AllocatorReplaceDecommissioningVoter ||
		action == allocatorimpl.AllocatorReplaceDecommissioningNonVoter
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
	existingVoters []roachpb.ReplicaDescriptor,
	remainingLiveVoters, remainingLiveNonVoters []roachpb.ReplicaDescriptor,
	removeIdx int,
	replicaStatus allocatorimpl.ReplicaStatus,
	allocatorPriority float64,
) (op AllocationOp, _ error) {
	effects := effectBuilder{}
	desc, conf := repl.DescAndSpanConfig()
	var replacing *roachpb.ReplicaDescriptor
	if removeIdx >= 0 {
		replacing = &existingVoters[removeIdx]
	}

	// The allocator should not try to re-add this replica since there is a reason
	// we're removing it (i.e. dead or decommissioning). If we left the replica in
	// the slice, the allocator would not be guaranteed to pick a replica that
	// fills the gap removeRepl leaves once it's gone.
	newVoter, details, err := rq.allocator.AllocateVoter(ctx, rq.storePool, conf, remainingLiveVoters, remainingLiveNonVoters, replacing, replicaStatus)
	if err != nil {
		return nil, err
	}

	isReplace := removeIdx >= 0
	if isReplace && newVoter.StoreID == existingVoters[removeIdx].StoreID {
		return nil, errors.AssertionFailedf("allocator suggested to replace replica on s%d with itself", newVoter.StoreID)
	}

	// We only want to up-replicate if there are suitable allocation targets such
	// that, either the replication goal is met, or it is possible to get to the next
	// odd number of replicas. A consensus group of size 2n has worse failure
	// tolerance properties than a group of size 2n - 1 because it has a larger
	// quorum. For example, up-replicating from 1 to 2 replicas only makes sense
	// if it is possible to be able to go to 3 replicas.
	if err := rq.allocator.CheckAvoidsFragileQuorum(ctx, rq.storePool, conf,
		existingVoters, remainingLiveNonVoters,
		replicaStatus, allocatorimpl.VoterTarget, newVoter, isReplace); err != nil {
		// It does not seem possible to go to the next odd replica state. Note
		// that AllocateVoter returns an allocatorError (a PurgatoryError)
		// when purgatory is requested.
		return nil, errors.Wrap(err, "avoid up-replicating to fragile quorum")
	}

	// Figure out whether we should be promoting an existing non-voting replica to
	// a voting replica or if we ought to be adding a voter afresh.
	var ops []kvpb.ReplicationChange
	replDesc, found := desc.GetReplicaDescriptor(newVoter.StoreID)
	if found {
		if replDesc.Type != roachpb.NON_VOTER {
			return nil, errors.AssertionFailedf("allocation target %s for a voter"+
				" already has an unexpected replica: %s", newVoter, replDesc)
		}
		// If the allocation target has a non-voter already, we will promote it to a
		// voter.
		effects = effects.add(func() {
			rq.metrics.NonVoterPromotionsCount.Inc(1)
		})
		ops = kvpb.ReplicationChangesForPromotion(newVoter)
	} else {
		effects = effects.add(func() {
			rq.metrics.trackAddReplicaCount(allocatorimpl.VoterTarget)
		})
		ops = kvpb.MakeReplicationChanges(roachpb.ADD_VOTER, newVoter)
	}
	if !isReplace {
		log.KvDistribution.Infof(ctx, "adding voter %+v: %s",
			newVoter, rangeRaftProgress(repl.RaftStatus(), existingVoters))
	} else {
		effects = effects.add(func() {
			rq.metrics.trackRemoveMetric(allocatorimpl.VoterTarget, replicaStatus)
		})

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
			kvpb.MakeReplicationChanges(roachpb.REMOVE_VOTER, roachpb.ReplicationTarget{
				StoreID: removeVoter.StoreID,
				NodeID:  removeVoter.NodeID,
			})...)
	}

	op = AllocationChangeReplicasOp{
		lhStore:           repl.StoreID(),
		sideEffects:       effects.f(),
		usage:             RangeUsageInfoForRepl(repl),
		chgs:              ops,
		priority:          kvserverpb.SnapshotRequest_RECOVERY,
		allocatorPriority: allocatorPriority,
		reason:            kvserverpb.ReasonRangeUnderReplicated,
		details:           details,
	}

	return op, nil
}

// addOrReplaceNonVoters adds a non-voting replica to `repl`s range.
func (rq *replicateQueue) addOrReplaceNonVoters(
	ctx context.Context,
	repl *Replica,
	existingNonVoters []roachpb.ReplicaDescriptor,
	liveVoterReplicas, liveNonVoterReplicas []roachpb.ReplicaDescriptor,
	removeIdx int,
	replicaStatus allocatorimpl.ReplicaStatus,
	allocatorPrio float64,
) (op AllocationOp, _ error) {
	effects := effectBuilder{}
	conf := repl.SpanConfig()
	var replacing *roachpb.ReplicaDescriptor
	if removeIdx >= 0 {
		replacing = &existingNonVoters[removeIdx]
	}

	newNonVoter, details, err := rq.allocator.AllocateNonVoter(ctx, rq.storePool, conf, liveVoterReplicas, liveNonVoterReplicas, replacing, replicaStatus)
	if err != nil {
		return nil, err
	}

	effects = effects.add(func() {
		rq.metrics.trackAddReplicaCount(allocatorimpl.NonVoterTarget)
	})

	ops := kvpb.MakeReplicationChanges(roachpb.ADD_NON_VOTER, newNonVoter)
	if removeIdx < 0 {
		log.KvDistribution.Infof(ctx, "adding non-voter %+v: %s",
			newNonVoter, rangeRaftProgress(repl.RaftStatus(), existingNonVoters))
	} else {
		effects = effects.add(func() {
			rq.metrics.trackRemoveMetric(allocatorimpl.NonVoterTarget, replicaStatus)
		})
		removeNonVoter := existingNonVoters[removeIdx]
		log.KvDistribution.Infof(ctx, "replacing non-voter %s with %+v: %s",
			removeNonVoter, newNonVoter, rangeRaftProgress(repl.RaftStatus(), existingNonVoters))
		ops = append(ops,
			kvpb.MakeReplicationChanges(roachpb.REMOVE_NON_VOTER, roachpb.ReplicationTarget{
				StoreID: removeNonVoter.StoreID,
				NodeID:  removeNonVoter.NodeID,
			})...)
	}

	op = AllocationChangeReplicasOp{
		lhStore:           repl.StoreID(),
		sideEffects:       effects.f(),
		usage:             RangeUsageInfoForRepl(repl),
		chgs:              ops,
		priority:          kvserverpb.SnapshotRequest_RECOVERY,
		allocatorPriority: allocatorPrio,
		reason:            kvserverpb.ReasonRangeUnderReplicated,
		details:           details,
	}
	return op, nil
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
	timeout := 5 * time.Second

	var candidates []roachpb.ReplicaDescriptor
	deadline := timeutil.Now().Add(timeout)
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
		rq.storePool,
		zone,
		candidates,
		existingVoters,
		existingNonVoters,
		rq.allocator.ScorerOptions(ctx),
	)
}

// maybeTransferLeaseAwayTarget is called whenever a replica on a given store
// is slated for removal. If the store corresponds to the store of the caller
// (which is very likely to be the leaseholder), then this removal would fail.
// Instead, this method will attempt to transfer the lease away, and returns
// true to indicate to the caller that it should not pursue the current
// replication change further because it is no longer the leaseholder. When the
// returned bool is false, it should continue. On error, the caller should also
// stop. If canTransferLeaseFrom is non-nil, it is consulted and an error is
// returned if it returns false.
func (rq *replicateQueue) maybeTransferLeaseAwayTarget(
	ctx context.Context,
	repl *Replica,
	removeStoreID roachpb.StoreID,
	canTransferLeaseFrom func(ctx context.Context, repl *Replica) bool,
) (op AllocationOp, _ error) {
	if removeStoreID != repl.store.StoreID() {
		return nil, nil
	}
	if canTransferLeaseFrom != nil && !canTransferLeaseFrom(ctx, repl) {
		return nil, errors.Errorf("cannot transfer lease")
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
	target := rq.allocator.TransferLeaseTarget(
		ctx,
		rq.storePool,
		conf,
		desc.Replicas().VoterDescriptors(),
		repl,
		RangeUsageInfoForRepl(repl),
		false, /* forceDecisionWithoutStats */
		allocator.TransferLeaseOptions{
			Goal: allocator.LeaseCountConvergence,
			// NB: This option means that the allocator is asked to not consider the
			// current replica in its set of potential candidates.
			ExcludeLeaseRepl: true,
		},
	)

	if target == (roachpb.ReplicaDescriptor{}) {
		return nil, nil
	}
	log.KvDistribution.Infof(ctx, "transferring lease to s%d", target.StoreID)

	op = AllocationTransferLeaseOp{
		source:             repl.StoreID(),
		target:             target.StoreID,
		usage:              RangeUsageInfoForRepl(repl),
		bypassSafetyChecks: false,
	}

	return op, nil
}

func (rq *replicateQueue) removeVoter(
	ctx context.Context, repl *Replica, existingVoters, existingNonVoters []roachpb.ReplicaDescriptor,
) (op AllocationOp, _ error) {
	effects := effectBuilder{}
	removeVoter, details, err := rq.findRemoveVoter(ctx, repl, existingVoters, existingNonVoters)
	if err != nil {
		return nil, err
	}

	transferOp, err := rq.maybeTransferLeaseAwayTarget(
		ctx, repl, removeVoter.StoreID, nil /* canTransferLeaseFrom */)
	if err != nil {
		return nil, err
	}
	// We found a lease transfer opportunity, exit early.
	if transferOp != nil {
		return transferOp, nil
	}
	effects = effects.add(func() {
		rq.metrics.trackRemoveMetric(allocatorimpl.VoterTarget, allocatorimpl.Alive)
	})

	// Remove a replica.
	log.KvDistribution.Infof(ctx, "removing voting replica %+v due to over-replication: %s",
		removeVoter, rangeRaftProgress(repl.RaftStatus(), existingVoters))
	// TODO(aayush): Directly removing the voter here is a bit of a missed
	// opportunity since we could potentially be 1 non-voter short and the
	// `target` could be a valid store for a non-voter. In such a scenario, we
	// could save a bunch of work by just performing an atomic demotion of a
	// voter.
	op = AllocationChangeReplicasOp{
		lhStore:           repl.StoreID(),
		sideEffects:       effects.f(),
		usage:             RangeUsageInfoForRepl(repl),
		chgs:              kvpb.MakeReplicationChanges(roachpb.REMOVE_VOTER, removeVoter),
		priority:          kvserverpb.SnapshotRequest_UNKNOWN, // unused
		allocatorPriority: 0.0,                                // unused
		reason:            kvserverpb.ReasonRangeOverReplicated,
		details:           details,
	}
	return op, nil
}

func (rq *replicateQueue) removeNonVoter(
	ctx context.Context, repl *Replica, existingVoters, existingNonVoters []roachpb.ReplicaDescriptor,
) (op AllocationOp, _ error) {
	effects := effectBuilder{}

	_, conf := repl.DescAndSpanConfig()
	removeNonVoter, details, err := rq.allocator.RemoveNonVoter(
		ctx,
		rq.storePool,
		conf,
		existingNonVoters,
		existingVoters,
		existingNonVoters,
		rq.allocator.ScorerOptions(ctx),
	)
	if err != nil {
		return nil, err
	}
	effects = effects.add(func() {
		rq.metrics.trackRemoveMetric(allocatorimpl.NonVoterTarget, allocatorimpl.Alive)
	})

	log.KvDistribution.Infof(ctx, "removing non-voting replica %+v due to over-replication: %s",
		removeNonVoter, rangeRaftProgress(repl.RaftStatus(), existingVoters))
	target := roachpb.ReplicationTarget{
		NodeID:  removeNonVoter.NodeID,
		StoreID: removeNonVoter.StoreID,
	}

	op = AllocationChangeReplicasOp{
		lhStore:           repl.StoreID(),
		sideEffects:       effects.f(),
		usage:             RangeUsageInfoForRepl(repl),
		chgs:              kvpb.MakeReplicationChanges(roachpb.REMOVE_NON_VOTER, target),
		priority:          kvserverpb.SnapshotRequest_UNKNOWN, // unused
		allocatorPriority: 0.0,                                // unused
		reason:            kvserverpb.ReasonRangeOverReplicated,
		details:           details,
	}
	return op, nil
}

func (rq *replicateQueue) removeDecommissioning(
	ctx context.Context, repl *Replica, targetType allocatorimpl.TargetReplicaType,
) (op AllocationOp, _ error) {
	desc := repl.Desc()
	effects := effectBuilder{}
	var decommissioningReplicas []roachpb.ReplicaDescriptor
	switch targetType {
	case allocatorimpl.VoterTarget:
		decommissioningReplicas = rq.storePool.DecommissioningReplicas(
			desc.Replicas().VoterDescriptors(),
		)
	case allocatorimpl.NonVoterTarget:
		decommissioningReplicas = rq.storePool.DecommissioningReplicas(
			desc.Replicas().NonVoterDescriptors(),
		)
	default:
		panic(fmt.Sprintf("unknown targetReplicaType: %s", targetType))
	}

	if len(decommissioningReplicas) == 0 {
		return nil, errors.AssertionFailedf("range of %[1]ss %[2]s was identified as having decommissioning %[1]ss, "+
			"but no decommissioning %[1]ss were found", targetType, repl)
	}
	decommissioningReplica := decommissioningReplicas[0]

	transferOp, err := rq.maybeTransferLeaseAwayTarget(
		ctx, repl, decommissioningReplica.StoreID, nil /* canTransferLeaseFrom */)
	if err != nil {
		return nil, err
	}
	// We found a lease transfer opportunity, exit early.
	if transferOp != nil {
		return transferOp, nil
	}

	effects = effects.add(func() {
		rq.metrics.trackRemoveMetric(targetType, allocatorimpl.Decommissioning)
	})

	log.KvDistribution.Infof(ctx, "removing decommissioning %s %+v from store", targetType, decommissioningReplica)
	target := roachpb.ReplicationTarget{
		NodeID:  decommissioningReplica.NodeID,
		StoreID: decommissioningReplica.StoreID,
	}

	op = AllocationChangeReplicasOp{
		lhStore:           repl.StoreID(),
		sideEffects:       effects.f(),
		usage:             RangeUsageInfoForRepl(repl),
		chgs:              kvpb.MakeReplicationChanges(targetType.RemoveChangeType(), target),
		priority:          kvserverpb.SnapshotRequest_UNKNOWN, // unused
		allocatorPriority: 0.0,                                // unused
		reason:            kvserverpb.ReasonStoreDecommissioning,
		details:           "",
	}
	return op, nil
}

func (rq *replicateQueue) removeDead(
	ctx context.Context,
	repl *Replica,
	deadReplicas []roachpb.ReplicaDescriptor,
	targetType allocatorimpl.TargetReplicaType,
) (op AllocationOp, _ error) {
	effects := effectBuilder{}
	if len(deadReplicas) == 0 {
		return nil, errors.AssertionFailedf(
			"range of %[1]s %[2]s was identified as having dead %[1]ss, but no dead %[1]ss were found",
			targetType,
			repl,
		)
	}
	deadReplica := deadReplicas[0]

	effects = effects.add(func() {
		rq.metrics.trackRemoveMetric(targetType, allocatorimpl.Dead)
	})

	log.KvDistribution.Infof(ctx, "removing dead %s %+v from store", targetType, deadReplica)
	target := roachpb.ReplicationTarget{
		NodeID:  deadReplica.NodeID,
		StoreID: deadReplica.StoreID,
	}

	// NB: When removing a dead voter, we don't check whether to transfer the
	// lease away because if the removal target is dead, it's not the voter being
	// removed (and if for some reason that happens, the removal is simply going
	// to fail).
	op = AllocationChangeReplicasOp{
		lhStore:           repl.StoreID(),
		sideEffects:       effects.f(),
		usage:             RangeUsageInfoForRepl(repl),
		chgs:              kvpb.MakeReplicationChanges(targetType.RemoveChangeType(), target),
		priority:          kvserverpb.SnapshotRequest_UNKNOWN, // unused
		allocatorPriority: 0.0,                                // unused
		reason:            kvserverpb.ReasonStoreDead,
		details:           "",
	}

	return op, nil
}

func (rq *replicateQueue) considerRebalance(
	ctx context.Context,
	repl *Replica,
	existingVoters, existingNonVoters []roachpb.ReplicaDescriptor,
	allocatorPrio float64,
	canTransferLeaseFrom func(ctx context.Context, repl *Replica) bool,
	scatter bool,
) (op AllocationOp, _ error) {
	// When replicate queue rebalancing is not enabled return early.
	if rq.store.TestingKnobs().DisableReplicaRebalancing {
		return nil, nil
	}
	effects := effectBuilder{}

	desc, conf := repl.DescAndSpanConfig()
	rebalanceTargetType := allocatorimpl.VoterTarget

	scorerOpts := allocatorimpl.ScorerOptions(rq.allocator.ScorerOptions(ctx))
	if scatter {
		scorerOpts = rq.allocator.ScorerOptionsForScatter(ctx)
	}
	rangeUsageInfo := RangeUsageInfoForRepl(repl)
	addTarget, removeTarget, details, ok := rq.allocator.RebalanceVoter(
		ctx,
		rq.storePool,
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
		log.KvDistribution.VInfof(ctx, 2, "no suitable rebalance target for voters")
		addTarget, removeTarget, details, ok = rq.allocator.RebalanceNonVoter(
			ctx,
			rq.storePool,
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

	// Determine whether we can remove the leaseholder without first
	// transferring the lease away.
	lhRemovalAllowed := addTarget != (roachpb.ReplicationTarget{})

	if !ok {
		log.KvDistribution.VInfof(ctx, 2, "no suitable rebalance target for non-voters")
	} else if !lhRemovalAllowed {
		if transferOp, err := rq.maybeTransferLeaseAwayTarget(
			ctx, repl, removeTarget.StoreID, canTransferLeaseFrom,
		); err != nil {
			// No transfer possible.
			ok = false
			log.KvDistribution.Infof(ctx, "want to remove self, but failed to find lease transfer target: %s", err)
		} else if transferOp != nil {
			// We found a lease transfer opportunity, exit early.
			return transferOp, nil
		}
	}

	// No rebalance target was found, check whether we are able and should
	// transfer the lease away to another store.
	if !ok {
		if !canTransferLeaseFrom(ctx, repl) {
			return nil, nil
		}
		return rq.shedLeaseTarget(
			ctx,
			repl,
			desc,
			conf,
			allocator.TransferLeaseOptions{
				Goal:                   allocator.FollowTheWorkload,
				ExcludeLeaseRepl:       false,
				CheckCandidateFullness: true,
			},
		), nil

	}

	// If we have a valid rebalance action (ok == true) and we haven't
	// transferred our lease away, find the rebalance changes and return them
	// in an operation.
	chgs, performingSwap, err := replicationChangesForRebalance(ctx, desc, len(existingVoters), addTarget,
		removeTarget, rebalanceTargetType)
	if err != nil {
		return nil, err
	}

	effects = effects.add(func() {
		rq.metrics.trackRebalanceReplicaCount(rebalanceTargetType)
		if performingSwap {
			rq.metrics.VoterDemotionsCount.Inc(1)
			rq.metrics.NonVoterPromotionsCount.Inc(1)
		}
	})

	log.KvDistribution.Infof(ctx,
		"rebalancing %s %+v to %+v: %s",
		rebalanceTargetType,
		removeTarget,
		addTarget,
		rangeRaftProgress(repl.RaftStatus(), existingVoters))

	op = AllocationChangeReplicasOp{
		lhStore:           repl.StoreID(),
		sideEffects:       effects.f(),
		usage:             RangeUsageInfoForRepl(repl),
		chgs:              chgs,
		priority:          kvserverpb.SnapshotRequest_REBALANCE,
		allocatorPriority: allocatorPrio,
		reason:            kvserverpb.ReasonRebalance,
		details:           details,
	}
	return op, nil
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
) (chgs []kvpb.ReplicationChange, performingSwap bool, err error) {
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
		chgs = []kvpb.ReplicationChange{
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
			promo := kvpb.ReplicationChangesForPromotion(addTarget)
			demo := kvpb.ReplicationChangesForDemotion(removeTarget)
			chgs = append(promo, demo...)
			performingSwap = true
		} else if found {
			return nil, false, errors.AssertionFailedf(
				"programming error:"+
					" store being rebalanced to(%s) already has a voting replica", addTarget.StoreID,
			)
		} else {
			// We have a replica to remove and one we can add, so let's swap them out.
			chgs = []kvpb.ReplicationChange{
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
		chgs = []kvpb.ReplicationChange{
			{ChangeType: roachpb.ADD_NON_VOTER, Target: addTarget},
			{ChangeType: roachpb.REMOVE_NON_VOTER, Target: removeTarget},
		}
	}
	return chgs, performingSwap, nil
}

// shedLeaseTarget takes in a leaseholder replica, looks for a target for
// transferring the lease and, if a suitable target is found (e.g. alive, not
// draining), returns an allocation op to transfer the lease away.
func (rq *replicateQueue) shedLeaseTarget(
	ctx context.Context,
	repl *Replica,
	desc *roachpb.RangeDescriptor,
	conf roachpb.SpanConfig,
	opts allocator.TransferLeaseOptions,
) (op AllocationOp) {
	usage := RangeUsageInfoForRepl(repl)
	// Learner replicas aren't allowed to become the leaseholder or raft leader,
	// so only consider the `VoterDescriptors` replicas.
	target := rq.allocator.TransferLeaseTarget(
		ctx,
		rq.storePool,
		conf,
		desc.Replicas().VoterDescriptors(),
		repl,
		usage,
		false, /* forceDecisionWithoutStats */
		opts,
	)
	if target == (roachpb.ReplicaDescriptor{}) {
		return nil
	}

	op = AllocationTransferLeaseOp{
		source:             repl.StoreID(),
		target:             target.StoreID,
		usage:              usage,
		bypassSafetyChecks: false,
	}
	return op
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
		rq.storePool,
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
//
// TODO(kvoli): Evaluate whether we want to keep this. It would be nice to move
// all application methods off of the replicate queue into somewhere neutral.
// This synchronous method won't work easily with simulation.
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

func (rq *replicateQueue) finalizeAtomicReplication(ctx context.Context, repl *Replica) error {
	var learnersRemoved int64
	_, learnersRemoved, err := repl.maybeLeaveAtomicChangeReplicasAndRemoveLearners(
		ctx, repl.Desc(),
	)
	if err == nil {
		rq.metrics.RemoveLearnerReplicaCount.Inc(learnersRemoved)
	}
	return err
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

	rq.storePool.UpdateLocalStoresAfterLeaseTransfer(source, target, rangeUsageInfo)
	rq.lastLeaseTransfer.Store(timeutil.Now())
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
	chgs kvpb.ReplicationChanges,
	desc *roachpb.RangeDescriptor,
	priority kvserverpb.SnapshotRequest_Priority,
	allocatorPriority float64,
	reason kvserverpb.RangeLogEventReason,
	details string,
) error {
	// NB: this calls the impl rather than ChangeReplicas because
	// the latter traps tests that try to call it while the replication
	// queue is active.
	_, err := repl.changeReplicasImpl(
		ctx, desc, priority, kvserverpb.SnapshotRequest_REPLICATE_QUEUE, allocatorPriority, reason,
		details, chgs,
	)
	return err
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
// reasonable when we only consider the leaseholder.
func RangeUsageInfoForRepl(repl *Replica) allocator.RangeUsageInfo {
	loadStats := repl.LoadStats()
	localityInfo := repl.loadStats.RequestLocalityInfo()
	return allocator.RangeUsageInfo{
		LogicalBytes:             repl.GetMVCCStats().Total(),
		QueriesPerSecond:         loadStats.QueriesPerSecond,
		WritesPerSecond:          loadStats.WriteKeysPerSecond,
		RaftCPUNanosPerSecond:    loadStats.RaftCPUNanosPerSecond,
		RequestCPUNanosPerSecond: loadStats.RequestCPUNanosPerSecond,
		RequestLocality: &allocator.RangeRequestLocalityInfo{
			Counts:   localityInfo.LocalityCounts,
			Duration: localityInfo.Duration,
		},
	}
}
