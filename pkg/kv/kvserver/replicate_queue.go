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
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/allocatorimpl"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/plan"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// The replicate queue processes replicas that required replication changes.
// Replication changes are most commonly required when a range:
// - Is under-replicated or over-replicated or violating its configured
//   constraints/preferences.
// - Has a learner, outside of processing a change.
// - Has a replica on a decommissioning store.
// - Has a lease on a draining store.
// - Has a replica on a store which is underfull/overfull w.r.t the mean replica
//   count.
//
// The flow of a replica within the replicate queue:
// (1) rq.shouldQueue(..) determines if a replica should be added to the queue
//     during replica scanning. The replica is enqueued at a priority proportional
//     to the severity of the issue requiring a replication change. See
//     allocatorimpl.AllocatorAction.Priority().
// (2) rq.process(..) is called when the queue is ready to process another
//     replica. By default, the replicate queue will only process at most one
//     replica at a time.
// (3) rq.processOneChange(..) called by (2), processes a single replication
//     change for the current replica.
// (4) rq.preProcessCheck(..) called by (3), ensures that the replica can be
//     processed. This checks whether the replica is destroyed, has the correct
//     lease type and holds a valid lease.
// (5) planner.PlanOneChange(..) called by (3), uses allocator to determine
//     necessary replication changes. This function is separate to the
//     replicate queue and stateless. The majority of the replication logic
//     lives within this function and.
// (6) rq.applyChange(..) called by (3), actually performs snapshot and
//     replication changes returned from (5). These changes are applied
//     synchronously.

const (
	// replicateQueuePurgatoryCheckInterval is the interval at which replicas in
	// the replicate queue purgatory are re-attempted. Note that these replicas
	// may be re-attempted more frequently by the replicateQueue in case there are
	// gossip updates that might affect allocation decisions.
	replicateQueuePurgatoryCheckInterval = 1 * time.Minute

	// replicateQueueTimerDuration is the duration between replication of queued
	// replicas.
	replicateQueueTimerDuration = 0 // zero duration to process replication greedily

	// replicateQueueLeasePreferencePriority is the priority replicas are
	// enqueued into the replicate queue with when violating lease preferences.
	// This priority is lower than any voter up-replication, yet higher than
	// removal, non-voter addition and rebalancing.
	// See allocatorimpl.AllocatorAction.Priority.
	replicateQueueLeasePreferencePriority = 1001
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

// trackPlanningStats updates the replicate queue metrics with the stats
// returned from replicate planning.
func (metrics *ReplicateQueueMetrics) trackPlanningStats(
	ctx context.Context, stats plan.ReplicateStats,
) {
	// NB: We don't wish to call into Inc unless we need to, check every field
	// and if greater than zero we increment the metric counter.
	if stats.AddReplicaCount > 0 {
		metrics.AddReplicaCount.Inc(stats.AddReplicaCount)
	}
	if stats.AddVoterReplicaCount > 0 {
		metrics.AddVoterReplicaCount.Inc(stats.AddVoterReplicaCount)
	}
	if stats.AddNonVoterReplicaCount > 0 {
		metrics.AddNonVoterReplicaCount.Inc(stats.AddNonVoterReplicaCount)
	}
	if stats.RemoveReplicaCount > 0 {
		metrics.RemoveReplicaCount.Inc(stats.RemoveReplicaCount)
	}
	if stats.RemoveVoterReplicaCount > 0 {
		metrics.RemoveVoterReplicaCount.Inc(stats.RemoveVoterReplicaCount)
	}
	if stats.RemoveNonVoterReplicaCount > 0 {
		metrics.RemoveNonVoterReplicaCount.Inc(stats.RemoveNonVoterReplicaCount)
	}
	if stats.RemoveDeadReplicaCount > 0 {
		metrics.RemoveDeadReplicaCount.Inc(stats.RemoveDeadReplicaCount)
	}
	if stats.RemoveDeadVoterReplicaCount > 0 {
		metrics.RemoveDeadVoterReplicaCount.Inc(stats.RemoveDeadVoterReplicaCount)
	}
	if stats.RemoveDeadNonVoterReplicaCount > 0 {
		metrics.RemoveDeadNonVoterReplicaCount.Inc(stats.RemoveDeadNonVoterReplicaCount)
	}
	if stats.RemoveDecommissioningReplicaCount > 0 {
		metrics.RemoveDecommissioningReplicaCount.Inc(stats.RemoveDecommissioningReplicaCount)
	}
	if stats.RemoveDecommissioningVoterReplicaCount > 0 {
		metrics.RemoveDecommissioningVoterReplicaCount.Inc(stats.RemoveDecommissioningVoterReplicaCount)
	}
	if stats.RemoveDecommissioningNonVoterReplicaCount > 0 {
		metrics.RemoveDecommissioningNonVoterReplicaCount.Inc(stats.RemoveDecommissioningNonVoterReplicaCount)
	}
	if stats.RemoveLearnerReplicaCount > 0 {
		metrics.RemoveLearnerReplicaCount.Inc(stats.RemoveLearnerReplicaCount)
	}
	if stats.RebalanceReplicaCount > 0 {
		metrics.RebalanceReplicaCount.Inc(stats.RebalanceReplicaCount)
	}
	if stats.RebalanceVoterReplicaCount > 0 {
		metrics.RebalanceVoterReplicaCount.Inc(stats.RebalanceVoterReplicaCount)
	}
	if stats.RebalanceNonVoterReplicaCount > 0 {
		metrics.RebalanceNonVoterReplicaCount.Inc(stats.RebalanceNonVoterReplicaCount)
	}
	if stats.NonVoterPromotionsCount > 0 {
		metrics.NonVoterPromotionsCount.Inc(stats.NonVoterPromotionsCount)
	}
	if stats.VoterDemotionsCount > 0 {
		metrics.VoterDemotionsCount.Inc(stats.VoterDemotionsCount)
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
	planner   plan.ReplicationPlanner

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
		metrics: makeReplicateQueueMetrics(),
		planner: plan.NewReplicaPlanner(allocator, storePool,
			store.TestingKnobs().ReplicaPlannerKnobs),
		// TODO(kvoli): Consider removing these from the replicate queue struct.
		allocator: allocator,
		storePool: storePool,
		purgCh:    time.NewTicker(replicateQueuePurgatoryCheckInterval).C,
		updateCh:  make(chan time.Time, 1),
		logTracesThresholdFunc: makeRateLimitedTimeoutFuncByPermittedSlowdown(
			permittedRangeScanSlowdown/2, rebalanceSnapshotRate,
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
			processTimeoutFunc: makeRateLimitedTimeoutFunc(rebalanceSnapshotRate),
			successes:          store.metrics.ReplicateQueueSuccesses,
			failures:           store.metrics.ReplicateQueueFailures,
			pending:            store.metrics.ReplicateQueuePending,
			processingNanos:    store.metrics.ReplicateQueueProcessingNanos,
			purgatory:          store.metrics.ReplicateQueuePurgatory,
			disabledConfig:     kvserverbase.ReplicateQueueEnabled,
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
	ctx context.Context, now hlc.ClockTimestamp, repl *Replica,
) (shouldQueue bool, priority float64) {
	conf, err := repl.SpanConfig()
	if err != nil {
		return false, 0
	}
	desc := repl.Desc()
	return rq.planner.ShouldPlanChange(
		ctx,
		now,
		repl,
		desc,
		conf,
		rq.canTransferLeaseFrom,
	)
}

func (rq *replicateQueue) process(ctx context.Context, repl *Replica) (processed bool, err error) {
	retryOpts := retry.Options{
		InitialBackoff: 50 * time.Millisecond,
		MaxBackoff:     1 * time.Second,
		Multiplier:     2,
		MaxRetries:     5,
	}
	conf, err := repl.SpanConfig()
	if err != nil {
		return false, err
	}
	desc := repl.Desc()
	// Use a retry loop in order to backoff in the case of snapshot errors,
	// usually signaling that a rebalancing reservation could not be made with the
	// selected target.
	for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
		requeue, err := rq.processOneChangeWithTracing(ctx, repl, desc, conf)
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
			if _, err := rq.store.consistencyQueue.process(ctx, repl); err != nil {
				log.KvDistribution.Warningf(ctx, "%v", err)
			}
		}

		if requeue {
			log.KvDistribution.VEventf(ctx, 1, "re-queuing")
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
	ctx context.Context, repl *Replica, desc *roachpb.RangeDescriptor, conf roachpb.SpanConfig,
) (requeue bool, _ error) {
	processStart := timeutil.Now()
	ctx, sp := tracing.EnsureChildSpan(ctx, rq.Tracer, "process replica",
		tracing.WithRecording(tracingpb.RecordingVerbose))
	defer sp.Finish()

	requeue, err := rq.processOneChange(ctx, repl, desc, conf, rq.canTransferLeaseFrom,
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

// applyChange applies a range allocation change. It is responsible only for
// application and returns an error if unsuccessful.
//
// TODO(kvoli): Currently applyChange is only called by the replicate queue. It
// is desirable to funnel all allocation changes via one function. Move this
// application phase onto a separate struct that will be used by both the
// replicate queue and the store rebalancer and specifically for operations
// rather than changes.
func (rq *replicateQueue) applyChange(
	ctx context.Context, change plan.ReplicateChange, replica *Replica,
) error {
	var err error
	switch op := change.Op.(type) {
	case plan.AllocationNoop:
		// Nothing to do.
	case plan.AllocationFinalizeAtomicReplicationOp:
		err = rq.finalizeAtomicReplication(ctx, replica)
	case plan.AllocationTransferLeaseOp:
		err = rq.TransferLease(ctx, replica, op.Source, op.Target, op.Usage)
	case plan.AllocationChangeReplicasOp:
		err = rq.changeReplicas(
			ctx,
			replica,
			op.Chgs,
			replica.Desc(),
			op.Priority,
			op.AllocatorPriority,
			op.Reason,
			op.Details,
		)
	default:
		panic(fmt.Sprintf("Unknown operation %+v, unable to apply replicate queue change", op))
	}

	return err
}

// ShouldRequeue determines whether a replica should be requeued into the
// replicate queue, using the planned change and error returned from either
// application or planning.
func ShouldRequeue(ctx context.Context, change plan.ReplicateChange, conf roachpb.SpanConfig) bool {
	var requeue bool

	if _, ok := change.Op.(plan.AllocationNoop); ok {
		// Don't requeue on a noop, as the replica had nothing to do the first
		// time around.
		requeue = false

	} else if change.Op.LHBeingRemoved() {
		// Don't requeue if the leaseholder was removed as a voter or the range
		// lease was transferred away.
		requeue = false

	} else if change.Action == allocatorimpl.AllocatorConsiderRebalance &&
		!change.Replica.LeaseViolatesPreferences(ctx, conf) {
		// Don't requeue after a successful rebalance operation, when the lease
		// does not violate any preferences. If the lease does violate preferences,
		// the next process attempt will either find a target to transfer the lease
		// or place the replica into purgatory if unable. See
		// CantTransferLeaseViolatingPreferencesError.
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
	desc *roachpb.RangeDescriptor,
	conf roachpb.SpanConfig,
	canTransferLeaseFrom plan.CanTransferLeaseFrom,
	scatter, dryRun bool,
) (requeue bool, _ error) {
	// Ensure that the replica can be processed. The replica must not be
	// destroyed. The replica must have a valid lease. The lease must be the
	// correct type.
	if err := rq.preProcessCheck(ctx, repl); err != nil {
		return false, err
	}

	change, err := rq.planner.PlanOneChange(ctx, repl, desc, conf, canTransferLeaseFrom, scatter)
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
	rq.metrics.trackPlanningStats(ctx, change.Stats)

	// Apply the change generated by PlanOneChange. This call will block until
	// the change has either been applied successfully or failed.
	err = rq.applyChange(ctx, change, repl)

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
	change.Op.ApplyImpact(rq.storePool)

	// Requeue the replica if it meets the criteria in ShouldRequeue.
	return ShouldRequeue(ctx, change, conf), nil
}

// preProcessCheck checks the lease  and destroy status of the replica. This is
// done to ensure that the replica has a valid lease, correct lease type and is
// not destroyed.
func (rq *replicateQueue) preProcessCheck(ctx context.Context, repl *Replica) error {
	// Check lease and destroy status here. The queue does this higher up already, but
	// adminScatter (and potential other future callers) also call this method and don't
	// perform this check, which could lead to infinite loops.
	if _, err := repl.IsDestroyed(); err != nil {
		return err
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
	// TODO(erikgrinaker): This is also done more eagerly during Raft ticks, but
	// that doesn't work for quiesced epoch-based ranges, so we have a fallback
	// here that usually runs within 10 minutes.
	leaseStatus, pErr := repl.redirectOnOrAcquireLease(ctx)
	if pErr != nil {
		return pErr.GoError()
	}
	pErr = repl.maybeSwitchLeaseType(ctx, leaseStatus)
	if pErr != nil {
		return pErr.GoError()
	}
	return nil
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
	rangeUsageInfo := repl.RangeUsageInfo()
	// Learner replicas aren't allowed to become the leaseholder or raft leader,
	// so only consider the `VoterDescriptors` replicas.
	target := rq.allocator.TransferLeaseTarget(
		ctx,
		rq.storePool,
		desc,
		conf,
		desc.Replicas().VoterDescriptors(),
		repl,
		rangeUsageInfo,
		false, /* forceDecisionWithoutStats */
		opts,
	)
	if target == (roachpb.ReplicaDescriptor{}) {
		return allocator.NoSuitableTarget, nil
	}

	if err := rq.TransferLease(ctx, repl, repl.store.StoreID(), target.StoreID, rangeUsageInfo); err != nil {
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
func (rq *replicateQueue) canTransferLeaseFrom(
	ctx context.Context, repl plan.LeaseCheckReplica, conf roachpb.SpanConfig,
) bool {
	if !repl.OwnsValidLease(ctx, rq.store.cfg.Clock.NowAsClockTimestamp()) {
		// This replica is not the leaseholder, so it can't transfer the lease.
		return false
	}
	// Do a best effort check to see if this replica conforms to the configured
	// lease preferences (if any), if it does not we want to encourage more
	// aggressive lease movement and not delay it.
	if repl.LeaseViolatesPreferences(ctx, conf) {
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
