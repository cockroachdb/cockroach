// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package kvfollowerreadsccl implements and injects the functionality needed to
// expose follower reads to clients.
package kvfollowerreadsccl

import (
	"context"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan/replicaoracle"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

// ClosedTimestampPropagationSlack is used by follower_read_timestamp() as a
// measure of how long closed timestamp updates are supposed to take from the
// leaseholder to the followers.
var ClosedTimestampPropagationSlack = settings.RegisterDurationSetting(
	settings.SystemVisible,
	"kv.closed_timestamp.propagation_slack",
	"a conservative estimate of the amount of time expect for closed timestamps to "+
		"propagate from a leaseholder to followers. This is taken into account by "+
		"follower_read_timestamp().",
	time.Second,
	settings.NonNegativeDuration,
)

// getFollowerReadLag returns the (negative) offset duration from hlc.Now()
// which should be used to request a follower read. The same value is used to
// determine at the kv layer if a query can use a follower read for ranges with
// the default LAG_BY_CLUSTER_SETTING closed timestamp policy.
func getFollowerReadLag(st *cluster.Settings) time.Duration {
	targetDuration := closedts.TargetDuration.Get(&st.SV)
	sideTransportInterval := closedts.SideTransportCloseInterval.Get(&st.SV)
	slack := ClosedTimestampPropagationSlack.Get(&st.SV)
	// Zero targetDuration means follower reads are disabled.
	if targetDuration == 0 {
		// Returning an infinitely large negative value would push safe
		// request timestamp into the distant past thus disabling follower reads.
		return math.MinInt64
	}
	return -targetDuration - sideTransportInterval - slack
}

// getGlobalReadsLead returns the (positive) offset duration from hlc.Now()
// which clients can expect followers of a range with the LEAD_FOR_GLOBAL_READS
// closed timestamp policy to have closed off. This offset is equal to the
// maximum clock offset, allowing present-time (i.e. those not pushed into the
// future) transactions to serve reads from followers.
func getGlobalReadsLead(clock *hlc.Clock) time.Duration {
	return clock.MaxOffset()
}

// checkEnterpriseEnabled checks whether the enterprise feature for follower
// reads is enabled, returning a detailed error if not. It is not suitable for
// use in hot paths since a new error may be instantiated on each call.
func checkEnterpriseEnabled(st *cluster.Settings) error {
	return utilccl.CheckEnterpriseEnabled(st, "follower reads")
}

// isEnterpriseEnabled is faster than checkEnterpriseEnabled, and suitable
// for hot paths.
func isEnterpriseEnabled(st *cluster.Settings) bool {
	return utilccl.IsEnterpriseEnabled(st, "follower reads")
}

func checkFollowerReadsEnabled(ctx context.Context, st *cluster.Settings) bool {
	if !kvserver.FollowerReadsEnabled.Get(&st.SV) {
		return false
	}
	return isEnterpriseEnabled(st)
}

func evalFollowerReadOffset(st *cluster.Settings) (time.Duration, error) {
	if err := checkEnterpriseEnabled(st); err != nil {
		return 0, err
	}
	// NOTE: we assume that at least some of the ranges being queried use a
	// LAG_BY_CLUSTER_SETTING closed timestamp policy. Otherwise, there would
	// be no reason to use AS OF SYSTEM TIME follower_read_timestamp().
	return getFollowerReadLag(st), nil
}

// closedTimestampLikelySufficient determines if a request with a given required
// frontier timestamp is likely to be below a follower's closed timestamp and
// serviceable as a follower read were the request to be sent to a follower
// replica.
func closedTimestampLikelySufficient(
	ctx context.Context,
	st *cluster.Settings,
	clock *hlc.Clock,
	ctPolicy roachpb.RangeClosedTimestampPolicy,
	requiredFrontierTS hlc.Timestamp,
) bool {
	var offset time.Duration
	switch ctPolicy {
	case roachpb.LAG_BY_CLUSTER_SETTING:
		offset = getFollowerReadLag(st)
	case roachpb.LEAD_FOR_GLOBAL_READS:
		offset = getGlobalReadsLead(clock)
	default:
		panic("unknown RangeClosedTimestampPolicy")
	}
	expectedClosedTS := clock.Now().Add(offset.Nanoseconds(), 0)
	return requiredFrontierTS.LessEq(expectedClosedTS)
}

// canSendToFollower implements the logic for checking whether a batch request
// may be sent to a follower.
func canSendToFollower(
	ctx context.Context,
	st *cluster.Settings,
	clock *hlc.Clock,
	ctPolicy roachpb.RangeClosedTimestampPolicy,
	ba *kvpb.BatchRequest,
) bool {
	result := kvserver.BatchCanBeEvaluatedOnFollower(ctx, ba) &&
		closedTimestampLikelySufficient(ctx, st, clock, ctPolicy, ba.RequiredFrontier()) &&
		// NOTE: this call can be expensive, so perform it last. See #62447.
		checkFollowerReadsEnabled(ctx, st)
	return result
}

type followerReadOracle struct {
	st    *cluster.Settings
	clock *hlc.Clock

	closest    replicaoracle.Oracle
	binPacking replicaoracle.Oracle
}

func newFollowerReadOracle(cfg replicaoracle.Config) replicaoracle.Oracle {
	return &followerReadOracle{
		st:         cfg.Settings,
		clock:      cfg.Clock,
		closest:    replicaoracle.NewOracle(replicaoracle.ClosestChoice, cfg),
		binPacking: replicaoracle.NewOracle(replicaoracle.BinPackingChoice, cfg),
	}
}

func (o *followerReadOracle) ChoosePreferredReplica(
	ctx context.Context,
	txn *kv.Txn,
	desc *roachpb.RangeDescriptor,
	leaseholder *roachpb.ReplicaDescriptor,
	ctPolicy roachpb.RangeClosedTimestampPolicy,
	queryState replicaoracle.QueryState,
) (_ roachpb.ReplicaDescriptor, ignoreMisplannedRanges bool, _ error) {
	var oracle replicaoracle.Oracle
	if o.useClosestOracle(ctx, txn, ctPolicy) {
		oracle = o.closest
	} else {
		oracle = o.binPacking
	}
	return oracle.ChoosePreferredReplica(ctx, txn, desc, leaseholder, ctPolicy, queryState)
}

func (o *followerReadOracle) useClosestOracle(
	ctx context.Context, txn *kv.Txn, ctPolicy roachpb.RangeClosedTimestampPolicy,
) bool {
	// NOTE: this logic is almost identical to canSendToFollower, except that it
	// operates on a *kv.Txn instead of a kvpb.BatchRequest. As a result, the
	// function does not check batchCanBeEvaluatedOnFollower. This is because we
	// assume that if a request is going to be executed in a distributed DistSQL
	// flow (which is why it is consulting a replicaoracle.Oracle), then all of
	// the individual BatchRequests that it send will be eligible to be served
	// on follower replicas as follower reads.
	//
	// If we were to get this assumption wrong, the flow might be planned on a
	// node with a follower replica, but individual BatchRequests would still be
	// sent to the correct replicas once canSendToFollower is checked for each
	// BatchRequests in the DistSender. This would hurt performance, but would
	// not violate correctness.
	return txn != nil &&
		closedTimestampLikelySufficient(ctx, o.st, o.clock, ctPolicy, txn.RequiredFrontier()) &&
		// NOTE: this call can be expensive, so perform it last. See #62447.
		checkFollowerReadsEnabled(ctx, o.st)
}

// followerReadOraclePolicy is a leaseholder choosing policy that detects
// whether a query can be used with a follower read.
var followerReadOraclePolicy = replicaoracle.RegisterPolicy(newFollowerReadOracle)

type bulkOracle struct {
	cfg       replicaoracle.Config
	locFilter roachpb.Locality
	streaks   StreakConfig
}

// StreakConfig controls the streak-preferring behavior of oracles that support
// it, such as the bulk oracle, for minimizing distinct spans in large plans.
// See the fields and ShouldExtend for details.
type StreakConfig struct {
	// Min is the streak lengths below which streaks are always extended, if able,
	// unless overridden in a "small" plan by SmallPlanMin below.
	Min int
	// SmallPlanMin and SmallPlanThreshold are used to override the cap on streak
	// lengths that are always extended to be lower when plans are still "small".
	// Being "small" is defined as the node with the fewest assigned spans having
	// fewer than SmallPlanThreshold assigned spans. If SmallPlanThreshold is >0,
	// then when this condition is met SmallPlanMin is used instead of Min.
	SmallPlanMin, SmallPlanThreshold int
	// MaxSkew is the fraction (e.g. 0.95) of the number of spans assigned to a
	// node that must be assigned to the node with the fewest assigned spans to
	// extend a streak on that node beyond the Min streak length.
	MaxSkew float64
}

// shouldExtend returns whether the current streak should be extended if able,
// according to its length, the number of spans assigned to the node on which it
// would be extended, and the number assigned to the candidate node with the
// fewest span assigned. This would be the case if the streak that would be
// extended is below the minimum streak length (which can be different
// initially/in smaller plans) or the plan is balanced enough to tolerate
// extending the streak. See the the fields of StreakConfig for details.
func (s StreakConfig) shouldExtend(streak, fewestSpansAssigned, assigned int) bool {
	if streak < s.SmallPlanMin {
		return true
	}
	if streak < s.Min && s.SmallPlanThreshold < fewestSpansAssigned {
		return true
	}
	return fewestSpansAssigned >= int(float64(assigned)*s.MaxSkew)
}

var _ replicaoracle.Oracle = bulkOracle{}

// NewBulkOracle returns an oracle for planning bulk operations, which will plan
// balancing randomly across all replicas (if follower reads are enabled).
// TODO(dt): respect streak preferences when using locality filtering. #120755.
func NewBulkOracle(
	cfg replicaoracle.Config, locFilter roachpb.Locality, streaks StreakConfig,
) replicaoracle.Oracle {
	return bulkOracle{cfg: cfg, locFilter: locFilter, streaks: streaks}
}

// ChoosePreferredReplica implements the replicaoracle.Oracle interface.
func (r bulkOracle) ChoosePreferredReplica(
	ctx context.Context,
	_ *kv.Txn,
	desc *roachpb.RangeDescriptor,
	leaseholder *roachpb.ReplicaDescriptor,
	_ roachpb.RangeClosedTimestampPolicy,
	qs replicaoracle.QueryState,
) (_ roachpb.ReplicaDescriptor, ignoreMisplannedRanges bool, _ error) {
	if leaseholder != nil && !checkFollowerReadsEnabled(ctx, r.cfg.Settings) {
		return *leaseholder, false, nil
	}

	replicas, err := kvcoord.NewReplicaSlice(ctx, r.cfg.NodeDescs, desc, nil, kvcoord.AllExtantReplicas)
	if err != nil {
		return roachpb.ReplicaDescriptor{}, false, sqlerrors.NewRangeUnavailableError(desc.RangeID, err)
	}
	if r.locFilter.NonEmpty() {
		var matches []int
		for i := range replicas {
			if ok, _ := replicas[i].Locality.Matches(r.locFilter); ok {
				matches = append(matches, i)
			}
		}
		// TODO(dt): ideally we'd just filter `replicas`  here, then continue on to
		// the code below to pick one as normal, just from the filtered slice.
		if len(matches) > 0 {
			return replicas[matches[randutil.FastUint32()%uint32(len(matches))]].ReplicaDescriptor, true, nil
		}
	}

	if r.streaks.Min > 0 {
		// Find the index of replica in replicas that is on the node that was last
		// assigned a span in this plan if it exists. While doing so, find the
		// number of spans assigned to that node and to the node with the fewest
		// spans assigned to it.
		prevIdx, prevAssigned, fewestAssigned := -1, -1, -1
		for i := range replicas {
			assigned := qs.RangesPerNode.GetDefault(int(replicas[i].NodeID))
			if replicas[i].NodeID == qs.LastAssignment {
				prevIdx = i
				prevAssigned = assigned
			}
			if assigned < fewestAssigned || fewestAssigned == -1 {
				fewestAssigned = assigned
			}
		}
		// If the previously chosen node is a candidate in replicas, check if we want
		// to pick it again to extend the node's streak instead of picking randomly.
		if prevIdx != -1 && r.streaks.shouldExtend(qs.NodeStreak, fewestAssigned, prevAssigned) {
			return replicas[prevIdx].ReplicaDescriptor, true, nil
		}
	}

	return replicas[randutil.FastUint32()%uint32(len(replicas))].ReplicaDescriptor, true, nil
}

func init() {
	sql.ReplicaOraclePolicy = followerReadOraclePolicy
	builtins.EvalFollowerReadOffset = evalFollowerReadOffset
	kvcoord.CanSendToFollower = canSendToFollower
}
