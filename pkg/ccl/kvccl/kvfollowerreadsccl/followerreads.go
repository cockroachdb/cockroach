// Copyright 2019 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

// Package kvfollowerreadsccl implements and injects the functionality needed to
// expose follower reads to clients.
package kvfollowerreadsccl

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan/replicaoracle"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// followerReadMultiple is the multiple of kv.closed_timestmap.target_duration
// which the implementation of the follower read capable replica policy ought
// to use to determine if a request can be used for reading.
var followerReadMultiple = settings.RegisterFloatSetting(
	"kv.follower_read.target_multiple",
	"if above 1, encourages the distsender to perform a read against the "+
		"closest replica if a request is older than kv.closed_timestamp.target_duration"+
		" * (1 + kv.closed_timestamp.close_fraction * this) less a clock uncertainty "+
		"interval. This value also is used to create follower_timestamp().",
	3,
	func(v float64) error {
		if v < 1 {
			return fmt.Errorf("%v is not >= 1", v)
		}
		return nil
	},
)

// getFollowerReadLag returns the (negative) offset duration from hlc.Now()
// which should be used to request a follower read. The same value is used to
// determine at the kv layer if a query can use a follower read for ranges with
// the default LAG_BY_CLUSTER_SETTING closed timestamp policy.
func getFollowerReadLag(st *cluster.Settings) time.Duration {
	targetMultiple := followerReadMultiple.Get(&st.SV)
	targetDuration := closedts.TargetDuration.Get(&st.SV)
	closeFraction := closedts.CloseFraction.Get(&st.SV)
	// Zero targetDuration means follower reads are disabled.
	if targetDuration == 0 {
		// Returning an infinitely large negative value would push safe
		// request timestamp into the distant past thus disabling follower reads.
		return math.MinInt64
	}
	return -1 * time.Duration(float64(targetDuration)*
		(1+closeFraction*targetMultiple))
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
func checkEnterpriseEnabled(clusterID uuid.UUID, st *cluster.Settings) error {
	org := sql.ClusterOrganization.Get(&st.SV)
	return utilccl.CheckEnterpriseEnabled(st, clusterID, org, "follower reads")
}

// isEnterpriseEnabled is faster than checkEnterpriseEnabled, and suitable
// for hot paths.
func isEnterpriseEnabled(clusterID uuid.UUID, st *cluster.Settings) bool {
	org := sql.ClusterOrganization.Get(&st.SV)
	return utilccl.IsEnterpriseEnabled(st, clusterID, org, "follower reads")
}

func checkFollowerReadsEnabled(clusterID uuid.UUID, st *cluster.Settings) bool {
	if !kvserver.FollowerReadsEnabled.Get(&st.SV) {
		return false
	}
	return isEnterpriseEnabled(clusterID, st)
}

func evalFollowerReadOffset(clusterID uuid.UUID, st *cluster.Settings) (time.Duration, error) {
	if err := checkEnterpriseEnabled(clusterID, st); err != nil {
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
	clusterID uuid.UUID,
	st *cluster.Settings,
	clock *hlc.Clock,
	ctPolicy roachpb.RangeClosedTimestampPolicy,
	ba roachpb.BatchRequest,
) bool {
	return kvserver.BatchCanBeEvaluatedOnFollower(ba) &&
		closedTimestampLikelySufficient(st, clock, ctPolicy, ba.Txn.RequiredFrontier()) &&
		// NOTE: this call can be expensive, so perform it last. See #62447.
		checkFollowerReadsEnabled(clusterID, st)
}

type followerReadOracle struct {
	clusterID *base.ClusterIDContainer
	st        *cluster.Settings
	clock     *hlc.Clock

	closest    replicaoracle.Oracle
	binPacking replicaoracle.Oracle
}

func newFollowerReadOracle(cfg replicaoracle.Config) replicaoracle.Oracle {
	return &followerReadOracle{
		clusterID:  &cfg.RPCContext.ClusterID,
		st:         cfg.Settings,
		clock:      cfg.RPCContext.Clock,
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
) (roachpb.ReplicaDescriptor, error) {
	var oracle replicaoracle.Oracle
	if o.useClosestOracle(txn, ctPolicy) {
		oracle = o.closest
	} else {
		oracle = o.binPacking
	}
	return oracle.ChoosePreferredReplica(ctx, txn, desc, leaseholder, ctPolicy, queryState)
}

func (o *followerReadOracle) useClosestOracle(
	txn *kv.Txn, ctPolicy roachpb.RangeClosedTimestampPolicy,
) bool {
	// NOTE: this logic is almost identical to canSendToFollower, except that it
	// operates on a *kv.Txn instead of a roachpb.BatchRequest. As a result, the
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
		closedTimestampLikelySufficient(o.st, o.clock, ctPolicy, txn.RequiredFrontier()) &&
		// NOTE: this call can be expensive, so perform it last. See #62447.
		checkFollowerReadsEnabled(o.clusterID.Get(), o.st)
}

// followerReadOraclePolicy is a leaseholder choosing policy that detects
// whether a query can be used with a follower read.
var followerReadOraclePolicy = replicaoracle.RegisterPolicy(newFollowerReadOracle)

func init() {
	sql.ReplicaOraclePolicy = followerReadOraclePolicy
	builtins.EvalFollowerReadOffset = evalFollowerReadOffset
	kvcoord.CanSendToFollower = canSendToFollower
}
