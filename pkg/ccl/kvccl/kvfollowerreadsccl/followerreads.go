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
	"time"

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

func checkEnterpriseEnabled(clusterID uuid.UUID, st *cluster.Settings) error {
	org := sql.ClusterOrganization.Get(&st.SV)
	return utilccl.CheckEnterpriseEnabled(st, clusterID, org, "follower reads")
}

func checkFollowerReadsEnabled(clusterID uuid.UUID, st *cluster.Settings) bool {
	if !kvserver.FollowerReadsEnabled.Get(&st.SV) {
		return false
	}
	return checkEnterpriseEnabled(clusterID, st) == nil
}

func evalFollowerReadOffset(clusterID uuid.UUID, st *cluster.Settings) (time.Duration, error) {
	if err := checkEnterpriseEnabled(clusterID, st); err != nil {
		return 0, err
	}
	return getFollowerReadLag(st), nil
}

// batchCanBeEvaluatedOnFollower determines if a batch consists exclusively of
// requests that can be evaluated on a follower replica.
func batchCanBeEvaluatedOnFollower(ba roachpb.BatchRequest) bool {
	return ba.Txn != nil && !ba.IsLocking() && ba.IsAllTransactional()
}

// closedTimestampLikelySufficient determines if a request at a given timestamp
// is likely to be below a follower's closed timestamp and servicable as a
// follower read were the request to be sent to a follower replica.
func closedTimestampLikelySufficient(
	st *cluster.Settings,
	clock *hlc.Clock,
	ctPolicy roachpb.RangeClosedTimestampPolicy,
	maxObservableTS hlc.Timestamp,
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
	return maxObservableTS.LessEq(expectedClosedTS)
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
	return checkFollowerReadsEnabled(clusterID, st) &&
		batchCanBeEvaluatedOnFollower(ba) &&
		closedTimestampLikelySufficient(st, clock, ctPolicy, ba.Txn.MaxObservableTimestamp())
}

type followerReadOracle struct {
	st    *cluster.Settings
	clock *hlc.Clock

	closest    replicaoracle.Oracle
	binPacking replicaoracle.Oracle
}

func newFollowerReadOracle(cfg replicaoracle.Config) replicaoracle.Oracle {
	if !checkFollowerReadsEnabled(cfg.RPCContext.ClusterID.Get(), cfg.Settings) {
		return replicaoracle.NewOracle(replicaoracle.BinPackingChoice, cfg)
	}
	return &followerReadOracle{
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
	if txn != nil && closedTimestampLikelySufficient(o.st, o.clock, ctPolicy, txn.MaxObservableTimestamp()) {
		oracle = o.closest
	} else {
		oracle = o.binPacking
	}
	return oracle.ChoosePreferredReplica(ctx, txn, desc, leaseholder, ctPolicy, queryState)
}

// followerReadOraclePolicy is a leaseholder choosing policy that detects
// whether a query can be used with a follower read.
var followerReadOraclePolicy = replicaoracle.RegisterPolicy(newFollowerReadOracle)

func init() {
	sql.ReplicaOraclePolicy = followerReadOraclePolicy
	builtins.EvalFollowerReadOffset = evalFollowerReadOffset
	kvcoord.CanSendToFollower = canSendToFollower
}
