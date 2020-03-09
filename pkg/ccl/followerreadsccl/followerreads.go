// Copyright 2019 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

// Package followerreadsccl implements and injects the functionality needed to
// expose follower reads to clients.
package followerreadsccl

import (
	"fmt"
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
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// followerReadMultiple is the multiple of kv.closed_timestmap.target_duration
// which the implementation of the follower read capable replica policy ought
// to use to determine if a request can be used for reading.
var followerReadMultiple = settings.RegisterValidatedFloatSetting(
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

// getFollowerReadOffset returns the offset duration which should be used to as
// the offset from now to request a follower read. The same value less the clock
// uncertainty, then is used to determine at the kv layer if a query can use a
// follower read.
func getFollowerReadDuration(st *cluster.Settings) time.Duration {
	targetMultiple := followerReadMultiple.Get(&st.SV)
	targetDuration := closedts.TargetDuration.Get(&st.SV)
	closeFraction := closedts.CloseFraction.Get(&st.SV)
	return -1 * time.Duration(float64(targetDuration)*
		(1+closeFraction*targetMultiple))
}

func checkEnterpriseEnabled(clusterID uuid.UUID, st *cluster.Settings) error {
	org := sql.ClusterOrganization.Get(&st.SV)
	return utilccl.CheckEnterpriseEnabled(st, clusterID, org, "follower reads")
}

func evalFollowerReadOffset(clusterID uuid.UUID, st *cluster.Settings) (time.Duration, error) {
	if err := checkEnterpriseEnabled(clusterID, st); err != nil {
		return 0, err
	}
	return getFollowerReadDuration(st), nil
}

// batchCanBeEvaluatedOnFollower determines if a batch consists exclusively of
// requests that can be evaluated on a follower replica.
func batchCanBeEvaluatedOnFollower(ba roachpb.BatchRequest) bool {
	return !ba.IsLocking() && ba.IsAllTransactional()
}

// txnCanPerformFollowerRead determines if the provided transaction can perform
// follower reads.
func txnCanPerformFollowerRead(txn *roachpb.Transaction) bool {
	// If the request is transactional and that transaction has acquired any
	// locks then that request should not perform follower reads. Doing so could
	// allow the request to miss its own writes or observe state that conflicts
	// with its locks.
	return txn != nil && !txn.IsLocking()
}

// canUseFollowerRead determines if a query can be sent to a follower.
func canUseFollowerRead(clusterID uuid.UUID, st *cluster.Settings, ts hlc.Timestamp) bool {
	if !kvserver.FollowerReadsEnabled.Get(&st.SV) {
		return false
	}
	threshold := (-1 * getFollowerReadDuration(st)) - 1*base.DefaultMaxClockOffset
	if timeutil.Since(ts.GoTime()) < threshold {
		return false
	}
	return checkEnterpriseEnabled(clusterID, st) == nil
}

// canSendToFollower implements the logic for checking whether a batch request
// may be sent to a follower.
func canSendToFollower(clusterID uuid.UUID, st *cluster.Settings, ba roachpb.BatchRequest) bool {
	return batchCanBeEvaluatedOnFollower(ba) &&
		txnCanPerformFollowerRead(ba.Txn) &&
		canUseFollowerRead(clusterID, st, forward(ba.Txn.ReadTimestamp, ba.Txn.MaxTimestamp))
}

func forward(ts hlc.Timestamp, to hlc.Timestamp) hlc.Timestamp {
	ts.Forward(to)
	return ts
}

type oracleFactory struct {
	clusterID *base.ClusterIDContainer
	st        *cluster.Settings

	binPacking replicaoracle.OracleFactory
	closest    replicaoracle.OracleFactory
}

func newOracleFactory(cfg replicaoracle.Config) replicaoracle.OracleFactory {
	return &oracleFactory{
		clusterID:  &cfg.RPCContext.ClusterID,
		st:         cfg.Settings,
		binPacking: replicaoracle.NewOracleFactory(replicaoracle.BinPackingChoice, cfg),
		closest:    replicaoracle.NewOracleFactory(replicaoracle.ClosestChoice, cfg),
	}
}

func (f oracleFactory) Oracle(txn *kv.Txn) replicaoracle.Oracle {
	if txn != nil && canUseFollowerRead(f.clusterID.Get(), f.st, txn.ReadTimestamp()) {
		return f.closest.Oracle(txn)
	}
	return f.binPacking.Oracle(txn)
}

// followerReadAwareChoice is a leaseholder choosing policy that detects
// whether a query can be used with a follower read.
var followerReadAwareChoice = replicaoracle.RegisterPolicy(newOracleFactory)

func init() {
	sql.ReplicaOraclePolicy = followerReadAwareChoice
	builtins.EvalFollowerReadOffset = evalFollowerReadOffset
	kvcoord.CanSendToFollower = canSendToFollower
}
