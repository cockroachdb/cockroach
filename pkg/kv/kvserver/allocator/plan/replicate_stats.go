// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package plan

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/allocatorimpl"
)

// ReplicateStats tracks the replication planning statistics for number of
// replicas added, removed and so on; for the different types of replicas and
// replica states.
type ReplicateStats struct {
	AddReplicaCount                           int64
	AddVoterReplicaCount                      int64
	AddNonVoterReplicaCount                   int64
	RemoveReplicaCount                        int64
	RemoveVoterReplicaCount                   int64
	RemoveNonVoterReplicaCount                int64
	RemoveDeadReplicaCount                    int64
	RemoveDeadVoterReplicaCount               int64
	RemoveDeadNonVoterReplicaCount            int64
	RemoveDecommissioningReplicaCount         int64
	RemoveDecommissioningVoterReplicaCount    int64
	RemoveDecommissioningNonVoterReplicaCount int64
	RemoveLearnerReplicaCount                 int64
	RebalanceReplicaCount                     int64
	RebalanceVoterReplicaCount                int64
	RebalanceNonVoterReplicaCount             int64
	NonVoterPromotionsCount                   int64
	VoterDemotionsCount                       int64
}

// Merge combines the calling ReplicateStats with the given ReplicateStats and
// returns the merger of the two.
func (rs ReplicateStats) Merge(other ReplicateStats) ReplicateStats {
	rs.AddReplicaCount += other.AddReplicaCount
	rs.AddVoterReplicaCount += other.AddVoterReplicaCount
	rs.AddNonVoterReplicaCount += other.AddNonVoterReplicaCount
	rs.RemoveReplicaCount += other.RemoveReplicaCount
	rs.RemoveVoterReplicaCount += other.RemoveVoterReplicaCount
	rs.RemoveNonVoterReplicaCount += other.RemoveNonVoterReplicaCount
	rs.RemoveDeadReplicaCount += other.RemoveDeadReplicaCount
	rs.RemoveDeadVoterReplicaCount += other.RemoveDeadVoterReplicaCount
	rs.RemoveDeadNonVoterReplicaCount += other.RemoveDeadNonVoterReplicaCount
	rs.RemoveDecommissioningReplicaCount += other.RemoveDecommissioningReplicaCount
	rs.RemoveDecommissioningVoterReplicaCount += other.RemoveDecommissioningVoterReplicaCount
	rs.RemoveDecommissioningNonVoterReplicaCount += other.RemoveDecommissioningNonVoterReplicaCount
	rs.RemoveLearnerReplicaCount += other.RemoveLearnerReplicaCount
	rs.RebalanceReplicaCount += other.RebalanceReplicaCount
	rs.RebalanceVoterReplicaCount += other.RebalanceVoterReplicaCount
	rs.RebalanceNonVoterReplicaCount += other.RebalanceNonVoterReplicaCount
	rs.NonVoterPromotionsCount += other.NonVoterPromotionsCount
	rs.VoterDemotionsCount += other.VoterDemotionsCount
	return rs
}

// trackAddReplicaCount increases the AddReplicaCount metric and separately
// tracks voter/non-voter metrics given a replica targetType.
func (rs ReplicateStats) trackAddReplicaCount(
	targetType allocatorimpl.TargetReplicaType,
) ReplicateStats {
	rs.AddReplicaCount++
	switch targetType {
	case allocatorimpl.VoterTarget:
		rs.AddVoterReplicaCount++
	case allocatorimpl.NonVoterTarget:
		rs.AddNonVoterReplicaCount++
	default:
		panic(fmt.Sprintf("unsupported targetReplicaType: %v", targetType))
	}
	return rs
}

// trackRemoveMetric increases total RemoveReplicaCount metrics and
// increments dead/decommissioning metrics depending on replicaStatus.
func (rs ReplicateStats) trackRemoveMetric(
	targetType allocatorimpl.TargetReplicaType, replicaStatus allocatorimpl.ReplicaStatus,
) ReplicateStats {
	rs = rs.trackRemoveReplicaCount(targetType)
	switch replicaStatus {
	case allocatorimpl.Dead:
		rs = rs.trackRemoveDeadReplicaCount(targetType)
	case allocatorimpl.Decommissioning:
		rs = rs.trackRemoveDecommissioningReplicaCount(targetType)
	case allocatorimpl.Alive:
		return rs
	default:
		panic(fmt.Sprintf("unknown replicaStatus %v", replicaStatus))
	}
	return rs
}

// trackRemoveReplicaCount increases the RemoveReplicaCount metric and
// separately tracks voter/non-voter metrics given a replica targetType.
func (rs ReplicateStats) trackRemoveReplicaCount(
	targetType allocatorimpl.TargetReplicaType,
) ReplicateStats {
	rs.RemoveReplicaCount++
	switch targetType {
	case allocatorimpl.VoterTarget:
		rs.RemoveVoterReplicaCount++
	case allocatorimpl.NonVoterTarget:
		rs.RemoveNonVoterReplicaCount++
	default:
		panic(fmt.Sprintf("unsupported targetReplicaType: %v", targetType))
	}
	return rs
}

// trackRemoveDeadReplicaCount increases the RemoveDeadReplicaCount metric and
// separately tracks voter/non-voter metrics given a replica targetType.
func (rs ReplicateStats) trackRemoveDeadReplicaCount(
	targetType allocatorimpl.TargetReplicaType,
) ReplicateStats {
	rs.RemoveDeadReplicaCount++
	switch targetType {
	case allocatorimpl.VoterTarget:
		rs.RemoveDeadVoterReplicaCount++
	case allocatorimpl.NonVoterTarget:
		rs.RemoveDeadNonVoterReplicaCount++
	default:
		panic(fmt.Sprintf("unsupported targetReplicaType: %v", targetType))
	}
	return rs
}

// trackRemoveDecommissioningReplicaCount increases the
// RemoveDecommissioningReplicaCount metric and separately tracks
// voter/non-voter metrics given a replica targetType.
func (rs ReplicateStats) trackRemoveDecommissioningReplicaCount(
	targetType allocatorimpl.TargetReplicaType,
) ReplicateStats {
	rs.RemoveDecommissioningReplicaCount++
	switch targetType {
	case allocatorimpl.VoterTarget:
		rs.RemoveDecommissioningVoterReplicaCount++
	case allocatorimpl.NonVoterTarget:
		rs.RemoveDecommissioningNonVoterReplicaCount++
	default:
		panic(fmt.Sprintf("unsupported targetReplicaType: %v", targetType))
	}
	return rs
}

// trackRebalanceReplicaCount increases the RebalanceReplicaCount metric and
// separately tracks voter/non-voter metrics given a replica targetType.
func (rs ReplicateStats) trackRebalanceReplicaCount(
	targetType allocatorimpl.TargetReplicaType,
) ReplicateStats {
	rs.RebalanceReplicaCount++
	switch targetType {
	case allocatorimpl.VoterTarget:
		rs.RebalanceVoterReplicaCount++
	case allocatorimpl.NonVoterTarget:
		rs.RebalanceNonVoterReplicaCount++
	default:
		panic(fmt.Sprintf("unsupported targetReplicaType: %v", targetType))
	}
	return rs
}
