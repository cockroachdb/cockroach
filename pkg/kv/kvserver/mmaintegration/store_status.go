// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaintegration

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototype"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
)

// translateStorePoolStatusToMMA translates a StorePool status to MMA's (health,
// disposition) model.
/*
 StorePool Status Meanings (see storepool.StoreDetailMu.status()):

   - Dead:             No gossip for 5min (TimeUntilNodeDead) OR NodeLivenessStatus is DEAD/DECOMMISSIONED.
   - Unknown:          No gossip received yet (Desc == nil) OR NodeLivenessStatus is UNAVAILABLE/UNKNOWN.
   - Decommissioning:  NodeLivenessStatus is DECOMMISSIONING.
   - Draining:         NodeLivenessStatus is DRAINING.
   - Throttled:        Rejected snapshots in last 5s (FailedReservationsTimeout).
   - Suspect:          LastUnavailable was within last 5min (TimeAfterNodeSuspect).
   - Available:        None of the above; store is healthy.

    | StorePool Status | MMA Health      | Lease Disposition | Replica Disposition | Rationale
    |------------------|-----------------|-------------------|---------------------|-------------------------------------------------------------------------------------------------------------------------------|
    | Dead             | HealthDead      | Shedding          | Shedding            | Store is gone: shed everything   (matches SMA)
    | Decommissioning  | HealthOK        | Refusing          | Shedding            | Store is leaving cluster: shed replicas (leases transfer with them) and refuse leases (SMA allows leases, MMA more aggressive)
    | Unknown          | HealthUnknown   | Shedding          | Refusing            | State is unknown: don't shed replicas but shed leases since it is important for availability (matches SMA)
    | Draining         | HealthOK        | Shedding          | Refusing            | Store is draining: shed leases, don't add new replicas   (matches SMA)
    | Throttled        | HealthOK        | OK                | Refusing            | Store recently rejected snapshots: accept leases but not replicas   (matches SMA)
    | Suspect          | HealthUnhealthy | Shedding          | Refusing            | Recently unavailable: shed leases for safety and don't accept replicas   (matches SMA)
    | Available        | HealthOK        | OK                | OK                  | Healthy store: accept all (matches SMA)

 SMA Behavior Reference:
 Replica Shedding:
   - Dead: AllocatorReplaceDeadVoter, AllocatorRemoveDeadVoter
   - Decommissioning: AllocatorReplaceDecommissioningVoter, AllocatorRemoveDecommissioningVoter

 Replica Refusing (excluded as targets):
   - isStoreReadyForRoutineReplicaTransfer excludes: Dead, Unknown, Decommissioning, Suspect, Draining
   - StoreFilterThrottled (used for allocation): excludes all except Available

 Lease Shedding (LeaseholderShouldMoveDueToPreferences triggers for):
   - Dead, Unknown, Suspect, Draining (excluded from LiveAndDeadReplicas candidates)
   - Decommissioning: leases move indirectly when replicas are shed (SMA doesn't actively shed leases)

 Lease Refusing (excluded as targets via ValidLeaseTargets):
   - LiveAndDeadReplicas(includeSuspectAndDrainingStores=false) excludes: Dead, Unknown, Suspect, Draining
*/
func translateStorePoolStatusToMMA(spStatus storepool.StoreStatus) mmaprototype.Status {
	switch spStatus {
	case storepool.StoreStatusDead:
		return mmaprototype.MakeStatus(
			mmaprototype.HealthDead,
			mmaprototype.LeaseDispositionShedding,
			mmaprototype.ReplicaDispositionShedding,
		)
	case storepool.StoreStatusDecommissioning:
		return mmaprototype.MakeStatus(
			mmaprototype.HealthOK,
			mmaprototype.LeaseDispositionRefusing,
			mmaprototype.ReplicaDispositionShedding,
		)
	case storepool.StoreStatusUnknown:
		return mmaprototype.MakeStatus(
			mmaprototype.HealthUnknown,
			mmaprototype.LeaseDispositionShedding,
			mmaprototype.ReplicaDispositionRefusing,
		)
	case storepool.StoreStatusDraining:
		return mmaprototype.MakeStatus(
			mmaprototype.HealthOK,
			mmaprototype.LeaseDispositionShedding,
			mmaprototype.ReplicaDispositionRefusing,
		)
	case storepool.StoreStatusThrottled:
		return mmaprototype.MakeStatus(
			mmaprototype.HealthOK,
			mmaprototype.LeaseDispositionOK,
			mmaprototype.ReplicaDispositionRefusing,
		)
	case storepool.StoreStatusSuspect:
		return mmaprototype.MakeStatus(
			mmaprototype.HealthUnhealthy,
			mmaprototype.LeaseDispositionShedding,
			mmaprototype.ReplicaDispositionRefusing,
		)
	case storepool.StoreStatusAvailable:
		return mmaprototype.MakeStatus(
			mmaprototype.HealthOK,
			mmaprototype.LeaseDispositionOK,
			mmaprototype.ReplicaDispositionOK,
		)
	default:
		panic(fmt.Sprintf("unknown store status: %d", spStatus))
	}
}
