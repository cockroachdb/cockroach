// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaintegration

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototype"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
)

// translateStorePoolStatusToMMA translates a StorePool status to MMA's (health,
// disposition) model.
//
//	| StorePool Status | MMA Health      | Lease Disposition | Replica Disposition | Rationale                                                             |
//	|------------------|-----------------|-------------------|---------------------|-----------------------------------------------------------------------|
//	| Dead             | HealthDead      | Shedding          | Shedding            | Store is gone: shed everything                                        |
//	| Unknown          | HealthUnknown   | Refusing          | Refusing            | State is unknown: don't add but don't remove either                   |
//	| Decommissioning  | HealthOK        | Shedding          | Shedding            | Store is leaving cluster: shed everything                             |
//	| Draining         | HealthOK        | Shedding          | Refusing            | Store is draining: shed leases, accept replicas                       |
//	| Throttled        | HealthOK        | OK                | Refusing            | Healthy but overlpaded: accept leases but not replicas                |
//	| Suspect          | HealthUnhealthy | Shedding          | Refusing            | Recently unavailable: shed leases for safety and don't accept replicas|
//	| Available        | HealthOK        | OK                | OK                  | Healthy store: accept all                                             |
func translateStorePoolStatusToMMA(spStatus storepool.StoreStatus) mmaprototype.Status {
	switch spStatus {
	case storepool.StoreStatusDead:
		return mmaprototype.MakeStatus(
			mmaprototype.HealthDead,
			mmaprototype.LeaseDispositionShedding,
			mmaprototype.ReplicaDispositionShedding,
		)
	case storepool.StoreStatusUnknown:
		return mmaprototype.MakeStatus(
			mmaprototype.HealthUnknown,
			mmaprototype.LeaseDispositionRefusing,
			mmaprototype.ReplicaDispositionRefusing,
		)
	case storepool.StoreStatusDecommissioning:
		return mmaprototype.MakeStatus(
			mmaprototype.HealthOK,
			mmaprototype.LeaseDispositionShedding,
			mmaprototype.ReplicaDispositionShedding,
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
		// Unknown status - treat as unavailable.
		return mmaprototype.MakeStatus(
			mmaprototype.HealthUnknown,
			mmaprototype.LeaseDispositionRefusing,
			mmaprototype.ReplicaDispositionRefusing,
		)
	}
}
