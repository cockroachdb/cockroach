// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaintegration

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototype"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// TestTranslateStorePoolStatusToMMA verifies the translation from StorePool
// status to MMA's (Health, Disposition) model by printing the translation
// table for all store statuses.
func TestTranslateStorePoolStatusToMMA(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// All possible StorePool statuses.
	statuses := []storepool.StoreStatus{
		storepool.StoreStatusDead,
		storepool.StoreStatusUnknown,
		storepool.StoreStatusThrottled,
		storepool.StoreStatusAvailable,
		storepool.StoreStatusDecommissioning,
		storepool.StoreStatusSuspect,
		storepool.StoreStatusDraining,
	}

	var output string
	for _, spStatus := range statuses {
		mmaStatus := translateStorePoolStatusToMMA(spStatus)
		output += fmt.Sprintf("%s: health=%s, lease=%s, replica=%s\n",
			spStatus.String(),
			formatHealth(mmaStatus.Health),
			formatLeaseDisposition(mmaStatus.Disposition.Lease),
			formatReplicaDisposition(mmaStatus.Disposition.Replica),
		)
	}

	echotest.Require(t, output, datapathutils.TestDataPath(t, t.Name()))
}

func formatHealth(h mmaprototype.Health) string {
	switch h {
	case mmaprototype.HealthOK:
		return "OK"
	case mmaprototype.HealthUnhealthy:
		return "Unhealthy"
	case mmaprototype.HealthDead:
		return "Dead"
	case mmaprototype.HealthUnknown:
		return "Unknown"
	default:
		return fmt.Sprintf("Unknown(%d)", h)
	}
}

func formatLeaseDisposition(d mmaprototype.LeaseDisposition) string {
	switch d {
	case mmaprototype.LeaseDispositionOK:
		return "OK"
	case mmaprototype.LeaseDispositionShedding:
		return "Shedding"
	case mmaprototype.LeaseDispositionRefusing:
		return "Refusing"
	default:
		return fmt.Sprintf("Unknown(%d)", d)
	}
}

func formatReplicaDisposition(d mmaprototype.ReplicaDisposition) string {
	switch d {
	case mmaprototype.ReplicaDispositionOK:
		return "OK"
	case mmaprototype.ReplicaDispositionShedding:
		return "Shedding"
	case mmaprototype.ReplicaDispositionRefusing:
		return "Refusing"
	default:
		return fmt.Sprintf("Unknown(%d)", d)
	}
}
