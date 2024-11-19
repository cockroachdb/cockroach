// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package row

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/errors"
)

// GetKeyLockingStrength returns the configured per-key locking strength to use
// for key-value scans.
func GetKeyLockingStrength(lockStrength descpb.ScanLockingStrength) lock.Strength {
	switch lockStrength {
	case descpb.ScanLockingStrength_FOR_NONE:
		return lock.None

	case descpb.ScanLockingStrength_FOR_KEY_SHARE:
		// Promote to FOR_SHARE.
		fallthrough
	case descpb.ScanLockingStrength_FOR_SHARE:
		return lock.Shared

	case descpb.ScanLockingStrength_FOR_NO_KEY_UPDATE:
		// Promote to FOR_UPDATE.
		fallthrough
	case descpb.ScanLockingStrength_FOR_UPDATE:
		return lock.Exclusive

	default:
		panic(errors.AssertionFailedf("unknown locking strength %s", lockStrength))
	}
}

// GetWaitPolicy returns the configured lock wait policy to use for key-value
// scans.
func GetWaitPolicy(lockWaitPolicy descpb.ScanLockingWaitPolicy) lock.WaitPolicy {
	switch lockWaitPolicy {
	case descpb.ScanLockingWaitPolicy_BLOCK:
		return lock.WaitPolicy_Block

	case descpb.ScanLockingWaitPolicy_SKIP_LOCKED:
		return lock.WaitPolicy_SkipLocked

	case descpb.ScanLockingWaitPolicy_ERROR:
		return lock.WaitPolicy_Error

	default:
		panic(errors.AssertionFailedf("unknown wait policy %s", lockWaitPolicy))
	}
}

// GetKeyLockingDurability returns the configured lock durability to use for
// key-value scans.
func GetKeyLockingDurability(lockDurability descpb.ScanLockingDurability) lock.Durability {
	switch lockDurability {
	case descpb.ScanLockingDurability_BEST_EFFORT:
		return lock.Unreplicated

	case descpb.ScanLockingDurability_GUARANTEED:
		return lock.Replicated

	default:
		panic(errors.AssertionFailedf("unknown lock durability %s", lockDurability))
	}
}
