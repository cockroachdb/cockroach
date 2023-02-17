// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
		// We currently perform no per-key locking when FOR_SHARE is used
		// because Shared locks have not yet been implemented.
		return lock.None

	case descpb.ScanLockingStrength_FOR_NO_KEY_UPDATE:
		// Promote to FOR_UPDATE.
		fallthrough
	case descpb.ScanLockingStrength_FOR_UPDATE:
		// We currently perform exclusive per-key locking when FOR_UPDATE is
		// used because Update locks have not yet been implemented.
		return lock.Exclusive

	default:
		panic(errors.AssertionFailedf("unknown locking strength %s", lockStrength))
	}
}

// getWaitPolicy returns the configured lock wait policy to use for key-value
// scans.
func getWaitPolicy(lockWaitPolicy descpb.ScanLockingWaitPolicy) lock.WaitPolicy {
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
