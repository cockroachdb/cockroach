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
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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

func PutUniqLock(
	ctx context.Context, b Putter, waitPolicy tree.LockingWaitPolicy, k roachpb.Key, traceKV bool,
) error {
	if waitPolicy != tree.LockWaitBlock {
		return errors.Errorf("Non-blocking predicate locks are not implemented")
	}

	if traceKV {
		log.VEventf(ctx, 2, "PredicateLock %s", k)
	}

	// CPut a tombstone to check for existance of the key and block writes. This will keep other writers
	// from writing the key in other regions unitl we commit.
	b.CPut(k, nil, nil)

	return nil
}
