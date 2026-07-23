// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package descpb

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// PrettyString returns the locking strength as a user-readable string.
func (s ScanLockingStrength) PrettyString() string {
	switch s {
	case ScanLockingStrength_FOR_NONE:
		return "for none"
	case ScanLockingStrength_FOR_KEY_SHARE:
		return "for key share"
	case ScanLockingStrength_FOR_SHARE:
		return "for share"
	case ScanLockingStrength_FOR_NO_KEY_UPDATE:
		return "for no key update"
	case ScanLockingStrength_FOR_UPDATE:
		return "for update"
	default:
		panic(errors.AssertionFailedf("unexpected strength"))
	}
}

// ToScanLockingStrength converts a tree.LockingStrength to its corresponding
// ScanLockingStrength.
func ToScanLockingStrength(s tree.LockingStrength) ScanLockingStrength {
	switch s {
	case tree.ForNone:
		return ScanLockingStrength_FOR_NONE
	case tree.ForKeyShare:
		return ScanLockingStrength_FOR_KEY_SHARE
	case tree.ForShare:
		return ScanLockingStrength_FOR_SHARE
	case tree.ForNoKeyUpdate:
		return ScanLockingStrength_FOR_NO_KEY_UPDATE
	case tree.ForUpdate:
		return ScanLockingStrength_FOR_UPDATE
	default:
		panic(errors.AssertionFailedf("unknown locking strength %s", s))
	}
}

// PrettyString returns the locking strength as a user-readable string.
func (wp ScanLockingWaitPolicy) PrettyString() string {
	switch wp {
	case ScanLockingWaitPolicy_BLOCK:
		return "block"
	case ScanLockingWaitPolicy_SKIP_LOCKED:
		return "skip locked"
	case ScanLockingWaitPolicy_ERROR:
		return "nowait"
	default:
		panic(errors.AssertionFailedf("unexpected wait policy"))
	}
}

// ToScanLockingWaitPolicy converts a tree.LockingWaitPolicy to its
// corresponding ScanLockingWaitPolicy.
func ToScanLockingWaitPolicy(wp tree.LockingWaitPolicy) ScanLockingWaitPolicy {
	switch wp {
	case tree.LockWaitBlock:
		return ScanLockingWaitPolicy_BLOCK
	case tree.LockWaitSkipLocked:
		return ScanLockingWaitPolicy_SKIP_LOCKED
	case tree.LockWaitError:
		return ScanLockingWaitPolicy_ERROR
	default:
		panic(errors.AssertionFailedf("unknown locking wait policy %s", wp))
	}
}

// PrettyString returns the locking durability as a user-readable string.
func (s ScanLockingDurability) PrettyString() string {
	switch s {
	case ScanLockingDurability_BEST_EFFORT:
		return "best effort"
	case ScanLockingDurability_GUARANTEED:
		return "guaranteed"
	default:
		panic(errors.AssertionFailedf("unexpected durability %s", s))
	}
}

// ToScanLockingDurability converts a tree.LockingDurability to its
// corresponding ScanLockingDurability.
func ToScanLockingDurability(s tree.LockingDurability) ScanLockingDurability {
	switch s {
	case tree.LockDurabilityBestEffort:
		return ScanLockingDurability_BEST_EFFORT
	case tree.LockDurabilityGuaranteed:
		return ScanLockingDurability_GUARANTEED
	default:
		panic(errors.AssertionFailedf("unknown locking durability %d", s))
	}
}
