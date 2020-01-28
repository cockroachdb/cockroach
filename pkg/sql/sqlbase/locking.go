// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlbase

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

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
		panic(fmt.Sprintf("unknown locking strength %s", s))
	}
}

// ToScanLockingWaitPolicy converts a tree.LockingWaitPolicy to its
// corresponding ScanLockingWaitPolicy.
func ToScanLockingWaitPolicy(wp tree.LockingWaitPolicy) ScanLockingWaitPolicy {
	switch wp {
	case tree.LockWaitBlock:
		return ScanLockingWaitPolicy_BLOCK
	case tree.LockWaitSkip:
		return ScanLockingWaitPolicy_SKIP
	case tree.LockWaitError:
		return ScanLockingWaitPolicy_ERROR
	default:
		panic(fmt.Sprintf("unknown locking wait policy %s", wp))
	}
}
