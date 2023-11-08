// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package concurrency

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/errors"
)

// ExclusiveLocksBlockNonLockingReads dictates locking interactions between
// non-locking reads and exclusive locks. Configuring this setting to true makes
// it such that non-locking reads from serializable transactions block on
// exclusive locks held by serializable transactions if the read's timestamp is
// at or above the timestamp at which the lock is held. If set to false,
// non-locking reads do not block on exclusive locks, regardless of isolation
// level or timestamp.
//
// Note that the setting only applies if both the reader and lock holder are
// running with serializable isolation level. If either of them is running with
// weaker isolation levels, the setting has no effect. To understand why,
// consider the tradeoff this setting presents -- the tradeoff here is increased
// concurrency (non-locking reads become non-blocking in the face of Exclusive
// locks) at the expense of forcing Exclusive lock holders to perform a read
// refresh (to prevent write skew), which in turn may force them to restart if
// the refresh fails.
//
// If the lock holder is running at a weaker isolation level (snapshot,
// read committed), then it is able to tolerate write skew. Thus, there is no
// tradeoff -- it is always a good idea to allow any non-locking read to
// proceed. On the other hand, non-locking reads running at weaker isolation
// levels should never block on exclusive locks.
var ExclusiveLocksBlockNonLockingReads = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.lock.exclusive_locks_block_non_locking_reads.enabled",
	"dictates the locking interactions between exclusive locks and non-locking reads",
	true,
)

// LockConflicts returns whether the supplied lock modes conflict with each
// other. Conflict rules are as described in the compatibility matrix in
// locking.pb.go.
func LockConflicts(m1, m2 lock.Mode, sv *settings.Values) bool {
	if m1.Empty() || m2.Empty() {
		return false // no conflict with empty lock modes
	}
	if m1.Strength > m2.Strength {
		// Conflict rules are symmetric, so reduce the number of cases we need to
		// handle.
		m1, m2 = m2, m1
	}
	switch m1.Strength {
	case lock.None:
		switch m2.Strength {
		case lock.None, lock.Shared, lock.Update:
			return false
		case lock.Exclusive:
			// Non-locking reads only conflict with Exclusive locks if the following
			// conditions hold:
			// 1. both the non-locking read and the Exclusive lock belong to
			// transactions that cannot tolerate write skew.
			// 2. AND the non-locking read is reading at or above the timestamp of the
			// lock.
			// 3. AND the ExclusiveLocksBlockNonLockingReads cluster setting is
			// configured to do as much.
			return !m1.IsoLevel.ToleratesWriteSkew() &&
				!m2.IsoLevel.ToleratesWriteSkew() && m2.Timestamp.LessEq(m1.Timestamp) &&
				ExclusiveLocksBlockNonLockingReads.Get(sv)
		case lock.Intent:
			// Non-locking read (m1) conflicts if the intent is at a lower or equal
			// timestamp.
			return m2.Timestamp.LessEq(m1.Timestamp)
		default:
			panic(errors.AssertionFailedf("unknown strength: %s", m2.Strength))
		}
	case lock.Shared:
		// m2.Strength >= Shared due to the normalization above.
		return m2.Strength == lock.Exclusive || m2.Strength == lock.Intent
	case lock.Update, lock.Exclusive, lock.Intent:
		// m2.Strength >= Update due to the normalization above.
		return true
	default:
		panic(errors.AssertionFailedf("unknown strength: %s", m1.Strength))
	}
}
