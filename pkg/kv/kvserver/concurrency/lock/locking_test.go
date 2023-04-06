// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package lock_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/stretchr/testify/require"
)

// TestCheckLockConflicts ensures the lock conflict semantics, as
// illustrated in the compatibility matrix in locking.pb.go, are upheld.
func TestCheckLockConflicts(t *testing.T) {
	makeTS := func(nanos int64) hlc.Timestamp {
		return hlc.Timestamp{
			WallTime: nanos,
		}
	}

	tsBelow := makeTS(1)
	tsLock := makeTS(2)
	tsAbove := makeTS(3)

	testCases := []struct {
		desc            string
		heldLockMode    lock.Mode
		toCheckLockMode lock.Mode
		// Expectation for when non-locking reads do not block on Exclusive locks.
		exp bool
		// Expectation when non-locking reads do block on Exclusive locks. This
		// distinction is only meaningful when testing for None(serializable) mode
		// interactions with Exclusive lock modes.
		expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions bool
	}{
		// A. Held lock mode is "Shared".
		// A1. Shared lock mode is doesn't conflict with non-locking reads.
		// A1a. Non-locking read from a serializable transaction.
		{
			desc:            "non-locking read(serializable), held shared lock",
			heldLockMode:    lock.MakeModeShared(),
			toCheckLockMode: lock.MakeModeNone(tsLock, isolation.Serializable),
			exp:             false,
			expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions: false,
		},
		// A1b. Non-locking read from a SI transaction.
		{
			desc:            "non-locking read(snapshot), held shared lock",
			heldLockMode:    lock.MakeModeShared(),
			toCheckLockMode: lock.MakeModeNone(tsLock, isolation.Snapshot),
			exp:             false,
			expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions: false,
		},
		// A1c. Non-locking read from a read-committed transaction.
		{
			desc:            "non-locking read(read committed), held shared lock",
			heldLockMode:    lock.MakeModeShared(),
			toCheckLockMode: lock.MakeModeNone(tsLock, isolation.ReadCommitted),
			exp:             false,
			expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions: false,
		},
		// A2. Shared lock mode doesn't conflict with itself.
		{
			desc:            "shared lock acquisition, held shared lock",
			heldLockMode:    lock.MakeModeShared(),
			toCheckLockMode: lock.MakeModeShared(),
			exp:             false,
			expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions: false,
		},
		// A3. Shared lock mode doesn't conflict with an Update lock mode.
		{
			desc:            "update lock acquisition, held shared lock",
			heldLockMode:    lock.MakeModeShared(),
			toCheckLockMode: lock.MakeModeUpdate(),
			exp:             false,
			expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions: false,
		},
		// A4. Shared lock mode conflicts with Exclusive lock mode.
		{
			desc:            "exclusive lock acquisition, held shared lock",
			heldLockMode:    lock.MakeModeShared(),
			toCheckLockMode: lock.MakeModeExclusive(tsLock),
			exp:             true,
			expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions: true,
		},
		// A5. Shared lock mode conflicts with Intent lock mode.
		{
			desc:            "update lock acquisition, held shared lock",
			heldLockMode:    lock.MakeModeShared(),
			toCheckLockMode: lock.MakeModeIntent(tsLock),
			exp:             true,
			expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions: true,
		},
		// B. Held lock mode is "Update".
		// B1. Update lock mode doesn't conflict with non-locking reads.
		// B1a. Non-locking read from a serializable transaction.
		{
			desc:            "non-locking read(serializable), held update lock",
			heldLockMode:    lock.MakeModeUpdate(),
			toCheckLockMode: lock.MakeModeNone(tsLock, isolation.Serializable),
			exp:             false,
			expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions: false,
		},
		// B1b. Non-locking read from a SI transaction.
		{
			desc:            "non-locking read(snapshot), held update lock",
			heldLockMode:    lock.MakeModeUpdate(),
			toCheckLockMode: lock.MakeModeNone(tsLock, isolation.Snapshot),
			exp:             false,
			expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions: false,
		},
		// B1c. Non-locking read from a read-committed transaction.
		{
			desc:            "non-locking read(snapshot), held update lock",
			heldLockMode:    lock.MakeModeUpdate(),
			toCheckLockMode: lock.MakeModeNone(tsLock, isolation.ReadCommitted),
			exp:             false,
			expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions: false,
		},
		// B2. Update lock mode doesn't conflict with Shared lock mode.
		{
			desc:            "shared lock acquisition, held update lock",
			heldLockMode:    lock.MakeModeUpdate(),
			toCheckLockMode: lock.MakeModeShared(),
			exp:             false,
			expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions: false,
		},
		// B3. Update lock mode conflicts with a concurrent attempt to acquire an
		// Update lock.
		{
			desc:            "update lock acquisition, held update lock",
			heldLockMode:    lock.MakeModeUpdate(),
			toCheckLockMode: lock.MakeModeUpdate(),
			exp:             true,
			expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions: true,
		},
		// B4. Update lock mode conflicts with Exclusive lock mode.
		{
			desc:            "exclusive lock acquisition, held update lock",
			heldLockMode:    lock.MakeModeUpdate(),
			toCheckLockMode: lock.MakeModeExclusive(tsLock),
			exp:             true,
			expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions: true,
		},
		// B5. Update lock mode conflicts with Intent lock mode.
		{
			desc:            "update lock acquisition, held shared lock",
			heldLockMode:    lock.MakeModeUpdate(),
			toCheckLockMode: lock.MakeModeIntent(tsLock),
			exp:             true,
			expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions: true,
		},

		// C. Held lock mode is "Exclusive".
		// C1. Non-locking reads below the timestamp of Exclusive locks should never
		// conflict.
		// C1a. Non-locking read from a serializable transaction.
		{
			desc:            "non-locking read(serializable) at lower timestamp, held exclusive lock",
			heldLockMode:    lock.MakeModeExclusive(tsLock),
			toCheckLockMode: lock.MakeModeNone(tsBelow, isolation.Serializable),
			exp:             false,
			expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions: false,
		},
		// C1b. Non-locking read from a SI transaction.
		{
			desc:            "non-locking read(snapshot) at lower timestamp, held exclusive lock",
			heldLockMode:    lock.MakeModeExclusive(tsLock),
			toCheckLockMode: lock.MakeModeNone(tsBelow, isolation.Snapshot),
			exp:             false,
			expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions: false,
		},
		// C1b. Non-locking read from a read-committed transaction.
		{
			desc:            "non-locking read(read-committed) at lower timestamp, held exclusive lock",
			heldLockMode:    lock.MakeModeExclusive(tsLock),
			toCheckLockMode: lock.MakeModeNone(tsBelow, isolation.ReadCommitted),
			exp:             false,
			expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions: false,
		},
		// C2. Non-locking reads, at the same timestamp at which an Exclusive lock
		// is held, conflict depending on the value of the
		// ExclusiveLocksBlockNonLockingReadsFromSerializableTransactions cluster
		// setting in conjunction with the non-locking read's isolation level.
		// C2a. Non-locking read from a serializable transaction.
		{
			desc:            "non-locking read(serializable) at lock timestamp, held exclusive lock",
			heldLockMode:    lock.MakeModeExclusive(tsLock),
			toCheckLockMode: lock.MakeModeNone(tsLock, isolation.Serializable),
			exp:             false,
			expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions: true,
		},
		// C2b. Non-locking read from a SI transaction.
		{
			desc:            "non-locking read(serializable) at lock timestamp, held exclusive lock",
			heldLockMode:    lock.MakeModeExclusive(tsLock),
			toCheckLockMode: lock.MakeModeNone(tsLock, isolation.Snapshot),
			exp:             false,
			expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions: false,
		},
		// C2c. Non-locking read from a RC transaction.
		{
			desc:            "non-locking read(read-committed) at lock timestamp, held exclusive lock",
			heldLockMode:    lock.MakeModeExclusive(tsLock),
			toCheckLockMode: lock.MakeModeNone(tsLock, isolation.ReadCommitted),
			exp:             false,
			expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions: false,
		},
		// C3. Ditto for non-locking reads above the timestamp of the Exclusive
		// lock.
		// C3a. Non-locking read from a serializable transaction.
		{
			desc:            "non-locking read(serializable) above lock timestamp, held exclusive lock",
			heldLockMode:    lock.MakeModeExclusive(tsLock),
			toCheckLockMode: lock.MakeModeNone(tsAbove, isolation.Serializable),
			exp:             false,
			expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions: true,
		},
		// C3b. Non-locking read from a SI transaction.
		{
			desc:            "non-locking read(snapshot) above lock timestamp, held exclusive lock",
			heldLockMode:    lock.MakeModeExclusive(tsLock),
			toCheckLockMode: lock.MakeModeNone(tsAbove, isolation.Snapshot),
			exp:             false,
			expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions: false,
		},
		// C3c. Non-locking read from a RC transaction.
		{
			desc:            "non-locking read(read-committed) above lock timestamp, held exclusive lock",
			heldLockMode:    lock.MakeModeExclusive(tsLock),
			toCheckLockMode: lock.MakeModeNone(tsAbove, isolation.ReadCommitted),
			exp:             false,
			expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions: false,
		},
		// C4. Exclusive lock mode conflicts with Shared lock mode.
		{
			desc:            "shared lock acquisition, held exclusive lock",
			heldLockMode:    lock.MakeModeExclusive(tsLock),
			toCheckLockMode: lock.MakeModeShared(),
			exp:             true,
			expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions: true,
		},
		// C5. Exclusive lock mode conflicts with Update lock mode.
		{
			desc:            "update lock acquisition, held exclusive lock",
			heldLockMode:    lock.MakeModeExclusive(tsLock),
			toCheckLockMode: lock.MakeModeUpdate(),
			exp:             true,
			expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions: true,
		},
		// C6. Exclusive lock mode conflicts with Exclusive locks at lower
		// timestamps.
		{
			desc:            "exclusive lock acquisition, held exclusive lock",
			heldLockMode:    lock.MakeModeExclusive(tsLock),
			toCheckLockMode: lock.MakeModeExclusive(tsBelow),
			exp:             true,
			expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions: true,
		},
		// C7. Ditto for the "at timestamp" case.
		{
			desc:            "exclusive lock acquisition, held exclusive lock",
			heldLockMode:    lock.MakeModeExclusive(tsLock),
			toCheckLockMode: lock.MakeModeExclusive(tsLock),
			exp:             true,
			expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions: true,
		},
		// C8. As well as the "above timestamp" case.
		{
			desc:            "exclusive lock acquisition, held exclusive lock",
			heldLockMode:    lock.MakeModeExclusive(tsLock),
			toCheckLockMode: lock.MakeModeExclusive(tsAbove),
			exp:             true,
			expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions: true,
		},
		// C9. Exclusive lock mode conflicts with Intent lock mode at lower
		// timestamps.
		{
			desc:            "intent lock mode acquisition, held exclusive lock",
			heldLockMode:    lock.MakeModeExclusive(tsLock),
			toCheckLockMode: lock.MakeModeIntent(tsBelow),
			exp:             true,
			expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions: true,
		},
		// C10. Ditto for the "at timestamp" case.
		{
			desc:            "intent lock mode acquisition, held exclusive lock",
			heldLockMode:    lock.MakeModeExclusive(tsLock),
			toCheckLockMode: lock.MakeModeIntent(tsLock),
			exp:             true,
			expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions: true,
		},
		// C11. As well as the "above timestamp" case.
		{
			desc:            "intent lock mode acquisition, held exclusive lock",
			heldLockMode:    lock.MakeModeExclusive(tsLock),
			toCheckLockMode: lock.MakeModeIntent(tsAbove),
			exp:             true,
			expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions: true,
		},

		// D. Held lock mode is "Intent".
		// D1. Non-locking reads below the timestamp of the Intent do not conflict.
		// D1a. Non-locking read belongs to a serializable transaction.
		{
			desc:            "non-locking(serializable) read at lower timestamp, held intent",
			heldLockMode:    lock.MakeModeIntent(tsLock),
			toCheckLockMode: lock.MakeModeNone(tsBelow, isolation.Serializable),
			exp:             false,
			expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions: false,
		},
		// D1b. Non-locking read belongs to a SI transaction.
		{
			desc:            "non-locking(snapshot) read at lower timestamp, held intent",
			heldLockMode:    lock.MakeModeIntent(tsLock),
			toCheckLockMode: lock.MakeModeNone(tsBelow, isolation.Snapshot),
			exp:             false,
			expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions: false,
		},
		// D1c. Non-locking read belongs to a RC transaction.
		{
			desc:            "non-locking(read-committed) read at lower timestamp, held intent",
			heldLockMode:    lock.MakeModeIntent(tsLock),
			toCheckLockMode: lock.MakeModeNone(tsBelow, isolation.ReadCommitted),
			exp:             false,
			expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions: false,
		},
		// D2. However, non-locking reads at the timestamp of the Intent conflict,
		// regardless its isolation level.
		// D2a. Non-locking read belongs to a serializable transaction.
		{
			desc:            "non-locking read(serializable) at lock timestamp, held intent",
			heldLockMode:    lock.MakeModeIntent(tsLock),
			toCheckLockMode: lock.MakeModeNone(tsLock, isolation.Serializable),
			exp:             true,
			expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions: true,
		},
		// D2b. Non-locking read belongs to a SI transaction.
		{
			desc:            "non-locking read(snapshot) at lock timestamp, held intent",
			heldLockMode:    lock.MakeModeIntent(tsLock),
			toCheckLockMode: lock.MakeModeNone(tsLock, isolation.Snapshot),
			exp:             true,
			expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions: true,
		},
		// D2c. Non-locking read belongs to a RC transaction.
		{
			desc:            "non-locking read(read-committed) at lock timestamp, held intent",
			heldLockMode:    lock.MakeModeIntent(tsLock),
			toCheckLockMode: lock.MakeModeNone(tsLock, isolation.ReadCommitted),
			exp:             true,
			expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions: true,
		},
		// D3. Ditto for non-locking reads above the Intent's timestamp.
		// D3a. Non-locking read belongs to a serializable transaction.
		{
			desc:            "non-locking read(serializable) at lock timestamp, held intent",
			heldLockMode:    lock.MakeModeIntent(tsLock),
			toCheckLockMode: lock.MakeModeNone(tsAbove, isolation.Serializable),
			exp:             true,
			expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions: true,
		},
		// D3b. Non-locking read belongs to a SI transaction.
		{
			desc:            "non-locking read(snapshot) at lock timestamp, held intent",
			heldLockMode:    lock.MakeModeIntent(tsLock),
			toCheckLockMode: lock.MakeModeNone(tsAbove, isolation.Snapshot),
			exp:             true,
			expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions: true,
		},
		// D3b. Non-locking read belongs to a RC transaction.
		{
			desc:            "non-locking read(read-committed) at lock timestamp, held intent",
			heldLockMode:    lock.MakeModeIntent(tsLock),
			toCheckLockMode: lock.MakeModeNone(tsAbove, isolation.ReadCommitted),
			exp:             true,
			expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions: true,
		},
		// D4. Intent lock mode conflicts with Shared lock mode.
		{
			desc:            "shared lock acquisition, held intent",
			heldLockMode:    lock.MakeModeIntent(tsLock),
			toCheckLockMode: lock.MakeModeShared(),
			exp:             true,
			expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions: true,
		},
		// D5. Intent lock mode conflicts with Update lock mode.
		{
			desc:            "update lock acquisition, held intent",
			heldLockMode:    lock.MakeModeIntent(tsLock),
			toCheckLockMode: lock.MakeModeUpdate(),
			exp:             true,
			expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions: true,
		},
		// D6. Intent lock mode conflicts with Exclusive locks at lower
		// timestamps.
		{
			desc:            "exclusive lock acquisition, held intent",
			heldLockMode:    lock.MakeModeIntent(tsLock),
			toCheckLockMode: lock.MakeModeExclusive(tsBelow),
			exp:             true,
			expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions: true,
		},
		// D7. Ditto for the "at timestamp" case.
		{
			desc:            "exclusive lock acquisition, held intent",
			heldLockMode:    lock.MakeModeIntent(tsLock),
			toCheckLockMode: lock.MakeModeExclusive(tsLock),
			exp:             true,
			expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions: true,
		},
		// D8. As well as the "above timestamp" case.
		{
			desc:            "exclusive lock acquisition, held intent",
			heldLockMode:    lock.MakeModeIntent(tsLock),
			toCheckLockMode: lock.MakeModeExclusive(tsAbove),
			exp:             true,
			expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions: true,
		},
		// D9. Intent lock mode conflicts with another Intent lock mode at a
		// lower timestamp.
		{
			desc:            "intent lock mode acquisition, held intent",
			heldLockMode:    lock.MakeModeIntent(tsLock),
			toCheckLockMode: lock.MakeModeIntent(tsBelow),
			exp:             true,
			expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions: true,
		},
		// D10. Ditto for the "at timestamp" case.
		{
			desc:            "intent lock mode acquisition, held intent",
			heldLockMode:    lock.MakeModeIntent(tsLock),
			toCheckLockMode: lock.MakeModeIntent(tsLock),
			exp:             true,
			expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions: true,
		},
		// D11. As well as the "above timestamp" case.
		{
			desc:            "intent lock mode acquisition, held intent",
			heldLockMode:    lock.MakeModeIntent(tsLock),
			toCheckLockMode: lock.MakeModeIntent(tsAbove),
			exp:             true,
			expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions: true,
		},
	}

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()

	for _, tc := range testCases {
		testutils.RunTrueAndFalse(t,
			"exclusive-locks-block-non-locking-reads-override",
			func(t *testing.T, exclusiveLocksBlockNonLockingReadsOverride bool) {
				t.Run(tc.desc, func(t *testing.T) {
					lock.ExclusiveLocksBlockNonLockingReadsFromSerializableTransactions.Override(
						ctx, &st.SV, exclusiveLocksBlockNonLockingReadsOverride,
					)
					exp := tc.exp
					if exclusiveLocksBlockNonLockingReadsOverride {
						exp = tc.expExclusiveLocksBlockNonLockingReadsFromSerializableTransactions
					}

					require.Equal(
						t, exp, tc.heldLockMode.Conflicts(&st.SV, &tc.toCheckLockMode),
					)
				})
			})
	}
}
