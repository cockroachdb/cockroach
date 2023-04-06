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
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestCheckLockConflicts ensures the lock conflict semantics, as
// illustrated in the compatibility matrix in locking.pb.go, are upheld.
func TestCheckLockConflicts(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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
		// interactions with Exclusive(serializable) lock modes.
		expExclusiveLocksBlockNonLockingReads bool
	}{
		// A. Held lock mode is "Shared".
		// A1. Shared lock mode is doesn't conflict with non-locking reads.
		// A1a. Non-locking read from a serializable transaction.
		{
			desc:                                  "non-locking read(serializable), held shared lock",
			heldLockMode:                          lock.MakeModeShared(),
			toCheckLockMode:                       lock.MakeModeNone(tsLock, isolation.Serializable),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// A1b. Non-locking read from a SI transaction.
		{
			desc:                                  "non-locking read(snapshot), held shared lock",
			heldLockMode:                          lock.MakeModeShared(),
			toCheckLockMode:                       lock.MakeModeNone(tsLock, isolation.Snapshot),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// A1c. Non-locking read from a read-committed transaction.
		{
			desc:                                  "non-locking read(read committed), held shared lock",
			heldLockMode:                          lock.MakeModeShared(),
			toCheckLockMode:                       lock.MakeModeNone(tsLock, isolation.ReadCommitted),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// A2. Shared lock mode doesn't conflict with itself.
		{
			desc:                                  "shared lock acquisition, held shared lock",
			heldLockMode:                          lock.MakeModeShared(),
			toCheckLockMode:                       lock.MakeModeShared(),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// A3. Shared lock mode doesn't conflict with an Update lock mode.
		{
			desc:                                  "update lock acquisition, held shared lock",
			heldLockMode:                          lock.MakeModeShared(),
			toCheckLockMode:                       lock.MakeModeUpdate(),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// A4. Shared lock mode conflicts with Exclusive lock mode.
		// A4a. Exclusive lock from a serializable transaction.
		{
			desc:                                  "exclusive lock(serializable) acquisition, held shared lock",
			heldLockMode:                          lock.MakeModeShared(),
			toCheckLockMode:                       lock.MakeModeExclusive(tsLock, isolation.Serializable),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: true,
		},
		// A4b. Exclusive lock from a SI transaction.
		{
			desc:                                  "exclusive lock(snapshot) acquisition, held shared lock",
			heldLockMode:                          lock.MakeModeShared(),
			toCheckLockMode:                       lock.MakeModeExclusive(tsLock, isolation.Snapshot),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: true,
		},
		// A4c. Exclusive lock from a RC transaction.
		{
			desc:                                  "exclusive lock(read committed) acquisition, held shared lock",
			heldLockMode:                          lock.MakeModeShared(),
			toCheckLockMode:                       lock.MakeModeExclusive(tsLock, isolation.ReadCommitted),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: true,
		},
		// A5. Shared lock mode conflicts with Intent lock mode.
		{
			desc:                                  "update lock acquisition, held shared lock",
			heldLockMode:                          lock.MakeModeShared(),
			toCheckLockMode:                       lock.MakeModeIntent(tsLock),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: true,
		},
		// B. Held lock mode is "Update".
		// B1. Update lock mode doesn't conflict with non-locking reads.
		// B1a. Non-locking read from a serializable transaction.
		{
			desc:                                  "non-locking read(serializable), held update lock",
			heldLockMode:                          lock.MakeModeUpdate(),
			toCheckLockMode:                       lock.MakeModeNone(tsLock, isolation.Serializable),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// B1b. Non-locking read from a SI transaction.
		{
			desc:                                  "non-locking read(snapshot), held update lock",
			heldLockMode:                          lock.MakeModeUpdate(),
			toCheckLockMode:                       lock.MakeModeNone(tsLock, isolation.Snapshot),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// B1c. Non-locking read from a read-committed transaction.
		{
			desc:                                  "non-locking read(read committed), held update lock",
			heldLockMode:                          lock.MakeModeUpdate(),
			toCheckLockMode:                       lock.MakeModeNone(tsLock, isolation.ReadCommitted),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// B2. Update lock mode doesn't conflict with Shared lock mode.
		{
			desc:                                  "shared lock acquisition, held update lock",
			heldLockMode:                          lock.MakeModeUpdate(),
			toCheckLockMode:                       lock.MakeModeShared(),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// B3. Update lock mode conflicts with a concurrent attempt to acquire an
		// Update lock.
		{
			desc:                                  "update lock acquisition, held update lock",
			heldLockMode:                          lock.MakeModeUpdate(),
			toCheckLockMode:                       lock.MakeModeUpdate(),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: true,
		},
		// B4. Update lock mode conflicts with Exclusive lock mode.
		// B4a. Exclusive lock from a serializable transaction.
		{
			desc:                                  "exclusive lock(serializable) acquisition, held update lock",
			heldLockMode:                          lock.MakeModeUpdate(),
			toCheckLockMode:                       lock.MakeModeExclusive(tsLock, isolation.Serializable),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: true,
		},
		// B4b. Exclusive lock from a SI transaction.
		{
			desc:                                  "exclusive lock(snapshot) acquisition, held update lock",
			heldLockMode:                          lock.MakeModeUpdate(),
			toCheckLockMode:                       lock.MakeModeExclusive(tsLock, isolation.Snapshot),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: true,
		},
		// B4c. Exclusive lock from a rc transaction.
		{
			desc:                                  "exclusive lock(read committed) acquisition, held update lock",
			heldLockMode:                          lock.MakeModeUpdate(),
			toCheckLockMode:                       lock.MakeModeExclusive(tsLock, isolation.ReadCommitted),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: true,
		},
		// B5. Update lock mode conflicts with Intent lock mode.
		{
			desc:                                  "update lock acquisition, held shared lock",
			heldLockMode:                          lock.MakeModeUpdate(),
			toCheckLockMode:                       lock.MakeModeIntent(tsLock),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: true,
		},

		// C. Held lock mode is "Exclusive" by a serializable transaction.
		// C1. Non-locking reads below the timestamp of Exclusive locks should never
		// conflict.
		// C1a. Non-locking read from a serializable transaction.
		{
			desc:                                  "non-locking read(serializable) at lower timestamp, held exclusive(serializable) lock",
			heldLockMode:                          lock.MakeModeExclusive(tsLock, isolation.Serializable),
			toCheckLockMode:                       lock.MakeModeNone(tsBelow, isolation.Serializable),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// C1b. Non-locking read from a SI transaction.
		{
			desc:                                  "non-locking read(snapshot) at lower timestamp, held exclusive(serializable) lock",
			heldLockMode:                          lock.MakeModeExclusive(tsLock, isolation.Serializable),
			toCheckLockMode:                       lock.MakeModeNone(tsBelow, isolation.Snapshot),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// C1b. Non-locking read from a read-committed transaction.
		{
			desc:                                  "non-locking read(read-committed) at lower timestamp, held exclusive(serializable) lock",
			heldLockMode:                          lock.MakeModeExclusive(tsLock, isolation.Serializable),
			toCheckLockMode:                       lock.MakeModeNone(tsBelow, isolation.ReadCommitted),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// C2. Non-locking reads, at the same timestamp at which an Exclusive lock
		// is held, conflict depending on the value of the
		// ExclusiveLocksBlockNonLockingReads cluster
		// setting in conjunction with the non-locking read's isolation level.
		// C2a. Non-locking read from a serializable transaction.
		{
			desc:                                  "non-locking read(serializable) at lock timestamp, held exclusive(serializable) lock",
			heldLockMode:                          lock.MakeModeExclusive(tsLock, isolation.Serializable),
			toCheckLockMode:                       lock.MakeModeNone(tsLock, isolation.Serializable),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: true,
		},
		// C2b. Non-locking read from a SI transaction.
		{
			desc:                                  "non-locking read(serializable) at lock timestamp, held exclusive(serializable) lock",
			heldLockMode:                          lock.MakeModeExclusive(tsLock, isolation.Serializable),
			toCheckLockMode:                       lock.MakeModeNone(tsLock, isolation.Snapshot),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// C2c. Non-locking read from a RC transaction.
		{
			desc:                                  "non-locking read(read-committed) at lock timestamp, held exclusive(serializable) lock",
			heldLockMode:                          lock.MakeModeExclusive(tsLock, isolation.Serializable),
			toCheckLockMode:                       lock.MakeModeNone(tsLock, isolation.ReadCommitted),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// C3. Ditto for non-locking reads above the timestamp of the Exclusive
		// lock.
		// C3a. Non-locking read from a serializable transaction.
		{
			desc:                                  "non-locking read(serializable) above lock timestamp, held exclusive(serializable) lock",
			heldLockMode:                          lock.MakeModeExclusive(tsLock, isolation.Serializable),
			toCheckLockMode:                       lock.MakeModeNone(tsAbove, isolation.Serializable),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: true,
		},
		// C3b. Non-locking read from a SI transaction.
		{
			desc:                                  "non-locking read(snapshot) above lock timestamp, held exclusive(serializable) lock",
			heldLockMode:                          lock.MakeModeExclusive(tsLock, isolation.Serializable),
			toCheckLockMode:                       lock.MakeModeNone(tsAbove, isolation.Snapshot),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// C3c. Non-locking read from a RC transaction.
		{
			desc:                                  "non-locking read(read-committed) above lock timestamp, held exclusive(serializable) lock",
			heldLockMode:                          lock.MakeModeExclusive(tsLock, isolation.Serializable),
			toCheckLockMode:                       lock.MakeModeNone(tsAbove, isolation.ReadCommitted),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// C4. Exclusive lock mode conflicts with Shared lock mode.
		{
			desc:                                  "shared lock acquisition, held exclusive(serializable) lock",
			heldLockMode:                          lock.MakeModeExclusive(tsLock, isolation.Serializable),
			toCheckLockMode:                       lock.MakeModeShared(),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: true,
		},
		// C5. Exclusive lock mode conflicts with Update lock mode.
		{
			desc:                                  "update lock acquisition, held exclusive(serializable) lock",
			heldLockMode:                          lock.MakeModeExclusive(tsLock, isolation.Serializable),
			toCheckLockMode:                       lock.MakeModeUpdate(),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: true,
		},
		// C6. Exclusive lock mode conflicts with Exclusive locks at lower
		// timestamps.
		{
			desc:                                  "exclusive lock acquisition, held exclusive(serializable) lock",
			heldLockMode:                          lock.MakeModeExclusive(tsLock, isolation.Serializable),
			toCheckLockMode:                       lock.MakeModeExclusive(tsBelow, isolation.Serializable),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: true,
		},
		// C7. Ditto for the "at timestamp" case.
		{
			desc:                                  "exclusive lock acquisition, held exclusive(serializable) lock",
			heldLockMode:                          lock.MakeModeExclusive(tsLock, isolation.Serializable),
			toCheckLockMode:                       lock.MakeModeExclusive(tsLock, isolation.Serializable),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: true,
		},
		// C8. As well as the "above timestamp" case.
		{
			desc:                                  "exclusive lock acquisition, held exclusive(serializable) lock",
			heldLockMode:                          lock.MakeModeExclusive(tsLock, isolation.Serializable),
			toCheckLockMode:                       lock.MakeModeExclusive(tsAbove, isolation.Serializable),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: true,
		},
		// C9. Exclusive lock mode conflicts with Intent lock mode at lower
		// timestamps.
		{
			desc:                                  "intent lock mode acquisition, held exclusive(serializable) lock",
			heldLockMode:                          lock.MakeModeExclusive(tsLock, isolation.Serializable),
			toCheckLockMode:                       lock.MakeModeIntent(tsBelow),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: true,
		},
		// C10. Ditto for the "at timestamp" case.
		{
			desc:                                  "intent lock mode acquisition, held exclusive(serializable) lock",
			heldLockMode:                          lock.MakeModeExclusive(tsLock, isolation.Serializable),
			toCheckLockMode:                       lock.MakeModeIntent(tsLock),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: true,
		},
		// C11. As well as the "above timestamp" case.
		{
			desc:                                  "intent lock mode acquisition, held exclusive(serializable) lock",
			heldLockMode:                          lock.MakeModeExclusive(tsLock, isolation.Serializable),
			toCheckLockMode:                       lock.MakeModeIntent(tsAbove),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: true,
		},

		// D. Held lock mode is "Intent".
		// D1. Non-locking reads below the timestamp of the Intent do not conflict.
		// D1a. Non-locking read belongs to a serializable transaction.
		{
			desc:                                  "non-locking(serializable) read at lower timestamp, held intent",
			heldLockMode:                          lock.MakeModeIntent(tsLock),
			toCheckLockMode:                       lock.MakeModeNone(tsBelow, isolation.Serializable),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// D1b. Non-locking read belongs to a SI transaction.
		{
			desc:                                  "non-locking(snapshot) read at lower timestamp, held intent",
			heldLockMode:                          lock.MakeModeIntent(tsLock),
			toCheckLockMode:                       lock.MakeModeNone(tsBelow, isolation.Snapshot),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// D1c. Non-locking read belongs to a RC transaction.
		{
			desc:                                  "non-locking(read-committed) read at lower timestamp, held intent",
			heldLockMode:                          lock.MakeModeIntent(tsLock),
			toCheckLockMode:                       lock.MakeModeNone(tsBelow, isolation.ReadCommitted),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// D2. However, non-locking reads at the timestamp of the Intent conflict,
		// regardless its isolation level.
		// D2a. Non-locking read belongs to a serializable transaction.
		{
			desc:                                  "non-locking read(serializable) at lock timestamp, held intent",
			heldLockMode:                          lock.MakeModeIntent(tsLock),
			toCheckLockMode:                       lock.MakeModeNone(tsLock, isolation.Serializable),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: true,
		},
		// D2b. Non-locking read belongs to a SI transaction.
		{
			desc:                                  "non-locking read(snapshot) at lock timestamp, held intent",
			heldLockMode:                          lock.MakeModeIntent(tsLock),
			toCheckLockMode:                       lock.MakeModeNone(tsLock, isolation.Snapshot),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: true,
		},
		// D2c. Non-locking read belongs to a RC transaction.
		{
			desc:                                  "non-locking read(read-committed) at lock timestamp, held intent",
			heldLockMode:                          lock.MakeModeIntent(tsLock),
			toCheckLockMode:                       lock.MakeModeNone(tsLock, isolation.ReadCommitted),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: true,
		},
		// D3. Ditto for non-locking reads above the Intent's timestamp.
		// D3a. Non-locking read belongs to a serializable transaction.
		{
			desc:                                  "non-locking read(serializable) at lock timestamp, held intent",
			heldLockMode:                          lock.MakeModeIntent(tsLock),
			toCheckLockMode:                       lock.MakeModeNone(tsAbove, isolation.Serializable),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: true,
		},
		// D3b. Non-locking read belongs to a SI transaction.
		{
			desc:                                  "non-locking read(snapshot) at lock timestamp, held intent",
			heldLockMode:                          lock.MakeModeIntent(tsLock),
			toCheckLockMode:                       lock.MakeModeNone(tsAbove, isolation.Snapshot),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: true,
		},
		// D3b. Non-locking read belongs to a RC transaction.
		{
			desc:                                  "non-locking read(read-committed) at lock timestamp, held intent",
			heldLockMode:                          lock.MakeModeIntent(tsLock),
			toCheckLockMode:                       lock.MakeModeNone(tsAbove, isolation.ReadCommitted),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: true,
		},
		// D4. Intent lock mode conflicts with Shared lock mode.
		{
			desc:                                  "shared lock acquisition, held intent",
			heldLockMode:                          lock.MakeModeIntent(tsLock),
			toCheckLockMode:                       lock.MakeModeShared(),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: true,
		},
		// D5. Intent lock mode conflicts with Update lock mode.
		{
			desc:                                  "update lock acquisition, held intent",
			heldLockMode:                          lock.MakeModeIntent(tsLock),
			toCheckLockMode:                       lock.MakeModeUpdate(),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: true,
		},
		// D6. Intent lock mode conflicts with Exclusive locks at lower
		// timestamps.
		// D6a. exclusive lock from a serializable transaction.
		{
			desc:                                  "exclusive lock(serializable) acquisition, held intent",
			heldLockMode:                          lock.MakeModeIntent(tsLock),
			toCheckLockMode:                       lock.MakeModeExclusive(tsBelow, isolation.Serializable),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: true,
		},
		// D6b. exclusive lock from a SI transaction.
		{
			desc:                                  "exclusive lock(snapshot) acquisition, held intent",
			heldLockMode:                          lock.MakeModeIntent(tsLock),
			toCheckLockMode:                       lock.MakeModeExclusive(tsBelow, isolation.Snapshot),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: true,
		},
		// D6c. exclusive lock from a RC transaction.
		{
			desc:                                  "exclusive lock(read committed) acquisition, held intent",
			heldLockMode:                          lock.MakeModeIntent(tsLock),
			toCheckLockMode:                       lock.MakeModeExclusive(tsBelow, isolation.ReadCommitted),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: true,
		},
		// D7. Ditto for the "at timestamp" case.
		// D7a. Exclusive lock from a serializable transaction.
		{
			desc:                                  "exclusive lock(serializable) acquisition, held intent",
			heldLockMode:                          lock.MakeModeIntent(tsLock),
			toCheckLockMode:                       lock.MakeModeExclusive(tsLock, isolation.Serializable),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: true,
		},
		// D7b. Exclusive lock from a SI transaction.
		{
			desc:                                  "exclusive lock(snapshot) acquisition, held intent",
			heldLockMode:                          lock.MakeModeIntent(tsLock),
			toCheckLockMode:                       lock.MakeModeExclusive(tsLock, isolation.Snapshot),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: true,
		},
		// D7c. Exclusive lock from a RC transaction.
		{
			desc:                                  "exclusive lock(read committed) acquisition, held intent",
			heldLockMode:                          lock.MakeModeIntent(tsLock),
			toCheckLockMode:                       lock.MakeModeExclusive(tsLock, isolation.ReadCommitted),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: true,
		},
		// D8. As well as the "above timestamp" case.
		// D8a. Exclusive lock from a serializable transaction.
		{
			desc:                                  "exclusive lock(serializable) acquisition, held intent",
			heldLockMode:                          lock.MakeModeIntent(tsLock),
			toCheckLockMode:                       lock.MakeModeExclusive(tsAbove, isolation.Serializable),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: true,
		},
		// D8b. Exclusive lock from a SI transaction.
		{
			desc:                                  "exclusive lock(snapshot) acquisition, held intent",
			heldLockMode:                          lock.MakeModeIntent(tsLock),
			toCheckLockMode:                       lock.MakeModeExclusive(tsAbove, isolation.Snapshot),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: true,
		},
		// D8c. Exclusive lock from a RC transaction.
		{
			desc:                                  "exclusive lock(read committed) acquisition, held intent",
			heldLockMode:                          lock.MakeModeIntent(tsLock),
			toCheckLockMode:                       lock.MakeModeExclusive(tsAbove, isolation.ReadCommitted),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: true,
		},
		// D9. Intent lock mode conflicts with another Intent lock mode at a
		// lower timestamp.
		{
			desc:                                  "intent lock mode acquisition, held intent",
			heldLockMode:                          lock.MakeModeIntent(tsLock),
			toCheckLockMode:                       lock.MakeModeIntent(tsBelow),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: true,
		},
		// D10. Ditto for the "at timestamp" case.
		{
			desc:                                  "intent lock mode acquisition, held intent",
			heldLockMode:                          lock.MakeModeIntent(tsLock),
			toCheckLockMode:                       lock.MakeModeIntent(tsLock),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: true,
		},
		// D11. As well as the "above timestamp" case.
		{
			desc:                                  "intent lock mode acquisition, held intent",
			heldLockMode:                          lock.MakeModeIntent(tsLock),
			toCheckLockMode:                       lock.MakeModeIntent(tsAbove),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: true,
		},
	}

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()

	for _, tc := range testCases {
		testutils.RunTrueAndFalse(t,
			"exclusive-locks-block-non-locking-reads-override",
			func(t *testing.T, exclusiveLocksBlockNonLockingReadsOverride bool) {
				t.Run(tc.desc, func(t *testing.T) {
					lock.ExclusiveLocksBlockNonLockingReads.Override(
						ctx, &st.SV, exclusiveLocksBlockNonLockingReadsOverride,
					)
					exp := tc.exp
					if exclusiveLocksBlockNonLockingReadsOverride {
						exp = tc.expExclusiveLocksBlockNonLockingReads
					}

					require.Equal(
						t, exp, tc.heldLockMode.Conflicts(&st.SV, &tc.toCheckLockMode),
					)
				})
			})
	}
}

// TestExclusiveLockWeakerIsolationLevelConflicts tests conflict semantics for
// exclusive locks held by transactions that run with weaker isolation levels
// (snapshot, read committed). The interesting test cases, compared to
// serializable isolation, are interactions with non-locking reads. Non-locking
// reads never conflict with exclusive locks held by transactions that run at
// weaker isolation levels -- regardless of the timestamp at which the
// non-locking read is being performed.
func TestExclusiveLockWeakerIsolationLevelConflicts(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	makeTS := func(nanos int64) hlc.Timestamp {
		return hlc.Timestamp{
			WallTime: nanos,
		}
	}

	tsBelow := makeTS(1)
	tsLock := makeTS(2)
	tsAbove := makeTS(3)

	for _, iso := range []isolation.Level{isolation.Snapshot, isolation.ReadCommitted} {
		heldExclusiveLock := lock.MakeModeExclusive(tsLock, iso)

		testCases := []struct {
			name        string
			toCheckMode lock.Mode
			conflicts   bool
		}{
			// Non-locking reads at lower timestamp.
			{
				name:        "non-locking read(serializable), lower ts",
				toCheckMode: lock.MakeModeNone(tsBelow, isolation.Serializable),
				conflicts:   false,
			},
			{
				name:        "non-locking read(snapshot), lower ts",
				toCheckMode: lock.MakeModeNone(tsBelow, isolation.Snapshot),
				conflicts:   false,
			},
			{
				name:        "non-locking read(read committed), lower ts",
				toCheckMode: lock.MakeModeNone(tsBelow, isolation.ReadCommitted),
				conflicts:   false,
			},
			// Non-locking reads at lock timestamp.
			{
				name:        "non-locking read(serializable), at lock ts",
				toCheckMode: lock.MakeModeNone(tsLock, isolation.Serializable),
				conflicts:   false,
			},
			{
				name:        "non-locking read(snapshot), at lock ts",
				toCheckMode: lock.MakeModeNone(tsLock, isolation.Snapshot),
				conflicts:   false,
			},
			{
				name:        "non-locking read(read committed), at lock ts",
				toCheckMode: lock.MakeModeNone(tsLock, isolation.ReadCommitted),
				conflicts:   false,
			},
			// Non-locking reads above lock timestamp.
			{
				name:        "non-locking read(serializable), above lock ts",
				toCheckMode: lock.MakeModeNone(tsAbove, isolation.Serializable),
				conflicts:   false,
			},
			{
				name:        "non-locking read(snapshot), above ts",
				toCheckMode: lock.MakeModeNone(tsAbove, isolation.Snapshot),
				conflicts:   false,
			},
			{
				name:        "non-locking read(read committed), above ts",
				toCheckMode: lock.MakeModeNone(tsAbove, isolation.ReadCommitted),
				conflicts:   false,
			},
			// Shared lock.
			{
				name:        "shared lock",
				toCheckMode: lock.MakeModeShared(),
				conflicts:   true,
			},
			// Update lock.
			{
				name:        "shared lock",
				toCheckMode: lock.MakeModeUpdate(),
				conflicts:   true,
			},
			// Exclusive locks.
			{
				name:        "exclusive lock(serializable)",
				toCheckMode: lock.MakeModeExclusive(tsBelow, isolation.Serializable),
				conflicts:   true,
			},
			{
				name:        "exclusive lock(snapshot)",
				toCheckMode: lock.MakeModeExclusive(tsBelow, isolation.Snapshot),
				conflicts:   true,
			},
			{
				name:        "exclusive lock(read committed)",
				toCheckMode: lock.MakeModeExclusive(tsBelow, isolation.ReadCommitted),
				conflicts:   true,
			},
			// Intent.
			{
				name:        "intent",
				toCheckMode: lock.MakeModeIntent(tsBelow),
				conflicts:   true,
			},
		}

		ctx := context.Background()
		st := cluster.MakeTestingClusterSettings()
		lock.ExclusiveLocksBlockNonLockingReads.Override(ctx, &st.SV, true)
		for _, tc := range testCases {
			t.Run(fmt.Sprintf("held exclusive lock(%s)-%s", iso.String(), tc.name), func(t *testing.T) {
				require.Equal(
					t, tc.conflicts, heldExclusiveLock.Conflicts(&st.SV, &tc.toCheckMode),
				)
			})
		}
	}
}
