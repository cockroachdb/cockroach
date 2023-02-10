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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/stretchr/testify/require"
)

// TestCheckLockCompatibility ensures the compatibility semantics, as
// illustrated in the compatibility matrix in locking.pb.go, are upheld.
func TestCheckLockCompatibility(t *testing.T) {
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
		// distinction is only meaningful when testing for None <-> Exclusive lock
		// mode interactions.
		expExclusiveLocksBlockNonLockingReads bool
	}{
		// A. Held lock mode is "Shared".
		// A1. Shared lock mode is compatible with non-locking reads.
		{
			desc:                                  "non-locking read, held shared lock",
			heldLockMode:                          lock.MakeModeShared(),
			toCheckLockMode:                       lock.MakeModeNone(tsLock),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: true,
		},
		// A2. Shared lock mode is compatible with itself.
		{
			desc:                                  "shared lock acquisition, held shared lock",
			heldLockMode:                          lock.MakeModeShared(),
			toCheckLockMode:                       lock.MakeModeShared(),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: true,
		},
		// A3. Shared lock mode is compatible with Update lock mode.
		{
			desc:                                  "update lock acquisition, held shared lock",
			heldLockMode:                          lock.MakeModeShared(),
			toCheckLockMode:                       lock.MakeModeUpdate(),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: true,
		},
		// A4. Shared lock mode is incompatible with Exclusive lock mode.
		{
			desc:                                  "exclusive lock acquisition, held shared lock",
			heldLockMode:                          lock.MakeModeShared(),
			toCheckLockMode:                       lock.MakeModeExclusive(tsLock),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// A5. Shared lock mode is incompatible with Intent lock mode.
		{
			desc:                                  "update lock acquisition, held shared lock",
			heldLockMode:                          lock.MakeModeShared(),
			toCheckLockMode:                       lock.MakeModeIntent(tsLock),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},

		// B. Held lock mode is "Update".
		// B1. Update lock mode is compatible with non-locking reads.
		{
			desc:                                  "non-locking read, held update lock",
			heldLockMode:                          lock.MakeModeUpdate(),
			toCheckLockMode:                       lock.MakeModeNone(tsLock),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: true,
		},
		// B2. Update lock mode is compatible with Shared lock mode.
		{
			desc:                                  "shared lock acquisition, held update lock",
			heldLockMode:                          lock.MakeModeUpdate(),
			toCheckLockMode:                       lock.MakeModeShared(),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: true,
		},
		// B3. Update lock mode is incompatible with a concurrent attempt to
		// acquire an Update lock.
		{
			desc:                                  "update lock acquisition, held update lock",
			heldLockMode:                          lock.MakeModeUpdate(),
			toCheckLockMode:                       lock.MakeModeUpdate(),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// B4. Update lock mode is incompatible with Exclusive lock mode.
		{
			desc:                                  "exclusive lock acquisition, held update lock",
			heldLockMode:                          lock.MakeModeUpdate(),
			toCheckLockMode:                       lock.MakeModeExclusive(tsLock),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// B5. Update lock mode is incompatible with Intent lock mode.
		{
			desc:                                  "update lock acquisition, held shared lock",
			heldLockMode:                          lock.MakeModeUpdate(),
			toCheckLockMode:                       lock.MakeModeIntent(tsLock),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},

		// C. Held lock mode is "Exclusive".
		// C1. Non-locking reads below the timestamp of Exclusive locks are always
		// compatible.
		{
			desc:                                  "non-locking read at lower timestamp, held exclusive lock",
			heldLockMode:                          lock.MakeModeExclusive(tsLock),
			toCheckLockMode:                       lock.MakeModeNone(tsBelow),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: true,
		},
		// C2. Non-locking reads, at the same timestamp at which an Exclusive lock
		// is held, are compatible depending on the value of the
		// ExclusiveLocksBlockNonLockingReads cluster setting.
		{
			desc:                                  "non-locking read at lock timestamp, held exclusive lock",
			heldLockMode:                          lock.MakeModeExclusive(tsLock),
			toCheckLockMode:                       lock.MakeModeNone(tsLock),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// C3. Non-locking reads above the timestamp of an Exclusive lock exhibit
		// the same behavior as above.
		{
			desc:                                  "non-locking read at lock timestamp, held exclusive lock",
			heldLockMode:                          lock.MakeModeExclusive(tsLock),
			toCheckLockMode:                       lock.MakeModeNone(tsAbove),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// C4. Exclusive lock mode is incompatible with Shared lock mode.
		{
			desc:                                  "shared lock acquisition, held exclusive lock",
			heldLockMode:                          lock.MakeModeExclusive(tsLock),
			toCheckLockMode:                       lock.MakeModeShared(),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// C5. Exclusive lock mode is incompatible with Update lock mode.
		{
			desc:                                  "update lock acquisition, held exclusive lock",
			heldLockMode:                          lock.MakeModeExclusive(tsLock),
			toCheckLockMode:                       lock.MakeModeUpdate(),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// C6. Exclusive lock mode is incompatible with Exclusive locks at lower
		// timestamps.
		{
			desc:                                  "exclusive lock acquisition, held exclusive lock",
			heldLockMode:                          lock.MakeModeExclusive(tsLock),
			toCheckLockMode:                       lock.MakeModeExclusive(tsBelow),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// C7. Ditto for the "at timestamp" case.
		{
			desc:                                  "exclusive lock acquisition, held exclusive lock",
			heldLockMode:                          lock.MakeModeExclusive(tsLock),
			toCheckLockMode:                       lock.MakeModeExclusive(tsLock),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// C8. As well as the "above timestamp" case.
		{
			desc:                                  "exclusive lock acquisition, held exclusive lock",
			heldLockMode:                          lock.MakeModeExclusive(tsLock),
			toCheckLockMode:                       lock.MakeModeExclusive(tsAbove),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// C9. Exclusive lock mode is incompatible with Intent lock mode at lower
		// timestamps.
		{
			desc:                                  "intent lock mode acquisition, held exclusive lock",
			heldLockMode:                          lock.MakeModeExclusive(tsLock),
			toCheckLockMode:                       lock.MakeModeIntent(tsBelow),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// C10. Ditto for the "at timestamp" case.
		{
			desc:                                  "intent lock mode acquisition, held exclusive lock",
			heldLockMode:                          lock.MakeModeExclusive(tsLock),
			toCheckLockMode:                       lock.MakeModeIntent(tsLock),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// C11. As well as the "above timestamp" case.
		{
			desc:                                  "intent lock mode acquisition, held exclusive lock",
			heldLockMode:                          lock.MakeModeExclusive(tsLock),
			toCheckLockMode:                       lock.MakeModeIntent(tsAbove),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},

		// D. Held lock mode is "Intent".
		// D1. Non-locking reads below the timestamp of the Intent are compatible.
		{
			desc:                                  "non-locking read at lower timestamp, held intent",
			heldLockMode:                          lock.MakeModeIntent(tsLock),
			toCheckLockMode:                       lock.MakeModeNone(tsBelow),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: true,
		},
		// D2. However, non-locking reads at the timestamp of the Intent are
		// incompatible.
		{
			desc:                                  "non-locking read at lock timestamp, held intent",
			heldLockMode:                          lock.MakeModeIntent(tsLock),
			toCheckLockMode:                       lock.MakeModeNone(tsLock),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// D3. Ditto for non-locking reads above the Intent's timestamp.
		{
			desc:                                  "non-locking read at lock timestamp, held intent",
			heldLockMode:                          lock.MakeModeIntent(tsLock),
			toCheckLockMode:                       lock.MakeModeNone(tsAbove),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// D4. Intent lock mode is incompatible with Shared lock mode.
		{
			desc:                                  "shared lock acquisition, held intent",
			heldLockMode:                          lock.MakeModeIntent(tsLock),
			toCheckLockMode:                       lock.MakeModeShared(),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// D5. Intent lock mode is incompatible with Update lock mode.
		{
			desc:                                  "update lock acquisition, held intent",
			heldLockMode:                          lock.MakeModeIntent(tsLock),
			toCheckLockMode:                       lock.MakeModeUpdate(),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// D6. Intent lock mode is incompatible with Exclusive locks at lower
		// timestamps.
		{
			desc:                                  "exclusive lock acquisition, held intent",
			heldLockMode:                          lock.MakeModeIntent(tsLock),
			toCheckLockMode:                       lock.MakeModeExclusive(tsBelow),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// D7. Ditto for the "at timestamp" case.
		{
			desc:                                  "exclusive lock acquisition, held intent",
			heldLockMode:                          lock.MakeModeIntent(tsLock),
			toCheckLockMode:                       lock.MakeModeExclusive(tsLock),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// D8. As well as the "above timestamp" case.
		{
			desc:                                  "exclusive lock acquisition, held intent",
			heldLockMode:                          lock.MakeModeIntent(tsLock),
			toCheckLockMode:                       lock.MakeModeExclusive(tsAbove),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// D9. Intent lock mode is incompatible with another Intent lock mode at a
		// lower timestamp.
		{
			desc:                                  "intent lock mode acquisition, held intent",
			heldLockMode:                          lock.MakeModeIntent(tsLock),
			toCheckLockMode:                       lock.MakeModeIntent(tsBelow),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// D10. Ditto for the "at timestamp" case.
		{
			desc:                                  "intent lock mode acquisition, held intent",
			heldLockMode:                          lock.MakeModeIntent(tsLock),
			toCheckLockMode:                       lock.MakeModeIntent(tsLock),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// D11. As well as the "above timestamp" case.
		{
			desc:                                  "intent lock mode acquisition, held intent",
			heldLockMode:                          lock.MakeModeIntent(tsLock),
			toCheckLockMode:                       lock.MakeModeIntent(tsAbove),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
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
						t, exp, tc.heldLockMode.CheckLockCompatibility(&st.SV, &tc.toCheckLockMode),
					)
				})
			})
	}
}
