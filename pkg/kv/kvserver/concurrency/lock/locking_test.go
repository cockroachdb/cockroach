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

	_ = tsBelow
	_ = tsAbove

	testCases := []struct {
		desc                string
		heldLockStrength    lock.Strength
		toCheckLockStrength lock.Strength
		// Expectation for when non-locking reads do not block on Exclusive locks.
		exp bool
		// Expectation when non-locking reads do block on Exclusive locks. This
		// distinction is only meaningful when testing for None <-> Exclusive lock
		// mode interactions.
		expExclusiveLocksBlockNonLockingReads bool
	}{
		// A. Held lock strength is "Shared".
		// A1. Shared lock strength is compatible with non-locking reads.
		{
			desc:                                  "non-locking read, held shared lock",
			heldLockStrength:                      lock.MakeStrength(lock.Shared),
			toCheckLockStrength:                   lock.MakeStrength(lock.None, lock.AtTimestamp(tsLock)),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: true,
		},
		// A2. Shared locking strength is compatible with itself.
		{
			desc:                                  "shared lock acquisition, held shared lock",
			heldLockStrength:                      lock.MakeStrength(lock.Shared),
			toCheckLockStrength:                   lock.MakeStrength(lock.Shared),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: true,
		},
		// A3. Shared locking strength is incompatible with Upgrade locking strength.
		{
			desc:                                  "upgrade lock acquisition, held shared lock",
			heldLockStrength:                      lock.MakeStrength(lock.Shared),
			toCheckLockStrength:                   lock.MakeStrength(lock.Upgrade),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// A4. Shared locking strength is incompatible with Exclusive locking
		// strength.
		{
			desc:                                  "exclusive lock acquisition, held shared lock",
			heldLockStrength:                      lock.MakeStrength(lock.Shared),
			toCheckLockStrength:                   lock.MakeStrength(lock.Exclusive, lock.AtTimestamp(tsLock)),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// A5. Shared locking strength is incompatible with Intent locking strength.
		{
			desc:                                  "upgrade lock acquisition, held shared lock",
			heldLockStrength:                      lock.MakeStrength(lock.Shared),
			toCheckLockStrength:                   lock.MakeStrength(lock.Intent, lock.AtTimestamp(tsLock)),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},

		// B. Held lock strength is "Upgrade".
		// B1. Upgrade lock strength is compatible with non-locking reads.
		{
			desc:                                  "non-locking read, held upgrade lock",
			heldLockStrength:                      lock.MakeStrength(lock.Upgrade),
			toCheckLockStrength:                   lock.MakeStrength(lock.None, lock.AtTimestamp(tsLock)),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: true,
		},
		// B2. Upgrade locking strength is incompatible with Shared locking strength.
		{
			desc:                                  "shared lock acquisition, held upgrade lock",
			heldLockStrength:                      lock.MakeStrength(lock.Upgrade),
			toCheckLockStrength:                   lock.MakeStrength(lock.Shared),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// B3. Upgrade locking strength is incompatible with a concurrent attempt to
		// acquire an Upgrade lock.
		{
			desc:                                  "upgrade lock acquisition, held upgrade lock",
			heldLockStrength:                      lock.MakeStrength(lock.Upgrade),
			toCheckLockStrength:                   lock.MakeStrength(lock.Upgrade),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// B4. Upgrade locking strength is incompatible with Exclusive locking
		// strength.
		{
			desc:                                  "exclusive lock acquisition, held shared lock",
			heldLockStrength:                      lock.MakeStrength(lock.Upgrade),
			toCheckLockStrength:                   lock.MakeStrength(lock.Exclusive, lock.AtTimestamp(tsLock)),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// B5. Upgrade locking strength is incompatible with Intent locking strength.
		{
			desc:                                  "upgrade lock acquisition, held shared lock",
			heldLockStrength:                      lock.MakeStrength(lock.Upgrade),
			toCheckLockStrength:                   lock.MakeStrength(lock.Intent, lock.AtTimestamp(tsLock)),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},

		// C. Held lock strength is "Exclusive".
		// C1. Non-locking reads below the timestamp of Exclusive locks are always
		// compatible.
		{
			desc:                                  "non-locking read at lower timestamp, held exclusive lock",
			heldLockStrength:                      lock.MakeStrength(lock.Exclusive, lock.AtTimestamp(tsLock)),
			toCheckLockStrength:                   lock.MakeStrength(lock.None, lock.AtTimestamp(tsBelow)),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: true,
		},
		// C2. Non-locking reads, at the same timestamp at which an Exclusive lock is
		// held, are compatible depending on the value of the
		// ExclusiveLocksBlockNonLockingReads cluster setting.
		{
			desc:                                  "non-locking read at lock timestamp, held exclusive lock",
			heldLockStrength:                      lock.MakeStrength(lock.Exclusive, lock.AtTimestamp(tsLock)),
			toCheckLockStrength:                   lock.MakeStrength(lock.None, lock.AtTimestamp(tsLock)),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// C3. Non-locking reads above the timestamp of an Exclusive lock exhibit the
		// same behavior as above.
		{
			desc:                                  "non-locking read at lock timestamp, held exclusive lock",
			heldLockStrength:                      lock.MakeStrength(lock.Exclusive, lock.AtTimestamp(tsLock)),
			toCheckLockStrength:                   lock.MakeStrength(lock.None, lock.AtTimestamp(tsAbove)),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// C4. Exclusive locks are incompatible with Shared locking strength.
		{
			desc:                                  "shared lock acquisition, held exclusive lock",
			heldLockStrength:                      lock.MakeStrength(lock.Exclusive, lock.AtTimestamp(tsLock)),
			toCheckLockStrength:                   lock.MakeStrength(lock.Shared),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// C5. Exclusive locks are incompatible with Upgrade locking strength.
		{
			desc:                                  "upgrade lock acquisition, held exclusive lock",
			heldLockStrength:                      lock.MakeStrength(lock.Exclusive, lock.AtTimestamp(tsLock)),
			toCheckLockStrength:                   lock.MakeStrength(lock.Upgrade),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// C6. Exclusive locks are incompatible with Exclusive locks at lower
		// timestamps.
		{
			desc:                                  "exclusive lock acquisition, held exclusive lock",
			heldLockStrength:                      lock.MakeStrength(lock.Exclusive, lock.AtTimestamp(tsLock)),
			toCheckLockStrength:                   lock.MakeStrength(lock.Exclusive, lock.AtTimestamp(tsBelow)),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// C7. Ditto for the "at timestamp" case.
		{
			desc:                                  "exclusive lock acquisition, held exclusive lock",
			heldLockStrength:                      lock.MakeStrength(lock.Exclusive, lock.AtTimestamp(tsLock)),
			toCheckLockStrength:                   lock.MakeStrength(lock.Exclusive, lock.AtTimestamp(tsLock)),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// C8. As well as the "above timestamp" case.
		{
			desc:                                  "exclusive lock acquisition, held exclusive lock",
			heldLockStrength:                      lock.MakeStrength(lock.Exclusive, lock.AtTimestamp(tsLock)),
			toCheckLockStrength:                   lock.MakeStrength(lock.Exclusive, lock.AtTimestamp(tsAbove)),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// C9. Exclusive locks are incompatible with Intent lock strengths at lower
		// timestamps.
		{
			desc:                                  "intent lock strength acquisition, held exclusive lock",
			heldLockStrength:                      lock.MakeStrength(lock.Exclusive, lock.AtTimestamp(tsLock)),
			toCheckLockStrength:                   lock.MakeStrength(lock.Intent, lock.AtTimestamp(tsBelow)),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// C10. Ditto for the "at timestamp" case.
		{
			desc:                                  "intent lock strength acquisition, held exclusive lock",
			heldLockStrength:                      lock.MakeStrength(lock.Exclusive, lock.AtTimestamp(tsLock)),
			toCheckLockStrength:                   lock.MakeStrength(lock.Intent, lock.AtTimestamp(tsLock)),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// C11. As well as the "above timestamp" case.
		{
			desc:                                  "intent lock strength acquisition, held exclusive lock",
			heldLockStrength:                      lock.MakeStrength(lock.Exclusive, lock.AtTimestamp(tsLock)),
			toCheckLockStrength:                   lock.MakeStrength(lock.Intent, lock.AtTimestamp(tsAbove)),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},

		// D. Held lock strength is "Intent".
		// D1. Non-locking reads below the timestamp of the Intent are compatible.
		{
			desc:                                  "non-locking read at lower timestamp, held intent",
			heldLockStrength:                      lock.MakeStrength(lock.Exclusive, lock.AtTimestamp(tsLock)),
			toCheckLockStrength:                   lock.MakeStrength(lock.None, lock.AtTimestamp(tsBelow)),
			exp:                                   true,
			expExclusiveLocksBlockNonLockingReads: true,
		},
		// D2. However, non-locking reads at the timestamp of the Intent are
		// incompatible.
		{
			desc:                                  "non-locking read at lock timestamp, held intent",
			heldLockStrength:                      lock.MakeStrength(lock.Intent, lock.AtTimestamp(tsLock)),
			toCheckLockStrength:                   lock.MakeStrength(lock.None, lock.AtTimestamp(tsLock)),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// D3. Ditto for non-locking reads above the Intent's timestamp.
		{
			desc:                                  "non-locking read at lock timestamp, held intent",
			heldLockStrength:                      lock.MakeStrength(lock.Intent, lock.AtTimestamp(tsLock)),
			toCheckLockStrength:                   lock.MakeStrength(lock.None, lock.AtTimestamp(tsAbove)),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// D4. Intent lock strengths are incompatible with Shared lock strength.
		{
			desc:                                  "shared lock acquisition, held intent locking strength",
			heldLockStrength:                      lock.MakeStrength(lock.Intent, lock.AtTimestamp(tsLock)),
			toCheckLockStrength:                   lock.MakeStrength(lock.Shared),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// D5. Intent lock strengths are incompatible with Upgrade locking strength.
		{
			desc:                                  "upgrade lock acquisition, held exclusive lock",
			heldLockStrength:                      lock.MakeStrength(lock.Intent, lock.AtTimestamp(tsLock)),
			toCheckLockStrength:                   lock.MakeStrength(lock.Upgrade),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// D6. Intent lock strengths are incompatible with Exclusive locks at lower
		// timestamps.
		{
			desc:                                  "exclusive lock acquisition, held exclusive lock",
			heldLockStrength:                      lock.MakeStrength(lock.Intent, lock.AtTimestamp(tsLock)),
			toCheckLockStrength:                   lock.MakeStrength(lock.Exclusive, lock.AtTimestamp(tsBelow)),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// D7. Ditto for the "at timestamp" case.
		{
			desc:                                  "exclusive lock acquisition, held exclusive lock",
			heldLockStrength:                      lock.MakeStrength(lock.Intent, lock.AtTimestamp(tsLock)),
			toCheckLockStrength:                   lock.MakeStrength(lock.Exclusive, lock.AtTimestamp(tsLock)),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// D8. As well as the "above timestamp" case.
		{
			desc:                                  "exclusive lock acquisition, held exclusive lock",
			heldLockStrength:                      lock.MakeStrength(lock.Intent, lock.AtTimestamp(tsLock)),
			toCheckLockStrength:                   lock.MakeStrength(lock.Exclusive, lock.AtTimestamp(tsAbove)),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// D9. Intent lock strengths are incompatible with other Intent lock
		// strengths at lower timestamps.
		{
			desc:                                  "intent lock strength acquisition, held intent",
			heldLockStrength:                      lock.MakeStrength(lock.Intent, lock.AtTimestamp(tsLock)),
			toCheckLockStrength:                   lock.MakeStrength(lock.Intent, lock.AtTimestamp(tsBelow)),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// D10. Ditto for the "at timestamp" case.
		{
			desc:                                  "intent lock strength acquisition, held exclusive lock",
			heldLockStrength:                      lock.MakeStrength(lock.Intent, lock.AtTimestamp(tsLock)),
			toCheckLockStrength:                   lock.MakeStrength(lock.Intent, lock.AtTimestamp(tsLock)),
			exp:                                   false,
			expExclusiveLocksBlockNonLockingReads: false,
		},
		// D11. As well as the "above timestamp" case.
		{
			desc:                                  "intent lock strength acquisition, held exclusive lock",
			heldLockStrength:                      lock.MakeStrength(lock.Intent, lock.AtTimestamp(tsLock)),
			toCheckLockStrength:                   lock.MakeStrength(lock.Intent, lock.AtTimestamp(tsAbove)),
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
						t, exp, tc.heldLockStrength.CheckLockCompatibility(&st.SV, &tc.toCheckLockStrength),
					)
				})
			})
	}
}
