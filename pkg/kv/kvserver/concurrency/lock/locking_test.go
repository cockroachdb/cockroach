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
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func makeTS(nanos int64) hlc.Timestamp {
	return hlc.Timestamp{
		WallTime: nanos,
	}
}

func testCheckLockConflicts(
	t *testing.T, desc string, m1 *lock.Mode, m2 *lock.Mode, st *cluster.Settings, exp bool,
) {
	t.Run(desc, func(t *testing.T) {
		require.Equal(t, lock.Conflicts(m1, m2, &st.SV), exp)
		// Test for symmetry -- things should work the other way around as well.
		require.Equal(t, lock.Conflicts(m2, m1, &st.SV), exp)
	})
}

// TestCheckLockConflicts_NoneWithSharedUpdateIntent tests the Conflicts
// function for all combinations of None lock modes with either Shared, or
// Update, or Intent lock modes. Interactions with None lock mode is
// nonsensical and interactions with Exclusive locks are tested separately.
func TestCheckLockConflicts_NoneWithSharedUpdateIntent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tsBelow := makeTS(1)
	ts := makeTS(2)
	tsAbove := makeTS(3)

	testCases := []struct {
		desc string
		mode lock.Mode
		exp  bool
	}{
		// A. With Shared locks.
		{
			desc: "shared lock",
			mode: lock.MakeModeShared(),
			exp:  false,
		},
		// B. With Update locks.
		{
			desc: "update lock",
			mode: lock.MakeModeUpdate(),
			exp:  false,
		},
		// C. With Intents.
		// C1. Below the read timestamp.
		{
			desc: "intent below ts",
			mode: lock.MakeModeIntent(tsBelow),
			exp:  true,
		},
		// C2. At the read timestamp.
		{
			desc: "intent at ts",
			mode: lock.MakeModeIntent(ts),
			exp:  true,
		},
		// C3. Above the read timestamp.
		{
			desc: "intent above ts",
			mode: lock.MakeModeIntent(tsAbove),
			exp:  false,
		},
	}
	st := cluster.MakeTestingClusterSettings()
	for _, tc := range testCases {
		for _, iso := range []isolation.Level{
			isolation.Serializable,
			isolation.Snapshot,
			isolation.ReadCommitted,
		} {
			nonLockingRead := lock.MakeModeNone(ts, iso)
			testCheckLockConflicts(
				t,
				fmt.Sprintf("non-locking read(%s)-%s", iso.String(), tc.desc),
				&nonLockingRead,
				&tc.mode,
				st,
				tc.exp,
			)
		}
	}
}

// TestCheckLockConflicts_NoneWithExclusive tests interactions between Exclusive
// locks and non-locking reads. It does so both with and without the
// ExclusiveLocksBlockNonLockingReads cluster setting overriden.
func TestCheckLockConflicts_NoneWithExclusive(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tsBelow := makeTS(1)
	tsLock := makeTS(2)
	tsAbove := makeTS(3)

	ctx := context.Background()
	for _, isoLock := range []isolation.Level{
		isolation.Serializable, isolation.Snapshot, isolation.ReadCommitted,
	} {
		for _, isoRead := range []isolation.Level{
			isolation.Serializable, isolation.Snapshot, isolation.ReadCommitted,
		} {
			if isoRead > isoLock {
				continue // we're only interested in unique permutations
			}

			for i, readTS := range []hlc.Timestamp{tsBelow, tsLock, tsAbove} {
				exclusiveLock := lock.MakeModeExclusive(tsLock, isoLock)
				nonLockingRead := lock.MakeModeNone(readTS, isoRead)

				expConflicts := isoLock == isolation.Serializable &&
					isoRead == isolation.Serializable && (readTS == tsLock || readTS == tsAbove)

				st := cluster.MakeTestingClusterSettings()
				testCheckLockConflicts(
					t,
					fmt.Sprintf("#%d non-locking read(%s) exclusive(%s)", i, isoRead, isoLock),
					&nonLockingRead,
					&exclusiveLock,
					st,
					expConflicts,
				)

				// Now, test with the ExclusiveLocksBlockNonLockingReads cluster setting
				// overriden, and ensure the two lock modes do not conflict.
				lock.ExclusiveLocksBlockNonLockingReads.Override(ctx, &st.SV, false)
				testCheckLockConflicts(
					t,
					fmt.Sprintf("#%d non-locking read(%s) exclusive(%s)", i, isoRead, isoLock),
					&nonLockingRead,
					&exclusiveLock,
					st,
					false,
				)
			}
		}
	}
}

// TestCheckLockConflicts_ExclusiveWithSharedUpdateIntent tests the Conflicts
// function for all combinations where one of the locks is Exclusive and the
// other is either Shared or Update or Intent.
func TestCheckLockConflicts_ExclusiveWithSharedUpdateIntent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tsBelow := makeTS(1)
	ts := makeTS(2)
	tsAbove := makeTS(3)

	testCases := []struct {
		desc string
		mode lock.Mode
		exp  bool
	}{
		// A. With Shared locks.
		{
			desc: "shared lock",
			mode: lock.MakeModeShared(),
			exp:  true,
		},
		// B. With Update locks.
		{
			desc: "update lock",
			mode: lock.MakeModeUpdate(),
			exp:  true,
		},
		// C. With Intents.
		// C1. Below the lock timestamp.
		{
			desc: "intent below ts",
			mode: lock.MakeModeIntent(tsBelow),
			exp:  true,
		},
		// C2. At the lock timestamp.
		{
			desc: "intent at ts",
			mode: lock.MakeModeIntent(ts),
			exp:  true,
		},
		// C3. Above the lock timestamp.
		{
			desc: "intent above ts",
			mode: lock.MakeModeIntent(tsAbove),
			exp:  true,
		},
	}
	st := cluster.MakeTestingClusterSettings()
	for _, tc := range testCases {
		for _, iso := range []isolation.Level{
			isolation.Serializable,
			isolation.Snapshot,
			isolation.ReadCommitted,
		} {
			exclusiveLock := lock.MakeModeExclusive(ts, iso)
			testCheckLockConflicts(
				t,
				fmt.Sprintf("exclusive lock(%s)-%s", iso.String(), tc.desc),
				&exclusiveLock,
				&tc.mode,
				st,
				tc.exp,
			)
		}
	}
}

// TestCheckLockConflicts_ExclusiveWithExclusive tests the Conflicts function
// for all combinations where both the locks have Exclusive lock strength.
// Exclusive locks always conflict with other Exclusive locks.
func TestCheckLockConflicts_ExclusiveWithExclusive(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tsBelow := makeTS(1)
	tsLock := makeTS(2)
	tsAbove := makeTS(3)

	st := cluster.MakeTestingClusterSettings()
	for _, ts := range []hlc.Timestamp{tsBelow, tsLock, tsAbove} {
		for _, iso1 := range []isolation.Level{
			isolation.Serializable,
			isolation.Snapshot,
			isolation.ReadCommitted,
		} {
			for _, iso2 := range []isolation.Level{
				isolation.Serializable,
				isolation.Snapshot,
				isolation.ReadCommitted,
			} {
				if iso2 > iso1 {
					continue // we only need unique permutations
				}
				exclusiveLock := lock.MakeModeExclusive(tsLock, iso1)
				exclusiveLock2 := lock.MakeModeExclusive(ts, iso2)
				testCheckLockConflicts(
					t,
					fmt.Sprintf("exclusive lock(%s)-exclusive lock(%s)", iso1.String(), iso2.String()),
					&exclusiveLock,
					&exclusiveLock2,
					st,
					true, // always conflicts
				)
			}
		}
	}
}

// TestCheckLockConflicts_Shared tests the Conflicts function where one of the
// lock modes is Shared. It tests these with other Shared, Update, and Intent
// lock modes. Tests for non-locking reads and Exclusive locks are already
// covered above.
func TestCheckLockConflicts_Shared(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ts := makeTS(2)

	testCases := []struct {
		desc string
		mode lock.Mode
		exp  bool
	}{
		// A. With Shared locks.
		{
			desc: "shared lock",
			mode: lock.MakeModeShared(),
			exp:  false,
		},
		// B. With Update locks.
		{
			desc: "update lock",
			mode: lock.MakeModeUpdate(),
			exp:  false,
		},
		// C. With Intents.
		{
			desc: "intent",
			mode: lock.MakeModeIntent(ts),
			exp:  true,
		},
	}
	st := cluster.MakeTestingClusterSettings()
	for _, tc := range testCases {
		sharedLock := lock.MakeModeShared()
		testCheckLockConflicts(
			t,
			fmt.Sprintf("shared lock-%s", tc.desc),
			&sharedLock,
			&tc.mode,
			st,
			tc.exp,
		)
	}
}

// TestCheckLockConflicts_Update tests the Conflicts function where one of the
// lock modes is Update. It tests these with other Update and Intent
// lock modes. Tests for non-locking reads, Shared locks and Exclusive locks are
// already covered above.
func TestCheckLockConflicts_Update(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ts := makeTS(2)

	testCases := []struct {
		desc string
		mode lock.Mode
		exp  bool
	}{
		// A. With Update locks.
		{
			desc: "update lock",
			mode: lock.MakeModeUpdate(),
			exp:  true,
		},
		// B. With Intents.
		{
			desc: "intent",
			mode: lock.MakeModeIntent(ts),
			exp:  true,
		},
	}
	st := cluster.MakeTestingClusterSettings()
	for _, tc := range testCases {
		updateLock := lock.MakeModeUpdate()
		testCheckLockConflicts(
			t,
			fmt.Sprintf("update lock-%s", tc.desc),
			&updateLock,
			&tc.mode,
			st,
			tc.exp,
		)
	}
}

// TestCheckLockConflicts_IntentWithIntent tests the Conflicts for all
// combinations where both the locks have Intent lock strength. Intents always
// conflict with other Intents.
func TestCheckLockConflicts_IntentWithIntent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tsBelow := makeTS(1)
	tsLock := makeTS(2)
	tsAbove := makeTS(3)

	st := cluster.MakeTestingClusterSettings()
	for i, ts := range []hlc.Timestamp{tsBelow, tsLock, tsAbove} {
		intent1 := lock.MakeModeIntent(tsLock)
		intent2 := lock.MakeModeIntent(ts)
		testCheckLockConflicts(
			t,
			fmt.Sprintf("%d-intent-intent", i),
			&intent1,
			&intent2,
			st,
			true, // always conflicts
		)
	}
}
