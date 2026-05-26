// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	t *testing.T, desc string, m1, m2 lock.Mode, st *cluster.Settings, exp bool,
) {
	t.Run(desc, func(t *testing.T) {
		require.Equal(t, exp, lock.Conflicts(m1, m2, &st.SV))
		// Test for symmetry -- things should work the other way around as well.
		require.Equal(t, exp, lock.Conflicts(m2, m1, &st.SV))
	})
}

// TestCheckLockConflicts_NoneWithNone tests the Conflicts function for all
// combinations where both modes belong to non-locking reads. We never expect
// this to happen in practice, as non-locking reads don't acquire locks in the
// lock table; however, this test ensures interactions are sane, and two
// non-locking reads never conflict.
func TestCheckLockConflicts_NoneWithNone(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tsBelow := makeTS(1)
	tsMid := makeTS(2)
	tsAbove := makeTS(3)

	st := cluster.MakeTestingClusterSettings()
	for _, ts := range []hlc.Timestamp{tsBelow, tsMid, tsAbove} {
		for _, iso1 := range isolation.Levels() {
			for _, iso2 := range isolation.Levels() {
				if iso2.WeakerThan(iso1) {
					continue // we only care about unique permutations
				}
				r1 := lock.MakeModeNone(tsMid, iso1)
				r2 := lock.MakeModeNone(ts, iso2)
				testCheckLockConflicts(
					t,
					fmt.Sprintf("exclusive lock(%s)-exclusive lock(%s)", iso1, iso2),
					r1,
					r2,
					st,
					false, // never conflicts
				)
			}
		}
	}
}

// TestCheckLockConflicts_NoneWithSharedUpdateIntent tests the Conflicts
// function for all combinations of None lock modes with either Shared, or
// Update, or Intent lock modes. Interactions with None lock mode (although
// nonsensical) are tested above and interactions with Exclusive locks are
// tested separately.
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
		for _, iso := range isolation.Levels() {
			nonLockingRead := lock.MakeModeNone(ts, iso)
			testCheckLockConflicts(
				t,
				fmt.Sprintf("non-locking read(%s)-%s", iso, tc.desc),
				nonLockingRead,
				tc.mode,
				st,
				tc.exp,
			)
		}
	}
}

// TestCheckLockConflicts_NoneWithExclusive tests interactions between
// non-locking reads and Exclusive locks. It does so both with and without the
// ExclusiveLocksBlockNonLockingReads cluster setting overridden.
func TestCheckLockConflicts_NoneWithExclusive(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tsBelow := makeTS(1)
	tsLock := makeTS(2)
	tsAbove := makeTS(3)

	ctx := context.Background()
	for _, isoLock := range isolation.Levels() {
		for _, isoRead := range isolation.Levels() {
			for i, readTS := range []hlc.Timestamp{tsBelow, tsLock, tsAbove} {
				exclusiveLock := lock.MakeModeExclusive(tsLock, isoLock)
				nonLockingRead := lock.MakeModeNone(readTS, isoRead)

				expConflicts := isoLock == isolation.Serializable &&
					isoRead == isolation.Serializable && (readTS == tsLock || readTS == tsAbove)

				st := cluster.MakeTestingClusterSettings()
				// Test with the ExclusiveLocksBlockNonLockingReads cluster setting
				// enabled.
				lock.ExclusiveLocksBlockNonLockingReads.Override(ctx, &st.SV, true)
				testCheckLockConflicts(
					t,
					fmt.Sprintf("#%d non-locking read(%s) exclusive(%s)", i, isoRead, isoLock),
					nonLockingRead,
					exclusiveLock,
					st,
					expConflicts,
				)

				// Now, test with the ExclusiveLocksBlockNonLockingReads cluster setting
				// disabled, and ensure the two lock modes do not conflict.
				lock.ExclusiveLocksBlockNonLockingReads.Override(ctx, &st.SV, false)
				testCheckLockConflicts(
					t,
					fmt.Sprintf("#%d non-locking read(%s) exclusive(%s)", i, isoRead, isoLock),
					nonLockingRead,
					exclusiveLock,
					st,
					false, // should not conflict
				)
			}
		}
	}
}

// TestCheckLockConflicts_Shared tests the Conflicts function where one of the
// lock modes is Shared. It tests these with other Shared, Update, and Intent
// lock modes. Tests for non-locking reads are already covered above and tests
// with Exclusive are covered below.
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
			sharedLock,
			tc.mode,
			st,
			tc.exp,
		)
	}
}

// TestCheckLockConflicts_Update tests the Conflicts function where one of the
// lock modes is Update. It tests these with other Update and Intent
// lock modes. Tests for non-locking reads and Shared locks are already covered
// above, and tests with Exclusive locks are covered below.
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
			updateLock,
			tc.mode,
			st,
			tc.exp,
		)
	}
}

// TestCheckLockConflicts_ExclusiveWithSharedUpdateIntent tests the Conflicts
// function for all combinations where one of the locks is Exclusive and the
// other is either Shared or Update or Intent. Tests with non-locking reads are
// already covered below and Exclusive-Exclusive interactions are covered below.
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
		for _, iso := range isolation.Levels() {
			exclusiveLock := lock.MakeModeExclusive(ts, iso)
			testCheckLockConflicts(
				t,
				fmt.Sprintf("exclusive lock(%s)-%s", iso, tc.desc),
				exclusiveLock,
				tc.mode,
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
		for _, iso1 := range isolation.Levels() {
			for _, iso2 := range isolation.Levels() {
				if iso2.WeakerThan(iso1) {
					continue // we only care about unique permutations
				}
				exclusiveLock := lock.MakeModeExclusive(tsLock, iso1)
				exclusiveLock2 := lock.MakeModeExclusive(ts, iso2)
				testCheckLockConflicts(
					t,
					fmt.Sprintf("exclusive lock(%s)-exclusive lock(%s)", iso1, iso2),
					exclusiveLock,
					exclusiveLock2,
					st,
					true, // always conflicts
				)
			}
		}
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
			intent1,
			intent2,
			st,
			true, // always conflicts
		)
	}
}

func TestCheckLockConflicts_Empty(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tsLock := makeTS(2)
	st := cluster.MakeTestingClusterSettings()
	for _, mode := range []lock.Mode{
		lock.MakeModeIntent(tsLock),
		lock.MakeModeExclusive(tsLock, isolation.Serializable),
		lock.MakeModeUpdate(),
		lock.MakeModeShared(),
		lock.MakeModeNone(tsLock, isolation.Serializable),
	} {
		var empty lock.Mode
		testCheckLockConflicts(t, fmt.Sprintf("empty with %s", mode), empty, mode, st, false)
	}
}

// TestLockModeWeaker tests strength comparison semantics for various lock mode
// combinations.
func TestLockModeWeaker(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tsBelow := makeTS(1)
	tsLock := makeTS(2)
	tsAbove := makeTS(3)

	testCases := []struct {
		m1  lock.Mode
		m2  lock.Mode
		exp bool
	}{
		{
			m1:  lock.MakeModeNone(tsLock, isolation.Serializable),
			m2:  lock.MakeModeNone(tsBelow, isolation.Serializable), // stronger
			exp: true,
		},
		{
			m1:  lock.MakeModeNone(tsLock, isolation.Serializable), // stronger
			m2:  lock.MakeModeNone(tsAbove, isolation.Serializable),
			exp: false,
		},
		{
			m1:  lock.MakeModeIntent(tsLock), // stronger
			m2:  lock.MakeModeNone(tsBelow, isolation.Serializable),
			exp: false,
		},
		{
			m1:  lock.MakeModeIntent(tsLock),
			m2:  lock.MakeModeIntent(tsBelow), // stronger
			exp: true,
		},
		{
			m1:  lock.MakeModeIntent(tsLock), // stronger
			m2:  lock.MakeModeIntent(tsAbove),
			exp: false,
		},
		{
			m1:  lock.MakeModeIntent(tsLock), // stronger
			m2:  lock.MakeModeShared(),
			exp: false,
		},
		{
			m1:  lock.MakeModeIntent(tsLock), // stronger
			m2:  lock.MakeModeUpdate(),
			exp: false,
		},
		{
			m1:  lock.MakeModeIntent(tsLock), // stronger
			m2:  lock.MakeModeExclusive(tsBelow, isolation.Serializable),
			exp: false,
		},
		{
			m1:  lock.MakeModeIntent(tsLock), // stronger
			m2:  lock.MakeModeExclusive(tsAbove, isolation.Serializable),
			exp: false,
		},
	}

	for _, tc := range testCases {
		require.Equal(t, tc.exp, tc.m1.Weaker(tc.m2))
	}
}
