// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnscheduler

import (
	"math/rand"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/stretchr/testify/require"
)

func TestScheduler(t *testing.T) {
	type transaction struct {
		time          int
		readLocks     []int
		writeLocks    []int
		dependsOn     []int
		dependsOnTime int
	}

	type testCase struct {
		name          string
		lockTableSize int32
		transactions  []transaction
	}

	makeHlc := func(t int) hlc.Timestamp {
		return hlc.Timestamp{WallTime: int64(t)}
	}

	testCases := []testCase{
		{
			name: "write series",
			transactions: []transaction{
				{time: 1, writeLocks: []int{1337}},
				{time: 2, writeLocks: []int{1337}, dependsOn: []int{1}},
				{time: 3, writeLocks: []int{1337}, dependsOn: []int{2}},
			},
		},
		{
			name: "sandwiched read",
			transactions: []transaction{
				{time: 1, writeLocks: []int{42}},
				{time: 2, readLocks: []int{42}, dependsOn: []int{1}},
				{time: 3, writeLocks: []int{42}, dependsOn: []int{1, 2}},
			},
		},
		{
			name: "multiple lock conflicts",
			transactions: []transaction{
				{time: 1, writeLocks: []int{1, 2}},
				{time: 2, writeLocks: []int{1, 2}, dependsOn: []int{1}},
			},
		},
		{
			name:          "independent writes",
			lockTableSize: 2,
			transactions: []transaction{
				{time: 1, writeLocks: []int{1}},
				{time: 2, writeLocks: []int{2}},
				{time: 3, writeLocks: []int{3}, dependsOnTime: 1},
			},
		},
		{
			name: "many reads",
			transactions: []transaction{
				{time: 1, readLocks: []int{7}},
				{time: 2, readLocks: []int{7}},
				{time: 3, readLocks: []int{7}},
				{time: 4, readLocks: []int{7}},
				{time: 5, readLocks: []int{7}},
				{time: 6, readLocks: []int{7}},
				{time: 7, readLocks: []int{7}},
				{time: 8, readLocks: []int{7}},
				{time: 9, readLocks: []int{7}},
				{time: 10, writeLocks: []int{7}, dependsOn: []int{1, 2, 3, 4, 5, 6, 7, 8, 9}},
				{time: 11, writeLocks: []int{7}, dependsOn: []int{10}},
			},
		},
		{
			name: "read and write dependencies",
			transactions: []transaction{
				{time: 1, writeLocks: []int{10}},
				{time: 2, readLocks: []int{20}},
				{time: 3, writeLocks: []int{20}, readLocks: []int{10}, dependsOn: []int{1, 2}},
			},
		},
		{
			name:          "large transaction",
			lockTableSize: 2,
			transactions: []transaction{
				{time: 1, writeLocks: []int{1}},
				// This one txn is too large for the table
				{time: 3, writeLocks: []int{1, 2, 3}, dependsOnTime: 1},
				{time: 5, writeLocks: []int{1}, dependsOnTime: 3},
				{time: 6, writeLocks: []int{1}, dependsOn: []int{5}, dependsOnTime: 3},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.lockTableSize == 0 {
				tc.lockTableSize = 1000
			}
			scheduler := NewScheduler(tc.lockTableSize)
			for _, txn := range tc.transactions {
				var locks []Lock
				for _, r := range txn.readLocks {
					locks = append(locks, Lock{
						Hash:   LockHash(r),
						IsRead: true,
					})
				}
				for _, w := range txn.writeLocks {
					locks = append(locks, Lock{
						Hash:   LockHash(w),
						IsRead: false,
					})
				}
				transaction := Transaction{
					CommitTime: makeHlc(txn.time),
					Locks:      locks,
				}
				deps, dependsOnTime := scheduler.Schedule(transaction, nil)
				require.Equal(t, makeHlc(txn.dependsOnTime), dependsOnTime)

				sort.Slice(deps, func(i, j int) bool {
					return deps[i].Less(deps[j])
				})

				require.Equal(t, len(txn.dependsOn), len(deps))
				for i, dep := range txn.dependsOn {
					expectedDep := makeHlc(dep)
					require.Equal(t, expectedDep, deps[i])
				}
			}

			checkLeaks(t, scheduler)
		})
	}
}

func checkLeaks(t *testing.T, s *Scheduler) {
	// Drain all of the transactions to ensure we didn't leak any lock table
	// entries.
	for range s.transactions.Len() {
		s.pushEventHorizon()
	}
	require.Equal(t, len(s.lockTable.freeList), len(s.lockTable.locks))
	require.Equal(t, len(s.lockTable.freeList), cap(s.lockTable.freeList))
}

func TestSchedulerAlternatingWriteReadRuns(t *testing.T) {
	// TestSchedulerAlternatingWriteReadRuns generates alternating runs of writes
	// and reads against a single lock hash. It verifies that:
	//   - A write depends on the previous write AND all reads since that write.
	//   - A read depends only on the previous write.
	// The goal of this test is to build up long read lock lists.

	makeHlc := func(ts int) hlc.Timestamp {
		return hlc.Timestamp{WallTime: int64(ts)}
	}

	rng := rand.New(rand.NewSource(42))
	scheduler := NewScheduler(10000)
	hash := LockHash(1)

	ts := 0
	makeTxn := func(isRead bool) Transaction {
		ts++
		t.Logf("txn: %d isRead: %t", ts, isRead)
		return Transaction{
			CommitTime: makeHlc(ts),
			Locks: []Lock{{
				Hash:   hash,
				IsRead: isRead,
			}},
		}
	}

	var lastWriteTxn hlc.Timestamp
	var readTxns []hlc.Timestamp

	for range 20 {
		for range rng.Intn(10) + 1 {
			txn := makeTxn(false)

			locks, horizon := scheduler.Schedule(txn, nil)
			require.True(t, horizon.IsEmpty())

			expect := readTxns
			if !lastWriteTxn.IsEmpty() {
				expect = append([]hlc.Timestamp{lastWriteTxn}, readTxns...)
			}
			require.Equal(t, expect, locks)

			lastWriteTxn = txn.CommitTime
			readTxns = nil
		}

		for range rng.Intn(100) + 1 {
			txn := makeTxn(true)

			locks, horizon := scheduler.Schedule(txn, nil)
			require.True(t, horizon.IsEmpty())
			require.Equal(t, []hlc.Timestamp{lastWriteTxn}, locks)

			readTxns = append(readTxns, txn.CommitTime)
		}
	}

	checkLeaks(t, scheduler)
}
