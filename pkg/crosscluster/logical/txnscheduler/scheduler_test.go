// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnscheduler

import (
	"fmt"
	"math/rand"
	"slices"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/txnlock"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestScheduler(t *testing.T) {
	type transaction struct {
		time                 int
		readLocks            []int
		writeLocks           []int
		expectedDependencies []int
		expectedHorizon      int
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
				{time: 2, writeLocks: []int{1337}, expectedDependencies: []int{1}},
				{time: 3, writeLocks: []int{1337}, expectedDependencies: []int{2}},
			},
		},
		{
			name: "sandwiched read",
			transactions: []transaction{
				{time: 1, writeLocks: []int{42}},
				{time: 2, readLocks: []int{42}, expectedDependencies: []int{1}},
				{time: 3, writeLocks: []int{42}, expectedDependencies: []int{1, 2}},
			},
		},
		{
			name: "multiple lock conflicts",
			transactions: []transaction{
				{time: 1, writeLocks: []int{1, 2}},
				{time: 2, writeLocks: []int{1, 2}, expectedDependencies: []int{1}},
			},
		},
		{
			name:          "independent writes",
			lockTableSize: 2,
			transactions: []transaction{
				{time: 1, writeLocks: []int{1}},
				{time: 2, writeLocks: []int{2}},
				{time: 3, writeLocks: []int{3}, expectedHorizon: 1},
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
				{time: 10, writeLocks: []int{7}, expectedDependencies: []int{1, 2, 3, 4, 5, 6, 7, 8, 9}},
				{time: 11, writeLocks: []int{7}, expectedDependencies: []int{10}},
			},
		},
		{
			name: "read and write dependencies",
			transactions: []transaction{
				{time: 1, writeLocks: []int{10}},
				{time: 2, readLocks: []int{20}},
				{time: 3, writeLocks: []int{20}, readLocks: []int{10}, expectedDependencies: []int{1, 2}},
			},
		},
		{
			name:          "large transaction",
			lockTableSize: 2,
			transactions: []transaction{
				{time: 1, writeLocks: []int{1}},
				// This one txn is too large for the table
				{time: 3, writeLocks: []int{1, 2, 3}, expectedHorizon: 1},
				{time: 5, writeLocks: []int{1}, expectedHorizon: 3},
				{time: 6, writeLocks: []int{1}, expectedDependencies: []int{5}, expectedHorizon: 3},
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
				var locks []txnlock.Lock
				for _, r := range txn.readLocks {
					locks = append(locks, txnlock.Lock{
						Hash: txnlock.LockHash(r),
						Read: true,
					})
				}
				for _, w := range txn.writeLocks {
					locks = append(locks, txnlock.Lock{
						Hash: txnlock.LockHash(w),
						Read: false,
					})
				}
				transaction := Transaction{
					CommitTime: makeHlc(txn.time),
					Locks:      locks,
				}
				deps, horizon := scheduler.Schedule(transaction, nil)
				require.Equal(t, makeHlc(txn.expectedHorizon), horizon)

				sort.Slice(deps, func(i, j int) bool {
					return deps[i].Less(deps[j])
				})

				require.Equal(t, len(txn.expectedDependencies), len(deps))
				for i, dep := range txn.expectedDependencies {
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
	require.Empty(t, s.lockMap)
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
	hash := txnlock.LockHash(1)

	ts := 0
	makeTxn := func(isRead bool) Transaction {
		ts++
		t.Logf("txn: %d isRead: %t", ts, isRead)
		return Transaction{
			CommitTime: makeHlc(ts),
			Locks: []txnlock.Lock{{
				Hash: hash,
				Read: isRead,
			}},
		}
	}

	var lastWriteTxn hlc.Timestamp
	var readTxns []hlc.Timestamp

	for range 20 {
		for range rng.Intn(10) + 1 {
			txn := makeTxn(false)

			dependencies, horizon := scheduler.Schedule(txn, nil)
			require.True(t, horizon.IsEmpty())

			expect := readTxns
			if !lastWriteTxn.IsEmpty() {
				expect = append([]hlc.Timestamp{lastWriteTxn}, readTxns...)
			}
			require.Equal(t, expect, dependencies)

			lastWriteTxn = txn.CommitTime
			readTxns = nil
		}

		for range rng.Intn(100) + 1 {
			txn := makeTxn(true)

			dependencies, horizon := scheduler.Schedule(txn, nil)
			require.True(t, horizon.IsEmpty())
			require.Equal(t, []hlc.Timestamp{lastWriteTxn}, dependencies)

			readTxns = append(readTxns, txn.CommitTime)
		}
	}

	checkLeaks(t, scheduler)
}

func TestSchedulerRandomOracle(t *testing.T) {
	rng, _ := randutil.NewTestRand()

	// Limit the table to 2^20 total cells to limit test runtime and memory
	// usage. We randomly distribute that budget between transactions and locks
	// in order to test cases with lots of transactions and cases with lots of
	// locks.
	const tableSize = 20
	txnExponent := rng.Intn(tableSize + 1)
	numTransactions := 1 << txnExponent
	numLocks := 1 << (tableSize - txnExponent)

	t.Logf(
		"numTransactions=%d numLocks=%d",
		numTransactions, numLocks,
	)

	transactions, oracleDeps := generateOracleTable(rng, numTransactions, numLocks)

	// Test with several lock table sizes to exercise both no-eviction and
	// eviction paths.
	for _, lockTableSize := range []int32{64, 1024, 1024 * 1024} {
		t.Run(fmt.Sprintf("lockTableSize=%d", lockTableSize), func(t *testing.T) {
			scheduler := NewScheduler(lockTableSize)

			for txnID := range numTransactions {
				txn := transactions[txnID]
				gotDeps, gotHorizon := scheduler.Schedule(txn, nil)

				// Filter out oracle dependencies at or before the event
				// horizon. The scheduler subsumes these into the horizon.
				var expectedDeps []hlc.Timestamp
				for _, dep := range oracleDeps[txnID] {
					if gotHorizon.Less(dep) {
						expectedDeps = append(expectedDeps, dep)
					}
				}

				slices.SortFunc(gotDeps, func(a, b hlc.Timestamp) int {
					return a.Compare(b)
				})

				require.Equal(t, expectedDeps, gotDeps,
					"txnID=%d commitTime=%s horizon=%s",
					txnID, txn.CommitTime, gotHorizon,
				)
			}

			checkLeaks(t, scheduler)
		})
	}
}

// lockStatus represents the type of lock a transaction holds on a key.
type lockStatus byte

const (
	lockNone  lockStatus = 0
	lockRead  lockStatus = 1
	lockWrite lockStatus = 2
)

// generateOracleTable builds a randomized test workload and independently
// computes the correct scheduling dependencies for it.
//
// It first materializes a 2D table of lockStatus values indexed as
// table[lockID][txnID]. Each cell is randomly assigned none, read, or write
// using a distribution whose thresholds are randomly selected from rng. For
// example, with 4 transactions and 3 locks the table might look like:
//
//	          txn0   txn1   txn2   txn3
//	lock0  [  W      R      -      W  ]
//	lock1  [  -      W      R      R  ]
//	lock2  [  R      -      W      -  ]
//
// From this table, two things are derived:
//
// Transactions are built by reading down each column. For example, txn1
// holds a read lock on lock0 and a write lock on lock1.
//
// Dependencies are computed by reading across each row. For each lock, a
// forward scan tracks the most recent writer and all readers since that
// writer:
//   - A read depends on the most recent prior write to the same lock.
//   - A write depends on the most recent prior write and every read since
//     that write.
//
// In the example above, txn3's write on lock0 depends on txn0 (last
// writer) and txn1 (reader since txn0). txn3's read on lock1 depends on
// txn1 (last writer on lock1). So txn3's combined dependencies are
// {txn0, txn1}.
func generateOracleTable(
	rng *rand.Rand, numTransactions int, numLocks int,
) (transactions []Transaction, oracleDeps [][]hlc.Timestamp) {
	// Randomly select the distribution thresholds. noneThreshold is in
	// [1,98] and readThreshold is in (noneThreshold, 99], so all three
	// statuses always have a nonzero probability.
	noneThreshold := rng.Intn(98) + 1                               // [1, 98]
	readThreshold := noneThreshold + 1 + rng.Intn(99-noneThreshold) // (noneThreshold, 99]

	// Materialize the 2D table as a flat slice indexed as
	// [lockID*numTransactions + txnID].
	table := make([]lockStatus, numTransactions*numLocks)
	for i := range table {
		r := rng.Intn(100)
		switch {
		case r < noneThreshold:
			table[i] = lockNone
		case r < readThreshold:
			table[i] = lockRead
		default:
			table[i] = lockWrite
		}
	}

	// Build transactions and compute dependencies in a single forward pass
	// per lock column. For each lockID we track the most recent writer and
	// all readers since that writer.
	txnLocks := make([][]txnlock.Lock, numTransactions)
	depSets := make([]map[int]struct{}, numTransactions)

	for lockID := range numLocks {
		base := lockID * numTransactions
		lastWriter := -1
		var readers []int

		for txnID := range numTransactions {
			status := table[base+txnID]
			if status == lockNone {
				continue
			}

			txnLocks[txnID] = append(txnLocks[txnID], txnlock.Lock{
				Hash: txnlock.LockHash(lockID),
				Read: status == lockRead,
			})

			if depSets[txnID] == nil {
				depSets[txnID] = map[int]struct{}{}
			}

			if lastWriter >= 0 {
				depSets[txnID][lastWriter] = struct{}{}
			}

			if status == lockWrite {
				for _, r := range readers {
					depSets[txnID][r] = struct{}{}
				}
				lastWriter = txnID
				readers = readers[:0]
			} else {
				readers = append(readers, txnID)
			}
		}
	}

	transactions = make([]Transaction, numTransactions)
	oracleDeps = make([][]hlc.Timestamp, numTransactions)
	for txnID := range numTransactions {
		transactions[txnID] = Transaction{
			CommitTime: hlc.Timestamp{WallTime: int64(txnID + 1)},
			Locks:      txnLocks[txnID],
		}

		deps := depSets[txnID]
		if len(deps) == 0 {
			continue
		}
		ts := make([]hlc.Timestamp, 0, len(deps))
		for dep := range deps {
			ts = append(ts, hlc.Timestamp{WallTime: int64(dep + 1)})
		}
		slices.SortFunc(ts, func(a, b hlc.Timestamp) int {
			return a.Compare(b)
		})
		oracleDeps[txnID] = ts
	}
	return transactions, oracleDeps
}
