package scheduler

import (
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

			maxHlc := hlc.Timestamp{}
			for _, txn := range tc.transactions {
				if maxHlc.Less(makeHlc(txn.time)) {
					maxHlc = makeHlc(txn.time)
				}
			}
			for len(scheduler.transactions) != 0 {
				scheduler.pushEventHorizion()
			}
			require.Equal(t, maxHlc, scheduler.eventHorizon)
			require.Equal(t, len(scheduler.lockTable.freeList), cap(scheduler.lockTable.freeList))
		})
	}
}
