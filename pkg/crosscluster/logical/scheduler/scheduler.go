package scheduler

import "github.com/cockroachdb/cockroach/pkg/util/hlc"

func NewScheduler(lockCount int32) *Scheduler {
	scheduler := &Scheduler{
		lockTable: makeLockTable(lockCount),
		lockMap:   make(map[LockHash]lockList, lockCount),
	}
	return scheduler
}

type Scheduler struct {
	lockMap      map[LockHash]lockList
	lockTable    lockTable
	eventHorizon hlc.Timestamp

	// TODO(jeffswenson): how do I get rid of the Transaction allocations? I
	// guess I could flatten them into a timestamp array and a lock array.
	transactions []Transaction
}

type LockHash uint64

type lockEntryIndex int32

func (l lockEntryIndex) valid() bool {
	return 0 <= l
}

// Goals:
// 1. No allocations in the steady state.
// 2. Scheduling cost is O(txn.Size()) amortized.
func (s *Scheduler) Schedule(
	transaction Transaction, scratch []hlc.Timestamp,
) ([]hlc.Timestamp, hlc.Timestamp) {
	// TODO handle the edge case where the transaction is larger than the entire
	// scheduler.
	for s.lockTable.availableCapacity() < len(transaction.Locks) {
		s.pushEventHorizion()
	}

	dependencies := scratch[:0]
	s.transactions = append(s.transactions, transaction)

	for _, lock := range transaction.Locks {
		entryIndex, exists := s.lockMap[lock.Hash]
		switch {
		case !exists:
			entryIndex = s.lockTable.allocateList()
		case lock.IsRead:
			dependencies = s.lockTable.addReadDependency(entryIndex, dependencies)
		default:
			dependencies = s.lockTable.addWriteDependency(entryIndex, dependencies)
		}
		if lock.IsRead {
			s.lockMap[lock.Hash] = s.lockTable.addReadLock(entryIndex, transaction.CommitTime)
		} else {
			s.lockMap[lock.Hash] = s.lockTable.setWriteLock(entryIndex, transaction.CommitTime)
		}
	}

	return dependencies, s.eventHorizon
}

func (s *Scheduler) pushEventHorizion() {
	txn := s.transactions[0]
	s.transactions = s.transactions[1:]
	s.eventHorizon = txn.CommitTime
	for _, lock := range txn.Locks {
		locks, exists := s.lockMap[lock.Hash]
		if !exists {
			continue
		}

		if lock.IsRead {
			locks, exists = s.lockTable.removeReadLock(locks, txn.CommitTime)
		} else {
			locks, exists = s.lockTable.removeWriteLock(locks, txn.CommitTime)
		}

		if exists {
			s.lockMap[lock.Hash] = locks
		} else {
			delete(s.lockMap, lock.Hash)
		}
	}
}
