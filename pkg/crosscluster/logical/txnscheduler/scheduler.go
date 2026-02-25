// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnscheduler

import (
	"slices"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/ring"
)

// NewScheduler constructs a scheduler that can track at least `size` locks.
func NewScheduler(size int32) *Scheduler {
	scheduler := &Scheduler{
		lockTable: makeLockTable(size),
		lockMap:   make(map[LockHash]lockList, size),
	}
	return scheduler
}

// Scheduler takes transactions + inferred locks and transforms it into a
// (dependent_transactions[], minimum_applied_time). All dependent transactions
// and all transactions older than the minimum_applied_time must be applied
// before the transaction can be applied.
//
// The Scheduler is heavily optimized because the algorithm is single threaded
// and can't be trivially parallelized, so it may be the bottleneck in
// transactional LDR replication throughput.
type Scheduler struct {
	lockMap      map[LockHash]lockList
	lockTable    lockTable
	eventHorizon hlc.Timestamp
	transactions ring.Buffer[Transaction]
}

// Schedule identifies the dependencies of a single transaction and records it
// so that future transactions can depend on it.
//
// Schedule requires transactions to be supplied in mvcc order. It's impossible
// to accurately identify transaction dependencies if transactions are supplied
// out of order.
//
// Schedule requires the lock hashes within each transaction to be unique.
// Duplicates must be filtered out at the lock synthesis phase.
func (s *Scheduler) Schedule(
	transaction Transaction, scratch []hlc.Timestamp,
) ([]hlc.Timestamp, hlc.Timestamp) {
	for s.lockTable.availableCapacity() < len(transaction.Locks) {
		if s.transactions.Len() == 0 {
			// If we still can't fit the transaction even after forgetting the locks of
			// every tracked transaction it means the transaction is too big to fit in
			// the table.
			horizon := s.eventHorizon
			s.eventHorizon = transaction.CommitTime
			return nil, horizon
		}
		s.pushEventHorizon()
	}

	dependencies := scratch[:0]
	s.transactions.AddLast(transaction)

	for _, lock := range transaction.Locks {
		entryIndex, exists := s.lockMap[lock.Hash]
		switch {
		case !exists:
			entryIndex = s.lockTable.allocateList()
		case lock.IsRead:
			dependencies = s.lockTable.appendReadDependency(entryIndex, dependencies)
		default:
			dependencies = s.lockTable.appendWriteDependency(entryIndex, dependencies)
		}
		if lock.IsRead {
			s.lockMap[lock.Hash] = s.lockTable.recordReadLock(entryIndex, transaction.CommitTime)
		} else {
			s.lockMap[lock.Hash] = s.lockTable.recordWriteLock(entryIndex, transaction.CommitTime)
		}
	}

	// Its possible we ended up with duplicate dependencies if two transactions
	// overlap on more than one lock. Filter it out here.
	slices.SortFunc(dependencies, func(a hlc.Timestamp, b hlc.Timestamp) int {
		return a.Compare(b)
	})
	dependencies = slices.Compact(dependencies)

	return dependencies, s.eventHorizon
}

// pushEventHorizon frees locks associated with the oldest transaction tracked
// by the scheduler. This is done as needed to free up memory to track more
// transactions.
func (s *Scheduler) pushEventHorizon() {
	txn := s.transactions.GetFirst()
	s.transactions.RemoveFirst()
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
