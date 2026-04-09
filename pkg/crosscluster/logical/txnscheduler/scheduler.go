// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnscheduler

import (
	"slices"

	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrdecoder"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/txnlock"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/ring"
)

// NewScheduler constructs a scheduler that can track at most `size` lock table
// entries. Each lock table entry can track one write lock and up to 8 read
// locks.
func NewScheduler(size int32) *Scheduler {
	scheduler := &Scheduler{
		lockTable: makeLockTable(size),
		lockMap:   make(map[txnlock.LockHash]lockList, size),
	}
	return scheduler
}

// Scheduler takes transactions with inferred locks and transforms them into
// (dependent_transactions[], minimum_applied_time). All dependent transactions
// and all transactions older than the minimum_applied_time must be applied
// before the transaction can be applied.
//
// Locks come in two flavors: read and write.
//
// |            | Read Lock   | Write Lock |
// |------------|-------------|------------|
// | Read Lock  |             | Dependency |
// | Write Lock | Dependency  | Dependency |
//
// The scheduler takes advantage of the fact that dependencies are transitive.
// So if there is a chain of transactions a, b, c that each share the same
// write lock, it gets emitted as a->b and b->c, but no a->c. This allows the
// scheduler to track exactly one write dependency per lock type.
//
// Because read locks do not depend on each other, but a subsequent write lock
// depends on all prior read locks, we need to track all read locks until the
// next write lock or the frontier is advanced past a read lock. We also need
// to keep tracking the old write when a read lock is added, because future
// read locks will still depend on the write lock.
//
// The fact we track multiple read locks means scheduling a single write lock
// can be O(n) in the number of read locks, but the amortized cost is still
// O(locks) because each read lock will be emitted as a dependency at most
// once.
//
// The Scheduler is heavily optimized because the algorithm is single threaded
// and can't be trivially parallelized, so it may be the bottleneck in
// transactional LDR replication throughput.
type Scheduler struct {
	lockMap      map[txnlock.LockHash]lockList
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
	transaction Transaction, dependenciesBuffer []ldrdecoder.TxnID,
) ([]ldrdecoder.TxnID, hlc.Timestamp) {
	for s.lockTable.availableCapacity() < len(transaction.Locks) {
		if s.transactions.Len() == 0 {
			// If we still can't fit the transaction even after forgetting the locks of
			// every tracked transaction it means the transaction is too big to fit in
			// the table.
			horizon := s.eventHorizon
			s.eventHorizon = transaction.TxnID.Timestamp
			return nil, horizon
		}
		s.pushEventHorizon()
	}

	dependencies := dependenciesBuffer[:0]
	s.transactions.AddLast(transaction)

	for _, lock := range transaction.Locks {
		entryIndex, exists := s.lockMap[lock.Hash]
		switch {
		case !exists:
			entryIndex = s.lockTable.allocateList()
		case lock.Read:
			dependencies = s.lockTable.appendReadDependency(entryIndex, dependencies)
		default:
			dependencies = s.lockTable.appendWriteDependency(entryIndex, dependencies)
		}
		if lock.Read {
			s.lockMap[lock.Hash] = s.lockTable.recordReadLock(entryIndex, transaction.TxnID)
		} else {
			s.lockMap[lock.Hash] = s.lockTable.recordWriteLock(entryIndex, transaction.TxnID)
		}
	}

	// Its possible we ended up with duplicate dependencies if two transactions
	// overlap on more than one lock. Filter it out here.
	slices.SortFunc(dependencies, func(a, b ldrdecoder.TxnID) int {
		return a.Timestamp.Compare(b.Timestamp)
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
	s.eventHorizon = txn.TxnID.Timestamp
	for _, lock := range txn.Locks {
		locks, exists := s.lockMap[lock.Hash]
		if !exists {
			if buildutil.CrdbTestBuild {
				// While the actual lock entry for this txn may have been removed if
				// there was a write lock, there should still be an entry for the lock
				// hash.
				panic("unexpectedly missing lock in lock map")
			}
			continue
		}

		if lock.Read {
			locks, exists = s.lockTable.removeReadLock(locks, txn.TxnID)
		} else {
			locks, exists = s.lockTable.removeWriteLock(locks, txn.TxnID)
		}

		if exists {
			s.lockMap[lock.Hash] = locks
		} else {
			delete(s.lockMap, lock.Hash)
		}
	}
}
