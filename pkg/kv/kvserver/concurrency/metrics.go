// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package concurrency

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanlatch"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// LatchMetrics holds information about the state of a latchManager.
type LatchMetrics = spanlatch.Metrics

// TopKLockMetrics holds the metrics on the top K (where K = 3) locks by
// various orderings.
type TopKLockMetrics = [3]LockMetrics

// LockTableMetrics holds information about the state of a lockTable.
type LockTableMetrics struct {
	// The number of locks.
	Locks int64
	// The number of locks actively held by transactions.
	LocksHeld int64
	// The aggregate nanoseconds locks have been active in the lock table and
	// marked as held.
	TotalLockHoldDurationNanos int64
	// The number of locks not held, but with reservations.
	LocksWithReservation int64
	// The number of locks with non-empty wait-queues.
	LocksWithWaitQueues int64

	// The aggregate number of waiters in wait-queues across all locks.
	Waiters int64
	// The aggregate number of waiting readers in wait-queues across all locks.
	WaitingReaders int64
	// The aggregate number of waiting writers in wait-queues across all locks.
	WaitingWriters int64
	// The aggregate nanoseconds spent in wait-queues, aggregated across each
	// waiter in the wait-queue of every lock in the lock table.
	TotalWaitDurationNanos int64

	// The top-k locks with the most waiters (readers + writers) in their
	// wait-queue, ordered in descending order.
	TopKLocksByWaiters TopKLockMetrics

	// The top-k locks by hold duration, ordered in descending order.
	TopKLocksByHoldDuration TopKLockMetrics

	// The top-k locks by longest waiting reader or writer in the wait-queue,
	// ordered in descending order.
	TopKLocksByWaitDuration TopKLockMetrics
}

// LockMetrics holds information about the state of a single lock in a lockTable.
type LockMetrics struct {
	// The lock's key.
	Key roachpb.Key
	// Is the lock actively held by a transaction, or just a reservation?
	Held bool
	// The number of nanoseconds this lock has been in the lock table and marked as held.
	HoldDurationNanos int64
	// The number of waiters in the lock's wait queue.
	Waiters int64
	// The number of waiting readers in the lock's wait queue.
	WaitingReaders int64
	// The number of waiting writers in the lock's wait queue.
	WaitingWriters int64
	// The total number of nanoseconds all waiters have been in the lock's wait queue.
	WaitDurationNanos int64
	// The maximum number of nanoseconds a waiter has been in the lock's wait queue.
	MaxWaitDurationNanos int64
}

// addLockMetrics adds the provided LockMetrics to the receiver.
func (m *LockTableMetrics) addLockMetrics(lm LockMetrics) {
	m.Locks++
	if lm.Held {
		m.LocksHeld++
		m.TotalLockHoldDurationNanos += lm.HoldDurationNanos
		m.addToTopKLocksByHoldDuration(lm)
	} else {
		m.LocksWithReservation++
	}
	if lm.Waiters > 0 {
		m.LocksWithWaitQueues++
		m.Waiters += lm.Waiters
		m.WaitingReaders += lm.WaitingReaders
		m.WaitingWriters += lm.WaitingWriters
		m.TotalWaitDurationNanos += lm.WaitDurationNanos
		m.addToTopKLocksByWaiters(lm)
		m.addToTopKLocksByWaitDuration(lm)
	}
}

// addToTopKLocksByWaiters adds the provided LockMetrics to the receiver's
// TopKLocksByWaiters list. If two LockMetrics structs have the same number
// of waiters, the first one added will be ordered first in the ordering.
func (m *LockTableMetrics) addToTopKLocksByWaiters(lm LockMetrics) {
	addToTopK(m.TopKLocksByWaiters[:], lm, func(cmp LockMetrics) int64 { return cmp.Waiters })
}

// addToTopKLocksByHoldDuration adds the provided LockMetrics to the receiver's
// TopKLocksByHoldDuration list. They are ordered by decreasing hold duration.
func (m *LockTableMetrics) addToTopKLocksByHoldDuration(lm LockMetrics) {
	addToTopK(m.TopKLocksByHoldDuration[:], lm, func(cmp LockMetrics) int64 { return cmp.HoldDurationNanos })
}

// addToTopKLocksByWaitDuration adds the provided LockMetrics to the receiver's
// TopKLocksByWaitDuration list. The LockMetrics are ordered by the maximum
// wait duration across all waiters on each lock, in decreasing order.
func (m *LockTableMetrics) addToTopKLocksByWaitDuration(lm LockMetrics) {
	addToTopK(m.TopKLocksByWaitDuration[:], lm, func(cmp LockMetrics) int64 { return cmp.MaxWaitDurationNanos })
}

func addToTopK(topK []LockMetrics, lm LockMetrics, cmp func(LockMetrics) int64) {
	cpy := false
	for i, cur := range topK {
		if cur.Key == nil {
			topK[i] = lm
			break
		}
		if cpy || cmp(lm) > cmp(cur) {
			topK[i] = lm
			lm = cur
			cpy = true
		}
	}
}
