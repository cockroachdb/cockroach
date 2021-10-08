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

// LockTableMetrics holds information about the state of a lockTable.
type LockTableMetrics struct {
	// The number of locks.
	Locks int64
	// The number of locks actively held by transactions.
	LocksHeld int64
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

	// The top-k locks with the most waiters (readers + writers) in their
	// wait-queue, ordered in descending order.
	TopKLocksByWaiters [3]LockMetrics
	// TODO(nvanbenschoten): add a top-k list ordered on the duration that the
	// locks have been held. See #67619.
	// TopKLocksByDuration [3]LockMetrics
}

// LockMetrics holds information about the state of a single lock in a lockTable.
type LockMetrics struct {
	// The lock's key.
	Key roachpb.Key
	// Is the lock actively held by a transaction, or just a reservation?
	Held bool
	// The number of waiters in the lock's wait queue.
	Waiters int64
	// The number of waiting readers in the lock's wait queue.
	WaitingReaders int64
	// The number of waiting writers in the lock's wait queue.
	WaitingWriters int64
}

// addLockMetrics adds the provided LockMetrics to the receiver.
func (m *LockTableMetrics) addLockMetrics(lm LockMetrics) {
	m.Locks++
	if lm.Held {
		m.LocksHeld++
	} else {
		m.LocksWithReservation++
	}
	if lm.Waiters > 0 {
		m.LocksWithWaitQueues++
		m.Waiters += lm.Waiters
		m.WaitingReaders += lm.WaitingReaders
		m.WaitingWriters += lm.WaitingWriters
		m.addToTopKLocksByWaiters(lm)
	}
}

// addToTopKLocksByWaiters adds the provided LockMetrics to the receiver's
// TopKLocksByWaiters list. If two LockMetrics structs have the same number
// of waiters, the first one added will be ordered first in the ordering.
func (m *LockTableMetrics) addToTopKLocksByWaiters(lm LockMetrics) {
	m.addToTopK(lm, func(cmp LockMetrics) int64 { return cmp.Waiters })
}

func (m *LockTableMetrics) addToTopK(lm LockMetrics, cmp func(LockMetrics) int64) {
	cpy := false
	for i, cur := range m.TopKLocksByWaiters {
		if cur.Key == nil {
			m.TopKLocksByWaiters[i] = lm
			break
		}
		if cpy || cmp(lm) > cmp(cur) {
			m.TopKLocksByWaiters[i] = lm
			lm = cur
			cpy = true
		}
	}
}
