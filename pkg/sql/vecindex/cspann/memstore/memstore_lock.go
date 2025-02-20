// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package memstore

import (
	"sync"
	"sync/atomic"
)

// memLock wraps a read-write memLock, adding support for reentrancy and
// ownership tracking.
// NOTE: This is only used in testing and benchmarking code.
type memLock struct {
	mu struct {
		// NOTE: Do not use syncutil.RWMutex here, because deadlock detection
		// reports spurious failures. Different partitions in the vector index
		// can be locked in different orders by merge, split, format and other
		// operations. In all these cases, we first acquire the in-memory store's
		// structure lock to prevent deadlocks. But the deadlock detection package
		// is not smart enough to realize this and reports false positives.
		sync.RWMutex

		// reentrancy counts how many times the same owner has acquired the same
		// lock.
		reentrancy int
	}

	// exclusiveOwner is the id of its current exclusive owner, or zero if it
	// has not been acquired, or if it has only been acquired for shared access.
	exclusiveOwner atomic.Uint64
}

// IsAcquiredBy returns true if the lock is exclusively owned by the given
// owner, or false if not.
func (pl *memLock) IsAcquiredBy(owner uint64) bool {
	return pl.exclusiveOwner.Load() == owner
}

// Acquire obtains exclusive write access to the resource protected by this
// lock. The same owner can obtain the lock multiple times. The caller must
// ensure that Release is called for each call to Acquire.
func (pl *memLock) Acquire(owner uint64) {
	if pl.exclusiveOwner.Load() == owner {
		// Exclusive lock has already been acquired by this owner.
		pl.mu.reentrancy++
		return
	}

	// Block until exclusive lock is acquired.
	pl.mu.Lock()
	pl.exclusiveOwner.Store(owner) //nolint:deferunlockcheck
}

// AcquireShared obtains shared read access to the resource protected by this
// lock. The same owner can obtain the lock multiple times. The caller must
// ensure that ReleaseShared is called for each all to AcquireShared.
func (pl *memLock) AcquireShared(owner uint64) {
	if owner != 0 && pl.exclusiveOwner.Load() == owner {
		// Exclusive lock has already been acquired by this owner.
		pl.mu.reentrancy++
		return
	}

	// Block until shared lock is acquired.
	pl.mu.RLock()
}

// Release unlocks exclusive write access to the protected resource obtained by
// a call to Acquire. If the same owner made multiple Acquire calls, the lock
// isn't released until Release is called the same number of times.
func (pl *memLock) Release() {
	if pl.mu.reentrancy > 0 {
		pl.mu.reentrancy--
		return
	}

	// No remaining reentrancy, so release lock.
	pl.exclusiveOwner.Store(0)
	pl.mu.Unlock()
}

// ReleaseShared unlocks shared read access to the protected resource obtained
// by a call to AcquireShared. If the same owner made multiple AcquireShared
// calls, the lock isn't released until ReleaseShared is called the same number
// of times.
func (pl *memLock) ReleaseShared() {
	if pl.mu.reentrancy > 0 {
		pl.mu.reentrancy--
		return
	}

	// No remaining reentrancy, so release lock.
	pl.mu.RUnlock()
}
