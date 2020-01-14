// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package lock provides type definitions for locking-related concepts used by
// concurrency control in the key-value layer.
package lock

// Strength represents the different locking modes that determine how key-values
// can be accessed by concurrent transactions.
//
// Locking modes have differing levels of strength, growing from "weakest" to
// "strongest" in the order that the variants are presented in the enumeration.
// The "stronger" a locking mode, the more protection it provides for the lock
// holder but the more restrictive it is to concurrent transactions attempting
// to access the same keys.
//
// Compatability Matrix
//
// The following matrix presents the compatibility of locking strengths with one
// another. A cell with an X means that the two strengths are incompatible with
// each other and that they can not both be held on a given key by different
// transactions, concurrently. A cell without an X means that the two strengths
// are compatable with each other and that they can be held on a given key by
// different transactions, concurrently.
//
//  +-----------+-----------+-----------+-----------+-----------+
//  |           |  (none)*  |  Shared   |  Upgrade  | Exclusive |
//  +-----------+-----------+-----------+-----------+-----------+
//  | (none)*   |           |           |           |     X^†   |
//  +-----------+-----------+-----------+-----------+-----------+
//  | Shared    |           |           |     X     |     X     |
//  +-----------+-----------+-----------+-----------+-----------+
//  | Upgrade   |           |     X     |     X     |     X     |
//  +-----------+-----------+-----------+-----------+-----------+
//  | Exclusive |     X^†   |     X     |     X     |     X     |
//  +-----------+-----------+-----------+-----------+-----------+
//
// [*] "none" corresponds to the behavior of key-value reads under optimistic
// concurrency control. No locks are acquired when these requests evaluate but
// they do respect exclusive locks already held by other transactions.
//
// [†] reads under optimistic concurrency control in CockroachDB only conflict
// with Exclusive locks if the read's tiemstamp is equal to or greater than the
// locks timestamp. If the read's timestamp is below the Exclusive locks
// timestamp then the two are compatable.
//
type Strength uint32

const (
	_ Strength = 1 << iota

	// Shared (S) locks are used by read-only operations and allow concurrent
	// transactions to read under pessimistic concurrency control. Shared locks
	// are compatable with each other but are not compatable with Upgrade or
	// Exclusive locks. This means that multiple transactions can hold a Shared
	// lock on the same key at the same time, but no other transaction can
	// modify the key at the same time. A holder of a Shared lock on a key is
	// only permitted to read from key while the lock is held.
	//
	// Share locks are currently unused, as all KV read locking is currently
	// performed optimistically. Optimistic concurrency control can improve
	// performance under some workloads because it avoids the need to acquire
	// any locks during reads. However, it does mandate a read validation phase
	// if/when transactions need to commit at a different timestamp than they
	// performed all reads at. CockroachDB calls this a "read refresh", which is
	// implemented by the txnSpanRefresher. See the comment there for more.
	Shared

	// Upgrade (U) locks are a hybrid of Shared and Exclusive locks which are
	// used to prevent a common form of deadlock. When a transaction intends to
	// modify existing KVs, it is often the case that it reads the KVs first and
	// then attempts to modify them. Under pessimistic concurrency control, this
	// would correspond to first acquiring a Shared lock on the keys and then
	// converting the lock to an Exclusive lock when modifying the keys. If two
	// transactions were to acquire the Shared lock initially and then attempt
	// to update the keys concurrently, both transactions would get stuck
	// waiting for the other to release its Shared lock and a deadlock would
	// occur. To resolve the deadlock, one of the two transactions would need to
	// be aborted.
	//
	// To avoid this potential deadlock problem, an Update lock can be used in
	// place of a Shared lock. Update locks are not compatable with any other
	// form of locking. As with Shared locks, the lock holder of a Shared lock
	// on a key is only allowed to read from the key while the lock is held.
	// This resolves the deadlock scenario presented above because only one of
	// the transactions would have been able to acquire an Upgrade lock at a
	// time while reading the initial state of the KVs. This means that the
	// Shared-to-Exclusive lock upgrade would never need to wait on another
	// transaction to release its locks.
	//
	// Under pure pessimistic concurrency control, an Update lock is equivalent
	// to an Exclusive lock. However, unlike with Exclusive locks, reads under
	// optimistic concurrency control do not conflict with Upgrade locks. This
	// is because a transaction can only hold an Upgrade lock on keys that it
	// has not yet modified. The trade-off here is that if the Upgrade lock
	// holder does convert its lock on a key to an Exclusive lock after an
	// optimistic read has observed the state of the key, the transaction that
	// performed the optimistic read may be unable to perform a successful read
	// refresh if it attempts to refresh to a timestamp at or past the timestamp
	// of the lock conversion.
	Upgrade

	// Exclusive (X) locks are used by read-write and read-only operations and
	// provide a transaction with exclusive access to a key. When an Exclusive
	// lock is held by a transaction on a given key, no other transaction can
	// read or write to that key. The lock holder is free to read and write to
	// the key as frequently as it would like.
	Exclusive
)

// Durability represents the different durability properties of a lock acquired
// by a transaction. Durability levels provide varying degrees of survivability,
// often in exchange for the cost of lock acquisition.
type Durability uint32

const (
	_ Durability = 1 << iota

	// Unreplicated locks are held only on a single Replica in a Range, which is
	// typically the leaseholder. Unreplicated locks are very fast to acquire
	// and release because they are held in memory or on fast local storage and
	// require no cross-node coordination to update. In exchange, Unreplicated
	// locks provide no guarantee of survivability across lease transfers or
	// leaseholder crashes. They should therefore be thought of as best-effort
	// and should not be relied upon for correctness.
	Unreplicated

	// Replicated locks are held on at least a quorum of Replicas in a Range.
	// They are slower to acquire and replease than Unreplicated locks because
	// updating them requires both cross-node coordination and interaction with
	// durable storage. In exchange, Replicated locks provide a guarantee of
	// survivability across lease transfers, leaseholder crashes, and other
	// forms of failure events. They will remain available as long as their
	// Range remains available and they will never be lost.
	Replicated
)
