// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package txnlock synthesizes replication locks for transactional logical data
// replication.
//
// # Replication Locks
//
// A replication lock is a (mode, kind, lock_id) triple:
//
//   - mode is one of {R, W}.
//   - kind is one of {primary_key, foreign_key_constraint,
//     unique_constraint}.
//   - lock_id is a datum tuple whose values depend on the lock's kind.
//
// Two rows or transactions conflict when they hold locks with the same
// lock_id and at least one of the locks is a write.
//
// # Primary Key Locks
//
// For a primary key lock, the lock_id is the primary key values of the
// row inserted, updated, or deleted. The primary key values are derived
// from the (prev_value, new_value) decoded from a rangefeed event.
// Because both values always share the same primary key, each row
// produces exactly one primary key lock.
//
// # Foreign Key Constraint Locks
//
// Foreign key constraint locks are derived from the (prev_value,
// new_value) decoded from a rangefeed event.
//
// If table_parent has tables that reference it via foreign key
// constraints, it generates a write lock: (W, foreign_key_constraint,
// referenced_columns). The write lock can be omitted when (prev_value,
// new_value) shows the referenced_columns did not change. If the
// replication event does not modify the referenced columns, the
// primary_key lock already orders it relative to any transaction that
// does modify the referenced columns, which is sufficient to ensure
// referential integrity.
//
// If table_child has a foreign key reference on table_parent, it
// generates a read lock: (R, foreign_key_constraint, reference_key),
// where reference_key is the referenced datums for the table_parent
// row referenced by table_child. A replicated row may generate two
// foreign key read locks for the same constraint if prev_value and
// new_value have different non-null reference_keys. The read lock can
// be omitted when prev_value and new_value have the same
// reference_key.
//
// If any column in the reference_key is NULL, no lock is generated for
// that constraint.
//
// # Unique Constraint Locks
//
// Unique constraint locks are derived from the (prev_value, new_value)
// decoded from a rangefeed event. The lock_id is the tuple of column
// values that form the unique constraint.
//
// A unique constraint lock is only generated when the constraint
// column values differ between prev_value and new_value. If both the
// old and new constraint values are non-null and different, the
// replication event generates two unique constraint locks: one for the
// old constraint values being released and one for the new constraint
// values being acquired.
//
// # Replication Locks vs Transaction Locks
//
// An alternative to lock inference is to record the locks acquired by
// the original transaction. This does not work because there are cases
// where the SQL engine validates a constraint using a non-locking read.
package txnlock
