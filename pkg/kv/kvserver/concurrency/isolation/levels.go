// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package isolation provides type definitions for isolation level-related
// concepts used by concurrency control in the key-value layer.
package isolation

// WeakerThan returns true if the receiver's strength is weaker than the
// parameter's strength. It returns false if the two isolation levels are
// equivalent or if the parameter's strength is weaker than the receiver's.
//
// WARNING: Because isolation levels are defined from strongest to weakest in
// their enumeration (which is important for backwards compatability), their
// value comparison semantics do not represent a comparison of strength. The
// equality operators (==, !=) may be used to match a specific isolation level,
// but ordering operators (<, <=, >, >=) must not be used. Use this method
// instead.
func (l Level) WeakerThan(l2 Level) bool {
	// Internally, we exploit the fact that the enum's value comparison semantics
	// represent a comparison of the inverse of strength. Users of isolation level
	// outside this package should not rely on this.
	return l > l2
}

// ToleratesWriteSkew returns whether the isolation level permits write skew. In
// an MVCC system, this property can be expressed as whether the isolation level
// allows transactions to write and commit at an MVCC timestamp above the MVCC
// timestamp of its read snapshot(s).
func (l Level) ToleratesWriteSkew() bool {
	return l.WeakerThan(Serializable)
}

// PerStatementReadSnapshot returns whether the isolation level establishes a
// new read snapshot for each statement. If not, a single read snapshot is used
// for the entire transaction.
func (l Level) PerStatementReadSnapshot() bool {
	return l == ReadCommitted
}

// SafeValue implements the redact.SafeValue interface.
func (Level) SafeValue() {}
