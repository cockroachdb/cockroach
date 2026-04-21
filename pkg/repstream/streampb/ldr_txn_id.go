// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package streampb

import "fmt"

// Less reports whether t sorts before s by timestamp.
func (t LDRTxnID) Less(s LDRTxnID) bool { return t.Timestamp.Less(s.Timestamp) }

// LessEq reports whether t sorts before or equal to s by timestamp.
func (t LDRTxnID) LessEq(s LDRTxnID) bool {
	return t.Timestamp.LessEq(s.Timestamp)
}

// IsSet reports whether the timestamp is non-zero.
func (t LDRTxnID) IsSet() bool { return t.Timestamp.IsSet() }

// String returns a human-readable representation.
func (t LDRTxnID) String() string {
	return fmt.Sprintf("(%d,%d)", t.ApplierID, t.Timestamp.WallTime)
}
