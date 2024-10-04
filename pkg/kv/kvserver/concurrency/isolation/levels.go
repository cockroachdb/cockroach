// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package isolation provides type definitions for isolation level-related
// concepts used by concurrency control in the key-value layer.
package isolation

import "github.com/gogo/protobuf/proto"

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

var levelNameLower = map[int32]string{
	int32(Serializable):  "serializable",
	int32(Snapshot):      "snapshot",
	int32(ReadCommitted): "read committed",
}

func init() {
	if len(levelNameLower) != len(Level_name) {
		panic("missing lower-case name for isolation level")
	}
}

// StringLower returns the lower-case name of the isolation level.
func (l Level) StringLower() string {
	return proto.EnumName(levelNameLower, int32(l))
}

// SafeValue implements the redact.SafeValue interface.
func (Level) SafeValue() {}

// Levels returns a list of all isolation levels, ordered from strongest to
// weakest.
func Levels() []Level { return []Level{Serializable, Snapshot, ReadCommitted} }

// RunEachLevel calls f in a subtest for each isolation level.
func RunEachLevel[T testingTB[T]](t T, f func(T, Level)) {
	for _, l := range Levels() {
		t.Run(l.String(), func(t T) { f(t, l) })
	}
}

// testingTB is an interface that matches *testing.T and *testing.B, without
// incurring the package dependency.
type testingTB[T any] interface {
	Run(name string, f func(t T)) bool
}
