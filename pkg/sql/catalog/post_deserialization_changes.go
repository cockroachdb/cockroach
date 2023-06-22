// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catalog

import "github.com/cockroachdb/cockroach/pkg/util/intsets"

// PostDeserializationChangeType is used to indicate the type of
// PostDeserializationChange which occurred for a descriptor.
type PostDeserializationChangeType int

// PostDeserializationChanges are a set of booleans to indicate which types of
// upgrades or fixes occurred when filling in the descriptor after
// deserialization.
type PostDeserializationChanges struct{ s intsets.Fast }

// HasChanges returns true if the set of changes is non-empty.
func (c PostDeserializationChanges) HasChanges() bool {
	return !c.s.Empty()
}

// Add adds a change to the set of changes.
func (c *PostDeserializationChanges) Add(change PostDeserializationChangeType) {
	c.s.Add(int(change))
}

// ForEach calls f for every change in the set of changes.
func (c PostDeserializationChanges) ForEach(f func(change PostDeserializationChangeType)) {
	c.s.ForEach(func(i int) { f(PostDeserializationChangeType(i)) })
}

// Contains returns true if the set of changes contains this change.
func (c PostDeserializationChanges) Contains(change PostDeserializationChangeType) bool {
	return c.s.Contains(int(change))
}

const (
	// UpgradedPrivileges indicates that the PrivilegeDescriptor version was upgraded.
	UpgradedPrivileges PostDeserializationChangeType = iota

	// RemovedSelfEntryInSchemas corresponds to a change which occurred in
	// database descriptors to recover from an earlier bug whereby when
	// dropping a schema, we'd mark the database itself as though it was the
	// schema which was dropped.
	RemovedSelfEntryInSchemas

	// SetModTimeToMVCCTimestamp indicates that a descriptor's ModificationTime
	// field was unset and that the MVCC timestamp value was assigned to it.
	SetModTimeToMVCCTimestamp

	// SetCreateAsOfTimeUsingModTime indicates that a table's CreateAsOfTime field
	// was unset and the ModificationTime value was assigned to it.
	SetCreateAsOfTimeUsingModTime

	// SetSystemDatabaseDescriptorVersion indicates that the system database
	// descriptor did not have its version set.
	SetSystemDatabaseDescriptorVersion

	// SetCheckConstraintColumnIDs indicates that a table's check constraint's
	// ColumnIDs slice hadn't been set yet, and was set to a non-empty slice.
	SetCheckConstraintColumnIDs

	// UpgradedDeclarativeSchemaChangerState indicates the declarative schema changer
	// state was modified.
	UpgradedDeclarativeSchemaChangerState

	// SetIndexInvisibility indicates that the invisibility of at least one index
	// descriptor was updated to a non-zero value.
	SetIndexInvisibility
)
