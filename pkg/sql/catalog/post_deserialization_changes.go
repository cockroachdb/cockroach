// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

// Len returns length of the set of changes.
func (c PostDeserializationChanges) Len() int {
	return c.s.Len()
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
	// UpgradedFormatVersion indicates that the FormatVersion was upgraded.
	UpgradedFormatVersion PostDeserializationChangeType = iota

	// UpgradedIndexFormatVersion indicates that the format version of at least
	// one index descriptor was upgraded.
	UpgradedIndexFormatVersion

	// UpgradedForeignKeyRepresentation indicates that the foreign key
	// representation was upgraded.
	UpgradedForeignKeyRepresentation

	// UpgradedPrivileges indicates that the PrivilegeDescriptor version was upgraded.
	UpgradedPrivileges

	// RemovedDuplicateIDsInRefs indicates that the table
	// has redundant IDs in its DependsOn, DependsOnTypes and DependedOnBy
	// references.
	RemovedDuplicateIDsInRefs

	// AddedConstraintIDs indicates that table descriptors had constraint ID
	// added.
	AddedConstraintIDs

	// RemovedSelfEntryInSchemas corresponds to a change which occurred in
	// database descriptors to recover from an earlier bug whereby when
	// dropping a schema, we'd mark the database itself as though it was the
	// schema which was dropped.
	RemovedSelfEntryInSchemas

	// UpgradedSequenceReference indicates that the table/view had upgraded
	// their sequence references, if any, from by-name to by-ID, if not already.
	UpgradedSequenceReference

	// SetModTimeToMVCCTimestamp indicates that a descriptor's ModificationTime
	// field was unset and that the MVCC timestamp value was assigned to it.
	SetModTimeToMVCCTimestamp

	// SetCreateAsOfTimeUsingModTime indicates that a table's CreateAsOfTime field
	// was unset and the ModificationTime value was assigned to it.
	SetCreateAsOfTimeUsingModTime

	// SetSystemDatabaseDescriptorVersion indicates that the system database
	// descriptor did not have its version set.
	SetSystemDatabaseDescriptorVersion

	// UpgradedDeclarativeSchemaChangerState indicates the declarative schema changer
	// state was modified.
	UpgradedDeclarativeSchemaChangerState

	// SetIndexInvisibility indicates that the invisibility of at least one index
	// descriptor was updated to a non-zero value.
	SetIndexInvisibility

	// StrippedDanglingBackReferences indicates that at least one dangling
	// back-reference was removed from the descriptor.
	StrippedDanglingBackReferences

	// StrippedDanglingSelfBackReferences indicates that at least one dangling
	// back-reference to something within the descriptor itself was removed
	// from the descriptor.
	StrippedDanglingSelfBackReferences

	// FixSecondaryIndexEncodingType indicates that a secondary index had its
	// encoding type fixed, so it is not incorrectly marked as a primary index.
	FixSecondaryIndexEncodingType

	// GrantExecuteOnFunctionToPublicRole indicates that EXECUTE was granted
	// to the public role for a function.
	GrantExecuteOnFunctionToPublicRole

	// StrippedNonExistentRoles indicates that at least one role identified did
	// not exist.
	StrippedNonExistentRoles

	// FixedIncorrectForeignKeyOrigins indicates that foreign key origin /
	// reference IDs that should point to the current descriptor were fixed.
	FixedIncorrectForeignKeyOrigins

	// FixedUsesSequencesIDForIdentityColumns indicates sequence ID references
	// are fixed for identity / serial columns.
	FixedUsesSequencesIDForIdentityColumns
)
