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

import "github.com/cockroachdb/cockroach/pkg/util"

// PostDeserializationChangeType is used to indicate the type of
// PostDeserializationChange which occurred for a descriptor.
type PostDeserializationChangeType int

// PostDeserializationChanges are a set of booleans to indicate which types of
// upgrades or fixes occurred when filling in the descriptor after
// deserialization.
type PostDeserializationChanges struct{ s util.FastIntSet }

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
	// UpgradedFormatVersion indicates that the FormatVersion was upgraded.
	UpgradedFormatVersion PostDeserializationChangeType = iota

	// FixedIndexEncodingType indicates that the encoding type of a public index
	// was fixed.
	FixedIndexEncodingType

	// UpgradedIndexFormatVersion indicates that the format version of at least
	// one index descriptor was upgraded.
	UpgradedIndexFormatVersion

	// UpgradedForeignKeyRepresentation indicates that the foreign key
	// representation was upgraded.
	UpgradedForeignKeyRepresentation

	// UpgradedNamespaceName indicates that the table was system.namespace
	// and it had its name upgraded from "namespace2".
	//
	// TODO(ajwerner): Remove this and the associated migration in 22.1 as
	// this will never be true due to the corresponding long-running migration.
	UpgradedNamespaceName

	// UpgradedPrivileges indicates that the PrivilegeDescriptor version was upgraded.
	UpgradedPrivileges

	// RemovedDefaultExprFromComputedColumn indicates that the table had at least
	// one computed column which also had a DEFAULT expression, which therefore
	// had to be removed. See issue #72881 for details.
	RemovedDefaultExprFromComputedColumn

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
)
