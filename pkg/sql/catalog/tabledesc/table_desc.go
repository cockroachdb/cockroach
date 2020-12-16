// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package tabledesc provides concrete implementations of catalog.TableDesc.
package tabledesc

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

var _ catalog.TableDescriptor = (*Immutable)(nil)
var _ catalog.TableDescriptor = (*Mutable)(nil)
var _ catalog.MutableDescriptor = (*Mutable)(nil)
var _ catalog.TableDescriptor = (*wrapper)(nil)

// wrapper is the base implementation of the catalog.Descriptor
// interface, which is overloaded by Immutable and Mutable.
type wrapper struct {
	descpb.TableDescriptor
	postDeserializationChanges PostDeserializationTableDescriptorChanges
}

// NameResolutionResult implements the tree.NameResolutionResult interface.
func (*wrapper) NameResolutionResult() {}

// IsUncommittedVersion implements the catalog.Descriptor interface.
func (*wrapper) IsUncommittedVersion() bool {
	return false
}

// GetPostDeserializationChanges returns the set of changes which occurred to
// this descriptor post deserialization.
func (desc *wrapper) GetPostDeserializationChanges() PostDeserializationTableDescriptorChanges {
	return desc.postDeserializationChanges
}

// PartialIndexOrds returns a set containing the ordinal of each partial index
// defined on the table.
func (desc *wrapper) PartialIndexOrds() util.FastIntSet {
	var partialIndexOrds util.FastIntSet
	for i, idx := range desc.DeletableIndexes() {
		if idx.IsPartial() {
			partialIndexOrds.Add(i)
		}
	}
	return partialIndexOrds
}

// mutationIndexes returns all non-public indexes in the specified state.
func (desc *wrapper) mutationIndexes(
	mutationState descpb.DescriptorMutation_State,
) []descpb.IndexDescriptor {
	if len(desc.Mutations) == 0 {
		return nil
	}
	indexes := make([]descpb.IndexDescriptor, 0, len(desc.Mutations))
	for _, m := range desc.Mutations {
		if m.State != mutationState {
			continue
		}
		if idx := m.GetIndex(); idx != nil {
			indexes = append(indexes, *idx)
		}
	}
	return indexes
}

// DeleteOnlyIndexes returns a list of delete-only mutation indexes.
func (desc *wrapper) DeleteOnlyIndexes() []descpb.IndexDescriptor {
	return desc.mutationIndexes(descpb.DescriptorMutation_DELETE_ONLY)
}

// WritableIndexes returns a list of public and write-only mutation indexes.
func (desc *wrapper) WritableIndexes() []descpb.IndexDescriptor {
	if len(desc.Mutations) == 0 {
		return desc.Indexes
	}
	indexes := make([]descpb.IndexDescriptor, 0, len(desc.Indexes)+len(desc.Mutations))
	// Add all public indexes.
	indexes = append(indexes, desc.Indexes...)
	// Add all non-public writable indexes.
	indexes = append(indexes, desc.mutationIndexes(descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY)...)
	return indexes
}

// DeletableIndexes implements the catalog.Descriptor interface.
func (desc *wrapper) DeletableIndexes() []descpb.IndexDescriptor {
	if len(desc.Mutations) == 0 {
		return desc.Indexes
	}
	indexes := make([]descpb.IndexDescriptor, 0, len(desc.Indexes)+len(desc.Mutations))
	// Add all writable indexes.
	indexes = append(indexes, desc.WritableIndexes()...)
	// Add all delete-only indexes.
	indexes = append(indexes, desc.DeleteOnlyIndexes()...)
	return indexes
}

// mutationColumns returns all non-public writable columns in the specified state.
func (desc *wrapper) mutationColumns(
	mutationState descpb.DescriptorMutation_State,
) []descpb.ColumnDescriptor {
	if len(desc.Mutations) == 0 {
		return nil
	}
	columns := make([]descpb.ColumnDescriptor, 0, len(desc.Mutations))
	for _, m := range desc.Mutations {
		if m.State != mutationState {
			continue
		}
		if col := m.GetColumn(); col != nil {
			columns = append(columns, *col)
		}
	}
	return columns
}

// DeletableColumns returns a list of public and non-public columns.
func (desc *wrapper) DeletableColumns() []descpb.ColumnDescriptor {
	if len(desc.Mutations) == 0 {
		return desc.Columns
	}
	columns := make([]descpb.ColumnDescriptor, 0, len(desc.Columns)+len(desc.Mutations))
	// Add writable columns.
	columns = append(columns, desc.WritableColumns()...)
	// Add delete-only columns.
	columns = append(columns, desc.mutationColumns(descpb.DescriptorMutation_DELETE_ONLY)...)
	return columns
}

// WritableColumns returns a list of public and write-only mutation columns.
func (desc *wrapper) WritableColumns() []descpb.ColumnDescriptor {
	if len(desc.Mutations) == 0 {
		return desc.Columns
	}
	columns := make([]descpb.ColumnDescriptor, 0, len(desc.Columns)+len(desc.Mutations))
	// Add public columns.
	columns = append(columns, desc.Columns...)
	// Add writable non-public columns.
	columns = append(columns, desc.mutationColumns(descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY)...)
	return columns
}

// MutationColumns returns a list of mutation columns.
func (desc *wrapper) MutationColumns() []descpb.ColumnDescriptor {
	if len(desc.Mutations) == 0 {
		return nil
	}
	columns := make([]descpb.ColumnDescriptor, 0, len(desc.Mutations))
	// Add all writable non-public columns.
	columns = append(columns, desc.mutationColumns(descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY)...)
	// Add all delete-only non-public columns.
	columns = append(columns, desc.mutationColumns(descpb.DescriptorMutation_DELETE_ONLY)...)
	return columns
}

// ActiveChecks returns a list of all check constraints that should be enforced
// on writes (including constraints being added/validated). The columns
// referenced by the returned checks are writable, but not necessarily public.
func (desc *wrapper) ActiveChecks() []descpb.TableDescriptor_CheckConstraint {
	checks := make([]descpb.TableDescriptor_CheckConstraint, len(desc.Checks))
	for i, c := range desc.Checks {
		checks[i] = *c
	}
	return checks
}

// GetColumnOrdinalsWithUserDefinedTypes returns a slice of column ordinals
// of columns that contain user defined types.
func (desc *wrapper) GetColumnOrdinalsWithUserDefinedTypes() []int {
	var ords []int
	for ord, col := range desc.DeletableColumns() {
		if col.Type != nil && col.Type.UserDefined() {
			ords = append(ords, ord)
		}
	}
	return ords
}

// Immutable is a custom type for TableDescriptors
// It holds precomputed values and the underlying TableDescriptor
// should be const.
type Immutable struct {
	wrapper

	// publicAndNonPublicCols is a list of public and non-public columns.
	// It is partitioned by the state of the column: public, write-only, delete-only
	publicAndNonPublicCols []descpb.ColumnDescriptor

	// publicAndNonPublicCols is a list of public and non-public indexes.
	// It is partitioned by the state of the index: public, write-only, delete-only
	publicAndNonPublicIndexes []descpb.IndexDescriptor

	writeOnlyColCount   int
	writeOnlyIndexCount int

	allChecks []descpb.TableDescriptor_CheckConstraint

	// partialIndexOrds contains the ordinal of each partial index.
	partialIndexOrds util.FastIntSet

	// readableColumns is a list of columns (including those undergoing a schema change)
	// which can be scanned. Columns in the process of a schema change
	// are all set to nullable while column backfilling is still in
	// progress, as mutation columns may have NULL values.
	readableColumns []descpb.ColumnDescriptor

	// columnsWithUDTs is a set of indexes into publicAndNonPublicCols containing
	// indexes of columns that contain user defined types.
	columnsWithUDTs []int

	// isUncommittedVersion is set to true if this descriptor was created from
	// a copy of a Mutable with an uncommitted version.
	isUncommittedVersion bool

	// TODO (lucy): populate these and use them
	// inboundFKs  []*ForeignKeyConstraint
	// outboundFKs []*ForeignKeyConstraint
}

// IsUncommittedVersion implements the Descriptor interface.
func (desc *Immutable) IsUncommittedVersion() bool {
	return desc.isUncommittedVersion
}

// DescriptorProto prepares desc for serialization.
func (desc *wrapper) DescriptorProto() *descpb.Descriptor {
	return &descpb.Descriptor{
		Union: &descpb.Descriptor_Table{Table: &desc.TableDescriptor},
	}
}

// GetPrimaryIndexID returns the ID of the primary index.
func (desc *wrapper) GetPrimaryIndexID() descpb.IndexID {
	return desc.PrimaryIndex.ID
}

// GetPublicNonPrimaryIndexes returns the public non-primary indexes of the descriptor.
func (desc *wrapper) GetPublicNonPrimaryIndexes() []descpb.IndexDescriptor {
	return desc.Indexes
}

// IsTemporary returns true if this is a temporary table.
func (desc *wrapper) IsTemporary() bool {
	return desc.GetTemporary()
}

// GetPublicColumns return the public columns in the descriptor.
func (desc *wrapper) GetPublicColumns() []descpb.ColumnDescriptor {
	return desc.Columns
}

// GetColumnAtIdx returns the column at the specified index.
func (desc *wrapper) GetColumnAtIdx(idx int) *descpb.ColumnDescriptor {
	return &desc.Columns[idx]
}

// ReadableColumns implements the catalog.TableDescriptor interface
func (desc *Immutable) ReadableColumns() []descpb.ColumnDescriptor {
	return desc.readableColumns
}

// ImmutableCopy implements the MutableDescriptor interface.
func (desc *Mutable) ImmutableCopy() catalog.Descriptor {
	// TODO (lucy): Should the immutable descriptor constructors always make a
	// copy, so we don't have to do it here?
	imm := NewImmutable(*protoutil.Clone(desc.TableDesc()).(*descpb.TableDescriptor))
	imm.isUncommittedVersion = desc.IsUncommittedVersion()
	return imm
}

// IsUncommittedVersion implements the Descriptor interface.
func (desc *Mutable) IsUncommittedVersion() bool {
	return desc.IsNew() || desc.GetVersion() != desc.ClusterVersion.GetVersion()
}

// SetDrainingNames implements the MutableDescriptor interface.
func (desc *Mutable) SetDrainingNames(names []descpb.NameInfo) {
	desc.DrainingNames = names
}
