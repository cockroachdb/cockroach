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
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

var _ catalog.TableDescriptor = (*Immutable)(nil)
var _ catalog.TableDescriptor = (*Mutable)(nil)
var _ catalog.MutableDescriptor = (*Mutable)(nil)
var _ catalog.TableDescriptor = (*wrapper)(nil)

// wrapper is the base implementation of the catalog.Descriptor
// interface, which is overloaded by Immutable and Mutable.
type wrapper struct {
	descpb.TableDescriptor

	// indexCache, when not nil, points to a struct containing precomputed
	// catalog.Index slices. This can therefore only be set when creating an
	// Immutable.
	indexCache *indexCache

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

// mutationColumns returns all non-public writable columns in the specified
// state.
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

	writeOnlyColCount int

	allChecks []descpb.TableDescriptor_CheckConstraint

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

// ReadableColumns returns a list of columns (including those undergoing a
// schema change) which can be scanned.
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

// RemovePublicNonPrimaryIndex removes a secondary index by ordinal.
// indexOrdinal must be in range [1, len(desc.Indexes)], 0 denotes the primary
// index.
func (desc *Mutable) RemovePublicNonPrimaryIndex(indexOrdinal int) {
	desc.Indexes = append(desc.Indexes[:indexOrdinal-1], desc.Indexes[indexOrdinal:]...)
}

// SetPublicNonPrimaryIndexes replaces all existing secondary indexes with new
// ones passed to it.
func (desc *Mutable) SetPublicNonPrimaryIndexes(indexes []descpb.IndexDescriptor) {
	desc.Indexes = append(desc.Indexes[:0], indexes...)
}

// AddPublicNonPrimaryIndex adds a new secondary index.
func (desc *Mutable) AddPublicNonPrimaryIndex(index descpb.IndexDescriptor) {
	desc.Indexes = append(desc.Indexes, index)
}

// SetPrimaryIndex sets the primary index.
func (desc *Mutable) SetPrimaryIndex(index descpb.IndexDescriptor) {
	desc.PrimaryIndex = index
}

// SetPublicNonPrimaryIndex sets one of the secondary indexes.
// indexOrdinal must be in range [1, len(desc.Indexes)], 0 denotes the primary
// index.
func (desc *Mutable) SetPublicNonPrimaryIndex(indexOrdinal int, index descpb.IndexDescriptor) {
	desc.Indexes[indexOrdinal-1] = index
}

// GetPrimaryIndex returns the primary index in the form of a catalog.Index
// interface.
func (desc *wrapper) GetPrimaryIndex() catalog.Index {
	return desc.getExistingOrNewIndexCache().primary
}

// getExistingOrNewIndexCache should be the only place where the indexCache
// field in wrapper is ever read.
func (desc *wrapper) getExistingOrNewIndexCache() *indexCache {
	if desc.indexCache != nil {
		return desc.indexCache
	}
	return newIndexCache(desc.TableDesc())
}

// AllIndexes returns a slice with all indexes, public and non-public,
// in the underlying proto, in their canonical order:
// - the primary index,
// - the public non-primary indexes in the Indexes array, in order,
// - the non-public indexes present in the Mutations array, in order.
//
// See also catalog.Index.Ordinal().
func (desc *wrapper) AllIndexes() []catalog.Index {
	return desc.getExistingOrNewIndexCache().all
}

// ActiveIndexes returns a slice with all public indexes in the underlying
// proto, in their canonical order:
// - the primary index,
// - the public non-primary indexes in the Indexes array, in order.
//
// See also catalog.Index.Ordinal().
func (desc *wrapper) ActiveIndexes() []catalog.Index {
	return desc.getExistingOrNewIndexCache().active
}

// NonDropIndexes returns a slice of all non-drop indexes in the underlying
// proto, in their canonical order. This means:
// - the primary index, if the table is a physical table,
// - the public non-primary indexes in the Indexes array, in order,
// - the non-public indexes present in the Mutations array, in order,
//   if the mutation is not a drop.
//
// See also catalog.Index.Ordinal().
func (desc *wrapper) NonDropIndexes() []catalog.Index {
	return desc.getExistingOrNewIndexCache().nonDrop
}

// NonDropIndexes returns a slice of all partial indexes in the underlying
// proto, in their canonical order. This is equivalent to taking the slice
// produced by AllIndexes and filtering indexes with non-empty expressions.
func (desc *wrapper) PartialIndexes() []catalog.Index {
	return desc.getExistingOrNewIndexCache().partial
}

// PublicNonPrimaryIndexes returns a slice of all active secondary indexes,
// in their canonical order. This is equivalent to the Indexes array in the
// proto.
func (desc *wrapper) PublicNonPrimaryIndexes() []catalog.Index {
	return desc.getExistingOrNewIndexCache().publicNonPrimary
}

// WritableNonPrimaryIndexes returns a slice of all non-primary indexes which
// allow being written to: public + delete-and-write-only, in their canonical
// order. This is equivalent to taking the slice produced by
// DeletableNonPrimaryIndexes and removing the indexes which are in mutations
// in the delete-only state.
func (desc *wrapper) WritableNonPrimaryIndexes() []catalog.Index {
	return desc.getExistingOrNewIndexCache().writableNonPrimary
}

// DeletableNonPrimaryIndexes returns a slice of all non-primary indexes
// which allow being deleted from: public + delete-and-write-only +
// delete-only, in  their canonical order. This is equivalent to taking
// the slice produced by AllIndexes and removing the primary index.
func (desc *wrapper) DeletableNonPrimaryIndexes() []catalog.Index {
	return desc.getExistingOrNewIndexCache().deletableNonPrimary
}

// DeleteOnlyNonPrimaryIndexes returns a slice of all non-primary indexes
// which allow only being deleted from, in their canonical order. This is
// equivalent to taking the slice produced by DeletableNonPrimaryIndexes and
// removing the indexes which are not in mutations or not in the delete-only
// state.
func (desc *wrapper) DeleteOnlyNonPrimaryIndexes() []catalog.Index {
	return desc.getExistingOrNewIndexCache().deleteOnlyNonPrimary
}

// FindIndexWithID returns the first catalog.Index that matches the id
// in the set of all indexes, or an error if none was found. The order of
// traversal is the canonical order, see catalog.Index.Ordinal().
func (desc *wrapper) FindIndexWithID(id descpb.IndexID) (catalog.Index, error) {
	if idx := catalog.FindIndex(desc, catalog.IndexOpts{
		NonPhysicalPrimaryIndex: true,
		DropMutations:           true,
		AddMutations:            true,
	}, func(idx catalog.Index) bool {
		return idx.GetID() == id
	}); idx != nil {
		return idx, nil
	}
	for _, m := range desc.GCMutations {
		if m.IndexID == id {
			return nil, ErrIndexGCMutationsList
		}
	}
	return nil, errors.Errorf("index-id \"%d\" does not exist", id)
}

// FindIndexWithName returns the first catalog.Index that matches the name in
// the set of all indexes, excluding the primary index of non-physical
// tables, or an error if none was found. The order of traversal is the
// canonical order, see catalog.Index.Ordinal().
func (desc *wrapper) FindIndexWithName(name string) (catalog.Index, error) {
	if idx := catalog.FindIndex(desc, catalog.IndexOpts{
		NonPhysicalPrimaryIndex: false,
		DropMutations:           true,
		AddMutations:            true,
	}, func(idx catalog.Index) bool {
		return idx.GetName() == name
	}); idx != nil {
		return idx, nil
	}
	return nil, errors.Errorf("index %q does not exist", name)
}
