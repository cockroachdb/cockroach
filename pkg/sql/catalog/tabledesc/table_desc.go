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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

var _ catalog.TableDescriptor = (*immutable)(nil)
var _ catalog.TableDescriptor = (*Mutable)(nil)
var _ catalog.MutableDescriptor = (*Mutable)(nil)
var _ catalog.TableDescriptor = (*wrapper)(nil)

// wrapper is the base implementation of the catalog.Descriptor
// interface, which is overloaded by immutable and Mutable.
type wrapper struct {
	descpb.TableDescriptor

	// mutationCache, indexCache and columnCache, when not nil, respectively point
	// to a struct containing precomputed catalog.Mutation, catalog.Index or
	// catalog.Column slices.
	// Those can therefore only be set when creating an immutable.
	mutationCache *mutationCache
	indexCache    *indexCache
	columnCache   *columnCache

	postDeserializationChanges PostDeserializationTableDescriptorChanges
}

// IsUncommittedVersion implements the catalog.Descriptor interface.
func (*wrapper) IsUncommittedVersion() bool {
	return false
}

// GetPostDeserializationChanges returns the set of changes which occurred to
// this descriptor post deserialization.
func (desc *wrapper) GetPostDeserializationChanges() PostDeserializationTableDescriptorChanges {
	return desc.postDeserializationChanges
}

// HasPostDeserializationChanges returns if the MutableDescriptor was changed after running
// RunPostDeserializationChanges.
func (desc *wrapper) HasPostDeserializationChanges() bool {
	return desc.postDeserializationChanges.UpgradedForeignKeyRepresentation ||
		desc.postDeserializationChanges.UpgradedFormatVersion ||
		desc.postDeserializationChanges.UpgradedIndexFormatVersion ||
		desc.postDeserializationChanges.UpgradedNamespaceName ||
		desc.postDeserializationChanges.UpgradedPrivileges
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

// immutable is a custom type for TableDescriptors
// It holds precomputed values and the underlying TableDescriptor
// should be const.
type immutable struct {
	wrapper

	allChecks []descpb.TableDescriptor_CheckConstraint

	// isUncommittedVersion is set to true if this descriptor was created from
	// a copy of a Mutable with an uncommitted version.
	isUncommittedVersion bool

	// TODO (lucy): populate these and use them
	// inboundFKs  []*ForeignKeyConstraint
	// outboundFKs []*ForeignKeyConstraint
}

// IsUncommittedVersion implements the Descriptor interface.
func (desc *immutable) IsUncommittedVersion() bool {
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

// ImmutableCopy implements the MutableDescriptor interface.
func (desc *Mutable) ImmutableCopy() catalog.Descriptor {
	if desc.IsUncommittedVersion() {
		return NewBuilderForUncommittedVersion(desc.TableDesc()).BuildImmutable()
	}
	return NewBuilder(desc.TableDesc()).BuildImmutable()
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

// UpdateIndexPartitioning applies the new partition and adjusts the column info
// for the specified index descriptor. Returns false iff this was a no-op.
func UpdateIndexPartitioning(
	idx *descpb.IndexDescriptor,
	isIndexPrimary bool,
	newImplicitCols []catalog.Column,
	newPartitioning descpb.PartitioningDescriptor,
) bool {
	oldNumImplicitCols := int(idx.Partitioning.NumImplicitColumns)
	isNoOp := oldNumImplicitCols == len(newImplicitCols) && idx.Partitioning.Equal(newPartitioning)
	numCols := len(idx.KeyColumnIDs)
	newCap := numCols + len(newImplicitCols) - oldNumImplicitCols
	newColumnIDs := make([]descpb.ColumnID, len(newImplicitCols), newCap)
	newColumnNames := make([]string, len(newImplicitCols), newCap)
	newColumnDirections := make([]descpb.IndexDescriptor_Direction, len(newImplicitCols), newCap)
	for i, col := range newImplicitCols {
		newColumnIDs[i] = col.GetID()
		newColumnNames[i] = col.GetName()
		newColumnDirections[i] = descpb.IndexDescriptor_ASC
		if isNoOp &&
			(idx.KeyColumnIDs[i] != newColumnIDs[i] ||
				idx.KeyColumnNames[i] != newColumnNames[i] ||
				idx.KeyColumnDirections[i] != newColumnDirections[i]) {
			isNoOp = false
		}
	}
	if isNoOp {
		return false
	}
	idx.KeyColumnIDs = append(newColumnIDs, idx.KeyColumnIDs[oldNumImplicitCols:]...)
	idx.KeyColumnNames = append(newColumnNames, idx.KeyColumnNames[oldNumImplicitCols:]...)
	idx.KeyColumnDirections = append(newColumnDirections, idx.KeyColumnDirections[oldNumImplicitCols:]...)
	idx.Partitioning = newPartitioning
	if !isIndexPrimary {
		return true
	}

	newStoreColumnIDs := make([]descpb.ColumnID, 0, len(idx.StoreColumnIDs))
	newStoreColumnNames := make([]string, 0, len(idx.StoreColumnNames))
	for i := range idx.StoreColumnIDs {
		id := idx.StoreColumnIDs[i]
		name := idx.StoreColumnNames[i]
		found := false
		for _, newColumnName := range newColumnNames {
			if newColumnName == name {
				found = true
				break
			}
		}
		if !found {
			newStoreColumnIDs = append(newStoreColumnIDs, id)
			newStoreColumnNames = append(newStoreColumnNames, name)
		}
	}
	idx.StoreColumnIDs = newStoreColumnIDs
	idx.StoreColumnNames = newStoreColumnNames
	if len(idx.StoreColumnNames) == 0 {
		idx.StoreColumnIDs = nil
		idx.StoreColumnNames = nil
	}
	return true
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
	return newIndexCache(desc.TableDesc(), desc.getExistingOrNewMutationCache())
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

// getExistingOrNewColumnCache should be the only place where the columnCache
// field in wrapper is ever read.
func (desc *wrapper) getExistingOrNewColumnCache() *columnCache {
	if desc.columnCache != nil {
		return desc.columnCache
	}
	return newColumnCache(desc.TableDesc(), desc.getExistingOrNewMutationCache())
}

// AllColumns returns a slice of Column interfaces containing the
// table's public columns and column mutations, in the canonical order:
// - all public columns in the same order as in the underlying
//   desc.TableDesc().Columns slice;
// - all column mutations in the same order as in the underlying
//   desc.TableDesc().Mutations slice.
// - all system columns defined in colinfo.AllSystemColumnDescs.
func (desc *wrapper) AllColumns() []catalog.Column {
	return desc.getExistingOrNewColumnCache().all
}

// PublicColumns returns a slice of Column interfaces containing the
// table's public columns, in the canonical order.
func (desc *wrapper) PublicColumns() []catalog.Column {
	return desc.getExistingOrNewColumnCache().public
}

// WritableColumns returns a slice of Column interfaces containing the
// table's public columns and DELETE_AND_WRITE_ONLY mutations, in the canonical
// order.
func (desc *wrapper) WritableColumns() []catalog.Column {
	return desc.getExistingOrNewColumnCache().writable
}

// DeletableColumns returns a slice of Column interfaces containing the
// table's public columns and mutations, in the canonical order.
func (desc *wrapper) DeletableColumns() []catalog.Column {
	return desc.getExistingOrNewColumnCache().deletable
}

// NonDropColumns returns a slice of Column interfaces containing the
// table's public columns and ADD mutations, in the canonical order.
func (desc *wrapper) NonDropColumns() []catalog.Column {
	return desc.getExistingOrNewColumnCache().nonDrop
}

// VisibleColumns returns a slice of Column interfaces containing the table's
// visible columns, in the canonical order. Visible columns are public columns
// with Hidden=false and Inaccessible=false. See ColumnDescriptor.Hidden and
// ColumnDescriptor.Inaccessible for more details.
func (desc *wrapper) VisibleColumns() []catalog.Column {
	return desc.getExistingOrNewColumnCache().visible
}

// AccessibleColumns returns a slice of Column interfaces containing the table's
// accessible columns, in the canonical order. Accessible columns are public
// columns with Inaccessible=false. See ColumnDescriptor.Inaccessible for more
// details.
func (desc *wrapper) AccessibleColumns() []catalog.Column {
	return desc.getExistingOrNewColumnCache().accessible
}

// UserDefinedTypeColumns returns a slice of Column interfaces
// containing the table's columns with user defined types, in the
// canonical order.
func (desc *wrapper) UserDefinedTypeColumns() []catalog.Column {
	return desc.getExistingOrNewColumnCache().withUDTs
}

// ReadableColumns is a list of columns (including those undergoing a schema
// change) which can be scanned. Columns in the process of a schema change
// are all set to nullable while column backfilling is still in
// progress, as mutation columns may have NULL values.
func (desc *wrapper) ReadableColumns() []catalog.Column {
	return desc.getExistingOrNewColumnCache().readable
}

// SystemColumns returns a slice of Column interfaces
// containing the table's system columns, as defined in
// colinfo.AllSystemColumnDescs.
func (desc *wrapper) SystemColumns() []catalog.Column {
	return desc.getExistingOrNewColumnCache().system
}

// FindColumnWithID returns the first column found whose ID matches the
// provided target ID, in the canonical order.
// If no column is found then an error is also returned.
func (desc *wrapper) FindColumnWithID(id descpb.ColumnID) (catalog.Column, error) {
	for _, col := range desc.AllColumns() {
		if col.GetID() == id {
			return col, nil
		}
	}

	return nil, pgerror.Newf(pgcode.UndefinedColumn, "column-id \"%d\" does not exist", id)
}

// FindColumnWithName returns the first column found whose name matches the
// provided target name, in the canonical order.
// If no column is found then an error is also returned.
func (desc *wrapper) FindColumnWithName(name tree.Name) (catalog.Column, error) {
	for _, col := range desc.AllColumns() {
		if col.ColName() == name {
			return col, nil
		}
	}
	return nil, colinfo.NewUndefinedColumnError(string(name))
}

// getExistingOrNewMutationCache should be the only place where the
// mutationCache field in wrapper is ever read.
func (desc *wrapper) getExistingOrNewMutationCache() *mutationCache {
	if desc.mutationCache != nil {
		return desc.mutationCache
	}
	return newMutationCache(desc.TableDesc())
}

// AllMutations returns all of the table descriptor's mutations.
func (desc *wrapper) AllMutations() []catalog.Mutation {
	return desc.getExistingOrNewMutationCache().all
}
