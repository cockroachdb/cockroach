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
// This method is deprecated, use DeleteOnlyNonPrimaryIndexes instead.
func (desc *wrapper) DeleteOnlyIndexes() []descpb.IndexDescriptor {
	return desc.mutationIndexes(descpb.DescriptorMutation_DELETE_ONLY)
}

// WritableIndexes returns a list of public and write-only mutation indexes.
// This method is deprecated, use WritableNonPrimaryIndexes instead.
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

// DeletableIndexes returns a list of deletable indexes.
// This method is deprecated, use DeletableNonPrimaryIndexes instead.
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

// GetPublicNonPrimaryIndexes returns the public non-primary indexes of the
// descriptor.
// This method is deprecated, use PublicNonPrimaryIndexes instead.
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

// PrimaryIndexInterface returns the primary index in the form of a
// catalog.Index interface.
func (desc *wrapper) PrimaryIndexInterface() catalog.Index {
	return index{desc: &desc.PrimaryIndex}
}

// getExistingOrNewIndexCache should be the only place where the indexCache
// field in wrapper is ever read.
func (desc *wrapper) getExistingOrNewIndexCache() *indexCache {
	if desc.indexCache == nil {
		return &indexCache{}
	}
	return desc.indexCache
}

// AllIndexes returns a slice containing all indexes represented in the table
// descriptor, including mutations.
func (desc *wrapper) AllIndexes() []catalog.Index {
	return desc.getExistingOrNewIndexCache().allIndexes(desc)
}

// ActiveIndexes returns a slice of all active (aka public) indexes.
func (desc *wrapper) ActiveIndexes() []catalog.Index {
	return desc.getExistingOrNewIndexCache().activeIndexes(desc)
}

// NonDropIndexes returns a slice of all indexes (including mutations) which are
// not being dropped.
func (desc *wrapper) NonDropIndexes() []catalog.Index {
	return desc.getExistingOrNewIndexCache().nonDropIndexes(desc)
}

// PartialIndexes returns a slice of all partial indexes in the table
// descriptor, including mutations.
func (desc *wrapper) PartialIndexes() []catalog.Index {
	return desc.getExistingOrNewIndexCache().partialIndexes(desc)
}

// PublicNonPrimaryIndexes returns a slice of all active secondary indexes.
func (desc *wrapper) PublicNonPrimaryIndexes() []catalog.Index {
	return desc.getExistingOrNewIndexCache().publicNonPrimaryIndexes(desc)
}

// WritableNonPrimaryIndexes returns a slice of all secondary indexes which
// allow being written to: active + delete-and-write-only.
func (desc *wrapper) WritableNonPrimaryIndexes() []catalog.Index {
	return desc.getExistingOrNewIndexCache().writableNonPrimaryIndexes(desc)
}

// DeletableNonPrimaryIndexes returns a slice of all secondary indexes which
// allow being deleted from: active + delete-and-write-only + delete-only.
func (desc *wrapper) DeletableNonPrimaryIndexes() []catalog.Index {
	return desc.getExistingOrNewIndexCache().deletableNonPrimaryIndexes(desc)
}

// DeletableNonPrimaryIndexes returns a slice of all secondary indexes which
// only allow being deleted from.
func (desc *wrapper) DeleteOnlyNonPrimaryIndexes() []catalog.Index {
	return desc.getExistingOrNewIndexCache().deleteOnlyNonPrimaryIndexes(desc)
}

// ForEachIndex runs f over each index in the table descriptor according to
// filter parameters in opts.
func (desc *wrapper) ForEachIndex(opts catalog.IndexOpts, f func(idx catalog.Index) error) error {
	for _, idx := range desc.AllIndexes() {
		if !opts.NonPhysicalPrimaryIndex && idx.Primary() && !desc.IsPhysicalTable() {
			continue
		}
		if !opts.AddMutations && idx.Adding() {
			continue
		}
		if !opts.DropMutations && idx.Dropped() {
			continue
		}
		if err := f(idx); err != nil {
			return err
		}
	}
	return nil
}

func forEachIndex(slice []catalog.Index, f func(idx catalog.Index) error) error {
	for _, idx := range slice {
		if err := f(idx); err != nil {
			return err
		}
	}
	return nil
}

// ForEachActiveIndex is like ForEachIndex over ActiveIndexes().
func (desc *wrapper) ForEachActiveIndex(f func(idx catalog.Index) error) error {
	return forEachIndex(desc.ActiveIndexes(), f)
}

// ForEachNonDropIndex is like ForEachIndex over NonDropIndexes().
func (desc *wrapper) ForEachNonDropIndex(f func(idx catalog.Index) error) error {
	return forEachIndex(desc.NonDropIndexes(), f)
}

// ForEachPartialIndex is like ForEachIndex over PartialIndexes().
func (desc *wrapper) ForEachPartialIndex(f func(idx catalog.Index) error) error {
	return forEachIndex(desc.PartialIndexes(), f)
}

// ForEachPublicNonPrimaryIndex is like ForEachIndex over
// PublicNonPrimaryIndexes().
func (desc *wrapper) ForEachPublicNonPrimaryIndex(f func(idx catalog.Index) error) error {
	return forEachIndex(desc.PublicNonPrimaryIndexes(), f)
}

// ForEachWritableNonPrimaryIndex is like ForEachIndex over
// WritableNonPrimaryIndexes().
func (desc *wrapper) ForEachWritableNonPrimaryIndex(f func(idx catalog.Index) error) error {
	return forEachIndex(desc.WritableNonPrimaryIndexes(), f)
}

// ForEachDeletableNonPrimaryIndex is like ForEachIndex over
// DeletableNonPrimaryIndexes().
func (desc *wrapper) ForEachDeletableNonPrimaryIndex(f func(idx catalog.Index) error) error {
	return forEachIndex(desc.DeletableNonPrimaryIndexes(), f)
}

// ForEachDeleteOnlyNonPrimaryIndex is like ForEachIndex over
// DeleteOnlyNonPrimaryIndexes().
func (desc *wrapper) ForEachDeleteOnlyNonPrimaryIndex(f func(idx catalog.Index) error) error {
	return forEachIndex(desc.DeleteOnlyNonPrimaryIndexes(), f)
}

// FindIndex returns the first index for which test returns true, nil otherwise,
// according to the parameters in opts just like ForEachIndex.
func (desc *wrapper) FindIndex(
	opts catalog.IndexOpts, test func(idx catalog.Index) bool,
) catalog.Index {
	for _, idx := range desc.AllIndexes() {
		if !opts.NonPhysicalPrimaryIndex && idx.Primary() && !desc.IsPhysicalTable() {
			continue
		}
		if !opts.AddMutations && idx.Adding() {
			continue
		}
		if !opts.DropMutations && idx.Dropped() {
			continue
		}
		if test(idx) {
			return idx
		}
	}
	return nil
}

func findIndex(slice []catalog.Index, test func(idx catalog.Index) bool) catalog.Index {
	for _, idx := range slice {
		if test(idx) {
			return idx
		}
	}
	return nil
}

// FindActiveIndex returns the first index in ActiveIndex() for which test
// returns true.
func (desc *wrapper) FindActiveIndex(test func(idx catalog.Index) bool) catalog.Index {
	return findIndex(desc.ActiveIndexes(), test)
}

// FindNonDropIndex returns the first index in NonDropIndex() for which test
// returns true.
func (desc *wrapper) FindNonDropIndex(test func(idx catalog.Index) bool) catalog.Index {
	return findIndex(desc.NonDropIndexes(), test)
}

// FindPartialIndex returns the first index in PartialIndex() for which test
// returns true.
func (desc *wrapper) FindPartialIndex(test func(idx catalog.Index) bool) catalog.Index {
	return findIndex(desc.PartialIndexes(), test)
}

// FindPublicNonPrimaryIndex returns the first index in PublicNonPrimaryIndex()
// for which test returns true.
func (desc *wrapper) FindPublicNonPrimaryIndex(test func(idx catalog.Index) bool) catalog.Index {
	return findIndex(desc.PublicNonPrimaryIndexes(), test)
}

// FindWritableNonPrimaryIndex returns the first index in
// WritableNonPrimaryIndex() for which test returns true.
func (desc *wrapper) FindWritableNonPrimaryIndex(test func(idx catalog.Index) bool) catalog.Index {
	return findIndex(desc.WritableNonPrimaryIndexes(), test)
}

// FindDeletableNonPrimaryIndex returns the first index in
// DeletableNonPrimaryIndex() for which test returns true.
func (desc *wrapper) FindDeletableNonPrimaryIndex(test func(idx catalog.Index) bool) catalog.Index {
	return findIndex(desc.DeletableNonPrimaryIndexes(), test)
}

// FindDeleteOnlyNonPrimaryIndex returns the first index in
// DeleteOnlyNonPrimaryIndex() for which test returns true.
func (desc *wrapper) FindDeleteOnlyNonPrimaryIndex(
	test func(idx catalog.Index) bool,
) catalog.Index {
	return findIndex(desc.DeleteOnlyNonPrimaryIndexes(), test)
}

// FindIndexWithID returns the first catalog.Index that matches the id
// in the set of all indexes.
func (desc *wrapper) FindIndexWithID(id descpb.IndexID) (catalog.Index, error) {
	if idx := desc.FindIndex(catalog.IndexOpts{
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
// the set of all indexes, excluding the primary index of non-physical tables.
func (desc *wrapper) FindIndexWithName(name string) (catalog.Index, error) {
	if idx := desc.FindIndex(catalog.IndexOpts{
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
