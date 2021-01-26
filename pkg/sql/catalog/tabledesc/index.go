// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tabledesc

import (
	"github.com/cockroachdb/cockroach/pkg/geo/geoindex"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

var _ catalog.Index = (*index)(nil)

// index implements the catalog.Index interface by wrapping the protobuf index
// descriptor along with some metadata from its parent table descriptor.
type index struct {
	desc              *descpb.IndexDescriptor
	ordinal           int
	mutationID        descpb.MutationID
	mutationDirection descpb.DescriptorMutation_Direction
	mutationState     descpb.DescriptorMutation_State
}

// IndexDesc returns the underlying protobuf descriptor.
// Ideally, this method should be called as rarely as possible.
func (w index) IndexDesc() *descpb.IndexDescriptor {
	return w.desc
}

// IndexDescDeepCopy returns a deep copy of the underlying protobuf descriptor.
func (w index) IndexDescDeepCopy() descpb.IndexDescriptor {
	return *protoutil.Clone(w.desc).(*descpb.IndexDescriptor)
}

// Ordinal returns the ordinal of the index in its parent TableDescriptor.
// The ordinal is defined as follows:
// - 0 is the ordinal of the primary index,
// - [1:1+len(desc.Indexes)] is the range of public non-primary indexes,
// - [1+len(desc.Indexes):] is the range of non-public indexes.
func (w index) Ordinal() int {
	return w.ordinal
}

// Primary returns true iff the index is the primary index for the table
// descriptor.
func (w index) Primary() bool {
	return w.ordinal == 0
}

// Public returns true iff the index is active, i.e. readable.
func (w index) Public() bool {
	return w.mutationState == descpb.DescriptorMutation_UNKNOWN
}

// Adding returns true iff the index is an add mutation in the table descriptor.
func (w index) Adding() bool {
	return w.mutationDirection == descpb.DescriptorMutation_ADD
}

// Dropped returns true iff the index is a drop mutation in the table descriptor.
func (w index) Dropped() bool {
	return w.mutationDirection == descpb.DescriptorMutation_DROP
}

// WriteAndDeleteOnly returns true iff the index is a mutation in the
// delete-and-write-only state.
func (w index) WriteAndDeleteOnly() bool {
	return w.mutationState == descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY
}

// DeleteOnly returns true iff the index is a mutation in the delete-only state.
func (w index) DeleteOnly() bool {
	return w.mutationState == descpb.DescriptorMutation_DELETE_ONLY
}

// GetID returns the index ID.
func (w index) GetID() descpb.IndexID {
	return w.desc.ID
}

// GetName returns the index name.
func (w index) GetName() string {
	return w.desc.Name
}

// IsInterleaved returns true iff the index is interleaved.
func (w index) IsInterleaved() bool {
	return w.desc.IsInterleaved()
}

// IsPartial returns true iff the index is a partial index.
func (w index) IsPartial() bool {
	return w.desc.IsPartial()
}

// IsUnique returns true iff the index is a unique index.
func (w index) IsUnique() bool {
	return w.desc.Unique
}

// IsDisabled returns true iff the index is disabled.
func (w index) IsDisabled() bool {
	return w.desc.Disabled
}

// IsSharded returns true iff the index is hash sharded.
func (w index) IsSharded() bool {
	return w.desc.IsSharded()
}

// IsCreatedExplicitly returns true iff this index was created explicitly, i.e.
// via 'CREATE INDEX' statement.
func (w index) IsCreatedExplicitly() bool {
	return w.desc.CreatedExplicitly
}

// GetPredicate returns the empty string when the index is not partial,
// otherwise it returns the corresponding expression of the partial index.
// Columns are referred to in the expression by their name.
func (w index) GetPredicate() string {
	return w.desc.Predicate
}

// GetType returns the type of index, inverted or forward.
func (w index) GetType() descpb.IndexDescriptor_Type {
	return w.desc.Type
}

// GetPartitioning returns the partitioning descriptor of the index.
func (w index) GetPartitioning() descpb.PartitioningDescriptor {
	return w.desc.Partitioning
}

// FindPartitionByName searches the index's partitioning descriptor for a
// partition whose name is the input and returns it, or nil if no match is found.
func (w index) FindPartitionByName(name string) descpb.PartitioningDescriptor {
	return *w.desc.Partitioning.FindPartitionByName(name)
}

// PartitionNames returns a slice containing the name of every partition and
// subpartition in an arbitrary order.
func (w index) PartitionNames() []string {
	return w.desc.Partitioning.PartitionNames()
}

// IsValidOriginIndex returns whether the index can serve as an origin index for
// a foreign key constraint with the provided set of originColIDs.
func (w index) IsValidOriginIndex(originColIDs descpb.ColumnIDs) bool {
	return w.desc.IsValidOriginIndex(originColIDs)
}

// IsValidReferencedUniqueConstraint returns whether the index can serve as a
// referenced index for a foreign  key constraint with the provided set of
// referencedColumnIDs.
func (w index) IsValidReferencedUniqueConstraint(referencedColIDs descpb.ColumnIDs) bool {
	return w.desc.IsValidReferencedUniqueConstraint(referencedColIDs)
}

// HasOldStoredColumns returns whether the index has stored columns in the old
// format (data encoded the same way as if they were in an implicit column).
func (w index) HasOldStoredColumns() bool {
	return w.desc.HasOldStoredColumns()
}

// InvertedColumnID returns the ColumnID of the inverted column of the inverted
// index. This is always the last column in ColumnIDs. Panics if the index is
// not inverted.
func (w index) InvertedColumnID() descpb.ColumnID {
	return w.desc.InvertedColumnID()
}

// InvertedColumnName returns the name of the inverted column of the inverted
// index. This is always the last column in ColumnNames. Panics if the index is
// not inverted.
func (w index) InvertedColumnName() string {
	return w.desc.InvertedColumnName()
}

// ContainsColumnID returns true if the index descriptor contains the specified
// column ID either in its explicit column IDs, the extra column IDs, or the
// stored column IDs.
func (w index) ContainsColumnID(colID descpb.ColumnID) bool {
	return w.desc.ContainsColumnID(colID)
}

// GetGeoConfig returns the geo config in the index descriptor.
func (w index) GetGeoConfig() geoindex.Config {
	return w.desc.GeoConfig
}

// GetSharded returns the ShardedDescriptor in the index descriptor
func (w index) GetSharded() descpb.ShardedDescriptor {
	return w.desc.Sharded
}

// GetShardColumnName returns the name of the shard column if the index is hash
// sharded, empty string otherwise.
func (w index) GetShardColumnName() string {
	return w.desc.Sharded.Name
}

// GetVersion returns the version of the index descriptor.
func (w index) GetVersion() descpb.IndexDescriptorVersion {
	return w.desc.Version
}

// GetEncodingType returns the encoding type of this index. For backward
// compatibility reasons, this might not match what is stored in
// w.desc.EncodingType.
func (w index) GetEncodingType() descpb.IndexDescriptorEncodingType {
	if w.Primary() {
		// Primary indexes always use the PrimaryIndexEncoding, regardless of what
		// desc.EncodingType indicates.
		return descpb.PrimaryIndexEncoding
	}
	return w.desc.EncodingType
}

// NumInterleaveAncestors returns the number of interleave ancestors as per the
// index descriptor.
func (w index) NumInterleaveAncestors() int {
	return len(w.desc.Interleave.Ancestors)
}

// GetInterleaveAncestor returns the ancestorOrdinal-th interleave ancestor.
func (w index) GetInterleaveAncestor(ancestorOrdinal int) descpb.InterleaveDescriptor_Ancestor {
	return w.desc.Interleave.Ancestors[ancestorOrdinal]
}

// NumInterleavedBy returns the number of tables/indexes that are interleaved
// into this index.
func (w index) NumInterleavedBy() int {
	return len(w.desc.InterleavedBy)
}

// GetInterleavedBy returns the interleavedByOrdinal-th table/index that is
// interleaved into this index.
func (w index) GetInterleavedBy(interleavedByOrdinal int) descpb.ForeignKeyReference {
	return w.desc.InterleavedBy[interleavedByOrdinal]
}

// NumColumns returns the number of columns as per the index descriptor.
func (w index) NumColumns() int {
	return len(w.desc.ColumnIDs)
}

// GetColumnID returns the ID of the columnOrdinal-th column.
func (w index) GetColumnID(columnOrdinal int) descpb.ColumnID {
	return w.desc.ColumnIDs[columnOrdinal]
}

// GetColumnName returns the name of the columnOrdinal-th column.
func (w index) GetColumnName(columnOrdinal int) string {
	return w.desc.ColumnNames[columnOrdinal]
}

// GetColumnDirection returns the direction of the columnOrdinal-th column.
func (w index) GetColumnDirection(columnOrdinal int) descpb.IndexDescriptor_Direction {
	return w.desc.ColumnDirections[columnOrdinal]
}

// ForEachColumnID applies its argument fn to each of the column IDs in the
// index descriptor. If there is an error, that error is returned immediately.
func (w index) ForEachColumnID(fn func(colID descpb.ColumnID) error) error {
	return w.desc.RunOverAllColumns(fn)
}

// NumStoredColumns returns the number of columns which the index stores in
// addition to the columns which are explicitly part of the index (STORING
// clause). Only used for secondary indexes.
func (w index) NumStoredColumns() int {
	return len(w.desc.StoreColumnIDs)
}

// GetStoredColumnID returns the ID of the storeColumnOrdinal-th store column.
func (w index) GetStoredColumnID(storedColumnOrdinal int) descpb.ColumnID {
	return w.desc.StoreColumnIDs[storedColumnOrdinal]
}

// GetStoredColumnName returns the name of the storeColumnOrdinal-th store column.
func (w index) GetStoredColumnName(storedColumnOrdinal int) string {
	return w.desc.StoreColumnNames[storedColumnOrdinal]
}

// NumExtraColumns returns the number of additional columns referenced by the
// index descriptor.
func (w index) NumExtraColumns() int {
	return len(w.desc.ExtraColumnIDs)
}

// GetExtraColumnID returns the ID of the extraColumnOrdinal-th extra column.
func (w index) GetExtraColumnID(extraColumnOrdinal int) descpb.ColumnID {
	return w.desc.ExtraColumnIDs[extraColumnOrdinal]
}

// NumCompositeColumns returns the number of composite columns referenced by the
// index descriptor.
func (w index) NumCompositeColumns() int {
	return len(w.desc.CompositeColumnIDs)
}

// GetCompositeColumnID returns the ID of the compositeColumnOrdinal-th
// composite column.
func (w index) GetCompositeColumnID(compositeColumnOrdinal int) descpb.ColumnID {
	return w.desc.CompositeColumnIDs[compositeColumnOrdinal]
}

// indexCache contains precomputed slices of catalog.Index interfaces.
type indexCache struct {
	primary              catalog.Index
	all                  []catalog.Index
	active               []catalog.Index
	nonDrop              []catalog.Index
	publicNonPrimary     []catalog.Index
	writableNonPrimary   []catalog.Index
	deletableNonPrimary  []catalog.Index
	deleteOnlyNonPrimary []catalog.Index
	partial              []catalog.Index
}

// newIndexCache returns a fresh fully-populated indexCache struct for the
// TableDescriptor.
func newIndexCache(desc *descpb.TableDescriptor) *indexCache {
	c := indexCache{}
	// Build a slice of structs to back the interfaces in c.all.
	// This is better than allocating memory once per struct.
	backingStructs := make([]index, 1+len(desc.Indexes), 1+len(desc.Indexes)+len(desc.Mutations))
	backingStructs[0] = index{desc: &desc.PrimaryIndex}
	for i := range desc.Indexes {
		backingStructs[i+1] = index{desc: &desc.Indexes[i], ordinal: i + 1}
	}
	for _, m := range desc.Mutations {
		if idxDesc := m.GetIndex(); idxDesc != nil {
			idx := index{
				desc:              idxDesc,
				ordinal:           len(backingStructs),
				mutationID:        m.MutationID,
				mutationState:     m.State,
				mutationDirection: m.Direction,
			}
			backingStructs = append(backingStructs, idx)
		}
	}
	// Populate the c.all slice with index interfaces.
	c.all = make([]catalog.Index, len(backingStructs))
	for i := range backingStructs {
		c.all[i] = &backingStructs[i]
	}
	// Populate the remaining fields.
	c.primary = c.all[0]
	c.active = c.all[:1+len(desc.Indexes)]
	c.publicNonPrimary = c.active[1:]
	c.deletableNonPrimary = c.all[1:]
	if len(c.active) == len(c.all) {
		c.writableNonPrimary = c.publicNonPrimary
	} else {
		for _, idx := range c.deletableNonPrimary {
			if idx.DeleteOnly() {
				lazyAllocAppend(&c.deleteOnlyNonPrimary, idx, len(c.all)-len(c.active))
			} else {
				lazyAllocAppend(&c.writableNonPrimary, idx, len(c.deletableNonPrimary))
			}
		}
	}
	for _, idx := range c.all {
		if !idx.Dropped() && (!idx.Primary() || desc.IsPhysicalTable()) {
			lazyAllocAppend(&c.nonDrop, idx, len(c.all))
		}
		if idx.IsPartial() {
			lazyAllocAppend(&c.partial, idx, len(c.all))
		}
	}
	return &c
}

func lazyAllocAppend(slice *[]catalog.Index, idx catalog.Index, cap int) {
	if *slice == nil {
		*slice = make([]catalog.Index, 0, cap)
	}
	*slice = append(*slice, idx)
}
