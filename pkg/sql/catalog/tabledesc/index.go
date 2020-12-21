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

// Ordinal returns the ordinal of the index within the table descriptor.
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

// Adding returns true iff the index is a drop mutation in the table descriptor.
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

// IsValidOriginIndex returns whether the index can serve as an origin index for
// a foreign key constraint with the provided set of originColIDs.
func (w index) IsValidOriginIndex(originColIDs descpb.ColumnIDs) bool {
	return w.desc.IsValidOriginIndex(originColIDs)
}

// IsValidReferencedIndex returns whether the index can serve as a referenced
// index for a foreign  key constraint with the provided set of
// referencedColumnIDs.
func (w index) IsValidReferencedIndex(referencedColIDs descpb.ColumnIDs) bool {
	return w.desc.IsValidReferencedIndex(referencedColIDs)
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

// ShardColumnName returns the name of the shard column if the index is hash
// sharded, empty string otherwise.
func (w index) ShardColumnName() string {
	return w.desc.Sharded.Name
}

// indexCache contains lazily precomputed slices of catalog.Index interfaces.
// A field value of nil indicates that the slice hasn't been precomputed yet.
type indexCache struct {
	all                  []catalog.Index
	active               []catalog.Index
	nonDrop              []catalog.Index
	publicNonPrimary     []catalog.Index
	writableNonPrimary   []catalog.Index
	deletableNonPrimary  []catalog.Index
	deleteOnlyNonPrimary []catalog.Index
	partial              []catalog.Index
}

// cachedIndexes returns an already-build slice of catalog.Index interfaces if
// it exists, if not it builds it using the provided factory function and args.
// Notice that, as a result, empty slices need to be handled carefully.
func (c *indexCache) cachedIndexes(
	cached *[]catalog.Index,
	factory func(c *indexCache, desc *wrapper) []catalog.Index,
	desc *wrapper,
) []catalog.Index {
	if *cached == nil {
		*cached = factory(c, desc)
		if *cached == nil {
			*cached = []catalog.Index{}
		}
	}
	if len(*cached) == 0 {
		return nil
	}
	return *cached
}

// buildPublicNonPrimary builds a fresh return value for
// desc.PublicNonPrimaryIndexes().
func buildPublicNonPrimary(_ *indexCache, desc *wrapper) []catalog.Index {
	s := make([]catalog.Index, len(desc.Indexes))
	for i := range s {
		s[i] = index{desc: &desc.Indexes[i], ordinal: i + 1}
	}
	return s
}

func (c *indexCache) publicNonPrimaryIndexes(desc *wrapper) []catalog.Index {
	return c.cachedIndexes(&c.publicNonPrimary, buildPublicNonPrimary, desc)
}

// buildActive builds fresh return value for desc.ActiveIndexes().
func buildActive(c *indexCache, desc *wrapper) []catalog.Index {
	publicNonPrimary := c.publicNonPrimaryIndexes(desc)
	s := make([]catalog.Index, 1, 1+len(publicNonPrimary))
	s[0] = index{desc: &desc.PrimaryIndex}
	return append(s, publicNonPrimary...)
}

func (c *indexCache) activeIndexes(desc *wrapper) []catalog.Index {
	return c.cachedIndexes(&c.active, buildActive, desc)
}

// buildAll builds fresh return value for desc.AllIndexes().
func buildAll(c *indexCache, desc *wrapper) []catalog.Index {
	s := make([]catalog.Index, 0, 1+len(desc.Indexes)+len(desc.Mutations))
	s = append(s, c.activeIndexes(desc)...)
	for _, m := range desc.Mutations {
		if idxDesc := m.GetIndex(); idxDesc != nil {
			idx := index{
				desc:              idxDesc,
				ordinal:           len(s),
				mutationID:        m.MutationID,
				mutationState:     m.State,
				mutationDirection: m.Direction,
			}
			s = append(s, idx)
		}
	}
	return s
}

func (c *indexCache) allIndexes(desc *wrapper) []catalog.Index {
	return c.cachedIndexes(&c.all, buildAll, desc)
}

// buildDeletableNonPrimary builds fresh return value for
// desc.DeletableNonPrimaryIndexes().
func buildDeletableNonPrimary(c *indexCache, desc *wrapper) []catalog.Index {
	return c.allIndexes(desc)[1:]
}

func (c *indexCache) deletableNonPrimaryIndexes(desc *wrapper) []catalog.Index {
	return c.cachedIndexes(&c.deletableNonPrimary, buildDeletableNonPrimary, desc)
}

// buildWritableNonPrimary builds fresh return value for
// desc.WritableNonPrimaryIndexes().
func buildWritableNonPrimary(c *indexCache, desc *wrapper) []catalog.Index {
	deletableNonPrimary := c.deletableNonPrimaryIndexes(desc)
	s := make([]catalog.Index, 0, len(deletableNonPrimary))
	for _, idx := range deletableNonPrimary {
		if idx.Public() || idx.WriteAndDeleteOnly() {
			s = append(s, idx)
		}
	}
	return s
}

func (c *indexCache) writableNonPrimaryIndexes(desc *wrapper) []catalog.Index {
	return c.cachedIndexes(&c.writableNonPrimary, buildWritableNonPrimary, desc)
}

// buildDeleteOnlyNonPrimary builds fresh return value for
// desc.DeleteOnlyNonPrimaryIndexes().
func buildDeleteOnlyNonPrimary(c *indexCache, desc *wrapper) []catalog.Index {
	deletableNonPublic := c.deletableNonPrimaryIndexes(desc)[len(desc.Indexes):]
	s := make([]catalog.Index, 0, len(deletableNonPublic))
	for _, idx := range deletableNonPublic {
		if idx.DeleteOnly() {
			s = append(s, idx)
		}
	}
	return s
}

func (c *indexCache) deleteOnlyNonPrimaryIndexes(desc *wrapper) []catalog.Index {
	return c.cachedIndexes(&c.deleteOnlyNonPrimary, buildDeleteOnlyNonPrimary, desc)
}

// buildNonDrop builds fresh return value for desc.NonDropIndexes().
func buildNonDrop(c *indexCache, desc *wrapper) []catalog.Index {
	all := c.allIndexes(desc)
	s := make([]catalog.Index, 0, len(all))
	for _, idx := range all {
		if !idx.Dropped() && (!idx.Primary() || desc.IsPhysicalTable()) {
			s = append(s, idx)
		}
	}
	return s
}

func (c *indexCache) nonDropIndexes(desc *wrapper) []catalog.Index {
	return c.cachedIndexes(&c.nonDrop, buildNonDrop, desc)
}

// buildPartial builds fresh return value for desc.PartialIndexes().
func buildPartial(c *indexCache, desc *wrapper) []catalog.Index {
	deletableNonPrimary := c.deletableNonPrimaryIndexes(desc)
	s := make([]catalog.Index, 0, len(deletableNonPrimary))
	for _, idx := range deletableNonPrimary {
		if idx.IsPartial() {
			s = append(s, idx)
		}
	}
	return s
}

func (c *indexCache) partialIndexes(desc *wrapper) []catalog.Index {
	return c.cachedIndexes(&c.partial, buildPartial, desc)
}
