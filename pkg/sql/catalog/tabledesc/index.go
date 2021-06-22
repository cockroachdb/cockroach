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
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

var _ catalog.Index = (*index)(nil)

// index implements the catalog.Index interface by wrapping the protobuf index
// descriptor along with some metadata from its parent table descriptor.
type index struct {
	maybeMutation
	desc    *descpb.IndexDescriptor
	ordinal int
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
	return !w.IsMutation()
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
func (w index) GetPartitioning() catalog.Partitioning {
	return &partitioning{desc: &w.desc.Partitioning}
}

// ExplicitColumnStartIdx returns the first index in which the column is
// explicitly part of the index.
func (w index) ExplicitColumnStartIdx() int {
	return w.desc.ExplicitColumnStartIdx()
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
// format, in which the IDs of the stored columns were kept in the "extra"
// column IDs slice, which is now called KeySuffixColumnIDs. Thus their data
// was encoded the same way as if they were in an implicit column.
// TODO(postamar): this concept should be migrated away.
func (w index) HasOldStoredColumns() bool {
	return w.NumKeySuffixColumns() > 0 &&
		!w.Primary() &&
		len(w.desc.StoreColumnIDs) < len(w.desc.StoreColumnNames)
}

// InvertedColumnID returns the ColumnID of the inverted column of the inverted
// index. This is always the last column in KeyColumnIDs. Panics if the index is
// not inverted.
func (w index) InvertedColumnID() descpb.ColumnID {
	return w.desc.InvertedColumnID()
}

// InvertedColumnName returns the name of the inverted column of the inverted
// index. This is always the last column in KeyColumnNames. Panics if the index is
// not inverted.
func (w index) InvertedColumnName() string {
	return w.desc.InvertedColumnName()
}

// CollectKeyColumnIDs creates a new set containing the column IDs in the key
// of this index.
func (w index) CollectKeyColumnIDs() catalog.TableColSet {
	return catalog.MakeTableColSet(w.desc.KeyColumnIDs...)
}

// CollectPrimaryStoredColumnIDs creates a new set containing the column IDs
// stored in this index if it is a primary index.
func (w index) CollectPrimaryStoredColumnIDs() catalog.TableColSet {
	if !w.Primary() {
		return catalog.TableColSet{}
	}
	return catalog.MakeTableColSet(w.desc.StoreColumnIDs...)
}

// CollectSecondaryStoredColumnIDs creates a new set containing the column IDs
// stored in this index if it is a secondary index.
func (w index) CollectSecondaryStoredColumnIDs() catalog.TableColSet {
	if w.Primary() {
		return catalog.TableColSet{}
	}
	return catalog.MakeTableColSet(w.desc.StoreColumnIDs...)
}

// CollectKeySuffixColumnIDs creates a new set containing the key suffix column
// IDs in this index. These are the columns from the table's primary index which
// are otherwise not in this index.
func (w index) CollectKeySuffixColumnIDs() catalog.TableColSet {
	return catalog.MakeTableColSet(w.desc.KeySuffixColumnIDs...)
}

// CollectCompositeColumnIDs creates a new set containing the composite column
// IDs.
func (w index) CollectCompositeColumnIDs() catalog.TableColSet {
	return catalog.MakeTableColSet(w.desc.CompositeColumnIDs...)
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

// NumKeyColumns returns the number of columns in the index key.
func (w index) NumKeyColumns() int {
	return len(w.desc.KeyColumnIDs)
}

// GetKeyColumnID returns the ID of the columnOrdinal-th column in the index key.
func (w index) GetKeyColumnID(columnOrdinal int) descpb.ColumnID {
	return w.desc.KeyColumnIDs[columnOrdinal]
}

// GetKeyColumnName returns the name of the columnOrdinal-th column in the index
// key.
func (w index) GetKeyColumnName(columnOrdinal int) string {
	return w.desc.KeyColumnNames[columnOrdinal]
}

// GetKeyColumnDirection returns the direction of the columnOrdinal-th column in
// the index key.
func (w index) GetKeyColumnDirection(columnOrdinal int) descpb.IndexDescriptor_Direction {
	return w.desc.KeyColumnDirections[columnOrdinal]
}

// NumPrimaryStoredColumns returns the number of columns which the index
// stores in addition to the columns which are part of the primary key.
// Returns 0 if the index isn't primary.
func (w index) NumPrimaryStoredColumns() int {
	if !w.Primary() {
		return 0
	}
	return len(w.desc.StoreColumnIDs)
}

// NumSecondaryStoredColumns returns the number of columns which the index
// stores in addition to the columns which are explicitly part of the index
// (STORING clause). Returns 0 if the index isn't secondary.
func (w index) NumSecondaryStoredColumns() int {
	if w.Primary() {
		return 0
	}
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

// NumKeySuffixColumns returns the number of additional columns referenced by
// the index descriptor, which are not part of the index key but which are part
// of the table's primary key.
func (w index) NumKeySuffixColumns() int {
	return len(w.desc.KeySuffixColumnIDs)
}

// GetKeySuffixColumnID returns the ID of the extraColumnOrdinal-th key suffix
// column.
func (w index) GetKeySuffixColumnID(keySuffixColumnOrdinal int) descpb.ColumnID {
	return w.desc.KeySuffixColumnIDs[keySuffixColumnOrdinal]
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

// partitioning is the backing struct for a catalog.Partitioning interface.
type partitioning struct {
	desc *descpb.PartitioningDescriptor
}

// PartitioningDesc returns the underlying protobuf descriptor.
func (p partitioning) PartitioningDesc() *descpb.PartitioningDescriptor {
	return p.desc
}

// DeepCopy returns a deep copy of the receiver.
func (p partitioning) DeepCopy() catalog.Partitioning {
	return &partitioning{desc: protoutil.Clone(p.desc).(*descpb.PartitioningDescriptor)}
}

// FindPartitionByName recursively searches the partitioning for a partition
// whose name matches the input and returns it, or nil if no match is found.
func (p partitioning) FindPartitionByName(name string) (found catalog.Partitioning) {
	_ = p.forEachPartitionName(func(partitioning catalog.Partitioning, currentName string) error {
		if name == currentName {
			found = partitioning
			return iterutil.StopIteration()
		}
		return nil
	})
	return found
}

// ForEachPartitionName applies fn on each of the partition names in this
// partition and recursively in its subpartitions.
// Supports iterutil.Done.
func (p partitioning) ForEachPartitionName(fn func(name string) error) error {
	err := p.forEachPartitionName(func(_ catalog.Partitioning, name string) error {
		return fn(name)
	})
	if iterutil.Done(err) {
		return nil
	}
	return err
}

func (p partitioning) forEachPartitionName(
	fn func(partitioning catalog.Partitioning, name string) error,
) error {
	for _, l := range p.desc.List {
		err := fn(p, l.Name)
		if err != nil {
			return err
		}
		err = partitioning{desc: &l.Subpartitioning}.forEachPartitionName(fn)
		if err != nil {
			return err
		}
	}
	for _, r := range p.desc.Range {
		err := fn(p, r.Name)
		if err != nil {
			return err
		}
	}
	return nil
}

// NumLists returns the number of list elements in the underlying partitioning
// descriptor.
func (p partitioning) NumLists() int {
	return len(p.desc.List)
}

// NumRanges returns the number of range elements in the underlying
// partitioning descriptor.
func (p partitioning) NumRanges() int {
	return len(p.desc.Range)
}

// ForEachList applies fn on each list element of the wrapped partitioning.
// Supports iterutil.Done.
func (p partitioning) ForEachList(
	fn func(name string, values [][]byte, subPartitioning catalog.Partitioning) error,
) error {
	for _, l := range p.desc.List {
		subp := partitioning{desc: &l.Subpartitioning}
		err := fn(l.Name, l.Values, subp)
		if err != nil {
			if iterutil.Done(err) {
				return nil
			}
			return err
		}
	}
	return nil
}

// ForEachRange applies fn on each range element of the wrapped partitioning.
// Supports iterutil.Done.
func (p partitioning) ForEachRange(fn func(name string, from, to []byte) error) error {
	for _, r := range p.desc.Range {
		err := fn(r.Name, r.FromInclusive, r.ToExclusive)
		if err != nil {
			if iterutil.Done(err) {
				return nil
			}
			return err
		}
	}
	return nil
}

// NumColumns is how large of a prefix of the columns in an index are used in
// the function mapping column values to partitions. If this is a
// subpartition, this is offset to start from the end of the parent
// partition's columns. If NumColumns is 0, then there is no partitioning.
func (p partitioning) NumColumns() int {
	return int(p.desc.NumColumns)
}

// NumImplicitColumns specifies the number of columns that implicitly prefix a
// given index. This occurs if a user specifies a PARTITION BY which is not a
// prefix of the given index, in which case the KeyColumnIDs are added in front
// of the index and this field denotes the number of columns added as a prefix.
// If NumImplicitColumns is 0, no implicit columns are defined for the index.
func (p partitioning) NumImplicitColumns() int {
	return int(p.desc.NumImplicitColumns)
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
func newIndexCache(desc *descpb.TableDescriptor, mutations *mutationCache) *indexCache {
	c := indexCache{}
	// Build a slice of structs to back the public interfaces in c.all.
	// This is better than allocating memory once per struct.
	numPublic := 1 + len(desc.Indexes)
	backingStructs := make([]index, numPublic)
	backingStructs[0] = index{desc: &desc.PrimaryIndex}
	for i := range desc.Indexes {
		backingStructs[i+1] = index{desc: &desc.Indexes[i], ordinal: i + 1}
	}
	// Populate the c.all slice with Index interfaces.
	numMutations := len(mutations.indexes)
	c.all = make([]catalog.Index, numPublic, numPublic+numMutations)
	for i := range backingStructs {
		c.all[i] = &backingStructs[i]
	}
	for _, m := range mutations.indexes {
		c.all = append(c.all, m.AsIndex())
	}
	// Populate the remaining fields in c.
	c.primary = c.all[0]
	c.active = c.all[:numPublic]
	c.publicNonPrimary = c.active[1:]
	c.deletableNonPrimary = c.all[1:]
	if numMutations == 0 {
		c.writableNonPrimary = c.publicNonPrimary
	} else {
		for _, idx := range c.deletableNonPrimary {
			if idx.DeleteOnly() {
				lazyAllocAppendIndex(&c.deleteOnlyNonPrimary, idx, numMutations)
			} else {
				lazyAllocAppendIndex(&c.writableNonPrimary, idx, len(c.deletableNonPrimary))
			}
		}
	}
	for _, idx := range c.all {
		if !idx.Dropped() && (!idx.Primary() || desc.IsPhysicalTable()) {
			lazyAllocAppendIndex(&c.nonDrop, idx, len(c.all))
		}
		if idx.IsPartial() {
			lazyAllocAppendIndex(&c.partial, idx, len(c.all))
		}
	}
	return &c
}

func lazyAllocAppendIndex(slice *[]catalog.Index, idx catalog.Index, cap int) {
	if *slice == nil {
		*slice = make([]catalog.Index, 0, cap)
	}
	*slice = append(*slice, idx)
}
