// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tabledesc

import (
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/idxtype"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecpb"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

var _ catalog.Index = (*index)(nil)
var _ catalog.UniqueWithIndexConstraint = (*index)(nil)

// index implements the catalog.Index interface by wrapping the protobuf index
// descriptor along with some metadata from its parent table descriptor.
//
// index also implements the catalog.Constraint interface for index-backed-constraints
// (i.e. PRIMARY KEY or UNIQUE).
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

// GetConstraintID returns the constraint ID.
func (w index) GetConstraintID() descpb.ConstraintID {
	return w.desc.ConstraintID
}

// GetName returns the index name.
func (w index) GetName() string {
	return w.desc.Name
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

// IsNotVisible returns true iff the index is not visible.
func (w index) IsNotVisible() bool {
	return w.desc.NotVisible
}

// GetInvisibility returns index invisibility.
func (w index) GetInvisibility() float64 {
	return w.desc.Invisibility
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
func (w index) GetType() idxtype.T {
	return w.desc.Type
}

// GetPartitioning returns the partitioning descriptor of the index.
func (w index) GetPartitioning() catalog.Partitioning {
	return &partitioning{desc: &w.desc.Partitioning}
}

// PartitioningColumnCount is how large of a prefix of the columns in an index
// are used in the function mapping column values to partitions. If this is a
// subpartition, this is offset to start from the end of the parent partition's
// columns. If PartitioningColumnCount is 0, then there is no partitioning.
func (w index) PartitioningColumnCount() int {
	return int(w.desc.Partitioning.NumColumns)
}

// ImplicitPartitioningColumnCount specifies the number of columns that
// implicitly prefix a given index. This occurs if a user specifies a PARTITION
// BY which is not a prefix of the given index, in which case the KeyColumnIDs
// are added in front of the index and this field denotes the number of columns
// added as a prefix. If ImplicitPartitioningColumnCount is 0, no implicit
// columns are defined for the index.
func (w index) ImplicitPartitioningColumnCount() int {
	return int(w.desc.Partitioning.NumImplicitColumns)
}

// ExplicitColumnStartIdx returns the first index in which the column is
// explicitly part of the index.
func (w index) ExplicitColumnStartIdx() int {
	return w.desc.ExplicitColumnStartIdx()
}

// IsValidOriginIndex implements the catalog.Index interface.
func (w index) IsValidOriginIndex(fk catalog.ForeignKeyConstraint) bool {
	if w.IsPartial() {
		return false
	}
	return descpb.ColumnIDs(w.desc.KeyColumnIDs).HasPrefix(fk.ForeignKeyDesc().OriginColumnIDs)
}

// IsValidReferencedUniqueConstraint implements the catalog.UniqueConstraint
// interface.
func (w index) IsValidReferencedUniqueConstraint(fk catalog.ForeignKeyConstraint) bool {
	return w.desc.IsValidReferencedUniqueConstraint(fk.ForeignKeyDesc().ReferencedColumnIDs)
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

// InvertedColumnKeyType returns the type of the data element that is encoded
// as the inverted index key. This is currently always EncodedKey.
//
// Panics if the index is not inverted.
func (w index) InvertedColumnKeyType() *types.T {
	return w.desc.InvertedColumnKeyType()
}

// InvertedColumnKind returns the kind of the inverted column of the inverted
// index.
//
// Panics if the index is not inverted.
func (w index) InvertedColumnKind() catpb.InvertedIndexColumnKind {
	if w.desc.Type != idxtype.INVERTED {
		panic(errors.AssertionFailedf("index is not inverted"))
	}
	if len(w.desc.InvertedColumnKinds) == 0 {
		// Not every inverted index has kinds inside, since no kinds were set prior
		// to version 22.2.
		return catpb.InvertedIndexColumnKind_DEFAULT
	}
	return w.desc.InvertedColumnKinds[0]
}

// VectorColumnID returns the ColumnID of the vector column of the vector index.
// This is always the last column in KeyColumnIDs. Panics if the index is not a
// vector index.
func (w index) VectorColumnID() descpb.ColumnID {
	return w.desc.VectorColumnID()
}

// VectorColumnName returns the name of the vector column of the vector index.
// This is always the last column in KeyColumnIDs. Panics if the index is not a
// vector index.
func (w index) VectorColumnName() string {
	return w.desc.VectorColumnName()
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
func (w index) GetGeoConfig() geopb.Config {
	return w.desc.GeoConfig
}

// GetVecConfig returns the vec config in the index descriptor.
func (w index) GetVecConfig() vecpb.Config {
	return w.desc.VecConfig
}

// GetSharded returns the ShardedDescriptor in the index descriptor
func (w index) GetSharded() catpb.ShardedDescriptor {
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
func (w index) GetEncodingType() catenumpb.IndexDescriptorEncodingType {
	if w.Primary() {
		// Primary indexes always use the PrimaryIndexEncoding, regardless of what
		// desc.EncodingType indicates.
		return catenumpb.PrimaryIndexEncoding
	}
	return w.desc.EncodingType
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
func (w index) GetKeyColumnDirection(columnOrdinal int) catenumpb.IndexColumn_Direction {
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

// UseDeletePreservingEncoding returns true if the index is to be encoded with
// an additional bit that indicates whether or not the value has been deleted.
//
// Index key-values that are deleted in this way are not actually deleted,
// but remain in the index with a value which has the delete bit set to true.
// This is necessary to preserve the delete history for the MVCC-compatible
// index backfiller
// docs/RFCS/20211004_incremental_index_backfiller.md#new-index-encoding-for-deletions-vs-mvcc
//
// We only use the delete preserving encoding if the index is
// writable. Otherwise, we may preserve a delete when in DELETE_ONLY but never
// see a subsequent write that replaces it. This a problem for the
// MVCC-compatible index backfiller which merges entries from a
// delete-preserving index into a newly-added index. A delete preserved in
// DELETE_ONLY could result in a value being erroneously deleted during the
// merge process. While we could filter such deletes, the filtering would
// require more data being stored in each deleted entry and further complicate
// the merge process. See #75720 for further details.
func (w index) UseDeletePreservingEncoding() bool {
	return w.desc.UseDeletePreservingEncoding && !w.maybeMutation.DeleteOnly()
}

// ForcePut forces all writes to use Put rather than CPut.
//
// Users of this options should take great care as it
// effectively mean unique constraints are not respected.
//
// Currently (2023-03-09) there are three users:
//   - delete preserving indexes
//   - merging indexes
//   - dropping primary indexes
//   - adding primary indexes with new columns (same key)
//
// Delete preserving encoding indexes are used only as a log of
// index writes during backfill, thus we can blindly put values into
// them.
//
// New indexes may miss updates during the backfilling process
// that would lead to CPut failures until the missed updates
// are merged into the index. Uniqueness for such indexes is
// checked by the schema changer before they are brought back
// online.
//
// In the case of dropping primary indexes, we always ensure that
// there's a replacement primary index which has become public.
// The reason we must not use cput is that the new primary index
// may not store all the columns stored in this index.
//
// In the case of adding primary indexes with new columns, we don't
// know the value of the new column when performing updates, so we can't
// synthesize the expectation. This is okay because the uniqueness of
// the key is being enforced by the existing primary index.
//
// When altering a primary key, we never simultaneously add new columns.
// When adding a new primary index which has a new key, we always ensure that
// the source primary index, which is the public primary index, has all
// columns in that new primary index. In this case, it's unsafe to avoid the
// ForcePut when that index is in WriteOnly because we need the write to be
// upholding uniqueness. To re-iterate, when changing the primary key of a
// table, we'll not be both adding new columns and creating the new primary
// key at the same time; we have to materialize the new columns and make them
// available as the public primary index on the table before proceeding to
// populate the new primary index with the new key structure.
func (w index) ForcePut() bool {
	return w.mutationForcePutForIndexWrites
}

func (w index) CreatedAt() time.Time {
	if w.desc.CreatedAtNanos == 0 {
		return time.Time{}
	}
	return timeutil.Unix(0, w.desc.CreatedAtNanos)
}

// IsTemporaryIndexForBackfill returns true iff the index is
// an index being used as the temporary index being used by an
// in-progress index backfill.
//
// TODO(ssd): This could be its own boolean or we could store the ID
// of the index it is a temporary index for.
func (w index) IsTemporaryIndexForBackfill() bool {
	return w.desc.UseDeletePreservingEncoding
}

// AsCheck implements the catalog.ConstraintProvider interface.
func (w index) AsCheck() catalog.CheckConstraint {
	return nil
}

// AsForeignKey implements the catalog.ConstraintProvider interface.
func (w index) AsForeignKey() catalog.ForeignKeyConstraint {
	return nil
}

// AsUniqueWithoutIndex implements the catalog.ConstraintProvider interface.
func (w index) AsUniqueWithoutIndex() catalog.UniqueWithoutIndexConstraint {
	return nil
}

// AsUniqueWithIndex implements the catalog.ConstraintProvider interface.
func (w index) AsUniqueWithIndex() catalog.UniqueWithIndexConstraint {
	if w.Primary() {
		return &w
	}
	if w.IsUnique() && !w.desc.UseDeletePreservingEncoding {
		return &w
	}
	return nil
}

// String implements the catalog.Constraint interface.
func (w index) String() string {
	return fmt.Sprintf("%v", w.desc)
}

// IsConstraintValidated implements the catalog.Constraint interface.
func (w index) IsConstraintValidated() bool {
	return !w.IsMutation()
}

// IsConstraintUnvalidated implements the catalog.Constraint interface.
func (w index) IsConstraintUnvalidated() bool {
	return false
}

// GetConstraintValidity implements the catalog.Constraint interface.
func (w index) GetConstraintValidity() descpb.ConstraintValidity {
	if w.Adding() {
		return descpb.ConstraintValidity_Validating
	}
	if w.Dropped() {
		return descpb.ConstraintValidity_Dropping
	}
	return descpb.ConstraintValidity_Validated
}

// IsEnforced implements the catalog.Constraint interface.
func (w index) IsEnforced() bool {
	return !w.IsMutation() || w.WriteAndDeleteOnly()
}

// NewTestIndex wraps an index descriptor in an index struct for use in unit tests.
func NewTestIndex(desc *descpb.IndexDescriptor, ordinal int) index {
	return index{desc: desc, ordinal: ordinal}
}

// partitioning is the backing struct for a catalog.Partitioning interface.
type partitioning struct {
	desc *catpb.PartitioningDescriptor
}

// PartitioningDesc returns the underlying protobuf descriptor.
func (p partitioning) PartitioningDesc() *catpb.PartitioningDescriptor {
	return p.desc
}

// DeepCopy returns a deep copy of the receiver.
func (p partitioning) DeepCopy() catalog.Partitioning {
	return &partitioning{desc: protoutil.Clone(p.desc).(*catpb.PartitioningDescriptor)}
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
// Supports iterutil.StopIteration.
func (p partitioning) ForEachPartitionName(fn func(name string) error) error {
	err := p.forEachPartitionName(func(_ catalog.Partitioning, name string) error {
		return fn(name)
	})
	return iterutil.Map(err)
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
// Supports iterutil.StopIteration.
func (p partitioning) ForEachList(
	fn func(name string, values [][]byte, subPartitioning catalog.Partitioning) error,
) error {
	for _, l := range p.desc.List {
		subp := partitioning{desc: &l.Subpartitioning}
		err := fn(l.Name, l.Values, subp)
		if err != nil {
			return iterutil.Map(err)
		}
	}
	return nil
}

// ForEachRange applies fn on each range element of the wrapped partitioning.
// Supports iterutil.StopIteration.
func (p partitioning) ForEachRange(fn func(name string, from, to []byte) error) error {
	for _, r := range p.desc.Range {
		err := fn(r.Name, r.FromInclusive, r.ToExclusive)
		if err != nil {
			return iterutil.Map(err)
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
	primary              catalog.UniqueWithIndexConstraint
	all                  []catalog.Index
	active               []catalog.Index
	nonDrop              []catalog.Index
	nonPrimary           []catalog.Index
	publicNonPrimary     []catalog.Index
	writableNonPrimary   []catalog.Index
	deletableNonPrimary  []catalog.Index
	deleteOnlyNonPrimary []catalog.Index
	partial              []catalog.Index
	vector               []catalog.Index
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
	c.primary = c.all[0].AsUniqueWithIndex()
	c.active = c.all[:numPublic]
	c.publicNonPrimary = c.active[1:]
	for _, idx := range c.all[1:] {
		if !idx.Backfilling() {
			lazyAllocAppendIndex(&c.deletableNonPrimary, idx, len(c.all[1:]))
		}
		lazyAllocAppendIndex(&c.nonPrimary, idx, len(c.all[1:]))
	}

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
		// Include only deletable indexes.
		if idx.IsPartial() && !idx.Backfilling() {
			lazyAllocAppendIndex(&c.partial, idx, len(c.all))
		}
		if idx.GetType() == idxtype.VECTOR && !idx.Backfilling() {
			lazyAllocAppendIndex(&c.vector, idx, len(c.all))
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

// ForeignKeyConstraintName forms a default foreign key constraint name.
func ForeignKeyConstraintName(fromTable string, columnNames []string) string {
	return fmt.Sprintf("%s_%s_fkey", fromTable, strings.Join(columnNames, "_"))
}
