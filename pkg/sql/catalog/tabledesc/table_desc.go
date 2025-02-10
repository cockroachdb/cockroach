// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package tabledesc provides concrete implementations of catalog.TableDesc.
package tabledesc

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/errors"
)

var _ catalog.TableDescriptor = (*immutable)(nil)
var _ catalog.TableDescriptor = (*Mutable)(nil)
var _ catalog.MutableDescriptor = (*Mutable)(nil)
var _ catalog.TableDescriptor = (*wrapper)(nil)

// OfflineReasonImporting hard codes the Offline Reason for Importing Tables
const OfflineReasonImporting = "importing"

// wrapper is the base implementation of the catalog.Descriptor
// interface, which is overloaded by immutable and Mutable.
type wrapper struct {
	descpb.TableDescriptor

	// mutationCache, indexCache and columnCache, when not nil, respectively point
	// to a struct containing precomputed catalog.Mutation, catalog.Index or
	// catalog.Column slices.
	// Those can therefore only be set when creating an immutable.
	mutationCache   *mutationCache
	indexCache      *indexCache
	columnCache     *columnCache
	constraintCache *constraintCache

	changes catalog.PostDeserializationChanges

	// This is the raw bytes (tag + data) of the table descriptor in storage.
	rawBytesInStorage []byte
}

// GetRawBytesInStorage implements the catalog.Descriptor interface.
func (desc *wrapper) GetRawBytesInStorage() []byte {
	return desc.rawBytesInStorage
}

// IsUncommittedVersion implements the catalog.Descriptor interface.
func (*wrapper) IsUncommittedVersion() bool {
	return false
}

// GetPostDeserializationChanges returns the set of changes which occurred to
// this descriptor post deserialization.
func (desc *wrapper) GetPostDeserializationChanges() catalog.PostDeserializationChanges {
	return desc.changes
}

// HasConcurrentSchemaChanges implements catalog.Descriptor.
func (desc *wrapper) HasConcurrentSchemaChanges() bool {
	return (desc.DeclarativeSchemaChangerState != nil &&
		desc.DeclarativeSchemaChangerState.JobID != catpb.InvalidJobID) ||
		len(desc.MutationJobs) > 0
}

// ConcurrentSchemaChangeJobIDs implements catalog.Descriptor.
func (desc *wrapper) ConcurrentSchemaChangeJobIDs() (ret []catpb.JobID) {
	if desc.DeclarativeSchemaChangerState != nil &&
		desc.DeclarativeSchemaChangerState.JobID != catpb.InvalidJobID {
		ret = append(ret, desc.DeclarativeSchemaChangerState.JobID)
	}
	if len(desc.MutationJobs) > 0 {
		for _, mutationJob := range desc.MutationJobs {
			ret = append(ret, mutationJob.JobID)
		}
	}
	return ret
}

// SkipNamespace implements the descriptor interface.
func (desc *wrapper) SkipNamespace() bool {
	// Virtual tables are hard-coded and don't have entries in the
	// system.namespace table.
	return desc.IsVirtualTable()
}

// immutable is a custom type for TableDescriptors
// It holds precomputed values and the underlying TableDescriptor
// should be const.
type immutable struct {
	wrapper

	// isUncommittedVersion is set to true if this descriptor was created from
	// a copy of a Mutable with an uncommitted version.
	isUncommittedVersion bool
}

// IsUncommittedVersion implements the Descriptor interface.
func (desc *immutable) IsUncommittedVersion() bool {
	return desc.isUncommittedVersion
}

// NewBuilder implements the Descriptor interface.
func (desc *immutable) NewBuilder() catalog.DescriptorBuilder {
	b := newBuilder(desc.TableDesc(), hlc.Timestamp{}, desc.isUncommittedVersion, desc.changes)
	b.SetRawBytesInStorage(desc.GetRawBytesInStorage())
	return b
}

// DescriptorProto implements the Descriptor interface.
func (desc *wrapper) DescriptorProto() *descpb.Descriptor {
	return &descpb.Descriptor{
		Union: &descpb.Descriptor_Table{Table: &desc.TableDescriptor},
	}
}

// GetDeclarativeSchemaChangerState is part of the catalog.MutableDescriptor
// interface.
func (desc *immutable) GetDeclarativeSchemaChangerState() *scpb.DescriptorState {
	return desc.DeclarativeSchemaChangerState.Clone()
}

// ByteSize implements the Descriptor interface.
func (desc *wrapper) ByteSize() int64 {
	return int64(desc.Size())
}

// NewBuilder implements the catalog.Descriptor interface.
func (desc *wrapper) NewBuilder() catalog.DescriptorBuilder {
	b := newBuilder(desc.TableDesc(), hlc.Timestamp{}, desc.IsUncommittedVersion(), desc.changes)
	b.SetRawBytesInStorage(desc.GetRawBytesInStorage())
	return b
}

// GetPrimaryIndexID implements the TableDescriptor interface.
func (desc *wrapper) GetPrimaryIndexID() descpb.IndexID {
	return desc.PrimaryIndex.ID
}

// IsTemporary implements the TableDescriptor interface.
func (desc *wrapper) IsTemporary() bool {
	return desc.GetTemporary()
}

// ImmutableCopy implements the MutableDescriptor interface.
func (desc *Mutable) ImmutableCopy() catalog.Descriptor {
	return desc.NewBuilder().BuildImmutable()
}

// NewBuilder implements the catalog.Descriptor interface.
//
// It overrides the wrapper's implementation to deal with the fact that
// mutable has overridden the definition of IsUncommittedVersion.
func (desc *Mutable) NewBuilder() catalog.DescriptorBuilder {
	b := newBuilder(desc.TableDesc(), hlc.Timestamp{}, desc.IsUncommittedVersion(), desc.changes)
	b.SetRawBytesInStorage(desc.GetRawBytesInStorage())
	return b
}

// IsUncommittedVersion implements the Descriptor interface.
func (desc *Mutable) IsUncommittedVersion() bool {
	clusterVersion := desc.ClusterVersion()
	return desc.IsNew() || desc.GetVersion() != clusterVersion.GetVersion()
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

// OfflineForImport sets the descriptor offline in advance of an
// import, bumping the ImportEpoch.
func (desc *Mutable) OfflineForImport() {
	desc.SetOffline(OfflineReasonImporting)
	desc.ImportType = descpb.ImportType_IMPORT_WITH_IMPORT_EPOCH
	desc.ImportEpoch++
}

// InitializeImport binds the import start time to the table descriptor.
func (desc *Mutable) InitializeImport(startWallTime int64) error {
	if desc.ImportStartWallTime != 0 {
		return errors.AssertionFailedf("Import in progress with start time %v", desc.ImportStartWallTime)
	}
	desc.ImportStartWallTime = startWallTime
	return nil
}

// FinalizeImport removes in progress import metadata from the descriptor
func (desc *Mutable) FinalizeImport() {
	desc.ImportType = 0
	desc.ImportStartWallTime = 0
}

// UpdateIndexPartitioning applies the new partition and adjusts the column info
// for the specified index descriptor. Returns false iff this was a no-op.
func UpdateIndexPartitioning(
	idx *descpb.IndexDescriptor,
	isIndexPrimary bool,
	newImplicitCols []catalog.Column,
	newPartitioning catpb.PartitioningDescriptor,
) bool {
	oldNumImplicitCols := int(idx.Partitioning.NumImplicitColumns)
	isNoOp := oldNumImplicitCols == len(newImplicitCols) && idx.Partitioning.Equal(newPartitioning)
	numCols := len(idx.KeyColumnIDs)
	newCap := numCols + len(newImplicitCols) - oldNumImplicitCols
	newColumnIDs := make([]descpb.ColumnID, len(newImplicitCols), newCap)
	newColumnNames := make([]string, len(newImplicitCols), newCap)
	newColumnDirections := make([]catenumpb.IndexColumn_Direction, len(newImplicitCols), newCap)
	for i, col := range newImplicitCols {
		newColumnIDs[i] = col.GetID()
		newColumnNames[i] = col.GetName()
		newColumnDirections[i] = catenumpb.IndexColumn_ASC
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

// NewPartitioning creates a new catalog.paritioning from the given
// partitioning descriptor. If the partitioning descriptor is nil, the resulting
// partitioning will have the default value for each entry.
func NewPartitioning(partDesc *catpb.PartitioningDescriptor) catalog.Partitioning {
	if partDesc != nil {
		return &partitioning{desc: partDesc}
	}
	partDesc = &catpb.PartitioningDescriptor{
		NumColumns:         0,
		NumImplicitColumns: 0,
		List:               nil,
		Range:              nil,
	}
	return partitioning{desc: partDesc}
}

// GetPrimaryIndex implements the TableDescriptor interface.
func (desc *wrapper) GetPrimaryIndex() catalog.UniqueWithIndexConstraint {
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
//   - the primary index, if the table is a physical table,
//   - the public non-primary indexes in the Indexes array, in order,
//   - the non-public indexes present in the Mutations array, in order,
//     if the mutation is not a drop.
//
// See also catalog.Index.Ordinal().
func (desc *wrapper) NonDropIndexes() []catalog.Index {
	return desc.getExistingOrNewIndexCache().nonDrop
}

// PartialIndexes returns a slice of all partial indexes in the underlying
// proto, in their canonical order. This is equivalent to taking the slice
// produced by DeletableNonPrimaryIndexes and filtering indexes with non-empty
// expressions.
//
// Backfilling indexes are excluded.
func (desc *wrapper) PartialIndexes() []catalog.Index {
	return desc.getExistingOrNewIndexCache().partial
}

// VectorIndexes returns a slice of all vector indexes in the underlying
// proto, in their canonical order. This is equivalent to taking the slice
// produced by DeletableNonPrimaryIndexes and filtering indexes that are not
// vector indexes.
//
// Backfilling indexes are excluded.
func (desc *wrapper) VectorIndexes() []catalog.Index {
	return desc.getExistingOrNewIndexCache().vector
}

// NonPrimaryIndexes returns a slice of all non-primary indexes, in
// their canonical order. This is equivalent to taking the slice
// produced by AllIndexes and removing the primary index.
func (desc *wrapper) NonPrimaryIndexes() []catalog.Index {
	return desc.getExistingOrNewIndexCache().nonPrimary
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

// DeletableNonPrimaryIndexes returns a slice of all non-primary
// indexes which allow being deleted from: public +
// delete-and-write-only + delete-only, in their canonical order. This
// is equivalent to taking the slice produced by AllIndexes and
// removing the primary index and backfilling indexes.
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

// getExistingOrNewColumnCache should be the only place where the columnCache
// field in wrapper is ever read.
func (desc *wrapper) getExistingOrNewColumnCache() *columnCache {
	if desc.columnCache != nil {
		return desc.columnCache
	}
	return newColumnCache(desc.TableDesc(), desc.getExistingOrNewMutationCache())
}

// AllColumns implements the TableDescriptor interface.
func (desc *wrapper) AllColumns() []catalog.Column {
	return desc.getExistingOrNewColumnCache().all
}

// PublicColumns implements the TableDescriptor interface.
func (desc *wrapper) PublicColumns() []catalog.Column {
	return desc.getExistingOrNewColumnCache().public
}

// WritableColumns implements the TableDescriptor interface.
func (desc *wrapper) WritableColumns() []catalog.Column {
	return desc.getExistingOrNewColumnCache().writable
}

// DeletableColumns implements the TableDescriptor interface.
func (desc *wrapper) DeletableColumns() []catalog.Column {
	return desc.getExistingOrNewColumnCache().deletable
}

// NonDropColumns implements the TableDescriptor interface.
func (desc *wrapper) NonDropColumns() []catalog.Column {
	return desc.getExistingOrNewColumnCache().nonDrop
}

// VisibleColumns implements the TableDescriptor interface.
func (desc *wrapper) VisibleColumns() []catalog.Column {
	return desc.getExistingOrNewColumnCache().visible
}

// AccessibleColumns implements the TableDescriptor interface.
func (desc *wrapper) AccessibleColumns() []catalog.Column {
	return desc.getExistingOrNewColumnCache().accessible
}

// UserDefinedTypeColumns implements the TableDescriptor interface.
func (desc *wrapper) UserDefinedTypeColumns() []catalog.Column {
	return desc.getExistingOrNewColumnCache().withUDTs
}

// ReadableColumns implements the TableDescriptor interface.
func (desc *wrapper) ReadableColumns() []catalog.Column {
	return desc.getExistingOrNewColumnCache().readable
}

// SystemColumns implements the TableDescriptor interface.
func (desc *wrapper) SystemColumns() []catalog.Column {
	return desc.getExistingOrNewColumnCache().system
}

// FamilyDefaultColumns implements the TableDescriptor interface.
func (desc *wrapper) FamilyDefaultColumns() []fetchpb.IndexFetchSpec_FamilyDefaultColumn {
	return desc.getExistingOrNewColumnCache().familyDefaultColumns
}

// PublicColumnIDs implements the TableDescriptor interface.
func (desc *wrapper) PublicColumnIDs() []descpb.ColumnID {
	cols := desc.PublicColumns()
	res := make([]descpb.ColumnID, len(cols))
	for i, c := range cols {
		res[i] = c.GetID()
	}
	return res
}

// IndexColumns implements the TableDescriptor interface.
func (desc *wrapper) IndexColumns(idx catalog.Index) []catalog.Column {
	if ic := desc.getExistingOrNewIndexColumnCache(idx); ic != nil {
		return ic.all
	}
	return nil
}

// IndexKeyColumns implements the TableDescriptor interface.
func (desc *wrapper) IndexKeyColumns(idx catalog.Index) []catalog.Column {
	if ic := desc.getExistingOrNewIndexColumnCache(idx); ic != nil {
		return ic.key
	}
	return nil
}

// IndexKeyColumnDirections implements the TableDescriptor interface.
func (desc *wrapper) IndexKeyColumnDirections(idx catalog.Index) []catenumpb.IndexColumn_Direction {
	if ic := desc.getExistingOrNewIndexColumnCache(idx); ic != nil {
		return ic.keyDirs
	}
	return nil
}

// IndexKeySuffixColumns implements the TableDescriptor interface.
func (desc *wrapper) IndexKeySuffixColumns(idx catalog.Index) []catalog.Column {
	if ic := desc.getExistingOrNewIndexColumnCache(idx); ic != nil {
		return ic.keySuffix
	}
	return nil
}

// IndexFullColumns implements the TableDescriptor interface.
func (desc *wrapper) IndexFullColumns(idx catalog.Index) []catalog.Column {
	if ic := desc.getExistingOrNewIndexColumnCache(idx); ic != nil {
		return ic.full
	}
	return nil
}

// IndexFullColumnDirections implements the TableDescriptor interface.
func (desc *wrapper) IndexFullColumnDirections(
	idx catalog.Index,
) []catenumpb.IndexColumn_Direction {
	if ic := desc.getExistingOrNewIndexColumnCache(idx); ic != nil {
		return ic.fullDirs
	}
	return nil
}

// IndexStoredColumns implements the TableDescriptor interface.
func (desc *wrapper) IndexStoredColumns(idx catalog.Index) []catalog.Column {
	if ic := desc.getExistingOrNewIndexColumnCache(idx); ic != nil {
		return ic.stored
	}
	return nil
}

// CheckConstraintColumns implements the TableDescriptor interface.
func (desc *wrapper) CheckConstraintColumns(ck catalog.CheckConstraint) []catalog.Column {
	n := ck.NumReferencedColumns()
	if n == 0 {
		return nil
	}
	ret := make([]catalog.Column, n)
	for i := 0; i < n; i++ {
		ret[i] = catalog.FindColumnByID(desc, ck.GetReferencedColumnID(i))
	}
	return ret
}

// ForeignKeyReferencedColumns implements the TableDescriptor interface.
func (desc *wrapper) ForeignKeyReferencedColumns(fk catalog.ForeignKeyConstraint) []catalog.Column {
	n := fk.NumReferencedColumns()
	if fk.GetReferencedTableID() != desc.GetID() || n == 0 {
		return nil
	}
	ret := make([]catalog.Column, n)
	for i := 0; i < n; i++ {
		ret[i] = catalog.FindColumnByID(desc, fk.GetReferencedColumnID(i))
	}
	return ret
}

// ForeignKeyOriginColumns implements the TableDescriptor interface.
func (desc *wrapper) ForeignKeyOriginColumns(fk catalog.ForeignKeyConstraint) []catalog.Column {
	n := fk.NumOriginColumns()
	if fk.GetOriginTableID() != desc.GetID() || n == 0 {
		return nil
	}
	ret := make([]catalog.Column, n)
	for i := 0; i < n; i++ {
		ret[i] = catalog.FindColumnByID(desc, fk.GetOriginColumnID(i))
	}
	return ret
}

// UniqueWithoutIndexColumns implements the TableDescriptor interface.
func (desc *wrapper) UniqueWithoutIndexColumns(
	uwoi catalog.UniqueWithoutIndexConstraint,
) []catalog.Column {
	n := uwoi.NumKeyColumns()
	if uwoi.ParentTableID() != desc.GetID() || n == 0 {
		return nil
	}
	ret := make([]catalog.Column, n)
	for i := 0; i < n; i++ {
		ret[i] = catalog.FindColumnByID(desc, uwoi.GetKeyColumnID(i))
	}
	return ret
}

// IndexFetchSpecKeyAndSuffixColumns implements the TableDescriptor interface.
func (desc *wrapper) IndexFetchSpecKeyAndSuffixColumns(
	idx catalog.Index,
) []fetchpb.IndexFetchSpec_KeyColumn {
	if ic := desc.getExistingOrNewIndexColumnCache(idx); ic != nil {
		return ic.keyAndSuffix
	}
	return nil
}

// getExistingOrNewIndexColumnCache is a convenience method for Index*Columns
// methods.
func (desc *wrapper) getExistingOrNewIndexColumnCache(idx catalog.Index) *indexColumnCache {
	if idx == nil {
		return nil
	}
	c := desc.getExistingOrNewColumnCache()
	if idx.Ordinal() >= len(c.index) {
		return nil
	}
	return &c.index[idx.Ordinal()]
}

// getExistingOrNewMutationCache should be the only place where the
// mutationCache field in wrapper is ever read.
func (desc *wrapper) getExistingOrNewMutationCache() *mutationCache {
	if desc.mutationCache != nil {
		return desc.mutationCache
	}
	return newMutationCache(desc.TableDesc())
}

// AllMutations implements the TableDescriptor interface.
func (desc *wrapper) AllMutations() []catalog.Mutation {
	return desc.getExistingOrNewMutationCache().all
}

// IsRefreshViewRequired implements the TableDescriptor interface.
func (desc *wrapper) IsRefreshViewRequired() bool {
	return desc.IsMaterializedView && desc.RefreshViewRequired
}

// GetObjectType implements the Object interface.
func (desc *wrapper) GetObjectType() privilege.ObjectType {
	if desc.IsVirtualTable() {
		return privilege.VirtualTable
	} else if desc.IsSequence() {
		return privilege.Sequence
	}
	return privilege.Table
}

// GetObjectTypeString implements the Object interface.
func (desc *wrapper) GetObjectTypeString() string {
	return string(desc.GetObjectType())
}

// GetInProgressImportStartTime returns the start wall time of the import if there's one in progress
func (desc *wrapper) GetInProgressImportStartTime() int64 {
	return desc.ImportStartWallTime
}

// ForEachUDTDependentForHydration implements the catalog.Descriptor interface.
func (desc *wrapper) ForEachUDTDependentForHydration(fn func(t *types.T) error) error {
	for _, c := range desc.UserDefinedTypeColumns() {
		if err := fn(c.GetType()); err != nil {
			return iterutil.Map(err)
		}
	}
	return nil
}

// MaybeRequiresTypeHydration implements the catalog.Descriptor interface.
func (desc *wrapper) MaybeRequiresTypeHydration() bool {
	return len(desc.UserDefinedTypeColumns()) > 0
}

// IsSchemaLocked implements the TableDescriptor interface.
func (desc *wrapper) IsSchemaLocked() bool {
	return desc.SchemaLocked
}

// IsPrimaryKeySwapMutation implements the TableDescriptor interface.
func (desc *wrapper) IsPrimaryKeySwapMutation(m *descpb.DescriptorMutation) bool {
	switch t := m.Descriptor_.(type) {
	case *descpb.DescriptorMutation_PrimaryKeySwap:
		return true
	case *descpb.DescriptorMutation_Index:
		// The declarative schema changer handles primary key swaps differently,
		// and does not use a PrimaryKeySwap mutation for this operation. Instead,
		// it will create a new index with a primary index encoding as an
		// index mutation. To detect a primary index swap scenario we are going to
		// in the declarative case detect the encoding type
		// and check if a declarative scpb.PrimaryIndex element with a matching ID
		// exists and is going public. Since a table can only have a single primary
		// index at a time, this would indicate this index is replacing the existing
		// primary index.
		if t.Index.EncodingType != catenumpb.PrimaryIndexEncoding {
			return false
		}
		state := desc.GetDeclarativeSchemaChangerState()
		if state == nil {
			return false
		}
		// Loop over all targets searching for primary indexes.
		for _, pk := range state.Targets {
			// Confirm the target is a primary index going to public.
			if pk.TargetStatus != scpb.Status_PUBLIC {
				continue
			}
			pk, ok := pk.ElementOneOf.(*scpb.ElementProto_PrimaryIndex)
			if !ok {
				continue
			}
			// If the primary index going public matches any current mutation
			// then this is an index swap scenario.
			if pk.PrimaryIndex.IndexID == t.Index.ID {
				return true
			}
		}
	}
	return false
}

// ExternalRowData implements the TableDescriptor interface.
func (desc *wrapper) ExternalRowData() *descpb.ExternalRowData {
	return desc.External
}

// IsRowLevelSecurityEnabled implements the TableDescriptor interface.
func (desc *wrapper) IsRowLevelSecurityEnabled() bool {
	return desc.RowLevelSecurityEnabled
}

// IsRowLevelSecurityForced implements the TableDescriptor interface.
func (desc *wrapper) IsRowLevelSecurityForced() bool {
	return desc.RowLevelSecurityForced
}
