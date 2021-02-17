// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catalog

import (
	"github.com/cockroachdb/cockroach/pkg/geo/geoindex"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
)

// TableElementMaybeMutation is an interface used as a subtype for the various
// table descriptor elements which may be present in a mutation.
type TableElementMaybeMutation interface {
	// IsMutation returns true iff this table element is in a mutation.
	IsMutation() bool

	// IsRollback returns true iff the table element is in a rollback mutation.
	IsRollback() bool

	// MutationID returns the table element's mutationID if applicable,
	// descpb.InvalidMutationID otherwise.
	MutationID() descpb.MutationID

	// WriteAndDeleteOnly returns true iff the table element is in a mutation in
	// the delete-and-write-only state.
	WriteAndDeleteOnly() bool

	// DeleteOnly returns true iff the table element is in a mutation in the
	// delete-only state.
	DeleteOnly() bool

	// Adding returns true iff the table element is in an add mutation.
	Adding() bool

	// Dropped returns true iff the table element is in a drop mutation.
	Dropped() bool
}

// Mutation is an interface around a table descriptor mutation.
type Mutation interface {
	TableElementMaybeMutation

	// AsColumn returns the corresponding Column if the mutation is on a column,
	// nil otherwise.
	AsColumn() Column

	// AsIndex returns the corresponding Index if the mutation is on an index,
	// nil otherwise.
	AsIndex() Index

	// AsConstraint returns the corresponding ConstraintToUpdate if the mutation
	// is on a constraint, nil otherwise.
	AsConstraint() ConstraintToUpdate

	// AsPrimaryKeySwap returns the corresponding PrimaryKeySwap if the mutation
	// is a primary key swap, nil otherwise.
	AsPrimaryKeySwap() PrimaryKeySwap

	// AsComputedColumnSwap returns the corresponding ComputedColumnSwap if the
	// mutation is a computed column swap, nil otherwise.
	AsComputedColumnSwap() ComputedColumnSwap

	// AsMaterializedViewRefresh returns the corresponding MaterializedViewRefresh
	// if the mutation is a materialized view refresh, nil otherwise.
	AsMaterializedViewRefresh() MaterializedViewRefresh

	// MutationOrdinal returns the ordinal of the mutation in the underlying table
	// descriptor's Mutations slice.
	MutationOrdinal() int
}

// Index is an interface around the index descriptor types.
type Index interface {
	TableElementMaybeMutation

	// IndexDesc returns the underlying protobuf descriptor.
	// Ideally, this method should be called as rarely as possible.
	IndexDesc() *descpb.IndexDescriptor

	// IndexDescDeepCopy returns a deep copy of the underlying proto.
	IndexDescDeepCopy() descpb.IndexDescriptor

	// Ordinal returns the ordinal of the index in its parent table descriptor.
	//
	// The ordinal of an index in a `tableDesc descpb.TableDescriptor` is
	// defined as follows:
	// - 0 is the ordinal of the primary index,
	// - [1:1+len(tableDesc.Indexes)] is the range of public non-primary indexes,
	// - [1+len(tableDesc.Indexes):] is the range of non-public indexes.
	//
	// In terms of a `table catalog.TableDescriptor` interface, it is defined
	// as the catalog.Index object's position in the table.AllIndexes() slice.
	Ordinal() int

	// Primary returns true iff the index is the primary index for the table
	// descriptor.
	Primary() bool

	// Public returns true iff the index is active, i.e. readable, in the table
	// descriptor.
	Public() bool

	// The remaining methods operate on the underlying descpb.IndexDescriptor object.

	GetID() descpb.IndexID
	GetName() string
	IsInterleaved() bool
	IsPartial() bool
	IsUnique() bool
	IsDisabled() bool
	IsSharded() bool
	IsCreatedExplicitly() bool
	GetPredicate() string
	GetType() descpb.IndexDescriptor_Type
	GetGeoConfig() geoindex.Config
	GetVersion() descpb.IndexDescriptorVersion
	GetEncodingType() descpb.IndexDescriptorEncodingType

	GetSharded() descpb.ShardedDescriptor
	GetShardColumnName() string

	IsValidOriginIndex(originColIDs descpb.ColumnIDs) bool
	IsValidReferencedUniqueConstraint(referencedColIDs descpb.ColumnIDs) bool

	GetPartitioning() descpb.PartitioningDescriptor
	FindPartitionByName(name string) descpb.PartitioningDescriptor
	PartitionNames() []string

	NumInterleaveAncestors() int
	GetInterleaveAncestor(ancestorOrdinal int) descpb.InterleaveDescriptor_Ancestor

	NumInterleavedBy() int
	GetInterleavedBy(interleavedByOrdinal int) descpb.ForeignKeyReference

	NumColumns() int
	GetColumnID(columnOrdinal int) descpb.ColumnID
	GetColumnName(columnOrdinal int) string
	GetColumnDirection(columnOrdinal int) descpb.IndexDescriptor_Direction

	ForEachColumnID(func(id descpb.ColumnID) error) error
	ContainsColumnID(colID descpb.ColumnID) bool
	InvertedColumnID() descpb.ColumnID
	InvertedColumnName() string

	NumStoredColumns() int
	GetStoredColumnID(storedColumnOrdinal int) descpb.ColumnID
	GetStoredColumnName(storedColumnOrdinal int) string
	HasOldStoredColumns() bool

	NumExtraColumns() int
	GetExtraColumnID(extraColumnOrdinal int) descpb.ColumnID

	NumCompositeColumns() int
	GetCompositeColumnID(compositeColumnOrdinal int) descpb.ColumnID
}

// Column is an interface around the index descriptor types.
type Column interface {
	TableElementMaybeMutation

	// ColumnDesc returns the underlying protobuf descriptor.
	// Ideally, this method should be called as rarely as possible.
	ColumnDesc() *descpb.ColumnDescriptor

	// ColumnDescDeepCopy returns a deep copy of the underlying proto.
	ColumnDescDeepCopy() descpb.ColumnDescriptor

	// DeepCopy returns a deep copy of the receiver.
	DeepCopy() Column

	// Ordinal returns the ordinal of the column in its parent table descriptor.
	//
	// The ordinal of a column in a `tableDesc descpb.TableDescriptor` is
	// defined as follows:
	// - [0:len(tableDesc.Columns)] is the range of public columns,
	// - [len(tableDesc.Columns):] is the range of non-public columns.
	//
	// In terms of a `table catalog.TableDescriptor` interface, it is defined
	// as the catalog.Column object's position in the table.AllColumns() slice.
	Ordinal() int

	// Public returns true iff the column is active, i.e. readable, in the table
	// descriptor.
	Public() bool

	// GetID returns the column ID.
	GetID() descpb.ColumnID

	// GetName returns the column name as a string.
	GetName() string

	// ColName returns the column name as a tree.Name.
	ColName() tree.Name

	// HasType returns true iff the column type is set.
	HasType() bool

	// GetType returns the column type.
	GetType() *types.T

	// IsNullable returns true iff the column allows NULL values.
	IsNullable() bool

	// HasDefault returns true iff the column has a default expression set.
	HasDefault() bool

	// GetDefaultExpr returns the column default expression if it exists,
	// empty string otherwise.
	GetDefaultExpr() string

	// IsComputed returns true iff the column is a computed column.
	IsComputed() bool

	// GetComputeExpr returns the column computed expression if it exists,
	// empty string otherwise.
	GetComputeExpr() string

	// IsHidden returns true iff the column is not visible.
	IsHidden() bool

	// NumUsesSequences returns the number of sequences used by this column.
	NumUsesSequences() int

	// GetUsesSequenceID returns the ID of a sequence used by this column.
	GetUsesSequenceID(usesSequenceOrdinal int) descpb.ID

	// NumOwnsSequences returns the number of sequences owned by this column.
	NumOwnsSequences() int

	// GetOwnsSequenceID returns the ID of a sequence owned by this column.
	GetOwnsSequenceID(ownsSequenceOrdinal int) descpb.ID

	// IsVirtual returns true iff the column is a virtual column.
	IsVirtual() bool

	// CheckCanBeInboundFKRef returns whether the given column can be on the
	// referenced (target) side of a foreign key relation.
	CheckCanBeInboundFKRef() error

	// CheckCanBeOutboundFKRef returns whether the given column can be on the
	// referencing (origin) side of a foreign key relation.
	CheckCanBeOutboundFKRef() error

	// GetPGAttributeNum returns the PGAttributeNum of the column descriptor
	// if the PGAttributeNum is set (non-zero). Returns the ID of the
	// column descriptor if the PGAttributeNum is not set.
	GetPGAttributeNum() uint32

	// IsSystemColumn returns true iff the column is a system column.
	IsSystemColumn() bool
}

// ConstraintToUpdate is an interface around a constraint mutation.
type ConstraintToUpdate interface {
	TableElementMaybeMutation

	// ConstraintToUpdateDesc returns the underlying protobuf descriptor.
	ConstraintToUpdateDesc() *descpb.ConstraintToUpdate
}

// PrimaryKeySwap is an interface around a primary key swap mutation.
type PrimaryKeySwap interface {
	TableElementMaybeMutation

	// PrimaryKeySwapDesc returns the underlying protobuf descriptor.
	PrimaryKeySwapDesc() *descpb.PrimaryKeySwap
}

// ComputedColumnSwap is an interface around a computed column swap mutation.
type ComputedColumnSwap interface {
	TableElementMaybeMutation

	// ComputedColumnSwapDesc returns the underlying protobuf descriptor.
	ComputedColumnSwapDesc() *descpb.ComputedColumnSwap
}

// MaterializedViewRefresh is an interface around a materialized view refresh
// mutation.
type MaterializedViewRefresh interface {
	TableElementMaybeMutation

	// MaterializedViewRefreshDesc returns the underlying protobuf descriptor.
	MaterializedViewRefreshDesc() *descpb.MaterializedViewRefresh
}

func isIndexInSearchSet(desc TableDescriptor, opts IndexOpts, idx Index) bool {
	if !opts.NonPhysicalPrimaryIndex && idx.Primary() && !desc.IsPhysicalTable() {
		return false
	}
	if !opts.AddMutations && idx.Adding() {
		return false
	}
	if !opts.DropMutations && idx.Dropped() {
		return false
	}
	return true
}

// ForEachIndex runs f over each index in the table descriptor according to
// filter parameters in opts. Indexes are visited in their canonical order,
// see Index.Ordinal(). ForEachIndex supports iterutil.StopIteration().
func ForEachIndex(desc TableDescriptor, opts IndexOpts, f func(idx Index) error) error {
	for _, idx := range desc.AllIndexes() {
		if !isIndexInSearchSet(desc, opts, idx) {
			continue
		}
		if err := f(idx); err != nil {
			if iterutil.Done(err) {
				return nil
			}
			return err
		}
	}
	return nil
}

func forEachIndex(slice []Index, f func(idx Index) error) error {
	for _, idx := range slice {
		if err := f(idx); err != nil {
			if iterutil.Done(err) {
				return nil
			}
			return err
		}
	}
	return nil
}

// ForEachActiveIndex is like ForEachIndex over ActiveIndexes().
func ForEachActiveIndex(desc TableDescriptor, f func(idx Index) error) error {
	return forEachIndex(desc.ActiveIndexes(), f)
}

// ForEachNonDropIndex is like ForEachIndex over NonDropIndexes().
func ForEachNonDropIndex(desc TableDescriptor, f func(idx Index) error) error {
	return forEachIndex(desc.NonDropIndexes(), f)
}

// ForEachPartialIndex is like ForEachIndex over PartialIndexes().
func ForEachPartialIndex(desc TableDescriptor, f func(idx Index) error) error {
	return forEachIndex(desc.PartialIndexes(), f)
}

// ForEachPublicNonPrimaryIndex is like ForEachIndex over
// PublicNonPrimaryIndexes().
func ForEachPublicNonPrimaryIndex(desc TableDescriptor, f func(idx Index) error) error {
	return forEachIndex(desc.PublicNonPrimaryIndexes(), f)
}

// ForEachWritableNonPrimaryIndex is like ForEachIndex over
// WritableNonPrimaryIndexes().
func ForEachWritableNonPrimaryIndex(desc TableDescriptor, f func(idx Index) error) error {
	return forEachIndex(desc.WritableNonPrimaryIndexes(), f)
}

// ForEachDeletableNonPrimaryIndex is like ForEachIndex over
// DeletableNonPrimaryIndexes().
func ForEachDeletableNonPrimaryIndex(desc TableDescriptor, f func(idx Index) error) error {
	return forEachIndex(desc.DeletableNonPrimaryIndexes(), f)
}

// ForEachDeleteOnlyNonPrimaryIndex is like ForEachIndex over
// DeleteOnlyNonPrimaryIndexes().
func ForEachDeleteOnlyNonPrimaryIndex(desc TableDescriptor, f func(idx Index) error) error {
	return forEachIndex(desc.DeleteOnlyNonPrimaryIndexes(), f)
}

// FindIndex returns the first index for which test returns true, nil otherwise,
// according to the parameters in opts just like ForEachIndex.
// Indexes are visited in their canonical order, see Index.Ordinal().
func FindIndex(desc TableDescriptor, opts IndexOpts, test func(idx Index) bool) Index {
	for _, idx := range desc.AllIndexes() {
		if !isIndexInSearchSet(desc, opts, idx) {
			continue
		}
		if test(idx) {
			return idx
		}
	}
	return nil
}

func findIndex(slice []Index, test func(idx Index) bool) Index {
	for _, idx := range slice {
		if test(idx) {
			return idx
		}
	}
	return nil
}

// FindActiveIndex returns the first index in ActiveIndex() for which test
// returns true.
func FindActiveIndex(desc TableDescriptor, test func(idx Index) bool) Index {
	return findIndex(desc.ActiveIndexes(), test)
}

// FindNonDropIndex returns the first index in NonDropIndex() for which test
// returns true.
func FindNonDropIndex(desc TableDescriptor, test func(idx Index) bool) Index {
	return findIndex(desc.NonDropIndexes(), test)
}

// FindPartialIndex returns the first index in PartialIndex() for which test
// returns true.
func FindPartialIndex(desc TableDescriptor, test func(idx Index) bool) Index {
	return findIndex(desc.PartialIndexes(), test)
}

// FindPublicNonPrimaryIndex returns the first index in PublicNonPrimaryIndex()
// for which test returns true.
func FindPublicNonPrimaryIndex(desc TableDescriptor, test func(idx Index) bool) Index {
	return findIndex(desc.PublicNonPrimaryIndexes(), test)
}

// FindWritableNonPrimaryIndex returns the first index in
// WritableNonPrimaryIndex() for which test returns true.
func FindWritableNonPrimaryIndex(desc TableDescriptor, test func(idx Index) bool) Index {
	return findIndex(desc.WritableNonPrimaryIndexes(), test)
}

// FindDeletableNonPrimaryIndex returns the first index in
// DeletableNonPrimaryIndex() for which test returns true.
func FindDeletableNonPrimaryIndex(desc TableDescriptor, test func(idx Index) bool) Index {
	return findIndex(desc.DeletableNonPrimaryIndexes(), test)
}

// FindDeleteOnlyNonPrimaryIndex returns the first index in
// DeleteOnlyNonPrimaryIndex() for which test returns true.
func FindDeleteOnlyNonPrimaryIndex(desc TableDescriptor, test func(idx Index) bool) Index {
	return findIndex(desc.DeleteOnlyNonPrimaryIndexes(), test)
}

// UserDefinedTypeColsHaveSameVersion returns whether one table descriptor's
// columns with user defined type metadata have the same versions of metadata
// as in the other descriptor. Note that this function is only valid on two
// descriptors representing the same table at the same version.
func UserDefinedTypeColsHaveSameVersion(desc TableDescriptor, otherDesc TableDescriptor) bool {
	otherCols := otherDesc.UserDefinedTypeColumns()
	for i, thisCol := range desc.UserDefinedTypeColumns() {
		this, other := thisCol.GetType(), otherCols[i].GetType()
		if this.TypeMeta.Version != other.TypeMeta.Version {
			return false
		}
	}
	return true
}

// ColumnIDToOrdinalMap returns a map from Column ID to the ordinal
// position of that column.
func ColumnIDToOrdinalMap(columns []Column) TableColMap {
	var m TableColMap
	for _, col := range columns {
		m.Set(col.GetID(), col.Ordinal())
	}
	return m
}

// ColumnTypes returns the types of the given columns
func ColumnTypes(columns []Column) []*types.T {
	return ColumnTypesWithVirtualCol(columns, nil)
}

// ColumnTypesWithVirtualCol returns the types of all given columns,
// If virtualCol is non-nil, substitutes the type of the virtual
// column instead of the column with the same ID.
func ColumnTypesWithVirtualCol(columns []Column, virtualCol Column) []*types.T {
	t := make([]*types.T, len(columns))
	for i, col := range columns {
		t[i] = col.GetType()
		if virtualCol != nil && col.GetID() == virtualCol.GetID() {
			t[i] = virtualCol.GetType()
		}
	}
	return t
}
