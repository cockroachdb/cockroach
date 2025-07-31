// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package descpb

import (
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/protoreflect"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
)

// ID, ColumnID, FamilyID, and IndexID are all uint32, but are each given a
// type alias to prevent accidental use of one of the types where
// another is expected.

// ID is a custom type for {Database,Table}Descriptor IDs.
type ID = catid.DescID

// InvalidID is the uninitialised descriptor id.
const InvalidID = catid.InvalidDescID

// IDs is a sortable list of IDs.
type IDs []ID

func (ids IDs) Len() int           { return len(ids) }
func (ids IDs) Less(i, j int) bool { return ids[i] < ids[j] }
func (ids IDs) Swap(i, j int)      { ids[i], ids[j] = ids[j], ids[i] }

// Contains returns whether `ids` contains `targetID`.
func (ids IDs) Contains(targetID ID) bool {
	for _, id := range ids {
		if id == targetID {
			return true
		}
	}
	return false
}

// FormatVersion is a custom type for TableDescriptor versions of the sql to
// key:value mapping.
//
//go:generate stringer -type=FormatVersion
type FormatVersion uint32

const (
	_ FormatVersion = iota
	// BaseFormatVersion corresponds to the encoding described in
	// https://www.cockroachlabs.com/blog/sql-in-cockroachdb-mapping-table-data-to-key-value-storage/.
	BaseFormatVersion
	// FamilyFormatVersion corresponds to the encoding described in
	// https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20151214_sql_column_families.md
	FamilyFormatVersion
	// InterleavedFormatVersion corresponds to the encoding described in
	// https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20160624_sql_interleaved_tables.md
	InterleavedFormatVersion
)

// FamilyID is a custom type for ColumnFamilyDescriptor IDs.
type FamilyID = catid.FamilyID

// IndexID is a custom type for IndexDescriptor IDs.
type IndexID = catid.IndexID

// ConstraintID is a custom type for TableDescriptor constraint IDs.
type ConstraintID = catid.ConstraintID

// TriggerID is a custom type for TableDescriptor trigger IDs.
type TriggerID = catid.TriggerID

// PolicyID is a custom type for TableDescriptor policy IDs.
type PolicyID = catid.PolicyID

// DescriptorVersion is a custom type for TableDescriptor Versions.
type DescriptorVersion uint64

// SafeValue implements the redact.SafeValue interface.
func (DescriptorVersion) SafeValue() {}

// IndexDescriptorVersion is a custom type for IndexDescriptor Versions.
type IndexDescriptorVersion uint32

// SafeValue implements the redact.SafeValue interface.
func (IndexDescriptorVersion) SafeValue() {}

const (
	// BaseIndexFormatVersion corresponds to the original encoding of secondary indexes that
	// don't respect table level column family definitions. We allow the 0 value of the type to
	// have a value so that existing index descriptors are denoted as having the base format.
	BaseIndexFormatVersion IndexDescriptorVersion = iota
	// SecondaryIndexFamilyFormatVersion corresponds to the encoding of secondary indexes that
	// use table level column family definitions.
	SecondaryIndexFamilyFormatVersion
	// EmptyArraysInInvertedIndexesVersion corresponds to the encoding of secondary indexes
	// that is identical to SecondaryIndexFamilyFormatVersion, but also includes a key encoding
	// for empty arrays in array inverted indexes.
	EmptyArraysInInvertedIndexesVersion
	// StrictIndexColumnIDGuaranteesVersion corresponds to the encoding of
	// secondary indexes that is identical to EmptyArraysInInvertedIndexesVersion,
	// but also includes guarantees on the column ID slices in the index:
	// each column ID in the ColumnIDs, StoreColumnIDs and KeySuffixColumnIDs
	// slices are unique within each slice, and the slices form disjoint sets.
	StrictIndexColumnIDGuaranteesVersion
	// PrimaryIndexWithStoredColumnsVersion corresponds to the encoding of
	// primary indexes that is identical to the unspecified scheme previously in
	// use (the IndexDescriptorVersion type was originally only used for
	// secondary indexes) but with the guarantee that the StoreColumnIDs and
	// StoreColumnNames slices are explicitly populated and maintained. Previously
	// these were implicitly derived based on the set of non-virtual columns in
	// the table.
	PrimaryIndexWithStoredColumnsVersion
	// LatestIndexDescriptorVersion corresponds to the latest encoding version.
	LatestIndexDescriptorVersion = PrimaryIndexWithStoredColumnsVersion
)

// PGAttributeNum is a custom type for ColumnDescriptor's PGAttributeNum field.
type PGAttributeNum = catid.PGAttributeNum

// ColumnID is a custom type for ColumnDescriptor IDs.
type ColumnID = catid.ColumnID

// ColumnIDs is a slice of ColumnDescriptor IDs.
type ColumnIDs []ColumnID

func (c ColumnIDs) Len() int           { return len(c) }
func (c ColumnIDs) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }
func (c ColumnIDs) Less(i, j int) bool { return c[i] < c[j] }

// HasPrefix returns true if the input list is a prefix of this list.
func (c ColumnIDs) HasPrefix(input ColumnIDs) bool {
	if len(input) > len(c) {
		return false
	}
	for i := range input {
		if input[i] != c[i] {
			return false
		}
	}
	return true
}

// Equals returns true if the input list is equal to this list.
func (c ColumnIDs) Equals(input ColumnIDs) bool {
	if len(input) != len(c) {
		return false
	}
	for i := range input {
		if input[i] != c[i] {
			return false
		}
	}
	return true
}

// PermutationOf returns true if this list and the input list contain the same
// set of column IDs in any order. Duplicate ColumnIDs have no effect.
func (c ColumnIDs) PermutationOf(input ColumnIDs) bool {
	ourColsSet := intsets.MakeFast()
	for _, col := range c {
		ourColsSet.Add(int(col))
	}

	inputColsSet := intsets.MakeFast()
	for _, inputCol := range input {
		inputColsSet.Add(int(inputCol))
	}

	return inputColsSet.Equals(ourColsSet)
}

// Contains returns whether this list contains the input ID.
func (c ColumnIDs) Contains(i ColumnID) bool {
	for _, id := range c {
		if i == id {
			return true
		}
	}
	return false
}

// MutationID is a custom type for TableDescriptor mutations.
type MutationID uint32

// SafeValue implements the redact.SafeValue interface.
func (MutationID) SafeValue() {}

// InvalidMutationID is the uninitialised mutation id.
const InvalidMutationID MutationID = 0

// IsSet returns whether or not the foreign key actually references a table.
func (f ForeignKeyReference) IsSet() bool {
	return f.Table != 0
}

// Public implements the Descriptor interface.
func (desc *TableDescriptor) Public() bool {
	return desc.State == DescriptorState_PUBLIC
}

// Offline implements the Descriptor interface.
func (desc *TableDescriptor) Offline() bool {
	return desc.State == DescriptorState_OFFLINE
}

// Dropped implements the Descriptor interface.
func (desc *TableDescriptor) Dropped() bool {
	return desc.State == DescriptorState_DROP
}

// Adding returns true if the table is being added.
func (desc *TableDescriptor) Adding() bool {
	return desc.State == DescriptorState_ADD
}

// IsTable implements the TableDescriptor interface.
func (desc *TableDescriptor) IsTable() bool {
	return !desc.IsView() && !desc.IsSequence()
}

// IsView implements the TableDescriptor interface.
func (desc *TableDescriptor) IsView() bool {
	return desc.ViewQuery != ""
}

// MaterializedView implements the TableDescriptor interface.
func (desc *TableDescriptor) MaterializedView() bool {
	return desc.IsMaterializedView
}

// IsReadOnly implements the TableDescriptor interface.
func (desc *TableDescriptor) IsReadOnly() bool {
	return desc.IsMaterializedView || desc.GetExternal() != nil
}

// IsPhysicalTable implements the TableDescriptor interface.
func (desc *TableDescriptor) IsPhysicalTable() bool {
	return desc.IsSequence() || (desc.IsTable() && !desc.IsVirtualTable()) || desc.MaterializedView()
}

// IsAs implements the TableDescriptor interface.
func (desc *TableDescriptor) IsAs() bool {
	return desc.CreateQuery != ""
}

// IsSequence implements the TableDescriptor interface.
func (desc *TableDescriptor) IsSequence() bool {
	return desc.SequenceOpts != nil
}

// IsVirtualTable implements the TableDescriptor interface.
func (desc *TableDescriptor) IsVirtualTable() bool {
	return IsVirtualTable(desc.ID)
}

// Persistence returns the Persistence from the TableDescriptor.
func (desc *TableDescriptor) Persistence() tree.Persistence {
	if desc.Temporary {
		return tree.PersistenceTemporary
	}
	return tree.PersistencePermanent
}

// ForEachPublicIndex is exported to provide low-overhead access to the set of
// public indexes in a table descriptor for use in backup planning.
//
// Most users should prefer the methods provided by the catalog package.
func (desc *TableDescriptor) ForEachPublicIndex(f func(*IndexDescriptor)) {
	f(&desc.PrimaryIndex)
	for i := range desc.Indexes {
		f(&desc.Indexes[i])
	}
}

// IsVirtualTable returns true if the TableDescriptor describes a
// virtual Table (like the information_schema tables) and thus doesn't
// need to be physically stored.
func IsVirtualTable(id ID) bool {
	return catconstants.MinVirtualID <= id
}

// IsSystemConfigID returns whether this ID is for a system config object.
func IsSystemConfigID(id ID) bool {
	return id == keys.DescriptorTableID || id == keys.ZonesTableID
}

// AnonymousTable is the empty table name, used when a data source
// has no own name, e.g. VALUES, subqueries or the empty source.
var AnonymousTable = tree.TableName{}

// HasOwner returns true if the sequence options indicate an owner exists.
func (opts *TableDescriptor_SequenceOpts) HasOwner() bool {
	return !opts.SequenceOwner.Equal(TableDescriptor_SequenceOpts_SequenceOwner{})
}

// EffectiveCacheSize returns the CacheSize or NodeCacheSize field of a sequence option with
// the exception that it will return 1 if both fields are set to 0.
// A cache size of 1 indicates that there is no caching. A node cache size of 0 indicates there is no
// node-level caching. The returned value
// will always be greater than or equal to 1.
//
// Prior to #51259, sequence caching was unimplemented and cache sizes were
// left uninitialized (ie. to have a value of 0). If a sequence has a cache
// size of 0, it should be treated in the same was as sequences with cache
// sizes of 1.
func (opts *TableDescriptor_SequenceOpts) EffectiveCacheSize() int64 {
	if opts.CacheSize == 0 && opts.NodeCacheSize == 0 {
		return 1
	}
	if opts.CacheSize == 1 && opts.NodeCacheSize != 0 {
		return opts.NodeCacheSize
	}
	return opts.CacheSize
}

// SafeValue implements the redact.SafeValue interface.
func (ConstraintValidity) SafeValue() {}

// SafeValue implements the redact.SafeValue interface.
func (DescriptorMutation_Direction) SafeValue() {}

// SafeValue implements the redact.SafeValue interface.
func (DescriptorMutation_State) SafeValue() {}

// SafeValue implements the redact.SafeValue interface.
func (DescriptorState) SafeValue() {}

// IsPartial returns true if the constraint is a partial unique constraint.
func (u *UniqueWithoutIndexConstraint) IsPartial() bool {
	return u.Predicate != ""
}

// GetParentID implements the catalog.NameKeyHaver interface.
func (ni NameInfo) GetParentID() ID {
	return ni.ParentID
}

// GetParentSchemaID implements the catalog.NameKeyHaver interface.
func (ni NameInfo) GetParentSchemaID() ID {
	return ni.ParentSchemaID
}

// GetName implements the catalog.NameKeyHaver interface.
func (ni NameInfo) GetName() string {
	return ni.Name
}

func init() {
	protoreflect.RegisterShorthands((*Descriptor)(nil), "descriptor", "desc")
}
