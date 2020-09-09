// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package descpb

import (
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/errors"
)

// ToEncodingDirection converts a direction from the proto to an encoding.Direction.
func (dir IndexDescriptor_Direction) ToEncodingDirection() (encoding.Direction, error) {
	switch dir {
	case IndexDescriptor_ASC:
		return encoding.Ascending, nil
	case IndexDescriptor_DESC:
		return encoding.Descending, nil
	default:
		return encoding.Ascending, errors.Errorf("invalid direction: %s", dir)
	}
}

// ID, ColumnID, FamilyID, and IndexID are all uint32, but are each given a
// type alias to prevent accidental use of one of the types where
// another is expected.

// ID is a custom type for {Database,Table}Descriptor IDs.
type ID tree.ID

// SafeValue implements the redact.SafeValue interface.
func (ID) SafeValue() {}

// InvalidID is the uninitialised descriptor id.
const InvalidID ID = 0

// IDs is a sortable list of IDs.
type IDs []ID

func (ids IDs) Len() int           { return len(ids) }
func (ids IDs) Less(i, j int) bool { return ids[i] < ids[j] }
func (ids IDs) Swap(i, j int)      { ids[i], ids[j] = ids[j], ids[i] }

// FormatVersion is a custom type for TableDescriptor versions of the sql to
// key:value mapping.
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
type FamilyID uint32

// SafeValue implements the redact.SafeValue interface.
func (FamilyID) SafeValue() {}

// IndexID is a custom type for IndexDescriptor IDs.
type IndexID tree.IndexID

// SafeValue implements the redact.SafeValue interface.
func (IndexID) SafeValue() {}

// DescriptorVersion is a custom type for TableDescriptor Versions.
type DescriptorVersion uint32

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
)

// ColumnID is a custom type for ColumnDescriptor IDs.
type ColumnID tree.ColumnID

// SafeValue implements the redact.SafeValue interface.
func (ColumnID) SafeValue() {}

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

// Contains returns whether this list contains the input ID.
func (c ColumnIDs) Contains(i ColumnID) bool {
	for _, id := range c {
		if i == id {
			return true
		}
	}
	return false
}

// IndexDescriptorEncodingType is a custom type to represent different encoding types
// for secondary indexes.
type IndexDescriptorEncodingType uint32

const (
	// SecondaryIndexEncoding corresponds to the standard way of encoding secondary indexes
	// as described in docs/tech-notes/encoding.md. We allow the 0 value of this type
	// to have a value so that existing descriptors are encoding using this encoding.
	SecondaryIndexEncoding IndexDescriptorEncodingType = iota
	// PrimaryIndexEncoding corresponds to when a secondary index is encoded using the
	// primary index encoding as described in docs/tech-notes/encoding.md.
	PrimaryIndexEncoding
)

// Remove unused warning.
var _ = SecondaryIndexEncoding

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

// FindPartitionByName searches this partitioning descriptor for a partition
// whose name is the input and returns it, or nil if no match is found.
func (desc *PartitioningDescriptor) FindPartitionByName(name string) *PartitioningDescriptor {
	for _, l := range desc.List {
		if l.Name == name {
			return desc
		}
		if s := l.Subpartitioning.FindPartitionByName(name); s != nil {
			return s
		}
	}
	for _, r := range desc.Range {
		if r.Name == name {
			return desc
		}
	}
	return nil
}

// PartitionNames returns a slice containing the name of every partition and
// subpartition in an arbitrary order.
func (desc *PartitioningDescriptor) PartitionNames() []string {
	var names []string
	for _, l := range desc.List {
		names = append(names, l.Name)
		names = append(names, l.Subpartitioning.PartitionNames()...)
	}
	for _, r := range desc.Range {
		names = append(names, r.Name)
	}
	return names
}

// Public implements the Descriptor interface.
func (desc *TableDescriptor) Public() bool {
	return desc.State == DescriptorState_PUBLIC
}

// Offline returns true if the table is importing.
func (desc *TableDescriptor) Offline() bool {
	return desc.State == DescriptorState_OFFLINE
}

// Dropped returns true if the table is being dropped.
func (desc *TableDescriptor) Dropped() bool {
	return desc.State == DescriptorState_DROP
}

// Adding returns true if the table is being added.
func (desc *TableDescriptor) Adding() bool {
	return desc.State == DescriptorState_ADD
}

// IsTable returns true if the TableDescriptor actually describes a
// Table resource, as opposed to a different resource (like a View).
func (desc *TableDescriptor) IsTable() bool {
	return !desc.IsView() && !desc.IsSequence()
}

// IsView returns true if the TableDescriptor actually describes a
// View resource rather than a Table.
func (desc *TableDescriptor) IsView() bool {
	return desc.ViewQuery != ""
}

// MaterializedView returns whether or not this TableDescriptor is a
// MaterializedView.
func (desc *TableDescriptor) MaterializedView() bool {
	return desc.IsMaterializedView
}

// IsAs returns true if the TableDescriptor actually describes
// a Table resource with an As source.
func (desc *TableDescriptor) IsAs() bool {
	return desc.CreateQuery != ""
}

// IsSequence returns true if the TableDescriptor actually describes a
// Sequence resource rather than a Table.
func (desc *TableDescriptor) IsSequence() bool {
	return desc.SequenceOpts != nil
}

// IsVirtualTable returns true if the TableDescriptor describes a
// virtual Table (like the information_schema tables) and thus doesn't
// need to be physically stored.
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

// IsVirtualTable returns true if the TableDescriptor describes a
// virtual Table (like the information_schema tables) and thus doesn't
// need to be physically stored.
func IsVirtualTable(id ID) bool {
	return catconstants.MinVirtualID <= id
}

// IsSystemConfigID returns whether this ID is for a system config object.
func IsSystemConfigID(id ID) bool {
	return id > 0 && id <= keys.MaxSystemConfigDescID
}

// IsReservedID returns whether this ID is for any system object.
func IsReservedID(id ID) bool {
	return id > 0 && id <= keys.MaxReservedDescID
}

// AnonymousTable is the empty table name, used when a data source
// has no own name, e.g. VALUES, subqueries or the empty source.
var AnonymousTable = tree.TableName{}

// HasOwner returns true if the sequence options indicate an owner exists.
func (opts *TableDescriptor_SequenceOpts) HasOwner() bool {
	return !opts.SequenceOwner.Equal(TableDescriptor_SequenceOpts_SequenceOwner{})
}
