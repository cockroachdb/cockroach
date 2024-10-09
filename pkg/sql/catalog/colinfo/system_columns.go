// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colinfo

import (
	"math"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// Similar to Postgres, we also expose hidden system columns on tables.
// These system columns are not explicitly part of the TableDescriptor,
// and instead are constructs that are manipulated purely during planning.
// As of now, these system columns are able to be generated from the set
// of KV's that make up a row, and are produced by the row and cfetcher.
// Each system column is given a kind, and associated with a particular
// column ID that is counting down from math.MaxUint32. This is done so that
// each system column ID won't conflict with existing column ID's and also
// will be stable across all changes to the table.
// * crdb_internal_mvcc_timestamp: contains a representation of the row's
//   HLC timestamp.
// * tableoid: A Postgres system column that contains the ID of the table
//   that a particular row came from.

const (
	// MVCCTimestampColumnID is the ColumnID of the MVCC timestamp column.
	MVCCTimestampColumnID descpb.ColumnID = math.MaxUint32 - iota

	// TableOIDColumnID is the ID of the tableoid system column.
	TableOIDColumnID

	// OriginIDColumnID is the ColumnID of the OriginID column
	// which returns the OriginID from the column family with the
	// largest OriginTimestamp.
	//
	// NB: The semantics of this column are subject to change and
	// should not be relied upon.
	OriginIDColumnID

	// OriginTimestampColumnID is the ColumnID of the OriginTimestamp column
	// which returns the most recent OriginTimestamp from the
	// MVCCValueHeader.
	//
	// In the presence of multiple column families, this column
	// will only be non-NULL if the latest OriginTimstamp is
	// larger than then MVCC timestamp of all column families
	// _without_ and OriginTimestamp.
	//
	// NB: The semantics of this column are subject to change and
	// should not be relied upon.
	OriginTimestampColumnID

	numSystemColumns = iota
)

func init() {
	if len(AllSystemColumnDescs) != numSystemColumns {
		panic("need to update system column IDs or descriptors")
	}
	if catalog.NumSystemColumns != numSystemColumns {
		panic("need to update catalog.NumSystemColumns")
	}
	if catalog.SmallestSystemColumnColumnID != math.MaxUint32-numSystemColumns+1 {
		panic("need to update catalog.SmallestSystemColumnColumnID")
	}
	for _, desc := range AllSystemColumnDescs {
		if desc.SystemColumnKind != GetSystemColumnKindFromColumnID(desc.ID) {
			panic("system column ID ordering must match SystemColumnKind value ordering")
		}
	}
}

// AllSystemColumnDescs contains all registered system columns.
var AllSystemColumnDescs = []descpb.ColumnDescriptor{
	MVCCTimestampColumnDesc,
	TableOIDColumnDesc,
	OriginIDColumnDesc,
	OriginTimestampColumnDesc,
}

// MVCCTimestampColumnDesc is a column descriptor for the MVCC system column.
var MVCCTimestampColumnDesc = descpb.ColumnDescriptor{
	Name:             MVCCTimestampColumnName,
	Type:             MVCCTimestampColumnType,
	Hidden:           true,
	Nullable:         true,
	SystemColumnKind: catpb.SystemColumnKind_MVCCTIMESTAMP,
	ID:               MVCCTimestampColumnID,
}

// MVCCTimestampColumnName is the name of the MVCC timestamp system column.
const MVCCTimestampColumnName = "crdb_internal_mvcc_timestamp"

// MVCCTimestampColumnType is the type of the MVCC timestamp system column.
var MVCCTimestampColumnType = types.Decimal

// OriginIDColumnDesc is a column descriptor for the OriginID system column.
var OriginIDColumnDesc = descpb.ColumnDescriptor{
	Name:             OriginIDColumnName,
	Type:             OriginIDColumnType,
	Hidden:           true,
	Nullable:         true,
	SystemColumnKind: catpb.SystemColumnKind_ORIGINID,
	ID:               OriginIDColumnID,
}

// OriginIDColumnName is the name of the OriginID system column.
const OriginIDColumnName = "crdb_internal_origin_id"

// OriginIDColumnType is the type of the OriginID system column.
var OriginIDColumnType = types.Int4

// OriginTimestampColumnDesc is a column descriptor for the OriginTimestamp system column.
var OriginTimestampColumnDesc = descpb.ColumnDescriptor{
	Name:             OriginTimestampColumnName,
	Type:             OriginTimestampColumnType,
	Hidden:           true,
	Nullable:         true,
	SystemColumnKind: catpb.SystemColumnKind_ORIGINTIMESTAMP,
	ID:               OriginTimestampColumnID,
}

// OriginTimestampColumnName is the name of the OriginTimestamp system column.
const OriginTimestampColumnName = "crdb_internal_origin_timestamp"

// OriginTimestampColumnType is the type of the OriginTimestamp system column.
var OriginTimestampColumnType = types.Decimal

// TableOIDColumnDesc is a column descriptor for the tableoid column.
var TableOIDColumnDesc = descpb.ColumnDescriptor{
	Name:             TableOIDColumnName,
	Type:             types.Oid,
	Hidden:           true,
	Nullable:         true,
	SystemColumnKind: catpb.SystemColumnKind_TABLEOID,
	ID:               TableOIDColumnID,
}

// TableOIDColumnName is the name of the tableoid system column.
const TableOIDColumnName = "tableoid"

// IsColIDSystemColumn returns whether a column ID refers to a system column.
func IsColIDSystemColumn(colID descpb.ColumnID) bool {
	return colID > math.MaxUint32-numSystemColumns
}

// GetSystemColumnKindFromColumnID returns the kind of system column that colID
// refers to.
func GetSystemColumnKindFromColumnID(colID descpb.ColumnID) catpb.SystemColumnKind {
	i := math.MaxUint32 - uint32(colID)
	if i >= numSystemColumns {
		return catpb.SystemColumnKind_NONE
	}
	return catpb.SystemColumnKind_NONE + 1 + catpb.SystemColumnKind(i)
}

// IsSystemColumnName returns whether or not a name is a reserved system
// column name.
func IsSystemColumnName(name string) bool {
	for i := range AllSystemColumnDescs {
		if AllSystemColumnDescs[i].Name == name {
			return true
		}
	}
	return false
}
